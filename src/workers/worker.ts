/**
 * The step worker.
 *
 * The loop is: take a lease, start heartbeating, execute the node, report the outcome,
 * release the lease. Four properties are load-bearing, and each of them is a thing that
 * goes wrong in a system that gets it slightly wrong.
 *
 * 1. **Heartbeating is independent of execution.** The heartbeat runs on a timer that
 *    the work never awaits. If renewal were interleaved with the work — renewed between
 *    steps of a loop, say — then any step slower than one lease duration would lose its
 *    lease and be reassigned while still running, and the slower the step the more
 *    certain the double-execution. So the only coupling is one-directional: a lost lease
 *    aborts the work, never the reverse.
 *
 * 2. **Execution is idempotent as far as it can be.** See the note on `IDEMPOTENCY`
 *    below; the honest summary is that the worker makes redelivery safe for idempotent
 *    tools and cannot make it safe for ones that declare themselves otherwise.
 *
 * 3. **Cancellation is prompt and gives the lease back.** A cancelled step that holds
 *    its lease until it finishes naturally keeps the scheduler waiting on work whose
 *    result will be discarded.
 *
 * 4. **An unexpected throw is an outcome, not a crash.** One worker process runs many
 *    steps. A handler that throws something unanticipated must become a classified
 *    `step.failed` for that step only; if it propagated, every other step in flight on
 *    the same worker would lose its lease along with it.
 */

import { stepRefKey } from '../workflow/types.js';
import type { NodeDef, StepName, StepRef } from '../workflow/types.js';
import { ToolError } from '../tools/types.js';
import type { ToolContext, ToolExecutor } from '../tools/types.js';
import { idempotencyKeyFor } from '../tools/idempotency.js';
import { PredicateError } from './predicates.js';
import type { PredicateRegistry } from './predicates.js';
import { LlmTimeoutError, runLlmNode } from './llm.js';
import type { LlmProvider } from './llm.js';
import { systemClock } from './ports.js';
import type {
  Clock,
  CompletedStep,
  Lease,
  LeaseManager,
  OutcomeReporter,
  StateReader,
  StepAssignment,
} from './ports.js';

/**
 * IDEMPOTENCY
 *
 * At-least-once delivery is what leasing buys: if a worker stops heartbeating, the step
 * is handed to somebody else, and the original may still be running. So every step this
 * worker executes may execute twice.
 *
 * What the worker relies on: the tool executor's idempotency keys. The key is derived
 * from (workflowId, step, attempt, tool, canonical args) by `idempotencyKeyFor`, so a
 * redelivery of the *same* attempt reaches the executor with the key the first delivery
 * used, and the executor returns the recorded result instead of calling the tool again.
 * The worker performs no deduplication of its own; it would be a second, weaker cache in
 * front of the real one.
 *
 * What it cannot protect:
 *
 *  - A tool whose permissions declare `idempotent: false`. The executor will not replay
 *    its recorded result as a substitute for a call it is not sure completed, and the
 *    worker has no way to find out whether the first delivery's side effect landed. Such
 *    a tool is attempted once per step attempt and a lease expiry mid-call becomes a
 *    step that must be resolved by a human or a compensating action, not by a retry.
 *  - A redelivery that arrives as a *new attempt* number. The key changes with the
 *    attempt, by design — a genuine retry after a genuine failure must be allowed to
 *    call the tool again — so a lease expiry that the scheduler turns into attempt N+1
 *    can duplicate the effect of an attempt N that was in fact still running. That is
 *    why a lease expiry is recorded as `step.lease_expired` and not as `step.failed`:
 *    the decision to re-attempt is the scheduler's, made with that distinction visible.
 *  - Anything the node does outside the tool executor. Nothing here does, and that is
 *    the reason the executor is the only route to a side effect.
 */

/** How a step attempt ended, from the worker's point of view. */
export type StepResult =
  | { readonly kind: 'completed'; readonly output: unknown }
  | { readonly kind: 'failed'; readonly message: string; readonly errorClass: string }
  | { readonly kind: 'cancelled' }
  /** The lease was lost mid-flight; the step now belongs to another worker. */
  | { readonly kind: 'lease_lost' }
  /** This worker does not execute this node kind. The lease goes straight back. */
  | { readonly kind: 'declined'; readonly reason: string };

/** A worker-level failure with a class the retry policy can read. */
export class NodeError extends Error {
  constructor(
    readonly errorClass: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
    this.name = 'NodeError';
  }
}

export interface WorkerOptions {
  readonly workerId: string;
  readonly leases: LeaseManager;
  readonly reporter: OutcomeReporter;
  readonly state: StateReader;
  readonly tools: ToolExecutor;
  readonly predicates?: PredicateRegistry;
  readonly llm?: LlmProvider;
  readonly clock?: Clock;

  /**
   * How often to renew. Defaults to a third of the lease duration, so two consecutive
   * lost heartbeats — a GC pause, a blip — do not cost the lease.
   */
  readonly heartbeatIntervalMs?: number;

  /** Role presented to the tool executor's permission check. */
  readonly role?: string;

  /** Idle poll interval for `run`. */
  readonly pollIntervalMs?: number;

  readonly log?: (message: string, fields?: Record<string, unknown>) => void;
}

/** Why the step's own signal was aborted, which decides what gets reported. */
type StopReason = 'cancelled' | 'attempt_timeout' | 'lease_lost';

export class Worker {
  private readonly clock: Clock;
  private readonly log: (message: string, fields?: Record<string, unknown>) => void;

  /** Heartbeats sent, exposed so a test can assert renewal continued during the work. */
  private heartbeats = 0;

  constructor(private readonly opts: WorkerOptions) {
    this.clock = opts.clock ?? systemClock;
    this.log = opts.log ?? (() => {});
  }

  get workerId(): string {
    return this.opts.workerId;
  }

  get heartbeatCount(): number {
    return this.heartbeats;
  }

  /**
   * Claims one step if there is one and runs it to an outcome.
   *
   * Returns undefined when the queue was empty, so the caller's polling policy stays
   * the caller's business.
   */
  async runOnce(signal?: AbortSignal): Promise<StepResult | undefined> {
    const assignment = await this.opts.leases.acquire(this.opts.workerId);
    if (!assignment) return undefined;
    return this.execute(assignment, signal);
  }

  /** Polls until the signal aborts. */
  async run(signal: AbortSignal): Promise<void> {
    const idle = this.opts.pollIntervalMs ?? 50;
    while (!signal.aborted) {
      let result: StepResult | undefined;
      try {
        result = await this.runOnce(signal);
      } catch (err) {
        // acquire() failing is an infrastructure problem, not a step problem. Logging
        // and continuing keeps a transient control-plane outage from killing the pool.
        this.log('acquire failed', { error: describe(err) });
        result = undefined;
      }
      if (result === undefined) {
        await sleep(idle, this.clock);
      }
    }
  }

  /**
   * Executes one already-leased assignment: heartbeat, dispatch, report, release.
   *
   * Public because the heartbeat and cancellation behaviour is only testable with an
   * assignment the test controls.
   */
  async execute(assignment: StepAssignment, outer?: AbortSignal): Promise<StepResult> {
    const { lease, node } = assignment;
    const controller = new AbortController();
    let stopReason: StopReason | undefined;

    const stop = (reason: StopReason, cause: Error) => {
      // First reason wins: a lease lost after a cancellation is still a cancellation,
      // and reporting the later one would misattribute why the step ended.
      if (stopReason !== undefined) return;
      stopReason = reason;
      controller.abort(cause);
    };

    const unlinkOuter = link(outer, () => stop('cancelled', cancelledError()));

    const cancelHeartbeat = this.startHeartbeat(assignment, () =>
      stop('lease_lost', new NodeError('lease_lost', `lease on ${stepRefKey(lease.ref)} lost`, true)),
    );

    const attemptTimeoutMs = node.attemptTimeoutMs ?? assignment.timeouts.attemptTimeoutMs;
    const attemptTimer = this.clock.setTimeout(() => {
      stop('attempt_timeout', new NodeError('timeout', `attempt exceeded ${attemptTimeoutMs}ms`, true));
    }, attemptTimeoutMs);

    let result: StepResult;
    try {
      const output = await this.dispatch(assignment, controller.signal);
      result =
        stopReason === undefined
          ? { kind: 'completed', output }
          : this.resultForStop(stopReason, attemptTimeoutMs);
    } catch (err) {
      result =
        stopReason === undefined
          ? classify(err)
          : this.resultForStop(stopReason, attemptTimeoutMs, err);
    } finally {
      this.clock.clearTimeout(attemptTimer);
      cancelHeartbeat();
      unlinkOuter();
    }

    await this.finish(lease, result);
    return result;
  }

  /**
   * Starts renewal on its own timer chain.
   *
   * Nothing in here is awaited by the caller: the renewal is fired from a timer and the
   * only thing its result can do is abort the work. That one-directional coupling is
   * what keeps a slow step from losing its lease.
   *
   * The next timer is armed only once the previous renewal has settled, so a renewal
   * slower than the interval cannot stack up behind itself — a queue of renewals against
   * a control plane that is already struggling makes the struggle worse, and the one at
   * the front of the queue is the only one whose answer still matters.
   */
  private startHeartbeat(assignment: StepAssignment, onLost: () => void): () => void {
    const leaseDuration = assignment.timeouts.leaseDurationMs;
    const interval = this.opts.heartbeatIntervalMs ?? Math.max(1, Math.floor(leaseDuration / 3));
    let lease = assignment.lease;
    let stopped = false;
    const self = this;
    let timer = this.clock.setTimeout(tick, interval);

    function tick(): void {
      if (stopped) return;
      self.opts.leases
        .heartbeat(lease)
        .then((alive) => {
          self.heartbeats += 1;
          if (stopped) return;
          if (!alive) {
            self.log('lease lost', { step: stepRefKey(lease.ref) });
            onLost();
            return;
          }
          // Renewal does not change identity, only the expiry the control plane tracks;
          // the token is what proves this holder is still the current one.
          lease = { ...lease, expiresAt: self.clock.now() + leaseDuration };
          timer = self.clock.setTimeout(tick, interval);
        })
        .catch((err: unknown) => {
          // A failed renewal is not immediately fatal: the lease still has time left and
          // the next attempt may succeed. Treating one network error as a lost lease
          // would abandon work that was fine.
          self.log('heartbeat failed', { step: stepRefKey(lease.ref), error: describe(err) });
          if (!stopped) timer = self.clock.setTimeout(tick, interval);
        });
    }

    return () => {
      stopped = true;
      this.clock.clearTimeout(timer);
    };
  }

  private resultForStop(reason: StopReason, attemptTimeoutMs: number, err?: unknown): StepResult {
    switch (reason) {
      case 'cancelled':
        return { kind: 'cancelled' };
      case 'lease_lost':
        return { kind: 'lease_lost' };
      case 'attempt_timeout':
        return {
          kind: 'failed',
          errorClass: 'timeout',
          message:
            err instanceof Error && !(err instanceof NodeError)
              ? `attempt exceeded ${attemptTimeoutMs}ms (${err.message})`
              : `attempt exceeded ${attemptTimeoutMs}ms`,
        };
    }
  }

  /**
   * Reports the outcome and gives the lease back.
   *
   * Reporting failures are swallowed deliberately: the event log is the authority, and
   * if the append did not happen the lease will expire and the step will be redelivered.
   * Throwing here would take out the worker over something the control plane will
   * recover from on its own.
   */
  private async finish(lease: Lease, result: StepResult): Promise<void> {
    try {
      if (result.kind === 'completed') {
        await this.opts.reporter.completed(lease, result.output);
      } else if (result.kind === 'failed') {
        await this.opts.reporter.failed(lease, {
          message: result.message,
          errorClass: result.errorClass,
        });
      }
    } catch (err) {
      this.log('report failed', { step: stepRefKey(lease.ref), error: describe(err) });
    }

    // A lost lease is not ours to release — releasing it would revoke the new holder's
    // claim and hand the same step to a third worker.
    if (result.kind === 'lease_lost') return;

    try {
      await this.opts.leases.release(lease);
    } catch (err) {
      this.log('release failed', { step: stepRefKey(lease.ref), error: describe(err) });
    }
  }

  /** Routes a node to its handler. Throws; `execute` classifies. */
  private async dispatch(assignment: StepAssignment, signal: AbortSignal): Promise<unknown> {
    const { node } = assignment;
    switch (node.kind) {
      case 'tool':
        return this.runTool(assignment, node, assignment.input, signal);
      case 'llm':
        return this.runLlm(assignment, signal);
      case 'condition':
        return this.runCondition(assignment, node);
      case 'parallel':
        return this.runParallel(assignment, signal);
      case 'join':
        return this.runJoin(assignment, node);
      case 'finalize':
        return this.runFinalize(assignment);
      case 'python':
        // Python nodes are executed by the Python worker pool over the wire protocol in
        // python-worker/PROTOCOL.md. Declining rather than failing keeps a mixed pool
        // working: this worker simply is not the one for this step.
        throw new DeclinedNode('python nodes are executed by the python worker pool');
      case 'approval':
        throw new DeclinedNode('approval nodes are resolved by the control plane, not a worker');
    }
  }

  private async runTool(
    assignment: StepAssignment,
    node: NodeDef,
    args: unknown,
    signal: AbortSignal,
  ): Promise<unknown> {
    if (!node.tool) {
      throw new NodeError('definition_error', `tool node "${node.name}" has no tool name`, false);
    }
    const ctx = this.toolContext(assignment, node, args, signal);
    const outcome = await this.opts.tools.invoke(node.tool, args, ctx);
    if (!outcome.ok) {
      throw (
        outcome.error ??
        new NodeError('internal', `tool "${node.tool}" failed without an error`, false)
      );
    }
    return outcome.result;
  }

  private toolContext(
    assignment: StepAssignment,
    node: NodeDef,
    args: unknown,
    signal: AbortSignal,
  ): ToolContext {
    const ref: StepRef = { ...assignment.lease.ref, step: node.name };
    return {
      ref,
      // Derived from the step attempt, which is what makes a redelivery of the same
      // attempt a no-op at the executor. See the IDEMPOTENCY note above. The executor
      // re-derives it the same way; passing it here keeps the key visible to anything
      // that inspects the context, such as a tool writing its own audit record.
      idempotencyKey: idempotencyKeyFor(ref, node.tool ?? node.name, args),
      role: assignment.role ?? this.opts.role ?? 'worker',
      ...(assignment.tenant !== undefined ? { tenant: assignment.tenant } : {}),
      signal,
      log: (message, fields) => this.log(message, { step: stepRefKey(ref), ...fields }),
    };
  }

  private async runLlm(assignment: StepAssignment, signal: AbortSignal): Promise<unknown> {
    const provider = this.opts.llm;
    if (!provider) {
      throw new NodeError('internal', 'worker has no llm provider configured', false);
    }
    const defaultTimeoutMs =
      assignment.node.attemptTimeoutMs ?? assignment.timeouts.attemptTimeoutMs;
    return runLlmNode(assignment.input, { provider, signal, defaultTimeoutMs });
  }

  private async runCondition(assignment: StepAssignment, node: NodeDef): Promise<unknown> {
    const registry = this.opts.predicates;
    if (!registry) {
      throw new NodeError('internal', 'worker has no predicate registry configured', false);
    }
    if (!node.predicate) {
      throw new NodeError('definition_error', `condition node "${node.name}" has no predicate`, false);
    }
    const outputs = await this.opts.state.completedOutputs(assignment.lease.ref.workflowId);
    const value = registry.evaluate(node.predicate, outputs);
    const next = value ? node.onTrue : node.onFalse;
    // The chosen branch is part of the output so the decision is in the event log. A
    // condition whose result is recorded but whose consequence is not leaves the next
    // reader of the history guessing which edge was taken.
    return {
      predicate: node.predicate,
      value,
      ...(next !== undefined ? { next } : {}),
    };
  }

  /**
   * Fans out over the branches in the node's input and gathers their outputs.
   *
   * Branches come from the resolved input rather than from NodeDef because a fan-out
   * width is usually data — one branch per item found by an earlier step — and baking it
   * into the definition would mean a new definition version per input.
   */
  private async runParallel(assignment: StepAssignment, signal: AbortSignal): Promise<unknown> {
    const branches = parseBranches(assignment.input, assignment.node.name);
    const inner = new AbortController();
    const unlink = link(signal, () => inner.abort(signal.reason));

    try {
      const settled = await Promise.allSettled(
        branches.map(async (branch) => {
          const child: StepAssignment = {
            ...assignment,
            node: branch.node,
            input: branch.input,
          };
          return { name: branch.name, output: await this.dispatch(child, inner.signal) };
        }),
      );

      const failures = settled.flatMap((s, i) =>
        s.status === 'rejected'
          ? [`${branches[i]?.name ?? i}: ${describe(s.reason as unknown)}`]
          : [],
      );

      if (failures.length > 0) {
        // Every failure is reported, not just the first. With one reported, a fan-out
        // that broke for three different reasons takes three runs to understand.
        inner.abort(new NodeError('internal', 'sibling branch failed', true));
        throw new NodeError(
          'internal',
          `${failures.length} of ${branches.length} branches failed: ${failures.join(' | ')}`,
          true,
        );
      }

      const outputs: Record<string, unknown> = {};
      for (const s of settled) {
        if (s.status === 'fulfilled') outputs[s.value.name] = s.value.output;
      }
      return { branches: outputs };
    } finally {
      unlink();
    }
  }

  /**
   * Gathers the outputs of the steps this join waits on.
   *
   * A missing input is a hard failure rather than an omission from the result: a join
   * that silently gathers three of four branches produces a plausible-looking output
   * that is quietly incomplete.
   */
  private async runJoin(assignment: StepAssignment, node: NodeDef): Promise<unknown> {
    const from = parseJoinSources(assignment.input) ?? node.dependsOn;
    if (from.length === 0) {
      throw new NodeError('definition_error', `join node "${node.name}" gathers nothing`, false);
    }
    const outputs = await this.opts.state.completedOutputs(assignment.lease.ref.workflowId);
    const gathered: Record<string, unknown> = {};
    const missing: StepName[] = [];
    for (const step of from) {
      const entry = outputs.get(step);
      if (entry === undefined) missing.push(step);
      else gathered[step] = entry.output;
    }
    if (missing.length > 0) {
      throw new NodeError(
        'definition_error',
        `join node "${node.name}" waited on steps that have not completed: ${missing.join(', ')}`,
        false,
      );
    }
    return { gathered };
  }

  /**
   * Produces the workflow's output.
   *
   * Uses the explicitly resolved input when the control plane supplied one, and falls
   * back to the full set of completed outputs, so a workflow that never declared a
   * projection still ends with something inspectable rather than with null.
   */
  private async runFinalize(assignment: StepAssignment): Promise<unknown> {
    const outputs = await this.opts.state.completedOutputs(assignment.lease.ref.workflowId);
    const steps = [...outputs.keys()].sort();
    if (assignment.input !== undefined && assignment.input !== null) {
      return { output: assignment.input, steps };
    }
    const collected: Record<string, unknown> = {};
    for (const [step, entry] of outputs) collected[step] = entry.output;
    return { output: collected, steps };
  }
}

/** Thrown by a handler for a node this worker does not run. */
class DeclinedNode extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DeclinedNode';
  }
}

interface ParallelBranch {
  readonly name: string;
  readonly node: NodeDef;
  readonly input: unknown;
}

function parseBranches(input: unknown, nodeName: StepName): readonly ParallelBranch[] {
  const branches = (input as { branches?: unknown } | null | undefined)?.branches;
  if (!Array.isArray(branches) || branches.length === 0) {
    throw new NodeError(
      'definition_error',
      `parallel node "${nodeName}" input must carry a non-empty "branches" array`,
      false,
    );
  }
  return branches.map((raw, i) => {
    const branch = raw as { name?: unknown; node?: unknown; input?: unknown } | null;
    const node = branch?.node as NodeDef | undefined;
    if (!node || typeof node.kind !== 'string' || typeof node.name !== 'string') {
      throw new NodeError(
        'definition_error',
        `parallel node "${nodeName}" branch ${i} has no usable node definition`,
        false,
      );
    }
    if (node.kind === 'parallel') {
      // Nested fan-out is rejected rather than supported: the concurrency cap belongs to
      // the scheduler, and a branch that fans out again escapes it.
      throw new NodeError(
        'definition_error',
        `parallel node "${nodeName}" branch ${i} is itself a parallel node`,
        false,
      );
    }
    return {
      name: typeof branch?.name === 'string' ? branch.name : node.name,
      node,
      input: branch?.input,
    };
  });
}

function parseJoinSources(input: unknown): readonly StepName[] | undefined {
  const from = (input as { from?: unknown } | null | undefined)?.from;
  if (from === undefined) return undefined;
  if (!Array.isArray(from) || from.some((s) => typeof s !== 'string')) {
    throw new NodeError('definition_error', 'join node input "from" must be an array of step names', false);
  }
  return from as readonly StepName[];
}

/**
 * Maps a thrown value onto a reportable outcome.
 *
 * Classes come from the closed sets the retry policy already understands
 * (`ToolErrorClass`, plus `definition_error` for things no retry can fix). An unknown
 * throw becomes `internal` rather than something retryable-by-default, because
 * retrying an error nobody has classified is how a permanent bug becomes an infinite
 * loop.
 */
export function classify(err: unknown): StepResult {
  if (err instanceof DeclinedNode) {
    return { kind: 'declined', reason: err.message };
  }
  if (err instanceof ToolError) {
    return { kind: 'failed', errorClass: err.errorClass, message: err.message };
  }
  if (err instanceof PredicateError) {
    return { kind: 'failed', errorClass: err.errorClass, message: err.message };
  }
  if (err instanceof NodeError) {
    return { kind: 'failed', errorClass: err.errorClass, message: err.message };
  }
  if (err instanceof LlmTimeoutError) {
    return { kind: 'failed', errorClass: 'timeout', message: err.message };
  }
  if (isAbortError(err)) {
    return { kind: 'cancelled' };
  }
  return { kind: 'failed', errorClass: 'internal', message: describe(err) };
}

function isAbortError(err: unknown): boolean {
  return err instanceof Error && err.name === 'AbortError';
}

function cancelledError(): Error {
  const err = new Error('step cancelled');
  err.name = 'AbortError';
  return err;
}

export function describe(err: unknown): string {
  if (err instanceof Error) {
    return err.message.length > 0 ? `${err.name}: ${err.message}` : err.name;
  }
  return String(err);
}

/**
 * Forwards an abort from one signal to a callback.
 *
 * Hand-rolled because `AbortSignal.any` is not available on the Node 18 baseline this
 * project targets, and the listener has to be removable so a long-lived outer signal
 * does not accumulate one listener per step executed.
 */
function link(signal: AbortSignal | undefined, onAbort: () => void): () => void {
  if (!signal) return () => {};
  if (signal.aborted) {
    onAbort();
    return () => {};
  }
  signal.addEventListener('abort', onAbort, { once: true });
  return () => signal.removeEventListener('abort', onAbort);
}

function sleep(ms: number, clock: Clock): Promise<void> {
  return new Promise<void>((resolve) => clock.setTimeout(resolve, ms));
}

export type { CompletedStep, Lease, LeaseManager, OutcomeReporter, StateReader, StepAssignment };

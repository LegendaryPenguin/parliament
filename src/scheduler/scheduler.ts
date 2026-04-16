/**
 * The scheduler loop.
 *
 * Each tick does the same four things, in this order and for a reason:
 *
 *  1. expire lapsed leases — a step whose worker vanished must become eligible again
 *     before the ready set is computed, or it sits idle until the next tick;
 *  2. fold the history into a projection — the log is the state, so the decision is
 *     made against the fold and nothing else;
 *  3. ask `readySteps` what may run;
 *  4. lease and dispatch, up to the per-workflow and global caps.
 *
 * Nothing here is "current state" held in a field. The in-memory maps track only what
 * *this process* has in flight — the timers it armed and the abort controllers it owns —
 * and all of it is reconstructible by replaying the log, which is what makes `recover()`
 * a replay rather than a reconciliation.
 *
 * The invariant everything else serves: downstream work runs once per upstream
 * completion, no matter how many times that completion is delivered. Duplicate delivery
 * is not an edge case in a durable system, it is the normal consequence of at-least-once
 * messaging, and the defence is that a dependent step can only start by winning a lease.
 */

import type { Event, EventStore, NewEvent, Sequence, WorkflowProjection } from '../events/types.js';
import { ConcurrencyError } from '../events/types.js';
import { project } from '../state/project.js';
import { readySteps } from '../state/ready.js';
import {
  isTerminal,
  stepRefKey,
  type NodeDef,
  type RetryPolicy,
  type StepName,
  type StepRef,
  type WorkflowDef,
  type WorkflowId,
} from '../workflow/types.js';
import type { Clock, TimerHandle } from './clock.js';
import { hasApprovalRequest, hasCompletion, headOf, isWorkflowTerminal, liveLeaseOf } from './history.js';
import { LeaseExpiredError, LeaseLostError, LeaseManager, type LeaseRecord } from './lease.js';
import { NO_RETRY, RetryEngine } from './retry.js';
import { SchedulerWorkerPort } from './worker-port.js';

/** Everything a worker is told about the step it has been handed. */
export interface DispatchRequest {
  readonly ref: StepRef;
  readonly node: NodeDef;
  readonly workerId: string;
  /** The workflow's input, as recorded at creation. */
  readonly input: unknown;
  /** Outputs of the steps this one depends on, so a handler need not read the log. */
  readonly upstream: ReadonlyMap<StepName, unknown>;
  /**
   * Aborted on cancellation, on attempt timeout, and when this worker's lease lapses.
   * A handler that ignores it is not stopped, merely abandoned.
   */
  readonly signal: AbortSignal;
}

/**
 * Runs one step attempt. Resolving gives the step's output; rejecting fails it.
 *
 * A promise rather than a callback pair because the scheduler needs to know when the
 * attempt is no longer occupying a slot, and "told me it finished" and "stopped
 * occupying a worker" have to be the same event or the concurrency cap drifts.
 */
export type Dispatcher = (req: DispatchRequest) => Promise<unknown>;

/** A failure with an explicit class, so the retry policy has something to match on. */
export class StepFailure extends Error {
  constructor(
    readonly errorClass: string,
    message: string,
  ) {
    super(message);
    this.name = 'StepFailure';
  }
}

export interface SchedulerOptions {
  readonly store: EventStore;
  readonly clock: Clock;
  readonly dispatch: Dispatcher;

  /** Identifies this process in `step.leased`. Two schedulers must not share one. */
  readonly schedulerId?: string;

  /** Ceiling on concurrent attempts across all workflows this process serves. */
  readonly maxInFlight?: number;

  /** Default lease duration when a definition does not state one. */
  readonly leaseDurationMs?: number;

  /** Injected so backoff is assertable in tests. */
  readonly random?: () => number;

  /**
   * How much one skipped tick counts for when ordering workflows. Priority is a
   * preference, not a precedence: see `orderWorkflows`.
   */
  readonly starvationWeight?: number;

  readonly tickIntervalMs?: number;
}

export interface StartOptions {
  readonly input?: unknown;
  readonly priority?: number;
  readonly tenant?: string;
  /** Absolute deadline. Defaults to creation time plus `timeouts.workflowDeadlineMs`. */
  readonly deadlineAt?: number;
}

interface InFlight {
  readonly key: string;
  readonly workflowId: WorkflowId;
  readonly ref: StepRef;
  readonly node: NodeDef;
  readonly workerId: string;
  readonly abort: AbortController;
  lease: LeaseRecord;
  attemptTimer?: TimerHandle | undefined;
  heartbeatTimer?: TimerHandle | undefined;
  expiryTimer?: TimerHandle | undefined;
  settled: boolean;
}

const DEFAULT_MAX_IN_FLIGHT = 16;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_TICK_MS = 250;
const DEFAULT_STARVATION_WEIGHT = 1;

/** Appends that are not a race for exclusivity may be retried against a fresh head. */
const APPEND_ATTEMPTS = 4;

export class Scheduler {
  private readonly store: EventStore;
  private readonly clock: Clock;
  private readonly dispatch: Dispatcher;
  private readonly leases: LeaseManager;
  private readonly retries: RetryEngine;
  private readonly schedulerId: string;
  private readonly maxInFlight: number;
  private readonly defaultLeaseMs: number;
  private readonly starvationWeight: number;
  private readonly tickIntervalMs: number;

  private readonly defs = new Map<string, WorkflowDef>();
  private readonly inFlight = new Map<string, InFlight>();

  /**
   * Priority and starvation credit, held only in memory.
   *
   * Priority is a scheduling preference rather than a property of the execution, and the
   * event envelope has nowhere to record it, so a recovered workflow comes back at the
   * default. That is a deliberate limitation and not a silent one: losing priority
   * degrades ordering after a restart, whereas inventing a durable field the log does not
   * define would break the compatibility contract.
   */
  private readonly priorities = new Map<WorkflowId, number>();
  private readonly skipped = new Map<WorkflowId, number>();

  private workerSeq = 0;
  private loopTimer: TimerHandle | undefined;
  private stopped = false;

  constructor(opts: SchedulerOptions) {
    this.store = opts.store;
    this.clock = opts.clock;
    this.dispatch = opts.dispatch;
    this.schedulerId = opts.schedulerId ?? 'scheduler-1';
    this.maxInFlight = opts.maxInFlight ?? DEFAULT_MAX_IN_FLIGHT;
    this.defaultLeaseMs = opts.leaseDurationMs ?? DEFAULT_LEASE_MS;
    this.starvationWeight = opts.starvationWeight ?? DEFAULT_STARVATION_WEIGHT;
    this.tickIntervalMs = opts.tickIntervalMs ?? DEFAULT_TICK_MS;
    this.leases = new LeaseManager({
      store: opts.store,
      clock: opts.clock,
      leaseDurationMs: this.defaultLeaseMs,
    });
    this.retries = new RetryEngine({
      store: opts.store,
      clock: opts.clock,
      ...(opts.random ? { random: opts.random } : {}),
    });
  }

  register(def: WorkflowDef): void {
    this.defs.set(defKey(def.name, def.version), def);
    this.defs.set(def.name, def);
  }

  /**
   * The worker-facing ports, backed by this scheduler's leases, definitions and ordering.
   *
   * Pull and push both go through the same `LeaseManager.acquire`, so a deployment that
   * runs both cannot double-dispatch a step — one of them simply loses the append. What it
   * can do is make the global in-flight cap approximate, which is why the port takes its
   * own limit rather than sharing this one.
   */
  workerPort(opts: { readonly maxInFlight?: number; readonly role?: string } = {}): SchedulerWorkerPort {
    return new SchedulerWorkerPort({
      store: this.store,
      clock: this.clock,
      leases: this.leases,
      retries: this.retries,
      defs: (name, version) => this.defs.get(defKey(name, version)) ?? this.defs.get(name),
      order: (ids) => this.orderWorkflows(ids),
      ...(opts.maxInFlight !== undefined ? { maxInFlight: opts.maxInFlight } : {}),
      ...(opts.role !== undefined ? { role: opts.role } : {}),
    });
  }

  /** Attempts in flight in this process. Approval waits are not counted; see `tickWorkflow`. */
  get inFlightCount(): number {
    return this.inFlight.size;
  }

  /** The step refs this process currently holds leases for, for assertions and draining. */
  activeSteps(): readonly StepRef[] {
    return [...this.inFlight.values()].map((e) => e.ref);
  }

  /**
   * Creates an execution and schedules its entry points in the same append.
   *
   * The entry points are scheduled here rather than discovered later because a workflow
   * whose history is only `workflow.created` is indistinguishable from one the scheduler
   * has not reached yet, and the difference matters during recovery.
   */
  async start(def: WorkflowDef, opts: StartOptions = {}): Promise<WorkflowId> {
    this.register(def);
    const id = `${def.name}-${++this.workerSeq}-${this.clock.now()}`;
    await this.startAs(id, def, opts);
    return id;
  }

  /** As `start`, with a caller-chosen id so a test can predict it. */
  async startAs(id: WorkflowId, def: WorkflowDef, opts: StartOptions = {}): Promise<void> {
    this.register(def);
    const now = this.clock.now();
    const deadlineAt =
      opts.deadlineAt ??
      (def.timeouts.workflowDeadlineMs !== undefined
        ? now + def.timeouts.workflowDeadlineMs
        : undefined);

    const batch: NewEvent[] = [
      {
        type: 'workflow.created',
        workflowId: id,
        at: now,
        workflow: def.name,
        definitionVersion: def.version,
        definition: def,
        input: opts.input ?? null,
        ...(deadlineAt !== undefined ? { deadlineAt } : {}),
        ...(opts.tenant !== undefined ? { tenant: opts.tenant } : {}),
      },
    ];

    for (const node of def.nodes) {
      if (node.dependsOn.length === 0) {
        batch.push({
          type: 'step.scheduled',
          workflowId: id,
          at: now,
          step: node.name,
          attempt: 1,
          notBefore: now,
        });
      }
    }

    await this.store.append(id, 0, batch);
    if (opts.priority !== undefined) this.priorities.set(id, opts.priority);
  }

  /** Runs the loop until `stop()`. Timers come from the clock, so tests drive it. */
  startLoop(): void {
    this.stopped = false;
    const run = (): void => {
      if (this.stopped) return;
      void this.tick().finally(() => {
        if (!this.stopped) this.loopTimer = this.clock.setTimeout(run, this.tickIntervalMs);
      });
    };
    this.loopTimer = this.clock.setTimeout(run, this.tickIntervalMs);
  }

  stop(): void {
    this.stopped = true;
    if (this.loopTimer !== undefined) this.clock.clearTimeout(this.loopTimer);
    this.loopTimer = undefined;
    for (const entry of this.inFlight.values()) this.clearTimers(entry);
  }

  /** One pass over every active workflow, in preference order. */
  async tick(): Promise<void> {
    const active = await this.store.listActive();
    for (const id of this.orderWorkflows(active)) {
      await this.tickWorkflow(id);
    }
  }

  /**
   * Replays the store and resumes.
   *
   * There is nothing to do beyond the fold: leases taken by the previous process are
   * either still live — in which case this one leaves them alone until they lapse — or
   * already lapsed, in which case step 1 of the tick reassigns them. No separate
   * reconciliation pass exists, because a reconciliation pass is a second source of
   * truth about what was running.
   */
  async recover(): Promise<void> {
    const active = await this.store.listActive();
    for (const id of active) {
      await this.tickWorkflow(id);
    }
  }

  async tickWorkflow(id: WorkflowId): Promise<void> {
    if (this.stopped) return;

    // Lapsed leases first, so a reassignable step is in the ready set this tick.
    const expired = await this.leases.expireOverdue(id);
    for (const lease of expired) this.abandonLocal(id, lease.step);

    let events = await this.store.read(id);
    if (events.length === 0) return;

    if (isWorkflowTerminal(events)) {
      // A terminal workflow is never scheduled into again, whatever arrives afterwards.
      // This is the single check that makes a duplicate completion delivered after a
      // cancellation harmless: it is recorded, and it advances nothing.
      this.abandonWorkflow(id, 'workflow is terminal');
      return;
    }

    const def = this.defFor(events);
    if (!def) return;

    let projection = project(def, events);
    const now = this.clock.now();

    if (projection.deadlineAt !== undefined && now > projection.deadlineAt) {
      // The whole execution has stopped being useful. Nothing further starts, and the
      // failure is recorded once so the log explains why work stopped.
      await this.tryAppend(id, () => [
        {
          type: 'workflow.failed',
          workflowId: id,
          at: now,
          error: `workflow deadline exceeded at ${projection.deadlineAt ?? 0}`,
          errorClass: 'deadline_exceeded',
        },
      ]);
      this.abandonWorkflow(id, 'workflow deadline exceeded');
      return;
    }

    if (isTerminal(projection.state)) return;

    let candidates = readySteps(def, projection, now).map((s) => s.step);

    // Approval gates are handled before any slot accounting, because an approval
    // consumes no worker: the workflow waits on a human, and holding a lease while it
    // does would idle a worker for as long as the human takes.
    const gated = candidates.filter((name) => def.nodes.find((n) => n.name === name)?.kind === 'approval');
    if (gated.length > 0) {
      const toRequest = gated.filter((name) => !hasApprovalRequest(events, name));
      if (toRequest.length > 0) {
        await this.tryAppend(id, (fresh) =>
          toRequest
            .filter((name) => !hasApprovalRequest(fresh, name))
            .map((name): NewEvent => {
              const node = def.nodes.find((n) => n.name === name);
              return {
                type: 'approval.requested',
                workflowId: id,
                at: this.clock.now(),
                step: name,
                approvers: node?.approvers ?? [],
                prompt: node?.approvalPrompt ?? `approve ${name}`,
                payload: this.upstreamOf(def, projection, name),
              };
            }),
        );
        events = await this.store.read(id);
        projection = project(def, events);
      }
      candidates = candidates.filter((name) => !gated.includes(name));
    }

    const runnable = candidates.filter((name) => {
      const step = projection.steps.get(name);
      if (step && isTerminal(step.state)) return false;
      if (step?.notBefore !== undefined && step.notBefore > now) return false;
      if (this.inFlight.has(stepRefKey({ workflowId: id, step: name, attempt: attemptOf(projection, name) }))) {
        return false;
      }
      // A live lease held by anyone — this process or another — means the step is taken.
      return liveLeaseOf(events, name, now) === undefined;
    });

    if (runnable.length === 0) {
      this.skipped.delete(id);
      await this.maybeComplete(def, id, events, projection);
      return;
    }

    const perWorkflowLimit = def.maxConcurrency ?? Number.POSITIVE_INFINITY;
    let running = this.runningFor(id, events, now);
    let dispatched = 0;

    for (const name of runnable) {
      if (running >= perWorkflowLimit) break;
      if (this.inFlight.size >= this.maxInFlight) break;

      const node = def.nodes.find((n) => n.name === name);
      if (!node) continue;

      const ref: StepRef = { workflowId: id, step: name, attempt: attemptOf(projection, name) };
      const dispatchedOk = await this.leaseAndDispatch(def, ref, node, projection);
      if (dispatchedOk) {
        running += 1;
        dispatched += 1;
      }
    }

    // Starvation credit, earned only by a workflow that had work and was served none of
    // it. Crediting "did not get everything it wanted" instead would let a wide fan-out
    // accrue credit as fast as the workflow it is starving, and the ordinary workflow
    // would never catch up — which is the bug this is supposed to prevent.
    if (dispatched === 0) {
      this.skipped.set(id, (this.skipped.get(id) ?? 0) + 1);
    } else {
      this.skipped.delete(id);
    }
  }

  /** Cancels an execution: no new steps, and running ones are asked to stop. */
  async cancel(id: WorkflowId, reason: string, requestedBy?: string): Promise<boolean> {
    const appended = await this.tryAppend(id, (events) =>
      isWorkflowTerminal(events)
        ? []
        : [
            {
              type: 'workflow.cancelled',
              workflowId: id,
              at: this.clock.now(),
              reason,
              ...(requestedBy !== undefined ? { requestedBy } : {}),
            },
          ],
    );
    // Cancellation propagates to the work itself, not just to the log: the abort frees
    // the slot immediately, so a cancelled workflow stops consuming capacity.
    this.abandonWorkflow(id, `cancelled: ${reason}`);
    return appended;
  }

  /**
   * Records an approval and completes the gate step.
   *
   * Ignored outright once the workflow is terminal. An approval granted after a
   * cancellation is a human answering a question that has been withdrawn; acting on it
   * would restart a cancelled workflow, which is the transition the state machine exists
   * to forbid.
   */
  async approve(id: WorkflowId, step: StepName, approver: string, note?: string): Promise<boolean> {
    const granted = await this.tryAppend(id, (events) => {
      if (isWorkflowTerminal(events) || !hasApprovalRequest(events, step)) return [];
      const attempt = lastScheduledAttempt(events, step);
      if (hasCompletion(events, step, attempt)) return [];
      const now = this.clock.now();
      return [
        {
          type: 'approval.granted',
          workflowId: id,
          at: now,
          step,
          approver,
          ...(note !== undefined ? { note } : {}),
        },
        {
          type: 'step.completed',
          workflowId: id,
          at: now,
          step,
          attempt,
          workerId: `approval:${approver}`,
          output: { approved: true, approver },
        },
      ];
    });
    if (granted) await this.tickWorkflow(id);
    return granted;
  }

  /**
   * Records a denial and fails the workflow.
   *
   * A denied gate is not a retryable failure and not a step that waits forever: the
   * protected step must never run, and the only state that guarantees that is a terminal
   * one.
   */
  async deny(id: WorkflowId, step: StepName, approver: string, reason: string): Promise<boolean> {
    const denied = await this.tryAppend(id, (events) => {
      if (isWorkflowTerminal(events) || !hasApprovalRequest(events, step)) return [];
      const attempt = lastScheduledAttempt(events, step);
      const now = this.clock.now();
      return [
        { type: 'approval.denied', workflowId: id, at: now, step, approver, reason },
        {
          type: 'step.failed',
          workflowId: id,
          at: now,
          step,
          attempt,
          error: `approval denied by ${approver}: ${reason}`,
          errorClass: 'approval_denied',
          // A denied gate is the end of the road: there is no attempt that would be
          // approved on the retry, because the decision was about the work itself.
          willRetry: false,
        },
        {
          type: 'workflow.failed',
          workflowId: id,
          at: now,
          step,
          error: `approval denied by ${approver}: ${reason}`,
          errorClass: 'approval_denied',
        },
      ];
    });
    if (denied) this.abandonWorkflow(id, 'approval denied');
    return denied;
  }

  private async leaseAndDispatch(
    def: WorkflowDef,
    ref: StepRef,
    node: NodeDef,
    projection: WorkflowProjection,
  ): Promise<boolean> {
    const workerId = `${this.schedulerId}/w${++this.workerSeq}`;
    const leaseMs = def.timeouts.leaseDurationMs || this.defaultLeaseMs;

    const lease = await this.leases.acquire(ref, workerId, leaseMs);
    // Losing the lease is the ordinary outcome when another scheduler is also awake, and
    // it is also what prevents a duplicate completion from dispatching a dependent twice.
    if (!lease) return false;

    const abort = new AbortController();
    const entry: InFlight = {
      key: stepRefKey(ref),
      workflowId: ref.workflowId,
      ref,
      node,
      workerId,
      abort,
      lease,
      settled: false,
    };
    this.inFlight.set(entry.key, entry);

    await this.tryAppend(ref.workflowId, () => [
      {
        type: 'step.started',
        workflowId: ref.workflowId,
        at: this.clock.now(),
        step: ref.step,
        attempt: ref.attempt,
        workerId,
      },
    ]);

    const attemptMs = node.attemptTimeoutMs ?? def.timeouts.attemptTimeoutMs;
    if (attemptMs > 0) {
      entry.attemptTimer = this.clock.setTimeout(() => {
        void this.onFailed(entry, new StepFailure('timeout', `${ref.step} exceeded ${attemptMs}ms`));
      }, attemptMs);
    }
    this.scheduleHeartbeat(entry, leaseMs);
    this.armExpiryCheck(entry, leaseMs);

    const req: DispatchRequest = {
      ref,
      node,
      workerId,
      input: projection.input,
      upstream: this.upstreamOf(def, projection, ref.step),
      signal: abort.signal,
    };

    void Promise.resolve()
      .then(() => this.dispatch(req))
      .then(
        (output) => this.onSucceeded(entry, output),
        (err: unknown) => this.onFailed(entry, err),
      );

    return true;
  }

  private async onSucceeded(entry: InFlight, output: unknown): Promise<void> {
    if (entry.settled) return;
    this.settle(entry);

    await this.tryAppend(entry.workflowId, (events) => {
      // A completion against a terminal workflow is recorded nowhere and advances
      // nothing. The work may well have been done; the workflow has stopped caring.
      if (isWorkflowTerminal(events)) return [];
      if (hasCompletion(events, entry.ref.step, entry.ref.attempt)) return [];
      return [
        {
          type: 'step.completed',
          workflowId: entry.workflowId,
          at: this.clock.now(),
          step: entry.ref.step,
          attempt: entry.ref.attempt,
          workerId: entry.workerId,
          output,
        },
      ];
    });

    // A full pass rather than just this workflow: the slot this step has given back is a
    // global resource, so it should go to whichever workflow the preference order picks,
    // not automatically to the one that happened to free it. Re-ticking only the local
    // workflow would let a busy workflow hold the last slot indefinitely, which is the
    // starvation the priority ordering is supposed to bound.
    await this.tick();
  }

  private async onFailed(entry: InFlight, err: unknown): Promise<void> {
    if (entry.settled) return;
    this.settle(entry);
    entry.abort.abort();

    const events = await this.store.read(entry.workflowId);
    const def = this.defFor(events);
    // An absent policy means *no* retry, not a default one. `project()` folds a failure
    // to terminal `failed` whenever the node declares no policy, so retrying here would
    // append a `step.retry_scheduled` that the fold then rejects as a transition out of
    // a terminal state. Opting a step into retries is the definition's job.
    const policy: RetryPolicy =
      entry.node.retry ?? def?.nodes.find((n) => n.name === entry.ref.step)?.retry ?? NO_RETRY;
    const deadlineAt = deadlineOf(events);

    const decision = await this.retries.recordFailure({
      workflowId: entry.workflowId,
      step: entry.ref.step,
      attempt: entry.ref.attempt,
      workerId: entry.workerId,
      error: err instanceof Error ? err.message : String(err),
      errorClass: classifyError(err),
      policy,
      ...(deadlineAt !== undefined ? { deadlineAt } : {}),
    });

    if (decision.kind === 'retry') {
      // Wake exactly when the backoff elapses. Without this the retry would wait for the
      // next poll, which makes the observed backoff the tick interval rather than the
      // policy's.
      const delay = Math.max(0, decision.notBefore - this.clock.now());
      this.clock.setTimeout(() => void this.tick(), delay);
      return;
    }

    await this.tick();
  }

  private scheduleHeartbeat(entry: InFlight, leaseMs: number): void {
    // A third of the lease: two consecutive heartbeats may be lost before the lease
    // lapses, so a single slow append does not cause a spurious reassignment.
    const interval = Math.max(1, Math.floor(leaseMs / 3));
    entry.heartbeatTimer = this.clock.setTimeout(() => {
      if (entry.settled) return;
      void this.leases
        .heartbeat(entry.lease, leaseMs)
        .then((renewed) => {
          entry.lease = renewed;
          this.scheduleHeartbeat(entry, leaseMs);
          this.armExpiryCheck(entry, leaseMs);
        })
        .catch((err: unknown) => {
          if (err instanceof LeaseExpiredError || err instanceof LeaseLostError) {
            // This worker can no longer prove it holds the step. It stops, and the step
            // is left to whoever the log says owns it. Nothing is appended here: the
            // reassignment path records `step.lease_expired`.
            this.settle(entry);
            entry.abort.abort();
            return;
          }
          throw err;
        });
    }, interval);
  }

  private armExpiryCheck(entry: InFlight, leaseMs: number): void {
    if (entry.expiryTimer !== undefined) this.clock.clearTimeout(entry.expiryTimer);
    const delay = Math.max(1, entry.lease.expiresAt - this.clock.now() + 1);
    entry.expiryTimer = this.clock.setTimeout(() => {
      if (entry.settled) return;
      void this.tickWorkflow(entry.workflowId).then(() => {
        if (!entry.settled) this.armExpiryCheck(entry, leaseMs);
      });
    }, delay);
  }

  /**
   * Completes a workflow that has nothing left to do.
   *
   * Quiescence rather than "all nodes succeeded", because a condition node leaves the
   * branch it did not take pending forever; a pending step whose dependencies were
   * satisfied would have appeared in the ready set, so one that has not is unreachable.
   */
  private async maybeComplete(
    def: WorkflowDef,
    id: WorkflowId,
    events: readonly Event[],
    projection: WorkflowProjection,
  ): Promise<void> {
    if (isTerminal(projection.state)) return;
    if ([...this.inFlight.values()].some((e) => e.workflowId === id)) return;

    const now = this.clock.now();
    let succeeded = 0;
    for (const step of projection.steps.values()) {
      if (step.state === 'succeeded') {
        succeeded += 1;
        continue;
      }
      if (step.state === 'pending') continue;
      // Anything ready, running, waiting or retrying is outstanding work; anything
      // terminal-but-not-succeeded means the workflow is failing, not completing, and
      // whoever recorded that failure owns the workflow-level outcome.
      return;
    }
    if (succeeded === 0) return;
    if (def.nodes.some((n) => liveLeaseOf(events, n.name, now) !== undefined)) return;

    const finalize = def.nodes.find((n) => n.kind === 'finalize');
    const output =
      finalize && projection.steps.get(finalize.name)?.state === 'succeeded'
        ? projection.steps.get(finalize.name)?.output
        : Object.fromEntries(
            [...projection.steps.entries()]
              .filter(([, s]) => s.state === 'succeeded')
              .map(([name, s]) => [name, s.output]),
          );

    await this.tryAppend(id, (fresh) =>
      isWorkflowTerminal(fresh)
        ? []
        : [{ type: 'workflow.completed', workflowId: id, at: this.clock.now(), output }],
    );
  }

  /**
   * Appends a batch computed from the current head, retrying on conflict.
   *
   * The batch is produced by a callback rather than passed in, because a conflict means
   * the history the decision was based on is stale: the decision has to be made again
   * against the new history, not merely re-submitted against a new sequence number.
   * Leasing deliberately does *not* go through here — there, losing is the answer.
   */
  private async tryAppend(
    id: WorkflowId,
    build: (events: readonly Event[], head: Sequence) => readonly NewEvent[],
  ): Promise<boolean> {
    for (let i = 0; i < APPEND_ATTEMPTS; i++) {
      const events = await this.store.read(id);
      const head = headOf(events);
      const batch = build(events, head);
      if (batch.length === 0) return false;
      try {
        await this.store.append(id, head, batch);
        return true;
      } catch (err) {
        if (err instanceof ConcurrencyError) continue;
        throw err;
      }
    }
    return false;
  }

  /** Steps occupying a worker for this workflow, counting other processes' leases. */
  private runningFor(id: WorkflowId, events: readonly Event[], now: number): number {
    const leased = new Set<StepName>();
    for (const e of events) {
      if (e.type === 'step.leased' && liveLeaseOf(events, e.step, now)) leased.add(e.step);
    }
    for (const entry of this.inFlight.values()) {
      if (entry.workflowId === id) leased.add(entry.ref.step);
    }
    return leased.size;
  }

  private upstreamOf(
    def: WorkflowDef,
    projection: WorkflowProjection,
    step: StepName,
  ): ReadonlyMap<StepName, unknown> {
    const node = def.nodes.find((n) => n.name === step);
    const upstream = new Map<StepName, unknown>();
    for (const dep of node?.dependsOn ?? []) {
      const projected = projection.steps.get(dep);
      if (projected) upstream.set(dep, projected.output);
    }
    return upstream;
  }

  private defFor(events: readonly Event[]): WorkflowDef | undefined {
    const created = events.find((e) => e.type === 'workflow.created');
    if (!created || created.type !== 'workflow.created') return undefined;
    return (
      this.defs.get(defKey(created.workflow, created.definitionVersion)) ??
      this.defs.get(created.workflow)
    );
  }

  /** Stops tracking one step locally without asserting anything about the work itself. */
  private abandonLocal(id: WorkflowId, step: StepName): void {
    for (const entry of this.inFlight.values()) {
      if (entry.workflowId === id && entry.ref.step === step) {
        this.settle(entry);
        entry.abort.abort();
      }
    }
  }

  private abandonWorkflow(id: WorkflowId, reason: string): void {
    for (const entry of this.inFlight.values()) {
      if (entry.workflowId !== id) continue;
      this.settle(entry);
      // Signalled rather than killed: a handler that honours the signal can release what
      // it holds, and one that does not is at least no longer counted against capacity.
      entry.abort.abort(new Error(reason));
    }
    this.skipped.delete(id);
  }

  private settle(entry: InFlight): void {
    entry.settled = true;
    this.clearTimers(entry);
    this.inFlight.delete(entry.key);
  }

  private clearTimers(entry: InFlight): void {
    for (const timer of [entry.attemptTimer, entry.heartbeatTimer, entry.expiryTimer]) {
      if (timer !== undefined) this.clock.clearTimeout(timer);
    }
    entry.attemptTimer = undefined;
    entry.heartbeatTimer = undefined;
    entry.expiryTimer = undefined;
  }

  /**
   * Orders workflows by priority with a starvation credit added.
   *
   * Bounded preference, not strict precedence. Strict priority is simple and starves:
   * a steady stream of high-priority work means an ordinary workflow never runs at all,
   * which is indistinguishable from the system having lost it. Each tick a workflow has
   * work but gets no slot, its credit grows, and it eventually outranks the busy
   * high-priority ones.
   */
  private orderWorkflows(ids: readonly WorkflowId[]): readonly WorkflowId[] {
    const score = (id: WorkflowId): number =>
      (this.priorities.get(id) ?? 0) + this.starvationWeight * (this.skipped.get(id) ?? 0);
    return [...ids].sort((a, b) => score(b) - score(a));
  }
}

function defKey(name: string, version: number): string {
  return `${name}@${version}`;
}

function attemptOf(projection: WorkflowProjection, step: StepName): number {
  // The fold starts every step at attempt 0 and only raises it from events, so a step
  // that has become ready through its dependencies rather than through an explicit
  // `step.scheduled` reports 0. Attempts are 1-based everywhere they are read — retry
  // budgets, idempotency keys — so the first dispatch is attempt 1.
  return Math.max(1, projection.steps.get(step)?.attempt ?? 1);
}

function deadlineOf(events: readonly Event[]): number | undefined {
  for (const e of events) {
    if (e.type === 'workflow.created') return e.deadlineAt;
  }
  return undefined;
}

function lastScheduledAttempt(events: readonly Event[], step: StepName): number {
  let attempt = 1;
  for (const e of events) {
    if (e.type === 'step.scheduled' && e.step === step) attempt = e.attempt;
  }
  return attempt;
}

/**
 * Maps a thrown value onto a retry class.
 *
 * Unknown throws become `internal` rather than something retryable-by-default, so a bug
 * that throws a plain `Error` is retried only if the policy actually lists `internal`.
 */
function classifyError(err: unknown): string {
  if (err instanceof StepFailure) return err.errorClass;
  if (typeof err === 'object' && err !== null && 'errorClass' in err) {
    const cls = (err as { errorClass: unknown }).errorClass;
    if (typeof cls === 'string') return cls;
  }
  return 'internal';
}


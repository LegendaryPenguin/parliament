/**
 * The scheduler's implementation of the ports a worker needs.
 *
 * Workers are the component most likely to run out of process — one pool per node kind,
 * on separate hosts — so `src/workers/ports.ts` declares what a worker needs in its own
 * terms and depends on nothing here. The two sides therefore disagree about shape, and
 * the adapter belongs on this side: deciding *what runs next* is a scheduling decision,
 * and a worker that reached into the projection to pick its own work would be making it.
 *
 * Two deliberate translations:
 *
 *  - The worker's `heartbeat` returns a boolean; the lease manager throws. A worker that
 *    had to catch two exception types to learn it lost a lease is worse than one that
 *    checks a boolean, so the exceptions stop here.
 *  - `Lease.token` carries the sequence of the `step.leased` event. Optimistic
 *    concurrency already keys on sequence numbers, and a second identifier is a second
 *    thing that can disagree with the first.
 */

import { ConcurrencyError } from '../events/types.js';
import type { Event, EventStore, NewEvent } from '../events/types.js';
import { project } from '../state/project.js';
import { readySteps } from '../state/ready.js';
import {
  isTerminal,
  type NodeDef,
  type NodeKind,
  type StepName,
  type StepRef,
  type Timeouts,
  type WorkflowDef,
  type WorkflowId,
  type WorkflowName,
} from '../workflow/types.js';
import type {
  CompletedStep,
  Lease,
  LeaseManager as LeaseManagerPort,
  OutcomeReporter,
  StateReader,
  StepAssignment,
} from '../workers/ports.js';
import type { Clock } from './clock.js';
import { hasCompletion, hasOutcome, headOf, isWorkflowTerminal, liveLeaseOf } from './history.js';
import { LeaseExpiredError, LeaseLostError, LeaseManager, type LeaseRecord } from './lease.js';
import { NO_RETRY, RetryEngine } from './retry.js';

/** How the port finds the definition a history was produced against. */
export type DefLookup = (name: WorkflowName, version: number) => WorkflowDef | undefined;

export interface WorkerPortOptions {
  readonly store: EventStore;
  readonly clock: Clock;
  readonly leases: LeaseManager;
  readonly retries: RetryEngine;
  readonly defs: DefLookup;

  /** Orders candidate workflows. Defaults to the store's order. */
  readonly order?: (ids: readonly WorkflowId[]) => readonly WorkflowId[];

  /**
   * Belt-and-braces cap on live leases. In a pull model the size of the worker pool is
   * the real limit — a worker that is busy does not ask for more work — so this defaults
   * to unlimited and exists for a deployment that also pushes.
   */
  readonly maxInFlight?: number;

  /** Role presented to the tool executor's permission check. */
  readonly role?: string;
}

export class SchedulerWorkerPort implements LeaseManagerPort, OutcomeReporter, StateReader {
  private readonly store: EventStore;
  private readonly clock: Clock;
  private readonly leases: LeaseManager;
  private readonly retries: RetryEngine;
  private readonly defs: DefLookup;
  private readonly order: (ids: readonly WorkflowId[]) => readonly WorkflowId[];
  private readonly maxInFlight: number;
  private readonly role: string | undefined;

  /**
   * Node kinds each worker has handed back undone.
   *
   * A worker declines an assignment whose kind it does not execute — a TypeScript worker
   * given a `python` node — and the only visible evidence is a lease returned with no
   * outcome. Without remembering it, the next `acquire` from the same worker offers the
   * same step again and the pair spins at the polling rate forever. The step stays ready
   * for a worker that does handle the kind; if no such worker exists, it waits until the
   * workflow deadline, which is the honest outcome for a workflow nobody can run.
   */
  private readonly declined = new Map<string, Set<NodeKind>>();

  constructor(opts: WorkerPortOptions) {
    this.store = opts.store;
    this.clock = opts.clock;
    this.leases = opts.leases;
    this.retries = opts.retries;
    this.defs = opts.defs;
    this.order = opts.order ?? ((ids) => ids);
    this.maxInFlight = opts.maxInFlight ?? Number.POSITIVE_INFINITY;
    this.role = opts.role;
  }

  /**
   * Finds one runnable step this worker is eligible for and leases it.
   *
   * Non-blocking by contract, so an idle worker polls instead of the control plane
   * holding a connection open per idle worker.
   */
  async acquire(workerId: string): Promise<StepAssignment | undefined> {
    const declined = this.declined.get(workerId);
    let leasedSeen = 0;

    for (const id of this.order(await this.store.listActive())) {
      // Lapsed leases are reassigned before anything is offered, so a step whose worker
      // vanished is eligible on this poll rather than the next one.
      await this.leases.expireOverdue(id);

      const events = await this.store.read(id);
      if (events.length === 0 || isWorkflowTerminal(events)) continue;

      const def = this.defFor(events);
      if (!def) continue;

      const projection = project(def, events);
      if (isTerminal(projection.state)) continue;

      const now = this.clock.now();
      // Nothing starts past the deadline. The execution is no longer useful, and handing
      // a worker an attempt that cannot be delivered on time only wastes it.
      if (projection.deadlineAt !== undefined && now > projection.deadlineAt) continue;

      for (const node of def.nodes) {
        if (liveLeaseOf(events, node.name, now)) leasedSeen += 1;
      }
      if (leasedSeen >= this.maxInFlight) return undefined;

      for (const candidate of readySteps(def, projection, now)) {
        const node = def.nodes.find((n) => n.name === candidate.step);
        if (!node) continue;
        // An approval gate is answered by a human, not executed by a worker, and holding
        // a worker for the length of a human decision is the thing gates must not do.
        if (node.kind === 'approval') continue;
        if (declined?.has(node.kind)) continue;

        const ref: StepRef = {
          workflowId: id,
          step: node.name,
          // The fold starts a step at attempt 0 and raises it from events; attempts are
          // 1-based wherever they are read, including in idempotency keys.
          attempt: Math.max(1, candidate.attempt),
        };
        const leaseMs = def.timeouts.leaseDurationMs || this.leases.leaseDurationMs;
        const lease = await this.leases.acquire(ref, workerId, leaseMs);
        // Another worker got there first. Ordinary, and the reason this needs no lock.
        if (!lease) continue;

        await this.recordStarted(ref, workerId);

        return {
          lease: toPortLease(lease),
          node,
          input: bindInput(node, projection.input, projection),
          timeouts: timeoutsFor(def, node),
          ...(projection.tenant !== undefined ? { tenant: projection.tenant } : {}),
          ...(this.role !== undefined ? { role: this.role } : {}),
        };
      }
    }

    return undefined;
  }

  /** Extends a lease, reporting the loss of one as `false` rather than as an exception. */
  async heartbeat(lease: Lease): Promise<boolean> {
    try {
      await this.leases.heartbeat(fromPortLease(lease));
      return true;
    } catch (err) {
      // Both of these mean the same thing to a worker: the step is now somebody else's
      // and the only correct response is to stop. Anything else is a real fault and is
      // allowed to propagate.
      if (err instanceof LeaseExpiredError || err instanceof LeaseLostError) return false;
      throw err;
    }
  }

  /**
   * Takes a lease back.
   *
   * A release that carries no recorded outcome is a decline — this worker does not run
   * this kind of node — unless the workflow has become terminal, in which case it is the
   * tail of a cancellation and says nothing about the worker's capabilities.
   */
  async release(lease: Lease): Promise<void> {
    const record = fromPortLease(lease);
    const events = await this.store.read(record.workflowId);
    if (hasOutcome(events, record.step, record.attempt)) return;

    if (!isWorkflowTerminal(events)) {
      const node = this.nodeFor(events, record.step);
      if (node) {
        const kinds = this.declined.get(lease.workerId) ?? new Set<NodeKind>();
        kinds.add(node.kind);
        this.declined.set(lease.workerId, kinds);
      }
    }

    await this.leases.release(record);
  }

  /** Records a successful attempt. */
  async completed(lease: Lease, output: unknown): Promise<void> {
    const ref = lease.ref;
    await this.appendOnce(ref.workflowId, (events) => {
      if (isWorkflowTerminal(events)) return [];
      // Keyed on the attempt, not on the worker. A reassigned attempt keeps its number,
      // so the second worker to finish the same attempt is reporting the same logical
      // outcome and the second report is absorbed rather than recorded twice.
      if (hasCompletion(events, ref.step, ref.attempt)) return [];
      return [
        {
          type: 'step.completed',
          workflowId: ref.workflowId,
          at: this.clock.now(),
          step: ref.step,
          attempt: ref.attempt,
          workerId: lease.workerId,
          output,
        },
      ];
    });
  }

  /**
   * Records a failed attempt through the retry engine.
   *
   * Routed through the engine rather than appended directly, because `step.failed` has to
   * carry whether a retry is coming and the backoff has to be computed from the same
   * policy the scheduler would have used. Two producers of that event with two answers
   * would make the fold disagree with itself.
   */
  async failed(lease: Lease, error: { readonly message: string; readonly errorClass: string }): Promise<void> {
    const ref = lease.ref;
    const events = await this.store.read(ref.workflowId);
    const node = this.nodeFor(events, ref.step);
    const deadlineAt = deadlineOf(events);

    await this.retries.recordFailure({
      workflowId: ref.workflowId,
      step: ref.step,
      attempt: ref.attempt,
      workerId: lease.workerId,
      error: error.message,
      errorClass: error.errorClass,
      // An absent policy means no retry rather than a default one: opting a step into
      // retries is the definition's job, and the fold reads the same absence the same way.
      policy: node?.retry ?? NO_RETRY,
      ...(deadlineAt !== undefined ? { deadlineAt } : {}),
    });
  }

  /**
   * Outputs of the steps that have succeeded.
   *
   * Derived from the projection rather than scanned off the raw completions, so a step
   * that completed and was then cancelled is not offered as an input. An unknown
   * definition yields nothing, which surfaces as the node failing for lack of input —
   * visibly wrong rather than quietly running on half a picture.
   */
  async completedOutputs(workflowId: WorkflowId): Promise<ReadonlyMap<StepName, CompletedStep>> {
    const events = await this.store.read(workflowId);
    const def = events.length > 0 ? this.defFor(events) : undefined;
    const outputs = new Map<StepName, CompletedStep>();
    if (!def) return outputs;

    for (const [name, step] of project(def, events).steps) {
      if (step.state === 'succeeded') outputs.set(name, { step: name, output: step.output });
    }
    return outputs;
  }

  /**
   * The assignment is the step's start, from the control plane's point of view.
   *
   * Recorded here rather than by the worker because the worker has no port for it, and
   * without it the fold leaves the step `ready` while a worker is demonstrably running it.
   */
  private async recordStarted(ref: StepRef, workerId: string): Promise<void> {
    await this.appendOnce(ref.workflowId, (events) =>
      isWorkflowTerminal(events)
        ? []
        : [
            {
              type: 'step.started',
              workflowId: ref.workflowId,
              at: this.clock.now(),
              step: ref.step,
              attempt: ref.attempt,
              workerId,
            },
          ],
    );
  }

  /** Appends a batch rebuilt from a fresh head, because a conflict invalidates the decision. */
  private async appendOnce(
    id: WorkflowId,
    build: (events: readonly Event[]) => readonly NewEvent[],
    attempts = 4,
  ): Promise<void> {
    for (let i = 0; i < attempts; i++) {
      const events = await this.store.read(id);
      const batch = build(events);
      if (batch.length === 0) return;
      try {
        await this.store.append(id, headOf(events), batch);
        return;
      } catch (err) {
        if (err instanceof ConcurrencyError) continue;
        throw err;
      }
    }
  }

  private defFor(events: readonly Event[]): WorkflowDef | undefined {
    const created = events.find((e) => e.type === 'workflow.created');
    if (!created || created.type !== 'workflow.created') return undefined;
    return this.defs(created.workflow, created.definitionVersion);
  }

  private nodeFor(events: readonly Event[], step: StepName): NodeDef | undefined {
    return this.defFor(events)?.nodes.find((n) => n.name === step);
  }
}

function toPortLease(lease: LeaseRecord): Lease {
  return {
    ref: { workflowId: lease.workflowId, step: lease.step, attempt: lease.attempt },
    workerId: lease.workerId,
    expiresAt: lease.expiresAt,
    token: String(lease.sequence),
  };
}

/**
 * Rebuilds the internal record from the worker's lease.
 *
 * The token is carried through but equality with the current grant is deliberately not
 * required: every renewal appends a new `step.leased`, so a holder's own token goes stale
 * the moment it heartbeats successfully. What rejects a stale holder is the identity check
 * on worker and attempt, which the lease manager does against the log.
 */
function fromPortLease(lease: Lease): LeaseRecord {
  return {
    workflowId: lease.ref.workflowId,
    step: lease.ref.step,
    attempt: lease.ref.attempt,
    workerId: lease.workerId,
    expiresAt: lease.expiresAt,
    sequence: Number(lease.token),
  };
}

/**
 * The step's own attempt budget, with the node's override applied.
 *
 * Passed as a whole `Timeouts` so the worker can tell the attempt budget from the lease
 * duration. They are different numbers with different consequences — one retries, the
 * other reassigns — and a worker given only "the timeout" cannot distinguish them.
 */
function timeoutsFor(def: WorkflowDef, node: NodeDef): Timeouts {
  return {
    ...def.timeouts,
    ...(node.attemptTimeoutMs !== undefined ? { attemptTimeoutMs: node.attemptTimeoutMs } : {}),
  };
}

/**
 * Binds a concrete payload for the step.
 *
 * A NodeDef carries no arguments — definitions are static, arguments depend on the run —
 * so something has to decide what a step is given, and doing it here keeps that binding
 * in the control plane where the event log can replay it. The convention: a step with no
 * dependencies receives the workflow input, a step with one receives that dependency's
 * output directly, and a fan-in receives an object keyed by dependency name.
 */
function bindInput(
  node: NodeDef,
  workflowInput: unknown,
  projection: { readonly steps: ReadonlyMap<StepName, { readonly output?: unknown }> },
): unknown {
  if (node.dependsOn.length === 0) return workflowInput;

  const single = node.dependsOn[0];
  if (node.dependsOn.length === 1 && single !== undefined) {
    return projection.steps.get(single)?.output;
  }

  const bound: Record<string, unknown> = {};
  for (const dep of node.dependsOn) {
    bound[dep] = projection.steps.get(dep)?.output;
  }
  return bound;
}

function deadlineOf(events: readonly Event[]): number | undefined {
  for (const e of events) {
    if (e.type === 'workflow.created') return e.deadlineAt;
  }
  return undefined;
}

/**
 * What a worker needs from the rest of the control plane.
 *
 * These are declared here, as narrow ports, rather than imported from the scheduler
 * or the state projection. The worker is the component most likely to be run out of
 * process — one worker pool per node kind, on different hosts — so it must not depend
 * on the scheduler's internals, only on the four things it genuinely needs: take an
 * assignment, keep it alive, read what earlier steps produced, report what happened.
 *
 * The scheduler is free to implement these over the event log; nothing here assumes it
 * does anything else.
 */

import type { NodeDef, StepName, StepRef, Timeouts, WorkflowId } from '../workflow/types.js';

/**
 * A claim on one step attempt, valid until `expiresAt`.
 *
 * The lease is the only thing that gives a worker the right to run a step, and it is
 * time-bounded rather than held until release, because a worker that dies silently
 * must not be able to block a step forever.
 */
export interface Lease {
  readonly ref: StepRef;
  readonly workerId: string;
  readonly expiresAt: number;
  /** Opaque token the control plane uses to reject renewals from a stale holder. */
  readonly token: string;
}

/**
 * One unit of work handed to a worker.
 *
 * `input` is resolved by the control plane rather than by the worker. NodeDef
 * deliberately carries no arguments — a definition is static and the arguments to a
 * step depend on the run — so somebody has to bind the workflow input and the prior
 * step outputs into a concrete payload. Doing it in the control plane keeps that
 * binding in the event log, where it can be replayed.
 */
export interface StepAssignment {
  readonly lease: Lease;
  readonly node: NodeDef;
  readonly input: unknown;
  readonly timeouts: Timeouts;
  readonly tenant?: string;
  /** Role used for the tool permission check. */
  readonly role?: string;
}

export interface LeaseManager {
  /**
   * Claims the next runnable step this worker is eligible for, or returns undefined
   * when there is nothing to do. Non-blocking by contract: polling cadence is the
   * worker's business, so a worker can back off without the control plane holding a
   * connection open per idle worker.
   */
  acquire(workerId: string): Promise<StepAssignment | undefined>;

  /**
   * Extends a lease. Returns false when the lease is gone — expired, revoked, or
   * reassigned. False is not an error: it is the signal that the step is now somebody
   * else's, and the only correct response is to stop.
   */
  heartbeat(lease: Lease): Promise<boolean>;

  /**
   * Gives the lease up without claiming an outcome. Used after cancellation and after
   * a declined assignment, so the step returns to the queue immediately instead of
   * waiting out the lease duration.
   */
  release(lease: Lease): Promise<void>;
}

/** One step that has reached `succeeded`, with whatever it produced. */
export interface CompletedStep {
  readonly step: StepName;
  readonly output: unknown;
}

/**
 * Read access to the parts of the projection a worker needs.
 *
 * Only completed steps are exposed. A worker that could see a running step's partial
 * output would be reading state that may never be committed, and a condition built on
 * that is unreplayable.
 */
export interface StateReader {
  completedOutputs(workflowId: WorkflowId): Promise<ReadonlyMap<StepName, CompletedStep>>;
}

/** How a step attempt ended, as the worker reports it. */
export interface OutcomeReporter {
  completed(lease: Lease, output: unknown): Promise<void>;
  failed(lease: Lease, error: { readonly message: string; readonly errorClass: string }): Promise<void>;
}

/**
 * Time comes from the scheduler's Clock rather than from a second one defined here.
 *
 * Heartbeat timing is the one worker behaviour that cannot be asserted without
 * controlling time, and the scheduler already ships a `FakeClock` whose timers fire in
 * deadline order. Two clock abstractions in one process would mean a test could advance
 * one and not the other, and the resulting "lease expired while the worker thought it
 * had heartbeated" would be an artefact of the test harness rather than a finding.
 *
 * Note that it offers `setTimeout` and no `setInterval`; the worker's heartbeat
 * reschedules itself, which is the behaviour it wants anyway — a renewal that overran
 * its interval should not have a second renewal queued behind it.
 */
export type { Clock } from '../scheduler/clock.js';
export { systemClock } from '../scheduler/clock.js';

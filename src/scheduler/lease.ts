/**
 * Leasing.
 *
 * A lease is permission to work on a step for a bounded time, not ownership of it. The
 * difference is the whole design: ownership would have to be revoked, and revoking
 * something held by a process that has stopped answering is not possible. A lease
 * simply lapses, and a lapsed lease makes the step eligible for someone else.
 *
 * The consequence is that a step can run twice — the original worker may be alive and
 * merely partitioned — so every step handler must be idempotent. That is a real cost,
 * paid deliberately, because the alternative is a workflow that stalls forever the first
 * time a worker is killed.
 *
 * Mutual exclusion comes from the event store's optimistic concurrency and nothing else.
 * An in-process mutex would work right up until the day a second scheduler process is
 * started, and then it would fail silently and intermittently, which is worse than
 * failing immediately.
 */

import { ConcurrencyError, type EventStore, type NewEvent } from '../events/types.js';
import type { StepName, StepRef, WorkflowId } from '../workflow/types.js';
import type { Clock } from './clock.js';
import {
  currentLeaseOf,
  headOf,
  isWorkflowTerminal,
  liveLeaseOf,
  overdueLeases,
  type LeaseRecord,
} from './history.js';

export type { LeaseRecord } from './history.js';

/** The lease is no longer this worker's: someone else took it, or the attempt moved on. */
export class LeaseLostError extends Error {
  constructor(
    readonly workflowId: WorkflowId,
    readonly step: StepName,
    readonly workerId: string,
    detail: string,
  ) {
    super(`lease on ${workflowId}/${step} is no longer held by ${workerId}: ${detail}`);
    this.name = 'LeaseLostError';
  }
}

/**
 * The lease lapsed before the heartbeat arrived.
 *
 * Separate from `LeaseLostError` and never silently repaired. Extending a lapsed lease
 * would be the easy thing to do and the wrong one: by the time it lapsed the step was
 * already eligible for reassignment, so quietly extending it can produce two live
 * leases, which is the one state the whole mechanism exists to prevent.
 */
export class LeaseExpiredError extends Error {
  constructor(
    readonly workflowId: WorkflowId,
    readonly step: StepName,
    readonly workerId: string,
    readonly expiredAt: number,
    now: number,
  ) {
    super(
      `lease on ${workflowId}/${step} held by ${workerId} expired at ${expiredAt} (now ${now})`,
    );
    this.name = 'LeaseExpiredError';
  }
}

export interface LeaseManagerOptions {
  readonly store: EventStore;
  readonly clock: Clock;
  readonly leaseDurationMs: number;
}

export class LeaseManager {
  private readonly store: EventStore;
  private readonly clock: Clock;
  readonly leaseDurationMs: number;

  constructor(opts: LeaseManagerOptions) {
    this.store = opts.store;
    this.clock = opts.clock;
    this.leaseDurationMs = opts.leaseDurationMs;
  }

  /**
   * Attempts to take the lease on a step.
   *
   * Returns `null` rather than throwing when the step is already leased, because losing
   * a race is the expected outcome for every scheduler but one and an exception would
   * make the normal path look like the error path.
   */
  async acquire(
    ref: StepRef,
    workerId: string,
    durationMs = this.leaseDurationMs,
  ): Promise<LeaseRecord | null> {
    const events = await this.store.read(ref.workflowId);
    if (isWorkflowTerminal(events)) return null;

    const now = this.clock.now();
    if (liveLeaseOf(events, ref.step, now)) return null;

    // The expected sequence is the head of the history the decision was made against.
    // If anything at all was appended in between — another scheduler's lease, a
    // cancellation, a completion — the append is rejected and this caller loses.
    const expected = headOf(events);
    const expiresAt = now + durationMs;

    const leased: NewEvent = {
      type: 'step.leased',
      workflowId: ref.workflowId,
      at: now,
      step: ref.step,
      attempt: ref.attempt,
      workerId,
      leaseExpiresAt: expiresAt,
    };

    try {
      const [written] = await this.store.append(ref.workflowId, expected, [leased]);
      if (!written) return null;
      return {
        workflowId: ref.workflowId,
        step: ref.step,
        attempt: ref.attempt,
        workerId,
        expiresAt,
        sequence: written.sequence,
      };
    } catch (err) {
      if (err instanceof ConcurrencyError) return null;
      throw err;
    }
  }

  /**
   * Extends a held lease.
   *
   * Fails loudly in three cases: the workflow has finished, the lease belongs to someone
   * else or to a superseded attempt, or it has already lapsed. A heartbeat is an
   * assertion that the worker is still the rightful holder, so a heartbeat that cannot
   * be justified has to be refused — the caller's correct response is to stop working
   * and drop whatever it was going to write.
   */
  async heartbeat(lease: LeaseRecord, durationMs = this.leaseDurationMs): Promise<LeaseRecord> {
    const events = await this.store.read(lease.workflowId);
    const now = this.clock.now();

    if (isWorkflowTerminal(events)) {
      throw new LeaseLostError(lease.workflowId, lease.step, lease.workerId, 'workflow is terminal');
    }

    const current = currentLeaseOf(events, lease.step);
    if (!current) {
      throw new LeaseLostError(
        lease.workflowId,
        lease.step,
        lease.workerId,
        'no lease is recorded for this attempt',
      );
    }
    if (current.workerId !== lease.workerId || current.attempt !== lease.attempt) {
      throw new LeaseLostError(
        lease.workflowId,
        lease.step,
        lease.workerId,
        `held by ${current.workerId} at attempt ${current.attempt}`,
      );
    }
    if (current.expiresAt <= now) {
      throw new LeaseExpiredError(
        lease.workflowId,
        lease.step,
        lease.workerId,
        current.expiresAt,
        now,
      );
    }

    const expiresAt = now + durationMs;
    const expected = headOf(events);
    try {
      // A renewal is `step.lease_extended`, not a second `step.leased`.
      //
      // Emitting `step.leased` here was a live bug: the projection deduplicates on
      // (type, step, attempt, worker), so every renewal after the first was absorbed
      // as a duplicate and `StepProjection.leaseExpiresAt` kept the *original*
      // expiry. `readySteps` would then offer a step this worker is actively
      // heartbeating on. The scheduler itself was unaffected because it checks
      // `liveLeaseOf` over raw events, but anything reading the projection — the
      // dashboard, the trace view, any second scheduler — saw a live lease as lapsed.
      const [written] = await this.store.append(lease.workflowId, expected, [
        {
          type: 'step.lease_extended',
          workflowId: lease.workflowId,
          at: now,
          step: lease.step,
          attempt: lease.attempt,
          workerId: lease.workerId,
          leaseExpiresAt: expiresAt,
        },
      ]);
      if (!written) {
        throw new LeaseLostError(
          lease.workflowId,
          lease.step,
          lease.workerId,
          'store accepted no events',
        );
      }
      return { ...lease, expiresAt, sequence: written.sequence };
    } catch (err) {
      if (err instanceof ConcurrencyError) {
        // Something changed this step's history while the heartbeat was in flight. The
        // honest answer is that this worker can no longer prove it holds the lease.
        throw new LeaseLostError(
          lease.workflowId,
          lease.step,
          lease.workerId,
          'history advanced during heartbeat',
        );
      }
      throw err;
    }
  }

  /**
   * Records every lapsed lease and, by default, returns the step to the ready set.
   *
   * `step.lease_expired` and never `step.failed`: the step has not failed, it has lost
   * its worker. Recording a failure here would let the scheduler report a workflow
   * failed while the original attempt is still running somewhere, and would consume a
   * retry attempt for something that was never a fault of the work.
   */
  async expireOverdue(workflowId: WorkflowId): Promise<readonly LeaseRecord[]> {
    const events = await this.store.read(workflowId);
    if (isWorkflowTerminal(events)) return [];

    const now = this.clock.now();
    const overdue = overdueLeases(events, now);
    if (overdue.length === 0) return [];

    // One event per lapsed lease and nothing else. `running -> ready` is a legal
    // transition precisely because this is what a lease expiry is, so the expiry notice
    // hands the step back on its own — no accompanying `step.scheduled` is needed, and
    // the attempt number is left alone. The attempt counter tracks failures of the work,
    // not of the worker holding it, so a reassignment must not spend a retry attempt.
    const batch: NewEvent[] = overdue.map((lease) => ({
      type: 'step.lease_expired',
      workflowId,
      at: now,
      step: lease.step,
      attempt: lease.attempt,
      workerId: lease.workerId,
    }));

    try {
      await this.store.append(workflowId, headOf(events), batch);
      return overdue;
    } catch (err) {
      // Another scheduler expired them first, which is the correct outcome either way.
      if (err instanceof ConcurrencyError) return [];
      throw err;
    }
  }

  /**
   * Gives a lease back without claiming an outcome.
   *
   * Recorded as `step.lease_expired`, which is the same transition a lapsed lease makes:
   * this worker no longer holds the step, the step is eligible again, the attempt number
   * is unchanged and no retry attempt is spent. The alternatives are both worse — a
   * `step.failed` would be a lie about the work, and recording nothing would strand the
   * step until the lease timed out, which is exactly the latency a voluntary release
   * exists to avoid.
   *
   * Refused when the log says somebody else now holds the step: releasing a lease that
   * has already been reassigned would revoke the new holder's claim and hand the same
   * step to a third worker.
   */
  async release(lease: LeaseRecord): Promise<boolean> {
    const events = await this.store.read(lease.workflowId);
    if (isWorkflowTerminal(events)) return false;

    const current = currentLeaseOf(events, lease.step);
    if (!current) return false;
    if (current.workerId !== lease.workerId || current.attempt !== lease.attempt) return false;

    try {
      await this.store.append(lease.workflowId, headOf(events), [
        {
          type: 'step.lease_expired',
          workflowId: lease.workflowId,
          at: this.clock.now(),
          step: lease.step,
          attempt: lease.attempt,
          workerId: lease.workerId,
        },
      ]);
      return true;
    } catch (err) {
      if (err instanceof ConcurrencyError) return false;
      throw err;
    }
  }

  /** The live lease on a step, if any, as of the clock's current time. */
  async live(workflowId: WorkflowId, step: StepName): Promise<LeaseRecord | undefined> {
    const events = await this.store.read(workflowId);
    return liveLeaseOf(events, step, this.clock.now());
  }
}

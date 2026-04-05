/**
 * Narrow folds over a raw event history.
 *
 * These exist alongside the full `project()` fold rather than on top of it, because the
 * scheduler needs two facts — "is the log still where I think it is" and "does anyone
 * hold this step's lease" — at the moment it decides to append. A projection is a
 * summary; leasing needs the sequence number the summary was computed at, and it needs
 * it to be the sequence the append is checked against. Reading the raw events and
 * claiming their last sequence is what makes the optimistic check meaningful.
 */

import type { Event, Sequence } from '../events/types.js';
import type { StepName, WorkflowId } from '../workflow/types.js';

/** The highest sequence in a history, or 0 for an empty one. */
export function headOf(events: readonly Event[]): Sequence {
  const last = events[events.length - 1];
  return last ? last.sequence : 0;
}

/** True once the workflow has reached a state nothing follows. */
export function isWorkflowTerminal(events: readonly Event[]): boolean {
  return events.some(
    (e) =>
      e.type === 'workflow.completed' ||
      e.type === 'workflow.failed' ||
      e.type === 'workflow.cancelled',
  );
}

/** A lease as recorded in the log. */
export interface LeaseRecord {
  readonly workflowId: WorkflowId;
  readonly step: StepName;
  readonly attempt: number;
  readonly workerId: string;
  readonly expiresAt: number;
  /** Sequence of the `step.leased` event that granted or last extended this lease. */
  readonly sequence: Sequence;
}

/**
 * The lease currently recorded for a step, expired or not.
 *
 * Returning an expired lease rather than `undefined` is deliberate: the caller has to
 * be able to tell "nobody ever leased this" from "the lease lapsed", because only the
 * second case needs a `step.lease_expired` recorded against a named worker.
 */
export function currentLeaseOf(
  events: readonly Event[],
  step: StepName,
): LeaseRecord | undefined {
  let lease: LeaseRecord | undefined;

  for (const e of events) {
    if (isWorkflowScoped(e)) {
      // A terminal workflow releases every lease it held; nothing is owed to a worker
      // whose workflow has stopped mattering.
      lease = undefined;
      continue;
    }
    if (!('step' in e) || e.step !== step) continue;

    switch (e.type) {
      case 'step.leased':
        lease = {
          workflowId: e.workflowId,
          step: e.step,
          attempt: e.attempt,
          workerId: e.workerId,
          expiresAt: e.leaseExpiresAt,
          sequence: e.sequence,
        };
        break;
      case 'step.completed':
      case 'step.failed':
      case 'step.lease_expired':
      case 'step.retry_scheduled':
      case 'step.scheduled':
        // Each of these settles the attempt the lease was taken for. A later attempt
        // has to take its own lease, which is what stops a stale heartbeat from
        // extending a claim over work that has already moved on.
        lease = undefined;
        break;
      default:
        break;
    }
  }

  return lease;
}

/** The lease on a step if it is still live at `now`. */
export function liveLeaseOf(
  events: readonly Event[],
  step: StepName,
  now: number,
): LeaseRecord | undefined {
  const lease = currentLeaseOf(events, step);
  return lease && lease.expiresAt > now ? lease : undefined;
}

/** Every step holding a lease that has lapsed by `now`. */
export function overdueLeases(events: readonly Event[], now: number): readonly LeaseRecord[] {
  const steps = new Set<StepName>();
  for (const e of events) {
    if (e.type === 'step.leased') steps.add(e.step);
  }

  const overdue: LeaseRecord[] = [];
  for (const step of steps) {
    const lease = currentLeaseOf(events, step);
    if (lease && lease.expiresAt <= now) overdue.push(lease);
  }
  return overdue;
}

/** Whether a given attempt of a step has already recorded a completion. */
export function hasCompletion(
  events: readonly Event[],
  step: StepName,
  attempt: number,
): boolean {
  return events.some(
    (e) => e.type === 'step.completed' && e.step === step && e.attempt === attempt,
  );
}

/**
 * Whether an attempt has reported any outcome at all.
 *
 * The question a release has to ask: a worker that hands a lease back having recorded
 * neither a completion nor a failure is claiming no outcome, which is a different thing
 * from finishing.
 */
export function hasOutcome(events: readonly Event[], step: StepName, attempt: number): boolean {
  return events.some(
    (e) =>
      (e.type === 'step.completed' || e.type === 'step.failed') &&
      e.step === step &&
      e.attempt === attempt,
  );
}

/** Whether an approval has already been requested for a step, so it is asked once. */
export function hasApprovalRequest(events: readonly Event[], step: StepName): boolean {
  return events.some((e) => e.type === 'approval.requested' && e.step === step);
}

function isWorkflowScoped(e: Event): boolean {
  return (
    e.type === 'workflow.completed' ||
    e.type === 'workflow.failed' ||
    e.type === 'workflow.cancelled'
  );
}

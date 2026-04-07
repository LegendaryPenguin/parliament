/**
 * The retry engine.
 *
 * Three independent reasons not to retry, each of which has to be checked separately
 * because they mean different things:
 *
 *  - the error class is not in the policy's allowlist — the work will fail the same way
 *    forever, so retrying converts one malformed request into an infinite loop;
 *  - the attempt budget is spent — the failure is real and should be reported;
 *  - the next attempt would land after the workflow deadline — the answer would arrive
 *    too late to be worth anything, so the attempt is waste even if it would succeed.
 *
 * The third is the one that gets collapsed into the others in most systems, and it is
 * the reason `Timeouts` keeps four distinct fields instead of one.
 */

import { ConcurrencyError, type EventStore, type NewEvent } from '../events/types.js';
import { backoffFor, type RetryPolicy, type StepName, type WorkflowId } from '../workflow/types.js';
import type { Clock } from './clock.js';
import { headOf, isWorkflowTerminal } from './history.js';

export type GiveUpReason =
  | 'not_retryable'
  | 'attempts_exhausted'
  | 'past_deadline'
  /** The workflow had already stopped, so there is nothing left to retry into. */
  | 'workflow_terminal';

export type RetryDecision =
  | {
      readonly kind: 'retry';
      /** The attempt that will run next, not the one that just failed. */
      readonly attempt: number;
      readonly notBefore: number;
      readonly backoffMs: number;
    }
  | { readonly kind: 'give_up'; readonly reason: GiveUpReason };

/**
 * Classes that are never retried, whatever a policy says.
 *
 * `errorClass` is free text on `step.failed` while the tool executor's own class list is
 * closed, so workers legitimately emit classes a policy author never saw. These three can
 * only ever be wrong to retry:
 *
 *  - `definition_error` — a definition that is malformed now is malformed on the next
 *    attempt too, so retrying it is a loop with a known end state;
 *  - `cancelled` — not a failure of the work, and the workflow has stopped wanting it;
 *  - `lease_lost` — the step belongs to another worker, who will report its own outcome;
 *    retrying here would run it a third time.
 *
 * Enforced here rather than by asking every policy author to remember, because the cost
 * of forgetting is an infinite retry loop against something that cannot succeed.
 */
const NEVER_RETRYABLE: ReadonlySet<string> = new Set(['definition_error', 'cancelled', 'lease_lost']);

/**
 * The policy for a node that declares none: one attempt, nothing retryable.
 *
 * Shared rather than redefined per caller, because `project()` also reads an absent policy
 * as "no retry". Two components that disagreed about what absence means would produce a
 * `step.retry_scheduled` the fold then rejects as a transition out of a terminal state.
 */
export const NO_RETRY: RetryPolicy = {
  maxAttempts: 1,
  initialBackoffMs: 0,
  maxBackoffMs: 0,
  jitter: 0,
  retryableErrors: [],
};

/** Whether an error class is one the policy is willing to retry. */
export function isRetryable(policy: RetryPolicy, errorClass: string): boolean {
  if (NEVER_RETRYABLE.has(errorClass)) return false;
  return policy.retryableErrors.includes(errorClass);
}

export interface RetryEngineOptions {
  readonly store: EventStore;
  readonly clock: Clock;
  /** Injectable so backoff is assertable; production leaves it as `Math.random`. */
  readonly random?: () => number;
}

export interface FailureReport {
  readonly workflowId: WorkflowId;
  readonly step: StepName;
  /** The attempt that failed. */
  readonly attempt: number;
  readonly workerId?: string;
  readonly error: string;
  readonly errorClass: string;
  readonly policy: RetryPolicy;
  /** Absolute workflow deadline, if the execution has one. */
  readonly deadlineAt?: number;
}

export class RetryEngine {
  private readonly store: EventStore;
  private readonly clock: Clock;
  private readonly random: () => number;

  constructor(opts: RetryEngineOptions) {
    this.store = opts.store;
    this.clock = opts.clock;
    this.random = opts.random ?? Math.random;
  }

  /**
   * Classifies a failure without writing anything. Pure, so the policy can be unit
   * tested and so the scheduler can ask the question before it decides what to append.
   */
  decide(report: FailureReport, now = this.clock.now()): RetryDecision {
    const { policy, attempt, errorClass, deadlineAt } = report;

    if (!isRetryable(policy, errorClass)) {
      return { kind: 'give_up', reason: 'not_retryable' };
    }
    if (attempt >= policy.maxAttempts) {
      return { kind: 'give_up', reason: 'attempts_exhausted' };
    }

    const next = attempt + 1;
    const backoffMs = backoffFor(policy, next, this.random);
    const notBefore = now + backoffMs;

    // Checked against the moment the retry would *start*, not the moment it was
    // scheduled. A retry whose backoff carries it past the deadline is work that cannot
    // be delivered on time, and starting it only wastes a worker and muddies the
    // eventual failure report.
    if (deadlineAt !== undefined && notBefore > deadlineAt) {
      return { kind: 'give_up', reason: 'past_deadline' };
    }

    return { kind: 'retry', attempt: next, notBefore, backoffMs };
  }

  /**
   * Records a step failure and whatever follows from it, in one atomic append.
   *
   * Atomic because the intermediate states are uninterpretable: a `step.failed` with no
   * retry and no workflow failure reads as a workflow that is still running but has
   * nothing left to run, and a fold cannot tell that apart from a scheduler that is
   * simply slow.
   */
  async recordFailure(report: FailureReport): Promise<RetryDecision> {
    const events = await this.store.read(report.workflowId);
    if (isWorkflowTerminal(events)) {
      // A failure arriving after the workflow stopped changes nothing. In particular a
      // cancelled workflow must not be dragged into `failed`.
      return { kind: 'give_up', reason: 'workflow_terminal' };
    }

    const now = this.clock.now();
    const decision = this.decide(report, now);

    const batch: NewEvent[] = [
      {
        type: 'step.failed',
        workflowId: report.workflowId,
        at: now,
        step: report.step,
        attempt: report.attempt,
        ...(report.workerId !== undefined ? { workerId: report.workerId } : {}),
        error: report.error,
        errorClass: report.errorClass,
        // Written here rather than re-derived by the fold. `failed` is terminal, so a
        // projection that had to ask the definition whether a retry was coming would be
        // a function of the *current* definition rather than of the history alone, and an
        // old history replayed against an edited retry policy would fold differently.
        // This engine knows the answer at the moment it decides, so it records it.
        willRetry: decision.kind === 'retry',
      },
    ];

    if (decision.kind === 'retry') {
      batch.push({
        type: 'step.retry_scheduled',
        workflowId: report.workflowId,
        at: now,
        step: report.step,
        attempt: decision.attempt,
        notBefore: decision.notBefore,
        backoffMs: decision.backoffMs,
      });
      batch.push({
        type: 'step.scheduled',
        workflowId: report.workflowId,
        at: now,
        step: report.step,
        attempt: decision.attempt,
        notBefore: decision.notBefore,
      });
    } else {
      batch.push({
        type: 'workflow.failed',
        workflowId: report.workflowId,
        at: now,
        step: report.step,
        error: `${report.step}: ${report.error}`,
        errorClass: report.errorClass,
      });
    }

    try {
      await this.store.append(report.workflowId, headOf(events), batch);
    } catch (err) {
      if (err instanceof ConcurrencyError) {
        // The history moved underneath us — most often the lease was expired by another
        // scheduler, meaning this worker no longer speaks for the step. Report the
        // decision but leave the log to whoever won.
        return decision;
      }
      throw err;
    }

    return decision;
  }
}

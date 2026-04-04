/**
 * The event history.
 *
 * This is the load-bearing idea of the whole project: **the event log is the state,
 * and everything else is a projection of it.** A workflow's current status is not
 * stored and then mutated; it is computed by folding the events. That is what makes
 * three otherwise-hard things fall out:
 *
 *  - **Crash recovery.** A control plane that restarts replays the log and arrives at
 *    exactly the state it left, with no reconciliation step and nothing to get wrong.
 *  - **Debugging.** "Why did this workflow end up here" is answerable by reading the
 *    events, rather than by inferring backwards from a mutated row.
 *  - **Duplicate tolerance.** Appending an event that has already been appended is
 *    detectable, because events carry identity.
 *
 * The cost is that the log is append-only and its encoding is a compatibility
 * surface forever. An event written today must still be readable by every future
 * version, which is why every event carries a version and why nothing is ever
 * rewritten in place.
 */

import type {
  RunState,
  StepName,
  WorkflowDef,
  WorkflowId,
  WorkflowName,
} from '../workflow/types.js';

/** Monotonically increasing position within one workflow's history. 1-based. */
export type Sequence = number;

/** Schema version for the envelope. Bumping it changes how events are read. */
export const EVENT_VERSION = 1 as const;

export type EventType =
  | 'workflow.created'
  | 'workflow.completed'
  | 'workflow.failed'
  | 'workflow.cancelled'
  | 'step.scheduled'
  | 'step.leased'
  | 'step.started'
  | 'step.completed'
  | 'step.failed'
  | 'step.retry_scheduled'
  | 'step.lease_expired'
  | 'tool.requested'
  | 'tool.succeeded'
  | 'tool.failed'
  | 'approval.requested'
  | 'approval.granted'
  | 'approval.denied';

/**
 * The common envelope.
 *
 * `sequence` is assigned by the store, not the caller, and is what makes the log a
 * log rather than a bag. `causedBy` points at the event that led to this one, which
 * is what lets a trace be reconstructed without timestamp arithmetic — timestamps
 * from different processes cannot be ordered reliably, sequence numbers can.
 */
export interface EventEnvelope {
  readonly version: typeof EVENT_VERSION;
  readonly sequence: Sequence;
  readonly workflowId: WorkflowId;
  readonly type: EventType;
  readonly at: number;
  readonly causedBy?: Sequence;
}

export interface WorkflowCreated extends EventEnvelope {
  readonly type: 'workflow.created';
  readonly workflow: WorkflowName;
  readonly definitionVersion: number;
  readonly input: unknown;
  readonly deadlineAt?: number;
  readonly tenant?: string;

  /**
   * Scheduling priority, recorded here because the log is the only durable thing.
   *
   * Without it, a control plane that restarts recovers every workflow at the default
   * priority — so a crash silently demotes urgent work, and the symptom is a workflow
   * that was being preferred suddenly waiting behind batch jobs with nothing in any
   * log to explain it.
   */
  readonly priority?: number;
}

export interface WorkflowCompleted extends EventEnvelope {
  readonly type: 'workflow.completed';
  readonly output: unknown;
}

export interface WorkflowFailed extends EventEnvelope {
  readonly type: 'workflow.failed';
  readonly step?: StepName;
  readonly error: string;
  readonly errorClass: string;
}

export interface WorkflowCancelled extends EventEnvelope {
  readonly type: 'workflow.cancelled';
  readonly reason: string;
  readonly requestedBy?: string;
}

export interface StepScheduled extends EventEnvelope {
  readonly type: 'step.scheduled';
  readonly step: StepName;
  readonly attempt: number;
  readonly notBefore?: number;
}

export interface StepLeased extends EventEnvelope {
  readonly type: 'step.leased';
  readonly step: StepName;
  readonly attempt: number;
  readonly workerId: string;
  readonly leaseExpiresAt: number;
}

export interface StepStarted extends EventEnvelope {
  readonly type: 'step.started';
  readonly step: StepName;
  readonly attempt: number;
  readonly workerId: string;
}

export interface StepCompleted extends EventEnvelope {
  readonly type: 'step.completed';
  readonly step: StepName;
  readonly attempt: number;
  readonly workerId: string;
  readonly output: unknown;
}

export interface StepFailed extends EventEnvelope {
  readonly type: 'step.failed';
  readonly step: StepName;
  readonly attempt: number;
  readonly workerId?: string;
  readonly error: string;
  readonly errorClass: string;

  /**
   * Whether the producer decided to retry this failure.
   *
   * Recorded in the event rather than re-derived by the fold, and this matters more
   * than it looks. `failed` is a terminal state, so a fold that must distinguish
   * "failed for good" from "failed, retry coming" has to consult the definition's
   * RetryPolicy — which makes the projection a function of the *current* definition
   * rather than of the history alone. Replaying an old history against a changed
   * retry policy would then produce different states, and the determinism the whole
   * event-sourced design rests on would quietly not hold.
   *
   * The scheduler already knows the answer at the moment it writes this event, so it
   * writes it down. The fold stays a pure function of the log.
   */
  readonly willRetry: boolean;
}

export interface StepRetryScheduled extends EventEnvelope {
  readonly type: 'step.retry_scheduled';
  readonly step: StepName;
  /** The attempt number that will run next, not the one that just failed. */
  readonly attempt: number;
  readonly notBefore: number;
  readonly backoffMs: number;
}

/**
 * The lease expired without the worker finishing.
 *
 * Recorded as its own event rather than as a `step.failed`, because it means
 * something genuinely different: the step may still be running somewhere. Treating
 * it as a failure would licence the scheduler to report the workflow failed while
 * work continues in the background.
 */
export interface StepLeaseExpired extends EventEnvelope {
  readonly type: 'step.lease_expired';
  readonly step: StepName;
  readonly attempt: number;
  readonly workerId: string;
}

export interface ToolRequested extends EventEnvelope {
  readonly type: 'tool.requested';
  readonly step: StepName;
  readonly attempt: number;
  readonly tool: string;
  readonly args: unknown;
  /** Derived from (workflowId, step, attempt) — see tools/idempotency.ts. */
  readonly idempotencyKey: string;
}

export interface ToolSucceeded extends EventEnvelope {
  readonly type: 'tool.succeeded';
  readonly step: StepName;
  readonly attempt: number;
  readonly tool: string;
  readonly result: unknown;
  readonly durationMs: number;
  /** True when the executor returned a memoized result for a repeated key. */
  readonly deduplicated: boolean;
}

export interface ToolFailed extends EventEnvelope {
  readonly type: 'tool.failed';
  readonly step: StepName;
  readonly attempt: number;
  readonly tool: string;
  readonly error: string;
  readonly errorClass: string;
}

/**
 * Note on workflow state during an approval: the *step* enters `waiting`, and the
 * workflow does not.
 *
 * An earlier version of this design said the workflow should also wait. That is wrong
 * whenever the graph has parallel branches: one pending approval would stall work that
 * has nothing to do with it. The workflow is blocked only if every remaining path runs
 * through the gate, and that is a property of the graph which `readySteps` already
 * computes — so there is nothing for a workflow-level `waiting` to add except a second
 * place for the two to disagree.
 */
export interface ApprovalRequested extends EventEnvelope {
  readonly type: 'approval.requested';
  readonly step: StepName;
  readonly approvers: readonly string[];
  readonly prompt: string;
  /** What the approver is actually authorizing, so the decision is informed. */
  readonly payload: unknown;
}

export interface ApprovalGranted extends EventEnvelope {
  readonly type: 'approval.granted';
  readonly step: StepName;
  readonly approver: string;
  readonly note?: string;
}

export interface ApprovalDenied extends EventEnvelope {
  readonly type: 'approval.denied';
  readonly step: StepName;
  readonly approver: string;
  readonly reason: string;
}

export type Event =
  | WorkflowCreated
  | WorkflowCompleted
  | WorkflowFailed
  | WorkflowCancelled
  | StepScheduled
  | StepLeased
  | StepStarted
  | StepCompleted
  | StepFailed
  | StepRetryScheduled
  | StepLeaseExpired
  | ToolRequested
  | ToolSucceeded
  | ToolFailed
  | ApprovalRequested
  | ApprovalGranted
  | ApprovalDenied;

/** An event as submitted, before the store assigns its sequence. */
export type NewEvent = Omit<Event, 'sequence' | 'version'> & {
  readonly version?: typeof EVENT_VERSION;
};

/**
 * Thrown when an append would violate the expected sequence.
 *
 * Optimistic concurrency on the log is what stops two schedulers from both deciding
 * a step is ready and both leasing it. The loser retries against the new history and
 * sees the step is already taken.
 */
export class ConcurrencyError extends Error {
  constructor(
    readonly workflowId: WorkflowId,
    readonly expected: Sequence,
    readonly actual: Sequence,
  ) {
    super(
      `concurrent append to ${workflowId}: expected sequence ${expected}, log is at ${actual}`,
    );
    this.name = 'ConcurrencyError';
  }
}

/**
 * The durable log.
 *
 * `append` takes the sequence the caller believes the log is at. Passing the wrong
 * one is a conflict rather than an overwrite, which is the whole mechanism behind
 * single-leasing without a distributed lock.
 */
export interface EventStore {
  /**
   * Appends events atomically. Either all are written or none are, because a
   * half-written transition is a state no fold can interpret.
   */
  append(workflowId: WorkflowId, expectedSequence: Sequence, events: readonly NewEvent[]): Promise<readonly Event[]>;

  /** Reads a workflow's history from `after` (exclusive) onward. */
  read(workflowId: WorkflowId, after?: Sequence): Promise<readonly Event[]>;

  /** The highest assigned sequence, or 0 for a workflow with no history. */
  head(workflowId: WorkflowId): Promise<Sequence>;

  /** Every workflow id the store knows, for recovery on startup. */
  list(): Promise<readonly WorkflowId[]>;

  /** Workflows not in a terminal state, so recovery can skip finished work. */
  listActive(): Promise<readonly WorkflowId[]>;
}

/** A step's position, as folded from the history. */
export interface StepProjection {
  readonly step: StepName;
  readonly state: RunState;
  readonly attempt: number;
  readonly workerId?: string;
  readonly leaseExpiresAt?: number;
  readonly notBefore?: number;
  readonly output?: unknown;
  readonly error?: string;
  readonly errorClass?: string;
  readonly awaitingApproval: boolean;
}

/** A whole workflow's position, as folded from the history. */
export interface WorkflowProjection {
  readonly workflowId: WorkflowId;
  readonly workflow: WorkflowName;
  readonly definitionVersion: number;
  readonly state: RunState;
  readonly input: unknown;
  readonly output?: unknown;
  readonly error?: string;
  readonly deadlineAt?: number;
  readonly tenant?: string;
  readonly steps: ReadonlyMap<StepName, StepProjection>;
  readonly sequence: Sequence;
  readonly createdAt: number;
  readonly updatedAt: number;
}

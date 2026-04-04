/**
 * The fold: events in, state out.
 *
 * Everything else in the control plane trusts this function, so it is constrained
 * harder than anything else in the codebase:
 *
 *  - **Pure.** No I/O, no clock, no randomness. `project` of the same events is the
 *    same projection on every machine, forever. Anything that needs the current time
 *    takes it as an argument (see `state/ready.ts`), because a fold that reads the
 *    clock produces a different answer on replay than it did live, and then replay
 *    stops being evidence of anything.
 *  - **Strict.** An event implying an illegal transition is a bug in whatever produced
 *    it, and is thrown rather than absorbed. The invariant worth protecting above all
 *    others is that a terminal workflow never starts running again: if a duplicate
 *    completion delivered after a cancellation could restart downstream work, the log
 *    would no longer explain the system's behaviour.
 *  - **Idempotent.** A redelivered event is folded once. Duplicate delivery is normal
 *    in a leased system, so "exactly once" has to be a property of the fold rather
 *    than a promise the transport cannot keep.
 */

import type { Event, EventType, StepProjection, WorkflowProjection, Sequence } from '../events/types.js';
import { canTransition, isTerminal } from '../workflow/types.js';
import type { NodeDef, RunState, StepName, WorkflowDef, WorkflowId } from '../workflow/types.js';

/** The history cannot be folded at all: it is not a history of this workflow. */
export class ProjectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ProjectionError';
  }
}

/**
 * An event asked for a transition the state machine forbids.
 *
 * The message names the offending transition, because "invalid state" tells a reader
 * nothing they can act on, whereas `step "publish" cannot go cancelled -> running`
 * identifies both the producer bug and the event that carried it.
 */
export class IllegalTransitionError extends Error {
  constructor(
    readonly workflowId: WorkflowId,
    readonly sequence: Sequence,
    readonly eventType: EventType,
    readonly subject: string,
    readonly from: RunState,
    readonly to: RunState,
  ) {
    super(
      `illegal transition in workflow ${workflowId} at sequence ${sequence} ` +
        `(${eventType}): ${subject} cannot go ${from} -> ${to}`,
    );
    this.name = 'IllegalTransitionError';
  }
}

interface MutableStep {
  state: RunState;
  attempt: number;
  workerId?: string | undefined;
  leaseExpiresAt?: number | undefined;
  notBefore?: number | undefined;
  output?: unknown;
  /** Distinguishes "no output yet" from "the step produced `undefined`". */
  outputSet: boolean;
  error?: string | undefined;
  errorClass?: string | undefined;
  awaitingApproval: boolean;
}

/**
 * Events whose identity includes the worker.
 *
 * A second `step.leased` for the same attempt is a genuine reassignment after a lease
 * expiry, not a duplicate, so the lease cycle is keyed by worker. Outcome events are
 * deliberately *not*: two workers reporting the same attempt completed is one logical
 * outcome reported twice, and the second report must be absorbed rather than throw.
 */
const WORKER_SCOPED: ReadonlySet<EventType> = new Set<EventType>([
  'step.leased',
  'step.started',
  'step.lease_expired',
]);

function dedupeKey(event: Event): string {
  const step = 'step' in event && event.step !== undefined ? event.step : '-';
  const attempt = 'attempt' in event ? String(event.attempt) : '-';
  const worker =
    WORKER_SCOPED.has(event.type) && 'workerId' in event ? `|${String(event.workerId)}` : '';
  // A renewal's identity includes the expiry it set. Keyed only by worker and attempt,
  // every heartbeat after the first would be absorbed as a duplicate and the
  // projection would keep reporting the original expiry for a long-renewed lease.
  // Two renewals to the same expiry really are one fact, and stay idempotent.
  const renewal =
    event.type === 'step.lease_extended' ? `|${String(event.workerId)}@${event.leaseExpiresAt}` : '';
  return `${event.type}|${step}|${attempt}${worker}${renewal}`;
}

export function project(def: WorkflowDef, events: readonly Event[]): WorkflowProjection {
  const first = events[0];
  if (first === undefined) {
    throw new ProjectionError('cannot project an empty history: no workflow.created event');
  }
  if (first.type !== 'workflow.created') {
    throw new ProjectionError(
      `history begins with ${first.type}; the first event must be workflow.created`,
    );
  }

  const workflowId = first.workflowId;
  const nodes: Array<[NodeDef, MutableStep]> = def.nodes.map((node) => [
    node,
    { state: 'pending', attempt: 0, outputSet: false, awaitingApproval: false },
  ]);
  const steps = new Map<StepName, MutableStep>(nodes.map(([node, step]) => [node.name, step]));

  let workflowState: RunState = 'pending';
  let workflow = def.name;
  let definitionVersion = def.version;
  let input: unknown;
  let output: unknown;
  let outputSet = false;
  let error: string | undefined;
  let deadlineAt: number | undefined;
  let tenant: string | undefined;
  let createdAt = first.at;
  let updatedAt = first.at;
  let sequence: Sequence = 0;

  const seen = new Set<string>();
  let current: Event = first;

  /**
   * Applies a transition, routing through `via` when the direct hop is forbidden but
   * the two-step path is legal.
   *
   * The `via` hops are not a loophole; each one records something that genuinely
   * happened. A completion arriving for a step the projection believes is only
   * `ready` means the step *was* running on a worker whose lease had lapsed, so
   * `ready -> running -> succeeded` is a more accurate history than a rejection.
   */
  const move = (subject: string, holder: { state: RunState }, to: RunState, via?: RunState): boolean => {
    const from = holder.state;
    // Re-asserting the state something is already in is not a transition. This is
    // what makes a redelivered outcome harmless rather than fatal.
    if (from === to) return false;
    if (canTransition(from, to)) {
      holder.state = to;
      return true;
    }
    if (via !== undefined && canTransition(from, via) && canTransition(via, to)) {
      holder.state = to;
      return true;
    }
    throw new IllegalTransitionError(workflowId, current.sequence, current.type, subject, from, to);
  };

  const workflowHolder = {
    get state(): RunState {
      return workflowState;
    },
    set state(value: RunState) {
      workflowState = value;
    },
  };
  const moveWorkflow = (to: RunState, via?: RunState): boolean =>
    move('workflow', workflowHolder, to, via);

  const stepOf = (name: StepName): MutableStep => {
    const step = steps.get(name);
    if (step === undefined) {
      throw new ProjectionError(
        `event at sequence ${current.sequence} (${current.type}) references step "${name}", ` +
          `which is not in definition ${def.name}@${def.version}`,
      );
    }
    return step;
  };
  const moveStep = (name: StepName, step: MutableStep, to: RunState, via?: RunState): boolean =>
    move(`step "${name}"`, step, to, via);

  for (const event of events) {
    if (event.workflowId !== workflowId) {
      throw new ProjectionError(
        `history mixes workflows: expected ${workflowId}, found ${String(event.workflowId)} ` +
          `at sequence ${String(event.sequence)}`,
      );
    }
    if (event.sequence <= sequence) {
      throw new ProjectionError(
        `history is not ordered: sequence ${String(event.sequence)} follows ${sequence}`,
      );
    }
    sequence = event.sequence;
    current = event;

    const key = dedupeKey(event);
    if (seen.has(key)) continue;
    seen.add(key);
    updatedAt = event.at;

    switch (event.type) {
      case 'workflow.created': {
        workflow = event.workflow;
        definitionVersion = event.definitionVersion;
        input = event.input;
        createdAt = event.at;
        deadlineAt = event.deadlineAt;
        tenant = event.tenant;
        break;
      }

      case 'workflow.completed': {
        if (moveWorkflow('succeeded')) {
          output = event.output;
          outputSet = true;
        }
        break;
      }

      case 'workflow.failed': {
        // `via: 'ready'` covers a workflow that fails before any step ran, which
        // `pending -> failed` forbids.
        if (moveWorkflow('failed', 'ready')) error = event.error;
        break;
      }

      case 'workflow.cancelled': {
        if (moveWorkflow('cancelled')) {
          error = event.reason;
          // Cancellation means stop everything, so every live step is cancelled with
          // it. A *failure* deliberately does not do this: a sibling still running on
          // some worker has not been cancelled just because another step failed, and
          // claiming otherwise in the projection would be a lie about the world.
          for (const [name, step] of steps) {
            if (isTerminal(step.state)) continue;
            moveStep(name, step, 'cancelled');
            step.awaitingApproval = false;
            step.leaseExpiresAt = undefined;
          }
        }
        break;
      }

      case 'step.scheduled': {
        const step = stepOf(event.step);
        moveStep(event.step, step, 'ready');
        step.attempt = event.attempt;
        step.notBefore = event.notBefore;
        step.workerId = undefined;
        step.leaseExpiresAt = undefined;
        break;
      }

      case 'step.leased': {
        // A lease reserves the step; it does not start it. Keeping them separate is
        // what lets `readySteps` tell "nobody has this" from "somebody has this and
        // has not reported in yet".
        const step = stepOf(event.step);
        step.attempt = event.attempt;
        step.workerId = event.workerId;
        step.leaseExpiresAt = event.leaseExpiresAt;
        break;
      }

      case 'step.lease_extended': {
        const step = stepOf(event.step);
        // A heartbeat from a worker that no longer holds the lease must not revive it.
        if (step.workerId !== undefined && step.workerId !== event.workerId) break;
        // Only the expiry moves. A renewal is not a new lease, so it resets neither the
        // attempt nor the holder, and it is not a transition: the step keeps running.
        step.leaseExpiresAt = event.leaseExpiresAt;
        break;
      }

      case 'step.started': {
        const step = stepOf(event.step);
        moveStep(event.step, step, 'running');
        step.attempt = event.attempt;
        step.workerId = event.workerId;
        moveWorkflow('running', 'ready');
        break;
      }

      case 'step.completed': {
        const step = stepOf(event.step);
        if (!moveStep(event.step, step, 'succeeded', 'running')) break;
        step.attempt = event.attempt;
        step.workerId = event.workerId;
        step.output = event.output;
        step.outputSet = true;
        step.leaseExpiresAt = undefined;
        step.awaitingApproval = false;
        break;
      }

      case 'step.failed': {
        const step = stepOf(event.step);
        // The producer recorded its own retry decision, so the fold does not consult
        // the definition's RetryPolicy. That is what keeps the projection a function of
        // the log alone: re-deriving it would make replaying an old history against a
        // changed policy produce different states.
        const to = event.willRetry ? 'retrying' : 'failed';
        if (!moveStep(event.step, step, to, 'running')) break;
        step.attempt = event.attempt;
        step.error = event.error;
        step.errorClass = event.errorClass;
        step.leaseExpiresAt = undefined;
        step.awaitingApproval = false;
        if (event.workerId !== undefined) step.workerId = event.workerId;
        break;
      }

      case 'step.retry_scheduled': {
        const step = stepOf(event.step);
        moveStep(event.step, step, 'ready', 'retrying');
        step.attempt = event.attempt;
        step.notBefore = event.notBefore;
        step.workerId = undefined;
        step.leaseExpiresAt = undefined;
        step.error = undefined;
        step.errorClass = undefined;
        break;
      }

      case 'step.lease_expired': {
        const step = stepOf(event.step);
        // Nothing to reassign once the step has settled: an expiry notice that races
        // a completion is information, not a state change.
        if (isTerminal(step.state)) break;
        // An expiry for a lease that has already been superseded must not evict the
        // worker currently holding it.
        if (step.workerId !== undefined && step.workerId !== event.workerId) break;
        // Straight back to `ready` for reassignment, explicitly *not* `failed` and
        // explicitly not through `retrying`: the step may still be running on the
        // unreachable worker, so this is a worker that went away rather than work that
        // went wrong, and it must not spend any of the step's retry budget.
        moveStep(event.step, step, 'ready');
        step.workerId = undefined;
        step.leaseExpiresAt = undefined;
        // The attempt number is deliberately unchanged, which is both what keeps the
        // retry budget intact and what makes the reassigned run reuse the same
        // idempotency key, so a possible double execution is harmless.
        break;
      }

      case 'tool.requested':
      case 'tool.succeeded':
      case 'tool.failed': {
        // Tool events are the audit trail of *why* a step did what it did. The step's
        // fate is recorded by the step.* events, so folding these into state as well
        // would give one outcome two sources of truth.
        stepOf(event.step);
        break;
      }

      case 'approval.requested': {
        const step = stepOf(event.step);
        moveStep(event.step, step, 'waiting', 'running');
        step.awaitingApproval = true;
        break;
      }

      case 'approval.granted': {
        const step = stepOf(event.step);
        if (isTerminal(step.state)) break;
        // A grant resumes the attempt that was blocked on it rather than completing
        // the step: the node still has to run and report its own outcome.
        moveStep(event.step, step, 'running');
        step.awaitingApproval = false;
        break;
      }

      case 'approval.denied': {
        const step = stepOf(event.step);
        if (!moveStep(event.step, step, 'failed')) break;
        step.awaitingApproval = false;
        step.error = `approval denied by ${event.approver}: ${event.reason}`;
        step.errorClass = 'approval_denied';
        break;
      }
    }

    if (!isTerminal(workflowState)) {
      // Readiness is derived, not announced: a step is ready exactly when every
      // dependency has succeeded. Recomputing it after each event means the scheduler
      // cannot forget to mark a fan-in step ready, and a step with no dependencies is
      // ready the moment the workflow exists.
      for (const [node, step] of nodes) {
        if (step.state !== 'pending') continue;
        if (!node.dependsOn.every((dep) => steps.get(dep)?.state === 'succeeded')) continue;
        moveStep(node.name, step, 'ready');
      }
      if (workflowState === 'pending' && nodes.some(([, step]) => step.state === 'ready')) {
        moveWorkflow('ready');
      }
    }
  }

  const projected = new Map<StepName, StepProjection>();
  for (const [node, step] of nodes) {
    projected.set(node.name, {
      step: node.name,
      state: step.state,
      attempt: step.attempt,
      awaitingApproval: step.awaitingApproval,
      ...(step.workerId !== undefined ? { workerId: step.workerId } : {}),
      ...(step.leaseExpiresAt !== undefined ? { leaseExpiresAt: step.leaseExpiresAt } : {}),
      ...(step.notBefore !== undefined ? { notBefore: step.notBefore } : {}),
      ...(step.outputSet ? { output: step.output } : {}),
      ...(step.error !== undefined ? { error: step.error } : {}),
      ...(step.errorClass !== undefined ? { errorClass: step.errorClass } : {}),
    });
  }

  return {
    workflowId,
    workflow,
    definitionVersion,
    state: workflowState,
    input,
    steps: projected,
    sequence,
    createdAt,
    updatedAt,
    ...(outputSet ? { output } : {}),
    ...(error !== undefined ? { error } : {}),
    ...(deadlineAt !== undefined ? { deadlineAt } : {}),
    ...(tenant !== undefined ? { tenant } : {}),
  };
}

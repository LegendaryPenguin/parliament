/**
 * Adapter between `src/state`'s projector and the API's `Projector` port.
 *
 * `project(def, events)` needs the definition, because whether a `step.failed` folds
 * to `failed` or to `retrying` depends on the retry policy — a decision that cannot be
 * made from the events alone. The API, on the other hand, is handed a workflow id and
 * has to answer for it whether or not the definition is still around. This closes that
 * gap in one place instead of at every call site.
 *
 * Three sources, in order of how much they can be trusted: the definition recorded in
 * the workflow's own `workflow.created` event, then the definition store, then a
 * reconstruction from the history. The first is normal; the third is a degraded mode and
 * is treated as such: see `synthesizeDefinition`.
 */

import type { Event, Sequence, WorkflowProjection } from '../events/types.js';
import { project as projectWithDef } from '../state/project.js';
import { replayTo as replayToWithDef } from '../state/replay.js';
import type { NodeDef, RetryPolicy, StepName, WorkflowDef } from '../workflow/types.js';
import type { DefinitionStore, Projector, ReplayProjector } from './deps.js';

export interface BoundProjection {
  readonly project: Projector;
  readonly replayTo: ReplayProjector;
  /** The definition used for a history, real or reconstructed. */
  readonly definitionFor: (events: readonly Event[]) => WorkflowDef;
}

export function bindProjection(definitions: DefinitionStore): BoundProjection {
  const definitionFor = (events: readonly Event[]): WorkflowDef => {
    const first = events[0];
    const stored = first !== undefined ? definitions.get(first.workflowId) : undefined;
    return stored ?? synthesizeDefinition(events);
  };

  return {
    definitionFor,
    project: (events: readonly Event[]): WorkflowProjection =>
      projectWithDef(definitionFor(events), events),
    replayTo: (events: readonly Event[], sequence: Sequence): WorkflowProjection =>
      replayToWithDef(definitionFor(events), events, sequence),
  };
}

/**
 * Reconstructs a definition from a history.
 *
 * Needed for a workflow whose definition the process no longer has — after a restart,
 * or for a history pasted in from elsewhere. Two things are inferred, both from
 * evidence in the log rather than from a default:
 *
 *  - **The graph** is a chain in first-seen order. Wrong for a parallel definition, and
 *    it will draw a fan-out as a line. It is the only ordering the events support, and
 *    a visibly-linear DAG is better than refusing to render one.
 *  - **The retry policy** is derived so that it permits exactly the retries that
 *    actually happened: `maxAttempts` is the highest attempt observed and the retryable
 *    classes are the ones a `step.retry_scheduled` was in fact seen after. This matters
 *    beyond cosmetics — the projector treats `failed` as terminal, so a policy that
 *    forbade an observed retry would make the real history an illegal transition and
 *    the projection would throw rather than render.
 */
export function synthesizeDefinition(events: readonly Event[]): WorkflowDef {
  const order: StepName[] = [];
  const maxAttempt = new Map<StepName, number>();
  const retriedClasses = new Set<string>();
  const lastFailureClass = new Map<StepName, string>();
  const approvers = new Map<StepName, readonly string[]>();
  let name = 'unknown';
  let version = 0;

  for (const event of events) {
    if (event.type === 'workflow.created') {
      name = event.workflow;
      version = event.definitionVersion;
      continue;
    }
    if (!('step' in event) || typeof event.step !== 'string') continue;
    const step: StepName = event.step;
    if (!order.includes(step)) order.push(step);

    if ('attempt' in event && typeof event.attempt === 'number') {
      maxAttempt.set(step, Math.max(maxAttempt.get(step) ?? 1, event.attempt));
    }
    if (event.type === 'step.failed') lastFailureClass.set(step, event.errorClass);
    if (event.type === 'step.retry_scheduled') {
      const failed = lastFailureClass.get(step);
      if (failed !== undefined) retriedClasses.add(failed);
    }
    if (event.type === 'approval.requested') approvers.set(step, event.approvers);
  }

  const attempts = Math.max(1, ...maxAttempt.values());
  const retry: RetryPolicy = {
    maxAttempts: attempts,
    initialBackoffMs: 0,
    maxBackoffMs: 0,
    jitter: 0,
    retryableErrors: [...retriedClasses],
  };

  const nodes: NodeDef[] = order.map((step, i) => {
    const approverList = approvers.get(step);
    return {
      name: step,
      kind: approverList !== undefined ? 'approval' : 'tool',
      dependsOn: i === 0 ? [] : [order[i - 1] as StepName],
      retry,
      ...(approverList !== undefined ? { approvers: approverList } : {}),
    };
  });

  return {
    name,
    version,
    start: order[0] ?? '',
    nodes,
    timeouts: { attemptTimeoutMs: 0, leaseDurationMs: 0 },
  };
}

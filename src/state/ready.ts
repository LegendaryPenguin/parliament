/**
 * Which steps may be dispatched right now.
 *
 * Separate from the fold and pure in the same way, with `now` passed in rather than
 * read. That is what lets the scheduler's decision be reproduced after the fact: given
 * the same history and the same instant, the answer is the same, so "why did it
 * dispatch that" is answerable instead of arguable.
 */

import type { StepProjection, WorkflowProjection } from '../events/types.js';
import { isTerminal } from '../workflow/types.js';
import type { StepName, WorkflowDef } from '../workflow/types.js';

/**
 * Counts the steps occupying a worker.
 *
 * A step counts from the moment it is leased, not from the moment it starts: counting
 * only running steps would let one scheduler pass hand out more leases than the cap,
 * and the cap exists precisely so a fan-out cannot run away.
 *
 * A step `waiting` on an approval does *not* count. It holds no worker — that is the
 * point of modelling approval as a state rather than a blocking call — and counting it
 * would let one pending approval throttle the workflow's unrelated parallel branches.
 */
function inFlight(projection: WorkflowProjection, now: number): number {
  let count = 0;
  for (const step of projection.steps.values()) {
    if (step.state === 'waiting') continue;
    if (step.state === 'running') count += 1;
    else if (step.leaseExpiresAt !== undefined && step.leaseExpiresAt > now) count += 1;
  }
  return count;
}

export function readySteps(
  def: WorkflowDef,
  projection: WorkflowProjection,
  now: number,
): readonly StepProjection[] {
  if (isTerminal(projection.state)) return [];
  // Past the deadline the execution is no longer useful, so nothing further starts.
  // Steps already running are left alone — stopping them is the scheduler's business,
  // and this function only answers what may *begin*.
  if (projection.deadlineAt !== undefined && now >= projection.deadlineAt) return [];

  const limit =
    def.maxConcurrency === undefined
      ? Number.POSITIVE_INFINITY
      : Math.max(0, def.maxConcurrency - inFlight(projection, now));

  const dispatchable: StepProjection[] = [];
  // Definition order, not map order, so two schedulers folding the same history pick
  // the same steps in the same order and the loser of the append race retries against
  // a history it can interpret.
  for (const node of def.nodes) {
    if (dispatchable.length >= limit) break;

    const step = projection.steps.get(node.name);
    if (step === undefined) continue;
    if (step.state !== 'ready') continue;
    if (step.awaitingApproval) continue;
    // Somebody already holds this one.
    if (step.leaseExpiresAt !== undefined && step.leaseExpiresAt > now) continue;
    // A retry backoff or a scheduled delay that has not elapsed.
    if (step.notBefore !== undefined && step.notBefore > now) continue;
    if (!dependenciesSatisfied(projection, node.dependsOn)) continue;

    dispatchable.push(step);
  }
  return dispatchable;
}

/**
 * Re-checked here even though the fold already enforces it. The projection is derived
 * from events produced by other processes, and dispatching a step whose dependency has
 * not succeeded is the one mistake that cannot be undone by a later event.
 */
function dependenciesSatisfied(
  projection: WorkflowProjection,
  dependsOn: readonly StepName[],
): boolean {
  return dependsOn.every((dep) => projection.steps.get(dep)?.state === 'succeeded');
}

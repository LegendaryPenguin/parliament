/**
 * Synthetic workflows and synthetic histories.
 *
 * Every number in `results.md` is only as meaningful as the history it was measured
 * against, so the generators here are documented in terms of what they contain rather
 * than left as "N events". Two properties are deliberate:
 *
 *  - **The histories are legal.** They are built from the same event shapes the scheduler
 *    writes, in an order `project()` accepts, so the fold runs its real code paths. A
 *    generator that emitted ten thousand copies of one event would be folded nine
 *    thousand nine hundred and ninety-nine times by the deduplication check and would
 *    measure a `Set.has` call instead of the projection.
 *  - **Length is grown by heartbeats, not by steps.** A long history in this system is
 *    overwhelmingly a long-running step renewing its lease — `step.lease_extended` is the
 *    only event whose count is unbounded for a fixed graph. Growing length by adding
 *    nodes instead would confound the two axes the fold is actually sensitive to, which
 *    is why they are measured separately.
 */

import type { Event, NewEvent } from '../src/events/types.js';
import { EVENT_VERSION } from '../src/events/types.js';
import {
  DEFAULT_RETRY,
  type NodeDef,
  type WorkflowDef,
  type WorkflowId,
} from '../src/workflow/types.js';

const T0 = 1_700_000_000_000;

/** A chain of `nodes` tool steps: the simplest shape with a non-trivial dependency edge. */
export function chainDef(nodes: number, name = `chain-${nodes}`): WorkflowDef {
  const list: NodeDef[] = [];
  for (let i = 0; i < nodes; i += 1) {
    list.push({
      name: `step-${i}`,
      kind: 'tool',
      tool: 'noop',
      dependsOn: i === 0 ? [] : [`step-${i - 1}`],
      retry: DEFAULT_RETRY,
    });
  }
  return {
    name,
    version: 1,
    start: 'step-0',
    nodes: list,
    timeouts: { attemptTimeoutMs: 30_000, leaseDurationMs: 15_000 },
  };
}

/** `width` independent root steps: the shape optimistic-concurrency contention needs. */
export function fanOutDef(width: number, name = `fan-${width}`): WorkflowDef {
  const list: NodeDef[] = [];
  for (let i = 0; i < width; i += 1) {
    list.push({ name: `step-${i}`, kind: 'tool', tool: 'noop', dependsOn: [] });
  }
  return {
    name,
    version: 1,
    start: 'step-0',
    nodes: list,
    timeouts: { attemptTimeoutMs: 30_000, leaseDurationMs: 15_000 },
  };
}

/**
 * A realistic agent pipeline: fetch, two parallel analyses, a join, and a finalize.
 *
 * Used by the end-to-end benchmark because the shape is what the latency breakdown is
 * about — a diamond has a fan-out whose branches overlap and a fan-in that cannot start
 * until both finish, so "where did the time go" has a non-obvious answer.
 */
export function diamondDef(name = 'pipeline'): WorkflowDef {
  return {
    name,
    version: 1,
    start: 'fetch',
    timeouts: { attemptTimeoutMs: 10_000, leaseDurationMs: 5_000 },
    nodes: [
      { name: 'fetch', kind: 'tool', tool: 'noop', dependsOn: [] },
      { name: 'classify', kind: 'tool', tool: 'noop', dependsOn: ['fetch'] },
      { name: 'score', kind: 'tool', tool: 'noop', dependsOn: ['fetch'] },
      { name: 'join', kind: 'join', dependsOn: ['classify', 'score'] },
      { name: 'finalize', kind: 'finalize', dependsOn: ['join'] },
    ],
  };
}

export interface HistoryShape {
  readonly workflowId: WorkflowId;
  readonly def: WorkflowDef;
  /** Target event count. The generator lands on at least this many and reports the real one. */
  readonly events: number;
}

/** Events a single completed step contributes, before any heartbeats. */
const PER_STEP = 6;

/**
 * A history of exactly `events` events for a chain definition.
 *
 * Per step: `scheduled`, `leased`, `started`, k x `lease_extended`, `tool.requested`,
 * `tool.succeeded`, `completed` — six events plus the heartbeats. The heartbeat count is
 * whatever is needed to reach the target, spread evenly across the steps that ran so no
 * single step's fold path is over-represented.
 *
 * A target too small for the whole graph produces a *partial* history: the first steps run
 * and the rest stay pending, which is a workflow in progress and a legal history. The
 * alternative — shrinking the graph to fit — would change the node count between rows of
 * the same table, and the node count is the other axis being measured.
 */
export function chainHistory(shape: HistoryShape): readonly Event[] {
  const { def, workflowId } = shape;
  const nodes = def.nodes.length;
  const stepsToRun = Math.max(0, Math.min(nodes, Math.floor((shape.events - 1) / PER_STEP)));
  let fixed = 1 + stepsToRun * PER_STEP;
  const complete = stepsToRun === nodes && shape.events >= fixed + 1;
  if (complete) fixed += 1;

  const heartbeats = Math.max(0, shape.events - fixed);
  const perStep = stepsToRun === 0 ? 0 : Math.floor(heartbeats / stepsToRun);
  const remainder = stepsToRun === 0 ? 0 : heartbeats - perStep * stepsToRun;

  let at = T0;
  const tick = (by = 1): number => (at += by);
  const out: NewEvent[] = [
    {
      type: 'workflow.created',
      workflowId,
      at,
      workflow: def.name,
      definitionVersion: def.version,
      definition: def,
      input: { seeded: true },
    },
  ];

  def.nodes.slice(0, stepsToRun).forEach((node, i) => {
    const worker = `worker-${i % 4}`;
    const renewals = perStep + (i < remainder ? 1 : 0);
    out.push({ type: 'step.scheduled', workflowId, at: tick(), step: node.name, attempt: 1 });
    out.push({
      type: 'step.leased',
      workflowId,
      at: tick(),
      step: node.name,
      attempt: 1,
      workerId: worker,
      leaseExpiresAt: at + 15_000,
    });
    out.push({ type: 'step.started', workflowId, at: tick(), step: node.name, attempt: 1, workerId: worker });
    for (let h = 0; h < renewals; h += 1) {
      // Each renewal carries a distinct expiry, which is what makes it a distinct fact to
      // the fold rather than a duplicate absorbed by the deduplication key.
      out.push({
        type: 'step.lease_extended',
        workflowId,
        at: tick(5_000),
        step: node.name,
        attempt: 1,
        workerId: worker,
        leaseExpiresAt: at + 15_000,
      });
    }
    out.push({
      type: 'tool.requested',
      workflowId,
      at: tick(),
      step: node.name,
      attempt: 1,
      tool: 'noop',
      args: { n: i },
      idempotencyKey: `${workflowId}/${node.name}/1`,
    });
    out.push({
      type: 'tool.succeeded',
      workflowId,
      at: tick(),
      step: node.name,
      attempt: 1,
      tool: 'noop',
      result: { ok: true, n: i },
      durationMs: 3,
      deduplicated: false,
    });
    out.push({
      type: 'step.completed',
      workflowId,
      at: tick(),
      step: node.name,
      attempt: 1,
      workerId: worker,
      output: { n: i },
    });
  });

  if (complete) {
    out.push({ type: 'workflow.completed', workflowId, at: tick(), output: { done: true } });
  }
  return seal(out);
}

/**
 * Assigns sequences the way a store would.
 *
 * Done here rather than by appending through a real store, because the projection
 * benchmark is supposed to measure the fold and not the store underneath it. The sealing
 * rule is the same one `sealBatch` applies: sequence is 1-based and dense.
 */
function seal(events: readonly NewEvent[]): readonly Event[] {
  return events.map((event, i) => ({ ...event, version: EVENT_VERSION, sequence: i + 1 }) as Event);
}

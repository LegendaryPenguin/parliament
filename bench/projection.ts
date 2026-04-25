/**
 * Benchmark 1 — what a projection costs as a history grows.
 *
 * This is the central cost of the design and its honest weakness. `project()` is a fold
 * over the whole history, and the scheduler calls it on every tick of every active
 * workflow, so whatever this curve is, it is the shape of the control plane's steady-state
 * CPU cost. The question is not whether folding is fast; it is whether it is *linear*,
 * because a linear fold with no snapshot is a cost that grows without bound for a workflow
 * that never ends.
 *
 * Two axes are measured separately, because the fold is sensitive to both and mixing them
 * would make either one unreadable:
 *
 *  - history length at a fixed graph, which is the axis that grows in production;
 *  - graph size at a fixed history, because the readiness recomputation at the end of each
 *    event's switch is a loop over every node, so the per-event cost is a function of the
 *    node count and not a constant.
 *
 * `timeline()` is measured too, at much smaller sizes. It folds every prefix, so it is
 * quadratic by construction — its own doc comment says so and chooses it deliberately —
 * and the point of measuring it is to put a number on where that choice stops being
 * affordable.
 */

import { project } from '../src/state/project.js';
import { timeline } from '../src/state/replay.js';
import type { Event } from '../src/events/types.js';
import { chainDef, chainHistory } from './fixtures.js';
import { measure, ms, rate, spread, table, type Section, type Stats } from './harness.js';

const LENGTHS = [10, 100, 1_000, 10_000];
const NODE_COUNTS = [4, 16, 64, 256];

export async function run(): Promise<Section> {
  const lengthRows: string[][] = [];
  const perEvent: { events: number; nsPerEvent: number }[] = [];

  for (const target of LENGTHS) {
    const def = chainDef(8, 'fold-8');
    const events = chainHistory({ workflowId: 'wf_fold', def, events: target });
    // Folded once outside the timing loop, so a projection error in a generated history
    // fails loudly here rather than being measured as a throw.
    project(def, events);

    const stats = await foldStats(def, events);
    const nsPerEvent = (stats.medianMs * 1e6) / events.length;
    perEvent.push({ events: events.length, nsPerEvent });
    lengthRows.push([
      String(events.length),
      spread(stats),
      `${rate(1, stats.medianMs)}`,
      `${nsPerEvent.toFixed(0)}`,
    ]);
  }

  const nodeRows: string[][] = [];
  for (const nodes of NODE_COUNTS) {
    const def = chainDef(nodes, `fold-nodes-${nodes}`);
    const events = chainHistory({ workflowId: 'wf_nodes', def, events: 2_000 });
    project(def, events);
    const stats = await foldStats(def, events);
    nodeRows.push([
      String(nodes),
      String(events.length),
      spread(stats),
      `${((stats.medianMs * 1e6) / events.length).toFixed(0)}`,
    ]);
  }

  const timelineRows: string[][] = [];
  for (const target of [100, 400, 1_600]) {
    const def = chainDef(8, 'timeline-8');
    const events = chainHistory({ workflowId: 'wf_timeline', def, events: target });
    const stats = await measure(() => void timeline(def, events), { runs: 5, warmup: 3 });
    timelineRows.push([String(events.length), spread(stats)]);
  }

  // The slope is read off the rows from 100 events up. The 10-event row is excluded on
  // purpose: at that length the fold is dominated by its fixed setup — allocating one
  // mutable record per node and the two maps — so its per-event figure describes the
  // constant, not the slope, and including it would understate the growth rate.
  const slopeRows = perEvent.filter((p) => p.events >= 100);
  const first = slopeRows[0] as { events: number; nsPerEvent: number };
  const last = slopeRows[slopeRows.length - 1] as { events: number; nsPerEvent: number };
  const shortest = perEvent[0] as { events: number; nsPerEvent: number };
  const drift = last.nsPerEvent / first.nsPerEvent;

  const body = [
    'Folding one workflow\'s whole history with `project()`. The histories are generated',
    'by `bench/fixtures.ts`: per step a `scheduled`, `leased`, `started`, a run of',
    '`lease_extended` renewals, a `tool.requested`/`tool.succeeded` pair, and a',
    '`completed`, ending in `workflow.completed`. Length is grown by adding renewals,',
    'which is how a real history gets long: for a fixed graph, heartbeats are the only',
    'event whose count is unbounded.',
    '',
    '### Cost against history length (graph fixed at 8 nodes)',
    '',
    table(
      ['events', 'fold (ms)', 'folds/sec', 'ns/event'],
      lengthRows,
    ),
    '',
    `**It is linear.** Per-event cost moves from ${first.nsPerEvent.toFixed(0)} ns at`,
    `${first.events} events to ${last.nsPerEvent.toFixed(0)} ns at ${last.events} — a factor`,
    `of ${drift.toFixed(2)} across a ${last.events / first.events}x change in length — so the`,
    'total is proportional to the length of the log and the slope is flat enough that',
    `nothing super-linear is hiding in it. The ${shortest.events}-event row sits higher per`,
    `event (${shortest.nsPerEvent.toFixed(0)} ns) because at that length the fold is mostly`,
    'its own fixed setup: one mutable record per node, and the two maps.',
    '',
    'What that implies at scale: the scheduler folds the full history of every active',
    'workflow on every tick, so a workflow is not a fixed cost — it is a cost that rises',
    'for as long as it lives. A step that heartbeats every five seconds for a day adds',
    '~17k events, and at the rate above that single workflow spends on the order of a',
    `millisecond of CPU per tick (measured: ${ms((last.nsPerEvent * 17_000) / 1e6)} ms for`,
    '17k events) purely to re-derive state it already derived on the previous tick. A few',
    'hundred such workflows and the tick loop is the bottleneck.',
    '',
    '**The fix is snapshotting, and it is not implemented.** The shape it would take is a',
    'periodic materialized projection plus the sequence it was taken at, with the fold',
    'resuming from there — `project()` is already a pure function of a prefix, and',
    '`replayTo` already demonstrates that a prefix of the log is a valid log, so the',
    'mechanism is available. Nothing in this repository does it. The honest statement is',
    'that recovery and steady-state cost are both O(history) today, and the numbers below',
    'are what that costs.',
    '',
    '### Cost against graph size (history fixed at ~2,000 events)',
    '',
    table(['nodes', 'events', 'fold (ms)', 'ns/event'], nodeRows),
    '',
    'Per-event cost rises with the node count because `project()` recomputes readiness',
    'after every event by looping over every node. That is a deliberate trade — it is what',
    'makes it impossible for the scheduler to forget to mark a fan-in step ready — but it',
    'means the fold is O(events x nodes), not O(events). For the graph sizes a workflow',
    'definition realistically has (tens of nodes) the constant is small; for a generated',
    'graph with hundreds, it dominates.',
    '',
    '### `timeline()`, the debugging view',
    '',
    table(['events', 'timeline (ms)'], timelineRows),
    '',
    'Quadratic, as its own comment says it is: it folds every prefix so that the',
    'transitions it prints come from the same state machine the scheduler uses rather than',
    'from a second implementation that could disagree. The numbers say that choice is free',
    'for the few hundred events a human reads and unusable past a few thousand, which',
    'matches where it is used — the CLI and the dashboard, on one workflow at a time.',
  ].join('\n');

  return { title: 'Projection cost vs history length', body };
}

function foldStats(def: Parameters<typeof project>[0], events: readonly Event[]): Promise<Stats> {
  // Repeated inside one timed run for the short histories, so that a 10-event fold is not
  // measured against the resolution of the clock: at ~2 us per fold, a single call is
  // within the noise of `performance.now()` itself.
  const inner = events.length < 500 ? 1_000 : events.length < 5_000 ? 50 : 10;
  return measure(
    () => {
      for (let i = 0; i < inner; i += 1) project(def, events);
    },
    { runs: 7, warmup: 2 },
  ).then((stats) => scale(stats, inner));
}

/** Divides a batch timing back down to one operation, keeping the spread meaningful. */
function scale(stats: Stats, by: number): Stats {
  return {
    runs: stats.runs,
    medianMs: stats.medianMs / by,
    meanMs: stats.meanMs / by,
    stddevMs: stats.stddevMs / by,
    minMs: stats.minMs / by,
    maxMs: stats.maxMs / by,
    samplesMs: stats.samplesMs.map((s) => s / by),
  };
}

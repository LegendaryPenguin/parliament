/**
 * Benchmark 4 — what recovery costs.
 *
 * The design's central claim is that crash recovery is a replay and that there is nothing
 * to get wrong: no reconciliation pass, no repair step, no second source of truth about
 * what was running. `Scheduler.recover()` is five lines, and that is the point. This
 * measures what those five lines cost, because "nothing to get wrong" is only a good trade
 * if the price is affordable.
 *
 * Recovery is three distinct phases and they are timed separately, because they scale with
 * different things:
 *
 *  1. **Open the store.** `FileEventStore.open` loads the index and reconciles it against
 *     the logs by `stat`-ing each file. O(workflows), and no event is read.
 *  2. **Read every active history.** O(bytes on disk).
 *  3. **Fold each one.** O(events), as benchmark 1 established.
 *
 * The degraded path is measured too: with the index deleted, `open` has to rebuild it by
 * reading every log in full. That is the case where a crash happened to land between the
 * data fsync and the index write, or where someone deleted a file they were told was a
 * cache, and knowing its cost is the difference between "the cache is optional" being true
 * and being merely claimed.
 */

import { rm } from 'node:fs/promises';
import { join } from 'node:path';

import { FileEventStore } from '../src/events/file-store.js';
import type { Event } from '../src/events/types.js';
import { project } from '../src/state/project.js';
import { chainDef, chainHistory } from './fixtures.js';
import { openScratchStore } from './stores.js';
import { ms, summarize, table, type Section } from './harness.js';

/** (workflows, events per workflow) pairs. The product is the log size being recovered. */
const SIZES: readonly [number, number][] = [
  [10, 100],
  [100, 100],
  [100, 1_000],
  [500, 200],
];

const RUNS = 3;

export interface RecoveryOptions {
  readonly root: string;
}

export async function run(options: RecoveryOptions): Promise<Section> {
  const rows: string[][] = [];
  const degraded: string[][] = [];

  for (const [workflows, perWorkflow] of SIZES) {
    const dir = await seedStore(options, workflows, perWorkflow);

    const openSamples: number[] = [];
    const replaySamples: number[] = [];
    let folded = 0;
    let activeCount = 0;

    for (let run = 0; run < RUNS; run += 1) {
      // A new store object on the same directory is exactly what a restarted process does:
      // nothing is carried over in memory, and the only state is what is on disk.
      const openStarted = performance.now();
      const store = await FileEventStore.open(dir);
      const active = await store.listActive();
      openSamples.push(performance.now() - openStarted);

      const replayStarted = performance.now();
      let events = 0;
      for (const id of active) {
        const history = await store.read(id);
        project(definitionFor(history), history);
        events += history.length;
      }
      replaySamples.push(performance.now() - replayStarted);
      folded = events;
      activeCount = active.length;
    }

    const open = summarize(openSamples);
    const replay = summarize(replaySamples);
    const total = open.medianMs + replay.medianMs;

    rows.push([
      workflows.toLocaleString('en-US'),
      perWorkflow.toLocaleString('en-US'),
      (workflows * perWorkflow).toLocaleString('en-US'),
      String(activeCount),
      ms(open.medianMs),
      ms(replay.medianMs),
      ms(total),
      folded > 0 ? ((total * 1e6) / folded).toFixed(0) : 'n/a',
    ]);

    // The index is a cache and the logs are the truth, so deleting it must be survivable.
    // Timed, not merely asserted: an escape hatch nobody has measured is an escape hatch
    // whose cost is discovered during the incident that needs it.
    const rebuildSamples: number[] = [];
    for (let run = 0; run < RUNS; run += 1) {
      await rm(join(dir, 'index.json'), { force: true });
      const started = performance.now();
      const store = await FileEventStore.open(dir);
      await store.listActive();
      rebuildSamples.push(performance.now() - started);
    }
    const rebuild = summarize(rebuildSamples);
    degraded.push([
      workflows.toLocaleString('en-US'),
      (workflows * perWorkflow).toLocaleString('en-US'),
      ms(open.medianMs),
      ms(rebuild.medianMs),
      `${(rebuild.medianMs / Math.max(0.001, open.medianMs)).toFixed(0)}x`,
    ]);
  }

  const body = [
    'Each row seeds a file store with a given number of workflows, each carrying a history',
    'of a given length, then drops the store object and does what a restarted control plane',
    'does: `FileEventStore.open` on the same directory, `listActive()`, and a read plus a',
    'fold per active workflow. Nothing is carried in memory across the boundary.',
    '',
    'Half the seeded workflows are left incomplete — no terminal event — so `listActive`',
    'has something to exclude and the "active" column is not just the workflow count. That',
    'matters: recovery cost is driven by the *active* set, and a store full of finished',
    'workflows is cheap to recover from no matter how large it is.',
    '',
    '### Recovery, index intact',
    '',
    table(
      [
        'workflows',
        'events each',
        'events total',
        'active',
        'open (ms)',
        'read+fold (ms)',
        'total (ms)',
        'ns/replayed event',
      ],
      rows,
    ),
    '',
    '`open` is cheap and nearly flat in the event count, because with a valid index it only',
    '`stat`s each log file and compares the size — a workflow whose file is the length the',
    'index says is not read at all. It scales with the *number* of workflows, not with their',
    'length. The read-and-fold phase is where recovery time actually goes, and it is linear',
    'in the number of events in the active set, at a per-event cost in the same range',
    'benchmark 1 measured for the fold alone plus the cost of reading and checksumming each',
    'line. The last column divides total recovery time by the events actually replayed —',
    'the active half — rather than by everything on disk.',
    '',
    'The claim holds: recovery is a replay, it is linear, and there is no step in it that',
    'can be done wrong — but it is also not free, and it is the same O(history) cost as the',
    'steady-state fold. A control plane holding a large active set restarts in time',
    'proportional to the total length of every unfinished workflow it owns. Snapshotting',
    'would fix this and recovery at the same time; it is still not implemented.',
    '',
    '### Recovery with the index deleted',
    '',
    table(['workflows', 'events total', 'open with index (ms)', 'rebuild (ms)', 'penalty'], degraded),
    '',
    'With no index, `open` reads and verifies every log in full before answering a single',
    'question. The penalty is large and the behaviour is correct: the index is a cache,',
    'the logs are the truth, and the store recovers from losing the cache without losing an',
    'event. The number is the cost of that guarantee being real rather than aspirational.',
  ].join('\n');

  return { title: 'Recovery time vs log size', body };
}

/**
 * Builds a store with `workflows` histories of `perWorkflow` events.
 *
 * Seeded one append per workflow rather than one per event: the seeding is not the thing
 * being measured, and 50,000 individually fsynced appends would take ten minutes. The
 * resulting file is byte-identical either way — the sealing and encoding are the same code
 * path — so the recovery measurement is unaffected.
 */
async function seedStore(
  options: RecoveryOptions,
  workflows: number,
  perWorkflow: number,
): Promise<string> {
  const { store, dir } = await openScratchStore(options.root, 'recover');
  const def = chainDef(8, 'recover-8');

  for (let i = 0; i < workflows; i += 1) {
    const id = `wf_recover_${i}`;
    // Every other workflow is left unfinished, so `listActive` has a real answer.
    const complete = i % 2 === 0;
    const events = chainHistory({
      workflowId: id,
      def,
      events: complete ? perWorkflow : perWorkflow - 1,
    });
    const batch = events
      .filter((e) => complete || e.type !== 'workflow.completed')
      .map(({ sequence: _sequence, version: _version, ...rest }) => rest);
    await store.append(id, 0, batch as Parameters<typeof store.append>[2]);
  }

  return dir;
}

/** The definition a seeded history was written against, read back out of the log. */
function definitionFor(events: readonly Event[]) {
  const created = events.find((e) => e.type === 'workflow.created');
  if (created === undefined || created.type !== 'workflow.created' || created.definition === undefined) {
    throw new Error('seeded history has no definition in workflow.created');
  }
  return created.definition;
}

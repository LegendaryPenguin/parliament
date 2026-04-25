/**
 * Benchmark 3 — optimistic concurrency under contention.
 *
 * This is the mechanism that replaces a distributed lock. N schedulers fold the same
 * history, all reach the same conclusion about which steps are ready, and all try to lease
 * them; the store's `expectedSequence` check means exactly one append lands and the rest
 * come back as `ConcurrencyError` or as a `null` from `LeaseManager.acquire`. There is no
 * lock service, no lease table, and nothing to be left holding a mutex when a process
 * dies.
 *
 * Two things have to be measured, and they are not the same question:
 *
 *  - **Does it hold?** Every step must be leased exactly once. That is asserted rather
 *    than reported: a benchmark of a mechanism that is quietly double-leasing is worse than
 *    no benchmark, so the run fails outright if the invariant breaks.
 *  - **Where does it degrade?** Optimistic concurrency trades a lock for wasted work. The
 *    loser of a race has already paid for a read and a fold, and it pays again on the
 *    retry. The conflict rate is therefore the cost of the design, and it should rise with
 *    N — if it did not, the contention being simulated would not be real.
 *
 * Both stores are measured, because the width of the conflict window is a property of the
 * store and not of the scheduler. The memory store resolves its reads on the microtask
 * queue, so the window between "read the head" and "append against it" is as narrow as it
 * can be; the file store reads a file and fsyncs a write, so the same window is milliseconds
 * wide and the conflict rate should be visibly worse. That difference is the finding.
 */

import { MemoryEventStore } from '../src/events/memory-store.js';
import { ConcurrencyError, type EventStore, type NewEvent } from '../src/events/types.js';
import { systemClock } from '../src/scheduler/clock.js';
import { LeaseManager } from '../src/scheduler/lease.js';
import { project } from '../src/state/project.js';
import { readySteps } from '../src/state/ready.js';
import type { StepRef, WorkflowDef } from '../src/workflow/types.js';
import { fanOutDef } from './fixtures.js';
import { openScratchStore } from './stores.js';
import { ms, rate, spread, summarize, table, type Section } from './harness.js';

/** Independent ready steps per workflow: the work the schedulers are competing over. */
const WIDTH = 24;
const RUNS = 5;
const SCHEDULER_COUNTS = [1, 2, 4, 8, 16];

export interface ContentionOptions {
  readonly root: string;
}

interface RaceResult {
  readonly granted: number;
  readonly lost: number;
  readonly attempts: number;
  readonly durationMs: number;
}

export async function run(options: ContentionOptions): Promise<Section> {
  const memory: Point[] = [];
  for (const n of SCHEDULER_COUNTS) {
    memory.push(await pointFor(n, () => Promise.resolve(new MemoryEventStore()), RUNS));
  }

  const file: Point[] = [];
  const fileCounts = [1, 2, 4, 8];
  for (const n of fileCounts) {
    file.push(
      await pointFor(n, async () => (await openScratchStore(options.root, 'race')).store, 3),
    );
  }

  const memoryRows = memory.map(toRow);
  const fileRows = file.map(toRow);

  // Whether the conflict count is exactly one loss per non-winning scheduler per lease, in
  // both stores. Checked rather than asserted in prose: if the two stores disagreed, the
  // explanation below would be wrong, and a confident paragraph attached to numbers that
  // contradict it is the specific failure this report is trying not to commit.
  const structural = [...memory, ...file].every(
    (p) => Math.abs(p.conflicts - (p.schedulers - 1) * WIDTH) < 0.5,
  );
  const slowest = memory[memory.length - 1] as Point;
  const fastest = memory[0] as Point;
  const fileSpan = `${ms(Math.min(...file.map((p) => p.wallMs)))}–${ms(Math.max(...file.map((p) => p.wallMs)))}`;

  const body = [
    `${WIDTH} independent ready steps in one workflow, N schedulers racing to lease them.`,
    'Each scheduler loops: read the history, fold it, ask `readySteps` what may run, call',
    '`LeaseManager.acquire` on the first candidate, and — win or lose — go round again',
    'against the fresh history. It stops when the fold offers nothing. No locks anywhere;',
    'the only arbitration is the store\'s `expectedSequence` check.',
    '',
    '**Every run asserts that each step was leased exactly once.** A run that violated it',
    'would throw rather than print, so the numbers below are only produced when the',
    'mechanism held.',
    '',
    '### Memory store',
    '',
    table(
      ['schedulers', 'wall (ms)', 'leases/sec', 'acquire calls', 'conflicts', 'conflict rate'],
      memoryRows,
    ),
    '',
    '### File store (fsync per append)',
    '',
    table(
      ['schedulers', 'wall (ms)', 'leases/sec', 'acquire calls', 'conflicts', 'conflict rate'],
      fileRows,
    ),
    '',
    '### Reading it',
    '',
    'At N=1 the conflict rate is zero and the acquire count equals the step count: with one',
    'scheduler there is nothing to race. As N rises, every lease costs exactly N attempts —',
    'one winner and N-1 losers, each of which has already paid for a read and a fold before',
    'discovering it lost. The conflict rate is therefore (N-1)/N: 50% at two schedulers, 94%',
    'at sixteen. Correct throughout, and wasteful in a way that grows with the fleet.',
    '',
    structural
      ? [
          '**The conflict rate is identical in both stores**, to the event. That is worth',
          'stating because the opposite would have been the natural guess: the file store\'s',
          'window between folding a history and appending against it is milliseconds rather',
          'than microseconds, so one would expect more losers to fit inside it. They do not,',
          'because the losers are synchronized by the same thing that creates them — every',
          'scheduler reads the same head, exactly one append wins, and the rest discover it',
          'together and retry together. The window width changes how long a round takes; it',
          'does not change how many schedulers are in it. A wider window would only raise the',
          'conflict rate if arrivals were independent of each other, which here they are not.',
        ].join('\n')
      : [
          'The conflict counts differ between the two stores, which means the window between',
          'folding and appending is wide enough in the file store for arrivals to desynchronize.',
          'Read the file-store table as the pessimistic case.',
        ].join('\n'),
    '',
    overlaps(memory).length > 0
      ? [
          `A caveat on the wall-clock column: ${overlaps(memory).join('; ')}. Those`,
          'configurations are not distinguishable by this benchmark, and reading an ordering into',
          'them would be reading noise. The conflict counts, which are integers and identical',
          'across runs, are the part of this table to trust.',
          '',
        ].join('\n')
      : '',
    `Wall-clock throughput degrades gently rather than collapsing: ${ms(fastest.wallMs)} ms at`,
    `one scheduler against ${ms(slowest.wallMs)} ms at ${slowest.schedulers} on the memory`,
    'store, for the same 24 leases. The wasted work is reads and folds, which are cheap; what',
    'the fleet pays is CPU, not latency. On the file store the wall time barely moves with N',
    `at all (${fileSpan} ms across the range) because the number of *successful* appends is`,
    'the same 24 either way, and fsyncs dominate everything else.',
    '',
    'So: correct at every N measured, efficient only at small N. This is a design for a',
    'handful of schedulers, not for fifty. Past that point the honest fix is not a lock — it',
    'is partitioning, so two schedulers rarely contend for the same workflow in the first',
    'place. Nothing in the current design prevents that, and the optimistic check stays as',
    'the safety net for the overlap at a partition boundary, which is the role it is good at.',
    '',
    'What this benchmark does **not** measure: schedulers in separate processes. Everything',
    'here shares one event loop, so a "concurrent" scheduler is one that interleaves at an',
    '`await`. That is the right model for the arbitration being tested — the store is the',
    'only shared state and its check is the only arbiter — but a real deployment adds network',
    'latency inside the conflict window and a store that can be written by a peer this',
    'process cannot see.',
  ].join('\n');

  return { title: 'Optimistic-concurrency contention', body };
}

interface Point {
  readonly schedulers: number;
  readonly wall: ReturnType<typeof summarize>;
  readonly wallMs: number;
  readonly attempts: number;
  readonly conflicts: number;
}

async function pointFor(
  schedulers: number,
  make: () => Promise<EventStore>,
  runs: number,
): Promise<Point> {
  const results: RaceResult[] = [];
  for (let i = 0; i < runs; i += 1) {
    results.push(await race(schedulers, await make(), `wf_race_${schedulers}_${i}`));
  }

  const wall = summarize(results.map((r) => r.durationMs));
  return {
    schedulers,
    wall,
    wallMs: wall.medianMs,
    attempts: results.reduce((a, r) => a + r.attempts, 0) / results.length,
    conflicts: results.reduce((a, r) => a + r.lost, 0) / results.length,
  };
}

/**
 * Adjacent scheduler counts whose observed wall-clock ranges overlap.
 *
 * Checked rather than eyeballed, because the report promises to say when two rows cannot be
 * told apart and a promise like that is only kept if something enforces it. Overlap here is
 * routine at small N — the whole race is a couple of milliseconds — and a sentence admitting
 * it is worth more than a table that invites a reader to rank noise.
 */
function overlaps(points: readonly Point[]): string[] {
  const out: string[] = [];
  for (let i = 1; i < points.length; i += 1) {
    const a = points[i - 1] as Point;
    const b = points[i] as Point;
    if (a.wall.minMs <= b.wall.maxMs && b.wall.minMs <= a.wall.maxMs) {
      out.push(`N=${a.schedulers} and N=${b.schedulers} overlap in range`);
    }
  }
  return out;
}

function toRow(point: Point): string[] {
  return [
    String(point.schedulers),
    spread(point.wall),
    rate(WIDTH, point.wallMs),
    point.attempts.toFixed(1),
    point.conflicts.toFixed(1),
    `${((point.conflicts / Math.max(1, point.attempts)) * 100).toFixed(0)}%`,
  ];
}

/**
 * Runs one race and returns what it cost.
 *
 * The schedulers are started with `Promise.all` and share the one event loop, which is
 * exactly the interleaving that matters: each `await` on a store read is a point at which
 * another scheduler can get in, decide against the same history, and append first. A
 * benchmark that ran them sequentially would report zero conflicts and prove nothing.
 */
async function race(schedulers: number, store: EventStore, workflowId: string): Promise<RaceResult> {
  const def = fanOutDef(WIDTH, `race-${WIDTH}`);
  await seed(store, workflowId, def);

  const leases = Array.from(
    { length: schedulers },
    () => new LeaseManager({ store, clock: systemClock, leaseDurationMs: 60_000 }),
  );

  const granted = new Map<string, string>();
  let attempts = 0;
  let lost = 0;

  const started = performance.now();
  await Promise.all(
    leases.map(async (manager, index) => {
      const workerId = `scheduler-${index}`;
      // A loser retries against the fresh history rather than crossing the step off, which
      // is what a real scheduler does and is the reason the conflict count is the cost of
      // the design rather than an artefact of this loop. Crossing it off would be wrong in
      // a way that quietly breaks the benchmark: a conflict caused by *another* step's
      // lease leaves this step unleased, and a scheduler that gave up on it could leave a
      // step nobody takes.
      const cap = WIDTH * schedulers * 20;

      for (let iteration = 0; ; iteration += 1) {
        if (iteration > cap) {
          throw new Error(`scheduler ${workerId} made no progress in ${cap} iterations`);
        }
        const events = await store.read(workflowId);
        const projection = project(def, events);
        const now = systemClock.now();
        const candidate = readySteps(def, projection, now)[0];
        if (candidate === undefined) return;

        const ref: StepRef = {
          workflowId,
          step: candidate.step,
          attempt: Math.max(1, candidate.attempt),
        };

        attempts += 1;
        try {
          const lease = await manager.acquire(ref, workerId);
          if (lease === null) {
            lost += 1;
            continue;
          }
          const previous = granted.get(candidate.step);
          if (previous !== undefined) {
            // The invariant the whole mechanism exists to provide. Throwing rather than
            // counting: a double lease is not a slow result, it is a wrong one, and a
            // report that recorded it as a statistic would be reporting a broken system
            // as a benchmark.
            throw new Error(
              `step ${candidate.step} was leased twice: by ${previous} and by ${workerId}`,
            );
          }
          granted.set(candidate.step, workerId);
        } catch (err) {
          if (err instanceof ConcurrencyError) {
            lost += 1;
            continue;
          }
          throw err;
        }
      }
    }),
  );
  const durationMs = performance.now() - started;

  if (granted.size !== WIDTH) {
    throw new Error(`expected ${WIDTH} leases, got ${granted.size}`);
  }

  return { granted: granted.size, lost, attempts, durationMs };
}

async function seed(store: EventStore, workflowId: string, def: WorkflowDef): Promise<void> {
  const at = Date.now();
  const batch: NewEvent[] = [
    {
      type: 'workflow.created',
      workflowId,
      at,
      workflow: def.name,
      definitionVersion: def.version,
      definition: def,
      input: null,
    },
    ...def.nodes.map(
      (node): NewEvent => ({
        type: 'step.scheduled',
        workflowId,
        at,
        step: node.name,
        attempt: 1,
        notBefore: at,
      }),
    ),
  ];
  await store.append(workflowId, 0, batch);
}

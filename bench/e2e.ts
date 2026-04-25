/**
 * Benchmark 5 — end-to-end latency, and where the time goes.
 *
 * A real `Scheduler` on the real system clock, driving a diamond DAG — fetch, two parallel
 * analyses, a join, a finalize — from submission to `workflow.completed`. The dispatcher
 * stands in for a tool call with a fixed sleep, so the work the workflow does is a known
 * quantity and everything else in the measurement is overhead.
 *
 * The breakdown is the point. A single "workflow took 40 ms" number cannot be acted on,
 * because the three things it could be made of have completely different fixes:
 *
 *  - **tool time** — the work itself. Nothing the control plane can do about it.
 *  - **store time** — reads and appends. Fixed by batching or by a faster store.
 *  - **scheduling overhead** — folds, lease arbitration, and above all *waiting for the
 *    next tick*. Fixed by tuning the loop, and the measurement below shows that this is
 *    where almost all of the avoidable latency is.
 *
 * Instrumentation is a decorator around the `EventStore` the scheduler is given, which is
 * why it can be done without touching `src`: the scheduler depends on the interface, so a
 * timing wrapper is a legal implementation of it. Two tick intervals are measured because
 * the tick interval is the one knob that trades latency against idle CPU, and a reader
 * should be able to see the size of that trade rather than be told about it.
 */

import { MemoryEventStore } from '../src/events/memory-store.js';
import type { Event, EventStore, NewEvent, Sequence } from '../src/events/types.js';
import { systemClock } from '../src/scheduler/clock.js';
import { Scheduler, type DispatchRequest } from '../src/scheduler/scheduler.js';
import type { WorkflowId } from '../src/workflow/types.js';
import { diamondDef } from './fixtures.js';
import { ms, summarize, table, type Section } from './harness.js';

/** Simulated tool duration per step. Long enough to be measurable, short enough to loop. */
const TOOL_MS = 5;
const STEPS = 5;
const RUNS = 7;

export interface E2eOptions {
  /**
   * Measured cost of one durable append, from benchmark 2.
   *
   * Passed in rather than hard-coded, so the extrapolation to a file-backed deployment is
   * arithmetic over a number this run actually measured instead of a constant someone typed
   * once and nobody re-checked.
   */
  readonly filePerAppendMs: number;
}

export async function run(options: E2eOptions): Promise<Section> {
  const rows: string[][] = [];
  const measured: Measured[] = [];

  for (const tickMs of [50, 5, 1]) {
    const result = await latency(tickMs, TOOL_MS);
    measured.push(result);
    rows.push([
      String(tickMs),
      ms(result.wall.medianMs),
      ms(result.wall.minMs),
      ms(result.wall.maxMs),
      ms(result.criticalToolMs),
      ms(result.toolMs),
      ms(result.appendMs),
      ms(result.readMs),
      ms(result.wall.medianMs - result.criticalToolMs - result.appendMs - result.readMs),
      String(result.appends),
      String(result.reads),
    ]);
  }

  const zeroTool = await latency(5, 0);
  const slow = measured.find((m) => m.tickMs === 50) as Measured;
  const fast = measured.find((m) => m.tickMs === 1) as Measured;

  const body = [
    'A real `Scheduler` on the system clock, memory store, diamond DAG:',
    '`fetch -> {classify, score} -> join -> finalize`. The dispatcher sleeps',
    `${TOOL_MS} ms per step to stand in for a tool call, so ${STEPS} steps of simulated work`,
    `is ${STEPS * TOOL_MS} ms with a critical path of ${4 * TOOL_MS} ms (the two analyses`,
    'overlap). Timed from `submit` returning to the `workflow.completed` event arriving on',
    'the store\'s own `subscribe`.',
    '',
    'Store time is measured by a decorator around the `EventStore` handed to the scheduler —',
    'legal precisely because the scheduler depends on the interface and not on a class.',
    '',
    '### Latency against tick interval',
    '',
    table(
      [
        'tick (ms)',
        'wall median',
        'min',
        'max',
        'tool (path)',
        'tool (sum)',
        'appends',
        'reads',
        'unaccounted',
        '# appends',
        '# reads',
      ],
      rows,
    ),
    '',
    `All times in milliseconds, over ${RUNS} runs. **tool (path)** is the tool time on the`,
    'critical path — `fetch`, then the slower of the two analyses, then `join`, then',
    '`finalize` — and **tool (sum)** is the total across all five steps. The two differ',
    'because the analyses run concurrently, and only the first is comparable with wall',
    'time. Subtracting the sum instead, which is the easy mistake, produces a negative',
    '"unaccounted" column and an unreadable table: that was the first version of this',
    'benchmark, and it was wrong.',
    '',
    '### Where the time goes',
    '',
    `At a 50 ms tick the workflow takes ${ms(slow.wall.medianMs)} ms to do`,
    `${ms(slow.criticalToolMs)} ms of critical-path work. At a 1 ms tick the same workflow takes`,
    `${ms(fast.wall.medianMs)} ms. The difference is not computation — the append and read`,
    'totals barely move between the two rows — it is time spent waiting for the loop to come',
    'round. The scheduler re-ticks immediately when a step completes, so the chain after the',
    'first dispatch is event-driven; what the tick interval buys is the delay before the',
    '*first* dispatch and the delay after any retry backoff.',
    '',
    `Store work is real but small on the memory store: ${ms(fast.appendMs)} ms of appends`,
    `across ${fast.appends} appends and ${ms(fast.readMs)} ms of reads across ${fast.reads}`,
    'reads. Note the read count: every scheduler decision reads the whole history, and a',
    'five-step workflow does it dozens of times. On the file store, at the',
    `${ms(options.filePerAppendMs)} ms per append benchmark 2 measured on this machine, those`,
    `${fast.appends} appends alone would cost about`,
    `${ms(fast.appends * options.filePerAppendMs)} ms — which is the honest answer to "what`,
    'does this cost in production": the tick interval stops being the dominant term and the',
    'fsyncs take over. That is an extrapolation, not a measurement; the two numbers it',
    'multiplies are both measured in this run.',
    '',
    `With the tool sleep removed entirely the same workflow completes in`,
    `${ms(zeroTool.wall.medianMs)} ms (5 ms tick), which is the floor this control plane`,
    'imposes on a workflow that does no work at all.',
    '',
    '### What this benchmark cannot tell you',
    '',
    'It runs one workflow at a time on one scheduler with an in-memory store, so it measures',
    'latency and not capacity. It says nothing about what happens when a hundred workflows',
    'tick at once, which is where the projection cost from benchmark 1 and the contention',
    'cost from benchmark 3 would start to interact — and that interaction is the thing most',
    'likely to be the real limit. Measuring it needs a load generator this does not have.',
  ].join('\n');

  return { title: 'End-to-end workflow latency', body };
}

interface Measured {
  readonly tickMs: number;
  readonly wall: ReturnType<typeof summarize>;
  /** Sum of every step's dispatch duration. Exceeds wall time whenever branches overlap. */
  readonly toolMs: number;
  /** Dispatch duration along the longest path, which is the part wall time must contain. */
  readonly criticalToolMs: number;
  readonly appendMs: number;
  readonly readMs: number;
  readonly appends: number;
  readonly reads: number;
}

async function latency(tickMs: number, toolMs: number): Promise<Measured> {
  const def = diamondDef(`e2e-${tickMs}-${toolMs}`);
  const samples: number[] = [];
  let tool = 0;
  let critical = 0;
  let append = 0;
  let read = 0;
  let appends = 0;
  let reads = 0;

  // Two discarded runs: the first execution of the scheduler's hot path is interpreted
  // rather than compiled, and including it would put the compiler in the median.
  for (let run = 0; run < RUNS + 2; run += 1) {
    const inner = new MemoryEventStore();
    const timed = new TimingStore(inner);
    let toolTime = 0;
    const perStep = new Map<string, number>();

    const scheduler = new Scheduler({
      store: timed,
      clock: systemClock,
      tickIntervalMs: tickMs,
      maxInFlight: 8,
      dispatch: async (req: DispatchRequest) => {
        const started = performance.now();
        if (toolMs > 0) await sleep(toolMs, req.signal);
        const elapsed = performance.now() - started;
        toolTime += elapsed;
        // Keyed by step and summed, so a retried step contributes every attempt it spent.
        perStep.set(req.node.name, (perStep.get(req.node.name) ?? 0) + elapsed);
        return { step: req.node.name };
      },
    });

    const done = completion(timed);
    scheduler.startLoop();

    const started = performance.now();
    const id = await scheduler.start(def, { input: { n: run } });
    await done(id);
    const wall = performance.now() - started;
    scheduler.stop();

    if (run >= 2) {
      samples.push(wall);
      tool += toolTime;
      critical += criticalPathOf(perStep);
      append += timed.appendMs;
      read += timed.readMs;
      appends += timed.appends;
      reads += timed.reads;
    }
  }

  const divisor = samples.length;
  return {
    tickMs,
    wall: summarize(samples),
    toolMs: tool / divisor,
    criticalToolMs: critical / divisor,
    appendMs: append / divisor,
    readMs: read / divisor,
    appends: Math.round(appends / divisor),
    reads: Math.round(reads / divisor),
  };
}

/**
 * Tool time along the diamond's longest path.
 *
 * Hard-coded to this graph's shape rather than derived from the definition, and that is the
 * limit of it: a general critical-path computation over dispatch durations belongs in the
 * trace builder (`src/api/trace.ts` already computes one over the event log) and
 * duplicating it here would mean two implementations that can disagree. For a five-node
 * diamond the shape is `fetch -> max(classify, score) -> join -> finalize`, which is short
 * enough to write down and check by eye.
 */
function criticalPathOf(perStep: ReadonlyMap<string, number>): number {
  const at = (name: string): number => perStep.get(name) ?? 0;
  return at('fetch') + Math.max(at('classify'), at('score')) + at('join') + at('finalize');
}

/**
 * Resolves when a workflow's completion is appended.
 *
 * Through `EventStore.subscribe` rather than by polling the projection, because polling
 * would add its own interval to every measurement and the thing being measured is latency.
 * The subscription is taken before the workflow is submitted: a completion that lands
 * between submission and subscription would otherwise hang the benchmark forever.
 */
function completion(store: EventStore): (id: WorkflowId) => Promise<void> {
  const finished = new Set<WorkflowId>();
  const waiters = new Map<WorkflowId, () => void>();

  store.subscribe((events) => {
    for (const event of events) {
      if (event.type !== 'workflow.completed') continue;
      finished.add(event.workflowId);
      waiters.get(event.workflowId)?.();
    }
  });

  return (id) =>
    new Promise<void>((resolve) => {
      if (finished.has(id)) {
        resolve();
        return;
      }
      waiters.set(id, resolve);
    });
}

/**
 * An `EventStore` that times what passes through it.
 *
 * Delegation rather than subclassing, so it works for any implementation and cannot
 * accidentally skip a method the real store relies on — the compiler rejects a missing one.
 */
class TimingStore implements EventStore {
  appendMs = 0;
  readMs = 0;
  appends = 0;
  reads = 0;

  constructor(private readonly inner: EventStore) {}

  async append(
    workflowId: WorkflowId,
    expectedSequence: Sequence,
    events: readonly NewEvent[],
  ): Promise<readonly Event[]> {
    const started = performance.now();
    try {
      return await this.inner.append(workflowId, expectedSequence, events);
    } finally {
      // Timed in a `finally` so a rejected append — a lost lease race, which is normal —
      // still counts the work it did. Excluding failures would flatter the store.
      this.appendMs += performance.now() - started;
      this.appends += 1;
    }
  }

  async read(workflowId: WorkflowId, after?: Sequence): Promise<readonly Event[]> {
    const started = performance.now();
    try {
      return await this.inner.read(workflowId, after);
    } finally {
      this.readMs += performance.now() - started;
      this.reads += 1;
    }
  }

  head(workflowId: WorkflowId): Promise<Sequence> {
    return this.inner.head(workflowId);
  }

  list(): Promise<readonly WorkflowId[]> {
    return this.inner.list();
  }

  listActive(): Promise<readonly WorkflowId[]> {
    return this.inner.listActive();
  }

  subscribe(listener: (events: readonly Event[]) => void): () => void {
    return this.inner.subscribe(listener);
  }
}

function sleep(msDelay: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, msDelay);
    // A cancelled or abandoned step must not hold the timer, or a benchmark that stops its
    // scheduler mid-run would keep the process alive for the length of the sleep.
    signal.addEventListener('abort', () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
  });
}

/**
 * Measurement plumbing shared by every benchmark.
 *
 * Two rules are enforced here rather than left to each benchmark, because both are easy
 * to get wrong in a way that produces a number nobody can challenge:
 *
 *  - **Never report one run.** A single timing on a laptop is a sample of the machine's
 *    mood: a CPU frequency step, a background indexer, or one unlucky garbage collection
 *    moves it by more than most of the effects being measured. Everything here returns a
 *    distribution and the report prints the spread next to the median, so a reader can
 *    see when two configurations are not actually distinguishable.
 *  - **Warm up, and say so.** The first iterations of anything in V8 run in the
 *    interpreter before the optimizing compiler has seen the function, so including them
 *    measures the compiler as much as the code. They are discarded — but discarding them
 *    silently would hide that the steady-state number does not describe the first call,
 *    which is the one a cold control plane actually makes.
 *
 * The median is reported rather than the mean. The distributions here are right-skewed
 * (a stall makes a run slower, nothing makes it faster), so the mean tracks the worst
 * outlier and the median tracks the typical run.
 */

import { execFileSync } from 'node:child_process';
import { cpus, totalmem } from 'node:os';

export interface Stats {
  readonly runs: number;
  readonly medianMs: number;
  readonly meanMs: number;
  readonly stddevMs: number;
  readonly minMs: number;
  readonly maxMs: number;
  /** Every sample, so a benchmark can report the raw numbers when the shape matters. */
  readonly samplesMs: readonly number[];
}

export interface MeasureOptions {
  /** Timed iterations. */
  readonly runs?: number;
  /** Discarded iterations, run first. */
  readonly warmup?: number;
}

/**
 * Times `fn` repeatedly.
 *
 * `fn` is awaited, and the clock is `performance.now()` rather than `Date.now()`: the
 * benchmarks here include operations in the tens of microseconds, and `Date.now()` has
 * millisecond granularity, so it would report many of them as zero.
 */
export async function measure(fn: () => Promise<void> | void, options: MeasureOptions = {}): Promise<Stats> {
  const runs = options.runs ?? 7;
  const warmup = options.warmup ?? 2;

  for (let i = 0; i < warmup; i += 1) await fn();

  const samples: number[] = [];
  for (let i = 0; i < runs; i += 1) {
    const started = performance.now();
    await fn();
    samples.push(performance.now() - started);
  }
  return summarize(samples);
}

export function summarize(samplesMs: readonly number[]): Stats {
  if (samplesMs.length === 0) {
    throw new Error('cannot summarize zero samples');
  }
  const sorted = [...samplesMs].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  const median =
    sorted.length % 2 === 1
      ? (sorted[mid] as number)
      : ((sorted[mid - 1] as number) + (sorted[mid] as number)) / 2;
  const mean = sorted.reduce((a, b) => a + b, 0) / sorted.length;
  // Population standard deviation: these are all the samples taken, not a sample of a
  // larger set, so there is no estimation correction to apply.
  const variance = sorted.reduce((acc, v) => acc + (v - mean) ** 2, 0) / sorted.length;

  return {
    runs: sorted.length,
    medianMs: median,
    meanMs: mean,
    stddevMs: Math.sqrt(variance),
    minMs: sorted[0] as number,
    maxMs: sorted[sorted.length - 1] as number,
    samplesMs: sorted,
  };
}

/** Milliseconds, with enough significant figures to be comparable but not invented. */
export function ms(value: number): string {
  if (value >= 100) return value.toFixed(0);
  if (value >= 10) return value.toFixed(1);
  if (value >= 1) return value.toFixed(2);
  return value.toFixed(3);
}

/** Operations per second, derived from a duration and a count. */
export function rate(count: number, durationMs: number): string {
  if (durationMs <= 0) return 'n/a';
  const perSecond = (count / durationMs) * 1000;
  if (perSecond >= 1000) return `${Math.round(perSecond).toLocaleString('en-US')}`;
  return perSecond.toFixed(1);
}

/** The median with its observed range, so a reader can see when a difference is noise. */
export function spread(stats: Stats): string {
  return `${ms(stats.medianMs)} (${ms(stats.minMs)}–${ms(stats.maxMs)}, sd ${ms(stats.stddevMs)})`;
}

export function table(headers: readonly string[], rows: readonly (readonly string[])[]): string {
  const widths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((r) => (r[i] ?? '').length)),
  );
  const line = (cells: readonly string[]): string =>
    `| ${cells.map((c, i) => (c ?? '').padEnd(widths[i] as number)).join(' | ')} |`;
  return [
    line(headers),
    `| ${widths.map((w) => '-'.repeat(w)).join(' | ')} |`,
    ...rows.map(line),
  ].join('\n');
}

/** One benchmark's contribution to the report. */
export interface Section {
  readonly title: string;
  readonly body: string;
}

export interface Environment {
  readonly node: string;
  readonly platform: string;
  readonly cpu: string;
  readonly cores: number;
  readonly memoryGb: string;
  /** Whether the filesystem the file-store benchmarks ran on is solid state. */
  readonly storage: string;
}

/**
 * Describes the machine.
 *
 * Collected by running the probes rather than written into the report by hand, because a
 * hand-written hardware line is the first thing to go stale when the benchmark is rerun
 * somewhere else — and a storage line that says SSD on a machine with a spinning disk
 * would invalidate the one number the file store exists to explain.
 */
export function environment(dataPath: string): Environment {
  return {
    node: process.version,
    platform: `${process.platform} ${process.arch}`,
    cpu: probe(['sysctl', ['-n', 'machdep.cpu.brand_string']])?.trim() ?? cpus()[0]?.model ?? 'unknown',
    cores: cpus().length,
    memoryGb: (totalmem() / 1024 ** 3).toFixed(0),
    storage: storageOf(dataPath),
  };
}

/**
 * Whether `path` lives on solid state.
 *
 * Reported as "unknown" when it cannot be established. Guessing SSD because the year is
 * late would be the kind of invented number this report exists not to contain: the file
 * store's throughput is almost entirely one fsync per append, so the answer changes the
 * meaning of every number in that section by an order of magnitude.
 */
function storageOf(path: string): string {
  // `diskutil info` wants a device or a mount point, not an arbitrary directory, so the
  // device is resolved with `df` first. Passing the path straight in silently answers
  // "could not find disk" and the report would say "unknown" on a machine where the answer
  // was available all along.
  const device = probe(['df', [path]])
    ?.split('\n')[1]
    ?.split(/\s+/)[0];
  const info = probe(['diskutil', ['info', device ?? path]]);
  if (info === undefined) return 'unknown';
  const solid = /Solid State:\s*(\w+)/.exec(info)?.[1];
  const media = /Device \/ Media Name:\s*(.+)/.exec(info)?.[1]?.trim();
  const filesystem = /Type \(Bundle\):\s*(\w+)/.exec(info)?.[1];
  if (solid === undefined) return 'unknown';
  const parts = [solid === 'Yes' ? 'SSD' : 'not solid state'];
  if (media !== undefined) parts.push(media);
  if (filesystem !== undefined) parts.push(filesystem);
  return parts.join(', ');
}

function probe([command, args]: [string, readonly string[]]): string | undefined {
  try {
    return execFileSync(command, [...args], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
  } catch {
    return undefined;
  }
}

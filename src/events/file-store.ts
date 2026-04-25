/**
 * A durable event store on the local filesystem.
 *
 * One append-only JSONL file per workflow, plus a side index so that recovery does
 * not have to read every event of every workflow just to find out which ones are
 * still running.
 *
 * Four properties are load-bearing and each one costs something:
 *
 *  - **fsync before resolving.** `append` does not resolve until the bytes are on the
 *    platter. A workflow that is told its event is durable and then loses it is the
 *    single failure this whole design exists to prevent: the log *is* the state, so a
 *    lost event is not a lost notification, it is a state that never existed.
 *  - **A torn final line is truncated, not fatal.** A crash between `write` and the
 *    newline leaves a partial last line. That is the expected shape of a crash, so it
 *    is repaired on read. Anything else — a bad checksum, a sequence gap, a partial
 *    line in the *middle* of a file — is corruption and is refused loudly.
 *  - **A checksum per line.** Silent bit rot that still parses as JSON is worse than
 *    a parse error, because it produces a plausible event. The digest covers the exact
 *    bytes of the serialized event, so any single-byte change is caught.
 *  - **The index is a cache, never the truth.** It can be deleted at any moment and
 *    rebuilt from the logs. Every open reconciles it against the files on disk, so a
 *    crash between the log write and the index write cannot lose events.
 */

import { createHash } from 'node:crypto';
import { open, mkdir, readFile, readdir, rename, stat, truncate } from 'node:fs/promises';
import * as path from 'node:path';

import { sealBatch, isTerminalEvent } from './common.js';
import type { Event, EventStore, NewEvent, Sequence } from './types.js';
import { AppendObservers, type AppendListener } from './observers.js';
import type { WorkflowId } from '../workflow/types.js';

/** The file could not be read as a history. Distinct from a torn tail, which is repaired. */
export class LogCorruptionError extends Error {
  constructor(
    readonly file: string,
    readonly line: number,
    detail: string,
  ) {
    super(`corrupt event log ${file} at line ${line}: ${detail}`);
    this.name = 'LogCorruptionError';
  }
}

const LOG_DIR = 'workflows';
const INDEX_FILE = 'index.json';
const INDEX_VERSION = 1;

/**
 * A line is `{"checksum":"<16 hex>","event":<event json>}`.
 *
 * The digest covers the raw text of the event, which is why reading extracts that
 * text with a pattern instead of re-serializing the parsed object: `JSON.stringify`
 * of a parsed value is not guaranteed to reproduce the original bytes, so verifying
 * against a re-serialization would reject valid lines and accept some invalid ones.
 */
const LINE_PATTERN = /^\{"checksum":"([0-9a-f]{16})","event":(.+)\}$/;

function checksum(text: string): string {
  return createHash('sha256').update(text, 'utf8').digest('hex').slice(0, 16);
}

export function encodeLine(event: Event): string {
  const json = JSON.stringify(event);
  return `{"checksum":"${checksum(json)}","event":${json}}\n`;
}

interface IndexEntry {
  head: Sequence;
  terminal: boolean;
  /**
   * File size when the index was last written. Compared against the real size on
   * open: a mismatch means the index is behind (or ahead of) the log, and the log
   * wins. This is what makes the index safe to write *after* the fsync of the data.
   */
  bytes: number;
}

interface IndexFile {
  readonly version: number;
  readonly workflows: Record<string, IndexEntry>;
}

/**
 * Maps a workflow id to a filename.
 *
 * `encodeURIComponent` escapes the separators, so no id can escape the directory.
 * It does not fold case, which a case-insensitive filesystem does — two ids differing
 * only in case would land in one file. Rather than hash the name and lose legibility,
 * every event read back is checked against the id that was asked for, so such a
 * collision surfaces as corruption instead of as silently interleaved histories.
 */
function fileNameFor(workflowId: WorkflowId): string {
  return `${encodeURIComponent(workflowId)}.jsonl`;
}

export class FileEventStore implements EventStore {
  private readonly index = new Map<WorkflowId, IndexEntry>();
  private readonly observers = new AppendObservers();

  /** Serializes appends per workflow: two in-flight appends must not both win. */
  private readonly locks = new Map<WorkflowId, Promise<unknown>>();

  /** Serializes index writes across workflows, and names each temp file; see `writeIndex`. */
  private indexChain: Promise<void> = Promise.resolve();
  private indexQueued = false;
  private indexWrites = 0;

  private constructor(private readonly root: string) {}

  static async open(root: string): Promise<FileEventStore> {
    const store = new FileEventStore(path.resolve(root));
    await mkdir(path.join(store.root, LOG_DIR), { recursive: true });
    await store.loadIndex();
    return store;
  }

  private get logDir(): string {
    return path.join(this.root, LOG_DIR);
  }

  private fileFor(workflowId: WorkflowId): string {
    return path.join(this.logDir, fileNameFor(workflowId));
  }

  async append(
    workflowId: WorkflowId,
    expectedSequence: Sequence,
    events: readonly NewEvent[],
  ): Promise<readonly Event[]> {
    return this.withLock(workflowId, async () => {
      const entry = this.index.get(workflowId);
      const head = entry?.head ?? 0;

      // Whole batch validated before a byte is written.
      const sealed = sealBatch(workflowId, head, expectedSequence, events);
      if (sealed.length === 0) return [];

      const file = this.fileFor(workflowId);
      const isNew = entry === undefined;
      const text = sealed.map(encodeLine).join('');

      const handle = await open(file, 'a');
      try {
        // One write for the whole batch: a batch split across writes could be torn
        // in the middle, and a half-applied transition is a state no fold can read.
        await handle.write(text, null, 'utf8');
        await handle.sync();
      } finally {
        await handle.close();
      }
      // A new file's directory entry is itself only durable after the directory is
      // synced; without this the data survives but the name may not.
      if (isNew) await this.syncDir();

      const last = sealed[sealed.length - 1];
      const bytes = (await stat(file)).size;
      this.index.set(workflowId, {
        head: last ? last.sequence : head,
        terminal: (entry?.terminal ?? false) || sealed.some((e) => isTerminalEvent(e.type)),
        bytes,
      });
      await this.writeIndex();

      return sealed;
    }).then((sealed) => {
      // Outside `withLock`, so the critical section covers the durable write and
      // nothing else.
      //
      // Not a deadlock guard, though it looks like one: a listener's natural reaction to
      // "the log grew" is to read the log, and `read` takes this same per-workflow lock.
      // That is still safe inside the lock today, because `notify` is synchronous and
      // this store never awaits a listener — the listener's `read` simply queues behind
      // the append that is already finishing. Verified by moving the call inside and
      // watching the tests still pass.
      //
      // It becomes load-bearing the moment a store awaits its observers, which is the
      // obvious way to add a durable or remote subscriber. Keeping the notification out
      // here means that change does not have to also rediscover the lock.
      this.observers.notify(sealed);
      return sealed;
    });
  }

  subscribe(listener: AppendListener): () => void {
    return this.observers.add(listener);
  }

  async read(workflowId: WorkflowId, after: Sequence = 0): Promise<readonly Event[]> {
    // Under the same lock as `append`, because reading can *repair* a torn tail and a
    // repair racing a write would truncate bytes the writer is in the middle of
    // acknowledging as durable.
    const events = await this.withLock(workflowId, () => this.loadFile(workflowId));
    return after > 0 ? events.filter((e) => e.sequence > after) : events;
  }

  async head(workflowId: WorkflowId): Promise<Sequence> {
    return this.index.get(workflowId)?.head ?? 0;
  }

  async list(): Promise<readonly WorkflowId[]> {
    return [...this.index.keys()];
  }

  async listActive(): Promise<readonly WorkflowId[]> {
    const active: WorkflowId[] = [];
    for (const [id, entry] of this.index) {
      if (!entry.terminal) active.push(id);
    }
    return active;
  }

  /**
   * Rebuilds the index by reading every log.
   *
   * Expensive, and that is acceptable because it is the escape hatch: the index may
   * be deleted, stale or garbage, and the logs are still the truth. Called
   * automatically when the index cannot be trusted.
   */
  async rebuildIndex(): Promise<void> {
    this.index.clear();
    for (const name of await this.logFiles()) {
      const workflowId = decodeURIComponent(name.slice(0, -'.jsonl'.length));
      const events = await this.withLock(workflowId, () => this.loadFile(workflowId));
      if (events.length === 0) continue;
      const last = events[events.length - 1];
      this.index.set(workflowId, {
        head: last ? last.sequence : 0,
        terminal: events.some((e) => isTerminalEvent(e.type)),
        bytes: (await stat(this.fileFor(workflowId))).size,
      });
    }
    await this.writeIndex();
  }

  private async logFiles(): Promise<string[]> {
    const names = await readdir(this.logDir);
    return names.filter((n) => n.endsWith('.jsonl')).sort();
  }

  /**
   * Loads the index and reconciles it with the logs.
   *
   * Reconciliation only stats files, so the common case stays cheap: a workflow whose
   * size matches the index is not read at all. Only a file whose size disagrees with
   * the index — the signature of a crash between the data fsync and the index write —
   * is re-scanned.
   */
  private async loadIndex(): Promise<void> {
    this.index.clear();

    let parsed: IndexFile | undefined;
    try {
      const raw = await readFile(path.join(this.root, INDEX_FILE), 'utf8');
      const candidate = JSON.parse(raw) as IndexFile;
      if (candidate.version === INDEX_VERSION && candidate.workflows !== null && typeof candidate.workflows === 'object') {
        parsed = candidate;
      }
    } catch {
      // Missing or unreadable index: the logs are the truth, so rebuild below.
      parsed = undefined;
    }

    if (parsed === undefined) {
      await this.rebuildIndex();
      return;
    }

    const files = new Map<string, string>();
    for (const name of await this.logFiles()) {
      files.set(decodeURIComponent(name.slice(0, -'.jsonl'.length)), name);
    }

    let dirty = false;
    for (const [workflowId, name] of files) {
      const entry = parsed.workflows[workflowId];
      const bytes = (await stat(path.join(this.logDir, name))).size;
      if (entry !== undefined && entry.bytes === bytes) {
        this.index.set(workflowId, { ...entry, bytes });
        continue;
      }
      dirty = true;
      const events = await this.withLock(workflowId, () => this.loadFile(workflowId));
      if (events.length === 0) continue;
      const last = events[events.length - 1];
      this.index.set(workflowId, {
        head: last ? last.sequence : 0,
        terminal: events.some((e) => isTerminalEvent(e.type)),
        // Re-read after loadFile, which may have truncated a torn tail.
        bytes: (await stat(path.join(this.logDir, name))).size,
      });
    }
    // An index entry with no log file is a leftover, not a workflow.
    if (Object.keys(parsed.workflows).length !== files.size) dirty = true;

    if (dirty) await this.writeIndex();
  }

  /**
   * Persists the index, serialized against every other index write.
   *
   * The serialization is a bug fix, not tidiness. `withLock` serializes appends per
   * *workflow*, and the index is the one piece of state shared by all of them, so two
   * appends to two different workflows ran this concurrently: both wrote the same
   * `index.json.tmp`, interleaving their bytes, and then both renamed it — the second
   * rename failing with ENOENT because the first had already moved the file away. That
   * rejection surfaced out of `append`, *after* the event's own fsync, so the caller was
   * told its append had failed for an event that was already durable. A lease acquired
   * that way is recorded in the log and believed lost by the worker holding it, which is
   * the one thing the whole leasing mechanism exists to prevent.
   *
   * Two changes close it. The temp file is named per write, so no two writers can ever be
   * holding the same one. And writes are chained, with at most one queued behind the one in
   * flight: a third concurrent append needs no write of its own, because the queued write
   * serializes `this.index` when it *runs* and by then contains that append's entry too.
   * Without the coalescing, a burst of N appends would cost N whole-index fsyncs to reach
   * one final state.
   *
   * A failure here still rejects the append, even though the event is durable by then. That
   * is deliberate: with the race fixed, the remaining ways this can fail are a full or
   * broken filesystem, and a store that swallowed those would report success while silently
   * ceasing to persist the index — recoverable, but only by a rebuild nobody knew to run.
   */
  private writeIndex(): Promise<void> {
    if (this.indexQueued) return this.indexChain;
    this.indexQueued = true;
    const flush = (): Promise<void> => {
      this.indexQueued = false;
      return this.flushIndex();
    };
    // Chained off the settled outcome, so one failed index write does not wedge the queue.
    const next = this.indexChain.then(flush, flush);
    this.indexChain = next.catch(() => undefined);
    return next;
  }

  private async flushIndex(): Promise<void> {
    const workflows: Record<string, IndexEntry> = {};
    for (const [id, entry] of this.index) workflows[id] = entry;
    const body: IndexFile = { version: INDEX_VERSION, workflows };

    // Written to a temp file and renamed so a crash mid-write leaves the previous
    // index intact rather than a half-parsed one. Cheap insurance even though a
    // garbage index is recoverable by rebuilding.
    const target = path.join(this.root, INDEX_FILE);
    const tmp = `${target}.${process.pid}.${++this.indexWrites}.tmp`;
    const handle = await open(tmp, 'w');
    try {
      await handle.write(`${JSON.stringify(body, null, 2)}\n`, null, 'utf8');
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(tmp, target);
    await this.syncDir(this.root);
  }

  /**
   * fsyncs a directory so a newly created name survives a crash.
   *
   * Best effort: opening a directory for sync is not portable, and failing to sync
   * the directory is strictly less bad than failing to sync the data, which is not
   * best effort.
   */
  private async syncDir(dir: string = this.logDir): Promise<void> {
    try {
      const handle = await open(dir, 'r');
      try {
        await handle.sync();
      } finally {
        await handle.close();
      }
    } catch {
      /* platform does not allow syncing a directory handle */
    }
  }

  /**
   * Reads one workflow's whole history, repairing a torn tail.
   *
   * Verification order matters: tearing is checked first because it is the expected
   * crash shape, and only the *final* fragment may be torn. A truncated line anywhere
   * else means bytes went missing from the middle of an append-only file, which no
   * crash can do and no read should paper over.
   */
  private async loadFile(workflowId: WorkflowId): Promise<Event[]> {
    const file = this.fileFor(workflowId);
    let raw: string;
    try {
      raw = await readFile(file, 'utf8');
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return [];
      throw err;
    }
    if (raw.length === 0) return [];

    const lastNewline = raw.lastIndexOf('\n');
    if (lastNewline !== raw.length - 1) {
      // Everything after the final newline was never completed, so it was never
      // acknowledged as durable either. Dropping it is the only way to reach a state
      // the writer could have observed.
      const keep = lastNewline + 1;
      await truncate(file, keep);
      raw = raw.slice(0, keep);
      if (raw.length === 0) return [];
    }

    const lines = raw.split('\n');
    lines.pop(); // trailing empty element after the final newline
    const events: Event[] = [];

    lines.forEach((line, i) => {
      const lineNo = i + 1;
      const match = LINE_PATTERN.exec(line);
      if (!match) {
        throw new LogCorruptionError(file, lineNo, 'line is not a checksummed event record');
      }
      const [, digest, json] = match;
      if (digest === undefined || json === undefined) {
        throw new LogCorruptionError(file, lineNo, 'line is not a checksummed event record');
      }
      const actual = checksum(json);
      if (actual !== digest) {
        throw new LogCorruptionError(
          file,
          lineNo,
          `checksum mismatch: recorded ${digest}, computed ${actual}`,
        );
      }

      let event: Event;
      try {
        event = JSON.parse(json) as Event;
      } catch {
        throw new LogCorruptionError(file, lineNo, 'event is not valid JSON');
      }
      if (event.workflowId !== workflowId) {
        throw new LogCorruptionError(
          file,
          lineNo,
          `event belongs to workflow ${String(event.workflowId)}`,
        );
      }
      if (event.sequence !== lineNo) {
        throw new LogCorruptionError(
          file,
          lineNo,
          `expected sequence ${lineNo}, found ${String(event.sequence)}`,
        );
      }
      events.push(Object.freeze(event));
    });

    return events;
  }

  private withLock<T>(workflowId: WorkflowId, fn: () => Promise<T>): Promise<T> {
    const previous = this.locks.get(workflowId) ?? Promise.resolve();
    // Chained off the settled outcome so one failed append does not wedge the queue.
    const next = previous.then(fn, fn);
    this.locks.set(
      workflowId,
      next.catch(() => undefined),
    );
    return next;
  }
}

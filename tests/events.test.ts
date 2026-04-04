/**
 * File store durability.
 *
 * The conformance suite in store.test.ts proves the file store honours the interface.
 * These tests cover what is specific to putting the log on a disk: a crash in the
 * middle of a write, a silently corrupted byte, a lost index, and the requirement that
 * reopening the store produces the same history it had before.
 */

import { readFile, readdir, rm, stat, truncate, writeFile, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import * as path from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { FileEventStore, LogCorruptionError, encodeLine } from '../src/events/file-store.js';
import { sealBatch, InvalidAppendError } from '../src/events/common.js';
import { ConcurrencyError, EVENT_VERSION } from '../src/events/types.js';
import type { NewEvent } from '../src/events/types.js';

const WF = 'wf-durable';

const HISTORY: readonly NewEvent[] = [
  {
    workflowId: WF,
    type: 'workflow.created',
    at: 1_000,
    workflow: 'ingest',
    definitionVersion: 1,
    input: { url: 'https://example.test' },
  },
  {
    workflowId: WF,
    type: 'step.scheduled',
    at: 1_001,
    step: 'fetch',
    attempt: 1,
  },
  {
    workflowId: WF,
    type: 'step.started',
    at: 1_002,
    step: 'fetch',
    attempt: 1,
    workerId: 'w1',
  },
  {
    workflowId: WF,
    type: 'step.completed',
    at: 1_003,
    step: 'fetch',
    attempt: 1,
    workerId: 'w1',
    output: { body: 'hello' },
  },
];

describe('FileEventStore durability', () => {
  let dir: string;

  beforeEach(async () => {
    dir = await mkdtemp(path.join(tmpdir(), 'parliament-file-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  const logPath = (workflowId = WF): string =>
    path.join(dir, 'workflows', `${encodeURIComponent(workflowId)}.jsonl`);

  async function seed(): Promise<FileEventStore> {
    const store = await FileEventStore.open(dir);
    await store.append(WF, 0, HISTORY);
    return store;
  }

  it('has the bytes on disk by the time append resolves', async () => {
    const store = await FileEventStore.open(dir);
    await store.append(WF, 0, HISTORY.slice(0, 1));

    // Read straight from the filesystem rather than through the store, so nothing
    // in-process can satisfy the read from memory.
    const raw = await readFile(logPath(), 'utf8');
    expect(raw.split('\n').filter(Boolean)).toHaveLength(1);
    expect(raw.endsWith('\n')).toBe(true);
  });

  it('reopening produces a byte-identical history', async () => {
    const first = await seed();
    const before = await first.read(WF);
    const bytesBefore = await readFile(logPath(), 'utf8');

    const reopened = await FileEventStore.open(dir);
    const after = await reopened.read(WF);

    expect(after).toEqual(before);
    expect(after.map(encodeLine).join('')).toBe(bytesBefore);
    expect(await readFile(logPath(), 'utf8')).toBe(bytesBefore);
    expect(await reopened.head(WF)).toBe(4);
    expect(await reopened.listActive()).toEqual([WF]);
  });

  it('appends continue from the head after a reopen', async () => {
    await seed();
    const reopened = await FileEventStore.open(dir);

    const written = await reopened.append(WF, 4, [
      {
        workflowId: WF,
        type: 'step.scheduled',
        at: 1_004,
        step: 'summarize',
        attempt: 1,
      },
    ]);

    expect(written[0]?.sequence).toBe(5);
    expect((await reopened.read(WF)).map((e) => e.sequence)).toEqual([1, 2, 3, 4, 5]);
  });

  it('truncates a torn final line and keeps every earlier event', async () => {
    await seed();
    const size = (await stat(logPath())).size;

    // A crash between the write and the newline: the final record is incomplete, so it
    // was never acknowledged as durable and must not be resurrected.
    await truncate(logPath(), size - 25);

    const reopened = await FileEventStore.open(dir);
    const recovered = await reopened.read(WF);

    expect(recovered.map((e) => e.type)).toEqual([
      'workflow.created',
      'step.scheduled',
      'step.started',
    ]);
    expect(await reopened.head(WF)).toBe(3);
    const repaired = await readFile(logPath(), 'utf8');
    expect(repaired.endsWith('\n')).toBe(true);
    // The repair is persisted, so the next append lands at the right offset.
    expect(repaired.split('\n').filter(Boolean)).toHaveLength(3);

    const written = await reopened.append(WF, 3, [HISTORY[3] as NewEvent]);
    expect(written[0]?.sequence).toBe(4);
    expect(await reopened.read(WF)).toHaveLength(4);
  });

  it('truncates a torn first line to an empty history rather than failing', async () => {
    const store = await FileEventStore.open(dir);
    await store.append(WF, 0, HISTORY.slice(0, 1));
    await truncate(logPath(), 10);

    const reopened = await FileEventStore.open(dir);
    expect(await reopened.read(WF)).toEqual([]);
    expect(await reopened.head(WF)).toBe(0);
  });

  it('detects a silently corrupted event instead of parsing it', async () => {
    await seed();

    // Still valid JSON, still the right shape: only a checksum can tell that the
    // worker id changed under us.
    const raw = await readFile(logPath(), 'utf8');
    const corrupted = raw.replace('"workerId":"w1"', '"workerId":"w9"');
    expect(corrupted).not.toBe(raw);
    await writeFile(logPath(), corrupted, 'utf8');

    const reopened = await FileEventStore.open(dir);
    await expect(reopened.read(WF)).rejects.toThrow(LogCorruptionError);
    await expect(reopened.read(WF)).rejects.toThrow(/checksum mismatch/);
  });

  it('refuses a line that is not a checksummed record, naming the file and line', async () => {
    await seed();
    const raw = await readFile(logPath(), 'utf8');
    const lines = raw.split('\n');
    lines[1] = '{"type":"step.scheduled","sequence":2}';
    await writeFile(logPath(), lines.join('\n'), 'utf8');

    // The rewrite changed the file size, so the stale-index check re-scans the log and
    // the corruption surfaces at open: a log that cannot be read is not a store that
    // should be handed to a scheduler.
    await expect(FileEventStore.open(dir)).rejects.toThrow(
      /corrupt event log .*wf-durable\.jsonl at line 2: line is not a checksummed event record/,
    );
  });

  it('rebuilds the index from the logs when it is missing', async () => {
    await seed();
    await rm(path.join(dir, 'index.json'));

    const reopened = await FileEventStore.open(dir);
    expect(await reopened.list()).toEqual([WF]);
    expect(await reopened.head(WF)).toBe(4);
    expect(await reopened.listActive()).toEqual([WF]);
    // Rebuilt and persisted, so the next open is cheap again.
    expect(await readdir(dir)).toContain('index.json');
  });

  it('rebuilds the index when it is unparseable', async () => {
    await seed();
    await writeFile(path.join(dir, 'index.json'), '{not json', 'utf8');

    const reopened = await FileEventStore.open(dir);
    expect(await reopened.head(WF)).toBe(4);
  });

  it('believes the log over a stale index', async () => {
    await seed();
    // The shape of a crash between the data fsync and the index write.
    await writeFile(
      path.join(dir, 'index.json'),
      JSON.stringify({ version: 1, workflows: { [WF]: { head: 2, terminal: true, bytes: 17 } } }),
      'utf8',
    );

    const reopened = await FileEventStore.open(dir);
    expect(await reopened.head(WF)).toBe(4);
    expect(await reopened.listActive()).toEqual([WF]);
  });

  it('answers listActive from the index without reading histories', async () => {
    await seed();
    const index = JSON.parse(await readFile(path.join(dir, 'index.json'), 'utf8')) as {
      workflows: Record<string, { head: number; terminal: boolean; bytes: number }>;
    };

    // The whole point of the side index: the answer is present without opening a log.
    expect(index.workflows[WF]).toEqual({ head: 4, terminal: false, bytes: expect.any(Number) });
  });

  it('rebuildIndex is idempotent and agrees with the incremental index', async () => {
    const store = await seed();
    const before = { list: await store.list(), head: await store.head(WF) };

    await store.rebuildIndex();

    expect(await store.list()).toEqual(before.list);
    expect(await store.head(WF)).toBe(before.head);
  });
});

describe('append validation', () => {
  it('seals a batch with contiguous sequences and the current version', () => {
    const sealed = sealBatch(WF, 2, 2, HISTORY.slice(0, 2));
    expect(sealed.map((e) => e.sequence)).toEqual([3, 4]);
    expect(sealed.every((e) => e.version === EVENT_VERSION)).toBe(true);
  });

  it('reports a concurrency conflict before inspecting the batch', () => {
    expect(() => sealBatch(WF, 5, 2, [{} as NewEvent])).toThrow(ConcurrencyError);
  });

  it('rejects an event with no timestamp', () => {
    expect(() =>
      sealBatch(WF, 0, 0, [{ workflowId: WF, type: 'workflow.completed' } as unknown as NewEvent]),
    ).toThrow(InvalidAppendError);
  });

  it('rejects an envelope from an unknown schema version', () => {
    expect(() =>
      sealBatch(WF, 0, 0, [
        { workflowId: WF, type: 'workflow.completed', at: 1, version: 99 } as unknown as NewEvent,
      ]),
    ).toThrow(/version 99/);
  });
});

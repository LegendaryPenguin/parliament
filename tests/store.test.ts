/**
 * The EventStore conformance suite.
 *
 * Written once against the interface and run against every implementation. A suite
 * per store hides exactly the divergence it is supposed to catch: the control plane
 * cannot tell which store it has, so neither should these tests.
 */

import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import * as path from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { FileEventStore } from '../src/events/file-store.js';
import { MemoryEventStore } from '../src/events/memory-store.js';
import { ConcurrencyError, EVENT_VERSION } from '../src/events/types.js';
import type { Event, EventStore, NewEvent, StepCompleted } from '../src/events/types.js';

const WF = 'wf-1';

function created(workflowId = WF, at = 1_000): NewEvent {
  return {
    workflowId,
    type: 'workflow.created',
    at,
    workflow: 'ingest',
    definitionVersion: 1,
    input: { url: 'https://example.test' },
  };
}

function scheduled(step: string, attempt: number, at: number, workflowId = WF): NewEvent {
  return { workflowId, type: 'step.scheduled', at, step, attempt };
}

function started(step: string, attempt: number, at: number, workflowId = WF): NewEvent {
  return {
    workflowId,
    type: 'step.started',
    at,
    step,
    attempt,
    workerId: 'w1',
  };
}

function completed(step: string, attempt: number, at: number, output: unknown): NewEvent {
  return {
    workflowId: WF,
    type: 'step.completed',
    at,
    step,
    attempt,
    workerId: 'w1',
    output,
  };
}

type Kind = 'memory' | 'file';

describe.each<Kind>(['memory', 'file'])('EventStore conformance (%s)', (kind) => {
  let dir: string;
  let store: EventStore;

  beforeEach(async () => {
    dir = await mkdtemp(path.join(tmpdir(), 'parliament-store-'));
    store = kind === 'memory' ? new MemoryEventStore() : await FileEventStore.open(dir);
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('assigns sequences from one and stamps the envelope version', async () => {
    const written = await store.append(WF, 0, [created(), scheduled('fetch', 1, 1_001)]);

    expect(written.map((e) => e.sequence)).toEqual([1, 2]);
    expect(written.every((e) => e.version === EVENT_VERSION)).toBe(true);
    expect(await store.head(WF)).toBe(2);
  });

  it('reports head zero and an empty history for an unknown workflow', async () => {
    expect(await store.head('never-created')).toBe(0);
    expect(await store.read('never-created')).toEqual([]);
  });

  it('appends in batches and reads the whole history back in order', async () => {
    await store.append(WF, 0, [created()]);
    await store.append(WF, 1, [scheduled('fetch', 1, 1_001), started('fetch', 1, 1_002)]);

    const history = await store.read(WF);
    expect(history.map((e) => `${e.sequence}:${e.type}`)).toEqual([
      '1:workflow.created',
      '2:step.scheduled',
      '3:step.started',
    ]);
  });

  it('reads from a position exclusively', async () => {
    await store.append(WF, 0, [created(), scheduled('fetch', 1, 1_001), started('fetch', 1, 1_002)]);

    expect((await store.read(WF, 2)).map((e) => e.sequence)).toEqual([3]);
    expect(await store.read(WF, 3)).toEqual([]);
  });

  it('preserves event payloads through a round trip', async () => {
    await store.append(WF, 0, [created()]);
    await store.append(WF, 1, [completed('fetch', 1, 1_003, { items: [1, 2], nested: { ok: true } })]);

    const history = await store.read(WF);
    const last = history[1] as StepCompleted | undefined;
    expect(last?.output).toEqual({ items: [1, 2], nested: { ok: true } });
  });

  it('rejects a stale expected sequence with ConcurrencyError', async () => {
    await store.append(WF, 0, [created(), scheduled('fetch', 1, 1_001)]);

    await expect(store.append(WF, 1, [started('fetch', 1, 1_002)])).rejects.toThrow(
      ConcurrencyError,
    );
  });

  it('rejects an expected sequence ahead of the log', async () => {
    await store.append(WF, 0, [created()]);

    await expect(store.append(WF, 7, [scheduled('fetch', 1, 1_001)])).rejects.toBeInstanceOf(
      ConcurrencyError,
    );
  });

  it('leaves the log untouched when an append loses the race', async () => {
    await store.append(WF, 0, [created(), scheduled('fetch', 1, 1_001)]);
    const before = await store.read(WF);

    await expect(store.append(WF, 1, [started('fetch', 1, 1_002)])).rejects.toThrow(
      ConcurrencyError,
    );

    expect(await store.read(WF)).toEqual(before);
    expect(await store.head(WF)).toBe(2);
  });

  it('only one of two concurrent appends at the same head wins', async () => {
    await store.append(WF, 0, [created()]);

    const results = await Promise.allSettled([
      store.append(WF, 1, [started('fetch', 1, 1_002)]),
      store.append(WF, 1, [started('fetch', 2, 1_003)]),
    ]);

    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    expect(fulfilled).toHaveLength(1);
    const rejected = results.find((r) => r.status === 'rejected');
    expect(rejected?.status === 'rejected' && rejected.reason).toBeInstanceOf(ConcurrencyError);
    expect(await store.head(WF)).toBe(2);
  });

  /**
   * A regression test. Appends to *different* workflows have no shared history and no reason
   * to interfere, but the file store keeps one index for all of them, and this is the shape
   * that caught the index write racing itself: every append fsynced its own log and then
   * every one of them rejected, so the caller was told its append had failed for an event
   * that was already on disk. A lease acquired that way is recorded in the log and believed
   * lost by the worker holding it.
   *
   * Twelve at once rather than two, because the window is one `await` wide and a pair hits it
   * only sometimes. Run against both stores, because "which appends may proceed in parallel"
   * is part of the contract and not an implementation detail.
   */
  it('appends concurrently to different workflows without interfering', async () => {
    const ids = Array.from({ length: 12 }, (_, i) => `wf-parallel-${i}`);

    const results = await Promise.allSettled(ids.map((id) => store.append(id, 0, [created(id)])));

    const rejected = results.filter((r) => r.status === 'rejected');
    expect(rejected.map((r) => String(r.status === 'rejected' && r.reason))).toEqual([]);
    for (const id of ids) {
      expect(await store.head(id)).toBe(1);
      expect(await store.read(id)).toHaveLength(1);
    }
    expect([...(await store.list())].sort()).toEqual([...ids].sort());
  });

  /** The same shape again, with the appends landing on workflows that already have history. */
  it('keeps the recovery index correct after concurrent appends to different workflows', async () => {
    const ids = Array.from({ length: 8 }, (_, i) => `wf-index-${i}`);
    await Promise.all(ids.map((id) => store.append(id, 0, [created(id)])));

    await Promise.all(ids.map((id) => store.append(id, 1, [scheduled('fetch', 1, 1_001, id)])));
    // Half of them finish, so `listActive` has something to leave out.
    await Promise.all(
      ids.filter((_, i) => i % 2 === 0).map((id) =>
        store.append(id, 2, [{ workflowId: id, type: 'workflow.completed', at: 1_002, output: null }]),
      ),
    );

    expect([...(await store.listActive())].sort()).toEqual(ids.filter((_, i) => i % 2 === 1).sort());

    // Reopened from disk for the file store: the index on disk is what a restart reads, and a
    // raced write is only visible after the in-memory copy is thrown away.
    if (kind === 'file') {
      const reopened = await FileEventStore.open(dir);
      expect([...(await reopened.listActive())].sort()).toEqual(
        ids.filter((_, i) => i % 2 === 1).sort(),
      );
      for (const [i, id] of ids.entries()) {
        expect(await reopened.head(id)).toBe(i % 2 === 0 ? 3 : 2);
      }
    }
  });

  it('writes nothing when any event in the batch is invalid', async () => {
    await store.append(WF, 0, [created()]);

    await expect(
      store.append(WF, 1, [
        scheduled('fetch', 1, 1_001),
        // Belongs to a different workflow: the batch must be refused as a whole.
        scheduled('fetch', 1, 1_002, 'other-workflow'),
        started('fetch', 1, 1_003),
      ]),
    ).rejects.toThrow(/other-workflow/);

    expect(await store.head(WF)).toBe(1);
    expect(await store.read(WF)).toHaveLength(1);
  });

  it('treats an empty batch as a no-op', async () => {
    await store.append(WF, 0, [created()]);
    expect(await store.append(WF, 1, [])).toEqual([]);
    expect(await store.head(WF)).toBe(1);
  });

  it('lists every workflow and omits terminal ones from listActive', async () => {
    await store.append('wf-a', 0, [created('wf-a')]);
    await store.append('wf-b', 0, [created('wf-b')]);
    await store.append('wf-b', 1, [
      {
        workflowId: 'wf-b',
        type: 'workflow.completed',
        at: 2_000,
        output: { ok: true },
      },
    ]);
    await store.append('wf-c', 0, [created('wf-c')]);
    await store.append('wf-c', 1, [
      {
        workflowId: 'wf-c',
        type: 'workflow.cancelled',
        at: 2_100,
        reason: 'operator asked',
      },
    ]);

    expect([...(await store.list())].sort()).toEqual(['wf-a', 'wf-b', 'wf-c']);
    expect([...(await store.listActive())].sort()).toEqual(['wf-a']);
  });

  it('keeps workflows independent', async () => {
    await store.append('wf-a', 0, [created('wf-a')]);
    await store.append('wf-b', 0, [created('wf-b')]);
    await store.append('wf-b', 1, [
      {
        workflowId: 'wf-b',
        type: 'step.scheduled',
        at: 1_001,
        step: 'fetch',
        attempt: 1,
      },
    ]);

    expect(await store.head('wf-a')).toBe(1);
    expect(await store.head('wf-b')).toBe(2);
    expect((await store.read('wf-a')).every((e) => e.workflowId === 'wf-a')).toBe(true);
  });

  it('handles workflow ids that are not filesystem-safe', async () => {
    const awkward = 'tenant/42:run #1';
    await store.append(awkward, 0, [created(awkward)]);

    expect(await store.head(awkward)).toBe(1);
    expect(await store.list()).toContain(awkward);
  });

  // --- subscribe ---------------------------------------------------------------
  //
  // On the contract rather than in a decorator, so these run against every
  // implementation — which is the point of having moved it there.

  it('notifies a subscriber with the durable batch, once per append', async () => {
    const batches: string[][] = [];
    store.subscribe((events) => batches.push(events.map((e) => `${e.sequence}:${e.type}`)));

    await store.append(WF, 0, [created()]);
    await store.append(WF, 1, [scheduled('fetch', 1, 1_001), started('fetch', 1, 1_002)]);

    expect(batches).toEqual([
      ['1:workflow.created'],
      ['2:step.scheduled', '3:step.started'],
    ]);
  });

  it('does not notify for a rejected append', async () => {
    await store.append(WF, 0, [created()]);

    let notified = 0;
    store.subscribe(() => {
      notified += 1;
    });

    await expect(store.append(WF, 0, [scheduled('fetch', 1, 1_001)])).rejects.toThrow(
      ConcurrencyError,
    );
    // An empty batch is a no-op, not an event worth waking anyone for.
    await store.append(WF, 1, []);

    expect(notified).toBe(0);
  });

  it('stops notifying after unsubscribe', async () => {
    let notified = 0;
    const off = store.subscribe(() => {
      notified += 1;
    });

    await store.append(WF, 0, [created()]);
    off();
    await store.append(WF, 1, [scheduled('fetch', 1, 1_001)]);

    expect(notified).toBe(1);
  });

  it('survives a listener that throws, and still runs the other listeners', async () => {
    // The events are already durable when listeners run. A listener that throws must not
    // make the caller believe the append failed, because the caller's response to a
    // failed append is to retry — against a log that already contains the write.
    const seen: string[] = [];
    store.subscribe(() => {
      throw new Error('a dashboard listener blew up');
    });
    store.subscribe((events) => seen.push(...events.map((e) => e.type)));

    await expect(store.append(WF, 0, [created()])).resolves.toHaveLength(1);
    expect(seen).toEqual(['workflow.created']);
    expect(await store.head(WF)).toBe(1);
  });

  it('lets a listener read the workflow it was just notified about', async () => {
    // A listener reading the log it was just told about is the most obvious thing a
    // listener does, and it must see the batch it was notified of already durable.
    //
    // This was written expecting to catch a deadlock in `FileEventStore`, which locks
    // per workflow for both `append` and `read`. It does not: moving the notification
    // inside the critical section keeps this green, because `notify` is synchronous and
    // no store awaits a listener, so the listener's `read` just queues behind the
    // append. The test is kept for the guarantee it does check — a listener is never
    // woken before the events it was woken for are readable.
    let readBack: readonly Event[] = [];
    const done = new Promise<void>((resolve) => {
      store.subscribe((events) => {
        void store.read(events[0]!.workflowId).then((all) => {
          readBack = all;
          resolve();
        });
      });
    });

    await store.append(WF, 0, [created(), scheduled('fetch', 1, 1_001)]);
    await done;

    expect(readBack.map((e) => e.sequence)).toEqual([1, 2]);
  });

});

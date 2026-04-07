/**
 * Lease tests.
 *
 * Every deadline here comes from the fake clock. A lease-expiry test built on real sleeps
 * is flaky by construction: it passes when the machine is idle and fails on a loaded CI
 * box, and the failure looks like a bug in the lease logic rather than in the test.
 */

import { describe, expect, it } from 'vitest';
import { MemoryEventStore } from '../src/events/memory-store.js';
import type { Event } from '../src/events/types.js';
import { FakeClock } from '../src/scheduler/clock.js';
import { LeaseExpiredError, LeaseLostError, LeaseManager } from '../src/scheduler/lease.js';
import { RetryEngine } from '../src/scheduler/retry.js';
import { Scheduler } from '../src/scheduler/scheduler.js';
import { SchedulerWorkerPort } from '../src/scheduler/worker-port.js';
import { project } from '../src/state/project.js';
import type { StepRef, WorkflowDef } from '../src/workflow/types.js';

const LEASE_MS = 1_000;

const def: WorkflowDef = {
  name: 'lease-demo',
  version: 1,
  start: 'a',
  nodes: [
    { name: 'a', kind: 'tool', tool: 'noop', dependsOn: [] },
    { name: 'b', kind: 'tool', tool: 'noop', dependsOn: ['a'] },
  ],
  timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: LEASE_MS },
};

async function seed(store: MemoryEventStore, id: string, at: number): Promise<void> {
  await store.append(id, 0, [
    {
      type: 'workflow.created',
      workflowId: id,
      at,
      workflow: def.name,
      definitionVersion: def.version,
      input: null,
    },
    { type: 'step.scheduled', workflowId: id, at, step: 'a', attempt: 1, notBefore: at },
  ]);
}

function fixture(): {
  store: MemoryEventStore;
  clock: FakeClock;
  leases: LeaseManager;
  id: string;
  ref: StepRef;
} {
  const store = new MemoryEventStore();
  const clock = new FakeClock(1_000);
  const leases = new LeaseManager({ store, clock, leaseDurationMs: LEASE_MS });
  return { store, clock, leases, id: 'wf-1', ref: { workflowId: 'wf-1', step: 'a', attempt: 1 } };
}

function typesOf(events: readonly Event[]): string[] {
  return events.map((e) => e.type);
}

describe('lease acquisition', () => {
  it('grants a lease and records it with an expiry', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());

    const lease = await leases.acquire(ref, 'worker-1');

    expect(lease).not.toBeNull();
    expect(lease?.expiresAt).toBe(clock.now() + LEASE_MS);
    expect(typesOf(await store.read(id))).toContain('step.leased');
  });

  it('refuses a second lease while the first is live', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());

    expect(await leases.acquire(ref, 'worker-1')).not.toBeNull();
    expect(await leases.acquire(ref, 'worker-2')).toBeNull();
  });

  /**
   * The headline property of the whole mechanism. Both schedulers fold the same history,
   * both conclude the step is free, and both append with the same expected sequence. The
   * store decides, and it can only accept one — which is why this needs no lock and
   * keeps working with the two schedulers in different processes.
   */
  it('two schedulers racing for one step: exactly one wins', async () => {
    const store = new MemoryEventStore();
    const clock = new FakeClock(5_000);
    const id = 'wf-race';
    const ref: StepRef = { workflowId: id, step: 'a', attempt: 1 };
    await seed(store, id, clock.now());

    const alpha = new LeaseManager({ store, clock, leaseDurationMs: LEASE_MS });
    const beta = new LeaseManager({ store, clock, leaseDurationMs: LEASE_MS });

    const results = await Promise.all([
      alpha.acquire(ref, 'alpha/w1'),
      beta.acquire(ref, 'beta/w1'),
    ]);

    const winners = results.filter((r) => r !== null);
    expect(winners).toHaveLength(1);

    const leasedEvents = (await store.read(id)).filter((e) => e.type === 'step.leased');
    expect(leasedEvents).toHaveLength(1);
    expect(winners[0]?.workerId).toBe(
      leasedEvents[0]?.type === 'step.leased' ? leasedEvents[0].workerId : undefined,
    );
  });

  it('refuses to lease a step in a terminal workflow', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    await store.append(id, await store.head(id), [
      { type: 'workflow.cancelled', workflowId: id, at: clock.now(), reason: 'operator' },
    ]);

    expect(await leases.acquire(ref, 'worker-1')).toBeNull();
  });
});

describe('a renewal reaches the projection', () => {
  // This is the regression test for a bug that every unit test missed.
  //
  // Renewals used to be appended as a second `step.leased`. The projection
  // deduplicates on (type, step, attempt, worker), so every renewal after the first
  // was absorbed as a duplicate and `StepProjection.leaseExpiresAt` kept the
  // ORIGINAL expiry. The scheduler never noticed, because it checks raw events via
  // `liveLeaseOf` — so the lease tests passed, the projection tests passed, and
  // `readySteps` would still hand out a step a worker was actively heartbeating on.
  //
  // The bug lived in the seam, which is why this test crosses it: drive the real
  // heartbeat, then read the projection rather than the raw log.
  it('extends the expiry the projection reports, not just the raw lease', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());

    const lease = await leases.acquire(ref, 'worker-1');
    const firstExpiry = lease!.expiresAt;

    clock.advance(LEASE_MS / 2);
    const renewed = await leases.heartbeat(lease!);
    expect(renewed.expiresAt).toBeGreaterThan(firstExpiry);

    const projected = project(def, await store.read(id));
    expect(projected.steps.get('a')?.leaseExpiresAt).toBe(renewed.expiresAt);

    // And the step must not be offered while the renewed lease is live. Under the old
    // behaviour the projection still reported `firstExpiry`, so a clock past it made
    // the step look free.
    const { readySteps } = await import('../src/state/ready.js');
    const atFirstExpiry = firstExpiry + 1;
    expect(atFirstExpiry).toBeLessThan(renewed.expiresAt);
    expect(readySteps(def, projected, atFirstExpiry).map((s) => s.step)).not.toContain('a');
  });

  it('ignores a renewal from a worker that no longer holds the step', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());

    const first = await leases.acquire(ref, 'worker-1');
    clock.advance(LEASE_MS + 1);
    await leases.expireOverdue(id);
    const second = await leases.acquire(ref, 'worker-2');
    const heldUntil = second!.expiresAt;

    // worker-1's heartbeat is in flight from before the reassignment. Handing its
    // renewal to worker-2 would silently extend a claim worker-2 never made.
    await store.append(id, await store.head(id), [
      {
        type: 'step.lease_extended',
        workflowId: id,
        at: clock.now(),
        step: 'a',
        attempt: first!.attempt,
        workerId: 'worker-1',
        leaseExpiresAt: heldUntil + 60_000,
      },
    ]);

    const projected = project(def, await store.read(id));
    expect(projected.steps.get('a')?.workerId).toBe('worker-2');
    expect(projected.steps.get('a')?.leaseExpiresAt).toBe(heldUntil);
  });
});

describe('heartbeats', () => {
  it('extends a live lease', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    const lease = await leases.acquire(ref, 'worker-1');

    await clock.advance(LEASE_MS / 2);
    const renewed = await leases.heartbeat(lease!);

    expect(renewed.expiresAt).toBe(clock.now() + LEASE_MS);

    // Past the original expiry, and still held: extension is what keeps the step out of
    // the ready set while the worker is demonstrably alive.
    await clock.advance(LEASE_MS * 0.75);
    expect(await leases.live(id, 'a')).not.toBeUndefined();
  });

  /**
   * Refusing this is the point. By the time the lease lapsed the step was already
   * eligible for reassignment, so extending it could produce two live leases — the exact
   * state the mechanism exists to prevent. Failing tells the worker to stop and drop
   * whatever it was about to write.
   */
  it('fails on a lease that has already expired', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    const lease = await leases.acquire(ref, 'worker-1');

    await clock.advance(LEASE_MS + 1);

    await expect(leases.heartbeat(lease!)).rejects.toThrow(LeaseExpiredError);
    expect(typesOf(await store.read(id))).not.toContain('step.failed');
  });

  it('fails once the step has been reassigned to another worker', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    const first = await leases.acquire(ref, 'worker-1');

    await clock.advance(LEASE_MS + 1);
    await leases.expireOverdue(id);
    const second = await leases.acquire(ref, 'worker-2');
    expect(second).not.toBeNull();

    await expect(leases.heartbeat(first!)).rejects.toThrow(LeaseLostError);
  });

  it('fails once the workflow is terminal', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    const lease = await leases.acquire(ref, 'worker-1');

    const head = await store.head(id);
    await store.append(id, head, [
      { type: 'workflow.cancelled', workflowId: id, at: clock.now(), reason: 'operator' },
    ]);

    await expect(leases.heartbeat(lease!)).rejects.toThrow(LeaseLostError);
  });
});

describe('expiry', () => {
  it('records step.lease_expired and never step.failed', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    await leases.acquire(ref, 'worker-1');

    await clock.advance(LEASE_MS + 1);
    const expired = await leases.expireOverdue(id);

    expect(expired.map((l) => l.step)).toEqual(['a']);
    const types = typesOf(await store.read(id));
    expect(types).toContain('step.lease_expired');
    // The step has not failed, it has lost its worker. Conflating the two would let the
    // scheduler report a workflow failed while the original attempt is still running.
    expect(types).not.toContain('step.failed');
  });

  it('makes the step eligible again and a second worker completes it', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    await leases.acquire(ref, 'worker-1');

    await clock.advance(LEASE_MS + 1);
    await leases.expireOverdue(id);

    const reassigned = await leases.acquire(ref, 'worker-2');
    expect(reassigned).not.toBeNull();
    // The reassignment keeps the attempt number, so the second worker derives the same
    // idempotency key as the first and a repeated side effect is deduplicated rather
    // than performed twice.
    expect(reassigned?.attempt).toBe(1);

    const head = await store.head(id);
    await store.append(id, head, [
      {
        type: 'step.started',
        workflowId: id,
        at: clock.now(),
        step: 'a',
        attempt: 1,
        workerId: 'worker-2',
      },
      {
        type: 'step.completed',
        workflowId: id,
        at: clock.now(),
        step: 'a',
        attempt: 1,
        workerId: 'worker-2',
        output: { ok: true },
      },
    ]);

    const projection = project(def, await store.read(id));
    expect(projection.steps.get('a')?.state).toBe('succeeded');
    expect(projection.steps.get('a')?.workerId).toBe('worker-2');
    // The dependent became ready off the back of the reassigned run, not the lost one.
    expect(projection.steps.get('b')?.state).toBe('ready');
  });

  /**
   * One event, not two. `running -> ready` is legal precisely because a lease expiry is a
   * handback rather than a failure, so the expiry notice is the whole story and an
   * accompanying `step.scheduled` would just be a second way of saying it.
   */
  it('hands the step back with a single event and the same attempt number', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    await leases.acquire(ref, 'worker-1');
    const head = await store.head(id);

    await clock.advance(LEASE_MS + 1);
    await leases.expireOverdue(id);

    const after = (await store.read(id, head)).map((e) => e.type);
    expect(after).toEqual(['step.lease_expired']);

    const step = project(def, await store.read(id)).steps.get('a');
    expect(step?.state).toBe('ready');
    // No retry attempt was spent: the work never failed, only its worker went away.
    expect(step?.attempt).toBe(1);
  });

  it('is idempotent: a second sweep finds nothing', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    await leases.acquire(ref, 'worker-1');

    await clock.advance(LEASE_MS + 1);
    expect(await leases.expireOverdue(id)).toHaveLength(1);
    expect(await leases.expireOverdue(id)).toHaveLength(0);
  });

  it('releases a lease on request, without claiming the step failed', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    const lease = await leases.acquire(ref, 'worker-1');
    const head = await store.head(id);

    expect(await leases.release(lease!)).toBe(true);

    const after = (await store.read(id, head)).map((e) => e.type);
    expect(after).toEqual(['step.lease_expired']);
    // Available again immediately, rather than after the lease duration elapses.
    expect(await leases.acquire(ref, 'worker-2')).not.toBeNull();
  });

  it('refuses a release from a worker that no longer holds the step', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    const stale = await leases.acquire(ref, 'worker-1');

    await clock.advance(LEASE_MS + 1);
    await leases.expireOverdue(id);
    const current = await leases.acquire(ref, 'worker-2');

    // Releasing here would revoke worker-2's claim and hand the step to a third worker.
    expect(await leases.release(stale!)).toBe(false);
    expect((await leases.live(id, 'a'))?.workerId).toBe(current?.workerId);
  });

  it('leaves a terminal workflow alone', async () => {
    const { store, clock, leases, id, ref } = fixture();
    await seed(store, id, clock.now());
    await leases.acquire(ref, 'worker-1');
    const head = await store.head(id);
    await store.append(id, head, [
      { type: 'workflow.cancelled', workflowId: id, at: clock.now(), reason: 'operator' },
    ]);

    await clock.advance(LEASE_MS + 1);

    expect(await leases.expireOverdue(id)).toHaveLength(0);
    expect(typesOf(await store.read(id))).not.toContain('step.lease_expired');
  });
});

/**
 * The adapter between the worker's ports and the scheduler's lease manager.
 *
 * The worker asks "give me something to run" and learns about a lost lease from a boolean;
 * the lease manager leases one named step and throws. Both sides are right for their own
 * callers, so the translation is tested rather than assumed.
 */
describe('worker lease port', () => {
  const portDef: WorkflowDef = {
    name: 'port-demo',
    version: 1,
    start: 'prepare',
    nodes: [
      {
        name: 'prepare',
        kind: 'tool',
        tool: 'read',
        dependsOn: [],
        retry: {
          maxAttempts: 3,
          initialBackoffMs: 100,
          maxBackoffMs: 1_000,
          jitter: 0,
          retryableErrors: ['unavailable'],
        },
      },
      { name: 'crunch', kind: 'python', dependsOn: ['prepare'] },
      { name: 'publish', kind: 'tool', tool: 'write', dependsOn: ['crunch'] },
    ],
    timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: LEASE_MS },
  };

  function portFixture(defs: readonly WorkflowDef[] = [portDef]): {
    store: MemoryEventStore;
    clock: FakeClock;
    leases: LeaseManager;
    port: SchedulerWorkerPort;
    create: (def: WorkflowDef, id: string, input?: unknown) => Promise<void>;
  } {
    const store = new MemoryEventStore();
    const clock = new FakeClock(1_000);
    const leases = new LeaseManager({ store, clock, leaseDurationMs: LEASE_MS });
    const retries = new RetryEngine({ store, clock, random: () => 0.5 });
    const port = new SchedulerWorkerPort({
      store,
      clock,
      leases,
      retries,
      defs: (name, version) => defs.find((d) => d.name === name && d.version === version),
    });

    return {
      store,
      clock,
      leases,
      port,
      create: async (def, id, input = { source: 's3://in' }) => {
        await store.append(id, 0, [
          {
            type: 'workflow.created',
            workflowId: id,
            at: clock.now(),
            workflow: def.name,
            definitionVersion: def.version,
            input,
          },
        ]);
      },
    };
  }

  it('hands out the entry step with its input and both timeouts bound', async () => {
    const { store, port, create } = portFixture();
    await create(portDef, 'wf');

    const assignment = await port.acquire('worker-1');

    expect(assignment?.node.name).toBe('prepare');
    expect(assignment?.input).toEqual({ source: 's3://in' });
    // Attempts are 1-based even though the fold starts a never-scheduled step at 0.
    expect(assignment?.lease.ref.attempt).toBe(1);
    // The attempt budget and the lease duration travel separately: one retries, the other
    // reassigns, and a worker given a single "timeout" could not tell them apart.
    expect(assignment?.timeouts.attemptTimeoutMs).toBe(5_000);
    expect(assignment?.timeouts.leaseDurationMs).toBe(LEASE_MS);

    const types = typesOf(await store.read('wf'));
    expect(types).toContain('step.leased');
    expect(types).toContain('step.started');
    expect(project(portDef, await store.read('wf')).steps.get('prepare')?.state).toBe('running');
  });

  it('two workers polling at once: exactly one gets the step', async () => {
    const { store, port, create } = portFixture();
    await create(portDef, 'wf');

    const [first, second] = await Promise.all([port.acquire('worker-1'), port.acquire('worker-2')]);

    expect([first, second].filter((a) => a !== undefined)).toHaveLength(1);
    expect((await store.read('wf')).filter((e) => e.type === 'step.leased')).toHaveLength(1);
  });

  it('reports a lost lease as false rather than throwing', async () => {
    const { clock, port, create } = portFixture();
    await create(portDef, 'wf');
    const assignment = await port.acquire('worker-1');

    expect(await port.heartbeat(assignment!.lease)).toBe(true);

    await clock.advance(LEASE_MS + 1);

    // The one thing a worker must be able to learn without catching anything.
    await expect(port.heartbeat(assignment!.lease)).resolves.toBe(false);
  });

  it('advances the graph when a worker reports a completion', async () => {
    const { port, create } = portFixture();
    await create(portDef, 'wf');
    const first = await port.acquire('worker-1');

    await port.completed(first!.lease, { rows: 4 });

    const next = await port.acquire('worker-1');
    expect(next?.node.name).toBe('crunch');
    // The single dependency's output is bound directly as the next step's input.
    expect(next?.input).toEqual({ rows: 4 });
  });

  it('absorbs a completion reported twice', async () => {
    const { store, port, create } = portFixture();
    await create(portDef, 'wf');
    const assignment = await port.acquire('worker-1');

    await port.completed(assignment!.lease, { rows: 4 });
    await port.completed(assignment!.lease, { rows: 4 });

    const completions = (await store.read('wf')).filter(
      (e) => e.type === 'step.completed' && e.step === 'prepare',
    );
    expect(completions).toHaveLength(1);
  });

  it('routes a reported failure through the retry engine', async () => {
    const { store, port, create } = portFixture();
    await create(portDef, 'wf');
    const assignment = await port.acquire('worker-1');

    await port.failed(assignment!.lease, { message: 'upstream down', errorClass: 'unavailable' });

    const events = await store.read('wf');
    const failed = events.find((e) => e.type === 'step.failed');
    expect(failed?.type === 'step.failed' ? failed.willRetry : undefined).toBe(true);
    expect(typesOf(events)).toContain('step.retry_scheduled');
  });

  it('does not retry a definition error, whatever the node policy says', async () => {
    const { store, port, create } = portFixture();
    await create(portDef, 'wf');
    const assignment = await port.acquire('worker-1');

    await port.failed(assignment!.lease, {
      message: 'tool node has no tool name',
      errorClass: 'definition_error',
    });

    const events = await store.read('wf');
    const failed = events.find((e) => e.type === 'step.failed');
    expect(failed?.type === 'step.failed' ? failed.willRetry : undefined).toBe(false);
    expect(typesOf(events)).not.toContain('step.retry_scheduled');
  });

  /**
   * A worker that does not run a node kind hands it straight back. Offering it to the same
   * worker again would spin at the polling rate, so the decline is remembered — and
   * remembered against the worker, not the step, because the step is perfectly runnable by
   * somebody else.
   */
  it('does not re-offer a declined node kind to the same worker, but offers it to another', async () => {
    const { store, port, create } = portFixture();
    await create(portDef, 'wf');

    const first = await port.acquire('typescript-worker');
    await port.completed(first!.lease, { rows: 1 });

    const declined = await port.acquire('typescript-worker');
    expect(declined?.node.kind).toBe('python');
    // Released with no outcome claimed: that is what a decline looks like on the wire.
    await port.release(declined!.lease);
    expect(typesOf(await store.read('wf'))).toContain('step.lease_expired');

    expect(await port.acquire('typescript-worker')).toBeUndefined();

    const takeover = await port.acquire('python-worker');
    expect(takeover?.node.name).toBe('crunch');
    expect(takeover?.lease.ref.attempt).toBe(1);
  });

  it('never offers an approval gate to a worker', async () => {
    const gated: WorkflowDef = {
      name: 'gated',
      version: 1,
      start: 'gate',
      nodes: [
        { name: 'gate', kind: 'approval', dependsOn: [], approvers: ['alice'] },
        { name: 'publish', kind: 'tool', tool: 'write', dependsOn: ['gate'] },
      ],
      timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: LEASE_MS },
    };
    const { port, create } = portFixture([gated]);
    await create(gated, 'wf');

    // A gate waits on a human. Leasing it would idle a worker for as long as that takes.
    expect(await port.acquire('worker-1')).toBeUndefined();
  });

  it('offers nothing once the workflow is terminal', async () => {
    const { store, clock, port, create } = portFixture();
    await create(portDef, 'wf');
    await store.append('wf', await store.head('wf'), [
      { type: 'workflow.cancelled', workflowId: 'wf', at: clock.now(), reason: 'operator' },
    ]);

    expect(await port.acquire('worker-1')).toBeUndefined();
  });

  it('exposes only succeeded outputs to a worker', async () => {
    const { port, create } = portFixture();
    await create(portDef, 'wf');
    const first = await port.acquire('worker-1');
    await port.completed(first!.lease, { rows: 4 });
    // In flight, so its output does not exist yet and must not be visible.
    await port.acquire('worker-2');

    const outputs = await port.completedOutputs('wf');

    expect([...outputs.keys()]).toEqual(['prepare']);
    expect(outputs.get('prepare')?.output).toEqual({ rows: 4 });
  });

  it('is reachable from the scheduler, sharing its definitions and ordering', async () => {
    const store = new MemoryEventStore();
    const clock = new FakeClock(1_000);
    const scheduler = new Scheduler({
      store,
      clock,
      dispatch: () => new Promise<unknown>(() => {}),
    });
    await scheduler.startAs('wf', portDef, { input: { source: 'x' } });

    const assignment = await scheduler.workerPort().acquire('worker-1');

    expect(assignment?.node.name).toBe('prepare');
    scheduler.stop();
  });
});

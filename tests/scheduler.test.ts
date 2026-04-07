/**
 * Scheduler tests.
 *
 * These are written around the invariants rather than around the methods, because the
 * invariants are what a durable control plane is for. The ones worth breaking a build
 * over: a dependent never starts before its dependency succeeded, a terminal workflow
 * never runs anything again, and a completion delivered twice executes downstream work
 * once.
 *
 * Every delay is virtual. Nothing here sleeps.
 */

import { describe, expect, it } from 'vitest';
import { MemoryEventStore } from '../src/events/memory-store.js';
import type { Event, EventStore } from '../src/events/types.js';
import { FakeClock } from '../src/scheduler/clock.js';
import {
  Scheduler,
  StepFailure,
  type DispatchRequest,
  type Dispatcher,
  type SchedulerOptions,
} from '../src/scheduler/scheduler.js';
import { project } from '../src/state/project.js';
import {
  stepRefKey,
  type RetryPolicy,
  type StepName,
  type WorkflowDef,
} from '../src/workflow/types.js';

const LEASE_MS = 1_000;
const ATTEMPT_MS = 10_000;
const START = 100_000;

const RETRY: RetryPolicy = {
  maxAttempts: 3,
  initialBackoffMs: 500,
  maxBackoffMs: 10_000,
  jitter: 0,
  retryableErrors: ['unavailable', 'timeout'],
};

/**
 * Lets queued promise callbacks run to completion.
 *
 * The store resolves without ever awaiting I/O, so one turn of the macrotask queue
 * drains the whole append-and-re-tick chain. Repeating it is cheap insurance, and it is
 * a yield rather than a delay — no elapsed wall-clock time is involved.
 */
async function settle(turns = 4): Promise<void> {
  for (let i = 0; i < turns; i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}

/** A dispatcher whose every step is resolved by the test, one at a time. */
function controllable(): {
  dispatch: Dispatcher;
  calls: DispatchRequest[];
  countOf: (step: StepName) => number;
  started: (step: StepName) => boolean;
  succeed: (step: StepName, output?: unknown) => Promise<void>;
  pendingSteps: () => StepName[];
  fail: (step: StepName, errorClass: string, message?: string) => Promise<void>;
  autoSucceed: Set<StepName>;
} {
  const calls: DispatchRequest[] = [];
  const pending = new Map<string, { resolve: (v: unknown) => void; reject: (e: unknown) => void }>();
  const autoSucceed = new Set<StepName>();

  const dispatch: Dispatcher = (req) => {
    calls.push(req);
    if (autoSucceed.has(req.node.name)) return Promise.resolve({ step: req.node.name });
    return new Promise<unknown>((resolve, reject) => {
      pending.set(req.node.name, { resolve, reject });
    });
  };

  const take = (step: StepName) => {
    const entry = pending.get(step);
    if (!entry) throw new Error(`step "${step}" was never dispatched`);
    pending.delete(step);
    return entry;
  };

  return {
    dispatch,
    calls,
    autoSucceed,
    countOf: (step) => calls.filter((c) => c.node.name === step).length,
    pendingSteps: () => [...pending.keys()],
    started: (step) => calls.some((c) => c.node.name === step),
    succeed: async (step, output = { step }) => {
      take(step).resolve(output);
      await settle();
    },
    fail: async (step, errorClass, message = `${step} failed`) => {
      take(step).reject(new StepFailure(errorClass, message));
      await settle();
    },
  };
}

function diamond(overrides: Partial<WorkflowDef> = {}): WorkflowDef {
  return {
    name: 'diamond',
    version: 1,
    start: 'ingest',
    nodes: [
      { name: 'ingest', kind: 'tool', tool: 'read', dependsOn: [] },
      { name: 'left', kind: 'llm', dependsOn: ['ingest'] },
      { name: 'right', kind: 'llm', dependsOn: ['ingest'] },
      { name: 'merge', kind: 'finalize', dependsOn: ['left', 'right'] },
    ],
    timeouts: { attemptTimeoutMs: ATTEMPT_MS, leaseDurationMs: LEASE_MS },
    ...overrides,
  };
}

function harness(
  opts: Partial<SchedulerOptions> & { clockStart?: number } = {},
): {
  store: EventStore;
  clock: FakeClock;
  worker: ReturnType<typeof controllable>;
  scheduler: Scheduler;
} {
  const store = opts.store ?? new MemoryEventStore();
  const clock = (opts.clock as FakeClock | undefined) ?? new FakeClock(opts.clockStart ?? START);
  const worker = controllable();
  const scheduler = new Scheduler({
    store,
    clock,
    dispatch: opts.dispatch ?? worker.dispatch,
    ...opts,
  });
  return { store, clock, worker, scheduler };
}

function typesOf(events: readonly Event[]): string[] {
  return events.map((e) => e.type);
}

function stateOf(def: WorkflowDef, events: readonly Event[]): string {
  return project(def, events).state;
}

describe('dependency ordering', () => {
  it('holds a dependent until its dependency has succeeded, over a diamond', async () => {
    const def = diamond();
    const { store, scheduler, worker } = harness();

    await scheduler.startAs('wf', def);
    await scheduler.tick();

    expect(worker.countOf('ingest')).toBe(1);
    expect(worker.started('left')).toBe(false);
    expect(worker.started('right')).toBe(false);

    await worker.succeed('ingest', { rows: 3 });

    // Both branches of the fan-out become eligible at once, and each is given the
    // dependency's output so a handler never has to read the log itself.
    expect(worker.countOf('left')).toBe(1);
    expect(worker.countOf('right')).toBe(1);
    expect(worker.calls.find((c) => c.node.name === 'left')?.upstream.get('ingest')).toEqual({
      rows: 3,
    });
    expect(worker.started('merge')).toBe(false);

    await worker.succeed('left');
    // One branch is not enough: a fan-in waits for all of its dependencies.
    expect(worker.started('merge')).toBe(false);

    await worker.succeed('right');
    expect(worker.countOf('merge')).toBe(1);

    await worker.succeed('merge', { report: 'ok' });
    const events = await store.read('wf');
    expect(stateOf(def, events)).toBe('succeeded');
    expect(project(def, events).output).toEqual({ report: 'ok' });
    expect(scheduler.inFlightCount).toBe(0);
  });

  it('records a lease and a start for every dispatch', async () => {
    const { store, scheduler, worker } = harness();
    await scheduler.startAs('wf', diamond());
    await scheduler.tick();
    await worker.succeed('ingest');

    const types = typesOf(await store.read('wf'));
    expect(types.filter((t) => t === 'step.leased').length).toBeGreaterThanOrEqual(3);
    expect(types).toContain('step.started');
    expect(types).toContain('step.completed');
  });
});

describe('duplicate delivery', () => {
  /**
   * The headline invariant. At-least-once delivery means a completion *will* be seen
   * twice eventually; if that re-ran the dependents, every retried message would fan out
   * a second copy of the rest of the workflow. The guard is that a dependent can only
   * start by winning a lease, and the lease is already taken.
   */
  it('does not execute downstream steps twice when a completion is delivered twice', async () => {
    const def = diamond();
    const { store, scheduler, worker } = harness();
    await scheduler.startAs('wf', def);
    await scheduler.tick();
    await worker.succeed('ingest', { rows: 1 });

    expect(worker.countOf('left')).toBe(1);

    // The same completion again, byte for byte, as a redelivery would arrive.
    const completed = (await store.read('wf')).find(
      (e) => e.type === 'step.completed' && e.step === 'ingest',
    );
    expect(completed?.type).toBe('step.completed');
    if (completed?.type !== 'step.completed') return;
    await store.append('wf', await store.head('wf'), [
      {
        type: 'step.completed',
        workflowId: 'wf',
        at: completed.at,
        step: 'ingest',
        attempt: completed.attempt,
        workerId: completed.workerId,
        output: completed.output,
      },
    ]);

    await scheduler.tick();
    await settle();

    expect(worker.countOf('left')).toBe(1);
    expect(worker.countOf('right')).toBe(1);
    expect(worker.countOf('ingest')).toBe(1);
  });

  it('does not re-dispatch a step that is already running', async () => {
    const { scheduler, worker } = harness();
    await scheduler.startAs('wf', diamond());

    await scheduler.tick();
    await scheduler.tick();
    await scheduler.tick();

    expect(worker.countOf('ingest')).toBe(1);
  });
});

describe('cancellation', () => {
  it('stops new steps being scheduled and asks running ones to stop', async () => {
    const def = diamond();
    const { store, scheduler, worker } = harness();
    await scheduler.startAs('wf', def);
    await scheduler.tick();

    const running = worker.calls[0];
    expect(running?.signal.aborted).toBe(false);

    await scheduler.cancel('wf', 'operator pulled it');

    expect(running?.signal.aborted).toBe(true);
    // The slot is given back immediately: a cancelled workflow must stop consuming
    // capacity, not wait for a handler to notice.
    expect(scheduler.inFlightCount).toBe(0);

    await scheduler.tick();
    expect(worker.countOf('left')).toBe(0);
    expect(stateOf(def, await store.read('wf'))).toBe('cancelled');
  });

  /**
   * The transition the state machine exists to forbid. A completion that was in flight
   * when the cancellation landed is recorded by whoever sent it, and must advance
   * nothing: no lease, no start, no downstream dispatch.
   */
  it('a terminal workflow never returns to running', async () => {
    const { store, scheduler, worker } = harness();
    await scheduler.startAs('wf', diamond());
    await scheduler.tick();
    await scheduler.cancel('wf', 'operator pulled it');

    const head = await store.head('wf');
    await store.append('wf', head, [
      {
        type: 'step.completed',
        workflowId: 'wf',
        at: 1,
        step: 'ingest',
        attempt: 1,
        workerId: 'late-worker',
        output: { rows: 9 },
      },
    ]);

    await scheduler.tick();
    await settle();

    const after = typesOf((await store.read('wf')).slice(head));
    expect(after).toEqual(['step.completed']);
    expect(worker.started('left')).toBe(false);
    expect(worker.started('right')).toBe(false);
    // A cancelled workflow is not active, so recovery will not pick it up either.
    expect(await store.listActive()).not.toContain('wf');
  });

  it('cancels mid-retry: the scheduled retry never runs', async () => {
    const def = diamond({
      nodes: [
        { name: 'ingest', kind: 'tool', tool: 'read', dependsOn: [], retry: RETRY },
        { name: 'left', kind: 'llm', dependsOn: ['ingest'] },
        { name: 'right', kind: 'llm', dependsOn: ['ingest'] },
        { name: 'merge', kind: 'finalize', dependsOn: ['left', 'right'] },
      ],
    });
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', def);
    await scheduler.tick();

    await worker.fail('ingest', 'unavailable');
    expect(typesOf(await store.read('wf'))).toContain('step.retry_scheduled');

    await scheduler.cancel('wf', 'no longer needed');

    // Past the backoff, so the retry timer fires against a cancelled workflow.
    await clock.advance(RETRY.initialBackoffMs * 4);
    await scheduler.tick();

    expect(worker.countOf('ingest')).toBe(1);
    expect(stateOf(def, await store.read('wf'))).toBe('cancelled');
  });
});

describe('retries', () => {
  const def = diamond({
    nodes: [
      { name: 'ingest', kind: 'tool', tool: 'read', dependsOn: [], retry: RETRY },
      { name: 'left', kind: 'llm', dependsOn: ['ingest'] },
      { name: 'right', kind: 'llm', dependsOn: ['ingest'] },
      { name: 'merge', kind: 'finalize', dependsOn: ['left', 'right'] },
    ],
  });

  it('retries a retryable failure and succeeds on the next attempt', async () => {
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', def);
    await scheduler.tick();

    await worker.fail('ingest', 'unavailable');
    // The backoff is respected: nothing is dispatched at the instant of the failure.
    expect(worker.countOf('ingest')).toBe(1);

    await clock.advance(RETRY.initialBackoffMs * 2);

    expect(worker.countOf('ingest')).toBe(2);
    const second = worker.calls.filter((c) => c.node.name === 'ingest')[1];
    // A new attempt number, so the retry derives a fresh idempotency key rather than
    // being deduplicated against the attempt that failed.
    expect(second?.ref.attempt).toBe(2);
    expect(stepRefKey(second!.ref)).toBe('wf/ingest/2');

    await worker.succeed('ingest');
    expect(worker.countOf('left')).toBe(1);
    expect(project(def, await store.read('wf')).steps.get('ingest')?.state).toBe('succeeded');
  });

  it('fails a non-retryable failure immediately, with no retry event', async () => {
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', def);
    await scheduler.tick();

    await worker.fail('ingest', 'invalid_args');

    const types = typesOf(await store.read('wf'));
    expect(types).not.toContain('step.retry_scheduled');
    expect(types).toContain('workflow.failed');

    await clock.advance(60_000);
    expect(worker.countOf('ingest')).toBe(1);
    expect(stateOf(def, await store.read('wf'))).toBe('failed');
  });

  it('gives up once maxAttempts is exhausted', async () => {
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', def);
    await scheduler.tick();

    for (let attempt = 1; attempt <= RETRY.maxAttempts; attempt++) {
      await worker.fail('ingest', 'unavailable');
      await clock.advance(RETRY.maxBackoffMs);
    }

    expect(worker.countOf('ingest')).toBe(RETRY.maxAttempts);
    expect(stateOf(def, await store.read('wf'))).toBe('failed');
    expect(worker.started('left')).toBe(false);
  });

  it('turns an attempt that overruns its own timeout into a retry', async () => {
    const quick = diamond({
      nodes: [
        {
          name: 'ingest',
          kind: 'tool',
          tool: 'read',
          dependsOn: [],
          retry: RETRY,
          attemptTimeoutMs: 200,
        },
        { name: 'left', kind: 'llm', dependsOn: ['ingest'] },
        { name: 'right', kind: 'llm', dependsOn: ['ingest'] },
        { name: 'merge', kind: 'finalize', dependsOn: ['left', 'right'] },
      ],
      timeouts: { attemptTimeoutMs: ATTEMPT_MS, leaseDurationMs: 60_000 },
    });
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', quick);
    await scheduler.tick();

    // Past the attempt budget but nowhere near the lease duration: these are different
    // timeouts with different meanings, and only the attempt one has elapsed.
    await clock.advance(201);

    const failed = (await store.read('wf')).find((e) => e.type === 'step.failed');
    expect(failed?.type === 'step.failed' ? failed.errorClass : '').toBe('timeout');
    expect(worker.calls[0]?.signal.aborted).toBe(true);

    // Exactly the backoff, not more: a longer jump would carry the second attempt past
    // its own 200ms budget and time that one out too.
    await clock.advance(RETRY.initialBackoffMs * 2);
    expect(worker.countOf('ingest')).toBe(2);
  });

  it('does not schedule a retry past the workflow deadline', async () => {
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', def, { deadlineAt: START + 100 });
    await scheduler.tick();

    await worker.fail('ingest', 'unavailable');

    const types = typesOf(await store.read('wf'));
    expect(types).not.toContain('step.retry_scheduled');
    await clock.advance(60_000);
    expect(worker.countOf('ingest')).toBe(1);
  });
});

describe('workflow deadline', () => {
  it('stops starting steps and fails the execution once the deadline passes', async () => {
    const def = diamond();
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', def, { deadlineAt: START + 500 });
    await scheduler.tick();
    expect(worker.countOf('ingest')).toBe(1);

    await worker.succeed('ingest');
    expect(worker.countOf('left')).toBe(1);

    clock.jump(1_000);
    await scheduler.tick();

    const failed = (await store.read('wf')).find((e) => e.type === 'workflow.failed');
    expect(failed?.type === 'workflow.failed' ? failed.errorClass : '').toBe('deadline_exceeded');
    expect(stateOf(def, await store.read('wf'))).toBe('failed');
    // Capacity is returned: there is no point holding workers for an execution whose
    // answer is already too late to use.
    expect(scheduler.inFlightCount).toBe(0);
  });
});

describe('lease expiry', () => {
  it('returns the step to the ready set and lets a second worker finish it', async () => {
    const def = diamond();
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', def);
    await scheduler.tick();

    const first = worker.calls[0];
    expect(first?.workerId).toBeTruthy();

    // The worker stops heartbeating. Stopping the heartbeat is modelled by stopping the
    // scheduler's timers for it, which is what a killed process looks like from here.
    scheduler.stop();
    await clock.advance(LEASE_MS + 1);

    const takeover = harness({ store, clock, schedulerId: 'scheduler-2' });
    takeover.scheduler.register(def);
    await takeover.scheduler.tick();

    const types = typesOf(await store.read('wf'));
    expect(types).toContain('step.lease_expired');
    // Reassignment, not failure: the first attempt may still be running somewhere.
    expect(types).not.toContain('step.failed');

    expect(takeover.worker.countOf('ingest')).toBe(1);
    const second = takeover.worker.calls[0];
    expect(second?.workerId).not.toBe(first?.workerId);
    // Same attempt number, so the reassigned run derives the same idempotency key.
    expect(second?.ref.attempt).toBe(first?.ref.attempt);

    await takeover.worker.succeed('ingest');
    expect(takeover.worker.countOf('left')).toBe(1);
  });

  it('keeps the lease while the worker heartbeats', async () => {
    const { store, clock, scheduler, worker } = harness();
    await scheduler.startAs('wf', diamond());
    await scheduler.tick();

    // Several lease durations with heartbeats firing from the clock.
    await clock.advance(LEASE_MS * 3);

    expect(typesOf(await store.read('wf'))).not.toContain('step.lease_expired');
    expect(worker.countOf('ingest')).toBe(1);
    expect(scheduler.inFlightCount).toBe(1);
    scheduler.stop();
  });
});

describe('concurrency limits', () => {
  const fanOut = (maxConcurrency?: number): WorkflowDef => ({
    name: 'fan-out',
    version: 1,
    start: 'a',
    nodes: [
      { name: 'a', kind: 'tool', tool: 't', dependsOn: [] },
      { name: 'b', kind: 'tool', tool: 't', dependsOn: [] },
      { name: 'c', kind: 'tool', tool: 't', dependsOn: [] },
    ],
    timeouts: { attemptTimeoutMs: ATTEMPT_MS, leaseDurationMs: LEASE_MS },
    ...(maxConcurrency !== undefined ? { maxConcurrency } : {}),
  });

  it('enforces maxConcurrency per workflow', async () => {
    const { scheduler, worker } = harness();
    await scheduler.startAs('wf', fanOut(2));

    await scheduler.tick();

    expect(worker.calls).toHaveLength(2);
    await worker.succeed('a');
    expect(worker.calls).toHaveLength(3);
    scheduler.stop();
  });

  it('enforces a global in-flight cap across workflows', async () => {
    const { scheduler, worker } = harness({ maxInFlight: 2 });
    await scheduler.startAs('wf-1', fanOut());
    await scheduler.startAs('wf-2', { ...fanOut(), name: 'fan-out-2' });

    await scheduler.tick();

    expect(scheduler.inFlightCount).toBe(2);
    expect(worker.calls).toHaveLength(2);
    scheduler.stop();
  });
});

describe('priority', () => {
  const roots = (name: string, count: number): WorkflowDef => ({
    name,
    version: 1,
    start: 's0',
    nodes: Array.from({ length: count }, (_, i) => ({
      name: `s${i}`,
      kind: 'tool' as const,
      tool: 't',
      dependsOn: [],
    })),
    timeouts: { attemptTimeoutMs: ATTEMPT_MS, leaseDurationMs: 60_000 },
  });

  /**
   * Bounded preference, not strict precedence. Strict priority is simpler and starves:
   * a steady stream of important work means an ordinary workflow never runs at all, which
   * from the outside is indistinguishable from the system having lost it.
   */
  it('prefers the higher-priority workflow without starving the ordinary one', async () => {
    const store = new MemoryEventStore();
    const clock = new FakeClock(START);
    const busy = controllable();
    const quiet = controllable();

    // One worker between them, so every dispatch is a decision about who is starved.
    const scheduler = new Scheduler({
      store,
      clock,
      maxInFlight: 1,
      starvationWeight: 1,
      dispatch: (req) =>
        req.ref.workflowId === 'wf-hot' ? busy.dispatch(req) : quiet.dispatch(req),
    });

    await scheduler.startAs('wf-hot', roots('hot', 12), { priority: 3 });
    await scheduler.startAs('wf-cold', roots('cold', 1));

    await scheduler.tick();
    // The first contested slot goes to the higher priority. That is the preference.
    expect(busy.calls).toHaveLength(1);
    expect(quiet.calls).toHaveLength(0);

    await busy.succeed('s0');
    // So does the slot it gave back.
    expect(busy.calls).toHaveLength(2);
    expect(quiet.calls).toHaveLength(0);

    let servedAt = 0;
    for (let round = 1; round <= 10 && servedAt === 0; round++) {
      await scheduler.tick();
      if (quiet.calls.length > 0) servedAt = round;
      else for (const step of busy.pendingSteps()) await busy.succeed(step);
    }

    // And the bound: the ordinary workflow is served within a few rounds rather than
    // never, while the high-priority workflow still has ready work queued behind it.
    expect(servedAt).toBeGreaterThan(0);
    expect(servedAt).toBeLessThanOrEqual(10);
    expect(busy.calls.length).toBeGreaterThan(quiet.calls.length);
    expect(busy.calls.length).toBeLessThan(12);
    scheduler.stop();
  });
});

describe('approval gates', () => {
  const gated: WorkflowDef = {
    name: 'gated',
    version: 1,
    start: 'draft',
    nodes: [
      { name: 'draft', kind: 'llm', dependsOn: [] },
      {
        name: 'gate',
        kind: 'approval',
        dependsOn: ['draft'],
        approvers: ['alice'],
        approvalPrompt: 'publish this?',
      },
      { name: 'publish', kind: 'tool', tool: 'publish', dependsOn: ['gate'] },
    ],
    timeouts: { attemptTimeoutMs: ATTEMPT_MS, leaseDurationMs: 60_000 },
  };

  async function waiting(): Promise<ReturnType<typeof harness>> {
    const h = harness({ maxInFlight: 1 });
    await h.scheduler.startAs('wf', gated);
    await h.scheduler.tick();
    await h.worker.succeed('draft', { text: 'hello' });
    return h;
  }

  /**
   * An approval is a wait on a human, which can take days. Holding a worker for it would
   * idle capacity for exactly as long, so the gate requests the approval and occupies
   * nothing.
   */
  it('a workflow waiting on approval occupies no worker', async () => {
    const { store, scheduler, worker } = await waiting();

    const types = typesOf(await store.read('wf'));
    expect(types).toContain('approval.requested');
    expect(worker.started('gate')).toBe(false);
    expect(scheduler.inFlightCount).toBe(0);

    const projection = project(gated, await store.read('wf'));
    expect(projection.steps.get('gate')?.state).toBe('waiting');
    expect(projection.steps.get('gate')?.awaitingApproval).toBe(true);

    // The single worker is genuinely free: another workflow can use it while this one
    // waits.
    const other: WorkflowDef = {
      name: 'other',
      version: 1,
      start: 'work',
      nodes: [{ name: 'work', kind: 'tool', tool: 't', dependsOn: [] }],
      timeouts: { attemptTimeoutMs: ATTEMPT_MS, leaseDurationMs: 60_000 },
    };
    await scheduler.startAs('wf-other', other);
    await scheduler.tick();
    expect(worker.countOf('work')).toBe(1);
    scheduler.stop();
  });

  it('asks only once, however many ticks pass', async () => {
    const { store, scheduler } = await waiting();
    await scheduler.tick();
    await scheduler.tick();

    const requests = (await store.read('wf')).filter((e) => e.type === 'approval.requested');
    expect(requests).toHaveLength(1);
  });

  it('runs the protected step once approval is granted', async () => {
    const { store, scheduler, worker } = await waiting();

    expect(await scheduler.approve('wf', 'gate', 'alice', 'looks right')).toBe(true);

    expect(worker.countOf('publish')).toBe(1);
    await worker.succeed('publish');
    expect(stateOf(gated, await store.read('wf'))).toBe('succeeded');
  });

  it('denied approval prevents the protected step from ever running', async () => {
    const { store, clock, scheduler, worker } = await waiting();

    expect(await scheduler.deny('wf', 'gate', 'alice', 'legal said no')).toBe(true);

    await scheduler.tick();
    await clock.advance(60_000);
    await scheduler.tick();

    expect(worker.started('publish')).toBe(false);
    const projection = project(gated, await store.read('wf'));
    expect(projection.state).toBe('failed');
    expect(projection.steps.get('gate')?.errorClass).toBe('approval_denied');
    // Terminal, so the protected step cannot be reached by any later event.
    expect(projection.steps.get('publish')?.state).toBe('pending');
  });

  /**
   * A human answering a question that has been withdrawn. Acting on it would restart a
   * cancelled workflow, so the grant is recorded nowhere and changes nothing.
   */
  it('ignores an approval that arrives after cancellation', async () => {
    const { store, scheduler, worker } = await waiting();
    await scheduler.cancel('wf', 'superseded');
    const head = await store.head('wf');

    expect(await scheduler.approve('wf', 'gate', 'alice')).toBe(false);

    expect((await store.read('wf', head)).length).toBe(0);
    expect(worker.started('publish')).toBe(false);
    expect(stateOf(gated, await store.read('wf'))).toBe('cancelled');
  });

  it('ignores a denial that arrives after cancellation', async () => {
    const { store, scheduler } = await waiting();
    await scheduler.cancel('wf', 'superseded');
    const head = await store.head('wf');

    expect(await scheduler.deny('wf', 'gate', 'alice', 'too late')).toBe(false);
    expect((await store.read('wf', head)).length).toBe(0);
  });
});

describe('crash recovery', () => {
  /**
   * Recovery is a replay and nothing else. The second scheduler is given the store and no
   * other information — no handover, no reconciliation pass — and has to arrive at the
   * same place the first one was.
   */
  it('rebuilds from the store with no in-memory state and finishes the workflow', async () => {
    const def = diamond();
    const store = new MemoryEventStore();
    const clock = new FakeClock(START);

    // First process: completes `ingest`, dispatches both branches, then dies.
    const first = harness({ store, clock, schedulerId: 'scheduler-1' });
    await first.scheduler.startAs('wf', def);
    await first.scheduler.tick();
    await first.worker.succeed('ingest', { rows: 7 });
    await first.worker.succeed('left', { summary: 'l' });
    expect(first.worker.countOf('right')).toBe(1);
    first.scheduler.stop();

    // Everything the first process knew is gone: its leases, its timers, its maps.
    const history = await store.read('wf');
    expect(typesOf(history)).toContain('step.completed');

    const second = harness({ store, clock, schedulerId: 'scheduler-2' });
    second.scheduler.register(def);

    // The lease the dead process held is still live, so recovery correctly leaves the
    // step alone rather than double-dispatching it.
    await second.scheduler.recover();
    expect(second.worker.calls).toHaveLength(0);

    await clock.advance(LEASE_MS + 1);
    await second.scheduler.tick();

    // Once the lease lapses the step is reassigned, and the rest of the graph follows.
    expect(second.worker.countOf('right')).toBe(1);
    await second.worker.succeed('right', { summary: 'r' });
    expect(second.worker.countOf('merge')).toBe(1);
    await second.worker.succeed('merge', { report: 'done' });

    const projection = project(def, await store.read('wf'));
    expect(projection.state).toBe('succeeded');
    expect(projection.output).toEqual({ report: 'done' });
    // The output of the step the first process completed survived the restart, because it
    // was never held anywhere but the log.
    expect(projection.steps.get('left')?.output).toEqual({ summary: 'l' });
    second.scheduler.stop();
  });

  it('skips workflows that are already terminal', async () => {
    const store = new MemoryEventStore();
    const clock = new FakeClock(START);
    const first = harness({ store, clock });
    await first.scheduler.startAs('wf', diamond());
    await first.scheduler.tick();
    await first.scheduler.cancel('wf', 'done with it');
    first.scheduler.stop();

    const second = harness({ store, clock });
    second.scheduler.register(diamond());
    await second.scheduler.recover();

    expect(second.worker.calls).toHaveLength(0);
  });
});

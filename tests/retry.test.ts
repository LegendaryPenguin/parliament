/**
 * Retry engine tests.
 *
 * The interesting cases are the three different reasons to stop, because each one is a
 * different claim: the work will never succeed, the budget is spent, or the answer would
 * arrive too late to matter. A system that collapses them reports the wrong cause and
 * retries work whose deadline has already passed.
 */

import { describe, expect, it } from 'vitest';
import { MemoryEventStore } from '../src/events/memory-store.js';
import type { Event } from '../src/events/types.js';
import { FakeClock } from '../src/scheduler/clock.js';
import { RetryEngine, isRetryable, type FailureReport } from '../src/scheduler/retry.js';
import { project } from '../src/state/project.js';
import { backoffFor, type RetryPolicy, type WorkflowDef } from '../src/workflow/types.js';

const POLICY: RetryPolicy = {
  maxAttempts: 3,
  initialBackoffMs: 1_000,
  maxBackoffMs: 60_000,
  jitter: 0.2,
  retryableErrors: ['timeout', 'unavailable'],
};

const def: WorkflowDef = {
  name: 'retry-demo',
  version: 1,
  start: 'a',
  nodes: [{ name: 'a', kind: 'tool', tool: 'flaky', dependsOn: [], retry: POLICY }],
  timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: 1_000 },
};

const START = 10_000;

async function fixture(opts: { deadlineAt?: number; random?: () => number } = {}): Promise<{
  store: MemoryEventStore;
  clock: FakeClock;
  engine: RetryEngine;
  id: string;
  report: (over?: Partial<FailureReport>) => FailureReport;
}> {
  const store = new MemoryEventStore();
  const clock = new FakeClock(START);
  const id = 'wf-1';
  const engine = new RetryEngine({ store, clock, ...(opts.random ? { random: opts.random } : {}) });

  await store.append(id, 0, [
    {
      type: 'workflow.created',
      workflowId: id,
      at: clock.now(),
      workflow: def.name,
      definitionVersion: def.version,
      input: null,
      ...(opts.deadlineAt !== undefined ? { deadlineAt: opts.deadlineAt } : {}),
    },
    {
      type: 'step.scheduled',
      workflowId: id,
      at: clock.now(),
      step: 'a',
      attempt: 1,
      notBefore: clock.now(),
    },
    {
      type: 'step.started',
      workflowId: id,
      at: clock.now(),
      step: 'a',
      attempt: 1,
      workerId: 'w1',
    },
  ]);

  const report = (over: Partial<FailureReport> = {}): FailureReport => ({
    workflowId: id,
    step: 'a',
    attempt: 1,
    workerId: 'w1',
    error: 'upstream unavailable',
    errorClass: 'unavailable',
    policy: POLICY,
    ...(opts.deadlineAt !== undefined ? { deadlineAt: opts.deadlineAt } : {}),
    ...over,
  });

  return { store, clock, engine, id, report };
}

function typesOf(events: readonly Event[]): string[] {
  return events.map((e) => e.type);
}

describe('classification', () => {
  it('retries only the classes the policy lists', () => {
    expect(isRetryable(POLICY, 'unavailable')).toBe(true);
    expect(isRetryable(POLICY, 'invalid_args')).toBe(false);
  });

  /**
   * An unlisted class is not a near miss to be retried hopefully. A permanently
   * malformed request fails identically every time, so defaulting to retry turns one bad
   * input into an unbounded loop against a dependency that is working perfectly.
   */
  it('fails an unlisted class immediately, with no retry event', async () => {
    const { engine, store, id, report } = await fixture();

    const decision = await engine.recordFailure(report({ errorClass: 'invalid_args' }));

    expect(decision).toEqual({ kind: 'give_up', reason: 'not_retryable' });
    const types = typesOf(await store.read(id));
    expect(types).toContain('step.failed');
    expect(types).not.toContain('step.retry_scheduled');
    expect(types).toContain('workflow.failed');
  });

  it('schedules the next attempt for a retryable class', async () => {
    const { engine, store, id, report } = await fixture();

    const decision = await engine.recordFailure(report());

    expect(decision.kind).toBe('retry');
    if (decision.kind !== 'retry') return;
    expect(decision.attempt).toBe(2);
    expect(decision.notBefore).toBeGreaterThan(START);

    const events = await store.read(id);
    const scheduled = events.find((e) => e.type === 'step.retry_scheduled');
    expect(scheduled?.type === 'step.retry_scheduled' ? scheduled.attempt : 0).toBe(2);

    // The fold has to agree that the step is runnable again, otherwise the retry is
    // recorded and never picked up.
    const projection = project(def, events);
    expect(projection.steps.get('a')?.state).toBe('ready');
    expect(projection.steps.get('a')?.attempt).toBe(2);
    expect(projection.steps.get('a')?.notBefore).toBe(decision.notBefore);
  });

  it('stops once maxAttempts is exhausted', async () => {
    const { engine, store, id, report } = await fixture();

    const decision = await engine.recordFailure(report({ attempt: POLICY.maxAttempts }));

    expect(decision).toEqual({ kind: 'give_up', reason: 'attempts_exhausted' });
    expect(typesOf(await store.read(id))).not.toContain('step.retry_scheduled');
  });

  /**
   * The case the four-timeout split exists for. The error is retryable and the attempt
   * budget has room, and the retry is still refused, because the backoff would carry it
   * past the point where anyone wants the answer.
   */
  it('does not schedule a retry that would land past the workflow deadline', async () => {
    const deadlineAt = START + 500;
    const { engine, store, id, report } = await fixture({ deadlineAt });
    const before = (await store.read(id)).length;

    const decision = await engine.recordFailure(report());

    expect(decision).toEqual({ kind: 'give_up', reason: 'past_deadline' });
    const after = typesOf((await store.read(id)).slice(before));
    // Nothing was queued for a later attempt: no retry notice and no re-scheduling.
    expect(after).toEqual(['step.failed', 'workflow.failed']);
  });

  it('still retries when the backoff lands inside the deadline', async () => {
    const { engine, report } = await fixture({ deadlineAt: START + 60_000 });
    expect((await engine.recordFailure(report())).kind).toBe('retry');
  });

  it('records nothing once the workflow is terminal', async () => {
    const { engine, store, clock, id, report } = await fixture();
    await store.append(id, await store.head(id), [
      { type: 'workflow.cancelled', workflowId: id, at: clock.now(), reason: 'operator' },
    ]);
    const before = (await store.read(id)).length;

    const decision = await engine.recordFailure(report());

    expect(decision).toEqual({ kind: 'give_up', reason: 'workflow_terminal' });
    // In particular a cancelled workflow is not dragged into `failed`.
    expect((await store.read(id)).length).toBe(before);
  });
});

describe('willRetry', () => {
  /**
   * The invariant the field exists for. `failed` is terminal, so the fold has to know
   * from the event alone whether a retry is coming; if the flag and the following events
   * could disagree, a replay would fold to a different state than the live run did.
   */
  const cases: ReadonlyArray<{ name: string; over: Partial<FailureReport>; deadlineAt?: number }> = [
    { name: 'retryable with budget left', over: {} },
    { name: 'non-retryable class', over: { errorClass: 'invalid_args' } },
    { name: 'attempt budget exhausted', over: { attempt: POLICY.maxAttempts } },
    { name: 'retry would land past the deadline', over: {}, deadlineAt: START + 100 },
  ];

  for (const { name, over, deadlineAt } of cases) {
    it(`agrees with what follows: ${name}`, async () => {
      const { engine, store, id, report } = await fixture(
        deadlineAt !== undefined ? { deadlineAt } : {},
      );

      await engine.recordFailure(report(over));

      const events = await store.read(id);
      const failed = events.find((e) => e.type === 'step.failed');
      expect(failed?.type).toBe('step.failed');
      if (failed?.type !== 'step.failed') return;

      const retryFollows = events.some(
        (e) =>
          e.type === 'step.retry_scheduled' &&
          e.step === failed.step &&
          e.sequence > failed.sequence,
      );
      expect(failed.willRetry).toBe(retryFollows);
    });
  }

  it('never retries a class that cannot improve, whatever the policy lists', async () => {
    const permissive: RetryPolicy = {
      ...POLICY,
      // A policy author who lists these has made a mistake the engine should not honour.
      retryableErrors: ['definition_error', 'cancelled', 'lease_lost', 'unavailable'],
    };
    const { engine, store, id, report } = await fixture();

    for (const errorClass of ['definition_error', 'cancelled', 'lease_lost']) {
      expect(isRetryable(permissive, errorClass)).toBe(false);
    }

    const decision = await engine.recordFailure(
      report({ errorClass: 'definition_error', policy: permissive }),
    );
    expect(decision).toEqual({ kind: 'give_up', reason: 'not_retryable' });
    expect(typesOf(await store.read(id))).not.toContain('step.retry_scheduled');
  });
});

describe('backoff', () => {
  it('grows exponentially with the attempt number', async () => {
    // A fixed random keeps the jitter band from hiding the growth.
    const { engine, report } = await fixture({ random: () => 0.5 });

    const delays = [1, 2, 3, 4].map((attempt) => {
      const decision = engine.decide(report({ attempt, policy: { ...POLICY, maxAttempts: 99 } }), START);
      return decision.kind === 'retry' ? decision.backoffMs : -1;
    });

    // The delay is computed for the attempt that will run next, so failing attempt 1
    // waits one doubling rather than the base interval.
    expect(delays).toEqual([2_000, 4_000, 8_000, 16_000]);
  });

  it('is capped by maxBackoffMs', () => {
    const capped: RetryPolicy = { ...POLICY, maxBackoffMs: 3_000, jitter: 0 };
    expect(backoffFor(capped, 10, () => 0.5)).toBe(3_000);
  });

  /**
   * Jitter is not decoration. Without it, every step that failed against the same
   * downstream outage retries at the same instant forever, which turns one outage into a
   * synchronized herd that keeps it down.
   */
  it('is jittered: the same attempt spreads across a band', async () => {
    const low = (await fixture({ random: () => 0 })).engine;
    const high = (await fixture({ random: () => 1 })).engine;
    const mid = (await fixture({ random: () => 0.5 })).engine;
    const report = (await fixture()).report;

    const at = (engine: RetryEngine): number => {
      const d = engine.decide(report({ attempt: 2 }), START);
      return d.kind === 'retry' ? d.backoffMs : -1;
    };

    const raw = POLICY.initialBackoffMs * 2 ** 2;
    const spread = raw * POLICY.jitter;

    expect(at(low)).toBe(raw - spread / 2);
    expect(at(high)).toBe(raw + spread / 2);
    expect(at(mid)).toBe(raw);
    expect(at(low)).toBeLessThan(at(high));
  });
});

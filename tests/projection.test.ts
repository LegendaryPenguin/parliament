/**
 * The fold, readiness, and replay.
 *
 * These tests are mostly about the properties the rest of the control plane assumes:
 * the fold is deterministic, duplicates are absorbed, illegal transitions are refused
 * loudly, and a lease expiry is a reassignment rather than a failure.
 */

import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import * as path from 'node:path';

import { describe, expect, it } from 'vitest';

import { FileEventStore } from '../src/events/file-store.js';
import { MemoryEventStore } from '../src/events/memory-store.js';
import { EVENT_VERSION } from '../src/events/types.js';
import type { Event, NewEvent, StepProjection, WorkflowProjection } from '../src/events/types.js';
import { IllegalTransitionError, ProjectionError, project } from '../src/state/project.js';
import { bindProjection } from '../src/api/projection.js';
import { readySteps } from '../src/state/ready.js';
import { renderTimeline, replayTo, timeline } from '../src/state/replay.js';
import { validateDef } from '../src/workflow/types.js';
import type { RetryPolicy, WorkflowDef } from '../src/workflow/types.js';

const WF = 'wf-1';

/** A submission with the workflow id left for `history` to fill in. */
type Submitted<T extends Event = Event> = T extends Event
  ? Omit<NewEvent<T>, 'workflowId'>
  : never;

/** Assigns sequences the way a store would, so a history can be written inline. */
function history(...events: readonly Submitted<Event>[]): Event[] {
  return events.map((event, i) => ({
    workflowId: WF,
    ...event,
    version: EVENT_VERSION,
    sequence: i + 1,
  }));
}

const RETRY: RetryPolicy = {
  maxAttempts: 3,
  initialBackoffMs: 100,
  maxBackoffMs: 1_000,
  jitter: 0,
  retryableErrors: ['timeout'],
};

const ingest: WorkflowDef = {
  name: 'ingest',
  version: 1,
  start: 'fetch',
  timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: 10_000 },
  nodes: [
    { name: 'fetch', kind: 'tool', tool: 'http.get', dependsOn: [], retry: RETRY },
    { name: 'summarize', kind: 'llm', dependsOn: ['fetch'] },
    {
      name: 'approve',
      kind: 'approval',
      dependsOn: ['summarize'],
      approvers: ['nrawal'],
      approvalPrompt: 'publish this summary?',
    },
    { name: 'publish', kind: 'tool', tool: 'http.post', dependsOn: ['approve'] },
  ],
};

const diamond: WorkflowDef = {
  name: 'diamond',
  version: 1,
  start: 'a',
  timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: 10_000 },
  nodes: [
    { name: 'a', kind: 'tool', tool: 'noop', dependsOn: [] },
    { name: 'b', kind: 'tool', tool: 'noop', dependsOn: ['a'] },
    { name: 'c', kind: 'tool', tool: 'noop', dependsOn: ['a'] },
    { name: 'd', kind: 'join', dependsOn: ['b', 'c'] },
  ],
};

/** A 2-node chain, standing in for a definition that has been edited since a history ran. */
const chainStandIn: WorkflowDef = {
  name: 'chain',
  version: 2,
  start: 'a',
  timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: 10_000 },
  nodes: [
    { name: 'a', kind: 'tool', tool: 'noop', dependsOn: [] },
    { name: 'z', kind: 'tool', tool: 'noop', dependsOn: ['a'] },
  ],
};

const created: Submitted<Event> = {
  type: 'workflow.created',
  at: 1_000,
  workflow: 'ingest',
  definitionVersion: 1,
  input: { url: 'https://example.test' },
};

function step(name: string, projection: WorkflowProjection): StepProjection {
  const found = projection.steps.get(name);
  if (!found) throw new Error(`no step ${name} in projection`);
  return found;
}

/** The happy path for `ingest`, including the approval gate. */
const HAPPY: readonly Submitted<Event>[] = [
  created,
  { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
  {
    type: 'step.leased',
    at: 1_002,
    step: 'fetch',
    attempt: 1,
    workerId: 'w1',
    leaseExpiresAt: 11_002,
  },
  { type: 'step.started', at: 1_003, step: 'fetch', attempt: 1, workerId: 'w1' },
  {
    type: 'tool.requested',
    at: 1_004,
    step: 'fetch',
    attempt: 1,
    tool: 'http.get',
    args: { url: 'https://example.test' },
    idempotencyKey: 'wf-1/fetch/1',
  },
  {
    type: 'tool.succeeded',
    at: 1_005,
    step: 'fetch',
    attempt: 1,
    tool: 'http.get',
    result: { status: 200 },
    durationMs: 12,
    deduplicated: false,
  },
  {
    type: 'step.completed',
    at: 1_006,
    step: 'fetch',
    attempt: 1,
    workerId: 'w1',
    output: { body: 'hello' },
  },
  { type: 'step.scheduled', at: 1_007, step: 'summarize', attempt: 1 },
  { type: 'step.started', at: 1_008, step: 'summarize', attempt: 1, workerId: 'w1' },
  {
    type: 'step.completed',
    at: 1_009,
    step: 'summarize',
    attempt: 1,
    workerId: 'w1',
    output: { summary: 'hi' },
  },
  { type: 'step.scheduled', at: 1_010, step: 'approve', attempt: 1 },
  { type: 'step.started', at: 1_011, step: 'approve', attempt: 1, workerId: 'w2' },
  {
    type: 'approval.requested',
    at: 1_012,
    step: 'approve',
    approvers: ['nrawal'],
    prompt: 'publish this summary?',
    payload: { summary: 'hi' },
  },
  { type: 'approval.granted', at: 1_013, step: 'approve', approver: 'nrawal' },
  {
    type: 'step.completed',
    at: 1_014,
    step: 'approve',
    attempt: 1,
    workerId: 'w2',
    output: { approved: true },
  },
  { type: 'step.scheduled', at: 1_015, step: 'publish', attempt: 1 },
  { type: 'step.started', at: 1_016, step: 'publish', attempt: 1, workerId: 'w2' },
  {
    type: 'step.completed',
    at: 1_017,
    step: 'publish',
    attempt: 1,
    workerId: 'w2',
    output: { id: 'post-1' },
  },
  { type: 'workflow.completed', at: 1_018, output: { id: 'post-1' } },
];

const happyPath = history(...HAPPY);

/** Up to and including the outstanding approval request. */
const UP_TO_APPROVAL = HAPPY.slice(0, 13);

const RETRYABLE_FAILURE: readonly Submitted<Event>[] = [
  created,
  { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
  { type: 'step.started', at: 1_002, step: 'fetch', attempt: 1, workerId: 'w1' },
  {
    type: 'step.failed',
    at: 1_003,
    step: 'fetch',
    attempt: 1,
    workerId: 'w1',
    error: 'upstream timed out',
    errorClass: 'timeout',
    willRetry: true,
  },
];

const LEASE_LOST: readonly Submitted<Event>[] = [
  created,
  { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
  {
    type: 'step.leased',
    at: 1_002,
    step: 'fetch',
    attempt: 1,
    workerId: 'w1',
    leaseExpiresAt: 11_002,
  },
  { type: 'step.started', at: 1_003, step: 'fetch', attempt: 1, workerId: 'w1' },
  { type: 'step.lease_expired', at: 11_003, step: 'fetch', attempt: 1, workerId: 'w1' },
];

const FETCH_DONE: readonly Submitted<Event>[] = [
  created,
  { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
  { type: 'step.started', at: 1_002, step: 'fetch', attempt: 1, workerId: 'w1' },
  {
    type: 'step.completed',
    at: 1_003,
    step: 'fetch',
    attempt: 1,
    workerId: 'w1',
    output: { body: 'first' },
  },
];

describe('fixtures', () => {
  it('uses valid definitions', () => {
    expect(validateDef(ingest)).toEqual([]);
    expect(validateDef(diamond)).toEqual([]);
  });
});

describe('project', () => {
  it('refuses a history that does not begin with workflow.created', () => {
    expect(() =>
      project(ingest, history({ type: 'step.scheduled', at: 1, step: 'fetch', attempt: 1 })),
    ).toThrow(ProjectionError);
    expect(() => project(ingest, [])).toThrow(/empty history/);
  });

  it('marks the start step ready as soon as the workflow exists', () => {
    const p = project(ingest, history(created));

    expect(p.state).toBe('ready');
    expect(step('fetch', p).state).toBe('ready');
    expect(step('summarize', p).state).toBe('pending');
    expect(p.input).toEqual({ url: 'https://example.test' });
    expect(p.createdAt).toBe(1_000);
  });

  it('folds a happy path to a succeeded workflow', () => {
    const p = project(ingest, happyPath);

    expect(p.state).toBe('succeeded');
    expect(p.output).toEqual({ id: 'post-1' });
    expect(p.sequence).toBe(happyPath.length);
    expect(p.updatedAt).toBe(1_018);
    expect([...p.steps.values()].map((s) => s.state)).toEqual([
      'succeeded',
      'succeeded',
      'succeeded',
      'succeeded',
    ]);
    expect(step('fetch', p).output).toEqual({ body: 'hello' });
    expect(step('approve', p).awaitingApproval).toBe(false);
  });

  it('holds a step in waiting while an approval is outstanding', () => {
    const p = project(ingest, history(...UP_TO_APPROVAL));

    expect(step('approve', p).state).toBe('waiting');
    expect(step('approve', p).awaitingApproval).toBe(true);
    expect(step('publish', p).state).toBe('pending');
    expect(readySteps(ingest, p, 1_013)).toEqual([]);
  });

  it('fails a step whose approval is denied', () => {
    const p = project(
      ingest,
      history(
        ...UP_TO_APPROVAL,
        {
          type: 'approval.denied',
          at: 1_013,
          step: 'approve',
          approver: 'nrawal',
          reason: 'summary is wrong',
        },
        {
          type: 'workflow.failed',
          at: 1_014,
          step: 'approve',
          error: 'approval denied',
          errorClass: 'approval_denied',
        },
      ),
    );

    expect(step('approve', p).state).toBe('failed');
    expect(step('approve', p).error).toMatch(/approval denied by nrawal: summary is wrong/);
    expect(step('approve', p).awaitingApproval).toBe(false);
    expect(p.state).toBe('failed');
  });

  it('folds a retryable failure to retrying and the retry back to ready', () => {
    const failed = project(ingest, history(...RETRYABLE_FAILURE));
    // Not `failed`: `failed` is terminal, and a step with retries left has not finished.
    expect(step('fetch', failed).state).toBe('retrying');
    expect(step('fetch', failed).errorClass).toBe('timeout');
    expect(readySteps(ingest, failed, 1_003)).toEqual([]);

    const scheduled = project(
      ingest,
      history(...RETRYABLE_FAILURE, {
        type: 'step.retry_scheduled',
        at: 1_004,
        step: 'fetch',
        attempt: 2,
        notBefore: 1_104,
        backoffMs: 100,
      }),
    );
    expect(step('fetch', scheduled).state).toBe('ready');
    expect(step('fetch', scheduled).attempt).toBe(2);
    // The backoff is honoured by the dispatcher, not by the fold.
    expect(readySteps(ingest, scheduled, 1_050)).toEqual([]);
    expect(readySteps(ingest, scheduled, 1_104).map((s) => s.step)).toEqual(['fetch']);

    const retried = project(
      ingest,
      history(
        ...RETRYABLE_FAILURE,
        {
          type: 'step.retry_scheduled',
          at: 1_004,
          step: 'fetch',
          attempt: 2,
          notBefore: 1_104,
          backoffMs: 100,
        },
        { type: 'step.started', at: 1_105, step: 'fetch', attempt: 2, workerId: 'w2' },
        {
          type: 'step.completed',
          at: 1_106,
          step: 'fetch',
          attempt: 2,
          workerId: 'w2',
          output: { body: 'second try' },
        },
      ),
    );

    expect(step('fetch', retried).state).toBe('succeeded');
    expect(step('fetch', retried).attempt).toBe(2);
    expect(step('fetch', retried).output).toEqual({ body: 'second try' });
    expect(step('summarize', retried).state).toBe('ready');
  });

  it('folds the producer\'s retry decision rather than re-deriving it', () => {
    // `summarize` has no retry policy at all, and the fold still honours willRetry.
    const willRetry = project(
      ingest,
      history(
        ...FETCH_DONE,
        { type: 'step.started', at: 1_004, step: 'summarize', attempt: 1, workerId: 'w1' },
        {
          type: 'step.failed',
          at: 1_005,
          step: 'summarize',
          attempt: 1,
          workerId: 'w1',
          error: 'model unavailable',
          errorClass: 'unavailable',
          willRetry: true,
        },
      ),
    );
    expect(step('summarize', willRetry).state).toBe('retrying');

    // `fetch` has a policy whose maxAttempts and retryableErrors would both permit a
    // retry, and the fold still honours the producer's decision not to take one. This
    // is what keeps the projection a function of the log rather than of whatever the
    // definition happens to say today.
    const wontRetry = project(
      ingest,
      history(
        created,
        { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
        { type: 'step.started', at: 1_002, step: 'fetch', attempt: 1, workerId: 'w1' },
        {
          type: 'step.failed',
          at: 1_003,
          step: 'fetch',
          attempt: 1,
          workerId: 'w1',
          error: 'upstream timed out',
          errorClass: 'timeout',
          willRetry: false,
        },
      ),
    );
    expect(ingest.nodes[0]?.retry?.retryableErrors).toContain('timeout');
    expect(step('fetch', wontRetry).state).toBe('failed');
  });

  it('fails a step immediately when the error class is not retryable', () => {
    const p = project(
      ingest,
      history(
        created,
        { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
        { type: 'step.started', at: 1_002, step: 'fetch', attempt: 1, workerId: 'w1' },
        {
          type: 'step.failed',
          at: 1_003,
          step: 'fetch',
          attempt: 1,
          workerId: 'w1',
          error: 'malformed url',
          errorClass: 'invalid_argument',
          willRetry: false,
        },
        {
          type: 'workflow.failed',
          at: 1_004,
          step: 'fetch',
          error: 'malformed url',
          errorClass: 'invalid_argument',
        },
      ),
    );

    expect(step('fetch', p).state).toBe('failed');
    expect(p.state).toBe('failed');
    // A failure does not retroactively cancel steps that never started.
    expect(step('summarize', p).state).toBe('pending');
  });

  it('returns a step to ready on lease expiry without marking it failed', () => {
    const p = project(ingest, history(...LEASE_LOST));
    const fetch = step('fetch', p);

    expect(fetch.state).toBe('ready');
    expect(fetch.state).not.toBe('failed');
    expect(fetch.workerId).toBeUndefined();
    expect(fetch.leaseExpiresAt).toBeUndefined();
    // The attempt is unchanged, so the reassigned run keeps the same idempotency key.
    expect(fetch.attempt).toBe(1);
    expect(fetch.error).toBeUndefined();
    expect(p.state).toBe('running');
    expect(readySteps(ingest, p, 11_004).map((s) => s.step)).toEqual(['fetch']);

    const reassigned = project(
      ingest,
      history(
        ...LEASE_LOST,
        {
          type: 'step.leased',
          at: 11_004,
          step: 'fetch',
          attempt: 1,
          workerId: 'w2',
          leaseExpiresAt: 21_004,
        },
        { type: 'step.started', at: 11_005, step: 'fetch', attempt: 1, workerId: 'w2' },
        {
          type: 'step.completed',
          at: 11_006,
          step: 'fetch',
          attempt: 1,
          workerId: 'w2',
          output: { body: 'hello' },
        },
      ),
    );
    expect(step('fetch', reassigned).state).toBe('succeeded');
    expect(step('fetch', reassigned).workerId).toBe('w2');
  });

  it('hands a step back on lease expiry without spending a retry attempt', () => {
    const entries = timeline(ingest, history(...LEASE_LOST));
    const expiry = entries.find((e) => e.type === 'step.lease_expired');

    // Straight running -> ready. Passing through `retrying` would say the work went
    // wrong when only the worker went away, and would spend retry budget doing it.
    expect(expiry?.transitions).toEqual(['fetch: running -> ready']);
    expect(entries.some((e) => e.transitions.some((t) => t.includes('retrying')))).toBe(false);
    expect(step('fetch', project(ingest, history(...LEASE_LOST))).attempt).toBe(1);
  });

  it('tracks a renewed lease at its new expiry', () => {
    const renewed = project(
      ingest,
      history(
        created,
        { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
        {
          type: 'step.leased',
          at: 1_002,
          step: 'fetch',
          attempt: 1,
          workerId: 'w1',
          leaseExpiresAt: 2_000,
        },
        { type: 'step.lease_extended', at: 1_900, step: 'fetch', attempt: 1, workerId: 'w1', leaseExpiresAt: 3_000 },
        { type: 'step.lease_extended', at: 2_900, step: 'fetch', attempt: 1, workerId: 'w1', leaseExpiresAt: 9_000 },
      ),
    );
    const fetch = step('fetch', renewed);

    // Every heartbeat takes effect. Treating the second one as a duplicate of the
    // first would leave the projection reporting a long-renewed lease as about to
    // lapse, and readySteps would then hand out a step somebody is still holding.
    expect(fetch.leaseExpiresAt).toBe(9_000);
    // A renewal is not a new lease: same holder, same attempt, no transition.
    expect(fetch.workerId).toBe('w1');
    expect(fetch.attempt).toBe(1);
    expect(fetch.state).toBe('ready');
    expect(readySteps(ingest, renewed, 2_500)).toEqual([]);
    expect(readySteps(ingest, renewed, 8_999)).toEqual([]);
    // And once the renewed lease really has lapsed, the step is offered again.
    expect(readySteps(ingest, renewed, 9_001).map((s) => s.step)).toEqual(['fetch']);
  });

  it('ignores a heartbeat from a worker that no longer holds the lease', () => {
    const p = project(
      ingest,
      history(
        created,
        { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
        {
          type: 'step.leased',
          at: 1_002,
          step: 'fetch',
          attempt: 1,
          workerId: 'w2',
          leaseExpiresAt: 5_000,
        },
        { type: 'step.lease_extended', at: 1_003, step: 'fetch', attempt: 1, workerId: 'w1', leaseExpiresAt: 99_000 },
      ),
    );

    expect(step('fetch', p).leaseExpiresAt).toBe(5_000);
  });

  it('absorbs a completion that arrives after the lease was declared expired', () => {
    // The old worker finished anyway. The step is `ready` at that point, and
    // ready -> succeeded is not a legal hop, so the fold routes it through `running`
    // rather than rejecting a completion that really happened.
    const p = project(
      ingest,
      history(...LEASE_LOST, {
        type: 'step.completed',
        at: 11_004,
        step: 'fetch',
        attempt: 1,
        workerId: 'w1',
        output: { body: 'late but real' },
      }),
    );

    expect(step('fetch', p).state).toBe('succeeded');
    expect(step('summarize', p).state).toBe('ready');
  });

  it('ignores a lease expiry aimed at a superseded lease', () => {
    const p = project(
      ingest,
      history(
        created,
        { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
        {
          type: 'step.leased',
          at: 1_002,
          step: 'fetch',
          attempt: 1,
          workerId: 'w2',
          leaseExpiresAt: 11_002,
        },
        { type: 'step.lease_expired', at: 1_003, step: 'fetch', attempt: 1, workerId: 'w1' },
      ),
    );

    expect(step('fetch', p).workerId).toBe('w2');
    expect(step('fetch', p).leaseExpiresAt).toBe(11_002);
  });

  it('cancels every live step when the workflow is cancelled', () => {
    const p = project(
      ingest,
      history(
        created,
        { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
        { type: 'step.started', at: 1_002, step: 'fetch', attempt: 1, workerId: 'w1' },
        {
          type: 'workflow.cancelled',
          at: 1_003,
          reason: 'operator stopped it',
          requestedBy: 'nrawal',
        },
      ),
    );

    expect(p.state).toBe('cancelled');
    expect([...p.steps.values()].every((s) => s.state === 'cancelled')).toBe(true);
    expect(p.error).toBe('operator stopped it');
    expect(readySteps(ingest, p, 1_004)).toEqual([]);
  });

  it('throws naming the transition when an event restarts a terminal workflow', () => {
    const events = history(
      created,
      { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
      { type: 'step.started', at: 1_002, step: 'fetch', attempt: 1, workerId: 'w1' },
      { type: 'workflow.cancelled', at: 1_003, reason: 'operator stopped it' },
      // A scheduler that lost track and dispatched the step again. This is a producer
      // bug, and absorbing it would let a cancelled workflow keep working.
      { type: 'step.started', at: 1_004, step: 'fetch', attempt: 2, workerId: 'w3' },
    );

    expect(() => project(ingest, events)).toThrow(IllegalTransitionError);
    expect(() => project(ingest, events)).toThrow(/step "fetch" cannot go cancelled -> running/);
    expect(() => project(ingest, events)).toThrow(/at sequence 5 \(step\.started\)/);
  });

  it('throws when a retry is scheduled for a step that is already failed', () => {
    const events = history(
      created,
      { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
      { type: 'step.started', at: 1_002, step: 'fetch', attempt: 1, workerId: 'w1' },
      {
        type: 'step.failed',
        at: 1_003,
        step: 'fetch',
        attempt: 1,
        workerId: 'w1',
        error: 'nope',
        errorClass: 'invalid_argument',
        willRetry: false,
      },
      {
        type: 'step.retry_scheduled',
        at: 1_004,
        step: 'fetch',
        attempt: 2,
        notBefore: 1_104,
        backoffMs: 100,
      },
    );

    expect(() => project(ingest, events)).toThrow(/cannot go failed -> ready/);
  });

  it('rejects an event naming a step the definition does not contain', () => {
    expect(() =>
      project(ingest, history(created, { type: 'step.scheduled', at: 1_001, step: 'ghost', attempt: 1 })),
    ).toThrow(/references step "ghost"/);
  });

  it('rejects a history that mixes workflows or is out of order', () => {
    const base = history(created, { type: 'step.scheduled', at: 1, step: 'fetch', attempt: 1 });
    const first = base[0] as Event;
    const second = base[1] as Event;

    expect(() => project(ingest, [first, { ...second, workflowId: 'other' }])).toThrow(
      /mixes workflows/,
    );
    expect(() => project(ingest, [first, { ...second, sequence: 1 }])).toThrow(/not ordered/);
  });
});

describe('idempotency', () => {
  it('absorbs a repeated step.completed without advancing anything twice', () => {
    const once = project(ingest, history(...FETCH_DONE));
    const twice = project(
      ingest,
      history(...FETCH_DONE, {
        // Redelivered by a worker that did not see its own append succeed, with a
        // different payload for good measure.
        type: 'step.completed',
        at: 1_004,
        step: 'fetch',
        attempt: 1,
        workerId: 'w1',
        output: { body: 'second' },
      }),
    );

    expect(twice.steps).toEqual(once.steps);
    expect(twice.state).toBe(once.state);
    expect(twice.updatedAt).toBe(once.updatedAt);
    // First outcome wins: the duplicate does not overwrite the recorded output.
    expect(step('fetch', twice).output).toEqual({ body: 'first' });
    // And the downstream step was promoted once, not advanced twice.
    expect(step('summarize', twice).state).toBe('ready');
    expect(step('summarize', twice).attempt).toBe(0);
  });

  it('absorbs a repeated step.scheduled without bumping the attempt', () => {
    const p = project(
      ingest,
      history(
        created,
        { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
        { type: 'step.scheduled', at: 1_002, step: 'fetch', attempt: 1 },
      ),
    );

    expect(step('fetch', p).attempt).toBe(1);
    expect(step('fetch', p).state).toBe('ready');
  });

  it('treats a second lease for the same attempt as a reassignment, not a duplicate', () => {
    const p = project(
      ingest,
      history(
        created,
        { type: 'step.scheduled', at: 1_001, step: 'fetch', attempt: 1 },
        {
          type: 'step.leased',
          at: 1_002,
          step: 'fetch',
          attempt: 1,
          workerId: 'w1',
          leaseExpiresAt: 2_000,
        },
        { type: 'step.lease_expired', at: 2_001, step: 'fetch', attempt: 1, workerId: 'w1' },
        {
          type: 'step.leased',
          at: 2_002,
          step: 'fetch',
          attempt: 1,
          workerId: 'w2',
          leaseExpiresAt: 3_000,
        },
      ),
    );

    expect(step('fetch', p).workerId).toBe('w2');
    expect(step('fetch', p).leaseExpiresAt).toBe(3_000);
  });
});

describe('determinism', () => {
  function comparable(p: WorkflowProjection): unknown {
    return { ...p, steps: [...p.steps.entries()] };
  }

  it('folds the same events to the same projection every time', () => {
    expect(comparable(project(ingest, happyPath))).toEqual(comparable(project(ingest, happyPath)));
  });

  it('folds to the same projection after a round trip through each store', async () => {
    const expected = comparable(project(ingest, happyPath));
    // The store re-stamps version and sequence, so handing it sealed events is
    // equivalent to handing it the submissions they came from.
    const submitted: readonly NewEvent[] = happyPath;

    const dir = await mkdtemp(path.join(tmpdir(), 'parliament-proj-'));
    try {
      const memory = new MemoryEventStore();
      await memory.append(WF, 0, submitted);
      expect(comparable(project(ingest, await memory.read(WF)))).toEqual(expected);

      const file = await FileEventStore.open(dir);
      await file.append(WF, 0, submitted);
      expect(comparable(project(ingest, await file.read(WF)))).toEqual(expected);

      // And again from a freshly opened store, which is what recovery does.
      const reopened = await FileEventStore.open(dir);
      expect(comparable(project(ingest, await reopened.read(WF)))).toEqual(expected);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});

describe('readySteps', () => {
  const AFTER_A: readonly Submitted<Event>[] = [
    { type: 'workflow.created', at: 1_000, workflow: 'diamond', definitionVersion: 1, input: {} },
    { type: 'step.scheduled', at: 1_001, step: 'a', attempt: 1 },
    { type: 'step.started', at: 1_002, step: 'a', attempt: 1, workerId: 'w1' },
    { type: 'step.completed', at: 1_003, step: 'a', attempt: 1, workerId: 'w1', output: 1 },
  ];

  it('dispatches both branches of a diamond and the join only after both', () => {
    const forked = project(diamond, history(...AFTER_A));
    expect(readySteps(diamond, forked, 1_004).map((s) => s.step)).toEqual(['b', 'c']);
    expect(step('d', forked).state).toBe('pending');

    const joined = project(
      diamond,
      history(
        ...AFTER_A,
        { type: 'step.started', at: 1_004, step: 'b', attempt: 1, workerId: 'w1' },
        { type: 'step.completed', at: 1_005, step: 'b', attempt: 1, workerId: 'w1', output: 2 },
        { type: 'step.started', at: 1_006, step: 'c', attempt: 1, workerId: 'w2' },
        { type: 'step.completed', at: 1_007, step: 'c', attempt: 1, workerId: 'w2', output: 3 },
      ),
    );
    expect(readySteps(diamond, joined, 1_008).map((s) => s.step)).toEqual(['d']);
  });

  it('respects maxConcurrency', () => {
    const capped: WorkflowDef = { ...diamond, maxConcurrency: 1 };
    expect(readySteps(capped, project(capped, history(...AFTER_A)), 1_004).map((s) => s.step)).toEqual(
      ['b'],
    );
  });

  it('counts a live lease against maxConcurrency', () => {
    const capped: WorkflowDef = { ...diamond, maxConcurrency: 2 };
    const leased = project(
      capped,
      history(...AFTER_A, {
        type: 'step.leased',
        at: 1_004,
        step: 'b',
        attempt: 1,
        workerId: 'w1',
        leaseExpiresAt: 11_004,
      }),
    );

    // `b` is held by w1, which both removes it from the candidates and consumes one of
    // the two slots, leaving exactly one dispatch.
    expect(readySteps(capped, leased, 1_005).map((s) => s.step)).toEqual(['c']);
  });

  it('does not count a step waiting on approval against maxConcurrency', () => {
    // Two independent roots and one slot. The gate occupies no worker while a human
    // thinks about it, so the unrelated branch must still be dispatchable; otherwise
    // one pending approval throttles work that has nothing to do with it.
    const gated: WorkflowDef = {
      name: 'gated',
      version: 1,
      start: 'gate',
      maxConcurrency: 1,
      timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: 10_000 },
      nodes: [
        { name: 'gate', kind: 'approval', dependsOn: [], approvers: ['nrawal'] },
        { name: 'work', kind: 'tool', tool: 'noop', dependsOn: [] },
      ],
    };

    const p = project(
      gated,
      history(
        { type: 'workflow.created', at: 1_000, workflow: 'gated', definitionVersion: 1, input: {} },
        { type: 'step.scheduled', at: 1_001, step: 'gate', attempt: 1 },
        { type: 'step.started', at: 1_002, step: 'gate', attempt: 1, workerId: 'w1' },
        {
          type: 'approval.requested',
          at: 1_003,
          step: 'gate',
          approvers: ['nrawal'],
          prompt: 'ok?',
          payload: null,
        },
      ),
    );

    expect(step('gate', p).state).toBe('waiting');
    expect(readySteps(gated, p, 1_004).map((s) => s.step)).toEqual(['work']);
  });

  it('withholds a step whose notBefore has not elapsed', () => {
    const delayed = project(
      diamond,
      history(...AFTER_A, { type: 'step.scheduled', at: 1_004, step: 'b', attempt: 1, notBefore: 9_000 }),
    );

    expect(readySteps(diamond, delayed, 1_005).map((s) => s.step)).toEqual(['c']);
    expect(readySteps(diamond, delayed, 9_000).map((s) => s.step)).toEqual(['b', 'c']);
  });

  it('dispatches nothing once the workflow deadline has passed', () => {
    const withDeadline = project(
      diamond,
      history({
        type: 'workflow.created',
        at: 1_000,
        workflow: 'diamond',
        definitionVersion: 1,
        input: {},
        deadlineAt: 5_000,
      }),
    );

    expect(readySteps(diamond, withDeadline, 4_999).map((s) => s.step)).toEqual(['a']);
    expect(readySteps(diamond, withDeadline, 5_000)).toEqual([]);
    expect(readySteps(diamond, withDeadline, 9_999)).toEqual([]);
  });

  it('dispatches nothing for a terminal workflow', () => {
    expect(readySteps(ingest, project(ingest, happyPath), 2_000)).toEqual([]);
  });
});

describe('replayTo', () => {
  it('reproduces the state the fold had at every sequence', () => {
    for (let s = 1; s <= happyPath.length; s += 1) {
      const replayed = replayTo(ingest, happyPath, s);
      const folded = project(ingest, happyPath.slice(0, s));

      expect([...replayed.steps.entries()]).toEqual([...folded.steps.entries()]);
      expect(replayed.state).toBe(folded.state);
      expect(replayed.sequence).toBe(s);
    }
  });

  it('matches the states a reader would expect at known points', () => {
    const at = (s: number): string => {
      const p = replayTo(ingest, happyPath, s);
      return `${p.state}/${step('fetch', p).state}`;
    };

    expect(at(1)).toBe('ready/ready');
    expect(at(4)).toBe('running/running');
    expect(at(7)).toBe('running/succeeded');
    expect(at(happyPath.length)).toBe('succeeded/succeeded');
  });

  it('ignores events beyond the requested sequence', () => {
    expect(replayTo(ingest, happyPath, 7).sequence).toBe(7);
    expect(step('publish', replayTo(ingest, happyPath, 7)).state).toBe('pending');
  });
});

describe('timeline', () => {
  it('records one entry per event with the transitions it caused', () => {
    const entries = timeline(ingest, happyPath);

    expect(entries).toHaveLength(happyPath.length);
    expect(entries[0]?.transitions).toEqual([
      'workflow: pending -> ready',
      'fetch: pending -> ready',
    ]);
    const startedFetch = entries.find((e) => e.type === 'step.started' && e.step === 'fetch');
    expect(startedFetch?.transitions).toEqual([
      'workflow: ready -> running',
      'fetch: ready -> running',
    ]);
    // A tool call changes no state, and the line still exists so the gap is explained.
    expect(entries.find((e) => e.type === 'tool.requested')?.transitions).toEqual([]);
  });

  it('renders a legible table', () => {
    const rendered = renderTimeline(ingest, happyPath);
    const lines = rendered.split('\n');

    expect(lines).toHaveLength(happyPath.length + 2);
    expect(lines[0]).toMatch(/^seq\s+event\s+step\s+try\s+transition$/);
    expect(rendered).toContain('fetch: ready -> running');
    expect(rendered).toContain('approve: running -> waiting');
    expect(rendered).toContain('workflow: running -> succeeded');
  });
});

describe('the definition comes from the log', () => {
  // The design claims the event log IS the state. That claim is only true if the log also
  // carries the thing needed to interpret it: the fold consults the retry policy to decide
  // whether a `step.failed` means `failed` or `retrying`, so a definition living only in
  // process memory means a restart replays the same events into a *different* workflow.

  const logged = history(
    { ...created, workflow: 'diamond', definition: diamond },
    { type: 'step.scheduled', at: 1_001, step: 'a', attempt: 1 },
  );

  it('prefers the logged definition over a stored one that has since changed', () => {
    const projection = bindProjection({
      get: () => chainStandIn,
      put: () => undefined,
    }).project(logged);

    // The stored definition is a 2-node chain; the logged one is the 4-node diamond this
    // history actually ran against. Folding against the store would silently answer for a
    // graph the workflow never had.
    expect(projection.workflow).toBe('diamond');
    expect([...projection.steps.keys()]).toContain('a');
    expect(bindProjection({ get: () => chainStandIn, put: () => undefined })
      .definitionFor(logged).nodes).toHaveLength(4);
  });

  it('falls back to the store, then to synthesis, for a history with no logged definition', () => {
    const old = history(
      { ...created, workflow: 'diamond' },  // no `definition`: written before the field existed
      { type: 'step.scheduled', at: 1_001, step: 'a', attempt: 1 },
    );

    const fromStore = bindProjection({ get: () => diamond, put: () => undefined });
    expect(fromStore.definitionFor(old).nodes).toHaveLength(4);

    // Nothing anywhere: synthesis lays out a chain from the steps the history mentions.
    // A degraded mode, and the point of testing it is that it degrades rather than throws.
    const synthesized = bindProjection({ get: () => undefined, put: () => undefined });
    const def = synthesized.definitionFor(old);
    expect(def.nodes).toHaveLength(1);
    expect(def.nodes[0]?.name).toBe('a');
  });
});

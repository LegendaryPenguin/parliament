/**
 * API tests.
 *
 * Driven over a real socket rather than by calling the handler with fake request and
 * response objects. The things most likely to be wrong in an HTTP layer are the things
 * a fake `ServerResponse` papers over: the body limit, the status line, the headers, and
 * above all the event stream — SSE behaviour is almost entirely about when bytes are
 * flushed, which a mock cannot tell you.
 *
 * The Prometheus assertion uses a strict parser written here rather than a regex for
 * "looks like metrics". A broken exposition usually still matches a loose pattern and is
 * then silently dropped by the scraper, which is the failure mode worth catching.
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import type { NewEvent } from '../src/events/types.js';
import type { WorkflowDef } from '../src/workflow/types.js';
import { DEFAULT_RETRY } from '../src/workflow/types.js';
import { createApiServer } from '../src/api/server.js';
import { CostLedger } from '../src/api/cost.js';
import { MAX_BODY_BYTES } from '../src/api/http.js';
import { defaultWiring, type Wiring } from '../src/api/wiring.js';
import type { ToolDef } from '../src/tools/types.js';

const DEF: WorkflowDef = {
  name: 'research',
  version: 2,
  start: 'fetch',
  timeouts: { attemptTimeoutMs: 5_000, leaseDurationMs: 2_000 },
  nodes: [
    { name: 'fetch', kind: 'tool', dependsOn: [], tool: 'http.fetch', retry: DEFAULT_RETRY },
    { name: 'summarize', kind: 'llm', dependsOn: ['fetch'], retry: DEFAULT_RETRY },
    {
      name: 'sign-off',
      kind: 'approval',
      dependsOn: ['summarize'],
      approvers: ['sam@example.com'],
      approvalPrompt: 'Publish this summary?',
    },
    { name: 'publish', kind: 'tool', dependsOn: ['sign-off'], tool: 'fs.write' },
  ],
};

interface Harness {
  readonly base: string;
  readonly server: Server;
  readonly wiring: Wiring;
}

let harness: Harness;

async function start(budgets?: ReadonlyMap<string, number>): Promise<Harness> {
  const wiring = defaultWiring(budgets !== undefined ? { budgets } : {});
  const server = createApiServer({
    deps: wiring.deps,
    cost: wiring.cost,
    serveDashboard: false,
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return { base: `http://127.0.0.1:${port}`, server, wiring };
}

beforeEach(async () => {
  harness = await start();
});

afterEach(async () => {
  await new Promise<void>((resolve) => harness.server.close(() => resolve()));
});

async function post(path: string, body: unknown, base = harness.base): Promise<Response> {
  return fetch(`${base}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

async function submit(overrides: Record<string, unknown> = {}, base = harness.base): Promise<string> {
  const response = await post('/workflows', { definition: DEF, input: { url: 'https://example.com' }, ...overrides }, base);
  expect(response.status).toBe(201);
  const body = (await response.json()) as { workflowId: string };
  return body.workflowId;
}

/** Appends events the scheduler would normally write, to reach a state worth testing. */
async function append(workflowId: string, events: readonly NewEvent[], wiring = harness.wiring): Promise<void> {
  const head = await wiring.store.head(workflowId);
  await wiring.store.append(workflowId, head, events);
}

function at(offset = 0): number {
  return Date.now() + offset;
}

describe('POST /workflows', () => {
  it('creates an execution and reports where to find it', async () => {
    const response = await post('/workflows', { definition: DEF, input: { url: 'https://example.com' }, tenant: 'acme' });
    expect(response.status).toBe(201);
    expect(response.headers.get('location')).toMatch(/^\/workflows\/wf_/);

    const body = (await response.json()) as { workflowId: string; definition: string };
    expect(body.definition).toBe('research');

    const projection = (await (await fetch(`${harness.base}/workflows/${body.workflowId}`)).json()) as {
      state: string;
      tenant: string;
      steps: { step: string; state: string }[];
    };
    // `ready` rather than `running`: the start step is schedulable but nothing has leased
    // it, and the projector is careful about that distinction.
    expect(projection.state).toBe('ready');
    expect(projection.tenant).toBe('acme');
    // The start step is scheduled by the submission; the rest are known but not started.
    expect(projection.steps.find((s) => s.step === 'fetch')?.state).toBe('ready');
    expect(projection.steps.find((s) => s.step === 'publish')?.state).toBe('pending');
  });

  it('rejects an invalid definition with 400 and every specific error', async () => {
    const broken: WorkflowDef = {
      ...DEF,
      start: 'nowhere',
      nodes: [
        { name: 'a', kind: 'tool', dependsOn: ['ghost'] },
        { name: 'b', kind: 'condition', dependsOn: ['a'] },
        { name: 'c', kind: 'approval', dependsOn: ['b'], approvers: [] },
      ],
    };
    const response = await post('/workflows', { definition: broken, input: {} });
    expect(response.status).toBe(400);

    const body = (await response.json()) as { error: string; errors: { node?: string; message: string }[] };
    expect(body.error).toBe('invalid_definition');
    const messages = body.errors.map((e) => `${e.node ?? ''} ${e.message}`);
    expect(messages).toEqual(
      expect.arrayContaining([
        expect.stringContaining('start step "nowhere" is not a node'),
        expect.stringContaining('depends on unknown step "ghost"'),
        expect.stringContaining('tool node has no tool name'),
        expect.stringContaining('condition node has no predicate'),
        expect.stringContaining('approval node has no approvers'),
      ]),
    );
  });

  it('reports a dependency cycle with its path', async () => {
    const cyclic: WorkflowDef = {
      ...DEF,
      start: 'a',
      nodes: [
        { name: 'a', kind: 'tool', dependsOn: ['c'], tool: 't' },
        { name: 'b', kind: 'tool', dependsOn: ['a'], tool: 't' },
        { name: 'c', kind: 'tool', dependsOn: ['b'], tool: 't' },
      ],
    };
    const body = (await (await post('/workflows', { definition: cyclic, input: {} })).json()) as {
      errors: { message: string }[];
    };
    expect(body.errors.some((e) => e.message.includes('dependency cycle'))).toBe(true);
  });

  it('rejects a malformed definition body before validation can throw', async () => {
    const response = await post('/workflows', { definition: { name: 'x' } });
    expect(response.status).toBe(400);
    const body = (await response.json()) as { detail: { problems: string[] } };
    expect(body.detail.problems).toEqual(
      expect.arrayContaining([expect.stringContaining('"nodes" must be a non-empty array')]),
    );
  });

  it('rejects a body over the limit with 413', async () => {
    // Oversized in the input document rather than the definition, since that is the
    // field a caller can realistically make enormous.
    const huge = 'x'.repeat(MAX_BODY_BYTES + 1_024);
    const response = await post('/workflows', { definition: DEF, input: { blob: huge } });
    expect(response.status).toBe(413);
    const body = (await response.json()) as { error: string; detail: { limitBytes: number } };
    expect(body.error).toBe('payload_too_large');
    expect(body.detail.limitBytes).toBe(MAX_BODY_BYTES);
  });

  it('refuses a duplicate submission that carries the same idempotency key', async () => {
    const first = await submit({ idempotencyKey: 'once' });
    const second = await submit({ idempotencyKey: 'once' });
    expect(second).toBe(first);
  });

  it('rejects a body that is not JSON', async () => {
    const response = await fetch(`${harness.base}/workflows`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{not json',
    });
    expect(response.status).toBe(400);
    expect(((await response.json()) as { message: string }).message).toContain('not valid JSON');
  });
});

describe('GET /workflows', () => {
  it('filters by state and tenant and paginates', async () => {
    await submit({ tenant: 'acme' });
    await submit({ tenant: 'acme' });
    const globex = await submit({ tenant: 'globex' });
    await post(`/workflows/${globex}/cancel`, { reason: 'not needed' });

    const all = (await (await fetch(`${harness.base}/workflows`)).json()) as { total: number };
    expect(all.total).toBe(3);

    const byTenant = (await (await fetch(`${harness.base}/workflows?tenant=acme`)).json()) as {
      total: number;
      workflows: { tenant: string }[];
    };
    expect(byTenant.total).toBe(2);
    expect(byTenant.workflows.every((w) => w.tenant === 'acme')).toBe(true);

    const cancelled = (await (await fetch(`${harness.base}/workflows?state=cancelled`)).json()) as {
      total: number;
      workflows: { workflowId: string }[];
    };
    expect(cancelled.total).toBe(1);
    expect(cancelled.workflows[0]?.workflowId).toBe(globex);

    const page = (await (await fetch(`${harness.base}/workflows?limit=2`)).json()) as {
      workflows: unknown[];
      nextOffset?: number;
    };
    expect(page.workflows).toHaveLength(2);
    expect(page.nextOffset).toBe(2);
  });

  it('rejects an unknown state filter rather than silently matching nothing', async () => {
    const response = await fetch(`${harness.base}/workflows?state=exploded`);
    expect(response.status).toBe(400);
    expect(((await response.json()) as { message: string }).message).toContain('unknown state');
  });

  it('surfaces the recorded priority', async () => {
    const id = await submit();
    await harness.wiring.store.append(`${id}-other`, 0, []);
    const list = (await (await fetch(`${harness.base}/workflows`)).json()) as {
      workflows: { workflowId: string; priority?: number }[];
    };
    // The submission endpoint does not set a priority, so none is reported — the field is
    // absent rather than defaulted, which is the point of recording it in the log.
    expect(list.workflows.find((w) => w.workflowId === id)?.priority).toBeUndefined();
  });
});

describe('GET /workflows/:id', () => {
  it('404s with the id that was not found', async () => {
    const response = await fetch(`${harness.base}/workflows/wf_nope`);
    expect(response.status).toBe(404);
    expect(((await response.json()) as { message: string }).message).toContain('wf_nope');
  });
});

describe('GET /workflows/:id/events', () => {
  it('returns the raw history and honours ?after=', async () => {
    const id = await submit();
    const all = (await (await fetch(`${harness.base}/workflows/${id}/events`)).json()) as {
      events: { sequence: number; type: string }[];
    };
    expect(all.events.map((e) => e.type)).toEqual(['workflow.created', 'step.scheduled']);

    const tail = (await (await fetch(`${harness.base}/workflows/${id}/events?after=1`)).json()) as {
      events: { sequence: number }[];
    };
    expect(tail.events).toHaveLength(1);
    expect(tail.events[0]?.sequence).toBe(2);

    // Tailing past the end is empty, not a 404 — otherwise a poller would give up.
    const beyond = await fetch(`${harness.base}/workflows/${id}/events?after=99`);
    expect(beyond.status).toBe(200);
    expect(((await beyond.json()) as { events: unknown[] }).events).toHaveLength(0);
  });

  it('rejects a junk ?after=', async () => {
    const id = await submit();
    const response = await fetch(`${harness.base}/workflows/${id}/events?after=soon`);
    expect(response.status).toBe(400);
  });
});

describe('GET /workflows/:id/trace', () => {
  it('groups attempts, attaches tool calls, and counts retries and lease events', async () => {
    const id = await submit({ tenant: 'acme' });
    await append(id, [
      { type: 'step.leased', workflowId: id, at: at(), step: 'fetch', attempt: 1, workerId: 'w1', leaseExpiresAt: at(2_000) },
      { type: 'step.started', workflowId: id, at: at(1), step: 'fetch', attempt: 1, workerId: 'w1' },
      {
        type: 'step.lease_extended',
        workflowId: id,
        at: at(2),
        step: 'fetch',
        attempt: 1,
        workerId: 'w1',
        leaseExpiresAt: at(4_000),
      },
      {
        type: 'tool.requested',
        workflowId: id,
        at: at(3),
        step: 'fetch',
        attempt: 1,
        tool: 'http.fetch',
        args: { url: 'https://example.com' },
        idempotencyKey: 'k1',
      },
      {
        type: 'tool.failed',
        workflowId: id,
        at: at(10),
        step: 'fetch',
        attempt: 1,
        tool: 'http.fetch',
        error: 'upstream 503',
        errorClass: 'unavailable',
      },
      {
        type: 'step.failed',
        workflowId: id,
        at: at(11),
        step: 'fetch',
        attempt: 1,
        workerId: 'w1',
        error: 'upstream 503',
        errorClass: 'unavailable',
        willRetry: true,
      },
      { type: 'step.retry_scheduled', workflowId: id, at: at(12), step: 'fetch', attempt: 2, notBefore: at(500), backoffMs: 500 },
      { type: 'step.scheduled', workflowId: id, at: at(520), step: 'fetch', attempt: 2 },
      { type: 'step.started', workflowId: id, at: at(521), step: 'fetch', attempt: 2, workerId: 'w2' },
      {
        type: 'tool.requested',
        workflowId: id,
        at: at(522),
        step: 'fetch',
        attempt: 2,
        tool: 'http.fetch',
        args: { url: 'https://example.com' },
        idempotencyKey: 'k2',
      },
      {
        type: 'tool.succeeded',
        workflowId: id,
        at: at(600),
        step: 'fetch',
        attempt: 2,
        tool: 'http.fetch',
        result: { status: 200, body: 'ok' },
        durationMs: 78,
        deduplicated: false,
      },
      { type: 'step.completed', workflowId: id, at: at(601), step: 'fetch', attempt: 2, workerId: 'w2', output: { body: 'ok' } },
    ]);

    const trace = (await (await fetch(`${harness.base}/workflows/${id}/trace`)).json()) as {
      totals: Record<string, number>;
      steps: {
        step: string;
        depth: number;
        state: string;
        attempts: {
          attempt: number;
          workerId?: string;
          leaseExtensions: number;
          willRetry?: boolean;
          toolCalls: { tool: string; outcome: string; result?: unknown; args?: unknown }[];
        }[];
      }[];
      cost: { toolUnits: number };
    };

    expect(trace.totals).toMatchObject({ retries: 1, leaseExtensions: 1, toolCalls: 2, toolFailures: 1 });

    const fetchStep = trace.steps.find((s) => s.step === 'fetch');
    expect(fetchStep?.state).toBe('succeeded');
    expect(fetchStep?.attempts).toHaveLength(2);
    expect(fetchStep?.attempts[0]?.workerId).toBe('w1');
    expect(fetchStep?.attempts[0]?.leaseExtensions).toBe(1);
    expect(fetchStep?.attempts[0]?.willRetry).toBe(true);
    expect(fetchStep?.attempts[0]?.toolCalls[0]?.outcome).toBe('failed');
    expect(fetchStep?.attempts[1]?.toolCalls[0]?.result).toEqual({ status: 200, body: 'ok' });

    // Depth comes from the definition, which is what the DAG view lays out on.
    expect(trace.steps.map((s) => [s.step, s.depth])).toEqual([
      ['fetch', 0],
      ['summarize', 1],
      ['sign-off', 2],
      ['publish', 3],
    ]);

    expect(trace.cost.toolUnits).toBeGreaterThan(0);
  });

  it('names the failing step and the path that led to it', async () => {
    const id = await submit();
    await append(id, [
      { type: 'step.started', workflowId: id, at: at(), step: 'fetch', attempt: 1, workerId: 'w1' },
      { type: 'step.completed', workflowId: id, at: at(1), step: 'fetch', attempt: 1, workerId: 'w1', output: {} },
      { type: 'step.scheduled', workflowId: id, at: at(2), step: 'summarize', attempt: 1 },
      { type: 'step.started', workflowId: id, at: at(3), step: 'summarize', attempt: 1, workerId: 'w1' },
      {
        type: 'step.failed',
        workflowId: id,
        at: at(4),
        step: 'summarize',
        attempt: 1,
        workerId: 'w1',
        error: 'malformed model output',
        errorClass: 'invalid_output',
        willRetry: false,
      },
      {
        type: 'workflow.failed',
        workflowId: id,
        at: at(5),
        step: 'summarize',
        error: 'malformed model output',
        errorClass: 'invalid_output',
      },
    ]);

    const trace = (await (await fetch(`${harness.base}/workflows/${id}/trace`)).json()) as {
      failure: { step: string; errorClass: string };
      criticalPath: string[];
      steps: { step: string; state: string }[];
    };
    expect(trace.failure).toMatchObject({ step: 'summarize', errorClass: 'invalid_output' });
    expect(trace.criticalPath).toEqual(['fetch', 'summarize']);
    // Steps that can no longer run are not left looking like they are still coming.
    expect(trace.steps.find((s) => s.step === 'publish')?.state).toBe('cancelled');
  });
});

describe('GET /workflows/:id/replay', () => {
  it('returns the projection as of a past sequence', async () => {
    const id = await submit();
    await append(id, [
      { type: 'step.started', workflowId: id, at: at(), step: 'fetch', attempt: 1, workerId: 'w1' },
      { type: 'step.completed', workflowId: id, at: at(1), step: 'fetch', attempt: 1, workerId: 'w1', output: { body: 'ok' } },
    ]);

    const head = (await (await fetch(`${harness.base}/workflows/${id}/replay`)).json()) as {
      sequence: number;
      head: number;
      projection: { steps: { step: string; state: string }[] };
    };
    expect(head.sequence).toBe(head.head);
    expect(head.projection.steps.find((s) => s.step === 'fetch')?.state).toBe('succeeded');

    const earlier = (await (await fetch(`${harness.base}/workflows/${id}/replay?sequence=3`)).json()) as {
      projection: { steps: { step: string; state: string }[] };
    };
    expect(earlier.projection.steps.find((s) => s.step === 'fetch')?.state).toBe('running');
  });

  it('rejects a sequence past the end of the log', async () => {
    const id = await submit();
    const response = await fetch(`${harness.base}/workflows/${id}/replay?sequence=500`);
    expect(response.status).toBe(400);
    const body = (await response.json()) as { message: string; detail: { head: number } };
    expect(body.message).toContain('beyond the end of the log');
    expect(body.detail.head).toBe(2);
  });
});

describe('POST /workflows/:id/cancel', () => {
  it('cancels a running workflow and refuses to cancel it twice', async () => {
    const id = await submit();
    const first = await post(`/workflows/${id}/cancel`, { reason: 'operator asked', requestedBy: 'sam' });
    expect(first.status).toBe(200);
    expect(((await first.json()) as { state: string }).state).toBe('cancelled');

    const second = await post(`/workflows/${id}/cancel`, { reason: 'again' });
    expect(second.status).toBe(409);
    const body = (await second.json()) as { error: string; message: string; detail: { state: string } };
    expect(body.error).toBe('conflict');
    expect(body.message).toContain('already cancelled');
    expect(body.detail.state).toBe('cancelled');
  });
});

describe('approvals', () => {
  /**
   * Drives a workflow to the point where its gate is genuinely waiting on a human.
   *
   * Timestamps are in the past so that the wait recorded against the approval is a real
   * positive duration once the decision lands.
   */
  async function toGate(): Promise<string> {
    const id = await submit({ tenant: 'acme' });
    await append(id, [
      { type: 'step.started', workflowId: id, at: at(-6_000), step: 'fetch', attempt: 1, workerId: 'w1' },
      { type: 'step.completed', workflowId: id, at: at(-5_900), step: 'fetch', attempt: 1, workerId: 'w1', output: {} },
      { type: 'step.scheduled', workflowId: id, at: at(-5_800), step: 'summarize', attempt: 1 },
      { type: 'step.started', workflowId: id, at: at(-5_700), step: 'summarize', attempt: 1, workerId: 'w1' },
      {
        type: 'step.completed',
        workflowId: id,
        at: at(-4_000),
        step: 'summarize',
        attempt: 1,
        workerId: 'w1',
        output: { text: 'a summary', usage: { model: 'default', inputTokens: 1_000, outputTokens: 500 } },
      },
      { type: 'step.scheduled', workflowId: id, at: at(-3_900), step: 'sign-off', attempt: 1 },
      {
        type: 'approval.requested',
        workflowId: id,
        at: at(-3_800),
        step: 'sign-off',
        approvers: ['sam@example.com'],
        prompt: 'Publish this summary?',
        payload: { headline: 'a summary' },
      },
    ]);
    return id;
  }

  it('records an approval with the approver and note', async () => {
    const id = await toGate();
    const response = await post(`/workflows/${id}/steps/sign-off/approve`, {
      approver: 'sam@example.com',
      note: 'checked the figures',
    });
    expect(response.status).toBe(200);

    const trace = (await (await fetch(`${harness.base}/workflows/${id}/trace`)).json()) as {
      approvals: { decision: string; approver: string; note: string; waitMs: number }[];
    };
    expect(trace.approvals[0]).toMatchObject({
      decision: 'granted',
      approver: 'sam@example.com',
      note: 'checked the figures',
    });
    expect(trace.approvals[0]?.waitMs).toBeGreaterThan(0);
  });

  it('requires a reason to deny, and fails the workflow when denied', async () => {
    const id = await toGate();
    const noReason = await post(`/workflows/${id}/steps/sign-off/deny`, { approver: 'sam@example.com' });
    expect(noReason.status).toBe(400);
    expect(((await noReason.json()) as { message: string }).message).toContain('"reason" is required');

    const denied = await post(`/workflows/${id}/steps/sign-off/deny`, {
      approver: 'sam@example.com',
      reason: 'the figures do not reconcile',
    });
    expect(denied.status).toBe(200);
    expect(((await denied.json()) as { state: string }).state).toBe('failed');

    const trace = (await (await fetch(`${harness.base}/workflows/${id}/trace`)).json()) as {
      approvals: { decision: string; reason: string }[];
    };
    expect(trace.approvals[0]).toMatchObject({ decision: 'denied', reason: 'the figures do not reconcile' });
  });

  it('409s when approving a workflow that has already reached a terminal state', async () => {
    const id = await toGate();
    await post(`/workflows/${id}/cancel`, { reason: 'no longer needed' });

    const response = await post(`/workflows/${id}/steps/sign-off/approve`, { approver: 'sam@example.com' });
    expect(response.status).toBe(409);
    const body = (await response.json()) as { error: string; message: string; detail: { state: string } };
    expect(body.error).toBe('conflict');
    expect(body.message).toContain('terminal workflow cannot be approved');
    expect(body.detail.state).toBe('cancelled');
  });

  it('409s when the step is not waiting on anyone', async () => {
    const id = await toGate();
    const response = await post(`/workflows/${id}/steps/fetch/approve`, { approver: 'sam@example.com' });
    expect(response.status).toBe(409);
    expect(((await response.json()) as { message: string }).message).toContain('not awaiting approval');
  });

  it('403s an identity that is not on the approver list', async () => {
    const id = await toGate();
    const response = await post(`/workflows/${id}/steps/sign-off/approve`, { approver: 'intruder@example.com' });
    expect(response.status).toBe(403);
    const body = (await response.json()) as { error: string; detail: { approvers: string[] } };
    expect(body.error).toBe('not_an_approver');
    expect(body.detail.approvers).toEqual(['sam@example.com']);
  });

  it('404s a step that is not in the history', async () => {
    const id = await toGate();
    const response = await post(`/workflows/${id}/steps/ghost/approve`, { approver: 'sam@example.com' });
    expect(response.status).toBe(404);
  });

  it('requires an approver identity', async () => {
    const id = await toGate();
    const response = await post(`/workflows/${id}/steps/sign-off/approve`, {});
    expect(response.status).toBe(400);
    expect(((await response.json()) as { message: string }).message).toContain('"approver" is required');
  });
});

describe('GET /tools', () => {
  it('lists the registry with schemas and permissions', async () => {
    const body = (await (await fetch(`${harness.base}/tools`)).json()) as {
      tools: { name: string; input: unknown; permissions: { effect: string; idempotent: boolean } }[];
    };
    expect(body.tools.length).toBeGreaterThan(0);
    const http = body.tools.find((t) => t.name === 'http.fetch');
    expect(http?.permissions.effect).toBe('read');
    expect(http?.input).toMatchObject({ type: 'object' });
  });
});

describe('GET /healthz', () => {
  it('reports total and active workflow counts', async () => {
    const id = await submit();
    await post(`/workflows/${id}/cancel`, { reason: 'done' });
    await submit();

    const body = (await (await fetch(`${harness.base}/healthz`)).json()) as {
      status: string;
      workflows: number;
      activeWorkflows: number;
    };
    expect(body.status).toBe('ok');
    expect(body.workflows).toBe(2);
    expect(body.activeWorkflows).toBe(1);
  });
});

describe('unknown routes', () => {
  it('404 with the method and path', async () => {
    const response = await fetch(`${harness.base}/nope`);
    expect(response.status).toBe(404);
    expect(((await response.json()) as { message: string }).message).toContain('GET /nope');
  });
});

describe('GET /workflows/:id/stream', () => {
  it('sends a snapshot and then emits on a real change', async () => {
    const id = await submit();
    const controller = new AbortController();
    const response = await fetch(`${harness.base}/workflows/${id}/stream`, { signal: controller.signal });
    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('text/event-stream');

    const reader = (response.body as ReadableStream<Uint8Array>).getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    /** Reads until the buffer contains `marker`, so the test does not race the flush. */
    const readUntil = async (marker: string): Promise<string> => {
      const deadline = Date.now() + 5_000;
      while (!buffer.includes(marker)) {
        if (Date.now() > deadline) throw new Error(`timed out waiting for ${marker}; got: ${buffer}`);
        const { value, done } = await reader.read();
        if (done) throw new Error(`stream ended waiting for ${marker}`);
        buffer += decoder.decode(value, { stream: true });
      }
      return buffer;
    };

    await readUntil('event: snapshot');

    // A change the API itself did not make: this is the case that matters, because the
    // appends a dashboard needs to see come from workers and the scheduler.
    await append(id, [
      { type: 'step.started', workflowId: id, at: at(), step: 'fetch', attempt: 1, workerId: 'w1' },
    ]);

    const seen = await readUntil('step.started');
    expect(seen).toContain('event: event');
    expect(seen).toContain('"type":"step.started"');
    // The id frame is the resume token: without it a reconnect would replay from zero.
    expect(seen).toMatch(/\nid: \d+\n/);
    await readUntil('event: projection');

    controller.abort();
    await reader.cancel().catch(() => undefined);
  });

  it('404s a stream for a workflow that does not exist', async () => {
    const response = await fetch(`${harness.base}/workflows/wf_ghost/stream`);
    expect(response.status).toBe(404);
    await response.text();
  });
});

describe('GET /metrics', () => {
  it('emits valid Prometheus exposition for a real history', async () => {
    const id = await submit({ tenant: 'acme' });
    await append(id, [
      { type: 'step.started', workflowId: id, at: at(), step: 'fetch', attempt: 1, workerId: 'w1' },
      {
        type: 'tool.requested',
        workflowId: id,
        at: at(1),
        step: 'fetch',
        attempt: 1,
        tool: 'http.fetch',
        args: {},
        idempotencyKey: 'k',
      },
      {
        type: 'tool.succeeded',
        workflowId: id,
        at: at(2),
        step: 'fetch',
        attempt: 1,
        tool: 'http.fetch',
        result: {},
        durationMs: 240,
        deduplicated: false,
      },
      { type: 'step.completed', workflowId: id, at: at(3), step: 'fetch', attempt: 1, workerId: 'w1', output: {} },
      { type: 'workflow.completed', workflowId: id, at: at(4), output: {} },
    ]);

    const response = await fetch(`${harness.base}/metrics`);
    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('text/plain');

    const text = await response.text();
    const parsed = parsePrometheus(text);

    expect(parsed.families.get('parliament_workflows')?.type).toBe('gauge');
    expect(
      parsed.samples.find((s) => s.name === 'parliament_workflows' && s.labels['state'] === 'succeeded')?.value,
    ).toBe(1);
    expect(
      parsed.samples.find((s) => s.name === 'parliament_tool_invocations_total' && s.labels['tool'] === 'http.fetch')
        ?.value,
    ).toBe(1);

    const toolHistogram = parsed.histograms.get('parliament_tool_duration_ms');
    expect(toolHistogram).toBeDefined();
    const series = [...(toolHistogram ?? new Map()).values()][0];
    expect(series?.count).toBe(1);
    expect(series?.sum).toBe(240);
    // 240ms belongs in the 250ms bucket and not the 100ms one.
    expect(series?.buckets.get('100')).toBe(0);
    expect(series?.buckets.get('250')).toBe(1);

    expect(parsed.histograms.get('parliament_workflow_duration_ms')).toBeDefined();
    // Cost is reported per tenant with the estimate kept separable.
    const kinds = parsed.samples
      .filter((s) => s.name === 'parliament_cost_units_total')
      .map((s) => s.labels['kind']);
    expect(kinds).toEqual(expect.arrayContaining(['tool', 'tokens_estimated']));
  });

  it('emits zero-valued series for states nothing is in', async () => {
    const parsed = parsePrometheus(await (await fetch(`${harness.base}/metrics`)).text());
    const failed = parsed.samples.find(
      (s) => s.name === 'parliament_workflows' && s.labels['state'] === 'failed',
    );
    // Present and zero, so an alert on it can fire at all.
    expect(failed?.value).toBe(0);
  });
});

describe('cost accounting', () => {
  const tool = (name: string, costUnits: number): ToolDef => ({
    name,
    description: name,
    input: { type: 'object' },
    output: { type: 'object' },
    permissions: { effect: 'read', allowedRoles: ['any'], requiresApproval: false, idempotent: true },
    timeoutMs: 1_000,
    costUnits,
  });

  const registry = {
    list: () => [tool('cheap', 1), tool('dear', 10)],
    get: (name: string) => [tool('cheap', 1), tool('dear', 10)].find((t) => t.name === name),
  };

  it('accumulates tool units, skips deduplicated calls, and estimates tokens separately', () => {
    const ledger = new CostLedger({ tools: registry });
    const workflowId = 'wf_1';
    const cost = ledger.forWorkflow([
      { version: 1, sequence: 1, workflowId, type: 'workflow.created', at: 0, workflow: 'w', definitionVersion: 1, input: {}, tenant: 'acme' },
      { version: 1, sequence: 2, workflowId, type: 'tool.succeeded', at: 1, step: 's', attempt: 1, tool: 'dear', result: {}, durationMs: 5, deduplicated: false },
      { version: 1, sequence: 3, workflowId, type: 'tool.succeeded', at: 2, step: 's', attempt: 2, tool: 'dear', result: {}, durationMs: 1, deduplicated: true },
      { version: 1, sequence: 4, workflowId, type: 'tool.succeeded', at: 3, step: 's', attempt: 2, tool: 'cheap', result: {}, durationMs: 1, deduplicated: false },
      { version: 1, sequence: 5, workflowId, type: 'tool.failed', at: 4, step: 's', attempt: 2, tool: 'cheap', error: 'x', errorClass: 'internal' },
      {
        version: 1,
        sequence: 6,
        workflowId,
        type: 'step.completed',
        at: 5,
        step: 'llm',
        attempt: 1,
        workerId: 'w',
        output: { usage: { model: 'default', inputTokens: 2_000, outputTokens: 1_000 } },
      },
    ]);

    // 10 for the first `dear` call, nothing for the deduplicated repeat, 1 for `cheap`.
    expect(cost.toolUnits).toBe(11);
    expect(cost.byTool['dear']).toMatchObject({ calls: 2, deduplicated: 1, units: 10 });
    expect(cost.byTool['cheap']).toMatchObject({ failures: 1 });

    // 2000/1000 * 0.3 + 1000/1000 * 1.5
    expect(cost.tokenUnits).toBeCloseTo(2.1, 4);
    expect(cost.totalUnits).toBeCloseTo(13.1, 4);
    expect(cost.tokenUnitsAreEstimated).toBe(true);
    expect(cost.tokens).toMatchObject({ inputTokens: 2_000, outputTokens: 1_000, reportingSteps: 1 });
  });

  it('rolls up per tenant and reports what is left of the budget', () => {
    const ledger = new CostLedger({ tools: registry, budgets: new Map([['acme', 20]]) });
    const spend = ledger.forTenant('acme', [
      { workflowId: 'a', tenant: 'acme', toolUnits: 5, tokenUnits: 1, totalUnits: 6, byTool: {}, tokens: { inputTokens: 0, outputTokens: 0, reportingSteps: 0, estimatedSteps: 0 }, tokenUnitsAreEstimated: true },
      { workflowId: 'b', tenant: 'acme', toolUnits: 3, tokenUnits: 0, totalUnits: 3, byTool: {}, tokens: { inputTokens: 0, outputTokens: 0, reportingSteps: 0, estimatedSteps: 0 }, tokenUnitsAreEstimated: true },
      { workflowId: 'c', tenant: 'other', toolUnits: 90, tokenUnits: 0, totalUnits: 90, byTool: {}, tokens: { inputTokens: 0, outputTokens: 0, reportingSteps: 0, estimatedSteps: 0 }, tokenUnitsAreEstimated: true },
    ]);
    expect(spend).toMatchObject({ workflows: 2, totalUnits: 9, budgetUnits: 20, remainingUnits: 11 });
  });

  it('admits inside the budget and refuses outside it', () => {
    const ledger = new CostLedger({ tools: registry, budgets: new Map([['acme', 10]]), reserveUnits: 2 });
    expect(ledger.admit('acme', 7).allowed).toBe(true);
    expect(ledger.admit('acme', 9).allowed).toBe(false);
    expect(ledger.admit('acme', 9).reason).toContain('exceeds the budget');
    // A tenant with no ceiling is never refused, and neither is an unattributed workflow.
    expect(ledger.admit('nobody-budgeted', 1_000).allowed).toBe(true);
    expect(ledger.admit(undefined, 1_000).allowed).toBe(true);
  });

  it('refuses a submission that would take the tenant past its budget', async () => {
    const budgeted = await start(new Map([['tight', 3]]));
    try {
      const id = await submit({ tenant: 'tight' }, budgeted.base);
      // Spend the budget through real tool events rather than by poking a counter.
      await append(
        id,
        [
          {
            type: 'tool.succeeded',
            workflowId: id,
            at: at(),
            step: 'fetch',
            attempt: 1,
            tool: 'fs.write',
            result: {},
            durationMs: 3,
            deduplicated: false,
          },
          {
            type: 'tool.succeeded',
            workflowId: id,
            at: at(1),
            step: 'fetch',
            attempt: 1,
            tool: 'http.fetch',
            result: {},
            durationMs: 3,
            deduplicated: false,
          },
          {
            type: 'tool.succeeded',
            workflowId: id,
            at: at(2),
            step: 'fetch',
            attempt: 1,
            tool: 'http.fetch',
            result: {},
            durationMs: 3,
            deduplicated: false,
          },
        ],
        budgeted.wiring,
      );

      const refused = await post('/workflows', { definition: DEF, input: {}, tenant: 'tight' }, budgeted.base);
      expect(refused.status).toBe(402);
      const body = (await refused.json()) as { error: string; message: string; detail: { budgetUnits: number } };
      expect(body.error).toBe('budget_exceeded');
      expect(body.message).toContain('cost units');
      expect(body.detail.budgetUnits).toBe(3);

      // Another tenant is unaffected: the budget is per tenant, not a global brake.
      const allowed = await post('/workflows', { definition: DEF, input: {}, tenant: 'roomy' }, budgeted.base);
      expect(allowed.status).toBe(201);
    } finally {
      await new Promise<void>((resolve) => budgeted.server.close(() => resolve()));
    }
  });

  it('serves per-workflow and per-tenant cost', async () => {
    const id = await submit({ tenant: 'acme' });
    const cost = (await (await fetch(`${harness.base}/workflows/${id}/cost`)).json()) as {
      totalUnits: number;
      tokenUnitsAreEstimated: boolean;
    };
    expect(cost.tokenUnitsAreEstimated).toBe(true);

    const tenant = (await (await fetch(`${harness.base}/tenants/acme/cost`)).json()) as {
      tenant: string;
      workflows: number;
    };
    expect(tenant).toMatchObject({ tenant: 'acme', workflows: 1 });
  });
});

/* ------------------------------------------------------------------------- *
 * A strict Prometheus text-format parser.
 *
 * Deliberately unforgiving. The point of asserting on the exposition is to catch an
 * output that a scraper would reject, and a lenient parser cannot do that: it would
 * accept a histogram with no `+Inf` bucket, a sample for a family that was never typed,
 * or a value like `1e-7` that trips line-oriented consumers. Each of those is a real
 * mistake this exporter could make.
 * ------------------------------------------------------------------------- */

interface ParsedSample {
  readonly name: string;
  readonly labels: Readonly<Record<string, string>>;
  readonly value: number;
}

interface HistogramSeries {
  buckets: Map<string, number>;
  sum: number | undefined;
  count: number | undefined;
}

interface Parsed {
  readonly families: Map<string, { type: string; help: string }>;
  readonly samples: readonly ParsedSample[];
  readonly histograms: Map<string, Map<string, HistogramSeries>>;
}

const NAME = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;
const SAMPLE = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})? (.+)$/;

function parsePrometheus(text: string): Parsed {
  if (!text.endsWith('\n')) throw new Error('exposition must end with a newline');

  const families = new Map<string, { type: string; help: string }>();
  const samples: ParsedSample[] = [];
  const histograms = new Map<string, Map<string, HistogramSeries>>();
  const seen = new Set<string>();

  for (const [index, line] of text.split('\n').slice(0, -1).entries()) {
    const where = `line ${index + 1}: ${line}`;
    if (line === '') throw new Error(`blank line in exposition at ${where}`);

    if (line.startsWith('#')) {
      const help = /^# HELP (\S+) (.*)$/.exec(line);
      const type = /^# TYPE (\S+) (\S+)$/.exec(line);
      if (help !== null) {
        const name = help[1] as string;
        if (!NAME.test(name)) throw new Error(`invalid metric name in ${where}`);
        families.set(name, { type: families.get(name)?.type ?? '', help: help[2] as string });
        continue;
      }
      if (type !== null) {
        const name = type[1] as string;
        const kind = type[2] as string;
        if (!['counter', 'gauge', 'histogram', 'summary', 'untyped'].includes(kind)) {
          throw new Error(`unknown metric type in ${where}`);
        }
        families.set(name, { type: kind, help: families.get(name)?.help ?? '' });
        continue;
      }
      throw new Error(`malformed comment at ${where}`);
    }

    const match = SAMPLE.exec(line);
    if (match === null) throw new Error(`malformed sample at ${where}`);
    const name = match[1] as string;
    const labels = parseLabels(match[3] ?? '', where);
    const raw = match[4] as string;
    if (!/^[+-]?(\d+(\.\d+)?|Inf|NaN)$/.test(raw)) {
      throw new Error(`value "${raw}" is not plain decimal at ${where}`);
    }
    const value = raw === '+Inf' ? Infinity : Number(raw);

    const family = baseName(name, families);
    const declared = families.get(family);
    if (declared === undefined || declared.type === '') {
      throw new Error(`sample for undeclared family "${family}" at ${where}`);
    }
    if (declared.help === '') throw new Error(`family "${family}" has no HELP`);

    const identity = `${name}${JSON.stringify(labels)}`;
    if (seen.has(identity)) throw new Error(`duplicate series at ${where}`);
    seen.add(identity);

    if (declared.type === 'histogram') {
      const byLabels = histograms.get(family) ?? new Map<string, HistogramSeries>();
      histograms.set(family, byLabels);
      const { le: _le, ...rest } = labels;
      const key = JSON.stringify(rest);
      const series = byLabels.get(key) ?? { buckets: new Map(), sum: undefined, count: undefined };
      byLabels.set(key, series);

      if (name.endsWith('_bucket')) {
        const le = labels['le'];
        if (le === undefined) throw new Error(`bucket without an le label at ${where}`);
        series.buckets.set(le, value);
      } else if (name.endsWith('_sum')) {
        series.sum = value;
      } else if (name.endsWith('_count')) {
        series.count = value;
      } else {
        throw new Error(`unexpected histogram sample name at ${where}`);
      }
      continue;
    }

    samples.push({ name, labels, value });
  }

  for (const [family, byLabels] of histograms) {
    for (const [key, series] of byLabels) {
      const inf = series.buckets.get('+Inf');
      if (inf === undefined) throw new Error(`${family}${key} has no +Inf bucket`);
      if (series.count === undefined) throw new Error(`${family}${key} has no _count`);
      if (series.sum === undefined) throw new Error(`${family}${key} has no _sum`);
      if (inf !== series.count) throw new Error(`${family}${key}: +Inf bucket ${inf} != count ${series.count}`);

      const edges = [...series.buckets.entries()]
        .filter(([le]) => le !== '+Inf')
        .map(([le, count]) => [Number(le), count] as const)
        .sort((a, b) => a[0] - b[0]);
      let previous = 0;
      for (const [edge, count] of edges) {
        if (count < previous) throw new Error(`${family}${key}: bucket ${edge} is not cumulative`);
        previous = count;
      }
    }
  }

  return { families, samples, histograms };
}

function baseName(name: string, families: ReadonlyMap<string, unknown>): string {
  for (const suffix of ['_bucket', '_sum', '_count']) {
    if (name.endsWith(suffix)) {
      const trimmed = name.slice(0, -suffix.length);
      if (families.has(trimmed)) return trimmed;
    }
  }
  return name;
}

function parseLabels(text: string, where: string): Record<string, string> {
  const labels: Record<string, string> = {};
  if (text === '') return labels;

  let rest = text;
  while (rest.length > 0) {
    const match = /^([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"(,|$)/.exec(rest);
    if (match === null) throw new Error(`malformed label set at ${where}`);
    const name = match[1] as string;
    if (name in labels) throw new Error(`duplicate label ${name} at ${where}`);
    labels[name] = (match[2] as string).replace(/\\n/g, '\n').replace(/\\"/g, '"').replace(/\\\\/g, '\\');
    rest = rest.slice(match[0].length);
  }
  return labels;
}

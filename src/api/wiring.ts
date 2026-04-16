/**
 * Composition root for the API process.
 *
 * Everything the API talks to is chosen here and nowhere else: the real store from
 * `src/events`, the real projector from `src/state`, the real tool executor from
 * `src/tools`. The one piece still local to the API layer is the control plane, because
 * `src/control-plane/` is not written yet; when it is, this function is the only thing
 * that changes.
 */

import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { EventStore, NewEvent } from '../events/types.js';
import { MemoryEventStore } from '../events/memory-store.js';
import { DefaultToolExecutor } from '../tools/executor.js';
import {
  createFlakyTool,
  createFsReadTool,
  createFileWriteTool,
  createHttpFetchTool,
  registerAll,
} from '../tools/builtin.js';
import { DEFAULT_RETRY, type WorkflowDef, type WorkflowId } from '../workflow/types.js';
import { CostLedger } from './cost.js';
import {
  LocalChangeFeed,
  LocalDefinitionStore,
  type ApiDeps,
  type ChangeFeed,
  type DefinitionStore,
} from './deps.js';
import { LocalControlPlane } from './local/control-plane.js';
import { bindProjection } from './projection.js';

export interface Wiring {
  readonly deps: ApiDeps;
  readonly cost: CostLedger;
  readonly store: EventStore;
  readonly changes: ChangeFeed;
  readonly definitions: DefinitionStore;
}

export interface WiringOptions {
  /** Per-tenant ceilings in cost units. */
  readonly budgets?: ReadonlyMap<string, number>;
  readonly now?: () => number;
  /** Root the filesystem tools are confined to. A temp directory by default. */
  readonly toolRoot?: string;
}

export function defaultWiring(options: WiringOptions = {}): Wiring {
  const changes = new LocalChangeFeed();
  const store = new MemoryEventStore();
  // Bridged through the store's own `subscribe`, not through a wrapper around it. The
  // appends that matter most to a dashboard are not the API's — a step completing is
  // written by a worker — and with a wrapper, any component handed the inner store
  // would keep working while every open dashboard silently went stale. Subscribing to
  // the store itself means there is no inner store to be handed.
  changes.bridge(store);
  const definitions = new LocalDefinitionStore();
  const { project, replayTo } = bindProjection(definitions);
  const controlPlane = new LocalControlPlane(store, options.now);

  const root = options.toolRoot ?? join(tmpdir(), 'parliament-tools');
  const tools = new DefaultToolExecutor();
  registerAll(tools, [
    createHttpFetchTool({ allowedHosts: ['example.com', 'localhost'] }),
    createFsReadTool({ root }),
    createFileWriteTool({ root }),
    createFlakyTool(),
  ]);

  const deps: ApiDeps = {
    store,
    project,
    replayTo,
    tools,
    controlPlane,
    definitions,
    changes,
    ...(options.now !== undefined ? { now: options.now } : {}),
  };

  return {
    deps,
    store,
    changes,
    definitions,
    cost: new CostLedger({
      tools,
      ...(options.budgets !== undefined ? { budgets: options.budgets } : {}),
    }),
  };
}

/** A seed event with the envelope fields the helper fills in made optional. */
type SeedEvent<T extends NewEvent = NewEvent> = T extends NewEvent
  ? Omit<T, 'workflowId' | 'at'> & { readonly at?: number }
  : never;

/**
 * A history worth looking at.
 *
 * Written straight to the store rather than driven through the scheduler, because the
 * point is to exercise the *views*: a diamond graph, a step that failed twice before
 * succeeding, a lease that expired, a tool call with a real result, a human gate still
 * waiting, and a failure at the end with a path leading to it. Every one of those is a
 * case the dashboard has to render legibly.
 */
export async function seedDemo(wiring: Wiring, t0 = Date.now() - 600_000): Promise<WorkflowId> {
  const definition: WorkflowDef = {
    name: 'refund-review',
    version: 3,
    start: 'intake',
    timeouts: { attemptTimeoutMs: 30_000, leaseDurationMs: 15_000 },
    nodes: [
      { name: 'intake', kind: 'tool', dependsOn: [], tool: 'http.fetch' },
      { name: 'classify', kind: 'llm', dependsOn: ['intake'] },
      // A retry policy, because the seeded history retries this step: the projector reads
      // the policy to decide whether a failure is terminal, so a definition without one
      // would make its own history an illegal transition.
      { name: 'fraud-score', kind: 'python', dependsOn: ['intake'], retry: DEFAULT_RETRY },
      { name: 'join-signals', kind: 'join', dependsOn: ['classify', 'fraud-score'] },
      {
        name: 'approve-refund',
        kind: 'approval',
        dependsOn: ['join-signals'],
        approvers: ['sam@example.com', 'dana@example.com'],
        approvalPrompt: 'Authorize a refund of $420.00 to charge ch_1a2b3c?',
      },
      { name: 'issue-refund', kind: 'tool', dependsOn: ['approve-refund'], tool: 'fs.write' },
      { name: 'notify', kind: 'tool', dependsOn: ['issue-refund'], tool: 'http.fetch' },
    ],
  };

  const workflowId = 'wf_demo_refund_review';
  wiring.definitions.put(workflowId, definition);

  let at = t0;
  const tick = (ms: number): number => (at += ms);
  const e = (event: SeedEvent): NewEvent => ({ workflowId, at, ...event }) as NewEvent;

  const events: NewEvent[] = [
    e({ type: 'workflow.created', workflow: definition.name, definitionVersion: 3, input: { chargeId: 'ch_1a2b3c', amountCents: 42_000 }, tenant: 'acme', priority: 10 }),
    e({ type: 'step.scheduled', step: 'intake', attempt: 1 }),
    e({ type: 'step.leased', step: 'intake', attempt: 1, workerId: 'worker-a', leaseExpiresAt: tick(40) + 15_000 }),
    e({ type: 'step.started', step: 'intake', attempt: 1, workerId: 'worker-a' }),
    e({ type: 'tool.requested', step: 'intake', attempt: 1, tool: 'http.fetch', args: { url: 'https://payments.internal/charges/ch_1a2b3c' }, idempotencyKey: 'wf_demo_refund_review/intake/1' }),
    e({ type: 'tool.succeeded', step: 'intake', attempt: 1, tool: 'http.fetch', result: { status: 200, body: '{"charge":"ch_1a2b3c","amountCents":42000,"customer":"cus_99"}' }, durationMs: 312, deduplicated: false, at: tick(312) }),
    e({ type: 'step.completed', step: 'intake', attempt: 1, workerId: 'worker-a', output: { charge: 'ch_1a2b3c', customer: 'cus_99' }, at: tick(8) }),

    e({ type: 'step.scheduled', step: 'classify', attempt: 1, at: tick(5) }),
    e({ type: 'step.scheduled', step: 'fraud-score', attempt: 1 }),
    e({ type: 'step.leased', step: 'classify', attempt: 1, workerId: 'worker-b', leaseExpiresAt: at + 15_000 }),
    e({ type: 'step.started', step: 'classify', attempt: 1, workerId: 'worker-b', at: tick(20) }),
    e({ type: 'step.completed', step: 'classify', attempt: 1, workerId: 'worker-b', output: { label: 'likely-duplicate-charge', usage: { model: 'default', inputTokens: 1_840, outputTokens: 96 } }, at: tick(1_430) }),

    e({ type: 'step.leased', step: 'fraud-score', attempt: 1, workerId: 'worker-c', leaseExpiresAt: at + 15_000 }),
    e({ type: 'step.started', step: 'fraud-score', attempt: 1, workerId: 'worker-c' }),
    // Two renewals and then an expiry: a step that was heartbeating and then stopped,
    // which is the case a repeated `step.leased` used to render as a stuck lease.
    e({ type: 'step.lease_extended', step: 'fraud-score', attempt: 1, workerId: 'worker-c', leaseExpiresAt: tick(7_000) + 15_000 }),
    e({ type: 'step.lease_extended', step: 'fraud-score', attempt: 1, workerId: 'worker-c', leaseExpiresAt: tick(7_000) + 15_000 }),
    e({ type: 'step.lease_expired', step: 'fraud-score', attempt: 1, workerId: 'worker-c', at: tick(15_000) }),
    e({ type: 'step.leased', step: 'fraud-score', attempt: 1, workerId: 'worker-d', leaseExpiresAt: at + 15_000, at: tick(60) }),
    e({ type: 'step.started', step: 'fraud-score', attempt: 1, workerId: 'worker-d' }),
    e({ type: 'step.failed', step: 'fraud-score', attempt: 1, workerId: 'worker-d', error: 'scoring service returned 503', errorClass: 'unavailable', willRetry: true, at: tick(900) }),
    e({ type: 'step.retry_scheduled', step: 'fraud-score', attempt: 2, notBefore: at + 540, backoffMs: 540 }),
    e({ type: 'step.scheduled', step: 'fraud-score', attempt: 2, at: tick(540) }),
    e({ type: 'step.leased', step: 'fraud-score', attempt: 2, workerId: 'worker-d', leaseExpiresAt: at + 15_000 }),
    e({ type: 'step.started', step: 'fraud-score', attempt: 2, workerId: 'worker-d' }),
    e({ type: 'step.completed', step: 'fraud-score', attempt: 2, workerId: 'worker-d', output: { score: 0.17, band: 'low' }, at: tick(770) }),

    e({ type: 'step.scheduled', step: 'join-signals', attempt: 1, at: tick(10) }),
    e({ type: 'step.started', step: 'join-signals', attempt: 1, workerId: 'worker-b' }),
    e({ type: 'step.completed', step: 'join-signals', attempt: 1, workerId: 'worker-b', output: { recommend: 'refund', confidence: 0.82 }, at: tick(24) }),

    e({ type: 'step.scheduled', step: 'approve-refund', attempt: 1, at: tick(6) }),
    e({ type: 'approval.requested', step: 'approve-refund', approvers: ['sam@example.com', 'dana@example.com'], prompt: 'Authorize a refund of $420.00 to charge ch_1a2b3c?', payload: { chargeId: 'ch_1a2b3c', amountCents: 42_000, recommendation: 'refund', fraudBand: 'low' } }),
  ];

  await wiring.store.append(workflowId, 0, events);

  // A second execution of the same definition that ran to a failure, so the list has
  // more than one row and the critical-path highlight has something to point at.
  const failedId = 'wf_demo_refund_failed';
  wiring.definitions.put(failedId, definition);
  let bt = t0 + 30_000;
  const btick = (ms: number): number => (bt += ms);
  const f = (event: SeedEvent): NewEvent =>
    ({ workflowId: failedId, at: bt, ...event }) as NewEvent;

  await wiring.store.append(failedId, 0, [
    f({ type: 'workflow.created', workflow: definition.name, definitionVersion: 3, input: { chargeId: 'ch_zzz', amountCents: 1_200 }, tenant: 'globex' }),
    f({ type: 'step.scheduled', step: 'intake', attempt: 1 }),
    f({ type: 'step.started', step: 'intake', attempt: 1, workerId: 'worker-a' }),
    f({ type: 'step.completed', step: 'intake', attempt: 1, workerId: 'worker-a', output: { charge: 'ch_zzz' } }),
    f({ type: 'step.scheduled', step: 'classify', attempt: 1, at: btick(40) }),
    f({ type: 'step.started', step: 'classify', attempt: 1, workerId: 'worker-b' }),
    f({ type: 'step.failed', step: 'classify', attempt: 1, workerId: 'worker-b', error: 'model returned malformed JSON for the third time', errorClass: 'invalid_output', willRetry: false, at: btick(2_400) }),
    f({ type: 'workflow.failed', step: 'classify', error: 'model returned malformed JSON for the third time', errorClass: 'invalid_output' }),
  ]);

  return workflowId;
}

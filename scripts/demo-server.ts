/**
 * The demo's control plane.
 *
 * This is a composition root, not new machinery. Everything load-bearing comes from `src`:
 * the durable `FileEventStore`, the real `Scheduler`, the real `LeaseManager` and
 * `RetryEngine` behind `scheduler.workerPort()`, the real projector, and the real HTTP API.
 * What this file adds is the two seams `src` does not ship:
 *
 *  1. **A control plane behind the API's port.** `src/api/wiring.ts` wires `LocalControlPlane`,
 *     which writes the events a request unambiguously implies and schedules nothing — it exists
 *     so the API is testable on its own, and its own comment says the real scheduler replaces
 *     it. `SchedulerControlPlane` below is that replacement, and it is four delegating methods.
 *
 *  2. **A worker transport.** `src/workers/ports.ts` declares what a worker needs and
 *     deliberately depends on nothing in the scheduler, because workers are the component most
 *     likely to run out of process. `python-worker/PROTOCOL.md` specifies one such transport
 *     over a socket; this is the same four operations over HTTP, which is what lets the demo
 *     run workers as real OS processes and kill one with a signal. A worker that could be
 *     "killed" only by setting a flag inside this process would prove nothing about leases.
 *
 * Why the store lives here and only here: `FileEventStore` serializes appends with an
 * in-process lock and assigns sequence numbers from an in-memory index. Two processes opening
 * the same directory would both believe they knew the head, and the second append would be
 * rejected as corrupt — or worse, accepted. One process owns the log; everyone else talks to
 * it. That is a property of this store, not of the design, and the HTTP seam is where a
 * different store with conditional writes would be swapped in.
 *
 * The scheduler here runs with `maxInFlight: 0`, which stops it dispatching while leaving
 * everything else it does intact: expiring lapsed leases, requesting approvals, enforcing
 * deadlines, and declaring a workflow complete. Execution belongs to the pulled worker pool.
 * Letting both dispatch would be safe — one of them simply loses the lease append — but it
 * would make the demo's output confusing about who ran what.
 */

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { FileEventStore } from '../src/events/file-store.js';
import type { EventStore } from '../src/events/types.js';
import { bindProjection } from '../src/api/projection.js';
import { CostLedger } from '../src/api/cost.js';
import {
  LocalChangeFeed,
  LocalDefinitionStore,
  type ApiDeps,
  type ControlPlanePort,
  type SubmitRequest,
} from '../src/api/deps.js';
import { createHandler } from '../src/api/server.js';
import { ApiError, asRecord, readJsonBody, sendJson } from '../src/api/http.js';
import { systemClock } from '../src/scheduler/clock.js';
import { Scheduler } from '../src/scheduler/scheduler.js';
import { DefaultToolExecutor } from '../src/tools/executor.js';
import { registerAll } from '../src/tools/builtin.js';
import type { RecordedOutcome } from '../src/tools/idempotency.js';
import type { ToolOutcome } from '../src/tools/types.js';
import { ToolError } from '../src/tools/types.js';
import type { Lease } from '../src/workers/ports.js';
import type { StepName, WorkflowId } from '../src/workflow/types.js';
import { demoTools } from './demo-tools.js';
import { ALL, LEASE_MS } from './demo-workflows.js';

/**
 * The API's write port, backed by the scheduler.
 *
 * Every method delegates. The one thing this adds is submission idempotency, which the API
 * accepts a key for and the scheduler has no concept of: a client that retries a submission it
 * never saw the response to must not get a second execution.
 */
class SchedulerControlPlane implements ControlPlanePort {
  private readonly submitted = new Map<string, WorkflowId>();
  private counter = 0;

  constructor(
    private readonly inner: Scheduler,
    private readonly store: EventStore,
  ) {}

  async submit(request: SubmitRequest): Promise<WorkflowId> {
    const key = request.idempotencyKey;
    const existing = key === undefined ? undefined : this.submitted.get(key);
    if (existing !== undefined) return existing;

    // Readable rather than a UUID, because every line the demo prints contains one of these
    // and a reader has to be able to tell two workflows apart at a glance. The cost of a
    // readable counter is that it restarts with the process, so the log is asked whether the
    // id is free: without that, the first submission after the control plane is restarted
    // mid-demo would collide with an existing workflow, and `startAs` would be appending at
    // sequence 0 against a history that already has one.
    let id = '';
    do {
      id = `wf_${request.definition.name}_${++this.counter}`;
    } while ((await this.store.head(id)) !== 0);

    await this.inner.startAs(id, request.definition, {
      input: request.input,
      ...(request.tenant !== undefined ? { tenant: request.tenant } : {}),
      ...(request.deadlineAt !== undefined ? { deadlineAt: request.deadlineAt } : {}),
    });
    if (key !== undefined) this.submitted.set(key, id);
    return id;
  }

  async cancel(workflowId: WorkflowId, reason: string, requestedBy?: string): Promise<void> {
    await this.inner.cancel(workflowId, reason, requestedBy);
  }

  async approve(workflowId: WorkflowId, step: StepName, approver: string, note?: string): Promise<void> {
    await this.inner.approve(workflowId, step, approver, note);
  }

  async deny(workflowId: WorkflowId, step: StepName, approver: string, reason: string): Promise<void> {
    await this.inner.deny(workflowId, step, approver, reason);
  }
}

const dataDir = required('DEMO_DATA_DIR');
const port = Number(process.env['DEMO_PORT'] ?? 0);
const tickMs = Number(process.env['DEMO_TICK_MS'] ?? 120);

const store = await FileEventStore.open(join(dataDir, 'events'));
const changes = new LocalChangeFeed();
changes.bridge(store);
const definitions = new LocalDefinitionStore();
const { project, replayTo } = bindProjection(definitions);

const scheduler = new Scheduler({
  store,
  clock: systemClock,
  tickIntervalMs: tickMs,
  // Zero: this process arbitrates and records, the worker processes execute. See the note
  // at the top of the file.
  maxInFlight: 0,
  leaseDurationMs: LEASE_MS,
  dispatch: () =>
    Promise.reject(
      new Error('the demo control plane does not execute steps; worker processes pull them'),
    ),
});
for (const def of ALL) scheduler.register(def);

const workerPort = scheduler.workerPort({ role: 'worker' });

/**
 * The tool registry, present for the API's `/tools` endpoint and the cost ledger.
 *
 * The handlers are registered but never invoked here — tools execute in the worker processes.
 * Registering the real definitions anyway keeps `/tools` honest about what this deployment
 * can do, which is the question that endpoint exists to answer.
 */
const tools = new DefaultToolExecutor();
registerAll(tools, demoTools({ ledgerPath: join(dataDir, 'ledger.tsv'), statePath: join(dataDir, 'state') }));

/** Shared recorded tool outcomes. See `ResultStore` in `src/tools/idempotency.ts`. */
const results = new Map<string, RecordedOutcome>();

const deps: ApiDeps = {
  store,
  project,
  replayTo,
  tools,
  controlPlane: new SchedulerControlPlane(scheduler, store),
  definitions,
  changes,
};

const api = createHandler({ deps, cost: new CostLedger({ tools }), serveDashboard: true });

const server = createServer((req, res) => {
  void handle(req, res).catch((cause: unknown) => {
    if (res.headersSent) {
      res.end();
      return;
    }
    if (cause instanceof ApiError) {
      sendJson(res, cause.status, cause.toBody());
      return;
    }
    sendJson(res, 500, {
      error: 'internal',
      message: cause instanceof Error ? cause.message : String(cause),
    });
  });
});

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://localhost');
  if (url.pathname.startsWith('/worker/') || url.pathname.startsWith('/results/')) {
    await workerTransport(req, res, url);
    return;
  }
  if (url.pathname.startsWith('/demo/')) {
    demoRoutes(res, url);
    return;
  }
  await api(req, res);
}

/**
 * Serves the demo's workflow definitions.
 *
 * So that `scripts/demo.sh` can submit one by name instead of carrying a second copy of every
 * definition as a JSON heredoc. Two copies of a graph is the kind of duplication that stays
 * correct for exactly as long as nobody edits either: the shell would keep submitting the old
 * shape, the server would keep accepting it, and the demo would quietly stop demonstrating the
 * definitions in `demo-workflows.ts`.
 */
function demoRoutes(res: ServerResponse, url: URL): void {
  if (url.pathname === '/demo/definitions') {
    sendJson(res, 200, { definitions: ALL.map((def) => def.name) });
    return;
  }
  const name = /^\/demo\/definitions\/([A-Za-z0-9_-]+)$/.exec(url.pathname)?.[1];
  const def = ALL.find((candidate) => candidate.name === name);
  if (def === undefined) {
    throw ApiError.notFound(`no demo definition named "${name ?? url.pathname}"`);
  }
  sendJson(res, 200, def);
}

/**
 * The worker-facing transport.
 *
 * A direct mapping onto `SchedulerWorkerPort`, which already implements the three ports a
 * worker needs. Nothing is decided here: the handler reads a body, calls one method, and
 * writes the result. Any judgement in this layer would be a scheduling decision made outside
 * the scheduler, which is the mistake `src/workers/ports.ts` is arranged to prevent.
 */
async function workerTransport(req: IncomingMessage, res: ServerResponse, url: URL): Promise<void> {
  const path = url.pathname;
  const method = req.method ?? 'GET';

  if (method === 'POST' && path === '/worker/acquire') {
    const body = asRecord(await readJsonBody(req));
    const assignment = await workerPort.acquire(stringField(body, 'workerId'));
    if (assignment === undefined) {
      // 204 rather than an empty 200: "nothing to do" is the common answer to a poll, and a
      // body-less status lets a worker tell it apart from a grant without parsing.
      res.writeHead(204).end();
      return;
    }
    sendJson(res, 200, assignment);
    return;
  }

  if (method === 'POST' && path === '/worker/heartbeat') {
    const body = asRecord(await readJsonBody(req));
    sendJson(res, 200, { alive: await workerPort.heartbeat(leaseField(body)) });
    return;
  }

  if (method === 'POST' && path === '/worker/release') {
    await workerPort.release(leaseField(asRecord(await readJsonBody(req))));
    res.writeHead(204).end();
    return;
  }

  if (method === 'POST' && path === '/worker/completed') {
    const body = asRecord(await readJsonBody(req));
    await workerPort.completed(leaseField(body), body['output']);
    res.writeHead(204).end();
    return;
  }

  if (method === 'POST' && path === '/worker/failed') {
    const body = asRecord(await readJsonBody(req));
    const error = asRecord(body['error']);
    await workerPort.failed(leaseField(body), {
      message: stringField(error, 'message'),
      errorClass: stringField(error, 'errorClass'),
    });
    res.writeHead(204).end();
    return;
  }

  if (method === 'GET' && path === '/worker/outputs') {
    const workflowId = url.searchParams.get('workflowId');
    if (workflowId === null) throw ApiError.badRequest('workflowId is required');
    const outputs = await workerPort.completedOutputs(workflowId);
    sendJson(res, 200, { outputs: [...outputs.values()] });
    return;
  }

  /**
   * The shared result store.
   *
   * Recorded tool outcomes have to be reachable from every worker or the deduplication claim
   * is false the moment a step is reassigned to a different process: an in-process store dies
   * with the worker that holds it, and the replacement worker re-executes a side effect that
   * already happened. Keeping it here — in the process that owns the log — is the smallest
   * arrangement that makes the claim true across a worker's death, and it is also exactly what
   * `ResultStore`'s comment means by "a durable store sized to its retention window".
   */
  const resultKey = /^\/results\/([A-Za-z0-9_-]{1,128})$/.exec(path)?.[1];
  if (resultKey !== undefined && method === 'GET') {
    const recorded = results.get(resultKey);
    if (recorded === undefined) {
      res.writeHead(404).end();
      return;
    }
    sendJson(res, 200, recorded);
    return;
  }
  if (resultKey !== undefined && method === 'PUT') {
    const body = asRecord(await readJsonBody(req));
    // First write wins, matching the `ResultStore` contract: the first observation is the one
    // the workflow may already have acted on, and replacing it would mean two steps reading
    // different results for one call.
    if (!results.has(resultKey)) {
      results.set(resultKey, {
        outcome: body['outcome'] as ToolOutcome,
        recordedAt: Number(body['recordedAt'] ?? Date.now()),
      });
    }
    res.writeHead(204).end();
    return;
  }

  throw ApiError.notFound(`no worker route for ${method} ${path}`);
}


function stringField(body: Record<string, unknown>, field: string): string {
  const value = body[field];
  if (typeof value !== 'string' || value === '') {
    throw ApiError.badRequest(`"${field}" must be a non-empty string`);
  }
  return value;
}

function leaseField(body: Record<string, unknown>): Lease {
  const lease = body['lease'];
  if (typeof lease !== 'object' || lease === null) {
    throw ApiError.badRequest('"lease" must be the lease object the grant carried');
  }
  return lease as Lease;
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === '') {
    throw new Error(`${name} must be set; run this through scripts/demo.sh`);
  }
  return value;
}

// Recovery before the loop starts, and before the port will hand anything out. On a fresh
// directory this does nothing; on a restart it is the whole of crash recovery — a replay of
// the log and a tick, with no reconciliation step and nothing to get wrong.
await mkdir(dataDir, { recursive: true });
await scheduler.recover();
scheduler.startLoop();

server.listen(port, '127.0.0.1', () => {
  const address = server.address();
  const bound = typeof address === 'object' && address !== null ? address.port : port;
  // Written to files as well as to stdout, because the shell needs both back and grepping a log
  // races with the log being written. The pid matters more than it looks: `tsx` runs the script
  // in a *child* node process, so the pid a shell gets from `$!` belongs to the launcher, and
  // killing that one leaves this one running — which would make the crash-recovery part of the
  // demo claim a recovery that never had to happen.
  void writeFile(join(dataDir, 'port'), `${bound}\n`, 'utf8');
  void writeFile(join(dataDir, 'server.pid'), `${process.pid}\n`, 'utf8');
  process.stdout.write(`control plane listening on http://127.0.0.1:${bound}\n`);
  process.stdout.write(`dashboard http://127.0.0.1:${bound}/ui/\n`);
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    scheduler.stop();
    server.close(() => process.exit(0));
  });
}

// A tool error reaching here means a bug in this file rather than a step failure, so it is
// named as such instead of being reported as an unhandled rejection with no context.
process.on('unhandledRejection', (reason: unknown) => {
  const detail = reason instanceof ToolError ? `${reason.errorClass}: ${reason.message}` : String(reason);
  process.stderr.write(`control plane unhandled rejection: ${detail}\n`);
});

/**
 * The HTTP control surface.
 *
 * `node:http` and nothing else. A framework would be the project's only runtime
 * dependency, bought for a routing table of thirteen entries and a JSON body reader —
 * and the body reader is the part that has to be bounded carefully anyway, which is
 * the part a framework would hide.
 *
 * Two conventions run through all of it:
 *
 *  - **Every error body says why.** A 400 lists the specific definition errors, a 409
 *    names the state that made the transition illegal. An operator should never have
 *    to read this file to understand a response.
 *  - **The transport does not own state.** Reads fold the log; writes go through the
 *    control-plane port. The one thing the transport decides for itself is
 *    admissibility, because that is an API-level concern: shape, size, budget, and
 *    whether the requested transition is legal from the current state.
 */

import { createServer as createHttpServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Event, Sequence, WorkflowProjection } from '../events/types.js';
import { ConcurrencyError } from '../events/types.js';
import type { ToolDef } from '../tools/types.js';
import {
  isTerminal,
  validateDef,
  type DefError,
  type RunState,
  type WorkflowDef,
  type WorkflowId,
} from '../workflow/types.js';
import type { ApiDeps } from './deps.js';
import { CostLedger, type WorkflowCost } from './cost.js';
import { renderMetrics, type MetricsInput } from './metrics.js';
import { buildTrace, type Trace } from './trace.js';
import {
  ApiError,
  asRecord,
  intParam,
  matchPath,
  optionalString,
  readJsonBody,
  requireString,
  sendJson,
  sendText,
  MAX_BODY_BYTES,
} from './http.js';

export interface ServerOptions {
  readonly deps: ApiDeps;
  readonly cost: CostLedger;
  /** Default page size for `GET /workflows`. */
  readonly pageSize?: number;
  /** Serve `dashboard/` under `/ui/`. Off in tests, on for the real server. */
  readonly serveDashboard?: boolean;
  readonly maxBodyBytes?: number;
}

const DASHBOARD_ROOT = fileURLToPath(new URL('../../dashboard/', import.meta.url));

const STATIC_TYPES: Readonly<Record<string, string>> = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.json': 'application/json; charset=utf-8',
};

/** The request handler, exposed separately so tests can drive it without a socket. */
export function createHandler(options: ServerOptions) {
  const { deps, cost } = options;
  const pageSize = options.pageSize ?? 25;
  const maxBody = options.maxBodyBytes ?? MAX_BODY_BYTES;
  const now = deps.now ?? Date.now;

  const historyOf = async (id: WorkflowId): Promise<readonly Event[]> => {
    const events = await deps.store.read(id);
    if (events.length === 0) throw ApiError.notFound(`no workflow with id "${id}"`);
    return events;
  };

  const projectionOf = async (id: WorkflowId): Promise<WorkflowProjection> =>
    deps.project(await historyOf(id));

  /** Loads every workflow once, for the list, the metrics scrape and tenant spend. */
  const loadAll = async (): Promise<{ projection: WorkflowProjection; events: readonly Event[] }[]> => {
    const ids = await deps.store.list();
    const loaded = await Promise.all(
      ids.map(async (id) => {
        const events = await deps.store.read(id);
        return { projection: deps.project(events), events };
      }),
    );
    return loaded.filter((entry) => entry.events.length > 0);
  };

  return async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const path = url.pathname;
    const method = req.method ?? 'GET';

    try {
      if (method === 'OPTIONS') {
        res.writeHead(204, {
          'access-control-allow-origin': '*',
          'access-control-allow-methods': 'GET,POST,OPTIONS',
          'access-control-allow-headers': 'content-type',
        });
        res.end();
        return;
      }

      if (method === 'GET' && path === '/healthz') {
        const ids = await deps.store.list();
        sendJson(res, 200, {
          status: 'ok',
          workflows: ids.length,
          activeWorkflows: (await deps.store.listActive()).length,
          at: now(),
        });
        return;
      }

      if (method === 'GET' && path === '/metrics') {
        const all = await loadAll();
        const inputs: MetricsInput[] = all.map(({ projection, events }) => ({
          projection,
          events,
          cost: cost.forWorkflow(events),
        }));
        sendText(res, 200, 'text/plain; version=0.0.4; charset=utf-8', renderMetrics(inputs));
        return;
      }

      if (method === 'GET' && path === '/tools') {
        sendJson(res, 200, { tools: deps.tools.list().map(serializeTool) });
        return;
      }

      if (method === 'POST' && path === '/workflows') {
        await submit(req, res);
        return;
      }

      if (method === 'GET' && path === '/workflows') {
        await list(url, res);
        return;
      }

      const stream = matchPath('/workflows/:id/stream', path);
      if (method === 'GET' && stream) {
        await openStream(req, res, stream['id'] as WorkflowId, url);
        return;
      }

      const events = matchPath('/workflows/:id/events', path);
      if (method === 'GET' && events) {
        const id = events['id'] as WorkflowId;
        const after = intParam(url.searchParams, 'after', 0);
        // The existence check uses the full history: `read(id, after)` returning empty
        // is ambiguous between "no such workflow" and "nothing new since `after`", and
        // answering 404 to the second would make tailing clients give up.
        await historyOf(id);
        sendJson(res, 200, { workflowId: id, after, events: await deps.store.read(id, after) });
        return;
      }

      const trace = matchPath('/workflows/:id/trace', path);
      if (method === 'GET' && trace) {
        const id = trace['id'] as WorkflowId;
        const history = await historyOf(id);
        const def = deps.definitions.get(id);
        sendJson(res, 200, serializeTrace(buildTrace(history, def), cost.forWorkflow(history)));
        return;
      }

      const replay = matchPath('/workflows/:id/replay', path);
      if (method === 'GET' && replay) {
        const id = replay['id'] as WorkflowId;
        const history = await historyOf(id);
        const head = (history[history.length - 1] as Event).sequence;
        const sequence = intParam(url.searchParams, 'sequence', head);
        if (sequence < 1) {
          throw ApiError.badRequest('sequence must be at least 1; the log is 1-based');
        }
        if (sequence > head) {
          throw ApiError.badRequest(
            `sequence ${sequence} is beyond the end of the log for "${id}" (head is ${head})`,
            { head },
          );
        }
        sendJson(res, 200, {
          workflowId: id,
          sequence,
          head,
          projection: serializeProjection(deps.replayTo(history, sequence)),
        });
        return;
      }

      const costPath = matchPath('/workflows/:id/cost', path);
      if (method === 'GET' && costPath) {
        const id = costPath['id'] as WorkflowId;
        sendJson(res, 200, cost.forWorkflow(await historyOf(id)));
        return;
      }

      const tenantCost = matchPath('/tenants/:tenant/cost', path);
      if (method === 'GET' && tenantCost) {
        const tenant = tenantCost['tenant'] as string;
        const all = await loadAll();
        sendJson(res, 200, cost.forTenant(tenant, all.map((w) => cost.forWorkflow(w.events))));
        return;
      }

      const cancel = matchPath('/workflows/:id/cancel', path);
      if (method === 'POST' && cancel) {
        await doCancel(req, res, cancel['id'] as WorkflowId);
        return;
      }

      const approve = matchPath('/workflows/:id/steps/:step/approve', path);
      if (method === 'POST' && approve) {
        await decide(req, res, approve['id'] as WorkflowId, approve['step'] as string, 'approve');
        return;
      }

      const deny = matchPath('/workflows/:id/steps/:step/deny', path);
      if (method === 'POST' && deny) {
        await decide(req, res, deny['id'] as WorkflowId, deny['step'] as string, 'deny');
        return;
      }

      const workflow = matchPath('/workflows/:id', path);
      if (method === 'GET' && workflow) {
        sendJson(res, 200, serializeProjection(await projectionOf(workflow['id'] as WorkflowId)));
        return;
      }

      if (method === 'GET' && options.serveDashboard !== false) {
        if (path === '/' || path === '/ui' || path === '/ui/') {
          await serveStatic(res, 'index.html');
          return;
        }
        if (path.startsWith('/ui/')) {
          await serveStatic(res, path.slice('/ui/'.length));
          return;
        }
      }

      throw ApiError.notFound(`no route for ${method} ${path}`);
    } catch (cause) {
      fail(res, cause);
    }
  };

  async function submit(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const body = asRecord(await readJsonBody(req, maxBody));
    const definition = parseDefinition(body['definition']);

    const errors = validateDef(definition);
    if (errors.length > 0) {
      // Every error, not the first: a definition is usually wrong in several places,
      // and a caller fixing them one round-trip at a time is the thing to avoid.
      sendJson(res, 400, {
        error: 'invalid_definition',
        message: `workflow definition "${definition.name}" has ${errors.length} error(s)`,
        errors: errors.map(describeDefError),
      });
      return;
    }

    const tenant = optionalString(body, 'tenant', 128);
    const deadlineAt = body['deadlineAt'];
    if (deadlineAt !== undefined && (typeof deadlineAt !== 'number' || !Number.isFinite(deadlineAt))) {
      throw ApiError.badRequest('"deadlineAt" must be an epoch-millisecond number if present');
    }

    if (tenant !== undefined && cost.budgetFor(tenant) !== undefined) {
      const all = await loadAll();
      const spend = cost.forTenant(tenant, all.map((w) => cost.forWorkflow(w.events)));
      const decision = cost.admit(tenant, spend.totalUnits);
      if (!decision.allowed) {
        // 402 rather than 403: the request is well-formed and the caller is permitted,
        // the tenant has simply run out of budget. A 403 would send an operator
        // looking at roles, which is the wrong place.
        sendJson(res, 402, {
          error: 'budget_exceeded',
          message: decision.reason ?? 'tenant budget exceeded',
          detail: decision,
        });
        return;
      }
    }

    const workflowId = await deps.controlPlane.submit({
      definition,
      input: body['input'],
      ...(tenant !== undefined ? { tenant } : {}),
      ...(typeof deadlineAt === 'number' ? { deadlineAt } : {}),
      ...(optionalString(body, 'idempotencyKey', 256) !== undefined
        ? { idempotencyKey: optionalString(body, 'idempotencyKey', 256) as string }
        : {}),
    });

    deps.definitions.put(workflowId, definition);
    deps.changes.emit(workflowId);

    // The state is read back from the projection rather than asserted. What a fresh
    // submission folds to is the projector's business — it depends on whether the start
    // step was merely scheduled or is dispatchable — and a response that guessed would
    // disagree with the very next GET.
    const projection = await projectionOf(workflowId);
    res.writeHead(201, {
      'content-type': 'application/json; charset=utf-8',
      location: `/workflows/${encodeURIComponent(workflowId)}`,
      'access-control-allow-origin': '*',
    });
    res.end(
      JSON.stringify(
        {
          workflowId,
          state: projection.state,
          sequence: projection.sequence,
          definition: definition.name,
          version: definition.version,
        },
        null,
        2,
      ),
    );
  }

  async function list(url: URL, res: ServerResponse): Promise<void> {
    const stateFilter = url.searchParams.get('state');
    if (stateFilter !== null && !isRunState(stateFilter)) {
      throw ApiError.badRequest(`unknown state "${stateFilter}"`, { allowed: ALL_STATES });
    }
    const tenantFilter = url.searchParams.get('tenant');
    const nameFilter = url.searchParams.get('workflow');
    const limit = intParam(url.searchParams, 'limit', pageSize, 200);
    const offset = intParam(url.searchParams, 'offset', 0);

    const all = await loadAll();
    const matching = all
      .filter((w) => stateFilter === null || w.projection.state === stateFilter)
      .filter((w) => tenantFilter === null || w.projection.tenant === tenantFilter)
      .filter((w) => nameFilter === null || w.projection.workflow === nameFilter)
      // Newest first: a list of executions is read to find the one that just broke.
      .sort(
        (a, b) =>
          b.projection.createdAt - a.projection.createdAt ||
          a.projection.workflowId.localeCompare(b.projection.workflowId),
      );

    const page = matching.slice(offset, offset + limit);
    sendJson(res, 200, {
      workflows: page.map((w) => summarize(w.projection, w.events)),
      total: matching.length,
      limit,
      offset,
      ...(offset + limit < matching.length ? { nextOffset: offset + limit } : {}),
    });
  }

  async function doCancel(req: IncomingMessage, res: ServerResponse, id: WorkflowId): Promise<void> {
    const body = (await readJsonBody(req, maxBody)) ?? {};
    const projection = await projectionOf(id);
    if (isTerminal(projection.state)) {
      throw ApiError.conflict(
        `workflow "${id}" is already ${projection.state} and cannot be cancelled`,
        { state: projection.state, sequence: projection.sequence },
      );
    }

    const reason = optionalString(body, 'reason', 1024) ?? 'cancelled via API';
    const requestedBy = optionalString(body, 'requestedBy', 256);
    await deps.controlPlane.cancel(id, reason, requestedBy);
    deps.changes.emit(id);
    sendJson(res, 200, serializeProjection(await projectionOf(id)));
  }

  async function decide(
    req: IncomingMessage,
    res: ServerResponse,
    id: WorkflowId,
    step: string,
    action: 'approve' | 'deny',
  ): Promise<void> {
    const body = asRecord((await readJsonBody(req, maxBody)) ?? {});
    const approver = requireString(body, 'approver', 256);
    // A denial without a reason is an audit record that cannot be audited: whoever
    // reads it later needs to know what was objected to, not just that someone said no.
    const reason = action === 'deny' ? requireString(body, 'reason', 2048) : undefined;
    const note = optionalString(body, 'note', 2048);

    const projection = await projectionOf(id);
    if (isTerminal(projection.state)) {
      throw ApiError.conflict(
        `workflow "${id}" is ${projection.state}; a terminal workflow cannot be ${action === 'approve' ? 'approved' : 'denied'}`,
        { state: projection.state, step, sequence: projection.sequence },
      );
    }

    const stepProjection = projection.steps.get(step);
    if (!stepProjection) {
      throw ApiError.notFound(`workflow "${id}" has no step "${step}" in its history`);
    }
    if (!stepProjection.awaitingApproval) {
      throw ApiError.conflict(
        `step "${step}" is ${stepProjection.state} and is not awaiting approval`,
        { state: stepProjection.state, awaitingApproval: false },
      );
    }

    const requested = findApprovalRequest(await historyOf(id), step);
    if (requested && !requested.approvers.includes(approver)) {
      throw new ApiError(
        403,
        'not_an_approver',
        `"${approver}" is not an approver for step "${step}"`,
        { approvers: requested.approvers },
      );
    }

    if (action === 'approve') {
      await deps.controlPlane.approve(id, step, approver, note);
    } else {
      await deps.controlPlane.deny(id, step, approver, reason as string);
    }
    deps.changes.emit(id);
    sendJson(res, 200, serializeProjection(await projectionOf(id)));
  }

  /**
   * Server-Sent Events.
   *
   * SSE rather than a WebSocket because the traffic is one-directional and SSE
   * reconnects by itself with a `Last-Event-ID`; the sequence number is already a
   * perfect resume token, so the dashboard can drop its connection and lose nothing.
   */
  async function openStream(
    req: IncomingMessage,
    res: ServerResponse,
    id: WorkflowId,
    url: URL,
  ): Promise<void> {
    const history = await historyOf(id);
    const resumeHeader = req.headers['last-event-id'];
    const resumeFrom =
      typeof resumeHeader === 'string' && /^\d+$/.test(resumeHeader)
        ? Number(resumeHeader)
        : intParam(url.searchParams, 'after', 0);

    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'access-control-allow-origin': '*',
      // Nginx buffers event streams by default, which turns a live view into a view
      // that updates whenever the buffer happens to flush.
      'x-accel-buffering': 'no',
    });

    let cursor: Sequence = resumeFrom;
    let closed = false;

    const write = (type: string, data: unknown, sequence?: Sequence): void => {
      if (closed || res.writableEnded) return;
      const lines = [`event: ${type}`];
      if (sequence !== undefined) lines.push(`id: ${sequence}`);
      lines.push(`data: ${JSON.stringify(data, mapReplacer)}`, '', '');
      res.write(lines.join('\n'));
    };

    const pushSince = async (): Promise<void> => {
      const fresh = await deps.store.read(id, cursor);
      if (fresh.length === 0) return;
      for (const event of fresh) {
        write('event', event, event.sequence);
        cursor = event.sequence;
      }
      write('projection', serializeProjection(deps.project(await deps.store.read(id))), cursor);
    };

    // An initial snapshot so a tab that connects late is not blank until something
    // happens — which, for a stalled workflow, could be never.
    write('snapshot', serializeProjection(deps.project(history)), (history[history.length - 1] as Event).sequence);
    await pushSince();

    let pushing: Promise<void> = Promise.resolve();
    const unsubscribe = deps.changes.subscribe(id, () => {
      // Serialized through one promise chain: two appends landing together must not
      // interleave their reads and emit events out of sequence order.
      pushing = pushing.then(pushSince).catch(() => undefined);
    });

    const heartbeat = setInterval(() => {
      if (!closed && !res.writableEnded) res.write(': keep-alive\n\n');
    }, 15_000);
    // Otherwise the interval keeps the process — and `vitest` — alive after the test.
    heartbeat.unref?.();

    const close = (): void => {
      if (closed) return;
      closed = true;
      clearInterval(heartbeat);
      unsubscribe();
      if (!res.writableEnded) res.end();
    };

    req.on('close', close);
    res.on('close', close);
  }

  async function serveStatic(res: ServerResponse, relative: string): Promise<void> {
    const requested = relative === '' ? 'index.html' : relative;
    const target = resolve(join(DASHBOARD_ROOT, normalize(requested)));
    // `normalize` collapses `..` but does not stop it escaping the root, so the
    // containment check is the actual guard.
    if (!target.startsWith(DASHBOARD_ROOT.endsWith(sep) ? DASHBOARD_ROOT : DASHBOARD_ROOT + sep)) {
      throw ApiError.notFound('not found');
    }
    const type = STATIC_TYPES[extname(target).toLowerCase()];
    if (type === undefined) {
      throw ApiError.notFound(`refusing to serve ${extname(target) || 'extensionless'} files`);
    }
    try {
      const body = await readFile(target);
      res.writeHead(200, { 'content-type': type, 'content-length': body.length });
      res.end(body);
    } catch {
      // A missing bundle is the one 404 here with a specific, likely cause: `dashboard/dist`
      // is a build output and is not in the repository, so a fresh clone that runs the
      // server without building it gets a blank page and a console 404 — which looks like a
      // broken project rather than a missing step. Naming the command turns that into a
      // one-line fix.
      if (requested.startsWith('/dist/')) {
        throw ApiError.notFound(
          `dashboard bundle "${requested}" is missing — run \`npm run build:dashboard\` ` +
            `(the API itself is working; only the UI assets are unbuilt)`,
        );
      }
      throw ApiError.notFound(`no dashboard asset "${requested}"`);
    }
  }
}

/** Builds a listening server with the dashboard mounted. */
export function createApiServer(options: ServerOptions): Server {
  const handle = createHandler(options);
  return createHttpServer((req, res) => {
    void handle(req, res);
  });
}

function fail(res: ServerResponse, cause: unknown): void {
  if (res.headersSent) {
    // Mid-response failure: the status line is already gone, so the only honest thing
    // left is to stop writing rather than append an error body to a partial document.
    res.end();
    return;
  }
  if (cause instanceof ApiError) {
    sendJson(res, cause.status, cause.toBody());
    return;
  }
  if (cause instanceof ConcurrencyError) {
    sendJson(res, 409, {
      error: 'concurrent_modification',
      message: cause.message,
      detail: { expected: cause.expected, actual: cause.actual },
    });
    return;
  }
  sendJson(res, 500, {
    error: 'internal',
    message: cause instanceof Error ? cause.message : 'unhandled error',
  });
}

const ALL_STATES: readonly RunState[] = [
  'pending',
  'ready',
  'running',
  'waiting',
  'retrying',
  'succeeded',
  'failed',
  'cancelled',
];

function isRunState(value: string): value is RunState {
  return (ALL_STATES as readonly string[]).includes(value);
}

/**
 * Checks the submitted definition is shaped like a `WorkflowDef` before handing it to
 * `validateDef`, which assumes the fields exist and would throw on a body that omits
 * `nodes`. A throw would be a 500 for what is plainly a client error.
 */
function parseDefinition(raw: unknown): WorkflowDef {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw ApiError.badRequest('"definition" must be a workflow definition object');
  }
  const def = raw as Record<string, unknown>;
  const problems: string[] = [];

  if (typeof def['name'] !== 'string' || def['name'] === '') problems.push('"name" must be a non-empty string');
  if (typeof def['version'] !== 'number' || !Number.isInteger(def['version'])) {
    problems.push('"version" must be an integer');
  }
  if (typeof def['start'] !== 'string' || def['start'] === '') problems.push('"start" must be a non-empty string');
  if (!Array.isArray(def['nodes']) || def['nodes'].length === 0) {
    problems.push('"nodes" must be a non-empty array');
  } else {
    def['nodes'].forEach((node: unknown, i) => {
      if (typeof node !== 'object' || node === null) {
        problems.push(`nodes[${i}] must be an object`);
        return;
      }
      const n = node as Record<string, unknown>;
      if (typeof n['name'] !== 'string' || n['name'] === '') problems.push(`nodes[${i}].name must be a non-empty string`);
      if (typeof n['kind'] !== 'string') problems.push(`nodes[${i}].kind must be a string`);
      if (n['dependsOn'] !== undefined && !Array.isArray(n['dependsOn'])) {
        problems.push(`nodes[${i}].dependsOn must be an array if present`);
      }
    });
  }
  const timeouts = def['timeouts'];
  if (typeof timeouts !== 'object' || timeouts === null) {
    problems.push('"timeouts" must be an object with attemptTimeoutMs and leaseDurationMs');
  }

  if (problems.length > 0) {
    throw ApiError.badRequest(`"definition" is malformed: ${problems.length} problem(s)`, { problems });
  }

  // `dependsOn` is defaulted rather than required, because omitting it for a root node
  // is the obvious thing for a caller to do and rejecting it teaches nothing.
  const nodes = (def['nodes'] as Record<string, unknown>[]).map((n) => ({
    ...n,
    dependsOn: (n['dependsOn'] as string[] | undefined) ?? [],
  }));

  return { ...def, nodes } as unknown as WorkflowDef;
}

function describeDefError(error: DefError): Record<string, unknown> {
  return {
    message: error.message,
    ...(error.node !== undefined ? { node: error.node } : {}),
  };
}

function findApprovalRequest(
  history: readonly Event[],
  step: string,
): { approvers: readonly string[] } | undefined {
  for (let i = history.length - 1; i >= 0; i -= 1) {
    const event = history[i];
    if (event && event.type === 'approval.requested' && event.step === step) return event;
  }
  return undefined;
}

function mapReplacer(_key: string, value: unknown): unknown {
  return value instanceof Map ? Object.fromEntries(value) : value;
}

/** Projections carry a `Map` of steps; JSON needs an array with stable ordering. */
export function serializeProjection(projection: WorkflowProjection): Record<string, unknown> {
  const { steps, ...rest } = projection;
  return {
    ...rest,
    steps: [...steps.values()].sort((a, b) => a.step.localeCompare(b.step)),
  };
}

/** The list view needs a badge, a name and a timestamp — not every step's output. */
function summarize(
  projection: WorkflowProjection,
  events: readonly Event[],
): Record<string, unknown> {
  // Priority lives on `workflow.created` rather than on the projection, and the list
  // already has the history in hand, so it is read from there rather than re-folded.
  const created = events[0];
  const priority = created?.type === 'workflow.created' ? created.priority : undefined;
  let awaitingApproval = 0;
  let failedSteps = 0;
  for (const step of projection.steps.values()) {
    if (step.awaitingApproval) awaitingApproval += 1;
    if (step.state === 'failed') failedSteps += 1;
  }
  return {
    workflowId: projection.workflowId,
    workflow: projection.workflow,
    definitionVersion: projection.definitionVersion,
    state: projection.state,
    createdAt: projection.createdAt,
    updatedAt: projection.updatedAt,
    sequence: projection.sequence,
    steps: projection.steps.size,
    awaitingApproval,
    failedSteps,
    ...(projection.tenant !== undefined ? { tenant: projection.tenant } : {}),
    ...(priority !== undefined ? { priority } : {}),
    ...(projection.error !== undefined ? { error: projection.error } : {}),
  };
}

function serializeTool(tool: ToolDef): Record<string, unknown> {
  return {
    name: tool.name,
    description: tool.description,
    input: tool.input,
    output: tool.output,
    permissions: tool.permissions,
    timeoutMs: tool.timeoutMs,
    ...(tool.costUnits !== undefined ? { costUnits: tool.costUnits } : {}),
  };
}

function serializeTrace(trace: Trace, cost: WorkflowCost): Record<string, unknown> {
  return { ...trace, cost };
}

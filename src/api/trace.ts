/**
 * The execution trace: the answer to "why is this workflow here".
 *
 * A projection says where a workflow *is*. It cannot say how it got there — it has
 * folded that away, which is the whole point of a projection. The trace is the other
 * half: it walks the same history and keeps the shape of it, grouping events into
 * step attempts, attaching tool calls to the attempt that made them, and attributing
 * elapsed time to the step that spent it.
 *
 * It is built here rather than in the browser for two reasons. The dashboard is not
 * the only consumer — the CLI prints the same structure — and the grouping rules are
 * the part most likely to be subtly wrong, so they belong somewhere testable.
 */

import type { Event, Sequence } from '../events/types.js';
import type {
  NodeKind,
  RunState,
  StepName,
  WorkflowDef,
  WorkflowId,
  WorkflowName,
} from '../workflow/types.js';
import { isTerminal } from '../workflow/types.js';

export interface TraceToolCall {
  readonly tool: string;
  readonly step: StepName;
  readonly attempt: number;
  readonly idempotencyKey: string;
  readonly args: unknown;
  readonly requestedAt: number;
  readonly requestedSequence: Sequence;
  readonly outcome: 'succeeded' | 'failed' | 'pending';
  readonly result?: unknown;
  readonly error?: string;
  readonly errorClass?: string;
  readonly durationMs?: number;
  readonly deduplicated?: boolean;
}

/**
 * One attempt at one step.
 *
 * Attempts are the unit a reader reasons in: "it failed twice on `unavailable` and
 * succeeded on the third try" is a sentence about attempts, and a flat event list
 * makes it work to recover. Lease expiry is kept as a flag on the attempt rather than
 * folded into `error`, because an expired lease is not a failure — the work may still
 * be running elsewhere — and conflating the two is how an operator ends up chasing a
 * failure that never happened.
 */
export interface TraceAttempt {
  readonly step: StepName;
  readonly attempt: number;
  readonly state: RunState;
  readonly workerId?: string;
  readonly leaseExpiresAt?: number;
  readonly scheduledAt?: number;
  readonly leasedAt?: number;
  readonly startedAt?: number;
  readonly endedAt?: number;
  /** From scheduled to started: time the step spent waiting for a worker. */
  readonly queuedMs?: number;
  /** From started to its terminal event: time the step spent actually running. */
  readonly runningMs?: number;
  readonly error?: string;
  readonly errorClass?: string;
  readonly leaseExpired: boolean;
  /**
   * Lease renewals observed for this attempt.
   *
   * Worth a field of its own rather than being folded into the lease expiry time: a
   * step renewed twenty times is slow, a step renewed none and then expired is stuck,
   * and the two are indistinguishable from the expiry alone.
   */
  readonly leaseExtensions: number;
  /** As recorded by the producer on `step.failed`, not re-derived from any policy. */
  readonly willRetry?: boolean;
  readonly retry?: { readonly notBefore: number; readonly backoffMs: number };
  readonly toolCalls: readonly TraceToolCall[];
  readonly sequences: readonly Sequence[];
}

export interface TraceStep {
  readonly step: StepName;
  readonly kind?: NodeKind;
  readonly dependsOn: readonly StepName[];
  /** Longest dependency distance from a root. Drives vertical layout in the DAG. */
  readonly depth: number;
  readonly state: RunState;
  readonly attempts: readonly TraceAttempt[];
  /** Summed running time across attempts, including the ones that failed. */
  readonly totalMs: number;
  readonly firstAt?: number;
  readonly lastAt?: number;
  readonly awaitingApproval: boolean;
  readonly retries: number;
  readonly leaseExpirations: number;
  readonly leaseExtensions: number;
  readonly toolCalls: number;
}

export interface TraceApproval {
  readonly step: StepName;
  readonly approvers: readonly string[];
  readonly prompt: string;
  readonly payload: unknown;
  readonly requestedAt: number;
  readonly requestedSequence: Sequence;
  readonly decision: 'granted' | 'denied' | 'pending';
  readonly approver?: string;
  readonly note?: string;
  readonly reason?: string;
  readonly decidedAt?: number;
  /** How long a human kept the workflow waiting. The metric operators ask for. */
  readonly waitMs?: number;
}

export interface TraceTotals {
  readonly events: number;
  readonly attempts: number;
  readonly retries: number;
  readonly leaseExpirations: number;
  readonly leaseExtensions: number;
  readonly toolCalls: number;
  readonly toolFailures: number;
}

export interface Trace {
  readonly workflowId: WorkflowId;
  readonly workflow: WorkflowName;
  readonly definitionVersion: number;
  readonly tenant?: string;
  readonly priority?: number;
  readonly state: RunState;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly endedAt?: number;
  readonly totalMs?: number;
  readonly input: unknown;
  readonly output?: unknown;
  readonly error?: string;
  readonly sequence: Sequence;
  readonly steps: readonly TraceStep[];
  readonly approvals: readonly TraceApproval[];
  readonly events: readonly Event[];
  readonly totals: TraceTotals;
  readonly failure?: {
    readonly step?: StepName;
    readonly error: string;
    readonly errorClass: string;
    readonly atSequence: Sequence;
  };
  /**
   * The dependency chain that leads to the failing step, or to the furthest-along
   * step if nothing has failed. Highlighted in the DAG because in a wide graph the
   * useful question is not "what failed" — the colour says that — but "which path
   * got us there".
   */
  readonly criticalPath: readonly StepName[];
}

interface AttemptDraft {
  step: StepName;
  attempt: number;
  state: RunState;
  workerId?: string | undefined;
  leaseExpiresAt?: number | undefined;
  scheduledAt?: number | undefined;
  leasedAt?: number | undefined;
  startedAt?: number | undefined;
  endedAt?: number | undefined;
  error?: string | undefined;
  errorClass?: string | undefined;
  leaseExpired: boolean;
  leaseExtensions: number;
  willRetry?: boolean | undefined;
  retry?: { notBefore: number; backoffMs: number } | undefined;
  toolCalls: TraceToolCall[];
  sequences: Sequence[];
}

/** Builds the trace from a complete history and, when known, its definition. */
export function buildTrace(events: readonly Event[], def?: WorkflowDef): Trace {
  let workflowId = '';
  let workflow = '';
  let definitionVersion = 0;
  let tenant: string | undefined;
  let priority: number | undefined;
  let state: RunState = 'pending';
  let createdAt = 0;
  let updatedAt = 0;
  let endedAt: number | undefined;
  let input: unknown;
  let output: unknown;
  let hasOutput = false;
  let error: string | undefined;
  let sequence: Sequence = 0;
  let failure: Trace['failure'];

  const attempts = new Map<string, AttemptDraft>();
  const order: AttemptDraft[] = [];
  const approvals: TraceApproval[] = [];
  const awaiting = new Set<StepName>();
  let retries = 0;
  let leaseExpirations = 0;
  let leaseExtensions = 0;
  let toolCalls = 0;
  let toolFailures = 0;

  const attemptOf = (step: StepName, attempt: number): AttemptDraft => {
    const key = `${step}#${attempt}`;
    const existing = attempts.get(key);
    if (existing) return existing;
    const made: AttemptDraft = {
      step,
      attempt,
      state: 'pending',
      leaseExpired: false,
      leaseExtensions: 0,
      toolCalls: [],
      sequences: [],
    };
    attempts.set(key, made);
    order.push(made);
    return made;
  };

  for (const event of events) {
    workflowId = event.workflowId;
    sequence = event.sequence;
    updatedAt = event.at;

    switch (event.type) {
      case 'workflow.created':
        workflow = event.workflow;
        definitionVersion = event.definitionVersion;
        tenant = event.tenant;
        priority = event.priority;
        input = event.input;
        createdAt = event.at;
        break;

      case 'workflow.completed':
        state = 'succeeded';
        output = event.output;
        hasOutput = true;
        endedAt = event.at;
        break;

      case 'workflow.failed':
        state = 'failed';
        error = event.error;
        endedAt = event.at;
        failure = {
          error: event.error,
          errorClass: event.errorClass,
          atSequence: event.sequence,
          ...(event.step !== undefined ? { step: event.step } : {}),
        };
        break;

      case 'workflow.cancelled':
        state = 'cancelled';
        error = `cancelled: ${event.reason}`;
        endedAt = event.at;
        break;

      case 'step.scheduled': {
        const a = attemptOf(event.step, event.attempt);
        a.state = 'ready';
        a.scheduledAt = event.at;
        a.sequences.push(event.sequence);
        if (state === 'pending') state = 'running';
        break;
      }

      case 'step.leased': {
        const a = attemptOf(event.step, event.attempt);
        a.workerId = event.workerId;
        a.leaseExpiresAt = event.leaseExpiresAt;
        a.leasedAt = event.at;
        a.sequences.push(event.sequence);
        break;
      }

      case 'step.started': {
        const a = attemptOf(event.step, event.attempt);
        a.state = 'running';
        a.startedAt = event.at;
        a.workerId = event.workerId;
        a.sequences.push(event.sequence);
        if (state === 'pending') state = 'running';
        break;
      }

      case 'step.completed': {
        const a = attemptOf(event.step, event.attempt);
        a.state = 'succeeded';
        a.endedAt = event.at;
        a.workerId = event.workerId;
        a.sequences.push(event.sequence);
        break;
      }

      case 'step.failed': {
        const a = attemptOf(event.step, event.attempt);
        // The producer's own verdict. `failed` is terminal, so an attempt that is about
        // to be retried is `retrying` — and taking that from the event rather than
        // re-deriving it from the current retry policy is what keeps a replayed history
        // from changing meaning when the policy is edited.
        a.willRetry = event.willRetry;
        a.state = event.willRetry ? 'retrying' : 'failed';
        a.endedAt = event.at;
        a.error = event.error;
        a.errorClass = event.errorClass;
        if (event.workerId !== undefined) a.workerId = event.workerId;
        a.sequences.push(event.sequence);
        break;
      }

      case 'step.retry_scheduled': {
        retries += 1;
        // The event names the attempt that will run *next*, so the backoff is
        // recorded against the attempt that just failed — that is where a reader
        // looking at a failure wants to see "retrying in 4s".
        const previous = attempts.get(`${event.step}#${event.attempt - 1}`);
        if (previous) {
          previous.retry = { notBefore: event.notBefore, backoffMs: event.backoffMs };
          previous.sequences.push(event.sequence);
        }
        const next = attemptOf(event.step, event.attempt);
        next.state = 'retrying';
        next.sequences.push(event.sequence);
        break;
      }

      case 'step.lease_extended': {
        leaseExtensions += 1;
        const a = attemptOf(event.step, event.attempt);
        a.leaseExtensions += 1;
        // The renewal, not the original grant, is the current expiry.
        a.leaseExpiresAt = event.leaseExpiresAt;
        a.workerId = event.workerId;
        a.sequences.push(event.sequence);
        break;
      }

      case 'step.lease_expired': {
        leaseExpirations += 1;
        const a = attemptOf(event.step, event.attempt);
        a.leaseExpired = true;
        a.endedAt = event.at;
        a.sequences.push(event.sequence);
        break;
      }

      case 'tool.requested': {
        toolCalls += 1;
        const a = attemptOf(event.step, event.attempt);
        a.toolCalls.push({
          tool: event.tool,
          step: event.step,
          attempt: event.attempt,
          idempotencyKey: event.idempotencyKey,
          args: event.args,
          requestedAt: event.at,
          requestedSequence: event.sequence,
          outcome: 'pending',
        });
        a.sequences.push(event.sequence);
        break;
      }

      case 'tool.succeeded': {
        const a = attemptOf(event.step, event.attempt);
        resolveToolCall(a, event.tool, {
          outcome: 'succeeded',
          result: event.result,
          durationMs: event.durationMs,
          deduplicated: event.deduplicated,
        });
        a.sequences.push(event.sequence);
        break;
      }

      case 'tool.failed': {
        toolFailures += 1;
        const a = attemptOf(event.step, event.attempt);
        resolveToolCall(a, event.tool, {
          outcome: 'failed',
          error: event.error,
          errorClass: event.errorClass,
        });
        a.sequences.push(event.sequence);
        break;
      }

      case 'approval.requested': {
        awaiting.add(event.step);
        approvals.push({
          step: event.step,
          approvers: event.approvers,
          prompt: event.prompt,
          payload: event.payload,
          requestedAt: event.at,
          requestedSequence: event.sequence,
          decision: 'pending',
        });
        const a = attemptOf(event.step, 1);
        a.state = 'waiting';
        a.sequences.push(event.sequence);
        break;
      }

      case 'approval.granted': {
        awaiting.delete(event.step);
        decide(approvals, event.step, {
          decision: 'granted',
          approver: event.approver,
          decidedAt: event.at,
          ...(event.note !== undefined ? { note: event.note } : {}),
        });
        const a = attemptOf(event.step, 1);
        a.state = 'ready';
        a.sequences.push(event.sequence);
        break;
      }

      case 'approval.denied': {
        awaiting.delete(event.step);
        decide(approvals, event.step, {
          decision: 'denied',
          approver: event.approver,
          reason: event.reason,
          decidedAt: event.at,
        });
        const a = attemptOf(event.step, 1);
        a.state = 'failed';
        a.error = `denied by ${event.approver}: ${event.reason}`;
        a.errorClass = 'approval_denied';
        a.endedAt = event.at;
        a.sequences.push(event.sequence);
        break;
      }
    }
  }

  const graph = buildGraph(def, order.map((a) => a.step));
  const steps = assembleSteps(order, graph, awaiting, state);

  return {
    workflowId,
    workflow,
    definitionVersion,
    state,
    createdAt,
    updatedAt,
    input,
    sequence,
    steps,
    approvals,
    events,
    totals: {
      events: events.length,
      attempts: order.length,
      retries,
      leaseExpirations,
      leaseExtensions,
      toolCalls,
      toolFailures,
    },
    criticalPath: criticalPath(steps, failure?.step),
    ...(tenant !== undefined ? { tenant } : {}),
    ...(priority !== undefined ? { priority } : {}),
    ...(endedAt !== undefined ? { endedAt, totalMs: endedAt - createdAt } : {}),
    ...(hasOutput ? { output } : {}),
    ...(error !== undefined ? { error } : {}),
    ...(failure !== undefined ? { failure } : {}),
  };
}

type ToolResolution =
  | { outcome: 'succeeded'; result: unknown; durationMs: number; deduplicated: boolean }
  | { outcome: 'failed'; error: string; errorClass: string };

/**
 * Attaches an outcome to the most recent unresolved request for the same tool.
 *
 * Matching on tool name and pending-ness rather than on `causedBy` because a step may
 * call the same tool more than once in one attempt and the events carry no call id;
 * within a single attempt the calls are ordered, so last-pending-wins is the only
 * reading consistent with the log.
 */
function resolveToolCall(a: AttemptDraft, tool: string, outcome: ToolResolution): void {
  for (let i = a.toolCalls.length - 1; i >= 0; i -= 1) {
    const call = a.toolCalls[i];
    if (call && call.tool === tool && call.outcome === 'pending') {
      a.toolCalls[i] = { ...call, ...outcome };
      return;
    }
  }
}

function decide(
  approvals: TraceApproval[],
  step: StepName,
  decision: Omit<TraceApproval, 'step' | 'approvers' | 'prompt' | 'payload' | 'requestedAt' | 'requestedSequence' | 'waitMs'>,
): void {
  for (let i = approvals.length - 1; i >= 0; i -= 1) {
    const approval = approvals[i];
    if (approval && approval.step === step && approval.decision === 'pending') {
      const decidedAt = decision.decidedAt;
      approvals[i] = {
        ...approval,
        ...decision,
        ...(decidedAt !== undefined ? { waitMs: decidedAt - approval.requestedAt } : {}),
      };
      return;
    }
  }
}

interface GraphNode {
  readonly step: StepName;
  readonly kind?: NodeKind;
  readonly dependsOn: readonly StepName[];
  readonly depth: number;
}

/**
 * Resolves each step's dependencies and depth.
 *
 * With a definition this is the real graph. Without one — a history read back from a
 * store whose definition was never recorded — it falls back to a chain in first-seen
 * order, which is wrong for parallel graphs but is at least a layout that renders and
 * is visibly linear, rather than an empty view.
 */
function buildGraph(def: WorkflowDef | undefined, seen: readonly StepName[]): Map<StepName, GraphNode> {
  const nodes = new Map<StepName, GraphNode>();

  if (!def) {
    const unique = [...new Set(seen)];
    unique.forEach((step, i) => {
      const previous = i === 0 ? [] : [unique[i - 1] as StepName];
      nodes.set(step, { step, dependsOn: previous, depth: i });
    });
    return nodes;
  }

  const byName = new Map(def.nodes.map((n) => [n.name, n]));
  const depths = new Map<StepName, number>();
  const depthOf = (name: StepName, guard: ReadonlySet<StepName>): number => {
    const cached = depths.get(name);
    if (cached !== undefined) return cached;
    // `validateDef` rejects cycles before submission, but a trace must not hang on a
    // definition that reached the store some other way.
    if (guard.has(name)) return 0;
    const node = byName.get(name);
    if (!node || node.dependsOn.length === 0) {
      depths.set(name, 0);
      return 0;
    }
    const next = new Set(guard).add(name);
    const depth = 1 + Math.max(...node.dependsOn.map((d) => depthOf(d, next)));
    depths.set(name, depth);
    return depth;
  };

  for (const node of def.nodes) {
    nodes.set(node.name, {
      step: node.name,
      kind: node.kind,
      dependsOn: node.dependsOn,
      depth: depthOf(node.name, new Set()),
    });
  }

  // Steps that ran but are not in the definition (a definition edited under a running
  // execution) still have to appear, or the trace would silently omit real work.
  for (const step of seen) {
    if (!nodes.has(step)) nodes.set(step, { step, dependsOn: [], depth: 0 });
  }
  return nodes;
}

function assembleSteps(
  order: readonly AttemptDraft[],
  graph: ReadonlyMap<StepName, GraphNode>,
  awaiting: ReadonlySet<StepName>,
  workflowState: RunState,
): readonly TraceStep[] {
  const byStep = new Map<StepName, AttemptDraft[]>();
  for (const a of order) {
    const list = byStep.get(a.step) ?? [];
    list.push(a);
    byStep.set(a.step, list);
  }

  const steps: TraceStep[] = [];
  for (const node of graph.values()) {
    const drafts = (byStep.get(node.step) ?? []).slice().sort((x, y) => x.attempt - y.attempt);
    const built = drafts.map(finishAttempt);
    const last = built[built.length - 1];
    const times = built.flatMap((a) => [a.scheduledAt, a.startedAt, a.endedAt]).filter(isNumber);

    const state: RunState =
      last === undefined
        ? // Never scheduled. If the workflow is over, this step will never run; saying
          // `pending` would imply it is still coming.
          isTerminal(workflowState) && workflowState !== 'succeeded'
          ? 'cancelled'
          : 'pending'
        : last.state;

    const firstAt = times.length > 0 ? Math.min(...times) : undefined;
    const lastAt = times.length > 0 ? Math.max(...times) : undefined;

    steps.push({
      step: node.step,
      dependsOn: node.dependsOn,
      depth: node.depth,
      state,
      attempts: built,
      totalMs: built.reduce((sum, a) => sum + (a.runningMs ?? 0), 0),
      awaitingApproval: awaiting.has(node.step),
      retries: Math.max(0, built.length - 1),
      leaseExpirations: built.filter((a) => a.leaseExpired).length,
      leaseExtensions: built.reduce((sum, a) => sum + a.leaseExtensions, 0),
      toolCalls: built.reduce((sum, a) => sum + a.toolCalls.length, 0),
      ...(node.kind !== undefined ? { kind: node.kind } : {}),
      ...(firstAt !== undefined ? { firstAt } : {}),
      ...(lastAt !== undefined ? { lastAt } : {}),
    });
  }

  return steps.sort((a, b) => a.depth - b.depth || a.step.localeCompare(b.step));
}

function finishAttempt(d: AttemptDraft): TraceAttempt {
  const queuedMs =
    d.scheduledAt !== undefined && d.startedAt !== undefined ? d.startedAt - d.scheduledAt : undefined;
  const runningMs =
    d.startedAt !== undefined && d.endedAt !== undefined ? d.endedAt - d.startedAt : undefined;

  return {
    step: d.step,
    attempt: d.attempt,
    state: d.state,
    leaseExpired: d.leaseExpired,
    leaseExtensions: d.leaseExtensions,
    toolCalls: d.toolCalls,
    sequences: d.sequences,
    ...(d.workerId !== undefined ? { workerId: d.workerId } : {}),
    ...(d.leaseExpiresAt !== undefined ? { leaseExpiresAt: d.leaseExpiresAt } : {}),
    ...(d.scheduledAt !== undefined ? { scheduledAt: d.scheduledAt } : {}),
    ...(d.leasedAt !== undefined ? { leasedAt: d.leasedAt } : {}),
    ...(d.startedAt !== undefined ? { startedAt: d.startedAt } : {}),
    ...(d.endedAt !== undefined ? { endedAt: d.endedAt } : {}),
    ...(queuedMs !== undefined ? { queuedMs } : {}),
    ...(runningMs !== undefined ? { runningMs } : {}),
    ...(d.error !== undefined ? { error: d.error } : {}),
    ...(d.errorClass !== undefined ? { errorClass: d.errorClass } : {}),
    ...(d.willRetry !== undefined ? { willRetry: d.willRetry } : {}),
    ...(d.retry !== undefined ? { retry: d.retry } : {}),
  };
}

function isNumber(v: number | undefined): v is number {
  return typeof v === 'number';
}

/**
 * Walks back from the endpoint through the dependency that finished last.
 *
 * The latest-finishing dependency is the one that was actually holding the step up,
 * so it is the edge worth drawing attention to. Ties fall back to declaration order,
 * which is arbitrary but stable — a path that reshuffles between two renders of the
 * same history would be worse than one that is occasionally uninteresting.
 */
function criticalPath(steps: readonly TraceStep[], failed?: StepName): readonly StepName[] {
  const byName = new Map(steps.map((s) => [s.step, s]));
  let endpoint: TraceStep | undefined = failed !== undefined ? byName.get(failed) : undefined;

  if (!endpoint) {
    endpoint = steps
      .filter((s) => s.state === 'failed' || s.lastAt !== undefined)
      .sort((a, b) => (b.lastAt ?? 0) - (a.lastAt ?? 0))[0];
  }
  if (!endpoint) return [];

  const path: StepName[] = [endpoint.step];
  const visited = new Set<StepName>([endpoint.step]);
  let current: TraceStep | undefined = endpoint;

  while (current !== undefined && current.dependsOn.length > 0) {
    const candidates: TraceStep[] = current.dependsOn
      .map((name) => byName.get(name))
      .filter((s): s is TraceStep => s !== undefined && !visited.has(s.step));
    const next: TraceStep | undefined = candidates.sort(
      (a, b) => (b.lastAt ?? 0) - (a.lastAt ?? 0),
    )[0];
    if (!next) break;
    path.unshift(next.step);
    visited.add(next.step);
    current = next;
  }

  return path;
}

/**
 * The workflow model.
 *
 * A workflow here is an explicit graph with typed nodes, not an unconstrained
 * conversation. That is the central design decision and the reason this project
 * exists: an agent loop that decides its own next step cannot be resumed after a
 * crash, because there is no durable record of what it had decided. A graph can be,
 * because the set of possible next steps is knowable before anything runs.
 */

/** Opaque identifier for one workflow definition. */
export type WorkflowName = string;

/** One execution of a workflow. Durable for the life of the execution. */
export type WorkflowId = string;

/**
 * One node's identity within a definition. Stable across executions, so a step's
 * history is comparable between runs of the same workflow.
 */
export type StepName = string;

/**
 * One attempt at one step of one execution. This triple is what idempotency keys
 * are derived from, which is why it is a type rather than three loose strings.
 */
export interface StepRef {
  readonly workflowId: WorkflowId;
  readonly step: StepName;
  readonly attempt: number;
}

/**
 * Renders a StepRef as a unique string.
 *
 * The escaping is not cosmetic. A plain `a/b/c` join is **not injective**: a workflow
 * id of `a/b` with step `c` and a workflow id of `a` with step `b/c` produce identical
 * text, so two unrelated steps would collide — and because this string feeds
 * idempotency keys, a collision means one step silently receives another's recorded
 * tool result. Escaping the separator inside each component makes the rendering
 * reversible and the collision impossible.
 *
 * This is readable enough for logs while still being safe to key on, which is why
 * there is one function rather than a pretty one and a correct one that can drift.
 */
export function stepRefKey(ref: StepRef): string {
  const esc = (s: string) => s.replace(/%/g, '%25').replace(/\//g, '%2F');
  return `${esc(ref.workflowId)}/${esc(ref.step)}/${ref.attempt}`;
}

/**
 * What a node does.
 *
 * `Condition` is deliberately separate from `LlmTask`. A branch decided by opaque
 * model text cannot be replayed deterministically and cannot be explained after the
 * fact, so conditions evaluate a predicate over earlier structured outputs. A model
 * may *produce* the value a condition reads; it may not *be* the condition.
 */
export type NodeKind =
  | 'llm'
  | 'tool'
  | 'python'
  | 'condition'
  | 'parallel'
  | 'join'
  | 'approval'
  | 'finalize';

/** Terminal and non-terminal execution states for a workflow and for a step. */
export type RunState =
  | 'pending'
  | 'ready'
  | 'running'
  | 'waiting'
  | 'retrying'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

/** The states from which nothing further happens. */
export const TERMINAL_STATES: ReadonlySet<RunState> = new Set<RunState>([
  'succeeded',
  'failed',
  'cancelled',
]);

export function isTerminal(state: RunState): boolean {
  return TERMINAL_STATES.has(state);
}

/**
 * Legal transitions. Enforced by the control plane rather than left to callers,
 * because the one invariant that makes replay trustworthy is that a terminal
 * workflow never becomes running again — otherwise a duplicate completion delivered
 * after a cancellation would restart downstream work.
 */
const ALLOWED: Readonly<Record<RunState, readonly RunState[]>> = {
  pending: ['ready', 'cancelled'],
  ready: ['running', 'cancelled', 'failed'],
  // `ready` is reachable from `running` because that is exactly what a lease expiry
  // is: the step is handed back for reassignment without having failed. Routing it
  // through `retrying` instead would conflate "the worker went away" with "the work
  // went wrong", and those have different retry-budget consequences.
  running: ['succeeded', 'failed', 'waiting', 'retrying', 'ready', 'cancelled'],
  waiting: ['ready', 'running', 'cancelled', 'failed'],
  retrying: ['ready', 'running', 'failed', 'cancelled'],
  succeeded: [],
  failed: [],
  cancelled: [],
};

export function canTransition(from: RunState, to: RunState): boolean {
  return (ALLOWED[from] ?? []).includes(to);
}

/** A node in a workflow definition. */
export interface NodeDef {
  readonly name: StepName;
  readonly kind: NodeKind;

  /** Steps that must have succeeded before this one becomes ready. */
  readonly dependsOn: readonly StepName[];

  /** For `tool` nodes: which registered tool to invoke. */
  readonly tool?: string;

  /**
   * For `condition` nodes: a named predicate evaluated over the outputs of
   * completed steps. Named rather than inlined as a function so a definition can be
   * serialized, stored, and replayed — a closure cannot be any of those.
   */
  readonly predicate?: string;

  /** For `condition` nodes: which step to proceed to on true and on false. */
  readonly onTrue?: StepName;
  readonly onFalse?: StepName;

  /** For `approval` nodes: who may approve, and what is being approved. */
  readonly approvers?: readonly string[];
  readonly approvalPrompt?: string;

  readonly retry?: RetryPolicy;

  /**
   * Wall-clock budget for one attempt at this step. Distinct from the workflow
   * deadline and from the lease duration; see the comment on Timeouts.
   */
  readonly attemptTimeoutMs?: number;
}

/**
 * Four different timeouts, deliberately not collapsed into one.
 *
 * They fail differently and the correct response to each differs:
 *
 * - `attemptTimeoutMs` — this attempt took too long. Retryable.
 * - `leaseDurationMs` — the worker stopped heartbeating. The step may still be
 *   running somewhere; it is reassigned, which is why handlers must be idempotent.
 * - `workflowDeadlineMs` — the whole execution is no longer useful. Not retryable,
 *   and remaining steps should not start.
 * - a tool's own `timeoutMs` — the external call hung. Retryable or not depending on
 *   whether the tool is idempotent.
 *
 * Collapsing these into a single "timeout" is how a system ends up retrying work
 * whose deadline has already passed.
 */
export interface Timeouts {
  readonly attemptTimeoutMs: number;
  readonly leaseDurationMs: number;
  readonly workflowDeadlineMs?: number;
}

export interface RetryPolicy {
  readonly maxAttempts: number;
  readonly initialBackoffMs: number;
  readonly maxBackoffMs: number;

  /**
   * Jitter fraction in [0, 1]. Non-zero by default and worth stating why: without
   * jitter, N steps that fail at the same instant retry at the same instant forever,
   * which converts one downstream outage into a synchronized thundering herd.
   */
  readonly jitter: number;

  /**
   * Error classes that are worth retrying. Anything else fails immediately.
   * Explicit rather than "retry unless told otherwise", because the default of
   * retrying an unknown error turns a permanently-malformed request into an
   * infinite loop.
   */
  readonly retryableErrors: readonly string[];
}

export const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 3,
  initialBackoffMs: 500,
  maxBackoffMs: 30_000,
  jitter: 0.2,
  retryableErrors: ['timeout', 'unavailable', 'rate_limited', 'internal'],
};

/** Computes the delay before a given attempt, with full jitter applied. */
export function backoffFor(policy: RetryPolicy, attempt: number, random = Math.random): number {
  const exponent = Math.max(0, attempt - 1);
  const raw = Math.min(policy.initialBackoffMs * 2 ** exponent, policy.maxBackoffMs);
  const spread = raw * policy.jitter;
  return Math.round(raw - spread / 2 + random() * spread);
}

/** A workflow definition: nodes plus the entry point. */
export interface WorkflowDef {
  readonly name: WorkflowName;
  readonly version: number;
  readonly start: StepName;
  readonly nodes: readonly NodeDef[];
  readonly timeouts: Timeouts;

  /**
   * Cap on steps *occupying a worker* at once for one execution, so a fan-out cannot
   * run away.
   *
   * "Occupying a worker" is the precise condition, and it excludes a step in
   * `waiting`. A step blocked on human approval holds no worker — that is the whole
   * point of modelling approval as a state rather than as a blocking call — so
   * counting it against this cap would let one pending approval throttle a workflow's
   * unrelated parallel branches, which is the opposite of what the cap is for.
   */
  readonly maxConcurrency?: number;
}

/** Why a definition was rejected. Validation is up front, not at first execution. */
export interface DefError {
  readonly node?: StepName;
  readonly message: string;
}

/**
 * Validates a definition. Called before an execution is created, because a cycle or
 * a dangling dependency discovered mid-run leaves a workflow that can neither
 * proceed nor be meaningfully failed.
 */
export function validateDef(def: WorkflowDef): DefError[] {
  const errors: DefError[] = [];
  const byName = new Map<StepName, NodeDef>();

  for (const node of def.nodes) {
    if (byName.has(node.name)) {
      errors.push({ node: node.name, message: 'duplicate node name' });
    }
    byName.set(node.name, node);
  }

  if (!byName.has(def.start)) {
    errors.push({ message: `start step "${def.start}" is not a node` });
  }

  for (const node of def.nodes) {
    for (const dep of node.dependsOn) {
      if (!byName.has(dep)) {
        errors.push({ node: node.name, message: `depends on unknown step "${dep}"` });
      }
    }
    if (node.kind === 'tool' && !node.tool) {
      errors.push({ node: node.name, message: 'tool node has no tool name' });
    }
    if (node.kind === 'condition') {
      if (!node.predicate) {
        errors.push({ node: node.name, message: 'condition node has no predicate' });
      }
      for (const target of [node.onTrue, node.onFalse]) {
        if (target !== undefined && !byName.has(target)) {
          errors.push({ node: node.name, message: `branches to unknown step "${target}"` });
        }
      }
    }
    if (node.kind === 'approval' && (!node.approvers || node.approvers.length === 0)) {
      errors.push({ node: node.name, message: 'approval node has no approvers' });
    }
  }

  errors.push(...findCycles(def, byName));
  return errors;
}

/**
 * Reports a dependency cycle if one exists.
 *
 * Reported as a cycle with its path rather than as "graph is invalid", because the
 * path is the only part a reader can act on.
 */
function findCycles(def: WorkflowDef, byName: Map<StepName, NodeDef>): DefError[] {
  const WHITE = 0, GREY = 1, BLACK = 2;
  const colour = new Map<StepName, number>();
  const stack: StepName[] = [];
  const errors: DefError[] = [];

  const visit = (name: StepName): boolean => {
    const node = byName.get(name);
    if (!node) return false;
    colour.set(name, GREY);
    stack.push(name);

    for (const dep of node.dependsOn) {
      const c = colour.get(dep) ?? WHITE;
      if (c === GREY) {
        const from = stack.indexOf(dep);
        const path = [...stack.slice(from), dep].join(' -> ');
        errors.push({ node: name, message: `dependency cycle: ${path}` });
        return true;
      }
      if (c === WHITE && visit(dep)) return true;
    }

    stack.pop();
    colour.set(name, BLACK);
    return false;
  };

  for (const node of def.nodes) {
    if ((colour.get(node.name) ?? WHITE) === WHITE) {
      if (visit(node.name)) break;
    }
  }
  return errors;
}

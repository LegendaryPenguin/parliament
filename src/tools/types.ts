/**
 * The tool registry and the execution boundary.
 *
 * Every externally visible action a workflow takes goes through here. That is the
 * point: if an agent can call an arbitrary function, then "what can this workflow
 * do" has no answer, and the permission model is a suggestion. Routing everything
 * through one executor makes the set of possible side effects enumerable.
 */

import type { StepRef } from '../workflow/types.js';

/**
 * A minimal JSON Schema subset, hand-defined rather than imported.
 *
 * Narrow on purpose. Arguments arrive from a language model, so they need validating
 * before a tool sees them, but supporting the whole of JSON Schema would mean
 * shipping a validator whose behaviour I could not predict. This covers what the
 * tools here actually declare, and rejects anything it does not understand rather
 * than passing it through.
 */
export type SchemaType = 'string' | 'number' | 'integer' | 'boolean' | 'object' | 'array';

export interface Schema {
  readonly type: SchemaType;
  readonly description?: string;
  readonly enum?: readonly (string | number)[];
  readonly minimum?: number;
  readonly maximum?: number;
  readonly minLength?: number;
  readonly maxLength?: number;
  readonly pattern?: string;
  readonly items?: Schema;
  readonly properties?: Readonly<Record<string, Schema>>;
  readonly required?: readonly string[];
  /** Unknown keys are rejected by default; a tool must opt in to accepting them. */
  readonly additionalProperties?: boolean;
}

/** A validation failure, with the path to the offending value. */
export interface SchemaError {
  readonly path: string;
  readonly message: string;
}

/**
 * What a tool is permitted to do, declared rather than inferred.
 *
 * `effect` is the field that matters. A tool that only reads can be retried freely;
 * a tool that writes cannot, unless it is idempotent. Making the caller declare this
 * means the retry logic does not have to guess, and guessing wrong here means either
 * a duplicated side effect or a needlessly failed workflow.
 */
export type ToolEffect = 'read' | 'write' | 'destructive';

export interface ToolPermissions {
  readonly effect: ToolEffect;

  /**
   * Roles allowed to invoke it. A research step and a step that can spend money
   * should not have the same capability set, and the only way to enforce that is to
   * attach capabilities to the step rather than to the workflow.
   */
  readonly allowedRoles: readonly string[];

  /**
   * Whether a human must approve each invocation. Orthogonal to `effect`: a cheap
   * write may need no approval and an expensive read may.
   */
  readonly requiresApproval: boolean;

  /**
   * Whether repeating the call with the same idempotency key is safe. A tool that
   * declares itself idempotent is promising the executor it may retry; one that does
   * not will only ever be attempted once per step attempt.
   */
  readonly idempotent: boolean;
}

export interface ToolDef {
  readonly name: string;
  readonly description: string;
  readonly input: Schema;
  readonly output: Schema;
  readonly permissions: ToolPermissions;

  /** Per-call budget. Distinct from the step's attempt timeout; see Timeouts. */
  readonly timeoutMs: number;

  /** Cost in whatever unit the tenant is billed in, for the accounting layer. */
  readonly costUnits?: number;
}

/** Everything a tool implementation is given. */
export interface ToolContext {
  readonly ref: StepRef;
  readonly idempotencyKey: string;
  readonly role: string;
  readonly tenant?: string;

  /**
   * Aborted when the step's deadline passes or the workflow is cancelled. A tool
   * that ignores it will be abandoned rather than stopped, which leaks whatever it
   * was holding — so honouring it is part of the contract, not an optimization.
   */
  readonly signal: AbortSignal;

  /** Structured log lines, captured into the execution trace. */
  log(message: string, fields?: Record<string, unknown>): void;
}

export type ToolHandler = (args: unknown, ctx: ToolContext) => Promise<unknown>;

/**
 * Why a tool call failed. The class drives the retry decision, so it is a closed set
 * rather than free text.
 */
export type ToolErrorClass =
  | 'invalid_args'
  | 'not_found'
  | 'permission_denied'
  | 'timeout'
  /**
   * The workflow was cancelled, or the step's context was aborted, before or during
   * the call. Distinct from `timeout` because nothing went wrong and nothing should
   * be retried — reporting it as a timeout invites exactly the retry that
   * cancellation was asking not to happen.
   */
  | 'cancelled'
  | 'unavailable'
  | 'rate_limited'
  | 'invalid_output'
  | 'internal';

export class ToolError extends Error {
  constructor(
    readonly errorClass: ToolErrorClass,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
    this.name = 'ToolError';
  }

  /** Argument validation failed. Never retryable: the arguments will not improve. */
  static invalidArgs(errors: readonly SchemaError[]): ToolError {
    const detail = errors.map((e) => `${e.path}: ${e.message}`).join('; ');
    return new ToolError('invalid_args', `arguments rejected: ${detail}`, false);
  }

  /**
   * The tool returned something its own schema forbids. Not retryable, and it is a
   * bug in the tool rather than in the caller — which is why it is a distinct class
   * instead of being folded into `internal`.
   */
  static invalidOutput(errors: readonly SchemaError[]): ToolError {
    const detail = errors.map((e) => `${e.path}: ${e.message}`).join('; ');
    return new ToolError('invalid_output', `tool returned invalid output: ${detail}`, false);
  }

  static permissionDenied(message: string): ToolError {
    return new ToolError('permission_denied', message, false);
  }

  static timeout(tool: string, ms: number): ToolError {
    return new ToolError('timeout', `${tool} exceeded ${ms}ms`, true);
  }

  /** Never retryable: the caller asked for this to stop. */
  static cancelled(tool: string): ToolError {
    return new ToolError('cancelled', `${tool} was cancelled before completing`, false);
  }
}

/** The outcome of an invocation, as the executor reports it. */
export interface ToolOutcome {
  readonly ok: boolean;
  readonly result?: unknown;
  readonly error?: ToolError;
  readonly durationMs: number;

  /**
   * True when a previously recorded result was returned for a repeated idempotency
   * key, rather than the tool being called again. Surfaced because a system that
   * silently dedupes is indistinguishable from one that silently drops.
   */
  readonly deduplicated: boolean;
}

/**
 * The tool executor.
 *
 * Deliberately the only path to a side effect. It validates arguments against the
 * declared schema before the handler runs, checks the caller's role against the
 * tool's permissions, enforces the timeout, validates the output, and records the
 * result against the idempotency key so a redelivery returns the first answer rather
 * than producing a second effect.
 */
export interface ToolExecutor {
  register(def: ToolDef, handler: ToolHandler): void;
  get(name: string): ToolDef | undefined;
  list(): readonly ToolDef[];

  /**
   * Invokes a tool. Never throws for an expected failure — the outcome carries it —
   * because a workflow step needs to record the failure in its history, and an
   * exception propagating past the executor would skip that.
   */
  invoke(name: string, args: unknown, ctx: ToolContext): Promise<ToolOutcome>;
}

/**
 * Validates a value against a schema.
 *
 * Returns every error rather than the first, because arguments produced by a model
 * are often wrong in several places at once and reporting one at a time turns
 * correction into a slow loop.
 */
export function validate(value: unknown, schema: Schema, path = '$'): SchemaError[] {
  const errors: SchemaError[] = [];

  const typeOf = (v: unknown): SchemaType | 'null' | 'unknown' => {
    if (v === null) return 'null';
    if (Array.isArray(v)) return 'array';
    const t = typeof v;
    if (t === 'string' || t === 'boolean' || t === 'object') return t as SchemaType;
    if (t === 'number') return Number.isInteger(v) ? 'integer' : 'number';
    return 'unknown';
  };

  const actual = typeOf(value);
  const matches =
    actual === schema.type ||
    // An integer satisfies a `number` schema; the reverse is not true.
    (schema.type === 'number' && actual === 'integer');

  if (!matches) {
    errors.push({ path, message: `expected ${schema.type}, got ${actual}` });
    return errors;
  }

  if (schema.enum && !schema.enum.includes(value as string | number)) {
    errors.push({ path, message: `must be one of ${schema.enum.join(', ')}` });
  }

  if (typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) {
      errors.push({ path, message: `must be >= ${schema.minimum}` });
    }
    if (schema.maximum !== undefined && value > schema.maximum) {
      errors.push({ path, message: `must be <= ${schema.maximum}` });
    }
  }

  if (typeof value === 'string') {
    if (schema.minLength !== undefined && value.length < schema.minLength) {
      errors.push({ path, message: `must be at least ${schema.minLength} characters` });
    }
    if (schema.maxLength !== undefined && value.length > schema.maxLength) {
      errors.push({ path, message: `must be at most ${schema.maxLength} characters` });
    }
    if (schema.pattern !== undefined && !new RegExp(schema.pattern).test(value)) {
      errors.push({ path, message: `must match ${schema.pattern}` });
    }
  }

  if (Array.isArray(value) && schema.items) {
    value.forEach((item, i) => {
      errors.push(...validate(item, schema.items as Schema, `${path}[${i}]`));
    });
  }

  if (actual === 'object' && schema.type === 'object') {
    const obj = value as Record<string, unknown>;
    for (const key of schema.required ?? []) {
      if (!(key in obj)) {
        errors.push({ path: `${path}.${key}`, message: 'required' });
      }
    }
    for (const [key, child] of Object.entries(obj)) {
      const childSchema = schema.properties?.[key];
      if (childSchema) {
        errors.push(...validate(child, childSchema, `${path}.${key}`));
      } else if (schema.additionalProperties !== true) {
        // Rejecting unknown keys is the safer default here: a model that invents an
        // argument name has misunderstood the tool, and silently dropping the key
        // would run the tool with the caller believing it was passed.
        errors.push({ path: `${path}.${key}`, message: 'unknown property' });
      }
    }
  }

  return errors;
}

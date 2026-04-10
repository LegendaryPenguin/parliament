/**
 * Idempotency keys and the result store.
 *
 * ## What this buys, and what it does not
 *
 * This layer makes *this system's* retries safe. If a worker loses its lease
 * mid-step, or a queue redelivers a message, or the scheduler re-runs an attempt it
 * already dispatched, the executor finds the recorded outcome under the same key and
 * returns it instead of calling the handler a second time. That is a real and useful
 * property, and it is the only one being claimed.
 *
 * It does **not** make the external service exactly-once. If the first call left this
 * process, reached the service, and the response was lost on the way back, then the
 * side effect has already happened and nothing recorded here can undo it or confirm
 * it. In that case the executor records a failure (it never saw a response), a retry
 * runs the handler again, and the remote effect happens twice. Conversely, if a
 * handler succeeded remotely but the process died before `record` returned, the store
 * has no entry and the retry duplicates the effect. The recorded result is a record
 * of what this process observed, not a proof of what the remote system did.
 *
 * Exactly-once against a remote service requires the *service* to deduplicate — it
 * has to accept a client-supplied token and collapse repeats on its side. That is why
 * `ToolContext.idempotencyKey` is handed to the tool rather than kept private: a tool
 * calling an API that supports an idempotency token should forward this key, and only
 * then does end-to-end deduplication exist. For tools that cannot, the honest
 * statement is at-least-once delivery with at-most-once *local* re-invocation.
 */

import { createHash } from 'node:crypto';
import type { StepRef } from '../workflow/types.js';
import type { ToolOutcome } from './types.js';

/**
 * Bumped if the canonical form ever changes. Keys from an older version must not
 * collide with keys from a newer one: a stored outcome would then be returned for a
 * call whose arguments were canonicalized under different rules, which is a silent
 * wrong answer rather than a loud failure.
 */
const KEY_VERSION = 'v1';

/**
 * Rewrites a value into a form whose JSON serialization depends only on its content.
 *
 * Two rules, both of which have a wrong alternative:
 *
 * - **Object keys are sorted.** `{a:1,b:2}` and `{b:2,a:1}` are the same call. Without
 *   sorting, a retry that happened to build the argument object in a different order
 *   gets a different key and repeats the effect, which is precisely the failure this
 *   module exists to prevent.
 *
 * - **Properties whose value is `undefined` are dropped**, making them identical to
 *   absent properties. This matches the only observable behaviour available to a
 *   handler receiving JSON-decoded arguments: `JSON.stringify` already erases such
 *   properties, so `{a:1,b:undefined}` and `{a:1}` cannot be distinguished by anything
 *   downstream, and treating them as different keys would mean two keys for one call.
 *   `undefined` appearing as an *array element* is not dropped — removing it would
 *   shift every later index — and is encoded as `null`, which is what JSON does to it.
 *
 * Non-JSON values are rejected rather than coerced. A `bigint`, a function, or a
 * circular reference cannot appear in a stable serialization, and hashing some
 * best-effort stand-in would produce a key that two different calls could share.
 */
export function canonicalize(value: unknown, path = '$', seen = new Set<object>()): unknown {
  if (value === null) return null;

  const t = typeof value;
  if (t === 'string' || t === 'boolean') return value;

  if (t === 'number') {
    const n = value as number;
    // NaN and the infinities serialize to `null`, which would make them collide with
    // each other and with an explicit null.
    if (!Number.isFinite(n)) {
      throw new TypeError(`cannot canonicalize non-finite number at ${path}`);
    }
    return n;
  }

  if (t === 'bigint' || t === 'function' || t === 'symbol' || t === 'undefined') {
    throw new TypeError(`cannot canonicalize ${t} at ${path}`);
  }

  const obj = value as object;
  if (seen.has(obj)) {
    throw new TypeError(`cannot canonicalize circular reference at ${path}`);
  }
  seen.add(obj);
  try {
    if (Array.isArray(obj)) {
      return obj.map((item, i) =>
        item === undefined ? null : canonicalize(item, `${path}[${i}]`, seen),
      );
    }

    // `toJSON` is honoured so a Date hashes by its instant rather than by its (empty)
    // own-property set, which would make every Date canonicalize identically.
    const maybe = obj as { toJSON?: () => unknown };
    if (typeof maybe.toJSON === 'function') {
      return canonicalize(maybe.toJSON(), path, seen);
    }

    const out: Record<string, unknown> = {};
    for (const key of Object.keys(obj as Record<string, unknown>).sort()) {
      const child = (obj as Record<string, unknown>)[key];
      if (child === undefined) continue;
      out[key] = canonicalize(child, `${path}.${key}`, seen);
    }
    return out;
  } finally {
    seen.delete(obj);
  }
}

/** The canonical JSON text that a key is computed over. Exported for tests and logs. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalize(value)) ?? 'null';
}

/**
 * Derives the idempotency key for one tool call.
 *
 * Over the step *attempt*, the tool name, and the canonicalized arguments. Including
 * the attempt is deliberate: a genuine retry — a new attempt, scheduled because the
 * previous one failed — must be able to actually run. If the key covered only
 * `workflowId/step`, the second attempt would find the first attempt's recorded
 * failure and return it forever, and the retry policy would be decorative. What the
 * key does suppress is the *same* attempt arriving twice, which is the redelivery
 * case and the one that duplicates effects.
 *
 * The step's fields are hashed individually, joined with a NUL byte, rather than being
 * run through `stepRefKey`. Both encodings are injective — `stepRefKey` percent-escapes
 * `/` within each component — so this is a second safe encoding rather than a
 * workaround for an unsafe one. It is kept because the property this hash depends on is
 * then local: NUL cannot occur in a workflow id, a step name, or a decimal integer, so
 * the concatenation has exactly one reading no matter what `stepRefKey` does later. A
 * change to the display encoding must not be able to silently merge two steps'
 * deduplication slots, and the only way to guarantee that is not to depend on it.
 */
export function idempotencyKeyFor(ref: StepRef, tool: string, args: unknown): string {
  const parts = [
    KEY_VERSION,
    ref.workflowId,
    ref.step,
    String(ref.attempt),
    tool,
    canonicalJson(args),
  ];
  return createHash('sha256').update(parts.join('\u0000')).digest('hex');
}

/** An outcome as it was recorded, with the time it was first produced. */
export interface RecordedOutcome {
  readonly outcome: ToolOutcome;
  readonly recordedAt: number;
}

/**
 * Where completed outcomes live.
 *
 * Synchronous on purpose for the in-memory implementation, but the interface returns
 * promises so a durable store can replace it without touching the executor — which is
 * the only configuration that survives a process restart, and therefore the only one
 * where the deduplication claim holds across a crash.
 */
export interface ResultStore {
  get(key: string): Promise<RecordedOutcome | undefined>;

  /**
   * Records an outcome. Implementations must not overwrite an existing entry: the
   * first observation is the one the workflow may already have acted on, and
   * replacing it would mean two steps reading different results for one call.
   */
  record(key: string, outcome: ToolOutcome, now?: number): Promise<void>;

  size(): number;
}

/**
 * In-memory store, with an entry cap.
 *
 * Failures are recorded as well as successes. A redelivered attempt that failed the
 * first time must see the same failure, otherwise the retry accounting differs between
 * the original delivery and the duplicate and the step can exceed its attempt budget.
 *
 * The cap evicts oldest-first. Eviction is a correctness loss, not just a capacity
 * one — an evicted key is one whose redelivery will re-run the handler — so it is
 * counted rather than hidden, and a deployment that cares should use a durable store
 * sized to its retention window instead of raising this number.
 */
export class InMemoryResultStore implements ResultStore {
  private readonly entries = new Map<string, RecordedOutcome>();
  private evictions = 0;

  constructor(private readonly maxEntries = 10_000) {}

  async get(key: string): Promise<RecordedOutcome | undefined> {
    return this.entries.get(key);
  }

  async record(key: string, outcome: ToolOutcome, now = Date.now()): Promise<void> {
    if (this.entries.has(key)) return;

    if (this.entries.size >= this.maxEntries) {
      const oldest = this.entries.keys().next();
      if (!oldest.done) {
        this.entries.delete(oldest.value);
        this.evictions += 1;
      }
    }
    this.entries.set(key, { outcome, recordedAt: now });
  }

  size(): number {
    return this.entries.size;
  }

  /** How many recorded outcomes were dropped, and so can no longer deduplicate. */
  evictedCount(): number {
    return this.evictions;
  }
}

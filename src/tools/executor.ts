/**
 * The one path to a side effect.
 *
 * ## Order of operations
 *
 * permission check -> argument validation -> idempotency lookup -> invoke with
 * timeout -> output validation -> record.
 *
 * Each position is load-bearing:
 *
 * - **Permissions first.** Checking idempotency before permissions would let a denied
 *   caller read a cached result that someone else was authorized to produce, which
 *   turns the result store into a confused-deputy read channel.
 * - **Validation before invocation.** Validating afterwards means the bad call already
 *   happened; the only thing left to do is report a side effect that should never have
 *   occurred.
 * - **Idempotency lookup after validation.** An invalid call has no legitimate
 *   recorded outcome to return, and hashing arguments that were about to be rejected
 *   would pollute the key space with keys nothing will ever match.
 * - **Output validation before recording.** A result that violates the tool's own
 *   schema must not be stored, because a stored result is replayed verbatim on
 *   redelivery and would then propagate into workflow state on every retry.
 *
 * ## Failures are returned, not thrown
 *
 * `invoke` resolves with a `ToolOutcome` for every expected failure. The step that
 * called it has to write the failure into the event history before it can decide to
 * retry; an exception escaping here would unwind past that write, and the history
 * would show a step that started and then nothing.
 */

import type {
  ToolContext,
  ToolDef,
  ToolExecutor,
  ToolHandler,
  ToolOutcome,
} from './types.js';
import { ToolError, validate } from './types.js';
import type { ResultStore } from './idempotency.js';
import { InMemoryResultStore, idempotencyKeyFor } from './idempotency.js';
import type { PermissionPolicy } from '../policy/permissions.js';
import { AllowAllPolicy, denialToError } from '../policy/permissions.js';

/** One captured log line from a tool. */
export interface LogLine {
  readonly at: number;
  readonly message: string;
  readonly fields?: Record<string, unknown>;
}

/**
 * The outcome plus the trace.
 *
 * `ToolOutcome` is frozen and has no field for logs, so the trace is carried by an
 * extension of it: callers typed against `ToolOutcome` are unaffected, and callers
 * that want the trace can narrow. Logs live on the outcome rather than being written
 * straight to a logger because they have to land in the same event the step records,
 * or a failure and its explanation end up in two different places.
 */
export interface TracedToolOutcome extends ToolOutcome {
  readonly logs: readonly LogLine[];
}

export interface ExecutorOptions {
  readonly policy?: PermissionPolicy;
  readonly store?: ResultStore;

  /** Injected so tests can control durations without sleeping. */
  readonly now?: () => number;

  /** Cap on captured log lines per call, so a chatty tool cannot grow an event. */
  readonly maxLogLines?: number;
}

/** Per-tenant accounting, in the tool's declared cost units. */
export interface CostLedger {
  readonly tenant: string;
  readonly units: number;
  readonly calls: number;
}

export interface ExecutorStats {
  readonly invocations: number;
  readonly deduplicated: number;
  readonly denied: number;
  readonly timedOut: number;
  readonly cancelled: number;
  readonly invalidArgs: number;
  readonly invalidOutput: number;
}

interface Registration {
  readonly def: ToolDef;
  readonly handler: ToolHandler;
}

export class DefaultToolExecutor implements ToolExecutor {
  private readonly tools = new Map<string, Registration>();
  private readonly policy: PermissionPolicy;
  private readonly store: ResultStore;
  private readonly now: () => number;
  private readonly maxLogLines: number;

  private readonly costs = new Map<string, { units: number; calls: number }>();

  /**
   * Calls in flight, by idempotency key.
   *
   * Two deliveries of the same attempt arriving concurrently would both miss the store
   * — nothing is recorded until the first one finishes — and both would call the
   * handler. Sharing the in-flight promise closes that window *within this process*.
   * Two workers on two hosts are not covered: only a store with a conditional write
   * can serialize those, which is a property of the store implementation and not
   * something this class can provide.
   */
  private readonly inFlight = new Map<string, Promise<TracedToolOutcome>>();

  private stats = {
    invocations: 0,
    deduplicated: 0,
    denied: 0,
    timedOut: 0,
    cancelled: 0,
    invalidArgs: 0,
    invalidOutput: 0,
  };

  constructor(options: ExecutorOptions = {}) {
    this.policy = options.policy ?? new AllowAllPolicy();
    this.store = options.store ?? new InMemoryResultStore();
    this.now = options.now ?? Date.now;
    this.maxLogLines = options.maxLogLines ?? 200;
  }

  register(def: ToolDef, handler: ToolHandler): void {
    if (this.tools.has(def.name)) {
      // Re-registration is a programming error, not a configuration choice: whichever
      // handler won would be decided by module load order, and the permissions
      // attached to the losing definition would silently stop applying.
      throw new Error(`tool "${def.name}" is already registered`);
    }
    this.tools.set(def.name, { def, handler });
  }

  get(name: string): ToolDef | undefined {
    return this.tools.get(name)?.def;
  }

  list(): readonly ToolDef[] {
    return [...this.tools.values()].map((r) => r.def);
  }

  statistics(): ExecutorStats {
    return { ...this.stats };
  }

  /** Cost accrued by one tenant, or zero if it has not called anything. */
  costFor(tenant: string): CostLedger {
    const entry = this.costs.get(tenant);
    return { tenant, units: entry?.units ?? 0, calls: entry?.calls ?? 0 };
  }

  allCosts(): readonly CostLedger[] {
    return [...this.costs.entries()].map(([tenant, v]) => ({ tenant, ...v }));
  }

  async invoke(name: string, args: unknown, ctx: ToolContext): Promise<TracedToolOutcome> {
    const started = this.now();
    try {
      return await this.run(name, args, ctx, started);
    } catch (err) {
      // Anything reaching here is a defect in this class or in the store, not an
      // expected tool failure. It still has to come back as an outcome, because the
      // caller's contract says exceptions do not escape.
      return this.failure(
        new ToolError('internal', `executor fault invoking "${name}": ${describe(err)}`, false),
        started,
        [],
      );
    }
  }

  private async run(
    name: string,
    args: unknown,
    ctx: ToolContext,
    started: number,
  ): Promise<TracedToolOutcome> {
    const registration = this.tools.get(name);
    if (!registration) {
      return this.failure(
        new ToolError('not_found', `no tool registered as "${name}"`, false),
        started,
        [],
      );
    }
    const { def, handler } = registration;

    const denial = this.policy.check({ role: ctx.role, def, ref: ctx.ref });
    if (denial) {
      this.stats.denied += 1;
      return this.failure(denialToError(denial), started, []);
    }

    const argErrors = validate(args, def.input);
    if (argErrors.length > 0) {
      this.stats.invalidArgs += 1;
      return this.failure(ToolError.invalidArgs(argErrors), started, []);
    }

    // The caller normally supplies the key so the same key can be used in its own
    // bookkeeping; deriving it here when absent keeps a caller that forgot from
    // silently losing deduplication.
    const key = ctx.idempotencyKey || idempotencyKeyFor(ctx.ref, def.name, args);

    const recorded = await this.store.get(key);
    if (recorded) {
      this.stats.deduplicated += 1;
      return {
        ...recorded.outcome,
        durationMs: this.now() - started,
        deduplicated: true,
        logs: logsOf(recorded.outcome),
      };
    }

    const existing = this.inFlight.get(key);
    if (existing) {
      this.stats.deduplicated += 1;
      const outcome = await existing;
      return { ...outcome, durationMs: this.now() - started, deduplicated: true };
    }

    const pending = this.invokeHandler(def, handler, args, ctx, key, started);
    this.inFlight.set(key, pending);
    try {
      return await pending;
    } finally {
      this.inFlight.delete(key);
    }
  }

  private async invokeHandler(
    def: ToolDef,
    handler: ToolHandler,
    args: unknown,
    ctx: ToolContext,
    key: string,
    started: number,
  ): Promise<TracedToolOutcome> {
    const logs: LogLine[] = [];
    let dropped = 0;
    const log = (message: string, fields?: Record<string, unknown>): void => {
      if (logs.length >= this.maxLogLines) {
        dropped += 1;
        return;
      }
      logs.push({ at: this.now(), message, ...(fields ? { fields } : {}) });
    };

    if (ctx.signal.aborted) {
      // Nothing is recorded: no call was made, so there is no observation to replay,
      // and a later attempt should be free to run.
      this.stats.cancelled += 1;
      return this.failure(ToolError.cancelled(def.name), started, logs);
    }

    /**
     * Why the signal was aborted, which the abort listener cannot otherwise tell.
     *
     * Both the deadline and the caller abort the same controller, and a tool must not
     * be able to distinguish them — it should just stop. The executor does have to
     * distinguish them, because one is a retryable failure and the other is a refusal
     * to keep going.
     */
    let abortReason: 'timeout' | 'cancelled' | undefined;

    const controller = new AbortController();
    const timer = setTimeout(() => {
      abortReason ??= 'timeout';
      controller.abort(new Error(`timeout after ${def.timeoutMs}ms`));
    }, def.timeoutMs);
    // Node keeps the event loop alive for pending timers; a tool that returns promptly
    // should not hold the process open for the rest of its timeout budget.
    timer.unref?.();

    const onCallerAbort = (): void => {
      abortReason ??= 'cancelled';
      controller.abort(ctx.signal.reason);
    };
    ctx.signal.addEventListener('abort', onCallerAbort, { once: true });

    const childCtx: ToolContext = {
      ref: ctx.ref,
      idempotencyKey: key,
      role: ctx.role,
      ...(ctx.tenant !== undefined ? { tenant: ctx.tenant } : {}),
      signal: controller.signal,
      log,
    };

    this.stats.invocations += 1;
    this.charge(def, ctx.tenant);

    let outcome: TracedToolOutcome;
    try {
      const result = await this.withTimeout(handler(args, childCtx), controller.signal, () =>
        abortReason === 'cancelled'
          ? ToolError.cancelled(def.name)
          : ToolError.timeout(def.name, def.timeoutMs),
      );

      const outErrors = validate(result, def.output);
      if (outErrors.length > 0) {
        this.stats.invalidOutput += 1;
        outcome = this.failure(ToolError.invalidOutput(outErrors), started, logs);
      } else {
        outcome = {
          ok: true,
          result,
          durationMs: this.now() - started,
          deduplicated: false,
          logs,
        };
      }
    } catch (err) {
      const error = toToolError(err, def, abortReason);
      if (error.errorClass === 'timeout') this.stats.timedOut += 1;
      if (error.errorClass === 'cancelled') this.stats.cancelled += 1;
      outcome = this.failure(error, started, logs);
    } finally {
      clearTimeout(timer);
      ctx.signal.removeEventListener('abort', onCallerAbort);
      if (dropped > 0) {
        logs.push({ at: this.now(), message: `${dropped} further log lines dropped` });
      }
    }

    // Recorded even when it failed: a redelivery of this same attempt must see the
    // same answer, or the two deliveries disagree about how many attempts were used.
    await this.store.record(key, stripLogs(outcome), this.now());
    return outcome;
  }

  /**
   * Races the handler against its deadline.
   *
   * The signal is aborted first, giving a cooperating tool the chance to stop and
   * release what it holds. A tool that ignores the signal is not waited for: the
   * outcome is decided either way, because the step's budget has been spent or the
   * work has been cancelled whether or not the tool agrees. Its promise is left with a
   * catch attached so that settling later — possibly with a rejection — does not
   * surface as an unhandled rejection in a process that has already moved on.
   *
   * `abortError` is supplied by the caller rather than built here, because only the
   * caller knows whether the abort came from the deadline or from a cancellation.
   */
  private withTimeout(
    work: Promise<unknown>,
    signal: AbortSignal,
    abortError: () => ToolError,
  ): Promise<unknown> {
    void work.catch(() => undefined);

    return new Promise<unknown>((resolve, reject) => {
      let settled = false;
      const onAbort = (): void => {
        if (settled) return;
        settled = true;
        reject(abortError());
      };

      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener('abort', onAbort, { once: true });

      work.then(
        (value) => {
          if (settled) return;
          settled = true;
          signal.removeEventListener('abort', onAbort);
          resolve(value);
        },
        (err) => {
          if (settled) return;
          settled = true;
          signal.removeEventListener('abort', onAbort);
          reject(err);
        },
      );
    });
  }

  /** Cost is charged when the handler actually ran, since that is when money moves. */
  private charge(def: ToolDef, tenant: string | undefined): void {
    const units = def.costUnits ?? 0;
    const who = tenant ?? '<untenanted>';
    const entry = this.costs.get(who) ?? { units: 0, calls: 0 };
    this.costs.set(who, { units: entry.units + units, calls: entry.calls + 1 });
  }

  private failure(error: ToolError, started: number, logs: readonly LogLine[]): TracedToolOutcome {
    return {
      ok: false,
      error,
      durationMs: this.now() - started,
      deduplicated: false,
      logs,
    };
  }
}

/**
 * Converts whatever a handler threw into a classified error.
 *
 * An unclassified throw becomes `internal` and retryable: the handler crashed for a
 * reason it did not describe, and crashes of that kind are more often transient than
 * permanent. A handler that knows better should throw a `ToolError`, which is passed
 * through untouched.
 *
 * An `AbortError` — what the standard library throws when a signal fires mid-call — is
 * classified by *why* the signal fired, not by the error itself. A cancelled call
 * reported as a timeout would be retried, which is the one thing cancellation asked
 * for it not to be.
 */
function toToolError(
  err: unknown,
  def: ToolDef,
  abortReason: 'timeout' | 'cancelled' | undefined,
): ToolError {
  if (err instanceof ToolError) return err;
  if (err instanceof Error && err.name === 'AbortError') {
    return abortReason === 'cancelled'
      ? ToolError.cancelled(def.name)
      : ToolError.timeout(def.name, def.timeoutMs);
  }
  return new ToolError('internal', `${def.name} threw: ${describe(err)}`, true);
}

function describe(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

function logsOf(outcome: ToolOutcome): readonly LogLine[] {
  const maybe = outcome as { logs?: readonly LogLine[] };
  return maybe.logs ?? [];
}

/**
 * Logs are dropped from the stored copy.
 *
 * A replayed trace that shows the first call's log lines under a second call's
 * timestamps reads as if the tool ran again. The outcome is shared; the narrative of
 * one execution is not.
 */
function stripLogs(outcome: TracedToolOutcome): ToolOutcome {
  const { logs: _logs, ...rest } = outcome;
  return rest;
}

/** Builds a context for a single call, deriving the key the executor would derive. */
export function toolContext(options: {
  ref: ToolContext['ref'];
  role: string;
  tool: string;
  args: unknown;
  tenant?: string;
  signal?: AbortSignal;
  log?: ToolContext['log'];
}): ToolContext {
  const { ref, role, tool, args } = options;
  return {
    ref,
    idempotencyKey: idempotencyKeyFor(ref, tool, args),
    role,
    ...(options.tenant !== undefined ? { tenant: options.tenant } : {}),
    signal: options.signal ?? new AbortController().signal,
    log: options.log ?? ((): void => {}),
  };
}

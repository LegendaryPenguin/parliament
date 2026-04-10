import { describe, expect, it } from 'vitest';

import { FakeClock } from '../src/scheduler/clock.js';
import { AllowAllPolicy } from '../src/policy/permissions.js';
import { DefaultToolExecutor } from '../src/tools/executor.js';
import { ToolError } from '../src/tools/types.js';
import type { Schema, ToolContext, ToolDef, ToolExecutor, ToolOutcome } from '../src/tools/types.js';
import type { NodeDef, Timeouts } from '../src/workflow/types.js';
import { Worker } from '../src/workers/worker.js';
import type { WorkerOptions } from '../src/workers/worker.js';
import type {
  CompletedStep,
  Lease,
  LeaseManager,
  OutcomeReporter,
  StateReader,
  StepAssignment,
} from '../src/workers/ports.js';
import { createPredicateRegistry, numericThreshold } from '../src/workers/predicates.js';
import { createFakeProvider } from '../src/workers/llm.js';
import type { LlmStepOutput } from '../src/workers/llm.js';

const TIMEOUTS: Timeouts = { attemptTimeoutMs: 10_000, leaseDurationMs: 90 };

function node(partial: Partial<NodeDef> & Pick<NodeDef, 'name' | 'kind'>): NodeDef {
  return { dependsOn: [], ...partial };
}

function assign(def: NodeDef, input: unknown, over: Partial<StepAssignment> = {}): StepAssignment {
  const lease: Lease = {
    ref: { workflowId: 'wf-1', step: def.name, attempt: 1 },
    workerId: 'worker-1',
    expiresAt: 1_000,
    token: 'token-1',
  };
  return { lease, node: def, input, timeouts: TIMEOUTS, role: 'researcher', ...over };
}

class FakeLeases implements LeaseManager {
  readonly queue: StepAssignment[] = [];
  renewals = 0;
  alive = true;
  readonly released: Lease[] = [];

  async acquire(): Promise<StepAssignment | undefined> {
    return this.queue.shift();
  }

  async heartbeat(_lease: Lease): Promise<boolean> {
    this.renewals += 1;
    return this.alive;
  }

  async release(lease: Lease): Promise<void> {
    this.released.push(lease);
  }
}

class RecordingReporter implements OutcomeReporter {
  readonly completions: { lease: Lease; output: unknown }[] = [];
  readonly failures: { lease: Lease; message: string; errorClass: string }[] = [];

  async completed(lease: Lease, output: unknown): Promise<void> {
    this.completions.push({ lease, output });
  }

  async failed(
    lease: Lease,
    error: { readonly message: string; readonly errorClass: string },
  ): Promise<void> {
    this.failures.push({ lease, ...error });
  }
}

class FakeState implements StateReader {
  constructor(private readonly outputs: Map<string, CompletedStep> = new Map()) {}

  static of(entries: Record<string, unknown>): FakeState {
    return new FakeState(
      new Map(Object.entries(entries).map(([step, output]) => [step, { step, output }])),
    );
  }

  async completedOutputs(): Promise<ReadonlyMap<string, CompletedStep>> {
    return this.outputs;
  }
}

/** A stand-in executor, for the cases that are about the worker rather than about tools. */
function stubExecutor(
  fn: (args: unknown, ctx: ToolContext) => Promise<unknown>,
): ToolExecutor {
  return {
    register: () => {},
    get: () => undefined,
    list: () => [],
    async invoke(_name, args, ctx): Promise<ToolOutcome> {
      try {
        return { ok: true, result: await fn(args, ctx), durationMs: 1, deduplicated: false };
      } catch (err) {
        return {
          ok: false,
          error:
            err instanceof ToolError ? err : new ToolError('internal', String(err), false),
          durationMs: 1,
          deduplicated: false,
        };
      }
    },
  };
}

interface Harness {
  readonly worker: Worker;
  readonly leases: FakeLeases;
  readonly reporter: RecordingReporter;
  readonly clock: FakeClock;
}

/** Worker options with the id defaulted, so each test only states what it cares about. */
type HarnessOptions = Omit<WorkerOptions, 'workerId'> & { readonly workerId?: string };

function harness(over: Partial<HarnessOptions> = {}): Harness {
  const leases = new FakeLeases();
  const reporter = new RecordingReporter();
  const clock = new FakeClock(1_000);
  const base: HarnessOptions = {
    leases,
    reporter,
    clock,
    tools: stubExecutor(async (args) => args),
    state: FakeState.of({}),
  };
  return { worker: buildWorker({ ...base, ...over }), leases, reporter, clock };
}

function buildWorker(opts: HarnessOptions): Worker {
  return new Worker({ ...opts, workerId: opts.workerId ?? 'worker-1' });
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe('executing a step', () => {
  it('runs a tool node and reports completion', async () => {
    const { worker, leases, reporter } = harness();
    const step = node({ name: 'fetch', kind: 'tool', tool: 'echo' });
    leases.queue.push(assign(step, { text: 'hello' }));

    const result = await worker.runOnce();

    expect(result).toEqual({ kind: 'completed', output: { text: 'hello' } });
    expect(reporter.completions).toHaveLength(1);
    expect(reporter.completions[0]?.output).toEqual({ text: 'hello' });
    expect(reporter.failures).toEqual([]);
    expect(leases.released).toHaveLength(1);
  });

  it('returns undefined when there is nothing to do', async () => {
    const { worker, reporter, leases } = harness();

    expect(await worker.runOnce()).toBeUndefined();
    expect(reporter.completions).toEqual([]);
    expect(leases.released).toEqual([]);
  });

  it('passes the step attempt through to the tool context', async () => {
    const seen: ToolContext[] = [];
    const { worker, leases } = harness({
      tools: stubExecutor(async (_args, ctx) => {
        seen.push(ctx);
        return { ok: true };
      }),
    });
    const step = node({ name: 'fetch', kind: 'tool', tool: 'echo' });
    leases.queue.push(assign(step, {}, { tenant: 'acme' }));

    await worker.runOnce();

    expect(seen[0]?.ref).toEqual({ workflowId: 'wf-1', step: 'fetch', attempt: 1 });
    expect(seen[0]?.role).toBe('researcher');
    expect(seen[0]?.tenant).toBe('acme');
    expect(seen[0]?.idempotencyKey).toMatch(/^[0-9a-f]{64}$/);
  });
});

describe('heartbeating', () => {
  it('keeps renewing while a long step runs', async () => {
    // The lease is 90ms, so renewal is every 30ms. The step does not finish until the test
    // says so: a worker that renewed only between steps would send nothing in that window.
    const work = deferred<unknown>();
    const { worker, leases, clock } = harness({
      tools: stubExecutor(async () => work.promise),
    });
    const step = node({ name: 'slow', kind: 'tool', tool: 'slow' });

    const running = worker.execute(assign(step, {}));
    await clock.advance(31);
    expect(leases.renewals).toBe(1);
    await clock.advance(31);
    await clock.advance(31);
    expect(leases.renewals).toBe(3);
    expect(worker.heartbeatCount).toBe(3);

    work.resolve({ done: true });
    expect(await running).toEqual({ kind: 'completed', output: { done: true } });
  });

  it('stops renewing once the step is done', async () => {
    const { worker, leases, clock } = harness();
    const step = node({ name: 'quick', kind: 'tool', tool: 'echo' });

    await worker.execute(assign(step, { a: 1 }));
    const after = leases.renewals;
    await clock.advance(500);

    expect(leases.renewals).toBe(after);
    // A leaked timer here would keep heartbeating a lease this worker no longer holds.
    expect(clock.pending).toBe(0);
  });

  it('abandons the step when the lease is lost, without reporting or releasing', async () => {
    const work = deferred<unknown>();
    const { worker, leases, reporter, clock } = harness({
      tools: stubExecutor(async (_args, ctx) => {
        await Promise.race([
          work.promise,
          new Promise((_resolve, reject) =>
            ctx.signal.addEventListener('abort', () => reject(new Error('aborted')), {
              once: true,
            }),
          ),
        ]);
        return { done: true };
      }),
    });
    leases.alive = false;
    const step = node({ name: 'slow', kind: 'tool', tool: 'slow' });

    const running = worker.execute(assign(step, {}));
    await clock.advance(31);

    expect(await running).toEqual({ kind: 'lease_lost' });
    expect(reporter.completions).toEqual([]);
    expect(reporter.failures).toEqual([]);
    // Releasing a lease another worker now holds would revoke its claim.
    expect(leases.released).toEqual([]);
  });
});

describe('cancellation', () => {
  it('stops the step promptly and gives the lease back', async () => {
    let aborted = false;
    const { worker, leases, reporter } = harness({
      tools: stubExecutor(
        async (_args, ctx) =>
          new Promise((_resolve, reject) => {
            ctx.signal.addEventListener(
              'abort',
              () => {
                aborted = true;
                const err = new Error('aborted');
                err.name = 'AbortError';
                reject(err);
              },
              { once: true },
            );
          }),
      ),
    });
    const controller = new AbortController();
    const step = node({ name: 'slow', kind: 'tool', tool: 'slow' });

    const running = worker.execute(assign(step, {}), controller.signal);
    controller.abort();

    expect(await running).toEqual({ kind: 'cancelled' });
    expect(aborted).toBe(true);
    // No outcome is claimed: the step did not finish, and the workflow is already ending.
    expect(reporter.completions).toEqual([]);
    expect(reporter.failures).toEqual([]);
    expect(leases.released).toHaveLength(1);
  });

  it('reports a timeout when the attempt outruns its budget', async () => {
    const { worker, reporter, clock } = harness({
      tools: stubExecutor(
        async (_args, ctx) =>
          new Promise((_resolve, reject) => {
            ctx.signal.addEventListener('abort', () => reject(new Error('aborted')), {
              once: true,
            });
          }),
      ),
    });
    const step = node({ name: 'slow', kind: 'tool', tool: 'slow', attemptTimeoutMs: 50 });

    const running = worker.execute(assign(step, {}));
    await clock.advance(51);

    expect(await running).toMatchObject({ kind: 'failed', errorClass: 'timeout' });
    expect((await running as { message: string }).message).toContain('attempt exceeded 50ms');
    expect(reporter.failures[0]?.errorClass).toBe('timeout');
  });
});

describe('surviving a bad step', () => {
  it('turns an unexpected throw into a classified failure and keeps working', async () => {
    let calls = 0;
    const { worker, leases, reporter } = harness({
      tools: {
        register: () => {},
        get: () => undefined,
        list: () => [],
        async invoke(): Promise<ToolOutcome> {
          calls += 1;
          // Not a ToolError and not an outcome: the executor itself misbehaving, which is
          // precisely the case a worker must not die on.
          if (calls === 1) throw new TypeError('cannot read properties of undefined');
          return { ok: true, result: 'fine', durationMs: 1, deduplicated: false };
        },
      },
    });
    leases.queue.push(assign(node({ name: 'bad', kind: 'tool', tool: 'x' }), {}));
    leases.queue.push(assign(node({ name: 'good', kind: 'tool', tool: 'x' }), {}));

    const first = await worker.runOnce();
    const second = await worker.runOnce();

    expect(first).toEqual({
      kind: 'failed',
      errorClass: 'internal',
      message: 'TypeError: cannot read properties of undefined',
    });
    expect(second).toEqual({ kind: 'completed', output: 'fine' });
    expect(reporter.failures).toHaveLength(1);
    expect(reporter.completions).toHaveLength(1);
    expect(leases.released).toHaveLength(2);
  });

  it('declines a node kind it does not run, releasing the lease', async () => {
    const { worker, leases, reporter } = harness();
    leases.queue.push(assign(node({ name: 'score', kind: 'python' }), {}));

    const result = await worker.runOnce();

    expect(result?.kind).toBe('declined');
    expect(reporter.failures).toEqual([]);
    expect(leases.released).toHaveLength(1);
  });
});

describe('idempotent redelivery', () => {
  const countingTool: ToolDef = {
    name: 'charge',
    description: 'an effect that must not happen twice',
    input: { type: 'object', properties: { amount: { type: 'integer' } }, required: ['amount'] },
    output: { type: 'object', properties: { receipt: { type: 'string' } }, required: ['receipt'] },
    permissions: {
      effect: 'write',
      allowedRoles: ['researcher'],
      requiresApproval: false,
      idempotent: true,
    },
    timeoutMs: 1_000,
  };

  it('does not repeat a tool effect when the same attempt is delivered twice', async () => {
    let charges = 0;
    const tools = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
    tools.register(countingTool, async () => {
      charges += 1;
      return { receipt: `receipt-${charges}` };
    });

    const { worker, leases, reporter } = harness({ tools });
    const step = node({ name: 'charge', kind: 'tool', tool: 'charge' });
    // The same lease twice: a redelivery after a lease expiry, with the attempt unchanged.
    const redelivered = assign(step, { amount: 100 });

    const first = await worker.execute(redelivered);
    const second = await worker.execute(redelivered);

    expect(charges).toBe(1);
    expect(first).toEqual({ kind: 'completed', output: { receipt: 'receipt-1' } });
    expect(second).toEqual(first);
    expect(reporter.completions).toHaveLength(2);
    expect(leases.released).toHaveLength(2);
  });

  it('calls the tool again for a genuine retry, which is a new attempt', async () => {
    let charges = 0;
    const tools = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
    tools.register(countingTool, async () => {
      charges += 1;
      return { receipt: `receipt-${charges}` };
    });
    const { worker } = harness({ tools });
    const step = node({ name: 'charge', kind: 'tool', tool: 'charge' });
    const first = assign(step, { amount: 100 });
    const retry = assign(step, { amount: 100 }, {
      lease: { ...first.lease, ref: { ...first.lease.ref, attempt: 2 } },
    });

    await worker.execute(first);
    await worker.execute(retry);

    // The key includes the attempt, so a retry is allowed to produce a new effect. The
    // deduplication window is one attempt wide, deliberately.
    expect(charges).toBe(2);
  });
});

describe('condition nodes', () => {
  function conditionHarness(outputs: Record<string, unknown>): Harness {
    const predicates = createPredicateRegistry();
    predicates.register(numericThreshold('score-passes', 'score', 'total', '>=', 70));
    predicates.register({
      name: 'reads-a-later-step',
      description: 'deliberately wired to a step that has not run',
      reads: ['review'],
      fn: (view) => view.require('review') === 'ok',
    });
    return harness({ predicates, state: FakeState.of(outputs) });
  }

  it('branches on a real prior output', async () => {
    const { worker, reporter } = conditionHarness({ score: { total: 88 } });
    const step = node({
      name: 'gate',
      kind: 'condition',
      predicate: 'score-passes',
      onTrue: 'publish',
      onFalse: 'revise',
      dependsOn: ['score'],
    });

    const result = await worker.execute(assign(step, undefined));

    expect(result).toEqual({
      kind: 'completed',
      output: { predicate: 'score-passes', value: true, next: 'publish' },
    });
    expect(reporter.completions[0]?.output).toMatchObject({ next: 'publish' });
  });

  it('takes the false branch on a real prior output', async () => {
    const { worker } = conditionHarness({ score: { total: 12 } });
    const step = node({
      name: 'gate',
      kind: 'condition',
      predicate: 'score-passes',
      onTrue: 'publish',
      onFalse: 'revise',
    });

    expect(await worker.execute(assign(step, undefined))).toEqual({
      kind: 'completed',
      output: { predicate: 'score-passes', value: false, next: 'revise' },
    });
  });

  it('fails clearly when a predicate reads a step that has not completed', async () => {
    const { worker, reporter } = conditionHarness({ score: { total: 88 } });
    const step = node({ name: 'gate', kind: 'condition', predicate: 'reads-a-later-step' });

    const result = await worker.execute(assign(step, undefined));

    expect(result.kind).toBe('failed');
    const failure = reporter.failures[0];
    expect(failure?.errorClass).toBe('definition_error');
    // Names the predicate, the step it wanted, and what was actually available.
    expect(failure?.message).toContain('reads-a-later-step');
    expect(failure?.message).toContain('"review"');
    expect(failure?.message).toContain('completed: score');
  });

  it('fails clearly when the predicate is not registered', async () => {
    const { worker, reporter } = conditionHarness({});
    const step = node({ name: 'gate', kind: 'condition', predicate: 'no-such-predicate' });

    expect((await worker.execute(assign(step, undefined))).kind).toBe('failed');
    expect(reporter.failures[0]?.message).toContain('not registered');
  });

  it('fails rather than defaulting to false when the field is the wrong type', async () => {
    const { worker, reporter } = conditionHarness({ score: { total: 'high' } });
    const step = node({ name: 'gate', kind: 'condition', predicate: 'score-passes' });

    expect((await worker.execute(assign(step, undefined))).kind).toBe('failed');
    expect(reporter.failures[0]?.message).toContain('expected number');
  });
});

describe('parallel, join and finalize', () => {
  it('fans out and gathers branch outputs', async () => {
    const { worker } = harness({
      tools: stubExecutor(async (args) => ({ echoed: args })),
    });
    const step = node({ name: 'fanout', kind: 'parallel' });
    const branches = {
      branches: [
        { name: 'a', node: node({ name: 'a', kind: 'tool', tool: 'echo' }), input: { n: 1 } },
        { name: 'b', node: node({ name: 'b', kind: 'tool', tool: 'echo' }), input: { n: 2 } },
      ],
    };

    expect(await worker.execute(assign(step, branches))).toEqual({
      kind: 'completed',
      output: { branches: { a: { echoed: { n: 1 } }, b: { echoed: { n: 2 } } } },
    });
  });

  it('reports every failing branch, not just the first', async () => {
    const { worker, reporter } = harness({
      tools: stubExecutor(async (args) => {
        const n = (args as { n: number }).n;
        if (n !== 1) throw new ToolError('unavailable', `branch ${n} is down`, true);
        return { ok: true };
      }),
    });
    const step = node({ name: 'fanout', kind: 'parallel' });
    const branches = {
      branches: [1, 2, 3].map((n) => ({
        name: `b${n}`,
        node: node({ name: `b${n}`, kind: 'tool', tool: 'echo' }),
        input: { n },
      })),
    };

    expect((await worker.execute(assign(step, branches))).kind).toBe('failed');
    const message = reporter.failures[0]?.message ?? '';
    expect(message).toContain('2 of 3 branches failed');
    expect(message).toContain('branch 2 is down');
    expect(message).toContain('branch 3 is down');
  });

  it('gathers the steps a join waits on', async () => {
    const { worker } = harness({ state: FakeState.of({ a: 1, b: 2, unrelated: 3 }) });
    const step = node({ name: 'join', kind: 'join', dependsOn: ['a', 'b'] });

    expect(await worker.execute(assign(step, undefined))).toEqual({
      kind: 'completed',
      output: { gathered: { a: 1, b: 2 } },
    });
  });

  it('fails a join whose inputs are incomplete rather than gathering a subset', async () => {
    const { worker, reporter } = harness({ state: FakeState.of({ a: 1 }) });
    const step = node({ name: 'join', kind: 'join', dependsOn: ['a', 'b'] });

    expect((await worker.execute(assign(step, undefined))).kind).toBe('failed');
    expect(reporter.failures[0]?.message).toContain('have not completed: b');
  });

  it('finalizes with every completed output when no projection was supplied', async () => {
    const { worker } = harness({ state: FakeState.of({ b: 2, a: 1 }) });
    const step = node({ name: 'done', kind: 'finalize' });

    expect(await worker.execute(assign(step, undefined))).toEqual({
      kind: 'completed',
      output: { output: { a: 1, b: 2 }, steps: ['a', 'b'] },
    });
  });
});

describe('llm nodes', () => {
  const summarySchema: Schema = {
    type: 'object',
    properties: {
      verdict: { type: 'string', enum: ['approve', 'revise'] },
      confidence: { type: 'number', minimum: 0, maximum: 1 },
    },
    required: ['verdict', 'confidence'],
  };

  const input = {
    context: { prompt: 'Summarise the filing and recommend an action.', system: 'Be terse.' },
    expect: summarySchema,
  };

  function llmHarness(provider = createFakeProvider({ seed: 'test' })): Harness {
    return harness({ llm: provider });
  }

  it('is deterministic across runs', async () => {
    const step = node({ name: 'review', kind: 'llm' });
    const first = await llmHarness().worker.execute(assign(step, input));
    const second = await llmHarness().worker.execute(assign(step, input));

    expect(first).toEqual(second);
    expect(first.kind).toBe('completed');
    const output = (first as { output: LlmStepOutput }).output;
    expect(output.value).toMatchObject({ verdict: expect.any(String) });
    expect(['approve', 'revise']).toContain((output.value as { verdict: string }).verdict);
  });

  it('keeps the prompt out of workflow state and the metadata in it', async () => {
    const step = node({ name: 'review', kind: 'llm' });
    const result = await llmHarness().worker.execute(assign(step, input));
    const output = (result as { output: LlmStepOutput }).output;

    expect(JSON.stringify(output)).not.toContain('Summarise the filing');
    expect(output.meta).toMatchObject({
      provider: 'fake:test',
      finishReason: 'stop',
      validated: true,
    });
    expect(output.meta.promptChars).toBeGreaterThan(0);
    expect(output.meta.promptDigest).toMatch(/^[0-9a-f]{8}$/);
  });

  it('produces different output for a different prompt', async () => {
    const step = node({ name: 'review', kind: 'llm' });
    const a = await llmHarness().worker.execute(assign(step, input));
    const b = await llmHarness().worker.execute(
      assign(step, { ...input, context: { ...input.context, prompt: 'A different question.' } }),
    );

    expect(a).not.toEqual(b);
  });

  it('fails the step on malformed output instead of crashing on the parse', async () => {
    const provider = createFakeProvider({ seed: 'test', behaviour: { kind: 'malformed' } });
    const { worker, reporter } = llmHarness(provider);
    const step = node({ name: 'review', kind: 'llm' });

    const result = await worker.execute(assign(step, input));

    expect(result.kind).toBe('failed');
    const failure = reporter.failures[0];
    expect(failure?.errorClass).toBe('invalid_output');
    expect(failure?.message).toContain('not JSON');
    // The offending text is quoted, so the failure is diagnosable from the event alone.
    expect(failure?.message).toContain('received:');
  });

  it('fails the step when the model answers with prose where an object was required', async () => {
    const provider = createFakeProvider({ seed: 'test', behaviour: { kind: 'prose' } });
    const { worker, reporter } = llmHarness(provider);

    const result = await worker.execute(assign(node({ name: 'review', kind: 'llm' }), input));

    expect(result.kind).toBe('failed');
    expect(reporter.failures[0]?.errorClass).toBe('invalid_output');
  });

  it('fails the step when the output misses the declared schema', async () => {
    const provider = createFakeProvider({ seed: 'test' });
    const { worker, reporter } = llmHarness(provider);
    // A schema the fake satisfies, swapped for one it cannot: the generated value is kept
    // but validated against a stricter shape, which is the real-world drift case.
    const step = node({ name: 'review', kind: 'llm' });
    const strict = {
      context: input.context,
      expect: {
        type: 'object',
        properties: { verdict: { type: 'string', enum: ['approve'] } },
        required: ['verdict', 'rationale'],
      } satisfies Schema,
    };

    const result = await worker.execute(assign(step, strict));

    expect(result.kind).toBe('failed');
    expect(reporter.failures[0]?.message).toContain('$.rationale: required');
  });

  it('reports a refusal as a step failure rather than as text', async () => {
    const provider = createFakeProvider({ seed: 'test', behaviour: { kind: 'refusal' } });
    const { worker, reporter } = llmHarness(provider);

    expect(
      (await worker.execute(assign(node({ name: 'review', kind: 'llm' }), input))).kind,
    ).toBe('failed');
    expect(reporter.failures[0]?.message).toContain('refused');
  });

  it('times out a hanging model call', async () => {
    // Real timers here: the call's own budget is enforced inside the llm handler, and the
    // point of the test is that the handler stops waiting on its own.
    const provider = createFakeProvider({ seed: 'test', behaviour: { kind: 'hang' } });
    const leases = new FakeLeases();
    const reporter = new RecordingReporter();
    const worker = buildWorker({
      leases,
      reporter,
      state: FakeState.of({}),
      tools: stubExecutor(async (args) => args),
      llm: provider,
      heartbeatIntervalMs: 10_000,
    });
    const step = node({ name: 'review', kind: 'llm', attemptTimeoutMs: 30 });

    const result = await worker.execute(assign(step, input));

    expect(result.kind).toBe('failed');
    expect(reporter.failures[0]?.errorClass).toBe('timeout');
    expect(leases.released).toHaveLength(1);
  });

  it('applies a rule only to the matching prompt', async () => {
    const provider = createFakeProvider({
      seed: 'test',
      rules: [{ when: 'audit', behaviour: { kind: 'malformed' } }],
    });
    const { worker } = llmHarness(provider);
    const step = node({ name: 'review', kind: 'llm' });

    const clean = await worker.execute(assign(step, input));
    const broken = await worker.execute(
      assign(step, { ...input, context: { prompt: 'Run an audit of the filing.' } }),
    );

    expect(clean.kind).toBe('completed');
    expect(broken.kind).toBe('failed');
  });

  it('rejects a node whose input carries no prompt', async () => {
    const { worker, reporter } = llmHarness();

    const result = await worker.execute(
      assign(node({ name: 'review', kind: 'llm' }), { context: {} }),
    );

    expect(result.kind).toBe('failed');
    expect(reporter.failures[0]?.errorClass).toBe('invalid_args');
  });
});

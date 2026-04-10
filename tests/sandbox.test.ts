import { describe, expect, it } from 'vitest';

import { runInSandbox, sandboxedHandler } from '../src/tools/sandbox.js';
import { DefaultToolExecutor, toolContext } from '../src/tools/executor.js';
import { AllowAllPolicy } from '../src/policy/permissions.js';
import type { ToolDef } from '../src/tools/types.js';
import type { StepRef } from '../src/workflow/types.js';

const ref: StepRef = { workflowId: 'wf-1', step: 'sandboxed', attempt: 1 };

/** Generous: process startup dominates, and a flaky CI box should not fail the suite. */
const SPAWN_BUDGET_MS = 15_000;

describe('sandboxed execution', () => {
  it(
    'returns a JSON-serializable result from a child process',
    async () => {
      const outcome = await runInSandbox(
        'export default async (args) => ({ doubled: args.n * 2 });',
        { n: 21 },
        { timeoutMs: SPAWN_BUDGET_MS },
      );

      expect(outcome.ok).toBe(true);
      expect(outcome.value).toEqual({ doubled: 42 });
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'does not inherit the parent environment',
    async () => {
      process.env.PARLIAMENT_SANDBOX_PROBE = 'leaked';
      try {
        const outcome = await runInSandbox(
          'export default async () => ({ probe: process.env.PARLIAMENT_SANDBOX_PROBE ?? null });',
          {},
          { timeoutMs: SPAWN_BUDGET_MS },
        );

        expect(outcome.ok).toBe(true);
        expect(outcome.value).toEqual({ probe: null });
      } finally {
        delete process.env.PARLIAMENT_SANDBOX_PROBE;
      }
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'reports a child that throws as a clean error, and stays alive',
    async () => {
      const outcome = await runInSandbox(
        'export default async () => { throw new Error("tool blew up"); };',
        {},
        { timeoutMs: SPAWN_BUDGET_MS },
      );

      expect(outcome.ok).toBe(false);
      expect(outcome.error?.errorClass).toBe('internal');
      expect(outcome.error?.message).toContain('tool blew up');
      // The parent is demonstrably still running the test.
      expect(typeof process.pid).toBe('number');
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'reports a child that kills itself as an exit without a result',
    async () => {
      const outcome = await runInSandbox(
        'export default async () => { process.exit(3); };',
        {},
        { timeoutMs: SPAWN_BUDGET_MS },
      );

      expect(outcome.ok).toBe(false);
      expect(outcome.error?.message).toContain('exited without a result');
      expect(outcome.error?.retryable).toBe(true);
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'kills an infinite loop on the wall clock',
    async () => {
      const started = Date.now();
      const outcome = await runInSandbox(
        'export default async () => { while (true) {} };',
        {},
        { timeoutMs: 300 },
      );

      expect(outcome.ok).toBe(false);
      expect(outcome.error?.errorClass).toBe('timeout');
      expect(outcome.error?.message).toContain('was killed');
      // A cooperative abort could never have interrupted that loop.
      expect(Date.now() - started).toBeLessThan(SPAWN_BUDGET_MS);
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'rejects a non-serializable return value',
    async () => {
      const bigint = await runInSandbox(
        'export default async () => ({ n: 1n });',
        {},
        { timeoutMs: SPAWN_BUDGET_MS },
      );
      expect(bigint.ok).toBe(false);
      expect(bigint.error?.message.toLowerCase()).toContain('bigint');

      const circular = await runInSandbox(
        'export default async () => { const o = {}; o.self = o; return o; };',
        {},
        { timeoutMs: SPAWN_BUDGET_MS },
      );
      expect(circular.ok).toBe(false);
      expect(circular.error?.errorClass).toBe('internal');

      const fn = await runInSandbox(
        'export default async () => (() => 1);',
        {},
        { timeoutMs: SPAWN_BUDGET_MS },
      );
      expect(fn.ok).toBe(false);
      expect(fn.error?.message).toContain('non-serializable function');
    },
    SPAWN_BUDGET_MS * 3,
  );

  it(
    'captures child stdout and stderr without confusing it for a result',
    async () => {
      const outcome = await runInSandbox(
        [
          'export default async () => {',
          '  console.log("chatty line one");',
          '  console.error("a warning");',
          '  return { ok: true };',
          '};',
        ].join('\n'),
        {},
        { timeoutMs: SPAWN_BUDGET_MS },
      );

      expect(outcome.ok).toBe(true);
      expect(outcome.value).toEqual({ ok: true });
      expect(outcome.stdout).toContain('chatty line one');
      expect(outcome.stderr).toContain('a warning');
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'truncates a child that prints without stopping',
    async () => {
      const outcome = await runInSandbox(
        [
          'export default async () => {',
          '  for (let i = 0; i < 500; i += 1) console.log("line " + i);',
          '  return { done: true };',
          '};',
        ].join('\n'),
        {},
        { timeoutMs: SPAWN_BUDGET_MS, maxOutputLines: 10 },
      );

      expect(outcome.ok).toBe(true);
      expect(outcome.stdout.length).toBeLessThanOrEqual(11);
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'reports a module with no default export',
    async () => {
      const outcome = await runInSandbox(
        'export const notDefault = 1;',
        {},
        { timeoutMs: SPAWN_BUDGET_MS },
      );

      expect(outcome.ok).toBe(false);
      expect(outcome.error?.message).toContain('no default export function');
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'reports a module that fails to parse',
    async () => {
      const outcome = await runInSandbox(
        'export default async () => { this is not javascript',
        {},
        { timeoutMs: SPAWN_BUDGET_MS },
      );

      expect(outcome.ok).toBe(false);
      expect(outcome.error?.errorClass).toBe('internal');
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'honours a caller abort',
    async () => {
      const controller = new AbortController();
      setTimeout(() => controller.abort(), 100);

      const outcome = await runInSandbox('export default async () => { while (true) {} };', {}, {
        timeoutMs: SPAWN_BUDGET_MS,
        signal: controller.signal,
      });

      expect(outcome.ok).toBe(false);
      // Cancelled, not timed out: the wall clock had 15s left, and a retry is the
      // opposite of what the caller asked for.
      expect(outcome.error?.errorClass).toBe('cancelled');
      expect(outcome.error?.retryable).toBe(false);
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'bounds the child heap',
    async () => {
      const outcome = await runInSandbox(
        [
          'export default async () => {',
          '  const held = [];',
          '  for (;;) held.push(new Array(1_000_000).fill(Math.random()));',
          '};',
        ].join('\n'),
        {},
        { timeoutMs: SPAWN_BUDGET_MS, memoryMb: 32 },
      );

      // Either the heap cap killed it or the wall clock did; both are the parent
      // surviving, which is the property being claimed.
      expect(outcome.ok).toBe(false);
      expect(['internal', 'timeout']).toContain(outcome.error?.errorClass);
    },
    SPAWN_BUDGET_MS + 10_000,
  );
});

describe('sandboxed tools through the executor', () => {
  const def: ToolDef = {
    name: 'untrusted',
    description: 'runs supplied code out of process',
    input: { type: 'object', properties: { n: { type: 'integer' } }, required: ['n'] },
    output: { type: 'object', properties: { doubled: { type: 'integer' } }, required: ['doubled'] },
    permissions: {
      effect: 'read',
      allowedRoles: ['researcher'],
      requiresApproval: false,
      idempotent: true,
    },
    timeoutMs: SPAWN_BUDGET_MS,
  };

  it(
    'surfaces a sandboxed success as a normal outcome',
    async () => {
      const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
      ex.register(
        def,
        sandboxedHandler('export default async (args) => ({ doubled: args.n * 2 });', {
          timeoutMs: SPAWN_BUDGET_MS,
        }),
      );
      const args = { n: 4 };

      const outcome = await ex.invoke(
        'untrusted',
        args,
        toolContext({ ref, role: 'researcher', tool: 'untrusted', args }),
      );

      expect(outcome.ok).toBe(true);
      expect(outcome.result).toEqual({ doubled: 8 });
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'surfaces a sandboxed crash as a failed outcome, not an exception',
    async () => {
      const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
      ex.register(
        def,
        sandboxedHandler('export default async () => { throw new Error("nope"); };', {
          timeoutMs: SPAWN_BUDGET_MS,
        }),
      );
      const args = { n: 4 };

      const outcome = await ex.invoke(
        'untrusted',
        args,
        toolContext({ ref, role: 'researcher', tool: 'untrusted', args }),
      );

      expect(outcome.ok).toBe(false);
      expect(outcome.error?.message).toContain('nope');
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'catches a sandboxed tool whose output breaks the declared schema',
    async () => {
      const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
      ex.register(
        def,
        sandboxedHandler('export default async () => ({ doubled: "eight" });', {
          timeoutMs: SPAWN_BUDGET_MS,
        }),
      );
      const args = { n: 4 };

      const outcome = await ex.invoke(
        'untrusted',
        args,
        toolContext({ ref, role: 'researcher', tool: 'untrusted', args }),
      );

      expect(outcome.error?.errorClass).toBe('invalid_output');
      expect(outcome.error?.message).toContain('$.doubled');
    },
    SPAWN_BUDGET_MS + 5_000,
  );

  it(
    'times out a sandboxed loop through the executor and reports it as a timeout',
    async () => {
      const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
      ex.register({ ...def, timeoutMs: 400 }, sandboxedHandler(
        'export default async () => { while (true) {} };',
        { timeoutMs: 10_000 },
      ));
      const args = { n: 4 };

      const outcome = await ex.invoke(
        'untrusted',
        args,
        toolContext({ ref, role: 'researcher', tool: 'untrusted', args }),
      );

      expect(outcome.ok).toBe(false);
      expect(outcome.error?.errorClass).toBe('timeout');
    },
    SPAWN_BUDGET_MS + 5_000,
  );
});

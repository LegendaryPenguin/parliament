import { describe, expect, it } from 'vitest';

import {
  InMemoryResultStore,
  canonicalJson,
  canonicalize,
  idempotencyKeyFor,
} from '../src/tools/idempotency.js';
import { DefaultToolExecutor, toolContext } from '../src/tools/executor.js';
import { AllowAllPolicy } from '../src/policy/permissions.js';
import { createFlakyTool } from '../src/tools/builtin.js';
import type { StepRef } from '../src/workflow/types.js';

const ref: StepRef = { workflowId: 'wf-1', step: 'call', attempt: 1 };

describe('canonicalization', () => {
  it('is insensitive to key order, at every depth', () => {
    const a = { b: 2, a: 1, nested: { y: [1, { q: 1, p: 2 }], x: true } };
    const b = { nested: { x: true, y: [1, { p: 2, q: 1 }] }, a: 1, b: 2 };

    expect(canonicalJson(a)).toBe(canonicalJson(b));
  });

  it('is sensitive to array order, because order is content', () => {
    expect(canonicalJson([1, 2])).not.toBe(canonicalJson([2, 1]));
  });

  it('treats an undefined-valued property as absent', () => {
    expect(canonicalJson({ a: 1, b: undefined })).toBe(canonicalJson({ a: 1 }));
  });

  it('keeps an undefined array element as null rather than shifting indices', () => {
    expect(canonicalJson([1, undefined, 3])).toBe('[1,null,3]');
  });

  it('refuses values that have no stable serialization', () => {
    expect(() => canonicalize(1n)).toThrow(/bigint/);
    expect(() => canonicalize(() => 1)).toThrow(/function/);
    expect(() => canonicalize(Number.NaN)).toThrow(/non-finite/);

    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    expect(() => canonicalize(cycle)).toThrow(/circular/);
  });

  it('hashes a Date by its instant', () => {
    const when = new Date('2026-01-01T00:00:00.000Z');
    expect(canonicalJson({ when })).toBe('{"when":"2026-01-01T00:00:00.000Z"}');
  });
});

describe('key derivation', () => {
  it('is stable under argument key reordering', () => {
    const left = idempotencyKeyFor(ref, 'tool', { a: 1, b: { c: 2, d: 3 } });
    const right = idempotencyKeyFor(ref, 'tool', { b: { d: 3, c: 2 }, a: 1 });

    expect(left).toBe(right);
  });

  it('differs across workflow, step, attempt, tool and arguments', () => {
    const base = idempotencyKeyFor(ref, 'tool', { a: 1 });
    const keys = new Set([
      base,
      idempotencyKeyFor({ ...ref, workflowId: 'wf-2' }, 'tool', { a: 1 }),
      idempotencyKeyFor({ ...ref, step: 'other' }, 'tool', { a: 1 }),
      idempotencyKeyFor({ ...ref, attempt: 2 }, 'tool', { a: 1 }),
      idempotencyKeyFor(ref, 'other-tool', { a: 1 }),
      idempotencyKeyFor(ref, 'tool', { a: 2 }),
    ]);

    expect(keys.size).toBe(6);
  });

  it('cannot be confused by moving content across field boundaries', () => {
    const left = idempotencyKeyFor({ workflowId: 'a/b', step: 'c', attempt: 1 }, 't', {});
    const right = idempotencyKeyFor({ workflowId: 'a', step: 'b/c', attempt: 1 }, 't', {});

    // Both of these would collapse to the same string under a naive '/' join. The key
    // does not use stepRefKey — which now escapes '/' and is injective in its own
    // right — because this hash should not inherit a correctness property from a
    // display encoding: it uses NUL, a byte that cannot occur in any of these fields,
    // so it stays injective regardless of how steps are rendered elsewhere.
    expect(left).not.toBe(right);

    // The same separability, checked against the shape the escaping is for.
    expect(idempotencyKeyFor({ workflowId: 'a%2Fb', step: 'c', attempt: 1 }, 't', {})).not.toBe(
      left,
    );
  });

  it('is a hex sha256 digest', () => {
    expect(idempotencyKeyFor(ref, 'tool', {})).toMatch(/^[0-9a-f]{64}$/);
  });
});

describe('result store', () => {
  it('keeps the first recorded outcome and ignores a second write', async () => {
    const store = new InMemoryResultStore();
    await store.record('k', { ok: true, result: 'first', durationMs: 1, deduplicated: false }, 10);
    await store.record('k', { ok: true, result: 'second', durationMs: 1, deduplicated: false }, 20);

    const recorded = await store.get('k');
    expect(recorded?.outcome.result).toBe('first');
    expect(recorded?.recordedAt).toBe(10);
  });

  it('evicts oldest-first at the cap and counts the loss of dedup coverage', async () => {
    const store = new InMemoryResultStore(2);
    for (const key of ['a', 'b', 'c']) {
      await store.record(key, { ok: true, result: key, durationMs: 0, deduplicated: false });
    }

    expect(store.size()).toBe(2);
    expect(await store.get('a')).toBeUndefined();
    expect((await store.get('c'))?.outcome.result).toBe('c');
    expect(store.evictedCount()).toBe(1);
  });
});

describe('deduplication through the executor', () => {
  function setup(): { ex: DefaultToolExecutor; flaky: ReturnType<typeof createFlakyTool> } {
    const flaky = createFlakyTool();
    const ex = new DefaultToolExecutor({
      policy: new AllowAllPolicy(),
      store: new InMemoryResultStore(),
    });
    ex.register(flaky.def, flaky.handler);
    return { ex, flaky };
  }

  it('runs the handler exactly once for a redelivered attempt', async () => {
    const { ex, flaky } = setup();
    const args = { label: 'x' };
    const ctx = toolContext({ ref, role: 'researcher', tool: 'flaky', args });

    const first = await ex.invoke('flaky', args, ctx);
    const second = await ex.invoke('flaky', args, ctx);

    expect(flaky.calls()).toBe(1);
    expect(first.deduplicated).toBe(false);
    expect(second.deduplicated).toBe(true);
    expect(second.result).toEqual(first.result);
  });

  it('deduplicates on reordered arguments, since they are the same call', async () => {
    const flaky = createFlakyTool('reorder');
    const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
    ex.register(flaky.def, flaky.handler);

    const a = { label: 'x', n: 1 };
    const b = { n: 1, label: 'x' };

    await ex.invoke('reorder', a, toolContext({ ref, role: 'researcher', tool: 'reorder', args: a }));
    const second = await ex.invoke(
      'reorder',
      b,
      toolContext({ ref, role: 'researcher', tool: 'reorder', args: b }),
    );

    expect(flaky.calls()).toBe(1);
    expect(second.deduplicated).toBe(true);
  });

  it('lets a genuine retry re-run, because a new attempt is a new key', async () => {
    const { ex, flaky } = setup();
    const args = { label: 'x' };

    await ex.invoke('flaky', args, toolContext({ ref, role: 'researcher', tool: 'flaky', args }));
    const retry = await ex.invoke(
      'flaky',
      args,
      toolContext({ ref: { ...ref, attempt: 2 }, role: 'researcher', tool: 'flaky', args }),
    );

    expect(flaky.calls()).toBe(2);
    expect(retry.deduplicated).toBe(false);
  });

  it('replays a recorded failure rather than re-running the same attempt', async () => {
    const { ex, flaky } = setup();
    flaky.failNext(5, 'unavailable');
    const args = { label: 'x' };
    const ctx = toolContext({ ref, role: 'researcher', tool: 'flaky', args });

    const first = await ex.invoke('flaky', args, ctx);
    const second = await ex.invoke('flaky', args, ctx);

    expect(first.ok).toBe(false);
    expect(flaky.calls()).toBe(1);
    expect(second.deduplicated).toBe(true);
    expect(second.error?.errorClass).toBe('unavailable');
  });

  it('coalesces two concurrent deliveries of one key into one handler call', async () => {
    let calls = 0;
    const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
    ex.register(
      {
        name: 'slowish',
        description: 'resolves after a tick',
        input: { type: 'object', properties: { a: { type: 'integer' } }, required: ['a'] },
        output: { type: 'object', properties: { a: { type: 'integer' } }, required: ['a'] },
        permissions: {
          effect: 'read',
          allowedRoles: ['researcher'],
          requiresApproval: false,
          idempotent: true,
        },
        timeoutMs: 500,
      },
      async (args) => {
        calls += 1;
        await new Promise((r) => setTimeout(r, 20));
        return args;
      },
    );

    const args = { a: 1 };
    const ctx = toolContext({ ref, role: 'researcher', tool: 'slowish', args });
    const [first, second] = await Promise.all([
      ex.invoke('slowish', args, ctx),
      ex.invoke('slowish', args, ctx),
    ]);

    expect(calls).toBe(1);
    expect(first.ok && second.ok).toBe(true);
    expect([first.deduplicated, second.deduplicated].filter(Boolean)).toHaveLength(1);
  });

  it('does not record an outcome when the call was never made', async () => {
    const store = new InMemoryResultStore();
    const flaky = createFlakyTool('unrecorded');
    const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy(), store });
    ex.register(flaky.def, flaky.handler);

    // Invalid arguments: rejected before the key is even looked up.
    await ex.invoke(
      'unrecorded',
      { nope: true },
      toolContext({ ref, role: 'researcher', tool: 'unrecorded', args: { nope: true } }),
    );

    expect(store.size()).toBe(0);
  });

  it('derives a key when the caller supplies none', async () => {
    const { ex, flaky } = setup();
    const args = { label: 'x' };
    const bare = {
      ref,
      idempotencyKey: '',
      role: 'researcher',
      signal: new AbortController().signal,
      log: (): void => {},
    };

    await ex.invoke('flaky', args, bare);
    await ex.invoke('flaky', args, bare);

    expect(flaky.calls()).toBe(1);
  });

  it('hands the handler the same key the store is indexed by', async () => {
    let seen = '';
    const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
    ex.register(
      {
        name: 'echokey',
        description: 'reports the key it was given',
        input: { type: 'object', properties: {} },
        output: { type: 'object', properties: { key: { type: 'string' } }, required: ['key'] },
        permissions: {
          effect: 'read',
          allowedRoles: ['researcher'],
          requiresApproval: false,
          idempotent: true,
        },
        timeoutMs: 100,
      },
      async (_args, ctx) => {
        seen = ctx.idempotencyKey;
        return { key: ctx.idempotencyKey };
      },
    );

    await ex.invoke(
      'echokey',
      {},
      toolContext({ ref, role: 'researcher', tool: 'echokey', args: {} }),
    );

    // Forwarding this key to a remote API's idempotency header is the only way the
    // deduplication claim extends past this process.
    expect(seen).toBe(idempotencyKeyFor(ref, 'echokey', {}));
  });
});

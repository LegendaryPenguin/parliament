import { describe, expect, it } from 'vitest';
import { mkdtemp, mkdir, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { DefaultToolExecutor, toolContext } from '../src/tools/executor.js';
import type { ToolDef, ToolHandler } from '../src/tools/types.js';
import { ToolError } from '../src/tools/types.js';
import {
  createFileWriteTool,
  createFlakyTool,
  createFsReadTool,
  createHttpFetchTool,
} from '../src/tools/builtin.js';
import { AllowAllPolicy } from '../src/policy/permissions.js';

const ref = { workflowId: 'wf-1', step: 'fetch', attempt: 1 };

function executor(): DefaultToolExecutor {
  return new DefaultToolExecutor({ policy: new AllowAllPolicy() });
}

const echoDef: ToolDef = {
  name: 'echo',
  description: 'echoes back a validated payload',
  input: {
    type: 'object',
    properties: {
      text: { type: 'string', minLength: 1 },
      count: { type: 'integer', minimum: 1, maximum: 10 },
      tags: { type: 'array', items: { type: 'string' } },
    },
    required: ['text', 'count'],
  },
  output: {
    type: 'object',
    properties: { text: { type: 'string' }, count: { type: 'integer' } },
    required: ['text', 'count'],
  },
  permissions: {
    effect: 'read',
    allowedRoles: ['researcher'],
    requiresApproval: false,
    idempotent: true,
  },
  timeoutMs: 1_000,
  costUnits: 3,
};

const echoHandler: ToolHandler = async (args) => {
  const { text, count } = args as { text: string; count: number };
  return { text, count };
};

describe('argument validation', () => {
  const cases: Array<{ name: string; args: unknown; expect: string }> = [
    { name: 'wrong type', args: { text: 'hi', count: 'three' }, expect: '$.count: expected integer' },
    { name: 'below minimum', args: { text: 'hi', count: 0 }, expect: '$.count: must be >= 1' },
    { name: 'above maximum', args: { text: 'hi', count: 99 }, expect: '$.count: must be <= 10' },
    { name: 'missing required', args: { text: 'hi' }, expect: '$.count: required' },
    {
      name: 'unknown property',
      args: { text: 'hi', count: 1, nope: true },
      expect: '$.nope: unknown property',
    },
    {
      name: 'nested array element',
      args: { text: 'hi', count: 1, tags: ['a', 7] },
      expect: '$.tags[1]: expected string',
    },
    { name: 'not an object at all', args: 'hi', expect: '$: expected object' },
  ];

  for (const c of cases) {
    it(`rejects ${c.name} and names the path`, async () => {
      const ex = executor();
      ex.register(echoDef, echoHandler);

      const outcome = await ex.invoke(
        'echo',
        c.args,
        toolContext({ ref, role: 'researcher', tool: 'echo', args: c.args }),
      );

      expect(outcome.ok).toBe(false);
      expect(outcome.error?.errorClass).toBe('invalid_args');
      expect(outcome.error?.retryable).toBe(false);
      expect(outcome.error?.message).toContain(c.expect);
    });
  }

  it('reports every error at once rather than the first', async () => {
    const ex = executor();
    ex.register(echoDef, echoHandler);
    const args = { text: '', count: 0, bogus: 1 };

    const outcome = await ex.invoke(
      'echo',
      args,
      toolContext({ ref, role: 'researcher', tool: 'echo', args }),
    );

    const message = outcome.error?.message ?? '';
    expect(message).toContain('$.text');
    expect(message).toContain('$.count');
    expect(message).toContain('$.bogus');
  });

  it('does not call the handler for invalid arguments', async () => {
    const ex = executor();
    let calls = 0;
    ex.register(echoDef, async (args) => {
      calls += 1;
      return args;
    });

    await ex.invoke(
      'echo',
      { text: 'hi' },
      toolContext({ ref, role: 'researcher', tool: 'echo', args: { text: 'hi' } }),
    );

    expect(calls).toBe(0);
  });
});

describe('output validation', () => {
  it('fails with invalid_output when a tool breaks its own contract', async () => {
    const ex = executor();
    ex.register(echoDef, async () => ({ text: 'hi', count: 'not a number' }));
    const args = { text: 'hi', count: 1 };

    const outcome = await ex.invoke(
      'echo',
      args,
      toolContext({ ref, role: 'researcher', tool: 'echo', args }),
    );

    expect(outcome.ok).toBe(false);
    expect(outcome.error?.errorClass).toBe('invalid_output');
    expect(outcome.error?.message).toContain('$.count');
  });

  it('rejects an extra property the output schema does not declare', async () => {
    const ex = executor();
    ex.register(echoDef, async () => ({ text: 'hi', count: 1, secret: 'leaked' }));
    const args = { text: 'hi', count: 1 };

    const outcome = await ex.invoke(
      'echo',
      args,
      toolContext({ ref, role: 'researcher', tool: 'echo', args }),
    );

    expect(outcome.error?.errorClass).toBe('invalid_output');
    expect(outcome.error?.message).toContain('$.secret');
  });
});

describe('timeouts', () => {
  const slowDef: ToolDef = { ...echoDef, name: 'slow', timeoutMs: 30 };

  it('aborts the signal so a cooperating tool can stop', async () => {
    const ex = executor();
    let sawAbort = false;

    ex.register(slowDef, async (_args, ctx) => {
      await new Promise<void>((res) => {
        ctx.signal.addEventListener('abort', () => {
          sawAbort = true;
          res();
        });
      });
      throw new ToolError('timeout', 'stopped on request', true);
    });

    const args = { text: 'hi', count: 1 };
    const outcome = await ex.invoke(
      'slow',
      args,
      toolContext({ ref, role: 'researcher', tool: 'slow', args }),
    );

    expect(sawAbort).toBe(true);
    expect(outcome.error?.errorClass).toBe('timeout');
  });

  it('still times out a tool that ignores the signal', async () => {
    const ex = executor();
    ex.register(slowDef, async () => {
      await new Promise((res) => setTimeout(res, 400));
      return { text: 'late', count: 1 };
    });

    const args = { text: 'hi', count: 1 };
    const started = Date.now();
    const outcome = await ex.invoke(
      'slow',
      args,
      toolContext({ ref, role: 'researcher', tool: 'slow', args }),
    );

    expect(outcome.ok).toBe(false);
    expect(outcome.error?.errorClass).toBe('timeout');
    expect(outcome.error?.retryable).toBe(true);
    expect(outcome.error?.message).toContain('30ms');
    // The executor returned without waiting for the abandoned handler.
    expect(Date.now() - started).toBeLessThan(350);
  });

});

describe('cancellation', () => {
  it('reports a pre-invocation abort as cancelled, and does not call the handler', async () => {
    const ex = executor();
    let calls = 0;
    ex.register(echoDef, async () => {
      calls += 1;
      return { text: 'hi', count: 1 };
    });

    const controller = new AbortController();
    controller.abort();
    const args = { text: 'hi', count: 1 };

    const outcome = await ex.invoke(
      'echo',
      args,
      toolContext({
        ref,
        role: 'researcher',
        tool: 'echo',
        args,
        signal: controller.signal,
      }),
    );

    expect(calls).toBe(0);
    expect(outcome.ok).toBe(false);
    expect(outcome.error?.errorClass).toBe('cancelled');
    // Retrying something the caller asked to stop is the failure this class prevents.
    expect(outcome.error?.retryable).toBe(false);
  });

  it('reports a mid-flight abort as cancelled rather than as a timeout', async () => {
    const ex = executor();
    // A long tool timeout, so the only thing that can end this call is the caller.
    ex.register({ ...echoDef, name: 'long', timeoutMs: 10_000 }, async (_args, ctx) => {
      await new Promise<void>((res) => ctx.signal.addEventListener('abort', () => res()));
      return { text: 'never', count: 1 };
    });

    const controller = new AbortController();
    const args = { text: 'hi', count: 1 };
    setTimeout(() => controller.abort(), 20);

    const outcome = await ex.invoke(
      'long',
      args,
      toolContext({ ref, role: 'researcher', tool: 'long', args, signal: controller.signal }),
    );

    expect(outcome.error?.errorClass).toBe('cancelled');
    expect(outcome.error?.retryable).toBe(false);
    expect(ex.statistics().cancelled).toBe(1);
    expect(ex.statistics().timedOut).toBe(0);
  });

  it('still calls a deadline a timeout when the caller never cancelled', async () => {
    const ex = executor();
    ex.register({ ...echoDef, name: 'brief', timeoutMs: 20 }, async () => {
      await new Promise((res) => setTimeout(res, 300));
      return { text: 'late', count: 1 };
    });
    const args = { text: 'hi', count: 1 };

    const outcome = await ex.invoke(
      'brief',
      args,
      toolContext({ ref, role: 'researcher', tool: 'brief', args }),
    );

    expect(outcome.error?.errorClass).toBe('timeout');
    expect(outcome.error?.retryable).toBe(true);
    expect(ex.statistics().cancelled).toBe(0);
  });

  it('reports an AbortError thrown by a cancelled tool as cancelled', async () => {
    const ex = executor();
    // AbortError is what fetch and the fs streams throw when their signal fires. Both
    // routes out of a cancelled call — the executor's own abort race and the handler's
    // rejection — have to land on the same class, or the reported error depends on
    // which happened to win.
    ex.register({ ...echoDef, name: 'aborty', timeoutMs: 10_000 }, async (_args, ctx) => {
      await new Promise<void>((res) => ctx.signal.addEventListener('abort', () => res()));
      const err = new Error('The operation was aborted');
      err.name = 'AbortError';
      throw err;
    });

    const controller = new AbortController();
    const args = { text: 'hi', count: 1 };
    setTimeout(() => controller.abort(), 20);

    const outcome = await ex.invoke(
      'aborty',
      args,
      toolContext({ ref, role: 'researcher', tool: 'aborty', args, signal: controller.signal }),
    );

    expect(outcome.error?.errorClass).toBe('cancelled');
    expect(outcome.error?.retryable).toBe(false);
  });
});

describe('executor bookkeeping', () => {
  it('never throws for an unregistered tool', async () => {
    const ex = executor();
    const outcome = await ex.invoke(
      'missing',
      {},
      toolContext({ ref, role: 'researcher', tool: 'missing', args: {} }),
    );

    expect(outcome.ok).toBe(false);
    expect(outcome.error?.errorClass).toBe('not_found');
  });

  it('turns a handler throwing a bare Error into a retryable internal error', async () => {
    const ex = executor();
    ex.register(echoDef, async () => {
      throw new Error('kaboom');
    });
    const args = { text: 'hi', count: 1 };

    const outcome = await ex.invoke(
      'echo',
      args,
      toolContext({ ref, role: 'researcher', tool: 'echo', args }),
    );

    expect(outcome.error?.errorClass).toBe('internal');
    expect(outcome.error?.retryable).toBe(true);
    expect(outcome.error?.message).toContain('kaboom');
  });

  it('captures log lines into the outcome', async () => {
    const ex = executor();
    ex.register(echoDef, async (_args, ctx) => {
      ctx.log('starting', { phase: 1 });
      ctx.log('done');
      return { text: 'hi', count: 1 };
    });
    const args = { text: 'hi', count: 1 };

    const outcome = await ex.invoke(
      'echo',
      args,
      toolContext({ ref, role: 'researcher', tool: 'echo', args }),
    );

    expect(outcome.logs.map((l) => l.message)).toEqual(['starting', 'done']);
    expect(outcome.logs[0]?.fields).toEqual({ phase: 1 });
  });

  it('caps captured log lines and says how many were dropped', async () => {
    const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy(), maxLogLines: 3 });
    ex.register(echoDef, async (_args, ctx) => {
      for (let i = 0; i < 10; i += 1) ctx.log(`line ${i}`);
      return { text: 'hi', count: 1 };
    });
    const args = { text: 'hi', count: 1 };

    const outcome = await ex.invoke(
      'echo',
      args,
      toolContext({ ref, role: 'researcher', tool: 'echo', args }),
    );

    expect(outcome.logs).toHaveLength(4);
    expect(outcome.logs[3]?.message).toContain('7 further log lines dropped');
  });

  it('charges cost units per tenant only when the handler runs', async () => {
    const ex = executor();
    ex.register(echoDef, echoHandler);

    for (const attempt of [1, 2]) {
      const args = { text: 'hi', count: attempt };
      await ex.invoke(
        'echo',
        args,
        toolContext({
          ref: { ...ref, attempt },
          role: 'researcher',
          tool: 'echo',
          args,
          tenant: 'acme',
        }),
      );
    }
    // Rejected before invocation, so it costs nothing.
    await ex.invoke(
      'echo',
      { text: 'hi' },
      toolContext({ ref, role: 'researcher', tool: 'echo', args: { text: 'hi' }, tenant: 'acme' }),
    );

    expect(ex.costFor('acme')).toEqual({ tenant: 'acme', units: 6, calls: 2 });
    expect(ex.costFor('other').units).toBe(0);
  });

  it('refuses to register the same tool name twice', () => {
    const ex = executor();
    ex.register(echoDef, echoHandler);
    expect(() => ex.register(echoDef, echoHandler)).toThrow(/already registered/);
  });

  it('lists what it knows about', () => {
    const ex = executor();
    const flaky = createFlakyTool();
    ex.register(echoDef, echoHandler);
    ex.register(flaky.def, flaky.handler);

    expect(ex.list().map((d) => d.name).sort()).toEqual(['echo', 'flaky']);
    expect(ex.get('echo')?.timeoutMs).toBe(1_000);
    expect(ex.get('nope')).toBeUndefined();
  });
});

describe('path-confined filesystem tool', () => {
  async function fixture(): Promise<{ root: string; outside: string }> {
    const base = await mkdtemp(join(tmpdir(), 'parliament-fs-'));
    const root = join(base, 'root');
    const outside = join(base, 'outside');
    await mkdir(root, { recursive: true });
    await mkdir(outside, { recursive: true });
    await writeFile(join(root, 'inside.txt'), 'allowed', 'utf8');
    await writeFile(join(outside, 'secret.txt'), 'forbidden', 'utf8');
    return { root, outside };
  }

  it('reads a file inside the root', async () => {
    const { root } = await fixture();
    const tool = createFsReadTool({ root });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { path: 'inside.txt' };

    const outcome = await ex.invoke(
      'fs.read',
      args,
      toolContext({ ref, role: 'researcher', tool: 'fs.read', args }),
    );

    expect(outcome.ok).toBe(true);
    expect((outcome.result as { contents: string }).contents).toBe('allowed');
  });

  it('rejects a ../ escape', async () => {
    const { root } = await fixture();
    const tool = createFsReadTool({ root });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { path: '../outside/secret.txt' };

    const outcome = await ex.invoke(
      'fs.read',
      args,
      toolContext({ ref, role: 'researcher', tool: 'fs.read', args }),
    );

    expect(outcome.ok).toBe(false);
    expect(outcome.error?.errorClass).toBe('permission_denied');
    expect(outcome.error?.message).toContain('escapes the root');
  });

  it('rejects a symlink pointing out of the root', async () => {
    const { root, outside } = await fixture();
    await symlink(join(outside, 'secret.txt'), join(root, 'link.txt'));
    const tool = createFsReadTool({ root });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { path: 'link.txt' };

    const outcome = await ex.invoke(
      'fs.read',
      args,
      toolContext({ ref, role: 'researcher', tool: 'fs.read', args }),
    );

    expect(outcome.ok).toBe(false);
    expect(outcome.error?.errorClass).toBe('permission_denied');
    expect(outcome.error?.message).toContain('escapes the root');
  });

  it('rejects an absolute path rather than re-rooting it', async () => {
    const { root, outside } = await fixture();
    const tool = createFsReadTool({ root });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { path: join(outside, 'secret.txt') };

    const outcome = await ex.invoke(
      'fs.read',
      args,
      toolContext({ ref, role: 'researcher', tool: 'fs.read', args }),
    );

    expect(outcome.error?.errorClass).toBe('permission_denied');
    expect(outcome.error?.message).toContain('must be relative');
  });

  it('reports a missing file as not_found', async () => {
    const { root } = await fixture();
    const tool = createFsReadTool({ root });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { path: 'nope.txt' };

    const outcome = await ex.invoke(
      'fs.read',
      args,
      toolContext({ ref, role: 'researcher', tool: 'fs.read', args }),
    );

    expect(outcome.error?.errorClass).toBe('not_found');
  });

  it('confines the write tool to the root as well', async () => {
    const { root } = await fixture();
    const tool = createFileWriteTool({ root });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { path: '../outside/escaped.txt', contents: 'x' };

    const outcome = await ex.invoke(
      'fs.write',
      args,
      toolContext({ ref, role: 'operator', tool: 'fs.write', args }),
    );

    expect(outcome.error?.errorClass).toBe('permission_denied');
  });
});

describe('http tool allowlist', () => {
  const okFetch = async (): Promise<{ status: number; text(): Promise<string> }> => ({
    status: 200,
    text: async () => 'body',
  });

  it('rejects a host that is not allowlisted, without calling fetch', async () => {
    let fetched = 0;
    const tool = createHttpFetchTool({
      allowedHosts: ['docs.example.com'],
      fetchImpl: async () => {
        fetched += 1;
        return okFetch();
      },
    });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { url: 'https://evil.test/data' };

    const outcome = await ex.invoke(
      'http.fetch',
      args,
      toolContext({ ref, role: 'researcher', tool: 'http.fetch', args }),
    );

    expect(fetched).toBe(0);
    expect(outcome.error?.errorClass).toBe('permission_denied');
    expect(outcome.error?.message).toContain('evil.test');
  });

  it('is not fooled by an allowlisted name in the userinfo', async () => {
    const tool = createHttpFetchTool({
      allowedHosts: ['docs.example.com'],
      fetchImpl: async () => okFetch(),
    });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { url: 'https://docs.example.com@evil.test/data' };

    const outcome = await ex.invoke(
      'http.fetch',
      args,
      toolContext({ ref, role: 'researcher', tool: 'http.fetch', args }),
    );

    expect(outcome.error?.errorClass).toBe('permission_denied');
  });

  it('is not fooled by a suffix of an allowlisted host', async () => {
    const tool = createHttpFetchTool({
      allowedHosts: ['example.com'],
      fetchImpl: async () => okFetch(),
    });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { url: 'https://notexample.com/data' };

    const outcome = await ex.invoke(
      'http.fetch',
      args,
      toolContext({ ref, role: 'researcher', tool: 'http.fetch', args }),
    );

    expect(outcome.error?.errorClass).toBe('permission_denied');
  });

  it('rejects a non-http scheme', async () => {
    const tool = createHttpFetchTool({
      allowedHosts: ['example.com'],
      fetchImpl: async () => okFetch(),
    });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { url: 'file:///etc/passwd' };

    const outcome = await ex.invoke(
      'http.fetch',
      args,
      toolContext({ ref, role: 'researcher', tool: 'http.fetch', args }),
    );

    expect(outcome.error?.message).toContain('scheme');
  });

  it('fetches an allowlisted host and truncates an oversized body', async () => {
    const tool = createHttpFetchTool({
      allowedHosts: ['docs.example.com'],
      maxBytes: 4,
      fetchImpl: async () => ({ status: 200, text: async () => 'abcdefgh' }),
    });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { url: 'https://docs.example.com/page' };

    const outcome = await ex.invoke(
      'http.fetch',
      args,
      toolContext({ ref, role: 'researcher', tool: 'http.fetch', args }),
    );

    expect(outcome.ok).toBe(true);
    expect(outcome.result).toEqual({ status: 200, body: 'abcd', truncated: true });
  });

  it('classifies a transport failure as retryable unavailable', async () => {
    const tool = createHttpFetchTool({
      allowedHosts: ['docs.example.com'],
      fetchImpl: async () => {
        throw new Error('ECONNRESET');
      },
    });
    const ex = executor();
    ex.register(tool.def, tool.handler);
    const args = { url: 'https://docs.example.com/page' };

    const outcome = await ex.invoke(
      'http.fetch',
      args,
      toolContext({ ref, role: 'researcher', tool: 'http.fetch', args }),
    );

    expect(outcome.error?.errorClass).toBe('unavailable');
    expect(outcome.error?.retryable).toBe(true);
  });
});

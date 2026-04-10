/**
 * Concrete tools, enough to exercise every layer with something that really does I/O.
 *
 * Each is a factory rather than a constant. A URL allowlist and a filesystem root are
 * deployment configuration, and a tool whose boundary is baked in at module scope can
 * only be tested against whatever the author happened to hard-code — which is how
 * allowlists end up containing a test host in production.
 */

import { createReadStream } from 'node:fs';
import { realpath, stat } from 'node:fs/promises';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, isAbsolute, join, resolve, sep } from 'node:path';
import type { ToolContext, ToolDef, ToolHandler } from './types.js';
import { ToolError } from './types.js';

export interface Tool {
  readonly def: ToolDef;
  readonly handler: ToolHandler;
}

/** Registers a tool pair against anything with the executor's `register` shape. */
export function registerAll(
  target: { register(def: ToolDef, handler: ToolHandler): void },
  tools: readonly Tool[],
): void {
  for (const tool of tools) target.register(tool.def, tool.handler);
}

type FetchLike = (url: string, init: { signal: AbortSignal }) => Promise<{
  status: number;
  text(): Promise<string>;
}>;

export interface HttpFetchOptions {
  /** Hosts this tool may talk to, matched exactly against the URL's hostname. */
  readonly allowedHosts: readonly string[];
  readonly allowedRoles?: readonly string[];
  readonly maxBytes?: number;
  readonly timeoutMs?: number;

  /** Injected in tests. Production passes nothing and gets the global `fetch`. */
  readonly fetchImpl?: FetchLike;
}

/**
 * HTTP GET against an allowlisted host.
 *
 * The allowlist is on the *host*, checked after parsing, and matched exactly. Matching
 * a suffix would accept `evil-example.com` for an allowlist entry of `example.com`;
 * matching the raw string before parsing would accept
 * `https://allowed.example.com@evil.test/` because the allowed name is in the userinfo
 * rather than the authority.
 *
 * A redirect is not followed. A 302 to a host outside the allowlist would defeat the
 * check entirely, and resolving that safely means re-validating each hop — so the
 * response is returned as-is and the caller decides whether to issue a second call.
 */
export function createHttpFetchTool(options: HttpFetchOptions): Tool {
  const allowed = new Set(options.allowedHosts.map((h) => h.toLowerCase()));
  const maxBytes = options.maxBytes ?? 256 * 1024;

  const def: ToolDef = {
    name: 'http.fetch',
    description: 'HTTP GET a URL on the configured allowlist and return its body as text.',
    input: {
      type: 'object',
      properties: {
        url: { type: 'string', minLength: 1, maxLength: 2048 },
      },
      required: ['url'],
    },
    output: {
      type: 'object',
      properties: {
        status: { type: 'integer', minimum: 100, maximum: 599 },
        body: { type: 'string' },
        truncated: { type: 'boolean' },
      },
      required: ['status', 'body', 'truncated'],
    },
    permissions: {
      effect: 'read',
      allowedRoles: options.allowedRoles ?? ['researcher', 'operator'],
      requiresApproval: false,
      idempotent: true,
    },
    timeoutMs: options.timeoutMs ?? 10_000,
    costUnits: 1,
  };

  const handler: ToolHandler = async (args, ctx) => {
    const { url } = args as { url: string };

    let parsed: URL;
    try {
      parsed = new URL(url);
    } catch {
      throw new ToolError('invalid_args', `not a valid URL: ${url}`, false);
    }
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
      throw new ToolError('permission_denied', `scheme "${parsed.protocol}" is not allowed`, false);
    }
    if (!allowed.has(parsed.hostname.toLowerCase())) {
      throw new ToolError(
        'permission_denied',
        `host "${parsed.hostname}" is not on the allowlist [${[...allowed].join(', ')}]`,
        false,
      );
    }

    const doFetch: FetchLike = options.fetchImpl ?? (globalThis.fetch as unknown as FetchLike);
    ctx.log('http.fetch', { host: parsed.hostname });

    let response: { status: number; text(): Promise<string> };
    try {
      response = await doFetch(parsed.toString(), { signal: ctx.signal });
    } catch (err) {
      // A transport failure is retryable; the request never got an answer, so the
      // caller cannot tell a dropped connection from a service that is simply down.
      throw new ToolError('unavailable', `fetch failed: ${describe(err)}`, true);
    }

    const text = await response.text();
    const truncated = text.length > maxBytes;
    return { status: response.status, body: truncated ? text.slice(0, maxBytes) : text, truncated };
  };

  return { def, handler };
}

export interface FsReadOptions {
  readonly root: string;
  readonly allowedRoles?: readonly string[];
  readonly maxBytes?: number;
}

/**
 * Reads a file, confined to a root directory.
 *
 * Confinement is enforced on the **resolved real path**, not on the requested one.
 * Rejecting strings containing `..` is not sufficient: the canonical escape is a
 * symlink inside the root pointing outside it, where every path component looks
 * innocent. `realpath` resolves the links first, so the prefix check sees where the
 * read would actually land.
 *
 * The root is itself resolved once, because a root given as a symlink would otherwise
 * never prefix-match its own resolved children and every read would be refused.
 */
export function createFsReadTool(options: FsReadOptions): Tool {
  const maxBytes = options.maxBytes ?? 1024 * 1024;

  const def: ToolDef = {
    name: 'fs.read',
    description: 'Read a UTF-8 text file from within the configured root directory.',
    input: {
      type: 'object',
      properties: {
        path: { type: 'string', minLength: 1, maxLength: 4096 },
      },
      required: ['path'],
    },
    output: {
      type: 'object',
      properties: {
        path: { type: 'string' },
        contents: { type: 'string' },
        bytes: { type: 'integer', minimum: 0 },
      },
      required: ['path', 'contents', 'bytes'],
    },
    permissions: {
      effect: 'read',
      allowedRoles: options.allowedRoles ?? ['researcher', 'operator'],
      requiresApproval: false,
      idempotent: true,
    },
    timeoutMs: 5_000,
    costUnits: 0,
  };

  const handler: ToolHandler = async (args, ctx) => {
    const { path: requested } = args as { path: string };
    const real = await confine(options.root, requested);

    const info = await stat(real);
    if (!info.isFile()) {
      throw new ToolError('invalid_args', `${requested} is not a regular file`, false);
    }
    if (info.size > maxBytes) {
      throw new ToolError(
        'invalid_args',
        `${requested} is ${info.size} bytes, over the ${maxBytes} byte limit`,
        false,
      );
    }

    ctx.log('fs.read', { bytes: info.size });
    const contents = await readAllText(real, ctx.signal);
    return { path: requested, contents, bytes: Buffer.byteLength(contents, 'utf8') };
  };

  return { def, handler };
}

export interface FileWriteOptions {
  readonly root: string;
  readonly allowedRoles?: readonly string[];
}

/**
 * Writes a file under a root. `effect: 'write'`, `requiresApproval: true`.
 *
 * Here to make the approval and escalation paths testable against a tool that really
 * does change something. Marked idempotent because the write is a full overwrite at a
 * fixed path: repeating it with the same arguments converges on the same state, which
 * is the actual definition rather than "it is a write so it must not be repeated".
 */
export function createFileWriteTool(options: FileWriteOptions): Tool {
  const def: ToolDef = {
    name: 'fs.write',
    description: 'Write a UTF-8 text file within the configured root directory.',
    input: {
      type: 'object',
      properties: {
        path: { type: 'string', minLength: 1, maxLength: 4096 },
        contents: { type: 'string', maxLength: 1_000_000 },
      },
      required: ['path', 'contents'],
    },
    output: {
      type: 'object',
      properties: {
        path: { type: 'string' },
        bytes: { type: 'integer', minimum: 0 },
      },
      required: ['path', 'bytes'],
    },
    permissions: {
      effect: 'write',
      allowedRoles: options.allowedRoles ?? ['operator'],
      requiresApproval: true,
      idempotent: true,
    },
    timeoutMs: 5_000,
    costUnits: 1,
  };

  const handler: ToolHandler = async (args, ctx) => {
    const { path: requested, contents } = args as { path: string; contents: string };
    // The target may not exist yet, so its parent is what gets confined: resolving a
    // non-existent path with `realpath` would fail before any check could run.
    const parent = await confine(options.root, dirname(requested));
    const target = join(parent, baseNameOf(requested));

    await mkdir(parent, { recursive: true });
    await writeFile(target, contents, 'utf8');
    ctx.log('fs.write', { bytes: Buffer.byteLength(contents, 'utf8') });
    return { path: requested, bytes: Buffer.byteLength(contents, 'utf8') };
  };

  return { def, handler };
}

export type FlakyMode = 'succeed' | 'timeout' | 'unavailable' | 'invalid_output' | 'throw';

export interface FlakyTool extends Tool {
  /** How many times the handler body has actually started. */
  calls(): number;

  /** Fails the next `n` calls with `mode`, then succeeds. */
  failNext(n: number, mode: Exclude<FlakyMode, 'succeed'>): void;

  reset(): void;
}

/**
 * A tool whose failure class is dictated by the test.
 *
 * The call counter is the reason this exists: "the handler ran exactly once" is the
 * only assertion that actually proves deduplication, and it cannot be made by
 * inspecting results, since a correctly deduplicated call and a duplicated one return
 * the same value.
 */
export function createFlakyTool(name = 'flaky'): FlakyTool {
  let calls = 0;
  let remaining = 0;
  let mode: FlakyMode = 'succeed';

  const def: ToolDef = {
    name,
    description: 'Test tool with a controllable failure class.',
    input: {
      type: 'object',
      properties: {
        label: { type: 'string' },
        n: { type: 'integer', minimum: 0, maximum: 100 },
      },
      required: ['label'],
    },
    output: {
      type: 'object',
      properties: {
        label: { type: 'string' },
        attempt: { type: 'integer', minimum: 1 },
      },
      required: ['label', 'attempt'],
    },
    permissions: {
      effect: 'read',
      allowedRoles: ['researcher', 'operator'],
      requiresApproval: false,
      idempotent: true,
    },
    timeoutMs: 50,
    costUnits: 2,
  };

  const handler: ToolHandler = async (args, ctx) => {
    calls += 1;
    const { label } = args as { label: string };

    if (remaining > 0) {
      remaining -= 1;
      switch (mode) {
        case 'timeout':
          // Ignores the signal on purpose: the executor must still report a timeout for
          // a tool that refuses to stop, which is the common real-world case.
          await new Promise((r) => setTimeout(r, def.timeoutMs * 20));
          break;
        case 'unavailable':
          throw new ToolError('unavailable', `${name} is unavailable`, true);
        case 'invalid_output':
          return { label, attempt: 'not-a-number' };
        case 'throw':
          throw new Error(`${name} exploded`);
        case 'succeed':
          break;
      }
    }

    ctx.log(`${name} succeeded`, { call: calls });
    return { label, attempt: calls };
  };

  return {
    def,
    handler,
    calls: () => calls,
    failNext: (n, m) => {
      remaining = n;
      mode = m;
    },
    reset: () => {
      calls = 0;
      remaining = 0;
      mode = 'succeed';
    },
  };
}

/**
 * Resolves `requested` against `root` and rejects anything landing outside it.
 *
 * Absolute inputs are rejected rather than re-rooted. Silently rewriting `/etc/passwd`
 * into `<root>/etc/passwd` would succeed at a path the caller did not ask for, and a
 * caller that believes it read `/etc/passwd` is worse off than one that got an error.
 */
async function confine(root: string, requested: string): Promise<string> {
  if (isAbsolute(requested)) {
    throw new ToolError('permission_denied', `path must be relative to the root: ${requested}`, false);
  }

  const realRoot = await realpath(resolve(root));
  const candidate = resolve(realRoot, requested);

  let real: string;
  try {
    real = await realpath(candidate);
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === 'ENOENT') {
      throw new ToolError('not_found', `no such path: ${requested}`, false);
    }
    throw new ToolError('internal', `cannot resolve ${requested}: ${describe(err)}`, false);
  }

  if (real !== realRoot && !real.startsWith(realRoot + sep)) {
    throw new ToolError(
      'permission_denied',
      `path escapes the root: ${requested} resolves outside ${realRoot}`,
      false,
    );
  }
  return real;
}

/** The final component of a relative path, with no directory traversal left in it. */
function baseNameOf(requested: string): string {
  const parts = requested.split(/[\\/]+/).filter((p) => p.length > 0 && p !== '.');
  const last = parts[parts.length - 1];
  if (last === undefined || last === '..') {
    throw new ToolError('invalid_args', `no file name in path: ${requested}`, false);
  }
  return last;
}

/**
 * Streams a file to a string, aborting if the signal fires.
 *
 * Streamed rather than `readFile`d so a cancellation is honoured partway through a
 * large read instead of after it, which is the difference between the step's timeout
 * meaning something and being advisory.
 */
async function readAllText(path: string, signal: AbortSignal): Promise<string> {
  const chunks: Buffer[] = [];
  const stream = createReadStream(path, { encoding: undefined });

  return await new Promise<string>((res, rej) => {
    const onAbort = (): void => {
      stream.destroy();
      rej(new ToolError('timeout', `read of ${path} was aborted`, true));
    };
    signal.addEventListener('abort', onAbort, { once: true });

    stream.on('data', (chunk) => chunks.push(chunk as Buffer));
    stream.on('error', (err) => {
      signal.removeEventListener('abort', onAbort);
      rej(new ToolError('internal', `read failed: ${err.message}`, true));
    });
    stream.on('end', () => {
      signal.removeEventListener('abort', onAbort);
      res(Buffer.concat(chunks).toString('utf8'));
    });
  });
}

function describe(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Convenience: the tool context fields a handler needs, for direct unit tests. */
export type MinimalToolContext = Pick<ToolContext, 'signal' | 'log'>;

/**
 * Out-of-process execution for tool code this process does not trust to behave.
 *
 * ## What this contains, stated plainly
 *
 * This bounds **accidents**, not **adversaries**.
 *
 * A child Node process is not a security boundary. It runs as the same user, on the
 * same filesystem, with the same network access, and with the whole Node standard
 * library available: code running in it can read `~/.ssh`, open sockets, and spawn its
 * own children. Clearing the environment removes credentials that happen to live in
 * environment variables and nothing more. Nothing here prevents hostile code from
 * doing anything this process could do.
 *
 * What it does contain:
 *
 * - **A runaway loop** cannot hang the orchestrator. The wall-clock timer fires in the
 *   parent and `SIGKILL` ends the child, which a `while (true)` in-process could not be
 *   made to respect — JavaScript has no preemption, so an unyielding loop on the main
 *   thread is unkillable without a separate OS process.
 * - **A crash** cannot take the parent down. A segfault, an uncaught throw, or a heap
 *   OOM is observed as an exit code and converted into a `ToolError`.
 * - **A memory leak** is bounded to the child's `--max-old-space-size`, so one tool
 *   cannot exhaust the orchestrator's heap. This bounds the V8 old space, not total
 *   RSS: large `Buffer`s, WASM memory, and native allocations sit outside it.
 * - **Garbage output** cannot corrupt state. Only a JSON round-trip crosses the
 *   boundary, so no object identity, prototype, or function reference from the child
 *   can reach the parent.
 *
 * Real isolation against untrusted code needs a VM, a container with seccomp and
 * dropped capabilities, or a per-tenant microVM — something that can actually deny
 * filesystem and network access. If this project ever runs code supplied by a tenant,
 * that is the requirement, and this file must not be mistaken for having met it.
 *
 * ## Why a child process rather than a worker thread
 *
 * `worker_threads` would be cheaper to start and has `resourceLimits` for memory. It
 * is the wrong choice here for two reasons. A worker shares the process: a native
 * crash or an OOM in the shared allocator takes the parent with it, and
 * `worker.terminate()` cannot interrupt a tight synchronous loop promptly in all cases
 * because termination is cooperative at the V8 level. A separate process can always be
 * killed by the kernel, which is the one guarantee this layer is actually offering.
 * The extra tens of milliseconds of startup cost is irrelevant next to the network
 * calls these tools make.
 */

import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { ToolError } from './types.js';

/**
 * The child half of the protocol.
 *
 * Shipped as a string to `node -e` rather than as a file on disk so the sandbox has no
 * build-output dependency: there is no compiled artifact to locate at runtime and no
 * way for the bootstrap and the parent to drift out of step across a packaging change.
 *
 * The child never exits on its own. The parent kills it once a reply has been read,
 * which removes a race where `process.exit` truncates an in-flight IPC write.
 */
const BOOTSTRAP = [
  "process.on('message', async (raw) => {",
  '  let reply;',
  '  try {',
  '    const req = JSON.parse(raw);',
  "    const url = 'data:text/javascript;base64,' + Buffer.from(req.source, 'utf8').toString('base64');",
  '    const mod = await import(url);',
  '    const fn = mod.default;',
  "    if (typeof fn !== 'function') throw new Error('sandboxed module has no default export function');",
  '    const value = await fn(req.args);',
  '    const kind = typeof value;',
  "    if (kind === 'undefined' || kind === 'function' || kind === 'symbol' || kind === 'bigint') {",
  "      throw new Error('handler returned a non-serializable ' + kind);",
  '    }',
  '    const json = JSON.stringify(value);',
  "    if (typeof json !== 'string') throw new Error('handler returned a non-serializable value');",
  '    reply = JSON.stringify({ ok: true, json: json });',
  '  } catch (err) {',
  '    const message = err && err.message ? String(err.message) : String(err);',
  '    reply = JSON.stringify({ ok: false, message: message });',
  '  }',
  '  try { process.send(reply); } catch (sendErr) { process.exit(70); }',
  '});',
  // An unhandled rejection anywhere in the loaded module would otherwise terminate the
  // child with no reply, which the parent can only report as an opaque crash.
  "process.on('unhandledRejection', (err) => {",
  "  try { process.send(JSON.stringify({ ok: false, message: 'unhandled rejection: ' + String(err) })); } catch (e) {}",
  '});',
].join('\n');

export interface SandboxOptions {
  /** Wall-clock budget. The child is `SIGKILL`ed when it expires. */
  readonly timeoutMs: number;

  /** V8 old-space cap for the child, in megabytes. */
  readonly memoryMb?: number;

  /** Kills the child early; used to propagate a step's cancellation. */
  readonly signal?: AbortSignal;

  /** Cap on captured output lines, so a child that prints in a loop cannot grow memory. */
  readonly maxOutputLines?: number;
}

export interface SandboxOutcome {
  readonly ok: boolean;
  readonly value?: unknown;
  readonly error?: ToolError;

  /**
   * Whatever the child wrote to its own stdout and stderr. Captured rather than
   * inherited: a `console.log` in tool code must not interleave with the
   * orchestrator's structured logs, and it must not be mistaken for a return value —
   * which is why the result travels over IPC on a separate descriptor.
   */
  readonly stdout: readonly string[];
  readonly stderr: readonly string[];
  readonly durationMs: number;
}

interface ChildReply {
  readonly ok: boolean;
  readonly json?: string;
  readonly message?: string;
}

/**
 * Runs `source` — an ES module whose default export is `(args) => result` — in a fresh
 * child process and returns its JSON-decoded result.
 *
 * Never rejects. Every failure mode (crash, hang, non-serializable return, malformed
 * reply, spawn failure) comes back as an outcome carrying a `ToolError`, because the
 * point of pushing this work out of process is that the parent keeps running.
 */
export async function runInSandbox(
  source: string,
  args: unknown,
  options: SandboxOptions,
): Promise<SandboxOutcome> {
  const started = Date.now();
  const maxLines = options.maxOutputLines ?? 100;
  const stdout: string[] = [];
  const stderr: string[] = [];

  const nodeArgs = ['--input-type=module'];
  if (options.memoryMb !== undefined) {
    nodeArgs.push(`--max-old-space-size=${options.memoryMb}`);
  }
  nodeArgs.push('-e', BOOTSTRAP);

  let child: ChildProcess;
  try {
    child = spawn(process.execPath, nodeArgs, {
      // No inherited environment. Credentials and endpoint configuration reach this
      // process through its environment, and tool code has no business reading them.
      env: {},
      // fd 3 is the IPC channel. Keeping the result off stdout means a child that
      // prints cannot be confused for one that returned.
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      detached: false,
    });
  } catch (err) {
    return {
      ok: false,
      error: new ToolError('internal', `sandbox could not start: ${message(err)}`, true),
      stdout,
      stderr,
      durationMs: Date.now() - started,
    };
  }

  const collect = (sink: string[]) => (chunk: Buffer | string): void => {
    if (sink.length >= maxLines) return;
    for (const line of String(chunk).split('\n')) {
      if (line.length === 0) continue;
      if (sink.length >= maxLines) {
        sink.push('<output truncated>');
        return;
      }
      sink.push(line);
    }
  };
  child.stdout?.on('data', collect(stdout));
  child.stderr?.on('data', collect(stderr));

  const finish = await new Promise<Omit<SandboxOutcome, 'stdout' | 'stderr' | 'durationMs'>>(
    (resolve) => {
      let settled = false;
      const done = (outcome: Omit<SandboxOutcome, 'stdout' | 'stderr' | 'durationMs'>): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        options.signal?.removeEventListener('abort', onAbort);
        // SIGKILL rather than SIGTERM: a child that installed a SIGTERM handler could
        // decline to die, and the whole point of the wall clock is that it is not
        // negotiable.
        child.kill('SIGKILL');
        resolve(outcome);
      };

      const timer = setTimeout(() => {
        done({
          ok: false,
          error: new ToolError(
            'timeout',
            `sandboxed handler exceeded ${options.timeoutMs}ms and was killed`,
            true,
          ),
        });
      }, options.timeoutMs);
      timer.unref?.();

      const onAbort = (): void => {
        done({ ok: false, error: ToolError.cancelled('sandboxed handler') });
      };
      if (options.signal?.aborted) {
        onAbort();
        return;
      }
      options.signal?.addEventListener('abort', onAbort, { once: true });

      child.on('message', (raw) => {
        if (typeof raw !== 'string') {
          done({
            ok: false,
            error: new ToolError('internal', 'sandbox sent a non-string frame', false),
          });
          return;
        }
        let reply: ChildReply;
        try {
          reply = JSON.parse(raw) as ChildReply;
        } catch {
          done({
            ok: false,
            error: new ToolError('internal', 'sandbox sent a malformed frame', false),
          });
          return;
        }
        if (!reply.ok) {
          done({
            ok: false,
            error: new ToolError(
              'internal',
              `sandboxed handler failed: ${reply.message ?? 'no detail'}`,
              false,
            ),
          });
          return;
        }
        if (typeof reply.json !== 'string') {
          done({
            ok: false,
            error: new ToolError('internal', 'sandbox reported success with no payload', false),
          });
          return;
        }
        try {
          done({ ok: true, value: JSON.parse(reply.json) as unknown });
        } catch {
          done({
            ok: false,
            error: new ToolError('internal', 'sandbox payload was not valid JSON', false),
          });
        }
      });

      child.on('error', (err) => {
        done({
          ok: false,
          error: new ToolError('internal', `sandbox process error: ${err.message}`, true),
        });
      });

      // Reached when the child died without replying: an uncaught throw, an OOM, or a
      // signal. Retryable, because a crash of unknown cause is as likely to be
      // environmental as deterministic, and the caller's retry policy decides.
      child.on('exit', (code, signalName) => {
        done({
          ok: false,
          error: new ToolError(
            'internal',
            `sandbox exited without a result (code=${String(code)} signal=${String(signalName)})`,
            true,
          ),
        });
      });

      child.send(JSON.stringify({ source, args: args ?? null }), (err) => {
        if (err) {
          done({
            ok: false,
            error: new ToolError('internal', `sandbox send failed: ${err.message}`, true),
          });
        }
      });
    },
  );

  return { ...finish, stdout, stderr, durationMs: Date.now() - started };
}

/**
 * Wraps sandboxed source as a `ToolHandler`.
 *
 * Throws a `ToolError` on failure rather than returning one, because that is what the
 * executor's catch expects: it is the component that owns turning errors into
 * outcomes, and having two components both doing it would mean two places to keep the
 * classification consistent.
 */
export function sandboxedHandler(
  source: string,
  options: { readonly timeoutMs: number; readonly memoryMb?: number },
): (args: unknown, ctx: { readonly signal: AbortSignal }) => Promise<unknown> {
  return async (args, ctx) => {
    const outcome = await runInSandbox(source, args, {
      timeoutMs: options.timeoutMs,
      ...(options.memoryMb !== undefined ? { memoryMb: options.memoryMb } : {}),
      signal: ctx.signal,
    });
    if (!outcome.ok) {
      throw outcome.error ?? new ToolError('internal', 'sandbox failed without an error', true);
    }
    return outcome.value;
  };
}

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

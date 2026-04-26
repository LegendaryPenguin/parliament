/**
 * The tools the demo's workflows call.
 *
 * Every one of them has an *observable* side effect outside the control plane — a line in a
 * file, a counter on disk — and that is the whole design of this file. The demo has to prove
 * things like "the step was not executed twice", and a tool that only returns a value cannot
 * prove it: a deduplicated call and a duplicated call return the same thing. A line in a file
 * can be counted from the shell afterwards.
 *
 * They are deliberately not the built-in tools from `src/tools/builtin.ts`. Those are the
 * real ones, with a URL allowlist and a confined filesystem root, and they are exercised by
 * the test suite. What the demo needs is tools whose behaviour it can dictate — fail twice
 * then succeed, hold a lease for four seconds — and baking that into a production tool so a
 * demo can drive it would be the wrong trade.
 *
 * Note the input schemas. The control plane binds a step's arguments from the output of the
 * step it depends on (`bindInput` in `src/scheduler/worker-port.ts`), and the executor
 * validates those arguments against the tool's declared input schema with unknown properties
 * rejected. So a tool that sits downstream of another has to accept that upstream shape, which
 * is why the consuming tools set `additionalProperties: true` and read only the fields they
 * need. The alternative — making every output exactly match the next input — is what a real
 * deployment should do, and it would make the demo's graph rigid for no gain.
 */

import { appendFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import type { Tool } from '../src/tools/builtin.js';
import { ToolError } from '../src/tools/types.js';

export interface DemoToolOptions {
  /** File every `demo.ledger` call appends a line to. The idempotency proof counts them. */
  readonly ledgerPath: string;
  /** Directory the flaky tool keeps its per-label attempt counters in. */
  readonly statePath: string;
}

export function demoTools(options: DemoToolOptions): readonly Tool[] {
  return [
    extractTool(),
    screenTool(),
    holdTool(),
    ledgerTool(options.ledgerPath),
    flakyTool(options.statePath),
  ];
}

/**
 * Turns a record id into something an `llm` node can consume.
 *
 * Its output is shaped as an `LlmNodeInput` because the next node in the demo graph is an
 * `llm` node, and the control plane hands a step its dependency's output verbatim. Building
 * the prompt in a tool rather than in the definition is also the honest arrangement: a prompt
 * that depends on fetched data cannot live in a static graph.
 */
function extractTool(): Tool {
  return {
    def: {
      name: 'demo.extract',
      description: 'Read a record and build the prompt the next step will send to a model.',
      input: {
        type: 'object',
        properties: { recordId: { type: 'string', minLength: 1, maxLength: 128 } },
        required: ['recordId'],
        additionalProperties: true,
      },
      output: {
        type: 'object',
        properties: {
          recordId: { type: 'string' },
          context: {
            type: 'object',
            properties: { prompt: { type: 'string' }, system: { type: 'string' } },
            required: ['prompt'],
          },
        },
        required: ['recordId', 'context'],
      },
      permissions: {
        effect: 'read',
        allowedRoles: ['worker', 'operator'],
        requiresApproval: false,
        idempotent: true,
      },
      timeoutMs: 5_000,
      costUnits: 1,
    },
    handler: async (args, ctx) => {
      const { recordId } = args as { recordId: string };
      ctx.log('demo.extract', { recordId });
      return {
        recordId,
        context: {
          system: 'You classify support records.',
          prompt: `Classify record ${recordId} and return a JSON object.`,
        },
      };
    },
  };
}

/** A cheap deterministic risk score, so the diamond has a second branch that does work. */
function screenTool(): Tool {
  return {
    def: {
      name: 'demo.screen',
      description: 'Score a record for risk, deterministically, from its id.',
      input: {
        type: 'object',
        properties: { recordId: { type: 'string', minLength: 1 } },
        required: ['recordId'],
        additionalProperties: true,
      },
      output: {
        type: 'object',
        properties: {
          recordId: { type: 'string' },
          score: { type: 'number', minimum: 0, maximum: 1 },
          band: { type: 'string', enum: ['low', 'medium', 'high'] },
        },
        required: ['recordId', 'score', 'band'],
      },
      permissions: {
        effect: 'read',
        allowedRoles: ['worker', 'operator'],
        requiresApproval: false,
        idempotent: true,
      },
      timeoutMs: 5_000,
      costUnits: 1,
    },
    handler: async (args, ctx) => {
      const { recordId } = args as { recordId: string };
      // Derived from the id rather than from `Math.random`, so two runs of the demo produce
      // the same numbers and a reader comparing two runs is comparing the control plane
      // rather than the dice.
      let hash = 0;
      for (const char of recordId) hash = (hash * 31 + char.charCodeAt(0)) % 1_000;
      const score = Number((hash / 1_000).toFixed(3));
      ctx.log('demo.screen', { score });
      return { recordId, score, band: score > 0.66 ? 'high' : score > 0.33 ? 'medium' : 'low' };
    },
  };
}

/**
 * Occupies a worker for a fixed time, honouring cancellation.
 *
 * This is what makes the killed-worker scenario demonstrable: the step has to still be
 * in flight when the worker dies, or there is nothing to recover. Honouring the signal
 * matters for the same reason in reverse — when the worker is told it has lost the lease,
 * the step must actually stop, or the demo would have two workers running one step for
 * real rather than demonstrating the mechanism that prevents it.
 */
function holdTool(): Tool {
  return {
    def: {
      name: 'demo.hold',
      description: 'Hold the step for a fixed number of milliseconds.',
      input: {
        type: 'object',
        properties: {
          label: { type: 'string', minLength: 1 },
          holdMs: { type: 'integer', minimum: 0, maximum: 60_000 },
        },
        required: ['label', 'holdMs'],
        additionalProperties: true,
      },
      output: {
        type: 'object',
        properties: { label: { type: 'string' }, heldMs: { type: 'integer' } },
        required: ['label', 'heldMs'],
      },
      permissions: {
        effect: 'read',
        allowedRoles: ['worker', 'operator'],
        requiresApproval: false,
        idempotent: true,
      },
      timeoutMs: 60_000,
      costUnits: 1,
    },
    handler: async (args, ctx) => {
      const { label, holdMs } = args as { label: string; holdMs: number };
      ctx.log('demo.hold', { label, holdMs });
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, holdMs);
        ctx.signal.addEventListener(
          'abort',
          () => {
            clearTimeout(timer);
            const err = new Error('hold aborted');
            err.name = 'AbortError';
            reject(err);
          },
          { once: true },
        );
      });
      return { label, heldMs: holdMs };
    },
  };
}

/**
 * Appends one line per *executed* call to a shared file.
 *
 * The line names the step attempt and the idempotency key, so counting lines answers the
 * question the demo needs answered: did this attempt's side effect happen once or twice? A
 * recorded outcome replayed from the result store does not reach this handler, so it does not
 * produce a line — which is exactly what makes the deduplication visible from outside.
 */
function ledgerTool(ledgerPath: string): Tool {
  return {
    def: {
      name: 'demo.ledger',
      description: 'Append one line to the demo ledger file. The side effect the demo counts.',
      input: { type: 'object', properties: { note: { type: 'string' } }, additionalProperties: true },
      output: {
        type: 'object',
        properties: { wrote: { type: 'string' }, key: { type: 'string' } },
        required: ['wrote', 'key'],
      },
      permissions: {
        effect: 'write',
        allowedRoles: ['worker', 'operator'],
        requiresApproval: false,
        // A full-line append is not idempotent in itself; what makes repeating this call
        // safe is the executor's recorded outcome, which is the mechanism under test.
        idempotent: true,
      },
      timeoutMs: 5_000,
      costUnits: 2,
    },
    handler: async (_args, ctx) => {
      const line = [
        ctx.ref.workflowId,
        ctx.ref.step,
        `attempt=${ctx.ref.attempt}`,
        `key=${ctx.idempotencyKey.slice(0, 12)}`,
      ].join('\t');
      await mkdir(dirname(ledgerPath), { recursive: true });
      // `appendFile` with no read-modify-write, so two concurrent writers cannot lose a
      // line and under-count — the demo's whole claim rests on this count being honest.
      await appendFile(ledgerPath, `${line}\n`, 'utf8');
      ctx.log('demo.ledger', { wrote: line });
      return { wrote: line, key: ctx.idempotencyKey };
    },
  };
}

/**
 * Fails its first `failures` executions for a given label, then succeeds.
 *
 * The counter is on disk rather than in memory because the point is to fail across *process*
 * boundaries: each attempt may land on a different worker, and an in-memory counter would
 * reset and the step would never stop failing.
 */
function flakyTool(statePath: string): Tool {
  return {
    def: {
      name: 'demo.flaky',
      description: 'Fail the first N executions for a label with a retryable error, then succeed.',
      input: {
        type: 'object',
        properties: {
          label: { type: 'string', minLength: 1, maxLength: 64, pattern: '^[A-Za-z0-9_.-]+$' },
          failures: { type: 'integer', minimum: 0, maximum: 10 },
        },
        required: ['label', 'failures'],
        additionalProperties: true,
      },
      output: {
        type: 'object',
        properties: { label: { type: 'string' }, executions: { type: 'integer', minimum: 1 } },
        required: ['label', 'executions'],
      },
      permissions: {
        effect: 'read',
        allowedRoles: ['worker', 'operator'],
        requiresApproval: false,
        idempotent: true,
      },
      timeoutMs: 5_000,
      costUnits: 1,
    },
    handler: async (args, ctx) => {
      const { label, failures } = args as { label: string; failures: number };
      // The pattern on `label` is what makes this join safe: the value reaches a filename,
      // and an unvalidated one could contain a path separator.
      const counter = join(statePath, `flaky-${label}.count`);
      await mkdir(statePath, { recursive: true });
      const previous = Number(await readFile(counter, 'utf8').catch(() => '0')) || 0;
      const executions = previous + 1;
      await writeFile(counter, String(executions), 'utf8');

      if (executions <= failures) {
        ctx.log('demo.flaky failing', { label, executions, failures });
        // `unavailable` and retryable: the retry policy on the node lists this class, so
        // the failure is the kind the policy is allowed to act on. A class the policy does
        // not list fails the step terminally, which is the other half of the demo.
        throw new ToolError(
          'unavailable',
          `${label} execution ${executions} of ${failures} is configured to fail`,
          true,
        );
      }
      ctx.log('demo.flaky succeeding', { label, executions });
      return { label, executions };
    },
  };
}

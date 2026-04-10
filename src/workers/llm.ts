/**
 * The `llm` node handler, and the provider seam underneath it.
 *
 * Two decisions shape this file.
 *
 * First, the default provider is a deterministic fake. Nothing in this repository
 * holds an API key and no test touches a network, so every run of the suite and every
 * run of the demo produces byte-identical output. A model call that varies between
 * runs makes every surrounding assertion probabilistic, and an orchestrator whose own
 * tests are flaky cannot be used to diagnose anything else. The fake is seeded and can
 * be told to misbehave on demand, so the malformed-output and timeout paths are tested
 * rather than assumed.
 *
 * Second, the model context and the orchestration metadata are kept apart. What goes
 * into workflow state is the validated value plus a small record of the call — provider,
 * finish reason, token counts, prompt size and digest. The prompt text itself does not,
 * because a history whose every entry is a page of prose is unreadable, and this
 * project's claim is that the interesting part is the infrastructure.
 */

import type { Schema } from '../tools/types.js';
import { ToolError, validate } from '../tools/types.js';

/** The model-facing half of an `llm` node's input. */
export interface LlmContext {
  readonly prompt: string;
  readonly system?: string;
  /** Few-shot pairs, kept structured so they can be counted and trimmed, not concatenated blindly. */
  readonly examples?: readonly { readonly input: string; readonly output: string }[];
}

/** The orchestration-facing half: what the control plane needs to judge the result. */
export interface LlmNodeInput {
  readonly context: LlmContext;

  /**
   * The schema the output must satisfy. When present, the handler parses and validates
   * before the value reaches workflow state, so a model that answers with prose fails
   * this step rather than crashing a JSON.parse three nodes downstream.
   */
  readonly expect?: Schema;

  readonly maxTokens?: number;

  /** Per-call budget. Falls back to the assignment's attempt timeout. */
  readonly timeoutMs?: number;
}

export type FinishReason = 'stop' | 'length' | 'refusal';

export interface LlmCompletion {
  readonly text: string;
  readonly finishReason: FinishReason;
  readonly usage: { readonly promptTokens: number; readonly completionTokens: number };
}

/**
 * A provider. One method, and it takes the signal, because a provider that cannot be
 * cancelled turns a cancelled workflow into a worker that is still paying for tokens.
 */
export interface LlmProvider {
  readonly name: string;
  complete(request: LlmRequest, signal: AbortSignal): Promise<LlmCompletion>;
}

export interface LlmRequest {
  readonly context: LlmContext;
  readonly expect?: Schema;
  readonly maxTokens: number;
}

/** What an `llm` node writes into workflow state. */
export interface LlmStepOutput {
  /** Validated structured output, or the raw text when the node declared no schema. */
  readonly value: unknown;
  readonly meta: LlmCallMeta;
}

export interface LlmCallMeta {
  readonly provider: string;
  readonly finishReason: FinishReason;
  readonly promptTokens: number;
  readonly completionTokens: number;
  /** Size and digest stand in for the prompt, so state stays legible and still traceable. */
  readonly promptChars: number;
  readonly promptDigest: string;
  readonly validated: boolean;
}

export class LlmTimeoutError extends Error {
  readonly errorClass = 'timeout' as const;
  constructor(ms: number) {
    super(`model call exceeded ${ms}ms`);
    this.name = 'LlmTimeoutError';
  }
}

/**
 * Runs one `llm` node.
 *
 * Throws on failure — the worker classifies and reports. Returning a union here would
 * duplicate the worker's error classification in two places, and the two would drift.
 */
export async function runLlmNode(
  input: unknown,
  opts: {
    readonly provider: LlmProvider;
    readonly signal: AbortSignal;
    readonly defaultTimeoutMs: number;
  },
): Promise<LlmStepOutput> {
  const parsed = parseNodeInput(input);
  const timeoutMs = parsed.timeoutMs ?? opts.defaultTimeoutMs;

  const request: LlmRequest = {
    context: parsed.context,
    ...(parsed.expect !== undefined ? { expect: parsed.expect } : {}),
    maxTokens: parsed.maxTokens ?? 1024,
  };

  const completion = await withTimeout(
    (signal) => opts.provider.complete(request, signal),
    timeoutMs,
    opts.signal,
  );

  const promptText = renderPrompt(parsed.context);
  const meta: LlmCallMeta = {
    provider: opts.provider.name,
    finishReason: completion.finishReason,
    promptTokens: completion.usage.promptTokens,
    completionTokens: completion.usage.completionTokens,
    promptChars: promptText.length,
    promptDigest: digest(promptText),
    validated: parsed.expect !== undefined,
  };

  if (completion.finishReason === 'refusal') {
    throw new ToolError('invalid_output', 'model refused the request', false);
  }

  if (parsed.expect === undefined) {
    return { value: completion.text, meta };
  }

  const value = parseStructured(completion.text, parsed.expect);
  return { value, meta };
}

/**
 * Validates the node's own input before anything is sent anywhere.
 *
 * An `llm` node with a malformed payload is a definition bug, and discovering it after
 * a billable call has already been made is the wrong order.
 */
function parseNodeInput(input: unknown): LlmNodeInput {
  if (typeof input !== 'object' || input === null) {
    throw new ToolError('invalid_args', 'llm node input must be an object', false);
  }
  const obj = input as Record<string, unknown>;
  const context = obj['context'];
  if (typeof context !== 'object' || context === null) {
    throw new ToolError('invalid_args', 'llm node input requires a "context" object', false);
  }
  const prompt = (context as Record<string, unknown>)['prompt'];
  if (typeof prompt !== 'string' || prompt.length === 0) {
    throw new ToolError('invalid_args', 'llm node context requires a non-empty "prompt"', false);
  }
  return input as LlmNodeInput;
}

/**
 * Turns model text into a validated value.
 *
 * Fenced code blocks are tolerated because every model emits them sometimes and
 * rejecting them would be pedantry; anything else that is not the declared shape is a
 * step failure with the offending text quoted, because "invalid output" with no sample
 * is unactionable.
 */
function parseStructured(text: string, schema: Schema): unknown {
  const candidate = stripFence(text).trim();
  let value: unknown;
  try {
    value = JSON.parse(candidate);
  } catch (err) {
    throw new ToolError(
      'invalid_output',
      `model output is not JSON (${err instanceof Error ? err.message : String(err)}); ` +
        `expected ${schema.type}, received: ${snippet(candidate)}`,
      false,
    );
  }

  const errors = validate(value, schema);
  if (errors.length > 0) {
    throw ToolError.invalidOutput(errors);
  }
  return value;
}

function stripFence(text: string): string {
  const fence = /^\s*```(?:json)?\s*\n([\s\S]*?)\n?\s*```\s*$/.exec(text);
  return fence?.[1] ?? text;
}

function snippet(text: string, max = 160): string {
  const flat = text.replace(/\s+/g, ' ').trim();
  return flat.length <= max ? JSON.stringify(flat) : `${JSON.stringify(flat.slice(0, max))}...`;
}

/**
 * Races a call against its own budget, cancelling the call on either timeout or an
 * outer abort. Without the inner controller a timed-out call keeps running, which is
 * how a "timeout" ends up costing full price.
 */
async function withTimeout<T>(
  fn: (signal: AbortSignal) => Promise<T>,
  ms: number,
  outer: AbortSignal,
): Promise<T> {
  const inner = new AbortController();
  const onOuterAbort = () => inner.abort(outer.reason);
  if (outer.aborted) inner.abort(outer.reason);
  else outer.addEventListener('abort', onOuterAbort, { once: true });

  let timer: ReturnType<typeof setTimeout> | undefined;
  const expiry = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      inner.abort(new LlmTimeoutError(ms));
      reject(new LlmTimeoutError(ms));
    }, ms);
  });

  try {
    return await Promise.race([fn(inner.signal), expiry]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    outer.removeEventListener('abort', onOuterAbort);
  }
}

function renderPrompt(context: LlmContext): string {
  const parts: string[] = [];
  if (context.system !== undefined) parts.push(context.system);
  for (const example of context.examples ?? []) {
    parts.push(`${example.input}\n${example.output}`);
  }
  parts.push(context.prompt);
  return parts.join('\n\n');
}

/**
 * FNV-1a, 32 bit. Not a security primitive — it only has to be stable across processes
 * and versions so two runs of the same prompt are recognisably the same prompt.
 */
export function digest(text: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}

/** How the fake should answer a matching request. */
export type FakeBehaviour =
  | { readonly kind: 'structured' }
  | { readonly kind: 'prose' }
  /** Syntactically broken JSON, for the parse-failure path. */
  | { readonly kind: 'malformed' }
  | { readonly kind: 'refusal' }
  /** Never resolves until aborted, so the caller's timeout is what ends the call. */
  | { readonly kind: 'hang' };

export interface FakeRule {
  /** Matched against the rendered prompt. */
  readonly when: string | RegExp;
  readonly behaviour: FakeBehaviour;
}

export interface FakeProviderOptions {
  /** Mixed into the per-call seed, so two providers can differ while each stays stable. */
  readonly seed?: string;
  readonly rules?: readonly FakeRule[];
  readonly behaviour?: FakeBehaviour;
  /** Simulated latency, in milliseconds of real time. */
  readonly latencyMs?: number;
}

/**
 * The default provider: deterministic, offline, and able to fail on cue.
 *
 * Output is derived from a hash of (seed, prompt, schema), so the same request always
 * produces the same answer and two different requests produce visibly different ones.
 * That is enough to exercise the real paths — validation, branching on a model-produced
 * field, retry on a bad response — without any of it depending on a live service.
 */
export function createFakeProvider(options: FakeProviderOptions = {}): LlmProvider {
  const seed = options.seed ?? 'parliament';
  const rules = options.rules ?? [];
  const fallback = options.behaviour ?? { kind: 'structured' };

  return {
    name: `fake:${seed}`,

    async complete(request, signal) {
      const prompt = renderPrompt(request.context);
      const behaviour = rules.find((r) => matches(r.when, prompt))?.behaviour ?? fallback;

      if (options.latencyMs !== undefined && options.latencyMs > 0) {
        await delay(options.latencyMs, signal);
      }
      throwIfAborted(signal);

      if (behaviour.kind === 'hang') {
        await never(signal);
      }

      const rng = seededRng(`${seed}|${prompt}|${request.expect ? schemaKey(request.expect) : 'text'}`);
      const promptTokens = Math.max(1, Math.round(prompt.length / 4));

      if (behaviour.kind === 'refusal') {
        return {
          text: 'I cannot help with that request.',
          finishReason: 'refusal',
          usage: { promptTokens, completionTokens: 8 },
        };
      }

      const text =
        behaviour.kind === 'prose'
          ? proseFor(rng)
          : behaviour.kind === 'malformed'
            ? `{"partial": ${JSON.stringify(proseFor(rng))},`
            : request.expect
              ? JSON.stringify(sample(request.expect, rng), null, 0)
              : proseFor(rng);

      return {
        text,
        finishReason: 'stop',
        usage: { promptTokens, completionTokens: Math.max(1, Math.round(text.length / 4)) },
      };
    },
  };
}

function matches(when: string | RegExp, prompt: string): boolean {
  return typeof when === 'string' ? prompt.includes(when) : when.test(prompt);
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw abortReason(signal);
}

function abortReason(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  if (reason instanceof Error) return reason;
  const err = new Error('aborted');
  err.name = 'AbortError';
  return err;
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(abortReason(signal));
    };
    if (signal.aborted) onAbort();
    else signal.addEventListener('abort', onAbort, { once: true });
  });
}

function never(signal: AbortSignal): Promise<never> {
  return new Promise<never>((_resolve, reject) => {
    const onAbort = () => reject(abortReason(signal));
    if (signal.aborted) onAbort();
    else signal.addEventListener('abort', onAbort, { once: true });
  });
}

/** mulberry32 over an FNV seed: small, stable, and good enough to look unpatterned. */
function seededRng(seedText: string): () => number {
  let state = Number.parseInt(digest(seedText), 16) >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const WORDS = [
  'ledger', 'quorum', 'replay', 'lease', 'cascade', 'pivot', 'margin', 'signal',
  'baseline', 'tranche', 'dispatch', 'audit', 'variance', 'threshold', 'cohort', 'drift',
] as const;

function word(rng: () => number): string {
  const index = Math.floor(rng() * WORDS.length) % WORDS.length;
  return WORDS[index] ?? 'ledger';
}

function proseFor(rng: () => number): string {
  const count = 6 + Math.floor(rng() * 6);
  const words: string[] = [];
  for (let i = 0; i < count; i += 1) words.push(word(rng));
  return `${words.join(' ')}.`;
}

/** A value that satisfies `schema`, chosen from the stream rather than invented freely. */
function sample(schema: Schema, rng: () => number): unknown {
  if (schema.enum && schema.enum.length > 0) {
    const index = Math.floor(rng() * schema.enum.length) % schema.enum.length;
    return schema.enum[index];
  }

  switch (schema.type) {
    case 'string': {
      const min = schema.minLength ?? 0;
      let text = proseFor(rng).slice(0, Math.max(min, Math.min(schema.maxLength ?? 48, 48)));
      while (text.length < min) text += 'x';
      return text;
    }
    case 'integer':
    case 'number': {
      const min = schema.minimum ?? 0;
      const max = schema.maximum ?? min + 100;
      const raw = min + rng() * (max - min);
      return schema.type === 'integer' ? Math.round(raw) : Math.round(raw * 100) / 100;
    }
    case 'boolean':
      return rng() < 0.5;
    case 'array': {
      const items = schema.items;
      if (!items) return [];
      const count = 1 + Math.floor(rng() * 3);
      return Array.from({ length: count }, () => sample(items, rng));
    }
    case 'object': {
      const out: Record<string, unknown> = {};
      // Only declared properties are produced. A key listed in `required` with no
      // property schema is not invented: there is no shape to invent it to, and guessing
      // one would produce a value the validator then rejects as an unknown property —
      // which reads as a provider bug rather than as the schema bug it is.
      for (const [key, child] of Object.entries(schema.properties ?? {})) {
        out[key] = sample(child, rng);
      }
      return out;
    }
  }
}

/** Stable rendering of a schema, so the seed changes when the expected shape changes. */
function schemaKey(schema: Schema): string {
  const parts: string[] = [schema.type];
  if (schema.enum) parts.push(`enum:${schema.enum.join(',')}`);
  if (schema.items) parts.push(`items(${schemaKey(schema.items)})`);
  for (const [key, child] of Object.entries(schema.properties ?? {})) {
    parts.push(`${key}:${schemaKey(child)}`);
  }
  return parts.join('|');
}

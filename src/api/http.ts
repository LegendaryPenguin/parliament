/**
 * Transport plumbing: bounded bodies, error shapes, and route matching.
 *
 * Kept separate from the handlers so that the handlers read as policy ("approving a
 * terminal workflow is a conflict") rather than as HTTP mechanics.
 */

import type { IncomingMessage, ServerResponse } from 'node:http';

/**
 * 256 KiB.
 *
 * Bounded because the body here is a workflow definition plus an input document, and
 * an unbounded read is a trivial way to exhaust the control plane's memory from
 * outside. The limit is enforced while reading rather than from `Content-Length`,
 * which a client controls and may simply lie about.
 */
export const MAX_BODY_BYTES = 256 * 1024;

/** A failure with an HTTP status and a body that says why. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly detail?: unknown,
  ) {
    super(message);
    this.name = 'ApiError';
  }

  static badRequest(message: string, detail?: unknown): ApiError {
    return new ApiError(400, 'invalid_request', message, detail);
  }

  static notFound(message: string): ApiError {
    return new ApiError(404, 'not_found', message);
  }

  static conflict(message: string, detail?: unknown): ApiError {
    return new ApiError(409, 'conflict', message, detail);
  }

  toBody(): Record<string, unknown> {
    return {
      error: this.code,
      message: this.message,
      ...(this.detail !== undefined ? { detail: this.detail } : {}),
    };
  }
}

/**
 * Reads a JSON body, refusing anything over the limit.
 *
 * The connection is destroyed on overflow rather than drained: continuing to read a
 * body that has already been rejected is doing the attacker's work. The 413 is written
 * first so a well-behaved client still learns why.
 */
export async function readJsonBody(req: IncomingMessage, limit = MAX_BODY_BYTES): Promise<unknown> {
  const declared = Number(req.headers['content-length'] ?? '0');
  if (Number.isFinite(declared) && declared > limit) {
    throw new ApiError(413, 'payload_too_large', `request body exceeds ${limit} bytes`, {
      limitBytes: limit,
      declaredBytes: declared,
    });
  }

  const chunks: Buffer[] = [];
  let size = 0;

  await new Promise<void>((resolve, reject) => {
    req.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > limit) {
        reject(
          new ApiError(413, 'payload_too_large', `request body exceeds ${limit} bytes`, {
            limitBytes: limit,
          }),
        );
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', resolve);
    req.on('error', reject);
  });

  const text = Buffer.concat(chunks).toString('utf8');
  if (text.trim() === '') return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch (cause) {
    throw ApiError.badRequest('body is not valid JSON', {
      parseError: cause instanceof Error ? cause.message : String(cause),
    });
  }
}

export function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body, jsonSafe, 2);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(text),
    // The dashboard is served from this same origin, but a developer poking at the API
    // from a file:// page or another port is a normal thing to do.
    'access-control-allow-origin': '*',
  });
  res.end(text);
}

/** Maps are used throughout the projections and do not survive JSON.stringify. */
function jsonSafe(_key: string, value: unknown): unknown {
  if (value instanceof Map) return Object.fromEntries(value);
  if (value instanceof Set) return [...value];
  return value;
}

export function sendText(res: ServerResponse, status: number, contentType: string, body: string): void {
  res.writeHead(status, {
    'content-type': contentType,
    'content-length': Buffer.byteLength(body),
    'access-control-allow-origin': '*',
  });
  res.end(body);
}

/**
 * Matches a path against a pattern with `:name` segments.
 *
 * Hand-rolled because the route table is a dozen entries: a router dependency would
 * be more code to audit than the thing it replaces. Segments are decoded, so a step
 * name containing a slash or a space round-trips.
 */
export function matchPath(pattern: string, path: string): Record<string, string> | undefined {
  const expected = pattern.split('/').filter((s) => s.length > 0);
  const actual = path.split('/').filter((s) => s.length > 0);
  if (expected.length !== actual.length) return undefined;

  const params: Record<string, string> = {};
  for (let i = 0; i < expected.length; i += 1) {
    const want = expected[i] as string;
    const got = actual[i] as string;
    if (want.startsWith(':')) {
      params[want.slice(1)] = safeDecode(got);
    } else if (want !== got) {
      return undefined;
    }
  }
  return params;
}

function safeDecode(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    // A malformed escape is not worth a 400 for a path segment that will simply fail
    // to match a known workflow id and produce a 404.
    return segment;
  }
}

/** Parses a positive integer query parameter, rejecting junk rather than ignoring it. */
export function intParam(
  params: URLSearchParams,
  name: string,
  fallback: number,
  max?: number,
): number {
  const raw = params.get(name);
  if (raw === null || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw ApiError.badRequest(`${name} must be a non-negative integer, got "${raw}"`);
  }
  return max !== undefined ? Math.min(value, max) : value;
}

/** Reads a required string field out of a parsed JSON body. */
export function requireString(body: unknown, field: string, maxLength = 1024): string {
  const record = asRecord(body);
  const value = record[field];
  if (typeof value !== 'string' || value.trim() === '') {
    throw ApiError.badRequest(`"${field}" is required and must be a non-empty string`);
  }
  if (value.length > maxLength) {
    throw ApiError.badRequest(`"${field}" must be at most ${maxLength} characters`);
  }
  return value;
}

export function optionalString(body: unknown, field: string, maxLength = 4096): string | undefined {
  const record = asRecord(body);
  const value = record[field];
  if (value === undefined || value === null) return undefined;
  if (typeof value !== 'string') {
    throw ApiError.badRequest(`"${field}" must be a string if present`);
  }
  if (value.length > maxLength) {
    throw ApiError.badRequest(`"${field}" must be at most ${maxLength} characters`);
  }
  return value;
}

export function asRecord(body: unknown): Record<string, unknown> {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    throw ApiError.badRequest('body must be a JSON object');
  }
  return body as Record<string, unknown>;
}

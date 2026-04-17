/**
 * The HTTP client.
 *
 * Thin by design: one `request` that turns a non-2xx into an `ApiFailure` carrying the
 * server's explanation, because the API goes to some trouble to say *why* a request was
 * refused and a client that reported "request failed" would throw that away.
 */

import type {
  ApiErrorBody,
  EventView,
  ProjectionView,
  ReplayView,
  TraceView,
  WorkflowListView,
} from './types.js';

export class ApiFailure extends Error {
  constructor(
    readonly status: number,
    readonly body: ApiErrorBody,
  ) {
    super(body.message || `request failed with ${status}`);
    this.name = 'ApiFailure';
  }

  /** The definition errors or problem list, if the server sent one. */
  get details(): string[] {
    const out: string[] = [];
    for (const error of this.body.errors ?? []) {
      out.push(error.node !== undefined ? `${error.node}: ${error.message}` : error.message);
    }
    const detail = this.body.detail;
    if (detail !== null && typeof detail === 'object' && 'problems' in detail) {
      const problems = (detail as { problems?: unknown }).problems;
      if (Array.isArray(problems)) out.push(...problems.map((p) => String(p)));
    }
    return out;
  }
}

export interface ListFilters {
  readonly state?: string;
  readonly tenant?: string;
  readonly workflow?: string;
  readonly limit?: number;
  readonly offset?: number;
}

export class Api {
  constructor(private readonly base = '') {}

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(`${this.base}${path}`, {
      ...init,
      headers: { ...(init?.body !== undefined ? { 'content-type': 'application/json' } : {}) },
    });
    const text = await response.text();
    const parsed: unknown = text === '' ? undefined : safeParse(text);
    if (!response.ok) {
      throw new ApiFailure(
        response.status,
        (parsed as ApiErrorBody | undefined) ?? { error: 'unknown', message: text || response.statusText },
      );
    }
    return parsed as T;
  }

  listWorkflows(filters: ListFilters = {}): Promise<WorkflowListView> {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(filters)) {
      if (value !== undefined && value !== '') query.set(key, String(value));
    }
    const suffix = query.toString();
    return this.request<WorkflowListView>(`/workflows${suffix === '' ? '' : `?${suffix}`}`);
  }

  workflow(id: string): Promise<ProjectionView> {
    return this.request<ProjectionView>(`/workflows/${encodeURIComponent(id)}`);
  }

  trace(id: string): Promise<TraceView> {
    return this.request<TraceView>(`/workflows/${encodeURIComponent(id)}/trace`);
  }

  events(id: string, after = 0): Promise<{ events: EventView[] }> {
    return this.request(`/workflows/${encodeURIComponent(id)}/events?after=${after}`);
  }

  replay(id: string, sequence: number): Promise<ReplayView> {
    return this.request<ReplayView>(`/workflows/${encodeURIComponent(id)}/replay?sequence=${sequence}`);
  }

  cancel(id: string, reason: string, requestedBy: string): Promise<ProjectionView> {
    return this.request<ProjectionView>(`/workflows/${encodeURIComponent(id)}/cancel`, {
      method: 'POST',
      body: JSON.stringify({ reason, requestedBy }),
    });
  }

  approve(id: string, step: string, approver: string, note?: string): Promise<ProjectionView> {
    return this.request<ProjectionView>(
      `/workflows/${encodeURIComponent(id)}/steps/${encodeURIComponent(step)}/approve`,
      { method: 'POST', body: JSON.stringify({ approver, ...(note !== undefined ? { note } : {}) }) },
    );
  }

  deny(id: string, step: string, approver: string, reason: string): Promise<ProjectionView> {
    return this.request<ProjectionView>(
      `/workflows/${encodeURIComponent(id)}/steps/${encodeURIComponent(step)}/deny`,
      { method: 'POST', body: JSON.stringify({ approver, reason }) },
    );
  }

  /**
   * Subscribes to one workflow's events.
   *
   * `EventSource` reconnects on its own and replays from the last `id:` it saw, which
   * the server reads as the sequence to resume from — so a dropped connection loses
   * nothing and the dashboard does not need reconnection logic of its own.
   */
  stream(id: string, handlers: { onChange: () => void; onStatus?: (open: boolean) => void }): () => void {
    const source = new EventSource(`${this.base}/workflows/${encodeURIComponent(id)}/stream`);
    const change = (): void => handlers.onChange();
    source.addEventListener('event', change);
    source.addEventListener('projection', change);
    source.addEventListener('open', () => handlers.onStatus?.(true));
    source.addEventListener('error', () => handlers.onStatus?.(false));
    return () => source.close();
  }
}

function safeParse(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return { error: 'unparseable', message: text.slice(0, 500) };
  }
}

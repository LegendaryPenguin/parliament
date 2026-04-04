/**
 * An in-memory event store.
 *
 * Used by tests and by single-process demos. It is not a toy implementation of a
 * different contract: it enforces the same optimistic concurrency and the same
 * atomicity as the file store, because a test suite that passes against a permissive
 * in-memory store and fails in production has tested nothing.
 */

import { sealBatch, isTerminalEvent } from './common.js';
import type { Event, EventStore, NewEvent, Sequence } from './types.js';
import { AppendObservers, type AppendListener } from './observers.js';
import type { WorkflowId } from '../workflow/types.js';

interface Log {
  readonly events: Event[];
  /** Tracked on write so `listActive` never has to scan histories. */
  terminal: boolean;
}

/**
 * Round-trips through JSON on append.
 *
 * This looks like pointless work for an in-memory store, and that is the point: the
 * durable store can only persist what JSON can represent, so a payload containing a
 * `Date`, a `Map` or an `undefined` must misbehave identically here. Otherwise the
 * in-memory store silently tolerates payloads that the file store mangles, and the
 * divergence is discovered in production. It also stops a caller from mutating an
 * event it has already appended.
 */
function freezePayload<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export class MemoryEventStore implements EventStore {
  private readonly logs = new Map<WorkflowId, Log>();
  private readonly observers = new AppendObservers();

  async append(
    workflowId: WorkflowId,
    expectedSequence: Sequence,
    events: readonly NewEvent[],
  ): Promise<readonly Event[]> {
    const log = this.logs.get(workflowId);
    const head = log ? (log.events[log.events.length - 1]?.sequence ?? 0) : 0;

    // Validate the entire batch before touching the log. Anything that throws below
    // this line leaves no partial write.
    const sealed = sealBatch(workflowId, head, expectedSequence, events).map((e) =>
      Object.freeze(freezePayload(e)),
    );
    if (sealed.length === 0) return [];

    const target = log ?? { events: [], terminal: false };
    if (!log) this.logs.set(workflowId, target);
    target.events.push(...sealed);
    if (sealed.some((e) => isTerminalEvent(e.type))) target.terminal = true;

    this.observers.notify(sealed);
    return sealed;
  }

  subscribe(listener: AppendListener): () => void {
    return this.observers.add(listener);
  }

  async read(workflowId: WorkflowId, after: Sequence = 0): Promise<readonly Event[]> {
    const log = this.logs.get(workflowId);
    if (!log) return [];
    return log.events.filter((e) => e.sequence > after);
  }

  async head(workflowId: WorkflowId): Promise<Sequence> {
    const log = this.logs.get(workflowId);
    if (!log) return 0;
    return log.events[log.events.length - 1]?.sequence ?? 0;
  }

  async list(): Promise<readonly WorkflowId[]> {
    return [...this.logs.keys()];
  }

  async listActive(): Promise<readonly WorkflowId[]> {
    const active: WorkflowId[] = [];
    for (const [id, log] of this.logs) {
      if (!log.terminal) active.push(id);
    }
    return active;
  }
}

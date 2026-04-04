/**
 * The append-observer list shared by every `EventStore` implementation.
 *
 * Shared rather than reimplemented per store because the two rules that matter are easy
 * to state and easy to forget, and getting either wrong turns a dashboard feature into a
 * correctness bug in the log:
 *
 *  - **A listener must never fail an append.** By the time observers run, the events are
 *    durable; the fold has already accepted them. Propagating a listener's exception
 *    would make the caller believe a write failed that in fact succeeded, and the
 *    caller's correct response to a failed append — retry against a re-read history —
 *    is then taken against a log that already contains the write.
 *  - **Observers run after the write, never during it.** For a store that takes a lock
 *    per workflow this is not a preference: a listener that reads the workflow it was
 *    just told about would take the same lock and deadlock against the append still
 *    holding it.
 */

import type { Event } from './types.js';

export type AppendListener = (events: readonly Event[]) => void;

export class AppendObservers {
  private readonly listeners = new Set<AppendListener>();

  /** Registers a listener. The returned function removes it. */
  add(listener: AppendListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * Notifies every listener of a durable batch.
   *
   * Iterates a copy, so a listener that unsubscribes itself — the normal shape of a
   * one-shot "wake me when this workflow moves" waiter — does not mutate the set being
   * walked.
   */
  notify(events: readonly Event[]): void {
    if (events.length === 0) return;
    for (const listener of [...this.listeners]) {
      try {
        listener(events);
      } catch {
        // Deliberately swallowed; see the note above on why this cannot throw.
      }
    }
  }

  /** How many listeners are attached. Exists for tests that assert unsubscribe works. */
  get size(): number {
    return this.listeners.size;
  }
}

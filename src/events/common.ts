/**
 * Append-path logic shared by every store implementation.
 *
 * The validation and sealing of a batch is kept here, and kept *pure*, so that both
 * stores reject exactly the same batches. A store that accepted an event the other
 * rejected would turn a storage choice into a behaviour change, and the whole point
 * of the `EventStore` interface is that the control plane cannot tell which one it
 * is talking to.
 */

import { ConcurrencyError, EVENT_VERSION } from './types.js';
import type { Event, EventType, NewEvent, Sequence } from './types.js';
import type { WorkflowId } from '../workflow/types.js';

/** Event types after which nothing further happens to a workflow. */
export const TERMINAL_EVENT_TYPES: ReadonlySet<EventType> = new Set<EventType>([
  'workflow.completed',
  'workflow.failed',
  'workflow.cancelled',
]);

export function isTerminalEvent(type: EventType): boolean {
  return TERMINAL_EVENT_TYPES.has(type);
}

/** Rejected for a reason other than a concurrent writer: the batch itself is wrong. */
export class InvalidAppendError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidAppendError';
  }
}

/**
 * Validates a whole batch and returns the sequenced events to commit.
 *
 * Nothing is written by this function and nothing is mutated. That is deliberate:
 * callers validate first and commit second, so a batch rejected on its fourth event
 * cannot leave the first three in the log. "Atomic append" is not achievable if
 * validation is interleaved with writing.
 */
export function sealBatch(
  workflowId: WorkflowId,
  head: Sequence,
  expectedSequence: Sequence,
  events: readonly NewEvent[],
): Event[] {
  if (!Number.isInteger(expectedSequence) || expectedSequence < 0) {
    throw new InvalidAppendError(
      `expectedSequence must be a non-negative integer, got ${String(expectedSequence)}`,
    );
  }
  if (expectedSequence !== head) {
    throw new ConcurrencyError(workflowId, expectedSequence, head);
  }

  const sealed: Event[] = [];
  events.forEach((event, i) => {
    const where = `events[${i}]`;
    if (event === null || typeof event !== 'object') {
      throw new InvalidAppendError(`${where} is not an event object`);
    }
    if (event.workflowId !== workflowId) {
      throw new InvalidAppendError(
        `${where} belongs to workflow ${String(event.workflowId)}, not ${workflowId}`,
      );
    }
    if (typeof event.type !== 'string' || event.type.length === 0) {
      throw new InvalidAppendError(`${where} has no event type`);
    }
    if (!Number.isFinite(event.at)) {
      throw new InvalidAppendError(`${where} (${event.type}) has no timestamp`);
    }
    if (event.version !== undefined && event.version !== EVENT_VERSION) {
      // Refusing an unknown version on write is the cheap half of the compatibility
      // contract: readers must cope with old events forever, but nothing should be
      // able to introduce an envelope this build does not understand.
      throw new InvalidAppendError(
        `${where} (${event.type}) has version ${String(event.version)}, expected ${EVENT_VERSION}`,
      );
    }

    sealed.push({
      ...event,
      version: EVENT_VERSION,
      sequence: head + i + 1,
    });
  });

  return sealed;
}

/**
 * Replay and the timeline.
 *
 * This is the debugging tool, which makes legibility the deliverable rather than a
 * nicety. When a workflow ends up somewhere surprising, the question is always "which
 * event moved it there", and the answer should be readable without a second tool: one
 * line per event, with the transition it caused spelled out.
 */

import type { Event, Sequence, WorkflowProjection } from '../events/types.js';
import type { RunState, WorkflowDef } from '../workflow/types.js';
import { project } from './project.js';

/** The state of the workflow before any event has been folded. */
const BEFORE_ANYTHING: RunState = 'pending';
const WORKFLOW_ROW = '(workflow)';

/**
 * The projection as of `sequence`.
 *
 * A prefix of the log is itself a valid log, which is the property that makes this a
 * three-line function rather than a second state machine that could disagree with the
 * first one.
 */
export function replayTo(
  def: WorkflowDef,
  events: readonly Event[],
  sequence: Sequence,
): WorkflowProjection {
  return project(
    def,
    events.filter((e) => e.sequence <= sequence),
  );
}

export interface TimelineEntry {
  readonly sequence: Sequence;
  readonly type: string;
  readonly step?: string;
  readonly attempt?: number;
  /** Rendered transitions this event caused, e.g. `fetch: ready -> running`. */
  readonly transitions: readonly string[];
  readonly at: number;
}

function states(def: WorkflowDef, projection: WorkflowProjection | undefined): Map<string, RunState> {
  const snapshot = new Map<string, RunState>();
  snapshot.set(WORKFLOW_ROW, projection?.state ?? BEFORE_ANYTHING);
  for (const node of def.nodes) {
    snapshot.set(node.name, projection?.steps.get(node.name)?.state ?? BEFORE_ANYTHING);
  }
  return snapshot;
}

/**
 * Builds the timeline by folding every prefix and diffing consecutive snapshots.
 *
 * Quadratic in the length of the history, and chosen anyway: the alternative is to
 * expose the fold's internals so the transitions can be observed as they happen, which
 * would mean a second code path through the state machine. A debugging tool that can
 * disagree with the function it is explaining is worse than a slow one.
 */
export function timeline(def: WorkflowDef, events: readonly Event[]): TimelineEntry[] {
  const entries: TimelineEntry[] = [];
  let previous = states(def, undefined);

  events.forEach((event, i) => {
    const after = states(def, project(def, events.slice(0, i + 1)));
    const transitions: string[] = [];
    for (const [subject, to] of after) {
      const from = previous.get(subject);
      if (from === undefined || from === to) continue;
      transitions.push(`${subject === WORKFLOW_ROW ? 'workflow' : subject}: ${from} -> ${to}`);
    }
    previous = after;

    entries.push({
      sequence: event.sequence,
      type: event.type,
      transitions,
      at: event.at,
      ...('step' in event && event.step !== undefined ? { step: event.step } : {}),
      ...('attempt' in event ? { attempt: event.attempt } : {}),
    });
  });

  return entries;
}

function pad(text: string, width: number): string {
  return text.length >= width ? text : text + ' '.repeat(width - text.length);
}

/**
 * Renders the timeline as fixed-width columns.
 *
 * Columns rather than prose so that the eye can scan down one of them — in practice
 * the question is "where did this step's attempt number jump" or "which event made the
 * workflow terminal", and both are answered by reading a single column.
 */
export function renderTimeline(def: WorkflowDef, events: readonly Event[]): string {
  const entries = timeline(def, events);
  const lines: string[] = [
    `${pad('seq', 5)}${pad('event', 22)}${pad('step', 18)}${pad('try', 5)}transition`,
    '-'.repeat(78),
  ];

  for (const entry of entries) {
    lines.push(
      pad(String(entry.sequence), 5) +
        pad(entry.type, 22) +
        pad(entry.step ?? '-', 18) +
        pad(entry.attempt === undefined ? '-' : String(entry.attempt), 5) +
        // An event that caused no transition is worth a line too: a tool call or a
        // lease renewal that changed nothing is often exactly what the reader is
        // looking for when they cannot explain a gap in time.
        (entry.transitions.length === 0 ? '-' : entry.transitions.join(', ')),
    );
  }

  return lines.join('\n');
}

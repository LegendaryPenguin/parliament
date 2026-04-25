/**
 * The timeline / trace view.
 *
 * Every event in order — nothing is filtered out, because the event you need is
 * reliably the one a summariser would have dropped — but grouped into step attempts,
 * since "attempt 2 of 3 on fraud-score, on worker-d, 770ms" is the unit a reader thinks
 * in. Retries, lease expiries and denials get their own visual treatment rather than
 * being another grey row, and tool calls collapse by default: arguments and results are
 * usually large and occasionally enormous, and the whole view would be unreadable with
 * them open.
 */

import { badge, el, formatClock, formatDuration, pre } from './dom.js';
import type { AttemptView, EventView, ToolCallView, TraceStepView, TraceView } from './types.js';

/** Events that deserve to stand out, and the class that makes them. */
const EVENT_TONE: Readonly<Record<string, string>> = {
  'workflow.created': 'neutral',
  'workflow.completed': 'good',
  'workflow.failed': 'bad',
  'workflow.cancelled': 'warn',
  'step.failed': 'bad',
  'step.retry_scheduled': 'retry',
  'step.lease_expired': 'lease',
  'step.lease_extended': 'renew',
  'step.completed': 'good',
  'tool.failed': 'bad',
  'tool.succeeded': 'tool',
  'tool.requested': 'tool',
  'approval.requested': 'warn',
  'approval.granted': 'good',
  'approval.denied': 'bad',
};

export interface TimelineOptions {
  readonly trace: TraceView;
  readonly selectedStep?: string | undefined;
  readonly onSelect: (step: string) => void;
}

export function renderTimeline(options: TimelineOptions): HTMLElement {
  const { trace } = options;
  const container = el('div', { className: 'timeline' });

  const attempts = new Map<string, { step: TraceStepView; attempt: AttemptView }>();
  for (const step of trace.steps) {
    for (const attempt of step.attempts) attempts.set(`${step.step}#${attempt.attempt}`, { step, attempt });
  }

  const t0 = trace.events[0]?.at ?? trace.createdAt;
  let currentKey: string | undefined;
  let currentBody: HTMLElement | undefined;

  for (const event of trace.events) {
    const key =
      event.step !== undefined && event.attempt !== undefined
        ? `${event.step}#${event.attempt}`
        : event.step !== undefined
          ? `${event.step}#1`
          : '';

    if (key !== currentKey) {
      currentKey = key;
      if (key === '') {
        currentBody = el('div', { className: 'group-body' });
        container.append(el('div', { className: 'group group-workflow', children: [currentBody] }));
      } else {
        const found = attempts.get(key);
        currentBody = el('div', { className: 'group-body' });
        container.append(
          el('div', {
            className: `group${found?.step.state === 'failed' ? ' group-failed' : ''}${
              options.selectedStep === found?.step.step ? ' group-selected' : ''
            }`,
            attrs: { 'data-step': found?.step.step ?? event.step ?? '' },
            children: [
              found !== undefined
                ? attemptHeader(found.step, found.attempt, options.onSelect)
                : el('div', { className: 'group-head', text: key }),
              currentBody,
            ],
          }),
        );
      }
    }

    currentBody?.append(eventRow(event, t0, attempts.get(key)?.attempt));
  }

  if (trace.events.length === 0) {
    container.append(el('p', { className: 'empty', text: 'No events yet.' }));
  }
  return container;
}

function attemptHeader(
  step: TraceStepView,
  attempt: AttemptView,
  onSelect: (step: string) => void,
): HTMLElement {
  const badges: HTMLElement[] = [badge(attempt.state, attempt.state)];
  if (attempt.leaseExtensions > 0) {
    badges.push(badge(`lease renewed ×${attempt.leaseExtensions}`, 'renew'));
  }
  if (attempt.leaseExpired) badges.push(badge('lease expired', 'lease'));
  if (attempt.willRetry === true) badges.push(badge('retry to follow', 'retry'));
  if (attempt.retry !== undefined) {
    badges.push(badge(`retry in ${formatDuration(attempt.retry.backoffMs)}`, 'retry'));
  }
  if (attempt.errorClass !== undefined) badges.push(badge(attempt.errorClass, 'bad'));

  const meta: string[] = [];
  if (attempt.workerId !== undefined) meta.push(`worker ${attempt.workerId}`);
  if (attempt.queuedMs !== undefined) meta.push(`queued ${formatDuration(attempt.queuedMs)}`);
  if (attempt.runningMs !== undefined) meta.push(`ran ${formatDuration(attempt.runningMs)}`);
  meta.push(`step total ${formatDuration(step.totalMs)}`);

  const title = el('button', {
    className: 'group-title',
    text: `${step.step} · attempt ${attempt.attempt} of ${step.attempts.length}`,
    attrs: { type: 'button' },
    on: { click: () => onSelect(step.step) },
  });

  return el('div', {
    className: 'group-head',
    children: [
      title,
      el('span', { className: 'group-badges', children: badges }),
      el('span', { className: 'group-meta', text: meta.join(' · ') }),
    ],
  });
}

function eventRow(event: EventView, t0: number, attempt: AttemptView | undefined): HTMLElement {
  const tone = EVENT_TONE[event.type] ?? 'neutral';
  const children: (Node | undefined)[] = [
    el('span', { className: 'row-seq', text: String(event.sequence) }),
    el('span', { className: 'row-clock', text: formatClock(event.at) }),
    el('span', { className: 'row-elapsed', text: `+${formatDuration(event.at - t0)}` }),
    el('span', { className: `row-type row-type-${tone}`, text: event.type }),
    el('span', { className: 'row-detail', text: detailOf(event) }),
  ];

  const row = el('div', { className: `row row-${tone}`, children });

  const call = matchToolCall(event, attempt);
  if (call !== undefined) {
    return el('div', { className: 'row-with-detail', children: [row, toolDetails(call)] });
  }
  if (event.type === 'approval.requested' && event.payload !== undefined) {
    return el('div', {
      className: 'row-with-detail',
      children: [row, expandable('payload being authorized', pre(event.payload))],
    });
  }
  if (event.type === 'step.completed' && event.output !== undefined) {
    return el('div', {
      className: 'row-with-detail',
      children: [row, expandable('step output', pre(event.output))],
    });
  }
  return row;
}

/**
 * Finds the tool call a `tool.*` event belongs to.
 *
 * Matched on the request sequence for `tool.requested`, and otherwise on the latest
 * call of the same tool in the attempt — the same rule the server-side trace builder
 * uses, since the events carry no call identifier.
 */
function matchToolCall(event: EventView, attempt: AttemptView | undefined): ToolCallView | undefined {
  if (attempt === undefined || event.tool === undefined) return undefined;
  if (event.type === 'tool.requested') {
    return attempt.toolCalls.find((c) => c.requestedSequence === event.sequence);
  }
  if (event.type !== 'tool.succeeded' && event.type !== 'tool.failed') return undefined;
  const candidates = attempt.toolCalls.filter((c) => c.tool === event.tool);
  return candidates[candidates.length - 1];
}

function toolDetails(call: ToolCallView): HTMLElement {
  const summary = `${call.tool} — ${call.outcome}${
    call.durationMs !== undefined ? ` in ${formatDuration(call.durationMs)}` : ''
  }${call.deduplicated === true ? ' (deduplicated)' : ''}`;

  return expandable(
    summary,
    el('div', {
      className: 'tool-body',
      children: [
        el('div', {
          className: 'tool-key',
          text: `idempotency key: ${call.idempotencyKey}`,
        }),
        el('h4', { text: 'arguments' }),
        pre(call.args),
        el('h4', { text: call.outcome === 'failed' ? 'error' : 'result' }),
        call.outcome === 'failed'
          ? pre({ error: call.error, errorClass: call.errorClass })
          : pre(call.outcome === 'pending' ? '(no outcome recorded yet)' : call.result),
      ],
    }),
  );
}

/** A `<details>`/`<summary>` pair: keyboard-operable and disclosed without JavaScript. */
function expandable(summary: string, body: Node): HTMLElement {
  const details = el('details', { className: 'expandable' });
  details.append(el('summary', { text: summary }), body);
  return details;
}

function detailOf(event: EventView): string {
  const parts: string[] = [];
  switch (event.type) {
    case 'workflow.created':
      parts.push(`${event.workflow ?? ''} v${event.definitionVersion ?? 0}`);
      if (event.tenant !== undefined) parts.push(`tenant ${event.tenant}`);
      break;
    case 'workflow.failed':
      parts.push(`${event.errorClass ?? ''}: ${event.error ?? ''}`);
      break;
    case 'workflow.cancelled':
      parts.push(`reason: ${event.reason ?? ''}`);
      break;
    case 'step.leased':
      parts.push(`${event.workerId ?? ''}`);
      if (event.leaseExpiresAt !== undefined) parts.push(`lease until ${formatClock(event.leaseExpiresAt)}`);
      break;
    case 'step.started':
    case 'step.completed':
      if (event.workerId !== undefined) parts.push(event.workerId);
      break;
    case 'step.failed':
      parts.push(`${event.errorClass ?? ''}: ${event.error ?? ''}`);
      parts.push(event.willRetry === true ? 'a retry will follow' : 'terminal for this step');
      break;
    case 'step.lease_extended':
      parts.push(`${event.workerId ?? ''} renewed its lease`);
      if (event.leaseExpiresAt !== undefined) parts.push(`now until ${formatClock(event.leaseExpiresAt)}`);
      parts.push('the step is slow, not stuck');
      break;
    case 'step.retry_scheduled':
      parts.push(`attempt ${event.attempt ?? 0} after ${formatDuration(event.backoffMs)}`);
      break;
    case 'step.lease_expired':
      parts.push(`${event.workerId ?? ''} lost its lease — the step may still be running`);
      break;
    case 'tool.requested':
    case 'tool.succeeded':
    case 'tool.failed':
      parts.push(event.tool ?? '');
      if (event.error !== undefined) parts.push(`${event.errorClass ?? ''}: ${event.error}`);
      break;
    case 'approval.requested':
      parts.push(event.prompt ?? '');
      parts.push(`approvers: ${(event.approvers ?? []).join(', ')}`);
      break;
    case 'approval.granted':
      parts.push(event.approver ?? '');
      if (event.note !== undefined) parts.push(`note: ${event.note}`);
      break;
    case 'approval.denied':
      parts.push(`${event.approver ?? ''} — ${event.reason ?? ''}`);
      break;
    default:
      break;
  }
  return parts.filter((p) => p !== '').join(' · ');
}

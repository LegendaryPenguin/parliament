/**
 * The approval panel.
 *
 * The payload is shown in full above the buttons, not behind a disclosure. An approval
 * gate whose "approve" button is easier to reach than the thing being approved is a
 * rubber stamp with extra steps.
 *
 * Denial requires a reason and the form enforces it client-side as well as the server
 * doing so, because the useful place to be told is next to the field.
 */

import { badge, el, formatDuration, formatWhen, pre } from './dom.js';
import type { ApprovalView } from './types.js';

export interface ApprovalOptions {
  readonly approvals: readonly ApprovalView[];
  readonly approver: string;
  readonly onApproverChange: (approver: string) => void;
  readonly onApprove: (step: string, note: string | undefined) => void;
  readonly onDeny: (step: string, reason: string) => void;
  readonly busy: boolean;
}

export function renderApprovals(options: ApprovalOptions): HTMLElement {
  const pending = options.approvals.filter((a) => a.decision === 'pending');
  const decided = options.approvals.filter((a) => a.decision !== 'pending');

  const container = el('div', { className: 'approvals' });

  const identity = el('input', {
    className: 'input',
    attrs: {
      type: 'text',
      id: 'approver-identity',
      placeholder: 'you@example.com',
      value: options.approver,
      autocomplete: 'email',
    },
  });
  identity.addEventListener('change', () => options.onApproverChange(identity.value.trim()));

  container.append(
    el('div', {
      className: 'approver-identity',
      children: [
        el('label', { text: 'Acting as', attrs: { for: 'approver-identity' } }),
        identity,
        el('span', {
          className: 'hint',
          text: 'Recorded on the decision. The server rejects an identity that is not on the approver list.',
        }),
      ],
    }),
  );

  if (pending.length === 0) {
    container.append(el('p', { className: 'empty', text: 'Nothing is waiting on a human.' }));
  }

  for (const approval of pending) {
    container.append(pendingCard(approval, options));
  }

  if (decided.length > 0) {
    container.append(el('h3', { text: 'Decided' }));
    for (const approval of decided) container.append(decidedCard(approval));
  }

  return container;
}

function pendingCard(approval: ApprovalView, options: ApprovalOptions): HTMLElement {
  const note = el('textarea', {
    className: 'input',
    attrs: {
      rows: 2,
      placeholder: 'Note (optional for approval, required to deny)',
      'aria-label': `Note or denial reason for ${approval.step}`,
    },
  });
  const error = el('p', { className: 'form-error', attrs: { role: 'alert' } });

  const approve = el('button', {
    className: 'btn btn-approve',
    text: 'Approve',
    attrs: { type: 'button', disabled: options.busy },
    on: {
      click: () => {
        if (options.approver === '') {
          error.textContent = 'Set who you are acting as first.';
          return;
        }
        const text = note.value.trim();
        options.onApprove(approval.step, text === '' ? undefined : text);
      },
    },
  });

  const deny = el('button', {
    className: 'btn btn-deny',
    text: 'Deny',
    attrs: { type: 'button', disabled: options.busy },
    on: {
      click: () => {
        if (options.approver === '') {
          error.textContent = 'Set who you are acting as first.';
          return;
        }
        const reason = note.value.trim();
        if (reason === '') {
          error.textContent = 'A denial needs a reason — it is the only record of why this stopped.';
          note.focus();
          return;
        }
        options.onDeny(approval.step, reason);
      },
    },
  });

  return el('section', {
    className: 'card approval-card',
    children: [
      el('header', {
        className: 'card-head',
        children: [
          el('h3', { text: approval.step }),
          badge('awaiting decision', 'waiting'),
          el('span', {
            className: 'card-meta',
            text: `requested ${formatWhen(approval.requestedAt)} · waiting ${formatDuration(
              Date.now() - approval.requestedAt,
            )}`,
          }),
        ],
      }),
      el('p', { className: 'approval-prompt', text: approval.prompt }),
      el('p', { className: 'hint', text: `Approvers: ${approval.approvers.join(', ')}` }),
      el('h4', { text: 'Being authorized' }),
      pre(approval.payload),
      note,
      error,
      el('div', { className: 'btn-row', children: [approve, deny] }),
    ],
  });
}

function decidedCard(approval: ApprovalView): HTMLElement {
  return el('section', {
    className: 'card approval-card approval-decided',
    children: [
      el('header', {
        className: 'card-head',
        children: [
          el('h3', { text: approval.step }),
          badge(approval.decision, approval.decision === 'granted' ? 'succeeded' : 'failed'),
          el('span', {
            className: 'card-meta',
            text: `${approval.approver ?? 'unknown'} · waited ${formatDuration(approval.waitMs)}`,
          }),
        ],
      }),
      approval.reason !== undefined
        ? el('p', { className: 'approval-reason', text: `Reason: ${approval.reason}` })
        : undefined,
      approval.note !== undefined ? el('p', { className: 'hint', text: `Note: ${approval.note}` }) : undefined,
    ],
  });
}

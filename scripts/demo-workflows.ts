/**
 * The workflow definitions the demo submits.
 *
 * One definition per property being demonstrated, rather than one definition exercising
 * everything. A single graph with a retry, a gate, a long hold and a fan-out would prove the
 * same things and would be unreadable: when the demo prints a projection, a reader has to be
 * able to see at a glance which step is the point.
 *
 * The lease duration is the number that makes the demo watchable. Production would use tens of
 * seconds — a lease that lapses while a healthy worker is merely slow causes exactly the double
 * execution leasing exists to bound — but a demo that waits thirty seconds to show a recovery
 * is a demo nobody runs twice. `LEASE_MS` is the one concession to the format, and it is a
 * concession to *time*, not to mechanism: the code path is identical.
 */

import { type RetryPolicy, type Timeouts, type WorkflowDef } from '../src/workflow/types.js';

/** Short enough that a reader sees the reassignment happen; see the note above. */
export const LEASE_MS = 2_500;

const TIMEOUTS: Timeouts = {
  // Comfortably longer than the longest `demo.hold` in the demo. An attempt timeout that
  // fired first would turn the killed-worker scenario into a timeout-and-retry scenario,
  // which is a different mechanism with a different event trail.
  attemptTimeoutMs: 30_000,
  leaseDurationMs: LEASE_MS,
};

/** Retries only `unavailable`, which is the class `demo.flaky` raises. */
export const DEMO_RETRY: RetryPolicy = {
  maxAttempts: 3,
  initialBackoffMs: 250,
  maxBackoffMs: 1_000,
  // Zero jitter so the demo's timing is reproducible. Production wants jitter: without it,
  // every step that fails at the same instant retries at the same instant forever.
  jitter: 0,
  retryableErrors: ['unavailable'],
};

/**
 * The happy path: a diamond with a tool, a model call, a fan-in and a finalize.
 *
 * `intake` produces the prompt that `classify` sends, because the control plane hands a step
 * its dependency's output verbatim. That is also the honest shape — a prompt built from
 * fetched data cannot live in a static definition.
 */
export const REVIEW: WorkflowDef = {
  name: 'review',
  version: 1,
  start: 'intake',
  timeouts: TIMEOUTS,
  nodes: [
    { name: 'intake', kind: 'tool', tool: 'demo.extract', dependsOn: [] },
    { name: 'classify', kind: 'llm', dependsOn: ['intake'] },
    { name: 'screen', kind: 'tool', tool: 'demo.screen', dependsOn: ['intake'] },
    { name: 'decide', kind: 'join', dependsOn: ['classify', 'screen'] },
    { name: 'report', kind: 'finalize', dependsOn: ['decide'] },
  ],
};

/** A step that fails twice and succeeds on the third attempt, with a policy that allows it. */
export const RETRYING: WorkflowDef = {
  name: 'flaky-with-retry',
  version: 1,
  start: 'work',
  timeouts: TIMEOUTS,
  nodes: [{ name: 'work', kind: 'tool', tool: 'demo.flaky', dependsOn: [], retry: DEMO_RETRY }],
};

/**
 * The same failing step with no retry policy at all.
 *
 * The control group. An absent policy means one attempt and nothing retryable — not a default
 * policy — so this workflow fails where the one above succeeds, from the same tool failing the
 * same way. Without this pair, "the retry worked" is unfalsifiable.
 */
export const NOT_RETRYING: WorkflowDef = {
  name: 'flaky-no-retry',
  version: 1,
  start: 'work',
  timeouts: TIMEOUTS,
  nodes: [{ name: 'work', kind: 'tool', tool: 'demo.flaky', dependsOn: [] }],
};

/** A step that occupies a worker long enough for the worker to be killed underneath it. */
export const HOLDING: WorkflowDef = {
  name: 'long-hold',
  version: 1,
  start: 'hold',
  timeouts: TIMEOUTS,
  nodes: [
    // Deliberately no retry policy. If the workflow recovers, it cannot be because the
    // failure was retried — a lost worker is not a failed step, and the event trail says
    // `step.lease_expired`, not `step.failed`.
    { name: 'hold', kind: 'tool', tool: 'demo.hold', dependsOn: [] },
    { name: 'wrap', kind: 'finalize', dependsOn: ['hold'] },
  ],
};

/** A human gate between preparing a payout and settling it. */
export const GATED: WorkflowDef = {
  name: 'gated-payout',
  version: 1,
  start: 'prepare',
  timeouts: TIMEOUTS,
  nodes: [
    { name: 'prepare', kind: 'tool', tool: 'demo.extract', dependsOn: [] },
    {
      name: 'approve',
      kind: 'approval',
      dependsOn: ['prepare'],
      approvers: ['alice@example.com'],
      approvalPrompt: 'Release the payout for this record?',
    },
    { name: 'settle', kind: 'tool', tool: 'demo.ledger', dependsOn: ['approve'] },
  ],
};

/** One observable side effect, so the demo can count how many times it happened. */
export const LEDGER: WorkflowDef = {
  name: 'ledger-write',
  version: 1,
  start: 'write',
  timeouts: TIMEOUTS,
  nodes: [
    { name: 'write', kind: 'tool', tool: 'demo.ledger', dependsOn: [] },
    { name: 'confirm', kind: 'finalize', dependsOn: ['write'] },
  ],
};

export const ALL: readonly WorkflowDef[] = [REVIEW, RETRYING, NOT_RETRYING, HOLDING, GATED, LEDGER];

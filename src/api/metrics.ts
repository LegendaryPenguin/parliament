/**
 * Prometheus exposition, written by hand.
 *
 * Two decisions worth stating.
 *
 * **Histograms, not averages.** Every latency here is a bucketed histogram. A mean
 * step duration is actively misleading for this system: the interesting runs are the
 * ones that sat in a retry backoff for thirty seconds or waited four hours on a human,
 * and a mean over mostly-fast steps hides exactly those. Buckets let an operator ask
 * "what fraction of approvals took longer than ten minutes", which is the question
 * that actually gets asked.
 *
 * **Derived from the log at scrape time, not accumulated.** The counters are recomputed
 * by folding histories on each scrape. That is O(events) per scrape and would not
 * survive a large durable store — a real deployment would maintain an incremental
 * registry fed by the same appends that drive the change feed. The upside, and the
 * reason it is right for now, is that the numbers cannot drift from the log: there is
 * no second source of truth to reconcile, and a counter that disagrees with the
 * history it claims to summarize is worse than a slow scrape.
 */

import type { Event, WorkflowProjection } from '../events/types.js';
import type { RunState } from '../workflow/types.js';
import type { WorkflowCost } from './cost.js';

/**
 * Bucket edges in milliseconds, from "fast tool call" to "waited an hour".
 *
 * Wide on purpose: this system's durations span six orders of magnitude, because a
 * cached tool call and a human approval are both measured here.
 */
export const DURATION_BUCKETS_MS: readonly number[] = [
  10, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000, 300_000, 1_800_000, 3_600_000,
];

type Labels = Readonly<Record<string, string>>;

/** Identity of a series. Sorted so two equal label sets never produce two series. */
function labelKey(labels: Labels): string {
  return Object.keys(labels)
    .sort()
    .map((key) => `${key}=${labels[key] ?? ''}`)
    .join(',');
}

class Counter {
  private readonly values = new Map<string, { labels: Labels; value: number }>();

  add(labels: Labels, delta = 1): void {
    const key = labelKey(labels);
    const existing = this.values.get(key);
    if (existing) existing.value += delta;
    else this.values.set(key, { labels, value: delta });
  }

  entries(): readonly { labels: Labels; value: number }[] {
    return [...this.values.values()].sort((a, b) => labelKey(a.labels).localeCompare(labelKey(b.labels)));
  }
}

class Histogram {
  private readonly series = new Map<string, { labels: Labels; counts: number[]; sum: number; count: number }>();

  constructor(private readonly buckets: readonly number[] = DURATION_BUCKETS_MS) {}

  observe(labels: Labels, value: number): void {
    const key = labelKey(labels);
    let series = this.series.get(key);
    if (!series) {
      series = { labels, counts: new Array(this.buckets.length).fill(0), sum: 0, count: 0 };
      this.series.set(key, series);
    }
    series.sum += value;
    series.count += 1;
    for (let i = 0; i < this.buckets.length; i += 1) {
      if (value <= (this.buckets[i] as number)) series.counts[i] = (series.counts[i] as number) + 1;
    }
  }

  render(name: string, help: string): string[] {
    const lines = [`# HELP ${name} ${help}`, `# TYPE ${name} histogram`];
    const sorted = [...this.series.values()].sort((a, b) =>
      labelKey(a.labels).localeCompare(labelKey(b.labels)),
    );
    for (const series of sorted) {
      this.buckets.forEach((edge, i) => {
        lines.push(sample(`${name}_bucket`, { ...series.labels, le: formatNumber(edge) }, series.counts[i] as number));
      });
      // `+Inf` is mandatory and must equal `_count`; a histogram without it is
      // rejected by Prometheus rather than partially ingested.
      lines.push(sample(`${name}_bucket`, { ...series.labels, le: '+Inf' }, series.count));
      lines.push(sample(`${name}_sum`, series.labels, series.sum));
      lines.push(sample(`${name}_count`, series.labels, series.count));
    }
    return lines;
  }

  get isEmpty(): boolean {
    return this.series.size === 0;
  }
}

/** One workflow's inputs to a scrape. */
export interface MetricsInput {
  readonly projection: WorkflowProjection;
  readonly events: readonly Event[];
  readonly cost: WorkflowCost;
}

const WORKFLOW_STATES: readonly RunState[] = [
  'pending',
  'ready',
  'running',
  'waiting',
  'retrying',
  'succeeded',
  'failed',
  'cancelled',
];

/**
 * Renders the whole registry in Prometheus text format.
 *
 * Zero-valued series for every known state are emitted deliberately. A missing series
 * and a series at zero look the same in a graph but behave differently in an alert:
 * `parliament_workflows{state="failed"} == 0` only fires if the series exists.
 */
export function renderMetrics(inputs: readonly MetricsInput[]): string {
  const workflows = new Counter();
  const steps = new Counter();
  const stepAttempts = new Counter();
  const retries = new Counter();
  const leaseExpirations = new Counter();
  const leaseExtensions = new Counter();
  const toolInvocations = new Counter();
  const toolFailures = new Counter();
  const costUnits = new Counter();
  const events = new Counter();
  const approvalsPending = new Counter();

  const toolDuration = new Histogram();
  const approvalWait = new Histogram();
  const workflowDuration = new Histogram();
  const stepDuration = new Histogram();

  for (const state of WORKFLOW_STATES) {
    workflows.add({ state }, 0);
    steps.add({ state }, 0);
  }

  for (const { projection, events: history, cost } of inputs) {
    const workflow = projection.workflow || 'unknown';
    const tenant = projection.tenant ?? 'none';

    workflows.add({ state: projection.state });
    events.add({ workflow }, history.length);

    for (const step of projection.steps.values()) {
      steps.add({ state: step.state });
      if (step.awaitingApproval) approvalsPending.add({ workflow });
    }

    costUnits.add({ tenant, kind: 'tool' }, cost.toolUnits);
    // Separate series rather than one total, because one of these is a sum of declared
    // units and the other is an estimate; adding them in the exporter would make the
    // estimate indistinguishable downstream.
    costUnits.add({ tenant, kind: 'tokens_estimated' }, cost.tokenUnits);

    const startedAt = new Map<string, number>();
    let createdAt = projection.createdAt;
    const approvalRequestedAt = new Map<string, number>();

    for (const event of history) {
      switch (event.type) {
        case 'workflow.created':
          createdAt = event.at;
          break;

        case 'workflow.completed':
          workflowDuration.observe({ workflow, outcome: 'succeeded' }, event.at - createdAt);
          break;

        case 'workflow.failed':
          workflowDuration.observe({ workflow, outcome: 'failed' }, event.at - createdAt);
          break;

        case 'workflow.cancelled':
          workflowDuration.observe({ workflow, outcome: 'cancelled' }, event.at - createdAt);
          break;

        case 'step.scheduled':
          stepAttempts.add({ workflow, step: event.step });
          break;

        case 'step.started':
          startedAt.set(`${event.step}#${event.attempt}`, event.at);
          break;

        case 'step.completed':
        case 'step.failed': {
          const key = `${event.step}#${event.attempt}`;
          const began = startedAt.get(key);
          if (began !== undefined) {
            const outcome = event.type === 'step.completed' ? 'succeeded' : 'failed';
            stepDuration.observe({ workflow, step: event.step, outcome }, event.at - began);
            startedAt.delete(key);
          }
          break;
        }

        case 'step.retry_scheduled':
          retries.add({ workflow, step: event.step });
          break;

        case 'step.lease_expired':
          leaseExpirations.add({ workflow, step: event.step });
          break;

        case 'step.lease_extended':
          // Renewals are the signal that separates a slow step from a stuck one, so they
          // are counted rather than folded into the lease gauge they overwrite.
          leaseExtensions.add({ workflow, step: event.step });
          break;

        case 'tool.requested':
          toolInvocations.add({ tool: event.tool });
          break;

        case 'tool.succeeded':
          toolDuration.observe({ tool: event.tool }, event.durationMs);
          break;

        case 'tool.failed':
          toolFailures.add({ tool: event.tool, error_class: event.errorClass });
          break;

        case 'approval.requested':
          approvalRequestedAt.set(event.step, event.at);
          break;

        case 'approval.granted':
        case 'approval.denied': {
          const requested = approvalRequestedAt.get(event.step);
          if (requested !== undefined) {
            const decision = event.type === 'approval.granted' ? 'granted' : 'denied';
            approvalWait.observe({ workflow, decision }, event.at - requested);
            approvalRequestedAt.delete(event.step);
          }
          break;
        }

        case 'step.leased':
          break;
      }
    }
  }

  const lines: string[] = [];
  const push = (name: string, help: string, type: 'counter' | 'gauge', counter: Counter): void => {
    const entries = counter.entries();
    if (entries.length === 0) return;
    lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`);
    for (const entry of entries) lines.push(sample(name, entry.labels, entry.value));
  };

  push('parliament_workflows', 'Workflow executions by current state.', 'gauge', workflows);
  push('parliament_steps', 'Steps by current state across all workflows.', 'gauge', steps);
  push('parliament_approvals_pending', 'Steps currently blocked on a human decision.', 'gauge', approvalsPending);
  push('parliament_step_attempts_total', 'Step attempts scheduled.', 'counter', stepAttempts);
  push('parliament_step_retries_total', 'Retries scheduled after a step attempt failed.', 'counter', retries);
  push(
    'parliament_lease_expirations_total',
    'Leases that expired before the worker reported an outcome.',
    'counter',
    leaseExpirations,
  );
  push(
    'parliament_lease_extensions_total',
    'Lease renewals: a worker heartbeating while a step is still running.',
    'counter',
    leaseExtensions,
  );
  push('parliament_tool_invocations_total', 'Tool calls requested.', 'counter', toolInvocations);
  push('parliament_tool_failures_total', 'Tool calls that failed, by error class.', 'counter', toolFailures);
  push(
    'parliament_cost_units_total',
    'Cost units by tenant. kind="tokens_estimated" is an estimate from the fake model provider, not a billed figure.',
    'counter',
    costUnits,
  );
  push('parliament_events_total', 'Events in the log, by workflow definition.', 'counter', events);

  if (!toolDuration.isEmpty) {
    lines.push(...toolDuration.render('parliament_tool_duration_ms', 'Tool call wall-clock duration.'));
  }
  if (!stepDuration.isEmpty) {
    lines.push(
      ...stepDuration.render('parliament_step_duration_ms', 'Per-attempt step duration, from started to outcome.'),
    );
  }
  if (!approvalWait.isEmpty) {
    lines.push(
      ...approvalWait.render('parliament_approval_wait_ms', 'Time from approval request to human decision.'),
    );
  }
  if (!workflowDuration.isEmpty) {
    lines.push(
      ...workflowDuration.render('parliament_workflow_duration_ms', 'Workflow duration from creation to terminal state.'),
    );
  }

  // Exposition must end with a newline; some scrapers drop a final unterminated line.
  return `${lines.join('\n')}\n`;
}

function sample(name: string, labels: Labels, value: number): string {
  const rendered = renderLabels(labels);
  return `${name}${rendered} ${formatNumber(value)}`;
}

function renderLabels(labels: Labels): string {
  const keys = Object.keys(labels).sort();
  if (keys.length === 0) return '';
  const parts = keys.map((key) => `${sanitizeLabelName(key)}="${escapeLabelValue(labels[key] ?? '')}"`);
  return `{${parts.join(',')}}`;
}

/** Label names are restricted; a step named `send-email` would otherwise be invalid. */
function sanitizeLabelName(name: string): string {
  const cleaned = name.replace(/[^a-zA-Z0-9_]/g, '_');
  return /^[a-zA-Z_]/.test(cleaned) ? cleaned : `_${cleaned}`;
}

function escapeLabelValue(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
}

/**
 * Formats a value for exposition.
 *
 * Integers are emitted without a decimal point and fractions without exponent
 * notation, because `1e-7` is legal but trips naive scrapers and log pipelines.
 */
function formatNumber(value: number): string {
  if (!Number.isFinite(value)) return value > 0 ? '+Inf' : '-Inf';
  if (Number.isInteger(value)) return String(value);
  return value.toFixed(4).replace(/0+$/, '').replace(/\.$/, '');
}

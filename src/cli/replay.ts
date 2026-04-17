/**
 * `replay` — read a workflow's history the way you would read a stack trace.
 *
 * This is the tool reached for when a workflow is in a state nobody expected, so the
 * design goal is not completeness but *legibility at a glance*: the failing step and
 * the reason should be findable without reading the whole timeline, and the abnormal
 * events — retries, lease expiries, denials — should be visually distinct from the
 * forty routine ones around them. Hence the marker column and the summary tables; a
 * raw `JSON.stringify` of the event array is already available over HTTP and is not
 * what anyone wants at 3am.
 *
 * Usage:
 *   replay <workflow-id> [--at <sequence>] [--url <api>] [--file <history.json>]
 *   replay --demo                      seed an in-process history and print it
 *
 * `--at` prints the projection as of that sequence instead of the head, which is the
 * command-line half of the dashboard's replay slider.
 */

import { readFile } from 'node:fs/promises';
import type { Event, Sequence, WorkflowProjection } from '../events/types.js';
import type { RunState, StepName, WorkflowDef, WorkflowId } from '../workflow/types.js';
import { buildTrace, type Trace, type TraceStep } from '../api/trace.js';
import { synthesizeDefinition } from '../api/projection.js';
import { project } from '../state/project.js';
import { replayTo } from '../state/replay.js';
import { defaultWiring, seedDemo } from '../api/wiring.js';

interface Args {
  readonly workflowId?: string;
  readonly at?: number;
  readonly url: string;
  readonly file?: string;
  readonly demo: boolean;
  readonly colour: boolean;
}

function parseArgs(argv: readonly string[]): Args {
  let workflowId: string | undefined;
  let at: number | undefined;
  let url = process.env['PARLIAMENT_API'] ?? 'http://localhost:8787';
  let file: string | undefined;
  let demo = false;
  let colour = process.stdout.isTTY === true && process.env['NO_COLOR'] === undefined;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i] as string;
    const next = (): string => {
      const value = argv[i + 1];
      if (value === undefined) throw new Error(`${arg} requires a value`);
      i += 1;
      return value;
    };
    switch (arg) {
      case '--at': {
        const value = Number(next());
        if (!Number.isInteger(value) || value < 1) throw new Error('--at requires a sequence >= 1');
        at = value;
        break;
      }
      case '--url':
        url = next();
        break;
      case '--file':
        file = next();
        break;
      case '--demo':
        demo = true;
        break;
      case '--no-color':
      case '--no-colour':
        colour = false;
        break;
      case '--color':
        colour = true;
        break;
      default:
        if (arg.startsWith('-')) throw new Error(`unknown flag ${arg}`);
        workflowId = arg;
    }
  }

  return {
    url,
    demo,
    colour,
    ...(workflowId !== undefined ? { workflowId } : {}),
    ...(at !== undefined ? { at } : {}),
    ...(file !== undefined ? { file } : {}),
  };
}

interface History {
  readonly events: readonly Event[];
  readonly definition?: WorkflowDef;
}

/**
 * Three sources, because the CLI is used in three situations: against a running
 * control plane, against a history someone pasted into a file from a bug report, and
 * against nothing at all while working on the output format itself.
 */
async function loadHistory(args: Args): Promise<History> {
  if (args.demo) {
    const wiring = defaultWiring();
    const seeded = await seedDemo(wiring);
    const id = (args.workflowId ?? seeded) as WorkflowId;
    const events = await wiring.store.read(id);
    const definition = wiring.definitions.get(id);
    return { events, ...(definition !== undefined ? { definition } : {}) };
  }

  if (args.file !== undefined) {
    const parsed = JSON.parse(await readFile(args.file, 'utf8')) as unknown;
    const events = Array.isArray(parsed)
      ? (parsed as Event[])
      : ((parsed as { events?: Event[] }).events ?? []);
    const definition = (parsed as { definition?: WorkflowDef }).definition;
    return { events, ...(definition !== undefined ? { definition } : {}) };
  }

  if (args.workflowId === undefined) throw new Error('a workflow id is required (or --demo)');
  const base = args.url.replace(/\/$/, '');
  const id = encodeURIComponent(args.workflowId);

  const eventsResponse = await fetch(`${base}/workflows/${id}/events`);
  if (!eventsResponse.ok) {
    const body = (await eventsResponse.json().catch(() => ({}))) as { message?: string };
    throw new Error(`GET /workflows/${args.workflowId}/events -> ${eventsResponse.status}: ${body.message ?? ''}`);
  }
  const { events } = (await eventsResponse.json()) as { events: Event[] };

  // The trace endpoint is the only place the submitted definition is exposed, and
  // without it the dependency graph — and so the critical path — cannot be drawn.
  const traceResponse = await fetch(`${base}/workflows/${id}/trace`);
  const trace = traceResponse.ok ? ((await traceResponse.json()) as { steps?: TraceStep[] }) : undefined;
  const definition = trace?.steps ? definitionFromSteps(trace.steps) : undefined;

  return { events, ...(definition !== undefined ? { definition } : {}) };
}

/**
 * Rebuilds just enough of a definition from a served trace to lay out the graph.
 *
 * The API exposes the resolved graph rather than the raw definition, so the shape is
 * reconstructed from it. Only `dependsOn` and `kind` matter for this output.
 */
function definitionFromSteps(steps: readonly TraceStep[]): WorkflowDef {
  return {
    name: 'reconstructed',
    version: 0,
    start: steps[0]?.step ?? '',
    timeouts: { attemptTimeoutMs: 0, leaseDurationMs: 0 },
    nodes: steps.map((s) => ({
      name: s.step,
      kind: s.kind ?? 'tool',
      dependsOn: s.dependsOn,
    })),
  };
}

const ANSI = {
  reset: '\u001B[0m',
  dim: '\u001B[2m',
  bold: '\u001B[1m',
  red: '\u001B[31m',
  green: '\u001B[32m',
  yellow: '\u001B[33m',
  blue: '\u001B[34m',
  magenta: '\u001B[35m',
  cyan: '\u001B[36m',
} as const;

type Colour = keyof typeof ANSI;

function makePaint(enabled: boolean) {
  return (text: string, ...colours: Colour[]): string =>
    enabled ? `${colours.map((c) => ANSI[c]).join('')}${text}${ANSI.reset}` : text;
}

/** Visible length, so padding is not thrown off by escape sequences. */
function width(text: string): number {
  return text.replace(/\u001B\[[0-9;]*m/g, '').length;
}

function pad(text: string, to: number): string {
  const spaces = to - width(text);
  return spaces > 0 ? text + ' '.repeat(spaces) : text;
}

function padStart(text: string, to: number): string {
  const spaces = to - width(text);
  return spaces > 0 ? ' '.repeat(spaces) + text : text;
}

const STATE_COLOUR: Readonly<Record<RunState, Colour>> = {
  pending: 'dim',
  ready: 'cyan',
  running: 'blue',
  waiting: 'yellow',
  retrying: 'magenta',
  succeeded: 'green',
  failed: 'red',
  cancelled: 'dim',
};

function formatDuration(ms: number | undefined): string {
  if (ms === undefined) return '-';
  if (ms < 1_000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
  const minutes = Math.floor(ms / 60_000);
  return `${minutes}m${Math.round((ms % 60_000) / 1_000)}s`;
}

function clockOf(at: number): string {
  return new Date(at).toISOString().slice(11, 23);
}

/**
 * The state change an event implies, rendered as `from -> to`.
 *
 * Shown on the event rather than left to be inferred from the event name, because the
 * mapping is not obvious in the two cases that matter: a lease expiry moves a step
 * *back* to `ready`, and a retry schedules the next attempt rather than resuming the
 * failed one.
 */
function transitionFor(event: Event, before: RunState | undefined): string | undefined {
  const to = (state: RunState): string => `${before ?? 'none'} -> ${state}`;
  switch (event.type) {
    case 'step.scheduled':
      return to(event.notBefore !== undefined ? 'pending' : 'ready');
    case 'step.started':
      return to('running');
    case 'step.completed':
      return to('succeeded');
    case 'approval.denied':
      return to('failed');
    case 'step.failed':
      return to(event.willRetry ? 'retrying' : 'failed');
    case 'step.retry_scheduled':
      return to('retrying');
    case 'step.lease_expired':
      return to('ready');
    case 'step.lease_extended':
      // A renewal is not a transition: the step keeps running, which is exactly the
      // point of showing it differently from a lease being granted or lost.
      return undefined;
    case 'approval.requested':
      return to('waiting');
    case 'approval.granted':
      return to('ready');
    default:
      return undefined;
  }
}

interface Row {
  readonly marker: string;
  readonly markerColour: Colour | undefined;
  readonly seq: string;
  readonly clock: string;
  readonly elapsed: string;
  readonly step: string;
  readonly event: string;
  readonly detail: string;
}

function buildRows(events: readonly Event[]): Row[] {
  const t0 = events[0]?.at ?? 0;
  const states = new Map<StepName, RunState>();
  const rows: Row[] = [];

  for (const event of events) {
    const step = 'step' in event && typeof event.step === 'string' ? event.step : '';
    const before = step === '' ? undefined : states.get(step);
    const transition = transitionFor(event, before);
    if (transition !== undefined) {
      const to = transition.split(' -> ')[1] as RunState;
      states.set(step, to);
    }

    let marker = ' ';
    let markerColour: Colour | undefined;
    let detail = '';

    switch (event.type) {
      case 'workflow.created':
        detail = `${event.workflow} v${event.definitionVersion}${event.tenant !== undefined ? `  tenant=${event.tenant}` : ''}`;
        break;
      case 'workflow.completed':
        marker = '=';
        markerColour = 'green';
        detail = 'workflow succeeded';
        break;
      case 'workflow.failed':
        marker = 'X';
        markerColour = 'red';
        detail = `${event.errorClass}: ${event.error}`;
        break;
      case 'workflow.cancelled':
        marker = '/';
        markerColour = 'yellow';
        detail = `reason: ${event.reason}`;
        break;
      case 'step.scheduled':
        detail = `attempt ${event.attempt}${event.notBefore !== undefined ? `  not before ${clockOf(event.notBefore)}` : ''}`;
        break;
      case 'step.leased':
        detail = `attempt ${event.attempt}  ${event.workerId}  lease until ${clockOf(event.leaseExpiresAt)}`;
        break;
      case 'step.started':
        detail = `attempt ${event.attempt}  ${event.workerId}`;
        break;
      case 'step.completed':
        detail = `attempt ${event.attempt}  ${event.workerId}`;
        break;
      case 'step.failed':
        marker = 'x';
        markerColour = 'red';
        detail = `attempt ${event.attempt}  ${event.errorClass}: ${event.error}${event.willRetry ? '  (retry to follow)' : '  (terminal)'}`;
        break;
      case 'step.retry_scheduled':
        marker = '~';
        markerColour = 'magenta';
        detail = `attempt ${event.attempt} in ${formatDuration(event.backoffMs)}`;
        break;
      case 'step.lease_extended':
        marker = '^';
        markerColour = 'cyan';
        detail = `attempt ${event.attempt}  ${event.workerId} renewed its lease until ${clockOf(event.leaseExpiresAt)}`;
        break;
      case 'step.lease_expired':
        marker = '!';
        markerColour = 'yellow';
        detail = `attempt ${event.attempt}  ${event.workerId} lost its lease — step may still be running`;
        break;
      case 'tool.requested':
        marker = '>';
        markerColour = 'dim';
        detail = `${event.tool}(${preview(event.args)})`;
        break;
      case 'tool.succeeded':
        marker = '<';
        markerColour = 'dim';
        detail = `${event.tool} ok in ${formatDuration(event.durationMs)}${event.deduplicated ? ' (deduplicated)' : ''}`;
        break;
      case 'tool.failed':
        marker = 'x';
        markerColour = 'red';
        detail = `${event.tool} ${event.errorClass}: ${event.error}`;
        break;
      case 'approval.requested':
        marker = '?';
        markerColour = 'yellow';
        detail = `${event.approvers.join(', ')} — ${event.prompt}`;
        break;
      case 'approval.granted':
        marker = '+';
        markerColour = 'green';
        detail = `${event.approver}${event.note !== undefined ? ` — ${event.note}` : ''}`;
        break;
      case 'approval.denied':
        marker = 'x';
        markerColour = 'red';
        detail = `${event.approver} — ${event.reason}`;
        break;
    }

    rows.push({
      marker,
      markerColour,
      seq: String(event.sequence),
      clock: clockOf(event.at),
      elapsed: `+${formatDuration(event.at - t0)}`,
      step,
      event: event.type,
      detail: transition !== undefined ? `${transition}   ${detail}` : detail,
    });
  }

  return rows;
}

function preview(value: unknown, max = 60): string {
  const text = typeof value === 'string' ? value : JSON.stringify(value) ?? String(value);
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function renderTimeline(rows: readonly Row[], paint: ReturnType<typeof makePaint>): string[] {
  const w = {
    seq: Math.max(3, ...rows.map((r) => r.seq.length)),
    clock: 12,
    elapsed: Math.max(7, ...rows.map((r) => r.elapsed.length)),
    step: Math.max(4, ...rows.map((r) => r.step.length)),
    event: Math.max(5, ...rows.map((r) => r.event.length)),
  };

  const header = paint(
    [
      padStart('SEQ', w.seq),
      pad('TIME', w.clock),
      padStart('ELAPSED', w.elapsed),
      ' ',
      pad('STEP', w.step),
      pad('EVENT', w.event),
      'DETAIL',
    ].join('  '),
    'dim',
    'bold',
  );

  const lines = [header];
  for (const row of rows) {
    const marker = row.markerColour ? paint(row.marker, row.markerColour, 'bold') : row.marker;
    const stepText = row.step === '' ? paint('·', 'dim') : row.step;
    lines.push(
      [
        padStart(paint(row.seq, 'dim'), w.seq),
        pad(row.clock, w.clock),
        padStart(paint(row.elapsed, 'dim'), w.elapsed),
        marker,
        pad(stepText, w.step),
        pad(row.markerColour === 'red' ? paint(row.event, 'red') : row.event, w.event),
        row.detail,
      ].join('  '),
    );
  }
  return lines;
}

function renderSteps(trace: Trace, paint: ReturnType<typeof makePaint>): string[] {
  const rows = trace.steps.map((step) => {
    const last = step.attempts[step.attempts.length - 1];
    const flags: string[] = [];
    if (step.retries > 0) flags.push(paint(`retried x${step.retries}`, 'magenta'));
    if (step.leaseExtensions > 0) flags.push(paint(`lease renewed x${step.leaseExtensions}`, 'cyan'));
    if (step.leaseExpirations > 0) flags.push(paint(`lease lost x${step.leaseExpirations}`, 'yellow'));
    if (step.awaitingApproval) flags.push(paint('AWAITING APPROVAL', 'yellow', 'bold'));
    if (step.state === 'failed') flags.push(paint('FAILED', 'red', 'bold'));
    if (trace.criticalPath.includes(step.step)) {
      const failed = trace.failure !== undefined || trace.steps.some((s) => s.state === 'failed');
      flags.push(paint(failed ? 'on path to failure' : 'on critical path', 'dim'));
    }

    return {
      step: step.step,
      kind: step.kind ?? '-',
      state: paint(step.state, STATE_COLOUR[step.state]),
      attempts: String(step.attempts.length),
      tools: String(step.toolCalls),
      total: formatDuration(step.totalMs > 0 ? step.totalMs : undefined),
      worker: last?.workerId ?? '-',
      flags: flags.join('  '),
    };
  });

  const w = {
    step: Math.max(4, ...rows.map((r) => r.step.length)),
    kind: Math.max(4, ...rows.map((r) => r.kind.length)),
    state: Math.max(5, ...rows.map((r) => width(r.state))),
    attempts: 8,
    tools: 5,
    total: Math.max(5, ...rows.map((r) => r.total.length)),
    worker: Math.max(6, ...rows.map((r) => r.worker.length)),
  };

  const lines = [
    paint(
      [
        pad('STEP', w.step),
        pad('KIND', w.kind),
        pad('STATE', w.state),
        padStart('ATTEMPTS', w.attempts),
        padStart('TOOLS', w.tools),
        padStart('TIME', w.total),
        pad('WORKER', w.worker),
        '',
      ].join('  '),
      'dim',
      'bold',
    ),
  ];
  for (const row of rows) {
    lines.push(
      [
        pad(row.step, w.step),
        pad(paint(row.kind, 'dim'), w.kind),
        pad(row.state, w.state),
        padStart(row.attempts, w.attempts),
        padStart(row.tools, w.tools),
        padStart(row.total, w.total),
        pad(paint(row.worker, 'dim'), w.worker),
        row.flags,
      ].join('  '),
    );
  }
  return lines;
}

function renderHeader(trace: Trace, paint: ReturnType<typeof makePaint>): string[] {
  const label = (text: string): string => pad(paint(text, 'dim'), 10);
  return [
    `${label('workflow')}${paint(trace.workflow, 'bold')} v${trace.definitionVersion}   ${paint(trace.workflowId, 'cyan')}`,
    `${label('state')}${paint(trace.state.toUpperCase(), STATE_COLOUR[trace.state], 'bold')}   ${label('head')}${trace.sequence}   ${label('elapsed')}${formatDuration(trace.totalMs ?? trace.updatedAt - trace.createdAt)}`,
    `${label('tenant')}${trace.tenant ?? '-'}   ${label('priority')}${trace.priority ?? 'default'}   ${label('created')}${new Date(trace.createdAt).toISOString()}`,
    `${label('totals')}${trace.totals.events} events   ${trace.totals.attempts} attempts   ${trace.totals.retries} retries   ${trace.totals.leaseExtensions} lease renewals   ${trace.totals.leaseExpirations} lease expiries   ${trace.totals.toolCalls} tool calls (${trace.totals.toolFailures} failed)`,
  ];
}

/**
 * The failure callout.
 *
 * Printed twice over — once at the top, once after the timeline — because the whole
 * reason the command was run is usually in it, and scrolling back up through two
 * hundred timeline rows to find it is the exact friction this is meant to remove.
 */
function renderFailure(trace: Trace, paint: ReturnType<typeof makePaint>): string[] {
  const failedSteps = trace.steps.filter((s) => s.state === 'failed');
  if (trace.failure === undefined && failedSteps.length === 0) return [];

  const lines = [paint('FAILURE', 'red', 'bold')];
  if (trace.failure !== undefined) {
    lines.push(
      `  ${pad('workflow', 10)}${paint(trace.failure.error, 'red')}`,
      `  ${pad('class', 10)}${trace.failure.errorClass}`,
      `  ${pad('at', 10)}sequence ${trace.failure.atSequence}`,
    );
  }
  for (const step of failedSteps) {
    const last = step.attempts[step.attempts.length - 1];
    lines.push(
      `  ${pad('step', 10)}${paint(step.step, 'red', 'bold')} (attempt ${last?.attempt ?? 1} of ${step.attempts.length})`,
      `  ${pad('error', 10)}${last?.error ?? '-'}`,
      `  ${pad('class', 10)}${last?.errorClass ?? '-'}`,
      `  ${pad('worker', 10)}${last?.workerId ?? '-'}`,
    );
  }
  if (trace.criticalPath.length > 1) {
    lines.push(`  ${pad('path', 10)}${trace.criticalPath.join(paint(' -> ', 'dim'))}`);
  }
  return lines;
}

function renderApprovals(trace: Trace, paint: ReturnType<typeof makePaint>): string[] {
  if (trace.approvals.length === 0) return [];
  const lines = [paint('APPROVALS', 'dim', 'bold')];
  for (const approval of trace.approvals) {
    const verdict =
      approval.decision === 'pending'
        ? paint('PENDING', 'yellow', 'bold')
        : approval.decision === 'granted'
          ? paint('granted', 'green')
          : paint('denied', 'red');
    lines.push(
      `  ${approval.step}  ${verdict}  ${approval.decision === 'pending' ? approval.approvers.join(', ') : (approval.approver ?? '')}` +
        `${approval.waitMs !== undefined ? paint(`  waited ${formatDuration(approval.waitMs)}`, 'dim') : ''}`,
    );
    lines.push(`    ${paint(approval.prompt, 'dim')}`);
    lines.push(`    ${paint('payload', 'dim')} ${preview(approval.payload, 120)}`);
    if (approval.reason !== undefined) lines.push(`    ${paint('reason', 'dim')} ${approval.reason}`);
    if (approval.note !== undefined) lines.push(`    ${paint('note', 'dim')} ${approval.note}`);
  }
  return lines;
}

function renderProjection(
  projection: WorkflowProjection,
  sequence: Sequence,
  head: Sequence,
  paint: ReturnType<typeof makePaint>,
): string[] {
  const steps = [...projection.steps.values()].sort((a, b) => a.step.localeCompare(b.step));
  const w = {
    step: Math.max(4, ...steps.map((s) => s.step.length)),
    state: 9,
  };
  const lines = [
    paint(
      `PROJECTION AT SEQUENCE ${sequence}${sequence === head ? ' (head)' : ` of ${head}`}`,
      'bold',
    ),
    `  ${pad(paint('workflow', 'dim'), 10)}${paint(projection.state, STATE_COLOUR[projection.state], 'bold')}`,
    `  ${pad(paint('updated', 'dim'), 10)}${projection.updatedAt === 0 ? '-' : new Date(projection.updatedAt).toISOString()}`,
  ];
  if (steps.length === 0) {
    lines.push(`  ${paint('no steps yet at this point in the log', 'dim')}`);
  }
  for (const step of steps) {
    const extras: string[] = [];
    if (step.workerId !== undefined) extras.push(`worker=${step.workerId}`);
    if (step.attempt > 1) extras.push(`attempt=${step.attempt}`);
    if (step.awaitingApproval) extras.push(paint('awaiting approval', 'yellow'));
    if (step.error !== undefined) extras.push(paint(step.error, 'red'));
    lines.push(
      `  ${pad(step.step, w.step)}  ${pad(paint(step.state, STATE_COLOUR[step.state]), w.state)}  ${paint(extras.join('  '), 'dim')}`,
    );
  }
  if (projection.output !== undefined) {
    lines.push(`  ${pad(paint('output', 'dim'), 10)}${preview(projection.output, 160)}`);
  }
  return lines;
}

export async function run(argv: readonly string[], write: (text: string) => void): Promise<number> {
  const args = parseArgs(argv);
  const { events, definition } = await loadHistory(args);

  if (events.length === 0) {
    write('no events for that workflow\n');
    return 1;
  }

  const paint = makePaint(args.colour);
  // Best evidence first, same order as the API's `definitionFor`: the definition recorded
  // in `workflow.created`, then one supplied alongside the history, then a reconstruction.
  //
  // The logged one comes first because it is the definition this history was produced
  // against. An externally supplied definition is the *current* one, which for an old
  // history can be a different graph entirely — and replaying a history against a graph it
  // did not run on is the one thing this tool exists to be trusted about.
  const logged = events.find((e) => e.type === 'workflow.created')?.definition;
  // Reconstruction is a degraded mode: it lays out a chain, so a parallel graph prints as
  // a line. Still better than refusing to print anything.
  const def = logged ?? definition ?? synthesizeDefinition(events);
  const trace = buildTrace(events, def);
  const head = (events[events.length - 1] as Event).sequence;
  const at = args.at ?? head;
  if (at > head) {
    write(`--at ${at} is past the end of the log (head is ${head})\n`);
    return 1;
  }

  const blocks: string[][] = [
    renderHeader(trace, paint),
    renderFailure(trace, paint),
    renderTimeline(buildRows(events), paint),
    renderSteps(trace, paint),
    renderApprovals(trace, paint),
    renderProjection(
      at === head ? project(def, events) : replayTo(def, events, at),
      at,
      head,
      paint,
    ),
  ];

  write(blocks.filter((b) => b.length > 0).map((b) => b.join('\n')).join('\n\n'));
  write('\n');
  return trace.state === 'failed' ? 1 : 0;
}

// Only when executed, not when imported by a test.
if (process.argv[1] !== undefined && /replay(\.[tj]s)?$/.test(process.argv[1])) {
  run(process.argv.slice(2), (text) => process.stdout.write(text))
    .then((code) => process.exit(code))
    .catch((error: unknown) => {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exit(2);
    });
}

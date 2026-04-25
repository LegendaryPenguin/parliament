/**
 * The application shell.
 *
 * One module-level state object and one `render`. No framework, no virtual DOM, no
 * reactive wrapper: the view has four panels and a slider, and the honest amount of
 * machinery for that is a function that rebuilds a panel. The constraint that there are
 * no runtime dependencies in this project is not suspended for the UI.
 *
 * Re-rendering is scoped to the panel that changed, because a full rebuild on every
 * server-sent event would collapse open disclosures and lose the scroll position in the
 * middle of someone reading a trace.
 */

import { Api, ApiFailure } from './api.js';
import { renderApprovals } from './approvals.js';
import { renderDag } from './dag.js';
import { badge, clear, el, field, formatDuration, formatWhen, pre } from './dom.js';
import { renderTimeline } from './timeline.js';
import type { RunState, TraceView, WorkflowSummary } from './types.js';

type Tab = 'graph' | 'timeline' | 'approvals' | 'data';

interface Filters {
  state: string;
  tenant: string;
}

interface State {
  workflows: readonly WorkflowSummary[];
  total: number;
  filters: Filters;
  selectedId: string | undefined;
  trace: TraceView | undefined;
  tab: Tab;
  selectedStep: string | undefined;
  /** `undefined` means "live at head"; a number pins the view to that sequence. */
  replaySequence: number | undefined;
  replayStates: ReadonlyMap<string, RunState> | undefined;
  approver: string;
  busy: boolean;
  message: { text: string; kind: 'error' | 'ok' } | undefined;
  connected: boolean;
}

const APPROVER_KEY = 'parliament.approver';

const api = new Api('');

const state: State = {
  workflows: [],
  total: 0,
  filters: { state: '', tenant: '' },
  selectedId: undefined,
  trace: undefined,
  tab: 'graph',
  selectedStep: undefined,
  replaySequence: undefined,
  replayStates: undefined,
  approver: localStorage.getItem(APPROVER_KEY) ?? '',
  busy: false,
  message: undefined,
  connected: false,
};

let closeStream: (() => void) | undefined;

function must<T extends Element>(id: string): T {
  const node = document.getElementById(id);
  if (node === null) throw new Error(`missing element #${id}`);
  return node as unknown as T;
}

const refs = {
  list: must<HTMLElement>('workflow-list'),
  listMeta: must<HTMLElement>('list-meta'),
  detail: must<HTMLElement>('detail'),
  message: must<HTMLElement>('message'),
  connection: must<HTMLElement>('connection'),
  stateFilter: must<HTMLSelectElement>('filter-state'),
  tenantFilter: must<HTMLInputElement>('filter-tenant'),
  refresh: must<HTMLButtonElement>('refresh'),
};

function showMessage(text: string, kind: 'error' | 'ok'): void {
  state.message = { text, kind };
  refs.message.textContent = text;
  refs.message.className = `message message-${kind}`;
  if (kind === 'ok') {
    window.setTimeout(() => {
      if (state.message?.text === text) {
        state.message = undefined;
        refs.message.textContent = '';
        refs.message.className = 'message';
      }
    }, 4_000);
  }
}

function describeFailure(error: unknown): string {
  if (error instanceof ApiFailure) {
    const details = error.details;
    return details.length > 0
      ? `${error.status} ${error.body.error}: ${error.message} — ${details.join('; ')}`
      : `${error.status} ${error.body.error}: ${error.message}`;
  }
  return error instanceof Error ? error.message : String(error);
}

async function loadList(): Promise<void> {
  try {
    const page = await api.listWorkflows({
      ...(state.filters.state !== '' ? { state: state.filters.state } : {}),
      ...(state.filters.tenant !== '' ? { tenant: state.filters.tenant } : {}),
      limit: 100,
    });
    state.workflows = page.workflows;
    state.total = page.total;
    renderList();
  } catch (error) {
    showMessage(describeFailure(error), 'error');
  }
}

function renderList(): void {
  clear(refs.list);
  refs.listMeta.textContent = `${state.workflows.length} of ${state.total}`;

  if (state.workflows.length === 0) {
    refs.list.append(el('li', { className: 'empty', text: 'No workflows match.' }));
    return;
  }

  for (const summary of state.workflows) {
    const button = el('button', {
      className: `wf${summary.workflowId === state.selectedId ? ' wf-selected' : ''}`,
      attrs: {
        type: 'button',
        'aria-current': summary.workflowId === state.selectedId,
      },
      children: [
        el('span', {
          className: 'wf-head',
          children: [
            el('span', { className: 'wf-name', text: summary.workflow }),
            badge(summary.state, summary.state),
          ],
        }),
        el('span', { className: 'wf-id', text: summary.workflowId }),
        el('span', {
          className: 'wf-meta',
          text: [
            `${summary.steps} steps`,
            summary.tenant !== undefined ? summary.tenant : undefined,
            summary.priority !== undefined ? `priority ${summary.priority}` : undefined,
            summary.awaitingApproval > 0 ? `${summary.awaitingApproval} awaiting approval` : undefined,
            summary.failedSteps > 0 ? `${summary.failedSteps} failed` : undefined,
            formatWhen(summary.updatedAt),
          ]
            .filter((p): p is string => p !== undefined)
            .join(' · '),
        }),
      ],
      on: { click: () => void select(summary.workflowId) },
    });
    refs.list.append(el('li', { children: [button] }));
  }
}

async function select(id: string): Promise<void> {
  state.selectedId = id;
  state.selectedStep = undefined;
  state.replaySequence = undefined;
  state.replayStates = undefined;
  state.tab = state.tab === 'approvals' ? 'approvals' : state.tab;
  if (location.hash !== `#/w/${encodeURIComponent(id)}`) {
    location.hash = `#/w/${encodeURIComponent(id)}`;
  }
  renderList();
  await loadTrace();
  listen(id);
}

function listen(id: string): void {
  closeStream?.();
  closeStream = api.stream(id, {
    onChange: () => {
      void loadTrace();
      void loadList();
    },
    onStatus: (open) => {
      state.connected = open;
      refs.connection.textContent = open ? 'live' : 'reconnecting…';
      refs.connection.className = `connection ${open ? 'connection-live' : 'connection-down'}`;
    },
  });
}

async function loadTrace(): Promise<void> {
  const id = state.selectedId;
  if (id === undefined) return;
  try {
    state.trace = await api.trace(id);
    renderDetail();
  } catch (error) {
    showMessage(describeFailure(error), 'error');
  }
}

function tabButton(tab: Tab, label: string, count?: number): HTMLElement {
  return el('button', {
    className: `tab${state.tab === tab ? ' tab-active' : ''}`,
    text: count !== undefined && count > 0 ? `${label} (${count})` : label,
    attrs: { type: 'button', role: 'tab', 'aria-selected': state.tab === tab },
    on: {
      click: () => {
        state.tab = tab;
        renderDetail();
      },
      keydown: (event) => {
        const key = (event as KeyboardEvent).key;
        const order: Tab[] = ['graph', 'timeline', 'approvals', 'data'];
        const delta = key === 'ArrowRight' ? 1 : key === 'ArrowLeft' ? -1 : 0;
        if (delta === 0) return;
        event.preventDefault();
        const index = order.indexOf(state.tab);
        state.tab = order[(index + delta + order.length) % order.length] as Tab;
        renderDetail();
        (document.querySelector('.tab-active') as HTMLElement | null)?.focus();
      },
    },
  });
}

function renderDetail(): void {
  const trace = state.trace;
  const scroll = refs.detail.scrollTop;
  clear(refs.detail);

  if (trace === undefined) {
    refs.detail.append(
      el('div', {
        className: 'placeholder',
        children: [
          el('h2', { text: 'Pick a workflow' }),
          el('p', {
            text:
              'The list on the left is live. Choose an execution to see its dependency graph, ' +
              'its full event history, and the state of the workflow at any point in that history.',
          }),
        ],
      }),
    );
    return;
  }

  const pending = trace.approvals.filter((a) => a.decision === 'pending').length;

  refs.detail.append(
    header(trace),
    el('div', {
      className: 'tabs',
      attrs: { role: 'tablist', 'aria-label': 'Workflow views' },
      children: [
        tabButton('graph', 'Graph'),
        tabButton('timeline', 'Timeline', trace.totals.events),
        tabButton('approvals', 'Approvals', pending),
        tabButton('data', 'Input / output'),
      ],
    }),
  );

  if (state.tab === 'graph') {
    refs.detail.append(replaySlider(trace), graphPanel(trace));
  } else if (state.tab === 'timeline') {
    refs.detail.append(
      renderTimeline({
        trace,
        selectedStep: state.selectedStep,
        onSelect: (step) => {
          state.selectedStep = step;
          renderDetail();
        },
      }),
    );
  } else if (state.tab === 'approvals') {
    refs.detail.append(
      renderApprovals({
        approvals: trace.approvals,
        approver: state.approver,
        busy: state.busy,
        onApproverChange: (approver) => {
          state.approver = approver;
          localStorage.setItem(APPROVER_KEY, approver);
        },
        onApprove: (step, note) => void decide('approve', step, note),
        onDeny: (step, reason) => void decide('deny', step, reason),
      }),
    );
  } else {
    refs.detail.append(dataPanel(trace));
  }

  refs.detail.scrollTop = scroll;
}

function header(trace: TraceView): HTMLElement {
  const failure = trace.failure;
  const children: (Node | undefined)[] = [
    el('div', {
      className: 'detail-head',
      children: [
        el('h2', { text: `${trace.workflow} v${trace.definitionVersion}` }),
        badge(trace.state, trace.state),
        el('code', { className: 'wf-id', text: trace.workflowId }),
        el('button', {
          className: 'btn btn-quiet',
          text: 'Cancel execution',
          attrs: { type: 'button', disabled: state.busy },
          on: { click: () => void cancel() },
        }),
      ],
    }),
    el('div', {
      className: 'fields',
      children: [
        field('tenant', trace.tenant ?? '—'),
        field('priority', trace.priority !== undefined ? String(trace.priority) : 'default'),
        field('events', String(trace.totals.events)),
        field('attempts', String(trace.totals.attempts)),
        field('retries', String(trace.totals.retries), trace.totals.retries > 0 ? 'field-warn' : ''),
        field('lease renewals', String(trace.totals.leaseExtensions)),
        field(
          'lease expiries',
          String(trace.totals.leaseExpirations),
          trace.totals.leaseExpirations > 0 ? 'field-warn' : '',
        ),
        field('tool calls', `${trace.totals.toolCalls} (${trace.totals.toolFailures} failed)`),
        field('elapsed', formatDuration(trace.totalMs ?? trace.updatedAt - trace.createdAt)),
        field('cost units', `${trace.cost.totalUnits}`),
        field('of which estimated tokens', `${trace.cost.tokenUnits}`, 'field-estimate'),
      ],
    }),
  ];

  const gates = trace.steps.filter((s) => s.awaitingApproval);
  if (gates.length > 0) {
    // Spelled out because the two states look contradictory at a glance and are not:
    // a gate holds no worker and blocks only what depends on it, so parallel branches
    // keep running and the workflow is correctly still `running`.
    children.push(
      el('p', {
        className: 'hint hint-gate',
        text:
          `Waiting on a human at ${gates.map((g) => g.step).join(', ')}. ` +
          `The workflow itself is ${trace.state} — a gate holds no worker and does not block ` +
          'branches that do not depend on it.',
      }),
    );
  }

  if (failure !== undefined) {
    children.push(
      el('div', {
        className: 'failure',
        attrs: { role: 'status' },
        children: [
          el('strong', { text: `Failed${failure.step !== undefined ? ` at ${failure.step}` : ''}` }),
          el('span', { text: `${failure.errorClass}: ${failure.error}` }),
          trace.criticalPath.length > 1
            ? el('span', { className: 'failure-path', text: `path: ${trace.criticalPath.join(' → ')}` })
            : undefined,
        ],
      }),
    );
  }

  return el('section', { className: 'detail-header', children });
}

/**
 * The replay slider.
 *
 * The argument for recording every transition is that the state at any past moment is
 * recoverable; this is where that claim is cashed. Scrubbing refetches the projection
 * as of the chosen sequence — the fold happens on the server so that the dashboard
 * cannot disagree with the scheduler about what the log means — and recolours the graph
 * from it, keeping the shape from the full trace so the layout does not jump about as
 * steps appear.
 */
function replaySlider(trace: TraceView): HTMLElement {
  const head = trace.sequence;
  const value = state.replaySequence ?? head;
  const readout = el('output', {
    className: 'replay-readout',
    text: value === head ? `live · sequence ${head}` : `as of sequence ${value} of ${head}`,
  });

  const slider = el('input', {
    className: 'replay-range',
    attrs: {
      type: 'range',
      min: 1,
      max: Math.max(1, head),
      value,
      step: 1,
      'aria-label': 'Replay to sequence',
    },
  });

  let timer: number | undefined;
  const scrub = (): void => {
    const sequence = Number(slider.value);
    readout.textContent = sequence === head ? `live · sequence ${head}` : `as of sequence ${sequence} of ${head}`;
    window.clearTimeout(timer);
    // Debounced because dragging fires a change per pixel and each one is a request.
    timer = window.setTimeout(() => void applyReplay(sequence, head), 90);
  };
  slider.addEventListener('input', scrub);

  const step = (delta: number): void => {
    slider.value = String(Math.min(head, Math.max(1, Number(slider.value) + delta)));
    scrub();
  };

  return el('div', {
    className: 'replay',
    children: [
      el('label', { className: 'replay-label', text: 'Replay' }),
      el('button', {
        className: 'btn btn-quiet btn-tiny',
        text: '◀',
        attrs: { type: 'button', 'aria-label': 'Previous sequence' },
        on: { click: () => step(-1) },
      }),
      slider,
      el('button', {
        className: 'btn btn-quiet btn-tiny',
        text: '▶',
        attrs: { type: 'button', 'aria-label': 'Next sequence' },
        on: { click: () => step(1) },
      }),
      readout,
      el('button', {
        className: 'btn btn-quiet btn-tiny',
        text: 'Live',
        attrs: { type: 'button' },
        on: {
          click: () => {
            state.replaySequence = undefined;
            state.replayStates = undefined;
            renderDetail();
          },
        },
      }),
    ],
  });
}

async function applyReplay(sequence: number, head: number): Promise<void> {
  const id = state.selectedId;
  if (id === undefined) return;
  if (sequence >= head) {
    state.replaySequence = undefined;
    state.replayStates = undefined;
    renderDetail();
    return;
  }
  try {
    const view = await api.replay(id, sequence);
    state.replaySequence = sequence;
    state.replayStates = new Map(view.projection.steps.map((s) => [s.step, s.state]));
    renderDetail();
  } catch (error) {
    showMessage(describeFailure(error), 'error');
  }
}

function graphPanel(trace: TraceView): HTMLElement {
  const children: (Node | undefined)[] = [
    renderDag({
      steps: trace.steps,
      criticalPath: trace.criticalPath,
      stateAt: state.replayStates,
      selected: state.selectedStep,
      onSelect: (step) => {
        state.selectedStep = step;
        renderDetail();
      },
    }),
  ];

  const selected = trace.steps.find((s) => s.step === state.selectedStep);
  if (selected !== undefined) {
    const last = selected.attempts[selected.attempts.length - 1];
    children.push(
      el('section', {
        className: 'card',
        children: [
          el('header', {
            className: 'card-head',
            children: [el('h3', { text: selected.step }), badge(selected.state, selected.state)],
          }),
          el('div', {
            className: 'fields',
            children: [
              field('kind', selected.kind ?? '—'),
              field('depends on', selected.dependsOn.length > 0 ? selected.dependsOn.join(', ') : '—'),
              field('attempts', String(selected.attempts.length)),
              field('retries', String(selected.retries)),
              field('lease expiries', String(selected.leaseExpirations)),
              field('time in step', formatDuration(selected.totalMs)),
              field('worker', last?.workerId ?? '—'),
            ],
          }),
          last?.error !== undefined
            ? el('p', { className: 'failure', text: `${last.errorClass ?? 'error'}: ${last.error}` })
            : undefined,
          el('button', {
            className: 'btn btn-quiet',
            text: 'Show this step in the timeline',
            attrs: { type: 'button' },
            on: {
              click: () => {
                state.tab = 'timeline';
                renderDetail();
                document
                  .querySelector(`[data-step="${CSS.escape(selected.step)}"]`)
                  ?.scrollIntoView({ block: 'center' });
              },
            },
          }),
        ],
      }),
    );
  }

  return el('div', { className: 'graph-panel', children });
}

function dataPanel(trace: TraceView): HTMLElement {
  const tools = Object.entries(trace.cost.byTool);
  return el('div', {
    className: 'data-panel',
    children: [
      el('h3', { text: 'Input' }),
      pre(trace.input),
      el('h3', { text: 'Output' }),
      pre(trace.output),
      trace.error !== undefined ? el('p', { className: 'failure', text: trace.error }) : undefined,
      el('h3', { text: 'Cost' }),
      el('p', {
        className: 'hint',
        text:
          'Tool units are summed from the registry’s declared costUnits over calls that happened. ' +
          'The token figure is an estimate from the fake model provider — not a billed number.',
      }),
      el('div', {
        className: 'fields',
        children: [
          field('tool units', String(trace.cost.toolUnits)),
          field('token units (estimated)', String(trace.cost.tokenUnits), 'field-estimate'),
          field('total', String(trace.cost.totalUnits)),
          field('input tokens', String(trace.cost.tokens.inputTokens)),
          field('output tokens', String(trace.cost.tokens.outputTokens)),
        ],
      }),
      tools.length > 0
        ? el('ul', {
            className: 'tool-spend',
            children: tools.map(([tool, spend]) =>
              el('li', {
                text: `${tool} — ${spend.calls} call(s), ${spend.units} units, ${spend.deduplicated} deduplicated, ${spend.failures} failed`,
              }),
            ),
          })
        : undefined,
    ],
  });
}

async function decide(action: 'approve' | 'deny', step: string, text: string | undefined): Promise<void> {
  const id = state.selectedId;
  if (id === undefined) return;
  state.busy = true;
  renderDetail();
  try {
    if (action === 'approve') {
      await api.approve(id, step, state.approver, text);
      showMessage(`Approved ${step}.`, 'ok');
    } else {
      await api.deny(id, step, state.approver, text ?? '');
      showMessage(`Denied ${step}.`, 'ok');
    }
    await loadTrace();
    await loadList();
  } catch (error) {
    showMessage(describeFailure(error), 'error');
  } finally {
    state.busy = false;
    renderDetail();
  }
}

async function cancel(): Promise<void> {
  const id = state.selectedId;
  if (id === undefined) return;
  state.busy = true;
  try {
    await api.cancel(id, 'cancelled from the dashboard', state.approver === '' ? 'dashboard' : state.approver);
    showMessage('Cancellation recorded.', 'ok');
    await loadTrace();
    await loadList();
  } catch (error) {
    showMessage(describeFailure(error), 'error');
  } finally {
    state.busy = false;
    renderDetail();
  }
}

function wireControls(): void {
  refs.stateFilter.addEventListener('change', () => {
    state.filters.state = refs.stateFilter.value;
    void loadList();
  });
  refs.tenantFilter.addEventListener('input', () => {
    state.filters.tenant = refs.tenantFilter.value.trim();
    void loadList();
  });
  refs.refresh.addEventListener('click', () => {
    void loadList();
    void loadTrace();
  });

  // `/` to filter is the one shortcut worth having: it is the first thing done on
  // arriving at a list of two hundred executions.
  document.addEventListener('keydown', (event) => {
    if (event.key === '/' && document.activeElement?.tagName !== 'INPUT') {
      event.preventDefault();
      refs.tenantFilter.focus();
    }
  });

  window.addEventListener('hashchange', () => {
    const id = idFromHash();
    if (id !== undefined && id !== state.selectedId) void select(id);
  });
}

function idFromHash(): string | undefined {
  const match = /^#\/w\/(.+)$/.exec(location.hash);
  return match?.[1] !== undefined ? decodeURIComponent(match[1]) : undefined;
}

async function start(): Promise<void> {
  wireControls();
  renderDetail();
  await loadList();
  const id = idFromHash();
  if (id !== undefined) await select(id);
}

void start();

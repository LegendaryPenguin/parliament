/**
 * The DAG view: hand-rolled SVG.
 *
 * Nodes are placed by dependency depth — one row per level, so an edge always points
 * downward and the eye can follow "what had to finish before this" without tracing
 * arrowheads. A force-directed or grid layout would look livelier and would destroy
 * that property.
 *
 * The highlighted path is the useful part. In a graph of twenty steps, colour alone
 * tells you *that* something failed; the path tells you *through what*, which is the
 * question an operator actually has. The path is computed server-side (see
 * `src/api/trace.ts`) because it depends on which dependency finished last, and that is
 * a fact about the log rather than about the drawing.
 */

import { el, svg, svgText } from './dom.js';
import type { RunState, TraceStepView } from './types.js';

const NODE_WIDTH = 168;
const NODE_HEIGHT = 52;
const GAP_X = 28;
const GAP_Y = 78;
const PADDING = 24;

export interface DagOptions {
  readonly steps: readonly TraceStepView[];
  readonly criticalPath: readonly string[];
  /**
   * States to draw instead of the live ones, used by the replay slider so the DAG shows
   * the graph as of a past sequence while keeping the shape from the full trace.
   */
  readonly stateAt?: ReadonlyMap<string, RunState> | undefined;
  readonly selected?: string | undefined;
  readonly onSelect: (step: string) => void;
}

interface Placed {
  readonly step: TraceStepView;
  readonly x: number;
  readonly y: number;
  readonly state: RunState;
}

export function renderDag(options: DagOptions): HTMLElement {
  const { steps } = options;
  if (steps.length === 0) {
    return el('p', { className: 'empty', text: 'No steps in this workflow yet.' });
  }

  const levels = new Map<number, TraceStepView[]>();
  for (const step of steps) {
    const list = levels.get(step.depth) ?? [];
    list.push(step);
    levels.set(step.depth, list);
  }
  const depths = [...levels.keys()].sort((a, b) => a - b);
  const widest = Math.max(...[...levels.values()].map((l) => l.length));

  const canvasWidth = PADDING * 2 + widest * NODE_WIDTH + (widest - 1) * GAP_X;
  const canvasHeight = PADDING * 2 + depths.length * NODE_HEIGHT + (depths.length - 1) * GAP_Y;

  const placed = new Map<string, Placed>();
  depths.forEach((depth, row) => {
    const list = (levels.get(depth) ?? []).slice().sort((a, b) => a.step.localeCompare(b.step));
    const rowWidth = list.length * NODE_WIDTH + (list.length - 1) * GAP_X;
    const startX = (canvasWidth - rowWidth) / 2;
    list.forEach((step, i) => {
      placed.set(step.step, {
        step,
        x: startX + i * (NODE_WIDTH + GAP_X),
        y: PADDING + row * (NODE_HEIGHT + GAP_Y),
        state: options.stateAt?.get(step.step) ?? step.state,
      });
    });
  });

  // Consecutive pairs only: the path is a walk, so highlighting every edge between any
  // two members of it would light up edges the walk never took.
  const pathEdges = new Set<string>();
  for (let i = 1; i < options.criticalPath.length; i += 1) {
    pathEdges.add(`${options.criticalPath[i - 1]}->${options.criticalPath[i]}`);
  }
  const onPath = new Set(options.criticalPath);
  const failing = steps.some((s) => (options.stateAt?.get(s.step) ?? s.state) === 'failed');

  const edges: SVGElement[] = [];
  for (const node of placed.values()) {
    for (const parentName of node.step.dependsOn) {
      const parent = placed.get(parentName);
      if (parent === undefined) continue;
      const highlighted = pathEdges.has(`${parentName}->${node.step.step}`);
      edges.push(
        svg('path', {
          d: edgePath(parent, node),
          class: `dag-edge${highlighted ? (failing ? ' dag-edge-failure' : ' dag-edge-path') : ''}`,
          'marker-end': highlighted ? 'url(#arrow-hot)' : 'url(#arrow)',
        }),
      );
    }
  }

  const nodes = [...placed.values()].map((node) => drawNode(node, options, onPath, failing));

  const root = svg(
    'svg',
    {
      class: 'dag',
      viewBox: `0 0 ${canvasWidth} ${canvasHeight}`,
      // Fixed aspect via viewBox plus CSS width means the graph scales down on a narrow
      // screen rather than overflowing or needing a second layout pass.
      preserveAspectRatio: 'xMidYMin meet',
      role: 'group',
      'aria-label': 'Workflow dependency graph',
    },
    [defs(), svg('g', {}, edges), svg('g', {}, nodes)],
  );

  return el('div', { className: 'dag-wrap', children: [root, legend()] });
}

function defs(): SVGElement {
  const marker = (id: string, className: string): SVGElement =>
    svg(
      'marker',
      {
        id,
        viewBox: '0 0 10 10',
        refX: '9',
        refY: '5',
        markerWidth: '6',
        markerHeight: '6',
        orient: 'auto-start-reverse',
      },
      [svg('path', { d: 'M 0 0 L 10 5 L 0 10 z', class: className })],
    );
  return svg('defs', {}, [marker('arrow', 'dag-arrow'), marker('arrow-hot', 'dag-arrow-hot')]);
}

function edgePath(from: Placed, to: Placed): string {
  const x1 = from.x + NODE_WIDTH / 2;
  const y1 = from.y + NODE_HEIGHT;
  const x2 = to.x + NODE_WIDTH / 2;
  const y2 = to.y;
  const mid = (y1 + y2) / 2;
  return `M ${x1} ${y1} C ${x1} ${mid}, ${x2} ${mid}, ${x2} ${y2 - 2}`;
}

function drawNode(
  node: Placed,
  options: DagOptions,
  onPath: ReadonlySet<string>,
  failing: boolean,
): SVGElement {
  const { step } = node;
  const classes = ['dag-node', `dag-node-${node.state}`];
  if (options.selected === step.step) classes.push('dag-node-selected');
  if (onPath.has(step.step)) classes.push(failing ? 'dag-node-onfailpath' : 'dag-node-onpath');

  const markers: string[] = [];
  if (step.retries > 0) markers.push(`↻${step.retries}`);
  if (step.leaseExtensions > 0) markers.push(`♥${step.leaseExtensions}`);
  if (step.leaseExpirations > 0) markers.push(`⏱${step.leaseExpirations}`);
  if (step.awaitingApproval) markers.push('⏸');

  const label = `${step.step}, ${node.state}${step.kind !== undefined ? `, ${step.kind} step` : ''}${
    step.retries > 0 ? `, retried ${step.retries} time(s)` : ''
  }${step.leaseExpirations > 0 ? `, ${step.leaseExpirations} lease expiry` : ''}`;

  const group = svg(
    'g',
    {
      class: classes.join(' '),
      transform: `translate(${node.x}, ${node.y})`,
      tabindex: '0',
      role: 'button',
      'aria-label': label,
    },
    [
      svg('rect', { class: 'dag-box', width: NODE_WIDTH, height: NODE_HEIGHT, rx: 6 }),
      svg('rect', { class: 'dag-stripe', width: 4, height: NODE_HEIGHT, rx: 2 }),
      svgText(truncate(step.step, 20), { class: 'dag-name', x: 14, y: 21 }),
      svgText(`${node.state}${step.kind !== undefined ? ` · ${step.kind}` : ''}`, {
        class: 'dag-meta',
        x: 14,
        y: 38,
      }),
      markers.length > 0
        ? svgText(markers.join(' '), { class: 'dag-markers', x: NODE_WIDTH - 12, y: 21, 'text-anchor': 'end' })
        : undefined,
      step.totalMs > 0
        ? svgText(shortMs(step.totalMs), {
            class: 'dag-meta',
            x: NODE_WIDTH - 12,
            y: 38,
            'text-anchor': 'end',
          })
        : undefined,
    ],
  );

  group.addEventListener('click', () => options.onSelect(step.step));
  group.addEventListener('keydown', (event) => {
    const key = (event as KeyboardEvent).key;
    if (key === 'Enter' || key === ' ') {
      event.preventDefault();
      options.onSelect(step.step);
    }
  });
  return group;
}

function legend(): HTMLElement {
  const item = (state: string, text: string): HTMLElement =>
    el('span', {
      className: 'legend-item',
      children: [
        el('i', { className: `legend-swatch legend-${state}` }),
        el('span', { text }),
      ],
    });
  return el('div', {
    className: 'legend',
    children: [
      item('succeeded', 'succeeded'),
      item('running', 'running'),
      item('waiting', 'waiting on approval'),
      item('retrying', 'retrying'),
      item('failed', 'failed'),
      item('pending', 'not started'),
      el('span', { className: 'legend-item legend-path', text: '— highlighted: path to the failure' }),
      el('span', {
        className: 'legend-item',
        text: '↻ retries · ♥ lease renewals (slow, not stuck) · ⏱ lease expiries · ⏸ awaiting approval',
      }),
    ],
  });
}

function truncate(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function shortMs(ms: number): string {
  return ms < 1_000 ? `${Math.round(ms)}ms` : `${(ms / 1_000).toFixed(1)}s`;
}

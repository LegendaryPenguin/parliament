/**
 * Element construction.
 *
 * There is no `innerHTML` anywhere in this dashboard, and this module is why: every
 * node is built from a tag, attributes, and `textContent`. The page renders workflow
 * inputs, tool arguments, tool results and approval payloads — all of which originate
 * outside the system, some of it written by a language model. A single interpolated
 * template string anywhere in the render path would turn "the dashboard displays
 * untrusted data" into "the dashboard executes it".
 *
 * `textContent` also happens to be faster than parsing HTML, but that is not the
 * reason.
 */

export interface ElementOptions {
  readonly className?: string;
  readonly text?: string;
  readonly title?: string;
  readonly attrs?: Readonly<Record<string, string | number | boolean>>;
  readonly children?: readonly (Node | undefined)[];
  readonly on?: Readonly<Record<string, (event: Event) => void>>;
}

export function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  options: ElementOptions = {},
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  if (options.className !== undefined) node.className = options.className;
  if (options.text !== undefined) node.textContent = options.text;
  if (options.title !== undefined) node.title = options.title;
  for (const [name, value] of Object.entries(options.attrs ?? {})) {
    if (value === false) continue;
    node.setAttribute(name, value === true ? '' : String(value));
  }
  for (const [type, listener] of Object.entries(options.on ?? {})) {
    node.addEventListener(type, listener);
  }
  for (const child of options.children ?? []) {
    if (child !== undefined) node.append(child);
  }
  return node;
}

const SVG_NS = 'http://www.w3.org/2000/svg';

export function svg(
  tag: string,
  attrs: Readonly<Record<string, string | number>> = {},
  children: readonly (Node | undefined)[] = [],
): SVGElement {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [name, value] of Object.entries(attrs)) node.setAttribute(name, String(value));
  for (const child of children) if (child !== undefined) node.append(child);
  return node;
}

/** An SVG text node. Separate helper because the content is always untrusted. */
export function svgText(
  text: string,
  attrs: Readonly<Record<string, string | number>> = {},
): SVGElement {
  const node = svg('text', attrs);
  node.textContent = text;
  return node;
}

export function clear(node: Element): void {
  node.replaceChildren();
}

export function fragment(children: readonly (Node | undefined)[]): DocumentFragment {
  const frag = document.createDocumentFragment();
  for (const child of children) if (child !== undefined) frag.append(child);
  return frag;
}

/** A labelled key/value pair, the unit most of this UI is made of. */
export function field(label: string, value: string, className = ''): HTMLElement {
  return el('div', {
    className: `field ${className}`.trim(),
    children: [
      el('span', { className: 'field-label', text: label }),
      el('span', { className: 'field-value', text: value }),
    ],
  });
}

export function badge(text: string, kind: string): HTMLElement {
  return el('span', { className: `badge badge-${kind}`, text, attrs: { 'data-state': kind } });
}

/**
 * Pretty-prints a value for display.
 *
 * Always via `JSON.stringify` into a `<pre>` with `textContent`, never interpolated.
 * Depth is not limited but length is: a tool that returned a megabyte of text should
 * not hang the tab, and the full value is a `curl` away.
 */
export function formatValue(value: unknown, maxChars = 20_000): string {
  if (value === undefined) return '(none)';
  let text: string;
  try {
    text = typeof value === 'string' ? value : JSON.stringify(value, null, 2) ?? String(value);
  } catch {
    // Circular structures cannot come off the wire, but a defensive fallback here is
    // cheaper than a blank panel if one ever does.
    text = String(value);
  }
  return text.length > maxChars ? `${text.slice(0, maxChars)}\n… truncated (${text.length} chars)` : text;
}

export function pre(value: unknown): HTMLElement {
  return el('pre', { className: 'data', text: formatValue(value) });
}

export function formatDuration(ms: number | undefined): string {
  if (ms === undefined) return '—';
  if (ms < 1_000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m${Math.round((ms % 60_000) / 1_000)}s`;
  return `${(ms / 3_600_000).toFixed(1)}h`;
}

export function formatClock(at: number): string {
  const date = new Date(at);
  return `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}:${String(
    date.getSeconds(),
  ).padStart(2, '0')}.${String(date.getMilliseconds()).padStart(3, '0')}`;
}

export function formatWhen(at: number): string {
  const delta = Date.now() - at;
  if (delta < 60_000) return 'just now';
  if (delta < 3_600_000) return `${Math.floor(delta / 60_000)}m ago`;
  if (delta < 86_400_000) return `${Math.floor(delta / 3_600_000)}h ago`;
  return new Date(at).toISOString().slice(0, 16).replace('T', ' ');
}

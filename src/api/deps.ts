/**
 * The seams between the HTTP layer and everything behind it.
 *
 * The API deliberately knows nothing about how events are stored, folded, or
 * produced. It is handed four narrow ports and talks only to those. That is not
 * abstraction for its own sake — the store, the projector, the scheduler and the
 * control plane are all being written in parallel, and a transport that imports
 * their concrete modules would break every time one of them moves. Ports also mean
 * the tests can drive the API with a hand-built history instead of booting a
 * scheduler.
 *
 * Each port is written to the *narrowest* shape the API actually uses, so the real
 * implementations satisfy them structurally without having to know this file exists.
 * `ToolRegistryView` is a one-method subset of `ToolExecutor`, for example: the API
 * lists tools and never invokes one.
 */

import type { Event, EventStore, Sequence, WorkflowProjection } from '../events/types.js';
import type { ToolDef } from '../tools/types.js';
import type { StepName, WorkflowDef, WorkflowId } from '../workflow/types.js';

/**
 * Folds a complete history into the current projection.
 *
 * Matches `src/state/project.ts`. The API never folds events itself — two
 * independent fold implementations would drift, and the one in `src/state` is the
 * one the scheduler makes decisions from, so it is the one the dashboard must show.
 */
export type Projector = (events: readonly Event[]) => WorkflowProjection;

/** Folds only the prefix up to `sequence`, for the replay endpoint and slider. */
export type ReplayProjector = (events: readonly Event[], sequence: Sequence) => WorkflowProjection;

/** The only thing the API needs from the tool layer. */
export interface ToolRegistryView {
  list(): readonly ToolDef[];
  get(name: string): ToolDef | undefined;
}

/** What a caller asked for when submitting an execution. */
export interface SubmitRequest {
  readonly definition: WorkflowDef;
  readonly input: unknown;
  readonly tenant?: string;
  readonly deadlineAt?: number;
  readonly idempotencyKey?: string;
}

/**
 * Writes to the log.
 *
 * Every mutating endpoint delegates here rather than appending events itself. The
 * HTTP layer decides *whether* a request is admissible (shape, budget, terminal
 * state) and the control plane decides what that means in events — keeping the two
 * separate is what stops the transport from becoming a second, divergent state
 * machine.
 */
export interface ControlPlanePort {
  submit(request: SubmitRequest): Promise<WorkflowId>;
  cancel(workflowId: WorkflowId, reason: string, requestedBy?: string): Promise<void>;
  approve(workflowId: WorkflowId, step: StepName, approver: string, note?: string): Promise<void>;
  deny(workflowId: WorkflowId, step: StepName, approver: string, reason: string): Promise<void>;
}

/**
 * Remembers the definition each execution was started from.
 *
 * The projection does not carry the graph, only the per-step states folded out of
 * it. But "which steps could still run, and what was this one waiting on" is
 * unanswerable without `dependsOn`, so the DAG view and the critical-path
 * computation both need the definition that was submitted — including for a
 * workflow whose definition has since been superseded, which is why this is keyed by
 * execution and not by `(name, version)`.
 */
export interface DefinitionStore {
  put(workflowId: WorkflowId, def: WorkflowDef): void;
  get(workflowId: WorkflowId): WorkflowDef | undefined;
}

/**
 * Notifies the SSE layer that a workflow's history grew.
 *
 * A port rather than polling, because polling every open dashboard tab against
 * every workflow is exactly the load the event log was supposed to make
 * unnecessary. Implementations call `emit` after a successful append; the server
 * subscribes per connection.
 */
export interface ChangeFeed {
  subscribe(workflowId: WorkflowId | '*', listener: (workflowId: WorkflowId) => void): () => void;
  emit(workflowId: WorkflowId): void;

  /**
   * Attaches this feed to a store, so every append anywhere in the process reaches it.
   *
   * Returns the unsubscribe function. One call at wiring time replaces the former
   * decorator: nothing downstream has to remember to route appends here, because the
   * store tells the feed directly.
   */
  bridge(store: EventStore): () => void;
}

/** Everything the server is constructed with. */
export interface ApiDeps {
  readonly store: EventStore;
  readonly project: Projector;
  readonly replayTo: ReplayProjector;
  readonly tools: ToolRegistryView;
  readonly controlPlane: ControlPlanePort;
  readonly definitions: DefinitionStore;
  readonly changes: ChangeFeed;
  /** Injected so tests can assert on durations without sleeping. */
  readonly now?: () => number;
}

/** In-memory `ChangeFeed`. Adequate for one process; a fan-out bus is not. */
export class LocalChangeFeed implements ChangeFeed {
  private readonly listeners = new Map<string, Set<(id: WorkflowId) => void>>();

  subscribe(workflowId: WorkflowId | '*', listener: (id: WorkflowId) => void): () => void {
    const set = this.listeners.get(workflowId) ?? new Set();
    set.add(listener);
    this.listeners.set(workflowId, set);
    return () => {
      set.delete(listener);
      if (set.size === 0) this.listeners.delete(workflowId);
    };
  }

  bridge(store: EventStore): () => void {
    return store.subscribe((events) => {
      // One emit per distinct workflow in the batch rather than one per event. An SSE
      // client re-reads the history when it is woken, so n wake-ups for one batch cost n
      // redundant reads and deliver nothing a single wake-up would not.
      const touched = new Set<WorkflowId>();
      for (const e of events) touched.add(e.workflowId);
      for (const id of touched) this.emit(id);
    });
  }

  emit(workflowId: WorkflowId): void {
    for (const key of [workflowId, '*']) {
      for (const listener of this.listeners.get(key) ?? []) {
        // A throwing dashboard listener must not fail the append that caused it.
        try {
          listener(workflowId);
        } catch {
          /* ignored on purpose */
        }
      }
    }
  }
}

/** In-memory `DefinitionStore`. */
export class LocalDefinitionStore implements DefinitionStore {
  private readonly defs = new Map<WorkflowId, WorkflowDef>();

  put(workflowId: WorkflowId, def: WorkflowDef): void {
    this.defs.set(workflowId, def);
  }

  get(workflowId: WorkflowId): WorkflowDef | undefined {
    return this.defs.get(workflowId);
  }
}

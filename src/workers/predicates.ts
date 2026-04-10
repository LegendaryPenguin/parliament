/**
 * The predicate registry for `condition` nodes.
 *
 * Predicates are registered under a name and referenced by that name from a NodeDef.
 * The alternative — an inline closure on the node — cannot be serialized into the
 * definition store, cannot be sent to a worker in another process, and cannot be
 * replayed by a later version of the control plane that never had the closure. A name
 * survives all three.
 *
 * The other rule here is that a predicate never silently returns false. A condition
 * that defaults to false on a missing input sends the workflow down the wrong branch
 * and leaves nothing in the history saying why, which is the single worst failure mode
 * in a branching orchestrator: the run looks successful and is wrong.
 */

import type { StepName } from '../workflow/types.js';
import type { CompletedStep } from './ports.js';

/**
 * A definition-level mistake: an unregistered predicate, a predicate that read a step
 * which has not completed, or a predicate that threw.
 *
 * Separate from ToolError because it is not a runtime fault to be retried. Retrying a
 * predicate that reads a nonexistent step produces the same error forever.
 */
export class PredicateError extends Error {
  readonly errorClass = 'definition_error' as const;
  readonly retryable = false;

  constructor(
    readonly predicate: string,
    message: string,
    override readonly cause?: unknown,
  ) {
    super(`predicate "${predicate}": ${message}`);
    this.name = 'PredicateError';
  }
}

/**
 * The view of prior outputs a predicate is given.
 *
 * Deliberately not a plain Map. With a Map, `outputs.get('missing')?.total > 0` reads
 * as false and nobody notices; here the same mistake raises. Access is the enforcement
 * point, so there is no path to an accidental default.
 */
export interface OutputsView {
  /** The step's output, or a PredicateError if that step has not completed. */
  require(step: StepName): unknown;

  /**
   * The step's output if it completed, otherwise `undefined`. For predicates where
   * absence is genuinely meaningful — it has to be asked for explicitly.
   */
  optional(step: StepName): unknown;

  has(step: StepName): boolean;

  /** Names of every completed step, for error messages and for debugging. */
  completed(): readonly StepName[];
}

export type PredicateFn = (outputs: OutputsView) => boolean;

export interface PredicateDef {
  readonly name: string;

  /** Why this branch exists, carried into the trace alongside the result. */
  readonly description: string;

  /**
   * Steps this predicate reads. Checked before the body runs, so a predicate wired to
   * a step that is not actually a dependency fails with "step not complete" naming
   * both sides, rather than with whatever TypeError the body happens to produce.
   */
  readonly reads: readonly StepName[];

  readonly fn: PredicateFn;
}

export interface PredicateRegistry {
  register(def: PredicateDef): void;
  get(name: string): PredicateDef | undefined;
  list(): readonly PredicateDef[];

  /**
   * Evaluates a named predicate. Throws PredicateError for every failure mode; never
   * returns a boolean it is not sure about.
   */
  evaluate(name: string, outputs: ReadonlyMap<StepName, CompletedStep>): boolean;
}

export function createOutputsView(
  outputs: ReadonlyMap<StepName, CompletedStep>,
  predicateName: string,
): OutputsView {
  return {
    require(step) {
      const entry = outputs.get(step);
      if (entry === undefined) {
        const known = [...outputs.keys()];
        throw new PredicateError(
          predicateName,
          `read step "${step}", which has not completed (completed: ${
            known.length > 0 ? known.join(', ') : 'none'
          })`,
        );
      }
      return entry.output;
    },
    optional(step) {
      return outputs.get(step)?.output;
    },
    has(step) {
      return outputs.has(step);
    },
    completed() {
      return [...outputs.keys()];
    },
  };
}

export function createPredicateRegistry(): PredicateRegistry {
  const defs = new Map<string, PredicateDef>();

  return {
    register(def) {
      // Re-registration is rejected rather than overwritten: two predicates fighting
      // over one name means one workflow definition is silently getting the other's
      // logic, and whichever module loaded last wins non-deterministically.
      if (defs.has(def.name)) {
        throw new PredicateError(def.name, 'already registered');
      }
      defs.set(def.name, def);
    },

    get(name) {
      return defs.get(name);
    },

    list() {
      return [...defs.values()];
    },

    evaluate(name, outputs) {
      const def = defs.get(name);
      if (!def) {
        const known = [...defs.keys()];
        throw new PredicateError(
          name,
          `not registered (registered: ${known.length > 0 ? known.join(', ') : 'none'})`,
        );
      }

      const view = createOutputsView(outputs, name);
      for (const step of def.reads) {
        if (!view.has(step)) {
          // Forces the same error for a declared-but-absent read as for an undeclared
          // one, so the failure does not depend on whether the body reached that line.
          view.require(step);
        }
      }

      let result: unknown;
      try {
        result = def.fn(view);
      } catch (err) {
        if (err instanceof PredicateError) throw err;
        throw new PredicateError(name, `threw: ${describe(err)}`, err);
      }

      if (typeof result !== 'boolean') {
        // A truthy string would branch correctly often enough to hide the bug, so the
        // return type is enforced rather than coerced.
        throw new PredicateError(name, `returned ${typeof result}, expected boolean`);
      }
      return result;
    },
  };
}

function describe(err: unknown): string {
  if (err instanceof Error) return `${err.name}: ${err.message}`;
  return String(err);
}

/**
 * Predicates used by the bundled demo workflows.
 *
 * Registered here rather than next to the workflow that uses them so that there is one
 * place to read to find out what branches this deployment can take.
 */
export function registerBuiltinPredicates(registry: PredicateRegistry): PredicateRegistry {
  registry.register({
    name: 'always',
    description: 'Unconditional true. Used to model a straight-line branch explicitly.',
    reads: [],
    fn: () => true,
  });

  registry.register({
    name: 'never',
    description: 'Unconditional false. Used to disable a branch without deleting it.',
    reads: [],
    fn: () => false,
  });

  return registry;
}

/**
 * Builds a predicate that compares one numeric field of one step's output.
 *
 * A factory, not a closure stored on a node: the result still has to be registered
 * under a name, so what the definition references remains a string.
 */
export function numericThreshold(
  name: string,
  step: StepName,
  field: string,
  op: '>' | '>=' | '<' | '<=',
  threshold: number,
): PredicateDef {
  return {
    name,
    description: `${step}.${field} ${op} ${threshold}`,
    reads: [step],
    fn: (outputs) => {
      const output = outputs.require(step);
      if (typeof output !== 'object' || output === null) {
        throw new PredicateError(name, `step "${step}" output is not an object`);
      }
      const value = (output as Record<string, unknown>)[field];
      if (typeof value !== 'number' || Number.isNaN(value)) {
        throw new PredicateError(
          name,
          `step "${step}" field "${field}" is ${value === undefined ? 'missing' : typeof value}, expected number`,
        );
      }
      switch (op) {
        case '>':
          return value > threshold;
        case '>=':
          return value >= threshold;
        case '<':
          return value < threshold;
        case '<=':
          return value <= threshold;
      }
    },
  };
}

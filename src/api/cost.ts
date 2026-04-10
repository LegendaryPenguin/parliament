/**
 * Cost accounting.
 *
 * Two separate numbers, deliberately not added up and presented as one:
 *
 *  - **Tool cost units.** Each `ToolDef` declares `costUnits`, and every invocation is
 *    in the log. This figure is exact in the sense that matters: it is a faithful sum
 *    of the units the registry declares, over calls that provably happened.
 *  - **Model token cost.** An *estimate*. The token counts come out of the fake model
 *    provider in this project, and the per-1k rates below are placeholders. Nothing
 *    here is a billed number, and it must not be read as one. It is included because
 *    a budget that ignores model spend would refuse the wrong workflows — but the two
 *    are reported as separate fields all the way to the dashboard so that nobody has
 *    to guess which part is solid.
 *
 * Both are derived from the event log rather than accumulated in a counter. A counter
 * that drifts from the log cannot be reconciled; a fold can always be recomputed.
 */

import type { Event } from '../events/types.js';
import type { WorkflowId } from '../workflow/types.js';
import type { ToolRegistryView } from './deps.js';

export interface ModelPricing {
  readonly per1kInputUnits: number;
  readonly per1kOutputUnits: number;
}

/**
 * Placeholder rates. Named `*Units` rather than `*Cents` on purpose: these are the
 * same abstract unit as `ToolDef.costUnits`, not currency.
 */
export const DEFAULT_MODEL_PRICING: Readonly<Record<string, ModelPricing>> = {
  default: { per1kInputUnits: 0.3, per1kOutputUnits: 1.5 },
};

export interface TokenTotals {
  readonly inputTokens: number;
  readonly outputTokens: number;
  /** Step completions that reported usage at all. */
  readonly reportingSteps: number;
  /** Steps whose token counts were estimated from output length, not reported. */
  readonly estimatedSteps: number;
}

export interface ToolSpend {
  readonly calls: number;
  readonly deduplicated: number;
  readonly failures: number;
  readonly units: number;
}

export interface WorkflowCost {
  readonly workflowId: WorkflowId;
  readonly tenant?: string;
  readonly toolUnits: number;
  /** Estimated. See the file comment. */
  readonly tokenUnits: number;
  readonly totalUnits: number;
  readonly byTool: Readonly<Record<string, ToolSpend>>;
  readonly tokens: TokenTotals;
  /** Always true while the model provider is a fake. Surfaced, not hidden. */
  readonly tokenUnitsAreEstimated: true;
}

export interface TenantCost {
  readonly tenant: string;
  readonly workflows: number;
  readonly toolUnits: number;
  readonly tokenUnits: number;
  readonly totalUnits: number;
  readonly budgetUnits?: number;
  readonly remainingUnits?: number;
}

export interface BudgetDecision {
  readonly allowed: boolean;
  readonly tenant?: string;
  readonly spentUnits: number;
  readonly requestedUnits: number;
  readonly budgetUnits?: number;
  readonly reason?: string;
}

export interface CostLedgerOptions {
  readonly tools: ToolRegistryView;
  /** Per-tenant ceiling in cost units. A tenant with no entry is unbudgeted. */
  readonly budgets?: ReadonlyMap<string, number>;
  readonly pricing?: Readonly<Record<string, ModelPricing>>;
  /**
   * What a submission is assumed to cost before it has run.
   *
   * A budget check at submission time has nothing to measure yet, so it has to charge
   * something: otherwise a tenant already at its ceiling can keep submitting, and the
   * refusal only arrives after the work is done and the money is spent. A flat
   * reservation is crude but it fails in the safe direction.
   */
  readonly reserveUnits?: number;
}

export class CostLedger {
  private readonly tools: ToolRegistryView;
  private readonly budgets: ReadonlyMap<string, number>;
  private readonly pricing: Readonly<Record<string, ModelPricing>>;
  private readonly reserveUnits: number;

  constructor(options: CostLedgerOptions) {
    this.tools = options.tools;
    this.budgets = options.budgets ?? new Map();
    this.pricing = options.pricing ?? DEFAULT_MODEL_PRICING;
    this.reserveUnits = options.reserveUnits ?? 1;
  }

  /** Folds one workflow's history into its cost. */
  forWorkflow(events: readonly Event[]): WorkflowCost {
    let tenant: string | undefined;
    let workflowId = '';
    const byTool = new Map<string, { calls: number; deduplicated: number; failures: number; units: number }>();
    let inputTokens = 0;
    let outputTokens = 0;
    let reportingSteps = 0;
    let estimatedSteps = 0;
    let tokenUnits = 0;

    const spendFor = (tool: string) => {
      const existing = byTool.get(tool);
      if (existing) return existing;
      const made = { calls: 0, deduplicated: 0, failures: 0, units: 0 };
      byTool.set(tool, made);
      return made;
    };

    for (const event of events) {
      workflowId = event.workflowId;
      switch (event.type) {
        case 'workflow.created':
          tenant = event.tenant;
          break;

        case 'tool.succeeded': {
          const spend = spendFor(event.tool);
          spend.calls += 1;
          if (event.deduplicated) {
            // A deduplicated call returned a memoized result without touching the
            // external system, so charging for it would bill a retry twice for one
            // effect. Counted separately so the saving is visible.
            spend.deduplicated += 1;
          } else {
            spend.units += this.tools.get(event.tool)?.costUnits ?? 0;
          }
          break;
        }

        case 'tool.failed': {
          const spend = spendFor(event.tool);
          spend.failures += 1;
          break;
        }

        case 'step.completed': {
          const usage = readUsage(event.output);
          if (usage) {
            inputTokens += usage.inputTokens;
            outputTokens += usage.outputTokens;
            if (usage.estimated) estimatedSteps += 1;
            else reportingSteps += 1;
            const rate = this.pricing[usage.model] ?? this.pricing['default'];
            if (rate) {
              tokenUnits +=
                (usage.inputTokens / 1000) * rate.per1kInputUnits +
                (usage.outputTokens / 1000) * rate.per1kOutputUnits;
            }
          }
          break;
        }

        default:
          break;
      }
    }

    const toolUnits = [...byTool.values()].reduce((sum, s) => sum + s.units, 0);
    const rounded = round(tokenUnits);

    return {
      workflowId,
      toolUnits,
      tokenUnits: rounded,
      totalUnits: round(toolUnits + rounded),
      byTool: Object.fromEntries(byTool),
      tokens: { inputTokens, outputTokens, reportingSteps, estimatedSteps },
      tokenUnitsAreEstimated: true,
      ...(tenant !== undefined ? { tenant } : {}),
    };
  }

  /** Rolls per-workflow costs up to a tenant, attaching the budget if there is one. */
  forTenant(tenant: string, costs: Iterable<WorkflowCost>): TenantCost {
    let workflows = 0;
    let toolUnits = 0;
    let tokenUnits = 0;
    for (const cost of costs) {
      if (cost.tenant !== tenant) continue;
      workflows += 1;
      toolUnits += cost.toolUnits;
      tokenUnits += cost.tokenUnits;
    }
    const total = round(toolUnits + tokenUnits);
    const budgetUnits = this.budgets.get(tenant);

    return {
      tenant,
      workflows,
      toolUnits: round(toolUnits),
      tokenUnits: round(tokenUnits),
      totalUnits: total,
      ...(budgetUnits !== undefined
        ? { budgetUnits, remainingUnits: round(budgetUnits - total) }
        : {}),
    };
  }

  budgetFor(tenant: string): number | undefined {
    return this.budgets.get(tenant);
  }

  /**
   * Decides whether a new workflow may start.
   *
   * Refusal is on *already-spent plus reservation* exceeding the ceiling, so a tenant
   * whose spend lands exactly on its budget is still refused the next workflow rather
   * than being allowed one more over the line.
   */
  admit(tenant: string | undefined, spentUnits: number, requestedUnits = this.reserveUnits): BudgetDecision {
    if (tenant === undefined) {
      return { allowed: true, spentUnits, requestedUnits };
    }
    const budgetUnits = this.budgets.get(tenant);
    if (budgetUnits === undefined) {
      return { allowed: true, tenant, spentUnits, requestedUnits };
    }
    if (spentUnits + requestedUnits > budgetUnits) {
      return {
        allowed: false,
        tenant,
        spentUnits,
        requestedUnits,
        budgetUnits,
        reason:
          `tenant "${tenant}" has spent ${round(spentUnits)} of ${budgetUnits} cost units; ` +
          `admitting this workflow reserves ${requestedUnits} more, which exceeds the budget`,
      };
    }
    return { allowed: true, tenant, spentUnits, requestedUnits, budgetUnits };
  }
}

interface Usage {
  readonly model: string;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly estimated: boolean;
}

/**
 * Pulls token usage out of a step output.
 *
 * Tolerant of shape because the output of a model step is `unknown` by contract —
 * both camelCase and snake_case are accepted since providers disagree, and a step
 * that reports no usage at all contributes nothing rather than being guessed at. The
 * one guess made is for a `{ text }` output with no usage block, where the character
 * count is divided by four; that path is marked `estimated` so the dashboard can say
 * so.
 */
function readUsage(output: unknown): Usage | undefined {
  if (typeof output !== 'object' || output === null) return undefined;
  const record = output as Record<string, unknown>;
  const raw = record['usage'];

  if (typeof raw === 'object' && raw !== null) {
    const usage = raw as Record<string, unknown>;
    const input = num(usage['inputTokens']) ?? num(usage['input_tokens']);
    const outputTokens = num(usage['outputTokens']) ?? num(usage['output_tokens']);
    if (input !== undefined || outputTokens !== undefined) {
      return {
        model: typeof usage['model'] === 'string' ? usage['model'] : modelOf(record) ?? 'default',
        inputTokens: input ?? 0,
        outputTokens: outputTokens ?? 0,
        estimated: false,
      };
    }
  }

  const text = record['text'];
  if (typeof text === 'string' && text.length > 0) {
    return {
      model: modelOf(record) ?? 'default',
      inputTokens: 0,
      outputTokens: Math.ceil(text.length / 4),
      estimated: true,
    };
  }
  return undefined;
}

function modelOf(record: Record<string, unknown>): string | undefined {
  return typeof record['model'] === 'string' ? record['model'] : undefined;
}

function num(v: unknown): number | undefined {
  return typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : undefined;
}

/** Four decimals: enough that a token estimate is not rounded to zero. */
function round(units: number): number {
  return Math.round(units * 10_000) / 10_000;
}

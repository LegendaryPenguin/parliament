/**
 * The wire shapes the dashboard consumes.
 *
 * Declared here rather than imported from `src/api/trace.ts` for two reasons. The
 * server-side types describe the in-process objects, and the JSON differs in ways that
 * matter — `WorkflowProjection.steps` is a `Map` and arrives as an array. And the
 * browser build compiles with the DOM lib and no `@types/node`, so pulling a module
 * out of `src/` into this program would drag the Node-typed graph along with it.
 *
 * So this is the HTTP contract, written down as a type. It is duplication, and the
 * mitigation is that the API's responses are a compatibility surface anyway: if these
 * drift, the fix belongs in whichever side broke the contract.
 */

export type RunState =
  | 'pending'
  | 'ready'
  | 'running'
  | 'waiting'
  | 'retrying'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export interface StepView {
  readonly step: string;
  readonly state: RunState;
  readonly attempt: number;
  readonly workerId?: string;
  readonly leaseExpiresAt?: number;
  readonly notBefore?: number;
  readonly output?: unknown;
  readonly error?: string;
  readonly errorClass?: string;
  readonly awaitingApproval: boolean;
}

export interface ProjectionView {
  readonly workflowId: string;
  readonly workflow: string;
  readonly definitionVersion: number;
  readonly state: RunState;
  readonly input: unknown;
  readonly output?: unknown;
  readonly error?: string;
  readonly tenant?: string;
  readonly steps: readonly StepView[];
  readonly sequence: number;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface WorkflowSummary {
  readonly workflowId: string;
  readonly workflow: string;
  readonly definitionVersion: number;
  readonly state: RunState;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly sequence: number;
  readonly steps: number;
  readonly awaitingApproval: number;
  readonly failedSteps: number;
  readonly tenant?: string;
  readonly priority?: number;
  readonly error?: string;
}

export interface WorkflowListView {
  readonly workflows: readonly WorkflowSummary[];
  readonly total: number;
  readonly limit: number;
  readonly offset: number;
  readonly nextOffset?: number;
}

export interface EventView {
  readonly sequence: number;
  readonly workflowId: string;
  readonly type: string;
  readonly at: number;
  readonly causedBy?: number;
  readonly step?: string;
  readonly attempt?: number;
  readonly workerId?: string;
  readonly tool?: string;
  readonly error?: string;
  readonly errorClass?: string;
  readonly reason?: string;
  readonly approver?: string;
  readonly approvers?: readonly string[];
  readonly prompt?: string;
  readonly note?: string;
  readonly backoffMs?: number;
  readonly willRetry?: boolean;
  readonly priority?: number;
  readonly notBefore?: number;
  readonly durationMs?: number;
  readonly deduplicated?: boolean;
  readonly leaseExpiresAt?: number;
  readonly output?: unknown;
  readonly input?: unknown;
  readonly args?: unknown;
  readonly result?: unknown;
  readonly payload?: unknown;
  readonly workflow?: string;
  readonly definitionVersion?: number;
  readonly tenant?: string;
}

export interface ToolCallView {
  readonly tool: string;
  readonly step: string;
  readonly attempt: number;
  readonly idempotencyKey: string;
  readonly args: unknown;
  readonly requestedAt: number;
  readonly requestedSequence: number;
  readonly outcome: 'succeeded' | 'failed' | 'pending';
  readonly result?: unknown;
  readonly error?: string;
  readonly errorClass?: string;
  readonly durationMs?: number;
  readonly deduplicated?: boolean;
}

export interface AttemptView {
  readonly step: string;
  readonly attempt: number;
  readonly state: RunState;
  readonly workerId?: string;
  readonly scheduledAt?: number;
  readonly startedAt?: number;
  readonly endedAt?: number;
  readonly queuedMs?: number;
  readonly runningMs?: number;
  readonly error?: string;
  readonly errorClass?: string;
  readonly leaseExpired: boolean;
  readonly leaseExtensions: number;
  readonly willRetry?: boolean;
  readonly retry?: { readonly notBefore: number; readonly backoffMs: number };
  readonly toolCalls: readonly ToolCallView[];
  readonly sequences: readonly number[];
}

export interface TraceStepView {
  readonly step: string;
  readonly kind?: string;
  readonly dependsOn: readonly string[];
  readonly depth: number;
  readonly state: RunState;
  readonly attempts: readonly AttemptView[];
  readonly totalMs: number;
  readonly firstAt?: number;
  readonly lastAt?: number;
  readonly awaitingApproval: boolean;
  readonly retries: number;
  readonly leaseExpirations: number;
  readonly leaseExtensions: number;
  readonly toolCalls: number;
}

export interface ApprovalView {
  readonly step: string;
  readonly approvers: readonly string[];
  readonly prompt: string;
  readonly payload: unknown;
  readonly requestedAt: number;
  readonly requestedSequence: number;
  readonly decision: 'granted' | 'denied' | 'pending';
  readonly approver?: string;
  readonly note?: string;
  readonly reason?: string;
  readonly decidedAt?: number;
  readonly waitMs?: number;
}

export interface CostView {
  readonly toolUnits: number;
  readonly tokenUnits: number;
  readonly totalUnits: number;
  readonly tenant?: string;
  readonly tokens: {
    readonly inputTokens: number;
    readonly outputTokens: number;
    readonly reportingSteps: number;
    readonly estimatedSteps: number;
  };
  readonly byTool: Readonly<Record<string, { calls: number; deduplicated: number; failures: number; units: number }>>;
  readonly tokenUnitsAreEstimated: boolean;
}

export interface TraceView {
  readonly workflowId: string;
  readonly workflow: string;
  readonly definitionVersion: number;
  readonly tenant?: string;
  readonly priority?: number;
  readonly state: RunState;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly endedAt?: number;
  readonly totalMs?: number;
  readonly input: unknown;
  readonly output?: unknown;
  readonly error?: string;
  readonly sequence: number;
  readonly steps: readonly TraceStepView[];
  readonly approvals: readonly ApprovalView[];
  readonly events: readonly EventView[];
  readonly totals: {
    readonly events: number;
    readonly attempts: number;
    readonly retries: number;
    readonly leaseExpirations: number;
    readonly leaseExtensions: number;
    readonly toolCalls: number;
    readonly toolFailures: number;
  };
  readonly failure?: {
    readonly step?: string;
    readonly error: string;
    readonly errorClass: string;
    readonly atSequence: number;
  };
  readonly criticalPath: readonly string[];
  readonly cost: CostView;
}

export interface ReplayView {
  readonly workflowId: string;
  readonly sequence: number;
  readonly head: number;
  readonly projection: ProjectionView;
}

export interface ApiErrorBody {
  readonly error: string;
  readonly message: string;
  readonly detail?: unknown;
  readonly errors?: readonly { readonly node?: string; readonly message: string }[];
}

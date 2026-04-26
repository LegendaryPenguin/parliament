/**
 * A demo worker process.
 *
 * The worker itself is `src/workers/worker.ts`, unmodified: the lease-heartbeat-execute-report
 * loop, the tool executor, the idempotency keys and the node handlers are all the real ones.
 * This file supplies the three ports that `src/workers/ports.ts` declares, over HTTP to the
 * control plane, plus a tool executor configured for the demo.
 *
 * Running workers as separate OS processes is the point. The property the demo has to prove —
 * a worker dies mid-step and another one finishes the work — is only proven if the worker can
 * really die. A worker simulated inside the control plane could be "killed" by setting a flag,
 * and a flag is not evidence of anything: the interesting question is what happens to a lease
 * when the process holding it stops existing without getting the chance to say so, and the only
 * way to ask that question is `kill -9`.
 *
 * Two environment knobs exist for the demo and are named for what they do:
 *
 *  - `DEMO_RESULT_STORE=shared|isolated` — whether recorded tool outcomes live in the control
 *    plane (reachable by every worker) or in this process (lost when it dies). The demo runs
 *    the same scenario both ways, and the difference in the ledger file is the proof that
 *    deduplication is doing something.
 *  - `DEMO_DIE_ON_COMPLETE=<step>` — kill this process, with SIGKILL, at the moment it is about
 *    to report that step's success. That is the interesting crash window: the tool's side
 *    effect has happened and its outcome is recorded, but the event log does not know the step
 *    finished. Simulated here rather than by an external signal because the window is a few
 *    milliseconds wide and a shell cannot aim at it.
 */

import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { DefaultToolExecutor } from '../src/tools/executor.js';
import { registerAll } from '../src/tools/builtin.js';
import { InMemoryResultStore, type RecordedOutcome, type ResultStore } from '../src/tools/idempotency.js';
import { ToolError, type ToolOutcome } from '../src/tools/types.js';
import { RolePermissionPolicy } from '../src/policy/permissions.js';
import { createFakeProvider } from '../src/workers/llm.js';
import { Worker } from '../src/workers/worker.js';
import type {
  CompletedStep,
  Lease,
  LeaseManager,
  OutcomeReporter,
  StateReader,
  StepAssignment,
} from '../src/workers/ports.js';
import type { StepName, WorkflowId } from '../src/workflow/types.js';
import { demoTools } from './demo-tools.js';

/**
 * The worker's three ports, over HTTP.
 *
 * One class rather than three, because they share a base URL and a failure policy and splitting
 * them would mean three copies of the same `fetch` wrapper. The worker depends on the three
 * interfaces, not on this class, so it cannot tell.
 */
class HttpWorkerPorts implements LeaseManager, OutcomeReporter, StateReader {
  constructor(
    private readonly base: string,
    private readonly write: (message: string, fields?: Record<string, unknown>) => void,
  ) {}

  async acquire(id: string): Promise<StepAssignment | undefined> {
    const response = await fetch(`${this.base}/worker/acquire`, post({ workerId: id }));
    // 204 is "nothing to do", which is the ordinary answer to a poll and not a failure.
    if (response.status === 204) return undefined;
    if (!response.ok) throw new Error(`acquire -> ${response.status}`);
    const assignment = (await response.json()) as StepAssignment;
    this.write('leased', {
      step: assignment.lease.ref.step,
      workflow: assignment.lease.ref.workflowId,
      attempt: assignment.lease.ref.attempt,
      kind: assignment.node.kind,
    });
    return assignment;
  }

  async heartbeat(lease: Lease): Promise<boolean> {
    const response = await fetch(`${this.base}/worker/heartbeat`, post({ lease }));
    // A transport failure is *not* reported as a lost lease. The lease still has time left and
    // the next renewal may succeed; treating one refused connection as a revocation would
    // abandon work that was fine, which is precisely what a control plane restart looks like.
    if (!response.ok) throw new Error(`heartbeat -> ${response.status}`);
    const body = (await response.json()) as { alive: boolean };
    return body.alive;
  }

  async release(lease: Lease): Promise<void> {
    await fetch(`${this.base}/worker/release`, post({ lease }));
  }

  async completed(lease: Lease, output: unknown): Promise<void> {
    if (dieOnComplete !== undefined && lease.ref.step === dieOnComplete) {
      this.write('killing myself before reporting success', {
        step: lease.ref.step,
        attempt: lease.ref.attempt,
        note: 'the tool already ran; the log does not know the step finished',
      });
      // SIGKILL to self: uncatchable, no flush, no release, nothing on the wire. The lease is
      // left live and will have to lapse on its own, which is the whole scenario.
      process.kill(process.pid, 'SIGKILL');
    }
    const response = await fetch(`${this.base}/worker/completed`, post({ lease, output }));
    if (!response.ok) throw new Error(`completed -> ${response.status}`);
    this.write('reported completed', { step: lease.ref.step, attempt: lease.ref.attempt });
  }

  async failed(lease: Lease, error: { readonly message: string; readonly errorClass: string }): Promise<void> {
    const response = await fetch(`${this.base}/worker/failed`, post({ lease, error }));
    if (!response.ok) throw new Error(`failed -> ${response.status}`);
    this.write('reported failed', {
      step: lease.ref.step,
      attempt: lease.ref.attempt,
      errorClass: error.errorClass,
    });
  }

  async completedOutputs(workflowId: WorkflowId): Promise<ReadonlyMap<StepName, CompletedStep>> {
    const response = await fetch(
      `${this.base}/worker/outputs?workflowId=${encodeURIComponent(workflowId)}`,
    );
    if (!response.ok) throw new Error(`outputs -> ${response.status}`);
    const body = (await response.json()) as { outputs: CompletedStep[] };
    return new Map(body.outputs.map((entry) => [entry.step, entry]));
  }
}

/**
 * A `ResultStore` backed by the control plane.
 *
 * The reason this exists at all: `InMemoryResultStore` makes redelivery safe *within one
 * process*, which is everything the test suite needs and not enough for a worker that dies. Put
 * the recorded outcomes where the next worker can find them and the deduplication claim starts
 * holding across a process boundary. `ResultStore`'s own comment anticipates this — its methods
 * return promises precisely so a durable implementation can replace the in-memory one without
 * the executor noticing.
 *
 * The `ToolError` round trip is the part worth watching. JSON cannot carry a class, so a failed
 * outcome has to be rebuilt into a real `ToolError` on the way out, or the executor would hand
 * the worker a plain object and the retry classification — which reads `errorClass` off the
 * error — would silently see `undefined`.
 */
class HttpResultStore implements ResultStore {
  private seen = 0;

  constructor(private readonly base: string) {}

  async get(key: string): Promise<RecordedOutcome | undefined> {
    const response = await fetch(`${this.base}/results/${encodeURIComponent(key)}`);
    if (response.status === 404) return undefined;
    if (!response.ok) throw new Error(`result lookup -> ${response.status}`);
    const body = (await response.json()) as RecordedOutcome;
    return { recordedAt: body.recordedAt, outcome: reviveOutcome(body.outcome) };
  }

  async record(key: string, outcome: ToolOutcome, now = Date.now()): Promise<void> {
    const response = await fetch(`${this.base}/results/${encodeURIComponent(key)}`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ outcome: serializeOutcome(outcome), recordedAt: now }),
    });
    if (!response.ok) throw new Error(`result record -> ${response.status}`);
    this.seen += 1;
  }

  size(): number {
    // What this process has written, not what the shared store holds. Named honestly rather
    // than answered with a second round trip the executor never asks for.
    return this.seen;
  }
}

const workerId = required('DEMO_WORKER_ID');
const baseUrl = required('DEMO_URL').replace(/\/$/, '');
const dataDir = required('DEMO_DATA_DIR');
const dieOnComplete = process.env['DEMO_DIE_ON_COMPLETE'];
const sharedResults = (process.env['DEMO_RESULT_STORE'] ?? 'shared') === 'shared';

const log = (message: string, fields?: Record<string, unknown>): void => {
  const detail = fields === undefined ? '' : ` ${JSON.stringify(fields)}`;
  process.stdout.write(`[${workerId}] ${message}${detail}\n`);
};

/**
 * The real permission policy, not `AllowAllPolicy`.
 *
 * A demo that ran with permissions disabled would be demonstrating a system nobody deploys.
 * The role is `worker`, it is granted every registered tool, and its effect ceiling is `write`
 * — so `demo.ledger` is permitted and anything `destructive` would be refused even though the
 * grant list says `*`.
 */
const policy = new RolePermissionPolicy([{ name: 'worker', grantedTools: ['*'], maxEffect: 'write' }]);

const tools = new DefaultToolExecutor({
  policy,
  store: sharedResults ? new HttpResultStore(baseUrl) : new InMemoryResultStore(),
});
registerAll(tools, demoTools({ ledgerPath: `${dataDir}/ledger.tsv`, statePath: `${dataDir}/state` }));

const transport = new HttpWorkerPorts(baseUrl, log);

const worker = new Worker({
  workerId,
  leases: transport,
  reporter: transport,
  state: transport,
  tools,
  // Deterministic and offline: the fake provider derives its answer from a hash of the prompt,
  // so the demo's output does not depend on a network or an API key. The latency is there so
  // the model call is visible in the timeline rather than instantaneous.
  llm: createFakeProvider({ seed: 'demo', latencyMs: 150 }),
  role: 'worker',
  pollIntervalMs: 60,
  log,
});

const stopping = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    stopping.abort();
  });
}

// The pid, so the demo can kill this worker for real. `tsx` runs the script in a child node
// process, so the pid the shell gets back from `$!` belongs to the launcher: killing that one
// leaves this one running, and the demo would claim a recovery that never had to happen.
await writeFile(join(dataDir, `${workerId}.pid`), `${process.pid}\n`, 'utf8');

log('started', { resultStore: sharedResults ? 'shared' : 'isolated', dieOnComplete: dieOnComplete ?? null });
await worker.run(stopping.signal);
log('stopped', { heartbeats: worker.heartbeatCount });



function serializeOutcome(outcome: ToolOutcome): unknown {
  const { error, ...rest } = outcome;
  return {
    ...rest,
    ...(error !== undefined
      ? { error: { errorClass: error.errorClass, message: error.message, retryable: error.retryable } }
      : {}),
  };
}

function reviveOutcome(outcome: ToolOutcome): ToolOutcome {
  const raw = outcome as unknown as {
    error?: { errorClass: string; message: string; retryable: boolean };
  };
  if (raw.error === undefined) return outcome;
  return {
    ...outcome,
    error: new ToolError(
      raw.error.errorClass as ToolError['errorClass'],
      raw.error.message,
      raw.error.retryable,
    ),
  };
}

function post(body: unknown): RequestInit {
  return {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  };
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === '') {
    throw new Error(`${name} must be set; run this through scripts/demo.sh`);
  }
  return value;
}

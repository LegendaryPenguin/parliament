/**
 * A stand-in `ControlPlanePort`, owned by the API layer.
 *
 * The real control plane (`src/control-plane/`, `src/scheduler/`) decides what a
 * submission, a cancellation or an approval means in events, and drives the steps
 * that follow. This writes only the events that the corresponding HTTP request
 * unambiguously implies — a submission creates the workflow and schedules the start
 * step; a denial fails the workflow — and schedules nothing further.
 *
 * It is here so the API is exercisable end to end on its own. When the real control
 * plane lands, `defaultDeps()` points at it and this file stops being used.
 */

import { randomUUID } from 'node:crypto';
import type { EventStore, NewEvent } from '../../events/types.js';
import type { StepName, WorkflowId } from '../../workflow/types.js';
import type { ControlPlanePort, SubmitRequest } from '../deps.js';

export class LocalControlPlane implements ControlPlanePort {
  /**
   * Submissions already accepted, by caller-supplied idempotency key. A client that
   * retries a submission it never saw the response to must not get a second
   * execution, and the key is the only way to tell a retry from a new request.
   */
  private readonly submitted = new Map<string, WorkflowId>();

  constructor(
    private readonly store: EventStore,
    private readonly now: () => number = Date.now,
    private readonly newId: () => string = () => `wf_${randomUUID()}`,
  ) {}

  async submit(request: SubmitRequest): Promise<WorkflowId> {
    const key = request.idempotencyKey;
    if (key !== undefined) {
      const existing = this.submitted.get(key);
      if (existing !== undefined) return existing;
    }

    const workflowId = this.newId();
    const at = this.now();
    const events: NewEvent[] = [
      {
        type: 'workflow.created',
        workflowId,
        at,
        workflow: request.definition.name,
        definitionVersion: request.definition.version,
        definition: request.definition,
        input: request.input,
        ...(request.deadlineAt !== undefined ? { deadlineAt: request.deadlineAt } : {}),
        ...(request.tenant !== undefined ? { tenant: request.tenant } : {}),
      },
      {
        type: 'step.scheduled',
        workflowId,
        at,
        causedBy: 1,
        step: request.definition.start,
        attempt: 1,
      },
    ];

    await this.store.append(workflowId, 0, events);
    if (key !== undefined) this.submitted.set(key, workflowId);
    return workflowId;
  }

  async cancel(workflowId: WorkflowId, reason: string, requestedBy?: string): Promise<void> {
    const head = await this.store.head(workflowId);
    const events: NewEvent[] = [
      {
        type: 'workflow.cancelled',
        workflowId,
        at: this.now(),
        reason,
        ...(requestedBy !== undefined ? { requestedBy } : {}),
      },
    ];
    await this.store.append(workflowId, head, events);
  }

  async approve(
    workflowId: WorkflowId,
    step: StepName,
    approver: string,
    note?: string,
  ): Promise<void> {
    const head = await this.store.head(workflowId);
    const events: NewEvent[] = [
      {
        type: 'approval.granted',
        workflowId,
        at: this.now(),
        step,
        approver,
        ...(note !== undefined ? { note } : {}),
      },
    ];
    await this.store.append(workflowId, head, events);
  }

  async deny(
    workflowId: WorkflowId,
    step: StepName,
    approver: string,
    reason: string,
  ): Promise<void> {
    const head = await this.store.head(workflowId);
    // A denial is terminal for the execution: nothing downstream of a gate that was
    // refused should run, and leaving the workflow `waiting` would hide that from
    // every list and every alarm.
    const events: NewEvent[] = [
      { type: 'approval.denied', workflowId, at: this.now(), step, approver, reason },
      {
        type: 'workflow.failed',
        workflowId,
        at: this.now(),
        causedBy: head + 1,
        step,
        error: `approval denied by ${approver}: ${reason}`,
        errorClass: 'approval_denied',
      },
    ];
    await this.store.append(workflowId, head, events);
  }
}

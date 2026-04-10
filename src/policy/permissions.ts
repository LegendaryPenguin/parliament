/**
 * The capability check that runs before any tool is invoked.
 *
 * Three independent questions, kept independent because collapsing them produces a
 * policy nobody can reason about:
 *
 * 1. Is this role allowed to use this tool at all? (grant)
 * 2. Is the *kind* of effect this tool has within the role's ceiling? (escalation)
 * 3. Has a human approved this specific step, if the tool demands it? (approval)
 *
 * A role could in principle be granted a tool whose effect exceeds its ceiling. That
 * combination is a configuration mistake, and the escalation rule is what makes it
 * fail closed instead of granting the write.
 *
 * Every denial names the role, the tool, and the rule that rejected it. An operator
 * reading "permission denied" in an event history and unable to tell which of three
 * rules fired has to re-derive the policy by hand to fix a broken workflow, which in
 * practice means the policy gets loosened until the error stops.
 */

import type { StepRef } from '../workflow/types.js';
import { stepRefKey } from '../workflow/types.js';
import type { ToolDef, ToolEffect } from '../tools/types.js';
import { ToolError } from '../tools/types.js';

/** The rules that can deny a call. A closed set so denials can be counted and alerted. */
export type DenialRule =
  | 'unknown_role'
  | 'tool_not_granted'
  | 'role_not_allowed_by_tool'
  | 'effect_escalation'
  | 'approval_required';

/**
 * Effects are ordered: a ceiling of `read` admits only `read`, `write` admits `read`
 * and `write`. Ordering them means a new role needs one bound rather than an
 * enumeration of permitted effects, and a tool whose effect is later tightened is
 * automatically denied to roles that were only ever meant to read.
 */
const EFFECT_RANK: Readonly<Record<ToolEffect, number>> = {
  read: 0,
  write: 1,
  destructive: 2,
};

export interface RoleDef {
  readonly name: string;

  /**
   * Tool names this role may invoke. `'*'` grants every registered tool and is
   * supported because an operator role that must be able to run anything is a real
   * requirement; the effect ceiling still applies on top of it, so `'*'` is not a
   * way to acquire `destructive` by accident.
   */
  readonly grantedTools: readonly string[];

  /** The strongest effect this role may cause. */
  readonly maxEffect: ToolEffect;
}

/** Convenience constructor for the common read-only role. */
export function readOnlyRole(name: string, grantedTools: readonly string[]): RoleDef {
  return { name, grantedTools, maxEffect: 'read' };
}

/**
 * Records that a human approved one tool call at one step.
 *
 * Keyed by step attempt *and* tool. Not by workflow: an approval that covered the
 * whole execution would let a later step invoke the same tool with different arguments
 * under an approval granted for something else.
 */
export interface ApprovalStore {
  isApproved(ref: StepRef, tool: string): boolean;
  approve(ref: StepRef, tool: string, approver: string): void;
  approverFor(ref: StepRef, tool: string): string | undefined;
}

export class InMemoryApprovalStore implements ApprovalStore {
  private readonly approvals = new Map<string, string>();

  private keyOf(ref: StepRef, tool: string): string {
    return `${stepRefKey(ref)}::${tool}`;
  }

  isApproved(ref: StepRef, tool: string): boolean {
    return this.approvals.has(this.keyOf(ref, tool));
  }

  approve(ref: StepRef, tool: string, approver: string): void {
    this.approvals.set(this.keyOf(ref, tool), approver);
  }

  approverFor(ref: StepRef, tool: string): string | undefined {
    return this.approvals.get(this.keyOf(ref, tool));
  }
}

/** What the policy was asked about. */
export interface PermissionRequest {
  readonly role: string;
  readonly def: ToolDef;
  readonly ref: StepRef;
}

/** A denial, before it is turned into a `ToolError`. */
export interface Denial {
  readonly rule: DenialRule;
  readonly message: string;
}

export interface PermissionPolicy {
  /** `undefined` means permitted. */
  check(request: PermissionRequest): Denial | undefined;
}

export class RolePermissionPolicy implements PermissionPolicy {
  private readonly roles = new Map<string, RoleDef>();

  constructor(
    roles: readonly RoleDef[] = [],
    private readonly approvals: ApprovalStore = new InMemoryApprovalStore(),
  ) {
    for (const role of roles) this.roles.set(role.name, role);
  }

  addRole(role: RoleDef): void {
    this.roles.set(role.name, role);
  }

  approvalStore(): ApprovalStore {
    return this.approvals;
  }

  check({ role: roleName, def, ref }: PermissionRequest): Denial | undefined {
    const where = `role "${roleName}" tool "${def.name}"`;

    const role = this.roles.get(roleName);
    if (!role) {
      // An unrecognized role is denied rather than defaulted to a minimal one. A typo
      // in a step's role should break the step loudly, not silently reduce it to
      // read-only and leave an operator wondering why writes vanished.
      return {
        rule: 'unknown_role',
        message: `${where}: denied by rule "unknown_role" — role is not defined in the policy`,
      };
    }

    // The tool's own allowlist is checked before the role's grants so that a tool can
    // restrict itself to a narrow audience even if a broad role holds `'*'`.
    if (!def.permissions.allowedRoles.includes(roleName)) {
      return {
        rule: 'role_not_allowed_by_tool',
        message:
          `${where}: denied by rule "role_not_allowed_by_tool" — the tool admits only ` +
          `[${def.permissions.allowedRoles.join(', ')}]`,
      };
    }

    const granted = role.grantedTools.includes('*') || role.grantedTools.includes(def.name);
    if (!granted) {
      return {
        rule: 'tool_not_granted',
        message:
          `${where}: denied by rule "tool_not_granted" — the role grants ` +
          `[${role.grantedTools.join(', ')}]`,
      };
    }

    if (EFFECT_RANK[def.permissions.effect] > EFFECT_RANK[role.maxEffect]) {
      return {
        rule: 'effect_escalation',
        message:
          `${where}: denied by rule "effect_escalation" — tool effect "${def.permissions.effect}" ` +
          `exceeds the role's maximum effect "${role.maxEffect}"`,
      };
    }

    if (def.permissions.requiresApproval && !this.approvals.isApproved(ref, def.name)) {
      return {
        rule: 'approval_required',
        message:
          `${where}: denied by rule "approval_required" — no recorded approval for step ` +
          `${stepRefKey(ref)}`,
      };
    }

    return undefined;
  }
}

/** Turns a denial into the error the executor reports. */
export function denialToError(denial: Denial): ToolError {
  return ToolError.permissionDenied(denial.message);
}

/**
 * A policy that permits everything. Exists so a test or a single-tenant script can
 * construct an executor without a role table, and named so that finding it in
 * production configuration is obvious.
 */
export class AllowAllPolicy implements PermissionPolicy {
  check(): undefined {
    return undefined;
  }
}

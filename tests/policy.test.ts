import { describe, expect, it } from 'vitest';

import {
  AllowAllPolicy,
  InMemoryApprovalStore,
  RolePermissionPolicy,
  denialToError,
  readOnlyRole,
} from '../src/policy/permissions.js';
import type { RoleDef } from '../src/policy/permissions.js';
import { DefaultToolExecutor, toolContext } from '../src/tools/executor.js';
import type { ToolDef, ToolEffect, ToolHandler } from '../src/tools/types.js';
import type { StepRef } from '../src/workflow/types.js';

const ref: StepRef = { workflowId: 'wf-1', step: 'act', attempt: 1 };

function toolDef(
  name: string,
  effect: ToolEffect,
  overrides: Partial<ToolDef['permissions']> = {},
): ToolDef {
  return {
    name,
    description: `${effect} tool`,
    input: { type: 'object', properties: { x: { type: 'integer' } }, required: ['x'] },
    output: { type: 'object', properties: { x: { type: 'integer' } }, required: ['x'] },
    permissions: {
      effect,
      allowedRoles: ['researcher', 'operator', 'admin'],
      requiresApproval: false,
      idempotent: true,
      ...overrides,
    },
    timeoutMs: 100,
    costUnits: 1,
  };
}

const passthrough: ToolHandler = async (args) => args;

const researcher = readOnlyRole('researcher', ['read.thing', 'write.thing', 'nuke.thing']);
const operator: RoleDef = {
  name: 'operator',
  grantedTools: ['read.thing', 'write.thing'],
  maxEffect: 'write',
};
const admin: RoleDef = { name: 'admin', grantedTools: ['*'], maxEffect: 'destructive' };

function policy(): RolePermissionPolicy {
  return new RolePermissionPolicy([researcher, operator, admin], new InMemoryApprovalStore());
}

describe('role grants', () => {
  it('permits a granted read for a read-only role', () => {
    const denial = policy().check({ role: 'researcher', def: toolDef('read.thing', 'read'), ref });
    expect(denial).toBeUndefined();
  });

  it('denies an unknown role and names the rule', () => {
    const denial = policy().check({ role: 'ghost', def: toolDef('read.thing', 'read'), ref });

    expect(denial?.rule).toBe('unknown_role');
    expect(denial?.message).toContain('role "ghost"');
    expect(denial?.message).toContain('tool "read.thing"');
    expect(denial?.message).toContain('"unknown_role"');
  });

  it('denies a tool the role was never granted', () => {
    const denial = policy().check({ role: 'operator', def: toolDef('nuke.thing', 'read'), ref });

    expect(denial?.rule).toBe('tool_not_granted');
    expect(denial?.message).toContain('role "operator"');
    expect(denial?.message).toContain('read.thing, write.thing');
  });

  it("denies a role the tool itself does not admit, even with a '*' grant", () => {
    const def = toolDef('read.thing', 'read', { allowedRoles: ['researcher'] });
    const denial = policy().check({ role: 'admin', def, ref });

    expect(denial?.rule).toBe('role_not_allowed_by_tool');
    expect(denial?.message).toContain('the tool admits only [researcher]');
  });
});

describe('effect escalation', () => {
  it('denies a read-only role a write tool', () => {
    const denial = policy().check({ role: 'researcher', def: toolDef('write.thing', 'write'), ref });

    expect(denial?.rule).toBe('effect_escalation');
    expect(denial?.message).toContain('tool effect "write"');
    expect(denial?.message).toContain('maximum effect "read"');
  });

  it('denies a read-only role a destructive tool', () => {
    const denial = policy().check({
      role: 'researcher',
      def: toolDef('nuke.thing', 'destructive'),
      ref,
    });

    expect(denial?.rule).toBe('effect_escalation');
  });

  it('denies a write role a destructive tool even when granted', () => {
    const def = toolDef('write.thing', 'destructive');
    const denial = policy().check({ role: 'operator', def, ref });

    expect(denial?.rule).toBe('effect_escalation');
    expect(denial?.message).toContain('maximum effect "write"');
  });

  it("does not let a '*' grant lift the effect ceiling", () => {
    const narrow: RoleDef = { name: 'wide', grantedTools: ['*'], maxEffect: 'read' };
    const p = new RolePermissionPolicy([narrow]);
    const def = toolDef('write.thing', 'write', { allowedRoles: ['wide'] });

    expect(p.check({ role: 'wide', def, ref })?.rule).toBe('effect_escalation');
  });

  it('permits an effect at exactly the ceiling', () => {
    expect(policy().check({ role: 'operator', def: toolDef('write.thing', 'write'), ref })).toBeUndefined();
  });
});

describe('approval', () => {
  const def = toolDef('write.thing', 'write', { requiresApproval: true });

  it('denies without a recorded approval and says which step is missing one', () => {
    const denial = policy().check({ role: 'operator', def, ref });

    expect(denial?.rule).toBe('approval_required');
    expect(denial?.message).toContain('wf-1/act/1');
  });

  it('permits once an approval is recorded for that step and tool', () => {
    const p = policy();
    p.approvalStore().approve(ref, 'write.thing', 'alice');

    expect(p.check({ role: 'operator', def, ref })).toBeUndefined();
    expect(p.approvalStore().approverFor(ref, 'write.thing')).toBe('alice');
  });

  it('does not let an approval for one tool cover another', () => {
    const p = policy();
    p.approvalStore().approve(ref, 'read.thing', 'alice');

    expect(p.check({ role: 'operator', def, ref })?.rule).toBe('approval_required');
  });

  it('does not let an approval for one step cover another step', () => {
    const p = policy();
    p.approvalStore().approve(ref, 'write.thing', 'alice');

    const other = { ...ref, step: 'act-2' };
    expect(p.check({ role: 'operator', def, ref: other })?.rule).toBe('approval_required');
  });

  it('does not let an approval for one attempt cover a later attempt', () => {
    const p = policy();
    p.approvalStore().approve(ref, 'write.thing', 'alice');

    expect(p.check({ role: 'operator', def, ref: { ...ref, attempt: 2 } })?.rule).toBe(
      'approval_required',
    );
  });
});

describe('denial reporting', () => {
  it('turns every denial into a non-retryable permission_denied', () => {
    const denial = policy().check({ role: 'ghost', def: toolDef('read.thing', 'read'), ref });
    const error = denialToError(denial!);

    expect(error.errorClass).toBe('permission_denied');
    expect(error.retryable).toBe(false);
    expect(error.message).toBe(denial!.message);
  });

  it('names role, tool and rule for every rule in the set', () => {
    const p = policy();
    const combos: Array<{ role: string; def: ToolDef; rule: string }> = [
      { role: 'ghost', def: toolDef('read.thing', 'read'), rule: 'unknown_role' },
      { role: 'operator', def: toolDef('nuke.thing', 'read'), rule: 'tool_not_granted' },
      {
        role: 'admin',
        def: toolDef('read.thing', 'read', { allowedRoles: ['researcher'] }),
        rule: 'role_not_allowed_by_tool',
      },
      { role: 'researcher', def: toolDef('write.thing', 'write'), rule: 'effect_escalation' },
      {
        role: 'operator',
        def: toolDef('write.thing', 'write', { requiresApproval: true }),
        rule: 'approval_required',
      },
    ];

    for (const combo of combos) {
      const denial = p.check({ role: combo.role, def: combo.def, ref });
      expect(denial?.rule, combo.rule).toBe(combo.rule);
      expect(denial?.message).toContain(`role "${combo.role}"`);
      expect(denial?.message).toContain(`tool "${combo.def.name}"`);
      expect(denial?.message).toContain(`denied by rule "${combo.rule}"`);
    }
  });
});

describe('the executor honours the policy', () => {
  it('denies before validating arguments or invoking', async () => {
    const ex = new DefaultToolExecutor({ policy: policy() });
    let calls = 0;
    ex.register(toolDef('write.thing', 'write'), async (args) => {
      calls += 1;
      return args;
    });

    // Arguments are invalid too; the denial must win, so a denied caller learns nothing
    // about the tool's schema.
    const args = { bogus: true };
    const outcome = await ex.invoke(
      'write.thing',
      args,
      toolContext({ ref, role: 'researcher', tool: 'write.thing', args }),
    );

    expect(calls).toBe(0);
    expect(outcome.error?.errorClass).toBe('permission_denied');
    expect(outcome.error?.message).toContain('effect_escalation');
  });

  it('does not let a denied caller read a cached result', async () => {
    const p = policy();
    const ex = new DefaultToolExecutor({ policy: p });
    ex.register(toolDef('write.thing', 'write'), passthrough);
    const args = { x: 1 };

    const allowed = await ex.invoke(
      'write.thing',
      args,
      toolContext({ ref, role: 'operator', tool: 'write.thing', args }),
    );
    expect(allowed.ok).toBe(true);

    // Same step, same tool, same arguments — so the same idempotency key — but a role
    // that may not write. The dedup lookup must never be reached.
    const denied = await ex.invoke(
      'write.thing',
      args,
      toolContext({ ref, role: 'researcher', tool: 'write.thing', args }),
    );

    expect(denied.ok).toBe(false);
    expect(denied.result).toBeUndefined();
    expect(denied.deduplicated).toBe(false);
    expect(denied.error?.errorClass).toBe('permission_denied');
  });

  it('runs an approval-gated tool only after the approval is recorded', async () => {
    const p = policy();
    const ex = new DefaultToolExecutor({ policy: p });
    const def = toolDef('write.thing', 'write', { requiresApproval: true });
    let calls = 0;
    ex.register(def, async (args) => {
      calls += 1;
      return args;
    });
    const args = { x: 1 };

    const before = await ex.invoke(
      'write.thing',
      args,
      toolContext({ ref, role: 'operator', tool: 'write.thing', args }),
    );
    expect(before.error?.message).toContain('approval_required');
    expect(calls).toBe(0);

    p.approvalStore().approve(ref, 'write.thing', 'alice');
    const after = await ex.invoke(
      'write.thing',
      args,
      toolContext({ ref, role: 'operator', tool: 'write.thing', args }),
    );

    expect(after.ok).toBe(true);
    expect(calls).toBe(1);
  });

  it('counts denials for alerting', async () => {
    const ex = new DefaultToolExecutor({ policy: policy() });
    ex.register(toolDef('write.thing', 'write'), passthrough);
    const args = { x: 1 };

    await ex.invoke(
      'write.thing',
      args,
      toolContext({ ref, role: 'researcher', tool: 'write.thing', args }),
    );

    expect(ex.statistics().denied).toBe(1);
    expect(ex.statistics().invocations).toBe(0);
  });

  it('AllowAllPolicy permits a destructive tool, as its name promises', async () => {
    const ex = new DefaultToolExecutor({ policy: new AllowAllPolicy() });
    ex.register(toolDef('nuke.thing', 'destructive', { requiresApproval: true }), passthrough);
    const args = { x: 1 };

    const outcome = await ex.invoke(
      'nuke.thing',
      args,
      toolContext({ ref, role: 'nobody', tool: 'nuke.thing', args }),
    );

    expect(outcome.ok).toBe(true);
  });
});

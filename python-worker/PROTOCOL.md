# Worker wire protocol

Newline-delimited JSON over a stream socket. One JSON object per line, UTF-8, `\n`
terminated. The worker connects; the control plane listens.

The protocol is deliberately small — nine message types — because it is the seam between
two languages. Every addition has to be implemented twice and can disagree twice, so the
rule is that anything derivable from the event log stays in the event log and does not
travel on the wire.

## Framing

Lines, not length prefixes. A log of this protocol is readable with `tail` and a line can
be pasted into a test, which is worth more here than the bytes a binary framing would
save: the traffic is a handful of messages per step, not a data path.

A line longer than `MAX_LINE_BYTES` (1 MiB) is a protocol error and closes the
connection. Without a cap, a peer that never sends `\n` makes the reader buffer until the
process dies.

## Correlation

Every message carries `id`, a positive integer chosen by the sender. A reply repeats the
`id` of the request it answers. Ids are per-connection and per-direction.

Correlation by id rather than by strict request/response ordering is what lets the
heartbeat share one socket with the lease and report traffic: a heartbeat reply arriving
between a report being sent and its ack is normal, not a desync.

## Messages

### Worker to control plane

| `t`              | Fields                                                   | Meaning |
|------------------|----------------------------------------------------------|---------|
| `lease.request`  | `worker`, `kinds`                                        | Asks for work this worker can run. |
| `lease.heartbeat`| `worker`, `lease`, `ref`                                 | Renews a held lease. |
| `step.completed` | `worker`, `lease`, `ref`, `output`                       | The step succeeded. |
| `step.failed`    | `worker`, `lease`, `ref`, `error`                        | The step failed. `error` is `{class, message, traceback?}`. |
| `lease.release`  | `worker`, `lease`, `ref`                                 | Gives the lease back without claiming an outcome. |

### Control plane to worker

| `t`              | Fields                                                                         | Meaning |
|------------------|--------------------------------------------------------------------------------|---------|
| `lease.granted`  | `ref`, `lease`, `callable`, `input`, `timeout_ms`, `lease_duration_ms`          | Work, plus the two budgets the worker needs. |
| `lease.empty`    | —                                                                              | Nothing to do. The worker decides when to ask again. |
| `lease.renewed`  | `ok`, `expires_at`                                                             | `ok: false` means the lease is gone; the worker must stop. |
| `ack`            | —                                                                              | A report or release was durably recorded. |
| `error`          | `message`                                                                      | The message was rejected. Not a step failure. |

`ref` is `{"workflow_id": str, "step": str, "attempt": int}` — the same triple
idempotency keys are derived from on the TypeScript side.

`lease` is an opaque token. The worker never parses it; it only echoes it back, so the
control plane can change what it encodes without a protocol version bump.

## Semantics

- **Lease, heartbeat, execute, report, release** — the same order as the TypeScript
  worker, and the heartbeat runs on its own thread so a slow callable cannot delay a
  renewal.
- `lease.renewed` with `ok: false` ends the step: the worker reports nothing and releases
  nothing, because the lease is already somebody else's and a release would revoke the
  new holder's claim.
- Delivery is at-least-once. A `lease.granted` for a `ref` the worker has already run may
  arrive again; the control plane's idempotency keys are derived from `ref`, so repeating
  the same attempt is safe for callables that are idempotent and is not safe for ones
  that are not. The protocol does not try to hide that difference.
- A `step.failed` carries a `traceback` when one exists. Sending it is the point of
  running a separate interpreter at all: without it a Python failure arrives as a one-line
  message and the only way to find the cause is to reproduce it.
- `error` is for malformed or unexpected messages. A failing *step* is always
  `step.failed`; conflating the two would make a bad payload look like a dead worker.

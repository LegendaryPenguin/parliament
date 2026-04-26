#!/usr/bin/env bash
#
# An end-to-end demo of the properties this project exists to provide.
#
# Each phase is built around a before/after that genuinely differs, because a demo step whose
# output looks the same whether or not the feature works is not a demonstration, it is a
# decoration. Where a single run could not distinguish the two, the phase runs a control: the
# retry phase submits the same failing step with and without a retry policy, and the idempotency
# phase runs the same crash with the recorded-outcome store shared and then isolated. The
# numbers printed under PROOF are read back out of the running control plane over HTTP, not
# computed by this script.
#
# What is real here: the event log on disk, the scheduler, the leases, the retry engine, the
# projector, the HTTP API, the tool executor and its idempotency keys. The workers are separate
# OS processes talking to the control plane over HTTP, which is what makes `kill -9` a meaningful
# thing to do to one.
#
# Environment:
#   DEMO_PORT   bind a fixed port instead of letting the kernel choose
#   DEMO_KEEP=1 leave the data directory and logs behind for inspection

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
ROOT="$PWD"
TSX="$ROOT/node_modules/.bin/tsx"

if [[ ! -x "$TSX" ]]; then
  echo "tsx is not installed; run 'npm install' first" >&2
  exit 1
fi

DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/parliament-demo.XXXXXX")"
LOGS="$DATA_DIR/logs"
mkdir -p "$LOGS"

# Every background process this script starts, so the trap can take them all down. Launcher pids
# and the real pids are both tracked: `tsx` runs the script in a child node process, so killing
# the launcher alone leaves an orphan holding the port.
LAUNCHERS=""
BASE=""

cleanup() {
  local status=$?
  set +e
  # Histories first: dumping them needs the control plane, which the next loop kills.
  if [[ "${DEMO_KEEP:-}" == "1" ]]; then
    dump_histories
  fi
  for pidfile in "$DATA_DIR"/*.pid; do
    [[ -f "$pidfile" ]] && kill -9 "$(cat "$pidfile")" 2>/dev/null
  done
  for pid in $LAUNCHERS; do
    kill -9 "$pid" 2>/dev/null
  done
  if [[ "${DEMO_KEEP:-}" == "1" ]]; then
    echo
    echo "kept $DATA_DIR (logs in $LOGS, histories in $DATA_DIR/histories)"
  else
    rm -rf "$DATA_DIR"
  fi
  exit $status
}
# Not a bare `wait`: nothing started here ever exits on its own, so waiting on them would hang
# forever and the trap would never run.
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------------------------
# Plumbing
# ---------------------------------------------------------------------------------------------

banner() {
  echo
  echo "=============================================================================="
  echo "  $*"
  echo "=============================================================================="
}

step() { echo; echo "-- $*"; }
proof() { echo "   PROOF  $*"; }
note() { echo "          $*"; }

# Evaluates a JavaScript expression against a JSON document on stdin, with the document as `d`.
#
# `node` rather than `jq`, because node is already a dependency of this project and jq is not —
# a demo that cannot run without first installing a tool is a demo that does not get run. The
# whole body is read before parsing, so this never half-consumes a pipe and provokes a SIGPIPE
# in curl, which under `set -o pipefail` would fail the script for no reason.
jget() {
  node -e '
    const chunks = [];
    process.stdin.on("data", (c) => chunks.push(c)).on("end", () => {
      const d = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      const value = new Function("d", `return (${process.argv[1]});`)(d);
      process.stdout.write(value === undefined || value === null ? "" : String(value));
    });
  ' "$1"
}

# A GET against the control plane, retried briefly.
#
# Every call through here is a read, so retrying is safe, and the retry is worth having: a demo
# that aborts because one request landed in the half-second a socket was unavailable would look
# like the control plane had failed when it had not. When it really is unreachable, the message
# says so and names the log, instead of leaving a bare curl exit code under `set -e`.
#
# Phase 4 deliberately does not use this: it is checking that the control plane is *down*, and a
# helper that retries would be fighting it.
api() {
  local path="$1" attempt
  for attempt in 1 2 3; do
    if curl -sS --fail-with-body -m 10 "$BASE$path" 2>/dev/null; then
      return 0
    fi
    sleep 0.3
  done
  echo "the control plane did not answer GET $path (tried 3 times)" >&2
  echo "see $LOGS/control-plane.log" >&2
  return 1
}

# Submits a workflow by definition name and prints the id.
#
# The definition is fetched from the control plane rather than written out here, so that
# `scripts/demo-workflows.ts` stays the only place a graph is defined.
#
# The whole 201 body is left in a file rather than a variable, because callers use this inside a
# command substitution to capture the id and a subshell cannot assign to its parent. It is worth
# keeping: what the control plane says at the moment of submission is the only honest "before"
# for a system this fast, and a follow-up GET has already lost the race.
submit() {
  local name="$1" input="$2" tenant="${3:-acme}" body code response
  body="$(api "/demo/definitions/$name" \
    | node -e '
        const chunks = [];
        process.stdin.on("data", (c) => chunks.push(c)).on("end", () => {
          process.stdout.write(JSON.stringify({
            definition: JSON.parse(Buffer.concat(chunks).toString("utf8")),
            input: JSON.parse(process.argv[1]),
            tenant: process.argv[2],
          }));
        });
      ' "$input" "$tenant")"

  response="$(curl -sS -X POST "$BASE/workflows" \
    -H 'content-type: application/json' --data-binary "$body" -w '\n%{http_code}')"
  code="${response##*$'\n'}"
  response="${response%$'\n'*}"
  if [[ "$code" != "201" ]]; then
    echo "submitting $name returned HTTP $code:" >&2
    echo "$response" >&2
    return 1
  fi
  printf '%s' "$response" > "$DATA_DIR/last-submit.json"
  printf '%s' "$response" | jget 'd.workflowId'
}

# The body of the most recent successful submission.
last_submit() { cat "$DATA_DIR/last-submit.json"; }

state_of() { api "/workflows/$1" | jget 'd.state'; }

# Polls until an expression over a workflow's projection is true. Polling rather than sleeping:
# a fixed sleep is either longer than it needs to be or shorter than it needs to be, and the
# second kind of failure looks like a bug in the control plane.
await_workflow() {
  local id="$1" expression="$2" label="$3" deadline=$((SECONDS + ${4:-45}))
  while (( SECONDS < deadline )); do
    if [[ "$(api "/workflows/$id" | jget "$expression")" == "true" ]]; then
      return 0
    fi
    sleep 0.2
  done
  echo "TIMED OUT waiting for $label on $id" >&2
  api "/workflows/$id" | jget 'JSON.stringify(d, null, 2)' >&2 || true
  return 1
}

await_terminal() {
  await_workflow "$1" '["succeeded","failed","cancelled"].includes(d.state)' "a terminal state" "${2:-45}"
}

steps_table() {
  api "/workflows/$1" | jget '
    d.steps
      .map((s) => `   ${s.step.padEnd(10)} ${s.state.padEnd(10)} attempt=${s.attempt} worker=${s.workerId ?? "-"}${s.error ? "  " + s.error : ""}`)
      .join("\n")
  '
  echo
}

events_of() {
  api "/workflows/$1/events" | jget '
    d.events.map((e) => `   ${String(e.sequence).padStart(3)}  ${e.type.padEnd(22)} ${(e.step ?? "").padEnd(10)} ${e.workerId ?? ""}`).join("\n")
  '
  echo
}

start_server() {
  DEMO_DATA_DIR="$DATA_DIR" DEMO_PORT="${DEMO_PORT:-0}" "$TSX" "$ROOT/scripts/demo-server.ts" \
    >>"$LOGS/control-plane.log" 2>&1 &
  LAUNCHERS="$LAUNCHERS $!"
  # Detached from job control so that the shell does not print its own "Killed" notice when the
  # exit trap takes it down. The pid is already recorded, so nothing is lost by untracking it.
  disown %% 2>/dev/null || true

  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if [[ -f "$DATA_DIR/port" ]]; then
      BASE="http://127.0.0.1:$(cat "$DATA_DIR/port")"
      if curl -sf -m 1 "$BASE/healthz" >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 0.2
  done
  echo "control plane did not come up; see $LOGS/control-plane.log" >&2
  tail -20 "$LOGS/control-plane.log" >&2 || true
  return 1
}

# Starts a worker and waits for it to publish its pid, so a later `kill` cannot race the start.
start_worker() {
  local id="$1"
  shift
  rm -f "$DATA_DIR/$id.pid"
  env DEMO_WORKER_ID="$id" DEMO_URL="$BASE" DEMO_DATA_DIR="$DATA_DIR" "$@" \
    "$TSX" "$ROOT/scripts/demo-worker.ts" >>"$LOGS/worker-$id.log" 2>&1 &
  LAUNCHERS="$LAUNCHERS $!"
  disown %% 2>/dev/null || true

  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    [[ -f "$DATA_DIR/$id.pid" ]] && return 0
    sleep 0.1
  done
  echo "worker $id did not start; see $LOGS/worker-$id.log" >&2
  return 1
}

stop_worker() {
  local id="$1" pidfile="$DATA_DIR/$1.pid"
  [[ -f "$pidfile" ]] || return 0
  local pid
  pid="$(cat "$pidfile")"
  kill -TERM "$pid" 2>/dev/null || true
  local deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$pidfile"
}

# Writes each workflow's history to a file the replay CLI can read with `--file`.
#
# The definition is pulled out of the workflow.created event and written alongside the events,
# because the CLI falls back to inferring a chain from the history when it has no definition —
# which turns a diamond into a line. The log already carries the real one.
dump_histories() {
  [[ -n "$BASE" ]] || return 0
  mkdir -p "$DATA_DIR/histories" || return 0
  local id
  for id in $(api "/workflows?limit=200" 2>/dev/null | jget 'd.workflows.map((w) => w.workflowId).join(" ")' 2>/dev/null); do
    api "/workflows/$id/events" 2>/dev/null \
      | node -e '
          const chunks = [];
          process.stdin.on("data", (c) => chunks.push(c)).on("end", () => {
            const { events } = JSON.parse(Buffer.concat(chunks).toString("utf8"));
            const definition = events.find((e) => e.type === "workflow.created")?.definition;
            process.stdout.write(JSON.stringify({ definition, events }, null, 2));
          });
        ' > "$DATA_DIR/histories/$id.json" 2>/dev/null || true
  done
}

ledger_lines() {
  grep -c "$1" "$DATA_DIR/ledger.tsv" 2>/dev/null || echo 0
}

# ---------------------------------------------------------------------------------------------
# Phase 0 — boot
# ---------------------------------------------------------------------------------------------

banner "parliament demo"
echo
echo "  data directory   $DATA_DIR"
echo "  event log        $DATA_DIR/events  (one append-only JSONL file per workflow)"

start_server
echo "  control plane    $BASE"
echo "  dashboard        $BASE/ui/"
note ""
note "The control plane owns the log and arbitrates leases. It executes nothing: the"
note "workers below are separate OS processes that pull work over HTTP, which is what"
note "makes killing one of them a real test rather than a simulated one."

for id in w1 w2 w3; do start_worker "$id"; done
echo
echo "  workers          w1 w2 w3 (separate processes)"

# ---------------------------------------------------------------------------------------------
# Phase 1 — a multi-step DAG runs to completion
# ---------------------------------------------------------------------------------------------

banner "1. a workflow completes through a multi-step DAG"

REVIEW_ID="$(submit review '{"recordId":"rec-4417"}')"
SUBMITTED_AT="$(last_submit | jget '`state=${d.state}, ${d.sequence} events in the log`')"
step "submitted $REVIEW_ID  (intake -> {classify, screen} -> decide -> report)"
proof "as the 201 came back:  $SUBMITTED_AT"
note "two events: workflow.created, and step.scheduled for the one step with no dependencies"

await_terminal "$REVIEW_ID"
proof "after the workers ran: $(api "/workflows/$REVIEW_ID" | jget '`state=${d.state}, ${d.sequence} events in the log`'), succeeded steps=$(api "/workflows/$REVIEW_ID" | jget 'd.steps.filter((s) => s.state === "succeeded").length')/5"
steps_table "$REVIEW_ID"
note "classify is an llm node answered by the offline deterministic provider; decide is a"
note "fan-in that could not start until both branches succeeded. Note the worker column:"
note "three processes competed for these steps and each one was leased exactly once."

# ---------------------------------------------------------------------------------------------
# Phase 2 — a step fails and is retried per its policy
# ---------------------------------------------------------------------------------------------

banner "2. a step fails and is retried per its policy"

RETRY_ID="$(submit flaky-with-retry '{"label":"with-policy","failures":2}')"
PLAIN_ID="$(submit flaky-no-retry '{"label":"no-policy","failures":2}')"
step "the same tool, configured to fail its first two executions, submitted twice:"
note "$RETRY_ID  has retry {maxAttempts: 3, retryableErrors: [unavailable]}"
note "$PLAIN_ID  has no retry policy at all"

await_terminal "$RETRY_ID"
await_terminal "$PLAIN_ID"

proof "with a policy:    $(api "/workflows/$RETRY_ID" | jget 'd.state + " on attempt " + d.steps[d.steps.length-1].attempt')"
proof "without a policy: $(api "/workflows/$PLAIN_ID" | jget 'd.state + " on attempt " + d.steps[d.steps.length-1].attempt')"
step "the retried workflow's history:"
events_of "$RETRY_ID"
note "An absent policy means one attempt and nothing retryable — not a default policy."
note "Both failures were classified 'unavailable'; only one of them was allowed to retry."

# ---------------------------------------------------------------------------------------------
# Phase 3 — a worker is killed mid-step and another finishes the work
# ---------------------------------------------------------------------------------------------

banner "3. a worker is SIGKILLed mid-step and the work is recovered"

for id in w1 w2 w3; do stop_worker "$id"; done
note "stopped the pool so that exactly one worker can take the next step"
start_worker victim

HOLD_ID="$(submit long-hold '{"label":"recovery","holdMs":6000}')"
step "submitted $HOLD_ID; its only real step holds a worker for 6s"
await_workflow "$HOLD_ID" 'd.steps.some((s) => s.step === "hold" && s.state === "running")' "the step to start"

VICTIM_WORKER="$(api "/workflows/$HOLD_ID" | jget 'd.steps.find((s) => s.step === "hold").workerId')"
LEASE_UNTIL="$(api "/workflows/$HOLD_ID" | jget 'd.steps.find((s) => s.step === "hold").leaseExpiresAt')"
proof "before: hold is running on worker '$VICTIM_WORKER', lease live until $LEASE_UNTIL"

kill -9 "$(cat "$DATA_DIR/victim.pid")"
rm -f "$DATA_DIR/victim.pid"
note "SIGKILLed worker '$VICTIM_WORKER' mid-step. It wrote nothing, released nothing, and"
note "said nothing: the log still shows the step running on a process that no longer exists."
start_worker rescuer
note "started worker 'rescuer'. Waiting for the lease to lapse (2.5s) and be reassigned..."

await_terminal "$HOLD_ID" 60
FINAL_WORKER="$(api "/workflows/$HOLD_ID" | jget 'd.steps.find((s) => s.step === "hold").workerId')"
FINAL_ATTEMPT="$(api "/workflows/$HOLD_ID" | jget 'd.steps.find((s) => s.step === "hold").attempt')"
proof "after:  $(state_of "$HOLD_ID"), hold completed by worker '$FINAL_WORKER' on attempt $FINAL_ATTEMPT"
events_of "$HOLD_ID"
note "Read the history: step.lease_expired against '$VICTIM_WORKER', then step.leased by"
note "'$FINAL_WORKER' — on the SAME attempt number. A lost worker is not a failed step, so"
note "no retry budget was spent and the idempotency key did not change. That last detail is"
note "what makes re-running the attempt safe, and it is the subject of phase 6."

# ---------------------------------------------------------------------------------------------
# Phase 4 — the control plane crashes and replays to the same state
# ---------------------------------------------------------------------------------------------

banner "4. the control plane is SIGKILLed and replays to the same state"

api "/workflows?limit=200" | jget '
  d.workflows.slice().sort((a, b) => a.workflowId.localeCompare(b.workflowId))
    .map((w) => `${w.workflowId} ${w.state} seq=${w.sequence} steps=${w.steps} updated=${w.updatedAt}`)
    .join("\n")
' > "$DATA_DIR/before.txt"
echo >> "$DATA_DIR/before.txt"
note "snapshotted $(grep -c . "$DATA_DIR/before.txt") existing workflows"

CRASH_ID="$(submit long-hold '{"label":"crash","holdMs":5000}')"
step "submitted $CRASH_ID and waited for it to be mid-step"
await_workflow "$CRASH_ID" 'd.steps.some((s) => s.step === "hold" && s.state === "running")' "the step to start"
BEFORE_EVENTS="$(api "/workflows/$CRASH_ID/events" | jget 'd.events.length')"
proof "before: $CRASH_ID is $(state_of "$CRASH_ID") with $BEFORE_EVENTS events on disk"

SERVER_PID="$(cat "$DATA_DIR/server.pid")"
kill -9 "$SERVER_PID"
rm -f "$DATA_DIR/server.pid"
DOWN_DEADLINE=$((SECONDS + 15))
while (( SECONDS < DOWN_DEADLINE )); do
  curl -sf -m 1 "$BASE/healthz" >/dev/null 2>&1 || break
  sleep 0.2
done
if curl -sf -m 1 "$BASE/healthz" >/dev/null 2>&1; then
  echo "the control plane survived SIGKILL, which should be impossible" >&2
  exit 1
fi
note "SIGKILLed the control plane (pid $SERVER_PID). Nothing was flushed, nothing was closed."
note "Confirmed unreachable. Its in-memory state — the scheduler's timers, the lease records,"
note "every projection — is gone. All that is left is the append-only log."

DEMO_PORT="${BASE##*:}" start_server
note "restarted on the same data directory. Recovery is scheduler.recover(): fold the log,"
note "tick. There is no reconciliation pass, because a second source of truth about what was"
note "running is the thing this design does not have."

api "/workflows?limit=200" | jget '
  d.workflows.slice().sort((a, b) => a.workflowId.localeCompare(b.workflowId))
    .map((w) => `${w.workflowId} ${w.state} seq=${w.sequence} steps=${w.steps} updated=${w.updatedAt}`)
    .join("\n")
' > "$DATA_DIR/after.txt"
echo >> "$DATA_DIR/after.txt"

if diff <(grep -v "$CRASH_ID" "$DATA_DIR/before.txt") <(grep -v "$CRASH_ID" "$DATA_DIR/after.txt") >/dev/null; then
  proof "every pre-crash workflow folded to a byte-identical projection in the new process"
  note "($(grep -vc "$CRASH_ID" "$DATA_DIR/before.txt") workflows compared on state, head sequence, step count and updatedAt)"
else
  echo "   FAILED  the replayed projections differ from the pre-crash ones:" >&2
  diff "$DATA_DIR/before.txt" "$DATA_DIR/after.txt" >&2 || true
  exit 1
fi

await_terminal "$CRASH_ID" 60
AFTER_EVENTS="$(api "/workflows/$CRASH_ID/events" | jget 'd.events.length')"
proof "and the workflow that was mid-crash finished: $(state_of "$CRASH_ID"), $BEFORE_EVENTS events before the crash, $AFTER_EVENTS after"
note "Identical state for what was done, continued progress for what was not. The worker"
note "outlived the control plane; its heartbeats failed while the server was gone and it"
note "kept working, because a refused connection is not evidence that a lease was revoked."

# ---------------------------------------------------------------------------------------------
# Phase 5 — a human approval gate blocks and then releases
# ---------------------------------------------------------------------------------------------

banner "5. a human approval gate blocks, then releases"

GATE_ID="$(submit gated-payout '{"recordId":"rec-pay-88"}')"
step "submitted $GATE_ID  (prepare -> approve[human] -> settle)"
await_workflow "$GATE_ID" 'd.steps.some((s) => s.awaitingApproval)' "the gate to open"
proof "blocked: $(api "/workflows/$GATE_ID" | jget 'd.steps.map((s) => s.step + "=" + s.state).join(" ")')"
note "The gate holds no worker — a step waiting on a human is 'waiting', not 'running' —"
note "so the pool is free the whole time it is blocked."

WRONG_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  "$BASE/workflows/$GATE_ID/steps/approve/approve" \
  -H 'content-type: application/json' -d '{"approver":"bob@example.com"}')"
proof "bob@example.com tries to approve it: HTTP $WRONG_CODE (not on the approver list)"
sleep 1
proof "still blocked one second later: settle=$(api "/workflows/$GATE_ID" | jget 'd.steps.find((s) => s.step === "settle").state')"

RIGHT_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  "$BASE/workflows/$GATE_ID/steps/approve/approve" \
  -H 'content-type: application/json' -d '{"approver":"alice@example.com","note":"spot checked"}')"
proof "alice@example.com approves it: HTTP $RIGHT_CODE"
await_terminal "$GATE_ID"
proof "released: $(state_of "$GATE_ID"), $(api "/workflows/$GATE_ID" | jget 'd.steps.map((s) => s.step + "=" + s.state).join(" ")')"

DENY_ID="$(submit gated-payout '{"recordId":"rec-pay-89"}')"
await_workflow "$DENY_ID" 'd.steps.some((s) => s.awaitingApproval)' "the gate to open"
curl -sS -o /dev/null -X POST "$BASE/workflows/$DENY_ID/steps/approve/deny" \
  -H 'content-type: application/json' -d '{"approver":"alice@example.com","reason":"amount over limit"}'
await_terminal "$DENY_ID"
proof "the control: a denied gate on $DENY_ID gives $(state_of "$DENY_ID"), settle=$(api "/workflows/$DENY_ID" | jget 'd.steps.find((s) => s.step === "settle").state')"
note "A denial is terminal. The protected step must never run, and the only state that"
note "guarantees that is a terminal one."

# ---------------------------------------------------------------------------------------------
# Phase 6 — idempotency: a duplicate tool call is deduplicated by key
# ---------------------------------------------------------------------------------------------

banner "6. a duplicate tool call is deduplicated by its idempotency key"

stop_worker rescuer
note "The scenario, run twice: a worker executes a step whose tool appends a line to a file,"
note "and is SIGKILLed in the window after the tool returned and before the completion is"
note "recorded. The lease lapses, a second worker is handed the SAME attempt, and calls the"
note "same tool with the same idempotency key. The only thing that differs between the two"
note "runs is where recorded tool outcomes live."

for MODE in shared isolated; do
  step "recorded outcomes: $MODE"
  start_worker "dying-$MODE" DEMO_RESULT_STORE="$MODE" DEMO_DIE_ON_COMPLETE=write
  LEDGER_ID="$(submit ledger-write "{\"note\":\"$MODE\"}")"

  DYING_DEADLINE=$((SECONDS + 30))
  while (( SECONDS < DYING_DEADLINE )); do
    kill -0 "$(cat "$DATA_DIR/dying-$MODE.pid")" 2>/dev/null || break
    sleep 0.2
  done
  rm -f "$DATA_DIR/dying-$MODE.pid"
  note "worker 'dying-$MODE' died after the side effect; $LEDGER_ID is $(state_of "$LEDGER_ID") with $(ledger_lines "$LEDGER_ID") ledger line(s)"

  start_worker "second-$MODE" DEMO_RESULT_STORE="$MODE"
  await_terminal "$LEDGER_ID" 60
  proof "$MODE: $(state_of "$LEDGER_ID"), reported by '$(api "/workflows/$LEDGER_ID" | jget 'd.steps.find((s) => s.step === "write").workerId')', side effect happened $(ledger_lines "$LEDGER_ID") time(s)"
  stop_worker "second-$MODE"
done

step "the ledger file, one line per tool execution that actually ran:"
sed 's/^/   /' "$DATA_DIR/ledger.tsv"
note ""
note "Same step, same attempt, same key, same crash — one line when the recorded outcome was"
note "reachable by the second worker, two when it died with the first. The deduplication is"
note "doing the work, and the isolated run is what its absence looks like."

# ---------------------------------------------------------------------------------------------
# The debugging view
# ---------------------------------------------------------------------------------------------

banner "the recovered workflow, read back by the replay CLI"

note "The same history as phase 3, printed by src/cli/replay.ts over the HTTP API. This is"
note "the tool to reach for when a workflow is somewhere nobody expected: the marker column"
note "picks the abnormal events out of the routine ones, and the state transition each event"
note "caused is spelled out rather than left to be inferred from its name."
echo
PARLIAMENT_API="$BASE" "$TSX" "$ROOT/src/cli/replay.ts" "$HOLD_ID" 2>&1 | sed 's/^/   /' || true

# ---------------------------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------------------------

banner "every workflow this demo created"

api "/workflows?limit=200" | jget '
  d.workflows.slice().sort((a, b) => a.createdAt - b.createdAt)
    .map((w) => `   ${w.workflowId.padEnd(24)} ${w.state.padEnd(10)} ${String(w.sequence).padStart(3)} events  ${w.workflow}`)
    .join("\n")
'
echo

TOTAL_EVENTS="$(api "/workflows?limit=200" | jget 'd.workflows.reduce((a, w) => a + w.sequence, 0)')"
echo "   $TOTAL_EVENTS events on disk, in $(ls "$DATA_DIR/events/workflows" | wc -l | tr -d ' ') append-only files."
echo
echo "   The control plane and its log go away when this script exits. To keep them:"
echo "     DEMO_KEEP=1 npm run demo"
echo "   which leaves every history dumped under <data dir>/histories, readable offline:"
echo "     npx tsx src/cli/replay.ts --file <data dir>/histories/$REVIEW_ID.json"
echo
echo "   For the dashboard — the DAG, the timeline and the replay slider, fed by SSE off the"
echo "   same log — start a server with a seeded history and open it:"
echo "     npm run serve:demo    then    http://localhost:8787/ui/"

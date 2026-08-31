# App Server Event Bridge (experimental, disabled by default)

The event bridge is an **opt-in notification transport** that resumes one
bound Codex thread and starts exactly one wake turn after a **verified,
immutable terminal record** appears. It is experimental until every release
gate in the repository README is met, it is disabled unless explicitly
configured, and it never becomes terminal authority.

> A terminal file cannot awaken an inactive Codex turn by itself. Only the
> bridge — an explicitly configured delivery daemon — turns verified
> terminal evidence into a wake turn, and only into the exact bound thread.

## Mode contract

| Mode | Model turns while unchanged | Automatic Codex resume | Long-lived agent slot |
| --- | --- | ---: | ---: |
| `unattended` (default) | 0 | No | No |
| `external-event-bridge` | 0 | Yes, when the App Server is available | No |
| `attached` (short commands only) | 0 during one blocking call | Same turn only | No subagent slot |
| `goal-worker` | Runtime-dependent | Conditional compatibility mode only | One worker slot |

Failures of the bridge (App Server offline, overload `-32001`, connection
loss, timeout, wake-turn failure) never authorize retry, cancellation,
resubmission, mutation, or business approval of the monitored work. They
only change local delivery state.

## Architecture

```text
verified immutable terminal record (unchanged authority)
      |
semantic-event publisher   (supervisor, local only; event_intent.json
                            written at start; status/wait reconcile any
                            crash window between terminal and event)
      |
durable outbox             (atomic, local filesystem, at-least-once)
      |
optional delivery daemon   (foreground, or explicit user service)
      |
thread/resume -> turn/start -> wait turn/completed
      |                        (session held open; lease renewed throughout)
      |
idempotent postflight     (postflight_guard.py begin/complete claim)
```

Publishing an event requires no App Server contact; a dead App Server only
leaves events pending. Delivery is at-least-once — no network-level
exactly-once delivery is claimed: a crash between App Server acceptance and
the local acknowledgement may redeliver one event, and the postflight claim
state machine makes the redelivered wake harmless.

## Explicit enablement

1. Create a private configuration (never world-readable):

```bash
python3 <skill-dir>/scripts/app_server_bridge.py init-config \
  --output ~/.config/codex-monitor/bridge.json \
  --instance-id workstation-1 \
  --workspace /absolute/project/path \
  --codex-home ~/.codex \
  --command codex app-server \
  --enabled
```

Missing output parent directories are created with mode `0700`; symlinked or
non-directory parent components are rejected.

`init-config` resolves `command[0]` and stores an absolute executable path.
Legacy v1 configs with bare or relative command tokens remain readable;
managed service install/repair resolves them once, freezes both the exact token
and resulting absolute executable in the service definition, and rejects a
later mismatch even when relative paths share a basename.
The generated systemd/launchd definition also carries an explicit `PATH`, so
an npm Codex shim can find its pinned runtime outside an interactive shell.

The configuration records both the `codex_home` path and its digest; a
mismatch between them is rejected at load time, and every spawned App
Server runs with `CODEX_HOME` pinned to that path.

2. Check the installed App Server schema, then verify capability; doctor must
   agree in `--format text` and `--format json`:

```bash
python3 <skill-dir>/scripts/app_server_bridge.py protocol-check --experimental

python3 <skill-dir>/scripts/supervise_slurm_job.py doctor \
  --bridge-config ~/.config/codex-monitor/bridge.json \
  --state-dir ~/.cache/<skill-state>
```

3. Audit the existing outbox **before the first delivery process starts**:

```bash
python3 <skill-dir>/scripts/app_server_bridge.py activation-check \
  --state-dir ~/.cache/<skill-state> \
  --bridge-config ~/.config/codex-monitor/bridge.json
```

Unreadable entries block activation. Matching pending/leased events require
human inspection and an exact `--accept-event-id sha256:...` per event when
writing the durable receipt with `activation-check --activate --i-mean-it`;
dead letters are visible but not auto-claimed. Foreground and managed delivery
both require this receipt. See [operations.md](operations.md). Run the managed daemon before binding a
new production monitor so a terminal event cannot wait unnoticed.

4. Create a per-monitor binding naming the exact thread to resume:

```bash
python3 <skill-dir>/scripts/app_server_bridge.py init-binding \
  --output ~/.config/codex-monitor/binding-<task>.json \
  --thread-id thr_<id> --instance-id workstation-1 \
  --workspace /absolute/project/path --codex-home ~/.codex
```

5. Start a **new** monitor with both the binding and config identity check:

```bash
python3 <skill-dir>/scripts/supervise_slurm_job.py start <job-id> --host <login-host> ... \
  --event-binding ~/.config/codex-monitor/binding-<task>.json \
  --bridge-config ~/.config/codex-monitor/bridge.json \
  --require-auto-resume
```

The strict flag verifies the binding/config identity, enabled config, and
durable activation receipt before launch. It cannot prove daemon liveness.
Without the strict flag, a binding with no config remains compatible but emits
both a prominent stderr warning and a structured JSON warning.

The launcher rejects an attempt to add or change a binding on an already
active run as `active_run_binding_conflict`; bindings are immutable run
intent, not an attach operation. Let an existing unattended run finish and
use a fresh test run. For diagnostics only, foreground delivery remains
available after the durable activation receipt has been written:

```bash
python3 <skill-dir>/scripts/app_server_bridge.py deliver \
  --state-dir ~/.cache/<skill-state> \
  --bridge-config ~/.config/codex-monitor/bridge.json
```

For durable use, install the explicit user service from
[operations.md](operations.md). The skill never installs or starts it merely
by being invoked.

## Delivery lifecycle

One delivery attempt is one App Server session:

1. spawn the configured command with `CODEX_HOME` pinned;
2. `initialize` handshake and `initialized` notification;
3. `thread/resume {threadId}` — the returned `thread.id` **and** the
   returned `thread.cwd` must match the bound thread and workspace
   (missing or wrong `cwd` dead-letters as `binding_mismatch`); a
   different thread is never created;
4. `turn/start` with the fixed wake text;
5. **keep the session open and read notifications until the turn reaches
   `turn/completed`** (bounded by `turn_completion_timeout_seconds`) —
   closing after `turn/start` would abort the wake turn mid-postflight;
6. acknowledge only when that notification explicitly names the started
   turn and carries `status=completed`, recording `turn_id` and
   `turn_status` together. Official `failed`/`interrupted` statuses remain
   retryable; missing ids or malformed statuses never acknowledge.

The delivery lease is renewed after each stage and periodically while
waiting for turn completion. Configuration validation additionally
requires `lease_seconds >= 2 * request_timeout_seconds` so the request
budget always fits the lease. To stay above practical OS scheduling
granularity, validation also requires `request_timeout_seconds >= 0.05`
and `lease_seconds >= 0.1`. The read deadline is capped by the next
renewal tick even for sub-second leases, and losing ownership stops the
stale attempt without mutating another owner's state. These mechanisms make
concurrent delivery of one event require both a lease expiry and a
stale-owner race, and the postflight claim keeps even that race harmless.
No stronger exclusivity is claimed.

## What one wake turn receives

A fixed, locally generated template — never event-controlled free text:

```text
codex_monitor_event/v1
event_id=sha256:...
backend=slurm|artifact|dispatch
handle=<opaque>
generation=<opaque>
event=<transport_success|transport_failure|deadline_exceeded|lost_observability|contract_violation>
exit_code=<integer-or-null>
terminal_digest=sha256:...
business_verdict=pending

Verify the immutable terminal record and process this event idempotently.
Do not retry, cancel, resubmit, mutate, or approve the workload solely
because of this notification.
```

Only terminal records whose watcher result is **verified** produce
success/failure/observability events; an unverified record publishes
`contract_violation` instead. A Slurm pending-threshold alert (watcher
exit 4) publishes nothing — a queue-wait alert is not a monitoring
deadline.

## Mandatory idempotent postflight protocol

The woken turn must, in order:

1. verify the immutable terminal record and that its digest equals the
   event's `terminal_digest` (a mismatch blocks all postflight work);
2. atomically claim the postflight:

```bash
python3 <skill-dir>/scripts/postflight_guard.py begin <event_id> \
  --terminal-digest sha256:... --owner <turn-identity> --state-dir ...
```

For CLI convenience, `--terminal-digest` may instead be the raw 64-character
lowercase hex value. The guard normalizes it to `sha256:<hex>` before touching
state; event/config APIs continue to require the prefixed wire form.

   Exit `0` = this turn owns the postflight; `3` = already completed;
   `5` = another turn's claim is in progress — **fail closed**, report and
   stop (an unknown result is never auto-taken-over); `4` = digest
   conflict. Concurrent turns cannot both win the claim.

3. perform the postflight side effects exactly once;
4. complete the claim:

```bash
python3 <skill-dir>/scripts/postflight_guard.py complete <event_id> \
  --owner <turn-identity> --state-dir ...
```

A stuck `in_progress` claim (owner died mid-postflight) is recovered only
by an explicit human `reset --i-mean-it`, never automatically. Reset uses
the same lock as completion and refuses a completed marker, so it cannot
race with `complete` and reopen an already-recorded side effect.

## Failure matrix

| Situation | Local effect | Wake created? |
| --- | --- | --- |
| App Server offline / spawn failure | retry with backoff (`spawn_failed`) | later |
| Overload `-32001` | retry with backoff (`overloaded`) | later |
| Connection dropped before/after request | retry (`connection_lost`) | maybe duplicated; postflight claim keeps effects single |
| Request timeout | retry (`request_timeout`) | maybe duplicated |
| Required MCP startup failure | retry (`required_mcp_failure`) | later |
| App Server requests command/file/permission/MCP/user-input approval | dead-letter (`operator_interaction_required`); never answers or auto-approves | possibly started, never acknowledged |
| Active-turn conflict | retry (`active_turn_conflict`) | later |
| `turn/completed` reports `failed` / `interrupted` | retry (`turn_failed`/`turn_aborted`) | possibly duplicated; claim keeps effects single |
| Completion misses the target turn id | ignored until valid completion or timeout | no acknowledgement |
| Completion has a missing/unknown status | dead-letter (`unsupported_response_shape`) | no acknowledgement |
| Turn not completed within budget | retry (`turn_completion_timeout`); event stays undelivered | started but unacknowledged |
| Thread missing | dead-letter (`thread_missing`) | no; never creates a different thread |
| Thread archived | dead-letter (`thread_archived`) | no |
| Thread `cwd` missing or wrong | dead-letter (`binding_mismatch`) | no |
| Unexpected response shape | dead-letter (`unsupported_response_shape`) | no |
| Wrong workspace/instance/CODEX_HOME binding | never claimed by this daemon | no |

Retries use exponential backoff with jitter and dead-letter after
`max_attempts`. Inspect with `app_server_bridge.py status` or
`monitor_events.py timeline`. A human may explicitly requeue one corrected
dead-letter with `monitor_events.py retry ... --i-mean-it`; this never changes
the immutable event or terminal evidence.

## Multiple Codex instance isolation

Each event carries its binding (`codex_home_id`, `app_server_instance`,
`thread_id`, `workspace`). A daemon claims **only** events whose instance,
Codex-home digest, and workspace equal its configured identity; everything else
stays pending for its owning daemon. The spawned App Server runs with the
configured `CODEX_HOME`, and the resumed thread must report the bound
`cwd`. Event ids are full `sha256:<64 hex>` digests, so they can never
become path components that escape the outbox, and symlinked outbox
entries are ignored.

## Security boundaries

- Config and binding files must be regular, non-symlink, `0600` files;
  group/world-readable ones are rejected.
- Events contain only enums, opaque handles, and digests — never raw logs,
  prompts, responses, artifact contents, credentials, or callback text.
- Only the stable `initialize`, `thread/resume`, and `turn/start` methods
  plus turn notifications over stdio are used; no shell/process methods,
  no experimental WebSocket, no TCP.
- No tokens on command lines; `codex_home_id` is a non-secret digest of the
  CODEX_HOME path.
- Outbox and state directories are `0700`, files `0600`, symlinks rejected,
  sizes bounded.
- Server-initiated requests are treated as untrusted operator interaction.
  The bridge records only their method name, never request parameters, and
  never sends an approval response.

Protocol checks, service management, event timelines, non-model sinks, and
explicit dead-letter retry are documented in
[operations.md](operations.md).

## Recovery and disable

- Remove `--event-binding` from future starts (or delete the binding file):
  monitors fall back to pure `unattended`.
- Stop the delivery daemon and set `"enabled": false` (or remove the
  config): undelivered events simply stay pending in the outbox.
- If the supervisor died between publishing the terminal and publishing
  its event, any later `status`/`wait` observation reconciles the
  publication (idempotently, by event id). A transient publication failure
  is recorded for inspection but does not disable later reconciliation.
- `cleanup` removes only settled (delivered, or dead-letter with
  `--include-dead-letter`) outbox entries; terminal evidence is never
  touched.
- The deterministic monitoring core keeps working exactly as before with
  the bridge fully disabled.

## Schema compatibility

New schemas (`codex-monitor.event/v1`, `codex-monitor.delivery/v1`,
`codex-monitor.bridge-config/v1`, `codex-monitor.event-binding/v1`,
`codex-monitor.bridge-activation/v1`,
`codex-monitor.postflight/v1`, `codex-monitor.doctor/v1`,
`codex-monitor.list/v1`, bridge attempt and event-intent records) are
additive; see `COMPATIBILITY.md` at the repository root for the full
inventory and migration behavior. Old terminal and manifest records remain
readable and are marked `evidence_strength: legacy` rather than rewritten.

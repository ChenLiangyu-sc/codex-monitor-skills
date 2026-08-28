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
| --- | ---: | ---: | ---: |
| `unattended` (default) | 0 | No | No |
| `external-event-bridge` | 0 | Yes, when the App Server is available | No |
| `attached` (short commands only) | 0 during one blocking call | Same turn only | No subagent slot |
| `goal-worker` | Runtime-dependent | Conditional | One worker slot |

Failures of the bridge (App Server offline, overload `-32001`, connection
loss, timeout) never authorize retry, cancellation, resubmission, mutation,
or business approval of the monitored work. They only change local delivery
state.

## Architecture

```text
verified immutable terminal record (unchanged authority)
      |
semantic-event publisher   (supervisor, local only)
      |
durable outbox             (atomic, local filesystem, at-least-once)
      |
optional delivery daemon   (app_server_bridge.py deliver, foreground)
      |
thread/resume -> turn/start  (one wake turn in the bound thread)
      |
idempotent postflight     (postflight_guard.py, business_verdict=pending)
```

Publishing an event requires no App Server contact; a dead App Server only
leaves events pending. Delivery is at-least-once: a crash between App
Server acceptance and the local acknowledgement may redeliver one event,
and the postflight guard makes that harmless.

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

2. Create a per-monitor binding naming the exact thread to resume:

```bash
python3 <skill-dir>/scripts/app_server_bridge.py init-binding \
  --output ~/.config/codex-monitor/binding-<task>.json \
  --thread-id thr_<id> --instance-id workstation-1 \
  --workspace /absolute/project/path --codex-home ~/.codex
```

3. Verify capability with one command; it must agree in `--format text`
   and `--format json`:

```bash
python3 <skill-dir>/scripts/supervise_<backend>.py doctor \
  --bridge-config ~/.config/codex-monitor/bridge.json \
  --state-dir ~/.cache/<skill-state>
```

4. Start the monitor with the binding (and, optionally, the config for an
   identity cross-check; mismatch fails closed):

```bash
python3 <skill-dir>/scripts/supervise_<backend>.py start ... \
  --event-binding ~/.config/codex-monitor/binding-<task>.json
```

5. Run the delivery daemon in the foreground (you own its lifecycle; the
   skill never installs or starts services automatically):

```bash
python3 <skill-dir>/scripts/app_server_bridge.py deliver \
  --state-dir ~/.cache/<skill-state> \
  --bridge-config ~/.config/codex-monitor/bridge.json
```

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

The woken turn must then: run `postflight_guard.py check <event_id>`;
verify the terminal record digest matches `terminal_digest`; perform the
postflight once; run `postflight_guard.py mark <event_id> --terminal-digest
sha256:...`. A duplicate wake reports "already handled" and repeats no side
effects. A digest mismatch blocks postflight entirely.

## Failure matrix

| Situation | Local effect | Wake created? |
| --- | --- | --- |
| App Server offline / spawn failure | retry with backoff (`spawn_failed`) | later |
| Overload `-32001` | retry with backoff (`overloaded`) | later |
| Connection dropped before/after request | retry (`connection_lost`) | maybe duplicated, idempotent postflight |
| Request timeout | retry (`request_timeout`) | maybe duplicated |
| Required MCP startup failure | retry (`required_mcp_failure`) | later |
| Active-turn conflict | retry (`active_turn_conflict`) | later |
| Thread missing | dead-letter (`thread_missing`) | no; never creates a different thread |
| Thread archived | dead-letter (`thread_archived`) | no |
| Unexpected response shape | dead-letter (`unsupported_response_shape`) | no |
| Wrong workspace/instance binding | dead-letter (`binding_mismatch`) or never claimed | no |

Retries use exponential backoff with jitter and stop dead-lettering after
`max_attempts`. Inspect with `app_server_bridge.py status` and settle with
the `cleanup` command.

## Multiple Codex instance isolation

Each event carries its binding (`codex_home_id`, `app_server_instance`,
`thread_id`, `workspace`). A daemon claims **only** events whose instance
and Codex-home digests equal its configured identity; everything else stays
pending for its owning daemon. Leases are exclusive with expiry; two
daemons cannot deliver the same event concurrently.

## Security boundaries

- Config and binding files must be regular, non-symlink, `0600` files;
  group/world-readable ones are rejected.
- Events contain only enums, opaque handles, and digests — never raw logs,
  prompts, responses, artifact contents, credentials, or callback text.
- Only the stable `initialize`, `thread/resume`, and `turn/start` methods
  over stdio are used; no shell/process methods, no experimental WebSocket,
  no TCP.
- No tokens on command lines; `codex_home_id` is a non-secret digest of the
  CODEX_HOME path.
- Outbox and state directories are `0700`, files `0600`, symlinks rejected,
  sizes bounded.

## Recovery and disable

- Remove `--event-binding` from future starts (or delete the binding file):
  monitors fall back to pure `unattended`.
- Stop the delivery daemon and set `"enabled": false` (or remove the
  config): undelivered events simply stay pending in the outbox.
- `cleanup` removes only settled (delivered, or dead-letter with
  `--include-dead-letter`) outbox entries; terminal evidence is never
  touched.
- The deterministic monitoring core keeps working exactly as before with
  the bridge fully disabled.

## Schema compatibility

New schemas (`codex-monitor.event/v1`, `codex-monitor.delivery/v1`,
`codex-monitor.bridge-config/v1`, `codex-monitor.event-binding/v1`,
`codex-monitor.postflight/v1`, `codex-monitor.doctor/v1`,
`codex-monitor.list/v1`, bridge attempt records) are additive; see
`COMPATIBILITY.md` at the repository root for the full inventory and
migration behavior. Old terminal and manifest records remain readable and
are marked `evidence_strength: legacy` rather than rewritten.

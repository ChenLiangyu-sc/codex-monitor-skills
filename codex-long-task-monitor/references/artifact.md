# Artifact And Callback Backend

Use this backend when an asynchronous task writes a unique JSON result or callback artifact.

## Preconditions

Require a task-specific path, documented terminal field and values, identity fields when available, a finite monitoring deadline, and a main-agent acceptance plan. Prefer producer-side atomic rename. Existence-only success is allowed only when the producer contract explicitly makes appearance terminal.

The watcher rejects symlinks, non-regular files, changing reads, stale artifacts, oversized JSON, identity mismatches, overlapping success/failure values, and persistent malformed JSON.

## Detached monitor

For work longer than about two minutes, start the detached supervisor:

```bash
python3 <skill-dir>/scripts/supervise_artifact.py start /absolute/path/result.json \
  --json-field status \
  --success-json true \
  --failure-json false \
  --expect-json 'requestId="request-123"' \
  --require-nonempty data.notes.note \
  --max-json-bytes 8388608 \
  --poll-seconds 30 \
  --timeout-seconds 7200
```

The start command performs one launch attempt, returns after a bounded handshake, and prints an opaque `task_handle` plus local `run_dir`. State defaults to `~/.cache/codex-long-task-monitor` and the script rejects known network filesystems. Do not place supervisor authority or locks on NFS. The observed artifact may be on NFS; polling is deterministic and does not invoke a model.

For a Codex dispatch terminal, use the narrower wrapper instead of manually repeating the generic JSON contract:

```bash
python3 <skill-dir>/scripts/monitor_dispatch.py start \
  /absolute/private/dispatch-state/dispatches/<handle> \
  --timeout-seconds 1800
```

The wrapper reads only the controlled dispatch manifest, binds its handle, SHA, and dispatch-verifier identity in a private local record, configures every dispatch transport outcome, and returns a compact response without terminal or monitor-state paths. Read `monitor_dispatch.py status <monitor-task-handle>` once on a later genuine turn. Reserve its `wait` command for the notification-worker exception. On an observed terminal, both commands run the pinned dispatch supervisor's complete status verification; missing bindings, changed verifier/manifest identities, malformed terminals, or outcome disagreement fail closed with exit `12`.

Start exit codes:

- `0`: supervisor handshake verified;
- `2`: the same contract already has an active supervisor;
- `3`: a prior run exists and requires reviewed `--restart`;
- `4`: launch handshake not confirmed;
- `12`: invalid contract, contract conflict, or infrastructure failure.

`--restart` starts only a new read-only watcher generation with the exact same contract. It never restarts the underlying task. Review prior terminal/lost state before using it. The observation deadline is absolute and frozen at the first generation; a restart generation is capped to the remaining window and an expired window refuses to restart with exit `12`.

## Optional wake events (experimental, opt-in)

Start with `--event-binding <file>` to have each verified terminal record
publish one durable semantic event into the local outbox. The binding names
the exact Codex home, App Server instance, thread, and workspace that a
separate delivery daemon may resume; nothing is published without it, and a
supervisor crash between the terminal and the event is reconciled by any
later `status`/`wait` observation. See
[app-server-bridge.md](app-server-bridge.md) for configuration, delivery
(including the awaited `turn/completed`), failure behavior, and the
mandatory idempotent postflight claim protocol
(`postflight_guard.py begin`/`complete`). The `monitor_dispatch.py` wrapper
forwards `--event-binding`/`--bridge-config` and labels its events
`backend=dispatch`.

The binding is new-run intent, not an attach operation. Adding or changing it
on an already active unattended run fails as
`active_run_binding_conflict`. Audit the shared outbox and start the delivery
service first, then use a fresh artifact monitor for the initial closed-loop
test.

## Read local state

Read once without reopening the observed artifact:

```bash
python3 <skill-dir>/scripts/supervise_artifact.py status <task_handle>
```

Add `--require-terminal` to `status` for machine use. Its exit codes are `0` for verified condition satisfaction, `3` for a verified non-success terminal observation, `10` while active, `11` for supervisor loss, and `12` for missing or unverifiable evidence.

Only a notification worker authorized by the automatic-continuation exception may invoke `wait`, and it must pass `--notification-worker-ack`. The flag is an intent acknowledgement, not role authentication. `wait` returns `4` when its one bounded local wait expires without changing monitor or task state. The main agent must not invoke or loop over it.

Local states are `not_started`, `active`, `terminal`, `launch_unconfirmed`, `supervisor_lost`, `exit_observed_terminal_missing`, and `verification_failed`. Only a verified terminal with `observer_outcome=condition_satisfied` is observation success. It remains `scope=artifact_observation_only` and `business_verdict=pending`.

Raw watcher stdout/stderr stays in private `0600` run files. `terminal.json` contains fixed enums and integrity metadata, never artifact content or watcher output.

## Direct watcher

For a predictably short wait, `scripts/watch_artifact.py` remains available with the same contract options. Its exit codes are `0` condition satisfied, `3` terminal/contract failure, `4` timeout, and `5` invalid artifact. Direct watching is attached and should not be used for unattended long work.

For `ppt_notes_pipeline_server`, observe the request-specific callback result, validate top-level request identity and boolean status, and require the note field when appropriate. `/ready` is service liveness, not request completion. The main agent must still inspect item counts, failure manifests, placeholder notes, and note-quality gates.

---
name: codex-hpc-monitor
description: Monitor long-running Slurm jobs from Codex with a detached deterministic read-only supervisor and local terminal artifacts, avoiding repeated model-driven polling. Use when Codex needs to monitor submitted training or postflight jobs, survive tool-session or agent exit, recover a job's status cheaply, or report important Slurm terminal states. This Codex-only orchestration skill complements hpc-train and does not authorize submit, cancel, retry, file edits, project-gate decisions, or protected-data access.
---

# Codex HPC Monitor

Let the detached Python supervisor poll Slurm. Keep the main agent out of polling loops.

## Negotiate the mode first

Run once per environment; it reads local state only and never queries Slurm:

```bash
python3 <skill-dir>/scripts/supervise_slurm_job.py doctor --state-dir ~/.cache/codex-hpc-monitor
```

The doctor reports the negotiated mode with a reason. Default installations
select `unattended`; any probe failure falls back to `unattended` with a
safe reason code.

| Mode | Model turns while unchanged | Automatic Codex resume | Long-lived agent slot |
| --- | ---: | ---: | ---: |
| `unattended` (default) | 0 | No | No |
| `external-event-bridge` | 0 | Yes, when an explicitly configured App Server bridge is available | No |
| `goal-worker` | Runtime-dependent | Conditional compatibility mode only | One worker slot |

Auto-resume exists **only** through the explicitly configured event bridge
(experimental, disabled by default; see
[references/app-server-bridge.md](references/app-server-bridge.md)). Never
infer auto-resume merely because a terminal file, worker script, or Goal
exists. A terminal file cannot awaken an inactive Codex turn by itself.

When a monitor is started with `--event-binding`, each verified terminal
record additionally publishes one durable semantic event into the local
outbox; a foreground delivery daemon you run separately resumes the bound
thread, starts exactly one wake turn, and holds the App Server session
open until that turn completes. The woken turn must verify the terminal
digest, atomically claim the postflight with `scripts/postflight_guard.py
begin`, perform its side effects once, then `complete` the claim; an
unverified terminal record wakes the thread only as `contract_violation`.

## Apply the authority boundary

Read the companion `hpc-train` skill's `SKILL.md` (resolved from your skill installation, not a hard-coded path) completely before issuing any HPC command. Read its debugging reference only after a failure, anomalous state, or prolonged pending result.

Use this skill only after a job has already been submitted under separate authority. Perform only read-only `squeue` and `sacct` queries. Never submit, retry, cancel, reprioritize, edit files, inspect training outputs, or access protected content through this skill.

Treat all watcher results as Slurm-only evidence. `COMPLETED / 0:0` does not establish project or model success; run the project-specific postflight separately.

## Choose the mode

- Query once directly only for a one-shot check or work expected to finish within about two minutes.
- For longer work, start or reuse one detached supervisor, verify its handshake once, and end the Codex turn while the job remains active.
- When the user returns or another genuine semantic event activates a later turn, read local `status` once.
- Never claim that `terminal.json` or another local artifact wakes an inactive Codex turn.

For work expected to exceed about two minutes, never let the main agent run `bridge_slurm_terminal.py run`, call `write_stdin` repeatedly, repeat `status`, `squeue`, `sacct`, or log reads, emit heartbeat commentary, or enter any periodic wait loop.

## Start one detached monitor

Run:

```bash
python3 <skill-dir>/scripts/supervise_slurm_job.py start <job-id> \
  --host hpc142 --poll-seconds 60 --pending-alert-seconds 0 \
  --terminal-observability-seconds 300 --max-watch-seconds 604800 \
  --expected-owner <owner> --expected-job-name <name> \
  --expected-partition <partition>
```

Include every known identity constraint. Omit unknown optional fields; never guess them. The launcher returns after a bounded handshake while the supervisor and watcher continue independently.

The monitoring contract (identity constraints and timing parameters) is
frozen with a digest in the manifest; restarting with a changed contract
fails with `contract_conflict` unless you explicitly pass
`--allow-contract-change`. The observation deadline is absolute and cannot
be extended by restarting. The watcher binds the scheduler identity it
observes (submit time, and cluster/SLUID where available) and fails closed
on job-id reuse or requeue identity conflicts.

Do not add `--notify-running`: one supervisor invocation follows the job until an important stopping event. The existing watcher lock and the supervisor lock ensure one query stream per host/job.

Keep `--pending-alert-seconds 0` for unattended monitoring so a long queue wait does not end observation before the job runs. Set a positive threshold only when a pending alert is intentionally terminal and a human will read it promptly.

Interpret `start` exit codes as:

- `0`: detached supervisor started and handshake observed.
- `2`: an active supervisor already owns this host/job; reuse it.
- `3`: a prior run exists; inspect it before an explicit `--restart`.
- `4`: launch handshake was not confirmed; inspect local status and ownership.
- `12`: launcher or supervisor infrastructure failure.

Use `--restart` only after reviewing a prior terminal or lost supervisor. This creates a new immutable run directory and preserves the old evidence.

## Read status without querying Slurm

Run once:

```bash
python3 <skill-dir>/scripts/supervise_slurm_job.py status <job-id> --host hpc142
```

Add `--require-terminal` for machine use; it returns `3` while the run is nonterminal. This command reads local JSON only and must not poll, wait, or open SSH.

## Continue active Goals automatically (conditional compatibility mode)

The durable automatic-resume path is the explicitly configured event bridge
above. The Goal/notification-worker path below remains available only as a
conditional compatibility mode when the runtime provides an eligible worker;
it is not the preferred durable path, and unattended monitoring is always
correct when no worker is proven. Never emulate notification with
main-agent polling.

For an active Goal with a proven eligible worker, Goal activation itself
supplies the continuation request, so do not ask the user to repeat it.
Also allow this path for a non-Goal task when the user explicitly requests
same-turn automatic continuation. Use unattended monitoring when the user
opts out, pauses the Goal, or no suitable notification worker exists.

Create at most one bounded low-cost notification worker after verifying the detached supervisor handshake. Give it exactly one operation: run the following deterministic local bridge wait once and publish one fixed-schema semantic event. Do not give it backend monitoring, logs, raw output, retries, cancellation, project checks, or acceptance work.

Only the notification worker may run:

```bash
python3 <skill-dir>/scripts/bridge_slurm_terminal.py run <job-id> \
  --host hpc142 --timeout-seconds 28800 --poll-seconds 1 \
  --notification-worker-ack
```

The acknowledgement is a deterministic misuse guard and records caller intent; it does not authenticate or cryptographically identify a model role. The wrapper holds one per-run lease, invokes local supervisor `wait` exactly once, and writes one immutable receipt. It returns `0` for verified scheduler success, `3` for a verified non-success terminal, `4` for bridge timeout, `11` for supervisor loss, and `12` for missing or unverifiable evidence. Exit `2` means another bridge owns the lease; exit `3` with `run_result=receipt_exists` means a receipt already exists. A bridge wait timeout (`4`) records one attempt record instead of a receipt: it never permanently blocks a later run from delivering the genuine verified terminal event.

Require the worker to publish exactly one mailbox message with this schema and no other commentary:

```text
monitor_event/v1 handle=<opaque> generation=<run-id>
event=<transport_success|transport_failure|deadline_exceeded|lost_observability|contract_violation>
exit_code=<integer> terminal_digest=<sha256-or-null> business_verdict=pending
```

Let the main agent make at most one long `wait_agent` call per Goal activation for that worker. If it times out before the semantic event, end the current activation while preserving the Goal, detached monitor, and existing bridge. Do not call `wait_agent` again in that activation, poll the worker, read monitor state, emit a heartbeat, or create a second bridge.

On a later genuine turn, recover bridge state without querying Slurm:

```bash
python3 <skill-dir>/scripts/bridge_slurm_terminal.py status <job-id> --host hpc142
```

Interpret `active` as leased custody, `terminal` as an immutable receipt, `bridge_lost` as fail-closed local notification loss, and `monitor_unavailable` as missing monitor authority. A bridge receipt remains Slurm-only evidence and never establishes project success.

Use `watcher_state.payload.snapshot` for the last verified local Slurm state while monitoring remains active. Query Slurm directly only when that snapshot is missing, stale, or unverified.

Interpret `state` as:

- `not_started`: no supervisor run is recorded.
- `active`: the supervisor PID and Linux process start ticks match.
- `terminal`: read the embedded verified terminal record.
- `launch_unconfirmed`: no supervisor handshake exists.
- `supervisor_lost`: the recorded supervisor identity is no longer live; no Slurm terminal result is established.
- `exit_observed_terminal_missing`: the watcher exit was observed but terminal publication is incomplete; fail closed.

Runs are stored under:

```text
~/.cache/codex-hpc-monitor/supervisors/<host>-<job-id>/runs/<run-id>/
```

Important artifacts are `manifest.json`, `supervisor_started.json`, `runtime.json`, `child_exit.json`, and atomically published `terminal.json`. The watcher retains its existing state and result files at:

```text
~/.cache/codex-hpc-monitor/<host>-<job-id>.state.json
~/.cache/codex-hpc-monitor/<host>-<job-id>.result.json
```

Never infer success from a missing terminal. If ownership is uncertain, run:

```bash
python3 <skill-dir>/scripts/inspect_watcher_owner.py <job-id> --host hpc142
```

Treat `active_verified` as an existing watcher, `inactive` as no proven watcher, and `inconsistent` as fail-closed. Never kill a PID or start a replacement from a stale payload alone.

## Interpret terminal results

The terminal separates observer outcome from the watcher result. Require `watcher_result.verified=true` before relaying a Slurm event. Verification binds freshness, Job ID, scope, project-gate flag, watcher exit code, event type, and explicit success evidence. Preserve `scope=slurm_only` and `project_gate_evaluated=false`.

Watcher exit codes are control outcomes, not project verdicts:

- `0`: explicit `COMPLETED / ExitCode=0:0`
- `3`: scheduler terminal failure or nonzero completed exit
- `4`: pending threshold reached
- `5`: repeated SSH/Slurm query failure
- `7`: requeue or anomalous state
- `8`: Slurm terminal observability lost
- `9`: expected identity mismatch
- `10`: total watcher timeout
- `11`: duplicate watcher
- `12`: watcher infrastructure failure

Report a terminal event in this shape:

```text
Job <id>: Slurm <state>, ExitCode=<code>, elapsed=<elapsed>, owner=<owner>,
name=<name>, partition=<partition>, classification=<slurm_classification>.
Project gate not evaluated; main-agent verification required.
```

After any terminal result, independently verify the exact Slurm state and required project postflight before authorizing another action.

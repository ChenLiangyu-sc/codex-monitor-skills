---
name: codex-long-task-monitor
description: Monitor long-running commands, asynchronous artifacts, callbacks, dispatched Codex work, and Slurm jobs with detached deterministic supervisors and local terminal records, keeping unchanged-state monitoring at zero model turns. Use when Codex must observe machine-verifiable work for several minutes or longer, recover status after an agent/tool session ends, or separate execution completion from business acceptance. This skill does not authorize starting, retrying, cancelling, mutating, or approving the underlying task, and it must not inspect protected content or make subjective acceptance decisions.
---

# Codex Long Task Monitor

Make deterministic software own observation. Use a model only to freeze the contract, handle a meaningful event, and perform business acceptance.

## Hard invariants

- Unchanged task state produces zero model turns.
- A replaceable watcher is never the sole owner of an execution exit status.
- Timeout and lost observability are alerts, not task terminal states and never retry authority.
- Transport completion, artifact delivery, and business acceptance remain separate.
- PID disappearance, silence, HTTP 200, file existence, and Slurm `COMPLETED` do not prove business success.
- Raw stdout, stderr, logs, callbacks, prompts, responses, and artifacts never enter mailbox messages.

## Freeze the contract

Record before monitoring:

1. Stable authority identity and generation, distinct from a temporary tool/session handle.
2. Exact machine success and failure conditions.
3. Monitoring deadline and escalation policy.
4. Allowed metadata and evidence paths.
5. Delivery barrier, when output completeness matters.
6. Main-agent checks required for business acceptance.

Fail closed when only a PID, log path, ambiguous job name, ordinary file path, or health endpoint is available and no documented terminal authority exists. Label such observation `non_authoritative`; never infer success.

Freeze which process owns the actual workload. A Codex dispatch terminal covers only the Codex child. If that child submits work to Slurm, systemd, or another durable runner, treat the persisted backend identity and runner terminal as a separate authority; never use the dispatch terminal or a stale `write_stdin` handle as its completion signal.

## Route to one backend

- **Slurm:** Read and use `/home/liangyu.chen02/.codex/skills/codex-hpc-monitor/SKILL.md`. Its detached supervisor owns scheduler reconciliation.
- **Codex dispatch:** Read and use `/home/liangyu.chen02/.codex/skills/codex-task-dispatch/SKILL.md`. Its direct-parent supervisor owns child exit and `dispatch_terminal.json`. Start observation with `scripts/monitor_dispatch.py start <dispatch-directory> --timeout-seconds <seconds>` so the fixed contract binds the handle, manifest SHA, and full dispatch verifier identity; retain the returned monitor task handle for `status` or `wait`. The wrapper reports a terminal outcome only after the dispatch supervisor verifies the complete terminal contract.
- **Artifact or callback:** Read [references/artifact.md](references/artifact.md). For work longer than about two minutes, use `scripts/supervise_artifact.py`; it detaches, freezes the watcher, and publishes an immutable local terminal.
- **Short synchronous command:** Read [references/process.md](references/process.md). The attached path is lifecycle-dependent and is only for predictably short work.
- **HTTP or queue:** Prefer a documented immutable callback/result record. Provider polling must be deterministic and read-only; readiness is not task completion.

Use one backend and one effective monitor per stable identity. A changed contract is a conflict, not an implicit second watcher.

For a custody-transfer workflow, this means one dispatch artifact monitor for the bounded submission child and one backend-specific monitor for the durable workload. They are different stable identities, not duplicate watchers. The main agent reconciles both before business acceptance.

## Prefer unattended monitoring

For work expected to exceed about two minutes, start or reuse one detached supervisor, verify its handshake once, retain its opaque handle, and end the Codex turn while work remains active. Read `status` once only when the user returns or another genuine semantic event activates a later turn. A terminal file cannot awaken an inactive Codex turn; never claim background notification.

Never let the main agent run a foreground bridge or local monitor `wait`, repeatedly call `write_stdin`, monitor `status`, query the backend, read logs, emit heartbeat commentary, or enter a periodic model-driven wait loop.

For an active Goal, use automatic continuation by default; Goal activation itself supplies the continuation request, so do not ask the user to repeat it. Also use it for a non-Goal task when the user explicitly requests same-turn continuation. After verifying the detached supervisor, let `codex-task-routing` create at most one bounded Luna/low notification worker. Let it run exactly one backend-provided deterministic local bridge wait with `--notification-worker-ack` and publish exactly one fixed-schema semantic event. The acknowledgement records intent and discourages accidental main-agent use; it does not authenticate a model role.

Let the main agent make at most one long `wait_agent` call per Goal activation. If that call times out before the event, end the current activation while preserving the Goal, detached monitor, and existing bridge. Do not start another wait in that activation, enter a polling loop, or create a second bridge. Use unattended monitoring when the user opts out, pauses the Goal, or no suitable notification worker exists.

Never let the worker poll the backend, read raw output or logs, own execution, retry, cancel, inspect project results, or perform business acceptance. Never duplicate a verified active deterministic supervisor.

## Notify only on semantic events

Allowed notifications are:

- acceptance-ready transport plus delivery state;
- terminal transport failure;
- one deadline-exceeded alert;
- lost observability or contract violation.

Use only opaque handle, generation, fixed enums, exit/status codes, elapsed time, and terminal digest. Exclude heartbeat timestamps, repeated native reasons, raw messages, paths containing protected identity, and all content.

Publish exactly one event in this shape:

```text
monitor_event/v1 handle=<opaque> generation=<opaque>
event=<transport_success|transport_failure|deadline_exceeded|lost_observability|contract_violation>
exit_code=<integer> terminal_digest=<sha256-or-null> business_verdict=pending
```

## Verify after observation

The deterministic monitor publishes observation evidence only. After a successful observation, the main agent independently verifies expected artifacts, counts, hashes, hard gates, and the requested business result. Do not overwrite transport evidence with a business verdict.

If a tool handle becomes unknown while no durable backend authority or runner terminal exists, report lost observability. The error does not prove that the business process stopped or continued and never authorizes retry.

Keep task launch, retry, cancellation, destructive actions, downstream authorization, and result acceptance outside this skill.

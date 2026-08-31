# Compatibility Inventory And Schema Notes

Status: WP0 baseline record for the safe auto-resume implementation.
Baseline before changes: 100 tests passing (55 `codex-hpc-monitor`,
45 `codex-long-task-monitor`), Python 3.10, standard library only.

## 1. Recorded schemas (before this change set)

### codex-hpc-monitor

| Record | Schema string | Where |
| --- | --- | --- |
| Supervisor manifest | `codex-hpc-monitor.manifest/v1` | `runs/<run-id>/manifest.json` |
| Current pointer | `codex-hpc-monitor.current/v1` | `supervisors/<host>-<job>/current.json` |
| Supervisor started | `codex-hpc-monitor.supervisor-started/v1` | run dir |
| Watcher runtime | `codex-hpc-monitor.runtime/v1` | run dir |
| Child exit | `codex-hpc-monitor.child-exit/v1` | run dir |
| Terminal | `codex-hpc-monitor.terminal/v1` | run dir (immutable, publish-once) |
| Status output | `codex-hpc-monitor.status/v1` | stdout |
| Wait output | `codex-hpc-monitor.wait/v1` | stdout |
| Supervisor failure | `codex-hpc-monitor.supervisor-failure/v1` | run dir |
| Bridge manifest | `codex-hpc-monitor.bridge.manifest/v1` | `bridges/<host>-<job>/<run>/manifest.json` |
| Bridge runtime | `codex-hpc-monitor.bridge.runtime/v1` | bridge dir |
| Bridge receipt | `codex-hpc-monitor.bridge.receipt/v1` | bridge dir (immutable) |
| Bridge status | `codex-hpc-monitor.bridge.status/v1` | stdout |
| Bridge error | `codex-monitor.bridge.error/v1` alias `codex-hpc-monitor.bridge.error/v1` | stdout |
| Watcher state | integer `schema_version: 1` | `<state-dir>/<host>-<job>.state.json` |
| Watcher result | no schema field (fixed keys) | `<state-dir>/<host>-<job>.result.json` |

### codex-long-task-monitor

| Record | Schema string | Where |
| --- | --- | --- |
| Artifact manifest | `codex-long-task-monitor.artifact.manifest/v1` | `artifacts/<handle>/runs/<run-id>/manifest.json` |
| Current pointer | `codex-long-task-monitor.artifact.current/v1` | `artifacts/<handle>/current.json` |
| Supervisor started / runtime / child-exit / terminal / wait / status / supervisor-failure | `codex-long-task-monitor.artifact.<name>/v1` | run dir / stdout |
| Dispatch wrapper output | `codex-long-task-monitor.dispatch-wrapper/v1` | stdout |
| Dispatch binding | `codex-long-task-monitor.dispatch-binding/v1` | `dispatch-wrapper-bindings/<handle>.json` |
| External dispatch manifest | `codex-task-dispatch.manifest/v3` | external dispatch directory |
| External dispatch terminal | `codex-task-dispatch.terminal/v3` | external dispatch directory |

## 2. New schemas added by this change set

| Schema | Introduced by | Purpose |
| --- | --- | --- |
| `codex-monitor.event/v1` | WP3 | Immutable semantic event published at verified terminal records |
| `codex-monitor.delivery/v1` | WP3 | Mutable outbox delivery metadata (state, lease, attempts, backoff) |
| `codex-monitor.bridge-config/v1` | WP1/WP4 | Explicit, opt-in bridge configuration file |
| `codex-monitor.event-binding/v1` | WP3 | Per-monitor binding (Codex home, instance, thread, workspace) |
| `codex-monitor.bridge-activation/v1` | Activation hardening | Private durable first-activation receipt, scoped to instance, Codex home, and workspace, with the exact accepted pre-cutover event IDs |
| `codex-monitor.postflight/v1` | WP5 | Idempotency marker for processed wake events (extended in review round 2 with an atomic begin/complete claim: `state`, `owner`, `started_at`, `completed_at`; legacy markers without `state` read as completed) |
| `codex-monitor.doctor/v1` | WP1 | Capability/mode probe output |
| `codex-monitor.list/v1` | WP6 | Monitor enumeration output |
| `codex-monitor.attempt/v1` | WP2 | Bridge wait attempt record (timeout is an attempt, not a terminal receipt) |
| `<skill>.event-intent/v1` | WP2 review | Run-dir record written at start when a wake binding exists; status/wait reconcile the crash window between terminal and event publication |
| `<skill>.semantic-event/v1` | WP3 | Run-dir record of the event publication outcome |
| `codex-monitor.sink-receipt/v1` | Operations hardening | Independent at-least-once receipt bound to sink ID, mode, and destination digest; does not alter wake delivery |
| `codex-monitor.app-server-lifecycle-smoke/v1` | Lifecycle diagnostics hardening | Private local receipt proving a confirmed two-connection initialize/thread-start/turn-completed/reinitialize/thread-resume/turn-completed smoke, bound to config and executable hash |

Operations hardening also adds output-only schemas under
`codex-monitor.bridge-service.*`, `codex-monitor.bridge.protocol-check/v1`,
`codex-monitor.bridge.activation-check/v1`, and `codex-monitor.events.*`.
They are command responses rather than monitor
authority. Dead-letter retry is an explicit human-confirmed transition on the
existing `codex-monitor.delivery/v1`; it never mutates `event.json`.

Review round 2 contract changes (all additive, validated strictly):

- `codex-monitor.bridge-config/v1` gained required `codex_home` (absolute
  path whose digest must equal `codex_home_id`) and
  `turn_completion_timeout_seconds`; `lease_seconds` must be at least twice
  `request_timeout_seconds`. Configs from round 1 without these fields are
  rejected fail-closed — regenerate with `init-config`.
- `codex-monitor.delivery/v1` gained `turn_status`, recorded together with
  the turn id at acknowledgement; acknowledgement now requires it.
- Delivery acknowledges only after `turn/completed`; sessions stay open
  for the whole wake turn and renew the lease throughout.
- `thread/resume` must return the bound thread id **and** a `cwd` equal to
  the bound workspace; missing or wrong `cwd` dead-letters.

Review round 3 behavior hardening (no schema-string changes):

- New bridge configs store an absolute transport executable. Existing
  `codex-monitor.bridge-config/v1` files with a bare or relative `codex` command
  remain readable; service install/repair freezes both the original token and
  resolved executable, writes an explicit service `PATH`, and delivery rejects
  token or executable drift.
  The schema field set is unchanged.
- `postflight_guard.py` CLI `begin`/`mark` accept either raw 64-character
  lowercase hex or `sha256:`-prefixed terminal digests and normalize to the
  prefixed form before calling `semantic_events.py`. Direct semantic-event
  APIs remain prefix-strict.

- `turn/completed` acknowledges only an exact target turn with
  `status=completed`; official `failed` and `interrupted` statuses retry,
  while malformed completion shapes fail closed.
- delivery preserves notifications received before a request response,
  renews before every blocking-read deadline (including sub-second leases),
  and stops a stale owner immediately after lease loss. Config validation
  rejects request timeouts below 50 ms and leases below 100 ms, where OS
  scheduling cannot uphold the renewal contract reliably.
- postflight reset now shares the begin/complete lock and can clear only an
  `in_progress` marker, never completed evidence.
- HPC terminal envelopes and artifact terminals are verified before crash
  reconciliation can publish an event; HPC lock files reject symlinks.
- transient terminal-to-outbox publication failures remain eligible for a
  later idempotent `status`/`wait` repair.

Operations hardening behavior:

- generated App Server schemas are checked for the minimal method and record
  shapes; exact versions with a recorded real lifecycle smoke are reported
  separately from merely schema-compatible versions;
- any server-initiated request during a wake turn fails closed as
  `operator_interaction_required`; the bridge never replies or records its
  parameters;
- systemd/LaunchAgent installation remains explicit and recoverable, with a
  required unique service name and overwrite/uninstall requiring separate
  confirmation; disabled service-mode delivery exits cleanly;
- notification sinks have independent receipts and cannot acknowledge or
  claim App Server delivery.

Activation hardening behavior (additive):

- bridge activation audits only the events matching the configured App Server
  instance, Codex home, and workspace; unreadable entries fail closed;
- explicit activation linearizes a durable receipt against publishers under
  the outbox lock and requires exact acknowledgement of every pre-cutover
  matching pending/leased event; foreground delivery, managed starts, and
  manager auto-restarts all verify that receipt;
- manifests created with a binding include an optional
  `event_binding_digest`; attempts to retrofit a different binding onto an
  active run return `active_run_binding_conflict` without changing the run;
- existing manifests without `event_binding_digest` remain readable.
- `start --event-binding` without `--bridge-config` remains accepted for
  compatibility but adds a structured `warnings` field and a prominent stderr
  warning; the additive `--require-auto-resume` option fails before launch
  unless binding, enabled matching config, and activation receipt are ready.

App Server lifecycle diagnostics hardening (additive, 2026-08-31):

- `--require-auto-resume` now also requires the configured direct
  `codex app-server` executable to report an exact version with a recorded real
  lifecycle smoke. Codex CLI 0.149.1 is rejected after a real output-closure
  failure; 0.150.1 and 0.151.0 are recorded. Schema generation alone is not a
  lifecycle attestation. Strict readiness additionally requires a matching
  local `codex-monitor.app-server-lifecycle-smoke/v1` receipt bound to the
  absolute executable SHA-256, full bridge config digest, Codex home, and
  workspace; missing or stale receipts fail before watcher launch. Delivery
  independently revalidates the receipt at daemon startup and after each
  event claim before starting a wake turn, releasing the claim on drift.
- New `codex-monitor.delivery/v1` writers extend `last_error` with nullable
  `stage`, `app_server_exit_code`, and a bounded redacted `stderr_tail`.
  Readers accept both the original two-field `last_error` and this additive
  diagnostic shape, so settled legacy outbox entries remain readable.

Capability and version evidence must not be collapsed into one boolean:

| Codex CLI | Schema/protocol | Fake client lifecycle | Real binary transport | Strict readiness |
| --- | --- | --- | --- | --- |
| 0.149.1 | compatible fixture | passed | observed output closure | rejected |
| 0.150.1 | compatible | passed | recorded 2026-08-29 | matching local receipt required |
| 0.151.0 | compatible | passed | recorded 2026-08-31 | matching local receipt required |
| unrecorded | probe result only | client test remains version-independent | unverified | rejected |

None of these rows claims a real Slurm terminal-to-postflight business
acceptance loop. That is a separate deployment-level release gate.

## 3. Migration behavior

- Every pre-existing on-disk schema keeps its schema string. New fields are
  added as optional members only; readers treat missing new fields as
  "legacy" evidence and never fail merely because a new field is absent.
- Old terminal records (for example an HPC terminal without
  `contract_digest`, or a watcher state file without `deadline_at_epoch` or
  `identity_binding`) remain readable. Verification marks their evidence
  strength as `legacy` instead of `full`; it never fabricates the new fields.
- Schema upgrades fail closed when a required security field (event binding,
  digests) is present but malformed: the record is rejected, not guessed.
- The watcher result file, watcher lock file, and bridge manifest/receipt
  formats from the initial release are unchanged.

## 4. Callers of changed functions

| Changed module | Existing callers that must keep working |
| --- | --- |
| `supervise_slurm_job.py` | `bridge_slurm_terminal.py` (spawns `start` never; spawns `status` and `wait`), both test suites, SKILL.md command examples |
| `watch_slurm_job.py` | `supervise_slurm_job.py` (spawns via `watcher_argv`), `inspect_watcher_owner.py` (matches the literal file name `watch_slurm_job.py` in `/proc/<pid>/cmdline`), unit tests calling `monitor()`/`SlurmClient.query()` |
| `supervise_artifact.py` | `monitor_dispatch.py` (subprocess `start`/`status`/`wait` with fixed argument shapes), test suite, frozen per-run watcher copies `watch_artifact_frozen.py` |
| `monitor_dispatch.py` | `codex-long-task-monitor/SKILL.md` and `references/artifact.md` examples, test suite with a pinned fake dispatch supervisor |
| `bridge_slurm_terminal.py` | `codex-hpc-monitor/SKILL.md` worker protocol, test suite with a fake supervisor |

Compatibility rules for this change set:

- All existing CLI subcommands, flags, exit codes, and stdout JSON shapes stay
  backward compatible; new behavior is additive or guarded by new flags.
- `monitor()` and `query()` keep keyword defaults so existing in-process tests
  compile and pass unchanged.
- `inspect_watcher_owner.py` keeps matching watchers by script file name, so
  the watcher entry point file name must stay `watch_slurm_job.py`.

## 5. Companion-skill references (review round 2 cleanup)

- `codex-hpc-monitor/SKILL.md` previously pointed at an author-absolute
  `hpc-train` path; both skills now reference companion skills by name and
  instruct resolving them from the local skill installation. No
  author-absolute paths remain in the public skills.
- `monitor_dispatch.py` keeps a home-relative default dispatch supervisor
  (`~/.codex/skills/codex-task-dispatch/scripts/dispatch_supervisor.py`),
  overridable with `--dispatch-supervisor-path`.
- Default SSH host alias `hpc142` remains in help text and examples only.

None of these are runtime dependencies of the deterministic core.

## 6. Vendor policy for shared code

`codex-hpc-monitor` and `codex-long-task-monitor` must remain independently
installable, so shared runtime code (`semantic_events.py`,
`app_server_bridge.py`, `postflight_guard.py`) is vendored as byte-identical
copies inside each skill's `scripts/` directory. A synchronization test in
each skill verifies the copies are identical whenever both skill directories
are present and skips (not fails) when a skill is installed alone.

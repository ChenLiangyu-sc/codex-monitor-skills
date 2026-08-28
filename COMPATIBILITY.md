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
| `codex-monitor.postflight/v1` | WP5 | Idempotency marker for processed wake events |
| `codex-monitor.doctor/v1` | WP1 | Capability/mode probe output |
| `codex-monitor.list/v1` | WP6 | Monitor enumeration output |
| `codex-monitor.attempt/v1` | WP2 | Bridge wait attempt record (timeout is an attempt, not a terminal receipt) |

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

## 5. Hard-coded companion paths (unchanged, documented)

- `codex-hpc-monitor/SKILL.md`: `/share/cv/data/liangyu.chen/skills/hpc-train/SKILL.md`.
- `codex-long-task-monitor/SKILL.md`:
  `/home/liangyu.chen02/.codex/skills/codex-hpc-monitor/SKILL.md` and
  `codex-task-dispatch` references.
- `monitor_dispatch.py`: default dispatch supervisor
  `~/.codex/skills/codex-task-dispatch/scripts/dispatch_supervisor.py`.
- Default SSH host alias `hpc142` in help text and examples.

None of these are runtime dependencies of the deterministic core; they are
author-environment integration points kept for workflow compatibility.

## 6. Vendor policy for shared code

`codex-hpc-monitor` and `codex-long-task-monitor` must remain independently
installable, so shared runtime code (`semantic_events.py`,
`app_server_bridge.py`, `postflight_guard.py`) is vendored as byte-identical
copies inside each skill's `scripts/` directory. A synchronization test in
each skill verifies the copies are identical whenever both skill directories
are present and skips (not fails) when a skill is installed alone.

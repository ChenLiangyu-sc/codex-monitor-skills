#!/usr/bin/env python3
"""Detach, supervise, and read local status for the Slurm watcher."""

from __future__ import annotations

import argparse
import ctypes
import fcntl
import hashlib
import json
import os
import re
import secrets
import signal
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
import semantic_events


JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")
HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SCHEMA_PREFIX = "codex-hpc-monitor"
# Watcher exit code -> semantic wake event. Verified scheduler outcomes only.
# Exit 4 (pending threshold alert) deliberately maps to nothing: a queue-wait
# alert is not a monitoring-deadline expiry and must not wake a thread with
# a misleading deadline_exceeded event. Duplicate-watcher, infrastructure
# failures, signals, and launch failures also never publish.
HPC_SEMANTIC_EVENTS = {
    0: "transport_success",
    3: "transport_failure",
    5: "lost_observability",
    7: "contract_violation",
    8: "lost_observability",
    9: "contract_violation",
    10: "deadline_exceeded",
}
WATCHER_EXIT_EVENTS = {
    0: {"completed"},
    3: {"terminal_failure"},
    4: {"pending_alert"},
    5: {"query_error"},
    6: {"running"},
    7: {"anomalous_state"},
    8: {"lost_observability"},
    9: {"identity_mismatch"},
    10: {"watch_timeout"},
    11: {"duplicate_watcher"},
    12: {"dependency_error", "watcher_error"},
}
WATCHER_EXIT_CLASSIFICATIONS = {
    0: {"scheduler_success"},
    3: {"scheduler_terminal_failure"},
    4: {"scheduler_pending_alert"},
    5: {"scheduler_query_failure"},
    6: {"scheduler_active"},
    7: {"scheduler_anomalous_state"},
    8: {"scheduler_observability_failure"},
    9: {"scheduler_identity_mismatch"},
    10: {"scheduler_watch_timeout"},
    11: {"watcher_duplicate"},
    12: {"watcher_infrastructure_failure"},
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(payload: object) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def fsync_directory(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"unsafe state directory: {path}")
    os.chmod(path, 0o700)


def write_temp(path: Path, payload: object) -> Path:
    ensure_private_directory(path.parent)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    fd = os.open(str(temp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        data = canonical_json(payload)
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    return temp


def replace_json(path: Path, payload: object) -> None:
    temp = write_temp(path, payload)
    try:
        os.replace(str(temp), str(path))
        os.chmod(path, 0o600)
        fsync_directory(path.parent)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def publish_json_no_replace(path: Path, payload: object) -> None:
    temp = write_temp(path, payload)
    try:
        os.link(str(temp), str(path), follow_symlinks=False)
        os.chmod(path, 0o600)
        fsync_directory(path.parent)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def read_json(path: Path) -> Dict[str, Any]:
    try:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def process_start_ticks(pid: int) -> Optional[str]:
    try:
        value = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        return value[value.rfind(")") + 2 :].split()[19]
    except (OSError, IndexError, ValueError):
        return None


def process_matches(pid: object, ticks: object) -> bool:
    return (
        isinstance(pid, int)
        and pid > 1
        and isinstance(ticks, str)
        and bool(ticks)
        and process_start_ticks(pid) == ticks
    )


def base_dir(state_dir: Path, host: str, job_id: str) -> Path:
    return state_dir / "supervisors" / f"{host}-{job_id}"


def current_run(base: Path) -> Optional[Path]:
    current = read_json(base / "current.json")
    run_id = current.get("run_id")
    if not isinstance(run_id, str) or not re.fullmatch(r"run_[A-Za-z0-9_-]+", run_id):
        return None
    run = base / "runs" / run_id
    try:
        run.relative_to(base / "runs")
    except ValueError:
        return None
    return run


def verified_local_state(run: Path, host: str, job_id: str) -> Dict[str, Any]:
    manifest_path = run / "manifest.json"
    manifest = read_json(manifest_path)
    started = read_json(run / "supervisor_started.json")
    expected_manifest_sha = started.get("manifest_sha256")
    if (
        not manifest
        or manifest.get("run_id") != run.name
        or manifest.get("host") != host
        or manifest.get("job_id") != job_id
        or not isinstance(expected_manifest_sha, str)
        or sha256_file(manifest_path) != expected_manifest_sha
    ):
        return {"present": False, "verified": False, "reason": "manifest_unverified"}

    state_dir = Path(str(manifest.get("state_dir", "")))
    path = state_dir / f"{host}-{job_id}.state.json"
    payload = read_json(path)
    if not payload:
        return {"present": False, "verified": False, "path": str(path)}

    problems = []
    if payload.get("schema_version") != 1:
        problems.append("schema_mismatch")
    if payload.get("host") != host:
        problems.append("host_mismatch")
    snapshot = payload.get("snapshot")
    if isinstance(snapshot, dict) and snapshot.get("job_id") != job_id:
        problems.append("job_id_mismatch")
    try:
        if path.stat().st_mtime_ns < manifest_path.stat().st_mtime_ns:
            problems.append("stale_state")
    except OSError:
        problems.append("state_stat_failed")
    return {
        "present": True,
        "verified": not problems,
        "path": str(path),
        "sha256": sha256_file(path) if not problems else None,
        "problems": problems,
        "payload": payload if not problems else None,
    }


def run_status(run: Optional[Path], host: str, job_id: str) -> Dict[str, Any]:
    if run is None:
        return {
            "schema_version": f"{SCHEMA_PREFIX}.status/v1",
            "state": "not_started",
            "host": host,
            "job_id": job_id,
        }

    watcher_state = verified_local_state(run, host, job_id)
    terminal = read_json(run / "terminal.json")
    if terminal:
        candidate = {
            "schema_version": f"{SCHEMA_PREFIX}.status/v1",
            "state": "terminal",
            "host": host,
            "job_id": job_id,
            "run_id": run.name,
            "run_dir": str(run),
            "evidence_strength": (
                "full"
                if isinstance(terminal.get("contract_digest"), str)
                else "legacy"
            ),
            "terminal": terminal,
            "watcher_state": watcher_state,
        }
        _code, verification = terminal_wait_result(candidate)
        candidate["terminal_verified"] = verification["terminal_verified"]
        if not verification["terminal_verified"]:
            candidate["problems"] = verification.get("problems", [])
        return candidate

    started = read_json(run / "supervisor_started.json")
    runtime = read_json(run / "runtime.json")
    child_exit = read_json(run / "child_exit.json")
    supervisor_alive = process_matches(started.get("pid"), started.get("pid_start_ticks"))
    watcher_alive = process_matches(runtime.get("pid"), runtime.get("pid_start_ticks"))
    if supervisor_alive:
        state = "active"
    elif child_exit:
        state = "exit_observed_terminal_missing"
    elif started:
        state = "supervisor_lost"
    else:
        state = "launch_unconfirmed"
    return {
        "schema_version": f"{SCHEMA_PREFIX}.status/v1",
        "state": state,
        "host": host,
        "job_id": job_id,
        "run_id": run.name,
        "run_dir": str(run),
        "supervisor_alive": supervisor_alive,
        "watcher_alive": watcher_alive,
        "supervisor": started or None,
        "runtime": runtime or None,
        "child_exit": child_exit or None,
        "watcher_state": watcher_state,
    }


def open_lifetime_lock(base: Path) -> int:
    ensure_private_directory(base)
    path = base / "monitor.lock"
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(str(path), flags, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("monitor lock is not a regular file")
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        os.close(fd)
        raise
    return fd


def watcher_command(args: argparse.Namespace) -> List[str]:
    command = [
        sys.executable,
        str(args.watcher_path.resolve()),
        args.job_id,
        "--host",
        args.host,
        "--poll-seconds",
        str(args.poll_seconds),
        "--pending-alert-seconds",
        str(args.pending_alert_seconds),
        "--terminal-observability-seconds",
        str(args.terminal_observability_seconds),
        "--max-watch-seconds",
        str(args.max_watch_seconds),
        "--query-failures",
        str(args.query_failures),
        "--state-dir",
        str(args.state_dir.resolve()),
    ]
    for option, value in (
        ("--expected-owner", args.expected_owner),
        ("--expected-job-name", args.expected_job_name),
        ("--expected-partition", args.expected_partition),
    ):
        if value is not None:
            command.extend([option, value])
    return command


def validate_identity(job_id: str, host: str) -> None:
    if not JOB_ID_RE.fullmatch(job_id):
        raise ValueError("job_id must be numeric, optionally followed by _<array-index>")
    if not HOST_RE.fullmatch(host):
        raise ValueError("host contains unsupported characters")


def monitoring_contract(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "host": args.host,
        "job_id": args.job_id,
        "poll_seconds": args.poll_seconds,
        "pending_alert_seconds": args.pending_alert_seconds,
        "terminal_observability_seconds": args.terminal_observability_seconds,
        "max_watch_seconds": args.max_watch_seconds,
        "query_failures": args.query_failures,
        "expected_owner": args.expected_owner,
        "expected_job_name": args.expected_job_name,
        "expected_partition": args.expected_partition,
    }


def contract_digest(contract: Dict[str, Any]) -> str:
    digest = hashlib.sha256(canonical_json(contract)).hexdigest()
    return f"sha256:{digest}"


def previous_contract_digest(run: Optional[Path]) -> Optional[str]:
    if run is None:
        return None
    manifest = read_json(run / "manifest.json")
    value = manifest.get("contract_digest")
    return value if isinstance(value, str) else None


def resolved_event_binding(args: argparse.Namespace) -> Optional[Dict[str, Any]]:
    """Load and validate the explicit opt-in wake binding for this monitor.

    Without --event-binding no semantic event is ever published: unattended
    remains the default and a terminal file alone never creates wake work.
    When --bridge-config is also given, the binding must agree with the
    configured instance identity, failing closed on any mismatch.
    """
    if args.event_binding is None:
        return None
    binding = semantic_events.load_event_binding(Path(args.event_binding))
    if args.bridge_config is not None:
        config = semantic_events.load_bridge_config(Path(args.bridge_config))
        for key, binding_key in (
            ("instance_id", "app_server_instance"),
            ("codex_home_id", "codex_home_id"),
            ("workspace", "workspace"),
        ):
            if config[key] != binding[binding_key]:
                raise ValueError(
                    f"event binding {binding_key} does not match bridge config"
                )
    return binding


def event_binding_digest(binding: Optional[Dict[str, Any]]) -> Optional[str]:
    if binding is None:
        return None
    return semantic_events.sha256_prefix(semantic_events.canonical_json(binding))


def auto_resume_preflight(
    args: argparse.Namespace, binding: Optional[Dict[str, Any]]
) -> List[Dict[str, str]]:
    """Report or reject start-time conditions that do not form a wake loop."""
    warnings: List[Dict[str, str]] = []
    require = bool(getattr(args, "require_auto_resume", False))
    if binding is None:
        if args.bridge_config is not None:
            warnings.append({
                "code": "bridge_config_without_event_binding",
                "message": "bridge config is present but this run has no event binding; no wake event will be published",
            })
    elif args.bridge_config is None:
        warnings.append({
            "code": "event_binding_without_bridge_config",
            "message": "event binding will publish wake events, but automatic resume is not configured or verified without --bridge-config",
        })
    else:
        config = semantic_events.load_bridge_config(Path(args.bridge_config))
        if not config["enabled"]:
            warnings.append({
                "code": "bridge_config_disabled",
                "message": "event binding is present but the bridge configuration is disabled",
            })
        else:
            try:
                semantic_events.read_bridge_activation(
                    semantic_events.outbox_root(Path(args.state_dir)), config
                )
            except semantic_events.SemanticEventError as exc:
                warnings.append({
                    "code": exc.reason,
                    "message": "event binding and bridge config are present, but the durable bridge activation receipt is not ready",
                })
    if require and warnings:
        codes = ",".join(item["code"] for item in warnings)
        raise ValueError(
            "--require-auto-resume preconditions failed: " + codes
            + "; this checks binding/config/activation, not daemon liveness"
        )
    if require and binding is None:
        raise ValueError("--require-auto-resume requires --event-binding and --bridge-config")
    return warnings


def emit_auto_resume_warnings(warnings: List[Dict[str, str]]) -> None:
    for warning in warnings:
        print(
            f"*** WARNING [{warning['code']}]: {warning['message']} ***",
            file=sys.stderr,
            flush=True,
        )


def print_start_payload(
    payload: Dict[str, Any], warnings: List[Dict[str, str]]
) -> None:
    if warnings:
        payload["warnings"] = warnings
    print(json.dumps(payload, sort_keys=True))


def reconcile_run_event(run: Optional[Path]) -> Optional[str]:
    """Close the crash window between terminal.json and event publication.

    If the supervisor died after publishing the terminal record but before
    publishing the semantic event, any later status/wait observation
    republishes it (idempotently, by event id). Returns the publication
    outcome when a repair happened, else None.
    """
    if run is None:
        return None
    manifest = read_json(run / "manifest.json")
    if not isinstance(manifest.get("event_binding"), dict):
        return None
    if not (run / "terminal.json").exists():
        return None
    if (run / "semantic_event.json").exists():
        return None
    prior_failure = read_json(run / "semantic_event_failure.json")
    if prior_failure and prior_failure.get("retryable") is False:
        return None
    terminal = read_json(run / "terminal.json")
    exit_code = terminal.get("watcher_exit_code")
    if not isinstance(exit_code, int):
        return None
    if not terminal_event_envelope_verified(run, manifest, terminal):
        return None
    publish_semantic_event(
        run,
        manifest=manifest,
        terminal=terminal,
        watcher_exit_code=exit_code,
    )
    published = read_json(run / "semantic_event.json")
    return published.get("state") if published else None


def terminal_event_envelope_verified(
    run: Path, manifest: Dict[str, Any], terminal: Dict[str, Any]
) -> bool:
    """Verify immutable run/child evidence before any wake publication.

    The watcher result may itself be unverified; that case intentionally
    publishes ``contract_violation``. The surrounding terminal envelope must
    still be fully bound to this run and its observed child exit.
    """
    try:
        binding = semantic_events.validate_event_binding(manifest.get("event_binding"))
        started = read_json(run / "supervisor_started.json")
        child_exit = read_json(run / "child_exit.json")
        manifest_sha = sha256_file(run / "manifest.json")
    except (OSError, ValueError, semantic_events.SemanticEventError):
        return False
    exit_code = terminal.get("watcher_exit_code")
    signal_number = terminal.get("watcher_signal")
    return bool(
        binding
        and manifest.get("schema_version") == f"{SCHEMA_PREFIX}.manifest/v1"
        and manifest.get("run_id") == run.name
        and terminal.get("schema_version") == f"{SCHEMA_PREFIX}.terminal/v1"
        and terminal.get("run_id") == run.name
        and terminal.get("host") == manifest.get("host")
        and terminal.get("job_id") == manifest.get("job_id")
        and terminal.get("scope") == "slurm_only"
        and terminal.get("project_gate_evaluated") is False
        and terminal.get("manifest_sha256") == manifest_sha
        and started.get("manifest_sha256") == manifest_sha
        and terminal.get("contract_digest") == manifest.get("contract_digest")
        and type(exit_code) is int
        and (signal_number is None or type(signal_number) is int)
        and child_exit.get("schema_version") == f"{SCHEMA_PREFIX}.child-exit/v1"
        and child_exit.get("run_id") == run.name
        and child_exit.get("exit_code") == exit_code
        and child_exit.get("signal") == signal_number
    )


def publish_semantic_event(
    run: Path,
    *,
    manifest: Dict[str, Any],
    terminal: Dict[str, Any],
    watcher_exit_code: Optional[int],
) -> None:
    """Best-effort publication of one durable semantic event per terminal.

    Failure never alters or invalidates the terminal record, which remains
    the sole terminal authority; the outcome is recorded beside the run.
    """
    binding = manifest.get("event_binding")
    if not isinstance(binding, dict):
        return
    if not terminal_event_envelope_verified(run, manifest, terminal):
        return
    event_enum = HPC_SEMANTIC_EVENTS.get(
        watcher_exit_code if watcher_exit_code is not None else -1
    )
    if event_enum is None:
        return
    # Only a terminal record whose watcher result is actually verified may
    # carry a success/failure/observability event. An unverified result
    # downgrades the event to contract_violation: the observation itself is
    # broken and the wake turn must reconcile manually, never trust a
    # scheduler outcome.
    watcher_result = terminal.get("watcher_result")
    verified = isinstance(watcher_result, dict) and watcher_result.get("verified") is True
    if not verified:
        event_enum = "contract_violation"
    try:
        terminal_digest = f"sha256:{sha256_file(run / 'terminal.json')}"
        event = semantic_events.build_event(
            backend="slurm",
            handle=f"{manifest['host']}-{manifest['job_id']}",
            generation=run.name,
            terminal_digest=terminal_digest,
            event=event_enum,
            exit_code=watcher_exit_code if isinstance(watcher_exit_code, int) else None,
            binding=binding,
        )
        outcome = semantic_events.publish_event(
            semantic_events.outbox_root(Path(str(manifest["state_dir"]))), event
        )
        try:
            publish_json_no_replace(
                run / "semantic_event.json",
                {
                    "schema_version": f"{SCHEMA_PREFIX}.semantic-event/v1",
                    "run_id": run.name,
                    "event_id": event["event_id"],
                    "event": event_enum,
                    "state": outcome,
                    "published_at": utc_now(),
                },
            )
        except FileExistsError:
            # A reconciling observer recorded this publication first.
            pass
        try:
            (run / "semantic_event_failure.json").unlink()
        except FileNotFoundError:
            pass
    except (OSError, ValueError, semantic_events.SemanticEventError) as exc:
        if (run / "semantic_event.json").exists():
            return
        replace_json(
            run / "semantic_event_failure.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}.semantic-event/v1",
                "run_id": run.name,
                "state": "publish_failed",
                "reason": getattr(exc, "reason", type(exc).__name__),
                "retryable": isinstance(exc, OSError),
                "observed_at": utc_now(),
            },
        )


def start_monitor(args: argparse.Namespace) -> int:
    validate_identity(args.job_id, args.host)
    event_binding = resolved_event_binding(args)
    auto_resume_warnings = auto_resume_preflight(args, event_binding)
    emit_auto_resume_warnings(auto_resume_warnings)
    watcher_input = args.watcher_path.expanduser()
    if watcher_input.is_symlink():
        raise ValueError(f"watcher path must not be a symlink: {watcher_input}")
    watcher = watcher_input.resolve(strict=True)
    if not watcher.is_file():
        raise ValueError(f"watcher is not a regular file: {watcher}")
    args.watcher_path = watcher
    ensure_private_directory(args.state_dir)
    base = base_dir(args.state_dir, args.host, args.job_id)
    try:
        lock_fd = open_lifetime_lock(base)
    except BlockingIOError:
        run = current_run(base)
        payload = run_status(run, args.host, args.job_id)
        current_binding = None
        if run is not None:
            candidate = read_json(run / "manifest.json").get("event_binding")
            if isinstance(candidate, dict):
                current_binding = candidate
        if event_binding is not None and current_binding != event_binding:
            payload["start_result"] = "active_run_binding_conflict"
            payload["requested_event_binding_digest"] = event_binding_digest(
                event_binding
            )
            payload["active_event_binding_digest"] = event_binding_digest(
                current_binding
            )
            print_start_payload(payload, auto_resume_warnings)
            return 12
        payload["start_result"] = "already_active"
        print_start_payload(payload, auto_resume_warnings)
        return 2

    try:
        previous = run_status(current_run(base), args.host, args.job_id)
        digest = contract_digest(monitoring_contract(args))
        prior_digest = previous_contract_digest(current_run(base))
        if (
            prior_digest is not None
            and prior_digest != digest
            and not args.allow_contract_change
        ):
            # The frozen monitoring contract for this host/job changed; that
            # is a conflict requiring an explicit reviewed decision, never an
            # implicit replacement watcher.
            previous["start_result"] = "contract_conflict"
            previous["contract_digest"] = digest
            previous["previous_contract_digest"] = prior_digest
            print_start_payload(previous, auto_resume_warnings)
            return 12
        if previous["state"] != "not_started" and not args.restart:
            previous["start_result"] = "restart_required"
            print_start_payload(previous, auto_resume_warnings)
            return 3

        run_id = f"run_{int(time.time())}_{os.getpid()}_{secrets.token_hex(4)}"
        run = base / "runs" / run_id
        ensure_private_directory(run)
        command = watcher_command(args)
        manifest = {
            "schema_version": f"{SCHEMA_PREFIX}.manifest/v1",
            "run_id": run_id,
            "host": args.host,
            "job_id": args.job_id,
            "created_at": utc_now(),
            "watcher_argv": command,
            "watcher_path_sha256": sha256_file(watcher),
            "state_dir": str(args.state_dir.resolve()),
            "scope": "slurm_only",
            "project_gate_evaluated": False,
            "contract": monitoring_contract(args),
            "contract_digest": digest,
        }
        if event_binding is not None:
            manifest["event_binding"] = event_binding
            manifest["event_binding_digest"] = event_binding_digest(event_binding)
            manifest["event_backend"] = "slurm"
        publish_json_no_replace(run / "manifest.json", manifest)
        if event_binding is not None:
            # Written before the supervisor exists: durable proof that this
            # run intended to publish a wake event, used by the reconciler
            # if the run dies between terminal and event publication.
            publish_json_no_replace(
                run / "event_intent.json",
                {
                    "schema_version": f"{SCHEMA_PREFIX}.event-intent/v1",
                    "run_id": run_id,
                    "host": args.host,
                    "job_id": args.job_id,
                    "event_backend": "slurm",
                    "binding_instance": event_binding.get("app_server_instance"),
                    "declared_at": utc_now(),
                },
            )
        manifest_sha = sha256_file(run / "manifest.json")
        replace_json(
            base / "current.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}.current/v1",
                "host": args.host,
                "job_id": args.job_id,
                "run_id": run_id,
                "updated_at": utc_now(),
            },
        )
        os.ftruncate(lock_fd, 0)
        os.write(lock_fd, canonical_json({"run_id": run_id, "launcher_pid": os.getpid()}))
        os.fsync(lock_fd)

        supervisor_stdout = os.open(
            str(run / "supervisor.log"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600
        )
        try:
            child = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "_supervise",
                    "--run-dir",
                    str(run),
                    "--manifest-sha256",
                    manifest_sha,
                    "--lock-fd",
                    str(lock_fd),
                ],
                stdin=subprocess.DEVNULL,
                stdout=supervisor_stdout,
                stderr=supervisor_stdout,
                close_fds=True,
                pass_fds=(lock_fd,),
                start_new_session=True,
            )
        finally:
            os.close(supervisor_stdout)
        os.close(lock_fd)
        lock_fd = -1

        deadline = time.monotonic() + args.handshake_seconds
        while time.monotonic() < deadline:
            status = run_status(run, args.host, args.job_id)
            if status["state"] in {"active", "terminal", "exit_observed_terminal_missing"}:
                status["start_result"] = "started"
                status["launcher_observed_pid"] = child.pid
                print_start_payload(status, auto_resume_warnings)
                return 0
            if child.poll() is not None:
                break
            time.sleep(0.05)
        status = run_status(run, args.host, args.job_id)
        status["start_result"] = "handshake_failed"
        status["launcher_observed_pid"] = child.pid
        print_start_payload(status, auto_resume_warnings)
        return 4
    finally:
        if lock_fd >= 0:
            os.close(lock_fd)


def set_parent_death_signal() -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGTERM) != 0:  # PR_SET_PDEATHSIG
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
    if os.getppid() == 1:
        os.kill(os.getpid(), signal.SIGTERM)


def validate_manifest_watcher(manifest: Dict[str, Any]) -> None:
    argv = manifest.get("watcher_argv")
    expected_sha = manifest.get("watcher_path_sha256")
    if not isinstance(argv, list) or len(argv) < 2 or not isinstance(argv[1], str):
        raise ValueError("manifest watcher argv is invalid")
    watcher = Path(argv[1])
    if watcher.is_symlink() or not watcher.is_file():
        raise ValueError("manifest watcher path is not a regular non-symlink file")
    if not isinstance(expected_sha, str) or sha256_file(watcher) != expected_sha:
        raise ValueError("watcher hash mismatch")


def verified_watcher_result(
    manifest: Dict[str, Any],
    started_epoch_ns: int,
    watcher_exit_code: Optional[int],
    watcher_signal: Optional[int],
) -> Dict[str, Any]:
    state_dir = Path(str(manifest["state_dir"]))
    path = state_dir / f"{manifest['host']}-{manifest['job_id']}.result.json"
    payload = read_json(path)
    if not payload:
        return {
            "present": False,
            "verified": False,
            "path": str(path),
            "problems": ["result_missing"],
        }
    problems = []
    if payload.get("job_id") != manifest["job_id"]:
        problems.append("job_id_mismatch")
    if payload.get("scope") != "slurm_only":
        problems.append("scope_mismatch")
    if payload.get("project_gate_evaluated") is not False:
        problems.append("project_gate_mismatch")
    if watcher_signal is not None:
        problems.append("watcher_signaled")
    exit_key = watcher_exit_code if watcher_exit_code is not None else -1
    allowed_events = WATCHER_EXIT_EVENTS.get(exit_key, set())
    if payload.get("event") not in allowed_events:
        problems.append("exit_event_mismatch")
    allowed_classifications = WATCHER_EXIT_CLASSIFICATIONS.get(exit_key, set())
    if payload.get("slurm_classification") not in allowed_classifications:
        problems.append("exit_classification_mismatch")
    if watcher_exit_code == 0 and (
        payload.get("state") != "COMPLETED" or payload.get("exit_code") != "0:0"
    ):
        problems.append("success_evidence_mismatch")
    if watcher_exit_code == 3 and (
        payload.get("state") == "COMPLETED" and payload.get("exit_code") == "0:0"
    ):
        problems.append("failure_evidence_mismatch")
    try:
        if path.stat().st_mtime_ns < started_epoch_ns:
            problems.append("stale_result")
    except OSError:
        problems.append("result_stat_failed")
    return {
        "present": True,
        "verified": not problems,
        "path": str(path),
        "sha256": sha256_file(path) if not problems else None,
        "event": payload.get("event"),
        "problems": problems,
        "payload": payload if not problems else None,
    }


def supervise(args: argparse.Namespace) -> int:
    run = args.run_dir.resolve()
    lock_fd = args.lock_fd
    child: Optional[subprocess.Popen[bytes]] = None
    started_monotonic = time.monotonic()
    started_epoch_ns = time.time_ns()
    try:
        manifest_path = run / "manifest.json"
        if sha256_file(manifest_path) != args.manifest_sha256:
            raise ValueError("manifest hash mismatch")
        manifest = read_json(manifest_path)
        if manifest.get("run_id") != run.name:
            raise ValueError("manifest run identity mismatch")
        validate_manifest_watcher(manifest)
        started = {
            "schema_version": f"{SCHEMA_PREFIX}.supervisor-started/v1",
            "run_id": run.name,
            "pid": os.getpid(),
            "pid_start_ticks": process_start_ticks(os.getpid()),
            "started_at": utc_now(),
            "manifest_sha256": args.manifest_sha256,
        }
        publish_json_no_replace(run / "supervisor_started.json", started)

        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        requested_signal: Dict[str, Optional[int]] = {"value": None}

        def forward(signum: int, _frame: object) -> None:
            requested_signal["value"] = signum
            if child is not None and child.poll() is None:
                try:
                    child.send_signal(signum)
                except ProcessLookupError:
                    pass

        signal.signal(signal.SIGTERM, forward)
        signal.signal(signal.SIGINT, forward)

        stdout_fd = os.open(
            str(run / "watcher.stdout.log"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600
        )
        stderr_fd = os.open(
            str(run / "watcher.stderr.log"), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600
        )
        try:
            child = subprocess.Popen(
                list(manifest["watcher_argv"]),
                stdin=subprocess.DEVNULL,
                stdout=stdout_fd,
                stderr=stderr_fd,
                close_fds=True,
                preexec_fn=set_parent_death_signal,
            )
            if requested_signal["value"] is not None:
                child.send_signal(requested_signal["value"])
        finally:
            os.close(stdout_fd)
            os.close(stderr_fd)
        replace_json(
            run / "runtime.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}.runtime/v1",
                "run_id": run.name,
                "pid": child.pid,
                "pid_start_ticks": process_start_ticks(child.pid),
                "started_at": utc_now(),
            },
        )
        return_code = child.wait()
        exit_code = return_code if return_code >= 0 else None
        signal_number = -return_code if return_code < 0 else None
        child_exit = {
            "schema_version": f"{SCHEMA_PREFIX}.child-exit/v1",
            "run_id": run.name,
            "observed_at": utc_now(),
            "exit_code": exit_code,
            "signal": signal_number,
            "requested_supervisor_signal": requested_signal["value"],
        }
        publish_json_no_replace(run / "child_exit.json", child_exit)
        result = verified_watcher_result(
            manifest, started_epoch_ns, exit_code, signal_number
        )
        if signal_number is not None:
            outcome = "watcher_signaled"
        elif exit_code == 0:
            outcome = "watcher_exit_zero"
        else:
            outcome = "watcher_exit_nonzero"
        terminal = {
            "schema_version": f"{SCHEMA_PREFIX}.terminal/v1",
            "run_id": run.name,
            "host": manifest["host"],
            "job_id": manifest["job_id"],
            "scope": "slurm_only",
            "project_gate_evaluated": False,
            "observer_state": "exited",
            "observer_outcome": outcome,
            "watcher_exit_code": exit_code,
            "watcher_signal": signal_number,
            "watcher_result": result,
            "started_at": started["started_at"],
            "ended_at": utc_now(),
            "duration_monotonic_ms": int((time.monotonic() - started_monotonic) * 1000),
            "manifest_sha256": args.manifest_sha256,
            "contract_digest": manifest.get("contract_digest"),
        }
        publish_json_no_replace(run / "terminal.json", terminal)
        publish_semantic_event(
            run,
            manifest=manifest,
            terminal=terminal,
            watcher_exit_code=exit_code,
        )
        return 0
    except Exception as exc:
        failure = {
            "schema_version": f"{SCHEMA_PREFIX}.supervisor-failure/v1",
            "run_id": run.name,
            "observed_at": utc_now(),
            "failure_type": type(exc).__name__,
            "child_started": child is not None,
        }
        try:
            replace_json(run / "supervisor_failure.json", failure)
        except Exception:
            pass
        if child is None:
            try:
                publish_json_no_replace(
                    run / "terminal.json",
                    {
                        "schema_version": f"{SCHEMA_PREFIX}.terminal/v1",
                        "run_id": run.name,
                        "scope": "observer_only",
                        "project_gate_evaluated": False,
                        "observer_state": "launch_failed",
                        "observer_outcome": "supervisor_failure",
                        "failure_type": type(exc).__name__,
                        "ended_at": utc_now(),
                    },
                )
            except Exception:
                pass
        return 12
    finally:
        os.close(lock_fd)


def status_command(args: argparse.Namespace) -> int:
    validate_identity(args.job_id, args.host)
    run = current_run(base_dir(args.state_dir, args.host, args.job_id))
    # Local-only repair: if the supervisor crashed between publishing the
    # terminal and publishing its semantic event, finish that publication.
    try:
        reconcile_run_event(run)
    except (OSError, ValueError):
        pass
    status = run_status(run, args.host, args.job_id)
    print(json.dumps(status, sort_keys=True))
    if args.require_terminal and (
        status["state"] != "terminal" or status.get("terminal_verified") is not True
    ):
        return 3
    return 0


def terminal_wait_result(status: Dict[str, Any]) -> tuple[int, Dict[str, Any]]:
    terminal = status.get("terminal")
    watcher_result = terminal.get("watcher_result") if isinstance(terminal, dict) else None
    problems = []
    expected_schema = f"{SCHEMA_PREFIX}.terminal/v1"
    if not isinstance(terminal, dict):
        problems.append("terminal_missing")
    else:
        if terminal.get("schema_version") != expected_schema:
            problems.append("terminal_schema_mismatch")
        if terminal.get("run_id") is not None and terminal.get("run_id") != status.get("run_id"):
            problems.append("terminal_run_id_mismatch")
        if terminal.get("host") != status.get("host"):
            problems.append("terminal_host_mismatch")
        if terminal.get("job_id") != status.get("job_id"):
            problems.append("terminal_job_id_mismatch")
        if terminal.get("scope") != "slurm_only":
            problems.append("terminal_scope_mismatch")
        if terminal.get("project_gate_evaluated") is not False:
            problems.append("terminal_project_gate_mismatch")
        if not isinstance(watcher_result, dict) or watcher_result.get("verified") is not True:
            problems.append("watcher_result_unverified")

    watcher_exit_code = terminal.get("watcher_exit_code") if isinstance(terminal, dict) else None
    if type(watcher_exit_code) is not int:
        problems.append("watcher_exit_code_invalid")

    terminal_sha = None
    run_dir = status.get("run_dir")
    if isinstance(run_dir, str):
        path = Path(run_dir) / "terminal.json"
        try:
            terminal_sha = sha256_file(path)
        except OSError:
            problems.append("terminal_hash_failed")
    else:
        problems.append("run_dir_missing")

    # Cross-check the monitoring contract binding when the new field exists.
    # Terminals from the initial release carry no digest; they stay readable
    # and are marked legacy evidence instead of failing.
    evidence_strength = "legacy"
    if isinstance(terminal, dict) and isinstance(terminal.get("contract_digest"), str):
        evidence_strength = "full"
        run_path = Path(str(run_dir)) if isinstance(run_dir, str) else None
        manifest = read_json(run_path / "manifest.json") if run_path is not None else {}
        if (
            terminal.get("run_id") != status.get("run_id")
            and "terminal_run_id_mismatch" not in problems
        ):
            problems.append("terminal_run_id_mismatch")
        if not manifest or manifest.get("contract_digest") != terminal.get("contract_digest"):
            problems.append("terminal_contract_digest_mismatch")
        if run_path is not None:
            started = read_json(run_path / "supervisor_started.json")
            child_exit = read_json(run_path / "child_exit.json")
            try:
                manifest_sha = sha256_file(run_path / "manifest.json")
            except OSError:
                manifest_sha = None
            if (
                manifest_sha is None
                or terminal.get("manifest_sha256") != manifest_sha
                or started.get("manifest_sha256") != manifest_sha
            ):
                problems.append("terminal_manifest_digest_mismatch")
            if (
                child_exit.get("schema_version") != f"{SCHEMA_PREFIX}.child-exit/v1"
                or child_exit.get("run_id") != status.get("run_id")
                or child_exit.get("exit_code") != watcher_exit_code
                or child_exit.get("signal") != terminal.get("watcher_signal")
            ):
                problems.append("terminal_child_exit_mismatch")

    payload: Dict[str, Any] = {
        "schema_version": f"{SCHEMA_PREFIX}.wait/v1",
        "state": "terminal",
        "host": status.get("host"),
        "job_id": status.get("job_id"),
        "run_id": status.get("run_id"),
        "terminal_verified": not problems,
        "terminal_outcome": terminal.get("observer_outcome") if isinstance(terminal, dict) else None,
        "watcher_exit_code": watcher_exit_code,
        "terminal_sha256": terminal_sha,
        "evidence_strength": evidence_strength,
    }
    if problems:
        payload["problems"] = problems
        return 12, payload

    result_payload = watcher_result.get("payload")
    if isinstance(result_payload, dict):
        payload["slurm_classification"] = result_payload.get("slurm_classification")
    return (0 if watcher_exit_code == 0 else 3), payload


def wait_command(args: argparse.Namespace) -> int:
    if not args.notification_worker_ack:
        raise ValueError(
            "wait requires --notification-worker-ack; the flag acknowledges the "
            "notification-worker contract but does not authenticate a model role"
        )
    validate_identity(args.job_id, args.host)
    deadline = time.monotonic() + args.timeout_seconds
    publication_missing_since: Optional[float] = None
    while True:
        run = current_run(base_dir(args.state_dir, args.host, args.job_id))
        try:
            reconcile_run_event(run)
        except (OSError, ValueError):
            pass
        status = run_status(run, args.host, args.job_id)
        state = status.get("state")
        if state == "terminal":
            exit_code, payload = terminal_wait_result(status)
            print(json.dumps(payload, sort_keys=True))
            return exit_code
        if state == "exit_observed_terminal_missing":
            if publication_missing_since is None:
                publication_missing_since = time.monotonic()
            elif time.monotonic() - publication_missing_since >= args.terminal_publication_grace_seconds:
                print(
                    json.dumps(
                        {
                            "schema_version": f"{SCHEMA_PREFIX}.wait/v1",
                            "state": state,
                            "host": args.host,
                            "job_id": args.job_id,
                            "run_id": status.get("run_id"),
                        },
                        sort_keys=True,
                    )
                )
                return 12
        elif state == "active":
            publication_missing_since = None
        else:
            print(
                json.dumps(
                    {
                        "schema_version": f"{SCHEMA_PREFIX}.wait/v1",
                        "state": state,
                        "host": args.host,
                        "job_id": args.job_id,
                        "run_id": status.get("run_id"),
                    },
                    sort_keys=True,
                )
            )
            return 11 if state == "supervisor_lost" else 12

        if time.monotonic() >= deadline:
            print(
                json.dumps(
                    {
                        "schema_version": f"{SCHEMA_PREFIX}.wait/v1",
                        "state": "wait_timeout",
                        "host": args.host,
                        "job_id": args.job_id,
                        "run_id": status.get("run_id"),
                    },
                    sort_keys=True,
                )
            )
            return 4
        time.sleep(min(args.poll_seconds, max(0.0, deadline - time.monotonic())))


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


# ---------------------------------------------------------------------------
# Capability probe (doctor), listing, explanation, and safe outbox cleanup
# ---------------------------------------------------------------------------

DOCTOR_SCHEMA = "codex-monitor.doctor/v1"
NETWORK_FILESYSTEMS = {
    "9p", "afs", "ceph", "cifs", "fuse.ceph", "fuse.glusterfs", "fuse.sshfs",
    "glusterfs", "lustre", "nfs", "nfs4", "smb3",
}


def decode_mount_field(value: str) -> str:
    replacements = {"\\040": " ", "\\011": "\t", "\\012": "\n", "\\134": "\\"}
    for encoded, decoded in replacements.items():
        value = value.replace(encoded, decoded)
    return value


def filesystem_type(path: Path) -> Optional[str]:
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    try:
        resolved = probe.resolve(strict=True)
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    best: Optional[tuple] = None
    for line in lines:
        before, separator, after = line.partition(" - ")
        fields = before.split()
        after_fields = after.split()
        if not separator or len(fields) < 5 or not after_fields:
            continue
        mount_point = Path(decode_mount_field(fields[4]))
        if resolved == mount_point or mount_point in resolved.parents:
            candidate = (len(str(mount_point)), after_fields[0])
            if best is None or candidate[0] > best[0]:
                best = candidate
    return best[1] if best else None


def probe_app_server(config: Dict[str, Any]) -> Dict[str, Any]:
    import app_server_bridge

    try:
        session = app_server_bridge.AppServerSession(
            list(config["transport"]["command"]),
            float(config["request_timeout_seconds"]),
            env=app_server_bridge.spawn_env(config),
        )
    except (OSError, ValueError) as exc:
        return {"healthy": False, "reason": f"spawn_failed: {type(exc).__name__}"}
    try:
        session.initialize()
    except app_server_bridge.DeliveryError as exc:
        return {"healthy": False, "reason": exc.code}
    except Exception as exc:  # defensive: probe must never crash doctor
        return {"healthy": False, "reason": type(exc).__name__}
    finally:
        session.close()
    return {"healthy": True, "reason": None}


def build_doctor_payload(args: argparse.Namespace) -> Dict[str, Any]:
    requested = args.mode
    checks: List[Dict[str, Any]] = []
    kind = filesystem_type(args.state_dir)
    suitable = kind is not None and kind.lower() not in NETWORK_FILESYSTEMS
    checks.append({
        "name": "state_root_filesystem",
        "state": "pass" if suitable else "warn",
        "detail": f"{args.state_dir} is on {kind or 'unknown filesystem'}",
    })
    app_server: Dict[str, Any] = {"configured": False}
    reason = "bridge_not_configured"
    if args.bridge_config is not None:
        try:
            config = semantic_events.load_bridge_config(args.bridge_config)
            app_server = {
                "configured": True,
                "transport": config["transport"]["type"],
                "instance_id": config["instance_id"],
                "enabled": config["enabled"],
            }
            if not config["enabled"]:
                reason = "bridge_disabled"
                checks.append({"name": "bridge_config", "state": "warn",
                               "detail": "configuration present but disabled"})
            elif args.probe_app_server:
                health = probe_app_server(config)
                app_server.update(health)
                if health["healthy"]:
                    reason = "bridge_configured_and_healthy"
                    checks.append({"name": "app_server_probe", "state": "pass",
                                   "detail": "initialize handshake succeeded"})
                else:
                    reason = f"bridge_probe_failed:{health['reason']}"
                    checks.append({"name": "app_server_probe", "state": "warn",
                                   "detail": health["reason"] or "unhealthy"})
            else:
                reason = "bridge_configured"
                checks.append({"name": "bridge_config", "state": "pass",
                               "detail": "valid and enabled (no live probe requested)"})
        except semantic_events.SemanticEventError as exc:
            reason = f"bridge_config_invalid:{exc.reason}"
            checks.append({"name": "bridge_config", "state": "warn",
                           "detail": exc.reason})
    else:
        checks.append({"name": "bridge_config", "state": "pass",
                       "detail": "no configuration file supplied"})

    selected = requested
    if requested == "external-event-bridge":
        if not reason.startswith("bridge_configured"):
            selected = "unattended"
    elif requested == "auto":
        selected = (
            "external-event-bridge"
            if reason.startswith("bridge_configured")
            else "unattended"
        )

    outbox_root = semantic_events.outbox_root(args.state_dir)
    entries = semantic_events.list_outbox(outbox_root) if outbox_root.exists() else []
    outbox_summary = {
        "present": outbox_root.exists(),
        "pending": sum(1 for e in entries if e.get("state") == "pending"),
        "delivered": sum(1 for e in entries if e.get("state") == "delivered"),
        "dead_letter": sum(1 for e in entries if e.get("state") == "dead_letter"),
    }
    auto_resume = selected == "external-event-bridge"
    return {
        "schema_version": DOCTOR_SCHEMA,
        "skill": "codex-hpc-monitor",
        "mode": {
            "selected": selected,
            "requested": requested,
            "reason": reason if selected == "unattended" else reason,
        },
        "zero_turns_while_unchanged": True,
        "agent_slot_occupied": False,
        "auto_resume_available": auto_resume,
        "notification_available": auto_resume,
        "state_root": {
            "path": str(args.state_dir),
            "filesystem": kind,
            "suitable": suitable,
            "reason": None if suitable else "network or unknown filesystem",
        },
        "outbox": outbox_summary,
        "app_server": app_server,
        "goal_worker": {
            "available": False,
            "reason": "runtime_capability_not_provable_deterministically",
        },
        "recovery_command": (
            f"python3 {Path(__file__).resolve()} status <job-id> "
            f"--host {args.host} --state-dir {args.state_dir}"
        ),
        "checks": checks,
    }


def render_doctor_text(payload: Dict[str, Any]) -> str:
    mode = payload["mode"]
    lines = [
        f"{payload['skill']} doctor",
        f"mode: {mode['selected']} (requested {mode['requested']}; reason: {mode['reason']})",
        f"zero model turns while unchanged: yes"
        if payload["zero_turns_while_unchanged"]
        else "zero model turns while unchanged: no",
        f"agent slot occupied: {'yes' if payload['agent_slot_occupied'] else 'no'}",
        f"auto-resume available: {'yes' if payload['auto_resume_available'] else 'no'}",
        "notification available: "
        f"{'yes' if payload['notification_available'] else 'no'}",
        "state root: {path} ({filesystem}, {suitability})".format(
            path=payload["state_root"]["path"],
            filesystem=payload["state_root"]["filesystem"] or "unknown",
            suitability="suitable" if payload["state_root"]["suitable"] else "unsuitable",
        ),
        "outbox: present={present} pending={pending} delivered={delivered} dead_letter={dead_letter}".format(
            **payload["outbox"]
        ),
        "app server: "
        + (
            "not configured"
            if not payload["app_server"]["configured"]
            else "transport={transport} instance={instance_id} enabled={enabled} healthy={healthy}".format(
                **{
                    "transport": payload["app_server"].get("transport"),
                    "instance_id": payload["app_server"].get("instance_id"),
                    "enabled": payload["app_server"].get("enabled"),
                    "healthy": payload["app_server"].get("healthy"),
                }
            )
        ),
        "goal worker: not available ({reason})".format(
            reason=payload["goal_worker"]["reason"]
        ),
        f"recovery: {payload['recovery_command']}",
    ]
    return "\n".join(lines)


def doctor_command(args: argparse.Namespace) -> int:
    payload = build_doctor_payload(args)
    if args.format == "text":
        print(render_doctor_text(payload))
    else:
        print(json.dumps(payload, sort_keys=True))
    failed = [c for c in payload["checks"] if c["state"] == "fail"]
    return 1 if failed else 0


def list_command(args: argparse.Namespace) -> int:
    supervisors = args.state_dir / "supervisors"
    entries = []
    if supervisors.is_dir():
        for child in sorted(supervisors.iterdir()):
            if not child.is_dir():
                continue
            host, separator, job_id = child.name.rpartition("-")
            if not separator or not JOB_ID_RE.fullmatch(job_id):
                continue
            status = run_status(current_run(child), host, job_id)
            entries.append({
                "host": host,
                "job_id": job_id,
                "state": status.get("state"),
                "run_id": status.get("run_id"),
                "evidence_strength": status.get("evidence_strength"),
                "observer_outcome": (status.get("terminal") or {}).get("observer_outcome"),
            })
    print(json.dumps({
        "schema_version": "codex-monitor.list/v1",
        "skill": "codex-hpc-monitor",
        "monitors": entries,
    }, sort_keys=True))
    return 0


EXPLAIN_GUIDANCE = {
    "not_started": "no supervisor run is recorded; start one if monitoring is intended",
    "active": (
        "the detached supervisor is alive; end the turn now and read status "
        "once on a later genuine event - a terminal file cannot wake an "
        "inactive Codex turn"
    ),
    "terminal": (
        "read the verified terminal record and perform business acceptance "
        "independently; the record is scheduler evidence only, and a "
        "terminal file cannot wake an inactive Codex turn by itself"
    ),
    "launch_unconfirmed": "handshake never observed; inspect ownership before restarting",
    "supervisor_lost": (
        "the recorded supervisor identity is no longer live and no terminal is "
        "established; fail closed and review the run evidence"
    ),
    "exit_observed_terminal_missing": (
        "the watcher exited but terminal publication is incomplete; fail closed"
    ),
}


def explain_command(args: argparse.Namespace) -> int:
    validate_identity(args.job_id, args.host)
    status = run_status(
        current_run(base_dir(args.state_dir, args.host, args.job_id)),
        args.host,
        args.job_id,
    )
    state = status.get("state", "unknown")
    payload = {
        "schema_version": "codex-monitor.explain/v1",
        "skill": "codex-hpc-monitor",
        "host": args.host,
        "job_id": args.job_id,
        "state": state,
        "guidance": EXPLAIN_GUIDANCE.get(
            state, "unknown state; inspect the run directory before acting"
        ),
        "evidence_strength": status.get("evidence_strength"),
        "run_dir": status.get("run_dir"),
        "wake_events_enabled": False,
        "next_commands": [],
    }
    run = current_run(base_dir(args.state_dir, args.host, args.job_id))
    if run is not None:
        manifest = read_json(run / "manifest.json")
        payload["wake_events_enabled"] = isinstance(
            manifest.get("event_binding"), dict
        )
    script = Path(__file__).resolve()
    payload["next_commands"] = [
        f"python3 {script} status {args.job_id} --host {args.host} --state-dir {args.state_dir}",
    ]
    if state == "terminal":
        payload["next_commands"].append(
            f"python3 {script} wait {args.job_id} --host {args.host} "
            f"--state-dir {args.state_dir} --timeout-seconds 5 --notification-worker-ack"
        )
    if args.format == "text":
        print(f"{args.host}/{args.job_id}: state={state}")
        print(f"evidence: {payload['evidence_strength'] or 'none'}")
        print(f"wake events enabled: {'yes' if payload['wake_events_enabled'] else 'no'}")
        print(f"guidance: {payload['guidance']}")
        for command in payload["next_commands"]:
            print(f"next: {command}")
    else:
        print(json.dumps(payload, sort_keys=True))
    return 0


def cleanup_command(args: argparse.Namespace) -> int:
    outbox = semantic_events.outbox_root(args.state_dir)
    older_than_seconds = max(0.0, args.older_than_days) * 86400.0
    from datetime import datetime, timezone

    removed = semantic_events.cleanup_outbox(
        outbox,
        now=datetime.now(timezone.utc),
        older_than_seconds=older_than_seconds,
        include_dead_letter=args.include_dead_letter,
        apply=args.yes,
    )
    payload = {
        "schema_version": "codex-monitor.cleanup/v1",
        "skill": "codex-hpc-monitor",
        "mode": "applied" if args.yes else "dry_run",
        "scope": "outbox only; run directories and terminal evidence are never touched",
        "removed_event_ids": removed,
        "removed_count": len(removed),
    }
    print(json.dumps(payload, sort_keys=True))
    return 0


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start", help="start one detached deterministic monitor")
    start.add_argument("job_id")
    start.add_argument("--host", default="hpc142")
    start.add_argument("--state-dir", type=Path, default=Path.home() / ".cache" / "codex-hpc-monitor")
    start.add_argument("--watcher-path", type=Path, default=Path(__file__).with_name("watch_slurm_job.py"))
    start.add_argument("--poll-seconds", type=positive_float, default=60.0)
    start.add_argument(
        "--pending-alert-seconds",
        type=nonnegative_float,
        default=0.0,
        help="stop on prolonged pending after this many seconds; 0 disables the stop",
    )
    start.add_argument("--terminal-observability-seconds", type=nonnegative_float, default=300.0)
    start.add_argument("--max-watch-seconds", type=nonnegative_float, default=604800.0)
    start.add_argument("--query-failures", type=int, default=3)
    start.add_argument("--expected-owner")
    start.add_argument("--expected-job-name")
    start.add_argument("--expected-partition")
    start.add_argument("--restart", action="store_true", help="start a new run after a prior terminal or lost supervisor")
    start.add_argument(
        "--allow-contract-change",
        action="store_true",
        help="explicitly accept a changed monitoring contract for this host/job",
    )
    start.add_argument(
        "--event-binding",
        type=Path,
        help="opt in to semantic wake events with a validated binding file",
    )
    start.add_argument(
        "--bridge-config",
        type=Path,
        help="bridge configuration the binding must agree with (optional)",
    )
    start.add_argument(
        "--require-auto-resume",
        action="store_true",
        help="fail before launch unless binding, enabled bridge config, and durable activation receipt are ready (does not prove daemon liveness)",
    )
    start.add_argument("--handshake-seconds", type=positive_float, default=10.0)
    start.set_defaults(func=start_monitor)

    status = sub.add_parser("status", help="read local artifacts without querying Slurm")
    status.add_argument("job_id")
    status.add_argument("--host", default="hpc142")
    status.add_argument("--state-dir", type=Path, default=Path.home() / ".cache" / "codex-hpc-monitor")
    status.add_argument("--require-terminal", action="store_true")
    status.set_defaults(func=status_command)

    wait = sub.add_parser("wait", help="block on local supervisor state without querying Slurm")
    wait.add_argument("job_id")
    wait.add_argument("--host", default="hpc142")
    wait.add_argument("--state-dir", type=Path, default=Path.home() / ".cache" / "codex-hpc-monitor")
    wait.add_argument("--timeout-seconds", type=positive_float, required=True)
    wait.add_argument("--poll-seconds", type=positive_float, default=1.0)
    wait.add_argument("--notification-worker-ack", action="store_true")
    wait.add_argument("--terminal-publication-grace-seconds", type=nonnegative_float, default=10.0)
    wait.set_defaults(func=wait_command)

    internal = sub.add_parser("_supervise", help=argparse.SUPPRESS)
    internal.add_argument("--run-dir", type=Path, required=True)
    internal.add_argument("--manifest-sha256", required=True)
    internal.add_argument("--lock-fd", type=int, required=True)
    internal.set_defaults(func=supervise)

    doctor = sub.add_parser("doctor", help="report mode capability and health")
    doctor.add_argument("--state-dir", type=Path, default=Path.home() / ".cache" / "codex-hpc-monitor")
    doctor.add_argument("--host", default="hpc142")
    doctor.add_argument("--bridge-config", type=Path)
    doctor.add_argument("--mode", choices=("auto", "unattended", "external-event-bridge"), default="auto")
    doctor.add_argument("--probe-app-server", action="store_true")
    doctor.add_argument("--format", choices=("text", "json"), default="json")
    doctor.set_defaults(func=doctor_command)

    listing = sub.add_parser("list", help="enumerate local monitors")
    listing.add_argument("--state-dir", type=Path, default=Path.home() / ".cache" / "codex-hpc-monitor")
    listing.set_defaults(func=list_command)

    explain = sub.add_parser("explain", help="explain one monitor in plain language")
    explain.add_argument("job_id")
    explain.add_argument("--host", default="hpc142")
    explain.add_argument("--state-dir", type=Path, default=Path.home() / ".cache" / "codex-hpc-monitor")
    explain.add_argument("--format", choices=("text", "json"), default="text")
    explain.set_defaults(func=explain_command)

    cleanup = sub.add_parser("cleanup", help="inspect or remove settled outbox events")
    cleanup.add_argument("--state-dir", type=Path, default=Path.home() / ".cache" / "codex-hpc-monitor")
    cleanup.add_argument("--older-than-days", type=float, default=7.0)
    cleanup.add_argument("--include-dead-letter", action="store_true")
    cleanup.add_argument("--yes", action="store_true", help="actually remove (default is dry-run)")
    cleanup.set_defaults(func=cleanup_command)
    return result


def main() -> int:
    args = parser().parse_args()
    if getattr(args, "query_failures", 1) < 1:
        raise SystemExit("--query-failures must be at least 1")
    try:
        return int(args.func(args))
    except (OSError, ValueError) as exc:
        print(json.dumps({"error": type(exc).__name__, "detail": str(exc)}, sort_keys=True))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

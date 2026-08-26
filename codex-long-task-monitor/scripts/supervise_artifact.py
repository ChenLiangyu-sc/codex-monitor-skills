#!/usr/bin/env python3
"""Start and inspect one detached deterministic artifact watcher."""

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
from typing import Any


SCHEMA_PREFIX = "codex-long-task-monitor.artifact"
HANDLE_RE = re.compile(r"^artifact_[0-9a-f]{32}$")
RUN_RE = re.compile(r"^run_[0-9]+_[0-9]+_[0-9a-f]{8}$")
WATCHER_OUTCOMES = {
    0: "condition_satisfied",
    3: "terminal_or_contract_failure",
    4: "deadline_exceeded",
    5: "artifact_invalid",
}
NETWORK_FILESYSTEMS = {
    "9p",
    "afs",
    "ceph",
    "cifs",
    "fuse.ceph",
    "fuse.glusterfs",
    "fuse.sshfs",
    "glusterfs",
    "lustre",
    "nfs",
    "nfs4",
    "smb3",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(payload: object) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fsync_directory(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ValueError(f"unsafe state directory: {path}")
    os.chmod(path, 0o700)


def write_bytes_exclusive(path: Path, payload: bytes, mode: int = 0o600) -> None:
    ensure_private_directory(path.parent)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while publishing controlled artifact")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    fsync_directory(path.parent)


def write_temp(path: Path, payload: object) -> Path:
    ensure_private_directory(path.parent)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    write_bytes_exclusive(temp, canonical_json(payload))
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


def read_json(path: Path) -> dict[str, Any]:
    try:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def read_regular_bytes_no_follow(path: Path) -> bytes:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(str(path), flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError(f"not a regular file: {path}")
        chunks = []
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            chunks.append(block)
        return b"".join(chunks)
    finally:
        os.close(fd)


def boot_id() -> str | None:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except OSError:
        return None


def decode_mount_field(value: str) -> str:
    replacements = {"\\040": " ", "\\011": "\t", "\\012": "\n", "\\134": "\\"}
    for encoded, decoded in replacements.items():
        value = value.replace(encoded, decoded)
    return value


def filesystem_type(path: Path) -> str | None:
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    resolved = probe.resolve(strict=True)
    best: tuple[int, str] | None = None
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
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


def validate_local_state_root(path: Path) -> None:
    kind = filesystem_type(path)
    if kind is None:
        raise ValueError("cannot determine state directory filesystem type")
    if kind.lower() in NETWORK_FILESYSTEMS:
        raise ValueError(f"state directory must use local storage, not {kind}")


def process_start_ticks(pid: int) -> str | None:
    try:
        value = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        return value[value.rfind(")") + 2 :].split()[19]
    except (OSError, IndexError, ValueError):
        return None


def process_matches(pid: object, ticks: object, expected_boot: object) -> bool:
    return (
        isinstance(pid, int)
        and pid > 1
        and isinstance(ticks, str)
        and bool(ticks)
        and isinstance(expected_boot, str)
        and expected_boot == boot_id()
        and process_start_ticks(pid) == ticks
    )


def normalized_repeat(values: list[str]) -> list[str]:
    return sorted(set(values))


def contract_from_args(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "artifact_path": str(args.path.resolve(strict=False)),
        "exists_is_success": args.exists_is_success,
        "json_field": args.json_field,
        "success_json": normalized_repeat(args.success_json),
        "failure_json": normalized_repeat(args.failure_json),
        "expect_json": normalized_repeat(args.expect_json),
        "require_nonempty": normalized_repeat(args.require_nonempty),
        "not_before_epoch_seconds": args.not_before_epoch_seconds,
        "min_bytes": args.min_bytes,
        "max_json_bytes": args.max_json_bytes,
        "poll_seconds": args.poll_seconds,
        "timeout_seconds": args.timeout_seconds,
        "invalid_grace_seconds": args.invalid_grace_seconds,
    }


def identity_from_contract(contract: dict[str, Any]) -> dict[str, Any]:
    return {
        key: contract[key]
        for key in (
            "artifact_path",
            "exists_is_success",
            "json_field",
            "expect_json",
            "not_before_epoch_seconds",
        )
    }


def task_handle(contract: dict[str, Any]) -> str:
    return f"artifact_{sha256_bytes(canonical_json(identity_from_contract(contract)))[:32]}"


def base_dir(state_dir: Path, handle: str) -> Path:
    return state_dir / "artifacts" / handle


def current_run(base: Path) -> Path | None:
    current = read_json(base / "current.json")
    run_id = current.get("run_id")
    if not isinstance(run_id, str) or not RUN_RE.fullmatch(run_id):
        return None
    return base / "runs" / run_id


def validate_manifest(run: Path, handle: str) -> tuple[dict[str, Any], str]:
    manifest_path = run / "manifest.json"
    manifest = read_json(manifest_path)
    started = read_json(run / "supervisor_started.json")
    current = read_json(run.parent.parent / "current.json")
    manifest_sha = started.get("manifest_sha256") or current.get("manifest_sha256")
    if (
        not manifest
        or manifest.get("schema_version") != f"{SCHEMA_PREFIX}.manifest/v1"
        or manifest.get("run_id") != run.name
        or manifest.get("task_handle") != handle
        or not isinstance(manifest_sha, str)
        or sha256_file(manifest_path) != manifest_sha
    ):
        raise ValueError("manifest_unverified")
    return manifest, manifest_sha


def run_status(run: Path | None, handle: str) -> dict[str, Any]:
    base = {
        "schema_version": f"{SCHEMA_PREFIX}.status/v1",
        "task_handle": handle,
    }
    if run is None:
        return {**base, "state": "not_started"}
    try:
        manifest, manifest_sha = validate_manifest(run, handle)
    except (OSError, ValueError):
        return {**base, "state": "verification_failed", "run_id": run.name, "run_dir": str(run)}

    terminal = read_json(run / "terminal.json")
    if terminal:
        child_exit = read_json(run / "child_exit.json")
        launch_failed = terminal.get("observer_state") == "launch_failed"
        if launch_failed:
            expected_outcome = "supervisor_failure"
        elif terminal.get("watcher_signal") is not None:
            expected_outcome = "watcher_signaled"
        else:
            expected_outcome = WATCHER_OUTCOMES.get(
                terminal.get("watcher_exit_code"), "watcher_infrastructure_failure"
            )
        child_evidence_matches = launch_failed or (
            child_exit.get("schema_version") == f"{SCHEMA_PREFIX}.child-exit/v1"
            and child_exit.get("task_handle") == handle
            and child_exit.get("run_id") == run.name
            and child_exit.get("exit_code") == terminal.get("watcher_exit_code")
            and child_exit.get("signal") == terminal.get("watcher_signal")
        )
        verified = (
            terminal.get("schema_version") == f"{SCHEMA_PREFIX}.terminal/v1"
            and terminal.get("task_handle") == handle
            and terminal.get("run_id") == run.name
            and terminal.get("manifest_sha256") == manifest_sha
            and terminal.get("contract_digest") == manifest.get("contract_digest")
            and terminal.get("generation") == manifest.get("generation")
            and terminal.get("scope") == "artifact_observation_only"
            and terminal.get("business_verdict") == "pending"
            and terminal.get("observer_outcome") == expected_outcome
            and child_evidence_matches
        )
        return {
            **base,
            "state": "terminal" if verified else "verification_failed",
            "run_id": run.name,
            "run_dir": str(run),
            "contract_digest": manifest.get("contract_digest"),
            "terminal_verified": verified,
            "terminal_sha256": sha256_file(run / "terminal.json") if verified else None,
            "terminal": terminal if verified else None,
        }

    started = read_json(run / "supervisor_started.json")
    runtime = read_json(run / "runtime.json")
    child_exit = read_json(run / "child_exit.json")
    supervisor_alive = process_matches(
        started.get("pid"), started.get("pid_start_ticks"), started.get("boot_id")
    )
    watcher_alive = process_matches(
        runtime.get("pid"), runtime.get("pid_start_ticks"), runtime.get("boot_id")
    )
    if supervisor_alive:
        state = "active"
    elif child_exit:
        state = "exit_observed_terminal_missing"
    elif started:
        state = "supervisor_lost"
    else:
        state = "launch_unconfirmed"
    return {
        **base,
        "state": state,
        "run_id": run.name,
        "run_dir": str(run),
        "contract_digest": manifest.get("contract_digest"),
        "supervisor_alive": supervisor_alive,
        "watcher_alive": watcher_alive,
        "supervisor": started or None,
        "runtime": runtime or None,
        "child_exit": child_exit or None,
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


def watcher_argv(run: Path, contract: dict[str, Any]) -> list[str]:
    command = [
        sys.executable,
        str(run / "watch_artifact_frozen.py"),
        contract["artifact_path"],
        "--poll-seconds",
        str(contract["poll_seconds"]),
        "--timeout-seconds",
        str(contract["timeout_seconds"]),
        "--invalid-grace-seconds",
        str(contract["invalid_grace_seconds"]),
        "--min-bytes",
        str(contract["min_bytes"]),
        "--max-json-bytes",
        str(contract["max_json_bytes"]),
        "--json-field",
        contract["json_field"],
    ]
    if contract["not_before_epoch_seconds"] is not None:
        command.extend(["--not-before-epoch-seconds", str(contract["not_before_epoch_seconds"])])
    if contract["exists_is_success"]:
        command.append("--exists-is-success")
    for key, option in (
        ("success_json", "--success-json"),
        ("failure_json", "--failure-json"),
        ("expect_json", "--expect-json"),
        ("require_nonempty", "--require-nonempty"),
    ):
        for value in contract[key]:
            command.extend([option, value])
    return command


def launch_handshake_confirmed(status_payload: dict[str, Any]) -> bool:
    """Require either a durable terminal or a verified live watcher."""
    if status_payload.get("state") == "terminal":
        return True
    return status_payload.get("state") == "active" and status_payload.get("watcher_alive") is True


def start_monitor(args: argparse.Namespace) -> int:
    watcher_input = args.watcher_path.expanduser()
    if watcher_input.is_symlink():
        raise ValueError(f"watcher path must not be a symlink: {watcher_input}")
    watcher = watcher_input.resolve(strict=True)
    if not watcher.is_file():
        raise ValueError(f"watcher is not a regular file: {watcher}")
    if not args.path.is_absolute():
        raise ValueError("artifact path must be absolute")

    contract = contract_from_args(args)
    contract_digest = sha256_bytes(canonical_json(contract))
    handle = task_handle(contract)
    ensure_private_directory(args.state_dir)
    base = base_dir(args.state_dir, handle)
    try:
        lock_fd = open_lifetime_lock(base)
    except BlockingIOError:
        payload = run_status(current_run(base), handle)
        if payload.get("contract_digest") not in {None, contract_digest}:
            payload["start_result"] = "contract_conflict"
            print(json.dumps(payload, sort_keys=True))
            return 12
        payload["start_result"] = "already_active"
        print(json.dumps(payload, sort_keys=True))
        return 2

    try:
        previous = run_status(current_run(base), handle)
        previous_digest = previous.get("contract_digest")
        if previous_digest is not None and previous_digest != contract_digest:
            previous["start_result"] = "contract_conflict"
            print(json.dumps(previous, sort_keys=True))
            return 12
        if previous["state"] != "not_started" and not args.restart:
            previous["start_result"] = "restart_required"
            print(json.dumps(previous, sort_keys=True))
            return 3

        previous_current = read_json(base / "current.json")
        generation = int(previous_current.get("generation", 0)) + 1
        run_id = f"run_{int(time.time())}_{os.getpid()}_{secrets.token_hex(4)}"
        run = base / "runs" / run_id
        ensure_private_directory(run)
        watcher_bytes = read_regular_bytes_no_follow(watcher)
        frozen = run / "watch_artifact_frozen.py"
        write_bytes_exclusive(frozen, watcher_bytes, 0o500)
        command = watcher_argv(run, contract)
        manifest = {
            "schema_version": f"{SCHEMA_PREFIX}.manifest/v1",
            "task_handle": handle,
            "run_id": run_id,
            "generation": generation,
            "created_at": utc_now(),
            "contract": contract,
            "contract_digest": contract_digest,
            "watcher_argv": command,
            "watcher_sha256": sha256_bytes(watcher_bytes),
            "scope": "artifact_observation_only",
            "business_verdict": "pending",
        }
        publish_json_no_replace(run / "manifest.json", manifest)
        manifest_sha = sha256_file(run / "manifest.json")
        replace_json(
            base / "current.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}.current/v1",
                "task_handle": handle,
                "run_id": run_id,
                "generation": generation,
                "contract_digest": contract_digest,
                "manifest_sha256": manifest_sha,
                "updated_at": utc_now(),
            },
        )
        os.ftruncate(lock_fd, 0)
        os.write(lock_fd, canonical_json({"run_id": run_id, "launcher_pid": os.getpid()}))
        os.fsync(lock_fd)

        log_fd = os.open(
            str(run / "supervisor.controlled.log"),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
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
                stdout=log_fd,
                stderr=log_fd,
                close_fds=True,
                pass_fds=(lock_fd,),
                start_new_session=True,
            )
        finally:
            os.close(log_fd)
        os.close(lock_fd)
        lock_fd = -1

        deadline = time.monotonic() + args.handshake_seconds
        while time.monotonic() < deadline:
            status_payload = run_status(run, handle)
            if launch_handshake_confirmed(status_payload):
                status_payload["start_result"] = "started"
                print(json.dumps(status_payload, sort_keys=True))
                return 0
            if child.poll() is not None:
                break
            time.sleep(0.05)
        status_payload = run_status(run, handle)
        status_payload["start_result"] = "handshake_failed"
        print(json.dumps(status_payload, sort_keys=True))
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


def validate_frozen_watcher(manifest: dict[str, Any]) -> None:
    argv = manifest.get("watcher_argv")
    expected_sha = manifest.get("watcher_sha256")
    if not isinstance(argv, list) or len(argv) < 2 or not isinstance(argv[1], str):
        raise ValueError("manifest watcher argv is invalid")
    watcher = Path(argv[1])
    info = watcher.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ValueError("frozen watcher is not a regular non-symlink file")
    if not isinstance(expected_sha, str) or sha256_file(watcher) != expected_sha:
        raise ValueError("frozen watcher hash mismatch")


def supervise(args: argparse.Namespace) -> int:
    run = args.run_dir.resolve()
    lock_fd = args.lock_fd
    child: subprocess.Popen[bytes] | None = None
    started_monotonic = time.monotonic()
    try:
        manifest_path = run / "manifest.json"
        if sha256_file(manifest_path) != args.manifest_sha256:
            raise ValueError("manifest hash mismatch")
        manifest = read_json(manifest_path)
        if manifest.get("run_id") != run.name:
            raise ValueError("manifest run identity mismatch")
        validate_frozen_watcher(manifest)
        started = {
            "schema_version": f"{SCHEMA_PREFIX}.supervisor-started/v1",
            "task_handle": manifest["task_handle"],
            "run_id": run.name,
            "pid": os.getpid(),
            "pid_start_ticks": process_start_ticks(os.getpid()),
            "boot_id": boot_id(),
            "started_at": utc_now(),
            "manifest_sha256": args.manifest_sha256,
        }
        publish_json_no_replace(run / "supervisor_started.json", started)

        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        requested_signal: dict[str, int | None] = {"value": None}

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
            str(run / "watcher.controlled.stdout"),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        stderr_fd = os.open(
            str(run / "watcher.controlled.stderr"),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
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
                "task_handle": manifest["task_handle"],
                "run_id": run.name,
                "pid": child.pid,
                "pid_start_ticks": process_start_ticks(child.pid),
                "boot_id": boot_id(),
                "started_at": utc_now(),
            },
        )
        return_code = child.wait()
        exit_code = return_code if return_code >= 0 else None
        signal_number = -return_code if return_code < 0 else None
        child_exit = {
            "schema_version": f"{SCHEMA_PREFIX}.child-exit/v1",
            "task_handle": manifest["task_handle"],
            "run_id": run.name,
            "observed_at": utc_now(),
            "exit_code": exit_code,
            "signal": signal_number,
            "requested_supervisor_signal": requested_signal["value"],
        }
        publish_json_no_replace(run / "child_exit.json", child_exit)
        if signal_number is not None:
            outcome = "watcher_signaled"
        else:
            outcome = WATCHER_OUTCOMES.get(exit_code, "watcher_infrastructure_failure")
        terminal = {
            "schema_version": f"{SCHEMA_PREFIX}.terminal/v1",
            "task_handle": manifest["task_handle"],
            "run_id": run.name,
            "generation": manifest["generation"],
            "scope": "artifact_observation_only",
            "business_verdict": "pending",
            "observer_state": "exited",
            "observer_outcome": outcome,
            "watcher_exit_code": exit_code,
            "watcher_signal": signal_number,
            "started_at": started["started_at"],
            "ended_at": utc_now(),
            "duration_monotonic_ms": int((time.monotonic() - started_monotonic) * 1000),
            "manifest_sha256": args.manifest_sha256,
            "contract_digest": manifest["contract_digest"],
        }
        publish_json_no_replace(run / "terminal.json", terminal)
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
                manifest = read_json(run / "manifest.json")
                publish_json_no_replace(
                    run / "terminal.json",
                    {
                        "schema_version": f"{SCHEMA_PREFIX}.terminal/v1",
                        "task_handle": manifest.get("task_handle"),
                        "run_id": run.name,
                        "generation": manifest.get("generation"),
                        "scope": "artifact_observation_only",
                        "business_verdict": "pending",
                        "observer_state": "launch_failed",
                        "observer_outcome": "supervisor_failure",
                        "failure_type": type(exc).__name__,
                        "ended_at": utc_now(),
                        "manifest_sha256": args.manifest_sha256,
                        "contract_digest": manifest.get("contract_digest"),
                    },
                )
            except Exception:
                pass
        return 12
    finally:
        os.close(lock_fd)


def status_exit_code(payload: dict[str, Any], require_terminal: bool) -> int:
    if not require_terminal:
        return 0
    state = payload.get("state")
    if state == "terminal":
        terminal = payload.get("terminal") or {}
        return 0 if terminal.get("observer_outcome") == "condition_satisfied" else 3
    if state == "active":
        return 10
    if state == "supervisor_lost":
        return 11
    return 12


def status_command(args: argparse.Namespace) -> int:
    if not HANDLE_RE.fullmatch(args.task_handle):
        raise ValueError("invalid artifact task handle")
    payload = run_status(current_run(base_dir(args.state_dir, args.task_handle)), args.task_handle)
    print(json.dumps(payload, sort_keys=True))
    return status_exit_code(payload, args.require_terminal)


def wait_command(args: argparse.Namespace) -> int:
    if not args.notification_worker_ack:
        raise ValueError(
            "wait requires --notification-worker-ack; the flag acknowledges the "
            "notification-worker contract but does not authenticate a model role"
        )
    if not HANDLE_RE.fullmatch(args.task_handle):
        raise ValueError("invalid artifact task handle")
    deadline = time.monotonic() + args.timeout_seconds
    while True:
        payload = run_status(
            current_run(base_dir(args.state_dir, args.task_handle)), args.task_handle
        )
        if payload.get("state") != "active":
            print(json.dumps(payload, sort_keys=True))
            return status_exit_code(payload, True)
        if time.monotonic() >= deadline:
            print(
                json.dumps(
                    {
                        "schema_version": f"{SCHEMA_PREFIX}.wait/v1",
                        "task_handle": args.task_handle,
                        "state": "wait_timeout",
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


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


def add_contract_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("path", type=Path, help="absolute path to the terminal artifact")
    parser.add_argument("--poll-seconds", type=positive_float, default=30.0)
    parser.add_argument("--timeout-seconds", type=positive_float, required=True)
    parser.add_argument("--invalid-grace-seconds", type=nonnegative_float, default=10.0)
    parser.add_argument("--min-bytes", type=nonnegative_int, default=1)
    parser.add_argument("--max-json-bytes", type=positive_int, default=8 * 1024 * 1024)
    parser.add_argument("--not-before-epoch-seconds", type=nonnegative_float)
    parser.add_argument("--exists-is-success", action="store_true")
    parser.add_argument("--json-field", default="status")
    parser.add_argument("--success-json", action="append", default=[])
    parser.add_argument("--failure-json", action="append", default=[])
    parser.add_argument("--expect-json", action="append", default=[])
    parser.add_argument("--require-nonempty", action="append", default=[])


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start", help="start one detached artifact monitor")
    add_contract_options(start)
    start.add_argument(
        "--state-dir",
        type=Path,
        default=Path.home() / ".cache" / "codex-long-task-monitor",
    )
    start.add_argument(
        "--watcher-path",
        type=Path,
        default=Path(__file__).with_name("watch_artifact.py"),
    )
    start.add_argument("--restart", action="store_true")
    start.add_argument("--handshake-seconds", type=positive_float, default=10.0)
    start.set_defaults(func=start_monitor)

    status = sub.add_parser("status", help="read local monitor state without opening the artifact")
    status.add_argument("task_handle")
    status.add_argument(
        "--state-dir",
        type=Path,
        default=Path.home() / ".cache" / "codex-long-task-monitor",
    )
    status.add_argument("--require-terminal", action="store_true")
    status.set_defaults(func=status_command)

    wait = sub.add_parser("wait", help="block on local supervisor state without opening the artifact")
    wait.add_argument("task_handle")
    wait.add_argument(
        "--state-dir",
        type=Path,
        default=Path.home() / ".cache" / "codex-long-task-monitor",
    )
    wait.add_argument("--timeout-seconds", type=positive_float, required=True)
    wait.add_argument("--poll-seconds", type=positive_float, default=1.0)
    wait.add_argument("--notification-worker-ack", action="store_true")
    wait.set_defaults(func=wait_command)

    internal = sub.add_parser("_supervise", help=argparse.SUPPRESS)
    internal.add_argument("--run-dir", type=Path, required=True)
    internal.add_argument("--manifest-sha256", required=True)
    internal.add_argument("--lock-fd", type=int, required=True)
    internal.set_defaults(func=supervise)
    return result


def validate_start_contract(args: argparse.Namespace) -> None:
    if args.exists_is_success:
        if any((args.success_json, args.failure_json, args.expect_json, args.require_nonempty)):
            raise ValueError("--exists-is-success cannot be combined with JSON contract options")
    elif not args.success_json or not args.failure_json:
        raise ValueError("JSON mode requires at least one --success-json and --failure-json")
    success = [parse_json_literal(value, "--success-json") for value in args.success_json]
    failure = [parse_json_literal(value, "--failure-json") for value in args.failure_json]
    if any(json_values_equal(left, right) for left in success for right in failure):
        raise ValueError("success and failure JSON values must not overlap")
    for value in args.expect_json:
        field, separator, literal = value.partition("=")
        if not separator or not field.strip():
            raise ValueError(f"--expect-json must use FIELD=JSON_LITERAL: {value!r}")
        parse_json_literal(literal, "--expect-json")


def parse_json_literal(raw: str, option: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{option} value must be valid JSON: {raw!r}") from exc


def json_values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, (int, float)) or isinstance(right, (int, float)):
        return (
            isinstance(left, (int, float))
            and not isinstance(left, bool)
            and isinstance(right, (int, float))
            and not isinstance(right, bool)
            and left == right
        )
    if isinstance(left, list) or isinstance(right, list):
        return (
            isinstance(left, list)
            and isinstance(right, list)
            and len(left) == len(right)
            and all(json_values_equal(a, b) for a, b in zip(left, right))
        )
    if isinstance(left, dict) or isinstance(right, dict):
        return (
            isinstance(left, dict)
            and isinstance(right, dict)
            and left.keys() == right.keys()
            and all(json_values_equal(left[key], right[key]) for key in left)
        )
    return type(left) is type(right) and left == right


def main() -> int:
    args = parser().parse_args()
    try:
        if hasattr(args, "state_dir"):
            args.state_dir = args.state_dir.expanduser().resolve()
            validate_local_state_root(args.state_dir)
        if args.command == "start":
            validate_start_contract(args)
        return int(args.func(args))
    except (OSError, ValueError) as exc:
        print(json.dumps({"error": type(exc).__name__, "detail": str(exc)}, sort_keys=True))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Single-instance notification-worker bridge for local Slurm monitor state."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PREFIX = "codex-hpc-monitor.bridge"
JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")
HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
RUN_ID_RE = re.compile(r"^run_[A-Za-z0-9_-]+$")
WAIT_EXIT_CODES = {0, 3, 4, 11, 12}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"unsafe bridge directory: {path}")
    os.chmod(path, 0o700)


def atomic_replace(path: Path, value: object) -> None:
    ensure_private_dir(path.parent)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, canonical_json(value))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temp, path)
    os.chmod(path, 0o600)


def publish_once(path: Path, value: object) -> None:
    ensure_private_dir(path.parent)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, canonical_json(value))
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        os.link(temp, path, follow_symlinks=False)
        os.chmod(path, 0o600)
    finally:
        temp.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    try:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def process_start_ticks(pid: int) -> str | None:
    try:
        value = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        return value[value.rfind(")") + 2 :].split()[19]
    except (OSError, IndexError, ValueError):
        return None


def process_matches(runtime: dict[str, Any]) -> bool:
    pid, ticks = runtime.get("pid"), runtime.get("pid_start_ticks")
    return isinstance(pid, int) and isinstance(ticks, str) and process_start_ticks(pid) == ticks


def validate_identity(job_id: str, host: str) -> None:
    if not JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job_id")
    if not HOST_RE.fullmatch(host):
        raise ValueError("invalid host")


def supervisor_command(args: argparse.Namespace, command: str, *extra: str) -> list[str]:
    return [
        sys.executable,
        str(args.supervisor_path),
        command,
        args.job_id,
        "--host",
        args.host,
        "--state-dir",
        str(args.state_dir),
        *extra,
    ]


def call_json(command: list[str], timeout: float = 30.0) -> tuple[int, dict[str, Any], str]:
    result = subprocess.run(command, text=True, capture_output=True, timeout=timeout, check=False)
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    payload = json.loads(lines[-1]) if lines else {}
    return result.returncode, payload if isinstance(payload, dict) else {}, result.stderr


def resolve_monitor(args: argparse.Namespace) -> dict[str, Any]:
    code, payload, _ = call_json(supervisor_command(args, "status"))
    run_id = payload.get("run_id")
    if code != 0 or payload.get("state") not in {"active", "terminal"}:
        raise RuntimeError(f"monitor is not active or terminal: {payload.get('state')}")
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise RuntimeError("monitor run_id is missing or invalid")
    return payload


def bridge_dir(args: argparse.Namespace, run_id: str) -> Path:
    return args.state_dir / "bridges" / f"{args.host}-{args.job_id}" / run_id


def bridge_attempts(path: Path) -> list[dict[str, Any]]:
    attempts_dir = path / "attempts"
    records: list[dict[str, Any]] = []
    if not attempts_dir.is_dir():
        return records
    for child in sorted(attempts_dir.iterdir()):
        record = read_json(child)
        if record:
            records.append(record)
    return records


def bridge_status_for(path: Path, host: str, job_id: str, run_id: str) -> dict[str, Any]:
    receipt = read_json(path / "receipt.json")
    runtime = read_json(path / "runtime.json")
    manifest = read_json(path / "manifest.json")
    attempts = bridge_attempts(path)
    if receipt:
        state = "terminal"
    elif process_matches(runtime):
        state = "active"
    elif runtime or manifest or attempts:
        state = "bridge_lost"
    else:
        state = "not_started"
    return {
        "schema_version": f"{PREFIX}.status/v1",
        "state": state,
        "host": host,
        "job_id": job_id,
        "run_id": run_id,
        "bridge_dir": str(path),
        "runtime": runtime or None,
        "receipt": receipt or None,
        "attempts_total": len(attempts),
        "last_attempt": attempts[-1] if attempts else None,
    }


def run_bridge(args: argparse.Namespace) -> int:
    if not args.notification_worker_ack:
        raise ValueError(
            "run requires --notification-worker-ack; the flag acknowledges the "
            "notification-worker contract but does not authenticate a model role"
        )
    validate_identity(args.job_id, args.host)
    supervisor = args.supervisor_path.expanduser()
    if supervisor.is_symlink():
        raise ValueError("supervisor path must not be a symlink")
    args.supervisor_path = supervisor.resolve(strict=True)
    args.state_dir = args.state_dir.expanduser().resolve()
    monitor = resolve_monitor(args)
    run_id = monitor["run_id"]
    path = bridge_dir(args, run_id)
    ensure_private_dir(path)
    lock_fd = os.open(path / "bridge.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            payload = bridge_status_for(path, args.host, args.job_id, run_id)
            payload["run_result"] = "already_active"
            print(json.dumps(payload, sort_keys=True))
            return 2
        existing = read_json(path / "receipt.json")
        if existing:
            payload = bridge_status_for(path, args.host, args.job_id, run_id)
            payload["run_result"] = "receipt_exists"
            print(json.dumps(payload, sort_keys=True))
            return 3

        manifest = {
            "schema_version": f"{PREFIX}.manifest/v1",
            "host": args.host,
            "job_id": args.job_id,
            "run_id": run_id,
            "created_at": utc_now(),
            "supervisor_path": str(args.supervisor_path),
            "supervisor_sha256": sha256(args.supervisor_path),
            "timeout_seconds": args.timeout_seconds,
            "poll_seconds": args.poll_seconds,
            "scope": "local_terminal_notification_only",
            "project_gate_evaluated": False,
            "notification_worker_acknowledged": True,
        }
        manifest_path = path / "manifest.json"
        if manifest_path.exists():
            prior = read_json(manifest_path)
            for key in (
                "host",
                "job_id",
                "run_id",
                "supervisor_sha256",
                "notification_worker_acknowledged",
            ):
                if prior.get(key) != manifest.get(key):
                    raise RuntimeError(f"existing bridge manifest mismatch: {key}")
            manifest = prior
        else:
            publish_once(manifest_path, manifest)
        started_at = utc_now()
        atomic_replace(
            path / "runtime.json",
            {
                "schema_version": f"{PREFIX}.runtime/v1",
                "pid": os.getpid(),
                "pid_start_ticks": process_start_ticks(os.getpid()),
                "started_at": started_at,
                "run_id": run_id,
            },
        )
        command = supervisor_command(
            args,
            "wait",
            "--timeout-seconds",
            str(args.timeout_seconds),
            "--poll-seconds",
            str(args.poll_seconds),
            "--notification-worker-ack",
        )
        wait_code, wait_payload, wait_stderr = call_json(
            command, timeout=args.timeout_seconds + args.subprocess_grace_seconds
        )
        problems: list[str] = []
        if wait_code not in WAIT_EXIT_CODES:
            problems.append("unexpected_wait_exit_code")
        if wait_payload.get("schema_version") != "codex-hpc-monitor.wait/v1":
            problems.append("wait_schema_mismatch")
        for key, expected in (("host", args.host), ("job_id", args.job_id), ("run_id", run_id)):
            if wait_payload.get(key) != expected:
                problems.append(f"wait_{key}_mismatch")
        attempt_id = f"attempt_{int(time.time())}_{os.getpid()}_{secrets.token_hex(4)}"
        attempt = {
            "schema_version": f"{PREFIX}.attempt/v1",
            "attempt_id": attempt_id,
            "host": args.host,
            "job_id": args.job_id,
            "run_id": run_id,
            "started_at": started_at,
            "ended_at": utc_now(),
            "wait_exit_code": wait_code,
            "outcome": (
                "bridge_failure"
                if problems
                else ("wait_timeout" if wait_code == 4 else "terminal")
            ),
            "wait_stderr_present": bool(wait_stderr),
            "problems": problems,
        }
        publish_once(path / "attempts" / f"{attempt_id}.json", attempt)
        if wait_code == 4 and not problems:
            # A bridge wait timeout is one attempt outcome, never a permanent
            # terminal notification: a later run may still deliver the genuine
            # verified terminal event, and no receipt is published here.
            print(json.dumps(attempt, sort_keys=True))
            return 4
        receipt = {
            "schema_version": f"{PREFIX}.receipt/v1",
            "state": "terminal" if not problems else "bridge_failure",
            "host": args.host,
            "job_id": args.job_id,
            "run_id": run_id,
            "started_at": started_at,
            "ended_at": utc_now(),
            "attempt_id": attempt_id,
            "wait_exit_code": wait_code,
            "wait_payload": wait_payload,
            "wait_stderr_present": bool(wait_stderr),
            "problems": problems,
            "manifest_sha256": sha256(manifest_path),
            "scope": "local_terminal_notification_only",
            "project_gate_evaluated": False,
        }
        publish_once(path / "receipt.json", receipt)
        print(json.dumps(receipt, sort_keys=True))
        return 12 if problems else wait_code
    finally:
        os.close(lock_fd)


def status_bridge(args: argparse.Namespace) -> int:
    validate_identity(args.job_id, args.host)
    args.supervisor_path = args.supervisor_path.expanduser().resolve(strict=True)
    args.state_dir = args.state_dir.expanduser().resolve()
    try:
        monitor = resolve_monitor(args)
    except Exception as exc:
        print(json.dumps({
            "schema_version": f"{PREFIX}.status/v1",
            "state": "monitor_unavailable",
            "host": args.host,
            "job_id": args.job_id,
            "detail": type(exc).__name__,
        }, sort_keys=True))
        return 12
    payload = bridge_status_for(bridge_dir(args, monitor["run_id"]), args.host, args.job_id, monitor["run_id"])
    print(json.dumps(payload, sort_keys=True))
    return 0


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    for name in ("run", "status"):
        command = sub.add_parser(name)
        command.add_argument("job_id")
        command.add_argument("--host", default="hpc142")
        command.add_argument("--state-dir", type=Path, default=Path.home() / ".cache" / "codex-hpc-monitor")
        command.add_argument("--supervisor-path", type=Path, default=Path(__file__).with_name("supervise_slurm_job.py"))
        if name == "run":
            command.add_argument(
                "--notification-worker-ack",
                action="store_true",
                help=(
                    "acknowledge notification-worker-only use; this is a misuse guard, "
                    "not role authentication"
                ),
            )
            command.add_argument("--timeout-seconds", type=positive_float, required=True)
            command.add_argument("--poll-seconds", type=positive_float, default=1.0)
            command.add_argument("--subprocess-grace-seconds", type=positive_float, default=30.0)
            command.set_defaults(func=run_bridge)
        else:
            command.set_defaults(func=status_bridge)
    return result


def main() -> int:
    try:
        args = parser().parse_args()
        return int(args.func(args))
    except Exception as exc:
        print(json.dumps({
            "schema_version": f"{PREFIX}.error/v1",
            "state": "bridge_error",
            "error_type": type(exc).__name__,
            "detail": str(exc),
        }, sort_keys=True))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

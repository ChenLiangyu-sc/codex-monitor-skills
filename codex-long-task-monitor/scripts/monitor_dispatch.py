#!/usr/bin/env python3
"""Start and inspect a compact deterministic monitor for one Codex dispatch."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any


SCHEMA = "codex-long-task-monitor.dispatch-wrapper/v1"
BINDING_SCHEMA = "codex-long-task-monitor.dispatch-binding/v1"
DISPATCH_MANIFEST_SCHEMA = "codex-task-dispatch.manifest/v3"
DISPATCH_TERMINAL_SCHEMA = "codex-task-dispatch.terminal/v3"
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_BINDING_BYTES = 64 * 1024
MAX_HELPER_OUTPUT_BYTES = 2 * 1024 * 1024
FAILURE_OUTCOMES = ("exit_nonzero", "signaled", "not_started", "contract_violation", "unknown")
HANDLE_RE = re.compile(r"^artifact_[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_STATE_DIR = Path.home() / ".cache" / "codex-long-task-monitor"
DEFAULT_DISPATCH_SUPERVISOR = (
    Path.home()
    / ".codex"
    / "skills"
    / "codex-task-dispatch"
    / "scripts"
    / "dispatch_supervisor.py"
)


class WrapperError(ValueError):
    pass


class DispatchVerificationError(WrapperError):
    pass


def read_regular_stable(path: Path, limit: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise WrapperError("invalid controlled file")
        chunks = []
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(fd)
        fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_size", "st_mtime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in fields):
            raise WrapperError("controlled file changed while reading")
        payload = b"".join(chunks)
        if len(payload) != before.st_size:
            raise WrapperError("controlled file changed while reading")
        return payload
    finally:
        os.close(fd)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def dispatch_identity(dispatch_directory: Path) -> tuple[Path, Path, str, str]:
    lexical = Path(os.path.abspath(dispatch_directory))
    metadata = lexical.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise WrapperError("dispatch directory must be a real directory")
    manifest_bytes = read_regular_stable(lexical / "dispatch_manifest.json", MAX_MANIFEST_BYTES)
    try:
        manifest = json.loads(manifest_bytes)
    except json.JSONDecodeError as exc:
        raise WrapperError("invalid dispatch manifest") from exc
    if not isinstance(manifest, dict) or manifest.get("schema_version") != DISPATCH_MANIFEST_SCHEMA:
        raise WrapperError("invalid dispatch manifest")
    handle = manifest.get("dispatch_handle")
    if not isinstance(handle, str) or not handle:
        raise WrapperError("invalid dispatch handle")
    return lexical, lexical / "dispatch_terminal.json", handle, sha256_bytes(manifest_bytes)


def state_directory(value: Path | None) -> Path:
    return Path(os.path.abspath(value if value is not None else DEFAULT_STATE_DIR))


def dispatch_supervisor_identity(path: Path) -> tuple[Path, str]:
    lexical = Path(os.path.abspath(path))
    if lexical.is_symlink() or not lexical.is_file():
        raise WrapperError("dispatch supervisor is invalid")
    payload = read_regular_stable(lexical, MAX_MANIFEST_BYTES)
    return lexical, sha256_bytes(payload)


def binding_path(root: Path, monitor_handle: str) -> Path:
    if HANDLE_RE.fullmatch(monitor_handle) is None:
        raise WrapperError("invalid monitor task handle")
    return root / "dispatch-wrapper-bindings" / f"{monitor_handle}.json"


def ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise WrapperError("monitor binding directory is invalid")
    os.chmod(path, 0o700)


def persist_binding(root: Path, monitor_handle: str, binding: dict[str, Any]) -> None:
    path = binding_path(root, monitor_handle)
    ensure_private_directory(path.parent)
    payload = (json.dumps(binding, sort_keys=True, separators=(",", ":")) + "\n").encode()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError:
        if json.loads(read_regular_stable(path, MAX_BINDING_BYTES)) != binding:
            raise WrapperError("monitor binding conflict")
        return
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while publishing monitor binding")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def load_binding(root: Path, monitor_handle: str) -> dict[str, Any]:
    try:
        value = json.loads(read_regular_stable(binding_path(root, monitor_handle), MAX_BINDING_BYTES))
    except (json.JSONDecodeError, TypeError) as exc:
        raise WrapperError("monitor binding is invalid") from exc
    required = {
        "schema_version",
        "monitor_task_handle",
        "dispatch_directory",
        "dispatch_handle",
        "manifest_sha256",
        "dispatch_supervisor_path",
        "dispatch_supervisor_sha256",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise WrapperError("monitor binding is invalid")
    if (
        value.get("schema_version") != BINDING_SCHEMA
        or value.get("monitor_task_handle") != monitor_handle
        or not isinstance(value.get("dispatch_directory"), str)
        or not isinstance(value.get("dispatch_handle"), str)
        or SHA256_RE.fullmatch(str(value.get("manifest_sha256"))) is None
        or not isinstance(value.get("dispatch_supervisor_path"), str)
        or SHA256_RE.fullmatch(str(value.get("dispatch_supervisor_sha256"))) is None
    ):
        raise WrapperError("monitor binding is invalid")
    return value


def new_binding(args: argparse.Namespace) -> dict[str, Any]:
    supervisor, supervisor_sha = dispatch_supervisor_identity(args.dispatch_supervisor_path)
    return {
        "schema_version": BINDING_SCHEMA,
        "monitor_task_handle": "",
        "dispatch_directory": str(args.resolved_dispatch_directory),
        "dispatch_handle": args.resolved_dispatch_handle,
        "manifest_sha256": args.resolved_manifest_sha256,
        "dispatch_supervisor_path": str(supervisor),
        "dispatch_supervisor_sha256": supervisor_sha,
    }


def helper_command(args: argparse.Namespace) -> list[str]:
    helper = Path(os.path.abspath(args.supervisor_path))
    if helper.is_symlink() or not helper.is_file():
        raise WrapperError("artifact supervisor is invalid")
    command = [sys.executable, str(helper), args.command]
    if args.command == "start":
        dispatch_directory, terminal, handle, manifest_sha = dispatch_identity(args.dispatch_directory)
        args.resolved_dispatch_directory = dispatch_directory
        args.resolved_dispatch_handle = handle
        args.resolved_manifest_sha256 = manifest_sha
        command += [
            str(terminal),
            "--json-field", "transport.outcome",
            "--success-json", json.dumps("exit_zero"),
        ]
        for outcome in FAILURE_OUTCOMES:
            command += ["--failure-json", json.dumps(outcome)]
        command += [
            "--expect-json", f"dispatch_handle={json.dumps(handle)}",
            "--expect-json", f"manifest_sha256={json.dumps(manifest_sha)}",
            "--expect-json", f"schema_version={json.dumps(DISPATCH_TERMINAL_SCHEMA)}",
            "--expect-json", "business_verdict=\"pending\"",
            "--require-nonempty", "manifest_sha256",
            "--poll-seconds", str(args.poll_seconds),
            "--timeout-seconds", str(args.timeout_seconds),
        ]
        if args.state_dir is not None:
            command += ["--state-dir", str(args.state_dir)]
        if args.restart:
            command.append("--restart")
        if getattr(args, "event_binding", None) is not None:
            command += ["--event-binding", str(args.event_binding)]
        if getattr(args, "bridge_config", None) is not None:
            command += ["--bridge-config", str(args.bridge_config)]
        command += ["--event-backend", "dispatch"]
        return command
    command.append(args.monitor_task_handle)
    if args.state_dir is not None:
        command += ["--state-dir", str(args.state_dir)]
    if args.command == "status" and args.require_terminal:
        command.append("--require-terminal")
    if args.command == "wait":
        command += [
            "--timeout-seconds", str(args.timeout_seconds),
            "--poll-seconds", str(args.poll_seconds),
            "--notification-worker-ack",
        ]
    return command


def run_helper(command: list[str]) -> tuple[int, dict[str, Any]]:
    result = subprocess.run(command, text=True, capture_output=True, check=False, timeout=None)
    if len(result.stdout.encode()) > MAX_HELPER_OUTPUT_BYTES:
        raise WrapperError("artifact supervisor output is oversized")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise WrapperError("artifact supervisor output is invalid") from exc
    if not isinstance(payload, dict):
        raise WrapperError("artifact supervisor output is invalid")
    return result.returncode, payload


def verify_dispatch_terminal(binding: dict[str, Any]) -> dict[str, Any]:
    supervisor = Path(binding["dispatch_supervisor_path"])
    current_supervisor, current_sha = dispatch_supervisor_identity(supervisor)
    if str(current_supervisor) != binding["dispatch_supervisor_path"] or current_sha != binding["dispatch_supervisor_sha256"]:
        raise DispatchVerificationError("dispatch supervisor identity changed")
    dispatch_directory = Path(binding["dispatch_directory"])
    _, _, handle, manifest_sha = dispatch_identity(dispatch_directory)
    if handle != binding["dispatch_handle"] or manifest_sha != binding["manifest_sha256"]:
        raise DispatchVerificationError("dispatch identity changed")
    result = subprocess.run(
        [sys.executable, str(supervisor), "status", str(dispatch_directory)],
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    if len(result.stdout.encode()) > MAX_HELPER_OUTPUT_BYTES:
        raise DispatchVerificationError("dispatch status output is oversized")
    try:
        payload = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise DispatchVerificationError("dispatch status output is invalid") from exc
    transport = payload.get("transport") if isinstance(payload, dict) else None
    terminal_sha = payload.get("terminal_sha256") if isinstance(payload, dict) else None
    if (
        result.returncode not in {0, 3}
        or not isinstance(payload, dict)
        or payload.get("verified") is not True
        or payload.get("terminal_present") is not True
        or payload.get("dispatch_handle") != binding["dispatch_handle"]
        or not isinstance(transport, dict)
        or transport.get("outcome") not in {"exit_zero", *FAILURE_OUTCOMES[:-1]}
        or not isinstance(terminal_sha, str)
        or SHA256_RE.fullmatch(terminal_sha) is None
    ):
        raise DispatchVerificationError("dispatch terminal is not fully verified")
    return {"dispatch_terminal_sha256": terminal_sha, "dispatch_transport_outcome": transport["outcome"]}


def compact_payload(
    command: str, payload: dict[str, Any], dispatch_handle: str | None = None
) -> dict[str, Any]:
    terminal = payload.get("terminal") if isinstance(payload.get("terminal"), dict) else {}
    result = {
        "schema_version": SCHEMA,
        "operation": command,
        "monitor_task_handle": payload.get("task_handle"),
        "state": payload.get("state"),
        "business_verdict": "pending",
    }
    if command == "start":
        result.update(
            {
                "dispatch_handle": dispatch_handle,
                "start_result": payload.get("start_result"),
                "supervisor_alive": payload.get("supervisor_alive") is True,
                "watcher_alive": payload.get("watcher_alive") is True,
            }
        )
    if terminal:
        result.update(
            {
                "observer_outcome": terminal.get("observer_outcome"),
                "observer_state": terminal.get("observer_state"),
                "generation": terminal.get("generation"),
                "terminal_sha256": payload.get("terminal_sha256"),
                "duration_monotonic_ms": terminal.get("duration_monotonic_ms"),
            }
        )
    return result


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--supervisor-path",
        type=Path,
        default=Path(__file__).with_name("supervise_artifact.py"),
    )
    result.add_argument(
        "--dispatch-supervisor-path",
        type=Path,
        default=DEFAULT_DISPATCH_SUPERVISOR,
    )
    sub = result.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start")
    start.add_argument("dispatch_directory", type=Path)
    start.add_argument("--state-dir", type=Path)
    start.add_argument("--poll-seconds", type=positive_float, default=10.0)
    start.add_argument("--timeout-seconds", type=positive_float, required=True)
    start.add_argument("--restart", action="store_true")
    start.add_argument("--event-binding", type=Path)
    start.add_argument("--bridge-config", type=Path)
    status = sub.add_parser("status")
    status.add_argument("monitor_task_handle")
    status.add_argument("--state-dir", type=Path)
    status.add_argument("--require-terminal", action="store_true")
    wait = sub.add_parser("wait")
    wait.add_argument("monitor_task_handle")
    wait.add_argument("--state-dir", type=Path)
    wait.add_argument("--timeout-seconds", type=positive_float, required=True)
    wait.add_argument("--poll-seconds", type=positive_float, default=1.0)
    wait.add_argument(
        "--notification-worker-ack",
        action="store_true",
        help="acknowledge notification-worker-only use; not role authentication",
    )
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "wait" and not args.notification_worker_ack:
            raise WrapperError(
                "wait requires --notification-worker-ack; the flag acknowledges the "
                "notification-worker contract but does not authenticate a model role"
            )
        if args.command != "start":
            args.binding = load_binding(state_directory(args.state_dir), args.monitor_task_handle)
        return_code, payload = run_helper(helper_command(args))
        if args.command == "start" and isinstance(payload.get("task_handle"), str):
            binding = new_binding(args)
            binding["monitor_task_handle"] = payload["task_handle"]
            persist_binding(state_directory(args.state_dir), payload["task_handle"], binding)
        verification: dict[str, Any] = {}
        terminal = payload.get("terminal") if isinstance(payload.get("terminal"), dict) else {}
        if args.command != "start" and terminal.get("observer_outcome") in {
            "condition_satisfied",
            "terminal_or_contract_failure",
        }:
            verification = verify_dispatch_terminal(args.binding)
            expected_code = 0 if verification["dispatch_transport_outcome"] == "exit_zero" else 3
            if return_code != expected_code:
                raise DispatchVerificationError("monitor and dispatch outcomes disagree")
        compact = compact_payload(
            args.command, payload, getattr(args, "resolved_dispatch_handle", None)
        )
        compact.update(verification)
        print(
            json.dumps(
                compact,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return return_code
    except (OSError, subprocess.SubprocessError, WrapperError) as exc:
        state = "dispatch_verification_failed" if isinstance(exc, DispatchVerificationError) else "wrapper_error"
        print(json.dumps({"schema_version": SCHEMA, "operation": args.command, "state": state}, sort_keys=True, separators=(",", ":")))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

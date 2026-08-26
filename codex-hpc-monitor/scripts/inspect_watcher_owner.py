#!/usr/bin/env python3
"""Verify whether one exact Codex Slurm watcher process owns a job lock."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
from typing import Callable, Dict, List, Optional, Tuple


JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")
HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def watcher_lock_path(state_dir: Path, host: str, job_id: str) -> Path:
    return state_dir / f"{host}-{job_id}.lock.json"


def read_process_identity(pid: int) -> Optional[Tuple[str, List[str]]]:
    try:
        stat_raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        tail = stat_raw[stat_raw.rfind(")") + 2 :].split()
        start_ticks = tail[19]
        command_raw = Path(f"/proc/{pid}/cmdline").read_bytes()
        command = [part.decode("utf-8") for part in command_raw.split(b"\0") if part]
    except (OSError, IndexError, UnicodeDecodeError, ValueError):
        return None
    return start_ticks, command


def read_locked_payload(fd: int) -> Dict[str, object]:
    os.lseek(fd, 0, os.SEEK_SET)
    raw = os.read(fd, 1024 * 1024)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def probe_lock_held(fd: int) -> bool:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    fcntl.flock(fd, fcntl.LOCK_UN)
    return False


def plausible_watcher_command(command: List[str], job_id: str) -> bool:
    script_indexes = [
        index for index, token in enumerate(command)
        if Path(token).name == "watch_slurm_job.py"
    ]
    return len(script_indexes) == 1 and job_id in command[script_indexes[0] + 1 :]


def commands_match(recorded: List[str], observed: List[str]) -> bool:
    if recorded == observed:
        return True
    if len(recorded) != len(observed) or recorded[1:] != observed[1:]:
        return False
    recorded_executable = shutil.which(recorded[0])
    observed_executable = shutil.which(observed[0])
    if recorded_executable is None or observed_executable is None:
        return False
    return os.path.realpath(recorded_executable) == os.path.realpath(observed_executable)


def inspect_owner(
    path: Path,
    *,
    host: str,
    job_id: str,
    process_reader: Callable[[int], Optional[Tuple[str, List[str]]]] = read_process_identity,
) -> Dict[str, object]:
    base: Dict[str, object] = {
        "schema_version": 1,
        "host": host,
        "job_id": job_id,
        "lock_path": str(path),
    }
    try:
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    except FileNotFoundError:
        return {**base, "status": "inactive", "reason": "lock_absent"}
    except OSError as exc:
        return {**base, "status": "inconsistent", "reason": f"lock_open_failed: {exc}"}
    try:
        payload = read_locked_payload(fd)
        held = probe_lock_held(fd)
    finally:
        os.close(fd)
    if not held:
        return {**base, "status": "inactive", "reason": "lock_not_held"}
    pid = payload.get("pid")
    start_ticks = payload.get("pid_start_ticks")
    command = payload.get("command")
    if (
        payload.get("host") != host
        or payload.get("job_id") != job_id
        or not isinstance(pid, int)
        or pid <= 0
        or not isinstance(start_ticks, str)
        or not isinstance(command, list)
        or not all(isinstance(token, str) for token in command)
        or not plausible_watcher_command(command, job_id)
    ):
        return {
            **base,
            "status": "inconsistent",
            "reason": "held_lock_payload_mismatch",
            "pid": pid,
        }
    observed = process_reader(pid)
    if observed is None:
        return {
            **base,
            "status": "inconsistent",
            "reason": "held_lock_process_absent",
            "pid": pid,
        }
    observed_start, observed_command = observed
    if observed_start != start_ticks or not commands_match(command, observed_command):
        return {
            **base,
            "status": "inconsistent",
            "reason": "pid_reuse_or_command_mismatch",
            "pid": pid,
        }
    return {
        **base,
        "status": "active_verified",
        "reason": "lock_pid_start_and_command_match",
        "pid": pid,
        "pid_start_ticks": start_ticks,
        "command": command,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job_id")
    parser.add_argument("--host", default="hpc142")
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path.home() / ".cache" / "codex-hpc-monitor",
    )
    args = parser.parse_args()
    if not JOB_ID_RE.fullmatch(args.job_id):
        parser.error("job_id must be numeric, optionally followed by _<array-index>")
    if not HOST_RE.fullmatch(args.host):
        parser.error("--host contains unsupported characters")
    return args


def main() -> int:
    args = parse_args()
    result = inspect_owner(
        watcher_lock_path(args.state_dir, args.host, args.job_id),
        host=args.host,
        job_id=args.job_id,
    )
    print(json.dumps(result, sort_keys=True))
    return {"active_verified": 0, "inactive": 1}.get(str(result["status"]), 2)


if __name__ == "__main__":
    raise SystemExit(main())

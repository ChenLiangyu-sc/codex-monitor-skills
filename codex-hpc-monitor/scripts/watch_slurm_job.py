#!/usr/bin/env python3
"""Wait for an important Slurm state using read-only squeue/sacct queries."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional


JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")
HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

ACTIVE_STATES = {
    "CONFIGURING",
    "COMPLETING",
    "PENDING",
    "RESIZING",
    "RUNNING",
    "SIGNALING",
    "STAGE_OUT",
    "SUSPENDED",
}
TERMINAL_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}
ANOMALOUS_STATES = {"REQUEUED", "REQUEUE_FED", "REQUEUE_HOLD"}


@dataclass(frozen=True)
class Snapshot:
    job_id: str
    state: str
    exit_code: str = ""
    elapsed: str = ""
    reason: str = ""
    owner: str = ""
    submit_time: str = ""
    job_name: str = ""
    partition: str = ""
    source: str = ""
    cluster: str = ""
    sluid: str = ""
    original_sluid: str = ""
    restarts: str = ""


def normalize_state(raw_state: str) -> str:
    """Normalize Slurm decorations such as CANCELLED by 1234 and FAILED+."""
    return raw_state.strip().split(maxsplit=1)[0].rstrip("+").upper()


def observed_at() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_json(path: Optional[Path]) -> Dict[str, object]:
    if path is None:
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(str(temp_path), flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(temp_path), str(path))
        os.chmod(path, 0o600)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def process_start_ticks(pid: int) -> Optional[str]:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        tail = stat[stat.rfind(")") + 2 :].split()
        return tail[19]
    except (OSError, IndexError, ValueError):
        return None


class DuplicateWatcherError(RuntimeError):
    def __init__(self, active: Dict[str, object]) -> None:
        super().__init__("a watcher already owns this host/job")
        self.active = active


class WatcherLock:
    def __init__(
        self,
        path: Path,
        *,
        host: str,
        job_id: str,
        command: List[str],
    ) -> None:
        self.path = path
        self.payload: Dict[str, object] = {
            "pid": os.getpid(),
            "pid_start_ticks": process_start_ticks(os.getpid()),
            "host": host,
            "job_id": job_id,
            "command": command,
            "started_at": observed_at(),
        }
        self.acquired = False
        self.fd: Optional[int] = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise DuplicateWatcherError(read_json(self.path))
        os.ftruncate(fd, 0)
        os.write(fd, f"{json.dumps(self.payload, sort_keys=True)}\n".encode("utf-8"))
        os.fsync(fd)
        self.fd = fd
        self.acquired = True

    def release(self) -> None:
        if not self.acquired or self.fd is None:
            return
        released = {**self.payload, "released_at": observed_at()}
        os.ftruncate(self.fd, 0)
        os.lseek(self.fd, 0, os.SEEK_SET)
        os.write(self.fd, f"{json.dumps(released, sort_keys=True)}\n".encode("utf-8"))
        os.fsync(self.fd)
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)
        self.fd = None
        self.acquired = False

    def __enter__(self) -> "WatcherLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()


class SlurmClient:
    # Extended identity formats are tried in order; older schedulers that do
    # not know a field make the whole call fail, and we fall back to the most
    # complete format the remote sacct accepts. Identity evidence is then
    # marked degraded rather than guessed.
    SACCT_FORMATS = (
        (
            "JobIDRaw,State,ExitCode,Elapsed,User,Submit,JobName,Partition,"
            "Cluster,SLUID,OriginalSLUID,Restart",
            ("cluster", "sluid", "original_sluid", "restarts"),
        ),
        (
            "JobIDRaw,State,ExitCode,Elapsed,User,Submit,JobName,Partition,Cluster",
            ("cluster",),
        ),
        (
            "JobIDRaw,State,ExitCode,Elapsed,User,Submit,JobName,Partition",
            (),
        ),
    )

    def __init__(self, host: str, timeout_seconds: int = 30) -> None:
        if not HOST_RE.fullmatch(host):
            raise ValueError(f"invalid SSH host: {host!r}")
        self.host = host
        self.timeout_seconds = timeout_seconds
        self._sacct_format_index = 0

    def _ssh(self, remote_command: str, *, missing_job_ok: bool = False) -> str:
        result = subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                f"ConnectTimeout={self.timeout_seconds}",
                self.host,
                remote_command,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds + 5,
        )
        if result.returncode != 0:
            combined = "\n".join(
                part.strip() for part in (result.stderr, result.stdout) if part.strip()
            )
            if missing_job_ok and "Invalid job id specified" in combined:
                return ""
            detail = combined or "no diagnostic"
            raise RuntimeError(f"SSH query failed ({result.returncode}): {detail}")
        return result.stdout

    def query(self, job_id: str) -> Optional[Snapshot]:
        if not JOB_ID_RE.fullmatch(job_id):
            raise ValueError(f"invalid Slurm Job ID: {job_id!r}")

        squeue = self._ssh(
            f"squeue -h -j {job_id} -o '%i|%T|%M|%R|%u|%V|%j|%P'",
            missing_job_ok=True,
        )
        for line in squeue.splitlines():
            fields = line.strip().split("|", 7)
            if len(fields) != 8 or fields[0] != job_id:
                continue
            return Snapshot(
                job_id=job_id,
                state=normalize_state(fields[1]),
                elapsed=fields[2],
                reason=fields[3],
                owner=fields[4],
                submit_time=fields[5],
                job_name=fields[6],
                partition=fields[7],
                source="squeue",
            )

        sacct, extra_fields = self._sacct_query(job_id)
        for line in sacct.splitlines():
            fields = line.strip().split("|")
            if len(fields) != 8 + len(extra_fields) or fields[0] != job_id:
                continue
            extras = {
                name: fields[8 + index]
                for index, name in enumerate(extra_fields)
            }
            return Snapshot(
                job_id=job_id,
                state=normalize_state(fields[1]),
                exit_code=fields[2],
                elapsed=fields[3],
                owner=fields[4],
                submit_time=fields[5],
                job_name=fields[6],
                partition=fields[7],
                source="sacct",
                **extras,
            )
        return None

    def _sacct_query(self, job_id: str) -> "tuple[str, tuple[str, ...]]":
        index = self._sacct_format_index
        while True:
            fmt, extra_fields = self.SACCT_FORMATS[index]
            try:
                payload = self._ssh(
                    f"sacct -X -n -P -j {job_id} --format={fmt}"
                )
            except RuntimeError as exc:
                text = str(exc).lower()
                if ("field" in text or "invalid option" in text) and index + 1 < len(
                    self.SACCT_FORMATS
                ):
                    index += 1
                    continue
                raise
            self._sacct_format_index = index
            return payload, extra_fields


def build_event(
    event: str,
    snapshot: Optional[Snapshot],
    detail: str = "",
    extra: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    classifications = {
        "completed": "scheduler_success",
        "terminal_failure": "scheduler_terminal_failure",
        "running": "scheduler_active",
        "pending_alert": "scheduler_pending_alert",
        "anomalous_state": "scheduler_anomalous_state",
        "identity_mismatch": "scheduler_identity_mismatch",
        "lost_observability": "scheduler_observability_failure",
        "query_error": "scheduler_query_failure",
        "watch_timeout": "scheduler_watch_timeout",
        "duplicate_watcher": "watcher_duplicate",
        "dependency_error": "watcher_infrastructure_failure",
        "watcher_error": "watcher_infrastructure_failure",
    }
    payload: Dict[str, object] = {
        "event": event,
        "observed_at": observed_at(),
        "scope": "slurm_only",
        "slurm_classification": classifications.get(event, "scheduler_observation"),
        "project_gate_evaluated": False,
    }
    if snapshot is not None:
        payload.update(asdict(snapshot))
    if detail:
        payload["detail"] = detail
    if extra:
        payload.update(extra)
    return payload


def emit(
    event: str,
    snapshot: Optional[Snapshot],
    detail: str = "",
    *,
    result_path: Optional[Path] = None,
    extra: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    payload = build_event(event, snapshot, detail, extra)
    if result_path is not None:
        atomic_write_json(result_path, payload)
    print(json.dumps(payload, sort_keys=True), flush=True)
    return payload


def parse_submit_epoch(value: str) -> Optional[float]:
    if not value or value in {"Unknown", "N/A", "None"}:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.timestamp()
    except ValueError:
        return None


def identity_mismatch(
    snapshot: Snapshot,
    *,
    expected_owner: Optional[str],
    expected_job_name: Optional[str],
    expected_partition: Optional[str],
) -> Optional[str]:
    checks = (
        ("owner", expected_owner, snapshot.owner),
        ("job_name", expected_job_name, snapshot.job_name),
        ("partition", expected_partition, snapshot.partition),
    )
    for field, expected, actual in checks:
        if expected is not None and actual != expected:
            return f"{field} mismatch: expected {expected!r}, observed {actual!r}"
    return None


# Stable scheduler identity: submit time is always available; cluster and
# SLUID fields strengthen the binding when the remote sacct supports them.
IDENTITY_BINDING_FIELDS = (
    "job_id",
    "submit_time",
    "cluster",
    "sluid",
    "original_sluid",
)
ABSENT_IDENTITY_VALUES = {"", "N/A", "n/a", "None", "Unknown", "unknown"}


def identity_values(snapshot: Snapshot) -> Dict[str, str]:
    candidates = {
        "job_id": snapshot.job_id,
        "submit_time": snapshot.submit_time,
        "cluster": snapshot.cluster,
        "sluid": snapshot.sluid,
        "original_sluid": snapshot.original_sluid,
        "restarts": snapshot.restarts,
    }
    return {
        field: value
        for field, value in candidates.items()
        if value and value not in ABSENT_IDENTITY_VALUES
    }


def identity_conflict(bound: Dict[str, str], observed: Dict[str, str]) -> Optional[str]:
    """Fail closed when a previously bound identity field changes value.

    Job-ID reuse or a requeued allocation appears as the same job id with a
    different submit time / SLUID. Only fields present on both sides are
    comparable; `restarts` tracks the same job restarting and never conflicts.
    """
    for field in IDENTITY_BINDING_FIELDS:
        if field in bound and field in observed and bound[field] != observed[field]:
            return (
                f"scheduler identity conflict on {field}: bound {bound[field]!r}, "
                f"observed {observed[field]!r} (possible job id reuse or requeue)"
            )
    return None


def merged_identity(bound: Dict[str, str], observed: Dict[str, str]) -> Dict[str, str]:
    merged = dict(bound)
    for field in (*IDENTITY_BINDING_FIELDS, "restarts"):
        if field in observed:
            merged[field] = observed[field]
    return merged


def persist_observation(
    path: Optional[Path],
    *,
    host: str,
    snapshot: Optional[Snapshot],
    event: str,
    monitor_started_epoch: float,
    pending_since_epoch: Optional[float],
    missing_exit_since_epoch: Optional[float],
    consecutive_failures: int,
    detail: str = "",
    deadline_at_epoch: Optional[float] = None,
    identity_binding: Optional[Dict[str, str]] = None,
) -> None:
    if path is None:
        return
    payload: Dict[str, object] = {
        "schema_version": 1,
        "host": host,
        "event": event,
        "updated_at": observed_at(),
        "monitor_started_epoch": monitor_started_epoch,
        "pending_since_epoch": pending_since_epoch,
        "missing_exit_since_epoch": missing_exit_since_epoch,
        "consecutive_failures": consecutive_failures,
        "snapshot": asdict(snapshot) if snapshot is not None else None,
    }
    if deadline_at_epoch is not None:
        payload["deadline_at_epoch"] = deadline_at_epoch
    if identity_binding is not None:
        payload["identity_binding"] = identity_binding
    if detail:
        payload["detail"] = detail
    atomic_write_json(path, payload)


def monitor(
    client: SlurmClient,
    job_id: str,
    poll_seconds: float,
    pending_alert_seconds: float,
    query_failures: int,
    notify_running: bool,
    *,
    host: str = "hpc142",
    terminal_observability_seconds: float = 300.0,
    max_watch_seconds: float = 604800.0,
    expected_owner: Optional[str] = None,
    expected_job_name: Optional[str] = None,
    expected_partition: Optional[str] = None,
    state_path: Optional[Path] = None,
    result_path: Optional[Path] = None,
    initial_state: Optional[Dict[str, object]] = None,
    clock: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    del clock  # The persisted state uses wall time so restarts retain thresholds.
    saved = initial_state or {}
    now_epoch = wall_clock()
    monitor_started_epoch = float(saved.get("monitor_started_epoch", now_epoch))
    pending_since_value = saved.get("pending_since_epoch")
    pending_since_epoch = (
        float(pending_since_value) if pending_since_value is not None else None
    )
    missing_exit_value = saved.get("missing_exit_since_epoch")
    missing_exit_since_epoch = (
        float(missing_exit_value) if missing_exit_value is not None else None
    )
    consecutive_failures = 0
    saved_identity = saved.get("identity_binding")
    identity_binding: Dict[str, str] = (
        {
            field: value
            for field, value in saved_identity.items()
            if isinstance(field, str) and isinstance(value, str)
        }
        if isinstance(saved_identity, dict)
        else {}
    )

    # Absolute observation deadline. It is derived once from the persisted
    # monitor start and the first max-watch duration, then persisted itself;
    # a watcher or supervisor restart can only shrink the remaining window,
    # never extend it.
    saved_deadline = saved.get("deadline_at_epoch")
    if isinstance(saved_deadline, (int, float)) and not isinstance(saved_deadline, bool):
        deadline_at_epoch = float(saved_deadline)
    else:
        deadline_at_epoch = (
            monitor_started_epoch + max_watch_seconds if max_watch_seconds > 0 else None
        )
    if deadline_at_epoch is not None and max_watch_seconds > 0:
        deadline_at_epoch = min(deadline_at_epoch, now_epoch + max_watch_seconds)

    while True:
        now_epoch = wall_clock()
        if deadline_at_epoch is not None and now_epoch >= deadline_at_epoch:
            persist_observation(
                state_path,
                host=host,
                snapshot=None,
                event="watch_timeout",
                monitor_started_epoch=monitor_started_epoch,
                pending_since_epoch=pending_since_epoch,
                missing_exit_since_epoch=missing_exit_since_epoch,
                consecutive_failures=consecutive_failures,
                detail="absolute observation deadline reached",
                deadline_at_epoch=deadline_at_epoch,
                identity_binding=identity_binding,
            )
            emit(
                "watch_timeout",
                None,
                "absolute observation deadline reached",
                result_path=result_path,
                extra={"job_id": job_id, "host": host},
            )
            return 10

        try:
            snapshot = client.query(job_id)
            if snapshot is None:
                raise RuntimeError("job is absent from both squeue and sacct")
            consecutive_failures = 0
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            consecutive_failures += 1
            persist_observation(
                state_path,
                host=host,
                snapshot=None,
                event="query_retry",
                monitor_started_epoch=monitor_started_epoch,
                pending_since_epoch=pending_since_epoch,
                missing_exit_since_epoch=missing_exit_since_epoch,
                consecutive_failures=consecutive_failures,
                detail=str(exc),
                deadline_at_epoch=deadline_at_epoch,
                identity_binding=identity_binding,
            )
            if consecutive_failures >= query_failures:
                emit(
                    "query_error",
                    None,
                    str(exc),
                    result_path=result_path,
                    extra={"job_id": job_id, "host": host},
                )
                return 5
            sleep(poll_seconds)
            continue

        mismatch = identity_mismatch(
            snapshot,
            expected_owner=expected_owner,
            expected_job_name=expected_job_name,
            expected_partition=expected_partition,
        )
        if mismatch is None:
            observed_identity = identity_values(snapshot)
            mismatch = identity_conflict(identity_binding, observed_identity)
            if mismatch is None:
                identity_binding = merged_identity(identity_binding, observed_identity)
        if mismatch:
            persist_observation(
                state_path,
                host=host,
                snapshot=snapshot,
                event="identity_mismatch",
                monitor_started_epoch=monitor_started_epoch,
                pending_since_epoch=pending_since_epoch,
                missing_exit_since_epoch=missing_exit_since_epoch,
                consecutive_failures=0,
                detail=mismatch,
                deadline_at_epoch=deadline_at_epoch,
                identity_binding=identity_binding,
            )
            emit(
                "identity_mismatch",
                snapshot,
                mismatch,
                result_path=result_path,
            )
            return 9

        state = snapshot.state
        if state == "COMPLETED":
            if snapshot.exit_code == "0:0":
                persist_observation(
                    state_path,
                    host=host,
                    snapshot=snapshot,
                    event="completed",
                    monitor_started_epoch=monitor_started_epoch,
                    pending_since_epoch=pending_since_epoch,
                    missing_exit_since_epoch=None,
                    consecutive_failures=0,
                    deadline_at_epoch=deadline_at_epoch,
                    identity_binding=identity_binding,
                )
                emit("completed", snapshot, result_path=result_path)
                return 0
            if snapshot.exit_code:
                persist_observation(
                    state_path,
                    host=host,
                    snapshot=snapshot,
                    event="terminal_failure",
                    monitor_started_epoch=monitor_started_epoch,
                    pending_since_epoch=pending_since_epoch,
                    missing_exit_since_epoch=None,
                    consecutive_failures=0,
                    deadline_at_epoch=deadline_at_epoch,
                    identity_binding=identity_binding,
                )
                emit(
                    "terminal_failure",
                    snapshot,
                    "COMPLETED has a nonzero exit code",
                    result_path=result_path,
                )
                return 3
            if missing_exit_since_epoch is None:
                missing_exit_since_epoch = now_epoch
            if (
                now_epoch - missing_exit_since_epoch
                >= terminal_observability_seconds
            ):
                persist_observation(
                    state_path,
                    host=host,
                    snapshot=snapshot,
                    event="lost_observability",
                    monitor_started_epoch=monitor_started_epoch,
                    pending_since_epoch=pending_since_epoch,
                    missing_exit_since_epoch=missing_exit_since_epoch,
                    consecutive_failures=0,
                    detail="COMPLETED never produced an explicit ExitCode",
                    deadline_at_epoch=deadline_at_epoch,
                    identity_binding=identity_binding,
                )
                emit(
                    "lost_observability",
                    snapshot,
                    "COMPLETED never produced an explicit ExitCode",
                    result_path=result_path,
                )
                return 8
            persist_observation(
                state_path,
                host=host,
                snapshot=snapshot,
                event="awaiting_exit_code",
                monitor_started_epoch=monitor_started_epoch,
                pending_since_epoch=pending_since_epoch,
                missing_exit_since_epoch=missing_exit_since_epoch,
                consecutive_failures=0,
                deadline_at_epoch=deadline_at_epoch,
                identity_binding=identity_binding,
            )
            sleep(poll_seconds)
            continue

        missing_exit_since_epoch = None
        if state in TERMINAL_STATES:
            persist_observation(
                state_path,
                host=host,
                snapshot=snapshot,
                event="terminal_failure",
                monitor_started_epoch=monitor_started_epoch,
                pending_since_epoch=pending_since_epoch,
                missing_exit_since_epoch=None,
                consecutive_failures=0,
                deadline_at_epoch=deadline_at_epoch,
                identity_binding=identity_binding,
            )
            emit("terminal_failure", snapshot, result_path=result_path)
            return 3
        if state in ANOMALOUS_STATES or state not in ACTIVE_STATES:
            persist_observation(
                state_path,
                host=host,
                snapshot=snapshot,
                event="anomalous_state",
                monitor_started_epoch=monitor_started_epoch,
                pending_since_epoch=pending_since_epoch,
                missing_exit_since_epoch=None,
                consecutive_failures=0,
                deadline_at_epoch=deadline_at_epoch,
                identity_binding=identity_binding,
            )
            emit("anomalous_state", snapshot, result_path=result_path)
            return 7
        if state == "RUNNING" and notify_running:
            persist_observation(
                state_path,
                host=host,
                snapshot=snapshot,
                event="running",
                monitor_started_epoch=monitor_started_epoch,
                pending_since_epoch=None,
                missing_exit_since_epoch=None,
                consecutive_failures=0,
                deadline_at_epoch=deadline_at_epoch,
                identity_binding=identity_binding,
            )
            emit("running", snapshot, result_path=result_path)
            return 6

        if state == "PENDING":
            submit_epoch = parse_submit_epoch(snapshot.submit_time)
            if submit_epoch is not None:
                pending_since_epoch = submit_epoch
            elif pending_since_epoch is None:
                pending_since_epoch = now_epoch
            if (
                pending_alert_seconds > 0
                and now_epoch - pending_since_epoch >= pending_alert_seconds
            ):
                persist_observation(
                    state_path,
                    host=host,
                    snapshot=snapshot,
                    event="pending_alert",
                    monitor_started_epoch=monitor_started_epoch,
                    pending_since_epoch=pending_since_epoch,
                    missing_exit_since_epoch=None,
                    consecutive_failures=0,
                    deadline_at_epoch=deadline_at_epoch,
                    identity_binding=identity_binding,
                )
                emit("pending_alert", snapshot, result_path=result_path)
                return 4
        else:
            pending_since_epoch = None

        persist_observation(
            state_path,
            host=host,
            snapshot=snapshot,
            event="observed",
            monitor_started_epoch=monitor_started_epoch,
            pending_since_epoch=pending_since_epoch,
            missing_exit_since_epoch=missing_exit_since_epoch,
            consecutive_failures=0,
            deadline_at_epoch=deadline_at_epoch,
            identity_binding=identity_binding,
        )
        sleep(poll_seconds)


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job_id", help="numeric Slurm Job ID, optionally with an array index")
    parser.add_argument("--host", default="hpc142", help="SSH host alias (default: hpc142)")
    parser.add_argument("--poll-seconds", type=positive_float, default=60.0)
    parser.add_argument("--pending-alert-seconds", type=nonnegative_float, default=1800.0)
    parser.add_argument(
        "--terminal-observability-seconds", type=nonnegative_float, default=300.0
    )
    parser.add_argument(
        "--max-watch-seconds", type=nonnegative_float, default=604800.0
    )
    parser.add_argument("--query-failures", type=int, default=3)
    parser.add_argument("--notify-running", action="store_true")
    parser.add_argument("--expected-owner")
    parser.add_argument("--expected-job-name")
    parser.add_argument("--expected-partition")
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
    if args.query_failures < 1:
        parser.error("--query-failures must be at least 1")
    return args


def watcher_paths(state_dir: Path, host: str, job_id: str) -> Dict[str, Path]:
    prefix = f"{host}-{job_id}"
    return {
        "lock": state_dir / f"{prefix}.lock.json",
        "state": state_dir / f"{prefix}.state.json",
        "result": state_dir / f"{prefix}.result.json",
    }


def clear_previous_result(path: Path) -> None:
    """Remove an event left by an earlier watcher phase after owning the lock."""
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def main() -> int:
    args = parse_args()
    paths = watcher_paths(args.state_dir, args.host, args.job_id)

    if shutil.which("ssh") is None:
        emit(
            "dependency_error",
            None,
            "ssh executable is not available",
            result_path=paths["result"],
            extra={"job_id": args.job_id, "host": args.host},
        )
        return 12

    try:
        client = SlurmClient(args.host)
        lock = WatcherLock(
            paths["lock"],
            host=args.host,
            job_id=args.job_id,
            command=[sys.executable, *sys.argv],
        )
        with lock:
            clear_previous_result(paths["result"])
            initial_state = read_json(paths["state"])
            return monitor(
                client=client,
                job_id=args.job_id,
                poll_seconds=args.poll_seconds,
                pending_alert_seconds=args.pending_alert_seconds,
                query_failures=args.query_failures,
                notify_running=args.notify_running,
                host=args.host,
                terminal_observability_seconds=args.terminal_observability_seconds,
                max_watch_seconds=args.max_watch_seconds,
                expected_owner=args.expected_owner,
                expected_job_name=args.expected_job_name,
                expected_partition=args.expected_partition,
                state_path=paths["state"],
                result_path=paths["result"],
                initial_state=initial_state,
            )
    except DuplicateWatcherError as exc:
        emit(
            "duplicate_watcher",
            None,
            "another process already owns this host/job",
            extra={"job_id": args.job_id, "host": args.host, "active": exc.active},
        )
        return 11
    except (OSError, RuntimeError, ValueError) as exc:
        emit(
            "watcher_error",
            None,
            str(exc),
            result_path=paths["result"],
            extra={"job_id": args.job_id, "host": args.host},
        )
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

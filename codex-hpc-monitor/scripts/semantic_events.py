#!/usr/bin/env python3
"""Shared monitor infrastructure for the Codex monitor skills.

This module is vendored as a byte-identical copy into every skill that needs
it (currently ``codex-hpc-monitor`` and ``codex-long-task-monitor``) so each
skill remains independently installable. A synchronization test in each skill
verifies the copies match whenever both skill directories are present.

Contents:

* the versioned semantic-event contract (``codex-monitor.event/v1``);
* a durable, at-least-once outbox with atomic publication, leases,
  exponential backoff with jitter, and dead-letter state;
* the idempotent postflight marker used to recognize already-processed wake
  events;
* strict validation for the opt-in bridge configuration and per-monitor event
  binding.

The outbox is a notification transport, never terminal authority. Only safe
metadata (enums, opaque handles, digests) is ever stored; raw logs, prompts,
responses, artifact contents, credentials, and free-form callback text must
never enter any structure handled here.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


EVENT_SCHEMA = "codex-monitor.event/v1"
DELIVERY_SCHEMA = "codex-monitor.delivery/v1"
POSTFLIGHT_SCHEMA = "codex-monitor.postflight/v1"
BRIDGE_CONFIG_SCHEMA = "codex-monitor.bridge-config/v1"
EVENT_BINDING_SCHEMA = "codex-monitor.event-binding/v1"

EVENT_ENUMS = frozenset(
    {
        "transport_success",
        "transport_failure",
        "deadline_exceeded",
        "lost_observability",
        "contract_violation",
    }
)
BACKEND_ENUMS = frozenset({"slurm", "artifact", "dispatch"})
DELIVERY_STATES = frozenset({"pending", "leased", "delivered", "dead_letter"})

SHA256_PREFIX_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
INSTANCE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
THREAD_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
RFC3339_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+Z$")

MAX_EVENT_BYTES = 16 * 1024
MAX_DELIVERY_BYTES = 8 * 1024
MAX_CONFIG_BYTES = 16 * 1024
MAX_BINDING_BYTES = 8 * 1024
MAX_POSTFLIGHT_BYTES = 4 * 1024
MAX_SAFE_MESSAGE_CHARS = 200


class SemanticEventError(ValueError):
    """Raised when event/outbox/config data violates the fixed contracts."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def canonical_json(payload: object) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_prefix(payload: bytes) -> str:
    return f"sha256:{sha256_hex(payload)}"


def parse_utc(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not RFC3339_RE.fullmatch(value):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Filesystem primitives (private directories, atomic publication)
# ---------------------------------------------------------------------------


def ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise SemanticEventError("unsafe_directory", f"unsafe state directory: {path}")
    os.chmod(path, 0o700)


def fsync_directory(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_exclusive(path: Path, payload: bytes, mode: int = 0o600) -> None:
    ensure_private_directory(path.parent)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while publishing monitor state")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    fsync_directory(path.parent)


def _write_temp(path: Path, payload: bytes) -> Path:
    ensure_private_directory(path.parent)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    _write_exclusive(temp, payload)
    return temp


def atomic_replace_json(path: Path, payload: object) -> None:
    temp = _write_temp(path, canonical_json(payload))
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
    temp = _write_temp(path, canonical_json(payload))
    try:
        os.link(str(temp), str(path), follow_symlinks=False)
        os.chmod(path, 0o600)
        fsync_directory(path.parent)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def read_regular_json(path: Path, limit: int) -> Dict[str, Any]:
    """Read a JSON object from a regular, non-symlink file of bounded size."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(str(path), flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise SemanticEventError("not_a_regular_file", str(path))
        if info.st_size > limit:
            raise SemanticEventError("file_too_large", str(path))
        chunks: List[bytes] = []
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            chunks.append(block)
        payload = b"".join(chunks)
        if len(payload) > limit:
            raise SemanticEventError("file_too_large", str(path))
    finally:
        os.close(fd)
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SemanticEventError("invalid_json", str(path)) from exc
    if not isinstance(value, dict):
        raise SemanticEventError("not_an_object", str(path))
    return value


def read_json_if_present(path: Path, limit: int) -> Optional[Dict[str, Any]]:
    try:
        return read_regular_json(path, limit)
    except FileNotFoundError:
        return None


# ---------------------------------------------------------------------------
# Bridge configuration and event binding (strict, fail-closed)
# ---------------------------------------------------------------------------


def codex_home_digest(codex_home: Path) -> str:
    """Stable non-secret digest identifying one CODEX_HOME directory."""
    resolved = Path(codex_home).expanduser().resolve(strict=False)
    return sha256_prefix(str(resolved).encode())


def validate_bridge_config(value: object) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise SemanticEventError("config_not_an_object")
    expected = {
        "schema",
        "enabled",
        "instance_id",
        "codex_home_id",
        "workspace",
        "transport",
        "request_timeout_seconds",
        "poll_seconds",
        "lease_seconds",
        "max_attempts",
        "backoff_initial_seconds",
        "backoff_max_seconds",
    }
    if set(value) != expected:
        missing = sorted(expected - set(value))
        unknown = sorted(set(value) - expected)
        raise SemanticEventError(
            "config_field_set_mismatch",
            f"missing={missing} unknown={unknown}",
        )
    if value["schema"] != BRIDGE_CONFIG_SCHEMA:
        raise SemanticEventError("config_schema_mismatch")
    if not isinstance(value["enabled"], bool):
        raise SemanticEventError("config_enabled_not_bool")
    if not isinstance(value["instance_id"], str) or not INSTANCE_ID_RE.fullmatch(
        value["instance_id"]
    ):
        raise SemanticEventError("config_instance_id_invalid")
    if not isinstance(value["codex_home_id"], str) or not SHA256_PREFIX_RE.fullmatch(
        value["codex_home_id"]
    ):
        raise SemanticEventError("config_codex_home_id_invalid")
    if not isinstance(value["workspace"], str) or not value["workspace"].startswith(
        "/"
    ):
        raise SemanticEventError("config_workspace_not_absolute")
    transport = value["transport"]
    if not isinstance(transport, dict) or set(transport) != {"type", "command"}:
        raise SemanticEventError("config_transport_shape_invalid")
    if transport["type"] != "stdio":
        # Only the stdio baseline is supported; unix socket / TCP transports
        # remain explicitly unsupported and fail closed.
        raise SemanticEventError(
            "config_transport_unsupported",
            f"transport {transport['type']!r} is not supported; only 'stdio' is",
        )
    command = transport["command"]
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(token, str) and token for token in command)
    ):
        raise SemanticEventError("config_transport_command_invalid")

    def positive_number(name: str, low: float, high: float) -> float:
        raw = value[name]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise SemanticEventError(f"config_{name}_invalid")
        number = float(raw)
        if not (low < number <= high):
            raise SemanticEventError(
                f"config_{name}_out_of_range", f"{name}={number}"
            )
        return number

    positive_number("request_timeout_seconds", 0.0, 600.0)
    positive_number("poll_seconds", 0.0, 3600.0)
    positive_number("lease_seconds", 0.0, 86400.0)
    attempts = value["max_attempts"]
    if isinstance(attempts, bool) or not isinstance(attempts, int) or not (
        1 <= attempts <= 1000
    ):
        raise SemanticEventError("config_max_attempts_invalid")
    initial = positive_number("backoff_initial_seconds", 0.0, 86400.0)
    maximum = positive_number("backoff_max_seconds", 0.0, 604800.0)
    if maximum < initial:
        raise SemanticEventError("config_backoff_max_below_initial")
    return value


def load_bridge_config(path: Path) -> Dict[str, Any]:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise SemanticEventError("config_missing", str(path)) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SemanticEventError("config_not_a_regular_file", str(path))
    if info.st_size > MAX_CONFIG_BYTES:
        raise SemanticEventError("config_too_large", str(path))
    if info.st_mode & 0o077:
        raise SemanticEventError(
            "config_permissions_too_open",
            f"{path} must not be group/world readable",
        )
    return validate_bridge_config(read_regular_json(path, MAX_CONFIG_BYTES))


def validate_event_binding(value: object) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise SemanticEventError("binding_not_an_object")
    expected = {
        "schema",
        "codex_home_id",
        "app_server_instance",
        "thread_id",
        "workspace",
    }
    if set(value) != expected:
        raise SemanticEventError("binding_field_set_mismatch", f"keys={sorted(value)}")
    if value["schema"] != EVENT_BINDING_SCHEMA:
        raise SemanticEventError("binding_schema_mismatch")
    if not isinstance(value["codex_home_id"], str) or not SHA256_PREFIX_RE.fullmatch(
        value["codex_home_id"]
    ):
        raise SemanticEventError("binding_codex_home_id_invalid")
    if not isinstance(value["app_server_instance"], str) or not INSTANCE_ID_RE.fullmatch(
        value["app_server_instance"]
    ):
        raise SemanticEventError("binding_instance_invalid")
    if not isinstance(value["thread_id"], str) or not THREAD_ID_RE.fullmatch(
        value["thread_id"]
    ):
        raise SemanticEventError("binding_thread_id_invalid")
    if not isinstance(value["workspace"], str) or not value["workspace"].startswith("/"):
        raise SemanticEventError("binding_workspace_not_absolute")
    if len(value["workspace"]) > 4096:
        raise SemanticEventError("binding_workspace_invalid", "workspace path too long")
    return value


def load_event_binding(path: Path) -> Dict[str, Any]:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise SemanticEventError("binding_missing", str(path)) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SemanticEventError("binding_not_a_regular_file", str(path))
    if info.st_size > MAX_BINDING_BYTES:
        raise SemanticEventError("binding_too_large", str(path))
    if info.st_mode & 0o077:
        raise SemanticEventError("binding_permissions_too_open", str(path))
    return validate_event_binding(read_regular_json(path, MAX_BINDING_BYTES))


# ---------------------------------------------------------------------------
# Semantic event contract
# ---------------------------------------------------------------------------


def build_event(
    *,
    backend: str,
    handle: str,
    generation: str,
    terminal_digest: str,
    event: str,
    exit_code: Optional[int],
    binding: Dict[str, Any],
    created_at: Optional[str] = None,
) -> Dict[str, Any]:
    if backend not in BACKEND_ENUMS:
        raise SemanticEventError("backend_invalid", backend)
    if event not in EVENT_ENUMS:
        raise SemanticEventError("event_invalid", event)
    for name, value in (("handle", handle), ("generation", generation)):
        if not isinstance(value, str) or not (1 <= len(value) <= 256):
            raise SemanticEventError(f"{name}_invalid")
    if not isinstance(terminal_digest, str) or not SHA256_PREFIX_RE.fullmatch(
        terminal_digest
    ):
        raise SemanticEventError("terminal_digest_invalid")
    if exit_code is not None and (isinstance(exit_code, bool) or not isinstance(exit_code, int)):
        raise SemanticEventError("exit_code_invalid")
    validate_event_binding(binding)
    payload: Dict[str, Any] = {
        "schema": EVENT_SCHEMA,
        "event_id": "",
        "created_at": created_at or utc_now(),
        "monitor": {
            "backend": backend,
            "handle": handle,
            "generation": generation,
            "terminal_digest": terminal_digest,
        },
        "event": event,
        "exit_code": exit_code,
        "business_verdict": "pending",
        "binding": dict(binding),
    }
    payload["event_id"] = compute_event_id(payload)
    validate_event(payload)
    return payload


def compute_event_id(event: Dict[str, Any]) -> str:
    """Deterministic identity over schema, monitor identity, event, binding.

    Never derived from a timestamp: the same terminal evidence, event enum,
    and binding always produce the same id, so duplicate observation or
    duplicate callbacks cannot create a second logical event.
    """
    identity = {
        "schema": event.get("schema"),
        "monitor": event.get("monitor"),
        "event": event.get("event"),
        "binding": event.get("binding"),
    }
    return f"sha256:{sha256_hex(canonical_json(identity))}"


def validate_event(value: object) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise SemanticEventError("event_not_an_object")
    expected = {
        "schema",
        "event_id",
        "created_at",
        "monitor",
        "event",
        "exit_code",
        "business_verdict",
        "binding",
    }
    if set(value) != expected:
        raise SemanticEventError("event_field_set_mismatch", f"keys={sorted(value)}")
    if value["schema"] != EVENT_SCHEMA:
        raise SemanticEventError("event_schema_mismatch")
    if not isinstance(value["event_id"], str) or not SHA256_PREFIX_RE.fullmatch(
        value["event_id"]
    ):
        raise SemanticEventError("event_id_invalid")
    if parse_utc(value["created_at"]) is None:
        raise SemanticEventError("event_created_at_invalid")
    monitor = value["monitor"]
    if not isinstance(monitor, dict) or set(monitor) != {
        "backend",
        "handle",
        "generation",
        "terminal_digest",
    }:
        raise SemanticEventError("event_monitor_shape_invalid")
    if monitor["backend"] not in BACKEND_ENUMS:
        raise SemanticEventError("event_backend_invalid")
    for name in ("handle", "generation"):
        if not isinstance(monitor[name], str) or not (1 <= len(monitor[name]) <= 256):
            raise SemanticEventError(f"event_{name}_invalid")
    if not isinstance(monitor["terminal_digest"], str) or not SHA256_PREFIX_RE.fullmatch(
        monitor["terminal_digest"]
    ):
        raise SemanticEventError("event_terminal_digest_invalid")
    if value["event"] not in EVENT_ENUMS:
        raise SemanticEventError("event_enum_invalid", str(value["event"]))
    exit_code = value["exit_code"]
    if exit_code is not None and (
        isinstance(exit_code, bool) or not isinstance(exit_code, int)
    ):
        raise SemanticEventError("event_exit_code_invalid")
    if value["business_verdict"] != "pending":
        raise SemanticEventError("event_business_verdict_not_pending")
    validate_event_binding(value["binding"])
    if compute_event_id(value) != value["event_id"]:
        raise SemanticEventError("event_id_mismatch")
    return value


def wake_message(event: Dict[str, Any]) -> str:
    """Fixed, locally generated wake template. Never event-controlled text."""
    monitor = event["monitor"]
    exit_code = event["exit_code"]
    return (
        "codex_monitor_event/v1\n"
        f"event_id={event['event_id']}\n"
        f"backend={monitor['backend']}\n"
        f"handle={monitor['handle']}\n"
        f"generation={monitor['generation']}\n"
        f"event={event['event']}\n"
        f"exit_code={exit_code if exit_code is not None else 'null'}\n"
        f"terminal_digest={monitor['terminal_digest']}\n"
        "business_verdict=pending\n"
        "\n"
        "Verify the immutable terminal record and process this event idempotently.\n"
        "Do not retry, cancel, resubmit, mutate, or approve the workload solely "
        "because of this notification."
    )


# ---------------------------------------------------------------------------
# Durable outbox
# ---------------------------------------------------------------------------


def outbox_root(state_dir: Path) -> Path:
    return Path(state_dir) / "outbox"


def event_dir(outbox: Path, event_id: str) -> Path:
    if not isinstance(event_id, str) or not event_id.startswith("sha256:"):
        raise SemanticEventError("event_id_invalid")
    return outbox / event_id[len("sha256:"):]


def _initial_delivery(now: Optional[str]) -> Dict[str, Any]:
    return {
        "schema": DELIVERY_SCHEMA,
        "event_id": "",
        "state": "pending",
        "attempts": 0,
        "next_attempt_at": now,
        "finished_at": None,
        "lease": {"owner": None, "expires_at": None},
        "delivery": {"thread_id": None, "turn_id": None, "delivered_at": None},
        "last_error": {"code": None, "safe_message": None},
    }


def _validate_delivery(value: object, event_id: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise SemanticEventError("delivery_not_an_object")
    expected = {
        "schema",
        "event_id",
        "state",
        "attempts",
        "next_attempt_at",
        "finished_at",
        "lease",
        "delivery",
        "last_error",
    }
    if set(value) != expected:
        raise SemanticEventError("delivery_field_set_mismatch", f"keys={sorted(value)}")
    if value["schema"] != DELIVERY_SCHEMA or value["event_id"] != event_id:
        raise SemanticEventError("delivery_identity_mismatch")
    if value["state"] not in DELIVERY_STATES:
        raise SemanticEventError("delivery_state_invalid")
    if isinstance(value["attempts"], bool) or not isinstance(value["attempts"], int):
        raise SemanticEventError("delivery_attempts_invalid")
    for key in ("lease", "delivery", "last_error"):
        if not isinstance(value[key], dict):
            raise SemanticEventError(f"delivery_{key}_invalid")
    for stamp in (
        value["next_attempt_at"],
        value["finished_at"],
        value["lease"]["expires_at"],
        value["delivery"]["delivered_at"],
    ):
        if stamp is not None and parse_utc(stamp) is None:
            raise SemanticEventError("delivery_timestamp_invalid")
    for text in (value["lease"]["owner"], value["delivery"]["thread_id"], value["delivery"]["turn_id"]):
        if text is not None and not isinstance(text, str):
            raise SemanticEventError("delivery_field_invalid")
    error = value["last_error"]
    if set(error) != {"code", "safe_message"}:
        raise SemanticEventError("delivery_last_error_invalid")
    if error["code"] is not None and not isinstance(error["code"], str):
        raise SemanticEventError("delivery_last_error_invalid")
    if error["safe_message"] is not None and not isinstance(error["safe_message"], str):
        raise SemanticEventError("delivery_last_error_invalid")
    return value


class _OutboxLock:
    """Exclusive claim lock serializing delivery-metadata transitions."""

    def __init__(self, outbox: Path) -> None:
        ensure_private_directory(outbox)
        self.path = outbox / ".claim.lock"
        self.fd = -1

    def __enter__(self) -> "_OutboxLock":
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
        self.fd = fd
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1


def _read_delivery(dir_path: Path, event_id: str) -> Optional[Dict[str, Any]]:
    try:
        raw = read_regular_json(dir_path / "delivery.json", MAX_DELIVERY_BYTES)
    except FileNotFoundError:
        return None
    except SemanticEventError:
        raise
    return _validate_delivery(raw, event_id)


def _write_delivery(dir_path: Path, delivery: Dict[str, Any]) -> None:
    _validate_delivery(delivery, delivery["event_id"])
    atomic_replace_json(dir_path / "delivery.json", delivery)


def _event_identity_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    """Event content minus the publication timestamp.

    Two publications of the same logical event may carry different
    ``created_at`` values (rebuild after a duplicate observation); identity
    equality is what makes them the same event.
    """
    return {key: value for key, value in event.items() if key != "created_at"}


def publish_event(outbox: Path, event: Dict[str, Any]) -> str:
    """Atomically publish one immutable event; returns published|duplicate.

    Concurrent publishers of the same logical event produce exactly one
    ``event.json``; the loser verifies identity equality and reports
    duplicate. Different content under the same event id is a conflict.
    """
    validate_event(event)
    if len(canonical_json(event)) > MAX_EVENT_BYTES:
        raise SemanticEventError("event_too_large")
    ensure_private_directory(outbox)
    dir_path = event_dir(outbox, event["event_id"])
    ensure_private_directory(dir_path)
    event_path = dir_path / "event.json"
    published_here = False
    existing = read_json_if_present(event_path, MAX_EVENT_BYTES)
    if existing is None:
        try:
            publish_json_no_replace(event_path, event)
            published_here = True
        except FileExistsError:
            existing = read_regular_json(event_path, MAX_EVENT_BYTES)
    if not published_here and existing is not None:
        try:
            validate_event(existing)
        except SemanticEventError as exc:
            raise SemanticEventError("event_conflict", f"stored event invalid: {exc.reason}")
        if _event_identity_payload(existing) != _event_identity_payload(event):
            raise SemanticEventError(
                "event_conflict",
                "outbox already holds different content for this event id",
            )
    delivery = _read_delivery(dir_path, event["event_id"])
    if delivery is None:
        initial = _initial_delivery(event["created_at"])
        initial["event_id"] = event["event_id"]
        try:
            publish_json_no_replace(dir_path / "delivery.json", initial)
        except FileExistsError:
            pass
    return "published" if published_here else "duplicate"


def read_event(outbox: Path, event_id: str) -> Dict[str, Any]:
    return validate_event(
        read_regular_json(event_dir(outbox, event_id) / "event.json", MAX_EVENT_BYTES)
    )


def claim_next_event(
    outbox: Path,
    *,
    owner: str,
    lease_seconds: float,
    now: datetime,
    binding_filter: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Claim the next due event for this owner under the exclusive claim lock.

    ``binding_filter`` restricts claiming to events this delivery instance is
    bound to, so unrelated Codex instances can never consume one another's
    events. Stale leases (owner died mid-delivery) are recovered after their
    expiry. Directories whose ``delivery.json`` is missing (crash between
    publication and delivery-state creation) are healed as pending.
    """
    if not isinstance(owner, str) or not owner:
        raise SemanticEventError("owner_invalid")
    with _OutboxLock(outbox):
        for child in sorted(outbox.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            try:
                event = validate_event(
                    read_regular_json(child / "event.json", MAX_EVENT_BYTES)
                )
            except (FileNotFoundError, SemanticEventError):
                continue
            if binding_filter is not None and not binding_filter(event):
                continue
            delivery = _read_delivery(child, event["event_id"])
            if delivery is None:
                # Crash between event publication and delivery-state creation:
                # heal as immediately claimable pending delivery.
                delivery = _initial_delivery(None)
                delivery["event_id"] = event["event_id"]
            state = delivery["state"]
            claimable = False
            if state == "pending":
                due = delivery["next_attempt_at"]
                claimable = due is None or parse_utc(due) <= now
            elif state == "leased":
                expires = delivery["lease"]["expires_at"]
                claimable = expires is not None and parse_utc(expires) <= now
            if not claimable:
                continue
            delivery["state"] = "leased"
            delivery["lease"] = {
                "owner": owner,
                "expires_at": (
                    now + timedelta(seconds=lease_seconds)
                ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            }
            delivery["next_attempt_at"] = None
            _write_delivery(child, delivery)
            return event, delivery
    return None


def _load_for_mutation(
    outbox: Path, event_id: str, owner: str
) -> Tuple[Path, Dict[str, Any]]:
    dir_path = event_dir(outbox, event_id)
    delivery = _read_delivery(dir_path, event_id)
    if delivery is None:
        raise SemanticEventError("delivery_missing", event_id)
    if delivery["state"] == "leased" and delivery["lease"]["owner"] not in (None, owner):
        raise SemanticEventError("lease_not_held", f"{event_id} leased by other owner")
    return dir_path, delivery


def acknowledge_event(
    outbox: Path,
    event_id: str,
    *,
    owner: str,
    now: datetime,
    thread_id: str,
    turn_id: str,
) -> str:
    """Mark delivered with the returned turn id, idempotently."""
    for value, name in ((thread_id, "thread_id"), (turn_id, "turn_id")):
        if not isinstance(value, str) or not value:
            raise SemanticEventError(f"{name}_invalid")
    with _OutboxLock(outbox):
        dir_path, delivery = _load_for_mutation(outbox, event_id, owner)
        if delivery["state"] == "delivered":
            return "already_delivered"
        if delivery["state"] != "leased":
            raise SemanticEventError("not_leased", event_id)
        stamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        delivery["state"] = "delivered"
        delivery["lease"] = {"owner": None, "expires_at": None}
        delivery["next_attempt_at"] = None
        delivery["finished_at"] = stamp
        delivery["delivery"] = {
            "thread_id": thread_id,
            "turn_id": turn_id,
            "delivered_at": stamp,
        }
        _write_delivery(dir_path, delivery)
    return "acknowledged"


def sanitize_safe_message(text: str) -> str:
    cleaned = "".join(ch if ch.isprintable() and ch not in "\r\n\t" else " " for ch in text)
    return cleaned[:MAX_SAFE_MESSAGE_CHARS]


def compute_backoff(
    attempts: int,
    *,
    initial_seconds: float,
    max_seconds: float,
    rng: Callable[[], float],
) -> float:
    """Exponential backoff with jitter, bounded by the configured maximum."""
    base = min(max_seconds, initial_seconds * (2 ** max(0, attempts - 1)))
    jitter = 0.5 + rng()
    return min(max_seconds, base * jitter)


def record_delivery_failure(
    outbox: Path,
    event_id: str,
    *,
    owner: str,
    code: str,
    safe_message: str,
    retryable: bool,
    now: datetime,
    max_attempts: int,
    backoff_initial_seconds: float,
    backoff_max_seconds: float,
    rng: Optional[Callable[[], float]] = None,
) -> str:
    """Record one failed attempt; schedule retry with backoff or dead-letter."""
    if not isinstance(code, str) or not (1 <= len(code) <= 64):
        raise SemanticEventError("failure_code_invalid")
    rng = rng or secrets.SystemRandom().random
    with _OutboxLock(outbox):
        dir_path, delivery = _load_for_mutation(outbox, event_id, owner)
        if delivery["state"] == "delivered":
            return "already_delivered"
        delivery["attempts"] += 1
        delivery["last_error"] = {
            "code": code,
            "safe_message": sanitize_safe_message(safe_message),
        }
        attempts = delivery["attempts"]
        if retryable and attempts < max_attempts:
            delay = compute_backoff(
                attempts,
                initial_seconds=backoff_initial_seconds,
                max_seconds=backoff_max_seconds,
                rng=rng,
            )
            delivery["state"] = "pending"
            delivery["lease"] = {"owner": None, "expires_at": None}
            delivery["next_attempt_at"] = (
                (now + timedelta(seconds=delay))
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
            outcome = "scheduled_retry"
        else:
            delivery["state"] = "dead_letter"
            delivery["lease"] = {"owner": None, "expires_at": None}
            delivery["next_attempt_at"] = None
            delivery["finished_at"] = (
                now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            )
            outcome = "dead_lettered"
        _write_delivery(dir_path, delivery)
    return outcome


def release_event(outbox: Path, event_id: str, *, owner: str) -> str:
    """Release a held lease without counting a failure (daemon shutdown)."""
    with _OutboxLock(outbox):
        dir_path, delivery = _load_for_mutation(outbox, event_id, owner)
        if delivery["state"] != "leased":
            return "not_leased"
        delivery["state"] = "pending"
        delivery["lease"] = {"owner": None, "expires_at": None}
        delivery["next_attempt_at"] = None
        _write_delivery(dir_path, delivery)
    return "released"


def list_outbox(outbox: Path) -> List[Dict[str, Any]]:
    """Safe summary of outbox entries for inspection commands."""
    entries: List[Dict[str, Any]] = []
    if not outbox.exists():
        return entries
    for child in sorted(outbox.iterdir()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        try:
            event = validate_event(
                read_regular_json(child / "event.json", MAX_EVENT_BYTES)
            )
        except (FileNotFoundError, SemanticEventError) as exc:
            entries.append(
                {"event_id": f"sha256:{child.name}", "state": "unreadable",
                 "problem": getattr(exc, "reason", "missing")}
            )
            continue
        delivery = _read_delivery(child, event["event_id"])
        entries.append(
            {
                "event_id": event["event_id"],
                "event": event["event"],
                "backend": event["monitor"]["backend"],
                "handle": event["monitor"]["handle"],
                "generation": event["monitor"]["generation"],
                "binding_instance": event["binding"]["app_server_instance"],
                "state": delivery["state"] if delivery else "pending",
                "attempts": delivery["attempts"] if delivery else 0,
                "next_attempt_at": delivery["next_attempt_at"] if delivery else None,
                "last_error_code": (delivery or {}).get("last_error", {}).get("code"),
            }
        )
    return entries


def cleanup_outbox(
    outbox: Path,
    *,
    now: datetime,
    older_than_seconds: float,
    include_dead_letter: bool = False,
    apply: bool = False,
) -> List[str]:
    """Remove delivered (and optionally dead-lettered) event directories.

    Terminal evidence in run directories is never touched. Dry-run by
    default; pass ``apply=True`` to actually remove.
    """
    older_than = now - timedelta(seconds=older_than_seconds)
    removed: List[str] = []
    if not outbox.exists():
        return removed
    with _OutboxLock(outbox):
        for child in sorted(outbox.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            try:
                delivery = _read_delivery(child, f"sha256:{child.name}")
            except (FileNotFoundError, SemanticEventError):
                continue
            if delivery is None:
                continue
            state = delivery["state"]
            eligible = state == "delivered" or (
                include_dead_letter and state == "dead_letter"
            )
            if not eligible:
                continue
            finished = parse_utc(delivery["finished_at"])
            if finished is None or finished > older_than:
                continue
            if apply:
                for name in ("delivery.json", "event.json"):
                    try:
                        (child / name).unlink()
                    except FileNotFoundError:
                        pass
                try:
                    child.rmdir()
                except OSError as exc:
                    if exc.errno != errno.ENOTEMPTY:
                        raise
            removed.append(f"sha256:{child.name}")
    return removed


# ---------------------------------------------------------------------------
# Idempotent postflight markers
# ---------------------------------------------------------------------------


def postflight_path(state_dir: Path, event_id: str) -> Path:
    if not isinstance(event_id, str) or not event_id.startswith("sha256:"):
        raise SemanticEventError("event_id_invalid")
    return Path(state_dir) / "postflight" / event_id[len("sha256:"):]


def postflight_check(state_dir: Path, event_id: str) -> Dict[str, Any]:
    path = postflight_path(state_dir, event_id)
    record = read_json_if_present(path, MAX_POSTFLIGHT_BYTES)
    if record is None:
        return {"event_id": event_id, "processed": False, "record": None}
    if (
        record.get("schema") != POSTFLIGHT_SCHEMA
        or record.get("event_id") != event_id
        or not isinstance(record.get("terminal_digest"), str)
        or not SHA256_PREFIX_RE.fullmatch(record["terminal_digest"])
    ):
        raise SemanticEventError("postflight_record_invalid", str(path))
    return {"event_id": event_id, "processed": True, "record": record}


def postflight_mark(
    state_dir: Path, event_id: str, *, terminal_digest: str, now: Optional[str] = None
) -> str:
    """Record that one event's postflight side effects have been performed.

    Returns ``marked`` on first call, ``already_marked`` when the identical
    marker exists (idempotent duplicate wake), and ``digest_conflict`` when
    the same event id was already marked against different terminal evidence
    (fail closed; the caller must not perform side effects).
    """
    if not isinstance(terminal_digest, str) or not SHA256_PREFIX_RE.fullmatch(
        terminal_digest
    ):
        raise SemanticEventError("terminal_digest_invalid")
    record = {
        "schema": POSTFLIGHT_SCHEMA,
        "event_id": event_id,
        "terminal_digest": terminal_digest,
        "marked_at": now or utc_now(),
    }
    path = postflight_path(state_dir, event_id)
    ensure_private_directory(path.parent)
    try:
        publish_json_no_replace(path, record)
        return "marked"
    except FileExistsError:
        existing = read_regular_json(path, MAX_POSTFLIGHT_BYTES)
        if existing.get("terminal_digest") == terminal_digest:
            return "already_marked"
        return "digest_conflict"

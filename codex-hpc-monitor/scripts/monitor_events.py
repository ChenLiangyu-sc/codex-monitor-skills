#!/usr/bin/env python3
"""Inspect monitor event lifecycles and emit optional non-model notifications.

This module is vendored as a byte-identical copy into both monitor skills.
Notification sinks are independent from the Codex wake delivery state: they
never acknowledge the App Server outbox, never start model turns, and never
become terminal authority. Sink receipts are at-least-once records; a crash
after an external notification but before its receipt may duplicate it.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
import semantic_events as se


PREFIX = "codex-monitor.events"
SINK_RECEIPT_SCHEMA = "codex-monitor.sink-receipt/v1"
SINK_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")


def _event_ids(state_dir: Path) -> Iterable[str]:
    for entry in se.list_outbox(se.outbox_root(state_dir)):
        event_id = entry.get("event_id")
        if isinstance(event_id, str):
            yield event_id


def _sink_dir(state_dir: Path, sink_id: str) -> Path:
    if not SINK_ID_RE.fullmatch(sink_id):
        raise se.SemanticEventError("sink_id_invalid")
    root = state_dir / "sinks"
    se.ensure_private_directory(root)
    directory = root / sink_id
    se.ensure_private_directory(directory)
    return directory


def _sink_receipt_path(state_dir: Path, sink_id: str, event_id: str) -> Path:
    event_path = se.event_dir(se.outbox_root(state_dir), event_id)
    return _sink_dir(state_dir, sink_id) / f"{event_path.name}.json"


def _read_sink_receipt(path: Path) -> Optional[Dict[str, Any]]:
    value = se.read_json_if_present(path, 4096)
    if value is None:
        return None
    if (
        not isinstance(value, dict)
        or set(value) != {
            "schema", "sink_id", "mode", "destination_id", "event_id", "emitted_at"
        }
        or value.get("schema") != SINK_RECEIPT_SCHEMA
        or not isinstance(value.get("sink_id"), str)
        or not SINK_ID_RE.fullmatch(value["sink_id"])
        or value.get("mode") not in {"stdout", "jsonl", "desktop"}
        or not isinstance(value.get("destination_id"), str)
        or not se.SHA256_PREFIX_RE.fullmatch(value["destination_id"])
        or not isinstance(value.get("event_id"), str)
        or not isinstance(value.get("emitted_at"), str)
        or se.parse_utc(value["emitted_at"]) is None
    ):
        raise se.SemanticEventError("sink_receipt_invalid")
    return value


def _safe_notification(event: Dict[str, Any], sink_id: str) -> Dict[str, Any]:
    monitor = event["monitor"]
    return {
        "schema_version": f"{PREFIX}.notification/v1",
        "sink_id": sink_id,
        "event_id": event["event_id"],
        "event": event["event"],
        "backend": monitor["backend"],
        "handle": monitor["handle"],
        "generation": monitor["generation"],
        "exit_code": event["exit_code"],
        "terminal_digest": monitor["terminal_digest"],
        "created_at": event["created_at"],
        "business_verdict": "pending",
    }


def _ensure_output_directory(path: Path) -> None:
    """Create a missing output directory without chmod-ing existing paths."""
    path = path.expanduser()
    try:
        info = path.lstat()
    except FileNotFoundError:
        parent = path.parent
        if parent == path:
            raise se.SemanticEventError("sink_output_parent_missing")
        _ensure_output_directory(parent)
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
        info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise se.SemanticEventError("sink_output_directory_unsafe")


def _append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    path = path.expanduser()
    _ensure_output_directory(path.parent)
    if path.exists():
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise se.SemanticEventError("sink_output_not_regular")
        if info.st_mode & 0o077:
            raise se.SemanticEventError("sink_output_permissions_too_open")
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(str(path), flags, 0o600)
    try:
        os.write(fd, se.canonical_json(record))
        os.fsync(fd)
    finally:
        os.close(fd)


def _emit_notification(args: argparse.Namespace, record: Dict[str, Any]) -> None:
    if args.mode == "stdout":
        print(json.dumps(record, sort_keys=True), flush=True)
        return
    if args.mode == "jsonl":
        if args.output is None:
            raise se.SemanticEventError("sink_output_required")
        _append_jsonl(Path(args.output), record)
        return
    title = f"Codex monitor: {record['event']}"
    body = f"{record['backend']} {record['handle']}"
    if sys.platform == "darwin":
        command = [
            "osascript", "-e",
            "on run argv\ndisplay notification (item 2 of argv) with title (item 1 of argv)\nend run",
            title, body,
        ]
    else:
        binary = shutil.which("notify-send")
        if binary is None:
            raise se.SemanticEventError("desktop_notifier_unavailable")
        command = [binary, title, body]
    completed = subprocess.run(
        command, text=True, capture_output=True, check=False, timeout=10
    )
    if completed.returncode != 0:
        raise se.SemanticEventError("desktop_notification_failed")


def _sink_destination_id(args: argparse.Namespace) -> str:
    if args.mode == "jsonl":
        if args.output is None:
            raise se.SemanticEventError("sink_output_required")
        destination = str(Path(args.output).expanduser().resolve(strict=False))
    else:
        destination = args.mode
    return se.sha256_prefix(f"{args.mode}\0{destination}".encode())


def _notify_one(args: argparse.Namespace, event_id: str) -> bool:
    state_dir = Path(args.state_dir)
    destination_id = _sink_destination_id(args)
    receipt_path = _sink_receipt_path(state_dir, args.sink_id, event_id)
    lock_path = _sink_dir(state_dir, args.sink_id) / ".notify.lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        receipt = _read_sink_receipt(receipt_path)
        if receipt is not None:
            if (
                receipt["sink_id"] != args.sink_id
                or receipt["event_id"] != event_id
                or receipt["mode"] != args.mode
                or receipt["destination_id"] != destination_id
            ):
                raise se.SemanticEventError("sink_receipt_identity_mismatch")
            return False
        event = se.read_event(se.outbox_root(state_dir), event_id)
        notification = _safe_notification(event, args.sink_id)
        _emit_notification(args, notification)
        receipt = {
            "schema": SINK_RECEIPT_SCHEMA,
            "sink_id": args.sink_id,
            "mode": args.mode,
            "destination_id": destination_id,
            "event_id": event_id,
            "emitted_at": se.utc_now(),
        }
        se.publish_json_no_replace(receipt_path, receipt)
    return True


def notify_command(args: argparse.Namespace) -> int:
    emitted = 0
    while True:
        for event_id in _event_ids(Path(args.state_dir)):
            if _notify_one(args, event_id):
                emitted += 1
                if args.once:
                    print(json.dumps({
                        "schema_version": f"{PREFIX}.notify/v1",
                        "state": "emitted", "emitted": emitted,
                    }, sort_keys=True))
                    return 0
        if args.once:
            print(json.dumps({
                "schema_version": f"{PREFIX}.notify/v1",
                "state": "idle", "emitted": emitted,
            }, sort_keys=True))
            return 0
        time.sleep(args.poll_seconds)


def _sink_receipts(state_dir: Path, event_id: str) -> list[Dict[str, Any]]:
    receipts: list[Dict[str, Any]] = []
    root = state_dir / "sinks"
    if not root.is_dir() or root.is_symlink():
        return receipts
    for sink in sorted(root.iterdir()):
        if not sink.is_dir() or sink.is_symlink() or not SINK_ID_RE.fullmatch(sink.name):
            continue
        try:
            receipt = _read_sink_receipt(
                _sink_receipt_path(state_dir, sink.name, event_id)
            )
        except (OSError, se.SemanticEventError):
            continue
        if (
            receipt is not None
            and receipt["sink_id"] == sink.name
            and receipt["event_id"] == event_id
        ):
            receipts.append(receipt)
    return receipts


def _timeline_record(state_dir: Path, event_id: str) -> Dict[str, Any]:
    outbox = se.outbox_root(state_dir)
    event = se.read_event(outbox, event_id)
    delivery = se._read_delivery(se.event_dir(outbox, event_id), event_id)
    postflight = se.postflight_check(state_dir, event_id)["record"]
    stages = [{"stage": "event_published", "at": event["created_at"]}]
    if delivery is not None:
        if delivery["delivery"]["delivered_at"] is not None:
            stages.append({
                "stage": "wake_turn_completed",
                "at": delivery["delivery"]["delivered_at"],
                "turn_id": delivery["delivery"]["turn_id"],
                "status": delivery["turn_status"],
            })
        elif delivery["last_error"]["code"] is not None:
            stages.append({
                "stage": "delivery_failed",
                "at": delivery["finished_at"],
                "code": delivery["last_error"]["code"],
                "state": delivery["state"],
            })
    for receipt in _sink_receipts(state_dir, event_id):
        stages.append({
            "stage": "notification_emitted",
            "at": receipt["emitted_at"],
            "sink_id": receipt["sink_id"],
            "mode": receipt["mode"],
        })
    if postflight is not None:
        stages.append({
            "stage": "postflight_begun",
            "at": postflight.get("started_at"),
            "owner": postflight.get("owner"),
        })
        if postflight.get("state", "completed") == "completed":
            stages.append({
                "stage": "postflight_completed",
                "at": postflight.get("completed_at"),
            })
    return {
        "event_id": event_id,
        "event": event["event"],
        "backend": event["monitor"]["backend"],
        "handle": event["monitor"]["handle"],
        "generation": event["monitor"]["generation"],
        "published_at": event["created_at"],
        "delivery_state": delivery["state"] if delivery else "missing",
        "attempts": delivery["attempts"] if delivery else 0,
        "last_error": delivery["last_error"] if delivery else None,
        "stages": stages,
    }


def timeline_command(args: argparse.Namespace) -> int:
    records = []
    for event_id in _event_ids(Path(args.state_dir)):
        record = _timeline_record(Path(args.state_dir), event_id)
        if args.event_id is not None and event_id != args.event_id:
            continue
        if args.handle is not None and record["handle"] != args.handle:
            continue
        records.append(record)
    records.sort(key=lambda record: (record["published_at"], record["event_id"]))
    if args.limit is not None:
        records = records[-args.limit:]
    print(json.dumps({
        "schema_version": f"{PREFIX}.timeline/v1",
        "events": records,
    }, sort_keys=True))
    if args.event_id is not None and not records:
        return 3
    return 0


def retry_command(args: argparse.Namespace) -> int:
    outcome = se.retry_dead_letter(
        se.outbox_root(Path(args.state_dir)), args.event_id,
        confirm=args.i_mean_it,
    )
    print(json.dumps({
        "schema_version": f"{PREFIX}.retry/v1",
        "event_id": args.event_id,
        "state": outcome,
    }, sort_keys=True))
    return {"scheduled": 0, "confirmation_required": 4}.get(outcome, 3)


def positive_float(value: str) -> float:
    result = float(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def positive_int(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)

    timeline = sub.add_parser("timeline", help="show event delivery/postflight history")
    timeline.add_argument("--state-dir", type=Path, required=True)
    selector = timeline.add_mutually_exclusive_group()
    selector.add_argument("--event-id")
    selector.add_argument("--handle")
    timeline.add_argument("--limit", type=positive_int)
    timeline.set_defaults(func=timeline_command)

    notify = sub.add_parser("notify", help="emit non-model notifications")
    notify.add_argument("--state-dir", type=Path, required=True)
    notify.add_argument("--sink-id", required=True)
    notify.add_argument("--mode", choices=("stdout", "jsonl", "desktop"), required=True)
    notify.add_argument("--output", type=Path)
    notify.add_argument("--once", action="store_true")
    notify.add_argument("--poll-seconds", type=positive_float, default=5.0)
    notify.set_defaults(func=notify_command)

    retry = sub.add_parser("retry", help="human-only retry of one dead-letter event")
    retry.add_argument("event_id")
    retry.add_argument("--state-dir", type=Path, required=True)
    retry.add_argument("--i-mean-it", action="store_true")
    retry.set_defaults(func=retry_command)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        return int(args.func(args))
    except (OSError, ValueError, subprocess.TimeoutExpired, se.SemanticEventError) as exc:
        print(json.dumps({
            "schema_version": f"{PREFIX}.error/v1",
            "state": "error",
            "reason": exc.reason if isinstance(exc, se.SemanticEventError) else type(exc).__name__,
        }, sort_keys=True))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

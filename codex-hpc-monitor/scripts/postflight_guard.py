#!/usr/bin/env python3
"""Idempotency guard for wake-event postflight work.

This module is vendored as a byte-identical copy into each monitor skill.
A woken Codex turn must, before performing any postflight side effects:

1. verify the immutable terminal record whose digest the event carries;
2. atomically run ``begin`` for the wake ``event_id`` and retain its owner;
3. only the successful claimant performs the postflight side effects;
4. run ``complete`` with the same owner.

``mark`` remains only for a postflight that is itself one atomic action. It
must not wrap a multi-step or external side effect because it cannot reserve
that work before execution. Digest mismatches always fail closed.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path


_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
import semantic_events as se


POSTFLIGHT_PREFIX = "codex-monitor.postflight"


def check_command(args: argparse.Namespace) -> int:
    try:
        result = se.postflight_check(Path(args.state_dir), args.event_id)
    except se.SemanticEventError as exc:
        print(json.dumps({
            "schema_version": f"{POSTFLIGHT_PREFIX}.check/v1",
            "state": "error",
            "reason": exc.reason,
        }, sort_keys=True))
        return 12
    payload = {
        "schema_version": f"{POSTFLIGHT_PREFIX}.check/v1",
        "event_id": result["event_id"],
        "processed": result["processed"],
        "record": result["record"],
        # The guard never authorizes anything by itself: reading and
        # verifying the terminal evidence remains mandatory.
        "terminal_evidence_required": True,
    }
    print(json.dumps(payload, sort_keys=True))
    return 3 if result["processed"] else 0


def mark_command(args: argparse.Namespace) -> int:
    try:
        outcome = se.postflight_mark(
            Path(args.state_dir),
            args.event_id,
            terminal_digest=args.terminal_digest,
        )
    except se.SemanticEventError as exc:
        print(json.dumps({
            "schema_version": f"{POSTFLIGHT_PREFIX}.mark/v1",
            "state": "error",
            "reason": exc.reason,
        }, sort_keys=True))
        return 12
    payload = {
        "schema_version": f"{POSTFLIGHT_PREFIX}.mark/v1",
        "event_id": args.event_id,
        "terminal_digest": args.terminal_digest,
        "state": outcome,
    }
    print(json.dumps(payload, sort_keys=True))
    return {"marked": 0, "already_marked": 3}.get(outcome, 4)


def begin_command(args: argparse.Namespace) -> int:
    owner = args.owner or f"{socket.gethostname()}:{os.getpid()}:{time.time_ns():x}"
    try:
        outcome = se.postflight_begin(
            Path(args.state_dir),
            args.event_id,
            terminal_digest=args.terminal_digest,
            owner=owner,
        )
    except se.SemanticEventError as exc:
        print(json.dumps({
            "schema_version": f"{POSTFLIGHT_PREFIX}.begin/v1",
            "state": "error",
            "reason": exc.reason,
        }, sort_keys=True))
        return 12
    print(json.dumps({
        "schema_version": f"{POSTFLIGHT_PREFIX}.begin/v1",
        "event_id": args.event_id,
        "terminal_digest": args.terminal_digest,
        "owner": owner,
        "state": outcome,
        # An unknown in_progress result must fail closed: report it and
        # never take over; a human decides via reset.
    }, sort_keys=True))
    return {
        "begun": 0,
        "already_completed": 3,
        "already_in_progress": 5,
        "digest_conflict": 4,
    }.get(outcome, 12)


def complete_command(args: argparse.Namespace) -> int:
    try:
        outcome = se.postflight_complete(
            Path(args.state_dir), args.event_id, owner=args.owner
        )
    except se.SemanticEventError as exc:
        print(json.dumps({
            "schema_version": f"{POSTFLIGHT_PREFIX}.complete/v1",
            "state": "error",
            "reason": exc.reason,
        }, sort_keys=True))
        return 12
    print(json.dumps({
        "schema_version": f"{POSTFLIGHT_PREFIX}.complete/v1",
        "event_id": args.event_id,
        "owner": args.owner,
        "state": outcome,
    }, sort_keys=True))
    return {"completed": 0, "already_completed": 3}.get(outcome, 4)


def reset_command(args: argparse.Namespace) -> int:
    try:
        outcome = se.postflight_reset(
            Path(args.state_dir), args.event_id, confirm=args.i_mean_it
        )
    except se.SemanticEventError as exc:
        print(json.dumps({
            "schema_version": f"{POSTFLIGHT_PREFIX}.reset/v1",
            "state": "error",
            "reason": exc.reason,
        }, sort_keys=True))
        return 12
    print(json.dumps({
        "schema_version": f"{POSTFLIGHT_PREFIX}.reset/v1",
        "event_id": args.event_id,
        "state": outcome,
    }, sort_keys=True))
    return {"reset": 0}.get(outcome, 4)


def list_command(args: argparse.Namespace) -> int:
    directory = Path(args.state_dir) / "postflight"
    records = []
    if directory.is_dir():
        for child in sorted(directory.iterdir()):
            try:
                result = se.postflight_check(Path(args.state_dir), f"sha256:{child.name}")
            except se.SemanticEventError:
                continue
            records.append(result["record"])
    print(json.dumps({
        "schema_version": f"{POSTFLIGHT_PREFIX}.list/v1",
        "records": records,
    }, sort_keys=True))
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check", help="has this event's postflight already run?")
    check.add_argument("event_id")
    check.add_argument("--state-dir", type=Path, required=True)
    check.set_defaults(func=check_command)
    mark = sub.add_parser("mark", help="record completed postflight (idempotent)")
    mark.add_argument("event_id")
    mark.add_argument("--terminal-digest", required=True)
    mark.add_argument("--state-dir", type=Path, required=True)
    mark.set_defaults(func=mark_command)
    begin = sub.add_parser(
        "begin", help="atomically claim the postflight before side effects"
    )
    begin.add_argument("event_id")
    begin.add_argument("--terminal-digest", required=True)
    begin.add_argument("--owner", help="claim owner; defaults to this turn identity")
    begin.add_argument("--state-dir", type=Path, required=True)
    begin.set_defaults(func=begin_command)

    complete = sub.add_parser("complete", help="record the claimed postflight done")
    complete.add_argument("event_id")
    complete.add_argument("--owner", required=True)
    complete.add_argument("--state-dir", type=Path, required=True)
    complete.set_defaults(func=complete_command)

    reset = sub.add_parser(
        "reset", help="human-only recovery for a stuck in_progress claim"
    )
    reset.add_argument("event_id")
    reset.add_argument("--state-dir", type=Path, required=True)
    reset.add_argument(
        "--i-mean-it",
        action="store_true",
        help="required confirmation; an unknown postflight result fails closed",
    )
    reset.set_defaults(func=reset_command)

    listing = sub.add_parser("list", help="list recorded postflight markers")
    listing.add_argument("--state-dir", type=Path, required=True)
    listing.set_defaults(func=list_command)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        return int(args.func(args))
    except (OSError, ValueError) as exc:
        print(json.dumps({
            "schema_version": f"{POSTFLIGHT_PREFIX}.error/v1",
            "state": "error",
            "error_type": type(exc).__name__,
            "detail": str(exc),
        }, sort_keys=True))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

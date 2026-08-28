#!/usr/bin/env python3
"""Idempotency guard for wake-event postflight work.

This module is vendored as a byte-identical copy into each monitor skill.
A woken Codex turn must, before performing any postflight side effects:

1. run ``check`` for the wake ``event_id`` — if already processed, report
   that and stop; never repeat external mutations or acceptance decisions;
2. verify the immutable terminal record whose digest the event carries;
3. perform the postflight exactly once;
4. run ``mark`` with the verified ``terminal_digest``.

``mark`` is idempotent for identical evidence and fails closed (exit 4)
when the same event id was already marked against different terminal
evidence — a digest mismatch blocks postflight.
"""

from __future__ import annotations

import argparse
import json
import sys
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

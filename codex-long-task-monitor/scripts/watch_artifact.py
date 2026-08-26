#!/usr/bin/env python3
"""Wait for a file or JSON artifact to satisfy an explicit terminal contract."""

from __future__ import annotations

import argparse
import errno
import json
import numbers
import os
import stat
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence


MISSING = object()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def emit(event: str, path: Path, **details: Any) -> None:
    payload = {"event": event, "observed_at": utc_now(), "path": str(path)}
    payload.update(details)
    print(json.dumps(payload, ensure_ascii=True, sort_keys=True), flush=True)


def parse_json_literal(raw: str, option: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(
            f"{option} value must be valid JSON: {raw!r}"
        ) from exc


def resolve_field(document: Any, dotted_path: str) -> Any:
    current = document
    for component in dotted_path.split("."):
        if not component:
            return MISSING
        if isinstance(current, dict) and component in current:
            current = current[component]
            continue
        if isinstance(current, list) and component.isdigit():
            index = int(component)
            if 0 <= index < len(current):
                current = current[index]
                continue
        return MISSING
    return current


def parse_expectations(values: Iterable[str]) -> list[tuple[str, Any]]:
    expectations = []
    for raw in values:
        field, separator, literal = raw.partition("=")
        if not separator or not field.strip():
            raise argparse.ArgumentTypeError(
                f"--expect-json must use FIELD=JSON_LITERAL: {raw!r}"
            )
        expectations.append(
            (field.strip(), parse_json_literal(literal, "--expect-json"))
        )
    return expectations


def nonempty(value: Any) -> bool:
    if value is MISSING or value is None:
        return False
    if isinstance(value, (str, bytes, list, tuple, dict, set)):
        return len(value) > 0
    return True


def json_values_equal(left: Any, right: Any) -> bool:
    """Compare decoded JSON values without Python's bool/int aliasing."""
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, numbers.Number) or isinstance(right, numbers.Number):
        return (
            isinstance(left, numbers.Number)
            and not isinstance(left, bool)
            and isinstance(right, numbers.Number)
            and not isinstance(right, bool)
            and left == right
        )
    if isinstance(left, str) or isinstance(right, str):
        return isinstance(left, str) and isinstance(right, str) and left == right
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


def json_value_in(value: Any, candidates: Sequence[Any]) -> bool:
    return any(json_values_equal(value, candidate) for candidate in candidates)


def json_value_sets_overlap(left: Sequence[Any], right: Sequence[Any]) -> bool:
    return any(json_value_in(value, right) for value in left)


def open_artifact(path: Path):
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    metadata = os.fstat(fd)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(fd)
        raise ValueError("artifact is not a regular file")
    return os.fdopen(fd, "rb"), metadata


def metadata_from_stat(metadata: os.stat_result) -> dict[str, Any]:
    return {"size_bytes": metadata.st_size, "mtime_ns": metadata.st_mtime_ns}


def read_json_artifact(path: Path, max_json_bytes: int) -> tuple[Any, dict[str, Any]]:
    handle, opened_metadata = open_artifact(path)
    with handle:
        before = os.fstat(handle.fileno())
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            opened_metadata.st_dev,
            opened_metadata.st_ino,
            opened_metadata.st_size,
            opened_metadata.st_mtime_ns,
        ):
            raise ValueError("artifact changed while being opened")
        if before.st_size > max_json_bytes:
            raise ValueError(f"artifact exceeds --max-json-bytes ({max_json_bytes})")
        payload = handle.read(max_json_bytes + 1)
        after = os.fstat(handle.fileno())
    if len(payload) > max_json_bytes:
        raise ValueError(f"artifact exceeds --max-json-bytes ({max_json_bytes})")
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ValueError("artifact changed while being read")
    return json.loads(payload), metadata_from_stat(after)


def evaluate_json(
    document: Any,
    status_field: str,
    success_values: Sequence[Any],
    failure_values: Sequence[Any],
    expectations: Sequence[tuple[str, Any]],
    required_nonempty: Sequence[str],
) -> tuple[str, dict[str, Any]]:
    for field, expected in expectations:
        actual = resolve_field(document, field)
        if actual is MISSING:
            return "contract_failure", {"detail": f"missing identity field: {field}"}
        if not json_values_equal(actual, expected):
            return "contract_failure", {"detail": f"identity mismatch: {field}"}

    status = resolve_field(document, status_field)
    if status is MISSING:
        return "invalid_artifact", {"detail": f"missing status field: {status_field}"}
    if json_value_in(status, failure_values):
        return "terminal_failure", {"status_match": "failure"}
    if json_value_in(status, success_values):
        missing_fields = [
            field for field in required_nonempty if not nonempty(resolve_field(document, field))
        ]
        if missing_fields:
            return "contract_failure", {
                "detail": "required success content is empty",
                "fields": missing_fields,
                "status_match": "success",
            }
        return "completed", {"status_match": "success"}
    return "waiting", {}


def watch(
    path: Path,
    *,
    poll_seconds: float,
    timeout_seconds: float,
    invalid_grace_seconds: float,
    min_bytes: int,
    exists_is_success: bool,
    status_field: str,
    success_values: Sequence[Any],
    failure_values: Sequence[Any],
    expectations: Sequence[tuple[str, Any]],
    required_nonempty: Sequence[str],
    max_json_bytes: int = 8 * 1024 * 1024,
    not_before_epoch_seconds: float | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    started = clock()
    invalid_since: float | None = None

    while True:
        now = clock()
        if now - started >= timeout_seconds:
            emit("timeout", path, elapsed_seconds=round(now - started, 3))
            return 4

        try:
            try:
                handle, raw_metadata = open_artifact(path)
            except FileNotFoundError:
                invalid_since = None
                sleep(poll_seconds)
                continue
            except OSError as exc:
                if exc.errno == errno.ENOENT:
                    invalid_since = None
                    sleep(poll_seconds)
                    continue
                raise
            metadata = metadata_from_stat(raw_metadata)
            handle.close()
            if (
                not_before_epoch_seconds is not None
                and metadata["mtime_ns"] < int(not_before_epoch_seconds * 1_000_000_000)
            ):
                invalid_since = None
                sleep(poll_seconds)
                continue
            if metadata["size_bytes"] < min_bytes:
                raise ValueError(
                    f"artifact has {metadata['size_bytes']} bytes; requires at least {min_bytes}"
                )

            if exists_is_success:
                emit(
                    "completed",
                    path,
                    elapsed_seconds=round(now - started, 3),
                    **metadata,
                )
                return 0

            document, metadata = read_json_artifact(path, max_json_bytes)
            event, details = evaluate_json(
                document,
                status_field,
                success_values,
                failure_values,
                expectations,
                required_nonempty,
            )
            if event == "waiting":
                invalid_since = None
                sleep(poll_seconds)
                continue
            if event == "completed":
                emit(
                    event,
                    path,
                    elapsed_seconds=round(now - started, 3),
                    **metadata,
                    **details,
                )
                return 0
            if event in {"terminal_failure", "contract_failure"}:
                emit(event, path, **metadata, **details)
                return 3
            raise ValueError(details["detail"])
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            if invalid_since is None:
                invalid_since = now
            if now - invalid_since >= invalid_grace_seconds:
                metadata = {}
                try:
                    handle, raw_metadata = open_artifact(path)
                    metadata = metadata_from_stat(raw_metadata)
                    handle.close()
                except (OSError, ValueError):
                    pass
                emit("invalid_artifact", path, detail=str(exc), **metadata)
                return 5

        sleep(poll_seconds)


def positive_float(raw: str) -> float:
    value = float(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def nonnegative_float(raw: str) -> float:
    value = float(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return value


def nonnegative_int(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return value


def positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="absolute path to the terminal artifact")
    parser.add_argument("--poll-seconds", type=positive_float, default=30.0)
    parser.add_argument("--timeout-seconds", type=positive_float, required=True)
    parser.add_argument("--invalid-grace-seconds", type=nonnegative_float, default=10.0)
    parser.add_argument("--min-bytes", type=nonnegative_int, default=1)
    parser.add_argument("--max-json-bytes", type=positive_int, default=8 * 1024 * 1024)
    parser.add_argument(
        "--not-before-epoch-seconds",
        type=nonnegative_float,
        help="ignore artifacts whose mtime predates this task start time",
    )
    parser.add_argument("--exists-is-success", action="store_true")
    parser.add_argument("--json-field", default="status")
    parser.add_argument("--success-json", action="append", default=[])
    parser.add_argument("--failure-json", action="append", default=[])
    parser.add_argument("--expect-json", action="append", default=[])
    parser.add_argument("--require-nonempty", action="append", default=[])
    args = parser.parse_args()

    if not args.path.is_absolute():
        parser.error("path must be absolute")
    if args.exists_is_success:
        if any(
            (
                args.success_json,
                args.failure_json,
                args.expect_json,
                args.require_nonempty,
            )
        ):
            parser.error("--exists-is-success cannot be combined with JSON contract options")
    elif not args.success_json or not args.failure_json:
        parser.error("JSON mode requires at least one --success-json and --failure-json")

    try:
        args.success_values = [
            parse_json_literal(value, "--success-json") for value in args.success_json
        ]
        args.failure_values = [
            parse_json_literal(value, "--failure-json") for value in args.failure_json
        ]
        args.expectations = parse_expectations(args.expect_json)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    if json_value_sets_overlap(args.success_values, args.failure_values):
        parser.error("success and failure JSON values must not overlap")
    return args


def main() -> int:
    args = parse_args()
    return watch(
        args.path,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.timeout_seconds,
        invalid_grace_seconds=args.invalid_grace_seconds,
        min_bytes=args.min_bytes,
        exists_is_success=args.exists_is_success,
        status_field=args.json_field,
        success_values=args.success_values,
        failure_values=args.failure_values,
        expectations=args.expectations,
        required_nonempty=args.require_nonempty,
        max_json_bytes=args.max_json_bytes,
        not_before_epoch_seconds=args.not_before_epoch_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())

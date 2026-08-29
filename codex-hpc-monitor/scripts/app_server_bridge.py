#!/usr/bin/env python3
"""Optional Codex App Server delivery adapter for the monitor outbox.

This module is vendored as a byte-identical copy into each monitor skill.
The adapter is disabled by default and must be enabled through an explicit,
validated bridge configuration. It is a notification transport only: it
never decides business outcomes, never retries/cancels/mutates workloads,
and never becomes terminal authority.

Protocol baseline (official Codex App Server interface): newline-delimited
JSON-RPC 2.0 objects without the ``jsonrpc`` envelope field, over the stdio
of the configured command. Only the stable ``initialize`` handshake,
``thread/resume``, and ``turn/start`` methods are used. Responses are
strictly validated; unrecognized shapes fail closed.

Delivery semantics are at-least-once: a crash between App Server acceptance
and the local acknowledgement may redeliver one event, and the postflight
guard makes that harmless.
"""

from __future__ import annotations

import argparse
from collections import deque
import json
import os
import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Optional, Tuple


_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
import semantic_events as se


BRIDGE_PREFIX = "codex-monitor.bridge"
CLIENT_INFO = {"name": "codex-monitor-skills", "title": "monitor bridge", "version": "1"}
REAL_SMOKE_TESTED_CODEX_VERSIONS = {"0.150.1"}
REQUIRED_PROTOCOL_METHODS = {
    "initialize",
    "initialized",
    "thread/resume",
    "turn/start",
    "turn/completed",
}

# Delivery failure reason codes. retryable failures back off and retry;
# the rest dead-letter immediately for human inspection.
RETRYABLE_CODES = {
    "spawn_failed",
    "initialize_failed",
    "request_timeout",
    "connection_lost",
    "protocol_error",
    "overloaded",
    "server_error",
    "required_mcp_failure",
    "active_turn_conflict",
    "turn_completion_timeout",
    "turn_failed",
    "turn_aborted",
}
DEAD_LETTER_CODES = {
    "thread_missing",
    "thread_archived",
    "unsupported_response_shape",
    "binding_mismatch",
    "operator_interaction_required",
}


# Delivery context used by the lease-renewal callback inside one attempt.
_DELIVERY_STATE_DIR: list[str] = [""]
_DELIVERY_OWNER: list[str] = [""]


class DeliveryError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message

    @property
    def retryable(self) -> bool:
        return self.code in RETRYABLE_CODES


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AppServerSession:
    """Minimal JSONL JSON-RPC client for one delivery attempt.

    The session must stay open until the started turn reaches
    ``turn/completed`` (or fails): closing it right after ``turn/start``
    would abort the wake turn before its postflight finishes.
    """

    def __init__(
        self,
        command: list[str],
        request_timeout_seconds: float,
        env: dict[str, str] | None = None,
    ) -> None:
        self.request_timeout = request_timeout_seconds
        self._next_id = 0
        self._line_buffer = b""
        self._notifications: Deque[Dict[str, Any]] = deque()
        self._stderr_file = tempfile.TemporaryFile()
        spawn_env = os.environ.copy()
        if env:
            spawn_env.update(env)
        self.process = subprocess.Popen(
            list(command),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._stderr_file,
            env=spawn_env,
        )

    # -- low-level IO ---------------------------------------------------

    def _write(self, payload: Dict[str, Any]) -> None:
        assert self.process.stdin is not None
        try:
            self.process.stdin.write(
                (json.dumps(payload, separators=(",", ":")) + "\n").encode()
            )
            self.process.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise DeliveryError("connection_lost", f"write failed: {exc}") from exc

    def _read_line(self, deadline: float) -> Optional[Dict[str, Any]]:
        """Read one JSON object line by the given absolute monotonic deadline.

        Raw ``os.read`` with our own line buffer: mixing a selector with
        buffered ``readline`` would hide already-buffered lines from the
        selector and fake a timeout while data is pending.
        """
        fd = self.process.stdout.fileno()
        while True:
            if b"\n" in self._line_buffer:
                line, _, self._line_buffer = self._line_buffer.partition(b"\n")
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    value = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise DeliveryError(
                        "protocol_error", f"non-JSON line: {exc}"
                    ) from exc
                if not isinstance(value, dict):
                    raise DeliveryError("protocol_error", "response is not an object")
                return value
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            selector = selectors.DefaultSelector()
            try:
                selector.register(fd, selectors.EVENT_READ)
                events = selector.select(remaining)
            finally:
                selector.close()
            if not events:
                return None
            chunk = os.read(fd, 65536)
            if not chunk:
                raise DeliveryError("connection_lost", "app server closed its output")
            self._line_buffer += chunk

    def _stderr_tail(self) -> str:
        try:
            self._stderr_file.seek(0)
            return (
                self._stderr_file.read(4096).decode("utf-8", errors="replace").strip()
            )
        except OSError:
            return ""

    # -- protocol -------------------------------------------------------

    def request(
        self, method: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        self._next_id += 1
        request_id = self._next_id
        self._write({"method": method, "id": request_id, "params": params})
        deadline = time.monotonic() + self.request_timeout
        while True:
            message = self._read_line(deadline)
            if message is None:
                raise DeliveryError("request_timeout", f"no reply to {method}")
            # JSON-RPC request ids are scoped independently in each direction,
            # so a server request may legally reuse our current client id.
            # Method presence distinguishes it from a response; check this
            # before comparing ids and never answer it automatically.
            if "id" in message and isinstance(message.get("method"), str):
                raise DeliveryError(
                    "operator_interaction_required",
                    f"app server requested operator interaction via {message['method']}",
                )
            if message.get("id") != request_id:
                # Preserve notifications: a very short turn may complete
                # before its turn/start response is consumed. Server-initiated
                # requests (which carry an id) are outside this adapter's
                # no-approval baseline. Fail closed immediately instead of
                # making an approval look like an unexplained turn timeout.
                if "id" not in message and isinstance(message.get("method"), str):
                    self._notifications.append(message)
                continue
            if "error" in message and message["error"] is not None:
                error = message["error"]
                code = error.get("code") if isinstance(error, dict) else None
                detail = error.get("message", "") if isinstance(error, dict) else str(error)
                raise _classify_server_error(code, detail, method)
            result = message.get("result")
            if not isinstance(result, dict):
                raise DeliveryError(
                    "unsupported_response_shape",
                    f"{method} result is not an object",
                )
            return result

    def notify(self, method: str, params: Dict[str, Any]) -> None:
        self._write({"method": method, "params": params})

    def initialize(self) -> Dict[str, Any]:
        try:
            result = self.request("initialize", {"clientInfo": CLIENT_INFO})
        except DeliveryError as exc:
            if exc.code in {
                "connection_lost",
                "request_timeout",
                "protocol_error",
                "operator_interaction_required",
            }:
                raise
            raise DeliveryError("initialize_failed", exc.message) from exc
        self.notify("initialized", {})
        return result

    def resume_thread(self, thread_id: str, expected_workspace: str) -> None:
        result = self.request("thread/resume", {"threadId": thread_id})
        thread = result.get("thread")
        if not isinstance(thread, dict) or thread.get("id") != thread_id:
            raise DeliveryError(
                "unsupported_response_shape",
                "thread/resume returned a different or malformed thread",
            )
        cwd = thread.get("cwd")
        if not isinstance(cwd, str) or cwd != expected_workspace:
            # Fail closed: a thread from another workspace/CODEX_HOME must
            # never receive this event.
            raise DeliveryError(
                "binding_mismatch",
                "thread cwd does not match the bound workspace",
            )

    def start_turn(self, thread_id: str, wake_text: str) -> str:
        result = self.request(
            "turn/start",
            {
                "threadId": thread_id,
                "input": [{"type": "text", "text": wake_text}],
            },
        )
        turn = result.get("turn")
        if not isinstance(turn, dict) or not isinstance(turn.get("id"), str) or not turn.get("id"):
            raise DeliveryError(
                "unsupported_response_shape",
                "turn/start result carries no usable turn id",
            )
        if isinstance(turn.get("error"), dict):
            raise DeliveryError("server_error", "turn/start reported a turn error")
        return str(turn["id"])

    def _notification_turn_id(self, params: object) -> str | None:
        if isinstance(params, dict):
            turn = params.get("turn")
            if isinstance(turn, dict) and isinstance(turn.get("id"), str):
                return str(turn["id"])
            raw = params.get("turnId")
            if isinstance(raw, str):
                return raw
        return None

    def wait_turn_completion(
        self,
        turn_id: str,
        timeout_seconds: float,
        on_tick: Callable[[], None] | None = None,
        tick_seconds: float = 1.0,
    ) -> str:
        """Read notifications until the target turn reaches a terminal state.

        Returns the turn status (for ``turn/completed`` normally
        ``"completed"``). Raises ``turn_completion_timeout`` when the turn
        does not finish within the budget; the event then stays undelivered
        (retryable) instead of being acknowledged blindly.
        """
        deadline = time.monotonic() + timeout_seconds
        tick_interval = min(
            5.0, max(0.01, min(tick_seconds, timeout_seconds / 10))
        )
        tick_deadline = time.monotonic() + tick_interval
        while True:
            now = time.monotonic()
            if now >= deadline:
                raise DeliveryError(
                    "turn_completion_timeout",
                    f"turn {turn_id} did not complete within {timeout_seconds}s",
                )
            if self._notifications:
                message = self._notifications.popleft()
            else:
                # Never block beyond the next lease-renewal tick. This is
                # essential for valid sub-second leases.
                message = self._read_line(min(deadline, tick_deadline))
            if message is None:
                if time.monotonic() >= tick_deadline:
                    if on_tick is not None:
                        on_tick()
                    tick_deadline = time.monotonic() + tick_interval
                continue
            if "id" in message:
                if isinstance(message.get("method"), str):
                    raise DeliveryError(
                        "operator_interaction_required",
                        f"app server requested operator interaction via {message['method']}",
                    )
                continue  # stray response; no requests are pending
            method = message.get("method")
            if not isinstance(method, str):
                continue
            if method == "turn/completed":
                if self._notification_turn_id(message.get("params")) == turn_id:
                    params = message.get("params")
                    turn = params.get("turn") if isinstance(params, dict) else {}
                    status = turn.get("status") if isinstance(turn, dict) else None
                    if status == "completed":
                        return status
                    if status == "failed":
                        raise DeliveryError(
                            "turn_failed", "wake turn completed with failed status"
                        )
                    if status == "interrupted":
                        raise DeliveryError(
                            "turn_aborted",
                            "wake turn completed with interrupted status",
                        )
                    raise DeliveryError(
                        "unsupported_response_shape",
                        "turn/completed carried no supported terminal status",
                    )
                continue
            if method in {"turn/failed", "turn/aborted"}:
                if self._notification_turn_id(message.get("params")) == turn_id:
                    raise DeliveryError(
                        "turn_failed" if method == "turn/failed" else "turn_aborted",
                        f"wake turn ended via {method}",
                    )
                continue
            if time.monotonic() >= tick_deadline:
                if on_tick is not None:
                    on_tick()
                tick_deadline = time.monotonic() + tick_interval

    def close(self) -> None:
        try:
            if self.process.stdin is not None:
                try:
                    self.process.stdin.close()
                except OSError:
                    pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
        finally:
            if self.process.stdout is not None:
                try:
                    self.process.stdout.close()
                except OSError:
                    pass
            self._stderr_file.close()


def _classify_server_error(
    code: object, detail: str, method: str
) -> "DeliveryError":
    text = str(detail).lower()
    if code == -32001:
        return DeliveryError("overloaded", "app server reports overload (-32001)")
    if "archiv" in text:
        return DeliveryError("thread_archived", str(detail))
    if "not found" in text or "no such" in text or "missing" in text or "unknown thread" in text:
        return DeliveryError("thread_missing", str(detail))
    if "mcp" in text:
        return DeliveryError("required_mcp_failure", str(detail))
    if "active turn" in text or "already running" in text:
        return DeliveryError("active_turn_conflict", str(detail))
    return DeliveryError("server_error", f"{method} failed: {detail}")


def spawn_env(config: Dict[str, Any]) -> Dict[str, str]:
    """Environment pinning the App Server to the configured CODEX_HOME."""
    return {"CODEX_HOME": str(Path(config["codex_home"]).expanduser())}


def attempt_delivery(
    config: Dict[str, Any], event: Dict[str, Any]
) -> Tuple[str, str, str]:
    """Deliver one wake event into its bound thread and wait for completion.

    Returns ``(turn_id, turn_status, wake_text)``. The session stays open
    until ``turn/completed`` so the wake turn is not aborted mid-postflight,
    and the delivery lease is renewed at every stage and during the wait.
    """
    binding = event["binding"]
    if binding["workspace"] != config["workspace"]:
        raise DeliveryError(
            "binding_mismatch",
            "event workspace does not match the configured bridge workspace",
        )
    wake_text = se.wake_message(event)
    session = AppServerSession(
        config["transport"]["command"],
        float(config["request_timeout_seconds"]),
        env=spawn_env(config),
    )

    def renew() -> None:
        try:
            outcome = se.renew_event(
                se.outbox_root(Path(_DELIVERY_STATE_DIR[0])),
                event["event_id"],
                owner=_DELIVERY_OWNER[0],
                lease_seconds=float(config["lease_seconds"]),
                now=datetime.now(timezone.utc),
            )
            if outcome != "renewed":
                raise DeliveryError("lease_lost", f"lease renewal returned {outcome}")
        except se.SemanticEventError as exc:
            raise DeliveryError("lease_lost", exc.reason) from exc

    try:
        session.initialize()
        renew()
        session.resume_thread(binding["thread_id"], binding["workspace"])
        renew()
        turn_id = session.start_turn(binding["thread_id"], wake_text)
        renew()
        turn_status = session.wait_turn_completion(
            turn_id,
            float(config["turn_completion_timeout_seconds"]),
            on_tick=renew,
            tick_seconds=max(0.01, float(config["lease_seconds"]) / 3.0),
        )
        renew()
        return turn_id, turn_status, wake_text
    finally:
        session.close()


def owner_identity(config: Dict[str, Any]) -> str:
    return (
        f"{config['instance_id']}:{socket.gethostname()}:{os.getpid()}"
        f":{time.time_ns():x}"
    )


def deliver_loop(args: argparse.Namespace) -> int:
    try:
        config = se.load_bridge_config(Path(args.bridge_config))
    except se.SemanticEventError as exc:
        print(
            json.dumps(
                {"schema_version": f"{BRIDGE_PREFIX}.deliver/v1", "state": "error",
                 "reason": exc.reason, "detail": exc.detail},
                sort_keys=True,
            )
        )
        return 12
    if not config["enabled"]:
        print(
            json.dumps(
                {"schema_version": f"{BRIDGE_PREFIX}.deliver/v1",
                 "state": "refused", "reason": "bridge_disabled"},
                sort_keys=True,
            )
        )
        return 0 if getattr(args, "exit_zero_if_disabled", False) else 3
    outbox = se.outbox_root(Path(args.state_dir))
    owner = owner_identity(config)

    def binding_matches(event: Dict[str, Any]) -> bool:
        return (
            event["binding"]["app_server_instance"] == config["instance_id"]
            and event["binding"]["codex_home_id"] == config["codex_home_id"]
        )

    held_event: Dict[str, Any] = {}

    def release_held(signum: int, _frame: object) -> None:
        if held_event:
            try:
                se.release_event(outbox, held_event["event_id"], owner=owner)
            except se.SemanticEventError:
                pass
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, release_held)
    signal.signal(signal.SIGINT, release_held)

    delivered = 0
    while True:
        claimed = None
        try:
            claimed = se.claim_next_event(
                outbox,
                owner=owner,
                lease_seconds=float(config["lease_seconds"]),
                now=utc_now(),
                binding_filter=binding_matches,
            )
        except se.SemanticEventError as exc:
            print(
                json.dumps(
                    {"schema_version": f"{BRIDGE_PREFIX}.deliver/v1",
                     "state": "error", "reason": exc.reason},
                    sort_keys=True,
                )
            )
            return 12
        if claimed is None:
            if args.once:
                break
            time.sleep(float(config["poll_seconds"]))
            continue
        event, _delivery = claimed
        held_event = event
        _DELIVERY_STATE_DIR[0] = str(Path(args.state_dir))
        _DELIVERY_OWNER[0] = owner
        record: Dict[str, Any] = {
            "schema_version": f"{BRIDGE_PREFIX}.delivery/v1",
            "event_id": event["event_id"],
            "event": event["event"],
        }
        try:
            turn_id, turn_status, _wake = attempt_delivery(config, event)
            # Acknowledge only after the turn reached an interpretable state.
            ack = se.acknowledge_event(
                outbox,
                event["event_id"],
                owner=owner,
                now=utc_now(),
                thread_id=event["binding"]["thread_id"],
                turn_id=turn_id,
                turn_status=turn_status,
            )
            delivered += 1
            record.update({"state": ack, "turn_id": turn_id, "turn_status": turn_status})
        except DeliveryError as exc:
            if exc.code == "lease_lost":
                # Another owner already controls the event. The stale owner
                # must stop without mutating delivery state.
                outcome = "lease_lost"
            else:
                outcome = se.record_delivery_failure(
                    outbox,
                    event["event_id"],
                    owner=owner,
                    code=exc.code,
                    safe_message=exc.message,
                    retryable=exc.retryable,
                    now=utc_now(),
                    max_attempts=int(config["max_attempts"]),
                    backoff_initial_seconds=float(config["backoff_initial_seconds"]),
                    backoff_max_seconds=float(config["backoff_max_seconds"]),
                )
            record.update({"state": outcome, "error_code": exc.code})
        except (OSError, se.SemanticEventError) as exc:
            outcome = se.record_delivery_failure(
                outbox,
                event["event_id"],
                owner=owner,
                code="server_error",
                safe_message=str(exc),
                retryable=True,
                now=utc_now(),
                max_attempts=int(config["max_attempts"]),
                backoff_initial_seconds=float(config["backoff_initial_seconds"]),
                backoff_max_seconds=float(config["backoff_max_seconds"]),
            )
            record.update({"state": outcome, "error_code": "server_error"})
        held_event = {}
        print(json.dumps(record, sort_keys=True))
        if args.once:
            break
    print(
        json.dumps(
            {"schema_version": f"{BRIDGE_PREFIX}.deliver/v1",
             "state": "idle" if delivered == 0 else "delivered",
             "delivered": delivered},
            sort_keys=True,
        )
    )
    return 0


def status_command(args: argparse.Namespace) -> int:
    payload: Dict[str, Any] = {
        "schema_version": f"{BRIDGE_PREFIX}.status/v1",
        "state_dir": str(Path(args.state_dir)),
    }
    try:
        config = se.load_bridge_config(Path(args.bridge_config))
        payload["config"] = {
            "enabled": config["enabled"],
            "instance_id": config["instance_id"],
            "transport": config["transport"]["type"],
            "healthy": True,
        }
        payload["mode"] = (
            "external-event-bridge" if config["enabled"] else "unattended"
        )
    except se.SemanticEventError as exc:
        payload["config"] = {"healthy": False, "reason": exc.reason}
        payload["mode"] = "unattended"
    entries = se.list_outbox(se.outbox_root(Path(args.state_dir)))
    payload["outbox"] = entries
    payload["pending"] = sum(1 for e in entries if e.get("state") == "pending")
    print(json.dumps(payload, sort_keys=True))
    return 0


def _method_variant(schema: Dict[str, Any], method: str) -> Optional[Dict[str, Any]]:
    for variant in schema.get("oneOf", []):
        if not isinstance(variant, dict):
            continue
        properties = variant.get("properties")
        if not isinstance(properties, dict):
            continue
        method_schema = properties.get("method")
        if (
            isinstance(method_schema, dict)
            and method_schema.get("enum") == [method]
        ):
            return variant
    return None


def _required_fields(schema: Dict[str, Any], *fields: str) -> bool:
    required = schema.get("required")
    properties = schema.get("properties")
    return (
        isinstance(required, list)
        and isinstance(properties, dict)
        and all(field in required and field in properties for field in fields)
    )


def _definition_has_fields(
    schema: Dict[str, Any], definition: str, *fields: str
) -> bool:
    definitions = schema.get("definitions")
    if not isinstance(definitions, dict):
        return False
    value = definitions.get(definition)
    return isinstance(value, dict) and _required_fields(value, *fields)


def _property_refs_definition(
    schema: Dict[str, Any], property_name: str, definition: str
) -> bool:
    properties = schema.get("properties")
    value = properties.get(property_name) if isinstance(properties, dict) else None
    return (
        isinstance(value, dict)
        and value.get("$ref") == f"#/definitions/{definition}"
    )


def _schema_candidates(root: Path, name: str) -> list[Dict[str, Any]]:
    candidates = []
    for path in sorted(root.rglob(name)):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            candidates.append(value)
    return candidates


def _protocol_contract_failures(root: Path) -> list[str]:
    """Validate only the exact schema locations and fields the bridge uses."""
    failures: list[str] = []

    client_requests = _schema_candidates(root, "ClientRequest.json")
    for method, params_ref in (
        ("initialize", "InitializeParams"),
        ("thread/resume", "ThreadResumeParams"),
        ("turn/start", "TurnStartParams"),
    ):
        matched = False
        for schema in client_requests:
            variant = _method_variant(schema, method)
            if variant is None or not _required_fields(variant, "id", "method", "params"):
                continue
            params = variant["properties"]["params"]
            if isinstance(params, dict) and str(params.get("$ref", "")).endswith(
                f"/{params_ref}"
            ):
                matched = True
                break
        if not matched:
            failures.append(f"client_request:{method}")

    notifications = _schema_candidates(root, "ClientNotification.json")
    if not any(
        (variant := _method_variant(schema, "initialized")) is not None
        and _required_fields(variant, "method")
        for schema in notifications
    ):
        failures.append("client_notification:initialized")

    server_notifications = _schema_candidates(root, "ServerNotification.json")
    matched_completion = False
    for schema in server_notifications:
        variant = _method_variant(schema, "turn/completed")
        if variant is None or not _required_fields(variant, "method", "params"):
            continue
        params = variant["properties"]["params"]
        if isinstance(params, dict) and str(params.get("$ref", "")).endswith(
            "/TurnCompletedNotification"
        ):
            matched_completion = True
            break
    if not matched_completion:
        failures.append("server_notification:turn/completed")

    initialize_params = _schema_candidates(root, "InitializeParams.json")
    if not any(
        _required_fields(schema, "clientInfo")
        and _property_refs_definition(schema, "clientInfo", "ClientInfo")
        and _definition_has_fields(schema, "ClientInfo", "name", "version")
        for schema in initialize_params
    ):
        failures.append("initialize_params:clientInfo[name,version]")

    resume_params = _schema_candidates(root, "ThreadResumeParams.json")
    if not any(
        _required_fields(schema, "threadId")
        and isinstance(schema["properties"]["threadId"], dict)
        and schema["properties"]["threadId"].get("type") == "string"
        for schema in resume_params
    ):
        failures.append("thread_resume_params:threadId")

    start_params = _schema_candidates(root, "TurnStartParams.json")
    valid_start_params = False
    for schema in start_params:
        if not _required_fields(schema, "threadId", "input"):
            continue
        properties = schema["properties"]
        if (
            not isinstance(properties["threadId"], dict)
            or properties["threadId"].get("type") != "string"
            or not isinstance(properties["input"], dict)
        ):
            continue
        input_schema = properties["input"]
        definitions = schema.get("definitions")
        user_input = (
            definitions.get("UserInput", {})
            if isinstance(definitions, dict)
            else {}
        )
        text_variant = any(
            isinstance(item, dict)
            and _required_fields(item, "type", "text")
            and isinstance(item["properties"]["type"], dict)
            and item["properties"]["type"].get("enum") == ["text"]
            for item in user_input.get("oneOf", [])
            if isinstance(user_input, dict)
        )
        input_items = input_schema.get("items")
        if (
            input_schema.get("type") == "array"
            and isinstance(input_items, dict)
            and input_items.get("$ref") == "#/definitions/UserInput"
            and text_variant
        ):
            valid_start_params = True
            break
    if not valid_start_params:
        failures.append("turn_start_params:threadId,input[text]")

    resume_responses = _schema_candidates(root, "ThreadResumeResponse.json")
    if not any(
        _required_fields(schema, "thread")
        and _property_refs_definition(schema, "thread", "Thread")
        and _definition_has_fields(schema, "Thread", "id", "cwd")
        for schema in resume_responses
    ):
        failures.append("thread_resume_response:thread.id,cwd")

    start_responses = _schema_candidates(root, "TurnStartResponse.json")
    if not any(
        _required_fields(schema, "turn")
        and _property_refs_definition(schema, "turn", "Turn")
        and _definition_has_fields(schema, "Turn", "id")
        for schema in start_responses
    ):
        failures.append("turn_start_response:turn.id")

    completion_params = _schema_candidates(root, "TurnCompletedNotification.json")
    valid_completion = False
    for schema in completion_params:
        definitions = schema.get("definitions")
        status_schema = (
            definitions.get("TurnStatus", {})
            if isinstance(definitions, dict)
            else {}
        )
        statuses = status_schema.get("enum", []) if isinstance(status_schema, dict) else []
        if (
            _required_fields(schema, "turn")
            and _property_refs_definition(schema, "turn", "Turn")
            and _definition_has_fields(schema, "Turn", "id", "status")
            and {"completed", "failed", "interrupted"}.issubset(set(statuses))
        ):
            valid_completion = True
            break
    if not valid_completion:
        failures.append("turn_completed:turn.id,status")
    return failures


def protocol_check_command(args: argparse.Namespace) -> int:
    """Generate the installed Codex protocol schema and check our baseline.

    A successful structural check means the installed binary still exposes
    the minimal methods used by this bridge. It is distinct from a recorded
    real lifecycle smoke, which is version-specific.
    """
    codex_bin = str(args.codex_bin)
    payload: Dict[str, Any] = {
        "schema_version": f"{BRIDGE_PREFIX}.protocol-check/v1",
        "codex_bin": codex_bin,
        "required_methods": sorted(REQUIRED_PROTOCOL_METHODS),
    }
    try:
        version_run = subprocess.run(
            [codex_bin, "--version"], text=True, capture_output=True,
            check=False, timeout=float(args.timeout_seconds),
        )
        if version_run.returncode != 0:
            raise DeliveryError("version_probe_failed", "codex --version failed")
        version_text = version_run.stdout.strip()
        match = re.search(r"(?:codex-cli\s+)?(\d+\.\d+\.\d+)", version_text)
        version = match.group(1) if match else None
        payload["codex_version"] = version
        payload["version_output"] = version_text[:160]
        with tempfile.TemporaryDirectory(prefix="codex-monitor-schema-") as schema_dir:
            command = [
                codex_bin, "app-server", "generate-json-schema",
                "--out", schema_dir,
            ]
            if args.experimental:
                command.append("--experimental")
            generated = subprocess.run(
                command, text=True, capture_output=True, check=False,
                timeout=float(args.timeout_seconds),
            )
            if generated.returncode != 0:
                raise DeliveryError(
                    "schema_generation_failed",
                    "codex app-server generate-json-schema failed",
                )
            schema_path = Path(schema_dir) / "codex_app_server_protocol.schemas.json"
            if not schema_path.is_file():
                raise DeliveryError(
                    "schema_bundle_missing", "generated schema bundle is missing"
                )
            json.loads(schema_path.read_text(encoding="utf-8"))
            required_files = {
                "ClientRequest.json", "ServerNotification.json", "ServerRequest.json",
                "InitializeResponse.json", "ThreadResumeResponse.json",
                "TurnStartResponse.json", "TurnCompletedNotification.json",
            }
            missing_files = sorted(
                name for name in required_files
                if not any(Path(schema_dir).rglob(name))
            )
            contract_failures = _protocol_contract_failures(Path(schema_dir))
        payload["contract_failures"] = contract_failures
        payload["missing_schema_files"] = missing_files
        payload["schema_compatible"] = not contract_failures and not missing_files
        payload["reported_version_matches_recorded_smoke"] = (
            version in REAL_SMOKE_TESTED_CODEX_VERSIONS
        )
        payload["compatibility"] = (
            "schema_compatible_recorded_version"
            if payload["schema_compatible"]
            and payload["reported_version_matches_recorded_smoke"]
            else "schema_compatible_unverified"
            if payload["schema_compatible"]
            else "incompatible"
        )
        print(json.dumps(payload, sort_keys=True))
        if not payload["schema_compatible"]:
            return 12
        if (
            args.require_verified_version
            and not payload["reported_version_matches_recorded_smoke"]
        ):
            return 4
        return 0
    except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError, DeliveryError) as exc:
        payload.update({
            "schema_compatible": False,
            "reported_version_matches_recorded_smoke": False,
            "compatibility": "probe_failed",
            "reason": exc.code if isinstance(exc, DeliveryError) else type(exc).__name__,
        })
        print(json.dumps(payload, sort_keys=True))
        return 12


def init_config_command(args: argparse.Namespace) -> int:
    codex_home = str(Path(args.codex_home).expanduser().resolve(strict=False))
    config = {
        "schema": se.BRIDGE_CONFIG_SCHEMA,
        "enabled": args.enabled,
        "instance_id": args.instance_id,
        "codex_home": codex_home,
        "codex_home_id": se.codex_home_digest(Path(codex_home)),
        "workspace": str(Path(args.workspace).resolve(strict=False)),
        "transport": {"type": "stdio", "command": args.command},
        "request_timeout_seconds": args.request_timeout_seconds,
        "poll_seconds": args.poll_seconds,
        "lease_seconds": args.lease_seconds,
        "max_attempts": args.max_attempts,
        "backoff_initial_seconds": args.backoff_initial_seconds,
        "backoff_max_seconds": args.backoff_max_seconds,
        "turn_completion_timeout_seconds": args.turn_completion_timeout_seconds,
    }
    se.validate_bridge_config(config)
    path = Path(args.output)
    if path.exists():
        print(json.dumps({"state": "error", "reason": "output_exists"}))
        return 12
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = (json.dumps(config, indent=2, sort_keys=True) + "\n").encode()
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    print(json.dumps({"state": "written", "path": str(path)}, sort_keys=True))
    return 0


def init_binding_command(args: argparse.Namespace) -> int:
    binding = {
        "schema": se.EVENT_BINDING_SCHEMA,
        "codex_home_id": se.codex_home_digest(Path(args.codex_home)),
        "app_server_instance": args.instance_id,
        "thread_id": args.thread_id,
        "workspace": str(Path(args.workspace).resolve(strict=False)),
    }
    se.validate_event_binding(binding)
    path = Path(args.output)
    if path.exists():
        print(json.dumps({"state": "error", "reason": "output_exists"}))
        return 12
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = (json.dumps(binding, indent=2, sort_keys=True) + "\n").encode()
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    print(json.dumps({"state": "written", "path": str(path)}, sort_keys=True))
    return 0


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)

    deliver = sub.add_parser("deliver", help="deliver due outbox events (foreground)")
    deliver.add_argument("--state-dir", type=Path, required=True)
    deliver.add_argument("--bridge-config", type=Path, required=True)
    deliver.add_argument("--once", action="store_true", help="process at most one event")
    deliver.add_argument(
        "--exit-zero-if-disabled", action="store_true",
        help="service mode: disabled config is a clean stop, avoiding restart loops",
    )
    deliver.set_defaults(func=deliver_loop)

    status = sub.add_parser("status", help="summarize outbox and configuration")
    status.add_argument("--state-dir", type=Path, required=True)
    status.add_argument("--bridge-config", type=Path, required=True)
    status.set_defaults(func=status_command)

    init_config = sub.add_parser("init-config", help="write a validated config template")
    init_config.add_argument("--output", type=Path, required=True)
    init_config.add_argument("--instance-id", required=True)
    init_config.add_argument("--workspace", type=Path, required=True)
    init_config.add_argument("--codex-home", type=Path, default=Path.home() / ".codex")
    init_config.add_argument("--command", nargs="+", default=["codex", "app-server"])
    init_config.add_argument("--enabled", action="store_true")
    init_config.add_argument("--request-timeout-seconds", type=positive_float, default=30.0)
    init_config.add_argument("--poll-seconds", type=positive_float, default=5.0)
    init_config.add_argument("--lease-seconds", type=positive_float, default=300.0)
    init_config.add_argument("--max-attempts", type=positive_int, default=16)
    init_config.add_argument("--backoff-initial-seconds", type=positive_float, default=5.0)
    init_config.add_argument("--backoff-max-seconds", type=positive_float, default=3600.0)
    init_config.add_argument(
        "--turn-completion-timeout-seconds", type=positive_float, default=3600.0
    )
    init_config.set_defaults(func=init_config_command)

    init_binding = sub.add_parser("init-binding", help="write a validated binding file")
    init_binding.add_argument("--output", type=Path, required=True)
    init_binding.add_argument("--thread-id", required=True)
    init_binding.add_argument("--instance-id", required=True)
    init_binding.add_argument("--workspace", type=Path, required=True)
    init_binding.add_argument("--codex-home", type=Path, default=Path.home() / ".codex")
    init_binding.set_defaults(func=init_binding_command)

    protocol_check = sub.add_parser(
        "protocol-check",
        help="verify the installed Codex App Server schema against the bridge baseline",
    )
    protocol_check.add_argument("--codex-bin", default="codex")
    protocol_check.add_argument("--timeout-seconds", type=positive_float, default=30.0)
    protocol_check.add_argument("--experimental", action="store_true")
    protocol_check.add_argument("--require-verified-version", action="store_true")
    protocol_check.set_defaults(func=protocol_check_command)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        return int(args.func(args))
    except (OSError, ValueError) as exc:
        print(
            json.dumps(
                {"schema_version": f"{BRIDGE_PREFIX}.error/v1",
                 "state": "bridge_error",
                 "error_type": type(exc).__name__,
                 "detail": str(exc)},
                sort_keys=True,
            )
        )
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

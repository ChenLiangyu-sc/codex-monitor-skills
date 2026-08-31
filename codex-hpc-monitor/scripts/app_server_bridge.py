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
import hashlib
import json
import os
import selectors
import shutil
import signal
import socket
import stat
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
LIFECYCLE_SMOKE_SCHEMA = "codex-monitor.app-server-lifecycle-smoke/v1"
MAX_LIFECYCLE_SMOKE_BYTES = 16 * 1024
CLIENT_INFO = {"name": "codex-monitor-skills", "title": "monitor bridge", "version": "1"}
REAL_SMOKE_TESTED_CODEX_VERSIONS = {"0.150.1", "0.151.0"}
CODEX_VERSION_RE = re.compile(r"(?:codex-cli\s+)?(\d+\.\d+\.\d+)")
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
    def __init__(
        self,
        code: str,
        message: str,
        *,
        stage: str | None = None,
        app_server_exit_code: int | None = None,
        stderr_tail: str | None = None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.stage = stage
        self.app_server_exit_code = app_server_exit_code
        self.stderr_tail = stderr_tail

    def attach_diagnostics(
        self,
        *,
        stage: str,
        app_server_exit_code: int | None,
        stderr_tail: str | None,
    ) -> "DeliveryError":
        """Attach bounded process evidence without replacing the root error."""
        if self.stage is None:
            self.stage = stage
        if self.app_server_exit_code is None:
            self.app_server_exit_code = app_server_exit_code
        if self.stderr_tail is None:
            self.stderr_tail = stderr_tail
        return self

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
        private_paths: tuple[str, ...] = (),
    ) -> None:
        self.request_timeout = request_timeout_seconds
        self._next_id = 0
        self._line_buffer = b""
        self._notifications: Deque[Dict[str, Any]] = deque()
        self._stderr_file = tempfile.TemporaryFile()
        self.final_exit_code: int | None = None
        self.final_stderr_tail: str | None = None
        self._closed = False
        self._redacted_paths = tuple(
            str(Path(path).expanduser())
            for path in (
                (env or {}).get("CODEX_HOME"), str(Path.home()), *private_paths
            )
            if path
        )
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
            self._stderr_file.seek(0, os.SEEK_END)
            size = self._stderr_file.tell()
            self._stderr_file.seek(max(0, size - 8192), os.SEEK_SET)
            raw = self._stderr_file.read(8192).decode("utf-8", errors="replace")
            return redact_stderr_tail(raw, self._redacted_paths)
        except OSError:
            return ""

    def failure_diagnostics(self) -> tuple[int | None, str | None]:
        """Capture evidence before close can terminate a still-live process."""
        if self.process.poll() is None:
            try:
                self.process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
        return self.process.poll(), self._stderr_tail() or None

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
        if self._closed:
            return
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
            self.final_exit_code = self.process.poll()
            self.final_stderr_tail = self._stderr_tail() or None
            if self.process.stdout is not None:
                try:
                    self.process.stdout.close()
                except OSError:
                    pass
            self._stderr_file.close()
            self._closed = True


_SECRET_PATTERNS = (
    re.compile(r"(?i)([\"']?authorization[\"']?\s*[:=]\s*[\"']?(?:bearer\s+)?)[^\s,;\"']+"),
    re.compile(r"(?i)([\"']?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|password|secret)[\"']?\s*[:=]\s*[\"']?)[^\s,;\"']+"),
    re.compile(r"\b(?:sk|sess|proj)-[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{12,}(?:\.[A-Za-z0-9_-]{8,}){1,2}\b"),
)


def redact_stderr_tail(text: str, private_paths: tuple[str, ...] = ()) -> str:
    """Return a printable, bounded diagnostic tail with common secrets removed."""
    redacted = text
    for path in sorted(set(private_paths), key=len, reverse=True):
        if path:
            redacted = redacted.replace(path, "<redacted-path>")
    for pattern in _SECRET_PATTERNS:
        if pattern.groups:
            redacted = pattern.sub(lambda match: match.group(1) + "<redacted>", redacted)
        else:
            redacted = pattern.sub("<redacted>", redacted)
    redacted = "".join(
        ch if ch.isprintable() or ch in "\r\n\t" else "?" for ch in redacted
    )
    return redacted.strip()[-2048:]


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
    """Pin CODEX_HOME and preserve the service's explicit executable PATH."""
    return {
        "CODEX_HOME": str(Path(config["codex_home"]).expanduser()),
        "PATH": os.environ.get("PATH") or os.defpath,
    }


def resolve_executable(value: str, *, search_path: str | None = None) -> str:
    """Resolve and validate one executable without changing config schemas.

    Legacy v1 bridge configs may still contain a bare ``codex`` token.  New
    configs and installed services freeze the absolute executable path so a
    non-interactive service does not depend on its launch-time ``PATH``.
    """
    if not isinstance(value, str) or not value or "\x00" in value:
        raise se.SemanticEventError("transport_executable_invalid")
    if os.path.sep in value or (os.path.altsep and os.path.altsep in value):
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        candidate_text = os.path.abspath(str(candidate))
    else:
        found = shutil.which(value, path=search_path)
        if found is None:
            raise se.SemanticEventError("transport_executable_missing", value)
        candidate_text = os.path.abspath(found)
    try:
        target = Path(candidate_text).resolve(strict=True)
        info = target.stat()
    except (FileNotFoundError, OSError) as exc:
        raise se.SemanticEventError(
            "transport_executable_missing", candidate_text
        ) from exc
    if not stat.S_ISREG(info.st_mode) or not os.access(target, os.X_OK):
        raise se.SemanticEventError(
            "transport_executable_not_executable", candidate_text
        )
    return candidate_text


def resolved_delivery_command(
    config: Dict[str, Any], resolved_executable: str | None = None,
    configured_executable: str | None = None,
) -> list[str]:
    """Return an absolute transport argv, validating a service-time freeze."""
    command = list(config["transport"]["command"])
    configured = command[0]
    if resolved_executable is None:
        command[0] = resolve_executable(configured)
        return command
    frozen = resolve_executable(resolved_executable)
    if not os.path.isabs(frozen):  # Defensive; resolve_executable guarantees this.
        raise se.SemanticEventError("resolved_executable_not_absolute")
    installed_token = configured_executable if configured_executable is not None else configured
    if configured_executable is not None and configured != configured_executable:
        raise se.SemanticEventError("configured_executable_mismatch")
    if os.path.isabs(installed_token):
        if resolve_executable(installed_token) != frozen:
            raise se.SemanticEventError("resolved_executable_mismatch")
    elif Path(frozen).name != Path(installed_token).name:
        raise se.SemanticEventError("resolved_executable_mismatch")
    command[0] = frozen
    return command


def attempt_delivery(
    config: Dict[str, Any], event: Dict[str, Any],
    resolved_executable: str | None = None,
    configured_executable: str | None = None,
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
    stage = "spawn"
    try:
        session = AppServerSession(
            resolved_delivery_command(config, resolved_executable, configured_executable),
            float(config["request_timeout_seconds"]),
            env=spawn_env(config),
            private_paths=(str(config["workspace"]),),
        )
    except (OSError, se.SemanticEventError) as exc:
        raise DeliveryError("spawn_failed", str(exc), stage=stage) from exc

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
        stage = "initialize"
        session.initialize()
        renew()
        stage = "thread_resume"
        session.resume_thread(binding["thread_id"], binding["workspace"])
        renew()
        stage = "turn_start"
        turn_id = session.start_turn(binding["thread_id"], wake_text)
        renew()
        stage = "turn_completion"
        turn_status = session.wait_turn_completion(
            turn_id,
            float(config["turn_completion_timeout_seconds"]),
            on_tick=renew,
            tick_seconds=max(0.01, float(config["lease_seconds"]) / 3.0),
        )
        renew()
        return turn_id, turn_status, wake_text
    except DeliveryError as exc:
        exit_code, stderr_tail = session.failure_diagnostics()
        session.close()
        raise exc.attach_diagnostics(
            stage=stage,
            app_server_exit_code=exit_code,
            stderr_tail=stderr_tail,
        )
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
    try:
        resolved_delivery_command(
            config,
            getattr(args, "resolved_executable", None),
            getattr(args, "configured_executable", None),
        )
    except se.SemanticEventError as exc:
        print(json.dumps({
            "schema_version": f"{BRIDGE_PREFIX}.deliver/v1",
            "state": "error", "reason": exc.reason, "detail": exc.detail,
        }, sort_keys=True))
        return 12
    outbox = se.outbox_root(Path(args.state_dir))
    try:
        se.read_bridge_activation(outbox, config)
    except se.SemanticEventError as exc:
        print(json.dumps({
            "schema_version": f"{BRIDGE_PREFIX}.deliver/v1",
            "state": "refused", "reason": exc.reason, "detail": exc.detail,
        }, sort_keys=True))
        return 4
    lifecycle = configured_lifecycle_compatibility(config, Path(args.state_dir))
    if not lifecycle["compatible"]:
        print(json.dumps({
            "schema_version": f"{BRIDGE_PREFIX}.deliver/v1",
            "state": "refused",
            "reason": lifecycle["reason"],
            "codex_version": lifecycle.get("codex_version"),
        }, sort_keys=True))
        return 4
    owner = owner_identity(config)

    def binding_matches(event: Dict[str, Any]) -> bool:
        return (
            event["binding"]["app_server_instance"] == config["instance_id"]
            and event["binding"]["codex_home_id"] == config["codex_home_id"]
            and event["binding"]["workspace"] == config["workspace"]
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
            # The receipt is also a revocation boundary: removing it stops the
            # daemon before the next claim, including after service restarts.
            se.read_bridge_activation(outbox, config)
            claimed = se.claim_next_event(
                outbox,
                owner=owner,
                lease_seconds=float(config["lease_seconds"]),
                now=utc_now(),
                binding_filter=binding_matches,
                pre_claim=lambda: se.read_bridge_activation(outbox, config),
            )
        except se.SemanticEventError as exc:
            print(
                json.dumps(
                    {"schema_version": f"{BRIDGE_PREFIX}.deliver/v1",
                     "state": "error", "reason": exc.reason},
                    sort_keys=True,
                )
            )
            return 4
        if claimed is None:
            if args.once:
                break
            time.sleep(float(config["poll_seconds"]))
            continue
        event, _delivery = claimed
        held_event = event
        # Re-check after claim so a daemon that outlives an executable/config
        # replacement cannot use a historical smoke receipt for a new wake.
        lifecycle = configured_lifecycle_compatibility(config, Path(args.state_dir))
        if not lifecycle["compatible"]:
            try:
                se.release_event(outbox, event["event_id"], owner=owner)
            finally:
                held_event = {}
            print(json.dumps({
                "schema_version": f"{BRIDGE_PREFIX}.deliver/v1",
                "state": "refused",
                "reason": lifecycle["reason"],
                "codex_version": lifecycle.get("codex_version"),
                "event_id": event["event_id"],
            }, sort_keys=True))
            return 4
        _DELIVERY_STATE_DIR[0] = str(Path(args.state_dir))
        _DELIVERY_OWNER[0] = owner
        record: Dict[str, Any] = {
            "schema_version": f"{BRIDGE_PREFIX}.delivery/v1",
            "event_id": event["event_id"],
            "event": event["event"],
        }
        try:
            turn_id, turn_status, _wake = attempt_delivery(
                config,
                event,
                getattr(args, "resolved_executable", None),
                getattr(args, "configured_executable", None),
            )
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
                    safe_message=redact_stderr_tail(
                        exc.message,
                        (str(config["codex_home"]), str(config["workspace"])),
                    ),
                    stage=exc.stage,
                    app_server_exit_code=exc.app_server_exit_code,
                    stderr_tail=exc.stderr_tail,
                    retryable=exc.retryable,
                    now=utc_now(),
                    max_attempts=int(config["max_attempts"]),
                    backoff_initial_seconds=float(config["backoff_initial_seconds"]),
                    backoff_max_seconds=float(config["backoff_max_seconds"]),
                )
            record.update({
                "state": outcome,
                "error_code": exc.code,
                "failure_stage": exc.stage,
                "app_server_exit_code": exc.app_server_exit_code,
            })
        except (OSError, se.SemanticEventError) as exc:
            outcome = se.record_delivery_failure(
                outbox,
                event["event_id"],
                owner=owner,
                code="server_error",
                safe_message=redact_stderr_tail(
                    str(exc),
                    (str(config["codex_home"]), str(config["workspace"])),
                ),
                stage="delivery_internal",
                app_server_exit_code=None,
                stderr_tail=None,
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
        "capabilities": {
            "schema": "not_probed_run_protocol_check",
            "fake_lifecycle": "covered_by_test_suite_not_runtime_evidence",
            "real_transport_smoke": "unverified",
            "real_monitor_closed_loop": "not_proven_by_bridge_status",
            "goal_control": "not_probed",
        },
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
        lifecycle = configured_lifecycle_compatibility(
            config, Path(args.state_dir)
        )
        payload["capabilities"]["real_transport_smoke"] = (
            "passed" if lifecycle["compatible"] else "unverified"
        )
        payload["capabilities"]["real_transport_reason"] = lifecycle["reason"]
        payload["capabilities"]["codex_version"] = lifecycle.get("codex_version")
        if lifecycle.get("receipt_completed_at") is not None:
            payload["capabilities"]["receipt_completed_at"] = lifecycle[
                "receipt_completed_at"
            ]
        try:
            activation = se.read_bridge_activation(
                se.outbox_root(Path(args.state_dir)), config
            )
            payload["activation"] = {
                "state": "activated",
                "activated_at": activation["activated_at"],
            }
        except se.SemanticEventError as exc:
            payload["activation"] = {"state": "inactive", "reason": exc.reason}
    except se.SemanticEventError as exc:
        payload["config"] = {"healthy": False, "reason": exc.reason}
        payload["mode"] = "unattended"
    entries = se.list_outbox(se.outbox_root(Path(args.state_dir)))
    payload["outbox"] = entries
    payload["pending"] = sum(1 for e in entries if e.get("state") == "pending")
    print(json.dumps(payload, sort_keys=True))
    return 0


def activation_check_command(args: argparse.Namespace) -> int:
    """Audit, and optionally durably authorize, first bridge activation."""
    config = se.load_bridge_config(Path(args.bridge_config))
    outbox = se.outbox_root(Path(args.state_dir))
    audit = se.audit_bridge_activation(
        outbox, config
    )
    existing_activation = None
    try:
        existing_activation = se.read_bridge_activation(outbox, config)
    except se.SemanticEventError as exc:
        if exc.reason != "bridge_not_activated":
            raise
    payload = {
        "schema_version": f"{BRIDGE_PREFIX}.activation-check/v1",
        "state": "safe_to_start" if audit["safe_to_start"] else "review_required",
        "state_dir": str(Path(args.state_dir)),
        "instance_id": config["instance_id"],
        "codex_home_id": config["codex_home_id"],
        "config_enabled": config["enabled"],
        "audit": audit,
        "next_action": (
            "start the bridge for future/newly published events"
            if audit["safe_to_start"]
            else "inspect every unreadable and wakeable event before activation"
        ),
    }
    if not config["enabled"]:
        payload["state"] = "bridge_disabled"
        payload["next_action"] = "enable and revalidate the bridge configuration"
    elif args.deactivate:
        if args.accept_event_id:
            raise se.SemanticEventError("deactivation_event_ids_not_allowed")
        if not args.i_mean_it:
            payload["state"] = "confirmation_required"
            payload["next_action"] = "stop the daemon, then repeat with --deactivate --i-mean-it"
        else:
            result = se.deactivate_bridge(outbox, config)
            payload["state"] = result["state"]
            payload["next_action"] = "keep delivery stopped or disable the bridge config"
    elif args.activate:
        if not args.i_mean_it:
            payload["state"] = "confirmation_required"
            payload["next_action"] = "repeat with --activate --i-mean-it"
        else:
            result = se.activate_bridge(outbox, config, args.accept_event_id)
            payload["state"] = result["state"]
            payload["activation"] = result["record"]
            payload["next_action"] = "foreground or managed delivery may now start"
    elif existing_activation is not None:
        payload["state"] = "already_activated"
        payload["activation"] = existing_activation
        payload["next_action"] = "foreground or managed delivery may start"
    print(json.dumps(payload, sort_keys=True))
    if not config["enabled"]:
        return 3
    if args.activate or args.deactivate:
        return 0 if args.i_mean_it else 4
    if existing_activation is not None:
        return 0
    return 0 if audit["safe_to_start"] else 4


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
    try:
        codex_bin = resolve_executable(str(args.codex_bin))
    except se.SemanticEventError as exc:
        print(json.dumps({
            "schema_version": f"{BRIDGE_PREFIX}.protocol-check/v1",
            "codex_bin": str(args.codex_bin),
            "required_methods": sorted(REQUIRED_PROTOCOL_METHODS),
            "schema_compatible": False,
            "reported_version_matches_recorded_smoke": False,
            "compatibility": "probe_failed",
            "reason": exc.reason,
            "detail": exc.detail,
        }, sort_keys=True))
        return 12
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
        match = CODEX_VERSION_RE.search(version_text)
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


def _executable_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _configured_cli_identity(
    config: Dict[str, Any], *, timeout_seconds: float
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "probe_ok": False,
        "codex_version": None,
        "reason": "codex_version_probe_failed",
    }
    try:
        configured_command = config["transport"]["command"]
        if not os.path.isabs(str(configured_command[0])):
            result["reason"] = "transport_executable_not_frozen"
            return result
        command = resolved_delivery_command(config)
        if len(command) < 2 or command[1] != "app-server":
            result["reason"] = "transport_not_direct_codex_app_server"
            return result
        probe = subprocess.run(
            [command[0], "--version"],
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
            env={**os.environ, **spawn_env(config)},
        )
        if probe.returncode != 0:
            result["reason"] = "codex_version_probe_failed"
            return result
        match = CODEX_VERSION_RE.search(probe.stdout.strip())
        if match is None:
            result["reason"] = "codex_version_unparseable"
            return result
        version = match.group(1)
        result["codex_version"] = version
        result.update({
            "probe_ok": True,
            "reason": "probe_ok",
            "command": command,
            "executable": command[0],
            "executable_sha256": _executable_sha256(command[0]),
        })
        return result
    except (OSError, subprocess.TimeoutExpired, se.SemanticEventError):
        result["reason"] = "codex_version_probe_failed"
        return result


def _lifecycle_smoke_config_digest(config: Dict[str, Any]) -> str:
    checked = se.validate_bridge_config(config)
    return se.sha256_prefix(se.canonical_json(checked))


def lifecycle_smoke_path(state_dir: Path, config: Dict[str, Any]) -> Path:
    identity = {
        "config_digest": _lifecycle_smoke_config_digest(config),
        "instance_id": config["instance_id"],
        "codex_home_id": config["codex_home_id"],
        "workspace": config["workspace"],
    }
    name = se.sha256_hex(se.canonical_json(identity)) + ".json"
    return se.outbox_root(Path(state_dir)) / ".lifecycle-smokes" / name


def record_lifecycle_smoke_receipt(
    state_dir: Path,
    config: Dict[str, Any],
    cli_identity: Dict[str, Any],
    *,
    thread_id: str,
    first_turn_id: str,
    second_turn_id: str,
    completed_at: str | None = None,
) -> Dict[str, Any]:
    """Atomically record one completed two-connection lifecycle smoke."""
    if not cli_identity.get("probe_ok"):
        raise se.SemanticEventError("lifecycle_smoke_cli_probe_invalid")
    for value in (thread_id, first_turn_id, second_turn_id):
        if not isinstance(value, str) or not value:
            raise se.SemanticEventError("lifecycle_smoke_turn_identity_invalid")
    record = {
        "schema": LIFECYCLE_SMOKE_SCHEMA,
        "config_digest": _lifecycle_smoke_config_digest(config),
        "instance_id": config["instance_id"],
        "codex_home_id": config["codex_home_id"],
        "workspace_id": se.sha256_prefix(config["workspace"].encode()),
        "command": list(cli_identity["command"]),
        "executable": cli_identity["executable"],
        "executable_sha256": cli_identity["executable_sha256"],
        "codex_version": cli_identity["codex_version"],
        "thread_id": thread_id,
        "turn_ids": [first_turn_id, second_turn_id],
        "stages": [
            "initialize", "thread_start", "first_turn_completed",
            "reinitialize", "thread_resume", "second_turn_completed",
        ],
        "completed_at": completed_at or se.utc_now(),
    }
    se.atomic_replace_json(lifecycle_smoke_path(state_dir, config), record)
    return record


def read_lifecycle_smoke_receipt(
    state_dir: Path, config: Dict[str, Any], cli_identity: Dict[str, Any]
) -> Dict[str, Any]:
    path = lifecycle_smoke_path(state_dir, config)
    try:
        value = se.read_regular_json(path, MAX_LIFECYCLE_SMOKE_BYTES)
    except FileNotFoundError as exc:
        raise se.SemanticEventError("lifecycle_smoke_receipt_missing", str(path)) from exc
    expected = {
        "schema", "config_digest", "instance_id", "codex_home_id",
        "workspace_id", "command", "executable", "executable_sha256",
        "codex_version", "thread_id", "turn_ids", "stages", "completed_at",
    }
    if set(value) != expected or value["schema"] != LIFECYCLE_SMOKE_SCHEMA:
        raise se.SemanticEventError("lifecycle_smoke_receipt_invalid")
    checks = {
        "config_digest": _lifecycle_smoke_config_digest(config),
        "instance_id": config["instance_id"],
        "codex_home_id": config["codex_home_id"],
        "workspace_id": se.sha256_prefix(config["workspace"].encode()),
        "command": list(cli_identity["command"]),
        "executable": cli_identity["executable"],
        "executable_sha256": cli_identity["executable_sha256"],
        "codex_version": cli_identity["codex_version"],
    }
    for key, expected_value in checks.items():
        if value[key] != expected_value:
            raise se.SemanticEventError(f"lifecycle_smoke_{key}_mismatch")
    if (
        not isinstance(value["thread_id"], str) or not value["thread_id"]
        or not isinstance(value["turn_ids"], list) or len(value["turn_ids"]) != 2
        or any(not isinstance(item, str) or not item for item in value["turn_ids"])
        or value["stages"] != [
            "initialize", "thread_start", "first_turn_completed",
            "reinitialize", "thread_resume", "second_turn_completed",
        ]
        or se.parse_utc(value["completed_at"]) is None
    ):
        raise se.SemanticEventError("lifecycle_smoke_receipt_invalid")
    return value


def configured_lifecycle_compatibility(
    config: Dict[str, Any], state_dir: Path, *, timeout_seconds: float = 5.0
) -> Dict[str, Any]:
    """Require reported version plus a config/binary-bound live-smoke receipt."""
    result: Dict[str, Any] = {
        "compatible": False,
        "codex_version": None,
        "reason": "unverified_codex_lifecycle",
        "real_smoke_tested_versions": sorted(REAL_SMOKE_TESTED_CODEX_VERSIONS),
    }
    identity = _configured_cli_identity(config, timeout_seconds=timeout_seconds)
    result["codex_version"] = identity.get("codex_version")
    if not identity.get("probe_ok"):
        result["reason"] = identity["reason"]
        return result
    if identity["codex_version"] not in REAL_SMOKE_TESTED_CODEX_VERSIONS:
        result["reason"] = "codex_lifecycle_version_unverified"
        return result
    try:
        receipt = read_lifecycle_smoke_receipt(state_dir, config, identity)
    except se.SemanticEventError as exc:
        result["reason"] = exc.reason
        return result
    result.update({
        "compatible": True,
        "reason": "recorded_real_lifecycle_smoke",
        "receipt_completed_at": receipt["completed_at"],
        "executable_sha256": identity["executable_sha256"],
    })
    return result


def _private_output_path(value: Path) -> Path:
    """Create missing parent components as 0700; reject symlink traversal."""
    path = Path(os.path.abspath(str(Path(value).expanduser())))
    parent = path.parent
    current = Path(parent.anchor)
    for part in parent.parts[1:]:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            try:
                current.mkdir(mode=0o700)
            except FileExistsError:
                pass
            info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise se.SemanticEventError("output_parent_unsafe", str(current))
    return path


def _write_private_json_output(path: Path, payload: Dict[str, Any]) -> None:
    if path.exists() or path.is_symlink():
        raise se.SemanticEventError("output_exists", str(path))
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview((json.dumps(payload, indent=2, sort_keys=True) + "\n").encode())
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    se.fsync_directory(path.parent)


def init_config_command(args: argparse.Namespace) -> int:
    codex_home = str(Path(args.codex_home).expanduser().resolve(strict=False))
    command = list(args.command)
    command[0] = resolve_executable(command[0])
    config = {
        "schema": se.BRIDGE_CONFIG_SCHEMA,
        "enabled": args.enabled,
        "instance_id": args.instance_id,
        "codex_home": codex_home,
        "codex_home_id": se.codex_home_digest(Path(codex_home)),
        "workspace": str(Path(args.workspace).resolve(strict=False)),
        "transport": {"type": "stdio", "command": command},
        "request_timeout_seconds": args.request_timeout_seconds,
        "poll_seconds": args.poll_seconds,
        "lease_seconds": args.lease_seconds,
        "max_attempts": args.max_attempts,
        "backoff_initial_seconds": args.backoff_initial_seconds,
        "backoff_max_seconds": args.backoff_max_seconds,
        "turn_completion_timeout_seconds": args.turn_completion_timeout_seconds,
    }
    se.validate_bridge_config(config)
    path = _private_output_path(args.output)
    _write_private_json_output(path, config)
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
    path = _private_output_path(args.output)
    _write_private_json_output(path, binding)
    print(json.dumps({"state": "written", "path": str(path)}, sort_keys=True))
    return 0


def lifecycle_smoke_command(args: argparse.Namespace) -> int:
    """Run an explicit two-connection real App Server lifecycle smoke."""
    if not args.i_mean_it:
        print(json.dumps({
            "schema_version": f"{BRIDGE_PREFIX}.lifecycle-smoke/v1",
            "state": "refused",
            "reason": "confirmation_required",
            "detail": "this creates a test thread and two small model turns",
        }, sort_keys=True))
        return 4
    config: Dict[str, Any] | None = None
    try:
        config = se.load_bridge_config(Path(args.bridge_config))
        if not config["enabled"]:
            raise se.SemanticEventError("bridge_disabled")
        identity = _configured_cli_identity(
            config, timeout_seconds=float(args.timeout_seconds)
        )
        if not identity.get("probe_ok"):
            raise DeliveryError(str(identity["reason"]), "Codex CLI probe failed")
        if identity["codex_version"] not in REAL_SMOKE_TESTED_CODEX_VERSIONS:
            raise DeliveryError(
                "codex_lifecycle_version_unverified",
                "this repository version has not recorded the reported CLI version",
            )
        command = list(identity["command"])
        session: AppServerSession | None = None
        stage = "spawn"
        try:
            session = AppServerSession(
                command,
                float(config["request_timeout_seconds"]),
                env=spawn_env(config),
                private_paths=(str(config["workspace"]),),
            )
            stage = "initialize"
            session.initialize()
            stage = "thread_start"
            started = session.request(
                "thread/start",
                {
                    "cwd": config["workspace"],
                    "approvalPolicy": "never",
                    "sandbox": "read-only",
                },
            )
            thread = started.get("thread")
            if (
                not isinstance(thread, dict)
                or not isinstance(thread.get("id"), str)
                or not thread.get("id")
                or thread.get("cwd") != config["workspace"]
            ):
                raise DeliveryError(
                    "unsupported_response_shape",
                    "thread/start returned a malformed or wrong-workspace thread",
                )
            thread_id = str(thread["id"])
            stage = "first_turn"
            first_turn_id = session.start_turn(
                thread_id,
                "Codex monitor lifecycle smoke 1/2. Reply exactly SMOKE_ONE_OK. Do not use tools or modify files.",
            )
            session.wait_turn_completion(
                first_turn_id, float(config["turn_completion_timeout_seconds"])
            )
            session.close()
            session = None

            stage = "respawn"
            session = AppServerSession(
                command,
                float(config["request_timeout_seconds"]),
                env=spawn_env(config),
                private_paths=(str(config["workspace"]),),
            )
            stage = "reinitialize"
            session.initialize()
            stage = "thread_resume"
            session.resume_thread(thread_id, config["workspace"])
            stage = "second_turn"
            second_turn_id = session.start_turn(
                thread_id,
                "Codex monitor lifecycle smoke 2/2 after reconnect and resume. Reply exactly SMOKE_TWO_OK. Do not use tools or modify files.",
            )
            session.wait_turn_completion(
                second_turn_id, float(config["turn_completion_timeout_seconds"])
            )
        except (OSError, se.SemanticEventError) as exc:
            raise DeliveryError("lifecycle_smoke_failed", str(exc), stage=stage) from exc
        except DeliveryError as exc:
            if session is not None:
                exit_code, stderr_tail = session.failure_diagnostics()
                exc.attach_diagnostics(
                    stage=stage,
                    app_server_exit_code=exit_code,
                    stderr_tail=stderr_tail,
                )
            raise
        finally:
            if session is not None:
                session.close()
        receipt = record_lifecycle_smoke_receipt(
            Path(args.state_dir), config, identity,
            thread_id=thread_id,
            first_turn_id=first_turn_id,
            second_turn_id=second_turn_id,
        )
        print(json.dumps({
            "schema_version": f"{BRIDGE_PREFIX}.lifecycle-smoke/v1",
            "state": "passed",
            "codex_version": identity["codex_version"],
            "executable_sha256": identity["executable_sha256"],
            "receipt": str(lifecycle_smoke_path(Path(args.state_dir), config)),
            "completed_at": receipt["completed_at"],
        }, sort_keys=True))
        return 0
    except (DeliveryError, se.SemanticEventError, OSError) as exc:
        message = exc.message if isinstance(exc, DeliveryError) else str(exc)
        private_paths = (
            (str(config["codex_home"]), str(config["workspace"]))
            if config is not None
            else ()
        )
        print(json.dumps({
            "schema_version": f"{BRIDGE_PREFIX}.lifecycle-smoke/v1",
            "state": "failed",
            "reason": exc.code if isinstance(exc, DeliveryError)
            else getattr(exc, "reason", type(exc).__name__),
            "failure_stage": exc.stage if isinstance(exc, DeliveryError) else None,
            "app_server_exit_code": (
                exc.app_server_exit_code if isinstance(exc, DeliveryError) else None
            ),
            "safe_message": redact_stderr_tail(message, private_paths),
            "stderr_tail": exc.stderr_tail if isinstance(exc, DeliveryError) else None,
        }, sort_keys=True))
        return 12


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
    deliver.add_argument(
        "--resolved-executable",
        help=argparse.SUPPRESS,
    )
    deliver.add_argument(
        "--configured-executable",
        help=argparse.SUPPRESS,
    )
    deliver.set_defaults(func=deliver_loop)

    status = sub.add_parser("status", help="summarize outbox and configuration")
    status.add_argument("--state-dir", type=Path, required=True)
    status.add_argument("--bridge-config", type=Path, required=True)
    status.set_defaults(func=status_command)

    activation_check = sub.add_parser(
        "activation-check",
        help="audit existing matching outbox events before enabling delivery",
    )
    activation_check.add_argument("--state-dir", type=Path, required=True)
    activation_check.add_argument("--bridge-config", type=Path, required=True)
    activation_mode = activation_check.add_mutually_exclusive_group()
    activation_mode.add_argument(
        "--activate", action="store_true",
        help="write the durable activation receipt after a fresh exact audit",
    )
    activation_mode.add_argument(
        "--deactivate", action="store_true",
        help="remove the activation receipt under the claim lock",
    )
    activation_check.add_argument(
        "--i-mean-it", action="store_true",
        help="confirm the state-changing --activate operation",
    )
    activation_check.add_argument(
        "--accept-event-id", action="append", default=[],
        help="exact pre-cutover wakeable event id accepted; repeat per id",
    )
    activation_check.set_defaults(func=activation_check_command)

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

    lifecycle_smoke = sub.add_parser(
        "lifecycle-smoke",
        help="run a confirmed real two-connection App Server lifecycle smoke",
    )
    lifecycle_smoke.add_argument("--state-dir", type=Path, required=True)
    lifecycle_smoke.add_argument("--bridge-config", type=Path, required=True)
    lifecycle_smoke.add_argument("--timeout-seconds", type=positive_float, default=10.0)
    lifecycle_smoke.add_argument("--i-mean-it", action="store_true")
    lifecycle_smoke.set_defaults(func=lifecycle_smoke_command)

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
                 "reason": exc.reason if isinstance(exc, se.SemanticEventError)
                 else type(exc).__name__,
                 "detail": exc.detail if isinstance(exc, se.SemanticEventError)
                 else str(exc)},
                sort_keys=True,
            )
        )
        return 12


if __name__ == "__main__":
    raise SystemExit(main())

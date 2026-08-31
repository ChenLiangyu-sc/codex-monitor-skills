#!/usr/bin/env python3
"""Integration tests for the vendored App Server delivery adapter.

Uses a deterministic fake JSONL JSON-RPC server; no real Codex App Server,
credentials, or network access is required. The fake server mirrors the
official lifecycle: initialize -> initialized, thread/resume returns the
thread id and cwd, turn/start returns a turn id, and the turn later reaches
turn/completed (or turn/failed) as a notification."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path


HERE = Path(__file__).resolve().parent
BRIDGE = HERE / "app_server_bridge.py"
import semantic_events as se
import app_server_bridge as bridge


FAKE_SERVER = r'''#!/usr/bin/env python3
import json, os, sys, time

MODE = os.environ.get("FAKE_MODE", "ok")
LOG = os.environ.get("FAKE_LOG")
CWD = os.environ.get("FAKE_THREAD_CWD", "/default/workspace")
TURN_DELAY = float(os.environ.get("FAKE_TURN_DELAY", "0"))

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def log(text):
    if LOG:
        with open(LOG, "a", encoding="utf-8") as handle:
            handle.write(text + "\n===\n")

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        continue
    method = message.get("method")
    mid = message.get("id")
    if MODE == "garbage":
        sys.stdout.write("this is not json\n")
        sys.stdout.flush()
        continue
    if method == "initialize":
        if MODE == "approval_id_collision":
            send({"id": mid, "method": "permissions/requestApproval", "params": {"reason": "secret"}})
            continue
        if MODE == "approval_during_initialize":
            send({"id": 990, "method": "permissions/requestApproval", "params": {"reason": "secret"}})
            continue
        if MODE == "overload_init":
            send({"id": mid, "error": {"code": -32001, "message": "overloaded"}})
            continue
        if MODE == "drop_before_init":
            sys.exit(0)
        send({"id": mid, "result": {"userInfo": {"id": "u"}, "supportedCommands": []}})
    elif method == "initialized":
        if MODE == "drop_after_init":
            sys.exit(1)
    elif method == "thread/resume":
        if MODE == "thread_missing":
            send({"id": mid, "error": {"code": -32602, "message": "thread thr_x not found"}})
        elif MODE == "thread_archived":
            send({"id": mid, "error": {"code": -32602, "message": "thread is archived"}})
        elif MODE == "mcp_required":
            send({"id": mid, "error": {"code": -32000, "message": "required MCP server failed to initialize"}})
        elif MODE == "overload":
            send({"id": mid, "error": {"code": -32001, "message": "overloaded"}})
        elif MODE == "resume_bad_shape":
            send({"id": mid, "result": {"thread": {"id": "thr_other"}}})
        elif MODE == "wrong_cwd":
            send({"id": mid, "result": {"thread": {"id": message["params"]["threadId"], "cwd": "/somewhere/else"}}})
        elif MODE == "no_cwd":
            send({"id": mid, "result": {"thread": {"id": message["params"]["threadId"]}}})
        else:
            send({"id": mid, "result": {"thread": {"id": message["params"]["threadId"], "cwd": CWD}}})
    elif method == "turn/start":
        if MODE == "turn_conflict":
            send({"id": mid, "error": {"code": -32000, "message": "thread already has an active turn"}})
        elif MODE == "turn_bad_shape":
            send({"id": mid, "result": {"turn": {"status": "inProgress"}}})
        elif MODE == "hang":
            time.sleep(30)
        elif MODE == "drop_before_turn_reply":
            sys.exit(0)
        else:
            log(message["params"]["input"][0]["text"])
            if MODE == "approval_before_turn_reply":
                send({"id": 991, "method": "item/commandExecution/requestApproval", "params": {"command": "sensitive"}})
            if MODE == "completion_before_reply":
                send({"method": "turn/completed", "params": {"turn": {"id": "turn_fake_1", "status": "completed"}}})
            send({"id": mid, "result": {"turn": {"id": "turn_fake_1", "status": "inProgress", "items": [], "error": None}}})
            if MODE == "approval_during_turn":
                send({"id": 992, "method": "item/fileChange/requestApproval", "params": {"path": "/secret"}})
            if MODE != "no_completion":
                time.sleep(TURN_DELAY)
                if MODE == "turn_failed_notification":
                    send({"method": "turn/failed", "params": {"turn": {"id": "turn_fake_1", "status": "failed"}}})
                elif MODE == "completed_failed":
                    send({"method": "turn/completed", "params": {"turn": {"id": "turn_fake_1", "status": "failed"}}})
                elif MODE == "completed_interrupted":
                    send({"method": "turn/completed", "params": {"turn": {"id": "turn_fake_1", "status": "interrupted"}}})
                elif MODE == "completion_missing_id":
                    send({"method": "turn/completed", "params": {"turn": {"status": "completed"}}})
                elif MODE == "completion_missing_status":
                    send({"method": "turn/completed", "params": {"turn": {"id": "turn_fake_1"}}})
                elif MODE == "completion_before_reply":
                    pass
                else:
                    send({"method": "turn/completed", "params": {"turn": {"id": "turn_fake_1", "status": "completed"}}})
'''


class BridgeAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.fake = self.root / "fake_app_server.py"
        self.fake.write_text(FAKE_SERVER, encoding="utf-8")
        self.fake.chmod(0o700)
        self.wake_log = self.root / "wake.log"
        self.project = self.root / "project"

    def tearDown(self) -> None:
        self.temp.cleanup()

    # -- fixtures ---------------------------------------------------------

    def write_config(self, **overrides) -> Path:
        config = {
            "schema": se.BRIDGE_CONFIG_SCHEMA,
            "enabled": True,
            "instance_id": "workstation-1",
            "codex_home": str(self.root / ".codex"),
            "codex_home_id": se.codex_home_digest(self.root / ".codex"),
            "workspace": str(self.project),
            "transport": {"type": "stdio", "command": [sys.executable, str(self.fake)]},
            "request_timeout_seconds": 5,
            "poll_seconds": 0.05,
            "lease_seconds": 60,
            "max_attempts": 16,
            "backoff_initial_seconds": 0.05,
            "backoff_max_seconds": 0.2,
            "turn_completion_timeout_seconds": 10,
        }
        config.update(overrides)
        path = self.root / "bridge.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        path.chmod(0o600)
        return path

    def write_binding_file(self, **overrides) -> Path:
        binding = {
            "schema": se.EVENT_BINDING_SCHEMA,
            "codex_home_id": se.codex_home_digest(self.root / ".codex"),
            "app_server_instance": "workstation-1",
            "thread_id": "thr_test_1",
            "workspace": str(self.project),
        }
        binding.update(overrides)
        path = self.root / "binding.json"
        path.write_text(json.dumps(binding), encoding="utf-8")
        path.chmod(0o600)
        return path

    def publish_event(self, *, event: str = "transport_success", exit_code: int | None = 0,
                      binding: dict | None = None) -> dict:
        payload = se.build_event(
            backend="slurm",
            handle="fakehost-12345",
            generation="run_1_2_abcd1234",
            terminal_digest="sha256:" + "b" * 64,
            event=event,
            exit_code=exit_code,
            binding=binding or json.loads(self.write_binding_file().read_text()),
        )
        se.publish_event(se.outbox_root(self.state), payload)
        return payload

    def deliver_env(self, mode: str, turn_delay: float | None = None) -> dict:
        env = os.environ.copy()
        env.update({
            "FAKE_MODE": mode,
            "FAKE_LOG": str(self.wake_log),
            "FAKE_THREAD_CWD": str(self.project),
        })
        if turn_delay is not None:
            env["FAKE_TURN_DELAY"] = str(turn_delay)
        return env

    def activate_config(self, config: Path) -> None:
        loaded = se.load_bridge_config(config)
        audit = se.audit_bridge_activation(se.outbox_root(self.state), loaded)
        se.activate_bridge(
            se.outbox_root(self.state), loaded, audit["wakeable_event_ids"]
        )

    def deliver(self, config: Path, once: bool = True, mode: str = "ok",
                turn_delay: float | None = None, timeout: int = 30,
                activate: bool = True) -> tuple[int, list[dict]]:
        if activate:
            try:
                loaded = se.load_bridge_config(config)
                if loaded["enabled"]:
                    audit = se.audit_bridge_activation(se.outbox_root(self.state), loaded)
                    se.activate_bridge(
                        se.outbox_root(self.state), loaded,
                        audit["wakeable_event_ids"],
                    )
            except se.SemanticEventError:
                pass
        command = [
            sys.executable, str(BRIDGE), "deliver",
            "--state-dir", str(self.state), "--bridge-config", str(config),
        ]
        if once:
            command.append("--once")
        result = subprocess.run(
            command, text=True, capture_output=True, check=False,
            env=self.deliver_env(mode, turn_delay), timeout=timeout,
        )
        records = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        return result.returncode, records

    def delivery_of(self, event_id: str) -> dict:
        return se._read_delivery(
            se.event_dir(se.outbox_root(self.state), event_id), event_id
        )

    def protocol_files(self) -> dict[str, dict]:
        request = lambda method, ref: {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "method": {"type": "string", "enum": [method]},
                "params": {"$ref": f"#/definitions/{ref}"},
            },
            "required": ["id", "method", "params"],
        }
        turn_definition = {
            "type": "object",
            "properties": {"id": {"type": "string"}, "status": {"$ref": "#/definitions/TurnStatus"}},
            "required": ["id", "status"],
        }
        return {
            "ClientRequest.json": {"oneOf": [
                request("initialize", "InitializeParams"),
                request("thread/resume", "ThreadResumeParams"),
                request("turn/start", "TurnStartParams"),
            ]},
            "ClientNotification.json": {"oneOf": [{
                "type": "object", "properties": {"method": {"enum": ["initialized"]}},
                "required": ["method"],
            }]},
            "ServerNotification.json": {"oneOf": [{
                "type": "object",
                "properties": {
                    "method": {"enum": ["turn/completed"]},
                    "params": {"$ref": "#/definitions/TurnCompletedNotification"},
                },
                "required": ["method", "params"],
            }]},
            "ServerRequest.json": {},
            "InitializeResponse.json": {},
            "InitializeParams.json": {
                "properties": {"clientInfo": {"$ref": "#/definitions/ClientInfo"}},
                "required": ["clientInfo"],
                "definitions": {"ClientInfo": {
                    "properties": {"name": {"type": "string"}, "version": {"type": "string"}},
                    "required": ["name", "version"],
                }},
            },
            "ThreadResumeParams.json": {
                "properties": {"threadId": {"type": "string"}},
                "required": ["threadId"],
            },
            "TurnStartParams.json": {
                "properties": {
                    "threadId": {"type": "string"},
                    "input": {"type": "array", "items": {"$ref": "#/definitions/UserInput"}},
                },
                "required": ["threadId", "input"],
                "definitions": {"UserInput": {"oneOf": [{
                    "properties": {"type": {"enum": ["text"]}, "text": {"type": "string"}},
                    "required": ["type", "text"],
                }]}},
            },
            "ThreadResumeResponse.json": {
                "properties": {"thread": {"$ref": "#/definitions/Thread"}},
                "required": ["thread"],
                "definitions": {"Thread": {
                    "properties": {"id": {"type": "string"}, "cwd": {"type": "string"}},
                    "required": ["id", "cwd"],
                }},
            },
            "TurnStartResponse.json": {
                "properties": {"turn": {"$ref": "#/definitions/Turn"}},
                "required": ["turn"],
                "definitions": {"Turn": turn_definition},
            },
            "TurnCompletedNotification.json": {
                "properties": {"turn": {"$ref": "#/definitions/Turn"}},
                "required": ["turn"],
                "definitions": {
                    "Turn": turn_definition,
                    "TurnStatus": {"type": "string", "enum": ["completed", "failed", "interrupted", "inProgress"]},
                },
            },
            "codex_app_server_protocol.schemas.json": {"title": "bundle"},
        }

    def write_protocol_codex(self, files: dict[str, dict], version: str = "0.150.1") -> Path:
        fake_codex = self.root / f"fake_codex_{len(list(self.root.glob('fake_codex_*')))}.py"
        fake_codex.write_text(
            "#!/usr/bin/env python3\n"
            "import json, pathlib, sys\n"
            f"VERSION = {version!r}\n"
            f"FILES = {files!r}\n"
            "if sys.argv[1:] == ['--version']:\n"
            "    print('codex-cli ' + VERSION)\n"
            "    raise SystemExit(0)\n"
            "out = pathlib.Path(sys.argv[sys.argv.index('--out') + 1])\n"
            "out.mkdir(parents=True, exist_ok=True)\n"
            "for name, payload in FILES.items():\n"
            "    path = out / name\n"
            "    path.parent.mkdir(parents=True, exist_ok=True)\n"
            "    path.write_text(json.dumps(payload))\n",
            encoding="utf-8",
        )
        fake_codex.chmod(0o700)
        return fake_codex

    # -- lifecycle -------------------------------------------------------

    def test_ack_only_after_turn_completed(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        code, records = self.deliver(config)
        self.assertEqual(code, 0)
        outcome = records[0]
        self.assertEqual(outcome["state"], "acknowledged")
        self.assertEqual(outcome["turn_status"], "completed")
        delivery = self.delivery_of(event["event_id"])
        self.assertEqual(delivery["state"], "delivered")
        self.assertEqual(delivery["turn_status"], "completed")
        self.assertEqual(delivery["delivery"]["turn_id"], "turn_fake_1")
        wake = self.wake_log.read_text()
        self.assertIn(f"event_id={event['event_id']}", wake)
        self.assertIn("business_verdict=pending", wake)
        self.assertIn("Do not retry, cancel, resubmit, mutate, or approve", wake)
        self.assertEqual(wake.count("==="), 1)

    def test_slow_turn_is_awaited_before_ack(self) -> None:
        config = self.write_config(turn_completion_timeout_seconds=30)
        event = self.publish_event()
        code, records = self.deliver(config, turn_delay=1.5)
        self.assertEqual(code, 0)
        self.assertEqual(records[0]["state"], "acknowledged")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "delivered")

    def test_turn_completion_timeout_keeps_event_undelivered(self) -> None:
        config = self.write_config(turn_completion_timeout_seconds=1)
        event = self.publish_event()
        _, records = self.deliver(config, mode="no_completion")
        self.assertEqual(records[0]["error_code"], "turn_completion_timeout")
        self.assertEqual(records[0]["state"], "scheduled_retry")
        delivery = self.delivery_of(event["event_id"])
        self.assertEqual(delivery["state"], "pending")
        # The wake turn was started but never acknowledged as delivered.
        self.assertTrue(self.wake_log.exists())

    def test_turn_failed_notification_is_retryable(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="turn_failed_notification")
        self.assertEqual(records[0]["error_code"], "turn_failed")
        self.assertEqual(records[0]["state"], "scheduled_retry")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_official_failed_completion_is_retryable(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="completed_failed")
        self.assertEqual(records[0]["error_code"], "turn_failed")
        self.assertEqual(records[0]["state"], "scheduled_retry")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_official_interrupted_completion_is_retryable(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="completed_interrupted")
        self.assertEqual(records[0]["error_code"], "turn_aborted")
        self.assertEqual(records[0]["state"], "scheduled_retry")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_completion_without_turn_id_never_acknowledges(self) -> None:
        config = self.write_config(turn_completion_timeout_seconds=0.2)
        event = self.publish_event()
        _, records = self.deliver(config, mode="completion_missing_id")
        self.assertEqual(records[0]["error_code"], "turn_completion_timeout")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_completion_without_status_dead_letters(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="completion_missing_status")
        self.assertEqual(records[0]["error_code"], "unsupported_response_shape")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "dead_letter")

    def test_completion_arriving_before_turn_reply_is_preserved(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="completion_before_reply")
        self.assertEqual(records[0]["state"], "acknowledged")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "delivered")

    def test_wait_without_renew_callback_keeps_reading_after_tick(self) -> None:
        session = __import__("app_server_bridge").AppServerSession(
            [sys.executable, str(self.fake)], 2,
            env=self.deliver_env("ok", turn_delay=0.2),
        )
        try:
            session.initialize()
            session.resume_thread("thr_test_1", str(self.project))
            turn_id = session.start_turn("thr_test_1", "fixed wake")
            status = session.wait_turn_completion(
                turn_id, 2, tick_seconds=0.05
            )
            self.assertEqual(status, "completed")
        finally:
            session.close()

    def test_second_run_has_nothing_left_to_deliver(self) -> None:
        config = self.write_config()
        self.publish_event()
        self.deliver(config)
        code, records = self.deliver(config)
        self.assertEqual(code, 0)
        self.assertEqual(records[-1]["state"], "idle")
        self.assertEqual(records[-1]["delivered"], 0)
        self.assertEqual(self.wake_log.read_text().count("==="), 1)

    def test_lease_renewal_prevents_concurrent_second_delivery(self) -> None:
        # The turn takes 5s while the lease is only 3s: without periodic
        # renewal the lease would expire mid-delivery and a second daemon
        # could steal the event at t=4s. Renewal must prevent that.
        config = self.write_config(
            request_timeout_seconds=1,
            lease_seconds=3,
            turn_completion_timeout_seconds=30,
        )
        self.publish_event()
        self.activate_config(config)
        first = subprocess.Popen(
            [sys.executable, str(BRIDGE), "deliver",
             "--state-dir", str(self.state), "--bridge-config", str(config), "--once"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=self.deliver_env("ok", turn_delay=5.0),
        )
        time.sleep(4.0)  # past the un-renewed lease expiry, mid-turn wait
        code, records = self.deliver(config, mode="ok", turn_delay=5.0)
        self.assertEqual(code, 0)
        self.assertEqual(records[-1]["state"], "idle")
        stdout, _ = first.communicate(timeout=30)
        first_records = [json.loads(line) for line in stdout.splitlines() if line.strip()]
        self.assertEqual(first_records[0]["state"], "acknowledged", first_records)
        self.assertEqual(self.wake_log.read_text().count("==="), 1)

    def test_subsecond_lease_is_renewed_before_blocking_read(self) -> None:
        config = self.write_config(
            request_timeout_seconds=0.1,
            lease_seconds=0.3,
            turn_completion_timeout_seconds=5,
        )
        self.publish_event()
        self.activate_config(config)
        first = subprocess.Popen(
            [sys.executable, str(BRIDGE), "deliver",
             "--state-dir", str(self.state), "--bridge-config", str(config), "--once"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=self.deliver_env("ok", turn_delay=1.0),
        )
        time.sleep(0.5)
        code, records = self.deliver(config, mode="ok")
        self.assertEqual(code, 0)
        self.assertEqual(records[-1]["state"], "idle")
        stdout, _ = first.communicate(timeout=10)
        first_records = [json.loads(line) for line in stdout.splitlines() if line.strip()]
        self.assertEqual(first_records[0]["state"], "acknowledged", first_records)
        self.assertEqual(self.wake_log.read_text().count("==="), 1)

    # -- retryable failures -------------------------------------------------

    def test_overload_is_retryable_with_backoff(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        code, records = self.deliver(config, mode="overload")
        self.assertEqual(code, 0)
        self.assertEqual(records[0]["state"], "scheduled_retry")
        self.assertEqual(records[0]["error_code"], "overloaded")
        delivery = self.delivery_of(event["event_id"])
        self.assertEqual(delivery["state"], "pending")
        self.assertEqual(delivery["attempts"], 1)
        self.assertIsNotNone(delivery["next_attempt_at"])

    def test_turn_conflict_is_retryable(self) -> None:
        config = self.write_config()
        self.publish_event()
        _, records = self.deliver(config, mode="turn_conflict")
        self.assertEqual(records[0]["error_code"], "active_turn_conflict")
        self.assertEqual(records[0]["state"], "scheduled_retry")

    def test_connection_drop_is_retryable(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="drop_after_init")
        self.assertEqual(records[0]["error_code"], "connection_lost")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_drop_before_turn_reply_is_retryable(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="drop_before_turn_reply")
        self.assertEqual(records[0]["error_code"], "connection_lost")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_request_timeout_is_retryable(self) -> None:
        config = self.write_config(request_timeout_seconds=0.5, lease_seconds=2)
        event = self.publish_event()
        _, records = self.deliver(config, mode="hang")
        self.assertEqual(records[0]["error_code"], "request_timeout")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_mcp_required_failure_is_retryable(self) -> None:
        config = self.write_config()
        self.publish_event()
        _, records = self.deliver(config, mode="mcp_required")
        self.assertEqual(records[0]["error_code"], "required_mcp_failure")
        self.assertEqual(records[0]["state"], "scheduled_retry")

    def test_approval_before_turn_reply_fails_closed_without_auto_approval(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="approval_before_turn_reply")
        self.assertEqual(records[0]["error_code"], "operator_interaction_required")
        self.assertEqual(records[0]["state"], "dead_lettered")
        delivery = self.delivery_of(event["event_id"])
        self.assertEqual(delivery["state"], "dead_letter")
        self.assertNotIn("sensitive", delivery["last_error"]["safe_message"])

    def test_approval_during_initialize_keeps_operator_error_classification(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="approval_during_initialize")
        self.assertEqual(records[0]["error_code"], "operator_interaction_required")
        self.assertEqual(records[0]["state"], "dead_lettered")
        delivery = self.delivery_of(event["event_id"])
        self.assertNotIn("secret", delivery["last_error"]["safe_message"])

    def test_server_request_id_collision_is_not_misread_as_response(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="approval_id_collision")
        self.assertEqual(records[0]["error_code"], "operator_interaction_required")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "dead_letter")

    def test_approval_during_turn_fails_closed_without_waiting_for_timeout(self) -> None:
        config = self.write_config(turn_completion_timeout_seconds=30)
        event = self.publish_event()
        started = time.monotonic()
        _, records = self.deliver(config, mode="approval_during_turn")
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(records[0]["error_code"], "operator_interaction_required")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "dead_letter")

    def test_retryable_failure_exhausts_into_dead_letter(self) -> None:
        config = self.write_config(max_attempts=1)
        event = self.publish_event()
        _, records = self.deliver(config, mode="overload")
        self.assertEqual(records[0]["state"], "dead_lettered")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "dead_letter")

    # -- permanent failures ---------------------------------------------------

    def test_missing_thread_dead_letters_without_new_thread(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="thread_missing")
        self.assertEqual(records[0]["error_code"], "thread_missing")
        self.assertEqual(records[0]["state"], "dead_lettered")
        self.assertFalse(self.wake_log.exists())

    def test_archived_thread_dead_letters(self) -> None:
        config = self.write_config()
        self.publish_event()
        _, records = self.deliver(config, mode="thread_archived")
        self.assertEqual(records[0]["error_code"], "thread_archived")
        self.assertEqual(records[0]["state"], "dead_lettered")

    def test_resume_shape_mismatch_dead_letters(self) -> None:
        config = self.write_config()
        self.publish_event()
        _, records = self.deliver(config, mode="resume_bad_shape")
        self.assertEqual(records[0]["error_code"], "unsupported_response_shape")
        self.assertEqual(records[0]["state"], "dead_lettered")

    def test_wrong_thread_cwd_dead_letters(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="wrong_cwd")
        self.assertEqual(records[0]["error_code"], "binding_mismatch")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "dead_letter")
        self.assertFalse(self.wake_log.exists())

    def test_missing_thread_cwd_dead_letters(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        _, records = self.deliver(config, mode="no_cwd")
        self.assertEqual(records[0]["error_code"], "binding_mismatch")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "dead_letter")

    def test_turn_shape_without_id_dead_letters(self) -> None:
        config = self.write_config()
        self.publish_event()
        _, records = self.deliver(config, mode="turn_bad_shape")
        self.assertEqual(records[0]["error_code"], "unsupported_response_shape")
        self.assertEqual(records[0]["state"], "dead_lettered")

    def test_workspace_mismatch_is_foreign_and_never_claimed(self) -> None:
        config = self.write_config()
        event = self.publish_event(
            binding=json.loads(
                self.write_binding_file(workspace="/somewhere/else").read_text()
            )
        )
        _, records = self.deliver(config)
        self.assertEqual(records[-1]["state"], "idle")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")
        audit = se.audit_bridge_activation(
            se.outbox_root(self.state), se.load_bridge_config(config)
        )
        self.assertEqual(audit["foreign_count"], 1)
        self.assertEqual(audit["wakeable_event_ids"], [])

    def test_codex_home_digest_mismatch_is_rejected_at_load(self) -> None:
        config = self.write_config()
        payload = json.loads(config.read_text())
        payload["codex_home"] = str(self.root / "other-codex")
        config.write_text(json.dumps(payload))
        code, records = self.deliver(config)
        self.assertEqual(code, 12)
        self.assertEqual(records[0]["reason"], "config_codex_home_mismatch")

    # -- instance isolation ----------------------------------------------------

    def test_foreign_instance_event_is_never_claimed(self) -> None:
        config = self.write_config()
        self.publish_event(
            binding=json.loads(
                self.write_binding_file(app_server_instance="other-host").read_text()
            )
        )
        code, records = self.deliver(config)
        self.assertEqual(code, 0)
        self.assertEqual(records[-1]["state"], "idle")
        entries = se.list_outbox(se.outbox_root(self.state))
        self.assertEqual(entries[0]["state"], "pending")

    def test_disabled_config_is_refused(self) -> None:
        config = self.write_config(enabled=False)
        self.publish_event()
        code, records = self.deliver(config)
        self.assertEqual(code, 3)
        self.assertEqual(records[0]["state"], "refused")
        self.assertEqual(records[0]["reason"], "bridge_disabled")

    def test_service_mode_treats_disabled_config_as_clean_stop(self) -> None:
        config = self.write_config(enabled=False)
        self.publish_event()
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "deliver", "--state-dir", str(self.state),
             "--bridge-config", str(config), "--exit-zero-if-disabled"],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(result.stdout)["reason"], "bridge_disabled")
        self.assertEqual(
            se.list_outbox(se.outbox_root(self.state))[0]["state"], "pending"
        )

    def test_service_mode_missing_receipt_is_failure_not_clean_stop(self) -> None:
        config = self.write_config()
        self.publish_event()
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "deliver", "--state-dir", str(self.state),
             "--bridge-config", str(config), "--exit-zero-if-disabled", "--once"],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 4, result.stdout)
        self.assertEqual(json.loads(result.stdout)["reason"], "bridge_not_activated")

    def test_invalid_config_fails_closed(self) -> None:
        bad = self.root / "bad.json"
        bad.write_text(json.dumps({"schema": "nope"}), encoding="utf-8")
        bad.chmod(0o600)
        code, records = self.deliver(bad)
        self.assertEqual(code, 12)
        self.assertEqual(records[0]["state"], "error")

    def test_lease_shorter_than_request_budget_is_rejected(self) -> None:
        with self.assertRaises(se.SemanticEventError) as ctx:
            se.validate_bridge_config(
                json.loads(self.write_config(lease_seconds=1).read_text())
            )
        self.assertEqual(ctx.exception.reason, "config_lease_too_short")

    # -- tooling commands --------------------------------------------------------

    def test_init_commands_write_valid_files(self) -> None:
        private_parent = self.root / "missing" / "codex-monitor"
        config_path = private_parent / "cfg.json"
        binding_path = private_parent / "bnd.json"
        codex_home = self.root / ".codex"
        codex_home.mkdir()
        workspace = self.root / "project"
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "init-config", "--output", str(config_path),
             "--instance-id", "workstation-1", "--workspace", str(workspace),
             "--codex-home", str(codex_home), "--enabled",
             "--command", sys.executable, str(self.fake)],
            text=True, capture_output=True, check=False, timeout=10)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(private_parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(config_path.stat().st_mode & 0o777, 0o600)
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "init-binding", "--output", str(binding_path),
             "--thread-id", "thr_123", "--instance-id", "workstation-1",
             "--workspace", str(workspace), "--codex-home", str(codex_home)],
            text=True, capture_output=True, check=False, timeout=10)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(binding_path.stat().st_mode & 0o777, 0o600)
        config = se.load_bridge_config(config_path)
        binding = se.load_event_binding(binding_path)
        self.assertTrue(config["enabled"])
        self.assertEqual(binding["thread_id"], "thr_123")
        self.assertEqual(config["codex_home_id"], binding["codex_home_id"])
        self.assertEqual(binding["app_server_instance"], config["instance_id"])
        self.assertEqual(binding["workspace"], config["workspace"])
        self.assertTrue(Path(config["transport"]["command"][0]).is_absolute())

    def test_init_config_resolves_bare_executable_and_rejects_missing(self) -> None:
        executable = self.root / "codex"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o700)
        output = self.root / "resolved.json"
        env = os.environ.copy()
        env["PATH"] = str(self.root)
        accepted = subprocess.run(
            [sys.executable, str(BRIDGE), "init-config", "--output", str(output),
             "--instance-id", "workstation-1", "--workspace", str(self.project),
             "--codex-home", str(self.root / ".codex"),
             "--command", "codex", "app-server"],
            text=True, capture_output=True, check=False, timeout=10, env=env,
        )
        self.assertEqual(accepted.returncode, 0, accepted.stdout)
        self.assertEqual(
            json.loads(output.read_text())["transport"]["command"][0], str(executable)
        )
        missing_output = self.root / "missing.json"
        refused = subprocess.run(
            [sys.executable, str(BRIDGE), "init-config", "--output", str(missing_output),
             "--instance-id", "workstation-1", "--workspace", str(self.project),
             "--codex-home", str(self.root / ".codex"),
             "--command", "missing-codex", "app-server"],
            text=True, capture_output=True, check=False, timeout=10, env=env,
        )
        self.assertEqual(refused.returncode, 12, refused.stdout)
        self.assertEqual(json.loads(refused.stdout)["reason"], "transport_executable_missing")
        self.assertFalse(missing_output.exists())

    def test_init_config_rejects_non_executable_before_write(self) -> None:
        executable = self.root / "codex-not-executable"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o600)
        output = self.root / "nonexec.json"
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "init-config", "--output", str(output),
             "--instance-id", "workstation-1", "--workspace", str(self.project),
             "--codex-home", str(self.root / ".codex"),
             "--command", str(executable), "app-server"],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 12, result.stdout)
        self.assertEqual(
            json.loads(result.stdout)["reason"],
            "transport_executable_not_executable",
        )
        self.assertFalse(output.exists())

    def test_frozen_executable_supports_legacy_config_without_path_lookup(self) -> None:
        executable = self.root / "codex"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o700)
        config = json.loads(self.write_config().read_text())
        config["transport"]["command"][0] = "codex"
        with mock.patch.dict(os.environ, {"PATH": ""}):
            command = bridge.resolved_delivery_command(config, str(executable))
        self.assertEqual(command[0], str(executable))

    def test_frozen_executable_mismatch_fails_closed(self) -> None:
        executable = self.root / "other"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o700)
        config = json.loads(self.write_config().read_text())
        config["transport"]["command"][0] = "codex"
        with self.assertRaises(se.SemanticEventError) as ctx:
            bridge.resolved_delivery_command(config, str(executable))
        self.assertEqual(ctx.exception.reason, "resolved_executable_mismatch")

    def test_service_executable_mismatch_fails_before_event_claim(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        self.activate_config(config)
        executable = self.root / "other"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o700)
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "deliver", "--once",
             "--state-dir", str(self.state), "--bridge-config", str(config),
             "--resolved-executable", str(executable)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 12, result.stdout)
        self.assertEqual(json.loads(result.stdout)["reason"], "resolved_executable_mismatch")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_spawn_environment_preserves_explicit_service_path(self) -> None:
        config = json.loads(self.write_config().read_text())
        with mock.patch.dict(os.environ, {"PATH": "/service/bin:/usr/bin"}):
            environment = bridge.spawn_env(config)
        self.assertEqual(environment["PATH"], "/service/bin:/usr/bin")
        self.assertEqual(environment["CODEX_HOME"], config["codex_home"])
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(bridge.spawn_env(config)["PATH"], os.defpath)

    def test_init_command_rejects_symlinked_parent(self) -> None:
        real = self.root / "real"
        real.mkdir()
        link = self.root / "linked"
        link.symlink_to(real, target_is_directory=True)
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "init-config",
             "--output", str(link / "bridge.json"),
             "--instance-id", "workstation-1", "--workspace", str(self.project),
             "--codex-home", str(self.root / ".codex")],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 12)
        self.assertEqual(json.loads(result.stdout)["reason"], "output_parent_unsafe")

    def test_status_reports_mode_and_outbox(self) -> None:
        config = self.write_config()
        self.publish_event()
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "status", "--state-dir", str(self.state),
             "--bridge-config", str(config)],
            text=True, capture_output=True, check=False, timeout=10)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["mode"], "external-event-bridge")
        self.assertEqual(payload["pending"], 1)

    def test_activation_check_requires_review_of_matching_pending_events(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "activation-check",
             "--state-dir", str(self.state), "--bridge-config", str(config)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 4, result.stdout)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["state"], "review_required")
        self.assertEqual(payload["audit"]["wakeable_event_ids"], [event["event_id"]])

    def test_activation_check_is_safe_for_empty_outbox(self) -> None:
        config = self.write_config()
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "activation-check",
             "--state-dir", str(self.state), "--bridge-config", str(config)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(result.stdout)["state"], "safe_to_start")

    def test_activation_check_reports_disabled_config(self) -> None:
        config = self.write_config(enabled=False)
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "activation-check",
             "--state-dir", str(self.state), "--bridge-config", str(config)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 3, result.stdout)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["state"], "bridge_disabled")
        self.assertFalse(payload["config_enabled"])

    def test_foreground_deliver_requires_durable_activation(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        code, records = self.deliver(config, activate=False)
        self.assertEqual(code, 4)
        self.assertEqual(records[0]["reason"], "bridge_not_activated")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "pending")

    def test_activation_write_requires_confirmation_and_exact_ids(self) -> None:
        config = self.write_config()
        event = self.publish_event()
        base = [
            sys.executable, str(BRIDGE), "activation-check",
            "--state-dir", str(self.state), "--bridge-config", str(config),
            "--activate",
        ]
        refused = subprocess.run(
            base, text=True, capture_output=True, check=False, timeout=10
        )
        self.assertEqual(refused.returncode, 4)
        self.assertEqual(json.loads(refused.stdout)["state"], "confirmation_required")
        missing = subprocess.run(
            [*base, "--i-mean-it"], text=True, capture_output=True,
            check=False, timeout=10,
        )
        self.assertEqual(missing.returncode, 12)
        self.assertEqual(
            json.loads(missing.stdout)["reason"],
            "activation_events_require_exact_acknowledgement",
        )
        accepted = subprocess.run(
            [*base, "--i-mean-it", "--accept-event-id", event["event_id"]],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(accepted.returncode, 0, accepted.stdout)
        self.assertEqual(json.loads(accepted.stdout)["state"], "activated")

    def test_activation_check_reports_existing_receipt_not_future_event_review(self) -> None:
        config = self.write_config()
        self.activate_config(config)
        self.publish_event()
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "activation-check",
             "--state-dir", str(self.state), "--bridge-config", str(config)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["state"], "already_activated")
        self.assertEqual(len(payload["audit"]["wakeable_event_ids"]), 1)

    def test_explicit_deactivation_requires_confirmation(self) -> None:
        config = self.write_config()
        self.activate_config(config)
        base = [
            sys.executable, str(BRIDGE), "activation-check",
            "--state-dir", str(self.state), "--bridge-config", str(config),
            "--deactivate",
        ]
        refused = subprocess.run(
            base, text=True, capture_output=True, check=False, timeout=10
        )
        self.assertEqual(refused.returncode, 4)
        accepted = subprocess.run(
            [*base, "--i-mean-it"], text=True, capture_output=True,
            check=False, timeout=10,
        )
        self.assertEqual(accepted.returncode, 0, accepted.stdout)
        self.assertEqual(json.loads(accepted.stdout)["state"], "deactivated")
        code, records = self.deliver(config, activate=False)
        self.assertEqual(code, 4)
        self.assertEqual(records[0]["reason"], "bridge_not_activated")

    def test_status_reports_unattended_without_config(self) -> None:
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "status", "--state-dir", str(self.state),
             "--bridge-config", str(self.root / "absent.json")],
            text=True, capture_output=True, check=False, timeout=10)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["mode"], "unattended")
        self.assertEqual(payload["config"]["reason"], "config_missing")

    def test_protocol_check_accepts_matching_generated_contract(self) -> None:
        fake_codex = self.write_protocol_codex(self.protocol_files())
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "protocol-check", "--codex-bin", str(fake_codex),
             "--require-verified-version"],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["compatibility"], "schema_compatible_recorded_version")
        self.assertTrue(payload["schema_compatible"])

    def test_protocol_check_reports_missing_method(self) -> None:
        files = self.protocol_files()
        files["ServerNotification.json"] = {
            "description": "mentions turn/completed but defines no method"
        }
        fake_codex = self.write_protocol_codex(files, "0.151.0")
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "protocol-check", "--codex-bin", str(fake_codex)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 12)
        payload = json.loads(result.stdout)
        self.assertIn(
            "server_notification:turn/completed", payload["contract_failures"]
        )

    def test_protocol_check_rejects_missing_initialized_and_required_fields(self) -> None:
        mutations = {
            "missing_initialized": lambda files: files.__setitem__("ClientNotification.json", {"oneOf": []}),
            "missing_thread_cwd": lambda files: files["ThreadResumeResponse.json"]["definitions"]["Thread"]["required"].remove("cwd"),
            "missing_turn_id": lambda files: files["TurnStartResponse.json"]["definitions"]["Turn"]["required"].remove("id"),
            "missing_completion_status": lambda files: files["TurnCompletedNotification.json"]["definitions"]["Turn"]["required"].remove("status"),
            "unlinked_user_input": lambda files: files["TurnStartParams.json"]["properties"]["input"]["items"].__setitem__("$ref", "#/definitions/OtherInput"),
            "unlinked_resume_thread": lambda files: files["ThreadResumeResponse.json"]["properties"].__setitem__("thread", {"type": "string"}),
            "missing_initialize_client_info": lambda files: files["InitializeParams.json"]["required"].remove("clientInfo"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                files = self.protocol_files()
                mutate(files)
                fake = self.write_protocol_codex(files)
                result = subprocess.run(
                    [sys.executable, str(BRIDGE), "protocol-check", "--codex-bin", str(fake)],
                    text=True, capture_output=True, check=False, timeout=10,
                )
                self.assertEqual(result.returncode, 12, result.stdout)


class VendorSyncTests(unittest.TestCase):
    SIBLING = "codex-long-task-monitor"

    def test_vendored_copies_are_identical(self) -> None:
        sibling = HERE.parent.parent / self.SIBLING / "scripts" / "app_server_bridge.py"
        if not sibling.exists():
            self.skipTest(f"sibling skill not installed: {self.SIBLING}")
        self.assertEqual(
            (HERE / "app_server_bridge.py").read_bytes(),
            sibling.read_bytes(),
            "vendored app_server_bridge.py copies have diverged",
        )


if __name__ == "__main__":
    unittest.main()

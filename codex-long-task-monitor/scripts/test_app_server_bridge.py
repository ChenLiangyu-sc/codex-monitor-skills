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
from datetime import datetime, timezone
from pathlib import Path


HERE = Path(__file__).resolve().parent
BRIDGE = HERE / "app_server_bridge.py"
import semantic_events as se


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
            if MODE == "completion_before_reply":
                send({"method": "turn/completed", "params": {"turn": {"id": "turn_fake_1", "status": "completed"}}})
            send({"id": mid, "result": {"turn": {"id": "turn_fake_1", "status": "inProgress", "items": [], "error": None}}})
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

    def deliver(self, config: Path, once: bool = True, mode: str = "ok",
                turn_delay: float | None = None, timeout: int = 30) -> tuple[int, list[dict]]:
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

    def test_workspace_mismatch_dead_letters(self) -> None:
        config = self.write_config()
        event = self.publish_event(
            binding=json.loads(
                self.write_binding_file(workspace="/somewhere/else").read_text()
            )
        )
        _, records = self.deliver(config)
        self.assertEqual(records[0]["error_code"], "binding_mismatch")
        self.assertEqual(self.delivery_of(event["event_id"])["state"], "dead_letter")

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
        self.assertEqual(
            se.list_outbox(se.outbox_root(self.state))[0]["state"], "pending"
        )

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
        config_path = self.root / "cfg.json"
        binding_path = self.root / "bnd.json"
        codex_home = self.root / ".codex"
        codex_home.mkdir()
        workspace = self.root / "project"
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "init-config", "--output", str(config_path),
             "--instance-id", "workstation-1", "--workspace", str(workspace),
             "--codex-home", str(codex_home), "--enabled"],
            text=True, capture_output=True, check=False, timeout=10)
        self.assertEqual(result.returncode, 0)
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "init-binding", "--output", str(binding_path),
             "--thread-id", "thr_123", "--instance-id", "workstation-1",
             "--workspace", str(workspace), "--codex-home", str(codex_home)],
            text=True, capture_output=True, check=False, timeout=10)
        self.assertEqual(result.returncode, 0)
        config = se.load_bridge_config(config_path)
        binding = se.load_event_binding(binding_path)
        self.assertTrue(config["enabled"])
        self.assertEqual(binding["thread_id"], "thr_123")
        self.assertEqual(config["codex_home_id"], binding["codex_home_id"])
        self.assertEqual(binding["app_server_instance"], config["instance_id"])
        self.assertEqual(binding["workspace"], config["workspace"])

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

    def test_status_reports_unattended_without_config(self) -> None:
        result = subprocess.run(
            [sys.executable, str(BRIDGE), "status", "--state-dir", str(self.state),
             "--bridge-config", str(self.root / "absent.json")],
            text=True, capture_output=True, check=False, timeout=10)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["mode"], "unattended")
        self.assertEqual(payload["config"]["reason"], "config_missing")


class VendorSyncTests(unittest.TestCase):
    SIBLING = "codex-hpc-monitor"

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

#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path


HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "monitor_events.py"
import semantic_events as se


class MonitorEventsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.binding = {
            "schema": se.EVENT_BINDING_SCHEMA,
            "codex_home_id": se.codex_home_digest(self.root / ".codex"),
            "app_server_instance": "local-1",
            "thread_id": "thr_1",
            "workspace": str(self.root / "workspace"),
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def publish(self) -> dict:
        event = se.build_event(
            backend="artifact",
            handle="task-opaque",
            generation="run_1",
            terminal_digest="sha256:" + "a" * 64,
            event="transport_success",
            exit_code=0,
            binding=self.binding,
        )
        se.publish_event(se.outbox_root(self.state), event)
        return event

    def run_cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            text=True, capture_output=True, check=False, timeout=10,
        )

    def test_stdout_sink_emits_once_without_touching_wake_delivery(self) -> None:
        event = self.publish()
        first = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "console",
            "--mode", "stdout", "--once",
        )
        self.assertEqual(first.returncode, 0, first.stderr)
        records = [json.loads(line) for line in first.stdout.splitlines()]
        self.assertEqual(records[0]["event_id"], event["event_id"])
        self.assertEqual(records[0]["business_verdict"], "pending")
        self.assertNotIn("binding", records[0])
        self.assertEqual(records[-1]["state"], "emitted")
        second = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "console",
            "--mode", "stdout", "--once",
        )
        self.assertEqual(json.loads(second.stdout)["state"], "idle")
        delivery = se._read_delivery(
            se.event_dir(se.outbox_root(self.state), event["event_id"]),
            event["event_id"],
        )
        self.assertEqual(delivery["state"], "pending")

    def test_jsonl_sink_uses_private_file_and_timeline_reports_receipt(self) -> None:
        event = self.publish()
        output = self.root / "notifications" / "events.jsonl"
        result = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "audit",
            "--mode", "jsonl", "--output", str(output), "--once",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        notification = json.loads(output.read_text())
        self.assertEqual(notification["event_id"], event["event_id"])
        timeline = self.run_cli(
            "timeline", "--state-dir", str(self.state),
            "--event-id", event["event_id"],
        )
        payload = json.loads(timeline.stdout)
        stages = payload["events"][0]["stages"]
        self.assertEqual(stages[0]["stage"], "event_published")
        self.assertTrue(any(stage["stage"] == "notification_emitted" for stage in stages))

    def test_concurrent_sink_workers_emit_one_record(self) -> None:
        self.publish()
        output = self.root / "events.jsonl"
        results: list[subprocess.CompletedProcess[str]] = []

        def worker() -> None:
            results.append(self.run_cli(
                "notify", "--state-dir", str(self.state), "--sink-id", "shared",
                "--mode", "jsonl", "--output", str(output), "--once",
            ))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertTrue(all(item.returncode == 0 for item in results))
        self.assertEqual(len(output.read_text().splitlines()), 1)

    def test_dead_letter_retry_requires_confirmation_and_preserves_event(self) -> None:
        event = self.publish()
        outbox = se.outbox_root(self.state)
        claimed = se.claim_next_event(
            outbox, owner="owner", lease_seconds=60,
            now=datetime.now(timezone.utc),
        )
        self.assertIsNotNone(claimed)
        se.record_delivery_failure(
            outbox, event["event_id"], owner="owner", code="binding_mismatch",
            safe_message="safe", retryable=False, now=datetime.now(timezone.utc),
            max_attempts=1, backoff_initial_seconds=1, backoff_max_seconds=1,
        )
        refused = self.run_cli(
            "retry", event["event_id"], "--state-dir", str(self.state)
        )
        self.assertEqual(refused.returncode, 4)
        accepted = self.run_cli(
            "retry", event["event_id"], "--state-dir", str(self.state),
            "--i-mean-it",
        )
        self.assertEqual(accepted.returncode, 0, accepted.stdout)
        delivery = se._read_delivery(
            se.event_dir(outbox, event["event_id"]), event["event_id"]
        )
        self.assertEqual(delivery["state"], "pending")
        self.assertEqual(delivery["attempts"], 1)
        self.assertEqual(se.read_event(outbox, event["event_id"]), event)

    def test_invalid_sink_id_and_world_readable_output_fail_closed(self) -> None:
        self.publish()
        bad = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "../escape",
            "--mode", "stdout", "--once",
        )
        self.assertEqual(bad.returncode, 12)
        output = self.root / "open.jsonl"
        output.write_text("")
        output.chmod(0o644)
        bad = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "audit",
            "--mode", "jsonl", "--output", str(output), "--once",
        )
        self.assertEqual(bad.returncode, 12)

    def test_symlinked_sink_root_is_rejected(self) -> None:
        self.publish()
        target = self.root / "outside"
        target.mkdir()
        (self.state / "sinks").symlink_to(target, target_is_directory=True)
        bad = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "audit",
            "--mode", "stdout", "--once",
        )
        self.assertEqual(bad.returncode, 12)
        self.assertEqual(list(target.iterdir()), [])

    def test_malformed_or_mismatched_sink_receipt_fails_closed(self) -> None:
        event = self.publish()
        sink = self.state / "sinks" / "audit"
        sink.mkdir(parents=True)
        receipt = sink / f"{event['event_id'].removeprefix('sha256:')}.json"
        receipt.write_text(json.dumps({
            "schema": "codex-monitor.sink-receipt/v1",
            "sink_id": "other",
            "mode": "jsonl",
            "event_id": event["event_id"],
            "emitted_at": "not-a-time",
        }))
        receipt.chmod(0o600)
        bad = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "audit",
            "--mode", "stdout", "--once",
        )
        self.assertEqual(bad.returncode, 12)

    def test_existing_output_parent_permissions_are_preserved(self) -> None:
        self.publish()
        event_dir = self.root / "shared-output"
        event_dir.mkdir(mode=0o755)
        output = event_dir / "events.jsonl"
        result = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "audit",
            "--mode", "jsonl", "--output", str(output), "--once",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(event_dir.stat().st_mode & 0o777, 0o755)

    def test_reusing_sink_id_with_different_destination_fails_closed(self) -> None:
        self.publish()
        first = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "stable",
            "--mode", "jsonl", "--output", str(self.root / "one.jsonl"), "--once",
        )
        self.assertEqual(first.returncode, 0, first.stdout)
        second = self.run_cli(
            "notify", "--state-dir", str(self.state), "--sink-id", "stable",
            "--mode", "jsonl", "--output", str(self.root / "two.jsonl"), "--once",
        )
        self.assertEqual(second.returncode, 12, second.stdout)
        self.assertFalse((self.root / "two.jsonl").exists())


class VendorSyncTests(unittest.TestCase):
    def test_vendored_copy_is_identical(self) -> None:
        sibling = HERE.parent.parent / "codex-long-task-monitor" / "scripts" / "monitor_events.py"
        if not sibling.exists():
            self.skipTest("sibling skill not installed")
        self.assertEqual(SCRIPT.read_bytes(), sibling.read_bytes())


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Integration tests for supervise_artifact.py."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "supervise_artifact.py"
WATCHER = HERE / "watch_artifact.py"
SPEC = importlib.util.spec_from_file_location("supervise_artifact", SCRIPT)
assert SPEC and SPEC.loader
SUPERVISOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUPERVISOR)


class ArtifactSupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.artifact = self.root / "result.json"

    def tearDown(self) -> None:
        self.stop_current()
        self.temp.cleanup()

    def start_command(self, *extra: str) -> list[str]:
        return [
            sys.executable,
            str(SCRIPT),
            "start",
            str(self.artifact),
            "--state-dir",
            str(self.state),
            "--watcher-path",
            str(WATCHER),
            "--poll-seconds",
            "0.05",
            "--timeout-seconds",
            "5",
            "--invalid-grace-seconds",
            "0.1",
            "--success-json",
            "true",
            "--failure-json",
            "false",
            "--expect-json",
            'request_id="req-1"',
            "--require-nonempty",
            "payload",
            "--handshake-seconds",
            "3",
            *extra,
        ]

    def run_cli(self, argv: list[str]) -> tuple[subprocess.CompletedProcess[str], dict]:
        result = subprocess.run(argv, text=True, capture_output=True, check=False, timeout=8)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        return result, payload

    def start(self, *extra: str) -> tuple[subprocess.CompletedProcess[str], dict]:
        return self.run_cli(self.start_command(*extra))

    def status(self, handle: str, require_terminal: bool = False) -> tuple[subprocess.CompletedProcess[str], dict]:
        argv = [
            sys.executable,
            str(SCRIPT),
            "status",
            handle,
            "--state-dir",
            str(self.state),
        ]
        if require_terminal:
            argv.append("--require-terminal")
        return self.run_cli(argv)

    def wait_terminal(self, handle: str, timeout: float = 5.0) -> dict:
        deadline = time.monotonic() + timeout
        latest = {}
        while time.monotonic() < deadline:
            _, latest = self.status(handle)
            if latest.get("state") in {"terminal", "verification_failed", "supervisor_lost"}:
                return latest
            time.sleep(0.05)
        self.fail(f"terminal not reached: {latest}")

    def stop_current(self) -> None:
        artifacts = self.state / "artifacts"
        if not artifacts.is_dir():
            return
        for current in artifacts.glob("*/current.json"):
            payload = json.loads(current.read_text(encoding="utf-8"))
            handle = payload["task_handle"]
            _, status = self.status(handle)
            supervisor = status.get("supervisor") or {}
            if status.get("state") == "active" and isinstance(supervisor.get("pid"), int):
                try:
                    os.kill(supervisor["pid"], signal.SIGTERM)
                except ProcessLookupError:
                    continue

    def write_artifact(self, status: bool = True, payload: str = "done") -> None:
        self.artifact.write_text(
            json.dumps({"request_id": "req-1", "status": status, "payload": payload}),
            encoding="utf-8",
        )

    def test_success_is_detached_and_verified(self) -> None:
        self.write_artifact()
        started, payload = self.start()
        self.assertEqual(started.returncode, 0)
        handle = payload["task_handle"]
        terminal = self.wait_terminal(handle)
        self.assertEqual(terminal["state"], "terminal")
        self.assertTrue(terminal["terminal_verified"])
        self.assertEqual(terminal["terminal"]["observer_outcome"], "condition_satisfied")
        checked, _ = self.status(handle, require_terminal=True)
        self.assertEqual(checked.returncode, 0)

    def test_wait_reads_only_local_supervisor_state(self) -> None:
        self.write_artifact()
        _, payload = self.start()
        result, waited = self.run_cli(
            [
                sys.executable,
                str(SCRIPT),
                "wait",
                payload["task_handle"],
                "--state-dir",
                str(self.state),
                "--timeout-seconds",
                "3",
                "--poll-seconds",
                "0.05",
                "--notification-worker-ack",
            ]
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(waited["state"], "terminal")
        self.assertEqual(waited["terminal"]["observer_outcome"], "condition_satisfied")

    def test_wait_timeout_does_not_change_monitor_state(self) -> None:
        _, payload = self.start()
        result, waited = self.run_cli(
            [
                sys.executable,
                str(SCRIPT),
                "wait",
                payload["task_handle"],
                "--state-dir",
                str(self.state),
                "--timeout-seconds",
                "0.1",
                "--poll-seconds",
                "0.05",
                "--notification-worker-ack",
            ]
        )
        self.assertEqual(result.returncode, 4)
        self.assertEqual(waited["state"], "wait_timeout")
        _, current = self.status(payload["task_handle"])
        self.assertEqual(current["state"], "active")

    def test_wait_requires_notification_worker_ack(self) -> None:
        result, payload = self.run_cli(
            [
                sys.executable,
                str(SCRIPT),
                "wait",
                "artifact_" + "a" * 32,
                "--state-dir",
                str(self.state),
                "--timeout-seconds",
                "1",
            ]
        )
        self.assertEqual(result.returncode, 12)
        self.assertIn("does not authenticate", payload["detail"])

    def test_failure_and_timeout_are_not_success(self) -> None:
        self.write_artifact(status=False)
        _, payload = self.start()
        handle = payload["task_handle"]
        terminal = self.wait_terminal(handle)
        self.assertEqual(terminal["terminal"]["observer_outcome"], "terminal_or_contract_failure")
        checked, _ = self.status(handle, require_terminal=True)
        self.assertEqual(checked.returncode, 3)

        self.artifact.unlink()
        result, payload = self.start("--restart")
        self.assertEqual(result.returncode, 0)
        terminal = self.wait_terminal(payload["task_handle"], timeout=7)
        self.assertEqual(terminal["terminal"]["observer_outcome"], "deadline_exceeded")

    def test_duplicate_start_uses_one_monitor(self) -> None:
        first, payload = self.start()
        self.assertEqual(first.returncode, 0)
        self.assertEqual(payload["state"], "active")
        self.assertTrue(payload["watcher_alive"])
        second, duplicate = self.start()
        self.assertEqual(second.returncode, 2)
        self.assertEqual(duplicate["start_result"], "already_active")

    def test_launch_handshake_requires_verified_watcher_or_terminal(self) -> None:
        self.assertFalse(
            SUPERVISOR.launch_handshake_confirmed(
                {"state": "active", "supervisor_alive": True, "watcher_alive": False}
            )
        )
        self.assertFalse(
            SUPERVISOR.launch_handshake_confirmed(
                {"state": "exit_observed_terminal_missing", "watcher_alive": False}
            )
        )
        self.assertTrue(
            SUPERVISOR.launch_handshake_confirmed(
                {"state": "active", "supervisor_alive": True, "watcher_alive": True}
            )
        )
        self.assertTrue(SUPERVISOR.launch_handshake_confirmed({"state": "terminal"}))

    def test_terminal_is_immutable_and_restart_increments_generation(self) -> None:
        self.write_artifact()
        _, payload = self.start()
        first = self.wait_terminal(payload["task_handle"])
        terminal_path = Path(first["run_dir"]) / "terminal.json"
        before = hashlib.sha256(terminal_path.read_bytes()).hexdigest()
        rejected, result = self.start()
        self.assertEqual(rejected.returncode, 3)
        self.assertEqual(result["start_result"], "restart_required")
        self.assertEqual(hashlib.sha256(terminal_path.read_bytes()).hexdigest(), before)
        restarted, result = self.start("--restart")
        self.assertEqual(restarted.returncode, 0)
        second = self.wait_terminal(result["task_handle"])
        self.assertEqual(second["terminal"]["generation"], 2)

    def test_contract_conflict_is_rejected(self) -> None:
        first, payload = self.start()
        self.assertEqual(first.returncode, 0)
        command = self.start_command()
        success_index = command.index("--success-json") + 1
        command[success_index] = '"complete"'
        conflict, result = self.run_cli(command)
        self.assertEqual(conflict.returncode, 12)
        self.assertEqual(result["start_result"], "contract_conflict")
        self.assertEqual(result["task_handle"], payload["task_handle"])

    def test_supervisor_crash_never_creates_false_terminal(self) -> None:
        _, payload = self.start()
        handle = payload["task_handle"]
        _, active = self.status(handle)
        run = Path(active["run_dir"])
        os.kill(active["supervisor"]["pid"], signal.SIGKILL)
        deadline = time.monotonic() + 4
        latest = {}
        while time.monotonic() < deadline:
            _, latest = self.status(handle)
            if latest.get("state") == "supervisor_lost" and not latest.get("watcher_alive"):
                break
            time.sleep(0.05)
        self.assertEqual(latest.get("state"), "supervisor_lost")
        self.assertFalse((run / "terminal.json").exists())

    def test_terminal_does_not_embed_artifact_content_or_watcher_output(self) -> None:
        secret_marker = "CONTROLLED_MARKER_NOT_FOR_TERMINAL"
        self.write_artifact(payload=secret_marker)
        _, payload = self.start()
        terminal = self.wait_terminal(payload["task_handle"])
        serialized = json.dumps(terminal["terminal"], sort_keys=True)
        self.assertNotIn(secret_marker, serialized)
        self.assertNotIn(str(self.artifact), serialized)

    def test_terminal_outcome_must_match_child_exit(self) -> None:
        self.write_artifact()
        _, payload = self.start()
        terminal = self.wait_terminal(payload["task_handle"])
        terminal_path = Path(terminal["run_dir"]) / "terminal.json"
        document = json.loads(terminal_path.read_text(encoding="utf-8"))
        document["observer_outcome"] = "terminal_or_contract_failure"
        terminal_path.write_text(json.dumps(document), encoding="utf-8")
        _, checked = self.status(payload["task_handle"])
        self.assertEqual(checked["state"], "verification_failed")
        self.assertFalse(checked["terminal_verified"])

    def test_publish_terminal_no_replace(self) -> None:
        path = self.root / "terminal.json"
        SUPERVISOR.publish_json_no_replace(path, {"value": 1})
        with self.assertRaises(FileExistsError):
            SUPERVISOR.publish_json_no_replace(path, {"value": 2})
        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["value"], 1)

    def test_launch_unconfirmed_uses_current_manifest_anchor(self) -> None:
        contract = {
            "artifact_path": str(self.artifact),
            "exists_is_success": True,
            "json_field": "status",
            "success_json": [],
            "failure_json": [],
            "expect_json": [],
            "require_nonempty": [],
            "not_before_epoch_seconds": None,
            "min_bytes": 1,
            "max_json_bytes": 1024,
            "poll_seconds": 1.0,
            "timeout_seconds": 5.0,
            "invalid_grace_seconds": 1.0,
        }
        handle = SUPERVISOR.task_handle(contract)
        base = SUPERVISOR.base_dir(self.state, handle)
        run = base / "runs" / "run_1_1_deadbeef"
        SUPERVISOR.ensure_private_directory(run)
        manifest = {
            "schema_version": f"{SUPERVISOR.SCHEMA_PREFIX}.manifest/v1",
            "task_handle": handle,
            "run_id": run.name,
            "contract_digest": SUPERVISOR.sha256_bytes(SUPERVISOR.canonical_json(contract)),
        }
        SUPERVISOR.publish_json_no_replace(run / "manifest.json", manifest)
        SUPERVISOR.replace_json(
            base / "current.json",
            {
                "task_handle": handle,
                "run_id": run.name,
                "manifest_sha256": SUPERVISOR.sha256_file(run / "manifest.json"),
            },
        )
        status = SUPERVISOR.run_status(run, handle)
        self.assertEqual(status["state"], "launch_unconfirmed")

    def test_symlink_monitor_lock_is_rejected(self) -> None:
        contract = SUPERVISOR.contract_from_args(
            SUPERVISOR.parser().parse_args(
                [
                    "start",
                    str(self.artifact),
                    "--timeout-seconds",
                    "5",
                    "--exists-is-success",
                ]
            )
        )
        base = SUPERVISOR.base_dir(self.state, SUPERVISOR.task_handle(contract))
        SUPERVISOR.ensure_private_directory(base)
        target = self.root / "lock-target"
        target.write_text("untouched", encoding="utf-8")
        (base / "monitor.lock").symlink_to(target)
        with self.assertRaises(OSError):
            SUPERVISOR.open_lifetime_lock(base)
        self.assertEqual(target.read_text(encoding="utf-8"), "untouched")

    def test_network_state_root_is_rejected(self) -> None:
        with mock.patch.object(SUPERVISOR, "filesystem_type", return_value="nfs"):
            with self.assertRaisesRegex(ValueError, "local storage"):
                SUPERVISOR.validate_local_state_root(self.state)

    def test_real_temp_state_root_is_local(self) -> None:
        self.assertNotIn(
            SUPERVISOR.filesystem_type(self.state), SUPERVISOR.NETWORK_FILESYSTEMS
        )

    def test_cli_rejects_invalid_or_overlapping_json_contract(self) -> None:
        invalid = self.start_command()
        invalid[invalid.index("--success-json") + 1] = "not-json"
        result, payload = self.run_cli(invalid)
        self.assertEqual(result.returncode, 12)
        self.assertIn("valid JSON", payload["detail"])

        overlap = self.start_command()
        overlap[overlap.index("--failure-json") + 1] = "true"
        result, payload = self.run_cli(overlap)
        self.assertEqual(result.returncode, 12)
        self.assertIn("must not overlap", payload["detail"])


if __name__ == "__main__":
    unittest.main()

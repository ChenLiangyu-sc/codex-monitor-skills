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
import semantic_events

FAKE_APP_SERVER_OK = r'''#!/usr/bin/env python3
import json, os, sys
LOG = os.environ["FAKE_LOG"]
CWD = os.environ.get("FAKE_THREAD_CWD", "/default/workspace")
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    message = json.loads(line)
    method = message.get("method")
    mid = message.get("id")
    if method == "initialize":
        send({"id": mid, "result": {"userInfo": {"id": "u"}}})
    elif method == "thread/resume":
        send({"id": mid, "result": {"thread": {"id": message["params"]["threadId"], "cwd": CWD}}})
    elif method == "turn/start":
        with open(LOG, "a", encoding="utf-8") as handle:
            handle.write(message["params"]["input"][0]["text"] + "\n===\n")
        send({"id": mid, "result": {"turn": {"id": "turn_chain_1", "status": "inProgress", "error": None}}})
        send({"method": "turn/completed", "params": {"turn": {"id": "turn_chain_1", "status": "completed"}}})
'''


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

    def test_deadline_is_frozen_across_restart_generations(self) -> None:
        self.write_artifact()
        _, started = self.start()
        handle = started["task_handle"]
        terminal = self.wait_terminal(handle)
        deadline = terminal["deadline_epoch_seconds"]
        self.assertIsNotNone(deadline)
        # Restart uses the same absolute deadline; the frozen watcher argv is
        # capped to the remaining window rather than a fresh full timeout.
        self.write_artifact(status=False)
        restart_result, restarted = self.start("--restart")
        self.assertEqual(restart_result.returncode, 0)
        manifest = json.loads(
            (Path(restarted["run_dir"]) / "manifest.json").read_text()
        )
        self.assertEqual(manifest["deadline_epoch_seconds"], deadline)
        argv_timeout = float(manifest["watcher_argv"][argv_index(manifest["watcher_argv"], "--timeout-seconds") + 1])
        self.assertLess(argv_timeout, 5.0)

    def test_expired_window_refuses_restart(self) -> None:
        self.write_artifact()
        _, started = self.start()
        handle = started["task_handle"]
        self.wait_terminal(handle)
        current_path = None
        for current in (self.state / "artifacts").glob("*/current.json"):
            current_path = current
        payload = json.loads(current_path.read_text())
        payload["deadline_epoch_seconds"] = time.time() - 10
        current_path.write_text(json.dumps(payload))
        result, refused = self.start("--restart")
        self.assertEqual(result.returncode, 12)
        self.assertIn("observation window expired", refused["detail"])
    def write_binding(self) -> Path:
        binding = {
            "schema": "codex-monitor.event-binding/v1",
            "codex_home_id": semantic_events.codex_home_digest(self.root / ".codex"),
            "app_server_instance": "workstation-1",
            "thread_id": "thr_test_1",
            "workspace": str(self.root),
        }
        path = self.root / "binding.json"
        path.write_text(json.dumps(binding), encoding="utf-8")
        path.chmod(0o600)
        return path

    def outbox(self) -> Path:
        return semantic_events.outbox_root(self.state)

    def wait_run_file(self, run: Path, name: str, timeout: float = 8.0) -> Path:
        deadline = time.monotonic() + timeout
        path = run / name
        handle_dir = run.parent.parent
        current = json.loads((handle_dir / "current.json").read_text())
        while time.monotonic() < deadline:
            if path.exists():
                return path
            # status observations trigger the reconciler, closing any crash
            # window between terminal and event publication.
            self.status(current["task_handle"])
            time.sleep(0.05)
        self.fail(f"{name} was not published for {run}")

    def test_terminal_publishes_one_semantic_event_with_binding(self) -> None:
        binding = self.write_binding()
        self.write_artifact()
        result, started = self.start("--event-binding", str(binding))
        self.assertEqual(result.returncode, 0)
        handle = started["task_handle"]
        terminal = self.wait_terminal(handle)
        run = Path(terminal["run_dir"])
        published = json.loads(self.wait_run_file(run, "semantic_event.json").read_text())
        self.assertEqual(published["event"], "transport_success")
        entries = semantic_events.list_outbox(self.outbox())
        self.assertEqual(len(entries), 1)
        event = semantic_events.read_event(self.outbox(), published["event_id"])
        self.assertEqual(event["monitor"]["backend"], "artifact")
        self.assertEqual(event["monitor"]["handle"], handle)
        terminal_sha = hashlib.sha256((run / "terminal.json").read_bytes()).hexdigest()
        self.assertEqual(event["monitor"]["terminal_digest"], f"sha256:{terminal_sha}")

    def test_failure_artifact_maps_to_transport_failure(self) -> None:
        binding = self.write_binding()
        self.write_artifact(status=False)
        result, started = self.start("--event-binding", str(binding))
        self.assertEqual(result.returncode, 0)
        self.wait_terminal(started["task_handle"])
        entries = semantic_events.list_outbox(self.outbox())
        self.assertEqual([entry["event"] for entry in entries], ["transport_failure"])

    def test_no_binding_publishes_no_event(self) -> None:
        self.write_artifact()
        result, started = self.start()
        self.assertEqual(result.returncode, 0)
        terminal = self.wait_terminal(started["task_handle"])
        run = Path(terminal["run_dir"])
        self.assertFalse((run / "semantic_event.json").exists())
        self.assertEqual(semantic_events.list_outbox(self.outbox()), [])

    def test_custom_event_backend_is_recorded(self) -> None:
        binding = self.write_binding()
        self.write_artifact()
        result, started = self.start(
            "--event-binding", str(binding), "--event-backend", "dispatch"
        )
        self.assertEqual(result.returncode, 0)
        self.wait_terminal(started["task_handle"])
        entries = semantic_events.list_outbox(self.outbox())
        self.assertEqual(entries[0]["backend"], "dispatch")

    def run_doctor(self, *extra: str):
        command = [
            sys.executable, str(SCRIPT), "doctor",
            "--state-dir", str(self.state), *extra,
        ]
        result = subprocess.run(command, text=True, capture_output=True, check=False, timeout=15)
        if "--format" in extra and extra[extra.index("--format") + 1] == "text":
            return result, result.stdout
        return result, json.loads(result.stdout)

    def write_bridge_config(self) -> Path:
        config = {
            "schema": semantic_events.BRIDGE_CONFIG_SCHEMA,
            "enabled": True,
            "instance_id": "workstation-1",
            "codex_home": str(self.root / ".codex"),
            "codex_home_id": semantic_events.codex_home_digest(self.root / ".codex"),
            "workspace": str(self.root),
            "transport": {"type": "stdio", "command": ["codex", "app-server"]},
            "request_timeout_seconds": 30,
            "poll_seconds": 5,
            "lease_seconds": 300,
            "max_attempts": 16,
            "backoff_initial_seconds": 5,
            "backoff_max_seconds": 3600,
            "turn_completion_timeout_seconds": 3600,
        }
        path = self.root / "bridge.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        path.chmod(0o600)
        return path

    def test_doctor_defaults_to_unattended_and_formats_agree(self) -> None:
        result, payload = self.run_doctor()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(payload["mode"]["selected"], "unattended")
        self.assertEqual(payload["mode"]["reason"], "bridge_not_configured")
        self.assertFalse(payload["auto_resume_available"])
        self.assertTrue(payload["state_root"]["suitable"])
        _, text = self.run_doctor("--format", "text")
        self.assertIn("mode: unattended (requested auto; reason: bridge_not_configured)", text)
        self.assertIn("auto-resume available: no", text)

    def test_doctor_selects_bridge_with_enabled_config(self) -> None:
        config = self.write_bridge_config()
        _, payload = self.run_doctor("--bridge-config", str(config))
        self.assertEqual(payload["mode"]["selected"], "external-event-bridge")
        self.assertTrue(payload["auto_resume_available"])
        _, payload = self.run_doctor(
            "--bridge-config", str(config), "--mode", "unattended"
        )
        self.assertEqual(payload["mode"]["selected"], "unattended")
        self.assertEqual(payload["mode"]["requested"], "unattended")

    def test_doctor_invalid_config_falls_back_safely(self) -> None:
        bad = self.root / "bad.json"
        bad.write_text(json.dumps({"enabled": true_json()}) if False else '{"schema": "x"}', encoding="utf-8")
        bad.chmod(0o600)
        _, payload = self.run_doctor("--bridge-config", str(bad))
        self.assertEqual(payload["mode"]["selected"], "unattended")
        self.assertTrue(payload["mode"]["reason"].startswith("bridge_config_invalid"))

    def test_list_explain_cleanup(self) -> None:
        self.write_artifact()
        result, started = self.start()
        handle = started["task_handle"]
        terminal = self.wait_terminal(handle)
        listing = subprocess.run(
            [sys.executable, str(SCRIPT), "list", "--state-dir", str(self.state)],
            text=True, capture_output=True, check=False, timeout=15)
        entries = json.loads(listing.stdout)["monitors"]
        self.assertEqual(entries[0]["task_handle"], handle)
        self.assertEqual(entries[0]["state"], "terminal")
        self.assertEqual(entries[0]["generation"], terminal["terminal"]["generation"])
        explain = subprocess.run(
            [sys.executable, str(SCRIPT), "explain", handle, "--state-dir", str(self.state)],
            text=True, capture_output=True, check=False, timeout=15)
        self.assertIn("state=terminal", explain.stdout)
        self.assertIn("wake events enabled: no", explain.stdout)
        self.assertIn("cannot wake", explain.stdout)
        cleanup = subprocess.run(
            [sys.executable, str(SCRIPT), "cleanup", "--state-dir", str(self.state)],
            text=True, capture_output=True, check=False, timeout=15)
        payload = json.loads(cleanup.stdout)
        self.assertEqual(payload["mode"], "dry_run")
        self.assertEqual(payload["removed_count"], 0)

    def test_full_chain_start_to_wake_and_idempotent_postflight(self) -> None:
        fake = self.root / "fake_app_server.py"
        fake.write_text(FAKE_APP_SERVER_OK, encoding="utf-8")
        fake.chmod(0o700)
        wake_log = self.root / "wake.log"
        config = {
            "schema": semantic_events.BRIDGE_CONFIG_SCHEMA,
            "enabled": True,
            "instance_id": "workstation-1",
            "codex_home": str(self.root / ".codex"),
            "codex_home_id": semantic_events.codex_home_digest(self.root / ".codex"),
            "workspace": str(self.root / "project"),
            "transport": {"type": "stdio", "command": [sys.executable, str(fake)]},
            "request_timeout_seconds": 10, "poll_seconds": 0.05,
            "lease_seconds": 60, "max_attempts": 5,
            "backoff_initial_seconds": 0.05, "backoff_max_seconds": 0.2,
            "turn_completion_timeout_seconds": 30,
        }
        config_path = self.root / "bridge.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        config_path.chmod(0o600)
        (self.root / "project").mkdir()
        binding = {
            "schema": semantic_events.EVENT_BINDING_SCHEMA,
            "codex_home_id": semantic_events.codex_home_digest(self.root / ".codex"),
            "app_server_instance": "workstation-1",
            "thread_id": "thr_chain",
            "workspace": str(self.root / "project"),
        }
        binding_path = self.root / "binding.json"
        binding_path.write_text(json.dumps(binding), encoding="utf-8")
        binding_path.chmod(0o600)

        self.write_artifact()
        result, started = self.start(
            "--event-binding", str(binding_path), "--bridge-config", str(config_path)
        )
        self.assertEqual(result.returncode, 0)
        handle = started["task_handle"]
        terminal = self.wait_terminal(handle)
        run = Path(terminal["run_dir"])
        published = json.loads((run / "semantic_event.json").read_text())
        event_id = published["event_id"]

        import os as _os
        delivered = subprocess.run(
            [sys.executable, str(HERE / "app_server_bridge.py"), "deliver",
             "--state-dir", str(self.state), "--bridge-config", str(config_path),
             "--once"],
            text=True, capture_output=True, check=False,
            env={
                **_os.environ,
                "FAKE_LOG": str(wake_log),
                "FAKE_THREAD_CWD": str(self.root / "project"),
            }, timeout=30)
        self.assertEqual(delivered.returncode, 0, delivered.stdout + delivered.stderr)
        records = [json.loads(line) for line in delivered.stdout.splitlines() if line.strip()]
        self.assertEqual(records[0]["state"], "acknowledged", records)
        self.assertEqual(records[0]["turn_status"], "completed")
        wake = wake_log.read_text()
        self.assertIn(f"event_id={event_id}", wake)
        self.assertEqual(wake.count("==="), 1)

        guard = str(HERE / "postflight_guard.py")
        check = subprocess.run([sys.executable, guard, "check", event_id,
                                "--state-dir", str(self.state)],
                               text=True, capture_output=True, check=False, timeout=10)
        self.assertEqual(check.returncode, 0)
        terminal_digest = "sha256:" + hashlib.sha256(
            (run / "terminal.json").read_bytes()
        ).hexdigest()
        mark = subprocess.run([sys.executable, guard, "mark", event_id,
                               "--terminal-digest", terminal_digest,
                               "--state-dir", str(self.state)],
                              text=True, capture_output=True, check=False, timeout=10)
        self.assertEqual(mark.returncode, 0)
        duplicate = subprocess.run([sys.executable, guard, "mark", event_id,
                                    "--terminal-digest", terminal_digest,
                                    "--state-dir", str(self.state)],
                                   text=True, capture_output=True, check=False, timeout=10)
        self.assertEqual(duplicate.returncode, 3)
        self.assertEqual(json.loads(duplicate.stdout)["state"], "already_marked")
def argv_index(argv: list, flag: str) -> int:
    return argv.index(flag)

if __name__ == "__main__":
    unittest.main()

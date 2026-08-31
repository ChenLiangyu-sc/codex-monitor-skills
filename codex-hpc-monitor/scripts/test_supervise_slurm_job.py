#!/usr/bin/env python3
"""Integration and artifact tests for supervise_slurm_job.py."""

from __future__ import annotations

import hashlib
import importlib.util
from datetime import datetime, timezone
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
SCRIPT = HERE / "supervise_slurm_job.py"
SPEC = importlib.util.spec_from_file_location("supervise_slurm_job", SCRIPT)
assert SPEC and SPEC.loader
SUPERVISOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUPERVISOR)
import semantic_events
import app_server_bridge

FAKE_APP_SERVER_OK = r'''#!/usr/bin/env python3
import json, os, sys
if sys.argv[1:] == ["--version"]:
    print("codex-cli 0.151.0")
    raise SystemExit(0)
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


FAKE_WATCHER = r'''#!/usr/bin/env python3
import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("job_id")
p.add_argument("--host", required=True)
p.add_argument("--state-dir", type=Path, required=True)
p.add_argument("--poll-seconds")
p.add_argument("--pending-alert-seconds")
p.add_argument("--terminal-observability-seconds")
p.add_argument("--max-watch-seconds")
p.add_argument("--query-failures")
p.add_argument("--expected-owner")
p.add_argument("--expected-job-name")
p.add_argument("--expected-partition")
a = p.parse_args()
if os.environ.get("FAKE_WRITE_STATE") == "1":
    state_path = a.state_dir / f"{a.host}-{a.job_id}.state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({
        "schema_version": 1,
        "host": a.host,
        "event": "observed",
        "snapshot": {"job_id": a.job_id, "state": "RUNNING", "elapsed": "00:00:01"},
    }) + "\n", encoding="utf-8")
counter = os.environ.get("FAKE_COUNTER")
if counter:
    with open(counter, "a", encoding="utf-8") as h:
        h.write(f"{os.getpid()}\n")
        h.flush()
        os.fsync(h.fileno())
delay = float(os.environ.get("FAKE_WATCH_SECONDS", "0.05"))
time.sleep(delay)
if os.environ.get("FAKE_SELF_SIGNAL"):
    os.kill(os.getpid(), int(os.environ["FAKE_SELF_SIGNAL"]))
code = int(os.environ.get("FAKE_EXIT", "0"))
if os.environ.get("FAKE_WRITE_RESULT", "1") == "1":
    event = os.environ.get("FAKE_EVENT", "completed" if code == 0 else "terminal_failure")
    state = "COMPLETED" if code == 0 else "FAILED"
    exit_code = "0:0" if code == 0 else "1:0"
    classification = "scheduler_success" if code == 0 else "scheduler_terminal_failure"
    path = a.state_dir / f"{a.host}-{a.job_id}.result.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "event": event,
        "job_id": a.job_id,
        "state": state,
        "exit_code": exit_code,
        "slurm_classification": classification,
        "scope": "slurm_only",
        "project_gate_evaluated": False,
    }) + "\n", encoding="utf-8")
sys.exit(code)
'''


class SupervisorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.fake = self.root / "fake_watcher.py"
        self.fake.write_text(FAKE_WATCHER, encoding="utf-8")
        self.fake.chmod(0o700)

    def tearDown(self) -> None:
        self.stop_current()
        self.temp.cleanup()

    def command(self, command: str, *extra: str) -> list[str]:
        base = [sys.executable, str(SCRIPT), command]
        if command == "start":
            base += [
                "12345",
                "--host", "fakehost",
                "--state-dir", str(self.state),
                "--watcher-path", str(self.fake),
                "--poll-seconds", "1",
                "--handshake-seconds", "3",
            ]
        elif command in {"status", "wait"}:
            base += ["12345", "--host", "fakehost", "--state-dir", str(self.state)]
            if command == "wait":
                base.append("--notification-worker-ack")
        return base + list(extra)

    def run_cli(self, command: str, *extra: str, env: dict[str, str] | None = None) -> tuple[subprocess.CompletedProcess[str], dict]:
        effective = os.environ.copy()
        if env:
            effective.update(env)
        result = subprocess.run(
            self.command(command, *extra),
            text=True,
            capture_output=True,
            check=False,
            env=effective,
            timeout=8,
        )
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        return result, payload

    def status(self) -> dict:
        return self.run_cli("status")[1]

    def wait_state(self, expected: str, timeout: float = 5.0) -> dict:
        deadline = time.monotonic() + timeout
        latest = {}
        while time.monotonic() < deadline:
            latest = self.status()
            if latest.get("state") == expected:
                return latest
            time.sleep(0.05)
        self.fail(f"did not reach {expected}: {latest}")

    def stop_current(self) -> None:
        try:
            status = self.status()
        except Exception:
            return
        supervisor = status.get("supervisor") or {}
        pid = supervisor.get("pid")
        if status.get("state") == "active" and isinstance(pid, int):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                if self.status().get("state") != "active":
                    break
                time.sleep(0.05)

    def test_fake_watcher_success_and_nonzero(self) -> None:
        started, _ = self.run_cli("start", env={"FAKE_EXIT": "0"})
        self.assertEqual(started.returncode, 0)
        terminal = self.wait_state("terminal")["terminal"]
        self.assertEqual(terminal["watcher_exit_code"], 0)
        self.assertTrue(terminal["watcher_result"]["verified"])
        second, _ = self.run_cli("start", "--restart", env={"FAKE_EXIT": "3"})
        self.assertEqual(second.returncode, 0)
        terminal = self.wait_state("terminal")["terminal"]
        self.assertEqual(terminal["watcher_exit_code"], 3)
        self.assertEqual(terminal["observer_outcome"], "watcher_exit_nonzero")

    def test_watcher_signal_is_terminal(self) -> None:
        result, _ = self.run_cli(
            "start", env={"FAKE_SELF_SIGNAL": str(int(signal.SIGTERM))}
        )
        self.assertEqual(result.returncode, 0)
        terminal = self.wait_state("terminal")["terminal"]
        self.assertEqual(terminal["watcher_signal"], signal.SIGTERM)
        self.assertEqual(terminal["observer_outcome"], "watcher_signaled")

    def test_duplicate_start_has_one_watcher(self) -> None:
        counter = self.root / "starts.txt"
        env = {"FAKE_WATCH_SECONDS": "20", "FAKE_COUNTER": str(counter)}
        first, payload = self.run_cli("start", env=env)
        self.assertEqual(first.returncode, 0)
        self.assertEqual(payload["state"], "active")
        second, payload = self.run_cli("start", env=env)
        self.assertEqual(second.returncode, 2)
        self.assertEqual(payload["start_result"], "already_active")
        time.sleep(0.1)
        self.assertEqual(len(counter.read_text(encoding="utf-8").splitlines()), 1)

    def test_binding_cannot_be_retrofitted_to_active_unattended_run(self) -> None:
        first, payload = self.run_cli(
            "start", env={"FAKE_WATCH_SECONDS": "20"}
        )
        self.assertEqual(first.returncode, 0)
        self.assertEqual(payload["state"], "active")
        binding = self.write_binding()
        second, payload = self.run_cli(
            "start", "--event-binding", str(binding),
            env={"FAKE_WATCH_SECONDS": "20"},
        )
        self.assertEqual(second.returncode, 12)
        self.assertEqual(payload["start_result"], "active_run_binding_conflict")
        self.assertIsNone(payload["active_event_binding_digest"])
        self.assertTrue(payload["requested_event_binding_digest"].startswith("sha256:"))

    def test_binding_without_config_emits_prominent_warning(self) -> None:
        binding = self.write_binding()
        result, payload = self.run_cli(
            "start", "--event-binding", str(binding), env={"FAKE_EXIT": "0"}
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("*** WARNING [event_binding_without_bridge_config]", result.stderr)
        self.assertEqual(
            payload["warnings"][0]["code"],
            "event_binding_without_bridge_config",
        )

    def test_require_auto_resume_fails_before_launch_without_ready_bridge(self) -> None:
        binding = self.write_binding()
        result, payload = self.run_cli(
            "start", "--event-binding", str(binding), "--require-auto-resume"
        )
        self.assertEqual(result.returncode, 12)
        self.assertIn("event_binding_without_bridge_config", payload["detail"])
        self.assertFalse(self.state.exists())

    def test_require_auto_resume_accepts_activated_matching_bridge(self) -> None:
        binding = self.write_binding()
        config = self.write_bridge_config()
        loaded = semantic_events.load_bridge_config(config)
        semantic_events.activate_bridge(self.outbox(), loaded, [])
        identity = app_server_bridge._configured_cli_identity(
            loaded, timeout_seconds=5
        )
        app_server_bridge.record_lifecycle_smoke_receipt(
            self.state, loaded, identity,
            thread_id="thr_smoke", first_turn_id="turn_1", second_turn_id="turn_2",
        )
        result, payload = self.run_cli(
            "start", "--event-binding", str(binding),
            "--bridge-config", str(config), "--require-auto-resume",
            env={"FAKE_EXIT": "0"},
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertNotIn("warnings", payload)

    def test_require_auto_resume_rejects_missing_live_smoke_receipt(self) -> None:
        binding = self.write_binding()
        config = self.write_bridge_config()
        semantic_events.activate_bridge(
            self.outbox(), semantic_events.load_bridge_config(config), []
        )
        result, payload = self.run_cli(
            "start", "--event-binding", str(binding),
            "--bridge-config", str(config), "--require-auto-resume",
        )
        self.assertEqual(result.returncode, 12)
        self.assertIn("lifecycle_smoke_receipt_missing", payload["detail"])
        self.assertFalse((self.state / "slurm").exists())

    def test_require_auto_resume_rejects_codex_0149_before_launch(self) -> None:
        binding = self.write_binding()
        config = self.write_bridge_config(codex_version="0.149.1")
        semantic_events.activate_bridge(
            self.outbox(), semantic_events.load_bridge_config(config), []
        )
        result, payload = self.run_cli(
            "start", "--event-binding", str(binding),
            "--bridge-config", str(config), "--require-auto-resume",
        )
        self.assertEqual(result.returncode, 12)
        self.assertIn("codex_lifecycle_version_unverified", payload["detail"])
        self.assertFalse((self.state / "slurm").exists())

    def test_require_auto_resume_rejects_legacy_bare_codex_path(self) -> None:
        binding = self.write_binding()
        config_path = self.write_bridge_config()
        config = json.loads(config_path.read_text(encoding="utf-8"))
        config["transport"]["command"] = ["codex", "app-server"]
        config_path.write_text(json.dumps(config), encoding="utf-8")
        semantic_events.activate_bridge(
            self.outbox(), semantic_events.load_bridge_config(config_path), []
        )
        result, payload = self.run_cli(
            "start", "--event-binding", str(binding),
            "--bridge-config", str(config_path), "--require-auto-resume",
        )
        self.assertEqual(result.returncode, 12)
        self.assertIn("transport_executable_not_frozen", payload["detail"])
        self.assertFalse((self.state / "slurm").exists())

    def test_status_observer_exit_does_not_stop_watcher(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "20"})
        first = self.status()
        time.sleep(0.1)
        second = self.status()
        self.assertEqual(first["state"], "active")
        self.assertEqual(second["state"], "active")
        self.assertEqual(first["runtime"]["pid"], second["runtime"]["pid"])

    def test_terminal_is_not_overwritten_without_restart(self) -> None:
        self.run_cli("start")
        status = self.wait_state("terminal")
        path = Path(status["run_dir"]) / "terminal.json"
        before = hashlib.sha256(path.read_bytes()).hexdigest()
        result, payload = self.run_cli("start")
        self.assertEqual(result.returncode, 3)
        self.assertEqual(payload["start_result"], "restart_required")
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)

    def test_publish_terminal_no_replace(self) -> None:
        path = self.root / "terminal.json"
        SUPERVISOR.publish_json_no_replace(path, {"value": 1})
        with self.assertRaises(FileExistsError):
            SUPERVISOR.publish_json_no_replace(path, {"value": 2})
        self.assertEqual(json.loads(path.read_text())["value"], 1)

    def test_symlink_monitor_lock_is_rejected_without_touching_target(self) -> None:
        base = SUPERVISOR.base_dir(self.state, "fakehost", "12345")
        base.mkdir(parents=True)
        victim = self.root / "victim.txt"
        victim.write_text("preserve-me", encoding="utf-8")
        (base / "monitor.lock").symlink_to(victim)
        with self.assertRaises(OSError):
            SUPERVISOR.open_lifetime_lock(base)
        self.assertEqual(victim.read_text(encoding="utf-8"), "preserve-me")

    def test_pid_start_tick_mismatch_fails_closed(self) -> None:
        run = self.root / "run_test"
        run.mkdir()
        SUPERVISOR.replace_json(
            run / "supervisor_started.json",
            {"pid": os.getpid(), "pid_start_ticks": "not-the-current-process"},
        )
        status = SUPERVISOR.run_status(run, "fakehost", "12345")
        self.assertEqual(status["state"], "supervisor_lost")
        self.assertFalse(status["supervisor_alive"])

    def test_supervisor_sigterm_forwards_and_publishes_terminal(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "20"})
        active = self.status()
        os.kill(active["supervisor"]["pid"], signal.SIGTERM)
        terminal = self.wait_state("terminal")["terminal"]
        self.assertEqual(terminal["watcher_signal"], signal.SIGTERM)
        self.assertEqual(terminal["observer_outcome"], "watcher_signaled")

    def test_supervisor_crash_never_creates_false_terminal(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "20"})
        active = self.status()
        run = Path(active["run_dir"])
        os.kill(active["supervisor"]["pid"], signal.SIGKILL)
        deadline = time.monotonic() + 4
        latest = {}
        while time.monotonic() < deadline:
            latest = self.status()
            if latest.get("state") == "supervisor_lost" and not latest.get("watcher_alive"):
                break
            time.sleep(0.05)
        self.assertEqual(latest.get("state"), "supervisor_lost")
        self.assertFalse((run / "terminal.json").exists())
        self.assertFalse(latest.get("watcher_alive"))

    def test_require_terminal_is_one_shot(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "20"})
        result, payload = self.run_cli("status", "--require-terminal")
        self.assertEqual(result.returncode, 3)
        self.assertEqual(payload["state"], "active")

    def test_wait_returns_verified_success_and_failure(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "0.1"})
        result, payload = self.run_cli(
            "wait", "--timeout-seconds", "2", "--poll-seconds", "0.01"
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(payload["state"], "terminal")
        self.assertTrue(payload["terminal_verified"])
        self.assertEqual(payload["slurm_classification"], "scheduler_success")

        self.run_cli(
            "start",
            "--restart",
            env={"FAKE_WATCH_SECONDS": "0.1", "FAKE_EXIT": "3"},
        )
        result, payload = self.run_cli(
            "wait", "--timeout-seconds", "2", "--poll-seconds", "0.01"
        )
        self.assertEqual(result.returncode, 3)
        self.assertTrue(payload["terminal_verified"])
        self.assertEqual(payload["slurm_classification"], "scheduler_terminal_failure")

    def test_wait_requires_notification_worker_ack(self) -> None:
        command = self.command("wait", "--timeout-seconds", "1")
        command.remove("--notification-worker-ack")
        result = subprocess.run(command, text=True, capture_output=True, check=False, timeout=3)
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 12)
        self.assertIn("does not authenticate", payload["detail"])

    def test_wait_timeout_does_not_change_or_duplicate_monitor(self) -> None:
        counter = self.root / "starts.txt"
        self.run_cli(
            "start",
            env={"FAKE_WATCH_SECONDS": "20", "FAKE_COUNTER": str(counter)},
        )
        before = self.status()
        result, payload = self.run_cli(
            "wait", "--timeout-seconds", "0.1", "--poll-seconds", "0.02"
        )
        after = self.status()
        self.assertEqual(result.returncode, 4)
        self.assertEqual(payload["state"], "wait_timeout")
        self.assertEqual(after["state"], "active")
        self.assertEqual(before["runtime"]["pid"], after["runtime"]["pid"])
        self.assertEqual(len(counter.read_text(encoding="utf-8").splitlines()), 1)

    def test_wait_fails_closed_on_lost_supervisor(self) -> None:
        base = SUPERVISOR.base_dir(self.state, "fakehost", "12345")
        run = base / "runs" / "run_lost"
        run.mkdir(parents=True)
        SUPERVISOR.replace_json(base / "current.json", {"run_id": run.name})
        SUPERVISOR.replace_json(
            run / "supervisor_started.json",
            {"pid": 999999999, "pid_start_ticks": "missing"},
        )
        result, payload = self.run_cli(
            "wait", "--timeout-seconds", "1", "--poll-seconds", "0.01"
        )
        self.assertEqual(result.returncode, 11)
        self.assertEqual(payload["state"], "supervisor_lost")

    def test_wait_rejects_unverified_terminal(self) -> None:
        base = SUPERVISOR.base_dir(self.state, "fakehost", "12345")
        run = base / "runs" / "run_invalid"
        run.mkdir(parents=True)
        SUPERVISOR.replace_json(base / "current.json", {"run_id": run.name})
        SUPERVISOR.replace_json(
            run / "terminal.json",
            {"schema_version": "unexpected", "observer_outcome": "unknown"},
        )
        result, payload = self.run_cli(
            "wait", "--timeout-seconds", "1", "--poll-seconds", "0.01"
        )
        self.assertEqual(result.returncode, 12)
        self.assertEqual(payload["state"], "terminal")
        self.assertFalse(payload["terminal_verified"])
        self.assertIn("terminal_schema_mismatch", payload["problems"])

    def test_unattended_pending_stop_is_disabled_by_default(self) -> None:
        args = SUPERVISOR.parser().parse_args(["start", "12345"])
        self.assertEqual(args.pending_alert_seconds, 0.0)

    def test_exit_event_mismatch_is_not_verified(self) -> None:
        self.run_cli(
            "start",
            env={"FAKE_EXIT": "3", "FAKE_EVENT": "completed"},
        )
        result = self.wait_state("terminal")["terminal"]["watcher_result"]
        self.assertFalse(result["verified"])
        self.assertIn("exit_event_mismatch", result["problems"])
        self.assertIsNone(result["payload"])

    def test_active_status_includes_verified_local_snapshot(self) -> None:
        self.run_cli(
            "start",
            env={"FAKE_WATCH_SECONDS": "20", "FAKE_WRITE_STATE": "1"},
        )
        state = self.status()["watcher_state"]
        self.assertTrue(state["verified"])
        self.assertEqual(state["payload"]["snapshot"]["state"], "RUNNING")

    def test_symlink_watcher_path_is_rejected(self) -> None:
        link = self.root / "watcher-link.py"
        link.symlink_to(self.fake)
        result, payload = self.run_cli("start", "--watcher-path", str(link))
        self.assertEqual(result.returncode, 12)
        self.assertIn("symlink", payload["detail"])

    def test_manifest_watcher_hash_is_enforced(self) -> None:
        manifest = {
            "watcher_argv": [sys.executable, str(self.fake)],
            "watcher_path_sha256": SUPERVISOR.sha256_file(self.fake),
        }
        SUPERVISOR.validate_manifest_watcher(manifest)
        self.fake.write_text(FAKE_WATCHER + "\n# changed\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            SUPERVISOR.validate_manifest_watcher(manifest)

    def test_contract_conflict_requires_explicit_allowance(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "0.1"})
        self.wait_state("terminal")
        # Same host/job with a changed contract is a conflict, not an
        # implicit replacement watcher, even with --restart.
        result, payload = self.run_cli("start", "--restart", "--poll-seconds", "2")
        self.assertEqual(result.returncode, 12)
        self.assertEqual(payload["start_result"], "contract_conflict")
        self.assertNotEqual(
            payload["contract_digest"], payload["previous_contract_digest"]
        )
        allowed, _ = self.run_cli(
            "start", "--restart", "--poll-seconds", "2", "--allow-contract-change"
        )
        self.assertEqual(allowed.returncode, 0)
        terminal = self.wait_state("terminal")["terminal"]
        self.assertTrue(str(terminal["contract_digest"]).startswith("sha256:"))

    def test_same_contract_restart_is_not_a_conflict(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "0.1"})
        self.wait_state("terminal")
        result, _ = self.run_cli("start", "--restart")
        self.assertEqual(result.returncode, 0)

    def test_terminal_evidence_strength_is_full_for_new_runs(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "0.1"})
        payload = self.wait_state("terminal")
        self.assertEqual(payload["evidence_strength"], "full")
        wait_payload = self.run_cli(
            "wait", "--timeout-seconds", "2", "--poll-seconds", "0.01"
        )[1]
        self.assertEqual(wait_payload["evidence_strength"], "full")
        self.assertTrue(wait_payload["terminal_verified"])

    def test_legacy_terminal_without_digest_stays_readable(self) -> None:
        base = SUPERVISOR.base_dir(self.state, "fakehost", "12345")
        run = base / "runs" / "run_1000_1_abcd1234"
        run.mkdir(parents=True)
        SUPERVISOR.replace_json(base / "current.json", {"run_id": run.name})
        SUPERVISOR.replace_json(
            run / "terminal.json",
            {
                "schema_version": "codex-hpc-monitor.terminal/v1",
                "host": "fakehost",
                "job_id": "12345",
                "scope": "slurm_only",
                "project_gate_evaluated": False,
                "observer_outcome": "watcher_exit_zero",
                "watcher_exit_code": 0,
                "watcher_result": {"verified": True, "payload": {}},
            },
        )
        status = self.status()
        self.assertEqual(status["state"], "terminal")
        self.assertEqual(status["evidence_strength"], "legacy")
        wait_payload = self.run_cli(
            "wait", "--timeout-seconds", "2", "--poll-seconds", "0.01"
        )[1]
        self.assertEqual(wait_payload["evidence_strength"], "legacy")
        self.assertEqual(wait_payload["terminal_verified"], True)

    def test_tampered_contract_digest_fails_closed(self) -> None:
        self.run_cli("start", env={"FAKE_WATCH_SECONDS": "0.1"})
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        terminal = json.loads((run / "terminal.json").read_text())
        terminal["contract_digest"] = "sha256:" + "0" * 64
        # terminal.json is immutable in real flows; direct rewrite simulates
        # corruption so verification must reject it.
        (run / "terminal.json").write_text(json.dumps(terminal, sort_keys=True) + "\n")
        result, wait_payload = self.run_cli(
            "wait", "--timeout-seconds", "2", "--poll-seconds", "0.01"
        )
        self.assertEqual(result.returncode, 12)
        self.assertIn("terminal_contract_digest_mismatch", wait_payload["problems"])

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

    def write_fake_codex(self, version: str = "0.150.1") -> Path:
        path = self.root / f"codex-{version}"
        path.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            f"print('codex-cli {version}') if sys.argv[1:] == ['--version'] else None\n",
            encoding="utf-8",
        )
        path.chmod(0o700)
        return path

    def write_bridge_config(
        self, instance: str = "workstation-1", codex_version: str = "0.150.1"
    ) -> Path:
        codex_bin = self.write_fake_codex(codex_version)
        config = {
            "schema": "codex-monitor.bridge-config/v1",
            "enabled": True,
            "instance_id": instance,
            "codex_home": str(self.root / ".codex"),
            "codex_home_id": semantic_events.codex_home_digest(self.root / ".codex"),
            "workspace": str(self.root),
            "transport": {"type": "stdio", "command": [str(codex_bin), "app-server"]},
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

    def outbox(self):
        return semantic_events.outbox_root(self.state)

    def wait_run_file(self, run: Path, name: str, timeout: float = 8.0) -> Path:
        deadline = time.monotonic() + timeout
        path = run / name
        while time.monotonic() < deadline:
            if path.exists():
                return path
            # status observations trigger the reconciler, closing any crash
            # window between terminal and event publication.
            self.run_cli("status")
            time.sleep(0.05)
        self.fail(f"{name} was not published for {run}")

    def test_terminal_publishes_one_semantic_event_with_binding(self) -> None:
        binding = self.write_binding()
        result, _ = self.run_cli(
            "start", "--event-binding", str(binding), env={"FAKE_EXIT": "0"}
        )
        self.assertEqual(result.returncode, 0)
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        published = json.loads(self.wait_run_file(run, "semantic_event.json").read_text())
        self.assertEqual(published["event"], "transport_success")
        # Either the supervisor or a reconciling observer published first.
        self.assertIn(published["state"], {"published", "duplicate"})
        entries = semantic_events.list_outbox(self.outbox())
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["state"], "pending")
        event = semantic_events.read_event(self.outbox(), published["event_id"])
        terminal_sha = hashlib.sha256((run / "terminal.json").read_bytes()).hexdigest()
        self.assertEqual(event["monitor"]["terminal_digest"], f"sha256:{terminal_sha}")
        self.assertEqual(event["monitor"]["handle"], "fakehost-12345")
        self.assertEqual(event["monitor"]["generation"], payload["run_id"])
        self.assertEqual(event["exit_code"], 0)
        self.assertEqual(event["binding"]["thread_id"], "thr_test_1")

    def test_failure_terminal_maps_to_transport_failure_event(self) -> None:
        binding = self.write_binding()
        self.run_cli(
            "start", "--event-binding", str(binding), env={"FAKE_EXIT": "3"}
        )
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        published = json.loads(self.wait_run_file(run, "semantic_event.json").read_text())
        self.assertEqual(published["event"], "transport_failure")
        event = semantic_events.read_event(self.outbox(), published["event_id"])
        self.assertEqual(event["exit_code"], 3)

    def test_no_binding_publishes_no_event(self) -> None:
        self.run_cli("start", env={"FAKE_EXIT": "0"})
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        self.assertFalse((run / "semantic_event.json").exists())
        self.assertEqual(semantic_events.list_outbox(self.outbox()), [])

    def test_signaled_watcher_publishes_no_event(self) -> None:
        binding = self.write_binding()
        self.run_cli(
            "start",
            "--event-binding",
            str(binding),
            env={"FAKE_SELF_SIGNAL": str(int(signal.SIGTERM))},
        )
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        self.assertEqual(payload["terminal"]["observer_outcome"], "watcher_signaled")
        self.assertFalse((run / "semantic_event.json").exists())
        self.assertEqual(semantic_events.list_outbox(self.outbox()), [])

    def test_binding_config_mismatch_fails_closed(self) -> None:
        binding = self.write_binding()
        config = self.write_bridge_config(instance="other-host")
        result, payload = self.run_cli(
            "start", "--event-binding", str(binding), "--bridge-config", str(config)
        )
        self.assertEqual(result.returncode, 12)
        self.assertIn("does not match bridge config", payload["detail"])
        self.assertEqual(semantic_events.list_outbox(self.outbox()), [])

    def test_binding_config_agreement_starts_normally(self) -> None:
        binding = self.write_binding()
        config = self.write_bridge_config()
        result, _ = self.run_cli(
            "start",
            "--event-binding",
            str(binding),
            "--bridge-config",
            str(config),
            env={"FAKE_EXIT": "0"},
        )
        self.assertEqual(result.returncode, 0)
        self.wait_state("terminal")
        self.assertEqual(len(semantic_events.list_outbox(self.outbox())), 1)

    def run_doctor(self, *extra: str) -> tuple[subprocess.CompletedProcess[str], dict | str]:
        command = [
            sys.executable, str(SCRIPT), "doctor",
            "--state-dir", str(self.state), *extra,
        ]
        result = subprocess.run(command, text=True, capture_output=True, check=False, timeout=15)
        if "--format" in extra and extra[extra.index("--format") + 1] == "text":
            return result, result.stdout
        return result, json.loads(result.stdout)

    def test_doctor_defaults_to_unattended_and_formats_agree(self) -> None:
        result, payload = self.run_doctor()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(payload["mode"]["selected"], "unattended")
        self.assertEqual(payload["mode"]["reason"], "bridge_not_configured")
        self.assertFalse(payload["auto_resume_available"])
        self.assertFalse(payload["configuration_ready"])
        self.assertEqual(payload["delivery_daemon_live"], "not_configured")
        self.assertTrue(payload["zero_turns_while_unchanged"])
        self.assertFalse(payload["agent_slot_occupied"])
        self.assertTrue(payload["state_root"]["suitable"])
        self.assertFalse(payload["app_server"]["configured"])
        _, text = self.run_doctor("--format", "text")
        self.assertIn("mode: unattended (requested auto; reason: bridge_not_configured)", text)
        self.assertIn("auto-resume available: no", text)
        self.assertIn("zero model turns while unchanged: yes", text)

    def test_doctor_selects_bridge_with_enabled_config(self) -> None:
        config = self.write_bridge_config()
        result, payload = self.run_doctor("--bridge-config", str(config))
        self.assertEqual(result.returncode, 0)
        self.assertEqual(payload["mode"]["selected"], "unattended")
        self.assertFalse(payload["auto_resume_available"])
        self.assertEqual(payload["app_server"]["real_transport_smoke"], "unverified")
        self.assertEqual(payload["app_server"]["activation"], "missing_or_stale")

        loaded = semantic_events.load_bridge_config(config)
        semantic_events.activate_bridge(self.outbox(), loaded, [])
        identity = app_server_bridge._configured_cli_identity(loaded, timeout_seconds=5)
        app_server_bridge.record_lifecycle_smoke_receipt(
            self.state,
            loaded,
            identity,
            thread_id="thr_doctor",
            first_turn_id="turn_doctor_1",
            second_turn_id="turn_doctor_2",
        )
        result, payload = self.run_doctor("--bridge-config", str(config))
        self.assertEqual(payload["mode"]["selected"], "external-event-bridge")
        self.assertTrue(payload["auto_resume_available"])
        self.assertTrue(payload["notification_available"])
        self.assertTrue(payload["configuration_ready"])
        self.assertEqual(payload["delivery_daemon_live"], "unknown_not_probed")
        self.assertEqual(payload["app_server"]["real_transport_smoke"], "passed")
        self.assertEqual(payload["app_server"]["activation"], "ready")
        _, text = self.run_doctor("--bridge-config", str(config), "--format", "text")
        self.assertIn("mode: external-event-bridge", text)
        self.assertIn("auto-resume available: yes", text)
        self.assertIn("delivery daemon live: unknown_not_probed", text)

    def test_doctor_invalid_or_disabled_config_falls_back_to_unattended(self) -> None:
        bad = self.root / "bad.json"
        bad.write_text(json.dumps({"schema": "wrong"}), encoding="utf-8")
        bad.chmod(0o600)
        _, payload = self.run_doctor("--bridge-config", str(bad))
        self.assertEqual(payload["mode"]["selected"], "unattended")
        self.assertTrue(payload["mode"]["reason"].startswith("bridge_config_invalid"))
        disabled = self.write_bridge_config()
        config = json.loads(disabled.read_text())
        config["enabled"] = False
        disabled.write_text(json.dumps(config))
        _, payload = self.run_doctor("--bridge-config", str(disabled))
        self.assertEqual(payload["mode"]["selected"], "unattended")
        self.assertEqual(payload["mode"]["reason"], "bridge_disabled")

    def test_doctor_explicit_bridge_without_healthy_config_falls_back(self) -> None:
        _, payload = self.run_doctor("--mode", "external-event-bridge")
        self.assertEqual(payload["mode"]["selected"], "unattended")
        self.assertEqual(payload["mode"]["requested"], "external-event-bridge")

    def test_list_explain_and_cleanup(self) -> None:
        self.run_cli("start", env={"FAKE_EXIT": "0"})
        terminal = self.wait_state("terminal")
        listing = subprocess.run(
            [sys.executable, str(SCRIPT), "list", "--state-dir", str(self.state)],
            text=True, capture_output=True, check=False, timeout=15,
        )
        entries = json.loads(listing.stdout)["monitors"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["job_id"], "12345")
        self.assertEqual(entries[0]["state"], "terminal")
        self.assertEqual(entries[0]["evidence_strength"], "full")
        explain = subprocess.run(
            [sys.executable, str(SCRIPT), "explain", "12345", "--host", "fakehost",
             "--state-dir", str(self.state)],
            text=True, capture_output=True, check=False, timeout=15,
        )
        self.assertIn("state=terminal", explain.stdout)
        self.assertIn("cannot wake", explain.stdout)
        self.assertIn("wake events enabled: no", explain.stdout)
        # cleanup: settle one outbox event in the past, then dry-run and apply
        binding = json.loads(self.write_binding().read_text())
        event = semantic_events.build_event(
            backend="slurm", handle="fakehost-12345",
            generation=terminal["run_id"],
            terminal_digest="sha256:" + "c" * 64,
            event="transport_success", exit_code=0, binding=binding,
        )
        semantic_events.publish_event(self.outbox(), event)
        settle = datetime.now(timezone.utc)
        semantic_events.claim_next_event(self.outbox(), owner="t", lease_seconds=600, now=settle)
        semantic_events.acknowledge_event(
            self.outbox(), event["event_id"], owner="t", now=settle,
            thread_id="thr", turn_id="turn", turn_status="completed",
        )
        dry = subprocess.run(
            [sys.executable, str(SCRIPT), "cleanup", "--state-dir", str(self.state),
             "--older-than-days", "0"],
            text=True, capture_output=True, check=False, timeout=15,
        )
        dry_payload = json.loads(dry.stdout)
        self.assertEqual(dry_payload["mode"], "dry_run")
        self.assertEqual(dry_payload["removed_count"], 1)
        self.assertEqual(len(semantic_events.list_outbox(self.outbox())), 1)
        applied = subprocess.run(
            [sys.executable, str(SCRIPT), "cleanup", "--state-dir", str(self.state),
             "--older-than-days", "0", "--yes"],
            text=True, capture_output=True, check=False, timeout=15,
        )
        applied_payload = json.loads(applied.stdout)
        self.assertEqual(applied_payload["mode"], "applied")
        self.assertEqual(applied_payload["removed_count"], 1)
        self.assertEqual(semantic_events.list_outbox(self.outbox()), [])

    def test_full_chain_start_to_wake_and_idempotent_postflight(self) -> None:
        fake_server = self.root / "fake_app_server.py"
        fake_server.write_text(FAKE_APP_SERVER_OK, encoding="utf-8")
        fake_server.chmod(0o700)
        wake_log = self.root / "wake.log"
        config = json.loads(self.write_bridge_config().read_text())
        config["transport"]["command"] = [str(fake_server), "app-server"]
        config_path = self.write_bridge_config()
        config_path.write_text(json.dumps(config))
        config_path.chmod(0o600)
        binding_path = self.write_binding()
        loaded_config = semantic_events.load_bridge_config(config_path)
        semantic_events.activate_bridge(self.outbox(), loaded_config, [])
        identity = app_server_bridge._configured_cli_identity(
            loaded_config, timeout_seconds=5
        )
        app_server_bridge.record_lifecycle_smoke_receipt(
            self.state, loaded_config, identity,
            thread_id="thr_chain",
            first_turn_id="turn_chain_smoke_1",
            second_turn_id="turn_chain_smoke_2",
        )

        result, _ = self.run_cli(
            "start",
            "--event-binding", str(binding_path),
            "--bridge-config", str(config_path),
            env={"FAKE_EXIT": "0"},
        )
        self.assertEqual(result.returncode, 0)
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        published = json.loads(self.wait_run_file(run, "semantic_event.json").read_text())
        event_id = published["event_id"]

        delivered = subprocess.run(
            [sys.executable, str(HERE / "app_server_bridge.py"), "deliver",
             "--state-dir", str(self.state), "--bridge-config", str(config_path),
             "--once"],
            text=True, capture_output=True, check=False,
            env={
                **os.environ,
                "FAKE_LOG": str(wake_log),
                "FAKE_THREAD_CWD": str(self.root),
            },
            timeout=30,
        )
        self.assertEqual(delivered.returncode, 0, delivered.stdout + delivered.stderr)
        records = [json.loads(line) for line in delivered.stdout.splitlines() if line.strip()]
        self.assertEqual(records[0]["state"], "acknowledged", records)
        self.assertEqual(records[0]["turn_status"], "completed")
        wake = wake_log.read_text()
        self.assertIn(f"event_id={event_id}", wake)
        self.assertIn("backend=slurm", wake)
        self.assertEqual(wake.count("==="), 1)

        guard = str(HERE / "postflight_guard.py")
        check = subprocess.run(
            [sys.executable, guard, "check", event_id, "--state-dir", str(self.state)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(check.returncode, 0)
        terminal_digest = "sha256:" + hashlib.sha256(
            (run / "terminal.json").read_bytes()
        ).hexdigest()
        mark = subprocess.run(
            [sys.executable, guard, "mark", event_id,
             "--terminal-digest", terminal_digest, "--state-dir", str(self.state)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(mark.returncode, 0)
        duplicate = subprocess.run(
            [sys.executable, guard, "mark", event_id,
             "--terminal-digest", terminal_digest, "--state-dir", str(self.state)],
            text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(duplicate.returncode, 3)

    def test_unverified_result_never_publishes_success(self) -> None:
        binding = self.write_binding()
        # Watcher exits 0 but leaves no result file: the terminal exists yet
        # its watcher result is unverified. A success event must never be
        # published; the observation problem is surfaced as contract_violation.
        result, _ = self.run_cli(
            "start",
            "--event-binding", str(binding),
            env={"FAKE_EXIT": "0", "FAKE_WRITE_RESULT": "0"},
        )
        self.assertEqual(result.returncode, 0)
        payload = self.wait_state("terminal")
        self.assertFalse(payload["terminal"]["watcher_result"]["verified"])
        run = Path(payload["run_dir"])
        published = json.loads(self.wait_run_file(run, "semantic_event.json").read_text())
        self.assertEqual(published["event"], "contract_violation")
        event = semantic_events.read_event(self.outbox(), published["event_id"])
        self.assertEqual(event["event"], "contract_violation")
        self.assertEqual(event["exit_code"], 0)

    def test_pending_alert_publishes_no_semantic_event(self) -> None:
        binding = self.write_binding()
        result, _ = self.run_cli(
            "start",
            "--event-binding", str(binding),
            env={"FAKE_EXIT": "4", "FAKE_EVENT": "pending_alert"},
        )
        self.assertEqual(result.returncode, 0)
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            self.run_cli("status")
            time.sleep(0.05)
        # A queue-wait alert is not a monitoring deadline: no wake event.
        self.assertFalse((run / "semantic_event.json").exists())
        self.assertEqual(semantic_events.list_outbox(self.outbox()), [])

    def test_event_intent_is_written_at_start(self) -> None:
        binding = self.write_binding()
        self.run_cli("start", "--event-binding", str(binding),
                     env={"FAKE_WATCH_SECONDS": "20"})
        payload = self.status()
        run = Path(payload["run_dir"])
        intent = json.loads((run / "event_intent.json").read_text())
        self.assertEqual(intent["event_backend"], "slurm")
        self.assertEqual(intent["binding_instance"], "workstation-1")
        self.assertEqual(intent["job_id"], "12345")

    def test_crash_window_between_terminal_and_event_is_reconciled(self) -> None:
        binding = self.write_binding()
        self.run_cli("start", "--event-binding", str(binding), env={"FAKE_EXIT": "0"})
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        published = json.loads(self.wait_run_file(run, "semantic_event.json").read_text())
        event_id = published["event_id"]
        # Simulate the supervisor dying between terminal and event publish:
        # remove the event markers and the outbox copy.
        (run / "semantic_event.json").unlink()
        import shutil as _shutil
        _shutil.rmtree(semantic_events.event_dir(self.outbox(), event_id))
        self.assertEqual(semantic_events.list_outbox(self.outbox()), [])
        # A later status observation reconciles the lost publication.
        self.run_cli("status")
        repaired = json.loads((run / "semantic_event.json").read_text())
        self.assertEqual(repaired["state"], "published")
        entries = semantic_events.list_outbox(self.outbox())
        self.assertEqual(len(entries), 1)

    def test_transient_event_publish_failure_is_reconciled_later(self) -> None:
        binding = self.write_binding()
        self.run_cli("start", "--event-binding", str(binding), env={"FAKE_EXIT": "0"})
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        published = json.loads(self.wait_run_file(run, "semantic_event.json").read_text())
        (run / "semantic_event.json").unlink()
        import shutil as _shutil
        _shutil.rmtree(semantic_events.event_dir(self.outbox(), published["event_id"]))
        manifest = SUPERVISOR.read_json(run / "manifest.json")
        terminal = SUPERVISOR.read_json(run / "terminal.json")
        with mock.patch.object(semantic_events, "publish_event", side_effect=OSError("temporary")):
            SUPERVISOR.publish_semantic_event(
                run,
                manifest=manifest,
                terminal=terminal,
                watcher_exit_code=terminal["watcher_exit_code"],
            )
        failure = json.loads((run / "semantic_event_failure.json").read_text())
        self.assertTrue(failure["retryable"])
        repaired = SUPERVISOR.reconcile_run_event(run)
        self.assertIn(repaired, {"published", "duplicate"})
        self.assertTrue((run / "semantic_event.json").exists())
        self.assertFalse((run / "semantic_event_failure.json").exists())

    def test_tampered_terminal_cannot_satisfy_status_or_reconcile_event(self) -> None:
        binding = self.write_binding()
        self.run_cli("start", "--event-binding", str(binding), env={"FAKE_EXIT": "0"})
        payload = self.wait_state("terminal")
        run = Path(payload["run_dir"])
        published = json.loads(self.wait_run_file(run, "semantic_event.json").read_text())
        (run / "semantic_event.json").unlink()
        import shutil as _shutil
        _shutil.rmtree(semantic_events.event_dir(self.outbox(), published["event_id"]))
        terminal = json.loads((run / "terminal.json").read_text())
        terminal["host"] = "wrong-host"
        (run / "terminal.json").write_text(json.dumps(terminal) + "\n")
        result, status = self.run_cli("status", "--require-terminal")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(status["terminal_verified"])
        self.assertEqual(semantic_events.list_outbox(self.outbox()), [])


if __name__ == "__main__":
    unittest.main()

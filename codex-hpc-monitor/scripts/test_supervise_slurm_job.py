#!/usr/bin/env python3
"""Integration and artifact tests for supervise_slurm_job.py."""

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


HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "supervise_slurm_job.py"
SPEC = importlib.util.spec_from_file_location("supervise_slurm_job", SCRIPT)
assert SPEC and SPEC.loader
SUPERVISOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUPERVISOR)


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


if __name__ == "__main__":
    unittest.main()

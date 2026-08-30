#!/usr/bin/env python3
"""Tests for the compact Codex dispatch monitor wrapper."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "monitor_dispatch.py"
REAL_SUPERVISOR = HERE / "supervise_artifact.py"


class DispatchMonitorWrapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.dispatch = self.root / "dispatch"
        self.dispatch.mkdir()
        self.manifest = self.dispatch / "dispatch_manifest.json"
        self.manifest.write_text(
            json.dumps({"schema_version": "codex-task-dispatch.manifest/v3", "dispatch_handle": "dsp_wrapper_test"}),
            encoding="utf-8",
        )
        self.capture = self.root / "argv.json"
        self.helper = self.root / "fake_supervisor.py"
        self.dispatch_verifier = self.root / "fake_dispatch_supervisor.py"
        self.write_helper(return_code=0, state="active")
        self.write_dispatch_verifier(verified=False)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_helper(self, *, return_code: int, state: str) -> None:
        payload = {
            "task_handle": "artifact_" + "a" * 32,
            "state": state,
            "start_result": "started",
            "supervisor_alive": True,
            "watcher_alive": True,
            "run_dir": "/private/run",
        }
        self.helper.write_text(
            "#!/usr/bin/env python3\n"
            "import json,sys\n"
            f"open({str(self.capture)!r},'w').write(json.dumps(sys.argv[1:]))\n"
            f"print({json.dumps(payload)!r})\n"
            f"sys.exit({return_code})\n",
            encoding="utf-8",
        )

    def write_dispatch_verifier(self, *, verified: bool) -> None:
        terminal = self.dispatch / "dispatch_terminal.json"
        terminal_sha = hashlib.sha256(terminal.read_bytes()).hexdigest() if terminal.exists() else "0" * 64
        transport = {"outcome": "exit_zero"}
        payload = {
            "dispatch_handle": "dsp_wrapper_test",
            "terminal_present": terminal.exists(),
            "terminal_sha256": terminal_sha,
            "transport": transport,
            "verified": verified,
        }
        self.dispatch_verifier.write_text(
            "#!/usr/bin/env python3\n"
            "import json,sys\n"
            f"print({json.dumps(payload)!r})\n"
            f"sys.exit({0 if verified else 4})\n",
            encoding="utf-8",
        )

    def run_wrapper(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--supervisor-path",
                str(self.helper),
                "--dispatch-supervisor-path",
                str(self.dispatch_verifier),
                *args,
            ],
            text=True,
            capture_output=True,
            check=False,
            timeout=5,
        )

    def test_start_freezes_complete_dispatch_terminal_contract(self) -> None:
        result = self.run_wrapper(
            "start",
            str(self.dispatch),
            "--state-dir",
            str(self.root / "state"),
            "--timeout-seconds",
            "60",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        compact = json.loads(result.stdout)
        argv = json.loads(self.capture.read_text(encoding="utf-8"))
        digest = hashlib.sha256(self.manifest.read_bytes()).hexdigest()
        self.assertEqual(compact["state"], "active")
        self.assertEqual(compact["dispatch_handle"], "dsp_wrapper_test")
        self.assertNotIn("/private/run", json.dumps(compact))
        self.assertIn("transport.outcome", argv)
        self.assertIn('dispatch_handle="dsp_wrapper_test"', argv)
        self.assertIn(f'manifest_sha256="{digest}"', argv)
        self.assertIn('schema_version="codex-task-dispatch.terminal/v3"', argv)
        self.assertIn('business_verdict="pending"', argv)
        for outcome in ("exit_nonzero", "signaled", "not_started", "contract_violation", "unknown"):
            self.assertIn(json.dumps(outcome), argv)

    def test_binding_without_config_warning_survives_compact_wrapper(self) -> None:
        result = self.run_wrapper(
            "start", str(self.dispatch), "--state-dir", str(self.root / "state"),
            "--timeout-seconds", "60", "--event-binding", str(self.root / "binding.json"),
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("*** WARNING [event_binding_without_bridge_config]", result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["warnings"][0]["code"], "event_binding_without_bridge_config")

    def test_require_auto_resume_requires_both_inputs_and_is_forwarded(self) -> None:
        refused = self.run_wrapper(
            "start", str(self.dispatch), "--state-dir", str(self.root / "state"),
            "--timeout-seconds", "60", "--event-binding", str(self.root / "binding.json"),
            "--require-auto-resume",
        )
        self.assertEqual(refused.returncode, 12)
        self.assertEqual(json.loads(refused.stdout)["reason"], "auto_resume_requirements_not_met")

        accepted = self.run_wrapper(
            "start", str(self.dispatch), "--state-dir", str(self.root / "state"),
            "--timeout-seconds", "60", "--event-binding", str(self.root / "binding.json"),
            "--bridge-config", str(self.root / "bridge.json"),
            "--require-auto-resume",
        )
        self.assertEqual(accepted.returncode, 0, accepted.stdout)
        argv = json.loads(self.capture.read_text(encoding="utf-8"))
        self.assertIn("--require-auto-resume", argv)

    def test_status_is_compact_and_preserves_exit_code(self) -> None:
        started = self.run_wrapper(
            "start", str(self.dispatch), "--state-dir", str(self.root / "state"), "--timeout-seconds", "60"
        )
        self.assertEqual(started.returncode, 0)
        self.write_helper(return_code=10, state="active")
        result = self.run_wrapper(
            "status", "artifact_" + "a" * 32, "--state-dir", str(self.root / "state")
        )
        self.assertEqual(result.returncode, 10)
        compact = json.loads(result.stdout)
        self.assertEqual(compact["monitor_task_handle"], "artifact_" + "a" * 32)
        self.assertNotIn("run_dir", compact)

    def test_wait_requires_notification_worker_ack_before_binding_access(self) -> None:
        result = self.run_wrapper(
            "wait",
            "artifact_" + "a" * 32,
            "--state-dir",
            str(self.root / "missing-state"),
            "--timeout-seconds",
            "1",
        )
        self.assertEqual(result.returncode, 12)
        self.assertEqual(json.loads(result.stdout)["state"], "wrapper_error")

    def test_invalid_manifest_fails_before_start(self) -> None:
        self.manifest.write_text("{}", encoding="utf-8")
        result = self.run_wrapper("start", str(self.dispatch), "--timeout-seconds", "60")
        self.assertEqual(result.returncode, 12)
        self.assertEqual(json.loads(result.stdout)["state"], "wrapper_error")

    def test_real_supervisor_reaches_compact_terminal(self) -> None:
        manifest_sha = hashlib.sha256(self.manifest.read_bytes()).hexdigest()
        (self.dispatch / "dispatch_terminal.json").write_text(
            json.dumps(
                {
                    "schema_version": "codex-task-dispatch.terminal/v3",
                    "dispatch_handle": "dsp_wrapper_test",
                    "manifest_sha256": manifest_sha,
                    "transport": {"outcome": "exit_zero"},
                    "business_verdict": "pending",
                }
            ),
            encoding="utf-8",
        )
        self.write_dispatch_verifier(verified=True)
        state = self.root / "monitor-state"
        started = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--supervisor-path",
                str(REAL_SUPERVISOR),
                "--dispatch-supervisor-path",
                str(self.dispatch_verifier),
                "start",
                str(self.dispatch),
                "--state-dir",
                str(state),
                "--poll-seconds",
                "0.05",
                "--timeout-seconds",
                "5",
            ],
            text=True,
            capture_output=True,
            check=False,
            timeout=8,
        )
        self.assertEqual(started.returncode, 0, started.stderr)
        monitor_handle = json.loads(started.stdout)["monitor_task_handle"]
        deadline = time.monotonic() + 5
        while True:
            status = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--supervisor-path",
                    str(REAL_SUPERVISOR),
                    "--dispatch-supervisor-path",
                    str(self.dispatch_verifier),
                    "status",
                    monitor_handle,
                    "--state-dir",
                    str(state),
                    "--require-terminal",
                ],
                text=True,
                capture_output=True,
                check=False,
                timeout=5,
            )
            compact = json.loads(status.stdout)
            if status.returncode != 10:
                break
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.05)
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertEqual(compact["observer_outcome"], "condition_satisfied")
        self.assertEqual(compact["dispatch_transport_outcome"], "exit_zero")
        self.assertEqual(compact["dispatch_terminal_sha256"], hashlib.sha256((self.dispatch / "dispatch_terminal.json").read_bytes()).hexdigest())
        self.assertNotIn("run_dir", compact)

    def test_incomplete_terminal_is_rejected_after_generic_observation(self) -> None:
        manifest_sha = hashlib.sha256(self.manifest.read_bytes()).hexdigest()
        (self.dispatch / "dispatch_terminal.json").write_text(
            json.dumps(
                {
                    "schema_version": "codex-task-dispatch.terminal/v3",
                    "dispatch_handle": "dsp_wrapper_test",
                    "manifest_sha256": manifest_sha,
                    "transport": {"outcome": "exit_zero"},
                    "business_verdict": "pending",
                }
            ),
            encoding="utf-8",
        )
        self.write_dispatch_verifier(verified=False)
        state = self.root / "rejected-state"
        started = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--supervisor-path",
                str(REAL_SUPERVISOR),
                "--dispatch-supervisor-path",
                str(self.dispatch_verifier),
                "start",
                str(self.dispatch),
                "--state-dir",
                str(state),
                "--poll-seconds",
                "0.05",
                "--timeout-seconds",
                "5",
            ],
            text=True,
            capture_output=True,
            check=False,
            timeout=8,
        )
        self.assertEqual(started.returncode, 0, started.stderr)
        monitor_handle = json.loads(started.stdout)["monitor_task_handle"]
        deadline = time.monotonic() + 5
        while True:
            status = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--supervisor-path",
                    str(REAL_SUPERVISOR),
                    "--dispatch-supervisor-path",
                    str(self.dispatch_verifier),
                    "status",
                    monitor_handle,
                    "--state-dir",
                    str(state),
                    "--require-terminal",
                ],
                text=True,
                capture_output=True,
                check=False,
                timeout=5,
            )
            if status.returncode != 10:
                break
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.05)
        self.assertEqual(status.returncode, 12)
        self.assertEqual(json.loads(status.stdout)["state"], "dispatch_verification_failed")


if __name__ == "__main__":
    unittest.main()

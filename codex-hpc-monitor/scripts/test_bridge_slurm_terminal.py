from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
BRIDGE = HERE / "bridge_slurm_terminal.py"

FAKE_SUPERVISOR = r'''#!/usr/bin/env python3
import json, os, sys, time
command, job_id = sys.argv[1:3]
host = sys.argv[sys.argv.index("--host") + 1]
run_id = os.environ.get("FAKE_RUN_ID", "run_test")
if command == "status":
    print(json.dumps({"schema_version":"codex-hpc-monitor.status/v1","state":"active","host":host,"job_id":job_id,"run_id":run_id}))
    raise SystemExit(0)
if command == "wait":
    counter = os.environ.get("FAKE_COUNTER")
    if counter:
        with open(counter, "a") as stream: stream.write("wait\n")
    time.sleep(float(os.environ.get("FAKE_DELAY", "0.02")))
    code = int(os.environ.get("FAKE_EXIT", "0"))
    print(json.dumps({"schema_version":"codex-hpc-monitor.wait/v1","state":"terminal","host":host,"job_id":job_id,"run_id":run_id,"terminal_verified":True}))
    raise SystemExit(code)
raise SystemExit(12)
'''


class BridgeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.fake = self.root / "fake_supervisor.py"
        self.fake.write_text(FAKE_SUPERVISOR, encoding="utf-8")
        self.fake.chmod(0o700)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def command(self, action: str = "run") -> list[str]:
        command = [sys.executable, str(BRIDGE), action, "12345", "--host", "fakehost", "--state-dir", str(self.state), "--supervisor-path", str(self.fake)]
        if action == "run":
            command += [
                "--timeout-seconds", "3",
                "--poll-seconds", "0.01",
                "--notification-worker-ack",
            ]
        return command

    def run_bridge(self, env: dict[str, str] | None = None) -> tuple[subprocess.CompletedProcess[str], dict]:
        effective = os.environ.copy()
        if env:
            effective.update(env)
        result = subprocess.run(self.command(), text=True, capture_output=True, check=False, env=effective, timeout=6)
        return result, json.loads(result.stdout.strip().splitlines()[-1])

    def test_success_publishes_receipt_and_status(self) -> None:
        result, receipt = self.run_bridge()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(receipt["state"], "terminal")
        self.assertEqual(receipt["wait_exit_code"], 0)
        manifest = json.loads(
            (self.state / "bridges" / "fakehost-12345" / "run_test" / "manifest.json").read_text()
        )
        self.assertIs(manifest["notification_worker_acknowledged"], True)
        status = subprocess.run(self.command("status"), text=True, capture_output=True, check=False, timeout=3)
        payload = json.loads(status.stdout)
        self.assertEqual(payload["state"], "terminal")
        self.assertEqual(payload["receipt"]["wait_exit_code"], 0)

    def test_run_requires_notification_worker_ack_before_state_access(self) -> None:
        command = [arg for arg in self.command() if arg != "--notification-worker-ack"]
        result = subprocess.run(command, text=True, capture_output=True, check=False, timeout=3)
        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 12)
        self.assertEqual(payload["state"], "bridge_error")
        self.assertIn("does not authenticate", payload["detail"])
        self.assertFalse(self.state.exists())

    def test_duplicate_bridge_has_one_wait_call(self) -> None:
        counter = self.root / "counter"
        env = os.environ.copy()
        env.update({"FAKE_DELAY": "1", "FAKE_COUNTER": str(counter)})
        first = subprocess.Popen(self.command(), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not counter.exists():
            time.sleep(0.02)
        second = subprocess.run(self.command(), text=True, capture_output=True, check=False, env=env, timeout=3)
        self.assertEqual(second.returncode, 2)
        self.assertEqual(json.loads(second.stdout)["run_result"], "already_active")
        first.communicate(timeout=4)
        self.assertEqual(counter.read_text().splitlines(), ["wait"])

    def test_existing_receipt_is_not_replayed_as_success(self) -> None:
        self.assertEqual(self.run_bridge()[0].returncode, 0)
        result, payload = self.run_bridge()
        self.assertEqual(result.returncode, 3)
        self.assertEqual(payload["run_result"], "receipt_exists")

    def test_wait_failure_is_preserved(self) -> None:
        result, receipt = self.run_bridge({"FAKE_EXIT": "3"})
        self.assertEqual(result.returncode, 3)
        self.assertEqual(receipt["wait_exit_code"], 3)
        self.assertEqual(receipt["problems"], [])


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3

import contextlib
import importlib.util
import io
import json
import os
import pathlib
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock


MODULE_PATH = pathlib.Path(__file__).with_name("watch_slurm_job.py")
SPEC = importlib.util.spec_from_file_location("watch_slurm_job", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeClient:
    def __init__(self, results):
        self.results = iter(results)

    def query(self, job_id):
        result = next(self.results)
        if isinstance(result, Exception):
            raise result
        return result


class FakeClock:
    def __init__(self, start=0.0):
        self.now = start

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def snapshot(
    state,
    exit_code="",
    reason="",
    *,
    owner="tester",
    submit_time="",
    job_name="train-job",
    partition="gpu",
):
    return MODULE.Snapshot(
        job_id="123",
        state=state,
        exit_code=exit_code,
        elapsed="00:01:00",
        reason=reason,
        owner=owner,
        submit_time=submit_time,
        job_name=job_name,
        partition=partition,
        source="fake",
    )


class MonitorTests(unittest.TestCase):
    def run_monitor(self, results, **overrides):
        clock = overrides.pop("test_clock", FakeClock())
        output = io.StringIO()
        kwargs = {
            "client": FakeClient(results),
            "job_id": "123",
            "poll_seconds": 10,
            "pending_alert_seconds": 30,
            "query_failures": 3,
            "notify_running": False,
            "host": "hpc142",
            "terminal_observability_seconds": 30,
            "max_watch_seconds": 1000,
            "clock": clock,
            "wall_clock": clock,
            "sleep": clock.sleep,
        }
        kwargs.update(overrides)
        with contextlib.redirect_stdout(output):
            code = MODULE.monitor(**kwargs)
        return code, json.loads(output.getvalue())

    def test_completed_zero_exit_is_success(self):
        code, event = self.run_monitor([snapshot("COMPLETED", "0:0")])
        self.assertEqual(code, 0)
        self.assertEqual(event["event"], "completed")
        self.assertEqual(event["scope"], "slurm_only")
        self.assertEqual(event["slurm_classification"], "scheduler_success")
        self.assertFalse(event["project_gate_evaluated"])

    def test_completed_without_exit_waits_for_explicit_zero(self):
        code, event = self.run_monitor(
            [snapshot("COMPLETED"), snapshot("COMPLETED", "0:0")]
        )
        self.assertEqual(code, 0)
        self.assertEqual(event["exit_code"], "0:0")

    def test_completed_without_exit_loses_observability(self):
        completed = snapshot("COMPLETED")
        code, event = self.run_monitor([completed, completed, completed, completed])
        self.assertEqual(code, 8)
        self.assertEqual(event["event"], "lost_observability")

    def test_completed_nonzero_exit_is_failure(self):
        code, event = self.run_monitor([snapshot("COMPLETED", "1:0")])
        self.assertEqual(code, 3)
        self.assertEqual(event["event"], "terminal_failure")

    def test_failed_is_terminal_failure(self):
        code, event = self.run_monitor([snapshot("FAILED", "1:0")])
        self.assertEqual(code, 3)
        self.assertEqual(event["state"], "FAILED")
        self.assertEqual(event["slurm_classification"], "scheduler_terminal_failure")
        self.assertFalse(event["project_gate_evaluated"])

    def test_business_like_exit_remains_scheduler_only(self):
        code, event = self.run_monitor([snapshot("FAILED", "3:0")])
        self.assertEqual(code, 3)
        self.assertEqual(event["event"], "terminal_failure")
        self.assertEqual(event["exit_code"], "3:0")
        self.assertEqual(event["scope"], "slurm_only")
        self.assertEqual(event["slurm_classification"], "scheduler_terminal_failure")
        self.assertFalse(event["project_gate_evaluated"])

    def test_running_notification(self):
        code, event = self.run_monitor(
            [snapshot("RUNNING")], notify_running=True
        )
        self.assertEqual(code, 6)
        self.assertEqual(event["event"], "running")

    def test_pending_alert_from_first_observation(self):
        pending = snapshot("PENDING", reason="Resources")
        code, event = self.run_monitor([pending, pending, pending, pending])
        self.assertEqual(code, 4)
        self.assertEqual(event["reason"], "Resources")

    def test_pending_alert_uses_slurm_submit_time(self):
        clock = FakeClock(start=1000.0)
        submit_time = datetime.fromtimestamp(900, timezone.utc).isoformat()
        pending = snapshot("PENDING", reason="Priority", submit_time=submit_time)
        code, event = self.run_monitor(
            [pending], test_clock=clock, pending_alert_seconds=30
        )
        self.assertEqual(code, 4)
        self.assertEqual(event["event"], "pending_alert")

    def test_requeue_is_anomalous(self):
        code, event = self.run_monitor([snapshot("REQUEUED")])
        self.assertEqual(code, 7)
        self.assertEqual(event["event"], "anomalous_state")

    def test_transient_query_failures_recover(self):
        code, event = self.run_monitor(
            [RuntimeError("temporary"), None, snapshot("COMPLETED", "0:0")]
        )
        self.assertEqual(code, 0)
        self.assertEqual(event["event"], "completed")

    def test_repeated_query_failures_stop(self):
        failures = [RuntimeError("offline")] * 3
        code, event = self.run_monitor(failures)
        self.assertEqual(code, 5)
        self.assertEqual(event["event"], "query_error")

    def test_total_watch_timeout(self):
        running = snapshot("RUNNING")
        code, event = self.run_monitor(
            [running, running], max_watch_seconds=20
        )
        self.assertEqual(code, 10)
        self.assertEqual(event["event"], "watch_timeout")

    def test_identity_mismatch_stops(self):
        code, event = self.run_monitor(
            [snapshot("RUNNING", owner="someone-else")],
            expected_owner="tester",
        )
        self.assertEqual(code, 9)
        self.assertEqual(event["event"], "identity_mismatch")
        self.assertIn("owner mismatch", event["detail"])

    def test_state_and_result_are_persisted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = pathlib.Path(temp_dir) / "state.json"
            result_path = pathlib.Path(temp_dir) / "result.json"
            code, event = self.run_monitor(
                [snapshot("COMPLETED", "0:0")],
                state_path=state_path,
                result_path=result_path,
            )
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(state_path.read_text())["event"], "completed")
            self.assertEqual(json.loads(result_path.read_text()), event)
            self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(result_path.stat().st_mode & 0o777, 0o600)

    def test_persisted_monitor_start_controls_timeout_after_restart(self):
        clock = FakeClock(start=100.0)
        code, event = self.run_monitor(
            [],
            test_clock=clock,
            max_watch_seconds=50,
            initial_state={"monitor_started_epoch": 0.0},
        )
        self.assertEqual(code, 10)
        self.assertEqual(event["event"], "watch_timeout")

    def test_normalize_decorated_state(self):
        self.assertEqual(MODULE.normalize_state("FAILED+"), "FAILED")
        self.assertEqual(MODULE.normalize_state("CANCELLED by 42"), "CANCELLED")

    def test_squeue_missing_job_can_fall_through_to_sacct(self):
        result = SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="slurm_load_jobs error: Invalid job id specified\n",
        )
        client = MODULE.SlurmClient("hpc142")
        with mock.patch.object(MODULE.subprocess, "run", return_value=result):
            self.assertEqual(client._ssh("squeue ...", missing_job_ok=True), "")

    def test_other_ssh_errors_remain_fail_closed(self):
        result = SimpleNamespace(returncode=255, stdout="", stderr="network down\n")
        client = MODULE.SlurmClient("hpc142")
        with mock.patch.object(MODULE.subprocess, "run", return_value=result):
            with self.assertRaises(RuntimeError):
                client._ssh("squeue ...", missing_job_ok=True)

    def test_squeue_snapshot_includes_identity_and_submit_time(self):
        result = SimpleNamespace(
            returncode=0,
            stdout="123|RUNNING|00:01|node1|tester|2026-08-15T10:00:00|job-x|gpu\n",
            stderr="",
        )
        client = MODULE.SlurmClient("hpc142")
        with mock.patch.object(MODULE.subprocess, "run", return_value=result):
            item = client.query("123")
        self.assertIsNotNone(item)
        self.assertEqual(item.owner, "tester")
        self.assertEqual(item.job_name, "job-x")
        self.assertEqual(item.partition, "gpu")
        self.assertEqual(item.submit_time, "2026-08-15T10:00:00")


class LockTests(unittest.TestCase):
    def make_lock(self, path):
        return MODULE.WatcherLock(
            path,
            host="hpc142",
            job_id="123",
            command=["python3", "watch_slurm_job.py", "123"],
        )

    def test_duplicate_live_watcher_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "watcher.lock.json"
            first = self.make_lock(path)
            first.acquire()
            try:
                with self.assertRaises(MODULE.DuplicateWatcherError):
                    self.make_lock(path).acquire()
            finally:
                first.release()

    def test_stale_lock_is_replaced(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "watcher.lock.json"
            path.write_text(
                json.dumps({"pid": 99999999, "pid_start_ticks": "0"}),
                encoding="utf-8",
            )
            lock = self.make_lock(path)
            lock.acquire()
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(state["pid"], os.getpid())
            finally:
                lock.release()
            released = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(released["pid"], os.getpid())
            self.assertIn("released_at", released)

    def test_new_watcher_phase_clears_previous_result(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result = pathlib.Path(temp_dir) / "result.json"
            result.write_text('{"event":"running"}\n', encoding="utf-8")
            MODULE.clear_previous_result(result)
            self.assertFalse(result.exists())
            MODULE.clear_previous_result(result)


if __name__ == "__main__":
    unittest.main()

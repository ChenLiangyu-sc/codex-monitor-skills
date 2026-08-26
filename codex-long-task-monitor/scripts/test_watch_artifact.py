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
from unittest import mock


MODULE_PATH = pathlib.Path(__file__).with_name("watch_artifact.py")
SPEC = importlib.util.spec_from_file_location("watch_artifact", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class WatchArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = pathlib.Path(self.tempdir.name) / "result.json"

    def tearDown(self):
        self.tempdir.cleanup()

    def write_json(self, payload):
        self.path.write_text(json.dumps(payload), encoding="utf-8")

    def run_watch(self, **overrides):
        clock = FakeClock()
        output = io.StringIO()
        kwargs = {
            "path": self.path,
            "poll_seconds": 1,
            "timeout_seconds": 3,
            "invalid_grace_seconds": 1,
            "min_bytes": 1,
            "exists_is_success": False,
            "status_field": "status",
            "success_values": [True],
            "failure_values": [False],
            "expectations": [("requestId", "req-1")],
            "required_nonempty": ["data.notes.note"],
            "max_json_bytes": 8 * 1024 * 1024,
            "not_before_epoch_seconds": None,
            "clock": clock,
            "sleep": clock.sleep,
        }
        kwargs.update(overrides)
        with contextlib.redirect_stdout(output):
            code = MODULE.watch(**kwargs)
        return code, json.loads(output.getvalue())

    def test_successful_callback(self):
        self.write_json(
            {"requestId": "req-1", "status": True, "data": {"notes": {"note": "# Note"}}}
        )
        code, event = self.run_watch()
        self.assertEqual(code, 0)
        self.assertEqual(event["event"], "completed")
        self.assertNotIn("document", event)

    def test_failure_callback(self):
        self.write_json({"requestId": "req-1", "status": False, "message": "failed"})
        code, event = self.run_watch(required_nonempty=[])
        self.assertEqual(code, 3)
        self.assertEqual(event["event"], "terminal_failure")

    def test_identity_mismatch(self):
        self.write_json(
            {"requestId": "other", "status": True, "data": {"notes": {"note": "x"}}}
        )
        code, event = self.run_watch()
        self.assertEqual(code, 3)
        self.assertEqual(event["event"], "contract_failure")

    def test_missing_success_content(self):
        self.write_json({"requestId": "req-1", "status": True, "data": {}})
        code, event = self.run_watch()
        self.assertEqual(code, 3)
        self.assertEqual(event["fields"], ["data.notes.note"])

    def test_unknown_status_waits_until_timeout(self):
        self.write_json({"requestId": "req-1", "status": "running"})
        code, event = self.run_watch(required_nonempty=[])
        self.assertEqual(code, 4)
        self.assertEqual(event["event"], "timeout")

    def test_absent_file_waits_until_timeout(self):
        code, event = self.run_watch(required_nonempty=[])
        self.assertEqual(code, 4)
        self.assertEqual(event["event"], "timeout")

    def test_persistent_invalid_json_fails(self):
        self.path.write_text("{", encoding="utf-8")
        code, event = self.run_watch(required_nonempty=[])
        self.assertEqual(code, 5)
        self.assertEqual(event["event"], "invalid_artifact")

    def test_symlink_artifact_is_rejected(self):
        target = pathlib.Path(self.tempdir.name) / "target.json"
        target.write_text(
            json.dumps({"requestId": "req-1", "status": True, "data": {"notes": {"note": "x"}}}),
            encoding="utf-8",
        )
        self.path.symlink_to(target)
        code, event = self.run_watch()
        self.assertEqual(code, 5)
        self.assertEqual(event["event"], "invalid_artifact")

    def test_oversized_json_artifact_is_rejected(self):
        self.path.write_text(json.dumps({"status": "x", "padding": "A" * 200}), encoding="utf-8")
        code, event = self.run_watch(required_nonempty=[], max_json_bytes=64)
        self.assertEqual(code, 5)
        self.assertEqual(event["event"], "invalid_artifact")
        self.assertIn("max-json-bytes", event["detail"])

    def test_exists_only_mode(self):
        self.path.write_text("done", encoding="utf-8")
        code, event = self.run_watch(
            exists_is_success=True,
            success_values=[],
            failure_values=[],
            expectations=[],
            required_nonempty=[],
        )
        self.assertEqual(code, 0)
        self.assertEqual(event["event"], "completed")

    def test_stale_artifact_waits_until_timeout(self):
        self.write_json(
            {"requestId": "req-1", "status": True, "data": {"notes": {"note": "x"}}}
        )
        os.utime(self.path, (100.0, 100.0))
        code, event = self.run_watch(not_before_epoch_seconds=200.0)
        self.assertEqual(code, 4)
        self.assertEqual(event["event"], "timeout")

    def test_artifact_at_or_after_task_start_is_accepted(self):
        self.write_json(
            {"requestId": "req-1", "status": True, "data": {"notes": {"note": "x"}}}
        )
        os.utime(self.path, (200.0, 200.0))
        code, event = self.run_watch(not_before_epoch_seconds=200.0)
        self.assertEqual(code, 0)
        self.assertEqual(event["event"], "completed")

    def test_nested_array_field(self):
        document = {"items": [{"state": "done"}]}
        self.assertEqual(MODULE.resolve_field(document, "items.0.state"), "done")
        self.assertIs(MODULE.resolve_field(document, "items.1.state"), MODULE.MISSING)

    def test_expectation_parser_uses_json_literals(self):
        self.assertEqual(
            MODULE.parse_expectations(['requestId="req-1"', "attempt=2"]),
            [("requestId", "req-1"), ("attempt", 2)],
        )

    def test_boolean_status_does_not_match_numeric_terminal_values(self):
        for status, success_values, failure_values in (
            (1, [True], [False]),
            (0, [True], [False]),
            (True, [1], [0]),
            (False, [1], [0]),
        ):
            event, _ = MODULE.evaluate_json(
                {"status": status},
                "status",
                success_values,
                failure_values,
                [],
                [],
            )
            self.assertEqual(event, "waiting")

    def test_numeric_json_values_share_the_json_number_type(self):
        self.assertTrue(MODULE.json_values_equal(1, 1.0))
        self.assertFalse(MODULE.json_values_equal(True, 1))
        self.assertFalse(MODULE.json_values_equal(False, 0))

    def test_nested_identity_comparison_is_type_strict(self):
        event, details = MODULE.evaluate_json(
            {"status": "running", "identity": {"attempt": True}},
            "status",
            ["completed"],
            ["failed"],
            [("identity", {"attempt": 1})],
            [],
        )
        self.assertEqual(event, "contract_failure")
        self.assertIn("identity mismatch", details["detail"])

    def test_success_and_failure_values_overlap_strictly(self):
        self.assertTrue(MODULE.json_value_sets_overlap([True], [True]))
        self.assertFalse(MODULE.json_value_sets_overlap([True], [1]))
        self.assertTrue(MODULE.json_value_sets_overlap([1], [1.0]))

    def test_cli_rejects_overlapping_terminal_values(self):
        argv = [
            "watch_artifact.py",
            str(self.path),
            "--timeout-seconds",
            "1",
            "--success-json",
            "true",
            "--failure-json",
            "true",
        ]
        with mock.patch.object(sys, "argv", argv), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(SystemExit, "2"):
                MODULE.parse_args()

    def test_cli_allows_boolean_and_numeric_terminal_values(self):
        argv = [
            "watch_artifact.py",
            str(self.path),
            "--timeout-seconds",
            "1",
            "--success-json",
            "true",
            "--failure-json",
            "1",
        ]
        with mock.patch.object(sys, "argv", argv):
            args = MODULE.parse_args()
        self.assertEqual(args.success_values, [True])
        self.assertEqual(args.failure_values, [1])

    def test_cli_main_forwards_max_json_bytes(self):
        argv = [
            "watch_artifact.py",
            str(self.path),
            "--timeout-seconds",
            "1",
            "--max-json-bytes",
            "64",
            "--success-json",
            "true",
            "--failure-json",
            "false",
        ]
        with mock.patch.object(sys, "argv", argv), mock.patch.object(
            MODULE, "watch", return_value=0
        ) as watched:
            self.assertEqual(MODULE.main(), 0)
        self.assertEqual(watched.call_args.kwargs["max_json_bytes"], 64)


if __name__ == "__main__":
    unittest.main()

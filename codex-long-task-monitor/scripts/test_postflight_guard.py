#!/usr/bin/env python3
"""CLI tests for the vendored postflight guard."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
GUARD = HERE / "postflight_guard.py"
import semantic_events as se


def run_guard(*args: str) -> tuple[int, dict]:
    result = subprocess.run(
        [sys.executable, str(GUARD), *args],
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    payload = json.loads(lines[-1]) if lines else {}
    return result.returncode, payload


class PostflightGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "state"
        self.event_id = "sha256:" + "1" * 64
        self.other_digest = "sha256:" + "2" * 64
        self.digest = "sha256:" + "3" * 64

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_check_before_and_after_mark(self) -> None:
        code, payload = run_guard("check", self.event_id, "--state-dir", str(self.state))
        self.assertEqual(code, 0)
        self.assertFalse(payload["processed"])
        self.assertTrue(payload["terminal_evidence_required"])
        code, payload = run_guard(
            "mark", self.event_id, "--terminal-digest", self.digest,
            "--state-dir", str(self.state),
        )
        self.assertEqual(code, 0)
        self.assertEqual(payload["state"], "marked")
        code, payload = run_guard("check", self.event_id, "--state-dir", str(self.state))
        self.assertEqual(code, 3)
        self.assertTrue(payload["processed"])
        self.assertEqual(payload["record"]["terminal_digest"], self.digest)

    def test_duplicate_mark_is_idempotent_without_new_effects(self) -> None:
        run_guard("mark", self.event_id, "--terminal-digest", self.digest,
                  "--state-dir", str(self.state))
        code, payload = run_guard(
            "mark", self.event_id, "--terminal-digest", self.digest,
            "--state-dir", str(self.state),
        )
        self.assertEqual(code, 3)
        self.assertEqual(payload["state"], "already_marked")

    def test_raw_and_prefixed_digest_forms_normalize_identically(self) -> None:
        raw = self.digest.removeprefix("sha256:")
        code, payload = run_guard(
            "mark", self.event_id, "--terminal-digest", raw,
            "--state-dir", str(self.state),
        )
        self.assertEqual(code, 0, payload)
        self.assertEqual(payload["terminal_digest"], self.digest)
        code, payload = run_guard(
            "mark", self.event_id, "--terminal-digest", self.digest,
            "--state-dir", str(self.state),
        )
        self.assertEqual(code, 3, payload)
        self.assertEqual(payload["terminal_digest"], self.digest)

    def test_raw_digest_begin_is_normalized_before_claim(self) -> None:
        code, payload = run_guard(
            "begin", self.event_id,
            "--terminal-digest", self.digest.removeprefix("sha256:"),
            "--owner", "turn-raw", "--state-dir", str(self.state),
        )
        self.assertEqual(code, 0, payload)
        self.assertEqual(payload["terminal_digest"], self.digest)
        _, checked = run_guard("check", self.event_id, "--state-dir", str(self.state))
        self.assertEqual(checked["record"]["terminal_digest"], self.digest)

    def test_digest_conflict_blocks_and_fails_closed(self) -> None:
        run_guard("mark", self.event_id, "--terminal-digest", self.digest,
                  "--state-dir", str(self.state))
        code, payload = run_guard(
            "mark", self.event_id, "--terminal-digest", self.other_digest,
            "--state-dir", str(self.state),
        )
        self.assertEqual(code, 4)
        self.assertEqual(payload["state"], "digest_conflict")
        # the original marker is unchanged
        _, payload = run_guard("check", self.event_id, "--state-dir", str(self.state))
        self.assertEqual(payload["record"]["terminal_digest"], self.digest)

    def test_invalid_inputs_are_rejected(self) -> None:
        for bad in ("not-a-sha", "sha256:xyz", "sha256:" + "4" * 63):
            code, payload = run_guard("check", bad, "--state-dir", str(self.state))
            self.assertEqual(code, 12, bad)
            self.assertEqual(payload["state"], "error")
        code, payload = run_guard(
            "mark", self.event_id, "--terminal-digest", "nope",
            "--state-dir", str(self.state),
        )
        self.assertEqual(code, 12)
        self.assertEqual(
            payload["detail"],
            "expected 64 lowercase hex or sha256:<64 lowercase hex>",
        )
        for bad in ("A" * 64, "a" * 63, "sha256:" + "A" * 64):
            code, payload = run_guard(
                "begin", self.event_id, "--terminal-digest", bad,
                "--owner", "turn-bad", "--state-dir", str(self.state),
            )
            self.assertEqual(code, 12, bad)
            self.assertEqual(payload["reason"], "terminal_digest_invalid")

    def test_semantic_events_contract_remains_prefix_strict(self) -> None:
        with self.assertRaises(se.SemanticEventError) as ctx:
            se.postflight_mark(
                self.state, self.event_id,
                terminal_digest=self.digest.removeprefix("sha256:"),
            )
        self.assertEqual(ctx.exception.reason, "terminal_digest_invalid")

    def test_list_reports_markers(self) -> None:
        run_guard("mark", self.event_id, "--terminal-digest", self.digest,
                  "--state-dir", str(self.state))
        code, payload = run_guard("list", "--state-dir", str(self.state))
        self.assertEqual(code, 0)
        self.assertEqual(len(payload["records"]), 1)
        self.assertEqual(payload["records"][0]["event_id"], self.event_id)

    def test_begin_complete_lifecycle_and_concurrency(self) -> None:
        proc_a = subprocess.Popen(
            [sys.executable, str(GUARD), "begin", self.event_id,
             "--terminal-digest", self.digest, "--owner", "turn-a",
             "--state-dir", str(self.state)],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc_b = subprocess.Popen(
            [sys.executable, str(GUARD), "begin", self.event_id,
             "--terminal-digest", self.digest, "--owner", "turn-b",
             "--state-dir", str(self.state)],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out_a, _ = proc_a.communicate(timeout=10)
        out_b, _ = proc_b.communicate(timeout=10)
        payload_a = json.loads(out_a.strip().splitlines()[-1])
        payload_b = json.loads(out_b.strip().splitlines()[-1])
        outcomes = sorted([payload_a["state"], payload_b["state"]])
        self.assertEqual(outcomes, ["already_in_progress", "begun"])
        winner, loser = (
            (payload_a, payload_b) if payload_a["state"] == "begun" else (payload_b, payload_a)
        )
        owner = winner["owner"]
        # The losing turn must not perform side effects; completing with the
        # wrong owner fails closed.
        code, payload = run_guard(
            "complete", self.event_id, "--owner", loser["owner"],
            "--state-dir", str(self.state),
        )
        self.assertEqual(code, 4)
        self.assertEqual(payload["state"], "not_owner")
        code, payload = run_guard(
            "complete", self.event_id, "--owner", owner, "--state-dir", str(self.state)
        )
        self.assertEqual(code, 0)
        self.assertEqual(payload["state"], "completed")
        code, _ = run_guard("check", self.event_id, "--state-dir", str(self.state))
        self.assertEqual(code, 3)

    def test_unknown_result_never_auto_taken_over_and_manual_reset(self) -> None:
        run_guard("begin", self.event_id, "--terminal-digest", self.digest,
                  "--owner", "turn-a", "--state-dir", str(self.state))
        code, payload = run_guard("begin", self.event_id, "--terminal-digest",
                                  self.digest, "--owner", "turn-b",
                                  "--state-dir", str(self.state))
        self.assertEqual(code, 5)
        self.assertEqual(payload["state"], "already_in_progress")
        # reset without confirmation changes nothing
        code, payload = run_guard("reset", self.event_id, "--state-dir", str(self.state))
        self.assertEqual(code, 4)
        self.assertEqual(payload["state"], "confirmation_required")
        code, payload = run_guard("reset", self.event_id, "--state-dir", str(self.state),
                                  "--i-mean-it")
        self.assertEqual(code, 0)
        self.assertEqual(payload["state"], "reset")
        # after human reset a new claim may begin
        code, payload = run_guard("begin", self.event_id, "--terminal-digest",
                                  self.digest, "--owner", "turn-c",
                                  "--state-dir", str(self.state))
        self.assertEqual(code, 0)


class VendorSyncTests(unittest.TestCase):
    SIBLING = "codex-hpc-monitor"

    def test_vendored_copies_are_identical(self) -> None:
        sibling = HERE.parent.parent / self.SIBLING / "scripts" / "postflight_guard.py"
        if not sibling.exists():
            self.skipTest(f"sibling skill not installed: {self.SIBLING}")
        self.assertEqual(
            (HERE / "postflight_guard.py").read_bytes(),
            sibling.read_bytes(),
            "vendored postflight_guard.py copies have diverged",
        )


if __name__ == "__main__":
    unittest.main()

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

    def test_list_reports_markers(self) -> None:
        run_guard("mark", self.event_id, "--terminal-digest", self.digest,
                  "--state-dir", str(self.state))
        code, payload = run_guard("list", "--state-dir", str(self.state))
        self.assertEqual(code, 0)
        self.assertEqual(len(payload["records"]), 1)
        self.assertEqual(payload["records"][0]["event_id"], self.event_id)


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

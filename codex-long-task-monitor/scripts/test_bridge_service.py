#!/usr/bin/env python3

from __future__ import annotations

import json
import plistlib
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path


HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "bridge_service.py"
import semantic_events as se
import bridge_service as service


SERVICE_NAME = "codex-monitor-test"


class BridgeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.service_dir = self.root / "services"
        self.state = self.root / "state"
        self.workspace = self.root / "workspace"
        self.codex_home = self.root / ".codex"
        self.config = self.root / "bridge.json"
        payload = {
            "schema": se.BRIDGE_CONFIG_SCHEMA,
            "enabled": True,
            "instance_id": "local-1",
            "codex_home": str(self.codex_home),
            "codex_home_id": se.codex_home_digest(self.codex_home),
            "workspace": str(self.workspace),
            "transport": {"type": "stdio", "command": ["codex", "app-server"]},
            "request_timeout_seconds": 30,
            "poll_seconds": 5,
            "lease_seconds": 300,
            "max_attempts": 16,
            "backoff_initial_seconds": 5,
            "backoff_max_seconds": 3600,
            "turn_completion_timeout_seconds": 3600,
        }
        self.config.write_text(json.dumps(payload))
        self.config.chmod(0o600)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args], text=True,
            capture_output=True, check=False, timeout=10,
        )

    def install_args(self, platform: str = "linux") -> list[str]:
        return [
            "install", "--platform", platform,
            "--service-name", SERVICE_NAME,
            "--service-dir", str(self.service_dir),
            "--state-dir", str(self.state),
            "--bridge-config", str(self.config),
            "--no-manager",
        ]

    def test_linux_dry_run_is_non_mutating_and_contains_no_config_secret(self) -> None:
        result = self.cli(*self.install_args(), "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["state"], "dry_run")
        self.assertIn("NoNewPrivileges=true", payload["definition"])
        self.assertIn(str(self.config), payload["definition"])
        self.assertFalse(self.service_dir.exists())

    def test_install_refuses_overwrite_and_replace_keeps_backup(self) -> None:
        first = self.cli(*self.install_args())
        self.assertEqual(first.returncode, 0, first.stdout)
        path = self.service_dir / f"{SERVICE_NAME}.service"
        self.assertTrue(path.is_file())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        refused = self.cli(*self.install_args())
        self.assertEqual(refused.returncode, 12)
        replaced = self.cli(*self.install_args(), "--replace")
        self.assertEqual(replaced.returncode, 0, replaced.stdout)
        backup = Path(json.loads(replaced.stdout)["backup"])
        self.assertTrue(backup.is_file())

    def test_uninstall_requires_confirmation_and_is_recoverable(self) -> None:
        self.cli(*self.install_args())
        common = [
            "--platform", "linux", "--service-dir", str(self.service_dir),
            "--service-name", SERVICE_NAME,
            "--no-manager",
        ]
        refused = self.cli("uninstall", *common)
        self.assertEqual(refused.returncode, 4)
        accepted = self.cli("uninstall", *common, "--i-mean-it")
        self.assertEqual(accepted.returncode, 0, accepted.stdout)
        payload = json.loads(accepted.stdout)
        self.assertEqual(payload["state"], "removed_recoverably")
        self.assertTrue(Path(payload["recoverable_path"]).is_file())

    def test_launchd_definition_is_valid_plist(self) -> None:
        result = self.cli(*self.install_args("darwin"), "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        plist = plistlib.loads(payload["definition"].encode())
        self.assertEqual(plist["Label"], SERVICE_NAME)
        self.assertTrue(plist["RunAtLoad"])
        self.assertNotIn("EnvironmentVariables", plist)

    def test_repair_detects_and_recovers_definition_drift(self) -> None:
        self.cli(*self.install_args())
        path = self.service_dir / f"{SERVICE_NAME}.service"
        path.write_text("drifted\n")
        path.chmod(0o600)
        base = [
            "repair", "--platform", "linux", "--service-dir", str(self.service_dir),
            "--service-name", SERVICE_NAME,
            "--state-dir", str(self.state), "--bridge-config", str(self.config),
            "--no-manager",
        ]
        detected = self.cli(*base)
        self.assertEqual(detected.returncode, 3)
        self.assertEqual(json.loads(detected.stdout)["state"], "drifted")
        repaired = self.cli(*base, "--apply")
        self.assertEqual(repaired.returncode, 0, repaired.stdout)
        payload = json.loads(repaired.stdout)
        self.assertEqual(payload["state"], "repaired")
        self.assertTrue(Path(payload["backup"]).is_file())
        self.assertIn("ExecStart=", path.read_text())

    def test_invalid_config_and_service_name_fail_closed(self) -> None:
        self.config.chmod(0o644)
        bad = self.cli(*self.install_args(), "--dry-run")
        self.assertEqual(bad.returncode, 12)
        self.config.chmod(0o600)
        args = self.install_args()
        args[args.index(SERVICE_NAME)] = "../escape"
        bad = self.cli(*args, "--dry-run")
        self.assertEqual(bad.returncode, 12)

    def test_service_name_is_required_and_prevents_cross_skill_collision(self) -> None:
        missing = [item for item in self.install_args() if item != SERVICE_NAME]
        index = missing.index("--service-name")
        del missing[index]
        self.assertNotEqual(self.cli(*missing, "--dry-run").returncode, 0)
        other = self.install_args()
        other[other.index(SERVICE_NAME)] = "codex-monitor-other"
        self.assertEqual(self.cli(*self.install_args()).returncode, 0)
        self.assertEqual(self.cli(*other).returncode, 0)
        self.assertTrue((self.service_dir / f"{SERVICE_NAME}.service").is_file())
        self.assertTrue((self.service_dir / "codex-monitor-other.service").is_file())

    def test_existing_service_parent_permissions_are_preserved(self) -> None:
        self.service_dir.mkdir(mode=0o755)
        self.assertEqual(self.cli(*self.install_args()).returncode, 0)
        self.assertEqual(self.service_dir.stat().st_mode & 0o777, 0o755)

    def test_manager_failure_rolls_back_new_definition(self) -> None:
        parsed = service.parser().parse_args([
            *self.install_args()[:-1],  # discard --no-manager
        ])
        with mock.patch.object(service, "_manager_action", return_value={
            "ok": False, "results": [{"returncode": 1}],
        }):
            with mock.patch("builtins.print"):
                result = service.install_command(parsed)
        self.assertEqual(result, 4)
        self.assertFalse((self.service_dir / f"{SERVICE_NAME}.service").exists())
        self.assertTrue(list(self.service_dir.glob("*.failed.*")))

    def test_rapid_replacements_use_distinct_backups(self) -> None:
        self.assertEqual(self.cli(*self.install_args()).returncode, 0)
        first = json.loads(self.cli(*self.install_args(), "--replace").stdout)
        second = json.loads(self.cli(*self.install_args(), "--replace").stdout)
        self.assertNotEqual(first["backup"], second["backup"])
        self.assertTrue(Path(first["backup"]).is_file())
        self.assertTrue(Path(second["backup"]).is_file())

    def test_start_with_disabled_config_fails_before_install(self) -> None:
        payload = json.loads(self.config.read_text())
        payload["enabled"] = False
        self.config.write_text(json.dumps(payload))
        self.config.chmod(0o600)
        args = self.install_args()
        args.remove("--no-manager")
        result = self.cli(*args, "--start")
        self.assertEqual(result.returncode, 12, result.stdout)
        self.assertEqual(json.loads(result.stdout)["reason"], "bridge_disabled")
        self.assertFalse((self.service_dir / f"{SERVICE_NAME}.service").exists())

    def test_action_and_uninstall_share_one_lifecycle_lock(self) -> None:
        self.assertEqual(self.cli(*self.install_args()).returncode, 0)
        common = [
            "--platform", "linux", "--service-name", SERVICE_NAME,
            "--service-dir", str(self.service_dir),
        ]
        action_args = service.parser().parse_args(["start", *common])
        uninstall_args = service.parser().parse_args([
            "uninstall", *common, "--no-manager", "--i-mean-it",
        ])
        entered = threading.Event()
        release = threading.Event()
        results: list[int] = []

        def blocking_manager(*_args: object, **_kwargs: object) -> dict:
            entered.set()
            self.assertTrue(release.wait(5))
            return {"ok": True, "results": []}

        with mock.patch.object(service, "_manager_action", side_effect=blocking_manager):
            with mock.patch("builtins.print"):
                action = threading.Thread(
                    target=lambda: results.append(service.action_command(action_args))
                )
                uninstall = threading.Thread(
                    target=lambda: results.append(service.uninstall_command(uninstall_args))
                )
                action.start()
                self.assertTrue(entered.wait(5))
                uninstall.start()
                time.sleep(0.1)
                self.assertTrue(uninstall.is_alive())
                self.assertTrue((self.service_dir / f"{SERVICE_NAME}.service").exists())
                release.set()
                action.join(5)
                uninstall.join(5)
        self.assertEqual(sorted(results), [0, 0])
        self.assertFalse((self.service_dir / f"{SERVICE_NAME}.service").exists())


class VendorSyncTests(unittest.TestCase):
    def test_vendored_copy_is_identical(self) -> None:
        sibling = HERE.parent.parent / "codex-hpc-monitor" / "scripts" / "bridge_service.py"
        if not sibling.exists():
            self.skipTest("sibling skill not installed")
        self.assertEqual(SCRIPT.read_bytes(), sibling.read_bytes())


if __name__ == "__main__":
    unittest.main()

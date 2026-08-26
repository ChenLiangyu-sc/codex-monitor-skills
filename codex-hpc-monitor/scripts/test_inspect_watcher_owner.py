#!/usr/bin/env python3

import fcntl
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("inspect_watcher_owner.py")
SPEC = importlib.util.spec_from_file_location("inspect_watcher_owner", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class OwnerInspectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "hpc142-123.lock.json"
        self.command = ["/usr/bin/python3", "/skill/watch_slurm_job.py", "123", "--host", "hpc142"]
        self.payload = {
            "pid": 4242,
            "pid_start_ticks": "9001",
            "host": "hpc142",
            "job_id": "123",
            "command": self.command,
        }

    def tearDown(self):
        self.temp.cleanup()

    def acquire_payload_lock(self):
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        os.write(fd, (json.dumps(self.payload) + "\n").encode())
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd

    def inspect(self, reader=None):
        kwargs = {"host": "hpc142", "job_id": "123"}
        if reader is not None:
            kwargs["process_reader"] = reader
        return MODULE.inspect_owner(self.path, **kwargs)

    def test_absent_lock_is_inactive(self):
        self.assertEqual(self.inspect()["status"], "inactive")

    def test_released_lock_is_inactive_even_with_stale_payload(self):
        self.path.write_text(json.dumps(self.payload), encoding="utf-8")
        self.assertEqual(self.inspect()["status"], "inactive")

    def test_active_owner_requires_four_way_match(self):
        fd = self.acquire_payload_lock()
        try:
            result = self.inspect(lambda pid: ("9001", self.command))
        finally:
            os.close(fd)
        self.assertEqual(result["status"], "active_verified")
        self.assertEqual(result["pid"], 4242)

    def test_pid_start_or_command_mismatch_is_inconsistent(self):
        fd = self.acquire_payload_lock()
        try:
            for observed in (("9002", self.command), ("9001", ["other"])):
                with self.subTest(observed=observed):
                    self.assertEqual(
                        self.inspect(lambda pid, value=observed: value)["status"],
                        "inconsistent",
                    )
        finally:
            os.close(fd)

    def test_equivalent_interpreter_argv_zero_is_accepted(self):
        self.command = [sys.executable, "/skill/watch_slurm_job.py", "123", "--host", "hpc142"]
        self.payload["command"] = self.command
        observed = [Path(sys.executable).name, *self.command[1:]]
        fd = self.acquire_payload_lock()
        try:
            result = self.inspect(lambda pid: ("9001", observed))
        finally:
            os.close(fd)
        self.assertEqual(result["status"], "active_verified")

    def test_equivalent_interpreter_does_not_hide_argument_mismatch(self):
        self.command = [sys.executable, "/skill/watch_slurm_job.py", "123", "--host", "hpc142"]
        self.payload["command"] = self.command
        observed = [Path(sys.executable).name, *self.command[1:-1], "other-host"]
        fd = self.acquire_payload_lock()
        try:
            result = self.inspect(lambda pid: ("9001", observed))
        finally:
            os.close(fd)
        self.assertEqual(result["status"], "inconsistent")

    def test_held_lock_with_wrong_job_payload_is_inconsistent(self):
        self.payload["job_id"] = "999"
        fd = self.acquire_payload_lock()
        try:
            result = self.inspect(lambda pid: ("9001", self.command))
        finally:
            os.close(fd)
        self.assertEqual(result["status"], "inconsistent")


if __name__ == "__main__":
    unittest.main()

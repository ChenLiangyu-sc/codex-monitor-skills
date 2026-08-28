#!/usr/bin/env python3
"""Behavioral tests for the vendored semantic_events module."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import semantic_events as se


HERE = Path(__file__).resolve().parent
NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def stamp(moment: datetime) -> str:
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_binding() -> dict:
    return {
        "schema": se.EVENT_BINDING_SCHEMA,
        "codex_home_id": "sha256:" + "a" * 64,
        "app_server_instance": "workstation-1",
        "thread_id": "thr_abc123",
        "workspace": "/home/user/project",
    }


def make_event(**overrides) -> dict:
    values = {
        "backend": "slurm",
        "handle": "fakehost-12345",
        "generation": "run_1_2_abcd1234",
        "terminal_digest": "sha256:" + "b" * 64,
        "event": "transport_success",
        "exit_code": 0,
        "binding": make_binding(),
        "created_at": stamp(NOW),
    }
    values.update(overrides)
    return se.build_event(**values)


def make_config() -> dict:
    return {
        "schema": se.BRIDGE_CONFIG_SCHEMA,
        "enabled": True,
        "instance_id": "workstation-1",
        "codex_home": "/home/user/.codex",
        "codex_home_id": se.codex_home_digest(Path("/home/user/.codex")),
        "workspace": "/home/user/project",
        "transport": {"type": "stdio", "command": ["codex", "app-server"]},
        "request_timeout_seconds": 30,
        "poll_seconds": 5,
        "lease_seconds": 300,
        "max_attempts": 3,
        "backoff_initial_seconds": 5,
        "backoff_max_seconds": 3600,
        "turn_completion_timeout_seconds": 3600,
    }


class EventContractTests(unittest.TestCase):
    def test_event_id_is_deterministic_over_identity_tuple(self) -> None:
        first = make_event()
        second = make_event()
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertTrue(first["event_id"].startswith("sha256:"))

    def test_event_id_changes_with_each_identity_component(self) -> None:
        base = make_event()
        variations = (
            make_event(handle="fakehost-99999"),
            make_event(generation="run_9_9_ffff0000"),
            make_event(terminal_digest="sha256:" + "c" * 64),
            make_event(event="transport_failure", exit_code=3),
            make_event(binding={**make_binding(), "thread_id": "thr_other"}),
            make_event(backend="artifact"),
        )
        ids = {base["event_id"]} | {item["event_id"] for item in variations}
        self.assertEqual(len(ids), 7)

    def test_event_id_ignores_created_at(self) -> None:
        # Identity is schema/monitor/event/binding only; a rebuilt event with
        # a fresh timestamp is the same logical event.
        left = make_event(exit_code=0, created_at=stamp(NOW))
        right = make_event(exit_code=0, created_at=stamp(NOW + timedelta(hours=3)))
        self.assertEqual(left["event_id"], right["event_id"])

    def test_validate_rejects_unknown_and_malformed_fields(self) -> None:
        with self.assertRaises(se.SemanticEventError):
            se.build_event(backend="http", handle="x", generation="g",
                           terminal_digest="sha256:" + "b" * 64,
                           event="transport_success", exit_code=0,
                           binding=make_binding())
        event = make_event()
        for mutation in (
            {**event, "extra": 1},
            {**event, "event": "not_an_enum"},
            {**event, "business_verdict": "accepted"},
            {**event, "event_id": "sha256:" + "0" * 64},
            {**event, "exit_code": "0"},
            {**event, "monitor": {**event["monitor"], "terminal_digest": "b" * 64}},
        ):
            with self.assertRaises(se.SemanticEventError):
                se.validate_event(mutation)

    def test_wake_message_is_the_fixed_template(self) -> None:
        event = make_event()
        text = se.wake_message(event)
        self.assertIn("codex_monitor_event/v1\n", text)
        self.assertIn(f"event_id={event['event_id']}", text)
        self.assertIn("backend=slurm", text)
        self.assertIn("handle=fakehost-12345", text)
        self.assertIn("event=transport_success", text)
        self.assertIn("exit_code=0", text)
        self.assertIn("business_verdict=pending", text)
        self.assertIn("process this event idempotently", text)
        self.assertIn("Do not retry, cancel, resubmit, mutate, or approve", text)
        null_event = make_event(event="lost_observability", exit_code=None)
        self.assertIn("exit_code=null", se.wake_message(null_event))


class BridgeConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "bridge.json"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_config(self, value: dict, mode: int = 0o600) -> Path:
        self.path.write_text(json.dumps(value), encoding="utf-8")
        self.path.chmod(mode)
        return self.path

    def test_valid_config_loads(self) -> None:
        config = self.write_config(make_config())
        self.assertEqual(se.load_bridge_config(config)["instance_id"], "workstation-1")

    def test_unknown_fields_and_bad_values_fail_closed(self) -> None:
        for mutation in (
            {**make_config(), "schema": "codex-monitor.bridge-config/v2"},
            {**make_config(), "enabled": "yes"},
            {**make_config(), "instance_id": "bad id!"},
            {**make_config(), "codex_home_id": "b" * 64},
            {**make_config(), "workspace": "relative/path"},
            {**make_config(), "transport": {"type": "tcp", "command": ["x"]}},
            {**make_config(), "transport": {"type": "unix_socket", "command": ["x"]}},
            {**make_config(), "transport": {"type": "stdio"}},
            {**make_config(), "transport": {"type": "stdio", "command": []}},
            {**make_config(), "request_timeout_seconds": 0},
            {**make_config(), "max_attempts": 0},
            {**make_config(), "backoff_max_seconds": 1},
            {**make_config(), "extra_key": True},
            {**make_config(), "codex_home": "relative/path"},
            {**make_config(), "codex_home_id": "sha256:" + "b" * 64},
            {**make_config(), "lease_seconds": 31},
            {**make_config(), "turn_completion_timeout_seconds": 0},
        ):
            with self.assertRaises(se.SemanticEventError):
                se.validate_bridge_config(mutation)

    def test_world_readable_config_is_rejected(self) -> None:
        path = self.write_config(make_config(), mode=0o644)
        with self.assertRaises(se.SemanticEventError) as ctx:
            se.load_bridge_config(path)
        self.assertEqual(ctx.exception.reason, "config_permissions_too_open")

    def test_missing_and_symlinked_config_fail_closed(self) -> None:
        with self.assertRaises(se.SemanticEventError):
            se.load_bridge_config(self.root / "absent.json")
        real = self.write_config(make_config())
        link = self.root / "link.json"
        link.symlink_to(real)
        with self.assertRaises(se.SemanticEventError):
            se.load_bridge_config(link)

    def test_event_binding_validation(self) -> None:
        se.validate_event_binding(make_binding())
        for mutation in (
            {**make_binding(), "schema": "other"},
            {**make_binding(), "thread_id": ""},
            {**make_binding(), "app_server_instance": "bad id!"},
            {**make_binding(), "workspace": "rel"},
            {**make_binding(), "codex_home_id": "zz"},
            {**make_binding(), "thread_id": "x" * 300},
            {**make_binding(), "extra": 1},
        ):
            with self.assertRaises(se.SemanticEventError):
                se.validate_event_binding(mutation)

    def test_codex_home_digest_is_stable_and_prefixed(self) -> None:
        first = se.codex_home_digest(Path("/home/user/.codex"))
        second = se.codex_home_digest(Path("/home/user/.codex/"))
        self.assertEqual(first, second)
        self.assertTrue(first.startswith("sha256:"))


class OutboxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.outbox = se.outbox_root(self.state)
        self.event = make_event()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def publish(self, event: dict | None = None) -> str:
        return se.publish_event(self.outbox, event or self.event)

    def test_publish_then_duplicate_is_one_logical_event(self) -> None:
        self.assertEqual(self.publish(), "published")
        self.assertEqual(self.publish(), "duplicate")
        entries = se.list_outbox(self.outbox)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["state"], "pending")

    def test_rebuilt_event_with_fresh_timestamp_is_duplicate(self) -> None:
        self.publish()
        rebuilt = make_event(created_at=stamp(NOW + timedelta(hours=5)))
        self.assertEqual(self.publish(rebuilt), "duplicate")
        self.assertEqual(len(se.list_outbox(self.outbox)), 1)

    def test_concurrent_publishers_produce_one_event(self) -> None:
        results: list[str] = []
        barrier = threading.Barrier(4)

        def worker() -> None:
            barrier.wait()
            results.append(self.publish())

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(results.count("published"), 1)
        self.assertEqual(results.count("duplicate"), 3)
        self.assertEqual(len(se.list_outbox(self.outbox)), 1)

    def test_conflicting_content_for_same_id_is_rejected(self) -> None:
        self.publish()
        # Same identity tuple (thus same event id) but different payload.
        conflicting = make_event(exit_code=3)
        self.assertEqual(
            conflicting["event_id"], self.event["event_id"]
        )
        with self.assertRaises(se.SemanticEventError) as ctx:
            se.publish_event(self.outbox, conflicting)
        self.assertEqual(ctx.exception.reason, "event_conflict")

    def test_claim_lease_ack_and_double_ack(self) -> None:
        self.publish()
        claimed = se.claim_next_event(
            self.outbox, owner="daemon-a", lease_seconds=300, now=NOW
        )
        self.assertIsNotNone(claimed)
        event, delivery = claimed
        self.assertEqual(event["event_id"], self.event["event_id"])
        self.assertEqual(delivery["state"], "leased")
        self.assertEqual(delivery["lease"]["owner"], "daemon-a")
        self.assertIsNone(
            se.claim_next_event(
                self.outbox, owner="daemon-b", lease_seconds=300, now=NOW
            )
        )
        result = se.acknowledge_event(
            self.outbox,
            self.event["event_id"],
            owner="daemon-a",
            now=NOW,
            thread_id="thr_abc123",
            turn_id="turn_1",
            turn_status="completed",
        )
        self.assertEqual(result, "acknowledged")
        delivery = se._read_delivery(
            se.event_dir(self.outbox, self.event["event_id"]),
            self.event["event_id"],
        )
        self.assertEqual(delivery["turn_status"], "completed")
        again = se.acknowledge_event(
            self.outbox,
            self.event["event_id"],
            owner="daemon-a",
            now=NOW,
            thread_id="thr_abc123",
            turn_id="turn_1",
            turn_status="completed",
        )
        self.assertEqual(again, "already_delivered")

    def test_ack_by_non_owner_fails_closed(self) -> None:
        self.publish()
        se.claim_next_event(self.outbox, owner="daemon-a", lease_seconds=300, now=NOW)
        with self.assertRaises(se.SemanticEventError):
            se.acknowledge_event(
                self.outbox,
                self.event["event_id"],
                owner="daemon-b",
                now=NOW,
                thread_id="t",
                turn_id="u",
                turn_status="completed",
            )

    def test_stale_lease_is_recovered_after_expiry(self) -> None:
        self.publish()
        claimed = se.claim_next_event(
            self.outbox, owner="daemon-a", lease_seconds=10, now=NOW
        )
        self.assertIsNotNone(claimed)
        later = NOW + timedelta(seconds=11)
        reclaimed = se.claim_next_event(
            self.outbox, owner="daemon-b", lease_seconds=10, now=later
        )
        self.assertIsNotNone(reclaimed)
        self.assertEqual(reclaimed[1]["lease"]["owner"], "daemon-b")
        # The dead first owner can no longer mutate state.
        with self.assertRaises(se.SemanticEventError):
            se.acknowledge_event(
                self.outbox,
                self.event["event_id"],
                owner="daemon-a",
                now=later,
                thread_id="t",
                turn_id="u",
                turn_status="completed",
            )

    def test_backoff_scheduling_and_dead_letter(self) -> None:
        self.publish()
        deterministic = iter([0.1, 0.1, 0.1]).__next__
        codes = []
        for index in range(3):
            se.claim_next_event(
                self.outbox, owner="daemon", lease_seconds=60, now=NOW
            )
            outcome = se.record_delivery_failure(
                self.outbox,
                self.event["event_id"],
                owner="daemon",
                code=f"failure_{index}",
                safe_message="x" * 500,
                retryable=True,
                now=NOW + timedelta(seconds=index),
                max_attempts=3,
                backoff_initial_seconds=10,
                backoff_max_seconds=100,
                rng=deterministic,
            )
            codes.append(outcome)
        self.assertEqual(codes, ["scheduled_retry", "scheduled_retry", "dead_lettered"])
        delivery = se._read_delivery(
            se.event_dir(self.outbox, self.event["event_id"]),
            self.event["event_id"],
        )
        self.assertEqual(delivery["state"], "dead_letter")
        self.assertEqual(delivery["attempts"], 3)
        self.assertEqual(delivery["last_error"]["code"], "failure_2")
        self.assertEqual(len(delivery["last_error"]["safe_message"]),
                         se.MAX_SAFE_MESSAGE_CHARS)
        self.assertIsNotNone(delivery["finished_at"])
        # A dead-lettered event is not claimable.
        self.assertIsNone(
            se.claim_next_event(
                self.outbox, owner="daemon", lease_seconds=60, now=NOW + timedelta(hours=2)
            )
        )

    def test_retry_not_claimable_before_next_attempt_at(self) -> None:
        self.publish()
        se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=NOW)
        se.record_delivery_failure(
            self.outbox,
            self.event["event_id"],
            owner="d",
            code="app_server_timeout",
            safe_message="request timed out",
            retryable=True,
            now=NOW,
            max_attempts=5,
            backoff_initial_seconds=50,
            backoff_max_seconds=200,
            rng=lambda: 0.5,
        )
        soon = NOW + timedelta(seconds=49)
        self.assertIsNone(
            se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=soon)
        )
        later = NOW + timedelta(seconds=51)
        claimed = se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=later)
        self.assertIsNotNone(claimed)

    def test_non_retryable_failure_dead_letters_immediately(self) -> None:
        self.publish()
        se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=NOW)
        outcome = se.record_delivery_failure(
            self.outbox,
            self.event["event_id"],
            owner="d",
            code="thread_missing",
            safe_message="thread does not exist",
            retryable=False,
            now=NOW,
            max_attempts=5,
            backoff_initial_seconds=1,
            backoff_max_seconds=2,
        )
        self.assertEqual(outcome, "dead_lettered")

    def test_binding_filter_isolates_instances(self) -> None:
        mine = self.event
        other = make_event(
            binding={**make_binding(), "app_server_instance": "other-host"}
        )
        self.publish(mine)
        self.publish(other)
        claimed = se.claim_next_event(
            self.outbox,
            owner="daemon-a",
            lease_seconds=60,
            now=NOW,
            binding_filter=lambda e: e["binding"]["app_server_instance"]
            == "workstation-1",
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed[0]["event_id"], mine["event_id"])

    def test_missing_delivery_state_is_healed_as_pending(self) -> None:
        self.publish()
        (se.event_dir(self.outbox, self.event["event_id"]) / "delivery.json").unlink()
        claimed = se.claim_next_event(
            self.outbox, owner="daemon", lease_seconds=60, now=NOW
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed[1]["state"], "leased")

    def test_release_returns_event_to_pending_without_penalty(self) -> None:
        self.publish()
        se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=NOW)
        self.assertEqual(
            se.release_event(self.outbox, self.event["event_id"], owner="d"),
            "released",
        )
        claimed = se.claim_next_event(
            self.outbox, owner="d2", lease_seconds=60, now=NOW
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed[1]["attempts"], 0)

    def test_unbounded_strings_are_rejected(self) -> None:
        # Bounded identifiers keep events small; a huge workspace is refused
        # at binding validation rather than ever reaching the outbox.
        with self.assertRaises(se.SemanticEventError) as ctx:
            make_event(
                binding={**make_binding(), "workspace": "/" + "w" * 8192}
            )
        self.assertEqual(ctx.exception.reason, "binding_workspace_invalid")
        with self.assertRaises(se.SemanticEventError):
            make_event(generation="g" * 9000)

    def test_cleanup_removes_only_old_delivered_and_optional_dead_letter(self) -> None:
        delivered = self.event
        dead = make_event(
            terminal_digest="sha256:" + "e" * 64, event="transport_failure", exit_code=3
        )
        pending = make_event(
            terminal_digest="sha256:" + "f" * 64, event="deadline_exceeded", exit_code=4
        )
        # delivered: claim + acknowledge
        se.publish_event(self.outbox, delivered)
        se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=NOW)
        se.acknowledge_event(
            self.outbox,
            delivered["event_id"],
            owner="d",
            now=NOW,
            thread_id="t",
            turn_id="u",
            turn_status="completed",
        )
        # dead: claim + non-retryable failure
        se.publish_event(self.outbox, dead)
        se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=NOW)
        se.record_delivery_failure(
            self.outbox,
            dead["event_id"],
            owner="d",
            code="thread_missing",
            safe_message="thread not found",
            retryable=False,
            now=NOW,
            max_attempts=1,
            backoff_initial_seconds=1,
            backoff_max_seconds=2,
        )
        # pending: publish only
        se.publish_event(self.outbox, pending)
        later = NOW + timedelta(days=8)
        dry = se.cleanup_outbox(self.outbox, now=later, older_than_seconds=86400 * 7)
        self.assertIn(delivered["event_id"], dry)
        self.assertNotIn(dead["event_id"], dry)
        self.assertNotIn(pending["event_id"], dry)
        self.assertEqual(len(se.list_outbox(self.outbox)), 3)  # dry-run removes nothing
        applied = se.cleanup_outbox(
            self.outbox, now=later, older_than_seconds=86400 * 7, apply=True
        )
        self.assertEqual(applied, [delivered["event_id"]])
        remaining = {entry["event_id"] for entry in se.list_outbox(self.outbox)}
        self.assertEqual(remaining, {dead["event_id"], pending["event_id"]})
        with_dead = se.cleanup_outbox(
            self.outbox,
            now=later,
            older_than_seconds=86400 * 7,
            include_dead_letter=True,
            apply=True,
        )
        self.assertIn(dead["event_id"], with_dead)
        self.assertNotIn(pending["event_id"], with_dead)

    def test_recently_delivered_events_are_kept(self) -> None:
        self.publish()
        se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=NOW)
        se.acknowledge_event(
            self.outbox, self.event["event_id"], owner="d", now=NOW,
            thread_id="t", turn_id="u", turn_status="completed",
        )
        soon = NOW + timedelta(days=1)
        removed = se.cleanup_outbox(
            self.outbox, now=soon, older_than_seconds=86400 * 7, apply=True
        )
        self.assertEqual(removed, [])

    def test_delivery_validation_rejects_unknown_state(self) -> None:
        bad = se._initial_delivery(stamp(NOW))
        bad["state"] = "flying"
        with self.assertRaises(se.SemanticEventError):
            se._validate_delivery(bad, "sha256:" + "b" * 64)


class PostflightTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "state"
        self.event_id = make_event()["event_id"]
        self.digest = "sha256:" + "9" * 64

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_check_then_mark_is_idempotent(self) -> None:
        self.assertFalse(se.postflight_check(self.state, self.event_id)["processed"])
        self.assertEqual(
            se.postflight_mark(self.state, self.event_id, terminal_digest=self.digest),
            "marked",
        )
        checked = se.postflight_check(self.state, self.event_id)
        self.assertTrue(checked["processed"])
        self.assertEqual(checked["record"]["terminal_digest"], self.digest)
        self.assertEqual(
            se.postflight_mark(self.state, self.event_id, terminal_digest=self.digest),
            "already_marked",
        )

    def test_digest_conflict_fails_closed(self) -> None:
        se.postflight_mark(self.state, self.event_id, terminal_digest=self.digest)
        self.assertEqual(
            se.postflight_mark(
                self.state, self.event_id, terminal_digest="sha256:" + "8" * 64
            ),
            "digest_conflict",
        )
        self.assertEqual(
            se.postflight_check(self.state, self.event_id)["record"]["terminal_digest"],
            self.digest,
        )

    def test_concurrent_marks_produce_one_record(self) -> None:
        outcomes: list[str] = []
        barrier = threading.Barrier(4)

        def worker() -> None:
            barrier.wait()
            outcomes.append(
                se.postflight_mark(
                    self.state, self.event_id, terminal_digest=self.digest
                )
            )

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("marked"), 1)
        self.assertEqual(outcomes.count("already_marked"), 3)


class VendorSyncTests(unittest.TestCase):
    SIBLING = "codex-long-task-monitor"

    def test_vendored_copies_are_identical(self) -> None:
        sibling = HERE.parent.parent / self.SIBLING / "scripts" / "semantic_events.py"
        if not sibling.exists():
            self.skipTest(f"sibling skill not installed: {self.SIBLING}")
        self.assertEqual(
            (HERE / "semantic_events.py").read_bytes(),
            sibling.read_bytes(),
            "vendored semantic_events.py copies have diverged",
        )


class SecurityHardeningTests(unittest.TestCase):
    """Regression tests for the independent-review blockers."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.outbox = se.outbox_root(self.state)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def make_event(self, **overrides) -> dict:
        values = {
            "backend": "slurm",
            "handle": "fakehost-12345",
            "generation": "run_1_2_abcd1234",
            "terminal_digest": "sha256:" + "b" * 64,
            "event": "transport_success",
            "exit_code": 0,
            "binding": make_binding(),
            "created_at": stamp(NOW),
        }
        values.update(overrides)
        return se.build_event(**values)

    def test_event_id_path_traversal_is_rejected(self) -> None:
        for malicious in ("sha256:../../victim", "sha256:..", "sha256:", "sha256:xyz"):
            with self.assertRaises(se.SemanticEventError):
                se.event_dir(self.outbox, malicious)

    def test_symlinked_outbox_entries_are_ignored(self) -> None:
        event = self.make_event()
        se.publish_event(self.outbox, event)
        target = self.root / "real-dir"
        target.mkdir()
        (target / "event.json").write_text("{}")
        link = self.outbox / ("f" * 64)
        link.symlink_to(target)
        entries = se.list_outbox(self.outbox)
        self.assertEqual([e["event_id"] for e in entries], [event["event_id"]])
        claimed = se.claim_next_event(
            self.outbox, owner="d", lease_seconds=60, now=NOW
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed[0]["event_id"], event["event_id"])

    def test_non_hex_directory_names_are_ignored(self) -> None:
        event = self.make_event()
        se.publish_event(self.outbox, event)
        weird = self.outbox / "not-a-digest"
        weird.mkdir()
        (weird / "event.json").write_text("{}")
        self.assertEqual(len(se.list_outbox(self.outbox)), 1)

    def test_renew_event_extends_lease_for_owner_only(self) -> None:
        event = self.make_event()
        se.publish_event(self.outbox, event)
        se.claim_next_event(self.outbox, owner="daemon-a", lease_seconds=10, now=NOW)
        self.assertEqual(
            se.renew_event(
                self.outbox, event["event_id"], owner="daemon-a",
                lease_seconds=100, now=NOW,
            ),
            "renewed",
        )
        delivery = se._read_delivery(
            se.event_dir(self.outbox, event["event_id"]), event["event_id"]
        )
        self.assertEqual(
            delivery["lease"]["expires_at"],
            (NOW + timedelta(seconds=100)).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        )
        with self.assertRaises(se.SemanticEventError):
            se.renew_event(
                self.outbox, event["event_id"], owner="daemon-b",
                lease_seconds=100, now=NOW,
            )

    def test_ack_requires_turn_status(self) -> None:
        event = self.make_event()
        se.publish_event(self.outbox, event)
        se.claim_next_event(self.outbox, owner="d", lease_seconds=60, now=NOW)
        with self.assertRaises(se.SemanticEventError):
            se.acknowledge_event(
                self.outbox, event["event_id"], owner="d", now=NOW,
                thread_id="t", turn_id="u", turn_status="",
            )


class PostflightStateMachineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "state"
        self.digest = "sha256:" + "9" * 64
        self.other_digest = "sha256:" + "8" * 64
        self.event_id = make_event()["event_id"]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_begin_perform_complete_lifecycle(self) -> None:
        outcome = se.postflight_begin(
            self.state, self.event_id, terminal_digest=self.digest, owner="turn-1"
        )
        self.assertEqual(outcome, "begun")
        mid = se.postflight_check(self.state, self.event_id)
        self.assertFalse(mid["processed"])
        self.assertEqual(mid["state"], "in_progress")
        self.assertEqual(
            se.postflight_complete(self.state, self.event_id, owner="turn-1"),
            "completed",
        )
        done = se.postflight_check(self.state, self.event_id)
        self.assertTrue(done["processed"])
        self.assertEqual(
            se.postflight_complete(self.state, self.event_id, owner="turn-1"),
            "already_completed",
        )
        self.assertEqual(
            se.postflight_begin(
                self.state, self.event_id, terminal_digest=self.digest, owner="turn-2"
            ),
            "already_completed",
        )

    def test_concurrent_begins_allow_exactly_one_claim(self) -> None:
        outcomes: list[str] = []
        barrier = threading.Barrier(4)

        def worker(index: int) -> None:
            barrier.wait()
            outcomes.append(
                se.postflight_begin(
                    self.state,
                    self.event_id,
                    terminal_digest=self.digest,
                    owner=f"turn-{index}",
                )
            )

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("begun"), 1)
        self.assertEqual(outcomes.count("already_in_progress"), 3)

    def test_unknown_result_is_never_auto_taken_over(self) -> None:
        se.postflight_begin(
            self.state, self.event_id, terminal_digest=self.digest, owner="turn-1"
        )
        self.assertEqual(
            se.postflight_complete(self.state, self.event_id, owner="turn-2"),
            "not_owner",
        )
        self.assertEqual(
            se.postflight_begin(
                self.state, self.event_id, terminal_digest=self.digest, owner="turn-2"
            ),
            "already_in_progress",
        )
        self.assertEqual(
            se.postflight_reset(self.state, self.event_id, confirm=False),
            "confirmation_required",
        )
        self.assertEqual(
            se.postflight_check(self.state, self.event_id)["state"], "in_progress"
        )
        self.assertEqual(
            se.postflight_reset(self.state, self.event_id, confirm=True), "reset"
        )
        self.assertFalse(se.postflight_check(self.state, self.event_id)["processed"])

    def test_begin_digest_conflict_fails_closed(self) -> None:
        se.postflight_begin(
            self.state, self.event_id, terminal_digest=self.digest, owner="turn-1"
        )
        self.assertEqual(
            se.postflight_begin(
                self.state, self.event_id, terminal_digest=self.other_digest,
                owner="turn-2",
            ),
            "digest_conflict",
        )

    def test_legacy_marker_without_state_is_completed(self) -> None:
        path = se.postflight_path(self.state, self.event_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "schema": se.POSTFLIGHT_SCHEMA,
            "event_id": self.event_id,
            "terminal_digest": self.digest,
            "marked_at": stamp(NOW),
        }))
        result = se.postflight_check(self.state, self.event_id)
        self.assertTrue(result["processed"])

    def test_mark_rejects_in_progress_claim(self) -> None:
        se.postflight_begin(
            self.state, self.event_id, terminal_digest=self.digest, owner="turn-1"
        )
        self.assertEqual(
            se.postflight_mark(self.state, self.event_id, terminal_digest=self.digest),
            "already_in_progress",
        )


if __name__ == "__main__":
    unittest.main()

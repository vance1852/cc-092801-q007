"""候选项目交接流程的幂等控制离线测试。

覆盖成功重放、内容冲突、并发竞争、进程重启后重试和失败后重试。
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from portfolio_ops.api import JsonApplication
from portfolio_ops.clock import FrozenClock
from portfolio_ops.errors import Conflict, NotFound, ValidationFailed
from portfolio_ops.service import CollectionLogisticsService
from portfolio_ops.storage import connect


NOW = datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)


def handoff_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "handoff_id": "ho-001",
        "candidate_id": "candidate-001",
        "candidate_version": 3,
        "corridor_id": "transfer-east-1",
        "destination_center_id": "receiving-vault-b",
        "preservation_resource_kind": "preservation-box",
        "requested_units": "80000",
        "priority": 10,
        "planned_date": "2026-09-25",
        "idempotency_key": "handoff-key-001",
    }
    payload.update(overrides)
    return payload


def bootstrap(service: CollectionLogisticsService) -> None:
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    service.create_facility("plan", {"center_id": "collection-east", "name": "北部实验样本事件保藏中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
    service.create_facility("plan", {"center_id": "receiving-vault-b", "name": "沿海终端", "kind": "receiving-vault", "timezone": "Asia/Shanghai", "capacity_units": "800000"})
    service.create_route("plan", {"corridor_id": "transfer-east-1", "origin_center_id": "collection-east", "destination_center_id": "receiving-vault-b", "preservation_resource_kind": "preservation-box", "hourly_capacity": "100000", "delay_basis_points": 25, "response_minutes": 36})


def side_effect_counts(connection: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in ("handoffs", "handoff_tasks", "resource_reservations", "handoff_idempotency", "handoff_conflicts"):
        counts[table] = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    counts["audit_events"] = connection.execute("SELECT count(*) FROM traffic_audit_events").fetchone()[0]
    return counts


class HandoffIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(NOW)
        self.service = CollectionLogisticsService(self.connection, self.clock)
        bootstrap(self.service)

    def tearDown(self) -> None:
        self.connection.close()

    def test_first_request_creates_handoff_atomically(self) -> None:
        response = self.service.submit_handoff("dispatch", handoff_payload())
        self.assertEqual(response["handoff_id"], "ho-001")
        self.assertEqual(response["candidate_id"], "candidate-001")
        self.assertEqual(response["candidate_version"], 3)
        self.assertEqual(response["state"], "accepted")
        self.assertEqual([task["kind"] for task in response["tasks"]], ["intake-review", "execution-plan"])
        self.assertEqual(response["reservation"]["reserved_units"], "80000")
        self.assertEqual(response["submitted_at"], "2026-09-24T08:00:00Z")
        counts = side_effect_counts(self.connection)
        self.assertEqual(
            (counts["handoffs"], counts["handoff_tasks"], counts["resource_reservations"], counts["handoff_idempotency"], counts["handoff_conflicts"]),
            (1, 2, 1, 1, 0),
        )
        events = self.connection.execute(
            "SELECT event_type FROM traffic_audit_events WHERE entity_type='handoff' AND entity_id='ho-001'"
        ).fetchall()
        self.assertEqual([row[0] for row in events], ["handoff.accepted"])

    def test_identical_retry_replays_without_duplicating_side_effects(self) -> None:
        first = self.service.submit_handoff("dispatch", handoff_payload())
        baseline = side_effect_counts(self.connection)
        self.clock.advance(hours=1)
        replayed = self.service.submit_handoff("dispatch", handoff_payload())
        self.assertEqual(replayed, first)
        self.assertEqual(replayed["submitted_at"], "2026-09-24T08:00:00Z")
        self.assertEqual(side_effect_counts(self.connection), baseline)

    def test_conflicting_retry_is_rejected_recorded_and_queryable(self) -> None:
        first = self.service.submit_handoff("dispatch", handoff_payload())
        baseline = side_effect_counts(self.connection)
        with self.assertRaises(Conflict) as caught:
            self.service.submit_handoff("dispatch", handoff_payload(requested_units="81000"))
        self.assertIn("冲突记录", str(caught.exception))
        with self.assertRaises(Conflict):
            self.service.submit_handoff("dispatch", handoff_payload(candidate_id="candidate-002"))
        with self.assertRaises(Conflict):
            self.service.submit_handoff("dispatch", handoff_payload(candidate_version=4))
        conflicts = self.service.handoff_conflicts("audit")
        self.assertEqual(len(conflicts), 3)
        units = conflicts[0]
        self.assertEqual(units["idempotency_key"], "handoff-key-001")
        self.assertEqual(units["handoff_id"], "ho-001")
        self.assertEqual(units["actor_id"], "dispatch")
        self.assertEqual(units["created_at"], "2026-09-24T08:00:00Z")
        self.assertEqual(units["diff"], {"requested_units": {"stored": "80000", "received": "81000"}})
        self.assertEqual(conflicts[1]["diff"], {"candidate_id": {"stored": "candidate-001", "received": "candidate-002"}})
        self.assertEqual(conflicts[2]["diff"], {"candidate_version": {"stored": 3, "received": 4}})
        self.assertEqual(self.service.handoff_conflicts("audit", idempotency_key="handoff-key-001"), conflicts)
        self.assertEqual(self.service.handoff_conflicts("audit", idempotency_key="other-key"), [])
        self.assertEqual(self.service.handoff_conflict("audit", units["conflict_id"]), units)
        counts = side_effect_counts(self.connection)
        self.assertEqual(
            (counts["handoffs"], counts["handoff_tasks"], counts["resource_reservations"], counts["handoff_idempotency"], counts["handoff_conflicts"]),
            (1, 2, 1, 1, 3),
        )
        self.assertEqual(counts["audit_events"], baseline["audit_events"] + 3)
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.assertEqual(self.service.submit_handoff("dispatch", handoff_payload()), first)

    def test_invalid_payload_is_rejected_without_residue(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.submit_handoff("dispatch", handoff_payload(candidate_version=0))
        with self.assertRaises(ValidationFailed):
            self.service.submit_handoff("dispatch", handoff_payload(preservation_resource_kind="unknown-kind"))
        counts = side_effect_counts(self.connection)
        self.assertEqual(
            (counts["handoffs"], counts["handoff_tasks"], counts["resource_reservations"], counts["handoff_idempotency"], counts["handoff_conflicts"]),
            (0, 0, 0, 0, 0),
        )

    def test_reference_failure_allows_later_retry_with_same_key(self) -> None:
        with self.assertRaises(NotFound):
            self.service.submit_handoff("dispatch", handoff_payload(destination_center_id="receiving-vault-c"))
        counts = side_effect_counts(self.connection)
        self.assertEqual((counts["handoffs"], counts["handoff_idempotency"], counts["handoff_conflicts"]), (0, 0, 0))
        self.service.create_facility("plan", {"center_id": "receiving-vault-c", "name": "内陆终端", "kind": "receiving-vault", "timezone": "Asia/Shanghai", "capacity_units": "100000"})
        response = self.service.submit_handoff("dispatch", handoff_payload(destination_center_id="receiving-vault-c"))
        self.assertEqual(response["state"], "accepted")
        counts = side_effect_counts(self.connection)
        self.assertEqual((counts["handoffs"], counts["handoff_tasks"], counts["resource_reservations"]), (1, 2, 1))

    def test_mid_transaction_crash_rolls_back_and_retry_succeeds(self) -> None:
        service = self.service

        class CrashingService(CollectionLogisticsService):
            crashed = False

            def _audit(self, entity_type, entity_id, event_type, actor_id, payload):  # noqa: ANN001, ANN202
                if event_type == "handoff.accepted" and not self.crashed:
                    self.crashed = True
                    raise RuntimeError("模拟交接事务中的进程崩溃")
                return super()._audit(entity_type, entity_id, event_type, actor_id, payload)

        crashing = CrashingService(self.connection, self.clock)
        baseline = side_effect_counts(self.connection)
        with self.assertRaises(RuntimeError):
            crashing.submit_handoff("dispatch", handoff_payload())
        self.assertEqual(side_effect_counts(self.connection), baseline)
        response = service.submit_handoff("dispatch", handoff_payload())
        self.assertEqual(response["state"], "accepted")
        counts = side_effect_counts(self.connection)
        self.assertEqual(
            (counts["handoffs"], counts["handoff_tasks"], counts["resource_reservations"], counts["handoff_idempotency"]),
            (1, 2, 1, 1),
        )
        self.assertEqual(counts["audit_events"], baseline["audit_events"] + 1)


class HandoffRestartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "handoff.sqlite3"
        connection, service = self._open_service()
        bootstrap(service)
        connection.close()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _open_service(self) -> tuple[sqlite3.Connection, CollectionLogisticsService]:
        connection = connect(self.path)
        return connection, CollectionLogisticsService(connection, FrozenClock(NOW))

    def test_retry_after_process_restart_replays_and_keeps_conflict_history(self) -> None:
        connection, service = self._open_service()
        first = service.submit_handoff("dispatch", handoff_payload())
        connection.close()
        connection, service = self._open_service()
        try:
            replayed = service.submit_handoff("dispatch", handoff_payload())
            self.assertEqual(replayed, first)
            counts = side_effect_counts(connection)
            self.assertEqual(
                (counts["handoffs"], counts["handoff_tasks"], counts["resource_reservations"], counts["handoff_idempotency"]),
                (1, 2, 1, 1),
            )
            with self.assertRaises(Conflict):
                service.submit_handoff("dispatch", handoff_payload(candidate_version=4))
            self.assertEqual(len(service.handoff_conflicts("audit")), 1)
        finally:
            connection.close()
        connection, service = self._open_service()
        try:
            self.assertEqual(service.submit_handoff("dispatch", handoff_payload()), first)
            conflicts = service.handoff_conflicts("audit")
            self.assertEqual(len(conflicts), 1)
            self.assertEqual(conflicts[0]["diff"], {"candidate_version": {"stored": 3, "received": 4}})
            self.assertTrue(service.audit_chain("audit")["valid"])
        finally:
            connection.close()


class HandoffConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "handoff.sqlite3"
        connection, service = self._open_service()
        bootstrap(service)
        connection.close()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _open_service(self) -> tuple[sqlite3.Connection, CollectionLogisticsService]:
        connection = connect(self.path)
        return connection, CollectionLogisticsService(connection, FrozenClock(NOW))

    def _submit_in_thread(self, payload: dict[str, object], barrier: threading.Barrier, results: list[tuple[str, object]]) -> None:
        connection, service = self._open_service()
        try:
            barrier.wait(timeout=10)
            results.append(("ok", service.submit_handoff("dispatch", payload)))
        except Conflict as exc:
            results.append(("conflict", str(exc)))
        finally:
            connection.close()

    def _race(self, *payloads: dict[str, object]) -> list[tuple[str, object]]:
        barrier = threading.Barrier(len(payloads))
        results: list[tuple[str, object]] = []
        threads = [
            threading.Thread(target=self._submit_in_thread, args=(payload, barrier, results))
            for payload in payloads
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(len(results), len(payloads))
        return results

    def test_concurrent_identical_requests_create_single_handoff(self) -> None:
        results = self._race(handoff_payload(), handoff_payload())
        kinds = sorted(kind for kind, _ in results)
        self.assertEqual(kinds, ["ok", "ok"])
        responses = [response for _, response in results]
        self.assertEqual(responses[0], responses[1])
        connection, service = self._open_service()
        try:
            counts = side_effect_counts(connection)
            self.assertEqual(
                (counts["handoffs"], counts["handoff_tasks"], counts["resource_reservations"], counts["handoff_idempotency"], counts["handoff_conflicts"]),
                (1, 2, 1, 1, 0),
            )
            events = connection.execute(
                "SELECT event_type FROM traffic_audit_events WHERE entity_type='handoff' AND entity_id='ho-001'"
            ).fetchall()
            self.assertEqual([row[0] for row in events], ["handoff.accepted"])
        finally:
            connection.close()

    def test_concurrent_conflicting_requests_record_exactly_one_conflict(self) -> None:
        results = self._race(handoff_payload(), handoff_payload(requested_units="81000"))
        kinds = sorted(kind for kind, _ in results)
        self.assertEqual(kinds, ["conflict", "ok"])
        winner = next(response for kind, response in results if kind == "ok")
        connection, service = self._open_service()
        try:
            counts = side_effect_counts(connection)
            self.assertEqual(
                (counts["handoffs"], counts["handoff_tasks"], counts["resource_reservations"], counts["handoff_idempotency"], counts["handoff_conflicts"]),
                (1, 2, 1, 1, 1),
            )
            conflicts = service.handoff_conflicts("audit")
            self.assertEqual(len(conflicts), 1)
            diff = conflicts[0]["diff"]
            self.assertEqual(set(diff), {"requested_units"})
            self.assertEqual(diff["requested_units"]["stored"], winner["requested_units"])
            self.assertEqual(
                {diff["requested_units"]["stored"], diff["requested_units"]["received"]},
                {"80000", "81000"},
            )
        finally:
            connection.close()


class HandoffApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.service = CollectionLogisticsService(self.connection, FrozenClock(NOW))
        bootstrap(self.service)
        self.app = JsonApplication(self.service)

    def tearDown(self) -> None:
        self.connection.close()

    def test_handoff_routes_and_conflict_query(self) -> None:
        body = json.dumps(handoff_payload()).encode()
        created = self.app.handle("POST", "/handoffs", {"X-Actor-Id": "dispatch"}, body)
        self.assertEqual(created.status, 201)
        replayed = self.app.handle("POST", "/handoffs", {"X-Actor-Id": "dispatch"}, body)
        self.assertEqual(replayed.status, 201)
        self.assertEqual(replayed.body, created.body)
        conflict_body = json.dumps(handoff_payload(requested_units="81000")).encode()
        conflict = self.app.handle("POST", "/handoffs", {"X-Actor-Id": "dispatch"}, conflict_body)
        self.assertEqual(conflict.status, 409)
        self.assertEqual(conflict.body["error"]["code"], "conflict")
        detail = self.app.handle("GET", "/handoffs/ho-001", {"X-Actor-Id": "audit"})
        self.assertEqual(detail.status, 200)
        self.assertEqual(detail.body["handoff"]["candidate_id"], "candidate-001")
        self.assertEqual(len(detail.body["tasks"]), 2)
        self.assertEqual(detail.body["reservation"]["reserved_units"], "80000")
        listing = self.app.handle("GET", "/handoff_conflicts", {"X-Actor-Id": "audit"})
        self.assertEqual(listing.status, 200)
        self.assertEqual(len(listing.body["conflicts"]), 1)
        record = listing.body["conflicts"][0]
        self.assertEqual(record["actor_id"], "dispatch")
        self.assertEqual(record["diff"], {"requested_units": {"stored": "80000", "received": "81000"}})
        single = self.app.handle("GET", f"/handoff_conflicts/{record['conflict_id']}", {"X-Actor-Id": "audit"})
        self.assertEqual(single.status, 200)
        self.assertEqual(single.body, record)
        filtered = self.app.handle("GET", "/handoff_conflicts?idempotency_key=handoff-key-001", {"X-Actor-Id": "audit"})
        self.assertEqual(len(filtered.body["conflicts"]), 1)
        empty = self.app.handle("GET", "/handoff_conflicts?idempotency_key=other", {"X-Actor-Id": "audit"})
        self.assertEqual(empty.body["conflicts"], [])

    def test_handoff_permissions_and_missing_records(self) -> None:
        body = json.dumps(handoff_payload()).encode()
        denied = self.app.handle("POST", "/handoffs", {"X-Actor-Id": "plan"}, body)
        self.assertEqual(denied.status, 403)
        self.assertEqual(denied.body["error"]["code"], "forbidden")
        created = self.app.handle("POST", "/handoffs", {"X-Actor-Id": "dispatch"}, body)
        self.assertEqual(created.status, 201)
        read_denied = self.app.handle("GET", "/handoff_conflicts", {"X-Actor-Id": "risk"})
        self.assertEqual(read_denied.status, 403)
        missing_handoff = self.app.handle("GET", "/handoffs/unknown", {"X-Actor-Id": "audit"})
        self.assertEqual(missing_handoff.status, 404)
        missing_conflict = self.app.handle("GET", "/handoff_conflicts/99", {"X-Actor-Id": "audit"})
        self.assertEqual(missing_conflict.status, 404)


if __name__ == "__main__":
    unittest.main()

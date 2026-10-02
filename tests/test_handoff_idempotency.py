"""交接计划幂等语义的离线测试：成功重放、内容冲突、并发竞争与失败后重试。"""

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
from portfolio_ops.errors import IdempotencyConflict, InvalidState, NotFound
from portfolio_ops.planning import canonical_json, digest
from portfolio_ops.service import CollectionLogisticsService
from portfolio_ops.storage import connect


CLOCK_TIME = datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)


def handoff_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "dispatch_id": "nom-1",
        "corridor_id": "transfer-east-1",
        "specimen_event_id": "herbarium-room",
        "duty_date": "2026-09-25",
        "requested_units": "80000",
        "priority": 10,
        "idempotency_key": "key-1",
    }
    payload.update(overrides)
    return payload


def seed_catalog(service: CollectionLogisticsService) -> None:
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    service.create_facility("plan", {"center_id": "collection-east", "name": "北部实验样本事件保藏中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_units": "500000"})
    service.create_facility("plan", {"center_id": "receiving-vault-b", "name": "沿海终端", "kind": "receiving-vault", "timezone": "Asia/Shanghai", "capacity_units": "800000"})
    service.create_route("plan", {"corridor_id": "transfer-east-1", "origin_center_id": "collection-east", "destination_center_id": "receiving-vault-b", "preservation_resource_kind": "preservation-box", "hourly_capacity": "100000", "delay_basis_points": 25, "response_minutes": 36})


class HandoffIdempotencyTests(unittest.TestCase):
    """单连接内存库上的幂等语义。"""

    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(CLOCK_TIME)
        self.service = CollectionLogisticsService(self.connection, self.clock)
        seed_catalog(self.service)

    def tearDown(self) -> None:
        self.connection.close()

    def count(self, sql: str) -> int:
        return int(self.connection.execute(sql).fetchone()[0])

    def test_identical_retry_replays_without_duplicate_side_effects(self) -> None:
        first = self.service.submit_dispatch("dispatch", handoff_payload())
        replay = self.service.submit_dispatch("dispatch", handoff_payload())
        self.assertEqual(first, replay)
        # 重试不产生第二份交接记录、幂等记录或审计事件
        self.assertEqual(self.count("SELECT count(*) FROM dispatch_requests"), 1)
        self.assertEqual(self.count("SELECT count(*) FROM traffic_idempotency"), 1)
        self.assertEqual(
            self.count("SELECT count(*) FROM traffic_audit_events WHERE event_type='dispatch_request.submitted'"),
            1,
        )
        self.assertEqual(self.service.idempotency_conflicts("audit")["conflicts"], [])

    def test_conflicting_retry_is_rejected_recorded_and_queryable(self) -> None:
        created = self.service.submit_dispatch("dispatch", handoff_payload())
        changed = handoff_payload(specimen_event_id="herbarium-room-b", requested_units="81000")
        with self.assertRaises(IdempotencyConflict) as caught:
            self.service.submit_dispatch("dispatch", changed)
        conflict_id = caught.exception.conflict_id
        self.assertIsNotNone(conflict_id)
        # 拒绝执行：不新增交接记录与提交审计事件
        self.assertEqual(self.count("SELECT count(*) FROM dispatch_requests"), 1)
        self.assertEqual(
            self.count("SELECT count(*) FROM traffic_audit_events WHERE event_type='dispatch_request.submitted'"),
            1,
        )
        # 冲突审计记录包含差异摘要、请求方和时间
        conflicts = self.service.idempotency_conflicts("audit")["conflicts"]
        self.assertEqual(len(conflicts), 1)
        record = conflicts[0]
        self.assertEqual(record["conflict_id"], conflict_id)
        self.assertEqual(record["scope"], "dispatch_request")
        self.assertEqual(record["idempotency_key"], "key-1")
        self.assertEqual(record["actor_id"], "dispatch")
        self.assertEqual(record["created_at"], "2026-09-24T08:00:00Z")
        self.assertNotEqual(record["stored_request_sha256"], record["incoming_request_sha256"])
        changed_fields = {item["field"]: item for item in record["diff_summary"]["changed_fields"]}
        self.assertEqual(changed_fields["specimen_event_id"]["stored"], "herbarium-room")
        self.assertEqual(changed_fields["specimen_event_id"]["incoming"], "herbarium-room-b")
        self.assertEqual(changed_fields["requested_units"]["stored"], "80000")
        self.assertEqual(changed_fields["requested_units"]["incoming"], "81000")
        self.assertNotIn("priority", changed_fields)
        # 冲突同时进入哈希审计链，链保持有效
        self.assertEqual(
            self.count("SELECT count(*) FROM traffic_audit_events WHERE event_type='dispatch_request.idempotency_conflict'"),
            1,
        )
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        # 每次冲突重试都追加一条记录，原幂等结果仍可重放
        with self.assertRaises(IdempotencyConflict):
            self.service.submit_dispatch("dispatch", handoff_payload(requested_units="82000"))
        self.assertEqual(len(self.service.idempotency_conflicts("audit")["conflicts"]), 2)
        self.assertEqual(self.service.submit_dispatch("dispatch", handoff_payload()), created)

    def test_conflicts_are_queryable_over_http(self) -> None:
        app = JsonApplication(self.service)
        created = app.handle("POST", "/dispatch_requests", {"X-Actor-Id": "dispatch"}, json.dumps(handoff_payload()).encode())
        self.assertEqual(created.status, 201)
        rejected = app.handle(
            "POST",
            "/dispatch_requests",
            {"X-Actor-Id": "dispatch"},
            json.dumps(handoff_payload(requested_units="81000")).encode(),
        )
        self.assertEqual(rejected.status, 409)
        self.assertEqual(rejected.body["error"]["code"], "idempotency_conflict")
        listed = app.handle("GET", "/idempotency_conflicts", {"X-Actor-Id": "audit"})
        self.assertEqual(listed.status, 200)
        self.assertEqual(len(listed.body["conflicts"]), 1)
        self.assertEqual(listed.body["conflicts"][0]["actor_id"], "dispatch")
        filtered = app.handle("GET", "/idempotency_conflicts?idempotency_key=key-1", {"X-Actor-Id": "audit"})
        self.assertEqual(len(filtered.body["conflicts"]), 1)
        empty = app.handle("GET", "/idempotency_conflicts?idempotency_key=other-key", {"X-Actor-Id": "audit"})
        self.assertEqual(empty.body["conflicts"], [])
        forbidden = app.handle("GET", "/idempotency_conflicts", {"X-Actor-Id": "dispatch"})
        self.assertEqual(forbidden.status, 403)

    def test_missing_route_failure_does_not_burn_idempotency_key(self) -> None:
        with self.assertRaises(NotFound):
            self.service.submit_dispatch("dispatch", handoff_payload(corridor_id="missing-route"))
        self.assertEqual(self.count("SELECT count(*) FROM traffic_idempotency"), 0)
        created = self.service.submit_dispatch("dispatch", handoff_payload())
        self.assertEqual(created["state"], "submitted")
        self.assertEqual(self.count("SELECT count(*) FROM dispatch_requests"), 1)

    def test_invalid_state_failure_allows_later_retry(self) -> None:
        self.connection.execute("UPDATE road_corridors SET state='suspended' WHERE corridor_id='transfer-east-1'")
        with self.assertRaises(InvalidState):
            self.service.submit_dispatch("dispatch", handoff_payload())
        self.assertEqual(self.count("SELECT count(*) FROM traffic_idempotency"), 0)
        self.connection.execute("UPDATE road_corridors SET state='active' WHERE corridor_id='transfer-east-1'")
        created = self.service.submit_dispatch("dispatch", handoff_payload())
        self.assertEqual(created["state"], "submitted")
        self.assertEqual(self.count("SELECT count(*) FROM dispatch_requests"), 1)


class HandoffConcurrencyTests(unittest.TestCase):
    """文件库与多连接模拟并发进程和进程重启。"""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "handoff.sqlite3"
        self.clock = FrozenClock(CLOCK_TIME)
        connection = connect(self.database)
        try:
            seed_catalog(CollectionLogisticsService(connection, self.clock))
        finally:
            connection.close()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _submit_on_new_connection(self, payload: dict[str, object], results: list, index: int) -> None:
        connection = connect(self.database)
        try:
            service = CollectionLogisticsService(connection, self.clock)
            results[index] = ("ok", service.submit_dispatch("dispatch", payload))
        except Exception as exc:  # noqa: BLE001 - 收集线程内异常供主线程断言
            results[index] = ("error", exc)
        finally:
            connection.close()

    def run_concurrently(self, payloads: list[dict[str, object]]) -> list:
        results: list = [None] * len(payloads)
        threads = [
            threading.Thread(target=self._submit_on_new_connection, args=(payload, results, index))
            for index, payload in enumerate(payloads)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return results

    def count_in_database(self, sql: str) -> int:
        connection = connect(self.database)
        try:
            return int(connection.execute(sql).fetchone()[0])
        finally:
            connection.close()

    def test_concurrent_identical_submissions_replay_single_result(self) -> None:
        results = self.run_concurrently([handoff_payload(), handoff_payload()])
        self.assertEqual([status for status, _ in results], ["ok", "ok"])
        self.assertEqual(results[0][1], results[1][1])
        self.assertEqual(self.count_in_database("SELECT count(*) FROM dispatch_requests"), 1)
        self.assertEqual(self.count_in_database("SELECT count(*) FROM traffic_idempotency"), 1)
        self.assertEqual(
            self.count_in_database("SELECT count(*) FROM traffic_audit_events WHERE event_type='dispatch_request.submitted'"),
            1,
        )

    def test_concurrent_conflicting_submissions_record_single_conflict(self) -> None:
        results = self.run_concurrently([handoff_payload(), handoff_payload(requested_units="81000")])
        statuses = sorted(status for status, _ in results)
        self.assertEqual(statuses, ["error", "ok"])
        errors = [value for status, value in results if status == "error"]
        self.assertIsInstance(errors[0], IdempotencyConflict)
        # 无论哪个请求先落地，只产生一份交接记录和一条冲突审计
        self.assertEqual(self.count_in_database("SELECT count(*) FROM dispatch_requests"), 1)
        self.assertEqual(self.count_in_database("SELECT count(*) FROM traffic_idempotency"), 1)
        self.assertEqual(self.count_in_database("SELECT count(*) FROM traffic_idempotency_conflicts"), 1)

    def test_replay_and_conflict_survive_process_restart(self) -> None:
        connection = connect(self.database)
        try:
            first = CollectionLogisticsService(connection, self.clock).submit_dispatch("dispatch", handoff_payload())
        finally:
            connection.close()
        # 模拟进程重启：新连接、新服务实例，相同请求重放首次结果
        connection = connect(self.database)
        try:
            service = CollectionLogisticsService(connection, self.clock)
            self.assertEqual(service.submit_dispatch("dispatch", handoff_payload()), first)
            with self.assertRaises(IdempotencyConflict):
                service.submit_dispatch("dispatch", handoff_payload(priority=20))
            conflicts = service.idempotency_conflicts("audit")["conflicts"]
            self.assertEqual(len(conflicts), 1)
            changed_fields = conflicts[0]["diff_summary"]["changed_fields"]
            self.assertEqual([(item["field"], item["stored"], item["incoming"]) for item in changed_fields], [("priority", 10, 20)])
        finally:
            connection.close()
        # 再次重启后冲突记录与幂等结果仍可查询
        connection = connect(self.database)
        try:
            service = CollectionLogisticsService(connection, self.clock)
            self.assertEqual(len(service.idempotency_conflicts("audit")["conflicts"]), 1)
            self.assertEqual(service.submit_dispatch("dispatch", handoff_payload()), first)
            self.assertTrue(service.audit_chain("audit")["valid"])
        finally:
            connection.close()

    def test_legacy_idempotency_table_is_migrated_on_open(self) -> None:
        legacy = Path(self.directory.name) / "legacy.sqlite3"
        connection = sqlite3.connect(str(legacy), isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(
                "CREATE TABLE traffic_idempotency (scope TEXT NOT NULL, idempotency_key TEXT NOT NULL, "
                "request_sha256 TEXT NOT NULL, response_json TEXT NOT NULL, created_at TEXT NOT NULL, "
                "PRIMARY KEY(scope, idempotency_key))"
            )
            payload = handoff_payload()
            response = {"dispatch_id": "nom-1", "corridor_id": "transfer-east-1", "state": "submitted", "revision": 1}
            connection.execute(
                "INSERT INTO traffic_idempotency VALUES('dispatch_request','key-1',?,?,?)",
                (digest(payload), canonical_json(response), "2026-09-24T07:00:00Z"),
            )
        finally:
            connection.close()
        # 旧库打开后自动补齐 request_json 列，旧行继续提供幂等语义
        connection = connect(legacy)
        try:
            service = CollectionLogisticsService(connection, self.clock)
            service.create_user("dispatch", "dispatch", "dispatcher")
            service.create_user("audit", "audit", "auditor")
            columns = {row[1] for row in connection.execute("PRAGMA table_info(traffic_idempotency)")}
            self.assertIn("request_json", columns)
            self.assertEqual(service.submit_dispatch("dispatch", handoff_payload()), response)
            # 旧行没有保存请求内容，冲突记录退化为仅摘要哈希对比
            with self.assertRaises(IdempotencyConflict):
                service.submit_dispatch("dispatch", handoff_payload(requested_units="81000"))
            conflicts = service.idempotency_conflicts("audit")["conflicts"]
            self.assertEqual(len(conflicts), 1)
            self.assertFalse(conflicts[0]["diff_summary"]["stored_request_available"])
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()

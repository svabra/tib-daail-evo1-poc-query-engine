from __future__ import annotations

from pathlib import Path
import sys
import threading
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_realtime_service_components():
    from bit_data_workbench.backend.service import (
        REALTIME_TOPIC_ORDER,
        WorkbenchService,
    )

    return REALTIME_TOPIC_ORDER, WorkbenchService


def build_realtime_service():
    REALTIME_TOPIC_ORDER, WorkbenchService = (
        import_realtime_service_components()
    )
    service = WorkbenchService.__new__(WorkbenchService)
    service._condition = threading.Condition()
    service._realtime_signal_version = 0
    service._realtime_snapshots = {}
    service._realtime_topic_versions = {
        topic: 0 for topic in REALTIME_TOPIC_ORDER
    }
    service._client_connections_version = 0
    service._active_realtime_clients = 0
    return REALTIME_TOPIC_ORDER, service


class RealtimeConnectionsTests(unittest.TestCase):
    def test_register_and_unregister_realtime_clients_publish_connection_count(
        self,
    ) -> None:
        REALTIME_TOPIC_ORDER, service = build_realtime_service()

        connected = service.register_realtime_client()

        self.assertEqual(connected, {"version": 1, "count": 1})
        self.assertEqual(service._active_realtime_clients, 1)
        self.assertEqual(
            service._realtime_topic_versions["client-connections"],
            1,
        )
        self.assertEqual(
            service._realtime_snapshots["client-connections"],
            {"version": 1, "count": 1},
        )

        disconnected = service.unregister_realtime_client()

        self.assertEqual(disconnected, {"version": 2, "count": 0})
        self.assertEqual(service._active_realtime_clients, 0)
        self.assertEqual(
            service._realtime_topic_versions["client-connections"],
            2,
        )
        self.assertEqual(
            service._realtime_snapshots["client-connections"],
            {"version": 2, "count": 0},
        )

    def test_wait_for_realtime_updates_returns_only_changed_topics(
        self,
    ) -> None:
        REALTIME_TOPIC_ORDER, service = build_realtime_service()

        service._set_realtime_snapshot_locked(
            "query-jobs",
            {"version": 1, "items": []},
            notify=False,
        )

        updates = service.wait_for_realtime_updates(
            {topic: 0 for topic in REALTIME_TOPIC_ORDER},
            timeout=0,
        )

        self.assertEqual(
            updates,
            [{"topic": "query-jobs", "snapshot": {"version": 1, "items": []}}],
        )

    def test_wait_for_realtime_updates_supports_service_consumption_topic(
        self,
    ) -> None:
        REALTIME_TOPIC_ORDER, service = build_realtime_service()

        service._set_realtime_snapshot_locked(
            "service-consumption",
            {
                "version": 4,
                "latest": {"timestampUtc": "2026-04-16T08:00:00+00:00"},
                "status": {"nodeMetricsAvailable": True},
                "financialSummary": {"annualBudgetChf": 120000.0},
            },
            notify=False,
        )

        updates = service.wait_for_realtime_updates(
            {topic: 0 for topic in REALTIME_TOPIC_ORDER},
            timeout=0,
        )

        self.assertEqual(
            updates,
            [
                {
                    "topic": "service-consumption",
                    "snapshot": {
                        "version": 4,
                        "latest": {"timestampUtc": "2026-04-16T08:00:00+00:00"},
                        "status": {"nodeMetricsAvailable": True},
                        "financialSummary": {"annualBudgetChf": 120000.0},
                    },
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()

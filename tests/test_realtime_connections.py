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
    from bit_data_workbench.backend.service import REALTIME_TOPIC_ORDER, WorkbenchService

    return REALTIME_TOPIC_ORDER, WorkbenchService


class RealtimeConnectionsTests(unittest.TestCase):
    def test_register_and_unregister_realtime_clients_publish_connection_count(self) -> None:
        REALTIME_TOPIC_ORDER, WorkbenchService = import_realtime_service_components()
        service = WorkbenchService.__new__(WorkbenchService)
        service._condition = threading.Condition()
        service._realtime_signal_version = 0
        service._realtime_snapshots = {}
        service._realtime_topic_versions = {topic: 0 for topic in REALTIME_TOPIC_ORDER}
        service._client_connections_version = 0
        service._active_realtime_clients = 0

        connected = service.register_realtime_client()

        self.assertEqual(connected, {"version": 1, "count": 1})
        self.assertEqual(service._active_realtime_clients, 1)
        self.assertEqual(service._realtime_topic_versions["client-connections"], 1)
        self.assertEqual(
            service._realtime_snapshots["client-connections"],
            {"version": 1, "count": 1},
        )

        disconnected = service.unregister_realtime_client()

        self.assertEqual(disconnected, {"version": 2, "count": 0})
        self.assertEqual(service._active_realtime_clients, 0)
        self.assertEqual(service._realtime_topic_versions["client-connections"], 2)
        self.assertEqual(
            service._realtime_snapshots["client-connections"],
            {"version": 2, "count": 0},
        )


if __name__ == "__main__":
    unittest.main()
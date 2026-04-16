from __future__ import annotations

from pathlib import Path
import json
import sys
import unittest

from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import service_consumption_state as api_service_consumption_state  # noqa: E402
from bit_data_workbench.web.router import service_consumption_page  # noqa: E402


class FakeWorkbenchService:
    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": "0.5.1",
            "hostname": "test-host",
            "pod_name": "bdw-pod",
            "pod_namespace": "bdw-namespace",
            "pod_ip": "127.0.0.1",
            "node_name": "bdw-node",
            "duckdb_database": "/tmp/workspace.duckdb",
            "timestamp_utc": "2026-04-16T00:00:00+00:00",
        }

    def catalogs(self):
        return []

    def notebooks(self):
        return []

    def notebook_tree(self):
        return []

    def source_options(self):
        return []

    def data_generators(self):
        return []

    def runbook_tree(self):
        return []

    def completion_schema(self):
        return {}

    def service_consumption_state(
        self,
        *,
        window: str = "24h",
    ) -> dict[str, object]:
        return {
            "version": 3,
            "window": window,
            "latest": {
                "timestampUtc": "2026-04-16T08:00:00+00:00",
                "cpu": {
                    "app": {"coresUsed": 1.1, "percentOfLimit": 14.0, "limitCores": 8.0},
                    "node": {"coresUsed": 2.2, "percentOfCapacity": 18.0, "capacityCores": 12.0},
                },
                "memory": {
                    "app": {
                        "bytesUsed": 2_048,
                        "percentOfLimit": 40.0,
                        "limitBytes": 4_096,
                    },
                    "node": {
                        "bytesUsed": 8_192,
                        "percentOfCapacity": 33.0,
                        "capacityBytes": 24_576,
                    },
                },
                "s3": {
                    "totalBytes": 4_096,
                    "bucketCount": 2,
                    "sampledAtUtc": "2026-04-16T08:00:00+00:00",
                },
                "persistentVolume": {
                    "bytesUsed": 1_024,
                    "bytesCapacity": 10_737_418_240,
                    "percentOfCapacity": 0.01,
                    "mountPath": "/workspace/service-consumption",
                },
                "status": {
                    "nodeMetrics": {"available": True, "detail": "ok"},
                    "s3Metrics": {"available": True, "detail": "ok"},
                    "persistentVolumeMetrics": {"available": True, "detail": "ok"},
                },
                "nodeName": "bdw-node",
                "podName": "bdw-pod",
                "podNamespace": "bdw-namespace",
            },
            "status": {
                "nodeMetrics": {"available": True, "detail": "ok"},
                "s3Metrics": {"available": True, "detail": "ok"},
                "persistentVolumeMetrics": {"available": True, "detail": "ok"},
                "nodeMetricsAvailable": True,
                "s3MetricsAvailable": True,
                "persistentVolumeMetricsAvailable": True,
                "s3SampledAtUtc": "2026-04-16T08:00:00+00:00",
                "persistentVolumeMountPath": "/workspace/service-consumption",
            },
            "topology": {
                "copy": "The current PoC runs API, backend, frontend, and query execution on a single node.",
                "scaleOutCopy": "Query nodes will scale out automatically under higher query pressure once DAAIFL goes live.",
                "nodeName": "bdw-node",
                "podName": "bdw-pod",
                "podNamespace": "bdw-namespace",
            },
            "cpuHistory": {"timestamps": ["2026-04-16T08:00:00+00:00"], "service": [1.1], "node": [2.2]},
            "memoryHistory": {"timestamps": ["2026-04-16T08:00:00+00:00"], "service": [2_048], "node": [8_192]},
            "s3History": {"timestamps": ["2026-04-16T08:00:00+00:00"], "values": [4_096]},
            "persistentVolumeHistory": {"timestamps": ["2026-04-16T08:00:00+00:00"], "values": [1_024]},
        }


def build_request(path: str, *, partial: bool) -> Request:
    headers = []
    if partial:
        headers.append((b"hx-request", b"true"))
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": headers,
        }
    )


class ServiceConsumptionRouteTests(unittest.TestCase):
    def test_service_consumption_partial_renders_expected_surface(self) -> None:
        response = service_consumption_page(
            request=build_request("/service-consumption", partial=True),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-service-consumption-page', body)
        self.assertIn('data-service-consumption-window="24h"', body)
        self.assertIn('data-service-consumption-cpu-legend', body)
        self.assertIn('data-service-consumption-memory-legend', body)
        self.assertIn('data-service-consumption-s3-legend', body)
        self.assertIn('data-service-consumption-pv-legend', body)
        self.assertIn('data-service-consumption-pv-chart', body)
        self.assertNotIn('data-service-consumption-year-select', body)
        self.assertIn(
            "Query nodes will scale out automatically under higher query pressure once DAAIFL goes live.",
            body,
        )

    def test_service_consumption_full_page_hides_sidebar(self) -> None:
        response = service_consumption_page(
            request=build_request("/service-consumption", partial=False),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("DAAIFL Service Consumption", body)
        self.assertIn("shell-sidebar-hidden", body)
        self.assertIn('data-open-service-consumption', body)
        self.assertNotIn('data-service-consumption-card="cpu"', body)
        self.assertNotIn('data-service-consumption-card="memory"', body)
        self.assertNotIn('data-service-consumption-card="s3"', body)

    def test_service_consumption_api_route_returns_payload(self) -> None:
        response = api_service_consumption_state(
            window="48h",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["window"], "48h")
        self.assertEqual(payload["version"], 3)
        self.assertEqual(payload["topology"]["nodeName"], "bdw-node")
        self.assertEqual(payload["s3History"]["values"], [4_096])
        self.assertEqual(payload["persistentVolumeHistory"]["values"], [1_024])


if __name__ == "__main__":
    unittest.main()

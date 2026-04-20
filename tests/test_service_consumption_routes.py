from __future__ import annotations

from pathlib import Path
import json
import sys
import unittest

from fastapi import HTTPException
from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import service_consumption_state as api_service_consumption_state  # noqa: E402
from bit_data_workbench.api.router import update_service_consumption_budget as api_update_service_consumption_budget  # noqa: E402
from bit_data_workbench.api.router import ServiceConsumptionBudgetPayload  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402
from bit_data_workbench.web.router import service_consumption_page  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


class FakeWorkbenchService:
    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": CURRENT_VERSION,
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
                    "bytesProvisioned": 10_737_418_240,
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
            "financial": {
                "currency": "CHF",
                "yearUtc": 2026,
                "annualBudgetChf": 120_000.0,
                "budgetUpdatedAtUtc": "2026-04-16T08:00:00+00:00",
                "spentYearToDateChf": 125.2,
                "remainingBudgetChf": 119_874.8,
                "forecastYearEndChf": 1_220.0,
                "breakdownYearToDate": {
                    "computeChf": 110.0,
                    "applicationChf": 7.0,
                    "s3Chf": 10.0,
                    "persistentVolumeChf": 5.2,
                    "pgChf": 3.0,
                },
                "servicesTotalChf": 125.2,
                "services": [
                    {
                        "key": "container",
                        "label": "Container Service",
                        "subtitle": "CPU and RAM share on the active node",
                        "costYtdChf": 100.0,
                        "shareOfTotalPercent": 79.9,
                        "status": {"state": "available", "label": "Available"},
                        "details": {
                            "cpuChf": 60.0,
                            "ramChf": 40.0,
                            "nodeChfPerHour": 2.4,
                            "cpuWeight": 0.5,
                            "ramWeight": 0.5,
                            "costYtdChf": 100.0,
                        },
                    },
                    {
                        "key": "application",
                        "label": "DAAIFL Application Service",
                        "subtitle": "Shared application fee prorated across the year",
                        "costYtdChf": 7.0,
                        "shareOfTotalPercent": 5.6,
                        "status": {"state": "available", "label": "Available"},
                        "details": {
                            "annualFeeChf": 500.0,
                            "monthlyFeeChf": 41.67,
                            "costYtdChf": 7.0,
                        },
                    },
                    {
                        "key": "filesystem",
                        "label": "FileSystem Service",
                        "subtitle": "Provisioned PVC capacity allocated to the app",
                        "costYtdChf": 5.2,
                        "shareOfTotalPercent": 4.2,
                        "status": {"state": "available", "label": "Available"},
                        "details": {
                            "provisionedBytes": 10_737_418_240,
                            "rateChfPerGbMonth": 0.12,
                            "costYtdChf": 5.2,
                        },
                    },
                    {
                        "key": "s3",
                        "label": "S3 Service",
                        "subtitle": "Visible shared workspace object storage",
                        "costYtdChf": 10.0,
                        "shareOfTotalPercent": 8.0,
                        "status": {"state": "available", "label": "Available"},
                        "details": {
                            "totalBytes": 4_096,
                            "bucketCount": 2,
                            "rateChfPerGbMonth": 0.03954,
                            "costYtdChf": 10.0,
                        },
                    },
                    {
                        "key": "pg",
                        "label": "PG Service",
                        "subtitle": "Static OLTP and OLAP database capacity placeholders",
                        "costYtdChf": 3.0,
                        "shareOfTotalPercent": 2.4,
                        "status": {"state": "available", "label": "Available"},
                        "details": {
                            "instances": [
                                {"label": "OLTP", "sizeBytes": 80_000_000_000, "sizeGb": 80.0},
                                {"label": "OLAP", "sizeBytes": 80_000_000_000, "sizeGb": 80.0},
                            ],
                            "annualFeePerInstanceChf": 5_526.1,
                            "dailyFeePerInstanceChf": 15.14,
                            "monthlyFeeChf": 921.02,
                            "totalBytes": 160_000_000_000,
                            "totalGb": 160.0,
                            "costYtdChf": 3.0,
                        },
                    },
                ],
                "monthly": {
                    "labels": [
                        "Jan",
                        "Feb",
                        "Mar",
                        "Apr",
                        "May",
                        "Jun",
                        "Jul",
                        "Aug",
                        "Sep",
                        "Oct",
                        "Nov",
                        "Dec",
                    ],
                    "currentYear": 2026,
                    "comparisonYear": 2025,
                    "comparisonActualCumulativeChf": [8_000.0, 16_000.0, 24_000.0, 32_000.0, 40_000.0, 48_000.0, 56_000.0, 64_000.0, 72_000.0, 80_000.0, 88_000.0, 96_000.0],
                    "actualCumulativeChf": [10.0, 30.0, 75.0, 125.2, None, None, None, None, None, None, None, None],
                    "planCumulativeChf": [10_000.0] * 12,
                    "forecastCumulativeChf": [10.0, 30.0, 75.0, 125.2, 240.0, 360.0, 480.0, 600.0, 720.0, 840.0, 960.0, 1_220.0],
                },
                "status": {
                    "available": True,
                    "detail": "Estimated CHF is available from the configured compute, S3, and persistent-volume rates.",
                    "budgetConfigured": True,
                    "budgetUpdatedAtUtc": "2026-04-16T08:00:00+00:00",
                    "computeAvailable": True,
                    "s3Available": True,
                    "persistentVolumeAvailable": True,
                },
            },
        }

    def update_service_consumption_budget(
        self,
        *,
        year: int,
        annual_budget_chf: float,
    ) -> dict[str, object]:
        return {
            "year": year,
            "annualBudgetChf": annual_budget_chf,
            "savedAtUtc": "2026-04-16T08:00:00+00:00",
            "version": 4,
        }


class RejectingBudgetWorkbenchService(FakeWorkbenchService):
    def update_service_consumption_budget(
        self,
        *,
        year: int,
        annual_budget_chf: float,
    ) -> dict[str, object]:
        raise ValueError("Only the current UTC year can be updated.")


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
        self.assertIn('data-service-consumption-financial-chart', body)
        self.assertIn('data-service-consumption-financial-service-mix-chart', body)
        self.assertIn('data-service-consumption-financial-service-mix-legend', body)
        self.assertIn('data-service-consumption-budget-input', body)
        self.assertIn('data-service-consumption-budget-form', body)
        self.assertIn('data-service-consumption-budget-highlight-value', body)
        self.assertIn('data-service-consumption-budget-highlight-value-meta', body)
        self.assertIn('data-service-consumption-budget-highlight-used', body)
        self.assertIn('data-service-consumption-budget-highlight-used-meta', body)
        self.assertIn('data-service-consumption-budget-highlight-remaining', body)
        self.assertIn('data-service-consumption-budget-highlight-remaining-meta', body)
        self.assertIn('data-service-consumption-budget-progress-bar', body)
        self.assertIn('data-service-consumption-services', body)
        self.assertIn('step="1000"', body)
        self.assertIn('data-service-consumption-pv-chart', body)
        self.assertIn('data-service-consumption-chart-limit="cpu"', body)
        self.assertIn('data-service-consumption-chart-limit="memory"', body)
        self.assertIn('service-consumption-summary-grid-compact', body)
        self.assertIn('service-consumption-panel-diagnostic', body)
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
        self.assertEqual(payload["financial"]["currency"], "CHF")
        self.assertEqual(payload["financial"]["annualBudgetChf"], 120_000.0)
        self.assertEqual(payload["financial"]["services"][4]["details"]["instances"][0]["label"], "OLTP")

    def test_service_consumption_budget_api_route_returns_payload(self) -> None:
        response = api_update_service_consumption_budget(
            payload=ServiceConsumptionBudgetPayload.model_validate(
                {"year": 2026, "annualBudgetChf": 120_000.0}
            ),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["year"], 2026)
        self.assertEqual(payload["annualBudgetChf"], 120_000.0)

    def test_service_consumption_budget_api_route_rejects_invalid_year(self) -> None:
        with self.assertRaises(HTTPException) as context:
            api_update_service_consumption_budget(
                payload=ServiceConsumptionBudgetPayload.model_validate(
                    {"year": 2025, "annualBudgetChf": 120_000.0}
                ),
                service=RejectingBudgetWorkbenchService(),
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "Only the current UTC year can be updated.")


if __name__ == "__main__":
    unittest.main()

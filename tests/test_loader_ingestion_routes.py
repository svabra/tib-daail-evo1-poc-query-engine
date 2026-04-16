from __future__ import annotations

from pathlib import Path
import sys
import unittest

from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.web.router import (  # noqa: E402
    ingestion_workbench_partial,
    loader_workbench_partial,
)


class FakeWorkbenchService:
    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": "0.5.1",
            "hostname": "test-host",
            "pod_name": "unknown",
            "pod_namespace": "unknown",
            "pod_ip": "unknown",
            "node_name": "unknown",
            "duckdb_database": "/tmp/workspace.duckdb",
            "timestamp_utc": "2026-04-15T00:00:00+00:00",
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


def build_request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"hx-request", b"true")],
        }
    )


class LoaderAndIngestionRouteTests(unittest.TestCase):
    def test_loader_workbench_partial_renders_loader_surface(self) -> None:
        response = loader_workbench_partial(
            request=build_request("/loader-workbench"),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-loader-workbench-page', body)
        self.assertIn("Loader Workbench", body)

    def test_ingestion_workbench_partial_renders_csv_entry_surface(self) -> None:
        response = ingestion_workbench_partial(
            request=build_request("/ingestion-workbench"),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-ingestion-workbench-page', body)
        self.assertIn('data-ingestion-tile="csv"', body)
        self.assertIn('data-ingestion-entry-panel="csv"', body)
        self.assertIn('id="csv-ingestion-panel"', body)
        self.assertIn('Back to ingest types', body)
        self.assertIn('data-close-ingestion-entry', body)
        self.assertIn('data-csv-preview-root', body)
        self.assertIn('data-csv-ingestion-form', body)
        self.assertIn('data-csv-s3-storage-format', body)
        self.assertIn("DuckDB remains the query engine for Shared Workspace files.", body)
        self.assertIn("Object key prefix", body)
        self.assertIn("S3 does not create real directories here.", body)
        self.assertIn("Local Workspace (IndexDB)", body)
        self.assertIn("Parquet", body)
        self.assertIn("JSON", body)
        self.assertIn("line-delimited JSON / JSONL", body)
        self.assertIn("CSV Files", body)

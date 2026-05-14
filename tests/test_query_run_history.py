from __future__ import annotations

import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend import query_run_history  # noqa: E402
from bit_data_workbench.backend.query_run_history import QueryRunHistoryStore  # noqa: E402
from bit_data_workbench.api.router import query_run_detail, query_runs_history  # noqa: E402
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs):
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]
        return {}

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = str(kwargs.get("Prefix") or "")
        contents = [
            {"Key": key, "Size": len(body)}
            for (object_bucket, key), body in self.objects.items()
            if object_bucket == bucket and key.startswith(prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(self.objects[(kwargs["Bucket"], kwargs["Key"])])}


def make_settings(root: Path) -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIFL Workbench",
        image_version=CURRENT_VERSION,
        port=8000,
        duckdb_database=root / "workspace.duckdb",
        duckdb_extension_directory=root / "duckdb-ext",
        service_consumption_data_dir=root / "service-consumption",
        service_consumption_cpu_memory_interval_seconds=3,
        service_consumption_s3_interval_seconds=3600,
        service_consumption_retention_hours=48,
        max_result_rows=50,
        s3_endpoint="http://127.0.0.1:9000",
        s3_bucket="shared-workspace",
        s3_access_key_id="key",
        s3_access_key_id_file=None,
        s3_secret_access_key="secret",
        s3_secret_access_key_file=None,
        s3_url_style="path",
        s3_use_ssl=False,
        s3_verify_ssl=False,
        s3_ca_cert_file=None,
        s3_session_token=None,
        s3_session_token_file=None,
        s3_startup_view_schema="s3",
        s3_startup_views=None,
        pg_host=None,
        pg_port=None,
        pg_user=None,
        pg_password=None,
        pg_oltp_database=None,
        pg_olap_database=None,
        pod_name=None,
        pod_namespace=None,
        pod_ip=None,
        node_name=None,
    )


class QueryRunHistoryStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="bdw-query-runs-")
        self.settings = make_settings(Path(self.temp_dir.name))
        self.client = FakeS3Client()
        self.store = QueryRunHistoryStore(self.settings)
        self.s3_client_patch = patch.object(query_run_history, "s3_client", return_value=self.client)
        self.ensure_bucket_patch = patch.object(query_run_history, "ensure_s3_bucket", return_value=None)
        self.s3_client_patch.start()
        self.ensure_bucket_patch.start()

    def tearDown(self) -> None:
        self.s3_client_patch.stop()
        self.ensure_bucket_patch.stop()
        self.temp_dir.cleanup()

    def test_records_telemetry_only_query_run_in_hidden_prefix(self) -> None:
        stored = self.store.record(
            {
                "jobId": "query-1",
                "notebookId": "nb",
                "notebookTitle": "Notebook",
                "cellId": "cell",
                "sql": "select 1",
                "status": "completed",
                "completedAt": "2026-05-13T10:00:00Z",
                "columns": ["value"],
                "rows": [[1]],
                "cpuPercent": 12.0,
                "averageCpuPercent": 8.0,
                "peakCpuPercent": 18.0,
                "memoryRssBytes": 1024,
                "averageMemoryRssBytes": 900,
                "peakMemoryRssBytes": 2048,
                "resourceSamples": [{"elapsedMs": 2000, "cpuPercent": 12.0}],
            }
        )

        self.assertTrue(stored["key"].startswith("--bdw-internal--/query-runs/2026/05/13/"))
        payload = json.loads(self.client.objects[(self.settings.s3_bucket, stored["key"])].decode("utf-8"))
        self.assertEqual(payload["jobId"], "query-1")
        self.assertNotIn("rows", payload)
        self.assertNotIn("columns", payload)
        self.assertEqual(payload["metrics"]["averageCpuPercent"], 8.0)

    def test_lists_failed_and_cancelled_runs_with_filters(self) -> None:
        self.store.record(
            {
                "jobId": "query-failed",
                "notebookId": "nb",
                "cellId": "cell-a",
                "sql": "select broken",
                "status": "failed",
                "completedAt": "2026-05-13T10:00:00Z",
            }
        )
        self.store.record(
            {
                "jobId": "query-cancelled",
                "notebookId": "nb",
                "cellId": "cell-b",
                "sql": "select slow",
                "status": "cancelled",
                "completedAt": "2026-05-13T10:01:00Z",
            }
        )

        cancelled = self.store.list_runs(notebook_id="nb", status="cancelled")

        self.assertTrue(cancelled["available"])
        self.assertEqual([run["jobId"] for run in cancelled["runs"]], ["query-cancelled"])
        self.assertEqual(self.store.get_run("query-failed")["status"], "failed")


class QueryRunHistoryApiTests(unittest.TestCase):
    def test_query_runs_api_returns_history_payload(self) -> None:
        class FakeService:
            def query_runs_history(self, *, notebook_id="", cell_id="", status="", limit=100):
                return {
                    "available": True,
                    "runs": [
                        {
                            "jobId": "query-1",
                            "notebookId": notebook_id,
                            "cellId": cell_id,
                            "status": status or "completed",
                        }
                    ],
                }

        response = query_runs_history(
            notebook_id="nb",
            cell_id="cell",
            status="completed",
            limit=10,
            service=FakeService(),
        )

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["runs"][0]["jobId"], "query-1")
        self.assertEqual(payload["runs"][0]["status"], "completed")

    def test_query_run_detail_api_returns_single_record(self) -> None:
        class FakeService:
            def query_run_detail(self, job_id):
                return {"jobId": job_id, "status": "cancelled"}

        response = query_run_detail("query-2", service=FakeService())

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["jobId"], "query-2")
        self.assertEqual(payload["status"], "cancelled")


if __name__ == "__main__":
    unittest.main()

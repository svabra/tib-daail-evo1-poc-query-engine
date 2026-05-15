from __future__ import annotations

from pathlib import Path
import sys
import unittest

from botocore.exceptions import ClientError


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.api import router as api_router  # noqa: E402
from bit_data_workbench.backend.s3_delete_jobs import S3DeleteJobManager  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


def build_settings(**overrides):
    values = {
        "service_name": "bit-data-workbench",
        "ui_title": "DAAIFL Workbench",
        "image_version": CURRENT_VERSION,
        "port": 8000,
        "duckdb_database": Path("/tmp/workspace/workspace.duckdb"),
        "duckdb_extension_directory": Path("/opt/duckdb/extensions"),
        "service_consumption_data_dir": Path("/tmp/service-consumption"),
        "service_consumption_cpu_memory_interval_seconds": 60,
        "service_consumption_s3_interval_seconds": 3600,
        "service_consumption_retention_hours": 48,
        "max_result_rows": 200,
        "s3_endpoint": "localhost:9000",
        "s3_bucket": "shared-workspace",
        "s3_access_key_id": "key",
        "s3_access_key_id_file": None,
        "s3_secret_access_key": "secret",
        "s3_secret_access_key_file": None,
        "s3_url_style": "path",
        "s3_use_ssl": False,
        "s3_verify_ssl": False,
        "s3_ca_cert_file": None,
        "s3_session_token": None,
        "s3_session_token_file": None,
        "s3_startup_view_schema": "s3",
        "s3_startup_views": None,
        "pg_host": None,
        "pg_port": None,
        "pg_user": None,
        "pg_password": None,
        "pg_oltp_database": None,
        "pg_olap_database": None,
        "pod_name": None,
        "pod_namespace": None,
        "pod_ip": None,
        "node_name": None,
    }
    values.update(overrides)
    return Settings(**values)


def s3_error(code: str, message: str = "") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message or code}},
        code,
    )


class _ObjectDeleteClient:
    def __init__(self) -> None:
        self.delete_objects_calls: list[list[dict[str, str]]] = []

    def list_object_versions(self, **_kwargs):
        return {
            "Versions": [{"Key": "exports/data.csv", "VersionId": "v1"}],
            "DeleteMarkers": [],
            "IsTruncated": False,
        }

    def delete_objects(self, **kwargs):
        self.delete_objects_calls.append(list(kwargs["Delete"]["Objects"]))
        return {}


class _FinalizingBucketClient:
    def __init__(self, *, disappear_after_head_calls: int | None = 3) -> None:
        self.head_calls = 0
        self.delete_bucket_calls = 0
        self.disappear_after_head_calls = disappear_after_head_calls

    def head_bucket(self, **_kwargs):
        self.head_calls += 1
        if (
            self.disappear_after_head_calls is not None
            and self.head_calls >= self.disappear_after_head_calls
        ):
            raise s3_error("NoSuchBucket", "Not found")
        return None

    def list_object_versions(self, **_kwargs):
        return {"Versions": [], "DeleteMarkers": [], "IsTruncated": False}

    def list_objects_v2(self, **_kwargs):
        return {"KeyCount": 0, "Contents": [], "IsTruncated": False}

    def delete_bucket(self, **_kwargs):
        self.delete_bucket_calls += 1
        return None

    def delete_objects(self, **_kwargs):
        raise AssertionError("Empty bucket finalization should not delete object batches.")


class _FakeDeleteService:
    def delete_s3_explorer_entry(self, *, entry_kind: str, bucket: str, prefix: str = ""):
        return {
            "jobId": "s3-delete-test",
            "entryKind": entry_kind,
            "bucket": bucket,
            "prefix": prefix,
            "path": f"s3://{bucket}/{prefix}",
            "status": "queued",
        }

    def s3_delete_jobs_state(self):
        return {"version": 1, "jobs": []}

    def s3_delete_job_state(self, *, job_id: str):
        return {"jobId": job_id, "status": "queued"}


class S3DeleteJobTests(unittest.TestCase):
    def test_delete_route_returns_202_with_job_payload(self) -> None:
        payload = api_router.S3ExplorerDeletePayload(
            entryKind="file",
            bucket="test-bucket",
            prefix="exports/data.csv",
        )

        response = api_router.delete_s3_explorer_entry(
            payload,
            service=_FakeDeleteService(),
        )

        self.assertEqual(response.status_code, 202)
        self.assertIn(b'"jobId":"s3-delete-test"', response.body)
        self.assertIn(b'"status":"queued"', response.body)

    def test_object_delete_logs_requested_and_completed_path(self) -> None:
        client = _ObjectDeleteClient()
        completed_jobs: list[dict[str, object]] = []
        manager = S3DeleteJobManager(
            settings=build_settings(),
            s3_client_factory=lambda _settings: client,
            completion_callback=lambda payload: completed_jobs.append(payload),
            run_jobs_inline=True,
        )

        with self.assertLogs(
            "bit_data_workbench.backend.s3_delete_jobs",
            level="INFO",
        ) as logs:
            job = manager.start_job(
                entry_kind="file",
                bucket="test-bucket",
                prefix="exports/data.csv",
            )

        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["deletedKeys"], 1)
        self.assertEqual(completed_jobs[0]["jobId"], job["jobId"])
        self.assertEqual(client.delete_objects_calls, [[{"Key": "exports/data.csv", "VersionId": "v1"}]])
        output = "\n".join(logs.output)
        self.assertIn('s3_delete_event="requested"', output)
        self.assertIn('s3_delete_event="completed"', output)
        self.assertIn('path="s3://test-bucket/exports/data.csv"', output)

    def test_bucket_delete_stays_finalizing_until_bucket_disappears(self) -> None:
        client = _FinalizingBucketClient(disappear_after_head_calls=3)
        observed_statuses: list[str] = []
        manager = S3DeleteJobManager(
            settings=build_settings(),
            s3_client_factory=lambda _settings: client,
            sleep=lambda _seconds: observed_statuses.append(
                manager.state_payload()["jobs"][0]["status"]
            ),
            run_jobs_inline=True,
        )

        with self.assertLogs(
            "bit_data_workbench.backend.s3_delete_jobs",
            level="INFO",
        ) as logs:
            job = manager.start_job(
                entry_kind="bucket",
                bucket="test-bucket",
            )

        self.assertEqual(job["status"], "completed")
        self.assertIn("finalizing", observed_statuses)
        self.assertGreaterEqual(client.delete_bucket_calls, 1)
        self.assertIn('s3_delete_event="bucket_finalizing"', "\n".join(logs.output))

    def test_bucket_delete_fails_only_after_finalize_timeout(self) -> None:
        client = _FinalizingBucketClient(disappear_after_head_calls=None)
        clock = {"value": 0.0}

        def monotonic() -> float:
            return clock["value"]

        def sleep(seconds: float) -> None:
            clock["value"] += seconds

        manager = S3DeleteJobManager(
            settings=build_settings(s3_delete_bucket_finalize_timeout_seconds=1),
            s3_client_factory=lambda _settings: client,
            sleep=sleep,
            monotonic=monotonic,
            run_jobs_inline=True,
        )

        with self.assertLogs(
            "bit_data_workbench.backend.s3_delete_jobs",
            level="WARNING",
        ) as logs:
            job = manager.start_job(
                entry_kind="bucket",
                bucket="test-bucket",
            )

        self.assertEqual(job["status"], "failed")
        self.assertIn("still visible after 1 seconds", job["error"])
        self.assertIn('s3_delete_event="failed"', "\n".join(logs.output))

    def test_heartbeat_logs_are_throttled(self) -> None:
        manager = S3DeleteJobManager(
            settings=build_settings(),
            s3_client_factory=lambda _settings: object(),
        )
        descriptor = manager._normalize_descriptor(  # noqa: SLF001
            entry_kind="file",
            bucket="test-bucket",
            prefix="exports/data.csv",
        )
        job = manager._new_job("s3-delete-heartbeat", descriptor)  # noqa: SLF001
        job["status"] = "running"
        job["startedAt"] = job["createdAt"]
        manager._jobs[job["jobId"]] = job  # noqa: SLF001

        with self.assertLogs(
            "bit_data_workbench.backend.s3_delete_jobs",
            level="INFO",
        ) as logs:
            manager._log_heartbeat_if_due(job["jobId"])  # noqa: SLF001
            manager._log_heartbeat_if_due(job["jobId"])  # noqa: SLF001
            job["lastLogHeartbeatMonotonic"] -= 11
            manager._log_heartbeat_if_due(job["jobId"])  # noqa: SLF001

        output = "\n".join(logs.output)
        self.assertEqual(output.count('s3_delete_event="heartbeat"'), 2)


if __name__ == "__main__":
    unittest.main()

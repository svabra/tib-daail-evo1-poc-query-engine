from __future__ import annotations

from pathlib import Path
import sys
import time
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.backend.s3_download_jobs import S3DownloadJobManager  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


def make_settings() -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIFL Workbench",
        image_version=current_repo_version(REPO_ROOT),
        port=8000,
        duckdb_database=Path("/tmp/workspace.duckdb"),
        duckdb_extension_directory=Path("/tmp/duckdb-ext"),
        service_consumption_data_dir=Path("/tmp/service-consumption"),
        service_consumption_cpu_memory_interval_seconds=3,
        service_consumption_s3_interval_seconds=3600,
        service_consumption_retention_hours=48,
        max_result_rows=200,
        s3_endpoint="http://127.0.0.1:9000",
        s3_bucket="workspace",
        s3_access_key_id="minio",
        s3_access_key_id_file=None,
        s3_secret_access_key="miniosecret",
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


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0
        self.read_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        if size < 0:
            size = len(self._payload) - self._offset
        start = self._offset
        self._offset = min(len(self._payload), self._offset + size)
        return self._payload[start:self._offset]

    def close(self) -> None:
        return None


class FakeS3Client:
    def __init__(self) -> None:
        self.objects = {
            "generated/table/part-00001.csv": b"id,name\n1,alpha\n",
            "generated/table/part-00002.csv": b"id,name\n2,beta\n",
        }
        self.bodies: list[FakeBody] = []
        self.uploaded_parts: list[bytes] = []
        self.completed_key = ""

    def list_objects_v2(self, **kwargs):
        prefix = kwargs.get("Prefix") or ""
        return {
            "Contents": [
                {"Key": key, "Size": len(payload)}
                for key, payload in self.objects.items()
                if key.startswith(prefix)
            ],
            "IsTruncated": False,
        }

    def head_object(self, **kwargs):
        return {"ContentLength": len(self.objects[kwargs["Key"]])}

    def get_object(self, **kwargs):
        body = FakeBody(self.objects[kwargs["Key"]])
        self.bodies.append(body)
        return {"Body": body}

    def create_multipart_upload(self, **_kwargs):
        return {"UploadId": "upload-1"}

    def upload_part(self, **kwargs):
        self.uploaded_parts.append(bytes(kwargs["Body"]))
        return {"ETag": f"etag-{len(self.uploaded_parts)}"}

    def complete_multipart_upload(self, **kwargs):
        self.completed_key = kwargs["Key"]

    def abort_multipart_upload(self, **_kwargs):
        return None


def test_generated_zip_job_streams_parts_to_same_bucket_object() -> None:
    fake_client = FakeS3Client()
    manager = S3DownloadJobManager(settings=make_settings())

    with patch("bit_data_workbench.backend.s3_download_jobs.s3_client", return_value=fake_client):
        started = manager.start_generated_zip_job(
            bucket="client-bucket",
            prefix="generated/table/",
            file_format="csv",
            file_name="table.zip",
        )
        deadline = time.time() + 5
        snapshot = started
        while time.time() < deadline:
            snapshot = manager.snapshot(started["jobId"])
            if snapshot["status"] in {"completed", "failed", "cancelled"}:
                break
            time.sleep(0.05)

    assert snapshot["status"] == "completed", snapshot
    assert snapshot["bucket"] == "client-bucket"
    assert snapshot["key"] == "generated/table/table.zip"
    assert fake_client.completed_key == "generated/table/table.zip"
    assert fake_client.uploaded_parts
    assert fake_client.bodies
    assert all(1024 * 1024 in body.read_sizes for body in fake_client.bodies)

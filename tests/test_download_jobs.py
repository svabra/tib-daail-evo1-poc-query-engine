from __future__ import annotations

import io
import json
from pathlib import Path
import sys
import time
from urllib.parse import parse_qs, urlparse
import unittest
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.download_jobs import (  # noqa: E402
    DownloadJobManager,
    DownloadRangeNotSatisfiable,
)
from bit_data_workbench.backend.s3_hidden import DOWNLOAD_JOB_S3_PREFIX  # noqa: E402
from test_csv_ingestion import make_settings  # noqa: E402


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset:self._offset + size]
        self._offset += len(chunk)
        return chunk

    def close(self) -> None:
        self.closed = True


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.uploads: dict[str, dict[str, object]] = {}
        self.aborted_uploads: list[str] = []

    def head_object(self, **kwargs):
        payload = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        return {
            "ContentLength": len(payload),
            "ETag": f'"etag-{len(payload)}"',
            "ContentType": "text/csv",
        }

    def get_object(self, **kwargs):
        payload = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        range_header = kwargs.get("Range")
        if range_header:
            start_text, end_text = str(range_header).removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = int(end_text) if end_text else len(payload) - 1
            payload = payload[start:end + 1]
        return {
            "Body": FakeBody(payload),
            "ContentLength": len(payload),
            "ContentType": "application/zip" if kwargs["Key"].endswith(".zip") else "text/csv",
        }

    def put_object(self, **kwargs) -> None:
        body = kwargs.get("Body", b"")
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = bytes(body)

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = kwargs.get("Prefix", "")
        contents = [
            {"Key": key, "Size": len(payload)}
            for (candidate_bucket, key), payload in sorted(self.objects.items())
            if candidate_bucket == bucket and key.startswith(prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}

    def create_multipart_upload(self, **kwargs):
        upload_id = f"upload-{len(self.uploads) + 1}"
        self.uploads[upload_id] = {
            "Bucket": kwargs["Bucket"],
            "Key": kwargs["Key"],
            "Parts": {},
        }
        return {"UploadId": upload_id}

    def upload_part(self, **kwargs):
        upload = self.uploads[kwargs["UploadId"]]
        upload["Parts"][int(kwargs["PartNumber"])] = bytes(kwargs["Body"])
        return {"ETag": f'"part-{kwargs["PartNumber"]}"'}

    def complete_multipart_upload(self, **kwargs) -> None:
        upload = self.uploads[kwargs["UploadId"]]
        parts = upload["Parts"]
        payload = b"".join(parts[index] for index in sorted(parts))
        self.objects[(upload["Bucket"], upload["Key"])] = payload

    def abort_multipart_upload(self, **kwargs) -> None:
        self.aborted_uploads.append(kwargs["UploadId"])


def manager_for(fake_s3: FakeS3Client) -> DownloadJobManager:
    settings = make_settings()
    settings.download_compression_level = 9
    settings.download_multipart_chunk_bytes = 5 * 1024 * 1024
    return DownloadJobManager(
        settings=settings,
        s3_client_factory=lambda _settings: fake_s3,
        ensure_bucket_factory=lambda _settings, _bucket: None,
    )


def wait_for_status(manager: DownloadJobManager, job_id: str, status: str) -> dict[str, object]:
    for _ in range(120):
        payload = manager.job_payload(job_id)
        if payload["status"] == status:
            return payload
        time.sleep(0.025)
    raise AssertionError(f"Job {job_id} did not reach {status}: {manager.job_payload(job_id)}")


def token_from_job(job: dict[str, object]) -> str:
    return parse_qs(urlparse(str(job["downloadUrl"])).query)["token"][0]


class DownloadJobManagerTests(unittest.TestCase):
    def test_s3_download_job_creates_zip64_artifact(self) -> None:
        fake_s3 = FakeS3Client()
        fake_s3.objects[("workspace", "data/alpha.csv")] = b"id,name\n1,alpha\n2,beta\n"
        manager = manager_for(fake_s3)

        started = manager.start_s3_job(
            bucket="workspace",
            key="data/alpha.csv",
            filename="alpha.csv",
            file_format="csv",
        )
        ready = wait_for_status(manager, str(started["jobId"]), "ready")

        stream = manager.artifact_stream(
            job_id=str(ready["jobId"]),
            token=token_from_job(ready),
            range_header="",
        )
        archive_payload = stream.body.read()
        stream.body.close()

        with zipfile.ZipFile(io.BytesIO(archive_payload)) as archive:
            info = archive.getinfo("alpha.csv")
            self.assertEqual(info.compress_type, zipfile.ZIP_DEFLATED)
            self.assertEqual(archive.read("alpha.csv"), b"id,name\n1,alpha\n2,beta\n")
            self.assertTrue(info.file_size >= 0)
        self.assertEqual(manager._settings.download_compression_level, 9)
        self.assertEqual(ready["sourceSizeBytes"], len(b"id,name\n1,alpha\n2,beta\n"))

    def test_s3_download_job_is_created_before_missing_source_fails(self) -> None:
        fake_s3 = FakeS3Client()
        manager = manager_for(fake_s3)

        started = manager.start_s3_job(
            bucket="workspace",
            key="data/missing.csv",
            filename="missing.csv",
            file_format="csv",
        )

        self.assertTrue(str(started["jobId"]).startswith("download-"))
        failed = wait_for_status(manager, str(started["jobId"]), "failed")
        self.assertEqual(failed["sourceBucket"], "workspace")
        self.assertEqual(failed["sourceKey"], "data/missing.csv")

    def test_s3_download_job_rejects_sql_alias_bucket_before_job_creation(self) -> None:
        fake_s3 = FakeS3Client()
        manager = manager_for(fake_s3)

        with self.assertRaisesRegex(ValueError, "physical bucket name"):
            manager.start_s3_job(
                bucket="n_1_workspace",
                key="data/alpha.csv",
                filename="alpha.csv",
                file_format="csv",
            )

    def test_s3_download_job_rejects_internal_locations(self) -> None:
        fake_s3 = FakeS3Client()
        fake_s3.objects[("workspace", "--bdw-internal--/secret.csv")] = b"id\n1\n"
        manager = manager_for(fake_s3)

        with self.assertRaisesRegex(ValueError, "Internal Workbench"):
            manager.start_s3_job(
                bucket="workspace",
                key="--bdw-internal--/secret.csv",
                filename="secret.csv",
                file_format="csv",
            )

    def test_artifact_stream_supports_200_206_and_416(self) -> None:
        fake_s3 = FakeS3Client()
        fake_s3.objects[("workspace", "data/alpha.csv")] = b"id,name\n1,alpha\n"
        manager = manager_for(fake_s3)
        started = manager.start_s3_job(
            bucket="workspace",
            key="data/alpha.csv",
            filename="alpha.csv",
            file_format="csv",
        )
        ready = wait_for_status(manager, str(started["jobId"]), "ready")
        token = token_from_job(ready)

        full = manager.artifact_stream(
            job_id=str(ready["jobId"]),
            token=token,
            range_header="",
        )
        self.assertEqual(full.status_code, 200)
        full_payload = full.body.read()
        full.body.close()

        ranged = manager.artifact_stream(
            job_id=str(ready["jobId"]),
            token=token,
            range_header="bytes=0-9",
        )
        self.assertEqual(ranged.status_code, 206)
        self.assertEqual(ranged.headers["Content-Range"], f"bytes 0-9/{len(full_payload)}")
        self.assertEqual(ranged.body.read(), full_payload[:10])
        ranged.body.close()

        with self.assertRaises(DownloadRangeNotSatisfiable):
            manager.artifact_stream(
                job_id=str(ready["jobId"]),
                token=token,
                range_header=f"bytes={len(full_payload)}-{len(full_payload) + 1}",
            )

    def test_startup_marks_interrupted_jobs_failed(self) -> None:
        fake_s3 = FakeS3Client()
        manifest = {
            "jobId": "download-interrupted",
            "sourceKind": "s3_object",
            "status": "running",
            "sourceName": "alpha.csv",
            "sourceBucket": "workspace",
            "sourceKey": "data/alpha.csv",
            "sourceSizeBytes": 10,
            "sourceFingerprint": "s3|workspace|data/alpha.csv|etag|10",
            "artifactFilename": "alpha.csv.zip",
            "token": "token",
            "createdAt": "2026-05-14T10:00:00Z",
            "updatedAt": "2026-05-14T10:00:00Z",
        }
        fake_s3.objects[("workspace", f"{DOWNLOAD_JOB_S3_PREFIX}download-interrupted.json")] = json.dumps(
            manifest
        ).encode("utf-8")

        manager = manager_for(fake_s3)
        payload = manager.state_payload()

        self.assertEqual(payload["jobs"][0]["status"], "failed")
        persisted = json.loads(
            fake_s3.objects[("workspace", f"{DOWNLOAD_JOB_S3_PREFIX}download-interrupted.json")].decode(
                "utf-8"
            )
        )
        self.assertEqual(persisted["status"], "failed")


if __name__ == "__main__":
    unittest.main()

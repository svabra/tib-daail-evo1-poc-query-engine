from __future__ import annotations

import io
from pathlib import Path
import shutil
import sys
import zipfile
from unittest.mock import patch

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


class _ConfiguredSettings:
    s3_endpoint = "http://127.0.0.1:9000"
    data_exchange_prefix = "--data-exchange--/"

    def current_s3_access_key_id(self) -> str:
        return "key"

    def current_s3_secret_access_key(self) -> str:
        return "secret"


class _TrackingBody(io.BytesIO):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.was_closed = False

    def close(self) -> None:
        self.was_closed = True
        super().close()


class _ChunkTrackingBody(io.BytesIO):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.read_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return super().read(size)


class _FakeS3Client:
    def __init__(self, body: _TrackingBody) -> None:
        self.body = body
        self.get_object_calls: list[dict[str, str]] = []

    def get_object(self, **kwargs):
        self.get_object_calls.append(dict(kwargs))
        return {
            "Body": self.body,
            "ContentLength": len(self.body.getvalue()),
            "ContentType": "text/csv",
        }


class _FakeListingClient:
    def list_buckets(self):
        return {
            "Buckets": [
                {"Name": "client-bucket"},
                {"Name": "client-data-exchange"},
            ]
        }

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = str(kwargs.get("Prefix") or "")
        delimiter = str(kwargs.get("Delimiter") or "")
        if bucket != "client-bucket":
            return {"Contents": [], "CommonPrefixes": [], "IsTruncated": False}
        if delimiter == "/":
            return {
                "CommonPrefixes": [
                    {"Prefix": "--data-exchange--/"},
                    {"Prefix": "published/"},
                ],
                "Contents": [
                    {"Key": "--data-exchange--/files/secret/alpha.csv", "Size": 10},
                    {"Key": "visible.csv", "Size": 20},
                ],
                "IsTruncated": False,
            }
        return {"Contents": [], "IsTruncated": False}


class _FakeGeneratedPartsClient:
    objects = {
        "generated/mwa/csv/table/part-00001.csv": b"id,name\n1,alpha\n2,beta\n",
        "generated/mwa/csv/table/part-00002.csv": b"id,name\n3,gamma\n",
        "generated/mwa/json/table/part-00001.jsonl": b'{"id":1}\n',
        "generated/mwa/json/table/part-00002.jsonl": b'{"id":2}\n\n',
    }

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = str(kwargs.get("Prefix") or "")
        if bucket != "client-bucket":
            return {"Contents": [], "IsTruncated": False}
        return {
            "Contents": [
                {"Key": key, "Size": len(payload)}
                for key, payload in self.objects.items()
                if key.startswith(prefix)
            ],
            "IsTruncated": False,
        }

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        payload = self.objects[key]
        return {
            "Body": io.BytesIO(payload),
            "ContentLength": len(payload),
        }


class _ChunkTrackingGeneratedPartsClient(_FakeGeneratedPartsClient):
    def __init__(self) -> None:
        self.bodies: list[_ChunkTrackingBody] = []

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        body = _ChunkTrackingBody(self.objects[key])
        self.bodies.append(body)
        return {
            "Body": body,
            "ContentLength": len(self.objects[key]),
        }


def import_s3_explorer():
    from bit_data_workbench.backend.data_sources.s3 import explorer

    return explorer


def test_normalize_s3_bucket_name_rejects_underscores() -> None:
    explorer = import_s3_explorer()

    with pytest.raises(ValueError, match="lowercase letters, numbers, dots, or hyphens"):
        explorer.normalize_s3_bucket_name("client_bucket")


def test_create_bucket_rejects_underscores_before_s3_call() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with patch.object(explorer, "ensure_s3_bucket") as ensure_s3_bucket:
        with pytest.raises(ValueError, match="lowercase letters, numbers, dots, or hyphens"):
            manager.create_bucket("client_bucket")

    ensure_s3_bucket.assert_not_called()


def test_create_bucket_normalizes_valid_bucket_name() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with patch.object(explorer, "ensure_s3_bucket") as ensure_s3_bucket:
        created = manager.create_bucket(" Client-Bucket ")

    ensure_s3_bucket.assert_called_once_with(manager._settings, "client-bucket")
    assert created.bucket == "client-bucket"


def test_stream_object_returns_s3_body_without_temp_file_download() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())
    body = _TrackingBody(b"id,name\n1,alpha\n")
    fake_client = _FakeS3Client(body)

    with patch.object(explorer, "s3_client", return_value=fake_client), patch.object(
        explorer,
        "download_s3_file",
    ) as download_s3_file:
        artifact = manager.stream_object(
            bucket="client-bucket",
            key="exports/large.csv",
            file_name="",
        )

    download_s3_file.assert_not_called()
    assert fake_client.get_object_calls == [
        {"Bucket": "client-bucket", "Key": "exports/large.csv"}
    ]
    assert artifact.filename == "large.csv"
    assert artifact.content_type == "text/csv"
    assert artifact.content_length == len(b"id,name\n1,alpha\n")
    assert artifact.body.read() == b"id,name\n1,alpha\n"
    artifact.body.close()
    assert body.was_closed


def test_snapshot_hides_data_exchange_bucket_and_prefix() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())
    fake_client = _FakeListingClient()

    with patch.object(explorer, "s3_client", return_value=fake_client):
        root_snapshot = manager.snapshot()
        bucket_snapshot = manager.snapshot(bucket="client-bucket")

    assert [entry.name for entry in root_snapshot.entries] == ["client-bucket"]
    assert [entry.name for entry in bucket_snapshot.entries] == ["published", "visible.csv"]


def test_stream_object_rejects_data_exchange_prefix() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with pytest.raises(ValueError, match="DataExchange"):
        manager.stream_object(
            bucket="client-bucket",
            key="--data-exchange--/files/secret/alpha.csv",
        )


def test_normalize_s3_object_key_preserves_object_name_spaces() -> None:
    explorer = import_s3_explorer()

    assert explorer.normalize_s3_object_key(" exports /April File.csv ") == " exports /April File.csv "


def test_download_generated_csv_parts_merges_sorted_parts_and_single_header() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())
    fake_client = _FakeGeneratedPartsClient()

    with patch.object(explorer, "s3_client", return_value=fake_client):
        artifact = manager.download_generated_parts(
            bucket="client-bucket",
            prefix="generated/mwa/csv/table/",
            file_format="csv",
            mode="merged",
            file_name="table.csv",
        )

    assert artifact.filename == "table.csv"
    assert artifact.content_type == "text/csv; charset=utf-8"
    assert artifact.body_iter is not None
    assert b"".join(artifact.body_iter()) == b"id,name\n1,alpha\n2,beta\n3,gamma\n"


def test_download_generated_jsonl_parts_merges_valid_jsonl_lines() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())
    fake_client = _FakeGeneratedPartsClient()

    with patch.object(explorer, "s3_client", return_value=fake_client):
        artifact = manager.download_generated_parts(
            bucket="client-bucket",
            prefix="generated/mwa/json/table/",
            file_format="json",
            mode="merged",
            file_name="table.jsonl",
        )

    assert artifact.filename == "table.jsonl"
    assert artifact.content_type == "application/x-ndjson; charset=utf-8"
    assert artifact.body_iter is not None
    assert b"".join(artifact.body_iter()) == b'{"id":1}\n{"id":2}\n'


def test_download_generated_parts_zip_contains_original_part_files() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())
    fake_client = _FakeGeneratedPartsClient()

    with patch.object(explorer, "s3_client", return_value=fake_client):
        artifact = manager.download_generated_parts(
            bucket="client-bucket",
            prefix="generated/mwa/csv/table/",
            file_format="csv",
            mode="zip",
            file_name="table.csv",
        )

    try:
        assert artifact.filename == "table.zip"
        assert artifact.content_type == "application/zip"
        assert artifact.local_path is not None
        with zipfile.ZipFile(artifact.local_path) as archive:
            assert archive.namelist() == ["part-00001.csv", "part-00002.csv"]
            assert archive.read("part-00001.csv") == b"id,name\n1,alpha\n2,beta\n"
            assert archive.read("part-00002.csv") == b"id,name\n3,gamma\n"
    finally:
        if artifact.cleanup_dir is not None:
            shutil.rmtree(artifact.cleanup_dir, ignore_errors=True)


def test_download_generated_parts_zip_streams_part_bodies_in_chunks() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())
    fake_client = _ChunkTrackingGeneratedPartsClient()

    with patch.object(explorer, "s3_client", return_value=fake_client):
        artifact = manager.download_generated_parts(
            bucket="client-bucket",
            prefix="generated/mwa/csv/table/",
            file_format="csv",
            mode="zip",
            file_name="table.zip",
        )

    try:
        assert fake_client.bodies
        assert all(body.read_sizes for body in fake_client.bodies)
        assert all(size == 1024 * 1024 for body in fake_client.bodies for size in body.read_sizes[:-1])
    finally:
        if artifact.cleanup_dir is not None:
            shutil.rmtree(artifact.cleanup_dir, ignore_errors=True)

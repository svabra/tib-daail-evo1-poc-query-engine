from __future__ import annotations

import io
from pathlib import Path
import sys
from unittest.mock import patch

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


class _ConfiguredSettings:
    s3_endpoint = "http://127.0.0.1:9000"

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

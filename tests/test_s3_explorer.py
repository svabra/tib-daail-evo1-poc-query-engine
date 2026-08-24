from __future__ import annotations

import io
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace
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
                    {"Prefix": "--bdw-internal--/"},
                    {"Prefix": "published/"},
                ],
                "Contents": [
                    {"Key": "--data-exchange--/files/secret/alpha.csv", "Size": 10},
                    {"Key": "--bdw-internal--/query-runs/2026/05/13/query-1.json", "Size": 30},
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


def import_s3_explorer():
    from bit_data_workbench.backend.data_sources.s3 import explorer

    return explorer


def import_workbench_service():
    from bit_data_workbench.backend.service import WorkbenchService

    return WorkbenchService


def test_normalize_s3_bucket_name_rejects_underscores() -> None:
    explorer = import_s3_explorer()

    with pytest.raises(ValueError, match="lowercase letters, numbers, dots, or hyphens"):
        explorer.normalize_s3_bucket_name("client_bucket")


def test_create_bucket_normalizes_sql_friendly_name_before_s3_call() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with patch.object(explorer, "ensure_s3_bucket") as ensure_s3_bucket:
        created = manager.create_bucket(" 1_3 imports ")

    ensure_s3_bucket.assert_called_once_with(manager._settings, "bdw-1-3-imports")
    assert created.bucket == "bdw-1-3-imports"


def test_create_bucket_normalizes_valid_bucket_name() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with patch.object(explorer, "ensure_s3_bucket") as ensure_s3_bucket:
        created = manager.create_bucket(" Client-Bucket ")

    ensure_s3_bucket.assert_called_once_with(manager._settings, "client-bucket")
    assert created.bucket == "client-bucket"


def test_service_create_bucket_uses_bounded_discovery_for_normalized_bucket() -> None:
    WorkbenchService = import_workbench_service()
    service = WorkbenchService.__new__(WorkbenchService)
    requests = []
    sync_calls = []
    service._s3_plugin = SimpleNamespace(
        create=lambda request: (
            requests.append(request)
            or SimpleNamespace(
                bucket="normalized-client-bucket",
                payload={"entryKind": "bucket", "bucket": "normalized-client-bucket"},
            )
        )
    )
    service._data_source_discovery = SimpleNamespace(
        sync_s3_buckets=lambda buckets, *, emit_event=True: sync_calls.append(
            (tuple(buckets), emit_event)
        ),
        sync_source=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("bucket creation must not run full S3 discovery")
        ),
    )

    payload = service.create_s3_bucket(" Client_Bucket ")

    assert [(request.kind, request.name) for request in requests] == [
        ("bucket", " Client_Bucket ")
    ]
    assert sync_calls == [(('normalized-client-bucket',), True)]
    assert payload == {"entryKind": "bucket", "bucket": "normalized-client-bucket"}


def test_service_create_bucket_preserves_full_discovery_compatibility_fallback() -> None:
    WorkbenchService = import_workbench_service()
    service = WorkbenchService.__new__(WorkbenchService)
    sync_calls = []
    service._s3_plugin = SimpleNamespace(
        create=lambda _request: SimpleNamespace(
            bucket="client-bucket",
            payload={"entryKind": "bucket", "bucket": "client-bucket"},
        )
    )
    service._data_source_discovery = SimpleNamespace(
        sync_source=lambda source_id, *, emit_event=True: sync_calls.append(
            (source_id, emit_event)
        )
    )

    service.create_s3_bucket("client-bucket")

    assert sync_calls == [("s3", True)]


def test_stream_object_allows_existing_digit_start_bucket() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())
    body = _TrackingBody(b"id,name\n1,alpha\n")
    fake_client = _FakeS3Client(body)

    with patch.object(explorer, "s3_client", return_value=fake_client):
        artifact = manager.stream_object(
            bucket="1-client-bucket",
            key="exports/large.csv",
            file_name="",
        )

    assert fake_client.get_object_calls == [
        {"Bucket": "1-client-bucket", "Key": "exports/large.csv"}
    ]
    assert artifact.filename == "large.csv"


def test_stream_object_rejects_sql_alias_bucket() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())
    body = _TrackingBody(b"id,name\n1,alpha\n")
    fake_client = _FakeS3Client(body)

    with patch.object(explorer, "s3_client", return_value=fake_client):
        with pytest.raises(ValueError, match="physical bucket name"):
            manager.stream_object(
                bucket="n_1_client_bucket",
                key="exports/large.csv",
                file_name="",
            )

    assert fake_client.get_object_calls == []


def test_delete_bucket_entry_cleans_contents_before_removing_bucket() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with patch.object(
        explorer,
        "delete_s3_bucket",
        return_value=2,
    ) as delete_s3_bucket, patch.object(
        explorer,
        "remove_s3_bucket",
        return_value=True,
    ) as remove_s3_bucket:
        result = manager.delete_entry(
            entry_kind="bucket",
            bucket=" Client-Bucket ",
        )

    delete_s3_bucket.assert_called_once_with(manager._settings, "client-bucket")
    remove_s3_bucket.assert_called_once_with(manager._settings, "client-bucket")
    assert result.bucket == "client-bucket"
    assert result.deleted_keys == 2
    assert result.bucket_deleted is True
    assert result.message == "Deleted bucket client-bucket and 2 contained object(s)."


def test_delete_bucket_entry_reports_when_bucket_still_cannot_be_removed() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with patch.object(
        explorer,
        "delete_s3_bucket",
        return_value=1,
    ), patch.object(
        explorer,
        "remove_s3_bucket",
        return_value=False,
    ):
        with pytest.raises(ValueError, match="object cleanup finished"):
            manager.delete_entry(
                entry_kind="bucket",
                bucket="client-bucket",
            )


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


def test_stream_object_rejects_internal_prefix() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with pytest.raises(ValueError, match="Internal Workbench"):
        manager.stream_object(
            bucket="client-bucket",
            key="--bdw-internal--/query-runs/2026/05/13/query-1.json",
        )


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

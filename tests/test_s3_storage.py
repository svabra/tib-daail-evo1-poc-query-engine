from __future__ import annotations

from tempfile import TemporaryDirectory
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_s3_storage():
    from bit_data_workbench.backend import s3_storage

    return s3_storage


def import_settings_type():
    from bit_data_workbench.config import Settings

    return Settings


def build_settings():
    settings_type = import_settings_type()
    return settings_type(
        service_name="bit-data-workbench",
        ui_title="DAAIFL Workbench",
        image_version="0.5.7",
        port=8000,
        duckdb_database=Path("/tmp/workspace/workspace.duckdb"),
        duckdb_extension_directory=Path("/opt/duckdb/extensions"),
        max_result_rows=200,
        s3_endpoint="ecspr01.sz.admin.ch:9021",
        s3_bucket="vat-smoke-test",
        s3_access_key_id="key",
        s3_access_key_id_file=None,
        s3_secret_access_key="secret",
        s3_secret_access_key_file=None,
        s3_url_style="path",
        s3_use_ssl=True,
        s3_verify_ssl=True,
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


class _FakeEvents:
    def __init__(self) -> None:
        self.registrations: list[tuple[str, object]] = []

    def register(self, event_name: str, handler) -> None:
        self.registrations.append((event_name, handler))


class _FakeMeta:
    def __init__(self) -> None:
        self.events = _FakeEvents()


class _FakeClient:
    def __init__(self) -> None:
        self.meta = _FakeMeta()


class _FakeRequest:
    def __init__(self, body: object) -> None:
        self.body = body
        self.headers: dict[str, str] = {}


class _RetryingBucketClient:
    def __init__(
        self,
        *,
        list_responses,
        delete_bucket_side_effects=None,
    ) -> None:
        self.meta = _FakeMeta()
        self._list_responses = list(list_responses)
        self._delete_bucket_side_effects = list(
            delete_bucket_side_effects or []
        )
        self.list_calls = 0
        self.delete_calls = 0

    def head_bucket(self, **_kwargs) -> None:
        return None

    def list_objects_v2(self, **_kwargs):
        self.list_calls += 1
        if self._list_responses:
            return self._list_responses.pop(0)
        return {"KeyCount": 0, "Contents": []}

    def delete_bucket(self, **_kwargs) -> None:
        self.delete_calls += 1
        if self._delete_bucket_side_effects:
            effect = self._delete_bucket_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        return None


class _DeleteTrackingClient:
    def __init__(
        self,
        *,
        list_responses=None,
        list_object_versions_side_effects=None,
        list_object_versions_responses=None,
        delete_bucket_side_effects=None,
        delete_objects_side_effects=None,
        delete_objects_responses=None,
        delete_object_side_effects=None,
    ) -> None:
        self.meta = _FakeMeta()
        self._list_responses = list(list_responses or [])
        self._list_object_versions_side_effects = list(
            list_object_versions_side_effects or []
        )
        self._list_object_versions_responses = list(
            list_object_versions_responses or []
        )
        self._delete_bucket_side_effects = list(
            delete_bucket_side_effects or []
        )
        self._delete_objects_side_effects = list(
            delete_objects_side_effects or []
        )
        self._delete_objects_responses = list(delete_objects_responses or [])
        self._delete_object_side_effects = list(delete_object_side_effects or [])
        self.delete_objects_calls: list[list[dict[str, str]]] = []
        self.delete_object_calls: list[dict[str, str]] = []

    def head_bucket(self, **_kwargs) -> None:
        return None

    def list_objects_v2(self, **_kwargs):
        if self._list_responses:
            return self._list_responses.pop(0)
        return {"KeyCount": 0, "Contents": []}

    def list_object_versions(self, **_kwargs):
        if self._list_object_versions_side_effects:
            effect = self._list_object_versions_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        if self._list_object_versions_responses:
            return self._list_object_versions_responses.pop(0)
        return {"Versions": [], "DeleteMarkers": []}

    def delete_objects(self, **kwargs):
        self.delete_objects_calls.append(kwargs["Delete"]["Objects"])
        if self._delete_objects_side_effects:
            effect = self._delete_objects_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        if self._delete_objects_responses:
            return self._delete_objects_responses.pop(0)
        return {}

    def delete_object(self, **kwargs) -> None:
        identifier = {"Key": kwargs["Key"]}
        if kwargs.get("VersionId"):
            identifier["VersionId"] = kwargs["VersionId"]
        self.delete_object_calls.append(identifier)
        if self._delete_object_side_effects:
            effect = self._delete_object_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        return None

    def delete_bucket(self, **_kwargs) -> None:
        if self._delete_bucket_side_effects:
            effect = self._delete_bucket_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        return None


class _BootstrapFlowClient:
    def __init__(self, s3_storage) -> None:
        self.meta = _FakeMeta()
        self._s3_storage = s3_storage
        self._buckets: set[str] = set()
        self._objects: dict[str, set[str]] = {}
        self.create_bucket_calls: list[str] = []
        self.upload_calls: list[dict[str, str]] = []
        self.delete_bucket_calls: list[str] = []

    def head_bucket(self, **kwargs) -> None:
        bucket = kwargs["Bucket"]
        if bucket not in self._buckets:
            raise self._s3_storage.ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadBucket",
            )

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        if bucket not in self._buckets:
            raise self._s3_storage.ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "ListObjectsV2",
            )
        prefix = str(kwargs.get("Prefix") or "")
        contents = [
            {"Key": key}
            for key in sorted(self._objects.get(bucket, set()))
            if not prefix or key.startswith(prefix)
        ]
        max_keys = int(kwargs.get("MaxKeys") or len(contents) or 1000)
        limited = contents[:max_keys]
        return {
            "KeyCount": len(limited),
            "Contents": limited,
            "IsTruncated": False,
        }

    def list_object_versions(self, **kwargs):
        bucket = kwargs["Bucket"]
        if bucket not in self._buckets:
            raise self._s3_storage.ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "ListObjectVersions",
            )
        return {"Versions": [], "DeleteMarkers": [], "IsTruncated": False}

    def create_bucket(self, **kwargs) -> None:
        bucket = kwargs["Bucket"]
        self.create_bucket_calls.append(bucket)
        self._buckets.add(bucket)
        self._objects.setdefault(bucket, set())

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        if bucket not in self._buckets:
            raise self._s3_storage.ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "PutObject",
            )
        self.upload_calls.append({"Bucket": bucket, "Key": key, "Filename": filename})
        self._objects.setdefault(bucket, set()).add(key)

    def delete_objects(self, **kwargs):
        bucket = kwargs["Bucket"]
        for item in kwargs["Delete"]["Objects"]:
            self._objects.setdefault(bucket, set()).discard(item["Key"])
        return {}

    def delete_object(self, **kwargs) -> None:
        bucket = kwargs["Bucket"]
        self._objects.setdefault(bucket, set()).discard(kwargs["Key"])

    def delete_bucket(self, **kwargs) -> None:
        bucket = kwargs["Bucket"]
        self.delete_bucket_calls.append(bucket)
        if self._objects.get(bucket):
            raise self._s3_storage.ClientError(
                {
                    "Error": {
                        "Code": "BucketNotEmpty",
                        "Message": "The bucket you tried to delete is not empty.",
                    }
                },
                "DeleteBucket",
            )
        self._buckets.discard(bucket)
        self._objects.pop(bucket, None)


class S3StorageTests(unittest.TestCase):
    def test_content_md5_header_value_matches_expected_fixture(self) -> None:
        s3_storage = import_s3_storage()
        self.assertEqual(
            s3_storage._content_md5_header_value(b"<Delete/>"),
            "ePbrFwvNFIRnzBrIv+T8Ag==",
        )

    def test_inject_delete_objects_content_md5_sets_header(self) -> None:
        s3_storage = import_s3_storage()
        request = _FakeRequest("<Delete/>")

        s3_storage._inject_delete_objects_content_md5(request)

        self.assertEqual(
            request.headers["Content-MD5"],
            "ePbrFwvNFIRnzBrIv+T8Ag==",
        )

    def test_s3_client_registers_delete_objects_md5_handler(self) -> None:
        s3_storage = import_s3_storage()
        fake_client = _FakeClient()
        settings = build_settings()

        with patch.object(
            s3_storage.boto3,
            "client",
            return_value=fake_client,
        ):
            client = s3_storage.s3_client(settings)

        self.assertIs(client, fake_client)
        self.assertEqual(len(fake_client.meta.events.registrations), 1)
        event_name, handler = fake_client.meta.events.registrations[0]
        self.assertEqual(event_name, "before-sign.s3.DeleteObjects")
        self.assertIs(handler, s3_storage._inject_delete_objects_content_md5)

    def test_remove_s3_bucket_retries_until_bucket_is_empty(self) -> None:
        s3_storage = import_s3_storage()
        settings = build_settings()
        fake_client = _RetryingBucketClient(
            list_responses=[
                {"KeyCount": 1, "Contents": [{"Key": "delayed-object"}]},
                {"KeyCount": 0, "Contents": []},
            ]
        )

        with patch.object(
            s3_storage,
            "s3_client",
            return_value=fake_client,
        ), patch.object(
            s3_storage,
            "_bucket_is_accessible",
            side_effect=[True, False],
        ), patch.object(s3_storage.time, "sleep"):
            deleted = s3_storage.remove_s3_bucket(settings, "vat-smoke-test")

        self.assertTrue(deleted)
        self.assertEqual(fake_client.list_calls, 2)
        self.assertEqual(fake_client.delete_calls, 1)

    def test_remove_s3_bucket_retries_bucket_not_empty_error(self) -> None:
        s3_storage = import_s3_storage()
        settings = build_settings()
        bucket_not_empty_error = s3_storage.ClientError(
            {
                "Error": {
                    "Code": "BucketNotEmpty",
                    "Message": "The bucket you tried to delete is not empty.",
                }
            },
            "DeleteBucket",
        )
        fake_client = _RetryingBucketClient(
            list_responses=[
                {"KeyCount": 0, "Contents": []},
                {"KeyCount": 0, "Contents": []},
            ],
            delete_bucket_side_effects=[bucket_not_empty_error, None],
        )

        with patch.object(
            s3_storage,
            "s3_client",
            return_value=fake_client,
        ), patch.object(
            s3_storage,
            "_bucket_is_accessible",
            side_effect=[True, False],
        ), patch.object(s3_storage.time, "sleep"):
            deleted = s3_storage.remove_s3_bucket(settings, "vat-smoke-test")

        self.assertTrue(deleted)
        self.assertEqual(fake_client.delete_calls, 2)

    def test_remove_s3_bucket_logs_delete_bucket_access_denied(self) -> None:
        s3_storage = import_s3_storage()
        settings = build_settings()
        access_denied_error = s3_storage.ClientError(
            {
                "Error": {
                    "Code": "AccessDenied",
                    "Message": "Access Denied",
                }
            },
            "DeleteBucket",
        )
        fake_client = _RetryingBucketClient(
            list_responses=[{"KeyCount": 0, "Contents": []}],
            delete_bucket_side_effects=[access_denied_error],
        )

        with patch.object(
            s3_storage,
            "s3_client",
            return_value=fake_client,
        ), patch.object(
            s3_storage,
            "_bucket_is_accessible",
            return_value=True,
        ), self.assertLogs("bit_data_workbench.backend.s3_storage", level="WARNING") as logs:
            with self.assertRaisesRegex(ValueError, "AccessDenied: Access Denied"):
                s3_storage.remove_s3_bucket(settings, "vat-smoke-test", max_attempts=1)

        self.assertTrue(
            any("operation=DeleteBucket" in message for message in logs.output)
        )

    def test_delete_s3_bucket_allows_empty_bucket_when_versions_denied(
        self,
    ) -> None:
        s3_storage = import_s3_storage()
        settings = build_settings()
        access_denied_error = s3_storage.ClientError(
            {
                "Error": {
                    "Code": "AccessDenied",
                    "Message": "Access Denied",
                }
            },
            "ListObjectVersions",
        )
        fake_client = _RetryingBucketClient(
            list_responses=[{"KeyCount": 0, "Contents": []}]
        )

        with patch.object(
            s3_storage,
            "s3_client",
            return_value=fake_client,
        ), patch.object(
            s3_storage,
            "iter_s3_object_versions",
            side_effect=access_denied_error,
        ), patch.object(
            s3_storage,
            "_bucket_is_accessible",
            return_value=True,
        ):
            deleted = s3_storage.delete_s3_bucket(settings, "vat-smoke-test")

        self.assertEqual(deleted, 0)

    def test_delete_s3_bucket_deletes_visible_keys_when_versions_denied(
        self,
    ) -> None:
        s3_storage = import_s3_storage()
        settings = build_settings()
        access_denied_error = s3_storage.ClientError(
            {
                "Error": {
                    "Code": "AccessDenied",
                    "Message": "Access Denied",
                }
            },
            "ListObjectVersions",
        )
        fake_client = _DeleteTrackingClient(
            list_responses=[
                {"KeyCount": 1, "Contents": [{"Key": "foo.parquet"}]}
            ]
        )

        with patch.object(
            s3_storage,
            "s3_client",
            return_value=fake_client,
        ), patch.object(
            s3_storage,
            "iter_s3_object_versions",
            side_effect=access_denied_error,
        ), patch.object(
            s3_storage,
            "_bucket_is_accessible",
            return_value=True,
        ):
            deleted = s3_storage.delete_s3_bucket(settings, "vat-smoke-test")

        self.assertEqual(deleted, 1)
        self.assertEqual(
            fake_client.delete_objects_calls,
            [[{"Key": "foo.parquet"}]],
        )

    def test_delete_s3_keys_falls_back_to_individual_deletes(
        self,
    ) -> None:
        s3_storage = import_s3_storage()
        access_denied_error = s3_storage.ClientError(
            {
                "Error": {
                    "Code": "AccessDenied",
                    "Message": "Access Denied",
                }
            },
            "DeleteObjects",
        )
        fake_client = _DeleteTrackingClient(
            delete_objects_side_effects=[access_denied_error]
        )

        deleted = s3_storage.delete_s3_keys(
            fake_client,
            "vat-smoke-test",
            ["alpha.parquet", "beta.parquet"],
        )

        self.assertEqual(deleted, 2)
        self.assertEqual(
            fake_client.delete_object_calls,
            [
                {"Key": "alpha.parquet"},
                {"Key": "beta.parquet"},
            ],
        )

    def test_delete_s3_object_ignores_null_version_id(
        self,
    ) -> None:
        s3_storage = import_s3_storage()
        fake_client = _DeleteTrackingClient()

        s3_storage.delete_s3_object(
            fake_client,
            "vat-smoke-test",
            {"Key": "generated/part-00001.parquet", "VersionId": "null"},
        )

        self.assertEqual(
            fake_client.delete_object_calls,
            [
                {"Key": "generated/part-00001.parquet"},
            ],
        )

    def test_delete_s3_bucket_strips_null_version_id_from_bulk_delete(
        self,
    ) -> None:
        s3_storage = import_s3_storage()
        settings = build_settings()
        fake_client = _DeleteTrackingClient()

        with patch.object(
            s3_storage,
            "s3_client",
            return_value=fake_client,
        ), patch.object(
            s3_storage,
            "iter_s3_object_versions",
            return_value=[{"Key": "foo.parquet", "VersionId": "null"}],
        ), patch.object(
            s3_storage,
            "_bucket_is_accessible",
            return_value=True,
        ):
            deleted = s3_storage.delete_s3_bucket(settings, "vat-smoke-test")

        self.assertEqual(deleted, 1)
        self.assertEqual(
            fake_client.delete_objects_calls,
            [[{"Key": "foo.parquet"}]],
        )

    def test_remove_s3_bucket_reports_hidden_versions(self) -> None:
        s3_storage = import_s3_storage()
        settings = build_settings()
        bucket_not_empty_error = s3_storage.ClientError(
            {
                "Error": {
                    "Code": "BucketNotEmpty",
                    "Message": "The bucket you tried to delete is not empty.",
                }
            },
            "DeleteBucket",
        )
        fake_client = _DeleteTrackingClient(
            list_responses=[{"KeyCount": 0, "Contents": []}],
            list_object_versions_responses=[
                {"Versions": [{"Key": "foo.parquet", "VersionId": "v1"}]}
            ],
            delete_bucket_side_effects=[bucket_not_empty_error],
        )

        with patch.object(
            s3_storage,
            "s3_client",
            return_value=fake_client,
        ), patch.object(
            s3_storage,
            "_bucket_is_accessible",
            return_value=True,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "still contains object versions or delete markers",
            ):
                s3_storage.remove_s3_bucket(
                    settings,
                    "vat-smoke-test",
                    max_attempts=1,
                )

    def test_s3_bootstrap_create_upload_and_delete_bucket(self) -> None:
        s3_storage = import_s3_storage()
        settings = build_settings()
        fake_client = _BootstrapFlowClient(s3_storage)

        with TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / "bootstrap.csv"
            local_path.write_text("value\n1\n", encoding="utf-8")

            with patch.object(
                s3_storage,
                "s3_client",
                return_value=fake_client,
            ), patch.object(s3_storage.time, "sleep"):
                created = s3_storage.ensure_s3_bucket(settings, "bootstrap-bucket")
                s3_storage.upload_s3_file(
                    fake_client,
                    local_path=local_path,
                    bucket="bootstrap-bucket",
                    key="bootstrap/data.csv",
                )
                deleted_keys = s3_storage.delete_s3_bucket(settings, "bootstrap-bucket")
                bucket_deleted = s3_storage.remove_s3_bucket(settings, "bootstrap-bucket")

        self.assertTrue(created)
        self.assertEqual(fake_client.create_bucket_calls, ["bootstrap-bucket"])
        self.assertEqual(
            fake_client.upload_calls,
            [
                {
                    "Bucket": "bootstrap-bucket",
                    "Key": "bootstrap/data.csv",
                    "Filename": str(local_path),
                }
            ],
        )
        self.assertEqual(deleted_keys, 1)
        self.assertTrue(bucket_deleted)
        self.assertEqual(fake_client.delete_bucket_calls, ["bootstrap-bucket"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

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
        image_version="0.4.4",
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

    def test_delete_s3_object_retries_plain_delete_for_null_version_access_denied(
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
            "DeleteObject",
        )
        fake_client = _DeleteTrackingClient(
            delete_object_side_effects=[access_denied_error]
        )

        s3_storage.delete_s3_object(
            fake_client,
            "vat-smoke-test",
            {"Key": "generated/part-00001.parquet", "VersionId": "null"},
        )

        self.assertEqual(
            fake_client.delete_object_calls,
            [
                {
                    "Key": "generated/part-00001.parquet",
                    "VersionId": "null",
                },
                {"Key": "generated/part-00001.parquet"},
            ],
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


if __name__ == "__main__":
    unittest.main()

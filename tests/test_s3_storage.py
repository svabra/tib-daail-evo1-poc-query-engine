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
        image_version="0.3.36",
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


if __name__ == "__main__":
    unittest.main()

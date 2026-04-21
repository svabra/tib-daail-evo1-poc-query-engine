from __future__ import annotations

from io import BytesIO
from pathlib import Path
import asyncio
import json
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from starlette.datastructures import Headers, UploadFile


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import (  # noqa: E402
    copy_local_workspace_export_to_s3,
    move_local_workspace_export_to_s3,
)
from bit_data_workbench.backend.local_workspace_transfers import (  # noqa: E402
    LocalWorkspaceTransferManager,
    normalize_local_workspace_transfer_filename,
)
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


def make_settings(database_path: Path) -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIFL Workbench",
        image_version=CURRENT_VERSION,
        port=8000,
        duckdb_database=database_path,
        duckdb_extension_directory=database_path.parent / "duckdb-ext",
        service_consumption_data_dir=database_path.parent / "service-consumption",
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
        pg_host="127.0.0.1",
        pg_port="5432",
        pg_user="postgres",
        pg_password="postgres",
        pg_oltp_database="oltp",
        pg_olap_database="olap",
        pod_name=None,
        pod_namespace=None,
        pod_ip=None,
        node_name=None,
    )


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[dict[str, object]] = []

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, object] | None = None,
    ) -> None:
        self.uploads.append(
            {
                "filename": filename,
                "bucket": bucket,
                "key": key,
                "extra_args": ExtraArgs,
                "bytes": Path(filename).read_bytes(),
            }
        )


class FakeWorkbenchService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def move_local_workspace_export_to_s3(self, **kwargs) -> dict[str, str]:
        self.calls.append(dict(kwargs))
        return {
            "entryId": kwargs["entry_id"],
            "bucket": kwargs["bucket"],
            "key": "shared/tax-office.csv",
            "fileName": kwargs["file_name"],
            "path": f"s3://{kwargs['bucket']}/shared/tax-office.csv",
            "message": "Moved tax-office.csv.",
        }

    def copy_local_workspace_export_to_s3(self, **kwargs) -> dict[str, str]:
        self.calls.append(dict(kwargs))
        return {
            "entryId": kwargs["entry_id"],
            "bucket": kwargs["bucket"],
            "key": "shared/tax-office.csv",
            "fileName": kwargs["file_name"],
            "path": f"s3://{kwargs['bucket']}/shared/tax-office.csv",
            "message": "Copied tax-office.csv.",
        }


class LocalWorkspaceTransferTests(TestCase):
    def test_normalize_transfer_filename_discards_folder_segments(self) -> None:
        self.assertEqual(
            normalize_local_workspace_transfer_filename("nested/folder/tax-office.csv"),
            "tax-office.csv",
        )

    def test_move_to_s3_uploads_bytes_with_local_workspace_metadata(self) -> None:
        with TemporaryDirectory() as temp_dir:
            manager = LocalWorkspaceTransferManager(
                settings=make_settings(Path(temp_dir) / "workspace.duckdb")
            )
            fake_client = FakeS3Client()

            with patch("bit_data_workbench.backend.local_workspace_transfers.ensure_s3_bucket"), patch(
                "bit_data_workbench.backend.local_workspace_transfers.s3_client",
                return_value=fake_client,
            ):
                result = manager.move_to_s3(
                    entry_id="entry-1",
                    file_name="tax-office.csv",
                    mime_type="text/csv",
                    file_bytes=b"id,name\n1,Zurich\n",
                    bucket="shared-workspace",
                    prefix="imports",
                )

        self.assertEqual(result.bucket, "shared-workspace")
        self.assertEqual(result.key, "imports/tax-office.csv")
        self.assertEqual(result.path, "s3://shared-workspace/imports/tax-office.csv")
        self.assertEqual(len(fake_client.uploads), 1)
        self.assertEqual(fake_client.uploads[0]["bucket"], "shared-workspace")
        self.assertEqual(fake_client.uploads[0]["key"], "imports/tax-office.csv")
        self.assertEqual(fake_client.uploads[0]["bytes"], b"id,name\n1,Zurich\n")
        self.assertEqual(
            fake_client.uploads[0]["extra_args"],
            {
                "Metadata": {
                    "bdw_source": "local-workspace",
                    "bdw_mime_type": "text/csv",
                }
            },
        )

    def test_copy_to_s3_uses_copy_message(self) -> None:
        with TemporaryDirectory() as temp_dir:
            manager = LocalWorkspaceTransferManager(
                settings=make_settings(Path(temp_dir) / "workspace.duckdb")
            )
            fake_client = FakeS3Client()

            with patch("bit_data_workbench.backend.local_workspace_transfers.ensure_s3_bucket"), patch(
                "bit_data_workbench.backend.local_workspace_transfers.s3_client",
                return_value=fake_client,
            ):
                result = manager.copy_to_s3(
                    entry_id="entry-2",
                    file_name="tax-office.csv",
                    mime_type="text/csv",
                    file_bytes=b"id,name\n1,Zurich\n",
                    bucket="shared-workspace",
                    prefix="copies",
                )

        self.assertEqual(result.bucket, "shared-workspace")
        self.assertEqual(result.key, "copies/tax-office.csv")
        self.assertEqual(result.message, "Copied tax-office.csv to s3://shared-workspace/copies/tax-office.csv.")
        self.assertEqual(len(fake_client.uploads), 1)
        self.assertEqual(fake_client.uploads[0]["key"], "copies/tax-office.csv")

    def test_move_route_forwards_uploaded_file_to_service(self) -> None:
        service = FakeWorkbenchService()
        upload = UploadFile(
            file=BytesIO(b"id,name\n1,Zurich\n"),
            filename="tax-office.csv",
            headers=Headers({"content-type": "text/csv"}),
        )

        response = asyncio.run(
            move_local_workspace_export_to_s3(
                file=upload,
                entry_id="entry-1",
                bucket="shared-workspace",
                prefix="shared",
                file_name="tax-office.csv",
                mime_type="text/csv",
                service=service,
                workbench_client_id="client-alpha",
            )
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["bucket"], "shared-workspace")
        self.assertEqual(payload["entryId"], "entry-1")
        self.assertEqual(len(service.calls), 1)
        self.assertEqual(service.calls[0]["client_id"], "client-alpha")
        self.assertEqual(service.calls[0]["entry_id"], "entry-1")
        self.assertEqual(service.calls[0]["bucket"], "shared-workspace")
        self.assertEqual(service.calls[0]["prefix"], "shared")
        self.assertEqual(service.calls[0]["file_name"], "tax-office.csv")
        self.assertEqual(service.calls[0]["mime_type"], "text/csv")
        self.assertEqual(service.calls[0]["file_bytes"], b"id,name\n1,Zurich\n")

    def test_copy_route_forwards_uploaded_file_to_service(self) -> None:
        service = FakeWorkbenchService()
        upload = UploadFile(
            file=BytesIO(b"id,name\n1,Zurich\n"),
            filename="tax-office.csv",
            headers=Headers({"content-type": "text/csv"}),
        )

        response = asyncio.run(
            copy_local_workspace_export_to_s3(
                file=upload,
                entry_id="entry-2",
                bucket="shared-workspace",
                prefix="copies",
                file_name="tax-office.csv",
                mime_type="text/csv",
                service=service,
            )
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["bucket"], "shared-workspace")
        self.assertEqual(payload["entryId"], "entry-2")
        self.assertEqual(len(service.calls), 1)
        self.assertEqual(service.calls[0]["entry_id"], "entry-2")
        self.assertEqual(service.calls[0]["bucket"], "shared-workspace")
        self.assertEqual(service.calls[0]["prefix"], "copies")
        self.assertEqual(service.calls[0]["file_name"], "tax-office.csv")
        self.assertEqual(service.calls[0]["mime_type"], "text/csv")
        self.assertEqual(service.calls[0]["file_bytes"], b"id,name\n1,Zurich\n")

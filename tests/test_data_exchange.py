from __future__ import annotations

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from fastapi import HTTPException


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.data_exchange import (  # noqa: E402
    DataExchangeManager,
    DataExchangeStore,
    DataExchangeUploadFileRequest,
    DataExchangeUploadSessionManager,
    is_data_exchange_key,
    normalize_data_exchange_prefix,
)
from bit_data_workbench.backend.ingestion_types.common.uploads import (  # noqa: E402
    IngestionLocalSource,
)
from bit_data_workbench.api.data_exchange import (  # noqa: E402
    DataExchangeMetadataPayload,
    list_data_exchange_files,
    update_data_exchange_file_metadata,
)
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
        self.deleted: list[tuple[str, str]] = []

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        ExtraArgs=None,
        Config=None,
    ) -> None:
        del ExtraArgs, Config
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def delete_object(self, **kwargs) -> None:
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        self.deleted.append((bucket, key))
        self.objects.pop((bucket, key), None)

    def get_object(self, **kwargs):
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        payload = self.objects[(bucket, key)]
        return {
            "Body": FakeBody(payload),
            "ContentLength": len(payload),
            "ContentType": "text/csv",
        }

    def copy_object(self, **kwargs) -> None:
        target = (kwargs["Bucket"], kwargs["Key"])
        source = kwargs["CopySource"]
        self.objects[target] = self.objects[(source["Bucket"], source["Key"])]


def manager_with_store(root: Path, fake_s3: FakeS3Client) -> DataExchangeManager:
    settings = make_settings()
    settings.duckdb_database = root / "workspace.duckdb"
    settings.data_exchange_prefix = "--data-exchange--/"
    settings.s3_bucket = "workspace"
    return DataExchangeManager(
        settings=settings,
        store=DataExchangeStore(root / "data-exchange.json"),
        s3_client_factory=lambda app_settings: fake_s3,
    )


class DataExchangeManagerTests(TestCase):
    def test_upload_stores_under_hidden_prefix_and_public_list_hides_key(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "alpha.csv"
            source.write_bytes(b"id,name\n1,alpha\n")
            fake_s3 = FakeS3Client()
            manager = manager_with_store(root, fake_s3)

            with patch("bit_data_workbench.backend.data_exchange.manager.ensure_s3_bucket"):
                result = manager.store_uploaded_sources(
                    sources=[IngestionLocalSource(file_name="alpha.csv", local_path=source)],
                    file_password="file-secret",
                    display_name="Alpha",
                    owner_contact="owner@example.test",
                    tags=["tax", "csv"],
                )

            self.assertEqual(result["importedCount"], 1)
            [stored_bucket, stored_key] = next(iter(fake_s3.objects.keys()))
            self.assertEqual(stored_bucket, "workspace")
            self.assertTrue(is_data_exchange_key(stored_key, "--data-exchange--/"))

            listing = manager.list_files()
            public_file = listing["files"][0]
            self.assertEqual(public_file["displayName"], "Alpha")
            self.assertNotIn("s3Key", public_file)
            self.assertTrue(public_file["isQueryable"])
            self.assertTrue(public_file["hasPassword"])

    def test_file_password_is_optional_for_unprotected_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "alpha.csv"
            source.write_bytes(b"id,name\n1,alpha\n")
            fake_s3 = FakeS3Client()
            manager = manager_with_store(root, fake_s3)

            with patch("bit_data_workbench.backend.data_exchange.manager.ensure_s3_bucket"):
                result = manager.store_uploaded_sources(
                    sources=[IngestionLocalSource(file_name="alpha.csv", local_path=source)],
                    file_password="",
                )

            file_id = result["files"][0]["fileId"]
            self.assertFalse(manager.list_files()["files"][0]["hasPassword"])

            token = manager.create_download_token(file_id=file_id, file_password="")
            artifact = manager.stream_download(file_id=file_id, token=token["token"])
            self.assertEqual(artifact.body.read(), b"id,name\n1,alpha\n")
            artifact.body.close()

            updated = manager.update_metadata(
                file_id=file_id,
                file_password="",
                display_name="Unprotected",
            )
            self.assertEqual(updated["displayName"], "Unprotected")

    def test_virtual_folders_group_files_and_must_be_empty_before_delete(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "alpha.csv"
            source.write_bytes(b"id,name\n1,alpha\n")
            fake_s3 = FakeS3Client()
            manager = manager_with_store(root, fake_s3)

            folder = manager.create_folder(name="Tax Returns")
            folder_id = str(folder["folderId"])

            with patch("bit_data_workbench.backend.data_exchange.manager.ensure_s3_bucket"):
                result = manager.store_uploaded_sources(
                    sources=[IngestionLocalSource(file_name="alpha.csv", local_path=source)],
                    file_password="",
                    folder_id=folder_id,
                )

            self.assertEqual(result["files"][0]["folderId"], folder_id)
            listing = manager.list_files()
            self.assertEqual(listing["folders"][0]["name"], "Tax Returns")
            self.assertEqual(listing["files"][0]["folderId"], folder_id)

            with self.assertRaisesRegex(ValueError, "empty"):
                manager.delete_folder(folder_id=folder_id)

            manager.delete_file(file_id=result["files"][0]["fileId"], file_password="")
            deleted = manager.delete_folder(folder_id=folder_id)
            self.assertTrue(deleted["ok"])
            self.assertFalse(manager.list_files()["folders"])

    def test_file_password_required_for_metadata_download_and_delete(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "alpha.csv"
            source.write_bytes(b"id,name\n1,alpha\n")
            fake_s3 = FakeS3Client()
            manager = manager_with_store(root, fake_s3)

            with patch("bit_data_workbench.backend.data_exchange.manager.ensure_s3_bucket"):
                result = manager.store_uploaded_sources(
                    sources=[IngestionLocalSource(file_name="alpha.csv", local_path=source)],
                    file_password="file-secret",
                )
            file_id = result["files"][0]["fileId"]

            with self.assertRaises(PermissionError):
                manager.update_metadata(
                    file_id=file_id,
                    file_password="wrong",
                    display_name="Renamed",
                )
            with self.assertRaises(PermissionError):
                manager.create_download_token(
                    file_id=file_id,
                    file_password="wrong",
                )
            with self.assertRaises(PermissionError):
                manager.delete_file(
                    file_id=file_id,
                    file_password="wrong",
                )

            token = manager.create_download_token(
                file_id=file_id,
                file_password="file-secret",
            )
            artifact = manager.stream_download(file_id=file_id, token=token["token"])
            self.assertEqual(artifact.body.read(), b"id,name\n1,alpha\n")
            artifact.body.close()

            updated = manager.update_metadata(
                file_id=file_id,
                file_password="file-secret",
                display_name="Renamed",
                description="updated metadata",
            )
            self.assertEqual(updated["displayName"], "Renamed")
            self.assertEqual(next(iter(fake_s3.objects.values())), b"id,name\n1,alpha\n")

            deleted = manager.delete_file(
                file_id=file_id,
                file_password="file-secret",
            )
            self.assertTrue(deleted["ok"])
            self.assertFalse(manager.list_files()["files"])

    def test_prepared_download_source_requires_password_and_supports_csv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "alpha.csv"
            source.write_bytes(b"id,name\n1,alpha\n")
            fake_s3 = FakeS3Client()
            manager = manager_with_store(root, fake_s3)

            with patch("bit_data_workbench.backend.data_exchange.manager.ensure_s3_bucket"):
                result = manager.store_uploaded_sources(
                    sources=[IngestionLocalSource(file_name="alpha.csv", local_path=source)],
                    file_password="file-secret",
                )
            file_id = result["files"][0]["fileId"]

            with self.assertRaises(PermissionError):
                manager.prepared_download_source(
                    file_id=file_id,
                    file_password="wrong",
                )

            prepared_source = manager.prepared_download_source(
                file_id=file_id,
                file_password="file-secret",
            )

            self.assertEqual(prepared_source["sourceKind"], "data_exchange_file")
            self.assertEqual(prepared_source["dataExchangeFileId"], file_id)
            self.assertEqual(prepared_source["format"], "csv")
            self.assertNotIn("filePassword", prepared_source)

    def test_copy_to_shared_s3_accepts_queryable_and_rejects_pdf(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            csv_path = root / "alpha.csv"
            csv_path.write_bytes(b"id,name\n1,alpha\n")
            pdf_path = root / "notice.pdf"
            pdf_path.write_bytes(b"%PDF-1.4")
            fake_s3 = FakeS3Client()
            manager = manager_with_store(root, fake_s3)

            with patch("bit_data_workbench.backend.data_exchange.manager.ensure_s3_bucket"):
                result = manager.store_uploaded_sources(
                    sources=[
                        IngestionLocalSource(file_name="alpha.csv", local_path=csv_path),
                        IngestionLocalSource(file_name="notice.pdf", local_path=pdf_path),
                    ],
                    file_password="file-secret",
                )
                csv_id = next(item["fileId"] for item in result["files"] if item["fileName"] == "alpha.csv")
                pdf_id = next(item["fileId"] for item in result["files"] if item["fileName"] == "notice.pdf")

                copied = manager.copy_to_shared_s3(
                    file_id=csv_id,
                    file_password="file-secret",
                    bucket="analytics",
                    prefix="incoming",
                )

            self.assertEqual(copied["importedCount"], 1)
            self.assertEqual(copied["imports"][0]["objectKey"], "incoming/alpha.csv")
            self.assertEqual(fake_s3.objects[("analytics", "incoming/alpha.csv")], b"id,name\n1,alpha\n")

            with self.assertRaisesRegex(ValueError, "queryable formats"):
                manager.copy_to_shared_s3(
                    file_id=pdf_id,
                    file_password="file-secret",
                    bucket="analytics",
                )


class DataExchangeUploadSessionManagerTests(TestCase):
    def test_upload_session_accepts_arbitrary_file_names_and_limit(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            settings.data_exchange_upload_max_bytes = 128
            manager = DataExchangeUploadSessionManager(settings=settings)

            state = manager.create_session(
                [DataExchangeUploadFileRequest(file_name="notice.pdf", size_bytes=128)]
            )
            self.assertEqual(state["files"][0]["fileName"], "notice.pdf")

            with self.assertRaisesRegex(ValueError, "upload size limit"):
                manager.create_session(
                    [DataExchangeUploadFileRequest(file_name="too-large.bin", size_bytes=129)]
                )

            with self.assertRaisesRegex(ValueError, "safe file name"):
                manager.create_session(
                    [DataExchangeUploadFileRequest(file_name="../secret.csv", size_bytes=1)]
                )

    def test_normalize_data_exchange_prefix_keeps_reserved_default_shape(self) -> None:
        self.assertEqual(normalize_data_exchange_prefix("--data-exchange--"), "--data-exchange--/")


class FakeDataExchangeRouteService:
    def __init__(self) -> None:
        self.updated_payload: dict[str, object] | None = None

    def data_exchange_files(self) -> dict[str, object]:
        return {"files": [{"fileId": "file-1", "fileName": "alpha.csv"}]}

    def update_data_exchange_file_metadata(self, **kwargs) -> dict[str, object]:
        self.updated_payload = dict(kwargs)
        if kwargs.get("file_password") != "file-secret":
            raise PermissionError("The DataExchange file password is invalid.")
        return {
            "fileId": kwargs["file_id"],
            "fileName": "alpha.csv",
            "displayName": kwargs["display_name"],
        }


class DataExchangeApiTests(TestCase):
    def test_list_route_is_open_without_workbench_password(self) -> None:
        service = FakeDataExchangeRouteService()

        response = list_data_exchange_files(service=service)

        self.assertEqual(response.status_code, 200)

    def test_metadata_route_requires_file_password_and_preserves_alias_payload(self) -> None:
        service = FakeDataExchangeRouteService()

        response = update_data_exchange_file_metadata(
            file_id="file-1",
            payload=DataExchangeMetadataPayload(
                filePassword="file-secret",
                displayName="Alpha renamed",
                ownerContact="owner@example.test",
                description="Updated",
                tags=["tax"],
            ),
            service=service,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(service.updated_payload["display_name"], "Alpha renamed")
        self.assertEqual(service.updated_payload["owner_contact"], "owner@example.test")

        with self.assertRaises(HTTPException) as context:
            update_data_exchange_file_metadata(
                file_id="file-1",
                payload=DataExchangeMetadataPayload(
                    filePassword="wrong",
                    displayName="Alpha renamed",
                ),
                service=service,
            )
        self.assertEqual(context.exception.status_code, 403)

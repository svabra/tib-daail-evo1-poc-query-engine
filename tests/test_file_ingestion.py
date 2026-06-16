from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import asyncio
import io
import json
import sys
import zipfile
from unittest import TestCase
from unittest.mock import patch

import duckdb
from openpyxl import Workbook
from starlette.datastructures import Headers, UploadFile

REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend.ingestion_types.common import (  # noqa: E402
    ArchivePolicy,
    extract_archive_files,
)
from bit_data_workbench.backend.s3_hidden import shared_notebooks_bucket_name
from bit_data_workbench.backend.ingestion_types.common.uploads import (  # noqa: E402
    IngestionLocalSource,
)
from bit_data_workbench.backend.ingestion_types.tabular import (  # noqa: E402
    FILE_INGESTOR_SPECS,
    FileIngestionManager,
    FileUploadFileRequest,
    FileUploadSessionManager,
    preview_parquet_upload_schema,
)
from bit_data_workbench.api.router import preview_parquet_schema  # noqa: E402
from test_csv_ingestion import (  # noqa: E402
    FakeConnection,
    FakeS3Client,
    THIRTY_GIB,
    make_settings,
    read_partitioned_upload_rows,
)


def zip_payload(entries: dict[str, bytes]) -> bytes:
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as handle:
        for name, content in entries.items():
            handle.writestr(name, content)
    return archive.getvalue()


def xlsx_payload() -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append(["id", "name"])
    worksheet.append([1, "alpha"])
    output = io.BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def parquet_payload() -> bytes:
    with TemporaryDirectory() as temp_dir:
        parquet_path = Path(temp_dir) / "alpha.parquet"
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(
                f"COPY (SELECT 1 AS id, 'alpha' AS name) TO '{parquet_path.as_posix()}' (FORMAT PARQUET)"
            )
        finally:
            connection.close()
        return parquet_path.read_bytes()


class SharedArchiveExtractionTests(TestCase):
    def test_extract_archive_uses_per_format_extension_policy(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive_path = root / "mixed.zip"
            archive_path.write_bytes(
                zip_payload(
                    {
                        "alpha.parquet": b"parquet-bytes",
                        "nested/beta.csv": b"id,name\n1,beta\n",
                    }
                )
            )

            with self.assertRaisesRegex(ValueError, "not a supported Parquet file"):
                extract_archive_files(
                    archive_path=archive_path,
                    output_dir=root / "extract",
                    policy=ArchivePolicy(
                        max_archive_bytes=THIRTY_GIB,
                        max_entry_bytes=THIRTY_GIB,
                        max_extracted_bytes=THIRTY_GIB,
                        max_entries=100,
                        max_expansion_ratio=100.0,
                    ),
                    allowed_extensions=(".parquet",),
                    format_label="Parquet",
                )

    def test_extract_archive_makes_duplicate_member_names_unique(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive_path = root / "json.zip"
            archive_path.write_bytes(
                zip_payload(
                    {
                        "alpha.jsonl": b'{"id":1}\n',
                        "nested/alpha.jsonl": b'{"id":2}\n',
                    }
                )
            )

            extracted = extract_archive_files(
                archive_path=archive_path,
                output_dir=root / "extract",
                policy=ArchivePolicy(
                    max_archive_bytes=THIRTY_GIB,
                    max_entry_bytes=THIRTY_GIB,
                    max_extracted_bytes=THIRTY_GIB,
                    max_entries=100,
                    max_expansion_ratio=100.0,
                ),
                allowed_extensions=(".json", ".jsonl", ".ndjson"),
                format_label="JSON",
            )

            self.assertEqual([item.file_name for item in extracted], ["alpha.jsonl", "alpha_2.jsonl"])


class FileUploadSessionManagerTests(TestCase):
    def test_large_file_formats_use_30_gib_session_limits(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            for ingestor_id in ("parquet", "json"):
                manager = FileUploadSessionManager(
                    settings=settings,
                    spec=FILE_INGESTOR_SPECS[ingestor_id],
                )
                accepted = manager.create_session(
                    [
                        FileUploadFileRequest(
                            file_name=f"large.{FILE_INGESTOR_SPECS[ingestor_id].allowed_extensions[0].lstrip('.')}",
                            size_bytes=THIRTY_GIB,
                        ),
                        FileUploadFileRequest(file_name="archive.zip", size_bytes=THIRTY_GIB),
                    ]
                )
                self.assertEqual(len(accepted["files"]), 2)

                with self.assertRaisesRegex(ValueError, "upload size limit"):
                    manager.create_session(
                        [
                            FileUploadFileRequest(
                                file_name=f"too-large.{FILE_INGESTOR_SPECS[ingestor_id].allowed_extensions[0].lstrip('.')}",
                                size_bytes=THIRTY_GIB + 1,
                            )
                        ]
                    )
                with self.assertRaisesRegex(ValueError, "upload size limit"):
                    manager.create_session(
                        [FileUploadFileRequest(file_name="too-large.zip", size_bytes=THIRTY_GIB + 1)]
                    )

    def test_xlsx_uses_conversion_limit_for_direct_entries(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            settings.ingestion_tabular_conversion_max_bytes = 8
            manager = FileUploadSessionManager(
                settings=settings,
                spec=FILE_INGESTOR_SPECS["xlsx"],
            )

            with self.assertRaisesRegex(ValueError, "upload size limit"):
                manager.create_session(
                    [FileUploadFileRequest(file_name="too-large.xlsx", size_bytes=9)]
                )


class FileIngestionManagerTests(TestCase):
    def test_preview_parquet_upload_schema_returns_columns(self) -> None:
        payload = preview_parquet_upload_schema(
            file_name="tax facts.parquet",
            input_file=io.BytesIO(parquet_payload()),
        )

        self.assertEqual(payload["fileName"], "tax facts.parquet")
        self.assertEqual(payload["columnCount"], 2)
        self.assertEqual(
            [(column["name"], column["dataType"]) for column in payload["columns"]],
            [("id", "INTEGER"), ("name", "VARCHAR")],
        )

    def test_parquet_schema_preview_route_reads_uploaded_file(self) -> None:
        class FakeSchemaPreviewService:
            def preview_parquet_schema(self, *, file_name, input_file):
                return preview_parquet_upload_schema(file_name=file_name, input_file=input_file)

        upload = UploadFile(
            file=io.BytesIO(parquet_payload()),
            filename="alpha.parquet",
            headers=Headers({"content-type": "application/vnd.apache.parquet"}),
        )

        response = asyncio.run(
            preview_parquet_schema(file=upload, service=FakeSchemaPreviewService())
        )
        payload = json.loads(response.body.decode("utf-8"))

        self.assertEqual(payload["fileName"], "alpha.parquet")
        self.assertEqual(payload["columnCount"], 2)
        self.assertEqual(payload["columns"][0]["name"], "id")

    def test_json_direct_and_zip_import_to_s3_create_one_object_per_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            direct = root / "alpha.jsonl"
            direct.write_text('{"id":1,"name":"alpha"}\n', encoding="utf-8")
            archive = root / "bundle.zip"
            archive.write_bytes(
                zip_payload(
                    {
                        "beta.jsonl": b'{"id":2,"name":"beta"}\n',
                        "nested/gamma.ndjson": b'{"id":3,"name":"gamma"}\n',
                    }
                )
            )
            settings = make_settings()
            fake_s3 = FakeS3Client()
            manager = FileIngestionManager(
                settings=settings,
                spec=FILE_INGESTOR_SPECS["json"],
                postgres_connection_factory=lambda target: None,
                s3_client_factory=lambda app_settings: fake_s3,
            )

            with patch(
                "bit_data_workbench.backend.ingestion_types.tabular.manager.ensure_s3_bucket"
            ):
                payload = manager.import_sources(
                    sources=[
                        IngestionLocalSource(file_name="alpha.jsonl", local_path=direct),
                        IngestionLocalSource(file_name="bundle.zip", local_path=archive),
                    ],
                    target_id="s3",
                    bucket="imports",
                    prefix="stage/json",
                )

            self.assertEqual(payload["importedCount"], 3)
            self.assertEqual(payload["failedCount"], 0)
            self.assertEqual(
                [upload[2] for upload in fake_s3.uploads],
                ["stage/json/alpha.jsonl", "stage/json/beta.jsonl", "stage/json/gamma.ndjson"],
            )

    def test_import_to_shared_notebooks_bucket_is_rejected_in_s3_target(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "alpha.jsonl"
            source.write_text('{"id":1,"name":"alpha"}\n', encoding="utf-8")
            settings = make_settings()
            target_bucket = shared_notebooks_bucket_name(settings)
            fake_s3 = FakeS3Client()
            manager = FileIngestionManager(
                settings=settings,
                spec=FILE_INGESTOR_SPECS["json"],
                postgres_connection_factory=lambda target: None,
                s3_client_factory=lambda app_settings: fake_s3,
            )

            with patch(
                "bit_data_workbench.backend.ingestion_types.tabular.manager.ensure_s3_bucket"
            ) as ensure_bucket:
                payload = manager.import_sources(
                    sources=[IngestionLocalSource(file_name="alpha.jsonl", local_path=source)],
                    target_id="s3",
                    bucket=target_bucket,
                    prefix="stage/json",
                )

            ensure_bucket.assert_not_called()
            self.assertEqual(payload["importedCount"], 0)
            self.assertEqual(payload["failedCount"], 1)
            self.assertEqual(payload["imports"][0]["fileName"], "alpha.jsonl")
            self.assertIn("shared notebook storage bucket is reserved", payload["imports"][0]["error"])
            self.assertEqual(fake_s3.uploads, [])

    def test_parquet_import_to_s3_echoes_recommended_optimization(self) -> None:
        with TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "alpha.parquet"
            source.write_bytes(parquet_payload())
            fake_s3 = FakeS3Client()
            manager = FileIngestionManager(
                settings=make_settings(),
                spec=FILE_INGESTOR_SPECS["parquet"],
                postgres_connection_factory=lambda target: None,
                s3_client_factory=lambda app_settings: fake_s3,
            )

            with patch(
                "bit_data_workbench.backend.ingestion_types.tabular.manager.ensure_s3_bucket"
            ):
                payload = manager.import_sources(
                    sources=[IngestionLocalSource(file_name="alpha.parquet", local_path=source)],
                    target_id="s3",
                    bucket="imports",
                    prefix="stage/parquet",
                    parquet_optimization={"mode": "recommended"},
                )

        self.assertEqual(payload["parquetOptimization"]["mode"], "recommended")
        self.assertEqual(payload["imports"][0]["parquetOptimization"]["mode"], "recommended")

    def test_parquet_import_to_s3_applies_manual_hive_partitioning(self) -> None:
        with TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "orders.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.execute(
                    f"""
                    COPY (
                        SELECT 2026 AS tax_year, 2 AS id, 'beta' AS name
                        UNION ALL
                        SELECT 2026 AS tax_year, 1 AS id, 'alpha' AS name
                        UNION ALL
                        SELECT 2025 AS tax_year, 3 AS id, 'gamma' AS name
                    ) TO '{source.as_posix()}' (FORMAT PARQUET)
                    """
                )
            finally:
                connection.close()
            fake_s3 = FakeS3Client()
            manager = FileIngestionManager(
                settings=make_settings(),
                spec=FILE_INGESTOR_SPECS["parquet"],
                postgres_connection_factory=lambda target: None,
                s3_client_factory=lambda app_settings: fake_s3,
            )

            with patch(
                "bit_data_workbench.backend.ingestion_types.tabular.manager.ensure_s3_bucket"
            ):
                payload = manager.import_sources(
                    sources=[IngestionLocalSource(file_name="orders.parquet", local_path=source)],
                    target_id="s3",
                    bucket="imports",
                    prefix="stage/parquet",
                    parquet_optimization={
                        "mode": "manual",
                        "hivePartitioning": True,
                        "partitionColumns": ["tax_year"],
                        "sortColumns": ["id"],
                    },
                )

        self.assertEqual(payload["importedCount"], 1)
        imported = payload["imports"][0]
        self.assertTrue(imported["partitioned"])
        self.assertEqual(imported["objectKey"], "stage/parquet/orders")
        self.assertEqual(imported["path"], "s3://imports/stage/parquet/orders/**/*.parquet")
        self.assertEqual(len(fake_s3.uploads), 2)
        self.assertTrue(any("tax_year=2025/" in upload[2] for upload in fake_s3.uploads))
        self.assertTrue(any("tax_year=2026/" in upload[2] for upload in fake_s3.uploads))

        rows = read_partitioned_upload_rows(
            fake_s3,
            key_prefix="stage/parquet/orders",
            hive_partitioning=True,
        )
        self.assertEqual(
            rows,
            [(2025, 3, "gamma"), (2026, 1, "alpha"), (2026, 2, "beta")],
        )

    def test_parquet_import_to_s3_hive_off_keeps_partition_columns_in_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "orders.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.execute(
                    f"""
                    COPY (
                        SELECT 2026 AS tax_year, 2 AS id, 'beta' AS name
                        UNION ALL
                        SELECT 2026 AS tax_year, 1 AS id, 'alpha' AS name
                        UNION ALL
                        SELECT 2025 AS tax_year, 3 AS id, 'gamma' AS name
                    ) TO '{source.as_posix()}' (FORMAT PARQUET)
                    """
                )
            finally:
                connection.close()
            fake_s3 = FakeS3Client()
            manager = FileIngestionManager(
                settings=make_settings(),
                spec=FILE_INGESTOR_SPECS["parquet"],
                postgres_connection_factory=lambda target: None,
                s3_client_factory=lambda app_settings: fake_s3,
            )

            with patch(
                "bit_data_workbench.backend.ingestion_types.tabular.manager.ensure_s3_bucket"
            ):
                payload = manager.import_sources(
                    sources=[IngestionLocalSource(file_name="orders.parquet", local_path=source)],
                    target_id="s3",
                    bucket="imports",
                    prefix="stage/no-hive",
                    parquet_optimization={
                        "mode": "manual",
                        "hivePartitioning": False,
                        "partitionColumns": ["tax_year"],
                        "sortColumns": ["id"],
                    },
                )

        imported = payload["imports"][0]
        self.assertTrue(imported["partitioned"])
        self.assertEqual(imported["parquetOptimization"]["hivePartitioning"], False)
        rows = read_partitioned_upload_rows(
            fake_s3,
            key_prefix="stage/no-hive/orders",
            hive_partitioning=False,
        )
        self.assertEqual(
            rows,
            [(2025, 3, "gamma"), (2026, 1, "alpha"), (2026, 2, "beta")],
        )

    def test_mixed_format_zip_imports_valid_members_and_reports_invalid_member(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive = root / "mixed.zip"
            archive.write_bytes(
                zip_payload(
                    {
                        "alpha.jsonl": b'{"id":1,"name":"alpha"}\n',
                        "notes.csv": b"id,name\n2,beta\n",
                    }
                )
            )
            settings = make_settings()
            fake_s3 = FakeS3Client()
            manager = FileIngestionManager(
                settings=settings,
                spec=FILE_INGESTOR_SPECS["json"],
                postgres_connection_factory=lambda target: None,
                s3_client_factory=lambda app_settings: fake_s3,
            )

            with patch(
                "bit_data_workbench.backend.ingestion_types.tabular.manager.ensure_s3_bucket"
            ):
                payload = manager.import_sources(
                    sources=[IngestionLocalSource(file_name="mixed.zip", local_path=archive)],
                    target_id="s3",
                    bucket="imports",
                    prefix="stage/json",
                )

            self.assertEqual(payload["importedCount"], 1)
            self.assertEqual(payload["failedCount"], 1)
            self.assertEqual(payload["imports"][0]["fileName"], "notes.csv")
            self.assertIn("not a supported JSON file", payload["imports"][0]["error"])
            self.assertEqual(fake_s3.uploads[0][2], "stage/json/alpha.jsonl")

    def test_parquet_zip_import_to_postgres_creates_one_table_per_member(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            parquet = parquet_payload()
            archive = root / "bundle.zip"
            archive.write_bytes(
                zip_payload(
                    {
                        "alpha.parquet": parquet,
                        "nested/beta.parquet": parquet,
                    }
                )
            )
            settings = make_settings()
            fake_connection = FakeConnection()
            manager = FileIngestionManager(
                settings=settings,
                spec=FILE_INGESTOR_SPECS["parquet"],
                postgres_connection_factory=lambda target: fake_connection,
            )

            payload = manager.import_sources(
                sources=[IngestionLocalSource(file_name="bundle.zip", local_path=archive)],
                target_id="pg_oltp",
                schema_name="stage",
                table_prefix="raw",
            )

            self.assertEqual(payload["importedCount"], 2)
            self.assertEqual(payload["failedCount"], 0)
            self.assertEqual(
                [item["relation"] for item in payload["imports"]],
                ["stage.raw_alpha", "stage.raw_beta"],
            )
            executed_sql = "\n".join(fake_connection.cursor_instance.executed)
            self.assertIn('CREATE SCHEMA IF NOT EXISTS "stage"', executed_sql)
            self.assertIn('CREATE TABLE "stage"."raw_alpha"', executed_sql)
            self.assertIn('CREATE TABLE "stage"."raw_beta"', executed_sql)

    def test_xlsx_and_xml_conversion_size_limit_is_reported(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for ingestor_id, file_name, payload in (
                ("xlsx", "too-large.xlsx", xlsx_payload()),
                ("xml", "too-large.xml", b"<rows><row><id>1</id></row></rows>"),
            ):
                local_path = root / file_name
                local_path.write_bytes(payload)
                settings = make_settings()
                settings.ingestion_tabular_conversion_max_bytes = 4
                manager = FileIngestionManager(
                    settings=settings,
                    spec=FILE_INGESTOR_SPECS[ingestor_id],
                    postgres_connection_factory=lambda target: None,
                )

                result = manager.import_sources(
                    sources=[IngestionLocalSource(file_name=file_name, local_path=local_path)],
                    target_id="s3",
                    bucket="imports",
                )

                self.assertEqual(result["importedCount"], 0)
                self.assertEqual(result["failedCount"], 1)
                self.assertIn("conversion size limit", result["imports"][0]["error"])

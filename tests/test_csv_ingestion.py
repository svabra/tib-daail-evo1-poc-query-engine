from __future__ import annotations

import io
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
import zipfile

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.ingestion_types.csv.manager import (  # noqa: E402
    CsvIngestionManager,
    CsvLocalSource,
    duckdb_type_to_postgres_type,
    normalize_csv_delimiter,
    normalize_csv_columns,
    normalize_csv_table_name,
)
from bit_data_workbench.backend.ingestion_types.csv.uploads import (  # noqa: E402
    CsvUploadFileRequest,
    CsvUploadSessionManager,
)
from bit_data_workbench.backend.ingestion_types.csv.s3_formats import (  # noqa: E402
    normalize_csv_s3_storage_format,
    resolve_csv_s3_file_name,
)
from bit_data_workbench.backend.ingestion_types.csv.validation import (  # noqa: E402
    detect_csv_delimiter,
    validate_csv_file,
)
from bit_data_workbench.backend.ingestion_types.parquet_optimization import (  # noqa: E402
    normalize_parquet_optimization_settings,
)
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.backend.s3_hidden import shared_notebooks_bucket_name  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)
THIRTY_GIB = 30 * 1024 * 1024 * 1024


class FakeUpload:
    def __init__(self, name: str, payload: bytes) -> None:
        self.filename = name
        self.file = io.BytesIO(payload)


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, str, dict[str, object] | None, bytes]] = []

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, object] | None = None,
        Config: object | None = None,
    ) -> None:
        self.uploads.append((filename, bucket, key, ExtraArgs, Path(filename).read_bytes()))


def read_partitioned_upload_rows(
    fake_client: FakeS3Client,
    *,
    key_prefix: str,
    hive_partitioning: bool,
) -> list[tuple[object, ...]]:
    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        for _filename, _bucket, key, _extra_args, payload_bytes in fake_client.uploads:
            relative = key.removeprefix(f"{key_prefix.rstrip('/')}/")
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload_bytes)
        connection = duckdb.connect(":memory:")
        try:
            return connection.execute(
                f"""
                SELECT tax_year, id, name
                FROM read_parquet(
                    '{root.as_posix()}/**/*.parquet',
                    hive_partitioning={'true' if hive_partitioning else 'false'}
                )
                ORDER BY tax_year, id
                """
            ).fetchall()
        finally:
            connection.close()


class FakeCopy:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    def __enter__(self) -> "FakeCopy":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def write(self, chunk: bytes) -> None:
        self.chunks.append(bytes(chunk))


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.copy_sql: str = ""
        self.copy_context = FakeCopy()

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def copy(self, sql: str) -> FakeCopy:
        self.copy_sql = sql
        return self.copy_context

    def fetchone(self) -> tuple[int]:
        return (3,)

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def close(self) -> None:
        return None


def make_settings() -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIFL Workbench",
        image_version=CURRENT_VERSION,
        port=8000,
        duckdb_database=Path("/tmp/workspace.duckdb"),
        duckdb_extension_directory=Path("/tmp/duckdb-ext"),
        service_consumption_data_dir=Path("/tmp/service-consumption"),
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


def zip_payload(entries: dict[str, str]) -> bytes:
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as handle:
        for name, content in entries.items():
            handle.writestr(name, content)
    return archive.getvalue()


def staged_upload_session_sources(
    *,
    settings: Settings,
    file_name: str,
    payload: bytes,
) -> list[CsvLocalSource]:
    upload_sessions = CsvUploadSessionManager(settings=settings)
    state = upload_sessions.create_session(
        [CsvUploadFileRequest(file_name=file_name, size_bytes=len(payload))]
    )
    session_id = str(state["sessionId"])
    file_id = str(state["files"][0]["fileId"])
    chunk_size = settings.ingestion_upload_chunk_bytes
    for offset in range(0, len(payload), chunk_size):
        end = min(offset + chunk_size, len(payload))
        upload_sessions.append_chunk(
            session_id=session_id,
            file_id=file_id,
            chunk_index=offset // chunk_size,
            content_range=f"bytes {offset}-{end - 1}/{len(payload)}",
            payload=payload[offset:end],
        )
    return upload_sessions.source_files(session_id)


class CsvIngestionHelperTests(TestCase):
    def test_default_csv_ingestion_size_limits_and_archive_policy_are_30_gib(self) -> None:
        settings = make_settings()
        manager = CsvIngestionManager(
            settings=settings,
            postgres_connection_factory=lambda target: None,
        )

        self.assertEqual(settings.ingestion_upload_max_archive_bytes, THIRTY_GIB)
        self.assertEqual(settings.ingestion_upload_max_csv_bytes, THIRTY_GIB)
        self.assertEqual(settings.ingestion_upload_max_extracted_bytes, THIRTY_GIB)
        self.assertEqual(manager._archive_policy.max_archive_bytes, THIRTY_GIB)
        self.assertEqual(manager._archive_policy.max_csv_bytes, THIRTY_GIB)
        self.assertEqual(manager._archive_policy.max_extracted_bytes, THIRTY_GIB)

    def test_normalize_csv_table_name_builds_sql_safe_identifier(self) -> None:
        self.assertEqual(
            normalize_csv_table_name("VAT Smoke Test.csv", prefix="raw"),
            "raw_vat_smoke_test",
        )
        self.assertEqual(
            normalize_csv_table_name("2026-results.csv"),
            "csv_import_2026_results",
        )

    def test_normalize_csv_columns_deduplicates_and_maps_types(self) -> None:
        self.assertEqual(
            normalize_csv_columns(
                [
                    ("VAT Amount", "INTEGER"),
                    ("VAT Amount", "DOUBLE"),
                    ("Filing Date", "DATE"),
                ]
            ),
            [
                ("vat_amount", "INTEGER"),
                ("vat_amount_2", "DOUBLE PRECISION"),
                ("filing_date", "DATE"),
            ],
        )

    def test_duckdb_type_to_postgres_type_defaults_to_text(self) -> None:
        self.assertEqual(duckdb_type_to_postgres_type("VARCHAR"), "TEXT")
        self.assertEqual(duckdb_type_to_postgres_type("DECIMAL(18,2)"), "DECIMAL(18,2)")

    def test_normalize_csv_delimiter_only_accepts_supported_single_characters(self) -> None:
        self.assertEqual(normalize_csv_delimiter(","), ",")
        self.assertEqual(normalize_csv_delimiter("\t"), "\t")
        self.assertEqual(normalize_csv_delimiter("::"), "")
        self.assertEqual(normalize_csv_delimiter(""), "")

    def test_normalize_csv_s3_storage_format_defaults_to_csv(self) -> None:
        self.assertEqual(normalize_csv_s3_storage_format(""), "csv")
        self.assertEqual(normalize_csv_s3_storage_format("PARQUET"), "parquet")
        self.assertEqual(resolve_csv_s3_file_name("vat_smoke.csv", "csv"), "vat_smoke.csv")
        self.assertEqual(resolve_csv_s3_file_name("vat_smoke.csv", "json"), "vat_smoke.jsonl")
        self.assertEqual(resolve_csv_s3_file_name("vat_smoke.csv", "parquet"), "vat_smoke.parquet")

    def test_detect_csv_delimiter_uses_semicolon_when_file_shape_matches(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "semicolon.csv"
            csv_path.write_text("id;name\n1;alpha\n2;beta\n", encoding="utf-8")
            self.assertEqual(detect_csv_delimiter(csv_path), ";")

    def test_validate_csv_file_rejects_unquoted_delimiter_inside_values(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "invalid.csv"
            csv_path.write_text(
                "id,name,amount\n1,alpha,9,536.31\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "CSV row width mismatch at line 2"):
                validate_csv_file(csv_path, delimiter=",", has_header=True)


class CsvIngestionManagerTests(TestCase):
    def test_parquet_optimization_validation_accepts_manual_source_column_names(self) -> None:
        settings = normalize_parquet_optimization_settings(
            {
                "mode": "manual",
                "partitionColumns": ["Tax Year", "Customer ID"],
                "sortColumns": ["Invoice Datum"],
                "indexColumns": ["id_"],
            },
            target_id="s3",
            storage_format="parquet",
        )

        self.assertEqual(settings.mode, "manual")
        self.assertEqual(settings.partition_columns, ["Tax Year", "Customer ID"])
        self.assertEqual(settings.sort_columns, ["Invoice Datum"])
        self.assertEqual(settings.index_columns, ["id_"])

    def test_parquet_optimization_validation_rejects_control_characters(self) -> None:
        with self.assertRaisesRegex(ValueError, "control characters"):
            normalize_parquet_optimization_settings(
                {
                    "mode": "manual",
                    "partitionColumns": ["Tax\nYear"],
                },
                target_id="s3",
                storage_format="parquet",
            )

    def test_import_csv_files_to_s3_uploads_files(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload("vat_smoke.csv", b"id,name\n1,alpha\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ) as ensure_bucket:
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming/april",
                delimiter=",",
                has_header=True,
            )

        ensure_bucket.assert_called_once()
        self.assertEqual(payload["importedCount"], 1)
        self.assertEqual(payload["failedCount"], 0)
        self.assertEqual(
            payload["imports"][0]["path"],
            "s3://csv-imports/incoming/april/vat_smoke.csv",
        )
        self.assertEqual(payload["imports"][0]["objectKey"], "incoming/april/vat_smoke.csv")
        self.assertEqual(payload["imports"][0]["objectKeyPrefix"], "incoming/april")
        self.assertEqual(payload["imports"][0]["storedFileName"], "vat_smoke.csv")
        self.assertEqual(len(fake_client.uploads), 1)
        self.assertEqual(
            fake_client.uploads[0][1:3],
            ("csv-imports", "incoming/april/vat_smoke.csv"),
        )
        self.assertEqual(
            fake_client.uploads[0][3],
            {
                "Metadata": {
                    "bdw_csv_has_header": "true",
                    "bdw_csv_delimiter": "comma",
                }
            },
        )
        self.assertEqual(payload["imports"][0]["storageFormat"], "csv")

    def test_import_to_shared_notebooks_bucket_is_rejected_in_s3_target(self) -> None:
        settings = make_settings()
        target_bucket = shared_notebooks_bucket_name(settings)
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=settings,
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload("shared-notebooks.csv", b"id,name\n1,alpha\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ) as ensure_bucket:
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket=target_bucket,
                delimiter=",",
                has_header=True,
            )

        ensure_bucket.assert_not_called()
        self.assertEqual(payload["importedCount"], 0)
        self.assertEqual(payload["failedCount"], 1)
        self.assertEqual(payload["imports"][0]["fileName"], "shared-notebooks.csv")
        self.assertIn(
            "shared notebook storage bucket is reserved",
            payload["imports"][0]["error"],
        )
        self.assertEqual(fake_client.uploads, [])

    def test_import_zip_files_to_s3_extracts_multiple_csv_entries(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as handle:
            handle.writestr("folder/alpha.csv", "id,name\n1,alpha\n")
            handle.writestr("beta.csv", "id,name\n2,beta\n")
        upload = FakeUpload("batch.zip", archive.getvalue())

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                delimiter=",",
                has_header=True,
            )

        self.assertEqual(payload["importedCount"], 2)
        self.assertEqual(payload["failedCount"], 0)
        self.assertEqual(
            [item["fileName"] for item in payload["imports"]],
            ["alpha.csv", "beta.csv"],
        )
        self.assertEqual(
            [upload_record[2] for upload_record in fake_client.uploads],
            ["alpha.csv", "beta.csv"],
        )

    def test_upload_session_zip_import_to_s3_creates_one_object_per_csv(self) -> None:
        fake_client = FakeS3Client()
        archive_payload = zip_payload(
            {
                "alpha.csv": "id,name\n1,alpha\n",
                "folder/beta.csv": "id,name\n2,beta\n",
            }
        )
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            settings.ingestion_upload_chunk_bytes = 17
            sources = staged_upload_session_sources(
                settings=settings,
                file_name="client-batch.zip",
                payload=archive_payload,
            )
            manager = CsvIngestionManager(
                settings=settings,
                postgres_connection_factory=lambda target: None,
                s3_client_factory=lambda settings: fake_client,
            )

            with patch(
                "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
            ) as ensure_bucket:
                payload = manager.import_csv_sources(
                    sources=sources,
                    target_id="s3",
                    bucket="client-imports",
                    prefix="incoming/batch-001",
                    delimiter=",",
                    has_header=True,
                )

        self.assertEqual(payload["importedCount"], 2)
        self.assertEqual(payload["failedCount"], 0)
        self.assertEqual(
            [item["fileName"] for item in payload["imports"]],
            ["alpha.csv", "beta.csv"],
        )
        self.assertEqual(
            [item["objectKey"] for item in payload["imports"]],
            ["incoming/batch-001/alpha.csv", "incoming/batch-001/beta.csv"],
        )
        self.assertEqual(
            {item["bucket"] for item in payload["imports"]},
            {"client-imports"},
        )
        self.assertEqual(
            [upload_record[1:3] for upload_record in fake_client.uploads],
            [
                ("client-imports", "incoming/batch-001/alpha.csv"),
                ("client-imports", "incoming/batch-001/beta.csv"),
            ],
        )
        self.assertEqual(ensure_bucket.call_count, 2)
        self.assertEqual(
            {call.args[1] for call in ensure_bucket.call_args_list},
            {"client-imports"},
        )

    def test_import_zip_files_rejects_archive_with_non_csv_entry(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as handle:
            handle.writestr("alpha.csv", "id,name\n1,alpha\n")
            handle.writestr("notes.txt", "not allowed\n")

        payload = manager.import_csv_files(
            files=[FakeUpload("batch.zip", archive.getvalue())],
            target_id="s3",
            bucket="csv-imports",
            delimiter=",",
            has_header=True,
        )

        self.assertEqual(payload["importedCount"], 0)
        self.assertEqual(payload["failedCount"], 1)
        self.assertIn("not a CSV file", payload["imports"][0]["error"])
        self.assertEqual(fake_client.uploads, [])

    def test_import_zip_files_rejects_zip_slip_entry(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as handle:
            handle.writestr("../evil.csv", "id,name\n1,alpha\n")

        payload = manager.import_csv_files(
            files=[FakeUpload("batch.zip", archive.getvalue())],
            target_id="s3",
            bucket="csv-imports",
            delimiter=",",
            has_header=True,
        )

        self.assertEqual(payload["importedCount"], 0)
        self.assertIn("unsafe path", payload["imports"][0]["error"])
        self.assertEqual(fake_client.uploads, [])

    def test_import_csv_sources_uses_existing_local_path_without_upload_copy(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )

        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "staged.csv"
            csv_path.write_text("id,name\n1,alpha\n", encoding="utf-8")
            with patch.object(manager, "_persist_upload") as persist_upload:
                with patch(
                    "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
                ):
                    payload = manager.import_csv_sources(
                        sources=[CsvLocalSource(file_name="staged.csv", local_path=csv_path)],
                        target_id="s3",
                        bucket="csv-imports",
                        delimiter=",",
                        has_header=True,
                    )

        persist_upload.assert_not_called()
        self.assertEqual(payload["importedCount"], 1)
        self.assertEqual(fake_client.uploads[0][0], str(csv_path))

    def test_import_csv_files_to_s3_auto_detects_delimiter_before_upload(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload("vat_smoke.csv", b"id;name\n1;alpha\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming/april",
                delimiter="",
                has_header=True,
            )

        self.assertEqual(payload["importedCount"], 1)
        self.assertEqual(payload["imports"][0]["objectKey"], "incoming/april/vat_smoke.csv")
        self.assertEqual(payload["imports"][0]["objectKeyPrefix"], "incoming/april")
        self.assertEqual(
            fake_client.uploads[0][3],
            {
                "Metadata": {
                    "bdw_csv_has_header": "true",
                    "bdw_csv_delimiter": "semicolon",
                }
            },
        )
        self.assertEqual(payload["imports"][0]["storageFormat"], "csv")

    def test_import_csv_files_to_s3_rejects_malformed_csv_shape(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload("vat_smoke.csv", b"id,name,amount\n1,alpha,9,536.31\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ) as ensure_bucket:
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                delimiter=",",
                has_header=True,
            )

        ensure_bucket.assert_not_called()
        self.assertEqual(payload["importedCount"], 0)
        self.assertEqual(payload["failedCount"], 1)
        self.assertIn("CSV row width mismatch at line 2", payload["imports"][0]["error"])
        self.assertEqual(fake_client.uploads, [])

    def test_import_csv_files_to_s3_converts_csv_to_parquet_before_upload(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload("vat_smoke.csv", b"id,name\n1,alpha\n2,beta\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming/april",
                delimiter=",",
                has_header=True,
                storage_format="parquet",
            )

        self.assertEqual(payload["importedCount"], 1)
        self.assertEqual(
            payload["imports"][0]["path"],
            "s3://csv-imports/incoming/april/vat_smoke.parquet",
        )
        self.assertEqual(payload["imports"][0]["objectKey"], "incoming/april/vat_smoke.parquet")
        self.assertEqual(payload["imports"][0]["objectKeyPrefix"], "incoming/april")
        self.assertEqual(payload["imports"][0]["storedFileName"], "vat_smoke.parquet")
        self.assertEqual(payload["imports"][0]["storageFormat"], "parquet")
        self.assertEqual(
            fake_client.uploads[0][1:3],
            ("csv-imports", "incoming/april/vat_smoke.parquet"),
        )
        self.assertIsNone(fake_client.uploads[0][3])
        self.assertEqual(fake_client.uploads[0][4][:4], b"PAR1")
        with TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "uploaded.parquet"
            parquet_path.write_bytes(fake_client.uploads[0][4])
            connection = duckdb.connect(":memory:")
            try:
                rows = connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
                ).fetchall()
            finally:
                connection.close()
        self.assertEqual(
            [(row[0], row[1]) for row in rows],
            [("id", "BIGINT"), ("name", "VARCHAR")],
        )

    def test_import_csv_files_to_s3_parquet_echoes_recommended_optimization(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[FakeUpload("vat_smoke.csv", b"id,name\n1,alpha\n")],
                target_id="s3",
                bucket="csv-imports",
                delimiter=",",
                has_header=True,
                storage_format="parquet",
                parquet_optimization={"mode": "recommended"},
            )

        self.assertEqual(payload["parquetOptimization"]["mode"], "recommended")
        self.assertEqual(payload["imports"][0]["parquetOptimization"]["mode"], "recommended")

    def test_import_csv_files_to_s3_parquet_applies_manual_hive_partitioning(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload(
            "vat_smoke.csv",
            (
                b"tax_year,id,name\n"
                b"2026,2,beta\n"
                b"2026,1,alpha\n"
                b"2025,3,gamma\n"
            ),
        )

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming/april",
                delimiter=",",
                has_header=True,
                storage_format="parquet",
                parquet_optimization={
                    "mode": "manual",
                    "hivePartitioning": True,
                    "partitionColumns": ["tax_year"],
                    "sortColumns": ["id"],
                    "createDuckdbCache": True,
                    "indexColumns": ["id"],
                },
            )

        self.assertEqual(payload["importedCount"], 1)
        imported = payload["imports"][0]
        self.assertTrue(imported["partitioned"])
        self.assertEqual(imported["objectKey"], "incoming/april/vat_smoke")
        self.assertEqual(
            imported["path"],
            "s3://csv-imports/incoming/april/vat_smoke/**/*.parquet",
        )
        self.assertEqual(imported["parquetOptimization"]["mode"], "manual")
        self.assertIn("ART index selections were recorded", imported["warnings"][0])
        uploaded_keys = [upload_record[2] for upload_record in fake_client.uploads]
        self.assertEqual(len(uploaded_keys), 2)
        self.assertTrue(any("tax_year=2025/" in key for key in uploaded_keys))
        self.assertTrue(any("tax_year=2026/" in key for key in uploaded_keys))

        rows = read_partitioned_upload_rows(
            fake_client,
            key_prefix="incoming/april/vat_smoke",
            hive_partitioning=True,
        )
        self.assertEqual(
            rows,
            [(2025, 3, "gamma"), (2026, 1, "alpha"), (2026, 2, "beta")],
        )

    def test_import_csv_files_to_s3_parquet_hive_off_keeps_partition_columns_in_files(
        self,
    ) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload(
            "vat_smoke.csv",
            (
                b"tax_year,id,name\n"
                b"2026,2,beta\n"
                b"2026,1,alpha\n"
                b"2025,3,gamma\n"
            ),
        )

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming/no-hive",
                delimiter=",",
                has_header=True,
                storage_format="parquet",
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
        self.assertTrue(
            any("tax_year=2025/" in upload_record[2] for upload_record in fake_client.uploads)
        )

        rows = read_partitioned_upload_rows(
            fake_client,
            key_prefix="incoming/no-hive/vat_smoke",
            hive_partitioning=False,
        )
        self.assertEqual(
            rows,
            [(2025, 3, "gamma"), (2026, 1, "alpha"), (2026, 2, "beta")],
        )

    def test_import_csv_files_to_s3_parquet_manual_cache_only_succeeds(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[
                    FakeUpload(
                        "federal_tax.csv",
                        b"taxpayer_id,tax_year,tax_due_chf\n1001,2025,4210.75\n",
                    )
                ],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming/cache-only",
                delimiter=",",
                has_header=True,
                storage_format="parquet",
                parquet_optimization={
                    "mode": "manual",
                    "createDuckdbCache": True,
                    "indexColumns": ["taxpayer_id"],
                },
            )

        imported = payload["imports"][0]
        self.assertNotIn("partitioned", imported)
        self.assertEqual(imported["objectKey"], "incoming/cache-only/federal_tax.parquet")
        self.assertEqual(imported["parquetOptimization"]["mode"], "manual")
        self.assertIn("ART index selections were recorded", imported["warnings"][0])
        self.assertEqual(len(fake_client.uploads), 1)

    def test_import_csv_files_to_s3_reports_step_two_progress(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload("vat_smoke.csv", b"id,name\n1,alpha\n")
        events: list[dict[str, object]] = []

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming",
                delimiter=",",
                has_header=True,
                storage_format="csv",
                progress_callback=events.append,
            )

        self.assertEqual(payload["importedCount"], 1)
        phases = [event.get("phase") for event in events]
        self.assertIn("csv_validate", phases)
        self.assertIn("s3_prepare", phases)
        self.assertIn("s3_preserve_csv", phases)
        self.assertIn("s3_bucket_check", phases)
        self.assertIn("s3_upload_start", phases)
        self.assertIn("s3_upload_done", phases)
        upload_start = next(event for event in events if event.get("phase") == "s3_upload_start")
        diagnostics = upload_start.get("diagnostics")
        self.assertIsInstance(diagnostics, dict)
        self.assertEqual(diagnostics["bucket"], "csv-imports")
        self.assertEqual(diagnostics["key"], "incoming/vat_smoke.csv")

    def test_import_csv_files_to_s3_parquet_handles_late_string_type_drift(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        rows = ["id,DOCO_AuslPositionStatistik\n"]
        rows.extend(f"{index},{index}\n" for index in range(25_000))
        rows.append("25000,H\n")
        upload = FakeUpload("KBPO2020.csv", "".join(rows).encode("utf-8"))

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming/april",
                delimiter=",",
                has_header=True,
                storage_format="parquet",
            )

        self.assertEqual(payload["importedCount"], 1)
        self.assertEqual(payload["failedCount"], 0)
        with TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "uploaded.parquet"
            parquet_path.write_bytes(fake_client.uploads[0][4])
            connection = duckdb.connect(":memory:")
            try:
                column_type = connection.execute(
                    f"""
                    DESCRIBE SELECT DOCO_AuslPositionStatistik
                    FROM read_parquet('{parquet_path.as_posix()}')
                    """
                ).fetchone()[1]
                last_value = connection.execute(
                    f"""
                    SELECT DOCO_AuslPositionStatistik
                    FROM read_parquet('{parquet_path.as_posix()}')
                    WHERE CAST(id AS VARCHAR) = '25000'
                    """
                ).fetchone()[0]
            finally:
                connection.close()
        self.assertEqual(column_type, "VARCHAR")
        self.assertEqual(last_value, "H")

    def test_import_csv_files_to_s3_converts_csv_to_jsonl_before_upload(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload("vat_smoke.csv", b"id,name\n1,alpha\n2,beta\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                prefix="incoming/april",
                delimiter=",",
                has_header=True,
                storage_format="json",
            )

        self.assertEqual(payload["importedCount"], 1)
        self.assertEqual(
            payload["imports"][0]["path"],
            "s3://csv-imports/incoming/april/vat_smoke.jsonl",
        )
        self.assertEqual(payload["imports"][0]["objectKey"], "incoming/april/vat_smoke.jsonl")
        self.assertEqual(payload["imports"][0]["objectKeyPrefix"], "incoming/april")
        self.assertEqual(payload["imports"][0]["storedFileName"], "vat_smoke.jsonl")
        self.assertEqual(payload["imports"][0]["storageFormat"], "json")
        self.assertEqual(
            fake_client.uploads[0][1:3],
            ("csv-imports", "incoming/april/vat_smoke.jsonl"),
        )
        self.assertIsNone(fake_client.uploads[0][3])
        uploaded_text = fake_client.uploads[0][4].decode("utf-8")
        self.assertEqual(
            uploaded_text.splitlines(),
            ['{"id":1,"name":"alpha"}', '{"id":2,"name":"beta"}'],
        )

    def test_import_csv_files_to_s3_rejects_unknown_storage_format(self) -> None:
        fake_client = FakeS3Client()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: None,
            s3_client_factory=lambda settings: fake_client,
        )
        upload = FakeUpload("vat_smoke.csv", b"id,name\n1,alpha\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.ensure_s3_bucket"
        ) as ensure_bucket:
            payload = manager.import_csv_files(
                files=[upload],
                target_id="s3",
                bucket="csv-imports",
                delimiter=",",
                has_header=True,
                storage_format="avro",
            )

        ensure_bucket.assert_not_called()
        self.assertEqual(payload["importedCount"], 0)
        self.assertIn(
            "S3 Object Storage format must be one of: csv, json, parquet.",
            payload["imports"][0]["error"],
        )

    def test_import_csv_files_to_postgres_creates_schema_table_and_copy(self) -> None:
        connection = FakeConnection()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: connection,
        )
        upload = FakeUpload("vat-smoke.csv", b"id,vat amount\n1,10\n2,20\n3,30\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.inspect_csv_file",
            return_value=([("id", "INTEGER"), ("vat amount", "DOUBLE")], 3),
        ):
            payload = manager.import_csv_files(
                files=[upload],
                target_id="pg_oltp",
                schema_name="stage",
                table_prefix="raw",
            )

        cursor = connection.cursor_instance
        self.assertEqual(payload["importedCount"], 1)
        self.assertIn('CREATE SCHEMA IF NOT EXISTS "stage"', cursor.executed[0])
        self.assertIn('DROP TABLE IF EXISTS "stage"."raw_vat_smoke"', cursor.executed[1])
        self.assertIn(
            'CREATE TABLE "stage"."raw_vat_smoke" ("id" INTEGER, "vat_amount" DOUBLE PRECISION)',
            cursor.executed[2],
        )
        self.assertIn(
            "COPY \"stage\".\"raw_vat_smoke\" (\"id\", \"vat_amount\") FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')",
            cursor.copy_sql,
        )
        self.assertTrue(cursor.copy_context.chunks)
        self.assertEqual(payload["imports"][0]["relation"], "stage.raw_vat_smoke")
        self.assertEqual(payload["imports"][0]["rowCount"], 3)

    def test_import_csv_files_to_postgres_honors_delimiter_header_and_replace_behavior(self) -> None:
        connection = FakeConnection()
        manager = CsvIngestionManager(
            settings=make_settings(),
            postgres_connection_factory=lambda target: connection,
        )
        upload = FakeUpload("vat-smoke.csv", b"1;10\n2;20\n")

        with patch(
            "bit_data_workbench.backend.ingestion_types.csv.manager.inspect_csv_file",
            return_value=([("column_1", "INTEGER"), ("column_2", "DOUBLE")], 2),
        ) as inspect_csv_file:
            payload = manager.import_csv_files(
                files=[upload],
                target_id="pg_oltp",
                schema_name="stage",
                table_prefix="raw",
                delimiter=";",
                has_header=False,
                replace_existing=False,
            )

        cursor = connection.cursor_instance
        inspect_csv_file.assert_called_once()
        self.assertEqual(inspect_csv_file.call_args.kwargs["delimiter"], ";")
        self.assertFalse(inspect_csv_file.call_args.kwargs["has_header"])
        self.assertNotIn('DROP TABLE IF EXISTS "stage"."raw_vat_smoke"', cursor.executed)
        self.assertIn(
            "COPY \"stage\".\"raw_vat_smoke\" (\"column_1\", \"column_2\") FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER ';')",
            cursor.copy_sql,
        )

    def test_upload_session_zip_import_to_postgres_creates_table_per_csv(self) -> None:
        connection = FakeConnection()
        archive_payload = zip_payload(
            {
                "alpha.csv": "id,name\n1,alpha\n",
                "folder/beta.csv": "id,name\n2,beta\n",
            }
        )
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            settings.ingestion_upload_chunk_bytes = 17
            sources = staged_upload_session_sources(
                settings=settings,
                file_name="client-batch.zip",
                payload=archive_payload,
            )
            manager = CsvIngestionManager(
                settings=settings,
                postgres_connection_factory=lambda target: connection,
            )

            payload = manager.import_csv_sources(
                sources=sources,
                target_id="pg_oltp",
                schema_name="stage",
                table_prefix="raw",
                delimiter=",",
                has_header=True,
            )

        cursor = connection.cursor_instance
        self.assertEqual(payload["importedCount"], 2)
        self.assertEqual(payload["failedCount"], 0)
        self.assertEqual(
            [item["relation"] for item in payload["imports"]],
            ["stage.raw_alpha", "stage.raw_beta"],
        )
        self.assertIn(
            'CREATE TABLE "stage"."raw_alpha" ("id" BIGINT, "name" TEXT)',
            cursor.executed,
        )
        self.assertIn(
            'CREATE TABLE "stage"."raw_beta" ("id" BIGINT, "name" TEXT)',
            cursor.executed,
        )
        self.assertEqual(payload["imports"][0]["rowCount"], 3)


class CsvUploadSessionManagerTests(TestCase):
    def test_upload_session_accepts_30_gib_csv_and_zip_size_limits(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            manager = CsvUploadSessionManager(settings=settings)

            state = manager.create_session(
                [
                    CsvUploadFileRequest(file_name="large.csv", size_bytes=THIRTY_GIB),
                    CsvUploadFileRequest(file_name="archive.zip", size_bytes=THIRTY_GIB),
                ]
            )

            self.assertEqual(
                [item["sizeBytes"] for item in state["files"]],
                [THIRTY_GIB, THIRTY_GIB],
            )
            with self.assertRaisesRegex(ValueError, "exceeds the configured upload size limit"):
                manager.create_session(
                    [CsvUploadFileRequest(file_name="too-large.csv", size_bytes=THIRTY_GIB + 1)]
                )
            with self.assertRaisesRegex(ValueError, "exceeds the configured upload size limit"):
                manager.create_session(
                    [CsvUploadFileRequest(file_name="too-large.zip", size_bytes=THIRTY_GIB + 1)]
                )

    def test_upload_session_accepts_chunks_reports_status_and_sources(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            settings.ingestion_upload_chunk_bytes = 4
            manager = CsvUploadSessionManager(settings=settings)

            state = manager.create_session(
                [CsvUploadFileRequest(file_name="large.csv", size_bytes=8)]
            )
            session_id = str(state["sessionId"])
            file_id = str(state["files"][0]["fileId"])

            first_state = manager.append_chunk(
                session_id=session_id,
                file_id=file_id,
                chunk_index=0,
                content_range="bytes 0-3/8",
                payload=b"id,n",
            )
            self.assertEqual(first_state["files"][0]["receivedBytes"], 4)
            retry_state = manager.append_chunk(
                session_id=session_id,
                file_id=file_id,
                chunk_index=0,
                content_range="bytes 0-3/8",
                payload=b"id,n",
            )
            self.assertEqual(retry_state["files"][0]["receivedBytes"], 4)
            complete_state = manager.append_chunk(
                session_id=session_id,
                file_id=file_id,
                chunk_index=1,
                content_range="bytes 4-7/8",
                payload=b"ame\n",
            )

            self.assertTrue(complete_state["files"][0]["complete"])
            sources = manager.source_files(session_id)
            self.assertEqual(len(sources), 1)
            self.assertEqual(sources[0].file_name, "large.csv")
            self.assertEqual(sources[0].local_path.read_bytes(), b"id,name\n")

    def test_upload_session_rejects_out_of_order_chunk(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            settings.ingestion_upload_chunk_bytes = 4
            manager = CsvUploadSessionManager(settings=settings)

            state = manager.create_session(
                [CsvUploadFileRequest(file_name="large.csv", size_bytes=8)]
            )
            with self.assertRaisesRegex(ValueError, "Expected byte offset 0"):
                manager.append_chunk(
                    session_id=str(state["sessionId"]),
                    file_id=str(state["files"][0]["fileId"]),
                    chunk_index=1,
                    content_range="bytes 4-7/8",
                    payload=b"ame\n",
                )

    def test_upload_session_cancel_removes_staged_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            settings.ingestion_upload_chunk_bytes = 4
            manager = CsvUploadSessionManager(settings=settings)

            state = manager.create_session(
                [CsvUploadFileRequest(file_name="large.csv", size_bytes=4)]
            )
            session_id = str(state["sessionId"])
            manager.append_chunk(
                session_id=session_id,
                file_id=str(state["files"][0]["fileId"]),
                chunk_index=0,
                content_range="bytes 0-3/4",
                payload=b"data",
            )
            session_dir = settings.ingestion_upload_dir / session_id
            self.assertTrue(session_dir.exists())

            cancel_state = manager.cancel_session(session_id)

            self.assertTrue(cancel_state["cancelled"])
            self.assertFalse(session_dir.exists())

    def test_upload_session_tracks_background_processing_result(self) -> None:
        with TemporaryDirectory() as temp_dir:
            settings = make_settings()
            settings.ingestion_upload_dir = Path(temp_dir) / "uploads"
            settings.ingestion_upload_chunk_bytes = 4
            manager = CsvUploadSessionManager(settings=settings)

            state = manager.create_session(
                [CsvUploadFileRequest(file_name="large.csv", size_bytes=4)]
            )
            session_id = str(state["sessionId"])
            manager.append_chunk(
                session_id=session_id,
                file_id=str(state["files"][0]["fileId"]),
                chunk_index=0,
                content_range="bytes 0-3/4",
                payload=b"data",
            )

            processing_state = manager.start_processing(session_id)

            self.assertEqual(processing_state["status"], "processing")
            self.assertTrue(processing_state["processingStarted"])
            self.assertEqual(processing_state["processingPhase"], "queued")
            self.assertIn("Queued server-side import", processing_state["processingMessage"])
            self.assertEqual(len(processing_state["processingEvents"]), 1)

            step_state = manager.update_processing_step(
                session_id,
                phase="s3_upload_start",
                message="Uploading large.csv to S3.",
                detail="Step 2 of 2: invoking the S3 upload.",
                diagnostics={"bucket": "workspace", "key": "large.csv"},
            )
            self.assertEqual(step_state["processingPhase"], "s3_upload_start")
            self.assertEqual(step_state["processingMessage"], "Uploading large.csv to S3.")
            self.assertEqual(step_state["processingEvents"][-1]["diagnostics"]["key"], "large.csv")

            already_processing = manager.start_processing(session_id)
            self.assertFalse(already_processing["processingStarted"])

            result = {
                "targetId": "s3",
                "importedCount": 1,
                "failedCount": 0,
                "imports": [{"fileName": "large.csv", "status": "imported"}],
            }
            completed_state = manager.finish_processing_success(session_id, result)

            self.assertEqual(completed_state["status"], "completed")
            self.assertEqual(completed_state["result"], result)
            self.assertFalse((settings.ingestion_upload_dir / session_id / "files").exists())

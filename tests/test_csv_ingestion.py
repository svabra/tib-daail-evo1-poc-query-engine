from __future__ import annotations

import io
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.ingestion_types.csv.manager import (  # noqa: E402
    CsvIngestionManager,
    duckdb_type_to_postgres_type,
    normalize_csv_delimiter,
    normalize_csv_columns,
    normalize_csv_table_name,
)
from bit_data_workbench.backend.ingestion_types.csv.s3_formats import (  # noqa: E402
    normalize_csv_s3_storage_format,
    resolve_csv_s3_file_name,
)
from bit_data_workbench.backend.ingestion_types.csv.validation import (  # noqa: E402
    detect_csv_delimiter,
    validate_csv_file,
)
from bit_data_workbench.config import Settings  # noqa: E402


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
    ) -> None:
        self.uploads.append((filename, bucket, key, ExtraArgs, Path(filename).read_bytes()))


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
        image_version="0.5.5",
        port=8000,
        duckdb_database=Path("/tmp/workspace.duckdb"),
        duckdb_extension_directory=Path("/tmp/duckdb-ext"),
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


class CsvIngestionHelperTests(TestCase):
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
                target_id="workspace.s3",
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
                target_id="workspace.s3",
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
                target_id="workspace.s3",
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
                target_id="workspace.s3",
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
                target_id="workspace.s3",
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
                target_id="workspace.s3",
                bucket="csv-imports",
                delimiter=",",
                has_header=True,
                storage_format="avro",
            )

        ensure_bucket.assert_not_called()
        self.assertEqual(payload["importedCount"], 0)
        self.assertIn(
            "Shared Workspace S3 storage format must be one of: csv, json, parquet.",
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
        self.assertEqual(payload["imports"][0]["rowCount"], 3)

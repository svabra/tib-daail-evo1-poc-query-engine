from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import patch
import xml.etree.ElementTree as ET

from openpyxl import load_workbook


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.query_result_exports import (  # noqa: E402
    QueryResultExportManager,
    build_default_filename,
    normalize_export_filename,
)
from bit_data_workbench.backend.data_exporters.formats import normalize_export_format  # noqa: E402
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.models import QueryJobDefinition  # noqa: E402


def make_settings() -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIFL Workbench",
        image_version="0.5.7",
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


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, str, dict[str, object] | None]] = []

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, object] | None = None,
    ) -> None:
        self.uploads.append((filename, bucket, key, ExtraArgs))


def make_job() -> QueryJobDefinition:
    return QueryJobDefinition(
        job_id="job-1",
        notebook_id="notebook-1",
        notebook_title="Tax Pressure Review",
        cell_id="cell-1",
        sql="select 1 as value",
        status="completed",
        started_at="2026-04-15T00:00:00+00:00",
        updated_at="2026-04-15T00:00:00+00:00",
        completed_at="2026-04-15T00:00:01+00:00",
        duration_ms=1.0,
        columns=["id", "name"],
        rows=[(1, "alpha"), (2, "beta")],
        row_count=2,
        rows_shown=2,
        truncated=False,
    )


def make_manager() -> QueryResultExportManager:
    return QueryResultExportManager(
        settings=make_settings(),
        connection_factory=lambda: None,
        postgres_connection_factory=lambda alias: None,
        query_job_resolver=lambda job_id: make_job(),
    )


class QueryResultExportTests(TestCase):
    def test_normalize_export_format_supports_jsonl_xml_and_xlsx(self) -> None:
        self.assertEqual(normalize_export_format("jsonl"), "jsonl")
        self.assertEqual(normalize_export_format("xml"), "xml")
        self.assertEqual(normalize_export_format("xlsx"), "xlsx")
        self.assertEqual(
            normalize_export_filename("tax-results", "jsonl", fallback="fallback"),
            "tax-results.jsonl",
        )
        self.assertEqual(
            normalize_export_filename("tax-results", "xlsx", fallback="fallback"),
            "tax-results.xlsx",
        )
        self.assertEqual(
            normalize_export_filename("tax-results.csv", "jsonl", fallback="fallback"),
            "tax-results.jsonl",
        )
        self.assertEqual(build_default_filename(make_job(), "xml"), "Tax-Pressure-Review-cell-1.xml")

    def test_download_writes_json_array_and_jsonl(self) -> None:
        manager = make_manager()

        json_artifact = manager.download(job_id="job-1", export_format="json")
        self.assertEqual(json_artifact.export.filename, "Tax-Pressure-Review-cell-1.json")
        self.assertEqual(json_artifact.export.content_type, "application/json")
        self.assertEqual(
            json_artifact.local_path.read_text(encoding="utf-8"),
            '[\n{"id":1,"name":"alpha"},\n{"id":2,"name":"beta"}\n]',
        )

        jsonl_artifact = manager.download(job_id="job-1", export_format="jsonl")
        self.assertEqual(jsonl_artifact.export.filename, "Tax-Pressure-Review-cell-1.jsonl")
        self.assertEqual(jsonl_artifact.export.content_type, "application/x-ndjson; charset=utf-8")
        self.assertEqual(
            jsonl_artifact.local_path.read_text(encoding="utf-8").splitlines(),
            ['{"id":1,"name":"alpha"}', '{"id":2,"name":"beta"}'],
        )

    def test_download_writes_csv_with_custom_delimiter_and_without_header(self) -> None:
        artifact = make_manager().download(
            job_id="job-1",
            export_format="csv",
            export_settings={"delimiter": ";", "includeHeader": False},
        )

        self.assertEqual(artifact.local_path.read_text(encoding="utf-8").splitlines(), ["1;alpha", "2;beta"])

    def test_download_writes_xml_with_custom_root_and_row_names(self) -> None:
        artifact = make_manager().download(
            job_id="job-1",
            export_format="xml",
            export_settings={"rootName": "tax_results", "rowName": "entry"},
        )

        root = ET.fromstring(artifact.local_path.read_text(encoding="utf-8"))
        self.assertEqual(root.tag, "tax_results")
        entries = list(root)
        self.assertEqual([entry.tag for entry in entries], ["entry", "entry"])
        self.assertEqual(entries[0].findtext("id"), "1")
        self.assertEqual(entries[0].findtext("name"), "alpha")

    def test_download_writes_xlsx_with_custom_sheet_name(self) -> None:
        artifact = make_manager().download(
            job_id="job-1",
            export_format="xlsx",
            export_settings={"sheetName": "Tax Results"},
        )

        workbook = load_workbook(filename=BytesIO(artifact.local_path.read_bytes()))
        worksheet = workbook["Tax Results"]
        self.assertEqual(worksheet["A1"].value, "id")
        self.assertEqual(worksheet["B1"].value, "name")
        self.assertEqual(worksheet["A2"].value, 1)
        self.assertEqual(worksheet["B2"].value, "alpha")
        self.assertEqual(worksheet.freeze_panes, "A2")

    def test_save_to_s3_uses_csv_metadata_for_custom_csv_settings(self) -> None:
        fake_client = FakeS3Client()
        manager = make_manager()

        with patch("bit_data_workbench.backend.query_result_exports.ensure_s3_bucket"), patch(
            "bit_data_workbench.backend.query_result_exports.s3_client",
            return_value=fake_client,
        ):
            result = manager.save_to_s3(
                job_id="job-1",
                export_format="csv",
                bucket="exports",
                prefix="results",
                file_name="tax-results",
                export_settings={"delimiter": ";", "includeHeader": False},
            )

        self.assertEqual(result.filename, "tax-results.csv")
        self.assertEqual(result.key, "results/tax-results.csv")
        self.assertEqual(len(fake_client.uploads), 1)
        self.assertEqual(
            fake_client.uploads[0][3],
            {
                "Metadata": {
                    "bdw_csv_has_header": "false",
                    "bdw_csv_delimiter": "semicolon",
                }
            },
        )

    def test_save_to_s3_uses_jsonl_suffix_and_content_type(self) -> None:
        fake_client = FakeS3Client()
        manager = make_manager()

        with patch("bit_data_workbench.backend.query_result_exports.ensure_s3_bucket"), patch(
            "bit_data_workbench.backend.query_result_exports.s3_client",
            return_value=fake_client,
        ):
            result = manager.save_to_s3(
                job_id="job-1",
                export_format="jsonl",
                bucket="exports",
                prefix="results",
                file_name="tax-results",
            )

        self.assertEqual(result.filename, "tax-results.jsonl")
        self.assertEqual(result.content_type, "application/x-ndjson; charset=utf-8")
        self.assertEqual(result.key, "results/tax-results.jsonl")
        self.assertEqual(len(fake_client.uploads), 1)
        self.assertEqual(fake_client.uploads[0][1:3], ("exports", "results/tax-results.jsonl"))

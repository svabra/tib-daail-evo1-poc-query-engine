from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase

from openpyxl import Workbook


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.source_discovery import S3DataSourceDiscoverer  # noqa: E402
from bit_data_workbench.config import Settings  # noqa: E402


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
        s3_bucket="vat-smoke-test",
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


def make_temp_settings(database_path: Path) -> Settings:
    settings = make_settings()
    settings.duckdb_database = database_path
    settings.duckdb_extension_directory = database_path.parent / "duckdb-ext"
    return settings


class FakeS3Client:
    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = str(kwargs.get("Prefix") or "")
        contents = []
        if bucket == "vat-smoke-test" and "incoming/tax-office.csv".startswith(prefix):
            contents.append({"Key": "incoming/tax-office.csv"})
        return {
            "Contents": contents,
            "IsTruncated": False,
        }

    def head_object(self, **kwargs):
        if kwargs["Bucket"] != "vat-smoke-test" or kwargs["Key"] != "incoming/tax-office.csv":
            raise AssertionError("Unexpected head_object request in test.")
        return {
            "ETag": '"abc123"',
            "ContentLength": 128,
            "Metadata": {
                "bdw_csv_has_header": "true",
                "bdw_csv_delimiter": "comma",
            }
        }


class CsvS3DiscoveryTests(TestCase):
    def test_discovered_csv_spec_uses_uploaded_csv_metadata_for_query_sql(self) -> None:
        discoverer = S3DataSourceDiscoverer(make_settings())

        specs = discoverer._build_desired_specs(FakeS3Client(), {"vat-smoke-test"})
        self.assertEqual(len(specs), 1)
        spec = next(iter(specs.values()))

        self.assertEqual(spec.relation_name, "tax_office")
        self.assertEqual(spec.object_revision, "abc123|128")
        self.assertEqual(spec.csv_delimiter, ",")
        self.assertTrue(spec.csv_has_header)
        self.assertIn("HEADER = TRUE", spec.query_sql)
        self.assertIn("DELIM = ','", spec.query_sql)

    def test_discovered_xml_and_xlsx_specs_materialize_to_queryable_csv_views(self) -> None:
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.append(["record_id", "tax_office"])
        worksheet.append([1, "Zurich Central Tax Office"])
        output = BytesIO()
        workbook.save(output)
        workbook.close()

        class QueryableFileClient:
            def list_objects_v2(self, **kwargs):
                bucket = kwargs["Bucket"]
                if bucket != "vat-smoke-test":
                    return {"Contents": [], "IsTruncated": False}
                return {
                    "Contents": [
                        {"Key": "incoming/tax-office.xml"},
                        {"Key": "incoming/tax-office.xlsx"},
                    ],
                    "IsTruncated": False,
                }

            def head_object(self, **kwargs):
                key = kwargs["Key"]
                if key.endswith(".xml"):
                    return {
                        "ETag": '"xml123"',
                        "ContentLength": 144,
                        "Metadata": {},
                    }
                if key.endswith(".xlsx"):
                    return {
                        "ETag": '"xlsx123"',
                        "ContentLength": 256,
                        "Metadata": {},
                    }
                raise AssertionError("Unexpected head_object request.")

            def get_object(self, **kwargs):
                key = kwargs["Key"]
                if key.endswith(".xml"):
                    return {
                        "Body": BytesIO(
                            b"<rows><row><record_id>1</record_id><tax_office>Zurich Central Tax Office</tax_office></row></rows>"
                        )
                    }
                if key.endswith(".xlsx"):
                    return {"Body": BytesIO(output.getvalue())}
                raise AssertionError("Unexpected get_object request.")

        with TemporaryDirectory() as temp_dir:
            discoverer = S3DataSourceDiscoverer(
                make_temp_settings(Path(temp_dir) / "workspace.duckdb")
            )

            specs = discoverer._build_desired_specs(QueryableFileClient(), {"vat-smoke-test"})

            xml_spec = next(spec for spec in specs.values() if spec.display_name == "tax-office.xml")
            self.assertEqual(xml_spec.object_format, "xml")
            self.assertEqual(xml_spec.display_name, "tax-office.xml")
            self.assertEqual(xml_spec.size_bytes, 144)
            self.assertIn("read_csv_auto(", xml_spec.query_sql)
            self.assertTrue((Path(temp_dir) / "s3-query-sources").exists())

            xlsx_spec = next(spec for spec in specs.values() if spec.display_name == "tax-office.xlsx")
            self.assertEqual(xlsx_spec.object_format, "xlsx")
            self.assertEqual(xlsx_spec.display_name, "tax-office.xlsx")
            self.assertEqual(xlsx_spec.size_bytes, 256)
            self.assertIn("read_csv_auto(", xlsx_spec.query_sql)

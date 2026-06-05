from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase

import duckdb
from openpyxl import Workbook


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402
from bit_data_workbench.backend.source_discovery import S3DataSourceDiscoverer  # noqa: E402
from bit_data_workbench.backend.source_discovery import build_s3_query  # noqa: E402
from bit_data_workbench.backend.source_discovery import drop_discovered_relation_object  # noqa: E402
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


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


class DataExchangeHiddenS3Client:
    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = str(kwargs.get("Prefix") or "")
        contents = []
        if bucket == "vat-smoke-test":
            for key in (
                "--data-exchange--/files/secret/hidden.csv",
                "incoming/visible.csv",
            ):
                if key.startswith(prefix):
                    contents.append({"Key": key})
        return {
            "Contents": contents,
            "IsTruncated": False,
        }

    def head_object(self, **kwargs):
        if kwargs["Key"] == "incoming/visible.csv":
            return {
                "ETag": '"visible123"',
                "ContentLength": 32,
                "Metadata": {},
            }
        raise AssertionError("Hidden DataExchange object should not be inspected.")


class GeneratedMwaLoaderS3Client:
    keys = (
        "generated/mwa_abrechnung_test/csv/mwa_abrechnung_entities/part-00001.csv",
        "generated/mwa_abrechnung_test/csv/mwa_abrechnung_entities/part-00002.csv",
        "generated/mwa_abrechnung_test/json/mwa_abrechnung_entities/part-00001.jsonl",
        "generated/mwa_abrechnung_test/json/mwa_abrechnung_entities/part-00002.jsonl",
        "generated/mwa_abrechnung_test/parquet/mwa_abrechnung_entities/part-00001.parquet",
        "generated/mwa_abrechnung_test/parquet/mwa_abrechnung_entities/part-00002.parquet",
    )
    sizes = {
        "generated/mwa_abrechnung_test/csv/mwa_abrechnung_entities/part-00001.csv": 128,
        "generated/mwa_abrechnung_test/csv/mwa_abrechnung_entities/part-00002.csv": 96,
        "generated/mwa_abrechnung_test/json/mwa_abrechnung_entities/part-00001.jsonl": 256,
        "generated/mwa_abrechnung_test/json/mwa_abrechnung_entities/part-00002.jsonl": 192,
        "generated/mwa_abrechnung_test/parquet/mwa_abrechnung_entities/part-00001.parquet": 512,
        "generated/mwa_abrechnung_test/parquet/mwa_abrechnung_entities/part-00002.parquet": 384,
    }

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = str(kwargs.get("Prefix") or "")
        if bucket != "vat-smoke-test":
            return {"Contents": [], "IsTruncated": False}
        return {
            "Contents": [
                {"Key": key}
                for key in self.keys
                if key.startswith(prefix)
            ],
            "IsTruncated": False,
        }

    def head_object(self, **kwargs):
        if kwargs["Bucket"] != "vat-smoke-test":
            raise AssertionError("Unexpected generated loader bucket.")
        key = kwargs["Key"]
        if key not in self.sizes:
            raise AssertionError(f"Unexpected generated loader head_object request: {key}")
        metadata = {}
        if key.endswith(".csv"):
            metadata = {
                "bdw_csv_has_header": "true",
                "bdw_csv_delimiter": "comma",
            }
        return {
            "ETag": f'"{Path(key).name}-etag"',
            "ContentLength": self.sizes[key],
            "Metadata": metadata,
        }


class PartitionedParquetS3Client:
    keys = (
        "incoming/april/vat_smoke/tax_year=2025/data_0.parquet",
        "incoming/april/vat_smoke/tax_year=2026/data_0.parquet",
    )

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = str(kwargs.get("Prefix") or "")
        if bucket != "vat-smoke-test":
            return {"Contents": [], "IsTruncated": False}
        return {
            "Contents": [
                {"Key": key}
                for key in self.keys
                if key.startswith(prefix)
            ],
            "IsTruncated": False,
        }

    def head_object(self, **kwargs):
        key = kwargs["Key"]
        if key not in self.keys:
            raise AssertionError(f"Unexpected partitioned parquet head_object request: {key}")
        return {
            "ETag": f'"{Path(key).parent.name}-etag"',
            "ContentLength": 256,
            "Metadata": {},
        }


class CsvS3DiscoveryTests(TestCase):
    def test_drop_discovered_relation_object_handles_stale_table(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute("CREATE SCHEMA stale")
            connection.execute("CREATE TABLE stale.cached_relation AS SELECT 1 AS value")

            drop_discovered_relation_object(
                connection,
                schema_name="stale",
                relation_name="cached_relation",
            )

            exists = connection.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'stale'
                  AND table_name = 'cached_relation'
                """
            ).fetchone()[0]
            self.assertEqual(exists, 0)
        finally:
            connection.close()

    def test_s3_existing_specs_ignore_loader_cache_tables(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute("CREATE SCHEMA generated")
            connection.execute("CREATE VIEW generated.s3_relation AS SELECT 1 AS value")
            connection.execute("CREATE TABLE generated.s3_relation_duckdb_cache AS SELECT 1 AS value")

            discoverer = S3DataSourceDiscoverer(make_settings())
            specs = discoverer._load_existing_specs(connection)

            self.assertIn("generated.s3_relation", specs)
            self.assertNotIn("generated.s3_relation_duckdb_cache", specs)
        finally:
            connection.close()

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

    def test_s3_discovery_ignores_data_exchange_prefix_objects(self) -> None:
        discoverer = S3DataSourceDiscoverer(make_settings())

        specs = discoverer._build_desired_specs(DataExchangeHiddenS3Client(), {"vat-smoke-test"})

        self.assertEqual(len(specs), 1)
        spec = next(iter(specs.values()))
        self.assertEqual(spec.display_name, "visible.csv")
        self.assertNotIn("data-exchange", spec.object_path)

    def test_partitioned_parquet_upload_is_discovered_as_one_relation(self) -> None:
        discoverer = S3DataSourceDiscoverer(make_settings())

        specs = discoverer._build_desired_specs(PartitionedParquetS3Client(), {"vat-smoke-test"})

        self.assertEqual(len(specs), 1)
        spec = next(iter(specs.values()))
        self.assertEqual(spec.relation_name, "vat_smoke")
        self.assertEqual(spec.object_path, "s3://vat-smoke-test/incoming/april/vat_smoke/**/*.parquet")
        self.assertIn("read_parquet(", spec.query_sql)
        self.assertIn("hive_partitioning=true", spec.query_sql)
        self.assertEqual(spec.display_name, "vat_smoke.parquet")
        self.assertEqual(spec.download_kind, "partitioned_parts")
        self.assertEqual(spec.part_prefix, "incoming/april/vat_smoke/")
        self.assertEqual(spec.part_count, 2)
        self.assertTrue(spec.zip_downloadable)
        service = object.__new__(WorkbenchService)
        service._data_source_discovery = SimpleNamespace(
            s3_relation_specs=lambda: specs,
        )
        metadata = WorkbenchService._workspace_s3_object_metadata(service)
        item = next(iter(metadata.values()))
        self.assertEqual(
            item["query_alias"],
            "s3.vat_smoke_test.incoming.april.vat_smoke.parquet",
        )
        self.assertNotIn("vat_smoke.vat_smoke", item["query_alias"])

    def test_build_s3_parquet_query_accepts_runtime_hive_option(self) -> None:
        self.assertEqual(
            build_s3_query(
                "parquet",
                "s3://vat-smoke-test/incoming/april/vat_smoke/**/*.parquet",
                hive_partitioning=True,
            ),
            "SELECT * FROM read_parquet('s3://vat-smoke-test/incoming/april/vat_smoke/**/*.parquet', hive_partitioning=true)",
        )
        self.assertIn(
            "hive_partitioning=false",
            build_s3_query(
                "parquet",
                "s3://vat-smoke-test/incoming/april/vat_smoke/**/*.parquet",
                hive_partitioning=False,
            ),
        )

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

    def test_generated_loader_multiformat_outputs_use_file_display_names(self) -> None:
        discoverer = S3DataSourceDiscoverer(make_settings())

        specs = discoverer._build_desired_specs(
            GeneratedMwaLoaderS3Client(),
            {"vat-smoke-test"},
        )
        specs_by_relation = {spec.relation_name: spec for spec in specs.values()}

        self.assertNotIn("mwa_abrechnung_test", specs_by_relation)

        csv_spec = specs_by_relation["mwa_abrechnung_entities_csv"]
        self.assertEqual(csv_spec.display_name, "mwa_abrechnung_entities.csv")
        self.assertEqual(csv_spec.object_format, "csv")
        self.assertTrue(csv_spec.object_path.endswith("/csv/mwa_abrechnung_entities/*.csv"))
        self.assertEqual(csv_spec.size_bytes, 224)
        self.assertTrue(csv_spec.csv_has_header)
        self.assertEqual(csv_spec.csv_delimiter, ",")
        self.assertEqual(csv_spec.download_kind, "generated_parts")
        self.assertEqual(
            csv_spec.part_prefix,
            "generated/mwa_abrechnung_test/csv/mwa_abrechnung_entities/",
        )
        self.assertEqual(csv_spec.part_file_format, "csv")
        self.assertEqual(csv_spec.part_count, 2)
        self.assertEqual(csv_spec.download_filename, "mwa_abrechnung_entities.csv")
        self.assertTrue(csv_spec.merge_downloadable)
        self.assertTrue(csv_spec.zip_downloadable)
        self.assertIn("read_csv_auto(", csv_spec.query_sql)
        self.assertIn("HEADER = TRUE", csv_spec.query_sql)

        json_spec = specs_by_relation["mwa_abrechnung_entities_json"]
        self.assertEqual(json_spec.display_name, "mwa_abrechnung_entities.jsonl")
        self.assertEqual(json_spec.object_format, "jsonl")
        self.assertTrue(json_spec.object_path.endswith("/json/mwa_abrechnung_entities/*.jsonl"))
        self.assertEqual(json_spec.size_bytes, 448)
        self.assertEqual(json_spec.download_kind, "generated_parts")
        self.assertEqual(
            json_spec.part_prefix,
            "generated/mwa_abrechnung_test/json/mwa_abrechnung_entities/",
        )
        self.assertEqual(json_spec.part_file_format, "jsonl")
        self.assertEqual(json_spec.part_count, 2)
        self.assertEqual(json_spec.download_filename, "mwa_abrechnung_entities.jsonl")
        self.assertTrue(json_spec.merge_downloadable)
        self.assertTrue(json_spec.zip_downloadable)
        self.assertIn("read_json_auto(", json_spec.query_sql)

        parquet_spec = specs_by_relation["mwa_abrechnung_entities_parquet"]
        self.assertEqual(parquet_spec.display_name, "mwa_abrechnung_entities.parquet")
        self.assertEqual(parquet_spec.object_format, "parquet")
        self.assertTrue(parquet_spec.object_path.endswith("/parquet/mwa_abrechnung_entities/*.parquet"))
        self.assertEqual(parquet_spec.size_bytes, 896)
        self.assertEqual(parquet_spec.download_kind, "generated_parts")
        self.assertEqual(
            parquet_spec.part_prefix,
            "generated/mwa_abrechnung_test/parquet/mwa_abrechnung_entities/",
        )
        self.assertEqual(parquet_spec.part_file_format, "parquet")
        self.assertEqual(parquet_spec.part_count, 2)
        self.assertEqual(parquet_spec.download_filename, "mwa_abrechnung_entities.parquet")
        self.assertFalse(parquet_spec.merge_downloadable)
        self.assertTrue(parquet_spec.zip_downloadable)

    def test_generated_parts_expose_download_metadata_in_workspace_metadata(self) -> None:
        discoverer = S3DataSourceDiscoverer(make_settings())
        specs = discoverer._build_desired_specs(
            GeneratedMwaLoaderS3Client(),
            {"vat-smoke-test"},
        )
        service = object.__new__(WorkbenchService)
        service._data_source_discovery = SimpleNamespace(
            s3_relation_specs=lambda: specs,
        )

        metadata = WorkbenchService._workspace_s3_object_metadata(service)
        by_display_name = {
            value["display_name"]: value
            for value in metadata.values()
        }

        csv_metadata = by_display_name["mwa_abrechnung_entities.csv"]
        self.assertFalse(csv_metadata["downloadable"])
        self.assertEqual(csv_metadata["download_kind"], "generated_parts")
        self.assertEqual(csv_metadata["part_count"], 2)
        self.assertEqual(csv_metadata["size_bytes"], 224)
        self.assertEqual(csv_metadata["download_filename"], "mwa_abrechnung_entities.csv")
        self.assertTrue(csv_metadata["merge_downloadable"])
        self.assertTrue(csv_metadata["zip_downloadable"])
        self.assertEqual(
            csv_metadata["query_alias"],
            "s3.vat_smoke_test.generated.mwa_abrechnung_test.csv.mwa_abrechnung_entities.csv",
        )
        self.assertNotIn("mwa_abrechnung_entities.mwa_abrechnung_entities", csv_metadata["query_alias"])

        json_metadata = by_display_name["mwa_abrechnung_entities.jsonl"]
        self.assertFalse(json_metadata["downloadable"])
        self.assertEqual(json_metadata["download_kind"], "generated_parts")
        self.assertEqual(json_metadata["part_count"], 2)
        self.assertEqual(json_metadata["size_bytes"], 448)
        self.assertEqual(json_metadata["download_filename"], "mwa_abrechnung_entities.jsonl")
        self.assertTrue(json_metadata["merge_downloadable"])
        self.assertTrue(json_metadata["zip_downloadable"])
        self.assertEqual(
            json_metadata["query_alias"],
            "s3.vat_smoke_test.generated.mwa_abrechnung_test.json.mwa_abrechnung_entities.jsonl",
        )
        self.assertNotIn("mwa_abrechnung_entities.mwa_abrechnung_entities", json_metadata["query_alias"])

        parquet_metadata = by_display_name["mwa_abrechnung_entities.parquet"]
        self.assertFalse(parquet_metadata["downloadable"])
        self.assertEqual(parquet_metadata["download_kind"], "generated_parts")
        self.assertEqual(parquet_metadata["part_count"], 2)
        self.assertEqual(parquet_metadata["size_bytes"], 896)
        self.assertEqual(parquet_metadata["download_filename"], "mwa_abrechnung_entities.parquet")
        self.assertFalse(parquet_metadata["merge_downloadable"])
        self.assertTrue(parquet_metadata["zip_downloadable"])
        self.assertEqual(
            parquet_metadata["query_alias"],
            "s3.vat_smoke_test.generated.mwa_abrechnung_test.parquet.mwa_abrechnung_entities.parquet",
        )
        self.assertNotIn("mwa_abrechnung_entities.mwa_abrechnung_entities", parquet_metadata["query_alias"])

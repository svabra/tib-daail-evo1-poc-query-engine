from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import (  # noqa: E402
    QueryExplainPayload,
    QuerySqlPreparePayload,
    explain_query as explain_query_route,
    prepare_query_sql as prepare_query_sql_route,
)
from bit_data_workbench.backend.query_explain import summarize_explain_plans  # noqa: E402
from bit_data_workbench.backend.query_jobs import DuckDBQueryAccessCoordinator  # noqa: E402
from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema  # noqa: E402


def make_settings(root: Path) -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIF Factory",
        image_version="test",
        port=8000,
        duckdb_database=root / "workspace.duckdb",
        duckdb_extension_directory=root / "duckdb-ext",
        service_consumption_data_dir=root / "service-consumption",
        service_consumption_cpu_memory_interval_seconds=3,
        service_consumption_s3_interval_seconds=3600,
        service_consumption_retention_hours=48,
        max_result_rows=50,
        s3_endpoint=None,
        s3_bucket=None,
        s3_access_key_id=None,
        s3_access_key_id_file=None,
        s3_secret_access_key=None,
        s3_secret_access_key_file=None,
        s3_url_style=None,
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


def sample_catalogs() -> list[SourceCatalog]:
    return [
        SourceCatalog(
            name="workspace",
            connection_source_id="s3",
            schemas=[
                SourceSchema(
                    name="test",
                    objects=[
                        SourceObject(
                            name="federal_tax_data_10gb",
                            kind="view",
                            relation="test.federal_tax_data_10gb",
                            query_alias="s3.test.federal_tax_data_10gb.csv",
                            s3_bucket="test-bucket",
                        )
                    ],
                )
            ],
        )
    ]


def make_service(settings: Settings) -> WorkbenchService:
    service = WorkbenchService.__new__(WorkbenchService)
    service.settings = settings
    service._lock = threading.RLock()
    service._catalogs = sample_catalogs()
    service._duckdb_query_access = DuckDBQueryAccessCoordinator()
    return service


class QueryExplainServiceTests(unittest.TestCase):
    def test_valid_duckdb_sql_returns_three_plans_and_summary(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            service = make_service(make_settings(Path(tmp)))

            payload = service.explain_query(
                sql="select * from range(3) where range > 0",
                notebook_id="notebook",
                notebook_title="Notebook",
                cell_id="cell-1",
                data_sources=[],
            )

        self.assertEqual(payload["status"], "completed")
        self.assertEqual(set(payload["plans"].keys()), {"logical_plan", "logical_opt", "physical_plan"})
        self.assertIn("FILTER", payload["plans"]["physical_plan"]["text"])
        self.assertTrue(payload["summary"]["operatorCounts"])
        self.assertTrue(payload["duckdbVersion"])

    def test_isolated_explain_uses_writable_memory_connection_when_database_exists(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            settings = make_settings(Path(tmp))
            settings.duckdb_database.parent.mkdir(parents=True, exist_ok=True)
            settings.duckdb_database.touch()
            service = make_service(settings)

            payload = service.explain_query(sql="select 1 as smoke_value", data_sources=[])

        self.assertEqual(payload["status"], "completed")
        self.assertIn("PROJECTION", payload["plans"]["physical_plan"]["text"])

    def test_missing_source_is_rejected_before_explain(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            service = make_service(make_settings(Path(tmp)))
            with self.assertRaises(ValueError) as exc:
                service.explain_query(
                    sql="select * from missing.schema_table",
                    data_sources=["s3"],
                    query_options={"validation": {"sourceExistence": "on"}},
                )

        self.assertIn("Referenced source(s) were not found", str(exc.exception))

    def test_syntax_errors_are_returned_as_readable_explain_errors(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            service = make_service(make_settings(Path(tmp)))
            with self.assertRaises(ValueError) as exc:
                service.explain_query(sql="select from")

        self.assertIn("Query plan could not be generated", str(exc.exception))

    def test_shared_duckdb_local_alias_is_rejected_before_explain(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            settings = make_settings(Path(tmp))
            settings.duckdb_database.parent.mkdir(parents=True, exist_ok=True)
            connection = duckdb.connect(str(settings.duckdb_database))
            try:
                connection.execute('create schema "client_browser_abc"')
                connection.execute('create table "client_browser_abc"."entry_1" as select 1 as value')
            finally:
                connection.close()

            service = make_service(settings)
            with self.assertRaises(ValueError) as exc:
                service.explain_query(
                    sql="select * from local.test.sample.csv",
                    display_sql="select * from local.test.sample.csv",
                    data_sources=["workspace.local"],
                    local_relation_map={
                        "local.test.sample.csv": "client_browser_abc.entry_1",
                    },
                )

        self.assertIn("Read queries no longer wait for shared DuckDB file access", str(exc.exception))
        self.assertIn("client_browser_abc.entry_1", str(exc.exception))

    def test_display_sql_does_not_override_runnable_sql_for_explain(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            service = make_service(make_settings(Path(tmp)))
            payload = service.explain_query(
                sql="select * from range(3)",
                display_sql="select * from stage.mwa_joined_abrechnungen",
                notebook_id="notebook",
                notebook_title="Notebook",
                cell_id="cell-1",
                data_sources=[],
            )

        self.assertEqual(payload["status"], "completed")

    def test_native_postgres_sources_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            service = make_service(make_settings(Path(tmp)))
            with self.assertRaises(ValueError) as exc:
                service.explain_query(sql="select 1", data_sources=["pg_oltp_native"])

        self.assertIn("DuckDB-backed SQL cells", str(exc.exception))


class QueryExplainSummaryTests(unittest.TestCase):
    def test_summary_extracts_plan_warnings_and_storage_hints(self) -> None:
        physical_plan = [
            {
                "name": "HASH_JOIN",
                "children": [
                    {
                        "name": "SEQ_SCAN",
                        "children": [],
                        "extra_info": {"Estimated Cardinality": "2500000"},
                    },
                    {"name": "CSV_SCAN", "children": [], "extra_info": {}},
                ],
                "extra_info": {"Estimated Cardinality": "1200000"},
            }
        ]

        summary = summarize_explain_plans(
            json_plans={
                "logical_plan": physical_plan,
                "logical_opt": physical_plan,
                "physical_plan": physical_plan,
            },
            text_plans={"physical_plan": "CSV_SCAN"},
            data_sources=["s3"],
            touched_relations=["s3.test.large.csv"],
            touched_buckets=["test"],
        )

        self.assertGreaterEqual(summary["operatorCategories"]["join"], 1)
        self.assertTrue(any("estimated operator cardinality" in warning for warning in summary["warnings"]))
        self.assertTrue(any("CSV" in warning for warning in summary["warnings"]))
        self.assertTrue(any("S3-backed" in hint for hint in summary["hints"]))


class QueryExplainApiTests(unittest.TestCase):
    def test_route_delegates_to_service(self) -> None:
        captured: dict[str, object] = {}

        class FakeWorkbenchService:
            def explain_query(self, **kwargs):
                captured.update(kwargs)
                return {"status": "completed", "plans": {}, "summary": {}}

        response = explain_query_route(
            payload=QueryExplainPayload.model_validate(
                {
                    "sql": "select 1",
                    "displaySql": "select 1",
                    "notebookId": "notebook",
                    "notebookTitle": "Notebook",
                    "cellId": "cell-1",
                    "dataSources": ["s3"],
                    "localRelations": {"alias": "physical"},
                    "queryOptions": {
                        "duckdb": {"parquetHivePartitioning": "on"},
                    },
                }
            ),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(json.loads(response.body.decode("utf-8"))["status"], "completed")
        self.assertEqual(captured["notebook_id"], "notebook")
        self.assertEqual(captured["local_relation_map"], {"alias": "physical"})
        self.assertEqual(
            captured["query_options"]["duckdb"]["parquetHivePartitioning"],
            "on",
        )

    def test_prepare_route_delegates_to_service(self) -> None:
        captured: dict[str, object] = {}

        class FakeWorkbenchService:
            def prepare_query_sql(self, **kwargs):
                captured.update(kwargs)
                return {
                    "displaySql": "select * from stage.mwa_joined_abrechnungen",
                    "executionSql": "select * from read_parquet('s3://bucket/stage.parquet')",
                    "duckdbExecutionPath": "isolated-read",
                }

        response = prepare_query_sql_route(
            payload=QuerySqlPreparePayload.model_validate(
                {
                    "sql": "select * from stage.mwa_joined_abrechnungen",
                    "displaySql": "select * from stage.mwa_joined_abrechnungen",
                    "notebookId": "notebook",
                    "notebookTitle": "Notebook",
                    "cellId": "cell-4",
                    "dataSources": ["s3"],
                    "localRelations": {"local.alias": "physical.alias"},
                    "queryOptions": {
                        "duckdb": {"parquetHivePartitioning": "auto"},
                    },
                }
            ),
            service=FakeWorkbenchService(),
        )

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["duckdbExecutionPath"], "isolated-read")
        self.assertEqual(captured["notebook_id"], "notebook")
        self.assertEqual(captured["cell_id"], "cell-4")
        self.assertEqual(captured["local_relation_map"], {"local.alias": "physical.alias"})


if __name__ == "__main__":
    unittest.main()

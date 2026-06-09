from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import (  # noqa: E402
    QuerySourceValidationPayload,
    validate_query_sources as validate_query_sources_route,
)
from bit_data_workbench.backend.query_analysis import (  # noqa: E402
    KnownRelationReference,
    build_relation_index,
    normalize_relation_key,
)
from bit_data_workbench.backend.query_source_validation import (  # noqa: E402
    QUERY_SOURCE_INVALID,
    QUERY_SOURCE_UNCHECKED,
    QUERY_SOURCE_VALID,
    extract_select_source_references,
    validate_query_sources,
)
from bit_data_workbench.backend.source_discovery import (  # noqa: E402
    DiscoveredRelationSpec,
    build_s3_query,
)
from bit_data_workbench.backend.source_references import (  # noqa: E402
    pg_source_reference,
    s3_source_reference,
)
from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402
from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema  # noqa: E402


def sample_catalogs() -> list[SourceCatalog]:
    return [
        SourceCatalog(
            name="pg_oltp",
            connection_source_id="pg_oltp",
            schemas=[
                SourceSchema(
                    name="public",
                    objects=[
                        SourceObject(
                            name="sales_orders",
                            kind="table",
                            relation="pg_oltp.public.sales_orders",
                        ),
                        SourceObject(
                            name="customers",
                            kind="table",
                            relation="pg_oltp.public.customers",
                        ),
                    ],
                )
            ],
        ),
        SourceCatalog(
            name="workspace",
            connection_source_id="workspace.s3",
            schemas=[
                SourceSchema(
                    name="test",
                    label="test",
                    objects=[
                        SourceObject(
                            name="federal_tax_data_10gb",
                            kind="view",
                            relation="test.federal_tax_data_10gb",
                            query_alias="s3.test.federal_tax_data_10gb.csv",
                            query_reference=s3_source_reference(
                                bucket="test",
                                key="federal_tax_data_10gb.csv",
                            ),
                            query_sql="read_csv_auto('s3://test/federal_tax_data_10gb.csv')",
                            s3_bucket="test",
                            s3_key="federal_tax_data_10gb.csv",
                            s3_file_format="csv",
                        ),
                        SourceObject(
                            name="vat_smoke_part_00001",
                            kind="view",
                            relation="test.vat_smoke_part_00001",
                            query_alias=(
                                "s3.test.generated.vat_smoke.part_00001."
                                "parquet"
                            ),
                            query_reference=s3_source_reference(
                                bucket="test",
                                key="generated/vat_smoke/part_00001.parquet",
                            ),
                            query_sql=(
                                "read_parquet('s3://test/generated/vat_smoke/"
                                "part_00001.parquet')"
                            ),
                            s3_bucket="test",
                            s3_key="generated/vat_smoke/part_00001.parquet",
                            s3_file_format="parquet",
                        )
                    ],
                )
            ],
        ),
    ]


def completed_stage_record(
    *,
    notebook_id: str = "notebook",
    stage_alias: str = "mwa_joined_abrechnungen",
    output_bucket: str = "vat-smoke-test",
    output_key: str = "_bdw_stages/notebook/stage-mwa-joined/rev-1/data.parquet",
) -> dict[str, object]:
    return {
        "runId": "run-stage-1",
        "notebookId": notebook_id,
        "stageId": "stage-mwa-joined",
        "cellId": "cell-stage",
        "stageAlias": stage_alias,
        "stageTitle": "MWA joined Abrechnungen",
        "status": "completed",
        "revisionId": "rev-1",
        "outputBucket": output_bucket,
        "outputKey": output_key,
        "outputPath": f"s3://{output_bucket}/{output_key}",
        "queryReference": f's3.{output_bucket}."{output_key}"',
        "querySql": f"read_parquet('s3://{output_bucket}/{output_key}')",
        "completedAt": "2026-06-09T10:00:00Z",
        "updatedAt": "2026-06-09T10:00:00Z",
    }


def validate(sql: str):
    return validate_query_sources(
        sql,
        relation_index=build_relation_index(sample_catalogs()),
    )


class QuerySourceValidationTests(unittest.TestCase):
    def test_existing_relation_validates(self) -> None:
        result = validate("select * from pg_oltp.public.sales_orders")

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.missing_references, [])
        self.assertEqual(
            result.matched_references[0].matched_relation,
            "pg_oltp.public.sales_orders",
        )

    def test_missing_relation_invalidates(self) -> None:
        result = validate("select * from missing.schema_table")

        self.assertEqual(result.status, QUERY_SOURCE_INVALID)
        self.assertEqual(result.missing_references, ["missing.schema_table"])

    def test_cte_names_are_skipped(self) -> None:
        result = validate(
            """
            with sales_orders as (
              select * from pg_oltp.public.customers
            )
            select * from sales_orders
            """
        )

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.references, ["pg_oltp.public.customers"])

        unchecked = validate("with staged as (select 1) select * from staged")
        self.assertEqual(unchecked.status, QUERY_SOURCE_UNCHECKED)

    def test_quoted_schema_qualified_identifiers_match(self) -> None:
        result = validate('select * from "pg_oltp"."public"."sales_orders"')

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.missing_references, [])

    def test_pg_source_reference_validates(self) -> None:
        result = validate(
            f"select * from {pg_source_reference(source_id='pg_oltp', relation='pg_oltp.public.sales_orders')}"
        )

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.missing_references, [])
        self.assertEqual(
            result.matched_references[0].matched_relation,
            "pg_oltp.public.sales_orders",
        )

    def test_s3_query_alias_validates(self) -> None:
        result = validate("select * from s3.test.federal_tax_data_10gb.csv")

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.missing_references, [])
        self.assertEqual(
            result.matched_references[0].matched_relation,
            "test.federal_tax_data_10gb",
        )

    def test_s3_source_reference_validates_with_quoted_object_key(self) -> None:
        result = validate(
            "select * from "
            + s3_source_reference(
                bucket="test",
                key="generated/vat_smoke/part_00001.parquet",
            )
        )

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.missing_references, [])
        self.assertEqual(
            result.matched_references[0].matched_relation,
            "test.vat_smoke_part_00001",
        )

    def test_s3_parquet_query_alias_validates(self) -> None:
        result = validate(
            "select * from s3.test.generated.vat_smoke.part_00001.parquet"
        )

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.missing_references, [])
        self.assertEqual(
            result.matched_references[0].matched_relation,
            "test.vat_smoke_part_00001",
        )

    def test_subqueries_aliases_and_table_functions_do_not_false_positive(self) -> None:
        references = extract_select_source_references(
            """
            select *
            from (select 1 as value) as local_alias
            join pg_oltp.public.sales_orders as orders on true
            """
        )

        self.assertEqual(references, ["pg_oltp.public.sales_orders"])
        self.assertEqual(
            validate("select * from read_parquet('s3://bucket/file.parquet')").status,
            QUERY_SOURCE_UNCHECKED,
        )
        self.assertEqual(
            validate("select * from table(pg_oltp.public.customers)").status,
            QUERY_SOURCE_VALID,
        )

    def test_local_workspace_logical_and_synced_relations_are_unchecked(self) -> None:
        for sql in (
            "select * from workspace.local.saved_results.local_entry_123",
            "select * from workspace_local_browser_abc.entry_local_entry_123",
        ):
            self.assertEqual(validate(sql).status, QUERY_SOURCE_UNCHECKED)

    def test_start_query_job_blocks_invalid_references_before_job_creation(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        fake_query_jobs = SimpleNamespace(called=False)

        def fail_if_called(**_kwargs):
            fake_query_jobs.called = True
            return SimpleNamespace(payload={"jobId": "query-1"})

        fake_query_jobs.start_job = fail_if_called
        service._query_jobs = fake_query_jobs

        with self.assertRaises(ValueError):
            service.start_query_job(
                sql="select * from missing.schema_table",
                notebook_id="notebook",
                notebook_title="Notebook",
                cell_id="cell-1",
                data_sources=[],
            )

        self.assertFalse(fake_query_jobs.called)

    def test_start_query_job_allows_synced_local_workspace_relation_map(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=[],
            touched_buckets=[],
        )
        fake_query_jobs = SimpleNamespace(called=False)

        def record_start(**_kwargs):
            fake_query_jobs.called = True
            return SimpleNamespace(payload={"jobId": "query-local"})

        fake_query_jobs.start_job = record_start
        service._query_jobs = fake_query_jobs

        snapshot = service.start_query_job(
            sql="select * from client_browser_abc.local_workspace_csv_entry",
            notebook_id="notebook",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=["workspace.local"],
            local_relation_map={
                "workspace.local.saved_results.local-entry": "client_browser_abc.local_workspace_csv_entry"
            },
        )

        self.assertEqual(snapshot["jobId"], "query-local")
        self.assertTrue(fake_query_jobs.called)

    def test_start_query_job_rewrites_s3_alias_but_keeps_display_sql(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=["test.federal_tax_data_10gb"],
            touched_buckets=["test"],
        )
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(payload={"jobId": "query-s3-alias"})

        service._query_jobs = SimpleNamespace(start_job=record_start)

        snapshot = service.start_query_job(
            sql="select * from s3.test.federal_tax_data_10gb.csv",
            notebook_id="notebook",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-s3-alias")
        self.assertEqual(captured["sql"], "select * from s3.test.federal_tax_data_10gb.csv")
        self.assertEqual(
            captured["execution_sql"],
            "select * from read_csv_auto('s3://test/federal_tax_data_10gb.csv')",
        )
        self.assertEqual(
            captured["source_summaries"],
            [
                {
                    "relation": "test.federal_tax_data_10gb",
                    "query_alias": "s3.test.federal_tax_data_10gb.csv",
                    "query_reference": 's3.test."federal_tax_data_10gb.csv"',
                    "bucket": "test",
                    "key": "federal_tax_data_10gb.csv",
                    "path": "",
                    "format": "csv",
                    "query_sql": "read_csv_auto('s3://test/federal_tax_data_10gb.csv')",
                    "size_bytes": 0,
                    "object_revision": "",
                    "display_name": "federal_tax_data_10gb",
                }
            ],
        )

    def test_start_query_job_uses_submitted_sql_for_validation_and_routing(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(payload={"jobId": "query-display-sql-routing"})

        service._query_jobs = SimpleNamespace(start_job=record_start)

        snapshot = service.start_query_job(
            sql="select * from s3.test.generated.vat_smoke.part_00001.parquet",
            display_sql="select * from stage.mwa_joined_abrechnungen",
            notebook_id="notebook",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-display-sql-routing")
        self.assertEqual(captured["sql"], "select * from stage.mwa_joined_abrechnungen")
        self.assertEqual(
            captured["execution_sql"],
            "select * from read_parquet('s3://test/generated/vat_smoke/part_00001.parquet')",
        )
        self.assertEqual(captured["touched_relations"], ["test.vat_smoke_part_00001"])
        self.assertEqual(captured["touched_buckets"], ["test"])
        self.assertEqual(len(captured["source_summaries"]), 1)
        self.assertEqual(captured["source_summaries"][0]["relation"], "test.vat_smoke_part_00001")
        self.assertEqual(captured["source_summaries"][0]["bucket"], "test")

    def test_validate_query_sources_allows_completed_stage_output_for_notebook(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._materialized_stage_store = SimpleNamespace(
            read_state=lambda: {"version": 1, "records": [completed_stage_record()]}
        )

        result = service.validate_query_sources(
            sql="select * from stage.mwa_joined_abrechnungen",
            notebook_id="notebook",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(result["status"], QUERY_SOURCE_VALID)
        self.assertEqual(result["matchedReferences"][0]["matchedRelation"], "stage.mwa_joined_abrechnungen")

    def test_validate_query_sources_keeps_missing_stage_output_invalid(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._materialized_stage_store = SimpleNamespace(
            read_state=lambda: {"version": 1, "records": []}
        )

        result = service.validate_query_sources(
            sql="select * from stage.mwa_joined_abrechnungen",
            notebook_id="notebook",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(result["status"], QUERY_SOURCE_INVALID)
        self.assertEqual(result["missingReferences"], ["stage.mwa_joined_abrechnungen"])

    def test_prepare_query_sql_rewrites_completed_stage_to_s3_parquet_isolated_read(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._materialized_stage_store = SimpleNamespace(
            read_state=lambda: {"version": 1, "records": [completed_stage_record()]}
        )

        payload = service.prepare_query_sql(
            sql="select * from stage.mwa_joined_abrechnungen",
            display_sql="select * from stage.mwa_joined_abrechnungen",
            notebook_id="notebook",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(
            payload["executionSql"],
            "select * from read_parquet('s3://vat-smoke-test/_bdw_stages/notebook/stage-mwa-joined/rev-1/data.parquet')",
        )
        self.assertEqual(payload["touchedRelations"], ["stage.mwa_joined_abrechnungen"])
        self.assertEqual(payload["touchedBuckets"], ["vat-smoke-test"])
        self.assertEqual(payload["duckdbExecutionPath"], "isolated-read")

    def test_start_query_job_rewrites_completed_stage_but_keeps_virtual_display_sql(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._materialized_stage_store = SimpleNamespace(
            read_state=lambda: {"version": 1, "records": [completed_stage_record()]}
        )
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(payload={"jobId": "query-stage-read"})

        service._query_jobs = SimpleNamespace(start_job=record_start)

        snapshot = service.start_query_job(
            sql="select * from stage.mwa_joined_abrechnungen",
            notebook_id="notebook",
            notebook_title="Notebook",
            cell_id="cell-4",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-stage-read")
        self.assertEqual(captured["sql"], "select * from stage.mwa_joined_abrechnungen")
        self.assertEqual(
            captured["execution_sql"],
            "select * from read_parquet('s3://vat-smoke-test/_bdw_stages/notebook/stage-mwa-joined/rev-1/data.parquet')",
        )
        self.assertEqual(captured["touched_relations"], ["stage.mwa_joined_abrechnungen"])
        self.assertEqual(captured["source_summaries"][0]["relation"], "stage.mwa_joined_abrechnungen")
        self.assertEqual(captured["source_summaries"][0]["query_sql"], completed_stage_record()["querySql"])

    def test_validate_workspace_s3_virtual_glob_without_discovery_metadata(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})

        result = service.validate_query_sources(
            sql=(
                'SELECT ENTI.*, ZIFF.* '
                'FROM workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet" AS ENTI '
                'JOIN workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet" AS ZIFF '
                "ON ZIFF.abrechnung_refer = ENTI.id_"
            ),
            data_sources=["workspace.s3"],
        )

        self.assertEqual(result["status"], QUERY_SOURCE_VALID)
        self.assertEqual(result["missingReferences"], [])
        self.assertIn("workspace.s3.", result["references"][0])

    def test_validate_plain_s3_virtual_glob_without_discovery_metadata(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})

        result = service.validate_query_sources(
            sql=(
                'SELECT ENTI.*, ZIFF.* '
                'FROM s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet" AS ENTI '
                'JOIN s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet" AS ZIFF '
                "ON ZIFF.abrechnung_refer = ENTI.id_"
            ),
            data_sources=["workspace.s3"],
        )

        self.assertEqual(result["status"], QUERY_SOURCE_VALID)
        self.assertEqual(result["missingReferences"], [])
        self.assertEqual(len(result["references"]), 2)

    def test_start_query_job_rewrites_plain_s3_virtual_glob_relations_without_discovery(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(payload={"jobId": "query-s3-plain-virtual-direct"})

        service._query_jobs = SimpleNamespace(start_job=record_start)
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=[
                's3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"',
                's3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet"',
            ],
            touched_buckets=[
                "poc-tests-performance-evaluation-mwa-abrechnung-3-2",
            ],
        )

        snapshot = service.start_query_job(
            sql=(
                'SELECT ENTI.*, ZIFF.* '
                'FROM s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet" AS ENTI '
                'JOIN s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet" AS ZIFF '
                "ON ZIFF.abrechnung_refer = ENTI.id_"
            ),
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-mwa-virtual-plain",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-s3-plain-virtual-direct")
        self.assertIn(
            "read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet')",
            captured["execution_sql"],
        )
        self.assertIn(
            "read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet')",
            captured["execution_sql"],
        )

    def test_start_query_job_uses_plain_s3_glob_relation_without_discovery_call(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        discovery_calls = {"count": 0}

        def s3_relation_specs() -> dict:
            discovery_calls["count"] += 1
            return {}

        service._data_source_discovery = SimpleNamespace(s3_relation_specs=s3_relation_specs)
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(payload={"jobId": "query-s3-plain-virtual-no-discovery"})

        service._query_jobs = SimpleNamespace(start_job=record_start)
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=[
                's3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"',
            ],
            touched_buckets=[
                "poc-tests-performance-evaluation-mwa-abrechnung-3-2",
            ],
        )

        snapshot = service.start_query_job(
            sql=(
                'SELECT * '
                'FROM s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"'
            ),
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-mwa-virtual-plain-2",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-s3-plain-virtual-no-discovery")
        self.assertEqual(discovery_calls["count"], 0)
        self.assertIn(
            "read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet')",
            captured["execution_sql"],
        )

    def test_start_query_job_rewrites_workspace_s3_virtual_glob_relations_without_discovery(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(payload={"jobId": "query-s3-virtual-direct"})

        service._query_jobs = SimpleNamespace(start_job=record_start)
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=[
                'workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"',
                'workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet"',
            ],
            touched_buckets=[
                "poc-tests-performance-evaluation-mwa-abrechnung-3-2",
            ],
        )

        snapshot = service.start_query_job(
            sql=(
                'SELECT ENTI.*, ZIFF.* '
                'FROM workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet" AS ENTI '
                'JOIN workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet" AS ZIFF '
                "ON ZIFF.abrechnung_refer = ENTI.id_"
            ),
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-mwa-virtual",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-s3-virtual-direct")
        self.assertIn("read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet')", captured["execution_sql"])
        self.assertIn("read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet')", captured["execution_sql"])

    def test_start_query_job_uses_workspace_s3_glob_relation_without_discovery_call(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        discovery_calls = {"count": 0}

        def s3_relation_specs() -> dict:
            discovery_calls["count"] += 1
            return {}

        service._data_source_discovery = SimpleNamespace(s3_relation_specs=s3_relation_specs)
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(payload={"jobId": "query-s3-virtual-direct-nodisco"})

        service._query_jobs = SimpleNamespace(start_job=record_start)
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=[
                'workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"',
            ],
            touched_buckets=[
                "poc-tests-performance-evaluation-mwa-abrechnung-3-2",
            ],
        )

        snapshot = service.start_query_job(
            sql=(
                'SELECT * '
                'FROM workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
                '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"'
            ),
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-mwa-virtual-2",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-s3-virtual-direct-nodisco")
        self.assertEqual(discovery_calls["count"], 0)
        self.assertIn("read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet')", captured["execution_sql"])

    def test_start_query_job_applies_parquet_hive_option_to_s3_source_summary(
        self,
    ) -> None:
        relation = "test.tax_federal"
        object_path = "s3://tax-bucket/federal/manual_hive/**/*.parquet"
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = [
            SourceCatalog(
                name="workspace",
                connection_source_id="workspace.s3",
                schemas=[
                    SourceSchema(
                        name="test",
                        objects=[
                            SourceObject(
                                name="tax_federal",
                                kind="view",
                                relation=relation,
                                query_alias="s3.tax_bucket.federal.manual_hive.parquet",
                                s3_bucket="tax-bucket",
                                s3_key="federal/manual_hive/**/*.parquet",
                                s3_path=object_path,
                                s3_file_format="parquet",
                            )
                        ],
                    )
                ],
            )
        ]
        service._data_source_discovery = SimpleNamespace(
            s3_relation_specs=lambda: {
                relation: DiscoveredRelationSpec(
                    schema_name="test",
                    relation_name="tax_federal",
                    query_sql=build_s3_query("parquet", object_path, hive_partitioning=True),
                    object_path=object_path,
                    object_format="parquet",
                )
            }
        )
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=[relation],
            touched_buckets=["tax-bucket"],
        )
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(payload={"jobId": "query-s3-hive-off"})

        service._query_jobs = SimpleNamespace(start_job=record_start)

        service.start_query_job(
            sql="select * from s3.tax_bucket.federal.manual_hive.parquet",
            notebook_id="notebook",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=["workspace.s3"],
            query_options={"duckdb": {"parquetHivePartitioning": "off"}},
        )

        summaries = captured["source_summaries"]
        self.assertEqual(captured["query_options"]["duckdb"]["parquetHivePartitioning"], "off")
        self.assertIn("hive_partitioning=false", summaries[0]["query_sql"])

    def test_validation_uses_current_s3_specs_when_catalog_is_stale(self) -> None:
        relation = "kbpoimports.kbpo2020_521a28d3"
        object_path = "s3://kbpoimports/kbpo2020.parquet"
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(
            s3_relation_specs=lambda: {
                relation: DiscoveredRelationSpec(
                    schema_name="kbpoimports",
                    relation_name="kbpo2020_521a28d3",
                    query_sql=build_s3_query("parquet", object_path),
                    object_path=object_path,
                    object_format="parquet",
                    display_name="kbpo2020.parquet",
                    size_bytes=2048,
                    object_revision="etag-kbpo2020",
                )
            }
        )

        result = service.validate_query_sources(
            sql="select * from s3.kbpoimports.kbpo2020.parquet",
            data_sources=["workspace.s3"],
        )

        self.assertEqual(result["status"], QUERY_SOURCE_VALID)
        self.assertEqual(result["missingReferences"], [])
        self.assertEqual(result["matchedReferences"][0]["matchedRelation"], relation)

    def test_cache_preview_uses_current_s3_specs_when_catalog_is_stale(self) -> None:
        relation = "kbpoimports.kbpo2020_521a28d3"
        object_path = "s3://kbpoimports/kbpo2020.parquet"
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(
            s3_relation_specs=lambda: {
                relation: DiscoveredRelationSpec(
                    schema_name="kbpoimports",
                    relation_name="kbpo2020_521a28d3",
                    query_sql=build_s3_query("parquet", object_path),
                    object_path=object_path,
                    object_format="parquet",
                    display_name="kbpo2020.parquet",
                    size_bytes=2048,
                    object_revision="etag-kbpo2020",
                )
            }
        )

        with tempfile.TemporaryDirectory() as raw_tmp:
            service.settings = SimpleNamespace(query_cache_dir=raw_tmp)
            preview = service.query_cache_preview(
                sql="select * from s3.kbpoimports.kbpo2020.parquet",
                data_sources=["workspace.s3"],
                query_options={
                    "duckdb": {
                        "parquetHivePartitioning": "auto",
                        "cacheHydration": {
                            "mode": "on",
                            "scope": "referencedS3Parquet",
                            "indexPolicy": "autoPredicates",
                        },
                    }
                },
            )

        self.assertEqual(preview["status"], "ready")
        self.assertEqual(len(preview["sources"]), 1)
        self.assertEqual(preview["sources"][0]["relation"], relation)
        self.assertEqual(preview["sources"][0]["path"], object_path)
        self.assertEqual(preview["sources"][0]["status"], "miss")


class QuerySourceValidationApiTests(unittest.TestCase):
    def route_payload(
        self,
        sql: str,
        *,
        local_relations: dict[str, str] | None = None,
    ) -> dict[str, object]:
        class FakeWorkbenchService:
            def validate_query_sources(
                self,
                *,
                sql: str,
                data_sources: list[str] | None = None,
                local_relation_map: dict[str, str] | None = None,
                notebook_id: str = "",
            ):
                del notebook_id
                relation_index = build_relation_index(sample_catalogs())
                for logical_relation, physical_relation in (local_relation_map or {}).items():
                    entry = KnownRelationReference(relation=physical_relation)
                    relation_index[normalize_relation_key(logical_relation)] = entry
                    relation_index[normalize_relation_key(physical_relation)] = entry
                return validate_query_sources(
                    sql,
                    relation_index=relation_index,
                    data_sources=data_sources,
                ).payload

        response = validate_query_sources_route(
            payload=QuerySourceValidationPayload.model_validate(
                {
                    "sql": sql,
                    "localRelations": local_relations or {},
                }
            ),
            service=FakeWorkbenchService(),
        )
        return json.loads(response.body.decode("utf-8"))

    def test_validate_route_returns_valid_invalid_and_unchecked(self) -> None:
        self.assertEqual(
            self.route_payload("select * from pg_oltp.public.sales_orders")["status"],
            QUERY_SOURCE_VALID,
        )
        self.assertEqual(
            self.route_payload("select * from missing.schema_table")["status"],
            QUERY_SOURCE_INVALID,
        )
        self.assertEqual(
            self.route_payload("select 1")["status"],
            QUERY_SOURCE_UNCHECKED,
        )

    def test_validate_route_accepts_readable_s3_and_local_aliases(self) -> None:
        self.assertEqual(
            self.route_payload("select * from s3.test.federal_tax_data_10gb.csv")["status"],
            QUERY_SOURCE_VALID,
        )
        self.assertEqual(
            self.route_payload(
                "select * from local.test.federal_tax_data_1000mb.csv",
                local_relations={
                    "local.test.federal_tax_data_1000mb.csv": "workspace.local.saved_results.entry-1"
                },
            )["status"],
            QUERY_SOURCE_VALID,
        )


if __name__ == "__main__":
    unittest.main()

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
from bit_data_workbench.backend.query_jobs import (  # noqa: E402
    DUCKDB_EXECUTION_PATH_ISOLATED_WRITE,
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


KOSTENBELEGE_3_1_GENERATED_BUCKET = (
    "poc-tests-performance-evaluation-kostenbelege-3-1"
)


def kostenbelege_3_1_s3_reference(table_name: str) -> str:
    return s3_source_reference(
        bucket=KOSTENBELEGE_3_1_GENERATED_BUCKET,
        key=f"generated/kostenbelege_3_1/parquet/{table_name}/*.parquet",
    )


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
            connection_source_id="s3",
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

    def test_nested_cte_names_are_skipped(self) -> None:
        result = validate(
            """
            with original_semantics as (
              with UNIO as (
                select * from pg_oltp.public.customers
              )
              select * from UNIO
            )
            select count(*) from original_semantics
            """
        )

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.references, ["pg_oltp.public.customers"])
        self.assertEqual(result.missing_references, [])

        unchecked = validate(
            """
            with wrapper as (
              with inner_cte as (select 1)
              select * from inner_cte
            )
            select * from wrapper
            """
        )
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

    def test_start_query_job_records_invalid_references_as_failed_preflight(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        fake_query_jobs = SimpleNamespace(
            preflight_called=False,
            prepared_called=False,
            failed_error="",
        )

        def start_preflight(**kwargs):
            fake_query_jobs.preflight_called = True
            return SimpleNamespace(job_id=kwargs.get("requested_job_id") or "query-preflight")

        def start_prepared(*_args, **_kwargs):
            fake_query_jobs.prepared_called = True
            return SimpleNamespace(payload={"jobId": "query-preflight", "status": "queued"})

        def fail_preflight(job_id, *, error, message, backend_prepare_ms=None):
            fake_query_jobs.failed_error = str(error)
            return SimpleNamespace(
                payload={
                    "jobId": job_id,
                    "status": "failed",
                    "message": message,
                    "error": str(error),
                    "timings": {"backendPrepareMs": backend_prepare_ms},
                }
            )

        fake_query_jobs.start_preflight_job = start_preflight
        fake_query_jobs.start_prepared_job = start_prepared
        fake_query_jobs.fail_preflight_job = fail_preflight
        service._query_jobs = fake_query_jobs

        payload = service.start_query_job(
            sql="select * from missing.schema_table",
            notebook_id="notebook",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=[],
            query_options={"validation": {"sourceExistence": "on"}},
            client_job_id="query-client-invalid-reference",
        )

        self.assertTrue(fake_query_jobs.preflight_called)
        self.assertFalse(fake_query_jobs.prepared_called)
        self.assertEqual(payload["jobId"], "query-client-invalid-reference")
        self.assertEqual(payload["status"], "failed")
        self.assertIn("missing.schema_table", payload["error"])
        self.assertIn("missing.schema_table", fake_query_jobs.failed_error)

    def test_start_query_job_records_unisolated_read_as_failed_preflight(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        service.validate_query_sources = lambda **_kwargs: self.fail(
            "source validation should be skipped"
        )
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=["missing.schema_table"],
            touched_buckets=[],
        )
        fake_query_jobs = SimpleNamespace(
            prepared_called=False,
            failed_error="",
        )

        def start_preflight(**kwargs):
            return SimpleNamespace(job_id=kwargs.get("requested_job_id") or "query-preflight")

        def start_prepared(*_args, **_kwargs):
            fake_query_jobs.prepared_called = True
            return SimpleNamespace(payload={"jobId": "query-preflight", "status": "queued"})

        def fail_preflight(job_id, *, error, message, backend_prepare_ms=None):
            fake_query_jobs.failed_error = str(error)
            return SimpleNamespace(
                payload={
                    "jobId": job_id,
                    "status": "failed",
                    "message": message,
                    "error": str(error),
                    "timings": {"backendPrepareMs": backend_prepare_ms},
                }
            )

        fake_query_jobs.start_preflight_job = start_preflight
        fake_query_jobs.start_prepared_job = start_prepared
        fake_query_jobs.fail_preflight_job = fail_preflight
        service._query_jobs = fake_query_jobs

        payload = service.start_query_job(
            sql="select * from missing.schema_table",
            notebook_id="notebook",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=[],
            query_options={"validation": {"sourceExistence": "off"}},
            client_job_id="query-client-unisolated-reference",
        )

        self.assertFalse(fake_query_jobs.prepared_called)
        self.assertEqual(payload["jobId"], "query-client-unisolated-reference")
        self.assertEqual(payload["status"], "failed")
        self.assertIn("Read queries no longer wait for shared DuckDB file access", payload["error"])
        self.assertIn("missing.schema_table", fake_query_jobs.failed_error)

    def test_prepare_query_sql_fails_fast_for_unisolated_read_when_source_validation_off(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        service.validate_query_sources = lambda **_kwargs: self.fail(
            "source validation should be skipped"
        )
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=["missing.schema_table"],
            touched_buckets=[],
        )

        with self.assertRaises(ValueError) as exc:
            service.prepare_query_sql(
                sql="select * from missing.schema_table",
                notebook_id="notebook",
                data_sources=[],
                query_options={"validation": {"sourceExistence": "off"}},
            )

        self.assertIn("Read queries no longer wait for shared DuckDB file access", str(exc.exception))
        self.assertIn("missing.schema_table", str(exc.exception))

    def test_prepare_query_sql_rewrites_relation_only_loader_schema_alias(self) -> None:
        bucket = "poc-tests-performance-evaluation-kostenbelege-3-1"
        query_sql = (
            f"read_parquet('s3://{bucket}/generated/kostenbelege_3_1/"
            "parquet/kbkp_2019/*.parquet', hive_partitioning=false)"
        )
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = [
            SourceCatalog(
                name="workspace",
                connection_source_id="s3",
                schemas=[
                    SourceSchema(
                        name="s3_3_1_imports_a08e7385",
                        objects=[
                            SourceObject(
                                name="kbkp_2019",
                                kind="view",
                                relation="s3_3_1_imports_a08e7385.kbkp_2019",
                            )
                        ],
                    )
                ],
            )
        ]

        payload = service.prepare_query_sql(
            sql="SELECT COUNT(*) FROM s3_3_1_imports_a08e7385.kbkp_2019 KBKP",
            notebook_id="test-3-1-problem-solving",
            data_sources=["s3"],
            query_options={"validation": {"sourceExistence": "off"}},
        )

        self.assertEqual(
            payload["executionSql"],
            f"SELECT COUNT(*) FROM {query_sql} KBKP",
        )
        self.assertNotIn("s3_3_1_imports_a08e7385.kbkp_2019", payload["executionSql"])

    def test_prepare_query_sql_adapts_kbpo_loader_alias_for_original_query(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        kbpo_reference = kostenbelege_3_1_s3_reference("kbpo_2019")
        service._catalogs = [
            SourceCatalog(
                name="workspace",
                connection_source_id="s3",
                schemas=[
                    SourceSchema(
                        name="poc_tests_performance_evaluation_kostenbelege_3_1",
                        objects=[
                            SourceObject(
                                name="kbpo_2019_parquet",
                                kind="view",
                                relation="poc_tests_performance_evaluation_kostenbelege_3_1.kbpo_2019_parquet",
                                query_reference=kbpo_reference,
                                query_sql=(
                                    "read_parquet('s3://poc-tests-performance-evaluation-"
                                    "kostenbelege-3-1/generated/kostenbelege_3_1/"
                                    "parquet/kbpo_2019/*.parquet')"
                                ),
                            )
                        ],
                    )
                ],
            )
        ]

        payload = service.prepare_query_sql(
            sql=(
                "SELECT COUNT(*) "
                f"FROM {kbpo_reference} KBPO "
                "WHERE KBPO.KBKP_Belegnummer IS NOT NULL"
            ),
            notebook_id="test-3-1-problem-solving",
            data_sources=["s3"],
            query_options={"validation": {"sourceExistence": "off"}},
        )

        self.assertIn(
            '"KBKP_AusgleichBelegnummer" AS "KBKP_Belegnummer"',
            payload["executionSql"],
        )
        self.assertIn("read_parquet('s3://poc-tests-performance-evaluation-kostenbelege-3-1", payload["executionSql"])
        self.assertNotIn(kbpo_reference, payload["executionSql"])

    def test_materialized_stage_rewrites_virtual_s3_kbpo_source_when_validation_off(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service.validate_query_sources = lambda **_kwargs: self.fail(
            "source validation should be skipped"
        )
        kbpo_reference = kostenbelege_3_1_s3_reference("kbpo_2019")
        sql = (
            "SELECT COUNT(*) "
            f"FROM {kbpo_reference} KBPO "
            "WHERE KBPO.KBKP_Belegnummer IS NOT NULL"
        )
        query_options = {"validation": {"sourceExistence": "off"}}

        source_summaries = service._materialized_stage_source_summaries(
            sql,
            ["s3"],
            query_options,
        )
        execution_sql = service._materialized_stage_execution_sql(
            sql,
            ["s3"],
            query_options,
        )

        self.assertEqual(len(source_summaries), 1)
        self.assertEqual(source_summaries[0]["relation"], kbpo_reference)
        self.assertIn(
            '"KBKP_AusgleichBelegnummer" AS "KBKP_Belegnummer"',
            source_summaries[0]["query_sql"],
        )
        self.assertIn(
            '"KBKP_AusgleichBelegnummer" AS "KBKP_Belegnummer"',
            execution_sql,
        )
        self.assertIn(
            "read_parquet('s3://poc-tests-performance-evaluation-kostenbelege-3-1",
            execution_sql,
        )
        self.assertNotIn(kbpo_reference, execution_sql)

    def test_prepare_and_materialized_stage_rewrite_quoted_mixed_case_s3_file_union(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})
        file_names = [
            "KBPO_2018undvorher.parquet",
            "KBPO_2019.parquet",
            "KBPO2020.parquet",
            "KBPO2021.parquet",
            "KBPO2022.parquet",
            "KBPO2023.parquet",
            "KBPO2024.parquet",
            "KBPO2025.parquet",
        ]
        sql = "\nUNION ALL\n".join(
            f'SELECT * FROM s3.KBPOimports."{file_name}"'
            for file_name in file_names
        )
        query_options = {"validation": {"sourceExistence": "off"}}

        prepared = service.prepare_query_sql(
            sql=sql,
            notebook_id="kbpo",
            data_sources=["s3"],
            query_options=query_options,
        )
        execution_sql = service._materialized_stage_execution_sql(
            sql,
            ["s3"],
            query_options,
        )
        source_summaries = service._materialized_stage_source_summaries(
            sql,
            ["s3"],
            query_options,
        )

        self.assertEqual(prepared["executionSql"], execution_sql)
        for file_name in file_names:
            self.assertIn(
                f"read_parquet('s3://KBPOimports/{file_name}')",
                execution_sql,
            )
            self.assertNotIn(f's3.KBPOimports."{file_name}"', execution_sql)
        self.assertEqual(len(source_summaries), len(file_names))
        self.assertEqual(
            {summary["key"] for summary in source_summaries},
            set(file_names),
        )

    def test_prepare_query_sql_wraps_pipeline_stage_preview_in_copy_to_parquet(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})

        payload = service.prepare_query_sql(
            sql='SELECT * FROM s3.kbpoimports."KBPO2020.parquet"',
            display_sql='SELECT * FROM s3.kbpoimports."KBPO2020.parquet"',
            notebook_id="notebook",
            notebook_title="Pipeline Notebook",
            cell_id="cell-stage-1",
            data_sources=["s3"],
            query_options={"validation": {"sourceExistence": "off"}},
            stage={
                "enabled": True,
                "stageId": "stage-merge-all",
                "alias": "merge all",
                "materialize": True,
                "outputFileName": "merge_all.parquet",
            },
        )

        expected_target = (
            Path(tempfile.gettempdir()) / "bdw-stage-<run>" / "merge_all.parquet"
        ).as_posix()
        self.assertEqual(payload["duckdbExecutionPath"], DUCKDB_EXECUTION_PATH_ISOLATED_WRITE)
        self.assertEqual(payload["stageOutputFileName"], "merge_all.parquet")
        self.assertEqual(payload["stageDuckdbCopyTarget"], expected_target)
        self.assertTrue(payload["stageDuckdbCopyTargetIsRuntimePattern"])
        self.assertEqual(
            payload["executionSql"],
            "COPY (SELECT * FROM read_parquet('s3://kbpoimports/KBPO2020.parquet')) "
            f"TO '{expected_target}' (FORMAT PARQUET)",
        )
        self.assertNotIn("https://", payload["executionSql"])
        self.assertNotIn('s3.kbpoimports."KBPO2020.parquet"', payload["executionSql"])

    def test_prepare_query_sql_pipeline_stage_preview_normalizes_table_function_copy_target(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []

        payload = service.prepare_query_sql(
            sql="read_parquet('s3://kbpoimports/KBPO2020.parquet');",
            display_sql="read_parquet('s3://kbpoimports/KBPO2020.parquet');",
            notebook_id="notebook",
            notebook_title="Pipeline Notebook",
            cell_id="cell-stage-2",
            data_sources=["s3"],
            query_options={"validation": {"sourceExistence": "off"}},
            stage={
                "enabled": True,
                "stageId": "stage-unsafe-output",
                "alias": "unsafe output",
                "materialize": True,
                "outputFileName": "../unsafe output",
            },
        )

        expected_target = (
            Path(tempfile.gettempdir()) / "bdw-stage-<run>" / "unsafe_output.parquet"
        ).as_posix()
        self.assertEqual(payload["duckdbExecutionPath"], DUCKDB_EXECUTION_PATH_ISOLATED_WRITE)
        self.assertEqual(payload["stageOutputFileName"], "unsafe_output.parquet")
        self.assertEqual(
            payload["executionSql"],
            "COPY (SELECT * FROM read_parquet('s3://kbpoimports/KBPO2020.parquet')) "
            f"TO '{expected_target}' (FORMAT PARQUET)",
        )

    def test_prepare_query_sql_does_not_copy_wrap_non_materialized_stage_preview(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})

        payload = service.prepare_query_sql(
            sql='SELECT * FROM s3.kbpoimports."KBPO2020.parquet"',
            display_sql='SELECT * FROM s3.kbpoimports."KBPO2020.parquet"',
            notebook_id="notebook",
            data_sources=["s3"],
            query_options={"validation": {"sourceExistence": "off"}},
            stage={
                "enabled": True,
                "stageId": "stage-preview-only",
                "alias": "preview_only",
                "materialize": False,
                "outputFileName": "preview_only.parquet",
            },
        )

        self.assertEqual(
            payload["executionSql"],
            "SELECT * FROM read_parquet('s3://kbpoimports/KBPO2020.parquet')",
        )
        self.assertEqual(payload["duckdbExecutionPath"], "isolated-read")
        self.assertNotIn("stageDuckdbCopyTarget", payload)

    def test_materialized_stage_rewrites_lowercase_s3_aliases_to_catalog_object_case(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        file_names = [
            "KBPO_2018undvorher.parquet",
            "KBPO_2019.parquet",
            "KBPO2020.parquet",
        ]
        service._catalogs = [
            SourceCatalog(
                name="workspace",
                connection_source_id="s3",
                schemas=[
                    SourceSchema(
                        name="KBPOimports",
                        objects=[
                            SourceObject(
                                name=file_name,
                                kind="file",
                                relation=f"KBPOimports.{file_name.replace('.', '_')}",
                                s3_bucket="KBPOimports",
                                s3_key=file_name,
                                s3_file_format="parquet",
                            )
                            for file_name in file_names
                        ],
                    )
                ],
            )
        ]
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})
        sql = "\nUNION ALL\n".join(
            f'SELECT * FROM s3.kbpoimports."{file_name.lower()}"'
            for file_name in file_names
        )
        query_options = {"validation": {"sourceExistence": "off"}}

        execution_sql = service._materialized_stage_execution_sql(
            sql,
            ["s3"],
            query_options,
        )
        source_summaries = service._materialized_stage_source_summaries(
            sql,
            ["s3"],
            query_options,
        )

        for file_name in file_names:
            self.assertIn(
                f"read_parquet('s3://KBPOimports/{file_name}')",
                execution_sql,
            )
            self.assertNotIn(
                f"read_parquet('s3://kbpoimports/{file_name.lower()}')",
                execution_sql,
            )
        self.assertEqual(
            {summary["path"] for summary in source_summaries},
            {f"s3://KBPOimports/{file_name}" for file_name in file_names},
        )

    def test_source_validation_rejects_unverified_direct_s3_fallback(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})

        result = service.validate_query_sources(
            sql='select * from s3.kbpoimports."kbpo_2018undvorher.parquet"',
            data_sources=["s3"],
        )

        self.assertEqual(result["status"], QUERY_SOURCE_INVALID)
        self.assertEqual(
            result["missingReferences"],
            ['s3.kbpoimports."kbpo_2018undvorher.parquet"'],
        )

    def test_materialized_stage_rewrites_seeded_problem_full_query_virtual_sources(self) -> None:
        from bit_data_workbench.backend.notebooks import build_restart_seeded_shared_notebooks
        from bit_data_workbench.backend.s3_storage import s3_bucket_schema_name

        table_names = ("kbkp_2019", "kbpo_2019", "kbhp_2019", "dim_kalender")
        schema_name = s3_bucket_schema_name(KOSTENBELEGE_3_1_GENERATED_BUCKET)
        catalogs = [
            SourceCatalog(
                name="workspace",
                connection_source_id="s3",
                schemas=[
                    SourceSchema(
                        name=schema_name,
                        objects=[
                            SourceObject(
                                name=f"{table_name}_parquet",
                                kind="view",
                                relation=f"{schema_name}.{table_name}_parquet",
                                query_reference=kostenbelege_3_1_s3_reference(table_name),
                                s3_bucket=KOSTENBELEGE_3_1_GENERATED_BUCKET,
                                s3_key=f"generated/kostenbelege_3_1/parquet/{table_name}/*.parquet",
                                s3_file_format="parquet",
                            )
                            for table_name in table_names
                        ],
                    )
                ],
            )
        ]
        notebook = next(
            item
            for item in build_restart_seeded_shared_notebooks(catalogs)
            if item.notebook_id == "test-3-1-problem-solving"
        )
        full_query_cell = notebook.cells[-1]
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = catalogs

        source_summaries = service._materialized_stage_source_summaries(
            full_query_cell.sql,
            full_query_cell.data_sources,
            full_query_cell.query_options,
        )
        execution_sql = service._materialized_stage_execution_sql(
            full_query_cell.sql,
            full_query_cell.data_sources,
            full_query_cell.query_options,
        )

        self.assertEqual(len(source_summaries), 4)
        self.assertIn("read_parquet(", execution_sql)
        self.assertIn(
            '"KBKP_AusgleichBelegnummer" AS "KBKP_Belegnummer"',
            execution_sql,
        )
        self.assertNotIn("PRDESTV_VIEWCORE01", execution_sql)
        self.assertNotIn(
            's3."poc-tests-performance-evaluation-kostenbelege-3-1"',
            execution_sql,
        )

    def test_prepare_query_sql_returns_postgres_source_object_for_navigation(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()

        payload = service.prepare_query_sql(
            sql="select * from pg_oltp.public.sales_orders",
            notebook_id="notebook",
            data_sources=["pg_oltp"],
        )

        self.assertEqual(payload["duckdbExecutionPath"], "isolated-read")
        self.assertEqual(
            payload["sourceObjects"],
            [
                {
                    "label": "sales_orders",
                    "kind": "table",
                    "sourceId": "pg_oltp",
                    "relation": "pg_oltp.public.sales_orders",
                    "queryAlias": "",
                    "queryReference": "",
                    "bucket": "",
                    "key": "",
                    "path": "",
                    "format": "",
                }
            ],
        )

    def test_start_query_job_fails_fast_for_unisolated_read_when_source_validation_off(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        service.validate_query_sources = lambda **_kwargs: self.fail(
            "source validation should be skipped"
        )
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=["missing.schema_table"],
            touched_buckets=[],
        )
        service._query_jobs = SimpleNamespace(
            start_job=lambda **_kwargs: self.fail("query should fail during prepare")
        )

        with self.assertRaises(ValueError) as exc:
            service.start_query_job(
                sql="select * from missing.schema_table",
                notebook_id="notebook",
                notebook_title="Notebook",
                cell_id="cell-1",
                data_sources=[],
                query_options={"validation": {"sourceExistence": "off"}},
            )

        self.assertIn("Read queries no longer wait for shared DuckDB file access", str(exc.exception))
        self.assertIn("missing.schema_table", str(exc.exception))

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

    def test_prepare_query_sql_returns_local_workspace_alias_for_navigation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            schema_name = "workspace_local_browser_abc"
            table_name = "entry_local_entry_123"
            source_dir = temp_path / "local-workspace-query-sources" / schema_name
            source_dir.mkdir(parents=True)
            (source_dir / f"{table_name}.csv").write_text("id\n1\n", encoding="utf-8")
            physical_relation = f"{schema_name}.{table_name}"
            logical_relation = "workspace.local.saved_results.local-entry-123"

            service = WorkbenchService.__new__(WorkbenchService)
            service._lock = threading.RLock()
            service._catalogs = []
            service.settings = SimpleNamespace(
                duckdb_database=temp_path / "bit-data-workbench.dev.duckdb"
            )

            payload = service.prepare_query_sql(
                sql=f"select * from {physical_relation}",
                display_sql=f"select * from {logical_relation}",
                notebook_id="notebook",
                data_sources=["workspace.local"],
                local_relation_map={logical_relation: physical_relation},
            )

            self.assertEqual(
                payload["sourceObjects"],
                [
                    {
                        "label": f"{table_name}.csv",
                        "kind": "table",
                        "sourceId": "workspace.local",
                        "relation": physical_relation,
                        "queryAlias": logical_relation,
                        "queryReference": "",
                        "bucket": "",
                        "key": f"{table_name}.csv",
                        "path": (source_dir / f"{table_name}.csv").as_posix(),
                        "format": "csv",
                    }
                ],
            )

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
            data_sources=["s3"],
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
                    "path": "s3://test/federal_tax_data_10gb.csv",
                    "format": "csv",
                    "query_sql": "read_csv_auto('s3://test/federal_tax_data_10gb.csv')",
                    "size_bytes": 0,
                    "object_revision": "",
                    "display_name": "federal_tax_data_10gb",
                }
            ],
        )

    def test_prepare_query_sql_returns_s3_source_object_for_navigation(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = sample_catalogs()
        service._analyze_query = lambda _sql, **_kwargs: SimpleNamespace(
            touched_relations=["test.federal_tax_data_10gb"],
            touched_buckets=["test"],
        )

        payload = service.prepare_query_sql(
            sql="select * from s3.test.federal_tax_data_10gb.csv",
            notebook_id="notebook",
            data_sources=["s3"],
        )

        self.assertEqual(
            payload["sourceObjects"],
            [
                {
                    "label": "federal_tax_data_10gb",
                    "kind": "s3-object",
                    "sourceId": "s3",
                    "relation": "test.federal_tax_data_10gb",
                    "queryAlias": "s3.test.federal_tax_data_10gb.csv",
                    "queryReference": 's3.test."federal_tax_data_10gb.csv"',
                    "bucket": "test",
                    "key": "federal_tax_data_10gb.csv",
                    "path": "s3://test/federal_tax_data_10gb.csv",
                    "format": "csv",
                }
            ],
        )

    def test_prepare_query_sql_returns_direct_s3_table_function_source_object_for_navigation(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []

        payload = service.prepare_query_sql(
            sql=(
                "select * from read_parquet("
                "'s3://direct-source-bucket/generated/entities/*.parquet'"
                ")"
            ),
            notebook_id="notebook",
            data_sources=["s3"],
        )

        self.assertEqual(
            payload["sourceObjects"],
            [
                {
                    "label": "*.parquet",
                    "kind": "s3-object",
                    "sourceId": "s3",
                    "relation": "s3://direct-source-bucket/generated/entities/*.parquet",
                    "queryAlias": "",
                    "queryReference": "",
                    "bucket": "direct-source-bucket",
                    "key": "generated/entities/*.parquet",
                    "path": "s3://direct-source-bucket/generated/entities/*.parquet",
                    "format": "parquet",
                }
            ],
        )

    def test_prepare_query_sql_maps_generated_part_file_to_dataset_for_navigation(self) -> None:
        bucket = "test"
        part_prefix = "generated/kostenbelege_3_1/parquet/dim_kalender/"
        dataset_reference = s3_source_reference(
            bucket=bucket,
            key=f"{part_prefix}*.parquet",
        )
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = [
            SourceCatalog(
                name="workspace",
                connection_source_id="s3",
                schemas=[
                    SourceSchema(
                        name="test",
                        label=bucket,
                        objects=[
                            SourceObject(
                                name="dim_kalender_parquet",
                                kind="file",
                                relation="test.dim_kalender_parquet",
                                display_name="dim_kalender.parquet",
                                query_alias=(
                                    "s3.test.generated.kostenbelege_3_1.parquet."
                                    "dim_kalender.parquet"
                                ),
                                query_reference=dataset_reference,
                                query_sql=(
                                    "read_parquet("
                                    "'s3://test/generated/kostenbelege_3_1/"
                                    "parquet/dim_kalender/*.parquet')"
                                ),
                                s3_bucket=bucket,
                                s3_path=f"s3://{bucket}/{part_prefix}*.parquet",
                                s3_file_format="parquet",
                                s3_download_kind="generated_parts",
                                s3_part_prefix=part_prefix,
                                s3_part_file_format="parquet",
                                s3_part_count=1,
                                s3_download_filename="dim_kalender.parquet",
                            )
                        ],
                    )
                ],
            )
        ]

        payload = service.prepare_query_sql(
            sql=(
                "select * from read_parquet("
                "'s3://test/generated/kostenbelege_3_1/parquet/"
                "dim_kalender/part-00001.parquet')"
            ),
            notebook_id="notebook",
            data_sources=["s3"],
        )

        self.assertEqual(len(payload["sourceObjects"]), 1)
        source = payload["sourceObjects"][0]
        self.assertEqual(source["label"], "dim_kalender.parquet")
        self.assertEqual(source["queryReference"], dataset_reference)
        self.assertEqual(source["path"], f"s3://{bucket}/{part_prefix}*.parquet")
        self.assertNotIn("part-00001.parquet", source["queryReference"])
        self.assertNotIn("part-00001.parquet", source["path"])

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
            data_sources=["s3"],
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
            data_sources=["s3"],
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
            data_sources=["s3"],
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
            data_sources=["s3"],
        )

        self.assertEqual(
            payload["executionSql"],
            "select * from read_parquet('s3://vat-smoke-test/_bdw_stages/notebook/stage-mwa-joined/rev-1/data.parquet')",
        )
        self.assertEqual(payload["touchedRelations"], ["stage.mwa_joined_abrechnungen"])
        self.assertEqual(payload["touchedBuckets"], ["vat-smoke-test"])
        self.assertEqual(payload["duckdbExecutionPath"], "isolated-read")
        self.assertEqual(
            payload["sourceObjects"],
            [
                {
                    "label": "mwa_joined_abrechnungen",
                    "kind": "s3-object",
                    "sourceId": "s3",
                    "relation": "stage.mwa_joined_abrechnungen",
                    "queryAlias": "",
                    "queryReference": "",
                    "bucket": "vat-smoke-test",
                    "key": "_bdw_stages/notebook/stage-mwa-joined/rev-1/data.parquet",
                    "path": (
                        "s3://vat-smoke-test/_bdw_stages/notebook/"
                        "stage-mwa-joined/rev-1/data.parquet"
                    ),
                    "format": "parquet",
                }
            ],
        )

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
            data_sources=["s3"],
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

    def test_materialized_stage_query_job_requests_isolated_write_path(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        captured: dict[str, object] = {}

        def record_start(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(job_id="query-stage-copy")

        def wait_for_terminal(job_id, **_kwargs):
            return SimpleNamespace(payload={"jobId": job_id, "status": "completed"})

        service._query_jobs = SimpleNamespace(
            start_job=record_start,
            wait_for_terminal=wait_for_terminal,
        )

        payload = service._run_materialized_stage_query_job(
            requested_job_id="query-stage-copy",
            display_sql="SELECT count(*) FROM stage.sample_data",
            execution_sql=(
                "COPY (SELECT count(*) FROM stage.sample_data) "
                "TO '/tmp/merge_all.parquet' (FORMAT PARQUET)"
            ),
            result_preview_sql="SELECT * FROM read_parquet('/tmp/merge_all.parquet')",
            notebook_id="notebook",
            notebook_title="Notebook",
            cell_id="cell-stage",
            data_sources=["s3"],
            source_summaries=[
                {
                    "relation": "stage.sample_data",
                    "query_sql": "SELECT * FROM read_parquet('s3://bucket/data.parquet')",
                }
            ],
            touched_relations=["stage.sample_data"],
            touched_buckets=["bucket"],
            query_options={},
            is_cancelled=lambda: False,
        )

        self.assertEqual(payload["status"], "completed")
        self.assertEqual(
            captured["duckdb_execution_path_override"],
            DUCKDB_EXECUTION_PATH_ISOLATED_WRITE,
        )

    def test_validate_s3_storage_virtual_glob_without_discovery_metadata(self) -> None:
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
            data_sources=["s3"],
        )

        self.assertEqual(result["status"], QUERY_SOURCE_VALID)
        self.assertEqual(result["missingReferences"], [])
        self.assertIn("s3.", result["references"][0])

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
            data_sources=["s3"],
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
            data_sources=["s3"],
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
            data_sources=["s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-s3-plain-virtual-no-discovery")
        self.assertEqual(discovery_calls["count"], 0)
        self.assertIn(
            "read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet')",
            captured["execution_sql"],
        )

    def test_start_query_job_rewrites_s3_storage_virtual_glob_relations_without_discovery(self) -> None:
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
            cell_id="cell-mwa-virtual",
            data_sources=["s3"],
        )

        self.assertEqual(snapshot["jobId"], "query-s3-virtual-direct")
        self.assertIn("read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet')", captured["execution_sql"])
        self.assertIn("read_parquet('s3://poc-tests-performance-evaluation-mwa-abrechnung-3-2/generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet')", captured["execution_sql"])

    def test_start_query_job_uses_s3_storage_glob_relation_without_discovery_call(self) -> None:
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
            cell_id="cell-mwa-virtual-2",
            data_sources=["s3"],
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
                connection_source_id="s3",
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
            data_sources=["s3"],
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
            data_sources=["s3"],
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
                data_sources=["s3"],
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

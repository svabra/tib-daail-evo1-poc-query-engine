from __future__ import annotations

import json
from pathlib import Path
import sys
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
                            s3_bucket="test",
                        ),
                        SourceObject(
                            name="vat_smoke_part_00001",
                            kind="view",
                            relation="test.vat_smoke_part_00001",
                            query_alias=(
                                "s3.test.generated.vat_smoke.part_00001."
                                "parquet"
                            ),
                            s3_bucket="test",
                        )
                    ],
                )
            ],
        ),
    ]


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

    def test_s3_query_alias_validates(self) -> None:
        result = validate("select * from s3.test.federal_tax_data_10gb.csv")

        self.assertEqual(result.status, QUERY_SOURCE_VALID)
        self.assertEqual(result.missing_references, [])
        self.assertEqual(
            result.matched_references[0].matched_relation,
            "test.federal_tax_data_10gb",
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
        self.assertEqual(captured["execution_sql"], "select * from test.federal_tax_data_10gb")
        self.assertEqual(
            captured["source_summaries"],
            [
                {
                    "relation": "test.federal_tax_data_10gb",
                    "query_alias": "s3.test.federal_tax_data_10gb.csv",
                    "bucket": "test",
                    "key": "",
                    "path": "",
                    "format": "",
                }
            ],
        )


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
            ):
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

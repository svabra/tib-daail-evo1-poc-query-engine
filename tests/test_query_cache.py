from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb
from fastapi import HTTPException


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import (  # noqa: E402
    QueryCachePayload,
    delete_query_cache as delete_query_cache_route,
    delete_runtime_storage_query_cache as delete_runtime_storage_query_cache_route,
    expire_query_cache as expire_query_cache_route,
    query_cache_preview as query_cache_preview_route,
    rehydrate_query_cache as rehydrate_query_cache_route,
    runtime_storage_state as runtime_storage_state_route,
)
from bit_data_workbench.backend.query_cache import (  # noqa: E402
    cache_preview,
    delete_cache_by_key,
    delete_cache,
    expire_cache,
    hydrate_cache,
    infer_predicate_index_columns,
    list_query_cache_datasets,
)


def _write_federal_tax_parquet(path: Path, *, rows: int = 1000) -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE federal_tax AS
            SELECT
              printf('TAX-%06d', i) AS taxpayer_id,
              2024 + (i % 2) AS tax_year,
              (i * 17) % 100000 AS federal_tax_due,
              DATE '2026-04-15' AS filing_date
            FROM range(?) AS generated(i)
            """,
            [rows],
        )
        connection.execute(f"COPY federal_tax TO '{path.as_posix()}' (FORMAT PARQUET)")
    finally:
        connection.close()


def _write_unindexed_parquet(path: Path) -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE unindexed_tax AS
            SELECT
              2026 AS tax_year,
              1200 + i AS federal_tax_due,
              DATE '2026-04-15' AS filing_date
            FROM range(100) AS generated(i)
            """
        )
        connection.execute(f"COPY unindexed_tax TO '{path.as_posix()}' (FORMAT PARQUET)")
    finally:
        connection.close()


def _summary_for(path: Path, *, revision: str = "etag-one") -> dict[str, object]:
    return {
        "relation": "s3.poc.federal_tax.parquet",
        "path": "s3://poc/federal_tax.parquet",
        "format": "parquet",
        "query_sql": f"SELECT * FROM read_parquet('{path.as_posix()}')",
        "size_bytes": path.stat().st_size,
        "object_revision": revision,
    }


def _cache_options() -> dict[str, object]:
    return {
        "duckdb": {
            "parquetHivePartitioning": "auto",
            "cacheHydration": {
                "mode": "on",
                "scope": "referencedS3Parquet",
                "indexPolicy": "autoPredicates",
            },
        }
    }


class QueryCacheTests(unittest.TestCase):
    def test_infers_predicate_columns_from_where_in_and_join(self) -> None:
        columns = infer_predicate_index_columns(
            """
            SELECT *
            FROM s3.poc.federal_tax.parquet AS tax
            JOIN lookup USING (taxpayer_id)
            WHERE tax.tax_year = 2026
              AND "filing status" IN ('single')
            """
        )

        self.assertIn("tax_year", columns)
        self.assertIn("filing status", columns)
        self.assertIn("taxpayer_id", columns)

    def test_hydrates_reuses_stales_and_expires_known_s3_parquet_cache(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            parquet_path = root / "federal_tax.parquet"
            cache_root = root / "cache"
            _write_federal_tax_parquet(parquet_path, rows=5000)
            source_summary = _summary_for(parquet_path)
            sql = (
                "SELECT taxpayer_id, federal_tax_due "
                "FROM s3.poc.federal_tax.parquet "
                "WHERE taxpayer_id = 'TAX-000123'"
            )

            with patch.dict(os.environ, {"BDW_QUERY_CACHE_DIR": str(cache_root)}):
                initial_preview = cache_preview(
                    settings=None,  # type: ignore[arg-type]
                    sql=sql,
                    source_summaries=[source_summary],
                    query_options=_cache_options(),
                )
                self.assertEqual(initial_preview["sources"][0]["status"], "miss")
                self.assertEqual(
                    initial_preview["sources"][0]["sourceViewRelation"],
                    "s3.poc.federal_tax.parquet",
                )
                self.assertTrue(initial_preview["sources"][0]["runtimeTable"])
                self.assertTrue(initial_preview["sources"][0]["temporary"])
                self.assertEqual(initial_preview["sources"][0]["sourceRevision"], "etag-one")
                self.assertIn("cache_", initial_preview["sources"][0]["cacheTable"])
                self.assertEqual(initial_preview["sources"][0]["rowCount"], 0)

                connection = duckdb.connect(":memory:")
                try:
                    updated_summaries, hydration = hydrate_cache(
                        connection=connection,
                        sql=sql,
                        source_summaries=[source_summary],
                        query_options=_cache_options(),
                    )

                    self.assertTrue(hydration["enabled"])
                    self.assertEqual(hydration["sources"][0]["status"], "hit")
                    self.assertEqual(
                        hydration["sources"][0]["indexColumns"],
                        ["taxpayer_id"],
                    )
                    self.assertEqual(hydration["sources"][0]["rowCount"], 5000)
                    self.assertTrue(hydration["sources"][0]["runtimeTable"])
                    self.assertTrue(hydration["sources"][0]["temporary"])
                    self.assertIn(
                        "temporary compute storage",
                        hydration["sources"][0]["temporaryWarning"],
                    )
                    self.assertIn("cache_", updated_summaries[0]["query_sql"])

                    connection.execute(
                        f"CREATE VIEW hydrated_tax AS {updated_summaries[0]['query_sql']}"
                    )
                    value = connection.execute(
                        """
                        SELECT federal_tax_due
                        FROM hydrated_tax
                        WHERE taxpayer_id = 'TAX-000123'
                        """
                    ).fetchone()[0]
                    self.assertEqual(value, (123 * 17) % 100000)
                finally:
                    connection.close()

                hit_preview = cache_preview(
                    settings=None,  # type: ignore[arg-type]
                    sql=sql,
                    source_summaries=[source_summary],
                    query_options=_cache_options(),
                )
                self.assertEqual(hit_preview["sources"][0]["status"], "hit")
                self.assertGreater(hit_preview["sources"][0]["cacheSizeBytes"], 0)

                stale_preview = cache_preview(
                    settings=None,  # type: ignore[arg-type]
                    sql=sql,
                    source_summaries=[_summary_for(parquet_path, revision="etag-two")],
                    query_options=_cache_options(),
                )
                self.assertEqual(stale_preview["sources"][0]["status"], "stale")
                self.assertIn("source data changed", stale_preview["sources"][0]["statusReason"])

                expired_preview = expire_cache(
                    sql=sql,
                    source_summaries=[source_summary],
                    query_options=_cache_options(),
                )
                self.assertEqual(expired_preview["sources"][0]["status"], "expired")
                self.assertIn("manually marked", expired_preview["sources"][0]["statusReason"])

                database_path = Path(str(expired_preview["sources"][0]["cacheDatabasePath"]))
                if database_path.exists():
                    database_path.unlink()
                missing_preview = cache_preview(
                    settings=None,  # type: ignore[arg-type]
                    sql=sql,
                    source_summaries=[source_summary],
                    query_options=_cache_options(),
                )
                self.assertEqual(missing_preview["sources"][0]["status"], "expired")
                self.assertFalse(missing_preview["sources"][0]["physicalCacheExists"])

    def test_delete_cache_removes_matching_runtime_cache_files(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            parquet_path = root / "federal_tax.parquet"
            cache_root = root / "cache"
            _write_federal_tax_parquet(parquet_path, rows=100)
            source_summary = _summary_for(parquet_path)
            sql = "SELECT * FROM s3.poc.federal_tax.parquet WHERE taxpayer_id = 'TAX-000001'"

            with patch.dict(os.environ, {"BDW_QUERY_CACHE_DIR": str(cache_root)}):
                connection = duckdb.connect(":memory:")
                try:
                    _updated_summaries, hydration = hydrate_cache(
                        connection=connection,
                        sql=sql,
                        source_summaries=[source_summary],
                        query_options=_cache_options(),
                    )
                finally:
                    connection.close()

                database_path = Path(str(hydration["sources"][0]["cacheDatabasePath"]))
                metadata_path = Path(str(hydration["sources"][0]["metadataPath"]))
                self.assertTrue(database_path.exists())
                self.assertTrue(metadata_path.exists())

                deleted = delete_cache(
                    sql=sql,
                    source_summaries=[source_summary],
                    query_options=_cache_options(),
                )

                self.assertTrue(deleted["deleted"])
                self.assertTrue(deleted["sources"][0]["deleted"])
                self.assertFalse(database_path.exists())
                self.assertFalse(metadata_path.exists())
                self.assertEqual(deleted["sources"][0]["status"], "miss")
                self.assertIn("deleted", deleted["sources"][0]["statusReason"])

    def test_cache_metadata_records_and_deduplicates_cell_refs(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            parquet_path = root / "federal_tax.parquet"
            cache_root = root / "cache"
            _write_federal_tax_parquet(parquet_path, rows=100)
            source_summary = _summary_for(parquet_path)
            sql = "SELECT * FROM s3.poc.federal_tax.parquet WHERE taxpayer_id = 'TAX-000001'"

            with patch.dict(os.environ, {"BDW_QUERY_CACHE_DIR": str(cache_root)}):
                connection = duckdb.connect(":memory:")
                try:
                    hydrate_cache(
                        connection=connection,
                        sql=sql,
                        source_summaries=[source_summary],
                        query_options=_cache_options(),
                        cache_context={
                            "notebookId": "notebook-one",
                            "notebookTitle": "Kostenbelege",
                            "cellId": "cell-one",
                        },
                    )
                    hydrate_cache(
                        connection=connection,
                        sql=sql,
                        source_summaries=[source_summary],
                        query_options=_cache_options(),
                        cache_context={
                            "notebookId": "notebook-one",
                            "notebookTitle": "Kostenbelege updated",
                            "cellId": "cell-one",
                        },
                    )
                    hydrate_cache(
                        connection=connection,
                        sql=sql,
                        source_summaries=[source_summary],
                        query_options=_cache_options(),
                        cache_context={
                            "notebookId": "notebook-one",
                            "notebookTitle": "Kostenbelege",
                            "cellId": "cell-two",
                        },
                    )
                finally:
                    connection.close()

                datasets = list_query_cache_datasets()

        self.assertEqual(len(datasets), 1)
        self.assertEqual(datasets[0]["notebookId"], "notebook-one")
        self.assertEqual(datasets[0]["cellId"], "cell-two")
        self.assertEqual(len(datasets[0]["cellRefs"]), 2)
        self.assertEqual(datasets[0]["cellRefs"][0]["cellId"], "cell-one")
        self.assertEqual(datasets[0]["cellRefs"][0]["notebookTitle"], "Kostenbelege updated")
        self.assertIn("lastUsedAt", datasets[0])

    def test_delete_cache_by_key_uses_server_derived_cache_paths(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            cache_key = "a" * 40
            cache_root = root / "cache"
            cache_root.mkdir()
            database_path = cache_root / f"{cache_key}.duckdb"
            wal_path = cache_root / f"{cache_key}.duckdb.wal"
            metadata_path = cache_root / f"{cache_key}.json"
            database_path.write_bytes(b"db")
            wal_path.write_bytes(b"wal")
            metadata_path.write_text("{}", encoding="utf-8")

            with patch.dict(os.environ, {"BDW_QUERY_CACHE_DIR": str(cache_root)}):
                result = delete_cache_by_key(cache_key)

        self.assertTrue(result["deleted"])
        self.assertFalse(database_path.exists())
        self.assertFalse(wal_path.exists())
        self.assertFalse(metadata_path.exists())

        with self.assertRaises(ValueError):
            delete_cache_by_key("../not-a-cache-key")

    def test_hydrates_without_art_index_when_no_useful_column_exists(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            parquet_path = root / "federal_tax.parquet"
            _write_unindexed_parquet(parquet_path)
            source_summary = _summary_for(parquet_path)

            with patch.dict(os.environ, {"BDW_QUERY_CACHE_DIR": str(root / "cache")}):
                connection = duckdb.connect(":memory:")
                try:
                    _updated_summaries, hydration = hydrate_cache(
                        connection=connection,
                        sql="SELECT count(*) FROM s3.poc.federal_tax.parquet WHERE federal_tax_due > 50",
                        source_summaries=[source_summary],
                        query_options=_cache_options(),
                    )
                finally:
                    connection.close()

            self.assertEqual(hydration["sources"][0]["indexColumns"], [])
            self.assertIn("without ART indexes", hydration["sources"][0]["indexReason"])


class QueryCacheRouteTests(unittest.TestCase):
    def test_routes_forward_payload_to_service(self) -> None:
        calls: list[tuple[str, dict[str, object]]] = []

        class FakeService:
            def query_cache_preview(self, **kwargs):
                calls.append(("preview", kwargs))
                return {"status": "ready", "sources": []}

            def rehydrate_query_cache(self, **kwargs):
                calls.append(("rehydrate", kwargs))
                return {"status": "ready", "hydration": {"enabled": True}}

            def expire_query_cache(self, **kwargs):
                calls.append(("expire", kwargs))
                return {"status": "ready", "sources": []}

            def delete_query_cache(self, **kwargs):
                calls.append(("delete", kwargs))
                return {"status": "ready", "sources": [], "deleted": False}

            def runtime_storage(self):
                calls.append(("runtime-storage", {}))
                return {"queryCache": {"datasets": []}}

            def delete_runtime_query_cache(self, cache_key):
                calls.append(("runtime-delete", {"cache_key": cache_key}))
                return {"cacheKey": cache_key, "deleted": True}

        payload = QueryCachePayload(
            sql="select * from s3.poc.federal_tax.parquet",
            dataSources=["workspace.s3"],
            localRelations={"local.source": "workspace.schema.table"},
            queryOptions=_cache_options(),
        )

        preview = query_cache_preview_route(payload, service=FakeService())
        rehydrated = rehydrate_query_cache_route(payload, service=FakeService())
        expired = expire_query_cache_route(payload, service=FakeService())
        deleted = delete_query_cache_route(payload, service=FakeService())
        runtime_storage = runtime_storage_state_route(service=FakeService())
        runtime_deleted = delete_runtime_storage_query_cache_route("a" * 40, service=FakeService())

        self.assertEqual(preview.status_code, 200)
        self.assertEqual(rehydrated.status_code, 200)
        self.assertEqual(expired.status_code, 200)
        self.assertEqual(deleted.status_code, 200)
        self.assertEqual(runtime_storage.status_code, 200)
        self.assertEqual(runtime_deleted.status_code, 200)
        self.assertEqual(
            [name for name, _kwargs in calls],
            ["preview", "rehydrate", "expire", "delete", "runtime-storage", "runtime-delete"],
        )
        self.assertEqual(calls[0][1]["sql"], payload.sql)
        self.assertEqual(calls[0][1]["data_sources"], ["workspace.s3"])
        self.assertEqual(
            calls[0][1]["local_relation_map"],
            {"local.source": "workspace.schema.table"},
        )
        self.assertEqual(
            calls[0][1]["query_options"]["duckdb"]["cacheHydration"]["mode"],
            "on",
        )
        self.assertEqual(calls[-1][1]["cache_key"], "a" * 40)

    def test_rehydrate_route_converts_runtime_failure_to_structured_http_error(self) -> None:
        class FakeService:
            def rehydrate_query_cache(self, **_kwargs):
                raise RuntimeError("DuckDB cache hydrate failed")

        payload = QueryCachePayload(
            sql="select * from s3.poc.federal_tax.parquet",
            notebookId="notebook-cache",
            cellId="cell-cache",
            dataSources=["workspace.s3"],
            queryOptions=_cache_options(),
        )

        with self.assertLogs("bit_data_workbench.api.router", level="ERROR") as captured:
            with self.assertRaises(HTTPException) as context:
                rehydrate_query_cache_route(payload, service=FakeService())

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(
            context.exception.detail,
            "Query cache hydration failed: DuckDB cache hydrate failed",
        )
        output = "\n".join(captured.output)
        self.assertIn("Query cache hydration failed", output)
        self.assertIn("notebook-cache", output)
        self.assertIn("cell-cache", output)
        self.assertNotIn(payload.sql, output)


if __name__ == "__main__":
    unittest.main()

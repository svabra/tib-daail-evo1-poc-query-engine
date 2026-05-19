from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.notebook_presets import (  # noqa: E402
    _build_kostenbelege_3_1_optimized_sql,
    _build_kostenbelege_3_1_sql,
)
from bit_data_workbench.data_generator.kostenbelege_3_1 import (  # noqa: E402
    DIM_KALENDER_COLUMNS,
    GENERATOR,
    KBHP_COLUMNS,
    KBKP_COLUMNS,
    KBPO_COLUMNS,
    KOSTENBELEGE_3_1_S3_SCHEMA,
    KOSTENBELEGE_3_1_TABLES,
    dim_kalender_select,
    kbhp_2019_select,
    kbkp_2019_select,
    kbpo_2019_select,
)
from bit_data_workbench.data_generator.registry import DataGeneratorRegistry  # noqa: E402


class FakeConnection:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.closed = False

    def execute(self, sql: str):
        self.executed_sql.append(sql)
        return self

    def close(self) -> None:
        self.closed = True


class Kostenbelege31LoaderTests(unittest.TestCase):
    def test_registry_discovers_kostenbelege_loader(self) -> None:
        generator = DataGeneratorRegistry().generator("kostenbelege_3_1_multi_source_loader")
        payload = generator.definition().payload

        self.assertEqual(payload["generatorId"], "kostenbelege_3_1_multi_source_loader")
        self.assertEqual(payload["targetKind"], "contest")
        self.assertEqual(payload["title"], "Kostenbelege Multi-Source Loader (3.1)")
        self.assertEqual(
            payload["treePath"],
            ["PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"],
        )
        self.assertIn("oltp", payload["tags"])
        self.assertIn("olap", payload["tags"])
        self.assertIn("parquet", payload["tags"])

    def test_row_counts_cover_query_tables(self) -> None:
        row_counts = GENERATOR._row_counts(4000)

        self.assertEqual(set(row_counts), set(KOSTENBELEGE_3_1_TABLES))
        self.assertEqual(row_counts["kbkp_2019"], 1000)
        self.assertEqual(row_counts["kbpo_2019"], 2000)
        self.assertEqual(row_counts["kbhp_2019"], 2000)
        self.assertEqual(row_counts["dim_kalender"], 61)

    def test_written_targets_include_oltp_olap_and_s3_parquet_for_each_table(self) -> None:
        targets = GENERATOR._written_targets(
            root_prefix="s3://loader-bucket/generated/kostenbelege_3_1"
        )

        self.assertEqual(len(targets), 12)
        self.assertEqual(
            {target.location for target in targets},
            {
                "pg_oltp.public.kbkp_2019",
                "pg_olap.public.kbkp_2019",
                "s3://loader-bucket/generated/kostenbelege_3_1/parquet/kbkp_2019",
                "pg_oltp.public.kbpo_2019",
                "pg_olap.public.kbpo_2019",
                "s3://loader-bucket/generated/kostenbelege_3_1/parquet/kbpo_2019",
                "pg_oltp.public.kbhp_2019",
                "pg_olap.public.kbhp_2019",
                "s3://loader-bucket/generated/kostenbelege_3_1/parquet/kbhp_2019",
                "pg_oltp.public.dim_kalender",
                "pg_olap.public.dim_kalender",
                "s3://loader-bucket/generated/kostenbelege_3_1/parquet/dim_kalender",
            },
        )

    def test_generated_selects_match_declared_schema(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(f"CREATE TEMP TABLE kbkp AS {kbkp_2019_select(0, 8)}")
            connection.execute(f"CREATE TEMP TABLE kbpo AS {kbpo_2019_select(0, 16, 8)}")
            connection.execute(f"CREATE TEMP TABLE kbhp AS {kbhp_2019_select(0, 16, 8)}")
            connection.execute(f"CREATE TEMP TABLE kale AS {dim_kalender_select(0, 61)}")

            expected = {
                "kbkp": [column.split(" ", 1)[0] for column in KBKP_COLUMNS],
                "kbpo": [column.split(" ", 1)[0] for column in KBPO_COLUMNS],
                "kbhp": [column.split(" ", 1)[0] for column in KBHP_COLUMNS],
                "kale": [column.split(" ", 1)[0] for column in DIM_KALENDER_COLUMNS],
            }

            for table_name, columns in expected.items():
                actual_columns = [
                    row[0]
                    for row in connection.execute(
                        f"DESCRIBE SELECT * FROM {table_name}"
                    ).fetchall()
                ]
                self.assertEqual(actual_columns, columns)
        finally:
            connection.close()

    def test_kostenbelege_notebook_query_executes_against_generated_tables(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(f"CREATE TEMP TABLE kbkp AS {kbkp_2019_select(0, 80)}")
            connection.execute(f"CREATE TEMP TABLE kbpo AS {kbpo_2019_select(0, 160, 80)}")
            connection.execute(f"CREATE TEMP TABLE kbhp AS {kbhp_2019_select(0, 160, 80)}")
            connection.execute(f"CREATE TEMP TABLE kale AS {dim_kalender_select(0, 61)}")

            sql = _build_kostenbelege_3_1_sql(
                kbkp_relation="kbkp",
                kbpo_relation="kbpo",
                kbhp_relation="kbhp",
                kalender_relation="kale",
            )
            query_sql = sql.rstrip().rstrip(";")
            columns = [
                row[0]
                for row in connection.execute(f"DESCRIBE {sql}").fetchall()
            ]

            self.assertIn("Belegnummer", columns)
            self.assertIn("BetragHauptbuch", columns)
            self.assertIn("TechnischesDatum", columns)
            self.assertGreater(
                connection.execute(f"SELECT COUNT(*) FROM ({query_sql}) q").fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_optimized_kostenbelege_query_matches_original_semantics(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(f"CREATE TEMP TABLE kbkp AS {kbkp_2019_select(0, 80)}")
            connection.execute(f"CREATE TEMP TABLE kbpo AS {kbpo_2019_select(0, 160, 80)}")
            connection.execute(f"CREATE TEMP TABLE kbhp AS {kbhp_2019_select(0, 160, 80)}")
            connection.execute(f"CREATE TEMP TABLE kale AS {dim_kalender_select(0, 61)}")

            original_sql = _build_kostenbelege_3_1_sql(
                kbkp_relation="kbkp",
                kbpo_relation="kbpo",
                kbhp_relation="kbhp",
                kalender_relation="kale",
            ).rstrip().rstrip(";")
            optimized_sql = _build_kostenbelege_3_1_optimized_sql(
                kbkp_relation="kbkp",
                kbpo_relation="kbpo",
                kbhp_relation="kbhp",
                kalender_relation="kale",
            ).rstrip().rstrip(";")

            symmetric_difference_count = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    (SELECT * FROM ({original_sql}) original_query
                     EXCEPT ALL
                     SELECT * FROM ({optimized_sql}) optimized_query)
                    UNION ALL
                    (SELECT * FROM ({optimized_sql}) optimized_query
                     EXCEPT ALL
                     SELECT * FROM ({original_sql}) original_query)
                ) differences
                """
            ).fetchone()[0]
            optimized_plan = "\n".join(
                str(row[1] if len(row) > 1 else row[0])
                for row in connection.execute(f"EXPLAIN {optimized_sql}").fetchall()
            )

            self.assertEqual(symmetric_difference_count, 0)
            self.assertIn("HASH_JOIN", optimized_plan)
            self.assertNotIn("BLOCKWISE_NL_JOIN", optimized_plan)
        finally:
            connection.close()

    def test_cleanup_drops_postgres_tables_s3_views_and_bucket(self) -> None:
        connection = FakeConnection()
        context = SimpleNamespace(
            settings=object(),
            connect=lambda: connection,
            report=lambda **_kwargs: None,
        )
        job = SimpleNamespace(
            target_name="kostenbelege_3_1",
            target_path="s3://loader-bucket/generated/kostenbelege_3_1",
        )

        with (
            patch(
                "bit_data_workbench.data_generator.kostenbelege_3_1.delete_s3_bucket",
                return_value=16,
            ) as delete_bucket,
            patch(
                "bit_data_workbench.data_generator.kostenbelege_3_1.remove_s3_bucket",
                return_value=True,
            ) as remove_bucket,
        ):
            result = GENERATOR.cleanup(context, job)

        self.assertTrue(connection.closed)
        self.assertEqual(delete_bucket.call_count, 1)
        self.assertEqual(remove_bucket.call_count, 1)
        self.assertEqual(
            sum("DROP TABLE IF EXISTS" in sql for sql in connection.executed_sql),
            8,
        )
        self.assertEqual(
            sum("DROP VIEW IF EXISTS" in sql for sql in connection.executed_sql),
            4,
        )
        self.assertTrue(
            any(KOSTENBELEGE_3_1_S3_SCHEMA in sql for sql in connection.executed_sql)
        )
        self.assertIn("Dropped 8 PostgreSQL table(s)", result.message)
        self.assertIn("removed 4 S3 view(s)", result.message)


if __name__ == "__main__":
    unittest.main()

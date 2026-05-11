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


from bit_data_workbench.data_generator.mwa_abrechnung import (  # noqa: E402
    GENERATOR,
    MWA_ABRECHNUNG_COLUMNS,
    MWA_ABRECHNUNG_TABLE,
    MWA_TABLE_NAMES,
    MWA_ZIFFERN_COLUMNS,
    MWA_ZIFFERN_TABLE,
    PARQUET_COPY_OPTIONS,
    mwa_abrechnung_entities_select,
    mwa_abrechnungs_ziffern_entities_select,
)
from bit_data_workbench.data_generator.registry import DataGeneratorRegistry  # noqa: E402
from bit_data_workbench.backend.notebook_presets import (  # noqa: E402
    _build_mwa_abrechnung_performance_sql,
)


class FakeConnection:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.closed = False

    def execute(self, sql: str):
        self.executed_sql.append(sql)
        return self

    def close(self) -> None:
        self.closed = True


class MwaAbrechnungLoaderTests(unittest.TestCase):
    def test_registry_discovers_mwa_abrechnung_loader(self) -> None:
        generator = DataGeneratorRegistry().generator("mwa_abrechnung_multi_format_loader")
        payload = generator.definition().payload

        self.assertEqual(payload["generatorId"], "mwa_abrechnung_multi_format_loader")
        self.assertEqual(payload["targetKind"], "contest")
        self.assertEqual(payload["title"], "MWA Abrechnung Multi-Format Loader (3.2)")
        self.assertEqual(
            payload["treePath"],
            ["PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"],
        )
        self.assertIn("parquet", payload["tags"])
        self.assertIn("json", payload["tags"])

    def test_row_counts_use_one_to_three_parent_child_split(self) -> None:
        row_counts = GENERATOR._row_counts(40)

        self.assertEqual(row_counts[MWA_ABRECHNUNG_TABLE], 10)
        self.assertEqual(row_counts[MWA_ZIFFERN_TABLE], 30)

    def test_written_targets_include_postgres_and_each_s3_format_for_each_table(self) -> None:
        targets = GENERATOR._written_targets(
            root_prefix="s3://loader-bucket/generated/mwa_abrechnung",
            table_names=MWA_TABLE_NAMES,
        )

        self.assertEqual(len(targets), 8)
        self.assertEqual(
            {target.location for target in targets},
            {
                "pg_oltp.public.mwa_abrechnung_entities",
                "pg_oltp.public.mwa_abrechnungs_ziffern_entities",
                "s3://loader-bucket/generated/mwa_abrechnung/parquet/mwa_abrechnung_entities",
                "s3://loader-bucket/generated/mwa_abrechnung/csv/mwa_abrechnung_entities",
                "s3://loader-bucket/generated/mwa_abrechnung/json/mwa_abrechnung_entities",
                "s3://loader-bucket/generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities",
                "s3://loader-bucket/generated/mwa_abrechnung/csv/mwa_abrechnungs_ziffern_entities",
                "s3://loader-bucket/generated/mwa_abrechnung/json/mwa_abrechnungs_ziffern_entities",
            },
        )

    def test_generated_selects_match_schema_and_child_rows_reference_parents(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(
                f"CREATE TEMP TABLE ab AS {mwa_abrechnung_entities_select(0, 4)}"
            )
            connection.execute(
                "CREATE TEMP TABLE z AS "
                f"{mwa_abrechnungs_ziffern_entities_select(0, 12, 4)}"
            )

            abrechnung_columns = [
                row[0]
                for row in connection.execute("DESCRIBE SELECT * FROM ab").fetchall()
            ]
            ziffern_columns = [
                row[0]
                for row in connection.execute("DESCRIBE SELECT * FROM z").fetchall()
            ]

            self.assertEqual(
                abrechnung_columns,
                [column.split(" ", 1)[0] for column in MWA_ABRECHNUNG_COLUMNS],
            )
            self.assertEqual(
                ziffern_columns,
                [column.split(" ", 1)[0] for column in MWA_ZIFFERN_COLUMNS],
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*), MIN(id_), MAX(id_) FROM ab").fetchone(),
                (4, 1, 4),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*), MIN(abrechnung_refer), MAX(abrechnung_refer) FROM z"
                ).fetchone(),
                (12, 1, 4),
            )
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM z
                    LEFT JOIN ab ON ab.id_ = z.abrechnung_refer
                    WHERE ab.id_ IS NULL
                    """
                ).fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_parquet_copy_options_request_zstd_and_fixed_row_groups(self) -> None:
        self.assertIn("COMPRESSION ZSTD", PARQUET_COPY_OPTIONS)
        self.assertIn("ROW_GROUP_SIZE 100000", PARQUET_COPY_OPTIONS)

    def test_mwa_notebook_query_executes_against_generated_tables(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(
                f"CREATE TEMP TABLE ab AS {mwa_abrechnung_entities_select(0, 80)}"
            )
            connection.execute(
                "CREATE TEMP TABLE z AS "
                f"{mwa_abrechnungs_ziffern_entities_select(0, 240, 80)}"
            )

            sql = _build_mwa_abrechnung_performance_sql(
                abrechnung_relation="ab",
                ziffern_relation="z",
            )
            columns = [
                row[0]
                for row in connection.execute(f"DESCRIBE {sql}").fetchall()
            ]

            self.assertIn("abrechnung_count", columns)
            self.assertIn("steuer_total", columns)
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
            target_name="mwa_abrechnung",
            target_path="s3://loader-bucket/generated/mwa_abrechnung",
        )

        with (
            patch(
                "bit_data_workbench.data_generator.mwa_abrechnung.delete_s3_bucket",
                return_value=12,
            ) as delete_bucket,
            patch(
                "bit_data_workbench.data_generator.mwa_abrechnung.remove_s3_bucket",
                return_value=True,
            ) as remove_bucket,
        ):
            result = GENERATOR.cleanup(context, job)

        self.assertTrue(connection.closed)
        self.assertEqual(delete_bucket.call_count, 1)
        self.assertEqual(remove_bucket.call_count, 1)
        self.assertEqual(
            sum("DROP TABLE IF EXISTS" in sql for sql in connection.executed_sql),
            2,
        )
        self.assertEqual(
            sum("DROP VIEW IF EXISTS" in sql for sql in connection.executed_sql),
            6,
        )
        self.assertIn("Dropped 2 PostgreSQL table(s)", result.message)
        self.assertIn("removed 6 S3 view(s)", result.message)


if __name__ == "__main__":
    unittest.main()

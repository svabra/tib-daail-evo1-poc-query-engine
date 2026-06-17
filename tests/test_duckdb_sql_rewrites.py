from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.duckdb_sql_rewrites import (  # noqa: E402
    rewrite_parquet_select_star_unions,
)


class DuckDBSqlRewriteTests(unittest.TestCase):
    def test_rewrites_select_star_parquet_union_all_chain_to_union_by_name_scan(self) -> None:
        paths = [
            "s3://KBPOimports/KBPO_2018undvorher.parquet",
            "s3://KBPOimports/KBPO_2019.parquet",
            "s3://KBPOimports/KBPO2020.parquet",
            "s3://KBPOimports/KBPO2021.parquet",
            "s3://KBPOimports/KBPO2022.parquet",
            "s3://KBPOimports/KBPO2023.parquet",
            "s3://KBPOimports/KBPO2024.parquet",
            "s3://KBPOimports/KBPO2025.parquet",
        ]
        sql = "\nUNION ALL\n".join(
            f"SELECT * FROM read_parquet('{path}')"
            for path in paths
        )

        rewritten = rewrite_parquet_select_star_unions(sql)

        self.assertIn("SELECT * FROM read_parquet([", rewritten)
        self.assertIn("union_by_name = true", rewritten)
        self.assertNotIn("UNION ALL", rewritten)
        for path in paths:
            self.assertIn(f"'{path}'", rewritten)

    def test_does_not_rewrite_projection_filter_generic_union_or_non_parquet(self) -> None:
        samples = [
            (
                "SELECT id FROM read_parquet('s3://bucket/a.parquet') "
                "UNION ALL SELECT id FROM read_parquet('s3://bucket/b.parquet')"
            ),
            (
                "SELECT * FROM read_parquet('s3://bucket/a.parquet') WHERE id > 0 "
                "UNION ALL SELECT * FROM read_parquet('s3://bucket/b.parquet') WHERE id > 0"
            ),
            (
                "SELECT * FROM read_parquet('s3://bucket/a.parquet') "
                "UNION SELECT * FROM read_parquet('s3://bucket/b.parquet')"
            ),
            (
                "SELECT * FROM read_csv_auto('s3://bucket/a.csv') "
                "UNION ALL SELECT * FROM read_csv_auto('s3://bucket/b.csv')"
            ),
        ]

        for sql in samples:
            self.assertEqual(rewrite_parquet_select_star_unions(sql), sql)

    def test_does_not_rewrite_commented_sql_text(self) -> None:
        sql = (
            "-- SELECT * FROM read_parquet('s3://bucket/a.parquet') "
            "UNION ALL SELECT * FROM read_parquet('s3://bucket/b.parquet')\n"
            "SELECT 1 AS value"
        )

        self.assertEqual(rewrite_parquet_select_star_unions(sql), sql)


if __name__ == "__main__":
    unittest.main()

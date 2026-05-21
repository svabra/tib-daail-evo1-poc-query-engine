from __future__ import annotations

from pathlib import Path
import sys
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.data_generator.parquet_performance_options import (  # noqa: E402
    GENERATORS,
    federal_tax_dataset_select,
)
from bit_data_workbench.data_generator.registry import DataGeneratorRegistry  # noqa: E402


class ParquetPerformanceOptionsLoaderTests(unittest.TestCase):
    def test_registry_discovers_each_performance_option_loader(self) -> None:
        generator_ids = {
            generator.generator_id
            for generator in DataGeneratorRegistry().discover()
            if generator.generator_id.startswith("parquet_performance_options_")
        }

        self.assertEqual(
            generator_ids,
            {
                "parquet_performance_options_off_loader",
                "parquet_performance_options_recommended_loader",
                "parquet_performance_options_manual_partition_no_hive_loader",
                "parquet_performance_options_manual_partition_hive_loader",
                "parquet_performance_options_manual_cache_only_loader",
            },
        )

    def test_loader_definitions_live_under_performance_options_folder(self) -> None:
        definitions = [generator.definition().payload for generator in GENERATORS]

        self.assertEqual(len(definitions), 5)
        for definition in definitions:
            self.assertEqual(definition["treePath"], ["PoC Tests", "Performance Options"])
            self.assertEqual(definition["targetKind"], "s3")
            self.assertEqual(definition["defaultSizeGb"], 1.0)
            self.assertIn("federal-tax", definition["tags"])

    def test_loader_bucket_names_are_unique_per_layout(self) -> None:
        bucket_names = [generator._loader_bucket_name("vat-smoke-test") for generator in GENERATORS]

        self.assertEqual(len(bucket_names), len(set(bucket_names)))
        self.assertTrue(all(name.startswith("poc-tests-performance-options-") for name in bucket_names))

    def test_federal_tax_dataset_contains_multiple_partition_years(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            rows = connection.execute(
                f"""
                SELECT tax_year, COUNT(*) AS row_count
                FROM ({federal_tax_dataset_select(0, 30)}) AS federal_tax
                GROUP BY tax_year
                ORDER BY tax_year
                """
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(rows, [(2024, 10), (2025, 10), (2026, 10)])


if __name__ == "__main__":
    unittest.main()

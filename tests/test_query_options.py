from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.query_options import (  # noqa: E402
    cache_hydration_enabled,
    cache_hydration_options,
    normalize_query_options,
    parquet_hive_partitioning_option,
    source_existence_validation_enabled,
)


class QueryOptionsTests(unittest.TestCase):
    def test_defaults_to_auto_hive_partitioning(self) -> None:
        options = normalize_query_options(None)

        self.assertEqual(options["duckdb"]["parquetHivePartitioning"], "auto")
        self.assertEqual(parquet_hive_partitioning_option(options), "auto")
        self.assertEqual(
            options["duckdb"]["cacheHydration"],
            {
                "mode": "off",
                "scope": "referencedS3Parquet",
                "indexPolicy": "autoPredicates",
            },
        )
        self.assertEqual(options["validation"]["sourceExistence"], "off")
        self.assertFalse(cache_hydration_enabled(options))
        self.assertFalse(source_existence_validation_enabled(options))

    def test_accepts_on_and_off_values(self) -> None:
        self.assertEqual(
            normalize_query_options(
                {"duckdb": {"parquetHivePartitioning": "ON"}}
            )["duckdb"]["parquetHivePartitioning"],
            "on",
        )
        self.assertEqual(
            normalize_query_options(
                {"duckdb": {"parquetHivePartitioning": "off"}}
            )["duckdb"]["parquetHivePartitioning"],
            "off",
        )

    def test_rejects_invalid_hive_partitioning_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "auto, on, off"):
            normalize_query_options(
                {"duckdb": {"parquetHivePartitioning": "sometimes"}}
            )

    def test_accepts_cache_hydration_enabled_option(self) -> None:
        options = normalize_query_options(
            {
                "duckdb": {
                    "cacheHydration": {
                        "mode": "ON",
                        "scope": "referencedS3Parquet",
                        "indexPolicy": "autoPredicates",
                    }
                }
            }
        )

        self.assertTrue(cache_hydration_enabled(options))
        self.assertEqual(cache_hydration_options(options)["mode"], "on")

    def test_accepts_source_existence_validation_options(self) -> None:
        off_options = normalize_query_options(
            {
                "validation": {
                    "sourceExistence": "OFF",
                }
            }
        )
        on_options = normalize_query_options(
            {
                "validation": {
                    "sourceExistence": "ON",
                }
            }
        )

        self.assertEqual(off_options["validation"]["sourceExistence"], "off")
        self.assertFalse(source_existence_validation_enabled(off_options))
        self.assertEqual(on_options["validation"]["sourceExistence"], "on")
        self.assertTrue(source_existence_validation_enabled(on_options))

    def test_rejects_invalid_cache_hydration_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "cacheHydration.mode"):
            normalize_query_options({"duckdb": {"cacheHydration": {"mode": "always"}}})
        with self.assertRaisesRegex(ValueError, "cacheHydration.scope"):
            normalize_query_options(
                {
                    "duckdb": {
                        "cacheHydration": {
                            "scope": "allS3Parquet",
                        }
                    }
                }
            )
        with self.assertRaisesRegex(ValueError, "cacheHydration.indexPolicy"):
            normalize_query_options(
                {
                    "duckdb": {
                        "cacheHydration": {
                            "indexPolicy": "allColumns",
                        }
                    }
                }
            )

    def test_rejects_invalid_source_existence_validation_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "validation.sourceExistence"):
            normalize_query_options({"validation": {"sourceExistence": "sometimes"}})


if __name__ == "__main__":
    unittest.main()

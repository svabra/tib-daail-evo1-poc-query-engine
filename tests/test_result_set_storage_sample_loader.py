from __future__ import annotations

from pathlib import Path
import sys
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.data_generator.registry import DataGeneratorRegistry  # noqa: E402
from bit_data_workbench.data_generator.result_set_storage_sample import (  # noqa: E402
    GENERATOR,
    RESULT_SET_STORAGE_ORDER_COLUMNS,
    RESULT_SET_STORAGE_SAMPLE_BUCKET,
    RESULT_SET_STORAGE_SAMPLE_RESULT_PATH,
    RESULT_SET_STORAGE_SAMPLE_SOURCE_NAME,
    RESULT_SET_STORAGE_SAMPLE_TREE_PATH,
    result_set_storage_orders_select,
)


class ResultSetStorageSampleLoaderTests(unittest.TestCase):
    def test_registry_discovers_result_set_storage_loader(self) -> None:
        generator = DataGeneratorRegistry().generator("result_set_storage_s3_loader")
        payload = generator.definition().payload

        self.assertEqual(payload["generatorId"], "result_set_storage_s3_loader")
        self.assertEqual(payload["targetKind"], "s3")
        self.assertEqual(payload["title"], "Result Set Storage S3 Loader")
        self.assertEqual(payload["treePath"], list(RESULT_SET_STORAGE_SAMPLE_TREE_PATH))
        self.assertIn("result-storage", payload["tags"])
        self.assertIn("parquet", payload["tags"])

    def test_loader_bucket_name_matches_notebook_folder(self) -> None:
        self.assertEqual(
            GENERATOR._loader_bucket_name("configured-bucket"),
            "poc-tests-general-functionalities-result-set-storage",
        )
        self.assertEqual(
            GENERATOR._loader_bucket_name("configured-bucket"),
            RESULT_SET_STORAGE_SAMPLE_BUCKET,
        )

    def test_written_targets_include_source_and_notebook_result_path(self) -> None:
        source_prefix = f"s3://{RESULT_SET_STORAGE_SAMPLE_BUCKET}/generated/{RESULT_SET_STORAGE_SAMPLE_SOURCE_NAME}"
        targets = GENERATOR._written_targets(
            source_prefix=source_prefix,
            result_path=RESULT_SET_STORAGE_SAMPLE_RESULT_PATH,
        )

        self.assertEqual(
            [target.payload for target in targets],
            [
                {
                    "targetKind": "s3_prefix",
                    "label": "Source S3 Parquet path",
                    "location": source_prefix,
                    "status": "pending",
                },
                {
                    "targetKind": "s3_object",
                    "label": "Notebook result-set S3 path",
                    "location": RESULT_SET_STORAGE_SAMPLE_RESULT_PATH,
                    "status": "pending",
                },
            ],
        )

    def test_generated_select_matches_declared_schema(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(
                f"CREATE TEMP TABLE result_set_storage_orders AS {result_set_storage_orders_select(0, 25)}"
            )
            actual_columns = [
                row[0]
                for row in connection.execute(
                    "DESCRIBE SELECT * FROM result_set_storage_orders"
                ).fetchall()
            ]
            self.assertEqual(
                actual_columns,
                [column.split(" ", 1)[0] for column in RESULT_SET_STORAGE_ORDER_COLUMNS],
            )

            rows = connection.execute(
                """
                SELECT
                  canton_code,
                  COUNT(*) AS order_count,
                  CAST(ROUND(SUM(gross_amount_chf), 2) AS DECIMAL(18,2)) AS gross_total_chf
                FROM result_set_storage_orders
                GROUP BY canton_code
                ORDER BY canton_code
                """
            ).fetchall()
        finally:
            connection.close()

        self.assertGreater(len(rows), 1)
        self.assertTrue(all(row[1] > 0 for row in rows))
        self.assertTrue(all(row[2] > 0 for row in rows))


if __name__ == "__main__":
    unittest.main()

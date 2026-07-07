from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.data_generator.helpers import loader_tree_bucket_name  # noqa: E402
from bit_data_workbench.data_generator.kostenbelege_3_1 import (  # noqa: E402
    GENERATOR as KOSTENBELEGE_GENERATOR,
)
from bit_data_workbench.data_generator.mwa_abrechnung import (  # noqa: E402
    GENERATOR as MWA_GENERATOR,
)
from bit_data_workbench.data_generator.pg_union_sql_functionality_s3 import (  # noqa: E402
    GENERATOR as UNION_S3_GENERATOR,
)
from bit_data_workbench.data_generator.pg_vs_s3_contest import (  # noqa: E402
    GENERATOR as SINGLE_TABLE_GENERATOR,
)
from bit_data_workbench.data_generator.pg_vs_s3_multi_table import (  # noqa: E402
    GENERATOR as MULTI_TABLE_GENERATOR,
)
from bit_data_workbench.data_generator.result_set_storage_sample import (  # noqa: E402
    GENERATOR as RESULT_SET_STORAGE_GENERATOR,
)
from bit_data_workbench.data_generator.s3_smoke import (  # noqa: E402
    GENERATOR as S3_SMOKE_GENERATOR,
)


class LoaderS3BucketNameTests(unittest.TestCase):
    def test_smoke_loader_keeps_configured_smoke_bucket_lineage(self) -> None:
        self.assertEqual(
            S3_SMOKE_GENERATOR._loader_bucket_name("vat-smoke-test"),
            "vat-smoke-test-s3-smoke",
        )

    def test_performance_loaders_use_notebook_structure_bucket_names(self) -> None:
        base_bucket = "vat-smoke-test"

        expected_names = {
            SINGLE_TABLE_GENERATOR._loader_bucket_name(base_bucket):
                "poc-tests-performance-evaluation-single-table-test",
            MULTI_TABLE_GENERATOR._loader_bucket_name(base_bucket):
                "poc-tests-performance-evaluation-multi-table-test",
            KOSTENBELEGE_GENERATOR._loader_bucket_name(base_bucket):
                "poc-tests-performance-evaluation-kostenbelege-3-1",
            MWA_GENERATOR._loader_bucket_name(base_bucket):
                "poc-tests-performance-evaluation-mwa-abrechnung-3-2",
        }

        for actual_name, expected_name in expected_names.items():
            self.assertEqual(actual_name, expected_name)
            self.assertFalse(actual_name.startswith(base_bucket))

    def test_sql_functionality_s3_loader_uses_notebook_structure_bucket_name(self) -> None:
        bucket_name = UNION_S3_GENERATOR._loader_bucket_name("vat-smoke-test")

        self.assertEqual(bucket_name, "poc-tests-sql-functionalities")
        self.assertFalse(bucket_name.startswith("vat-smoke-test"))

    def test_result_set_storage_loader_uses_notebook_structure_bucket_name(self) -> None:
        bucket_name = RESULT_SET_STORAGE_GENERATOR._loader_bucket_name("vat-smoke-test")

        self.assertEqual(bucket_name, "poc-tests-general-functionalities-result-set-storage")
        self.assertFalse(bucket_name.startswith("vat-smoke-test"))

    def test_tree_bucket_name_is_s3_safe_and_truncated_with_hash_when_needed(self) -> None:
        bucket_name = loader_tree_bucket_name(
            (
                "PoC Tests",
                "Performance Evaluation",
                "This folder name is intentionally far too long for a raw S3 bucket name",
            ),
            "fallback",
        )

        self.assertLessEqual(len(bucket_name), 63)
        self.assertRegex(bucket_name, r"^[a-z0-9][a-z0-9-]+[a-z0-9]$")


if __name__ == "__main__":
    unittest.main()

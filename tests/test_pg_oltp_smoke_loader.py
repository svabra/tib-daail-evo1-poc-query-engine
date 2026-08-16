from __future__ import annotations

from pathlib import Path
import sys
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.data_generator.helpers import (  # noqa: E402
    VAT_SMOKE_DATASET_COLUMNS,
    vat_smoke_dataset_select,
)
from bit_data_workbench.data_generator.pg_oltp_smoke import GENERATOR  # noqa: E402


class SchemaCursor:
    def __init__(self, columns: tuple[str, ...]) -> None:
        self.columns = columns
        self.parameters = None

    def execute(self, _sql: str, parameters=None):
        self.parameters = parameters
        return self

    def fetchall(self):
        return [(column,) for column in self.columns]


class PostgresOltpSmokeRegressionTests(unittest.TestCase):
    def test_synthetic_vat_turnover_has_stable_top_five_canton_ranking(self) -> None:
        dataset_sql = vat_smoke_dataset_select(0, 1_200)
        connection = duckdb.connect(":memory:")
        try:
            ranking = connection.execute(
                "SELECT canton_code "
                f"FROM ({dataset_sql}) AS vat "
                "WHERE tax_period_end >= DATE '2025-01-01' "
                "GROUP BY canton_code "
                "ORDER BY SUM(net_vat_due_chf) DESC"
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(
            [canton_code for (canton_code,) in ranking[:5]],
            ["ZH", "VD", "AG", "LU", "FR"],
        )

    def test_loader_uses_unique_relation_and_requires_the_full_vat_schema(self) -> None:
        expected_columns = tuple(
            definition.split(" ", 1)[0]
            for definition in VAT_SMOKE_DATASET_COLUMNS
        )
        self.assertEqual(GENERATOR.default_target_name, "vat_filing_smoke_generated")

        ready_connection = SchemaCursor(expected_columns)
        GENERATOR._assert_expected_schema(
            ready_connection,
            table_name=GENERATOR.default_target_name,
        )
        self.assertEqual(
            ready_connection.parameters,
            [GENERATOR.default_target_name],
        )

        legacy_connection = SchemaCursor(("canton_code", "category"))
        with self.assertRaisesRegex(RuntimeError, "readiness failed"):
            GENERATOR._assert_expected_schema(
                legacy_connection,
                table_name=GENERATOR.default_target_name,
            )


if __name__ == "__main__":
    unittest.main()

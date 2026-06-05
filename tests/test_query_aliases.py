from __future__ import annotations

from pathlib import Path
import re
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.query_aliases import (  # noqa: E402
    local_query_alias,
    rewrite_query_aliases,
    s3_query_alias,
    unique_query_aliases,
)


class QueryAliasTests(unittest.TestCase):
    def test_local_alias_generation_uses_folder_stem_and_extension(self) -> None:
        self.assertEqual(
            local_query_alias(folder_path="", file_name="Federal Tax Data 10MB.csv"),
            "local.federal_tax_data_10mb.csv",
        )
        self.assertEqual(
            local_query_alias(
                folder_path="Tax Files/Test Sub",
                file_name="Federal-Tax.Data 1000MB.csv",
            ),
            "local.tax_files.test_sub.federal_tax_data_1000mb.csv",
        )

    def test_s3_alias_generation_uses_bucket_prefix_stem_and_extension(self) -> None:
        self.assertEqual(
            s3_query_alias(bucket="test", key="federal_tax_data_10gb.csv"),
            "s3.test.federal_tax_data_10gb.csv",
        )
        self.assertEqual(
            s3_query_alias(bucket="PoC Tests", key="tax/year 2026/federal-tax.jsonl"),
            "s3.poc_tests.tax.year_2026.federal_tax.jsonl",
        )

    def test_s3_alias_generation_collapses_collection_leaf_duplicate(self) -> None:
        self.assertEqual(
            s3_query_alias(
                bucket="poc-tests-performance-evaluation-mwa-abrechnung-3-2",
                key="generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet",
                display_name="mwa_abrechnung_entities.parquet",
            ),
            (
                "s3.poc_tests_performance_evaluation_mwa_abrechnung_3_2."
                "generated.mwa_abrechnung.parquet.mwa_abrechnung_entities.parquet"
            ),
        )
        self.assertEqual(
            s3_query_alias(
                bucket="tax-bucket",
                key="federal/manual_hive/**/*.parquet",
                display_name="manual_hive.parquet",
            ),
            "s3.tax_bucket.federal.manual_hive.parquet",
        )

    def test_s3_alias_generation_keeps_literal_object_leafs_unchanged(self) -> None:
        self.assertEqual(
            s3_query_alias(
                bucket="test",
                key="foo/foo.csv",
                display_name="foo.csv",
            ),
            "s3.test.foo.foo.csv",
        )

    def test_alias_collisions_append_stable_suffix_to_stem(self) -> None:
        aliases = unique_query_aliases(
            [
                ("entry-one", "local.test.federal_tax.csv"),
                ("entry-two", "local.test.federal_tax.csv"),
            ]
        )

        self.assertNotEqual(aliases["entry-one"], aliases["entry-two"])
        self.assertRegex(aliases["entry-one"], r"^local\.test\.federal_tax_[0-9a-f]{8}\.csv$")
        self.assertRegex(aliases["entry-two"], r"^local\.test\.federal_tax_[0-9a-f]{8}\.csv$")

    def test_alias_rewrite_skips_comments_strings_and_quoted_identifiers(self) -> None:
        sql = """
        select 's3.test.federal_tax_data_10gb.csv' as literal
        -- from s3.test.federal_tax_data_10gb.csv
        from s3.test.federal_tax_data_10gb.csv
        join local.test.federal_tax_data_1000mb.csv on true
        join "s3.test.quoted.csv" on true
        """

        rewritten = rewrite_query_aliases(
            sql,
            {
                "s3.test.federal_tax_data_10gb.csv": "test.federal_tax_data_10gb",
                "local.test.federal_tax_data_1000mb.csv": "workspace_local.entry_1",
                "s3.test.quoted.csv": "test.quoted",
            },
        )

        self.assertIn("'s3.test.federal_tax_data_10gb.csv'", rewritten)
        self.assertIn("-- from s3.test.federal_tax_data_10gb.csv", rewritten)
        self.assertIn('"s3.test.quoted.csv"', rewritten)
        self.assertIn("from test.federal_tax_data_10gb", rewritten)
        self.assertIn("join workspace_local.entry_1", rewritten)
        self.assertNotRegex(
            re.sub(r"--.*", "", rewritten),
            r"\bfrom\s+s3\.test\.federal_tax_data_10gb\.csv\b",
        )

    def test_alias_rewrite_supports_hyphenated_legacy_local_workspace_ids(self) -> None:
        sql = "select * from workspace.local.saved_results.local-workspace-csv-mp4h33ie-omkue1"

        self.assertEqual(
            rewrite_query_aliases(
                sql,
                {
                    "workspace.local.saved_results.local-workspace-csv-mp4h33ie-omkue1":
                        "workspace_local_browser.entry_local_workspace_csv_mp4h33ie_omkue1"
                },
            ),
            "select * from workspace_local_browser.entry_local_workspace_csv_mp4h33ie_omkue1",
        )


if __name__ == "__main__":
    unittest.main()

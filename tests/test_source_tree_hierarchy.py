from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema  # noqa: E402
from bit_data_workbench.web.source_tree import build_source_tree_s3_hierarchy  # noqa: E402


class SourceTreeHierarchyTests(unittest.TestCase):
    def test_exact_s3_file_keeps_exact_leaf_reference(self) -> None:
        source_object = SourceObject(
            name="kbkp_2019",
            kind="file",
            relation="workspace.shared.kbkp_2019",
            display_name="kbkp_2019.parquet",
            query_reference='s3."shared-finance"."exports/kbkp_2019.parquet"',
            s3_bucket="shared-finance",
            s3_key="exports/kbkp_2019.parquet",
            s3_path="s3://shared-finance/exports/kbkp_2019.parquet",
            s3_file_format="parquet",
            s3_downloadable=True,
        )

        hierarchy = build_source_tree_s3_hierarchy(
            [_workspace_catalog([source_object])]
        )

        exports = hierarchy["workspace::shared-finance"][0]
        self.assertEqual(exports["label"], "exports")
        leaf = exports["children"][0]
        self.assertEqual(leaf["kind"], "object")
        self.assertEqual(leaf["label"], "kbkp_2019.parquet")
        self.assertIs(leaf["source_object"], source_object)

    def test_generated_parquet_dataset_uses_logical_folder_leaf(self) -> None:
        source_object = SourceObject(
            name="kbkp_2019",
            kind="file",
            relation="workspace.shared.kbkp_2019",
            display_name="kbkp_2019.parquet",
            query_reference=(
                's3."shared-finance".'
                '"generated/kostenbelege_3_1/parquet/kbkp_2019/*.parquet"'
            ),
            s3_bucket="shared-finance",
            s3_key="",
            s3_path=(
                "s3://shared-finance/"
                "generated/kostenbelege_3_1/parquet/kbkp_2019/*.parquet"
            ),
            s3_file_format="parquet",
            s3_download_kind="generated_parts",
            s3_part_prefix="generated/kostenbelege_3_1/parquet/kbkp_2019/",
            s3_part_file_format="parquet",
            s3_part_count=2,
            s3_zip_downloadable=True,
        )

        hierarchy = build_source_tree_s3_hierarchy(
            [_workspace_catalog([source_object])]
        )

        generated = hierarchy["workspace::shared-finance"][0]
        kostenbelege = generated["children"][0]
        parquet = kostenbelege["children"][0]
        leaf = parquet["children"][0]
        self.assertEqual(
            [generated["label"], kostenbelege["label"], parquet["label"]],
            ["generated", "kostenbelege_3_1", "parquet"],
        )
        self.assertEqual(leaf["kind"], "object")
        self.assertEqual(leaf["label"], "kbkp_2019")
        self.assertIn("*.parquet", leaf["searchable"])
        self.assertIs(leaf["source_object"], source_object)

    def test_duplicate_file_names_under_different_prefixes_stay_separate(self) -> None:
        first = SourceObject(
            name="first",
            kind="file",
            relation="workspace.shared.first",
            display_name="kbkp_2019.parquet",
            query_reference='s3."shared-finance"."first/kbkp_2019.parquet"',
            s3_bucket="shared-finance",
            s3_key="first/kbkp_2019.parquet",
        )
        second = SourceObject(
            name="second",
            kind="file",
            relation="workspace.shared.second",
            display_name="kbkp_2019.parquet",
            query_reference='s3."shared-finance"."second/kbkp_2019.parquet"',
            s3_bucket="shared-finance",
            s3_key="second/kbkp_2019.parquet",
        )

        hierarchy = build_source_tree_s3_hierarchy(
            [_workspace_catalog([second, first])]
        )

        self.assertEqual(
            [(node["label"], node["children"][0]["source_object"].name) for node in hierarchy["workspace::shared-finance"]],
            [("first", "first"), ("second", "second")],
        )


def _workspace_catalog(objects: list[SourceObject]) -> SourceCatalog:
    return SourceCatalog(
        name="workspace",
        connection_source_id="s3",
        schemas=[
            SourceSchema(
                name="shared-finance",
                label="shared-finance",
                objects=objects,
            )
        ],
    )


if __name__ == "__main__":
    unittest.main()

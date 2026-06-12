from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]


class S3SourceTreeFrontendTests(unittest.TestCase):
    def test_source_navigation_opens_nested_s3_folder_ancestors(self) -> None:
        app_source = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "static" / "js" / "app.js"
        ).read_text(encoding="utf-8")

        self.assertIn('ancestor.hasAttribute("data-source-s3-folder")', app_source)
        self.assertIn("while (ancestor instanceof Element)", app_source)

    def test_s3_source_tree_uses_compact_hierarchy_indentation(self) -> None:
        css_source = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "static" / "css" / "app.css"
        ).read_text(encoding="utf-8")

        self.assertIn(
            '.source-catalog[data-source-catalog-source-id="workspace.s3"] > .source-schema,\n'
            ".source-s3-folder {\n  margin-left: 8px;",
            css_source,
        )
        self.assertIn(
            '.source-catalog[data-source-catalog-source-id="workspace.s3"] .source-object-list {\n'
            "  margin-left: 15px;",
            css_source,
        )
        self.assertIn(
            ".s3-explorer-node.tree-folder > .tree-children {\n"
            "  margin-left: 7px;\n  padding-left: 5px;",
            css_source,
        )
        self.assertIn(".source-schema {\n  margin-left: 16px;", css_source)
        self.assertIn("margin: 2px 0 8px 30px;", css_source)

    def test_s3_browser_rows_preserve_generated_dataset_metadata(self) -> None:
        explorer_source = (
            REPO_ROOT
            / "bdw"
            / "bit_data_workbench"
            / "static"
            / "js"
            / "data-source-explorers"
            / "s3-explorer.js"
        ).read_text(encoding="utf-8")

        self.assertIn('"data-s3-download-kind": entry.s3DownloadKind', explorer_source)
        self.assertIn('"data-s3-part-prefix": entry.s3PartPrefix', explorer_source)
        self.assertIn("download-generated-merged", explorer_source)
        self.assertIn("download-generated-zip", explorer_source)
        self.assertIn("entryFlag(entry.s3Downloadable, true)", explorer_source)


if __name__ == "__main__":
    unittest.main()

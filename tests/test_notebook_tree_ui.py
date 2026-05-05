from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
STATIC_JS_ROOT = REPO_ROOT / "bdw" / "bit_data_workbench" / "static" / "js"


class NotebookTreeUiRegressionTests(unittest.TestCase):
    def test_unassigned_folder_delete_preserves_notebooks_at_tree_root(self) -> None:
        source = (STATIC_JS_ROOT / "notebook-tree-ui.js").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            "const isDeletingUnassignedFolder = isUnassignedFolder(folder);",
            source,
        )
        self.assertIn("targetContainer = notebookTreeRoot();", source)
        self.assertNotIn(
            "if (isUnassignedFolder(folder) && notebooks.length > 0) {\n"
            "      return;\n"
            "    }",
            source,
        )

    def test_unassigned_folder_delete_copy_matches_preserve_target(self) -> None:
        controller_source = (
            STATIC_JS_ROOT / "notebook-tree-controller.js"
        ).read_text(encoding="utf-8")
        ui_source = (STATIC_JS_ROOT / "notebook-tree-ui.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("isDeletingUnassignedFolder", controller_source)
        self.assertIn("notebook tree root", controller_source)
        self.assertIn("notebook tree root", ui_source)


if __name__ == "__main__":
    unittest.main()

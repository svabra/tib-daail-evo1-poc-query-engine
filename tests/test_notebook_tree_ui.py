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

    def test_folder_visibility_is_persisted_and_restored_from_server_state(self) -> None:
        ui_source = (STATIC_JS_ROOT / "notebook-tree-ui.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("data-toggle-folder-shared", ui_source)
        self.assertIn("folder.dataset.folderShared", ui_source)
        self.assertIn("isShared: node.dataset.folderShared === \"true\"", ui_source)
        self.assertIn("collectServerFolderMetadata", ui_source)
        self.assertIn("serverPolicy.isShared", ui_source)

    def test_tree_state_exposes_folder_metadata_helper(self) -> None:
        state_source = (STATIC_JS_ROOT / "notebook-tree-state.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("function findStoredFolderMetadata", state_source)
        self.assertIn("isShared: folder.isShared === true", state_source)
        self.assertIn("findStoredFolderMetadata", state_source)

    def test_unassigned_folder_is_renamed_and_legacy_name_is_migrated(self) -> None:
        app_source = (STATIC_JS_ROOT / "app.js").read_text(encoding="utf-8")
        ui_source = (STATIC_JS_ROOT / "notebook-tree-ui.js").read_text(
            encoding="utf-8"
        )

        self.assertIn('const unassignedFolderName = "Unassigned Notebooks";', app_source)
        self.assertIn('label === "Unassigned"', ui_source)
        self.assertIn("label.textContent = unassignedFolderName", ui_source)

    def test_query_explain_analyze_controls_and_download_jobs_are_wired(self) -> None:
        app_source = (STATIC_JS_ROOT / "app.js").read_text(encoding="utf-8")
        query_ui_source = (STATIC_JS_ROOT / "query-ui.js").read_text(encoding="utf-8")
        workspace_markup = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "workspace.html"
        ).read_text(encoding="utf-8")

        self.assertIn("data-explain-query", workspace_markup)
        self.assertIn("data-analyze-query", workspace_markup)
        self.assertIn("/api/query-jobs/explain", app_source)
        self.assertIn("/api/query-jobs/analyze", app_source)
        self.assertIn("/api/s3/generated/zip-jobs", app_source)
        self.assertIn("workloadType === \"analyze\"", query_ui_source)
        self.assertIn("Download:", app_source)


if __name__ == "__main__":
    unittest.main()

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

    def test_folder_delete_removes_shared_folder_metadata(self) -> None:
        controller_source = (
            STATIC_JS_ROOT / "notebook-tree-controller.js"
        ).read_text(encoding="utf-8")
        app_source = (STATIC_JS_ROOT / "app.js").read_text(encoding="utf-8")

        self.assertIn("deleteSharedNotebookFolder", controller_source)
        self.assertIn("await deleteSharedNotebookFolder?.(folder);", controller_source)
        self.assertIn('method: "DELETE"', app_source)
        self.assertIn('"/api/notebooks/shared/folders"', app_source)

    def test_tree_state_exposes_folder_metadata_helper(self) -> None:
        state_source = (STATIC_JS_ROOT / "notebook-tree-state.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("function findStoredFolderMetadata", state_source)
        self.assertIn("isShared: folder.isShared === true", state_source)
        self.assertIn("findStoredFolderMetadata", state_source)

    def test_tree_state_places_pipeline_seeds_in_data_pipelines_folder(self) -> None:
        state_source = (STATIC_JS_ROOT / "notebook-tree-state.js").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            'notebookId: "mwa-abrechnung-s3-parquet-pipeline"',
            state_source,
        )
        self.assertIn(
            'notebookId: "kostenbelege-3-1-s3-parquet-pipeline"',
            state_source,
        )
        self.assertIn(
            'folderPath: ["PoC Tests", "Performance Evaluation", "Data Pipelines"]',
            state_source,
        )

    def test_tree_state_places_result_storage_sample_in_general_functionalities(self) -> None:
        state_source = (STATIC_JS_ROOT / "notebook-tree-state.js").read_text(
            encoding="utf-8"
        )

        self.assertIn('notebookId: "result-set-storage-s3-demo"', state_source)
        self.assertIn('notebookId: "kostenbelege-fact-builder-s3-demo"', state_source)
        self.assertIn(
            'notebookId: "kostenbelege-fact-builder-s3-pipeline-demo"',
            state_source,
        )
        self.assertIn(
            'folderPath: ["PoC Tests", "General Functionalities"]',
            state_source,
        )

    def test_drop_target_resolves_folder_summary_before_parent_container(self) -> None:
        ui_source = (STATIC_JS_ROOT / "notebook-tree-ui.js").read_text(
            encoding="utf-8"
        )

        summary_index = ui_source.index('const summary = target.closest("summary");')
        container_index = ui_source.index(
            'const explicitContainer = target.closest("[data-tree-children]");'
        )

        self.assertLess(summary_index, container_index)
        self.assertIn(
            'summaryFolder.matches("[data-tree-folder]")',
            ui_source,
        )
        self.assertIn(
            "return directChildrenContainer(summaryFolder);",
            ui_source,
        )


if __name__ == "__main__":
    unittest.main()

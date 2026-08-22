from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
STATIC_JS_ROOT = REPO_ROOT / "bdw" / "bit_data_workbench" / "static" / "js"


class NotebookCreationPerformanceUiTests(unittest.TestCase):
    def test_query_source_new_notebook_opens_before_field_loading(self) -> None:
        source = (STATIC_JS_ROOT / "source-query-actions.js").read_text(
            encoding="utf-8"
        )
        start = source.index("async function querySourceInNewNotebook")
        end = source.index("  return {", start)
        function_source = source[start:end]

        self.assertIn(
            "const nextCell = createSourceQueryCellState(sourceDescriptor, []);",
            function_source,
        )
        self.assertIn("const notebookId = await createNotebook", function_source)
        self.assertIn("enrichSourceQueryNotebook(", function_source)
        self.assertNotIn("await loadFieldsForSourceQuery", function_source)
        self.assertIn('await refreshSidebar("notebook", {', function_source)
        self.assertIn("force: true", function_source)
        self.assertIn("forceNotebookTree: true", function_source)

    def test_forced_notebook_tree_load_overrides_hidden_sidebar_deferment(self) -> None:
        source = (STATIC_JS_ROOT / "sidebar-refresh-controller.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("forceNotebookTree = false", source)
        self.assertIn(
            'forceNotebookTree || sidebarState.notebookSectionOpen ? "full" : "deferred"',
            source,
        )

    def test_notebook_workspace_render_avoids_full_sidebar_metadata_sweep(self) -> None:
        source = (STATIC_JS_ROOT / "app.js").read_text(encoding="utf-8")
        start = source.index("function renderLocalNotebookWorkspace")
        end = source.index("function defaultNotebookCreateTarget", start)
        function_source = source[start:end]

        self.assertIn("applyWorkspaceMetadata(metaRoot, metadata);", function_source)
        self.assertNotIn("applyNotebookMetadata();", function_source)


if __name__ == "__main__":
    unittest.main()

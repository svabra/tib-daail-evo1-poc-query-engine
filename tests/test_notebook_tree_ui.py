from __future__ import annotations

from pathlib import Path
import subprocess
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

    def test_tree_state_places_non_pipeline_samples_in_general_functionalities(self) -> None:
        state_source = (STATIC_JS_ROOT / "notebook-tree-state.js").read_text(
            encoding="utf-8"
        )

        self.assertIn('notebookId: "result-set-storage-s3-demo"', state_source)
        self.assertIn('notebookId: "kostenbelege-fact-builder-s3-demo"', state_source)
        self.assertIn(
            'folderPath: ["PoC Tests", "General Functionalities"]',
            state_source,
        )

    def test_tree_state_migrates_python_demos_to_direct_jupyter_python_folder(self) -> None:
        module_uri = (STATIC_JS_ROOT / "notebook-tree-state.js").resolve().as_uri()
        script = f"""
          import assert from 'node:assert/strict';
          const {{ createNotebookTreeState }} = await import({module_uri!r});
          const storage = new Map();
          globalThis.window = {{
            localStorage: {{
              getItem: key => storage.get(key) ?? null,
              setItem: (key, value) => storage.set(key, value),
            }},
          }};
          const oldTree = [{{
            type: 'folder',
            name: 'PoC Tests',
            folderId: 'poc-tests',
            children: [{{
              type: 'folder',
              name: 'General Functionalities',
              folderId: 'poc-tests-general-functionalities',
              children: [
                {{ type: 'notebook', notebookId: 'python-pandas-vat-demo' }},
                {{ type: 'notebook', notebookId: 'result-set-storage-s3-demo' }},
                {{ type: 'notebook', notebookId: 'python-chart-vat-demo' }},
              ],
            }}, {{
              type: 'folder',
              name: 'Jupyter/Python',
              folderId: 'poc-tests-jupyter/python',
              children: [
                {{ type: 'notebook', notebookId: 'python-chart-vat-demo' }},
              ],
            }}],
          }}];
          storage.set('test-tree', JSON.stringify(oldTree));
          const treeState = createNotebookTreeState({{
            deleteStoredNotebookState: () => {{}},
            isLocalNotebookId: () => false,
            notebookTreeStorageKey: 'test-tree',
          }});
          const migrated = treeState.readStoredNotebookTree();
          const poc = migrated.find(node => node.type === 'folder' && node.name === 'PoC Tests');
          const jupyter = poc.children.find(
            node => node.type === 'folder' && node.name === 'Jupyter/Python'
          );
          const general = poc.children.find(
            node => node.type === 'folder' && node.name === 'General Functionalities'
          );
          assert.deepEqual(
            jupyter.children.map(node => node.notebookId).sort(),
            ['python-chart-vat-demo', 'python-pandas-vat-demo']
          );
          assert.deepEqual(
            general.children.map(node => node.notebookId),
            ['result-set-storage-s3-demo']
          );
          const ids = [];
          const visit = nodes => nodes.forEach(node => {{
            if (node.type === 'notebook') ids.push(node.notebookId);
            if (Array.isArray(node.children)) visit(node.children);
          }});
          visit(migrated);
          assert.equal(ids.length, 3);
          assert.equal(new Set(ids).size, 3);
        """
        subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            cwd=REPO_ROOT,
        )

    def test_tree_state_migrates_fact_builder_pipeline_to_direct_data_pipelines_folder(self) -> None:
        module_uri = (STATIC_JS_ROOT / "notebook-tree-state.js").resolve().as_uri()
        script = f"""
          import assert from 'node:assert/strict';
          const {{ createNotebookTreeState }} = await import({module_uri!r});
          const storage = new Map();
          globalThis.window = {{
            localStorage: {{
              getItem: key => storage.get(key) ?? null,
              setItem: (key, value) => storage.set(key, value),
            }},
          }};
          const oldTree = [{{
            type: 'folder',
            name: 'PoC Tests',
            folderId: 'poc-tests',
            children: [{{
              type: 'folder',
              name: 'General Functionalities',
              folderId: 'poc-tests-general-functionalities',
              children: [
                {{ type: 'notebook', notebookId: 'kostenbelege-fact-builder-s3-demo' }},
                {{ type: 'notebook', notebookId: 'kostenbelege-fact-builder-s3-pipeline-demo' }},
              ],
            }}, {{
              type: 'folder',
              name: 'Data Pipelines',
              folderId: 'poc-tests-data-pipelines',
              children: [
                {{ type: 'notebook', notebookId: 'kostenbelege-fact-builder-s3-pipeline-demo' }},
              ],
            }}],
          }}];
          storage.set('test-tree', JSON.stringify(oldTree));
          const treeState = createNotebookTreeState({{
            deleteStoredNotebookState: () => {{}},
            isLocalNotebookId: () => false,
            notebookTreeStorageKey: 'test-tree',
          }});
          const migrated = treeState.readStoredNotebookTree();
          const poc = migrated.find(node => node.type === 'folder' && node.name === 'PoC Tests');
          const pipelines = poc.children.find(
            node => node.type === 'folder' && node.name === 'Data Pipelines'
          );
          const general = poc.children.find(
            node => node.type === 'folder' && node.name === 'General Functionalities'
          );
          assert.deepEqual(
            pipelines.children.map(node => node.notebookId),
            ['kostenbelege-fact-builder-s3-pipeline-demo']
          );
          assert.deepEqual(
            general.children.map(node => node.notebookId),
            ['kostenbelege-fact-builder-s3-demo']
          );
          const ids = [];
          const visit = nodes => nodes.forEach(node => {{
            if (node.type === 'notebook') ids.push(node.notebookId);
            if (Array.isArray(node.children)) visit(node.children);
          }});
          visit(migrated);
          assert.deepEqual(ids.sort(), [
            'kostenbelege-fact-builder-s3-demo',
            'kostenbelege-fact-builder-s3-pipeline-demo',
          ]);
        """
        subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            cwd=REPO_ROOT,
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

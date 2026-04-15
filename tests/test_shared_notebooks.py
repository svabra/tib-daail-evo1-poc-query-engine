from __future__ import annotations

from pathlib import Path
import sys
import threading
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_shared_notebook_components():
    from bit_data_workbench.backend.service import WorkbenchService
    from bit_data_workbench.models import (
        NotebookCellDefinition,
        NotebookDefinition,
    )

    return WorkbenchService, NotebookCellDefinition, NotebookDefinition


class InMemorySharedNotebookStore:
    def __init__(self, notebooks=None):
        self._notebooks = {
            notebook.notebook_id: notebook for notebook in (notebooks or [])
        }

    def list_notebooks(self):
        return list(self._notebooks.values())

    def upsert_notebook(self, notebook):
        action = (
            "updated"
            if notebook.notebook_id in self._notebooks
            else "created"
        )
        self._notebooks[notebook.notebook_id] = notebook
        return notebook, action

    def delete_notebook(self, notebook_id):
        return self._notebooks.pop(notebook_id)


def build_shared_notebook_service(existing_notebooks=None):
    WorkbenchService, _, _ = import_shared_notebook_components()
    service = WorkbenchService.__new__(WorkbenchService)
    service._condition = threading.Condition()
    service._shared_notebook_store = InMemorySharedNotebookStore(
        existing_notebooks
    )
    rebuild_calls: list[str] = []
    appended_events: list[dict[str, object]] = []
    service._rebuild_notebooks_locked = lambda: rebuild_calls.append("rebuild")
    service._append_notebook_event_locked = (
        lambda **kwargs: appended_events.append(kwargs)
    )
    return service, rebuild_calls, appended_events


class SharedNotebookServiceTests(unittest.TestCase):
    def test_upsert_shared_notebook_normalizes_defaults_and_emits_event(
        self,
    ) -> None:
        service, rebuild_calls, appended_events = (
            build_shared_notebook_service()
        )

        result = service.upsert_shared_notebook(
            notebook_id=" ",
            title=" ",
            summary=" ",
            tags=[" analysis ", "", "vat"],
            tree_path=["", " "],
            linked_generator_id=" loader-a ",
            created_at="2026-04-14T10:00:00+00:00",
            cells=[
                {
                    "sql": "select 1",
                    "dataSources": [
                        " pg_oltp.public.tax_assessment ",
                        "",
                        "workspace.s3.vat_smoke",
                    ],
                },
                "ignored",
            ],
            versions=[
                {
                    "title": "",
                    "summary": "",
                    "tags": [" saved ", ""],
                    "cells": [
                        {
                            "sql": "select 2",
                            "dataSources": [" workspace.s3.vat_smoke ", ""],
                        }
                    ],
                }
            ],
            origin_client_id="client-1",
        )

        notebook = result["notebook"]

        self.assertEqual(result["action"], "created")
        self.assertTrue(notebook["notebookId"].startswith("shared-notebook-"))
        self.assertEqual(notebook["title"], "Untitled Notebook")
        self.assertEqual(notebook["summary"], "Describe this notebook.")
        self.assertEqual(notebook["treePath"], ["Shared Notebooks"])
        self.assertEqual(notebook["tags"], ["analysis", "vat"])
        self.assertEqual(notebook["linkedGeneratorId"], "loader-a")
        self.assertEqual(notebook["createdAt"], "2026-04-14T10:00:00+00:00")
        self.assertEqual(
            notebook["cells"][0]["dataSources"],
            ["pg_oltp.public.tax_assessment", "workspace.s3.vat_smoke"],
        )
        self.assertTrue(
            notebook["cells"][0]["cellId"].startswith("shared-cell-")
        )
        self.assertEqual(notebook["versions"][0]["title"], "Untitled Notebook")
        self.assertEqual(
            notebook["versions"][0]["summary"],
            "Describe this notebook.",
        )
        self.assertEqual(notebook["versions"][0]["tags"], ["saved"])
        self.assertEqual(
            notebook["versions"][0]["cells"][0]["dataSources"],
            ["workspace.s3.vat_smoke"],
        )
        self.assertTrue(
            notebook["versions"][0]["versionId"].startswith("shared-version-")
        )
        self.assertTrue(
            notebook["versions"][0]["cells"][0]["cellId"].startswith(
                "shared-cell-"
            )
        )
        self.assertEqual(rebuild_calls, ["rebuild"])
        self.assertEqual(len(appended_events), 1)
        self.assertEqual(appended_events[0]["event_type"], "created")
        self.assertEqual(appended_events[0]["origin_client_id"], "client-1")

    def test_upsert_reuses_existing_tree_path_and_delete_emits_deleted_event(
        self,
    ) -> None:
        _, notebook_cell_type, notebook_type = (
            import_shared_notebook_components()
        )
        existing_notebook = notebook_type(
            notebook_id="shared-notebook-a",
            title="Original Title",
            summary="Original Summary",
            cells=[notebook_cell_type(cell_id="cell-a", sql="select 1")],
            tree_path=("Pinned", "Team A"),
            shared=True,
            created_at="2025-01-01T00:00:00+00:00",
        )
        service, rebuild_calls, appended_events = (
            build_shared_notebook_service([existing_notebook])
        )

        updated = service.upsert_shared_notebook(
            notebook_id="shared-notebook-a",
            title=" Updated Notebook ",
            summary=" Updated Summary ",
            tags=[" shared ", ""],
            tree_path=[],
            linked_generator_id="",
            cells=[],
            versions=[],
            origin_client_id="client-2",
        )

        notebook = updated["notebook"]

        self.assertEqual(updated["action"], "updated")
        self.assertEqual(notebook["notebookId"], "shared-notebook-a")
        self.assertEqual(notebook["title"], "Updated Notebook")
        self.assertEqual(notebook["summary"], "Updated Summary")
        self.assertEqual(notebook["treePath"], ["Pinned", "Team A"])
        self.assertEqual(notebook["createdAt"], "2025-01-01T00:00:00+00:00")
        self.assertEqual(notebook["tags"], ["shared"])
        self.assertEqual(len(notebook["cells"]), 1)
        self.assertTrue(
            notebook["cells"][0]["cellId"].startswith("shared-cell-")
        )
        self.assertEqual(
            notebook["versions"][0]["versionId"],
            "initial-shared-notebook-a",
        )

        deleted = service.delete_shared_notebook(
            "shared-notebook-a",
            origin_client_id="client-2",
        )

        self.assertEqual(deleted["action"], "deleted")
        self.assertEqual(
            deleted["notebook"]["notebookId"],
            "shared-notebook-a",
        )
        self.assertEqual(rebuild_calls, ["rebuild", "rebuild"])
        self.assertEqual(
            [event["event_type"] for event in appended_events],
            ["updated", "deleted"],
        )
        self.assertEqual(appended_events[1]["origin_client_id"], "client-2")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import sys
import unittest

from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.models import (  # noqa: E402
    NotebookCellDefinition,
    NotebookDefinition,
)
from bit_data_workbench.version_info import current_repo_version  # noqa: E402
from bit_data_workbench.web.router import notebook_workspace  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


class FakeWorkbenchService:
    def __init__(self) -> None:
        self._notebook = NotebookDefinition(
            notebook_id="revenue-analysis",
            title="Revenue Analysis",
            summary="Notebook title test.",
            cells=[
                NotebookCellDefinition(
                    cell_id="cell-1",
                    sql="SELECT 1;",
                    processing_hints="Inspect the smoke query.",
                    result_expectations="Returns one row.",
                )
            ],
        )

    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": CURRENT_VERSION,
            "hostname": "test-host",
            "pod_name": "unknown",
            "pod_namespace": "unknown",
            "pod_ip": "unknown",
            "node_name": "unknown",
            "duckdb_database": "/tmp/workspace.duckdb",
            "timestamp_utc": "2026-05-19T00:00:00+00:00",
        }

    def catalogs(self):
        return []

    def notebooks(self):
        return [self._notebook]

    def notebook(self, notebook_id: str) -> NotebookDefinition:
        if notebook_id != self._notebook.notebook_id:
            raise KeyError(notebook_id)
        return self._notebook

    def notebook_tree(self):
        return []

    def source_options(self):
        return []

    def data_generators(self):
        return []

    def runbook_tree(self):
        return []

    def completion_schema(self):
        return {}


def build_request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"host", b"testserver")],
        }
    )


class BrowserTitleTests(unittest.TestCase):
    def test_notebook_route_uses_notebook_title_as_browser_title(self) -> None:
        response = notebook_workspace(
            notebook_id="revenue-analysis",
            request=build_request("/notebooks/revenue-analysis"),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("<title>DAAIF Factory - Revenue Analysis</title>", body)
        self.assertIn("<h1>DAAIF Factory - Query Workbench</h1>", body)

    def test_notebook_editor_renders_expand_control_next_to_copy_sql(self) -> None:
        response = notebook_workspace(
            notebook_id="revenue-analysis",
            request=build_request("/notebooks/revenue-analysis"),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("data-copy-editor-sql", body)
        self.assertIn("data-expand-editor", body)
        self.assertIn("data-compare-editor-sql", body)
        self.assertIn('aria-label="Expand SQL editor"', body)
        self.assertIn('aria-label="Compare"', body)
        self.assertIn('aria-pressed="false"', body)
        self.assertIn("Cell processing hints", body)
        self.assertIn("Cell result expectations", body)
        self.assertIn('data-cell-descriptor="processingHints"', body)
        self.assertIn('data-cell-descriptor="resultExpectations"', body)
        self.assertIn("Inspect the smoke query.", body)
        self.assertIn("Returns one row.", body)
        self.assertIn("data-share-notebook", body)
        self.assertIn("Share Notebook ...", body)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.models import PythonJobOutputDefinition  # noqa: E402
from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema  # noqa: E402
from bit_data_workbench.backend.python_execution.output_parser import (  # noqa: E402
    mime_bundle_to_output,
)
from bit_data_workbench.backend.python_execution.kernel_runtime import (  # noqa: E402
    PythonKernelRuntime,
)
from bit_data_workbench.backend.python_execution.python_jobs import (  # noqa: E402
    PythonJobManager,
)
from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402


class FakeKernelSession:
    def __init__(self) -> None:
        self.lock = threading.Lock()


class FakeKernelSessions:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []
        self.restarts: list[tuple[str, str]] = []
        self.interrupts: list[tuple[str, str]] = []
        self._session = FakeKernelSession()

    def get_session(self, *, client_id: str, notebook_id: str):
        self.requests.append((client_id, notebook_id))
        return self._session

    def execute(self, session, *, code: str, context: dict[str, object], is_cancelled):
        if is_cancelled():
            return []
        return [
            PythonJobOutputDefinition(
                output_type="text",
                text=f"executed:{code}",
            )
        ]

    def interrupt_session(self, *, client_id: str, notebook_id: str) -> None:
        self.interrupts.append((client_id, notebook_id))

    def restart_session(self, *, client_id: str, notebook_id: str) -> None:
        self.restarts.append((client_id, notebook_id))


def wait_for_terminal_job(manager: PythonJobManager, job_id: str, timeout_seconds: float = 2.0) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        snapshot = manager.state_payload()
        job = next((item for item in snapshot["jobs"] if item["jobId"] == job_id), None)
        if job and job["status"] in {"completed", "failed", "cancelled"}:
            return job
        time.sleep(0.02)
    raise AssertionError(f"Timed out waiting for Python job {job_id} to reach a terminal state.")


class PythonOutputParserTests(unittest.TestCase):
    def test_mime_bundle_to_output_sanitizes_html_and_detects_tables(self) -> None:
        html_output = mime_bundle_to_output(
            {
                "text/html": "<div>safe<script>alert(1)</script></div>",
                "text/plain": "safe",
            }
        )
        table_output = mime_bundle_to_output(
            {
                "text/html": "<table><tr><td>1</td></tr></table>",
                "text/plain": "1",
            }
        )

        self.assertIsNotNone(html_output)
        self.assertEqual(html_output.output_type, "html")
        self.assertIn("safe", html_output.html)
        self.assertNotIn("script", html_output.html.lower())

        self.assertIsNotNone(table_output)
        self.assertEqual(table_output.output_type, "table")

    def test_mime_bundle_to_output_preserves_json_payloads(self) -> None:
        payload = {"rows": [1, 2, 3], "ok": True}
        output = mime_bundle_to_output({"application/json": payload})

        self.assertIsNotNone(output)
        self.assertEqual(output.output_type, "json")
        self.assertEqual(output.data, payload)


class PythonJobManagerTests(unittest.TestCase):
    def test_python_job_manager_scopes_requests_and_hides_old_outputs_for_same_cell(self) -> None:
        fake_sessions = FakeKernelSessions()
        refresh_calls: list[str] = []
        state_updates: list[dict[str, object]] = []
        manager = PythonJobManager(
            kernel_sessions=fake_sessions,
            metadata_refresher=lambda: refresh_calls.append("refresh"),
            state_change_callback=lambda snapshot: state_updates.append(snapshot),
        )

        first = manager.start_job(
            client_id="client-a",
            code="print('first')",
            notebook_id="notebook-1",
            notebook_title="Notebook 1",
            cell_id="cell-1",
            data_sources=["pg_oltp"],
            source_context={"selectedSources": [], "relations": [], "localRelationMap": {}},
        )
        second = manager.start_job(
            client_id="client-a",
            code="print('second')",
            notebook_id="notebook-1",
            notebook_title="Notebook 1",
            cell_id="cell-1",
            data_sources=["pg_oltp"],
            source_context={"selectedSources": [], "relations": [], "localRelationMap": {}},
        )

        first_job = wait_for_terminal_job(manager, first.job_id)
        second_job = wait_for_terminal_job(manager, second.job_id)
        payload = manager.state_payload()

        self.assertEqual(first_job["status"], "completed")
        self.assertEqual(second_job["status"], "completed")
        self.assertEqual(fake_sessions.requests, [("client-a", "notebook-1"), ("client-a", "notebook-1")])
        self.assertGreaterEqual(len(refresh_calls), 2)
        self.assertTrue(state_updates)
        self.assertEqual(payload["jobs"][0]["jobId"], second.job_id)
        self.assertTrue(payload["jobs"][0]["outputs"])
        self.assertEqual(payload["jobs"][1]["jobId"], first.job_id)
        self.assertEqual(payload["jobs"][1]["outputs"], [])

    def test_restart_kernel_delegates_to_kernel_sessions(self) -> None:
        fake_sessions = FakeKernelSessions()
        manager = PythonJobManager(
            kernel_sessions=fake_sessions,
            metadata_refresher=lambda: None,
        )

        result = manager.restart_kernel(client_id="client-b", notebook_id="notebook-2")

        self.assertEqual(fake_sessions.restarts, [("client-b", "notebook-2")])
        self.assertEqual(
            result,
            {
                "ok": True,
                "notebookId": "notebook-2",
                "message": "Python kernel restarted.",
            },
        )


class PythonExecutionContextTests(unittest.TestCase):
    def test_python_execution_context_only_mounts_selected_sources(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = [
            SourceCatalog(
                name="pg_oltp",
                connection_source_id="pg_oltp",
                schemas=[
                    SourceSchema(
                        name="public",
                        objects=[
                            SourceObject(
                                name="orders",
                                kind="table",
                                relation="pg_oltp.public.orders",
                            )
                        ],
                    )
                ],
            ),
            SourceCatalog(
                name="workspace",
                connection_source_id="workspace.s3",
                schemas=[
                    SourceSchema(
                        name="s3",
                        objects=[
                            SourceObject(
                                name="vat_smoke",
                                kind="view",
                                relation="workspace.s3.vat_smoke_generated",
                            )
                        ],
                    )
                ],
            ),
        ]
        service._source_options = [
            {"source_id": "pg_oltp", "label": "PostgreSQL OLTP", "classification": "Internal", "computation_mode": "VMTP"},
            {"source_id": "workspace.s3", "label": "Workspace S3", "classification": "Internal", "computation_mode": "MPP"},
        ]
        service.source_object_fields = lambda relation: []

        context = service._python_execution_context(
            data_sources=["pg_oltp"],
            local_relation_map={},
        )

        self.assertEqual(
            [item["sourceId"] for item in context["selectedSources"]],
            ["pg_oltp"],
        )
        self.assertEqual(
            [item["sourceId"] for item in context["relations"]],
            ["pg_oltp"],
        )
        self.assertEqual(
            [item["relation"] for item in context["relations"]],
            ["pg_oltp.public.orders"],
        )


class PythonKernelRuntimeTests(unittest.TestCase):
    def test_kernel_runtime_uses_in_memory_connection_for_postgres_only_context(self) -> None:
        connection = object()
        context = {
            "selectedSources": [
                {
                    "sourceId": "pg_oltp",
                    "canonicalSourceId": "pg_oltp",
                }
            ],
            "relations": [],
            "localRelationMap": {},
        }

        with (
            patch(
                "bit_data_workbench.backend.python_execution.kernel_runtime.Settings.from_env",
                return_value=object(),
            ),
            patch(
                "bit_data_workbench.backend.python_execution.kernel_runtime.create_duckdb_worker_connection",
                return_value=connection,
            ) as create_connection,
            patch.object(PythonKernelRuntime, "_import_pandas", return_value=object()),
        ):
            runtime = PythonKernelRuntime()
            runtime.configure(context)

        self.assertIs(runtime._connection, connection)
        self.assertEqual(
            create_connection.call_args.kwargs,
            {"database_path": ":memory:"},
        )

    def test_kernel_runtime_raises_clear_error_for_workspace_lock_conflict(self) -> None:
        context = {
            "selectedSources": [
                {
                    "sourceId": "workspace.s3",
                    "canonicalSourceId": "workspace.s3",
                }
            ],
            "relations": [],
            "localRelationMap": {},
        }
        lock_error = duckdb.IOException(
            'IO Error: Could not set lock on file "/tmp/workspace/workspace.duckdb": '
            "Conflicting lock is held in /usr/local/bin/python3.12 (PID 2)."
        )

        with (
            patch(
                "bit_data_workbench.backend.python_execution.kernel_runtime.Settings.from_env",
                return_value=object(),
            ),
            patch(
                "bit_data_workbench.backend.python_execution.kernel_runtime.create_duckdb_worker_connection",
                side_effect=lock_error,
            ),
            patch.object(PythonKernelRuntime, "_import_pandas", return_value=object()),
        ):
            runtime = PythonKernelRuntime()
            with self.assertRaises(RuntimeError) as error_context:
                runtime.configure(context)

        self.assertIn("Shared Workspace or Local Workspace sources", str(error_context.exception))
        self.assertIn("locked", str(error_context.exception))


if __name__ == "__main__":
    unittest.main()

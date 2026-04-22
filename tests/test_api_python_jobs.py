from __future__ import annotations

from pathlib import Path
import json
import sys
import unittest

from fastapi import HTTPException


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import (  # noqa: E402
    python_jobs_state,
    restart_python_kernel,
    start_python_job,
)


class FakePythonJobService:
    def __init__(self) -> None:
        self.python_job_calls: list[dict[str, object]] = []
        self.restart_calls: list[dict[str, str]] = []

    def python_jobs_state(self) -> dict[str, object]:
        return {
            "version": 3,
            "summary": {"runningCount": 1, "totalCount": 2},
            "jobs": [],
        }

    def start_python_job(
        self,
        *,
        client_id: str,
        code: str,
        notebook_id: str,
        notebook_title: str,
        cell_id: str,
        data_sources: list[str] | None = None,
        local_relation_map: dict[str, str] | None = None,
    ) -> dict[str, object]:
        call = {
            "client_id": client_id,
            "code": code,
            "notebook_id": notebook_id,
            "notebook_title": notebook_title,
            "cell_id": cell_id,
            "data_sources": list(data_sources or []),
            "local_relation_map": dict(local_relation_map or {}),
        }
        self.python_job_calls.append(call)
        return {
            "jobId": "python-job-1",
            "notebookId": notebook_id,
            "notebookTitle": notebook_title,
            "cellId": cell_id,
            "code": code,
            "language": "python",
            "status": "queued",
            "startedAt": "2026-04-22T12:00:00+00:00",
            "updatedAt": "2026-04-22T12:00:00+00:00",
            "outputs": [],
            "dataSources": list(data_sources or []),
            "sourceTypes": [],
            "backendName": "Headless Jupyter Kernel",
            "canCancel": True,
        }

    def restart_python_kernel(self, *, client_id: str, notebook_id: str) -> dict[str, object]:
        self.restart_calls.append({"client_id": client_id, "notebook_id": notebook_id})
        return {
            "ok": True,
            "notebookId": notebook_id,
            "message": "Python kernel restarted.",
        }


class ApiPythonJobRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = FakePythonJobService()

    def test_python_jobs_state_route_returns_service_payload(self) -> None:
        response = python_jobs_state(service=self.service)

        self.assertEqual(
            json.loads(response.body),
            {
                "version": 3,
                "summary": {"runningCount": 1, "totalCount": 2},
                "jobs": [],
            },
        )

    def test_start_python_job_route_accepts_local_relation_map(self) -> None:
        response = start_python_job(
            code="",
            sql="print('hello')",
            notebook_id="notebook-123",
            notebook_title="Notebook 123",
            cell_id="cell-7",
            data_sources="pg_oltp||workspace.local",
            local_relations='{"workspace.local.saved_results.local-entry":"local_workspace.client_x.local_entry"}',
            service=self.service,
            workbench_client_id="client-123",
        )

        self.assertEqual(json.loads(response.body)["jobId"], "python-job-1")
        self.assertEqual(
            self.service.python_job_calls,
            [
                {
                    "client_id": "client-123",
                    "code": "print('hello')",
                    "notebook_id": "notebook-123",
                    "notebook_title": "Notebook 123",
                    "cell_id": "cell-7",
                    "data_sources": ["pg_oltp", "workspace.local"],
                    "local_relation_map": {
                        "workspace.local.saved_results.local-entry": "local_workspace.client_x.local_entry",
                    },
                }
            ],
        )

    def test_start_python_job_route_rejects_invalid_local_relation_payload(self) -> None:
        with self.assertRaises(HTTPException) as context:
            start_python_job(
                code="print('hello')",
                sql="",
                notebook_id="notebook-123",
                notebook_title="Notebook 123",
                cell_id="cell-7",
                data_sources="",
                local_relations='["invalid"]',
                service=self.service,
                workbench_client_id="client-123",
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("must be a JSON object", str(context.exception.detail))

    def test_restart_python_kernel_route_passes_client_scope(self) -> None:
        response = restart_python_kernel(
            notebook_id="notebook-777",
            service=self.service,
            workbench_client_id="client-777",
        )

        self.assertEqual(
            json.loads(response.body),
            {
                "ok": True,
                "notebookId": "notebook-777",
                "message": "Python kernel restarted.",
            },
        )
        self.assertEqual(
            self.service.restart_calls,
            [{"client_id": "client-777", "notebook_id": "notebook-777"}],
        )


if __name__ == "__main__":
    unittest.main()

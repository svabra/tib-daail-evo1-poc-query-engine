from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable

from ...models import PythonJobDefinition, PythonJobOutputDefinition
from ..query_jobs import infer_source_types
from .kernel_sessions import KernelSessionManager


RUNNING_PYTHON_JOB_STATUSES = {"queued", "running"}
TERMINAL_PYTHON_JOB_STATUSES = {"completed", "failed", "cancelled"}
MAX_PYTHON_JOB_HISTORY = 80


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class PythonJobRecord:
    snapshot: PythonJobDefinition
    client_id: str
    sort_index: int
    thread: threading.Thread | None = None
    cancel_requested: bool = False


class PythonJobManager:
    def __init__(
        self,
        *,
        kernel_sessions: KernelSessionManager,
        metadata_refresher: Callable[[], None],
        state_change_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._kernel_sessions = kernel_sessions
        self._metadata_refresher = metadata_refresher
        self._state_change_callback = state_change_callback
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._jobs: dict[str, PythonJobRecord] = {}
        self._sort_counter = 0
        self._state_version = 0

    def start_job(
        self,
        *,
        client_id: str,
        code: str,
        notebook_id: str,
        notebook_title: str,
        cell_id: str,
        data_sources: list[str] | None = None,
        source_context: dict[str, Any] | None = None,
    ) -> PythonJobDefinition:
        normalized_code = str(code or "").strip()
        if not normalized_code:
            raise ValueError("Provide Python code before running the cell.")

        normalized_client_id = str(client_id or "").strip()
        if not normalized_client_id:
            raise ValueError("Missing workbench client id for Python execution.")

        source_ids = [source_id.strip() for source_id in (data_sources or []) if source_id.strip()]
        now = utc_now_iso()
        snapshot = PythonJobDefinition(
            job_id=f"python-{uuid.uuid4().hex}",
            notebook_id=str(notebook_id or "").strip(),
            notebook_title=str(notebook_title or "").strip() or "Notebook",
            cell_id=str(cell_id or "").strip(),
            code=str(code or ""),
            language="python",
            status="queued",
            started_at=now,
            updated_at=now,
            progress_label="Queued...",
            message="Waiting to start.",
            data_sources=source_ids,
            source_types=infer_source_types(source_ids),
            backend_name="Headless Jupyter Kernel",
            can_cancel=True,
        )

        with self._condition:
            self._sort_counter += 1
            record = PythonJobRecord(
                snapshot=snapshot,
                client_id=normalized_client_id,
                sort_index=self._sort_counter,
            )
            self._jobs[snapshot.job_id] = record
            self._touch_locked()

        worker = threading.Thread(
            target=self._run_job,
            args=(snapshot.job_id, dict(source_context or {})),
            daemon=True,
            name=f"bdw-python-{snapshot.job_id[:8]}",
        )
        with self._condition:
            record.thread = worker
        worker.start()
        return snapshot

    def cancel_job(self, job_id: str) -> PythonJobDefinition:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown Python job: {job_id}")

            if record.snapshot.status in TERMINAL_PYTHON_JOB_STATUSES:
                return record.snapshot

            record.cancel_requested = True
            if record.snapshot.status == "queued":
                completed_at = utc_now_iso()
                record.snapshot.status = "cancelled"
                record.snapshot.completed_at = completed_at
                record.snapshot.updated_at = completed_at
                record.snapshot.progress_label = "Cancelled"
                record.snapshot.message = "Python execution cancelled."
                record.snapshot.can_cancel = False
                self._touch_locked()
                return record.snapshot

            record.snapshot.updated_at = utc_now_iso()
            record.snapshot.progress_label = "Cancelling..."
            record.snapshot.message = "Interrupt requested."
            self._touch_locked()
            client_id = record.client_id
            notebook_id = record.snapshot.notebook_id

        self._kernel_sessions.interrupt_session(
            client_id=client_id,
            notebook_id=notebook_id,
        )

        with self._condition:
            return self._jobs[job_id].snapshot

    def restart_kernel(self, *, client_id: str, notebook_id: str) -> dict[str, object]:
        self._kernel_sessions.restart_session(client_id=client_id, notebook_id=notebook_id)
        return {
            "ok": True,
            "notebookId": str(notebook_id or "").strip(),
            "message": "Python kernel restarted.",
        }

    def state_payload(self) -> dict[str, Any]:
        with self._condition:
            jobs = sorted(
                self._jobs.values(),
                key=lambda record: record.sort_index,
                reverse=True,
            )
            latest_cell_keys: set[tuple[str, str]] = set()
            job_payloads: list[dict[str, Any]] = []
            for record in jobs:
                payload = record.snapshot.payload
                cell_key = (record.snapshot.notebook_id, record.snapshot.cell_id)
                if cell_key in latest_cell_keys:
                    payload["outputs"] = []
                else:
                    latest_cell_keys.add(cell_key)
                job_payloads.append(payload)
            running_jobs = [
                record.snapshot
                for record in jobs
                if record.snapshot.status in RUNNING_PYTHON_JOB_STATUSES
            ]
            return {
                "version": self._state_version,
                "summary": {
                    "runningCount": len(running_jobs),
                    "totalCount": len(jobs),
                },
                "jobs": job_payloads,
            }

    def _run_job(self, job_id: str, source_context: dict[str, Any]) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status == "cancelled":
                return
            record.snapshot.progress_label = "Queued..."
            record.snapshot.message = "Waiting for the notebook kernel..."
            self._touch_locked()

        started = time.perf_counter()
        outputs: list[PythonJobOutputDefinition] = []

        try:
            record = self._jobs[job_id]
            session = self._kernel_sessions.get_session(
                client_id=record.client_id,
                notebook_id=record.snapshot.notebook_id,
            )

            while True:
                acquired = session.lock.acquire(timeout=0.1)
                if acquired:
                    break
                with self._condition:
                    live_record = self._jobs.get(job_id)
                    if live_record is None:
                        return
                    if live_record.cancel_requested:
                        self._finalize_job(
                            job_id,
                            status="cancelled",
                            duration_ms=(time.perf_counter() - started) * 1000,
                            progress_label="Cancelled",
                            message="Python execution cancelled.",
                        )
                        return

            try:
                self._patch_job(
                    job_id,
                    status="running",
                    progress_label="Running...",
                    message="Running Python cell...",
                )
                outputs = self._kernel_sessions.execute(
                    session,
                    code=record.snapshot.code,
                    context=source_context,
                    is_cancelled=lambda: self._is_cancelled(job_id),
                )
            finally:
                session.lock.release()

            duration_ms = (time.perf_counter() - started) * 1000
            with self._condition:
                record = self._jobs.get(job_id)
                cancel_requested = bool(record and record.cancel_requested)

            error_output_item = next(
                (output for output in reversed(outputs) if output.output_type == "error"),
                None,
            )
            if cancel_requested:
                self._finalize_job(
                    job_id,
                    status="cancelled",
                    duration_ms=duration_ms,
                    progress_label="Cancelled",
                    message="Python execution cancelled.",
                    outputs=outputs,
                    error=error_output_item.text if error_output_item is not None else None,
                )
                return

            if error_output_item is not None:
                self._finalize_job(
                    job_id,
                    status="failed",
                    duration_ms=duration_ms,
                    progress_label="Failed",
                    message="Python execution failed.",
                    outputs=outputs,
                    error=error_output_item.text or error_output_item.error_value,
                )
                return

            self._finalize_job(
                job_id,
                status="completed",
                duration_ms=duration_ms,
                progress_label="Completed",
                message="Python execution completed.",
                outputs=outputs,
            )
        except Exception as exc:
            self._finalize_job(
                job_id,
                status="failed",
                duration_ms=(time.perf_counter() - started) * 1000,
                progress_label="Failed",
                message="Python execution failed.",
                error=str(exc),
            )
        finally:
            try:
                self._metadata_refresher()
            except Exception:
                pass

    def _is_cancelled(self, job_id: str) -> bool:
        with self._condition:
            record = self._jobs.get(job_id)
            return bool(record and record.cancel_requested)

    def _patch_job(self, job_id: str, **changes: Any) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return

            for key, value in changes.items():
                setattr(record.snapshot, key, value)
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

    def _finalize_job(self, job_id: str, *, status: str, duration_ms: float, **changes: Any) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return

            completed_at = utc_now_iso()
            record.snapshot.status = status
            record.snapshot.duration_ms = duration_ms
            record.snapshot.completed_at = completed_at
            record.snapshot.updated_at = completed_at
            record.snapshot.can_cancel = False
            for key, value in changes.items():
                setattr(record.snapshot, key, value)
            self._prune_history_locked()
            self._touch_locked()

    def _touch_locked(self) -> None:
        self._state_version += 1
        payload = self.state_payload()
        self._condition.notify_all()
        if self._state_change_callback is not None:
            self._state_change_callback(payload)

    def _prune_history_locked(self) -> None:
        terminal_jobs = [
            record
            for record in sorted(self._jobs.values(), key=lambda item: item.sort_index)
            if record.snapshot.status in TERMINAL_PYTHON_JOB_STATUSES
        ]
        overflow = max(0, len(terminal_jobs) - MAX_PYTHON_JOB_HISTORY)
        for record in terminal_jobs[:overflow]:
            self._jobs.pop(record.snapshot.job_id, None)

from __future__ import annotations

import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from ..config import Settings
from .runtime_connections import create_duckdb_worker_connection
from .runtime_storage import delete_query_spill_directory


PURE_DUCKDB_DIRECT_EXECUTION_PATH = "direct-in-process"
PURE_DUCKDB_RUNNING_STATUSES = {"queued", "running"}
PURE_DUCKDB_TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _safe_query_spill_directory_name(job_id: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in job_id)


def _serialize_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if hasattr(value, "isoformat"):
        with suppress(Exception):
            return value.isoformat()
    return str(value)


def _serialize_row(row: Any) -> list[Any]:
    values = row if isinstance(row, (list, tuple)) else [row]
    return [_serialize_value(value) for value in values]


@dataclass(slots=True)
class PureDuckDBJobSnapshot:
    job_id: str
    cell_id: str
    sql: str
    status: str = "queued"
    message: str = "Waiting to start."
    error: str = ""
    columns: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    row_count: int = 0
    rows_shown: int = 0
    truncated: bool = False
    started_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    completed_at: str = ""
    duration_ms: float = 0.0
    timings: dict[str, float] = field(default_factory=dict)
    duckdb_execution_path: str = PURE_DUCKDB_DIRECT_EXECUTION_PATH

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "cellId": self.cell_id,
            "sql": self.sql,
            "executionSql": self.sql,
            "status": self.status,
            "message": self.message,
            "error": self.error,
            "columns": list(self.columns),
            "rows": [list(row) for row in self.rows],
            "rowCount": self.row_count,
            "rowsShown": self.rows_shown,
            "truncated": self.truncated,
            "startedAt": self.started_at,
            "updatedAt": self.updated_at,
            "completedAt": self.completed_at,
            "durationMs": self.duration_ms,
            "timings": dict(self.timings),
            "duckdbExecutionPath": self.duckdb_execution_path,
            "backendName": "Pure DuckDB",
        }


@dataclass(slots=True)
class PureDuckDBJobRecord:
    snapshot: PureDuckDBJobSnapshot
    thread: threading.Thread | None = None
    connection: duckdb.DuckDBPyConnection | None = None
    spill_temp_directory: Path | None = None


class PureDuckDBJobManager:
    def __init__(self, *, settings: Settings, max_result_rows: int) -> None:
        self._settings = settings
        self._max_result_rows = max(1, int(max_result_rows or 50))
        self._condition = threading.Condition(threading.RLock())
        self._jobs: dict[str, PureDuckDBJobRecord] = {}

    def start_job(self, *, cell_id: str, sql: str) -> PureDuckDBJobSnapshot:
        normalized_sql = str(sql or "").strip()
        if not normalized_sql:
            raise ValueError("Provide a DuckDB SQL statement before running the cell.")

        job_id = f"pure-duckdb-{uuid.uuid4().hex}"
        snapshot = PureDuckDBJobSnapshot(
            job_id=job_id,
            cell_id=str(cell_id or "").strip() or "pure-duckdb-cell",
            sql=normalized_sql,
        )
        record = PureDuckDBJobRecord(snapshot=snapshot)
        worker = threading.Thread(
            target=self._run_job,
            args=(job_id,),
            daemon=True,
            name=f"bdw-pure-duckdb-{job_id[-8:]}",
        )
        record.thread = worker
        with self._condition:
            self._jobs[job_id] = record
            self._condition.notify_all()
        worker.start()
        return snapshot

    def snapshot(self, job_id: str) -> PureDuckDBJobSnapshot:
        normalized_job_id = str(job_id or "").strip()
        with self._condition:
            record = self._jobs.get(normalized_job_id)
            if record is None:
                raise KeyError(f"Unknown Pure DuckDB job: {normalized_job_id}")
            return record.snapshot

    def wait_for_terminal(
        self,
        job_id: str,
        *,
        timeout: float | None = None,
    ) -> PureDuckDBJobSnapshot:
        normalized_job_id = str(job_id or "").strip()
        deadline = time.monotonic() + float(timeout) if timeout is not None else None
        with self._condition:
            while True:
                record = self._jobs.get(normalized_job_id)
                if record is None:
                    raise KeyError(f"Unknown Pure DuckDB job: {normalized_job_id}")
                if record.snapshot.status in PURE_DUCKDB_TERMINAL_STATUSES:
                    return record.snapshot
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(f"Timed out waiting for Pure DuckDB job {normalized_job_id}.")
                    wait_seconds = min(0.1, remaining)
                else:
                    wait_seconds = 0.1
                self._condition.wait(timeout=wait_seconds)

    def _set_running(self, record: PureDuckDBJobRecord) -> None:
        snapshot = record.snapshot
        snapshot.status = "running"
        snapshot.message = "DuckDB is executing the statement directly in the application process."
        snapshot.updated_at = _utc_now_iso()
        self._condition.notify_all()

    def _set_completed(
        self,
        record: PureDuckDBJobRecord,
        *,
        started: float,
        columns: list[str],
        rows: list[list[Any]],
        truncated: bool,
        engine_query_ms: float,
        result_fetch_ms: float,
    ) -> None:
        duration_ms = (time.perf_counter() - started) * 1000
        snapshot = record.snapshot
        snapshot.status = "completed"
        snapshot.columns = columns
        snapshot.rows = rows
        snapshot.row_count = len(rows)
        snapshot.rows_shown = len(rows)
        snapshot.truncated = truncated
        if columns:
            snapshot.message = (
                f"{self._max_result_rows} row(s) shown. The result was truncated for the UI."
                if truncated
                else f"{len(rows)} row(s) shown."
            )
        else:
            snapshot.message = "Statement executed successfully."
        snapshot.duration_ms = duration_ms
        snapshot.timings = {
            "backendTotalMs": duration_ms,
            "engineQueryMs": engine_query_ms,
            "resultFetchMs": result_fetch_ms,
            "engineAccessWaitMs": 0.0,
        }
        snapshot.completed_at = _utc_now_iso()
        snapshot.updated_at = snapshot.completed_at
        self._condition.notify_all()

    def _set_failed(self, record: PureDuckDBJobRecord, *, started: float, error: Exception) -> None:
        duration_ms = (time.perf_counter() - started) * 1000
        snapshot = record.snapshot
        snapshot.status = "failed"
        snapshot.message = "Query failed."
        snapshot.error = str(error)
        snapshot.duration_ms = duration_ms
        snapshot.timings = {
            "backendTotalMs": duration_ms,
            "engineAccessWaitMs": 0.0,
        }
        snapshot.completed_at = _utc_now_iso()
        snapshot.updated_at = snapshot.completed_at
        self._condition.notify_all()

    def _run_job(self, job_id: str) -> None:
        started = time.perf_counter()
        connection: duckdb.DuckDBPyConnection | None = None
        spill_temp_directory: Path | None = None
        with self._condition:
            record = self._jobs[job_id]
            if self._settings.duckdb_temp_directory is not None:
                spill_root = Path(self._settings.duckdb_temp_directory)
                spill_temp_directory = spill_root / _safe_query_spill_directory_name(job_id)
                spill_temp_directory.mkdir(parents=True, exist_ok=True)
                record.spill_temp_directory = spill_temp_directory
            self._set_running(record)

        try:
            connection = create_duckdb_worker_connection(
                self._settings,
                database_path=":memory:",
                read_only=False,
                temp_directory_override=spill_temp_directory,
            )
            with self._condition:
                record = self._jobs[job_id]
                record.connection = connection

            query_started = time.perf_counter()
            cursor = connection.execute(record.snapshot.sql)
            columns = [column[0] for column in cursor.description] if cursor.description else []
            engine_query_ms = (time.perf_counter() - query_started) * 1000

            rows: list[list[Any]] = []
            truncated = False
            fetch_started = time.perf_counter()
            if columns:
                while len(rows) <= self._max_result_rows:
                    batch = cursor.fetchmany(max(1, min(1000, self._max_result_rows + 1)))
                    if not batch:
                        break
                    rows.extend(_serialize_row(row) for row in batch)
                    if len(rows) > self._max_result_rows:
                        truncated = True
                        rows = rows[: self._max_result_rows]
                        break
            result_fetch_ms = (time.perf_counter() - fetch_started) * 1000 if columns else 0.0

            with self._condition:
                record = self._jobs[job_id]
                self._set_completed(
                    record,
                    started=started,
                    columns=columns,
                    rows=rows,
                    truncated=truncated,
                    engine_query_ms=engine_query_ms,
                    result_fetch_ms=result_fetch_ms,
                )
        except Exception as exc:
            with self._condition:
                record = self._jobs[job_id]
                self._set_failed(record, started=started, error=exc)
        finally:
            if connection is not None:
                with suppress(Exception):
                    connection.close()
            with self._condition:
                record = self._jobs.get(job_id)
                if record is not None:
                    record.connection = None
                    spill_temp_directory = record.spill_temp_directory
                    record.spill_temp_directory = None
            self._cleanup_query_spill_directory(spill_temp_directory)

    def _cleanup_query_spill_directory(self, spill_temp_directory: Path | None) -> None:
        if spill_temp_directory is None or self._settings.duckdb_temp_directory is None:
            return
        with suppress(Exception):
            delete_query_spill_directory(self._settings.duckdb_temp_directory, spill_temp_directory)

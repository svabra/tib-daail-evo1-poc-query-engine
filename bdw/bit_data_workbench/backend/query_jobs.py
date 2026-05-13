from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Protocol

import duckdb

from ..models import QueryJobDefinition, QueryJobMetricPoint, QueryResult


RUNNING_QUERY_STATUSES = {"queued", "running"}
TERMINAL_QUERY_STATUSES = {"completed", "failed", "cancelled"}
MAX_QUERY_HISTORY = 80
QUERY_PROGRESS_POLL_SECONDS = 0.35


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def infer_source_types(data_sources: list[str]) -> list[str]:
    source_types: list[str] = []
    for source_id in data_sources:
        normalized = source_id.strip().lower()
        if not normalized:
            continue
        if normalized.endswith("_native"):
            source_type = "postgres-native"
        elif normalized.startswith("pg_"):
            source_type = "postgres"
        elif normalized.endswith(".s3") or normalized == "workspace.s3":
            source_type = "s3"
        elif normalized.startswith("workspace"):
            source_type = "workspace"
        else:
            source_type = "unknown"
        if source_type not in source_types:
            source_types.append(source_type)
    return source_types


def percentile(values: list[float], ratio: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = ratio * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


class QueryProgressReporter(Protocol):
    backend_name: str

    def progress(self, connection: duckdb.DuckDBPyConnection) -> float | None: ...


class DuckDBQueryProgressReporter:
    backend_name = "VMTP DUCKDB"

    def progress(self, connection: duckdb.DuckDBPyConnection) -> float | None:
        try:
            progress = float(connection.query_progress())
        except Exception:
            return None

        if progress < 0:
            return None
        return max(0.0, min(progress, 1.0))


class PostgresNativeQueryProgressReporter:
    backend_name = "PostgreSQL Native"

    def progress(self, connection: Any) -> float | None:
        return None


class QueryProgressReporterRegistry:
    def __init__(self) -> None:
        self._default = DuckDBQueryProgressReporter()
        self._postgres_native = PostgresNativeQueryProgressReporter()
        self._by_source_type: dict[str, QueryProgressReporter] = {
            "postgres": self._default,
            "postgres-native": self._postgres_native,
            "s3": self._default,
            "workspace": self._default,
            "unknown": self._default,
        }

    def for_sources(self, source_types: list[str]) -> QueryProgressReporter:
        for source_type in source_types:
            reporter = self._by_source_type.get(source_type)
            if reporter is not None:
                return reporter
        return self._default


@dataclass(slots=True)
class QueryJobRecord:
    snapshot: QueryJobDefinition
    reporter: QueryProgressReporter
    sort_index: int
    connection: duckdb.DuckDBPyConnection | None = None
    cancel_requested: bool = False
    thread: threading.Thread | None = None


class QueryJobManager:
    def __init__(
        self,
        *,
        max_result_rows: int,
        connection_factory: Callable[[], duckdb.DuckDBPyConnection],
        postgres_connection_factory: Callable[[str], Any],
        notebook_title_resolver: Callable[[str], str | None],
        metadata_refresher: Callable[[], None],
        state_change_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._max_result_rows = max(1, max_result_rows)
        self._connection_factory = connection_factory
        self._postgres_connection_factory = postgres_connection_factory
        self._notebook_title_resolver = notebook_title_resolver
        self._metadata_refresher = metadata_refresher
        self._state_change_callback = state_change_callback
        self._reporters = QueryProgressReporterRegistry()
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._jobs: dict[str, QueryJobRecord] = {}
        self._sort_counter = 0
        self._state_version = 0

    def start_job(
        self,
        *,
        sql: str,
        notebook_id: str,
        notebook_title: str,
        cell_id: str,
        data_sources: list[str] | None = None,
        touched_relations: list[str] | None = None,
        touched_buckets: list[str] | None = None,
    ) -> QueryJobDefinition:
        normalized_sql = sql.strip()
        if not normalized_sql:
            raise ValueError("Provide a SQL statement before running the query.")

        source_ids = [source_id.strip() for source_id in (data_sources or []) if source_id.strip()]
        source_types = infer_source_types(source_ids)
        reporter = self._reporters.for_sources(source_types)
        now = utc_now_iso()
        resolved_title = notebook_title.strip() or self._notebook_title_resolver(notebook_id) or "Notebook"
        snapshot = QueryJobDefinition(
            job_id=f"query-{uuid.uuid4().hex}",
            notebook_id=notebook_id.strip(),
            notebook_title=resolved_title,
            cell_id=cell_id.strip(),
            sql=sql,
            status="queued",
            started_at=now,
            updated_at=now,
            progress=0.0,
            progress_label="Queued...",
            message="Waiting to start.",
            data_sources=source_ids,
            source_types=source_types,
            touched_relations=[str(value).strip() for value in (touched_relations or []) if str(value).strip()],
            touched_buckets=[str(value).strip() for value in (touched_buckets or []) if str(value).strip()],
            backend_name=reporter.backend_name,
            can_cancel=True,
        )

        with self._condition:
            self._sort_counter += 1
            record = QueryJobRecord(snapshot=snapshot, reporter=reporter, sort_index=self._sort_counter)
            self._jobs[snapshot.job_id] = record
            self._touch_locked()

        worker = threading.Thread(
            target=self._run_job,
            args=(snapshot.job_id,),
            daemon=True,
            name=f"bdw-query-{snapshot.job_id[:8]}",
        )
        with self._condition:
            record.thread = worker
        worker.start()
        return snapshot

    def cancel_job(self, job_id: str) -> QueryJobDefinition:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown query job: {job_id}")

            if record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return record.snapshot

            record.cancel_requested = True
            if record.snapshot.status == "queued":
                completed_at = utc_now_iso()
                record.snapshot.status = "cancelled"
                record.snapshot.completed_at = completed_at
                record.snapshot.updated_at = completed_at
                record.snapshot.progress = None
                record.snapshot.progress_label = "Cancelled"
                record.snapshot.message = "Query cancelled."
                record.snapshot.can_cancel = False
                self._touch_locked()
                return record.snapshot

            record.snapshot.updated_at = utc_now_iso()
            record.snapshot.progress_label = "Cancelling..."
            record.snapshot.message = "Interrupt requested."
            self._touch_locked()
            connection = record.connection

        if connection is not None:
            try:
                if hasattr(connection, "interrupt"):
                    connection.interrupt()
                elif hasattr(connection, "cancel"):
                    connection.cancel()
            except Exception:
                pass

        with self._condition:
            record = self._jobs[job_id]
            return record.snapshot

    def snapshot(self, job_id: str) -> QueryJobDefinition:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown query job: {job_id}")
            return record.snapshot

    def state_payload(self) -> dict[str, Any]:
        with self._condition:
            return self._state_payload_locked()

    def _run_job(self, job_id: str) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status == "cancelled":
                return
            record.snapshot.status = "running"
            record.snapshot.progress = None
            record.snapshot.progress_label = "Running..."
            record.snapshot.message = "Running query..."
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

        started = time.perf_counter()
        connection: duckdb.DuckDBPyConnection | None = None
        execution_result: QueryResult | None = None
        execution_error: Exception | None = None

        try:
            use_postgres_native = any(
                source_id.strip().lower() == "pg_oltp_native"
                for source_id in record.snapshot.data_sources
            )
            first_row_ms: float | None = None
            connection = (
                self._postgres_connection_factory("oltp")
                if use_postgres_native
                else self._connection_factory()
            )
            with self._condition:
                record = self._jobs.get(job_id)
                if record is None:
                    return
                record.connection = connection

            def execute_query() -> None:
                nonlocal execution_result, execution_error, first_row_ms
                try:
                    if use_postgres_native:
                        with connection.cursor() as cursor:
                            cursor.execute(record.snapshot.sql)
                            columns = [column.name for column in (cursor.description or [])]
                            self._patch_job(
                                job_id,
                                columns=columns,
                                progress_label="Fetching rows..." if columns else "Finalizing...",
                                message="Query is fetching rows..." if columns else "Statement executed successfully.",
                            )

                            rows_buffer: list[tuple[Any, ...]] = []
                            truncated = False
                            row_count = 0
                            message = "Statement executed successfully."

                            if columns:
                                batch_size = max(1, min(25, self._max_result_rows))
                                while len(rows_buffer) <= self._max_result_rows:
                                    batch = cursor.fetchmany(batch_size)
                                    if not batch:
                                        break
                                    if first_row_ms is None:
                                        first_row_ms = (time.perf_counter() - started) * 1000
                                        self._patch_job(job_id, first_row_ms=first_row_ms)
                                    rows_buffer.extend(tuple(item) for item in batch)
                                    truncated = len(rows_buffer) > self._max_result_rows
                                    visible_rows = rows_buffer[: self._max_result_rows]
                                    row_count = len(visible_rows)
                                    message = f"{row_count} row(s) shown."
                                    if truncated:
                                        message = (
                                            f"{self._max_result_rows} row(s) shown. "
                                            "The result was truncated for the UI."
                                        )
                                    self._patch_job(
                                        job_id,
                                        rows=visible_rows,
                                        row_count=row_count,
                                        rows_shown=row_count,
                                        truncated=truncated,
                                        message=message,
                                    )
                                    if truncated:
                                        break

                            execution_result = QueryResult(
                                sql=record.snapshot.sql,
                                columns=columns,
                                rows=rows_buffer[: self._max_result_rows],
                                row_count=row_count,
                                truncated=truncated,
                                message=message,
                            )
                    else:
                        cursor = connection.execute(record.snapshot.sql)
                        columns = [column[0] for column in cursor.description] if cursor.description else []
                        self._patch_job(
                            job_id,
                            columns=columns,
                            progress_label="Fetching rows..." if columns else "Finalizing...",
                            message="Query is streaming rows..." if columns else "Statement executed successfully.",
                        )

                        rows_buffer: list[tuple[Any, ...]] = []
                        truncated = False
                        row_count = 0
                        message = "Statement executed successfully."

                        if columns:
                            batch_size = max(1, min(25, self._max_result_rows))
                            while len(rows_buffer) <= self._max_result_rows:
                                batch = connection.fetchmany(batch_size)
                                if not batch:
                                    break
                                if first_row_ms is None:
                                    first_row_ms = (time.perf_counter() - started) * 1000
                                    self._patch_job(job_id, first_row_ms=first_row_ms)
                                rows_buffer.extend(tuple(item) for item in batch)
                                truncated = len(rows_buffer) > self._max_result_rows
                                visible_rows = rows_buffer[: self._max_result_rows]
                                row_count = len(visible_rows)
                                message = f"{row_count} row(s) shown."
                                if truncated:
                                    message = (
                                        f"{self._max_result_rows} row(s) shown. "
                                        "The result was truncated for the UI."
                                    )
                                self._patch_job(
                                    job_id,
                                    rows=visible_rows,
                                    row_count=row_count,
                                    rows_shown=row_count,
                                    truncated=truncated,
                                    message=message,
                                )
                                if truncated:
                                    break

                        execution_result = QueryResult(
                            sql=record.snapshot.sql,
                            columns=columns,
                            rows=rows_buffer[: self._max_result_rows],
                            row_count=row_count,
                            truncated=truncated,
                            message=message,
                        )
                except Exception as exc:
                    execution_error = exc

            execution_thread = threading.Thread(
                target=execute_query,
                daemon=True,
                name=f"bdw-query-exec-{job_id[:8]}",
            )
            execution_thread.start()

            while execution_thread.is_alive():
                time.sleep(QUERY_PROGRESS_POLL_SECONDS)
                progress = record.reporter.progress(connection)
                duration_ms = (time.perf_counter() - started) * 1000
                with self._condition:
                    live_record = self._jobs.get(job_id)
                    cancelling = bool(live_record and live_record.cancel_requested)
                progress_label = "Cancelling..." if cancelling else "Running..."
                if progress is not None and not cancelling:
                    progress_label = f"Running... {progress * 100:.0f}%"
                self._patch_job(
                    job_id,
                    duration_ms=duration_ms,
                    progress=progress,
                    progress_label=progress_label,
                )

            execution_thread.join()

            duration_ms = (time.perf_counter() - started) * 1000
            with self._condition:
                record = self._jobs.get(job_id)
                cancel_requested = bool(record and record.cancel_requested)

            if execution_error is not None:
                if cancel_requested:
                    self._finalize_job(
                        job_id,
                        status="cancelled",
                        duration_ms=duration_ms,
                        message="Query cancelled.",
                        progress_label="Cancelled",
                    )
                else:
                    self._finalize_job(
                        job_id,
                        status="failed",
                        duration_ms=duration_ms,
                        message="Query failed.",
                        error=str(execution_error),
                        progress_label="Failed",
                    )
                return

            if execution_result is None:
                self._finalize_job(
                    job_id,
                    status="failed",
                    duration_ms=duration_ms,
                    message="Query failed.",
                    error="The query finished without returning a result.",
                    progress_label="Failed",
                )
                return

            self._finalize_job(
                job_id,
                status="completed",
                duration_ms=duration_ms,
                progress=1.0,
                progress_label="Completed",
                message=execution_result.message,
                columns=execution_result.columns,
                rows=execution_result.rows,
                row_count=execution_result.row_count,
                rows_shown=execution_result.row_count,
                truncated=execution_result.truncated,
                first_row_ms=first_row_ms,
                fetch_ms=max(0.0, duration_ms - first_row_ms) if first_row_ms is not None else None,
            )
        except Exception as exc:
            self._finalize_job(
                job_id,
                status="failed",
                duration_ms=(time.perf_counter() - started) * 1000,
                message="Query failed.",
                error=str(exc),
                progress_label="Failed",
            )
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

            with self._condition:
                record = self._jobs.get(job_id)
                if record is not None:
                    record.connection = None

            try:
                self._metadata_refresher()
            except Exception:
                pass

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
            if status != "completed":
                record.snapshot.progress = None
            for key, value in changes.items():
                setattr(record.snapshot, key, value)
            self._prune_history_locked()
            self._touch_locked()

    def _state_payload_locked(self) -> dict[str, Any]:
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
                payload["columns"] = []
                payload["rows"] = []
            else:
                latest_cell_keys.add(cell_key)
            job_payloads.append(payload)
        running_jobs = [record.snapshot for record in jobs if record.snapshot.status in RUNNING_QUERY_STATUSES]
        completed_jobs = sorted(
            (
                record.snapshot
                for record in self._jobs.values()
                if record.snapshot.status == "completed" and record.snapshot.completed_at
            ),
            key=lambda job: (job.completed_at or job.updated_at or "", job.job_id),
        )
        recent_metrics = [
            QueryJobMetricPoint(
                job_id=job.job_id,
                notebook_id=job.notebook_id,
                notebook_title=job.notebook_title,
                completed_at=job.completed_at or job.updated_at,
                duration_ms=job.duration_ms,
                status=job.status,
                row_count=job.row_count,
            )
            for job in completed_jobs[-18:]
        ]
        duration_values = [metric.duration_ms for metric in recent_metrics]

        return {
            "version": self._state_version,
            "summary": {
                "runningCount": len(running_jobs),
                "totalCount": len(jobs),
            },
            "jobs": job_payloads,
            "performance": {
                "recent": [metric.payload for metric in recent_metrics],
                "stats": {
                    "latestMs": recent_metrics[-1].duration_ms if recent_metrics else None,
                    "p50Ms": percentile(duration_values, 0.5),
                    "p95Ms": percentile(duration_values, 0.95),
                },
            },
        }

    def _touch_locked(self) -> None:
        self._state_version += 1
        payload = self._state_payload_locked()
        self._condition.notify_all()
        if self._state_change_callback is not None:
            self._state_change_callback(payload)

    def _prune_history_locked(self) -> None:
        terminal_jobs = [
            record
            for record in sorted(self._jobs.values(), key=lambda item: item.sort_index)
            if record.snapshot.status in TERMINAL_QUERY_STATUSES
        ]
        overflow = max(0, len(terminal_jobs) - MAX_QUERY_HISTORY)
        for record in terminal_jobs[:overflow]:
            self._jobs.pop(record.snapshot.job_id, None)

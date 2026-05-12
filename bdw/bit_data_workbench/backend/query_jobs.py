from __future__ import annotations

import multiprocessing as mp
import queue
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Protocol

import duckdb

from ..config import Settings
from ..models import QueryJobDefinition, QueryJobMetricPoint, QueryResult
from .runtime_connections import create_duckdb_worker_connection, open_postgres_native_connection

try:
    import psutil
except ImportError:  # pragma: no cover - exercised only when optional dependency is absent.
    psutil = None


RUNNING_QUERY_STATUSES = {"queued", "running"}
TERMINAL_QUERY_STATUSES = {"completed", "failed", "cancelled"}
MAX_QUERY_HISTORY = 80
QUERY_PROGRESS_POLL_SECONDS = 0.35
QUERY_CANCEL_GRACE_SECONDS = 1.5
READ_ONLY_SQL_PATTERN = re.compile(r"^\s*(?:with\b[\s\S]+?\bselect\b|select\b|explain\b)", re.IGNORECASE)


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


def is_read_only_sql(sql: str) -> bool:
    normalized = str(sql or "").strip()
    if not normalized:
        return False
    disallowed = re.search(
        r"\b(insert|update|delete|merge|create|drop|alter|truncate|copy|call|attach|detach|install|load|set|pragma)\b",
        normalized,
        re.IGNORECASE,
    )
    return bool(READ_ONLY_SQL_PATTERN.search(normalized)) and disallowed is None


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
    process: mp.Process | None = None
    event_queue: mp.Queue | None = None
    cancel_event: Any = None
    monitor_thread: threading.Thread | None = None
    cancel_requested: bool = False
    peak_memory_rss_bytes: int = 0


def _job_payload_message(message_type: str, **payload: Any) -> dict[str, Any]:
    return {"type": message_type, **payload}


def _safe_close_connection(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass


def _query_worker_entry(
    *,
    settings: Settings,
    sql: str,
    source_ids: list[str],
    max_result_rows: int,
    workload_type: str,
    event_queue: mp.Queue,
    cancel_event: Any,
) -> None:
    started = time.perf_counter()
    use_postgres_native = any(source_id.strip().lower() == "pg_oltp_native" for source_id in source_ids)
    connection: Any = None
    reporter: QueryProgressReporter = PostgresNativeQueryProgressReporter() if use_postgres_native else DuckDBQueryProgressReporter()
    first_row_ms: float | None = None
    execution_result: QueryResult | None = None
    execution_error: Exception | None = None
    plan_text = ""
    plan_rows: list[tuple[Any, ...]] = []

    try:
        if use_postgres_native:
            connection = open_postgres_native_connection(settings, "oltp")
        else:
            duckdb_read_only = workload_type == "analyze" or is_read_only_sql(sql)
            try:
                connection = create_duckdb_worker_connection(
                    settings,
                    read_only=duckdb_read_only,
                )
            except duckdb.IOException as exc:
                if not duckdb_read_only or "being used by another process" not in str(exc).lower():
                    raise
                connection = create_duckdb_worker_connection(
                    settings,
                    database_path=":memory:",
                )
        event_queue.put(
            _job_payload_message(
                "started",
                processId=mp.current_process().pid,
                backendName=reporter.backend_name,
                engine="postgres-native" if use_postgres_native else "duckdb",
            )
        )

        def execute_query() -> None:
            nonlocal execution_result, execution_error, first_row_ms, plan_text, plan_rows
            try:
                if use_postgres_native:
                    execution_sql = sql
                    if workload_type == "analyze":
                        execution_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"
                    with connection.cursor() as cursor:
                        cursor.execute(execution_sql)
                        columns = [column.name for column in (cursor.description or [])]
                        rows_buffer: list[tuple[Any, ...]] = []
                        row_count = 0
                        truncated = False
                        if columns:
                            batch_size = max(1, min(25, max_result_rows))
                            while len(rows_buffer) <= max_result_rows:
                                if cancel_event.is_set():
                                    raise KeyboardInterrupt("Query cancelled.")
                                batch = cursor.fetchmany(batch_size)
                                if not batch:
                                    break
                                if first_row_ms is None:
                                    first_row_ms = (time.perf_counter() - started) * 1000
                                rows_buffer.extend(tuple(item) for item in batch)
                                truncated = len(rows_buffer) > max_result_rows
                                visible_rows = rows_buffer[:max_result_rows]
                                row_count = len(visible_rows)
                                event_queue.put(
                                    _job_payload_message(
                                        "rows",
                                        columns=columns,
                                        rows=visible_rows,
                                        rowCount=row_count,
                                        rowsShown=row_count,
                                        truncated=truncated,
                                        firstRowMs=first_row_ms,
                                    )
                                )
                                if truncated:
                                    break
                        if workload_type == "analyze":
                            plan_rows = rows_buffer[:max_result_rows]
                            plan_text = "\n".join(str(row[0]) if row else "" for row in plan_rows)
                        execution_result = QueryResult(
                            sql=sql,
                            columns=columns,
                            rows=rows_buffer[:max_result_rows],
                            row_count=row_count,
                            truncated=truncated,
                            message="Statement analyzed." if workload_type == "analyze" else "Statement executed successfully.",
                        )
                    return

                execution_sql = sql
                if workload_type == "analyze":
                    execution_sql = f"EXPLAIN ANALYZE {sql}"
                cursor = connection.execute(execution_sql)
                columns = [column[0] for column in cursor.description] if cursor.description else []
                rows_buffer: list[tuple[Any, ...]] = []
                row_count = 0
                truncated = False
                if columns:
                    batch_size = max(1, min(25, max_result_rows))
                    while len(rows_buffer) <= max_result_rows:
                        if cancel_event.is_set():
                            raise KeyboardInterrupt("Query cancelled.")
                        batch = connection.fetchmany(batch_size)
                        if not batch:
                            break
                        if first_row_ms is None:
                            first_row_ms = (time.perf_counter() - started) * 1000
                        rows_buffer.extend(tuple(item) for item in batch)
                        truncated = len(rows_buffer) > max_result_rows
                        visible_rows = rows_buffer[:max_result_rows]
                        row_count = len(visible_rows)
                        event_queue.put(
                            _job_payload_message(
                                "rows",
                                columns=columns,
                                rows=visible_rows,
                                rowCount=row_count,
                                rowsShown=row_count,
                                truncated=truncated,
                                firstRowMs=first_row_ms,
                            )
                        )
                        if truncated:
                            break
                if workload_type == "analyze":
                    plan_rows = rows_buffer[:max_result_rows]
                    plan_text = "\n".join(str(row[1] if len(row) > 1 else row[0]) for row in plan_rows)
                execution_result = QueryResult(
                    sql=sql,
                    columns=columns,
                    rows=rows_buffer[:max_result_rows],
                    row_count=row_count,
                    truncated=truncated,
                    message="Statement analyzed." if workload_type == "analyze" else "Statement executed successfully.",
                )
            except BaseException as exc:
                execution_error = exc if isinstance(exc, Exception) else RuntimeError(str(exc))

        execution_thread = threading.Thread(target=execute_query, daemon=True, name="bdw-query-child-exec")
        execution_thread.start()
        while execution_thread.is_alive():
            time.sleep(QUERY_PROGRESS_POLL_SECONDS)
            if cancel_event.is_set():
                try:
                    if hasattr(connection, "interrupt"):
                        connection.interrupt()
                    elif hasattr(connection, "cancel"):
                        connection.cancel()
                except Exception:
                    pass
            progress = reporter.progress(connection) if not use_postgres_native else None
            event_queue.put(
                _job_payload_message(
                    "progress",
                    durationMs=(time.perf_counter() - started) * 1000,
                    progress=progress,
                    progressLabel="Cancelling..." if cancel_event.is_set() else (
                        f"Running... {progress * 100:.0f}%" if progress is not None else "Running..."
                    ),
                )
            )
        execution_thread.join()

        duration_ms = (time.perf_counter() - started) * 1000
        if execution_error is not None:
            if cancel_event.is_set():
                event_queue.put(
                    _job_payload_message(
                        "final",
                        status="cancelled",
                        durationMs=duration_ms,
                        message="Query cancelled." if workload_type == "query" else "Analyze cancelled.",
                        progressLabel="Cancelled",
                    )
                )
            else:
                event_queue.put(
                    _job_payload_message(
                        "final",
                        status="failed",
                        durationMs=duration_ms,
                        message="Query failed." if workload_type == "query" else "Analyze failed.",
                        error=str(execution_error),
                        progressLabel="Failed",
                    )
                )
            return

        if execution_result is None:
            event_queue.put(
                _job_payload_message(
                    "final",
                    status="failed",
                    durationMs=duration_ms,
                    message="Query failed.",
                    error="The query finished without returning a result.",
                    progressLabel="Failed",
                )
            )
            return

        event_queue.put(
            _job_payload_message(
                "final",
                status="completed",
                durationMs=duration_ms,
                progress=1.0,
                progressLabel="Completed",
                message=execution_result.message,
                columns=execution_result.columns,
                rows=execution_result.rows,
                rowCount=execution_result.row_count,
                rowsShown=execution_result.row_count,
                truncated=execution_result.truncated,
                firstRowMs=first_row_ms,
                fetchMs=max(0.0, duration_ms - first_row_ms) if first_row_ms is not None else None,
                planText=plan_text,
                planRows=plan_rows,
            )
        )
    except Exception as exc:
        event_queue.put(
            _job_payload_message(
                "final",
                status="failed",
                durationMs=(time.perf_counter() - started) * 1000,
                message="Query failed." if workload_type == "query" else "Analyze failed.",
                error=str(exc),
                progressLabel="Failed",
            )
        )
    finally:
        if connection is not None:
            _safe_close_connection(connection)


class QueryJobManager:
    def __init__(
        self,
        *,
        settings: Settings,
        max_result_rows: int,
        notebook_title_resolver: Callable[[str], str | None],
        metadata_refresher: Callable[[], None],
        state_change_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._settings = settings
        self._max_result_rows = max(1, max_result_rows)
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
        bytes_touched_estimate: int | None = None,
        workload_type: str = "query",
    ) -> QueryJobDefinition:
        normalized_sql = sql.strip()
        normalized_workload = str(workload_type or "query").strip().lower()
        if normalized_workload not in {"query", "analyze"}:
            raise ValueError("Unsupported query workload type.")
        if not normalized_sql:
            raise ValueError("Provide a SQL statement before running the query.")
        if normalized_workload == "analyze" and not is_read_only_sql(normalized_sql):
            raise ValueError("Analyze is only available for read-only SELECT statements.")

        source_ids = [source_id.strip() for source_id in (data_sources or []) if source_id.strip()]
        source_types = infer_source_types(source_ids)
        reporter = self._reporters.for_sources(source_types)
        now = utc_now_iso()
        resolved_title = notebook_title.strip() or self._notebook_title_resolver(notebook_id) or "Notebook"
        job_prefix = "analyze" if normalized_workload == "analyze" else "query"
        snapshot = QueryJobDefinition(
            job_id=f"{job_prefix}-{uuid.uuid4().hex}",
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
            workload_type=normalized_workload,
            engine="postgres-native" if "postgres-native" in source_types else "duckdb",
            bytes_touched_estimate=bytes_touched_estimate,
            can_cancel=True,
        )

        event_queue: mp.Queue = mp.Queue()
        cancel_event = mp.Event()
        process = mp.Process(
            target=_query_worker_entry,
            kwargs={
                "settings": self._settings,
                "sql": normalized_sql,
                "source_ids": source_ids,
                "max_result_rows": self._max_result_rows,
                "workload_type": normalized_workload,
                "event_queue": event_queue,
                "cancel_event": cancel_event,
            },
            daemon=True,
            name=f"bdw-{job_prefix}-{snapshot.job_id[-8:]}",
        )

        with self._condition:
            self._sort_counter += 1
            record = QueryJobRecord(
                snapshot=snapshot,
                reporter=reporter,
                sort_index=self._sort_counter,
                process=process,
                event_queue=event_queue,
                cancel_event=cancel_event,
            )
            self._jobs[snapshot.job_id] = record
            self._touch_locked()

        process.start()
        monitor = threading.Thread(
            target=self._monitor_job,
            args=(snapshot.job_id,),
            daemon=True,
            name=f"bdw-query-monitor-{snapshot.job_id[:8]}",
        )
        with self._condition:
            record.monitor_thread = monitor
            record.snapshot.process_id = process.pid
        monitor.start()
        self._touch()
        return snapshot

    def cancel_job(self, job_id: str) -> QueryJobDefinition:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown query job: {job_id}")

            if record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return record.snapshot

            record.cancel_requested = True
            if record.cancel_event is not None:
                record.cancel_event.set()
            if record.snapshot.status == "queued":
                record.snapshot.status = "cancelled"
                record.snapshot.completed_at = utc_now_iso()
                record.snapshot.progress = None
                record.snapshot.progress_label = "Cancelled"
                record.snapshot.message = "Query cancelled."
                record.snapshot.can_cancel = False
            else:
                record.snapshot.progress_label = "Cancelling..."
                record.snapshot.message = "Cancellation requested."
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()
            snapshot = record.snapshot

        threading.Thread(
            target=self._terminate_after_grace,
            args=(job_id,),
            daemon=True,
            name=f"bdw-query-cancel-{job_id[:8]}",
        ).start()
        return snapshot

    def snapshot(self, job_id: str) -> QueryJobDefinition:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown query job: {job_id}")
            return record.snapshot

    def state_payload(self) -> dict[str, Any]:
        with self._condition:
            return self._state_payload_locked()

    def _monitor_job(self, job_id: str) -> None:
        process: mp.Process | None
        event_queue: mp.Queue | None
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return
            process = record.process
            event_queue = record.event_queue

        psutil_process = None
        if psutil is not None and process is not None and process.pid:
            try:
                psutil_process = psutil.Process(process.pid)
                psutil_process.cpu_percent(interval=None)
            except Exception:
                psutil_process = None

        while True:
            drained = False
            if event_queue is not None:
                while True:
                    try:
                        message = event_queue.get_nowait()
                    except queue.Empty:
                        break
                    drained = True
                    self._apply_worker_message(job_id, message)

            self._sample_process_metrics(job_id, psutil_process)

            alive = bool(process and process.is_alive())
            with self._condition:
                record = self._jobs.get(job_id)
                terminal = record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES
            if terminal and not alive:
                break
            if not alive and not drained:
                with self._condition:
                    record = self._jobs.get(job_id)
                    if record and record.snapshot.status not in TERMINAL_QUERY_STATUSES:
                        exitcode = process.exitcode if process is not None else None
                        status = "cancelled" if record.cancel_requested else "failed"
                        message = "Query cancelled." if status == "cancelled" else "Query worker stopped unexpectedly."
                        record.snapshot.status = status
                        record.snapshot.completed_at = utc_now_iso()
                        record.snapshot.updated_at = record.snapshot.completed_at
                        record.snapshot.progress = None
                        record.snapshot.progress_label = "Cancelled" if status == "cancelled" else "Failed"
                        record.snapshot.message = message
                        record.snapshot.error = None if status == "cancelled" else f"Worker exit code: {exitcode}"
                        record.snapshot.can_cancel = False
                        self._prune_history_locked()
                        self._touch_locked()
                break
            time.sleep(QUERY_PROGRESS_POLL_SECONDS)

        if process is not None:
            try:
                process.join(timeout=0.2)
            except Exception:
                pass
        try:
            self._metadata_refresher()
        except Exception:
            pass

    def _terminate_after_grace(self, job_id: str) -> None:
        time.sleep(QUERY_CANCEL_GRACE_SECONDS)
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return
            process = record.process
        if process is not None and process.is_alive():
            try:
                process.terminate()
            except Exception:
                pass

    def _sample_process_metrics(self, job_id: str, psutil_process: Any) -> None:
        if psutil_process is None:
            return
        try:
            cpu_percent = float(psutil_process.cpu_percent(interval=None))
            memory_rss = int(psutil_process.memory_info().rss)
        except Exception:
            return
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return
            record.peak_memory_rss_bytes = max(record.peak_memory_rss_bytes, memory_rss)
            record.snapshot.cpu_percent = cpu_percent
            record.snapshot.memory_rss_bytes = memory_rss
            record.snapshot.peak_memory_rss_bytes = record.peak_memory_rss_bytes
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

    def _apply_worker_message(self, job_id: str, message: dict[str, Any]) -> None:
        message_type = str(message.get("type") or "").strip()
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return
            snapshot = record.snapshot
            if snapshot.status in TERMINAL_QUERY_STATUSES:
                return

            if message_type == "started":
                snapshot.status = "running"
                snapshot.process_id = int(message.get("processId") or snapshot.process_id or 0) or None
                snapshot.backend_name = str(message.get("backendName") or snapshot.backend_name)
                snapshot.engine = str(message.get("engine") or snapshot.engine)
                snapshot.progress = None
                snapshot.progress_label = "Running..."
                snapshot.message = "Running query..." if snapshot.workload_type == "query" else "Analyzing query..."
            elif message_type == "progress":
                snapshot.duration_ms = float(message.get("durationMs") or snapshot.duration_ms or 0)
                progress = message.get("progress")
                snapshot.progress = progress if isinstance(progress, float) else None
                snapshot.progress_label = str(message.get("progressLabel") or snapshot.progress_label)
            elif message_type == "rows":
                snapshot.columns = [str(column) for column in (message.get("columns") or [])]
                snapshot.rows = [tuple(row) for row in (message.get("rows") or [])]
                snapshot.row_count = int(message.get("rowCount") or 0)
                snapshot.rows_shown = int(message.get("rowsShown") or snapshot.row_count)
                snapshot.truncated = bool(message.get("truncated"))
                if message.get("firstRowMs") is not None:
                    snapshot.first_row_ms = float(message.get("firstRowMs") or 0)
                snapshot.message = (
                    "Analyze output is available."
                    if snapshot.workload_type == "analyze"
                    else f"{snapshot.rows_shown} row(s) shown."
                )
            elif message_type == "final":
                snapshot.status = str(message.get("status") or "failed")
                snapshot.duration_ms = float(message.get("durationMs") or snapshot.duration_ms or 0)
                snapshot.completed_at = utc_now_iso()
                snapshot.can_cancel = False
                snapshot.progress = message.get("progress") if snapshot.status == "completed" else None
                snapshot.progress_label = str(message.get("progressLabel") or snapshot.progress_label)
                snapshot.message = str(message.get("message") or snapshot.message or "")
                snapshot.error = str(message.get("error") or "") or None
                snapshot.columns = [str(column) for column in (message.get("columns") or snapshot.columns or [])]
                snapshot.rows = [tuple(row) for row in (message.get("rows") or snapshot.rows or [])]
                snapshot.row_count = int(message.get("rowCount") or snapshot.row_count or 0)
                snapshot.rows_shown = int(message.get("rowsShown") or snapshot.rows_shown or 0)
                snapshot.truncated = bool(message.get("truncated") or snapshot.truncated)
                snapshot.first_row_ms = (
                    float(message.get("firstRowMs")) if message.get("firstRowMs") is not None else snapshot.first_row_ms
                )
                snapshot.fetch_ms = (
                    float(message.get("fetchMs")) if message.get("fetchMs") is not None else snapshot.fetch_ms
                )
                snapshot.plan_text = str(message.get("planText") or snapshot.plan_text or "")
                snapshot.plan_rows = [tuple(row) for row in (message.get("planRows") or snapshot.plan_rows or [])]
                self._prune_history_locked()
            else:
                return

            snapshot.updated_at = utc_now_iso()
            self._touch_locked()

    def _touch(self) -> None:
        with self._condition:
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
                if record.snapshot.status == "completed" and record.snapshot.completed_at and record.snapshot.workload_type == "query"
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

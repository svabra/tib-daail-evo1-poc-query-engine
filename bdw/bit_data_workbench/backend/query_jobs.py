from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import queue
import tempfile
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import duckdb

try:  # pragma: no cover - exercised by dependency availability in runtime tests.
    import psutil
except Exception:  # pragma: no cover
    psutil = None  # type: ignore[assignment]

from ..config import Settings
from ..models import QueryJobDefinition, QueryJobMetricPoint, QueryResourceSample, QueryResult
from .runtime_connections import create_duckdb_worker_connection, open_postgres_native_connection


logger = logging.getLogger(__name__)

RUNNING_QUERY_STATUSES = {"queued", "running"}
TERMINAL_QUERY_STATUSES = {"completed", "failed", "cancelled"}
MAX_QUERY_HISTORY = 80
QUERY_PROGRESS_POLL_SECONDS = 0.35
QUERY_PARENT_POLL_SECONDS = 0.1
QUERY_METRICS_SAMPLE_SECONDS = 1.0
MAX_QUERY_RESOURCE_SAMPLES = 180
QUERY_INTERRUPT_GRACE_SECONDS = 1.5
QUERY_TERMINATE_GRACE_SECONDS = 1.5
QUERY_LOG_MAX_LIST_ITEMS = 5
QUERY_LOG_MAX_TEXT_CHARS = 320
QUERY_LOG_MAX_ERROR_CHARS = 500

QUERY_EXECUTION_DUCKDB_READ = "duckdb-read"
QUERY_EXECUTION_DUCKDB_WRITE = "duckdb-write"
QUERY_EXECUTION_POSTGRES_NATIVE = "postgres-native"
READ_ONLY_START_KEYWORDS = {"select", "with", "values", "describe", "show", "summarize"}
WRITE_START_KEYWORDS = {
    "alter",
    "attach",
    "call",
    "copy",
    "create",
    "delete",
    "detach",
    "drop",
    "export",
    "import",
    "insert",
    "install",
    "load",
    "pragma",
    "set",
    "truncate",
    "update",
    "vacuum",
}
MUTATING_KEYWORDS = WRITE_START_KEYWORDS | {
    "checkpoint",
    "force",
    "reset",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _query_log_timestamp(timezone_name: str) -> str:
    normalized_timezone = str(timezone_name or "").strip() or "Europe/Zurich"
    try:
        timezone = ZoneInfo(normalized_timezone)
    except Exception:
        with suppress(Exception):
            timezone = ZoneInfo("CET")
        if "timezone" not in locals():
            timezone = UTC
    return datetime.now(UTC).astimezone(timezone).strftime("%Y-%m-%d %H:%M:%S %Z")


def _truncate_log_text(value: object, max_chars: int = QUERY_LOG_MAX_TEXT_CHARS) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if len(text) <= max_chars:
        return text
    return f"{text[: max(0, max_chars - 3)]}..."


def _safe_error_log_text(value: object) -> str:
    safe_lines: list[str] = []
    sql_starters = tuple(f"{keyword} " for keyword in sorted(READ_ONLY_START_KEYWORDS | WRITE_START_KEYWORDS))
    for raw_line in str(value or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lowered = line.lower()
        if lowered.startswith("line ") or lowered.startswith(sql_starters):
            continue
        if set(line) <= {"^", "~", " "}:
            continue
        safe_lines.append(line)
        if len(safe_lines) >= 3:
            break
    return _truncate_log_text(" | ".join(safe_lines) or "Query failed.", QUERY_LOG_MAX_ERROR_CHARS)


def _format_query_log_value(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    return json.dumps(_truncate_log_text(value), ensure_ascii=True)


def _format_query_log_fields(fields: dict[str, object]) -> str:
    return " ".join(
        f"{key}={_format_query_log_value(value)}"
        for key, value in fields.items()
        if value not in (None, "", [], {})
    )


def _capped_log_list(values: list[object]) -> tuple[list[object], int]:
    compact_values = [value for value in values if value not in (None, "", [], {})]
    return compact_values[:QUERY_LOG_MAX_LIST_ITEMS], max(0, len(compact_values) - QUERY_LOG_MAX_LIST_ITEMS)


def _duckdb_sql_string(value: object) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


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


def _strip_leading_comments(sql: str) -> str:
    text = str(sql or "").lstrip()
    while text:
        if text.startswith("--"):
            newline_index = text.find("\n")
            if newline_index < 0:
                return ""
            text = text[newline_index + 1 :].lstrip()
            continue
        if text.startswith("/*"):
            end_index = text.find("*/", 2)
            if end_index < 0:
                return ""
            text = text[end_index + 2 :].lstrip()
            continue
        return text
    return ""


def _split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    text = str(sql or "")
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if quote is None and char == "-" and next_char == "-":
            while index < len(text) and text[index] not in "\r\n":
                current.append(text[index])
                index += 1
            continue
        if quote is None and char == "/" and next_char == "*":
            current.append(char)
            current.append(next_char)
            index += 2
            while index + 1 < len(text) and not (text[index] == "*" and text[index + 1] == "/"):
                current.append(text[index])
                index += 1
            if index + 1 < len(text):
                current.append(text[index])
                current.append(text[index + 1])
                index += 2
            continue
        if quote is None and char in {"'", '"', "`"}:
            quote = char
            current.append(char)
            index += 1
            continue
        if quote is not None:
            current.append(char)
            if char == quote:
                if quote in {"'", '"'} and next_char == quote:
                    current.append(next_char)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue
        current.append(char)
        index += 1

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)
    return statements


def _first_keyword(sql: str) -> str:
    text = _strip_leading_comments(sql)
    while text.startswith("("):
        text = text[1:].lstrip()
    token: list[str] = []
    for char in text:
        if char.isalnum() or char == "_":
            token.append(char.lower())
            continue
        break
    return "".join(token)


def _contains_mutating_keyword(sql: str) -> bool:
    token: list[str] = []
    for char in str(sql or ""):
        if char.isalnum() or char == "_":
            token.append(char.lower())
            continue
        if token and "".join(token) in MUTATING_KEYWORDS:
            return True
        token = []
    return bool(token and "".join(token) in MUTATING_KEYWORDS)


def classify_query_execution(sql: str, data_sources: list[str] | None = None) -> str:
    source_ids = [source_id.strip().lower() for source_id in (data_sources or []) if source_id.strip()]
    if any(source_id.endswith("_native") for source_id in source_ids):
        return QUERY_EXECUTION_POSTGRES_NATIVE

    statements = _split_sql_statements(sql)
    if len(statements) != 1:
        return QUERY_EXECUTION_DUCKDB_WRITE

    statement = statements[0] if statements else str(sql or "")
    first_keyword = _first_keyword(statement)
    if first_keyword == "explain":
        remainder = _strip_leading_comments(statement)
        remainder = remainder[len("explain") :].strip() if remainder.lower().startswith("explain") else remainder
        first_keyword = _first_keyword(remainder)
    if first_keyword in READ_ONLY_START_KEYWORDS and not _contains_mutating_keyword(statement):
        return QUERY_EXECUTION_DUCKDB_READ
    if first_keyword in WRITE_START_KEYWORDS:
        return QUERY_EXECUTION_DUCKDB_WRITE
    return QUERY_EXECUTION_DUCKDB_WRITE


def _duckdb_query_progress(connection: duckdb.DuckDBPyConnection) -> float | None:
    try:
        progress = float(connection.query_progress())
    except Exception:
        return None
    if progress < 0:
        return None
    return max(0.0, min(progress, 1.0))


def _enable_duckdb_progress(connection: duckdb.DuckDBPyConnection) -> None:
    for statement in (
        "SET enable_progress_bar=true",
        "SET enable_progress_bar_print=false",
        "SET progress_bar_time=0",
    ):
        with suppress(Exception):
            connection.execute(statement)


def _enable_duckdb_profiling(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
) -> Path | None:
    if not bool(getattr(settings, "query_job_duckdb_profiling_enabled", True)):
        return None

    profile_file = tempfile.NamedTemporaryFile(
        prefix="bdw-duckdb-query-profile-",
        suffix=".json",
        delete=False,
    )
    profile_path = Path(profile_file.name)
    profile_file.close()
    try:
        connection.execute("PRAGMA enable_profiling='json'")
        connection.execute(f"PRAGMA profiling_output={_duckdb_sql_string(profile_path.as_posix())}")
    except Exception:
        with suppress(Exception):
            profile_path.unlink(missing_ok=True)
        return None
    return profile_path


def _profile_float_ms(payload: dict[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return round(float(value) * 1000, 3)
    except (TypeError, ValueError):
        return None


def _profile_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return None


def _read_duckdb_profile_summary(profile_path: Path | None) -> dict[str, object]:
    if profile_path is None:
        return {}
    try:
        if not profile_path.exists():
            return {}
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {}
        summary = {
            "duckdb_latency_ms": _profile_float_ms(payload, "latency"),
            "duckdb_cpu_ms": _profile_float_ms(payload, "cpu_time"),
            "duckdb_rows_returned": _profile_int(payload, "rows_returned"),
            "duckdb_rows_scanned": _profile_int(payload, "cumulative_rows_scanned"),
            "duckdb_bytes_read": _profile_int(payload, "total_bytes_read"),
            "duckdb_bytes_written": _profile_int(payload, "total_bytes_written"),
            "duckdb_peak_buffer_memory_bytes": _profile_int(payload, "system_peak_buffer_memory"),
            "duckdb_peak_temp_dir_bytes": _profile_int(payload, "system_peak_temp_dir_size"),
        }
        return {key: value for key, value in summary.items() if value is not None}
    except Exception:
        return {}
    finally:
        with suppress(Exception):
            profile_path.unlink(missing_ok=True)


def _put_final_worker_event(
    event_queue: Any,
    event: dict[str, Any],
    duckdb_profile_summary: dict[str, object] | None = None,
) -> None:
    if duckdb_profile_summary:
        event["duckdbProfile"] = dict(duckdb_profile_summary)
    _put_worker_event(event_queue, event)


def _put_worker_event(event_queue: Any, event: dict[str, Any]) -> None:
    try:
        event_queue.put(event)
    except Exception:
        pass


def _execute_postgres_native_query(
    *,
    connection: Any,
    sql: str,
    max_result_rows: int,
    event_queue: Any,
    started: float,
) -> tuple[QueryResult, float | None]:
    first_row_ms: float | None = None
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [column.name for column in (cursor.description or [])]
        _put_worker_event(
            event_queue,
            {
                "type": "columns",
                "columns": columns,
                "progressLabel": "Fetching rows..." if columns else "Finalizing...",
                "message": "Query is fetching rows..." if columns else "Statement executed successfully.",
            },
        )

        rows_buffer: list[tuple[Any, ...]] = []
        truncated = False
        row_count = 0
        message = "Statement executed successfully."

        if columns:
            batch_size = max(1, min(25, max_result_rows))
            while len(rows_buffer) <= max_result_rows:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                if first_row_ms is None:
                    first_row_ms = (time.perf_counter() - started) * 1000
                rows_buffer.extend(tuple(item) for item in batch)
                truncated = len(rows_buffer) > max_result_rows
                visible_rows = rows_buffer[:max_result_rows]
                row_count = len(visible_rows)
                message = f"{row_count} row(s) shown."
                if truncated:
                    message = f"{max_result_rows} row(s) shown. The result was truncated for the UI."
                _put_worker_event(
                    event_queue,
                    {
                        "type": "rows",
                        "rows": visible_rows,
                        "rowCount": row_count,
                        "rowsShown": row_count,
                        "truncated": truncated,
                        "firstRowMs": first_row_ms,
                        "message": message,
                    },
                )
                if truncated:
                    break

    return (
        QueryResult(
            sql=sql,
            columns=columns,
            rows=rows_buffer[:max_result_rows],
            row_count=row_count,
            truncated=truncated,
            message=message,
        ),
        first_row_ms,
    )


def _execute_duckdb_query(
    *,
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    max_result_rows: int,
    event_queue: Any,
    started: float,
) -> tuple[QueryResult, float | None]:
    first_row_ms: float | None = None
    cursor = connection.execute(sql)
    columns = [column[0] for column in cursor.description] if cursor.description else []
    _put_worker_event(
        event_queue,
        {
            "type": "columns",
            "columns": columns,
            "progressLabel": "Fetching rows..." if columns else "Finalizing...",
            "message": "Query is streaming rows..." if columns else "Statement executed successfully.",
        },
    )

    rows_buffer: list[tuple[Any, ...]] = []
    truncated = False
    row_count = 0
    message = "Statement executed successfully."

    if columns:
        batch_size = max(1, min(25, max_result_rows))
        while len(rows_buffer) <= max_result_rows:
            batch = connection.fetchmany(batch_size)
            if not batch:
                break
            if first_row_ms is None:
                first_row_ms = (time.perf_counter() - started) * 1000
            rows_buffer.extend(tuple(item) for item in batch)
            truncated = len(rows_buffer) > max_result_rows
            visible_rows = rows_buffer[:max_result_rows]
            row_count = len(visible_rows)
            message = f"{row_count} row(s) shown."
            if truncated:
                message = f"{max_result_rows} row(s) shown. The result was truncated for the UI."
            _put_worker_event(
                event_queue,
                {
                    "type": "rows",
                    "rows": visible_rows,
                    "rowCount": row_count,
                    "rowsShown": row_count,
                    "truncated": truncated,
                    "firstRowMs": first_row_ms,
                    "message": message,
                },
            )
            if truncated:
                break

    return (
        QueryResult(
            sql=sql,
            columns=columns,
            rows=rows_buffer[:max_result_rows],
            row_count=row_count,
            truncated=truncated,
            message=message,
        ),
        first_row_ms,
    )


def _query_worker_entry(
    *,
    event_queue: Any,
    cancel_event: Any,
    settings: Settings,
    sql: str,
    execution_mode: str,
    max_result_rows: int,
    database_path: str | None = None,
) -> None:
    started = time.perf_counter()
    connection: Any = None
    execution_result: QueryResult | None = None
    first_row_ms: float | None = None
    execution_error: Exception | None = None
    duckdb_profile_path: Path | None = None

    try:
        if cancel_event.is_set():
            _put_final_worker_event(
                event_queue,
                {
                    "type": "final",
                    "status": "cancelled",
                    "durationMs": 0.0,
                    "message": "Query cancelled before the worker started.",
                    "progressLabel": "Cancelled",
                    "cancellationPhase": "cancelled",
                },
            )
            return

        if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE:
            connection = open_postgres_native_connection(settings, "oltp")
        else:
            worker_database_path = str(database_path or "").strip() or None
            connection = create_duckdb_worker_connection(
                settings,
                database_path=worker_database_path,
                read_only=execution_mode == QUERY_EXECUTION_DUCKDB_READ and worker_database_path != ":memory:",
            )
            _enable_duckdb_progress(connection)
            duckdb_profile_path = _enable_duckdb_profiling(connection, settings)

        _put_worker_event(
            event_queue,
            {
                "type": "started",
                "processId": os.getpid(),
            },
        )

        def execute_query() -> None:
            nonlocal execution_result, first_row_ms, execution_error
            try:
                if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE:
                    execution_result, first_row_ms = _execute_postgres_native_query(
                        connection=connection,
                        sql=sql,
                        max_result_rows=max_result_rows,
                        event_queue=event_queue,
                        started=started,
                    )
                else:
                    execution_result, first_row_ms = _execute_duckdb_query(
                        connection=connection,
                        sql=sql,
                        max_result_rows=max_result_rows,
                        event_queue=event_queue,
                        started=started,
                    )
            except Exception as exc:
                execution_error = exc

        execution_thread = threading.Thread(
            target=execute_query,
            daemon=True,
            name="bdw-query-worker-exec",
        )
        execution_thread.start()
        interrupt_sent = False

        while execution_thread.is_alive():
            if cancel_event.is_set() and not interrupt_sent:
                interrupt_sent = True
                _put_worker_event(
                    event_queue,
                    {
                        "type": "cancellation",
                        "cancellationPhase": "interrupting",
                        "progressLabel": "Cancelling...",
                        "message": "Interrupting the query worker.",
                    },
                )
                with suppress(Exception):
                    if hasattr(connection, "interrupt"):
                        connection.interrupt()
                    elif hasattr(connection, "cancel"):
                        connection.cancel()

            duration_ms = (time.perf_counter() - started) * 1000
            progress = (
                _duckdb_query_progress(connection)
                if execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE and connection is not None
                else None
            )
            progress_label = "Cancelling..." if cancel_event.is_set() else "Running..."
            if progress is not None and not cancel_event.is_set():
                progress_label = f"Running... {progress * 100:.0f}%"
            _put_worker_event(
                event_queue,
                {
                    "type": "progress",
                    "durationMs": duration_ms,
                    "progress": progress,
                    "progressLabel": progress_label,
                },
            )
            time.sleep(QUERY_PROGRESS_POLL_SECONDS)

        execution_thread.join()
        duration_ms = (time.perf_counter() - started) * 1000
        duckdb_profile_summary = _read_duckdb_profile_summary(duckdb_profile_path)

        if execution_error is not None:
            if cancel_event.is_set():
                _put_final_worker_event(
                    event_queue,
                    {
                        "type": "final",
                        "status": "cancelled",
                        "durationMs": duration_ms,
                        "message": "Query cancellation completed.",
                        "progressLabel": "Cancelled",
                        "cancellationPhase": "cancelled",
                    },
                    duckdb_profile_summary,
                )
            else:
                _put_final_worker_event(
                    event_queue,
                    {
                        "type": "final",
                        "status": "failed",
                        "durationMs": duration_ms,
                        "message": "Query failed.",
                        "error": str(execution_error),
                        "progressLabel": "Failed",
                    },
                    duckdb_profile_summary,
                )
            return

        if execution_result is None:
            _put_final_worker_event(
                event_queue,
                {
                    "type": "final",
                    "status": "failed",
                    "durationMs": duration_ms,
                    "message": "Query failed.",
                    "error": "The query finished without returning a result.",
                    "progressLabel": "Failed",
                },
                duckdb_profile_summary,
            )
            return

        _put_final_worker_event(
            event_queue,
            {
                "type": "final",
                "status": "completed",
                "durationMs": duration_ms,
                "progress": 1.0,
                "progressLabel": "Completed",
                "message": execution_result.message,
                "columns": execution_result.columns,
                "rows": execution_result.rows,
                "rowCount": execution_result.row_count,
                "rowsShown": execution_result.row_count,
                "truncated": execution_result.truncated,
                "firstRowMs": first_row_ms,
                "fetchMs": max(0.0, duration_ms - first_row_ms) if first_row_ms is not None else None,
            },
            duckdb_profile_summary,
        )
    except Exception as exc:
        _put_final_worker_event(
            event_queue,
            {
                "type": "final",
                "status": "cancelled" if cancel_event.is_set() else "failed",
                "durationMs": (time.perf_counter() - started) * 1000,
                "message": "Query cancellation completed." if cancel_event.is_set() else "Query failed.",
                "error": None if cancel_event.is_set() else str(exc),
                "progressLabel": "Cancelled" if cancel_event.is_set() else "Failed",
                "cancellationPhase": "cancelled" if cancel_event.is_set() else None,
            },
            _read_duckdb_profile_summary(duckdb_profile_path),
        )
    finally:
        if connection is not None:
            with suppress(Exception):
                connection.close()


class DuckDBQueryAccessCoordinator:
    def __init__(self) -> None:
        self._condition = threading.Condition(threading.RLock())
        self._active_reads = 0
        self._active_write = False
        self._waiting_writes = 0

    def acquire(
        self,
        execution_mode: str,
        is_cancelled: Any,
        *,
        on_waiting: Callable[[], None] | None = None,
    ) -> bool:
        if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE:
            return True

        with self._condition:
            if execution_mode == QUERY_EXECUTION_DUCKDB_READ:
                while self._active_write or self._waiting_writes > 0:
                    if is_cancelled():
                        return False
                    if on_waiting is not None:
                        on_waiting()
                    self._condition.wait(timeout=0.1)
                self._active_reads += 1
                return True

            self._waiting_writes += 1
            try:
                while self._active_write or self._active_reads > 0:
                    if is_cancelled():
                        return False
                    if on_waiting is not None:
                        on_waiting()
                    self._condition.wait(timeout=0.1)
                self._active_write = True
                return True
            finally:
                self._waiting_writes = max(0, self._waiting_writes - 1)

    def release(self, execution_mode: str) -> None:
        if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE:
            return
        with self._condition:
            if execution_mode == QUERY_EXECUTION_DUCKDB_READ:
                self._active_reads = max(0, self._active_reads - 1)
            else:
                self._active_write = False
            self._condition.notify_all()


@dataclass(slots=True)
class QueryJobRecord:
    snapshot: QueryJobDefinition
    sort_index: int
    execution_mode: str
    execution_sql: str
    worker_database_path: str | None = None
    source_summaries: list[dict[str, object]] = field(default_factory=list)
    cancel_requested: bool = False
    cancellation_started_monotonic: float | None = None
    terminate_sent: bool = False
    kill_sent: bool = False
    process: Any | None = None
    cancel_event: Any | None = None
    event_queue: Any | None = None
    thread: threading.Thread | None = None
    process_metrics: Any | None = None
    cpu_percent_initialized: bool = False
    last_metric_sample_monotonic: float = 0.0
    cpu_sample_total: float = 0.0
    cpu_sample_count: int = 0
    memory_sample_total: int = 0
    memory_sample_count: int = 0
    final_received: bool = False
    access_acquired: bool = False
    last_log_heartbeat_monotonic: float = 0.0
    logged_events: set[str] = field(default_factory=set)


class QueryJobManager:
    def __init__(
        self,
        *,
        settings: Settings,
        max_result_rows: int,
        notebook_title_resolver: Any,
        metadata_refresher: Any,
        state_change_callback: Any | None = None,
        terminal_job_callback: Any | None = None,
        access_coordinator: DuckDBQueryAccessCoordinator | None = None,
        multiprocessing_context: Any | None = None,
    ) -> None:
        self._settings = settings
        self._max_result_rows = max(1, max_result_rows)
        self._notebook_title_resolver = notebook_title_resolver
        self._metadata_refresher = metadata_refresher
        self._state_change_callback = state_change_callback
        self._terminal_job_callback = terminal_job_callback
        self._access_coordinator = access_coordinator or DuckDBQueryAccessCoordinator()
        self._mp_context = multiprocessing_context or mp.get_context("spawn")
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._jobs: dict[str, QueryJobRecord] = {}
        self._sort_counter = 0
        self._state_version = 0

    def start_job(
        self,
        *,
        sql: str,
        execution_sql: str = "",
        notebook_id: str,
        notebook_title: str,
        cell_id: str,
        data_sources: list[str] | None = None,
        touched_relations: list[str] | None = None,
        touched_buckets: list[str] | None = None,
        source_summaries: list[dict[str, object]] | None = None,
    ) -> QueryJobDefinition:
        normalized_sql = sql.strip()
        if not normalized_sql:
            raise ValueError("Provide a SQL statement before running the query.")

        source_ids = [source_id.strip() for source_id in (data_sources or []) if source_id.strip()]
        source_types = infer_source_types(source_ids)
        normalized_execution_sql = str(execution_sql or sql or "").strip()
        execution_mode = classify_query_execution(normalized_execution_sql, source_ids)
        now = utc_now_iso()
        resolved_title = notebook_title.strip() or self._notebook_title_resolver(notebook_id) or "Notebook"
        backend_name = "PostgreSQL Native" if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE else "VMTP DUCKDB"
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
            backend_name=backend_name,
            execution_mode=execution_mode,
            can_cancel=True,
        )

        with self._condition:
            self._sort_counter += 1
            record = QueryJobRecord(
                snapshot=snapshot,
                sort_index=self._sort_counter,
                execution_mode=execution_mode,
                execution_sql=normalized_execution_sql,
                worker_database_path=self._worker_database_path(
                    execution_mode=execution_mode,
                    source_ids=source_ids,
                    touched_relations=touched_relations,
                    touched_buckets=touched_buckets,
                ),
                source_summaries=[
                    dict(item)
                    for item in (source_summaries or [])
                    if isinstance(item, dict)
                ],
                last_log_heartbeat_monotonic=time.monotonic(),
            )
            self._jobs[snapshot.job_id] = record
            self._touch_locked()

        self._log_query_job_event(snapshot.job_id, "queued")
        self._log_query_job_event(snapshot.job_id, "prepared")

        worker = threading.Thread(
            target=self._run_job,
            args=(snapshot.job_id,),
            daemon=True,
            name=f"bdw-query-supervisor-{snapshot.job_id[:8]}",
        )
        with self._condition:
            record.thread = worker
        worker.start()
        return snapshot

    def cancel_job(self, job_id: str) -> QueryJobDefinition:
        cancel_event = None
        terminal_payload: dict[str, Any] | None = None
        queued_cancel_snapshot: QueryJobDefinition | None = None
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown query job: {job_id}")

            if record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return record.snapshot

            record.cancel_requested = True
            record.cancellation_started_monotonic = record.cancellation_started_monotonic or time.monotonic()
            cancel_event = record.cancel_event
            if record.snapshot.status == "queued" and record.process is None:
                completed_at = utc_now_iso()
                record.snapshot.status = "cancelled"
                record.snapshot.completed_at = completed_at
                record.snapshot.updated_at = completed_at
                record.snapshot.progress = None
                record.snapshot.progress_label = "Cancelled"
                record.snapshot.message = "Query cancelled before the worker started."
                record.snapshot.cancellation_phase = "cancelled"
                record.snapshot.cancellation_requested_at = completed_at
                record.snapshot.can_cancel = False
                self._touch_locked()
                terminal_payload = record.snapshot.payload
                queued_cancel_snapshot = record.snapshot
            else:
                record.snapshot.updated_at = utc_now_iso()
                record.snapshot.progress_label = "Cancelling..."
                record.snapshot.message = "Cancellation requested."
                record.snapshot.cancellation_phase = "requested"
                record.snapshot.cancellation_requested_at = record.snapshot.cancellation_requested_at or record.snapshot.updated_at
                record.snapshot.can_cancel = False
                self._touch_locked()

        self._log_query_job_event(job_id, "cancel_requested")
        if terminal_payload is not None and self._terminal_job_callback is not None:
            with suppress(Exception):
                self._terminal_job_callback(terminal_payload)
        if queued_cancel_snapshot is not None:
            self._log_query_job_event(job_id, "cancelled")
            return queued_cancel_snapshot

        if cancel_event is not None:
            with suppress(Exception):
                cancel_event.set()

        with self._condition:
            return self._jobs[job_id].snapshot

    def snapshot(self, job_id: str) -> QueryJobDefinition:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown query job: {job_id}")
            return record.snapshot

    def state_payload(self) -> dict[str, Any]:
        with self._condition:
            return self._state_payload_locked()

    def _log_query_job_event(
        self,
        job_id: str,
        event: str,
        *,
        level: int = logging.INFO,
        extra: dict[str, object] | None = None,
        once: bool = True,
    ) -> None:
        if not bool(getattr(self._settings, "query_job_logging_enabled", True)):
            return
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return
            if once and event in record.logged_events:
                return
            if once:
                record.logged_events.add(event)
            fields = self._query_job_log_fields(record, event=event, extra=extra or {})
        logger.log(level, "[bdw-query] %s", _format_query_log_fields(fields))

    def _log_query_heartbeat_if_due(self, job_id: str) -> None:
        if not bool(getattr(self._settings, "query_job_logging_enabled", True)):
            return
        now_monotonic = time.monotonic()
        with self._condition:
            record = self._jobs.get(job_id)
            if (
                record is None
                or record.cancel_requested
                or record.snapshot.status not in RUNNING_QUERY_STATUSES
            ):
                return
            interval = max(
                5,
                int(getattr(self._settings, "query_job_log_heartbeat_seconds", 10) or 10),
            )
            if (
                record.last_log_heartbeat_monotonic
                and now_monotonic - record.last_log_heartbeat_monotonic < interval
            ):
                return
            record.last_log_heartbeat_monotonic = now_monotonic
            fields = self._query_job_log_fields(record, event="heartbeat", extra={})
        logger.info("[bdw-query] %s", _format_query_log_fields(fields))

    def _query_job_log_fields(
        self,
        record: QueryJobRecord,
        *,
        event: str,
        extra: dict[str, object],
    ) -> dict[str, object]:
        snapshot = record.snapshot
        fields: dict[str, object] = {
            "query_job_time": _query_log_timestamp(
                str(getattr(self._settings, "query_job_log_timezone", "Europe/Zurich") or "Europe/Zurich")
            ),
            "query_job_event": event,
            "job_id": snapshot.job_id,
            "notebook_id": snapshot.notebook_id,
            "notebook_title": snapshot.notebook_title,
            "cell_id": snapshot.cell_id,
            "status": snapshot.status,
            "backend": snapshot.backend_name,
            "execution_mode": snapshot.execution_mode,
        }
        if snapshot.process_id is not None:
            fields["process_id"] = snapshot.process_id
        if snapshot.duration_ms:
            fields["duration_ms"] = round(float(snapshot.duration_ms), 3)
        if snapshot.progress_label:
            fields["progress_label"] = snapshot.progress_label
        if (
            snapshot.progress is not None
            and event == "heartbeat"
            and snapshot.status == "running"
            and snapshot.execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE
        ):
            fields["duckdb_progress_percent"] = round(float(snapshot.progress) * 100, 1)
        if snapshot.cpu_percent is not None and event == "heartbeat":
            fields["cpu_percent"] = round(float(snapshot.cpu_percent), 2)
        if snapshot.memory_rss_bytes is not None and event == "heartbeat":
            fields["ram_mb"] = round(float(snapshot.memory_rss_bytes) / (1024 * 1024), 1)
        if event == "heartbeat":
            fields["elapsed_seconds"] = round(self._elapsed_duration_ms_locked(record) / 1000, 1)

        data_sources, source_overflow = _capped_log_list(list(snapshot.data_sources))
        if data_sources:
            fields["data_sources"] = data_sources
        if source_overflow:
            fields["data_source_overflow_count"] = source_overflow

        touched_relations, relation_overflow = _capped_log_list(list(snapshot.touched_relations))
        if touched_relations:
            fields["touched_relations"] = touched_relations
        if relation_overflow:
            fields["touched_relation_overflow_count"] = relation_overflow

        touched_buckets, bucket_overflow = _capped_log_list(list(snapshot.touched_buckets))
        if touched_buckets:
            fields["touched_buckets"] = touched_buckets
        if bucket_overflow:
            fields["touched_bucket_overflow_count"] = bucket_overflow

        if event == "prepared":
            source_summaries, source_summary_overflow = _capped_log_list(
                [self._compact_source_summary(item) for item in record.source_summaries]
            )
            if source_summaries:
                fields["s3_sources"] = source_summaries
            if source_summary_overflow:
                fields["source_overflow_count"] = source_summary_overflow

        if event in TERMINAL_QUERY_STATUSES:
            fields["row_count"] = snapshot.row_count
            fields["rows_shown"] = snapshot.rows_shown
            fields["truncated"] = snapshot.truncated
            if snapshot.worker_exit_code is not None:
                fields["worker_exit_code"] = snapshot.worker_exit_code
            if snapshot.error:
                fields["error"] = _safe_error_log_text(snapshot.error)
        for key, value in (extra or {}).items():
            if key == "error" and value:
                fields[key] = _safe_error_log_text(value)
                continue
            fields[key] = value
        return fields

    @staticmethod
    def _compact_source_summary(summary: dict[str, object]) -> dict[str, object]:
        compact: dict[str, object] = {}
        for key in ("relation", "query_alias", "bucket", "key", "path", "format"):
            value = summary.get(key)
            if value not in (None, "", [], {}):
                compact[key] = _truncate_log_text(value, QUERY_LOG_MAX_TEXT_CHARS)
        return compact

    def shutdown(self) -> None:
        records: list[QueryJobRecord]
        with self._condition:
            records = list(self._jobs.values())
        for record in records:
            if record.snapshot.status in TERMINAL_QUERY_STATUSES:
                continue
            with suppress(Exception):
                if record.cancel_event is not None:
                    record.cancel_event.set()
            with suppress(Exception):
                if record.process is not None and record.process.is_alive():
                    record.process.terminate()
        for record in records:
            with suppress(Exception):
                if record.thread is not None and record.thread.is_alive():
                    record.thread.join(timeout=0.5)

    def _run_job(self, job_id: str) -> None:
        access_acquired = False
        try:
            with self._condition:
                record = self._jobs.get(job_id)
                if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                    return
                record.snapshot.progress_label = "Queued..."
                record.snapshot.message = "Waiting for an available query worker."
                self._touch_locked()

            requires_duckdb_file_access = not (
                record.execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE
                or record.worker_database_path == ":memory:"
            )
            if requires_duckdb_file_access:
                access_acquired = self._access_coordinator.acquire(
                    record.execution_mode,
                    lambda: self._is_cancelled_or_terminal(job_id),
                    on_waiting=lambda: self._log_query_heartbeat_if_due(job_id),
                )
                if not access_acquired:
                    return
            with self._condition:
                record = self._jobs.get(job_id)
                if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                    return
                record.access_acquired = access_acquired

            self._start_and_monitor_process(job_id)
        finally:
            with self._condition:
                record = self._jobs.get(job_id)
                execution_mode = record.execution_mode if record is not None else QUERY_EXECUTION_DUCKDB_WRITE
            if access_acquired:
                self._access_coordinator.release(execution_mode)
            try:
                self._metadata_refresher()
            except Exception:
                pass

    def _start_and_monitor_process(self, job_id: str) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return
            event_queue = self._mp_context.Queue()
            cancel_event = self._mp_context.Event()
            if record.cancel_requested:
                cancel_event.set()
            process = self._mp_context.Process(
                target=_query_worker_entry,
                kwargs={
                    "event_queue": event_queue,
                    "cancel_event": cancel_event,
                    "settings": self._settings,
                    "sql": record.execution_sql,
                    "execution_mode": record.execution_mode,
                    "max_result_rows": self._max_result_rows,
                    "database_path": record.worker_database_path,
                },
                daemon=True,
                name=f"bdw-query-worker-{job_id[:8]}",
            )
            record.event_queue = event_queue
            record.cancel_event = cancel_event
            record.process = process
            record.snapshot.status = "running"
            record.snapshot.progress = None
            record.snapshot.progress_label = "Starting worker..."
            record.snapshot.message = "Starting isolated query worker."
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

        process.start()
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return
            record.snapshot.process_id = process.pid
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

        if psutil is not None and process.pid:
            with suppress(Exception):
                record.process_metrics = psutil.Process(process.pid)
                record.process_metrics.cpu_percent(interval=None)
                record.cpu_percent_initialized = True

        while process.is_alive():
            self._drain_worker_events(job_id)
            self._sample_process_metrics(job_id)
            self._apply_cancellation_pressure(job_id)
            self._log_query_heartbeat_if_due(job_id)
            time.sleep(QUERY_PARENT_POLL_SECONDS)

        process.join(timeout=0.2)
        self._drain_worker_events(job_id)
        self._sample_process_metrics(job_id, force=True)
        self._finalize_after_process_exit(job_id, process.exitcode)

        with self._condition:
            record = self._jobs.get(job_id)
            if record is not None:
                record.process = None
                record.cancel_event = None
                record.event_queue = None
            record.process_metrics = None

    @staticmethod
    def _worker_database_path(
        *,
        execution_mode: str,
        source_ids: list[str],
        touched_relations: list[str] | None,
        touched_buckets: list[str] | None,
    ) -> str | None:
        if execution_mode != QUERY_EXECUTION_DUCKDB_READ:
            return None
        if source_ids or touched_relations or touched_buckets:
            return None
        return ":memory:"

    def _drain_worker_events(self, job_id: str) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            event_queue = record.event_queue if record is not None else None
        if event_queue is None:
            return

        while True:
            try:
                event = event_queue.get_nowait()
            except queue.Empty:
                return
            except Exception:
                return
            if isinstance(event, dict):
                self._apply_worker_event(job_id, event)

    def _apply_worker_event(self, job_id: str, event: dict[str, Any]) -> None:
        event_type = str(event.get("type") or "").strip()
        if event_type == "final":
            status = str(event.get("status") or "failed")
            duration_ms = float(event.get("durationMs") or 0.0)
            duckdb_profile = event.get("duckdbProfile")
            if not isinstance(duckdb_profile, dict):
                duckdb_profile = {}
            changes = self._payload_changes(event)
            changes.pop("duration_ms", None)
            self._finalize_job(
                job_id,
                status=status,
                duration_ms=duration_ms,
                duckdb_profile=duckdb_profile,
                **changes,
            )
            with self._condition:
                record = self._jobs.get(job_id)
                if record is not None:
                    record.final_received = True
            return

        changes = self._payload_changes(event)
        if event_type == "started":
            changes.setdefault("progress_label", "Running...")
            changes.setdefault("message", "Running query in an isolated worker process.")
        if changes:
            self._patch_job(job_id, **changes)
        if event_type == "started":
            self._log_query_job_event(job_id, "worker_started")
        elif event_type == "columns":
            self._log_query_job_event(job_id, "fetching_rows")
        elif event_type == "cancellation":
            phase = str(event.get("cancellationPhase") or "").strip()
            if phase:
                self._log_query_job_event(job_id, f"cancel_{phase}")

    def _payload_changes(self, payload: dict[str, Any]) -> dict[str, Any]:
        mapping = {
            "processId": "process_id",
            "durationMs": "duration_ms",
            "progress": "progress",
            "progressLabel": "progress_label",
            "message": "message",
            "error": "error",
            "columns": "columns",
            "rows": "rows",
            "rowCount": "row_count",
            "rowsShown": "rows_shown",
            "truncated": "truncated",
            "firstRowMs": "first_row_ms",
            "fetchMs": "fetch_ms",
            "cancellationPhase": "cancellation_phase",
            "workerExitCode": "worker_exit_code",
        }
        changes: dict[str, Any] = {}
        for source_key, target_key in mapping.items():
            if source_key in payload:
                changes[target_key] = payload[source_key]
        return changes

    def _sample_process_metrics(self, job_id: str, *, force: bool = False) -> None:
        now_monotonic = time.monotonic()
        with self._condition:
            record = self._jobs.get(job_id)
            process_metrics = record.process_metrics if record is not None else None
            last_sample = record.last_metric_sample_monotonic if record is not None else 0.0
            if (
                record is None
                or record.snapshot.status in TERMINAL_QUERY_STATUSES
                or (not force and last_sample and now_monotonic - last_sample < QUERY_METRICS_SAMPLE_SECONDS)
            ):
                return
        if process_metrics is None or psutil is None:
            return

        try:
            cpu_percent = float(process_metrics.cpu_percent(interval=None))
            memory_rss = int(process_metrics.memory_info().rss)
            for child in process_metrics.children(recursive=True):
                with suppress(Exception):
                    memory_rss += int(child.memory_info().rss)
                    cpu_percent += float(child.cpu_percent(interval=None))
        except Exception:
            return

        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return
            if not force and record.last_metric_sample_monotonic and now_monotonic - record.last_metric_sample_monotonic < QUERY_METRICS_SAMPLE_SECONDS:
                return

            normalized_cpu_percent = max(0.0, cpu_percent)
            normalized_memory_rss = max(0, memory_rss)
            record.last_metric_sample_monotonic = now_monotonic
            record.cpu_sample_total += normalized_cpu_percent
            record.cpu_sample_count += 1
            record.memory_sample_total += normalized_memory_rss
            record.memory_sample_count += 1
            average_cpu = record.cpu_sample_total / max(1, record.cpu_sample_count)
            average_memory = int(record.memory_sample_total / max(1, record.memory_sample_count))

            record.snapshot.cpu_percent = normalized_cpu_percent
            record.snapshot.average_cpu_percent = average_cpu
            record.snapshot.peak_cpu_percent = max(
                float(record.snapshot.peak_cpu_percent or 0.0),
                normalized_cpu_percent,
            )
            record.snapshot.memory_rss_bytes = normalized_memory_rss
            record.snapshot.average_memory_rss_bytes = average_memory
            record.snapshot.peak_memory_rss_bytes = max(
                int(record.snapshot.peak_memory_rss_bytes or 0),
                normalized_memory_rss,
            )
            record.snapshot.resource_samples.append(
                QueryResourceSample(
                    elapsed_ms=self._elapsed_duration_ms_locked(record),
                    cpu_percent=normalized_cpu_percent,
                    average_cpu_percent=average_cpu,
                    memory_rss_bytes=normalized_memory_rss,
                    average_memory_rss_bytes=average_memory,
                )
            )
            if len(record.snapshot.resource_samples) > MAX_QUERY_RESOURCE_SAMPLES:
                record.snapshot.resource_samples = record.snapshot.resource_samples[-MAX_QUERY_RESOURCE_SAMPLES:]
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

    def _apply_cancellation_pressure(self, job_id: str) -> None:
        cancel_event = None
        process = None
        action: str | None = None
        phase_event: str | None = None
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or not record.cancel_requested or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return
            cancel_event = record.cancel_event
            process = record.process
            previous_phase = record.snapshot.cancellation_phase
            cancel_started = record.cancellation_started_monotonic or time.monotonic()
            record.cancellation_started_monotonic = cancel_started
            elapsed = time.monotonic() - cancel_started
            if cancel_event is not None and not cancel_event.is_set():
                cancel_event.set()
            if elapsed >= QUERY_INTERRUPT_GRACE_SECONDS + QUERY_TERMINATE_GRACE_SECONDS and not record.kill_sent:
                record.kill_sent = True
                action = "kill"
                record.snapshot.cancellation_phase = "killing"
                record.snapshot.message = "Hard-stopping the query worker process."
                record.snapshot.progress_label = "Cancelling..."
            elif elapsed >= QUERY_INTERRUPT_GRACE_SECONDS and not record.terminate_sent:
                record.terminate_sent = True
                action = "terminate"
                record.snapshot.cancellation_phase = "terminating"
                record.snapshot.message = "Stopping the query worker process."
                record.snapshot.progress_label = "Cancelling..."
            elif not record.snapshot.cancellation_phase or record.snapshot.cancellation_phase == "requested":
                record.snapshot.cancellation_phase = "interrupting"
                record.snapshot.message = "Interrupting the query worker."
                record.snapshot.progress_label = "Cancelling..."
            if record.snapshot.cancellation_phase and record.snapshot.cancellation_phase != previous_phase:
                phase_event = f"cancel_{record.snapshot.cancellation_phase}"
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

        if phase_event:
            self._log_query_job_event(job_id, phase_event)
        if process is not None and action == "terminate":
            with suppress(Exception):
                if process.is_alive():
                    process.terminate()
        elif process is not None and action == "kill":
            with suppress(Exception):
                if process.is_alive():
                    process.kill()

    def _finalize_after_process_exit(self, job_id: str, exit_code: int | None) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return
            if record.final_received:
                return
            cancelled = record.cancel_requested

        if cancelled:
            self._finalize_job(
                job_id,
                status="cancelled",
                duration_ms=self._elapsed_duration_ms(job_id),
                progress_label="Cancelled",
                message="Query cancellation completed.",
                cancellation_phase="cancelled",
                worker_exit_code=exit_code,
            )
        elif exit_code not in (0, None):
            self._finalize_job(
                job_id,
                status="failed",
                duration_ms=self._elapsed_duration_ms(job_id),
                progress_label="Failed",
                message="Query failed.",
                error=f"Query worker exited unexpectedly with code {exit_code}.",
                worker_exit_code=exit_code,
            )
        else:
            self._finalize_job(
                job_id,
                status="failed",
                duration_ms=self._elapsed_duration_ms(job_id),
                progress_label="Failed",
                message="Query failed.",
                error="The query worker exited without returning a result.",
                worker_exit_code=exit_code,
            )

    def _elapsed_duration_ms(self, job_id: str) -> float:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return 0.0
            return self._elapsed_duration_ms_locked(record)

    def _elapsed_duration_ms_locked(self, record: QueryJobRecord) -> float:
        started_at = record.snapshot.started_at if record is not None else ""
        try:
            started = datetime.fromisoformat(started_at)
            return max(0.0, (datetime.now(UTC) - started).total_seconds() * 1000)
        except Exception:
            return 0.0

    def _is_cancelled_or_terminal(self, job_id: str) -> bool:
        with self._condition:
            record = self._jobs.get(job_id)
            return bool(
                record is None
                or record.cancel_requested
                or record.snapshot.status in TERMINAL_QUERY_STATUSES
            )

    def _patch_job(self, job_id: str, **changes: Any) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return

            for key, value in changes.items():
                if key in {"columns", "rows"} and value is None:
                    continue
                setattr(record.snapshot, key, value)
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

    def _finalize_job(self, job_id: str, *, status: str, duration_ms: float, **changes: Any) -> None:
        duckdb_profile = changes.pop("duckdb_profile", None)
        if not isinstance(duckdb_profile, dict):
            duckdb_profile = {}
        terminal_payload: dict[str, Any] | None = None
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return

            completed_at = utc_now_iso()
            record.snapshot.status = status
            record.snapshot.duration_ms = duration_ms
            record.snapshot.completed_at = completed_at
            record.snapshot.updated_at = completed_at
            record.snapshot.can_cancel = False
            if status != "completed":
                record.snapshot.progress = None
            if status == "cancelled":
                record.snapshot.cancellation_phase = "cancelled"
                record.snapshot.message = record.snapshot.message or "Query cancellation completed."
            for key, value in changes.items():
                if key in {"columns", "rows"} and value is None:
                    continue
                setattr(record.snapshot, key, value)
            terminal_payload = record.snapshot.payload
            self._prune_history_locked()
            self._touch_locked()
        log_level = logging.WARNING if status == "failed" else logging.INFO
        self._log_query_job_event(
            job_id,
            status,
            level=log_level,
            extra=duckdb_profile,
        )
        if terminal_payload is not None and self._terminal_job_callback is not None:
            with suppress(Exception):
                self._terminal_job_callback(terminal_payload)

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

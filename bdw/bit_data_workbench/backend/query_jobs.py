from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import queue
import shutil
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
from .query_cache import hydrate_cache
from .query_options import cache_hydration_enabled
from .runtime_connections import create_duckdb_worker_connection, open_postgres_native_connection
from .runtime_storage import directory_size, parse_storage_size_bytes
from .sql_utils import qualified_name


logger = logging.getLogger(__name__)

RUNNING_QUERY_STATUSES = {"queued", "running"}
TERMINAL_QUERY_STATUSES = {"completed", "failed", "cancelled"}
MAX_QUERY_HISTORY = 80
QUERY_PROGRESS_POLL_SECONDS = 0.35
QUERY_PARENT_POLL_SECONDS = 0.1
QUERY_METRICS_SAMPLE_SECONDS = 1.0
MAX_QUERY_RESOURCE_SAMPLES = 180
MAX_QUERY_PROGRESS_EVENTS = 160
QUERY_INTERRUPT_GRACE_SECONDS = 1.5
QUERY_TERMINATE_GRACE_SECONDS = 1.5
QUERY_LOG_MAX_LIST_ITEMS = 5
QUERY_LOG_MAX_TEXT_CHARS = 320
QUERY_LOG_MAX_ERROR_CHARS = 500
QUERY_DUCKDB_PROFILE_MAX_OPERATORS = 8

QUERY_EXECUTION_DUCKDB_READ = "duckdb-read"
QUERY_EXECUTION_DUCKDB_WRITE = "duckdb-write"
QUERY_EXECUTION_POSTGRES_NATIVE = "postgres-native"
DUCKDB_EXECUTION_PATH_ISOLATED_READ = "isolated-read"
DUCKDB_EXECUTION_PATH_SHARED_FILE_READ = "shared-file-read"
DUCKDB_EXECUTION_PATH_SHARED_FILE_WRITE = "shared-file-write"
DUCKDB_EXECUTION_PATH_POSTGRES_NATIVE = "postgres-native"
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


def _safe_query_spill_directory_name(job_id: str) -> str:
    normalized = "".join(
        char if char.isalnum() or char in {"-", "_"} else "-"
        for char in str(job_id or "").strip()
    ).strip("-_")
    if not normalized.startswith("query-"):
        normalized = f"query-{normalized or uuid.uuid4().hex}"
    return normalized[:96]


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


def _utc_now_z() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


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


def _format_query_log_line(fields: dict[str, object]) -> str:
    timestamp = str(fields.get("query_job_time") or "").strip()
    prefix = f"{timestamp} [bdw-query]" if timestamp else "[bdw-query]"
    return f"{prefix} {_format_query_log_fields(fields)}"


def _safe_timing_ms(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric < 0:
        return None
    return round(numeric, 3)


def _read_positive_float(path: str) -> float | None:
    try:
        value = float(Path(path).read_text(encoding="utf-8").strip())
    except Exception:
        return None
    return value if value > 0 else None


def _cgroup_cpu_quota_cores() -> float | None:
    cpu_max_path = Path("/sys/fs/cgroup/cpu.max")
    if cpu_max_path.exists():
        try:
            quota_text, period_text, *_rest = cpu_max_path.read_text(encoding="utf-8").strip().split()
            if quota_text != "max":
                quota = float(quota_text)
                period = float(period_text)
                if quota > 0 and period > 0:
                    return max(0.001, quota / period)
        except Exception:
            pass

    quota = _read_positive_float("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period = _read_positive_float("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if quota is not None and period is not None:
        return max(0.001, quota / period)
    return None


def _effective_cpu_capacity_cores() -> float:
    quota_cores = _cgroup_cpu_quota_cores()
    if quota_cores is not None:
        return quota_cores

    if psutil is not None:
        with suppress(Exception):
            affinity = psutil.Process(os.getpid()).cpu_affinity()
            if affinity:
                return float(len(affinity))
        with suppress(Exception):
            count = psutil.cpu_count() or 0
            if count > 0:
                return float(count)

    return float(os.cpu_count() or 1)


def _cpu_capacity_percent(raw_cpu_percent: float, capacity_cores: float) -> float:
    cores = max(0.001, float(capacity_cores or 1.0))
    return max(0.0, float(raw_cpu_percent or 0.0) / cores)


def _capped_log_list(values: list[object]) -> tuple[list[object], int]:
    compact_values = [value for value in values if value not in (None, "", [], {})]
    return compact_values[:QUERY_LOG_MAX_LIST_ITEMS], max(0, len(compact_values) - QUERY_LOG_MAX_LIST_ITEMS)


def _progress_event_repeat_key(event: dict[str, object]) -> tuple[object, ...]:
    return (
        event.get("event"),
        event.get("status"),
        event.get("phase"),
        event.get("message"),
        event.get("progress_kind"),
        event.get("backend"),
        event.get("execution_mode"),
        event.get("duckdb_execution_path"),
        event.get("duckdb_progress_available"),
    )


def _merge_repeated_progress_event(
    existing: dict[str, object],
    incoming: dict[str, object],
) -> dict[str, object]:
    merged = dict(existing)
    previous_count = int(merged.get("occurrenceCount") or 1)
    if "firstOccurredAt" not in merged:
        merged["firstOccurredAt"] = merged.get("occurredAt")
    if "firstDisplayTime" not in merged:
        merged["firstDisplayTime"] = merged.get("displayTime")
    if "firstDurationMs" not in merged:
        merged["firstDurationMs"] = merged.get("durationMs")

    merged["occurrenceCount"] = previous_count + 1
    merged["lastOccurredAt"] = incoming.get("occurredAt")
    merged["lastDisplayTime"] = incoming.get("displayTime")
    merged["lastDurationMs"] = incoming.get("durationMs")

    for key, value in incoming.items():
        if key in {
            "occurredAt",
            "displayTime",
            "durationMs",
            "firstOccurredAt",
            "firstDisplayTime",
            "firstDurationMs",
            "occurrenceCount",
        }:
            continue
        merged[key] = value
    return merged


def _progress_events_with_preserved_edges(
    events: list[dict[str, object]],
    limit: int,
) -> list[dict[str, object]]:
    if len(events) <= limit:
        return events
    if limit <= 2:
        return events[:1] + events[-1:]

    head_count = min(len(events), max(1, min(max(8, limit // 4), limit - 2)))
    tail_count = max(1, limit - head_count - 1)
    if head_count + tail_count >= len(events):
        return events[:limit]

    omitted = events[head_count:-tail_count]
    first_omitted = omitted[0]
    last_omitted = omitted[-1]
    omitted_occurrences = sum(max(1, int(event.get("occurrenceCount") or 1)) for event in omitted)
    summary_event = {
        "occurredAt": first_omitted.get("occurredAt"),
        "displayTime": first_omitted.get("displayTime"),
        "lastOccurredAt": last_omitted.get("lastOccurredAt") or last_omitted.get("occurredAt"),
        "lastDisplayTime": last_omitted.get("lastDisplayTime") or last_omitted.get("displayTime"),
        "event": "progress_events_compacted",
        "status": last_omitted.get("status") or first_omitted.get("status"),
        "phase": "Progress events compacted",
        "message": (
            f"{omitted_occurrences} middle progress event(s) were summarized "
            "to preserve the first and latest records."
        ),
        "occurrenceCount": omitted_occurrences,
    }
    return events[:head_count] + [summary_event] + events[-tail_count:]


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


def _profile_node_float_ms(payload: dict[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return round(float(value) * 1000, 3)
    except (TypeError, ValueError):
        return None


def _walk_duckdb_profile_operators(payload: dict[str, Any]) -> list[dict[str, object]]:
    operators: list[dict[str, object]] = []

    def visit(node: object) -> None:
        if not isinstance(node, dict):
            return
        operator_name = str(node.get("operator_name") or "").strip()
        operator_type = str(node.get("operator_type") or "").strip()
        if operator_name or operator_type:
            operator: dict[str, object] = {
                "name": _truncate_log_text(operator_name or operator_type, 80),
                "type": _truncate_log_text(operator_type or operator_name, 80),
            }
            timing_ms = _profile_node_float_ms(node, "operator_timing")
            cpu_ms = _profile_node_float_ms(node, "cpu_time")
            rows_scanned = _profile_int(node, "operator_rows_scanned")
            cardinality = _profile_int(node, "operator_cardinality")
            cumulative_cardinality = _profile_int(node, "cumulative_cardinality")
            result_set_size = _profile_int(node, "result_set_size")
            bytes_read = _profile_int(node, "total_bytes_read")
            bytes_written = _profile_int(node, "total_bytes_written")
            if timing_ms is not None:
                operator["time_ms"] = timing_ms
            if cpu_ms is not None:
                operator["cpu_ms"] = cpu_ms
            if rows_scanned is not None:
                operator["rows_scanned"] = rows_scanned
            if cardinality is not None:
                operator["cardinality"] = cardinality
            if cumulative_cardinality is not None:
                operator["cumulative_cardinality"] = cumulative_cardinality
            if result_set_size is not None:
                operator["result_set_bytes"] = result_set_size
            if bytes_read is not None:
                operator["bytes_read"] = bytes_read
            if bytes_written is not None:
                operator["bytes_written"] = bytes_written
            operators.append(operator)
        for child in node.get("children") or []:
            visit(child)

    for child in payload.get("children") or []:
        visit(child)
    return operators


def _duckdb_profile_operator_summary(payload: dict[str, Any]) -> dict[str, object]:
    operators = _walk_duckdb_profile_operators(payload)
    if not operators:
        return {}

    operator_types = sorted(
        {
            str(operator.get("type") or operator.get("name") or "").strip()
            for operator in operators
            if str(operator.get("type") or operator.get("name") or "").strip()
        }
    )
    top_operators = sorted(
        operators,
        key=lambda operator: float(operator.get("time_ms") or operator.get("cpu_ms") or 0.0),
        reverse=True,
    )[:QUERY_DUCKDB_PROFILE_MAX_OPERATORS]
    return {
        "duckdb_operator_count": len(operators),
        "duckdb_operator_types": operator_types[:QUERY_DUCKDB_PROFILE_MAX_OPERATORS],
        "duckdb_top_operators": top_operators,
    }


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
            "duckdb_blocked_thread_ms": _profile_float_ms(payload, "blocked_thread_time"),
            "duckdb_rows_returned": _profile_int(payload, "rows_returned"),
            "duckdb_rows_scanned": _profile_int(payload, "cumulative_rows_scanned"),
            "duckdb_cumulative_cardinality": _profile_int(payload, "cumulative_cardinality"),
            "duckdb_result_set_bytes": _profile_int(payload, "result_set_size"),
            "duckdb_bytes_read": _profile_int(payload, "total_bytes_read"),
            "duckdb_bytes_written": _profile_int(payload, "total_bytes_written"),
            "duckdb_peak_buffer_memory_bytes": _profile_int(payload, "system_peak_buffer_memory"),
            "duckdb_peak_temp_dir_bytes": _profile_int(payload, "system_peak_temp_dir_size"),
        }
        compact_summary = {key: value for key, value in summary.items() if value is not None}
        compact_summary.update(_duckdb_profile_operator_summary(payload))
        return compact_summary
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
) -> tuple[QueryResult, float | None, float, float]:
    first_row_ms: float | None = None
    with connection.cursor() as cursor:
        query_started = time.perf_counter()
        cursor.execute(sql)
        columns = [column.name for column in (cursor.description or [])]
        engine_query_ms = (time.perf_counter() - query_started) * 1000
        _put_worker_event(
            event_queue,
            {
                "type": "columns",
                "columns": columns,
                "progressLabel": "Fetching rows..." if columns else "Finalizing...",
                "message": "Query is fetching rows..." if columns else "Statement executed successfully.",
                "timings": {"engineQueryMs": engine_query_ms},
            },
        )

        rows_buffer: list[tuple[Any, ...]] = []
        truncated = False
        row_count = 0
        message = "Statement executed successfully."
        fetch_started = time.perf_counter()

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
        result_fetch_ms = (time.perf_counter() - fetch_started) * 1000 if columns else 0.0

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
        engine_query_ms,
        result_fetch_ms,
    )


def _execute_duckdb_query(
    *,
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    max_result_rows: int,
    event_queue: Any,
    started: float,
) -> tuple[QueryResult, float | None, float, float]:
    first_row_ms: float | None = None
    query_started = time.perf_counter()
    cursor = connection.execute(sql)
    columns = [column[0] for column in cursor.description] if cursor.description else []
    engine_query_ms = (time.perf_counter() - query_started) * 1000
    _put_worker_event(
        event_queue,
        {
            "type": "columns",
            "columns": columns,
            "progressLabel": "Fetching rows..." if columns else "Finalizing...",
            "message": "Query is streaming rows..." if columns else "Statement executed successfully.",
            "timings": {"engineQueryMs": engine_query_ms},
        },
    )

    rows_buffer: list[tuple[Any, ...]] = []
    truncated = False
    row_count = 0
    message = "Statement executed successfully."
    fetch_started = time.perf_counter()

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
    result_fetch_ms = (time.perf_counter() - fetch_started) * 1000 if columns else 0.0

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
        engine_query_ms,
        result_fetch_ms,
    )


def _relation_parts(relation: object) -> tuple[str, ...]:
    parts = [
        part.strip().strip('"').strip("`").strip("[]")
        for part in str(relation or "").split(".")
        if part.strip()
    ]
    return tuple(parts)


def _normalize_relation_key(value: object) -> str:
    return ".".join(part.lower() for part in _relation_parts(value))


def _is_direct_file_relation(value: object) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized.startswith(("s3://", "http://", "https://", "file://"))


def _source_summary_has_query_sql(summary: dict[str, object]) -> bool:
    return bool(
        str(summary.get("relation") or "").strip()
        and str(summary.get("query_sql") or "").strip()
    )


def _bootstrap_duckdb_source_views(
    connection: duckdb.DuckDBPyConnection,
    source_summaries: list[dict[str, object]],
) -> None:
    for summary in source_summaries:
        if not isinstance(summary, dict):
            continue
        relation_parts = _relation_parts(summary.get("relation"))
        query_sql = str(summary.get("query_sql") or "").strip()
        if not relation_parts or not query_sql:
            continue
        if len(relation_parts) > 1:
            schema_parts = relation_parts[:-1]
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(*schema_parts)}")
        connection.execute(
            f"CREATE OR REPLACE VIEW {qualified_name(*relation_parts)} AS {query_sql}"
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
    temp_directory: str | None = None,
    source_summaries: list[dict[str, object]] | None = None,
    query_options: dict[str, object] | None = None,
    notebook_id: str = "",
    notebook_title: str = "",
    cell_id: str = "",
    sql_preview: str = "",
) -> None:
    started = time.perf_counter()
    connection: Any = None
    execution_result: QueryResult | None = None
    first_row_ms: float | None = None
    engine_query_ms: float = 0.0
    result_fetch_ms: float = 0.0
    execution_error: Exception | None = None
    duckdb_profile_path: Path | None = None
    cache_hydration_summary: dict[str, object] = {}

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
                temp_directory_override=temp_directory or None,
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

        worker_source_summaries = source_summaries or []
        if execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE and worker_source_summaries:
            worker_source_summaries, cache_hydration_summary = hydrate_cache(
                connection=connection,
                sql=sql,
                source_summaries=worker_source_summaries,
                query_options=query_options,
                settings=settings,
                cache_context={
                    "notebookId": notebook_id,
                    "notebookTitle": notebook_title,
                    "cellId": cell_id,
                    "sqlPreview": sql_preview or sql,
                },
                progress_callback=lambda event: _put_worker_event(event_queue, event),
            )

        if execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE and worker_source_summaries:
            _put_worker_event(
                event_queue,
                {
                    "type": "phase",
                    "phase": "preparing_sources",
                    "progressLabel": "Preparing query sources...",
                    "message": "Preparing isolated query sources.",
                },
            )
            bootstrap_started = time.perf_counter()
            _bootstrap_duckdb_source_views(connection, worker_source_summaries)
            _put_worker_event(
                event_queue,
                {
                    "type": "phase",
                    "phase": "sources_prepared",
                    "progressLabel": "Query sources ready.",
                    "message": "Isolated query sources are ready.",
                    "timings": {
                        "sourceBootstrapMs": (time.perf_counter() - bootstrap_started) * 1000,
                    },
                    "cacheHydration": cache_hydration_summary,
                },
            )

        def execute_query() -> None:
            nonlocal execution_result, first_row_ms, engine_query_ms, result_fetch_ms, execution_error
            try:
                if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE:
                    _put_worker_event(
                        event_queue,
                        {
                            "type": "phase",
                            "phase": "querying",
                            "progressLabel": "Querying...",
                            "message": "PostgreSQL is executing the statement.",
                        },
                    )
                    (
                        execution_result,
                        first_row_ms,
                        engine_query_ms,
                        result_fetch_ms,
                    ) = _execute_postgres_native_query(
                        connection=connection,
                        sql=sql,
                        max_result_rows=max_result_rows,
                        event_queue=event_queue,
                        started=started,
                    )
                else:
                    _put_worker_event(
                        event_queue,
                        {
                            "type": "phase",
                            "phase": "querying",
                            "progressLabel": "Querying...",
                            "message": "DuckDB is planning and executing the statement.",
                        },
                    )
                    (
                        execution_result,
                        first_row_ms,
                        engine_query_ms,
                        result_fetch_ms,
                    ) = _execute_duckdb_query(
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
            if progress is not None and progress >= 0.999 and execution_thread.is_alive():
                progress = None
            progress_label = "Cancelling..." if cancel_event.is_set() else "Running..."
            if progress is not None and not cancel_event.is_set():
                progress_label = f"Running... {progress * 100:.0f}%"
            duckdb_progress = (
                {
                    "available": True,
                    "fraction": progress,
                    "percent": round(progress * 100, 1),
                }
                if progress is not None
                else {"available": False}
                if execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE
                else {}
            )
            _put_worker_event(
                event_queue,
                {
                    "type": "progress",
                    "durationMs": duration_ms,
                    "progress": progress,
                    "progressLabel": progress_label,
                    "duckdbProgress": duckdb_progress,
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
                "timings": {
                    "engineQueryMs": engine_query_ms,
                    "resultFetchMs": result_fetch_ms,
                },
                "cacheHydration": cache_hydration_summary,
            },
            duckdb_profile_summary,
        )
    except Exception as exc:
        if (
            not cache_hydration_summary
            and execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE
            and source_summaries
            and cache_hydration_enabled(query_options)
        ):
            cache_hydration_summary = {
                "enabled": True,
                "status": "error",
                "sources": [],
                "statusLabel": "Error",
                "statusReason": str(exc),
            }
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
                "cacheHydration": cache_hydration_summary,
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
        self._read_owners: dict[str, float] = {}
        self._write_owner_job_id = ""
        self._write_owner_started_monotonic: float | None = None

    def acquire(
        self,
        execution_mode: str,
        is_cancelled: Any,
        *,
        on_waiting: Callable[[], None] | None = None,
        owner_job_id: str = "",
    ) -> bool:
        if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE:
            return True

        with self._condition:
            if execution_mode == QUERY_EXECUTION_DUCKDB_READ:
                while self._active_write:
                    if is_cancelled():
                        return False
                    if on_waiting is not None:
                        on_waiting()
                    self._condition.wait(timeout=0.1)
                self._active_reads += 1
                normalized_owner = str(owner_job_id or "").strip()
                if normalized_owner:
                    self._read_owners[normalized_owner] = time.monotonic()
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
                self._write_owner_job_id = str(owner_job_id or "").strip()
                self._write_owner_started_monotonic = time.monotonic()
                return True
            finally:
                self._waiting_writes = max(0, self._waiting_writes - 1)

    def release(self, execution_mode: str, *, owner_job_id: str = "") -> None:
        if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE:
            return
        with self._condition:
            if execution_mode == QUERY_EXECUTION_DUCKDB_READ:
                normalized_owner = str(owner_job_id or "").strip()
                if normalized_owner and normalized_owner in self._read_owners:
                    self._read_owners.pop(normalized_owner, None)
                    self._active_reads = max(0, self._active_reads - 1)
                elif not normalized_owner:
                    self._active_reads = max(0, self._active_reads - 1)
                    if self._active_reads == 0:
                        self._read_owners.clear()
            else:
                self._active_write = False
                self._write_owner_job_id = ""
                self._write_owner_started_monotonic = None
            self._condition.notify_all()

    def force_release_owner(self, owner_job_id: str) -> bool:
        normalized_owner = str(owner_job_id or "").strip()
        if not normalized_owner:
            return False
        with self._condition:
            released = False
            if self._write_owner_job_id == normalized_owner and self._active_write:
                self._active_write = False
                self._write_owner_job_id = ""
                self._write_owner_started_monotonic = None
                released = True
            if normalized_owner in self._read_owners:
                self._read_owners.pop(normalized_owner, None)
                self._active_reads = max(0, self._active_reads - 1)
                released = True
            if released:
                self._condition.notify_all()
            return released

    def state(self) -> dict[str, object]:
        with self._condition:
            now_monotonic = time.monotonic()
            write_owner_age_ms = (
                (now_monotonic - self._write_owner_started_monotonic) * 1000
                if self._write_owner_started_monotonic is not None
                else None
            )
            read_owner_ages = {
                owner: (now_monotonic - started) * 1000
                for owner, started in self._read_owners.items()
            }
            return {
                "active_reads": self._active_reads,
                "active_write": self._active_write,
                "waiting_writes": self._waiting_writes,
                "write_owner_job_id": self._write_owner_job_id,
                "write_owner_age_ms": write_owner_age_ms,
                "read_owner_job_ids": sorted(self._read_owners),
                "read_owner_ages_ms": read_owner_ages,
            }


@dataclass(slots=True)
class QueryJobRecord:
    snapshot: QueryJobDefinition
    sort_index: int
    execution_mode: str
    execution_sql: str
    duckdb_execution_path: str = ""
    worker_database_path: str | None = None
    spill_temp_directory: Path | None = None
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
    process_start_monotonic: float = 0.0
    cpu_percent_initialized: bool = False
    last_metric_sample_monotonic: float = 0.0
    cpu_sample_total: float = 0.0
    cpu_sample_count: int = 0
    cpu_capacity_sample_total: float = 0.0
    cpu_capacity_sample_count: int = 0
    memory_sample_total: int = 0
    memory_sample_count: int = 0
    final_received: bool = False
    access_acquired: bool = False
    last_log_heartbeat_monotonic: float = 0.0
    logged_events: set[str] = field(default_factory=set)
    latest_duckdb_progress: dict[str, object] = field(default_factory=dict)
    last_wait_coordinator_fields: dict[str, object] = field(default_factory=dict)


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
        self._cpu_capacity_cores = _effective_cpu_capacity_cores()
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
        query_options: dict[str, object] | None = None,
        client_pre_submit_ms: float | None = None,
        backend_prepare_ms: float | None = None,
    ) -> QueryJobDefinition:
        normalized_sql = sql.strip()
        if not normalized_sql:
            raise ValueError("Provide a SQL statement before running the query.")

        source_ids = [source_id.strip() for source_id in (data_sources or []) if source_id.strip()]
        source_types = infer_source_types(source_ids)
        normalized_execution_sql = str(execution_sql or sql or "").strip()
        execution_mode = classify_query_execution(normalized_execution_sql, source_ids)
        normalized_touched_relations = [
            str(value).strip()
            for value in (touched_relations or [])
            if str(value).strip()
        ]
        normalized_touched_buckets = [
            str(value).strip()
            for value in (touched_buckets or [])
            if str(value).strip()
        ]
        normalized_source_summaries = [
            dict(item)
            for item in (source_summaries or [])
            if isinstance(item, dict)
        ]
        duckdb_execution_path = self._duckdb_execution_path(
            execution_mode=execution_mode,
            source_ids=source_ids,
            touched_relations=normalized_touched_relations,
            touched_buckets=normalized_touched_buckets,
            source_summaries=normalized_source_summaries,
        )
        worker_database_path = self._worker_database_path(
            execution_mode=execution_mode,
            duckdb_execution_path=duckdb_execution_path,
        )
        now = utc_now_iso()
        resolved_title = notebook_title.strip() or self._notebook_title_resolver(notebook_id) or "Notebook"
        backend_name = "PostgreSQL Native" if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE else "VMTP DUCKDB"
        duckdb_thread_limit = (
            max(1, int(self._settings.duckdb_threads))
            if execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE
            and self._settings.duckdb_threads is not None
            else None
        )
        timings: dict[str, float] = {}
        for key, value in (
            ("clientPreSubmitMs", client_pre_submit_ms),
            ("backendPrepareMs", backend_prepare_ms),
        ):
            timing = _safe_timing_ms(value)
            if timing is not None:
                timings[key] = timing
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
            query_options=dict(query_options or {}),
            source_types=source_types,
            touched_relations=normalized_touched_relations,
            touched_buckets=normalized_touched_buckets,
            backend_name=backend_name,
            execution_mode=execution_mode,
            duckdb_execution_path=duckdb_execution_path,
            cpu_capacity_cores=self._cpu_capacity_cores,
            duckdb_thread_limit=duckdb_thread_limit,
            timings=timings,
            can_cancel=True,
        )

        with self._condition:
            self._sort_counter += 1
            record = QueryJobRecord(
                snapshot=snapshot,
                sort_index=self._sort_counter,
                execution_mode=execution_mode,
                execution_sql=normalized_execution_sql,
                duckdb_execution_path=duckdb_execution_path,
                worker_database_path=worker_database_path,
                source_summaries=normalized_source_summaries,
                last_log_heartbeat_monotonic=time.monotonic(),
            )
            self._jobs[snapshot.job_id] = record
            self._touch_locked()

        self._log_query_job_event(snapshot.job_id, "queued")
        self._log_query_job_event(snapshot.job_id, "backend_prepared")
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
        if queued_cancel_snapshot is not None:
            self._log_query_job_event(job_id, "cancelled")
            with self._condition:
                record = self._jobs.get(job_id)
                terminal_payload = record.snapshot.payload if record is not None else None
        if terminal_payload is not None and self._terminal_job_callback is not None:
            with suppress(Exception):
                self._terminal_job_callback(terminal_payload)
        if queued_cancel_snapshot is not None:
            return queued_cancel_snapshot

        if cancel_event is not None:
            with suppress(Exception):
                cancel_event.set()

        with self._condition:
            return self._jobs[job_id].snapshot

    def record_client_timing(
        self,
        job_id: str,
        *,
        client_total_ms: float | None = None,
    ) -> QueryJobDefinition:
        terminal_payload: dict[str, Any] | None = None
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown query job: {job_id}")
            self._set_timing_locked(record, "clientTotalMs", client_total_ms)
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()
            snapshot = record.snapshot
            if snapshot.status in TERMINAL_QUERY_STATUSES:
                terminal_payload = snapshot.payload

        if terminal_payload is not None and self._terminal_job_callback is not None:
            with suppress(Exception):
                self._terminal_job_callback(terminal_payload)
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

    def query_runs_payloads(
        self,
        *,
        notebook_id: str = "",
        cell_id: str = "",
        status: str = "",
        live_only: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_notebook_id = str(notebook_id or "").strip()
        normalized_cell_id = str(cell_id or "").strip()
        normalized_status = str(status or "").strip().lower()
        normalized_limit = max(1, min(500, int(limit or 100)))
        with self._condition:
            records = sorted(
                self._jobs.values(),
                key=lambda record: (
                    record.snapshot.updated_at or record.snapshot.started_at,
                    record.snapshot.job_id,
                ),
                reverse=True,
            )
            payloads: list[dict[str, Any]] = []
            for record in records:
                snapshot = record.snapshot
                if snapshot.status not in RUNNING_QUERY_STATUSES:
                    continue
                if live_only and snapshot.status not in RUNNING_QUERY_STATUSES:
                    continue
                if normalized_notebook_id and snapshot.notebook_id != normalized_notebook_id:
                    continue
                if normalized_cell_id and snapshot.cell_id != normalized_cell_id:
                    continue
                if normalized_status and snapshot.status != normalized_status:
                    continue
                payload = snapshot.payload
                payload["columns"] = []
                payload["rows"] = []
                payload["live"] = True
                payloads.append(payload)
                if len(payloads) >= normalized_limit:
                    break
            return payloads

    def _log_query_job_event(
        self,
        job_id: str,
        event: str,
        *,
        level: int = logging.INFO,
        extra: dict[str, object] | None = None,
        once: bool = True,
    ) -> None:
        should_log = bool(getattr(self._settings, "query_job_logging_enabled", True))
        fields: dict[str, object] | None = None
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return
            if once and event in record.logged_events:
                return
            if once:
                record.logged_events.add(event)
            fields = self._query_job_log_fields(record, event=event, extra=extra or {})
            self._append_progress_event_locked(record, event=event, fields=fields)
            self._touch_locked()
        if should_log and fields is not None:
            logger.log(level, "%s", _format_query_log_line(fields))

    def _log_query_heartbeat_if_due(self, job_id: str) -> None:
        if not bool(getattr(self._settings, "query_job_logging_enabled", True)):
            should_log = False
        else:
            should_log = True
        now_monotonic = time.monotonic()
        fields: dict[str, object] | None = None
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
            fields = self._query_job_log_fields(
                record,
                event="progress",
                extra={"progress_kind": "heartbeat", **self._coordinator_log_fields()},
            )
            self._append_progress_event_locked(record, event="progress", fields=fields)
            self._touch_locked()
        if should_log and fields is not None:
            logger.info("%s", _format_query_log_line(fields))

    def _append_progress_event_locked(
        self,
        record: QueryJobRecord,
        *,
        event: str,
        fields: dict[str, object],
    ) -> None:
        snapshot = record.snapshot
        progress_event: dict[str, object] = {
            "occurredAt": _utc_now_z(),
            "displayTime": str(fields.get("query_job_time") or ""),
            "event": event,
            "status": snapshot.status,
            "phase": str(fields.get("progress_label") or snapshot.progress_label or ""),
            "message": _truncate_log_text(snapshot.message or fields.get("message") or ""),
            "durationMs": snapshot.duration_ms or self._elapsed_duration_ms_locked(record),
            "progress": snapshot.progress,
        }
        if snapshot.timings:
            progress_event["timings"] = dict(snapshot.timings)
        for key in (
            "job_id",
            "notebook_id",
            "notebook_title",
            "cell_id",
            "backend",
            "execution_mode",
            "duckdb_execution_path",
            "process_id",
            "elapsed_seconds",
            "cpu_percent",
            "cpu_capacity_percent",
            "cpu_capacity_cores",
            "process_thread_count",
            "duckdb_thread_limit",
            "ram_mb",
            "row_count",
            "rows_shown",
            "truncated",
            "worker_exit_code",
            "cancellation_phase",
            "error",
            "progress_kind",
            "data_sources",
            "touched_relations",
            "touched_buckets",
            "s3_sources",
            "duckdb_progress_fraction",
            "duckdb_progress_percent",
            "duckdb_progress_available",
            "duckdb_coordinator_active_reads",
            "duckdb_coordinator_active_write",
            "duckdb_coordinator_waiting_writes",
            "duckdb_lock_owner_job_id",
            "duckdb_lock_owner_mode",
            "duckdb_lock_owner_age_ms",
            "duckdb_lock_queue_depth",
        ):
            value = fields.get(key)
            if value not in (None, "", [], {}):
                progress_event[key] = value
        duckdb_profile = {
            key: value
            for key, value in fields.items()
            if key.startswith("duckdb_")
            and key
            not in {
                "duckdb_progress_fraction",
                "duckdb_progress_percent",
                "duckdb_progress_available",
                "duckdb_execution_path",
                "duckdb_coordinator_active_reads",
                "duckdb_coordinator_active_write",
                "duckdb_coordinator_waiting_writes",
                "duckdb_lock_owner_job_id",
                "duckdb_lock_owner_mode",
                "duckdb_lock_owner_age_ms",
                "duckdb_lock_queue_depth",
            }
            and value not in (None, "", [], {})
        }
        if duckdb_profile:
            progress_event["duckdbProfile"] = duckdb_profile
        if (
            snapshot.progress_events
            and _progress_event_repeat_key(snapshot.progress_events[-1])
            == _progress_event_repeat_key(progress_event)
        ):
            snapshot.progress_events[-1] = _merge_repeated_progress_event(
                snapshot.progress_events[-1],
                progress_event,
            )
        else:
            snapshot.progress_events.append(progress_event)
        if len(snapshot.progress_events) > MAX_QUERY_PROGRESS_EVENTS:
            snapshot.progress_events = _progress_events_with_preserved_edges(
                snapshot.progress_events,
                MAX_QUERY_PROGRESS_EVENTS,
            )

    def _set_timing_locked(self, record: QueryJobRecord, key: str, value: object) -> None:
        timing = _safe_timing_ms(value)
        if timing is None:
            return
        record.snapshot.timings[str(key)] = timing

    def _merge_timings_locked(self, record: QueryJobRecord, timings: dict[str, object]) -> None:
        for key, value in (timings or {}).items():
            self._set_timing_locked(record, str(key), value)

    def _coordinator_log_fields(self) -> dict[str, object]:
        state = self._access_coordinator.state()
        fields: dict[str, object] = {
            "duckdb_coordinator_active_reads": state.get("active_reads"),
            "duckdb_coordinator_active_write": state.get("active_write"),
            "duckdb_coordinator_waiting_writes": state.get("waiting_writes"),
            "duckdb_lock_queue_depth": state.get("waiting_writes"),
        }
        write_owner = str(state.get("write_owner_job_id") or "").strip()
        if write_owner:
            fields["duckdb_lock_owner_job_id"] = write_owner
            fields["duckdb_lock_owner_mode"] = QUERY_EXECUTION_DUCKDB_WRITE
            owner_age_ms = _safe_timing_ms(state.get("write_owner_age_ms"))
            if owner_age_ms is not None:
                fields["duckdb_lock_owner_age_ms"] = owner_age_ms
        read_owners = state.get("read_owner_job_ids")
        if isinstance(read_owners, list) and read_owners and not write_owner:
            fields["duckdb_lock_owner_job_id"] = read_owners[0]
            fields["duckdb_lock_owner_mode"] = QUERY_EXECUTION_DUCKDB_READ
            read_ages = state.get("read_owner_ages_ms")
            if isinstance(read_ages, dict):
                owner_age_ms = _safe_timing_ms(read_ages.get(read_owners[0]))
                if owner_age_ms is not None:
                    fields["duckdb_lock_owner_age_ms"] = owner_age_ms
        return fields

    def _apply_coordinator_state_locked(
        self,
        record: QueryJobRecord,
        state: dict[str, object],
    ) -> None:
        write_owner = str(state.get("write_owner_job_id") or "").strip()
        waiting_writes = state.get("waiting_writes")
        if write_owner:
            record.snapshot.duckdb_lock_owner_job_id = write_owner
            record.snapshot.duckdb_lock_owner_mode = QUERY_EXECUTION_DUCKDB_WRITE
            owner_age_ms = _safe_timing_ms(state.get("write_owner_age_ms"))
            record.snapshot.duckdb_lock_owner_age_ms = owner_age_ms
        else:
            read_owners = state.get("read_owner_job_ids")
            if isinstance(read_owners, list) and read_owners:
                owner = str(read_owners[0] or "").strip()
                record.snapshot.duckdb_lock_owner_job_id = owner
                record.snapshot.duckdb_lock_owner_mode = QUERY_EXECUTION_DUCKDB_READ
                read_ages = state.get("read_owner_ages_ms")
                owner_age_ms = None
                if isinstance(read_ages, dict):
                    owner_age_ms = _safe_timing_ms(read_ages.get(owner))
                record.snapshot.duckdb_lock_owner_age_ms = owner_age_ms
            else:
                record.snapshot.duckdb_lock_owner_job_id = ""
                record.snapshot.duckdb_lock_owner_mode = ""
                record.snapshot.duckdb_lock_owner_age_ms = None
        try:
            record.snapshot.duckdb_lock_queue_depth = int(waiting_writes or 0)
        except (TypeError, ValueError):
            record.snapshot.duckdb_lock_queue_depth = None

    def _recover_stale_coordinator_owner(self, waiting_job_id: str) -> None:
        state = self._access_coordinator.state()
        owner_job_id = str(state.get("write_owner_job_id") or "").strip()
        if not owner_job_id or owner_job_id == waiting_job_id:
            return

        should_release = False
        with self._condition:
            owner_record = self._jobs.get(owner_job_id)
            if owner_record is None:
                return
            owner_process = owner_record.process
            if owner_record.snapshot.status in TERMINAL_QUERY_STATUSES:
                should_release = True
            elif owner_process is not None:
                with suppress(Exception):
                    should_release = not bool(owner_process.is_alive())

        if not should_release:
            return
        if self._access_coordinator.force_release_owner(owner_job_id):
            self._log_query_job_event(
                waiting_job_id,
                "duckdb_lock_recovered",
                level=logging.WARNING,
                extra={"duckdb_lock_owner_job_id": owner_job_id},
            )

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
            "duckdb_execution_path": snapshot.duckdb_execution_path,
        }
        if snapshot.process_id is not None:
            fields["process_id"] = snapshot.process_id
        if snapshot.duckdb_lock_owner_job_id:
            fields["duckdb_lock_owner_job_id"] = snapshot.duckdb_lock_owner_job_id
        if snapshot.duckdb_lock_owner_mode:
            fields["duckdb_lock_owner_mode"] = snapshot.duckdb_lock_owner_mode
        if snapshot.duckdb_lock_owner_age_ms is not None:
            fields["duckdb_lock_owner_age_ms"] = round(float(snapshot.duckdb_lock_owner_age_ms), 3)
        if snapshot.duckdb_lock_queue_depth is not None:
            fields["duckdb_lock_queue_depth"] = snapshot.duckdb_lock_queue_depth
        if snapshot.duration_ms:
            fields["duration_ms"] = round(float(snapshot.duration_ms), 3)
        if snapshot.progress_label:
            fields["progress_label"] = snapshot.progress_label
        if snapshot.cancellation_phase:
            fields["cancellation_phase"] = snapshot.cancellation_phase
        if (
            snapshot.progress is not None
            and event == "progress"
            and snapshot.status == "running"
            and snapshot.execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE
        ):
            fields["duckdb_progress_fraction"] = round(float(snapshot.progress), 4)
            fields["duckdb_progress_percent"] = round(float(snapshot.progress) * 100, 1)
            fields["duckdb_progress_available"] = True
        elif event == "progress" and snapshot.execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE:
            fields["duckdb_progress_available"] = False
        if snapshot.cpu_percent is not None and event == "progress":
            fields["cpu_percent"] = round(float(snapshot.cpu_percent), 2)
        if snapshot.cpu_capacity_percent is not None and event == "progress":
            fields["cpu_capacity_percent"] = round(float(snapshot.cpu_capacity_percent), 2)
        if snapshot.cpu_capacity_cores is not None and event == "progress":
            fields["cpu_capacity_cores"] = round(float(snapshot.cpu_capacity_cores), 3)
        if snapshot.process_thread_count is not None and event == "progress":
            fields["process_thread_count"] = int(snapshot.process_thread_count)
        if snapshot.duckdb_thread_limit is not None and event == "progress":
            fields["duckdb_thread_limit"] = int(snapshot.duckdb_thread_limit)
        if snapshot.memory_rss_bytes is not None and event == "progress":
            fields["ram_mb"] = round(float(snapshot.memory_rss_bytes) / (1024 * 1024), 1)
        if event == "progress":
            fields["elapsed_seconds"] = round(self._elapsed_duration_ms_locked(record) / 1000, 1)
        for timing_key, log_key in (
            ("clientTotalMs", "client_total_ms"),
            ("clientPreSubmitMs", "client_pre_submit_ms"),
            ("backendPrepareMs", "backend_prepare_ms"),
            ("engineAccessWaitMs", "engine_access_wait_ms"),
            ("sourceBootstrapMs", "source_bootstrap_ms"),
            ("workerStartupMs", "worker_startup_ms"),
            ("engineQueryMs", "engine_query_ms"),
            ("resultFetchMs", "result_fetch_ms"),
            ("backendTotalMs", "backend_total_ms"),
        ):
            value = snapshot.timings.get(timing_key)
            if value is not None:
                fields[log_key] = round(float(value), 3)

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
        for key in ("relation", "query_reference", "query_alias", "bucket", "key", "path", "format"):
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
                record.snapshot.progress_label = "Preparing query..."
                record.snapshot.message = "Preparing query execution."
                self._touch_locked()

            requires_duckdb_file_access = record.duckdb_execution_path in {
                DUCKDB_EXECUTION_PATH_SHARED_FILE_READ,
                DUCKDB_EXECUTION_PATH_SHARED_FILE_WRITE,
            }
            if requires_duckdb_file_access:
                wait_started = time.monotonic()
                wait_reported = False

                def on_waiting() -> None:
                    nonlocal wait_reported
                    wait_reported = True
                    wait_fields = self._coordinator_log_fields()
                    self._recover_stale_coordinator_owner(job_id)
                    with self._condition:
                        waiting_record = self._jobs.get(job_id)
                        if waiting_record is None or waiting_record.snapshot.status in TERMINAL_QUERY_STATUSES:
                            return
                        waiting_record.snapshot.progress_label = "Waiting for DuckDB access..."
                        waiting_record.snapshot.message = "Waiting for DuckDB file access to become available."
                        self._apply_coordinator_state_locked(waiting_record, self._access_coordinator.state())
                        waiting_record.last_wait_coordinator_fields = dict(wait_fields)
                        waiting_record.snapshot.updated_at = utc_now_iso()
                        self._touch_locked()
                    self._log_query_heartbeat_if_due(job_id)

                access_acquired = self._access_coordinator.acquire(
                    record.execution_mode,
                    lambda: self._is_cancelled_or_terminal(job_id),
                    on_waiting=on_waiting,
                    owner_job_id=job_id,
                )
                if not access_acquired:
                    return
                access_wait_ms = (time.monotonic() - wait_started) * 1000 if wait_reported else 0.0
            else:
                access_wait_ms = 0.0
            with self._condition:
                record = self._jobs.get(job_id)
                if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                    return
                record.access_acquired = access_acquired
                self._set_timing_locked(record, "engineAccessWaitMs", access_wait_ms)
                self._apply_coordinator_state_locked(record, self._access_coordinator.state())
                record.snapshot.updated_at = utc_now_iso()
                self._touch_locked()

            if requires_duckdb_file_access and access_wait_ms > 0:
                with self._condition:
                    record = self._jobs.get(job_id)
                    wait_fields = (
                        dict(record.last_wait_coordinator_fields)
                        if record is not None and record.last_wait_coordinator_fields
                        else self._coordinator_log_fields()
                    )
                self._log_query_job_event(
                    job_id,
                    "engine_waiting",
                    extra=wait_fields,
                )
            self._log_query_job_event(
                job_id,
                "engine_allocated",
                extra=self._coordinator_log_fields(),
            )

            self._start_and_monitor_process(job_id)
        finally:
            with self._condition:
                record = self._jobs.get(job_id)
                execution_mode = record.execution_mode if record is not None else QUERY_EXECUTION_DUCKDB_WRITE
            if access_acquired:
                self._access_coordinator.release(execution_mode, owner_job_id=job_id)
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
            worker_source_summaries = record.source_summaries
            if (
                record.execution_mode == QUERY_EXECUTION_DUCKDB_READ
                and record.duckdb_execution_path != DUCKDB_EXECUTION_PATH_ISOLATED_READ
            ):
                worker_source_summaries = []
            spill_temp_directory: Path | None = None
            if (
                record.execution_mode != QUERY_EXECUTION_POSTGRES_NATIVE
                and self._settings.duckdb_temp_directory is not None
            ):
                spill_root = Path(self._settings.duckdb_temp_directory)
                spill_temp_directory = spill_root / _safe_query_spill_directory_name(job_id)
                with suppress(Exception):
                    spill_temp_directory.mkdir(parents=True, exist_ok=True)
            process = self._mp_context.Process(
                target=_query_worker_entry,
                kwargs={
                    "event_queue": event_queue,
                    "cancel_event": cancel_event,
                    "settings": self._settings,
                    "sql": record.execution_sql,
                    "notebook_id": record.snapshot.notebook_id,
                    "notebook_title": record.snapshot.notebook_title,
                    "cell_id": record.snapshot.cell_id,
                    "sql_preview": record.snapshot.sql,
                    "execution_mode": record.execution_mode,
                    "max_result_rows": self._max_result_rows,
                    "database_path": record.worker_database_path,
                    "temp_directory": spill_temp_directory.as_posix()
                    if spill_temp_directory is not None
                    else None,
                    "source_summaries": worker_source_summaries,
                    "query_options": record.snapshot.query_options,
                },
                daemon=True,
                name=f"bdw-query-worker-{job_id[:8]}",
            )
            record.event_queue = event_queue
            record.cancel_event = cancel_event
            record.process = process
            record.spill_temp_directory = spill_temp_directory
            record.snapshot.status = "running"
            record.snapshot.progress = None
            record.snapshot.progress_label = "Starting isolated query worker..."
            record.snapshot.message = "Starting isolated query worker."
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

        self._log_query_job_event(job_id, "worker_starting")
        process_start_monotonic = time.monotonic()
        process.start()
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return
            record.process_start_monotonic = process_start_monotonic
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
        self._sample_process_metrics(job_id, force=True)
        self._drain_worker_events(job_id)
        self._sample_process_metrics(job_id, force=True)
        self._finalize_after_process_exit(job_id, process.exitcode)

        with self._condition:
            record = self._jobs.get(job_id)
            if record is not None:
                spill_temp_directory = record.spill_temp_directory
                record.process = None
                record.cancel_event = None
                record.event_queue = None
                record.process_metrics = None
            else:
                spill_temp_directory = None

        self._cleanup_query_spill_directory(spill_temp_directory)

    def _cleanup_query_spill_directory(self, spill_temp_directory: Path | None) -> None:
        if spill_temp_directory is None or self._settings.duckdb_temp_directory is None:
            return
        try:
            spill_root = Path(self._settings.duckdb_temp_directory).resolve()
            target = Path(spill_temp_directory).resolve()
            if target == spill_root or spill_root not in target.parents:
                return
            if not target.name.startswith("query-"):
                return
            shutil.rmtree(target, ignore_errors=True)
        except Exception:
            logger.debug(
                "Failed to clean query spill directory %s",
                spill_temp_directory,
                exc_info=True,
            )

    @staticmethod
    def _duckdb_execution_path(
        *,
        execution_mode: str,
        source_ids: list[str],
        touched_relations: list[str] | None,
        touched_buckets: list[str] | None,
        source_summaries: list[dict[str, object]] | None,
    ) -> str:
        if execution_mode == QUERY_EXECUTION_POSTGRES_NATIVE:
            return DUCKDB_EXECUTION_PATH_POSTGRES_NATIVE
        if execution_mode != QUERY_EXECUTION_DUCKDB_READ:
            return DUCKDB_EXECUTION_PATH_SHARED_FILE_WRITE

        normalized_touched_relations = [
            str(relation or "").strip()
            for relation in (touched_relations or [])
            if str(relation or "").strip()
        ]
        if not source_ids and not normalized_touched_relations and not touched_buckets:
            return DUCKDB_EXECUTION_PATH_ISOLATED_READ

        relation_manifest = {
            _normalize_relation_key(summary.get("relation"))
            for summary in (source_summaries or [])
            if isinstance(summary, dict) and _source_summary_has_query_sql(summary)
        }
        missing_relations = [
            relation
            for relation in normalized_touched_relations
            if not _is_direct_file_relation(relation)
            and _normalize_relation_key(relation) not in relation_manifest
        ]
        if missing_relations:
            return DUCKDB_EXECUTION_PATH_SHARED_FILE_READ
        return DUCKDB_EXECUTION_PATH_ISOLATED_READ

    @staticmethod
    def _worker_database_path(
        *,
        execution_mode: str,
        duckdb_execution_path: str,
    ) -> str | None:
        if execution_mode != QUERY_EXECUTION_DUCKDB_READ:
            return None
        if duckdb_execution_path == DUCKDB_EXECUTION_PATH_ISOLATED_READ:
            return ":memory:"
        return None

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

        duckdb_progress = event.get("duckdbProgress")
        if isinstance(duckdb_progress, dict):
            with self._condition:
                record = self._jobs.get(job_id)
                if record is not None:
                    record.latest_duckdb_progress = dict(duckdb_progress)

        changes = self._payload_changes(event)
        if event_type == "started":
            changes.setdefault("progress_label", "Running...")
            changes.setdefault("message", "Running query in an isolated worker process.")
            with self._condition:
                record = self._jobs.get(job_id)
                if record is not None and record.process_start_monotonic:
                    self._set_timing_locked(
                        record,
                        "workerStartupMs",
                        (time.monotonic() - record.process_start_monotonic) * 1000,
                    )
        if event_type == "phase":
            changes.setdefault("progress_label", str(event.get("progressLabel") or "Querying..."))
            changes.setdefault("message", str(event.get("message") or "Query is running."))
        if changes:
            self._patch_job(job_id, **changes)
        if event_type == "started":
            self._log_query_job_event(job_id, "worker_started")
        elif event_type == "phase":
            self._log_query_job_event(job_id, str(event.get("phase") or "querying").strip() or "querying")
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
            "timings": "timings",
            "cacheHydration": "cache_hydration",
        }
        changes: dict[str, Any] = {}
        for source_key, target_key in mapping.items():
            if source_key in payload:
                changes[target_key] = payload[source_key]
        return changes

    def _spill_metrics_for_record(self, record: QueryJobRecord) -> dict[str, int] | None:
        spill_root = self._settings.duckdb_temp_directory
        if spill_root is None:
            return None
        spill_root_path = Path(spill_root)
        query_spill_bytes = (
            directory_size(record.spill_temp_directory)
            if record.spill_temp_directory is not None
            else 0
        )
        total_spill_bytes = directory_size(spill_root_path)
        other_spill_bytes = max(0, total_spill_bytes - query_spill_bytes)
        limit_bytes = parse_storage_size_bytes(self._settings.duckdb_max_temp_directory_size)
        try:
            disk_usage = shutil.disk_usage(spill_root_path if spill_root_path.exists() else spill_root_path.parent)
            disk_free_bytes = max(0, int(disk_usage.free))
        except OSError:
            disk_free_bytes = 0
        return {
            "query": max(0, int(query_spill_bytes)),
            "total": max(0, int(total_spill_bytes)),
            "other": max(0, int(other_spill_bytes)),
            "limit": max(0, int(limit_bytes or 0)),
            "free": disk_free_bytes,
        }

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

        cpu_percent: float | None = None
        memory_rss: int | None = None
        process_thread_count: int | None = None
        try:
            if process_metrics is not None and psutil is not None:
                cpu_percent = float(process_metrics.cpu_percent(interval=None))
                memory_rss = int(process_metrics.memory_info().rss)
                with suppress(Exception):
                    process_thread_count = int(process_metrics.num_threads())
                for child in process_metrics.children(recursive=True):
                    with suppress(Exception):
                        memory_rss += int(child.memory_info().rss)
                        cpu_percent += float(child.cpu_percent(interval=None))
        except Exception:
            cpu_percent = None
            memory_rss = None

        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return
            spill_record = record
        spill_metrics = self._spill_metrics_for_record(spill_record)
        if cpu_percent is None and memory_rss is None and spill_metrics is None:
            return

        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status in TERMINAL_QUERY_STATUSES:
                return
            if not force and record.last_metric_sample_monotonic and now_monotonic - record.last_metric_sample_monotonic < QUERY_METRICS_SAMPLE_SECONDS:
                return

            normalized_cpu_percent = max(0.0, cpu_percent) if cpu_percent is not None else None
            cpu_capacity_cores = max(
                0.001,
                float(record.snapshot.cpu_capacity_cores or self._cpu_capacity_cores or 1.0),
            )
            normalized_cpu_capacity_percent = (
                _cpu_capacity_percent(
                    normalized_cpu_percent,
                    cpu_capacity_cores,
                )
                if normalized_cpu_percent is not None
                else None
            )
            normalized_memory_rss = max(0, memory_rss) if memory_rss is not None else None
            normalized_process_thread_count = (
                max(0, int(process_thread_count))
                if process_thread_count is not None
                else None
            )
            duckdb_thread_limit = (
                max(1, int(self._settings.duckdb_threads))
                if self._settings.duckdb_threads is not None
                else None
            )
            record.last_metric_sample_monotonic = now_monotonic
            average_cpu = record.snapshot.average_cpu_percent
            average_cpu_capacity = record.snapshot.average_cpu_capacity_percent
            average_memory = record.snapshot.average_memory_rss_bytes
            if normalized_cpu_percent is not None:
                record.cpu_sample_total += normalized_cpu_percent
                record.cpu_sample_count += 1
                average_cpu = record.cpu_sample_total / max(1, record.cpu_sample_count)
                record.snapshot.cpu_percent = normalized_cpu_percent
                record.snapshot.average_cpu_percent = average_cpu
                record.snapshot.peak_cpu_percent = max(
                    float(record.snapshot.peak_cpu_percent or 0.0),
                    normalized_cpu_percent,
                )
            if normalized_cpu_capacity_percent is not None:
                record.cpu_capacity_sample_total += normalized_cpu_capacity_percent
                record.cpu_capacity_sample_count += 1
                average_cpu_capacity = record.cpu_capacity_sample_total / max(
                    1,
                    record.cpu_capacity_sample_count,
                )
                record.snapshot.cpu_capacity_percent = normalized_cpu_capacity_percent
                record.snapshot.average_cpu_capacity_percent = average_cpu_capacity
                record.snapshot.peak_cpu_capacity_percent = max(
                    float(record.snapshot.peak_cpu_capacity_percent or 0.0),
                    normalized_cpu_capacity_percent,
                )
                record.snapshot.cpu_capacity_cores = cpu_capacity_cores
            if normalized_memory_rss is not None:
                record.memory_sample_total += normalized_memory_rss
                record.memory_sample_count += 1
                average_memory = int(record.memory_sample_total / max(1, record.memory_sample_count))
                record.snapshot.memory_rss_bytes = normalized_memory_rss
                record.snapshot.average_memory_rss_bytes = average_memory
                record.snapshot.peak_memory_rss_bytes = max(
                    int(record.snapshot.peak_memory_rss_bytes or 0),
                    normalized_memory_rss,
                )
            if normalized_process_thread_count is not None:
                record.snapshot.process_thread_count = normalized_process_thread_count
                record.snapshot.peak_process_thread_count = max(
                    int(record.snapshot.peak_process_thread_count or 0),
                    normalized_process_thread_count,
                )
            record.snapshot.duckdb_thread_limit = duckdb_thread_limit
            duckdb_spill_bytes = spill_metrics["query"] if spill_metrics else None
            if spill_metrics is not None:
                record.snapshot.duckdb_spill_bytes = spill_metrics["query"]
                record.snapshot.duckdb_spill_peak_bytes = max(
                    int(record.snapshot.duckdb_spill_peak_bytes or 0),
                    spill_metrics["query"],
                )
                record.snapshot.duckdb_spill_total_bytes = spill_metrics["total"]
                record.snapshot.duckdb_spill_other_bytes = spill_metrics["other"]
                record.snapshot.duckdb_spill_limit_bytes = spill_metrics["limit"]
                record.snapshot.duckdb_spill_disk_free_bytes = spill_metrics["free"]
            record.snapshot.resource_samples.append(
                QueryResourceSample(
                    elapsed_ms=self._elapsed_duration_ms_locked(record),
                    cpu_percent=normalized_cpu_percent,
                    average_cpu_percent=average_cpu,
                    cpu_capacity_percent=normalized_cpu_capacity_percent,
                    average_cpu_capacity_percent=average_cpu_capacity,
                    memory_rss_bytes=normalized_memory_rss,
                    average_memory_rss_bytes=average_memory,
                    process_thread_count=normalized_process_thread_count,
                    duckdb_thread_limit=duckdb_thread_limit,
                    duckdb_spill_bytes=duckdb_spill_bytes,
                    duckdb_spill_total_bytes=spill_metrics["total"] if spill_metrics else None,
                    duckdb_spill_other_bytes=spill_metrics["other"] if spill_metrics else None,
                    duckdb_spill_limit_bytes=spill_metrics["limit"] if spill_metrics else None,
                    duckdb_spill_disk_free_bytes=spill_metrics["free"] if spill_metrics else None,
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
                if key == "timings" and isinstance(value, dict):
                    self._merge_timings_locked(record, value)
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
            timings = changes.pop("timings", None)
            if isinstance(timings, dict):
                self._merge_timings_locked(record, timings)
            backend_prepare_ms = float(record.snapshot.timings.get("backendPrepareMs") or 0.0)
            self._set_timing_locked(
                record,
                "backendTotalMs",
                self._elapsed_duration_ms_locked(record) + backend_prepare_ms,
            )
            for key, value in changes.items():
                if key in {"columns", "rows"} and value is None:
                    continue
                setattr(record.snapshot, key, value)
            self._prune_history_locked()
            self._touch_locked()
        log_level = logging.WARNING if status == "failed" else logging.INFO
        self._log_query_job_event(
            job_id,
            status,
            level=log_level,
            extra=duckdb_profile,
        )
        with self._condition:
            record = self._jobs.get(job_id)
            terminal_payload = record.snapshot.payload if record is not None else None
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
        running_process_count = sum(
            1
            for record in jobs
            if record.snapshot.status in RUNNING_QUERY_STATUSES
            and record.snapshot.process_id is not None
        )
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
                "runningProcessCount": running_process_count,
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

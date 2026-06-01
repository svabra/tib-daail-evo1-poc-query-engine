from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import duckdb

from ..config import Settings
from .query_options import cache_hydration_enabled, parquet_hive_partitioning_option
from .sql_utils import qualified_name, sql_literal


CACHE_TABLE_NAME = "source_cache"
CACHE_STATUS_HIT = "hit"
CACHE_STATUS_MISS = "miss"
CACHE_STATUS_STALE = "stale"
CACHE_STATUS_EXPIRED = "expired"
CACHE_STATUS_UNSUPPORTED = "unsupported"
CACHE_STATUS_ERROR = "error"
RUNTIME_CACHE_EXPECTED_BEHAVIOR = (
    "When Hydrate cache is enabled, the next query run uses this runtime DuckDB table "
    "through a temporary source view if the cache is current."
)
RUNTIME_CACHE_TEMPORARY_WARNING = (
    "This runtime cache table lives in temporary compute storage and can disappear after a pod restart."
)

PREDICATE_COLUMN_PATTERN = re.compile(
    r"(?:where|and|or|on)\s+(?:[a-zA-Z_][\w$]*\.)?"
    r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([a-zA-Z_][\w$]*))\s*"
    r"(?:=|in\b)",
    re.IGNORECASE,
)
USING_COLUMN_PATTERN = re.compile(r"\busing\s*\(([^)]+)\)", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def query_cache_root() -> Path:
    configured = os.environ.get("BDW_QUERY_CACHE_DIR")
    if configured:
        return Path(configured)
    return Path(tempfile.gettempdir()) / "bdw-query-cache"


def _sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _safe_name(value: str, *, prefix: str = "cache") -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()
    if not normalized or normalized[0].isdigit():
        normalized = f"{prefix}_{normalized}"
    return normalized[:48]


def _source_revision(summary: dict[str, object]) -> str:
    revision = str(summary.get("object_revision") or summary.get("objectRevision") or "").strip()
    if revision:
        return revision
    return "|".join(
        str(part or "").strip()
        for part in (
            summary.get("path"),
            summary.get("size_bytes") or summary.get("sizeBytes"),
            summary.get("key"),
        )
        if str(part or "").strip()
    )


def _source_size_bytes(summary: dict[str, object]) -> int:
    try:
        return max(0, int(summary.get("size_bytes") or summary.get("sizeBytes") or 0))
    except (TypeError, ValueError):
        return 0


def _format_mb(size_bytes: int) -> float:
    return round(max(0, int(size_bytes)) / (1024 * 1024), 2)


def infer_predicate_index_columns(sql: str) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()

    def add(raw_value: object) -> None:
        value = str(raw_value or "").strip().strip('"').strip("`").strip("[]")
        if not value:
            return
        normalized = value.lower()
        if normalized not in seen:
            seen.add(normalized)
            columns.append(value)

    for match in PREDICATE_COLUMN_PATTERN.finditer(str(sql or "")):
        add(next((group for group in match.groups() if group), ""))

    for match in USING_COLUMN_PATTERN.finditer(str(sql or "")):
        for item in match.group(1).split(","):
            add(item)

    return columns[:4]


def _id_like_columns(columns: list[str]) -> list[str]:
    matches = []
    for column in columns:
        normalized = column.lower()
        if normalized in {"id", "id_"} or normalized.endswith("_id") or normalized.endswith("id_"):
            matches.append(column)
    return matches[:2]


def _eligible_summary(summary: dict[str, object]) -> bool:
    return (
        isinstance(summary, dict)
        and str(summary.get("relation") or "").strip()
        and str(summary.get("query_sql") or "").strip()
        and str(summary.get("format") or "").strip().lower() == "parquet"
        and str(summary.get("path") or "").strip().lower().startswith("s3://")
    )


def _cache_plan(
    summary: dict[str, object],
    *,
    sql: str,
    query_options: dict[str, object] | None,
) -> dict[str, object]:
    relation = str(summary.get("relation") or "").strip()
    path = str(summary.get("path") or "").strip()
    source_revision = _source_revision(summary)
    source_size_bytes = _source_size_bytes(summary)
    hive_option = parquet_hive_partitioning_option(query_options)
    index_columns = infer_predicate_index_columns(sql)
    key_material = json.dumps(
        {
            "relation": relation,
            "path": path,
            "hive": hive_option,
            "indexColumns": [column.lower() for column in index_columns],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    cache_key = _sha1(key_material)
    root = query_cache_root()
    cache_alias = f"cache_{cache_key[:16]}"
    return {
        "cacheKey": cache_key,
        "relation": relation,
        "sourceViewRelation": relation,
        "path": path,
        "format": "parquet",
        "querySql": str(summary.get("query_sql") or "").strip(),
        "sourceRevision": source_revision,
        "sourceSizeBytes": source_size_bytes,
        "sourceSizeMb": _format_mb(source_size_bytes),
        "hivePartitioning": hive_option,
        "indexColumns": index_columns,
        "cacheDatabasePath": (root / f"{cache_key}.duckdb").as_posix(),
        "metadataPath": (root / f"{cache_key}.json").as_posix(),
        "cacheAlias": cache_alias,
        "cacheTable": f"{cache_alias}.main.{CACHE_TABLE_NAME}",
        "cacheTableName": CACHE_TABLE_NAME,
        "runtimeTable": True,
        "temporary": True,
        "temporaryWarning": RUNTIME_CACHE_TEMPORARY_WARNING,
        "expectedBehavior": RUNTIME_CACHE_EXPECTED_BEHAVIOR,
    }


def _metadata_path(plan: dict[str, object]) -> Path:
    return Path(str(plan["metadataPath"]))


def _database_path(plan: dict[str, object]) -> Path:
    return Path(str(plan["cacheDatabasePath"]))


def _read_metadata(plan: dict[str, object]) -> dict[str, object] | None:
    path = _metadata_path(plan)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_metadata(plan: dict[str, object], metadata: dict[str, object]) -> None:
    path = _metadata_path(plan)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")


def _cache_file_size(plan: dict[str, object]) -> int:
    database_path = _database_path(plan)
    total = database_path.stat().st_size if database_path.exists() else 0
    wal_path = database_path.with_name(f"{database_path.name}.wal")
    if wal_path.exists():
        total += wal_path.stat().st_size
    return total


def _cache_database_wal_path(plan: dict[str, object]) -> Path:
    database_path = _database_path(plan)
    return database_path.with_name(f"{database_path.name}.wal")


def _delete_cache_files(plan: dict[str, object]) -> tuple[bool, list[str], list[str]]:
    paths = [_database_path(plan), _cache_database_wal_path(plan), _metadata_path(plan)]
    deleted_paths: list[str] = []
    errors: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            path.unlink()
            deleted_paths.append(path.as_posix())
        except OSError as exc:
            errors.append(f"{path.as_posix()}: {exc}")
    return bool(deleted_paths) and not errors, deleted_paths, errors


def _physical_cache_state(plan: dict[str, object]) -> tuple[bool, list[str]]:
    database_path = _database_path(plan)
    if not database_path.exists():
        return False, []
    try:
        connection = duckdb.connect(str(database_path), read_only=True)
        try:
            table_exists = bool(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'main' AND table_name = ?
                    """,
                    [CACHE_TABLE_NAME],
                ).fetchone()[0]
            )
            try:
                indexes = [
                    str(row[0])
                    for row in connection.execute(
                        "SELECT index_name FROM duckdb_indexes() WHERE table_name = ? ORDER BY index_name",
                        [CACHE_TABLE_NAME],
                    ).fetchall()
                ]
            except duckdb.Error:
                indexes = []
            return table_exists, indexes
        finally:
            connection.close()
    except duckdb.Error:
        return False, []


def cache_status_for_plan(plan: dict[str, object]) -> dict[str, object]:
    metadata = _read_metadata(plan)
    physical_exists, physical_indexes = _physical_cache_state(plan)
    now = utc_now_iso()
    base = {
        **plan,
        "lastCheckedAt": now,
        "physicalCacheExists": physical_exists,
        "physicalIndexes": physical_indexes,
        "cacheSizeBytes": _cache_file_size(plan),
        "cacheSizeMb": _format_mb(_cache_file_size(plan)),
        "statusReason": "",
        "rowCount": 0,
        "lastHydratedAt": "",
    }
    if metadata is None:
        return {
            **base,
            "status": CACHE_STATUS_MISS,
            "statusLabel": "Cache miss",
            "statusReason": "No local DuckDB cache metadata exists for this source and index plan.",
            "lastHydratedAt": "",
        }
    if metadata.get("expired") is True:
        return {
            **base,
            **metadata,
            "lastCheckedAt": now,
            "status": CACHE_STATUS_EXPIRED,
            "statusLabel": "Expired cache",
            "statusReason": "You manually marked this cache as expired. It will not be reused until it is rebuilt.",
            "physicalCacheExists": physical_exists,
            "cacheSizeBytes": _cache_file_size(plan),
            "cacheSizeMb": _format_mb(_cache_file_size(plan)),
        }
    if str(metadata.get("sourceRevision") or "") != str(plan.get("sourceRevision") or ""):
        return {
            **base,
            **metadata,
            "lastCheckedAt": now,
            "status": CACHE_STATUS_STALE,
            "statusLabel": "Stale cache",
            "statusReason": "The source data changed since this cache was built. The next run will rebuild the cache before querying.",
            "physicalCacheExists": physical_exists,
            "cacheSizeBytes": _cache_file_size(plan),
            "cacheSizeMb": _format_mb(_cache_file_size(plan)),
        }
    if not physical_exists:
        return {
            **base,
            **metadata,
            "lastCheckedAt": now,
            "status": CACHE_STATUS_MISS,
            "statusLabel": "Cache miss",
            "statusReason": "The metadata exists, but the temporary DuckDB cache table is missing. It may have disappeared after a pod restart.",
            "physicalCacheExists": False,
            "cacheSizeBytes": _cache_file_size(plan),
            "cacheSizeMb": _format_mb(_cache_file_size(plan)),
        }
    return {
        **base,
        **metadata,
        "lastCheckedAt": now,
        "status": CACHE_STATUS_HIT,
        "statusLabel": "Cache hit",
        "statusReason": "A local DuckDB cache table exists and matches the current S3 source revision.",
        "physicalCacheExists": True,
        "cacheSizeBytes": _cache_file_size(plan),
        "cacheSizeMb": _format_mb(_cache_file_size(plan)),
    }


def cache_preview(
    *,
    settings: Settings | None = None,
    sql: str,
    source_summaries: list[dict[str, object]],
    query_options: dict[str, object] | None,
) -> dict[str, object]:
    enabled = cache_hydration_enabled(query_options)
    sources: list[dict[str, object]] = []
    unsupported: list[dict[str, object]] = []
    for summary in source_summaries:
        if not isinstance(summary, dict):
            continue
        if _eligible_summary(summary):
            plan = _cache_plan(summary, sql=sql, query_options=query_options)
            sources.append(cache_status_for_plan(plan))
        elif str(summary.get("relation") or "").strip():
            unsupported.append(
                {
                    "relation": str(summary.get("relation") or "").strip(),
                    "path": str(summary.get("path") or "").strip(),
                    "format": str(summary.get("format") or "").strip(),
                    "status": CACHE_STATUS_UNSUPPORTED,
                    "statusLabel": "Unsupported",
                    "statusReason": "Hydrate cache applies to known S3 Parquet sources selected in the notebook. Direct read_parquet('s3://...') calls are not rewritten in this version.",
                }
            )
    return {
        "enabled": enabled,
        "status": "ready" if enabled else "off",
        "freshnessWindowSeconds": 120,
        "cacheRoot": query_cache_root().as_posix(),
        "copy": (
            "Copies the S3 Parquet data referenced by this cell into a temporary local DuckDB table before the query runs. "
            "DuckDB can then reuse the local table and optional ART indexes for repeated filters and lookups."
        ),
        "ephemeralWarning": RUNTIME_CACHE_TEMPORARY_WARNING,
        "rawSqlLimitation": (
            "Hydrate cache applies to known S3 Parquet sources selected in the notebook. Direct read_parquet('s3://...') calls are not rewritten in this version."
        ),
        "sources": sources,
        "unsupportedSources": unsupported,
        "checkedAt": utc_now_iso(),
    }


def expire_cache(
    *,
    sql: str,
    source_summaries: list[dict[str, object]],
    query_options: dict[str, object] | None,
) -> dict[str, object]:
    for summary in source_summaries:
        if not _eligible_summary(summary):
            continue
        plan = _cache_plan(summary, sql=sql, query_options=query_options)
        metadata = _read_metadata(plan) or {
            **plan,
            "rowCount": 0,
            "lastHydratedAt": "",
        }
        metadata["expired"] = True
        metadata["expiredAt"] = utc_now_iso()
        _write_metadata(plan, metadata)
    return cache_preview(
        settings=None,  # type: ignore[arg-type]
        sql=sql,
        source_summaries=source_summaries,
        query_options=query_options,
    )


def delete_cache(
    *,
    sql: str,
    source_summaries: list[dict[str, object]],
    query_options: dict[str, object] | None,
) -> dict[str, object]:
    deletion_by_key: dict[str, dict[str, object]] = {}
    for summary in source_summaries:
        if not _eligible_summary(summary):
            continue
        plan = _cache_plan(summary, sql=sql, query_options=query_options)
        deleted, deleted_paths, errors = _delete_cache_files(plan)
        deletion_by_key[str(plan["cacheKey"])] = {
            "deleted": deleted,
            "deletedPaths": deleted_paths,
            "deleteErrors": errors,
        }

    preview = cache_preview(
        settings=None,
        sql=sql,
        source_summaries=source_summaries,
        query_options=query_options,
    )
    preview["deleted"] = any(
        bool(item.get("deleted")) for item in deletion_by_key.values()
    )
    for source in preview.get("sources", []):
        if not isinstance(source, dict):
            continue
        deletion = deletion_by_key.get(str(source.get("cacheKey") or ""))
        if deletion is None:
            continue
        source.update(deletion)
        if deletion.get("deleteErrors"):
            source["status"] = CACHE_STATUS_ERROR
            source["statusLabel"] = "Delete failed"
            source["statusReason"] = "; ".join(str(error) for error in deletion["deleteErrors"])
        elif deletion.get("deleted"):
            source["statusReason"] = "The runtime cache table and metadata were deleted for this cache plan."
    return preview


def _attach_cache_database(connection: duckdb.DuckDBPyConnection, plan: dict[str, object]) -> str:
    alias = str(plan["cacheAlias"])
    database_path = _database_path(plan)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        connection.execute(f"ATTACH {sql_literal(database_path.as_posix())} AS {qualified_name(alias)}")
    except duckdb.Error as exc:
        if "already exists" not in str(exc).lower() and "already attached" not in str(exc).lower():
            raise
    return alias


def _table_columns(connection: duckdb.DuckDBPyConnection, alias: str) -> list[str]:
    try:
        return [
            str(row[1])
            for row in connection.execute(
                f"PRAGMA table_info({qualified_name(alias, 'main', CACHE_TABLE_NAME)})"
            ).fetchall()
        ]
    except duckdb.Error:
        return []


def _select_index_columns(
    *,
    requested_columns: list[str],
    available_columns: list[str],
) -> tuple[list[str], str]:
    by_normalized = {column.lower(): column for column in available_columns}
    selected = []
    for column in requested_columns:
        resolved = by_normalized.get(column.lower())
        if resolved and resolved not in selected:
            selected.append(resolved)
    if selected:
        return selected[:4], "Auto-selected from WHERE, IN, and JOIN predicates in this SQL cell."
    fallback = _id_like_columns(available_columns)
    if fallback:
        return fallback, "No predicate column was found; clear id-like columns were selected as an ART index fallback."
    return [], "No useful ART index column was found; the source is cached without ART indexes."


def hydrate_cache(
    *,
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    source_summaries: list[dict[str, object]],
    query_options: dict[str, object] | None,
    force: bool = False,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    if not cache_hydration_enabled(query_options):
        return source_summaries, {"enabled": False, "sources": []}

    updated_summaries: list[dict[str, object]] = []
    hydrated_sources: list[dict[str, object]] = []
    timings: dict[str, float] = {}
    started = datetime.now(UTC)
    for summary in source_summaries:
        if not _eligible_summary(summary):
            updated_summaries.append(summary)
            continue

        plan = _cache_plan(summary, sql=sql, query_options=query_options)
        status = cache_status_for_plan(plan)
        should_rehydrate = force or status.get("status") != CACHE_STATUS_HIT
        relation = str(plan["relation"])
        if should_rehydrate and progress_callback is not None:
            progress_callback(
                {
                    "type": "phase",
                    "phase": "cache_hydration",
                    "progressLabel": f"Hydrating cache: {relation}",
                    "message": (
                        "Rebuilds the local DuckDB table from S3 and recreates the selected ART indexes."
                    ),
                    "cacheHydration": {"status": "rehydrating", "relation": relation},
                }
            )

        alias = _attach_cache_database(connection, plan)
        table_ref = qualified_name(alias, "main", CACHE_TABLE_NAME)
        selected_index_columns: list[str] = []
        index_reason = ""
        if should_rehydrate:
            import time

            hydrate_started = time.perf_counter()
            connection.execute(f"DROP TABLE IF EXISTS {table_ref}")
            connection.execute(f"CREATE TABLE {table_ref} AS {plan['querySql']}")
            available_columns = _table_columns(connection, alias)
            selected_index_columns, index_reason = _select_index_columns(
                requested_columns=list(plan.get("indexColumns") or []),
                available_columns=available_columns,
            )
            for column in selected_index_columns:
                index_name = _safe_name(f"idx_{plan['cacheKey']}_{column}", prefix="idx")
                connection.execute(
                    f"CREATE INDEX {index_name} ON {table_ref} ({qualified_name(column)})"
                )
            row_count = int(connection.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()[0])
            cache_size_bytes = _cache_file_size(plan)
            metadata = {
                **plan,
                "indexColumns": selected_index_columns,
                "indexReason": index_reason,
                "rowCount": row_count,
                "cacheSizeBytes": cache_size_bytes,
                "cacheSizeMb": _format_mb(cache_size_bytes),
                "lastHydratedAt": utc_now_iso(),
                "expired": False,
                "runtimeTable": True,
                "temporary": True,
                "temporaryWarning": RUNTIME_CACHE_TEMPORARY_WARNING,
                "expectedBehavior": RUNTIME_CACHE_EXPECTED_BEHAVIOR,
            }
            _write_metadata(plan, metadata)
            timings[f"{relation}.cacheHydrationMs"] = (time.perf_counter() - hydrate_started) * 1000
            status = {
                **metadata,
                "lastCheckedAt": utc_now_iso(),
                "status": CACHE_STATUS_HIT,
                "statusLabel": "Cache hit",
                "statusReason": "A local DuckDB cache table exists and matches the current S3 source revision.",
                "physicalCacheExists": True,
                "physicalIndexes": selected_index_columns,
                "cacheSizeBytes": _cache_file_size(plan),
                "cacheSizeMb": _format_mb(_cache_file_size(plan)),
            }
        else:
            selected_index_columns = [
                str(column)
                for column in (status.get("indexColumns") or [])
                if str(column).strip()
            ]

        updated = dict(summary)
        updated["query_sql"] = f"SELECT * FROM {table_ref}"
        updated_summaries.append(updated)
        hydrated_sources.append(
            {
                **status,
                "relation": relation,
                "indexColumns": selected_index_columns,
                "expectedBehavior": RUNTIME_CACHE_EXPECTED_BEHAVIOR,
                "runtimeTable": True,
                "temporary": True,
                "temporaryWarning": RUNTIME_CACHE_TEMPORARY_WARNING,
            }
        )

    summary = {
        "enabled": True,
        "status": "ready",
        "sources": hydrated_sources,
        "timings": timings,
        "startedAt": started.isoformat(),
        "completedAt": utc_now_iso(),
    }
    return updated_summaries, summary

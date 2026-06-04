from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator

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
CACHE_WRITE_LOCK_SUFFIX = ".duckdb.write-lock"
CACHE_WRITE_LOCK_MAX_WAIT_SECONDS = 90
CACHE_WRITE_LOCK_POLL_SECONDS = 0.25
CACHE_WRITE_LOCK_RETRY_ATTEMPTS = 3
CACHE_WRITE_LOCK_RETRY_DELAY_SECONDS = 0.5
QUERY_CACHE_LOCK_STALE_SECONDS = 600
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
CACHE_KEY_PATTERN = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def query_cache_root(settings: Settings | None = None) -> Path:
    configured_setting = getattr(settings, "query_cache_dir", None)
    if configured_setting:
        return Path(configured_setting)
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


def _format_sql_preview(sql: str, *, max_chars: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(sql or "")).strip()
    if len(text) <= max_chars:
        return text
    return f"{text[: max(0, max_chars - 3)]}..."


def _normalize_cache_key(cache_key: str) -> str:
    normalized = str(cache_key or "").strip().lower()
    if not CACHE_KEY_PATTERN.match(normalized):
        raise ValueError("Invalid query cache key.")
    return normalized


def _cache_file_paths_for_key(
    cache_key: str,
    *,
    settings: Settings | None = None,
) -> tuple[Path, Path, Path]:
    normalized = _normalize_cache_key(cache_key)
    root = query_cache_root(settings)
    database_path = root / f"{normalized}.duckdb"
    wal_path = root / f"{normalized}.duckdb.wal"
    metadata_path = root / f"{normalized}.json"
    return database_path, wal_path, metadata_path


def _cache_lock_holder_is_running(pid: int) -> bool:
    normalized_pid = int(pid)
    if normalized_pid <= 0:
        return False
    with suppress(Exception):
        os.kill(normalized_pid, 0)
        return True
    return False


def _is_cache_lock_stale(lock_path: Path, *, now: float) -> bool:
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    owner_pid = payload.get("pid")
    if isinstance(owner_pid, int):
        if _cache_lock_holder_is_running(owner_pid):
            return False
        return True
    if isinstance(owner_pid, str):
        normalized_owner_pid = None
        with suppress(ValueError, TypeError):
            normalized_owner_pid = int(owner_pid)
        if isinstance(normalized_owner_pid, int) and normalized_owner_pid > 0:
            if _cache_lock_holder_is_running(normalized_owner_pid):
                return False
            return True

    with suppress(OSError):
        return (now - lock_path.stat().st_mtime) > QUERY_CACHE_LOCK_STALE_SECONDS
    return True


@contextmanager
def _acquire_cache_write_lock(
    plan: dict[str, object],
    *,
    max_wait_seconds: float = CACHE_WRITE_LOCK_MAX_WAIT_SECONDS,
) -> Iterator[None]:
    database_path = _database_path(plan)
    lock_path = Path(str(database_path) + CACHE_WRITE_LOCK_SUFFIX)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(
                    fd,
                    json.dumps(
                        {
                            "started_at": utc_now_iso(),
                            "pid": os.getpid(),
                            "cache_key": str(plan.get("cacheKey") or ""),
                        },
                        sort_keys=True,
                    ).encode("utf-8"),
                )
            finally:
                os.close(fd)
            break
        except FileExistsError:
            try:
                if _is_cache_lock_stale(lock_path, now=time.time()):
                    try:
                        os.unlink(lock_path)
                        continue
                    except OSError:
                        pass
                if time.time() - start >= max_wait_seconds:
                    raise TimeoutError(
                        f"Timed out waiting for cache lock on {database_path.as_posix()}"
                    ) from None
                time.sleep(CACHE_WRITE_LOCK_POLL_SECONDS)
            except TimeoutError:
                raise
            except OSError:
                pass
        except OSError as exc:
            raise RuntimeError(
                f"Could not create cache lock file {lock_path.as_posix()}: {exc}"
            ) from exc
    try:
        yield
    finally:
        with suppress(OSError):
            os.unlink(lock_path)


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
    settings: Settings | None = None,
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
    root = query_cache_root(settings)
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


def _cache_context_entry(
    *,
    sql: str,
    cache_context: dict[str, object] | None,
) -> dict[str, object] | None:
    if not isinstance(cache_context, dict):
        cache_context = {}
    notebook_id = str(cache_context.get("notebookId") or cache_context.get("notebook_id") or "").strip()
    notebook_title = str(
        cache_context.get("notebookTitle") or cache_context.get("notebook_title") or ""
    ).strip()
    cell_id = str(cache_context.get("cellId") or cache_context.get("cell_id") or "").strip()
    sql_preview = _format_sql_preview(
        str(cache_context.get("sqlPreview") or cache_context.get("sql_preview") or sql or "")
    )
    if not any((notebook_id, notebook_title, cell_id, sql_preview)):
        return None
    entry: dict[str, object] = {
        "notebookId": notebook_id,
        "notebookTitle": notebook_title,
        "cellId": cell_id,
        "sqlPreview": sql_preview,
        "lastUsedAt": utc_now_iso(),
    }
    return {key: value for key, value in entry.items() if value not in ("", None)}


def _merge_cache_context(
    metadata: dict[str, object],
    *,
    sql: str,
    cache_context: dict[str, object] | None,
    used_at: str,
) -> dict[str, object]:
    updated = dict(metadata)
    updated["lastUsedAt"] = used_at
    entry = _cache_context_entry(sql=sql, cache_context=cache_context)
    if entry is None:
        return updated
    entry["lastUsedAt"] = used_at
    for key in ("notebookId", "notebookTitle", "cellId", "sqlPreview"):
        if entry.get(key):
            updated[key] = entry[key]
    refs = [
        dict(item)
        for item in updated.get("cellRefs", [])
        if isinstance(item, dict)
    ]
    ref_key = (
        str(entry.get("notebookId") or ""),
        str(entry.get("cellId") or ""),
    )
    replaced = False
    for index, existing in enumerate(refs):
        existing_key = (
            str(existing.get("notebookId") or ""),
            str(existing.get("cellId") or ""),
        )
        if existing_key == ref_key and any(ref_key):
            refs[index] = {**existing, **entry}
            replaced = True
            break
    if not replaced:
        refs.append(entry)
    updated["cellRefs"] = refs[-20:]
    return updated


def _cache_file_size(plan: dict[str, object]) -> int:
    database_path = _database_path(plan)
    total = database_path.stat().st_size if database_path.exists() else 0
    wal_path = database_path.with_name(f"{database_path.name}.wal")
    if wal_path.exists():
        total += wal_path.stat().st_size
    return total


def _is_cache_lock_conflict(exc: BaseException) -> bool:
    message = str(exc).lower()
    return (
        "conflicting lock is held" in message
        or "could not set lock on file" in message
        or "another process" in message
        or "already open" in message
        or "being used by another process" in message
    )


def _wait_for_release_of_cache_lock(plan: dict[str, object]) -> None:
    lock_path = Path(str(_database_path(plan)) + CACHE_WRITE_LOCK_SUFFIX)
    if not lock_path.exists():
        return
    with _acquire_cache_write_lock(plan):
        pass


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


def delete_cache_by_key(
    cache_key: str,
    *,
    settings: Settings | None = None,
) -> dict[str, object]:
    normalized = _normalize_cache_key(cache_key)
    database_path, wal_path, metadata_path = _cache_file_paths_for_key(
        normalized,
        settings=settings,
    )
    plan = {
        "cacheKey": normalized,
        "cacheDatabasePath": database_path.as_posix(),
        "metadataPath": metadata_path.as_posix(),
    }
    deleted, deleted_paths, errors = _delete_cache_files(plan)
    return {
        "cacheKey": normalized,
        "deleted": deleted,
        "deletedPaths": deleted_paths,
        "deleteErrors": errors,
    }


def list_query_cache_datasets(
    *,
    settings: Settings | None = None,
) -> list[dict[str, object]]:
    root = query_cache_root(settings)
    if not root.exists() or not root.is_dir():
        return []
    datasets: list[dict[str, object]] = []
    for metadata_path in sorted(root.glob("*.json"), key=lambda path: path.name.lower()):
        cache_key = metadata_path.stem.lower()
        if not CACHE_KEY_PATTERN.match(cache_key):
            continue
        database_path, wal_path, safe_metadata_path = _cache_file_paths_for_key(
            cache_key,
            settings=settings,
        )
        try:
            metadata = json.loads(safe_metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        size_bytes = 0
        for path in (database_path, wal_path, safe_metadata_path):
            try:
                if path.exists() and path.is_file():
                    size_bytes += path.stat().st_size
            except OSError:
                continue
        datasets.append(
            {
                **metadata,
                "cacheKey": cache_key,
                "cacheDatabasePath": database_path.as_posix(),
                "metadataPath": safe_metadata_path.as_posix(),
                "cacheSizeBytes": size_bytes,
                "cacheSizeMb": _format_mb(size_bytes),
                "physicalCacheExists": database_path.exists(),
                "runtimeTable": True,
                "temporary": True,
                "temporaryWarning": RUNTIME_CACHE_TEMPORARY_WARNING,
                "cellRefs": [
                    dict(item)
                    for item in metadata.get("cellRefs", [])
                    if isinstance(item, dict)
                ],
            }
        )
    return datasets


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
            plan = _cache_plan(
                summary,
                sql=sql,
                query_options=query_options,
                settings=settings,
            )
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
        "cacheRoot": query_cache_root(settings).as_posix(),
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
    settings: Settings | None = None,
) -> dict[str, object]:
    for summary in source_summaries:
        if not _eligible_summary(summary):
            continue
        plan = _cache_plan(summary, sql=sql, query_options=query_options, settings=settings)
        metadata = _read_metadata(plan) or {
            **plan,
            "rowCount": 0,
            "lastHydratedAt": "",
        }
        metadata["expired"] = True
        metadata["expiredAt"] = utc_now_iso()
        _write_metadata(plan, metadata)
    return cache_preview(
        settings=settings,
        sql=sql,
        source_summaries=source_summaries,
        query_options=query_options,
    )


def delete_cache(
    *,
    sql: str,
    source_summaries: list[dict[str, object]],
    query_options: dict[str, object] | None,
    settings: Settings | None = None,
) -> dict[str, object]:
    deletion_by_key: dict[str, dict[str, object]] = {}
    for summary in source_summaries:
        if not _eligible_summary(summary):
            continue
        plan = _cache_plan(summary, sql=sql, query_options=query_options, settings=settings)
        deleted, deleted_paths, errors = _delete_cache_files(plan)
        deletion_by_key[str(plan["cacheKey"])] = {
            "deleted": deleted,
            "deletedPaths": deleted_paths,
            "deleteErrors": errors,
        }

    preview = cache_preview(
        settings=settings,
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


def _attach_cache_database(
    connection: duckdb.DuckDBPyConnection,
    plan: dict[str, object],
    *,
    read_only: bool = False,
) -> str:
    alias = str(plan["cacheAlias"])
    database_path = _database_path(plan)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        statement = f"ATTACH {sql_literal(database_path.as_posix())} AS {qualified_name(alias)}"
        if read_only:
            statement += " (READ_ONLY)"
        connection.execute(statement)
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
    settings: Settings | None = None,
    cache_context: dict[str, object] | None = None,
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

        plan = _cache_plan(summary, sql=sql, query_options=query_options, settings=settings)
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

        def _materialize_cache(alias: str, *, rebuild: bool) -> tuple[str, list[str], dict[str, object]]:
            table_ref = qualified_name(alias, "main", CACHE_TABLE_NAME)
            selected_index_columns: list[str] = []
            materialized_status = status
            if rebuild:
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
                used_at = utc_now_iso()
                previous_metadata = _read_metadata(plan) or {}
                metadata = {
                    **previous_metadata,
                    **plan,
                    "indexColumns": selected_index_columns,
                    "indexReason": index_reason,
                    "rowCount": row_count,
                    "cacheSizeBytes": cache_size_bytes,
                    "cacheSizeMb": _format_mb(cache_size_bytes),
                    "lastHydratedAt": used_at,
                    "lastUsedAt": used_at,
                    "expired": False,
                    "runtimeTable": True,
                    "temporary": True,
                    "temporaryWarning": RUNTIME_CACHE_TEMPORARY_WARNING,
                    "expectedBehavior": RUNTIME_CACHE_EXPECTED_BEHAVIOR,
                }
                metadata = _merge_cache_context(
                    metadata,
                    sql=sql,
                    cache_context=cache_context,
                    used_at=used_at,
                )
                _write_metadata(plan, metadata)
                timings[f"{relation}.cacheHydrationMs"] = (
                    time.perf_counter() - hydrate_started
                ) * 1000
                materialized_status = {
                    **metadata,
                    "lastCheckedAt": utc_now_iso(),
                    "status": CACHE_STATUS_HIT,
                    "statusLabel": "Cache hit",
                    "statusReason": (
                        "A local DuckDB cache table exists and matches the current S3 source revision."
                    ),
                    "physicalCacheExists": True,
                    "physicalIndexes": selected_index_columns,
                    "cacheSizeBytes": _cache_file_size(plan),
                    "cacheSizeMb": _format_mb(_cache_file_size(plan)),
                }
            else:
                used_at = utc_now_iso()
                metadata = _read_metadata(plan)
                if metadata is not None:
                    metadata = _merge_cache_context(
                        metadata,
                        sql=sql,
                        cache_context=cache_context,
                        used_at=used_at,
                    )
                    _write_metadata(plan, metadata)
                    materialized_status = cache_status_for_plan(plan)
                else:
                    materialized_status = {
                        **materialized_status,
                        "lastCheckedAt": utc_now_iso(),
                    }
            table_indexes = [
                str(column)
                for column in (materialized_status.get("indexColumns") or [])
                if str(column).strip()
            ]
            return table_ref, table_indexes, materialized_status

        table_ref = ""
        selected_index_columns: list[str] = []
        if should_rehydrate:
            lock_attempts = CACHE_WRITE_LOCK_RETRY_ATTEMPTS
            for attempt in range(1, lock_attempts + 1):
                try:
                    with _acquire_cache_write_lock(plan):
                        status = cache_status_for_plan(plan)
                        should_rehydrate = force or status.get("status") != CACHE_STATUS_HIT
                        alias = _attach_cache_database(
                            connection,
                            plan,
                            read_only=not should_rehydrate,
                        )
                        table_ref, selected_index_columns, status = _materialize_cache(
                            alias,
                            rebuild=should_rehydrate,
                        )
                    break
                except TimeoutError as exc:
                    raise RuntimeError(str(exc)) from exc
                except duckdb.Error as exc:
                    if not _is_cache_lock_conflict(exc) or attempt >= lock_attempts:
                        if _is_cache_lock_conflict(exc):
                            raise RuntimeError(
                                "Cache database is currently locked by another process. "
                                "Retry this query when the cache write is complete."
                            ) from exc
                        raise
                    with suppress(Exception):
                        connection.execute("ROLLBACK")
                    time.sleep(CACHE_WRITE_LOCK_RETRY_DELAY_SECONDS * attempt)
        else:
            lock_attempts = CACHE_WRITE_LOCK_RETRY_ATTEMPTS
            for attempt in range(1, lock_attempts + 1):
                try:
                    if attempt == 1:
                        _wait_for_release_of_cache_lock(plan)
                    alias = _attach_cache_database(connection, plan, read_only=True)
                    table_ref, selected_index_columns, status = _materialize_cache(
                        alias,
                        rebuild=False,
                    )
                    break
                except duckdb.Error as exc:
                    if not _is_cache_lock_conflict(exc):
                        raise
                    if attempt < lock_attempts:
                        with suppress(Exception):
                            connection.execute("ROLLBACK")
                        time.sleep(CACHE_WRITE_LOCK_RETRY_DELAY_SECONDS * attempt)
                        continue
                    with _acquire_cache_write_lock(plan):
                        status = cache_status_for_plan(plan)
                        should_rehydrate = force or status.get("status") != CACHE_STATUS_HIT
                        alias = _attach_cache_database(
                            connection,
                            plan,
                            read_only=not should_rehydrate,
                        )
                        table_ref, selected_index_columns, status = _materialize_cache(
                            alias,
                            rebuild=should_rehydrate,
                        )
                        break

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

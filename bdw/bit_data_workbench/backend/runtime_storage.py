from __future__ import annotations

import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..config import Settings
from .query_cache import (
    delete_cache_by_key,
    list_query_cache_datasets,
    query_cache_root,
)


RUNTIME_SPILL_WARNING = (
    "DuckDB spill files are reported for visibility only. Active spill files are not "
    "deleted from the UI; they are released when the running query finishes or the pod restarts."
)
_STORAGE_SIZE_PATTERN = re.compile(
    r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[kmgtp]?i?b?|bytes?)?\s*$",
    re.IGNORECASE,
)
_BINARY_SIZE_UNITS = {
    "": 1,
    "b": 1,
    "byte": 1,
    "bytes": 1,
    "k": 1024,
    "kb": 1000,
    "ki": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1000**2,
    "mi": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1000**3,
    "gi": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1000**4,
    "ti": 1024**4,
    "tib": 1024**4,
    "p": 1024**5,
    "pb": 1000**5,
    "pi": 1024**5,
    "pib": 1024**5,
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def parse_storage_size_bytes(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = _STORAGE_SIZE_PATTERN.fullmatch(text)
    if match is None:
        return None
    try:
        numeric = float(match.group("value"))
    except ValueError:
        return None
    if numeric < 0:
        return None
    unit = (match.group("unit") or "").strip().lower()
    multiplier = _BINARY_SIZE_UNITS.get(unit)
    if multiplier is None:
        return None
    return int(numeric * multiplier)


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    total = 0
    try:
        iterator = path.rglob("*")
        for item in iterator:
            try:
                if item.is_file():
                    total += item.stat().st_size
            except OSError:
                continue
    except OSError:
        return total
    return total


def _managed_query_spill_directory(
    spill_root: Path | str | None,
    spill_temp_directory: Path | str | None,
) -> Path | None:
    if spill_root is None or spill_temp_directory is None:
        return None
    try:
        root = Path(spill_root).resolve()
        target = Path(spill_temp_directory).resolve()
    except Exception:
        return None
    if target == root or target.parent != root:
        return None
    if not target.name.startswith("query-"):
        return None
    if not target.is_dir():
        return None
    return target


def delete_query_spill_directory(
    spill_root: Path | str | None,
    spill_temp_directory: Path | str | None,
) -> tuple[bool, int]:
    target = _managed_query_spill_directory(spill_root, spill_temp_directory)
    if target is None or not target.is_dir():
        return False, 0
    reclaimed_bytes = directory_size(target)
    shutil.rmtree(target, ignore_errors=True)
    deleted = not target.exists()
    return deleted, reclaimed_bytes if deleted else 0


def cleanup_stale_query_spill_directories(
    spill_root: Path | str | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "root": "",
        "inspectedCount": 0,
        "deletedCount": 0,
        "failedCount": 0,
        "reclaimedBytes": 0,
        "skippedCount": 0,
    }
    if spill_root is None:
        return payload
    try:
        root = Path(spill_root).resolve()
    except Exception:
        return payload
    payload["root"] = root.as_posix()
    if not root.exists() or not root.is_dir():
        return payload
    for child in root.iterdir():
        payload["inspectedCount"] = int(payload["inspectedCount"]) + 1
        candidate = _managed_query_spill_directory(root, child)
        if candidate is None:
            payload["skippedCount"] = int(payload["skippedCount"]) + 1
            continue
        deleted, reclaimed_bytes = delete_query_spill_directory(root, child)
        if deleted:
            payload["deletedCount"] = int(payload["deletedCount"]) + 1
            payload["reclaimedBytes"] = int(payload["reclaimedBytes"]) + reclaimed_bytes
        else:
            payload["failedCount"] = int(payload["failedCount"]) + 1
    return payload


def _format_mb(size_bytes: int) -> float:
    return round(max(0, int(size_bytes)) / (1024 * 1024), 2)


def _nearest_existing_path(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate if candidate.exists() else Path.cwd()


def _disk_usage_payload(path: Path) -> dict[str, Any]:
    usage_path = _nearest_existing_path(path)
    try:
        usage = shutil.disk_usage(usage_path)
    except OSError:
        return {
            "path": path.as_posix(),
            "usagePath": usage_path.as_posix(),
            "exists": path.exists(),
            "totalBytes": 0,
            "usedBytes": 0,
            "freeBytes": 0,
            "totalMb": 0.0,
            "usedMb": 0.0,
            "freeMb": 0.0,
        }
    used = usage.total - usage.free
    return {
        "path": path.as_posix(),
        "usagePath": usage_path.as_posix(),
        "exists": path.exists(),
        "totalBytes": usage.total,
        "usedBytes": used,
        "freeBytes": usage.free,
        "totalMb": _format_mb(usage.total),
        "usedMb": _format_mb(used),
        "freeMb": _format_mb(usage.free),
    }


def _storage_root(settings: Settings, cache_root: Path, spill_dir: Path | None) -> Path:
    workspace = Path("/workspace")
    candidates = [cache_root]
    if spill_dir is not None:
        candidates.append(spill_dir)
    if any(candidate.as_posix().startswith("/workspace/") for candidate in candidates):
        return workspace
    if spill_dir is not None:
        return spill_dir.parent
    return cache_root.parent


def _path_payload(path: Path) -> dict[str, Any]:
    size_bytes = directory_size(path)
    return {
        "path": path.as_posix(),
        "exists": path.exists(),
        "sizeBytes": size_bytes,
        "sizeMb": _format_mb(size_bytes),
    }


def active_query_spill_size(spill_path: Path | None) -> int:
    if spill_path is None or not spill_path.exists() or not spill_path.is_dir():
        return 0
    total = 0
    try:
        children = list(spill_path.iterdir())
    except OSError:
        return 0
    for child in children:
        if child.is_dir() and child.name.startswith("query-"):
            total += directory_size(child)
    return total


def runtime_storage_usage_metrics(settings: Settings) -> dict[str, Any]:
    cache_root = query_cache_root(settings)
    spill_dir = getattr(settings, "duckdb_temp_directory", None)
    spill_path = Path(spill_dir) if spill_dir is not None else None
    storage_root = _storage_root(settings, cache_root, spill_path)
    spill_total_bytes = directory_size(spill_path) if spill_path is not None else 0
    spill_active_bytes = active_query_spill_size(spill_path)
    spill_other_bytes = max(0, spill_total_bytes - spill_active_bytes)
    query_cache_bytes = directory_size(cache_root)
    return {
        "storageRoot": _disk_usage_payload(storage_root),
        "queryCache": {
            "path": cache_root.as_posix(),
            "sizeBytes": query_cache_bytes,
            "sizeMb": _format_mb(query_cache_bytes),
        },
        "duckdbSpill": {
            "path": spill_path.as_posix() if spill_path is not None else "",
            "exists": spill_path.exists() if spill_path is not None else False,
            "totalBytes": spill_total_bytes,
            "activeQueryBytes": spill_active_bytes,
            "otherBytes": spill_other_bytes,
            "sizeBytes": spill_total_bytes,
            "sizeMb": _format_mb(spill_total_bytes),
            "maxTempDirectorySize": settings.duckdb_max_temp_directory_size or "",
            "maxTempDirectorySizeBytes": parse_storage_size_bytes(
                settings.duckdb_max_temp_directory_size
            ),
        },
    }


def runtime_storage_snapshot(settings: Settings) -> dict[str, Any]:
    cache_root = query_cache_root(settings)
    spill_dir = getattr(settings, "duckdb_temp_directory", None)
    spill_path = Path(spill_dir) if spill_dir is not None else None
    storage_root = _storage_root(settings, cache_root, spill_path)
    datasets = list_query_cache_datasets(settings=settings)
    cache_size_bytes = directory_size(cache_root)
    max_temp_directory_size_bytes = parse_storage_size_bytes(
        settings.duckdb_max_temp_directory_size
    )
    spill_payload = (
        {
            **_path_payload(spill_path),
            "activeQueryBytes": active_query_spill_size(spill_path),
            "maxTempDirectorySizeBytes": max_temp_directory_size_bytes,
            "deletable": False,
            "warning": RUNTIME_SPILL_WARNING,
        }
        if spill_path is not None
        else {
            "path": "",
            "exists": False,
            "sizeBytes": 0,
            "sizeMb": 0.0,
            "activeQueryBytes": 0,
            "maxTempDirectorySizeBytes": max_temp_directory_size_bytes,
            "deletable": False,
            "warning": RUNTIME_SPILL_WARNING,
        }
    )
    return {
        "checkedAt": _utc_now_iso(),
        "storageRoot": _disk_usage_payload(storage_root),
        "queryCache": {
            **_path_payload(cache_root),
            "sizeBytes": cache_size_bytes,
            "sizeMb": _format_mb(cache_size_bytes),
            "datasets": datasets,
            "datasetCount": len(datasets),
        },
        "duckdbSpill": spill_payload,
        "duckdbSettings": {
            "memoryLimit": settings.duckdb_memory_limit or "",
            "threads": settings.duckdb_threads,
            "tempDirectory": spill_path.as_posix() if spill_path is not None else "",
            "maxTempDirectorySize": settings.duckdb_max_temp_directory_size or "",
            "maxTempDirectorySizeBytes": max_temp_directory_size_bytes,
            "preserveInsertionOrder": settings.duckdb_preserve_insertion_order,
        },
    }


def delete_runtime_query_cache(settings: Settings, cache_key: str) -> dict[str, Any]:
    result = delete_cache_by_key(cache_key, settings=settings)
    return {
        **result,
        "storage": runtime_storage_snapshot(settings),
    }

from __future__ import annotations

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


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _directory_size(path: Path) -> int:
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
    size_bytes = _directory_size(path)
    return {
        "path": path.as_posix(),
        "exists": path.exists(),
        "sizeBytes": size_bytes,
        "sizeMb": _format_mb(size_bytes),
    }


def runtime_storage_snapshot(settings: Settings) -> dict[str, Any]:
    cache_root = query_cache_root(settings)
    spill_dir = getattr(settings, "duckdb_temp_directory", None)
    spill_path = Path(spill_dir) if spill_dir is not None else None
    storage_root = _storage_root(settings, cache_root, spill_path)
    datasets = list_query_cache_datasets(settings=settings)
    cache_size_bytes = _directory_size(cache_root)
    spill_payload = (
        {
            **_path_payload(spill_path),
            "deletable": False,
            "warning": RUNTIME_SPILL_WARNING,
        }
        if spill_path is not None
        else {
            "path": "",
            "exists": False,
            "sizeBytes": 0,
            "sizeMb": 0.0,
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
            "preserveInsertionOrder": settings.duckdb_preserve_insertion_order,
        },
    }


def delete_runtime_query_cache(settings: Settings, cache_key: str) -> dict[str, Any]:
    result = delete_cache_by_key(cache_key, settings=settings)
    return {
        **result,
        "storage": runtime_storage_snapshot(settings),
    }

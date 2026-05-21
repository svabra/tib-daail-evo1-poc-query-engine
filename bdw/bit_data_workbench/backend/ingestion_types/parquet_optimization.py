from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Any

import duckdb

from ..sql_utils import sql_identifier, sql_literal


PARQUET_OPTIMIZATION_MODES = {"off", "recommended", "manual"}
_CONTROL_COLUMN_NAME_CHAR = re.compile(r"[\x00-\x1f\x7f]")


@dataclass(frozen=True, slots=True)
class ParquetOptimizationSettings:
    mode: str = "off"
    hive_partitioning: bool = False
    partition_columns: list[str] = field(default_factory=list)
    sort_columns: list[str] = field(default_factory=list)
    create_duckdb_cache: bool = False
    index_columns: list[str] = field(default_factory=list)

    @property
    def payload(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "hivePartitioning": self.hive_partitioning,
            "partitionColumns": list(self.partition_columns),
            "sortColumns": list(self.sort_columns),
            "createDuckdbCache": self.create_duckdb_cache,
            "indexColumns": list(self.index_columns),
        }

    @property
    def is_default(self) -> bool:
        return self == ParquetOptimizationSettings()


def normalize_parquet_optimization_settings(
    value: Any,
    *,
    target_id: str,
    storage_format: str,
) -> ParquetOptimizationSettings:
    payload = _coerce_payload(value)
    mode = str(payload.get("mode") or "off").strip().lower()
    if mode not in PARQUET_OPTIMIZATION_MODES:
        raise ValueError("Parquet optimization mode must be one of: off, recommended, manual.")

    settings = ParquetOptimizationSettings(
        mode=mode,
        hive_partitioning=_coerce_bool(payload.get("hivePartitioning")),
        partition_columns=_coerce_column_list(payload.get("partitionColumns"), "partition columns"),
        sort_columns=_coerce_column_list(payload.get("sortColumns"), "sort columns"),
        create_duckdb_cache=_coerce_bool(payload.get("createDuckdbCache")),
        index_columns=_coerce_column_list(payload.get("indexColumns"), "ART index columns"),
    )
    normalized_target = str(target_id or "").strip()
    normalized_format = str(storage_format or "").strip().lower()
    if settings.mode != "off" and (
        normalized_target != "workspace.s3" or normalized_format != "parquet"
    ):
        raise ValueError(
            "Parquet optimization is only available for Shared Workspace S3 imports stored as Parquet."
        )

    if settings.mode == "off":
        return ParquetOptimizationSettings()

    if settings.mode == "recommended":
        if settings.partition_columns or settings.sort_columns or settings.index_columns:
            raise ValueError(
                "Recommended Parquet optimization does not accept manual column lists. "
                "Use Manual mode for explicit columns."
            )
        if settings.hive_partitioning or settings.create_duckdb_cache:
            raise ValueError(
                "Recommended Parquet optimization currently records the recommendation request only; "
                "manual Hive partitioning and DuckDB cache creation are not executed yet."
            )
        return settings

    return settings


def manual_parquet_layout_requested(settings: ParquetOptimizationSettings) -> bool:
    return (
        settings.mode == "manual"
        and (bool(settings.partition_columns) or bool(settings.sort_columns))
    )


def parquet_art_cache_warning(settings: ParquetOptimizationSettings) -> str:
    if settings.mode == "manual" and (settings.create_duckdb_cache or settings.index_columns):
        return (
            "ART index selections were recorded, but no S3-native index was created. "
            "DuckDB ART indexes apply to local DuckDB tables after materialization."
        )
    return ""


def copy_query_to_optimized_parquet(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_query_sql: str,
    target_path,
    optimization: ParquetOptimizationSettings,
) -> tuple[list[str], list[str]]:
    partition_columns = _resolve_selected_source_columns(
        connection=connection,
        source_query_sql=source_query_sql,
        selected_columns=optimization.partition_columns,
        label="partition",
    )
    sort_columns = _resolve_selected_source_columns(
        connection=connection,
        source_query_sql=source_query_sql,
        selected_columns=optimization.sort_columns,
        label="sort",
    )
    select_sql = f"SELECT * FROM ({source_query_sql}) AS bdw_parquet_source"
    if sort_columns:
        select_sql += " ORDER BY " + ", ".join(sql_identifier(column) for column in sort_columns)

    copy_options = ["FORMAT PARQUET", "COMPRESSION ZSTD"]
    if partition_columns:
        copy_options.append(
            "PARTITION_BY (" + ", ".join(sql_identifier(column) for column in partition_columns) + ")"
        )
        if not optimization.hive_partitioning:
            copy_options.append("WRITE_PARTITION_COLUMNS TRUE")
    connection.execute(
        f"COPY ({select_sql}) TO {sql_literal(target_path.as_posix())} "
        f"({', '.join(copy_options)})"
    )
    return partition_columns, sort_columns


def _coerce_payload(value: Any) -> dict[str, Any]:
    if value is None or value == "":
        return {}
    if hasattr(value, "model_dump"):
        value = value.model_dump(by_alias=True)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return {}
        try:
            value = json.loads(normalized)
        except json.JSONDecodeError as exc:
            raise ValueError("parquetOptimization must be a JSON object.") from exc
    if not isinstance(value, dict):
        raise ValueError("parquetOptimization must be an object.")
    return dict(value)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    return bool(value)


def _coerce_column_list(value: Any, label: str) -> list[str]:
    if value in (None, ""):
        return []
    raw_values: list[Any]
    if isinstance(value, str):
        raw_values = [item.strip() for item in value.split(",")]
    elif isinstance(value, (list, tuple)):
        raw_values = list(value)
    else:
        raise ValueError(f"Parquet optimization {label} must be a list of column names.")

    columns: list[str] = []
    seen: set[str] = set()
    for raw_column in raw_values:
        column = str(raw_column or "").strip()
        if not column:
            continue
        if _CONTROL_COLUMN_NAME_CHAR.search(column):
            raise ValueError(
                f"Parquet optimization {label} must not contain control characters."
            )
        normalized = column.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        columns.append(column)
    return columns


def _resolve_selected_source_columns(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_query_sql: str,
    selected_columns: list[str],
    label: str,
) -> list[str]:
    if not selected_columns:
        return []
    rows = connection.execute(
        f"DESCRIBE SELECT * FROM ({source_query_sql}) AS bdw_parquet_source"
    ).fetchall()
    available = [
        str(row[0] or "").strip()
        for row in rows
        if str(row[0] or "").strip()
    ]
    available_by_key = {column.lower(): column for column in available}
    resolved: list[str] = []
    missing: list[str] = []
    for selected in selected_columns:
        column = available_by_key.get(str(selected or "").strip().lower())
        if column is None:
            missing.append(str(selected))
            continue
        if column not in resolved:
            resolved.append(column)
    if missing:
        available_preview = ", ".join(available[:12])
        if len(available) > 12:
            available_preview += ", ..."
        raise ValueError(
            "Manual Parquet optimization "
            f"{label} column(s) not found: {', '.join(missing)}. "
            f"Available columns: {available_preview or 'none'}."
        )
    return resolved

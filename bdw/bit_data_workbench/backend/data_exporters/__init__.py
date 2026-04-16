from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .csv_exporter import csv_export_metadata, write_csv
from .formats import EXPORT_FORMATS, FORMAT_CONTENT_TYPES, normalize_export_format
from .json_exporter import write_json, write_jsonl
from .options import QueryResultExportOptions, normalize_export_settings
from .parquet_exporter import write_parquet
from .xlsx_exporter import write_xlsx
from .xml_exporter import write_xml


StreamRows = Callable[[Callable[[list[str], list[tuple[Any, ...]]], None]], None]


def write_export(
    *,
    export_format: str,
    local_path: Path,
    stream_rows: StreamRows,
    options: QueryResultExportOptions,
) -> None:
    normalized_format = normalize_export_format(export_format)
    if normalized_format == "json":
        write_json(local_path, stream_rows)
        return
    if normalized_format == "jsonl":
        write_jsonl(local_path, stream_rows)
        return
    if normalized_format == "csv":
        write_csv(local_path, stream_rows, options)
        return
    if normalized_format == "parquet":
        write_parquet(local_path, stream_rows, options)
        return
    if normalized_format == "xml":
        write_xml(local_path, stream_rows, options)
        return
    if normalized_format == "xlsx":
        write_xlsx(local_path, stream_rows, options)
        return
    raise ValueError(f"Unsupported export format: {export_format}")


def export_s3_metadata(
    export_format: str,
    options: QueryResultExportOptions,
) -> dict[str, str]:
    normalized_format = normalize_export_format(export_format)
    if normalized_format == "csv":
        return csv_export_metadata(options)
    return {}

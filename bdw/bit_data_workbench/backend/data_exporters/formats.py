from __future__ import annotations


EXPORT_FORMATS = {"csv", "json", "jsonl", "parquet", "xml", "xlsx"}

FORMAT_CONTENT_TYPES = {
    "csv": "text/csv; charset=utf-8",
    "json": "application/json",
    "jsonl": "application/x-ndjson; charset=utf-8",
    "parquet": "application/vnd.apache.parquet",
    "xml": "application/xml; charset=utf-8",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


def normalize_export_format(export_format: str) -> str:
    normalized = str(export_format or "").strip().lower()
    if normalized not in EXPORT_FORMATS:
        raise ValueError(f"Unsupported export format: {export_format}")
    return normalized

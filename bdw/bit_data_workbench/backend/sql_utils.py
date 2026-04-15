from __future__ import annotations

import re


SUPPORTED_S3_VIEW_FORMATS = {"parquet", "csv", "json"}


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def qualified_name(*parts: str) -> str:
    return ".".join(sql_identifier(part) for part in parts)


def infer_s3_view_format(path: str) -> str:
    lowered = path.lower()
    if ".parquet" in lowered:
        return "parquet"
    if ".csv" in lowered or ".tsv" in lowered:
        return "csv"
    if ".json" in lowered or ".jsonl" in lowered or ".ndjson" in lowered:
        return "json"
    raise ValueError(
        "Could not infer the S3 startup view format from the configured path. "
        "Use 'view_name=format:s3://bucket/path'."
    )


def parse_s3_startup_views(raw_value: str | None) -> list[tuple[str, str, str]]:
    if not raw_value:
        return []

    entries: list[tuple[str, str, str]] = []
    for item in re.split(r"[;\r\n]+", raw_value):
        entry = item.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(
                "Each S3 startup view entry must use 'view_name=s3://bucket/path' "
                "or 'view_name=format:s3://bucket/path'."
            )

        view_name, spec = entry.split("=", 1)
        view_name = view_name.strip()
        spec = spec.strip()
        if not view_name or not spec:
            raise ValueError(f"Invalid S3 startup view entry: {entry}")

        prefix = spec.split(":", 1)[0].lower()
        if prefix in SUPPORTED_S3_VIEW_FORMATS and ":" in spec:
            data_format, path = spec.split(":", 1)
            entries.append((view_name, data_format.strip().lower(), path.strip()))
        else:
            entries.append((view_name, infer_s3_view_format(spec), spec))

    return entries

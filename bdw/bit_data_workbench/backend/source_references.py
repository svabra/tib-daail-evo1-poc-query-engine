from __future__ import annotations

import re
from dataclasses import dataclass

from .sql_utils import sql_literal


SAFE_REFERENCE_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
S3_FORMAT_BY_SUFFIX = {
    ".parquet": "parquet",
    ".csv": "csv",
    ".tsv": "csv",
    ".json": "json",
    ".jsonl": "json",
    ".ndjson": "json",
}


@dataclass(frozen=True, slots=True)
class SourceReference:
    root: str
    container: str
    object_name: str

    @property
    def normalized_key(self) -> str:
        return normalize_reference_parts((self.root, self.container, self.object_name))


def quote_reference_part(value: str) -> str:
    part = str(value or "").strip()
    if SAFE_REFERENCE_PART_RE.fullmatch(part):
        return part
    return '"' + part.replace('"', '""') + '"'


def join_reference_parts(parts: tuple[str, ...] | list[str]) -> str:
    return ".".join(quote_reference_part(part) for part in parts if str(part or "").strip())


def normalize_reference_parts(parts: tuple[str, ...] | list[str]) -> str:
    return ".".join(str(part or "").strip().lower() for part in parts if str(part or "").strip())


def split_qualified_reference(value: str) -> list[str]:
    text = str(value or "").strip()
    parts: list[str] = []
    index = 0
    length = len(text)

    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            break

        char = text[index]
        if char in {'"', "`"}:
            part, index = _read_quoted_part(text, index, char)
        elif char == "[":
            end = text.find("]", index + 1)
            if end < 0:
                part = text[index + 1 :]
                index = length
            else:
                part = text[index + 1 : end]
                index = end + 1
        else:
            start = index
            while index < length and text[index] != ".":
                index += 1
            part = text[start:index].strip()

        if part:
            parts.append(part)
        if index < length and text[index] == ".":
            index += 1
            continue
        break

    return parts


def _read_quoted_part(text: str, index: int, quote: str) -> tuple[str, int]:
    index += 1
    fragments: list[str] = []
    length = len(text)
    while index < length:
        char = text[index]
        if char == quote:
            if quote == '"' and index + 1 < length and text[index + 1] == quote:
                fragments.append(quote)
                index += 2
                continue
            return "".join(fragments), index + 1
        fragments.append(char)
        index += 1
    return "".join(fragments), length


def normalize_qualified_name(value: str) -> str:
    return normalize_reference_parts(split_qualified_reference(value))


def s3_source_reference(*, bucket: str, key: str) -> str:
    normalized_bucket = str(bucket or "").strip()
    normalized_key = str(key or "").strip().lstrip("/")
    if not normalized_bucket or not normalized_key:
        return ""
    return join_reference_parts(("s3", normalized_bucket, normalized_key))


def pg_source_reference(*, source_id: str, relation: str) -> str:
    normalized_source_id = str(source_id or "").strip()
    normalized_relation = str(relation or "").strip()
    if not normalized_source_id or not normalized_relation:
        return ""
    prefix = f"{normalized_source_id}."
    relation_part = (
        normalized_relation[len(prefix) :]
        if normalized_relation.startswith(prefix)
        else normalized_relation
    )
    return join_reference_parts(("pg", normalized_source_id, relation_part))


def parse_source_reference(value: str) -> SourceReference | None:
    parts = split_qualified_reference(value)
    if len(parts) != 3:
        return None
    root = parts[0].strip().lower()
    if root not in {"s3", "pg"}:
        return None
    container = parts[1].strip()
    object_name = parts[2].strip()
    if not container or not object_name:
        return None
    return SourceReference(root=root, container=container, object_name=object_name)


def infer_s3_reference_format(*, key: str, fallback: str = "") -> str:
    normalized_fallback = str(fallback or "").strip().lower()
    if normalized_fallback:
        return "json" if normalized_fallback in {"jsonl", "ndjson"} else normalized_fallback
    lowered_key = str(key or "").strip().lower()
    for suffix, file_format in S3_FORMAT_BY_SUFFIX.items():
        if lowered_key.endswith(suffix):
            return file_format
    return ""


def s3_table_function_sql(
    *,
    bucket: str,
    key: str,
    file_format: str = "",
    hive_partitioning: bool | None = None,
) -> str:
    s3_path = f"s3://{str(bucket or '').strip()}/{str(key or '').strip().lstrip('/')}"
    normalized_format = infer_s3_reference_format(key=key, fallback=file_format)
    if normalized_format == "parquet":
        if hive_partitioning is not None:
            return (
                f"read_parquet({sql_literal(s3_path)}, "
                f"hive_partitioning={'true' if hive_partitioning else 'false'})"
            )
        return f"read_parquet({sql_literal(s3_path)})"
    if normalized_format == "csv":
        return f"read_csv_auto({sql_literal(s3_path)})"
    if normalized_format == "json":
        return f"read_json_auto({sql_literal(s3_path)})"
    return f"read_parquet({sql_literal(s3_path)})"

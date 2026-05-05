from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath

from ...models import SourceField
from ..sql_utils import sql_identifier


DDL_TYPE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_ ()\[\],.]*$")


@dataclass(frozen=True, slots=True)
class SourceDdlDownload:
    ddl: str
    filename: str
    content_type: str = "application/sql; charset=utf-8"


def safe_sql_type(type_name: str) -> str:
    normalized_type = str(type_name or "").strip().upper()
    if not normalized_type:
        return "TEXT"
    if not DDL_TYPE_PATTERN.fullmatch(normalized_type):
        return "TEXT"
    return normalized_type


def ddl_filename(value: str, *, fallback: str = "source-ddl") -> str:
    candidate = str(value or "").strip()
    if candidate:
        candidate = PurePosixPath(candidate.replace("\\", "/")).name.strip()
    if candidate.lower().endswith(".sql"):
        candidate = candidate[:-4]
    if "." in candidate:
        candidate = PurePosixPath(candidate).stem.strip()
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", candidate).strip(".-")
    if not normalized:
        normalized = fallback
    return f"{normalized}.sql"


def suggested_relation_name(value: str, *, fallback: str = "source_object") -> str:
    candidate = str(value or "").strip()
    if candidate:
        candidate = PurePosixPath(candidate.replace("\\", "/")).name.strip()
    if "." in candidate:
        candidate = PurePosixPath(candidate).stem.strip()
    normalized = re.sub(r"[^A-Za-z0-9_]+", "_", candidate.lower()).strip("_")
    if not normalized:
        normalized = fallback
    if normalized[0].isdigit():
        normalized = f"{fallback}_{normalized}"
    return normalized


def split_relation(relation: str) -> list[str]:
    return [part.strip() for part in str(relation or "").strip().split(".") if part.strip()]


def create_table_ddl(
    *,
    table_name: str,
    fields: list[SourceField],
    schema_name: str = "",
    source_comment: str = "",
    suggested: bool = False,
) -> str:
    if not fields:
        raise ValueError("The source object does not expose any fields for DDL generation.")

    qualified_table = (
        f"{sql_identifier(schema_name)}.{sql_identifier(table_name)}"
        if schema_name
        else sql_identifier(table_name)
    )
    lines: list[str] = []
    if suggested:
        lines.append("-- Suggested DDL generated from the source metadata.")
    else:
        lines.append("-- DDL generated from the source metadata.")
    if source_comment:
        lines.append(f"-- Source: {source_comment}")
    lines.append(f"CREATE TABLE {qualified_table} (")
    for index, field in enumerate(fields):
        suffix = "," if index < len(fields) - 1 else ""
        lines.append(
            f"  {sql_identifier(str(field.name or '').strip() or f'column_{index + 1}')} "
            f"{safe_sql_type(field.data_type)}{suffix}"
        )
    lines.append(");")
    lines.append("")
    return "\n".join(lines)


def synthetic_source_ddl(
    *,
    fields: list[SourceField],
    relation: str = "",
    object_name: str = "",
    source_id: str = "",
    source_path: str = "",
) -> SourceDdlDownload:
    relation_parts = split_relation(relation)
    resolved_name = (
        suggested_relation_name(object_name)
        if object_name
        else suggested_relation_name(relation_parts[-1] if relation_parts else "source_object")
    )
    source_comment_parts = [
        part
        for part in (
            source_id,
            source_path,
            relation,
        )
        if str(part or "").strip()
    ]
    ddl = create_table_ddl(
        table_name=resolved_name,
        fields=fields,
        source_comment=" | ".join(source_comment_parts),
        suggested=True,
    )
    return SourceDdlDownload(
        ddl=ddl,
        filename=ddl_filename(object_name or resolved_name),
    )

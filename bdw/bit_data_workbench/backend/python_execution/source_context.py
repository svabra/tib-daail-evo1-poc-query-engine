from __future__ import annotations

from urllib.parse import urlparse

from ...models import SourceObject
from ..source_references import (
    parse_source_reference,
    s3_table_function_sql,
)


def _query_sql_relation(query_sql: str) -> str:
    normalized = str(query_sql or "").strip().rstrip(";").strip()
    if not normalized:
        return ""
    if normalized.lower().startswith("select * from "):
        return normalized[len("select * from ") :].strip()
    if normalized.lower().startswith(("select", "with", "values")):
        return f"({normalized})"
    return normalized


def python_runtime_relation(
    source_object: SourceObject,
    *,
    canonical_source_id: str,
) -> tuple[str, bool]:
    """Return a worker-local relation and whether it still needs the shared catalog."""
    catalog_relation = str(source_object.relation or "").strip()
    if str(canonical_source_id or "").strip().lower() != "s3":
        return catalog_relation, False

    query_relation = _query_sql_relation(source_object.query_sql)
    if query_relation:
        return query_relation, False

    bucket = str(source_object.s3_bucket or "").strip()
    key = str(source_object.s3_key or "").strip().lstrip("/")
    if not bucket or not key:
        reference = parse_source_reference(source_object.query_reference)
        if reference is not None and reference.root == "s3":
            bucket, key = reference.container, reference.object_name

    if (not bucket or not key) and source_object.s3_path:
        parsed = urlparse(str(source_object.s3_path or "").strip())
        if parsed.scheme.lower() == "s3" and parsed.netloc:
            bucket, key = parsed.netloc, parsed.path.lstrip("/")

    if bucket and key:
        return (
            s3_table_function_sql(
                bucket=bucket,
                key=key,
                file_format=str(source_object.s3_file_format or ""),
            ),
            False,
        )

    # A legacy S3 catalog view without object metadata cannot be recreated in an
    # isolated worker, so retain the old shared-catalog fallback for that case.
    return catalog_relation, True


def python_source_aliases(source_object: SourceObject, *, schema_name: str) -> list[str]:
    candidates = (
        source_object.relation,
        source_object.query_reference,
        source_object.query_alias,
        source_object.name,
        source_object.display_name,
        f"{schema_name}.{source_object.name}" if schema_name else source_object.name,
    )
    aliases: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        alias = str(candidate or "").strip()
        if not alias or alias.lower() in seen:
            continue
        seen.add(alias.lower())
        aliases.append(alias)
    return aliases

from __future__ import annotations

from pathlib import PurePosixPath
import re

from ..models import SourceCatalog, SourceObject, SourceSchema
from .query_aliases import normalize_relation_key as normalize_query_alias_key
from .query_analysis import KnownRelationReference
from .source_discovery import parse_s3_path
from .source_references import infer_s3_reference_format


def _source_object_key(payload: dict[str, object]) -> str:
    return "||".join(
        [
            str(payload.get("sourceId") or "").strip().lower(),
            str(payload.get("relation") or "").strip().lower(),
            str(payload.get("bucket") or "").strip(),
            str(payload.get("key") or "").strip(),
        ]
    )


def _first_s3_path_from_query_sql(query_sql: str) -> str:
    match = re.search(r"s3://[^'\"\s,)]+", str(query_sql or ""))
    return match.group(0) if match else ""


def _source_object_display_label(
    *,
    display_name: str = "",
    relation: str = "",
    bucket: str = "",
    key: str = "",
) -> str:
    normalized_display = str(display_name or "").strip()
    if normalized_display:
        return normalized_display
    if key:
        return PurePosixPath(str(key)).name or str(key)
    if relation:
        return str(relation).rsplit(".", 1)[-1]
    if bucket:
        return str(bucket)
    return "Source object"


def _source_object_payload_from_summary(
    summary: dict[str, object],
) -> dict[str, object]:
    relation = str(summary.get("relation") or "").strip()
    bucket = str(summary.get("bucket") or "").strip()
    key = str(summary.get("key") or "").strip()
    path = str(summary.get("path") or "").strip()
    if (not bucket or not key) and path.startswith("s3://"):
        try:
            parsed_bucket, parsed_key = parse_s3_path(path)
            bucket = bucket or parsed_bucket
            key = key or parsed_key
        except ValueError:
            pass
    if not bucket or not key:
        query_path = _first_s3_path_from_query_sql(
            str(summary.get("query_sql") or "")
        )
        if query_path:
            try:
                parsed_bucket, parsed_key = parse_s3_path(query_path)
                bucket = bucket or parsed_bucket
                key = key or parsed_key
                path = path or query_path
            except ValueError:
                pass
    source_id = "workspace.s3" if bucket and key else ""
    if not source_id and relation.startswith("workspace_local_"):
        source_id = "workspace.local"
    return {
        "label": _source_object_display_label(
            display_name=str(summary.get("display_name") or "").strip(),
            relation=relation,
            bucket=bucket,
            key=key,
        ),
        "kind": "s3-object" if bucket and key else "table",
        "sourceId": source_id,
        "relation": relation,
        "queryAlias": str(summary.get("query_alias") or "").strip(),
        "queryReference": str(summary.get("query_reference") or "").strip(),
        "bucket": bucket,
        "key": key,
        "path": path or (f"s3://{bucket}/{key}" if bucket and key else ""),
        "format": str(summary.get("format") or "").strip(),
    }


def _source_object_payload_from_s3_path(path: str) -> dict[str, object] | None:
    normalized_path = str(path or "").strip()
    if not normalized_path.lower().startswith("s3://"):
        return None
    try:
        bucket, key = parse_s3_path(normalized_path)
    except ValueError:
        return None
    if not bucket or not key:
        return None
    return {
        "label": _source_object_display_label(bucket=bucket, key=key),
        "kind": "s3-object",
        "sourceId": "workspace.s3",
        "relation": normalized_path,
        "queryAlias": "",
        "queryReference": "",
        "bucket": bucket,
        "key": key,
        "path": normalized_path,
        "format": infer_s3_reference_format(key=key),
    }


def _source_object_alias_keys(
    *,
    catalog: SourceCatalog,
    schema: SourceSchema,
    source_object: SourceObject,
) -> set[str]:
    aliases = {
        str(source_object.relation or "").strip(),
        str(source_object.query_alias or "").strip(),
        str(source_object.query_reference or "").strip(),
        str(source_object.name or "").strip(),
    }
    if schema.name and source_object.name:
        aliases.add(f"{schema.name}.{source_object.name}")
    source_id = str(catalog.connection_source_id or catalog.name or "").strip()
    if source_id and source_object.relation:
        aliases.add(f"{source_id}.{source_object.relation}")
    return {
        normalized
        for normalized in (normalize_query_alias_key(alias) for alias in aliases)
        if normalized
    }


def _source_object_payload_from_catalog(
    *,
    catalog: SourceCatalog,
    source_object: SourceObject,
) -> dict[str, object]:
    source_id = str(catalog.connection_source_id or catalog.name or "").strip()
    if catalog.name == "workspace" and not source_id:
        source_id = "workspace.s3"
    bucket = str(source_object.s3_bucket or "").strip()
    key = str(source_object.s3_key or "").strip()
    relation = str(source_object.relation or "").strip()
    return {
        "label": _source_object_display_label(
            display_name=str(source_object.display_name or source_object.name or "").strip(),
            relation=relation,
            bucket=bucket,
            key=key,
        ),
        "kind": "s3-object" if bucket and key else str(source_object.kind or "table").strip() or "table",
        "sourceId": source_id,
        "relation": relation,
        "queryAlias": str(source_object.query_alias or "").strip(),
        "queryReference": str(source_object.query_reference or "").strip(),
        "bucket": bucket,
        "key": key,
        "path": str(source_object.s3_path or "").strip()
        or (f"s3://{bucket}/{key}" if bucket and key else ""),
        "format": str(source_object.s3_file_format or "").strip(),
    }


def query_source_objects(
    touched_relations: list[str] | None,
    *,
    relation_index: dict[str, KnownRelationReference] | None = None,
    source_summaries: list[dict[str, object]] | None = None,
    catalogs: list[SourceCatalog] | None = None,
) -> list[dict[str, object]]:
    touched = [
        str(relation or "").strip()
        for relation in (touched_relations or [])
        if str(relation or "").strip()
    ]
    if not touched:
        return []

    summaries_by_key: dict[str, dict[str, object]] = {}
    for summary in source_summaries or []:
        if not isinstance(summary, dict):
            continue
        for candidate in (
            summary.get("relation"),
            summary.get("query_alias"),
            summary.get("query_reference"),
        ):
            normalized = normalize_query_alias_key(str(candidate or "").strip())
            if normalized:
                summaries_by_key.setdefault(normalized, summary)

    payloads: list[dict[str, object]] = []
    seen: set[str] = set()

    def add(payload: dict[str, object]) -> None:
        key = _source_object_key(payload)
        if key in seen:
            return
        seen.add(key)
        payloads.append(payload)

    for relation in touched:
        normalized_relation = normalize_query_alias_key(relation)
        reference = (relation_index or {}).get(normalized_relation)
        normalized_reference = normalize_query_alias_key(
            str(reference.relation if reference else relation)
        )
        summary = summaries_by_key.get(normalized_relation) or summaries_by_key.get(
            normalized_reference
        )
        if summary is not None:
            add(_source_object_payload_from_summary(summary))
            continue

        direct_s3_payload = _source_object_payload_from_s3_path(relation)
        if direct_s3_payload is not None:
            add(direct_s3_payload)
            continue

        matched_payload: dict[str, object] | None = None
        for catalog in catalogs or []:
            for schema in catalog.schemas:
                for source_object in schema.objects:
                    alias_keys = _source_object_alias_keys(
                        catalog=catalog,
                        schema=schema,
                        source_object=source_object,
                    )
                    if normalized_relation in alias_keys or normalized_reference in alias_keys:
                        matched_payload = _source_object_payload_from_catalog(
                            catalog=catalog,
                            source_object=source_object,
                        )
                        break
                if matched_payload:
                    break
            if matched_payload:
                break
        if matched_payload:
            add(matched_payload)

    return payloads

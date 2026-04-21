from __future__ import annotations

from typing import TYPE_CHECKING

from ...models import SourceObject

if TYPE_CHECKING:
    from ..service import WorkbenchService


POSTGRES_EXPLORER_SOURCE_ALIASES = {
    "pg_oltp_native": "pg_oltp",
}


def canonical_explorer_source_id(source_id: str) -> str:
    normalized_source_id = str(source_id or "").strip().lower()
    if not normalized_source_id:
        raise KeyError("Missing data source identifier.")
    return POSTGRES_EXPLORER_SOURCE_ALIASES.get(
        normalized_source_id,
        normalized_source_id,
    )


def explorer_kind_for_source(source_id: str) -> str:
    normalized_source_id = canonical_explorer_source_id(source_id)
    if normalized_source_id in {"pg_oltp", "pg_olap"}:
        return "postgres"
    if normalized_source_id == "workspace.s3":
        return "s3"
    if normalized_source_id == "workspace.local":
        return "local-workspace"
    raise KeyError(f"Unsupported data source explorer: {source_id}")


def _source_object_payload(
    service: WorkbenchService,
    *,
    source_id: str,
    source_object: SourceObject,
) -> dict[str, object]:
    payload = {
        "name": source_object.name,
        "displayName": source_object.display_name or source_object.name,
        "kind": source_object.kind,
        "relation": source_object.relation,
        "s3Bucket": source_object.s3_bucket,
        "s3Key": source_object.s3_key,
        "s3Path": source_object.s3_path,
        "s3FileFormat": source_object.s3_file_format,
        "s3Downloadable": source_object.s3_downloadable,
        "sizeBytes": int(source_object.size_bytes or 0),
    }
    relation = str(source_object.relation or "").strip()
    publication_source: dict[str, object] | None = None

    if source_id == "workspace.s3":
        bucket = str(source_object.s3_bucket or "").strip()
        key = str(source_object.s3_key or "").strip()
        if source_object.s3_downloadable and bucket and key:
            publication_source = {
                "sourceKind": "object",
                "sourceId": "workspace.s3",
                "bucket": bucket,
                "key": key,
            }
        elif relation:
            publication_source = {
                "sourceKind": "relation",
                "sourceId": "workspace.s3",
                "relation": relation,
            }
    elif relation:
        publication_source = {
            "sourceKind": "relation",
            "sourceId": source_id,
            "relation": relation,
        }

    payload["publishedDataProducts"] = (
        service.published_data_products_for_source(source=publication_source)
        if publication_source
        else []
    )
    return payload


def _annotate_s3_snapshot(
    service: WorkbenchService,
    snapshot: dict[str, object],
) -> dict[str, object]:
    annotated_snapshot = dict(snapshot or {})
    bucket = str(annotated_snapshot.get("bucket") or "").strip()
    prefix = str(annotated_snapshot.get("prefix") or "").strip()
    annotated_entries: list[dict[str, object]] = []

    for raw_entry in list(annotated_snapshot.get("entries") or []):
        if not isinstance(raw_entry, dict):
            continue
        entry = dict(raw_entry)
        entry_kind = str(entry.get("entryKind") or "").strip()
        publication_source: dict[str, object] | None = None

        if entry_kind == "bucket":
            entry_bucket = str(entry.get("bucket") or "").strip()
            if entry_bucket:
                publication_source = {
                    "sourceKind": "bucket",
                    "sourceId": "workspace.s3",
                    "bucket": entry_bucket,
                }
        elif entry_kind == "file":
            entry_bucket = str(entry.get("bucket") or "").strip()
            entry_key = str(entry.get("prefix") or "").strip()
            if entry_bucket and entry_key:
                publication_source = {
                    "sourceKind": "object",
                    "sourceId": "workspace.s3",
                    "bucket": entry_bucket,
                    "key": entry_key,
                }

        entry["publishedDataProducts"] = (
            service.published_data_products_for_source(source=publication_source)
            if publication_source
            else []
        )
        annotated_entries.append(entry)

    annotated_snapshot["entries"] = annotated_entries
    annotated_snapshot["publishedDataProducts"] = (
        service.published_data_products_for_source(
            source={
                "sourceKind": "bucket",
                "sourceId": "workspace.s3",
                "bucket": bucket,
            }
        )
        if bucket and not prefix
        else []
    )
    return annotated_snapshot


def _postgres_explorer_payload(
    service: WorkbenchService,
    *,
    source_id: str,
) -> dict[str, object]:
    objects_by_schema = service._postgres_plugin_by_source_id(source_id).catalog_objects()
    schemas: list[dict[str, object]] = []
    default_relation = ""

    for schema_name in sorted(objects_by_schema):
        objects = [
            _source_object_payload(
                service,
                source_id=source_id,
                source_object=source_object,
            )
            for source_object in objects_by_schema.get(schema_name, [])
        ]
        if not default_relation and objects:
            default_relation = str(objects[0].get("relation") or "").strip()
        schemas.append(
            {
                "name": schema_name,
                "label": schema_name,
                "objectCount": len(objects),
                "objects": objects,
            }
        )

    return {
        "sourceId": source_id,
        "explorerKind": "postgres",
        "schemas": schemas,
        "defaultRelation": default_relation,
    }


def _s3_explorer_payload(
    service: WorkbenchService,
    *,
    source_id: str,
    bucket: str = "",
    prefix: str = "",
) -> dict[str, object]:
    return {
        "sourceId": source_id,
        "explorerKind": "s3",
        "snapshot": _annotate_s3_snapshot(
            service,
            service.s3_explorer_snapshot(bucket=bucket, prefix=prefix),
        ),
    }


def _local_workspace_explorer_payload(
    *,
    source_id: str,
) -> dict[str, object]:
    return {
        "sourceId": source_id,
        "explorerKind": "local-workspace",
    }


def build_data_source_explorer_payload(
    service: WorkbenchService,
    *,
    source_id: str,
    bucket: str = "",
    prefix: str = "",
) -> dict[str, object]:
    canonical_source_id = canonical_explorer_source_id(source_id)
    explorer_kind = explorer_kind_for_source(canonical_source_id)

    if explorer_kind == "postgres":
        return _postgres_explorer_payload(service, source_id=canonical_source_id)

    if explorer_kind == "s3":
        return _s3_explorer_payload(
            service,
            source_id=canonical_source_id,
            bucket=bucket,
            prefix=prefix,
        )

    if explorer_kind == "local-workspace":
        return _local_workspace_explorer_payload(source_id=canonical_source_id)

    raise KeyError(f"Unsupported data source explorer: {source_id}")

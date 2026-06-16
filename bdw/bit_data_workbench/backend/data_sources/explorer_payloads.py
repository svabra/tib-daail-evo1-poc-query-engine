from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ...models import SourceObject
from ..query_aliases import normalize_query_alias_segment
from .s3_paths import S3SourceObjectLocation, source_object_s3_location

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
    if normalized_source_id == "s3":
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
        "queryAlias": source_object.query_alias,
        "queryReference": source_object.query_reference,
        "querySql": source_object.query_sql,
        "s3Bucket": source_object.s3_bucket,
        "s3Key": source_object.s3_key,
        "s3Path": source_object.s3_path,
        "s3FileFormat": source_object.s3_file_format,
        "s3Downloadable": source_object.s3_downloadable,
        "sizeBytes": int(source_object.size_bytes or 0),
        "s3DownloadKind": source_object.s3_download_kind,
        "s3PartPrefix": source_object.s3_part_prefix,
        "s3PartFileFormat": source_object.s3_part_file_format,
        "s3PartCount": int(source_object.s3_part_count or 0),
        "s3DownloadFilename": source_object.s3_download_filename,
        "s3MergeDownloadable": source_object.s3_merge_downloadable,
        "s3ZipDownloadable": source_object.s3_zip_downloadable,
    }
    relation = str(source_object.relation or "").strip()
    publication_source: dict[str, object] | None = None

    if source_id == "s3":
        bucket = str(source_object.s3_bucket or "").strip()
        key = str(source_object.s3_key or "").strip()
        if source_object.s3_downloadable and bucket and key:
            publication_source = {
                "sourceKind": "object",
                "sourceId": "s3",
                "bucket": bucket,
                "key": key,
            }
        elif relation:
            publication_source = {
                "sourceKind": "relation",
                "sourceId": "s3",
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


def _s3_query_hierarchy(bucket: str, prefix: str = "") -> str:
    bucket_alias = normalize_query_alias_segment(bucket)
    if not bucket_alias:
        return "s3"

    parts = ["s3", bucket_alias]
    parts.extend(
        normalize_query_alias_segment(segment)
        for segment in str(prefix or "").strip("/").split("/")
        if segment.strip()
    )
    return ".".join(part for part in parts if part)


@dataclass(frozen=True, slots=True)
class _S3SourceObjectIndex:
    exact: dict[tuple[str, str], tuple[SourceObject, S3SourceObjectLocation]]
    datasets: dict[tuple[str, str], tuple[SourceObject, S3SourceObjectLocation]]


def _s3_storage_source_objects(
    service: WorkbenchService,
) -> _S3SourceObjectIndex:
    exact: dict[tuple[str, str], tuple[SourceObject, S3SourceObjectLocation]] = {}
    datasets: dict[tuple[str, str], tuple[SourceObject, S3SourceObjectLocation]] = {}
    for catalog in service.catalogs():
        if str(catalog.connection_source_id or "").strip() != "s3":
            continue
        for schema in catalog.schemas:
            for source_object in schema.objects:
                if not (source_object.query_reference or source_object.query_alias):
                    continue
                location = source_object_s3_location(source_object)
                if location is None:
                    continue
                if not location.is_wildcard:
                    exact[(location.bucket, location.key)] = (source_object, location)
                if _is_virtual_dataset_source(source_object, location):
                    datasets[(location.bucket, location.logical_prefix)] = (
                        source_object,
                        location,
                    )
    return _S3SourceObjectIndex(exact=exact, datasets=datasets)


def _is_virtual_dataset_source(
    source_object: SourceObject,
    location: S3SourceObjectLocation,
) -> bool:
    download_kind = str(source_object.s3_download_kind or "").strip()
    return location.is_wildcard or download_kind in {
        "generated_parts",
        "partitioned_parts",
    }


def _source_object_publication_source(
    source_object: SourceObject,
    location: S3SourceObjectLocation,
) -> dict[str, object] | None:
    relation = str(source_object.relation or "").strip()
    if source_object.s3_downloadable and not location.is_wildcard:
        return {
            "sourceKind": "object",
            "sourceId": "s3",
            "bucket": location.bucket,
            "key": location.key,
        }
    if relation:
        return {
            "sourceKind": "relation",
            "sourceId": "s3",
            "relation": relation,
        }
    return None


def _apply_s3_source_object_entry(
    entry: dict[str, object],
    *,
    source_object: SourceObject,
    location: S3SourceObjectLocation,
    dataset_leaf: bool = False,
) -> None:
    display_name = (
        location.leaf_label
        if dataset_leaf
        else str(source_object.display_name or source_object.name or "").strip()
    )
    entry["entryKind"] = "file"
    entry["name"] = display_name
    entry["displayName"] = display_name
    entry["sourceObjectName"] = source_object.name
    entry["relation"] = source_object.relation
    entry["queryAlias"] = source_object.query_alias
    entry["queryReference"] = source_object.query_reference
    entry["querySql"] = source_object.query_sql
    entry["queryPath"] = source_object.query_reference or source_object.query_alias
    entry["prefix"] = location.key
    entry["path"] = source_object.s3_path or f"s3://{location.bucket}/{location.key}"
    if source_object.s3_file_format:
        entry["fileFormat"] = source_object.s3_file_format
    entry["s3Downloadable"] = source_object.s3_downloadable
    entry["sizeBytes"] = int(source_object.size_bytes or 0)
    entry["s3DownloadKind"] = source_object.s3_download_kind
    entry["s3PartPrefix"] = source_object.s3_part_prefix
    entry["s3PartFileFormat"] = source_object.s3_part_file_format
    entry["s3PartCount"] = int(source_object.s3_part_count or 0)
    entry["s3DownloadFilename"] = source_object.s3_download_filename
    entry["s3MergeDownloadable"] = source_object.s3_merge_downloadable
    entry["s3ZipDownloadable"] = source_object.s3_zip_downloadable


def _annotate_s3_breadcrumbs(
    breadcrumbs: object,
) -> list[dict[str, object]]:
    annotated_breadcrumbs: list[dict[str, object]] = []
    for raw_breadcrumb in list(breadcrumbs or []):
        if not isinstance(raw_breadcrumb, dict):
            continue
        breadcrumb = dict(raw_breadcrumb)
        bucket = str(breadcrumb.get("bucket") or "").strip()
        prefix = str(breadcrumb.get("prefix") or "").strip()
        breadcrumb["queryPath"] = _s3_query_hierarchy(bucket, prefix)
        if not bucket:
            breadcrumb["queryLabel"] = "s3"
        elif not prefix:
            breadcrumb["queryLabel"] = normalize_query_alias_segment(bucket)
        else:
            breadcrumb["queryLabel"] = normalize_query_alias_segment(
                str(prefix).strip("/").split("/")[-1]
            )
        annotated_breadcrumbs.append(breadcrumb)
    return annotated_breadcrumbs


def _annotate_s3_snapshot(
    service: WorkbenchService,
    snapshot: dict[str, object],
) -> dict[str, object]:
    annotated_snapshot = dict(snapshot or {})
    bucket = str(annotated_snapshot.get("bucket") or "").strip()
    prefix = str(annotated_snapshot.get("prefix") or "").strip()
    annotated_entries: list[dict[str, object]] = []
    s3_source_objects = _s3_storage_source_objects(service)

    annotated_snapshot["queryPath"] = _s3_query_hierarchy(bucket, prefix)
    annotated_snapshot["breadcrumbs"] = _annotate_s3_breadcrumbs(
        annotated_snapshot.get("breadcrumbs") or []
    )

    for raw_entry in list(annotated_snapshot.get("entries") or []):
        if not isinstance(raw_entry, dict):
            continue
        entry = dict(raw_entry)
        entry_kind = str(entry.get("entryKind") or "").strip()
        publication_source: dict[str, object] | None = None
        entry_bucket = str(entry.get("bucket") or "").strip()
        entry_prefix = str(entry.get("prefix") or "").strip()

        if entry_kind == "bucket":
            if entry_bucket:
                entry["queryPath"] = _s3_query_hierarchy(entry_bucket)
                publication_source = {
                    "sourceKind": "bucket",
                    "sourceId": "s3",
                    "bucket": entry_bucket,
                }
        elif entry_kind == "file":
            entry_key = entry_prefix
            if entry_bucket and entry_key:
                source_record = s3_source_objects.exact.get((entry_bucket, entry_key))
                if source_record:
                    source_object, location = source_record
                    _apply_s3_source_object_entry(
                        entry,
                        source_object=source_object,
                        location=location,
                    )
                    publication_source = _source_object_publication_source(
                        source_object,
                        location,
                    )
                else:
                    publication_source = {
                        "sourceKind": "object",
                        "sourceId": "s3",
                        "bucket": entry_bucket,
                        "key": entry_key,
                    }
        elif entry_bucket and entry_prefix:
            dataset_record = s3_source_objects.datasets.get((entry_bucket, entry_prefix))
            if dataset_record:
                source_object, location = dataset_record
                _apply_s3_source_object_entry(
                    entry,
                    source_object=source_object,
                    location=location,
                    dataset_leaf=True,
                )
                publication_source = _source_object_publication_source(
                    source_object,
                    location,
                )
            else:
                entry["queryPath"] = _s3_query_hierarchy(entry_bucket, entry_prefix)

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
                "sourceId": "s3",
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

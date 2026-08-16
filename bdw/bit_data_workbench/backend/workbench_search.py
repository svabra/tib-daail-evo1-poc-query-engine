from __future__ import annotations

from hashlib import sha256
import json
from urllib.parse import quote

from ..models import SourceCatalog, SourceObject, SourceSchema
from .source_references import pg_source_reference, s3_source_reference


_SOURCE_DEFINITIONS: tuple[dict[str, object], ...] = (
    {
        "sourceId": "workspace.local",
        "title": "Local Workspace",
        "summary": (
            "Browser-local workspace backed by IndexedDB for temporary files "
            "and intermediate results."
        ),
        "tags": ["Workspace Storage", "IndexedDB", "Browser-managed"],
        "type": "IndexedDB",
    },
    {
        "sourceId": "s3",
        "title": "S3 Object Storage",
        "summary": "Shared S3-compatible object storage for files and analytical datasets.",
        "tags": ["Workspace Storage", "S3", "Object Storage"],
        "type": "S3",
    },
    {
        "sourceId": "pg_oltp",
        "title": "PostgreSQL OLTP",
        "summary": "Transactional PostgreSQL source exposed through the workbench query path.",
        "tags": ["RDBMS", "PostgreSQL", "OLTP", "VMTP"],
        "type": "PostgreSQL",
    },
    {
        "sourceId": "pg_oltp_native",
        "title": "PostgreSQL OLTP Direct",
        "summary": (
            "Direct PostgreSQL execution path for the OLTP source without VMTP "
            "indirection."
        ),
        "tags": ["RDBMS", "PostgreSQL", "OLTP", "Native"],
        "type": "PostgreSQL Native",
    },
    {
        "sourceId": "pg_olap",
        "title": "PostgreSQL OLAP",
        "summary": "Analytical PostgreSQL source for read-oriented workloads.",
        "tags": ["RDBMS", "PostgreSQL", "OLAP", "VMTP"],
        "type": "PostgreSQL",
    },
)


def _catalog_source_id(catalog: SourceCatalog) -> str:
    source_id = str(catalog.connection_source_id or "").strip()
    if source_id:
        return source_id
    if catalog.name == "workspace":
        return "s3"
    if catalog.name == "workspace_local":
        return "workspace.local"
    return str(catalog.name or "").strip()


def _catalogs_by_source(catalogs: list[SourceCatalog]) -> dict[str, SourceCatalog]:
    result: dict[str, SourceCatalog] = {}
    for catalog in catalogs:
        source_id = _catalog_source_id(catalog)
        if source_id:
            result[source_id] = catalog
    return result


def _source_search_items(catalogs: list[SourceCatalog]) -> list[dict[str, object]]:
    catalogs_by_source = _catalogs_by_source(catalogs)
    items: list[dict[str, object]] = []
    for definition in _SOURCE_DEFINITIONS:
        source_id = str(definition["sourceId"])
        status_source_id = "pg_oltp" if source_id == "pg_oltp_native" else source_id
        catalog = catalogs_by_source.get(status_source_id)
        tags = list(definition["tags"])
        status = str(catalog.connection_status or "").strip() if catalog else ""
        if status:
            tags.append(status)
        items.append(
            {
                "id": f"source:{source_id}",
                "kind": "source",
                "kindLabel": "Data Source",
                "title": definition["title"],
                "summary": definition["summary"],
                "tags": tags,
                "path": source_id,
                "type": definition["type"],
                "sourceId": source_id,
                "targetUrl": f"/data-sources?source_id={quote(source_id, safe='')}",
            }
        )
    return items


def _object_reference(
    *,
    source_id: str,
    schema: SourceSchema,
    source_object: SourceObject,
) -> str:
    query_reference = str(source_object.query_reference or "").strip()
    if query_reference:
        return query_reference
    if source_id == "s3":
        return s3_source_reference(
            bucket=str(source_object.s3_bucket or schema.label or schema.name or ""),
            key=str(source_object.s3_key or ""),
        ) or str(source_object.s3_path or source_object.relation or "").strip()
    if source_id in {"pg_oltp", "pg_olap"}:
        relation = str(source_object.relation or "").strip()
        if not relation:
            relation = f"{source_id}.{schema.name}.{source_object.name}"
        return pg_source_reference(source_id=source_id, relation=relation)
    return str(source_object.relation or source_object.name or "").strip()


def _object_location(
    *,
    source_id: str,
    schema: SourceSchema,
    source_object: SourceObject,
    reference: str,
) -> str:
    if source_id == "s3":
        return str(source_object.s3_path or "").strip() or reference
    return str(source_object.relation or "").strip() or (
        f"{source_id}.{schema.name}.{source_object.name}"
    )


def _object_search_item(
    *,
    source_id: str,
    schema: SourceSchema,
    source_object: SourceObject,
) -> dict[str, object] | None:
    reference = _object_reference(
        source_id=source_id,
        schema=schema,
        source_object=source_object,
    )
    if not reference:
        return None
    title = str(
        source_object.display_name or source_object.name or reference
    ).strip()
    location = _object_location(
        source_id=source_id,
        schema=schema,
        source_object=source_object,
        reference=reference,
    )
    object_kind = str(source_object.kind or "object").strip() or "object"
    file_format = str(source_object.s3_file_format or "").strip()
    type_label = file_format.upper() if file_format else object_kind.replace("_", " ").title()
    source_label = {
        "s3": "S3",
        "pg_oltp": "PostgreSQL OLTP",
        "pg_olap": "PostgreSQL OLAP",
        "workspace.local": "Local Workspace",
    }.get(source_id, source_id)
    tags = [source_label, str(schema.label or schema.name or "").strip(), object_kind]
    if file_format:
        tags.append(file_format)
    return {
        "id": f"object:{reference}",
        "kind": "object",
        "kindLabel": "Datenobjekt",
        "title": title,
        "summary": f"{type_label} in {source_label}.",
        "tags": [tag for tag in tags if tag],
        "path": location,
        "type": type_label,
        "sourceId": source_id,
        "sourceReference": reference,
        "targetUrl": (
            f"/data-sources/browser?source_id={quote(source_id, safe='')}"
        ),
    }


def workbench_catalog_search_items(
    catalogs: list[SourceCatalog],
) -> list[dict[str, object]]:
    """Build a lightweight index that keeps connectors and their objects distinct."""

    items = _source_search_items(catalogs)
    objects_by_id: dict[str, dict[str, object]] = {}
    for catalog in catalogs:
        source_id = _catalog_source_id(catalog)
        if source_id not in {"s3", "pg_oltp", "pg_olap", "workspace.local"}:
            continue
        for schema in catalog.schemas:
            for source_object in schema.objects:
                item = _object_search_item(
                    source_id=source_id,
                    schema=schema,
                    source_object=source_object,
                )
                if item is not None:
                    objects_by_id[str(item["id"])] = item
    items.extend(objects_by_id.values())
    return sorted(
        items,
        key=lambda item: (
            str(item["kind"]),
            str(item["title"]).casefold(),
            str(item["id"]),
        ),
    )


def versioned_workbench_catalog_search_document(
    catalogs: list[SourceCatalog],
) -> tuple[dict[str, object], str]:
    items = workbench_catalog_search_items(catalogs)
    canonical = json.dumps(
        items,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    version = sha256(canonical).hexdigest()[:16]
    return {"version": version, "items": items}, version

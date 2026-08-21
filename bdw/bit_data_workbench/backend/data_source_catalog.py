from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

from ..config import Settings
from ..models import SourceCatalog


PLATFORM_SOURCE_IDS = ("workspace.local", "s3", "pg_oltp", "pg_olap")
SUPPORTED_INGESTION_FORMATS = frozenset({"csv", "json", "jsonl", "ndjson", "parquet"})


def canonical_data_source_id(source_id: object) -> str:
    normalized = str(source_id or "").strip()
    return "pg_oltp" if normalized == "pg_oltp_native" else normalized


def _catalog_metrics(catalog: SourceCatalog | None) -> tuple[int, int]:
    if catalog is None:
        return 0, 0
    return len(catalog.schemas), sum(len(schema.objects) for schema in catalog.schemas)


def _runtime_status(catalog: SourceCatalog | None, *, configured: bool) -> tuple[str, str]:
    state = str(getattr(catalog, "connection_status", "") or "").strip().lower()
    _schemas, objects = _catalog_metrics(catalog)
    if state in {"connected", "available", "ready"} or objects:
        return "available", str(getattr(catalog, "connection_label", "") or "Available")
    if state in {"disconnected", "failed", "error"}:
        return "unavailable", str(getattr(catalog, "connection_label", "") or "Unavailable")
    if configured:
        return "configured", "Configured"
    return "unavailable", "Not configured"


def _relation_count(catalog: SourceCatalog | None, *, supported_only: bool = False) -> int:
    if catalog is None:
        return 0
    return sum(
        1
        for schema in catalog.schemas
        for source_object in schema.objects
        if not supported_only
        or str(source_object.s3_file_format or "").strip().lower() in SUPPORTED_INGESTION_FORMATS
    )


def build_data_source_records(
    settings: Settings,
    catalogs: Iterable[SourceCatalog],
) -> list[dict[str, Any]]:
    catalog_list = list(catalogs)
    by_name = {catalog.name: catalog for catalog in catalog_list}
    records: list[dict[str, Any]] = []

    records.append(
        {
            "id": "workspace.local",
            "name": "Local Workspace",
            "technology": "Local Workspace",
            "technologyKey": "local",
            "kind": "local-browser",
            "status": "reference",
            "statusLabel": "Reference",
            "location": "Current browser",
            "schemaCount": 0,
            "objectCount": 0,
            "accessModel": "Browser-local access",
            "ingestionCapable": False,
            "managementPath": "/data-sources?source_id=workspace.local",
            "browsePath": "/data-sources/browser?source_id=workspace.local",
            "summary": "Browser-local scratch data and temporary workspace artifacts.",
        }
    )

    s3_catalog = by_name.get("workspace")
    s3_configured = bool(str(settings.s3_endpoint or "").strip() and str(settings.s3_bucket or "").strip())
    s3_status, s3_status_label = _runtime_status(s3_catalog, configured=s3_configured)
    s3_schemas, s3_objects = _catalog_metrics(s3_catalog)
    records.append(
        {
            "id": "s3",
            "name": "Shared Workspace",
            "technology": "MinIO / S3",
            "technologyKey": "s3",
            "kind": "object-storage",
            "status": s3_status,
            "statusLabel": s3_status_label,
            "location": "BIT object storage",
            "schemaCount": s3_schemas,
            "objectCount": s3_objects,
            "accessModel": "Platform source · runtime access",
            "ingestionCapable": s3_status == "available" and _relation_count(s3_catalog, supported_only=True) > 0,
            "managementPath": "/data-sources?source_id=s3",
            "browsePath": "/data-sources/browser?source_id=s3",
            "summary": "Shared bucket-backed object storage with queryable CSV, JSON and Parquet objects.",
        }
    )

    for source_id, name, database in (
        ("pg_oltp", "PostgreSQL OLTP", settings.pg_oltp_database),
        ("pg_olap", "PostgreSQL OLAP", settings.pg_olap_database),
    ):
        catalog = by_name.get(source_id)
        configured = bool(str(settings.pg_host or "").strip() and str(database or "").strip())
        status, status_label = _runtime_status(catalog, configured=configured)
        schemas, objects = _catalog_metrics(catalog)
        records.append(
            {
                "id": source_id,
                "name": name,
                "technology": "PostgreSQL",
                "technologyKey": "postgresql",
                "kind": "postgres",
                "status": status,
                "statusLabel": status_label,
                "location": "BIT data platform",
                "schemaCount": schemas,
                "objectCount": objects,
                "accessModel": "Platform source · runtime access",
                "accessPaths": ["VMTP", "Native"] if source_id == "pg_oltp" else ["VMTP"],
                "ingestionCapable": status == "available" and objects > 0,
                "managementPath": f"/data-sources?source_id={source_id}",
                "browsePath": f"/data-sources/browser?source_id={source_id}",
                "summary": "Transactional PostgreSQL source." if source_id == "pg_oltp" else "Analytical PostgreSQL source.",
            }
        )

    for catalog in catalog_list:
        source_id = canonical_data_source_id(catalog.connection_source_id or catalog.name)
        if not source_id.startswith("ora_"):
            continue
        schemas, objects = _catalog_metrics(catalog)
        records.append(
            {
                "id": source_id,
                "name": catalog.display_name or source_id,
                "technology": "Oracle",
                "technologyKey": "oracle",
                "kind": "oracle-poc",
                "status": "available",
                "statusLabel": "Grant active",
                "location": catalog.site_label or "BIT data center",
                "schemaCount": schemas,
                "objectCount": objects,
                "accessModel": "DaCa grant · checked per run",
                "ingestionCapable": objects > 0,
                "managementPath": f"/data-sources?source_id={source_id}",
                "browsePath": f"/data-sources/browser?source_id={source_id}",
                "summary": catalog.connection_detail or "Governed Oracle PoC source with synthetic read-only data.",
                "owner": catalog.owner_label,
                "databaseName": catalog.database_name,
            }
        )

    return records


def data_source_catalog_payload(
    settings: Settings,
    catalogs: Iterable[SourceCatalog],
    *,
    query: str = "",
    technology: str = "",
    status: str = "",
    location: str = "",
    ingestion_capable: bool | None = None,
    offset: int = 0,
    limit: int = 25,
) -> dict[str, Any]:
    records = build_data_source_records(settings, catalogs)
    facets = {
        "technologies": dict(sorted(Counter(item["technology"] for item in records).items())),
        "statuses": dict(sorted(Counter(item["status"] for item in records).items())),
        "locations": dict(sorted(Counter(item["location"] for item in records).items())),
    }
    needle = str(query or "").strip().casefold()
    technology_key = str(technology or "").strip().casefold()
    status_key = str(status or "").strip().casefold()
    location_key = str(location or "").strip().casefold()
    filtered = [
        item
        for item in records
        if (not needle or needle in " ".join(str(value or "") for value in item.values()).casefold())
        and (not technology_key or technology_key in {str(item["technology"]).casefold(), str(item["technologyKey"]).casefold()})
        and (not status_key or status_key == str(item["status"]).casefold())
        and (not location_key or location_key in str(item["location"]).casefold())
        and (ingestion_capable is None or bool(item["ingestionCapable"]) is ingestion_capable)
    ]
    safe_offset = max(0, int(offset))
    safe_limit = min(100, max(1, int(limit)))
    page = filtered[safe_offset : safe_offset + safe_limit]
    return {
        "summary": {
            "total": len(records),
            "available": sum(1 for item in records if item["status"] == "available"),
            "schemas": sum(int(item["schemaCount"]) for item in records),
            "objects": sum(int(item["objectCount"]) for item in records),
            "matched": len(filtered),
        },
        "facets": facets,
        "pagination": {
            "offset": safe_offset,
            "limit": safe_limit,
            "total": len(filtered),
            "hasPrevious": safe_offset > 0,
            "hasNext": safe_offset + safe_limit < len(filtered),
        },
        "items": page,
    }

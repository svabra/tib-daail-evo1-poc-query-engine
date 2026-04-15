from __future__ import annotations

from typing import Any

from ....models import SourceCatalog


def attach_query_sources_to_csv_imports(
    payload: dict[str, Any],
    catalogs: list[SourceCatalog],
) -> dict[str, Any]:
    next_payload = dict(payload)
    target_id = str(payload.get("targetId") or "").strip()
    imports = []
    first_query_source: dict[str, str] | None = None

    for item in payload.get("imports", []) or []:
        next_item = dict(item)
        query_source = resolve_query_source_for_csv_import(
            item=next_item,
            target_id=target_id,
            catalogs=catalogs,
        )
        if query_source:
            next_item["querySource"] = query_source
            if first_query_source is None:
                first_query_source = dict(query_source)
        elif (
            str(next_item.get("status") or "").strip().lower() == "imported"
            and target_id == "workspace.s3"
        ):
            next_item["queryUnavailableReason"] = (
                "Saved to Shared Workspace S3, but the query source is not visible yet. "
                "Refresh Data Sources and try again."
            )
        imports.append(next_item)

    next_payload["imports"] = imports
    if first_query_source is not None:
        next_payload["firstQuerySource"] = first_query_source
    return next_payload


def resolve_query_source_for_csv_import(
    *,
    item: dict[str, Any],
    target_id: str,
    catalogs: list[SourceCatalog],
) -> dict[str, str] | None:
    if str(item.get("status") or "").strip().lower() != "imported":
        return None

    normalized_target_id = str(target_id or "").strip()
    if normalized_target_id in {"pg_oltp", "pg_olap"}:
        relation = str(item.get("relation") or "").strip()
        if not relation:
            return None
        source_relation = f"{normalized_target_id}.{relation}"
        return _find_query_source(
            catalogs,
            source_id=normalized_target_id,
            relation=source_relation,
        )

    if normalized_target_id == "workspace.s3":
        path = str(item.get("path") or "").strip()
        if not path:
            return None
        return _find_query_source(catalogs, source_id="workspace.s3", s3_path=path)

    return None


def _find_query_source(
    catalogs: list[SourceCatalog],
    *,
    source_id: str,
    relation: str = "",
    s3_path: str = "",
) -> dict[str, str] | None:
    normalized_source_id = str(source_id or "").strip()
    normalized_relation = str(relation or "").strip()
    normalized_s3_path = str(s3_path or "").strip()

    for catalog, schema, source_object in _iter_catalog_source_objects(catalogs):
        catalog_source_id = str(catalog.connection_source_id or catalog.name).strip()
        if catalog_source_id != normalized_source_id:
            continue
        if normalized_relation and source_object.relation != normalized_relation:
            continue
        if normalized_s3_path and str(source_object.s3_path or "").strip() != normalized_s3_path:
            continue
        return {
            "sourceId": catalog_source_id,
            "catalogName": catalog.name,
            "schemaName": schema.name,
            "schemaLabel": str(schema.label or schema.name),
            "relation": source_object.relation,
            "name": source_object.name,
        }

    return None


def _iter_catalog_source_objects(
    catalogs: list[SourceCatalog],
):
    for catalog in catalogs:
        for schema in catalog.schemas:
            for source_object in schema.objects:
                yield catalog, schema, source_object

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from ...models import SourceCatalog, SourceObject, SourceSchema


def catalog_source_id(catalog: SourceCatalog) -> str:
    return str(catalog.connection_source_id or catalog.name or "").strip()


def schema_publication_source(
    catalog: SourceCatalog,
    schema: SourceSchema,
) -> dict[str, object] | None:
    source_id = catalog_source_id(catalog)
    if source_id != "workspace.s3":
        return None

    bucket = str(schema.label or schema.name or "").strip()
    if not bucket:
        return None

    return {
        "sourceKind": "bucket",
        "sourceId": "workspace.s3",
        "bucket": bucket,
        "sourceDisplayName": bucket,
        "sourcePlatform": "s3",
    }


def object_publication_source(
    catalog: SourceCatalog,
    source_object: SourceObject,
) -> dict[str, object] | None:
    source_id = catalog_source_id(catalog)
    relation = str(source_object.relation or "").strip()

    if source_id == "workspace.s3":
        bucket = str(source_object.s3_bucket or "").strip()
        key = str(source_object.s3_key or "").strip()
        if source_object.s3_downloadable and bucket and key:
            return {
                "sourceKind": "object",
                "sourceId": "workspace.s3",
                "bucket": bucket,
                "key": key,
                "sourceDisplayName": str(
                    source_object.display_name or source_object.name or key
                ).strip(),
                "sourcePlatform": "s3",
            }

        if relation:
            return {
                "sourceKind": "relation",
                "sourceId": "workspace.s3",
                "relation": relation,
                "sourceDisplayName": str(
                    source_object.display_name or source_object.name or relation
                ).strip(),
                "sourcePlatform": "s3",
            }
        return None

    if not relation:
        return None

    return {
        "sourceKind": "relation",
        "sourceId": source_id,
        "relation": relation,
        "sourceDisplayName": str(
            source_object.display_name or source_object.name or relation
        ).strip(),
        "sourcePlatform": "postgres",
    }


def annotate_catalogs_with_published_products(
    catalogs: list[SourceCatalog],
    *,
    publication_links_for_source: Callable[[dict[str, object]], list[dict[str, object]]],
) -> list[SourceCatalog]:
    annotated_catalogs: list[SourceCatalog] = []

    for catalog in catalogs:
        annotated_schemas: list[SourceSchema] = []

        for schema in catalog.schemas:
            schema_source = schema_publication_source(catalog, schema)
            schema_publications = (
                publication_links_for_source(schema_source) if schema_source else []
            )

            annotated_objects = [
                replace(
                    source_object,
                    published_data_products=publication_links_for_source(source)
                    if (source := object_publication_source(catalog, source_object))
                    else [],
                )
                for source_object in schema.objects
            ]

            annotated_schemas.append(
                replace(
                    schema,
                    objects=annotated_objects,
                    published_data_products=schema_publications,
                )
            )

        annotated_catalogs.append(replace(catalog, schemas=annotated_schemas))

    return annotated_catalogs

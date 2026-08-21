from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from ..models import SourceCatalog, SourceObject
from .data_source_catalog import SUPPORTED_INGESTION_FORMATS, canonical_data_source_id
from .source_references import join_reference_parts, pg_source_reference, s3_source_reference
from .source_sourcing import SourceSourcingCoordinator, SourceSourcingError


PLATFORM_INGESTION_SOURCE_IDS = frozenset({"pg_oltp", "pg_olap", "s3"})


@dataclass(slots=True)
class SourceResolutionError(RuntimeError):
    status_code: int
    detail: str

    def __str__(self) -> str:
        return self.detail


def normalize_ingestion_source_id(value: object) -> str:
    source_id = canonical_data_source_id(value).lower()
    if source_id in PLATFORM_INGESTION_SOURCE_IDS or (
        source_id.startswith("ora_")
        and all(character.isalnum() or character == "_" for character in source_id)
    ):
        return source_id
    raise SourceResolutionError(422, "Choose an available Oracle, PostgreSQL or S3 source.")


def normalize_relation_selector(value: object, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 512 or any(ord(character) < 32 for character in normalized):
        raise SourceResolutionError(422, f"Choose a valid source {label}.")
    return normalized


class SourceIngestionSourceResolver:
    """Resolves trusted source catalog objects; request payloads never provide SQL."""

    def __init__(
        self,
        source_sourcing: SourceSourcingCoordinator,
        *,
        catalog_provider: Callable[[str], Iterable[SourceCatalog]] | None = None,
        fields_provider: Callable[[str, str, SourceObject], list[object]] | None = None,
    ) -> None:
        self._source_sourcing = source_sourcing
        self._catalog_provider = catalog_provider
        self._fields_provider = fields_provider

    def catalogs(self, actor: str) -> list[SourceCatalog]:
        if self._catalog_provider is not None:
            return list(self._catalog_provider(actor))
        try:
            return list(self._source_sourcing.catalogs_for_actor(actor))
        except SourceSourcingError as exc:
            raise SourceResolutionError(exc.status_code, exc.detail) from exc

    @staticmethod
    def _catalog_source_id(catalog: SourceCatalog) -> str:
        if catalog.name == "workspace":
            return "s3"
        return canonical_data_source_id(catalog.connection_source_id or catalog.name)

    def context(self, actor: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for catalog in self.catalogs(actor):
            source_id = self._catalog_source_id(catalog)
            if source_id not in PLATFORM_INGESTION_SOURCE_IDS and not source_id.startswith("ora_"):
                continue
            source_kind, technology, access_model = self._source_metadata(source_id)
            relations = []
            for schema in catalog.schemas:
                for source_object in schema.objects:
                    if source_id == "s3" and not self._supported_s3_object(source_object):
                        continue
                    relations.append(
                        {
                            "schema": schema.name,
                            "name": source_object.name,
                            "kind": source_object.kind,
                            "relation": source_object.relation,
                            "displayName": source_object.display_name or source_object.name,
                            "format": source_object.s3_file_format if source_id == "s3" else "",
                        }
                    )
            if source_id.startswith("ora_") and not relations:
                continue
            items.append(
                {
                    "id": source_id,
                    "displayName": catalog.display_name or self._display_name(source_id),
                    "databaseName": catalog.database_name or catalog.name,
                    "platform": catalog.source_platform or technology,
                    "technology": technology,
                    "technologyKey": "oracle" if source_id.startswith("ora_") else ("s3" if source_id == "s3" else "postgresql"),
                    "sourceKind": source_kind,
                    "accessModel": access_model,
                    "site": catalog.site_label or self._location(source_id),
                    "owner": catalog.owner_label or "Data Platform BIT",
                    "relations": relations,
                }
            )
        return sorted(items, key=lambda item: (str(item["technology"]), str(item["displayName"])))

    def resolve(
        self,
        actor: str,
        source_id: str,
        schema_name: str,
        relation_name: str,
        *,
        refresh: bool,
    ) -> dict[str, Any]:
        normalized_source_id = normalize_ingestion_source_id(source_id)
        requested_schema = normalize_relation_selector(schema_name, "schema or bucket")
        requested_relation = normalize_relation_selector(relation_name, "relation")
        if normalized_source_id.startswith("ora_"):
            try:
                active = self._source_sourcing.active_oracle_sources(actor, refresh=refresh)
            except SourceSourcingError as exc:
                raise SourceResolutionError(exc.status_code, exc.detail) from exc
            if normalized_source_id not in {str(item.get("id") or "") for item in active}:
                raise SourceResolutionError(403, "An active DaCa source grant is required.")

        catalogs = self.catalogs(actor)
        for catalog in catalogs:
            if self._catalog_source_id(catalog) != normalized_source_id:
                continue
            for schema in catalog.schemas:
                if schema.name.casefold() != requested_schema.casefold():
                    continue
                for source_object in schema.objects:
                    if source_object.name.casefold() != requested_relation.casefold():
                        continue
                    if normalized_source_id == "s3" and not self._supported_s3_object(source_object):
                        raise SourceResolutionError(422, "Only discovered CSV, JSON/JSONL or Parquet objects can be ingested.")
                    fields = self._fields(actor, normalized_source_id, source_object)
                    if not fields:
                        raise SourceResolutionError(422, "The selected source relation has no schema fields.")
                    source_kind, technology, access_model = self._source_metadata(normalized_source_id)
                    query_reference = self._query_reference(
                        normalized_source_id, schema.name, source_object
                    )
                    if not query_reference:
                        raise SourceResolutionError(422, "The selected source relation has no safe query reference.")
                    locator: dict[str, Any] = {
                        "sourceId": normalized_source_id,
                        "schema": schema.name,
                        "relation": source_object.name,
                        "relationKind": source_object.kind,
                        "queryReference": query_reference,
                    }
                    if normalized_source_id == "s3":
                        locator.update(
                            {
                                "bucket": source_object.s3_bucket or schema.name,
                                "key": source_object.s3_key,
                                "path": source_object.s3_path,
                                "format": self._normalized_s3_format(source_object),
                                "partPrefix": source_object.s3_part_prefix,
                            }
                        )
                    return {
                        "sourceId": normalized_source_id,
                        "sourceKind": source_kind,
                        "technology": technology,
                        "accessModel": access_model,
                        "schema": schema.name,
                        "name": source_object.name,
                        "kind": source_object.kind,
                        "queryReference": query_reference,
                        "locator": locator,
                    }
        label = "Oracle relation" if normalized_source_id.startswith("ora_") else "source relation"
        raise SourceResolutionError(404, f"The {label} is not available to this user.")

    def _fields(self, actor: str, source_id: str, source_object: SourceObject) -> list[object]:
        if self._fields_provider is not None:
            try:
                return list(self._fields_provider(actor, source_id, source_object))
            except (KeyError, ValueError) as exc:
                raise SourceResolutionError(404, str(exc)) from exc
            except Exception as exc:
                raise SourceResolutionError(503, f"The {self._display_name(source_id)} source is unavailable: {exc}") from exc
        if source_id.startswith("ora_"):
            try:
                return list(self._source_sourcing.fields_for_relation(actor, source_object.relation))
            except SourceSourcingError as exc:
                raise SourceResolutionError(exc.status_code, exc.detail) from exc
        return [object()] if source_object.query_reference or source_object.query_sql else []

    @staticmethod
    def _normalized_s3_format(source_object: SourceObject) -> str:
        value = str(source_object.s3_file_format or "").strip().lower()
        return "json" if value in {"jsonl", "ndjson"} else value

    @classmethod
    def _supported_s3_object(cls, source_object: SourceObject) -> bool:
        return cls._normalized_s3_format(source_object) in {"csv", "json", "parquet"}

    @staticmethod
    def _query_reference(source_id: str, schema_name: str, source_object: SourceObject) -> str:
        trusted = str(source_object.query_reference or "").strip()
        if trusted:
            return trusted
        if source_id in {"pg_oltp", "pg_olap"}:
            return pg_source_reference(
                source_id=source_id,
                relation=source_object.relation or f"{source_id}.{schema_name}.{source_object.name}",
            )
        if source_id == "s3":
            return s3_source_reference(
                bucket=source_object.s3_bucket or schema_name,
                key=source_object.s3_key,
            )
        return join_reference_parts((source_id, schema_name, source_object.name))

    @staticmethod
    def _source_metadata(source_id: str) -> tuple[str, str, str]:
        if source_id.startswith("ora_"):
            return "oracle-poc", "Oracle", "DaCa grant · checked per run"
        if source_id in {"pg_oltp", "pg_olap"}:
            return "postgresql", "PostgreSQL", "Platform source · runtime access"
        return "s3-object", "MinIO / S3", "Platform source · runtime access"

    @staticmethod
    def _display_name(source_id: str) -> str:
        return {"pg_oltp": "PostgreSQL OLTP", "pg_olap": "PostgreSQL OLAP", "s3": "Shared Workspace"}.get(source_id, source_id)

    @staticmethod
    def _location(source_id: str) -> str:
        return "BIT object storage" if source_id == "s3" else "BIT data platform"

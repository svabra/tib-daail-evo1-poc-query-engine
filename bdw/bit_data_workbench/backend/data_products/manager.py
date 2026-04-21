from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
import mimetypes
from pathlib import PurePosixPath
import re
import uuid

import duckdb

from ...config import Settings
from ...models import DataProductDefinition, DataProductSourceDescriptor, SourceCatalog
from ..data_sources.s3.explorer import normalize_s3_prefix
from ..s3_storage import list_s3_buckets, s3_client
from ..sql_utils import qualified_name
from .registry import DataProductStore


DEFAULT_PUBLIC_DATA_PRODUCT_LIMIT = 100
MAX_PUBLIC_DATA_PRODUCT_LIMIT = 1000
PUBLIC_DATA_PRODUCTS_PATH_PREFIX = "/api/public/data-products"
SUPPORTED_ACCESS_LEVELS = {"internal", "restricted", "confidential"}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    normalized = normalized.strip("-")
    if not normalized:
        raise ValueError("Provide a title or slug for the data product.")
    return normalized


def normalize_tags(tags: list[str] | tuple[str, ...] | None) -> list[str]:
    unique_tags: list[str] = []
    seen: set[str] = set()

    for value in tags or []:
        tag = str(value or "").strip()
        if not tag:
            continue
        normalized = tag.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_tags.append(tag)

    return unique_tags


def normalize_access_level(value: str) -> str:
    normalized = str(value or "").strip().lower() or "internal"
    if normalized not in SUPPORTED_ACCESS_LEVELS:
        raise ValueError(
            "Access level must be one of: internal, restricted, confidential."
        )
    return normalized


def relation_identifier(relation: str) -> str:
    parts = [part.strip() for part in str(relation or "").split(".") if part.strip()]
    if len(parts) not in {2, 3}:
        raise ValueError(f"Unsupported relation identifier: {relation}")
    return qualified_name(*parts)


@dataclass(slots=True)
class DataProductPublicStreamArtifact:
    filename: str
    content_type: str
    content_length: int | None
    iterator: Iterator[bytes]


class DataProductManager:
    def __init__(
        self,
        *,
        settings: Settings,
        store: DataProductStore,
        create_worker_connection: Callable[[], duckdb.DuckDBPyConnection],
        relation_fields_provider: Callable[[str], list],
        catalog_provider: Callable[[], list[SourceCatalog]],
        s3_bucket_snapshot_provider: Callable[..., dict[str, object]],
    ) -> None:
        self._settings = settings
        self._store = store
        self._create_worker_connection = create_worker_connection
        self._relation_fields_provider = relation_fields_provider
        self._catalog_provider = catalog_provider
        self._s3_bucket_snapshot_provider = s3_bucket_snapshot_provider

    def list_products(
        self,
        *,
        base_url: str | None = None,
    ) -> list[dict[str, object]]:
        products = sorted(
            self._store.list_products(),
            key=lambda item: (item.updated_at, item.title.lower(), item.product_id),
            reverse=True,
        )
        return [product.payload(base_url=base_url) for product in products]

    def source_options(self) -> list[dict[str, object]]:
        options: list[dict[str, object]] = []
        for catalog in self._catalog_provider():
            source_id = str(catalog.connection_source_id or catalog.name).strip()
            if source_id == "workspace.local":
                continue

            if source_id == "workspace.s3":
                for schema in catalog.schemas:
                    bucket_name = str(schema.label or schema.name or "").strip()
                    if bucket_name:
                        options.append(
                            {
                                "optionId": f"bucket::{bucket_name}",
                                "label": f"Shared Workspace bucket / {bucket_name}",
                                "description": f"s3://{bucket_name}/",
                                "source": DataProductSourceDescriptor(
                                    source_kind="bucket",
                                    source_id="workspace.s3",
                                    bucket=bucket_name,
                                    source_display_name=bucket_name,
                                    source_platform="s3",
                                ).payload,
                            }
                        )

                    for source_object in schema.objects:
                        source_display_name = (
                            str(source_object.display_name or source_object.name or "").strip()
                            or bucket_name
                        )
                        if (
                            source_object.s3_downloadable
                            and str(source_object.s3_bucket or "").strip()
                            and str(source_object.s3_key or "").strip()
                        ):
                            options.append(
                                {
                                    "optionId": (
                                        "object::"
                                        f"{source_object.s3_bucket}::{source_object.s3_key}"
                                    ),
                                    "label": f"Shared Workspace object / {source_display_name}",
                                    "description": str(source_object.s3_path or "").strip()
                                    or f"s3://{source_object.s3_bucket}/{source_object.s3_key}",
                                    "source": DataProductSourceDescriptor(
                                        source_kind="object",
                                        source_id="workspace.s3",
                                        bucket=str(source_object.s3_bucket or "").strip(),
                                        key=str(source_object.s3_key or "").strip(),
                                        source_display_name=source_display_name,
                                        source_platform="s3",
                                    ).payload,
                                }
                            )
                            continue

                        relation = str(source_object.relation or "").strip()
                        if relation:
                            options.append(
                                {
                                    "optionId": f"relation::{relation}",
                                    "label": f"Shared Workspace relation / {source_display_name}",
                                    "description": relation,
                                    "source": DataProductSourceDescriptor(
                                        source_kind="relation",
                                        source_id="workspace.s3",
                                        relation=relation,
                                        source_display_name=source_display_name,
                                        source_platform="s3",
                                    ).payload,
                                }
                            )
                continue

            for schema in catalog.schemas:
                for source_object in schema.objects:
                    relation = str(source_object.relation or "").strip()
                    if not relation:
                        continue
                    source_display_name = (
                        str(source_object.display_name or source_object.name or "").strip()
                        or relation
                    )
                    options.append(
                        {
                            "optionId": f"relation::{relation}",
                            "label": f"{source_id} relation / {source_display_name}",
                            "description": relation,
                            "source": DataProductSourceDescriptor(
                                source_kind="relation",
                                source_id=source_id,
                                relation=relation,
                                source_display_name=source_display_name,
                                source_platform="postgres",
                            ).payload,
                        }
                    )

        return options

    def preview_product(
        self,
        *,
        source: dict[str, object],
        title: str = "",
        slug: str = "",
        description: str = "",
        owner: str = "",
        domain: str = "",
        tags: list[str] | None = None,
        access_level: str = "internal",
        access_note: str = "",
        request_access_contact: str = "",
        custom_properties: dict[str, str] | None = None,
        base_url: str | None = None,
    ) -> dict[str, object]:
        descriptor = self._resolve_source_descriptor(source, allow_unsupported=True)
        normalized_title = self._normalized_title(title, descriptor)
        normalized_slug = normalize_slug(slug or normalized_title)
        blocked_reason = descriptor.unsupported_reason
        if not blocked_reason and self._slug_exists(normalized_slug):
            blocked_reason = (
                f"The data product slug '{normalized_slug}' is already in use."
            )

        normalized_access_level = (
            normalize_access_level(access_level)
            if not blocked_reason
            else str(access_level or "").strip().lower() or "internal"
        )
        product = DataProductDefinition(
            product_id="preview",
            slug=normalized_slug,
            title=normalized_title,
            description=str(description or "").strip(),
            source=descriptor,
            public_path=self.public_path(normalized_slug),
            owner=str(owner or "").strip(),
            domain=str(domain or "").strip(),
            tags=normalize_tags(tags),
            access_level=normalized_access_level,
            access_note=str(access_note or "").strip(),
            request_access_contact=str(request_access_contact or "").strip(),
            custom_properties={
                str(name).strip(): str(value).strip()
                for name, value in (custom_properties or {}).items()
                if str(name).strip() and str(value).strip()
            },
        )
        return {
            "compatible": not blocked_reason,
            "blocked": bool(blocked_reason),
            "blockedReason": blocked_reason,
            **self._documentation_contract(product, base_url=base_url),
            "openApiNamespace": PUBLIC_DATA_PRODUCTS_PATH_PREFIX + "/{slug}",
        }

    def create_product(
        self,
        *,
        source: dict[str, object],
        title: str,
        slug: str = "",
        description: str = "",
        owner: str = "",
        domain: str = "",
        tags: list[str] | None = None,
        access_level: str = "internal",
        access_note: str = "",
        request_access_contact: str = "",
        custom_properties: dict[str, str] | None = None,
        base_url: str | None = None,
    ) -> dict[str, object]:
        descriptor = self._resolve_source_descriptor(source, allow_unsupported=False)
        normalized_title = self._normalized_title(title, descriptor)
        normalized_slug = normalize_slug(slug or normalized_title)
        if self._slug_exists(normalized_slug):
            raise ValueError(
                f"The data product slug '{normalized_slug}' is already in use."
            )

        now = utc_now_iso()
        product = DataProductDefinition(
            product_id=f"data-product-{uuid.uuid4().hex}",
            slug=normalized_slug,
            title=normalized_title,
            description=str(description or "").strip(),
            source=descriptor,
            public_path=self.public_path(normalized_slug),
            owner=str(owner or "").strip(),
            domain=str(domain or "").strip(),
            tags=normalize_tags(tags),
            access_level=normalize_access_level(access_level),
            access_note=str(access_note or "").strip(),
            request_access_contact=str(request_access_contact or "").strip(),
            custom_properties={
                str(name).strip(): str(value).strip()
                for name, value in (custom_properties or {}).items()
                if str(name).strip() and str(value).strip()
            },
            created_at=now,
            updated_at=now,
        )
        created = self._store.create_product(product)
        return {
            "action": "created",
            "product": created.payload(base_url=base_url),
        }

    def update_product_metadata(
        self,
        *,
        product_id: str,
        title: str,
        description: str = "",
        owner: str = "",
        domain: str = "",
        tags: list[str] | None = None,
        access_level: str = "internal",
        access_note: str = "",
        request_access_contact: str = "",
        custom_properties: dict[str, str] | None = None,
        base_url: str | None = None,
    ) -> dict[str, object]:
        existing = self.product_by_id(product_id)
        updated = DataProductDefinition(
            product_id=existing.product_id,
            slug=existing.slug,
            title=self._normalized_title(title, existing.source),
            description=str(description or "").strip(),
            source=existing.source,
            public_path=existing.public_path,
            publication_mode=existing.publication_mode,
            owner=str(owner or "").strip(),
            domain=str(domain or "").strip(),
            tags=normalize_tags(tags),
            access_level=normalize_access_level(access_level),
            access_note=str(access_note or "").strip(),
            request_access_contact=str(request_access_contact or "").strip(),
            custom_properties={
                str(name).strip(): str(value).strip()
                for name, value in (custom_properties or {}).items()
                if str(name).strip() and str(value).strip()
            },
            created_at=existing.created_at,
            updated_at=utc_now_iso(),
        )
        self._store.update_product(updated)
        return {
            "action": "updated",
            "product": updated.payload(base_url=base_url),
        }

    def delete_product(
        self,
        *,
        product_id: str,
        base_url: str | None = None,
    ) -> dict[str, object]:
        removed = self._store.delete_product(product_id)
        return {
            "action": "deleted",
            "product": removed.payload(base_url=base_url),
        }

    def product_by_id(self, product_id: str) -> DataProductDefinition:
        normalized_product_id = str(product_id or "").strip()
        for product in self._store.list_products():
            if product.product_id == normalized_product_id:
                return product
        raise KeyError(f"Unknown data product: {product_id}")

    def product_by_slug(self, slug: str) -> DataProductDefinition:
        normalized_slug = str(slug or "").strip().lower()
        for product in self._store.list_products():
            if product.slug.lower() == normalized_slug:
                return product
        raise KeyError(f"Unknown data product: {slug}")

    def documentation_payload(
        self,
        *,
        slug: str,
        base_url: str | None = None,
    ) -> dict[str, object]:
        product = self.product_by_slug(slug)
        return self._documentation_contract(product, base_url=base_url)

    def public_relation_payload(
        self,
        *,
        slug: str,
        limit: int,
        offset: int,
        base_url: str | None = None,
    ) -> dict[str, object]:
        product = self.product_by_slug(slug)
        if product.source.source_kind != "relation" or not product.source.relation:
            raise KeyError(f"Data product '{slug}' does not publish relation rows.")

        normalized_limit = self._normalized_limit(limit)
        normalized_offset = self._normalized_offset(offset)
        fields = self._relation_fields_provider(product.source.relation)
        sql = (
            f"SELECT * FROM {relation_identifier(product.source.relation)} "
            f"LIMIT {normalized_limit + 1} OFFSET {normalized_offset}"
        )
        connection = self._create_worker_connection()
        try:
            rows = connection.execute(sql).fetchall()
        finally:
            connection.close()

        has_more = len(rows) > normalized_limit
        visible_rows = rows[:normalized_limit]
        column_names = [field.name for field in fields]
        items = [
            {
                column_names[index]: row[index]
                for index in range(min(len(column_names), len(row)))
            }
            for row in visible_rows
        ]
        return {
            "product": product.payload(base_url=base_url),
            "columns": [field.payload for field in fields],
            "items": items,
            "limit": normalized_limit,
            "offset": normalized_offset,
            "hasMore": has_more,
        }

    def public_bucket_payload(
        self,
        *,
        slug: str,
        prefix: str = "",
        base_url: str | None = None,
    ) -> dict[str, object]:
        product = self.product_by_slug(slug)
        if product.source.source_kind != "bucket" or not product.source.bucket:
            raise KeyError(f"Data product '{slug}' does not publish a bucket listing.")

        normalized_prefix = normalize_s3_prefix(prefix)
        snapshot = self._s3_bucket_snapshot_provider(
            bucket=product.source.bucket,
            prefix=normalized_prefix,
        )
        return {
            "product": product.payload(base_url=base_url),
            "prefix": normalized_prefix,
            "entries": list(snapshot.get("entries") or []),
        }

    def public_object_stream(
        self,
        *,
        slug: str,
    ) -> DataProductPublicStreamArtifact:
        product = self.product_by_slug(slug)
        if (
            product.source.source_kind != "object"
            or not product.source.bucket
            or not product.source.key
        ):
            raise KeyError(f"Data product '{slug}' does not publish an object stream.")

        client = s3_client(self._settings)
        response = client.get_object(
            Bucket=product.source.bucket,
            Key=product.source.key,
        ) or {}
        body = response.get("Body")
        if body is None:
            raise KeyError(
                f"Published object '{product.source.bucket}/{product.source.key}' is unavailable."
            )

        content_type = str(response.get("ContentType") or "").strip()
        if not content_type:
            content_type = (
                mimetypes.guess_type(product.source.key)[0]
                or "application/octet-stream"
            )
        filename = PurePosixPath(product.source.key).name or product.slug
        content_length_value = response.get("ContentLength")
        content_length = (
            int(content_length_value) if content_length_value is not None else None
        )

        def iter_chunks() -> Iterator[bytes]:
            try:
                for chunk in body.iter_chunks(chunk_size=64 * 1024):
                    if chunk:
                        yield chunk
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()

        return DataProductPublicStreamArtifact(
            filename=filename,
            content_type=content_type,
            content_length=content_length,
            iterator=iter_chunks(),
        )

    def public_path(self, slug: str) -> str:
        return f"{PUBLIC_DATA_PRODUCTS_PATH_PREFIX}/{slug}"

    def _slug_exists(self, slug: str) -> bool:
        normalized_slug = str(slug or "").strip().lower()
        return any(
            product.slug.lower() == normalized_slug
            for product in self._store.list_products()
        )

    def _normalized_title(
        self,
        title: str,
        descriptor: DataProductSourceDescriptor,
    ) -> str:
        normalized_title = str(title or "").strip()
        if normalized_title:
            return normalized_title
        fallback = (
            descriptor.source_display_name
            or descriptor.key
            or descriptor.bucket
            or descriptor.relation
        )
        normalized_fallback = str(fallback or "").strip()
        if normalized_fallback:
            return normalized_fallback
        raise ValueError("Provide a title for the data product.")

    def _resolve_source_descriptor(
        self,
        source: dict[str, object],
        *,
        allow_unsupported: bool,
    ) -> DataProductSourceDescriptor:
        if not isinstance(source, dict):
            raise ValueError("A source descriptor is required.")

        raw_source_kind = str(
            source.get("sourceKind") or source.get("source_kind") or ""
        ).strip()
        raw_source_id = str(
            source.get("sourceId") or source.get("source_id") or ""
        ).strip()
        raw_relation = str(source.get("relation") or "").strip()
        raw_bucket = str(source.get("bucket") or "").strip()
        raw_key = str(source.get("key") or "").strip()
        raw_display_name = str(
            source.get("sourceDisplayName")
            or source.get("source_display_name")
            or ""
        ).strip()
        raw_platform = str(
            source.get("sourcePlatform") or source.get("source_platform") or ""
        ).strip()

        if raw_source_id == "workspace.local" or raw_source_kind == "local-object":
            descriptor = DataProductSourceDescriptor(
                source_kind="local-object",
                source_id="workspace.local",
                relation=raw_relation,
                source_display_name=raw_display_name or raw_relation or "Local Workspace file",
                source_platform=raw_platform or "indexeddb",
                unsupported_reason=(
                    "Live publication requires a server-visible source; move this file to Shared Workspace first."
                ),
            )
            if allow_unsupported:
                return descriptor
            raise ValueError(descriptor.unsupported_reason)

        if raw_source_kind == "bucket":
            if raw_source_id != "workspace.s3":
                raise ValueError("Bucket publications are only supported for Shared Workspace.")
            if not raw_bucket:
                raise ValueError("Choose a bucket before publishing it.")
            if raw_bucket not in set(list_s3_buckets(self._settings)):
                raise ValueError(f"The S3 bucket '{raw_bucket}' is not available.")
            return DataProductSourceDescriptor(
                source_kind="bucket",
                source_id="workspace.s3",
                bucket=raw_bucket,
                source_display_name=raw_display_name or raw_bucket,
                source_platform=raw_platform or "s3",
            )

        if raw_source_kind == "object":
            if raw_source_id != "workspace.s3":
                raise ValueError("Object publications are only supported for Shared Workspace.")
            if not raw_bucket or not raw_key:
                raise ValueError("Choose a concrete Shared Workspace object before publishing it.")
            client = s3_client(self._settings)
            try:
                client.head_object(Bucket=raw_bucket, Key=raw_key)
            except Exception as exc:
                raise ValueError(
                    f"The S3 object s3://{raw_bucket}/{raw_key} is not available: {exc}"
                ) from exc
            return DataProductSourceDescriptor(
                source_kind="object",
                source_id="workspace.s3",
                bucket=raw_bucket,
                key=raw_key,
                source_display_name=raw_display_name or PurePosixPath(raw_key).name,
                source_platform=raw_platform or "s3",
            )

        if raw_source_kind == "relation":
            if not raw_relation:
                raise ValueError("Choose a relation before publishing it.")
            try:
                self._relation_fields_provider(raw_relation)
            except Exception as exc:
                raise ValueError(
                    f"The relation '{raw_relation}' is not available for publication: {exc}"
                ) from exc
            return DataProductSourceDescriptor(
                source_kind="relation",
                source_id=raw_source_id or self._source_id_for_relation(raw_relation),
                relation=raw_relation,
                source_display_name=raw_display_name or raw_relation.split(".")[-1],
                source_platform=raw_platform or self._platform_for_source_id(raw_source_id, raw_relation),
            )

        raise ValueError("Unsupported data product source.")

    def _platform_for_source_id(self, source_id: str, relation: str) -> str:
        normalized_source_id = str(source_id or "").strip()
        if normalized_source_id in {"pg_oltp", "pg_olap"}:
            return "postgres"
        if normalized_source_id == "workspace.s3":
            return "s3"
        if str(relation or "").startswith(("pg_oltp.", "pg_olap.")):
            return "postgres"
        return "duckdb"

    def _source_id_for_relation(self, relation: str) -> str:
        normalized_relation = str(relation or "").strip()
        if normalized_relation.startswith("pg_oltp."):
            return "pg_oltp"
        if normalized_relation.startswith("pg_olap."):
            return "pg_olap"
        return "workspace.s3"

    def _response_kind_for_source(self, source: DataProductSourceDescriptor) -> str:
        if source.source_kind == "relation":
            return "relation"
        if source.source_kind == "bucket":
            return "bucket"
        if source.source_kind == "object":
            return "object"
        return "unsupported"

    def _source_summary(self, source: DataProductSourceDescriptor) -> str:
        if source.source_kind == "relation":
            return f"Live relation rows from {source.relation}."
        if source.source_kind == "bucket":
            return f"Live Shared Workspace bucket listing for s3://{source.bucket}/."
        if source.source_kind == "object":
            return f"Raw Shared Workspace object stream for s3://{source.bucket}/{source.key}."
        return source.unsupported_reason or "Unsupported source."

    def _sample_response(
        self,
        product: DataProductDefinition,
        response_kind: str,
        *,
        base_url: str | None = None,
    ) -> dict[str, object]:
        if response_kind == "relation":
            fields = self._relation_source_fields(product.source)
            item = {
                str(getattr(field, "name", "")).strip(): self._example_value_for_type(
                    str(getattr(field, "data_type", ""))
                )
                for field in fields
                if str(getattr(field, "name", "")).strip()
            }
            return {
                "product": product.payload(base_url=base_url),
                "columns": [
                    {
                        "name": str(getattr(field, "name", "")).strip(),
                        "dataType": str(getattr(field, "data_type", "")).strip() or "VARCHAR",
                    }
                    for field in fields
                    if str(getattr(field, "name", "")).strip()
                ]
                or [{"name": "example_column", "dataType": "VARCHAR"}],
                "items": [item] if item else [{"example_column": "value"}],
                "limit": DEFAULT_PUBLIC_DATA_PRODUCT_LIMIT,
                "offset": 0,
                "hasMore": False,
            }
        if response_kind == "bucket":
            return {
                "product": product.payload(base_url=base_url),
                "prefix": "",
                "entries": [
                    {
                        "entryKind": "file",
                        "name": "example.csv",
                        "bucket": product.source.bucket,
                        "prefix": "example.csv",
                        "path": f"s3://{product.source.bucket}/example.csv",
                        "fileFormat": "csv",
                        "sizeBytes": 1024,
                        "hasChildren": False,
                        "selectable": False,
                    }
                ],
            }
        return {
            "contentType": mimetypes.guess_type(product.source.key)[0]
            or "application/octet-stream",
            "note": "The public endpoint streams the raw object content.",
            "filename": PurePosixPath(product.source.key).name if product.source.key else product.slug,
        }

    def _documentation_contract(
        self,
        product: DataProductDefinition,
        *,
        base_url: str | None = None,
    ) -> dict[str, object]:
        response_kind = self._response_kind_for_source(product.source)
        return {
            "product": product.payload(base_url=base_url),
            "sourceSummary": self._source_summary(product.source),
            "liveReadOnlyCopy": "Published data products are live and read-only in v1.",
            "responseKind": response_kind,
            "requestParameters": self._request_parameters(product.source),
            "responseSchema": self._response_schema(
                product,
                response_kind,
                base_url=base_url,
            ),
            "sampleResponse": self._sample_response(
                product,
                response_kind,
                base_url=base_url,
            ),
            "openApiDocument": self._openapi_document(
                product,
                response_kind,
                base_url=base_url,
            ),
        }

    def _request_parameters(
        self,
        source: DataProductSourceDescriptor,
    ) -> list[dict[str, object]]:
        if source.source_kind == "relation":
            return [
                {
                    "name": "limit",
                    "type": "integer",
                    "required": False,
                    "default": DEFAULT_PUBLIC_DATA_PRODUCT_LIMIT,
                    "description": (
                        "Maximum number of rows to return. "
                        f"Allowed range: 1-{MAX_PUBLIC_DATA_PRODUCT_LIMIT}."
                    ),
                },
                {
                    "name": "offset",
                    "type": "integer",
                    "required": False,
                    "default": 0,
                    "description": "Row offset for pagination.",
                },
            ]
        if source.source_kind == "bucket":
            return [
                {
                    "name": "prefix",
                    "type": "string",
                    "required": False,
                    "default": "",
                    "description": "Optional S3 prefix used to filter the bucket listing.",
                }
            ]
        return []

    def _relation_source_fields(
        self,
        source: DataProductSourceDescriptor,
    ) -> list[object]:
        if source.source_kind != "relation" or not source.relation:
            return []
        try:
            fields = self._relation_fields_provider(source.relation)
        except Exception:
            return []
        return [
            field
            for field in list(fields or [])
            if str(getattr(field, "name", "")).strip()
        ]

    def _product_metadata_schema(self) -> dict[str, object]:
        return {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "productId",
                "slug",
                "title",
                "description",
                "sourceKind",
                "sourceId",
                "publicPath",
                "publishedUrl",
                "documentationPath",
                "documentationUrl",
                "publicationMode",
                "owner",
                "domain",
                "tags",
                "accessLevel",
                "accessNote",
                "requestAccessContact",
                "customProperties",
                "createdAt",
                "updatedAt",
            ],
            "properties": {
                "productId": {"type": "string"},
                "slug": {"type": "string"},
                "title": {"type": "string"},
                "description": {"type": "string"},
                "sourceKind": {"type": "string"},
                "sourceId": {"type": "string"},
                "relation": {"type": "string"},
                "bucket": {"type": "string"},
                "key": {"type": "string"},
                "sourceDisplayName": {"type": "string"},
                "sourcePlatform": {"type": "string"},
                "unsupportedReason": {"type": "string"},
                "publicPath": {"type": "string"},
                "publishedUrl": {"type": "string"},
                "documentationPath": {"type": "string"},
                "documentationUrl": {"type": "string"},
                "publicationMode": {"type": "string"},
                "owner": {"type": "string"},
                "domain": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "accessLevel": {"type": "string"},
                "accessNote": {"type": "string"},
                "requestAccessContact": {"type": "string"},
                "customProperties": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
                "createdAt": {"type": "string", "format": "date-time"},
                "updatedAt": {"type": "string", "format": "date-time"},
            },
        }

    def _column_schema(self) -> dict[str, object]:
        return {
            "type": "object",
            "additionalProperties": False,
            "required": ["name", "dataType"],
            "properties": {
                "name": {"type": "string"},
                "dataType": {"type": "string"},
            },
        }

    def _bucket_entry_schema(self) -> dict[str, object]:
        return {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "entryKind",
                "name",
                "bucket",
                "prefix",
                "path",
                "fileFormat",
                "sizeBytes",
                "hasChildren",
                "selectable",
            ],
            "properties": {
                "entryKind": {"type": "string"},
                "name": {"type": "string"},
                "bucket": {"type": "string"},
                "prefix": {"type": "string"},
                "path": {"type": "string"},
                "fileFormat": {"type": "string"},
                "sizeBytes": {"type": "integer"},
                "hasChildren": {"type": "boolean"},
                "selectable": {"type": "boolean"},
            },
        }

    def _field_schema_for_type(self, data_type: str) -> dict[str, object]:
        normalized_type = str(data_type or "").strip().upper()

        if any(token in normalized_type for token in ("BOOL",)):
            return {"type": "boolean"}
        if any(token in normalized_type for token in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "SERIAL")):
            return {"type": "integer"}
        if any(token in normalized_type for token in ("DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT")):
            return {"type": "number"}
        if "TIMESTAMP" in normalized_type or "DATETIME" in normalized_type:
            return {"type": "string", "format": "date-time"}
        if normalized_type == "DATE":
            return {"type": "string", "format": "date"}
        if normalized_type == "TIME" or normalized_type.startswith("TIME "):
            return {"type": "string", "format": "time"}
        if "JSON" in normalized_type:
            return {
                "type": ["object", "array", "string", "number", "boolean", "null"],
            }
        if any(token in normalized_type for token in ("BYTEA", "BLOB", "BINARY", "VARBINARY")):
            return {"type": "string", "format": "binary"}
        return {"type": "string"}

    def _example_value_for_type(self, data_type: str) -> object:
        normalized_type = str(data_type or "").strip().upper()

        if any(token in normalized_type for token in ("BOOL",)):
            return True
        if any(token in normalized_type for token in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "SERIAL")):
            return 1
        if any(token in normalized_type for token in ("DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT")):
            return 12.5
        if "TIMESTAMP" in normalized_type or "DATETIME" in normalized_type:
            return "2026-01-01T00:00:00Z"
        if normalized_type == "DATE":
            return "2026-01-01"
        if normalized_type == "TIME" or normalized_type.startswith("TIME "):
            return "12:00:00"
        if "JSON" in normalized_type:
            return {"example": True}
        if any(token in normalized_type for token in ("BYTEA", "BLOB", "BINARY", "VARBINARY")):
            return "base64-encoded-content"
        return "value"

    def _json_media_type_for_source(self, source: DataProductSourceDescriptor) -> str:
        if source.source_kind in {"relation", "bucket"}:
            return "application/json"
        return mimetypes.guess_type(source.key)[0] or "application/octet-stream"

    def _response_schema(
        self,
        product: DataProductDefinition,
        response_kind: str,
        *,
        base_url: str | None = None,
    ) -> dict[str, object]:
        if response_kind == "relation":
            fields = self._relation_source_fields(product.source)
            item_properties = {
                str(getattr(field, "name", "")).strip(): self._field_schema_for_type(
                    str(getattr(field, "data_type", ""))
                )
                for field in fields
                if str(getattr(field, "name", "")).strip()
            }
            item_schema: dict[str, object] = {
                "type": "object",
                "additionalProperties": False,
                "properties": item_properties,
            }
            if item_properties:
                item_schema["required"] = list(item_properties.keys())
            return {
                "type": "object",
                "additionalProperties": False,
                "required": ["product", "columns", "items", "limit", "offset", "hasMore"],
                "properties": {
                    "product": self._product_metadata_schema(),
                    "columns": {
                        "type": "array",
                        "items": self._column_schema(),
                    },
                    "items": {
                        "type": "array",
                        "items": item_schema,
                    },
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                    "hasMore": {"type": "boolean"},
                },
            }
        if response_kind == "bucket":
            return {
                "type": "object",
                "additionalProperties": False,
                "required": ["product", "prefix", "entries"],
                "properties": {
                    "product": self._product_metadata_schema(),
                    "prefix": {"type": "string"},
                    "entries": {
                        "type": "array",
                        "items": self._bucket_entry_schema(),
                    },
                },
            }
        return {
            "type": "string",
            "format": "binary",
            "contentMediaType": self._json_media_type_for_source(product.source),
            "description": "The endpoint streams the raw published object body.",
        }

    def _openapi_document(
        self,
        product: DataProductDefinition,
        response_kind: str,
        *,
        base_url: str | None = None,
    ) -> dict[str, object]:
        response_schema = self._response_schema(
            product,
            response_kind,
            base_url=base_url,
        )
        media_type = self._json_media_type_for_source(product.source)
        success_response: dict[str, object] = {
            "description": self._source_summary(product.source),
            "content": {
                media_type: {
                    "schema": response_schema,
                }
            },
        }
        if response_kind in {"relation", "bucket"}:
            success_response["content"][media_type]["example"] = self._sample_response(
                product,
                response_kind,
                base_url=base_url,
            )

        openapi_document: dict[str, object] = {
            "openapi": "3.1.0",
            "info": {
                "title": product.title,
                "version": "1.0.0",
                "description": product.description or self._source_summary(product.source),
            },
            "paths": {
                product.public_path: {
                    "get": {
                        "summary": f"Read {product.title}",
                        "operationId": f"read_data_product_{product.slug.replace('-', '_')}",
                        "parameters": [
                            {
                                "name": parameter["name"],
                                "in": "query",
                                "required": bool(parameter.get("required")),
                                "description": parameter.get("description", ""),
                                "schema": {
                                    "type": parameter.get("type", "string"),
                                    **(
                                        {"default": parameter["default"]}
                                        if "default" in parameter
                                        else {}
                                    ),
                                },
                            }
                            for parameter in self._request_parameters(product.source)
                        ],
                        "responses": {
                            "200": success_response,
                        },
                    }
                }
            },
        }
        if base_url:
            openapi_document["servers"] = [{"url": base_url}]
        return openapi_document

    def _normalized_limit(self, limit: int) -> int:
        normalized_limit = int(limit or DEFAULT_PUBLIC_DATA_PRODUCT_LIMIT)
        if normalized_limit <= 0:
            raise ValueError("Limit must be greater than zero.")
        if normalized_limit > MAX_PUBLIC_DATA_PRODUCT_LIMIT:
            raise ValueError(
                f"Limit must be less than or equal to {MAX_PUBLIC_DATA_PRODUCT_LIMIT}."
            )
        return normalized_limit

    def _normalized_offset(self, offset: int) -> int:
        normalized_offset = int(offset or 0)
        if normalized_offset < 0:
            raise ValueError("Offset must be zero or greater.")
        return normalized_offset

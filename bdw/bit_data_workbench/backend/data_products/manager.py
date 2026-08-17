from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import mimetypes
from pathlib import PurePosixPath
import re
from threading import RLock
import uuid

import duckdb

from ...config import Settings
from ...models import DataProductDefinition, DataProductSourceDescriptor, SourceCatalog
from ..data_sources.s3.explorer import normalize_s3_prefix
from ..s3_storage import list_s3_buckets, s3_client
from ..sql_utils import qualified_name
from .registry import DataProductOverwriteConflict, DataProductStore


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


@dataclass(slots=True)
class _DataProductPublicationReservation:
    product: DataProductDefinition
    operation: str
    expected_product_id: str = ""
    expected_updated_at: str = ""


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
        s3_relation_source_resolver: Callable[
            [dict[str, object]], dict[str, object]
        ]
        | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._create_worker_connection = create_worker_connection
        self._relation_fields_provider = relation_fields_provider
        self._catalog_provider = catalog_provider
        self._s3_bucket_snapshot_provider = s3_bucket_snapshot_provider
        self._s3_relation_source_resolver = s3_relation_source_resolver
        self._publication_lock = RLock()
        self._publication_reservations: dict[
            str, _DataProductPublicationReservation
        ] = {}

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

    def managed_product_definitions(self) -> list[DataProductDefinition]:
        return [
            product
            for product in self._store.list_products()
            if product.daca_publication is not None
        ]

    def reconcile_daca_publication(
        self,
        *,
        product_id: str,
        state: str,
        missing_fields: list[str],
    ) -> DataProductDefinition:
        with self._publication_lock:
            existing = self.product_by_id(product_id)
            if existing.product_id in self._publication_reservations:
                return existing
            reference = existing.daca_publication
            if reference is None:
                return existing
            if reference.state == state and reference.missing_fields == missing_fields:
                return existing
            updated = replace(
                existing,
                daca_publication=replace(
                    reference,
                    state=state,
                    missing_fields=list(missing_fields),
                    synced_at=utc_now_iso(),
                ),
                updated_at=utc_now_iso(),
            )
            self._store.update_product(updated)
            return updated

    def published_products_for_source(
        self,
        *,
        source: dict[str, object],
        base_url: str | None = None,
    ) -> list[dict[str, object]]:
        normalized_source = self._normalized_match_source(source)
        if not normalized_source:
            return []

        products = sorted(
            (
                product
                for product in self._store.list_products()
                if self._product_matches_source(product, normalized_source)
            ),
            key=lambda item: (item.updated_at, item.title.lower(), item.product_id),
            reverse=True,
        )
        return [
            self._publication_link_payload(product, base_url=base_url)
            for product in products
        ]

    def source_options(self) -> list[dict[str, object]]:
        options: list[dict[str, object]] = []
        for catalog in self._catalog_provider():
            source_id = str(catalog.connection_source_id or catalog.name).strip()
            if source_id == "workspace.local":
                continue

            if source_id == "s3":
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
                                    source_id="s3",
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
                                        source_id="s3",
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
                                        source_id="s3",
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
        existing_product = self._existing_product_hint(title=title, slug=slug)
        descriptor = self._reusable_source_descriptor(existing_product, source)
        if descriptor is None:
            descriptor = self._resolve_source_descriptor(source, allow_unsupported=True)
        normalized_title = self._normalized_title(title, descriptor)
        normalized_slug = normalize_slug(slug or normalized_title)
        blocked_reason = descriptor.unsupported_reason
        existing_product = self._existing_product_for_slug(normalized_slug)
        if not blocked_reason and existing_product is not None:
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
            "operation": "replace" if existing_product is not None else "create",
            "overwriteRequired": existing_product is not None,
            "canOverwrite": bool(existing_product is not None and not descriptor.unsupported_reason),
            "existingProduct": (
                existing_product.payload(base_url=base_url)
                if existing_product is not None
                else None
            ),
            **self._documentation_contract(
                product,
                base_url=base_url,
                resolve_relation_fields=existing_product is None,
            ),
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
        overwrite_existing: bool = False,
        expected_product_id: str = "",
        expected_updated_at: str = "",
        base_url: str | None = None,
    ) -> dict[str, object]:
        product = self.reserve_product(
            source=source,
            title=title,
            slug=slug,
            description=description,
            owner=owner,
            domain=domain,
            tags=tags,
            access_level=access_level,
            access_note=access_note,
            request_access_contact=request_access_contact,
            custom_properties=custom_properties,
            overwrite_existing=overwrite_existing,
            expected_product_id=expected_product_id,
            expected_updated_at=expected_updated_at,
        )
        operation = self.reservation_operation(product.product_id)
        try:
            created = self.activate_reserved_product(product)
        except Exception:
            self.cancel_reserved_product(product.product_id)
            raise
        return {
            "action": "replaced" if operation == "replace" else "created",
            "product": created.payload(base_url=base_url),
        }

    def reserve_product(
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
        overwrite_existing: bool = False,
        expected_product_id: str = "",
        expected_updated_at: str = "",
        allow_managed_overwrite: bool = False,
    ) -> DataProductDefinition:
        existing_hint = self._existing_product_hint(title=title, slug=slug)
        if existing_hint is not None and not overwrite_existing:
            raise ValueError(
                f"The data product slug '{existing_hint.slug}' is already in use."
            )
        descriptor = self._reusable_source_descriptor(existing_hint, source)
        if descriptor is None:
            descriptor = self._resolve_source_descriptor(source, allow_unsupported=False)
        normalized_title = self._normalized_title(title, descriptor)
        normalized_slug = normalize_slug(slug or normalized_title)
        normalized_expected_product_id = str(expected_product_id or "").strip()
        normalized_expected_updated_at = str(expected_updated_at or "").strip()
        with self._publication_lock:
            existing = self._existing_product_for_slug(normalized_slug)
            reservation_conflict = any(
                item.product.slug.lower() == normalized_slug.lower()
                for item in self._publication_reservations.values()
            )
            if reservation_conflict:
                raise DataProductOverwriteConflict(
                    f"The data product slug '{normalized_slug}' is currently being published."
                )

            if existing is not None and not overwrite_existing:
                raise ValueError(
                    f"The data product slug '{normalized_slug}' is already in use."
                )
            if existing is None and overwrite_existing:
                raise DataProductOverwriteConflict(
                    f"The data product slug '{normalized_slug}' no longer exists. Review the preview and try again."
                )
            if overwrite_existing and (
                not normalized_expected_product_id or not normalized_expected_updated_at
            ):
                raise DataProductOverwriteConflict(
                    "Replacing a data product requires its preview identity and update timestamp."
                )
            if not overwrite_existing and (
                normalized_expected_product_id or normalized_expected_updated_at
            ):
                raise DataProductOverwriteConflict(
                    "Overwrite preview tokens require overwriteExisting=true."
                )

            if existing is not None:
                if existing.product_id != normalized_expected_product_id:
                    raise DataProductOverwriteConflict(
                        "The data product identity changed after the overwrite preview."
                    )
                if existing.updated_at != normalized_expected_updated_at:
                    raise DataProductOverwriteConflict(
                        "The data product changed after the overwrite preview. Review it and confirm again."
                    )
                if existing.daca_publication is not None and not allow_managed_overwrite:
                    raise DataProductOverwriteConflict(
                        "DaCa-managed data products must be replaced with Publish to DaCa enabled."
                    )

            now = utc_now_iso()
            product = DataProductDefinition(
                product_id=(
                    existing.product_id
                    if existing is not None
                    else f"data-product-{uuid.uuid4().hex}"
                ),
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
                daca_publication=(
                    existing.daca_publication if existing is not None else None
                ),
                created_at=existing.created_at if existing is not None else now,
                updated_at=now,
            )
            self._publication_reservations[product.product_id] = (
                _DataProductPublicationReservation(
                    product=product,
                    operation="replace" if existing is not None else "create",
                    expected_product_id=normalized_expected_product_id,
                    expected_updated_at=normalized_expected_updated_at,
                )
            )
            return product

    def activate_reserved_product(
        self,
        product: DataProductDefinition,
    ) -> DataProductDefinition:
        with self._publication_lock:
            reserved = self._publication_reservations.get(product.product_id)
            if reserved is None or reserved.product.slug != product.slug:
                raise ValueError("The data product publication reservation is no longer active.")
            if reserved.operation == "replace":
                created = self._store.replace_product(
                    product,
                    expected_product_id=reserved.expected_product_id,
                    expected_updated_at=reserved.expected_updated_at,
                )
            else:
                created = self._store.create_product(product)
            self._publication_reservations.pop(product.product_id, None)
            return created

    def reservation_operation(self, product_id: str) -> str:
        with self._publication_lock:
            reservation = self._publication_reservations.get(
                str(product_id or "").strip()
            )
            if reservation is None:
                raise ValueError("The data product publication reservation is no longer active.")
            return reservation.operation

    def cancel_reserved_product(self, product_id: str) -> None:
        with self._publication_lock:
            self._publication_reservations.pop(str(product_id or "").strip(), None)

    def publication_schema_fields(
        self,
        product: DataProductDefinition,
    ) -> list[dict[str, object]]:
        fields = self._relation_source_fields(product.source)
        if not fields:
            raise ValueError(
                "Publishing to DaCa requires a relation source with at least one schema field."
            )
        key_names = {"canton_code", "tax_year"}
        return [
            {
                "name": str(getattr(field, "name", "")).strip(),
                "dataType": str(getattr(field, "data_type", "")).strip()
                or "VARCHAR",
                "nullable": bool(getattr(field, "nullable", False)),
                "keyField": str(getattr(field, "name", "")).strip().lower()
                in key_names,
            }
            for field in fields[:100]
            if str(getattr(field, "name", "")).strip()
        ]

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
        with self._publication_lock:
            existing = self.product_by_id(product_id)
            if existing.product_id in self._publication_reservations:
                raise DataProductOverwriteConflict(
                    "The data product is currently being replaced."
                )
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
                daca_publication=existing.daca_publication,
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
        with self._publication_lock:
            if str(product_id or "").strip() in self._publication_reservations:
                raise DataProductOverwriteConflict(
                    "The data product is currently being replaced."
                )
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
        return self._existing_product_for_slug(slug) is not None

    def _existing_product_for_slug(
        self, slug: str
    ) -> DataProductDefinition | None:
        normalized_slug = str(slug or "").strip().lower()
        return next(
            (
                product
                for product in self._store.list_products()
                if product.slug.lower() == normalized_slug
            ),
            None,
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

    def _publication_link_payload(
        self,
        product: DataProductDefinition,
        *,
        base_url: str | None = None,
    ) -> dict[str, object]:
        return {
            "productId": product.product_id,
            "slug": product.slug,
            "title": product.title,
            "documentationPath": product.documentation_path(),
            "documentationUrl": product.documentation_url(base_url),
            "publicPath": product.public_path,
            "publishedUrl": product.published_url(base_url),
        }

    def _normalized_match_source(
        self,
        source: dict[str, object] | None,
    ) -> dict[str, str] | None:
        if not isinstance(source, dict):
            return None

        source_kind = str(
            source.get("sourceKind") or source.get("source_kind") or ""
        ).strip()
        source_id = str(
            source.get("sourceId") or source.get("source_id") or ""
        ).strip()
        relation = str(source.get("relation") or "").strip()
        bucket = str(source.get("bucket") or "").strip()
        key = str(source.get("key") or "").strip()

        if not source_kind:
            if bucket and key:
                source_kind = "object"
            elif bucket and source_id == "s3":
                source_kind = "bucket"
            elif relation:
                source_kind = "relation"

        if source_kind == "local-object" or source_id == "workspace.local":
            return None
        if source_kind == "relation" and relation:
            return {
                "sourceKind": "relation",
                "sourceId": source_id,
                "relation": relation,
                "bucket": "",
                "key": "",
            }
        if source_kind == "bucket" and bucket:
            return {
                "sourceKind": "bucket",
                "sourceId": source_id,
                "relation": "",
                "bucket": bucket,
                "key": "",
            }
        if source_kind == "object" and bucket and key:
            return {
                "sourceKind": "object",
                "sourceId": source_id,
                "relation": "",
                "bucket": bucket,
                "key": key,
            }
        return None

    def _existing_product_hint(
        self,
        *,
        title: str,
        slug: str,
    ) -> DataProductDefinition | None:
        raw_slug = str(slug or title or "").strip()
        if not raw_slug:
            return None
        try:
            normalized_slug = normalize_slug(raw_slug)
        except ValueError:
            return None
        return self._existing_product_for_slug(normalized_slug)

    def _reusable_source_descriptor(
        self,
        existing_product: DataProductDefinition | None,
        source: dict[str, object],
    ) -> DataProductSourceDescriptor | None:
        if existing_product is None:
            return None
        normalized_source = self._normalized_match_source(source)
        if normalized_source is None or not self._product_matches_source(
            existing_product,
            normalized_source,
        ):
            return None

        display_name = str(
            source.get("sourceDisplayName")
            or source.get("source_display_name")
            or existing_product.source.source_display_name
        ).strip()
        platform = str(
            source.get("sourcePlatform")
            or source.get("source_platform")
            or existing_product.source.source_platform
        ).strip()
        return replace(
            existing_product.source,
            source_display_name=display_name,
            source_platform=platform,
            unsupported_reason="",
        )

    def _product_matches_source(
        self,
        product: DataProductDefinition,
        source: dict[str, str],
    ) -> bool:
        source_kind = str(source.get("sourceKind") or "").strip()
        if source_kind == "relation":
            return (
                product.source.source_kind == "relation"
                and str(product.source.relation).strip()
                == str(source.get("relation") or "").strip()
            )
        if source_kind == "bucket":
            return (
                product.source.source_kind == "bucket"
                and str(product.source.bucket).strip()
                == str(source.get("bucket") or "").strip()
            )
        if source_kind == "object":
            return (
                product.source.source_kind == "object"
                and str(product.source.bucket).strip()
                == str(source.get("bucket") or "").strip()
                and str(product.source.key).strip()
                == str(source.get("key") or "").strip()
            )
        return False

    def _resolve_source_descriptor(
        self,
        source: dict[str, object],
        *,
        allow_unsupported: bool,
    ) -> DataProductSourceDescriptor:
        if not isinstance(source, dict):
            raise ValueError("A source descriptor is required.")

        if self._s3_relation_source_resolver is not None:
            source = self._s3_relation_source_resolver(source)

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
            if raw_source_id != "s3":
                raise ValueError("Bucket publications are only supported for Shared Workspace.")
            if not raw_bucket:
                raise ValueError("Choose a bucket before publishing it.")
            if raw_bucket not in set(list_s3_buckets(self._settings)):
                raise ValueError(f"The S3 bucket '{raw_bucket}' is not available.")
            return DataProductSourceDescriptor(
                source_kind="bucket",
                source_id="s3",
                bucket=raw_bucket,
                source_display_name=raw_display_name or raw_bucket,
                source_platform=raw_platform or "s3",
            )

        if raw_source_kind == "object":
            if raw_source_id != "s3":
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
                source_id="s3",
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
                bucket=raw_bucket,
                key=raw_key,
                source_display_name=raw_display_name or raw_relation.split(".")[-1],
                source_platform=raw_platform or self._platform_for_source_id(raw_source_id, raw_relation),
            )

        raise ValueError("Unsupported data product source.")

    def _platform_for_source_id(self, source_id: str, relation: str) -> str:
        normalized_source_id = str(source_id or "").strip()
        if normalized_source_id in {"pg_oltp", "pg_olap"}:
            return "postgres"
        if normalized_source_id == "s3":
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
        return "s3"

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
        relation_fields: list[object] | None = None,
    ) -> dict[str, object]:
        if response_kind == "relation":
            fields = (
                relation_fields
                if relation_fields is not None
                else self._relation_source_fields(product.source)
            )
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
        resolve_relation_fields: bool = True,
    ) -> dict[str, object]:
        response_kind = self._response_kind_for_source(product.source)
        relation_fields: list[object] | None = None
        if response_kind == "relation":
            relation_fields = (
                self._relation_source_fields(product.source)
                if resolve_relation_fields
                else []
            )
        return {
            "product": product.payload(base_url=base_url),
            "sourceSummary": self._source_summary(product.source),
            "liveReadOnlyCopy": "Published data products are live and read-only in v1.",
            "responseKind": response_kind,
            "requestParameters": [
                *self._request_parameters(product.source),
                *self._authorization_parameters(product),
            ],
            "responseSchema": self._response_schema(
                product,
                response_kind,
                base_url=base_url,
                relation_fields=relation_fields,
            ),
            "sampleResponse": self._sample_response(
                product,
                response_kind,
                base_url=base_url,
                relation_fields=relation_fields,
            ),
            "openApiDocument": self._openapi_document(
                product,
                response_kind,
                base_url=base_url,
                relation_fields=relation_fields,
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

    def _authorization_parameters(
        self,
        product: DataProductDefinition,
    ) -> list[dict[str, object]]:
        if product.daca_publication is None:
            return []
        return [
            {
                "name": "X-DaCa-User",
                "in": "header",
                "type": "string",
                "required": True,
                "description": (
                    "PoC identity evaluated by DaCa OPA. This demo header is not "
                    "a production authentication mechanism."
                ),
            }
        ]

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
                "dacaManaged",
                "dacaPublication",
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
                "dacaManaged": {"type": "boolean"},
                "dacaPublication": {
                    "anyOf": [
                        {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "sourceProductId",
                                "publicationId",
                                "productId",
                                "state",
                                "created",
                                "taskIds",
                                "missingFields",
                                "catalogUrl",
                                "syncedAt",
                            ],
                            "properties": {
                                "sourceProductId": {"type": "string"},
                                "publicationId": {"type": "string", "format": "uuid"},
                                "productId": {"type": "string", "format": "uuid"},
                                "state": {"type": "string"},
                                "created": {"type": "boolean"},
                                "taskIds": {
                                    "type": "array",
                                    "items": {"type": "string", "format": "uuid"},
                                },
                                "missingFields": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "catalogUrl": {"type": "string", "format": "uri"},
                                "syncedAt": {"type": "string", "format": "date-time"},
                            },
                        },
                        {"type": "null"},
                    ]
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
        relation_fields: list[object] | None = None,
    ) -> dict[str, object]:
        if response_kind == "relation":
            fields = (
                relation_fields
                if relation_fields is not None
                else self._relation_source_fields(product.source)
            )
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
        relation_fields: list[object] | None = None,
    ) -> dict[str, object]:
        response_schema = self._response_schema(
            product,
            response_kind,
            base_url=base_url,
            relation_fields=relation_fields,
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
                relation_fields=relation_fields,
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
                                "in": parameter.get("in", "query"),
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
                            for parameter in [
                                *self._request_parameters(product.source),
                                *self._authorization_parameters(product),
                            ]
                        ],
                        "responses": {
                            "200": success_response,
                            **(
                                {
                                    "401": {
                                        "description": "The X-DaCa-User header is missing."
                                    },
                                    "403": {
                                        "description": "DaCa policy denied data.read."
                                    },
                                    "503": {
                                        "description": "DaCa OPA returned no valid decision."
                                    },
                                }
                                if product.daca_publication is not None
                                else {}
                            ),
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

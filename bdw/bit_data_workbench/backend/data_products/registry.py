from __future__ import annotations

import json
from pathlib import Path

from ...models import DataProductDefinition, DataProductSourceDescriptor


def deserialize_source(payload: object) -> DataProductSourceDescriptor | None:
    if not isinstance(payload, dict):
        return None

    source_kind = str(
        payload.get("sourceKind") or payload.get("source_kind") or ""
    ).strip()
    source_id = str(
        payload.get("sourceId") or payload.get("source_id") or ""
    ).strip()
    if not source_kind or not source_id:
        return None

    return DataProductSourceDescriptor(
        source_kind=source_kind,
        source_id=source_id,
        relation=str(payload.get("relation") or "").strip(),
        bucket=str(payload.get("bucket") or "").strip(),
        key=str(payload.get("key") or "").strip(),
        source_display_name=str(
            payload.get("sourceDisplayName")
            or payload.get("source_display_name")
            or ""
        ).strip(),
        source_platform=str(
            payload.get("sourcePlatform") or payload.get("source_platform") or ""
        ).strip(),
        unsupported_reason=str(
            payload.get("unsupportedReason")
            or payload.get("unsupported_reason")
            or ""
        ).strip(),
    )


def serialize_product(product: DataProductDefinition) -> dict[str, object]:
    return {
        "productId": product.product_id,
        "slug": product.slug,
        "title": product.title,
        "description": product.description,
        "source": product.source.payload,
        "publicPath": product.public_path,
        "publicationMode": product.publication_mode,
        "owner": product.owner,
        "domain": product.domain,
        "tags": list(product.tags),
        "accessLevel": product.access_level,
        "accessNote": product.access_note,
        "requestAccessContact": product.request_access_contact,
        "customProperties": dict(product.custom_properties),
        "createdAt": product.created_at,
        "updatedAt": product.updated_at,
    }


def deserialize_product(payload: object) -> DataProductDefinition | None:
    if not isinstance(payload, dict):
        return None

    product_id = str(
        payload.get("productId") or payload.get("product_id") or ""
    ).strip()
    slug = str(payload.get("slug") or "").strip()
    title = str(payload.get("title") or "").strip()
    source = deserialize_source(payload.get("source"))
    if not product_id or not slug or not title or source is None:
        return None

    custom_properties = payload.get("customProperties") or payload.get(
        "custom_properties"
    ) or {}
    if not isinstance(custom_properties, dict):
        custom_properties = {}

    return DataProductDefinition(
        product_id=product_id,
        slug=slug,
        title=title,
        description=str(payload.get("description") or "").strip(),
        source=source,
        public_path=str(
            payload.get("publicPath") or payload.get("public_path") or ""
        ).strip(),
        publication_mode=str(
            payload.get("publicationMode")
            or payload.get("publication_mode")
            or "live"
        ).strip()
        or "live",
        owner=str(payload.get("owner") or "").strip(),
        domain=str(payload.get("domain") or "").strip(),
        tags=[
            str(tag).strip()
            for tag in payload.get("tags", []) or []
            if str(tag).strip()
        ],
        access_level=str(
            payload.get("accessLevel") or payload.get("access_level") or "internal"
        ).strip()
        or "internal",
        access_note=str(
            payload.get("accessNote") or payload.get("access_note") or ""
        ).strip(),
        request_access_contact=str(
            payload.get("requestAccessContact")
            or payload.get("request_access_contact")
            or ""
        ).strip(),
        custom_properties={
            str(name).strip(): str(value).strip()
            for name, value in custom_properties.items()
            if str(name).strip() and str(value).strip()
        },
        created_at=str(
            payload.get("createdAt") or payload.get("created_at") or ""
        ).strip(),
        updated_at=str(
            payload.get("updatedAt") or payload.get("updated_at") or ""
        ).strip(),
    )


class DataProductStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def list_products(self) -> list[DataProductDefinition]:
        state = self._read_state()
        products: list[DataProductDefinition] = []
        for payload in state.get("products", []):
            product = deserialize_product(payload)
            if product is not None:
                products.append(product)
        return products

    def create_product(self, product: DataProductDefinition) -> DataProductDefinition:
        state = self._read_state()
        products = list(state.get("products", []))
        existing_slug = next(
            (
                item
                for item in products
                if str(item.get("slug") or "").strip().lower() == product.slug.lower()
            ),
            None,
        )
        if existing_slug is not None:
            raise ValueError(
                f"The data product slug '{product.slug}' is already in use."
            )

        products.append(serialize_product(product))
        self._write_state({"products": products})
        return product

    def update_product(self, product: DataProductDefinition) -> DataProductDefinition:
        state = self._read_state()
        products = list(state.get("products", []))
        product_index = next(
            (
                index
                for index, item in enumerate(products)
                if str(
                    item.get("productId") or item.get("product_id") or ""
                ).strip()
                == product.product_id
            ),
            -1,
        )
        if product_index < 0:
            raise KeyError(f"Unknown data product: {product.product_id}")

        for index, item in enumerate(products):
            if index == product_index:
                continue
            if str(item.get("slug") or "").strip().lower() == product.slug.lower():
                raise ValueError(
                    f"The data product slug '{product.slug}' is already in use."
                )

        products[product_index] = serialize_product(product)
        self._write_state({"products": products})
        return product

    def delete_product(self, product_id: str) -> DataProductDefinition:
        state = self._read_state()
        products = list(state.get("products", []))
        remaining: list[dict[str, object]] = []
        removed_payload: dict[str, object] | None = None

        for payload in products:
            payload_id = str(
                payload.get("productId") or payload.get("product_id") or ""
            ).strip()
            if payload_id == product_id and removed_payload is None:
                removed_payload = payload
                continue
            remaining.append(payload)

        if removed_payload is None:
            raise KeyError(f"Unknown data product: {product_id}")

        self._write_state({"products": remaining})
        removed = deserialize_product(removed_payload)
        if removed is None:
            raise ValueError(
                f"Failed to deserialize removed data product {product_id}."
            )
        return removed

    def _read_state(self) -> dict[str, list[dict[str, object]]]:
        if not self._path.exists():
            return {"products": []}

        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"products": []}

        if not isinstance(raw, dict):
            return {"products": []}

        products = raw.get("products")
        if not isinstance(products, list):
            return {"products": []}
        return {"products": [item for item in products if isinstance(item, dict)]}

    def _write_state(self, state: dict[str, list[dict[str, object]]]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temp_path.replace(self._path)

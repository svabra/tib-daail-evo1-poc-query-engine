from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import unittest

from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.data_products import (  # noqa: E402
    DataProductCreatePayload,
    DataProductPreviewPayload,
    DataProductUpdatePayload,
    create_data_product,
    delete_data_product,
    list_data_products,
    preview_data_product,
    read_public_data_product,
    update_data_product,
)
from bit_data_workbench.models import (  # noqa: E402
    DataProductDefinition,
    DataProductSourceDescriptor,
)
from bit_data_workbench.main import app as main_app  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402
from bit_data_workbench.web.data_products import (  # noqa: E402
    data_products_page,
    public_data_product_page,
    public_data_products_page,
)
from bit_data_workbench.web.router import index  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


def build_request(path: str, *, partial: bool = False) -> Request:
    headers = [(b"host", b"testserver")]
    if partial:
        headers.append((b"hx-request", b"true"))
    return Request(
        {
            "type": "http",
            "scheme": "http",
            "method": "GET",
            "path": path,
            "headers": headers,
            "server": ("testserver", 80),
        }
    )


def build_product(
    *,
    product_id: str,
    slug: str,
    title: str,
    source: DataProductSourceDescriptor,
) -> DataProductDefinition:
    return DataProductDefinition(
        product_id=product_id,
        slug=slug,
        title=title,
        description=f"{title} description.",
        source=source,
        public_path=f"/api/public/data-products/{slug}",
        owner="Data Platform",
        domain="Finance",
        tags=["managed", "published"],
        access_level="internal",
        access_note="Internal network boundary only.",
        request_access_contact="data@example.test",
        custom_properties={"entityType": "dataset"},
        created_at="2026-04-20T08:00:00+00:00",
        updated_at="2026-04-20T08:00:00+00:00",
    )


class FakeWorkbenchService:
    def __init__(self) -> None:
        self.settings = type(
            "Settings",
            (),
            {
                "pg_host": None,
                "pg_port": None,
                "pg_user": None,
                "pg_password": None,
                "pg_oltp_database": None,
                "pg_olap_database": None,
                "s3_endpoint": None,
                "s3_bucket": None,
                "s3_url_style": "path",
                "s3_use_ssl": False,
                "s3_verify_ssl": False,
                "current_s3_access_key_id": lambda self: None,
                "current_s3_secret_access_key": lambda self: None,
                "effective_s3_ca_cert_file": lambda self: None,
            },
        )()
        self.products_by_id: dict[str, DataProductDefinition] = {}
        self.products_by_slug: dict[str, DataProductDefinition] = {}

        for product in (
            build_product(
                product_id="product-relation",
                slug="published-orders",
                title="Published Orders",
                source=DataProductSourceDescriptor(
                    source_kind="relation",
                    source_id="pg_oltp",
                    relation="pg_oltp.finance.orders",
                    source_display_name="finance.orders",
                    source_platform="postgres",
                ),
            ),
            build_product(
                product_id="product-bucket",
                slug="shared-workspace-bucket",
                title="Shared Workspace Bucket",
                source=DataProductSourceDescriptor(
                    source_kind="bucket",
                    source_id="workspace.s3",
                    bucket="shared-finance",
                    source_display_name="shared-finance",
                    source_platform="s3",
                ),
            ),
            build_product(
                product_id="product-object",
                slug="finance-object",
                title="Finance Object",
                source=DataProductSourceDescriptor(
                    source_kind="object",
                    source_id="workspace.s3",
                    bucket="shared-finance",
                    key="exports/orders.csv",
                    source_display_name="orders.csv",
                    source_platform="s3",
                ),
            ),
        ):
            self.products_by_id[product.product_id] = product
            self.products_by_slug[product.slug] = product

    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": CURRENT_VERSION,
            "hostname": "test-host",
            "pod_name": "bdw-pod",
            "pod_namespace": "bdw-namespace",
            "pod_ip": "127.0.0.1",
            "node_name": "bdw-node",
            "duckdb_database": "/tmp/workspace.duckdb",
            "timestamp_utc": "2026-04-20T00:00:00+00:00",
        }

    def catalogs(self):
        return []

    def notebooks(self):
        return []

    def notebook_tree(self):
        return []

    def source_options(self):
        return []

    def data_generators(self):
        return []

    def runbook_tree(self):
        return []

    def completion_schema(self):
        return {}

    def data_product_source_options(self) -> list[dict[str, object]]:
        return [
            {
                "optionId": "relation::pg_oltp.finance.orders",
                "label": "pg_oltp relation / finance.orders",
                "description": "pg_oltp.finance.orders",
                "source": {
                    "sourceKind": "relation",
                    "sourceId": "pg_oltp",
                    "relation": "pg_oltp.finance.orders",
                    "bucket": "",
                    "key": "",
                    "sourceDisplayName": "finance.orders",
                    "sourcePlatform": "postgres",
                },
            }
        ]

    def list_data_products(self, *, base_url: str | None = None) -> list[dict[str, object]]:
        return [
            product.payload(base_url=base_url)
            for product in self.products_by_id.values()
            if product.source.source_kind == "relation"
        ]

    def preview_data_product(self, **kwargs) -> dict[str, object]:
        slug = kwargs.get("slug") or "preview-product"
        title = kwargs.get("title") or "Preview Product"
        preview_product = build_product(
            product_id="preview",
            slug=slug,
            title=title,
            source=DataProductSourceDescriptor(
                source_kind="relation",
                source_id="pg_oltp",
                relation="pg_oltp.finance.orders",
                source_display_name="finance.orders",
                source_platform="postgres",
            ),
        )
        return {
            "compatible": True,
            "blocked": False,
            "blockedReason": "",
            "product": preview_product.payload(base_url=kwargs.get("base_url")),
            "sourceSummary": "Live relation rows from pg_oltp.finance.orders.",
            "liveReadOnlyCopy": "Published data products are live and read-only in v1.",
            "openApiNamespace": "/api/public/data-products/{slug}",
            "responseKind": "relation",
            "responseSchema": {
                "type": "object",
                "properties": {
                    "product": {"type": "object"},
                    "columns": {"type": "array"},
                    "items": {"type": "array"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                    "hasMore": {"type": "boolean"},
                },
            },
            "sampleResponse": {
                "product": preview_product.payload(base_url=kwargs.get("base_url")),
                "columns": [{"name": "order_id", "dataType": "INTEGER"}],
                "items": [{"order_id": 1}],
                "limit": 100,
                "offset": 0,
                "hasMore": False,
            },
            "openApiDocument": {
                "openapi": "3.1.0",
                "paths": {
                    preview_product.public_path: {
                        "get": {
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {"type": "object"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            },
        }

    def create_data_product(self, **kwargs) -> dict[str, object]:
        product = build_product(
            product_id="created-product",
            slug=kwargs.get("slug") or "new-product",
            title=kwargs["title"],
            source=DataProductSourceDescriptor(
                source_kind=kwargs["source"]["sourceKind"],
                source_id=kwargs["source"]["sourceId"],
                relation=kwargs["source"].get("relation", ""),
                bucket=kwargs["source"].get("bucket", ""),
                key=kwargs["source"].get("key", ""),
                source_display_name=kwargs["source"].get("sourceDisplayName", ""),
                source_platform=kwargs["source"].get("sourcePlatform", ""),
            ),
        )
        self.products_by_id[product.product_id] = product
        self.products_by_slug[product.slug] = product
        return {"action": "created", "product": product.payload(base_url=kwargs.get("base_url"))}

    def update_data_product_metadata(self, **kwargs) -> dict[str, object]:
        existing = self.products_by_id[kwargs["product_id"]]
        updated = build_product(
            product_id=existing.product_id,
            slug=existing.slug,
            title=kwargs["title"],
            source=existing.source,
        )
        updated.description = kwargs["description"]
        updated.owner = kwargs["owner"]
        updated.domain = kwargs["domain"]
        updated.tags = list(kwargs["tags"])
        updated.access_level = kwargs["access_level"]
        updated.access_note = kwargs["access_note"]
        updated.request_access_contact = kwargs["request_access_contact"]
        updated.updated_at = "2026-04-20T09:30:00+00:00"
        self.products_by_id[updated.product_id] = updated
        self.products_by_slug[updated.slug] = updated
        return {"action": "updated", "product": updated.payload(base_url=kwargs.get("base_url"))}

    def delete_data_product(self, *, product_id: str, base_url: str | None = None) -> dict[str, object]:
        product = self.products_by_id.pop(product_id)
        self.products_by_slug.pop(product.slug, None)
        return {"action": "deleted", "product": product.payload(base_url=base_url)}

    def data_product_by_slug(self, slug: str) -> DataProductDefinition:
        return self.products_by_slug[slug]

    def data_product_documentation(
        self,
        *,
        slug: str,
        base_url: str | None = None,
    ) -> dict[str, object]:
        product = self.products_by_slug[slug]
        return {
            "product": product.payload(base_url=base_url),
            "sourceSummary": "Live relation rows from pg_oltp.finance.orders.",
            "liveReadOnlyCopy": "Published data products are live and read-only in v1.",
            "responseKind": "relation",
            "requestParameters": [
                {
                    "name": "limit",
                    "type": "integer",
                    "required": False,
                    "default": 100,
                    "description": "Maximum number of rows to return.",
                },
                {
                    "name": "offset",
                    "type": "integer",
                    "required": False,
                    "default": 0,
                    "description": "Row offset for pagination.",
                },
            ],
            "responseSchema": {
                "type": "object",
                "properties": {
                    "product": {"type": "object"},
                    "columns": {"type": "array"},
                    "items": {"type": "array"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                    "hasMore": {"type": "boolean"},
                },
            },
            "sampleResponse": {
                "product": product.payload(base_url=base_url),
                "columns": [{"name": "order_id", "dataType": "INTEGER"}],
                "items": [{"order_id": 1}],
                "limit": 100,
                "offset": 0,
                "hasMore": False,
            },
            "openApiDocument": {
                "openapi": "3.1.0",
                "paths": {
                    product.public_path: {
                        "get": {
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {"type": "object"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            },
        }

    def public_data_product_relation(
        self,
        *,
        slug: str,
        limit: int,
        offset: int,
        base_url: str | None = None,
    ) -> dict[str, object]:
        product = self.products_by_slug[slug]
        return {
            "product": product.payload(base_url=base_url),
            "columns": [{"name": "order_id", "dataType": "INTEGER"}],
            "items": [{"order_id": 1 + offset}],
            "limit": limit,
            "offset": offset,
            "hasMore": True,
        }

    def public_data_product_bucket(
        self,
        *,
        slug: str,
        prefix: str,
        base_url: str | None = None,
    ) -> dict[str, object]:
        product = self.products_by_slug[slug]
        return {
            "product": product.payload(base_url=base_url),
            "prefix": prefix,
            "entries": [
                {
                    "entryKind": "file",
                    "name": "orders.csv",
                    "bucket": "shared-finance",
                    "prefix": "exports/orders.csv",
                    "path": "s3://shared-finance/exports/orders.csv",
                    "fileFormat": "csv",
                    "sizeBytes": 1024,
                    "hasChildren": False,
                    "selectable": False,
                }
            ],
        }

    def public_data_product_stream(self, *, slug: str):
        _ = self.products_by_slug[slug]
        return type(
            "Artifact",
            (),
            {
                "iterator": iter([b"order_id,total\n1,10\n"]),
                "content_type": "text/csv",
                "content_length": 20,
            },
        )()


async def read_streaming_body(response) -> bytes:
    chunks: list[bytes] = []
    async for chunk in response.body_iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode("utf-8"))
    return b"".join(chunks)


class DataProductsRouteTests(unittest.TestCase):
    def test_data_products_partial_renders_expected_surface(self) -> None:
        response = data_products_page(
            request=build_request("/data-products", partial=True),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-data-products-page', body)
        self.assertIn('data-open-data-product-dialog', body)
        self.assertIn('data-data-product-search', body)
        self.assertIn('data-data-product-card', body)
        self.assertIn('data-copy-data-product-url', body)
        self.assertIn('data-open-data-product-page', body)
        self.assertIn('data-edit-data-product', body)
        self.assertIn('data-delete-data-product', body)
        self.assertIn('data-product-sources-json', body)
        self.assertIn('Open Data Product Page', body)
        self.assertNotIn('OpenAPI Documentation', body)

    def test_data_products_full_page_hides_sidebar(self) -> None:
        response = data_products_page(
            request=build_request("/data-products", partial=False),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("DAAIFL Data Products Workbench", body)
        self.assertIn("shell-sidebar-hidden", body)
        self.assertIn('data-open-data-products-workbench', body)

    def test_home_partial_links_to_public_catalog(self) -> None:
        response = index(
            request=build_request("/", partial=True),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Published Catalog", body)
        self.assertIn('href="/dataproducts/"', body)

    def test_public_catalog_page_lists_published_products(self) -> None:
        response = public_data_products_page(
            request=build_request("/dataproducts/"),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Published Data Products", body)
        self.assertIn('href="/dataproducts/published-orders"', body)
        self.assertIn("/api/public/data-products/published-orders", body)

    def test_public_detail_page_renders_contract(self) -> None:
        response = public_data_product_page(
            slug="published-orders",
            request=build_request("/dataproducts/published-orders"),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Request contract", body)
        self.assertIn("Response schema", body)
        self.assertIn("OpenAPI excerpt", body)
        self.assertIn("Sample response", body)
        self.assertIn("limit", body)
        self.assertIn("Open Endpoint", body)

    def test_fastapi_builtin_docs_are_disabled(self) -> None:
        self.assertIsNone(main_app.docs_url)
        self.assertIsNone(main_app.redoc_url)
        self.assertIsNone(main_app.openapi_url)

    def test_management_api_routes_return_expected_payload_shapes(self) -> None:
        service = FakeWorkbenchService()

        listed = list_data_products(
            request=build_request("/api/data-products"),
            service=service,
        )
        listed_payload = json.loads(listed.body.decode("utf-8"))
        self.assertEqual(len(listed_payload["products"]), 1)
        self.assertEqual(listed_payload["products"][0]["slug"], "published-orders")

        previewed = preview_data_product(
            payload=DataProductPreviewPayload.model_validate(
                {
                    "source": {
                        "sourceKind": "relation",
                        "sourceId": "pg_oltp",
                        "relation": "pg_oltp.finance.orders",
                        "sourceDisplayName": "finance.orders",
                        "sourcePlatform": "postgres",
                    },
                    "title": "VAT Orders",
                    "slug": "vat-orders",
                }
            ),
            request=build_request("/api/data-products/preview"),
            service=service,
        )
        preview_payload = json.loads(previewed.body.decode("utf-8"))
        self.assertEqual(preview_payload["product"]["slug"], "vat-orders")
        self.assertEqual(preview_payload["responseKind"], "relation")

        created = create_data_product(
            payload=DataProductCreatePayload.model_validate(
                {
                    "source": {
                        "sourceKind": "relation",
                        "sourceId": "pg_oltp",
                        "relation": "pg_oltp.finance.orders",
                        "sourceDisplayName": "finance.orders",
                        "sourcePlatform": "postgres",
                    },
                    "title": "VAT Orders",
                    "slug": "vat-orders",
                    "description": "VAT orders endpoint.",
                }
            ),
            request=build_request("/api/data-products"),
            service=service,
        )
        created_payload = json.loads(created.body.decode("utf-8"))
        self.assertEqual(created_payload["action"], "created")
        self.assertEqual(created_payload["product"]["slug"], "vat-orders")

        updated = update_data_product(
            product_id="created-product",
            payload=DataProductUpdatePayload.model_validate(
                {
                    "title": "VAT Orders Updated",
                    "description": "Updated description.",
                    "owner": "Finance",
                    "domain": "Tax",
                    "tags": ["vat", "orders"],
                    "accessLevel": "restricted",
                    "accessNote": "Managed publication.",
                    "requestAccessContact": "owner@example.test",
                }
            ),
            request=build_request("/api/data-products/created-product"),
            service=service,
        )
        updated_payload = json.loads(updated.body.decode("utf-8"))
        self.assertEqual(updated_payload["action"], "updated")
        self.assertEqual(updated_payload["product"]["title"], "VAT Orders Updated")
        self.assertEqual(updated_payload["product"]["accessLevel"], "restricted")

        deleted = delete_data_product(
            product_id="created-product",
            request=build_request("/api/data-products/created-product"),
            service=service,
        )
        deleted_payload = json.loads(deleted.body.decode("utf-8"))
        self.assertEqual(deleted_payload["action"], "deleted")
        self.assertEqual(deleted_payload["product"]["slug"], "vat-orders")

    def test_public_relation_route_returns_paginated_rows(self) -> None:
        response = read_public_data_product(
            slug="published-orders",
            request=build_request("/api/public/data-products/published-orders"),
            limit=25,
            offset=10,
            prefix="",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["product"]["slug"], "published-orders")
        self.assertEqual(payload["limit"], 25)
        self.assertEqual(payload["offset"], 10)
        self.assertEqual(payload["columns"][0]["name"], "order_id")
        self.assertTrue(payload["hasMore"])

    def test_public_bucket_route_returns_listing(self) -> None:
        response = read_public_data_product(
            slug="shared-workspace-bucket",
            request=build_request("/api/public/data-products/shared-workspace-bucket"),
            limit=100,
            offset=0,
            prefix="exports/",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["product"]["slug"], "shared-workspace-bucket")
        self.assertEqual(payload["prefix"], "exports/")
        self.assertEqual(payload["entries"][0]["entryKind"], "file")

    def test_public_object_route_streams_raw_content(self) -> None:
        response = read_public_data_product(
            slug="finance-object",
            request=build_request("/api/public/data-products/finance-object"),
            limit=100,
            offset=0,
            prefix="",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.media_type, "text/csv")
        self.assertEqual(response.headers["Content-Length"], "20")
        body = asyncio.run(read_streaming_body(response))
        self.assertEqual(body, b"order_id,total\n1,10\n")


if __name__ == "__main__":
    unittest.main()

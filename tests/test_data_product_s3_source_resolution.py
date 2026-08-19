from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
import sys
import tempfile
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.data_products.daca_client import (  # noqa: E402
    DacaProductStatus,
    DacaPublicationResult,
)
from bit_data_workbench.backend.data_products.manager import DataProductManager  # noqa: E402
from bit_data_workbench.backend.data_products.parquet_object import (  # noqa: E402
    S3ParquetObjectReader,
)
from bit_data_workbench.backend.data_products.publication import (  # noqa: E402
    DacaPublicationCoordinator,
    JOURNEY_SLUG,
)
from bit_data_workbench.backend.data_products.registry import DataProductStore  # noqa: E402
from bit_data_workbench.backend.data_products.source_resolution import (  # noqa: E402
    relation_backed_daca_source,
)
from bit_data_workbench.models import (  # noqa: E402
    DacaPublicationReference,
    DataProductDefinition,
    DataProductSourceDescriptor,
    SourceField,
)


BUCKET = "data-analysts-journey"
KEY = "products/tax-product.parquet"
OBJECT_PATH = f"s3://{BUCKET}/{KEY}"
FIELDS = [
    SourceField(name="canton_code", data_type="VARCHAR"),
    SourceField(name="tax_year", data_type="INTEGER"),
    SourceField(name="annual_projection_chf", data_type="DECIMAL(18,2)"),
]


class FakeDuckDBConnection:
    def __init__(self, rows: list[tuple[Any, ...]], commands: list[str]) -> None:
        self._rows = rows
        self._commands = commands
        self.closed = False

    def execute(self, sql: str) -> "FakeDuckDBConnection":
        self._commands.append(sql)
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def close(self) -> None:
        self.closed = True


def build_manager(
    temp_dir: str,
    *,
    fields_provider=lambda _bucket, _key: list(FIELDS),
    page_provider=None,
    relation_fields_provider=lambda _relation: [],
    create_worker_connection=lambda: None,
) -> DataProductManager:
    return DataProductManager(
        settings=SimpleNamespace(),
        store=DataProductStore(Path(temp_dir) / "data-products.json"),
        create_worker_connection=create_worker_connection,
        relation_fields_provider=relation_fields_provider,
        catalog_provider=lambda: [],
        s3_bucket_snapshot_provider=lambda **_kwargs: {},
        s3_parquet_fields_provider=fields_provider,
        s3_parquet_page_provider=page_provider,
    )


class S3ParquetObjectReaderTests(unittest.TestCase):
    def test_daca_converts_s3_object_to_path_backed_relation_candidate(self) -> None:
        source = {
            "sourceKind": "object",
            "sourceId": "s3",
            "bucket": BUCKET,
            "key": KEY,
        }

        resolved = relation_backed_daca_source(source)

        self.assertEqual(resolved["sourceKind"], "relation")
        self.assertEqual(resolved["relation"], "")
        self.assertEqual(resolved["bucket"], BUCKET)
        self.assertEqual(resolved["key"], KEY)
        self.assertEqual(source["sourceKind"], "object")
        self.assertEqual(
            relation_backed_daca_source(
                {"sourceKind": "object", "sourceId": "workspace.local"}
            )["sourceKind"],
            "object",
        )

    def test_reads_schema_from_exact_parquet_path_and_caches_by_revision(self) -> None:
        commands: list[str] = []
        connections: list[FakeDuckDBConnection] = []
        head_calls: list[tuple[str, str]] = []

        def connection_factory() -> FakeDuckDBConnection:
            connection = FakeDuckDBConnection(
                [
                    ("canton_code", "VARCHAR", "YES", None, None, None),
                    ("tax_year", "INTEGER", "YES", None, None, None),
                ],
                commands,
            )
            connections.append(connection)
            return connection

        reader = S3ParquetObjectReader(
            connection_factory=connection_factory,  # type: ignore[arg-type]
            object_head_provider=lambda bucket, key: (
                head_calls.append((bucket, key))
                or {"ETag": '"revision-1"', "ContentLength": 512}
            ),
        )

        first = reader.fields(BUCKET, f"/{KEY}")
        second = reader.fields(BUCKET, KEY)

        self.assertEqual(first, second)
        self.assertEqual(
            [(field.name, field.data_type) for field in first],
            [("canton_code", "VARCHAR"), ("tax_year", "INTEGER")],
        )
        self.assertEqual(head_calls, [(BUCKET, KEY), (BUCKET, KEY)])
        self.assertEqual(len(connections), 1)
        self.assertTrue(connections[0].closed)
        self.assertEqual(
            commands,
            [f"DESCRIBE SELECT * FROM read_parquet('{OBJECT_PATH}')"],
        )

    def test_page_reads_only_exact_object_and_returns_has_more(self) -> None:
        commands: list[str] = []
        results = iter(
            [
                [("canton_code", "VARCHAR", "YES", None, None, None)],
                [("AG",), ("BE",), ("ZH",)],
            ]
        )
        reader = S3ParquetObjectReader(
            connection_factory=lambda: FakeDuckDBConnection(  # type: ignore[arg-type]
                next(results), commands
            ),
            object_head_provider=lambda _bucket, _key: {"ETag": '"revision-1"'},
        )

        fields, rows, has_more = reader.page(BUCKET, KEY, limit=2, offset=5)

        self.assertEqual([field.name for field in fields], ["canton_code"])
        self.assertEqual(rows, [("AG",), ("BE",)])
        self.assertTrue(has_more)
        self.assertEqual(
            commands,
            [
                f"DESCRIBE SELECT * FROM read_parquet('{OBJECT_PATH}')",
                f"SELECT * FROM read_parquet('{OBJECT_PATH}') LIMIT 3 OFFSET 5",
            ],
        )

    def test_escapes_quote_in_object_key_as_sql_literal(self) -> None:
        commands: list[str] = []
        reader = S3ParquetObjectReader(
            connection_factory=lambda: FakeDuckDBConnection(  # type: ignore[arg-type]
                [("value", "INTEGER")], commands
            ),
            object_head_provider=lambda _bucket, _key: {},
        )

        reader.fields(BUCKET, "products/o'hare.parquet")

        self.assertEqual(
            commands,
            [
                "DESCRIBE SELECT * FROM read_parquet("
                "'s3://data-analysts-journey/products/o''hare.parquet')"
            ],
        )

    def test_rejects_missing_non_parquet_and_fieldless_objects(self) -> None:
        reader = S3ParquetObjectReader(
            connection_factory=lambda: FakeDuckDBConnection([], []),  # type: ignore[arg-type]
            object_head_provider=lambda _bucket, _key: {},
        )
        with self.assertRaisesRegex(ValueError, "require one concrete Parquet"):
            reader.fields(BUCKET, "products/tax-product.csv")

        missing_reader = S3ParquetObjectReader(
            connection_factory=lambda: self.fail("Missing object must not be opened"),
            object_head_provider=lambda _bucket, _key: (_ for _ in ()).throw(
                FileNotFoundError("missing")
            ),
        )
        with self.assertRaisesRegex(ValueError, "is not available in Shared Workspace"):
            missing_reader.fields(BUCKET, KEY)

        with self.assertRaisesRegex(ValueError, "does not expose any schema fields"):
            reader.fields(BUCKET, KEY)


class DataProductManagerS3ResolutionTests(unittest.TestCase):
    def test_preview_and_create_retain_exact_path_and_derived_schema(self) -> None:
        field_calls: list[tuple[str, str]] = []

        def fields_provider(bucket: str, key: str) -> list[SourceField]:
            field_calls.append((bucket, key))
            return list(FIELDS)

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = build_manager(temp_dir, fields_provider=fields_provider)
            source = {
                "sourceKind": "relation",
                "sourceId": "s3",
                "relation": "stale.hard_coded_name",
                "bucket": BUCKET,
                "key": f"/{KEY}",
                "sourceDisplayName": "Tax product",
            }

            preview = manager.preview_product(source=source, title="Tax product")
            created = manager.create_product(source=source, title="Tax product")

        self.assertEqual(preview["responseKind"], "relation")
        item_properties = preview["responseSchema"]["properties"]["items"][
            "items"
        ]["properties"]
        self.assertEqual(
            list(item_properties),
            ["canton_code", "tax_year", "annual_projection_chf"],
        )
        self.assertEqual(created["product"]["relation"], "")
        self.assertEqual(created["product"]["bucket"], BUCKET)
        self.assertEqual(created["product"]["key"], KEY)
        self.assertGreaterEqual(field_calls.count((BUCKET, KEY)), 2)

    def test_daca_payload_uses_direct_schema_and_persists_path(self) -> None:
        published_payloads: list[dict[str, object]] = []

        class RecordingClient:
            def publish(self, payload: dict[str, object]) -> DacaPublicationResult:
                published_payloads.append(payload)
                return DacaPublicationResult(
                    publication_id="publication-1",
                    product_id="product-1",
                    state="pending_review",
                    created=True,
                    task_ids=["task-1"],
                    missing_fields=["dcatReviewed"],
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = build_manager(temp_dir)
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=RecordingClient(),  # type: ignore[arg-type]
                daca_ui_url="https://catalog.example.test",
                public_base_url="https://daaif.example.test",
            )

            created = coordinator.create_managed_product(
                source={
                    "sourceKind": "object",
                    "sourceId": "s3",
                    "bucket": BUCKET,
                    "key": KEY,
                    "sourceDisplayName": "Tax product",
                    "sourcePlatform": "s3",
                },
                title="Tax product",
                slug=JOURNEY_SLUG,
                description="Synthetic tax metrics.",
                owner="Joel Ruod",
                domain="Tax",
                tags=["synthetic"],
            )

        schema_fields = published_payloads[0]["technicalMetadata"]["schemaFields"]
        self.assertEqual(
            [(field["name"], field["dataType"]) for field in schema_fields],
            [(field.name, field.data_type) for field in FIELDS],
        )
        self.assertEqual(
            published_payloads[0]["technicalMetadata"]["endpoint"],
            {
                "baseUrl": "https://daaif.example.test",
                "path": f"/api/public/data-products/{JOURNEY_SLUG}",
                "method": "GET",
            },
        )
        self.assertEqual(created["product"]["relation"], "")
        self.assertEqual(created["product"]["bucket"], BUCKET)
        self.assertEqual(created["product"]["key"], KEY)

    def test_schema_failure_does_not_reserve_or_publish(self) -> None:
        published_payloads: list[dict[str, object]] = []

        class RecordingClient:
            def publish(self, payload: dict[str, object]) -> None:
                published_payloads.append(payload)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = DataProductStore(Path(temp_dir) / "data-products.json")
            manager = DataProductManager(
                settings=SimpleNamespace(),
                store=store,
                create_worker_connection=lambda: None,
                relation_fields_provider=lambda _relation: [],
                catalog_provider=lambda: [],
                s3_bucket_snapshot_provider=lambda **_kwargs: {},
                s3_parquet_fields_provider=lambda _bucket, _key: (_ for _ in ()).throw(
                    ValueError("stored Parquet result is not readable")
                ),
            )
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=RecordingClient(),  # type: ignore[arg-type]
                daca_ui_url="https://catalog.example.test",
                public_base_url="https://daaif.example.test",
            )

            with self.assertRaisesRegex(ValueError, "not readable"):
                coordinator.create_managed_product(
                    source={
                        "sourceKind": "object",
                        "sourceId": "s3",
                        "bucket": BUCKET,
                        "key": "products/missing.parquet",
                    },
                    title="Missing product",
                    slug="missing-product",
                    owner="Joel Ruod",
                )

            self.assertEqual(store.list_products(), [])
            self.assertEqual(published_payloads, [])

    def test_path_backed_parquet_is_exposed_as_paginated_json_rows(self) -> None:
        page_calls: list[tuple[str, str, int, int]] = []
        all_rows = [
            ("AG", 2025, 125000.50),
            ("BE", 2026, 175000.25),
            ("ZH", 2026, 250000.75),
        ]

        def page_provider(bucket: str, key: str, limit: int, offset: int):
            page_calls.append((bucket, key, limit, offset))
            rows = all_rows[offset : offset + limit]
            return list(FIELDS), rows, offset + len(rows) < len(all_rows)

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = build_manager(
                temp_dir,
                page_provider=page_provider,
                create_worker_connection=lambda: self.fail(
                    "Path-backed Parquet must not use shared relation connection"
                ),
            )
            manager.create_product(
                source={
                    "sourceKind": "relation",
                    "sourceId": "s3",
                    "bucket": BUCKET,
                    "key": KEY,
                },
                title="Tax product",
                slug="tax-product",
            )

            first_page = manager.public_relation_payload(
                slug="tax-product", limit=2, offset=0
            )
            second_page = manager.public_relation_payload(
                slug="tax-product", limit=2, offset=2
            )

        self.assertEqual(
            [column["name"] for column in first_page["columns"]],
            [field.name for field in FIELDS],
        )
        self.assertEqual(len(first_page["items"]), 2)
        self.assertTrue(first_page["hasMore"])
        self.assertEqual(second_page["items"][0]["canton_code"], "ZH")
        self.assertFalse(second_page["hasMore"])
        self.assertEqual(
            page_calls,
            [(BUCKET, KEY, 2, 0), (BUCKET, KEY, 2, 2)],
        )

    def test_legacy_discovered_relation_uses_path_and_is_cleared_on_preview(self) -> None:
        page_calls: list[tuple[str, str, int, int]] = []

        def page_provider(bucket: str, key: str, limit: int, offset: int):
            page_calls.append((bucket, key, limit, offset))
            return list(FIELDS), [("AG", 2026, 1.25)], False

        with tempfile.TemporaryDirectory() as temp_dir:
            store = DataProductStore(Path(temp_dir) / "data-products.json")
            store.create_product(
                DataProductDefinition(
                    product_id="legacy-product",
                    slug="legacy-tax-product",
                    title="Legacy tax product",
                    description="",
                    source=DataProductSourceDescriptor(
                        source_kind="relation",
                        source_id="s3",
                        relation=(
                            "data_analysts_journey_6f15a669."
                            "kantonale_gewerbesteuer_soll_ist_2022_2026"
                        ),
                        bucket=BUCKET,
                        key=KEY,
                    ),
                    public_path="/api/public/data-products/legacy-tax-product",
                    created_at="2026-08-18T00:00:00+00:00",
                    updated_at="2026-08-18T00:00:00+00:00",
                )
            )
            manager = DataProductManager(
                settings=SimpleNamespace(),
                store=store,
                create_worker_connection=lambda: self.fail(
                    "Legacy S3 products must no longer query a discovered relation"
                ),
                relation_fields_provider=lambda _relation: self.fail(
                    "Legacy S3 products must inspect the exact object path"
                ),
                catalog_provider=lambda: [],
                s3_bucket_snapshot_provider=lambda **_kwargs: {},
                s3_parquet_fields_provider=lambda _bucket, _key: list(FIELDS),
                s3_parquet_page_provider=page_provider,
            )

            public_page = manager.public_relation_payload(
                slug="legacy-tax-product", limit=10, offset=0
            )
            preview = manager.preview_product(
                source={
                    "sourceKind": "relation",
                    "sourceId": "s3",
                    "relation": "",
                    "bucket": BUCKET,
                    "key": KEY,
                },
                title="Legacy tax product",
                slug="legacy-tax-product",
            )

        self.assertEqual(public_page["items"][0]["canton_code"], "AG")
        self.assertEqual(page_calls, [(BUCKET, KEY, 10, 0)])
        self.assertEqual(preview["product"]["relation"], "")
        self.assertEqual(
            list(
                preview["responseSchema"]["properties"]["items"]["items"][
                    "properties"
                ]
            ),
            [field.name for field in FIELDS],
        )

    def test_legacy_relation_name_does_not_force_daca_metadata_republication(self) -> None:
        status_calls: list[tuple[str, str]] = []

        class StatusOnlyClient:
            def publish(self, _payload: dict[str, object]) -> DacaPublicationResult:
                raise AssertionError(
                    "Changing only the obsolete relation name must not replay DaCa metadata"
                )

            def product_status(
                self, *, product_id: str, owner_user_id: str
            ) -> DacaProductStatus:
                status_calls.append((product_id, owner_user_id))
                return DacaProductStatus(
                    product_id=product_id,
                    state="published",
                    missing_fields=(),
                )

            def record_source_refresh(
                self,
                *,
                product_id: str,
                owner_user_id: str,
                source_updated_at: str,
            ) -> None:
                if not source_updated_at:
                    raise AssertionError("The DAAIF source refresh timestamp is required")
                status_calls.append((f"refresh:{product_id}", owner_user_id))

        with tempfile.TemporaryDirectory() as temp_dir:
            store = DataProductStore(Path(temp_dir) / "data-products.json")
            existing = store.create_product(
                DataProductDefinition(
                    product_id="local-product",
                    slug=JOURNEY_SLUG,
                    title="Tax product",
                    description="Synthetic tax metrics.",
                    source=DataProductSourceDescriptor(
                        source_kind="relation",
                        source_id="s3",
                        relation="obsolete.discovery_relation",
                        bucket=BUCKET,
                        key=KEY,
                        source_display_name="Tax product",
                        source_platform="s3",
                    ),
                    public_path=f"/api/public/data-products/{JOURNEY_SLUG}",
                    owner="Joel Ruod",
                    domain="Tax",
                    tags=["synthetic"],
                    access_level="internal",
                    daca_publication=DacaPublicationReference(
                        source_product_id=f"daaif:{JOURNEY_SLUG}:v1",
                        publication_id="publication-1",
                        product_id="product-1",
                        state="pending_review",
                        created=True,
                    ),
                    created_at="2026-08-18T00:00:00+00:00",
                    updated_at="2026-08-18T00:00:00+00:00",
                )
            )
            manager = DataProductManager(
                settings=SimpleNamespace(),
                store=store,
                create_worker_connection=lambda: None,
                relation_fields_provider=lambda _relation: [],
                catalog_provider=lambda: [],
                s3_bucket_snapshot_provider=lambda **_kwargs: {},
                s3_parquet_fields_provider=lambda _bucket, _key: list(FIELDS),
            )
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=StatusOnlyClient(),  # type: ignore[arg-type]
                daca_ui_url="https://catalog.example.test",
                public_base_url="https://daaif.example.test",
            )

            result = coordinator.create_managed_product(
                source={
                    "sourceKind": "object",
                    "sourceId": "s3",
                    "bucket": BUCKET,
                    "key": KEY,
                    "sourceDisplayName": "Tax product",
                    "sourcePlatform": "s3",
                },
                title=existing.title,
                slug=existing.slug,
                description=existing.description,
                owner=existing.owner,
                domain=existing.domain,
                tags=existing.tags,
                access_level=existing.access_level,
                overwrite_existing=True,
                expected_product_id=existing.product_id,
                expected_updated_at=existing.updated_at,
            )

        self.assertEqual(
            status_calls,
            [("refresh:product-1", "joel.ruod"), ("product-1", "joel.ruod")],
        )
        self.assertEqual(result["product"]["relation"], "")
        self.assertEqual(result["dacaPublication"]["state"], "published")

    def test_existing_registered_relation_keeps_old_path(self) -> None:
        relation = "pg_oltp.orders"
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "registered-relation.duckdb"
            connection = duckdb.connect(str(database_path))
            connection.execute(
                "CREATE SCHEMA pg_oltp; "
                "CREATE TABLE pg_oltp.orders AS "
                "SELECT 1 AS order_id UNION ALL SELECT 2"
            )
            connection.close()

            manager = build_manager(
                temp_dir,
                fields_provider=lambda _bucket, _key: self.fail(
                    "Registered relations must not inspect S3"
                ),
                relation_fields_provider=lambda requested: (
                    [SourceField(name="order_id", data_type="INTEGER")]
                    if requested == relation
                    else []
                ),
                create_worker_connection=lambda: duckdb.connect(str(database_path)),
            )
            manager.create_product(
                source={
                    "sourceKind": "relation",
                    "sourceId": "pg_oltp",
                    "relation": relation,
                    "sourceDisplayName": "Orders",
                },
                title="Orders",
                slug="orders",
            )

            payload = manager.public_relation_payload(
                slug="orders", limit=1, offset=0
            )

        self.assertEqual(payload["items"], [{"order_id": 1}])
        self.assertTrue(payload["hasMore"])


if __name__ == "__main__":
    unittest.main()

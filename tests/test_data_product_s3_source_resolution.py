from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.data_products.manager import DataProductManager  # noqa: E402
from bit_data_workbench.backend.data_products.daca_client import (  # noqa: E402
    DacaPublicationResult,
)
from bit_data_workbench.backend.data_products.publication import (  # noqa: E402
    DacaPublicationCoordinator,
    JOURNEY_SLUG,
)
from bit_data_workbench.backend.data_products.registry import DataProductStore  # noqa: E402
from bit_data_workbench.backend.data_products.source_resolution import (  # noqa: E402
    S3RelationSourceResolver,
)
from bit_data_workbench.models import SourceField  # noqa: E402


OBJECT_PATH = "s3://data-analysts-journey/products/tax-product.parquet"


class S3RelationSourceResolverTests(unittest.TestCase):
    def test_uses_exact_object_path_and_actual_collision_safe_relation(self) -> None:
        sync_calls: list[tuple[tuple[str, ...], bool]] = []
        actual_relation = "data_analysts_journey.tax_product_a19f27c1"
        resolver = S3RelationSourceResolver(
            sync_s3_buckets=lambda buckets, *, emit_event=True: sync_calls.append(
                (tuple(buckets), emit_event)
            ),
            relation_specs_provider=lambda: {
                "data_analysts_journey.tax_product": SimpleNamespace(
                    object_path="s3://data-analysts-journey/archive/tax-product.parquet"
                ),
                actual_relation: SimpleNamespace(object_path=OBJECT_PATH),
            },
            relation_fields_provider=lambda relation: (
                [SourceField(name="canton_code", data_type="VARCHAR")]
                if relation == actual_relation
                else []
            ),
            object_head_provider=lambda _bucket, _key: None,
        )

        resolved = resolver.resolve(
            {
                "sourceKind": "relation",
                "sourceId": "s3",
                "relation": "stale.relation",
                "bucket": "data-analysts-journey",
                "key": "/products/tax-product.parquet",
            }
        )

        self.assertEqual(resolved["relation"], actual_relation)
        self.assertEqual(resolved["key"], "products/tax-product.parquet")
        self.assertEqual(sync_calls, [])

    def test_runs_one_bounded_bucket_sync_when_relation_is_not_ready(self) -> None:
        specs: dict[str, object] = {}
        sync_calls: list[tuple[tuple[str, ...], bool]] = []

        def sync(buckets, *, emit_event=True):
            sync_calls.append((tuple(buckets), emit_event))
            specs["data_analysts_journey.tax_product"] = SimpleNamespace(
                object_path=OBJECT_PATH
            )

        resolver = S3RelationSourceResolver(
            sync_s3_buckets=sync,
            relation_specs_provider=lambda: dict(specs),
            relation_fields_provider=lambda _relation: [
                SourceField(name="tax_year", data_type="INTEGER")
            ],
            object_head_provider=lambda _bucket, _key: None,
        )

        resolved = resolver.resolve(
            {
                "sourceKind": "relation",
                "sourceId": "s3",
                "bucket": "data-analysts-journey",
                "key": "products/tax-product.parquet",
            }
        )

        self.assertEqual(
            resolved["relation"], "data_analysts_journey.tax_product"
        )
        self.assertEqual(sync_calls, [(('data-analysts-journey',), True)])

    def test_rejects_missing_or_fieldless_relation_after_sync(self) -> None:
        for specs in (
            {},
            {
                "data_analysts_journey.tax_product": SimpleNamespace(
                    object_path=OBJECT_PATH
                )
            },
        ):
            with self.subTest(specs=specs):
                resolver = S3RelationSourceResolver(
                    sync_s3_buckets=lambda _buckets, *, emit_event=True: None,
                    relation_specs_provider=lambda specs=specs: specs,
                    relation_fields_provider=lambda _relation: [],
                    object_head_provider=lambda _bucket, _key: None,
                )

                with self.assertRaisesRegex(
                    ValueError,
                    "could not be registered as a queryable relation",
                ):
                    resolver.resolve(
                        {
                            "sourceKind": "relation",
                            "sourceId": "s3",
                            "bucket": "data-analysts-journey",
                            "key": "products/tax-product.parquet",
                        }
                    )

    def test_does_not_change_non_s3_relations(self) -> None:
        source = {
            "sourceKind": "relation",
            "sourceId": "pg_oltp",
            "relation": "pg_oltp.public.orders",
        }
        resolver = S3RelationSourceResolver(
            sync_s3_buckets=lambda *_args, **_kwargs: self.fail(
                "PostgreSQL must not trigger S3 discovery"
            ),
            relation_specs_provider=lambda: {},
            relation_fields_provider=lambda _relation: [],
            object_head_provider=lambda _bucket, _key: None,
        )

        self.assertEqual(resolver.resolve(source), source)

    def test_rejects_a_stale_relation_when_the_object_is_gone(self) -> None:
        resolver = S3RelationSourceResolver(
            sync_s3_buckets=lambda *_args, **_kwargs: self.fail(
                "A missing object must fail before discovery"
            ),
            relation_specs_provider=lambda: {
                "data_analysts_journey.tax_product": SimpleNamespace(
                    object_path=OBJECT_PATH
                )
            },
            relation_fields_provider=lambda _relation: [
                SourceField(name="tax_year", data_type="INTEGER")
            ],
            object_head_provider=lambda _bucket, _key: (_ for _ in ()).throw(
                FileNotFoundError("missing")
            ),
        )

        with self.assertRaisesRegex(
            ValueError,
            "is not available in Shared Workspace",
        ):
            resolver.resolve(
                {
                    "sourceKind": "relation",
                    "sourceId": "s3",
                    "bucket": "data-analysts-journey",
                    "key": "products/tax-product.parquet",
                }
            )


class DataProductManagerS3ResolutionTests(unittest.TestCase):
    def test_preview_and_create_retain_relation_provenance_and_schema(self) -> None:
        relation = "data_analysts_journey.tax_product"
        fields = [
            SourceField(name="canton_code", data_type="VARCHAR"),
            SourceField(name="tax_year", data_type="INTEGER"),
        ]

        def resolve(source: dict[str, object]) -> dict[str, object]:
            return {**source, "relation": relation}

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = DataProductManager(
                settings=SimpleNamespace(),
                store=DataProductStore(Path(temp_dir) / "data-products.json"),
                create_worker_connection=lambda: None,
                relation_fields_provider=lambda requested: (
                    fields if requested == relation else []
                ),
                catalog_provider=lambda: [],
                s3_bucket_snapshot_provider=lambda **_kwargs: {},
                s3_relation_source_resolver=resolve,
            )
            source = {
                "sourceKind": "relation",
                "sourceId": "s3",
                "bucket": "data-analysts-journey",
                "key": "products/tax-product.parquet",
                "sourceDisplayName": "Tax product",
            }

            preview = manager.preview_product(source=source, title="Tax product")
            created = manager.create_product(source=source, title="Tax product")

        self.assertEqual(preview["responseKind"], "relation")
        item_properties = preview["responseSchema"]["properties"]["items"][
            "items"
        ]["properties"]
        self.assertEqual(list(item_properties), ["canton_code", "tax_year"])
        created_source = created["product"]
        self.assertEqual(created_source["relation"], relation)
        self.assertEqual(created_source["bucket"], "data-analysts-journey")
        self.assertEqual(created_source["key"], "products/tax-product.parquet")

    def test_daca_publication_uses_resolved_schema_and_persists_provenance(self) -> None:
        relation = "data_analysts_journey.tax_product"
        fields = [
            SourceField(name="canton_code", data_type="VARCHAR"),
            SourceField(name="tax_year", data_type="INTEGER"),
            SourceField(name="annual_projection_chf", data_type="DECIMAL(18,2)"),
        ]
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
            manager = DataProductManager(
                settings=SimpleNamespace(),
                store=DataProductStore(Path(temp_dir) / "data-products.json"),
                create_worker_connection=lambda: None,
                relation_fields_provider=lambda requested: (
                    fields if requested == relation else []
                ),
                catalog_provider=lambda: [],
                s3_bucket_snapshot_provider=lambda **_kwargs: {},
                s3_relation_source_resolver=lambda source: {
                    **source,
                    "relation": relation,
                },
            )
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=RecordingClient(),
                daca_ui_url="https://catalog.example.test",
                public_base_url="https://daaif.example.test",
            )

            created = coordinator.create_managed_product(
                source={
                    "sourceKind": "relation",
                    "sourceId": "s3",
                    "bucket": "data-analysts-journey",
                    "key": "products/tax-product.parquet",
                    "sourceDisplayName": "Tax product",
                    "sourcePlatform": "s3",
                },
                title="Tax product",
                slug=JOURNEY_SLUG,
                description="Synthetic tax metrics.",
                owner="Joel Ruod",
                domain="Tax",
                tags=["synthetic"],
                base_url="https://ignored.example.test",
            )

        schema_fields = published_payloads[0]["technicalMetadata"]["schemaFields"]
        self.assertEqual(
            [(field["name"], field["dataType"]) for field in schema_fields],
            [
                ("canton_code", "VARCHAR"),
                ("tax_year", "INTEGER"),
                ("annual_projection_chf", "DECIMAL(18,2)"),
            ],
        )
        self.assertEqual(
            published_payloads[0]["technicalMetadata"]["endpoint"],
            {
                "baseUrl": "https://daaif.example.test",
                "path": f"/api/public/data-products/{JOURNEY_SLUG}",
                "method": "GET",
            },
        )
        self.assertEqual(created["product"]["relation"], relation)
        self.assertEqual(created["product"]["bucket"], "data-analysts-journey")
        self.assertEqual(
            created["product"]["key"], "products/tax-product.parquet"
        )

    def test_resolution_failure_does_not_reserve_or_publish(self) -> None:
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
                s3_relation_source_resolver=lambda _source: (_ for _ in ()).throw(
                    ValueError("stored result is not queryable")
                ),
            )
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=RecordingClient(),
                daca_ui_url="https://catalog.example.test",
                public_base_url="https://daaif.example.test",
            )

            with self.assertRaisesRegex(ValueError, "not queryable"):
                coordinator.create_managed_product(
                    source={
                        "sourceKind": "relation",
                        "sourceId": "s3",
                        "bucket": "data-analysts-journey",
                        "key": "products/missing.parquet",
                    },
                    title="Missing product",
                    slug="missing-product",
                    owner="Joel Ruod",
                )

            self.assertEqual(store.list_products(), [])
            self.assertEqual(published_payloads, [])

    def test_resolved_parquet_relation_is_exposed_as_paginated_json_rows(self) -> None:
        relation = "data_analysts_journey.tax_product"
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "data-product.duckdb"
            connection = duckdb.connect(str(database_path))
            connection.execute("CREATE SCHEMA data_analysts_journey")
            connection.execute(
                """
                CREATE TABLE data_analysts_journey.tax_product AS
                SELECT * FROM (VALUES
                    ('AG', 2025, 125000.50::DECIMAL(18,2)),
                    ('BE', 2026, 175000.25::DECIMAL(18,2)),
                    ('ZH', 2026, 250000.75::DECIMAL(18,2))
                ) AS metrics(canton_code, tax_year, annual_projection_chf)
                """
            )
            connection.close()

            fields = [
                SourceField(name="canton_code", data_type="VARCHAR"),
                SourceField(name="tax_year", data_type="INTEGER"),
                SourceField(
                    name="annual_projection_chf",
                    data_type="DECIMAL(18,2)",
                ),
            ]
            manager = DataProductManager(
                settings=SimpleNamespace(),
                store=DataProductStore(Path(temp_dir) / "data-products.json"),
                create_worker_connection=lambda: duckdb.connect(str(database_path)),
                relation_fields_provider=lambda requested: (
                    fields if requested == relation else []
                ),
                catalog_provider=lambda: [],
                s3_bucket_snapshot_provider=lambda **_kwargs: {},
                s3_relation_source_resolver=lambda source: {
                    **source,
                    "relation": relation,
                },
            )
            manager.create_product(
                source={
                    "sourceKind": "relation",
                    "sourceId": "s3",
                    "bucket": "data-analysts-journey",
                    "key": "products/tax-product.parquet",
                },
                title="Tax product",
                slug="tax-product",
            )

            first_page = manager.public_relation_payload(
                slug="tax-product",
                limit=2,
                offset=0,
            )
            second_page = manager.public_relation_payload(
                slug="tax-product",
                limit=2,
                offset=2,
            )

        self.assertEqual(
            [column["name"] for column in first_page["columns"]],
            ["canton_code", "tax_year", "annual_projection_chf"],
        )
        self.assertEqual(len(first_page["items"]), 2)
        self.assertTrue(first_page["hasMore"])
        self.assertEqual(second_page["items"][0]["canton_code"], "ZH")
        self.assertFalse(second_page["hasMore"])


if __name__ == "__main__":
    unittest.main()

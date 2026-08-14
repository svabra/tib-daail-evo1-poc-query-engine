from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.data_products.registry import (  # noqa: E402
    DataProductOverwriteConflict,
    DataProductStore,
)
from bit_data_workbench.models import (  # noqa: E402
    DacaPublicationReference,
    DataProductDefinition,
    DataProductSourceDescriptor,
)


def build_product(
    *,
    product_id: str = "product-1",
    slug: str = "published-orders",
    title: str = "Published Orders",
) -> DataProductDefinition:
    return DataProductDefinition(
        product_id=product_id,
        slug=slug,
        title=title,
        description="Orders exposed as a managed data product.",
        source=DataProductSourceDescriptor(
            source_kind="relation",
            source_id="pg_oltp",
            relation="pg_oltp.finance.orders",
            source_display_name="finance.orders",
            source_platform="postgres",
        ),
        public_path=f"/api/public/data-products/{slug}",
        owner="Finance",
        domain="Billing",
        tags=["orders", "published"],
        access_level="internal",
        access_note="Internal analytics use only.",
        request_access_contact="data@example.test",
        custom_properties={"externalId": "urn:li:dataset:(urn:li:dataPlatform:postgres,finance.orders,PROD)"},
        created_at="2026-04-20T08:00:00+00:00",
        updated_at="2026-04-20T08:00:00+00:00",
    )


class DataProductStoreTests(unittest.TestCase):
    def test_create_update_delete_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = DataProductStore(Path(temp_dir) / "data-products.json")
            created = store.create_product(build_product())

            listed = store.list_products()
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0].product_id, created.product_id)
            self.assertEqual(listed[0].slug, "published-orders")

            updated = build_product(title="Published Orders v2")
            updated.updated_at = "2026-04-20T09:00:00+00:00"
            store.update_product(updated)

            after_update = store.list_products()
            self.assertEqual(after_update[0].title, "Published Orders v2")
            self.assertEqual(after_update[0].updated_at, "2026-04-20T09:00:00+00:00")

            removed = store.delete_product(created.product_id)
            self.assertEqual(removed.slug, "published-orders")
            self.assertEqual(store.list_products(), [])

    def test_create_rejects_duplicate_slug(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = DataProductStore(Path(temp_dir) / "data-products.json")
            store.create_product(build_product())

            with self.assertRaises(ValueError) as context:
                store.create_product(
                    build_product(product_id="product-2", title="Another title")
                )

        self.assertIn("already in use", str(context.exception))

    def test_daca_publication_linkage_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = DataProductStore(Path(temp_dir) / "data-products.json")
            product = build_product()
            product.daca_publication = DacaPublicationReference(
                source_product_id="daaif:published-orders:v1",
                publication_id="70fcab5b-233f-4191-bf6e-99ea63de1bd9",
                product_id="8431e889-8d70-49e4-b790-7ef9fe8bb1df",
                state="pending_review",
                created=True,
                task_ids=["103b6fb6-614a-42fd-a10c-55fef4c0b520"],
                missing_fields=["dcatReviewed"],
                catalog_url="http://localhost:8080/products/8431e889/quality",
                synced_at="2026-08-13T12:00:00+00:00",
            )
            store.create_product(product)

            loaded = store.list_products()[0]

        self.assertIsNotNone(loaded.daca_publication)
        self.assertEqual(
            loaded.daca_publication.product_id,
            "8431e889-8d70-49e4-b790-7ef9fe8bb1df",
        )
        self.assertTrue(loaded.payload()["dacaManaged"])

    def test_replace_is_atomic_and_stale_compare_token_leaves_product_unchanged(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = Path(temp_dir) / "data-products.json"
            store = DataProductStore(store_path)
            original = store.create_product(build_product())
            replacement = replace(
                original,
                title="Published Orders from Curated Finance",
                description="Replacement backed by the curated relation.",
                source=DataProductSourceDescriptor(
                    source_kind="relation",
                    source_id="pg_olap",
                    relation="pg_olap.finance.curated_orders",
                    source_display_name="finance.curated_orders",
                    source_platform="postgres",
                ),
                owner="Curated Finance",
                tags=["orders", "curated"],
                custom_properties={"qualityTier": "gold"},
                updated_at="2026-04-20T09:00:00+00:00",
            )

            replaced = store.replace_product(
                replacement,
                expected_product_id=original.product_id,
                expected_updated_at=original.updated_at,
            )

            self.assertEqual(replaced.product_id, original.product_id)
            self.assertEqual(replaced.created_at, original.created_at)
            self.assertEqual(len(store.list_products()), 1)
            self.assertEqual(
                store.list_products()[0].source.relation,
                "pg_olap.finance.curated_orders",
            )
            state_after_success = store_path.read_bytes()

            stale_replacement = replace(
                replacement,
                title="This stale write must not win",
                updated_at="2026-04-20T10:00:00+00:00",
            )
            with self.assertRaises(DataProductOverwriteConflict):
                store.replace_product(
                    stale_replacement,
                    expected_product_id=original.product_id,
                    expected_updated_at=original.updated_at,
                )

            self.assertEqual(store_path.read_bytes(), state_after_success)
            current = store.list_products()[0]
            self.assertEqual(current.title, replacement.title)
            self.assertEqual(current.updated_at, replacement.updated_at)


if __name__ == "__main__":
    unittest.main()

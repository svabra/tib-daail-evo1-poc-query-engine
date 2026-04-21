from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.data_products.registry import DataProductStore  # noqa: E402
from bit_data_workbench.models import (  # noqa: E402
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


if __name__ == "__main__":
    unittest.main()

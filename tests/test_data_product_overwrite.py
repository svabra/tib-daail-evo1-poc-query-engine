from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.data_products.manager import (  # noqa: E402
    DataProductManager,
)
from bit_data_workbench.backend.data_products.registry import (  # noqa: E402
    DataProductOverwriteConflict,
    DataProductStore,
)
from bit_data_workbench.models import (  # noqa: E402
    DacaPublicationReference,
    DataProductDefinition,
    DataProductSourceDescriptor,
    SourceField,
)


ORIGINAL_UPDATED_AT = "2026-08-13T08:00:00+00:00"


def relation_source(relation: str) -> dict[str, object]:
    source_id = "pg_olap" if relation.startswith("pg_olap.") else "pg_oltp"
    return {
        "sourceKind": "relation",
        "sourceId": source_id,
        "relation": relation,
        "sourceDisplayName": relation.removeprefix(f"{source_id}."),
        "sourcePlatform": "postgres",
    }


def build_manager(path: Path) -> tuple[DataProductManager, DataProductStore]:
    store = DataProductStore(path)
    manager = DataProductManager(
        settings=SimpleNamespace(),
        store=store,
        create_worker_connection=lambda: None,
        relation_fields_provider=lambda _relation: [
            SourceField(name="record_id", data_type="INTEGER"),
            SourceField(name="amount_chf", data_type="DECIMAL(18,2)"),
        ],
        catalog_provider=lambda: [],
        s3_bucket_snapshot_provider=lambda **_kwargs: {},
    )
    return manager, store


def seed_product(
    store: DataProductStore,
    *,
    managed: bool = False,
) -> DataProductDefinition:
    product = DataProductDefinition(
        product_id="data-product-stable",
        slug="published-orders",
        title="Published Orders",
        description="Original description.",
        source=DataProductSourceDescriptor(
            source_kind="relation",
            source_id="pg_oltp",
            relation="pg_oltp.finance.orders",
            source_display_name="finance.orders",
            source_platform="postgres",
        ),
        public_path="/api/public/data-products/published-orders",
        owner="Original Owner",
        domain="Billing",
        tags=["orders"],
        access_level="internal",
        access_note="Original note.",
        request_access_contact="original@example.test",
        custom_properties={"qualityTier": "silver"},
        daca_publication=(
            DacaPublicationReference(
                source_product_id="daaif:published-orders:v1",
                publication_id="70fcab5b-233f-4191-bf6e-99ea63de1bd9",
                product_id="8431e889-8d70-49e4-b790-7ef9fe8bb1df",
                state="pending_review",
                created=True,
            )
            if managed
            else None
        ),
        created_at="2026-08-12T08:00:00+00:00",
        updated_at=ORIGINAL_UPDATED_AT,
    )
    return store.create_product(product)


class DataProductManagerOverwriteTests(unittest.TestCase):
    def test_same_source_replace_reuses_the_validated_descriptor_without_reprobing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = DataProductStore(Path(temp_dir) / "data-products.json")
            existing = seed_product(store)
            manager = DataProductManager(
                settings=SimpleNamespace(),
                store=store,
                create_worker_connection=lambda: None,
                relation_fields_provider=lambda _relation: (_ for _ in ()).throw(
                    AssertionError("same-source overwrite must not reprobe the relation")
                ),
                catalog_provider=lambda: [],
                s3_bucket_snapshot_provider=lambda **_kwargs: {},
            )

            preview = manager.preview_product(
                source=relation_source(existing.source.relation),
                title=existing.title,
                slug=existing.slug,
            )
            result = manager.create_product(
                source=relation_source(existing.source.relation),
                title=existing.title,
                slug=existing.slug,
                description=existing.description,
                owner=existing.owner,
                domain=existing.domain,
                tags=existing.tags,
                access_level=existing.access_level,
                access_note=existing.access_note,
                request_access_contact=existing.request_access_contact,
                custom_properties=existing.custom_properties,
                overwrite_existing=True,
                expected_product_id=existing.product_id,
                expected_updated_at=existing.updated_at,
            )

        self.assertTrue(preview["canOverwrite"])
        self.assertEqual(result["action"], "replaced")
        self.assertEqual(result["product"]["productId"], existing.product_id)

    def test_duplicate_preview_exposes_explicit_replace_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager, store = build_manager(Path(temp_dir) / "data-products.json")
            existing = seed_product(store)

            preview = manager.preview_product(
                source=relation_source("pg_olap.finance.curated_orders"),
                title="Curated Published Orders",
                slug=existing.slug,
                owner="Curated Finance",
                base_url="http://testserver",
            )

        self.assertFalse(preview["compatible"])
        self.assertTrue(preview["blocked"])
        self.assertIn("already in use", preview["blockedReason"])
        self.assertEqual(preview["operation"], "replace")
        self.assertTrue(preview["overwriteRequired"])
        self.assertTrue(preview["canOverwrite"])
        self.assertEqual(preview["existingProduct"]["productId"], existing.product_id)
        self.assertEqual(preview["existingProduct"]["updatedAt"], existing.updated_at)
        self.assertEqual(
            preview["product"]["relation"],
            "pg_olap.finance.curated_orders",
        )

    def test_confirmed_replace_preserves_identity_and_replaces_source_and_metadata(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager, store = build_manager(Path(temp_dir) / "data-products.json")
            existing = seed_product(store)

            result = manager.create_product(
                source=relation_source("pg_olap.finance.curated_orders"),
                title="Curated Published Orders",
                slug=existing.slug,
                description="Replacement description.",
                owner="Curated Finance",
                domain="Financial Reporting",
                tags=["orders", "gold"],
                access_level="restricted",
                access_note="Approved analysts only.",
                request_access_contact="curated@example.test",
                custom_properties={"qualityTier": "gold"},
                overwrite_existing=True,
                expected_product_id=existing.product_id,
                expected_updated_at=existing.updated_at,
                base_url="http://testserver",
            )

            persisted = store.list_products()

        self.assertEqual(result["action"], "replaced")
        self.assertEqual(len(persisted), 1)
        replacement = persisted[0]
        self.assertEqual(replacement.product_id, existing.product_id)
        self.assertEqual(replacement.created_at, existing.created_at)
        self.assertEqual(replacement.public_path, existing.public_path)
        self.assertEqual(replacement.source.source_id, "pg_olap")
        self.assertEqual(
            replacement.source.relation,
            "pg_olap.finance.curated_orders",
        )
        self.assertEqual(replacement.title, "Curated Published Orders")
        self.assertEqual(replacement.owner, "Curated Finance")
        self.assertEqual(replacement.domain, "Financial Reporting")
        self.assertEqual(replacement.tags, ["orders", "gold"])
        self.assertEqual(replacement.access_level, "restricted")
        self.assertEqual(replacement.custom_properties, {"qualityTier": "gold"})
        self.assertEqual(result["product"]["productId"], existing.product_id)

    def test_stale_or_mismatched_preview_tokens_raise_conflict_without_mutation(
        self,
    ) -> None:
        cases = (
            ("different-product", ORIGINAL_UPDATED_AT),
            ("data-product-stable", "2026-08-13T07:59:59+00:00"),
        )
        for expected_product_id, expected_updated_at in cases:
            with self.subTest(
                expected_product_id=expected_product_id,
                expected_updated_at=expected_updated_at,
            ), tempfile.TemporaryDirectory() as temp_dir:
                store_path = Path(temp_dir) / "data-products.json"
                manager, store = build_manager(store_path)
                existing = seed_product(store)
                state_before = store_path.read_bytes()

                with self.assertRaises(DataProductOverwriteConflict):
                    manager.create_product(
                        source=relation_source("pg_olap.finance.curated_orders"),
                        title="Stale replacement",
                        slug=existing.slug,
                        overwrite_existing=True,
                        expected_product_id=expected_product_id,
                        expected_updated_at=expected_updated_at,
                    )

                self.assertEqual(store_path.read_bytes(), state_before)
                self.assertEqual(store.list_products()[0], existing)

    def test_daca_managed_product_cannot_be_overwritten_locally(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = Path(temp_dir) / "data-products.json"
            manager, store = build_manager(store_path)
            existing = seed_product(store, managed=True)
            state_before = store_path.read_bytes()

            with self.assertRaises(DataProductOverwriteConflict) as context:
                manager.create_product(
                    source=relation_source("pg_olap.finance.curated_orders"),
                    title="Local replacement",
                    slug=existing.slug,
                    overwrite_existing=True,
                    expected_product_id=existing.product_id,
                    expected_updated_at=existing.updated_at,
                )

            self.assertIn("Publish to DaCa enabled", str(context.exception))
            self.assertEqual(store_path.read_bytes(), state_before)
            self.assertEqual(store.list_products()[0], existing)


if __name__ == "__main__":
    unittest.main()

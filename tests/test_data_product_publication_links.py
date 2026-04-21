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


from bit_data_workbench.backend.data_products.manager import DataProductManager  # noqa: E402
from bit_data_workbench.backend.data_products.registry import DataProductStore  # noqa: E402
from bit_data_workbench.backend.data_sources.publication_links import (  # noqa: E402
    annotate_catalogs_with_published_products,
)
from bit_data_workbench.models import (  # noqa: E402
    DataProductDefinition,
    DataProductSourceDescriptor,
    SourceCatalog,
    SourceObject,
    SourceSchema,
)


def build_manager(path: Path) -> DataProductManager:
    return DataProductManager(
        settings=SimpleNamespace(),
        store=DataProductStore(path),
        create_worker_connection=lambda: None,
        relation_fields_provider=lambda relation: [],
        catalog_provider=lambda: [],
        s3_bucket_snapshot_provider=lambda **kwargs: {},
    )


class DataProductPublicationLinkTests(unittest.TestCase):
    def test_manager_matches_relation_bucket_and_object_products(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = build_manager(Path(temp_dir) / "data-products.json")

            manager._store.create_product(
                DataProductDefinition(
                    product_id="relation-product",
                    slug="orders-product",
                    title="Orders Product",
                    description="",
                    source=DataProductSourceDescriptor(
                        source_kind="relation",
                        source_id="pg_oltp",
                        relation="pg_oltp.public.orders",
                    ),
                    public_path="/api/public/data-products/orders-product",
                )
            )
            manager._store.create_product(
                DataProductDefinition(
                    product_id="bucket-product",
                    slug="shared-bucket-product",
                    title="Shared Bucket Product",
                    description="",
                    source=DataProductSourceDescriptor(
                        source_kind="bucket",
                        source_id="workspace.s3",
                        bucket="shared-finance",
                    ),
                    public_path="/api/public/data-products/shared-bucket-product",
                )
            )
            manager._store.create_product(
                DataProductDefinition(
                    product_id="object-product",
                    slug="orders-csv-product",
                    title="Orders CSV Product",
                    description="",
                    source=DataProductSourceDescriptor(
                        source_kind="object",
                        source_id="workspace.s3",
                        bucket="shared-finance",
                        key="exports/orders.csv",
                    ),
                    public_path="/api/public/data-products/orders-csv-product",
                )
            )

            relation_links = manager.published_products_for_source(
                source={
                    "sourceKind": "relation",
                    "sourceId": "pg_oltp",
                    "relation": "pg_oltp.public.orders",
                }
            )
            bucket_links = manager.published_products_for_source(
                source={
                    "sourceKind": "bucket",
                    "sourceId": "workspace.s3",
                    "bucket": "shared-finance",
                }
            )
            object_links = manager.published_products_for_source(
                source={
                    "sourceKind": "object",
                    "sourceId": "workspace.s3",
                    "bucket": "shared-finance",
                    "key": "exports/orders.csv",
                }
            )

            self.assertEqual([item["slug"] for item in relation_links], ["orders-product"])
            self.assertEqual([item["slug"] for item in bucket_links], ["shared-bucket-product"])
            self.assertEqual([item["slug"] for item in object_links], ["orders-csv-product"])

    def test_catalog_annotation_marks_published_sidebar_objects(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = build_manager(Path(temp_dir) / "data-products.json")
            manager._store.create_product(
                DataProductDefinition(
                    product_id="relation-product",
                    slug="orders-product",
                    title="Orders Product",
                    description="",
                    source=DataProductSourceDescriptor(
                        source_kind="relation",
                        source_id="pg_oltp",
                        relation="pg_oltp.public.orders",
                    ),
                    public_path="/api/public/data-products/orders-product",
                )
            )
            manager._store.create_product(
                DataProductDefinition(
                    product_id="bucket-product",
                    slug="shared-bucket-product",
                    title="Shared Bucket Product",
                    description="",
                    source=DataProductSourceDescriptor(
                        source_kind="bucket",
                        source_id="workspace.s3",
                        bucket="shared-finance",
                    ),
                    public_path="/api/public/data-products/shared-bucket-product",
                )
            )

            catalogs = [
                SourceCatalog(
                    name="workspace",
                    connection_source_id="workspace.s3",
                    schemas=[
                        SourceSchema(
                            name="shared-finance",
                            label="shared-finance",
                            objects=[
                                SourceObject(
                                    name="orders.csv",
                                    kind="file",
                                    relation="workspace.shared_finance.orders",
                                    s3_bucket="shared-finance",
                                    s3_key="exports/orders.csv",
                                    s3_path="s3://shared-finance/exports/orders.csv",
                                    s3_file_format="csv",
                                    s3_downloadable=True,
                                )
                            ],
                        )
                    ],
                ),
                SourceCatalog(
                    name="pg_oltp",
                    connection_source_id="pg_oltp",
                    schemas=[
                        SourceSchema(
                            name="public",
                            objects=[
                                SourceObject(
                                    name="orders",
                                    kind="table",
                                    relation="pg_oltp.public.orders",
                                )
                            ],
                        )
                    ],
                ),
            ]

            annotated = annotate_catalogs_with_published_products(
                catalogs,
                publication_links_for_source=lambda source: manager.published_products_for_source(
                    source=source
                ),
            )

            self.assertEqual(
                annotated[0].schemas[0].published_data_products[0]["slug"],
                "shared-bucket-product",
            )
            self.assertEqual(
                annotated[1].schemas[0].objects[0].published_data_products[0]["slug"],
                "orders-product",
            )


if __name__ == "__main__":
    unittest.main()

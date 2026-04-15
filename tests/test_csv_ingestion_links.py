from __future__ import annotations

from pathlib import Path
import sys
from unittest import TestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.ingestion_types.csv.query_links import (  # noqa: E402
    attach_query_sources_to_csv_imports,
)
from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema  # noqa: E402


class CsvIngestionLinkTests(TestCase):
    def test_attach_query_sources_to_postgres_imports(self) -> None:
        catalogs = [
            SourceCatalog(
                name="pg_oltp",
                connection_source_id="pg_oltp",
                schemas=[
                    SourceSchema(
                        name="public",
                        objects=[
                            SourceObject(
                                name="raw_vat_smoke",
                                kind="table",
                                relation="pg_oltp.public.raw_vat_smoke",
                            )
                        ],
                    )
                ],
            )
        ]

        payload = attach_query_sources_to_csv_imports(
            {
                "targetId": "pg_oltp",
                "imports": [
                    {
                        "fileName": "vat-smoke.csv",
                        "status": "imported",
                        "relation": "public.raw_vat_smoke",
                        "rowCount": 3,
                    }
                ],
            },
            catalogs,
        )

        self.assertEqual(payload["firstQuerySource"]["sourceId"], "pg_oltp")
        self.assertEqual(
            payload["imports"][0]["querySource"]["relation"],
            "pg_oltp.public.raw_vat_smoke",
        )
        self.assertEqual(
            payload["imports"][0]["querySource"]["name"],
            "raw_vat_smoke",
        )

    def test_attach_query_sources_to_s3_imports_by_object_path(self) -> None:
        catalogs = [
            SourceCatalog(
                name="workspace",
                connection_source_id="workspace.s3",
                schemas=[
                    SourceSchema(
                        name="vat_smoke_test_bucket",
                        label="vat-smoke-test-bucket",
                        objects=[
                            SourceObject(
                                name="vat_smoke",
                                kind="view",
                                relation="vat_smoke_test_bucket.vat_smoke",
                                s3_bucket="vat-smoke-test-bucket",
                                s3_key="incoming/vat_smoke.csv",
                                s3_path="s3://vat-smoke-test-bucket/incoming/vat_smoke.csv",
                                s3_file_format="csv",
                                s3_downloadable=True,
                            )
                        ],
                    )
                ],
            )
        ]

        payload = attach_query_sources_to_csv_imports(
            {
                "targetId": "workspace.s3",
                "imports": [
                    {
                        "fileName": "vat_smoke.csv",
                        "status": "imported",
                        "path": "s3://vat-smoke-test-bucket/incoming/vat_smoke.csv",
                    }
                ],
            },
            catalogs,
        )

        self.assertEqual(payload["firstQuerySource"]["sourceId"], "workspace.s3")
        self.assertEqual(
            payload["imports"][0]["querySource"]["relation"],
            "vat_smoke_test_bucket.vat_smoke",
        )
        self.assertEqual(
            payload["imports"][0]["querySource"]["schemaLabel"],
            "vat-smoke-test-bucket",
        )

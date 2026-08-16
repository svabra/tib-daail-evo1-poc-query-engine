from __future__ import annotations

from json import loads
from pathlib import Path
import sys
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.workbench_metadata import catalog_search_index
from bit_data_workbench.backend.workbench_search import workbench_catalog_search_items
from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema


def _catalogs() -> list[SourceCatalog]:
    return [
        SourceCatalog(
            name="workspace",
            connection_source_id="s3",
            connection_status="connected",
            schemas=[
                SourceSchema(
                    name="finance",
                    objects=[
                        SourceObject(
                            name="orders.parquet",
                            kind="file",
                            relation="workspace.finance.orders",
                            query_reference='s3."finance"."orders.parquet"',
                            s3_bucket="finance",
                            s3_key="orders.parquet",
                            s3_path="s3://finance/orders.parquet",
                            s3_file_format="parquet",
                        )
                    ],
                )
            ],
        ),
        SourceCatalog(
            name="pg_oltp",
            connection_source_id="pg_oltp",
            connection_status="connected",
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
        SourceCatalog(
            name="pg_olap",
            connection_source_id="pg_olap",
            connection_status="connected",
            schemas=[
                SourceSchema(
                    name="mart",
                    objects=[
                        SourceObject(
                            name="orders_summary",
                            kind="view",
                            relation="pg_olap.mart.orders_summary",
                        )
                    ],
                )
            ],
        ),
        SourceCatalog(
            name="workspace_local",
            connection_source_id="workspace.local",
            connection_status="connected",
        ),
    ]


def test_catalog_search_separates_sources_from_objects_without_native_duplicates() -> None:
    items = workbench_catalog_search_items(_catalogs())
    sources = [item for item in items if item["kind"] == "source"]
    objects = [item for item in items if item["kind"] == "object"]

    assert {item["sourceId"] for item in sources} == {
        "workspace.local",
        "s3",
        "pg_oltp",
        "pg_oltp_native",
        "pg_olap",
    }
    assert {item["sourceId"] for item in objects} == {"s3", "pg_oltp", "pg_olap"}
    assert len(objects) == 3
    assert len({item["id"] for item in objects}) == len(objects)
    assert all(item["kindLabel"] == "Data Source" for item in sources)
    assert all(item["kindLabel"] == "Datenobjekt" for item in objects)
    assert next(item for item in objects if item["sourceId"] == "s3")["path"] == (
        "s3://finance/orders.parquet"
    )
    assert next(item for item in objects if item["sourceId"] == "pg_oltp")[
        "targetUrl"
    ] == "/data-sources/browser?source_id=pg_oltp"


def test_catalog_search_index_is_lightweight_and_etagged() -> None:
    service = SimpleNamespace(catalogs=_catalogs)
    response = catalog_search_index(service=service, if_none_match=None)
    payload = loads(response.body)

    assert payload["items"] == workbench_catalog_search_items(_catalogs())
    assert all("objects" not in item and "schemas" not in item for item in payload["items"])
    replay = catalog_search_index(
        service=service,
        if_none_match=response.headers["etag"],
    )
    assert replay.status_code == 304

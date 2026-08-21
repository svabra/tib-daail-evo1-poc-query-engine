from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend.data_source_catalog import (  # noqa: E402
    build_data_source_records,
    canonical_data_source_id,
    data_source_catalog_payload,
)
from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema  # noqa: E402


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        s3_endpoint="http://127.0.0.1:9000",
        s3_bucket="visible-bucket",
        pg_host="127.0.0.1",
        pg_oltp_database="oltp",
        pg_olap_database="olap",
    )


def catalog(
    name: str,
    source_id: str,
    *,
    objects: list[SourceObject],
    technology: str = "",
) -> SourceCatalog:
    return SourceCatalog(
        name=name,
        connection_source_id=source_id,
        display_name=source_id,
        source_platform=technology,
        connection_status="connected",
        connection_label="Connected",
        schemas=[SourceSchema(name="public", objects=objects)],
    )


def discovered_catalogs() -> list[SourceCatalog]:
    relation = lambda name: SourceObject(  # noqa: E731 - concise fixture factory
        name=name,
        kind="table",
        relation=f"public.{name}",
        query_reference=f"pg.pg_oltp.public.{name}",
    )
    return [
        catalog(
            "workspace",
            "s3",
            objects=[
                SourceObject(
                    name="orders",
                    kind="view",
                    relation="workspace.orders",
                    s3_bucket="visible-bucket",
                    s3_key="input/orders.csv",
                    s3_path="s3://visible-bucket/input/orders.csv",
                    s3_file_format="csv",
                    query_reference='s3."visible-bucket"."input/orders.csv"',
                )
            ],
        ),
        catalog("pg_oltp", "pg_oltp", objects=[relation("customers"), relation("orders")]),
        catalog("pg_olap", "pg_olap", objects=[relation("facts")]),
        # The compatibility alias may still be present in a runtime catalog. It
        # must never become a second logical data source in the public catalog.
        catalog("pg_oltp_native", "pg_oltp_native", objects=[relation("customers")]),
        SourceCatalog(
            name="ora_bazg_zoll",
            connection_source_id="ora_bazg_zoll",
            display_name="BAZG Zentrale Zollabwicklung",
            database_name="BZGZOLL1",
            source_platform="BIT Oracle RDBMS",
            site_label="PRIMUS + CAMPUS",
            owner_label="Sandro Wenger",
            schemas=[
                SourceSchema(
                    name="ZOLL",
                    objects=[SourceObject("ANMELDUNGEN", "table", "ora_bazg_zoll.ZOLL.ANMELDUNGEN")],
                )
            ],
        ),
    ]


def test_catalog_normalizes_platform_sources_and_hides_native_alias() -> None:
    records = build_data_source_records(settings(), discovered_catalogs())
    ids = [item["id"] for item in records]
    assert ids == ["workspace.local", "s3", "pg_oltp", "pg_olap", "ora_bazg_zoll"]
    assert canonical_data_source_id("pg_oltp_native") == "pg_oltp"
    assert next(item for item in records if item["id"] == "s3")["ingestionCapable"] is True
    assert next(item for item in records if item["id"] == "pg_oltp")["accessPaths"] == ["VMTP", "Native"]
    assert next(item for item in records if item["id"] == "workspace.local")["ingestionCapable"] is False


def test_catalog_search_filters_facets_and_server_side_pagination() -> None:
    payload = data_source_catalog_payload(
        settings(),
        discovered_catalogs(),
        technology="postgresql",
        status="available",
        offset=1,
        limit=1,
    )
    assert payload["summary"]["total"] == 5
    assert payload["summary"]["matched"] == 2
    assert payload["pagination"] == {
        "offset": 1,
        "limit": 1,
        "total": 2,
        "hasPrevious": True,
        "hasNext": False,
    }
    assert [item["id"] for item in payload["items"]] == ["pg_olap"]
    assert payload["facets"]["technologies"]["PostgreSQL"] == 2

    searched = data_source_catalog_payload(
        settings(), discovered_catalogs(), query="BZGZOLL1"
    )
    assert searched["summary"]["matched"] == 1
    assert searched["items"][0]["id"] == "ora_bazg_zoll"


def test_catalog_can_limit_results_to_ingestion_capable_sources() -> None:
    payload = data_source_catalog_payload(
        settings(), discovered_catalogs(), ingestion_capable=True
    )
    assert {item["id"] for item in payload["items"]} == {
        "s3",
        "pg_oltp",
        "pg_olap",
        "ora_bazg_zoll",
    }

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import sys
import threading

import duckdb
import httpx
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend.query_aliases import rewrite_query_aliases  # noqa: E402
from bit_data_workbench.backend.query_analysis import build_relation_index  # noqa: E402
from bit_data_workbench.backend.query_jobs import infer_source_types  # noqa: E402
from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402
from bit_data_workbench.backend.source_sourcing import (  # noqa: E402
    SourceSourcingCoordinator,
    SourceSourcingError,
)


SOURCE = {
    "id": "ora_bazg_zoll",
    "databaseName": "BZGZOLL1",
    "displayName": "BAZG Zentrale Zollabwicklung",
    "sites": ["PRIMUS", "CAMPUS"],
}


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        daca_base_url="http://daca.test",
        daca_http_timeout_seconds=1.0,
    )


def test_catalog_proxy_preserves_actor_filters_and_30_of_38_summary() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "summary": {"total": 38, "discoverable": 30, "hidden": 8, "matched": 1},
                "offset": 12,
                "limit": 12,
                "items": [SOURCE],
            },
        )

    coordinator = SourceSourcingCoordinator(settings(), transport=httpx.MockTransport(handler))
    payload = coordinator.catalog("joel.ruod", query="zoll", site="BOTH", offset=12, limit=12)

    assert payload["summary"] == {"total": 38, "discoverable": 30, "hidden": 8, "matched": 1}
    assert seen[0].headers["X-DaCa-User"] == "joel.ruod"
    assert seen[0].url.params["q"] == "zoll"
    assert seen[0].url.params["site"] == "both"


def test_catalog_proxy_omits_empty_optional_filters() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"summary": {}, "items": []})

    SourceSourcingCoordinator(settings(), transport=httpx.MockTransport(handler)).catalog("joel.ruod")
    assert "q" not in seen[0].url.params
    assert "site" not in seen[0].url.params


def test_active_group_grant_builds_three_queryable_typed_oracle_relations() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/source-access-grants/mine"
        return httpx.Response(
            200,
            json=[
                {
                    "state": "active",
                    "source": SOURCE,
                    "subject": {"type": "group", "id": "estv-business-intelligence"},
                    "validFrom": "2026-08-20",
                    "validUntil": None,
                }
            ],
        )

    coordinator = SourceSourcingCoordinator(settings(), transport=httpx.MockTransport(handler))
    catalogs = coordinator.catalogs_for_actor("joel.ruod")
    assert len(catalogs) == 1
    assert [item.name for item in catalogs[0].schemas[0].objects] == [
        "ANMELDUNGEN",
        "WARENPOSITIONEN",
        "ABGABEN_UEBERSICHT_V",
    ]

    relation_index = build_relation_index(catalogs)
    alias_map = {key: value.query_sql for key, value in relation_index.items() if value.query_sql}
    sql = rewrite_query_aliases(
        "SELECT a.MRN, w.WARENBESCHREIBUNG, w.WERT_CHF "
        "FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN a "
        "JOIN ora_bazg_zoll.ZOLL.WARENPOSITIONEN w USING (ANMELDUNG_ID) "
        "ORDER BY w.WERT_CHF DESC",
        alias_map,
    )
    rows = duckdb.connect(":memory:").execute(sql).fetchall()
    assert rows[0] == ("26CH000002B7", "Pharmaceutical Products", pytest.approx(930000.0))
    assert len(rows) == 5

    fields = coordinator.fields_for_relation("joel.ruod", "ZOLL.WARENPOSITIONEN")
    assert [(field.name, field.data_type) for field in fields][-2:] == [
        ("GEWICHT_KG", "DECIMAL(12,2)"),
        ("WERT_CHF", "DECIMAL(14,2)"),
    ]


def test_oracle_inline_relations_are_isolated_query_sources() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/source-access-grants/mine"
        return httpx.Response(200, json=[{"state": "active", "source": SOURCE}])

    coordinator = SourceSourcingCoordinator(settings(), transport=httpx.MockTransport(handler))
    catalogs = coordinator.catalogs_for_actor("joel.ruod")
    relation_index = build_relation_index(catalogs)
    service = WorkbenchService.__new__(WorkbenchService)
    service._catalogs = catalogs
    service._lock = threading.RLock()
    service._data_source_discovery = None

    sql = (
        "SELECT a.MRN, w.WARENBESCHREIBUNG "
        "FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN a "
        "JOIN ora_bazg_zoll.ZOLL.WARENPOSITIONEN w USING (ANMELDUNG_ID)"
    )
    touches = service._analyze_query(sql, relation_index=relation_index)
    summaries = service._query_source_summaries(
        touches.touched_relations,
        relation_index=relation_index,
    )

    assert touches.touched_relations == ["ZOLL.ANMELDUNGEN", "ZOLL.WARENPOSITIONEN"]
    assert [summary["relation"] for summary in summaries] == [
        "ZOLL.ANMELDUNGEN",
        "ZOLL.WARENPOSITIONEN",
    ]
    assert all(str(summary["query_sql"]).startswith("(SELECT") for summary in summaries)
    assert service._unresolved_isolated_read_relations(
        touched_relations=touches.touched_relations,
        source_summaries=summaries,
    ) == []


def test_generic_granted_oracle_source_uses_catalog_metadata_and_queryable_mock_objects() -> None:
    generic_source = {
        "id": "ora_estv_02",
        "databaseName": "ESTVORA02",
        "displayName": "ESTV Fachanwendung 02",
        "organization": "Eidgenössische Steuerverwaltung ESTV",
        "ownerName": "Sandro Wenger",
        "sites": ["CAMPUS"],
        "objects": [
            {"schema": "ESTV", "name": "STAMMDATEN", "kind": "table"},
            {"schema": "ESTV", "name": "BEWEGUNGSDATEN", "kind": "table"},
            {"schema": "ESTV", "name": "AKTUELLE_UEBERSICHT_V", "kind": "view"},
        ],
        "mockProfile": {"profile": "generic", "schema": "ESTV"},
    }
    coordinator = SourceSourcingCoordinator(
        settings(),
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=[{"state": "active", "source": generic_source}])
        ),
    )

    catalog = coordinator.catalogs_for_actor("joel.ruod")[0]
    assert catalog.display_name == "ESTV Fachanwendung 02"
    assert catalog.database_name == "ESTVORA02"
    assert catalog.site_label == "CAMPUS"
    assert [item.name for item in catalog.schemas[0].objects] == [
        "STAMMDATEN",
        "BEWEGUNGSDATEN",
        "AKTUELLE_UEBERSICHT_V",
    ]
    rows = duckdb.connect(":memory:").execute(catalog.schemas[0].objects[1].query_sql).fetchall()
    assert len(rows) == 3
    fields = coordinator.fields_for_relation("joel.ruod", "ora_estv_02.ESTV.BEWEGUNGSDATEN")
    assert fields[-1].data_type == "DECIMAL(14,2)"


def test_active_grant_does_not_authorize_a_different_oracle_source() -> None:
    coordinator = SourceSourcingCoordinator(
        settings(),
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=[{"state": "active", "source": SOURCE}])
        ),
    )
    with pytest.raises(SourceSourcingError, match="active DaCa source grant"):
        coordinator.assert_query_authorized(
            "joel.ruod",
            "SELECT * FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN a "
            "JOIN ora_estv_02.ESTV.STAMMDATEN b ON 1 = 1",
        )


def test_multiple_active_subject_grants_expose_an_oracle_source_only_once() -> None:
    coordinator = SourceSourcingCoordinator(
        settings(),
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json=[
                    {"state": "active", "source": SOURCE, "subject": {"type": "person"}},
                    {"state": "active", "source": SOURCE, "subject": {"type": "group"}},
                ],
            )
        ),
    )

    assert [source["id"] for source in coordinator.active_oracle_sources("joel.ruod")] == [
        "ora_bazg_zoll"
    ]
    assert len(coordinator.catalogs_for_actor("joel.ruod")) == 1
    assert infer_source_types(["ora_bazg_zoll"]) == ["oracle-poc"]


def test_oracle_query_is_fail_closed_without_active_grant_or_when_daca_fails() -> None:
    empty = SourceSourcingCoordinator(
        settings(), transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=[]))
    )
    with pytest.raises(SourceSourcingError, match="active DaCa source grant"):
        empty.assert_query_authorized(
            "noemie.rochat",
            "SELECT * FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN",
        )

    failed = SourceSourcingCoordinator(
        settings(),
        transport=httpx.MockTransport(
            lambda request: (_ for _ in ()).throw(httpx.ConnectError("offline", request=request))
        ),
    )
    with pytest.raises(SourceSourcingError) as exc:
        failed.assert_query_authorized(
            "joel.ruod",
            "SELECT * FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN",
        )
    assert exc.value.status_code == 503


def test_request_submission_is_idempotency_key_preserving_and_never_accepts_actor_in_body() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["actor"] = request.headers.get("X-DaCa-User")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": "request-1", "clientRequestId": "client-1"})

    coordinator = SourceSourcingCoordinator(settings(), transport=httpx.MockTransport(handler))
    payload = {"clientRequestId": "client-1", "sourceId": "ora_bazg_zoll", "actor": "sandro.wenger"}
    coordinator.create_request("joel.ruod", payload)
    assert seen["actor"] == "joel.ruod"
    assert seen["body"] == {"clientRequestId": "client-1", "sourceId": "ora_bazg_zoll"}

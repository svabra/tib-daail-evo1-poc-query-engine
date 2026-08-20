from __future__ import annotations

import re
import time
from dataclasses import dataclass
from threading import RLock
from typing import Any

import httpx

from ..config import Settings
from ..models import SourceCatalog, SourceField, SourceObject, SourceSchema


DEMO_USER_IDS = frozenset(
    {
        "joel.ruod",
        "kassandra.valdata",
        "noemie.rochat",
        "beat.stalder",
        "thomas.kriegli",
        "sandro.wenger",
    }
)
DEFAULT_DEMO_USER_ID = "joel.ruod"
ORACLE_POC_SOURCE_ID = "ora_bazg_zoll"

ORACLE_FIELDS: dict[str, tuple[tuple[str, str], ...]] = {
    "zoll.anmeldungen": (
        ("ANMELDUNG_ID", "VARCHAR"), ("MRN", "VARCHAR"), ("ANMELDUNGSDATUM", "DATE"),
        ("ZOLLSTELLE", "VARCHAR"), ("ANMELDER", "VARCHAR"), ("VERFAHREN", "VARCHAR"), ("STATUS", "VARCHAR"),
    ),
    "zoll.warenpositionen": (
        ("POSITION_ID", "VARCHAR"), ("ANMELDUNG_ID", "VARCHAR"), ("WARENNUMMER", "VARCHAR"),
        ("WARENBESCHREIBUNG", "VARCHAR"), ("URSPRUNGSLAND", "VARCHAR"), ("GEWICHT_KG", "DECIMAL(12,2)"), ("WERT_CHF", "DECIMAL(14,2)"),
    ),
    "zoll.abgaben_uebersicht_v": (
        ("ANMELDUNG_ID", "VARCHAR"), ("ZOLL_CHF", "DECIMAL(14,2)"), ("MWST_CHF", "DECIMAL(14,2)"),
        ("LENKUNGSABGABE_CHF", "DECIMAL(14,2)"), ("TOTAL_CHF", "DECIMAL(14,2)"),
    ),
}

GENERIC_ORACLE_FIELDS: dict[str, tuple[tuple[str, str], ...]] = {
    "stammdaten": (
        ("ID", "VARCHAR"),
        ("BEZEICHNUNG", "VARCHAR"),
        ("STATUS", "VARCHAR"),
        ("GUELTIG_AB", "DATE"),
    ),
    "bewegungsdaten": (
        ("VORGANG_ID", "VARCHAR"),
        ("STAMMDATEN_ID", "VARCHAR"),
        ("EREIGNISDATUM", "DATE"),
        ("BETRAG_CHF", "DECIMAL(14,2)"),
    ),
    "aktuelle_uebersicht_v": (
        ("STAMMDATEN_ID", "VARCHAR"),
        ("BEZEICHNUNG", "VARCHAR"),
        ("LETZTER_VORGANG", "DATE"),
        ("SUMME_CHF", "DECIMAL(14,2)"),
    ),
}

OracleRelationSpec = tuple[
    str,
    str,
    str,
    tuple[tuple[str, str], ...],
    tuple[tuple[object, ...], ...],
]


class SourceSourcingError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def validated_demo_actor(value: object) -> str:
    actor = str(value or "").strip()
    return actor if actor in DEMO_USER_IDS else DEFAULT_DEMO_USER_ID


def _response_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("title")
        if detail:
            return str(detail)
    return response.text.strip() or f"DaCa returned HTTP {response.status_code}."


@dataclass(frozen=True, slots=True)
class _GrantCacheEntry:
    expires_at: float
    grants: tuple[dict[str, Any], ...]


def _sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _values_relation(columns: tuple[tuple[str, str], ...], rows: tuple[tuple[object, ...], ...]) -> str:
    values = ", ".join(
        "(" + ", ".join("NULL" if value is None else _sql_string(value) for value in row) + ")"
        for row in rows
    )
    raw_columns = ", ".join(f"c{index}" for index in range(len(columns)))
    projections = ", ".join(
        f'CAST(c{index} AS {data_type}) AS "{name}"'
        for index, (name, data_type) in enumerate(columns)
    )
    return f"(SELECT {projections} FROM (VALUES {values}) AS oracle_mock({raw_columns}))"


def _source_metadata(source: dict[str, Any]) -> dict[str, str]:
    sites = [str(site).strip() for site in source.get("sites", []) if str(site).strip()]
    return {
        "display_name": str(source.get("displayName") or source.get("id") or "Oracle PoC"),
        "database_name": str(source.get("databaseName") or source.get("id") or ""),
        "source_platform": "BIT Oracle RDBMS",
        "site_label": " + ".join(sites),
        "owner_label": " · ".join(
            value
            for value in (
                str(source.get("ownerName") or "").strip(),
                str(source.get("organization") or "").strip(),
            )
            if value
        ),
    }


def _generic_oracle_specs(source: dict[str, Any]) -> tuple[OracleRelationSpec, ...]:
    source_id = str(source.get("id") or "").strip()
    profile = source.get("mockProfile") if isinstance(source.get("mockProfile"), dict) else {}
    fallback_schema = str(profile.get("schema") or "DATA").strip().upper()
    raw_objects = source.get("objects") if isinstance(source.get("objects"), list) else []
    if not raw_objects:
        raw_objects = [
            {"schema": fallback_schema, "name": "STAMMDATEN", "kind": "table"},
            {"schema": fallback_schema, "name": "BEWEGUNGSDATEN", "kind": "table"},
            {"schema": fallback_schema, "name": "AKTUELLE_UEBERSICHT_V", "kind": "view"},
        ]
    database_name = str(source.get("databaseName") or source_id).strip()
    organization = str(source.get("organization") or "BIT").strip()
    specs: list[OracleRelationSpec] = []
    for index, raw_object in enumerate(raw_objects):
        if not isinstance(raw_object, dict):
            continue
        schema = str(raw_object.get("schema") or fallback_schema).strip().upper()
        name = str(raw_object.get("name") or f"OBJEKT_{index + 1}").strip().upper()
        kind = str(raw_object.get("kind") or "table").strip().lower()
        normalized_name = name.casefold()
        fields = GENERIC_ORACLE_FIELDS.get(normalized_name)
        if fields is None:
            fields = GENERIC_ORACLE_FIELDS["aktuelle_uebersicht_v" if kind == "view" else "stammdaten"]
        if fields == GENERIC_ORACLE_FIELDS["stammdaten"]:
            rows = (
                (f"{database_name}-001", f"{organization} Referenz A", "AKTIV", "2026-01-01"),
                (f"{database_name}-002", f"{organization} Referenz B", "AKTIV", "2026-04-01"),
                (f"{database_name}-003", f"{organization} Referenz C", "IN_PRUEFUNG", "2026-07-01"),
            )
        elif fields == GENERIC_ORACLE_FIELDS["bewegungsdaten"]:
            rows = (
                (f"{database_name}-V001", f"{database_name}-001", "2026-08-18", "12500.00"),
                (f"{database_name}-V002", f"{database_name}-001", "2026-08-19", "8600.50"),
                (f"{database_name}-V003", f"{database_name}-002", "2026-08-20", "42100.00"),
            )
        else:
            rows = (
                (f"{database_name}-001", f"{organization} Referenz A", "2026-08-19", "21100.50"),
                (f"{database_name}-002", f"{organization} Referenz B", "2026-08-20", "42100.00"),
            )
        specs.append((schema, name, kind, fields, rows))
    return tuple(specs)


def _relation_specs(source: dict[str, Any]) -> tuple[OracleRelationSpec, ...]:
    if str(source.get("id") or "") == ORACLE_POC_SOURCE_ID:
        return (
            (
                "ZOLL",
                "ANMELDUNGEN",
                "table",
                ORACLE_FIELDS["zoll.anmeldungen"],
                (
                    ("A-2026-0001", "26CH000001A1", "2026-08-18", "Basel Nord", "Helvetia Logistics AG", "Import", "FREIGEGEBEN"),
                    ("A-2026-0002", "26CH000002B7", "2026-08-19", "Zürich Flughafen", "Alpine Cargo SA", "Transit", "IN_PRUEFUNG"),
                    ("A-2026-0003", "26CH000003C4", "2026-08-20", "Genève Aéroport", "Leman Trade Sàrl", "Export", "FREIGEGEBEN"),
                    ("A-2026-0004", "26CH000004D9", "2026-08-20", "Chiasso Strada", "Ticino Freight SA", "Import", "ANGENOMMEN"),
                ),
            ),
            (
                "ZOLL",
                "WARENPOSITIONEN",
                "table",
                ORACLE_FIELDS["zoll.warenpositionen"],
                (
                    ("P-1001", "A-2026-0001", "84713000", "Mobile Computer", "DE", "84.50", "125000.00"),
                    ("P-1002", "A-2026-0001", "85176200", "Network Equipment", "NL", "41.20", "68000.00"),
                    ("P-2001", "A-2026-0002", "30049000", "Pharmaceutical Products", "FR", "215.00", "930000.00"),
                    ("P-3001", "A-2026-0003", "91022100", "Wrist Watches", "CH", "17.40", "420000.00"),
                    ("P-4001", "A-2026-0004", "87089900", "Vehicle Components", "IT", "385.00", "154000.00"),
                ),
            ),
            (
                "ZOLL",
                "ABGABEN_UEBERSICHT_V",
                "view",
                ORACLE_FIELDS["zoll.abgaben_uebersicht_v"],
                (
                    ("A-2026-0001", "1250.00", "15660.00", "0.00", "16910.00"),
                    ("A-2026-0002", "0.00", "0.00", "0.00", "0.00"),
                    ("A-2026-0003", "0.00", "0.00", "0.00", "0.00"),
                    ("A-2026-0004", "2310.00", "12536.00", "84.00", "14930.00"),
                ),
            ),
        )
    return _generic_oracle_specs(source)


def _oracle_catalog(source: dict[str, Any]) -> SourceCatalog:
    source_id = str(source.get("id") or "").strip()
    schemas: dict[str, list[SourceObject]] = {}
    for schema, name, kind, fields, rows in _relation_specs(source):
        relation = f"{schema}.{name}"
        schemas.setdefault(schema, []).append(
            SourceObject(
                name=name,
                kind=kind,
                relation=relation,
                query_alias=f"{source_id}.{relation}",
                query_reference=f"{source_id}.{relation}",
                query_sql=_values_relation(fields, rows),
            )
        )
    return SourceCatalog(
        name=source_id,
        connection_source_id=source_id,
        **_source_metadata(source),
        schemas=[SourceSchema(name=name, label=name, objects=objects) for name, objects in schemas.items()],
        connection_status="available",
        connection_label="Grant active",
        connection_detail="Oracle PoC via DuckDB simulation; no Oracle credentials are stored.",
        connection_controls_enabled=False,
    )


class SourceSourcingCoordinator:
    """DaCa-backed source grants plus actor-scoped Oracle PoC catalogs."""

    def __init__(self, settings: Settings, *, transport: httpx.BaseTransport | None = None) -> None:
        self._base_url = str(settings.daca_base_url or "").strip().rstrip("/")
        self._timeout = max(0.1, float(settings.daca_http_timeout_seconds))
        self._transport = transport
        self._lock = RLock()
        self._grant_cache: dict[str, _GrantCacheEntry] = {}

    def _request(self, actor: str, method: str, path: str, **kwargs: Any) -> Any:
        try:
            with httpx.Client(
                timeout=self._timeout,
                transport=self._transport,
                follow_redirects=False,
            ) as client:
                response = client.request(
                    method,
                    f"{self._base_url}{path}",
                    headers={"Accept": "application/json", "X-DaCa-User": validated_demo_actor(actor)},
                    **kwargs,
                )
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            raise SourceSourcingError(503, f"DaCa source governance is unavailable: {exc}") from exc
        if response.status_code >= 400:
            raise SourceSourcingError(response.status_code, _response_detail(response))
        return response.json()

    def catalog(self, actor: str, *, query: str = "", site: str = "", offset: int = 0, limit: int = 12) -> dict[str, Any]:
        params: dict[str, object] = {
            "sourceType": "oracle",
            "offset": offset,
            "limit": limit,
        }
        if query.strip():
            params["q"] = query.strip()
        if site.strip():
            params["site"] = "both" if site.strip().upper() == "BOTH" else site.strip().upper()
        return self._request(
            actor,
            "GET",
            "/api/v1/source-catalog",
            params=params,
        )

    def access_context(self, actor: str, source_id: str) -> dict[str, Any]:
        return self._request(actor, "GET", f"/api/v1/source-catalog/{source_id}/access-context")

    def create_request(self, actor: str, payload: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            key: payload.get(key)
            for key in (
                "clientRequestId",
                "sourceId",
                "requestTitle",
                "subject",
                "purpose",
                "legalBasis",
                "validFrom",
                "validUntil",
                "conditionsAccepted",
            )
            if key in payload
        }
        return self._request(actor, "POST", "/api/v1/source-access-requests", json=allowed)

    def my_requests(self, actor: str) -> list[dict[str, Any]]:
        payload = self._request(actor, "GET", "/api/v1/source-access-requests/mine")
        return list(payload if isinstance(payload, list) else [])

    def grants(self, actor: str, *, refresh: bool = False) -> tuple[dict[str, Any], ...]:
        normalized_actor = validated_demo_actor(actor)
        now = time.monotonic()
        with self._lock:
            cached = self._grant_cache.get(normalized_actor)
            if not refresh and cached is not None and cached.expires_at > now:
                return cached.grants
        payload = self._request(normalized_actor, "GET", "/api/v1/source-access-grants/mine")
        grants = tuple(dict(item) for item in payload if isinstance(item, dict))
        with self._lock:
            self._grant_cache[normalized_actor] = _GrantCacheEntry(now + 3.0, grants)
        return grants

    def active_oracle_sources(self, actor: str, *, refresh: bool = False) -> tuple[dict[str, Any], ...]:
        sources_by_id: dict[str, dict[str, Any]] = {}
        for grant in self.grants(actor, refresh=refresh):
            source = grant.get("source")
            if grant.get("state") != "active" or not isinstance(source, dict):
                continue
            source_id = str(source.get("id") or "").strip()
            if source_id:
                sources_by_id.setdefault(source_id, dict(source))
        return tuple(sources_by_id.values())

    def catalogs_for_actor(self, actor: str) -> list[SourceCatalog]:
        return [_oracle_catalog(source) for source in self.active_oracle_sources(actor)]

    def fields_for_relation(self, actor: str, relation: str) -> list[SourceField]:
        self.assert_query_authorized(actor, f"SELECT * FROM {relation}")
        normalized = str(relation or "").strip().casefold()
        for source in self.active_oracle_sources(actor):
            source_id = str(source.get("id") or "").casefold()
            candidate = normalized[len(source_id) + 1 :] if normalized.startswith(f"{source_id}.") else normalized
            for schema, name, _kind, fields, _rows in _relation_specs(source):
                if candidate == f"{schema}.{name}".casefold():
                    return [SourceField(name=field_name, data_type=data_type) for field_name, data_type in fields]
        raise KeyError(f"Unknown Oracle PoC relation: {relation}")

    def explorer_payload(self, actor: str, source_id: str) -> dict[str, Any]:
        catalogs = [catalog for catalog in self.catalogs_for_actor(actor) if catalog.name == source_id]
        if not catalogs:
            raise KeyError(f"Unknown or unauthorized Oracle data source: {source_id}")
        schemas: list[dict[str, Any]] = []
        default_relation = ""
        for schema in catalogs[0].schemas:
            objects = []
            for source_object in schema.objects:
                default_relation = default_relation or source_object.relation
                objects.append(
                    {
                        "name": source_object.name,
                        "displayName": source_object.display_name or source_object.name,
                        "kind": source_object.kind,
                        "relation": source_object.relation,
                        "queryAlias": source_object.query_alias,
                        "queryReference": source_object.query_reference,
                        "querySql": source_object.query_sql,
                        "publishedDataProducts": [],
                    }
                )
            schemas.append({"name": schema.name, "label": schema.label or schema.name, "objectCount": len(objects), "objects": objects})
        return {"sourceId": source_id, "explorerKind": "postgres", "schemas": schemas, "defaultRelation": default_relation}

    def assert_query_authorized(self, actor: str, sql: str, data_sources: list[str] | None = None) -> None:
        normalized = str(sql or "").casefold()
        requested = {str(item or "").strip().casefold() for item in (data_sources or [])}
        if "ora_" not in normalized and not any(item.startswith("ora_") for item in requested):
            return
        active_ids = {
            str(source.get("id") or "").casefold()
            for source in self.active_oracle_sources(actor)
        }
        referenced = set(re.findall(r"\bora_[a-z0-9_]+\b", normalized))
        referenced.update(item for item in requested if item.startswith("ora_"))
        if not referenced or referenced.difference(active_ids):
            raise SourceSourcingError(403, "An active DaCa source grant is required for this Oracle query.")

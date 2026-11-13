"""Repeatable demo preparation through normal product, SLA and governance APIs."""

from __future__ import annotations

from datetime import datetime, timedelta
import time
from zoneinfo import ZoneInfo

import httpx

from ..data_generator.mwst_journey import (
    BUCKET,
    COMPANY_KEY,
    COMPANY_SLUG,
    EXAMPLE_KEY,
    EXAMPLE_SLUG,
    FIELD_DESCRIPTIONS,
    FIELD_TERMS,
    GENERATOR_ID,
    NATIONAL_KEY,
    NATIONAL_SLUG,
)
from .data_products.daca_client import normalize_loopback_url

OWNER = "kassandra.valdata"
CONTROLLER = "thomas.kriegli"
PRODUCTS = (
    (
        NATIONAL_SLUG,
        "MWST-Erträge Schweiz 2016–2026",
        NATIONAL_KEY,
        "Elf synthetische Schweizer Jahreswerte, aus denselben 260 Firmen wie das Firmenprodukt summiert. "
        "2016–2025 sind vollständige Jahreswerte; 2026 ist vorläufig per 12.11.2026. Keine amtlichen Zahlen.",
        [OWNER],
    ),
    (
        COMPANY_SLUG,
        "MWST-Erträge nach Firmen 2016–2026",
        COMPANY_KEY,
        "Synthetische MWST-Erträge von 260 fiktiven Firmen in allen 26 Kantonen: 60 Firmen aus SG und je acht aus "
        "den übrigen Kantonen, insgesamt 2.860 Firmen-Jahreszeilen. Die Kantonszuordnung folgt dem Firmensitz. "
        "2016–2025 vollständig; 2026 vorläufig per 12.11.2026. Keine realen Firmen oder amtlichen Zahlen.",
        [OWNER],
    ),
    (
        EXAMPLE_SLUG,
        "MWST-Erträge nach Firmen 2026 – St. Gallen (fertiges Beispiel)",
        EXAMPLE_KEY,
        "Getrennter fertiger Beispielstand: 60 synthetische Firmenzeilen aus SG für 2026, vorläufig per 12.11.2026. "
        "Reiner Filter, keine Summierung. Kassandras eigenes SG-Produkt und ihre Anträge bleiben davon getrennt.",
        [OWNER, "jan.waser"],
    ),
)


class JourneyApi:
    def __init__(self, base_url):
        self.base_url = normalize_loopback_url(base_url)
        self.client = httpx.Client(timeout=60)

    def close(self):
        self.client.close()

    def call(self, method, path, *, actor=OWNER, body=None, revision=None, form=None):
        headers = {"X-DaCa-User": actor, "Accept": "application/json"}
        if revision is not None:
            headers["If-Match"] = f'"{revision}"'
        response = self.client.request(
            method, self.base_url + path, headers=headers, json=body, data=form
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"{method} {path}: HTTP {response.status_code}: {response.text[:1500]}"
            )
        return response.json() if response.content else None

    def get(self, path, *, actor=OWNER):
        return self.call("GET", path, actor=actor)


def prepare_quality(daca: JourneyApi, product_id: str) -> None:
    base = f"/api/v1/data-products/{product_id}"
    product = daca.get(base)
    workspace = daca.get(base + "/quality")
    if all(criterion["complete"] for criterion in workspace["quality"]["criteria"]):
        return
    source_id = product["metadata"].get("sourceProductId", "")
    national = NATIONAL_SLUG in source_id
    fields = []
    for field in workspace["fields"]:
        name = field["name"]
        fields.append(
            {
                "id": field["id"],
                "keyField": name in {"company_id", "tax_year"},
                "businessDescription": FIELD_DESCRIPTIONS.get(
                    name, field.get("businessDescription") or name
                ),
                "ontologyTermUri": f"urn:daca:ontology:tax:{FIELD_TERMS[name]}"
                if name in FIELD_TERMS
                else None,
            }
        )
    daca.call(
        "PUT",
        base + "/quality",
        body={
            "title": product["title"],
            "description": product["description"],
            "domain": "Mehrwertsteuer",
            "classification": product["classification"],
            "contactEmail": "kassandra.valdata@estv.admin.ch",
            "updateFrequency": "annually",
            "dcatReviewed": True,
            "productClassUri": "urn:daca:ontology:tax:"
            + ("VatNationalRevenueDataset" if national else "VatCompanyRevenueDataset"),
            "fields": fields,
            "graph": workspace["graph"] or {},
            "graphConfirmed": True,
            "discoverable": True,
            "discoverabilityConfirmed": True,
        },
    )


def prepare_sla(daca: JourneyApi, product_id: str) -> None:
    base = f"/api/v1/data-products/{product_id}/service-level"
    summary = daca.get(base)
    if summary["source"] == "published":
        return
    revisions = daca.get(base + "/revisions")["items"]
    open_revisions = [row for row in revisions if row["status"] in {"draft", "pending_approval"}]
    if open_revisions:
        row = max(open_revisions, key=lambda value: value["revision"])
    else:
        definition = dict(summary["current"]["definition"])
        product = daca.get(f"/api/v1/data-products/{product_id}")
        definition["purposeAndSuitableUse"] = (
            f"Das synthetische Datenprodukt «{product['title']}» dient der behördlichen Analyse "
            f"in der MWST-Journey. {product['description']} "
            "Die Kantonszuordnung folgt dem Firmensitz. "
            "Keine amtlichen Zahlen und kein produktiver Aktualisierungsdienst."
        )
        limitation = "Feststehender synthetischer Demonstrationsbestand per 12.11.2026; keine echten Firmen oder Steuerdaten."
        definition["knownLimitations"] = list(
            dict.fromkeys([*definition["knownLimitations"], limitation])
        )
        today = datetime.now(ZoneInfo("Europe/Zurich")).date()
        definition["nextReviewOn"] = (today + timedelta(days=365)).isoformat()
        row = daca.call(
            "POST",
            base + "/revisions",
            revision=max((row["revision"] for row in revisions), default=0),
            body={"validFrom": today.isoformat(), "validUntil": None, "definition": definition},
        )
    path = base + "/revisions/" + row["revisionId"]
    if row["status"] == "draft":
        row = daca.call("POST", path + "/submit", revision=row["lockVersion"])
    if row["status"] == "pending_approval":
        daca.call(
            "POST",
            path + "/decision",
            actor=CONTROLLER,
            revision=row["lockVersion"],
            body={"decision": "approve"},
        )


def complete_product(daca: JourneyApi, product_id: str, readers: list[str]) -> None:
    """Use normal review and two-person approval; never set publication flags in SQL."""
    prepare_quality(daca, product_id)
    prepare_sla(daca, product_id)
    base = f"/api/v1/data-products/{product_id}"
    product = daca.get(base)
    if product["lifecycle"] == "active" and product["activePolicyRevision"]:
        return
    today = datetime.now(ZoneInfo("Europe/Zurich")).date()
    tasks = daca.get("/api/v1/tasks/mine", actor=CONTROLLER)
    existing = next(
        (
            task
            for task in tasks
            if task["taskType"] == "publication_approval"
            and task["dataProductId"] == product_id
            and task["status"] != "completed"
        ),
        None,
    )
    if existing:
        submission = daca.get(
            "/api/v1/governance-submissions/" + existing["governanceSubmissionId"], actor=CONTROLLER
        )
    else:
        submission = daca.call(
            "POST",
            base + "/governance-submissions",
            body={
                "discoverable": True,
                "discoverabilityConfirmed": True,
                "barArchive": {"enabled": False},
                "grants": [
                    {
                        "subject": {"type": "person", "id": reader},
                        "actions": ["data.read"],
                        "protocols": ["http"],
                        "validFrom": today.isoformat(),
                        "validUntil": (today + timedelta(days=365)).isoformat(),
                        "dataVariant": "original",
                        "metadataChannels": {"i14y": False, "kobyMcp": False},
                    }
                    for reader in readers
                ],
            },
        )
    path = "/api/v1/governance-submissions/" + submission["id"]
    if submission["status"] in {"pending_approval", "deployment_failed"}:
        submission = daca.call(
            "POST",
            path + "/decision",
            actor=CONTROLLER,
            revision=submission["revision"],
            body={
                "decision": "approve",
                "policyRevision": submission["policyRevision"],
                "comment": "Synthetisches MWST-Journey-Produkt geprüft.",
            },
        )
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        product = daca.get(base)
        if product["lifecycle"] == "active" and product["activePolicyRevision"]:
            effective = daca.get(base + "/effective-access", actor=readers[-1])
            if effective["granted"]:
                return
        time.sleep(1)
    raise RuntimeError("Die OPA- und PostgreSQL-Aktivierung wurde noch nicht bestätigt.")


def setup_journey(daaif_url: str, daca_url: str, *, log=print) -> list[dict]:
    daaif, daca = JourneyApi(daaif_url), JourneyApi(daca_url)
    try:
        job = daaif.call(
            "POST",
            "/api/data-generation-jobs",
            form={"generator_id": GENERATOR_ID, "size_gb": "0.01"},
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            state = daaif.get("/api/data-generation-jobs")
            current = next((item for item in state["jobs"] if item["jobId"] == job["jobId"]), {})
            if current.get("status") == "failed":
                raise RuntimeError(current.get("message", "MWST-Loader fehlgeschlagen"))
            if current.get("status") == "completed":
                break
            time.sleep(1)
        else:
            raise RuntimeError("MWST-Loader hat das Zeitlimit überschritten.")
        log("MWST-Dateien: 2.860 Firmen-Jahreszeilen, elf Jahressummen, 60 SG-Beispielzeilen.")
        existing = {
            product["slug"]: product for product in daaif.get("/api/data-products")["products"]
        }
        prepared = []
        for slug, title, key, description, readers in PRODUCTS:
            product = existing.get(slug)
            if product is None:
                properties = {
                    "updateFrequency": "annually",
                    "targetAudience": "Behördliche Analyse · synthetische MWST-Journey",
                }
                if slug == EXAMPLE_SLUG:
                    properties.update(cantonCode="SG", taxYear="2026")
                result = daaif.call(
                    "POST",
                    "/api/data-products",
                    body={
                        "source": {
                            "sourceKind": "relation",
                            "sourceId": "s3",
                            "relation": "",
                            "bucket": BUCKET,
                            "key": key,
                            "sourcePlatform": "s3",
                        },
                        "title": title,
                        "slug": slug,
                        "description": description,
                        "owner": "Kassandra Valdata",
                        "domain": "Mehrwertsteuer",
                        "tags": ["mwst", "synthetisch", "2016-2026"],
                        "accessLevel": "internal",
                        "requestAccessContact": "kassandra.valdata@estv.admin.ch",
                        "customProperties": properties,
                        "publishToDaca": True,
                    },
                )
                product = result["product"]
            if (
                not product.get("dacaPublication")
                or product.get("bucket") != BUCKET
                or product.get("key") != key
                or product.get("owner") != "Kassandra Valdata"
            ):
                raise RuntimeError(
                    f"Der reservierte Demo-Slug {slug} wird bereits von einem anderen Produkt verwendet."
                )
            complete_product(daca, product["dacaPublication"]["productId"], readers)
            prepared.append(product)
            log(f"Bereit: {title} · {product['dacaPublication']['productId']}")
        return prepared
    finally:
        daaif.close()
        daca.close()

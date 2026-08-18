from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from time import monotonic
from urllib.parse import urlencode

from ...models import DacaPublicationReference, DataProductDefinition
from .daca_client import DacaMetadataPublicationClient, DacaPublicationError
from .manager import DataProductManager
from .source_resolution import relation_backed_daca_source


JOURNEY_SLUG = "kantonale-gewerbesteuer-soll-ist-2022-2026"
JOURNEY_SOURCE_PRODUCT_ID = f"daaif:{JOURNEY_SLUG}:v1"

DEMO_USER_ALIASES = {
    "beat stalder": "beat.stalder",
    "beat.stalder": "beat.stalder",
    "beat.stalder@sg.ch": "beat.stalder",
    "joel ruod": "joel.ruod",
    "joel.ruod": "joel.ruod",
    "joel.ruod@estv.admin.ch": "joel.ruod",
    "kassandra valdata": "kassandra.valdata",
    "kassandra.valdata": "kassandra.valdata",
    "kassandra.valdata@estv.admin.ch": "kassandra.valdata",
    "noémie rochat": "noemie.rochat",
    "noemie rochat": "noemie.rochat",
    "noemie.rochat": "noemie.rochat",
    "noemie.rochat@ne.ch": "noemie.rochat",
    "thomas kriegli": "thomas.kriegli",
    "thomas.kriegli": "thomas.kriegli",
    "thomas.kriegli@estv.admin.ch": "thomas.kriegli",
}

FIELD_DESCRIPTIONS = {
    "canton_code": "Offizieller zweistelliger Kantonscode.",
    "canton_name": "Bezeichnung des Kantons.",
    "tax_year": "Steuerjahr der aggregierten Kennzahl.",
    "annual_plan_chf": "Jahresplanwert der kantonalen Gewerbesteuer in CHF.",
    "expected_receipts_to_date_chf": (
        "Bis zum Stichtag erwarteter Ist-Eingang in CHF."
    ),
    "actual_receipts_to_date_chf": "Bis zum Stichtag effektiv gemeldeter Eingang in CHF.",
    "annual_projection_chf": "Auf das vollständige Steuerjahr hochgerechneter Betrag in CHF.",
    "variance_chf": "Differenz zwischen Hochrechnung beziehungsweise Ist und Jahresplan in CHF.",
    "variance_pct": "Planabweichung in Prozent.",
    "reported_month_count": "Anzahl der gemeldeten Monatswerte.",
    "complete_month_count": "Anzahl der vollständig abgeschlossenen Monate.",
    "completeness_pct": "Vollständigkeit der Monatsmeldungen in Prozent.",
}


def owner_user_id(owner: str, *, slug: str) -> str:
    normalized = str(owner or "").strip().casefold()
    if not normalized and slug == JOURNEY_SLUG:
        return "joel.ruod"
    resolved = DEMO_USER_ALIASES.get(normalized)
    if resolved is None:
        raise ValueError(
            "Publishing to DaCa requires one of the selectable PoC users as owner."
        )
    if slug == JOURNEY_SLUG and resolved != "joel.ruod":
        raise ValueError("The Data Analyst's Journey must be published by Joel Ruod.")
    return resolved


def field_business_description(name: str) -> str:
    normalized = str(name or "").strip().lower()
    if normalized in FIELD_DESCRIPTIONS:
        return FIELD_DESCRIPTIONS[normalized]
    return normalized.replace("_", " ").capitalize() + "."


def same_publishable_definition(
    existing: DataProductDefinition,
    candidate: DataProductDefinition,
) -> bool:
    """Compare user-controlled publication fields, excluding durable identity/state."""
    return all(
        (
            existing.slug == candidate.slug,
            existing.title == candidate.title,
            existing.description == candidate.description,
            existing.source == candidate.source,
            existing.public_path == candidate.public_path,
            existing.owner == candidate.owner,
            existing.domain == candidate.domain,
            existing.tags == candidate.tags,
            existing.access_level == candidate.access_level,
            existing.access_note == candidate.access_note,
            existing.request_access_contact == candidate.request_access_contact,
            existing.custom_properties == candidate.custom_properties,
        )
    )


class DacaPublicationCoordinator:
    """Reserves locally, publishes metadata, then atomically activates the endpoint."""

    def __init__(
        self,
        *,
        manager: DataProductManager,
        client: DacaMetadataPublicationClient,
        daca_ui_url: str,
        public_base_url: str | None = None,
    ) -> None:
        self._manager = manager
        self._client = client
        self._daca_ui_url = str(daca_ui_url or "").strip().rstrip("/")
        self._public_base_url = str(public_base_url or "").strip().rstrip("/")
        self._last_reconciliation_attempt: dict[str, float] = {}

    def create_managed_product(
        self,
        *,
        source: dict[str, object],
        title: str,
        slug: str = "",
        description: str = "",
        owner: str = "",
        domain: str = "",
        tags: list[str] | None = None,
        access_level: str = "internal",
        access_note: str = "",
        request_access_contact: str = "",
        custom_properties: dict[str, str] | None = None,
        overwrite_existing: bool = False,
        expected_product_id: str = "",
        expected_updated_at: str = "",
        base_url: str | None = None,
    ) -> dict[str, object]:
        existing_product: DataProductDefinition | None = None
        product_by_id = getattr(self._manager, "product_by_id", None)
        if overwrite_existing and expected_product_id and callable(product_by_id):
            try:
                existing_product = product_by_id(expected_product_id)
            except KeyError:
                existing_product = None
        candidate = self._manager.reserve_product(
            source=relation_backed_daca_source(source),
            title=title,
            slug=slug,
            description=description,
            owner=owner,
            domain=domain,
            tags=tags,
            access_level=access_level,
            access_note=access_note,
            request_access_contact=request_access_contact,
            custom_properties=custom_properties,
            overwrite_existing=overwrite_existing,
            expected_product_id=expected_product_id,
            expected_updated_at=expected_updated_at,
            allow_managed_overwrite=True,
        )
        operation = "replace" if overwrite_existing else "create"
        existing_reference = candidate.daca_publication
        try:
            if (
                existing_product is not None
                and existing_reference is not None
                and same_publishable_definition(existing_product, candidate)
            ):
                status = self._client.product_status(
                    product_id=existing_reference.product_id,
                    owner_user_id=owner_user_id(candidate.owner, slug=candidate.slug),
                )
                reference = replace(
                    existing_reference,
                    state=status.state,
                    missing_fields=list(status.missing_fields),
                    synced_at=datetime.now(UTC).isoformat(),
                )
                activated = self._manager.activate_reserved_product(
                    replace(candidate, daca_publication=reference)
                )
                return {
                    "action": "replaced",
                    "product": activated.payload(base_url=base_url),
                    "dacaPublication": reference.payload,
                }

            metadata_payload = self.metadata_payload(
                candidate,
                base_url=base_url,
            )
            outcome = self._client.publish(metadata_payload)
            if existing_reference is not None and (
                outcome.product_id != existing_reference.product_id
                or outcome.publication_id != existing_reference.publication_id
            ):
                raise DacaPublicationError(
                    "DaCa returned different identifiers for the existing managed data product. "
                    "The previous local publication remains active.",
                    kind="invalid_response",
                )
            catalog_url = self.catalog_url(
                product_id=outcome.product_id,
                demo_user_id=str(metadata_payload["ownerUserId"]),
            )
            reference = DacaPublicationReference(
                source_product_id=str(metadata_payload["sourceProductId"]),
                publication_id=outcome.publication_id,
                product_id=outcome.product_id,
                state=outcome.state,
                created=(
                    existing_reference.created
                    if existing_reference is not None
                    else outcome.created
                ),
                task_ids=list(outcome.task_ids),
                missing_fields=list(outcome.missing_fields),
                catalog_url=catalog_url,
            )
            activated = self._manager.activate_reserved_product(
                replace(candidate, daca_publication=reference)
            )
        except Exception:
            self._manager.cancel_reserved_product(candidate.product_id)
            raise

        return {
            "action": "replaced" if operation == "replace" else "created",
            "product": activated.payload(base_url=base_url),
            "dacaPublication": reference.payload,
        }

    def reconcile_products(self, *, minimum_interval_seconds: float = 5.0) -> None:
        now = monotonic()
        for product in self._manager.managed_product_definitions():
            reference = product.daca_publication
            if reference is None or reference.state == "published":
                continue
            last_attempt = self._last_reconciliation_attempt.get(reference.product_id, 0.0)
            if now - last_attempt < max(0.0, minimum_interval_seconds):
                continue
            self._last_reconciliation_attempt[reference.product_id] = now
            try:
                status = self._client.product_status(
                    product_id=reference.product_id,
                    owner_user_id=owner_user_id(product.owner, slug=product.slug),
                )
                self._manager.reconcile_daca_publication(
                    product_id=product.product_id,
                    state=status.state,
                    missing_fields=list(status.missing_fields),
                )
            except Exception:
                # Catalog status is display metadata. A temporary DaCa failure
                # must not make the local workbench or its already protected
                # data-product endpoint unavailable.
                continue

    def metadata_payload(
        self,
        product: DataProductDefinition,
        *,
        base_url: str | None,
    ) -> dict[str, object]:
        endpoint_base_url = self._public_base_url or str(base_url or "").strip().rstrip("/")
        if not endpoint_base_url:
            raise ValueError("A public DAAIF base URL is required for DaCa publication.")

        schema_fields = self._manager.publication_schema_fields(product)
        for field in schema_fields:
            field["businessDescription"] = field_business_description(
                str(field.get("name") or "")
            )

        properties = dict(product.custom_properties)
        frequency = properties.get("updateFrequency") or (
            "monthly" if product.slug == JOURNEY_SLUG else ""
        )
        target_audience = properties.get("targetAudience") or (
            "EFD – EFV / Bundestresorerie" if product.slug == JOURNEY_SLUG else ""
        )
        resolved_owner = owner_user_id(product.owner, slug=product.slug)
        source_product_id = f"daaif:{product.slug}:v1"

        keywords = list(product.tags)
        for keyword in (
            "synthetisch" if product.slug == JOURNEY_SLUG else "",
            "EFD" if target_audience else "",
            "EFV" if target_audience else "",
            "Bundestresorerie" if target_audience else "",
        ):
            if keyword and keyword.casefold() not in {
                item.casefold() for item in keywords
            }:
                keywords.append(keyword)

        return {
            "sourceSystem": "DAAIF",
            "sourceProductId": source_product_id,
            "ownerUserId": resolved_owner,
            "publicationMode": "governance_review",
            # DaCa keeps governance-review records undiscoverable until the
            # four-eyes approval and both enforcement deployments complete.
            # This value records the requested final state.
            "discoverable": True,
            "technicalMetadata": {
                "serviceName": f"{product.slug}-api",
                "endpoint": {
                    "baseUrl": endpoint_base_url,
                    "path": product.public_path,
                    "method": "GET",
                },
                "schemaFields": schema_fields,
            },
            "businessMetadata": {
                "title": product.title,
                "description": product.description,
                "domain": product.domain,
                "classification": product.access_level,
                "keywords": keywords[:30],
                "contactEmail": (
                    "joel.ruod@estv.admin.ch"
                    if resolved_owner == "joel.ruod"
                    else product.request_access_contact or None
                ),
                "updateFrequency": frequency or None,
                "graph": {
                    "@context": {
                        "dcat": "http://www.w3.org/ns/dcat#",
                        "dct": "http://purl.org/dc/terms/",
                        "daca": "urn:daca:ontology:tax:",
                    },
                    "@type": "dcat:Dataset",
                    "dct:title": product.title,
                    "dct:spatial": "Schweiz / 26 Kantone",
                    "dct:audience": target_audience,
                    "dct:provenance": (
                        "Vollständig synthetischer PoC-Datenbestand aus DAAIF."
                        if product.slug == JOURNEY_SLUG
                        else "Metadaten aus DAAIF."
                    ),
                },
                "dcatReviewed": False,
            },
        }

    def catalog_url(self, *, product_id: str, demo_user_id: str) -> str:
        query = urlencode({"demoUser": demo_user_id})
        return (
            f"{self._daca_ui_url}/products/{product_id}/quality?{query}"
        )

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import httpx
from fastapi import HTTPException
from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.data_products.authorization import (  # noqa: E402
    DacaPolicyDenied,
    DacaPolicyEnforcer,
    DacaPolicyUnavailable,
)
from bit_data_workbench.backend.data_products.daca_client import (  # noqa: E402
    DacaMetadataPublicationClient,
    DacaPublicationError,
)
from bit_data_workbench.backend.data_products.publication import (  # noqa: E402
    DacaPublicationCoordinator,
    JOURNEY_SLUG,
    JOURNEY_SOURCE_PRODUCT_ID,
    field_business_description,
)
from bit_data_workbench.backend.data_products.manager import (  # noqa: E402
    DataProductManager,
)
from bit_data_workbench.backend.data_products.registry import (  # noqa: E402
    DataProductStore,
)
from bit_data_workbench.api.data_products import read_public_data_product  # noqa: E402
from bit_data_workbench.models import (  # noqa: E402
    DacaPublicationReference,
    DataProductDefinition,
    DataProductSourceDescriptor,
    SourceField,
)


PUBLICATION_ID = "70fcab5b-233f-4191-bf6e-99ea63de1bd9"
PRODUCT_ID = "8431e889-8d70-49e4-b790-7ef9fe8bb1df"
TASK_ID = "103b6fb6-614a-42fd-a10c-55fef4c0b520"


def successful_response(*, created: bool = True, status_code: int = 201) -> httpx.Response:
    return httpx.Response(
        status_code,
        json={
            "publicationId": PUBLICATION_ID,
            "productId": PRODUCT_ID,
            "state": "pending_review",
            "created": created,
            "missingFields": ["dcatReviewed"],
            "taskIds": [TASK_ID],
        },
    )


class FakeManager:
    def __init__(self) -> None:
        self.reserved: DataProductDefinition | None = None
        self.activated: DataProductDefinition | None = None
        self.cancelled: list[str] = []
        self.reconciled: list[tuple[str, str, list[str]]] = []

    def reserve_product(self, **kwargs) -> DataProductDefinition:
        source_payload = kwargs["source"]
        self.reserved = DataProductDefinition(
            product_id="data-product-reserved",
            slug=kwargs["slug"],
            title=kwargs["title"],
            description=kwargs["description"],
            source=DataProductSourceDescriptor(
                source_kind=source_payload["sourceKind"],
                source_id=source_payload["sourceId"],
                relation=source_payload.get("relation", ""),
                source_display_name=source_payload.get("sourceDisplayName", ""),
                source_platform=source_payload.get("sourcePlatform", ""),
            ),
            public_path=f"/api/public/data-products/{kwargs['slug']}",
            owner=kwargs["owner"],
            domain=kwargs["domain"],
            tags=list(kwargs["tags"] or []),
            access_level=kwargs["access_level"],
            access_note=kwargs["access_note"],
            request_access_contact=kwargs["request_access_contact"],
            custom_properties=dict(kwargs["custom_properties"] or {}),
        )
        return self.reserved

    def publication_schema_fields(self, _product) -> list[dict[str, object]]:
        return [
            {
                "name": "canton_code",
                "dataType": "VARCHAR",
                "nullable": False,
                "keyField": True,
            },
            {
                "name": "tax_year",
                "dataType": "INTEGER",
                "nullable": False,
                "keyField": True,
            },
            {
                "name": "annual_projection_chf",
                "dataType": "DECIMAL(18,2)",
                "nullable": False,
                "keyField": False,
            },
        ]

    def activate_reserved_product(
        self, product: DataProductDefinition
    ) -> DataProductDefinition:
        self.activated = product
        return product

    def cancel_reserved_product(self, product_id: str) -> None:
        self.cancelled.append(product_id)

    def managed_product_definitions(self) -> list[DataProductDefinition]:
        return [self.activated] if self.activated is not None else []

    def reconcile_daca_publication(
        self,
        *,
        product_id: str,
        state: str,
        missing_fields: list[str],
        catalog_url: str = "",
    ) -> DataProductDefinition:
        self.reconciled.append((product_id, state, missing_fields))
        assert self.activated is not None
        return self.activated


def journey_create_args() -> dict[str, object]:
    return {
        "source": {
            "sourceKind": "relation",
            "sourceId": "s3",
            "relation": "s3.products.kantonale_gewerbesteuer",
            "sourceDisplayName": "Kantonale Gewerbesteuer",
            "sourcePlatform": "s3",
        },
        "title": "Kantonale Gewerbesteuer: Soll/Ist und Jahreshochrechnung 2022–2026",
        "slug": JOURNEY_SLUG,
        "description": "Vollständig synthetische kantonale Plan-/Ist-Kennzahlen.",
        "owner": "Joel Ruod",
        "domain": "Unternehmens-/Gewerbesteuer",
        "tags": ["Gewerbesteuer", "Kantone"],
        "access_level": "internal",
        "access_note": "DaCa Default Deny bis zur Freigabe.",
        "request_access_contact": "joel.ruod@estv.admin.ch",
        "custom_properties": {
            "updateFrequency": "monthly",
            "targetAudience": "EFD – EFV / Bundestresorerie",
        },
        "base_url": "http://localhost:8000",
    }


def real_managed_manager(
    path: Path,
) -> tuple[DataProductManager, DataProductStore, DataProductDefinition]:
    store = DataProductStore(path)
    existing = store.create_product(
        DataProductDefinition(
            product_id="local-managed-product",
            slug=JOURNEY_SLUG,
            title="Existing managed journey product",
            description="Existing public endpoint metadata.",
            source=DataProductSourceDescriptor(
                source_kind="relation",
                source_id="s3",
                relation="s3.products.kantonale_gewerbesteuer",
                source_display_name="Kantonale Gewerbesteuer",
                source_platform="s3",
            ),
            public_path=f"/api/public/data-products/{JOURNEY_SLUG}",
            owner="Joel Ruod",
            domain="Unternehmens-/Gewerbesteuer",
            tags=["existing"],
            access_level="internal",
            custom_properties={
                "updateFrequency": "monthly",
                "targetAudience": "EFD - EFV / Bundestresorerie",
            },
            daca_publication=DacaPublicationReference(
                source_product_id=JOURNEY_SOURCE_PRODUCT_ID,
                publication_id=PUBLICATION_ID,
                product_id=PRODUCT_ID,
                state="pending_review",
                created=True,
                task_ids=[TASK_ID],
                missing_fields=["dcatReviewed"],
                catalog_url=(
                    f"http://localhost:8080/products/{PRODUCT_ID}/quality"
                    "?demoUser=joel.ruod"
                ),
                synced_at="2026-08-13T08:00:00+00:00",
            ),
            created_at="2026-08-12T08:00:00+00:00",
            updated_at="2026-08-13T08:00:00+00:00",
        )
    )
    manager = DataProductManager(
        settings=SimpleNamespace(),
        store=store,
        create_worker_connection=lambda: None,
        relation_fields_provider=lambda _relation: [
            SourceField(name="canton_code", data_type="VARCHAR"),
            SourceField(name="tax_year", data_type="INTEGER"),
            SourceField(name="annual_projection_chf", data_type="DECIMAL(18,2)"),
        ],
        catalog_provider=lambda: [],
        s3_bucket_snapshot_provider=lambda **_kwargs: {},
    )
    return manager, store, existing


class DacaMetadataPublicationClientTests(unittest.TestCase):
    def client_for(self, handler) -> DacaMetadataPublicationClient:
        return DacaMetadataPublicationClient(
            base_url="http://localhost:8080",
            timeout_seconds=0.2,
            transport=httpx.MockTransport(handler),
        )

    def test_accepts_create_and_idempotent_replay(self) -> None:
        seen_hosts: list[str] = []

        def create_handler(request: httpx.Request) -> httpx.Response:
            seen_hosts.append(request.url.host)
            return successful_response()

        created = self.client_for(create_handler).publish({"sourceSystem": "DAAIF"})
        replayed = self.client_for(
            lambda _request: successful_response(created=False, status_code=200)
        ).publish({"sourceSystem": "DAAIF"})

        self.assertTrue(created.created)
        self.assertFalse(replayed.created)
        self.assertEqual(seen_hosts, ["127.0.0.1"])

    def test_maps_upstream_failure_modes(self) -> None:
        cases = (
            (409, "conflict", 409),
            (422, "validation", 422),
            (404, "disabled", 503),
            (503, "unavailable", 503),
        )
        for status_code, kind, client_status in cases:
            with self.subTest(status_code=status_code):
                client = self.client_for(
                    lambda _request, code=status_code: httpx.Response(
                        code, json={"detail": "upstream failure"}
                    )
                )
                with self.assertRaises(DacaPublicationError) as context:
                    client.publish({"sourceSystem": "DAAIF"})
                self.assertEqual(context.exception.kind, kind)
                self.assertEqual(context.exception.client_status, client_status)

    def test_timeout_and_unreachable_are_fail_closed(self) -> None:
        def timeout_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("late", request=request)

        def unavailable_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("offline", request=request)

        for handler, kind in (
            (timeout_handler, "timeout"),
            (unavailable_handler, "unavailable"),
        ):
            with self.subTest(kind=kind):
                with self.assertRaises(DacaPublicationError) as context:
                    self.client_for(handler).publish({"sourceSystem": "DAAIF"})
                self.assertEqual(context.exception.kind, kind)

    def test_timeout_is_reconciled_with_one_identical_replay(self) -> None:
        calls: list[bytes] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.content)
            if len(calls) == 1:
                raise httpx.ReadTimeout("response lost", request=request)
            return successful_response(created=False, status_code=200)

        result = self.client_for(handler).publish(
            {
                "sourceSystem": "DAAIF",
                "sourceProductId": JOURNEY_SOURCE_PRODUCT_ID,
            }
        )

        self.assertFalse(result.created)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])

    def test_product_status_maps_final_quality_to_published(self) -> None:
        captured_user = ""

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_user
            captured_user = request.headers.get("X-DaCa-User", "")
            return httpx.Response(
                200,
                json={
                    "id": PRODUCT_ID,
                    "lifecycle": "active",
                    "discoverable": True,
                    "activePolicyRevision": 1,
                    "quality": {
                        "score": 6,
                        "criteria": [
                            {"id": "access", "label": "Access", "complete": True}
                        ],
                    },
                },
            )

        status = self.client_for(handler).product_status(
            product_id=PRODUCT_ID,
            owner_user_id="joel.ruod",
        )

        self.assertEqual(captured_user, "joel.ruod")
        self.assertEqual(status.state, "published")
        self.assertEqual(status.missing_fields, ())

    def test_source_refresh_merges_metadata_and_uses_product_revision(self) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.method == "GET":
                return httpx.Response(
                    200,
                    headers={"ETag": '"3"'},
                    json={
                        "id": PRODUCT_ID,
                        "metadata": {"origin": "DAAIF"},
                    },
                )
            return httpx.Response(200, json={"id": PRODUCT_ID, "revision": 4})

        self.client_for(handler).record_source_refresh(
            product_id=PRODUCT_ID,
            owner_user_id="joel.ruod",
            source_updated_at="2026-08-19T09:07:00+00:00",
        )

        self.assertEqual([request.method for request in requests], ["GET", "PATCH"])
        self.assertEqual(requests[1].headers["If-Match"], '"3"')
        self.assertEqual(requests[1].headers["X-DaCa-User"], "joel.ruod")
        self.assertEqual(
            json.loads(requests[1].content),
            {
                "metadata": {
                    "origin": "DAAIF",
                    "sourceUpdatedAt": "2026-08-19T09:07:00+00:00",
                }
            },
        )

    def test_source_refresh_reloads_revision_once_after_concurrent_change(self) -> None:
        methods: list[str] = []
        patch_attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal patch_attempts
            methods.append(request.method)
            if request.method == "GET":
                revision = 3 if patch_attempts == 0 else 4
                return httpx.Response(
                    200,
                    headers={"ETag": f'"{revision}"'},
                    json={"id": PRODUCT_ID, "metadata": {}},
                )
            patch_attempts += 1
            if patch_attempts == 1:
                return httpx.Response(412, json={"detail": "reload and retry"})
            self.assertEqual(request.headers["If-Match"], '"4"')
            return httpx.Response(200, json={"id": PRODUCT_ID, "revision": 5})

        self.client_for(handler).record_source_refresh(
            product_id=PRODUCT_ID,
            owner_user_id="joel.ruod",
            source_updated_at="2026-08-19T09:07:00+00:00",
        )

        self.assertEqual(methods, ["GET", "PATCH", "GET", "PATCH"])


class DacaPublicationCoordinatorTests(unittest.TestCase):
    def coordinator(self, manager: FakeManager, handler) -> DacaPublicationCoordinator:
        return DacaPublicationCoordinator(
            manager=manager,  # type: ignore[arg-type]
            client=DacaMetadataPublicationClient(
                base_url="http://127.0.0.1:8080",
                transport=httpx.MockTransport(handler),
            ),
            daca_ui_url="http://localhost:8080",
        )

    def test_success_activates_once_with_daca_linkage(self) -> None:
        manager = FakeManager()
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured.update(json.loads(request.content))
            return successful_response()

        result = self.coordinator(manager, handler).create_managed_product(
            **journey_create_args()
        )

        self.assertIsNotNone(manager.activated)
        self.assertIsNotNone(manager.activated.daca_publication)
        self.assertEqual(captured["sourceProductId"], JOURNEY_SOURCE_PRODUCT_ID)
        self.assertEqual(captured["ownerUserId"], "joel.ruod")
        self.assertEqual(captured["publicationMode"], "governance_review")
        self.assertTrue(captured["discoverable"])
        self.assertEqual(
            result["dacaPublication"]["catalogUrl"],
            f"http://localhost:8080/products/{PRODUCT_ID}/overview?demoUser=joel.ruod",
        )
        self.assertEqual(manager.cancelled, [])

    def test_journey_aggregate_fields_keep_curated_business_descriptions(self) -> None:
        expected_fields = (
            "expected_receipts_to_date_chf",
            "actual_receipts_to_date_chf",
            "variance_pct",
            "reported_month_count",
            "complete_month_count",
            "completeness_pct",
        )

        for field_name in expected_fields:
            with self.subTest(field_name=field_name):
                self.assertNotEqual(
                    field_business_description(field_name),
                    field_name.replace("_", " ").capitalize() + ".",
                )

    def test_every_daca_failure_cancels_without_activation(self) -> None:
        handlers = (
            lambda _request: httpx.Response(409, json={"detail": "conflict"}),
            lambda _request: httpx.Response(422, json={"detail": "invalid"}),
            lambda _request: httpx.Response(404, json={"detail": "disabled"}),
            lambda _request: httpx.Response(503, json={"detail": "offline"}),
        )
        for handler in handlers:
            with self.subTest(handler=handler):
                manager = FakeManager()
                with self.assertRaises(DacaPublicationError):
                    self.coordinator(manager, handler).create_managed_product(
                        **journey_create_args()
                    )
                self.assertIsNone(manager.activated)
                self.assertEqual(manager.cancelled, ["data-product-reserved"])

    def test_reconciles_pending_reference_after_daca_approval(self) -> None:
        manager = FakeManager()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return successful_response()
            return httpx.Response(
                200,
                json={
                    "id": PRODUCT_ID,
                    "lifecycle": "active",
                    "discoverable": True,
                    "activePolicyRevision": 1,
                    "quality": {"score": 6, "criteria": []},
                },
            )

        coordinator = self.coordinator(manager, handler)
        coordinator.create_managed_product(**journey_create_args())
        coordinator.reconcile_products(minimum_interval_seconds=0)

        self.assertEqual(
            manager.reconciled,
            [("data-product-reserved", "published", [])],
        )

    def test_reconcile_migrates_legacy_link_to_product_overview(self) -> None:
        class NoCatalogRequestClient:
            def product_status(self, **_kwargs) -> None:
                raise AssertionError("Published products need no status request")

        with tempfile.TemporaryDirectory() as temp_dir:
            manager, store, existing = real_managed_manager(
                Path(temp_dir) / "data-products.json"
            )
            manager.reconcile_daca_publication(
                product_id=existing.product_id,
                state="published",
                missing_fields=[],
            )
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=NoCatalogRequestClient(),  # type: ignore[arg-type]
                daca_ui_url="http://localhost:8080",
            )

            coordinator.reconcile_products(minimum_interval_seconds=0)
            persisted = store.list_products()[0]

        self.assertEqual(
            persisted.daca_publication.catalog_url,
            f"http://localhost:8080/products/{PRODUCT_ID}/overview?demoUser=joel.ruod",
        )

    def test_identical_managed_replace_reconciles_status_without_republishing_schema(
        self,
    ) -> None:
        methods: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            methods.append(request.method)
            if request.method == "PATCH":
                payload = json.loads(request.content)
                self.assertIn("sourceUpdatedAt", payload["metadata"])
                self.assertEqual(request.headers["If-Match"], '"3"')
                return httpx.Response(200, json={"id": PRODUCT_ID, "revision": 4})
            return httpx.Response(
                200,
                headers={"ETag": '"3"'},
                json={
                    "id": PRODUCT_ID,
                    "metadata": {"sourceSystem": "DAAIF"},
                    "lifecycle": "active",
                    "discoverable": True,
                    "activePolicyRevision": 1,
                    "quality": {"score": 6, "criteria": []},
                },
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            manager, store, existing = real_managed_manager(
                Path(temp_dir) / "data-products.json"
            )
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=DacaMetadataPublicationClient(
                    base_url="http://127.0.0.1:8080",
                    transport=httpx.MockTransport(handler),
                ),
                daca_ui_url="http://localhost:8080",
            )

            result = coordinator.create_managed_product(
                source=existing.source.payload,
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
                base_url="http://localhost:8000",
            )
            persisted = store.list_products()[0]

        self.assertEqual(methods, ["GET", "PATCH", "GET"])
        self.assertEqual(result["action"], "replaced")
        self.assertEqual(persisted.product_id, existing.product_id)
        self.assertEqual(persisted.created_at, existing.created_at)
        self.assertEqual(persisted.daca_publication.publication_id, PUBLICATION_ID)
        self.assertEqual(persisted.daca_publication.product_id, PRODUCT_ID)
        self.assertEqual(persisted.daca_publication.state, "published")
        self.assertEqual(
            persisted.daca_publication.catalog_url,
            f"http://localhost:8080/products/{PRODUCT_ID}/overview?demoUser=joel.ruod",
        )

    def test_managed_overwrite_exact_replay_preserves_local_and_daca_identifiers(
        self,
    ) -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured.update(json.loads(request.content))
            return successful_response(created=False, status_code=200)

        with tempfile.TemporaryDirectory() as temp_dir:
            manager, store, existing = real_managed_manager(
                Path(temp_dir) / "data-products.json"
            )
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=DacaMetadataPublicationClient(
                    base_url="http://127.0.0.1:8080",
                    transport=httpx.MockTransport(handler),
                ),
                daca_ui_url="http://localhost:8080",
            )
            create_args = journey_create_args()
            create_args.update(
                {
                    "description": "Reviewed replacement metadata.",
                    "tags": ["Gewerbesteuer", "2026"],
                    "overwrite_existing": True,
                    "expected_product_id": existing.product_id,
                    "expected_updated_at": existing.updated_at,
                }
            )

            result = coordinator.create_managed_product(**create_args)
            persisted = store.list_products()

        self.assertEqual(result["action"], "replaced")
        self.assertEqual(len(persisted), 1)
        replaced = persisted[0]
        self.assertEqual(replaced.product_id, existing.product_id)
        self.assertEqual(replaced.created_at, existing.created_at)
        self.assertEqual(replaced.daca_publication.publication_id, PUBLICATION_ID)
        self.assertEqual(replaced.daca_publication.product_id, PRODUCT_ID)
        self.assertEqual(
            replaced.daca_publication.source_product_id,
            JOURNEY_SOURCE_PRODUCT_ID,
        )
        self.assertTrue(replaced.daca_publication.created)
        self.assertEqual(result["product"]["productId"], existing.product_id)
        self.assertEqual(result["dacaPublication"]["publicationId"], PUBLICATION_ID)
        self.assertEqual(result["dacaPublication"]["productId"], PRODUCT_ID)
        self.assertEqual(captured["sourceProductId"], JOURNEY_SOURCE_PRODUCT_ID)

    def test_managed_overwrite_failure_leaves_previous_product_byte_for_byte(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = Path(temp_dir) / "data-products.json"
            manager, store, existing = real_managed_manager(store_path)
            state_before = store_path.read_bytes()
            coordinator = DacaPublicationCoordinator(
                manager=manager,
                client=DacaMetadataPublicationClient(
                    base_url="http://127.0.0.1:8080",
                    transport=httpx.MockTransport(
                        lambda _request: httpx.Response(
                            422,
                            json={"detail": "invalid replacement metadata"},
                        )
                    ),
                ),
                daca_ui_url="http://localhost:8080",
            )
            create_args = journey_create_args()
            create_args.update(
                {
                    "title": "Replacement that DaCa rejects",
                    "overwrite_existing": True,
                    "expected_product_id": existing.product_id,
                    "expected_updated_at": existing.updated_at,
                }
            )

            with self.assertRaises(DacaPublicationError):
                coordinator.create_managed_product(**create_args)

            self.assertEqual(store_path.read_bytes(), state_before)
            self.assertEqual(store.list_products(), [existing])


class DacaPolicyEnforcerTests(unittest.TestCase):
    def enforcer(self, handler) -> DacaPolicyEnforcer:
        return DacaPolicyEnforcer(
            decision_url="http://localhost:8181/v1/data/daca/authz/decision",
            now_provider=lambda: datetime(2026, 8, 13, 7, 0, tzinfo=UTC),
            transport=httpx.MockTransport(handler),
        )

    def test_allow_sends_trusted_timestamp_and_person_subject(self) -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured.update(json.loads(request.content)["input"])
            return httpx.Response(
                200,
                json={
                    "decision_id": str(uuid.uuid4()),
                    "result": {"allow": True, "reason": "policy_allow"},
                },
            )

        decision = self.enforcer(handler).authorize(
            subject_id="thomas.kriegli",
            product_id=PRODUCT_ID,
            method="GET",
            path=f"/api/public/data-products/{JOURNEY_SLUG}",
            request_id="request-1",
        )

        self.assertTrue(decision.allow)
        self.assertEqual(captured["subject"], {"id": "thomas.kriegli", "type": "person"})
        self.assertEqual(captured["context"]["requestTimestamp"], "2026-08-13T07:00:00Z")
        self.assertEqual(captured["action"], "data.read")

    def test_deny_and_malformed_response_fail_closed(self) -> None:
        denied = self.enforcer(
            lambda _request: httpx.Response(
                200, json={"result": {"allow": False, "reason": "default_deny"}}
            )
        )
        with self.assertRaises(DacaPolicyDenied):
            denied.authorize(
                subject_id="beat.stalder",
                product_id=PRODUCT_ID,
                method="GET",
                path="/api/public/data-products/example",
            )

        malformed = self.enforcer(
            lambda _request: httpx.Response(200, json={"result": {"allow": "yes"}})
        )
        with self.assertRaises(DacaPolicyUnavailable):
            malformed.authorize(
                subject_id="beat.stalder",
                product_id=PRODUCT_ID,
                method="GET",
                path="/api/public/data-products/example",
            )


class ManagedPublicDataProductRouteTests(unittest.TestCase):
    class Service:
        def __init__(self) -> None:
            self.read_count = 0
            self.settings = type(
                "Settings",
                (),
                {
                    "daca_opa_url": "http://127.0.0.1:8181/v1/data/daca/authz/decision",
                    "daca_http_timeout_seconds": 2.0,
                },
            )()
            self.product = DataProductDefinition(
                product_id="local-product",
                slug=JOURNEY_SLUG,
                title="Journey",
                description="Synthetic",
                source=DataProductSourceDescriptor(
                    source_kind="relation",
                    source_id="s3",
                    relation="s3.products.journey",
                ),
                public_path=f"/api/public/data-products/{JOURNEY_SLUG}",
                daca_publication=DacaPublicationReference(
                    source_product_id=JOURNEY_SOURCE_PRODUCT_ID,
                    publication_id=PUBLICATION_ID,
                    product_id=PRODUCT_ID,
                    state="pending_review",
                    created=True,
                ),
            )

        def data_product_by_slug(self, _slug: str) -> DataProductDefinition:
            return self.product

        def public_data_product_relation(self, **_kwargs) -> dict[str, object]:
            self.read_count += 1
            return {"items": [], "columns": []}

    @staticmethod
    def request() -> Request:
        return Request(
            {
                "type": "http",
                "scheme": "http",
                "method": "GET",
                "path": f"/api/public/data-products/{JOURNEY_SLUG}",
                "query_string": b"",
                "headers": [(b"host", b"testserver")],
                "server": ("testserver", 80),
            }
        )

    def test_missing_header_returns_401_without_reading_data(self) -> None:
        service = self.Service()
        with self.assertRaises(HTTPException) as context:
            read_public_data_product(
                slug=JOURNEY_SLUG,
                request=self.request(),
                limit=100,
                offset=0,
                prefix="",
                daca_user=None,
                service=service,  # type: ignore[arg-type]
            )
        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(service.read_count, 0)

    def test_deny_and_opa_failure_return_before_data_access(self) -> None:
        for error, status_code in (
            (DacaPolicyDenied("default_deny"), 403),
            (DacaPolicyUnavailable("offline"), 503),
        ):
            with self.subTest(status_code=status_code):
                service = self.Service()
                with patch(
                    "bit_data_workbench.api.data_products.DacaPolicyEnforcer.authorize",
                    side_effect=error,
                ):
                    with self.assertRaises(HTTPException) as context:
                        read_public_data_product(
                            slug=JOURNEY_SLUG,
                            request=self.request(),
                            limit=100,
                            offset=0,
                            prefix="",
                            daca_user="beat.stalder",
                            service=service,  # type: ignore[arg-type]
                        )
                self.assertEqual(context.exception.status_code, status_code)
                self.assertEqual(service.read_count, 0)

    def test_allow_reads_after_policy_decision(self) -> None:
        service = self.Service()
        with patch(
            "bit_data_workbench.api.data_products.DacaPolicyEnforcer.authorize"
        ) as authorize:
            response = read_public_data_product(
                slug=JOURNEY_SLUG,
                request=self.request(),
                limit=100,
                offset=0,
                prefix="",
                daca_user="thomas.kriegli",
                service=service,  # type: ignore[arg-type]
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(service.read_count, 1)
        self.assertEqual(authorize.call_args.kwargs["product_id"], PRODUCT_ID)


if __name__ == "__main__":
    unittest.main()

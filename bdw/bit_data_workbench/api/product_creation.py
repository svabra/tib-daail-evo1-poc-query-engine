"""Read-only DaCa context for the DAAIF commissioning and consumer screens."""

from __future__ import annotations

import uuid
import httpx
from fastapi import APIRouter, Depends, Header, HTTPException

from ..backend.data_products.daca_client import normalize_loopback_url, response_error_detail
from ..backend.service import WorkbenchService
from ..backend.source_sourcing import DEMO_USER_IDS
from ..dependencies import get_workbench_service

router = APIRouter(tags=["product-creation"])


def read_daca(service, path, actor):
    if actor not in DEMO_USER_IDS:
        raise HTTPException(401, "Select a known demo user.")
    try:
        with httpx.Client(timeout=service.settings.daca_http_timeout_seconds) as client:
            response = client.get(
                normalize_loopback_url(service.settings.daca_base_url) + path,
                headers={"X-DaCa-User": actor, "Accept": "application/json"},
            )
    except httpx.HTTPError as exc:
        raise HTTPException(503, "DaCa ist derzeit nicht erreichbar.") from exc
    if response.status_code != 200:
        raise HTTPException(response.status_code, response_error_detail(response))
    return response.json()


@router.get("/api/product-creation-requests/{request_id}")
def creation_context(
    request_id: uuid.UUID,
    actor: str = Header(alias="X-DaCa-User"),
    service: WorkbenchService = Depends(get_workbench_service),
):
    return read_daca(service, f"/api/v1/product-creation-requests/{request_id}", actor)


@router.get("/api/public/data-products/{slug}/service-level")
def product_service_level(
    slug: str,
    actor: str = Header(alias="X-DaCa-User"),
    service: WorkbenchService = Depends(get_workbench_service),
):
    try:
        product = service.data_product_by_slug(slug)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    if not product.daca_publication:
        raise HTTPException(404, "Dieses Produkt hat keine DaCa-SLA.")
    return read_daca(
        service, f"/api/v1/data-products/{product.daca_publication.product_id}/service-level", actor
    )

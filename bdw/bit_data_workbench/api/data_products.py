from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from ..backend.data_products import DEFAULT_PUBLIC_DATA_PRODUCT_LIMIT
from ..backend.data_products.authorization import (
    DacaPolicyDenied,
    DacaPolicyEnforcer,
    DacaPolicyUnavailable,
)
from ..backend.data_products.daca_client import DacaPublicationError
from ..backend.data_products.registry import DataProductOverwriteConflict
from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service


router = APIRouter(tags=["data-products"])


def request_base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def enforce_daca_policy(
    *,
    product,
    request: Request,
    service: WorkbenchService,
    daca_user: str | None,
) -> None:
    reference = product.daca_publication
    if reference is None:
        return

    normalized_user = daca_user.strip() if isinstance(daca_user, str) else ""
    if not normalized_user:
        raise HTTPException(
            status_code=401,
            detail=(
                "This DaCa-managed data product requires the PoC header "
                "X-DaCa-User."
            ),
        )

    settings = service.settings
    enforcer = DacaPolicyEnforcer(
        decision_url=getattr(
            settings,
            "daca_opa_url",
            "http://127.0.0.1:8181/v1/data/daca/authz/decision",
        ),
        timeout_seconds=min(
            2.0,
            float(getattr(settings, "daca_http_timeout_seconds", 2.0)),
        ),
    )
    try:
        enforcer.authorize(
            subject_id=normalized_user,
            product_id=reference.product_id,
            method=request.method,
            path=request.url.path,
            request_id=request.headers.get("x-request-id", ""),
        )
    except DacaPolicyDenied as exc:
        raise HTTPException(
            status_code=403,
            detail=f"DaCa policy denied data.read ({exc.reason}).",
        ) from exc
    except DacaPolicyUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail="DaCa policy enforcement is unavailable; access is denied fail-closed.",
        ) from exc


class DataProductSourcePayload(BaseModel):
    source_kind: str = Field(
        validation_alias="sourceKind",
        serialization_alias="sourceKind",
    )
    source_id: str = Field(
        validation_alias="sourceId",
        serialization_alias="sourceId",
    )
    relation: str = ""
    bucket: str = ""
    key: str = ""
    source_display_name: str = Field(
        default="",
        validation_alias="sourceDisplayName",
        serialization_alias="sourceDisplayName",
    )
    source_platform: str = Field(
        default="",
        validation_alias="sourcePlatform",
        serialization_alias="sourcePlatform",
    )


class DataProductPreviewPayload(BaseModel):
    source: DataProductSourcePayload
    title: str = ""
    slug: str = ""
    description: str = ""
    owner: str = ""
    domain: str = ""
    tags: list[str] = Field(default_factory=list)
    access_level: str = Field(
        default="internal",
        validation_alias="accessLevel",
        serialization_alias="accessLevel",
    )
    access_note: str = Field(
        default="",
        validation_alias="accessNote",
        serialization_alias="accessNote",
    )
    request_access_contact: str = Field(
        default="",
        validation_alias="requestAccessContact",
        serialization_alias="requestAccessContact",
    )
    custom_properties: dict[str, str] = Field(
        default_factory=dict,
        validation_alias="customProperties",
        serialization_alias="customProperties",
    )


class DataProductCreatePayload(DataProductPreviewPayload):
    title: str
    publish_to_daca: bool = Field(
        default=False,
        validation_alias="publishToDaca",
        serialization_alias="publishToDaca",
    )
    overwrite_existing: bool = Field(
        default=False,
        validation_alias="overwriteExisting",
        serialization_alias="overwriteExisting",
    )
    expected_product_id: str = Field(
        default="",
        validation_alias="expectedProductId",
        serialization_alias="expectedProductId",
    )
    expected_updated_at: str = Field(
        default="",
        validation_alias="expectedUpdatedAt",
        serialization_alias="expectedUpdatedAt",
    )


class DataProductUpdatePayload(BaseModel):
    title: str
    description: str = ""
    owner: str = ""
    domain: str = ""
    tags: list[str] = Field(default_factory=list)
    access_level: str = Field(
        default="internal",
        validation_alias="accessLevel",
        serialization_alias="accessLevel",
    )
    access_note: str = Field(
        default="",
        validation_alias="accessNote",
        serialization_alias="accessNote",
    )
    request_access_contact: str = Field(
        default="",
        validation_alias="requestAccessContact",
        serialization_alias="requestAccessContact",
    )
    custom_properties: dict[str, str] = Field(
        default_factory=dict,
        validation_alias="customProperties",
        serialization_alias="customProperties",
    )


@router.get("/api/data-products")
def list_data_products(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        {
            "products": jsonable_encoder(
                service.list_data_products(base_url=request_base_url(request))
            )
        }
    )


@router.post("/api/data-products/preview")
def preview_data_product(
    payload: DataProductPreviewPayload,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        preview = service.preview_data_product(
            source=payload.source.model_dump(by_alias=True),
            title=payload.title,
            slug=payload.slug,
            description=payload.description,
            owner=payload.owner,
            domain=payload.domain,
            tags=list(payload.tags),
            access_level=payload.access_level,
            access_note=payload.access_note,
            request_access_contact=payload.request_access_contact,
            custom_properties=dict(payload.custom_properties),
            base_url=request_base_url(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(preview))


@router.post("/api/data-products")
def create_data_product(
    payload: DataProductCreatePayload,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        create_kwargs = {
            "source": payload.source.model_dump(by_alias=True),
            "title": payload.title,
            "slug": payload.slug,
            "description": payload.description,
            "owner": payload.owner,
            "domain": payload.domain,
            "tags": list(payload.tags),
            "access_level": payload.access_level,
            "access_note": payload.access_note,
            "request_access_contact": payload.request_access_contact,
            "custom_properties": dict(payload.custom_properties),
            "overwrite_existing": payload.overwrite_existing,
            "expected_product_id": payload.expected_product_id,
            "expected_updated_at": payload.expected_updated_at,
            "base_url": request_base_url(request),
        }
        if payload.publish_to_daca:
            created = service.create_daca_data_product(**create_kwargs)
        else:
            created = service.create_data_product(**create_kwargs)
    except DacaPublicationError as exc:
        raise HTTPException(status_code=exc.client_status, detail=str(exc)) from exc
    except DataProductOverwriteConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(created))


@router.put("/api/data-products/{product_id}")
def update_data_product(
    product_id: str,
    payload: DataProductUpdatePayload,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        updated = service.update_data_product_metadata(
            product_id=product_id,
            title=payload.title,
            description=payload.description,
            owner=payload.owner,
            domain=payload.domain,
            tags=list(payload.tags),
            access_level=payload.access_level,
            access_note=payload.access_note,
            request_access_contact=payload.request_access_contact,
            custom_properties=dict(payload.custom_properties),
            base_url=request_base_url(request),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except DataProductOverwriteConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(updated))


@router.delete("/api/data-products/{product_id}")
def delete_data_product(
    product_id: str,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        removed = service.delete_data_product(
            product_id=product_id,
            base_url=request_base_url(request),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except DataProductOverwriteConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(removed))


@router.get(
    "/api/public/data-products/{slug}",
    tags=["data-products-public"],
)
def read_public_data_product(
    slug: str,
    request: Request,
    limit: int = Query(default=DEFAULT_PUBLIC_DATA_PRODUCT_LIMIT),
    offset: int = Query(default=0),
    prefix: str = Query(default=""),
    daca_user: str | None = Header(default=None, alias="X-DaCa-User"),
    service: WorkbenchService = Depends(get_workbench_service),
):
    try:
        product = service.data_product_by_slug(slug)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    enforce_daca_policy(
        product=product,
        request=request,
        service=service,
        daca_user=daca_user,
    )

    base_url = request_base_url(request)
    try:
        if product.source.source_kind == "relation":
            payload = service.public_data_product_relation(
                slug=slug,
                limit=limit,
                offset=offset,
                base_url=base_url,
            )
            return JSONResponse(jsonable_encoder(payload))

        if product.source.source_kind == "bucket":
            payload = service.public_data_product_bucket(
                slug=slug,
                prefix=prefix,
                base_url=base_url,
            )
            return JSONResponse(jsonable_encoder(payload))

        if product.source.source_kind == "object":
            artifact = service.public_data_product_stream(slug=slug)
            headers: dict[str, str] = {}
            if artifact.content_length is not None:
                headers["Content-Length"] = str(artifact.content_length)
            return StreamingResponse(
                artifact.iterator,
                media_type=artifact.content_type,
                headers=headers,
            )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    raise HTTPException(status_code=400, detail="Unsupported published data product.")

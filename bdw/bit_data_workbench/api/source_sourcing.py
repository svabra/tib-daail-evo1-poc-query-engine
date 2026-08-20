from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from ..backend.service import WorkbenchService
from ..backend.source_sourcing import DEMO_USER_IDS, SourceSourcingError, validated_demo_actor
from ..dependencies import get_workbench_service


router = APIRouter(prefix="/api/ingestion/sourcing", tags=["source-sourcing"])
COOKIE_NAME = "daaif_demo_user"


def actor_from_request(request: Request) -> str:
    return validated_demo_actor(request.cookies.get(COOKIE_NAME))


def _call(callback):
    try:
        return callback()
    except SourceSourcingError as exc:
        raise HTTPException(exc.status_code, exc.detail) from exc


@router.post("/identity")
def sync_identity(
    request: Request,
    response: Response,
    payload: dict[str, Any] = Body(default_factory=dict),
) -> dict[str, str]:
    requested = str(payload.get("userId") or "").strip()
    if requested not in DEMO_USER_IDS:
        raise HTTPException(422, "Unknown DAAIF demo identity")
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip().lower()
    response.set_cookie(
        COOKIE_NAME,
        requested,
        httponly=True,
        secure=request.url.scheme == "https" or forwarded_proto == "https",
        samesite="lax",
        path="/",
        max_age=60 * 60 * 8,
    )
    return {"userId": requested}


@router.get("/catalog")
def catalog(
    request: Request,
    q: str = Query(""),
    site: str = Query(""),
    offset: int = Query(0, ge=0),
    limit: int = Query(12, ge=1, le=50),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(jsonable_encoder(_call(lambda: service.source_sourcing.catalog(actor_from_request(request), query=q, site=site, offset=offset, limit=limit))))


@router.get("/catalog/{source_id}/access-context")
def access_context(source_id: str, request: Request, service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse(jsonable_encoder(_call(lambda: service.source_sourcing.access_context(actor_from_request(request), source_id))))


@router.post("/requests")
def create_request(payload: dict[str, Any], request: Request, service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse(jsonable_encoder(_call(lambda: service.source_sourcing.create_request(actor_from_request(request), payload))), status_code=201)


@router.get("/requests/mine")
def my_requests(request: Request, service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse(jsonable_encoder(_call(lambda: service.source_sourcing.my_requests(actor_from_request(request)))))


@router.get("/grants/mine")
def my_grants(request: Request, service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse(jsonable_encoder(_call(lambda: service.source_sourcing.grants(actor_from_request(request), refresh=True))))

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service
from .source_sourcing import actor_from_request


router = APIRouter(prefix="/api/data-sources", tags=["data-sources"])


@router.get("")
def data_sources(
    request: Request,
    q: str = Query("", max_length=200),
    technology: str = Query("", max_length=80),
    status: str = Query("", max_length=80),
    location: str = Query("", max_length=120),
    ingestion_capable: bool | None = Query(None, alias="ingestionCapable"),
    offset: int = Query(0, ge=0),
    limit: int = Query(25, ge=1, le=100),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            service.data_source_catalog(
                actor_from_request(request),
                query=q,
                technology=technology,
                status=status,
                location=location,
                ingestion_capable=ingestion_capable,
                offset=offset,
                limit=limit,
            )
        )
    )

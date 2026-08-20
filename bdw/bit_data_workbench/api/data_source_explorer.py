from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service
from .source_sourcing import actor_from_request


router = APIRouter(tags=["api"])


@router.get("/api/data-sources/{source_id}/explorer")
def data_source_explorer_payload(
    source_id: str,
    request: Request,
    bucket: str = Query(""),
    prefix: str = Query(""),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        kwargs = {"source_id": source_id, "bucket": bucket, "prefix": prefix}
        if source_id.startswith("ora_"):
            kwargs["actor"] = actor_from_request(request)
        payload = service.data_source_explorer_payload(**kwargs)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(payload))

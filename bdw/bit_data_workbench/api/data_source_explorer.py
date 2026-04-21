from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service


router = APIRouter(tags=["api"])


@router.get("/api/data-sources/{source_id}/explorer")
def data_source_explorer_payload(
    source_id: str,
    bucket: str = Query(""),
    prefix: str = Query(""),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        payload = service.data_source_explorer_payload(
            source_id=source_id,
            bucket=bucket,
            prefix=prefix,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(payload))

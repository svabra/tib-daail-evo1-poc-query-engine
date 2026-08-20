from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from ..backend.service import WorkbenchService
from ..backend.source_ingestions import SourceIngestionError
from ..dependencies import get_workbench_service
from .source_sourcing import actor_from_request


router = APIRouter(
    prefix="/api/ingestion/source-ingestions",
    tags=["source-ingestions"],
)


def _call(callback):
    try:
        return callback()
    except SourceIngestionError as exc:
        raise HTTPException(exc.status_code, exc.detail) from exc


@router.get("/context")
def context(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            _call(lambda: service.source_ingestion_context(actor_from_request(request)))
        )
    )


@router.get("")
def list_definitions(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            _call(lambda: service.list_source_ingestions(actor_from_request(request)))
        )
    )


@router.post("")
def create_definition(
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            _call(
                lambda: service.create_source_ingestion(
                    actor_from_request(request), payload
                )
            )
        ),
        status_code=201,
    )


@router.get("/{definition_id}")
def get_definition(
    definition_id: str,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            _call(
                lambda: service.source_ingestion(
                    actor_from_request(request), definition_id
                )
            )
        )
    )


@router.patch("/{definition_id}")
def patch_definition(
    definition_id: str,
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            _call(
                lambda: service.patch_source_ingestion(
                    actor_from_request(request), definition_id, payload
                )
            )
        )
    )


@router.get("/{definition_id}/runs")
def list_runs(
    definition_id: str,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    result = _call(
        lambda: service.source_ingestion(actor_from_request(request), definition_id)
    )
    return JSONResponse(jsonable_encoder({"items": result.get("runs", [])}))


@router.post("/{definition_id}/runs")
def start_run(
    definition_id: str,
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            _call(
                lambda: service.start_source_ingestion_run(
                    actor_from_request(request), definition_id, payload
                )
            )
        ),
        status_code=202,
    )


@router.put("/{definition_id}/schedule")
def update_schedule(
    definition_id: str,
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            _call(
                lambda: service.update_source_ingestion_schedule(
                    actor_from_request(request), definition_id, payload
                )
            )
        )
    )

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service


router = APIRouter(tags=["api"])
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))


@router.get("/info")
def info(service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse({"ok": True, "runtime": service.runtime_info()})


@router.post("/api/query", response_class=HTMLResponse)
def run_query(
    request: Request,
    sql: str = Form(""),
    notebook_id: str = Form(""),
    cell_id: str = Form(""),
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    result = service.execute_query(sql)
    return templates.TemplateResponse(
        request=request,
        name="partials/query_payload.html",
        context={
            "query_result": result,
            "cell_id": cell_id,
            "runtime": service.runtime_info(),
            "notebooks": service.notebooks(),
            "notebook_tree": service.notebook_tree(),
            "catalogs": service.catalogs(),
            "active_notebook_id": notebook_id,
        },
    )


@router.get("/api/source-object-fields")
def source_object_fields(
    relation: str = Query(""),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        fields = service.source_object_fields(relation)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return JSONResponse({"fields": [field.payload for field in fields]})


@router.get("/api/data-source-events")
def data_source_events_state(service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.data_source_events_state()))


@router.get("/api/data-generators")
def data_generators(service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse({"generators": jsonable_encoder(service.data_generators())})


@router.get("/api/data-generation-jobs")
def data_generation_jobs_state(service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.data_generation_jobs_state()))


@router.post("/api/data-generation-jobs")
def start_data_generation_job(
    generator_id: str = Form(""),
    size_gb: float = Form(1.0),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        snapshot = service.start_data_generation_job(
            generator_id=generator_id,
            size_gb=size_gb,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(snapshot))


@router.post("/api/data-generation-jobs/{job_id}/cancel")
def cancel_data_generation_job(
    job_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        snapshot = service.cancel_data_generation_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(snapshot))


@router.post("/api/data-generation-jobs/{job_id}/cleanup")
def cleanup_data_generation_job(
    job_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        snapshot = service.cleanup_data_generation_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(snapshot))


@router.post("/api/ingestion-cleanup/{target_id}")
def cleanup_ingestion_target(
    target_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        payload = service.cleanup_ingestion_target(target_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(payload))


@router.get("/api/data-generation-jobs/stream")
async def stream_data_generation_jobs(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> StreamingResponse:
    async def event_stream():
        last_version: int | None = None
        while True:
            if await request.is_disconnected():
                break

            snapshot = await asyncio.to_thread(
                service.wait_for_data_generation_jobs_state,
                last_version,
                15.0,
            )
            version = int(snapshot.get("version", 0))
            if last_version is None or version != last_version:
                last_version = version
                yield f"event: jobs\ndata: {json.dumps(jsonable_encoder(snapshot))}\n\n"
            else:
                yield "event: ping\ndata: {}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/api/data-source-events/stream")
async def stream_data_source_events(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> StreamingResponse:
    async def event_stream():
        last_version: int | None = None
        while True:
            if await request.is_disconnected():
                break

            snapshot = await asyncio.to_thread(
                service.wait_for_data_source_events_state,
                last_version,
                15.0,
            )
            version = int(snapshot.get("version", 0))
            if last_version is None or version != last_version:
                last_version = version
                yield f"event: sources\ndata: {json.dumps(jsonable_encoder(snapshot))}\n\n"
            else:
                yield "event: ping\ndata: {}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/api/query-jobs")
def query_jobs_state(service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.query_jobs_state()))


@router.post("/api/query-jobs")
def start_query_job(
    sql: str = Form(""),
    notebook_id: str = Form(""),
    notebook_title: str = Form(""),
    cell_id: str = Form(""),
    data_sources: str = Form(""),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        snapshot = service.start_query_job(
            sql=sql,
            notebook_id=notebook_id,
            notebook_title=notebook_title,
            cell_id=cell_id,
            data_sources=[source for source in data_sources.split("||") if source.strip()],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(snapshot))


@router.post("/api/query-jobs/{job_id}/cancel")
def cancel_query_job(
    job_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        snapshot = service.cancel_query_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(snapshot))


@router.get("/api/query-jobs/stream")
async def stream_query_jobs(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> StreamingResponse:
    async def event_stream():
        last_version: int | None = None
        while True:
            if await request.is_disconnected():
                break

            snapshot = await asyncio.to_thread(
                service.wait_for_query_jobs_state,
                last_version,
                15.0,
            )
            version = int(snapshot.get("version", 0))
            if last_version is None or version != last_version:
                last_version = version
                yield f"event: jobs\ndata: {json.dumps(jsonable_encoder(snapshot))}\n\n"
            else:
                yield "event: ping\ndata: {}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

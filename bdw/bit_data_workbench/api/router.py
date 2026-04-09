from __future__ import annotations

import asyncio
import json
from pathlib import Path
import shutil

from fastapi import APIRouter, Depends, Form, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from botocore.exceptions import BotoCoreError
from pydantic import BaseModel, Field
from botocore.exceptions import ClientError
from starlette.background import BackgroundTask

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service


router = APIRouter(tags=["api"])
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))


class NotebookCellPayload(BaseModel):
    cell_id: str = Field(alias="cellId")
    sql: str = ""
    data_sources: list[str] = Field(default_factory=list, alias="dataSources")


class NotebookVersionPayload(BaseModel):
    version_id: str = Field(alias="versionId")
    created_at: str = Field(alias="createdAt")
    title: str = ""
    summary: str = ""
    tags: list[str] = Field(default_factory=list)
    cells: list[NotebookCellPayload] = Field(default_factory=list)


class SharedNotebookUpsertPayload(BaseModel):
    notebook_id: str | None = Field(default=None, alias="notebookId")
    title: str = ""
    summary: str = ""
    tags: list[str] = Field(default_factory=list)
    tree_path: list[str] = Field(default_factory=list, alias="treePath")
    linked_generator_id: str = Field(default="", alias="linkedGeneratorId")
    created_at: str | None = Field(default=None, alias="createdAt")
    cells: list[NotebookCellPayload] = Field(default_factory=list)
    versions: list[NotebookVersionPayload] = Field(default_factory=list)


class S3BucketCreatePayload(BaseModel):
    bucket_name: str = Field(alias="bucketName")


class S3FolderCreatePayload(BaseModel):
    bucket: str = ""
    prefix: str = ""
    folder_name: str = Field(alias="folderName")


class S3ExplorerDeletePayload(BaseModel):
    entry_kind: str = Field(alias="entryKind")
    bucket: str = ""
    prefix: str = ""


class QueryResultS3ExportPayload(BaseModel):
    export_format: str = Field(alias="format")
    bucket: str = ""
    prefix: str = ""
    file_name: str = Field(default="", alias="fileName")


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


@router.get("/api/s3/explorer")
def s3_explorer_snapshot(
    bucket: str = Query(""),
    prefix: str = Query(""),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        snapshot = service.s3_explorer_snapshot(bucket=bucket, prefix=prefix)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(snapshot))


@router.post("/api/s3/explorer/buckets")
def create_s3_bucket(
    payload: S3BucketCreatePayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        result = service.create_s3_bucket(payload.bucket_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))


@router.post("/api/s3/explorer/folders")
def create_s3_folder(
    payload: S3FolderCreatePayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        result = service.create_s3_folder(
            bucket=payload.bucket,
            prefix=payload.prefix,
            folder_name=payload.folder_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))


@router.delete("/api/s3/explorer/entries")
def delete_s3_explorer_entry(
    payload: S3ExplorerDeletePayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        result = service.delete_s3_explorer_entry(
            entry_kind=payload.entry_kind,
            bucket=payload.bucket,
            prefix=payload.prefix,
        )
    except (ValueError, ClientError, BotoCoreError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))


@router.get("/api/s3/object/download")
def download_s3_object(
    bucket: str = Query(""),
    key: str = Query(""),
    file_name: str = Query(default="", alias="filename"),
    service: WorkbenchService = Depends(get_workbench_service),
) -> FileResponse:
    try:
        artifact = service.download_s3_object(bucket=bucket, key=key, file_name=file_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return FileResponse(
        path=artifact.local_path,
        media_type=artifact.content_type,
        filename=artifact.filename,
        background=BackgroundTask(shutil.rmtree, artifact.cleanup_dir, True),
    )


@router.get("/api/data-source-events")
def data_source_events_state(service: WorkbenchService = Depends(get_workbench_service)) -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.data_source_events_state()))


@router.post("/api/data-sources/{source_id}/connect")
def connect_data_source(
    source_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        payload = service.connect_data_source(source_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(payload))


@router.post("/api/data-sources/{source_id}/disconnect")
def disconnect_data_source(
    source_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        payload = service.disconnect_data_source(source_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(payload))


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


@router.get("/api/query-jobs/{job_id}/export/download")
def download_query_job_export(
    job_id: str,
    export_format: str = Query(alias="format"),
    service: WorkbenchService = Depends(get_workbench_service),
) -> FileResponse:
    try:
        artifact = service.download_query_result_export(job_id=job_id, export_format=export_format)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return FileResponse(
        path=artifact.local_path,
        media_type=artifact.export.content_type,
        filename=artifact.export.filename,
        background=BackgroundTask(shutil.rmtree, artifact.cleanup_dir, True),
    )


@router.post("/api/query-jobs/{job_id}/export/s3")
def save_query_job_export_to_s3(
    job_id: str,
    payload: QueryResultS3ExportPayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        result = service.save_query_result_export_to_s3(
            job_id=job_id,
            export_format=payload.export_format,
            bucket=payload.bucket,
            prefix=payload.prefix,
            file_name=payload.file_name,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))


@router.post("/api/notebooks/shared")
def upsert_shared_notebook(
    payload: SharedNotebookUpsertPayload,
    service: WorkbenchService = Depends(get_workbench_service),
    workbench_client_id: str | None = Header(default=None, alias="X-Workbench-Client-Id"),
) -> JSONResponse:
    try:
        result = service.upsert_shared_notebook(
            notebook_id=payload.notebook_id,
            title=payload.title,
            summary=payload.summary,
            tags=list(payload.tags),
            tree_path=list(payload.tree_path),
            linked_generator_id=payload.linked_generator_id,
            created_at=payload.created_at,
            cells=[cell.model_dump(by_alias=True) for cell in payload.cells],
            versions=[version.model_dump(by_alias=True) for version in payload.versions],
            origin_client_id=str(workbench_client_id or "").strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))


@router.delete("/api/notebooks/shared/{notebook_id}")
def delete_shared_notebook(
    notebook_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
    workbench_client_id: str | None = Header(default=None, alias="X-Workbench-Client-Id"),
) -> JSONResponse:
    try:
        result = service.delete_shared_notebook(
            notebook_id,
            origin_client_id=str(workbench_client_id or "").strip(),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return JSONResponse(jsonable_encoder(result))


@router.get("/api/notebooks/state")
def notebook_events_state(
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.notebook_events_state()))


@router.get("/api/events/stream")
async def stream_realtime_events(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
    query_jobs_version: int | None = Query(default=None, alias="queryJobsVersion"),
    data_generation_jobs_version: int | None = Query(
        default=None,
        alias="dataGenerationJobsVersion",
    ),
    data_source_events_version: int | None = Query(
        default=None,
        alias="dataSourceEventsVersion",
    ),
    notebook_events_version: int | None = Query(
        default=None,
        alias="notebookEventsVersion",
    ),
) -> StreamingResponse:
    async def event_stream():
        last_versions = {
            "query-jobs": query_jobs_version,
            "data-generation-jobs": data_generation_jobs_version,
            "data-source-events": data_source_events_version,
            "notebook-events": notebook_events_version,
        }
        yield "event: ready\ndata: {}\n\n"

        while True:
            if await request.is_disconnected():
                break

            updates = await asyncio.to_thread(
                service.wait_for_realtime_updates,
                last_versions,
                15.0,
            )
            if not updates:
                yield "event: ping\ndata: {}\n\n"
                continue

            for update in updates:
                topic = str(update.get("topic") or "").strip()
                snapshot = update.get("snapshot") or {}
                if not topic:
                    continue
                last_versions[topic] = int(
                    snapshot.get("version", last_versions.get(topic, 0))
                )
                yield (
                    f"event: {topic}\n"
                    f"data: {json.dumps(jsonable_encoder(snapshot))}\n\n"
                )

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

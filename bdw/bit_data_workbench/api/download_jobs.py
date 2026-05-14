from __future__ import annotations

from urllib.parse import quote

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from ..backend.download_jobs import DownloadRangeNotSatisfiable
from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service


router = APIRouter(tags=["download-jobs"])


class S3DownloadJobPayload(BaseModel):
    bucket: str
    key: str
    filename: str = ""
    file_format: str = Field(default="csv", validation_alias="format", serialization_alias="format")


def _api_error(error: Exception) -> HTTPException:
    if isinstance(error, PermissionError):
        return HTTPException(status_code=403, detail=str(error))
    if isinstance(error, KeyError):
        return HTTPException(status_code=404, detail=str(error))
    return HTTPException(status_code=400, detail=str(error))


@router.get("/api/download-jobs")
def list_download_jobs(
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.download_jobs_state()))


@router.get("/api/download-jobs/{job_id}")
def get_download_job(
    job_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        payload = service.download_job_state(job_id=job_id)
    except KeyError as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(payload))


@router.delete("/api/download-jobs/{job_id}")
def cancel_download_job(
    job_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        payload = service.cancel_download_job(job_id=job_id)
    except KeyError as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(payload))


@router.get("/api/download-jobs/{job_id}/artifact", response_model=None)
def download_job_artifact(
    job_id: str,
    token: str = Query(""),
    range_header: str = Header(default="", alias="Range"),
    service: WorkbenchService = Depends(get_workbench_service),
) -> Response:
    try:
        artifact = service.stream_download_job_artifact(
            job_id=job_id,
            token=token,
            range_header=range_header,
        )
    except DownloadRangeNotSatisfiable as exc:
        return Response(
            status_code=416,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Range": exc.content_range,
            },
        )
    except (PermissionError, KeyError, ValueError, ClientError, BotoCoreError) as exc:
        raise _api_error(exc) from exc

    def iter_body():
        try:
            while True:
                chunk = artifact.body.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            artifact.body.close()

    fallback_filename = (
        artifact.filename.encode("ascii", "ignore")
        .decode("ascii")
        .replace("\\", "_")
        .replace('"', "_")
        or "download.zip"
    )
    headers = {
        **artifact.headers,
        "Content-Disposition": (
            f"attachment; filename=\"{fallback_filename}\"; "
            f"filename*=UTF-8''{quote(artifact.filename, safe='')}"
        ),
    }
    return StreamingResponse(
        iter_body(),
        status_code=artifact.status_code,
        media_type=artifact.content_type,
        headers=headers,
    )


@router.post("/api/s3/download-jobs")
def create_s3_download_job(
    payload: S3DownloadJobPayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        job = service.start_s3_download_job(
            bucket=payload.bucket,
            key=payload.key,
            filename=payload.filename,
            file_format=payload.file_format,
        )
    except (ValueError, ClientError, BotoCoreError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(job))

from __future__ import annotations

from urllib.parse import quote

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service


router = APIRouter(tags=["data-exchange"])


class DataExchangeUploadSessionFilePayload(BaseModel):
    file_name: str = Field(validation_alias="fileName", serialization_alias="fileName")
    size_bytes: int = Field(validation_alias="sizeBytes", serialization_alias="sizeBytes")


class DataExchangeUploadSessionCreatePayload(BaseModel):
    files: list[DataExchangeUploadSessionFilePayload] = Field(default_factory=list)


class DataExchangeUploadSessionCompletePayload(BaseModel):
    file_password: str = Field(default="", validation_alias="filePassword", serialization_alias="filePassword")
    display_name: str = Field(default="", validation_alias="displayName", serialization_alias="displayName")
    description: str = ""
    owner_contact: str = Field(default="", validation_alias="ownerContact", serialization_alias="ownerContact")
    tags: list[str] = Field(default_factory=list)
    folder_id: str = Field(default="", validation_alias="folderId", serialization_alias="folderId")


class DataExchangeFilePasswordPayload(BaseModel):
    file_password: str = Field(default="", validation_alias="filePassword", serialization_alias="filePassword")


class DataExchangeMetadataPayload(DataExchangeFilePasswordPayload):
    display_name: str = Field(default="", validation_alias="displayName", serialization_alias="displayName")
    description: str = ""
    owner_contact: str = Field(default="", validation_alias="ownerContact", serialization_alias="ownerContact")
    tags: list[str] = Field(default_factory=list)
    folder_id: str | None = Field(default=None, validation_alias="folderId", serialization_alias="folderId")


class DataExchangeCopyToS3Payload(DataExchangeFilePasswordPayload):
    bucket: str = ""
    prefix: str = ""
    file_name: str = Field(default="", validation_alias="fileName", serialization_alias="fileName")


class DataExchangeFolderPayload(BaseModel):
    name: str
    parent_folder_id: str = Field(default="", validation_alias="parentFolderId", serialization_alias="parentFolderId")


def _api_error(error: Exception) -> HTTPException:
    if isinstance(error, PermissionError):
        return HTTPException(status_code=403, detail=str(error))
    if isinstance(error, KeyError):
        return HTTPException(status_code=404, detail=str(error))
    return HTTPException(status_code=400, detail=str(error))


@router.get("/api/data-exchange/files")
def list_data_exchange_files(
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        payload = service.data_exchange_files()
    except (PermissionError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(payload))


@router.post("/api/data-exchange/upload-sessions")
def create_data_exchange_upload_session(
    payload: DataExchangeUploadSessionCreatePayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        state = service.create_data_exchange_upload_session(
            files=[item.model_dump(by_alias=True) for item in payload.files],
        )
    except (PermissionError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(state))


@router.get("/api/data-exchange/upload-sessions/{session_id}")
def data_exchange_upload_session_state(
    session_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        state = service.data_exchange_upload_session_state(
            session_id=session_id,
        )
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(state))


@router.put("/api/data-exchange/upload-sessions/{session_id}/files/{file_id}/chunks/{chunk_index}")
async def append_data_exchange_upload_session_chunk(
    session_id: str,
    file_id: str,
    chunk_index: int,
    request: Request,
    content_range: str | None = Header(default=None, alias="Content-Range"),
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    if not content_range:
        raise HTTPException(
            status_code=411,
            detail="Content-Range is required for DataExchange upload chunks.",
        )
    try:
        state = service.append_data_exchange_upload_session_chunk(
            session_id=session_id,
            file_id=file_id,
            chunk_index=chunk_index,
            content_range=content_range,
            payload=await request.body(),
        )
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(state))


@router.post("/api/data-exchange/upload-sessions/{session_id}/complete")
def complete_data_exchange_upload_session(
    session_id: str,
    payload: DataExchangeUploadSessionCompletePayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        state = service.complete_data_exchange_upload_session(
            session_id=session_id,
            file_password=payload.file_password,
            display_name=payload.display_name,
            description=payload.description,
            owner_contact=payload.owner_contact,
            tags=payload.tags,
            folder_id=payload.folder_id,
        )
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(state))


@router.delete("/api/data-exchange/upload-sessions/{session_id}")
def cancel_data_exchange_upload_session(
    session_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        state = service.cancel_data_exchange_upload_session(
            session_id=session_id,
        )
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(state))


@router.patch("/api/data-exchange/files/{file_id}")
def update_data_exchange_file_metadata(
    file_id: str,
    payload: DataExchangeMetadataPayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        updated = service.update_data_exchange_file_metadata(
            file_id=file_id,
            file_password=payload.file_password,
            display_name=payload.display_name,
            description=payload.description,
            owner_contact=payload.owner_contact,
            tags=payload.tags,
            folder_id=payload.folder_id,
        )
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder({"file": updated}))


@router.delete("/api/data-exchange/files/{file_id}")
def delete_data_exchange_file(
    file_id: str,
    payload: DataExchangeFilePasswordPayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        result = service.delete_data_exchange_file(
            file_id=file_id,
            file_password=payload.file_password,
        )
    except (PermissionError, KeyError, ValueError, ClientError, BotoCoreError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(result))


@router.post("/api/data-exchange/folders")
def create_data_exchange_folder(
    payload: DataExchangeFolderPayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        folder = service.create_data_exchange_folder(
            name=payload.name,
            parent_folder_id=payload.parent_folder_id,
        )
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder({"folder": folder}))


@router.delete("/api/data-exchange/folders/{folder_id}")
def delete_data_exchange_folder(
    folder_id: str,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        result = service.delete_data_exchange_folder(folder_id=folder_id)
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(result))


@router.post("/api/data-exchange/files/{file_id}/download-token")
def create_data_exchange_download_token(
    file_id: str,
    payload: DataExchangeFilePasswordPayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        token = service.create_data_exchange_download_token(
            file_id=file_id,
            file_password=payload.file_password,
        )
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(token))


@router.get("/api/data-exchange/files/{file_id}/download")
def download_data_exchange_file(
    file_id: str,
    token: str = Query(""),
    service: WorkbenchService = Depends(get_workbench_service),
) -> StreamingResponse:
    try:
        artifact = service.stream_data_exchange_file(file_id=file_id, token=token)
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
        or "download"
    )
    headers = {
        "Content-Disposition": (
            f"attachment; filename=\"{fallback_filename}\"; "
            f"filename*=UTF-8''{quote(artifact.filename, safe='')}"
        )
    }
    if artifact.content_length is not None:
        headers["Content-Length"] = str(artifact.content_length)

    return StreamingResponse(
        iter_body(),
        media_type=artifact.content_type,
        headers=headers,
    )


@router.post("/api/data-exchange/files/{file_id}/copy-to-shared-s3")
def copy_data_exchange_file_to_shared_s3(
    file_id: str,
    payload: DataExchangeCopyToS3Payload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        result = service.copy_data_exchange_file_to_shared_s3(
            file_id=file_id,
            file_password=payload.file_password,
            bucket=payload.bucket,
            prefix=payload.prefix,
            file_name=payload.file_name,
        )
    except (PermissionError, KeyError, ValueError, ClientError, BotoCoreError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(result))


@router.post("/api/data-exchange/files/{file_id}/local-workspace-handoff")
def data_exchange_local_workspace_handoff(
    file_id: str,
    payload: DataExchangeFilePasswordPayload,
    service: WorkbenchService = Depends(get_workbench_service),
) -> JSONResponse:
    try:
        result = service.data_exchange_local_workspace_handoff(
            file_id=file_id,
            file_password=payload.file_password,
        )
    except (PermissionError, KeyError, ValueError) as exc:
        raise _api_error(exc) from exc
    return JSONResponse(jsonable_encoder(result))

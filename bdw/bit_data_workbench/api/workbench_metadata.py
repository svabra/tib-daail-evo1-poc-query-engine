from __future__ import annotations

from hashlib import sha256
import json

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse, Response

from ..backend.notebook_search import versioned_notebook_search_document
from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service
from ..release_notes import release_notes


router = APIRouter(tags=["workbench-metadata"])


def _etagged_json(
    payload: object,
    *,
    if_none_match: str | None = None,
    cache_control: str = "private, max-age=0, must-revalidate",
) -> Response:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    etag = f'"{sha256(canonical).hexdigest()[:16]}"'
    headers = {"Cache-Control": cache_control, "ETag": etag}
    if if_none_match and etag in {
        candidate.strip() for candidate in if_none_match.split(",")
    }:
        return Response(status_code=304, headers=headers)
    return JSONResponse(payload, headers=headers)


@router.get("/api/notebooks/search-index")
def notebook_search_index(
    service: WorkbenchService = Depends(get_workbench_service),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> Response:
    payload, version = versioned_notebook_search_document(service.notebooks())
    etag = f'"{version}"'
    headers = {
        "Cache-Control": "private, max-age=0, must-revalidate",
        "ETag": etag,
    }
    if if_none_match and etag in {
        candidate.strip() for candidate in if_none_match.split(",")
    }:
        return Response(status_code=304, headers=headers)
    return JSONResponse(payload, headers=headers)


@router.get("/api/workbench/source-options")
def source_options(
    service: WorkbenchService = Depends(get_workbench_service),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> Response:
    return _etagged_json(service.source_options(), if_none_match=if_none_match)


@router.get("/api/workbench/completion-schema")
def completion_schema(
    service: WorkbenchService = Depends(get_workbench_service),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> Response:
    return _etagged_json(service.completion_schema(), if_none_match=if_none_match)


@router.get("/api/workbench/release-notes")
def feature_release_notes(
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> Response:
    return _etagged_json(release_notes(), if_none_match=if_none_match)

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
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

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service
router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))


@router.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    notebooks = service.notebooks()
    active_notebook = notebooks[0] if notebooks else None
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "title": service.settings.ui_title,
            "runtime": service.runtime_info(),
            "catalogs": service.catalogs(),
            "notebooks": notebooks,
            "notebook_tree": service.notebook_tree(),
            "source_options": service.source_options(),
            "source_options_json": json.dumps(service.source_options()),
            "active_notebook_id": active_notebook.notebook_id if active_notebook else None,
            "active_notebook": active_notebook,
            "completion_schema_json": json.dumps(service.completion_schema()),
        },
    )


@router.get("/notebooks/{notebook_id}", response_class=HTMLResponse)
def notebook_workspace(
    notebook_id: str,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    try:
        notebook = service.notebook(notebook_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return templates.TemplateResponse(
        request=request,
        name="partials/workspace.html",
        context={
            "active_notebook": notebook,
            "source_options": service.source_options(),
        },
    )

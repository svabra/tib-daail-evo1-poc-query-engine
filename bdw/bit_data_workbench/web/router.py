from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service
router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))


def is_partial_request(request: Request) -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"


def brand_title_for_mode(workspace_mode: str) -> str:
    return (
        "DAAIFL Ingestion Workbench"
        if workspace_mode == "ingestion"
        else "DAAIFL Query Workbench"
    )


def shell_context(
    request: Request,
    service: WorkbenchService,
    *,
    active_notebook,
    workspace_mode: str,
    workspace_partial_template: str,
) -> dict[str, object]:
    return {
        "title": brand_title_for_mode(workspace_mode),
        "runtime": service.runtime_info(),
        "catalogs": service.catalogs(),
        "notebooks": service.notebooks(),
        "notebook_tree": service.notebook_tree(),
        "source_options": service.source_options(),
        "source_options_json": json.dumps(service.source_options()),
        "active_notebook_id": active_notebook.notebook_id if active_notebook else None,
        "active_notebook": active_notebook,
        "workspace_mode": workspace_mode,
        "workspace_partial_template": workspace_partial_template,
        "data_generators": service.data_generators(),
        "runbook_tree": service.runbook_tree(),
        "completion_schema_json": json.dumps(service.completion_schema()),
    }


@router.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context=shell_context(
            request,
            service,
            active_notebook=None,
            workspace_mode="notebook",
            workspace_partial_template="partials/home.html",
        ),
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

    if not is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context=shell_context(
                request,
                service,
                active_notebook=notebook,
                workspace_mode="notebook",
                workspace_partial_template="partials/workspace.html",
            ),
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/workspace.html",
        context={
            "active_notebook": notebook,
            "source_options": service.source_options(),
        },
    )


@router.get("/sidebar", response_class=HTMLResponse)
def sidebar_partial(
    request: Request,
    active_notebook_id: str | None = Query(default=None),
    mode: str = Query(default="notebook"),
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    workspace_mode = "ingestion" if mode == "ingestion" else "notebook"
    return templates.TemplateResponse(
        request=request,
        name="partials/sidebar.html",
        context={
            "sidebar_oob": False,
            "runtime": service.runtime_info(),
            "catalogs": service.catalogs(),
            "notebooks": service.notebooks(),
            "notebook_tree": service.notebook_tree(),
            "active_notebook_id": active_notebook_id,
            "workspace_mode": workspace_mode,
            "data_generators": service.data_generators(),
            "runbook_tree": service.runbook_tree(),
        },
    )


@router.get("/ingestion-workbench", response_class=HTMLResponse)
def ingestion_workbench_partial(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    if not is_partial_request(request):
        notebooks = service.notebooks()
        active_notebook = notebooks[0] if notebooks else None
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context=shell_context(
                request,
                service,
                active_notebook=active_notebook,
                workspace_mode="ingestion",
                workspace_partial_template="partials/ingestion_workbench.html",
            ),
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/ingestion_workbench.html",
        context={
            "data_generators": service.data_generators(),
            "runbook_tree": service.runbook_tree(),
        },
    )

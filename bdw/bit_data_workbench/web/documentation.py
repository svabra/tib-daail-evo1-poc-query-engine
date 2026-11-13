from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse

from ..backend.service import WorkbenchService
from ..data_generator.mwst_journey import COMPANY_SLUG, EXAMPLE_SLUG, NATIONAL_SLUG, NOTEBOOK_ID
from ..dependencies import get_workbench_service

router = APIRouter(include_in_schema=False)


@router.get("/api/documentation/config")
def documentation_config(service: WorkbenchService = Depends(get_workbench_service)):
    return {"dacaUiUrl": service.settings.daca_ui_url.rstrip("/")}


@router.get("/documentation", response_class=HTMLResponse)
@router.get("/documentation/journeys", response_class=HTMLResponse)
@router.get("/documentation/journeys/{journey_id}", response_class=HTMLResponse)
def documentation_page(
    request: Request,
    journey_id: str | None = None,
    service: WorkbenchService = Depends(get_workbench_service),
):
    from .router import shell_context, templates

    if journey_id and journey_id != NOTEBOOK_ID:
        raise HTTPException(404, "Journey nicht gefunden")
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            **shell_context(
                request,
                service,
                active_notebook=None,
                workspace_mode="notebook",
                workspace_partial_template="partials/documentation.html",
                shell_sidebar_hidden=True,
            ),
            "title": "DAAIF · Dokumentation",
            "journey_detail": bool(journey_id),
            "daca_ui_url": service.settings.daca_ui_url.rstrip("/"),
            "company_slug": COMPANY_SLUG,
            "national_slug": NATIONAL_SLUG,
            "example_slug": EXAMPLE_SLUG,
            "journey_id": NOTEBOOK_ID,
        },
    )

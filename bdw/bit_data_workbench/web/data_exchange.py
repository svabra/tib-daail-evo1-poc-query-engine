from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service


router = APIRouter(include_in_schema=False)


@router.get("/data-exchange", response_class=HTMLResponse)
def data_exchange_page(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    from .router import is_partial_request, shell_context, templates

    if is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="partials/data_exchange_workbench.html",
            context={},
        )

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            **shell_context(
                request,
                service,
                active_notebook=None,
                workspace_mode="notebook",
                workspace_partial_template="partials/data_exchange_workbench.html",
                shell_sidebar_hidden=True,
            ),
            "title": "DAAIFL DataExchange Workbench",
        },
    )

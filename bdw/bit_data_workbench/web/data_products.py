from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service


router = APIRouter(include_in_schema=False)


def request_base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def data_products_page_context(
    request: Request,
    service: WorkbenchService,
) -> dict[str, object]:
    return {
        "data_products": service.list_data_products(
            base_url=request_base_url(request)
        ),
        "data_product_sources_json": json.dumps(
            service.data_product_source_options()
        ),
    }


def public_data_products_context(
    request: Request,
    service: WorkbenchService,
) -> dict[str, object]:
    return {
        "runtime": service.runtime_info(),
        "data_products": service.list_data_products(
            base_url=request_base_url(request)
        ),
    }


def public_data_product_page_context(
    request: Request,
    service: WorkbenchService,
    slug: str,
) -> dict[str, object]:
    documentation = service.data_product_documentation(
        slug=slug,
        base_url=request_base_url(request),
    )
    return {
        "runtime": service.runtime_info(),
        "documentation": documentation,
        "product": documentation["product"],
        "request_parameters": documentation["requestParameters"],
    }


@router.get("/data-products", response_class=HTMLResponse)
def data_products_page(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    from .router import is_partial_request, shell_context, templates

    context = data_products_page_context(request, service)

    if is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="partials/data_products_workbench.html",
            context=context,
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
                workspace_partial_template="partials/data_products_workbench.html",
                shell_sidebar_hidden=True,
            ),
            "title": "DAAIFL Data Products Workbench",
            **context,
        },
    )


@router.get("/dataproducts", include_in_schema=False)
def public_data_products_redirect() -> RedirectResponse:
    return RedirectResponse(url="/dataproducts/", status_code=307)


@router.get("/dataproducts/", response_class=HTMLResponse)
def public_data_products_page(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    from .router import templates

    return templates.TemplateResponse(
        request=request,
        name="data_products_catalog.html",
        context=public_data_products_context(request, service),
    )


@router.get("/dataproducts/{slug}", response_class=HTMLResponse)
def public_data_product_page(
    slug: str,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    from .router import templates

    try:
        context = public_data_product_page_context(request, service, slug)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return templates.TemplateResponse(
        request=request,
        name="data_product_detail.html",
        context=context,
    )

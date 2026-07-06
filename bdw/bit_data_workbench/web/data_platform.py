from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi import HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

from .data_platform_content import (
    DATA_PLATFORM_TOPICS,
    DATA_PLATFORM_TOPIC_USE_CASES,
    get_data_platform_topic,
)


router = APIRouter(include_in_schema=False)


def _runtime_context() -> dict[str, str]:
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/the-data-platform", response_class=HTMLResponse)
def data_platform_page(
    request: Request,
) -> HTMLResponse:
    from .router import templates

    return templates.TemplateResponse(
        request=request,
        name="data_platform.html",
        context={
            "title": "DAAIF Factory - Data Platform BIT",
            "runtime": _runtime_context(),
            "topics": DATA_PLATFORM_TOPICS,
            "topic_use_cases": DATA_PLATFORM_TOPIC_USE_CASES,
        },
    )


@router.get("/the-data-platform/{topic_slug}", response_class=HTMLResponse)
def data_platform_topic_page(
    request: Request,
    topic_slug: str,
) -> HTMLResponse:
    topic = get_data_platform_topic(topic_slug)
    if topic is None:
        raise HTTPException(status_code=404, detail="Data platform topic not found")

    from .router import templates

    return templates.TemplateResponse(
        request=request,
        name="data_platform_topic.html",
        context={
            "title": f"DAAIF Factory - {topic.title} - Data Platform BIT",
            "runtime": _runtime_context(),
            "topic": topic,
            "topics": DATA_PLATFORM_TOPICS,
        },
    )


@router.get("/the-data-plattform", response_class=RedirectResponse)
def legacy_data_platform_page() -> RedirectResponse:
    return RedirectResponse(url="/the-data-platform", status_code=308)

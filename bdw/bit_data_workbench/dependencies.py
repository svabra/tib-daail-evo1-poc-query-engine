from __future__ import annotations

from fastapi import Request

from .backend.service import WorkbenchService


def get_workbench_service(request: Request) -> WorkbenchService:
    return request.app.state.workbench

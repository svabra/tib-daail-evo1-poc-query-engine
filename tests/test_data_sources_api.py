from __future__ import annotations

from pathlib import Path
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.api.data_sources import router  # noqa: E402
from bit_data_workbench.dependencies import get_workbench_service  # noqa: E402


class Service:
    def __init__(self) -> None:
        self.call = None

    def data_source_catalog(self, actor, **filters):
        self.call = (actor, filters)
        return {"summary": {"total": 0}, "facets": {}, "pagination": {}, "items": []}


def test_data_source_catalog_api_uses_cookie_actor_and_forwards_filters() -> None:
    service = Service()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_workbench_service] = lambda: service
    client = TestClient(app)
    client.cookies.set("daaif_demo_user", "joel.ruod")

    response = client.get(
        "/api/data-sources",
        params={
            "q": "tax",
            "technology": "postgresql",
            "status": "available",
            "location": "BIT",
            "ingestionCapable": "true",
            "offset": 25,
            "limit": 25,
        },
    )

    assert response.status_code == 200
    assert service.call == (
        "joel.ruod",
        {
            "query": "tax",
            "technology": "postgresql",
            "status": "available",
            "location": "BIT",
            "ingestion_capable": True,
            "offset": 25,
            "limit": 25,
        },
    )


def test_data_source_catalog_api_enforces_page_size_bounds() -> None:
    service = Service()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_workbench_service] = lambda: service
    client = TestClient(app)
    assert client.get("/api/data-sources?limit=101").status_code == 422

from __future__ import annotations

from pathlib import Path
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.api.source_ingestions import router  # noqa: E402
from bit_data_workbench.dependencies import get_workbench_service  # noqa: E402


class Service:
    def __init__(self) -> None:
        self.calls = []

    def source_ingestion_context(self, actor):
        self.calls.append(("context", actor))
        return {"actorId": actor, "sources": [], "visibleBuckets": ["target"]}

    def list_source_ingestions(self, actor):
        self.calls.append(("list", actor))
        return {"items": [], "runs": [], "summary": {}}

    def create_source_ingestion(self, actor, payload):
        self.calls.append(("create", actor, payload))
        return {"definition": {"id": "source-ingestion-1"}, "run": {"id": "run-1"}}

    def source_ingestion(self, actor, definition_id):
        self.calls.append(("get", actor, definition_id))
        return {"definition": {"id": definition_id}, "runs": [{"id": "run-1"}]}

    def patch_source_ingestion(self, actor, definition_id, payload):
        self.calls.append(("patch", actor, definition_id, payload))
        return {"id": definition_id, **payload}

    def start_source_ingestion_run(self, actor, definition_id, payload):
        self.calls.append(("run", actor, definition_id, payload))
        return {"id": "run-2", "status": "queued"}

    def update_source_ingestion_schedule(self, actor, definition_id, payload):
        self.calls.append(("schedule", actor, definition_id, payload))
        return {"definition": {"id": definition_id, "schedule": payload}}


def client_and_service():
    service = Service()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_workbench_service] = lambda: service
    client = TestClient(app)
    client.cookies.set("daaif_demo_user", "joel.ruod")
    return client, service


def test_source_ingestion_api_uses_validated_cookie_actor_and_never_body_actor() -> None:
    client, service = client_and_service()
    payload = {
        "actorId": "noemie.rochat",
        "clientRequestId": "client-1",
        "sourceId": "ora_bazg_zoll",
    }
    response = client.post("/api/ingestion/source-ingestions", json=payload)
    assert response.status_code == 201
    assert response.json()["definition"]["id"] == "source-ingestion-1"
    assert service.calls == [("create", "joel.ruod", payload)]


def test_source_ingestion_api_exposes_context_runs_patch_and_schedule_contracts() -> None:
    client, service = client_and_service()
    assert client.get("/api/ingestion/source-ingestions/context").status_code == 200
    assert client.get("/api/ingestion/source-ingestions").status_code == 200
    assert client.get("/api/ingestion/source-ingestions/source-ingestion-1").status_code == 200
    assert client.get("/api/ingestion/source-ingestions/source-ingestion-1/runs").json() == {
        "items": [{"id": "run-1"}]
    }
    assert client.patch(
        "/api/ingestion/source-ingestions/source-ingestion-1",
        json={"name": "New name"},
    ).status_code == 200
    assert client.post(
        "/api/ingestion/source-ingestions/source-ingestion-1/runs",
        json={"clientRequestId": "run-client"},
    ).status_code == 202
    assert client.put(
        "/api/ingestion/source-ingestions/source-ingestion-1/schedule",
        json={"enabled": True},
    ).status_code == 200
    assert all(call[1] == "joel.ruod" for call in service.calls)

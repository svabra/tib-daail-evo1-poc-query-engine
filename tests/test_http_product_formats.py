import json
from pathlib import Path
import sys

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bdw"))
from bit_data_workbench.backend.data_products.daca_client import DacaMetadataPublicationClient, DacaPublicationError

PRODUCT_ID = "11111111-1111-4111-8111-111111111111"
PUBLIC_PATH = "/api/public/data-products/test-tabular"


@pytest.mark.parametrize("conflict", [False, True])
def test_registers_four_formats_on_one_product_and_replays_without_changes(conflict):
    state = {"revision":1, "activePolicyRevision":7, "ownerUserId":"kassandra.valdata", "id":PRODUCT_ID}
    endpoints = [{"protocol":"http-rest", "connection":{"baseUrl":"http://127.0.0.1:8011", "path":PUBLIC_PATH, "method":"GET", "mediaType":"application/json"}}]
    writes = []
    conflict_pending = conflict
    def handler(request):
        nonlocal conflict_pending
        assert request.headers["X-DaCa-User"] == "kassandra.valdata"
        if request.method == "GET":
            return httpx.Response(200, json=endpoints if request.url.path.endswith("/endpoints") else state)
        assert request.headers["If-Match"] == f'"{state["revision"]}"'
        body = json.loads(request.content)
        if conflict_pending:
            conflict_pending = False
            endpoints.append(body)
            state["revision"] += 1
            return httpx.Response(412)
        assert not any(item["connection"]["path"] == body["connection"]["path"] for item in endpoints)
        writes.append(body)
        endpoints.append(body)
        state["revision"] += 1
        return httpx.Response(201, json=body)

    client = DacaMetadataPublicationClient(base_url="http://localhost:8080", transport=httpx.MockTransport(handler))
    args = dict(product_id=PRODUCT_ID,owner_user_id="kassandra.valdata",public_path=PUBLIC_PATH)
    client.ensure_http_formats(**args)
    assert len(endpoints) == 4
    assert {item["connection"]["mediaType"] for item in endpoints} == {"application/json", "text/csv; charset=utf-8", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.apache.parquet"}
    count = len(writes)
    client.ensure_http_formats(**args)
    assert len(writes) == count
    assert state["activePolicyRevision"] == 7
    assert all(item["protocol"] == "http-rest" for item in endpoints)


@pytest.mark.parametrize("failure", ["unavailable", "wrong_owner", "malformed"])
def test_format_sync_fails_before_any_endpoint_write(failure):
    requests = []
    def handler(request):
        requests.append(request)
        if failure == "unavailable": return httpx.Response(503)
        if failure == "malformed": return httpx.Response(200, text="not-json")
        return httpx.Response(200, json={"id":PRODUCT_ID,"ownerUserId":"jan.waser","revision":2})
    client = DacaMetadataPublicationClient(base_url="http://localhost:8080",transport=httpx.MockTransport(handler))
    with pytest.raises(DacaPublicationError):
        client.ensure_http_formats(product_id=PRODUCT_ID,owner_user_id="kassandra.valdata",public_path=PUBLIC_PATH)
    assert not any(request.method == "POST" for request in requests)

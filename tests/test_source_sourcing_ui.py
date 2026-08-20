from __future__ import annotations

from pathlib import Path
import sys

from starlette.requests import Request
from starlette.responses import Response


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.web.router import (  # noqa: E402
    ingestion_workbench_sourcing,
    ingestion_workbench_splitter,
)
from bit_data_workbench.api.source_sourcing import sync_identity  # noqa: E402


def request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"hx-request", b"true")],
            "query_string": b"",
            "server": ("testserver", 80),
            "client": ("test", 1),
            "scheme": "http",
            "root_path": "",
            "http_version": "1.1",
        }
    )


def identity_request(*, forwarded_proto: str = "") -> Request:
    headers = []
    if forwarded_proto:
        headers.append((b"x-forwarded-proto", forwarded_proto.encode()))
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/ingestion/sourcing/identity",
            "headers": headers,
            "query_string": b"",
            "server": ("testserver", 80),
            "client": ("test", 1),
            "scheme": "http",
            "root_path": "",
            "http_version": "1.1",
        }
    )


def test_ingestion_splitter_preserves_manual_flow_and_adds_governed_sourcing() -> None:
    response = ingestion_workbench_splitter(request("/ingestion-workbench"), service=object())
    body = response.body.decode()
    assert "Manual and Simple Data Ingestion" in body
    assert 'href="/ingestion-workbench/manual"' in body
    assert "Sophisticated and Scheduled Data Sourcing" in body
    assert 'href="/ingestion-workbench/sourcing"' in body


def test_sourcing_wizard_exposes_four_steps_and_no_client_actor_field() -> None:
    response = ingestion_workbench_sourcing(request("/ingestion-workbench/sourcing"), service=object())
    body = response.body.decode()
    assert body.count("data-sourcing-step-indicator") == 4
    assert "30 of 38" not in body  # live server summary, never a forged client constant
    assert "BIT Oracle RDBMS" in body
    assert "ESTV Business Intelligence" not in body  # trusted groups arrive from DaCa
    assert 'name="actor"' not in body


def test_wizard_script_uses_cookie_backed_proxy_pagination_and_three_second_poll() -> None:
    script = (BDW_ROOT / "bit_data_workbench/static/js/source-sourcing-wizard.js").read_text(encoding="utf-8")
    assert "/api/ingestion/sourcing/identity" in script
    assert "X-DaCa-User" not in script
    assert "const PAGE_SIZE = 12" in script
    assert "setInterval(pollStatus, 3000)" in script
    assert "crypto.randomUUID()" in script


def test_demo_identity_cookie_is_http_only_same_site_and_secure_behind_rhos_tls() -> None:
    response = Response()
    assert sync_identity(identity_request(forwarded_proto="https"), response, {"userId": "joel.ruod"}) == {
        "userId": "joel.ruod"
    }
    cookie = response.headers["set-cookie"]
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert "Secure" in cookie

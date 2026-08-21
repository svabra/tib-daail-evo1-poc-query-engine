from __future__ import annotations

from pathlib import Path
import sys

from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.web.router import (  # noqa: E402
    source_ingestion_detail,
    source_ingestion_new,
)


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


def test_source_ingestion_wizard_exposes_four_steps_and_one_shared_mode_switch() -> None:
    response = source_ingestion_new(
        request("/ingestion-workbench/sourcing/ingestions/new"), service=object()
    )
    body = response.body.decode()
    assert body.count("data-source-ingestion-step-indicator") == 4
    assert 'list="source-ingestion-visible-buckets"' in body
    assert "data-source-ingestion-buckets" in body
    assert "data-source-ingestion-create-bucket" in body
    assert "data-source-ingestion-bucket-status" in body
    assert 'value="once"' in body
    assert 'value="scheduled"' in body
    assert "Every hour" in body
    assert "Europe/Zurich" in body
    assert "Atomic Full Replace" in body
    assert "Parquet" in body
    assert "Oracle, PostgreSQL or S3" in body
    assert "Source &amp; Relation" in body
    assert 'name="sql"' not in body
    assert 'name="actor"' not in body


def test_source_ingestion_detail_preserves_run_history_and_schedule_controls() -> None:
    response = source_ingestion_detail(
        "source-ingestion-123",
        request("/ingestion-workbench/sourcing/ingestions/source-ingestion-123"),
        service=object(),
    )
    body = response.body.decode()
    assert 'data-source-ingestion-id="source-ingestion-123"' in body
    assert "Run now" in body
    assert "One-Time" in body
    assert "Scheduled" in body
    assert "Run history" in body


def test_source_ingestion_frontend_uses_actor_protected_api_and_server_generated_sql() -> None:
    script = (
        BDW_ROOT / "bit_data_workbench/static/js/source-ingestion-workbench.js"
    ).read_text(encoding="utf-8")
    assert 'const API_ROOT = "/api/ingestion/source-ingestions"' in script
    assert "X-DaCa-User" not in script
    assert "actorId:" not in script
    assert "sql:" not in script
    assert "resultStorage" not in script
    assert "activateAfterSuccessfulRun" in script
    assert "sourceId:" in script
    assert "relation:" in script
    assert "destination:" in script
    assert 'requestJson("/api/s3/explorer/buckets"' in script
    assert "normalizeS3BucketNameForCreate" in script
    assert "Create the S3 bucket" in script


def test_supported_source_entry_points_prefill_ingestion_context() -> None:
    management = (
        BDW_ROOT / "bit_data_workbench/templates/partials/data_source_management.html"
    ).read_text(encoding="utf-8")
    tree = (
        BDW_ROOT / "bit_data_workbench/templates/partials/source_tree.html"
    ).read_text(encoding="utf-8")
    assert "Create ingestion" in management
    assert "sourceId={{ selected_data_source.source_id" in management
    assert "Ingest to S3 ..." in tree
    assert "relationReference={{ source_object.relation" in tree


def test_sourcing_hub_reuses_the_shared_list_table_catalog_component() -> None:
    template = (
        BDW_ROOT / "bit_data_workbench/templates/partials/source_sourcing_hub.html"
    ).read_text(encoding="utf-8")
    script = (
        BDW_ROOT / "bit_data_workbench/static/js/source-ingestion-workbench.js"
    ).read_text(encoding="utf-8")
    shared = (
        BDW_ROOT / "bit_data_workbench/static/js/source-catalog.js"
    ).read_text(encoding="utf-8")
    assert 'data-source-ingestion-catalog-view="list"' in template
    assert 'data-source-ingestion-catalog-view="table"' in template
    assert "data-source-ingestion-sources" in template
    assert "renderSourceCatalog" in script
    assert 'const VIEW_STORAGE_KEY = "bdw.dataSources.viewMode"' in shared
    assert 'viewMode = "table"' in shared
    assert "sessionStorage" in shared
    assert "source-technology-icon" in shared


def test_data_source_explorer_reuses_the_shared_list_table_catalog_component() -> None:
    template = (
        BDW_ROOT / "bit_data_workbench/templates/partials/data_source_explorer.html"
    ).read_text(encoding="utf-8")
    controller = (
        BDW_ROOT
        / "bit_data_workbench/static/js/data-source-explorers/controller.js"
    ).read_text(encoding="utf-8")
    shared = (
        BDW_ROOT / "bit_data_workbench/static/js/source-catalog.js"
    ).read_text(encoding="utf-8")
    assert 'data-source-catalog-mode="browser"' in template
    assert 'data-source-catalog-view="list"' in template
    assert 'data-source-catalog-view="table"' in template
    assert "data-source-card-grid" not in template
    assert "<svg" not in template
    assert "initializeRemoteSourceCatalog(root)" in controller
    assert 'mode === "browser"' in shared
    assert "selectedSourceId" in shared

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..backend.service import WorkbenchService
from ..dependencies import get_workbench_service
from ..models import SourceCatalog
from ..release_notes import release_notes
from .data_sources import (
    data_source_explorer_context as build_data_source_explorer_context,
    data_source_management_context as build_data_source_management_context,
    home_data_source_context as build_home_data_source_context,
)
from .template_filters import register_template_filters
router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parents[1] / "templates")
)
register_template_filters(templates)


def is_partial_request(request: Request) -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"


def brand_title_for_mode(workspace_mode: str) -> str:
    if workspace_mode == "loader":
        return "DAAIFL Loader Workbench"
    if workspace_mode == "ingestion":
        return "DAAIFL Ingestion Workbench"
    return "DAAIFL Query Workbench"


def _display_value(value: object, fallback: str = "Not configured") -> str:
    if value is None:
        return fallback

    text = str(value).strip()
    return text or fallback


def _presence_label(
    value: object,
    *,
    configured_label: str = "Configured",
    missing_label: str = "Not configured",
) -> str:
    if value is None:
        return missing_label

    return configured_label if str(value).strip() else missing_label


def _source_metrics(catalog: SourceCatalog | None) -> tuple[int, int]:
    if catalog is None:
        return 0, 0

    schema_count = len(catalog.schemas)
    object_count = sum(len(schema.objects) for schema in catalog.schemas)
    return schema_count, object_count


def _source_status(
    catalog: SourceCatalog | None,
    *,
    configured: bool,
    schema_count: int,
    object_count: int,
) -> tuple[str, str, str]:
    if catalog is not None and catalog.connection_label:
        state = (catalog.connection_status or "").strip().lower()
        tone = "configured"
        if state in {"connected", "ready", "available", "ok"}:
            tone = "available"
        elif state in {"error", "failed"}:
            tone = "attention"

        detail = catalog.connection_detail
        if not detail and (schema_count or object_count):
            detail = (
                f"{schema_count} schema(s) and {object_count} "
                "discovered object(s) are currently visible."
            )
        return (
            tone,
            catalog.connection_label,
            detail or "Current source state reported by the workbench.",
        )

    if schema_count or object_count:
        return (
            "available",
            "Catalog available",
            f"{schema_count} schema(s) and {object_count} "
            "discovered object(s) are available in the catalog.",
        )

    if configured:
        return (
            "configured",
            "Configured",
            "Connection settings are present, but the source catalog has "
            "not been discovered yet.",
        )

    return (
        "muted",
        "Not configured",
        "This source type is shown for the PoC, but no active "
        "configuration is available right now.",
    )


def _postgres_source_record(
    *,
    source_id: str,
    name: str,
    label: str,
    source_type: str,
    execution_mode: str,
    catalog_name: str,
    database_name: str | None,
    summary: str,
    service: WorkbenchService,
    catalogs_by_name: dict[str, SourceCatalog],
) -> dict[str, object]:
    settings = service.settings
    catalog = catalogs_by_name.get(catalog_name)
    schema_count, object_count = _source_metrics(catalog)
    configured = any(
        (
            settings.pg_host,
            settings.pg_port,
            settings.pg_user,
            database_name,
        )
    )
    status_tone, status_label, status_detail = _source_status(
        catalog,
        configured=configured,
        schema_count=schema_count,
        object_count=object_count,
    )

    return {
        "source_id": source_id,
        "name": name,
        "label": label,
        "source_type": source_type,
        "kind": "postgres",
        "family": "PostgreSQL",
        "execution_mode": execution_mode,
        "catalog_name": catalog_name,
        "summary": summary,
        "configured": configured,
        "schema_count": schema_count,
        "object_count": object_count,
        "status_tone": status_tone,
        "status_label": status_label,
        "status_detail": status_detail,
        "summary_metrics": [
            {"label": "Type", "value": source_type},
            {"label": "Family", "value": "PostgreSQL"},
            {"label": "Execution", "value": execution_mode},
            {"label": "Schemas", "value": str(schema_count)},
            {"label": "Objects", "value": str(object_count)},
        ],
        "settings": [
            {"label": "FQDN", "value": _display_value(settings.pg_host)},
            {"label": "Port", "value": _display_value(settings.pg_port)},
            {"label": "Database", "value": _display_value(database_name)},
            {"label": "User", "value": _display_value(settings.pg_user)},
            {
                "label": "Password",
                "value": _presence_label(settings.pg_password),
                "hint": "Secrets stay masked in this PoC view.",
            },
            {"label": "Catalog", "value": catalog_name},
        ],
    }


def _s3_source_record(
    *,
    service: WorkbenchService,
    catalogs_by_name: dict[str, SourceCatalog],
) -> dict[str, object]:
    settings = service.settings
    catalog = catalogs_by_name.get("workspace")
    schema_count, object_count = _source_metrics(catalog)
    access_key = settings.current_s3_access_key_id()
    secret_key = settings.current_s3_secret_access_key()
    configured = any(
        (settings.s3_endpoint, settings.s3_bucket, access_key, secret_key)
    )
    status_tone, status_label, status_detail = _source_status(
        catalog,
        configured=configured,
        schema_count=schema_count,
        object_count=object_count,
    )
    effective_ca_bundle = settings.effective_s3_ca_cert_file()

    return {
        "source_id": "workspace.s3",
        "name": "Shared Workspace",
        "label": "MinIO / S3",
        "source_type": "Workspace Storage",
        "kind": "object-storage",
        "family": "MinIO / S3",
        "execution_mode": "DuckDB httpfs",
        "catalog_name": "workspace",
        "summary": (
            "Shared bucket-backed object storage surfaced into the workbench "
            "as queryable relations."
        ),
        "storage_tooltip": (
            "Stored in the configured MinIO / S3 bucket and shared through "
            "the workbench runtime."
        ),
        "configured": configured,
        "schema_count": schema_count,
        "object_count": object_count,
        "status_tone": status_tone,
        "status_label": status_label,
        "status_detail": status_detail,
        "summary_metrics": [
            {"label": "Type", "value": "Workspace Storage"},
            {"label": "Family", "value": "MinIO / S3"},
            {"label": "Execution", "value": "DuckDB httpfs"},
            {"label": "Schemas", "value": str(schema_count)},
            {"label": "Objects", "value": str(object_count)},
        ],
        "settings": [
            {
                "label": "Endpoint",
                "value": _display_value(settings.s3_endpoint),
            },
            {
                "label": "Default Bucket",
                "value": _display_value(settings.s3_bucket),
            },
            {
                "label": "URL Style",
                "value": _display_value(settings.s3_url_style, "path"),
            },
            {
                "label": "TLS",
                "value": "Enabled" if settings.s3_use_ssl else "Disabled",
            },
            {
                "label": "Certificate Verification",
                "value": "Enabled" if settings.s3_verify_ssl else "Disabled",
            },
            {
                "label": "CA Certificate",
                "value": _display_value(
                    effective_ca_bundle.as_posix()
                    if effective_ca_bundle is not None
                    else None,
                    "Automatic / not set",
                ),
            },
            {
                "label": "Access Key",
                "value": _presence_label(access_key),
                "hint": (
                    "Credentials are detected but never displayed in full."
                ),
            },
            {
                "label": "Secret Key",
                "value": _presence_label(secret_key),
                "hint": (
                    "Credentials are detected but never displayed in full."
                ),
            },
        ],
    }


def _local_browser_source_record(
    *,
    service: WorkbenchService,
    catalogs_by_name: dict[str, SourceCatalog],
) -> dict[str, object]:
    return {
        "source_id": "workspace.local",
        "name": "Local Workspace",
        "label": "IndexedDB",
        "source_type": "Workspace Storage",
        "kind": "local-browser",
        "family": "IndexedDB",
        "execution_mode": "Browser-managed",
        "catalog_name": "browser-local",
        "summary": (
            "Reference model for browser-local scratch work, uploaded JSON "
            "and Parquet files, and temporary intermediate results."
        ),
        "storage_tooltip": (
            "Stored in this browser profile under this app's origin using "
            "IndexedDB. Nothing is shared unless you export it."
        ),
        "configured": False,
        "schema_count": 0,
        "object_count": 0,
        "status_tone": "configured",
        "status_label": "Reference",
        "status_detail": (
            "If temporary work moves into the browser, IndexedDB is the "
            "right reference storage model for larger local datasets and "
            "file-like artifacts."
        ),
        "summary_metrics": [
            {"label": "Type", "value": "Workspace Storage"},
            {"label": "Family", "value": "IndexedDB"},
            {"label": "Execution", "value": "Browser-managed"},
            {"label": "Schemas", "value": "0"},
            {"label": "Objects", "value": "0"},
        ],
        "settings": [
            {
                "label": "Storage Backend",
                "value": "IndexedDB",
            },
            {
                "label": "Scope",
                "value": "Current browser profile and application origin",
            },
            {
                "label": "Persistence",
                "value": "Browser quota-managed persistent storage",
            },
            {
                "label": "Suitable Payloads",
                "value": "Scratch tables, JSON, Parquet, cached previews",
            },
            {
                "label": "Sharing",
                "value": (
                    "Local to the current browser unless explicitly "
                    "exported"
                ),
            },
            {
                "label": "Cleanup",
                "value": "Clear site data or run a workspace reset in the UI",
            },
        ],
    }


def data_source_management_context(
    service: WorkbenchService,
    selected_source_id: str | None,
) -> dict[str, object]:
    catalogs_by_name = {
        catalog.name: catalog for catalog in service.catalogs()
    }
    sources = [
        _local_browser_source_record(
            service=service,
            catalogs_by_name=catalogs_by_name,
        ),
        _s3_source_record(service=service, catalogs_by_name=catalogs_by_name),
        _postgres_source_record(
            source_id="pg_oltp",
            name="OLTP",
            label="PostgreSQL OLTP",
            source_type="RDBMS",
            execution_mode="VMTP",
            catalog_name="pg_oltp",
            database_name=service.settings.pg_oltp_database,
            summary=(
                "Transactional PostgreSQL catalog exposed through the "
                "workbench query path."
            ),
            service=service,
            catalogs_by_name=catalogs_by_name,
        ),
        _postgres_source_record(
            source_id="pg_oltp_native",
            name="OLTP Direct",
            label="PostgreSQL Native",
            source_type="RDBMS",
            execution_mode="PostgreSQL Native",
            catalog_name="pg_oltp",
            database_name=service.settings.pg_oltp_database,
            summary=(
                "Direct PostgreSQL execution path for the OLTP source "
                "without the VMTP indirection."
            ),
            service=service,
            catalogs_by_name=catalogs_by_name,
        ),
        _postgres_source_record(
            source_id="pg_olap",
            name="OLAP",
            label="PostgreSQL OLAP",
            source_type="RDBMS",
            execution_mode="VMTP",
            catalog_name="pg_olap",
            database_name=service.settings.pg_olap_database,
            summary=(
                "Analytical PostgreSQL catalog intended for heavier "
                "read-oriented workloads."
            ),
            service=service,
            catalogs_by_name=catalogs_by_name,
        ),
    ]

    selected_source = next(
        (
            source for source in sources
            if source["source_id"] == selected_source_id
        ),
        None,
    )
    if selected_source is None and sources:
        selected_source = next(
            (source for source in sources if source["configured"]),
            sources[0],
        )

    return {
        "data_sources": sources,
        "selected_data_source": selected_source,
    }


def home_data_source_context(service: WorkbenchService) -> dict[str, object]:
    data_sources = data_source_management_context(
        service,
        None,
    )["data_sources"]
    home_data_sources = [
        source
        for source in data_sources
        if source["status_tone"] == "available"
        or source["source_id"] == "workspace.local"
    ]
    return {
        "home_data_sources": home_data_sources,
    }


def shell_context(
    request: Request,
    service: WorkbenchService,
    *,
    active_notebook,
    workspace_mode: str,
    workspace_partial_template: str,
    shell_sidebar_hidden: bool = False,
    shared_notebooks: list | None = None,
) -> dict[str, object]:
    return {
        "title": brand_title_for_mode(workspace_mode),
        "runtime": service.runtime_info(),
        "catalogs": service.catalogs(),
        "notebooks": service.notebooks(),
        "notebook_tree": service.notebook_tree(),
        "source_options": service.source_options(),
        "source_options_json": json.dumps(service.source_options()),
        "release_notes_json": json.dumps(release_notes()),
        "active_notebook_id": (
            active_notebook.notebook_id if active_notebook else None
        ),
        "active_notebook": active_notebook,
        "workspace_mode": workspace_mode,
        "workspace_partial_template": workspace_partial_template,
        "shell_sidebar_hidden": shell_sidebar_hidden,
        "shared_notebooks": shared_notebooks
        if shared_notebooks is not None
        else [notebook for notebook in service.notebooks() if notebook.shared],
        "data_generators": service.data_generators(),
        "runbook_tree": service.runbook_tree(),
        "completion_schema_json": json.dumps(service.completion_schema()),
    }


def service_consumption_page_context() -> dict[str, object]:
    return {
        "service_consumption_default_window": "24h",
        "service_consumption_budget_year": datetime.now(UTC).year,
    }


@router.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    if is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="partials/home.html",
            context=build_home_data_source_context(service),
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
                workspace_partial_template="partials/home.html",
                shell_sidebar_hidden=True,
            ),
            "title": "DAAIFL Workbench",
            **build_home_data_source_context(service),
        },
    )


@router.get("/query-workbench", response_class=HTMLResponse)
def query_workbench_entry(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    shared_notebooks = sorted(
        (notebook for notebook in service.notebooks() if notebook.shared),
        key=lambda notebook: (notebook.title.lower(), notebook.notebook_id),
    )

    if is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="partials/query_workbench_entry.html",
            context={
                "catalogs": service.catalogs(),
                "notebook_tree": service.notebook_tree(),
                "shared_notebooks": shared_notebooks,
            },
        )

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context=shell_context(
            request,
            service,
            active_notebook=None,
            workspace_mode="notebook",
            workspace_partial_template="partials/query_workbench_entry.html",
            shell_sidebar_hidden=True,
            shared_notebooks=shared_notebooks,
        ),
    )


@router.get("/query-workbench/data-sources", response_class=HTMLResponse)
def query_workbench_data_sources(
    request: Request,
    source_id: str | None = Query(default=None),
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    context = build_data_source_management_context(service, source_id)

    if is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="partials/data_source_management.html",
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
                workspace_partial_template=(
                    "partials/data_source_management.html"
                ),
                shell_sidebar_hidden=True,
            ),
            "title": "DAAIFL Data Source Workbench",
            **context,
        },
    )


@router.get("/query-workbench/data-sources/explorer", response_class=HTMLResponse)
def query_workbench_data_source_explorer(
    request: Request,
    source_id: str | None = Query(default=None),
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    context = build_data_source_explorer_context(service, source_id)

    if is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="partials/data_source_explorer.html",
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
                workspace_partial_template=(
                    "partials/data_source_explorer.html"
                ),
                shell_sidebar_hidden=True,
            ),
            "title": "DAAIFL Data Source Workbench",
            **context,
        },
    )


@router.get("/service-consumption", response_class=HTMLResponse)
def service_consumption_page(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    context = service_consumption_page_context()

    if is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="partials/service_consumption.html",
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
                workspace_partial_template="partials/service_consumption.html",
                shell_sidebar_hidden=True,
            ),
            "title": "DAAIFL Service Consumption",
            **context,
        },
    )


@router.get("/notebooks/{notebook_id}", response_class=HTMLResponse)
def notebook_workspace(
    notebook_id: str,
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    try:
        notebook = service.notebook(notebook_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context=shell_context(
                request,
                service,
                active_notebook=notebook,
                workspace_mode="notebook",
                workspace_partial_template="partials/workspace.html",
            ),
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/workspace.html",
        context={
            "active_notebook": notebook,
            "source_options": service.source_options(),
        },
    )


@router.get("/sidebar", response_class=HTMLResponse)
def sidebar_partial(
    request: Request,
    active_notebook_id: str | None = Query(default=None),
    mode: str = Query(default="notebook"),
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    workspace_mode = "loader" if mode == "loader" else "notebook"
    return templates.TemplateResponse(
        request=request,
        name="partials/sidebar.html",
        context={
            "sidebar_oob": False,
            "runtime": service.runtime_info(),
            "catalogs": service.catalogs(),
            "notebooks": service.notebooks(),
            "notebook_tree": service.notebook_tree(),
            "active_notebook_id": active_notebook_id,
            "workspace_mode": workspace_mode,
            "data_generators": service.data_generators(),
            "runbook_tree": service.runbook_tree(),
        },
    )


@router.get("/ingestion-workbench", response_class=HTMLResponse)
def ingestion_workbench_partial(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    if not is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context=shell_context(
                request,
                service,
                active_notebook=None,
                workspace_mode="ingestion",
                workspace_partial_template="partials/ingestion_workbench.html",
                shell_sidebar_hidden=True,
            ),
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/ingestion_workbench.html",
        context={},
    )


@router.get("/loader-workbench", response_class=HTMLResponse)
def loader_workbench_partial(
    request: Request,
    service: WorkbenchService = Depends(get_workbench_service),
) -> HTMLResponse:
    if not is_partial_request(request):
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context=shell_context(
                request,
                service,
                active_notebook=None,
                workspace_mode="loader",
                workspace_partial_template="partials/loader_workbench.html",
            ),
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/loader_workbench.html",
        context={
            "data_generators": service.data_generators(),
            "runbook_tree": service.runbook_tree(),
        },
    )


from .data_products import router as data_products_router


router.include_router(data_products_router)

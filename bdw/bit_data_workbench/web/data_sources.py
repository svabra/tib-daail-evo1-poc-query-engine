from __future__ import annotations

from ..backend.service import WorkbenchService
from ..backend.data_sources.explorer_payloads import (
    canonical_explorer_source_id,
    explorer_kind_for_source,
)
from ..backend.data_source_catalog import canonical_data_source_id
from ..models import SourceCatalog
from .source_tree import build_source_tree_s3_hierarchy


DATA_SOURCES_PATH = "/data-sources"
DATA_SOURCES_BROWSER_PATH = "/data-sources/browser"


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


def management_path_for_source(source_id: str) -> str:
    normalized_source_id = str(source_id or "").strip()
    if not normalized_source_id:
        return DATA_SOURCES_PATH
    return f"{DATA_SOURCES_PATH}?source_id={normalized_source_id}"


def explorer_path_for_source(source_id: str) -> str:
    normalized_source_id = str(source_id or "").strip()
    if not normalized_source_id:
        return DATA_SOURCES_BROWSER_PATH
    return f"{DATA_SOURCES_BROWSER_PATH}?source_id={normalized_source_id}"


def browse_path_for_source(source_id: str) -> str:
    normalized_source_id = str(source_id or "").strip()
    if not normalized_source_id:
        return f"{DATA_SOURCES_PATH}?browse=1"
    return f"{DATA_SOURCES_PATH}?source_id={normalized_source_id}&browse=1"


def explorer_copy_for_source(source_kind: str) -> str:
    normalized_kind = str(source_kind or "").strip().lower()
    if normalized_kind == "postgres":
        return "Open the schema and relation explorer for this PostgreSQL source."
    if normalized_kind == "object-storage":
        return "Browse buckets, prefixes, and files in Shared Workspace."
    if normalized_kind == "local-browser":
        return "Browse folders and files stored in this browser workspace."
    if normalized_kind == "oracle-poc":
        return "Browse the granted Oracle schemas and synthetic PoC relations."
    return "Open the explorer for this data source."


def _attach_source_navigation(record: dict[str, object]) -> dict[str, object]:
    source_id = str(record.get("source_id") or "").strip()
    explorer_source_id = canonical_explorer_source_id(source_id)
    record["management_path"] = management_path_for_source(source_id)
    record["explorer_path"] = explorer_path_for_source(source_id)
    record["browse_path"] = browse_path_for_source(source_id)
    record["explorer_source_id"] = explorer_source_id
    record["explorer_kind"] = explorer_kind_for_source(explorer_source_id)
    record["explorer_cta_copy"] = explorer_copy_for_source(
        str(record.get("kind") or "")
    )
    return record


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

    access_paths = "VMTP · Native" if source_id == "pg_oltp" else execution_mode
    return _attach_source_navigation(
        {
            "source_id": source_id,
            "name": name,
            "label": label,
            "source_type": source_type,
            "kind": "postgres",
            "family": "PostgreSQL",
            "execution_mode": access_paths,
            "catalog_name": catalog_name,
            "summary": summary,
            "configured": configured,
            "schema_count": schema_count,
            "object_count": object_count,
            "status_tone": status_tone,
            "status_label": status_label,
            "status_detail": status_detail,
            "technology_key": "postgresql",
            "icon_path": "/static/img/source-icons/postgresql.svg",
            "access_model": "Platform source · runtime access",
            "ingestion_capable": status_tone == "available" and object_count > 0,
            "summary_metrics": [
                {"label": "Type", "value": source_type},
                {"label": "Family", "value": "PostgreSQL"},
                {"label": "Access paths", "value": access_paths},
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
    )


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

    return _attach_source_navigation(
        {
            "source_id": "s3",
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
            "technology_key": "s3",
            "icon_path": "/static/img/source-icons/s3.svg",
            "access_model": "Platform source · runtime access",
            "ingestion_capable": status_tone == "available" and object_count > 0,
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
                    "value": (
                        "Enabled" if settings.s3_verify_ssl else "Disabled"
                    ),
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
    )


def _local_browser_source_record(
    *,
    service: WorkbenchService,
    catalogs_by_name: dict[str, SourceCatalog],
) -> dict[str, object]:
    return _attach_source_navigation(
        {
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
            "technology_key": "local",
            "icon_path": "/static/img/source-icons/local.svg",
            "access_model": "Browser-local access",
            "ingestion_capable": False,
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
    )


def _oracle_source_record(catalog: SourceCatalog) -> dict[str, object]:
    schema_count, object_count = _source_metrics(catalog)
    return _attach_source_navigation(
        {
            "source_id": catalog.connection_source_id or catalog.name,
            "name": catalog.display_name or catalog.name,
            "label": catalog.database_name or catalog.name,
            "source_type": "RDBMS",
            "kind": "oracle-poc",
            "family": "BIT Oracle RDBMS",
            "execution_mode": "Oracle PoC via DuckDB",
            "catalog_name": catalog.name,
            "summary": "Governed Oracle source granted by DaCa and simulated read-only with synthetic PoC data.",
            "configured": True,
            "schema_count": schema_count,
            "object_count": object_count,
            "status_tone": "available",
            "status_label": "Grant active",
            "status_detail": catalog.connection_detail or "Active DaCa source grant.",
            "technology_key": "oracle",
            "icon_path": "/static/img/source-icons/oracle.svg",
            "access_model": "DaCa grant · checked per run",
            "ingestion_capable": object_count > 0,
            "summary_metrics": [
                {"label": "Technology", "value": "BIT Oracle RDBMS"},
                {"label": "Location", "value": catalog.site_label or "BIT data center"},
                {"label": "Schemas", "value": str(schema_count)},
                {"label": "Objects", "value": str(object_count)},
                {"label": "Access", "value": "DaCa grant"},
            ],
            "settings": [
                {"label": "Database", "value": catalog.database_name or catalog.name},
                {"label": "Data Owner", "value": catalog.owner_label or "Governed in DaCa"},
                {"label": "Runtime", "value": "DuckDB-generated synthetic relations"},
                {"label": "Credentials", "value": "None stored", "hint": "This is an explicitly labelled PoC simulation."},
                {"label": "Query mode", "value": "Read-only"},
            ],
        }
    )


def data_source_management_context(
    service: WorkbenchService,
    selected_source_id: str | None,
    actor: str = "",
) -> dict[str, object]:
    actor_catalogs = getattr(service, "catalogs_for_actor", None)
    catalogs = actor_catalogs(actor) if actor and callable(actor_catalogs) else service.catalogs()
    catalogs_by_name = {catalog.name: catalog for catalog in catalogs}
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
    sources.extend(
        _oracle_source_record(catalog)
        for catalog in catalogs
        if str(catalog.connection_source_id or catalog.name).startswith("ora_")
    )

    for source in sources:
        kind = str(source.get("kind") or "").strip()
        if kind == "oracle-poc":
            location_label = next(
                (
                    str(metric.get("value") or "").strip()
                    for metric in source.get("summary_metrics", [])
                    if metric.get("label") == "Location"
                ),
                "BIT data center",
            )
        elif kind == "object-storage":
            location_label = "BIT object storage"
        elif kind == "local-browser":
            location_label = "Current browser"
        else:
            location_label = "BIT data platform"

        source["location_label"] = location_label
        source["last_checked_label"] = "Current runtime"
        source["search_text"] = " ".join(
            str(source.get(field) or "")
            for field in (
                "name",
                "label",
                "source_type",
                "family",
                "summary",
                "status_label",
                "location_label",
            )
        ).casefold()

    source_summary = {
        "total": len(sources),
        "available": sum(
            1 for source in sources if source.get("status_tone") == "available"
        ),
        "schemas": sum(int(source.get("schema_count") or 0) for source in sources),
        "objects": sum(int(source.get("object_count") or 0) for source in sources),
    }
    source_technologies = sorted(
        {
            str(source.get("family") or source.get("source_type") or "Other").strip()
            for source in sources
        },
        key=str.casefold,
    )

    normalized_selected_source_id = canonical_data_source_id(selected_source_id)
    selected_source = next(
        (
            source
            for source in sources
            if source["source_id"] == normalized_selected_source_id
        ),
        None,
    )

    inline_catalog_source_id = (
        str(selected_source.get("explorer_source_id") or "").strip()
        if selected_source
        else ""
    )
    inline_browse_catalogs = [
        catalog
        for catalog in catalogs
        if (catalog.connection_source_id or catalog.name) == inline_catalog_source_id
        or catalog.name == inline_catalog_source_id
    ]
    inline_source_option_id = (
        "pg_oltp_native" if selected_source_id == "pg_oltp_native" else ""
    )

    return {
        "data_sources": sources,
        "source_summary": source_summary,
        "source_technologies": source_technologies,
        "selected_data_source": selected_source,
        "inline_browse_catalogs": inline_browse_catalogs,
        "inline_source_tree_s3_hierarchy": build_source_tree_s3_hierarchy(
            inline_browse_catalogs
        ),
        "inline_source_option_id": inline_source_option_id,
    }


def data_source_explorer_context(
    service: WorkbenchService,
    selected_source_id: str | None,
    actor: str = "",
) -> dict[str, object]:
    return data_source_management_context(service, selected_source_id, actor)


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

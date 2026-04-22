from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class SourceField:
    name: str
    data_type: str

    @property
    def payload(self) -> dict[str, str]:
        return {
            "name": self.name,
            "dataType": self.data_type,
        }


@dataclass(slots=True)
class SourceObject:
    name: str
    kind: str
    relation: str
    display_name: str = ""
    s3_bucket: str = ""
    s3_key: str = ""
    s3_path: str = ""
    s3_file_format: str = ""
    s3_downloadable: bool = False
    size_bytes: int = 0
    published_data_products: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class SourceSchema:
    name: str
    label: str | None = None
    objects: list[SourceObject] = field(default_factory=list)
    published_data_products: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class SourceConnectionStatus:
    source_id: str
    state: str
    label: str
    detail: str | None = None
    checked_at: str | None = None

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "sourceId": self.source_id,
            "state": self.state,
            "label": self.label,
            "detail": self.detail,
            "checkedAt": self.checked_at,
        }


@dataclass(slots=True)
class SourceCatalog:
    name: str
    connection_source_id: str | None = None
    schemas: list[SourceSchema] = field(default_factory=list)
    connection_status: str | None = None
    connection_label: str | None = None
    connection_detail: str | None = None
    connection_controls_enabled: bool = False


@dataclass(slots=True)
class DataProductSourceDescriptor:
    source_kind: str
    source_id: str
    relation: str = ""
    bucket: str = ""
    key: str = ""
    source_display_name: str = ""
    source_platform: str = ""
    unsupported_reason: str = ""

    @property
    def payload(self) -> dict[str, str]:
        return {
            "sourceKind": self.source_kind,
            "sourceId": self.source_id,
            "relation": self.relation,
            "bucket": self.bucket,
            "key": self.key,
            "sourceDisplayName": self.source_display_name,
            "sourcePlatform": self.source_platform,
            "unsupportedReason": self.unsupported_reason,
        }


@dataclass(slots=True)
class DataProductDefinition:
    product_id: str
    slug: str
    title: str
    description: str
    source: DataProductSourceDescriptor
    public_path: str
    publication_mode: str = "live"
    owner: str = ""
    domain: str = ""
    tags: list[str] = field(default_factory=list)
    access_level: str = "internal"
    access_note: str = ""
    request_access_contact: str = ""
    custom_properties: dict[str, str] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def published_url(self, base_url: str | None = None) -> str:
        normalized_base_url = str(base_url or "").strip().rstrip("/")
        if not normalized_base_url:
            return self.public_path
        return f"{normalized_base_url}{self.public_path}"

    def documentation_path(self) -> str:
        return f"/dataproducts/{self.slug}"

    def documentation_url(self, base_url: str | None = None) -> str:
        normalized_base_url = str(base_url or "").strip().rstrip("/")
        if not normalized_base_url:
            return self.documentation_path()
        return f"{normalized_base_url}{self.documentation_path()}"

    def payload(self, *, base_url: str | None = None) -> dict[str, Any]:
        return {
            "productId": self.product_id,
            "slug": self.slug,
            "title": self.title,
            "description": self.description,
            **self.source.payload,
            "publicPath": self.public_path,
            "publishedUrl": self.published_url(base_url),
            "documentationPath": self.documentation_path(),
            "documentationUrl": self.documentation_url(base_url),
            "publicationMode": self.publication_mode,
            "owner": self.owner,
            "domain": self.domain,
            "tags": list(self.tags),
            "accessLevel": self.access_level,
            "accessNote": self.access_note,
            "requestAccessContact": self.request_access_contact,
            "customProperties": dict(self.custom_properties),
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }


@dataclass(slots=True)
class DataSourceDiscoveryEventDefinition:
    event_id: str
    source_type: str
    source_id: str
    source_label: str
    detected_at: str
    message: str
    added_relations: list[str] = field(default_factory=list)
    removed_relations: list[str] = field(default_factory=list)
    updated_relations: list[str] = field(default_factory=list)

    @property
    def total_changes(self) -> int:
        return len(self.added_relations) + len(self.removed_relations) + len(self.updated_relations)

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "eventId": self.event_id,
            "sourceType": self.source_type,
            "sourceId": self.source_id,
            "sourceLabel": self.source_label,
            "detectedAt": self.detected_at,
            "message": self.message,
            "addedRelations": list(self.added_relations),
            "removedRelations": list(self.removed_relations),
            "updatedRelations": list(self.updated_relations),
            "totalChanges": self.total_changes,
        }


@dataclass(slots=True)
class NotebookCellDefinition:
    cell_id: str
    sql: str
    language: str = "sql"
    data_sources: list[str] = field(default_factory=list)

    @property
    def access_mode(self) -> str:
        return "Read / Query only" if len(self.data_sources) > 1 else "Read / Write"

    @property
    def access_mode_hint(self) -> str:
        if len(self.data_sources) > 1:
            return "Multiple selected sources keep this cell in query-only mode."
        return "A single selected source keeps this cell read/write capable."

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "cellId": self.cell_id,
            "sql": self.sql,
            "language": self.language,
            "dataSources": list(self.data_sources),
        }


@dataclass(slots=True)
class NotebookVersionDefinition:
    version_id: str
    created_at: str
    title: str
    summary: str
    tags: list[str] = field(default_factory=list)
    cells: list[dict[str, Any]] = field(default_factory=list)

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "versionId": self.version_id,
            "createdAt": self.created_at,
            "title": self.title,
            "summary": self.summary,
            "tags": list(self.tags),
            "cells": list(self.cells),
        }


@dataclass(slots=True)
class NotebookDefinition:
    notebook_id: str
    title: str
    summary: str
    cells: list[NotebookCellDefinition]
    tags: list[str] = field(default_factory=list)
    tree_path: tuple[str, ...] = ()
    linked_generator_id: str = ""
    can_edit: bool = True
    can_delete: bool = True
    shared: bool = False
    saved_versions: list[NotebookVersionDefinition] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def data_sources(self) -> list[str]:
        sources: list[str] = []
        seen: set[str] = set()
        for cell in self.cells:
            for source_id in cell.data_sources:
                if source_id in seen:
                    continue
                seen.add(source_id)
                sources.append(source_id)
        return sources

    @property
    def sql(self) -> str:
        return self.cells[0].sql if self.cells else ""

    @property
    def access_mode(self) -> str:
        return (
            "Read / Query only"
            if any(len(cell.data_sources) > 1 for cell in self.cells)
            else "Read / Write"
        )

    @property
    def access_mode_hint(self) -> str:
        if any(len(cell.data_sources) > 1 for cell in self.cells):
            return "At least one cell selects multiple sources and stays in query-only mode."
        return "All current cells are configured for single-source read/write execution."

    @property
    def initial_version(self) -> NotebookVersionDefinition:
        return NotebookVersionDefinition(
            version_id=f"initial-{self.notebook_id}",
            created_at=self.created_at,
            title=self.title,
            summary=self.summary,
            tags=list(self.tags),
            cells=self.cells_payload,
        )

    @property
    def versions(self) -> list[NotebookVersionDefinition]:
        return list(self.saved_versions) if self.saved_versions else [self.initial_version]

    @property
    def versions_payload(self) -> list[dict[str, Any]]:
        return [version.payload for version in self.versions]

    @property
    def cells_payload(self) -> list[dict[str, Any]]:
        return [cell.payload for cell in self.cells]

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "notebookId": self.notebook_id,
            "title": self.title,
            "summary": self.summary,
            "cells": self.cells_payload,
            "tags": list(self.tags),
            "treePath": list(self.tree_path),
            "linkedGeneratorId": self.linked_generator_id,
            "canEdit": self.can_edit,
            "canDelete": self.can_delete,
            "shared": self.shared,
            "createdAt": self.created_at,
            "versions": self.versions_payload,
        }


@dataclass(slots=True)
class LinkedNotebookReference:
    notebook_id: str
    title: str

    @property
    def payload(self) -> dict[str, str]:
        return {
            "notebookId": self.notebook_id,
            "title": self.title,
        }


@dataclass(slots=True)
class NotebookEventDefinition:
    event_id: str
    event_type: str
    notebook_id: str
    notebook_title: str
    occurred_at: str
    origin_client_id: str = ""

    @property
    def payload(self) -> dict[str, str]:
        return {
            "eventId": self.event_id,
            "eventType": self.event_type,
            "notebookId": self.notebook_id,
            "notebookTitle": self.notebook_title,
            "occurredAt": self.occurred_at,
            "originClientId": self.origin_client_id,
        }


@dataclass(slots=True)
class NotebookFolder:
    folder_id: str
    name: str
    folders: list["NotebookFolder"] = field(default_factory=list)
    notebooks: list[NotebookDefinition] = field(default_factory=list)
    can_edit: bool = True
    can_delete: bool = True

    @property
    def notebook_count(self) -> int:
        return len(self.notebooks) + sum(folder.notebook_count for folder in self.folders)


@dataclass(slots=True)
class DataGeneratorFolder:
    folder_id: str
    name: str
    folders: list["DataGeneratorFolder"] = field(default_factory=list)
    generators: list[dict[str, Any]] = field(default_factory=list)

    @property
    def generator_count(self) -> int:
        return len(self.generators) + sum(folder.generator_count for folder in self.folders)


@dataclass(slots=True)
class S3ExplorerEntry:
    entry_kind: str
    name: str
    bucket: str
    prefix: str = ""
    path: str = ""
    file_format: str = ""
    size_bytes: int = 0
    has_children: bool = False
    selectable: bool = False
    published_data_products: list[dict[str, Any]] = field(default_factory=list)

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "entryKind": self.entry_kind,
            "name": self.name,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "path": self.path,
            "fileFormat": self.file_format,
            "sizeBytes": self.size_bytes,
            "hasChildren": self.has_children,
            "selectable": self.selectable,
            "publishedDataProducts": list(self.published_data_products),
        }


@dataclass(slots=True)
class S3ExplorerBreadcrumb:
    label: str
    bucket: str = ""
    prefix: str = ""
    path: str = ""

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "path": self.path,
        }


@dataclass(slots=True)
class S3ExplorerSnapshot:
    bucket: str = ""
    prefix: str = ""
    path: str = ""
    entries: list[S3ExplorerEntry] = field(default_factory=list)
    breadcrumbs: list[S3ExplorerBreadcrumb] = field(default_factory=list)
    published_data_products: list[dict[str, Any]] = field(default_factory=list)
    can_create_bucket: bool = True
    can_create_folder: bool = False
    empty_message: str = ""

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "prefix": self.prefix,
            "path": self.path,
            "entries": [entry.payload for entry in self.entries],
            "breadcrumbs": [breadcrumb.payload for breadcrumb in self.breadcrumbs],
            "publishedDataProducts": list(self.published_data_products),
            "canCreateBucket": self.can_create_bucket,
            "canCreateFolder": self.can_create_folder,
            "emptyMessage": self.empty_message,
        }


@dataclass(slots=True)
class S3ExplorerDeleteResult:
    entry_kind: str
    bucket: str
    prefix: str = ""
    path: str = ""
    deleted_keys: int = 0
    bucket_deleted: bool = False
    message: str = ""

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "entryKind": self.entry_kind,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "path": self.path,
            "deletedKeys": self.deleted_keys,
            "bucketDeleted": self.bucket_deleted,
            "message": self.message,
        }


@dataclass(slots=True)
class QueryResult:
    sql: str
    columns: list[str] = field(default_factory=list)
    rows: list[tuple[Any, ...]] = field(default_factory=list)
    duration_ms: float = 0.0
    row_count: int = 0
    truncated: bool = False
    message: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass(slots=True)
class QueryResultExportDefinition:
    job_id: str
    export_format: str
    destination: str
    filename: str
    content_type: str
    message: str
    bucket: str = ""
    key: str = ""
    path: str = ""

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "format": self.export_format,
            "destination": self.destination,
            "filename": self.filename,
            "contentType": self.content_type,
            "message": self.message,
            "bucket": self.bucket,
            "key": self.key,
            "path": self.path,
        }


@dataclass(slots=True)
class QueryJobMetricPoint:
    job_id: str
    notebook_id: str
    notebook_title: str
    completed_at: str
    duration_ms: float
    status: str
    row_count: int = 0

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "notebookId": self.notebook_id,
            "notebookTitle": self.notebook_title,
            "completedAt": self.completed_at,
            "durationMs": self.duration_ms,
            "status": self.status,
            "rowCount": self.row_count,
        }


@dataclass(slots=True)
class QueryJobDefinition:
    job_id: str
    notebook_id: str
    notebook_title: str
    cell_id: str
    sql: str
    status: str
    started_at: str
    updated_at: str
    completed_at: str | None = None
    duration_ms: float = 0.0
    progress: float | None = None
    progress_label: str = "Queued"
    message: str | None = None
    error: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[tuple[Any, ...]] = field(default_factory=list)
    row_count: int = 0
    rows_shown: int = 0
    truncated: bool = False
    first_row_ms: float | None = None
    fetch_ms: float | None = None
    data_sources: list[str] = field(default_factory=list)
    source_types: list[str] = field(default_factory=list)
    touched_relations: list[str] = field(default_factory=list)
    touched_buckets: list[str] = field(default_factory=list)
    backend_name: str = "duckdb"
    can_cancel: bool = False

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "notebookId": self.notebook_id,
            "notebookTitle": self.notebook_title,
            "cellId": self.cell_id,
            "sql": self.sql,
            "status": self.status,
            "startedAt": self.started_at,
            "updatedAt": self.updated_at,
            "completedAt": self.completed_at,
            "durationMs": self.duration_ms,
            "progress": self.progress,
            "progressLabel": self.progress_label,
            "message": self.message,
            "error": self.error,
            "columns": list(self.columns),
            "rows": [list(row) for row in self.rows],
            "rowCount": self.row_count,
            "rowsShown": self.rows_shown,
            "truncated": self.truncated,
            "firstRowMs": self.first_row_ms,
            "fetchMs": self.fetch_ms,
            "dataSources": list(self.data_sources),
            "sourceTypes": list(self.source_types),
            "touchedRelations": list(self.touched_relations),
            "touchedBuckets": list(self.touched_buckets),
            "backendName": self.backend_name,
            "canCancel": self.can_cancel,
        }


@dataclass(slots=True)
class PythonJobOutputDefinition:
    output_type: str
    text: str = ""
    name: str = ""
    html: str = ""
    data: Any = None
    mime_type: str = ""
    traceback: list[str] = field(default_factory=list)
    error_name: str = ""
    error_value: str = ""

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "type": self.output_type,
            "text": self.text,
            "name": self.name,
            "html": self.html,
            "data": self.data,
            "mimeType": self.mime_type,
            "traceback": list(self.traceback),
            "errorName": self.error_name,
            "errorValue": self.error_value,
        }


@dataclass(slots=True)
class PythonJobDefinition:
    job_id: str
    notebook_id: str
    notebook_title: str
    cell_id: str
    code: str
    language: str
    status: str
    started_at: str
    updated_at: str
    completed_at: str | None = None
    duration_ms: float = 0.0
    progress_label: str = "Queued"
    message: str | None = None
    error: str | None = None
    outputs: list[PythonJobOutputDefinition] = field(default_factory=list)
    data_sources: list[str] = field(default_factory=list)
    source_types: list[str] = field(default_factory=list)
    backend_name: str = "Headless Jupyter Kernel"
    can_cancel: bool = False

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "notebookId": self.notebook_id,
            "notebookTitle": self.notebook_title,
            "cellId": self.cell_id,
            "code": self.code,
            "language": self.language,
            "status": self.status,
            "startedAt": self.started_at,
            "updatedAt": self.updated_at,
            "completedAt": self.completed_at,
            "durationMs": self.duration_ms,
            "progressLabel": self.progress_label,
            "message": self.message,
            "error": self.error,
            "outputs": [output.payload for output in self.outputs],
            "dataSources": list(self.data_sources),
            "sourceTypes": list(self.source_types),
            "backendName": self.backend_name,
            "canCancel": self.can_cancel,
        }


@dataclass(slots=True)
class DataGeneratorDefinition:
    generator_id: str
    title: str
    description: str
    target_kind: str
    module_name: str
    tree_path: tuple[str, ...] = ()
    default_size_gb: float = 1.0
    min_size_gb: float = 0.1
    max_size_gb: float = 1024.0
    approximate_row_bytes: int = 256
    default_target_name: str = ""
    supports_cancel: bool = True
    supports_cleanup: bool = True
    tags: list[str] = field(default_factory=list)
    linked_notebooks: list[LinkedNotebookReference] = field(default_factory=list)

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "generatorId": self.generator_id,
            "title": self.title,
            "description": self.description,
            "targetKind": self.target_kind,
            "moduleName": self.module_name,
            "treePath": list(self.tree_path),
            "defaultSizeGb": self.default_size_gb,
            "minSizeGb": self.min_size_gb,
            "maxSizeGb": self.max_size_gb,
            "approximateRowBytes": self.approximate_row_bytes,
            "defaultTargetName": self.default_target_name,
            "supportsCancel": self.supports_cancel,
            "supportsCleanup": self.supports_cleanup,
            "tags": list(self.tags),
            "linkedNotebooks": [notebook.payload for notebook in self.linked_notebooks],
        }


@dataclass(slots=True)
class DataGenerationTargetDefinition:
    target_kind: str
    label: str
    location: str
    status: str = "pending"

    @property
    def payload(self) -> dict[str, str]:
        return {
            "targetKind": self.target_kind,
            "label": self.label,
            "location": self.location,
            "status": self.status,
        }


def normalize_data_generation_targets(targets: Any) -> list["DataGenerationTargetDefinition"]:
    if not isinstance(targets, list):
        return []

    normalized: list[DataGenerationTargetDefinition] = []
    seen: set[tuple[str, str]] = set()

    for item in targets:
        if isinstance(item, DataGenerationTargetDefinition):
            target = item
        elif isinstance(item, dict):
            target_kind = str(item.get("targetKind") or item.get("target_kind") or "").strip() or "target"
            label = str(item.get("label") or "").strip()
            location = str(item.get("location") or "").strip()
            status = str(item.get("status") or "").strip() or "pending"
            if not location:
                continue
            target = DataGenerationTargetDefinition(
                target_kind=target_kind,
                label=label or location,
                location=location,
                status=status,
            )
        else:
            continue

        key = (target.target_kind.strip() or "target", target.location.strip())
        if not key[1] or key in seen:
            continue
        seen.add(key)
        normalized.append(
            DataGenerationTargetDefinition(
                target_kind=key[0],
                label=target.label.strip() or key[1],
                location=key[1],
                status=target.status.strip() or "pending",
            )
        )

    return normalized


@dataclass(slots=True)
class DataGenerationJobDefinition:
    job_id: str
    generator_id: str
    title: str
    description: str
    target_kind: str
    requested_size_gb: float
    status: str
    started_at: str
    updated_at: str
    completed_at: str | None = None
    duration_ms: float = 0.0
    progress: float | None = None
    progress_label: str = "Queued"
    message: str | None = None
    error: str | None = None
    target_name: str = ""
    target_relation: str = ""
    target_path: str = ""
    written_targets: list[DataGenerationTargetDefinition] = field(default_factory=list)
    generated_rows: int = 0
    generated_size_gb: float = 0.0
    backend_name: str = "Python module"
    can_cancel: bool = False
    can_cleanup: bool = False

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "generatorId": self.generator_id,
            "title": self.title,
            "description": self.description,
            "targetKind": self.target_kind,
            "requestedSizeGb": self.requested_size_gb,
            "status": self.status,
            "startedAt": self.started_at,
            "updatedAt": self.updated_at,
            "completedAt": self.completed_at,
            "durationMs": self.duration_ms,
            "progress": self.progress,
            "progressLabel": self.progress_label,
            "message": self.message,
            "error": self.error,
            "targetName": self.target_name,
            "targetRelation": self.target_relation,
            "targetPath": self.target_path,
            "writtenTargets": [target.payload for target in self.written_targets],
            "generatedRows": self.generated_rows,
            "generatedSizeGb": self.generated_size_gb,
            "backendName": self.backend_name,
            "canCancel": self.can_cancel,
            "canCleanup": self.can_cleanup,
        }

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


@dataclass(slots=True)
class SourceSchema:
    name: str
    label: str | None = None
    objects: list[SourceObject] = field(default_factory=list)


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
        return [self.initial_version]

    @property
    def versions_payload(self) -> list[dict[str, Any]]:
        return [version.payload for version in self.versions]

    @property
    def cells_payload(self) -> list[dict[str, Any]]:
        return [cell.payload for cell in self.cells]


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
    data_sources: list[str] = field(default_factory=list)
    source_types: list[str] = field(default_factory=list)
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
        }


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
            "generatedRows": self.generated_rows,
            "generatedSizeGb": self.generated_size_gb,
            "backendName": self.backend_name,
            "canCancel": self.can_cancel,
            "canCleanup": self.can_cleanup,
        }


@dataclass(slots=True)
class IngestionCleanupTargetDefinition:
    target_id: str
    title: str
    description: str
    confirm_copy: str

    @property
    def payload(self) -> dict[str, str]:
        return {
            "targetId": self.target_id,
            "title": self.title,
            "description": self.description,
            "confirmCopy": self.confirm_copy,
        }

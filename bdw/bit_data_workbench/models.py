from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class SourceObject:
    name: str
    kind: str
    relation: str


@dataclass(slots=True)
class SourceSchema:
    name: str
    objects: list[SourceObject] = field(default_factory=list)


@dataclass(slots=True)
class SourceCatalog:
    name: str
    schemas: list[SourceSchema] = field(default_factory=list)


@dataclass(slots=True)
class NotebookDefinition:
    notebook_id: str
    title: str
    summary: str
    target_label: str
    sql: str
    tags: list[str] = field(default_factory=list)
    tree_path: tuple[str, ...] = ()
    can_edit: bool = True
    can_delete: bool = True


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

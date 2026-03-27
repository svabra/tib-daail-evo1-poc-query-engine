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
class NotebookDefinition:
    notebook_id: str
    title: str
    summary: str
    cells: list[NotebookCellDefinition]
    tags: list[str] = field(default_factory=list)
    tree_path: tuple[str, ...] = ()
    can_edit: bool = True
    can_delete: bool = True

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

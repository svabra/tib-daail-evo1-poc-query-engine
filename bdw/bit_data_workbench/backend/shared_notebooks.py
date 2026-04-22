from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

from ..models import NotebookCellDefinition, NotebookDefinition, NotebookVersionDefinition


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_notebook_cell_language(value: object) -> str:
    normalized = str(value or "").strip().lower()
    return "python" if normalized == "python" else "sql"


def notebook_cell_from_payload(payload: object) -> NotebookCellDefinition | None:
    if not isinstance(payload, dict):
        return None

    cell_id = str(payload.get("cellId") or payload.get("cell_id") or "").strip()
    if not cell_id:
        cell_id = f"shared-cell-{uuid.uuid4().hex[:12]}"

    return NotebookCellDefinition(
        cell_id=cell_id,
        sql=str(payload.get("sql") or ""),
        language=normalize_notebook_cell_language(payload.get("language")),
        data_sources=[
            str(source_id).strip()
            for source_id in payload.get("dataSources", payload.get("data_sources", [])) or []
            if str(source_id).strip()
        ],
    )


def notebook_version_from_payload(payload: object) -> NotebookVersionDefinition | None:
    if not isinstance(payload, dict):
        return None

    version_id = str(payload.get("versionId") or payload.get("version_id") or "").strip()
    if not version_id:
        version_id = f"shared-version-{uuid.uuid4().hex[:12]}"

    cells = [
        cell.payload
        for cell in (
            notebook_cell_from_payload(cell_payload)
            for cell_payload in payload.get("cells", []) or []
        )
        if cell is not None
    ]

    return NotebookVersionDefinition(
        version_id=version_id,
        created_at=str(payload.get("createdAt") or payload.get("created_at") or utc_now_iso()),
        title=str(payload.get("title") or ""),
        summary=str(payload.get("summary") or ""),
        tags=[
            str(tag).strip()
            for tag in payload.get("tags", []) or []
            if str(tag).strip()
        ],
        cells=cells,
    )


def serialize_notebook(notebook: NotebookDefinition) -> dict[str, object]:
    return {
        "notebookId": notebook.notebook_id,
        "title": notebook.title,
        "summary": notebook.summary,
        "cells": notebook.cells_payload,
        "tags": list(notebook.tags),
        "treePath": list(notebook.tree_path),
        "linkedGeneratorId": notebook.linked_generator_id,
        "createdAt": notebook.created_at,
        "shared": True,
        "versions": notebook.versions_payload,
    }


def deserialize_notebook(payload: object) -> NotebookDefinition | None:
    if not isinstance(payload, dict):
        return None

    notebook_id = str(payload.get("notebookId") or payload.get("notebook_id") or "").strip()
    if not notebook_id:
        return None

    cells = [
        cell
        for cell in (
            notebook_cell_from_payload(cell_payload)
            for cell_payload in payload.get("cells", []) or []
        )
        if cell is not None
    ]
    versions = [
        version
        for version in (
            notebook_version_from_payload(version_payload)
            for version_payload in payload.get("versions", []) or []
        )
        if version is not None
    ]

    return NotebookDefinition(
        notebook_id=notebook_id,
        title=str(payload.get("title") or "Untitled Notebook"),
        summary=str(payload.get("summary") or "Describe this notebook."),
        cells=cells or [NotebookCellDefinition(cell_id=f"shared-cell-{uuid.uuid4().hex[:12]}", sql="", data_sources=[])],
        tags=[
            str(tag).strip()
            for tag in payload.get("tags", []) or []
            if str(tag).strip()
        ],
        tree_path=tuple(
            str(segment).strip()
            for segment in payload.get("treePath", payload.get("tree_path", [])) or []
            if str(segment).strip()
        ),
        linked_generator_id=str(payload.get("linkedGeneratorId") or payload.get("linked_generator_id") or ""),
        can_edit=True,
        can_delete=True,
        shared=True,
        saved_versions=versions,
        created_at=str(payload.get("createdAt") or payload.get("created_at") or utc_now_iso()),
    )


class SharedNotebookStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def list_notebooks(self) -> list[NotebookDefinition]:
        state = self._read_state()
        notebooks: list[NotebookDefinition] = []
        for payload in state.get("notebooks", []):
            notebook = deserialize_notebook(payload)
            if notebook is not None:
                notebooks.append(notebook)
        return notebooks

    def upsert_notebook(self, notebook: NotebookDefinition) -> tuple[NotebookDefinition, str]:
        state = self._read_state()
        notebooks = list(state.get("notebooks", []))
        serialized = serialize_notebook(notebook)
        existing_index = next(
            (
                index
                for index, item in enumerate(notebooks)
                if str(item.get("notebookId") or item.get("notebook_id") or "").strip() == notebook.notebook_id
            ),
            -1,
        )

        action = "created" if existing_index < 0 else "updated"
        if existing_index < 0:
            notebooks.append(serialized)
        else:
            notebooks[existing_index] = serialized

        self._write_state({"notebooks": notebooks})
        refreshed = deserialize_notebook(serialized)
        if refreshed is None:
            raise ValueError(f"Failed to deserialize shared notebook {notebook.notebook_id}.")
        return refreshed, action

    def delete_notebook(self, notebook_id: str) -> NotebookDefinition:
        state = self._read_state()
        notebooks = list(state.get("notebooks", []))
        remaining: list[dict[str, object]] = []
        removed_payload: dict[str, object] | None = None

        for payload in notebooks:
            payload_id = str(payload.get("notebookId") or payload.get("notebook_id") or "").strip()
            if payload_id == notebook_id and removed_payload is None:
                removed_payload = payload
                continue
            remaining.append(payload)

        if removed_payload is None:
            raise KeyError(f"Unknown shared notebook: {notebook_id}")

        self._write_state({"notebooks": remaining})
        removed = deserialize_notebook(removed_payload)
        if removed is None:
            raise ValueError(f"Failed to deserialize removed shared notebook {notebook_id}.")
        return removed

    def _read_state(self) -> dict[str, list[dict[str, object]]]:
        if not self._path.exists():
            return {"notebooks": []}

        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"notebooks": []}

        if not isinstance(raw, dict):
            return {"notebooks": []}

        notebooks = raw.get("notebooks")
        if not isinstance(notebooks, list):
            return {"notebooks": []}
        return {"notebooks": [item for item in notebooks if isinstance(item, dict)]}

    def _write_state(self, state: dict[str, list[dict[str, object]]]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temp_path.replace(self._path)

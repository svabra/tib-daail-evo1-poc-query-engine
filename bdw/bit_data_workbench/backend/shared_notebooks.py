from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from urllib.parse import quote

from ..config import Settings
from ..models import NotebookCellDefinition, NotebookDefinition, NotebookVersionDefinition
from .query_options import normalize_query_options
from .s3_hidden import shared_notebooks_bucket_name
from .s3_storage import ensure_s3_bucket, iter_s3_keys, s3_client


logger = logging.getLogger(__name__)
S3_NOTEBOOK_PREFIX = "notebooks/"
S3_FOLDER_MANIFEST_KEY = "folders/shared-folders.json"
S3_MIGRATION_MARKER_KEY = "metadata/local-migration-complete.json"
SHARED_NOTEBOOK_DEFAULT_FOLDER_PATH = ("Shared Notebooks",)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_folder_path(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(segment).strip() for segment in value if str(segment).strip())


@dataclass(frozen=True, slots=True)
class SharedNotebookFolder:
    path: tuple[str, ...]
    display_name: str = ""
    is_public: bool = False
    can_edit: bool = True
    can_delete: bool = True
    updated_at: str = ""
    version: int = 1

    @property
    def name(self) -> str:
        return self.display_name or (self.path[-1] if self.path else "Folder")

    @property
    def payload(self) -> dict[str, object]:
        return serialize_folder(self)


def serialize_folder(folder: SharedNotebookFolder) -> dict[str, object]:
    return {
        "path": list(folder.path),
        "displayName": folder.name,
        "isPublic": bool(folder.is_public),
        "canEdit": bool(folder.can_edit),
        "canDelete": bool(folder.can_delete),
        "updatedAt": folder.updated_at or utc_now_iso(),
        "version": max(1, int(folder.version or 1)),
    }


def deserialize_folder(payload: object) -> SharedNotebookFolder | None:
    if not isinstance(payload, dict):
        return None

    path = normalize_folder_path(payload.get("path") or payload.get("treePath"))
    if not path:
        return None

    display_name = str(
        payload.get("displayName") or payload.get("display_name") or path[-1]
    ).strip() or path[-1]
    return SharedNotebookFolder(
        path=path,
        display_name=display_name,
        is_public=bool(payload.get("isPublic", payload.get("is_public", False))),
        can_edit=bool(payload.get("canEdit", payload.get("can_edit", True))),
        can_delete=bool(payload.get("canDelete", payload.get("can_delete", True))),
        updated_at=str(payload.get("updatedAt") or payload.get("updated_at") or utc_now_iso()),
        version=max(1, int(payload.get("version") or 1)),
    )


def default_shared_notebook_folder() -> SharedNotebookFolder:
    return SharedNotebookFolder(
        path=SHARED_NOTEBOOK_DEFAULT_FOLDER_PATH,
        display_name=SHARED_NOTEBOOK_DEFAULT_FOLDER_PATH[-1],
        is_public=True,
        can_edit=True,
        can_delete=True,
        updated_at=utc_now_iso(),
        version=1,
    )


def folder_manifest_payload(folders: list[SharedNotebookFolder]) -> dict[str, object]:
    return {
        "folders": [serialize_folder(folder) for folder in folders],
        "updatedAt": utc_now_iso(),
    }


def folders_from_manifest(payload: object) -> list[SharedNotebookFolder]:
    if not isinstance(payload, dict):
        return []

    folders = []
    for folder_payload in payload.get("folders", []) or []:
        folder = deserialize_folder(folder_payload)
        if folder is not None:
            folders.append(folder)
    return folders


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
        query_options=normalize_query_options(payload.get("queryOptions")),
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

    def list_folders(self) -> list[SharedNotebookFolder]:
        state = self._read_state()
        return folders_from_manifest({"folders": state.get("folders", [])})

    def upsert_folder(self, folder: SharedNotebookFolder) -> tuple[SharedNotebookFolder, str]:
        state = self._read_state()
        folders = folders_from_manifest({"folders": state.get("folders", [])})
        folder_index = next(
            (index for index, item in enumerate(folders) if item.path == folder.path),
            -1,
        )
        refreshed = SharedNotebookFolder(
            path=folder.path,
            display_name=folder.name,
            is_public=folder.is_public,
            can_edit=folder.can_edit,
            can_delete=folder.can_delete,
            updated_at=utc_now_iso(),
            version=(folders[folder_index].version + 1 if folder_index >= 0 else 1),
        )
        action = "created" if folder_index < 0 else "updated"
        if folder_index < 0:
            folders.append(refreshed)
        else:
            folders[folder_index] = refreshed
        self._write_state(
            {
                "notebooks": list(state.get("notebooks", [])),
                "folders": [serialize_folder(item) for item in folders],
            }
        )
        return refreshed, action

    def set_folder_visibility(
        self,
        *,
        path: list[str] | tuple[str, ...],
        is_public: bool,
        display_name: str = "",
    ) -> tuple[SharedNotebookFolder, str]:
        normalized_path = normalize_folder_path(path)
        if not normalized_path:
            raise ValueError("Notebook folder path is required.")

        existing = next(
            (folder for folder in self.list_folders() if folder.path == normalized_path),
            None,
        )
        folder = SharedNotebookFolder(
            path=normalized_path,
            display_name=str(display_name or "").strip()
            or (existing.name if existing is not None else normalized_path[-1]),
            is_public=bool(is_public),
            can_edit=True if existing is None else existing.can_edit,
            can_delete=True if existing is None else existing.can_delete,
            updated_at=utc_now_iso(),
            version=existing.version if existing is not None else 1,
        )
        return self.upsert_folder(folder)

    def ensure_default_folders(self) -> None:
        if any(folder.path == SHARED_NOTEBOOK_DEFAULT_FOLDER_PATH for folder in self.list_folders()):
            return
        self.upsert_folder(default_shared_notebook_folder())

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

        self._write_state({"notebooks": notebooks, "folders": list(state.get("folders", []))})
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

        self._write_state({"notebooks": remaining, "folders": list(state.get("folders", []))})
        removed = deserialize_notebook(removed_payload)
        if removed is None:
            raise ValueError(f"Failed to deserialize removed shared notebook {notebook_id}.")
        return removed

    def _read_state(self) -> dict[str, list[dict[str, object]]]:
        if not self._path.exists():
            return {"notebooks": [], "folders": []}

        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"notebooks": [], "folders": []}

        if not isinstance(raw, dict):
            return {"notebooks": [], "folders": []}

        notebooks = raw.get("notebooks")
        folders = raw.get("folders")
        return {
            "notebooks": [item for item in notebooks if isinstance(item, dict)]
            if isinstance(notebooks, list)
            else [],
            "folders": [item for item in folders if isinstance(item, dict)]
            if isinstance(folders, list)
            else [],
        }

    def _write_state(self, state: dict[str, list[dict[str, object]]]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temp_path.replace(self._path)


class S3SharedNotebookStore:
    def __init__(
        self,
        settings: Settings,
        *,
        s3_client_factory=s3_client,
        ensure_bucket=ensure_s3_bucket,
    ) -> None:
        self._settings = settings
        self._s3_client_factory = s3_client_factory
        self._ensure_bucket = ensure_bucket
        self._bucket = shared_notebooks_bucket_name(settings)
        self._lock = RLock()
        self._available = False
        self._initialization_error = ""

    @property
    def bucket(self) -> str:
        return self._bucket

    def initialize(self, *, migrate_from: SharedNotebookStore | None = None) -> None:
        with self._lock:
            self._available = False
            self._initialization_error = ""
            try:
                self._require_configured()
                self._ensure_bucket(self._settings, self._bucket)
                if migrate_from is not None:
                    self._migrate_from_file_store(migrate_from)
                self._ensure_default_folders_locked()
                self._available = True
            except Exception as exc:
                self._initialization_error = str(exc)
                logger.warning(
                    "Shared notebook S3 storage is unavailable; shared notebooks will not be loaded: %s",
                    exc,
                    exc_info=True,
                )

    def list_notebooks(self) -> list[NotebookDefinition]:
        if not self._available:
            return []

        notebooks: list[NotebookDefinition] = []
        try:
            client = self._client()
            for key in iter_s3_keys(client, self._bucket, S3_NOTEBOOK_PREFIX):
                if not key.endswith(".json"):
                    continue
                payload = self._read_payload(client, key)
                notebook = deserialize_notebook(payload)
                if notebook is not None:
                    notebooks.append(notebook)
        except Exception as exc:
            logger.warning(
                "Failed to list shared notebooks from S3 bucket %r: %s",
                self._bucket,
                exc,
                exc_info=True,
            )
            return []

        return notebooks

    def list_folders(self) -> list[SharedNotebookFolder]:
        if not self._available:
            return []

        try:
            return folders_from_manifest(self._read_payload(self._client(), S3_FOLDER_MANIFEST_KEY))
        except KeyError:
            return []
        except Exception as exc:
            logger.warning(
                "Failed to list shared notebook folders from S3 bucket %r: %s",
                self._bucket,
                exc,
                exc_info=True,
            )
            return []

    def upsert_notebook(self, notebook: NotebookDefinition) -> tuple[NotebookDefinition, str]:
        self._require_available()
        serialized = serialize_notebook(notebook)
        key = self._notebook_key(notebook.notebook_id)
        client = self._client()
        action = "updated" if self._object_exists(client, key) else "created"
        self._write_payload(client, key, serialized)
        refreshed = deserialize_notebook(serialized)
        if refreshed is None:
            raise ValueError(f"Failed to deserialize shared notebook {notebook.notebook_id}.")
        return refreshed, action

    def delete_notebook(self, notebook_id: str) -> NotebookDefinition:
        self._require_available()
        normalized_notebook_id = str(notebook_id or "").strip()
        key = self._notebook_key(normalized_notebook_id)
        client = self._client()
        try:
            payload = self._read_payload(client, key)
        except KeyError as exc:
            raise KeyError(f"Unknown shared notebook: {normalized_notebook_id}") from exc

        removed = deserialize_notebook(payload)
        if removed is None:
            raise ValueError(f"Failed to deserialize removed shared notebook {normalized_notebook_id}.")
        client.delete_object(Bucket=self._bucket, Key=key)
        return removed

    def upsert_folder(self, folder: SharedNotebookFolder) -> tuple[SharedNotebookFolder, str]:
        self._require_available()
        return self._upsert_folder_locked(folder)

    def set_folder_visibility(
        self,
        *,
        path: list[str] | tuple[str, ...],
        is_public: bool,
        display_name: str = "",
    ) -> tuple[SharedNotebookFolder, str]:
        self._require_available()
        normalized_path = normalize_folder_path(path)
        if not normalized_path:
            raise ValueError("Notebook folder path is required.")

        existing = next(
            (folder for folder in self.list_folders() if folder.path == normalized_path),
            None,
        )
        folder = SharedNotebookFolder(
            path=normalized_path,
            display_name=str(display_name or "").strip()
            or (existing.name if existing is not None else normalized_path[-1]),
            is_public=bool(is_public),
            can_edit=True if existing is None else existing.can_edit,
            can_delete=True if existing is None else existing.can_delete,
            updated_at=utc_now_iso(),
            version=existing.version if existing is not None else 1,
        )
        return self._upsert_folder_locked(folder)

    def ensure_default_folders(self) -> None:
        self._require_available()
        self._ensure_default_folders_locked()

    def _require_configured(self) -> None:
        if not self._bucket:
            raise ValueError(
                "Configure S3_BUCKET or BDW_SHARED_NOTEBOOKS_BUCKET before sharing notebooks."
            )
        if not all(
            (
                self._settings.s3_endpoint,
                self._settings.current_s3_access_key_id(),
                self._settings.current_s3_secret_access_key(),
            )
        ):
            raise ValueError("S3 must be configured before sharing notebooks.")

    def _require_available(self) -> None:
        if not self._available:
            self.initialize()
        if self._available:
            return
        detail = self._initialization_error or "S3 storage is not initialized."
        raise ValueError(f"Shared notebook S3 storage is unavailable: {detail}")

    def _client(self):
        return self._s3_client_factory(self._settings)

    def _migrate_from_file_store(self, file_store: SharedNotebookStore) -> None:
        client = self._client()
        if self._object_exists(client, S3_MIGRATION_MARKER_KEY):
            return

        for notebook in file_store.list_notebooks():
            key = self._notebook_key(notebook.notebook_id)
            if self._object_exists(client, key):
                continue
            self._write_payload(client, key, serialize_notebook(notebook))

        folders = [
            folder
            for folder in getattr(file_store, "list_folders", lambda: [])()
            if isinstance(folder, SharedNotebookFolder)
        ]
        if folders and not self._object_exists(client, S3_FOLDER_MANIFEST_KEY):
            self._write_payload(client, S3_FOLDER_MANIFEST_KEY, folder_manifest_payload(folders))

        self._write_payload(
            client,
            S3_MIGRATION_MARKER_KEY,
            {
                "migratedAt": utc_now_iso(),
                "source": "shared-notebooks.json",
            },
        )

    def _notebook_key(self, notebook_id: str) -> str:
        normalized_notebook_id = str(notebook_id or "").strip()
        if not normalized_notebook_id:
            raise ValueError("Shared notebook id is required.")
        return f"{S3_NOTEBOOK_PREFIX}{quote(normalized_notebook_id, safe='')}.json"

    def _ensure_default_folders_locked(self) -> None:
        client = self._client()
        try:
            folders = folders_from_manifest(self._read_payload(client, S3_FOLDER_MANIFEST_KEY))
        except KeyError:
            folders = []

        if any(folder.path == SHARED_NOTEBOOK_DEFAULT_FOLDER_PATH for folder in folders):
            return

        folders.append(default_shared_notebook_folder())
        self._write_payload(client, S3_FOLDER_MANIFEST_KEY, folder_manifest_payload(folders))

    def _upsert_folder_locked(self, folder: SharedNotebookFolder) -> tuple[SharedNotebookFolder, str]:
        normalized_path = normalize_folder_path(folder.path)
        if not normalized_path:
            raise ValueError("Notebook folder path is required.")

        client = self._client()
        try:
            folders = folders_from_manifest(self._read_payload(client, S3_FOLDER_MANIFEST_KEY))
        except KeyError:
            folders = []

        folder_index = next(
            (index for index, item in enumerate(folders) if item.path == normalized_path),
            -1,
        )
        refreshed = SharedNotebookFolder(
            path=normalized_path,
            display_name=folder.name,
            is_public=folder.is_public,
            can_edit=folder.can_edit,
            can_delete=folder.can_delete,
            updated_at=utc_now_iso(),
            version=(folders[folder_index].version + 1 if folder_index >= 0 else 1),
        )
        action = "created" if folder_index < 0 else "updated"
        if folder_index < 0:
            folders.append(refreshed)
        else:
            folders[folder_index] = refreshed

        self._write_payload(client, S3_FOLDER_MANIFEST_KEY, folder_manifest_payload(folders))
        return refreshed, action

    def _read_payload(self, client, key: str) -> dict[str, object]:
        try:
            response = client.get_object(Bucket=self._bucket, Key=key) or {}
        except Exception as exc:
            if self._is_missing_object_error(exc):
                raise KeyError(key) from exc
            raise

        body = response.get("Body")
        raw = b"" if body is None else body.read()
        close = getattr(body, "close", None)
        if callable(close):
            close()
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("Skipping malformed shared notebook object s3://%s/%s: %s", self._bucket, key, exc)
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _write_payload(self, client, key: str, payload: dict[str, object]) -> None:
        client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

    def _object_exists(self, client, key: str) -> bool:
        try:
            client.head_object(Bucket=self._bucket, Key=key)
            return True
        except Exception as exc:
            if self._is_missing_object_error(exc):
                return False
            return False

    def _is_missing_object_error(self, error: Exception) -> bool:
        response = getattr(error, "response", None)
        if isinstance(response, dict):
            code = str((response.get("Error") or {}).get("Code") or "").strip()
            if code in {"404", "NoSuchKey", "NotFound"}:
                return True
        return error.__class__.__name__ in {"NoSuchKey", "NotFound"}

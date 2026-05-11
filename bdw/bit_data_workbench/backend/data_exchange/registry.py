from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from threading import RLock


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


@dataclass(frozen=True, slots=True)
class DataExchangeFolderRecord:
    folder_id: str
    name: str
    parent_folder_id: str
    created_at: str
    updated_at: str

    @property
    def payload(self) -> dict[str, object]:
        return {
            "folderId": self.folder_id,
            "name": self.name,
            "parentFolderId": self.parent_folder_id,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }


@dataclass(frozen=True, slots=True)
class DataExchangeFileRecord:
    file_id: str
    file_name: str
    display_name: str
    description: str
    owner_contact: str
    tags: list[str]
    folder_id: str
    bucket: str
    s3_key: str
    size_bytes: int
    content_type: str
    uploaded_at: str
    updated_at: str
    password_hash: str

    @property
    def payload(self) -> dict[str, object]:
        suffix = Path(self.file_name).suffix.lstrip(".").lower()
        return {
            "fileId": self.file_id,
            "fileName": self.file_name,
            "displayName": self.display_name or self.file_name,
            "description": self.description,
            "ownerContact": self.owner_contact,
            "tags": list(self.tags),
            "folderId": self.folder_id,
            "bucket": self.bucket,
            "sizeBytes": self.size_bytes,
            "contentType": self.content_type,
            "uploadedAt": self.uploaded_at,
            "updatedAt": self.updated_at,
            "extension": suffix,
            "hasPassword": bool(self.password_hash),
        }


def serialize_record(record: DataExchangeFileRecord) -> dict[str, object]:
    return {
        "fileId": record.file_id,
        "fileName": record.file_name,
        "displayName": record.display_name,
        "description": record.description,
        "ownerContact": record.owner_contact,
        "tags": list(record.tags),
        "folderId": record.folder_id,
        "bucket": record.bucket,
        "s3Key": record.s3_key,
        "sizeBytes": record.size_bytes,
        "contentType": record.content_type,
        "uploadedAt": record.uploaded_at,
        "updatedAt": record.updated_at,
        "passwordHash": record.password_hash,
    }


def deserialize_record(payload: object) -> DataExchangeFileRecord | None:
    if not isinstance(payload, dict):
        return None
    file_id = str(payload.get("fileId") or payload.get("file_id") or "").strip()
    file_name = str(payload.get("fileName") or payload.get("file_name") or "").strip()
    bucket = str(payload.get("bucket") or "").strip()
    s3_key = str(payload.get("s3Key") or payload.get("s3_key") or "").strip()
    password_hash = str(
        payload.get("passwordHash") or payload.get("password_hash") or ""
    ).strip()
    if not file_id or not file_name or not bucket or not s3_key:
        return None
    return DataExchangeFileRecord(
        file_id=file_id,
        file_name=file_name,
        display_name=str(payload.get("displayName") or payload.get("display_name") or "").strip(),
        description=str(payload.get("description") or "").strip(),
        owner_contact=str(payload.get("ownerContact") or payload.get("owner_contact") or "").strip(),
        tags=_string_list(payload.get("tags")),
        folder_id=str(payload.get("folderId") or payload.get("folder_id") or "").strip(),
        bucket=bucket,
        s3_key=s3_key,
        size_bytes=max(0, int(payload.get("sizeBytes") or payload.get("size_bytes") or 0)),
        content_type=str(payload.get("contentType") or payload.get("content_type") or "").strip(),
        uploaded_at=str(payload.get("uploadedAt") or payload.get("uploaded_at") or "").strip(),
        updated_at=str(payload.get("updatedAt") or payload.get("updated_at") or "").strip(),
        password_hash=password_hash,
    )


def serialize_folder(record: DataExchangeFolderRecord) -> dict[str, object]:
    return {
        "folderId": record.folder_id,
        "name": record.name,
        "parentFolderId": record.parent_folder_id,
        "createdAt": record.created_at,
        "updatedAt": record.updated_at,
    }


def deserialize_folder(payload: object) -> DataExchangeFolderRecord | None:
    if not isinstance(payload, dict):
        return None
    folder_id = str(payload.get("folderId") or payload.get("folder_id") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not folder_id or not name:
        return None
    return DataExchangeFolderRecord(
        folder_id=folder_id,
        name=name,
        parent_folder_id=str(
            payload.get("parentFolderId") or payload.get("parent_folder_id") or ""
        ).strip(),
        created_at=str(payload.get("createdAt") or payload.get("created_at") or "").strip(),
        updated_at=str(payload.get("updatedAt") or payload.get("updated_at") or "").strip(),
    )


class DataExchangeStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = RLock()

    def list_records(self) -> list[DataExchangeFileRecord]:
        with self._lock:
            state = self._read_state()
            records: list[DataExchangeFileRecord] = []
            for payload in state.get("files", []):
                record = deserialize_record(payload)
                if record is not None:
                    records.append(record)
            return records

    def list_folders(self) -> list[DataExchangeFolderRecord]:
        with self._lock:
            state = self._read_state()
            folders: list[DataExchangeFolderRecord] = []
            for payload in state.get("folders", []):
                folder = deserialize_folder(payload)
                if folder is not None:
                    folders.append(folder)
            return folders

    def upsert_record(self, record: DataExchangeFileRecord) -> DataExchangeFileRecord:
        with self._lock:
            state = self._read_state()
            files = list(state.get("files", []))
            next_payload = serialize_record(record)
            for index, item in enumerate(files):
                item_id = str(item.get("fileId") or item.get("file_id") or "").strip()
                if item_id == record.file_id:
                    files[index] = next_payload
                    self._write_state({**state, "files": files})
                    return record
            files.append(next_payload)
            self._write_state({**state, "files": files})
            return record

    def upsert_folder(self, record: DataExchangeFolderRecord) -> DataExchangeFolderRecord:
        with self._lock:
            state = self._read_state()
            folders = list(state.get("folders", []))
            next_payload = serialize_folder(record)
            for index, item in enumerate(folders):
                item_id = str(item.get("folderId") or item.get("folder_id") or "").strip()
                if item_id == record.folder_id:
                    folders[index] = next_payload
                    self._write_state({**state, "folders": folders})
                    return record
            folders.append(next_payload)
            self._write_state({**state, "folders": folders})
            return record

    def delete_record(self, file_id: str) -> DataExchangeFileRecord:
        normalized_file_id = str(file_id or "").strip()
        with self._lock:
            state = self._read_state()
            files = list(state.get("files", []))
            remaining: list[dict[str, object]] = []
            removed: dict[str, object] | None = None
            for payload in files:
                item_id = str(payload.get("fileId") or payload.get("file_id") or "").strip()
                if item_id == normalized_file_id and removed is None:
                    removed = payload
                    continue
                remaining.append(payload)
            if removed is None:
                raise KeyError(f"Unknown DataExchange file: {normalized_file_id}")
            self._write_state({**state, "files": remaining})
        record = deserialize_record(removed)
        if record is None:
            raise ValueError(f"Failed to deserialize removed DataExchange file {normalized_file_id}.")
        return record

    def delete_folder(self, folder_id: str) -> DataExchangeFolderRecord:
        normalized_folder_id = str(folder_id or "").strip()
        with self._lock:
            state = self._read_state()
            folders = list(state.get("folders", []))
            remaining: list[dict[str, object]] = []
            removed: dict[str, object] | None = None
            for payload in folders:
                item_id = str(payload.get("folderId") or payload.get("folder_id") or "").strip()
                if item_id == normalized_folder_id and removed is None:
                    removed = payload
                    continue
                remaining.append(payload)
            if removed is None:
                raise KeyError(f"Unknown DataExchange folder: {normalized_folder_id}")
            self._write_state({**state, "folders": remaining})
        folder = deserialize_folder(removed)
        if folder is None:
            raise ValueError(f"Failed to deserialize removed DataExchange folder {normalized_folder_id}.")
        return folder

    def record(self, file_id: str) -> DataExchangeFileRecord:
        normalized_file_id = str(file_id or "").strip()
        for record in self.list_records():
            if record.file_id == normalized_file_id:
                return record
        raise KeyError(f"Unknown DataExchange file: {normalized_file_id}")

    def folder(self, folder_id: str) -> DataExchangeFolderRecord:
        normalized_folder_id = str(folder_id or "").strip()
        for folder in self.list_folders():
            if folder.folder_id == normalized_folder_id:
                return folder
        raise KeyError(f"Unknown DataExchange folder: {normalized_folder_id}")

    def _read_state(self) -> dict[str, list[dict[str, object]]]:
        if not self._path.exists():
            return {"files": [], "folders": []}
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"files": [], "folders": []}
        if not isinstance(raw, dict):
            return {"files": [], "folders": []}
        files = raw.get("files")
        folders = raw.get("folders")
        if not isinstance(files, list):
            files = []
        if not isinstance(folders, list):
            folders = []
        return {
            "files": [item for item in files if isinstance(item, dict)],
            "folders": [item for item in folders if isinstance(item, dict)],
        }

    def _write_state(self, state: dict[str, list[dict[str, object]]]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(self._path)

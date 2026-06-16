from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import mimetypes
from pathlib import Path, PurePosixPath
import re
from threading import RLock
from uuid import uuid4

from boto3.s3.transfer import TransferConfig

from ...config import Settings
from ..ingestion_types.common.uploads import IngestionLocalSource
from ..s3_hidden import (
    is_data_exchange_bucket_name,
    is_data_exchange_key,
    normalize_data_exchange_prefix,
    reject_hidden_s3_location,
)
from ..s3_storage import ensure_s3_bucket, s3_client, upload_s3_file
from .registry import DataExchangeFileRecord, DataExchangeFolderRecord, DataExchangeStore
from .security import hash_password, verify_password


DATA_EXCHANGE_QUERYABLE_EXTENSIONS = {
    "csv",
    "json",
    "jsonl",
    "ndjson",
    "parquet",
    "xlsx",
    "xml",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def normalize_tags(tags: list[str] | str | None) -> list[str]:
    if isinstance(tags, str):
        candidates = tags.split(",")
    else:
        candidates = list(tags or [])
    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        tag = str(candidate or "").strip()
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(tag)
    return result


def safe_folder_name(folder_name: str) -> str:
    name = str(folder_name or "").strip()
    if not name:
        raise ValueError("Choose a folder name.")
    if "/" in name or "\\" in name or name in {".", ".."}:
        raise ValueError("Folder names cannot contain path separators.")
    if re.match(r"^[a-zA-Z]:", name) or any(ord(character) < 32 for character in name):
        raise ValueError("Choose a safe folder name.")
    sanitized = re.sub(r"[^A-Za-z0-9._ -]+", "_", name).strip(" .")
    if not sanitized:
        raise ValueError("Choose a safe folder name.")
    return sanitized


def safe_storage_file_name(file_name: str) -> str:
    raw_name = str(file_name or "").strip().replace("\\", "/")
    name = PurePosixPath(raw_name).name.strip()
    if not name or name in {".", ".."}:
        raise ValueError("Every DataExchange file must include a safe file name.")
    if re.match(r"^[a-zA-Z]:", name) or any(ord(character) < 32 for character in name):
        raise ValueError("Every DataExchange file must include a safe file name.")
    sanitized = re.sub(r"[^A-Za-z0-9._ -]+", "_", name).strip(" .")
    return sanitized or f"exchange-file-{uuid4().hex[:8]}"


def queryable_extension(file_name: str) -> str:
    suffix = PurePosixPath(str(file_name or "")).suffix.lstrip(".").lower()
    return suffix if suffix in DATA_EXCHANGE_QUERYABLE_EXTENSIONS else ""


@dataclass(frozen=True, slots=True)
class DataExchangeDownloadStream:
    body: object
    filename: str
    content_type: str
    content_length: int | None


@dataclass(frozen=True, slots=True)
class _DownloadToken:
    file_id: str
    expires_at: datetime


class DataExchangeManager:
    def __init__(
        self,
        *,
        settings: Settings,
        store: DataExchangeStore,
        s3_client_factory=s3_client,
    ) -> None:
        self._settings = settings
        self._store = store
        self._s3_client_factory = s3_client_factory
        self._lock = RLock()
        self._download_tokens: dict[str, _DownloadToken] = {}

    @property
    def exchange_prefix(self) -> str:
        return normalize_data_exchange_prefix(self._settings.data_exchange_prefix)

    def list_files(self) -> dict[str, object]:
        records = sorted(
            self._store.list_records(),
            key=lambda item: item.uploaded_at,
            reverse=True,
        )
        folders = sorted(
            self._store.list_folders(),
            key=lambda item: item.name.lower(),
        )
        return {
            "files": [self._public_payload(record) for record in records],
            "folders": [folder.payload for folder in folders],
        }

    def create_folder(self, *, name: str, parent_folder_id: str = "") -> dict[str, object]:
        normalized_name = safe_folder_name(name)
        normalized_parent_id = str(parent_folder_id or "").strip()
        folders = self._store.list_folders()
        if normalized_parent_id and not any(
            folder.folder_id == normalized_parent_id for folder in folders
        ):
            raise KeyError(f"Unknown DataExchange folder: {normalized_parent_id}")
        for folder in folders:
            if (
                folder.parent_folder_id == normalized_parent_id
                and folder.name.lower() == normalized_name.lower()
            ):
                raise ValueError("A DataExchange folder with this name already exists here.")
        now = utc_now_iso()
        folder = DataExchangeFolderRecord(
            folder_id=uuid4().hex,
            name=normalized_name,
            parent_folder_id=normalized_parent_id,
            created_at=now,
            updated_at=now,
        )
        self._store.upsert_folder(folder)
        return folder.payload

    def delete_folder(self, *, folder_id: str) -> dict[str, object]:
        normalized_folder_id = str(folder_id or "").strip()
        if any(
            folder.parent_folder_id == normalized_folder_id
            for folder in self._store.list_folders()
        ):
            raise ValueError("Only empty DataExchange folders can be deleted.")
        if any(
            record.folder_id == normalized_folder_id
            for record in self._store.list_records()
        ):
            raise ValueError("Only empty DataExchange folders can be deleted.")
        folder = self._store.delete_folder(normalized_folder_id)
        return {"ok": True, "folderId": folder.folder_id}

    def store_uploaded_sources(
        self,
        *,
        sources: list[IngestionLocalSource],
        file_password: str,
        display_name: str = "",
        description: str = "",
        owner_contact: str = "",
        tags: list[str] | str | None = None,
        folder_id: str = "",
    ) -> dict[str, object]:
        if not sources:
            raise ValueError("Choose at least one file before uploading to DataExchange.")

        bucket = str(self._settings.s3_bucket or "").strip()
        if not bucket:
            raise ValueError("Configure S3_BUCKET before uploading to DataExchange.")

        ensure_s3_bucket(self._settings, bucket)
        client = self._s3_client_factory(self._settings)
        uploaded: list[dict[str, object]] = []
        failed: list[dict[str, object]] = []
        batch_tags = normalize_tags(tags)
        now = utc_now_iso()
        password_hash = hash_password(file_password) if str(file_password or "") else ""
        normalized_folder_id = str(folder_id or "").strip()
        if normalized_folder_id:
            self._store.folder(normalized_folder_id)

        for source in sources:
            try:
                file_name = safe_storage_file_name(source.file_name)
                size_bytes = source.local_path.stat().st_size
                if size_bytes > self._settings.data_exchange_upload_max_bytes:
                    raise ValueError(
                        f"The file '{file_name}' exceeds the configured DataExchange upload size limit."
                    )
                file_id = uuid4().hex
                object_key = f"{self.exchange_prefix}files/{file_id}/{file_name}"
                upload_s3_file(
                    client,
                    local_path=source.local_path,
                    bucket=bucket,
                    key=object_key,
                    metadata={"bdw-data-exchange-file-id": file_id},
                    transfer_config=TransferConfig(
                        multipart_threshold=64 * 1024 * 1024,
                        multipart_chunksize=64 * 1024 * 1024,
                        max_concurrency=4,
                    ),
                )
                record = DataExchangeFileRecord(
                    file_id=file_id,
                    file_name=file_name,
                    display_name=str(display_name or "").strip() or file_name,
                    description=str(description or "").strip(),
                    owner_contact=str(owner_contact or "").strip(),
                    tags=batch_tags,
                    folder_id=normalized_folder_id,
                    bucket=bucket,
                    s3_key=object_key,
                    size_bytes=size_bytes,
                    content_type=mimetypes.guess_type(file_name)[0] or "application/octet-stream",
                    uploaded_at=now,
                    updated_at=now,
                    password_hash=password_hash,
                )
                self._store.upsert_record(record)
                uploaded.append(
                    {
                        "fileName": file_name,
                        "status": "imported",
                        "dataExchangeFile": self._public_payload(record),
                    }
                )
            except Exception as exc:
                failed.append(
                    {
                        "fileName": str(source.file_name or "file").strip() or "file",
                        "status": "failed",
                        "error": str(exc),
                    }
                )

        imports = [*uploaded, *failed]
        return {
            "targetId": "data-exchange",
            "importedCount": len(uploaded),
            "failedCount": len(failed),
            "imports": imports,
            "files": [item["dataExchangeFile"] for item in uploaded],
        }

    def update_metadata(
        self,
        *,
        file_id: str,
        file_password: str,
        display_name: str = "",
        description: str = "",
        owner_contact: str = "",
        tags: list[str] | str | None = None,
        folder_id: str | None = None,
    ) -> dict[str, object]:
        record = self._require_file_password(file_id, file_password)
        if folder_id is None:
            normalized_folder_id = record.folder_id
        else:
            normalized_folder_id = str(folder_id or "").strip()
            if normalized_folder_id:
                self._store.folder(normalized_folder_id)
        updated = DataExchangeFileRecord(
            file_id=record.file_id,
            file_name=record.file_name,
            display_name=str(display_name or "").strip() or record.file_name,
            description=str(description or "").strip(),
            owner_contact=str(owner_contact or "").strip(),
            tags=normalize_tags(tags),
            folder_id=normalized_folder_id,
            bucket=record.bucket,
            s3_key=record.s3_key,
            size_bytes=record.size_bytes,
            content_type=record.content_type,
            uploaded_at=record.uploaded_at,
            updated_at=utc_now_iso(),
            password_hash=record.password_hash,
        )
        self._store.upsert_record(updated)
        return self._public_payload(updated)

    def delete_file(
        self,
        *,
        file_id: str,
        file_password: str,
    ) -> dict[str, object]:
        record = self._require_file_password(file_id, file_password)
        client = self._s3_client_factory(self._settings)
        client.delete_object(Bucket=record.bucket, Key=record.s3_key)
        self._store.delete_record(record.file_id)
        return {"ok": True, "fileId": record.file_id}

    def create_download_token(
        self,
        *,
        file_id: str,
        file_password: str,
    ) -> dict[str, object]:
        record = self._require_file_password(file_id, file_password)
        token = uuid4().hex
        expires_at = datetime.now(UTC) + timedelta(
            seconds=self._settings.data_exchange_download_token_ttl_seconds
        )
        with self._lock:
            self._download_tokens[token] = _DownloadToken(
                file_id=record.file_id,
                expires_at=expires_at,
            )
            self._remove_expired_download_tokens_locked()
        return {
            "token": token,
            "expiresAt": expires_at.isoformat().replace("+00:00", "Z"),
            "downloadUrl": f"/api/data-exchange/files/{record.file_id}/download?token={token}",
        }

    def prepared_download_source(
        self,
        *,
        file_id: str,
        file_password: str,
    ) -> dict[str, object]:
        record = self._require_file_password(file_id, file_password)
        extension = queryable_extension(record.file_name)
        if extension != "csv":
            raise ValueError("Prepared ZIP downloads currently support CSV files.")
        return {
            "sourceKind": "data_exchange_file",
            "dataExchangeFileId": record.file_id,
            "bucket": record.bucket,
            "key": record.s3_key,
            "filename": record.file_name,
            "sourceName": record.display_name or record.file_name,
            "sourceSizeBytes": record.size_bytes,
            "sourceRevision": f"{record.updated_at}:{record.size_bytes}",
            "format": extension,
        }

    def stream_download(self, *, file_id: str, token: str) -> DataExchangeDownloadStream:
        normalized_token = str(token or "").strip()
        with self._lock:
            self._remove_expired_download_tokens_locked()
            token_state = self._download_tokens.get(normalized_token)
            if token_state is None or token_state.file_id != str(file_id or "").strip():
                raise PermissionError("The DataExchange download token is invalid or expired.")
        record = self._store.record(file_id)
        response = self._s3_client_factory(self._settings).get_object(
            Bucket=record.bucket,
            Key=record.s3_key,
        )
        content_type = (
            str(response.get("ContentType") or "").strip()
            or record.content_type
            or mimetypes.guess_type(record.file_name)[0]
            or "application/octet-stream"
        )
        raw_length = response.get("ContentLength")
        content_length = int(raw_length) if raw_length is not None else record.size_bytes
        return DataExchangeDownloadStream(
            body=response["Body"],
            filename=record.file_name,
            content_type=content_type,
            content_length=content_length,
        )

    def copy_to_shared_s3(
        self,
        *,
        file_id: str,
        file_password: str,
        bucket: str = "",
        prefix: str = "",
        file_name: str = "",
    ) -> dict[str, object]:
        record = self._require_file_password(file_id, file_password)
        extension = queryable_extension(record.file_name)
        if not extension:
            raise ValueError("Only queryable formats can be copied from DataExchange to S3 Object Storage.")

        target_bucket = str(bucket or "").strip() or str(self._settings.s3_bucket or "").strip()
        if not target_bucket:
            raise ValueError("Choose a S3 Object Storage bucket.")
        target_name = safe_storage_file_name(file_name or record.file_name)
        normalized_prefix = "/".join(
            segment.strip()
            for segment in str(prefix or "").replace("\\", "/").split("/")
            if segment.strip()
        )
        target_key = f"{normalized_prefix}/{target_name}" if normalized_prefix else target_name
        reject_hidden_s3_location(
            target_bucket,
            target_key,
            self._settings,
            data_exchange_prefix=self.exchange_prefix,
        )
        if target_bucket == record.bucket and is_data_exchange_key(target_key, self.exchange_prefix):
            raise ValueError("DataExchange files cannot be copied back into the hidden exchange prefix.")

        ensure_s3_bucket(self._settings, target_bucket)
        client = self._s3_client_factory(self._settings)
        client.copy_object(
            Bucket=target_bucket,
            Key=target_key,
            CopySource={"Bucket": record.bucket, "Key": record.s3_key},
            Metadata={"bdw-data-exchange-origin": record.file_id},
            MetadataDirective="REPLACE",
        )
        return {
            "targetId": "s3",
            "importedCount": 1,
            "failedCount": 0,
            "imports": [
                {
                    "fileName": record.file_name,
                    "status": "imported",
                    "destination": "s3",
                    "bucket": target_bucket,
                    "objectKey": target_key,
                    "objectKeyPrefix": normalized_prefix,
                    "storedFileName": target_name,
                    "path": f"s3://{target_bucket}/{target_key}",
                    "storageFormat": extension,
                }
            ],
        }

    def local_workspace_handoff(
        self,
        *,
        file_id: str,
        file_password: str,
    ) -> dict[str, object]:
        record = self._require_file_password(file_id, file_password)
        extension = queryable_extension(record.file_name)
        if not extension:
            raise ValueError("Only queryable formats can be copied from DataExchange to Local Workspace.")
        token = self.create_download_token(
            file_id=record.file_id,
            file_password=file_password,
        )
        return {
            **token,
            "file": self._public_payload(record),
            "localWorkspace": {
                "fileName": record.file_name,
                "folderPath": "DataExchange",
                "exportFormat": extension,
                "mimeType": record.content_type or "application/octet-stream",
                "sizeBytes": record.size_bytes,
            },
        }

    def _public_payload(self, record: DataExchangeFileRecord) -> dict[str, object]:
        payload = record.payload
        payload["isQueryable"] = bool(queryable_extension(record.file_name))
        return payload

    def _require_file_password(self, file_id: str, file_password: str) -> DataExchangeFileRecord:
        record = self._store.record(file_id)
        if not record.password_hash:
            return record
        if not verify_password(str(file_password or ""), record.password_hash):
            raise PermissionError("The DataExchange file password is invalid.")
        return record

    def _remove_expired_download_tokens_locked(self) -> None:
        now = datetime.now(UTC)
        expired = [
            token
            for token, token_state in self._download_tokens.items()
            if token_state.expires_at <= now
        ]
        for token in expired:
            self._download_tokens.pop(token, None)

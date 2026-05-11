from __future__ import annotations

from pathlib import PurePosixPath
import re

from ...config import Settings
from ..ingestion_types.common.uploads import (
    IngestionLocalSource,
    IngestionUploadFileRequest,
    IngestionUploadSessionManager,
    _isoformat,
    _utc_now,
)


DataExchangeUploadFileRequest = IngestionUploadFileRequest


def _is_safe_file_name(file_name: str) -> bool:
    if not file_name or file_name in {".", ".."}:
        return False
    if "/" in file_name or "\\" in file_name:
        return False
    if re.match(r"^[a-zA-Z]:", file_name):
        return False
    return not any(ord(character) < 32 for character in file_name)


class DataExchangeUploadSessionManager(IngestionUploadSessionManager):
    def __init__(self, *, settings: Settings) -> None:
        super().__init__(
            settings=settings,
            allowed_extensions=(),
            format_label="DataExchange",
            empty_files_message="Choose at least one file before uploading to DataExchange.",
            invalid_extension_message="DataExchange accepts arbitrary files.",
            direct_file_size_limit=lambda app_settings: app_settings.data_exchange_upload_max_bytes,
            source_factory=lambda file_name, local_path: IngestionLocalSource(
                file_name=file_name,
                local_path=local_path,
            ),
        )

    def create_session(self, files: list[IngestionUploadFileRequest]) -> dict[str, object]:
        if not files:
            raise ValueError("Choose at least one file before uploading to DataExchange.")

        with self._lock:
            self.cleanup_expired_sessions()
            self._root.mkdir(parents=True, exist_ok=True)
            import uuid

            session_id = uuid.uuid4().hex
            now = _utc_now()
            expires_at = now + self._session_ttl()
            session_dir = self._session_dir(session_id)
            files_dir = session_dir / "files"
            files_dir.mkdir(parents=True, exist_ok=False)
            file_entries: list[dict[str, object]] = []
            for request in files:
                file_name = self._safe_upload_file_name(request.file_name)
                size_bytes = int(request.size_bytes)
                if size_bytes <= 0:
                    raise ValueError(f"The file '{file_name}' is empty.")
                if size_bytes > self._settings.data_exchange_upload_max_bytes:
                    raise ValueError(f"The file '{file_name}' exceeds the configured DataExchange upload size limit.")
                file_id = uuid.uuid4().hex
                file_entries.append(
                    {
                        "fileId": file_id,
                        "fileName": file_name,
                        "sizeBytes": size_bytes,
                        "receivedBytes": 0,
                        "complete": False,
                        "path": str(files_dir / f"{file_id}.upload"),
                    }
                )

            state = {
                "sessionId": session_id,
                "createdAt": _isoformat(now),
                "expiresAt": _isoformat(expires_at),
                "status": "uploading",
                "chunkSizeBytes": self._settings.ingestion_upload_chunk_bytes,
                "files": file_entries,
            }
            self._write_state(session_id, state)
            return self._public_state(state)

    def _session_ttl(self):
        from datetime import timedelta

        return timedelta(hours=self._settings.ingestion_upload_session_ttl_hours)

    def _safe_upload_file_name(self, file_name: str) -> str:
        raw_name = str(file_name or "").strip()
        if "/" in raw_name or "\\" in raw_name:
            raise ValueError("Every DataExchange upload must use a safe file name.")
        name = PurePosixPath(raw_name.replace("\\", "/")).name.strip()
        if not _is_safe_file_name(name):
            raise ValueError("Every DataExchange upload must use a safe file name.")
        return name

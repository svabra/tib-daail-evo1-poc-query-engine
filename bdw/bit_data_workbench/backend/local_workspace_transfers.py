from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import shutil
import tempfile

from ..config import Settings
from .data_sources.s3.explorer import normalize_s3_bucket_name, normalize_s3_prefix, s3_path
from .s3_storage import ensure_s3_bucket, s3_client, upload_s3_file


def normalize_local_workspace_transfer_filename(
    file_name: str | None,
    *,
    fallback: str = "local-workspace-file",
) -> str:
    candidate = str(file_name or "").strip()
    if not candidate:
        candidate = fallback

    candidate = PurePosixPath(candidate.replace("\\", "/")).name.strip()
    if not candidate or candidate in {".", ".."}:
        raise ValueError("Provide a file name.")
    if "/" in candidate or "\\" in candidate:
        raise ValueError("File names cannot include folder separators.")
    return candidate


@dataclass(slots=True)
class LocalWorkspaceS3MoveResult:
    entry_id: str
    bucket: str
    key: str
    filename: str
    path: str
    message: str

    @property
    def payload(self) -> dict[str, str]:
        return {
            "entryId": self.entry_id,
            "bucket": self.bucket,
            "key": self.key,
            "fileName": self.filename,
            "path": self.path,
            "message": self.message,
        }


class LocalWorkspaceTransferManager:
    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings

    def move_to_s3(
        self,
        *,
        entry_id: str,
        file_name: str,
        fallback_file_name: str = "local-workspace-file",
        mime_type: str = "",
        file_bytes: bytes,
        bucket: str,
        prefix: str = "",
    ) -> LocalWorkspaceS3MoveResult:
        return self._upload_to_s3(
            entry_id=entry_id,
            file_name=file_name,
            fallback_file_name=fallback_file_name,
            mime_type=mime_type,
            file_bytes=file_bytes,
            bucket=bucket,
            prefix=prefix,
            operation_label="Moved",
        )

    def copy_to_s3(
        self,
        *,
        entry_id: str,
        file_name: str,
        fallback_file_name: str = "local-workspace-file",
        mime_type: str = "",
        file_bytes: bytes,
        bucket: str,
        prefix: str = "",
    ) -> LocalWorkspaceS3MoveResult:
        return self._upload_to_s3(
            entry_id=entry_id,
            file_name=file_name,
            fallback_file_name=fallback_file_name,
            mime_type=mime_type,
            file_bytes=file_bytes,
            bucket=bucket,
            prefix=prefix,
            operation_label="Copied",
        )

    def _upload_to_s3(
        self,
        *,
        entry_id: str,
        file_name: str,
        fallback_file_name: str = "local-workspace-file",
        mime_type: str = "",
        file_bytes: bytes,
        bucket: str,
        prefix: str = "",
        operation_label: str,
    ) -> LocalWorkspaceS3MoveResult:
        normalized_entry_id = str(entry_id or "").strip()
        if not normalized_entry_id:
            raise ValueError(
                "Missing Local Workspace entry id for Shared Workspace transfer."
            )
        if not file_bytes:
            raise ValueError(
                "The Local Workspace file is empty and cannot be transferred."
            )

        normalized_bucket = normalize_s3_bucket_name(bucket)
        normalized_prefix = normalize_s3_prefix(prefix)
        normalized_file_name = normalize_local_workspace_transfer_filename(
            file_name,
            fallback=fallback_file_name,
        )
        key = f"{normalized_prefix}{normalized_file_name}"
        metadata = {"bdw_source": "local-workspace"}
        normalized_mime_type = str(mime_type or "").strip()
        if normalized_mime_type:
            metadata["bdw_mime_type"] = normalized_mime_type

        temp_dir = Path(tempfile.mkdtemp(prefix="bdw-local-workspace-s3-"))
        local_path = temp_dir / normalized_file_name
        local_path.write_bytes(file_bytes)

        try:
            ensure_s3_bucket(self._settings, normalized_bucket)
            upload_s3_file(
                s3_client(self._settings),
                local_path=local_path,
                bucket=normalized_bucket,
                key=key,
                metadata=metadata,
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        path = f"{s3_path(normalized_bucket, normalized_prefix)}{normalized_file_name}"
        return LocalWorkspaceS3MoveResult(
            entry_id=normalized_entry_id,
            bucket=normalized_bucket,
            key=key,
            filename=normalized_file_name,
            path=path,
            message=f"{operation_label} {normalized_file_name} to {path}.",
        )

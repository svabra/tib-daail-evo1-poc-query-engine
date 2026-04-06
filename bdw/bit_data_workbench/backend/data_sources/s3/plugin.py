from __future__ import annotations

from ....config import Settings
from ..base import DataSourceCreateRequest, DataSourceDeleteRequest, DataSourcePlugin
from .explorer import S3ExplorerManager


class S3DataSourcePlugin(DataSourcePlugin):
    def __init__(self, settings: Settings) -> None:
        self._explorer = S3ExplorerManager(settings)

    @property
    def source_id(self) -> str:
        return "workspace.s3"

    @property
    def source_label(self) -> str:
        return "S3 Object Storage"

    @property
    def source_type(self) -> str:
        return "s3"

    def supports_create(self, kind: str) -> bool:
        return str(kind or "").strip().lower() in {"bucket", "folder"}

    def supports_delete(self, kind: str) -> bool:
        return str(kind or "").strip().lower() in {"bucket", "folder", "file"}

    def snapshot(self, *, bucket: str = "", prefix: str = ""):
        return self._explorer.snapshot(bucket=bucket, prefix=prefix)

    def create(self, request: DataSourceCreateRequest):
        normalized_kind = str(request.kind or "").strip().lower()
        if normalized_kind == "bucket":
            return self._explorer.create_bucket(request.name)
        if normalized_kind == "folder":
            return self._explorer.create_folder(
                bucket=request.container,
                prefix=request.path,
                folder_name=request.name,
            )
        return super().create(request)

    def delete(self, request: DataSourceDeleteRequest):
        return self._explorer.delete_entry(
            entry_kind=request.kind,
            bucket=request.container,
            prefix=request.path,
        )

    def download_object(self, *, bucket: str, key: str, file_name: str = ""):
        return self._explorer.download_object(bucket=bucket, key=key, file_name=file_name)
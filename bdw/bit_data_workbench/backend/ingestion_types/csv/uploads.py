from __future__ import annotations

from pathlib import Path

from ....config import Settings
from ..common import IngestionUploadFileRequest, IngestionUploadSessionManager
from .manager import CsvLocalSource


CsvUploadFileRequest = IngestionUploadFileRequest


class CsvUploadSessionManager(IngestionUploadSessionManager):
    def __init__(self, *, settings: Settings) -> None:
        super().__init__(
            settings=settings,
            allowed_extensions=(".csv",),
            format_label="CSV",
            empty_files_message="Choose at least one CSV or ZIP file before importing.",
            invalid_extension_message="Only .csv and .zip files are supported in this ingestion flow.",
            direct_file_size_limit=lambda app_settings: app_settings.ingestion_upload_max_csv_bytes,
            source_factory=lambda file_name, local_path: CsvLocalSource(
                file_name=file_name,
                local_path=Path(local_path),
            ),
        )

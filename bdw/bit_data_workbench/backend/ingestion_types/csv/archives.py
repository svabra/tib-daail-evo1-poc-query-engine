from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..common import ExtractedArchiveFile, extract_archive_files


ExtractedCsvFile = ExtractedArchiveFile


@dataclass(frozen=True, slots=True)
class CsvArchivePolicy:
    max_archive_bytes: int
    max_csv_bytes: int
    max_extracted_bytes: int
    max_entries: int
    max_expansion_ratio: float
    copy_chunk_bytes: int = 1024 * 1024

    @property
    def max_entry_bytes(self) -> int:
        return self.max_csv_bytes


def extract_csv_archive(
    *,
    archive_path: Path,
    output_dir: Path,
    policy: CsvArchivePolicy,
) -> list[ExtractedCsvFile]:
    return extract_archive_files(
        archive_path=archive_path,
        output_dir=output_dir,
        policy=policy,
        allowed_extensions=(".csv",),
        format_label="CSV",
    )

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile


CSV_ARCHIVE_ALLOWED_COMPRESSION = {
    zipfile.ZIP_STORED,
    zipfile.ZIP_DEFLATED,
}


@dataclass(frozen=True, slots=True)
class CsvArchivePolicy:
    max_archive_bytes: int
    max_csv_bytes: int
    max_extracted_bytes: int
    max_entries: int
    max_expansion_ratio: float
    copy_chunk_bytes: int = 1024 * 1024


@dataclass(frozen=True, slots=True)
class ExtractedCsvFile:
    file_name: str
    local_path: Path


def _safe_csv_archive_member_name(raw_name: str) -> str:
    normalized_name = str(raw_name or "").replace("\\", "/").strip()
    if not normalized_name:
        raise ValueError("ZIP archive contains an entry without a file name.")
    if normalized_name.startswith("/"):
        raise ValueError(f"ZIP archive entry '{raw_name}' uses an absolute path.")

    parts = PurePosixPath(normalized_name).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"ZIP archive entry '{raw_name}' uses an unsafe path.")
    if parts[0].endswith(":") or re.match(r"^[a-zA-Z]:$", parts[0]):
        raise ValueError(f"ZIP archive entry '{raw_name}' uses a drive-letter path.")

    file_name = parts[-1].strip()
    if not file_name:
        raise ValueError(f"ZIP archive entry '{raw_name}' is missing a file name.")
    if not file_name.lower().endswith(".csv"):
        raise ValueError(
            f"ZIP archive entry '{raw_name}' is not a CSV file. "
            "Archives may contain directories and .csv files only."
        )
    return file_name


def _zip_info_is_directory(info: zipfile.ZipInfo) -> bool:
    return info.is_dir() or str(info.filename or "").replace("\\", "/").endswith("/")


def _zip_info_is_symlink(info: zipfile.ZipInfo) -> bool:
    mode = (int(info.external_attr or 0) >> 16) & 0o777777
    return stat.S_ISLNK(mode)


def _unique_output_name(file_name: str, seen: dict[str, int]) -> str:
    normalized_key = file_name.lower()
    next_index = seen.get(normalized_key, 0)
    seen[normalized_key] = next_index + 1
    if next_index == 0:
        return file_name

    path = Path(file_name)
    stem = path.stem or "csv-import"
    suffix = path.suffix or ".csv"
    return f"{stem}_{next_index + 1}{suffix}"


def _validate_zip_member(info: zipfile.ZipInfo, *, policy: CsvArchivePolicy) -> str | None:
    if _zip_info_is_directory(info):
        return None
    if int(info.flag_bits or 0) & 0x1:
        raise ValueError(f"ZIP archive entry '{info.filename}' is encrypted.")
    if _zip_info_is_symlink(info):
        raise ValueError(f"ZIP archive entry '{info.filename}' is a symbolic link.")
    if info.compress_type not in CSV_ARCHIVE_ALLOWED_COMPRESSION:
        raise ValueError(
            f"ZIP archive entry '{info.filename}' uses an unsupported compression method."
        )
    file_name = _safe_csv_archive_member_name(info.filename)
    if int(info.file_size or 0) > policy.max_csv_bytes:
        raise ValueError(
            f"ZIP archive entry '{info.filename}' exceeds the configured CSV size limit."
        )
    compressed_size = int(info.compress_size or 0)
    if compressed_size > 0:
        ratio = int(info.file_size or 0) / compressed_size
        if ratio > policy.max_expansion_ratio:
            raise ValueError(
                f"ZIP archive entry '{info.filename}' expands too much to be accepted."
            )
    return file_name


def extract_csv_archive(
    *,
    archive_path: Path,
    output_dir: Path,
    policy: CsvArchivePolicy,
) -> list[ExtractedCsvFile]:
    archive_size = archive_path.stat().st_size
    if archive_size > policy.max_archive_bytes:
        raise ValueError("The ZIP archive exceeds the configured upload size limit.")

    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        archive = zipfile.ZipFile(archive_path)
    except zipfile.BadZipFile as exc:
        raise ValueError("The uploaded .zip file is not a valid ZIP archive.") from exc

    extracted: list[ExtractedCsvFile] = []
    seen_names: dict[str, int] = {}
    total_declared_bytes = 0
    total_actual_bytes = 0

    with archive:
        members: list[tuple[zipfile.ZipInfo, str]] = []
        for info in archive.infolist():
            file_name = _validate_zip_member(info, policy=policy)
            if file_name is None:
                continue
            members.append((info, file_name))
            total_declared_bytes += int(info.file_size or 0)
            if len(members) > policy.max_entries:
                raise ValueError("The ZIP archive contains too many CSV files.")
            if total_declared_bytes > policy.max_extracted_bytes:
                raise ValueError(
                    "The ZIP archive expands beyond the configured extracted-size limit."
                )

        if not members:
            raise ValueError("The ZIP archive does not contain any CSV files.")

        for info, file_name in members:
            output_name = _unique_output_name(file_name, seen_names)
            output_path = output_dir / output_name
            actual_entry_bytes = 0
            try:
                with archive.open(info, "r") as input_file, output_path.open("wb") as output_file:
                    while True:
                        chunk = input_file.read(policy.copy_chunk_bytes)
                        if not chunk:
                            break
                        actual_entry_bytes += len(chunk)
                        total_actual_bytes += len(chunk)
                        if actual_entry_bytes > policy.max_csv_bytes:
                            raise ValueError(
                                f"ZIP archive entry '{info.filename}' exceeds the configured CSV size limit."
                            )
                        if total_actual_bytes > policy.max_extracted_bytes:
                            raise ValueError(
                                "The ZIP archive expands beyond the configured extracted-size limit."
                            )
                        output_file.write(chunk)
            except Exception:
                try:
                    output_path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
            extracted.append(ExtractedCsvFile(file_name=output_name, local_path=output_path))

    return extracted

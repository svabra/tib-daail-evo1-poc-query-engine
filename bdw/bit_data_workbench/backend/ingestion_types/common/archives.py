from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile


ARCHIVE_ALLOWED_COMPRESSION = {
    zipfile.ZIP_STORED,
    zipfile.ZIP_DEFLATED,
}


@dataclass(frozen=True, slots=True)
class ArchivePolicy:
    max_archive_bytes: int
    max_entry_bytes: int
    max_extracted_bytes: int
    max_entries: int
    max_expansion_ratio: float
    copy_chunk_bytes: int = 1024 * 1024

    @property
    def max_csv_bytes(self) -> int:
        return self.max_entry_bytes


@dataclass(frozen=True, slots=True)
class ExtractedArchiveFile:
    file_name: str
    local_path: Path


@dataclass(frozen=True, slots=True)
class ArchiveMemberFailure:
    file_name: str
    error: str


def _safe_archive_member_name(
    raw_name: str,
    *,
    allowed_extensions: tuple[str, ...],
    format_label: str,
) -> str:
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
    if not file_name.lower().endswith(allowed_extensions):
        if format_label.strip().upper() == "CSV":
            raise ValueError(
                f"ZIP archive entry '{raw_name}' is not a CSV file. "
                "Archives may contain directories and .csv files only."
            )
        allowed = ", ".join(allowed_extensions)
        raise ValueError(
            f"ZIP archive entry '{raw_name}' is not a supported {format_label} file. "
            f"Archives may contain directories and {allowed} files only."
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
    stem = path.stem or "ingestion-import"
    suffix = path.suffix
    return f"{stem}_{next_index + 1}{suffix}"


def _validate_zip_member(
    info: zipfile.ZipInfo,
    *,
    policy: ArchivePolicy,
    allowed_extensions: tuple[str, ...],
    format_label: str,
) -> str | None:
    if _zip_info_is_directory(info):
        return None
    if int(info.flag_bits or 0) & 0x1:
        raise ValueError(f"ZIP archive entry '{info.filename}' is encrypted.")
    if _zip_info_is_symlink(info):
        raise ValueError(f"ZIP archive entry '{info.filename}' is a symbolic link.")
    if info.compress_type not in ARCHIVE_ALLOWED_COMPRESSION:
        raise ValueError(
            f"ZIP archive entry '{info.filename}' uses an unsupported compression method."
        )
    file_name = _safe_archive_member_name(
        info.filename,
        allowed_extensions=allowed_extensions,
        format_label=format_label,
    )
    if int(info.file_size or 0) > policy.max_entry_bytes:
        raise ValueError(
            f"ZIP archive entry '{info.filename}' exceeds the configured {format_label} size limit."
        )
    compressed_size = int(info.compress_size or 0)
    if compressed_size > 0:
        ratio = int(info.file_size or 0) / compressed_size
        if ratio > policy.max_expansion_ratio:
            raise ValueError(
                f"ZIP archive entry '{info.filename}' expands too much to be accepted."
            )
    return file_name


def extract_archive_files(
    *,
    archive_path: Path,
    output_dir: Path,
    policy: ArchivePolicy,
    allowed_extensions: tuple[str, ...],
    format_label: str,
) -> list[ExtractedArchiveFile]:
    extracted, failures = extract_archive_files_with_failures(
        archive_path=archive_path,
        output_dir=output_dir,
        policy=policy,
        allowed_extensions=allowed_extensions,
        format_label=format_label,
    )
    if failures:
        raise ValueError(failures[0].error)
    return extracted


def extract_archive_files_with_failures(
    *,
    archive_path: Path,
    output_dir: Path,
    policy: ArchivePolicy,
    allowed_extensions: tuple[str, ...],
    format_label: str,
) -> tuple[list[ExtractedArchiveFile], list[ArchiveMemberFailure]]:
    archive_size = archive_path.stat().st_size
    if archive_size > policy.max_archive_bytes:
        raise ValueError("The ZIP archive exceeds the configured upload size limit.")

    normalized_extensions = tuple(str(item).lower() for item in allowed_extensions)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        archive = zipfile.ZipFile(archive_path)
    except zipfile.BadZipFile as exc:
        raise ValueError("The uploaded .zip file is not a valid ZIP archive.") from exc

    extracted: list[ExtractedArchiveFile] = []
    failures: list[ArchiveMemberFailure] = []
    seen_names: dict[str, int] = {}
    total_declared_bytes = 0
    total_actual_bytes = 0

    with archive:
        members: list[tuple[zipfile.ZipInfo, str]] = []
        for info in archive.infolist():
            try:
                file_name = _validate_zip_member(
                    info,
                    policy=policy,
                    allowed_extensions=normalized_extensions,
                    format_label=format_label,
                )
            except ValueError as exc:
                if "not a supported" not in str(exc):
                    raise
                failures.append(
                    ArchiveMemberFailure(
                        file_name=Path(str(info.filename or "unsupported")).name or "unsupported",
                        error=str(exc),
                    )
                )
                continue
            if file_name is None:
                continue
            members.append((info, file_name))
            total_declared_bytes += int(info.file_size or 0)
            if len(members) > policy.max_entries:
                raise ValueError(f"The ZIP archive contains too many {format_label} files.")
            if total_declared_bytes > policy.max_extracted_bytes:
                raise ValueError(
                    "The ZIP archive expands beyond the configured extracted-size limit."
                )

        if not members and not failures:
            raise ValueError(f"The ZIP archive does not contain any {format_label} files.")

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
                        if actual_entry_bytes > policy.max_entry_bytes:
                            raise ValueError(
                                f"ZIP archive entry '{info.filename}' exceeds the configured {format_label} size limit."
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
            extracted.append(ExtractedArchiveFile(file_name=output_name, local_path=output_path))

    return extracted, failures

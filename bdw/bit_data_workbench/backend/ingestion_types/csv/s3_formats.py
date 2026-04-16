from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from ...sql_utils import sql_literal
from .dialect import csv_s3_metadata, normalize_csv_delimiter


SUPPORTED_CSV_S3_STORAGE_FORMATS = {"csv", "json", "parquet"}


@dataclass(frozen=True, slots=True)
class CsvS3UploadArtifact:
    local_path: Path
    file_name: str
    storage_format: str
    metadata: dict[str, str]


def normalize_csv_s3_storage_format(value: str) -> str:
    normalized_value = str(value or "").strip().lower() or "csv"
    if normalized_value not in SUPPORTED_CSV_S3_STORAGE_FORMATS:
        raise ValueError(
            "Shared Workspace S3 storage format must be one of: csv, json, parquet."
        )
    return normalized_value


def resolve_csv_s3_file_name(file_name: str, storage_format: str) -> str:
    normalized_storage_format = normalize_csv_s3_storage_format(storage_format)
    normalized_file_name = Path(str(file_name or "").strip()).name or "csv-import.csv"
    if normalized_storage_format == "csv":
        return normalized_file_name

    normalized_stem = Path(normalized_file_name).stem.strip() or "csv_import"
    if normalized_storage_format == "json":
        return f"{normalized_stem}.jsonl"
    return f"{normalized_stem}.{normalized_storage_format}"


def build_csv_s3_upload_artifact(
    *,
    local_path: Path,
    file_name: str,
    storage_format: str,
    delimiter: str = "",
    has_header: bool = True,
) -> CsvS3UploadArtifact:
    normalized_storage_format = normalize_csv_s3_storage_format(storage_format)
    resolved_file_name = resolve_csv_s3_file_name(file_name, normalized_storage_format)

    if normalized_storage_format == "csv":
        return CsvS3UploadArtifact(
            local_path=local_path,
            file_name=resolved_file_name,
            storage_format=normalized_storage_format,
            metadata=csv_s3_metadata(
                delimiter=delimiter,
                has_header=has_header,
            ),
        )

    converted_path = local_path.with_name(resolved_file_name)
    _convert_csv_file_for_s3_storage(
        source_path=local_path,
        target_path=converted_path,
        storage_format=normalized_storage_format,
        delimiter=delimiter,
        has_header=has_header,
    )
    return CsvS3UploadArtifact(
        local_path=converted_path,
        file_name=resolved_file_name,
        storage_format=normalized_storage_format,
        metadata={},
    )


def _convert_csv_file_for_s3_storage(
    *,
    source_path: Path,
    target_path: Path,
    storage_format: str,
    delimiter: str = "",
    has_header: bool = True,
) -> None:
    normalized_storage_format = normalize_csv_s3_storage_format(storage_format)
    if normalized_storage_format == "csv":
        raise ValueError("CSV-to-CSV conversion does not require a transform step.")

    connection = duckdb.connect(":memory:")
    try:
        read_options = [f"HEADER = {'TRUE' if has_header else 'FALSE'}"]
        normalized_delimiter = normalize_csv_delimiter(delimiter)
        if normalized_delimiter:
            read_options.append(f"DELIM = {sql_literal(normalized_delimiter)}")
        source_sql = (
            f"SELECT * FROM read_csv_auto({sql_literal(source_path.as_posix())}, "
            f"{', '.join(read_options)})"
        )
        if normalized_storage_format == "parquet":
            copy_options = "FORMAT PARQUET, COMPRESSION ZSTD"
        else:
            copy_options = "FORMAT JSON"
        connection.execute(
            f"COPY ({source_sql}) TO {sql_literal(target_path.as_posix())} ({copy_options})"
        )
    finally:
        connection.close()

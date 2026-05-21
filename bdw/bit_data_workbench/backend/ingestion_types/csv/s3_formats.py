from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import shutil
import time

import duckdb

from ...sql_utils import sql_literal
from ..parquet_optimization import (
    ParquetOptimizationSettings,
    copy_query_to_optimized_parquet,
    manual_parquet_layout_requested,
    parquet_art_cache_warning,
)
from .dialect import csv_s3_metadata, normalize_csv_delimiter


SUPPORTED_CSV_S3_STORAGE_FORMATS = {"csv", "json", "parquet"}
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CsvS3UploadArtifact:
    local_path: Path
    file_name: str
    storage_format: str
    metadata: dict[str, str]
    partitioned: bool = False
    partition_columns: tuple[str, ...] = ()
    sort_columns: tuple[str, ...] = ()
    warning: str = ""

    @property
    def part_count(self) -> int:
        if not self.partitioned:
            return 1 if self.local_path.exists() else 0
        return sum(1 for path in self.local_path.rglob("*.parquet") if path.is_file())

    @property
    def size_bytes(self) -> int:
        if self.local_path.is_file():
            return self.local_path.stat().st_size
        if self.local_path.is_dir():
            return sum(path.stat().st_size for path in self.local_path.rglob("*") if path.is_file())
        return 0


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
    parquet_optimization: ParquetOptimizationSettings | None = None,
) -> CsvS3UploadArtifact:
    normalized_storage_format = normalize_csv_s3_storage_format(storage_format)
    resolved_file_name = resolve_csv_s3_file_name(file_name, normalized_storage_format)
    optimization = parquet_optimization or ParquetOptimizationSettings()

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

    partitioned = (
        normalized_storage_format == "parquet"
        and optimization.mode == "manual"
        and bool(optimization.partition_columns)
    )
    converted_path = (
        local_path.with_name(Path(resolved_file_name).stem)
        if partitioned
        else local_path.with_name(resolved_file_name)
    )
    _convert_csv_file_for_s3_storage(
        source_path=local_path,
        target_path=converted_path,
        storage_format=normalized_storage_format,
        delimiter=delimiter,
        has_header=has_header,
        parquet_optimization=optimization,
    )
    return CsvS3UploadArtifact(
        local_path=converted_path,
        file_name=Path(resolved_file_name).stem if partitioned else resolved_file_name,
        storage_format=normalized_storage_format,
        metadata={},
        partitioned=partitioned,
        partition_columns=tuple(optimization.partition_columns if partitioned else ()),
        sort_columns=tuple(optimization.sort_columns if manual_parquet_layout_requested(optimization) else ()),
        warning=parquet_art_cache_warning(optimization),
    )


def _convert_csv_file_for_s3_storage(
    *,
    source_path: Path,
    target_path: Path,
    storage_format: str,
    delimiter: str = "",
    has_header: bool = True,
    parquet_optimization: ParquetOptimizationSettings | None = None,
) -> None:
    normalized_storage_format = normalize_csv_s3_storage_format(storage_format)
    if normalized_storage_format == "csv":
        raise ValueError("CSV-to-CSV conversion does not require a transform step.")

    connection = duckdb.connect(":memory:")
    try:
        try:
            started = time.perf_counter()
            logger.info(
                "CSV S3 transform start: source=%r target=%r storage_format=%s all_varchar=%s",
                source_path.as_posix(),
                target_path.as_posix(),
                normalized_storage_format,
                False,
            )
            _copy_csv_to_storage_format(
                connection=connection,
                source_path=source_path,
                target_path=target_path,
                storage_format=normalized_storage_format,
                delimiter=delimiter,
                has_header=has_header,
                all_varchar=False,
                parquet_optimization=parquet_optimization,
            )
            logger.info(
                "CSV S3 transform completed: source=%r target=%r storage_format=%s target_size_bytes=%s elapsed_ms=%s",
                source_path.as_posix(),
                target_path.as_posix(),
                normalized_storage_format,
                _path_size(target_path),
                round((time.perf_counter() - started) * 1000),
            )
        except duckdb.Error as exc:
            if not _should_retry_csv_conversion_as_varchar(exc):
                logger.exception(
                    "CSV S3 transform failed without retry: source=%r target=%r storage_format=%s",
                    source_path.as_posix(),
                    target_path.as_posix(),
                    normalized_storage_format,
                )
                raise
            logger.warning(
                "CSV S3 transform hit a type conversion error; retrying with ALL_VARCHAR: source=%r target=%r storage_format=%s detail=%s",
                source_path.as_posix(),
                target_path.as_posix(),
                normalized_storage_format,
                exc,
            )
            if target_path.is_dir():
                shutil.rmtree(target_path)
            else:
                target_path.unlink(missing_ok=True)
            retry_started = time.perf_counter()
            _copy_csv_to_storage_format(
                connection=connection,
                source_path=source_path,
                target_path=target_path,
                storage_format=normalized_storage_format,
                delimiter=delimiter,
                has_header=has_header,
                all_varchar=True,
                parquet_optimization=parquet_optimization,
            )
            logger.info(
                "CSV S3 transform retry completed: source=%r target=%r storage_format=%s target_size_bytes=%s elapsed_ms=%s",
                source_path.as_posix(),
                target_path.as_posix(),
                normalized_storage_format,
                _path_size(target_path),
                round((time.perf_counter() - retry_started) * 1000),
            )
    finally:
        connection.close()


def _copy_csv_to_storage_format(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_path: Path,
    target_path: Path,
    storage_format: str,
    delimiter: str,
    has_header: bool,
    all_varchar: bool,
    parquet_optimization: ParquetOptimizationSettings | None = None,
) -> None:
    optimization = parquet_optimization or ParquetOptimizationSettings()
    read_options = ", ".join(
        _csv_reader_options(
            delimiter=delimiter,
            has_header=has_header,
            all_varchar=all_varchar,
        )
    )
    source_sql = (
        f"SELECT * FROM read_csv_auto({sql_literal(source_path.as_posix())}, "
        f"{read_options})"
    )
    if storage_format == "parquet":
        if manual_parquet_layout_requested(optimization):
            copy_query_to_optimized_parquet(
                connection=connection,
                source_query_sql=source_sql,
                target_path=target_path,
                optimization=optimization,
            )
            return
        copy_options = "FORMAT PARQUET, COMPRESSION ZSTD"
    else:
        copy_options = "FORMAT JSON"
    connection.execute(
        f"COPY ({source_sql}) TO {sql_literal(target_path.as_posix())} ({copy_options})"
    )


def _path_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    if path.is_dir():
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
    return 0


def _csv_reader_options(*, delimiter: str, has_header: bool, all_varchar: bool) -> list[str]:
    options = [
        f"HEADER = {'TRUE' if has_header else 'FALSE'}",
        "SAMPLE_SIZE = -1",
    ]
    normalized_delimiter = normalize_csv_delimiter(delimiter)
    if normalized_delimiter:
        options.append(f"DELIM = {sql_literal(normalized_delimiter)}")
    if all_varchar:
        options.append("ALL_VARCHAR = TRUE")
    return options


def _should_retry_csv_conversion_as_varchar(exc: duckdb.Error) -> bool:
    message = str(exc).lower()
    return "conversion error" in message and "csv error" in message

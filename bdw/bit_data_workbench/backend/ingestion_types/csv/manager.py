from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from typing import Any, BinaryIO, Callable, Protocol

import duckdb
from boto3.s3.transfer import TransferConfig

from ....config import Settings
from ...s3_storage import ensure_s3_bucket, s3_client, upload_s3_file
from ...sql_utils import sql_identifier, sql_literal
from .archives import CsvArchivePolicy, extract_csv_archive
from .dialect import normalize_csv_delimiter
from .s3_formats import build_csv_s3_upload_artifact, normalize_csv_s3_storage_format
from .validation import validate_csv_file
from ...s3_hidden import reject_hidden_s3_location


logger = logging.getLogger(__name__)
ProgressCallback = Callable[[dict[str, Any]], None]


class CsvUpload(Protocol):
    filename: str | None
    file: BinaryIO


@dataclass(frozen=True, slots=True)
class CsvLocalSource:
    file_name: str
    local_path: Path


def normalize_csv_identifier(value: str, *, default_prefix: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        normalized = default_prefix
    if normalized[0].isdigit():
        normalized = f"{default_prefix}_{normalized}"
    return normalized


def normalize_csv_table_name(file_name: str, *, prefix: str = "") -> str:
    base_name = Path(str(file_name or "").strip()).stem
    normalized_base = normalize_csv_identifier(base_name, default_prefix="csv_import")
    normalized_prefix = normalize_csv_identifier(prefix, default_prefix="csv").strip("_") if prefix else ""
    return f"{normalized_prefix}_{normalized_base}" if normalized_prefix else normalized_base


def normalize_csv_columns(columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
    normalized: list[tuple[str, str]] = []
    seen: dict[str, int] = {}

    for name, type_name in columns:
        base_name = normalize_csv_identifier(name, default_prefix="column")
        next_index = seen.get(base_name, 0)
        seen[base_name] = next_index + 1
        column_name = base_name if next_index == 0 else f"{base_name}_{next_index + 1}"
        normalized.append((column_name, duckdb_type_to_postgres_type(type_name)))

    return normalized


def duckdb_type_to_postgres_type(type_name: str) -> str:
    normalized_type = str(type_name or "").strip().upper()
    if not normalized_type:
        return "TEXT"

    if normalized_type in {"BOOLEAN", "BOOL"}:
        return "BOOLEAN"
    if normalized_type in {"TINYINT", "SMALLINT", "SHORT"}:
        return "SMALLINT"
    if normalized_type in {"INTEGER", "INT", "SIGNED", "INT4"}:
        return "INTEGER"
    if normalized_type in {"BIGINT", "LONG", "INT8", "UBIGINT"}:
        return "BIGINT"
    if normalized_type in {"REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION"}:
        return "DOUBLE PRECISION"
    if normalized_type.startswith("DECIMAL") or normalized_type.startswith("NUMERIC"):
        return normalized_type
    if normalized_type in {"DATE"}:
        return "DATE"
    if normalized_type.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if normalized_type in {"TIME"}:
        return "TIME"
    return "TEXT"


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _emit_progress(
    callback: ProgressCallback | None,
    *,
    phase: str,
    message: str,
    detail: str = "",
    **diagnostics: object,
) -> None:
    if callback is None:
        return
    callback(
        {
            "phase": phase,
            "message": message,
            "detail": detail,
            "diagnostics": {
                key: value
                for key, value in diagnostics.items()
                if str(key).strip() and value is not None
            },
        }
    )


def inspect_csv_file(
    local_path: Path,
    *,
    delimiter: str = "",
    has_header: bool = True,
) -> tuple[list[tuple[str, str]], int]:
    connection = duckdb.connect(":memory:")
    try:
        options = [f"HEADER = {'TRUE' if has_header else 'FALSE'}"]
        normalized_delimiter = normalize_csv_delimiter(delimiter)
        if normalized_delimiter:
            options.append(f"DELIM = {sql_literal(normalized_delimiter)}")
        source_sql = (
            f"read_csv_auto({sql_literal(local_path.as_posix())}, {', '.join(options)})"
        )
        rows = connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
        columns = [
            (str(row[0] or "").strip() or "column", str(row[1] or "").strip() or "VARCHAR")
            for row in rows
        ]
        row_count = int(connection.execute(f"SELECT COUNT(*) FROM {source_sql}").fetchone()[0] or 0)
        return columns, row_count
    finally:
        connection.close()


class CsvIngestionManager:
    def __init__(
        self,
        *,
        settings: Settings,
        postgres_connection_factory,
        s3_client_factory=s3_client,
    ) -> None:
        self._settings = settings
        self._postgres_connection_factory = postgres_connection_factory
        self._s3_client_factory = s3_client_factory
        self._archive_policy = CsvArchivePolicy(
            max_archive_bytes=settings.ingestion_upload_max_archive_bytes,
            max_csv_bytes=settings.ingestion_upload_max_csv_bytes,
            max_extracted_bytes=settings.ingestion_upload_max_extracted_bytes,
            max_entries=settings.ingestion_zip_max_entries,
            max_expansion_ratio=settings.ingestion_zip_max_expansion_ratio,
            copy_chunk_bytes=settings.ingestion_copy_chunk_bytes,
        )

    def import_csv_files(
        self,
        *,
        files: list[CsvUpload],
        target_id: str,
        bucket: str = "",
        prefix: str = "",
        schema_name: str = "public",
        table_prefix: str = "",
        delimiter: str = "",
        has_header: bool = True,
        replace_existing: bool = True,
        storage_format: str = "csv",
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        with TemporaryDirectory() as temp_dir:
            sources, failures = self._materialize_uploads(
                files=list(files or []),
                temp_dir=Path(temp_dir),
            )
            return self.import_csv_sources(
                sources=sources,
                target_id=target_id,
                bucket=bucket,
                prefix=prefix,
                schema_name=schema_name,
                table_prefix=table_prefix,
                delimiter=delimiter,
                has_header=has_header,
                replace_existing=replace_existing,
                storage_format=storage_format,
                initial_imports=failures,
                progress_callback=progress_callback,
            )

    def import_csv_sources(
        self,
        *,
        sources: list[CsvLocalSource],
        target_id: str,
        bucket: str = "",
        prefix: str = "",
        schema_name: str = "public",
        table_prefix: str = "",
        delimiter: str = "",
        has_header: bool = True,
        replace_existing: bool = True,
        storage_format: str = "csv",
        initial_imports: list[dict[str, Any]] | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        normalized_target_id = str(target_id or "").strip()
        if normalized_target_id not in {"workspace.s3", "pg_oltp", "pg_olap"}:
            raise ValueError(f"Unsupported CSV ingestion target: {target_id}")
        if not sources and not initial_imports:
            raise ValueError("Choose at least one CSV or ZIP file before importing.")

        imports: list[dict[str, Any]] = list(initial_imports or [])
        with TemporaryDirectory() as extract_temp_dir:
            csv_sources: list[CsvLocalSource] = []
            extraction_root = Path(extract_temp_dir)
            for source_index, source in enumerate(sources):
                file_name = Path(str(source.file_name or "")).name.strip()
                if not file_name:
                    imports.append(
                        {
                            "fileName": "unnamed.csv",
                            "status": "failed",
                            "error": "The uploaded file is missing its file name.",
                        }
                    )
                    continue
                suffix = file_name.lower()
                try:
                    if suffix.endswith(".csv"):
                        csv_sources.append(CsvLocalSource(file_name=file_name, local_path=source.local_path))
                    elif suffix.endswith(".zip"):
                        extracted = extract_csv_archive(
                            archive_path=source.local_path,
                            output_dir=extraction_root / normalize_csv_identifier(
                                f"{Path(file_name).stem}_{source_index + 1}",
                                default_prefix="archive",
                            ),
                            policy=self._archive_policy,
                        )
                        csv_sources.extend(
                            CsvLocalSource(file_name=item.file_name, local_path=item.local_path)
                            for item in extracted
                        )
                    else:
                        imports.append(
                            {
                                "fileName": file_name,
                                "status": "failed",
                                "error": "Only .csv and .zip files are supported in this ingestion flow.",
                            }
                        )
                except Exception as exc:
                    imports.append(
                        {
                            "fileName": file_name,
                            "status": "failed",
                            "error": str(exc),
                        }
                    )

            for source in csv_sources:
                file_name = source.file_name
                local_path = source.local_path
                try:
                    logger.info(
                        "CSV ingestion file start: file=%r target=%s size_bytes=%s storage_format=%s",
                        file_name,
                        normalized_target_id,
                        _file_size(local_path),
                        storage_format,
                    )
                    _emit_progress(
                        progress_callback,
                        phase="csv_validate",
                        message=f"Validating {file_name}.",
                        detail="Step 2 of 2: checking CSV structure before import.",
                        fileName=file_name,
                        targetId=normalized_target_id,
                        sizeBytes=_file_size(local_path),
                    )
                    resolved_delimiter = validate_csv_file(
                        local_path,
                        delimiter=delimiter,
                        has_header=has_header,
                    )
                    if normalized_target_id == "workspace.s3":
                        result = self._import_csv_to_s3(
                            local_path=local_path,
                            file_name=file_name,
                            bucket=bucket,
                            prefix=prefix,
                            delimiter=resolved_delimiter,
                            has_header=has_header,
                            storage_format=storage_format,
                            progress_callback=progress_callback,
                        )
                    else:
                        result = self._import_csv_to_postgres(
                            local_path=local_path,
                            file_name=file_name,
                            target_id=normalized_target_id,
                            schema_name=schema_name,
                            table_prefix=table_prefix,
                            delimiter=resolved_delimiter,
                            has_header=has_header,
                            replace_existing=replace_existing,
                        )
                except Exception as exc:
                    logger.exception(
                        "CSV ingestion file failed: file=%r target=%s storage_format=%s size_bytes=%s",
                        file_name,
                        normalized_target_id,
                        storage_format,
                        _file_size(local_path),
                    )
                    _emit_progress(
                        progress_callback,
                        phase="csv_failed",
                        message=f"Failed to import {file_name}.",
                        detail=str(exc),
                        fileName=file_name,
                        targetId=normalized_target_id,
                        storageFormat=storage_format,
                    )
                    imports.append(
                        {
                            "fileName": file_name,
                            "status": "failed",
                            "error": str(exc),
                        }
                    )
                    continue

                imports.append(
                    {
                        "fileName": file_name,
                        "status": "imported",
                        **result,
                    }
                )

        imported_count = sum(1 for item in imports if item.get("status") == "imported")
        return {
            "targetId": normalized_target_id,
            "importedCount": imported_count,
            "failedCount": len(imports) - imported_count,
            "imports": imports,
        }

    def _materialize_uploads(
        self,
        *,
        files: list[CsvUpload],
        temp_dir: Path,
    ) -> tuple[list[CsvLocalSource], list[dict[str, Any]]]:
        if not files:
            return [], []

        upload_dir = temp_dir / "uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        sources: list[CsvLocalSource] = []
        failures: list[dict[str, Any]] = []
        seen_names: dict[str, int] = {}
        for upload in files:
            file_name = Path(str(getattr(upload, "filename", "") or "")).name.strip()
            if not file_name:
                failures.append(
                    {
                        "fileName": "unnamed.csv",
                        "status": "failed",
                        "error": "The uploaded file is missing its file name.",
                    }
                )
                continue
            if not file_name.lower().endswith((".csv", ".zip")):
                failures.append(
                    {
                        "fileName": file_name,
                        "status": "failed",
                        "error": "Only .csv and .zip files are supported in this ingestion flow.",
                    }
                )
                continue

            local_path = self._persist_upload(
                upload,
                upload_dir,
                file_name=self._unique_materialized_name(file_name, seen_names),
            )
            sources.append(CsvLocalSource(file_name=file_name, local_path=local_path))
        return sources, failures

    def _unique_materialized_name(self, file_name: str, seen: dict[str, int]) -> str:
        normalized_key = file_name.lower()
        next_index = seen.get(normalized_key, 0)
        seen[normalized_key] = next_index + 1
        if next_index == 0:
            return file_name
        path = Path(file_name)
        return f"{path.stem}_{next_index + 1}{path.suffix}"

    def _persist_upload(
        self,
        upload: CsvUpload,
        temp_dir: Path,
        *,
        file_name: str | None = None,
    ) -> Path:
        file_name = (
            Path(str(file_name or getattr(upload, "filename", "") or "upload.csv")).name
            or "upload.csv"
        )
        target_path = temp_dir / file_name
        input_file = getattr(upload, "file", None)
        if input_file is None:
            raise ValueError(f"The uploaded file '{file_name}' could not be read.")

        if hasattr(input_file, "seek"):
            input_file.seek(0)
        with target_path.open("wb") as output_file:
            shutil.copyfileobj(input_file, output_file)
        if hasattr(input_file, "seek"):
            input_file.seek(0)
        return target_path

    def _import_csv_to_s3(
        self,
        *,
        local_path: Path,
        file_name: str,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        has_header: bool = True,
        storage_format: str = "csv",
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        normalized_bucket = str(bucket or "").strip() or str(self._settings.s3_bucket or "").strip()
        if not normalized_bucket:
            raise ValueError("Provide a bucket or configure S3_BUCKET before importing CSV files.")

        normalized_storage_format = normalize_csv_s3_storage_format(storage_format)
        source_size = _file_size(local_path)
        logger.info(
            "CSV S3 import prepare: file=%r source_size_bytes=%s bucket=%r prefix=%r storage_format=%s endpoint=%r use_ssl=%s verify_ssl=%s url_style=%r",
            file_name,
            source_size,
            normalized_bucket,
            prefix,
            normalized_storage_format,
            self._settings.s3_endpoint,
            self._settings.s3_use_ssl,
            self._settings.s3_verify_ssl,
            self._settings.s3_url_style,
        )
        _emit_progress(
            progress_callback,
            phase="s3_prepare",
            message=f"Preparing {file_name} for S3.",
            detail=f"Step 2 of 2: resolving target object and {normalized_storage_format.upper()} storage format.",
            fileName=file_name,
            bucket=normalized_bucket,
            prefix=prefix,
            storageFormat=normalized_storage_format,
            sourceSizeBytes=source_size,
        )
        normalized_prefix = "/".join(
            segment for segment in str(prefix or "").split("/") if str(segment).strip()
        )
        convert_started = time.perf_counter()
        try:
            if normalized_storage_format == "csv":
                _emit_progress(
                    progress_callback,
                    phase="s3_preserve_csv",
                    message=f"Using uploaded CSV bytes for {file_name}.",
                    detail="Step 2 of 2: no Parquet conversion requested; the CSV object will be uploaded as-is.",
                    fileName=file_name,
                    bucket=normalized_bucket,
                    storageFormat=normalized_storage_format,
                )
            else:
                _emit_progress(
                    progress_callback,
                    phase="s3_convert_start",
                    message=f"Converting {file_name} to {normalized_storage_format.upper()}.",
                    detail="Step 2 of 2: DuckDB is transforming the staged CSV before S3 upload.",
                    fileName=file_name,
                    storageFormat=normalized_storage_format,
                    sourceSizeBytes=source_size,
                )
            upload_artifact = build_csv_s3_upload_artifact(
                local_path=local_path,
                file_name=file_name,
                storage_format=normalized_storage_format,
                delimiter=delimiter,
                has_header=has_header,
            )
        except Exception as exc:
            logger.exception(
                "CSV S3 import transform failed: file=%r storage_format=%s source_size_bytes=%s",
                file_name,
                normalized_storage_format,
                source_size,
            )
            raise ValueError(
                f"Failed to prepare '{file_name}' as {normalized_storage_format.upper()} before S3 upload: {exc}"
            ) from exc

        artifact_size = _file_size(upload_artifact.local_path)
        if normalized_storage_format != "csv":
            _emit_progress(
                progress_callback,
                phase="s3_convert_done",
                message=f"Converted {file_name} to {upload_artifact.file_name}.",
                detail=(
                    "Step 2 of 2: conversion completed; preparing the S3 write."
                ),
                fileName=file_name,
                storedFileName=upload_artifact.file_name,
                storageFormat=upload_artifact.storage_format,
                sourceSizeBytes=source_size,
                artifactSizeBytes=artifact_size,
                elapsedMs=round((time.perf_counter() - convert_started) * 1000),
            )
        key = (
            f"{normalized_prefix}/{upload_artifact.file_name}"
            if normalized_prefix
            else upload_artifact.file_name
        )
        reject_hidden_s3_location(
            normalized_bucket,
            key,
            self._settings,
            data_exchange_prefix=self._settings.data_exchange_prefix,
        )
        _emit_progress(
            progress_callback,
            phase="s3_bucket_check",
            message=f"Checking S3 bucket {normalized_bucket}.",
            detail="Step 2 of 2: verifying that the target bucket is accessible.",
            bucket=normalized_bucket,
            key=key,
        )
        try:
            bucket_created = ensure_s3_bucket(self._settings, normalized_bucket)
        except Exception as exc:
            logger.exception(
                "CSV S3 import bucket access failed: file=%r bucket=%r key=%r endpoint=%r",
                file_name,
                normalized_bucket,
                key,
                self._settings.s3_endpoint,
            )
            raise ValueError(
                f"Failed to access or create S3 bucket '{normalized_bucket}' before uploading '{key}': {exc}"
            ) from exc
        _emit_progress(
            progress_callback,
            phase="s3_bucket_ready",
            message=f"S3 bucket {normalized_bucket} is ready.",
            detail="Step 2 of 2: bucket access succeeded; creating the S3 client for upload.",
            bucket=normalized_bucket,
            key=key,
            bucketCreated=bool(bucket_created),
        )
        client = self._s3_client_factory(self._settings)
        _emit_progress(
            progress_callback,
            phase="s3_upload_start",
            message=f"Uploading {upload_artifact.file_name} to S3.",
            detail="Step 2 of 2: invoking the S3 PutObject/multipart upload.",
            fileName=file_name,
            storedFileName=upload_artifact.file_name,
            bucket=normalized_bucket,
            key=key,
            artifactSizeBytes=artifact_size,
        )
        upload_started = time.perf_counter()
        try:
            upload_s3_file(
                client,
                local_path=upload_artifact.local_path,
                bucket=normalized_bucket,
                key=key,
                metadata=upload_artifact.metadata,
                transfer_config=TransferConfig(
                    multipart_threshold=64 * 1024 * 1024,
                    multipart_chunksize=64 * 1024 * 1024,
                    max_concurrency=4,
                ),
            )
        except Exception as exc:
            logger.exception(
                "CSV S3 import upload failed: file=%r stored_file=%r bucket=%r key=%r artifact_size_bytes=%s endpoint=%r",
                file_name,
                upload_artifact.file_name,
                normalized_bucket,
                key,
                artifact_size,
                self._settings.s3_endpoint,
            )
            raise ValueError(
                f"S3 upload failed for '{upload_artifact.file_name}' to s3://{normalized_bucket}/{key}: {exc}"
            ) from exc

        verification: dict[str, object] = {}
        try:
            response = client.head_object(Bucket=normalized_bucket, Key=key)
            metadata = response.get("ResponseMetadata") or {}
            verification = {
                "contentLength": response.get("ContentLength"),
                "etag": str(response.get("ETag") or "").strip('"'),
                "httpStatusCode": metadata.get("HTTPStatusCode"),
                "requestId": metadata.get("RequestId"),
            }
            logger.info(
                "CSV S3 import upload confirmed: file=%r bucket=%r key=%r size_bytes=%s verification=%s elapsed_ms=%s",
                file_name,
                normalized_bucket,
                key,
                artifact_size,
                verification,
                round((time.perf_counter() - upload_started) * 1000),
            )
            _emit_progress(
                progress_callback,
                phase="s3_upload_done",
                message=f"Uploaded {upload_artifact.file_name} to S3.",
                detail="Step 2 of 2: S3 upload returned success and the object metadata was readable.",
                fileName=file_name,
                storedFileName=upload_artifact.file_name,
                bucket=normalized_bucket,
                key=key,
                artifactSizeBytes=artifact_size,
                elapsedMs=round((time.perf_counter() - upload_started) * 1000),
                verification=verification,
            )
        except Exception as exc:
            logger.warning(
                "CSV S3 import upload returned success but HeadObject verification failed: file=%r bucket=%r key=%r detail=%s",
                file_name,
                normalized_bucket,
                key,
                exc,
                exc_info=True,
            )
            _emit_progress(
                progress_callback,
                phase="s3_upload_done",
                message=f"Uploaded {upload_artifact.file_name} to S3.",
                detail=(
                    "Step 2 of 2: upload returned success, but object metadata verification failed. "
                    f"Technical detail: {exc}"
                ),
                fileName=file_name,
                storedFileName=upload_artifact.file_name,
                bucket=normalized_bucket,
                key=key,
                artifactSizeBytes=artifact_size,
                elapsedMs=round((time.perf_counter() - upload_started) * 1000),
            )
        return {
            "destination": "s3",
            "bucket": normalized_bucket,
            "objectKey": key,
            "objectKeyPrefix": normalized_prefix,
            "storedFileName": upload_artifact.file_name,
            "path": f"s3://{normalized_bucket}/{key}",
            "storageFormat": upload_artifact.storage_format,
            "uploadedBytes": artifact_size,
            "s3Verification": verification,
        }

    def _import_csv_to_postgres(
        self,
        *,
        local_path: Path,
        file_name: str,
        target_id: str,
        schema_name: str,
        table_prefix: str,
        delimiter: str,
        has_header: bool,
        replace_existing: bool,
    ) -> dict[str, Any]:
        normalized_schema = normalize_csv_identifier(schema_name or "public", default_prefix="public")
        table_name = normalize_csv_table_name(file_name, prefix=table_prefix)
        normalized_delimiter = normalize_csv_delimiter(delimiter)
        columns, row_count = inspect_csv_file(
            local_path,
            delimiter=normalized_delimiter,
            has_header=has_header,
        )
        normalized_columns = normalize_csv_columns(columns)
        if not normalized_columns:
            raise ValueError("The CSV file does not expose any columns that can be imported.")

        target = "oltp" if target_id == "pg_oltp" else "olap"
        connection = self._postgres_connection_factory(target)
        cursor = connection.cursor()
        qualified_table = f"{sql_identifier(normalized_schema)}.{sql_identifier(table_name)}"
        column_definition_sql = ", ".join(
            f"{sql_identifier(column_name)} {column_type}"
            for column_name, column_type in normalized_columns
        )
        copy_columns_sql = ", ".join(sql_identifier(column_name) for column_name, _ in normalized_columns)
        copy_options = [
            "FORMAT CSV",
            f"HEADER {'TRUE' if has_header else 'FALSE'}",
        ]
        if normalized_delimiter:
            copy_options.append(f"DELIMITER {sql_literal(normalized_delimiter)}")

        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_identifier(normalized_schema)}")
            if replace_existing:
                cursor.execute(f"DROP TABLE IF EXISTS {qualified_table}")
            cursor.execute(f"CREATE TABLE {qualified_table} ({column_definition_sql})")
            with cursor.copy(
                f"COPY {qualified_table} ({copy_columns_sql}) FROM STDIN WITH ({', '.join(copy_options)})"
            ) as copy:
                with local_path.open("rb") as input_file:
                    while True:
                        chunk = input_file.read(self._settings.ingestion_copy_chunk_bytes)
                        if not chunk:
                            break
                        copy.write(chunk)
            cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}")
            imported_row_count = int(cursor.fetchone()[0] or 0)
        finally:
            if hasattr(cursor, "close"):
                cursor.close()
            if hasattr(connection, "close"):
                connection.close()

        return {
            "destination": target_id,
            "relation": f"{normalized_schema}.{table_name}",
            "rowCount": imported_row_count if imported_row_count >= 0 else row_count,
        }

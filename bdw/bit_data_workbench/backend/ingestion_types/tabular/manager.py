from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO, Protocol

import duckdb
from boto3.s3.transfer import TransferConfig

from ....config import Settings
from ...queryable_files import materialize_queryable_file
from ...s3_storage import ensure_s3_bucket, s3_client, upload_s3_file
from ...sql_utils import sql_identifier, sql_literal
from ...s3_hidden import reject_hidden_s3_location
from ..parquet_optimization import (
    ParquetOptimizationSettings,
    copy_query_to_optimized_parquet,
    manual_parquet_layout_requested,
    normalize_parquet_optimization_settings,
    parquet_art_cache_warning,
)
from ..common import (
    ArchivePolicy,
    IngestionLocalSource,
    IngestionUploadFileRequest,
    IngestionUploadSessionManager,
    extract_archive_files_with_failures,
)
from ..csv.manager import (
    duckdb_type_to_postgres_type,
    normalize_csv_columns,
    normalize_csv_identifier,
    normalize_csv_table_name,
)


class FileUpload(Protocol):
    filename: str | None
    file: BinaryIO


def _path_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    if path.is_dir():
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
    return 0


@dataclass(frozen=True, slots=True)
class FileIngestorSpec:
    ingestor_id: str
    display_name: str
    format_label: str
    allowed_extensions: tuple[str, ...]
    source_format: str
    direct_size_limit_setting: str
    mime_type: str
    default_table_prefix: str
    conversion_size_limited: bool = False


FILE_INGESTOR_SPECS: dict[str, FileIngestorSpec] = {
    "parquet": FileIngestorSpec(
        ingestor_id="parquet",
        display_name="Parquet Files",
        format_label="Parquet",
        allowed_extensions=(".parquet",),
        source_format="parquet",
        direct_size_limit_setting="ingestion_upload_max_csv_bytes",
        mime_type="application/vnd.apache.parquet",
        default_table_prefix="parquet",
    ),
    "json": FileIngestorSpec(
        ingestor_id="json",
        display_name="JSON Files",
        format_label="JSON",
        allowed_extensions=(".json", ".jsonl", ".ndjson"),
        source_format="json",
        direct_size_limit_setting="ingestion_upload_max_csv_bytes",
        mime_type="application/json",
        default_table_prefix="json",
    ),
    "xlsx": FileIngestorSpec(
        ingestor_id="xlsx",
        display_name="Excel Files",
        format_label="Excel",
        allowed_extensions=(".xlsx",),
        source_format="xlsx",
        direct_size_limit_setting="ingestion_tabular_conversion_max_bytes",
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        default_table_prefix="xlsx",
        conversion_size_limited=True,
    ),
    "xml": FileIngestorSpec(
        ingestor_id="xml",
        display_name="XML Files",
        format_label="XML",
        allowed_extensions=(".xml",),
        source_format="xml",
        direct_size_limit_setting="ingestion_tabular_conversion_max_bytes",
        mime_type="application/xml",
        default_table_prefix="xml",
        conversion_size_limited=True,
    ),
}


FileUploadFileRequest = IngestionUploadFileRequest


class FileUploadSessionManager(IngestionUploadSessionManager):
    def __init__(self, *, settings: Settings, spec: FileIngestorSpec) -> None:
        allowed = ", ".join(spec.allowed_extensions)
        super().__init__(
            settings=settings,
            allowed_extensions=spec.allowed_extensions,
            format_label=spec.format_label,
            empty_files_message=(
                f"Choose at least one {spec.format_label} or ZIP file before importing."
            ),
            invalid_extension_message=(
                f"Only {allowed} and .zip files are supported in this ingestion flow."
            ),
            direct_file_size_limit=lambda app_settings: int(
                getattr(app_settings, spec.direct_size_limit_setting)
            ),
            source_factory=lambda file_name, local_path: IngestionLocalSource(
                file_name=file_name,
                local_path=Path(local_path),
            ),
        )


class FileIngestionManager:
    def __init__(
        self,
        *,
        settings: Settings,
        spec: FileIngestorSpec,
        postgres_connection_factory,
        s3_client_factory=s3_client,
    ) -> None:
        self._settings = settings
        self._spec = spec
        self._postgres_connection_factory = postgres_connection_factory
        self._s3_client_factory = s3_client_factory
        self._archive_policy = ArchivePolicy(
            max_archive_bytes=settings.ingestion_upload_max_archive_bytes,
            max_entry_bytes=self._entry_size_limit(),
            max_extracted_bytes=(
                settings.ingestion_upload_max_extracted_bytes
                if not spec.conversion_size_limited
                else min(
                    settings.ingestion_upload_max_extracted_bytes,
                    settings.ingestion_tabular_conversion_max_bytes * settings.ingestion_zip_max_entries,
                )
            ),
            max_entries=settings.ingestion_zip_max_entries,
            max_expansion_ratio=settings.ingestion_zip_max_expansion_ratio,
            copy_chunk_bytes=settings.ingestion_copy_chunk_bytes,
        )

    def import_files(
        self,
        *,
        files: list[FileUpload],
        target_id: str,
        bucket: str = "",
        prefix: str = "",
        schema_name: str = "public",
        table_prefix: str = "",
        replace_existing: bool = True,
        parquet_optimization: Any = None,
    ) -> dict[str, Any]:
        with TemporaryDirectory() as temp_dir:
            sources, failures = self._materialize_uploads(
                files=list(files or []),
                temp_dir=Path(temp_dir),
            )
            return self.import_sources(
                sources=sources,
                target_id=target_id,
                bucket=bucket,
                prefix=prefix,
                schema_name=schema_name,
                table_prefix=table_prefix,
                replace_existing=replace_existing,
                parquet_optimization=parquet_optimization,
                initial_imports=failures,
            )

    def import_sources(
        self,
        *,
        sources: list[IngestionLocalSource],
        target_id: str,
        bucket: str = "",
        prefix: str = "",
        schema_name: str = "public",
        table_prefix: str = "",
        replace_existing: bool = True,
        parquet_optimization: Any = None,
        initial_imports: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        normalized_target_id = str(target_id or "").strip()
        if normalized_target_id not in {"s3", "pg_oltp", "pg_olap"}:
            raise ValueError(f"Unsupported {self._spec.format_label} ingestion target: {target_id}")
        if not sources and not initial_imports:
            raise ValueError(
                f"Choose at least one {self._spec.format_label} or ZIP file before importing."
            )
        optimization = normalize_parquet_optimization_settings(
            parquet_optimization,
            target_id=normalized_target_id,
            storage_format=self._spec.source_format,
        )

        imports: list[dict[str, Any]] = list(initial_imports or [])
        with TemporaryDirectory() as extract_temp_dir:
            file_sources: list[IngestionLocalSource] = []
            extraction_root = Path(extract_temp_dir)
            for source_index, source in enumerate(sources):
                file_name = Path(str(source.file_name or "")).name.strip()
                if not file_name:
                    imports.append(
                        {
                            "fileName": f"unnamed.{self._spec.source_format}",
                            "status": "failed",
                            "error": "The uploaded file is missing its file name.",
                        }
                    )
                    continue
                try:
                    if file_name.lower().endswith(self._spec.allowed_extensions):
                        self._validate_conversion_size(source.local_path, file_name)
                        file_sources.append(
                            IngestionLocalSource(file_name=file_name, local_path=source.local_path)
                        )
                    elif file_name.lower().endswith(".zip"):
                        extracted, member_failures = extract_archive_files_with_failures(
                            archive_path=source.local_path,
                            output_dir=extraction_root
                            / normalize_csv_identifier(
                                f"{Path(file_name).stem}_{source_index + 1}",
                                default_prefix="archive",
                            ),
                            policy=self._archive_policy,
                            allowed_extensions=self._spec.allowed_extensions,
                            format_label=self._spec.format_label,
                        )
                        imports.extend(
                            {
                                "fileName": failure.file_name,
                                "status": "failed",
                                "error": failure.error,
                            }
                            for failure in member_failures
                        )
                        for item in extracted:
                            self._validate_conversion_size(item.local_path, item.file_name)
                            file_sources.append(
                                IngestionLocalSource(
                                    file_name=item.file_name,
                                    local_path=item.local_path,
                                )
                            )
                    else:
                        imports.append(
                            {
                                "fileName": file_name,
                                "status": "failed",
                                "error": self._unsupported_file_message(),
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

            for source in file_sources:
                file_name = source.file_name
                local_path = source.local_path
                try:
                    if normalized_target_id == "s3":
                        result = self._import_to_s3(
                            local_path=local_path,
                            file_name=file_name,
                            bucket=bucket,
                            prefix=prefix,
                            parquet_optimization=optimization,
                        )
                    else:
                        result = self._import_to_postgres(
                            local_path=local_path,
                            file_name=file_name,
                            target_id=normalized_target_id,
                            schema_name=schema_name,
                            table_prefix=table_prefix,
                            replace_existing=replace_existing,
                        )
                except Exception as exc:
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
            "parquetOptimization": optimization.payload,
            "imports": imports,
        }

    def _materialize_uploads(
        self,
        *,
        files: list[FileUpload],
        temp_dir: Path,
    ) -> tuple[list[IngestionLocalSource], list[dict[str, Any]]]:
        if not files:
            return [], []

        upload_dir = temp_dir / "uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        sources: list[IngestionLocalSource] = []
        failures: list[dict[str, Any]] = []
        seen_names: dict[str, int] = {}
        for upload in files:
            file_name = Path(str(getattr(upload, "filename", "") or "")).name.strip()
            if not file_name:
                failures.append(
                    {
                        "fileName": f"unnamed.{self._spec.source_format}",
                        "status": "failed",
                        "error": "The uploaded file is missing its file name.",
                    }
                )
                continue
            if not self._is_supported_upload_name(file_name):
                failures.append(
                    {
                        "fileName": file_name,
                        "status": "failed",
                        "error": self._unsupported_file_message(),
                    }
                )
                continue

            local_path = self._persist_upload(
                upload,
                upload_dir,
                file_name=self._unique_materialized_name(file_name, seen_names),
            )
            sources.append(IngestionLocalSource(file_name=file_name, local_path=local_path))
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
        upload: FileUpload,
        temp_dir: Path,
        *,
        file_name: str | None = None,
    ) -> Path:
        file_name = (
            Path(str(file_name or getattr(upload, "filename", "") or f"upload.{self._spec.source_format}")).name
            or f"upload.{self._spec.source_format}"
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

    def _import_to_s3(
        self,
        *,
        local_path: Path,
        file_name: str,
        bucket: str,
        prefix: str = "",
        parquet_optimization: ParquetOptimizationSettings | None = None,
    ) -> dict[str, Any]:
        normalized_bucket = str(bucket or "").strip() or str(self._settings.s3_bucket or "").strip()
        if not normalized_bucket:
            raise ValueError(
                f"Provide a bucket or configure S3_BUCKET before importing {self._spec.format_label} files."
            )

        normalized_prefix = "/".join(
            segment for segment in str(prefix or "").split("/") if str(segment).strip()
        )
        optimization = parquet_optimization or ParquetOptimizationSettings()
        if self._spec.source_format == "parquet" and manual_parquet_layout_requested(optimization):
            with TemporaryDirectory() as temp_dir:
                partitioned = bool(optimization.partition_columns)
                stored_file_name = Path(file_name).stem if partitioned else file_name
                prepared_path = Path(temp_dir) / stored_file_name
                duckdb_connection = duckdb.connect(":memory:")
                try:
                    copy_query_to_optimized_parquet(
                        connection=duckdb_connection,
                        source_query_sql=(
                            f"SELECT * FROM read_parquet({sql_literal(local_path.as_posix())})"
                        ),
                        target_path=prepared_path,
                        optimization=optimization,
                    )
                finally:
                    duckdb_connection.close()
                return self._upload_prepared_s3_artifact(
                    local_path=prepared_path,
                    file_name=stored_file_name,
                    bucket=normalized_bucket,
                    prefix=normalized_prefix,
                    metadata={"bdw-ingestion-format": self._spec.source_format},
                    partitioned=partitioned,
                    optimization=optimization,
                )

        return self._upload_prepared_s3_artifact(
            local_path=local_path,
            file_name=file_name,
            bucket=normalized_bucket,
            prefix=normalized_prefix,
            metadata={"bdw-ingestion-format": self._spec.source_format},
            partitioned=False,
            optimization=optimization,
        )

    def _upload_prepared_s3_artifact(
        self,
        *,
        local_path: Path,
        file_name: str,
        bucket: str,
        prefix: str,
        metadata: dict[str, str],
        partitioned: bool,
        optimization: ParquetOptimizationSettings,
    ) -> dict[str, Any]:
        key = f"{prefix}/{file_name}" if prefix else file_name
        reject_hidden_s3_location(
            bucket,
            key,
            self._settings,
            data_exchange_prefix=self._settings.data_exchange_prefix,
        )
        ensure_s3_bucket(self._settings, bucket)
        client = self._s3_client_factory(self._settings)
        uploaded_keys = self._upload_s3_artifact(
            client,
            local_path=local_path,
            bucket=bucket,
            key=key,
            metadata=metadata,
            partitioned=partitioned,
        )
        object_path = f"s3://{bucket}/{key}/**/*.parquet" if partitioned else f"s3://{bucket}/{key}"
        result: dict[str, Any] = {
            "destination": "s3",
            "bucket": bucket,
            "objectKey": key,
            "objectKeyPrefix": prefix,
            "storedFileName": file_name,
            "path": object_path,
            "storageFormat": self._spec.source_format,
            "uploadedBytes": _path_size(local_path),
        }
        if partitioned:
            result.update(
                {
                    "partitioned": True,
                    "partCount": len(uploaded_keys),
                    "uploadedKeys": uploaded_keys,
                }
            )
        warning = parquet_art_cache_warning(optimization)
        if warning:
            result["warnings"] = [warning]
        if self._spec.source_format == "parquet" or not optimization.is_default:
            result["parquetOptimization"] = optimization.payload
        return result

    def _upload_s3_artifact(
        self,
        client,
        *,
        local_path: Path,
        bucket: str,
        key: str,
        metadata: dict[str, str],
        partitioned: bool,
    ) -> list[str]:
        transfer_config = TransferConfig(
            multipart_threshold=64 * 1024 * 1024,
            multipart_chunksize=64 * 1024 * 1024,
            max_concurrency=4,
        )
        if not partitioned:
            upload_s3_file(
                client,
                local_path=local_path,
                bucket=bucket,
                key=key,
                metadata=metadata,
                transfer_config=transfer_config,
            )
            return [key]

        uploaded_keys: list[str] = []
        for part_path in sorted(path for path in local_path.rglob("*.parquet") if path.is_file()):
            relative_key = part_path.relative_to(local_path).as_posix()
            part_key = f"{key.rstrip('/')}/{relative_key}"
            reject_hidden_s3_location(
                bucket,
                part_key,
                self._settings,
                data_exchange_prefix=self._settings.data_exchange_prefix,
            )
            upload_s3_file(
                client,
                local_path=part_path,
                bucket=bucket,
                key=part_key,
                metadata=metadata,
                transfer_config=transfer_config,
            )
            uploaded_keys.append(part_key)
        if not uploaded_keys:
            raise ValueError("DuckDB did not produce any Parquet part files for the selected partition columns.")
        return uploaded_keys

    def _import_to_postgres(
        self,
        *,
        local_path: Path,
        file_name: str,
        target_id: str,
        schema_name: str,
        table_prefix: str,
        replace_existing: bool,
    ) -> dict[str, Any]:
        normalized_schema = normalize_csv_identifier(schema_name or "public", default_prefix="public")
        default_prefix = table_prefix or self._spec.default_table_prefix
        table_name = normalize_csv_table_name(file_name, prefix=default_prefix)

        target = "oltp" if target_id == "pg_oltp" else "olap"
        connection = self._postgres_connection_factory(target)
        cursor = connection.cursor()
        qualified_table = f"{sql_identifier(normalized_schema)}.{sql_identifier(table_name)}"

        with TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            duckdb_csv = temp_root / "copy.csv"
            duckdb_connection = duckdb.connect(":memory:")
            try:
                source_sql = self._duckdb_source_sql(
                    local_path=local_path,
                    file_name=file_name,
                    temp_root=temp_root,
                )
                rows = duckdb_connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
                columns = [
                    (str(row[0] or "").strip() or "column", str(row[1] or "").strip() or "VARCHAR")
                    for row in rows
                ]
                normalized_columns = normalize_csv_columns(columns)
                if not normalized_columns:
                    raise ValueError(
                        f"The {self._spec.format_label} file does not expose any columns that can be imported."
                    )
                row_count = int(
                    duckdb_connection.execute(f"SELECT COUNT(*) FROM {source_sql}").fetchone()[0] or 0
                )
                duckdb_connection.execute(
                    f"COPY (SELECT * FROM {source_sql}) TO {sql_literal(duckdb_csv.as_posix())} "
                    "(FORMAT CSV, HEADER TRUE)"
                )
            finally:
                duckdb_connection.close()

            column_definition_sql = ", ".join(
                f"{sql_identifier(column_name)} {column_type}"
                for column_name, column_type in normalized_columns
            )
            copy_columns_sql = ", ".join(sql_identifier(column_name) for column_name, _ in normalized_columns)
            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_identifier(normalized_schema)}")
                if replace_existing:
                    cursor.execute(f"DROP TABLE IF EXISTS {qualified_table}")
                cursor.execute(f"CREATE TABLE {qualified_table} ({column_definition_sql})")
                with cursor.copy(
                    f"COPY {qualified_table} ({copy_columns_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                ) as copy:
                    with duckdb_csv.open("rb") as input_file:
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

    def _duckdb_source_sql(self, *, local_path: Path, file_name: str, temp_root: Path) -> str:
        source_path = local_path.as_posix()
        if self._spec.source_format == "parquet":
            return f"read_parquet({sql_literal(source_path)})"
        if self._spec.source_format == "json":
            return f"read_json_auto({sql_literal(source_path)})"

        self._validate_conversion_size(local_path, file_name)
        file_bytes = local_path.read_bytes()
        materialized = materialize_queryable_file(
            root=temp_root / "materialized",
            base_name=normalize_csv_identifier(Path(file_name).stem, default_prefix=self._spec.source_format),
            file_name=file_name,
            source_format=self._spec.source_format,
            file_bytes=file_bytes,
        )
        return f"read_csv_auto({sql_literal(materialized.local_path.as_posix())}, HEADER = TRUE)"

    def _is_supported_upload_name(self, file_name: str) -> bool:
        lower_name = file_name.lower()
        return lower_name.endswith(self._spec.allowed_extensions) or lower_name.endswith(".zip")

    def _unsupported_file_message(self) -> str:
        allowed = ", ".join(self._spec.allowed_extensions)
        return f"Only {allowed} and .zip files are supported in this ingestion flow."

    def _entry_size_limit(self) -> int:
        return int(getattr(self._settings, self._spec.direct_size_limit_setting))

    def _validate_conversion_size(self, local_path: Path, file_name: str) -> None:
        if not self._spec.conversion_size_limited:
            return
        size_bytes = local_path.stat().st_size
        if size_bytes > self._settings.ingestion_tabular_conversion_max_bytes:
            raise ValueError(
                f"The file '{file_name}' exceeds the configured {self._spec.format_label} conversion size limit."
            )

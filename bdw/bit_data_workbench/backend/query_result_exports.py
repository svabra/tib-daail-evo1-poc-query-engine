from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import re
import shutil
import tempfile
from typing import Any, Callable

import duckdb

from ..config import Settings
from ..models import QueryJobDefinition, QueryResultExportDefinition
from .data_exporters import (
    EXPORT_FORMATS,
    FORMAT_CONTENT_TYPES,
    export_s3_metadata,
    normalize_export_format,
    normalize_export_settings,
    write_export,
)
from .s3_explorer import normalize_s3_bucket_name, normalize_s3_prefix, s3_path
from .s3_storage import ensure_s3_bucket, s3_client, upload_s3_file


FETCH_BATCH_SIZE = 1000
POSTGRES_SOURCE_TARGETS = {
    "pg_oltp": "oltp",
    "pg_olap": "olap",
    "pg_oltp_native": "oltp",
}
ATTACHED_POSTGRES_CATALOG_PATTERN = re.compile(
    r'(?i)(?<![\w])(?:"(?P<quoted>pg_oltp|pg_olap)"|(?P<plain>pg_oltp|pg_olap))\.'
)
def sanitize_filename_stem(value: str) -> str:
    normalized = re.sub(r"[^\w.-]+", "-", str(value or "").strip())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-.")
    return normalized or "query-result"


def build_default_filename(job: QueryJobDefinition, export_format: str) -> str:
    base_name = sanitize_filename_stem(f"{job.notebook_title}-{job.cell_id}")
    return f"{base_name}.{normalize_export_format(export_format)}"


def normalize_export_filename(file_name: str | None, export_format: str, *, fallback: str) -> str:
    normalized_format = normalize_export_format(export_format)
    candidate = str(file_name or "").strip()
    if not candidate:
        candidate = fallback
    candidate = PurePosixPath(candidate.replace("\\", "/")).name.strip()
    if not candidate:
        raise ValueError("Provide a file name.")
    if "/" in candidate or "\\" in candidate:
        raise ValueError("File names cannot include folder separators.")
    if not candidate.lower().endswith(f".{normalized_format}"):
        stem = re.sub(r"\.[^.]+$", "", candidate).strip() or fallback
        candidate = f"{stem}.{normalized_format}"
    return candidate


def strip_attached_postgres_catalog(sql: str, alias: str) -> str:
    normalized_alias = str(alias or "").strip().lower()
    if not normalized_alias:
        return sql

    def replace(match: re.Match[str]) -> str:
        matched_alias = (match.group("quoted") or match.group("plain") or "").strip().lower()
        return "" if matched_alias == normalized_alias else match.group(0)

    return ATTACHED_POSTGRES_CATALOG_PATTERN.sub(replace, sql)


@dataclass(slots=True)
class QueryResultExportArtifact:
    export: QueryResultExportDefinition
    local_path: Path
    cleanup_dir: Path


class QueryResultExportManager:
    def __init__(
        self,
        *,
        settings: Settings,
        connection_factory: Callable[[], duckdb.DuckDBPyConnection],
        postgres_connection_factory: Callable[[str], Any],
        query_job_resolver: Callable[[str], QueryJobDefinition],
    ) -> None:
        self._settings = settings
        self._connection_factory = connection_factory
        self._postgres_connection_factory = postgres_connection_factory
        self._query_job_resolver = query_job_resolver

    def download(
        self,
        *,
        job_id: str,
        export_format: str,
        export_settings: dict[str, Any] | None = None,
    ) -> QueryResultExportArtifact:
        job = self._exportable_job(job_id)
        normalized_format = normalize_export_format(export_format)
        export_options = normalize_export_settings(normalized_format, export_settings)
        filename = build_default_filename(job, normalized_format)
        temp_dir = Path(tempfile.mkdtemp(prefix="bdw-query-export-"))
        local_path = temp_dir / filename
        self._write_export(
            job=job,
            export_format=normalized_format,
            local_path=local_path,
            export_settings=export_options,
        )
        export = QueryResultExportDefinition(
            job_id=job.job_id,
            export_format=normalized_format,
            destination="download",
            filename=filename,
            content_type=FORMAT_CONTENT_TYPES[normalized_format],
            message=f"Prepared {filename} for download.",
        )
        return QueryResultExportArtifact(export=export, local_path=local_path, cleanup_dir=temp_dir)

    def save_to_s3(
        self,
        *,
        job_id: str,
        export_format: str,
        bucket: str,
        prefix: str = "",
        file_name: str = "",
        export_settings: dict[str, Any] | None = None,
    ) -> QueryResultExportDefinition:
        job = self._exportable_job(job_id)
        normalized_format = normalize_export_format(export_format)
        export_options = normalize_export_settings(normalized_format, export_settings)
        normalized_bucket = normalize_s3_bucket_name(bucket)
        normalized_prefix = normalize_s3_prefix(prefix)
        default_filename = build_default_filename(job, normalized_format)
        filename = normalize_export_filename(file_name, normalized_format, fallback=default_filename)
        key = f"{normalized_prefix}{filename}"
        artifact = self.download(
            job_id=job.job_id,
            export_format=normalized_format,
            export_settings=export_settings,
        )
        try:
            ensure_s3_bucket(self._settings, normalized_bucket)
            upload_s3_file(
                s3_client(self._settings),
                local_path=artifact.local_path,
                bucket=normalized_bucket,
                key=key,
                metadata=export_s3_metadata(normalized_format, export_options),
            )
        finally:
            shutil.rmtree(artifact.cleanup_dir, ignore_errors=True)

        return QueryResultExportDefinition(
            job_id=job.job_id,
            export_format=normalized_format,
            destination="s3",
            filename=filename,
            content_type=FORMAT_CONTENT_TYPES[normalized_format],
            bucket=normalized_bucket,
            key=key,
            path=f"{s3_path(normalized_bucket, normalized_prefix)}{filename}",
            message=f"Saved {filename} to s3://{normalized_bucket}/{key}.",
        )

    def _exportable_job(self, job_id: str) -> QueryJobDefinition:
        job = self._query_job_resolver(job_id)
        if job.status != "completed":
            raise ValueError("Only completed query jobs can be exported.")
        if not job.columns:
            raise ValueError("This query job has no tabular result to export.")
        return job

    def _write_export(
        self,
        *,
        job: QueryJobDefinition,
        export_format: str,
        local_path: Path,
        export_settings,
    ) -> None:
        write_export(
            export_format=export_format,
            local_path=local_path,
            stream_rows=lambda consume: self._stream_query_rows(job, consume),
            options=export_settings,
        )

    def _has_cached_result_snapshot(self, job: QueryJobDefinition) -> bool:
        if job.status != "completed" or not job.columns:
            return False
        return len(job.rows) == job.rows_shown

    def _stream_cached_rows(
        self,
        job: QueryJobDefinition,
        consume: Callable[[list[str], list[tuple[Any, ...]]], None],
    ) -> None:
        columns = list(job.columns)
        if not columns:
            raise ValueError("This query job did not return tabular rows.")

        rows = [tuple(row) for row in job.rows]
        for index in range(0, len(rows), FETCH_BATCH_SIZE):
            consume(columns, rows[index : index + FETCH_BATCH_SIZE])

    def _stream_query_rows(
        self,
        job: QueryJobDefinition,
        consume: Callable[[list[str], list[tuple[Any, ...]]], None],
    ) -> None:
        if self._has_cached_result_snapshot(job):
            self._stream_cached_rows(job, consume)
            return

        native_export = self._native_postgres_export(job)
        use_postgres_native = native_export is not None
        execution_sql = native_export[1] if native_export is not None else job.sql
        connection = (
            self._postgres_connection_factory(native_export[0])
            if native_export is not None
            else self._connection_factory()
        )
        try:
            if use_postgres_native:
                with connection.cursor() as cursor:
                    cursor.execute(execution_sql)
                    columns = [column.name for column in (cursor.description or [])]
                    if not columns:
                        raise ValueError("This query job did not return tabular rows.")
                    while True:
                        batch = cursor.fetchmany(FETCH_BATCH_SIZE)
                        if not batch:
                            break
                        consume(columns, [tuple(item) for item in batch])
            else:
                cursor = connection.execute(execution_sql)
                columns = [column[0] for column in cursor.description] if cursor.description else []
                if not columns:
                    raise ValueError("This query job did not return tabular rows.")
                while True:
                    batch = cursor.fetchmany(FETCH_BATCH_SIZE)
                    if not batch:
                        break
                    consume(columns, [tuple(item) for item in batch])
        finally:
            try:
                connection.close()
            except Exception:
                pass

    def _native_postgres_export(self, job: QueryJobDefinition) -> tuple[str, str] | None:
        source_ids = {
            source_id.strip().lower()
            for source_id in job.data_sources
            if source_id and source_id.strip()
        }
        if not source_ids:
            return None

        if "pg_oltp_native" in source_ids:
            return "oltp", job.sql

        postgres_sources = {
            source_id: POSTGRES_SOURCE_TARGETS[source_id]
            for source_id in source_ids
            if source_id in {"pg_oltp", "pg_olap"}
        }
        if len(postgres_sources) != 1 or source_ids != set(postgres_sources):
            return None

        alias, target = next(iter(postgres_sources.items()))
        return target, strip_attached_postgres_catalog(job.sql, alias)

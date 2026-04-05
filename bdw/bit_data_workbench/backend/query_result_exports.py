from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import tempfile
from typing import Any, Callable

import duckdb
from fastapi.encoders import jsonable_encoder

from ..config import Settings
from ..models import QueryJobDefinition, QueryResultExportDefinition
from .s3_explorer import normalize_s3_bucket_name, normalize_s3_prefix, s3_path
from .s3_storage import ensure_s3_bucket, s3_client, upload_s3_file


FETCH_BATCH_SIZE = 1000
EXPORT_FORMATS = {"csv", "json", "parquet"}
FORMAT_CONTENT_TYPES = {
    "csv": "text/csv; charset=utf-8",
    "json": "application/json",
    "parquet": "application/vnd.apache.parquet",
}
POSTGRES_SOURCE_TARGETS = {
    "pg_oltp": "oltp",
    "pg_olap": "olap",
    "pg_oltp_native": "oltp",
}
ATTACHED_POSTGRES_CATALOG_PATTERN = re.compile(
    r'(?i)(?<![\w])(?:"(?P<quoted>pg_oltp|pg_olap)"|(?P<plain>pg_oltp|pg_olap))\.'
)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def normalize_export_format(export_format: str) -> str:
    normalized = str(export_format or "").strip().lower()
    if normalized not in EXPORT_FORMATS:
        raise ValueError(f"Unsupported export format: {export_format}")
    return normalized


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
        candidate = f"{candidate}.{normalized_format}"
    return candidate


def export_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


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

    def download(self, *, job_id: str, export_format: str) -> QueryResultExportArtifact:
        job = self._exportable_job(job_id)
        normalized_format = normalize_export_format(export_format)
        filename = build_default_filename(job, normalized_format)
        temp_dir = Path(tempfile.mkdtemp(prefix="bdw-query-export-"))
        local_path = temp_dir / filename
        self._write_export(job=job, export_format=normalized_format, local_path=local_path)
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
    ) -> QueryResultExportDefinition:
        job = self._exportable_job(job_id)
        normalized_format = normalize_export_format(export_format)
        normalized_bucket = normalize_s3_bucket_name(bucket)
        normalized_prefix = normalize_s3_prefix(prefix)
        default_filename = build_default_filename(job, normalized_format)
        filename = normalize_export_filename(file_name, normalized_format, fallback=default_filename)
        key = f"{normalized_prefix}{filename}"
        artifact = self.download(job_id=job.job_id, export_format=normalized_format)
        try:
            ensure_s3_bucket(self._settings, normalized_bucket)
            upload_s3_file(
                s3_client(self._settings),
                local_path=artifact.local_path,
                bucket=normalized_bucket,
                key=key,
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

    def _write_export(self, *, job: QueryJobDefinition, export_format: str, local_path: Path) -> None:
        normalized_format = normalize_export_format(export_format)
        if normalized_format == "json":
            self._write_json(job, local_path)
            return
        if normalized_format == "csv":
            self._write_csv(job, local_path)
            return
        self._write_parquet(job, local_path)

    def _write_json(self, job: QueryJobDefinition, local_path: Path) -> None:
        with local_path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("[\n")
            first_row = True

            def consume(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
                nonlocal first_row
                for row in rows:
                    payload = jsonable_encoder(
                        {
                            column: row[index] if index < len(row) else None
                            for index, column in enumerate(columns)
                        }
                    )
                    if not first_row:
                        handle.write(",\n")
                    handle.write(json.dumps(payload, ensure_ascii=False))
                    first_row = False

            self._stream_query_rows(job, consume)
            handle.write("\n]")

    def _write_csv(self, job: QueryJobDefinition, local_path: Path) -> None:
        with local_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            header_written = False

            def consume(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
                nonlocal header_written
                if not header_written:
                    writer.writerow(columns)
                    header_written = True
                for row in rows:
                    writer.writerow([export_cell_value(value) for value in row])

            self._stream_query_rows(job, consume)
            if not header_written:
                writer.writerow(list(job.columns))

    def _write_parquet(self, job: QueryJobDefinition, local_path: Path) -> None:
        temp_csv_path = local_path.with_suffix(".tmp.csv")
        self._write_csv(job, temp_csv_path)
        connection = duckdb.connect(database=":memory:")
        try:
            connection.execute(
                "COPY ("
                f"SELECT * FROM read_csv_auto({sql_literal(temp_csv_path.as_posix())}, HEADER = TRUE)"
                f") TO {sql_literal(local_path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            connection.close()
            temp_csv_path.unlink(missing_ok=True)

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

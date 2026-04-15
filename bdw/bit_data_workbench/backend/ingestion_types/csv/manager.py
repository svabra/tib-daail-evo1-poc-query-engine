from __future__ import annotations

import re
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO, Protocol

import duckdb

from ....config import Settings
from ...s3_storage import ensure_s3_bucket, s3_client, upload_s3_file
from ...sql_utils import sql_identifier, sql_literal
from .dialect import normalize_csv_delimiter
from .s3_formats import build_csv_s3_upload_artifact, normalize_csv_s3_storage_format
from .validation import validate_csv_file


class CsvUpload(Protocol):
    filename: str | None
    file: BinaryIO


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
    ) -> dict[str, Any]:
        normalized_target_id = str(target_id or "").strip()
        if normalized_target_id not in {"workspace.s3", "pg_oltp", "pg_olap"}:
            raise ValueError(f"Unsupported CSV ingestion target: {target_id}")
        if not files:
            raise ValueError("Choose at least one CSV file before importing.")

        imports: list[dict[str, Any]] = []
        for upload in files:
            file_name = Path(str(getattr(upload, "filename", "") or "")).name.strip()
            if not file_name:
                imports.append(
                    {
                        "fileName": "unnamed.csv",
                        "status": "failed",
                        "error": "The uploaded file is missing its file name.",
                    }
                )
                continue
            if not file_name.lower().endswith(".csv"):
                imports.append(
                    {
                        "fileName": file_name,
                        "status": "failed",
                        "error": "Only .csv files are supported in this ingestion flow.",
                    }
                )
                continue

            try:
                with TemporaryDirectory() as temp_dir:
                    local_path = self._persist_upload(upload, Path(temp_dir))
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

    def _persist_upload(self, upload: CsvUpload, temp_dir: Path) -> Path:
        file_name = Path(str(getattr(upload, "filename", "") or "upload.csv")).name or "upload.csv"
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
    ) -> dict[str, Any]:
        normalized_bucket = str(bucket or "").strip() or str(self._settings.s3_bucket or "").strip()
        if not normalized_bucket:
            raise ValueError("Provide a bucket or configure S3_BUCKET before importing CSV files.")

        upload_artifact = build_csv_s3_upload_artifact(
            local_path=local_path,
            file_name=file_name,
            storage_format=normalize_csv_s3_storage_format(storage_format),
            delimiter=delimiter,
            has_header=has_header,
        )
        normalized_prefix = "/".join(
            segment for segment in str(prefix or "").split("/") if str(segment).strip()
        )
        key = (
            f"{normalized_prefix}/{upload_artifact.file_name}"
            if normalized_prefix
            else upload_artifact.file_name
        )
        ensure_s3_bucket(self._settings, normalized_bucket)
        client = self._s3_client_factory(self._settings)
        upload_s3_file(
            client,
            local_path=upload_artifact.local_path,
            bucket=normalized_bucket,
            key=key,
            metadata=upload_artifact.metadata,
        )
        return {
            "destination": "s3",
            "bucket": normalized_bucket,
            "objectKey": key,
            "objectKeyPrefix": normalized_prefix,
            "storedFileName": upload_artifact.file_name,
            "path": f"s3://{normalized_bucket}/{key}",
            "storageFormat": upload_artifact.storage_format,
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
                        chunk = input_file.read(1024 * 1024)
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

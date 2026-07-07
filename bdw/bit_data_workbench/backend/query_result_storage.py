from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

from .query_options import result_storage_enabled, result_storage_options
from .source_references import s3_source_reference
from .sql_utils import sql_literal


@dataclass(frozen=True, slots=True)
class QueryResultStorageTarget:
    bucket: str
    key: str
    s3_path: str
    virtual_path: str
    duckdb_path: str
    duckdb_reference: str
    format: str = "parquet"

    def payload(self, *, status: str = "planned", message: str = "") -> dict[str, Any]:
        return {
            "enabled": True,
            "status": status,
            "format": self.format,
            "path": self.s3_path,
            "bucket": self.bucket,
            "key": self.key,
            "virtualPath": self.virtual_path,
            "duckdbPath": self.duckdb_path,
            "duckdbReference": self.duckdb_reference,
            "message": message,
        }


def _strip_terminal_semicolon(sql: str) -> str:
    text = str(sql or "").strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    return text


def _normalize_s3_result_path(path: object) -> tuple[str, str, str]:
    raw_path = str(path or "").strip()
    if not raw_path:
        raise ValueError("Provide an S3 path for the result set.")
    parsed = urlparse(raw_path)
    if parsed.scheme.lower() != "s3":
        raise ValueError("Result set storage path must start with s3://.")
    bucket = unquote(str(parsed.netloc or "").strip())
    key = unquote(str(parsed.path or "").lstrip("/").strip())
    if not bucket:
        raise ValueError("Result set storage path must include an S3 bucket.")
    if not key:
        raise ValueError("Result set storage path must include an object key.")
    if key.endswith("/"):
        raise ValueError("Result set storage path must point to a Parquet file, not a folder.")
    if any(char in key for char in "*?[]"):
        raise ValueError("Result set storage path must not contain wildcard characters.")
    if not key.lower().endswith(".parquet"):
        raise ValueError("Result set storage writes Parquet; the path must end with .parquet.")
    return bucket, key, f"s3://{bucket}/{key}"


def normalize_result_storage_target(query_options: Any) -> QueryResultStorageTarget | None:
    if not result_storage_enabled(query_options):
        return None
    options = result_storage_options(query_options)
    bucket, key, s3_path = _normalize_s3_result_path(options.get("path"))
    duckdb_reference = f"read_parquet({sql_literal(s3_path)})"
    return QueryResultStorageTarget(
        bucket=bucket,
        key=key,
        s3_path=s3_path,
        virtual_path=s3_source_reference(bucket=bucket, key=key),
        duckdb_path=s3_path,
        duckdb_reference=duckdb_reference,
    )


def planned_result_storage_payload(query_options: Any) -> dict[str, Any]:
    target = normalize_result_storage_target(query_options)
    if target is None:
        return {}
    return target.payload(status="planned", message="Result set will be stored in S3.")


def result_storage_copy_sql(sql: str, target: QueryResultStorageTarget) -> str:
    execution_sql = _strip_terminal_semicolon(sql)
    if not execution_sql:
        raise ValueError("Provide a SQL statement before storing the result set.")
    return f"COPY (\n{execution_sql}\n) TO {sql_literal(target.s3_path)} (FORMAT PARQUET)"


def result_storage_preview_sql(target: QueryResultStorageTarget) -> str:
    return f"SELECT * FROM {target.duckdb_reference}"


def validate_result_storage_request(query_options: Any, *, execution_mode: str) -> None:
    target = normalize_result_storage_target(query_options)
    if target is None:
        return
    if execution_mode != "duckdb-read":
        raise ValueError(
            "Store result set in S3 is available only for DuckDB read queries."
        )

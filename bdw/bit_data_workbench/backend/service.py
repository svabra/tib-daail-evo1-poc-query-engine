from __future__ import annotations

import re
import time
from threading import RLock
from urllib.parse import urlparse

import duckdb

from ..config import Settings
from .query_jobs import QueryJobManager
from ..models import (
    NotebookDefinition,
    QueryResult,
    SourceCatalog,
    SourceField,
    SourceObject,
    SourceSchema,
)
from .notebooks import build_completion_schema, build_notebook_tree, build_notebooks, build_source_options


SUPPORTED_S3_VIEW_FORMATS = {"parquet", "csv", "json"}


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def qualified_name(*parts: str) -> str:
    return ".".join(sql_identifier(part) for part in parts)


def normalize_port(value: str, variable_name: str) -> str:
    if not value.isdigit():
        raise ValueError(f"{variable_name} must be numeric, got: {value}")
    return value


def normalize_s3_endpoint(raw_value: str, use_ssl: bool) -> tuple[str, bool]:
    if "://" in raw_value:
        parsed = urlparse(raw_value)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError(f"Unsupported S3 endpoint scheme: {parsed.scheme}")
        if not parsed.netloc:
            raise ValueError(f"Invalid S3 endpoint: {raw_value}")
        return parsed.netloc, parsed.scheme == "https"
    return raw_value, use_ssl


def infer_s3_view_format(path: str) -> str:
    lowered = path.lower()
    if ".parquet" in lowered:
        return "parquet"
    if ".csv" in lowered or ".tsv" in lowered:
        return "csv"
    if ".json" in lowered or ".jsonl" in lowered or ".ndjson" in lowered:
        return "json"
    raise ValueError(
        "Could not infer the S3 startup view format from the configured path. "
        "Use 'view_name=format:s3://bucket/path'."
    )


def parse_s3_startup_views(raw_value: str | None) -> list[tuple[str, str, str]]:
    if not raw_value:
        return []

    entries: list[tuple[str, str, str]] = []
    for item in re.split(r"[;\r\n]+", raw_value):
        entry = item.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(
                "Each S3 startup view entry must use 'view_name=s3://bucket/path' "
                "or 'view_name=format:s3://bucket/path'."
            )

        view_name, spec = entry.split("=", 1)
        view_name = view_name.strip()
        spec = spec.strip()
        if not view_name or not spec:
            raise ValueError(f"Invalid S3 startup view entry: {entry}")

        prefix = spec.split(":", 1)[0].lower()
        if prefix in SUPPORTED_S3_VIEW_FORMATS and ":" in spec:
            data_format, path = spec.split(":", 1)
            entries.append((view_name, data_format.strip().lower(), path.strip()))
        else:
            entries.append((view_name, infer_s3_view_format(spec), spec))

    return entries


class WorkbenchService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = RLock()
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._catalogs: list[SourceCatalog] = []
        self._notebooks: list[NotebookDefinition] = []
        self._completion_schema: dict[str, object] = {}
        self._source_options: list[dict[str, str]] = []
        self._query_jobs = QueryJobManager(
            max_result_rows=settings.max_result_rows,
            connection_factory=self._create_connection,
            notebook_title_resolver=self._resolve_notebook_title,
            metadata_refresher=self.refresh_metadata_state,
        )

    def start(self) -> None:
        self.settings.duckdb_database.parent.mkdir(parents=True, exist_ok=True)
        self.settings.duckdb_extension_directory.mkdir(parents=True, exist_ok=True)
        conn = self._create_connection()

        with self._lock:
            self._conn = conn
            self._refresh_state()

    def stop(self) -> None:
        with self._lock:
            if self._conn is None:
                return
            self._conn.close()
            self._conn = None

    def runtime_info(self) -> dict[str, str]:
        return self.settings.runtime_info()

    def notebooks(self) -> list[NotebookDefinition]:
        with self._lock:
            return list(self._notebooks)

    def notebook(self, notebook_id: str) -> NotebookDefinition:
        with self._lock:
            for notebook in self._notebooks:
                if notebook.notebook_id == notebook_id:
                    return notebook
        raise KeyError(f"Unknown notebook: {notebook_id}")

    def catalogs(self) -> list[SourceCatalog]:
        with self._lock:
            return list(self._catalogs)

    def completion_schema(self) -> dict[str, object]:
        with self._lock:
            return dict(self._completion_schema)

    def source_options(self) -> list[dict[str, str]]:
        with self._lock:
            return [dict(option) for option in self._source_options]

    def notebook_tree(self):
        with self._lock:
            return build_notebook_tree(self._notebooks)

    def query_jobs_state(self) -> dict[str, object]:
        return self._query_jobs.state_payload()

    def wait_for_query_jobs_state(
        self,
        last_version: int | None,
        timeout: float = 15.0,
    ) -> dict[str, object]:
        return self._query_jobs.wait_for_state(last_version, timeout=timeout)

    def start_query_job(
        self,
        *,
        sql: str,
        notebook_id: str,
        notebook_title: str,
        cell_id: str,
        data_sources: list[str] | None = None,
    ) -> dict[str, object]:
        snapshot = self._query_jobs.start_job(
            sql=sql,
            notebook_id=notebook_id,
            notebook_title=notebook_title,
            cell_id=cell_id,
            data_sources=data_sources,
        )
        return snapshot.payload

    def cancel_query_job(self, job_id: str) -> dict[str, object]:
        snapshot = self._query_jobs.cancel_job(job_id)
        return snapshot.payload

    def source_object_fields(self, relation: str) -> list[SourceField]:
        normalized_relation = relation.strip()
        if not normalized_relation:
            raise KeyError("Missing source object relation.")

        parts = [part.strip() for part in normalized_relation.split(".") if part.strip()]
        if len(parts) == 3 and parts[0] in {"pg_oltp", "pg_olap"}:
            catalog_name = parts[0]
            schema_name = parts[1]
            object_name = parts[2]
            query = """
                SELECT
                    column_name,
                    COALESCE(NULLIF(UPPER(data_type), ''), NULLIF(UPPER(udt_name), ''), 'UNKNOWN')
                FROM information_schema.columns
                WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """
            parameters = [catalog_name, schema_name, object_name]
        elif len(parts) == 2:
            schema_name = parts[0]
            object_name = parts[1]
            query = """
                SELECT
                    column_name,
                    COALESCE(NULLIF(UPPER(data_type), ''), NULLIF(UPPER(udt_name), ''), 'UNKNOWN')
                FROM information_schema.columns
                WHERE table_catalog NOT IN ('pg_oltp', 'pg_olap')
                  AND table_schema = ?
                  AND table_name = ?
                ORDER BY ordinal_position
            """
            parameters = [schema_name, object_name]
        else:
            raise KeyError(f"Unsupported source object relation: {relation}")

        with self._lock:
            conn = self._require_connection()
            rows = conn.execute(query, parameters).fetchall()

        if not rows:
            raise KeyError(f"Unknown source object: {relation}")

        return [SourceField(name=column_name, data_type=data_type) for column_name, data_type in rows]

    def execute_query(self, sql: str) -> QueryResult:
        query = sql.strip()
        if not query:
            return QueryResult(sql=sql, error="Provide a SQL statement before running the query.")

        started = time.perf_counter()
        try:
            with self._lock:
                conn = self._require_connection()
                cursor = conn.execute(query)
                columns = [column[0] for column in cursor.description] if cursor.description else []
                rows: list[tuple[object, ...]] = []
                truncated = False
                row_count = 0
                message = "Statement executed successfully."

                if columns:
                    batch = cursor.fetchmany(self.settings.max_result_rows + 1)
                    truncated = len(batch) > self.settings.max_result_rows
                    rows = [tuple(item) for item in batch[: self.settings.max_result_rows]]
                    row_count = len(rows)
                    message = f"{row_count} row(s) shown."
                    if truncated:
                        message = (
                            f"{self.settings.max_result_rows} row(s) shown. "
                            "The result was truncated for the UI."
                        )

                self._refresh_state()

            return QueryResult(
                sql=sql,
                columns=columns,
                rows=rows,
                row_count=row_count,
                truncated=truncated,
                message=message,
                duration_ms=(time.perf_counter() - started) * 1000,
            )
        except Exception as exc:
            with self._lock:
                if self._conn is not None:
                    self._refresh_state()
            return QueryResult(
                sql=sql,
                error=str(exc),
                duration_ms=(time.perf_counter() - started) * 1000,
            )

    def refresh_metadata_state(self) -> None:
        with self._lock:
            if self._conn is None:
                return
            self._refresh_state()

    def _require_connection(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("The workbench service has not been initialized.")
        return self._conn

    def _resolve_notebook_title(self, notebook_id: str) -> str | None:
        with self._lock:
            for notebook in self._notebooks:
                if notebook.notebook_id == notebook_id:
                    return notebook.title
        return None

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        self.settings.duckdb_database.parent.mkdir(parents=True, exist_ok=True)
        self.settings.duckdb_extension_directory.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(str(self.settings.duckdb_database))
        conn.execute(
            f"SET extension_directory = {sql_literal(self.settings.duckdb_extension_directory.as_posix())}"
        )

        self._ensure_extension(conn, "httpfs")
        self._ensure_extension(conn, "postgres")
        self._bootstrap_integrations(conn)
        return conn

    def _ensure_extension(self, conn: duckdb.DuckDBPyConnection, extension: str) -> None:
        try:
            conn.execute(f"LOAD {extension}")
        except duckdb.Error:
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")

    def _bootstrap_integrations(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._bootstrap_s3(conn)
        self._bootstrap_postgres(conn)

    def _bootstrap_s3(self, conn: duckdb.DuckDBPyConnection) -> None:
        required_values = (
            self.settings.s3_endpoint,
            self.settings.s3_region,
            self.settings.s3_bucket,
            self.settings.s3_access_key_id,
            self.settings.s3_secret_access_key,
        )

        if not any(required_values):
            return

        missing = [
            name
            for name, value in (
                ("S3_ENDPOINT", self.settings.s3_endpoint),
                ("S3_REGION", self.settings.s3_region),
                ("S3_BUCKET", self.settings.s3_bucket),
                ("S3_ACCESS_KEY_ID", self.settings.s3_access_key_id),
                ("S3_SECRET_ACCESS_KEY", self.settings.s3_secret_access_key),
            )
            if value is None
        ]
        if missing:
            raise ValueError(f"Missing required S3 bootstrap variables: {', '.join(missing)}")

        endpoint, use_ssl = normalize_s3_endpoint(
            self.settings.s3_endpoint,
            self.settings.s3_use_ssl,
        )

        options = [
            "TYPE s3",
            "PROVIDER config",
            f"KEY_ID {sql_literal(self.settings.s3_access_key_id)}",
            f"SECRET {sql_literal(self.settings.s3_secret_access_key)}",
            f"REGION {sql_literal(self.settings.s3_region)}",
            f"ENDPOINT {sql_literal(endpoint)}",
            f"USE_SSL {'true' if use_ssl else 'false'}",
            f"SCOPE {sql_literal(f's3://{self.settings.s3_bucket}')}",
        ]
        if self.settings.s3_url_style:
            options.append(f"URL_STYLE {sql_literal(self.settings.s3_url_style)}")
        if self.settings.s3_session_token:
            options.append(f"SESSION_TOKEN {sql_literal(self.settings.s3_session_token)}")

        conn.execute(f"CREATE OR REPLACE SECRET bdw_s3 ({', '.join(options)})")
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_identifier(self.settings.s3_startup_view_schema)}")

        for view_name, data_format, path in parse_s3_startup_views(self.settings.s3_startup_views):
            if data_format == "parquet":
                query = f"SELECT * FROM read_parquet({sql_literal(path)})"
            elif data_format == "csv":
                query = f"SELECT * FROM read_csv_auto({sql_literal(path)})"
            elif data_format == "json":
                query = f"SELECT * FROM read_json_auto({sql_literal(path)})"
            else:
                raise ValueError(f"Unsupported S3 startup view format: {data_format}")

            conn.execute(
                "CREATE OR REPLACE VIEW "
                f"{qualified_name(self.settings.s3_startup_view_schema, view_name)} "
                f"AS {query}"
            )

    def _bootstrap_postgres(self, conn: duckdb.DuckDBPyConnection) -> None:
        if not all(
            (
                self.settings.pg_host,
                self.settings.pg_port,
                self.settings.pg_user,
                self.settings.pg_password,
            )
        ):
            return

        port = normalize_port(self.settings.pg_port, "PG_PORT")

        if self.settings.pg_oltp_database:
            self._create_postgres_secret(
                conn,
                secret_name="bdw_pg_oltp",
                database=self.settings.pg_oltp_database,
                port=port,
            )
            self._attach_postgres(
                conn,
                alias="pg_oltp",
                secret_name="bdw_pg_oltp",
                read_only=True,
            )

        if self.settings.pg_olap_database:
            self._create_postgres_secret(
                conn,
                secret_name="bdw_pg_olap",
                database=self.settings.pg_olap_database,
                port=port,
            )
            self._attach_postgres(
                conn,
                alias="pg_olap",
                secret_name="bdw_pg_olap",
                read_only=False,
            )

    def _create_postgres_secret(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        secret_name: str,
        database: str,
        port: str,
    ) -> None:
        conn.execute(
            "CREATE OR REPLACE SECRET "
            f"{sql_identifier(secret_name)} "
            "("
            "TYPE postgres, "
            f"HOST {sql_literal(self.settings.pg_host)}, "
            f"PORT {port}, "
            f"DATABASE {sql_literal(database)}, "
            f"USER {sql_literal(self.settings.pg_user)}, "
            f"PASSWORD {sql_literal(self.settings.pg_password)}"
            ")"
        )

    def _attach_postgres(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        alias: str,
        secret_name: str,
        read_only: bool,
    ) -> None:
        options = [
            "TYPE postgres",
            f"SECRET {sql_identifier(secret_name)}",
        ]
        if read_only:
            options.append("READ_ONLY")

        conn.execute(f"ATTACH OR REPLACE '' AS {sql_identifier(alias)} ({', '.join(options)})")

    def _refresh_state(self) -> None:
        conn = self._require_connection()
        object_rows = conn.execute(
            """
            SELECT
                table_catalog,
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'main')
            ORDER BY table_catalog, table_schema, table_name
            """
        ).fetchall()

        grouped: dict[str, dict[str, list[SourceObject]]] = {
            "workspace": {},
            "pg_oltp": {},
            "pg_olap": {},
        }

        for catalog, schema, table_name, table_type in object_rows:
            catalog_name = catalog if catalog in {"pg_oltp", "pg_olap"} else "workspace"
            grouped.setdefault(catalog_name, {}).setdefault(schema, []).append(
                SourceObject(
                    name=table_name,
                    kind="view" if table_type.upper() == "VIEW" else "table",
                    relation=(
                        f"{schema}.{table_name}"
                        if catalog_name == "workspace"
                        else f"{catalog_name}.{schema}.{table_name}"
                    ),
                )
            )

        catalogs: list[SourceCatalog] = []
        for catalog_name in ("workspace", "pg_oltp", "pg_olap"):
            schemas = [
                SourceSchema(name=schema_name, objects=objects)
                for schema_name, objects in grouped.get(catalog_name, {}).items()
            ]
            if schemas:
                catalogs.append(SourceCatalog(name=catalog_name, schemas=schemas))

        self._catalogs = catalogs
        self._notebooks = build_notebooks(catalogs)
        self._completion_schema = build_completion_schema(catalogs)
        self._source_options = build_source_options(catalogs)

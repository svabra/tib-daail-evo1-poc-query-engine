from __future__ import annotations

import logging
import re
import time
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import RLock, Thread

import duckdb

from ..config import Settings
from ..data_generator.registry import DataGeneratorRegistry
from ..models import (
    NotebookCellDefinition,
    NotebookDefinition,
    NotebookEventDefinition,
    NotebookVersionDefinition,
    QueryResult,
    SourceCatalog,
    SourceConnectionStatus,
    SourceField,
    SourceObject,
    SourceSchema,
)
from .s3_storage import (
    download_s3_file,
    effective_s3_url_style,
    list_s3_buckets,
    list_s3_buckets_from_client,
    normalize_s3_endpoint,
    s3_bucket_schema_name,
    s3_client,
    s3_verify_value,
    upload_s3_file,
)
from .data_generation_jobs import DataGenerationJobManager
from .query_jobs import QueryJobManager
from .query_result_exports import QueryResultExportManager
from .notebooks import build_completion_schema, build_notebook_tree, build_notebooks, build_source_options
from .runbooks import build_runbook_tree
from .s3_explorer import S3ExplorerManager
from .shared_notebooks import SharedNotebookStore
from .source_discovery import (
    DataSourceDiscoveryManager,
    S3DataSourceDiscoverer,
    SqlDiscoveredRelation,
    SqlSourceDiscoverer,
)


SUPPORTED_S3_VIEW_FORMATS = {"parquet", "csv", "json"}
logger = logging.getLogger(__name__)
STARTUP_DIVIDER = "-------------------------------"
S3_BOOTSTRAP_SAMPLE_KEY = "startup/vat_context_bootstrap.csv"
MAX_NOTEBOOK_EVENT_HISTORY = 40
S3_BOOTSTRAP_SAMPLE_CSV = """company_uid,company_name,canton_code,tax_period_end,transaction_id,declared_turnover_chf,net_vat_due_chf,refund_claim_chf,filing_status,category
CHE-100.000.001,Alpine Foods AG,ZH,2025-03-31,TX-10001,124000.00,9300.00,0.00,filed,standard
CHE-100.000.002,Bern Logistics GmbH,BE,2025-03-31,TX-10002,88000.00,4200.00,0.00,filed,reduced
CHE-100.000.003,Lac Retail SA,GE,2025-03-31,TX-10003,146000.00,11800.00,0.00,assessed,standard
CHE-100.000.004,Helvetic Advisory AG,ZH,2025-06-30,TX-10004,211000.00,18450.00,0.00,filed,services
CHE-100.000.005,Romandie Pharma SA,VD,2025-06-30,TX-10005,175500.00,0.00,6200.00,refund,health
CHE-100.000.006,Transit Basel AG,BS,2025-06-30,TX-10006,99000.00,5100.00,0.00,filed,transport
CHE-100.000.007,Ticino Hospitality SA,TI,2025-09-30,TX-10007,132400.00,8400.00,0.00,under_review,hospitality
CHE-100.000.008,Winterthur Components AG,ZH,2025-09-30,TX-10008,265000.00,22700.00,0.00,filed,manufacturing
CHE-100.000.009,Lausanne Digital SARL,VD,2025-09-30,TX-10009,118500.00,7600.00,0.00,filed,services
CHE-100.000.010,Lucerne Trade AG,LU,2025-12-31,TX-10010,142200.00,9600.00,0.00,filed,standard
CHE-100.000.011,St. Gallen Medical AG,SG,2025-12-31,TX-10011,156300.00,0.00,7100.00,refund,health
CHE-100.000.012,Valais Hydro SA,VS,2025-12-31,TX-10012,301400.00,25400.00,0.00,assessed,energy
"""


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
        self._condition = threading.Condition(self._lock)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._catalogs: list[SourceCatalog] = []
        self._notebooks: list[NotebookDefinition] = []
        self._shared_notebook_store = SharedNotebookStore(self._shared_notebook_store_path())
        self._notebook_events: list[NotebookEventDefinition] = []
        self._notebook_events_version = 0
        self._completion_schema: dict[str, object] = {}
        self._source_options: list[dict[str, str]] = []
        self._postgres_health_connections: dict[str, object] = {}
        self._startup_threads: list[Thread] = []
        self._query_jobs = QueryJobManager(
            max_result_rows=settings.max_result_rows,
            connection_factory=self._create_worker_connection,
            postgres_connection_factory=self._create_postgres_native_connection,
            notebook_title_resolver=self._resolve_notebook_title,
            metadata_refresher=self.refresh_metadata_state,
        )
        self._s3_explorer = S3ExplorerManager(settings)
        self._query_result_exports = QueryResultExportManager(
            settings=settings,
            connection_factory=self._create_worker_connection,
            postgres_connection_factory=self._create_postgres_native_connection,
            query_job_resolver=self._query_jobs.snapshot,
        )
        self._data_generators = DataGeneratorRegistry()
        self._data_generation_jobs = DataGenerationJobManager(
            settings=settings,
            registry=self._data_generators,
            connection_factory=self._create_worker_connection,
            metadata_refresher=self.refresh_metadata_state,
        )
        self._data_source_discovery = DataSourceDiscoveryManager(
            connection_factory=self._create_worker_connection,
            metadata_refresher=self.refresh_metadata_state,
            discoverers=[
                SqlSourceDiscoverer(
                    source_id="pg_oltp",
                    source_label="PostgreSQL OLTP",
                    snapshot_provider=lambda: self._postgres_source_snapshot(
                        target="oltp",
                        source_id="pg_oltp",
                        source_label="PostgreSQL OLTP",
                    ),
                    disconnect_handler=lambda: self._close_persistent_postgres_connection("pg_oltp"),
                ),
                SqlSourceDiscoverer(
                    source_id="pg_olap",
                    source_label="PostgreSQL OLAP",
                    snapshot_provider=lambda: self._postgres_source_snapshot(
                        target="olap",
                        source_id="pg_olap",
                        source_label="PostgreSQL OLAP",
                    ),
                    disconnect_handler=lambda: self._close_persistent_postgres_connection("pg_olap"),
                ),
                S3DataSourceDiscoverer(settings),
            ],
        )

    def start(self) -> None:
        started = time.perf_counter()
        self._log_startup_section("Initialize workbench startup")
        self._log_startup(
            "Workbench startup begin: duckdb=%s extension_dir=%s image_version=%s",
            self.settings.duckdb_database,
            self.settings.duckdb_extension_directory,
            self.settings.image_version,
        )
        self._log_startup_section("Ensure DuckDB directories exist")
        self.settings.duckdb_database.parent.mkdir(parents=True, exist_ok=True)
        self.settings.duckdb_extension_directory.mkdir(parents=True, exist_ok=True)
        self._log_startup_section("Open primary DuckDB connection")
        conn = self._create_connection(startup_context=True, run_s3_startup_diagnostics=False)

        with self._lock:
            self._conn = conn
        self._log_startup("Startup step complete: primary DuckDB connection is ready")
        self._log_startup_section("Refresh initial metadata state")
        try:
            with self._lock:
                self._refresh_state()
        except Exception as exc:
            with self._lock:
                self._set_minimal_state()
            self._log_startup(
                "Initial metadata refresh failed; continuing with minimal state: %s",
                exc,
                level=logging.WARNING,
                exc_info=True,
            )
        else:
            self._log_startup("Startup step complete: initial metadata state is ready")

        self._log_startup_section("Start data source discovery manager")
        try:
            self._data_source_discovery.start()
        except Exception as exc:
            self._log_startup(
                "Data source discovery manager failed to start; continuing without background discovery: %s",
                exc,
                level=logging.WARNING,
                exc_info=True,
            )
        else:
            self._log_startup("Startup step complete: data source discovery manager started")

        self._log_startup_section("Schedule background S3 startup diagnostics")
        self._start_background_s3_startup_diagnostics()
        self._log_startup("Workbench startup completed in %.2fs", time.perf_counter() - started)

    def stop(self) -> None:
        self._log_startup_section("Shutdown workbench service")
        self._log_startup("Workbench shutdown begin")
        self._data_source_discovery.stop()
        self._close_persistent_postgres_connections()
        for thread in list(self._startup_threads):
            if thread.is_alive():
                thread.join(timeout=0.2)
        self._startup_threads.clear()
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
        self._log_startup("Workbench shutdown complete")

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

    def notebook_events_state(self) -> dict[str, object]:
        with self._condition:
            return self._notebook_events_state_locked()

    def wait_for_notebook_events_state(
        self,
        last_version: int | None,
        timeout: float = 15.0,
    ) -> dict[str, object]:
        with self._condition:
            if last_version is None or last_version != self._notebook_events_version:
                return self._notebook_events_state_locked()

            self._condition.wait_for(lambda: self._notebook_events_version != last_version, timeout=timeout)
            return self._notebook_events_state_locked()

    def upsert_shared_notebook(
        self,
        *,
        notebook_id: str | None,
        title: str,
        summary: str,
        tags: list[str],
        tree_path: list[str],
        linked_generator_id: str,
        cells: list[dict[str, object]],
        versions: list[dict[str, object]],
        created_at: str | None = None,
        origin_client_id: str = "",
    ) -> dict[str, object]:
        normalized_notebook_id = str(notebook_id or "").strip() or f"shared-notebook-{uuid.uuid4().hex}"
        existing_notebook = next(
            (item for item in self._shared_notebook_store.list_notebooks() if item.notebook_id == normalized_notebook_id),
            None,
        )
        normalized_title = str(title).strip() or "Untitled Notebook"
        normalized_summary = str(summary).strip() or "Describe this notebook."
        provided_tree_path = tuple(
            segment for segment in (str(item).strip() for item in tree_path) if segment
        )
        normalized_tree_path = provided_tree_path or (
            existing_notebook.tree_path if existing_notebook is not None else ("Shared Notebooks",)
        )
        normalized_tags = [tag for tag in (str(item).strip() for item in tags) if tag]

        normalized_cells = [
            NotebookCellDefinition(
                cell_id=str(cell.get("cellId") or "").strip() or f"shared-cell-{uuid.uuid4().hex[:12]}",
                sql=str(cell.get("sql") or ""),
                data_sources=[
                    source_id
                    for source_id in (str(value).strip() for value in cell.get("dataSources", []) or [])
                    if source_id
                ],
            )
            for cell in cells
            if isinstance(cell, dict)
        ]
        if not normalized_cells:
            normalized_cells = [NotebookCellDefinition(cell_id=f"shared-cell-{uuid.uuid4().hex[:12]}", sql="", data_sources=[])]

        normalized_versions = [
            NotebookVersionDefinition(
                version_id=str(version.get("versionId") or "").strip() or f"shared-version-{uuid.uuid4().hex[:12]}",
                created_at=str(version.get("createdAt") or created_at or datetime.now(UTC).isoformat()),
                title=str(version.get("title") or normalized_title),
                summary=str(version.get("summary") or normalized_summary),
                tags=[
                    tag
                    for tag in (str(item).strip() for item in version.get("tags", []) or [])
                    if tag
                ],
                cells=[
                    NotebookCellDefinition(
                        cell_id=str(cell.get("cellId") or "").strip() or f"shared-cell-{uuid.uuid4().hex[:12]}",
                        sql=str(cell.get("sql") or ""),
                        data_sources=[
                            source_id
                            for source_id in (str(value).strip() for value in cell.get("dataSources", []) or [])
                            if source_id
                        ],
                    ).payload
                    for cell in version.get("cells", []) or []
                    if isinstance(cell, dict)
                ],
            )
            for version in versions
            if isinstance(version, dict)
        ]

        notebook = NotebookDefinition(
            notebook_id=normalized_notebook_id,
            title=normalized_title,
            summary=normalized_summary,
            cells=normalized_cells,
            tags=normalized_tags,
            tree_path=normalized_tree_path,
            linked_generator_id=str(linked_generator_id or "").strip(),
            can_edit=True,
            can_delete=True,
            shared=True,
            saved_versions=normalized_versions,
            created_at=str(created_at or (existing_notebook.created_at if existing_notebook is not None else datetime.now(UTC).isoformat())),
        )

        with self._condition:
            refreshed, action = self._shared_notebook_store.upsert_notebook(notebook)
            self._rebuild_notebooks_locked()
            self._append_notebook_event_locked(
                event_type=action,
                notebook=refreshed,
                origin_client_id=origin_client_id,
            )
            return {
                "action": action,
                "notebook": refreshed.payload,
            }

    def delete_shared_notebook(
        self,
        notebook_id: str,
        *,
        origin_client_id: str = "",
    ) -> dict[str, object]:
        with self._condition:
            removed = self._shared_notebook_store.delete_notebook(notebook_id)
            self._rebuild_notebooks_locked()
            self._append_notebook_event_locked(
                event_type="deleted",
                notebook=removed,
                origin_client_id=origin_client_id,
            )
            return {
                "action": "deleted",
                "notebook": removed.payload,
            }

    def query_jobs_state(self) -> dict[str, object]:
        return self._query_jobs.state_payload()

    def data_generators(self) -> list[dict[str, object]]:
        return self._data_generation_jobs.generators_payload()

    def runbook_tree(self):
        return build_runbook_tree(self.data_generators())

    def data_generation_jobs_state(self) -> dict[str, object]:
        return self._data_generation_jobs.state_payload()

    def data_source_events_state(self) -> dict[str, object]:
        return self._data_source_discovery.state_payload()

    def connect_data_source(self, source_id: str) -> dict[str, object]:
        return self._data_source_discovery.connect_source(source_id)

    def disconnect_data_source(self, source_id: str) -> dict[str, object]:
        return self._data_source_discovery.disconnect_source(source_id)

    def wait_for_query_jobs_state(
        self,
        last_version: int | None,
        timeout: float = 15.0,
    ) -> dict[str, object]:
        return self._query_jobs.wait_for_state(last_version, timeout=timeout)

    def wait_for_data_generation_jobs_state(
        self,
        last_version: int | None,
        timeout: float = 15.0,
    ) -> dict[str, object]:
        return self._data_generation_jobs.wait_for_state(last_version, timeout=timeout)

    def wait_for_data_source_events_state(
        self,
        last_version: int | None,
        timeout: float = 15.0,
    ) -> dict[str, object]:
        return self._data_source_discovery.wait_for_state(last_version, timeout=timeout)

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

    def s3_explorer_snapshot(self, *, bucket: str = "", prefix: str = "") -> dict[str, object]:
        return self._s3_explorer.snapshot(bucket=bucket, prefix=prefix).payload

    def create_s3_bucket(self, bucket_name: str) -> dict[str, object]:
        return self._s3_explorer.create_bucket(bucket_name).payload

    def create_s3_folder(self, *, bucket: str, prefix: str = "", folder_name: str) -> dict[str, object]:
        return self._s3_explorer.create_folder(bucket=bucket, prefix=prefix, folder_name=folder_name).payload

    def download_query_result_export(self, *, job_id: str, export_format: str):
        return self._query_result_exports.download(job_id=job_id, export_format=export_format)

    def save_query_result_export_to_s3(
        self,
        *,
        job_id: str,
        export_format: str,
        bucket: str,
        prefix: str = "",
        file_name: str = "",
    ) -> dict[str, object]:
        result = self._query_result_exports.save_to_s3(
            job_id=job_id,
            export_format=export_format,
            bucket=bucket,
            prefix=prefix,
            file_name=file_name,
        )
        return result.payload

    def start_data_generation_job(
        self,
        *,
        generator_id: str,
        size_gb: float,
    ) -> dict[str, object]:
        snapshot = self._data_generation_jobs.start_job(
            generator_id=generator_id,
            size_gb=size_gb,
        )
        return snapshot.payload

    def cancel_data_generation_job(self, job_id: str) -> dict[str, object]:
        snapshot = self._data_generation_jobs.cancel_job(job_id)
        return snapshot.payload

    def cleanup_data_generation_job(self, job_id: str) -> dict[str, object]:
        snapshot = self._data_generation_jobs.cleanup_job(job_id)
        self.refresh_metadata_state()
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
            target = "oltp" if catalog_name == "pg_oltp" else "olap"
            status, _relations = self._postgres_source_snapshot(
                target=target,
                source_id=catalog_name,
                source_label="PostgreSQL OLTP" if target == "oltp" else "PostgreSQL OLAP",
            )
            if status.state != "connected":
                raise KeyError(f"Source object is unavailable because {catalog_name} is disconnected.")
            connection = None
            with self._lock:
                connection = self._postgres_health_connections.get(catalog_name)
            if connection is None:
                raise KeyError(f"Source object is unavailable because {catalog_name} is disconnected.")
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        attribute.attname AS column_name,
                        UPPER(format_type(attribute.atttypid, attribute.atttypmod)) AS data_type
                    FROM pg_class AS relation
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    JOIN pg_attribute AS attribute
                      ON attribute.attrelid = relation.oid
                    WHERE namespace.nspname = %s
                      AND relation.relname = %s
                      AND relation.relkind IN ('r', 'v', 'm')
                      AND attribute.attnum > 0
                      AND NOT attribute.attisdropped
                    ORDER BY attribute.attnum
                    """,
                    [schema_name, object_name],
                )
                rows = cursor.fetchall()
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
            with self._lock:
                conn = self._require_connection()
                rows = conn.execute(query, parameters).fetchall()
        else:
            raise KeyError(f"Unsupported source object relation: {relation}")

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

    def _create_worker_connection(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            if self._conn is not None:
                return self._conn.cursor()
        return self._create_connection()

    def _create_postgres_native_connection(self, target: str):
        normalized_target = str(target).strip().lower()
        if normalized_target == "oltp":
            database = self.settings.pg_oltp_database
        elif normalized_target == "olap":
            database = self.settings.pg_olap_database
        else:
            raise ValueError(f"Unsupported PostgreSQL native target: {target}")
        return self._open_postgres_connection(database)

    def _open_postgres_connection(self, database: str | None):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "psycopg is required for native PostgreSQL query execution."
            ) from exc

        if not all(
            (
                self.settings.pg_host,
                self.settings.pg_port,
                self.settings.pg_user,
                self.settings.pg_password,
                database,
            )
        ):
            raise RuntimeError(
                "PostgreSQL native execution requires PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, "
                "and the target database to be configured."
            )

        return psycopg.connect(
            host=self.settings.pg_host,
            port=int(self.settings.pg_port),
            user=self.settings.pg_user,
            password=self.settings.pg_password,
            dbname=database,
            autocommit=True,
            connect_timeout=10,
        )

    def _postgres_target_database(self, target: str) -> str | None:
        normalized_target = str(target).strip().lower()
        if normalized_target == "oltp":
            return self.settings.pg_oltp_database
        if normalized_target == "olap":
            return self.settings.pg_olap_database
        raise ValueError(f"Unsupported PostgreSQL target: {target}")

    def _close_persistent_postgres_connections(self) -> None:
        with self._lock:
            connections = list(self._postgres_health_connections.values())
            self._postgres_health_connections.clear()

        for connection in connections:
            try:
                connection.close()
            except Exception:
                pass

    def _close_persistent_postgres_connection(self, source_id: str) -> None:
        connection = None
        with self._lock:
            connection = self._postgres_health_connections.pop(source_id, None)

        if connection is None:
            return

        try:
            connection.close()
        except Exception:
            pass

    def _postgres_source_snapshot(
        self,
        *,
        target: str,
        source_id: str,
        source_label: str,
    ) -> tuple[SourceConnectionStatus, list[SqlDiscoveredRelation]]:
        database = self._postgres_target_database(target)
        if not database:
            return (
                SourceConnectionStatus(
                    source_id=source_id,
                    state="disconnected",
                    label="Disconnected",
                    detail=f"{source_label} is not configured.",
                ),
                [],
            )

        connection = None
        with self._lock:
            connection = self._postgres_health_connections.get(source_id)

        if connection is not None:
            try:
                relations = self._fetch_postgres_relations(connection)
                return (
                    SourceConnectionStatus(
                        source_id=source_id,
                        state="connected",
                        label="Connected",
                        detail=f"{source_label} is connected.",
                    ),
                    relations,
                )
            except Exception:
                self._close_persistent_postgres_connection(source_id)

        try:
            connection = self._open_postgres_connection(database)
            relations = self._fetch_postgres_relations(connection)
            with self._lock:
                self._postgres_health_connections[source_id] = connection
            return (
                SourceConnectionStatus(
                    source_id=source_id,
                    state="connected",
                    label="Connected",
                    detail=f"{source_label} is connected.",
                ),
                relations,
            )
        except Exception as exc:
            try:
                if connection is not None:
                    connection.close()
            except Exception:
                pass
            self._close_persistent_postgres_connection(source_id)
            return (
                SourceConnectionStatus(
                    source_id=source_id,
                    state="disconnected",
                    label="Disconnected",
                    detail=f"{source_label} connection failed: {exc}",
                ),
                [],
            )

    def _fetch_postgres_relations(self, connection) -> list[SqlDiscoveredRelation]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    namespace.nspname AS schema_name,
                    relation.relname AS relation_name,
                    CASE relation.relkind
                        WHEN 'r' THEN 'table'
                        WHEN 'v' THEN 'view'
                        WHEN 'm' THEN 'materialized view'
                        ELSE 'table'
                    END AS relation_kind
                FROM pg_class AS relation
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE relation.relkind IN ('r', 'v', 'm')
                  AND namespace.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND namespace.nspname NOT LIKE 'pg_toast%'
                ORDER BY namespace.nspname, relation.relname
                """
            )
            rows = cursor.fetchall()

        return [
            SqlDiscoveredRelation(
                schema_name=str(schema_name),
                relation_name=str(relation_name),
                relation_kind=str(relation_kind),
            )
            for schema_name, relation_name, relation_kind in rows
        ]

    def _postgres_catalog_objects(self, target: str) -> dict[str, list[SourceObject]]:
        source_id = "pg_oltp" if target == "oltp" else "pg_olap"
        _status, relations = self._postgres_source_snapshot(
            target=target,
            source_id=source_id,
            source_label="PostgreSQL OLTP" if target == "oltp" else "PostgreSQL OLAP",
        )
        grouped: dict[str, list[SourceObject]] = {}
        for relation in relations:
            grouped.setdefault(relation.schema_name, []).append(
                SourceObject(
                    name=relation.relation_name,
                    kind=relation.relation_kind,
                    relation=f"{source_id}.{relation.schema_name}.{relation.relation_name}",
                )
            )
        for objects in grouped.values():
            objects.sort(key=lambda item: item.name.lower())
        return grouped

    def _resolve_notebook_title(self, notebook_id: str) -> str | None:
        with self._lock:
            for notebook in self._notebooks:
                if notebook.notebook_id == notebook_id:
                    return notebook.title
        return None

    def _log_startup(
        self,
        message: str,
        *args: object,
        level: int = logging.INFO,
        exc_info: bool = False,
    ) -> None:
        logger.log(level, f"[bdw-startup] {message}", *args, exc_info=exc_info)

    def _log_startup_section(self, title: str) -> None:
        self._log_startup(STARTUP_DIVIDER)
        self._log_startup("Startup task: %s", title)

    def _shared_notebook_store_path(self) -> Path:
        return self.settings.duckdb_database.parent / "shared-notebooks.json"

    def _combined_notebooks(self, catalogs: list[SourceCatalog]) -> list[NotebookDefinition]:
        return [*build_notebooks(catalogs), *self._shared_notebook_store.list_notebooks()]

    def _rebuild_notebooks_locked(self) -> None:
        self._notebooks = self._combined_notebooks(self._catalogs)

    def _notebook_events_state_locked(self) -> dict[str, object]:
        return {
            "version": self._notebook_events_version,
            "events": [event.payload for event in self._notebook_events],
        }

    def _append_notebook_event_locked(
        self,
        *,
        event_type: str,
        notebook: NotebookDefinition,
        origin_client_id: str = "",
    ) -> None:
        self._notebook_events.append(
            NotebookEventDefinition(
                event_id=f"notebook-event-{uuid.uuid4().hex}",
                event_type=event_type,
                notebook_id=notebook.notebook_id,
                notebook_title=notebook.title,
                occurred_at=datetime.now(UTC).isoformat(),
                origin_client_id=origin_client_id,
            )
        )
        if len(self._notebook_events) > MAX_NOTEBOOK_EVENT_HISTORY:
            self._notebook_events = self._notebook_events[-MAX_NOTEBOOK_EVENT_HISTORY:]
        self._notebook_events_version += 1
        self._condition.notify_all()

    def _set_minimal_state(self) -> None:
        catalogs: list[SourceCatalog] = []
        self._catalogs = catalogs
        self._notebooks = self._combined_notebooks(catalogs)
        self._completion_schema = build_completion_schema(catalogs)
        self._source_options = build_source_options(catalogs)

    def _open_duckdb_connection(
        self,
        *,
        purpose: str,
        startup_context: bool,
    ) -> duckdb.DuckDBPyConnection:
        max_attempts = 20
        retry_delay_seconds = 0.5
        database_path = str(self.settings.duckdb_database)

        for attempt in range(1, max_attempts + 1):
            try:
                return duckdb.connect(database_path)
            except duckdb.IOException as exc:
                message = str(exc).lower()
                lock_conflict = (
                    "being used by another process" in message
                    or "file is already open" in message
                )
                if not lock_conflict or attempt >= max_attempts:
                    raise

                retry_message = (
                    "DuckDB connection for %s is waiting for the database file lock to clear "
                    "(attempt %d/%d, path=%s)."
                )
                if startup_context:
                    self._log_startup(
                        retry_message,
                        purpose,
                        attempt,
                        max_attempts,
                        self.settings.duckdb_database,
                        level=logging.WARNING,
                    )
                else:
                    logger.warning(
                        retry_message,
                        purpose,
                        attempt,
                        max_attempts,
                        self.settings.duckdb_database,
                    )
                time.sleep(retry_delay_seconds)

    def _create_connection(
        self,
        *,
        startup_context: bool = False,
        run_s3_startup_diagnostics: bool = False,
    ) -> duckdb.DuckDBPyConnection:
        self.settings.duckdb_database.parent.mkdir(parents=True, exist_ok=True)
        self.settings.duckdb_extension_directory.mkdir(parents=True, exist_ok=True)

        conn = self._open_duckdb_connection(
            purpose="primary workbench startup" if startup_context else "worker connection",
            startup_context=startup_context,
        )
        conn.execute(
            f"SET extension_directory = {sql_literal(self.settings.duckdb_extension_directory.as_posix())}"
        )
        self._configure_s3_tls(conn)

        self._ensure_extension(conn, "httpfs")
        self._ensure_extension(conn, "postgres")
        self._bootstrap_integrations(
            conn,
            startup_context=startup_context,
            run_s3_startup_diagnostics=run_s3_startup_diagnostics,
        )
        return conn

    def _configure_s3_tls(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            f"SET enable_server_cert_verification = {'true' if self.settings.s3_verify_ssl else 'false'}"
        )
        effective_ca_bundle = self.settings.effective_s3_ca_cert_file()
        if effective_ca_bundle is not None:
            conn.execute(f"SET ca_cert_file = {sql_literal(effective_ca_bundle.as_posix())}")

    def _ensure_extension(self, conn: duckdb.DuckDBPyConnection, extension: str) -> None:
        try:
            conn.execute(f"LOAD {extension}")
        except duckdb.Error:
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")

    def _bootstrap_integrations(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        startup_context: bool = False,
        run_s3_startup_diagnostics: bool = False,
    ) -> None:
        if startup_context:
            self._log_startup_section("Bootstrap optional integrations")
        try:
            self._bootstrap_s3(
                conn,
                startup_context=startup_context,
                run_startup_diagnostics=run_s3_startup_diagnostics,
            )
        except Exception as exc:
            if startup_context:
                self._log_startup(
                    "S3 bootstrap failed; continuing without S3 integration: %s",
                    exc,
                    level=logging.WARNING,
                    exc_info=True,
                )
            else:
                logger.warning("S3 bootstrap failed; continuing without S3 integration: %s", exc, exc_info=True)

        try:
            self._bootstrap_postgres(conn, startup_context=startup_context)
        except Exception as exc:
            if startup_context:
                self._log_startup(
                    "PostgreSQL bootstrap failed; continuing without attached PostgreSQL catalogs: %s",
                    exc,
                    level=logging.WARNING,
                    exc_info=True,
                )
            else:
                logger.warning(
                    "PostgreSQL bootstrap failed; continuing without attached PostgreSQL catalogs: %s",
                    exc,
                    exc_info=True,
                )

        if startup_context:
            self._log_startup("Startup step complete: optional integrations bootstrapped")

    def _bootstrap_s3(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        startup_context: bool = False,
        run_startup_diagnostics: bool = False,
    ) -> None:
        access_key_id = self.settings.current_s3_access_key_id()
        secret_access_key = self.settings.current_s3_secret_access_key()
        required_values = (
            self.settings.s3_endpoint,
            access_key_id,
            secret_access_key,
        )

        if not any(required_values):
            if startup_context:
                self._log_startup("S3 bootstrap skipped: no S3 configuration is present")
            return

        missing = [
            name
            for name, value in (
                ("S3_ENDPOINT", self.settings.s3_endpoint),
                ("S3_ACCESS_KEY_ID", access_key_id),
                ("S3_SECRET_ACCESS_KEY", secret_access_key),
            )
            if value is None
        ]
        if missing:
            message = f"S3 bootstrap skipped because required variables are missing: {', '.join(missing)}"
            if startup_context:
                self._log_startup(message, level=logging.WARNING)
            else:
                logger.warning(message)
            return

        endpoint, use_ssl, transport_reason = normalize_s3_endpoint(
            self.settings.s3_endpoint,
            use_ssl=self.settings.s3_use_ssl,
            verify_ssl=self.settings.s3_verify_ssl,
        )
        if startup_context and transport_reason:
            self._log_startup(
                "S3 transport override: raw_endpoint=%s configured_use_ssl=%s effective_use_ssl=%s reason=%s",
                self.settings.s3_endpoint,
                self.settings.s3_use_ssl,
                use_ssl,
                transport_reason,
            )
        if startup_context:
            self._log_startup_section("Ensure bootstrap S3 bucket and seed data")
            self._ensure_s3_startup_seed_data(use_ssl=use_ssl)

        startup_views = parse_s3_startup_views(self.settings.s3_startup_views)
        options = self._s3_secret_options(endpoint=endpoint, use_ssl=use_ssl)
        if startup_context:
            self._log_startup_section("Configure DuckDB S3 secret")
            self._log_startup(
                "S3 bootstrap step: configuring DuckDB secret for endpoint=%s bucket=%s url_style=%s startup_views=%d",
                endpoint,
                self.settings.s3_bucket,
                (self.settings.s3_url_style or "").strip() or "default",
                len(startup_views),
            )
        conn.execute(f"CREATE OR REPLACE SECRET bdw_s3 ({', '.join(options)})")
        if startup_context:
            self._log_startup("S3 bootstrap step complete: DuckDB secret is configured")
        if run_startup_diagnostics:
            self._log_s3_startup_diagnostics(
                conn,
                endpoint=endpoint,
                use_ssl=use_ssl,
                startup_views=startup_views,
            )
            conn.execute(f"CREATE OR REPLACE SECRET bdw_s3 ({', '.join(options)})")
        if startup_views and startup_context:
            self._log_startup(
                "S3 startup views are managed by S3 data source discovery: %d configured view(s)",
                len(startup_views),
            )

    def _s3_secret_options(
        self,
        *,
        endpoint: str,
        use_ssl: bool,
        url_style: str | None = None,
    ) -> list[str]:
        effective_url_style = effective_s3_url_style(
            self.settings,
            endpoint=endpoint,
            use_ssl=use_ssl,
            explicit_url_style=url_style,
        )
        access_key_id = self.settings.current_s3_access_key_id()
        secret_access_key = self.settings.current_s3_secret_access_key()
        session_token = self.settings.current_s3_session_token()
        if access_key_id is None or secret_access_key is None:
            raise ValueError("S3 credentials are incomplete. Configure access key and secret key values or file paths.")
        options = [
            "TYPE s3",
            "PROVIDER config",
            f"KEY_ID {sql_literal(access_key_id)}",
            f"SECRET {sql_literal(secret_access_key)}",
            f"ENDPOINT {sql_literal(endpoint)}",
            f"USE_SSL {'true' if use_ssl else 'false'}",
        ]
        if effective_url_style:
            options.append(f"URL_STYLE {sql_literal(effective_url_style)}")
        if session_token:
            options.append(f"SESSION_TOKEN {sql_literal(session_token)}")
        return options

    def _log_s3_diagnostic_trial(
        self,
        *,
        backend: str,
        trial: str,
        success: bool,
        detail: str,
    ) -> None:
        self._log_startup(
            "S3 startup diagnostic [%s] %s %s: %s",
            backend,
            "ok" if success else "failed",
            trial,
            detail,
            level=logging.INFO,
        )

    def _s3_duckdb_probe_query(self, data_format: str, path: str) -> str:
        if data_format == "parquet":
            return f"SELECT * FROM read_parquet({sql_literal(path)}) LIMIT 1"
        if data_format == "csv":
            return f"SELECT * FROM read_csv_auto({sql_literal(path)}) LIMIT 1"
        if data_format == "json":
            return f"SELECT * FROM read_json_auto({sql_literal(path)}) LIMIT 1"
        raise ValueError(f"Unsupported S3 startup view format: {data_format}")

    def _ensure_s3_startup_seed_data(self, *, use_ssl: bool) -> None:
        bucket_name = (self.settings.s3_bucket or "").strip()
        if not bucket_name:
            self._log_startup(
                "S3 startup seed skipped: no S3_BUCKET is configured",
                level=logging.INFO,
            )
            return

        try:
            client = s3_client(self.settings, use_ssl=use_ssl)
        except Exception as exc:
            self._log_startup(
                "S3 startup seed skipped because the boto3 client could not be created: %s",
                exc,
                level=logging.WARNING,
            )
            return

        try:
            initial_buckets = list_s3_buckets_from_client(client)
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial="list_buckets[startup-seed:before]",
                success=True,
                detail=f"discovered {len(initial_buckets)} bucket(s): {initial_buckets}",
            )
        except Exception as exc:
            initial_buckets = []
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial="list_buckets[startup-seed:before]",
                success=False,
                detail=str(exc),
            )

        bucket_created = False
        if bucket_name not in initial_buckets:
            try:
                client.create_bucket(Bucket=bucket_name)
                bucket_created = True
                self._log_s3_diagnostic_trial(
                    backend="boto3",
                    trial=f"create_bucket[startup-seed]:{bucket_name}",
                    success=True,
                    detail="created bootstrap bucket successfully",
                )
            except Exception as exc:
                self._log_s3_diagnostic_trial(
                    backend="boto3",
                    trial=f"create_bucket[startup-seed]:{bucket_name}",
                    success=False,
                    detail=str(exc),
                )
                return
        else:
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial=f"create_bucket[startup-seed]:{bucket_name}",
                success=True,
                detail="bucket already existed; creation was not required",
            )

        try:
            bucket_names = list_s3_buckets_from_client(client)
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial="list_buckets[startup-seed:after]",
                success=True,
                detail=f"discovered {len(bucket_names)} bucket(s): {bucket_names}",
            )
        except Exception as exc:
            bucket_names = initial_buckets
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial="list_buckets[startup-seed:after]",
                success=False,
                detail=str(exc),
            )

        existing_keys: list[str] = []
        try:
            response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=20)
            existing_keys = [
                str(item.get("Key"))
                for item in (response.get("Contents") or [])
                if str(item.get("Key") or "").strip()
            ]
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial=f"list_objects_v2[startup-seed:before]:{bucket_name}",
                success=True,
                detail=f"returned {len(existing_keys)} object(s): {existing_keys}",
            )
        except Exception as exc:
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial=f"list_objects_v2[startup-seed:before]:{bucket_name}",
                success=False,
                detail=str(exc),
            )
            return

        if S3_BOOTSTRAP_SAMPLE_KEY not in existing_keys:
            try:
                payload = S3_BOOTSTRAP_SAMPLE_CSV.encode("utf-8")
                with TemporaryDirectory(prefix="bdw-s3-startup-seed-") as temp_dir:
                    temp_path = Path(temp_dir) / "vat_context_bootstrap.csv"
                    temp_path.write_bytes(payload)
                    upload_s3_file(
                        client,
                        local_path=temp_path,
                        bucket=bucket_name,
                        key=S3_BOOTSTRAP_SAMPLE_KEY,
                    )
                    download_path = Path(temp_dir) / "vat_context_bootstrap.download.csv"
                    download_s3_file(
                        client,
                        bucket=bucket_name,
                        key=S3_BOOTSTRAP_SAMPLE_KEY,
                        local_path=download_path,
                    )
                    downloaded_payload = download_path.read_bytes()
                self._log_s3_diagnostic_trial(
                    backend="boto3",
                    trial=f"upload_file[startup-seed]:{bucket_name}/{S3_BOOTSTRAP_SAMPLE_KEY}",
                    success=True,
                    detail=(
                        f"uploaded {len(payload)} byte(s), downloaded {len(downloaded_payload)} byte(s), "
                        f"payload_match={downloaded_payload == payload}"
                    ),
                )
            except Exception as exc:
                self._log_s3_diagnostic_trial(
                    backend="boto3",
                    trial=f"upload_file[startup-seed]:{bucket_name}/{S3_BOOTSTRAP_SAMPLE_KEY}",
                    success=False,
                    detail=str(exc),
                )
                return
        else:
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial=f"upload_file[startup-seed]:{bucket_name}/{S3_BOOTSTRAP_SAMPLE_KEY}",
                success=True,
                detail="bootstrap CSV already existed; upload was not required",
            )

        try:
            response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=20)
            listed_keys = [
                str(item.get("Key"))
                for item in (response.get("Contents") or [])
                if str(item.get("Key") or "").strip()
            ]
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial=f"list_objects_v2[startup-seed:after]:{bucket_name}",
                success=True,
                detail=f"returned {len(listed_keys)} object(s): {listed_keys}",
            )
        except Exception as exc:
            self._log_s3_diagnostic_trial(
                backend="boto3",
                trial=f"list_objects_v2[startup-seed:after]:{bucket_name}",
                success=False,
                detail=str(exc),
            )

        if bucket_created:
            self._log_startup(
                "S3 startup seed complete: created bootstrap bucket %s and ensured %s is present.",
                bucket_name,
                S3_BOOTSTRAP_SAMPLE_KEY,
            )

    def _log_s3_startup_diagnostics(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        endpoint: str,
        use_ssl: bool,
        startup_views: list[tuple[str, str, str]],
    ) -> None:
        configured_url_style = (self.settings.s3_url_style or "").strip().lower() or None
        effective_default_url_style = effective_s3_url_style(
            self.settings,
            endpoint=endpoint,
            use_ssl=use_ssl,
        )
        bucket_name = self.settings.s3_bucket or ""
        verify_value = s3_verify_value(self.settings)

        self._log_startup(
            "Starting S3 startup diagnostics: endpoint=%s use_ssl=%s verify_ssl=%s verify_value=%s ca_cert_file=%s url_style=%s bucket=%s startup_views=%d",
            endpoint,
            use_ssl,
            self.settings.s3_verify_ssl,
            verify_value,
            self.settings.effective_s3_ca_cert_file().as_posix()
            if self.settings.effective_s3_ca_cert_file()
            else "none",
            effective_default_url_style or "default",
            bucket_name or "n/a",
            len(startup_views),
        )

        successful_read_probe = False
        boto3_styles: list[tuple[str, str | None]] = []
        for label, style in (
            ((effective_default_url_style or "auto"), effective_default_url_style),
            ("path", "path"),
            ("virtual", "virtual"),
        ):
            if any(existing_label == label for existing_label, _ in boto3_styles):
                continue
            boto3_styles.append((label, style))

        for style_label, style_value in boto3_styles:
            style_read_ok = False
            try:
                client = s3_client(self.settings, url_style=style_value)
            except Exception as exc:
                self._log_s3_diagnostic_trial(
                    backend="boto3",
                    trial=f"client[{style_label}]",
                    success=False,
                    detail=str(exc),
                )
                continue

            try:
                response = client.list_buckets()
                bucket_names = [
                    str(item.get("Name")).strip()
                    for item in (response.get("Buckets") or [])
                    if str(item.get("Name") or "").strip()
                ]
                self._log_s3_diagnostic_trial(
                    backend="boto3",
                    trial=f"list_buckets[{style_label}]",
                    success=True,
                    detail=f"discovered {len(bucket_names)} bucket(s): {bucket_names}",
                )
                successful_read_probe = True
                style_read_ok = True
            except Exception as exc:
                self._log_s3_diagnostic_trial(
                    backend="boto3",
                    trial=f"list_buckets[{style_label}]",
                    success=False,
                    detail=str(exc),
                )
                continue

            for discovered_bucket_name in bucket_names:
                try:
                    response = client.list_objects_v2(Bucket=discovered_bucket_name, MaxKeys=20)
                    discovered_keys = [
                        item.get("Key")
                        for item in (response.get("Contents") or [])
                        if item.get("Key")
                    ]
                    successful_read_probe = True
                    style_read_ok = True
                    self._log_s3_diagnostic_trial(
                        backend="boto3",
                        trial=f"list_objects_v2[{style_label}]:{discovered_bucket_name}",
                        success=True,
                        detail=f"returned {len(discovered_keys)} object(s): {discovered_keys}",
                    )
                except Exception as exc:
                    self._log_s3_diagnostic_trial(
                        backend="boto3",
                        trial=f"list_objects_v2[{style_label}]:{discovered_bucket_name}",
                        success=False,
                        detail=str(exc),
                    )

            configured_bucket_name = bucket_name.strip()
            if configured_bucket_name and configured_bucket_name not in bucket_names:
                try:
                    response = client.list_objects_v2(Bucket=configured_bucket_name, MaxKeys=20)
                    configured_keys = [
                        item.get("Key")
                        for item in (response.get("Contents") or [])
                        if item.get("Key")
                    ]
                    successful_read_probe = True
                    style_read_ok = True
                    self._log_s3_diagnostic_trial(
                        backend="boto3",
                        trial=f"list_objects_v2[{style_label}]:configured:{configured_bucket_name}",
                        success=True,
                        detail=f"returned {len(configured_keys)} object(s): {configured_keys}",
                    )
                except Exception as exc:
                    self._log_s3_diagnostic_trial(
                        backend="boto3",
                        trial=f"list_objects_v2[{style_label}]:configured:{configured_bucket_name}",
                        success=False,
                        detail=str(exc),
                    )

            if style_read_ok:
                self._log_startup(
                    "S3 startup diagnostic [boto3] using style %s succeeded; skipping additional boto3 style probes.",
                    style_label,
                )
                break

        duckdb_styles: list[tuple[str, str | None]] = []
        for label, style in (
            ((effective_default_url_style or "default"), effective_default_url_style),
            ("path", "path"),
            ("vhost", "vhost"),
        ):
            if any(existing_label == label for existing_label, _ in duckdb_styles):
                continue
            duckdb_styles.append((label, style))

        if not startup_views:
            self._log_s3_diagnostic_trial(
                backend="duckdb",
                trial="read_probe",
                success=False,
                detail="skipped because no S3_STARTUP_VIEWS are configured",
            )
        else:
            for style_label, style_value in duckdb_styles:
                style_read_ok = False
                try:
                    options = self._s3_secret_options(
                        endpoint=endpoint,
                        use_ssl=use_ssl,
                        url_style=style_value,
                    )
                    conn.execute(f"CREATE OR REPLACE SECRET bdw_s3 ({', '.join(options)})")
                    self._log_s3_diagnostic_trial(
                        backend="duckdb",
                        trial=f"secret[{style_label}]",
                        success=True,
                        detail="secret configured successfully",
                    )
                except Exception as exc:
                    self._log_s3_diagnostic_trial(
                        backend="duckdb",
                        trial=f"secret[{style_label}]",
                        success=False,
                        detail=str(exc),
                    )
                    continue

                for view_name, data_format, path in startup_views:
                    try:
                        rows = conn.execute(self._s3_duckdb_probe_query(data_format, path)).fetchmany(1)
                        successful_read_probe = True
                        self._log_s3_diagnostic_trial(
                            backend="duckdb",
                            trial=f"read[{style_label}]:{view_name}",
                            success=True,
                            detail=f"returned {len(rows)} row(s) from {path}",
                        )
                        style_read_ok = True
                    except Exception as exc:
                        self._log_s3_diagnostic_trial(
                            backend="duckdb",
                            trial=f"read[{style_label}]:{view_name}",
                            success=False,
                            detail=str(exc),
                        )

                if style_read_ok:
                    self._log_startup(
                        "S3 startup diagnostic [duckdb] using style %s succeeded; skipping additional DuckDB style probes.",
                        style_label,
                    )
                    break

        try:
            restored_options = self._s3_secret_options(endpoint=endpoint, use_ssl=use_ssl)
            conn.execute(f"CREATE OR REPLACE SECRET bdw_s3 ({', '.join(restored_options)})")
            self._log_s3_diagnostic_trial(
                backend="duckdb",
                trial=f"secret-restore[{effective_default_url_style or 'default'}]",
                success=True,
                detail="restored configured S3 secret after diagnostics",
            )
        except Exception as exc:
            self._log_s3_diagnostic_trial(
                backend="duckdb",
                trial=f"secret-restore[{effective_default_url_style or 'default'}]",
                success=False,
                detail=str(exc),
            )

        if not successful_read_probe:
            logger.warning(
                "S3 startup diagnostics did not complete a successful read probe for bucket %r. Review the trial logs above.",
                bucket_name,
            )

    def _create_s3_diagnostic_connection(self) -> duckdb.DuckDBPyConnection:
        conn = self._open_duckdb_connection(
            purpose="background S3 diagnostics",
            startup_context=False,
        )
        conn.execute(
            f"SET extension_directory = {sql_literal(self.settings.duckdb_extension_directory.as_posix())}"
        )
        self._configure_s3_tls(conn)
        self._ensure_extension(conn, "httpfs")
        return conn

    def _start_background_s3_startup_diagnostics(self) -> None:
        access_key_id = self.settings.current_s3_access_key_id()
        secret_access_key = self.settings.current_s3_secret_access_key()
        if not any(
            (
                self.settings.s3_endpoint,
                self.settings.s3_bucket,
                access_key_id,
                secret_access_key,
            )
        ):
            self._log_startup("Background S3 startup diagnostics skipped: S3 is not configured")
            return

        worker = Thread(
            target=self._run_background_s3_startup_diagnostics,
            daemon=True,
            name="bdw-startup-s3-diagnostics",
        )
        worker.start()
        self._startup_threads.append(worker)
        self._log_startup("Background S3 startup diagnostics scheduled on thread %s", worker.name)

    def _run_background_s3_startup_diagnostics(self) -> None:
        started = time.perf_counter()
        self._log_startup_section("Run background S3 startup diagnostics")
        self._log_startup("Background S3 startup diagnostics begin")
        conn: duckdb.DuckDBPyConnection | None = None
        try:
            access_key_id = self.settings.current_s3_access_key_id()
            secret_access_key = self.settings.current_s3_secret_access_key()
            missing = [
                name
                for name, value in (
                    ("S3_ENDPOINT", self.settings.s3_endpoint),
                    ("S3_ACCESS_KEY_ID", access_key_id),
                    ("S3_SECRET_ACCESS_KEY", secret_access_key),
                )
                if value is None
            ]
            if missing:
                self._log_startup(
                    "Background S3 startup diagnostics skipped because required variables are missing: %s",
                    ", ".join(missing),
                    level=logging.WARNING,
                )
                return

            endpoint, use_ssl, transport_reason = normalize_s3_endpoint(
                self.settings.s3_endpoint,
                use_ssl=self.settings.s3_use_ssl,
                verify_ssl=self.settings.s3_verify_ssl,
            )
            if transport_reason:
                self._log_startup(
                    "Background S3 diagnostics transport override: raw_endpoint=%s configured_use_ssl=%s effective_use_ssl=%s reason=%s",
                    self.settings.s3_endpoint,
                    self.settings.s3_use_ssl,
                    use_ssl,
                    transport_reason,
                )
            startup_views = parse_s3_startup_views(self.settings.s3_startup_views)
            conn = self._create_s3_diagnostic_connection()
            options = self._s3_secret_options(endpoint=endpoint, use_ssl=use_ssl)
            conn.execute(f"CREATE OR REPLACE SECRET bdw_s3 ({', '.join(options)})")
            self._log_s3_startup_diagnostics(
                conn,
                endpoint=endpoint,
                use_ssl=use_ssl,
                startup_views=startup_views,
            )
            try:
                self.refresh_metadata_state()
            except Exception as exc:
                self._log_startup(
                    "Background S3 startup metadata refresh failed after diagnostics/views: %s",
                    exc,
                    level=logging.WARNING,
                    exc_info=True,
                )
        except Exception as exc:
            self._log_startup(
                "Background S3 startup diagnostics failed: %s",
                exc,
                level=logging.WARNING,
                exc_info=True,
            )
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            self._log_startup(
                "Background S3 startup diagnostics finished in %.2fs",
                time.perf_counter() - started,
            )

    def _bootstrap_postgres(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        startup_context: bool = False,
    ) -> None:
        if not all(
            (
                self.settings.pg_host,
                self.settings.pg_port,
                self.settings.pg_user,
                self.settings.pg_password,
            )
        ):
            if startup_context:
                self._log_startup("PostgreSQL bootstrap skipped: core PostgreSQL connection settings are incomplete")
            return

        port = normalize_port(self.settings.pg_port, "PG_PORT")

        if self.settings.pg_oltp_database:
            if startup_context:
                self._log_startup_section("Attach PostgreSQL OLTP catalog")
                self._log_startup(
                    "PostgreSQL bootstrap step: attaching OLTP catalog %s on %s:%s",
                    self.settings.pg_oltp_database,
                    self.settings.pg_host,
                    port,
                )
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
                read_only=False,
            )
            if startup_context:
                self._log_startup("PostgreSQL bootstrap step complete: OLTP catalog attached")

        if self.settings.pg_olap_database:
            if startup_context:
                self._log_startup_section("Attach PostgreSQL OLAP catalog")
                self._log_startup(
                    "PostgreSQL bootstrap step: attaching OLAP catalog %s on %s:%s",
                    self.settings.pg_olap_database,
                    self.settings.pg_host,
                    port,
                )
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
            if startup_context:
                self._log_startup("PostgreSQL bootstrap step complete: OLAP catalog attached")

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
        source_statuses = self._data_source_discovery.source_statuses()
        workspace_rows = conn.execute(
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
        workspace_schema_labels: dict[str, str] = {}

        workspace_source_id = "workspace.s3"
        workspace_status = source_statuses.get(workspace_source_id)
        workspace_discovery_enabled = workspace_status is None or workspace_status.state == "connected"

        if (
            workspace_discovery_enabled
            and self.settings.s3_endpoint
            and self.settings.current_s3_access_key_id()
            and self.settings.current_s3_secret_access_key()
        ):
            try:
                bucket_names = list_s3_buckets(self.settings)
            except Exception as exc:
                logger.warning("Failed to list S3 buckets during metadata refresh: %s", exc)
                bucket_names = [self.settings.s3_bucket] if self.settings.s3_bucket else []

            for bucket_name in bucket_names:
                schema_name = s3_bucket_schema_name(bucket_name)
                grouped["workspace"].setdefault(schema_name, [])
                workspace_schema_labels[schema_name] = bucket_name
        elif self.settings.s3_bucket:
            schema_name = s3_bucket_schema_name(self.settings.s3_bucket)
            grouped["workspace"].setdefault(schema_name, [])
            workspace_schema_labels[schema_name] = self.settings.s3_bucket
        if self.settings.pg_oltp_database:
            grouped["pg_oltp"].setdefault("public", [])
        if self.settings.pg_olap_database:
            grouped["pg_olap"].setdefault("public", [])

        for catalog, schema, table_name, table_type in workspace_rows:
            if catalog in {"pg_oltp", "pg_olap"}:
                continue
            grouped.setdefault("workspace", {}).setdefault(schema, []).append(
                SourceObject(
                    name=table_name,
                    kind="view" if table_type.upper() == "VIEW" else "table",
                    relation=f"{schema}.{table_name}",
                )
            )

        pg_oltp_status = source_statuses.get("pg_oltp")
        if self.settings.pg_oltp_database:
            if pg_oltp_status is None or pg_oltp_status.state == "connected":
                grouped["pg_oltp"] = self._postgres_catalog_objects("oltp")
            else:
                grouped["pg_oltp"] = {}
            grouped["pg_oltp"].setdefault("public", [])
        pg_olap_status = source_statuses.get("pg_olap")
        if self.settings.pg_olap_database:
            if pg_olap_status is None or pg_olap_status.state == "connected":
                grouped["pg_olap"] = self._postgres_catalog_objects("olap")
            else:
                grouped["pg_olap"] = {}
            grouped["pg_olap"].setdefault("public", [])

        catalogs: list[SourceCatalog] = []
        for catalog_name in ("workspace", "pg_oltp", "pg_olap"):
            catalog_source_id = workspace_source_id if catalog_name == "workspace" else catalog_name
            status = source_statuses.get(catalog_source_id)
            schemas = [
                SourceSchema(
                    name=schema_name,
                    label=workspace_schema_labels.get(schema_name, schema_name)
                    if catalog_name == "workspace"
                    else None,
                    objects=objects,
                )
                for schema_name, objects in sorted(
                    grouped.get(catalog_name, {}).items(),
                    key=lambda item: (
                        workspace_schema_labels.get(item[0], item[0]).lower()
                        if catalog_name == "workspace"
                        else item[0].lower()
                    ),
                )
            ]
            if schemas or status is not None or self._data_source_discovery.supports_manual_connection_control(catalog_source_id):
                catalogs.append(
                    SourceCatalog(
                        name=catalog_name,
                        connection_source_id=catalog_source_id,
                        schemas=schemas,
                        connection_status=status.state if status is not None else None,
                        connection_label=status.label if status is not None else None,
                        connection_detail=status.detail if status is not None else None,
                        connection_controls_enabled=self._data_source_discovery.supports_manual_connection_control(
                            catalog_source_id
                        ),
                    )
                )

        self._catalogs = catalogs
        self._notebooks = self._combined_notebooks(catalogs)
        self._completion_schema = build_completion_schema(catalogs)
        self._source_options = build_source_options(catalogs)

    def _drop_catalog_schema_objects(self, catalog_name: str, schema_name: str) -> int:
        target = "oltp" if catalog_name == "pg_oltp" else "olap"
        status, relations = self._postgres_source_snapshot(
            target=target,
            source_id=catalog_name,
            source_label="PostgreSQL OLTP" if target == "oltp" else "PostgreSQL OLAP",
        )
        if status.state != "connected":
            raise ValueError(f"{catalog_name} is disconnected; cleanup cannot proceed.")

        with self._lock:
            connection = self._postgres_health_connections.get(catalog_name)
        if connection is None:
            raise ValueError(f"{catalog_name} is disconnected; cleanup cannot proceed.")

        scoped_relations = [relation for relation in relations if relation.schema_name == schema_name]
        for relation in scoped_relations:
            drop_kind = "MATERIALIZED VIEW" if relation.relation_kind == "materialized view" else relation.relation_kind.upper()
            with connection.cursor() as cursor:
                cursor.execute(
                    f"DROP {drop_kind} IF EXISTS {sql_identifier(schema_name)}.{sql_identifier(relation.relation_name)} CASCADE"
                )
        return len(scoped_relations)

    def _drop_workspace_schema_objects(self, schema_name: str) -> int:
        with self._lock:
            conn = self._require_connection()
            rows = conn.execute(
                """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_catalog NOT IN ('pg_oltp', 'pg_olap') AND table_schema = ?
                ORDER BY CASE WHEN table_type = 'VIEW' THEN 0 ELSE 1 END, table_name
                """,
                [schema_name],
            ).fetchall()
            for table_name, table_type in rows:
                relation = qualified_name(schema_name, table_name)
                if str(table_type).upper() == "VIEW":
                    conn.execute(f"DROP VIEW IF EXISTS {relation} CASCADE")
                else:
                    conn.execute(f"DROP TABLE IF EXISTS {relation} CASCADE")
            return len(rows)

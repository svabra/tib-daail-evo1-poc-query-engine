from __future__ import annotations

import hashlib
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb

from ..config import Settings
from ..models import SourceField
from .sql_utils import qualified_name, sql_identifier, sql_literal


SUPPORTED_LOCAL_WORKSPACE_QUERY_FORMATS = {"csv", "json", "parquet"}


def _normalized_identifier(
    value: str,
    *,
    prefix: str,
    max_base_length: int = 24,
) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        normalized = prefix
    if normalized[0].isdigit():
        normalized = f"{prefix}_{normalized}"
    normalized = normalized[:max_base_length].strip("_") or prefix
    digest = hashlib.sha1(str(value or "").encode("utf-8")).hexdigest()[:10]
    return f"{normalized}_{digest}"


def normalize_local_workspace_query_format(
    *,
    file_name: str,
    export_format: str = "",
    mime_type: str = "",
) -> str:
    normalized_export_format = str(export_format or "").strip().lower()
    if normalized_export_format in SUPPORTED_LOCAL_WORKSPACE_QUERY_FORMATS:
        return normalized_export_format

    suffix = Path(str(file_name or "").strip()).suffix.lower().lstrip(".")
    if suffix in SUPPORTED_LOCAL_WORKSPACE_QUERY_FORMATS:
        return suffix
    if suffix in {"jsonl", "ndjson"}:
        return "json"

    normalized_mime_type = str(mime_type or "").strip().lower()
    if "csv" in normalized_mime_type:
        return "csv"
    if "json" in normalized_mime_type:
        return "json"
    if "parquet" in normalized_mime_type:
        return "parquet"

    raise ValueError(
        "Local Workspace query support currently handles CSV, JSON, and Parquet files only."
    )


@dataclass(slots=True)
class LocalWorkspaceQuerySourceSyncResult:
    logical_relation: str
    relation: str
    fields: list[SourceField]

    @property
    def payload(self) -> dict[str, object]:
        return {
            "logicalRelation": self.logical_relation,
            "relation": self.relation,
            "fields": [field.payload for field in self.fields],
        }


class LocalWorkspaceQuerySourceManager:
    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings

    def sync_source(
        self,
        *,
        conn: duckdb.DuckDBPyConnection,
        client_id: str,
        entry_id: str,
        logical_relation: str,
        file_name: str,
        export_format: str = "",
        mime_type: str = "",
        file_bytes: bytes,
        csv_delimiter: str = "",
        csv_has_header: bool = True,
    ) -> LocalWorkspaceQuerySourceSyncResult:
        normalized_client_id = str(client_id or "").strip()
        normalized_entry_id = str(entry_id or "").strip()
        normalized_logical_relation = str(logical_relation or "").strip()
        normalized_file_name = Path(str(file_name or "").strip()).name
        if not normalized_client_id:
            raise ValueError("Missing workbench client id for Local Workspace query sync.")
        if not normalized_entry_id:
            raise ValueError("Missing Local Workspace entry id for Local Workspace query sync.")
        if not normalized_logical_relation:
            raise ValueError("Missing Local Workspace relation for Local Workspace query sync.")
        if not normalized_file_name:
            raise ValueError("Missing file name for Local Workspace query sync.")
        if not file_bytes:
            raise ValueError("The Local Workspace file is empty and cannot be queried.")

        query_format = normalize_local_workspace_query_format(
            file_name=normalized_file_name,
            export_format=export_format,
            mime_type=mime_type,
        )
        schema_name = self._schema_name(normalized_client_id)
        table_name = self._table_name(normalized_entry_id)
        relation = f"{schema_name}.{table_name}"

        local_path = self._query_file_path(
            client_id=normalized_client_id,
            entry_id=normalized_entry_id,
            file_name=normalized_file_name,
            query_format=query_format,
        )
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._remove_stale_entry_files(local_path.parent, table_name, keep_path=local_path)
        local_path.write_bytes(file_bytes)

        relation_name = qualified_name(schema_name, table_name)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_identifier(schema_name)}")
        conn.execute(f"DROP VIEW IF EXISTS {relation_name}")
        conn.execute(f"DROP TABLE IF EXISTS {relation_name}")
        conn.execute(
            f"CREATE VIEW {relation_name} AS SELECT * FROM "
            f"{self._reader_sql(local_path, query_format, csv_delimiter, csv_has_header)}"
        )

        return LocalWorkspaceQuerySourceSyncResult(
            logical_relation=normalized_logical_relation,
            relation=relation,
            fields=self._relation_fields(conn, schema_name=schema_name, table_name=table_name),
        )

    def delete_source(
        self,
        *,
        conn: duckdb.DuckDBPyConnection,
        client_id: str,
        entry_id: str,
    ) -> None:
        normalized_client_id = str(client_id or "").strip()
        normalized_entry_id = str(entry_id or "").strip()
        if not normalized_client_id or not normalized_entry_id:
            return

        schema_name = self._schema_name(normalized_client_id)
        table_name = self._table_name(normalized_entry_id)
        relation_name = qualified_name(schema_name, table_name)
        conn.execute(f"DROP VIEW IF EXISTS {relation_name}")
        conn.execute(f"DROP TABLE IF EXISTS {relation_name}")

        client_root = self._client_root(normalized_client_id)
        self._remove_stale_entry_files(client_root, table_name)
        self._remove_empty_directories(client_root)

    def clear_client_sources(
        self,
        *,
        conn: duckdb.DuckDBPyConnection,
        client_id: str,
    ) -> None:
        normalized_client_id = str(client_id or "").strip()
        if not normalized_client_id:
            return

        schema_name = self._schema_name(normalized_client_id)
        conn.execute(f"DROP SCHEMA IF EXISTS {sql_identifier(schema_name)} CASCADE")
        shutil.rmtree(self._client_root(normalized_client_id), ignore_errors=True)

    def _schema_name(self, client_id: str) -> str:
        return _normalized_identifier(client_id, prefix="workspace_local")

    def _table_name(self, entry_id: str) -> str:
        return _normalized_identifier(entry_id, prefix="entry")

    def _query_root(self) -> Path:
        return self._settings.duckdb_database.parent / "local-workspace-query-sources"

    def _client_root(self, client_id: str) -> Path:
        return self._query_root() / self._schema_name(client_id)

    def _query_file_path(
        self,
        *,
        client_id: str,
        entry_id: str,
        file_name: str,
        query_format: str,
    ) -> Path:
        suffix = Path(file_name).suffix if Path(file_name).suffix else f".{query_format}"
        return self._client_root(client_id) / f"{self._table_name(entry_id)}{suffix.lower()}"

    def _remove_stale_entry_files(
        self,
        client_root: Path,
        table_name: str,
        *,
        keep_path: Path | None = None,
    ) -> None:
        if not client_root.exists():
            return

        for candidate in client_root.glob(f"{table_name}.*"):
            if keep_path is not None and candidate == keep_path:
                continue
            candidate.unlink(missing_ok=True)

    def _remove_empty_directories(self, root: Path) -> None:
        current = root
        query_root = self._query_root()
        while current != query_root and current.exists():
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent

    def _reader_sql(
        self,
        local_path: Path,
        query_format: str,
        csv_delimiter: str,
        csv_has_header: bool,
    ) -> str:
        if query_format == "parquet":
            return f"read_parquet({sql_literal(local_path.as_posix())})"
        if query_format == "json":
            return f"read_json_auto({sql_literal(local_path.as_posix())})"

        options = [f"HEADER = {'TRUE' if csv_has_header else 'FALSE'}"]
        normalized_delimiter = str(csv_delimiter or "").strip()
        if normalized_delimiter:
            options.append(f"DELIM = {sql_literal(normalized_delimiter)}")
        return f"read_csv_auto({sql_literal(local_path.as_posix())}, {', '.join(options)})"

    def _relation_fields(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        schema_name: str,
        table_name: str,
    ) -> list[SourceField]:
        rows = conn.execute(
            """
            SELECT
                column_name,
                COALESCE(NULLIF(UPPER(data_type), ''), NULLIF(UPPER(udt_name), ''), 'UNKNOWN')
            FROM information_schema.columns
            WHERE table_catalog NOT IN ('pg_oltp', 'pg_olap')
              AND table_schema = ?
              AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema_name, table_name],
        ).fetchall()
        return [SourceField(name=column_name, data_type=data_type) for column_name, data_type in rows]

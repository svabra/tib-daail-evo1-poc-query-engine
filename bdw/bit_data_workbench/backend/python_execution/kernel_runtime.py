from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import duckdb

from ...config import Settings
from ..runtime_connections import create_duckdb_worker_connection


@dataclass(slots=True)
class PythonSourceHandle:
    runtime: "PythonKernelRuntime"
    relation_entry: dict[str, Any]

    @property
    def relation(self) -> str:
        return str(self.relation_entry.get("relation") or "").strip()

    @property
    def logical_relation(self) -> str:
        return str(self.relation_entry.get("logicalRelation") or "").strip()

    @property
    def fields(self) -> list[dict[str, Any]]:
        return list(self.relation_entry.get("fields") or [])

    def df(self, limit: int | None = None):
        relation = self.relation
        if not relation:
            raise KeyError("The selected source does not expose a queryable relation.")
        sql = f"SELECT * FROM {relation}"
        if isinstance(limit, int) and limit > 0:
            sql = f"{sql} LIMIT {limit}"
        return self.runtime.sql(sql)

    def __repr__(self) -> str:
        relation = self.logical_relation or self.relation or "<unknown>"
        return f"PythonSourceHandle(relation={relation!r})"


class PythonKernelRuntime:
    def __init__(self) -> None:
        self._settings = Settings.from_env()
        self._connection = self._create_connection()
        self._selected_sources: list[dict[str, Any]] = []
        self._relation_entries: list[dict[str, Any]] = []
        self._relation_index: dict[str, dict[str, Any]] = {}
        self._local_relation_map: dict[str, str] = {}
        self.pd = self._import_pandas()

    def _create_connection(self):
        try:
            return create_duckdb_worker_connection(self._settings)
        except duckdb.IOException as exc:
            message = str(exc).lower()
            if "cannot open file" not in message and "being used by another process" not in message:
                raise
            return create_duckdb_worker_connection(
                self._settings,
                database_path=":memory:",
            )

    def _import_pandas(self):
        import pandas as pd

        return pd

    def close(self) -> None:
        self._connection.close()

    def configure(self, context: dict[str, Any]) -> None:
        self._selected_sources = [dict(item) for item in context.get("selectedSources", []) or []]
        self._relation_entries = [dict(item) for item in context.get("relations", []) or []]
        self._local_relation_map = {
            str(key): str(value)
            for key, value in (context.get("localRelationMap", {}) or {}).items()
            if str(key).strip() and str(value).strip()
        }
        self._relation_index = {}

        for relation_entry in self._relation_entries:
            aliases = {
                str(relation_entry.get("relation") or "").strip(),
                str(relation_entry.get("logicalRelation") or "").strip(),
                str(relation_entry.get("name") or "").strip(),
                str(relation_entry.get("displayName") or "").strip(),
            }
            aliases.update(
                str(alias).strip()
                for alias in relation_entry.get("aliases", []) or []
                if str(alias).strip()
            )
            for alias in aliases:
                if not alias:
                    continue
                self._relation_index.setdefault(alias.lower(), relation_entry)

    def rewrite_query(self, query: str) -> str:
        rewritten = str(query or "")
        for logical_relation, physical_relation in self._local_relation_map.items():
            rewritten = rewritten.replace(logical_relation, physical_relation)
        return rewritten

    def sql(self, query: str):
        rewritten_query = self.rewrite_query(query)
        cursor = self._connection.execute(rewritten_query)
        if not cursor.description:
            return self.pd.DataFrame()
        return cursor.fetch_df()

    def source(self, name_or_relation: str) -> PythonSourceHandle:
        normalized_name = str(name_or_relation or "").strip()
        if not normalized_name:
            raise KeyError("Provide a source relation or relation alias.")

        direct_match = self._relation_index.get(normalized_name.lower())
        if direct_match is not None:
            return PythonSourceHandle(runtime=self, relation_entry=direct_match)

        physical_relation = self._local_relation_map.get(normalized_name)
        if physical_relation:
            return PythonSourceHandle(
                runtime=self,
                relation_entry={
                    "name": normalized_name,
                    "displayName": normalized_name,
                    "logicalRelation": normalized_name,
                    "relation": physical_relation,
                    "fields": [],
                    "aliases": [normalized_name, physical_relation],
                },
            )

        raise KeyError(f"Unknown source relation: {normalized_name}")

    def selected_sources_namespace(self) -> SimpleNamespace:
        return SimpleNamespace(selected=[dict(item) for item in self._selected_sources])


def apply_execution_context(namespace: dict[str, Any], context: dict[str, Any]) -> None:
    runtime = namespace.get("_bdw_kernel_runtime")
    if not isinstance(runtime, PythonKernelRuntime):
        runtime = PythonKernelRuntime()
        namespace["_bdw_kernel_runtime"] = runtime

    runtime.configure(context)
    namespace["pd"] = runtime.pd
    namespace["sql"] = runtime.sql
    namespace["source"] = runtime.source
    namespace["sources"] = runtime.selected_sources_namespace()

    try:
        ipython = namespace.get("get_ipython", lambda: None)()
        if ipython is not None:
            ipython.run_line_magic("matplotlib", "inline")
    except Exception:
        pass

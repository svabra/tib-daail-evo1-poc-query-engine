from __future__ import annotations

from typing import Callable

from ....models import SourceConnectionStatus, SourceField, SourceObject
from ...sql_utils import sql_identifier
from ...source_discovery import SqlDiscoveredRelation
from ..ddl import SourceDdlDownload, ddl_filename, safe_sql_type


class PostgresExplorerManager:
    def __init__(
        self,
        *,
        source_id: str,
        source_label: str,
        database: str | None,
        connection_factory: Callable[[str | None], object],
    ) -> None:
        self._source_id = source_id
        self._source_label = source_label
        self._database = database
        self._connection_factory = connection_factory
        self._health_connection: object | None = None

    def close_connection(self) -> None:
        connection = self._health_connection
        self._health_connection = None
        if connection is None:
            return
        try:
            connection.close()
        except Exception:
            pass

    def source_snapshot(self) -> tuple[SourceConnectionStatus, list[SqlDiscoveredRelation]]:
        if not self._database:
            return (
                SourceConnectionStatus(
                    source_id=self._source_id,
                    state="disconnected",
                    label="Disconnected",
                    detail=f"{self._source_label} is not configured.",
                ),
                [],
            )

        if self._health_connection is not None:
            try:
                relations = self._fetch_relations(self._health_connection)
                return self._connected_snapshot(relations)
            except Exception:
                self.close_connection()

        connection = None
        try:
            connection = self._connection_factory(self._database)
            relations = self._fetch_relations(connection)
            self._health_connection = connection
            return self._connected_snapshot(relations)
        except Exception as exc:
            try:
                if connection is not None:
                    connection.close()
            except Exception:
                pass
            self.close_connection()
            return (
                SourceConnectionStatus(
                    source_id=self._source_id,
                    state="disconnected",
                    label="Disconnected",
                    detail=f"{self._source_label} connection failed: {exc}",
                ),
                [],
            )

    def catalog_objects(self) -> dict[str, list[SourceObject]]:
        _status, relations = self.source_snapshot()
        grouped: dict[str, list[SourceObject]] = {}
        for relation in relations:
            grouped.setdefault(relation.schema_name, []).append(
                SourceObject(
                    name=relation.relation_name,
                    kind=relation.relation_kind,
                    relation=(
                        f"{self._source_id}.{relation.schema_name}.{relation.relation_name}"
                    ),
                )
            )
        for objects in grouped.values():
            objects.sort(key=lambda item: item.name.lower())
        return grouped

    def relation_fields(self, relation: str) -> list[SourceField]:
        normalized_relation = str(relation or "").strip()
        parts = [part.strip() for part in normalized_relation.split(".") if part.strip()]
        if len(parts) != 3 or parts[0] != self._source_id:
            raise KeyError(f"Unsupported source object relation: {relation}")

        status, _relations = self.source_snapshot()
        if status.state != "connected":
            raise KeyError(
                f"Source object is unavailable because {self._source_id} is disconnected."
            )

        if self._health_connection is None:
            raise KeyError(
                f"Source object is unavailable because {self._source_id} is disconnected."
            )

        schema_name = parts[1]
        object_name = parts[2]
        with self._health_connection.cursor() as cursor:
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

        if not rows:
            raise KeyError(f"Unknown source object: {relation}")

        return [SourceField(name=column_name, data_type=data_type) for column_name, data_type in rows]

    def relation_ddl(self, relation: str, *, object_name: str = "") -> SourceDdlDownload:
        normalized_relation = str(relation or "").strip()
        parts = [part.strip() for part in normalized_relation.split(".") if part.strip()]
        if len(parts) != 3 or parts[0] != self._source_id:
            raise KeyError(f"Unsupported source object relation: {relation}")

        status, _relations = self.source_snapshot()
        if status.state != "connected" or self._health_connection is None:
            raise KeyError(
                f"Source object is unavailable because {self._source_id} is disconnected."
            )

        schema_name = parts[1]
        object_name = parts[2]
        with self._health_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    relation.oid,
                    relation.relkind,
                    pg_get_viewdef(relation.oid, true) AS view_definition
                FROM pg_class AS relation
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = %s
                  AND relation.relname = %s
                  AND relation.relkind IN ('r', 'p', 'v', 'm')
                """,
                [schema_name, object_name],
            )
            relation_row = cursor.fetchone()

        if relation_row is None:
            raise KeyError(f"Unknown source object: {relation}")

        relation_oid, relation_kind, view_definition = relation_row
        filename = ddl_filename(object_name)
        if str(relation_kind or "") in {"v", "m"}:
            return SourceDdlDownload(
                ddl=_postgres_view_ddl(
                    schema_name=schema_name,
                    object_name=object_name,
                    relation_kind=str(relation_kind or ""),
                    view_definition=str(view_definition or "").strip(),
                    source_relation=normalized_relation,
                ),
                filename=filename,
            )

        with self._health_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    attribute.attname AS column_name,
                    format_type(attribute.atttypid, attribute.atttypmod) AS data_type,
                    attribute.attnotnull AS not_null,
                    pg_get_expr(default_value.adbin, default_value.adrelid) AS default_value
                FROM pg_attribute AS attribute
                LEFT JOIN pg_attrdef AS default_value
                  ON default_value.adrelid = attribute.attrelid
                 AND default_value.adnum = attribute.attnum
                WHERE attribute.attrelid = %s
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
                ORDER BY attribute.attnum
                """,
                [relation_oid],
            )
            columns = cursor.fetchall()
            cursor.execute(
                """
                SELECT
                    constraint_row.conname AS constraint_name,
                    pg_get_constraintdef(constraint_row.oid, true) AS constraint_definition
                FROM pg_constraint AS constraint_row
                WHERE constraint_row.conrelid = %s
                  AND constraint_row.contype IN ('p', 'u', 'f', 'c')
                ORDER BY
                    CASE constraint_row.contype
                        WHEN 'p' THEN 0
                        WHEN 'u' THEN 1
                        WHEN 'f' THEN 2
                        ELSE 3
                    END,
                    constraint_row.conname
                """,
                [relation_oid],
            )
            constraints = cursor.fetchall()

        if not columns:
            raise KeyError(f"Unknown source object: {relation}")

        return SourceDdlDownload(
            ddl=_postgres_table_ddl(
                schema_name=schema_name,
                object_name=object_name,
                columns=columns,
                constraints=constraints,
                source_relation=normalized_relation,
            ),
            filename=filename,
        )

    def drop_schema_objects(self, schema_name: str) -> int:
        status, relations = self.source_snapshot()
        if status.state != "connected":
            raise ValueError(f"{self._source_id} is disconnected; cleanup cannot proceed.")
        if self._health_connection is None:
            raise ValueError(f"{self._source_id} is disconnected; cleanup cannot proceed.")

        scoped_relations = [relation for relation in relations if relation.schema_name == schema_name]
        for relation in scoped_relations:
            drop_kind = (
                "MATERIALIZED VIEW"
                if relation.relation_kind == "materialized view"
                else relation.relation_kind.upper()
            )
            with self._health_connection.cursor() as cursor:
                cursor.execute(
                    f"DROP {drop_kind} IF EXISTS \"{schema_name}\".\"{relation.relation_name}\" CASCADE"
                )
        return len(scoped_relations)

    def _connected_snapshot(
        self,
        relations: list[SqlDiscoveredRelation],
    ) -> tuple[SourceConnectionStatus, list[SqlDiscoveredRelation]]:
        return (
            SourceConnectionStatus(
                source_id=self._source_id,
                state="connected",
                label="Connected",
                detail=f"{self._source_label} is connected.",
            ),
            relations,
        )

    def _fetch_relations(self, connection) -> list[SqlDiscoveredRelation]:
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


def _postgres_table_ddl(
    *,
    schema_name: str,
    object_name: str,
    columns,
    constraints,
    source_relation: str,
) -> str:
    qualified_table = f"{sql_identifier(schema_name)}.{sql_identifier(object_name)}"
    definition_lines: list[str] = []
    for column_name, data_type, not_null, default_value in columns:
        column_sql = (
            f"{sql_identifier(str(column_name or '').strip())} "
            f"{safe_sql_type(str(data_type or 'TEXT'))}"
        )
        if default_value:
            column_sql += f" DEFAULT {default_value}"
        if bool(not_null):
            column_sql += " NOT NULL"
        definition_lines.append(column_sql)

    for constraint_name, constraint_definition in constraints:
        normalized_definition = str(constraint_definition or "").strip()
        if not normalized_definition:
            continue
        definition_lines.append(
            f"CONSTRAINT {sql_identifier(str(constraint_name or '').strip())} "
            f"{normalized_definition}"
        )

    lines = [
        "-- DDL generated from the PostgreSQL catalog.",
        f"-- Source: {source_relation}",
        f"CREATE TABLE {qualified_table} (",
    ]
    for index, definition_line in enumerate(definition_lines):
        suffix = "," if index < len(definition_lines) - 1 else ""
        lines.append(f"  {definition_line}{suffix}")
    lines.append(");")
    lines.append("")
    return "\n".join(lines)


def _postgres_view_ddl(
    *,
    schema_name: str,
    object_name: str,
    relation_kind: str,
    view_definition: str,
    source_relation: str,
) -> str:
    qualified_table = f"{sql_identifier(schema_name)}.{sql_identifier(object_name)}"
    create_kind = "MATERIALIZED VIEW" if relation_kind == "m" else "VIEW"
    normalized_definition = view_definition.strip().rstrip(";")
    if not normalized_definition:
        normalized_definition = "SELECT NULL AS placeholder"
    return "\n".join(
        [
            "-- DDL generated from the PostgreSQL catalog.",
            f"-- Source: {source_relation}",
            f"CREATE {create_kind} {qualified_table} AS",
            f"{normalized_definition};",
            "",
        ]
    )

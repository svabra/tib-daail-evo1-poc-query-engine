from __future__ import annotations

from typing import Callable

from ....models import SourceConnectionStatus, SourceField, SourceObject
from ...source_discovery import SqlDiscoveredRelation


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
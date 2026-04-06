from __future__ import annotations

from ....config import Settings
from ..base import DataSourcePlugin
from .explorer import PostgresExplorerManager


class PostgresDataSourcePlugin(DataSourcePlugin):
    def __init__(
        self,
        settings: Settings,
        *,
        source_id: str,
        source_label: str,
        target: str,
        connection_factory,
    ) -> None:
        self._settings = settings
        self._source_id = source_id
        self._source_label = source_label
        self._target = target
        database = (
            settings.pg_oltp_database if target == "oltp" else settings.pg_olap_database
        )
        self._explorer = PostgresExplorerManager(
            source_id=source_id,
            source_label=source_label,
            database=database,
            connection_factory=connection_factory,
        )

    @property
    def source_id(self) -> str:
        return self._source_id

    @property
    def source_label(self) -> str:
        return self._source_label

    @property
    def source_type(self) -> str:
        return "postgres"

    @property
    def target(self) -> str:
        return self._target

    def source_snapshot(self):
        return self._explorer.source_snapshot()

    def catalog_objects(self):
        return self._explorer.catalog_objects()

    def relation_fields(self, relation: str):
        return self._explorer.relation_fields(relation)

    def drop_schema_objects(self, schema_name: str) -> int:
        return self._explorer.drop_schema_objects(schema_name)

    def disconnect(self) -> None:
        self._explorer.close_connection()

    def close(self) -> None:
        self.disconnect()
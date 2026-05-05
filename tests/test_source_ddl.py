from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.router import download_source_object_ddl  # noqa: E402
from bit_data_workbench.backend.data_sources.ddl import (  # noqa: E402
    SourceDdlDownload,
    safe_sql_type,
    synthetic_source_ddl,
)
from bit_data_workbench.backend.data_sources.postgres.explorer import (  # noqa: E402
    PostgresExplorerManager,
)
from bit_data_workbench.models import SourceField  # noqa: E402


class SourceDdlHelperTests(unittest.TestCase):
    def test_synthetic_source_ddl_uses_inferred_types_and_safe_table_name(self) -> None:
        artifact = synthetic_source_ddl(
            fields=[
                SourceField("Record ID", "BIGINT"),
                SourceField("Assessed Amount CHF", "DECIMAL(18,2)"),
                SourceField("Unsafe", "TEXT); DROP TABLE public.orders; --"),
            ],
            relation="workspace.shared_finance.orders",
            object_name="orders.parquet",
            source_id="workspace.s3",
            source_path="s3://shared-finance/orders.parquet",
        )

        self.assertEqual(artifact.filename, "orders.sql")
        self.assertIn('CREATE TABLE "orders" (', artifact.ddl)
        self.assertIn('"Record ID" BIGINT', artifact.ddl)
        self.assertIn('"Assessed Amount CHF" DECIMAL(18,2)', artifact.ddl)
        self.assertIn('"Unsafe" TEXT', artifact.ddl)
        self.assertIn("-- Suggested DDL generated", artifact.ddl)
        self.assertIn("s3://shared-finance/orders.parquet", artifact.ddl)

    def test_safe_sql_type_rejects_statement_fragments(self) -> None:
        self.assertEqual(safe_sql_type("DOUBLE PRECISION"), "DOUBLE PRECISION")
        self.assertEqual(safe_sql_type("VARCHAR); DROP TABLE x; --"), "TEXT")


class FakePostgresCursor:
    def __init__(self) -> None:
        self._result: object = None

    def __enter__(self) -> "FakePostgresCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, parameters: list[object] | None = None) -> None:
        if "pg_get_viewdef" in sql:
            self._result = (42, "r", None)
            return
        if "FROM pg_class AS relation" in sql:
            self._result = [("public", "orders", "table")]
            return
        if "FROM pg_attribute" in sql:
            self._result = [
                ("id", "integer", True, None),
                ("amount_chf", "numeric(18,2)", False, "0"),
            ]
            return
        if "FROM pg_constraint" in sql:
            self._result = [("orders_pkey", "PRIMARY KEY (id)")]
            return
        raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    def fetchall(self):
        if isinstance(self._result, list):
            return self._result
        return []


class FakePostgresConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakePostgresCursor()

    def cursor(self) -> FakePostgresCursor:
        return self.cursor_instance


class PostgresExplorerDdlTests(unittest.TestCase):
    def test_relation_ddl_uses_postgres_catalog_columns_and_constraints(self) -> None:
        manager = PostgresExplorerManager(
            source_id="pg_oltp",
            source_label="PostgreSQL OLTP",
            database="oltp",
            connection_factory=lambda database: FakePostgresConnection(),
        )

        artifact = manager.relation_ddl("pg_oltp.public.orders")

        self.assertEqual(artifact.filename, "orders.sql")
        self.assertIn("-- DDL generated from the PostgreSQL catalog.", artifact.ddl)
        self.assertIn('CREATE TABLE "public"."orders" (', artifact.ddl)
        self.assertIn('"id" INTEGER NOT NULL,', artifact.ddl)
        self.assertIn('"amount_chf" NUMERIC(18,2) DEFAULT 0,', artifact.ddl)
        self.assertIn('CONSTRAINT "orders_pkey" PRIMARY KEY (id)', artifact.ddl)


class FakeDdlService:
    def source_object_ddl(self, **kwargs) -> SourceDdlDownload:
        self.kwargs = dict(kwargs)
        return SourceDdlDownload(
            ddl='CREATE TABLE "orders" ("id" BIGINT);\n',
            filename="orders.sql",
        )


class SourceDdlRouteTests(unittest.TestCase):
    def test_download_route_returns_sql_attachment(self) -> None:
        service = FakeDdlService()

        response = download_source_object_ddl(
            relation="pg_oltp.public.orders",
            source_id="pg_oltp",
            bucket="",
            key="",
            object_name="orders",
            file_format="",
            service=service,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.media_type, "application/sql; charset=utf-8")
        self.assertIn("filename*=UTF-8''orders.sql", response.headers["content-disposition"])
        self.assertEqual(response.body.decode("utf-8"), 'CREATE TABLE "orders" ("id" BIGINT);\n')
        self.assertEqual(
            json.dumps(service.kwargs, sort_keys=True),
            json.dumps(
                {
                    "bucket": "",
                    "file_format": "",
                    "key": "",
                    "object_name": "orders",
                    "relation": "pg_oltp.public.orders",
                    "source_id": "pg_oltp",
                },
                sort_keys=True,
            ),
        )


if __name__ == "__main__":
    unittest.main()

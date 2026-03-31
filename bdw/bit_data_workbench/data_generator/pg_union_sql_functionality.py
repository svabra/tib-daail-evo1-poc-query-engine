from __future__ import annotations

from .base import (
    DataGenerationCancelled,
    DataGenerator,
    DataGeneratorContext,
    DataGeneratorResult,
    estimated_rows_for_size,
    generated_name,
)
from .helpers import (
    CROSS_DATABASE_UNION_DATASET_COLUMNS,
    approximate_size_gb,
    cross_database_union_dataset_select,
    qualified_name,
)


class PgUnionSqlFunctionalityDataGenerator(DataGenerator):
    generator_id = "pg_union_sql_functionality_loader"
    title = "PostgreSQL OLTP + OLAP UNION Loader"
    description = (
        "Generates matching SQL-functionality reference data in PostgreSQL OLTP and PostgreSQL OLAP "
        "so cross-database UNION queries can be executed directly in notebooks."
    )
    target_kind = "postgres"
    tree_path = ("PoC Tests", "SQL Functionalities")
    default_size_gb = 0.25
    min_size_gb = 0.01
    max_size_gb = 64.0
    approximate_row_bytes = 176
    default_target_name = "pg_union_tax_reference"
    tags = ("postgres", "oltp", "olap", "sql", "union", "functionalities")

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        if not context.settings.pg_oltp_database or not context.settings.pg_olap_database:
            raise ValueError(
                "PG_OLTP_DATABASE and PG_OLAP_DATABASE must both be configured before running the UNION loader."
            )

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        oltp_rows = max(1, total_rows // 2)
        olap_rows = max(1, total_rows - oltp_rows)
        batch_rows = 100_000
        oltp_batch_count = max(1, (oltp_rows + batch_rows - 1) // batch_rows)
        olap_batch_count = max(1, (olap_rows + batch_rows - 1) // batch_rows)
        table_name = generated_name(self.default_target_name, context.job_id)
        oltp_relation_name = f"pg_oltp.public.{table_name}"
        olap_relation_name = f"pg_olap.public.{table_name}"
        oltp_relation = qualified_name("pg_oltp", "public", table_name)
        olap_relation = qualified_name("pg_olap", "public", table_name)
        connection = context.connect()

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing targets...",
                message=(
                    f"Creating UNION-ready PostgreSQL tables {oltp_relation_name} and {olap_relation_name}."
                ),
                target_name=table_name,
                target_relation=f"{oltp_relation_name} + {olap_relation_name}",
            )
            connection.execute(f"DROP TABLE IF EXISTS {oltp_relation}")
            connection.execute(f"DROP TABLE IF EXISTS {olap_relation}")
            connection.execute(f"CREATE TABLE {oltp_relation} ({', '.join(CROSS_DATABASE_UNION_DATASET_COLUMNS)})")
            connection.execute(f"CREATE TABLE {olap_relation} ({', '.join(CROSS_DATABASE_UNION_DATASET_COLUMNS)})")

            written_rows = 0
            for batch_index, start_row in enumerate(range(0, oltp_rows, batch_rows), start=1):
                context.raise_if_cancelled()
                end_row = min(oltp_rows, start_row + batch_rows)
                connection.execute(
                    f"INSERT INTO {oltp_relation} {cross_database_union_dataset_select(start_row, end_row, 'oltp')}"
                )
                written_rows += end_row - start_row
                context.report(
                    progress=written_rows / total_rows,
                    progress_label=f"Writing OLTP batch {batch_index} / {oltp_batch_count}",
                    message=(
                        f"Wrote {written_rows:,} total row(s) while preparing {oltp_relation_name} "
                        f"and {olap_relation_name}."
                    ),
                    target_name=table_name,
                    target_relation=f"{oltp_relation_name} + {olap_relation_name}",
                    generated_rows=written_rows,
                    generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                )

            for batch_index, start_row in enumerate(range(0, olap_rows, batch_rows), start=1):
                context.raise_if_cancelled()
                end_row = min(olap_rows, start_row + batch_rows)
                connection.execute(
                    f"INSERT INTO {olap_relation} {cross_database_union_dataset_select(start_row, end_row, 'olap')}"
                )
                written_rows += end_row - start_row
                context.report(
                    progress=written_rows / total_rows,
                    progress_label=f"Writing OLAP batch {batch_index} / {olap_batch_count}",
                    message=(
                        f"Wrote {written_rows:,} total row(s) while preparing {oltp_relation_name} "
                        f"and {olap_relation_name}."
                    ),
                    target_name=table_name,
                    target_relation=f"{oltp_relation_name} + {olap_relation_name}",
                    generated_rows=written_rows,
                    generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                )

            return DataGeneratorResult(
                target_name=table_name,
                target_relation=f"{oltp_relation_name} + {olap_relation_name}",
                generated_rows=written_rows,
                generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                message=(
                    f"Generated {written_rows:,} UNION-ready rows across {oltp_relation_name} "
                    f"and {olap_relation_name}."
                ),
            )
        except DataGenerationCancelled:
            self._drop_targets(connection=connection, oltp_relation=oltp_relation, olap_relation=olap_relation)
            context.report(
                message="Cancellation requested. Partial OLTP and OLAP output were removed.",
                target_relation="",
                generated_rows=0,
                generated_size_gb=0.0,
            )
            raise
        except Exception:
            self._drop_targets(connection=connection, oltp_relation=oltp_relation, olap_relation=olap_relation)
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        table_name = job.target_name.strip()
        if not table_name:
            raise ValueError("This UNION loader job has no target table name to clean.")

        oltp_relation_name = f"pg_oltp.public.{table_name}"
        olap_relation_name = f"pg_olap.public.{table_name}"
        oltp_relation = qualified_name("pg_oltp", "public", table_name)
        olap_relation = qualified_name("pg_olap", "public", table_name)
        connection = context.connect()

        try:
            context.report(message=f"Cleaning UNION test data from {oltp_relation_name} and {olap_relation_name}...")
            connection.execute(f"DELETE FROM {oltp_relation}")
            connection.execute(f"DELETE FROM {olap_relation}")
            return DataGeneratorResult(
                target_name=table_name,
                target_relation=f"{oltp_relation_name} + {olap_relation_name}",
                generated_rows=0,
                generated_size_gb=0.0,
                message=(
                    f"Deleted all rows from {oltp_relation_name} and {olap_relation_name}. "
                    "The table definitions were kept."
                ),
            )
        finally:
            connection.close()

    def _drop_targets(self, *, connection, oltp_relation: str, olap_relation: str) -> None:
        try:
            connection.execute(f"DROP TABLE IF EXISTS {oltp_relation}")
        except Exception:
            pass
        try:
            connection.execute(f"DROP TABLE IF EXISTS {olap_relation}")
        except Exception:
            pass


GENERATOR = PgUnionSqlFunctionalityDataGenerator()

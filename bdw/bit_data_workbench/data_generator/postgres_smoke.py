from __future__ import annotations

from .base import (
    DataGenerationCancelled,
    DataGenerator,
    DataGeneratorContext,
    DataGeneratorResult,
    estimated_rows_for_size,
    generation_target,
    generated_name,
    update_generation_target_status,
)
from .helpers import TAX_ASSESSMENT_DATASET_COLUMNS, approximate_size_gb, qualified_name, tax_assessment_dataset_select


class PostgresSmokeDataGenerator(DataGenerator):
    generator_id = "postgres_smoke_orders"
    title = "PostgreSQL OLAP Tax Assessment Loader"
    description = (
        "Generates a semantically meaningful federal-tax assessment dataset and writes it into "
        "PostgreSQL OLAP through the attached DuckDB integration."
    )
    target_kind = "postgres"
    tree_path = ("PoC Tests", "Smoke Tests")
    default_size_gb = 1.0
    min_size_gb = 0.01
    max_size_gb = 512.0
    approximate_row_bytes = 320
    default_target_name = "tax_assessment_olap_smoke"
    tags = ("postgres", "olap", "tax", "assessment", "smoke")

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        batch_rows = 100_000
        batch_count = max(1, (total_rows + batch_rows - 1) // batch_rows)
        table_name = generated_name(self.default_target_name, context.job_id)
        relation = qualified_name("pg_olap", "public", table_name)
        relation_name = f"pg_olap.public.{table_name}"
        written_targets = [
            generation_target(
                target_kind="postgres_table",
                label="PostgreSQL OLAP table",
                location=relation_name,
            )
        ]
        connection = context.connect()

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing target...",
                message=f"Creating PostgreSQL target table {table_name}.",
                target_name=table_name,
                target_relation=relation_name,
                written_targets=written_targets,
            )
            connection.execute(f"DROP TABLE IF EXISTS {relation}")
            connection.execute(f"CREATE TABLE {relation} ({', '.join(TAX_ASSESSMENT_DATASET_COLUMNS)})")

            inserted_rows = 0
            for batch_index, start_row in enumerate(range(0, total_rows, batch_rows), start=1):
                context.raise_if_cancelled()
                end_row = min(total_rows, start_row + batch_rows)
                connection.execute(f"INSERT INTO {relation} {tax_assessment_dataset_select(start_row, end_row)}")
                inserted_rows = end_row
                written_targets = update_generation_target_status(
                    written_targets,
                    relation_name,
                    status="written",
                )
                context.report(
                    progress=inserted_rows / total_rows,
                    progress_label=f"Writing batch {batch_index} / {batch_count}",
                    message=f"Inserted {inserted_rows:,} rows into {relation_name}.",
                    target_name=table_name,
                    target_relation=relation_name,
                    written_targets=written_targets,
                    generated_rows=inserted_rows,
                    generated_size_gb=approximate_size_gb(inserted_rows, self.approximate_row_bytes),
                )

            return DataGeneratorResult(
                target_name=table_name,
                target_relation=relation_name,
                written_targets=written_targets,
                generated_rows=inserted_rows,
                generated_size_gb=approximate_size_gb(inserted_rows, self.approximate_row_bytes),
                message=f"Generated {inserted_rows:,} rows in {relation_name}.",
            )
        except DataGenerationCancelled:
            connection.execute(f"DROP TABLE IF EXISTS {relation}")
            context.report(
                message="Cancellation requested. Partial PostgreSQL output was removed.",
                target_relation="",
                written_targets=[],
                generated_rows=0,
                generated_size_gb=0.0,
            )
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        relation = job.target_relation.strip()
        if not relation:
            raise ValueError("This PostgreSQL generation job has no target relation to clean.")

        connection = context.connect()
        try:
            context.report(message=f"Cleaning rows from {relation}...")
            connection.execute(f"DELETE FROM {relation}")
            return DataGeneratorResult(
                target_name=job.target_name,
                target_relation=relation,
                generated_rows=0,
                generated_size_gb=0.0,
                message=f"Deleted all rows from {relation}. The table definition was kept.",
            )
        finally:
            connection.close()


GENERATOR = PostgresSmokeDataGenerator()

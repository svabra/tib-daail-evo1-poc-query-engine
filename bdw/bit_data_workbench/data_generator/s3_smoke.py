from __future__ import annotations

from ..backend.s3_storage import (
    delete_s3_bucket,
    derived_s3_bucket_name,
    ensure_s3_bucket,
    parse_s3_url,
    remove_s3_bucket,
    s3_bucket_schema_name,
)
from .base import DataGenerationCancelled, DataGenerator, DataGeneratorContext, DataGeneratorResult, estimated_rows_for_size, generated_name
from .helpers import approximate_size_gb, qualified_name, sql_literal, vat_smoke_dataset_select

class S3SmokeDataGenerator(DataGenerator):
    generator_id = "s3_smoke_orders"
    title = "S3 VAT Smoke Loader"
    description = (
        "Generates a VAT filing smoke dataset for the Swiss Federal Tax Administration, writes "
        "partitioned Parquet files to S3, and registers a queryable DuckDB view over the result."
    )
    target_kind = "s3"
    tree_path = ("PoC Tests", "Smoke Tests")
    default_size_gb = 1.0
    min_size_gb = 0.01
    max_size_gb = 512.0
    approximate_row_bytes = 220
    default_target_name = "vat_smoke"
    tags = ("s3", "parquet", "vat", "tax", "smoke")

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return derived_s3_bucket_name(base_bucket, "s3-smoke")

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the S3 data generator.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        batch_rows = 150_000
        batch_count = max(1, (total_rows + batch_rows - 1) // batch_rows)
        view_name = generated_name(self.default_target_name, context.job_id)
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        schema_name = s3_bucket_schema_name(bucket_name)
        relation = qualified_name(schema_name, view_name)
        object_prefix = f"s3://{bucket_name}/generated/{view_name}"
        connection = context.connect()

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing S3 target...",
                message=f"Generating Parquet files in dedicated loader bucket {bucket_name} under {object_prefix}.",
                target_name=view_name,
                target_relation=f"{schema_name}.{view_name}",
                target_path=object_prefix,
            )
            ensure_s3_bucket(settings, bucket_name)
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(schema_name)}")
            delete_s3_bucket(settings, bucket_name)
            ensure_s3_bucket(settings, bucket_name)

            written_rows = 0
            for batch_index, start_row in enumerate(range(0, total_rows, batch_rows), start=1):
                context.raise_if_cancelled()
                end_row = min(total_rows, start_row + batch_rows)
                part_path = f"{object_prefix}/part-{batch_index:05d}.parquet"
                connection.execute(
                    "COPY ("
                    f"{vat_smoke_dataset_select(start_row, end_row)}"
                    f") TO {sql_literal(part_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                written_rows = end_row
                context.report(
                    progress=written_rows / total_rows,
                    progress_label=f"Writing batch {batch_index} / {batch_count}",
                    message=f"Wrote {written_rows:,} rows to {object_prefix}.",
                    target_name=view_name,
                    target_relation=f"{schema_name}.{view_name}",
                    target_path=object_prefix,
                    generated_rows=written_rows,
                    generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                )

            connection.execute(
                f"CREATE OR REPLACE VIEW {relation} AS "
                f"SELECT * FROM read_parquet({sql_literal(f'{object_prefix}/*.parquet')})"
            )

            return DataGeneratorResult(
                target_name=view_name,
                target_relation=f"{schema_name}.{view_name}",
                target_path=object_prefix,
                generated_rows=written_rows,
                generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                message=f"Generated {written_rows:,} rows in {schema_name}.{view_name}.",
            )
        except DataGenerationCancelled:
            self._cleanup_partial_output(connection=connection, relation=relation, target_path=object_prefix, context=context)
            context.report(
                message="Cancellation requested. Partial S3 output and the dedicated loader bucket were removed.",
                target_relation="",
                target_path="",
                generated_rows=0,
                generated_size_gb=0.0,
            )
            raise
        except Exception:
            self._cleanup_partial_output(connection=connection, relation=relation, target_path=object_prefix, context=context)
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        if not job.target_path:
            raise ValueError("This S3 generation job has no target path to clean.")

        bucket, _prefix = parse_s3_url(job.target_path)
        connection = context.connect()

        try:
            context.report(message=f"Cleaning dedicated loader bucket {bucket} for {job.target_name}...")
            deleted_objects = delete_s3_bucket(context.settings, bucket)
            if job.target_relation:
                schema_name, _, view_name = job.target_relation.partition(".")
                if schema_name and view_name:
                    connection.execute(f"DROP VIEW IF EXISTS {qualified_name(schema_name, view_name)}")
            bucket_deleted = remove_s3_bucket(context.settings, bucket)
            return DataGeneratorResult(
                target_name=job.target_name,
                generated_rows=0,
                generated_size_gb=0.0,
                message=(
                    f"Deleted {deleted_objects:,} generated object(s) from s3://{bucket}, "
                    f"{'removed the bucket' if bucket_deleted else 'left the empty bucket in place'}, "
                    "and removed the workspace view."
                ),
            )
        finally:
            connection.close()

    def _cleanup_partial_output(
        self,
        *,
        connection,
        relation: str,
        target_path: str,
        context: DataGeneratorContext,
    ) -> None:
        try:
            connection.execute(f"DROP VIEW IF EXISTS {relation}")
        except Exception:
            pass

        try:
            bucket, _prefix = parse_s3_url(target_path)
        except Exception:
            return

        try:
            delete_s3_bucket(context.settings, bucket)
        except Exception:
            pass
        try:
            remove_s3_bucket(context.settings, bucket)
        except Exception:
            pass


GENERATOR = S3SmokeDataGenerator()

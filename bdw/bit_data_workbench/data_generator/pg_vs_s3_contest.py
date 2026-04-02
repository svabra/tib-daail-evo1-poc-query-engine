from __future__ import annotations

from ..backend.s3_storage import (
    delete_s3_bucket,
    derived_s3_bucket_name,
    ensure_s3_bucket,
    parse_s3_url,
    remove_s3_bucket,
    s3_bucket_schema_name,
)
from .base import (
    DataGenerationCancelled,
    DataGenerator,
    DataGeneratorContext,
    DataGeneratorResult,
    estimated_rows_for_size,
    generated_name,
)
from .helpers import (
    TAX_ASSESSMENT_DATASET_COLUMNS,
    approximate_size_gb,
    qualified_name,
    tax_assessment_dataset_select,
    sql_literal,
)


class PgVsS3ContestDataGenerator(DataGenerator):
    generator_id = "pg_vs_s3_contest_loader"
    title = "PG vs S3 Contest Loader"
    description = (
        "Generates one deterministic federal-tax assessment dataset and writes the exact same "
        "records into PostgreSQL OLTP and into S3-backed Parquet, so both targets can be "
        "benchmarked against each other."
    )
    target_kind = "contest"
    tree_path = ("PoC Tests", "Performance Evaluation")
    default_size_gb = 1.0
    min_size_gb = 0.01
    max_size_gb = 128.0
    approximate_row_bytes = 320
    default_target_name = "tax_assessment_pg_vs_s3"
    tags = ("postgres", "s3", "oltp", "contest", "tax", "assessment")

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return derived_s3_bucket_name(base_bucket, "pg-vs-s3-contest")

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the PG vs S3 contest loader.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        batch_rows = 100_000
        batch_count = max(1, (total_rows + batch_rows - 1) // batch_rows)
        target_name = generated_name(self.default_target_name, context.job_id)
        postgres_relation_name = f"pg_oltp.public.{target_name}"
        postgres_relation = qualified_name("pg_oltp", "public", target_name)
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        s3_schema = s3_bucket_schema_name(bucket_name)
        s3_relation_name = f"{s3_schema}.{target_name}"
        s3_relation = qualified_name(s3_schema, target_name)
        object_prefix = f"s3://{bucket_name}/generated/{target_name}"
        connection = context.connect()

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing targets...",
                message=(
                    f"Creating paired targets {postgres_relation_name} and {s3_relation_name} "
                    f"with S3 bucket {bucket_name} from the same generated records."
                ),
                target_name=target_name,
                target_relation=postgres_relation_name,
                target_path=object_prefix,
            )
            ensure_s3_bucket(settings, bucket_name)
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(s3_schema)}")
            connection.execute(f"DROP TABLE IF EXISTS {postgres_relation}")
            connection.execute(f"CREATE TABLE {postgres_relation} ({', '.join(TAX_ASSESSMENT_DATASET_COLUMNS)})")
            delete_s3_bucket(settings, bucket_name)

            written_rows = 0
            for batch_index, start_row in enumerate(range(0, total_rows, batch_rows), start=1):
                context.raise_if_cancelled()
                end_row = min(total_rows, start_row + batch_rows)
                select_sql = tax_assessment_dataset_select(start_row, end_row)
                parquet_path = f"{object_prefix}/part-{batch_index:05d}.parquet"

                connection.execute(f"INSERT INTO {postgres_relation} {select_sql}")
                connection.execute(
                    "COPY ("
                    f"{select_sql}"
                    f") TO {sql_literal(parquet_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
                )

                written_rows = end_row
                context.report(
                    progress=written_rows / total_rows,
                    progress_label=f"Writing identical batch {batch_index} / {batch_count}",
                    message=(
                        f"Wrote {written_rows:,} mirrored row(s) into {postgres_relation_name} "
                        f"and {s3_relation_name}."
                    ),
                    target_name=target_name,
                    target_relation=postgres_relation_name,
                    target_path=object_prefix,
                    generated_rows=written_rows,
                    generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                )

            connection.execute(
                f"CREATE OR REPLACE VIEW {s3_relation} AS "
                f"SELECT * FROM read_parquet({sql_literal(f'{object_prefix}/*.parquet')})"
            )

            return DataGeneratorResult(
                target_name=target_name,
                target_relation=postgres_relation_name,
                target_path=object_prefix,
                generated_rows=written_rows,
                generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                message=(
                    f"Generated {written_rows:,} identical rows in {postgres_relation_name} "
                    f"and {s3_relation_name}."
                ),
            )
        except DataGenerationCancelled:
            self._cleanup_partial_output(
                connection=connection,
                postgres_relation=postgres_relation,
                s3_relation=s3_relation,
                target_path=object_prefix,
                context=context,
            )
            context.report(
                message="Cancellation requested. Partial PostgreSQL and S3 output were removed.",
                target_relation="",
                target_path="",
                generated_rows=0,
                generated_size_gb=0.0,
            )
            raise
        except Exception:
            self._cleanup_partial_output(
                connection=connection,
                postgres_relation=postgres_relation,
                s3_relation=s3_relation,
                target_path=object_prefix,
                context=context,
            )
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        relation = job.target_relation.strip()
        target_name = job.target_name.strip()
        target_path = job.target_path.strip()
        if not relation or not target_name or not target_path:
            raise ValueError("This contest generation job does not have both PostgreSQL and S3 targets to clean.")

        bucket, _prefix = parse_s3_url(target_path)
        s3_relation = qualified_name(s3_bucket_schema_name(bucket), target_name)
        connection = context.connect()

        try:
            context.report(message=f"Cleaning mirrored PostgreSQL data and dedicated S3 bucket {bucket} for {target_name}...")
            connection.execute(f"DELETE FROM {relation}")
            connection.execute(f"DROP VIEW IF EXISTS {s3_relation}")
            deleted_objects = delete_s3_bucket(context.settings, bucket)
            bucket_deleted = remove_s3_bucket(context.settings, bucket)
            return DataGeneratorResult(
                target_name=target_name,
                target_relation=relation,
                generated_rows=0,
                generated_size_gb=0.0,
                message=(
                    f"Deleted all rows from {relation}, removed the S3 workspace view, deleted "
                    f"{deleted_objects:,} object(s) from s3://{bucket}, and "
                    f"{'removed the bucket' if bucket_deleted else 'left the empty bucket in place'}."
                ),
            )
        finally:
            connection.close()

    def _cleanup_partial_output(
        self,
        *,
        connection,
        postgres_relation: str,
        s3_relation: str,
        target_path: str,
        context: DataGeneratorContext,
    ) -> None:
        try:
            connection.execute(f"DROP TABLE IF EXISTS {postgres_relation}")
        except Exception:
            pass
        try:
            connection.execute(f"DROP VIEW IF EXISTS {s3_relation}")
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


GENERATOR = PgVsS3ContestDataGenerator()

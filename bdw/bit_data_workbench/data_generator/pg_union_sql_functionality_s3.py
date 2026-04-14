from __future__ import annotations

import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..backend.s3_storage import (
    delete_s3_bucket,
    duckdb_scan_query,
    derived_s3_bucket_name,
    ensure_s3_bucket,
    parse_s3_url,
    remove_s3_bucket,
    s3_client,
    s3_bucket_schema_name,
    upload_s3_file,
)
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
from .helpers import (
    CROSS_DATABASE_UNION_DATASET_COLUMNS,
    approximate_size_gb,
    cross_database_union_dataset_select,
    qualified_name,
    sql_literal,
)


class PgUnionSqlFunctionalityS3DataGenerator(DataGenerator):
    generator_id = "pg_union_sql_functionality_s3_loader"
    title = "PostgreSQL OLTP + S3 UNION Loader"
    description = (
        "Generates matching SQL-functionality reference data in PostgreSQL OLTP and in S3-backed "
        "Parquet so cross-source UNION queries can be executed through DuckDB."
    )
    target_kind = "contest"
    tree_path = ("PoC Tests", "SQL Functionalities")
    default_size_gb = 0.25
    min_size_gb = 0.01
    max_size_gb = 64.0
    approximate_row_bytes = 176
    default_target_name = "pg_union_tax_reference_s3"
    tags = ("postgres", "s3", "oltp", "sql", "union", "functionalities")

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return derived_s3_bucket_name(base_bucket, "pg-union-s3-functionality")

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.pg_oltp_database:
            raise ValueError("PG_OLTP_DATABASE must be configured before running the OLTP + S3 UNION loader.")
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the OLTP + S3 UNION loader.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        oltp_rows = max(1, total_rows // 2)
        s3_rows = max(1, total_rows - oltp_rows)
        oltp_batch_rows = 100_000
        s3_batch_rows = 100_000
        oltp_batch_count = max(1, (oltp_rows + oltp_batch_rows - 1) // oltp_batch_rows)
        s3_batch_count = max(1, (s3_rows + s3_batch_rows - 1) // s3_batch_rows)
        target_name = generated_name(self.default_target_name, context.job_id)
        postgres_relation_name = f"pg_oltp.public.{target_name}"
        postgres_relation = qualified_name("pg_oltp", "public", target_name)
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        s3_schema = s3_bucket_schema_name(bucket_name)
        s3_relation_name = f"{s3_schema}.{target_name}"
        s3_relation = qualified_name(s3_schema, target_name)
        object_prefix = f"s3://{bucket_name}/generated/{target_name}"
        object_key_prefix = f"generated/{target_name}"
        connection = context.connect()
        s3_upload_client = s3_client(settings)
        written_targets = [
            generation_target(
                target_kind="postgres_table",
                label="PostgreSQL OLTP table",
                location=postgres_relation_name,
            ),
            generation_target(
                target_kind="s3_prefix",
                label="Dedicated S3 Parquet path",
                location=object_prefix,
            ),
        ]

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing targets...",
                message=(
                    f"Creating UNION-ready PostgreSQL table {postgres_relation_name} and S3 workspace view "
                    f"{s3_relation_name} using the same column structure."
                ),
                target_name=target_name,
                target_relation=postgres_relation_name,
                target_path=object_prefix,
                written_targets=written_targets,
            )
            ensure_s3_bucket(settings, bucket_name)
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(s3_schema)}")
            connection.execute(f"DROP TABLE IF EXISTS {postgres_relation}")
            connection.execute(f"DROP VIEW IF EXISTS {s3_relation}")
            connection.execute(f"CREATE TABLE {postgres_relation} ({', '.join(CROSS_DATABASE_UNION_DATASET_COLUMNS)})")
            delete_s3_bucket(settings, bucket_name)
            ensure_s3_bucket(settings, bucket_name)

            written_rows = 0
            with TemporaryDirectory(prefix=f"bdw-{self.generator_id}-{context.job_id[:8]}-") as temp_dir:
                temp_dir_path = Path(temp_dir)

                for batch_index, start_row in enumerate(range(0, oltp_rows, oltp_batch_rows), start=1):
                    context.raise_if_cancelled()
                    end_row = min(oltp_rows, start_row + oltp_batch_rows)
                    connection.execute(
                        f"INSERT INTO {postgres_relation} "
                        f"{cross_database_union_dataset_select(start_row, end_row, 'oltp')}"
                    )
                    written_rows += end_row - start_row
                    written_targets = update_generation_target_status(
                        written_targets,
                        postgres_relation_name,
                        status="written",
                    )
                    context.report(
                        progress=written_rows / total_rows,
                        progress_label=f"Writing OLTP batch {batch_index} / {oltp_batch_count}",
                        message=(
                            f"Wrote {written_rows:,} total row(s) while preparing {postgres_relation_name} "
                            f"and {s3_relation_name}."
                        ),
                        target_name=target_name,
                        target_relation=postgres_relation_name,
                        target_path=object_prefix,
                        written_targets=written_targets,
                        generated_rows=written_rows,
                        generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                    )

                s3_row_offset = oltp_rows
                for batch_index, start_row in enumerate(range(0, s3_rows, s3_batch_rows), start=1):
                    context.raise_if_cancelled()
                    end_row = min(s3_rows, start_row + s3_batch_rows)
                    select_sql = cross_database_union_dataset_select(
                        s3_row_offset + start_row,
                        s3_row_offset + end_row,
                        "s3",
                    )
                    object_key = f"{object_key_prefix}/part-{batch_index:05d}.parquet"
                    local_parquet_path = temp_dir_path / f"part-{batch_index:05d}.parquet"
                    connection.execute(
                        "COPY ("
                        f"{select_sql}"
                        f") TO {sql_literal(local_parquet_path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    upload_s3_file(
                        s3_upload_client,
                        local_path=local_parquet_path,
                        bucket=bucket_name,
                        key=object_key,
                    )
                    local_parquet_path.unlink(missing_ok=True)
                    written_rows += end_row - start_row
                    written_targets = update_generation_target_status(
                        written_targets,
                        object_prefix,
                        status="written",
                    )
                    context.report(
                        progress=written_rows / total_rows,
                        progress_label=f"Writing S3 batch {batch_index} / {s3_batch_count}",
                        message=(
                            f"Wrote {written_rows:,} total row(s) while preparing {postgres_relation_name} "
                            f"and {s3_relation_name}."
                        ),
                        target_name=target_name,
                        target_relation=postgres_relation_name,
                        target_path=object_prefix,
                        written_targets=written_targets,
                        generated_rows=written_rows,
                        generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                    )

            s3_query = duckdb_scan_query("parquet", [f"{object_prefix}/*.parquet"])
            self._ensure_s3_view(
                connection=connection,
                s3_relation=s3_relation,
                schema_name=s3_schema,
                relation_name=target_name,
                s3_query=s3_query,
            )

            return DataGeneratorResult(
                target_name=target_name,
                target_relation=postgres_relation_name,
                target_path=object_prefix,
                written_targets=written_targets,
                generated_rows=written_rows,
                generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                message=(
                    f"Generated {written_rows:,} UNION-ready rows across {postgres_relation_name} "
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
                written_targets=[],
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
            raise ValueError("This UNION loader job does not have both PostgreSQL and S3 targets to clean.")

        bucket, _prefix = parse_s3_url(target_path)
        s3_relation = qualified_name(s3_bucket_schema_name(bucket), target_name)
        connection = context.connect()

        try:
            context.report(
                message=f"Cleaning UNION test data from {relation} and dedicated S3 bucket {bucket}..."
            )
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

    def _ensure_s3_view(
        self,
        *,
        connection,
        s3_relation: str,
        schema_name: str,
        relation_name: str,
        s3_query: str,
    ) -> None:
        try:
            connection.execute(f"CREATE OR REPLACE VIEW {s3_relation} AS {s3_query}")
        except Exception:
            if not self._wait_for_relation(connection, schema_name=schema_name, relation_name=relation_name):
                raise
        else:
            self._wait_for_relation(connection, schema_name=schema_name, relation_name=relation_name)

    def _wait_for_relation(
        self,
        connection,
        *,
        schema_name: str,
        relation_name: str,
        timeout_seconds: float = 8.0,
    ) -> bool:
        deadline = time.monotonic() + max(0.1, timeout_seconds)
        while time.monotonic() < deadline:
            try:
                row = connection.execute(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = ?
                      AND table_name = ?
                    LIMIT 1
                    """,
                    [schema_name, relation_name],
                ).fetchone()
            except Exception:
                row = None

            if row:
                return True
            time.sleep(0.2)
        return False


GENERATOR = PgUnionSqlFunctionalityS3DataGenerator()

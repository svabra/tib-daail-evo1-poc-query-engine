from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from ..backend.s3_storage import (
    delete_s3_bucket,
    duckdb_scan_query,
    ensure_s3_bucket,
    parse_s3_url,
    remove_s3_bucket,
    s3_bucket_schema_name,
    s3_client,
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
from .helpers import approximate_size_gb, loader_tree_bucket_name, qualified_name, sql_literal


RESULT_SET_STORAGE_SAMPLE_TREE_PATH = ("PoC Tests", "General Functionalities")
RESULT_SET_STORAGE_SAMPLE_BUCKET = loader_tree_bucket_name(
    (*RESULT_SET_STORAGE_SAMPLE_TREE_PATH, "result-set-storage"),
    "result-set-storage",
)
RESULT_SET_STORAGE_SAMPLE_SOURCE_NAME = "result_set_storage_orders"
RESULT_SET_STORAGE_SAMPLE_SOURCE_PREFIX = (
    f"s3://{RESULT_SET_STORAGE_SAMPLE_BUCKET}/generated/{RESULT_SET_STORAGE_SAMPLE_SOURCE_NAME}"
)
RESULT_SET_STORAGE_SAMPLE_RESULT_KEY = "result-sets/canton_quarter_order_summary.parquet"
RESULT_SET_STORAGE_SAMPLE_RESULT_PATH = (
    f"s3://{RESULT_SET_STORAGE_SAMPLE_BUCKET}/{RESULT_SET_STORAGE_SAMPLE_RESULT_KEY}"
)
RESULT_SET_STORAGE_SAMPLE_SCHEMA = s3_bucket_schema_name(RESULT_SET_STORAGE_SAMPLE_BUCKET)

RESULT_SET_STORAGE_ORDER_COLUMNS = (
    "order_id BIGINT",
    "taxpayer_uid VARCHAR",
    "canton_code VARCHAR",
    "order_channel VARCHAR",
    "order_status VARCHAR",
    "order_date DATE",
    "net_amount_chf DECIMAL(18,2)",
    "vat_amount_chf DECIMAL(18,2)",
    "gross_amount_chf DECIMAL(18,2)",
    "needs_review BOOLEAN",
    "updated_at TIMESTAMP",
)


def result_set_storage_orders_select(start_row: int, end_row: int) -> str:
    if end_row <= start_row:
        return (
            "SELECT "
            "0::BIGINT AS order_id, "
            "''::VARCHAR AS taxpayer_uid, "
            "''::VARCHAR AS canton_code, "
            "''::VARCHAR AS order_channel, "
            "''::VARCHAR AS order_status, "
            "DATE '2025-01-01' AS order_date, "
            "0::DECIMAL(18,2) AS net_amount_chf, "
            "0::DECIMAL(18,2) AS vat_amount_chf, "
            "0::DECIMAL(18,2) AS gross_amount_chf, "
            "false::BOOLEAN AS needs_review, "
            "TIMESTAMP '2025-01-01 00:00:00' AS updated_at "
            "LIMIT 0"
        )

    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        valued AS (
            SELECT
                row_id,
                CASE row_id % 10
                    WHEN 0 THEN 'ZH'
                    WHEN 1 THEN 'BE'
                    WHEN 2 THEN 'GE'
                    WHEN 3 THEN 'VD'
                    WHEN 4 THEN 'AG'
                    WHEN 5 THEN 'SG'
                    WHEN 6 THEN 'TI'
                    WHEN 7 THEN 'BS'
                    WHEN 8 THEN 'LU'
                    ELSE 'FR'
                END AS canton_code,
                CASE row_id % 4
                    WHEN 0 THEN 'portal'
                    WHEN 1 THEN 'api'
                    WHEN 2 THEN 'advisor'
                    ELSE 'branch'
                END AS order_channel,
                CASE row_id % 6
                    WHEN 0 THEN 'posted'
                    WHEN 1 THEN 'posted'
                    WHEN 2 THEN 'reviewed'
                    WHEN 3 THEN 'reconciled'
                    WHEN 4 THEN 'correction_requested'
                    ELSE 'posted'
                END AS order_status,
                DATE '2025-01-01' + CAST((row_id * 5) % 540 AS INTEGER) AS order_date,
                ROUND(
                    950.0
                    + (((row_id * 7919) % 875000) / 100.0)
                    + ((row_id % 23) * 12.75),
                    2
                ) AS net_amount_raw,
                CASE row_id % 5
                    WHEN 0 THEN 0.081
                    WHEN 1 THEN 0.026
                    WHEN 2 THEN 0.038
                    ELSE 0.081
                END AS vat_rate
            FROM base
        )
        SELECT
            row_id + 1 AS order_id,
            'CHE-' || CAST(100000000 + (row_id % 900000000) AS VARCHAR) AS taxpayer_uid,
            canton_code,
            order_channel,
            order_status,
            order_date,
            CAST(net_amount_raw AS DECIMAL(18,2)) AS net_amount_chf,
            CAST(ROUND(net_amount_raw * vat_rate, 2) AS DECIMAL(18,2)) AS vat_amount_chf,
            CAST(ROUND(net_amount_raw * (1 + vat_rate), 2) AS DECIMAL(18,2)) AS gross_amount_chf,
            ((row_id % 100) < 11) OR order_status = 'correction_requested' AS needs_review,
            TIMESTAMP '2025-01-01 07:00:00' + ((row_id * 67) % 46656000) * INTERVAL 1 SECOND AS updated_at
        FROM valued
    """


class ResultSetStorageSampleDataGenerator(DataGenerator):
    generator_id = "result_set_storage_s3_loader"
    title = "Result Set Storage S3 Loader"
    description = (
        "Generates a deterministic S3 Parquet order dataset and prepares the bucket used by "
        "the PoC notebook that stores a DuckDB result set back to S3."
    )
    target_kind = "s3"
    tree_path = RESULT_SET_STORAGE_SAMPLE_TREE_PATH
    default_size_gb = 0.01
    min_size_gb = 0.001
    max_size_gb = 16.0
    approximate_row_bytes = 216
    default_target_name = RESULT_SET_STORAGE_SAMPLE_SOURCE_NAME
    tags = ("s3", "parquet", "result-storage", "duckdb", "sample")

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return RESULT_SET_STORAGE_SAMPLE_BUCKET

    def _written_targets(
        self,
        *,
        source_prefix: str,
        result_path: str,
    ) -> list:
        return [
            generation_target(
                target_kind="s3_prefix",
                label="Source S3 Parquet path",
                location=source_prefix,
            ),
            generation_target(
                target_kind="s3_object",
                label="Notebook result-set S3 path",
                location=result_path,
            ),
        ]

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the result-set storage loader.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        batch_rows = 100_000
        batch_count = max(1, (total_rows + batch_rows - 1) // batch_rows)
        target_name = generated_name(self.default_target_name, context.job_id)
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        schema_name = s3_bucket_schema_name(bucket_name)
        relation = qualified_name(schema_name, target_name)
        relation_name = f"{schema_name}.{target_name}"
        object_prefix = f"s3://{bucket_name}/generated/{target_name}"
        object_key_prefix = f"generated/{target_name}"
        result_path = RESULT_SET_STORAGE_SAMPLE_RESULT_PATH
        connection = context.connect()
        upload_client = s3_client(settings)
        written_targets = self._written_targets(
            source_prefix=object_prefix,
            result_path=result_path,
        )

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing result-set storage sample...",
                message=f"Writing sample S3 Parquet source data to {object_prefix}.",
                target_name=target_name,
                target_relation=relation_name,
                target_path=object_prefix,
                written_targets=written_targets,
            )
            ensure_s3_bucket(settings, bucket_name)
            delete_s3_bucket(settings, bucket_name)
            ensure_s3_bucket(settings, bucket_name)
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(schema_name)}")
            connection.execute(f"DROP VIEW IF EXISTS {relation}")

            written_rows = 0
            with TemporaryDirectory(prefix=f"bdw-{self.generator_id}-{context.job_id[:8]}-") as temp_dir:
                temp_dir_path = Path(temp_dir)
                for batch_index, start_row in enumerate(range(0, total_rows, batch_rows), start=1):
                    context.raise_if_cancelled()
                    end_row = min(total_rows, start_row + batch_rows)
                    local_parquet_path = temp_dir_path / f"part-{batch_index:05d}.parquet"
                    object_key = f"{object_key_prefix}/part-{batch_index:05d}.parquet"
                    connection.execute(
                        "COPY ("
                        f"{result_set_storage_orders_select(start_row, end_row)}"
                        f") TO {sql_literal(local_parquet_path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    upload_s3_file(
                        upload_client,
                        local_path=local_parquet_path,
                        bucket=bucket_name,
                        key=object_key,
                    )
                    local_parquet_path.unlink(missing_ok=True)
                    written_rows = end_row
                    written_targets = update_generation_target_status(
                        written_targets,
                        object_prefix,
                        status="written",
                    )
                    context.report(
                        progress=written_rows / total_rows,
                        progress_label=f"Writing source batch {batch_index} / {batch_count}",
                        message=(
                            f"Wrote {written_rows:,} row(s) to {object_prefix}. "
                            f"The notebook will store its result set at {result_path}."
                        ),
                        target_name=target_name,
                        target_relation=relation_name,
                        target_path=object_prefix,
                        written_targets=written_targets,
                        generated_rows=written_rows,
                        generated_size_gb=approximate_size_gb(
                            written_rows,
                            self.approximate_row_bytes,
                        ),
                    )

            source_query = duckdb_scan_query("parquet", [f"{object_prefix}/*.parquet"])
            connection.execute(f"CREATE OR REPLACE VIEW {relation} AS {source_query}")

            return DataGeneratorResult(
                target_name=target_name,
                target_relation=relation_name,
                target_path=object_prefix,
                written_targets=written_targets,
                generated_rows=written_rows,
                generated_size_gb=approximate_size_gb(written_rows, self.approximate_row_bytes),
                message=(
                    f"Generated {written_rows:,} sample order row(s) in {relation_name}. "
                    f"Run the linked notebook to write {result_path}."
                ),
            )
        except DataGenerationCancelled:
            self._cleanup_partial_output(
                connection=connection,
                relation=relation,
                target_path=object_prefix,
                context=context,
            )
            context.report(
                message="Cancellation requested. Partial S3 source and result-set output were removed.",
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
                relation=relation,
                target_path=object_prefix,
                context=context,
            )
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        if not job.target_path:
            raise ValueError("This result-set storage sample job has no target path to clean.")

        bucket, _prefix = parse_s3_url(job.target_path)
        schema_name = s3_bucket_schema_name(bucket)
        target_name = job.target_name.strip() or self.default_target_name
        relation = qualified_name(schema_name, target_name)
        connection = context.connect()
        try:
            context.report(message=f"Cleaning result-set storage sample bucket {bucket}...")
            connection.execute(f"DROP VIEW IF EXISTS {relation}")
            deleted_objects = delete_s3_bucket(context.settings, bucket)
            bucket_deleted = remove_s3_bucket(context.settings, bucket)
            return DataGeneratorResult(
                target_name=target_name,
                generated_rows=0,
                generated_size_gb=0.0,
                message=(
                    f"Deleted {deleted_objects:,} generated object(s) from s3://{bucket}, "
                    f"{'removed the bucket' if bucket_deleted else 'left the empty bucket in place'}, "
                    "and removed the DuckDB view."
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


GENERATOR = ResultSetStorageSampleDataGenerator()

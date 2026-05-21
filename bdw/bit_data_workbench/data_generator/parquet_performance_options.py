from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from ..backend.s3_storage import (
    delete_s3_bucket,
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


@dataclass(frozen=True, slots=True)
class ParquetLayout:
    layout_id: str
    title_suffix: str
    description: str
    default_target_name: str
    bucket_segment: str
    hive_partitioning: bool = False
    partition_columns: tuple[str, ...] = ()
    sort_columns: tuple[str, ...] = ()
    create_duckdb_cache: bool = False
    index_columns: tuple[str, ...] = ()
    query_hive_option: str = "auto"


PARQUET_LAYOUTS = (
    ParquetLayout(
        layout_id="off",
        title_suffix="Off",
        description="Writes one S3 Parquet object without partitioning, sorting, or local ART cache setup.",
        default_target_name="federal_tax_parquet_off",
        bucket_segment="parquet-off",
    ),
    ParquetLayout(
        layout_id="recommended",
        title_suffix="Recommended",
        description="Writes one S3 Parquet object and represents the conservative recommended mode.",
        default_target_name="federal_tax_parquet_recommended",
        bucket_segment="parquet-recommended",
    ),
    ParquetLayout(
        layout_id="manual_partition_no_hive",
        title_suffix="Manual Partition, Hive Off",
        description=(
            "Writes tax_year partition folders while keeping tax_year inside every Parquet file "
            "for read_parquet(..., hive_partitioning=false)."
        ),
        default_target_name="federal_tax_parquet_manual_partition",
        bucket_segment="parquet-manual-partition-no-hive",
        partition_columns=("tax_year",),
        sort_columns=("filing_date", "taxpayer_id"),
        query_hive_option="off",
    ),
    ParquetLayout(
        layout_id="manual_partition_hive",
        title_suffix="Manual Partition, Hive On",
        description=(
            "Writes tax_year Hive partition folders and expects queries to read those values "
            "from the folder layout."
        ),
        default_target_name="federal_tax_parquet_manual_hive",
        bucket_segment="parquet-manual-partition-hive",
        hive_partitioning=True,
        partition_columns=("tax_year",),
        sort_columns=("filing_date", "taxpayer_id"),
        query_hive_option="on",
    ),
    ParquetLayout(
        layout_id="manual_cache_only",
        title_suffix="Manual Cache Only",
        description=(
            "Writes one S3 Parquet object and materializes a local DuckDB cache table with an ART "
            "index on taxpayer_id."
        ),
        default_target_name="federal_tax_parquet_manual_cache",
        bucket_segment="parquet-manual-cache-only",
        create_duckdb_cache=True,
        index_columns=("taxpayer_id",),
    ),
)


def federal_tax_dataset_select(start_row: int, end_row: int) -> str:
    if end_row <= start_row:
        return (
            "SELECT "
            "''::VARCHAR AS taxpayer_id, "
            "2024::INTEGER AS tax_year, "
            "''::VARCHAR AS canton, "
            "0::DECIMAL(18,2) AS income_chf, "
            "0::DECIMAL(18,2) AS deductions_chf, "
            "0::DECIMAL(18,2) AS taxable_income_chf, "
            "0::DECIMAL(8,2) AS tax_rate_percent, "
            "0::DECIMAL(18,2) AS tax_due_chf, "
            "''::VARCHAR AS payment_status, "
            "DATE '2025-01-01' AS filing_date "
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
                2024 + CAST(row_id % 3 AS INTEGER) AS tax_year,
                CASE row_id % 8
                    WHEN 0 THEN 'ZH'
                    WHEN 1 THEN 'BE'
                    WHEN 2 THEN 'LU'
                    WHEN 3 THEN 'UR'
                    WHEN 4 THEN 'SZ'
                    WHEN 5 THEN 'SG'
                    WHEN 6 THEN 'GR'
                    ELSE 'GE'
                END AS canton,
                ROUND(68000.0 + ((row_id * 7919) % 175000) + ((row_id % 17) * 312.45), 2) AS income_raw,
                ROUND(8500.0 + ((row_id * 3571) % 28000) + ((row_id % 11) * 97.25), 2) AS deduction_raw,
                CASE row_id % 5
                    WHEN 0 THEN 8.70
                    WHEN 1 THEN 10.20
                    WHEN 2 THEN 11.80
                    WHEN 3 THEN 12.90
                    ELSE 13.40
                END AS tax_rate_percent,
                CASE row_id % 4
                    WHEN 0 THEN 'paid'
                    WHEN 1 THEN 'pending'
                    WHEN 2 THEN 'paid'
                    ELSE 'overdue'
                END AS payment_status,
                DATE '2025-01-01' + CAST((row_id * 7) % 365 AS INTEGER) AS filing_date
            FROM base
        ),
        taxable AS (
            SELECT
                row_id,
                tax_year,
                canton,
                income_raw,
                LEAST(deduction_raw, income_raw * 0.38) AS deductions_raw,
                tax_rate_percent,
                payment_status,
                filing_date
            FROM valued
        )
        SELECT
            'TX-' || LPAD(CAST(100001 + row_id AS VARCHAR), 6, '0') AS taxpayer_id,
            tax_year,
            canton,
            CAST(income_raw AS DECIMAL(18,2)) AS income_chf,
            CAST(deductions_raw AS DECIMAL(18,2)) AS deductions_chf,
            CAST(ROUND(income_raw - deductions_raw, 2) AS DECIMAL(18,2)) AS taxable_income_chf,
            CAST(tax_rate_percent AS DECIMAL(8,2)) AS tax_rate_percent,
            CAST(ROUND((income_raw - deductions_raw) * tax_rate_percent / 100.0, 2) AS DECIMAL(18,2)) AS tax_due_chf,
            payment_status,
            filing_date
        FROM taxable
    """


def _parquet_scan_sql(path: str, *, hive_partitioning: bool | None = None) -> str:
    if hive_partitioning is None:
        return f"SELECT * FROM read_parquet({sql_literal(path)})"
    return (
        f"SELECT * FROM read_parquet({sql_literal(path)}, "
        f"hive_partitioning={'true' if hive_partitioning else 'false'})"
    )


class ParquetPerformanceOptionGenerator(DataGenerator):
    target_kind = "s3"
    tree_path = ("PoC Tests", "Performance Options")
    default_size_gb = 1.0
    min_size_gb = 0.001
    max_size_gb = 16.0
    approximate_row_bytes = 240
    supports_cancel = True
    supports_cleanup = True
    tags = ("s3", "parquet", "performance", "federal-tax")

    def __init__(self, layout: ParquetLayout) -> None:
        self.layout = layout
        self.generator_id = f"parquet_performance_options_{layout.layout_id}_loader"
        self.title = f"Federal Tax Parquet Optimization - {layout.title_suffix}"
        self.description = layout.description
        self.default_target_name = layout.default_target_name

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return loader_tree_bucket_name(
            (*self.tree_path, self.layout.bucket_segment),
            self.layout.bucket_segment,
        )

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the Parquet performance option loader.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        target_name = generated_name(self.default_target_name, context.job_id)
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        schema_name = s3_bucket_schema_name(bucket_name)
        relation = qualified_name(schema_name, target_name)
        relation_name = f"{schema_name}.{target_name}"
        object_key_prefix = f"generated/{target_name}"
        object_path = (
            f"s3://{bucket_name}/{object_key_prefix}/**/*.parquet"
            if self.layout.partition_columns
            else f"s3://{bucket_name}/{object_key_prefix}.parquet"
        )
        cache_relation = qualified_name(schema_name, f"{target_name}_duckdb_cache")
        cache_relation_name = f"{schema_name}.{target_name}_duckdb_cache"
        connection = context.connect()
        upload_client = s3_client(settings)
        written_targets = [
            generation_target(
                target_kind="s3_prefix" if self.layout.partition_columns else "s3_object",
                label="S3 Parquet layout",
                location=object_path,
            )
        ]
        if self.layout.create_duckdb_cache:
            written_targets.append(
                generation_target(
                    target_kind="duckdb_table",
                    label="DuckDB ART cache table",
                    location=cache_relation_name,
                )
            )

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing Parquet layout...",
                message=f"Writing {self.layout.title_suffix} federal tax Parquet layout to {object_path}.",
                target_name=target_name,
                target_relation=relation_name,
                target_path=object_path,
                written_targets=written_targets,
            )
            ensure_s3_bucket(settings, bucket_name)
            delete_s3_bucket(settings, bucket_name)
            ensure_s3_bucket(settings, bucket_name)
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(schema_name)}")
            connection.execute(f"DROP VIEW IF EXISTS {relation}")
            connection.execute(f"DROP TABLE IF EXISTS {cache_relation}")

            context.raise_if_cancelled()
            with TemporaryDirectory(prefix=f"bdw-{self.generator_id}-{context.job_id[:8]}-") as temp_dir:
                temp_dir_path = Path(temp_dir)
                if self.layout.partition_columns:
                    local_output = temp_dir_path / target_name
                    self._copy_dataset_to_parquet(
                        connection=connection,
                        target_path=local_output,
                        total_rows=total_rows,
                    )
                    uploaded_files = 0
                    for part_path in sorted(local_output.rglob("*.parquet")):
                        relative_key = part_path.relative_to(local_output).as_posix()
                        upload_s3_file(
                            upload_client,
                            local_path=part_path,
                            bucket=bucket_name,
                            key=f"{object_key_prefix}/{relative_key}",
                        )
                        uploaded_files += 1
                    if uploaded_files <= 0:
                        raise ValueError("DuckDB did not produce any partitioned Parquet files.")
                else:
                    local_output = temp_dir_path / f"{target_name}.parquet"
                    self._copy_dataset_to_parquet(
                        connection=connection,
                        target_path=local_output,
                        total_rows=total_rows,
                    )
                    upload_s3_file(
                        upload_client,
                        local_path=local_output,
                        bucket=bucket_name,
                        key=f"{object_key_prefix}.parquet",
                    )

            written_targets = update_generation_target_status(
                written_targets,
                object_path,
                status="written",
            )
            context.report(
                progress=0.8,
                progress_label="Registering query view...",
                message=f"Registering DuckDB view {relation_name} for {object_path}.",
                target_name=target_name,
                target_relation=relation_name,
                target_path=object_path,
                written_targets=written_targets,
                generated_rows=total_rows,
                generated_size_gb=approximate_size_gb(total_rows, self.approximate_row_bytes),
            )

            hive_option: bool | None
            if self.layout.partition_columns:
                hive_option = self.layout.hive_partitioning
            else:
                hive_option = None
            connection.execute(
                f"CREATE OR REPLACE VIEW {relation} AS {_parquet_scan_sql(object_path, hive_partitioning=hive_option)}"
            )

            if self.layout.create_duckdb_cache:
                connection.execute(f"CREATE OR REPLACE TABLE {cache_relation} AS SELECT * FROM {relation}")
                for column in self.layout.index_columns:
                    index_name = f"{target_name}_{column}_art_idx"
                    connection.execute(
                        f"CREATE INDEX {qualified_name(index_name)} ON {cache_relation} ({qualified_name(column)})"
                    )
                written_targets = update_generation_target_status(
                    written_targets,
                    cache_relation_name,
                    status="written",
                )

            return DataGeneratorResult(
                target_name=target_name,
                target_relation=relation_name,
                target_path=object_path,
                written_targets=written_targets,
                generated_rows=total_rows,
                generated_size_gb=approximate_size_gb(total_rows, self.approximate_row_bytes),
                message=f"Generated {total_rows:,} federal tax row(s) for {self.layout.title_suffix}.",
            )
        except DataGenerationCancelled:
            self._cleanup_partial_output(
                connection=connection,
                relation=relation,
                cache_relation=cache_relation,
                target_path=object_path,
                context=context,
            )
            context.report(
                message="Cancellation requested. Partial S3 layout and DuckDB cache output were removed.",
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
                cache_relation=cache_relation,
                target_path=object_path,
                context=context,
            )
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        if not job.target_path:
            raise ValueError("This Parquet performance option loader job has no target path to clean.")

        bucket, _prefix = parse_s3_url(job.target_path)
        schema_name = s3_bucket_schema_name(bucket)
        relation = qualified_name(schema_name, job.target_name)
        cache_relation = qualified_name(schema_name, f"{job.target_name}_duckdb_cache")
        connection = context.connect()
        try:
            connection.execute(f"DROP VIEW IF EXISTS {relation}")
            connection.execute(f"DROP TABLE IF EXISTS {cache_relation}")
            deleted_objects = delete_s3_bucket(context.settings, bucket)
            bucket_deleted = remove_s3_bucket(context.settings, bucket)
            return DataGeneratorResult(
                target_name=job.target_name,
                generated_rows=0,
                generated_size_gb=0.0,
                message=(
                    f"Deleted {deleted_objects:,} generated object(s) from s3://{bucket}, "
                    f"{'removed the bucket' if bucket_deleted else 'left the empty bucket in place'}, "
                    "and removed the DuckDB view/cache objects."
                ),
            )
        finally:
            connection.close()

    def _copy_dataset_to_parquet(
        self,
        *,
        connection,
        target_path: Path,
        total_rows: int,
    ) -> None:
        source_sql = federal_tax_dataset_select(0, total_rows)
        select_sql = f"SELECT * FROM ({source_sql}) AS federal_tax_source"
        if self.layout.sort_columns:
            select_sql += " ORDER BY " + ", ".join(
                qualified_name(column) for column in self.layout.sort_columns
            )
        copy_options = ["FORMAT PARQUET", "COMPRESSION ZSTD"]
        if self.layout.partition_columns:
            copy_options.append(
                "PARTITION_BY ("
                + ", ".join(qualified_name(column) for column in self.layout.partition_columns)
                + ")"
            )
            if not self.layout.hive_partitioning:
                copy_options.append("WRITE_PARTITION_COLUMNS TRUE")
        connection.execute(
            f"COPY ({select_sql}) TO {sql_literal(target_path.as_posix())} ({', '.join(copy_options)})"
        )

    def _cleanup_partial_output(
        self,
        *,
        connection,
        relation: str,
        cache_relation: str,
        target_path: str,
        context: DataGeneratorContext,
    ) -> None:
        try:
            connection.execute(f"DROP VIEW IF EXISTS {relation}")
        except Exception:
            pass
        try:
            connection.execute(f"DROP TABLE IF EXISTS {cache_relation}")
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


GENERATORS = tuple(ParquetPerformanceOptionGenerator(layout) for layout in PARQUET_LAYOUTS)

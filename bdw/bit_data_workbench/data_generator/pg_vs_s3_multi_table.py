from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from ..backend.s3_storage import (
    delete_s3_bucket,
    duckdb_scan_query,
    derived_s3_bucket_name,
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
    generated_name,
)
from .helpers import approximate_size_gb, qualified_name, sql_literal


MULTI_TABLE_OBJECT_NAMES = {
    "taxpayers": "federal_tax_taxpayers_mt",
    "filings": "federal_tax_filings_mt",
    "assessments": "federal_tax_assessments_mt",
    "payments": "federal_tax_payments_mt",
    "audits": "federal_tax_audits_mt",
    "enforcements": "federal_tax_enforcements_mt",
    "appeals": "federal_tax_appeals_mt",
}

TAXPAYER_COLUMNS = (
    "taxpayer_id BIGINT",
    "taxpayer_uid VARCHAR",
    "taxpayer_name VARCHAR",
    "canton_code VARCHAR",
    "taxpayer_type VARCHAR",
    "industry_sector VARCHAR",
    "registration_status VARCHAR",
    "risk_tier VARCHAR",
    "registered_at TIMESTAMP",
)

FILING_COLUMNS = (
    "filing_id BIGINT",
    "taxpayer_id BIGINT",
    "tax_type VARCHAR",
    "filing_channel VARCHAR",
    "filing_status VARCHAR",
    "tax_period_start DATE",
    "tax_period_end DATE",
    "declared_revenue_chf DECIMAL(18,2)",
    "declared_deduction_chf DECIMAL(18,2)",
    "declared_at TIMESTAMP",
)

ASSESSMENT_COLUMNS = (
    "assessment_id BIGINT",
    "filing_id BIGINT",
    "assessment_status VARCHAR",
    "assessed_tax_chf DECIMAL(18,2)",
    "surcharge_chf DECIMAL(18,2)",
    "waiver_chf DECIMAL(18,2)",
    "due_date DATE",
    "assessed_at TIMESTAMP",
)

PAYMENT_COLUMNS = (
    "payment_id BIGINT",
    "assessment_id BIGINT",
    "payment_status VARCHAR",
    "payment_method VARCHAR",
    "collected_tax_chf DECIMAL(18,2)",
    "settled_at TIMESTAMP",
)

AUDIT_COLUMNS = (
    "audit_id BIGINT",
    "taxpayer_id BIGINT",
    "filing_id BIGINT",
    "audit_status VARCHAR",
    "finding_severity VARCHAR",
    "additional_tax_chf DECIMAL(18,2)",
    "audit_risk_score DOUBLE",
    "opened_at TIMESTAMP",
    "closed_at TIMESTAMP",
)

ENFORCEMENT_COLUMNS = (
    "enforcement_id BIGINT",
    "assessment_id BIGINT",
    "action_status VARCHAR",
    "action_stage VARCHAR",
    "enforced_amount_chf DECIMAL(18,2)",
    "action_date TIMESTAMP",
)

APPEAL_COLUMNS = (
    "appeal_id BIGINT",
    "assessment_id BIGINT",
    "appeal_status VARCHAR",
    "ruling_stage VARCHAR",
    "contested_amount_chf DECIMAL(18,2)",
    "filed_at TIMESTAMP",
    "closed_at TIMESTAMP",
)


def taxpayers_select(start_row: int, end_row: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        )
        SELECT
            row_id + 1 AS taxpayer_id,
            'CHE-' || LPAD(CAST(100000000 + row_id AS VARCHAR), 9, '0') AS taxpayer_uid,
            (
                CASE row_id % 8
                    WHEN 0 THEN 'Alpine Trading AG'
                    WHEN 1 THEN 'Bern Logistics GmbH'
                    WHEN 2 THEN 'Lac Retail SA'
                    WHEN 3 THEN 'Helvetic Advisory AG'
                    WHEN 4 THEN 'Rhine Energy AG'
                    WHEN 5 THEN 'Jura Services SA'
                    WHEN 6 THEN 'Ticino Hospitality SA'
                    ELSE 'Nordstern Pharma AG'
                END
                || ' '
                || CAST(row_id + 1 AS VARCHAR)
            ) AS taxpayer_name,
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
            CASE row_id % 5
                WHEN 0 THEN 'Corporation'
                WHEN 1 THEN 'SME'
                WHEN 2 THEN 'Sole Proprietor'
                WHEN 3 THEN 'Importer'
                ELSE 'Public Institution'
            END AS taxpayer_type,
            CASE row_id % 6
                WHEN 0 THEN 'Consumer Goods'
                WHEN 1 THEN 'Manufacturing'
                WHEN 2 THEN 'Hospitality'
                WHEN 3 THEN 'Energy'
                WHEN 4 THEN 'Logistics'
                ELSE 'Financial Services'
            END AS industry_sector,
            CASE
                WHEN row_id % 29 = 0 THEN 'suspended'
                WHEN row_id % 13 = 0 THEN 'watchlist'
                ELSE 'active'
            END AS registration_status,
            CASE
                WHEN row_id % 41 = 0 THEN 'critical'
                WHEN row_id % 11 IN (0, 1, 2) THEN 'high'
                WHEN row_id % 5 = 0 THEN 'medium'
                ELSE 'standard'
            END AS risk_tier,
            TIMESTAMP '2022-01-01 08:00:00'
            + ((row_id * 271) % 94608000) * INTERVAL 1 SECOND AS registered_at
        FROM base
    """


def filings_select(start_row: int, end_row: int, taxpayer_count: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                ((row_id % {int(taxpayer_count)}) + 1) AS taxpayer_id,
                DATE '2023-01-01' + CAST((row_id % 1095) AS INTEGER) AS period_start,
                ROUND(
                    180000.0
                    + (((row_id * 311) % 24000000) / 10.0)
                    + ((row_id % 17) * 1300.0),
                    2
                ) AS revenue_raw,
                CASE row_id % 5
                    WHEN 0 THEN 0.09
                    WHEN 1 THEN 0.14
                    WHEN 2 THEN 0.06
                    WHEN 3 THEN 0.18
                    ELSE 0.11
                END AS deduction_rate
            FROM base
        )
        SELECT
            row_id + 1 AS filing_id,
            taxpayer_id,
            CASE row_id % 4
                WHEN 0 THEN 'VAT'
                WHEN 1 THEN 'COMPANY_TAX'
                WHEN 2 THEN 'WITHHOLDING_TAX'
                ELSE 'STAMP_DUTY'
            END AS tax_type,
            CASE row_id % 4
                WHEN 0 THEN 'portal'
                WHEN 1 THEN 'tax_advisor'
                WHEN 2 THEN 'api_batch'
                ELSE 'regional_office'
            END AS filing_channel,
            CASE
                WHEN row_id % 19 = 0 THEN 'escalated'
                WHEN row_id % 7 = 0 THEN 'under_review'
                WHEN row_id % 7 = 1 THEN 'submitted'
                WHEN row_id % 7 = 2 THEN 'amended'
                WHEN row_id % 7 IN (3, 4) THEN 'assessed'
                ELSE 'closed'
            END AS filing_status,
            period_start AS tax_period_start,
            period_start + CAST(89 AS INTEGER) AS tax_period_end,
            CAST(revenue_raw AS DECIMAL(18,2)) AS declared_revenue_chf,
            CAST(ROUND(revenue_raw * deduction_rate, 2) AS DECIMAL(18,2)) AS declared_deduction_chf,
            TIMESTAMP '2023-01-01 06:00:00'
            + ((row_id * 97) % 94608000) * INTERVAL 1 SECOND AS declared_at
        FROM prepared
    """


def assessments_select(start_row: int, end_row: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                ROUND(
                    24000.0
                    + (((row_id * 173) % 4200000) / 10.0)
                    + ((row_id % 17) * 280.0),
                    2
                ) AS assessed_tax_raw
            FROM base
        )
        SELECT
            row_id + 1 AS assessment_id,
            row_id + 1 AS filing_id,
            CASE row_id % 6
                WHEN 0 THEN 'under_review'
                WHEN 1 THEN 'assessed'
                WHEN 2 THEN 'assessed'
                WHEN 3 THEN 'appealed'
                WHEN 4 THEN 'enforced'
                ELSE 'closed'
            END AS assessment_status,
            CAST(assessed_tax_raw AS DECIMAL(18,2)) AS assessed_tax_chf,
            CAST(
                ROUND(
                    assessed_tax_raw
                    * CASE
                        WHEN row_id % 5 IN (0, 1) THEN 0.09
                        WHEN row_id % 5 = 2 THEN 0.04
                        ELSE 0.0
                    END,
                    2
                )
                AS DECIMAL(18,2)
            ) AS surcharge_chf,
            CAST(
                ROUND(
                    assessed_tax_raw
                    * CASE
                        WHEN row_id % 11 = 0 THEN 0.03
                        WHEN row_id % 13 = 0 THEN 0.015
                        ELSE 0.0
                    END,
                    2
                )
                AS DECIMAL(18,2)
            ) AS waiver_chf,
            DATE '2023-04-15' + CAST((row_id % 1095) AS INTEGER) AS due_date,
            TIMESTAMP '2023-02-01 09:00:00'
            + ((row_id * 137) % 94608000) * INTERVAL 1 SECOND AS assessed_at
        FROM prepared
    """


def payments_select(start_row: int, end_row: int, assessment_count: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                (((row_id * 7) % {int(assessment_count)}) + 1) AS assessment_id,
                ROUND(
                    3500.0
                    + (((row_id * 151) % 1800000) / 10.0)
                    + ((row_id % 9) * 90.0),
                    2
                ) AS collected_tax_raw
            FROM base
        )
        SELECT
            row_id + 1 AS payment_id,
            assessment_id,
            CASE row_id % 5
                WHEN 0 THEN 'posted'
                WHEN 1 THEN 'posted'
                WHEN 2 THEN 'late'
                WHEN 3 THEN 'partial'
                ELSE 'pending'
            END AS payment_status,
            CASE row_id % 4
                WHEN 0 THEN 'wire'
                WHEN 1 THEN 'debit'
                WHEN 2 THEN 'cash'
                ELSE 'offset'
            END AS payment_method,
            CAST(
                CASE row_id % 5
                    WHEN 0 THEN collected_tax_raw
                    WHEN 1 THEN ROUND(collected_tax_raw * 0.88, 2)
                    WHEN 2 THEN ROUND(collected_tax_raw * 0.63, 2)
                    WHEN 3 THEN ROUND(collected_tax_raw * 0.35, 2)
                    ELSE 0
                END
                AS DECIMAL(18,2)
            ) AS collected_tax_chf,
            TIMESTAMP '2023-03-01 10:00:00'
            + ((row_id * 83) % 94608000) * INTERVAL 1 SECOND AS settled_at
        FROM prepared
    """


def audits_select(start_row: int, end_row: int, taxpayer_count: int, filing_count: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                (((row_id * 5) % {int(taxpayer_count)}) + 1) AS taxpayer_id,
                (((row_id * 11) % {int(filing_count)}) + 1) AS filing_id,
                ROUND(28.0 + (((row_id * 17) % 6400) / 100.0), 2) AS risk_score,
                ROUND(1400.0 + (((row_id * 199) % 2100000) / 10.0), 2) AS additional_tax_raw
            FROM base
        )
        SELECT
            row_id + 1 AS audit_id,
            taxpayer_id,
            filing_id,
            CASE row_id % 4
                WHEN 0 THEN 'open'
                WHEN 1 THEN 'closed'
                WHEN 2 THEN 'escalated'
                ELSE 'closed'
            END AS audit_status,
            CASE
                WHEN row_id % 19 = 0 THEN 'critical'
                WHEN row_id % 7 IN (0, 1) THEN 'high'
                WHEN row_id % 5 = 0 THEN 'medium'
                ELSE 'low'
            END AS finding_severity,
            CAST(
                CASE row_id % 6
                    WHEN 0 THEN ROUND(additional_tax_raw * 1.80, 2)
                    WHEN 1 THEN ROUND(additional_tax_raw * 1.25, 2)
                    ELSE additional_tax_raw
                END
                AS DECIMAL(18,2)
            ) AS additional_tax_chf,
            risk_score AS audit_risk_score,
            TIMESTAMP '2023-02-15 08:30:00'
            + ((row_id * 191) % 94608000) * INTERVAL 1 SECOND AS opened_at,
            CASE
                WHEN row_id % 4 = 0 THEN NULL
                ELSE TIMESTAMP '2023-03-01 08:30:00'
                    + ((row_id * 211) % 94608000) * INTERVAL 1 SECOND
            END AS closed_at
        FROM prepared
    """


def enforcements_select(start_row: int, end_row: int, assessment_count: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                (((row_id * 13) % {int(assessment_count)}) + 1) AS assessment_id,
                ROUND(2200.0 + (((row_id * 107) % 1500000) / 10.0), 2) AS enforced_amount_raw
            FROM base
        )
        SELECT
            row_id + 1 AS enforcement_id,
            assessment_id,
            CASE row_id % 4
                WHEN 0 THEN 'active'
                WHEN 1 THEN 'resolved'
                WHEN 2 THEN 'escalated'
                ELSE 'closed'
            END AS action_status,
            CASE row_id % 4
                WHEN 0 THEN 'notice'
                WHEN 1 THEN 'collection'
                WHEN 2 THEN 'legal'
                ELSE 'closure'
            END AS action_stage,
            CAST(
                CASE
                    WHEN row_id % 4 = 3 THEN 0
                    ELSE enforced_amount_raw
                END
                AS DECIMAL(18,2)
            ) AS enforced_amount_chf,
            TIMESTAMP '2023-04-01 07:45:00'
            + ((row_id * 149) % 94608000) * INTERVAL 1 SECOND AS action_date
        FROM prepared
    """


def appeals_select(start_row: int, end_row: int, assessment_count: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                (((row_id * 17) % {int(assessment_count)}) + 1) AS assessment_id,
                ROUND(7000.0 + (((row_id * 163) % 2600000) / 10.0), 2) AS contested_amount_raw
            FROM base
        )
        SELECT
            row_id + 1 AS appeal_id,
            assessment_id,
            CASE row_id % 4
                WHEN 0 THEN 'open'
                WHEN 1 THEN 'resolved'
                WHEN 2 THEN 'escalated'
                ELSE 'withdrawn'
            END AS appeal_status,
            CASE row_id % 4
                WHEN 0 THEN 'cantonal'
                WHEN 1 THEN 'federal'
                WHEN 2 THEN 'tribunal'
                ELSE 'withdrawn'
            END AS ruling_stage,
            CAST(
                CASE
                    WHEN row_id % 4 = 3 THEN 0
                    ELSE contested_amount_raw
                END
                AS DECIMAL(18,2)
            ) AS contested_amount_chf,
            TIMESTAMP '2023-05-01 11:00:00'
            + ((row_id * 173) % 94608000) * INTERVAL 1 SECOND AS filed_at,
            CASE
                WHEN row_id % 4 IN (0, 2) THEN NULL
                ELSE TIMESTAMP '2023-07-01 11:00:00'
                    + ((row_id * 181) % 94608000) * INTERVAL 1 SECOND
            END AS closed_at
        FROM prepared
    """


class PgVsS3MultiTableDataGenerator(DataGenerator):
    generator_id = "pg_vs_s3_multi_table_loader"
    title = "PG vs S3 Multi-Table Federal Tax Loader"
    description = (
        "Generates a mirrored federal-tax dataset across seven related tables in PostgreSQL OLTP "
        "and S3-backed Parquet so multi-table benchmark queries can be compared across engines."
    )
    target_kind = "contest"
    tree_path = ("PoC Tests", "Performance Evaluation", "Multi-Table Test")
    default_size_gb = 1.0
    min_size_gb = 0.01
    max_size_gb = 128.0
    approximate_row_bytes = 1180
    default_target_name = "federal_tax_multi_table"
    tags = ("postgres", "s3", "oltp", "contest", "tax", "assessment", "multi-table")

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return derived_s3_bucket_name(base_bucket, "pg-vs-s3-multi-table")

    def _row_counts(self, total_rows: int) -> dict[str, int]:
        return {
            "taxpayers": max(1000, total_rows // 4),
            "filings": max(1, total_rows),
            "assessments": max(1, total_rows),
            "payments": max(1000, (total_rows * 7) // 5),
            "audits": max(1000, total_rows // 3),
            "enforcements": max(1000, total_rows // 4),
            "appeals": max(1000, total_rows // 5),
        }

    def _table_specs(self, row_counts: dict[str, int]) -> list[dict[str, object]]:
        taxpayer_count = row_counts["taxpayers"]
        filing_count = row_counts["filings"]
        assessment_count = row_counts["assessments"]
        return [
            {
                "name": MULTI_TABLE_OBJECT_NAMES["taxpayers"],
                "columns": TAXPAYER_COLUMNS,
                "row_count": taxpayer_count,
                "select_builder": taxpayers_select,
            },
            {
                "name": MULTI_TABLE_OBJECT_NAMES["filings"],
                "columns": FILING_COLUMNS,
                "row_count": filing_count,
                "select_builder": lambda start, end: filings_select(start, end, taxpayer_count),
            },
            {
                "name": MULTI_TABLE_OBJECT_NAMES["assessments"],
                "columns": ASSESSMENT_COLUMNS,
                "row_count": assessment_count,
                "select_builder": assessments_select,
            },
            {
                "name": MULTI_TABLE_OBJECT_NAMES["payments"],
                "columns": PAYMENT_COLUMNS,
                "row_count": row_counts["payments"],
                "select_builder": lambda start, end: payments_select(start, end, assessment_count),
            },
            {
                "name": MULTI_TABLE_OBJECT_NAMES["audits"],
                "columns": AUDIT_COLUMNS,
                "row_count": row_counts["audits"],
                "select_builder": lambda start, end: audits_select(start, end, taxpayer_count, filing_count),
            },
            {
                "name": MULTI_TABLE_OBJECT_NAMES["enforcements"],
                "columns": ENFORCEMENT_COLUMNS,
                "row_count": row_counts["enforcements"],
                "select_builder": lambda start, end: enforcements_select(start, end, assessment_count),
            },
            {
                "name": MULTI_TABLE_OBJECT_NAMES["appeals"],
                "columns": APPEAL_COLUMNS,
                "row_count": row_counts["appeals"],
                "select_builder": lambda start, end: appeals_select(start, end, assessment_count),
            },
        ]

    def _drop_postgres_tables(self, connection) -> int:
        dropped = 0
        for table_name in reversed(tuple(MULTI_TABLE_OBJECT_NAMES.values())):
            connection.execute(f"DROP TABLE IF EXISTS {qualified_name('pg_oltp', 'public', table_name)}")
            dropped += 1
        return dropped

    def _drop_s3_views(self, connection, schema_name: str) -> int:
        dropped = 0
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(schema_name)}")
        for table_name in reversed(tuple(MULTI_TABLE_OBJECT_NAMES.values())):
            connection.execute(f"DROP VIEW IF EXISTS {qualified_name(schema_name, table_name)}")
            dropped += 1
        return dropped

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the multi-table loader.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        row_counts = self._row_counts(total_rows)
        table_specs = self._table_specs(row_counts)
        total_generated_rows = sum(int(spec["row_count"]) for spec in table_specs)
        batch_rows = 100_000
        target_name = generated_name(self.default_target_name, context.job_id)
        primary_relation_name = f"pg_oltp.public.{MULTI_TABLE_OBJECT_NAMES['assessments']}"
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        s3_schema = s3_bucket_schema_name(bucket_name)
        object_prefix = f"s3://{bucket_name}"
        connection = context.connect()
        s3_upload_client = s3_client(settings)

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing targets...",
                message=(
                    "Creating seven mirrored federal-tax tables in PostgreSQL OLTP and S3-backed "
                    f"Parquet under {object_prefix}."
                ),
                target_name=target_name,
                target_relation=primary_relation_name,
                target_path=object_prefix,
            )
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(s3_schema)}")
            self._drop_postgres_tables(connection)
            self._drop_s3_views(connection, s3_schema)
            delete_s3_bucket(settings, bucket_name)
            ensure_s3_bucket(settings, bucket_name)

            processed_rows = 0
            with TemporaryDirectory(prefix=f"bdw-{self.generator_id}-{context.job_id[:8]}-") as temp_dir:
                temp_dir_path = Path(temp_dir)
                for spec in table_specs:
                    table_name = str(spec["name"])
                    row_count = int(spec["row_count"])
                    columns = tuple(spec["columns"])
                    select_builder = spec["select_builder"]
                    postgres_relation = qualified_name("pg_oltp", "public", table_name)
                    s3_relation = qualified_name(s3_schema, table_name)
                    batch_count = max(1, (row_count + batch_rows - 1) // batch_rows)

                    connection.execute(f"CREATE TABLE {postgres_relation} ({', '.join(columns)})")

                    for batch_index, start_row in enumerate(range(0, row_count, batch_rows), start=1):
                        context.raise_if_cancelled()
                        end_row = min(row_count, start_row + batch_rows)
                        select_sql = select_builder(start_row, end_row)
                        object_key = f"{table_name}/part-{batch_index:05d}.parquet"
                        table_object_prefix = f"{object_prefix}/{table_name}"
                        local_parquet_path = temp_dir_path / table_name / f"part-{batch_index:05d}.parquet"
                        local_parquet_path.parent.mkdir(parents=True, exist_ok=True)

                        connection.execute(f"INSERT INTO {postgres_relation} {select_sql}")
                        connection.execute(
                            "COPY ("
                            f"{select_sql}"
                            f") TO {sql_literal(local_parquet_path.as_posix())} "
                            "(FORMAT PARQUET, COMPRESSION ZSTD)"
                        )
                        upload_s3_file(
                            s3_upload_client,
                            local_path=local_parquet_path,
                            bucket=bucket_name,
                            key=object_key,
                        )
                        local_parquet_path.unlink(missing_ok=True)

                        processed_rows += end_row - start_row
                        context.report(
                            progress=processed_rows / total_generated_rows,
                            progress_label=f"Writing {table_name} batch {batch_index} / {batch_count}",
                            message=(
                                f"Wrote {end_row:,} row(s) for {table_name} into PostgreSQL OLTP and "
                                f"{table_object_prefix}."
                            ),
                            target_name=target_name,
                            target_relation=primary_relation_name,
                            target_path=object_prefix,
                            generated_rows=processed_rows,
                            generated_size_gb=approximate_size_gb(processed_rows, self.approximate_row_bytes),
                        )

                    s3_query = duckdb_scan_query("parquet", [f"{object_prefix}/{table_name}/*.parquet"])
                    connection.execute(f"CREATE OR REPLACE VIEW {s3_relation} AS {s3_query}")

            return DataGeneratorResult(
                target_name=target_name,
                target_relation=primary_relation_name,
                target_path=object_prefix,
                generated_rows=processed_rows,
                generated_size_gb=approximate_size_gb(processed_rows, self.approximate_row_bytes),
                message=(
                    f"Generated {processed_rows:,} mirrored multi-table row(s) across "
                    f"{len(table_specs)} PostgreSQL tables and {len(table_specs)} S3 views."
                ),
            )
        except DataGenerationCancelled:
            self._cleanup_partial_output(
                connection=connection,
                s3_schema=s3_schema,
                target_path=object_prefix,
                context=context,
            )
            context.report(
                message="Cancellation requested. Partial PostgreSQL tables, S3 views, and bucket objects were removed.",
                target_relation="",
                target_path="",
                generated_rows=0,
                generated_size_gb=0.0,
            )
            raise
        except Exception:
            self._cleanup_partial_output(
                connection=connection,
                s3_schema=s3_schema,
                target_path=object_prefix,
                context=context,
            )
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        if not job.target_path:
            raise ValueError("This multi-table generation job has no target path to clean.")

        bucket, _prefix = parse_s3_url(job.target_path)
        s3_schema = s3_bucket_schema_name(bucket)
        connection = context.connect()

        try:
            context.report(message=f"Cleaning mirrored multi-table targets from PostgreSQL OLTP and s3://{bucket}...")
            dropped_tables = self._drop_postgres_tables(connection)
            dropped_views = self._drop_s3_views(connection, s3_schema)
            deleted_objects = delete_s3_bucket(context.settings, bucket)
            bucket_deleted = remove_s3_bucket(context.settings, bucket)
            return DataGeneratorResult(
                target_name=job.target_name,
                target_relation="",
                target_path="",
                generated_rows=0,
                generated_size_gb=0.0,
                message=(
                    f"Dropped {dropped_tables} PostgreSQL table(s), removed {dropped_views} S3 view(s), "
                    f"deleted {deleted_objects:,} object(s) from s3://{bucket}, and "
                    f"{'removed the bucket' if bucket_deleted else 'left the empty bucket in place'}."
                ),
            )
        finally:
            connection.close()

    def _cleanup_partial_output(
        self,
        *,
        connection,
        s3_schema: str,
        target_path: str,
        context: DataGeneratorContext,
    ) -> None:
        try:
            self._drop_postgres_tables(connection)
        except Exception:
            pass
        try:
            self._drop_s3_views(connection, s3_schema)
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


GENERATOR = PgVsS3MultiTableDataGenerator()

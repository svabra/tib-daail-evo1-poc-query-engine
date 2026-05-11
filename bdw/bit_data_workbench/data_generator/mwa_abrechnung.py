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


MWA_ABRECHNUNG_TABLE = "mwa_abrechnung_entities"
MWA_ZIFFERN_TABLE = "mwa_abrechnungs_ziffern_entities"
MWA_TABLE_NAMES = (MWA_ABRECHNUNG_TABLE, MWA_ZIFFERN_TABLE)
PARQUET_COPY_OPTIONS = "FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000"

MWA_ABRECHNUNG_COLUMNS = (
    "id_ INTEGER",
    "created_at TIMESTAMP",
    "updated_at TIMESTAMP",
    "deleted_at TIMESTAMP",
    "status BIGINT",
    "einreiche_datum TIMESTAMP",
    "umsaetze_in_netto VARCHAR(20)",
    "abrechnungs_aufforderung_refer BIGINT",
    "contact_role INTEGER",
    "contact_first_name VARCHAR(255)",
    "contact_last_name VARCHAR(255)",
    "contact_phone_number VARCHAR(255)",
    "contact_email VARCHAR(255)",
    "final_abrechnung_form_id BIGINT",
    "einfordern VARCHAR(20)",
    "address_number VARCHAR(255)",
    "bankdata_number VARCHAR(255)",
    "beguenstigter_name VARCHAR(255)",
    "beguenstigter_address_post_code VARCHAR(255)",
    "beguenstigter_address_city VARCHAR(255)",
    "beguenstigter_account_iban VARCHAR(255)",
    "beguenstigter_account_bank_identifier_code VARCHAR(255)",
    "beguenstigter_account_account_number VARCHAR(255)",
    "beguenstigter_account_bank_name VARCHAR(255)",
    "beguenstigter_account_bank_city VARCHAR(255)",
    "beguenstigter_account_bank_country VARCHAR(255)",
    "status_update_received_at TIMESTAMP",
    "rounded_total VARCHAR(20)",
    "is_sub_form1056_needed VARCHAR(20)",
    "version_ BIGINT",
    "tax_period_refer BIGINT",
    "ausfueller VARCHAR(255)",
    "einreicher VARCHAR(255)",
    "is_approved VARCHAR(20)",
    "is_ready_for_audit VARCHAR(20)",
    "moe_id VARCHAR(100)",
    "approval_message_address VARCHAR(100)",
    "beguenstigter_partner_id VARCHAR(100)",
    "uid VARCHAR(100)",
    "partner_id VARCHAR(100)",
    "rounding VARCHAR(100)",
    "vlv_data VARCHAR(32000)",
    "prla_prozesslauf DECIMAL(18,0)",
    "proz_prozess INTEGER",
)

MWA_ZIFFERN_COLUMNS = (
    "id_ INTEGER",
    "created_at TIMESTAMP",
    "updated_at TIMESTAMP",
    "deleted_at TIMESTAMP",
    "ziffer_nummer VARCHAR(255)",
    "umsatz DOUBLE",
    "steuer DOUBLE",
    "satz DOUBLE",
    "steuersatz_type BIGINT",
    "kommentar VARCHAR(255)",
    "satz_editable VARCHAR(20)",
    "abrechnung_refer BIGINT",
    "moe_id VARCHAR(32000)",
    "migration_abrechnung_refer INTEGER",
    "prla_prozesslauf DECIMAL(18,0)",
    "proz_prozess INTEGER",
)


def mwa_abrechnung_entities_select(start_row: int, end_row: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                ROUND(
                    5000.0
                    + (((row_id * 197) % 9500000) / 10.0)
                    + ((row_id % 17) * 31.75),
                    2
                ) AS total_raw
            FROM base
        )
        SELECT
            CAST(row_id + 1 AS INTEGER) AS id_,
            TIMESTAMP '2024-01-01 08:00:00'
                + ((row_id * 37) % 63072000) * INTERVAL 1 SECOND AS created_at,
            TIMESTAMP '2024-01-02 09:00:00'
                + ((row_id * 41) % 63072000) * INTERVAL 1 SECOND AS updated_at,
            CASE
                WHEN row_id % 31 = 0 THEN TIMESTAMP '2025-01-01 00:00:00'
                    + ((row_id * 43) % 15768000) * INTERVAL 1 SECOND
                ELSE NULL
            END AS deleted_at,
            CAST(row_id % 7 AS BIGINT) AS status,
            TIMESTAMP '2024-02-01 10:00:00'
                + ((row_id * 59) % 31536000) * INTERVAL 1 SECOND AS einreiche_datum,
            CASE WHEN row_id % 2 = 0 THEN 'true' ELSE 'false' END AS umsaetze_in_netto,
            CAST(700000000 + row_id AS BIGINT) AS abrechnungs_aufforderung_refer,
            CAST((row_id % 5) + 1 AS INTEGER) AS contact_role,
            CASE row_id % 6
                WHEN 0 THEN 'Anna'
                WHEN 1 THEN 'Bruno'
                WHEN 2 THEN 'Clara'
                WHEN 3 THEN 'David'
                WHEN 4 THEN 'Eva'
                ELSE 'Felix'
            END AS contact_first_name,
            CASE row_id % 6
                WHEN 0 THEN 'Meyer'
                WHEN 1 THEN 'Schmid'
                WHEN 2 THEN 'Keller'
                WHEN 3 THEN 'Weber'
                WHEN 4 THEN 'Fischer'
                ELSE 'Huber'
            END AS contact_last_name,
            '+41 58 ' || LPAD(CAST(1000000 + (row_id % 9000000) AS VARCHAR), 7, '0')
                AS contact_phone_number,
            'mwa.contact.' || CAST(row_id + 1 AS VARCHAR) || '@example.admin.ch'
                AS contact_email,
            CAST(900000000 + row_id AS BIGINT) AS final_abrechnung_form_id,
            CASE WHEN row_id % 4 IN (0, 1) THEN 'true' ELSE 'false' END AS einfordern,
            'ADR-' || LPAD(CAST(row_id + 1 AS VARCHAR), 10, '0') AS address_number,
            'BNK-' || LPAD(CAST((row_id * 3) + 1 AS VARCHAR), 10, '0') AS bankdata_number,
            CASE row_id % 5
                WHEN 0 THEN 'Alpine Trading AG'
                WHEN 1 THEN 'Bern Logistics GmbH'
                WHEN 2 THEN 'Lac Retail SA'
                WHEN 3 THEN 'Helvetic Advisory AG'
                ELSE 'Rhine Energy AG'
            END AS beguenstigter_name,
            LPAD(CAST(1000 + (row_id % 8500) AS VARCHAR), 4, '0')
                AS beguenstigter_address_post_code,
            CASE row_id % 8
                WHEN 0 THEN 'Bern'
                WHEN 1 THEN 'Zurich'
                WHEN 2 THEN 'Geneva'
                WHEN 3 THEN 'Lausanne'
                WHEN 4 THEN 'Basel'
                WHEN 5 THEN 'Lugano'
                WHEN 6 THEN 'St. Gallen'
                ELSE 'Lucerne'
            END AS beguenstigter_address_city,
            'CH' || LPAD(CAST(1000000000000000000 + row_id AS VARCHAR), 19, '0')
                AS beguenstigter_account_iban,
            CASE row_id % 4
                WHEN 0 THEN 'POFICHBEXXX'
                WHEN 1 THEN 'UBSWCHZH80A'
                WHEN 2 THEN 'CRESCHZZ80A'
                ELSE 'RAIFCH22XXX'
            END AS beguenstigter_account_bank_identifier_code,
            LPAD(CAST(1000000000 + row_id AS VARCHAR), 12, '0')
                AS beguenstigter_account_account_number,
            CASE row_id % 4
                WHEN 0 THEN 'PostFinance'
                WHEN 1 THEN 'UBS Switzerland'
                WHEN 2 THEN 'Credit Suisse'
                ELSE 'Raiffeisen'
            END AS beguenstigter_account_bank_name,
            CASE row_id % 4
                WHEN 0 THEN 'Bern'
                WHEN 1 THEN 'Zurich'
                WHEN 2 THEN 'Geneva'
                ELSE 'St. Gallen'
            END AS beguenstigter_account_bank_city,
            'CH' AS beguenstigter_account_bank_country,
            TIMESTAMP '2024-03-01 08:30:00'
                + ((row_id * 67) % 31536000) * INTERVAL 1 SECOND
                AS status_update_received_at,
            CAST(CAST(ROUND(total_raw, 2) AS DECIMAL(18,2)) AS VARCHAR) AS rounded_total,
            CASE WHEN row_id % 9 = 0 THEN 'true' ELSE 'false' END AS is_sub_form1056_needed,
            CAST(1 + (row_id % 12) AS BIGINT) AS version_,
            CAST(202000 + (row_id % 12) AS BIGINT) AS tax_period_refer,
            'ausfueller-' || LPAD(CAST(row_id % 10000 AS VARCHAR), 4, '0') AS ausfueller,
            'einreicher-' || LPAD(CAST((row_id * 7) % 10000 AS VARCHAR), 4, '0')
                AS einreicher,
            CASE WHEN row_id % 6 IN (0, 1, 2, 3) THEN 'true' ELSE 'false' END AS is_approved,
            CASE WHEN row_id % 5 IN (0, 1, 2) THEN 'true' ELSE 'false' END AS is_ready_for_audit,
            'MOE-' || LPAD(CAST(row_id + 1 AS VARCHAR), 12, '0') AS moe_id,
            CASE row_id % 4
                WHEN 0 THEN 'portal'
                WHEN 1 THEN 'mail'
                WHEN 2 THEN 'api'
                ELSE 'office'
            END AS approval_message_address,
            'BP-' || LPAD(CAST(500000 + (row_id % 500000) AS VARCHAR), 6, '0')
                AS beguenstigter_partner_id,
            'UID-' || LPAD(CAST(100000000 + row_id AS VARCHAR), 9, '0') AS uid,
            'PID-' || LPAD(CAST(200000000 + row_id AS VARCHAR), 9, '0') AS partner_id,
            CASE row_id % 3
                WHEN 0 THEN 'commercial'
                WHEN 1 THEN 'bankers'
                ELSE 'none'
            END AS rounding,
            'vlv-row-' || CAST(row_id + 1 AS VARCHAR) || '-total-' || CAST(total_raw AS VARCHAR)
                AS vlv_data,
            CAST(row_id + 1 AS DECIMAL(18,0)) AS prla_prozesslauf,
            CAST(100 + (row_id % 9) AS INTEGER) AS proz_prozess
        FROM prepared
    """


def mwa_abrechnungs_ziffern_entities_select(
    start_row: int,
    end_row: int,
    parent_row_count: int,
) -> str:
    safe_parent_count = max(1, int(parent_row_count))
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                CAST((row_id % {safe_parent_count}) + 1 AS BIGINT) AS abrechnung_id,
                ROUND(
                    100.0
                    + (((row_id * 149) % 3500000) / 100.0)
                    + ((row_id % 11) * 14.25),
                    2
                ) AS umsatz_raw,
                CASE row_id % 4
                    WHEN 0 THEN 0.081
                    WHEN 1 THEN 0.026
                    WHEN 2 THEN 0.038
                    ELSE 0.0
                END AS steuer_rate
            FROM base
        )
        SELECT
            CAST(row_id + 1 AS INTEGER) AS id_,
            TIMESTAMP '2024-01-01 08:15:00'
                + ((row_id * 31) % 63072000) * INTERVAL 1 SECOND AS created_at,
            TIMESTAMP '2024-01-02 09:15:00'
                + ((row_id * 47) % 63072000) * INTERVAL 1 SECOND AS updated_at,
            CASE
                WHEN row_id % 43 = 0 THEN TIMESTAMP '2025-02-01 00:00:00'
                    + ((row_id * 53) % 15768000) * INTERVAL 1 SECOND
                ELSE NULL
            END AS deleted_at,
            'Z' || LPAD(CAST(100 + (row_id % 900) AS VARCHAR), 3, '0') AS ziffer_nummer,
            umsatz_raw AS umsatz,
            ROUND(umsatz_raw * steuer_rate, 2) AS steuer,
            ROUND(steuer_rate * 100, 2) AS satz,
            CAST(row_id % 5 AS BIGINT) AS steuersatz_type,
            CASE row_id % 5
                WHEN 0 THEN 'standard turnover'
                WHEN 1 THEN 'reduced rate'
                WHEN 2 THEN 'special declaration'
                WHEN 3 THEN 'manual correction'
                ELSE 'tax exempt'
            END AS kommentar,
            CASE WHEN row_id % 3 = 0 THEN 'true' ELSE 'false' END AS satz_editable,
            abrechnung_id AS abrechnung_refer,
            'MOE-Z-' || LPAD(CAST(row_id + 1 AS VARCHAR), 12, '0') AS moe_id,
            CAST(abrechnung_id AS INTEGER) AS migration_abrechnung_refer,
            CAST(row_id + 1 AS DECIMAL(18,0)) AS prla_prozesslauf,
            CAST(200 + (row_id % 9) AS INTEGER) AS proz_prozess
        FROM prepared
    """


def _ordered_select(select_sql: str, order_by: str) -> str:
    return f"SELECT * FROM ({select_sql}) AS generated_batch ORDER BY {order_by}"


class MwaAbrechnungMultiFormatDataGenerator(DataGenerator):
    generator_id = "mwa_abrechnung_multi_format_loader"
    title = "MWA Abrechnung Multi-Format Loader (3.2)"
    description = (
        "Generates paired MWA Abrechnung and Abrechnungs-Ziffern entities and writes them to "
        "PostgreSQL OLTP plus S3-backed Parquet, CSV, and JSONL comparison targets."
    )
    target_kind = "contest"
    tree_path = ("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)")
    default_size_gb = 1.0
    min_size_gb = 0.01
    max_size_gb = 128.0
    approximate_row_bytes = 720
    default_target_name = "mwa_abrechnung"
    tags = (
        "postgres",
        "s3",
        "oltp",
        "native",
        "parquet",
        "csv",
        "json",
        "mwa",
        "abrechnung",
    )

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return loader_tree_bucket_name(self.tree_path, "mwa-abrechnung")

    def _row_counts(self, total_rows: int) -> dict[str, int]:
        parent_rows = max(1, total_rows // 4)
        ziffer_rows = max(1, total_rows - parent_rows)
        return {
            MWA_ABRECHNUNG_TABLE: parent_rows,
            MWA_ZIFFERN_TABLE: ziffer_rows,
        }

    def _table_specs(self, row_counts: dict[str, int]) -> list[dict[str, object]]:
        parent_rows = row_counts[MWA_ABRECHNUNG_TABLE]
        return [
            {
                "name": MWA_ABRECHNUNG_TABLE,
                "columns": MWA_ABRECHNUNG_COLUMNS,
                "row_count": parent_rows,
                "order_by": "id_",
                "select_builder": mwa_abrechnung_entities_select,
            },
            {
                "name": MWA_ZIFFERN_TABLE,
                "columns": MWA_ZIFFERN_COLUMNS,
                "row_count": row_counts[MWA_ZIFFERN_TABLE],
                "order_by": "abrechnung_refer, id_",
                "select_builder": lambda start, end: mwa_abrechnungs_ziffern_entities_select(
                    start,
                    end,
                    parent_rows,
                ),
            },
        ]

    def _s3_format_specs(self) -> tuple[dict[str, str], ...]:
        return (
            {
                "name": "parquet",
                "extension": "parquet",
                "copy_options": PARQUET_COPY_OPTIONS,
                "scan_format": "parquet",
                "label": "Dedicated S3 Parquet path",
            },
            {
                "name": "csv",
                "extension": "csv",
                "copy_options": "FORMAT CSV, HEADER TRUE",
                "scan_format": "csv",
                "label": "Dedicated S3 CSV path",
            },
            {
                "name": "json",
                "extension": "jsonl",
                "copy_options": "FORMAT JSON",
                "scan_format": "json",
                "label": "Dedicated S3 JSONL path",
            },
        )

    def _s3_prefix(self, root_prefix: str, format_name: str, table_name: str) -> str:
        return f"{root_prefix}/{format_name}/{table_name}"

    def _s3_view_name(self, table_name: str, format_name: str) -> str:
        return f"{table_name}_{format_name}"

    def _written_targets(self, *, root_prefix: str, table_names: tuple[str, ...]) -> list:
        written_targets = []
        for table_name in table_names:
            written_targets.append(
                generation_target(
                    target_kind="postgres_table",
                    label="PostgreSQL OLTP table",
                    location=f"pg_oltp.public.{table_name}",
                )
            )
            for format_spec in self._s3_format_specs():
                written_targets.append(
                    generation_target(
                        target_kind="s3_prefix",
                        label=format_spec["label"],
                        location=self._s3_prefix(root_prefix, format_spec["name"], table_name),
                    )
                )
        return written_targets

    def _drop_postgres_tables(self, connection) -> int:
        dropped = 0
        for table_name in reversed(MWA_TABLE_NAMES):
            connection.execute(f"DROP TABLE IF EXISTS {qualified_name('pg_oltp', 'public', table_name)}")
            dropped += 1
        return dropped

    def _drop_s3_views(self, connection, schema_name: str) -> int:
        dropped = 0
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(schema_name)}")
        for table_name in MWA_TABLE_NAMES:
            for format_spec in self._s3_format_specs():
                view_name = self._s3_view_name(table_name, format_spec["name"])
                connection.execute(f"DROP VIEW IF EXISTS {qualified_name(schema_name, view_name)}")
                dropped += 1
        return dropped

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.pg_oltp_database:
            raise ValueError("PG_OLTP_DATABASE must be configured before running the MWA loader.")
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the MWA loader.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        row_counts = self._row_counts(total_rows)
        table_specs = self._table_specs(row_counts)
        total_generated_rows = sum(int(spec["row_count"]) for spec in table_specs)
        batch_rows = 100_000
        target_name = generated_name(self.default_target_name, context.job_id)
        primary_relation_name = f"pg_oltp.public.{MWA_ABRECHNUNG_TABLE}"
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        s3_schema = s3_bucket_schema_name(bucket_name)
        object_prefix = f"s3://{bucket_name}/generated/{target_name}"
        connection = context.connect()
        s3_upload_client = s3_client(settings)
        written_targets = self._written_targets(root_prefix=object_prefix, table_names=MWA_TABLE_NAMES)

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing targets...",
                message=(
                    "Creating paired MWA Abrechnung tables in PostgreSQL OLTP and S3-backed "
                    f"Parquet, CSV, and JSONL under {object_prefix}."
                ),
                target_name=target_name,
                target_relation=primary_relation_name,
                target_path=object_prefix,
                written_targets=written_targets,
            )
            ensure_s3_bucket(settings, bucket_name)
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
                    order_by = str(spec["order_by"])
                    postgres_relation = qualified_name("pg_oltp", "public", table_name)
                    postgres_relation_name = f"pg_oltp.public.{table_name}"
                    batch_relation = qualified_name(f"{table_name}_batch")
                    batch_count = max(1, (row_count + batch_rows - 1) // batch_rows)

                    connection.execute(f"CREATE TABLE {postgres_relation} ({', '.join(columns)})")

                    for batch_index, start_row in enumerate(range(0, row_count, batch_rows), start=1):
                        context.raise_if_cancelled()
                        end_row = min(row_count, start_row + batch_rows)
                        select_sql = select_builder(start_row, end_row)
                        ordered_sql = _ordered_select(select_sql, order_by)

                        connection.execute(
                            f"CREATE OR REPLACE TEMP TABLE {batch_relation} AS {ordered_sql}"
                        )
                        connection.execute(f"INSERT INTO {postgres_relation} SELECT * FROM {batch_relation}")

                        format_locations = []
                        for format_spec in self._s3_format_specs():
                            format_name = format_spec["name"]
                            extension = format_spec["extension"]
                            s3_prefix = self._s3_prefix(object_prefix, format_name, table_name)
                            object_key = (
                                f"generated/{target_name}/{format_name}/{table_name}/"
                                f"part-{batch_index:05d}.{extension}"
                            )
                            local_path = (
                                temp_dir_path
                                / format_name
                                / table_name
                                / f"part-{batch_index:05d}.{extension}"
                            )
                            local_path.parent.mkdir(parents=True, exist_ok=True)
                            connection.execute(
                                "COPY ("
                                f"SELECT * FROM {batch_relation} ORDER BY {order_by}"
                                f") TO {sql_literal(local_path.as_posix())} "
                                f"({format_spec['copy_options']})"
                            )
                            upload_s3_file(
                                s3_upload_client,
                                local_path=local_path,
                                bucket=bucket_name,
                                key=object_key,
                            )
                            local_path.unlink(missing_ok=True)
                            format_locations.append(s3_prefix)

                        connection.execute(f"DROP TABLE IF EXISTS {batch_relation}")
                        processed_rows += end_row - start_row
                        written_targets = update_generation_target_status(
                            written_targets,
                            postgres_relation_name,
                            *format_locations,
                            status="written",
                        )
                        context.report(
                            progress=processed_rows / total_generated_rows,
                            progress_label=f"Writing {table_name} batch {batch_index} / {batch_count}",
                            message=(
                                f"Wrote {end_row:,} row(s) for {table_name} into PostgreSQL OLTP "
                                "and S3 Parquet, CSV, and JSONL."
                            ),
                            target_name=target_name,
                            target_relation=primary_relation_name,
                            target_path=object_prefix,
                            written_targets=written_targets,
                            generated_rows=processed_rows,
                            generated_size_gb=approximate_size_gb(
                                processed_rows,
                                self.approximate_row_bytes,
                            ),
                        )

                    for format_spec in self._s3_format_specs():
                        format_name = format_spec["name"]
                        extension = format_spec["extension"]
                        view_name = self._s3_view_name(table_name, format_name)
                        s3_relation = qualified_name(s3_schema, view_name)
                        scan_path = (
                            f"{object_prefix}/{format_name}/{table_name}/*.{extension}"
                        )
                        s3_query = duckdb_scan_query(format_spec["scan_format"], [scan_path])
                        connection.execute(f"CREATE OR REPLACE VIEW {s3_relation} AS {s3_query}")

            return DataGeneratorResult(
                target_name=target_name,
                target_relation=primary_relation_name,
                target_path=object_prefix,
                written_targets=written_targets,
                generated_rows=processed_rows,
                generated_size_gb=approximate_size_gb(processed_rows, self.approximate_row_bytes),
                message=(
                    f"Generated {processed_rows:,} MWA row(s) across 2 PostgreSQL tables and "
                    "6 S3-backed views for Parquet, CSV, and JSONL comparison."
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
                written_targets=[],
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
            raise ValueError("This MWA generation job has no target path to clean.")

        bucket, _prefix = parse_s3_url(job.target_path)
        s3_schema = s3_bucket_schema_name(bucket)
        connection = context.connect()

        try:
            context.report(message=f"Cleaning MWA targets from PostgreSQL OLTP and s3://{bucket}...")
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


GENERATOR = MwaAbrechnungMultiFormatDataGenerator()

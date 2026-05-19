from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from ..backend.s3_storage import (
    delete_s3_bucket,
    duckdb_scan_query,
    ensure_s3_bucket,
    parse_s3_url,
    remove_s3_bucket,
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
from .helpers import (
    approximate_size_gb,
    loader_tree_bucket_name,
    qualified_name,
    sql_literal,
)


KOSTENBELEGE_3_1_S3_SCHEMA = "s3_3_1_imports_a08e7385"
KOSTENBELEGE_3_1_TABLES = ("kbkp_2019", "kbpo_2019", "kbhp_2019", "dim_kalender")

KBKP_COLUMNS = (
    "KBKP_Belegnummer BIGINT",
    "DOCO_Belegart VARCHAR",
    "KBKP_BelegDt DATE",
    "KBKP_BuchungDt DATE",
    "KBKP_ErstellungVon VARCHAR",
    "KBKP_StorniertBelegNummer BIGINT",
    "KBKP_StornoBelegNummer BIGINT",
    "DOCO_BelegHerkunft VARCHAR",
    "DOCO_Buchunggrund VARCHAR",
    "KBKP_TechBeginnDt DATE",
    "KBKP_TechEndeDt DATE",
)

KBPO_COLUMNS = (
    "KBPO_PositionId BIGINT",
    "KBKP_AusgleichBelegnummer BIGINT",
    "KBPO_VtgKtoWiederholPos INTEGER",
    "KBPO_VtgKtoPositionNr INTEGER",
    "KBPO_Teilposition INTEGER",
    "GEFA_GeschaeftFall VARCHAR",
    "PART_Partner VARCHAR",
    "KBPO_KtoFindMerkmal VARCHAR",
    "DOCO_Hauptvorgang VARCHAR",
    "DOCO_Teilvorgang VARCHAR",
    "DOCO_Belegtyp VARCHAR",
    "DOCO_VtrKtoTyp VARCHAR",
    "DOCO_Waehrung VARCHAR",
    "DOCO_FormArt VARCHAR",
    "KBPO_GesamtBetrag DECIMAL(18,2)",
    "KBPO_TWhrBetrag DECIMAL(18,2)",
    "KBPO_HbWaehrung VARCHAR",
    "KBPO_HbBetrag DECIMAL(18,2)",
    "KBPO_HWhrBetrag1 DECIMAL(18,2)",
    "KBPO_Umrechnungkurs DECIMAL(18,6)",
    "KBPO_NettoFaelligkeitDT DATE",
    "VTGP_VtrGegenstand VARCHAR",
    "KBPO_VtrKtoNummer VARCHAR",
    "KBPO_AusgleichStatus VARCHAR",
    "KBPO_Ausgleichgrund VARCHAR",
    "KBPO_AusgleichDt DATE",
    "KBPO_AusgleichBuchungDt DATE",
    "KBPO_HBSachkto VARCHAR",
    "KBPO_Beschreibung VARCHAR",
    "DOCO_SteuerCd VARCHAR",
    "KBPO_WertInternDt DATE",
    "KBPO_Bankverbindung VARCHAR",
    "DOCO_RecordArt VARCHAR",
    "KBPO_TechBeginnDt DATE",
    "KBPO_TechEndeDt DATE",
)

KBHP_COLUMNS = (
    "KBHP_Id BIGINT",
    "KBKP_BelegNummer BIGINT",
    "KBHP_VTGKtoPositionNr INTEGER",
    "KBHP_SachKto VARCHAR",
    "KBHP_HBAbstimmschluessel VARCHAR",
    "KBHP_TechBeginnDt DATE",
    "KBHP_TechEndeDt DATE",
)

DIM_KALENDER_COLUMNS = ("Datum DATE",)


def kbkp_2019_select(start_row: int, end_row: int) -> str:
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        )
        SELECT
            row_id + 1 AS KBKP_Belegnummer,
            CASE row_id % 5
                WHEN 0 THEN 'RE'
                WHEN 1 THEN 'ZA'
                WHEN 2 THEN 'GU'
                WHEN 3 THEN 'ST'
                ELSE 'UM'
            END AS DOCO_Belegart,
            DATE '2019-01-01' + CAST((row_id % 365) AS INTEGER) AS KBKP_BelegDt,
            DATE '2019-01-02' + CAST((row_id % 365) AS INTEGER) AS KBKP_BuchungDt,
            'USR' || LPAD(CAST(row_id % 1000 AS VARCHAR), 4, '0') AS KBKP_ErstellungVon,
            CASE WHEN row_id % 97 = 0 THEN row_id ELSE NULL END AS KBKP_StorniertBelegNummer,
            CASE WHEN row_id % 113 = 0 THEN row_id + 1000000 ELSE NULL END AS KBKP_StornoBelegNummer,
            CASE row_id % 4
                WHEN 0 THEN 'POLICE'
                WHEN 1 THEN 'CLAIM'
                WHEN 2 THEN 'COLLECTION'
                ELSE 'MIGRATION'
            END AS DOCO_BelegHerkunft,
            CASE row_id % 6
                WHEN 0 THEN 'PREMIUM'
                WHEN 1 THEN 'REFUND'
                WHEN 2 THEN 'CLAIM'
                WHEN 3 THEN 'TAX'
                WHEN 4 THEN 'FEE'
                ELSE 'CLEARING'
            END AS DOCO_Buchunggrund,
            DATE '2018-01-01' AS KBKP_TechBeginnDt,
            DATE '2099-12-31' AS KBKP_TechEndeDt
        FROM base
    """


def kbpo_2019_select(start_row: int, end_row: int, document_count: int) -> str:
    document_mod = max(1, int(document_count))
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                ((row_id % {document_mod}) + 1) AS belegnummer,
                ((row_id % 4) + 1) AS position_nr,
                ROUND(100.0 + (((row_id * 131) % 900000) / 10.0), 2) AS betrag
            FROM base
        )
        SELECT
            row_id + 1 AS KBPO_PositionId,
            belegnummer AS KBKP_AusgleichBelegnummer,
            CAST(row_id % 3 AS INTEGER) AS KBPO_VtgKtoWiederholPos,
            CAST(position_nr AS INTEGER) AS KBPO_VtgKtoPositionNr,
            CAST(row_id % 2 AS INTEGER) AS KBPO_Teilposition,
            'GF' || LPAD(CAST(row_id % 200 AS VARCHAR), 4, '0') AS GEFA_GeschaeftFall,
            'P' || LPAD(CAST((row_id * 17) % 500000 AS VARCHAR), 8, '0') AS PART_Partner,
            'KF' || LPAD(CAST(row_id % 90 AS VARCHAR), 3, '0') AS KBPO_KtoFindMerkmal,
            'HV' || LPAD(CAST(row_id % 60 AS VARCHAR), 3, '0') AS DOCO_Hauptvorgang,
            'TV' || LPAD(CAST(row_id % 80 AS VARCHAR), 3, '0') AS DOCO_Teilvorgang,
            CASE row_id % 4
                WHEN 0 THEN 'OR'
                WHEN 1 THEN 'AU'
                WHEN 2 THEN 'ST'
                ELSE 'KO'
            END AS DOCO_Belegtyp,
            CASE row_id % 3
                WHEN 0 THEN 'PR'
                WHEN 1 THEN 'CL'
                ELSE 'IN'
            END AS DOCO_VtrKtoTyp,
            CASE row_id % 5
                WHEN 0 THEN 'CHF'
                WHEN 1 THEN 'EUR'
                WHEN 2 THEN 'USD'
                WHEN 3 THEN 'GBP'
                ELSE 'CHF'
            END AS DOCO_Waehrung,
            CASE row_id % 4
                WHEN 0 THEN 'FORM-A'
                WHEN 1 THEN 'FORM-B'
                WHEN 2 THEN 'FORM-C'
                ELSE 'FORM-D'
            END AS DOCO_FormArt,
            CAST(betrag AS DECIMAL(18,2)) AS KBPO_GesamtBetrag,
            CAST(ROUND(betrag * 0.98, 2) AS DECIMAL(18,2)) AS KBPO_TWhrBetrag,
            'CHF' AS KBPO_HbWaehrung,
            CAST(ROUND(betrag * 1.01, 2) AS DECIMAL(18,2)) AS KBPO_HbBetrag,
            CAST(ROUND(betrag * 1.02, 2) AS DECIMAL(18,2)) AS KBPO_HWhrBetrag1,
            CAST(ROUND(0.85 + ((row_id % 40) / 100.0), 6) AS DECIMAL(18,6)) AS KBPO_Umrechnungkurs,
            DATE '2019-02-01' + CAST((row_id % 365) AS INTEGER) AS KBPO_NettoFaelligkeitDT,
            'VTG' || LPAD(CAST(row_id % 100000 AS VARCHAR), 8, '0') AS VTGP_VtrGegenstand,
            'VK' || LPAD(CAST(row_id % 100000 AS VARCHAR), 8, '0') AS KBPO_VtrKtoNummer,
            CASE row_id % 4
                WHEN 0 THEN 'OPEN'
                WHEN 1 THEN 'CLEARED'
                WHEN 2 THEN 'PARTIAL'
                ELSE 'REVERSED'
            END AS KBPO_AusgleichStatus,
            CASE row_id % 5
                WHEN 0 THEN 'MANUAL'
                WHEN 1 THEN 'AUTO'
                WHEN 2 THEN 'PAYMENT'
                WHEN 3 THEN 'REVERSAL'
                ELSE 'MIGRATION'
            END AS KBPO_Ausgleichgrund,
            DATE '2019-03-01' + CAST((row_id % 365) AS INTEGER) AS KBPO_AusgleichDt,
            DATE '2019-03-02' + CAST((row_id % 365) AS INTEGER) AS KBPO_AusgleichBuchungDt,
            'NB' || LPAD(CAST(row_id % 1000 AS VARCHAR), 5, '0') AS KBPO_HBSachkto,
            'Generated Kostenbeleg position ' || CAST(row_id + 1 AS VARCHAR) AS KBPO_Beschreibung,
            CASE row_id % 4
                WHEN 0 THEN 'A0'
                WHEN 1 THEN 'A1'
                WHEN 2 THEN 'B0'
                ELSE 'C0'
            END AS DOCO_SteuerCd,
            DATE '2019-01-15' + CAST((row_id % 365) AS INTEGER) AS KBPO_WertInternDt,
            'IBAN-CH' || LPAD(CAST(row_id % 1000000 AS VARCHAR), 10, '0') AS KBPO_Bankverbindung,
            CASE row_id % 3
                WHEN 0 THEN 'ACTIVE'
                WHEN 1 THEN 'HIST'
                ELSE 'TECH'
            END AS DOCO_RecordArt,
            DATE '2018-01-01' AS KBPO_TechBeginnDt,
            DATE '2099-12-31' AS KBPO_TechEndeDt
        FROM prepared
    """


def kbhp_2019_select(start_row: int, end_row: int, document_count: int) -> str:
    document_mod = max(1, int(document_count))
    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        )
        SELECT
            row_id + 1 AS KBHP_Id,
            ((row_id % {document_mod}) + 1) AS KBKP_BelegNummer,
            CAST((row_id % 4) + 1 AS INTEGER) AS KBHP_VTGKtoPositionNr,
            'HB' || LPAD(CAST(row_id % 5000 AS VARCHAR), 6, '0') AS KBHP_SachKto,
            'ABS' || LPAD(CAST(row_id % 800 AS VARCHAR), 5, '0') AS KBHP_HBAbstimmschluessel,
            DATE '2018-01-01' AS KBHP_TechBeginnDt,
            DATE '2099-12-31' AS KBHP_TechEndeDt
        FROM base
    """


def dim_kalender_select(start_row: int, end_row: int) -> str:
    return f"""
        SELECT
            CURRENT_DATE - CAST(30 AS INTEGER) + CAST(row_id AS INTEGER) AS Datum
        FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
    """


class Kostenbelege31DataGenerator(DataGenerator):
    generator_id = "kostenbelege_3_1_multi_source_loader"
    title = "Kostenbelege Multi-Source Loader (3.1)"
    description = (
        "Generates KBKP, KBPO, KBHP, and calendar reference data for the Kostenbelege 3.1 query "
        "across PostgreSQL OLTP, PostgreSQL OLAP, and S3-backed Parquet."
    )
    target_kind = "contest"
    tree_path = ("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)")
    default_size_gb = 1.0
    min_size_gb = 0.01
    max_size_gb = 128.0
    approximate_row_bytes = 880
    default_target_name = "kostenbelege_3_1"
    tags = ("postgres", "oltp", "olap", "s3", "parquet", "kostenbelege", "3.1")

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return loader_tree_bucket_name(self.tree_path, "kostenbelege-3-1")

    def _row_counts(self, total_rows: int) -> dict[str, int]:
        kbkp_rows = max(1000, total_rows // 4)
        kbpo_rows = max(1000, total_rows // 2)
        kbhp_rows = max(1000, kbpo_rows)
        return {
            "kbkp_2019": kbkp_rows,
            "kbpo_2019": kbpo_rows,
            "kbhp_2019": kbhp_rows,
            "dim_kalender": 61,
        }

    def _table_specs(self, row_counts: dict[str, int]) -> list[dict[str, object]]:
        document_count = row_counts["kbkp_2019"]
        return [
            {
                "name": "kbkp_2019",
                "columns": KBKP_COLUMNS,
                "row_count": row_counts["kbkp_2019"],
                "select_builder": kbkp_2019_select,
            },
            {
                "name": "kbpo_2019",
                "columns": KBPO_COLUMNS,
                "row_count": row_counts["kbpo_2019"],
                "select_builder": lambda start, end: kbpo_2019_select(start, end, document_count),
            },
            {
                "name": "kbhp_2019",
                "columns": KBHP_COLUMNS,
                "row_count": row_counts["kbhp_2019"],
                "select_builder": lambda start, end: kbhp_2019_select(start, end, document_count),
            },
            {
                "name": "dim_kalender",
                "columns": DIM_KALENDER_COLUMNS,
                "row_count": row_counts["dim_kalender"],
                "select_builder": dim_kalender_select,
            },
        ]

    def _written_targets(self, *, root_prefix: str) -> list:
        targets = []
        for table_name in KOSTENBELEGE_3_1_TABLES:
            targets.append(
                generation_target(
                    target_kind="postgres_table",
                    label="PostgreSQL OLTP table",
                    location=f"pg_oltp.public.{table_name}",
                )
            )
            targets.append(
                generation_target(
                    target_kind="postgres_table",
                    label="PostgreSQL OLAP table",
                    location=f"pg_olap.public.{table_name}",
                )
            )
            targets.append(
                generation_target(
                    target_kind="s3_prefix",
                    label="Dedicated S3 Parquet path",
                    location=f"{root_prefix}/parquet/{table_name}",
                )
            )
        return targets

    def _drop_postgres_tables(self, connection) -> int:
        dropped = 0
        for table_name in reversed(KOSTENBELEGE_3_1_TABLES):
            for catalog in ("pg_oltp", "pg_olap"):
                connection.execute(f"DROP TABLE IF EXISTS {qualified_name(catalog, 'public', table_name)}")
                dropped += 1
        return dropped

    def _drop_s3_views(self, connection) -> int:
        dropped = 0
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(KOSTENBELEGE_3_1_S3_SCHEMA)}")
        for table_name in reversed(KOSTENBELEGE_3_1_TABLES):
            connection.execute(f"DROP VIEW IF EXISTS {qualified_name(KOSTENBELEGE_3_1_S3_SCHEMA, table_name)}")
            dropped += 1
        return dropped

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.pg_oltp_database or not settings.pg_olap_database:
            raise ValueError(
                "PG_OLTP_DATABASE and PG_OLAP_DATABASE must be configured before running the Kostenbelege 3.1 loader."
            )
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the Kostenbelege 3.1 loader.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        total_rows = estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes)
        row_counts = self._row_counts(total_rows)
        table_specs = self._table_specs(row_counts)
        total_generated_rows = sum(int(spec["row_count"]) for spec in table_specs)
        batch_rows = 100_000
        target_name = generated_name(self.default_target_name, context.job_id)
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        object_prefix = f"s3://{bucket_name}/generated/{target_name}"
        primary_relation_name = "pg_oltp.public.kbkp_2019 + pg_olap.public.kbkp_2019"
        context.report(
            progress=0.0,
            progress_label="Opening loader connection...",
            message=(
                "Opening an exclusive DuckDB worker connection before preparing "
                "Kostenbelege 3.1 targets."
            ),
        )
        connection = context.connect()
        context.report(
            progress=0.0,
            progress_label="Opening S3 client...",
            message="Opening the S3 client for Kostenbelege 3.1 Parquet uploads.",
        )
        s3_upload_client = s3_client(settings)
        written_targets = self._written_targets(root_prefix=object_prefix)

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing targets...",
                message=(
                    "Creating Kostenbelege 3.1 tables in PostgreSQL OLTP, PostgreSQL OLAP, "
                    f"and S3-backed Parquet under {object_prefix}."
                ),
                target_name=target_name,
                target_relation=primary_relation_name,
                target_path=object_prefix,
                written_targets=written_targets,
            )
            context.raise_if_cancelled()
            context.report(
                progress=0.0,
                progress_label="Ensuring S3 bucket...",
                message=f"Ensuring loader bucket s3://{bucket_name} exists.",
            )
            ensure_s3_bucket(settings, bucket_name)
            context.raise_if_cancelled()
            context.report(
                progress=0.0,
                progress_label="Dropping old PostgreSQL tables...",
                message="Dropping previous Kostenbelege 3.1 OLTP and OLAP tables.",
            )
            self._drop_postgres_tables(connection)
            context.raise_if_cancelled()
            context.report(
                progress=0.0,
                progress_label="Dropping old S3 views...",
                message="Dropping previous Kostenbelege 3.1 S3 Parquet views.",
            )
            self._drop_s3_views(connection)
            context.raise_if_cancelled()
            context.report(
                progress=0.0,
                progress_label="Cleaning existing S3 objects...",
                message=f"Deleting existing objects from loader bucket s3://{bucket_name}.",
            )
            delete_s3_bucket(settings, bucket_name)
            context.raise_if_cancelled()
            context.report(
                progress=0.0,
                progress_label="Rechecking S3 bucket...",
                message=f"Rechecking loader bucket s3://{bucket_name} after cleanup.",
            )
            ensure_s3_bucket(settings, bucket_name)

            processed_rows = 0
            with TemporaryDirectory(prefix=f"bdw-{self.generator_id}-{context.job_id[:8]}-") as temp_dir:
                temp_dir_path = Path(temp_dir)
                for spec in table_specs:
                    table_name = str(spec["name"])
                    row_count = int(spec["row_count"])
                    columns = tuple(spec["columns"])
                    select_builder = spec["select_builder"]
                    batch_count = max(1, (row_count + batch_rows - 1) // batch_rows)
                    oltp_relation = qualified_name("pg_oltp", "public", table_name)
                    olap_relation = qualified_name("pg_olap", "public", table_name)
                    s3_relation = qualified_name(KOSTENBELEGE_3_1_S3_SCHEMA, table_name)
                    oltp_relation_name = f"pg_oltp.public.{table_name}"
                    olap_relation_name = f"pg_olap.public.{table_name}"
                    s3_prefix = f"{object_prefix}/parquet/{table_name}"

                    context.raise_if_cancelled()
                    context.report(
                        progress=processed_rows / total_generated_rows,
                        progress_label=f"Creating {table_name} tables...",
                        message=f"Creating OLTP and OLAP tables for {table_name}.",
                        generated_rows=processed_rows,
                        generated_size_gb=approximate_size_gb(processed_rows, self.approximate_row_bytes),
                    )
                    connection.execute(f"CREATE TABLE {oltp_relation} ({', '.join(columns)})")
                    connection.execute(f"CREATE TABLE {olap_relation} ({', '.join(columns)})")

                    for batch_index, start_row in enumerate(range(0, row_count, batch_rows), start=1):
                        context.raise_if_cancelled()
                        end_row = min(row_count, start_row + batch_rows)
                        select_sql = select_builder(start_row, end_row)
                        object_key = (
                            f"generated/{target_name}/parquet/{table_name}/"
                            f"part-{batch_index:05d}.parquet"
                        )
                        local_parquet_path = (
                            temp_dir_path / "parquet" / table_name / f"part-{batch_index:05d}.parquet"
                        )
                        local_parquet_path.parent.mkdir(parents=True, exist_ok=True)

                        context.report(
                            progress=processed_rows / total_generated_rows,
                            progress_label=f"Writing {table_name} batch {batch_index} / {batch_count}",
                            message=(
                                f"Writing rows {start_row + 1:,}-{end_row:,} for {table_name} "
                                f"into OLTP, OLAP, and {s3_prefix}."
                            ),
                            target_name=target_name,
                            target_relation=primary_relation_name,
                            target_path=object_prefix,
                            written_targets=written_targets,
                            generated_rows=processed_rows,
                            generated_size_gb=approximate_size_gb(processed_rows, self.approximate_row_bytes),
                        )
                        connection.execute(f"INSERT INTO {oltp_relation} {select_sql}")
                        connection.execute(f"INSERT INTO {olap_relation} {select_sql}")
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
                        written_targets = update_generation_target_status(
                            written_targets,
                            oltp_relation_name,
                            olap_relation_name,
                            s3_prefix,
                            status="written",
                        )
                        context.report(
                            progress=processed_rows / total_generated_rows,
                            progress_label=f"Writing {table_name} batch {batch_index} / {batch_count}",
                            message=(
                                f"Wrote {end_row:,} row(s) for {table_name} into OLTP, OLAP, "
                                f"and {s3_prefix}."
                            ),
                            target_name=target_name,
                            target_relation=primary_relation_name,
                            target_path=object_prefix,
                            written_targets=written_targets,
                            generated_rows=processed_rows,
                            generated_size_gb=approximate_size_gb(processed_rows, self.approximate_row_bytes),
                        )

                    context.raise_if_cancelled()
                    context.report(
                        progress=processed_rows / total_generated_rows,
                        progress_label=f"Creating {table_name} S3 view...",
                        message=f"Creating the DuckDB S3 Parquet view for {table_name}.",
                        target_name=target_name,
                        target_relation=primary_relation_name,
                        target_path=object_prefix,
                        written_targets=written_targets,
                        generated_rows=processed_rows,
                        generated_size_gb=approximate_size_gb(processed_rows, self.approximate_row_bytes),
                    )
                    s3_query = duckdb_scan_query("parquet", [f"{s3_prefix}/*.parquet"])
                    connection.execute(f"CREATE OR REPLACE VIEW {s3_relation} AS {s3_query}")

            return DataGeneratorResult(
                target_name=target_name,
                target_relation=primary_relation_name,
                target_path=object_prefix,
                written_targets=written_targets,
                generated_rows=processed_rows,
                generated_size_gb=approximate_size_gb(processed_rows, self.approximate_row_bytes),
                message=(
                    f"Generated {processed_rows:,} Kostenbelege 3.1 row(s) across OLTP, OLAP, "
                    "and S3 Parquet targets."
                ),
            )
        except DataGenerationCancelled:
            self._cleanup_partial_output(connection=connection, target_path=object_prefix, context=context)
            context.report(
                message="Cancellation requested. Partial OLTP, OLAP, S3 views, and S3 objects were removed.",
                target_relation="",
                target_path="",
                written_targets=[],
                generated_rows=0,
                generated_size_gb=0.0,
            )
            raise
        except Exception:
            self._cleanup_partial_output(connection=connection, target_path=object_prefix, context=context)
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        if not job.target_path:
            raise ValueError("This Kostenbelege 3.1 generation job has no target path to clean.")

        bucket, _prefix = parse_s3_url(job.target_path)
        connection = context.connect()

        try:
            context.report(message=f"Cleaning Kostenbelege 3.1 targets from PostgreSQL and s3://{bucket}...")
            dropped_tables = self._drop_postgres_tables(connection)
            dropped_views = self._drop_s3_views(connection)
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
        target_path: str,
        context: DataGeneratorContext,
    ) -> None:
        try:
            self._drop_postgres_tables(connection)
        except Exception:
            pass
        try:
            self._drop_s3_views(connection)
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


GENERATOR = Kostenbelege31DataGenerator()

from __future__ import annotations

from pathlib import Path, PurePosixPath
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
    update_generation_target_status,
)
from .helpers import approximate_size_gb, loader_tree_bucket_name, qualified_name, sql_literal


KOSTENBELEGE_FACT_BUILDER_TREE_PATH = ("PoC Tests", "General Functionalities")
KOSTENBELEGE_FACT_BUILDER_PIPELINE_TREE_PATH = ("PoC Tests", "Data Pipelines")
KOSTENBELEGE_FACT_BUILDER_BUCKET = loader_tree_bucket_name(
    (*KOSTENBELEGE_FACT_BUILDER_PIPELINE_TREE_PATH, "kostenbelege-fact-builder"),
    "kostenbelege-fact-builder",
)
KOSTENBELEGE_FACT_BUILDER_SCHEMA = s3_bucket_schema_name(KOSTENBELEGE_FACT_BUILDER_BUCKET)
KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID = "kostenbelege_fact_builder_s3_loader"
KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID = "kostenbelege-fact-builder-s3-demo"
KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID = "kostenbelege-fact-builder-s3-pipeline-demo"
KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX = "generated/kostenbelege_fact_builder/source"
KOSTENBELEGE_FACT_BUILDER_RESULT_PREFIX = "generated/kostenbelege_fact_builder/result-sets"

KBKP_SOURCE_KEY = f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbkpfull.parquet"
KBHP_SOURCE_KEY = f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbhpfull.parquet"
KBPO_SOURCE_KEYS = (
    f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbpo_2018undvorher.parquet",
    f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbpo_2019.parquet",
    f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbpo2020.parquet",
    f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbpo2021.parquet",
    f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbpo2022.parquet",
    f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbpo2023.parquet",
    f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbpo2024.parquet",
    f"{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}/kbpo2025.parquet",
)
RESULT_SET_KEYS = {
    "kbkp_today": f"{KOSTENBELEGE_FACT_BUILDER_RESULT_PREFIX}/kbkp_today.parquet",
    "kbpo_today": f"{KOSTENBELEGE_FACT_BUILDER_RESULT_PREFIX}/kbpo_today.parquet",
    "kbhp_today": f"{KOSTENBELEGE_FACT_BUILDER_RESULT_PREFIX}/kbhp_today.parquet",
    "kbhp_pos1": f"{KOSTENBELEGE_FACT_BUILDER_RESULT_PREFIX}/kbhp_pos1.parquet",
    "fact_buchungsbelegposition": (
        f"{KOSTENBELEGE_FACT_BUILDER_RESULT_PREFIX}/fact_buchungsbelegposition.parquet"
    ),
    "fact_buchungsbelegposition_metrics": (
        f"{KOSTENBELEGE_FACT_BUILDER_RESULT_PREFIX}/fact_buchungsbelegposition_metrics.parquet"
    ),
}

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
    "KBKP_Belegnummer BIGINT",
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


def source_path(key: str) -> str:
    return f"s3://{KOSTENBELEGE_FACT_BUILDER_BUCKET}/{key}"


def result_path(result_name: str) -> str:
    return source_path(RESULT_SET_KEYS[result_name])


def kbkp_full_select(start_row: int, end_row: int) -> str:
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
            DATE '2025-01-01' + CAST((row_id % 365) AS INTEGER) AS KBKP_BelegDt,
            DATE '2025-01-02' + CAST((row_id % 365) AS INTEGER) AS KBKP_BuchungDt,
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
            DATE '2020-01-01' AS KBKP_TechBeginnDt,
            DATE '2099-12-31' AS KBKP_TechEndeDt
        FROM base
    """


def kbpo_slice_select(
    start_row: int,
    end_row: int,
    document_count: int,
    *,
    row_offset: int = 0,
) -> str:
    document_mod = max(1, int(document_count))
    offset = int(row_offset)
    return f"""
        WITH base AS (
            SELECT row_id + {offset} AS row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                ((row_id % {document_mod}) + 1) AS belegnummer,
                (((row_id * 3 + 7) % {document_mod}) + 1) AS ausgleich_belegnummer,
                ((row_id % 4) + 1) AS position_nr,
                ROUND(100.0 + (((row_id * 131) % 900000) / 10.0), 2) AS betrag
            FROM base
        )
        SELECT
            row_id + 1 AS KBPO_PositionId,
            belegnummer AS KBKP_Belegnummer,
            ausgleich_belegnummer AS KBKP_AusgleichBelegnummer,
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
            DATE '2025-02-01' + CAST((row_id % 365) AS INTEGER) AS KBPO_NettoFaelligkeitDT,
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
            DATE '2025-03-01' + CAST((row_id % 365) AS INTEGER) AS KBPO_AusgleichDt,
            DATE '2025-03-02' + CAST((row_id % 365) AS INTEGER) AS KBPO_AusgleichBuchungDt,
            'NB' || LPAD(CAST(row_id % 1000 AS VARCHAR), 5, '0') AS KBPO_HBSachkto,
            'Generated Kostenbeleg position ' || CAST(row_id + 1 AS VARCHAR) AS KBPO_Beschreibung,
            CASE row_id % 4
                WHEN 0 THEN 'A0'
                WHEN 1 THEN 'A1'
                WHEN 2 THEN 'B0'
                ELSE 'C0'
            END AS DOCO_SteuerCd,
            DATE '2025-01-15' + CAST((row_id % 365) AS INTEGER) AS KBPO_WertInternDt,
            'IBAN-CH' || LPAD(CAST(row_id % 1000000 AS VARCHAR), 10, '0') AS KBPO_Bankverbindung,
            CASE row_id % 3
                WHEN 0 THEN 'ACTIVE'
                WHEN 1 THEN 'HIST'
                ELSE 'TECH'
            END AS DOCO_RecordArt,
            DATE '2020-01-01' AS KBPO_TechBeginnDt,
            DATE '2099-12-31' AS KBPO_TechEndeDt
        FROM prepared
    """


def kbhp_full_select(start_row: int, end_row: int, document_count: int) -> str:
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
            DATE '2020-01-01' AS KBHP_TechBeginnDt,
            DATE '2099-12-31' AS KBHP_TechEndeDt
        FROM base
    """


class KostenbelegeFactBuilderSampleDataGenerator(DataGenerator):
    generator_id = KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID
    title = "Kostenbelege Fact Builder S3 Loader"
    description = (
        "Generates deterministic S3 Parquet test data for the linked six-cell "
        "Kostenbelege exploration and pipeline notebooks."
    )
    target_kind = "s3"
    tree_path = KOSTENBELEGE_FACT_BUILDER_PIPELINE_TREE_PATH
    default_size_gb = 0.005
    min_size_gb = 0.001
    max_size_gb = 4.0
    approximate_row_bytes = 512
    default_target_name = "kostenbelege_fact_builder"
    tags = (
        "s3",
        "parquet",
        "duckdb",
        "result-storage",
        "pipeline",
        "kostenbelege",
        "fact",
    )

    def _loader_bucket_name(self, base_bucket: str) -> str:
        return KOSTENBELEGE_FACT_BUILDER_BUCKET

    def _written_targets(self) -> list:
        return [
            generation_target(
                target_kind="s3_object",
                label="KBKP source",
                location=source_path(KBKP_SOURCE_KEY),
            ),
            *[
                generation_target(
                    target_kind="s3_object",
                    label=f"KBPO source {index}",
                    location=source_path(key),
                )
                for index, key in enumerate(KBPO_SOURCE_KEYS, start=1)
            ],
            generation_target(
                target_kind="s3_object",
                label="KBHP source",
                location=source_path(KBHP_SOURCE_KEY),
            ),
            *[
                generation_target(
                    target_kind="s3_object",
                    label=f"Notebook result {name}",
                    location=source_path(key),
                )
                for name, key in RESULT_SET_KEYS.items()
            ],
        ]

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        settings = context.settings
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET must be configured before running the Kostenbelege fact-builder loader.")

        requested_size_gb = self.normalize_size_gb(context.requested_size_gb)
        document_rows = max(64, estimated_rows_for_size(requested_size_gb, self.approximate_row_bytes))
        kbpo_rows = max(document_rows * 2, 128)
        kbhp_rows = max(document_rows * 4, 256)
        bucket_name = self._loader_bucket_name(settings.s3_bucket)
        schema_name = s3_bucket_schema_name(bucket_name)
        connection = context.connect()
        upload_client = s3_client(settings)
        written_targets = self._written_targets()
        all_source_locations = [
            source_path(KBKP_SOURCE_KEY),
            *(source_path(key) for key in KBPO_SOURCE_KEYS),
            source_path(KBHP_SOURCE_KEY),
        ]

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing Kostenbelege fact-builder data...",
                message=f"Writing sample S3 Parquet objects to s3://{bucket_name}.",
                target_name=self.default_target_name,
                target_path=f"s3://{bucket_name}/{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}",
                written_targets=written_targets,
            )
            ensure_s3_bucket(settings, bucket_name)
            delete_s3_bucket(settings, bucket_name)
            ensure_s3_bucket(settings, bucket_name)
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(schema_name)}")

            with TemporaryDirectory(prefix=f"bdw-{self.generator_id}-{context.job_id[:8]}-") as temp_dir:
                temp_dir_path = Path(temp_dir)
                write_specs = [
                    (KBKP_SOURCE_KEY, kbkp_full_select(0, document_rows)),
                    (KBHP_SOURCE_KEY, kbhp_full_select(0, kbhp_rows, document_rows)),
                ]
                kbpo_batch_size = max(1, (kbpo_rows + len(KBPO_SOURCE_KEYS) - 1) // len(KBPO_SOURCE_KEYS))
                for index, key in enumerate(KBPO_SOURCE_KEYS):
                    start_row = index * kbpo_batch_size
                    end_row = min(kbpo_rows, start_row + kbpo_batch_size)
                    write_specs.append(
                        (
                            key,
                            kbpo_slice_select(
                                0,
                                max(0, end_row - start_row),
                                document_rows,
                                row_offset=start_row,
                            ),
                        )
                    )

                total_specs = len(write_specs)
                for index, (object_key, select_sql) in enumerate(write_specs, start=1):
                    context.raise_if_cancelled()
                    local_path = temp_dir_path / f"part-{index:02d}.parquet"
                    connection.execute(
                        "COPY ("
                        f"{select_sql}"
                        f") TO {sql_literal(local_path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    upload_s3_file(
                        upload_client,
                        local_path=local_path,
                        bucket=bucket_name,
                        key=object_key,
                    )
                    local_path.unlink(missing_ok=True)
                    written_targets = update_generation_target_status(
                        written_targets,
                        source_path(object_key),
                        status="written",
                    )
                    context.report(
                        progress=index / total_specs,
                        progress_label=f"Writing source object {index} / {total_specs}",
                        message=f"Wrote {source_path(object_key)}.",
                        target_name=self.default_target_name,
                        target_path=f"s3://{bucket_name}/{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}",
                        written_targets=written_targets,
                        generated_rows=document_rows + kbpo_rows + kbhp_rows,
                        generated_size_gb=approximate_size_gb(
                            document_rows + kbpo_rows + kbhp_rows,
                            self.approximate_row_bytes,
                        ),
                    )

            for view_name, location in {
                "kbkpfull": source_path(KBKP_SOURCE_KEY),
                "kbhpfull": source_path(KBHP_SOURCE_KEY),
            }.items():
                relation = qualified_name(schema_name, view_name)
                connection.execute(
                    f"CREATE OR REPLACE VIEW {relation} AS {duckdb_scan_query('parquet', [location])}"
                )
            for object_key in KBPO_SOURCE_KEYS:
                view_name = PurePosixPath(object_key).stem
                relation = qualified_name(schema_name, view_name)
                connection.execute(
                    f"CREATE OR REPLACE VIEW {relation} AS {duckdb_scan_query('parquet', [source_path(object_key)])}"
                )

            return DataGeneratorResult(
                target_name=self.default_target_name,
                target_path=f"s3://{bucket_name}/{KOSTENBELEGE_FACT_BUILDER_SOURCE_PREFIX}",
                written_targets=update_generation_target_status(
                    written_targets,
                    *all_source_locations,
                    status="written",
                ),
                generated_rows=document_rows + kbpo_rows + kbhp_rows,
                generated_size_gb=approximate_size_gb(
                    document_rows + kbpo_rows + kbhp_rows,
                    self.approximate_row_bytes,
                ),
                message=(
                    f"Generated Kostenbelege fact-builder sample data in s3://{bucket_name}. "
                    "Run one of the linked notebooks to build the stored or staged result sets."
                ),
            )
        except DataGenerationCancelled:
            self._cleanup_partial_output(connection=connection, context=context, bucket_name=bucket_name)
            context.report(
                message="Cancellation requested. Partial Kostenbelege fact-builder S3 output was removed.",
                target_path="",
                written_targets=[],
                generated_rows=0,
                generated_size_gb=0.0,
            )
            raise
        except Exception:
            self._cleanup_partial_output(connection=connection, context=context, bucket_name=bucket_name)
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        target_path = str(job.target_path or f"s3://{KOSTENBELEGE_FACT_BUILDER_BUCKET}")
        bucket, _prefix = parse_s3_url(target_path)
        schema_name = s3_bucket_schema_name(bucket)
        connection = context.connect()
        try:
            context.report(message=f"Cleaning Kostenbelege fact-builder bucket {bucket}...")
            for view_name in ("kbkpfull", "kbhpfull", *(PurePosixPath(key).stem for key in KBPO_SOURCE_KEYS)):
                connection.execute(f"DROP VIEW IF EXISTS {qualified_name(schema_name, view_name)}")
            deleted_objects = delete_s3_bucket(context.settings, bucket)
            bucket_deleted = remove_s3_bucket(context.settings, bucket)
            return DataGeneratorResult(
                target_name=str(job.target_name or self.default_target_name),
                generated_rows=0,
                generated_size_gb=0.0,
                message=(
                    f"Deleted {deleted_objects:,} generated object(s) from s3://{bucket}, "
                    f"{'removed the bucket' if bucket_deleted else 'left the empty bucket in place'}, "
                    "and removed the DuckDB views."
                ),
            )
        finally:
            connection.close()

    def _cleanup_partial_output(self, *, connection, context: DataGeneratorContext, bucket_name: str) -> None:
        schema_name = s3_bucket_schema_name(bucket_name)
        for view_name in ("kbkpfull", "kbhpfull", *(PurePosixPath(key).stem for key in KBPO_SOURCE_KEYS)):
            try:
                connection.execute(f"DROP VIEW IF EXISTS {qualified_name(schema_name, view_name)}")
            except Exception:
                pass
        try:
            delete_s3_bucket(context.settings, bucket_name)
        except Exception:
            pass
        try:
            remove_s3_bucket(context.settings, bucket_name)
        except Exception:
            pass


GENERATOR = KostenbelegeFactBuilderSampleDataGenerator()

from __future__ import annotations

from dataclasses import dataclass

from .notebook_presets import (
    _build_kostenbelege_3_1_optimized_sql,
    _build_kostenbelege_pipeline_canonical_output_sql,
    _build_kostenbelege_pipeline_headers_sql,
    _build_kostenbelege_pipeline_ledger_accounts_sql,
    _build_kostenbelege_pipeline_position_projection_sql,
    _build_kostenbelege_pipeline_positions_sql,
    _build_kostenbelege_pipeline_resolved_positions_sql,
)
from .pure_duckdb import (
    FACT_BUPO_TARGET,
    KBHP_FULL_PATH,
    KBKP_FULL_PATH,
    KALENDER_PATH,
    _fact_bupo_select_sql,
    _kbpo_union_by_name_relation,
    _query_1_sql,
    _read_parquet,
)
from .sql_utils import sql_literal


BENCHMARK_FACT_TARGET_PREFIX = "s3://core/pure-duckdb-benchmarks"


@dataclass(frozen=True, slots=True)
class PureDuckDBBenchmarkVariant:
    variant_id: str
    query_number: int
    statements: tuple[str, ...]
    change_summary: str
    sql_strategy: str
    output_layout: str
    duckdb_settings: tuple[str, ...]
    expected_effect: str
    validation_sql: str
    output_s3_url: str = ""

    @property
    def query_label(self) -> str:
        return f"Q{self.query_number}"

    @property
    def comparison_metadata(self) -> dict[str, str]:
        return {
            "variant_id": self.variant_id,
            "query": self.query_label,
            "change_summary": self.change_summary,
            "sql_strategy": self.sql_strategy,
            "output_layout": self.output_layout,
            "duckdb_settings": "; ".join(self.duckdb_settings) if self.duckdb_settings else "default",
            "expected_effect": self.expected_effect,
        }


def benchmark_fact_target(variant_id: str, *, dataset: bool = False) -> str:
    normalized = "".join(
        char if char.isalnum() or char in {"-", "_"} else "-"
        for char in variant_id.strip().lower()
    ).strip("-")
    suffix = "fact_bupo_dataset/" if dataset else "fact_bupo.parquet"
    return f"{BENCHMARK_FACT_TARGET_PREFIX}/{normalized}/{suffix}"


def fact_scan_sql(target: str) -> str:
    if target.rstrip().endswith("/"):
        return _read_parquet(f"{target.rstrip('/')}/*.parquet")
    return _read_parquet(target)


def _optimized_fact_select_sql(
    *,
    kbkp_relation: str | None = None,
    kbpo_relation: str | None = None,
    kbhp_relation: str | None = None,
    kalender_relation: str | None = None,
) -> str:
    return _build_kostenbelege_3_1_optimized_sql(
        kbkp_relation=kbkp_relation or _read_parquet(KBKP_FULL_PATH),
        kbpo_relation=kbpo_relation or _kbpo_union_by_name_relation(),
        kbhp_relation=kbhp_relation or _read_parquet(KBHP_FULL_PATH),
        kalender_relation=kalender_relation or _read_parquet(KALENDER_PATH),
    ).rstrip(";")


def _q1_from_fact_select(fact_select_sql: str) -> str:
    return f"""
WITH fact_bupo AS (
{fact_select_sql.rstrip().rstrip(";")}
)
SELECT
      COUNT(*) AS cnt
    , SUM(BetragHauswaehrung) AS total
FROM fact_bupo
WHERE Buchungsdatum >= DATE '2023-01-01'
  AND Buchungsdatum <  DATE '2024-01-01';
""".strip()


def _q1_aggregate_pushdown_sql() -> str:
    kbkp_relation = _read_parquet(KBKP_FULL_PATH)
    kbpo_relation = _kbpo_union_by_name_relation()
    kbhp_relation = _read_parquet(KBHP_FULL_PATH)
    kalender_relation = _read_parquet(KALENDER_PATH)
    return f"""
-- Narrow Q1 aggregate that preserves the current FACT branch multiplicity and sign rules.
WITH current_kalender AS (
    SELECT Datum
    FROM {kalender_relation}
    WHERE Datum = CURRENT_DATE
),
branch_amounts AS (
    SELECT
          KBKP.KBKP_BuchungDt AS Buchungsdatum
        , KBPO.KBPO_HWhrBetrag1 AS BetragHauswaehrung
    FROM {kbkp_relation} KBKP
    INNER JOIN current_kalender KALE
        ON KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
    INNER JOIN {kbpo_relation} KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer
        AND KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
    LEFT JOIN {kbhp_relation} KBHP
        ON  KBKP.KBKP_BelegNummer = KBHP.KBKP_BelegNummer
        AND KBHP.KBHP_VTGKtoPositionNr = KBPO.KBPO_VtgKtoPositionNr
        AND KALE.Datum BETWEEN KBHP.KBHP_TechBeginnDt AND KBHP.KBHP_TechEndeDt
    LEFT JOIN {kbhp_relation} KBHH
        ON  KBKP.KBKP_BelegNummer = KBHH.KBKP_BelegNummer
        AND KALE.Datum BETWEEN KBHH.KBHP_TechBeginnDt AND KBHH.KBHP_TechEndeDt
        AND KBHP.KBKP_BelegNummer IS NULL
        AND KBHH.KBHP_VTGKtoPositionNr = 1
    UNION ALL
    SELECT
          KBKP.KBKP_BuchungDt AS Buchungsdatum
        , KBPO.KBPO_HWhrBetrag1 * -1 AS BetragHauswaehrung
    FROM {kbkp_relation} KBKP
    INNER JOIN current_kalender KALE
        ON KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
    INNER JOIN {kbpo_relation} KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer
        AND KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
    LEFT JOIN {kbhp_relation} KBHP
        ON  KBKP.KBKP_BelegNummer = KBHP.KBKP_BelegNummer
        AND KBHP.KBHP_VTGKtoPositionNr = KBPO.KBPO_VtgKtoPositionNr
        AND KALE.Datum BETWEEN KBHP.KBHP_TechBeginnDt AND KBHP.KBHP_TechEndeDt
    LEFT JOIN {kbhp_relation} KBHH
        ON  KBKP.KBKP_BelegNummer = KBHH.KBKP_BelegNummer
        AND KALE.Datum BETWEEN KBHH.KBHP_TechBeginnDt AND KBHH.KBHP_TechEndeDt
        AND KBHP.KBKP_BelegNummer IS NULL
        AND KBHH.KBHP_VTGKtoPositionNr = 1
)
SELECT
      COUNT(*) AS cnt
    , SUM(BetragHauswaehrung) AS total
FROM branch_amounts
WHERE Buchungsdatum >= DATE '2023-01-01'
  AND Buchungsdatum <  DATE '2024-01-01';
""".strip()


def _fact_select_for_current_sources(*, optimized: bool) -> str:
    builder = _optimized_fact_select_sql if optimized else _fact_bupo_select_sql
    return builder(
        kbkp_relation="kbkp_today",
        kbpo_relation="kbpo_today",
        kbhp_relation="kbhp_today",
        kalender_relation="(SELECT CURRENT_DATE AS Datum)",
    ).rstrip(";")


def _q2_source_ctes(fact_select_sql: str) -> str:
    fact_select_ctes = (
        fact_select_sql[5:] if fact_select_sql.upper().startswith("WITH ") else fact_select_sql
    )
    return f"""
WITH kbkp_today AS (
    SELECT *
    FROM {_read_parquet(KBKP_FULL_PATH)}
    WHERE CURRENT_DATE BETWEEN KBKP_TechBeginnDt AND KBKP_TechEndeDt
),
kbpo_today AS (
    SELECT *
    FROM {_kbpo_union_by_name_relation()}
    WHERE CURRENT_DATE BETWEEN KBPO_TechBeginnDt AND KBPO_TechEndeDt
),
kbhp_today AS (
    SELECT *
    FROM {_read_parquet(KBHP_FULL_PATH)}
    WHERE CURRENT_DATE BETWEEN KBHP_TechBeginnDt AND KBHP_TechEndeDt
),
{fact_select_ctes}
""".strip()


def _copy_sql(
    select_sql: str,
    *,
    target: str,
    dataset_folder: bool = False,
    compression: str = "zstd",
    row_group_size: int = 250000,
    file_size_bytes: str = "450MB",
    overwrite_or_ignore: bool = True,
) -> str:
    options = [
        "FORMAT parquet",
        f"COMPRESSION {compression}",
        f"ROW_GROUP_SIZE {row_group_size}",
    ]
    if dataset_folder:
        options.append("PER_THREAD_OUTPUT true")
        if file_size_bytes:
            options.append(f"FILE_SIZE_BYTES {sql_literal(file_size_bytes)}")
    elif overwrite_or_ignore:
        options.append("OVERWRITE_OR_IGNORE true")
    rendered_options = ",\n    ".join(options)
    return f"""
COPY (
{select_sql.rstrip().rstrip(";")}
)
TO {sql_literal(target)}
(
    {rendered_options}
);
""".strip()


def _with_materialized_ctes(sql: str) -> str:
    rewritten = sql
    for cte_name in (
        "kbkp_today",
        "kbpo_today",
        "kbhp_today",
        "current_kalender",
        "base_positions",
        "position_specific",
        "resolved_positions",
    ):
        rewritten = rewritten.replace(f"{cte_name} AS (", f"{cte_name} AS MATERIALIZED (")
    return rewritten


def _q2_staged_statements(target: str) -> tuple[str, ...]:
    kbkp_relation = _read_parquet(KBKP_FULL_PATH)
    kbpo_relation = _kbpo_union_by_name_relation()
    kbhp_relation = _read_parquet(KBHP_FULL_PATH)
    kalender_relation = _read_parquet(KALENDER_PATH)
    return (
        "CREATE SCHEMA IF NOT EXISTS stage;",
        f"""
CREATE OR REPLACE TABLE stage.kb_current_headers AS
{_build_kostenbelege_pipeline_headers_sql(kbkp_relation=kbkp_relation, kalender_relation=kalender_relation)};
""".strip(),
        f"""
CREATE OR REPLACE TABLE stage.kb_current_positions AS
{_build_kostenbelege_pipeline_positions_sql(kbpo_relation=kbpo_relation, kalender_relation=kalender_relation)};
""".strip(),
        f"""
CREATE OR REPLACE TABLE stage.kb_current_ledger_accounts AS
{_build_kostenbelege_pipeline_ledger_accounts_sql(kbhp_relation=kbhp_relation, kalender_relation=kalender_relation)};
""".strip(),
        f"""
CREATE OR REPLACE TABLE stage.kb_resolved_positions AS
{_build_kostenbelege_pipeline_resolved_positions_sql()};
""".strip(),
        f"""
CREATE OR REPLACE TABLE stage.kb_original_positions AS
{_build_kostenbelege_pipeline_position_projection_sql(positions_art="Originalposition", amount_sign=1, settlement_dates=False)};
""".strip(),
        f"""
CREATE OR REPLACE TABLE stage.kb_settlement_positions AS
{_build_kostenbelege_pipeline_position_projection_sql(positions_art="Ausgleichsposition", amount_sign=-1, settlement_dates=True)};
""".strip(),
        _copy_sql(_build_kostenbelege_pipeline_canonical_output_sql(), target=target),
    )


def pure_duckdb_q1_q2_benchmark_variants() -> tuple[PureDuckDBBenchmarkVariant, ...]:
    q1_baseline_sql = _query_1_sql()
    q1_optimized_sql = _q1_from_fact_select(_optimized_fact_select_sql())
    q1_pushdown_sql = _q1_aggregate_pushdown_sql()

    q2_baseline_target = benchmark_fact_target("q2_baseline_current")
    q2_optimized_target = benchmark_fact_target("q2_optimized_single_file_v1")
    q2_dataset_target = benchmark_fact_target("q2_dataset_folder_v1", dataset=True)
    q2_materialized_target = benchmark_fact_target("q2_materialized_ctes_v1")
    q2_staged_target = benchmark_fact_target("q2_staged_materialization_v1")
    q2_runtime_target = benchmark_fact_target("q2_runtime_unordered_threads4_v1")
    q2_snappy_target = benchmark_fact_target("q2_optimized_snappy_single_file_v1")
    q2_uncompressed_target = benchmark_fact_target("q2_optimized_uncompressed_single_file_v1")
    q2_large_row_group_target = benchmark_fact_target("q2_optimized_zstd_rowgroup_1000000_v1")
    q2_preserve_false_target = benchmark_fact_target("q2_optimized_preserve_false_v1")
    q2_direct_sources_target = benchmark_fact_target("q2_optimized_direct_sources_v1")
    q2_direct_uncompressed_target = benchmark_fact_target(
        "q2_optimized_direct_uncompressed_v1"
    )

    q2_baseline_select = _q2_source_ctes(_fact_select_for_current_sources(optimized=False))
    q2_optimized_select = _q2_source_ctes(_fact_select_for_current_sources(optimized=True))
    q2_materialized_select = _with_materialized_ctes(q2_optimized_select)
    q2_direct_sources_select = _optimized_fact_select_sql()

    return (
        PureDuckDBBenchmarkVariant(
            variant_id="q1_baseline_current",
            query_number=1,
            statements=(q1_baseline_sql,),
            change_summary="Current production Query 1 SQL, used as the semantic and timing baseline.",
            sql_strategy="Build full FACT_Buchungsbelegposition in a CTE, then aggregate the 2023 rows.",
            output_layout="none",
            duckdb_settings=(),
            expected_effect="Baseline only.",
            validation_sql=q1_baseline_sql,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q1_optimized_fact_v1",
            query_number=1,
            statements=(q1_optimized_sql,),
            change_summary="Uses the existing optimized FACT join shape before applying the same Q1 aggregate.",
            sql_strategy="Resolved-position CTE plus CROSS JOIN amount-sign projection, then aggregate.",
            output_layout="none",
            duckdb_settings=(),
            expected_effect="Avoids the duplicated wide UNION branches in the current FACT builder.",
            validation_sql=q1_optimized_sql,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q1_pushdown_v1",
            query_number=1,
            statements=(q1_pushdown_sql,),
            change_summary="Avoids projecting full FACT rows and keeps only date plus signed amount for the aggregate.",
            sql_strategy="Narrow two-branch aggregate over source joins, preserving branch multiplicity and sign flip.",
            output_layout="none",
            duckdb_settings=(),
            expected_effect="Reduces projection width and avoids reading large unused text columns.",
            validation_sql=q1_pushdown_sql,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_baseline_current",
            query_number=2,
            statements=(_copy_sql(q2_baseline_select, target=q2_baseline_target),),
            change_summary="Current Query 2 materialization logic, with only the benchmark output path changed.",
            sql_strategy="Current FACT builder inside COPY after filtering current KBKP, KBPO, and KBHP snapshots.",
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect="Baseline only.",
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_baseline_target)}",
            output_s3_url=q2_baseline_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_optimized_single_file_v1",
            query_number=2,
            statements=(_copy_sql(q2_optimized_select, target=q2_optimized_target),),
            change_summary="Same single-file artifact, but uses the optimized FACT join shape.",
            sql_strategy="Optimized resolved-position CTE plus CROSS JOIN amount-sign projection inside COPY.",
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect="Reduces duplicated join work while keeping the same output artifact shape.",
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_optimized_target)}",
            output_s3_url=q2_optimized_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_dataset_folder_v1",
            query_number=2,
            statements=(
                _copy_sql(q2_optimized_select, target=q2_dataset_target, dataset_folder=True),
            ),
            change_summary="Uses optimized FACT SQL and writes a parallel Parquet dataset folder.",
            sql_strategy="Optimized FACT builder inside COPY TO folder with PER_THREAD_OUTPUT and target file sizing.",
            output_layout="parquet dataset folder",
            duckdb_settings=(),
            expected_effect="Lets DuckDB write partitions in parallel and avoid one large serialized output file.",
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_dataset_target)}",
            output_s3_url=q2_dataset_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_materialized_ctes_v1",
            query_number=2,
            statements=(_copy_sql(q2_materialized_select, target=q2_materialized_target),),
            change_summary="Forces key source and resolved-position CTEs to materialize before final projection.",
            sql_strategy="Optimized FACT builder with AS MATERIALIZED CTE hints inside COPY.",
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect="Tests whether preventing CTE re-planning or repeated source scans improves stability.",
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_materialized_target)}",
            output_s3_url=q2_materialized_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_optimized_snappy_single_file_v1",
            query_number=2,
            statements=(
                _copy_sql(q2_optimized_select, target=q2_snappy_target, compression="snappy"),
            ),
            change_summary=(
                "Keeps the optimized FACT SQL and one-file artifact, but writes Snappy Parquet instead "
                "of ZSTD."
            ),
            sql_strategy="Optimized FACT builder inside COPY; only the Parquet compression codec changes.",
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect=(
                "Reduces CPU spent compressing the output; can be faster if a larger artifact is acceptable."
            ),
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_snappy_target)}",
            output_s3_url=q2_snappy_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_optimized_uncompressed_single_file_v1",
            query_number=2,
            statements=(
                _copy_sql(
                    q2_optimized_select,
                    target=q2_uncompressed_target,
                    compression="uncompressed",
                ),
            ),
            change_summary=(
                "Keeps the optimized FACT SQL and one-file artifact, but disables Parquet compression."
            ),
            sql_strategy="Optimized FACT builder inside COPY; only the Parquet compression codec changes.",
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect=(
                "Removes output compression CPU entirely; useful to separate compute cost from write "
                "compression cost, but produces larger files."
            ),
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_uncompressed_target)}",
            output_s3_url=q2_uncompressed_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_optimized_zstd_rowgroup_1000000_v1",
            query_number=2,
            statements=(
                _copy_sql(
                    q2_optimized_select,
                    target=q2_large_row_group_target,
                    row_group_size=1000000,
                ),
            ),
            change_summary=(
                "Keeps optimized FACT SQL and ZSTD compression, but writes larger Parquet row groups."
            ),
            sql_strategy="Optimized FACT builder inside COPY with ROW_GROUP_SIZE 1000000.",
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect=(
                "Reduces row-group metadata and write coordination overhead; may trade off later scan "
                "pruning granularity."
            ),
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_large_row_group_target)}",
            output_s3_url=q2_large_row_group_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_optimized_preserve_false_v1",
            query_number=2,
            statements=(_copy_sql(q2_optimized_select, target=q2_preserve_false_target),),
            change_summary=(
                "Keeps optimized FACT SQL and ZSTD output, but disables insertion-order preservation."
            ),
            sql_strategy="Optimized FACT builder inside COPY; only preserve_insertion_order changes.",
            output_layout="single parquet file",
            duckdb_settings=("SET preserve_insertion_order = false",),
            expected_effect=(
                "Lets DuckDB reorder intermediate work where row order is irrelevant without also "
                "forcing a thread count."
            ),
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_preserve_false_target)}",
            output_s3_url=q2_preserve_false_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_optimized_direct_sources_v1",
            query_number=2,
            statements=(_copy_sql(q2_direct_sources_select, target=q2_direct_sources_target),),
            change_summary=(
                "Runs the optimized FACT builder directly against source Parquet plus DIM_Kalender, "
                "without separate kbkp_today/kbpo_today/kbhp_today CTEs."
            ),
            sql_strategy=(
                "Optimized FACT builder owns the current-date filtering and joins directly to each source."
            ),
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect=(
                "Tests whether removing prefiltered source CTEs gives DuckDB a better global plan."
            ),
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_direct_sources_target)}",
            output_s3_url=q2_direct_sources_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_optimized_direct_uncompressed_v1",
            query_number=2,
            statements=(
                _copy_sql(
                    q2_direct_sources_select,
                    target=q2_direct_uncompressed_target,
                    compression="uncompressed",
                ),
            ),
            change_summary=(
                "Combines direct optimized source planning with uncompressed Parquet output."
            ),
            sql_strategy=(
                "Optimized FACT builder owns current-date filtering and COPY skips output compression."
            ),
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect=(
                "Tests whether the faster plan and lower write CPU compound; output files are larger."
            ),
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_direct_uncompressed_target)}",
            output_s3_url=q2_direct_uncompressed_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_staged_materialization_v1",
            query_number=2,
            statements=_q2_staged_statements(q2_staged_target),
            change_summary="Breaks Q2 into explicit current-source, resolved-position, and final COPY stages.",
            sql_strategy="Creates in-memory stage tables, then copies the canonical final projection to Parquet.",
            output_layout="single parquet file",
            duckdb_settings=(),
            expected_effect="Measures whether explicit materialization beats one very large optimized statement.",
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_staged_target)}",
            output_s3_url=q2_staged_target,
        ),
        PureDuckDBBenchmarkVariant(
            variant_id="q2_runtime_unordered_threads4_v1",
            query_number=2,
            statements=(_copy_sql(q2_optimized_select, target=q2_runtime_target),),
            change_summary="Uses optimized FACT SQL with DuckDB insertion-order preservation disabled and 4 threads.",
            sql_strategy="Same as optimized single-file COPY; only DuckDB runtime settings change.",
            output_layout="single parquet file",
            duckdb_settings=("SET preserve_insertion_order = false", "SET threads = 4"),
            expected_effect="Allows DuckDB more freedom to reorder work and tests a controlled thread count.",
            validation_sql=f"SELECT * FROM {fact_scan_sql(q2_runtime_target)}",
            output_s3_url=q2_runtime_target,
        ),
    )


def pure_duckdb_q1_q2_comparison_columns() -> tuple[str, ...]:
    return (
        "variant_id",
        "query",
        "dataset_size",
        "elapsed",
        "change_summary",
        "sql_strategy",
        "output_layout",
        "duckdb_settings",
        "expected_effect",
        "consistency_status",
        "consistency_details",
    )

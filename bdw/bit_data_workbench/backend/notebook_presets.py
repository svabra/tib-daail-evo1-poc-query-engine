# flake8: noqa
# ruff: noqa: E501

from __future__ import annotations

import re

from ..data_generator.data_analysts_journey import (
    JOURNEY_AS_OF_DATE,
    JOURNEY_GENERATOR_ID,
    JOURNEY_MANUAL_QUERY_REFERENCE,
    JOURNEY_NOTEBOOK_ID,
    JOURNEY_POSTGRES_QUERY_REFERENCE,
    JOURNEY_PRODUCT_PATH,
    JOURNEY_TREE_PATH,
)
from ..data_generator.result_set_storage_sample import (
    RESULT_SET_STORAGE_SAMPLE_BUCKET,
    RESULT_SET_STORAGE_SAMPLE_RESULT_KEY,
    RESULT_SET_STORAGE_SAMPLE_RESULT_PATH,
    RESULT_SET_STORAGE_SAMPLE_TREE_PATH,
)
from ..data_generator.kostenbelege_fact_builder_sample import (
    KBHP_SOURCE_KEY,
    KBKP_SOURCE_KEY,
    KBPO_SOURCE_KEYS,
    KOSTENBELEGE_FACT_BUILDER_BUCKET,
    KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID,
    KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID,
    KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID,
    KOSTENBELEGE_FACT_BUILDER_PIPELINE_TREE_PATH,
    KOSTENBELEGE_FACT_BUILDER_TREE_PATH,
    RESULT_SET_KEYS as KOSTENBELEGE_FACT_BUILDER_RESULT_KEYS,
    result_path as kostenbelege_fact_builder_result_path,
)
from ..models import NotebookCellDefinition, NotebookDefinition
from .source_references import s3_source_reference


DATA_PIPELINES_TREE_PATH = (
    "PoC Tests",
    "Performance Evaluation",
    "Data Pipelines",
)
JUPYTER_PYTHON_TREE_PATH = ("PoC Tests", "Jupyter/Python")
MWA_PARQUET_PIPELINE_NOTEBOOK_ID = "mwa-abrechnung-s3-parquet-pipeline"
MWA_PARQUET_PIPELINE_CREATED_AT = "2026-06-06T00:00:00+00:00"
MWA_PARQUET_PIPELINE_TREE_PATH = DATA_PIPELINES_TREE_PATH
KOSTENBELEGE_3_1_PIPELINE_NOTEBOOK_ID = "kostenbelege-3-1-s3-parquet-pipeline"
KOSTENBELEGE_3_1_PIPELINE_CREATED_AT = "2026-06-07T00:00:00+00:00"
KOSTENBELEGE_3_1_PIPELINE_TREE_PATH = DATA_PIPELINES_TREE_PATH
KOSTENBELEGE_3_1_PROBLEM_NOTEBOOK_ID = "test-3-1-problem-solving"
KOSTENBELEGE_3_1_PROBLEM_CREATED_AT = "2026-06-10T00:00:00+00:00"
KOSTENBELEGE_3_1_PROBLEM_TREE_PATH = (
    "PoC Tests",
    "Performance Evaluation",
    "Kostenbelege (3.1)",
)
KOSTENBELEGE_3_1_OPTIMIZED_DATASET_BUCKET = (
    "poc-tests-performance-evaluation-kostenbelege-3-1"
)
KOSTENBELEGE_3_1_OPTIMIZED_DATASET_PREFIX = (
    "optimized/3_1/fact_buchungsbelegposition"
)
KOSTENBELEGE_3_1_OPTIMIZED_DATASET_TARGET = (
    f"s3://{KOSTENBELEGE_3_1_OPTIMIZED_DATASET_BUCKET}/"
    f"{KOSTENBELEGE_3_1_OPTIMIZED_DATASET_PREFIX}/"
)
KOSTENBELEGE_3_1_OPTIMIZED_DATASET_RELATION = (
    f's3."{KOSTENBELEGE_3_1_OPTIMIZED_DATASET_BUCKET}".'
    f'"{KOSTENBELEGE_3_1_OPTIMIZED_DATASET_PREFIX}/*.parquet"'
)
KOSTENBELEGE_3_1_FULL_KBPO_RELATIONS = (
    "s3.kbpoimports.kbpo_2018undvorher.parquet",
    "s3.kbpoimports.kbpo_2019.parquet",
    "s3.kbpoimports.kbpo2020.parquet",
    "s3.kbpoimports.kbpo2021.parquet",
    "s3.kbpoimports.kbpo2022.parquet",
    "s3.kbpoimports.kbpo2023.parquet",
    "s3.kbpoimports.kbpo2024.parquet",
    "s3.kbpoimports.kbpo2025.parquet",
)


def _s3_data_sources(*relations: str | None) -> list[str]:
    sources: list[str] = []
    seen: set[str] = set()
    for relation in relations:
        source = str(relation or "").strip()
        if not source or not source.lower().startswith("s3."):
            continue
        if source in seen:
            continue
        seen.add(source)
        sources.append(source)
    return sources


def build_data_analysts_journey_aggregation_sql(
    postgres_relation: str = JOURNEY_POSTGRES_QUERY_REFERENCE,
    aargau_relation: str = JOURNEY_MANUAL_QUERY_REFERENCE,
) -> str:
    """Build the reviewable UNION aggregation used by the immutable journey."""
    as_of_date = JOURNEY_AS_OF_DATE.isoformat()
    current_month = JOURNEY_AS_OF_DATE.replace(day=1).isoformat()
    elapsed_fraction = f"{JOURNEY_AS_OF_DATE.day}.0 / 31.0"
    return f"""
-- Synthetic PoC data only; not official cantonal financial statistics.
-- Grain before UNION ALL: one canton and reporting month (26 x 60 = 1,560 rows).
WITH electronic_normalized AS (
  SELECT
    CAST(record_id AS VARCHAR) AS record_id,
    UPPER(TRIM(CAST(canton_code AS VARCHAR))) AS canton_code,
    TRIM(CAST(canton_name AS VARCHAR)) AS canton_name,
    CAST(report_month AS DATE) AS report_month,
    CAST(as_of_date AS DATE) AS as_of_date,
    CAST(tax_year AS INTEGER) AS tax_year,
    CAST(planned_monthly_chf AS DECIMAL(18,2)) AS planned_monthly_chf,
    CAST(actual_receipt_chf AS DECIMAL(18,2)) AS actual_receipt_chf,
    LOWER(TRIM(CAST(status AS VARCHAR))) AS status,
    CAST(delivery_channel AS VARCHAR) AS delivery_channel,
    CAST(reported_at AS TIMESTAMPTZ) AS reported_at,
    CAST(is_synthetic AS BOOLEAN) AS is_synthetic
  FROM {postgres_relation}
),
aargau_normalized AS (
  SELECT
    CAST(record_id AS VARCHAR) AS record_id,
    UPPER(TRIM(CAST(canton_code AS VARCHAR))) AS canton_code,
    TRIM(CAST(canton_name AS VARCHAR)) AS canton_name,
    CAST(report_month AS DATE) AS report_month,
    CAST(as_of_date AS DATE) AS as_of_date,
    CAST(tax_year AS INTEGER) AS tax_year,
    CAST(planned_monthly_chf AS DECIMAL(18,2)) AS planned_monthly_chf,
    CAST(actual_receipt_chf AS DECIMAL(18,2)) AS actual_receipt_chf,
    LOWER(TRIM(CAST(status AS VARCHAR))) AS status,
    CAST(delivery_channel AS VARCHAR) AS delivery_channel,
    CAST(reported_at AS TIMESTAMPTZ) AS reported_at,
    CAST(is_synthetic AS BOOLEAN) AS is_synthetic
  FROM {aargau_relation}
),
monthly_union AS (
  SELECT * FROM electronic_normalized
  UNION ALL
  SELECT * FROM aargau_normalized
),
annual_base AS (
  SELECT
    canton_code,
    MAX(canton_name) AS canton_name,
    tax_year,
    MAX(as_of_date) AS as_of_date,
    CAST(ROUND(SUM(planned_monthly_chf), 2) AS DECIMAL(18,2)) AS annual_plan_chf,
    CAST(ROUND(SUM(
      CASE
        WHEN report_month < DATE '{current_month}' THEN planned_monthly_chf
        WHEN report_month = DATE '{current_month}'
          THEN planned_monthly_chf * ({elapsed_fraction})
        ELSE 0
      END
    ), 2) AS DECIMAL(18,2)) AS expected_receipts_to_date_chf,
    CAST(ROUND(SUM(actual_receipt_chf), 2) AS DECIMAL(18,2)) AS actual_receipts_to_date_chf,
    COUNT(*) AS source_month_count,
    COUNT(DISTINCT report_month) AS distinct_month_count,
    SUM(CASE WHEN status = 'final' THEN 1 ELSE 0 END) AS complete_month_count,
    SUM(CASE WHEN status IN ('final', 'month_to_date') THEN 1 ELSE 0 END) AS reported_month_count,
    SUM(CASE WHEN planned_monthly_chf < 0 OR actual_receipt_chf < 0 THEN 1 ELSE 0 END)
      AS negative_amount_count,
    BOOL_AND(is_synthetic) AS is_synthetic
  FROM monthly_union
  GROUP BY canton_code, tax_year
),
projected AS (
  SELECT
    *,
    CAST(ROUND(
      CASE
        WHEN tax_year < 2026 THEN actual_receipts_to_date_chf
        WHEN expected_receipts_to_date_chf > 0 THEN
          CAST(annual_plan_chf AS DOUBLE)
            * CAST(actual_receipts_to_date_chf AS DOUBLE)
            / CAST(expected_receipts_to_date_chf AS DOUBLE)
        ELSE NULL
      END,
      2
    ) AS DECIMAL(18,2)) AS annual_projection_chf
  FROM annual_base
)
SELECT
  canton_code,
  canton_name,
  tax_year,
  DATE '{as_of_date}' AS as_of_date,
  annual_plan_chf,
  expected_receipts_to_date_chf,
  actual_receipts_to_date_chf,
  annual_projection_chf,
  CAST(ROUND(annual_projection_chf - annual_plan_chf, 2) AS DECIMAL(18,2))
    AS variance_chf,
  CAST(ROUND(
    100.0 * (annual_projection_chf - annual_plan_chf) / NULLIF(annual_plan_chf, 0),
    2
  ) AS DECIMAL(10,2)) AS variance_pct,
  source_month_count,
  distinct_month_count,
  complete_month_count,
  reported_month_count,
  CAST(ROUND(100.0 * reported_month_count / 12.0, 2) AS DECIMAL(6,2))
    AS completeness_pct,
  source_month_count - distinct_month_count AS duplicate_month_count,
  negative_amount_count,
  is_synthetic
FROM projected
ORDER BY canton_code, tax_year;
""".strip()


def build_data_analysts_journey_chart_python(
    result_path: str = JOURNEY_PRODUCT_PATH,
) -> str:
    escaped_path = str(result_path).replace("'", "''")
    return f'''
import matplotlib.pyplot as plt
import numpy as np

# Load the exact Parquet result materialized by SQL cell 1 in an in-memory DuckDB worker.
journey_result = sql("""
SELECT *
FROM read_parquet('{escaped_path}')
ORDER BY canton_code, tax_year
""")
if len(journey_result) != 130:
    raise ValueError(f"Expected 130 canton/year rows, received {{len(journey_result)}}.")

money_columns = [
    "annual_plan_chf",
    "expected_receipts_to_date_chf",
    "actual_receipts_to_date_chf",
    "annual_projection_chf",
    "variance_chf",
    "variance_pct",
]
for column in money_columns:
    journey_result[column] = pd.to_numeric(journey_result[column], errors="raise")

annual = (
    journey_result.groupby("tax_year", as_index=False)
    .agg(
        annual_plan_chf=("annual_plan_chf", "sum"),
        expected_receipts_to_date_chf=("expected_receipts_to_date_chf", "sum"),
        actual_receipts_to_date_chf=("actual_receipts_to_date_chf", "sum"),
        annual_projection_chf=("annual_projection_chf", "sum"),
    )
    .sort_values("tax_year")
)
annual["comparison_chf"] = np.where(
    annual["tax_year"].eq(2026),
    annual["annual_projection_chf"],
    annual["actual_receipts_to_date_chf"],
)

# Chart contract: grouped annual comparison plus a true 26-canton distribution.
# Explicit palette and hatches keep both panels readable without relying on colour alone.
ink = "#17202a"
federal_blue = "#2f4356"
swiss_red = "#d8232a"
gold = "#a36b00"
grid = "#d9dee3"
years = annual["tax_year"].astype(str).tolist()
x = np.arange(len(years))
width = 0.36

fig, (ax_year, ax_hist) = plt.subplots(1, 2, figsize=(14, 6.6))
fig.patch.set_facecolor("white")

plan_bars = ax_year.bar(
    x - width / 2,
    annual["annual_plan_chf"] / 1_000_000_000,
    width,
    label="Jahresplan",
    color="white",
    edgecolor=federal_blue,
    linewidth=1.4,
    hatch="///",
)
comparison_bars = ax_year.bar(
    x + width / 2,
    annual["comparison_chf"] / 1_000_000_000,
    width,
    label="Ist 2022–2025 / Hochrechnung 2026",
    color="#91a3b5",
    edgecolor=ink,
    linewidth=1.1,
    hatch="xx",
)
ax_year.set_title("Jahresvergleich: Plan, Ist und Hochrechnung", loc="left", color=ink)
ax_year.set_ylabel("Mrd. CHF")
ax_year.set_xlabel("Steuerjahr")
ax_year.set_xticks(x, years)
ax_year.set_ylim(bottom=0)
ax_year.grid(axis="y", color=grid, linewidth=0.8)
ax_year.set_axisbelow(True)
ax_year.bar_label(plan_bars, fmt="%.1f", padding=3, fontsize=8, color=federal_blue)
ax_year.bar_label(comparison_bars, fmt="%.1f", padding=3, fontsize=8, color=ink)

ytd_2026 = annual.loc[annual["tax_year"].eq(2026)].iloc[0]
last_x = x[-1]
ax_year.scatter(
    [last_x - width / 2, last_x + width / 2],
    [
        ytd_2026["expected_receipts_to_date_chf"] / 1_000_000_000,
        ytd_2026["actual_receipts_to_date_chf"] / 1_000_000_000,
    ],
    color=[gold, swiss_red],
    edgecolor=ink,
    linewidth=0.8,
    marker="D",
    s=55,
    zorder=4,
)
ax_year.annotate(
    "Soll bis 12.08.",
    (last_x - width / 2, ytd_2026["expected_receipts_to_date_chf"] / 1_000_000_000),
    xytext=(-7, -18),
    textcoords="offset points",
    ha="right",
    fontsize=8,
    color=gold,
)
ax_year.annotate(
    "Ist bis 12.08.",
    (last_x + width / 2, ytd_2026["actual_receipts_to_date_chf"] / 1_000_000_000),
    xytext=(7, -18),
    textcoords="offset points",
    ha="left",
    fontsize=8,
    color=swiss_red,
)
ax_year.legend(frameon=False, loc="upper left", fontsize=8)

deviations = journey_result.loc[
    journey_result["tax_year"].eq(2026), "variance_pct"
].dropna()
if len(deviations) != 26:
    raise ValueError(f"Expected 26 canton deviations for 2026, received {{len(deviations)}}.")
mean_deviation = float(deviations.mean())
median_deviation = float(deviations.median())
ax_hist.hist(
    deviations,
    bins=8,
    color="#d9e1e8",
    edgecolor=federal_blue,
    linewidth=1.1,
    hatch="..",
)
ax_hist.axvline(0, color=ink, linewidth=1.4, linestyle="-", label="Nullabweichung")
ax_hist.axvline(
    mean_deviation,
    color=swiss_red,
    linewidth=1.5,
    linestyle="--",
    label=f"Mittelwert {{mean_deviation:+.1f}} %",
)
ax_hist.axvline(
    median_deviation,
    color=gold,
    linewidth=1.5,
    linestyle=":",
    label=f"Median {{median_deviation:+.1f}} %",
)
ax_hist.set_title("Verteilung der kantonalen Planabweichung 2026", loc="left", color=ink)
ax_hist.set_xlabel("Abweichung zur Jahresplanung (%)")
ax_hist.set_ylabel("Anzahl Kantone")
ax_hist.set_ylim(bottom=0)
ax_hist.grid(axis="y", color=grid, linewidth=0.8)
ax_hist.set_axisbelow(True)
ax_hist.legend(frameon=False, fontsize=8, loc="upper left")

fig.suptitle(
    "Kantonale Gewerbesteuer: Soll/Ist und Jahreshochrechnung 2022–2026",
    x=0.055,
    y=0.985,
    ha="left",
    fontsize=15,
    fontweight="bold",
    color=ink,
)
fig.text(
    0.055,
    0.015,
    "Synthetische PoC-Daten · Stichtag 12.08.2026 (Europe/Zurich) · "
    "2026: Hochrechnung auf Basis des Soll/Ist bis Stichtag · Beträge in CHF",
    fontsize=9,
    color=ink,
)
plt.tight_layout(rect=(0.04, 0.06, 0.99, 0.92))
plt.show()
'''.strip()


def build_data_analysts_journey_notebook() -> NotebookDefinition:
    return NotebookDefinition(
        notebook_id=JOURNEY_NOTEBOOK_ID,
        title="A Data Analyst's Journey – Kantonale Gewerbesteuer",
        summary=(
            "Vereinigt 25 elektronisch gelieferte Kantone aus PostgreSQL mit der "
            "manuell eingelesenen Aargau-CSV, aggregiert Soll/Ist und visualisiert "
            "die vollständig synthetische Jahreshochrechnung per 12.08.2026."
        ),
        cells=[
            NotebookCellDefinition(
                cell_id=f"{JOURNEY_NOTEBOOK_ID}-cell-1",
                language="sql",
                data_sources=["pg_oltp", JOURNEY_MANUAL_QUERY_REFERENCE],
                query_options={
                    "duckdb": {
                        "resultStorage": {
                            "mode": "on",
                            "path": JOURNEY_PRODUCT_PATH,
                        }
                    },
                    "validation": {"sourceExistence": "off"},
                },
                processing_hints=(
                    "Expected input: 1,500 PostgreSQL rows plus 60 Aargau CSV rows."
                ),
                result_expectations=(
                    "Exactly 130 rows (26 cantons × 5 years), persisted as Parquet."
                ),
                sql=build_data_analysts_journey_aggregation_sql(),
            ),
            NotebookCellDefinition(
                cell_id=f"{JOURNEY_NOTEBOOK_ID}-cell-2",
                language="python",
                data_sources=["s3"],
                processing_hints=(
                    "Loads only the materialized result from cell 1 in an in-memory DuckDB worker."
                ),
                result_expectations=(
                    "One image/png containing the annual comparison and a 26-canton histogram."
                ),
                sql=build_data_analysts_journey_chart_python(),
            ),
        ],
        tags=[
            "customer-journey",
            "gewerbesteuer",
            "postgres",
            "s3",
            "csv",
            "union",
            "python",
            "synthetic",
        ],
        tree_path=JOURNEY_TREE_PATH,
        linked_generator_id=JOURNEY_GENERATOR_ID,
        can_edit=False,
        can_delete=False,
        shared=True,
        created_at="2026-08-12T07:15:00+00:00",
    )


def _result_set_storage_query_options() -> dict[str, object]:
    return {
        "duckdb": {
            "resultStorage": {
                "mode": "on",
                "path": RESULT_SET_STORAGE_SAMPLE_RESULT_PATH,
            }
        }
    }


def _build_result_set_storage_store_sql(source_relation: str) -> str:
    return f"""
-- Store this complete aggregation in S3 through the cell option "store result set in S3".
-- The result panel exposes both the virtual path and the DuckDB read_parquet(...) path.
WITH quarterly_orders AS (
  SELECT
    canton_code,
    order_channel,
    order_status,
    CAST(date_trunc('quarter', order_date) AS DATE) AS order_quarter_start,
    COUNT(*) AS order_count,
    CAST(ROUND(SUM(net_amount_chf), 2) AS DECIMAL(18,2)) AS net_total_chf,
    CAST(ROUND(SUM(vat_amount_chf), 2) AS DECIMAL(18,2)) AS vat_total_chf,
    CAST(ROUND(SUM(gross_amount_chf), 2) AS DECIMAL(18,2)) AS gross_total_chf,
    SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) AS review_order_count,
    MAX(updated_at) AS latest_update_at
  FROM {source_relation}
  WHERE order_date >= DATE '2025-01-01'
  GROUP BY canton_code, order_channel, order_status, order_quarter_start
)
SELECT
  canton_code,
  order_channel,
  order_status,
  order_quarter_start,
  order_count,
  net_total_chf,
  vat_total_chf,
  gross_total_chf,
  review_order_count,
  latest_update_at
FROM quarterly_orders
ORDER BY gross_total_chf DESC, review_order_count DESC, order_count DESC
LIMIT 75;
""".strip()


def _build_result_set_storage_load_sql() -> str:
    virtual_path = s3_source_reference(
        bucket=RESULT_SET_STORAGE_SAMPLE_BUCKET,
        key=RESULT_SET_STORAGE_SAMPLE_RESULT_KEY,
    )
    return f"""
-- Load the result set written by the previous cell and continue computing from it.
SELECT
  canton_code,
  SUM(order_count) AS stored_order_count,
  CAST(ROUND(SUM(gross_total_chf), 2) AS DECIMAL(18,2)) AS stored_gross_total_chf,
  CAST(ROUND(SUM(vat_total_chf), 2) AS DECIMAL(18,2)) AS stored_vat_total_chf,
  SUM(review_order_count) AS stored_review_order_count
FROM {virtual_path}
GROUP BY canton_code
ORDER BY stored_gross_total_chf DESC, stored_review_order_count DESC;
""".strip()


def build_result_set_storage_s3_demo_notebook(
    result_set_storage_source_relation: str | None,
) -> NotebookDefinition:
    result_set_storage_sources = _s3_data_sources(result_set_storage_source_relation)
    result_set_storage_store_sql = (
        _build_result_set_storage_store_sql(result_set_storage_source_relation)
        if result_set_storage_source_relation
        else "SELECT 'Run the Result Set Storage S3 Loader from the Loader Workbench first.' AS status;"
    )
    result_set_storage_load_sql = (
        _build_result_set_storage_load_sql()
        if result_set_storage_source_relation
        else "SELECT 'Run the Result Set Storage S3 Loader, then run the store-result cell first.' AS status;"
    )
    result_set_storage_store_options = (
        _result_set_storage_query_options()
        if result_set_storage_source_relation
        else {}
    )
    return NotebookDefinition(
        notebook_id="result-set-storage-s3-demo",
        title="Store Result Set in S3 Demo",
        summary=(
            "Stores a complete DuckDB aggregation as Parquet in S3 through the "
            "notebook result-storage option, then loads that stored result set "
            "again for follow-up computation."
        ),
        cells=[
            NotebookCellDefinition(
                cell_id="result-set-storage-s3-demo-cell-1",
                data_sources=result_set_storage_sources,
                query_options=result_set_storage_store_options,
                sql=result_set_storage_store_sql,
            ),
            NotebookCellDefinition(
                cell_id="result-set-storage-s3-demo-cell-2",
                data_sources=["s3"] if result_set_storage_source_relation else [],
                sql=result_set_storage_load_sql,
            ),
        ],
        tags=["s3", "parquet", "duckdb", "result-storage", "demo"],
        tree_path=RESULT_SET_STORAGE_SAMPLE_TREE_PATH,
        linked_generator_id="result_set_storage_s3_loader",
        can_edit=False,
        can_delete=False,
        shared=True,
    )


def _kostenbelege_fact_builder_reference(key: str) -> str:
    return s3_source_reference(
        bucket=KOSTENBELEGE_FACT_BUILDER_BUCKET,
        key=key,
    )


def _kostenbelege_fact_builder_result_reference(result_name: str) -> str:
    return _kostenbelege_fact_builder_reference(
        KOSTENBELEGE_FACT_BUILDER_RESULT_KEYS[result_name]
    )


def _kostenbelege_fact_builder_pipeline_output_path(alias: str) -> str:
    notebook_slug = KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID.replace("-", "_")
    return (
        f"s3://{KOSTENBELEGE_FACT_BUILDER_BUCKET}/"
        f"generated/kostenbelege_fact_builder/pipeline-results/{notebook_slug}/{alias}.parquet"
    )


def _kostenbelege_fact_builder_result_options(result_name: str) -> dict[str, object]:
    return {
        "duckdb": {
            "resultStorage": {
                "mode": "on",
                "path": kostenbelege_fact_builder_result_path(result_name),
            }
        },
        "validation": {
            "sourceExistence": "off",
        },
    }


def _kostenbelege_fact_builder_pipeline_options() -> dict[str, object]:
    return {
        "validation": {
            "sourceExistence": "off",
        },
    }


def _build_kostenbelege_fact_builder_kbkp_sql() -> str:
    return f"""
-- ---- Zelle 1: KBKP heute ------------------------------------------------
SELECT *
FROM {_kostenbelege_fact_builder_reference(KBKP_SOURCE_KEY)}
WHERE CURRENT_DATE BETWEEN KBKP_TechBeginnDt AND KBKP_TechEndeDt;
""".strip()


def _build_kostenbelege_fact_builder_kbpo_sql() -> str:
    union_sql = "\n    UNION ALL\n    ".join(
        f"SELECT * FROM {_kostenbelege_fact_builder_reference(key)}"
        for key in KBPO_SOURCE_KEYS
    )
    return f"""
-- ---- Zelle 2: KBPO heute ------------------------------------------------
WITH kbpo_union AS (
    {union_sql}
)
SELECT *
FROM kbpo_union
WHERE CURRENT_DATE BETWEEN KBPO_TechBeginnDt AND KBPO_TechEndeDt;
""".strip()


def _build_kostenbelege_fact_builder_kbhp_sql() -> str:
    return f"""
-- ---- Zelle 3: KBHP heute (positionsspezifisch) --------------------------
SELECT *
FROM {_kostenbelege_fact_builder_reference(KBHP_SOURCE_KEY)}
WHERE CURRENT_DATE BETWEEN KBHP_TechBeginnDt AND KBHP_TechEndeDt;
""".strip()


def _build_kostenbelege_fact_builder_kbhp_pos1_sql_for_reference(kbhp_today: str) -> str:
    return f"""
-- ---- Zelle 4: KBHP-Fallback (nur PositionNr = 1) ------------------------
-- Ersetzt den problematischen KBHH-Join durch eine kleine, vorgefilterte
-- Tabelle -> sauberer Hash-Join.
SELECT
      KBKP_BelegNummer
    , KBHP_SachKto
    , KBHP_HBAbstimmschluessel
FROM {kbhp_today}
WHERE KBHP_VTGKtoPositionNr = 1;
""".strip()


def _build_kostenbelege_fact_builder_kbhp_pos1_sql() -> str:
    return _build_kostenbelege_fact_builder_kbhp_pos1_sql_for_reference(
        _kostenbelege_fact_builder_result_reference("kbhp_today")
    )


def _build_kostenbelege_fact_builder_pipeline_kbhp_pos1_sql() -> str:
    return _build_kostenbelege_fact_builder_kbhp_pos1_sql_for_reference(
        "stage.kbhp_today"
    )


def _build_kostenbelege_fact_builder_fact_sql_for_references(
    *,
    kbkp_today: str,
    kbpo_today: str,
    kbhp_today: str,
    kbhp_pos1: str,
) -> str:
    return f"""
-- ---- Zelle 5: fact_buchungsbelegposition --------------------------------
WITH
kbkp_today AS (
    SELECT * FROM {kbkp_today}
),
kbpo_today AS (
    SELECT * FROM {kbpo_today}
),
kbhp_today AS (
    SELECT * FROM {kbhp_today}
),
kbhp_pos1 AS (
    SELECT * FROM {kbhp_pos1}
),
unio AS (
    -- ---------------------------------------------------------------------
    -- Branch 1: Originalpositionen  (KBKP_Belegnummer = KBPO_Belegnummer)
    -- ---------------------------------------------------------------------
    SELECT
          KBKP.KBKP_Belegnummer                                                  AS KBKP_Belegnummer
        , KBPO.KBPO_VtgKtoWiederholPos                                           AS KBPO_VtgKtoWiederholPos
        , KBKP.DOCO_Belegart                                                     AS DOCO_Belegart
        , KBKP.KBKP_BelegDt                                                      AS KBKP_BelegDt
        , KBKP.KBKP_BuchungDt                                                    AS KBKP_BuchungDt
        , KBKP.KBKP_ErstellungVon                                                AS KBKP_ErstellungVon
        , KBKP.KBKP_StorniertBelegNummer                                         AS KBKP_StorniertBelegNummer
        , KBKP.KBKP_StornoBelegNummer                                            AS KBKP_StornoBelegNummer
        , KBKP.DOCO_BelegHerkunft                                                AS DOCO_BelegHerkunft
        , KBPO.KBPO_VtgKtoPositionNr                                             AS KBPO_VtgKtoPositionNr
        , KBPO.KBPO_Teilposition                                                 AS KBPO_Teilposition
        , KBPO.GEFA_GeschaeftFall                                                AS GEFA_GeschaeftFall
        , KBPO.PART_Partner                                                      AS PART_Partner
        , KBPO.KBPO_KtoFindMerkmal                                               AS KBPO_KtoFindMerkmal
        , KBPO.DOCO_Hauptvorgang                                                 AS DOCO_Hauptvorgang
        , KBPO.DOCO_Teilvorgang                                                  AS DOCO_Teilvorgang
        , KBPO.DOCO_Belegtyp                                                     AS DOCO_Belegtyp
        , KBPO.DOCO_VtrKtoTyp                                                    AS DOCO_VtrKtoTyp
        , KBPO.DOCO_Waehrung                                                     AS DOCO_Waehrung
        , KBPO.DOCO_FormArt                                                      AS DOCO_FormArt
        , KBPO.KBPO_GesamtBetrag                                                 AS KBPO_GesamtBetrag
        , KBPO.KBPO_TWhrBetrag                                                   AS KBPO_TWhrBetrag
        , KBPO.KBPO_HbWaehrung                                                   AS KBPO_HbWaehrung
        , KBPO.KBPO_HbBetrag                                                     AS KBPO_HbBetrag
        , KBPO.KBPO_HWhrBetrag1                                                  AS KBPO_HWhrBetrag1
        , KBPO.KBPO_Umrechnungkurs                                               AS KBPO_Umrechnungkurs
        , KBPO.KBPO_NettoFaelligkeitDT                                           AS KBPO_NettoFaelligkeitDT
        , KBPO.VTGP_VtrGegenstand                                                AS VTGP_VtrGegenstand
        , KBPO.KBPO_VtrKtoNummer                                                 AS KBPO_VtrKtoNummer
        , KBPO.KBKP_AusgleichBelegnummer                                         AS KBKP_AusgleichBelegnummer
        , KBPO.KBPO_AusgleichStatus                                              AS KBPO_AusgleichStatus
        , KBPO.KBPO_Ausgleichgrund                                               AS KBPO_Ausgleichgrund
        , KBPO.KBPO_AusgleichDt                                                  AS KBPO_AusgleichDt
        , KBPO.KBPO_AusgleichBuchungDt                                           AS KBPO_AusgleichBuchungDt
        , KBPO.KBPO_HBSachkto                                                    AS KBPO_HBSachkto
        , COALESCE(KBHP.KBHP_SachKto, KBHH.KBHP_SachKto)                         AS KBHP_SachKto
        , COALESCE(KBHP.KBHP_HBAbstimmschluessel, KBHH.KBHP_HBAbstimmschluessel) AS KBHP_HBAbstimmschluessel
        , KBPO.KBPO_Beschreibung                                                 AS KBPO_Beschreibung
        , KBPO.DOCO_SteuerCd                                                     AS DOCO_SteuerCd
        , KBPO.KBPO_WertInternDt                                                 AS KBPO_WertInternDt
        , KBPO.KBPO_Bankverbindung                                               AS KBPO_Bankverbindung
        , KBPO.DOCO_RecordArt                                                    AS DOCO_RecordArt
        , KBKP.DOCO_Buchunggrund                                                 AS DOCO_Buchunggrund
        , CAST('Originalposition' AS VARCHAR(20))                                AS PositionsArt
    FROM kbkp_today KBKP
    INNER JOIN kbpo_today KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_Belegnummer
    LEFT JOIN kbhp_today KBHP
        ON  KBKP.KBKP_BelegNummer = KBHP.KBKP_BelegNummer
        AND KBHP.KBHP_VTGKtoPositionNr = KBPO.KBPO_VtgKtoPositionNr
    LEFT JOIN kbhp_pos1 KBHH
        ON  KBKP.KBKP_BelegNummer = KBHH.KBKP_BelegNummer

    UNION ALL

    -- ---------------------------------------------------------------------
    -- Branch 2: Ausgleichspositionen
    -- (KBKP_Belegnummer = KBPO_AusgleichBelegnummer; Betraege * -1)
    -- ---------------------------------------------------------------------
    SELECT
          KBKP.KBKP_Belegnummer                                                  AS KBKP_Belegnummer
        , KBPO.KBPO_VtgKtoWiederholPos                                           AS KBPO_VtgKtoWiederholPos
        , KBKP.DOCO_Belegart                                                     AS DOCO_Belegart
        , KBKP.KBKP_BelegDt                                                      AS KBKP_BelegDt
        , KBKP.KBKP_BuchungDt                                                    AS KBKP_BuchungDt
        , KBKP.KBKP_ErstellungVon                                                AS KBKP_ErstellungVon
        , KBKP.KBKP_StorniertBelegNummer                                         AS KBKP_StorniertBelegNummer
        , KBKP.KBKP_StornoBelegNummer                                            AS KBKP_StornoBelegNummer
        , KBKP.DOCO_BelegHerkunft                                                AS DOCO_BelegHerkunft
        , KBPO.KBPO_VtgKtoPositionNr                                             AS KBPO_VtgKtoPositionNr
        , KBPO.KBPO_Teilposition                                                 AS KBPO_Teilposition
        , KBPO.GEFA_GeschaeftFall                                                AS GEFA_GeschaeftFall
        , KBPO.PART_Partner                                                      AS PART_Partner
        , KBPO.KBPO_KtoFindMerkmal                                               AS KBPO_KtoFindMerkmal
        , KBPO.DOCO_Hauptvorgang                                                 AS DOCO_Hauptvorgang
        , KBPO.DOCO_Teilvorgang                                                  AS DOCO_Teilvorgang
        , KBPO.DOCO_Belegtyp                                                     AS DOCO_Belegtyp
        , KBPO.DOCO_VtrKtoTyp                                                    AS DOCO_VtrKtoTyp
        , KBPO.DOCO_Waehrung                                                     AS DOCO_Waehrung
        , KBPO.DOCO_FormArt                                                      AS DOCO_FormArt
        , KBPO.KBPO_GesamtBetrag                                                 AS KBPO_GesamtBetrag
        , KBPO.KBPO_TWhrBetrag      * -1                                         AS KBPO_TWhrBetrag
        , KBPO.KBPO_HbWaehrung                                                   AS KBPO_HbWaehrung
        , KBPO.KBPO_HbBetrag        * -1                                         AS KBPO_HbBetrag
        , KBPO.KBPO_HWhrBetrag1     * -1                                         AS KBPO_HWhrBetrag1
        , KBPO.KBPO_Umrechnungkurs                                               AS KBPO_Umrechnungkurs
        , KBPO.KBPO_NettoFaelligkeitDT                                           AS KBPO_NettoFaelligkeitDT
        , KBPO.VTGP_VtrGegenstand                                                AS VTGP_VtrGegenstand
        , KBPO.KBPO_VtrKtoNummer                                                 AS KBPO_VtrKtoNummer
        , KBPO.KBKP_AusgleichBelegnummer                                         AS KBKP_AusgleichBelegnummer
        , KBPO.KBPO_AusgleichStatus                                              AS KBPO_AusgleichStatus
        , KBPO.KBPO_Ausgleichgrund                                               AS KBPO_Ausgleichgrund
        , KBKP.KBKP_BelegDt                                                      AS KBPO_AusgleichDt
        , KBKP.KBKP_BuchungDt                                                    AS KBPO_AusgleichBuchungDt
        , KBPO.KBPO_HBSachkto                                                    AS KBPO_HBSachkto
        , COALESCE(KBHP.KBHP_SachKto, KBHH.KBHP_SachKto)                         AS KBHP_SachKto
        , COALESCE(KBHP.KBHP_HBAbstimmschluessel, KBHH.KBHP_HBAbstimmschluessel) AS KBHP_HBAbstimmschluessel
        , KBPO.KBPO_Beschreibung                                                 AS KBPO_Beschreibung
        , KBPO.DOCO_SteuerCd                                                     AS DOCO_SteuerCd
        , KBPO.KBPO_WertInternDt                                                 AS KBPO_WertInternDt
        , KBPO.KBPO_Bankverbindung                                               AS KBPO_Bankverbindung
        , KBPO.DOCO_RecordArt                                                    AS DOCO_RecordArt
        , KBKP.DOCO_Buchunggrund                                                 AS DOCO_Buchunggrund
        , CAST('Ausgleichsposition' AS VARCHAR(20))                              AS PositionsArt
    FROM kbkp_today KBKP
    INNER JOIN kbpo_today KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer
    LEFT JOIN kbhp_today KBHP
        ON  KBKP.KBKP_BelegNummer = KBHP.KBKP_BelegNummer
        AND KBHP.KBHP_VTGKtoPositionNr = KBPO.KBPO_VtgKtoPositionNr
    LEFT JOIN kbhp_pos1 KBHH
        ON  KBKP.KBKP_BelegNummer = KBHH.KBKP_BelegNummer
)

SELECT
      UNIO.KBKP_Belegnummer            AS Belegnummer
    , UNIO.KBPO_VtgKtoWiederholPos     AS Wiederholungsposition
    , UNIO.KBPO_VtgKtoPositionNr       AS Belegposition
    , UNIO.KBPO_Teilposition           AS Belegteilposition
    , UNIO.DOCO_Belegart               AS BelegartID
    , UNIO.DOCO_BelegHerkunft          AS HerkunftID
    , UNIO.KBKP_ErstellungVon          AS AngelegtVonID
    , UNIO.KBKP_StornoBelegNummer      AS StorniertDurch
    , UNIO.KBKP_StorniertBelegNummer   AS StornobelegZu
    , UNIO.GEFA_GeschaeftFall          AS GeschaeftsfallID
    , UNIO.DOCO_Hauptvorgang           AS HauptvorgangID
    , UNIO.PART_Partner                AS PartnerID
    , UNIO.KBHP_SachKto                AS SachkontoHBID
    , UNIO.DOCO_Teilvorgang            AS TeilvorgangID
    , UNIO.DOCO_VtrKtoTyp              AS VertragskontotypID
    , UNIO.KBKP_AusgleichBelegnummer   AS Ausgleichsbelegnummer
    , UNIO.KBPO_AusgleichDt            AS Ausgleichsbelegdatum
    , UNIO.KBPO_AusgleichBuchungDt     AS Ausgleichsbuchungsdatum
    , UNIO.KBPO_Ausgleichgrund         AS AusgleichsgrundID
    , UNIO.KBKP_BelegDt                AS Belegdatum
    , UNIO.KBKP_BuchungDt              AS Buchungsdatum
    , UNIO.KBPO_NettoFaelligkeitDT     AS Nettofaelligkeitsdatum
    , UNIO.DOCO_SteuerCd               AS SteuercodeAusFachsystem
    , UNIO.KBPO_AusgleichStatus        AS Ausgleichsstatus
    , UNIO.KBPO_HbWaehrung             AS WaehrungHauptbuchID
    , UNIO.KBPO_HbBetrag               AS BetragHauptbuch
    , CAST('CHF' AS VARCHAR(3))        AS HauswaehrungID
    , UNIO.KBPO_HWhrBetrag1            AS BetragHauswaehrung
    , UNIO.KBPO_GesamtBetrag           AS Gesamtbetrag
    , UNIO.DOCO_Waehrung               AS TransaktionWaehrung
    , UNIO.KBPO_TWhrBetrag             AS BetragTransaktionswaehrung
    , UNIO.DOCO_FormArt                AS Formart
    , UNIO.KBPO_Umrechnungkurs         AS Umrechnungskurs
    , UNIO.KBKP_AusgleichBelegnummer   AS Ausgleichsbeleg
    , UNIO.VTGP_VtrGegenstand          AS Vertragsgegenstand
    , UNIO.KBPO_VtrKtoNummer           AS Vertragskontonummer
    , UNIO.KBHP_HBAbstimmschluessel    AS Abstimmschluessel
    , UNIO.KBPO_Beschreibung           AS TextZurPosition
    , UNIO.PositionsArt                AS Positionsart
    , UNIO.KBPO_HBSachkto              AS SachkontoNBID
    , UNIO.KBPO_WertInternDt           AS Zinsvalutadatum
    , UNIO.KBPO_Bankverbindung         AS BankverbindungID
    , UNIO.DOCO_RecordArt              AS RecordArt
    , UNIO.KBPO_KtoFindMerkmal         AS Kontenfindung
    , UNIO.DOCO_Buchunggrund           AS BuchungsgrundID
    , CURRENT_DATE                     AS TechnischesDatum
FROM unio UNIO;
""".strip()


def _build_kostenbelege_fact_builder_fact_sql() -> str:
    return _build_kostenbelege_fact_builder_fact_sql_for_references(
        kbkp_today=_kostenbelege_fact_builder_result_reference("kbkp_today"),
        kbpo_today=_kostenbelege_fact_builder_result_reference("kbpo_today"),
        kbhp_today=_kostenbelege_fact_builder_result_reference("kbhp_today"),
        kbhp_pos1=_kostenbelege_fact_builder_result_reference("kbhp_pos1"),
    )


def _build_kostenbelege_fact_builder_pipeline_fact_sql() -> str:
    return _build_kostenbelege_fact_builder_fact_sql_for_references(
        kbkp_today="stage.kbkp_today",
        kbpo_today="stage.kbpo_today",
        kbhp_today="stage.kbhp_today",
        kbhp_pos1="stage.kbhp_pos1",
    )


def _build_kostenbelege_fact_builder_metrics_sql_for_reference(fact_reference: str) -> str:
    return f"""
-- ---- Zelle 6: analytische Test-Query (warm auf dem gespeicherten Fact-Resultat) --------
SELECT
      COUNT(*)                  AS total_rows
    , SUM(BetragHauswaehrung)   AS sum_betrag_hw
    , AVG(BetragHauswaehrung)   AS avg_betrag_hw
    , MIN(BetragHauswaehrung)   AS min_betrag_hw
    , MAX(BetragHauswaehrung)   AS max_betrag_hw
FROM {fact_reference};
""".strip()


def _build_kostenbelege_fact_builder_metrics_sql() -> str:
    return _build_kostenbelege_fact_builder_metrics_sql_for_reference(
        _kostenbelege_fact_builder_result_reference("fact_buchungsbelegposition")
    )


def _build_kostenbelege_fact_builder_pipeline_metrics_sql() -> str:
    return _build_kostenbelege_fact_builder_metrics_sql_for_reference(
        "stage.fact_buchungsbelegposition"
    )


def build_kostenbelege_fact_builder_s3_demo_notebook() -> NotebookDefinition:
    cell_specs = (
        (
            "kostenbelege-fact-builder-cell-1",
            _build_kostenbelege_fact_builder_kbkp_sql(),
            "kbkp_today",
        ),
        (
            "kostenbelege-fact-builder-cell-2",
            _build_kostenbelege_fact_builder_kbpo_sql(),
            "kbpo_today",
        ),
        (
            "kostenbelege-fact-builder-cell-3",
            _build_kostenbelege_fact_builder_kbhp_sql(),
            "kbhp_today",
        ),
        (
            "kostenbelege-fact-builder-cell-4",
            _build_kostenbelege_fact_builder_kbhp_pos1_sql(),
            "kbhp_pos1",
        ),
        (
            "kostenbelege-fact-builder-cell-5",
            _build_kostenbelege_fact_builder_fact_sql(),
            "fact_buchungsbelegposition",
        ),
        (
            "kostenbelege-fact-builder-cell-6",
            _build_kostenbelege_fact_builder_metrics_sql(),
            "fact_buchungsbelegposition_metrics",
        ),
    )
    return NotebookDefinition(
        notebook_id=KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID,
        title="Kostenbelege Fact Builder Stored Results Demo",
        summary=(
            "Runs the six-cell Kostenbelege fact-builder flow with every intermediate "
            "result stored as S3 Parquet so later cells can consume the previous cell outputs."
        ),
        cells=[
            NotebookCellDefinition(
                cell_id=cell_id,
                data_sources=["s3"],
                query_options=_kostenbelege_fact_builder_result_options(result_name),
                sql=sql,
            )
            for cell_id, sql, result_name in cell_specs
        ],
        tags=["s3", "parquet", "duckdb", "result-storage", "kostenbelege", "fact"],
        tree_path=KOSTENBELEGE_FACT_BUILDER_TREE_PATH,
        linked_generator_id=KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID,
        can_edit=False,
        can_delete=False,
        shared=True,
    )


def build_kostenbelege_fact_builder_s3_pipeline_notebook() -> NotebookDefinition:
    pipeline_options = _kostenbelege_fact_builder_pipeline_options()
    stage_ids = {
        "kbkp_today": "stage-kfb-kbkp-today",
        "kbpo_today": "stage-kfb-kbpo-today",
        "kbhp_today": "stage-kfb-kbhp-today",
        "kbhp_pos1": "stage-kfb-kbhp-pos1",
        "fact_buchungsbelegposition": "stage-kfb-fact-buchungsbelegposition",
        "fact_buchungsbelegposition_metrics": "stage-kfb-fact-buchungsbelegposition-metrics",
    }

    return NotebookDefinition(
        notebook_id=KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID,
        title="Kostenbelege Fact Builder Pipeline Demo",
        summary=(
            "Runs the same six-step Kostenbelege fact-builder flow in Pipeline Mode. "
            "Intermediate outputs are materialized as stages and reused with stage aliases."
        ),
        cells=[
            NotebookCellDefinition(
                cell_id="kostenbelege-fact-builder-pipeline-cell-1",
                data_sources=["s3"],
                query_options=pipeline_options,
                sql=_build_kostenbelege_fact_builder_kbkp_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id=stage_ids["kbkp_today"],
                    alias="kbkp_today",
                    title="KBKP Today",
                    description="Filters KBKP document headers to the current technical version.",
                    output_path=_kostenbelege_fact_builder_pipeline_output_path("kbkp_today"),
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-fact-builder-pipeline-cell-2",
                data_sources=["s3"],
                query_options=pipeline_options,
                sql=_build_kostenbelege_fact_builder_kbpo_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id=stage_ids["kbpo_today"],
                    alias="kbpo_today",
                    title="KBPO Today",
                    description="Unions generated KBPO Parquet partitions and filters them to the current technical version.",
                    output_path=_kostenbelege_fact_builder_pipeline_output_path("kbpo_today"),
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-fact-builder-pipeline-cell-3",
                data_sources=["s3"],
                query_options=pipeline_options,
                sql=_build_kostenbelege_fact_builder_kbhp_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id=stage_ids["kbhp_today"],
                    alias="kbhp_today",
                    title="KBHP Today",
                    description="Filters KBHP ledger account assignments to the current technical version.",
                    output_path=_kostenbelege_fact_builder_pipeline_output_path("kbhp_today"),
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-fact-builder-pipeline-cell-4",
                data_sources=[],
                query_options=pipeline_options,
                sql=_build_kostenbelege_fact_builder_pipeline_kbhp_pos1_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id=stage_ids["kbhp_pos1"],
                    alias="kbhp_pos1",
                    title="KBHP Position 1 Fallback",
                    description="Prepares the document-level KBHP fallback table from the materialized KBHP stage.",
                    predecessors=[stage_ids["kbhp_today"]],
                    output_path=_kostenbelege_fact_builder_pipeline_output_path("kbhp_pos1"),
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-fact-builder-pipeline-cell-5",
                data_sources=[],
                query_options=pipeline_options,
                sql=_build_kostenbelege_fact_builder_pipeline_fact_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id=stage_ids["fact_buchungsbelegposition"],
                    alias="fact_buchungsbelegposition",
                    title="Fact Buchungsbelegposition",
                    description="Builds the final Kostenbelege fact table from the materialized header, position, and ledger stages.",
                    predecessors=[
                        stage_ids["kbkp_today"],
                        stage_ids["kbpo_today"],
                        stage_ids["kbhp_today"],
                        stage_ids["kbhp_pos1"],
                    ],
                    output_path=_kostenbelege_fact_builder_pipeline_output_path("fact_buchungsbelegposition"),
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-fact-builder-pipeline-cell-6",
                data_sources=[],
                query_options=pipeline_options,
                sql=_build_kostenbelege_fact_builder_pipeline_metrics_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id=stage_ids["fact_buchungsbelegposition_metrics"],
                    alias="fact_buchungsbelegposition_metrics",
                    title="Fact Metrics",
                    description="Computes the validation metrics from the materialized fact table.",
                    predecessors=[stage_ids["fact_buchungsbelegposition"]],
                    kind="final",
                    output_path=_kostenbelege_fact_builder_pipeline_output_path("fact_buchungsbelegposition_metrics"),
                ),
            ),
        ],
        tags=["s3", "parquet", "duckdb", "pipeline", "kostenbelege", "fact"],
        tree_path=KOSTENBELEGE_FACT_BUILDER_PIPELINE_TREE_PATH,
        linked_generator_id=KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID,
        pipeline_mode="pipeline",
        pipeline_paths=[
            {
                "pathId": "path-kfb-fact-metrics",
                "terminalStageId": stage_ids["fact_buchungsbelegposition_metrics"],
                "label": "Fact metrics",
                "priority": 1,
            }
        ],
        can_edit=True,
        can_delete=True,
        shared=True,
    )


KOSTENBELEGE_3_1_SOURCE_COLUMNS = {
    "KBKP": (
        "KBKP_Belegnummer",
        "DOCO_Belegart",
        "KBKP_BelegDt",
        "KBKP_BuchungDt",
        "KBKP_ErstellungVon",
        "KBKP_StorniertBelegNummer",
        "KBKP_StornoBelegNummer",
        "DOCO_BelegHerkunft",
        "DOCO_Buchunggrund",
        "KBKP_TechBeginnDt",
        "KBKP_TechEndeDt",
    ),
    "KBPO": (
        "KBPO_PositionId",
        "KBKP_AusgleichBelegnummer",
        "KBPO_VtgKtoWiederholPos",
        "KBPO_VtgKtoPositionNr",
        "KBPO_Teilposition",
        "GEFA_GeschaeftFall",
        "PART_Partner",
        "KBPO_KtoFindMerkmal",
        "DOCO_Hauptvorgang",
        "DOCO_Teilvorgang",
        "DOCO_Belegtyp",
        "DOCO_VtrKtoTyp",
        "DOCO_Waehrung",
        "DOCO_FormArt",
        "KBPO_GesamtBetrag",
        "KBPO_TWhrBetrag",
        "KBPO_HbWaehrung",
        "KBPO_HbBetrag",
        "KBPO_HWhrBetrag1",
        "KBPO_Umrechnungkurs",
        "KBPO_NettoFaelligkeitDT",
        "VTGP_VtrGegenstand",
        "KBPO_VtrKtoNummer",
        "KBPO_AusgleichStatus",
        "KBPO_Ausgleichgrund",
        "KBPO_AusgleichDt",
        "KBPO_AusgleichBuchungDt",
        "KBPO_HBSachkto",
        "KBPO_Beschreibung",
        "DOCO_SteuerCd",
        "KBPO_WertInternDt",
        "KBPO_Bankverbindung",
        "DOCO_RecordArt",
        "KBPO_TechBeginnDt",
        "KBPO_TechEndeDt",
    ),
    "KBHP": (
        "KBHP_Id",
        "KBKP_BelegNummer",
        "KBHP_VTGKtoPositionNr",
        "KBHP_SachKto",
        "KBHP_HBAbstimmschluessel",
        "KBHP_TechBeginnDt",
        "KBHP_TechEndeDt",
    ),
    "KBHH": (
        "KBHP_Id",
        "KBKP_BelegNummer",
        "KBHP_VTGKtoPositionNr",
        "KBHP_SachKto",
        "KBHP_HBAbstimmschluessel",
        "KBHP_TechBeginnDt",
        "KBHP_TechEndeDt",
    ),
    "KALE": ("Datum",),
}
KOSTENBELEGE_3_1_SOURCE_COLUMN_LOOKUP = {
    alias: {column.lower(): column for column in columns}
    for alias, columns in KOSTENBELEGE_3_1_SOURCE_COLUMNS.items()
}


def _quote_kostenbelege_3_1_source_columns(sql: str) -> str:
    def replace(match: re.Match[str]) -> str:
        alias = match.group(1)
        column = match.group(2)
        canonical_column = KOSTENBELEGE_3_1_SOURCE_COLUMN_LOOKUP.get(alias, {}).get(
            column.lower(),
            column,
        )
        return f'{alias}."{canonical_column}"'

    return re.sub(
        r"\b(KBKP|KBPO|KBHP|KBHH|KALE)\.([A-Za-z_][A-Za-z0-9_]*)",
        replace,
        sql,
    )


def _build_multi_table_performance_sql(
    *,
    taxpayers_relation: str,
    filings_relation: str,
    assessments_relation: str,
    payments_relation: str,
    audits_relation: str,
    enforcements_relation: str,
    appeals_relation: str,
) -> str:
    return (
        "-- Approximation: highlight quarterly cantonal tax hotspots with high open exposure.\n"
        "-- Logic: join taxpayers, filings, assessments, payments, audits, enforcements, and appeals.\n"
        "-- Result: surface compliance-pressure and appeal-heavy segments by canton, tax type, and sector.\n"
        "WITH active_taxpayers AS (\n"
        "  SELECT\n"
        "    taxpayer_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    industry_sector,\n"
        "    taxpayer_type,\n"
        "    registration_status,\n"
        "    risk_tier\n"
        f"  FROM {taxpayers_relation}\n"
        "  WHERE registration_status IN ('active', 'watchlist')\n"
        "),\n"
        "filing_scope AS (\n"
        "  SELECT\n"
        "    f.filing_id,\n"
        "    f.taxpayer_id,\n"
        "    atp.taxpayer_uid,\n"
        "    atp.canton_code,\n"
        "    atp.industry_sector,\n"
        "    atp.taxpayer_type,\n"
        "    atp.risk_tier,\n"
        "    f.tax_type,\n"
        "    f.filing_status,\n"
        "    CAST(date_trunc('quarter', f.tax_period_end) AS DATE) AS tax_quarter_start,\n"
        "    f.declared_revenue_chf,\n"
        "    f.declared_deduction_chf,\n"
        "    a.assessment_id,\n"
        "    a.assessment_status,\n"
        "    a.assessed_tax_chf,\n"
        "    a.surcharge_chf,\n"
        "    a.waiver_chf,\n"
        "    a.due_date\n"
        f"  FROM {filings_relation} AS f\n"
        "  JOIN active_taxpayers AS atp\n"
        "    ON atp.taxpayer_id = f.taxpayer_id\n"
        f"  JOIN {assessments_relation} AS a\n"
        "    ON a.filing_id = f.filing_id\n"
        "  WHERE f.tax_period_end >= DATE '2024-01-01'\n"
        "    AND f.tax_type IN ('VAT', 'COMPANY_TAX', 'WITHHOLDING_TAX', 'STAMP_DUTY')\n"
        "    AND f.filing_status IN ('under_review', 'assessed', 'escalated', 'closed')\n"
        "),\n"
        "payment_rollup AS (\n"
        "  SELECT\n"
        "    assessment_id,\n"
        "    CAST(ROUND(SUM(collected_tax_chf), 2) AS DECIMAL(18,2)) AS collected_tax_total_chf,\n"
        "    CAST(\n"
        "      ROUND(\n"
        "        SUM(CASE WHEN payment_status IN ('late', 'partial', 'pending') THEN collected_tax_chf ELSE 0 END),\n"
        "        2\n"
        "      )\n"
        "      AS DECIMAL(18,2)\n"
        "    ) AS stressed_collection_chf,\n"
        "    COUNT(*) AS payment_event_count\n"
        f"  FROM {payments_relation}\n"
        "  WHERE settled_at >= TIMESTAMP '2024-01-01 00:00:00'\n"
        "  GROUP BY assessment_id\n"
        "),\n"
        "audit_rollup AS (\n"
        "  SELECT\n"
        "    filing_id,\n"
        "    MAX(audit_risk_score) AS max_audit_risk_score,\n"
        "    CAST(ROUND(SUM(additional_tax_chf), 2) AS DECIMAL(18,2)) AS additional_tax_total_chf,\n"
        "    SUM(CASE WHEN finding_severity IN ('high', 'critical') THEN 1 ELSE 0 END) AS severe_finding_count\n"
        f"  FROM {audits_relation}\n"
        "  WHERE audit_status IN ('open', 'closed', 'escalated')\n"
        "  GROUP BY filing_id\n"
        "),\n"
        "enforcement_rollup AS (\n"
        "  SELECT\n"
        "    assessment_id,\n"
        "    COUNT(*) AS enforcement_action_count,\n"
        "    CAST(ROUND(SUM(enforced_amount_chf), 2) AS DECIMAL(18,2)) AS enforced_amount_total_chf,\n"
        "    MAX(CASE WHEN action_status IN ('active', 'escalated') THEN 1 ELSE 0 END) AS has_active_enforcement\n"
        f"  FROM {enforcements_relation}\n"
        "  WHERE action_stage IN ('notice', 'collection', 'legal')\n"
        "  GROUP BY assessment_id\n"
        "),\n"
        "appeal_rollup AS (\n"
        "  SELECT\n"
        "    assessment_id,\n"
        "    COUNT(*) AS appeal_count,\n"
        "    CAST(ROUND(SUM(contested_amount_chf), 2) AS DECIMAL(18,2)) AS contested_amount_total_chf,\n"
        "    MAX(CASE WHEN appeal_status IN ('open', 'escalated') THEN 1 ELSE 0 END) AS has_open_appeal\n"
        f"  FROM {appeals_relation}\n"
        "  WHERE ruling_stage IN ('cantonal', 'federal', 'tribunal')\n"
        "  GROUP BY assessment_id\n"
        "),\n"
        "joined_positions AS (\n"
        "  SELECT\n"
        "    fs.canton_code,\n"
        "    fs.tax_type,\n"
        "    fs.industry_sector,\n"
        "    fs.taxpayer_type,\n"
        "    fs.risk_tier,\n"
        "    fs.tax_quarter_start,\n"
        "    fs.filing_id,\n"
        "    fs.assessment_id,\n"
        "    fs.assessed_tax_chf,\n"
        "    fs.surcharge_chf,\n"
        "    fs.waiver_chf,\n"
        "    COALESCE(pr.collected_tax_total_chf, 0) AS collected_tax_total_chf,\n"
        "    COALESCE(pr.stressed_collection_chf, 0) AS stressed_collection_chf,\n"
        "    COALESCE(pr.payment_event_count, 0) AS payment_event_count,\n"
        "    COALESCE(ar.max_audit_risk_score, 0) AS max_audit_risk_score,\n"
        "    COALESCE(ar.additional_tax_total_chf, 0) AS additional_tax_total_chf,\n"
        "    COALESCE(ar.severe_finding_count, 0) AS severe_finding_count,\n"
        "    COALESCE(er.enforcement_action_count, 0) AS enforcement_action_count,\n"
        "    COALESCE(er.enforced_amount_total_chf, 0) AS enforced_amount_total_chf,\n"
        "    COALESCE(er.has_active_enforcement, 0) AS has_active_enforcement,\n"
        "    COALESCE(apr.appeal_count, 0) AS appeal_count,\n"
        "    COALESCE(apr.contested_amount_total_chf, 0) AS contested_amount_total_chf,\n"
        "    COALESCE(apr.has_open_appeal, 0) AS has_open_appeal,\n"
        "    GREATEST(\n"
        "      (fs.assessed_tax_chf + fs.surcharge_chf - fs.waiver_chf) - COALESCE(pr.collected_tax_total_chf, 0),\n"
        "      0\n"
        "    ) AS open_tax_exposure_chf\n"
        "  FROM filing_scope AS fs\n"
        "  LEFT JOIN payment_rollup AS pr\n"
        "    ON pr.assessment_id = fs.assessment_id\n"
        "  LEFT JOIN audit_rollup AS ar\n"
        "    ON ar.filing_id = fs.filing_id\n"
        "  LEFT JOIN enforcement_rollup AS er\n"
        "    ON er.assessment_id = fs.assessment_id\n"
        "  LEFT JOIN appeal_rollup AS apr\n"
        "    ON apr.assessment_id = fs.assessment_id\n"
        "),\n"
        "compliance_pressure AS (\n"
        "  SELECT\n"
        "    'compliance_pressure' AS segment,\n"
        "    canton_code,\n"
        "    tax_type,\n"
        "    industry_sector,\n"
        "    tax_quarter_start,\n"
        "    COUNT(*) AS assessment_count,\n"
        "    CAST(ROUND(SUM(assessed_tax_chf + surcharge_chf - waiver_chf), 2) AS DECIMAL(18,2)) AS gross_assessed_total_chf,\n"
        "    CAST(ROUND(SUM(collected_tax_total_chf), 2) AS DECIMAL(18,2)) AS collected_tax_total_chf,\n"
        "    CAST(ROUND(SUM(open_tax_exposure_chf), 2) AS DECIMAL(18,2)) AS open_tax_exposure_total_chf,\n"
        "    CAST(CAST(AVG(max_audit_risk_score) AS DECIMAL(18,2)) AS DOUBLE PRECISION) AS avg_audit_risk_score,\n"
        "    SUM(severe_finding_count) AS severe_finding_count,\n"
        "    SUM(enforcement_action_count) AS enforcement_action_count,\n"
        "    SUM(appeal_count) AS appeal_count,\n"
        "    SUM(CASE WHEN has_active_enforcement = 1 OR has_open_appeal = 1 THEN 1 ELSE 0 END) AS escalated_case_count\n"
        "  FROM joined_positions\n"
        "  WHERE taxpayer_type IN ('Corporation', 'SME', 'Importer')\n"
        "    AND risk_tier IN ('high', 'critical', 'medium')\n"
        "  GROUP BY canton_code, tax_type, industry_sector, tax_quarter_start\n"
        "  HAVING COUNT(*) >= 20\n"
        "    AND SUM(open_tax_exposure_chf) >= 250000\n"
        "),\n"
        "appeal_exposure AS (\n"
        "  SELECT\n"
        "    'appeal_exposure' AS segment,\n"
        "    canton_code,\n"
        "    tax_type,\n"
        "    industry_sector,\n"
        "    tax_quarter_start,\n"
        "    COUNT(*) AS assessment_count,\n"
        "    CAST(ROUND(SUM(assessed_tax_chf + surcharge_chf - waiver_chf), 2) AS DECIMAL(18,2)) AS gross_assessed_total_chf,\n"
        "    CAST(ROUND(SUM(collected_tax_total_chf), 2) AS DECIMAL(18,2)) AS collected_tax_total_chf,\n"
        "    CAST(ROUND(SUM(open_tax_exposure_chf), 2) AS DECIMAL(18,2)) AS open_tax_exposure_total_chf,\n"
        "    CAST(CAST(AVG(max_audit_risk_score) AS DECIMAL(18,2)) AS DOUBLE PRECISION) AS avg_audit_risk_score,\n"
        "    SUM(severe_finding_count) AS severe_finding_count,\n"
        "    SUM(enforcement_action_count) AS enforcement_action_count,\n"
        "    SUM(appeal_count) AS appeal_count,\n"
        "    SUM(CASE WHEN has_active_enforcement = 1 OR has_open_appeal = 1 THEN 1 ELSE 0 END) AS escalated_case_count\n"
        "  FROM joined_positions\n"
        "  WHERE has_open_appeal = 1 OR has_active_enforcement = 1\n"
        "  GROUP BY canton_code, tax_type, industry_sector, tax_quarter_start\n"
        "  HAVING COUNT(*) >= 8\n"
        "    AND SUM(contested_amount_total_chf) >= 100000\n"
        ")\n"
        "SELECT\n"
        "  segment,\n"
        "  canton_code,\n"
        "  tax_type,\n"
        "  industry_sector,\n"
        "  tax_quarter_start,\n"
        "  assessment_count,\n"
        "  gross_assessed_total_chf,\n"
        "  collected_tax_total_chf,\n"
        "  open_tax_exposure_total_chf,\n"
        "  avg_audit_risk_score,\n"
        "  severe_finding_count,\n"
        "  enforcement_action_count,\n"
        "  appeal_count,\n"
        "  escalated_case_count\n"
        "FROM compliance_pressure\n"
        "UNION ALL\n"
        "SELECT\n"
        "  segment,\n"
        "  canton_code,\n"
        "  tax_type,\n"
        "  industry_sector,\n"
        "  tax_quarter_start,\n"
        "  assessment_count,\n"
        "  gross_assessed_total_chf,\n"
        "  collected_tax_total_chf,\n"
        "  open_tax_exposure_total_chf,\n"
        "  avg_audit_risk_score,\n"
        "  severe_finding_count,\n"
        "  enforcement_action_count,\n"
        "  appeal_count,\n"
        "  escalated_case_count\n"
        "FROM appeal_exposure\n"
        "WHERE gross_assessed_total_chf >= 500000\n"
        "ORDER BY open_tax_exposure_total_chf DESC, avg_audit_risk_score DESC, gross_assessed_total_chf DESC\n"
        "LIMIT 40;"
    )


def _build_mwa_abrechnung_performance_sql(
    *,
    abrechnung_relation: str,
    ziffern_relation: str,
) -> str:
    return (
        "-- Approximation: summarize MWA Abrechnung submissions with their ziffer totals.\n"
        "-- Logic: join Abrechnung parent rows to Abrechnungs-Ziffern children and aggregate by quarter, status, and approval readiness.\n"
        "-- Result: rank the MWA segments with the largest submitted turnover and tax totals.\n"
        "WITH scoped_abrechnungen AS (\n"
        "  SELECT\n"
        "    id_,\n"
        "    status,\n"
        "    CAST(date_trunc('quarter', einreiche_datum) AS DATE) AS submission_quarter,\n"
        "    contact_role,\n"
        "    is_approved,\n"
        "    is_ready_for_audit,\n"
        "    is_sub_form1056_needed,\n"
        "    tax_period_refer,\n"
        "    partner_id,\n"
        "    CAST(CAST(rounded_total AS DOUBLE PRECISION) AS DECIMAL(18,2)) AS rounded_total_chf\n"
        f"  FROM {abrechnung_relation}\n"
        "  WHERE deleted_at IS NULL\n"
        "    AND einreiche_datum >= TIMESTAMP '2024-01-01 00:00:00'\n"
        "),\n"
        "ziffer_rollup AS (\n"
        "  SELECT\n"
        "    abrechnung_refer,\n"
        "    COUNT(*) AS ziffer_count,\n"
        "    CAST(SUM(umsatz) AS DECIMAL(18,2)) AS umsatz_total,\n"
        "    CAST(SUM(steuer) AS DECIMAL(18,2)) AS steuer_total,\n"
        "    CAST(AVG(satz) AS DECIMAL(18,2)) AS avg_satz,\n"
        "    SUM(CASE WHEN satz_editable = 'true' THEN 1 ELSE 0 END) AS editable_ziffer_count\n"
        f"  FROM {ziffern_relation}\n"
        "  WHERE deleted_at IS NULL\n"
        "  GROUP BY abrechnung_refer\n"
        "),\n"
        "joined_abrechnungen AS (\n"
        "  SELECT\n"
        "    a.status,\n"
        "    a.submission_quarter,\n"
        "    a.contact_role,\n"
        "    a.is_approved,\n"
        "    a.is_ready_for_audit,\n"
        "    a.is_sub_form1056_needed,\n"
        "    a.tax_period_refer,\n"
        "    a.partner_id,\n"
        "    a.rounded_total_chf,\n"
        "    COALESCE(z.ziffer_count, 0) AS ziffer_count,\n"
        "    COALESCE(z.umsatz_total, 0) AS umsatz_total,\n"
        "    COALESCE(z.steuer_total, 0) AS steuer_total,\n"
        "    COALESCE(z.avg_satz, 0) AS avg_satz,\n"
        "    COALESCE(z.editable_ziffer_count, 0) AS editable_ziffer_count\n"
        "  FROM scoped_abrechnungen AS a\n"
        "  LEFT JOIN ziffer_rollup AS z\n"
        "    ON z.abrechnung_refer = a.id_\n"
        ")\n"
        "SELECT\n"
        "  submission_quarter,\n"
        "  status,\n"
        "  is_approved,\n"
        "  is_ready_for_audit,\n"
        "  is_sub_form1056_needed,\n"
        "  COUNT(*) AS abrechnung_count,\n"
        "  SUM(ziffer_count) AS ziffer_count,\n"
        "  CAST(SUM(rounded_total_chf) AS DECIMAL(18,2)) AS rounded_total_chf,\n"
        "  CAST(SUM(umsatz_total) AS DECIMAL(18,2)) AS umsatz_total,\n"
        "  CAST(SUM(steuer_total) AS DECIMAL(18,2)) AS steuer_total,\n"
        "  CAST(AVG(avg_satz) AS DECIMAL(18,2)) AS avg_satz,\n"
        "  SUM(editable_ziffer_count) AS editable_ziffer_count,\n"
        "  COUNT(DISTINCT partner_id) AS partner_count\n"
        "FROM joined_abrechnungen\n"
        "GROUP BY submission_quarter, status, is_approved, is_ready_for_audit, is_sub_form1056_needed\n"
        "HAVING COUNT(*) >= 10\n"
        "ORDER BY rounded_total_chf DESC, steuer_total DESC, abrechnung_count DESC\n"
        "LIMIT 40;"
    )


def _build_mwa_art_index_demo_sql(
    *,
    abrechnung_relation: str,
) -> str:
    return f"""
-- DuckDB ART indexes apply to DuckDB tables, not directly to S3 Parquet views.
-- This cell materializes the S3 Parquet relation, creates a single-column ART index,
-- then uses EXPLAIN ANALYZE to verify that the point lookup can use an index scan.
CREATE SCHEMA IF NOT EXISTS art_demo;

DROP TABLE IF EXISTS art_demo.mwa_abrechnung_entities_cache;

CREATE TABLE art_demo.mwa_abrechnung_entities_cache AS
SELECT *
FROM {abrechnung_relation};

CREATE INDEX mwa_abrechnung_art_id_idx
ON art_demo.mwa_abrechnung_entities_cache (id_);

EXPLAIN ANALYZE
SELECT
  id_,
  status,
  einreiche_datum,
  rounded_total,
  partner_id
FROM art_demo.mwa_abrechnung_entities_cache
WHERE id_ = 1;
""".strip()


def _mwa_pipeline_stage(
    *,
    stage_id: str,
    alias: str,
    title: str,
    description: str,
    predecessors: list[str] | None = None,
    kind: str = "intermediate",
) -> dict[str, object]:
    return {
        "enabled": True,
        "stageId": stage_id,
        "alias": alias,
        "title": title,
        "description": description,
        "kind": kind,
        "materialize": True,
        "predecessorStageIds": predecessors or [],
    }


def _mwa_pipeline_loader_status_sql() -> str:
    return (
        "SELECT 'Run the MWA Abrechnung Multi-Format Loader (3.2) from the "
        "Loader Workbench first. This pipeline seed switches to five Parquet "
        "stages after the generated Parquet relations are discovered.' AS status;"
    )


def _build_mwa_pipeline_abrechnung_scope_sql(*, abrechnung_relation: str) -> str:
    return f"""
-- Pipeline split of the MWA Abrechnung S3 Parquet performance notebook.
-- Stage 1: normalize parent Abrechnung rows and derive approval/audit buckets.
WITH scoped_abrechnungen AS (
  SELECT
    id_,
    status,
    CAST(date_trunc('quarter', einreiche_datum) AS DATE) AS submission_quarter,
    CAST(einreiche_datum AS TIMESTAMP) AS submitted_at,
    contact_role,
    is_approved,
    is_ready_for_audit,
    is_sub_form1056_needed,
    tax_period_refer,
    partner_id,
    uid,
    moe_id,
    beguenstigter_partner_id,
    approval_message_address,
    CAST(CAST(rounded_total AS DOUBLE PRECISION) AS DECIMAL(18,2)) AS rounded_total_chf,
    CASE
      WHEN is_approved = 'true' AND is_ready_for_audit = 'true' THEN 'approved_ready'
      WHEN is_approved = 'true' THEN 'approved_waiting_audit'
      WHEN is_ready_for_audit = 'true' THEN 'audit_ready_not_approved'
      ELSE 'open_workflow'
    END AS approval_readiness_bucket,
    CASE
      WHEN is_sub_form1056_needed = 'true' AND is_ready_for_audit <> 'true' THEN 4
      WHEN is_sub_form1056_needed = 'true' THEN 3
      WHEN is_ready_for_audit <> 'true' THEN 2
      ELSE 1
    END AS audit_priority_score
  FROM {abrechnung_relation}
  WHERE deleted_at IS NULL
    AND einreiche_datum >= TIMESTAMP '2024-01-01 00:00:00'
),
ranked_abrechnungen AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY submission_quarter, status
      ORDER BY rounded_total_chf DESC, audit_priority_score DESC, id_ DESC
    ) AS status_amount_rank
  FROM scoped_abrechnungen
)
SELECT
  id_,
  status,
  submission_quarter,
  submitted_at,
  contact_role,
  is_approved,
  is_ready_for_audit,
  is_sub_form1056_needed,
  tax_period_refer,
  partner_id,
  uid,
  moe_id,
  beguenstigter_partner_id,
  approval_message_address,
  rounded_total_chf,
  approval_readiness_bucket,
  audit_priority_score,
  status_amount_rank
FROM ranked_abrechnungen
WHERE status_amount_rank <= 5000
""".strip()


def _build_mwa_pipeline_ziffer_rollup_sql(*, ziffern_relation: str) -> str:
    return f"""
-- Pipeline split of the MWA Abrechnung S3 Parquet performance notebook.
-- Stage 2: aggregate Abrechnungs-Ziffern children by parent Abrechnung id.
WITH scoped_ziffern AS (
  SELECT
    id_,
    abrechnung_refer,
    ziffer_nummer,
    CAST(umsatz AS DECIMAL(18,2)) AS umsatz_chf,
    CAST(steuer AS DECIMAL(18,2)) AS steuer_chf,
    CAST(satz AS DECIMAL(18,4)) AS satz_percent,
    steuersatz_type,
    satz_editable,
    kommentar,
    moe_id
  FROM {ziffern_relation}
  WHERE deleted_at IS NULL
),
ziffer_rollup AS (
  SELECT
    abrechnung_refer,
    COUNT(*) AS ziffer_count,
    COUNT(DISTINCT ziffer_nummer) AS distinct_ziffer_count,
    CAST(SUM(umsatz_chf) AS DECIMAL(18,2)) AS umsatz_total_chf,
    CAST(SUM(steuer_chf) AS DECIMAL(18,2)) AS steuer_total_chf,
    CAST(AVG(satz_percent) AS DECIMAL(18,4)) AS avg_satz_percent,
    CAST(MAX(satz_percent) AS DECIMAL(18,4)) AS max_satz_percent,
    SUM(CASE WHEN satz_editable = 'true' THEN 1 ELSE 0 END) AS editable_ziffer_count,
    SUM(CASE WHEN steuer_chf = 0 THEN 1 ELSE 0 END) AS zero_tax_ziffer_count,
    SUM(CASE WHEN kommentar IS NOT NULL AND kommentar <> '' THEN 1 ELSE 0 END) AS commented_ziffer_count
  FROM scoped_ziffern
  GROUP BY abrechnung_refer
)
SELECT
  abrechnung_refer,
  ziffer_count,
  distinct_ziffer_count,
  umsatz_total_chf,
  steuer_total_chf,
  avg_satz_percent,
  max_satz_percent,
  editable_ziffer_count,
  zero_tax_ziffer_count,
  commented_ziffer_count,
  CASE
    WHEN ziffer_count = 0 THEN 'missing_children'
    WHEN zero_tax_ziffer_count > ziffer_count / 2 THEN 'mostly_zero_tax'
    WHEN editable_ziffer_count > 0 THEN 'editable_positions'
    ELSE 'standard_positions'
  END AS ziffer_quality_bucket
FROM ziffer_rollup
""".strip()


def _build_mwa_pipeline_joined_sql() -> str:
    return """
-- Stage 3: join parent Abrechnungen with their Ziffer rollup once.
-- Downstream stages fork from this shared materialized result.
WITH joined_abrechnungen AS (
  SELECT
    a.id_,
    a.status,
    a.submission_quarter,
    a.submitted_at,
    a.contact_role,
    a.is_approved,
    a.is_ready_for_audit,
    a.is_sub_form1056_needed,
    a.tax_period_refer,
    a.partner_id,
    a.uid,
    a.moe_id,
    a.beguenstigter_partner_id,
    a.approval_message_address,
    a.rounded_total_chf,
    a.approval_readiness_bucket,
    a.audit_priority_score,
    a.status_amount_rank,
    COALESCE(z.ziffer_count, 0) AS ziffer_count,
    COALESCE(z.distinct_ziffer_count, 0) AS distinct_ziffer_count,
    COALESCE(z.umsatz_total_chf, 0) AS umsatz_total_chf,
    COALESCE(z.steuer_total_chf, 0) AS steuer_total_chf,
    COALESCE(z.avg_satz_percent, 0) AS avg_satz_percent,
    COALESCE(z.max_satz_percent, 0) AS max_satz_percent,
    COALESCE(z.editable_ziffer_count, 0) AS editable_ziffer_count,
    COALESCE(z.zero_tax_ziffer_count, 0) AS zero_tax_ziffer_count,
    COALESCE(z.commented_ziffer_count, 0) AS commented_ziffer_count,
    COALESCE(z.ziffer_quality_bucket, 'no_ziffern') AS ziffer_quality_bucket
  FROM stage.mwa_abrechnung_scope AS a
  LEFT JOIN stage.mwa_ziffer_rollup AS z
    ON z.abrechnung_refer = a.id_
),
classified_abrechnungen AS (
  SELECT
    *,
    CAST(rounded_total_chf - umsatz_total_chf AS DECIMAL(18,2)) AS turnover_reconciliation_gap_chf,
    CAST(steuer_total_chf / NULLIF(umsatz_total_chf, 0) AS DECIMAL(18,6)) AS effective_tax_rate,
    CASE
      WHEN ziffer_count = 0 THEN 'missing_ziffern'
      WHEN ABS(rounded_total_chf - umsatz_total_chf) > 100000 THEN 'large_reconciliation_gap'
      WHEN editable_ziffer_count > 0 THEN 'manual_tax_rate_review'
      WHEN zero_tax_ziffer_count > 0 THEN 'contains_zero_tax_positions'
      ELSE 'standard_review'
    END AS review_reason
  FROM joined_abrechnungen
)
SELECT
  *
FROM classified_abrechnungen
WHERE rounded_total_chf >= 5000
""".strip()


def _build_mwa_pipeline_status_pressure_sql() -> str:
    return """
-- Stage 4: branch A, quarterly status and approval pressure.
WITH quarterly_status_pressure AS (
  SELECT
    submission_quarter,
    status,
    approval_readiness_bucket,
    ziffer_quality_bucket,
    COUNT(*) AS abrechnung_count,
    COUNT(DISTINCT partner_id) AS partner_count,
    CAST(SUM(rounded_total_chf) AS DECIMAL(18,2)) AS rounded_total_chf,
    CAST(SUM(umsatz_total_chf) AS DECIMAL(18,2)) AS umsatz_total_chf,
    CAST(SUM(steuer_total_chf) AS DECIMAL(18,2)) AS steuer_total_chf,
    CAST(AVG(effective_tax_rate) AS DECIMAL(18,6)) AS avg_effective_tax_rate,
    SUM(ziffer_count) AS ziffer_count,
    SUM(editable_ziffer_count) AS editable_ziffer_count,
    SUM(zero_tax_ziffer_count) AS zero_tax_ziffer_count,
    SUM(CASE WHEN review_reason <> 'standard_review' THEN 1 ELSE 0 END) AS review_case_count,
    SUM(CASE WHEN is_sub_form1056_needed = 'true' THEN 1 ELSE 0 END) AS sub_form1056_count
  FROM stage.mwa_joined_abrechnungen
  GROUP BY submission_quarter, status, approval_readiness_bucket, ziffer_quality_bucket
),
ranked_pressure AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY submission_quarter
      ORDER BY rounded_total_chf DESC, steuer_total_chf DESC, review_case_count DESC
    ) AS quarter_rank
  FROM quarterly_status_pressure
  WHERE abrechnung_count >= 10
)
SELECT
  submission_quarter,
  status,
  approval_readiness_bucket,
  ziffer_quality_bucket,
  abrechnung_count,
  partner_count,
  rounded_total_chf,
  umsatz_total_chf,
  steuer_total_chf,
  avg_effective_tax_rate,
  ziffer_count,
  editable_ziffer_count,
  zero_tax_ziffer_count,
  review_case_count,
  sub_form1056_count,
  quarter_rank
FROM ranked_pressure
WHERE quarter_rank <= 25
ORDER BY submission_quarter DESC, quarter_rank, rounded_total_chf DESC
""".strip()


def _build_mwa_pipeline_audit_backlog_sql() -> str:
    return """
-- Stage 5: branch B, audit readiness backlog and contact-role ownership.
WITH audit_worklist AS (
  SELECT
    contact_role,
    approval_message_address,
    tax_period_refer,
    review_reason,
    CASE
      WHEN is_ready_for_audit = 'true' AND is_approved = 'true' THEN 'ready_and_approved'
      WHEN is_ready_for_audit = 'true' THEN 'ready_waiting_approval'
      WHEN is_sub_form1056_needed = 'true' THEN 'blocked_on_form1056'
      ELSE 'workflow_backlog'
    END AS audit_lane,
    COUNT(*) AS abrechnung_count,
    COUNT(DISTINCT partner_id) AS partner_count,
    CAST(SUM(rounded_total_chf) AS DECIMAL(18,2)) AS rounded_total_chf,
    CAST(SUM(steuer_total_chf) AS DECIMAL(18,2)) AS steuer_total_chf,
    CAST(SUM(ABS(turnover_reconciliation_gap_chf)) AS DECIMAL(18,2)) AS abs_reconciliation_gap_chf,
    CAST(AVG(audit_priority_score) AS DECIMAL(18,2)) AS avg_audit_priority_score,
    MAX(submitted_at) AS latest_submission_at,
    SUM(CASE WHEN ziffer_count = 0 THEN 1 ELSE 0 END) AS missing_ziffer_count,
    SUM(CASE WHEN editable_ziffer_count > 0 THEN 1 ELSE 0 END) AS editable_case_count
  FROM stage.mwa_joined_abrechnungen
  WHERE review_reason <> 'standard_review'
     OR is_ready_for_audit <> 'true'
     OR is_sub_form1056_needed = 'true'
  GROUP BY contact_role, approval_message_address, tax_period_refer, review_reason, audit_lane
),
ranked_worklist AS (
  SELECT
    *,
    DENSE_RANK() OVER (
      PARTITION BY audit_lane
      ORDER BY rounded_total_chf DESC, abs_reconciliation_gap_chf DESC, abrechnung_count DESC
    ) AS audit_lane_rank
  FROM audit_worklist
)
SELECT
  contact_role,
  approval_message_address,
  tax_period_refer,
  review_reason,
  audit_lane,
  abrechnung_count,
  partner_count,
  rounded_total_chf,
  steuer_total_chf,
  abs_reconciliation_gap_chf,
  avg_audit_priority_score,
  latest_submission_at,
  missing_ziffer_count,
  editable_case_count,
  audit_lane_rank
FROM ranked_worklist
WHERE audit_lane_rank <= 30
ORDER BY audit_lane, audit_lane_rank, rounded_total_chf DESC
""".strip()


def build_mwa_s3_parquet_pipeline_notebook(
    *,
    mwa_s3_parquet_relations: dict[str, str | None],
) -> NotebookDefinition:
    abrechnung_relation = (
        mwa_s3_parquet_relations.get("mwa_abrechnung_entities") or ""
    )
    ziffern_relation = (
        mwa_s3_parquet_relations.get("mwa_abrechnungs_ziffern_entities") or ""
    )
    abrechnung_sources = _s3_data_sources(abrechnung_relation)
    ziffern_sources = _s3_data_sources(ziffern_relation)

    if abrechnung_relation and ziffern_relation:
        cells = [
            NotebookCellDefinition(
                cell_id="mwa-parquet-pipeline-cell-1",
                data_sources=abrechnung_sources,
                query_options={"duckdb": {"parquetHivePartitioning": "auto"}},
                sql=_build_mwa_pipeline_abrechnung_scope_sql(
                    abrechnung_relation=abrechnung_relation,
                ),
                stage=_mwa_pipeline_stage(
                    stage_id="stage-mwa-abrechnung-scope",
                    alias="mwa_abrechnung_scope",
                    title="MWA Abrechnung Scope",
                    description="Normalize generated MWA Abrechnung parent rows from S3 Parquet.",
                ),
            ),
            NotebookCellDefinition(
                cell_id="mwa-parquet-pipeline-cell-2",
                data_sources=ziffern_sources,
                query_options={"duckdb": {"parquetHivePartitioning": "auto"}},
                sql=_build_mwa_pipeline_ziffer_rollup_sql(
                    ziffern_relation=ziffern_relation,
                ),
                stage=_mwa_pipeline_stage(
                    stage_id="stage-mwa-ziffer-rollup",
                    alias="mwa_ziffer_rollup",
                    title="MWA Ziffer Rollup",
                    description="Aggregate generated Abrechnungs-Ziffern children from S3 Parquet.",
                ),
            ),
            NotebookCellDefinition(
                cell_id="mwa-parquet-pipeline-cell-3",
                data_sources=[],
                sql=_build_mwa_pipeline_joined_sql(),
                stage=_mwa_pipeline_stage(
                    stage_id="stage-mwa-joined-abrechnungen",
                    alias="mwa_joined_abrechnungen",
                    title="Joined Abrechnungen",
                    description="Join parent Abrechnungen to ziffer totals before the analytics fork.",
                    predecessors=[
                        "stage-mwa-abrechnung-scope",
                        "stage-mwa-ziffer-rollup",
                    ],
                ),
            ),
            NotebookCellDefinition(
                cell_id="mwa-parquet-pipeline-cell-4",
                data_sources=[],
                sql=_build_mwa_pipeline_status_pressure_sql(),
                stage=_mwa_pipeline_stage(
                    stage_id="stage-mwa-status-pressure",
                    alias="mwa_status_pressure",
                    title="Status Pressure",
                    description="Branch A: summarize quarterly approval and status pressure.",
                    predecessors=["stage-mwa-joined-abrechnungen"],
                    kind="final",
                ),
            ),
            NotebookCellDefinition(
                cell_id="mwa-parquet-pipeline-cell-5",
                data_sources=[],
                sql=_build_mwa_pipeline_audit_backlog_sql(),
                stage=_mwa_pipeline_stage(
                    stage_id="stage-mwa-audit-backlog",
                    alias="mwa_audit_backlog",
                    title="Audit Backlog",
                    description="Branch B: prioritize audit readiness and form 1056 backlog.",
                    predecessors=["stage-mwa-joined-abrechnungen"],
                    kind="final",
                ),
            ),
        ]
    else:
        cells = [
            NotebookCellDefinition(
                cell_id="mwa-parquet-pipeline-loader-status",
                data_sources=[],
                sql=_mwa_pipeline_loader_status_sql(),
                stage=_mwa_pipeline_stage(
                    stage_id="stage-mwa-loader-status",
                    alias="mwa_loader_status",
                    title="Run MWA Loader",
                    description="The generated MWA 3.2 S3 Parquet relations were not discovered yet.",
                    kind="final",
                ),
            )
        ]

    return NotebookDefinition(
        notebook_id=MWA_PARQUET_PIPELINE_NOTEBOOK_ID,
        title="MWA Abrechnung (3.2) S3 Parquet Pipeline",
        summary=(
            "Editable five-stage PoC pipeline over the generated MWA 3.2 S3 Parquet "
            "data, split into two downstream analytics branches."
        ),
        cells=cells,
        tags=["performance", "mwa", "abrechnung", "s3", "parquet", "pipeline"],
        tree_path=MWA_PARQUET_PIPELINE_TREE_PATH,
        linked_generator_id="mwa_abrechnung_multi_format_loader",
        pipeline_mode="pipeline",
        can_edit=True,
        can_delete=True,
        shared=True,
        created_at=MWA_PARQUET_PIPELINE_CREATED_AT,
    )


def _build_kostenbelege_3_1_sql(
    *,
    kbkp_relation: str,
    kbpo_relation: str,
    kbhp_relation: str,
    kalender_relation: str,
    quote_source_columns: bool = False,
) -> str:
    sql = f"""
WITH UNIO AS
    (
    SELECT
          KBKP.KBKP_Belegnummer                                                   AS KBKP_Belegnummer
        , KBPO.KBPO_VtgKtoWiederholPos                                            AS KBPO_VtgKtoWiederholPos
        , KBKP.DOCO_Belegart                                                      AS DOCO_Belegart
        , KBKP.KBKP_BelegDt                                                       AS KBKP_BelegDt
        , KBKP.KBKP_BuchungDt                                                     AS KBKP_BuchungDt
        , KBKP.KBKP_ErstellungVon                                                 AS KBKP_ErstellungVon
        , KBKP.KBKP_StorniertBelegNummer                                          AS KBKP_StorniertBelegNummer
        , KBKP.KBKP_StornoBelegNummer                                             AS KBKP_StornoBelegNummer
        , KBKP.DOCO_BelegHerkunft                                                 AS DOCO_BelegHerkunft
        , KBPO.KBPO_VtgKtoPositionNr                                              AS KBPO_VtgKtoPositionNr
        , KBPO.KBPO_Teilposition                                                  AS KBPO_Teilposition
        , KBPO.GEFA_GeschaeftFall                                                 AS GEFA_GeschaeftFall
        , KBPO.PART_Partner                                                       AS PART_Partner
        , KBPO.KBPO_KtoFindMerkmal                                                AS KBPO_KtoFindMerkmal
        , KBPO.DOCO_Hauptvorgang                                                  AS DOCO_Hauptvorgang
        , KBPO.DOCO_Teilvorgang                                                   AS DOCO_Teilvorgang
        , KBPO.DOCO_Belegtyp                                                      AS DOCO_Belegtyp
        , KBPO.DOCO_VtrKtoTyp                                                     AS DOCO_VtrKtoTyp
        , KBPO.DOCO_Waehrung                                                      AS DOCO_Waehrung
        , KBPO.DOCO_FormArt                                                       AS DOCO_FormArt
        , KBPO.KBPO_GesamtBetrag                                                  AS KBPO_GesamtBetrag
        , KBPO.KBPO_TWhrBetrag                                                    AS KBPO_TWhrBetrag
        , KBPO.KBPO_HbWaehrung                                                    AS KBPO_HbWaehrung
        , KBPO.KBPO_HbBetrag                                                      AS KBPO_HbBetrag
        , KBPO.KBPO_HWhrBetrag1                                                   AS KBPO_HWhrBetrag1
        , KBPO.KBPO_Umrechnungkurs                                                AS KBPO_Umrechnungkurs
        , KBPO.KBPO_NettoFaelligkeitDT                                            AS KBPO_NettoFaelligkeitDT
        , KBPO.VTGP_VtrGegenstand                                                 AS VTGP_VtrGegenstand
        , KBPO.KBPO_VtrKtoNummer                                                  AS KBPO_VtrKtoNummer
        , KBPO.KBKP_ausgleichbelegnummer                                          AS KBKP_ausgleichbelegnummer
        , KBPO.KBPO_ausgleichstatus                                               AS KBPO_ausgleichstatus
        , KBPO.KBPO_Ausgleichgrund                                                AS KBPO_Ausgleichgrund
        , KBPO.KBPO_AusgleichDt                                                   AS KBPO_AusgleichDt
        , KBPO.KBPO_AusgleichBuchungDt                                            AS KBPO_AusgleichBuchungDt
        , KBPO.KBPO_HBSachkto                                                     AS KBPO_HBSachkto
        , COALESCE(KBHP.KBHP_SachKto, KBHH.KBHP_SachKto)                          AS KBHP_SachKto
        , COALESCE(KBHP.KBHP_HBAbstimmschluessel, KBHH.KBHP_HBAbstimmschluessel)  AS KBHP_HBAbstimmschluessel
        , KBPO.KBPO_Beschreibung                                                  AS KBPO_Beschreibung
        , KBPO.DOCO_SteuerCd                                                      AS DOCO_SteuerCd
        , KBPO.KBPO_WertInternDt                                                  AS KBPO_WertInternDt
        , KBPO.KBPO_Bankverbindung                                                AS KBPO_Bankverbindung
        , KBPO.DOCO_RecordArt                                                     AS DOCO_RecordArt
        , KBKP.DOCO_Buchunggrund                                                  AS DOCO_Buchunggrund
        , CAST('Originalposition' AS VARCHAR(20))                                 AS PositionsArt
        , KALE.Datum                                                              AS Datum
    FROM {kbkp_relation} KBKP
    INNER JOIN {kalender_relation} KALE
        ON  KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
        AND KALE.Datum = CURRENT_DATE
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
          KBKP.KBKP_Belegnummer                                                   AS KBKP_Belegnummer
        , KBPO.KBPO_VtgKtoWiederholPos                                            AS KBPO_VtgKtoWiederholPos
        , KBKP.DOCO_Belegart                                                      AS DOCO_Belegart
        , KBKP.KBKP_BelegDt                                                       AS KBKP_BelegDt
        , KBKP.KBKP_BuchungDt                                                     AS KBKP_BuchungDt
        , KBKP.KBKP_ErstellungVon                                                 AS KBKP_ErstellungVon
        , KBKP.KBKP_StorniertBelegNummer                                          AS KBKP_StorniertBelegNummer
        , KBKP.KBKP_StornoBelegNummer                                             AS KBKP_StornoBelegNummer
        , KBKP.DOCO_BelegHerkunft                                                 AS DOCO_BelegHerkunft
        , KBPO.KBPO_VtgKtoPositionNr                                              AS KBPO_VtgKtoPositionNr
        , KBPO.KBPO_Teilposition                                                  AS KBPO_Teilposition
        , KBPO.GEFA_GeschaeftFall                                                 AS GEFA_GeschaeftFall
        , KBPO.PART_Partner                                                       AS PART_Partner
        , KBPO.KBPO_KtoFindMerkmal                                                AS KBPO_KtoFindMerkmal
        , KBPO.DOCO_Hauptvorgang                                                  AS DOCO_Hauptvorgang
        , KBPO.DOCO_Teilvorgang                                                   AS DOCO_Teilvorgang
        , KBPO.DOCO_Belegtyp                                                      AS DOCO_Belegtyp
        , KBPO.DOCO_VtrKtoTyp                                                     AS DOCO_VtrKtoTyp
        , KBPO.DOCO_Waehrung                                                      AS DOCO_Waehrung
        , KBPO.DOCO_FormArt                                                       AS DOCO_FormArt
        , KBPO.KBPO_GesamtBetrag                                                  AS KBPO_GesamtBetrag
        , KBPO.KBPO_TWhrBetrag        * -1                                        AS KBPO_TWhrBetrag
        , KBPO.KBPO_HbWaehrung                                                    AS KBPO_HbWaehrung
        , KBPO.KBPO_HbBetrag          * -1                                        AS KBPO_HbBetrag
        , KBPO.KBPO_HWhrBetrag1       * -1                                        AS KBPO_HWhrBetrag1
        , KBPO.KBPO_Umrechnungkurs                                                AS KBPO_Umrechnungkurs
        , KBPO.KBPO_NettoFaelligkeitDT                                            AS KBPO_NettoFaelligkeitDT
        , KBPO.VTGP_VtrGegenstand                                                 AS VTGP_VtrGegenstand
        , KBPO.KBPO_VtrKtoNummer                                                  AS KBPO_VtrKtoNummer
        , KBPO.KBKP_AusgleichBelegnummer                                          AS KBKP_AusgleichBelegnummer
        , KBPO.KBPO_AusgleichStatus                                               AS KBPO_AusgleichStatus
        , KBPO.KBPO_Ausgleichgrund                                                AS KBPO_Ausgleichgrund
        , KBKP.KBKP_BelegDt                                                       AS KBPO_AusgleichDt
        , KBKP.KBKP_BuchungDt                                                     AS KBPO_AusgleichBuchungDt
        , KBPO.KBPO_HBSachkto                                                     AS KBPO_HBSachkto
        , COALESCE(KBHP.KBHP_SachKto, KBHH.KBHP_SachKto)                          AS KBHP_SachKto
        , COALESCE(KBHP.KBHP_HBAbstimmschluessel, KBHH.KBHP_HBAbstimmschluessel)  AS KBHP_HBAbstimmschluessel
        , KBPO.KBPO_Beschreibung                                                  AS KBPO_Beschreibung
        , KBPO.DOCO_SteuerCd                                                      AS DOCO_SteuerCd
        , KBPO.KBPO_WertInternDt                                                  AS KBPO_WertInternDt
        , KBPO.KBPO_Bankverbindung                                                AS KBPO_Bankverbindung
        , KBPO.DOCO_RecordArt                                                     AS DOCO_RecordArt
        , KBKP.DOCO_Buchunggrund                                                  AS DOCO_Buchunggrund
        , CAST('Ausgleichsposition' AS VARCHAR(20))                               AS PositionsArt
        , KALE.Datum                                                              AS Datum
    FROM {kbkp_relation} KBKP
    INNER JOIN {kalender_relation} KALE
        ON  KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
        AND KALE.Datum = CURRENT_DATE
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
      UNIO.KBKP_Belegnummer                     AS Belegnummer
    , UNIO.KBPO_VtgKtoWiederholPos              AS Wiederholungsposition
    , UNIO.KBPO_VtgKtoPositionNr                AS Belegposition
    , UNIO.KBPO_Teilposition                    AS Belegteilposition
    , UNIO.DOCO_Belegart                        AS BelegartID
    , UNIO.DOCO_BelegHerkunft                   AS HerkunftID
    , UNIO.KBKP_ErstellungVon                   AS AngelegtVonID
    , UNIO.KBKP_StornoBelegNummer               AS StorniertDurch
    , UNIO.KBKP_StorniertBelegNummer            AS StornobelegZu
    , UNIO.GEFA_GeschaeftFall                   AS GeschaeftsfallID
    , UNIO.DOCO_Hauptvorgang                    AS HauptvorgangID
    , UNIO.PART_Partner                         AS PartnerID
    , UNIO.KBHP_SachKto                         AS SachkontoHBID
    , UNIO.DOCO_Teilvorgang                     AS TeilvorgangID
    , UNIO.DOCO_VtrKtoTyp                       AS VertragskontotypID
    , UNIO.KBKP_ausgleichbelegnummer            AS Ausgleichsbelegnummer
    , UNIO.KBPO_AusgleichDt                     AS Ausgleichsbelegdatum
    , UNIO.KBPO_AusgleichBuchungDt              AS Ausgleichsbuchungsdatum
    , UNIO.KBPO_Ausgleichgrund                  AS AusgleichsgrundID
    , UNIO.KBKP_BelegDt                         AS Belegdatum
    , UNIO.KBKP_BuchungDt                       AS Buchungsdatum
    , UNIO.KBPO_NettofaelligkeitDt              AS Nettofaelligkeitsdatum
    , UNIO.DOCO_SteuerCd                        AS SteuercodeAusFachsystem
    , UNIO.KBPO_ausgleichstatus                 AS Ausgleichsstatus
    , UNIO.KBPO_HbWaehrung                      AS WaehrungHauptbuchID
    , UNIO.KBPO_HbBetrag                        AS BetragHauptbuch
    , CAST('CHF' AS VARCHAR(3))                 AS HauswaehrungID
    , UNIO.KBPO_HWhrBetrag1                     AS BetragHauswaehrung
    , UNIO.KBPO_GesamtBetrag                    AS Gesamtbetrag
    , UNIO.DOCO_Waehrung                        AS TransaktionWaehrung
    , UNIO.KBPO_TWhrBetrag                      AS BetragTransaktionswaehrung
    , UNIO.DOCO_FormArt                         AS Formart
    , UNIO.KBPO_Umrechnungkurs                  AS Umrechnungskurs
    , UNIO.KBKP_ausgleichbelegnummer            AS Ausgleichsbeleg
    , UNIO.VTGP_VtrGegenstand                   AS Vertragsgegenstand
    , UNIO.KBPO_VtrKtoNummer                    AS Vertragskontonummer
    , UNIO.KBHP_HBAbstimmschluessel             AS Abstimmschluessel
    , UNIO.KBPO_Beschreibung                    AS TextZurPosition
    , UNIO.Positionsart                         AS Positionsart
    , UNIO.KBPO_HBSachkto                       AS SachkontoNBID
    , UNIO.KBPO_WertInternDt                    AS Zinsvalutadatum
    , UNIO.KBPO_Bankverbindung                  AS BankverbindungID
    , UNIO.DOCO_RecordArt                       AS RecordArt
    , UNIO.KBPO_KtoFindMerkmal                  AS Kontenfindung
    , UNIO.DOCO_Buchunggrund                    AS BuchungsgrundID
    , UNIO.Datum                                AS TechnischesDatum
FROM UNIO;
""".strip()
    if not quote_source_columns:
        return sql
    return _quote_kostenbelege_3_1_source_columns(sql)


def _kostenbelege_3_1_problem_loader_status_sql() -> str:
    return (
        "SELECT 'Run the Kostenbelege Multi-Source Loader (3.1) from the Loader "
        "Workbench first. This problem-solving seed switches to five S3 Parquet "
        "investigation cells after KBKP, KBPO, KBHP, and DIM_KALENDER are "
        "discovered.' AS status;"
    )


def _build_kostenbelege_3_1_problem_readiness_sql(
    *,
    kbkp_relation: str,
    kbpo_relation: str,
    kbhp_relation: str,
    kalender_relation: str,
) -> str:
    sql = f"""
SELECT
      'KBKP' AS SourceName
    , COUNT(*) AS RowCount
    , MIN(KBKP.KBKP_TechBeginnDt) AS MinTechnicalDate
    , MAX(KBKP.KBKP_TechEndeDt) AS MaxTechnicalDate
FROM {kbkp_relation} KBKP
UNION ALL
SELECT
      'KBPO' AS SourceName
    , COUNT(*) AS RowCount
    , MIN(KBPO.KBPO_TechBeginnDt) AS MinTechnicalDate
    , MAX(KBPO.KBPO_TechEndeDt) AS MaxTechnicalDate
FROM {kbpo_relation} KBPO
UNION ALL
SELECT
      'KBHP' AS SourceName
    , COUNT(*) AS RowCount
    , MIN(KBHP.KBHP_TechBeginnDt) AS MinTechnicalDate
    , MAX(KBHP.KBHP_TechEndeDt) AS MaxTechnicalDate
FROM {kbhp_relation} KBHP
UNION ALL
SELECT
      'DIM_KALENDER' AS SourceName
    , COUNT(*) AS RowCount
    , MIN(KALE.Datum) AS MinTechnicalDate
    , MAX(KALE.Datum) AS MaxTechnicalDate
FROM {kalender_relation} KALE
ORDER BY SourceName;
""".strip()
    return _quote_kostenbelege_3_1_source_columns(sql)


def _build_kostenbelege_3_1_problem_calendar_sql(
    *,
    kalender_relation: str,
) -> str:
    sql = f"""
SELECT
      COUNT(*) AS CalendarRows
    , MIN(KALE.Datum) AS FirstCalendarDate
    , MAX(KALE.Datum) AS LastCalendarDate
FROM {kalender_relation} KALE
WHERE KALE.Datum BETWEEN DATE '2018-07-01' AND CURRENT_DATE;
""".strip()
    return _quote_kostenbelege_3_1_source_columns(sql)


def _build_kostenbelege_3_1_original_semantics_sql(
    *,
    kbkp_relation: str,
    kbpo_relation: str,
    kbhp_relation: str,
    kalender_relation: str,
) -> str:
    sql = _build_kostenbelege_3_1_sql(
        kbkp_relation=kbkp_relation,
        kbpo_relation=kbpo_relation,
        kbhp_relation=kbhp_relation,
        kalender_relation=kalender_relation,
        quote_source_columns=True,
    )
    sql = sql.replace(
        'AND KALE."Datum" = CURRENT_DATE',
        'AND KALE."Datum" BETWEEN DATE \'2018-07-01\' AND CURRENT_DATE',
    )
    sql = sql.replace(
        'KBKP."KBKP_Belegnummer" = KBPO."KBKP_AusgleichBelegnummer"',
        'KBKP."KBKP_Belegnummer" = KBPO.KBKP_Belegnummer',
        1,
    )
    return sql


def _build_kostenbelege_3_1_original_explain_sql(
    *,
    kbkp_relation: str,
    kbpo_relation: str,
    kbhp_relation: str,
    kalender_relation: str,
) -> str:
    full_sql = _build_kostenbelege_3_1_original_semantics_sql(
        kbkp_relation=kbkp_relation,
        kbpo_relation=kbpo_relation,
        kbhp_relation=kbhp_relation,
        kalender_relation=kalender_relation,
    ).rstrip(";")
    return f"EXPLAIN {full_sql};"


def _build_kostenbelege_3_1_original_branch_counts_sql(
    *,
    kbkp_relation: str,
    kbpo_relation: str,
    kbhp_relation: str,
    kalender_relation: str,
) -> str:
    del kbhp_relation
    sql = f"""
WITH branch_counts AS (
    SELECT
          CAST('Originalposition' AS VARCHAR(20)) AS PositionsArt
        , COUNT(*) AS RowCount
    FROM {kbkp_relation} KBKP
    INNER JOIN {kalender_relation} KALE
        ON  KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
        AND KALE.Datum BETWEEN DATE '2018-07-01' AND CURRENT_DATE
    INNER JOIN {kbpo_relation} KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_Belegnummer
        AND KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
    UNION ALL
    SELECT
          CAST('Ausgleichsposition' AS VARCHAR(20)) AS PositionsArt
        , COUNT(*) AS RowCount
    FROM {kbkp_relation} KBKP
    INNER JOIN {kalender_relation} KALE
        ON  KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
        AND KALE.Datum BETWEEN DATE '2018-07-01' AND CURRENT_DATE
    INNER JOIN {kbpo_relation} KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer
        AND KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
)
SELECT
      PositionsArt
    , RowCount
FROM branch_counts
ORDER BY PositionsArt;
""".strip()
    return _quote_kostenbelege_3_1_source_columns(sql)


def _build_kostenbelege_3_1_optimized_sql(
    *,
    kbkp_relation: str,
    kbpo_relation: str,
    kbhp_relation: str,
    kalender_relation: str,
) -> str:
    return f"""
WITH current_kalender AS (
    SELECT Datum
    FROM {kalender_relation}
    WHERE Datum = CURRENT_DATE
),
base_positions AS (
    SELECT
          KBKP.KBKP_Belegnummer
        , KBPO.KBPO_VtgKtoWiederholPos
        , KBKP.DOCO_Belegart
        , KBKP.KBKP_BelegDt
        , KBKP.KBKP_BuchungDt
        , KBKP.KBKP_ErstellungVon
        , KBKP.KBKP_StorniertBelegNummer
        , KBKP.KBKP_StornoBelegNummer
        , KBKP.DOCO_BelegHerkunft
        , KBPO.KBPO_VtgKtoPositionNr
        , KBPO.KBPO_Teilposition
        , KBPO.GEFA_GeschaeftFall
        , KBPO.PART_Partner
        , KBPO.KBPO_KtoFindMerkmal
        , KBPO.DOCO_Hauptvorgang
        , KBPO.DOCO_Teilvorgang
        , KBPO.DOCO_Belegtyp
        , KBPO.DOCO_VtrKtoTyp
        , KBPO.DOCO_Waehrung
        , KBPO.DOCO_FormArt
        , KBPO.KBPO_GesamtBetrag
        , KBPO.KBPO_TWhrBetrag
        , KBPO.KBPO_HbWaehrung
        , KBPO.KBPO_HbBetrag
        , KBPO.KBPO_HWhrBetrag1
        , KBPO.KBPO_Umrechnungkurs
        , KBPO.KBPO_NettoFaelligkeitDT
        , KBPO.VTGP_VtrGegenstand
        , KBPO.KBPO_VtrKtoNummer
        , KBPO.KBKP_AusgleichBelegnummer
        , KBPO.KBPO_AusgleichStatus
        , KBPO.KBPO_Ausgleichgrund
        , KBPO.KBPO_AusgleichDt
        , KBPO.KBPO_AusgleichBuchungDt
        , KBPO.KBPO_HBSachkto
        , KBPO.KBPO_Beschreibung
        , KBPO.DOCO_SteuerCd
        , KBPO.KBPO_WertInternDt
        , KBPO.KBPO_Bankverbindung
        , KBPO.DOCO_RecordArt
        , KBKP.DOCO_Buchunggrund
        , KALE.Datum
    FROM {kbkp_relation} KBKP
    INNER JOIN current_kalender KALE
        ON KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
    INNER JOIN {kbpo_relation} KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer
        AND KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
),
position_specific AS (
    SELECT
          BP.*
        , KBHP.KBKP_BelegNummer AS KBHP_MatchedBelegNummer
        , KBHP.KBHP_SachKto AS KBHP_MatchedSachKto
        , KBHP.KBHP_HBAbstimmschluessel AS KBHP_MatchedHBAbstimmschluessel
    FROM base_positions BP
    LEFT JOIN {kbhp_relation} KBHP
        ON  BP.KBKP_Belegnummer = KBHP.KBKP_BelegNummer
        AND KBHP.KBHP_VTGKtoPositionNr = BP.KBPO_VtgKtoPositionNr
        AND BP.Datum BETWEEN KBHP.KBHP_TechBeginnDt AND KBHP.KBHP_TechEndeDt
),
resolved_positions AS (
    SELECT
          PS.*
        , PS.KBHP_MatchedSachKto AS KBHP_SachKto
        , PS.KBHP_MatchedHBAbstimmschluessel AS KBHP_HBAbstimmschluessel
    FROM position_specific PS
    WHERE PS.KBHP_MatchedBelegNummer IS NOT NULL
    UNION ALL
    SELECT
          PS.*
        , KBHH.KBHP_SachKto AS KBHP_SachKto
        , KBHH.KBHP_HBAbstimmschluessel AS KBHP_HBAbstimmschluessel
    FROM position_specific PS
    LEFT JOIN {kbhp_relation} KBHH
        ON  PS.KBKP_Belegnummer = KBHH.KBKP_BelegNummer
        AND PS.Datum BETWEEN KBHH.KBHP_TechBeginnDt AND KBHH.KBHP_TechEndeDt
        AND KBHH.KBHP_VTGKtoPositionNr = 1
    WHERE PS.KBHP_MatchedBelegNummer IS NULL
)
SELECT
      RP.KBKP_Belegnummer                     AS Belegnummer
    , RP.KBPO_VtgKtoWiederholPos              AS Wiederholungsposition
    , RP.KBPO_VtgKtoPositionNr                AS Belegposition
    , RP.KBPO_Teilposition                    AS Belegteilposition
    , RP.DOCO_Belegart                        AS BelegartID
    , RP.DOCO_BelegHerkunft                   AS HerkunftID
    , RP.KBKP_ErstellungVon                   AS AngelegtVonID
    , RP.KBKP_StornoBelegNummer               AS StorniertDurch
    , RP.KBKP_StorniertBelegNummer            AS StornobelegZu
    , RP.GEFA_GeschaeftFall                   AS GeschaeftsfallID
    , RP.DOCO_Hauptvorgang                    AS HauptvorgangID
    , RP.PART_Partner                         AS PartnerID
    , RP.KBHP_SachKto                         AS SachkontoHBID
    , RP.DOCO_Teilvorgang                     AS TeilvorgangID
    , RP.DOCO_VtrKtoTyp                       AS VertragskontotypID
    , RP.KBKP_AusgleichBelegnummer            AS Ausgleichsbelegnummer
    , CASE
          WHEN Positions.PositionsArt = 'Originalposition' THEN RP.KBPO_AusgleichDt
          ELSE RP.KBKP_BelegDt
      END                                     AS Ausgleichsbelegdatum
    , CASE
          WHEN Positions.PositionsArt = 'Originalposition' THEN RP.KBPO_AusgleichBuchungDt
          ELSE RP.KBKP_BuchungDt
      END                                     AS Ausgleichsbuchungsdatum
    , RP.KBPO_Ausgleichgrund                  AS AusgleichsgrundID
    , RP.KBKP_BelegDt                         AS Belegdatum
    , RP.KBKP_BuchungDt                       AS Buchungsdatum
    , RP.KBPO_NettoFaelligkeitDT              AS Nettofaelligkeitsdatum
    , RP.DOCO_SteuerCd                        AS SteuercodeAusFachsystem
    , RP.KBPO_AusgleichStatus                 AS Ausgleichsstatus
    , RP.KBPO_HbWaehrung                      AS WaehrungHauptbuchID
    , RP.KBPO_HbBetrag * Positions.AmountSign AS BetragHauptbuch
    , CAST('CHF' AS VARCHAR(3))               AS HauswaehrungID
    , RP.KBPO_HWhrBetrag1 * Positions.AmountSign AS BetragHauswaehrung
    , RP.KBPO_GesamtBetrag                    AS Gesamtbetrag
    , RP.DOCO_Waehrung                        AS TransaktionWaehrung
    , RP.KBPO_TWhrBetrag * Positions.AmountSign AS BetragTransaktionswaehrung
    , RP.DOCO_FormArt                         AS Formart
    , RP.KBPO_Umrechnungkurs                  AS Umrechnungskurs
    , RP.KBKP_AusgleichBelegnummer            AS Ausgleichsbeleg
    , RP.VTGP_VtrGegenstand                   AS Vertragsgegenstand
    , RP.KBPO_VtrKtoNummer                    AS Vertragskontonummer
    , RP.KBHP_HBAbstimmschluessel             AS Abstimmschluessel
    , RP.KBPO_Beschreibung                    AS TextZurPosition
    , Positions.PositionsArt                  AS Positionsart
    , RP.KBPO_HBSachkto                       AS SachkontoNBID
    , RP.KBPO_WertInternDt                    AS Zinsvalutadatum
    , RP.KBPO_Bankverbindung                  AS BankverbindungID
    , RP.DOCO_RecordArt                       AS RecordArt
    , RP.KBPO_KtoFindMerkmal                  AS Kontenfindung
    , RP.DOCO_Buchunggrund                    AS BuchungsgrundID
    , RP.Datum                                AS TechnischesDatum
FROM resolved_positions RP
CROSS JOIN (VALUES
    (CAST('Originalposition' AS VARCHAR(20)), 1),
    (CAST('Ausgleichsposition' AS VARCHAR(20)), -1)
) AS Positions(PositionsArt, AmountSign);
""".strip()


def _build_kostenbelege_3_1_full_kbpo_union_sql() -> str:
    return "\n    UNION ALL\n    ".join(
        f"SELECT * FROM {relation}"
        for relation in KOSTENBELEGE_3_1_FULL_KBPO_RELATIONS
    )


def _build_kostenbelege_3_1_optimized_dataset_copy_sql() -> str:
    base_sql = _build_kostenbelege_3_1_sql(
        kbkp_relation="s3.core.kbkpfull.parquet",
        kbpo_relation="kbpofull",
        kbhp_relation="s3.core.kbhpfull.parquet",
        kalender_relation="current_kalender",
        quote_source_columns=False,
    ).rstrip(";")
    base_sql = base_sql.replace(
        "WITH UNIO AS",
        (
            "WITH kbpofull AS (\n"
            f"    {_build_kostenbelege_3_1_full_kbpo_union_sql()}\n"
            "),\n"
            "current_kalender AS (\n"
            "    SELECT Datum\n"
            "    FROM s3.n_3_1_imports.dim_kalender.parquet\n"
            "    WHERE Datum = CURRENT_DATE\n"
            "),\n"
            "UNIO AS"
        ),
        1,
    )
    base_sql = base_sql.replace(
        "KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer",
        "KBKP.KBKP_Belegnummer = KBPO.KBKP_Belegnummer",
        1,
    )
    return f"""
-- Materialize the full Kostenbelege 3.1 FACT_Buchungsbelegposition snapshot.
-- The kbpofull CTE intentionally uses ANSI-style UNION ALL over virtual S3
-- dot-notation; the DuckDB SQL translation collapses it to
-- read_parquet([...], union_by_name = true).
COPY (
{base_sql}
)
TO '{KOSTENBELEGE_3_1_OPTIMIZED_DATASET_TARGET}'
(
    FORMAT parquet,
    COMPRESSION zstd,
    ROW_GROUP_SIZE 250000,
    PER_THREAD_OUTPUT true,
    FILE_SIZE_BYTES '450MB'
);
""".strip()


def _build_kostenbelege_3_1_optimized_dataset_query_sql() -> str:
    return f"""
-- Query the optimized Parquet dataset written by Cell 1.
SELECT
      COUNT(*)                AS cnt
    , SUM(BetragHauswaehrung) AS total
FROM {KOSTENBELEGE_3_1_OPTIMIZED_DATASET_RELATION}
WHERE Buchungsdatum >= DATE '2023-01-01'
  AND Buchungsdatum <  DATE '2024-01-01';
""".strip()


def _kostenbelege_pipeline_stage(
    *,
    stage_id: str,
    alias: str,
    title: str,
    description: str,
    predecessors: list[str] | None = None,
    kind: str = "intermediate",
    output_path: str = "",
) -> dict[str, object]:
    stage = {
        "enabled": True,
        "stageId": stage_id,
        "alias": alias,
        "title": title,
        "description": description,
        "kind": kind,
        "materialize": True,
        "predecessorStageIds": predecessors or [],
    }
    if output_path:
        stage["outputPath"] = output_path
    return stage


def _kostenbelege_pipeline_loader_status_sql() -> str:
    return (
        "SELECT 'Run the Kostenbelege Multi-Source Loader (3.1) from the Loader "
        "Workbench first. This pipeline seed switches to nine S3 Parquet stages "
        "after KBKP, KBPO, KBHP, and DIM_KALENDER are discovered.' AS status;"
    )


def _build_kostenbelege_pipeline_headers_sql(
    *,
    kbkp_relation: str,
    kalender_relation: str,
) -> str:
    return f"""
-- Stage 1: current technical document headers from generated S3 Parquet.
WITH current_kalender AS (
  SELECT Datum
  FROM {kalender_relation}
  WHERE Datum = CURRENT_DATE
)
SELECT
    KBKP.KBKP_Belegnummer,
    KBKP.DOCO_Belegart,
    KBKP.KBKP_BelegDt,
    KBKP.KBKP_BuchungDt,
    KBKP.KBKP_ErstellungVon,
    KBKP.KBKP_StorniertBelegNummer,
    KBKP.KBKP_StornoBelegNummer,
    KBKP.DOCO_BelegHerkunft,
    KBKP.DOCO_Buchunggrund,
    KALE.Datum
FROM {kbkp_relation} KBKP
INNER JOIN current_kalender KALE
  ON KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
""".strip()


def _build_kostenbelege_pipeline_positions_sql(
    *,
    kbpo_relation: str,
    kalender_relation: str,
) -> str:
    return f"""
-- Stage 2: current technical line positions from generated S3 Parquet.
WITH current_kalender AS (
  SELECT Datum
  FROM {kalender_relation}
  WHERE Datum = CURRENT_DATE
)
SELECT
    KBPO.KBPO_PositionId,
    KBPO.KBKP_AusgleichBelegnummer,
    KBPO.KBPO_VtgKtoWiederholPos,
    KBPO.KBPO_VtgKtoPositionNr,
    KBPO.KBPO_Teilposition,
    KBPO.GEFA_GeschaeftFall,
    KBPO.PART_Partner,
    KBPO.KBPO_KtoFindMerkmal,
    KBPO.DOCO_Hauptvorgang,
    KBPO.DOCO_Teilvorgang,
    KBPO.DOCO_Belegtyp,
    KBPO.DOCO_VtrKtoTyp,
    KBPO.DOCO_Waehrung,
    KBPO.DOCO_FormArt,
    KBPO.KBPO_GesamtBetrag,
    KBPO.KBPO_TWhrBetrag,
    KBPO.KBPO_HbWaehrung,
    KBPO.KBPO_HbBetrag,
    KBPO.KBPO_HWhrBetrag1,
    KBPO.KBPO_Umrechnungkurs,
    KBPO.KBPO_NettoFaelligkeitDT,
    KBPO.VTGP_VtrGegenstand,
    KBPO.KBPO_VtrKtoNummer,
    KBPO.KBPO_AusgleichStatus,
    KBPO.KBPO_Ausgleichgrund,
    KBPO.KBPO_AusgleichDt,
    KBPO.KBPO_AusgleichBuchungDt,
    KBPO.KBPO_HBSachkto,
    KBPO.KBPO_Beschreibung,
    KBPO.DOCO_SteuerCd,
    KBPO.KBPO_WertInternDt,
    KBPO.KBPO_Bankverbindung,
    KBPO.DOCO_RecordArt,
    KALE.Datum
FROM {kbpo_relation} KBPO
INNER JOIN current_kalender KALE
  ON KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
""".strip()


def _build_kostenbelege_pipeline_ledger_accounts_sql(
    *,
    kbhp_relation: str,
    kalender_relation: str,
) -> str:
    return f"""
-- Stage 3: current technical ledger account assignments from generated S3 Parquet.
WITH current_kalender AS (
  SELECT Datum
  FROM {kalender_relation}
  WHERE Datum = CURRENT_DATE
)
SELECT
    KBHP.KBHP_Id,
    KBHP.KBKP_BelegNummer,
    KBHP.KBHP_VTGKtoPositionNr,
    KBHP.KBHP_SachKto,
    KBHP.KBHP_HBAbstimmschluessel,
    KALE.Datum
FROM {kbhp_relation} KBHP
INNER JOIN current_kalender KALE
  ON KALE.Datum BETWEEN KBHP.KBHP_TechBeginnDt AND KBHP.KBHP_TechEndeDt
""".strip()


def _build_kostenbelege_pipeline_resolved_positions_sql() -> str:
    return """
-- Stage 4: resolve document headers, positions, and ledger account fallback once.
SELECT
    H.KBKP_Belegnummer,
    P.KBPO_VtgKtoWiederholPos,
    H.DOCO_Belegart,
    H.KBKP_BelegDt,
    H.KBKP_BuchungDt,
    H.KBKP_ErstellungVon,
    H.KBKP_StorniertBelegNummer,
    H.KBKP_StornoBelegNummer,
    H.DOCO_BelegHerkunft,
    P.KBPO_VtgKtoPositionNr,
    P.KBPO_Teilposition,
    P.GEFA_GeschaeftFall,
    P.PART_Partner,
    P.KBPO_KtoFindMerkmal,
    P.DOCO_Hauptvorgang,
    P.DOCO_Teilvorgang,
    P.DOCO_Belegtyp,
    P.DOCO_VtrKtoTyp,
    P.DOCO_Waehrung,
    P.DOCO_FormArt,
    P.KBPO_GesamtBetrag,
    P.KBPO_TWhrBetrag,
    P.KBPO_HbWaehrung,
    P.KBPO_HbBetrag,
    P.KBPO_HWhrBetrag1,
    P.KBPO_Umrechnungkurs,
    P.KBPO_NettoFaelligkeitDT,
    P.VTGP_VtrGegenstand,
    P.KBPO_VtrKtoNummer,
    P.KBKP_AusgleichBelegnummer,
    P.KBPO_AusgleichStatus,
    P.KBPO_Ausgleichgrund,
    P.KBPO_AusgleichDt,
    P.KBPO_AusgleichBuchungDt,
    P.KBPO_HBSachkto,
    COALESCE(EXACT_ACCOUNT.KBHP_SachKto, FALLBACK_ACCOUNT.KBHP_SachKto) AS KBHP_SachKto,
    COALESCE(EXACT_ACCOUNT.KBHP_HBAbstimmschluessel, FALLBACK_ACCOUNT.KBHP_HBAbstimmschluessel) AS KBHP_HBAbstimmschluessel,
    CASE
      WHEN EXACT_ACCOUNT.KBKP_BelegNummer IS NOT NULL THEN 'position_specific'
      WHEN FALLBACK_ACCOUNT.KBKP_BelegNummer IS NOT NULL THEN 'document_fallback'
      ELSE 'missing'
    END AS ledger_resolution,
    P.KBPO_Beschreibung,
    P.DOCO_SteuerCd,
    P.KBPO_WertInternDt,
    P.KBPO_Bankverbindung,
    P.DOCO_RecordArt,
    H.DOCO_Buchunggrund,
    H.Datum
FROM stage.kb_current_headers H
INNER JOIN stage.kb_current_positions P
  ON H.KBKP_Belegnummer = P.KBKP_AusgleichBelegnummer
 AND H.Datum = P.Datum
LEFT JOIN stage.kb_current_ledger_accounts EXACT_ACCOUNT
  ON H.KBKP_Belegnummer = EXACT_ACCOUNT.KBKP_BelegNummer
 AND EXACT_ACCOUNT.KBHP_VTGKtoPositionNr = P.KBPO_VtgKtoPositionNr
 AND H.Datum = EXACT_ACCOUNT.Datum
LEFT JOIN stage.kb_current_ledger_accounts FALLBACK_ACCOUNT
  ON H.KBKP_Belegnummer = FALLBACK_ACCOUNT.KBKP_BelegNummer
 AND FALLBACK_ACCOUNT.KBHP_VTGKtoPositionNr = 1
 AND H.Datum = FALLBACK_ACCOUNT.Datum
 AND EXACT_ACCOUNT.KBKP_BelegNummer IS NULL
""".strip()


def _build_kostenbelege_pipeline_position_projection_sql(
    *,
    positions_art: str,
    amount_sign: int,
    settlement_dates: bool,
) -> str:
    ausgleichsbelegdatum = "RP.KBKP_BelegDt" if settlement_dates else "RP.KBPO_AusgleichDt"
    ausgleichsbuchungsdatum = "RP.KBKP_BuchungDt" if settlement_dates else "RP.KBPO_AusgleichBuchungDt"
    return f"""
-- Stage projection: {positions_art} business semantics from resolved Kostenbelege positions.
SELECT
    RP.KBKP_Belegnummer AS Belegnummer,
    RP.KBPO_VtgKtoWiederholPos AS Wiederholungsposition,
    RP.KBPO_VtgKtoPositionNr AS Belegposition,
    RP.KBPO_Teilposition AS Belegteilposition,
    RP.DOCO_Belegart AS BelegartID,
    RP.DOCO_BelegHerkunft AS HerkunftID,
    RP.KBKP_ErstellungVon AS AngelegtVonID,
    RP.KBKP_StornoBelegNummer AS StorniertDurch,
    RP.KBKP_StorniertBelegNummer AS StornobelegZu,
    RP.GEFA_GeschaeftFall AS GeschaeftsfallID,
    RP.DOCO_Hauptvorgang AS HauptvorgangID,
    RP.PART_Partner AS PartnerID,
    RP.KBHP_SachKto AS SachkontoHBID,
    RP.DOCO_Teilvorgang AS TeilvorgangID,
    RP.DOCO_VtrKtoTyp AS VertragskontotypID,
    RP.KBKP_AusgleichBelegnummer AS Ausgleichsbelegnummer,
    {ausgleichsbelegdatum} AS Ausgleichsbelegdatum,
    {ausgleichsbuchungsdatum} AS Ausgleichsbuchungsdatum,
    RP.KBPO_Ausgleichgrund AS AusgleichsgrundID,
    RP.KBKP_BelegDt AS Belegdatum,
    RP.KBKP_BuchungDt AS Buchungsdatum,
    RP.KBPO_NettoFaelligkeitDT AS Nettofaelligkeitsdatum,
    RP.DOCO_SteuerCd AS SteuercodeAusFachsystem,
    RP.KBPO_AusgleichStatus AS Ausgleichsstatus,
    RP.KBPO_HbWaehrung AS WaehrungHauptbuchID,
    RP.KBPO_HbBetrag * {amount_sign} AS BetragHauptbuch,
    CAST('CHF' AS VARCHAR(3)) AS HauswaehrungID,
    RP.KBPO_HWhrBetrag1 * {amount_sign} AS BetragHauswaehrung,
    RP.KBPO_GesamtBetrag AS Gesamtbetrag,
    RP.DOCO_Waehrung AS TransaktionWaehrung,
    RP.KBPO_TWhrBetrag * {amount_sign} AS BetragTransaktionswaehrung,
    RP.DOCO_FormArt AS Formart,
    RP.KBPO_Umrechnungkurs AS Umrechnungskurs,
    RP.KBKP_AusgleichBelegnummer AS Ausgleichsbeleg,
    RP.VTGP_VtrGegenstand AS Vertragsgegenstand,
    RP.KBPO_VtrKtoNummer AS Vertragskontonummer,
    RP.KBHP_HBAbstimmschluessel AS Abstimmschluessel,
    RP.KBPO_Beschreibung AS TextZurPosition,
    CAST('{positions_art}' AS VARCHAR(20)) AS Positionsart,
    RP.KBPO_HBSachkto AS SachkontoNBID,
    RP.KBPO_WertInternDt AS Zinsvalutadatum,
    RP.KBPO_Bankverbindung AS BankverbindungID,
    RP.DOCO_RecordArt AS RecordArt,
    RP.KBPO_KtoFindMerkmal AS Kontenfindung,
    RP.DOCO_Buchunggrund AS BuchungsgrundID,
    RP.Datum AS TechnischesDatum,
    RP.ledger_resolution AS LedgerResolution
FROM stage.kb_resolved_positions RP
""".strip()


def _build_kostenbelege_pipeline_canonical_output_sql() -> str:
    return """
-- Stage 7: canonical Kostenbelege output with the final business projection.
SELECT
    Belegnummer,
    Wiederholungsposition,
    Belegposition,
    Belegteilposition,
    BelegartID,
    HerkunftID,
    AngelegtVonID,
    StorniertDurch,
    StornobelegZu,
    GeschaeftsfallID,
    HauptvorgangID,
    PartnerID,
    SachkontoHBID,
    TeilvorgangID,
    VertragskontotypID,
    Ausgleichsbelegnummer,
    Ausgleichsbelegdatum,
    Ausgleichsbuchungsdatum,
    AusgleichsgrundID,
    Belegdatum,
    Buchungsdatum,
    Nettofaelligkeitsdatum,
    SteuercodeAusFachsystem,
    Ausgleichsstatus,
    WaehrungHauptbuchID,
    BetragHauptbuch,
    HauswaehrungID,
    BetragHauswaehrung,
    Gesamtbetrag,
    TransaktionWaehrung,
    BetragTransaktionswaehrung,
    Formart,
    Umrechnungskurs,
    Ausgleichsbeleg,
    Vertragsgegenstand,
    Vertragskontonummer,
    Abstimmschluessel,
    TextZurPosition,
    Positionsart,
    SachkontoNBID,
    Zinsvalutadatum,
    BankverbindungID,
    RecordArt,
    Kontenfindung,
    BuchungsgrundID,
    TechnischesDatum
FROM stage.kb_original_positions
UNION ALL
SELECT
    Belegnummer,
    Wiederholungsposition,
    Belegposition,
    Belegteilposition,
    BelegartID,
    HerkunftID,
    AngelegtVonID,
    StorniertDurch,
    StornobelegZu,
    GeschaeftsfallID,
    HauptvorgangID,
    PartnerID,
    SachkontoHBID,
    TeilvorgangID,
    VertragskontotypID,
    Ausgleichsbelegnummer,
    Ausgleichsbelegdatum,
    Ausgleichsbuchungsdatum,
    AusgleichsgrundID,
    Belegdatum,
    Buchungsdatum,
    Nettofaelligkeitsdatum,
    SteuercodeAusFachsystem,
    Ausgleichsstatus,
    WaehrungHauptbuchID,
    BetragHauptbuch,
    HauswaehrungID,
    BetragHauswaehrung,
    Gesamtbetrag,
    TransaktionWaehrung,
    BetragTransaktionswaehrung,
    Formart,
    Umrechnungskurs,
    Ausgleichsbeleg,
    Vertragsgegenstand,
    Vertragskontonummer,
    Abstimmschluessel,
    TextZurPosition,
    Positionsart,
    SachkontoNBID,
    Zinsvalutadatum,
    BankverbindungID,
    RecordArt,
    Kontenfindung,
    BuchungsgrundID,
    TechnischesDatum
FROM stage.kb_settlement_positions
""".strip()


def _build_kostenbelege_pipeline_exception_candidates_sql() -> str:
    return """
-- Stage 8: settlement records that deserve audit attention before final backlog grouping.
WITH candidates AS (
  SELECT
      *,
      ABS(COALESCE(BetragHauptbuch, BetragHauswaehrung, BetragTransaktionswaehrung, 0)) AS exposure_chf,
      CASE
        WHEN SachkontoHBID IS NULL OR LedgerResolution = 'missing' THEN 'missing_ledger_account'
        WHEN UPPER(COALESCE(Ausgleichsstatus, '')) IN ('OPEN', 'PARTIAL', 'REVERSED') THEN 'settlement_state_review'
        WHEN StorniertDurch IS NOT NULL OR StornobelegZu IS NOT NULL THEN 'reversal_review'
        WHEN ABS(COALESCE(BetragHauptbuch, BetragHauswaehrung, BetragTransaktionswaehrung, 0)) >= 100000 THEN 'high_value_settlement'
        ELSE 'monitor'
      END AS exception_reason
  FROM stage.kb_settlement_positions
)
SELECT *
FROM candidates
WHERE exception_reason <> 'monitor'
ORDER BY exposure_chf DESC, Belegnummer, Belegposition
""".strip()


def _build_kostenbelege_pipeline_audit_backlog_sql() -> str:
    return """
-- Stage 9: grouped settlement audit backlog for analysts.
WITH grouped_backlog AS (
  SELECT
      exception_reason,
      Ausgleichsstatus,
      AusgleichsgrundID,
      WaehrungHauptbuchID,
      SachkontoHBID,
      COUNT(*) AS position_count,
      COUNT(DISTINCT Belegnummer) AS document_count,
      COUNT(DISTINCT PartnerID) AS partner_count,
      CAST(SUM(exposure_chf) AS DECIMAL(18,2)) AS exposure_total_chf,
      CAST(AVG(exposure_chf) AS DECIMAL(18,2)) AS exposure_avg_chf,
      MIN(Nettofaelligkeitsdatum) AS earliest_due_date,
      MAX(Ausgleichsbuchungsdatum) AS latest_settlement_booking_date
  FROM stage.kb_settlement_exception_candidates
  GROUP BY
      exception_reason,
      Ausgleichsstatus,
      AusgleichsgrundID,
      WaehrungHauptbuchID,
      SachkontoHBID
),
ranked_backlog AS (
  SELECT
      *,
      DENSE_RANK() OVER (
        PARTITION BY exception_reason
        ORDER BY exposure_total_chf DESC, position_count DESC, document_count DESC
      ) AS backlog_rank
  FROM grouped_backlog
)
SELECT *
FROM ranked_backlog
WHERE backlog_rank <= 25
ORDER BY exception_reason, backlog_rank, exposure_total_chf DESC
""".strip()


def build_kostenbelege_3_1_s3_parquet_pipeline_notebook(
    *,
    kostenbelege_3_1_s3_relations: dict[str, str | None],
) -> NotebookDefinition:
    kbkp_relation = kostenbelege_3_1_s3_relations.get("kbkp_2019") or ""
    kbpo_relation = kostenbelege_3_1_s3_relations.get("kbpo_2019") or ""
    kbhp_relation = kostenbelege_3_1_s3_relations.get("kbhp_2019") or ""
    kalender_relation = kostenbelege_3_1_s3_relations.get("dim_kalender") or ""
    kbkp_sources = _s3_data_sources(kbkp_relation, kalender_relation)
    kbpo_sources = _s3_data_sources(kbpo_relation, kalender_relation)
    kbhp_sources = _s3_data_sources(kbhp_relation, kalender_relation)

    if kbkp_relation and kbpo_relation and kbhp_relation and kalender_relation:
        s3_query_options = {"duckdb": {"parquetHivePartitioning": "auto"}}
        cells = [
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-1",
                data_sources=kbkp_sources,
                query_options=s3_query_options,
                sql=_build_kostenbelege_pipeline_headers_sql(
                    kbkp_relation=kbkp_relation,
                    kalender_relation=kalender_relation,
                ),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-current-headers",
                    alias="kb_current_headers",
                    title="KB Current Headers",
                    description="Filters KBKP document headers to today's technical version and keeps document dates, origin, reversal, and booking reason fields.",
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-2",
                data_sources=kbpo_sources,
                query_options=s3_query_options,
                sql=_build_kostenbelege_pipeline_positions_sql(
                    kbpo_relation=kbpo_relation,
                    kalender_relation=kalender_relation,
                ),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-current-positions",
                    alias="kb_current_positions",
                    title="KB Current Positions",
                    description="Filters KBPO line items to today's technical version and keeps partner, contract, amount, currency, due-date, settlement, tax, and bank fields.",
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-3",
                data_sources=kbhp_sources,
                query_options=s3_query_options,
                sql=_build_kostenbelege_pipeline_ledger_accounts_sql(
                    kbhp_relation=kbhp_relation,
                    kalender_relation=kalender_relation,
                ),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-current-ledger-accounts",
                    alias="kb_current_ledger_accounts",
                    title="KB Current Ledger Accounts",
                    description="Prepares current KBHP ledger assignments for exact position matches and document-level fallback account matches.",
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-4",
                data_sources=[],
                sql=_build_kostenbelege_pipeline_resolved_positions_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-resolved-positions",
                    alias="kb_resolved_positions",
                    title="KB Resolved Positions",
                    description="Joins headers, positions, and ledger assignments, resolving exact KBHP account matches before document-level fallback matches.",
                    predecessors=[
                        "stage-kb-current-headers",
                        "stage-kb-current-positions",
                        "stage-kb-current-ledger-accounts",
                    ],
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-5",
                data_sources=[],
                sql=_build_kostenbelege_pipeline_position_projection_sql(
                    positions_art="Originalposition",
                    amount_sign=1,
                    settlement_dates=False,
                ),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-original-positions",
                    alias="kb_original_positions",
                    title="Original Positions",
                    description="Branch A: applies original-position semantics with positive amounts and original document dates.",
                    predecessors=["stage-kb-resolved-positions"],
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-6",
                data_sources=[],
                sql=_build_kostenbelege_pipeline_position_projection_sql(
                    positions_art="Ausgleichsposition",
                    amount_sign=-1,
                    settlement_dates=True,
                ),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-settlement-positions",
                    alias="kb_settlement_positions",
                    title="Settlement Positions",
                    description="Branch B: applies settlement-position semantics with inverted amounts and settlement document/date mapping.",
                    predecessors=["stage-kb-resolved-positions"],
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-7",
                data_sources=[],
                sql=_build_kostenbelege_pipeline_canonical_output_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-canonical-output",
                    alias="kb_canonical_output",
                    title="Kostenbelege Canonical Output",
                    description="Terminal path: combines original and settlement positions into the final Kostenbelege business projection.",
                    predecessors=[
                        "stage-kb-original-positions",
                        "stage-kb-settlement-positions",
                    ],
                    kind="final",
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-8",
                data_sources=[],
                sql=_build_kostenbelege_pipeline_exception_candidates_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-settlement-exception-candidates",
                    alias="kb_settlement_exception_candidates",
                    title="Settlement Exception Candidates",
                    description="Filters settlement rows for audit-relevant cases such as missing ledger accounts, unusual settlement states, reversals, and high-value settlements.",
                    predecessors=["stage-kb-settlement-positions"],
                ),
            ),
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-cell-9",
                data_sources=[],
                sql=_build_kostenbelege_pipeline_audit_backlog_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-settlement-audit-backlog",
                    alias="kb_settlement_audit_backlog",
                    title="Settlement Audit Backlog",
                    description="Terminal path: groups and ranks settlement exception candidates into an analyst backlog.",
                    predecessors=["stage-kb-settlement-exception-candidates"],
                    kind="final",
                ),
            ),
        ]
    else:
        cells = [
            NotebookCellDefinition(
                cell_id="kostenbelege-3-1-pipeline-loader-status",
                data_sources=[],
                sql=_kostenbelege_pipeline_loader_status_sql(),
                stage=_kostenbelege_pipeline_stage(
                    stage_id="stage-kb-loader-status",
                    alias="kb_loader_status",
                    title="Run Kostenbelege Loader",
                    description="The generated Kostenbelege 3.1 S3 Parquet relations were not discovered yet.",
                    kind="final",
                ),
            )
        ]

    return NotebookDefinition(
        notebook_id=KOSTENBELEGE_3_1_PIPELINE_NOTEBOOK_ID,
        title="Kostenbelege (3.1) S3 Parquet Pipeline",
        summary=(
            "Editable nine-stage PoC pipeline over the generated Kostenbelege 3.1 "
            "S3 Parquet data, with a canonical output path and a settlement audit fork."
        ),
        cells=cells,
        tags=["performance", "kostenbelege", "3.1", "s3", "parquet", "pipeline"],
        tree_path=KOSTENBELEGE_3_1_PIPELINE_TREE_PATH,
        linked_generator_id="kostenbelege_3_1_multi_source_loader",
        pipeline_mode="pipeline",
        can_edit=True,
        can_delete=True,
        shared=True,
        created_at=KOSTENBELEGE_3_1_PIPELINE_CREATED_AT,
    )


def build_kostenbelege_3_1_problem_solving_notebook(
    *,
    kostenbelege_3_1_s3_relations: dict[str, str | None],
) -> NotebookDefinition:
    kbkp_relation = kostenbelege_3_1_s3_relations.get("kbkp_2019") or ""
    kbpo_relation = kostenbelege_3_1_s3_relations.get("kbpo_2019") or ""
    kbhp_relation = kostenbelege_3_1_s3_relations.get("kbhp_2019") or ""
    kalender_relation = kostenbelege_3_1_s3_relations.get("dim_kalender") or ""
    all_s3_sources = _s3_data_sources(
        kbkp_relation,
        kbpo_relation,
        kbhp_relation,
        kalender_relation,
    )
    calendar_sources = _s3_data_sources(kalender_relation)

    if kbkp_relation and kbpo_relation and kbhp_relation and kalender_relation:
        s3_query_options = {
            "duckdb": {"parquetHivePartitioning": "auto"},
            "validation": {"sourceExistence": "off"},
        }
        cells = [
            NotebookCellDefinition(
                cell_id="test-3-1-problem-solving-cell-1",
                data_sources=all_s3_sources,
                query_options=s3_query_options,
                processing_hints=(
                    "Confirm KBKP, KBPO, KBHP, and DIM_KALENDER S3 Parquet views "
                    "or generated read_parquet relations exist before running the "
                    "original query probes."
                ),
                result_expectations=(
                    "One row per source with row counts and min/max technical "
                    "dates, or a surfaced DuckDB relation/read failure."
                ),
                sql=_build_kostenbelege_3_1_problem_readiness_sql(
                    kbkp_relation=kbkp_relation,
                    kbpo_relation=kbpo_relation,
                    kbhp_relation=kbhp_relation,
                    kalender_relation=kalender_relation,
                ),
            ),
            NotebookCellDefinition(
                cell_id="test-3-1-problem-solving-cell-2",
                data_sources=calendar_sources,
                query_options=s3_query_options,
                processing_hints=(
                    "Inspect how many generated calendar dates participate under "
                    "the original date window."
                ),
                result_expectations=(
                    "Calendar row count with the first and last DIM_KALENDER dates "
                    "between DATE '2018-07-01' and CURRENT_DATE."
                ),
                sql=_build_kostenbelege_3_1_problem_calendar_sql(
                    kalender_relation=kalender_relation,
                ),
            ),
            NotebookCellDefinition(
                cell_id="test-3-1-problem-solving-cell-3",
                data_sources=all_s3_sources,
                query_options=s3_query_options,
                processing_hints=(
                    "Inspect DuckDB binding and planning for the original-semantics "
                    "query without returning the full result."
                ),
                result_expectations=(
                    "EXPLAIN output showing scans, joins, union, and projection, "
                    "or the full DuckDB bind/planning error."
                ),
                sql=_build_kostenbelege_3_1_original_explain_sql(
                    kbkp_relation=kbkp_relation,
                    kbpo_relation=kbpo_relation,
                    kbhp_relation=kbhp_relation,
                    kalender_relation=kalender_relation,
                ),
            ),
            NotebookCellDefinition(
                cell_id="test-3-1-problem-solving-cell-4",
                data_sources=all_s3_sources,
                query_options=s3_query_options,
                processing_hints=(
                    "Run the original two-branch CTE shape and count rows by "
                    "PositionsArt before inspecting the full business projection."
                ),
                result_expectations=(
                    "Counts for Originalposition and Ausgleichsposition, or a "
                    "surfaced DuckDB failure if the original query cannot bind or run."
                ),
                sql=_build_kostenbelege_3_1_original_branch_counts_sql(
                    kbkp_relation=kbkp_relation,
                    kbpo_relation=kbpo_relation,
                    kbhp_relation=kbhp_relation,
                    kalender_relation=kalender_relation,
                ),
            ),
            NotebookCellDefinition(
                cell_id="test-3-1-problem-solving-cell-5",
                data_sources=all_s3_sources,
                query_options=s3_query_options,
                processing_hints=(
                    "Run the complete original SQL shape against the generated S3 "
                    "Parquet relations."
                ),
                result_expectations=(
                    "Final business projection rows with UI truncation if large, "
                    "or the full DuckDB error visible in the cell result."
                ),
                sql=_build_kostenbelege_3_1_original_semantics_sql(
                    kbkp_relation=kbkp_relation,
                    kbpo_relation=kbpo_relation,
                    kbhp_relation=kbhp_relation,
                    kalender_relation=kalender_relation,
                ),
            ),
        ]
    else:
        cells = [
            NotebookCellDefinition(
                cell_id="test-3-1-problem-solving-loader-status",
                data_sources=[],
                processing_hints=(
                    "Run the Kostenbelege Multi-Source Loader (3.1) first so this "
                    "notebook can bind generated KBKP, KBPO, KBHP, and DIM_KALENDER "
                    "S3 Parquet relations."
                ),
                result_expectations=(
                    "A status row explaining that the loader output has not been "
                    "discovered yet."
                ),
                sql=_kostenbelege_3_1_problem_loader_status_sql(),
            )
        ]

    return NotebookDefinition(
        notebook_id=KOSTENBELEGE_3_1_PROBLEM_NOTEBOOK_ID,
        title="Test 3.1 - Problem Solving",
        summary=(
            "Editable investigation notebook for the original Kostenbelege 3.1 SQL "
            "semantics against generated S3 Parquet loader output."
        ),
        cells=cells,
        tags=["performance", "kostenbelege", "3.1", "s3", "parquet", "problem-solving"],
        tree_path=KOSTENBELEGE_3_1_PROBLEM_TREE_PATH,
        linked_generator_id="kostenbelege_3_1_multi_source_loader",
        can_edit=True,
        can_delete=True,
        shared=True,
        created_at=KOSTENBELEGE_3_1_PROBLEM_CREATED_AT,
    )


def build_static_notebooks(
    *,
    preferred_s3_relation: str | None,
    preferred_postgres_relation: str | None,
    preferred_postgres_olap_relation: str | None,
    contest_postgres_relation: str | None,
    contest_s3_relation: str | None,
    contest_postgres_native_relation: str | None,
    multi_table_postgres_relations: dict[str, str | None],
    multi_table_s3_relations: dict[str, str | None],
    multi_table_postgres_native_relations: dict[str, str | None],
    mwa_postgres_relations: dict[str, str | None],
    mwa_postgres_native_relations: dict[str, str | None],
    mwa_s3_parquet_relations: dict[str, str | None],
    mwa_s3_csv_relations: dict[str, str | None],
    mwa_s3_json_relations: dict[str, str | None],
    kostenbelege_3_1_oltp_relations: dict[str, str | None],
    kostenbelege_3_1_olap_relations: dict[str, str | None],
    kostenbelege_3_1_oltp_native_relations: dict[str, str | None],
    kostenbelege_3_1_olap_native_relations: dict[str, str | None],
    kostenbelege_3_1_s3_relations: dict[str, str | None],
    union_oltp_relation: str | None,
    union_olap_relation: str | None,
    union_oltp_s3_relation: str | None,
    union_s3_relation: str | None,
    parquet_performance_option_relations: dict[str, str | None],
    result_set_storage_source_relation: str | None,
) -> list[NotebookDefinition]:
    s3_sql = (
        "SELECT\n"
        "  filing_id,\n"
        "  company_uid,\n"
        "  canton_code,\n"
        "  tax_period_end,\n"
        "  declared_turnover_chf,\n"
        "  net_vat_due_chf,\n"
        "  refund_claim_chf,\n"
        "  filing_status\n"
        f"FROM {preferred_s3_relation}\n"
        "WHERE tax_period_end >= DATE '2025-01-01'\n"
        "  AND (net_vat_due_chf > 20000 OR refund_claim_chf > 5000)\n"
        "ORDER BY tax_period_end DESC, net_vat_due_chf DESC, refund_claim_chf DESC\n"
        "LIMIT 100;"
        if preferred_s3_relation
        else "SELECT 'Run the S3 VAT Smoke Loader from the Loader Workbench first.' AS status;"
    )

    postgres_sql = (
        "SELECT\n"
        "  filing_id,\n"
        "  company_uid,\n"
        "  canton_code,\n"
        "  tax_period_end,\n"
        "  output_vat_chf,\n"
        "  input_vat_chf,\n"
        "  net_vat_due_chf,\n"
        "  filing_status,\n"
        "  audit_flag\n"
        f"FROM {preferred_postgres_relation}\n"
        "WHERE tax_period_end >= DATE '2025-01-01'\n"
        "  AND (net_vat_due_chf > 15000 OR audit_flag = true)\n"
        "ORDER BY tax_period_end DESC, net_vat_due_chf DESC, filing_id DESC\n"
        "LIMIT 100;"
        if preferred_postgres_relation
        else "SELECT 'Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.' AS status;"
    )
    postgres_olap_sql = (
        "SELECT\n"
        "  assessment_id,\n"
        "  taxpayer_uid,\n"
        "  canton_code,\n"
        "  tax_type,\n"
        "  assessment_status,\n"
        "  payment_status,\n"
        "  assessed_tax_chf,\n"
        "  collected_tax_chf,\n"
        "  open_balance_chf,\n"
        "  audit_risk_score,\n"
        "  audit_flag\n"
        f"FROM {preferred_postgres_olap_relation}\n"
        "WHERE tax_period_end >= DATE '2025-01-01'\n"
        "  AND (open_balance_chf > 25000 OR audit_risk_score >= 80 OR audit_flag = true)\n"
        "ORDER BY open_balance_chf DESC, audit_risk_score DESC, assessment_id DESC\n"
        "LIMIT 100;"
        if preferred_postgres_olap_relation
        else "SELECT 'Run the PostgreSQL OLAP Tax Assessment Loader from the Loader Workbench first.' AS status;"
    )
    pandas_preview_sql = (
        "SELECT\n"
        "  filing_id,\n"
        "  company_uid,\n"
        "  canton_code,\n"
        "  tax_period_end,\n"
        "  output_vat_chf,\n"
        "  input_vat_chf,\n"
        "  net_vat_due_chf,\n"
        "  filing_status,\n"
        "  audit_flag\n"
        f"FROM {preferred_postgres_relation}\n"
        "WHERE tax_period_end >= DATE '2025-01-01'\n"
        "ORDER BY tax_period_end DESC, net_vat_due_chf DESC, filing_id DESC\n"
        "LIMIT 25;"
        if preferred_postgres_relation
        else "SELECT 'Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.' AS status;"
    )
    pandas_python_load_code = (
        "# Load the static VAT smoke table into pandas via the explicit source helper.\n"
        f"vat_df = source({preferred_postgres_relation!r}).df()\n"
        'vat_df["tax_period_end"] = pd.to_datetime(vat_df["tax_period_end"])\n'
        'vat_df["net_gap_chf"] = vat_df["output_vat_chf"] - vat_df["input_vat_chf"]\n'
        "vat_df = (\n"
        '    vat_df.loc[vat_df["tax_period_end"] >= "2025-01-01"]\n'
        "    .sort_values([\"tax_period_end\", \"net_vat_due_chf\"], ascending=[False, False])\n"
        "    .reset_index(drop=True)\n"
        ")\n"
        "vat_df.head(12)\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
    )
    pandas_python_wrangle_code = (
        "# Reuse the DataFrame from the previous cell and wrangle it with pandas.\n"
        "canton_summary = (\n"
        "    vat_df.assign(\n"
        '        quarter=vat_df["tax_period_end"].dt.to_period("Q").astype(str),\n'
        '        needs_attention=vat_df["audit_flag"] | vat_df["net_vat_due_chf"].gt(15000),\n'
        "    )\n"
        "    .groupby([\"quarter\", \"canton_code\", \"filing_status\"], as_index=False)\n"
        "    .agg(\n"
        '        filing_count=("filing_id", "count"),\n'
        '        net_vat_total_chf=("net_vat_due_chf", "sum"),\n'
        '        average_gap_chf=("net_gap_chf", "mean"),\n'
        '        attention_cases=("needs_attention", "sum"),\n'
        "    )\n"
        "    .sort_values([\"net_vat_total_chf\", \"filing_count\"], ascending=[False, False])\n"
        "    .reset_index(drop=True)\n"
        ")\n"
        "canton_summary.head(15)\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
    )
    chart_python_load_code = (
        "# Cell 1: aggregate and retain the five strongest synthetic VAT cantons.\n"
        "# Provisorische ständige Wohnbevölkerung, BFS STATPOP, Stand 31.12.2025.\n"
        "CANTON_POPULATION_2025 = {\n"
        '    "ZH": 1_632_191,\n'
        '    "VD": 863_375,\n'
        '    "AG": 743_563,\n'
        '    "LU": 441_400,\n'
        '    "FR": 350_807,\n'
        "}\n\n"
        'quarterly_all = sql("""\n'
        "SELECT\n"
        "  canton_code,\n"
        "  CAST(date_trunc('quarter', tax_period_end) AS DATE) AS tax_quarter_start,\n"
        "  CAST(SUM(net_vat_due_chf) AS DOUBLE) AS mwst_umsatz_chf,\n"
        "  COUNT(*) AS filing_count\n"
        f"FROM {preferred_postgres_relation}\n"
        "WHERE tax_period_end >= DATE '2025-01-01'\n"
        "GROUP BY canton_code, tax_quarter_start\n"
        "ORDER BY tax_quarter_start, mwst_umsatz_chf DESC\n"
        '""")\n'
        'quarterly_all["tax_quarter_start"] = pd.to_datetime(quarterly_all["tax_quarter_start"])\n'
        'quarterly_all["mwst_umsatz_chf"] = pd.to_numeric(quarterly_all["mwst_umsatz_chf"])\n\n'
        "canton_ranking = (\n"
        '    quarterly_all.groupby("canton_code", as_index=False)\n'
        "    .agg(\n"
        '        mwst_umsatz_chf=("mwst_umsatz_chf", "sum"),\n'
        '        average_quarter_chf=("mwst_umsatz_chf", "mean"),\n'
        '        filing_count=("filing_count", "sum"),\n'
        "    )\n"
        '    .sort_values(["mwst_umsatz_chf", "canton_code"], ascending=[False, True])\n'
        "    .reset_index(drop=True)\n"
        ")\n"
        "top_five_vat = canton_ranking.head(5).copy()\n"
        'top_five_vat.insert(0, "rank", range(1, len(top_five_vat) + 1))\n'
        'top_five_vat["population"] = top_five_vat["canton_code"].map(CANTON_POPULATION_2025)\n'
        'top_five_vat["population_mio"] = top_five_vat["population"] / 1_000_000\n'
        'top_five_vat["mwst_umsatz_mio_chf"] = top_five_vat["mwst_umsatz_chf"] / 1_000_000\n'
        'top_five_vat["average_quarter_mio_chf"] = top_five_vat["average_quarter_chf"] / 1_000_000\n'
        'top_cantons = top_five_vat["canton_code"].tolist()\n'
        'expected_top_cantons = ["ZH", "VD", "AG", "LU", "FR"]\n'
        "if top_cantons != expected_top_cantons:\n"
        '    raise RuntimeError(f"Expected {expected_top_cantons}, got {top_cantons}. Re-run the PostgreSQL OLTP VAT Smoke Loader.")\n\n'
        'quarterly_vat = quarterly_all[quarterly_all["canton_code"].isin(top_cantons)].copy()\n'
        'quarterly_vat["mwst_umsatz_mio_chf"] = quarterly_vat["mwst_umsatz_chf"] / 1_000_000\n\n'
        "top_five_display = top_five_vat.rename(columns={\n"
        '    "rank": "Rang",\n'
        '    "canton_code": "Kanton",\n'
        '    "mwst_umsatz_mio_chf": "MWST-Umsatz (Mio. CHF)",\n'
        '    "average_quarter_mio_chf": "Ø je Quartal (Mio. CHF)",\n'
        '    "population_mio": "Bevölkerung (Mio.)",\n'
        "})[[\n"
        '    "Rang", "Kanton", "MWST-Umsatz (Mio. CHF)",\n'
        '    "Ø je Quartal (Mio. CHF)", "Bevölkerung (Mio.)",\n'
        "]]\n"
        "top_five_display.round(2)\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
    )
    chart_python_plot_code = (
        "# Cell 2: compare the quarterly VAT turnover of the Top 5.\n"
        "import matplotlib.pyplot as plt\n\n"
        'plt.close("all")\n'
        "chart_ready = (\n"
        '    quarterly_vat.assign(quarter_label=quarterly_vat["tax_quarter_start"].dt.to_period("Q").astype(str))\n'
        '    .pivot(index="quarter_label", columns="canton_code", values="mwst_umsatz_mio_chf")\n'
        "    .fillna(0)\n"
        "    .sort_index()\n"
        "    .reindex(columns=top_cantons)\n"
        ")\n\n"
        'colors = ["#2166ac", "#b2182b", "#4d9221", "#762a83", "#e08214"]\n'
        'hatches = ["", "//", "xx", "..", "\\\\"]\n'
        'fig, ax = plt.subplots(figsize=(12, 6.2))\n'
        'chart_ready.plot(kind="bar", ax=ax, width=0.82, rot=0, color=colors, edgecolor="#25364a", linewidth=0.7)\n'
        "for container, hatch in zip(ax.containers, hatches):\n"
        "    for bar in container:\n"
        "        bar.set_hatch(hatch)\n"
        'ax.set_title("MWST-Umsatz der fünf umsatzstärksten Kantone nach Quartal", loc="left", fontweight="bold")\n'
        'ax.set_xlabel("Quartal")\n'
        'ax.set_ylabel("MWST-Umsatz (in Mio. CHF)")\n'
        "ax.set_ylim(bottom=0)\n"
        'ax.grid(axis="y", alpha=0.25)\n'
        'ax.legend(title="Kanton", ncols=5, loc="upper center", bbox_to_anchor=(0.5, 1.0))\n'
        'fig.text(0.01, 0.01, "Synthetische PoC-Daten · Netto-MWST-Schuld · Zeitraum ab 2025", fontsize=9, color="#52657a")\n'
        "fig.tight_layout(rect=(0, 0.05, 1, 1))\n"
        "plt.show()\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
    )
    chart_python_scatter_code = (
        "# Cell 3: compare average quarterly VAT turnover with population.\n"
        "import matplotlib.pyplot as plt\n\n"
        'plt.close("all")\n'
        'scatter_ready = top_five_vat.sort_values("rank").copy()\n'
        'colors = ["#2166ac", "#b2182b", "#4d9221", "#762a83", "#e08214"]\n'
        'markers = ["o", "s", "^", "D", "P"]\n'
        'fig, ax = plt.subplots(figsize=(11.5, 6.2))\n'
        "for (_, row), color, marker in zip(scatter_ready.iterrows(), colors, markers):\n"
        "    ax.scatter(\n"
        '        row["population_mio"], row["average_quarter_mio_chf"],\n'
        '        s=180, color=color, marker=marker, edgecolor="#25364a", linewidth=0.9, zorder=3,\n'
        "    )\n"
        "    ax.annotate(\n"
        '        f"{row[\'canton_code\']}  #{int(row[\'rank\'])}",\n'
        '        (row["population_mio"], row["average_quarter_mio_chf"]),\n'
        '        xytext=(8, 8), textcoords="offset points", fontweight="bold",\n'
        "    )\n"
        'ax.set_title("MWST-Umsatz vs. Bevölkerung der Top-5-Kantone", loc="left", fontweight="bold")\n'
        'ax.set_xlabel("Bevölkerung (in Mio.)")\n'
        'ax.set_ylabel("MWST-Umsatz (in Mio. CHF)")\n'
        "ax.set_xlim(left=0)\n"
        "ax.set_ylim(bottom=0)\n"
        'ax.grid(alpha=0.25)\n'
        'fig.text(0.01, 0.01, "MWST: synthetische PoC-Daten · Bevölkerung: BFS STATPOP, provisorischer Stand 31.12.2025", fontsize=9, color="#52657a")\n'
        "fig.tight_layout(rect=(0, 0.05, 1, 1))\n"
        "plt.show()\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
    )
    performance_sql_template = (
        "-- Approximation: summarize quarterly tax-assessment pressure across cantons and sectors.\n"
        "-- Logic: filter recent assessments, aggregate assessed, collected, and open balances, and keep only large exposure groups.\n"
        "-- Result: rank the tax segments with the highest open balance and audit pressure.\n"
        "WITH scoped_assessments AS (\n"
        "  SELECT\n"
        "    canton_code,\n"
        "    tax_type,\n"
        "    industry_sector,\n"
        "    assessment_status,\n"
        "    payment_status,\n"
        "    CAST(date_trunc('quarter', tax_period_end) AS DATE) AS tax_quarter_start,\n"
        "    assessed_tax_chf,\n"
        "    collected_tax_chf,\n"
        "    open_balance_chf,\n"
        "    taxable_base_chf,\n"
        "    declared_deduction_chf,\n"
        "    audit_risk_score,\n"
        "    CASE\n"
        "      WHEN payment_status IN ('overdue', 'enforcement') THEN 1\n"
        "      ELSE 0\n"
        "    END AS enforcement_risk_flag\n"
        "  FROM {relation}\n"
        "  WHERE tax_period_end >= DATE '2024-01-01'\n"
        "    AND tax_type IN ('VAT', 'COMPANY_TAX', 'ALCOHOL_TAX', 'INCOME_TAX')\n"
        "    AND assessment_status IN ('under_review', 'assessed', 'appealed', 'enforced')\n"
        "),\n"
        "quarterly_pressure AS (\n"
        "  SELECT\n"
        "    canton_code,\n"
        "    tax_type,\n"
        "    industry_sector,\n"
        "    tax_quarter_start,\n"
        "    COUNT(*) AS assessment_count,\n"
        "    CAST(ROUND(SUM(assessed_tax_chf), 2) AS DECIMAL(18,2)) AS assessed_tax_total_chf,\n"
        "    CAST(ROUND(SUM(collected_tax_chf), 2) AS DECIMAL(18,2)) AS collected_tax_total_chf,\n"
        "    CAST(ROUND(SUM(open_balance_chf), 2) AS DECIMAL(18,2)) AS open_balance_total_chf,\n"
        "    CAST(CAST(AVG(taxable_base_chf - declared_deduction_chf) AS DECIMAL(18,2)) AS DOUBLE PRECISION) AS avg_net_tax_base_chf,\n"
        "    CAST(CAST(AVG(audit_risk_score) AS DECIMAL(18,2)) AS DOUBLE PRECISION) AS avg_audit_risk_score,\n"
        "    SUM(enforcement_risk_flag) AS enforcement_risk_count\n"
        "  FROM scoped_assessments\n"
        "  GROUP BY canton_code, tax_type, industry_sector, tax_quarter_start\n"
        ")\n"
        "SELECT\n"
        "  canton_code,\n"
        "  tax_type,\n"
        "  industry_sector,\n"
        "  tax_quarter_start,\n"
        "  assessment_count,\n"
        "  assessed_tax_total_chf,\n"
        "  collected_tax_total_chf,\n"
        "  open_balance_total_chf,\n"
        "  avg_net_tax_base_chf,\n"
        "  avg_audit_risk_score,\n"
        "  enforcement_risk_count\n"
        "FROM quarterly_pressure\n"
        "WHERE assessed_tax_total_chf >= 750000\n"
        "ORDER BY open_balance_total_chf DESC, avg_audit_risk_score DESC, assessment_count DESC\n"
        "LIMIT 30;"
    )
    contest_postgres_sql = (
        performance_sql_template.format(relation=contest_postgres_relation)
        if contest_postgres_relation
        else "SELECT 'Run the PG vs S3 Contest Loader from the Loader Workbench first.' AS status;"
    )
    contest_s3_sql = (
        performance_sql_template.format(relation=contest_s3_relation)
        if contest_s3_relation
        else "SELECT 'Run the PG vs S3 Contest Loader from the Loader Workbench first.' AS status;"
    )
    contest_postgres_native_sql = (
        performance_sql_template.format(relation=contest_postgres_native_relation)
        if contest_postgres_native_relation
        else "SELECT 'Run the PG vs S3 Contest Loader from the Loader Workbench first.' AS status;"
    )
    multi_table_status_sql = (
        "SELECT 'Run the PG vs S3 Multi-Table Federal Tax Loader from the Loader Workbench first.' AS status;"
    )
    multi_table_postgres_sql = (
        _build_multi_table_performance_sql(
            taxpayers_relation=multi_table_postgres_relations["federal_tax_taxpayers_mt"],
            filings_relation=multi_table_postgres_relations["federal_tax_filings_mt"],
            assessments_relation=multi_table_postgres_relations["federal_tax_assessments_mt"],
            payments_relation=multi_table_postgres_relations["federal_tax_payments_mt"],
            audits_relation=multi_table_postgres_relations["federal_tax_audits_mt"],
            enforcements_relation=multi_table_postgres_relations["federal_tax_enforcements_mt"],
            appeals_relation=multi_table_postgres_relations["federal_tax_appeals_mt"],
        )
        if all(multi_table_postgres_relations.values())
        else multi_table_status_sql
    )
    multi_table_s3_sql = (
        _build_multi_table_performance_sql(
            taxpayers_relation=multi_table_s3_relations["federal_tax_taxpayers_mt"],
            filings_relation=multi_table_s3_relations["federal_tax_filings_mt"],
            assessments_relation=multi_table_s3_relations["federal_tax_assessments_mt"],
            payments_relation=multi_table_s3_relations["federal_tax_payments_mt"],
            audits_relation=multi_table_s3_relations["federal_tax_audits_mt"],
            enforcements_relation=multi_table_s3_relations["federal_tax_enforcements_mt"],
            appeals_relation=multi_table_s3_relations["federal_tax_appeals_mt"],
        )
        if all(multi_table_s3_relations.values())
        else multi_table_status_sql
    )
    multi_table_postgres_native_sql = (
        _build_multi_table_performance_sql(
            taxpayers_relation=multi_table_postgres_native_relations["federal_tax_taxpayers_mt"],
            filings_relation=multi_table_postgres_native_relations["federal_tax_filings_mt"],
            assessments_relation=multi_table_postgres_native_relations["federal_tax_assessments_mt"],
            payments_relation=multi_table_postgres_native_relations["federal_tax_payments_mt"],
            audits_relation=multi_table_postgres_native_relations["federal_tax_audits_mt"],
            enforcements_relation=multi_table_postgres_native_relations["federal_tax_enforcements_mt"],
            appeals_relation=multi_table_postgres_native_relations["federal_tax_appeals_mt"],
        )
        if all(multi_table_postgres_native_relations.values())
        else multi_table_status_sql
    )
    mwa_status_sql = (
        "SELECT 'Run the MWA Abrechnung Multi-Format Loader (3.2) from the Loader Workbench first.' AS status;"
    )
    mwa_postgres_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_postgres_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_postgres_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_postgres_relations.values())
        else mwa_status_sql
    )
    mwa_postgres_native_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_postgres_native_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_postgres_native_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_postgres_native_relations.values())
        else mwa_status_sql
    )
    mwa_s3_parquet_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_s3_parquet_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_s3_parquet_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_s3_parquet_relations.values())
        else mwa_status_sql
    )
    mwa_s3_parquet_art_index_sql = (
        _build_mwa_art_index_demo_sql(
            abrechnung_relation=mwa_s3_parquet_relations["mwa_abrechnung_entities"],
        )
        if mwa_s3_parquet_relations["mwa_abrechnung_entities"]
        else mwa_status_sql
    )
    mwa_s3_csv_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_s3_csv_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_s3_csv_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_s3_csv_relations.values())
        else mwa_status_sql
    )
    mwa_s3_json_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_s3_json_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_s3_json_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_s3_json_relations.values())
        else mwa_status_sql
    )
    kostenbelege_status_sql = (
        "SELECT 'Run the Kostenbelege Multi-Source Loader (3.1) from the Loader Workbench first.' AS status;"
    )
    kostenbelege_3_1_oltp_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_oltp_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_oltp_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_oltp_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_oltp_relations["dim_kalender"],
        )
        if all(kostenbelege_3_1_oltp_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_olap_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_olap_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_olap_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_olap_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_olap_relations["dim_kalender"],
        )
        if all(kostenbelege_3_1_olap_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_oltp_native_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_oltp_native_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_oltp_native_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_oltp_native_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_oltp_native_relations["dim_kalender"],
            quote_source_columns=True,
        )
        if all(kostenbelege_3_1_oltp_native_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_olap_native_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_olap_native_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_olap_native_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_olap_native_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_olap_native_relations["dim_kalender"],
            quote_source_columns=True,
        )
        if all(kostenbelege_3_1_olap_native_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_s3_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_s3_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_s3_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_s3_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_s3_relations["dim_kalender"],
        )
        if all(kostenbelege_3_1_s3_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_s3_optimized_sql = (
        _build_kostenbelege_3_1_optimized_sql(
            kbkp_relation=kostenbelege_3_1_s3_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_s3_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_s3_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_s3_relations["dim_kalender"],
        )
        if all(kostenbelege_3_1_s3_relations.values())
        else kostenbelege_status_sql
    )
    cross_database_union_sql = (
        "-- Approximation: compare how the same tax-position reference shape behaves across OLTP and OLAP.\n"
        "-- Logic: UNION identical columns from PostgreSQL OLTP and PostgreSQL OLAP, then roll them up by source and risk slice.\n"
        "-- Result: highlight which database contributes the largest net tax totals per canton, status, and risk band.\n"
        "WITH combined_tax_positions AS (\n"
        "  SELECT\n"
        "    'OLTP' AS database_name,\n"
        "    record_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    tax_period_end,\n"
        "    net_tax_amount_chf,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    updated_at\n"
        f"  FROM {union_oltp_relation}\n"
        "  UNION\n"
        "  SELECT\n"
        "    'OLAP' AS database_name,\n"
        "    record_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    tax_period_end,\n"
        "    net_tax_amount_chf,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    updated_at\n"
        f"  FROM {union_olap_relation}\n"
        "),\n"
        "aggregated_positions AS (\n"
        "  SELECT\n"
        "    database_name,\n"
        "    canton_code,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    COUNT(*) AS record_count,\n"
        "    CAST(ROUND(SUM(net_tax_amount_chf), 2) AS DECIMAL(18,2)) AS net_tax_amount_total_chf,\n"
        "    MIN(tax_period_end) AS earliest_tax_period_end,\n"
        "    MAX(tax_period_end) AS latest_tax_period_end\n"
        "  FROM combined_tax_positions\n"
        "  GROUP BY database_name, canton_code, processing_status, risk_band\n"
        "),\n"
        "ranked_positions AS (\n"
        "  SELECT\n"
        "    database_name,\n"
        "    canton_code,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    record_count,\n"
        "    net_tax_amount_total_chf,\n"
        "    earliest_tax_period_end,\n"
        "    latest_tax_period_end,\n"
        "    ROW_NUMBER() OVER (\n"
        "      PARTITION BY database_name\n"
        "      ORDER BY net_tax_amount_total_chf DESC, canton_code, processing_status, risk_band\n"
        "    ) AS source_rank\n"
        "  FROM aggregated_positions\n"
        ")\n"
        "SELECT\n"
        "  database_name,\n"
        "  canton_code,\n"
        "  processing_status,\n"
        "  risk_band,\n"
        "  record_count,\n"
        "  net_tax_amount_total_chf,\n"
        "  earliest_tax_period_end,\n"
        "  latest_tax_period_end\n"
        "FROM ranked_positions\n"
        "WHERE source_rank <= 30\n"
        "ORDER BY database_name, source_rank, canton_code, processing_status, risk_band;"
        if union_oltp_relation and union_olap_relation
        else "SELECT 'Run the PostgreSQL OLTP + OLAP UNION Loader from the Loader Workbench first.' AS status;"
    )
    cross_source_union_sql = (
        "-- Approximation: compare the same tax-position reference shape across OLTP and S3-backed object storage.\n"
        "-- Logic: UNION matching OLTP rows with the mirrored S3 dataset through DuckDB, then roll them up by source and risk slice.\n"
        "-- Result: show which source contributes the largest net tax totals per canton, status, and risk band.\n"
        "WITH combined_tax_positions AS (\n"
        "  SELECT\n"
        "    'OLTP' AS source_name,\n"
        "    record_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    tax_period_end,\n"
        "    net_tax_amount_chf,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    updated_at\n"
        f"  FROM {union_oltp_s3_relation}\n"
        "  UNION\n"
        "  SELECT\n"
        "    'S3' AS source_name,\n"
        "    record_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    tax_period_end,\n"
        "    net_tax_amount_chf,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    updated_at\n"
        f"  FROM {union_s3_relation}\n"
        "),\n"
        "aggregated_positions AS (\n"
        "  SELECT\n"
        "    source_name,\n"
        "    canton_code,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    COUNT(*) AS record_count,\n"
        "    CAST(ROUND(SUM(net_tax_amount_chf), 2) AS DECIMAL(18,2)) AS net_tax_amount_total_chf,\n"
        "    MIN(tax_period_end) AS earliest_tax_period_end,\n"
        "    MAX(tax_period_end) AS latest_tax_period_end,\n"
        "    MIN(updated_at) AS first_update_at,\n"
        "    MAX(updated_at) AS last_update_at\n"
        "  FROM combined_tax_positions\n"
        "  GROUP BY source_name, canton_code, processing_status, risk_band\n"
        "),\n"
        "ranked_positions AS (\n"
        "  SELECT\n"
        "    source_name,\n"
        "    canton_code,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    record_count,\n"
        "    net_tax_amount_total_chf,\n"
        "    earliest_tax_period_end,\n"
        "    latest_tax_period_end,\n"
        "    first_update_at,\n"
        "    last_update_at,\n"
        "    ROW_NUMBER() OVER (\n"
        "      PARTITION BY source_name\n"
        "      ORDER BY net_tax_amount_total_chf DESC, canton_code, processing_status, risk_band\n"
        "    ) AS source_rank\n"
        "  FROM aggregated_positions\n"
        ")\n"
        "SELECT\n"
        "  source_name,\n"
        "  canton_code,\n"
        "  processing_status,\n"
        "  risk_band,\n"
        "  record_count,\n"
        "  net_tax_amount_total_chf,\n"
        "  earliest_tax_period_end,\n"
        "  latest_tax_period_end,\n"
        "  first_update_at,\n"
        "  last_update_at\n"
        "FROM ranked_positions\n"
        "WHERE source_rank <= 30\n"
        "ORDER BY source_name, source_rank, canton_code, processing_status, risk_band;"
        if union_oltp_s3_relation and union_s3_relation
        else "SELECT 'Run the PostgreSQL OLTP + S3 UNION Loader from the Loader Workbench first.' AS status;"
    )
    parquet_performance_status_sql = (
        "SELECT 'Run the matching Federal Tax Parquet Optimization loader from the Loader Workbench first.' AS status;"
    )

    def federal_tax_parquet_performance_sql(relation: str | None, layout_label: str) -> str:
        if not relation:
            return parquet_performance_status_sql
        return (
            f"-- Federal tax Parquet optimization layout: {layout_label}.\n"
            "-- Compare filtered and aggregate reads across the same generated federal tax data.\n"
            "WITH scoped_tax AS (\n"
            "  SELECT\n"
            "    taxpayer_id,\n"
            "    tax_year,\n"
            "    canton,\n"
            "    income_chf,\n"
            "    deductions_chf,\n"
            "    taxable_income_chf,\n"
            "    tax_rate_percent,\n"
            "    tax_due_chf,\n"
            "    payment_status,\n"
            "    filing_date\n"
            f"  FROM {relation}\n"
            "  WHERE tax_year = 2025\n"
            ")\n"
            "SELECT\n"
            "  tax_year,\n"
            "  canton,\n"
            "  COUNT(*) AS filing_count,\n"
            "  CAST(ROUND(SUM(income_chf), 2) AS DECIMAL(18,2)) AS income_total_chf,\n"
            "  CAST(ROUND(SUM(tax_due_chf), 2) AS DECIMAL(18,2)) AS tax_due_total_chf,\n"
            "  CAST(ROUND(AVG(tax_rate_percent), 2) AS DECIMAL(8,2)) AS avg_tax_rate_percent,\n"
            "  SUM(CASE WHEN payment_status = 'overdue' THEN 1 ELSE 0 END) AS overdue_count,\n"
            "  MIN(filing_date) AS first_filing_date,\n"
            "  MAX(filing_date) AS last_filing_date\n"
            "FROM scoped_tax\n"
            "GROUP BY tax_year, canton\n"
            "ORDER BY tax_due_total_chf DESC, filing_count DESC, canton\n"
            "LIMIT 25;"
        )

    def federal_tax_parquet_duckdb_cache_relation(relation: str | None) -> str | None:
        normalized_relation = str(relation or "").strip()
        if not normalized_relation or "." not in normalized_relation:
            return None
        schema_name, table_name = normalized_relation.rsplit(".", 1)
        return f"{schema_name}.{table_name}_duckdb_cache"

    def federal_tax_parquet_cache_lookup_sql(relation: str | None) -> str:
        if not relation:
            return parquet_performance_status_sql
        return (
            "-- Loader-created DuckDB ART cache lookup.\n"
            "-- The cache-only loader materializes the generated S3 Parquet data into this\n"
            "-- local DuckDB table and creates an ART index on taxpayer_id in the background.\n"
            "-- This lookup measures that cached table directly instead of scanning S3 Parquet.\n"
            "SELECT\n"
            "  taxpayer_id,\n"
            "  tax_year,\n"
            "  canton,\n"
            "  income_chf,\n"
            "  taxable_income_chf,\n"
            "  tax_due_chf,\n"
            "  payment_status,\n"
            "  filing_date\n"
            f"FROM {relation}\n"
            "WHERE taxpayer_id = 'TX-100001';"
        )

    oltp_write_test_table = "public.notebook_oltp_write_test"
    oltp_write_test_setup_sql = (
        "-- Reset the OLTP write-test table so the notebook can be rerun safely.\n"
        f"DROP TABLE IF EXISTS {oltp_write_test_table};\n"
        f"CREATE TABLE {oltp_write_test_table} (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  taxpayer_uid TEXT NOT NULL,\n"
        "  canton_code TEXT NOT NULL,\n"
        "  declared_turnover_chf NUMERIC(12,2) NOT NULL,\n"
        "  note TEXT NOT NULL,\n"
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
        ");"
    )
    oltp_write_test_insert_sql = (
        f"INSERT INTO {oltp_write_test_table} (\n"
        "  id,\n"
        "  taxpayer_uid,\n"
        "  canton_code,\n"
        "  declared_turnover_chf,\n"
        "  note\n"
        ")\n"
        "SELECT\n"
        "  series_id,\n"
        "  'UID-' || LPAD(series_id::text, 5, '0') AS taxpayer_uid,\n"
        "  CASE MOD(series_id, 5)\n"
        "    WHEN 0 THEN 'ZH'\n"
        "    WHEN 1 THEN 'BE'\n"
        "    WHEN 2 THEN 'GE'\n"
        "    WHEN 3 THEN 'VD'\n"
        "    ELSE 'TI'\n"
        "  END AS canton_code,\n"
        "  ROUND((1500 + series_id * 87.5)::numeric, 2) AS declared_turnover_chf,\n"
        "  'OLTP write test row ' || series_id::text AS note\n"
        "FROM generate_series(1, 20) AS series(series_id);"
    )
    oltp_write_test_verify_summary_sql = (
        "SELECT\n"
        "  COUNT(*) AS inserted_rows,\n"
        "  MIN(id) AS min_id,\n"
        "  MAX(id) AS max_id,\n"
        "  CAST(ROUND(SUM(declared_turnover_chf), 2) AS NUMERIC(12,2)) AS turnover_total_chf\n"
        f"FROM {oltp_write_test_table};"
    )
    oltp_write_test_verify_rows_sql = (
        "SELECT\n"
        "  id,\n"
        "  taxpayer_uid,\n"
        "  canton_code,\n"
        "  declared_turnover_chf,\n"
        "  note,\n"
        "  created_at\n"
        f"FROM {oltp_write_test_table}\n"
        "ORDER BY id;"
    )
    oltp_write_test_cleanup_sql = (
        "-- Optional cleanup when you are done validating OLTP write access.\n"
        f"DROP TABLE IF EXISTS {oltp_write_test_table};"
    )
    preferred_s3_sources = _s3_data_sources(preferred_s3_relation)
    cross_source_s3_sources = _s3_data_sources(union_s3_relation)
    parquet_off_sources = _s3_data_sources(
        parquet_performance_option_relations.get("federal_tax_parquet_off")
    )
    parquet_recommended_sources = _s3_data_sources(
        parquet_performance_option_relations.get("federal_tax_parquet_recommended")
    )
    parquet_manual_partition_sources = _s3_data_sources(
        parquet_performance_option_relations.get("federal_tax_parquet_manual_partition")
    )
    parquet_manual_hive_sources = _s3_data_sources(
        parquet_performance_option_relations.get("federal_tax_parquet_manual_hive")
    )
    parquet_manual_cache_sources = _s3_data_sources(
        parquet_performance_option_relations.get("federal_tax_parquet_manual_cache")
    )
    contest_s3_sources = _s3_data_sources(contest_s3_relation)
    multi_table_s3_sources = _s3_data_sources(*multi_table_s3_relations.values())
    kostenbelege_3_1_s3_sources = _s3_data_sources(*kostenbelege_3_1_s3_relations.values())
    kostenbelege_3_1_optimized_dataset_sources = ["s3"]
    mwa_s3_parquet_sources = _s3_data_sources(*mwa_s3_parquet_relations.values())
    mwa_s3_parquet_abrechnung_sources = _s3_data_sources(
        mwa_s3_parquet_relations.get("mwa_abrechnung_entities")
    )
    mwa_s3_csv_sources = _s3_data_sources(*mwa_s3_csv_relations.values())
    mwa_s3_json_sources = _s3_data_sources(*mwa_s3_json_relations.values())

    return [
        build_data_analysts_journey_notebook(),
        NotebookDefinition(
            notebook_id="s3-smoke-test",
            title="S3 Smoke Test",
            summary="Reviews VAT filing smoke data from S3 through DuckDB for Federal Tax Administration analysis.",
            cells=[
                NotebookCellDefinition(
                    cell_id="s3-smoke-test-cell-1",
                    data_sources=preferred_s3_sources,
                    sql=s3_sql,
                )
            ],
            tags=["smoke", "s3"],
            tree_path=("PoC Tests", "Smoke Tests", "Object Storage"),
            linked_generator_id="s3_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="postgres-smoke-test",
            title="PostgreSQL Smoke Test",
            summary="Queries VAT filing reference data in PostgreSQL OLTP for Federal Tax Administration smoke testing.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-smoke-test-cell-1",
                    data_sources=["pg_oltp"],
                    sql=postgres_sql,
                )
            ],
            tags=["smoke", "postgres"],
            tree_path=("PoC Tests", "Smoke Tests", "Relational"),
            linked_generator_id="postgres_oltp_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="postgres-olap-smoke-test",
            title="PostgreSQL OLAP Tax Assessment Smoke Test",
            summary="Queries generated tax assessment smoke data in PostgreSQL OLAP for Federal Tax Administration analysis.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-olap-smoke-test-cell-1",
                    data_sources=["pg_olap"],
                    sql=postgres_olap_sql,
                )
            ],
            tags=["smoke", "postgres", "olap", "tax", "assessment"],
            tree_path=("PoC Tests", "Smoke Tests", "Relational"),
            linked_generator_id="postgres_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="python-pandas-vat-demo",
            title="Python Pandas VAT Demo",
            summary="Shows how an immutable notebook can combine SQL with Python and pandas on the static PostgreSQL VAT smoke reference table, including persistent state across Python cells.",
            cells=[
                NotebookCellDefinition(
                    cell_id="python-pandas-vat-demo-cell-1",
                    data_sources=["pg_oltp"],
                    sql=pandas_preview_sql,
                ),
                NotebookCellDefinition(
                    cell_id="python-pandas-vat-demo-cell-2",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=pandas_python_load_code,
                ),
                NotebookCellDefinition(
                    cell_id="python-pandas-vat-demo-cell-3",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=pandas_python_wrangle_code,
                ),
            ],
            tags=["python", "pandas", "demo", "postgres"],
            tree_path=JUPYTER_PYTHON_TREE_PATH,
            linked_generator_id="postgres_oltp_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="python-chart-vat-demo",
            title="Python Chart VAT Demo",
            summary="Filters the five highest-turnover VAT cantons and visualizes their quarterly totals and population relationship with pandas and matplotlib in a headless Jupyter kernel.",
            cells=[
                NotebookCellDefinition(
                    cell_id="python-chart-vat-demo-cell-1",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=chart_python_load_code,
                ),
                NotebookCellDefinition(
                    cell_id="python-chart-vat-demo-cell-2",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=chart_python_plot_code,
                ),
                NotebookCellDefinition(
                    cell_id="python-chart-vat-demo-cell-3",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=chart_python_scatter_code,
                ),
            ],
            tags=["python", "pandas", "chart", "scatter", "matplotlib", "demo", "postgres"],
            tree_path=JUPYTER_PYTHON_TREE_PATH,
            linked_generator_id="postgres_oltp_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        build_result_set_storage_s3_demo_notebook(result_set_storage_source_relation),
        build_kostenbelege_fact_builder_s3_demo_notebook(),
        build_kostenbelege_fact_builder_s3_pipeline_notebook(),
        NotebookDefinition(
            notebook_id="postgres-oltp-write-test",
            title="PostgreSQL OLTP Write Test",
            summary="Creates a PostgreSQL OLTP test table, inserts 20 rows with pure SQL, verifies the inserted data, and includes an optional cleanup cell.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_setup_sql,
                ),
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-2",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_insert_sql,
                ),
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-3",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_verify_summary_sql,
                ),
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-4",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_verify_rows_sql,
                ),
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-5",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_cleanup_sql,
                ),
            ],
            tags=["smoke", "write-test", "postgres", "oltp"],
            tree_path=("PoC Tests", "Smoke Tests", "Write Access"),
            linked_generator_id="postgres_oltp_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="postgres-oltp-olap-union-test",
            title="PostgreSQL OLTP + OLAP UNION",
            summary="Executes a UNION across PostgreSQL OLTP and PostgreSQL OLAP using the same reference structure in both databases.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-oltp-olap-union-test-cell-1",
                    data_sources=["pg_oltp", "pg_olap"],
                    sql=cross_database_union_sql,
                )
            ],
            tags=["sql", "union", "postgres", "oltp", "olap"],
            tree_path=("PoC Tests", "SQL Functionalities"),
            linked_generator_id="pg_union_sql_functionality_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="postgres-oltp-s3-union-test",
            title="PostgreSQL OLTP + S3 UNION",
            summary="Executes a UNION across PostgreSQL OLTP and mirrored S3-backed reference data through DuckDB using the same structure in both sources.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-oltp-s3-union-test-cell-1",
                    data_sources=["pg_oltp", *cross_source_s3_sources],
                    sql=cross_source_union_sql,
                )
            ],
            tags=["sql", "union", "postgres", "oltp", "s3"],
            tree_path=("PoC Tests", "SQL Functionalities"),
            linked_generator_id="pg_union_sql_functionality_s3_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="federal-tax-parquet-optimization-off",
            title="Federal Tax Parquet Optimization - Off",
            summary="Queries the federal tax S3 Parquet layout written as one object without partitioning, sorting, or DuckDB cache indexes.",
            cells=[
                NotebookCellDefinition(
                    cell_id="federal-tax-parquet-optimization-off-cell-1",
                    data_sources=parquet_off_sources,
                    query_options={"duckdb": {"parquetHivePartitioning": "auto"}},
                    sql=federal_tax_parquet_performance_sql(
                        parquet_performance_option_relations.get("federal_tax_parquet_off"),
                        "Off",
                    ),
                )
            ],
            tags=["performance", "parquet", "federal-tax", "optimization", "off"],
            tree_path=("PoC Tests", "Performance Options"),
            linked_generator_id="parquet_performance_options_off_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="federal-tax-parquet-optimization-recommended",
            title="Federal Tax Parquet Optimization - Recommended",
            summary="Queries the federal tax S3 Parquet layout written by the recommended mode, currently a conservative single-object Parquet write.",
            cells=[
                NotebookCellDefinition(
                    cell_id="federal-tax-parquet-optimization-recommended-cell-1",
                    data_sources=parquet_recommended_sources,
                    query_options={"duckdb": {"parquetHivePartitioning": "auto"}},
                    sql=federal_tax_parquet_performance_sql(
                        parquet_performance_option_relations.get("federal_tax_parquet_recommended"),
                        "Recommended",
                    ),
                )
            ],
            tags=["performance", "parquet", "federal-tax", "optimization", "recommended"],
            tree_path=("PoC Tests", "Performance Options"),
            linked_generator_id="parquet_performance_options_recommended_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="federal-tax-parquet-optimization-manual-no-hive",
            title="Federal Tax Parquet Optimization - Manual Partition Hive Off",
            summary="Queries the federal tax S3 Parquet layout written into tax_year folders while keeping the partition column inside the Parquet files.",
            cells=[
                NotebookCellDefinition(
                    cell_id="federal-tax-parquet-optimization-manual-no-hive-cell-1",
                    data_sources=parquet_manual_partition_sources,
                    query_options={"duckdb": {"parquetHivePartitioning": "off"}},
                    sql=federal_tax_parquet_performance_sql(
                        parquet_performance_option_relations.get("federal_tax_parquet_manual_partition"),
                        "Manual partition, Hive off",
                    ),
                )
            ],
            tags=["performance", "parquet", "federal-tax", "optimization", "manual", "partitioned"],
            tree_path=("PoC Tests", "Performance Options"),
            linked_generator_id="parquet_performance_options_manual_partition_no_hive_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="federal-tax-parquet-optimization-manual-hive",
            title="Federal Tax Parquet Optimization - Manual Partition Hive On",
            summary="Queries the federal tax S3 Parquet layout written into Hive-style tax_year folders and read with Hive partition interpretation enabled.",
            cells=[
                NotebookCellDefinition(
                    cell_id="federal-tax-parquet-optimization-manual-hive-cell-1",
                    data_sources=parquet_manual_hive_sources,
                    query_options={"duckdb": {"parquetHivePartitioning": "on"}},
                    sql=federal_tax_parquet_performance_sql(
                        parquet_performance_option_relations.get("federal_tax_parquet_manual_hive"),
                        "Manual partition, Hive on",
                    ),
                )
            ],
            tags=["performance", "parquet", "federal-tax", "optimization", "manual", "hive"],
            tree_path=("PoC Tests", "Performance Options"),
            linked_generator_id="parquet_performance_options_manual_partition_hive_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="federal-tax-parquet-optimization-manual-cache",
            title="Federal Tax Parquet Optimization - Manual Cache Only",
            summary="Queries the federal tax S3 Parquet cache-only layout and includes an ART index demonstration after local DuckDB materialization.",
            cells=[
                NotebookCellDefinition(
                    cell_id="federal-tax-parquet-optimization-manual-cache-cell-1",
                    data_sources=parquet_manual_cache_sources,
                    query_options={"duckdb": {"parquetHivePartitioning": "auto"}},
                    sql=federal_tax_parquet_performance_sql(
                        federal_tax_parquet_duckdb_cache_relation(
                            parquet_performance_option_relations.get("federal_tax_parquet_manual_cache")
                        ),
                        "Manual cache only DuckDB table",
                    ),
                ),
                NotebookCellDefinition(
                    cell_id="federal-tax-parquet-optimization-manual-cache-cell-2",
                    data_sources=parquet_manual_cache_sources,
                    query_options={"duckdb": {"parquetHivePartitioning": "auto"}},
                    sql=federal_tax_parquet_cache_lookup_sql(
                        federal_tax_parquet_duckdb_cache_relation(
                            parquet_performance_option_relations.get("federal_tax_parquet_manual_cache")
                        )
                    ),
                ),
            ],
            tags=["performance", "parquet", "federal-tax", "optimization", "manual", "art"],
            tree_path=("PoC Tests", "Performance Options"),
            linked_generator_id="parquet_performance_options_manual_cache_only_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-oltp",
            title="PG vs S3 Contest OLTP via DuckDB",
            summary="Approximates a quarterly tax-pressure dashboard by aggregating recent tax assessments in PostgreSQL OLTP and ranking cantons, tax types, and sectors with high open balances and audit pressure.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=contest_postgres_sql,
                )
            ],
            tags=["performance", "contest", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation", "Single-Table Test"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-s3",
            title="PG vs S3 Contest S3 via DuckDB",
            summary="Runs the same quarterly tax-pressure aggregation against the mirrored S3 dataset to compare how the same high-exposure tax segments perform through DuckDB on object storage.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-s3-cell-1",
                    data_sources=contest_s3_sources,
                    sql=contest_s3_sql,
                )
            ],
            tags=["performance", "contest", "s3"],
            tree_path=("PoC Tests", "Performance Evaluation", "Single-Table Test"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-pg-native",
            title="PG vs S3 Contest OLTP via Native",
            summary="Runs the same quarterly tax-pressure aggregation directly inside PostgreSQL OLTP, without DuckDB, to compare native execution on the high-exposure tax segments.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-pg-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=contest_postgres_native_sql,
                )
            ],
            tags=["performance", "contest", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "Single-Table Test"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-multi-table-oltp",
            title="Multi-Table Test OLTP via DuckDB",
            summary="Approximates a federal-tax risk dashboard by joining taxpayers, filings, assessments, payments, audits, enforcements, and appeals, then ranking quarterly cantonal segments with the highest open exposure through DuckDB on PostgreSQL OLTP.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-multi-table-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=multi_table_postgres_sql,
                )
            ],
            tags=["performance", "multi-table", "contest", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation", "Multi-Table Test"),
            linked_generator_id="pg_vs_s3_multi_table_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-multi-table-s3",
            title="Multi-Table Test S3 via DuckDB",
            summary="Runs the same federal-tax risk dashboard query against the mirrored S3-backed tables through DuckDB to compare quarterly compliance-pressure and appeal-heavy segments on object storage.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-multi-table-s3-cell-1",
                    data_sources=multi_table_s3_sources,
                    sql=multi_table_s3_sql,
                )
            ],
            tags=["performance", "multi-table", "contest", "s3"],
            tree_path=("PoC Tests", "Performance Evaluation", "Multi-Table Test"),
            linked_generator_id="pg_vs_s3_multi_table_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-multi-table-pg-native",
            title="Multi-Table Test OLTP via Native",
            summary="Runs the same federal-tax risk dashboard query directly inside PostgreSQL OLTP, without DuckDB, to compare native execution for the same joined quarterly exposure analysis.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-multi-table-pg-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=multi_table_postgres_native_sql,
                )
            ],
            tags=["performance", "multi-table", "contest", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "Multi-Table Test"),
            linked_generator_id="pg_vs_s3_multi_table_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-oltp",
            title="Kostenbelege (3.1) OLTP via DuckDB",
            summary="Runs the Kostenbelege 3.1 union query against generated KBKP, KBPO, KBHP, and calendar tables in PostgreSQL OLTP through DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=kostenbelege_3_1_oltp_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "postgres", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-olap",
            title="Kostenbelege (3.1) OLAP via DuckDB",
            summary="Runs the Kostenbelege 3.1 union query against generated KBKP, KBPO, KBHP, and calendar tables in PostgreSQL OLAP through DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-olap-cell-1",
                    data_sources=["pg_olap"],
                    sql=kostenbelege_3_1_olap_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "postgres", "olap"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-s3-parquet",
            title="Kostenbelege (3.1) S3 Parquet via DuckDB",
            summary="Runs the Kostenbelege 3.1 union query against generated S3-backed Parquet views for KBKP, KBPO, KBHP, and calendar data.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-s3-parquet-cell-1",
                    data_sources=kostenbelege_3_1_s3_sources,
                    sql=kostenbelege_3_1_s3_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "s3", "parquet"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-s3-parquet-optimized",
            title="Kostenbelege (3.1) S3 Parquet Optimized via DuckDB",
            summary="Runs the rewritten Kostenbelege 3.1 query against generated S3-backed Parquet views, resolving the KBHP fallback in hash-joinable branches.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-s3-parquet-optimized-cell-1",
                    data_sources=kostenbelege_3_1_s3_sources,
                    sql=kostenbelege_3_1_s3_optimized_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "s3", "parquet", "optimized"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-optimized-parquet-dataset",
            title="3.1 Optimized",
            summary=(
                "Materializes the Kostenbelege 3.1 FACT_Buchungsbelegposition "
                "snapshot as an optimized Parquet dataset folder and queries the "
                "resulting S3 dataset."
            ),
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-optimized-parquet-dataset-cell-1",
                    data_sources=kostenbelege_3_1_optimized_dataset_sources,
                    query_options={
                        "duckdb": {"parquetHivePartitioning": "auto"},
                        "validation": {"sourceExistence": "off"},
                    },
                    sql=_build_kostenbelege_3_1_optimized_dataset_copy_sql(),
                ),
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-optimized-parquet-dataset-cell-2",
                    data_sources=kostenbelege_3_1_optimized_dataset_sources,
                    query_options={
                        "duckdb": {"parquetHivePartitioning": "auto"},
                        "validation": {"sourceExistence": "off"},
                    },
                    sql=_build_kostenbelege_3_1_optimized_dataset_query_sql(),
                ),
            ],
            tags=[
                "performance",
                "parquet",
                "kostenbelege",
                "3.1",
                "optimized",
                "dataset",
            ],
            tree_path=("PoC Tests", "Parquet Optimization"),
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-oltp-native",
            title="Kostenbelege (3.1) OLTP via Native PostgreSQL",
            summary="Runs the Kostenbelege 3.1 union query directly in PostgreSQL OLTP with quoted source columns for the generated mixed-case schema.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-oltp-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=kostenbelege_3_1_oltp_native_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-olap-native",
            title="Kostenbelege (3.1) OLAP via Native PostgreSQL",
            summary="Runs the Kostenbelege 3.1 union query directly in PostgreSQL OLAP with quoted source columns for the generated mixed-case schema.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-olap-native-cell-1",
                    data_sources=["pg_olap_native"],
                    sql=kostenbelege_3_1_olap_native_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-oltp",
            title="MWA Abrechnung (3.2) OLTP via DuckDB",
            summary="Aggregates generated MWA Abrechnung and Abrechnungs-Ziffern records from PostgreSQL OLTP through DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=mwa_postgres_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "postgres", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-pg-native",
            title="MWA Abrechnung (3.2) OLTP via Native",
            summary="Runs the same MWA Abrechnung aggregation directly inside PostgreSQL OLTP, without DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-pg-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=mwa_postgres_native_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-s3-parquet",
            title="MWA Abrechnung (3.2) S3 Parquet via DuckDB",
            summary="Runs the MWA Abrechnung aggregation against the Parquet S3 views, using columnar storage as the primary object-storage benchmark.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-s3-parquet-cell-1",
                    data_sources=mwa_s3_parquet_sources,
                    sql=mwa_s3_parquet_sql,
                ),
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-s3-parquet-art-index-cell-2",
                    data_sources=mwa_s3_parquet_abrechnung_sources,
                    sql=mwa_s3_parquet_art_index_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "s3", "parquet"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-s3-csv",
            title="MWA Abrechnung (3.2) S3 CSV via DuckDB",
            summary="Runs the MWA Abrechnung aggregation against the CSV S3 views to compare text parsing overhead against Parquet.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-s3-csv-cell-1",
                    data_sources=mwa_s3_csv_sources,
                    sql=mwa_s3_csv_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "s3", "csv"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-s3-json",
            title="MWA Abrechnung (3.2) S3 JSONL via DuckDB",
            summary="Runs the MWA Abrechnung aggregation against the JSONL S3 views to compare row-oriented JSON parsing against Parquet.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-s3-json-cell-1",
                    data_sources=mwa_s3_json_sources,
                    sql=mwa_s3_json_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "s3", "json"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
    ]

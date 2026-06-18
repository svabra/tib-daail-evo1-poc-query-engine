from __future__ import annotations

from dataclasses import dataclass

from .notebook_presets import (
    _build_kostenbelege_3_1_optimized_sql,
    _build_kostenbelege_3_1_sql,
)
from .s3_storage import is_likely_local_s3_endpoint
from .sql_utils import sql_literal


FACT_BUPO_TARGET = "s3://core/fact_bupo.parquet"
KBKP_TODAY_TARGET = "s3://core/kbkp_today.parquet"
KBKP_FULL_PATH = "s3://CORE/KBKPfull.parquet"
KBHP_FULL_PATH = "s3://CORE/KBHPfull.parquet"
KALENDER_PATH = "s3://3_1_imports/DIM_Kalender.parquet"
KBPO_PATHS = (
    "s3://KBPOimports/KBPO_2018undvorher.parquet",
    "s3://KBPOimports/KBPO_2019.parquet",
    "s3://KBPOimports/KBPO2020.parquet",
    "s3://KBPOimports/KBPO2021.parquet",
    "s3://KBPOimports/KBPO2022.parquet",
    "s3://KBPOimports/KBPO2023.parquet",
    "s3://KBPOimports/KBPO2024.parquet",
    "s3://KBPOimports/KBPO2025.parquet",
)
LOCAL_COMPATIBLE_S3_PATH_REPLACEMENTS = {
    "s3://CORE/": "s3://core/",
    "s3://KBPOimports/": "s3://kbpoimports/",
    "s3://3_1_imports/": "s3://3-1-imports/",
}


@dataclass(frozen=True, slots=True)
class PureDuckDBCell:
    cell_id: str
    label: str
    sql: str
    remarks: tuple[str, ...] = ()

    @property
    def payload(self) -> dict[str, object]:
        return {
            "cellId": self.cell_id,
            "label": self.label,
            "sql": self.sql,
            "remarks": list(self.remarks),
        }

    def payload_with_sql(self, sql: str) -> dict[str, object]:
        payload = self.payload
        payload["sql"] = sql
        return payload


def _read_parquet(path: str) -> str:
    return f"read_parquet({sql_literal(path)})"


def _kbpo_union_by_name_relation() -> str:
    paths = ",\n    ".join(sql_literal(path) for path in KBPO_PATHS)
    return f"read_parquet([\n    {paths}\n], union_by_name = true)"


def _fact_bupo_select_sql(
    *,
    kbkp_relation: str | None = None,
    kbpo_relation: str | None = None,
    kbhp_relation: str | None = None,
    kalender_relation: str | None = None,
) -> str:
    return _build_kostenbelege_3_1_sql(
        kbkp_relation=kbkp_relation or _read_parquet(KBKP_FULL_PATH),
        kbpo_relation=kbpo_relation or _kbpo_union_by_name_relation(),
        kbhp_relation=kbhp_relation or _read_parquet(KBHP_FULL_PATH),
        kalender_relation=kalender_relation or _read_parquet(KALENDER_PATH),
        quote_source_columns=False,
    ).rstrip(";")


def _optimized_fact_bupo_select_sql(
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


def _fact_bupo_cte_sql() -> str:
    return f"""
WITH fact_bupo AS (
{_fact_bupo_select_sql()}
)
""".strip()


def _optimized_fact_bupo_cte_sql() -> str:
    return f"""
WITH fact_bupo AS (
{_optimized_fact_bupo_select_sql()}
)
""".strip()


def _query_1_sql() -> str:
    return f"""
-- Single-query FACT_Buchungsbelegposition replication with a filtered aggregate.
{_fact_bupo_cte_sql()}
SELECT
      COUNT(*) AS cnt
    , SUM(BetragHauswaehrung) AS total
FROM fact_bupo
WHERE Buchungsdatum >= DATE '2023-01-01'
  AND Buchungsdatum <  DATE '2024-01-01';
""".strip()


def _query_1b_sql() -> str:
    return f"""
-- Optimized single-query FACT_Buchungsbelegposition replication with a filtered aggregate.
{_optimized_fact_bupo_cte_sql()}
SELECT
      COUNT(*) AS cnt
    , SUM(BetragHauswaehrung) AS total
FROM fact_bupo
WHERE Buchungsdatum >= DATE '2023-01-01'
  AND Buchungsdatum <  DATE '2024-01-01';
""".strip()


QUERY_1B_OPTIMIZATION_REMARKS = (
    "Query 1b keeps the result shape and business semantics of Query 1: it returns the same cnt and total aggregate over FACT_Buchungsbelegposition for the 2023 booking-date range. The result remains consistent with Query 1.",
    "The optimization is in the FACT-building SQL. The expensive joined row set is built once through current_kalender, base_positions, position_specific, and resolved_positions instead of repeating the large join tree in two UNION ALL branches.",
    "Ledger-account fallback is resolved once: DuckDB first keeps exact KBHP position matches and only performs the document-level fallback for rows where the exact position match is missing.",
    "Original and settlement rows are then derived from the resolved rows with a two-row CROSS JOIN that supplies PositionsArt and AmountSign. Amount signs and original-versus-settlement date semantics are preserved with the same business mapping as Query 1.",
    "No runtime path changes are part of this optimization. It still uses the Pure DuckDB direct in-process execution path, the same S3 configuration, the same read_parquet inputs, and no shared DuckDB catalog access.",
)


QUERY_2B_OPTIMIZATION_REMARKS = (
    "Query 2b keeps the output contract of Query 2: it writes the same FACT_Buchungsbelegposition row shape to the same single ZSTD Parquet artifact. The result remains consistent with Query 2.",
    "The optimization is in how the FACT rows are built before COPY. Query 2 repeats the wide KBKP, KBPO, and KBHP join work in separate UNION ALL branches for original and settlement rows. Query 2b builds the resolved joined row set once.",
    "The expensive KBKP, KBPO, and KBHP joins are performed once through current source snapshots. Ledger-account fallback is resolved once before the final projection: exact KBHP position matches are kept first, and the document-level fallback is used only for rows without an exact position match.",
    "Original and settlement rows are derived from the resolved position set with a two-row CROSS JOIN. Originalposition keeps the amount sign, Ausgleichsposition applies the negative sign, and the original-versus-settlement date semantics are preserved.",
    "No runtime path, S3 configuration, or execution engine behavior changes are part of this optimization. It still uses Pure DuckDB direct in-process execution, read_parquet inputs, and a single COPY TO Parquet output.",
    "Regression checks confirm that schema, row count, grouped fingerprints, and amount totals match Query 2.",
)


def _materialized_fact_bupo_copy_sql(*, optimized: bool, comment: str) -> str:
    builder = _optimized_fact_bupo_select_sql if optimized else _fact_bupo_select_sql
    fact_select = builder(
        kbkp_relation="kbkp_today",
        kbpo_relation="kbpo_today",
        kbhp_relation="kbhp_today",
        kalender_relation="(SELECT CURRENT_DATE AS Datum)",
    )
    fact_select_ctes = fact_select[5:] if fact_select.upper().startswith("WITH ") else fact_select
    return f"""
{comment}
COPY (
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
)
TO {sql_literal(FACT_BUPO_TARGET)}
(
    FORMAT parquet,
    COMPRESSION zstd,
    ROW_GROUP_SIZE 250000,
    OVERWRITE_OR_IGNORE true
);
""".strip()


def _query_2_sql() -> str:
    return _materialized_fact_bupo_copy_sql(
        optimized=False,
        comment="-- Materialize FACT_Buchungsbelegposition once as an S3 Parquet artifact.",
    )


def _query_2b_sql() -> str:
    return _materialized_fact_bupo_copy_sql(
        optimized=True,
        comment="-- Optimized materialization of FACT_Buchungsbelegposition as an S3 Parquet artifact.",
    )


def _query_3_sql() -> str:
    return f"""
-- Single-query FACT_Buchungsbelegposition replication with a full aggregate.
{_fact_bupo_cte_sql()}
SELECT
      COUNT(*) AS total_rows
    , SUM(BetragHauswaehrung) AS sum_betrag_hw
    , AVG(BetragHauswaehrung) AS avg_betrag_hw
    , MIN(BetragHauswaehrung) AS min_betrag_hw
    , MAX(BetragHauswaehrung) AS max_betrag_hw
FROM fact_bupo;
""".strip()


def _query_4_sql() -> str:
    return f"""
COPY (
    SELECT *
    FROM {_read_parquet(KBKP_FULL_PATH)}
    WHERE CURRENT_DATE BETWEEN KBKP_TechBeginnDt AND KBKP_TechEndeDt
)
TO {sql_literal(KBKP_TODAY_TARGET)}
(
    FORMAT parquet,
    COMPRESSION zstd,
    OVERWRITE_OR_IGNORE true
);
""".strip()


def _query_5_sql() -> str:
    return f"""
SELECT *
FROM {_read_parquet(KBKP_TODAY_TARGET)};
""".strip()


def _query_6_sql() -> str:
    return f"""
SELECT
    COUNT(*) AS total_rows,
    SUM(BetragHauswaehrung) AS sum_betrag_hw,
    AVG(BetragHauswaehrung) AS avg_betrag_hw,
    MIN(BetragHauswaehrung) AS min_betrag_hw,
    MAX(BetragHauswaehrung) AS max_betrag_hw
FROM {_read_parquet(FACT_BUPO_TARGET)};
""".strip()


def _query_7_sql() -> str:
    return f"""
SELECT
    COUNT(*) AS cnt,
    SUM(BetragHauswaehrung) AS total
FROM {_read_parquet(FACT_BUPO_TARGET)}
WHERE Buchungsdatum >= DATE '2023-01-01'
  AND Buchungsdatum < DATE '2024-01-01';
""".strip()


def _query_8_sql() -> str:
    return f"""
SELECT
    BelegartID,
    COUNT(*) AS cnt,
    SUM(BetragHauswaehrung) AS total_amount
FROM {_read_parquet(FACT_BUPO_TARGET)}
GROUP BY BelegartID
ORDER BY total_amount DESC;
""".strip()


def _query_9_sql() -> str:
    return f"""
SELECT
    BelegartID,
    WaehrungHauptbuchID,
    Ausgleichsstatus,
    COUNT(*) AS cnt,
    SUM(BetragHauswaehrung) AS total_amount
FROM {_read_parquet(FACT_BUPO_TARGET)}
GROUP BY
    BelegartID,
    WaehrungHauptbuchID,
    Ausgleichsstatus
ORDER BY cnt DESC;
""".strip()


def _query_10_sql() -> str:
    return f"""
-- 5. HIGH CARDINALITY GROUP BY
SELECT
    SachkontoHBID,
    Buchungsdatum,
    COUNT(*) AS cnt,
    SUM(BetragHauswaehrung) AS total_amount
FROM {_read_parquet(FACT_BUPO_TARGET)}
GROUP BY
    SachkontoHBID,
    Buchungsdatum;
""".strip()


def _query_11_sql() -> str:
    return f"""
-- 6. TIME SERIES AGGREGATION
SELECT
    Buchungsdatum,
    SUM(BetragHauswaehrung) AS daily_total,
    COUNT(*) AS transactions
FROM {_read_parquet(FACT_BUPO_TARGET)}
GROUP BY Buchungsdatum
ORDER BY Buchungsdatum;
""".strip()


def _query_12_sql() -> str:
    return f"""
-- 7. DERIVED EXPRESSION AGGREGATION
SELECT
    BelegartID,
    SUM(BetragHauswaehrung * Umrechnungskurs) AS adjusted_amount
FROM {_read_parquet(FACT_BUPO_TARGET)}
GROUP BY BelegartID;
""".strip()


def _query_13_sql() -> str:
    return f"""
-- 8. CONDITIONAL (CASE WHEN) AGGREGATION
SELECT
    BelegartID,
    SUM(CASE
        WHEN Ausgleichsstatus = 'A' THEN BetragHauswaehrung
        ELSE 0
    END) AS cleared_amount,
    SUM(CASE
        WHEN Ausgleichsstatus <> 'A' THEN BetragHauswaehrung
        ELSE 0
    END) AS open_amount
FROM {_read_parquet(FACT_BUPO_TARGET)}
GROUP BY BelegartID;
""".strip()


def _query_14_sql() -> str:
    return f"""
-- 9. TOP-N QUERY WITH SORT
SELECT
    SachkontoHBID,
    SUM(BetragHauswaehrung) AS total_amount
FROM {_read_parquet(FACT_BUPO_TARGET)}
GROUP BY SachkontoHBID
ORDER BY total_amount DESC
LIMIT 10;
""".strip()


def _query_15_sql() -> str:
    return f"""
-- 10. WINDOW FUNCTION (RUNNING TOTAL)
SELECT
    SachkontoHBID,
    Buchungsdatum,
    BetragHauswaehrung,
    SUM(BetragHauswaehrung) OVER (
        PARTITION BY SachkontoHBID
        ORDER BY Buchungsdatum
    ) AS running_total
FROM {_read_parquet(FACT_BUPO_TARGET)};
""".strip()


def _query_16_sql() -> str:
    return f"""
-- 11. DISTINCT COUNT
SELECT
    COUNT(DISTINCT SachkontoHBID) AS distinct_accounts,
    COUNT(DISTINCT BelegartID) AS distinct_doc_types
FROM {_read_parquet(FACT_BUPO_TARGET)};
""".strip()


def _query_17_sql() -> str:
    return f"""
-- 12. COMPLEX ANALYTICAL QUERY
SELECT
    BelegartID,
    WaehrungHauptbuchID,
    DATE_TRUNC('month', Buchungsdatum)::DATE AS mmonth,
    COUNT(*) AS cnt,
    SUM(BetragHauswaehrung) AS total,
    AVG(BetragHauswaehrung) AS avg_amount
FROM {_read_parquet(FACT_BUPO_TARGET)}
WHERE Buchungsdatum >= DATE '2022-01-01'
GROUP BY
    BelegartID,
    WaehrungHauptbuchID,
    DATE_TRUNC('month', Buchungsdatum)::DATE
ORDER BY mmonth, total DESC;
""".strip()


PURE_DUCKDB_CELLS: tuple[PureDuckDBCell, ...] = (
    PureDuckDBCell("pure-duckdb-query-1", "Query 1", _query_1_sql()),
    PureDuckDBCell(
        "pure-duckdb-query-1b",
        "Query 1b",
        _query_1b_sql(),
        remarks=QUERY_1B_OPTIMIZATION_REMARKS,
    ),
    PureDuckDBCell("pure-duckdb-query-2", "Query 2", _query_2_sql()),
    PureDuckDBCell(
        "pure-duckdb-query-2b",
        "Query 2b",
        _query_2b_sql(),
        remarks=QUERY_2B_OPTIMIZATION_REMARKS,
    ),
    PureDuckDBCell("pure-duckdb-query-3", "Query 3", _query_3_sql()),
    PureDuckDBCell("pure-duckdb-query-4", "Query 4", _query_4_sql()),
    PureDuckDBCell("pure-duckdb-query-5", "Query 5", _query_5_sql()),
    PureDuckDBCell("pure-duckdb-query-6", "Query 6", _query_6_sql()),
    PureDuckDBCell("pure-duckdb-query-7", "Query 7", _query_7_sql()),
    PureDuckDBCell("pure-duckdb-query-8", "Query 8", _query_8_sql()),
    PureDuckDBCell("pure-duckdb-query-9", "Query 9", _query_9_sql()),
    PureDuckDBCell("pure-duckdb-query-10", "Query 10", _query_10_sql()),
    PureDuckDBCell("pure-duckdb-query-11", "Query 11", _query_11_sql()),
    PureDuckDBCell("pure-duckdb-query-12", "Query 12", _query_12_sql()),
    PureDuckDBCell("pure-duckdb-query-13", "Query 13", _query_13_sql()),
    PureDuckDBCell("pure-duckdb-query-14", "Query 14", _query_14_sql()),
    PureDuckDBCell("pure-duckdb-query-15", "Query 15", _query_15_sql()),
    PureDuckDBCell("pure-duckdb-query-16", "Query 16", _query_16_sql()),
    PureDuckDBCell("pure-duckdb-query-17", "Query 17", _query_17_sql()),
)


def _local_compatible_sql(sql: str) -> str:
    rewritten = sql
    for source, replacement in LOCAL_COMPATIBLE_S3_PATH_REPLACEMENTS.items():
        rewritten = rewritten.replace(source, replacement)
    return rewritten


def pure_duckdb_cells_payload(settings: object | None = None) -> list[dict[str, object]]:
    endpoint = getattr(settings, "s3_endpoint", None) if settings is not None else None
    if not is_likely_local_s3_endpoint(endpoint):
        return [cell.payload for cell in PURE_DUCKDB_CELLS]
    return [
        cell.payload_with_sql(_local_compatible_sql(cell.sql))
        for cell in PURE_DUCKDB_CELLS
    ]

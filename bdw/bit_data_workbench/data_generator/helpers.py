from __future__ import annotations

from .base import BYTES_PER_GB


VAT_SMOKE_DATASET_COLUMNS = (
    "filing_id BIGINT",
    "company_uid VARCHAR",
    "canton_code VARCHAR",
    "tax_period_start DATE",
    "tax_period_end DATE",
    "submission_channel VARCHAR",
    "filing_status VARCHAR",
    "declared_turnover_chf DECIMAL(18,2)",
    "output_vat_chf DECIMAL(18,2)",
    "input_vat_chf DECIMAL(18,2)",
    "net_vat_due_chf DECIMAL(18,2)",
    "refund_claim_chf DECIMAL(18,2)",
    "audit_flag BOOLEAN",
    "updated_at TIMESTAMP",
)

TAX_ASSESSMENT_DATASET_COLUMNS = (
    "assessment_id BIGINT",
    "taxpayer_uid VARCHAR",
    "canton_code VARCHAR",
    "tax_type VARCHAR",
    "taxpayer_type VARCHAR",
    "filing_channel VARCHAR",
    "assessment_status VARCHAR",
    "payment_status VARCHAR",
    "tax_period_start DATE",
    "tax_period_end DATE",
    "taxable_base_chf DECIMAL(18,2)",
    "declared_deduction_chf DECIMAL(18,2)",
    "assessed_tax_chf DECIMAL(18,2)",
    "collected_tax_chf DECIMAL(18,2)",
    "open_balance_chf DECIMAL(18,2)",
    "industry_sector VARCHAR",
    "audit_risk_score DOUBLE",
    "audit_flag BOOLEAN",
    "assessed_at TIMESTAMP",
)

CROSS_DATABASE_UNION_DATASET_COLUMNS = (
    "record_id BIGINT",
    "taxpayer_uid VARCHAR",
    "canton_code VARCHAR",
    "tax_period_end DATE",
    "net_tax_amount_chf DECIMAL(18,2)",
    "processing_status VARCHAR",
    "risk_band VARCHAR",
    "updated_at TIMESTAMP",
)


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def qualified_name(*parts: str) -> str:
    return ".".join(sql_identifier(part) for part in parts)


def approximate_size_gb(row_count: int, approximate_row_bytes: int) -> float:
    return round((max(0, row_count) * max(64, approximate_row_bytes)) / BYTES_PER_GB, 3)


def vat_smoke_dataset_select(start_row: int, end_row: int) -> str:
    if end_row <= start_row:
        return (
            "SELECT "
            "0::BIGINT AS filing_id, "
            "''::VARCHAR AS company_uid, "
            "''::VARCHAR AS canton_code, "
            "DATE '2024-01-01' AS tax_period_start, "
            "DATE '2024-03-31' AS tax_period_end, "
            "''::VARCHAR AS submission_channel, "
            "''::VARCHAR AS filing_status, "
            "0::DECIMAL(18,2) AS declared_turnover_chf, "
            "0::DECIMAL(18,2) AS output_vat_chf, "
            "0::DECIMAL(18,2) AS input_vat_chf, "
            "0::DECIMAL(18,2) AS net_vat_due_chf, "
            "0::DECIMAL(18,2) AS refund_claim_chf, "
            "false::BOOLEAN AS audit_flag, "
            "TIMESTAMP '2024-01-01 00:00:00' AS updated_at "
            "LIMIT 0"
        )

    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                DATE '2024-01-01' + CAST((row_id % 730) AS INTEGER) AS period_start,
                DATE '2024-01-01' + CAST((row_id % 730) AS INTEGER) + CAST(89 AS INTEGER) AS period_end,
                round(
                    120000.0
                    + (((row_id * 193) % 3500000) / 10.0)
                    + ((row_id % 17) * 275.0),
                    2
                ) AS turnover_raw,
                CASE row_id % 5
                    WHEN 0 THEN 0.081
                    WHEN 1 THEN 0.026
                    WHEN 2 THEN 0.038
                    WHEN 3 THEN 0.081
                    ELSE 0.081
                END AS vat_rate,
                CASE
                    WHEN row_id % 11 IN (0, 1) THEN 1.08 + ((row_id % 5) / 100.0)
                    ELSE 0.24 + ((row_id % 26) / 100.0)
                END AS input_factor
            FROM base
        ),
        valued AS (
            SELECT
                row_id,
                period_start,
                period_end,
                turnover_raw,
                round(turnover_raw * vat_rate, 2) AS output_vat_raw,
                round(round(turnover_raw * vat_rate, 2) * input_factor, 2) AS input_vat_raw
            FROM prepared
        )
        SELECT
            row_id + 1 AS filing_id,
            'CHE-' || CAST(100000000 + (row_id % 900000000) AS VARCHAR) AS company_uid,
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
            period_start AS tax_period_start,
            period_end AS tax_period_end,
            CASE row_id % 4
                WHEN 0 THEN 'portal'
                WHEN 1 THEN 'tax_advisor'
                WHEN 2 THEN 'bulk_upload'
                ELSE 'branch_office'
            END AS submission_channel,
            CASE
                WHEN input_vat_raw > output_vat_raw THEN 'refund_review'
                WHEN row_id % 7 = 0 THEN 'assessed'
                WHEN row_id % 7 = 1 THEN 'reviewed'
                WHEN row_id % 7 = 2 THEN 'submitted'
                WHEN row_id % 7 = 3 THEN 'correction_requested'
                WHEN row_id % 7 = 4 THEN 'assessed'
                WHEN row_id % 7 = 5 THEN 'reviewed'
                ELSE 'closed'
            END AS filing_status,
            CAST(turnover_raw AS DECIMAL(18,2)) AS declared_turnover_chf,
            CAST(output_vat_raw AS DECIMAL(18,2)) AS output_vat_chf,
            CAST(input_vat_raw AS DECIMAL(18,2)) AS input_vat_chf,
            CAST(
                CASE
                    WHEN output_vat_raw >= input_vat_raw THEN round(output_vat_raw - input_vat_raw, 2)
                    ELSE 0
                END
                AS DECIMAL(18,2)
            ) AS net_vat_due_chf,
            CAST(
                CASE
                    WHEN input_vat_raw > output_vat_raw THEN round(input_vat_raw - output_vat_raw, 2)
                    ELSE 0
                END
                AS DECIMAL(18,2)
            ) AS refund_claim_chf,
            (row_id % 100) < 9 AS audit_flag,
            TIMESTAMP '2024-01-01 08:00:00' + ((row_id * 41) % 63072000) * INTERVAL 1 SECOND AS updated_at
        FROM valued
    """


def tax_assessment_dataset_select(start_row: int, end_row: int) -> str:
    if end_row <= start_row:
        return (
            "SELECT "
            "0::BIGINT AS assessment_id, "
            "''::VARCHAR AS taxpayer_uid, "
            "''::VARCHAR AS canton_code, "
            "''::VARCHAR AS tax_type, "
            "''::VARCHAR AS taxpayer_type, "
            "''::VARCHAR AS filing_channel, "
            "''::VARCHAR AS assessment_status, "
            "''::VARCHAR AS payment_status, "
            "DATE '2023-01-01' AS tax_period_start, "
            "DATE '2023-03-31' AS tax_period_end, "
            "0::DECIMAL(18,2) AS taxable_base_chf, "
            "0::DECIMAL(18,2) AS declared_deduction_chf, "
            "0::DECIMAL(18,2) AS assessed_tax_chf, "
            "0::DECIMAL(18,2) AS collected_tax_chf, "
            "0::DECIMAL(18,2) AS open_balance_chf, "
            "''::VARCHAR AS industry_sector, "
            "0::DOUBLE AS audit_risk_score, "
            "false::BOOLEAN AS audit_flag, "
            "TIMESTAMP '2023-01-01 00:00:00' AS assessed_at "
            "LIMIT 0"
        )

    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                DATE '2023-01-01' + CAST((row_id % 1095) AS INTEGER) AS period_start,
                DATE '2023-01-01' + CAST((row_id % 1095) AS INTEGER) + CAST(89 AS INTEGER) AS period_end,
                CASE row_id % 4
                    WHEN 0 THEN 'VAT'
                    WHEN 1 THEN 'COMPANY_TAX'
                    WHEN 2 THEN 'ALCOHOL_TAX'
                    ELSE 'INCOME_TAX'
                END AS tax_type,
                round(
                    250000.0
                    + (((row_id * 257) % 15000000) / 10.0)
                    + ((row_id % 23) * 1500.0),
                    2
                ) AS taxable_base_raw,
                CASE row_id % 4
                    WHEN 0 THEN 0.081
                    WHEN 1 THEN 0.125
                    WHEN 2 THEN 0.036
                    ELSE 0.142
                END AS tax_rate,
                CASE row_id % 5
                    WHEN 0 THEN 0.12
                    WHEN 1 THEN 0.18
                    WHEN 2 THEN 0.08
                    WHEN 3 THEN 0.22
                    ELSE 0.15
                END AS deduction_rate
            FROM base
        ),
        valued AS (
            SELECT
                row_id,
                period_start,
                period_end,
                tax_type,
                taxable_base_raw,
                round(taxable_base_raw * deduction_rate, 2) AS declared_deduction_raw,
                round((taxable_base_raw - round(taxable_base_raw * deduction_rate, 2)) * tax_rate, 2) AS assessed_tax_raw
            FROM prepared
        ),
        settled AS (
            SELECT
                row_id,
                period_start,
                period_end,
                tax_type,
                taxable_base_raw,
                declared_deduction_raw,
                assessed_tax_raw,
                CASE row_id % 6
                    WHEN 0 THEN round(assessed_tax_raw, 2)
                    WHEN 1 THEN round(assessed_tax_raw * 0.82, 2)
                    WHEN 2 THEN round(assessed_tax_raw * 0.64, 2)
                    WHEN 3 THEN round(assessed_tax_raw * 0.48, 2)
                    WHEN 4 THEN round(assessed_tax_raw * 0.15, 2)
                    ELSE 0
                END AS collected_tax_raw
            FROM valued
        )
        SELECT
            row_id + 1 AS assessment_id,
            'CHE-' || CAST(100000000 + (row_id % 900000000) AS VARCHAR) AS taxpayer_uid,
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
            tax_type,
            CASE row_id % 5
                WHEN 0 THEN 'Corporation'
                WHEN 1 THEN 'SME'
                WHEN 2 THEN 'Sole Proprietor'
                WHEN 3 THEN 'Importer'
                ELSE 'Public Institution'
            END AS taxpayer_type,
            CASE row_id % 4
                WHEN 0 THEN 'portal'
                WHEN 1 THEN 'tax_advisor'
                WHEN 2 THEN 'api_batch'
                ELSE 'field_office'
            END AS filing_channel,
            CASE row_id % 6
                WHEN 0 THEN 'under_review'
                WHEN 1 THEN 'assessed'
                WHEN 2 THEN 'appealed'
                WHEN 3 THEN 'enforced'
                WHEN 4 THEN 'assessed'
                ELSE 'closed'
            END AS assessment_status,
            CASE row_id % 5
                WHEN 0 THEN 'paid'
                WHEN 1 THEN 'instalment'
                WHEN 2 THEN 'overdue'
                WHEN 3 THEN 'refund_pending'
                ELSE 'enforcement'
            END AS payment_status,
            period_start AS tax_period_start,
            period_end AS tax_period_end,
            CAST(taxable_base_raw AS DECIMAL(18,2)) AS taxable_base_chf,
            CAST(declared_deduction_raw AS DECIMAL(18,2)) AS declared_deduction_chf,
            CAST(assessed_tax_raw AS DECIMAL(18,2)) AS assessed_tax_chf,
            CAST(collected_tax_raw AS DECIMAL(18,2)) AS collected_tax_chf,
            CAST(
                CASE
                    WHEN assessed_tax_raw > collected_tax_raw THEN round(assessed_tax_raw - collected_tax_raw, 2)
                    ELSE 0
                END
                AS DECIMAL(18,2)
            ) AS open_balance_chf,
            CASE row_id % 6
                WHEN 0 THEN 'Consumer Goods'
                WHEN 1 THEN 'Manufacturing'
                WHEN 2 THEN 'Hospitality'
                WHEN 3 THEN 'Logistics'
                WHEN 4 THEN 'Energy'
                ELSE 'Beverage Production'
            END AS industry_sector,
            round(18.0 + ((row_id * 13) % 8300) / 100.0, 2) AS audit_risk_score,
            ((row_id % 100) < 12) OR ((row_id % 19) = 0) AS audit_flag,
            TIMESTAMP '2023-01-01 07:00:00' + ((row_id * 73) % 94608000) * INTERVAL 1 SECOND AS assessed_at
        FROM settled
    """


def cross_database_union_dataset_select(start_row: int, end_row: int, profile: str) -> str:
    normalized_profile = profile.strip().lower()
    if normalized_profile == "oltp":
        seed = 11
        channel_shift = 0
    elif normalized_profile == "olap":
        seed = 47
        channel_shift = 2
    elif normalized_profile == "s3":
        seed = 83
        channel_shift = 1
    else:
        raise ValueError(f"Unsupported cross-database union profile: {profile}")

    if end_row <= start_row:
        return (
            "SELECT "
            "0::BIGINT AS record_id, "
            "''::VARCHAR AS taxpayer_uid, "
            "''::VARCHAR AS canton_code, "
            "DATE '2024-01-01' AS tax_period_end, "
            "0::DECIMAL(18,2) AS net_tax_amount_chf, "
            "''::VARCHAR AS processing_status, "
            "''::VARCHAR AS risk_band, "
            "TIMESTAMP '2024-01-01 00:00:00' AS updated_at "
            "LIMIT 0"
        )

    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        ),
        prepared AS (
            SELECT
                row_id,
                DATE '2024-01-01' + CAST(((row_id + {seed}) % 540) AS INTEGER) AS tax_period_end,
                round(
                    8500.0
                    + ((((row_id + {seed} * 101) * 137) % 480000) / 10.0)
                    + ((row_id % 13) * 75.0),
                    2
                ) AS net_tax_amount_raw
            FROM base
        )
        SELECT
            row_id + 1 AS record_id,
            'CHE-' || CAST(100000000 + ((row_id + {seed} * 1000) % 900000000) AS VARCHAR) AS taxpayer_uid,
            CASE (row_id + {seed}) % 10
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
            tax_period_end,
            CAST(net_tax_amount_raw AS DECIMAL(18,2)) AS net_tax_amount_chf,
            CASE (row_id + {channel_shift}) % 4
                WHEN 0 THEN 'received'
                WHEN 1 THEN 'validated'
                WHEN 2 THEN 'posted'
                ELSE 'reconciled'
            END AS processing_status,
            CASE
                WHEN ((row_id + {seed}) % 100) < 12 THEN 'critical'
                WHEN ((row_id + {seed}) % 100) < 35 THEN 'high'
                WHEN ((row_id + {seed}) % 100) < 68 THEN 'medium'
                ELSE 'low'
            END AS risk_band,
            TIMESTAMP '2024-01-01 06:00:00' + ((row_id * 53 + {seed} * 600) % 63072000) * INTERVAL 1 SECOND AS updated_at
        FROM prepared
    """

from __future__ import annotations

from .base import BYTES_PER_GB


SMOKE_DATASET_COLUMNS = (
    "order_id BIGINT",
    "customer_id BIGINT",
    "account_id BIGINT",
    "canton_code VARCHAR",
    "country_code VARCHAR",
    "sales_channel VARCHAR",
    "product_category VARCHAR",
    "order_status VARCHAR",
    "amount_chf DECIMAL(18,2)",
    "quantity INTEGER",
    "discount_pct DOUBLE",
    "priority_flag BOOLEAN",
    "order_date DATE",
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


def smoke_dataset_select(start_row: int, end_row: int) -> str:
    if end_row <= start_row:
      return (
          "SELECT "
          "0::BIGINT AS order_id, "
          "0::BIGINT AS customer_id, "
          "0::BIGINT AS account_id, "
          "''::VARCHAR AS canton_code, "
          "''::VARCHAR AS country_code, "
          "''::VARCHAR AS sales_channel, "
          "''::VARCHAR AS product_category, "
          "''::VARCHAR AS order_status, "
          "0::DECIMAL(18,2) AS amount_chf, "
          "0::INTEGER AS quantity, "
          "0::DOUBLE AS discount_pct, "
          "false::BOOLEAN AS priority_flag, "
          "DATE '2024-01-01' AS order_date, "
          "TIMESTAMP '2024-01-01 00:00:00' AS updated_at "
          "LIMIT 0"
      )

    return f"""
        WITH base AS (
            SELECT row_id
            FROM range({int(start_row)}, {int(end_row)}) AS series(row_id)
        )
        SELECT
            row_id + 1 AS order_id,
            (row_id % 500000) + 1 AS customer_id,
            (row_id % 120000) + 1 AS account_id,
            CASE row_id % 8
                WHEN 0 THEN 'ZH'
                WHEN 1 THEN 'BE'
                WHEN 2 THEN 'GE'
                WHEN 3 THEN 'VD'
                WHEN 4 THEN 'BS'
                WHEN 5 THEN 'SG'
                WHEN 6 THEN 'TI'
                ELSE 'LU'
            END AS canton_code,
            CASE row_id % 5
                WHEN 0 THEN 'CH'
                WHEN 1 THEN 'DE'
                WHEN 2 THEN 'FR'
                WHEN 3 THEN 'IT'
                ELSE 'AT'
            END AS country_code,
            CASE row_id % 4
                WHEN 0 THEN 'portal'
                WHEN 1 THEN 'mobile'
                WHEN 2 THEN 'partner'
                ELSE 'branch'
            END AS sales_channel,
            CASE row_id % 7
                WHEN 0 THEN 'energy'
                WHEN 1 THEN 'mobility'
                WHEN 2 THEN 'health'
                WHEN 3 THEN 'tax'
                WHEN 4 THEN 'permits'
                WHEN 5 THEN 'compliance'
                ELSE 'benefits'
            END AS product_category,
            CASE row_id % 6
                WHEN 0 THEN 'new'
                WHEN 1 THEN 'validated'
                WHEN 2 THEN 'processing'
                WHEN 3 THEN 'approved'
                WHEN 4 THEN 'rejected'
                ELSE 'closed'
            END AS order_status,
            CAST(
                round(
                    (((row_id * 17) % 250000) / 100.0)
                    + ((row_id % 11) * 0.37)
                    + ((row_id % 97) / 10.0),
                    2
                )
                AS DECIMAL(18,2)
            ) AS amount_chf,
            CAST(((row_id % 9) + 1) AS INTEGER) AS quantity,
            round(((row_id % 25) / 100.0), 3) AS discount_pct,
            (row_id % 100) < 9 AS priority_flag,
            DATE '2024-01-01' + CAST((row_id % 730) AS INTEGER) AS order_date,
            TIMESTAMP '2024-01-01 00:00:00' + ((row_id * 37) % 63072000) * INTERVAL 1 SECOND AS updated_at
        FROM base
    """

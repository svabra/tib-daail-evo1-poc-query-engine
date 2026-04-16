from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from fastapi.encoders import jsonable_encoder


def export_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def export_excel_cell_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        try:
            return float(value)
        except Exception:
            return format(value, "f")
    return value


def row_payload(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return jsonable_encoder(
        {
            column: row[index] if index < len(row) else None
            for index, column in enumerate(columns)
        }
    )

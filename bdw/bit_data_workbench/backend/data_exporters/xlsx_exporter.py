from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from openpyxl import Workbook

from .options import QueryResultExportOptions
from .tabular_rows import export_excel_cell_value


StreamRows = Callable[[Callable[[list[str], list[tuple[Any, ...]]], None]], None]


def write_xlsx(local_path: Path, stream_rows: StreamRows, options: QueryResultExportOptions) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = options.excel_sheet_name

    if options.excel_freeze_header and options.excel_include_header:
        worksheet.freeze_panes = "A2"

    header_written = False

    def consume(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        nonlocal header_written
        if options.excel_include_header and not header_written:
            worksheet.append(list(columns))
            header_written = True
        for row in rows:
            worksheet.append([export_excel_cell_value(value) for value in row])

    stream_rows(consume)
    workbook.save(local_path)

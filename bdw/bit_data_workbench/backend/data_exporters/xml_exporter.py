from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
from xml.sax.saxutils import escape

from .options import QueryResultExportOptions, normalize_xml_name
from .tabular_rows import export_cell_value


StreamRows = Callable[[Callable[[list[str], list[tuple[Any, ...]]], None]], None]


def write_xml(local_path: Path, stream_rows: StreamRows, options: QueryResultExportOptions) -> None:
    newline = "\n" if options.xml_pretty_print else ""
    indent = "  " if options.xml_pretty_print else ""

    with local_path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(f'<?xml version="1.0" encoding="UTF-8"?>{newline}')
        handle.write(f"<{options.xml_root_name}>{newline}")

        def consume(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
            normalized_columns = [
                normalize_xml_name(column, f"column_{index + 1}")
                for index, column in enumerate(columns)
            ]
            for row in rows:
                if options.xml_pretty_print:
                    handle.write(f"{indent}<{options.xml_row_name}>{newline}")
                else:
                    handle.write(f"<{options.xml_row_name}>")
                for index, column_name in enumerate(normalized_columns):
                    value = row[index] if index < len(row) else None
                    text = escape(export_cell_value(value))
                    if options.xml_pretty_print:
                        handle.write(f"{indent}{indent}<{column_name}>{text}</{column_name}>{newline}")
                    else:
                        handle.write(f"<{column_name}>{text}</{column_name}>")
                if options.xml_pretty_print:
                    handle.write(f"{indent}</{options.xml_row_name}>{newline}")
                else:
                    handle.write(f"</{options.xml_row_name}>")

        stream_rows(consume)
        handle.write(f"</{options.xml_root_name}>{newline}")

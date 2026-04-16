from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Callable

from ..ingestion_types.csv.dialect import csv_s3_metadata
from .options import QueryResultExportOptions
from .tabular_rows import export_cell_value


StreamRows = Callable[[Callable[[list[str], list[tuple[Any, ...]]], None]], None]


def csv_export_metadata(options: QueryResultExportOptions) -> dict[str, str]:
    return csv_s3_metadata(
        delimiter=options.csv_delimiter,
        has_header=options.csv_include_header,
    )


def write_csv(local_path: Path, stream_rows: StreamRows, options: QueryResultExportOptions) -> None:
    with local_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=options.csv_delimiter)
        header_written = False

        def consume(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
            nonlocal header_written
            if options.csv_include_header and not header_written:
                writer.writerow(columns)
                header_written = True
            for row in rows:
                writer.writerow([export_cell_value(value) for value in row])

        stream_rows(consume)

from __future__ import annotations

import tempfile
from pathlib import Path
import shutil
from typing import Any, Callable

import duckdb

from .csv_exporter import write_csv
from .options import QueryResultExportOptions


StreamRows = Callable[[Callable[[list[str], list[tuple[Any, ...]]], None]], None]


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def write_parquet(local_path: Path, stream_rows: StreamRows, _options: QueryResultExportOptions) -> None:
    temp_dir = Path(tempfile.mkdtemp(prefix="bdw-query-export-parquet-"))
    temp_csv_path = temp_dir / "query-result.csv"
    try:
        write_csv(
            temp_csv_path,
            stream_rows,
            QueryResultExportOptions(csv_delimiter=",", csv_include_header=True),
        )
        connection = duckdb.connect(database=":memory:")
        try:
            connection.execute(
                "COPY ("
                f"SELECT * FROM read_csv_auto({_sql_literal(temp_csv_path.as_posix())}, HEADER = TRUE)"
                f") TO {_sql_literal(local_path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            connection.close()
    finally:
        temp_csv_path.unlink(missing_ok=True)
        shutil.rmtree(temp_dir, ignore_errors=True)

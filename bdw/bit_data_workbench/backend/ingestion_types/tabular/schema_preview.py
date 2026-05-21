from __future__ import annotations

import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO

import duckdb

from ...sql_utils import sql_literal


def preview_parquet_upload_schema(
    *,
    file_name: str,
    input_file: BinaryIO,
) -> dict[str, Any]:
    normalized_file_name = Path(str(file_name or "").strip()).name or "preview.parquet"
    if not normalized_file_name.lower().endswith(".parquet"):
        raise ValueError("Only .parquet files can be previewed by the Parquet schema endpoint.")

    with TemporaryDirectory() as temp_dir:
        local_path = Path(temp_dir) / normalized_file_name
        if hasattr(input_file, "seek"):
            input_file.seek(0)
        with local_path.open("wb") as output_file:
            shutil.copyfileobj(input_file, output_file)
        if hasattr(input_file, "seek"):
            input_file.seek(0)

        connection = duckdb.connect(":memory:")
        try:
            rows = connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet({sql_literal(local_path.as_posix())})"
            ).fetchall()
        finally:
            connection.close()

    columns = [
        {
            "name": str(row[0] or "").strip() or f"column_{index + 1}",
            "dataType": str(row[1] or "").strip() or "UNKNOWN",
        }
        for index, row in enumerate(rows)
    ]
    return {
        "fileName": normalized_file_name,
        "columns": columns,
        "columnCount": len(columns),
    }

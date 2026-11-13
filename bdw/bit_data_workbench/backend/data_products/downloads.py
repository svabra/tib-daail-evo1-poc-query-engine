from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import tempfile

from ..data_exporters import FORMAT_CONTENT_TYPES, normalize_export_settings, write_export
from ..sql_utils import sql_literal
from .tabular import select_sql


@dataclass(frozen=True)
class ProductDownload:
    local_path: Path
    cleanup_dir: Path
    filename: str
    content_type: str


def prepare_download(product, export_format: str, connection_factory) -> ProductDownload:
    if export_format not in {"xlsx", "parquet", "csv"}:
        raise ValueError("Choose xlsx, parquet or csv.")
    directory = Path(tempfile.mkdtemp(prefix="bdw-product-download-"))
    connection = None
    try:
        connection = connection_factory()
        snapshot = directory / f"{product.slug}.parquet"
        # One typed snapshot, full source, with the trusted product scope applied.
        # Do not reuse the paginated JSON preview or CSV type inference for Parquet.
        connection.execute(
            f"COPY ({select_sql(product)}) TO {sql_literal(snapshot.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        path = directory / f"{product.slug}.{export_format}"
        if export_format != "parquet":

            def stream(consume):
                cursor = connection.execute(
                    f"SELECT * FROM read_parquet({sql_literal(snapshot.as_posix())})"
                )
                columns = [column[0] for column in cursor.description]
                consume(columns, [])
                while rows := cursor.fetchmany(1000):
                    consume(columns, rows)

            write_export(
                export_format=export_format,
                local_path=path,
                stream_rows=stream,
                options=normalize_export_settings(export_format, {"sheetName": "Datenprodukt"}),
            )
        return ProductDownload(path, directory, path.name, FORMAT_CONTENT_TYPES[export_format])
    except Exception:
        shutil.rmtree(directory, ignore_errors=True)
        raise
    finally:
        if connection is not None:
            connection.close()

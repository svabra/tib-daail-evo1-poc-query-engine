from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Iterable

import duckdb


KBPO_FILES = (
    "KBPO_2018undvorher.parquet",
    "KBPO_2019.parquet",
    "KBPO2020.parquet",
    "KBPO2021.parquet",
    "KBPO2022.parquet",
    "KBPO2023.parquet",
    "KBPO2024.parquet",
    "KBPO2025.parquet",
)


def sql_literal(value: object) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate local/test-only Kostenbelege 3.1 Parquet mock files for "
            "the optimized dataset notebook. No production data is included."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(tempfile.gettempdir()) / "bdw-kostenbelege-3-1-mock",
        help="Local output root. Existing files are replaced only with --force.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete the output directory before generating files.",
    )
    parser.add_argument(
        "--target-compressed-mib",
        type=float,
        default=450.0,
        help="Approximate compressed size per KBPO Parquet file. Use 0 for fixed row counts.",
    )
    parser.add_argument(
        "--rows-per-kbpo-file",
        type=int,
        default=0,
        help="Fixed KBPO row count per file. Overrides size tuning when > 0.",
    )
    parser.add_argument(
        "--dimension-rows",
        type=int,
        default=1000,
        help="Rows for KBKP and KBHP support files.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=31,
        help="Deterministic seed used in synthetic payload values.",
    )
    parser.add_argument(
        "--max-tuning-passes",
        type=int,
        default=8,
        help="Maximum attempts to tune KBPO files toward the target size.",
    )
    parser.add_argument("--s3-bucket", default="", help="Optional S3 bucket upload target.")
    parser.add_argument("--s3-prefix", default="", help="Optional S3 prefix for uploaded files.")
    parser.add_argument("--s3-endpoint-url", default="", help="Optional S3 endpoint URL.")
    parser.add_argument("--quiet", action="store_true", help="Print only JSON summary.")
    return parser.parse_args()


def prepare_output_dir(path: Path, *, force: bool) -> None:
    if path.exists() and force:
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def copy_query(connection: duckdb.DuckDBPyConnection, select_sql: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"COPY ({select_sql}) TO {sql_literal(path.as_posix())} "
        "(FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 250000)"
    )


def kbpo_select(*, file_index: int, rows: int, seed: int) -> str:
    return f"""
SELECT
    CAST({file_index} AS BIGINT) * 1000000000 + i AS KBPO_PositionId,
    100000 + (i % 100000) AS KBKP_Belegnummer,
    100000 + ((i + {file_index}) % 100000) AS KBKP_AusgleichBelegnummer,
    1 AS KBPO_VtgKtoWiederholPos,
    1 + (i % 12) AS KBPO_VtgKtoPositionNr,
    i % 3 AS KBPO_Teilposition,
    'GEFA-' || CAST(i % 17 AS VARCHAR) AS GEFA_GeschaeftFall,
    200000 + (i % 50000) AS PART_Partner,
    'KFM-' || CAST(i % 31 AS VARCHAR) AS KBPO_KtoFindMerkmal,
    'HV-' || CAST(i % 11 AS VARCHAR) AS DOCO_Hauptvorgang,
    'TV-' || CAST(i % 19 AS VARCHAR) AS DOCO_Teilvorgang,
    'BT-' || CAST(i % 5 AS VARCHAR) AS DOCO_Belegtyp,
    'VKT-' || CAST(i % 7 AS VARCHAR) AS DOCO_VtrKtoTyp,
    'CHF' AS DOCO_Waehrung,
    'FORM-' || CAST(i % 4 AS VARCHAR) AS DOCO_FormArt,
    CAST(100 + (i % 100000) / 10.0 AS DOUBLE) AS KBPO_GesamtBetrag,
    CAST(50 + (i % 50000) / 10.0 AS DOUBLE) AS KBPO_TWhrBetrag,
    'CHF' AS KBPO_HbWaehrung,
    CAST(75 + (i % 75000) / 10.0 AS DOUBLE) AS KBPO_HbBetrag,
    CAST(80 + (i % 80000) / 10.0 AS DOUBLE) AS KBPO_HWhrBetrag1,
    CAST(1.0 AS DOUBLE) AS KBPO_Umrechnungkurs,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBPO_NettoFaelligkeitDT,
    'VTGP-' || CAST(i % 23 AS VARCHAR) AS VTGP_VtrGegenstand,
    300000 + (i % 90000) AS KBPO_VtrKtoNummer,
    'OPEN' AS KBPO_AusgleichStatus,
    'GR-' || CAST(i % 9 AS VARCHAR) AS KBPO_Ausgleichgrund,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBPO_AusgleichDt,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBPO_AusgleichBuchungDt,
    'NB-' || CAST(i % 100 AS VARCHAR) AS KBPO_HBSachkto,
    repeat(md5(CAST(i + {seed} + {file_index} AS VARCHAR)), 12) AS KBPO_Beschreibung,
    'ST-' || CAST(i % 6 AS VARCHAR) AS DOCO_SteuerCd,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBPO_WertInternDt,
    'BANK-' || CAST(i % 41 AS VARCHAR) AS KBPO_Bankverbindung,
    'RA-' || CAST(i % 3 AS VARCHAR) AS DOCO_RecordArt,
    DATE '2020-01-01' AS KBPO_TechBeginnDt,
    DATE '2999-12-31' AS KBPO_TechEndeDt
FROM range({max(1, int(rows))}) AS source(i)
""".strip()


def kbkp_select(rows: int) -> str:
    return f"""
SELECT
    100000 + i AS KBKP_Belegnummer,
    'BA-' || CAST(i % 8 AS VARCHAR) AS DOCO_Belegart,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBKP_BelegDt,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBKP_BuchungDt,
    'USER-' || CAST(i % 50 AS VARCHAR) AS KBKP_ErstellungVon,
    NULL::BIGINT AS KBKP_StorniertBelegNummer,
    NULL::BIGINT AS KBKP_StornoBelegNummer,
    'SRC-' || CAST(i % 5 AS VARCHAR) AS DOCO_BelegHerkunft,
    'BG-' || CAST(i % 10 AS VARCHAR) AS DOCO_Buchunggrund,
    DATE '2020-01-01' AS KBKP_TechBeginnDt,
    DATE '2999-12-31' AS KBKP_TechEndeDt
FROM range({max(1, int(rows))}) AS source(i)
""".strip()


def kbhp_select(rows: int) -> str:
    return f"""
SELECT
    i AS KBHP_Id,
    100000 + i AS KBKP_BelegNummer,
    1 + (i % 12) AS KBHP_VTGKtoPositionNr,
    'HB-' || CAST(i % 100 AS VARCHAR) AS KBHP_SachKto,
    'ABS-' || CAST(i % 20 AS VARCHAR) AS KBHP_HBAbstimmschluessel,
    DATE '2020-01-01' AS KBHP_TechBeginnDt,
    DATE '2999-12-31' AS KBHP_TechEndeDt
FROM range({max(1, int(rows))}) AS source(i)
""".strip()


def kalender_select() -> str:
    return """
SELECT CURRENT_DATE AS Datum
UNION ALL
SELECT DATE '2023-01-01' AS Datum
""".strip()


def tune_rows_for_size(
    connection: duckdb.DuckDBPyConnection,
    *,
    path: Path,
    file_index: int,
    target_bytes: int,
    seed: int,
    max_passes: int,
) -> int:
    rows = 10000
    for _attempt in range(max(1, int(max_passes))):
        copy_query(
            connection,
            kbpo_select(file_index=file_index, rows=rows, seed=seed),
            path,
        )
        current_size = path.stat().st_size
        if current_size >= target_bytes:
            return rows
        scale = max(1.2, min(6.0, target_bytes / max(1, current_size)))
        rows = int(math.ceil(rows * scale))
    return rows


def generate_mock_data(args: argparse.Namespace) -> dict[str, object]:
    output_dir = Path(args.output_dir)
    prepare_output_dir(output_dir, force=bool(args.force))
    target_bytes = max(0, int(float(args.target_compressed_mib) * 1024 * 1024))

    connection = duckdb.connect(":memory:")
    generated: list[dict[str, object]] = []
    try:
        for file_index, file_name in enumerate(KBPO_FILES, start=1):
            target_path = output_dir / "KBPOimports" / file_name
            if int(args.rows_per_kbpo_file or 0) > 0 or target_bytes <= 0:
                rows = max(1, int(args.rows_per_kbpo_file or 1000))
                copy_query(
                    connection,
                    kbpo_select(file_index=file_index, rows=rows, seed=int(args.seed)),
                    target_path,
                )
            else:
                rows = tune_rows_for_size(
                    connection,
                    path=target_path,
                    file_index=file_index,
                    target_bytes=target_bytes,
                    seed=int(args.seed),
                    max_passes=int(args.max_tuning_passes),
                )
            generated.append(_file_summary(output_dir, target_path, rows=rows))

        dimension_rows = max(1, int(args.dimension_rows))
        support_files = (
            (output_dir / "CORE" / "kbkpfull.parquet", kbkp_select(dimension_rows)),
            (output_dir / "CORE" / "kbhpfull.parquet", kbhp_select(dimension_rows)),
            (
                output_dir / "n_3_1_imports" / "dim_kalender.parquet",
                kalender_select(),
            ),
        )
        for target_path, select_sql in support_files:
            copy_query(connection, select_sql, target_path)
            generated.append(_file_summary(output_dir, target_path, rows=dimension_rows))
    finally:
        connection.close()

    uploaded = []
    if str(args.s3_bucket or "").strip():
        uploaded = upload_to_s3(
            output_dir,
            bucket=str(args.s3_bucket).strip(),
            prefix=str(args.s3_prefix or "").strip().strip("/"),
            endpoint_url=str(args.s3_endpoint_url or "").strip() or None,
        )

    return {
        "outputDir": output_dir.resolve().as_posix(),
        "targetCompressedMiB": float(args.target_compressed_mib),
        "generated": generated,
        "uploaded": uploaded,
    }


def _file_summary(root: Path, path: Path, *, rows: int) -> dict[str, object]:
    return {
        "path": path.resolve().as_posix(),
        "relativePath": path.relative_to(root).as_posix(),
        "sizeBytes": path.stat().st_size,
        "rowsRequested": rows,
    }


def upload_to_s3(
    output_dir: Path,
    *,
    bucket: str,
    prefix: str,
    endpoint_url: str | None,
) -> list[dict[str, str]]:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for --s3-bucket uploads.") from exc

    client = boto3.client("s3", endpoint_url=endpoint_url)
    uploaded: list[dict[str, str]] = []
    for path in sorted(output_dir.rglob("*.parquet")):
        relative = path.relative_to(output_dir).as_posix()
        key = f"{prefix}/{relative}" if prefix else relative
        client.upload_file(path.as_posix(), bucket, key)
        uploaded.append({"bucket": bucket, "key": key})
    return uploaded


def main() -> int:
    args = parse_args()
    summary = generate_mock_data(args)
    if not args.quiet:
        for item in summary["generated"]:
            print(
                f"{item['relativePath']}: {item['sizeBytes']} bytes "
                f"({item['rowsRequested']} rows requested)"
            )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

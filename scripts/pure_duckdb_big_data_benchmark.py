from __future__ import annotations

import argparse
import json
import math
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend.pure_duckdb import (  # noqa: E402
    FACT_BUPO_TARGET,
    KBHP_FULL_PATH,
    KBKP_FULL_PATH,
    KBKP_TODAY_TARGET,
    KALENDER_PATH,
    KBPO_PATHS,
    PURE_DUCKDB_CELLS,
)
from bit_data_workbench.backend.sql_utils import sql_literal  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate large Pure DuckDB Kostenbelege S3 Parquet fixtures and run "
            "the standalone Pure DuckDB cells. No Parquet data is committed."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(tempfile.gettempdir()) / "bdw-pure-duckdb-big-data",
        help="Local working directory for generated Parquet files.",
    )
    parser.add_argument(
        "--target-compressed-mib",
        type=float,
        default=500.0,
        help="Minimum compressed size for each KBPO input file.",
    )
    parser.add_argument(
        "--rows-per-kbpo-file",
        type=int,
        default=0,
        help="Use a fixed KBPO row count instead of size tuning when > 0.",
    )
    parser.add_argument(
        "--dimension-rows",
        type=int,
        default=200000,
        help="Rows in the KBKP and KBHP support files.",
    )
    parser.add_argument("--seed", type=int, default=31029)
    parser.add_argument("--max-tuning-passes", type=int, default=8)
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--run-api", action="store_true", help="Run all Pure DuckDB cells through the API.")
    parser.add_argument("--run-ui", action="store_true", help="Run all Pure DuckDB cells through the browser UI.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--job-timeout-seconds", type=float, default=7200.0)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--s3-endpoint-url", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key-id", default="minioadmin")
    parser.add_argument("--s3-secret-access-key", default="minioadmin")
    parser.add_argument("--s3-region", default="us-east-1")
    parser.add_argument(
        "--local-compatible-s3-names",
        action="store_true",
        help=(
            "Rewrite production S3 bucket names to AWS/MinIO-compatible local "
            "aliases for upload and API/UI benchmark SQL. Object key casing is preserved."
        ),
    )
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def s3_parts(s3_url: str) -> tuple[str, str]:
    parsed = urlparse(s3_url)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"Expected s3://bucket/key path, got: {s3_url}")
    return parsed.netloc, parsed.path.lstrip("/")


def local_compatible_bucket_name(bucket: str) -> str:
    normalized = "".join(char.lower() if char.isalnum() else "-" for char in bucket)
    normalized = "-".join(part for part in normalized.split("-") if part)
    if not normalized:
        normalized = "bdw"
    if not normalized[0].isalnum():
        normalized = f"b{normalized}"
    if not normalized[-1].isalnum():
        normalized = f"{normalized}b"
    return normalized


def benchmark_s3_url(args: argparse.Namespace, s3_url: str) -> str:
    if not args.local_compatible_s3_names:
        return s3_url
    bucket, key = s3_parts(s3_url)
    return f"s3://{local_compatible_bucket_name(bucket)}/{key}"


def benchmark_sql(args: argparse.Namespace, sql: str) -> str:
    if not args.local_compatible_s3_names:
        return sql
    rewritten = sql
    for s3_url in [KBKP_FULL_PATH, KBHP_FULL_PATH, KALENDER_PATH, FACT_BUPO_TARGET, KBKP_TODAY_TARGET, *KBPO_PATHS]:
        rewritten = rewritten.replace(s3_url, benchmark_s3_url(args, s3_url))
    return rewritten


def local_path_for_s3(output_dir: Path, s3_url: str) -> Path:
    bucket, key = s3_parts(s3_url)
    return output_dir / bucket / key


def copy_query(connection: duckdb.DuckDBPyConnection, select_sql: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"COPY ({select_sql}) TO {sql_literal(path.as_posix())} "
        "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 250000)"
    )


def kbkp_select(rows: int) -> str:
    return f"""
SELECT
    100000 + i AS KBKP_Belegnummer,
    'BA-' || CAST(i % 8 AS VARCHAR) AS DOCO_Belegart,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBKP_BelegDt,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBKP_BuchungDt,
    'USER-' || CAST(i % 500 AS VARCHAR) AS KBKP_ErstellungVon,
    NULL::BIGINT AS KBKP_StorniertBelegNummer,
    NULL::BIGINT AS KBKP_StornoBelegNummer,
    'SRC-' || CAST(i % 13 AS VARCHAR) AS DOCO_BelegHerkunft,
    'BG-' || CAST(i % 17 AS VARCHAR) AS DOCO_Buchunggrund,
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
    'HB-' || CAST(i % 5000 AS VARCHAR) AS KBHP_SachKto,
    'ABS-' || CAST(i % 100 AS VARCHAR) AS KBHP_HBAbstimmschluessel,
    DATE '2020-01-01' AS KBHP_TechBeginnDt,
    DATE '2999-12-31' AS KBHP_TechEndeDt
FROM range({max(1, int(rows))}) AS source(i)
""".strip()


def kbpo_select(*, file_index: int, rows: int, dimension_rows: int, seed: int) -> str:
    matching_span = max(1, int(dimension_rows))
    return f"""
SELECT
    CAST({file_index} AS BIGINT) * 1000000000 + i AS KBPO_PositionId,
    100000 + (i % {matching_span}) AS KBKP_Belegnummer,
    100000 + ((i + {file_index}) % {matching_span}) AS KBKP_AusgleichBelegnummer,
    1 AS KBPO_VtgKtoWiederholPos,
    1 + (i % 12) AS KBPO_VtgKtoPositionNr,
    i % 3 AS KBPO_Teilposition,
    'GEFA-' || CAST(i % 170 AS VARCHAR) AS GEFA_GeschaeftFall,
    200000 + (i % 50000) AS PART_Partner,
    'KFM-' || CAST(i % 310 AS VARCHAR) AS KBPO_KtoFindMerkmal,
    'HV-' || CAST(i % 110 AS VARCHAR) AS DOCO_Hauptvorgang,
    'TV-' || CAST(i % 190 AS VARCHAR) AS DOCO_Teilvorgang,
    'BT-' || CAST(i % 50 AS VARCHAR) AS DOCO_Belegtyp,
    'VKT-' || CAST(i % 70 AS VARCHAR) AS DOCO_VtrKtoTyp,
    'CHF' AS DOCO_Waehrung,
    'FORM-' || CAST(i % 40 AS VARCHAR) AS DOCO_FormArt,
    CAST(100 + (i % 100000) / 10.0 AS DOUBLE) AS KBPO_GesamtBetrag,
    CAST(50 + (i % 50000) / 10.0 AS DOUBLE) AS KBPO_TWhrBetrag,
    'CHF' AS KBPO_HbWaehrung,
    CAST(75 + (i % 75000) / 10.0 AS DOUBLE) AS KBPO_HbBetrag,
    CAST(80 + (i % 80000) / 10.0 AS DOUBLE) AS KBPO_HWhrBetrag1,
    CAST(1.0 AS DOUBLE) AS KBPO_Umrechnungkurs,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBPO_NettoFaelligkeitDT,
    'VTGP-' || CAST(i % 230 AS VARCHAR) AS VTGP_VtrGegenstand,
    300000 + (i % 90000) AS KBPO_VtrKtoNummer,
    CASE WHEN i % 3 = 0 THEN 'A' ELSE 'O' END AS KBPO_AusgleichStatus,
    'GR-' || CAST(i % 90 AS VARCHAR) AS KBPO_Ausgleichgrund,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBPO_AusgleichDt,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBPO_AusgleichBuchungDt,
    'NB-' || CAST(i % 1000 AS VARCHAR) AS KBPO_HBSachkto,
    repeat(md5(CAST(i + {seed} + {file_index} AS VARCHAR)), 14) AS KBPO_Beschreibung,
    'ST-' || CAST(i % 60 AS VARCHAR) AS DOCO_SteuerCd,
    DATE '2023-01-01' + CAST(i % 365 AS INTEGER) AS KBPO_WertInternDt,
    'BANK-' || CAST(i % 410 AS VARCHAR) AS KBPO_Bankverbindung,
    'RA-' || CAST(i % 30 AS VARCHAR) AS DOCO_RecordArt,
    DATE '2020-01-01' AS KBPO_TechBeginnDt,
    DATE '2999-12-31' AS KBPO_TechEndeDt
FROM range({max(1, int(rows))}) AS source(i)
""".strip()


def kalender_select() -> str:
    return """
SELECT CURRENT_DATE AS Datum
UNION ALL
SELECT DATE '2023-01-01' AS Datum
""".strip()


def tune_rows_for_target(
    connection: duckdb.DuckDBPyConnection,
    *,
    path: Path,
    file_index: int,
    target_bytes: int,
    dimension_rows: int,
    seed: int,
    max_passes: int,
) -> int:
    rows = 25000
    for _attempt in range(max(1, int(max_passes))):
        copy_query(
            connection,
            kbpo_select(
                file_index=file_index,
                rows=rows,
                dimension_rows=dimension_rows,
                seed=seed,
            ),
            path,
        )
        current_size = path.stat().st_size
        if current_size >= target_bytes:
            return rows
        rows = int(math.ceil(rows * max(1.25, min(6.0, target_bytes / max(1, current_size)))))
    return rows


def generate_files(args: argparse.Namespace) -> list[dict[str, Any]]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    target_bytes = max(0, int(float(args.target_compressed_mib) * 1024 * 1024))
    dimension_rows = max(1, int(args.dimension_rows))
    summaries: list[dict[str, Any]] = []
    connection = duckdb.connect(":memory:")
    try:
        support_files = (
            (KBKP_FULL_PATH, kbkp_select(dimension_rows), dimension_rows),
            (KBHP_FULL_PATH, kbhp_select(dimension_rows), dimension_rows),
            (KALENDER_PATH, kalender_select(), 2),
        )
        for s3_url, select_sql, rows in support_files:
            path = local_path_for_s3(output_dir, s3_url)
            copy_query(connection, select_sql, path)
            summaries.append(file_summary(output_dir, s3_url, path, rows))

        fixed_rows = int(args.rows_per_kbpo_file or 0)
        for index, s3_url in enumerate(KBPO_PATHS, start=1):
            path = local_path_for_s3(output_dir, s3_url)
            if fixed_rows > 0 or target_bytes <= 0:
                rows = fixed_rows or 1000
                copy_query(
                    connection,
                    kbpo_select(
                        file_index=index,
                        rows=rows,
                        dimension_rows=dimension_rows,
                        seed=int(args.seed),
                    ),
                    path,
                )
            else:
                rows = tune_rows_for_target(
                    connection,
                    path=path,
                    file_index=index,
                    target_bytes=target_bytes,
                    dimension_rows=dimension_rows,
                    seed=int(args.seed),
                    max_passes=int(args.max_tuning_passes),
                )
            summaries.append(file_summary(output_dir, s3_url, path, rows))
    finally:
        connection.close()
    return summaries


def file_summary(root: Path, s3_url: str, path: Path, rows: int) -> dict[str, Any]:
    return {
        "s3Url": s3_url,
        "localPath": path.resolve().as_posix(),
        "relativePath": path.relative_to(root).as_posix(),
        "sizeBytes": path.stat().st_size,
        "sizeMiB": round(path.stat().st_size / 1024 / 1024, 3),
        "rowsRequested": rows,
    }


def s3_client(args: argparse.Namespace):
    return boto3.client(
        "s3",
        endpoint_url=str(args.s3_endpoint_url or "").strip() or None,
        aws_access_key_id=str(args.s3_access_key_id or "").strip() or None,
        aws_secret_access_key=str(args.s3_secret_access_key or "").strip() or None,
        region_name=str(args.s3_region or "").strip() or None,
    )


def ensure_bucket(client: Any, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


def upload_files(args: argparse.Namespace) -> list[dict[str, Any]]:
    output_dir = Path(args.output_dir)
    client = s3_client(args)
    uploaded: list[dict[str, Any]] = []
    s3_urls = [KBKP_FULL_PATH, KBHP_FULL_PATH, KALENDER_PATH, *KBPO_PATHS]
    output_s3_urls = [FACT_BUPO_TARGET, KBKP_TODAY_TARGET]
    for s3_url in [*s3_urls, *output_s3_urls]:
        bucket, _key = s3_parts(benchmark_s3_url(args, s3_url))
        ensure_bucket(client, bucket)
    for s3_url in s3_urls:
        bucket, key = s3_parts(benchmark_s3_url(args, s3_url))
        path = local_path_for_s3(output_dir, s3_url)
        started = time.perf_counter()
        client.upload_file(path.as_posix(), bucket, key)
        uploaded.append(
            {
                "s3Url": s3_url,
                "sizeBytes": path.stat().st_size,
                "uploadMs": round((time.perf_counter() - started) * 1000, 3),
            }
        )
    return uploaded


def request_json(url: str, *, payload: dict[str, Any] | None = None, timeout: float = 60.0) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    method = "GET"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
        method = "POST"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {body}") from exc


def run_api_benchmark(args: argparse.Namespace) -> list[dict[str, Any]]:
    base_url = str(args.base_url or "").rstrip("/")
    results: list[dict[str, Any]] = []
    for cell in PURE_DUCKDB_CELLS:
        started = request_json(
            f"{base_url}/api/pure-duckdb/jobs",
            payload={"cellId": cell.cell_id, "sql": benchmark_sql(args, cell.sql)},
            timeout=60,
        )
        job_id = str(started.get("jobId") or "").strip()
        if not job_id:
            raise RuntimeError(f"No Pure DuckDB job id returned for {cell.label}.")
        deadline = time.monotonic() + float(args.job_timeout_seconds)
        latest = started
        while time.monotonic() < deadline:
            latest = request_json(
                f"{base_url}/api/pure-duckdb/jobs/{job_id}",
                timeout=60,
            )
            if str(latest.get("status") or "").lower() in {"completed", "failed", "cancelled"}:
                break
            time.sleep(max(0.2, float(args.poll_seconds)))
        else:
            raise TimeoutError(f"Timed out waiting for {cell.label} ({job_id}).")

        results.append(
            {
                "cellId": cell.cell_id,
                "label": cell.label,
                "status": latest.get("status"),
                "durationMs": latest.get("durationMs"),
                "message": latest.get("message"),
                "error": latest.get("error"),
                "rowCount": latest.get("rowCount"),
                "timings": latest.get("timings"),
            }
        )
        if str(latest.get("status") or "").lower() != "completed":
            break
    return results


async def run_ui_benchmark(args: argparse.Namespace) -> list[dict[str, Any]]:
    from playwright.async_api import async_playwright

    base_url = str(args.base_url or "").rstrip("/")
    results: list[dict[str, Any]] = []
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1440, "height": 1000})
        try:
            await page.goto(f"{base_url}/pure-duckdb", wait_until="domcontentloaded", timeout=60000)
            for index, cell in enumerate(PURE_DUCKDB_CELLS):
                locator = page.locator("[data-pure-duckdb-cell]").nth(index)
                await locator.locator("[data-pure-duckdb-sql]").fill(benchmark_sql(args, cell.sql))
                await locator.locator("[data-run-pure-duckdb-cell]").click(timeout=60000)
                await locator.locator("[data-run-pure-duckdb-cell]").wait_for(
                    state="visible",
                    timeout=int(float(args.job_timeout_seconds) * 1000),
                )
                await page.wait_for_function(
                    "(el) => !el.querySelector('[data-run-pure-duckdb-cell]').disabled",
                    arg=await locator.element_handle(),
                    timeout=int(float(args.job_timeout_seconds) * 1000),
                )
                status_text = (await locator.locator("[data-pure-duckdb-result]").inner_text()).strip()
                duration_text = (await locator.locator("[data-pure-duckdb-duration]").inner_text()).strip()
                failed = "Query failed." in status_text
                results.append(
                    {
                        "cellId": cell.cell_id,
                        "label": cell.label,
                        "status": "failed" if failed else "completed",
                        "duration": duration_text,
                        "resultText": status_text[:500],
                    }
                )
                if failed:
                    break
        finally:
            await browser.close()
    return results


def main() -> int:
    args = parse_args()
    summary: dict[str, Any] = {
        "generated": [],
        "uploaded": [],
        "apiResults": [],
        "uiResults": [],
    }

    if not args.skip_generate:
        summary["generated"] = generate_files(args)
        if not args.quiet:
            for item in summary["generated"]:
                print(f"{item['s3Url']}: {item['sizeMiB']} MiB", flush=True)

    if not args.skip_upload:
        summary["uploaded"] = upload_files(args)
        if not args.quiet:
            for item in summary["uploaded"]:
                print(f"uploaded {item['s3Url']} in {item['uploadMs']} ms", flush=True)

    if args.run_api:
        summary["apiResults"] = run_api_benchmark(args)
        for item in summary["apiResults"]:
            print(f"API {item['label']}: {item['status']} {item['durationMs']} ms", flush=True)

    if args.run_ui:
        import asyncio

        summary["uiResults"] = asyncio.run(run_ui_benchmark(args))
        for item in summary["uiResults"]:
            print(f"UI {item['label']}: {item['status']} {item['duration']}", flush=True)

    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

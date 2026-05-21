from __future__ import annotations

import argparse
import asyncio
import csv
from dataclasses import dataclass
from decimal import Decimal
import io
import json
from pathlib import Path
import re
import sys
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
TERMINAL_QUERY_STATUSES = {"completed", "failed", "cancelled"}


@dataclass(frozen=True, slots=True)
class UploadCase:
    case_id: str
    label: str
    mode: str
    hive_partitioning: bool = False
    partition_columns: tuple[str, ...] = ()
    sort_columns: tuple[str, ...] = ()
    create_duckdb_cache: bool = False
    index_columns: tuple[str, ...] = ()
    query_hive_option: str = "auto"
    expect_partitioned: bool = False


CASES = (
    UploadCase(
        case_id="off",
        label="Off",
        mode="off",
    ),
    UploadCase(
        case_id="recommended",
        label="Recommended",
        mode="recommended",
    ),
    UploadCase(
        case_id="manual_no_hive_partitioned",
        label="Manual Hive Off Partitioned",
        mode="manual",
        hive_partitioning=False,
        partition_columns=("tax_year",),
        sort_columns=("filing_date", "taxpayer_id"),
        query_hive_option="off",
        expect_partitioned=True,
    ),
    UploadCase(
        case_id="manual_hive_partitioned",
        label="Manual Hive On Partitioned",
        mode="manual",
        hive_partitioning=True,
        partition_columns=("tax_year",),
        sort_columns=("filing_date", "taxpayer_id"),
        query_hive_option="on",
        expect_partitioned=True,
    ),
    UploadCase(
        case_id="manual_cache_only",
        label="Manual Cache Only",
        mode="manual",
        create_duckdb_cache=True,
        index_columns=("taxpayer_id",),
        query_hive_option="auto",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload federal tax CSV data to S3 as Parquet across optimization modes, "
            "save one shared notebook per upload, and run the notebook queries."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="vat-smoke-test")
    parser.add_argument("--s3-smoke-prefix-root", default="playwright/parquet-options")
    parser.add_argument("--sample-path", default=str(REPO_ROOT / "sample_tax_data.csv"))
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=90000)
    return parser.parse_args()


def s3_client(args: argparse.Namespace):
    return boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret_key,
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if status not in {400, 404} and code not in {"404", "NoSuchBucket"}:
            raise
        client.create_bucket(Bucket=bucket)


def list_s3_keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        keys.extend(str(item["Key"]) for item in response.get("Contents", []))
        if not response.get("IsTruncated"):
            return sorted(keys)
        token = response.get("NextContinuationToken")


def delete_s3_prefix(client, bucket: str, prefix: str) -> None:
    normalized_prefix = "/".join(segment for segment in prefix.split("/") if segment)
    if not normalized_prefix:
        return
    while True:
        response = client.list_objects_v2(Bucket=bucket, Prefix=f"{normalized_prefix}/")
        objects = [{"Key": item["Key"]} for item in response.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        if not response.get("IsTruncated"):
            return


def federal_tax_csv(sample_path: Path) -> tuple[bytes, dict[str, object]]:
    rows = list(csv.DictReader(io.StringIO(sample_path.read_text(encoding="utf-8"))))
    if not rows:
        raise RuntimeError(f"Sample federal tax CSV has no rows: {sample_path}")
    years = ("2024", "2025", "2026")
    for index, row in enumerate(rows):
        row["tax_year"] = years[index % len(years)]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    total_due = sum((Decimal(row["tax_due_chf"]) for row in rows), Decimal("0"))
    taxpayer_due = next(
        Decimal(row["tax_due_chf"])
        for row in rows
        if row.get("taxpayer_id") == "TX-100001"
    )
    return output.getvalue().encode("utf-8"), {
        "row_count": len(rows),
        "min_year": min(int(row["tax_year"]) for row in rows),
        "max_year": max(int(row["tax_year"]) for row in rows),
        "total_due": float(total_due.quantize(Decimal("0.01"))),
        "taxpayer_due": float(taxpayer_due.quantize(Decimal("0.01"))),
    }


async def open_csv_ingestion(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/ingestion-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    csv_tile = page.locator('[data-ingestion-tile="csv"]').first
    form = page.locator("[data-csv-ingestion-form]")
    for _attempt in range(5):
        await csv_tile.click()
        try:
            await form.wait_for(state="visible", timeout=2000)
            break
        except PlaywrightTimeoutError:
            await page.wait_for_timeout(500)
    await form.wait_for(state="visible", timeout=timeout_ms)


async def poll_upload_session_result(
    page,
    base_url: str,
    session_id: str,
    timeout_ms: int,
) -> dict[str, object]:
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)
    session_url = f"{base_url.rstrip('/')}/api/ingestion/csv/upload-sessions/{session_id}"
    while asyncio.get_running_loop().time() < deadline:
        response = await page.context.request.get(
            session_url,
            headers={"Accept": "application/json"},
        )
        if not response.ok:
            raise RuntimeError(f"Upload session polling failed with HTTP {response.status}.")
        payload = await response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if isinstance(result, dict):
            return result
        if isinstance(payload, dict) and payload.get("status") == "failed":
            raise RuntimeError(f"Upload session failed: {payload!r}")
        await page.wait_for_timeout(500)
    raise RuntimeError("Timed out waiting for upload session import result.")


async def expect_complete_payload(page, base_url: str, timeout_ms: int, click_locator):
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and "/api/ingestion/csv/upload-sessions/" in response.url
        and response.url.endswith("/complete"),
        timeout=timeout_ms,
    ) as response_info:
        await click_locator.click()
    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"CSV completion request failed with HTTP {response.status}.")
    payload = await response.json()
    if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
        return payload["result"]
    if isinstance(payload, dict) and "targetId" in payload:
        return payload
    session_id = str(payload.get("sessionId") or "") if isinstance(payload, dict) else ""
    if session_id:
        return await poll_upload_session_result(page, base_url, session_id, timeout_ms)
    raise RuntimeError(f"Unexpected CSV completion response payload: {payload!r}")


async def close_message_dialog(page, timeout_ms: int) -> None:
    dialog = page.locator("[data-message-dialog]")
    if await dialog.count() and await dialog.is_visible():
        await page.locator("[data-message-submit]").click()
        await dialog.wait_for(state="hidden", timeout=timeout_ms)


async def set_column_checkbox(page, root_selector: str, value: str, checked: bool) -> None:
    checkbox = page.locator(f'{root_selector} input[value="{value}"]').first
    await checkbox.wait_for(state="attached", timeout=15000)
    if await checkbox.is_checked() != checked:
        await checkbox.set_checked(checked)


async def clear_column_group(page, root_selector: str) -> None:
    checkboxes = page.locator(f"{root_selector} input[type='checkbox']")
    count = await checkboxes.count()
    for index in range(count):
        checkbox = checkboxes.nth(index)
        if await checkbox.is_checked():
            await checkbox.set_checked(False)


async def configure_manual_optimization(page, case: UploadCase) -> None:
    await page.locator('[data-csv-parquet-optimization-mode][value="manual"]').check()
    await page.locator("[data-csv-parquet-manual-options]").wait_for(
        state="visible",
        timeout=15000,
    )
    await clear_column_group(page, "[data-csv-partition-column-options]")
    await clear_column_group(page, "[data-csv-sort-column-options]")
    await clear_column_group(page, "[data-csv-index-column-options]")
    for column in case.partition_columns:
        await set_column_checkbox(page, "[data-csv-partition-column-options]", column, True)
    for column in case.sort_columns:
        await set_column_checkbox(page, "[data-csv-sort-column-options]", column, True)
    hive = page.locator("[data-csv-hive-partitioning]").first
    await hive.set_checked(case.hive_partitioning)
    cache = page.locator("[data-csv-create-duckdb-cache]").first
    await cache.set_checked(case.create_duckdb_cache)
    for column in case.index_columns:
        await set_column_checkbox(page, "[data-csv-index-column-options]", column, True)


async def upload_case(
    page,
    args: argparse.Namespace,
    case: UploadCase,
    unique_id: str,
    csv_bytes: bytes,
) -> dict[str, object]:
    base_name = f"federal_tax_{case.case_id}"
    prefix = f"{args.s3_smoke_prefix_root.strip('/')}/{unique_id}/{case.case_id}"
    await open_csv_ingestion(page, args.base_url, args.timeout_ms)
    await close_message_dialog(page, args.timeout_ms)
    await page.locator('[data-csv-target-option][value="workspace.s3"]').check()
    await page.locator('[data-csv-config-panel="workspace.s3"] [data-csv-s3-bucket]').fill(args.bucket)
    await page.locator('[data-csv-config-panel="workspace.s3"] [data-csv-s3-prefix]').fill(prefix)
    await page.locator('[data-csv-s3-storage-format][value="parquet"]').check()
    if case.mode == "recommended":
        await page.locator('[data-csv-parquet-optimization-mode][value="recommended"]').check()
    elif case.mode == "off":
        await page.locator('[data-csv-parquet-optimization-mode][value="off"]').check()

    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": f"{base_name}.csv",
                "mimeType": "text/csv",
                "buffer": csv_bytes,
            }
        ]
    )
    await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").first.wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    await page.locator("[data-csv-import-base-name]").first.fill(base_name)
    await page.locator("[data-csv-import-base-name]").first.evaluate("node => node.blur()")
    if case.mode == "manual":
        await configure_manual_optimization(page, case)

    payload = await expect_complete_payload(
        page,
        args.base_url,
        args.timeout_ms,
        page.locator("[data-csv-import-submit]"),
    )
    await page.locator("[data-csv-result-list] .ingestion-csv-result-card-imported").first.wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    await close_message_dialog(page, args.timeout_ms)
    if payload.get("targetId") != "workspace.s3":
        raise RuntimeError(f"Unexpected target in CSV import payload: {payload!r}")
    if payload.get("importedCount") != 1 or payload.get("failedCount") != 0:
        raise RuntimeError(f"Unexpected import counts for {case.case_id}: {payload!r}")
    imports = payload.get("imports") if isinstance(payload.get("imports"), list) else []
    imported = imports[0] if imports and isinstance(imports[0], dict) else {}
    if not imported:
        raise RuntimeError(f"Import payload did not include imported object details: {payload!r}")
    return {
        "case": case,
        "prefix": prefix,
        "base_name": base_name,
        "imported": imported,
        "query_source": imported.get("querySource") or {},
    }


def assert_uploaded_layout(client, bucket: str, uploaded: dict[str, object]) -> None:
    case: UploadCase = uploaded["case"]  # type: ignore[assignment]
    prefix = str(uploaded["prefix"])
    base_name = str(uploaded["base_name"])
    imported = uploaded["imported"]
    keys = list_s3_keys(client, bucket, prefix)
    if case.expect_partitioned:
        expected_root = f"{prefix}/{base_name}/"
        part_keys = [key for key in keys if key.startswith(expected_root)]
        if not part_keys:
            raise RuntimeError(f"No partitioned Parquet keys found below {expected_root!r}.")
        for year in ("2024", "2025", "2026"):
            if not any(f"/tax_year={year}/" in key for key in part_keys):
                raise RuntimeError(f"Partitioned upload missing tax_year={year}: {part_keys!r}")
        if not all(re.search(r"/data_.*\.parquet$", key) for key in part_keys):
            raise RuntimeError(f"Partitioned upload used unexpected part key names: {part_keys!r}")
        if not imported.get("partitioned"):
            raise RuntimeError(f"Import payload did not mark partitioned upload: {imported!r}")
        return

    expected_key = f"{prefix}/{base_name}.parquet"
    if expected_key not in keys:
        raise RuntimeError(f"Expected single Parquet object {expected_key!r}, got {keys!r}")
    if any("/tax_year=" in key for key in keys):
        raise RuntimeError(f"Single-object case unexpectedly wrote partition folders: {keys!r}")


async def api_post_json(request, base_url: str, path: str, payload: dict[str, object]):
    response = await request.post(
        f"{base_url.rstrip('/')}{path}",
        data=json.dumps(payload),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    if not response.ok:
        raise RuntimeError(f"POST {path} failed with HTTP {response.status}: {await response.text()}")
    return await response.json()


def notebook_query(alias: str) -> str:
    return f"""
SELECT
  CAST(COUNT(*) AS INTEGER) AS row_count,
  CAST(MIN(tax_year) AS INTEGER) AS min_tax_year,
  CAST(MAX(tax_year) AS INTEGER) AS max_tax_year,
  ROUND(SUM(CAST(tax_due_chf AS DOUBLE)), 2) AS total_tax_due_chf,
  ROUND(SUM(
    CASE
      WHEN taxpayer_id = 'TX-100001' THEN CAST(tax_due_chf AS DOUBLE)
      ELSE 0
    END
  ), 2) AS taxpayer_100001_due
FROM {alias}
""".strip()


async def create_notebook_for_case(
    page,
    args: argparse.Namespace,
    uploaded: dict[str, object],
    unique_id: str,
) -> dict[str, object]:
    case: UploadCase = uploaded["case"]  # type: ignore[assignment]
    query_source = uploaded["query_source"]
    alias = str(query_source.get("queryAlias") or query_source.get("relation") or "").strip()
    if not alias:
        raise RuntimeError(f"Import did not return a query source for {case.case_id}: {uploaded!r}")
    cell_id = f"cell-{case.case_id}"
    notebook_id = f"shared-notebook-parquet-options-{unique_id}-{case.case_id}"
    query_options = {"duckdb": {"parquetHivePartitioning": case.query_hive_option}}
    sql = notebook_query(alias)
    await api_post_json(
        page.context.request,
        args.base_url,
        "/api/notebooks/shared/folders",
        {
            "path": ["PoC Tests", "Performance Options"],
            "displayName": "Performance Options",
            "isPublic": True,
            "canEdit": True,
            "canDelete": True,
        },
    )
    result = await api_post_json(
        page.context.request,
        args.base_url,
        "/api/notebooks/shared",
        {
            "notebookId": notebook_id,
            "title": f"Parquet Optimization - {case.label}",
            "summary": (
                "Federal tax CSV regression for S3 Parquet optimization mode "
                f"{case.mode}."
            ),
            "tags": ["poc", "performance", "parquet"],
            "treePath": ["PoC Tests", "Performance Options"],
            "cells": [
                {
                    "cellId": cell_id,
                    "language": "sql",
                    "sql": sql,
                    "dataSources": ["workspace.s3"],
                    "queryOptions": query_options,
                }
            ],
            "versions": [],
        },
    )
    returned_id = str(result.get("notebook", {}).get("notebookId") or "")
    if returned_id != notebook_id:
        raise RuntimeError(f"Shared notebook upsert returned unexpected id: {result!r}")
    return {
        "case": case,
        "notebook_id": notebook_id,
        "cell_id": cell_id,
        "sql": sql,
        "query_options": query_options,
    }


async def verify_notebook_option_ui(page, args: argparse.Namespace, notebook: dict[str, object]) -> None:
    case: UploadCase = notebook["case"]  # type: ignore[assignment]
    await page.goto(
        f"{args.base_url.rstrip('/')}/notebooks/{notebook['notebook_id']}",
        wait_until="domcontentloaded",
        timeout=args.timeout_ms,
    )
    await page.locator("[data-workspace-notebook]").wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    option = page.locator('[data-cell-query-option="duckdb.parquetHivePartitioning"]').first
    await option.wait_for(state="attached", timeout=args.timeout_ms)
    value = await option.input_value()
    if value != case.query_hive_option:
        raise RuntimeError(
            f"Notebook {notebook['notebook_id']} rendered Hive option {value!r}, "
            f"expected {case.query_hive_option!r}."
        )


async def run_query_job(
    page,
    args: argparse.Namespace,
    notebook: dict[str, object],
    expected: dict[str, object],
) -> None:
    response = await page.context.request.post(
        f"{args.base_url.rstrip('/')}/api/query-jobs",
        form={
            "sql": notebook["sql"],
            "displaySql": notebook["sql"],
            "notebook_id": notebook["notebook_id"],
            "notebook_title": f"Parquet Optimization - {notebook['case'].label}",
            "cell_id": notebook["cell_id"],
            "data_sources": "workspace.s3",
            "localRelations": "{}",
            "queryOptions": json.dumps(notebook["query_options"]),
        },
    )
    if not response.ok:
        raise RuntimeError(f"Query job creation failed with HTTP {response.status}: {await response.text()}")
    payload = await response.json()
    job_id = str(payload.get("jobId") or "")
    if not job_id:
        raise RuntimeError(f"Query job response did not include a job id: {payload!r}")

    deadline = asyncio.get_running_loop().time() + (args.timeout_ms / 1000)
    job = None
    while asyncio.get_running_loop().time() < deadline:
        state_response = await page.context.request.get(
            f"{args.base_url.rstrip('/')}/api/query-jobs",
            headers={"Accept": "application/json"},
        )
        if not state_response.ok:
            raise RuntimeError(f"Query job polling failed with HTTP {state_response.status}.")
        state = await state_response.json()
        jobs = state.get("jobs") if isinstance(state, dict) else []
        job = next(
            (item for item in jobs if isinstance(item, dict) and item.get("jobId") == job_id),
            None,
        )
        if job and job.get("status") in TERMINAL_QUERY_STATUSES:
            break
        await page.wait_for_timeout(500)
    if not job:
        raise RuntimeError(f"Timed out waiting for query job {job_id}.")
    if job.get("status") != "completed":
        raise RuntimeError(f"Query job failed for {notebook['notebook_id']}: {job!r}")
    rows = job.get("rows") if isinstance(job.get("rows"), list) else []
    if not rows:
        raise RuntimeError(f"Query job returned no rows: {job!r}")
    row = rows[0]
    expected_values = [
        int(expected["row_count"]),
        int(expected["min_year"]),
        int(expected["max_year"]),
        float(expected["total_due"]),
        float(expected["taxpayer_due"]),
    ]
    actual_values = [int(row[0]), int(row[1]), int(row[2]), float(row[3]), float(row[4])]
    for actual, expected_value in zip(actual_values[:3], expected_values[:3], strict=True):
        if actual != expected_value:
            raise RuntimeError(
                f"Unexpected integer query result for {notebook['notebook_id']}: "
                f"{actual_values!r} != {expected_values!r}"
            )
    for actual, expected_value in zip(actual_values[3:], expected_values[3:], strict=True):
        if abs(actual - expected_value) > 0.01:
            raise RuntimeError(
                f"Unexpected tax value for {notebook['notebook_id']}: "
                f"{actual_values!r} != {expected_values!r}"
            )


async def run_smoke(args: argparse.Namespace) -> int:
    sample_path = Path(args.sample_path)
    csv_bytes, expected = federal_tax_csv(sample_path)
    unique_id = uuid4().hex[:8]
    root_prefix = f"{args.s3_smoke_prefix_root.strip('/')}/{unique_id}"
    client = s3_client(args)
    ensure_bucket(client, args.bucket)
    delete_s3_prefix(client, args.bucket, root_prefix)
    console_messages: list[str] = []
    network_responses: list[tuple[str, str, int]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))
        page.on(
            "response",
            lambda resp: network_responses.append((resp.request.method, resp.url, resp.status)),
        )

        try:
            uploaded_cases = []
            notebooks = []
            for case in CASES:
                uploaded = await upload_case(page, args, case, unique_id, csv_bytes)
                assert_uploaded_layout(client, args.bucket, uploaded)
                uploaded_cases.append(uploaded)
                notebook = await create_notebook_for_case(page, args, uploaded, unique_id)
                await verify_notebook_option_ui(page, args, notebook)
                notebooks.append(notebook)
            for notebook in notebooks:
                await run_query_job(page, args, notebook, expected)
        except (ClientError, PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for method, url, status in network_responses:
                if "/api/ingestion" in url or "/api/query-jobs" in url or "/api/notebooks/shared" in url:
                    print(f"HTTP {method} {status} {url}", file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1
        finally:
            await browser.close()

    print(
        "Playwright Parquet optimization performance-options regression passed "
        f"for id {unique_id}. S3 prefix: s3://{args.bucket}/{root_prefix}/"
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

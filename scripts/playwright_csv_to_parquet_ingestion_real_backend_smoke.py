from __future__ import annotations

import argparse
import asyncio
import contextlib
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
import duckdb
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise real browser CSV ingestion to Shared Workspace S3 as Parquet, "
            "including a late string value that used to break DuckDB CSV type inference."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="vat-smoke-test")
    parser.add_argument("--s3-smoke-prefix-root", default="playwright/csv-to-parquet")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=60000)
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


def delete_s3_prefix(client, bucket: str, prefix: str) -> None:
    normalized_prefix = "/".join(segment for segment in prefix.split("/") if segment)
    if not normalized_prefix:
        return
    list_prefix = f"{normalized_prefix}/"
    while True:
        response = client.list_objects_v2(Bucket=bucket, Prefix=list_prefix)
        objects = [{"Key": item["Key"]} for item in response.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        if not response.get("IsTruncated"):
            return


def build_type_drift_csv() -> bytes:
    rows = ["id,DOCO_AuslPositionStatistik\n"]
    rows.extend(f"{index},{index}\n" for index in range(25_000))
    rows.append("25000,H\n")
    return "".join(rows).encode("utf-8")


def verify_parquet_object(client, bucket: str, key: str) -> None:
    parquet_bytes = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if parquet_bytes[:4] != b"PAR1":
        raise RuntimeError(f"S3 object {key!r} is not a Parquet file.")
    with TemporaryDirectory() as temp_dir:
        parquet_path = Path(temp_dir) / "KBPO2020.parquet"
        parquet_path.write_bytes(parquet_bytes)
        connection = duckdb.connect(":memory:")
        try:
            row_count = connection.execute(
                f"SELECT count(*) FROM read_parquet('{parquet_path.as_posix()}')"
            ).fetchone()[0]
            column_type = connection.execute(
                f"""
                DESCRIBE SELECT DOCO_AuslPositionStatistik
                FROM read_parquet('{parquet_path.as_posix()}')
                """
            ).fetchone()[1]
            late_value = connection.execute(
                f"""
                SELECT DOCO_AuslPositionStatistik
                FROM read_parquet('{parquet_path.as_posix()}')
                WHERE CAST(id AS VARCHAR) = '25000'
                """
            ).fetchone()[0]
        finally:
            connection.close()
    if row_count != 25_001:
        raise RuntimeError(f"Unexpected Parquet row count: {row_count!r}.")
    if column_type != "VARCHAR":
        raise RuntimeError(f"Expected drifted Parquet column to be VARCHAR, got {column_type!r}.")
    if late_value != "H":
        raise RuntimeError(f"Expected late drift value 'H', got {late_value!r}.")


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


async def assert_success_dialog(page, timeout_ms: int, expected_fragments: list[str]) -> None:
    dialog = page.locator("[data-message-dialog]")
    await dialog.wait_for(state="visible", timeout=timeout_ms)
    dialog_text = (await dialog.text_content() or "").strip()
    missing = [fragment for fragment in expected_fragments if fragment not in dialog_text]
    if missing:
        raise RuntimeError(f"Success dialog is missing {missing!r}: {dialog_text!r}")
    await page.locator("[data-message-submit]").click()
    await dialog.wait_for(state="hidden", timeout=timeout_ms)


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


async def import_csv_to_parquet_s3(page, args: argparse.Namespace, unique_id: str) -> tuple[str, str]:
    prefix = f"{args.s3_smoke_prefix_root.strip('/')}/{unique_id}"
    expected_key = f"{prefix}/KBPO2020.parquet"
    await open_csv_ingestion(page, args.base_url, args.timeout_ms)
    await page.locator('[data-csv-target-option][value="workspace.s3"]').check()
    await page.locator('[data-csv-config-panel="workspace.s3"] [data-csv-s3-bucket]').fill(args.bucket)
    await page.locator('[data-csv-config-panel="workspace.s3"] [data-csv-s3-prefix]').fill(prefix)
    await page.locator('[data-csv-s3-storage-format][value="parquet"]').check()
    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": "KBPO2020.csv",
                "mimeType": "text/csv",
                "buffer": build_type_drift_csv(),
            }
        ]
    )
    preview = page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").first
    await preview.wait_for(state="visible", timeout=args.timeout_ms)
    preview_text = (await preview.text_content() or "").strip()
    if "KBPO2020.csv" not in preview_text:
        raise RuntimeError(f"CSV preview is missing the uploaded file name: {preview_text!r}")

    payload = await expect_complete_payload(
        page,
        args.base_url,
        args.timeout_ms,
        page.locator("[data-csv-import-submit]"),
    )
    if payload.get("targetId") != "workspace.s3":
        raise RuntimeError(f"Unexpected S3 target in completion payload: {payload!r}")
    if payload.get("importedCount") != 1 or payload.get("failedCount") != 0:
        raise RuntimeError(f"Unexpected CSV-to-Parquet import counts: {payload!r}")
    imports = payload.get("imports") if isinstance(payload.get("imports"), list) else []
    actual_key = imports[0].get("objectKey") if imports and isinstance(imports[0], dict) else ""
    if actual_key != expected_key:
        raise RuntimeError(f"Unexpected imported object key: {actual_key!r} != {expected_key!r}.")

    result_list = page.locator("[data-csv-result-list]")
    await result_list.locator(".ingestion-csv-result-card-imported").first.wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    result_text = (await result_list.text_content() or "").strip()
    missing = [fragment for fragment in ("KBPO2020.parquet", "Parquet", prefix) if fragment not in result_text]
    if missing:
        raise RuntimeError(f"CSV-to-Parquet result UI is missing {missing!r}: {result_text!r}")
    await assert_success_dialog(
        page,
        args.timeout_ms,
        ["CSV import finished", "1 file(s) processed", "Shared Workspace S3"],
    )
    return prefix, expected_key


async def run_smoke(args: argparse.Namespace) -> int:
    unique_id = uuid4().hex[:8]
    client = s3_client(args)
    ensure_bucket(client, args.bucket)
    prefix = f"{args.s3_smoke_prefix_root.strip('/')}/{unique_id}"
    delete_s3_prefix(client, args.bucket, prefix)
    console_messages: list[str] = []
    responses: list[tuple[str, str, int]] = []

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
            lambda resp: responses.append((resp.request.method, resp.url, resp.status)),
        )

        try:
            prefix, parquet_key = await import_csv_to_parquet_s3(page, args, unique_id)
            verify_parquet_object(client, args.bucket, parquet_key)
        except (ClientError, PlaywrightTimeoutError, RuntimeError, duckdb.Error) as exc:
            print(str(exc), file=sys.stderr)
            for method, url, status in responses:
                if "/api/ingestion/csv/upload-sessions" in url:
                    print(f"HTTP {method} {status} {url}", file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1
        finally:
            await browser.close()
            with contextlib.suppress(Exception):
                delete_s3_prefix(client, args.bucket, prefix)

    print(f"Playwright CSV-to-Parquet real backend smoke passed for id {unique_id}.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

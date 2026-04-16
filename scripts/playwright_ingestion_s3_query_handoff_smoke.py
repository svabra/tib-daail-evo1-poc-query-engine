from __future__ import annotations

import argparse
import asyncio
import contextlib
import sys
from uuid import uuid4

import boto3
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the CSV import to Shared Workspace S3 handoff into the "
            "Query Workbench. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="vat-smoke-test")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=25000)
    return parser.parse_args()


def s3_client(args: argparse.Namespace):
    return boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret_key,
    )


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
    await page.locator('[data-ingestion-tile="csv"]').click()
    await page.locator("[data-csv-ingestion-form]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def import_csv_to_s3(page, args: argparse.Namespace) -> tuple[str, str]:
    unique_id = uuid4().hex[:10]
    file_name = f"playwright-s3-handoff-{unique_id}.csv"
    object_base_name = f"playwright-renamed-{unique_id}"
    prefix = f"playwright/csv-imports/{unique_id}"

    await page.locator('[data-csv-target-option][value="workspace.s3"]').check()
    await page.locator('[data-csv-config-panel="workspace.s3"] [data-csv-s3-bucket]').fill(args.bucket)
    await page.locator('[data-csv-config-panel="workspace.s3"] [data-csv-s3-prefix]').fill(prefix)
    await page.locator('[data-csv-s3-storage-format][value="json"]').check()
    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": file_name,
                "mimeType": "text/csv",
                "buffer": (
                    b"record_id,canton_code,tax_office,tax_type,assessed_amount_chf\n"
                    b"1,ZH,Zurich Central Tax Office,VAT,1200.50\n"
                    b"2,BE,Bern Regional Tax Office,INCOME_TAX,918.25\n"
                ),
            }
        ]
    )

    await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    await page.locator("[data-csv-import-base-name]").first.fill(object_base_name)
    await page.locator("[data-csv-import-base-name]").first.evaluate("node => node.blur()")
    review_card = page.locator("[data-csv-review-list] .ingestion-csv-review-card").first
    review_copy = (await review_card.text_content() or "").strip()
    if "s3://" in review_copy:
        raise RuntimeError(f"S3 review card should not render a path-like URI anymore: {review_copy!r}")
    if "Key prefix" not in review_copy or prefix not in review_copy:
        raise RuntimeError(f"S3 review card does not show the key prefix explicitly: {review_copy!r}")
    if "Object name" not in review_copy or f"{object_base_name}.jsonl" not in review_copy:
        raise RuntimeError(f"S3 review card does not show the object name explicitly: {review_copy!r}")
    if "stored as JSONL" not in review_copy:
        raise RuntimeError(f"Expected JSONL storage copy, got: {review_copy!r}")

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/ingestion/csv/import"),
        timeout=args.timeout_ms,
    ) as response_info:
        await page.locator("[data-csv-import-submit]").click()

    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"S3 CSV import failed with status {response.status}.")

    await page.locator("[data-csv-result-list] .ingestion-csv-result-card-imported").first.wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    message_dialog = page.locator("[data-message-dialog]")
    if await message_dialog.is_visible():
        await page.locator("[data-message-submit]").click()
        await message_dialog.wait_for(state="hidden", timeout=args.timeout_ms)

    query_button = page.locator("[data-csv-import-open-query]").first
    await query_button.wait_for(state="visible", timeout=args.timeout_ms)
    relation = (await query_button.get_attribute("data-csv-query-source-relation") or "").strip()
    if not relation:
        raise RuntimeError("S3 import result did not expose a query relation.")

    result_copy = (
        await page.locator(".ingestion-csv-result-card-imported").first.text_content() or ""
    ).strip()
    if "stored as JSONL" not in result_copy:
        raise RuntimeError(f"Expected JSONL storage copy, got: {result_copy!r}")
    if "s3://" in result_copy:
        raise RuntimeError(f"S3 result card should not render a path-like URI anymore: {result_copy!r}")
    if "Key prefix" not in result_copy or prefix not in result_copy:
        raise RuntimeError(f"Expected explicit key prefix in result copy, got: {result_copy!r}")
    if "Object name" not in result_copy or f"{object_base_name}.jsonl" not in result_copy:
        raise RuntimeError(f"Expected explicit object name in result copy, got: {result_copy!r}")

    await query_button.click()
    return relation, f"{prefix}/{object_base_name}.jsonl"


async def assert_query_handoff(page, expected_relation: str, timeout_ms: int) -> None:
    await page.locator("[data-workspace-notebook]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    if not page.url.rstrip("/").endswith("/query-workbench"):
        raise RuntimeError(f"Expected Query Workbench URL after handoff, got {page.url}.")

    selected_source = page.locator(
        f'[data-source-object].is-selected[data-source-option-id="workspace.s3"][data-source-object-relation="{expected_relation}"]'
    )
    await selected_source.wait_for(state="visible", timeout=timeout_ms)

    editor = page.locator("[data-query-cell] [data-editor-source]").first
    sql_text = (await editor.input_value()).strip()
    if "record_id" not in sql_text or "canton_code" not in sql_text:
        raise RuntimeError(
            "The new notebook SQL does not reference the real CSV header names."
        )
    if "column00" in sql_text or "column01" in sql_text:
        raise RuntimeError(
            "The new notebook SQL still references synthetic CSV column names."
        )


async def run_smoke(args: argparse.Namespace) -> int:
    client = s3_client(args)
    uploaded_key = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        console_messages: list[str] = []
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await open_csv_ingestion(page, args.base_url, args.timeout_ms)
            relation, uploaded_key = await import_csv_to_s3(page, args)
            await assert_query_handoff(page, relation, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1
        finally:
            await browser.close()
            if uploaded_key:
                with contextlib.suppress(Exception):
                    client.delete_object(Bucket=args.bucket, Key=uploaded_key)

    print("Playwright ingestion S3 query handoff smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

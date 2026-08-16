from __future__ import annotations

import argparse
import asyncio
import contextlib
import re
import sys
from uuid import uuid4

import boto3
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the CSV import to S3 Object Storage handoff into the "
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


def normalize_alias_segment(value: str, fallback: str = "item") -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        normalized = fallback
    if normalized[0].isdigit():
        normalized = f"n_{normalized}"
    return normalized


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
    await page.wait_for_timeout(1000)
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


async def import_csv_to_s3(page, args: argparse.Namespace) -> tuple[str, str, str, str]:
    unique_id = uuid4().hex[:10]
    file_name = f"playwright-s3-handoff-{unique_id}.csv"
    object_base_name = f"playwright-renamed-{unique_id}"
    prefix = f"playwright/csv-imports/{unique_id}"
    expected_s3_uri = f"s3://{args.bucket}/{prefix}/{object_base_name}.jsonl"
    expected_query_reference = (
        f's3."{args.bucket}"."{prefix}/{object_base_name}.jsonl"'
    )
    expected_alias = ".".join(
        [
            "s3",
            normalize_alias_segment(args.bucket, "bucket"),
            "playwright",
            "csv_imports",
            normalize_alias_segment(unique_id, "folder"),
            normalize_alias_segment(object_base_name, "s3_object"),
            "jsonl",
        ]
    )

    await page.locator('[data-csv-target-option][value="s3"]').check()
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
    bucket_input = page.locator('[data-csv-config-panel="s3"] [data-csv-s3-bucket]')
    await page.context.grant_permissions(
        ["clipboard-read", "clipboard-write"],
        origin=args.base_url.rstrip("/"),
    )
    await bucket_input.fill(args.bucket)
    await page.evaluate(
        "value => navigator.clipboard.writeText(value)",
        "s3://ab/path/object.csv",
    )
    await bucket_input.focus()
    await page.keyboard.press("Control+V")
    invalid_aria = await bucket_input.get_attribute("aria-invalid")
    if invalid_aria != "true":
        invalid_status = (
            await page.locator("[data-csv-s3-uri-paste-status]").text_content() or ""
        ).strip()
        raise RuntimeError(
            "An invalid pasted S3 URI did not mark the Bucket field invalid: "
            f"value={await bucket_input.input_value()!r}, aria-invalid={invalid_aria!r}, "
            f"status={invalid_status!r}."
        )
    if not await page.locator("[data-csv-import-submit]").is_disabled():
        raise RuntimeError("An invalid pasted S3 URI did not block the import action.")

    await page.evaluate(
        "value => navigator.clipboard.writeText(value)",
        expected_s3_uri,
    )
    await bucket_input.focus()
    await page.keyboard.press("Control+V")
    split_state = {
        "bucket": await page.locator("[data-csv-s3-bucket]").input_value(),
        "prefix": await page.locator("[data-csv-s3-prefix]").input_value(),
        "objectName": await page.locator("[data-csv-import-base-name]").first.input_value(),
        "storageFormat": await page.locator(
            "[data-csv-s3-storage-format]:checked"
        ).get_attribute("value"),
    }
    expected_split_state = {
        "bucket": args.bucket,
        "prefix": prefix,
        "objectName": object_base_name,
        "storageFormat": "json",
    }
    if split_state != expected_split_state:
        raise RuntimeError(
            f"Full S3 URI was not split correctly: {split_state!r} != {expected_split_state!r}"
        )
    paste_status = (
        await page.locator("[data-csv-s3-uri-paste-status]").text_content() or ""
    ).strip()
    if "split into Bucket, Object key prefix, and Object name" not in paste_status:
        raise RuntimeError(f"Expected successful S3 URI split feedback, got: {paste_status!r}")
    review_card = page.locator("[data-csv-review-list] .ingestion-csv-review-card").first
    review_copy = (await review_card.text_content() or "").strip()
    review_s3_uri = (
        await review_card.locator("[data-csv-s3-summary-uri]").text_content() or ""
    ).strip()
    if "Full S3 URI" not in review_copy or review_s3_uri != expected_s3_uri:
        raise RuntimeError(
            "S3 review card does not show the complete object URI: "
            f"expected {expected_s3_uri!r}, got {review_s3_uri!r}"
        )
    if "Key prefix" not in review_copy or prefix not in review_copy:
        raise RuntimeError(f"S3 review card does not show the key prefix explicitly: {review_copy!r}")
    if "Object name" not in review_copy or f"{object_base_name}.jsonl" not in review_copy:
        raise RuntimeError(f"S3 review card does not show the object name explicitly: {review_copy!r}")
    if "stored as JSONL" not in review_copy:
        raise RuntimeError(f"Expected JSONL storage copy, got: {review_copy!r}")

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and "/api/ingestion/csv/upload-sessions/" in response.url
        and response.url.endswith("/complete"),
        timeout=args.timeout_ms,
    ) as response_info:
        await page.locator("[data-csv-import-submit]").click()

    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"S3 CSV import failed with status {response.status}.")

    result_card = page.locator(
        "[data-csv-result-list] .ingestion-csv-result-card-imported"
    ).first
    await result_card.wait_for(
        state="visible",
        timeout=max(args.timeout_ms, 90_000),
    )
    message_dialog = page.locator("[data-message-dialog]")
    if await message_dialog.is_visible():
        await page.locator("[data-message-submit]").click()
        await message_dialog.wait_for(state="hidden", timeout=args.timeout_ms)

    query_button = page.locator("[data-csv-import-open-query]").first
    await query_button.wait_for(state="visible", timeout=args.timeout_ms)
    unavailable_note = page.locator("[data-csv-result-query-note]").first
    if await unavailable_note.count() and await unavailable_note.is_visible():
        note_copy = (await unavailable_note.text_content() or "").strip()
        raise RuntimeError(
            "S3 import reported success before the uploaded object was queryable: "
            f"{note_copy!r}"
        )
    relation = (await query_button.get_attribute("data-csv-query-source-relation") or "").strip()
    if not relation:
        raise RuntimeError("S3 import result did not expose a query relation.")
    if re.fullmatch(r"[a-z][a-z0-9]*", args.bucket) and not relation.startswith(
        f"{args.bucket}."
    ):
        raise RuntimeError(
            "Simple S3 bucket names should remain readable in generated SQL. "
            f"Expected relation to start with {args.bucket!r}, got {relation!r}."
        )

    result_copy = (await result_card.text_content() or "").strip()
    if "stored as JSONL" not in result_copy:
        raise RuntimeError(f"Expected JSONL storage copy, got: {result_copy!r}")
    result_s3_uri = (
        await result_card.locator("[data-csv-s3-summary-uri]").text_content() or ""
    ).strip()
    if "Full S3 URI" not in result_copy or result_s3_uri != expected_s3_uri:
        raise RuntimeError(
            "S3 result card does not show the complete object URI: "
            f"expected {expected_s3_uri!r}, got {result_s3_uri!r}"
        )
    if "Key prefix" not in result_copy or prefix not in result_copy:
        raise RuntimeError(f"Expected explicit key prefix in result copy, got: {result_copy!r}")
    if "Object name" not in result_copy or f"{object_base_name}.jsonl" not in result_copy:
        raise RuntimeError(f"Expected explicit object name in result copy, got: {result_copy!r}")

    await query_button.click()
    return (
        relation,
        f"{prefix}/{object_base_name}.jsonl",
        expected_alias,
        expected_query_reference,
    )


async def assert_query_handoff(
    page,
    expected_relation: str,
    expected_alias: str,
    expected_query_reference: str,
    timeout_ms: int,
) -> None:
    await page.locator("[data-workspace-notebook]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    if not page.url.rstrip("/").endswith("/query-workbench"):
        raise RuntimeError(f"Expected Query Workbench URL after handoff, got {page.url}.")

    selected_source = page.locator(
        f'[data-source-object].is-selected[data-source-option-id="s3"][data-source-object-relation="{expected_relation}"]'
    )
    await selected_source.wait_for(state="visible", timeout=timeout_ms)
    actual_alias = (await selected_source.get_attribute("data-source-object-query-alias") or "").strip()
    if actual_alias != expected_alias:
        raise RuntimeError(f"Expected S3 query alias {expected_alias!r}, got {actual_alias!r}.")
    actual_query_reference = (
        await selected_source.get_attribute("data-source-object-query-reference") or ""
    ).strip()
    if actual_query_reference != expected_query_reference:
        raise RuntimeError(
            "Expected canonical S3 query reference "
            f"{expected_query_reference!r}, got {actual_query_reference!r}."
        )

    editor = page.locator("[data-query-cell] [data-editor-source]").first
    sql_text = (await editor.input_value()).strip()
    if expected_query_reference not in sql_text:
        raise RuntimeError(
            f"The new notebook SQL does not use the canonical S3 query reference: {sql_text!r}."
        )
    if expected_relation in sql_text:
        raise RuntimeError(f"The new notebook SQL still exposes the physical S3 relation: {sql_text!r}.")
    if "record_id" not in sql_text or "canton_code" not in sql_text:
        raise RuntimeError(
            "The new notebook SQL does not reference the real CSV header names."
        )
    if "column00" in sql_text or "column01" in sql_text:
        raise RuntimeError(
            "The new notebook SQL still references synthetic CSV column names."
        )

    await page.locator("[data-query-cell]:visible [data-run-cell]").first.click()
    result_root = page.locator("[data-query-cell]:visible [data-cell-result]").first
    await result_root.wait_for(state="visible", timeout=timeout_ms)
    await result_root.get_by_text("Zurich Central Tax Office").first.wait_for(
        state="visible",
        timeout=timeout_ms,
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
            relation, uploaded_key, expected_alias, expected_query_reference = (
                await import_csv_to_s3(page, args)
            )
            await assert_query_handoff(
                page,
                relation,
                expected_alias,
                expected_query_reference,
                args.timeout_ms,
            )
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

from __future__ import annotations

import argparse
import asyncio
import io
import json
import sys
import zipfile

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the CSV-first ingestion landing page in the browser "
            "using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


async def open_ingestion_workbench(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/ingestion-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator('[data-ingestion-tile="csv"]').wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator('[data-ingestion-entry-panel="csv"]').wait_for(
        state="hidden",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(1000)


async def open_csv_ingestor(page, timeout_ms: int) -> None:
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
    await page.locator('[data-csv-target-option][value="workspace.s3"]').check()
    for value in ("csv", "parquet", "json"):
        await page.locator(f'[data-csv-s3-storage-format][value="{value}"]').wait_for(
            state="visible",
            timeout=timeout_ms,
        )
    json_guidance = (
        await page.locator('[aria-label="JSON format guidance"]').get_attribute("title")
        or ""
    )
    if "line-delimited JSON / JSONL" not in json_guidance:
        raise RuntimeError("Expected JSON guidance to explain line-delimited JSON / JSONL.")
    duckdb_guidance = (
        await page.locator('[aria-label="Shared Workspace storage format guidance"]').get_attribute("title")
        or ""
    )
    if "DuckDB remains the query engine" not in duckdb_guidance:
        raise RuntimeError("Expected Shared Workspace storage format guidance to mention DuckDB.")
    prefix_guidance = (
        await page.locator('[aria-label="Object key prefix guidance"]').get_attribute("title")
        or ""
    )
    if "literal S3 key text" not in prefix_guidance:
        raise RuntimeError("Expected object key prefix guidance to explain S3 prefix semantics.")
    await page.locator('[data-csv-target-option][value="workspace.local"]').check()


async def assert_ingestion_returns_to_landing_after_navigation(page, timeout_ms: int) -> None:
    await open_csv_ingestor(page, timeout_ms)

    await page.locator(
        '[data-open-query-workbench][data-open-query-workbench-navigation="true"]'
    ).click()
    await page.locator("[data-ingestion-workbench-page]").wait_for(state="hidden", timeout=timeout_ms)

    await page.locator("[data-open-ingestion-workbench]").click()
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator('[data-ingestion-entry-panel="csv"]').wait_for(
        state="hidden",
        timeout=timeout_ms,
    )


async def reject_invalid_csv_file(page, timeout_ms: int) -> None:
    file_input = page.locator("[data-csv-file-input]").first
    await file_input.set_input_files(
        files=[
            {
                "name": "playwright-invalid.csv",
                "mimeType": "text/csv",
                "buffer": b"id,name,amount\n1,alpha,9,536.31\n",
            }
        ]
    )

    preview_error = page.locator(
        "[data-csv-preview-root] .ingestion-csv-preview-card-error"
    ).first
    await preview_error.wait_for(state="visible", timeout=timeout_ms)
    error_text = (await preview_error.text_content() or "").strip()
    if "CSV row width mismatch at line 2" not in error_text:
        raise RuntimeError(f"Expected CSV validation error, got: {error_text!r}")

    import_button = page.locator("[data-csv-import-submit]").first
    if await import_button.is_enabled():
        raise RuntimeError("Invalid CSV preview must keep the import button disabled.")


async def import_local_csv_file(page, timeout_ms: int) -> None:
    file_input = page.locator("[data-csv-file-input]").first
    await file_input.set_input_files(
        files=[
            {
                "name": "playwright-sample.csv",
                "mimeType": "text/csv",
                "buffer": b"id,name\n1,alpha\n2,beta\n",
            }
        ]
    )

    await page.locator("[data-csv-review-list] .ingestion-csv-review-card").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )

    import_button = page.locator("[data-csv-import-submit]").first
    await import_button.wait_for(state="visible", timeout=timeout_ms)
    await import_button.click()

    await page.locator("[data-csv-result-list] .ingestion-csv-result-card-imported").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    message_dialog = page.locator("[data-message-dialog]")
    if await message_dialog.is_visible():
        await page.locator("[data-message-submit]").click()
        await message_dialog.wait_for(state="hidden", timeout=timeout_ms)

    query_button = page.locator("[data-csv-import-open-query]").first
    await query_button.wait_for(state="visible", timeout=timeout_ms)


def zip_bytes(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)
    return buffer.getvalue()


async def import_local_zip_file(page, timeout_ms: int) -> None:
    file_input = page.locator("[data-csv-file-input]").first
    await file_input.set_input_files(
        files=[
            {
                "name": "playwright-local-archive.zip",
                "mimeType": "application/zip",
                "buffer": zip_bytes(
                    {
                        "alpha.csv": b"id,name\n1,alpha\n",
                        "nested/beta.csv": b"id,name\n2,beta\n",
                    }
                ),
            }
        ]
    )

    preview_card = page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").first
    await preview_card.wait_for(state="visible", timeout=timeout_ms)
    preview_text = (await preview_card.text_content() or "").strip()
    if "2 CSV file(s)" not in preview_text:
        raise RuntimeError(f"Expected ZIP preview to report two CSV files, got: {preview_text!r}")

    await page.locator("[data-csv-import-submit]").click()
    await page.locator("[data-csv-result-list] .ingestion-csv-result-card-imported").nth(1).wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    message_dialog = page.locator("[data-message-dialog]")
    if await message_dialog.is_visible():
        await page.locator("[data-message-submit]").click()
        await message_dialog.wait_for(state="hidden", timeout=timeout_ms)


async def import_server_csv_with_progress(page, timeout_ms: int) -> None:
    upload_state = {
        "sessionId": "playwright-session",
        "chunkSizeBytes": 8,
        "files": [
            {
                "fileId": "playwright-file",
                "fileName": "playwright-progress.csv",
                "sizeBytes": 23,
                "receivedBytes": 0,
                "complete": False,
            }
        ],
    }

    async def handle_create(route):
        await route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(upload_state),
        )

    async def handle_session(route):
        request = route.request
        await asyncio.sleep(0.2)
        if request.method == "PUT":
            chunk_index = int(request.url.rstrip("/").rsplit("/", 1)[-1])
            received = min(23, (chunk_index + 1) * 8)
            upload_state["files"][0]["receivedBytes"] = received
            upload_state["files"][0]["complete"] = received == 23
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(upload_state),
            )
            return
        if request.method == "POST" and request.url.endswith("/complete"):
            payload = json.loads(request.post_data or "{}")
            if payload.get("targetId") != "workspace.s3" or payload.get("storageFormat") != "parquet":
                await route.fulfill(
                    status=400,
                    content_type="application/json",
                    body=json.dumps({"detail": f"Unexpected completion payload: {payload!r}"}),
                )
                return
            await asyncio.sleep(0.5)
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "targetId": "workspace.s3",
                        "importedCount": 1,
                        "failedCount": 0,
                        "imports": [
                            {
                                "fileName": "playwright-progress.csv",
                                "storedFileName": "playwright-progress.parquet",
                                "status": "imported",
                                "destination": "workspace.s3",
                                "bucket": "playwright-progress-bucket",
                                "objectKey": "playwright-progress.parquet",
                                "storageFormat": "parquet",
                                "rowCount": 2,
                            }
                        ],
                    }
                ),
            )
            return
        await route.fulfill(status=200, content_type="application/json", body="{}")

    await page.route("**/api/ingestion/csv/upload-sessions", handle_create)
    await page.route("**/api/ingestion/csv/upload-sessions/**", handle_session)
    await page.locator('[data-csv-target-option][value="workspace.s3"]').check()
    await page.locator("[data-csv-s3-bucket]").fill("playwright-progress-bucket")
    await page.locator('[data-csv-s3-storage-format][value="parquet"]').check()
    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": "playwright-progress.csv",
                "mimeType": "text/csv",
                "buffer": b"id,name\n1,alpha\n2,beta\n",
            }
        ]
    )
    await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").wait_for(
        state="visible",
        timeout=timeout_ms,
    )

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/complete"),
        timeout=timeout_ms,
    ):
        await page.locator("[data-csv-import-submit]").click()
        progress = page.locator("[data-csv-upload-progress]").first
        await progress.wait_for(state="visible", timeout=timeout_ms)
        progress_text = (await progress.text_content() or "").strip()
        if "Uploading" not in progress_text or "MB" not in progress_text:
            raise RuntimeError(f"Expected upload progress next to the import button, got: {progress_text!r}")
        if "Step 1 of 2" not in progress_text:
            raise RuntimeError(f"Expected upload progress to identify step 1, got: {progress_text!r}")
        await page.wait_for_function(
            """() => {
                const text = document.querySelector("[data-csv-upload-progress]")?.textContent || "";
                return text.includes("Processing")
                    && text.includes("Step 2 of 2")
                    && text.includes("Upload complete")
                    && text.includes("Converting CSV to Parquet");
            }""",
            timeout=timeout_ms,
        )

    await page.locator("[data-csv-result-list] .ingestion-csv-result-card-imported").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.unroute("**/api/ingestion/csv/upload-sessions", handle_create)
    await page.unroute("**/api/ingestion/csv/upload-sessions/**", handle_session)
    message_dialog = page.locator("[data-message-dialog]")
    if await message_dialog.is_visible():
        await page.locator("[data-message-submit]").click()
        await message_dialog.wait_for(state="hidden", timeout=timeout_ms)


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        console_messages: list[str] = []
        responses: list[tuple[str, str, int]] = []
        page.on(
            "console",
            lambda msg: console_messages.append(
                f"console:{msg.type}:{msg.text}"
            ),
        )
        page.on(
            "pageerror",
            lambda exc: console_messages.append(f"pageerror:{exc}"),
        )
        page.on(
            "response",
            lambda resp: responses.append(
                (resp.request.method, resp.url, resp.status)
            ),
        )

        try:
            await open_ingestion_workbench(page, args.base_url, args.timeout_ms)
            await assert_ingestion_returns_to_landing_after_navigation(page, args.timeout_ms)
            await open_csv_ingestor(page, args.timeout_ms)
            await reject_invalid_csv_file(page, args.timeout_ms)
            await import_local_csv_file(page, args.timeout_ms)
            await import_local_zip_file(page, args.timeout_ms)
            await import_server_csv_with_progress(page, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for method, url, status in responses:
                if "/api/ingestion/csv/import" in url or "/api/ingestion/csv/upload-sessions" in url:
                    print(f"HTTP {method} {status} {url}", file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright ingestion workbench smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

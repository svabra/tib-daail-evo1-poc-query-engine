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
    await page.locator('[data-csv-target-option][value="s3"]').check()
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
        await page.locator('#csv-ingestion-panel [aria-label="Object key prefix guidance"]').get_attribute("title")
        or ""
    )
    if "literal S3 key text" not in prefix_guidance:
        raise RuntimeError("Expected object key prefix guidance to explain S3 prefix semantics.")
    await page.locator('[data-csv-target-option][value="workspace.local"]').check()


async def assert_ingestion_tile_copy(page, timeout_ms: int) -> None:
    expected_copy = {
        "parquet": "Import direct Parquet files or ZIP archives containing Parquet files.",
        "json": "Import JSON, JSONL, NDJSON, or ZIP archives.",
        "xlsx": "Import XLSX files or ZIP archives containing XLSX files using the active worksheet.",
        "xml": "Import simple table-like XML files or ZIP archives containing XML files.",
    }
    for ingestor_id, expected in expected_copy.items():
        tile = page.locator(f'[data-ingestion-tile="{ingestor_id}"]').first
        await tile.wait_for(state="visible", timeout=timeout_ms)
        tile_text = (await tile.text_content() or "").strip()
        if "built-in method copy" in tile_text:
            raise RuntimeError(f"{ingestor_id} tile rendered a Python dict method: {tile_text!r}")
        if expected not in tile_text:
            raise RuntimeError(
                f"{ingestor_id} tile copy is missing expected text {expected!r}: {tile_text!r}"
            )


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
    await page.reload(wait_until="domcontentloaded")
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(500)


async def assert_ingestion_state_survives_out_of_order_back_forward(
    page, timeout_ms: int
) -> None:
    await open_csv_ingestor(page, timeout_ms)
    file_input = page.locator("[data-csv-file-input]").first
    await file_input.set_input_files(
        files=[
            {
                "name": "playwright-history.csv",
                "mimeType": "text/csv",
                "buffer": b"canton;amount\nAG;42\n",
            }
        ]
    )
    await page.locator("[data-csv-delimiter-mode]").select_option("semicolon")
    await page.locator('[data-csv-target-option][value="s3"]').check()
    await page.locator("[data-csv-s3-bucket]").fill("playwright-history")
    prefix = page.locator("[data-csv-s3-prefix]")
    await prefix.fill("manual/aargau/")
    await prefix.scroll_into_view_if_needed()
    await prefix.focus()
    scroll_before = await page.evaluate(
        """
        () => {
          const input = document.querySelector('[data-csv-s3-prefix]');
          window.scrollTo(0, Math.max(0, document.documentElement.scrollHeight - window.innerHeight));
          input?.scrollIntoView({ block: 'center' });
          input?.focus();
          return {
            scrollY: window.scrollY,
            prefixTop: input?.getBoundingClientRect().top ?? null,
          };
        }
        """
    )
    await page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__pwHistoryFetch = originalFetch;
          window.__pwLateNotebookStarted = false;
          window.__pwLateNotebookReturned = false;
          window.__pwForwardIngestionReturned = false;
          window.fetch = async (input, init = {}) => {
            const raw = typeof input === 'string' ? input : input?.url || '';
            const url = new URL(raw, location.origin);
            if (
              url.pathname === '/notebooks/data-analysts-journey-cantonal-business-tax' &&
              init?.headers?.['HX-Request'] === 'true'
            ) {
              window.__pwLateNotebookStarted = true;
              const response = await originalFetch(raw, { ...init, signal: undefined });
              await new Promise((resolve) => window.setTimeout(resolve, 1200));
              window.__pwLateNotebookReturned = true;
              return response;
            }
            if (url.pathname === '/ingestion-workbench' && init?.headers?.['HX-Request'] === 'true') {
              const response = await originalFetch(input, init);
              window.__pwForwardIngestionReturned = true;
              return response;
            }
            return originalFetch(input, init);
          };
          history.replaceState({ pw: 'ingestion-origin' }, '', '/ingestion-workbench');
          history.pushState(
            { pw: 'notebook' },
            '',
            '/notebooks/data-analysts-journey-cantonal-business-tax'
          );
          history.pushState({ pw: 'ingestion-final' }, '', '/ingestion-workbench');
        }
        """
    )
    await page.go_back()
    await page.wait_for_function("window.__pwLateNotebookStarted === true", timeout=timeout_ms)
    await page.go_forward()
    await page.wait_for_function("window.__pwForwardIngestionReturned === true", timeout=timeout_ms)
    await page.wait_for_function("window.__pwLateNotebookReturned === true", timeout=timeout_ms)
    await page.wait_for_timeout(250)
    state = await page.evaluate(
        """
        () => ({
          pathname: location.pathname,
          historyState: history.state?.pw || '',
          fileName: document.querySelector('[data-csv-file-input]')?.files?.[0]?.name || '',
          delimiter: document.querySelector('[data-csv-delimiter-mode]')?.value || '',
          target: document.querySelector('[data-csv-target-option]:checked')?.value || '',
          bucket: document.querySelector('[data-csv-s3-bucket]')?.value || '',
          prefix: document.querySelector('[data-csv-s3-prefix]')?.value || '',
          focusPreserved: document.activeElement?.matches('[data-csv-s3-prefix]') === true,
          scrollY: window.scrollY,
          prefixTop: document.querySelector('[data-csv-s3-prefix]')?.getBoundingClientRect().top ?? null,
          notebookVisible: Boolean(document.querySelector('[data-notebook-meta]')),
        })
        """
    )
    await page.evaluate(
        """
        () => {
          if (window.__pwHistoryFetch) window.fetch = window.__pwHistoryFetch;
          delete window.__pwHistoryFetch;
        }
        """
    )
    expected = {
        "pathname": "/ingestion-workbench",
        "historyState": "ingestion-final",
        "fileName": "playwright-history.csv",
        "delimiter": "semicolon",
        "target": "s3",
        "bucket": "playwright-history",
        "prefix": "manual/aargau/",
        "focusPreserved": True,
        "notebookVisible": False,
    }
    scroll_after = state.pop("scrollY")
    prefix_after = state.pop("prefixTop")
    scroll_clamped_to_document = (
        abs(scroll_after - scroll_before["scrollY"]) <= 2
        or abs(prefix_after - scroll_before["prefixTop"]) <= 24
    )
    if state != expected or not scroll_clamped_to_document:
        raise RuntimeError(
            "Ingestion state changed after out-of-order Back/Forward: "
            f"{state!r}, scroll {scroll_after!r}, prefixTop {prefix_after!r} != "
            f"{expected!r}, scroll {scroll_before!r}"
        )
    await page.locator("[data-close-ingestion-entry]").first.click()
    await page.locator('[data-ingestion-entry-panel="csv"]').wait_for(
        state="hidden",
        timeout=timeout_ms,
    )
    await page.reload(wait_until="domcontentloaded")
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(500)


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


async def close_message_dialog(page, timeout_ms: int) -> None:
    message_dialog = page.locator("[data-message-dialog]")
    if await message_dialog.is_visible():
        await page.locator("[data-message-submit]").click()
        await message_dialog.wait_for(state="hidden", timeout=timeout_ms)


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
            if payload.get("targetId") != "s3" or payload.get("storageFormat") != "parquet":
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
                        "targetId": "s3",
                        "importedCount": 1,
                        "failedCount": 0,
                        "firstQuerySource": {
                            "sourceId": "s3",
                            "catalogName": "workspace",
                            "schemaName": "playwright_progress_bucket",
                            "schemaLabel": "playwright-progress-bucket",
                            "relation": "playwright_progress_bucket.playwright_progress",
                            "name": "playwright-progress.parquet",
                        },
                        "imports": [
                            {
                                "fileName": "playwright-progress.csv",
                                "storedFileName": "playwright-progress.parquet",
                                "status": "imported",
                                "destination": "s3",
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
    await page.locator('[data-csv-target-option][value="s3"]').check()
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
        progress_track = page.locator(".ingestion-csv-upload-progress-track").first
        await progress_track.wait_for(state="visible", timeout=timeout_ms)
        initial_track_width = await progress_track.evaluate(
            "(element) => Math.round(element.getBoundingClientRect().width)"
        )
        progress_text = (await progress.text_content() or "").strip()
        if "Uploading" not in progress_text or "MB" not in progress_text:
            raise RuntimeError(f"Expected upload progress next to the import button, got: {progress_text!r}")
        if "Step 1 of 2" not in progress_text:
            raise RuntimeError(f"Expected upload progress to identify step 1, got: {progress_text!r}")
        await page.wait_for_function(
            """() => {
                const text = document.querySelector("[data-csv-upload-progress]")?.textContent || "";
                return text.includes("chunk") && text.includes("/3");
            }""",
            timeout=timeout_ms,
        )
        upload_track_width = await progress_track.evaluate(
            "(element) => Math.round(element.getBoundingClientRect().width)"
        )
        if abs(upload_track_width - initial_track_width) > 1:
            raise RuntimeError(
                "Expected upload progress track width to stay fixed while upload text changes, "
                f"got {initial_track_width}px then {upload_track_width}px."
            )
        await page.wait_for_function(
            """() => {
                const text = document.querySelector("[data-csv-upload-progress]")?.textContent || "";
                return text.includes("Processing")
                    && text.includes("Step 2 of 2")
                    && text.includes("Upload complete")
                    && text.includes("Transforming file to match target data format");
            }""",
            timeout=timeout_ms,
        )
        processing_track_width = await progress_track.evaluate(
            "(element) => Math.round(element.getBoundingClientRect().width)"
        )
        if abs(processing_track_width - initial_track_width) > 1:
            raise RuntimeError(
                "Expected upload progress track width to stay fixed when processing begins, "
                f"got {initial_track_width}px then {processing_track_width}px."
            )

    await page.locator("[data-csv-result-list] .ingestion-csv-result-card-imported").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    message_dialog = page.locator("[data-message-dialog]")
    await message_dialog.wait_for(state="visible", timeout=timeout_ms)
    message_text = (await message_dialog.text_content() or "").strip()
    if "1 file(s) processed for S3 Object Storage." not in message_text:
        raise RuntimeError(f"Expected successful server import count in dialog, got: {message_text!r}")
    await page.locator("[data-csv-import-open-query]").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.unroute("**/api/ingestion/csv/upload-sessions", handle_create)
    await page.unroute("**/api/ingestion/csv/upload-sessions/**", handle_session)
    await close_message_dialog(page, timeout_ms)


async def import_server_csv_with_retried_chunk(page, timeout_ms: int) -> None:
    file_payload = b"id,name,value\n1,alpha,10\n2,beta,20\n3,gamma,30\n"
    chunk_size = 10
    upload_state = {
        "sessionId": "playwright-retry-session",
        "chunkSizeBytes": chunk_size,
        "files": [
            {
                "fileId": "playwright-retry-file",
                "fileName": "playwright-large-s3-parquet.csv",
                "sizeBytes": len(file_payload),
                "receivedBytes": 0,
                "complete": False,
            }
        ],
    }
    chunk_attempts: dict[int, int] = {}

    async def handle_create(route):
        await route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(upload_state),
        )

    async def handle_session(route):
        request = route.request
        if request.method == "PUT":
            chunk_index = int(request.url.rstrip("/").rsplit("/", 1)[-1])
            chunk_attempts[chunk_index] = chunk_attempts.get(chunk_index, 0) + 1
            if chunk_index == 1 and chunk_attempts[chunk_index] < 5:
                await route.fulfill(
                    status=503,
                    content_type="application/json",
                    body=json.dumps({"detail": "Mock transient chunk failure"}),
                )
                return
            received = min(len(file_payload), (chunk_index + 1) * chunk_size)
            upload_state["files"][0]["receivedBytes"] = received
            upload_state["files"][0]["complete"] = received == len(file_payload)
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(upload_state),
            )
            return
        if request.method == "POST" and request.url.endswith("/complete"):
            payload = json.loads(request.post_data or "{}")
            if payload.get("targetId") != "s3" or payload.get("storageFormat") != "parquet":
                await route.fulfill(
                    status=400,
                    content_type="application/json",
                    body=json.dumps({"detail": f"Unexpected completion payload: {payload!r}"}),
                )
                return
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "targetId": "s3",
                        "importedCount": 1,
                        "failedCount": 0,
                        "imports": [
                            {
                                "fileName": "playwright-large-s3-parquet.csv",
                                "storedFileName": "playwright-large-s3-parquet.parquet",
                                "status": "imported",
                                "destination": "s3",
                                "bucket": "playwright-retry-bucket",
                                "objectKey": "playwright-large-s3-parquet.parquet",
                                "storageFormat": "parquet",
                                "rowCount": 3,
                            }
                        ],
                    }
                ),
            )
            return
        await route.fulfill(status=200, content_type="application/json", body="{}")

    await page.route("**/api/ingestion/csv/upload-sessions", handle_create)
    await page.route("**/api/ingestion/csv/upload-sessions/**", handle_session)
    try:
        await page.locator('[data-csv-target-option][value="s3"]').check()
        await page.locator("[data-csv-s3-bucket]").fill("playwright-retry-bucket")
        await page.locator('[data-csv-s3-storage-format][value="parquet"]').check()
        await page.locator("[data-csv-file-input]").set_input_files(
            files=[
                {
                    "name": "playwright-large-s3-parquet.csv",
                    "mimeType": "text/csv",
                    "buffer": file_payload,
                }
            ]
        )
        await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        await page.locator("[data-csv-import-submit]").click()
        await page.locator("[data-csv-result-list] .ingestion-csv-result-card-imported").first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        if chunk_attempts.get(1) != 5:
            raise RuntimeError(f"Expected chunk 2 to be attempted five times, got: {chunk_attempts!r}")
        await close_message_dialog(page, timeout_ms)
    finally:
        await page.unroute("**/api/ingestion/csv/upload-sessions", handle_create)
        await page.unroute("**/api/ingestion/csv/upload-sessions/**", handle_session)


async def import_server_csv_recovers_after_complete_gateway_timeout(page, timeout_ms: int) -> None:
    file_payload = b"id,name,value\n1,alpha,10\n2,beta,20\n"
    chunk_size = 10
    upload_state = {
        "sessionId": "playwright-timeout-session",
        "status": "uploading",
        "chunkSizeBytes": chunk_size,
        "files": [
            {
                "fileId": "playwright-timeout-file",
                "fileName": "playwright-timeout.csv",
                "sizeBytes": len(file_payload),
                "receivedBytes": 0,
                "complete": False,
            }
        ],
    }
    completed_payload = {
        "targetId": "s3",
        "importedCount": 1,
        "failedCount": 0,
        "imports": [
            {
                "fileName": "playwright-timeout.csv",
                "storedFileName": "playwright-timeout.parquet",
                "status": "imported",
                "destination": "s3",
                "bucket": "playwright-timeout-bucket",
                "objectKey": "playwright-timeout.parquet",
                "storageFormat": "parquet",
                "rowCount": 2,
            }
        ],
    }
    delete_requests = 0

    async def handle_create(route):
        await route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(upload_state),
        )

    async def handle_session(route):
        nonlocal delete_requests
        request = route.request
        if request.method == "PUT":
            chunk_index = int(request.url.rstrip("/").rsplit("/", 1)[-1])
            received = min(len(file_payload), (chunk_index + 1) * chunk_size)
            upload_state["files"][0]["receivedBytes"] = received
            upload_state["files"][0]["complete"] = received == len(file_payload)
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(upload_state),
            )
            return
        if request.method == "POST" and request.url.endswith("/complete"):
            upload_state["status"] = "processing"
            await route.fulfill(
                status=504,
                content_type="text/html",
                body="<html><body>Gateway Time-out</body></html>",
            )
            return
        if request.method == "GET":
            upload_state["status"] = "completed"
            upload_state["result"] = completed_payload
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(upload_state),
            )
            return
        if request.method == "DELETE":
            delete_requests += 1
            await route.fulfill(
                status=404,
                content_type="application/json",
                body=json.dumps({"detail": "Unknown upload session."}),
            )
            return
        await route.fulfill(status=200, content_type="application/json", body="{}")

    await page.route("**/api/ingestion/csv/upload-sessions", handle_create)
    await page.route("**/api/ingestion/csv/upload-sessions/**", handle_session)
    try:
        await page.locator('[data-csv-target-option][value="s3"]').check()
        await page.locator("[data-csv-s3-bucket]").fill("playwright-timeout-bucket")
        await page.locator('[data-csv-s3-storage-format][value="parquet"]').check()
        await page.locator("[data-csv-file-input]").set_input_files(
            files=[
                {
                    "name": "playwright-timeout.csv",
                    "mimeType": "text/csv",
                    "buffer": file_payload,
                }
            ]
        )
        await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        await page.locator("[data-csv-import-submit]").click()
        await page.locator(
            "[data-csv-result-list] .ingestion-csv-result-card-imported",
            has_text="playwright-timeout.csv",
        ).first.wait_for(state="visible", timeout=timeout_ms)
        if delete_requests:
            raise RuntimeError(
                f"Expected gateway-timeout recovery to avoid DELETE cleanup, got {delete_requests} DELETE request(s)."
            )
        await close_message_dialog(page, timeout_ms)
    finally:
        await page.unroute("**/api/ingestion/csv/upload-sessions", handle_create)
        await page.unroute("**/api/ingestion/csv/upload-sessions/**", handle_session)


async def reject_server_csv_chunk_failure_with_context(page, timeout_ms: int) -> None:
    file_payload = b"id,name,value\n1,alpha,10\n2,beta,20\n3,gamma,30\n"
    chunk_size = 10
    upload_state = {
        "sessionId": "playwright-failed-chunk-session",
        "chunkSizeBytes": chunk_size,
        "files": [
            {
                "fileId": "playwright-failed-chunk-file",
                "fileName": "playwright-failed-chunk.csv",
                "sizeBytes": len(file_payload),
                "receivedBytes": 0,
                "complete": False,
            }
        ],
    }
    chunk_attempts: dict[int, int] = {}

    async def handle_create(route):
        await route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(upload_state),
        )

    async def handle_session(route):
        request = route.request
        if request.method == "PUT":
            chunk_index = int(request.url.rstrip("/").rsplit("/", 1)[-1])
            chunk_attempts[chunk_index] = chunk_attempts.get(chunk_index, 0) + 1
            if chunk_index == 1:
                await route.fulfill(
                    status=503,
                    content_type="application/json",
                    body=json.dumps({"detail": "Mock persistent chunk failure"}),
                )
                return
            received = min(len(file_payload), (chunk_index + 1) * chunk_size)
            upload_state["files"][0]["receivedBytes"] = received
            upload_state["files"][0]["complete"] = received == len(file_payload)
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(upload_state),
            )
            return
        await route.fulfill(status=200, content_type="application/json", body="{}")

    await page.route("**/api/ingestion/csv/upload-sessions", handle_create)
    await page.route("**/api/ingestion/csv/upload-sessions/**", handle_session)
    try:
        await page.locator('[data-csv-target-option][value="s3"]').check()
        await page.locator("[data-csv-s3-bucket]").fill("playwright-failed-chunk-bucket")
        await page.locator('[data-csv-s3-storage-format][value="parquet"]').check()
        await page.locator("[data-csv-file-input]").set_input_files(
            files=[
                {
                    "name": "playwright-failed-chunk.csv",
                    "mimeType": "text/csv",
                    "buffer": file_payload,
                }
            ]
        )
        await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        await page.locator("[data-csv-import-submit]").click()
        message_dialog = page.locator("[data-message-dialog]")
        await message_dialog.wait_for(state="visible", timeout=timeout_ms)
        message_text = (await message_dialog.text_content() or "").strip()
        expected_fragments = [
            "CSV import failed",
            "chunk 2/5",
            "5 attempts",
            "% complete",
            "MB uploaded",
            "Mock persistent chunk failure",
        ]
        missing = [fragment for fragment in expected_fragments if fragment not in message_text]
        if missing:
            raise RuntimeError(
                f"Expected chunk failure dialog to contain {missing!r}, got: {message_text!r}"
            )
        if chunk_attempts.get(1) != 5:
            raise RuntimeError(f"Expected failed chunk to be attempted five times, got: {chunk_attempts!r}")
        await close_message_dialog(page, timeout_ms)
    finally:
        await page.unroute("**/api/ingestion/csv/upload-sessions", handle_create)
        await page.unroute("**/api/ingestion/csv/upload-sessions/**", handle_session)


async def reject_server_csv_processing_failure_with_context(page, timeout_ms: int) -> None:
    file_payload = b"id,name,value\n1,alpha,10\n2,beta,20\n3,gamma,30\n"
    chunk_size = 10
    upload_state = {
        "sessionId": "playwright-processing-failure-session",
        "chunkSizeBytes": chunk_size,
        "files": [
            {
                "fileId": "playwright-processing-failure-file",
                "fileName": "playwright-processing-failure.csv",
                "sizeBytes": len(file_payload),
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
        if request.method == "PUT":
            chunk_index = int(request.url.rstrip("/").rsplit("/", 1)[-1])
            received = min(len(file_payload), (chunk_index + 1) * chunk_size)
            upload_state["files"][0]["receivedBytes"] = received
            upload_state["files"][0]["complete"] = received == len(file_payload)
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(upload_state),
            )
            return
        if request.method == "POST" and request.url.endswith("/complete"):
            payload = json.loads(request.post_data or "{}")
            if payload.get("targetId") != "s3" or payload.get("storageFormat") != "parquet":
                await route.fulfill(
                    status=400,
                    content_type="application/json",
                    body=json.dumps({"detail": f"Unexpected completion payload: {payload!r}"}),
                )
                return
            await asyncio.sleep(0.2)
            await route.fulfill(
                status=500,
                content_type="application/json",
                body=json.dumps({"detail": "Mock Parquet conversion failure"}),
            )
            return
        await route.fulfill(status=200, content_type="application/json", body="{}")

    await page.route("**/api/ingestion/csv/upload-sessions", handle_create)
    await page.route("**/api/ingestion/csv/upload-sessions/**", handle_session)
    try:
        await page.locator('[data-csv-target-option][value="s3"]').check()
        await page.locator("[data-csv-s3-bucket]").fill("playwright-processing-failure-bucket")
        await page.locator('[data-csv-s3-storage-format][value="parquet"]').check()
        await page.locator("[data-csv-file-input]").set_input_files(
            files=[
                {
                    "name": "playwright-processing-failure.csv",
                    "mimeType": "text/csv",
                    "buffer": file_payload,
                }
            ]
        )
        await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        await page.locator("[data-csv-import-submit]").click()
        await page.wait_for_function(
            """() => {
                const text = document.querySelector("[data-csv-upload-progress]")?.textContent || "";
                return text.includes("Processing")
                    && text.includes("Step 2 of 2")
                    && text.includes("Transforming file to match target data format");
            }""",
            timeout=timeout_ms,
        )
        message_dialog = page.locator("[data-message-dialog]")
        await message_dialog.wait_for(state="visible", timeout=timeout_ms)
        message_text = (await message_dialog.text_content() or "").strip()
        expected_fragments = [
            "CSV import failed",
            "Upload finished",
            "100%",
            "Step 2 of 2",
            "Transforming file to match target data format",
            "Mock Parquet conversion failure",
        ]
        missing = [fragment for fragment in expected_fragments if fragment not in message_text]
        if missing:
            raise RuntimeError(
                f"Expected processing failure dialog to contain {missing!r}, got: {message_text!r}"
            )
        await close_message_dialog(page, timeout_ms)
    finally:
        await page.unroute("**/api/ingestion/csv/upload-sessions", handle_create)
        await page.unroute("**/api/ingestion/csv/upload-sessions/**", handle_session)


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
            await assert_ingestion_tile_copy(page, args.timeout_ms)
            await assert_ingestion_returns_to_landing_after_navigation(page, args.timeout_ms)
            await assert_ingestion_state_survives_out_of_order_back_forward(page, args.timeout_ms)
            await open_csv_ingestor(page, args.timeout_ms)
            await reject_invalid_csv_file(page, args.timeout_ms)
            await import_local_csv_file(page, args.timeout_ms)
            await import_local_zip_file(page, args.timeout_ms)
            await import_server_csv_with_progress(page, args.timeout_ms)
            await import_server_csv_with_retried_chunk(page, args.timeout_ms)
            await import_server_csv_recovers_after_complete_gateway_timeout(page, args.timeout_ms)
            await reject_server_csv_chunk_failure_with_context(page, args.timeout_ms)
            await reject_server_csv_processing_failure_with_context(page, args.timeout_ms)
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

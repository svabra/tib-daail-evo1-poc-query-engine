from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the CSV import to Query Workbench handoff in the browser "
            "using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=25000)
    return parser.parse_args()


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


async def import_csv_to_pg(page, timeout_ms: int) -> str:
    file_name = "playwright-query-handoff.csv"
    expected_relation = "pg_oltp.public.pw_csv_handoff_playwright_query_handoff"

    await page.locator('[data-csv-target-option][value="pg_oltp"]').check()
    await page.locator(
        '[data-csv-config-panel="pg_oltp"] [data-csv-table-prefix]'
    ).fill("pw_csv_handoff")
    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": file_name,
                "mimeType": "text/csv",
                "buffer": b"id,canton_code,assessed_amount_chf\n1,ZH,1200.50\n2,BE,918.25\n",
            }
        ]
    )

    await page.locator("[data-csv-result-list]").wait_for(
        state="hidden",
        timeout=timeout_ms,
    )
    await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").wait_for(
        state="visible",
        timeout=timeout_ms,
    )

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/ingestion/csv/import"),
        timeout=timeout_ms,
    ) as response_info:
        await page.locator("[data-csv-import-submit]").click()

    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"CSV import failed with status {response.status}.")

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
    actual_relation = (await query_button.get_attribute("data-csv-query-source-relation") or "").strip()
    if actual_relation != expected_relation:
        raise RuntimeError(
            f"Unexpected imported relation. Expected {expected_relation}, got {actual_relation or '<empty>'}."
        )

    await query_button.click()
    return expected_relation


async def assert_query_handoff(page, expected_relation: str, timeout_ms: int) -> None:
    await page.locator("[data-workspace-notebook]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    if not page.url.rstrip("/").endswith("/query-workbench"):
        raise RuntimeError(f"Expected Query Workbench URL after handoff, got {page.url}.")

    selected_source = page.locator(
        f'[data-source-object].is-selected[data-source-option-id="pg_oltp"][data-source-object-relation="{expected_relation}"]'
    )
    await selected_source.wait_for(state="visible", timeout=timeout_ms)

    editor = page.locator("[data-query-cell] [data-editor-source]").first
    sql_text = (await editor.input_value()).strip()
    if expected_relation not in sql_text:
        raise RuntimeError("The new notebook SQL does not reference the imported PostgreSQL relation.")


async def run_smoke(args: argparse.Namespace) -> int:
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
            relation = await import_csv_to_pg(page, args.timeout_ms)
            await assert_query_handoff(page, relation, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright ingestion query handoff smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

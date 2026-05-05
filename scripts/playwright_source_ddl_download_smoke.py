from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from uuid import uuid4

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise source-object DDL downloads from the sidebar. "
            "The target app must already be running."
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
    await page.locator('[data-ingestion-tile="csv"]').wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator('[data-ingestion-entry-panel="csv"]').wait_for(
        state="hidden",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(1000)
    await page.locator('[data-ingestion-tile="csv"]').click()
    await page.locator("[data-csv-ingestion-form]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def import_csv_to_postgres(page, timeout_ms: int) -> str:
    unique_id = uuid4().hex[:8]
    file_name = f"playwright-source-ddl-{unique_id}.csv"
    expected_relation = f"pg_oltp.public.pw_ddl_playwright_source_ddl_{unique_id}"

    await page.locator('[data-csv-target-option][value="pg_oltp"]').check()
    await page.locator(
        '[data-csv-config-panel="pg_oltp"] [data-csv-table-prefix]'
    ).fill("pw_ddl")
    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": file_name,
                "mimeType": "text/csv",
                "buffer": (
                    b"id,canton_code,assessed_amount_chf,filing_date\n"
                    b"1,ZH,1200.50,2026-04-30\n"
                    b"2,BE,918.25,2026-05-01\n"
                ),
            }
        ]
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


async def assert_sidebar_ddl_download(page, relation: str, timeout_ms: int) -> None:
    selected_source = page.locator(
        f'[data-source-object].is-selected[data-source-option-id="pg_oltp"][data-source-object-relation="{relation}"]'
    )
    await selected_source.wait_for(state="visible", timeout=timeout_ms)
    await selected_source.hover()
    await selected_source.locator("[data-source-action-menu-toggle]").click(force=True)

    async with page.expect_download(timeout=timeout_ms) as download_info:
        await selected_source.locator("[data-download-source-ddl]").evaluate("node => node.click()")

    download = await download_info.value
    download_path = await download.path()
    if download_path is None:
        raise RuntimeError("The DDL download did not expose a local download path.")

    ddl_text = Path(str(download_path)).read_text(encoding="utf-8")
    table_name = relation.rsplit(".", 1)[-1]
    expected_fragments = [
        f'CREATE TABLE "public"."{table_name}"',
        '"id"',
        '"assessed_amount_chf" DOUBLE PRECISION',
        '"filing_date" DATE',
    ]
    for fragment in expected_fragments:
        if fragment not in ddl_text:
            raise RuntimeError(f"Downloaded DDL is missing {fragment!r}:\n{ddl_text}")


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
            accept_downloads=True,
        )
        console_messages: list[str] = []
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await open_csv_ingestion(page, args.base_url, args.timeout_ms)
            relation = await import_csv_to_postgres(page, args.timeout_ms)
            await assert_sidebar_ddl_download(page, relation, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright source DDL download smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

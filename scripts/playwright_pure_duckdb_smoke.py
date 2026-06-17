from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise the standalone Pure DuckDB page."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def assert_no_page_overflow(page, timeout_ms: int) -> None:
    await page.wait_for_function(
        "() => document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1",
        timeout=timeout_ms,
    )


async def main() -> None:
    args = parse_args()
    errors: list[str] = []
    console_messages: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(
            viewport={"width": 1366, "height": 900},
            accept_downloads=True,
        )
        page = await context.new_page()
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on(
            "console",
            lambda message: console_messages.append(f"{message.type}: {message.text}"),
        )
        try:
            await page.goto(args.base_url.rstrip("/"), wait_until="domcontentloaded", timeout=args.timeout_ms)
            await page.locator("[data-home-page]").wait_for(state="visible", timeout=args.timeout_ms)
            await page.locator("[data-open-pure-duckdb]").click()
            await page.locator("[data-pure-duckdb-page]").wait_for(state="visible", timeout=args.timeout_ms)
            path = await page.evaluate("window.location.pathname")
            if path != "/pure-duckdb":
                raise RuntimeError(f"Unexpected Pure DuckDB path: {path!r}")

            cell_count = await page.locator("[data-pure-duckdb-cell]").count()
            if cell_count != 17:
                raise RuntimeError(f"Expected 17 Pure DuckDB cells, found {cell_count}.")
            forbidden_count = await page.locator("[data-sidebar], .topbar, [data-query-cell]").count()
            if forbidden_count:
                raise RuntimeError("Pure DuckDB page rendered notebook shell elements.")
            app_script_loaded = await page.evaluate(
                "() => Array.from(document.scripts).some(script => String(script.src || '').includes('/static/js/app.js'))"
            )
            if app_script_loaded:
                raise RuntimeError("Pure DuckDB page loaded the full app.js bundle.")
            await assert_no_page_overflow(page, args.timeout_ms)

            first_cell = page.locator("[data-pure-duckdb-cell]").first
            await first_cell.locator("[data-pure-duckdb-sql]").fill("SELECT 1 AS pure_value")
            async with page.expect_response(
                lambda response: response.request.method == "POST"
                and response.url.endswith("/api/pure-duckdb/jobs"),
                timeout=args.timeout_ms,
            ) as response_info:
                await first_cell.locator("[data-run-pure-duckdb-cell]").click()
            response = await response_info.value
            if not response.ok:
                raise RuntimeError(f"Pure DuckDB job creation failed with HTTP {response.status}.")
            await first_cell.locator(".pure-duckdb-table tbody tr td").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            value = (await first_cell.locator(".pure-duckdb-table tbody tr td").first.inner_text()).strip()
            if value != "1":
                raise RuntimeError(f"Unexpected Pure DuckDB result value: {value!r}")
            await first_cell.locator("[data-download-pure-duckdb-csv]").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            async with page.expect_download(timeout=args.timeout_ms) as download_info:
                await first_cell.locator("[data-download-pure-duckdb-csv]").click()
            download = await download_info.value
            if download.suggested_filename != "pure-duckdb-query-1.csv":
                raise RuntimeError(
                    f"Unexpected CSV filename: {download.suggested_filename!r}"
                )
            csv_path = await download.path()
            csv_text = Path(str(csv_path)).read_text(encoding="utf-8")
            if csv_text.replace("\r\n", "\n") != "pure_value\n1\n":
                raise RuntimeError(f"Unexpected CSV content: {csv_text!r}")
            duration_one = (await first_cell.locator("[data-pure-duckdb-duration]").inner_text()).strip()
            await page.wait_for_timeout(800)
            duration_two = (await first_cell.locator("[data-pure-duckdb-duration]").inner_text()).strip()
            if duration_one != duration_two:
                raise RuntimeError("Pure DuckDB elapsed time kept changing after completion.")
            await assert_no_page_overflow(page, args.timeout_ms)

            mobile = await context.new_page()
            mobile.on("pageerror", lambda error: errors.append(str(error)))
            await mobile.set_viewport_size({"width": 390, "height": 844})
            await mobile.goto(
                f"{args.base_url.rstrip('/')}/pure-duckdb",
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await mobile.locator("[data-pure-duckdb-page]").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await assert_no_page_overflow(mobile, args.timeout_ms)
            await mobile.close()
        except Exception:
            if errors:
                print("Page errors:")
                for error in errors:
                    print(f"  {error}")
            if console_messages:
                print("Console messages:")
                for message in console_messages[-25:]:
                    print(f"  {message}")
            raise
        finally:
            await context.close()
            await browser.close()

    print("Pure DuckDB smoke passed.")


if __name__ == "__main__":
    asyncio.run(main())

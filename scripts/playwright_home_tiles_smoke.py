from __future__ import annotations

import argparse
import asyncio

from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exercise landing-page workbench tile navigation.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    errors: list[str] = []
    console_messages: list[str] = []
    requests: list[str] = []

    async def open_home(page) -> None:
        await page.goto(args.base_url.rstrip("/"), wait_until="domcontentloaded", timeout=args.timeout_ms)
        await page.locator("[data-home-page]").wait_for(state="visible", timeout=args.timeout_ms)

    async def click_tile_and_wait(page, selector: str, root_selector: str, expected_paths: set[str]) -> None:
        await open_home(page)
        await page.locator(selector).click()
        await page.locator(root_selector).wait_for(state="visible", timeout=args.timeout_ms)
        path = await page.evaluate("window.location.pathname")
        if path not in expected_paths:
            raise RuntimeError(f"Unexpected path after clicking {selector}: {path!r}")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(java_script_enabled=False)
        page = await context.new_page()
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on(
            "console",
            lambda message: console_messages.append(f"{message.type}: {message.text}"),
        )
        page.on("request", lambda request: requests.append(f"{request.method} {request.url}"))
        try:
            await open_home(page)
            shortcut_count = await page.locator("[data-home-query-shortcut]").count()
            if shortcut_count != 3:
                raise RuntimeError(f"Expected 3 Query Workbench shortcuts, found {shortcut_count}.")
            await page.locator("[data-home-query-shortcut='create-notebook']").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.locator("[data-home-query-shortcut='continue-last']").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.locator("[data-home-query-shortcut='query-monitoring']").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await click_tile_and_wait(
                page,
                "[data-home-query-workbench-entry]",
                "[data-query-workbench-entry-page]",
                {"/query-workbench"},
            )
            await click_tile_and_wait(
                page,
                ".home-workbench-card[data-open-data-products-workbench]",
                "[data-data-products-page]",
                {"/data-products"},
            )
            await click_tile_and_wait(
                page,
                ".home-workbench-card[data-open-query-data-sources]",
                "[data-data-source-management-page]",
                {"/data-sources", "/query-workbench/data-sources"},
            )
            await click_tile_and_wait(
                page,
                ".home-workbench-card[data-open-ingestion-workbench]",
                "[data-ingestion-workbench-page]",
                {"/ingestion-workbench"},
            )
            await click_tile_and_wait(
                page,
                ".home-workbench-card[data-open-loader-workbench]",
                "[data-loader-workbench-page]",
                {"/loader-workbench"},
            )
            await click_tile_and_wait(
                page,
                ".home-workbench-card[data-open-data-exchange-workbench]",
                "[data-data-exchange-page]",
                {"/data-exchange"},
            )
        except Exception:
            if errors:
                print("Page errors:")
                for error in errors:
                    print(f"  {error}")
            if console_messages:
                print("Console messages:")
                for message in console_messages[-25:]:
                    print(f"  {message}")
            if requests:
                print("Recent requests:")
                for request in requests[-25:]:
                    print(f"  {request}")
            raise
        finally:
            await context.close()
            await browser.close()

    print("Landing-page tile smoke passed.")


if __name__ == "__main__":
    asyncio.run(main())

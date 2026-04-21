from __future__ import annotations

import argparse
import asyncio
import sys
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise home-page and topbar navigation in the browser using "
            "Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


async def open_home(page, args: argparse.Namespace) -> None:
    await page.add_init_script(
        """
        () => {
          const keys = [];
          for (let index = 0; index < window.localStorage.length; index += 1) {
            const key = window.localStorage.key(index);
            if (key && key.startsWith("bdw.")) {
              keys.push(key);
            }
          }
          for (const key of keys) {
            window.localStorage.removeItem(key);
          }
        }
        """
    )
    await page.goto(
        urljoin(args.base_url, "/"),
        wait_until="domcontentloaded",
        timeout=args.timeout_ms,
    )
    await page.locator("[data-home-page]").wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )


async def open_query_workbench_from_home(page, timeout_ms: int) -> None:
    button = page.locator("[data-home-page] [data-open-query-workbench]").first
    await button.wait_for(state="visible", timeout=timeout_ms)
    await button.click()
    await page.wait_for_url("**/query-workbench", timeout=timeout_ms)
    await page.locator("[data-query-workbench-entry-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_ingestion_workbench_from_home(page, timeout_ms: int) -> None:
    button = page.locator("[data-home-page] [data-open-ingestion-workbench]").first
    await button.wait_for(state="visible", timeout=timeout_ms)
    await button.click()
    await page.wait_for_url("**/ingestion-workbench", timeout=timeout_ms)
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_loader_workbench_from_home(page, timeout_ms: int) -> None:
    button = page.locator("[data-home-page] [data-open-loader-workbench]").first
    await button.wait_for(state="visible", timeout=timeout_ms)
    await button.click()
    await page.wait_for_url("**/loader-workbench", timeout=timeout_ms)
    await page.locator("[data-loader-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_data_source_management(page, timeout_ms: int) -> None:
    button = page.locator("[data-open-query-data-sources]").first
    await button.wait_for(state="visible", timeout=timeout_ms)
    await button.click()
    await page.wait_for_url("**/query-workbench/data-sources**", timeout=timeout_ms)
    await page.locator("[data-data-source-management-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_focused_data_source_and_explorer_from_home(page, timeout_ms: int) -> None:
    link = page.locator(
        "[data-home-data-source-link][data-open-query-data-source='workspace.local']"
    ).first
    await link.wait_for(state="visible", timeout=timeout_ms)
    await link.click()
    await page.wait_for_url(
        "**/query-workbench/data-sources?source_id=workspace.local",
        timeout=timeout_ms,
    )
    await page.locator("[data-data-source-management-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator(
        "[data-data-source-management-page] [data-open-query-data-source='workspace.local'][aria-current='page']"
    ).wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    browse_button = page.locator(
        "[data-data-source-management-page] [data-open-data-source-explorer='workspace.local']"
    ).first
    await browse_button.wait_for(state="visible", timeout=timeout_ms)
    await browse_button.click()
    await page.wait_for_url(
        "**/query-workbench/data-sources/explorer?source_id=workspace.local",
        timeout=timeout_ms,
    )
    await page.locator(
        "[data-data-source-explorer-page][data-selected-source-id='workspace.local'][data-explorer-kind='local-workspace']"
    ).wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    detail_button = page.locator(
        "[data-data-source-explorer-page] .data-source-detail-action-cluster [data-open-query-data-source='workspace.local']"
    ).first
    await detail_button.wait_for(state="visible", timeout=timeout_ms)
    await detail_button.click(force=True)
    await page.wait_for_url(
        "**/query-workbench/data-sources?source_id=workspace.local",
        timeout=timeout_ms,
    )
    await page.locator(
        "[data-data-source-management-page] [data-open-query-data-source='workspace.local'][aria-current='page']"
    ).wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_query_navigation_from_topbar(page, timeout_ms: int) -> None:
    button = page.locator(
        "[data-open-query-workbench][data-open-query-workbench-navigation='true']"
    ).first
    await button.wait_for(state="visible", timeout=timeout_ms)
    await button.click()
    await page.wait_for_function(
        """
        () => {
          const path = window.location.pathname;
          return path === "/query-workbench" || path.startsWith("/notebooks/");
        }
        """,
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        """
        () => Boolean(
          document.querySelector("[data-query-workbench-entry-page]") ||
          document.querySelector("[data-workspace-notebook]")
        )
        """,
        timeout=timeout_ms,
    )


async def open_feature_list_dialog(page, timeout_ms: int) -> None:
    settings_menu = page.locator("[data-settings-menu]").first
    settings_summary = settings_menu.locator(":scope > summary")
    await settings_summary.wait_for(state="visible", timeout=timeout_ms)
    await settings_summary.click()

    feature_button = page.locator("[data-open-feature-list]").first
    await feature_button.wait_for(state="visible", timeout=timeout_ms)
    await feature_button.click()

    await page.wait_for_function(
        "() => Boolean(document.querySelector('[data-feature-list-dialog]')?.open)",
        timeout=timeout_ms,
    )
    await page.locator("[data-feature-list-submit]").first.click()
    await page.wait_for_function(
        "() => !document.querySelector('[data-feature-list-dialog]')?.open",
        timeout=timeout_ms,
    )


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        console_messages: list[str] = []
        page.on(
            "console",
            lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"),
        )
        page.on(
            "pageerror",
            lambda exc: console_messages.append(f"pageerror:{exc}"),
        )

        try:
            await open_home(page, args)
            await open_focused_data_source_and_explorer_from_home(
                page,
                args.timeout_ms,
            )
            await open_home(page, args)
            await open_query_workbench_from_home(page, args.timeout_ms)
            await open_home(page, args)
            await open_ingestion_workbench_from_home(page, args.timeout_ms)
            await open_home(page, args)
            await open_loader_workbench_from_home(page, args.timeout_ms)
            await open_home(page, args)
            await open_data_source_management(page, args.timeout_ms)
            await open_query_navigation_from_topbar(page, args.timeout_ms)
            await open_feature_list_dialog(page, args.timeout_ms)
            page_errors = [
                message for message in console_messages if message.startswith("pageerror:")
            ]
            if page_errors:
                raise RuntimeError(
                    "The browser reported page errors during home-page navigation."
                )
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright home navigation smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

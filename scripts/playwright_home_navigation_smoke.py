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
    await page.wait_for_timeout(1000)


async def click_control(locator, timeout_ms: int) -> None:
    await locator.wait_for(state="visible", timeout=timeout_ms)
    await locator.evaluate("(node) => node.click()")


async def wait_for_location(
    page,
    timeout_ms: int,
    *,
    pathname: str,
    search: str = "",
) -> None:
    await page.wait_for_function(
        """(expected) => (
            window.location.pathname === expected.pathname
            && window.location.search === expected.search
        )""",
        arg={"pathname": pathname, "search": search},
        timeout=timeout_ms,
    )


async def open_query_workbench_from_home(page, timeout_ms: int) -> None:
    button = page.locator("[data-home-page] [data-open-query-workbench]").first
    await click_control(button, timeout_ms)
    await wait_for_location(page, timeout_ms, pathname="/query-workbench")
    await page.locator("[data-query-workbench-entry-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_ingestion_workbench_from_home(page, timeout_ms: int) -> None:
    button = page.locator("[data-home-page] [data-open-ingestion-workbench]").first
    await click_control(button, timeout_ms)
    await wait_for_location(page, timeout_ms, pathname="/ingestion-workbench")
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_loader_workbench_from_home(page, timeout_ms: int) -> None:
    button = page.locator("[data-home-page] [data-open-loader-workbench]").first
    await click_control(button, timeout_ms)
    await wait_for_location(page, timeout_ms, pathname="/loader-workbench")
    await page.locator("[data-loader-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_data_source_management(page, timeout_ms: int) -> None:
    button = page.locator("[data-open-query-data-sources]").first
    await click_control(button, timeout_ms)
    await wait_for_location(page, timeout_ms, pathname="/data-sources")
    await page.locator("[data-data-source-management-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-shell].shell-sidebar-hidden").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_focused_data_source_and_explorer_from_home(page, timeout_ms: int) -> None:
    link = page.locator(
        "[data-home-data-source-link][data-open-query-data-source='workspace.local']"
    ).first
    await click_control(link, timeout_ms)
    await wait_for_location(
        page,
        timeout_ms,
        pathname="/data-sources",
        search="?source_id=workspace.local",
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
        "[data-data-source-management-page] [data-browse-data-source='workspace.local']"
    ).first
    await click_control(browse_button, timeout_ms)
    await wait_for_location(
        page,
        timeout_ms,
        pathname="/data-sources",
        search="?source_id=workspace.local&browse=1",
    )
    await page.locator(
        "[data-data-source-management-page][data-selected-source-id='workspace.local'][data-browse-source-id='workspace.local']"
    ).wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    inline_browser = page.locator("[data-inline-source-browser]").first
    await inline_browser.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await inline_browser.locator(
        "[data-source-catalog][data-source-catalog-source-id='workspace.local']"
    ).wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """() => {
          const shell = document.querySelector("[data-shell]");
          const inlineBrowser = document.querySelector("[data-inline-source-browser]");
          const catalog = inlineBrowser?.querySelector("[data-source-catalog][data-source-catalog-source-id='workspace.local']");
          return shell && shell.classList.contains("shell-sidebar-hidden")
            && inlineBrowser
            && catalog?.open === true
            && !document.querySelector("[data-data-source-explorer-page]");
        }""",
        timeout=timeout_ms,
    )
    await page.locator(
        "[data-data-source-management-page] [data-open-query-data-source='workspace.local'][aria-current='page']"
    ).wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def verify_inline_browser_for_source(
    page,
    args: argparse.Namespace,
    source_id: str,
    sidebar_source_id: str,
) -> None:
    await page.goto(
        urljoin(args.base_url, f"/data-sources?source_id={source_id}"),
        wait_until="domcontentloaded",
        timeout=args.timeout_ms,
    )
    await wait_for_location(
        page,
        args.timeout_ms,
        pathname="/data-sources",
        search=f"?source_id={source_id}",
    )
    await page.locator(
        f"[data-data-source-management-page][data-selected-source-id='{source_id}']"
    ).wait_for(state="visible", timeout=args.timeout_ms)
    await page.locator("[data-inline-source-browser]").wait_for(
        state="detached",
        timeout=args.timeout_ms,
    )

    browse_button = page.locator(
        f"[data-data-source-management-page] [data-browse-data-source='{source_id}']"
    ).first
    await click_control(browse_button, args.timeout_ms)
    await wait_for_location(
        page,
        args.timeout_ms,
        pathname="/data-sources",
        search=f"?source_id={source_id}&browse=1",
    )

    inline_browser = page.locator(
        f"[data-inline-source-browser][data-browse-source-id='{source_id}']"
    ).first
    await inline_browser.wait_for(state="visible", timeout=args.timeout_ms)
    await inline_browser.locator(
        f"[data-source-catalog][data-source-catalog-source-id='{sidebar_source_id}']"
    ).wait_for(state="visible", timeout=args.timeout_ms)
    await page.wait_for_function(
        """(expected) => {
          const shell = document.querySelector("[data-shell]");
          const inlineBrowser = document.querySelector(
            `[data-inline-source-browser][data-browse-source-id="${expected.sourceId}"]`
          );
          const catalog = inlineBrowser?.querySelector(
            `[data-source-catalog][data-source-catalog-source-id="${expected.sidebarSourceId}"]`
          );
          return shell?.classList.contains("shell-sidebar-hidden")
            && inlineBrowser
            && catalog?.open === true
            && !document.querySelector("[data-data-source-explorer-page]");
        }""",
        arg={"sourceId": source_id, "sidebarSourceId": sidebar_source_id},
        timeout=args.timeout_ms,
    )

    if source_id == "pg_oltp_native":
        native_option_count = await inline_browser.locator(
            "[data-source-object][data-source-option-id='pg_oltp_native']"
        ).count()
        non_native_option_count = await inline_browser.locator(
            "[data-source-object]:not([data-source-option-id='pg_oltp_native'])"
        ).count()
        if native_option_count and non_native_option_count:
            raise AssertionError(
                "Native PostgreSQL inline browser mixed native and non-native source option IDs."
            )


async def verify_inline_browsers_for_all_sources(page, args: argparse.Namespace) -> None:
    source_cases = [
        ("workspace.local", "workspace.local"),
        ("workspace.s3", "workspace.s3"),
        ("pg_oltp", "pg_oltp"),
        ("pg_olap", "pg_olap"),
        ("pg_oltp_native", "pg_oltp"),
    ]
    for source_id, sidebar_source_id in source_cases:
        await verify_inline_browser_for_source(
            page,
            args,
            source_id,
            sidebar_source_id,
        )


async def open_query_navigation_from_topbar(page, timeout_ms: int) -> None:
    button = page.locator(
        "[data-open-query-workbench][data-open-query-workbench-navigation='true']"
    ).first
    await click_control(button, timeout_ms)
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
    await click_control(settings_summary, timeout_ms)

    feature_button = page.locator("[data-open-feature-list]").first
    await click_control(feature_button, timeout_ms)

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
            await verify_inline_browsers_for_all_sources(page, args)
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

from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the query-cell ellipsis menu in the browser using "
            "Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


async def create_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(250)
    if await page.locator("[data-query-cell]").count():
        return

    create_button = page.locator(
        "[data-notebook-section] > summary [data-create-notebook]"
    )
    await create_button.wait_for(state="visible", timeout=timeout_ms)
    await create_button.click(force=True)

    await page.locator("[data-query-cell]").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def assert_cell_action_menu_stays_open(page, timeout_ms: int) -> None:
    cell = page.locator("[data-query-cell]").first
    menu = page.locator("[data-cell-action-menu]").first
    summary = menu.locator(":scope > summary")
    panel = menu.locator(":scope > .workspace-action-menu-panel")

    before_count = await page.locator("[data-query-cell]").count()
    await cell.hover()
    await summary.wait_for(state="visible", timeout=timeout_ms)
    await summary.click()

    await page.wait_for_function(
        "selector => !!document.querySelector(selector)?.hasAttribute('open')",
        arg="[data-cell-action-menu]",
        timeout=timeout_ms,
    )
    await panel.wait_for(state="visible", timeout=timeout_ms)

    summary_box = await summary.bounding_box()
    panel_box = await panel.bounding_box()
    if not summary_box or not panel_box:
        raise RuntimeError(
            "The cell action menu geometry could not be resolved."
        )

    await page.mouse.move(
        summary_box["x"] + summary_box["width"] / 2,
        summary_box["y"] + summary_box["height"] / 2,
    )
    await page.mouse.move(
        panel_box["x"] + panel_box["width"] / 2,
        panel_box["y"] + min(
            panel_box["height"] * 0.35,
            panel_box["height"] - 8,
        ),
        steps=18,
    )

    still_open = await menu.evaluate("node => node.hasAttribute('open')")
    if not still_open:
        raise RuntimeError(
            "The query-cell action menu closed while the pointer moved "
            "into the popup menu."
        )

    add_cell_button = panel.locator("[data-add-cell-after]:not([disabled])")
    if await add_cell_button.count():
        await add_cell_button.click()
        await page.wait_for_function(
            (
                "before => document.querySelectorAll("
                "'[data-query-cell]').length "
                "> before"
            ),
            arg=before_count,
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
            lambda msg: console_messages.append(
                f"console:{msg.type}:{msg.text}"
            ),
        )
        page.on(
            "pageerror",
            lambda exc: console_messages.append(f"pageerror:{exc}"),
        )

        try:
            await create_notebook(page, args.base_url, args.timeout_ms)
            await assert_cell_action_menu_stays_open(page, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright cell action menu smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

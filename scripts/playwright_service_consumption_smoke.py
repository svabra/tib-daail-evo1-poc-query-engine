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
            "Exercise service-consumption navigation and page rendering in the "
            "browser using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


async def open_home(page, args: argparse.Namespace) -> None:
    await page.goto(
        urljoin(args.base_url, "/"),
        wait_until="domcontentloaded",
        timeout=args.timeout_ms,
    )
    await page.locator("[data-home-page]").wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )


async def open_service_consumption_from_settings(page, timeout_ms: int) -> None:
    settings_menu = page.locator("[data-settings-menu]").first
    await settings_menu.locator(":scope > summary").click()
    service_button = page.locator("[data-open-service-consumption]").first
    await service_button.wait_for(state="visible", timeout=timeout_ms)
    await service_button.click()
    await page.wait_for_url("**/service-consumption", timeout=timeout_ms)
    await page.locator("[data-service-consumption-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def assert_page_surface(page, timeout_ms: int) -> None:
    await page.locator("[data-shell].shell-sidebar-hidden").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-service-consumption-cpu-chart]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-service-consumption-memory-chart]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-service-consumption-s3-chart]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-service-consumption-pv-chart]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("text=Query nodes will scale out automatically under higher query pressure once DAAIFL goes live.").wait_for(
        state="visible",
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
            await open_service_consumption_from_settings(page, args.timeout_ms)
            await assert_page_surface(page, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright service consumption smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

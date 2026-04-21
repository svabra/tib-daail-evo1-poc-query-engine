from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the Data Products workbench and guided publication flow "
            "in the browser using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
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


async def open_data_products_workbench(page, timeout_ms: int) -> None:
    button = page.locator("[data-home-page] [data-open-data-products-workbench]").first
    await button.wait_for(state="visible", timeout=timeout_ms)
    await button.click()
    await page.wait_for_url("**/data-products", timeout=timeout_ms)
    await page.locator("[data-data-products-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-shell].shell-sidebar-hidden").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def publish_first_available_data_product(page, timeout_ms: int) -> str:
    await page.locator("[data-open-data-product-dialog]").first.click()
    dialog = page.locator("[data-data-product-dialog]").first
    await dialog.wait_for(state="visible", timeout=timeout_ms)

    source_select = dialog.locator("[data-data-product-source-select]").first
    await source_select.wait_for(state="visible", timeout=timeout_ms)
    option_count = await source_select.locator("option").count()
    if option_count < 2:
        raise RuntimeError(
            "The data-products smoke requires at least one publishable source option."
        )
    await source_select.select_option(index=1)

    next_button = dialog.locator("[data-data-product-dialog-next]").first
    await next_button.click()
    await next_button.click()

    slug = f"playwright-smoke-{uuid.uuid4().hex[:8]}"
    title = f"Playwright Smoke {slug[-8:]}"
    await dialog.locator("[data-data-product-title-input]").fill(title)
    await dialog.locator("[data-data-product-slug-input]").fill(slug)
    await dialog.locator("[data-data-product-description-input]").fill(
        "Playwright-managed data product smoke coverage."
    )
    await dialog.locator("[data-data-product-owner-input]").fill("QA Automation")

    await next_button.click()
    await dialog.locator("[data-data-product-dialog-publish]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await dialog.locator("[data-data-product-preview-summary]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await dialog.locator("[data-data-product-dialog-publish]").click()
    await dialog.wait_for(state="hidden", timeout=timeout_ms)
    message_dialog = page.locator("[data-message-dialog]").first
    await message_dialog.wait_for(state="visible", timeout=timeout_ms)
    await message_dialog.locator("[data-message-submit]").click()
    await message_dialog.wait_for(state="hidden", timeout=timeout_ms)

    card = page.locator(f"[data-data-product-card][data-data-product-slug='{slug}']").first
    await card.wait_for(state="visible", timeout=timeout_ms)
    return slug


async def assert_published_endpoint_responds(page, base_url: str, slug: str) -> None:
    card = page.locator(f"[data-data-product-card][data-data-product-slug='{slug}']").first
    menu_toggle = card.locator("[data-data-product-card-menu-toggle]").first
    await menu_toggle.click()

    endpoint_link = card.locator("[data-open-data-product-endpoint]").first
    await endpoint_link.wait_for(state="visible")
    href = await endpoint_link.get_attribute("href")
    if not href:
        raise RuntimeError("The published data product card did not expose an endpoint link.")

    response = await page.context.request.get(urljoin(base_url, href))
    if not response.ok:
        raise RuntimeError(
            f"Published endpoint returned {response.status} for {href}."
        )

    content_type = response.headers.get("content-type", "").strip()
    if not content_type:
        raise RuntimeError("Published endpoint did not return a content type.")


async def assert_tile_content_does_not_overflow(page) -> None:
    overflowing_cards = await page.eval_on_selector_all(
        "[data-data-product-card]",
        """
        (nodes) => nodes
          .map((node) => {
            const cardRect = node.getBoundingClientRect();
            const contentNodes = Array.from(
              node.querySelectorAll(
                '.data-product-card-topbar, .data-product-card-copy, .data-product-card-detail-grid, .data-product-card-chip-row'
              )
            );
            const lowestBottom = contentNodes.reduce((maxBottom, child) => {
              const rect = child.getBoundingClientRect();
              return Math.max(maxBottom, rect.bottom);
            }, cardRect.top);
            return {
              title: node.querySelector('.data-product-link-title')?.textContent?.trim() || '',
              overflow: Math.round(lowestBottom - cardRect.bottom),
            };
          })
          .filter((entry) => entry.overflow > 0)
        """,
    )
    if overflowing_cards:
        raise RuntimeError(
            f"One or more data product tiles overflow their card bounds: {overflowing_cards}"
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
            await open_data_products_workbench(page, args.timeout_ms)
            slug = await publish_first_available_data_product(page, args.timeout_ms)
            await assert_tile_content_does_not_overflow(page)
            await assert_published_endpoint_responds(page, args.base_url, slug)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright data products smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

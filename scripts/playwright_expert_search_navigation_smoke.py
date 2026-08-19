from __future__ import annotations

import argparse
import asyncio
from urllib.parse import urljoin

from playwright.async_api import async_playwright


PRODUCT_SLUG = "search-product"
PRODUCT_PATH = f"/dataproducts/{PRODUCT_SLUG}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify native mouse navigation from Home search results to data "
            "products and from Home to the expert search."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def install_search_fixtures(page) -> None:
    await page.add_init_script(
        """
        () => {
          for (const key of Object.keys(window.localStorage)) {
            if (key.startsWith("bdw.")) window.localStorage.removeItem(key);
          }
        }
        """
    )
    await page.route(
        "**/api/notebooks/search-index",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=(
                '{"items":['
                '{"id":"common-notebook-one","title":"Common Notebook One",'
                '"summary":"Common demo","tags":[],"path":"PoC Tests",'
                '"type":"built-in","targetUrl":"/notebooks/common-notebook-one"},'
                '{"id":"common-notebook-two","title":"Common Notebook Two",'
                '"summary":"Common demo","tags":[],"path":"PoC Tests",'
                '"type":"built-in","targetUrl":"/notebooks/common-notebook-two"},'
                '{"id":"common-notebook-three","title":"Common Notebook Three",'
                '"summary":"Common demo","tags":[],"path":"PoC Tests",'
                '"type":"built-in","targetUrl":"/notebooks/common-notebook-three"}'
                ']}'
            ),
        ),
    )
    await page.route(
        "**/api/workbench/catalog-search-index",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=(
                '{"items":[{"id":"source:s3","kind":"source",'
                '"kindLabel":"Data Source","title":"Common S3",'
                '"summary":"Common demo","tags":["S3"],"path":"s3",'
                '"type":"object-storage",'
                '"targetUrl":"/data-sources?source_id=s3"}]}'
            ),
        ),
    )
    await page.route(
        "**/api/data-products",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=(
                '{"products":[{"productId":"data-product-search",'
                '"slug":"search-product","title":"Search Product Alpha",'
                '"description":"Common demo data product",'
                '"tags":["common"],"owner":"QA","domain":"Search",'
                '"accessLevel":"internal",'
                '"documentationPath":"/dataproducts/search-product"}]}'
            ),
        ),
    )
    await page.route(
        f"**{PRODUCT_PATH}",
        lambda route: route.fulfill(
            status=200,
            content_type="text/html",
            body=(
                "<!doctype html><html><head><title>Search Product Alpha</title></head>"
                '<body><main data-search-product-detail><h1>Search Product Alpha</h1></main></body></html>'
            ),
        ),
    )


async def open_home(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(urljoin(base_url, "/"), wait_until="domcontentloaded", timeout=timeout_ms)
    await page.locator("[data-home-page]").wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """() => (
          document.querySelector('[data-home-notebook-search-form]')?.dataset.bound === 'true'
        )""",
        timeout=timeout_ms,
    )


async def assert_home_product_click(page, base_url: str, timeout_ms: int) -> None:
    await open_home(page, base_url, timeout_ms)
    search = page.locator("[data-home-notebook-search-input]")
    await search.fill("Search Product Alpha")
    product = page.locator(
        f'[data-home-notebook-result-index][href="{PRODUCT_PATH}"]'
    )
    await product.wait_for(state="visible", timeout=timeout_ms)
    await product.click(timeout=timeout_ms)
    await page.wait_for_url(f"**{PRODUCT_PATH}", timeout=timeout_ms)
    await page.locator("[data-search-product-detail]").wait_for(
        state="visible", timeout=timeout_ms
    )


async def assert_all_results_and_expert_product_click(
    page, base_url: str, timeout_ms: int
) -> None:
    await open_home(page, base_url, timeout_ms)
    await page.locator("[data-home-notebook-search-input]").fill("common")
    all_results = page.locator("[data-home-notebook-search-all]")
    await all_results.wait_for(state="visible", timeout=timeout_ms)
    if await all_results.get_attribute("href") != "/search?q=common":
        raise AssertionError("Home search did not preserve the query in the expert-search URL.")
    await all_results.click(timeout=timeout_ms)
    await page.wait_for_url("**/search?q=common", timeout=timeout_ms)
    await page.locator("[data-workbench-expert-search-page]").wait_for(
        state="visible", timeout=timeout_ms
    )
    await page.wait_for_function(
        """() => (
          document.querySelector("[data-workbench-expert-search-input]")?.value === "common"
        )""",
        timeout=timeout_ms,
    )
    if await page.locator("[data-workbench-expert-search-input]").input_value() != "common":
        raise AssertionError("Expert search did not restore the Home query.")

    product = page.locator(
        '[data-workbench-expert-search-result-kind="product"]'
    )
    await product.wait_for(state="visible", timeout=timeout_ms)
    product_link = product.locator(f'h3 a[href="{PRODUCT_PATH}"]')
    await product_link.click(timeout=timeout_ms)
    await page.wait_for_url(f"**{PRODUCT_PATH}", timeout=timeout_ms)
    await page.locator("[data-search-product-detail]").wait_for(
        state="visible", timeout=timeout_ms
    )


async def assert_hover_expert_search_button(
    page, base_url: str, timeout_ms: int
) -> None:
    await open_home(page, base_url, timeout_ms)
    await page.mouse.move(1, 1)
    form = page.locator("[data-home-notebook-search-form]")
    expert_search = page.locator("[data-home-notebook-search-expert]")
    await expert_search.wait_for(state="hidden", timeout=timeout_ms)
    await form.hover()
    await expert_search.wait_for(state="visible", timeout=timeout_ms)
    await page.locator("[data-home-notebook-search-input]").evaluate(
        """input => {
          input.value = 'common';
          input.dispatchEvent(new Event('input', { bubbles: true }));
        }"""
    )
    results = page.locator("[data-home-notebook-search-results]")
    await page.wait_for_function(
        "document.querySelectorAll('[data-home-notebook-search-results] li').length === 3",
        timeout=timeout_ms,
    )
    result_metrics = await results.evaluate(
        """node => ({
          clientHeight: node.clientHeight,
          scrollHeight: node.scrollHeight,
          overflowY: getComputedStyle(node).overflowY,
        })"""
    )
    if result_metrics["scrollHeight"] > result_metrics["clientHeight"]:
        raise AssertionError("Home search preview unexpectedly requires vertical scrolling.")
    if result_metrics["overflowY"] in {"auto", "scroll"}:
        raise AssertionError("Home search preview exposes an inner vertical scrollbar.")
    if await expert_search.get_attribute("href") != "/search?q=common":
        raise AssertionError("Expert-search button did not preserve the current query.")
    if await form.evaluate("node => node.classList.contains('is-expanded')"):
        raise AssertionError("Hovering the expert-search action unexpectedly expanded the quick search.")
    box = await expert_search.bounding_box()
    if box is None:
        raise AssertionError("Expert-search action has no clickable bounding box.")
    await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    await page.mouse.down()
    if await form.evaluate("node => node.classList.contains('is-expanded')"):
        raise AssertionError("The first pointer press expanded the quick search instead of activating the link.")
    await page.mouse.up()
    await page.wait_for_url("**/search?q=common", timeout=timeout_ms)
    await page.locator("[data-workbench-expert-search-page]").wait_for(
        state="visible", timeout=timeout_ms
    )


async def main() -> None:
    args = parse_args()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1440, "height": 1000})
        try:
            await install_search_fixtures(page)
            await assert_home_product_click(page, args.base_url, args.timeout_ms)
            await assert_hover_expert_search_button(page, args.base_url, args.timeout_ms)
            await assert_all_results_and_expert_product_click(
                page, args.base_url, args.timeout_ms
            )
        finally:
            await browser.close()
    print(
        "Playwright expert-search navigation passed: native product clicks "
        "and both Home expert-search links reached their intended pages."
    )


if __name__ == "__main__":
    asyncio.run(main())

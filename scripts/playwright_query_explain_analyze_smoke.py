from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise SQL Explain and Analyze controls in the query workbench."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def ensure_query_cell(page, base_url: str, timeout_ms: int):
    await page.goto(f"{base_url.rstrip('/')}/query-workbench", wait_until="domcontentloaded", timeout=timeout_ms)
    await page.wait_for_timeout(250)
    query_cells = page.locator("[data-query-cell]:visible")
    if await query_cells.count():
        await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
        cell = query_cells.first
    else:
        for _ in range(3):
            clicked = await page.evaluate(
                """
                async () => {
                  const visibleCells = Array.from(document.querySelectorAll("[data-query-cell]"))
                    .filter((element) => Boolean(element.offsetWidth || element.offsetHeight || element.getClientRects().length));
                  if (visibleCells.length) {
                    return true;
                  }

                  const entryCreate = document.querySelector(
                    "[data-query-workbench-entry-page] [data-create-notebook]"
                  );
                  if (entryCreate instanceof HTMLElement) {
                    entryCreate.click();
                    return true;
                  }

                  const sidebarCreate = document.querySelector(
                    "[data-notebook-section] > summary [data-create-notebook]"
                  );
                  if (sidebarCreate instanceof HTMLElement) {
                    sidebarCreate.click();
                    return true;
                  }

                  const response = await window.fetch("/query-workbench", {
                    headers: {
                      Accept: "text/html",
                      "HX-Request": "true",
                    },
                  });
                  if (!response.ok) {
                    throw new Error(`failed to refresh query workbench (${response.status})`);
                  }
                  const html = await response.text();
                  const panel = document.getElementById("workspace-panel");
                  if (!(panel instanceof HTMLElement)) {
                    throw new Error("workspace-panel was not found.");
                  }
                  panel.innerHTML = html;

                  const refreshedCreate = document.querySelector(
                    "[data-query-workbench-entry-page] [data-create-notebook], "
                    + "[data-notebook-section] > summary [data-create-notebook]"
                  );
                  if (refreshedCreate instanceof HTMLElement) {
                    refreshedCreate.click();
                    return true;
                  }
                  return false;
                }
                """
            )
            if not clicked:
                raise RuntimeError("No notebook create action was available in the workbench.")
            try:
                await page.wait_for_function(
                    """
                    () => Array.from(document.querySelectorAll("[data-query-cell]"))
                      .some((element) => Boolean(element.offsetWidth || element.offsetHeight || element.getClientRects().length))
                    """,
                    timeout=timeout_ms,
                )
                break
            except PlaywrightTimeoutError:
                await page.goto(f"{base_url.rstrip('/')}/query-workbench", wait_until="domcontentloaded", timeout=timeout_ms)
        else:
            raise RuntimeError("Creating a notebook did not render a visible query cell.")
        cell = query_cells.first
        await cell.wait_for(state="visible", timeout=timeout_ms)
    await cell.evaluate(
        """
        (cell) => {
          const textarea = cell.querySelector("[data-editor-source]");
          textarea.value = "select range as value from range(250000)";
          textarea.dispatchEvent(new Event("input", { bubbles: true }));
          textarea.dispatchEvent(new Event("change", { bubbles: true }));
        }
        """
    )
    return cell


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1440, "height": 1100})
        console_messages: list[str] = []
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))
        try:
            cell = await ensure_query_cell(page, args.base_url, args.timeout_ms)
            async with page.expect_response(lambda response: response.url.endswith("/api/query-jobs/explain")):
                await cell.locator("[data-explain-query]").evaluate("button => button.click()")
            await page.locator("[data-message-title]").filter(has_text="Query Explain").wait_for(timeout=args.timeout_ms)
            await page.locator("[data-message-submit]").click()

            async with page.expect_response(lambda response: response.url.endswith("/api/query-jobs/analyze")):
                await cell.locator("[data-analyze-query]").evaluate("button => button.click()")
            await cell.locator("[data-cell-result] .result-badge").filter(has_text="Analyze").wait_for(timeout=args.timeout_ms)
            await page.locator(".query-plan-output").wait_for(timeout=args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError, AssertionError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1
        await browser.close()
    print("Playwright query explain/analyze smoke passed.")
    return 0


def main() -> int:
    return asyncio.run(run_smoke(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

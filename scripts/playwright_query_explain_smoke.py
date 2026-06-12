from __future__ import annotations

import argparse
import asyncio

from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise the notebook DuckDB Explain button and query plan modal."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def ensure_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        """
        () => Boolean(
          document.querySelector("[data-query-cell]")
          || document.querySelector("[data-query-workbench-entry-page]")
        )
        """,
        timeout=timeout_ms,
    )
    query_cells = page.locator("[data-query-cell]:visible")
    if not await query_cells.count():
        await page.wait_for_timeout(1500)
        await page.wait_for_function(
            """
            () => Array.from(document.querySelectorAll("[data-query-workbench-entry-page] [data-create-notebook]"))
              .some((button) => {
                const rect = button.getBoundingClientRect();
                return button instanceof HTMLButtonElement
                  && !button.disabled
                  && rect.width > 0
                  && rect.height > 0;
              })
            """,
            timeout=timeout_ms,
        )
        create_button = page.locator(
            "[data-query-workbench-entry-page] [data-create-notebook]:visible"
        ).first
        await create_button.click(force=True)
    await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
    await page.evaluate(
        """
        () => {
          const switchButton = document.querySelector('[data-query-cell] [data-cell-query-option="validation.sourceExistence"]');
          if (
            switchButton instanceof HTMLButtonElement &&
            switchButton.getAttribute("aria-checked") !== "true"
          ) {
            switchButton.click();
          }
        }
        """
    )
    await page.wait_for_function(
        """
        () => document
          .querySelector('[data-query-cell] [data-cell-query-option="validation.sourceExistence"]')
          ?.getAttribute("aria-checked") === "true"
        """,
        timeout=timeout_ms,
    )


async def write_cell_sql(page, sql: str) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.evaluate(
        """
        (cell, sql) => {
          const textarea = cell.querySelector("[data-editor-source]");
          if (!(textarea instanceof HTMLTextAreaElement)) {
            throw new Error("The visible SQL editor source could not be located.");
          }
          textarea.value = sql;
          textarea.dispatchEvent(new Event("input", { bubbles: true }));
          textarea.dispatchEvent(new Event("change", { bubbles: true }));
        }
        """,
        sql,
    )


async def main() -> None:
    args = parse_args()
    query_job_posts: list[str] = []
    explain_posts: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1440, "height": 1000})
        try:
            page.on(
                "request",
                lambda request: (
                    query_job_posts.append(request.url)
                    if request.method == "POST" and request.url.endswith("/api/query-jobs")
                    else explain_posts.append(request.url)
                    if request.method == "POST" and request.url.endswith("/api/query-explain")
                    else None
                ),
            )

            await ensure_query_notebook(page, args.base_url, args.timeout_ms)

            cell = page.locator("[data-query-cell]:visible").first
            await cell.hover()
            explain_button = page.locator("[data-query-cell]:visible [data-explain-cell]").first
            await explain_button.wait_for(state="visible", timeout=args.timeout_ms)
            if await explain_button.is_disabled():
                raise RuntimeError("Explain button started disabled for a simple SQL cell.")

            await write_cell_sql(page, "select * from missing.schema_table")
            await page.wait_for_function(
                """
                () => {
                  const explainButton = document.querySelector('[data-query-cell] [data-explain-cell]');
                  const indicator = document.querySelector('[data-query-cell] [data-query-source-validation]');
                  return explainButton instanceof HTMLButtonElement
                    && explainButton.disabled
                    && indicator?.dataset.querySourceValidationStatus === 'invalid';
                }
                """,
                timeout=args.timeout_ms,
            )

            await write_cell_sql(page, "select * from range(3) as t(i) where i > 0")
            await cell.hover()
            await page.wait_for_function(
                """
                () => {
                  const explainButton = document.querySelector('[data-query-cell] [data-explain-cell]');
                  return explainButton instanceof HTMLButtonElement && !explainButton.disabled;
                }
                """,
                timeout=args.timeout_ms,
            )

            query_job_posts.clear()
            explain_posts.clear()
            await cell.hover()
            await explain_button.click()

            await page.locator("[data-query-explain-dialog][open]").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            briefing_text = await page.locator("[data-query-explain-body]").inner_text(
                timeout=args.timeout_ms
            )
            if "Plan Warnings" not in briefing_text or "Operators" not in briefing_text:
                raise RuntimeError("Explain briefing did not render expected sections.")
            if query_job_posts:
                raise RuntimeError("Explain posted to /api/query-jobs instead of /api/query-explain.")
            if not explain_posts:
                raise RuntimeError("Explain did not post to /api/query-explain.")

            await page.locator('[data-query-explain-tab="physical_plan"]').click()
            physical_text = await page.locator("[data-query-explain-body]").inner_text(
                timeout=args.timeout_ms
            )
            if "FILTER" not in physical_text or "RANGE" not in physical_text:
                raise RuntimeError("Physical plan tab did not show the DuckDB plan.")

            await page.locator("[data-query-explain-dialog] [data-query-explain-submit]").click()
            await page.wait_for_timeout(250)

            await page.locator("[data-query-cell]:visible [data-set-cell-language='python']").click()
            await page.wait_for_function(
                """
                () => {
                  const explainButton = document.querySelector('[data-query-cell] [data-explain-cell]');
                  return explainButton instanceof HTMLButtonElement && explainButton.hidden;
                }
                """,
                timeout=args.timeout_ms,
            )
        finally:
            await browser.close()

    print("Playwright query explain smoke passed.")


if __name__ == "__main__":
    asyncio.run(main())

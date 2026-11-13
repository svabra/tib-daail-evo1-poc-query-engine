from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import async_playwright

from playwright_query_source_new_notebook_smoke import expand_source_tree
from playwright_query_workbench_smoke import open_isolated_notebook


MISSING_RELATION = "missing.schema_table"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify tracked source-validation failures and a successful retry in the browser."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def write_cell_sql(page, sql: str) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    content = cell.locator(".cm-content").first
    if await content.count():
        await content.click()
        await page.keyboard.press("Meta+A" if sys.platform == "darwin" else "Control+A")
        await page.keyboard.insert_text(sql)
    else:
        await cell.locator("[data-editor-source]").evaluate(
            """(textarea, sql) => {
              textarea.value = sql;
              textarea.dispatchEvent(new Event('input', { bubbles: true }));
              textarea.dispatchEvent(new Event('change', { bubbles: true }));
            }""",
            sql,
        )
    await page.wait_for_function(
        "sql => document.querySelector('[data-query-cell] [data-editor-source]')?.value === sql",
        arg=sql,
    )


async def run_cell(page, base_url: str, timeout_ms: int) -> dict:
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/query-jobs"),
        timeout=timeout_ms,
    ) as response_info:
        await page.locator("[data-query-cell]:visible [data-run-cell]").first.click()
    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"Query creation failed with HTTP {response.status}.")
    submitted = await response.json()
    job_id = submitted["jobId"]
    deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
    while asyncio.get_running_loop().time() < deadline:
        response = await page.request.get(f"{base_url.rstrip('/')}/api/query-jobs")
        if not response.ok:
            raise RuntimeError(f"Query monitoring failed with HTTP {response.status}.")
        payload = await response.json()
        job = next((item for item in payload["jobs"] if item["jobId"] == job_id), {})
        if job.get("status") in {"completed", "failed", "cancelled"}:
            return job
        await page.wait_for_timeout(100)
    raise RuntimeError(f"Query {job_id} did not finish within {timeout_ms} ms.")


async def run(args: argparse.Namespace) -> None:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page()
        try:
            await open_isolated_notebook(page, args.base_url, args.timeout_ms)
            await expand_source_tree(page)
            relation = await page.locator(
                '[data-source-object]:not([data-source-option-id="workspace.local"])'
            ).first.evaluate(
                "node => node.dataset.sourceObjectQueryReference "
                "|| node.dataset.sourceObjectQueryAlias || node.dataset.sourceObjectRelation"
            )
            if not relation:
                raise RuntimeError("No catalog relation is available for the retry.")

            toggle = page.locator(
                '[data-query-cell]:visible [data-cell-query-option="validation.sourceExistence"]'
            ).first
            if await toggle.get_attribute("aria-checked") != "true":
                await toggle.click()

            await write_cell_sql(page, f"select * from {MISSING_RELATION}")
            failed = await run_cell(page, args.base_url, args.timeout_ms)
            if failed.get("status") != "failed" or MISSING_RELATION not in str(failed.get("error")):
                raise RuntimeError(f"Missing source did not produce a named, tracked failure: {failed}")
            result_error = page.locator("[data-query-cell]:visible .result-error").first
            await result_error.wait_for(state="visible", timeout=args.timeout_ms)
            if MISSING_RELATION not in await result_error.inner_text():
                raise RuntimeError("The query result did not explain which source is missing.")

            await write_cell_sql(page, f"select * from {relation} limit 1")
            succeeded = await run_cell(page, args.base_url, args.timeout_ms)
            if succeeded.get("status") != "completed" or succeeded["jobId"] == failed["jobId"]:
                raise RuntimeError(f"Retry with an existing source did not complete as a new run: {succeeded}")
            await page.locator("[data-query-cell]:visible .result-table tbody tr").first.wait_for(
                state="visible", timeout=args.timeout_ms
            )
        finally:
            await browser.close()
    print("Source validation smoke passed: tracked missing-source failure and successful retry.")


if __name__ == "__main__":
    asyncio.run(run(parse_args()))

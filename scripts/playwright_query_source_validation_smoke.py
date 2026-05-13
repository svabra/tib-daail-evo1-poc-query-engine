from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


MISSING_RELATION = "missing.schema_table"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise notebook SQL source validation, disabled Run Cell state, "
            "and fresh run-time validation before query job creation."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def ensure_empty_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(1500)
    if await page.locator("[data-query-workbench-entry-page]").count():
        create_button = page.locator(
            "[data-query-workbench-entry-page] [data-create-notebook]"
        ).first
        await create_button.wait_for(state="visible", timeout=timeout_ms)
        await create_button.click(force=True)

    cell = page.locator("[data-query-cell]:visible").first
    await cell.wait_for(state="visible", timeout=timeout_ms)
    await write_cell_sql(page, "select 1")
    await page.wait_for_timeout(250)


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


async def expand_source_tree(page) -> None:
    await page.evaluate(
        """
        () => {
          for (const selector of [
            '[data-data-sources-section]',
            '[data-source-catalog]',
            '[data-source-schema]',
          ]) {
            document.querySelectorAll(selector).forEach((node) => {
              node.open = true;
              node.setAttribute('open', '');
            });
          }
        }
        """
    )
    await page.wait_for_timeout(250)


async def first_catalog_relation(page) -> str:
    relation = await page.evaluate(
        """
        () => {
          const sourceObjects = Array.from(document.querySelectorAll('[data-source-object]'));
          const candidate = sourceObjects.find((node) => {
            if (!(node instanceof HTMLElement) || node.offsetParent === null) {
              return false;
            }
            const sourceId = String(node.dataset.sourceOptionId || '').trim();
            if (sourceId === 'workspace.local') {
              return false;
            }
            const relation = String(node.dataset.sourceObjectRelation || '').trim();
            return relation && !relation.startsWith('workspace.local.saved_results.');
          });
          return candidate instanceof HTMLElement
            ? String(candidate.dataset.sourceObjectQueryAlias || candidate.dataset.sourceObjectRelation || '').trim()
            : '';
        }
        """
    )
    if not relation:
        raise RuntimeError("No visible catalog relation was available for source validation.")
    return relation


async def assert_invalid_source_blocks_run(page, timeout_ms: int, query_job_posts: list[str]) -> None:
    await write_cell_sql(page, f"select * from {MISSING_RELATION}")
    indicator = page.locator("[data-query-cell]:visible [data-query-source-validation]").first
    await indicator.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """
        (missingRelation) => {
          const indicator = document.querySelector('[data-query-cell] [data-query-source-validation]');
          return indicator?.dataset.querySourceValidationStatus === 'invalid'
            && (indicator.textContent || '').includes(missingRelation)
            && (indicator.textContent || '').includes('Run Cell is blocked.');
        }
        """,
        arg=MISSING_RELATION,
        timeout=timeout_ms,
    )

    run_button = page.locator("[data-query-cell]:visible [data-run-cell]").first
    if not await run_button.is_disabled():
        raise RuntimeError("Run Cell was not disabled for a missing source reference.")
    title = (await run_button.get_attribute("title") or "").strip()
    expected_title = f"Run Cell is disabled because these sources were not found: {MISSING_RELATION}"
    if title != expected_title:
        raise RuntimeError(f"Unexpected disabled Run Cell tooltip: {title!r}.")

    query_job_posts.clear()
    await page.evaluate(
        """
        () => {
          const form = document.querySelector('[data-query-cell] [data-query-form]');
          if (!(form instanceof HTMLFormElement)) {
            throw new Error("The query form could not be located.");
          }
          form.requestSubmit();
        }
        """
    )
    await page.wait_for_timeout(1200)
    if query_job_posts:
        raise RuntimeError("Invalid source submission still posted to /api/query-jobs.")
    result_error = page.locator("[data-query-cell]:visible [data-cell-result] .result-error").first
    await result_error.wait_for(state="visible", timeout=timeout_ms)
    result_text = (await result_error.inner_text()).strip()
    if MISSING_RELATION not in result_text:
        raise RuntimeError("The local failed result did not name the missing source.")


async def assert_valid_source_runs_after_runtime_check(
    page,
    relation: str,
    timeout_ms: int,
    request_events: list[str],
) -> None:
    await write_cell_sql(page, f"select * from {relation} limit 1")
    await page.wait_for_function(
        """
        () => {
          const indicator = document.querySelector('[data-query-cell] [data-query-source-validation]');
          const runButton = document.querySelector('[data-query-cell] [data-run-cell]');
          return indicator?.dataset.querySourceValidationStatus === 'valid'
            && (indicator.textContent || '').includes('Sources checked: all referenced sources exist.')
            && runButton instanceof HTMLButtonElement
            && !runButton.disabled;
        }
        """,
        timeout=timeout_ms,
    )

    run_button = page.locator("[data-query-cell]:visible [data-run-cell]").first
    enabled_title = (await run_button.get_attribute("title") or "").strip()
    if enabled_title != "Run this SQL cell. Referenced sources were found.":
        raise RuntimeError(f"Unexpected enabled Run Cell tooltip: {enabled_title!r}.")

    validation_delay_used = False
    query_delay_used = False

    async def delay_runtime_validation(route):
        nonlocal validation_delay_used
        if not validation_delay_used:
            validation_delay_used = True
            await page.wait_for_timeout(500)
        await route.continue_()

    async def delay_query_job(route):
        nonlocal query_delay_used
        if route.request.method == "POST" and not query_delay_used:
            query_delay_used = True
            await page.wait_for_timeout(500)
        await route.continue_()

    await page.route("**/api/query-sources/validate", delay_runtime_validation)
    await page.route("**/api/query-jobs", delay_query_job)
    request_events.clear()

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/query-jobs"),
        timeout=timeout_ms,
    ) as response_info:
        await page.evaluate(
            """
            () => {
              const button = document.querySelector('[data-query-cell] [data-run-cell]');
              if (!(button instanceof HTMLButtonElement)) {
                throw new Error("The Run Cell button could not be located.");
              }
              button.click();
            }
            """
        )
        await page.wait_for_function(
            """
            () => {
              const indicator = document.querySelector('[data-query-cell] [data-query-source-validation]');
              return indicator?.dataset.querySourceValidationStatus === 'checking'
                && (indicator.textContent || '').includes('Checking source existence before running...');
            }
            """,
            timeout=timeout_ms,
        )
        await page.wait_for_function(
            """
            () => {
              const indicator = document.querySelector('[data-query-cell] [data-query-source-validation]');
              return indicator?.dataset.querySourceValidationStatus === 'starting'
                && (indicator.textContent || '').includes('Sources checked. Starting query...');
            }
            """,
            timeout=timeout_ms,
        )

    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"Query job creation failed with status {response.status}.")

    if "validate" not in request_events or "query-job" not in request_events:
        raise RuntimeError(f"Expected validation and query job requests, received {request_events!r}.")
    if request_events.index("validate") > request_events.index("query-job"):
        raise RuntimeError(f"Query job was posted before source validation: {request_events!r}.")

    await page.wait_for_function(
        """
        () => {
          const indicator = document.querySelector('[data-query-cell] [data-query-source-validation]');
          return indicator?.dataset.querySourceValidationStatus === 'completed'
            && (indicator.textContent || '').includes('Query completed.');
        }
        """,
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(3800)
    await page.wait_for_function(
        """
        () => {
          const indicator = document.querySelector('[data-query-cell] [data-query-source-validation]');
          return indicator?.dataset.querySourceValidationStatus === 'valid'
            && (indicator.textContent || '').includes('Sources checked: all referenced sources exist.')
            && !(indicator.textContent || '').includes('Starting query');
        }
        """,
        timeout=timeout_ms,
    )


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1100},
            base_url=args.base_url.rstrip("/"),
        )
        console_messages: list[str] = []
        request_events: list[str] = []
        query_job_posts: list[str] = []
        page.on(
            "console",
            lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"),
        )
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))
        page.on(
            "request",
            lambda request: (
                request_events.append("validate")
                if request.method == "POST" and request.url.endswith("/api/query-sources/validate")
                else (
                    request_events.append("query-job"),
                    query_job_posts.append(request.url),
                )
                if request.method == "POST" and request.url.endswith("/api/query-jobs")
                else None
            ),
        )

        try:
            await ensure_empty_query_notebook(page, args.base_url, args.timeout_ms)
            await expand_source_tree(page)
            relation = await first_catalog_relation(page)
            await assert_invalid_source_blocks_run(page, args.timeout_ms, query_job_posts)
            await assert_valid_source_runs_after_runtime_check(
                page,
                relation,
                args.timeout_ms,
                request_events,
            )
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(f"Playwright query source validation smoke passed for relation {relation}.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

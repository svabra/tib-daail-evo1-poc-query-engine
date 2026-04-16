from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


SMOKE_SQL = "select 1 as smoke_value"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the query workbench notebook creation and cell-run flow "
            "in the browser using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


async def ensure_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(250)
    query_cells = page.locator("[data-query-cell]:visible")
    if await query_cells.count():
        await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
        return

    create_button = page.locator(
        "[data-query-workbench-entry-page] [data-create-notebook]"
    )
    await create_button.wait_for(state="visible", timeout=timeout_ms)
    await create_button.click(force=True)
    await query_cells.first.wait_for(state="visible", timeout=timeout_ms)


async def write_smoke_sql(page, timeout_ms: int) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.wait_for(state="visible", timeout=timeout_ms)
    await cell.evaluate(
        """
        (cell, sql) => {
          if (!(cell instanceof HTMLElement)) {
            throw new Error("The visible query cell could not be located.");
          }
          const textarea = cell.querySelector("[data-editor-source]");
          if (!(textarea instanceof HTMLTextAreaElement)) {
            throw new Error("The first query editor source could not be located.");
          }
          textarea.value = sql;
          textarea.dispatchEvent(new Event("input", { bubbles: true }));
          textarea.dispatchEvent(new Event("change", { bubbles: true }));
        }
        """,
        SMOKE_SQL,
    )


async def run_query_and_assert_result(page, timeout_ms: int) -> str:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.locator("[data-query-form]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/query-jobs"),
        timeout=timeout_ms,
    ) as response_info:
        await cell.evaluate(
            """
            (cell) => {
              if (!(cell instanceof HTMLElement)) {
                throw new Error("The visible query cell could not be located.");
              }
              const form = cell.querySelector("[data-query-form]");
              if (!(form instanceof HTMLFormElement)) {
                throw new Error("The visible query form could not be located.");
              }
              form.requestSubmit();
            }
            """
        )

    response = await response_info.value
    if not response.ok:
        raise RuntimeError(
            f"Query job creation failed with status {response.status}."
        )

    payload = await response.json()
    job_id = str(payload.get("jobId") or payload.get("job_id") or "").strip()
    if not job_id:
        raise RuntimeError("The query job response did not include a job id.")

    result_root = cell.locator("[data-cell-result]")
    await result_root.wait_for(state="visible", timeout=timeout_ms)
    await cell.locator(".result-table tbody tr").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )

    rendered_job_id = (
        await result_root.get_attribute("data-query-job-id") or ""
    ).strip()
    if rendered_job_id != job_id:
        raise RuntimeError(
            "The query result panel did not bind to the returned query job id."
        )

    first_value = (
        await cell.locator(".result-table tbody tr td").first.inner_text()
    ).strip()
    if first_value != "1":
        raise RuntimeError(
            f"Unexpected query result value: expected '1', received '{first_value}'."
        )

    error_count = await cell.locator(".result-error").count()
    if error_count:
        raise RuntimeError(
            "The query workbench rendered an error panel for the smoke query."
        )

    export_menu = cell.locator("[data-result-action-menu]")
    await export_menu.wait_for(state="visible", timeout=timeout_ms)
    export_text = (await export_menu.text_content() or "").strip()
    if "Save Results in Local Workspace (IndexDB) ..." not in export_text:
        raise RuntimeError(
            "The query export menu does not expose the Local Workspace (IndexDB) export action."
        )
    if "Save Results in Shared Workspace (S3) ..." not in export_text:
        raise RuntimeError(
            "The query export menu does not expose the Shared Workspace (S3) export action."
        )
    if "Download Results as ..." not in export_text:
        raise RuntimeError(
            "The query export menu does not expose the download action."
        )

    await export_menu.locator("summary").click()
    await export_menu.locator('[data-result-export-local]').click()
    local_dialog = page.locator("[data-local-workspace-save-dialog]")
    await local_dialog.wait_for(state="visible", timeout=timeout_ms)
    format_options = await local_dialog.locator("[data-export-format-select] option").all_text_contents()
    expected_options = {"CSV", "JSON Array", "JSONL", "Parquet", "XML", "Excel"}
    if not expected_options.issubset(set(option.strip() for option in format_options)):
        raise RuntimeError(f"Unexpected export format options: {format_options!r}")
    await local_dialog.locator('[data-export-format-select]').select_option("csv")
    delimiter_label = (await local_dialog.text_content() or "").strip()
    if "Delimiter" not in delimiter_label:
        raise RuntimeError("CSV export settings were not rendered in the Local Workspace export dialog.")
    await local_dialog.locator("[data-modal-cancel]").click()

    return job_id


async def assert_notebook_load_scrolls_to_top(page, timeout_ms: int) -> None:
    workspace_root = page.locator("[data-workspace-notebook]").first
    await workspace_root.wait_for(state="visible", timeout=timeout_ms)
    previous_notebook_id = (
        await workspace_root.get_attribute("data-notebook-id") or ""
    ).strip()
    if not previous_notebook_id:
        raise RuntimeError("The current workspace notebook id could not be determined.")

    await page.evaluate(
        """
        () => {
          document.body.style.minHeight = "3200px";
          window.scrollTo({ top: document.body.scrollHeight, behavior: "instant" });
        }
        """
    )
    await page.wait_for_timeout(150)

    scroll_before = await page.evaluate("() => window.scrollY")
    if scroll_before < 400:
        raise RuntimeError(
            f"The page did not reach a deep scroll position before notebook switching: {scroll_before}."
        )

    await page.evaluate(
        """
        () => {
          const button = document.querySelector("[data-notebook-section] > summary [data-create-notebook]");
          if (!(button instanceof HTMLButtonElement)) {
            throw new Error("The sidebar create notebook action could not be located.");
          }
          button.click();
        }
        """
    )

    await page.locator(
        f'[data-workspace-notebook]:not([data-notebook-id="{previous_notebook_id}"])'
    ).first.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_timeout(500)

    scroll_after = await page.evaluate("() => window.scrollY")
    if scroll_after > 200:
        raise RuntimeError(
            f"Opening a new notebook did not scroll the workspace back to the top. Remaining scrollY={scroll_after}."
        )


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        console_messages: list[str] = []
        responses: list[tuple[str, str, int]] = []
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
        page.on(
            "response",
            lambda resp: responses.append(
                (resp.request.method, resp.url, resp.status)
            ),
        )

        try:
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            await write_smoke_sql(page, args.timeout_ms)
            job_id = await run_query_and_assert_result(page, args.timeout_ms)
            await assert_notebook_load_scrolls_to_top(page, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for method, url, status in responses:
                if "/api/query-jobs" in url:
                    print(f"HTTP {method} {status} {url}", file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(f"Playwright query workbench smoke passed for job {job_id}.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

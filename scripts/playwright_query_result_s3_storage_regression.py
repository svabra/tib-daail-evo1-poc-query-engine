from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from datetime import UTC, datetime

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


SMOKE_SQL = "select 1 as smoke_value"
CUSTOM_S3_PATH = "s3://workspace/query-results/ui-regression/custom-result.parquet"
CUSTOM_VIRTUAL_PATH = 's3.workspace."query-results/ui-regression/custom-result.parquet"'
CUSTOM_DUCKDB_REFERENCE = (
    "read_parquet('s3://workspace/query-results/ui-regression/custom-result.parquet')"
)
PREPARED_DUCKDB_SQL = (
    "select * from read_parquet('s3://workspace/query-results/ui-regression/custom-result.parquet')"
)
EDITED_DUCKDB_SQL = (
    "select * from read_parquet('s3://workspace/query-results/ui-regression/edited-result.parquet')"
)
EDITED_VIRTUAL_SQL = 'select * from s3.workspace."query-results/ui-regression/edited-result.parquet"'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify the notebook result-storage S3 controls and copy actions. "
            "The target app must already be running."
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
    await page.wait_for_timeout(1200)
    query_cells = page.locator("[data-query-cell]:visible")
    if await query_cells.count():
        await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
        return

    create_button = page.locator(
        "[data-query-workbench-entry-page] [data-create-notebook]"
    )
    await create_button.wait_for(state="visible", timeout=timeout_ms)
    await create_button.first.evaluate(
        """
        (button) => {
          if (!(button instanceof HTMLElement)) {
            throw new Error("The create notebook action was not an HTMLElement.");
          }
          button.click();
        }
        """
    )
    await page.wait_for_function(
        """() => Array.from(document.querySelectorAll("[data-query-cell]")).some((cell) => {
          const style = window.getComputedStyle(cell);
          const rect = cell.getBoundingClientRect();
          return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
        })""",
        timeout=timeout_ms,
    )


async def write_smoke_sql(page, timeout_ms: int) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.wait_for(state="visible", timeout=timeout_ms)
    await cell.evaluate(
        """
        (cell, sql) => {
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


async def configure_result_storage_and_assert_copy(page, timeout_ms: int) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    toggle = cell.locator("[data-result-storage-toggle]").first
    path_input = cell.locator("input[data-result-storage-path]").first
    await toggle.wait_for(state="visible", timeout=timeout_ms)
    await path_input.wait_for(state="visible", timeout=timeout_ms)

    await toggle.check()
    await page.wait_for_function(
        """input => input instanceof HTMLInputElement && input.value.startsWith("s3://") && input.value.endsWith(".parquet")""",
        arg=await path_input.element_handle(),
        timeout=timeout_ms,
    )

    await path_input.fill(CUSTOM_S3_PATH)
    await path_input.dispatch_event("input")
    await path_input.dispatch_event("change")

    virtual_button = cell.locator("[data-cell-result-storage] [data-copy-result-storage-virtual]").first
    duckdb_button = cell.locator("[data-cell-result-storage] [data-copy-result-storage-duckdb]").first
    await virtual_button.click()
    copied_virtual = await page.evaluate("navigator.clipboard.readText()")
    if copied_virtual != CUSTOM_VIRTUAL_PATH:
        raise RuntimeError(
            f"Virtual result path copy mismatch: expected {CUSTOM_VIRTUAL_PATH!r}, got {copied_virtual!r}."
        )

    await duckdb_button.click()
    copied_duckdb = await page.evaluate("navigator.clipboard.readText()")
    if copied_duckdb != CUSTOM_DUCKDB_REFERENCE:
        raise RuntimeError(
            f"DuckDB result path copy mismatch: expected {CUSTOM_DUCKDB_REFERENCE!r}, got {copied_duckdb!r}."
        )


async def install_prepare_sql_stub(page, prepare_requests: list[dict[str, object]]) -> None:
    async def handle_prepare_sql(route) -> None:
        if route.request.method.upper() != "POST":
            await route.continue_()
            return
        try:
            request_payload = json.loads(route.request.post_data or "{}")
        except json.JSONDecodeError:
            request_payload = {}
        prepare_requests.append(request_payload)
        await route.fulfill(
            status=200,
            json={
                "submittedSql": str(request_payload.get("displaySql") or request_payload.get("sql") or ""),
                "executionSql": PREPARED_DUCKDB_SQL,
                "sourceObjects": [],
            },
        )

    await page.route("**/api/query-sql/prepare", handle_prepare_sql)


async def assert_duckdb_virtual_editor_sync(page, timeout_ms: int) -> None:
    prepare_requests: list[dict[str, object]] = []
    await install_prepare_sql_stub(page, prepare_requests)

    cell = page.locator("[data-query-cell]:visible").first
    duckdb_toggle = cell.locator('[data-editor-sql-view="duckdb"]').first
    virtual_toggle = cell.locator('[data-editor-sql-view="virtual"]').first
    panel = cell.locator("[data-duckdb-sql-panel]").first
    await duckdb_toggle.wait_for(state="visible", timeout=timeout_ms)

    await duckdb_toggle.click()
    await panel.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """expected => {
          const panel = document.querySelector("[data-query-cell]:not([hidden]) [data-duckdb-sql-panel]");
          return panel instanceof HTMLTextAreaElement && panel.value === expected;
        }""",
        arg=PREPARED_DUCKDB_SQL,
        timeout=timeout_ms,
    )

    await panel.fill(EDITED_DUCKDB_SQL)
    await page.wait_for_function(
        """expected => {
          const textarea = document.querySelector("[data-query-cell]:not([hidden]) [data-editor-source]");
          return textarea instanceof HTMLTextAreaElement && textarea.value === expected;
        }""",
        arg=EDITED_VIRTUAL_SQL,
        timeout=timeout_ms,
    )

    await virtual_toggle.click()
    await page.wait_for_function(
        """() => {
          const editor = document.querySelector("[data-query-cell]:not([hidden]) [data-editor-root]");
          const panel = document.querySelector("[data-query-cell]:not([hidden]) [data-duckdb-sql-panel]");
          return editor instanceof HTMLElement
            && panel instanceof HTMLTextAreaElement
            && !editor.classList.contains("is-duckdb-sql-view")
            && panel.hidden;
        }""",
        timeout=timeout_ms,
    )

    await duckdb_toggle.click()
    await panel.wait_for(state="visible", timeout=timeout_ms)
    if not any(
        request.get("displaySql") == EDITED_VIRTUAL_SQL
        or request.get("sql") == EDITED_VIRTUAL_SQL
        for request in prepare_requests[1:]
    ):
        raise RuntimeError(
            "Editing the DuckDB SQL panel did not update the virtual SQL used by the next prepare request."
        )


def multipart_field(raw_body: str, field_name: str, fallback: str = "") -> str:
    match = re.search(
        rf'name="{re.escape(field_name)}"\r?\n\r?\n(.*?)\r?\n--',
        raw_body,
        flags=re.DOTALL,
    )
    return (match.group(1).strip() if match else fallback) or fallback


def completed_job_payload(*, notebook_id: str, notebook_title: str, cell_id: str) -> dict[str, object]:
    now = datetime.now(UTC).isoformat()
    query_options = {
        "duckdb": {
            "parquetHivePartitioning": "auto",
            "cacheHydration": {
                "mode": "off",
                "scope": "referencedS3Parquet",
                "indexPolicy": "autoPredicates",
            },
            "resultStorage": {
                "mode": "on",
                "path": CUSTOM_S3_PATH,
            },
        },
        "validation": {"sourceExistence": "off"},
    }
    return {
        "jobId": "query-ui-s3-storage-regression",
        "notebookId": notebook_id,
        "notebookTitle": notebook_title or "UI Regression Notebook",
        "cellId": cell_id,
        "sql": SMOKE_SQL,
        "executionSql": SMOKE_SQL,
        "status": "completed",
        "startedAt": now,
        "updatedAt": now,
        "completedAt": now,
        "durationMs": 25.0,
        "progress": 1.0,
        "progressLabel": "Completed",
        "message": "1 row(s) shown.",
        "columns": ["smoke_value"],
        "rows": [[1]],
        "rowCount": 1,
        "rowsShown": 1,
        "truncated": False,
        "dataSources": [],
        "queryOptions": query_options,
        "sourceTypes": [],
        "touchedRelations": [],
        "touchedBuckets": [],
        "backendName": "VMTP DUCKDB",
        "executionMode": "duckdb-read",
        "duckdbExecutionPath": "isolated-read",
        "timings": {"engineQueryMs": 10.0, "resultFetchMs": 1.0, "backendTotalMs": 25.0},
        "cacheHydration": {},
        "resultStorage": {
            "enabled": True,
            "status": "completed",
            "format": "parquet",
            "path": CUSTOM_S3_PATH,
            "bucket": "workspace",
            "key": "query-results/ui-regression/custom-result.parquet",
            "virtualPath": CUSTOM_VIRTUAL_PATH,
            "duckdbPath": CUSTOM_S3_PATH,
            "duckdbReference": CUSTOM_DUCKDB_REFERENCE,
            "message": "DuckDB stored the complete result set in S3.",
        },
        "progressEvents": [],
        "canCancel": False,
    }


async def install_query_job_stub(page) -> None:
    async def handle_query_job(route) -> None:
        if route.request.method.upper() != "POST":
            await route.continue_()
            return
        raw_body = route.request.post_data or ""
        if "resultStorage" not in raw_body or CUSTOM_S3_PATH not in raw_body:
            await route.fulfill(
                status=422,
                content_type="application/json",
                body=json.dumps(
                    {
                        "detail": (
                            "The submitted queryOptions did not include the enabled "
                            "resultStorage path."
                        )
                    }
                ),
            )
            return
        await route.fulfill(
            status=200,
            json=completed_job_payload(
                notebook_id=multipart_field(raw_body, "notebook_id", "ui-regression-notebook"),
                notebook_title=multipart_field(raw_body, "notebook_title", "UI Regression Notebook"),
                cell_id=multipart_field(raw_body, "cell_id", "ui-regression-cell"),
            ),
        )

    await page.route("**/api/query-jobs", handle_query_job)


async def run_query_and_assert_result_storage(page, timeout_ms: int) -> None:
    await install_query_job_stub(page)
    cell = page.locator("[data-query-cell]:visible").first
    await cell.evaluate(
        """
        (cell) => {
          const form = cell.querySelector("[data-query-form]");
          if (!(form instanceof HTMLFormElement)) {
            throw new Error("The visible query form could not be located.");
          }
          form.requestSubmit();
        }
        """
    )

    result_summary = cell.locator("[data-result-storage-summary]").first
    await result_summary.wait_for(state="visible", timeout=timeout_ms)
    result_text = (await result_summary.inner_text()).strip()
    if CUSTOM_S3_PATH not in result_text or "stored" not in result_text.lower():
        raise RuntimeError(
            f"The completed result storage summary did not render the expected copy: {result_text!r}."
        )

    await result_summary.locator("[data-copy-result-storage-virtual]").click()
    copied_virtual = await page.evaluate("navigator.clipboard.readText()")
    if copied_virtual != CUSTOM_VIRTUAL_PATH:
        raise RuntimeError("The completed result panel copied the wrong virtual path.")

    await result_summary.locator("[data-copy-result-storage-duckdb]").click()
    copied_duckdb = await page.evaluate("navigator.clipboard.readText()")
    if copied_duckdb != CUSTOM_DUCKDB_REFERENCE:
        raise RuntimeError("The completed result panel copied the wrong DuckDB path.")


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 1100},
            base_url=args.base_url.rstrip("/"),
            permissions=["clipboard-read", "clipboard-write"],
        )
        page = await context.new_page()
        console_messages: list[str] = []
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            await write_smoke_sql(page, args.timeout_ms)
            await configure_result_storage_and_assert_copy(page, args.timeout_ms)
            await assert_duckdb_virtual_editor_sync(page, args.timeout_ms)
            await run_query_and_assert_result_storage(page, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await context.close()
            await browser.close()
            return 1

        await context.close()
        await browser.close()

    print("Playwright query result S3 storage regression passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

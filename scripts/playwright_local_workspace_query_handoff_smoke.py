from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the Local Workspace CSV import to Query Workbench handoff "
            "and run the generated query using Playwright."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=25000)
    return parser.parse_args()


async def open_local_csv_ingestor(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/ingestion-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(1000)
    csv_tile = page.locator('[data-ingestion-tile="csv"]').first
    form = page.locator("[data-csv-ingestion-form]")
    for _attempt in range(5):
        await csv_tile.click()
        try:
            await form.wait_for(state="visible", timeout=2000)
            break
        except PlaywrightTimeoutError:
            await page.wait_for_timeout(500)
    await form.wait_for(state="visible", timeout=timeout_ms)


async def import_local_csv(page, timeout_ms: int) -> str:
    file_name = "playwright-local-query.csv"
    await page.locator("[data-csv-folder-path]").fill("test/test-sub")
    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": file_name,
                "mimeType": "text/csv",
                "buffer": (
                    b"record_id,canton_code,tax_office,assessed_amount_chf\n"
                    b"1,ZH,Zurich Central Tax Office,1200.50\n"
                    b"2,BE,Bern Regional Tax Office,918.25\n"
                ),
            }
        ]
    )

    await page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").wait_for(
        state="visible",
        timeout=timeout_ms,
    )

    import_button = page.locator("[data-csv-import-submit]")
    await import_button.wait_for(state="visible", timeout=timeout_ms)
    await import_button.click()

    await page.locator("[data-csv-result-list] .ingestion-csv-result-card-imported").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    message_dialog = page.locator("[data-message-dialog]")
    if await message_dialog.is_visible():
        await page.locator("[data-message-submit]").click()
        await message_dialog.wait_for(state="hidden", timeout=timeout_ms)

    query_button = page.locator("[data-csv-import-open-query]").first
    await query_button.wait_for(state="visible", timeout=timeout_ms)
    relation = (await query_button.get_attribute("data-csv-query-source-relation") or "").strip()
    if not relation.startswith("workspace.local.saved_results."):
        raise RuntimeError(f"Unexpected Local Workspace relation for handoff: {relation or '<empty>'}.")

    return relation


async def assert_upload_query_link_opens_without_pre_run_sync(page, timeout_ms: int) -> None:
    sync_calls: list[str] = []

    async def delayed_sync(route):
        sync_calls.append(route.request.url)
        await asyncio.sleep(6)
        await route.fulfill(
            status=503,
            content_type="application/json",
            body='{"detail":"Delayed Local Workspace sync should not block notebook creation."}',
        )

    await page.route("**/api/local-workspace/query-sources/sync", delayed_sync)
    try:
        query_button = page.locator("[data-csv-import-open-query]").first
        await query_button.wait_for(state="visible", timeout=timeout_ms)
        await query_button.click()
        await page.locator("[data-workspace-notebook]").wait_for(
            state="visible",
            timeout=min(timeout_ms, 4000),
        )
        await page.wait_for_timeout(250)
        if sync_calls:
            raise RuntimeError(
                "The Local Workspace upload result link tried to sync the file before "
                "opening the new notebook. Large browser-local uploads would make the "
                f"link appear stuck. Sync calls: {sync_calls!r}"
            )
    finally:
        await page.unroute("**/api/local-workspace/query-sources/sync", delayed_sync)


async def reveal_local_workspace_source(page, expected_relation: str, timeout_ms: int):
    data_sources = page.locator("[data-data-sources-section]").first
    if await data_sources.count():
        await data_sources.evaluate(
            """
            (node) => {
              if (node instanceof HTMLDetailsElement) {
                node.open = true;
              }
            }
            """
        )

    source_object = page.locator(
        f'[data-source-object][data-source-option-id="workspace.local"][data-source-object-relation="{expected_relation}"]'
    ).first
    await source_object.wait_for(state="attached", timeout=timeout_ms)
    await source_object.evaluate(
        """
        (node) => {
          let current = node.parentElement;
          while (current) {
            if (current instanceof HTMLDetailsElement) {
              current.open = true;
            }
            current = current.parentElement;
          }
          node.scrollIntoView({ block: "center" });
        }
        """
    )
    await source_object.wait_for(state="visible", timeout=timeout_ms)
    return source_object


async def assert_local_query_notebook(
    page,
    expected_relation: str,
    expected_alias: str,
    timeout_ms: int,
) -> None:
    await page.locator("[data-workspace-notebook]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    if not page.url.rstrip("/").endswith("/query-workbench"):
        raise RuntimeError(f"Expected Query Workbench URL after handoff, got {page.url}.")

    await reveal_local_workspace_source(page, expected_relation, timeout_ms)

    editor = page.locator("[data-query-cell] [data-editor-source]").first
    sql_text = (await editor.input_value()).strip()
    if expected_alias not in sql_text:
        raise RuntimeError(
            f"The new notebook SQL does not reference the readable Local Workspace alias: {sql_text!r}."
        )
    if expected_relation in sql_text:
        raise RuntimeError(
            f"The new notebook SQL still exposes the internal Local Workspace relation: {sql_text!r}."
        )

    shared_workspace_label = page.locator(
        '[data-source-catalog-source-id="s3"] > summary .source-node-label span'
    ).first
    await shared_workspace_label.wait_for(state="visible", timeout=timeout_ms)
    shared_workspace_text = (await shared_workspace_label.text_content() or "").strip()
    if shared_workspace_text != "S3 Object Storage":
        raise RuntimeError(
            f"Unexpected Shared Workspace sidebar label: {shared_workspace_text!r}"
        )


async def assert_local_workspace_sidebar_actions(page, expected_relation: str, timeout_ms: int) -> None:
    source_object = await reveal_local_workspace_source(page, expected_relation, timeout_ms)
    await source_object.scroll_into_view_if_needed(timeout=timeout_ms)
    action_menu = source_object.locator("[data-source-action-menu]").first
    await action_menu.evaluate(
        """
        (menu) => {
          if (menu instanceof HTMLDetailsElement) {
            menu.open = true;
          }
        }
        """
    )

    menu_panel = action_menu.locator(".workspace-action-menu-panel").first
    await menu_panel.wait_for(state="visible", timeout=timeout_ms)

    for label in (
        "View Data",
        "Query in current notebook",
        "Query in new notebook",
        "Move ...",
        "Download",
        "Download DDL",
        "Delete ...",
    ):
        item = menu_panel.get_by_role("button", name=label, exact=True)
        await item.wait_for(state="visible", timeout=timeout_ms)


async def run_local_query(page, timeout_ms: int) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.locator("[data-query-form]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
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
    result_root = page.locator("[data-cell-result]").first
    await result_root.wait_for(state="visible", timeout=timeout_ms)
    await result_root.get_by_text("Zurich Central Tax Office").first.wait_for(
      state="visible",
      timeout=timeout_ms,
    )

    result_text = " ".join(((await result_root.text_content()) or "").split())
    for expected_header in ("record_id", "canton_code", "tax_office", "assessed_amount_chf"):
        if expected_header not in result_text:
            raise RuntimeError(f"Missing Local Workspace query header {expected_header!r}: {result_text!r}")
    if "Zurich Central Tax Office" not in result_text:
        raise RuntimeError(f"Unexpected Local Workspace query result content: {result_text!r}")


async def assert_legacy_local_relation_still_runs(
    page,
    legacy_relation: str,
    timeout_ms: int,
    query_job_posts: list[str],
) -> None:
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
        f"select * from {legacy_relation}",
    )
    try:
        await page.wait_for_function(
            """
            () => {
              const cell = document.querySelector('[data-query-cell]');
              const indicator = cell?.querySelector('[data-query-source-validation]');
              const runButton = cell?.querySelector('[data-run-cell]');
          const status = indicator?.dataset.querySourceValidationStatus || '';
          return ['unchecked', 'skipped'].includes(status)
            && runButton instanceof HTMLButtonElement
            && !runButton.disabled;
            }
            """,
            timeout=timeout_ms,
        )
    except PlaywrightTimeoutError as exc:
        state = await cell.evaluate(
            """
            (cell) => {
              const indicator = cell.querySelector('[data-query-source-validation]');
              const runButton = cell.querySelector('[data-run-cell]');
              const textarea = cell.querySelector('[data-editor-source]');
              return {
                validationStatus: indicator?.dataset.querySourceValidationStatus || '',
                validationText: indicator?.textContent || '',
                runDisabled: runButton instanceof HTMLButtonElement ? runButton.disabled : null,
                runTitle: runButton instanceof HTMLButtonElement ? runButton.title : '',
                sql: textarea instanceof HTMLTextAreaElement ? textarea.value : '',
              };
            }
            """
        )
        raise RuntimeError(
            "Legacy Local Workspace relation did not become runnable after SQL edit. "
            f"State: {state!r}"
        ) from exc
    query_job_posts.clear()
    await cell.locator("[data-run-cell]").first.click()
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)
    while not query_job_posts and asyncio.get_running_loop().time() < deadline:
        await page.wait_for_timeout(100)
    if not query_job_posts:
        raise RuntimeError("Legacy Local Workspace relation did not post to /api/query-jobs.")
    result_root = cell.locator("[data-cell-result]").first
    await page.wait_for_function(
        """
        (cell) => {
          if (!(cell instanceof HTMLElement)) {
            return false;
          }
          const runButton = cell.querySelector("[data-run-cell]");
          return !cell.classList.contains("is-query-running")
            && runButton instanceof HTMLButtonElement
            && !runButton.classList.contains("is-running")
            && runButton.textContent.includes("Run Cell");
        }
        """,
        arg=await cell.element_handle(),
        timeout=timeout_ms,
    )
    await result_root.get_by_text("Zurich Central Tax Office").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def enable_source_existence_validation(page, timeout_ms: int) -> None:
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


async def assert_missing_local_alias_blocks_run(
    page,
    timeout_ms: int,
    query_job_posts: list[str],
) -> None:
    await enable_source_existence_validation(page, timeout_ms)
    missing_alias = "local.test.test_sub.missing_federal_tax_file.csv"
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
        f"select * from {missing_alias}",
    )
    await page.wait_for_function(
        """
        (missingAlias) => {
          const cell = document.querySelector('[data-query-cell]');
          const indicator = cell?.querySelector('[data-query-source-validation]');
          const runButton = cell?.querySelector('[data-run-cell]');
          return indicator?.dataset.querySourceValidationStatus === 'invalid'
            && (indicator.textContent || '').includes(missingAlias)
            && runButton instanceof HTMLButtonElement
            && runButton.disabled
            && (runButton.title || '').includes(missingAlias);
        }
        """,
        arg=missing_alias,
        timeout=timeout_ms,
    )

    query_job_posts.clear()
    await cell.evaluate(
        """
        (cell) => {
          const form = cell.querySelector("[data-query-form]");
          if (!(form instanceof HTMLFormElement)) {
            throw new Error("The query form could not be located.");
          }
          form.requestSubmit();
        }
        """
    )
    await page.wait_for_timeout(1000)
    if query_job_posts:
        raise RuntimeError("A missing readable Local Workspace alias still posted to /api/query-jobs.")


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        console_messages: list[str] = []
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))
        query_job_posts: list[str] = []
        page.on(
            "request",
            lambda request: query_job_posts.append(request.url)
            if request.method == "POST" and request.url.endswith("/api/query-jobs")
            else None,
        )

        stage = "open Local Workspace CSV ingestor"
        try:
            stage = "open Local Workspace CSV ingestor"
            await open_local_csv_ingestor(page, args.base_url, args.timeout_ms)
            stage = "import Local Workspace CSV"
            relation = await import_local_csv(page, args.timeout_ms)
            expected_alias = "local.test.test_sub.playwright_local_query.csv"
            stage = "open uploaded Local Workspace source in new notebook without pre-run sync"
            await assert_upload_query_link_opens_without_pre_run_sync(page, args.timeout_ms)
            stage = "assert Local Workspace query notebook"
            await assert_local_query_notebook(page, relation, expected_alias, args.timeout_ms)
            stage = "assert Local Workspace sidebar actions"
            await assert_local_workspace_sidebar_actions(page, relation, args.timeout_ms)
            stage = "run generated Local Workspace query"
            await run_local_query(page, args.timeout_ms)
            stage = "run legacy Local Workspace relation"
            await assert_legacy_local_relation_still_runs(
                page,
                relation,
                args.timeout_ms,
                query_job_posts,
            )
            stage = "block missing Local Workspace alias"
            await assert_missing_local_alias_blocks_run(page, args.timeout_ms, query_job_posts)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(f"{stage}: {exc}", file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright Local Workspace query handoff smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

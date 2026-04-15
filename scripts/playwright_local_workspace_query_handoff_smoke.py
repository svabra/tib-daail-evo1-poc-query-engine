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
    await page.locator('[data-ingestion-tile="csv"]').click()
    await page.locator("[data-csv-ingestion-form]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def import_local_csv(page, timeout_ms: int) -> str:
    file_name = "playwright-local-query.csv"
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

    await query_button.click()
    return relation


async def assert_local_query_notebook(page, expected_relation: str, timeout_ms: int) -> None:
    await page.locator("[data-workspace-notebook]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    if not page.url.rstrip("/").endswith("/query-workbench"):
        raise RuntimeError(f"Expected Query Workbench URL after handoff, got {page.url}.")

    selected_source = page.locator(
        f'[data-source-object].is-selected[data-source-option-id="workspace.local"][data-source-object-relation="{expected_relation}"]'
    )
    await selected_source.wait_for(state="visible", timeout=timeout_ms)

    editor = page.locator("[data-query-cell] [data-editor-source]").first
    sql_text = (await editor.input_value()).strip()
    if expected_relation not in sql_text:
        raise RuntimeError("The new notebook SQL does not reference the Local Workspace relation.")

    shared_workspace_label = page.locator(
        '[data-source-catalog-source-id="workspace.s3"] > summary .source-node-label span'
    ).first
    await shared_workspace_label.wait_for(state="visible", timeout=timeout_ms)
    shared_workspace_text = (await shared_workspace_label.text_content() or "").strip()
    if shared_workspace_text != "Shared Workspace (S3)":
        raise RuntimeError(
            f"Unexpected Shared Workspace sidebar label: {shared_workspace_text!r}"
        )


async def assert_local_workspace_sidebar_actions(page, expected_relation: str, timeout_ms: int) -> None:
    source_object = page.locator(
        f'[data-source-object][data-source-option-id="workspace.local"][data-source-object-relation="{expected_relation}"]'
    ).first
    await source_object.wait_for(state="visible", timeout=timeout_ms)
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
        "Move local file",
        "Download local file",
        "Delete local file",
    ):
        item = menu_panel.get_by_role("button", name=label)
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

        try:
            await open_local_csv_ingestor(page, args.base_url, args.timeout_ms)
            relation = await import_local_csv(page, args.timeout_ms)
            await assert_local_query_notebook(page, relation, args.timeout_ms)
            await assert_local_workspace_sidebar_actions(page, relation, args.timeout_ms)
            await run_local_query(page, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
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

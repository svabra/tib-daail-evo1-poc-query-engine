from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the sidebar 'Query in new notebook' action in the browser "
            "using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def open_query_workbench(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-sidebar]").wait_for(
        state="visible",
        timeout=timeout_ms,
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


async def query_source_in_new_notebook(page, timeout_ms: int) -> tuple[str, str, str]:
    first_visible_source = await page.evaluate(
        """
        () => {
            const sourceObjects = Array.from(document.querySelectorAll('[data-source-object]'));
            const firstVisible = sourceObjects.find((node) => node instanceof HTMLElement && node.offsetParent !== null);
            if (!(firstVisible instanceof HTMLElement)) {
                return null;
            }

            const menu = firstVisible.querySelector('[data-source-action-menu]');
            if (menu instanceof HTMLDetailsElement) {
                menu.open = true;
                menu.setAttribute('open', '');
            }

            return {
                relation: String(firstVisible.dataset.sourceObjectRelation || '').trim(),
                name: String(firstVisible.dataset.sourceObjectName || '').trim(),
            };
        }
        """
    )
    if not first_visible_source:
        raise RuntimeError("No visible source object was available for the smoke test.")

    relation = str(first_visible_source.get("relation") or "").strip()
    if not relation:
        raise RuntimeError("The first source object did not expose a queryable relation.")

    previous_notebook_id = (
        await page.locator("[data-notebook-meta]").first.get_attribute("data-notebook-id")
    ) or ""

    action = page.locator("[data-query-source-new]:visible").first
    await action.wait_for(state="visible", timeout=timeout_ms)
    await action.click()

    await page.wait_for_function(
        """
        ({ previousNotebookId, expectedRelation }) => {
          const meta = document.querySelector('[data-notebook-meta]');
          const textarea = document.querySelector('[data-query-cell] [data-editor-source]');
          return Boolean(
            meta &&
            meta.dataset.notebookId &&
            meta.dataset.notebookId !== previousNotebookId &&
            meta.dataset.canEdit === 'true' &&
            textarea instanceof HTMLTextAreaElement &&
            textarea.value.includes(expectedRelation)
          );
        }
        """,
        arg={
            "previousNotebookId": previous_notebook_id,
            "expectedRelation": relation,
        },
        timeout=timeout_ms,
    )

    notebook_id = (
        await page.locator("[data-notebook-meta]").first.get_attribute("data-notebook-id")
    ) or ""
    if not notebook_id.startswith("local-notebook-"):
        raise RuntimeError(
            "The sidebar action did not open a new local notebook."
        )

    sql_text = await page.locator("[data-query-cell] [data-editor-source]").first.input_value()
    if relation not in sql_text:
        raise RuntimeError(
            "The new notebook query did not target the selected source relation."
        )

    return relation, notebook_id, sql_text


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        console_messages: list[str] = []
        page_errors: list[str] = []
        page.on(
            "console",
            lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"),
        )
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        try:
            await open_query_workbench(page, args.base_url, args.timeout_ms)
            await expand_source_tree(page)
            relation, notebook_id, _ = await query_source_in_new_notebook(
                page,
                args.timeout_ms,
            )
            if page_errors:
                raise RuntimeError(
                    "The browser reported a page error during the query-in-new-notebook flow."
                )
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            for page_error in page_errors:
                print(f"pageerror:{page_error}", file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(
        "Playwright query-in-new-notebook smoke passed "
        f"for relation {relation} and notebook {notebook_id}."
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
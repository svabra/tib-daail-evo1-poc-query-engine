from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime, timedelta

from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise the Query Workbench latest-notebooks-used panel."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def open_query_workbench_entry(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-query-workbench-entry-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def main() -> None:
    args = parse_args()
    console_messages: list[str] = []
    page_errors: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context()
        await context.add_init_script(
            """
            () => {
              window.localStorage.removeItem("bdw.lastNotebook.v1");
            }
            """
        )
        page = await context.new_page()
        page.on("console", lambda message: console_messages.append(f"{message.type}: {message.text}"))
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        try:
            await open_query_workbench_entry(page, args.base_url, args.timeout_ms)
            notebooks = await page.evaluate(
                """
                () => {
                  const seen = new Set();
                  return Array.from(document.querySelectorAll(
                    '[data-query-workbench-entry-page] .query-entry-browser [data-notebook-id], '
                    + '[data-query-workbench-entry-page] .query-entry-shared-list [data-notebook-id]'
                  ))
                    .map((node) => ({
                      notebookId: String(node.dataset.notebookId || "").trim(),
                      title: String(node.dataset.notebookTitle || node.textContent || "").trim(),
                    }))
                    .filter((entry) => {
                      if (!entry.notebookId || seen.has(entry.notebookId)) {
                        return false;
                      }
                      seen.add(entry.notebookId);
                      return true;
                    })
                    .slice(0, 6);
                }
                """
            )
            if len(notebooks) < 5:
                raise RuntimeError(
                    f"The recent-notebooks smoke requires at least 5 notebooks; found {len(notebooks)}."
                )

            selected = notebooks[:6]
            base_time = datetime.now(UTC) - timedelta(minutes=len(selected))
            activity = {}
            for index, notebook in enumerate(selected):
                title = f"Recent Smoke Notebook {index + 1}"
                activity[notebook["notebookId"]] = {
                    "notebookId": notebook["notebookId"],
                    "title": title,
                    "summary": f"Recent smoke summary {index + 1}",
                    "touchedAt": (base_time + timedelta(minutes=index)).isoformat(),
                    "reason": "run" if index % 2 else "edited",
                }

            expected_entries = list(reversed(selected[-5:]))
            expected_titles = [
                activity[entry["notebookId"]]["title"] for entry in expected_entries
            ]
            oldest_title = activity[selected[0]["notebookId"]]["title"]
            newest_notebook_id = expected_entries[0]["notebookId"]

            await page.evaluate(
                """
                (activity) => {
                  window.localStorage.setItem("bdw.notebookActivity.v1", JSON.stringify(activity));
                  window.localStorage.removeItem("bdw.lastNotebook.v1");
                }
                """,
                activity,
            )
            await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
            await page.locator("[data-query-workbench-entry-page]").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                """
                (expectedTitles) => {
                  const buttons = Array.from(document.querySelectorAll(
                    '[data-query-entry-recent-notebooks] [data-open-recent-notebook]'
                  ));
                  const titles = buttons.map((button) =>
                    button.querySelector('.query-entry-recent-title')?.textContent?.trim()
                  );
                  return buttons.length === expectedTitles.length
                    && titles.every((title, index) => title === expectedTitles[index]);
                }
                """,
                arg=expected_titles,
                timeout=args.timeout_ms,
            )
            rendered_text = await page.locator("[data-query-entry-recent-notebooks]").inner_text()
            if oldest_title in rendered_text and len(selected) > 5:
                raise RuntimeError("The Query Workbench recent list rendered more than five notebooks.")

            first_recent = page.locator("[data-query-entry-recent-notebooks] [data-open-recent-notebook]").first
            await first_recent.click()
            await page.wait_for_function(
                """
                (notebookId) => window.location.pathname === `/notebooks/${encodeURIComponent(notebookId)}`
                """,
                arg=newest_notebook_id,
                timeout=args.timeout_ms,
            )
        except Exception:
            if page_errors:
                print("Page errors:")
                for error in page_errors:
                    print(f"  {error}")
            if console_messages:
                print("Recent console messages:")
                for message in console_messages[-25:]:
                    print(f"  {message}")
            raise
        finally:
            await context.close()
            await browser.close()

    print("Playwright Query Workbench recent notebooks smoke passed.")


if __name__ == "__main__":
    asyncio.run(main())

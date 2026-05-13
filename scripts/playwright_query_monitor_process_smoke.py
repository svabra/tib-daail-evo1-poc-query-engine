from __future__ import annotations

import argparse
import asyncio
from uuid import uuid4

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright


LONG_SQL = """
select sum(a.i * b.j) as total_value
from range(2000000) as a(i)
cross join range(2000) as b(j)
"""

SMOKE_NOTEBOOK_ID = "s3-smoke-test"
SMOKE_CELL_PREFIX = "query-monitor-process-smoke"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise process-backed query monitoring, simultaneous query execution, "
            "resource metrics, and cancellation visibility."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def ensure_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/notebooks/{SMOKE_NOTEBOOK_ID}",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-query-cell]").first.wait_for(state="visible", timeout=timeout_ms)


async def ensure_monitor_query_cells(page, cell_ids: list[str], timeout_ms: int) -> None:
    await page.locator("[data-query-cell]").first.wait_for(state="attached", timeout=timeout_ms)
    await page.evaluate(
        """
        (cellIds) => {
          const first = document.querySelector("[data-query-cell]");
          if (!(first instanceof HTMLElement)) {
            throw new Error("A query cell is required for the monitor smoke.");
          }
          document.querySelectorAll("[data-monitor-smoke-cell]").forEach((node) => node.remove());
          let insertAfter = first;
          cellIds.forEach((cellId, index) => {
            const clone = first.cloneNode(true);
            clone.dataset.cellId = cellId;
            clone.dataset.monitorSmokeCell = "true";
            clone.dataset.defaultCellSources = "";
            clone.dataset.defaultCellLanguage = "sql";
            clone.classList.remove("is-active", "is-query-running");
            const label = clone.querySelector(".cell-label");
            if (label) {
              label.textContent = `Cell ${index + 1}`;
            }
            const cellInput = clone.querySelector('input[name="cell_id"]');
            if (cellInput instanceof HTMLInputElement) {
              cellInput.value = cellId;
            }
            const result = clone.querySelector("[data-cell-result]");
            if (result instanceof HTMLElement) {
              result.id = `query-results-${cellId}`;
              result.dataset.queryJobId = "";
              result.hidden = true;
              result.innerHTML = "";
            }
            const editorRoot = clone.querySelector("[data-editor-root]");
            if (editorRoot instanceof HTMLElement) {
              editorRoot.dataset.editorName = `sql-${cellId}`;
              editorRoot.dataset.editorLanguage = "sql";
            }
            const textarea = clone.querySelector("[data-editor-source]");
            if (textarea instanceof HTMLTextAreaElement) {
              textarea.dataset.editorLanguage = "sql";
              textarea.dataset.defaultSql = "";
              textarea.value = "";
            }
            clone.querySelectorAll("[data-cell-source-option]").forEach((option) => {
              if (option instanceof HTMLInputElement) {
                option.checked = false;
              }
            });
            insertAfter.after(clone);
            insertAfter = clone;
          });
        }
        """,
        cell_ids,
    )
    await page.wait_for_function(
        """
        (expectedCount) => document.querySelectorAll("[data-monitor-smoke-cell]").length === expectedCount
        """,
        arg=len(cell_ids),
        timeout=timeout_ms,
    )


async def start_two_queries(page, cell_ids: list[str], timeout_ms: int) -> None:
    await ensure_monitor_query_cells(page, cell_ids, timeout_ms)
    snapshots = await page.evaluate(
        """
        async ({ sql, cellIds }) => {
          const cells = cellIds.map((cellId) =>
            Array.from(document.querySelectorAll("[data-monitor-smoke-cell]"))
              .find((cell) => cell.dataset.cellId === cellId)
          );
          if (cells.length < 2) {
            throw new Error("Expected at least two query cells.");
          }
          for (const cell of cells) {
            cell.dataset.defaultCellSources = "";
            cell.querySelectorAll("[data-cell-source-option]").forEach((option) => {
              if (option instanceof HTMLInputElement) {
                option.checked = false;
              }
            });
            const textarea = cell.querySelector("[data-editor-source]");
            if (!(textarea instanceof HTMLTextAreaElement)) {
              throw new Error("A query editor source could not be located.");
            }
            textarea.value = sql;
            textarea.dispatchEvent(new Event("input", { bubbles: true }));
            textarea.dispatchEvent(new Event("change", { bubbles: true }));
          }
          for (const cell of cells) {
            const form = cell.querySelector("[data-query-form]");
            if (!(form instanceof HTMLFormElement)) {
              throw new Error("A query form could not be located.");
            }
          }
          const notebookTitle =
            document.querySelector("[data-notebook-title-display]")?.textContent?.trim() ||
            "Query Monitor Smoke";
          return Promise.all(cells.map(async (cell) => {
            const form = cell.querySelector("[data-query-form]");
            const formData = new FormData(form);
            formData.set("sql", sql);
            formData.set("notebook_id", formData.get("notebook_id") || "");
            formData.set("cell_id", cell.dataset.cellId || "");
            formData.set("notebook_title", notebookTitle);
            formData.set("data_sources", "");
            const response = await fetch("/api/query-jobs", {
              method: "POST",
              body: formData,
              headers: { Accept: "application/json" },
            });
            if (!response.ok) {
              throw new Error(`Query job creation failed with status ${response.status}.`);
            }
            return response.json();
          }));
        }
        """,
        {"sql": LONG_SQL, "cellIds": cell_ids},
    )
    if len(snapshots or []) < 2:
        raise RuntimeError(f"Expected two query job snapshots, received {snapshots!r}.")


async def wait_for_two_live_monitor_items(page, timeout_ms: int) -> list[int]:
    for _attempt in range(5):
        try:
            await page.evaluate(
                """
                () => {
                  const monitor = document.querySelector("[data-query-monitor-section]");
                  if (monitor instanceof HTMLDetailsElement) {
                    monitor.open = true;
                    monitor.setAttribute("open", "");
                  }
                }
                """
            )
            break
        except PlaywrightError:
            await page.wait_for_timeout(500)
    await page.wait_for_function(
        """
        () => {
          const items = Array.from(document.querySelectorAll(".query-monitor-item-running"));
          const liveItems = items.filter((item) => {
            const text = item.textContent || "";
            return /PID\\s+\\d+/.test(text) && /CPU/.test(text) && /RAM/.test(text);
          });
          const pids = liveItems.map((item) => {
            const match = (item.textContent || "").match(/PID\\s+(\\d+)/);
            return match ? match[1] : "";
          }).filter(Boolean);
          return liveItems.length >= 2 && new Set(pids).size >= 2;
        }
        """,
        timeout=timeout_ms,
    )
    return await page.evaluate(
        """
        () => Array.from(document.querySelectorAll(".query-monitor-item-running"))
          .map((item) => {
            const match = (item.textContent || "").match(/PID\\s+(\\d+)/);
            return match ? Number(match[1]) : 0;
          })
          .filter(Boolean)
          .slice(0, 2)
        """
    )


async def cancel_first_query_and_assert_visibility(page, timeout_ms: int) -> None:
    await page.locator(".query-monitor-item-running [data-cancel-query-job]").first.wait_for(
        state="attached",
        timeout=timeout_ms,
    )
    await page.evaluate(
        """
        () => {
          const button = document.querySelector(".query-monitor-item-running [data-cancel-query-job]");
          if (!(button instanceof HTMLButtonElement)) {
            throw new Error("No query monitor cancel button is attached.");
          }
          button.click();
        }
        """
    )

    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll(".query-monitor-item"))
          .some((item) => /Cancelling|Interrupting|Stopping|Hard-stopping/.test(item.textContent || ""))
        """,
        timeout=timeout_ms,
    )

    notifications = page.locator("[data-query-notifications]")
    await notifications.locator("summary").click()
    await page.wait_for_function(
        """
        () => {
          const text = document.querySelector("[data-query-notification-list]")?.textContent || "";
          return /Cancellation requested|Interrupting|Stopping|Hard-stopping|cancelled/i.test(text);
        }
        """,
        timeout=timeout_ms,
    )
    await notifications.locator("summary").click()

    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll("[data-cell-result]"))
          .some((result) => /Cancellation requested|Interrupting|Stopping|Hard-stopping|Query cancelled successfully/i.test(result.textContent || ""))
        """,
        timeout=timeout_ms,
    )

    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll("[data-cell-result]"))
          .some((result) => /Query cancelled successfully/i.test(result.textContent || ""))
        """,
        timeout=timeout_ms,
    )


async def cancel_remaining_queries(page) -> None:
    await page.evaluate(
        """
        () => {
          for (const button of document.querySelectorAll("[data-cancel-query-job]")) {
            if (button instanceof HTMLButtonElement && !button.disabled) {
              button.click();
            }
          }
          for (const button of document.querySelectorAll("[data-cancel-query]")) {
            if (button instanceof HTMLButtonElement && !button.disabled) {
              button.click();
            }
          }
        }
        """
    )


async def run() -> None:
    args = parse_args()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1280, "height": 900})
        try:
            cell_ids = [
                f"{SMOKE_CELL_PREFIX}-{uuid4().hex[:8]}-1",
                f"{SMOKE_CELL_PREFIX}-{uuid4().hex[:8]}-2",
            ]
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            await start_two_queries(page, cell_ids, args.timeout_ms)
            pids = await wait_for_two_live_monitor_items(page, args.timeout_ms)
            if len(set(pids)) < 2:
                raise RuntimeError(f"Expected two distinct worker PIDs, received {pids!r}.")
            await cancel_first_query_and_assert_visibility(page, args.timeout_ms)
            print(f"Playwright query monitor process smoke passed for worker PIDs: {pids}.")
        finally:
            await cancel_remaining_queries(page)
            await browser.close()


if __name__ == "__main__":
    asyncio.run(run())

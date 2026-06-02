from __future__ import annotations

import argparse
import asyncio
import json
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


SMOKE_SQL = "select 1 as cache_hydration_smoke"
FAILURE_DETAIL = "Query cache hydration failed: DuckDB cache hydrate failed"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the notebook Hydrate cache toggle when the backend returns a "
            "structured cache hydration failure."
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
    await page.wait_for_timeout(1500)
    query_cells = page.locator("[data-query-cell]:visible")
    if await query_cells.count():
        await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
        return

    create_button = page.locator(
        "[data-query-workbench-entry-page] [data-create-notebook]",
        has_text="Create New Workbench",
    ).first
    await create_button.wait_for(state="visible", timeout=timeout_ms)
    await create_button.scroll_into_view_if_needed(timeout=timeout_ms)
    await create_button.click(force=True)
    await query_cells.first.wait_for(state="visible", timeout=timeout_ms)


async def prepare_cell(page, timeout_ms: int) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.wait_for(state="visible", timeout=timeout_ms)
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

          const switchButton = cell.querySelector("[data-cache-hydration-switch]");
          if (!(switchButton instanceof HTMLButtonElement)) {
            throw new Error("The Hydrate cache switch could not be located.");
          }
          switchButton.disabled = false;
          switchButton.removeAttribute("aria-busy");
          switchButton.setAttribute("aria-checked", "false");
          const stateLabel = cell.querySelector("[data-cache-hydration-state-label]");
          if (stateLabel) {
            stateLabel.textContent = "Off";
          }
          const cacheRoot = cell.querySelector("[data-cell-cache-hydration]");
          if (cacheRoot instanceof HTMLElement) {
            cacheRoot.dataset.cacheHydrationState = "off";
          }
        }
        """,
        SMOKE_SQL,
    )


async def assert_failed_hydration_is_rendered(page, timeout_ms: int) -> None:
    requests: list[dict[str, object]] = []

    async def fail_rehydrate(route):
        try:
            requests.append(json.loads(route.request.post_data or "{}"))
        except json.JSONDecodeError:
            requests.append({})
        await route.fulfill(
            status=500,
            content_type="application/json",
            body=json.dumps({"detail": FAILURE_DETAIL}),
        )

    await page.route("**/api/query-cache/rehydrate", fail_rehydrate)
    try:
        switch_button = page.locator("[data-query-cell]:visible [data-cache-hydration-switch]").first
        await switch_button.wait_for(state="visible", timeout=timeout_ms)
        await switch_button.click()
        await page.wait_for_function(
            """
            (failureDetail) => {
              const cell = document.querySelector("[data-query-cell]");
              const cacheRoot = cell?.querySelector("[data-cell-cache-hydration]");
              const stateLabel = cell?.querySelector("[data-cache-hydration-state-label]");
              const badge = cell?.querySelector("[data-cache-hydration-badge]");
              return cacheRoot?.dataset.cacheHydrationState === "error"
                && (stateLabel?.textContent || "").includes("Error")
                && (badge?.title || "").includes(failureDetail);
            }
            """,
            arg=FAILURE_DETAIL,
            timeout=timeout_ms,
        )
    finally:
        await page.unroute("**/api/query-cache/rehydrate", fail_rehydrate)

    if not requests:
        raise RuntimeError("The Hydrate cache toggle did not call /api/query-cache/rehydrate.")
    payload = requests[-1]
    if payload.get("sql") != SMOKE_SQL:
        raise RuntimeError(f"Unexpected cache hydration SQL payload: {payload!r}")
    mode = (
        ((payload.get("queryOptions") or {}).get("duckdb") or {})
        .get("cacheHydration", {})
        .get("mode")
    )
    if mode != "on":
        raise RuntimeError(f"Hydrate cache payload did not enable cache mode: {payload!r}")


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            base_url=args.base_url.rstrip("/"),
        )
        page = await context.new_page()
        console_messages: list[str] = []
        page.on(
            "console",
            lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}")
            if msg.type in {"error", "warning"}
            else None,
        )
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            await prepare_cell(page, args.timeout_ms)
            await assert_failed_hydration_is_rendered(page, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await context.close()
            await browser.close()
            return 1

        relevant_console_errors = [
            message
            for message in console_messages
            if "Query cache hydration failed" not in message
            and "Failed to load resource: the server responded with a status of 500" not in message
        ]
        await context.close()
        await browser.close()

    if relevant_console_errors:
        print("\n".join(relevant_console_errors), file=sys.stderr)
        return 1
    print("Playwright cache hydration failure regression passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

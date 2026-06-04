from __future__ import annotations

import argparse
import asyncio
import json
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


CACHE_SQL = "select count(*) as row_count from s3.kbpoimports.kbpo2020.parquet"
CACHE_ALIAS = "s3.kbpoimports.kbpo2020.parquet"
SOURCE_RELATION = "kbpoimports.kbpo2020_521a28d3"
S3_PATH = "s3://kbpoimports/kbpo2020.parquet"
NO_SOURCE_COPY = "No known S3 Parquet source relation is referenced by this cell"
MISSING_SOURCE_COPY = "Referenced source(s) were not found"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the notebook Hydrate cache success path in the browser. "
            "The cache API responses are mocked so the test verifies the UI flow "
            "without requiring a live S3 bucket."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


def cache_status_payload() -> dict[str, object]:
    return {
        "enabled": True,
        "status": "ready",
        "freshnessWindowSeconds": 120,
        "cacheRoot": "/workspace/query-cache",
        "copy": (
            "Copies the S3 Parquet data referenced by this cell into a temporary "
            "local DuckDB table before the query runs."
        ),
        "ephemeralWarning": (
            "This runtime cache table lives in temporary compute storage and can "
            "disappear after a pod restart."
        ),
        "rawSqlLimitation": (
            "Hydrate cache applies to known S3 Parquet sources selected in the notebook. "
            "Direct read_parquet('s3://...') calls are not rewritten in this version."
        ),
        "sources": [
            {
                "relation": SOURCE_RELATION,
                "sourceViewRelation": SOURCE_RELATION,
                "queryAlias": CACHE_ALIAS,
                "path": S3_PATH,
                "sourceRevision": "playwright-revision-1",
                "sourceSizeBytes": 10_737_418_240,
                "cacheTable": "cache_kbpo2020_playwright",
                "rowCount": 12345,
                "cacheSizeBytes": 4_194_304,
                "indexColumns": ["taxpayer_id"],
                "lastCheckedAt": "2026-06-04T12:00:00Z",
                "lastHydratedAt": "2026-06-04T12:00:10Z",
                "expectedBehavior": (
                    "The cell checks this cache before it runs and rebuilds it if "
                    "it is missing, stale, or expired."
                ),
                "temporaryWarning": (
                    "This runtime cache table lives in temporary compute storage."
                ),
                "status": "hit",
                "statusLabel": "Cache hit",
                "statusReason": (
                    "A local DuckDB cache table exists and matches the current "
                    "S3 source revision."
                ),
            }
        ],
        "unsupportedSources": [],
        "checkedAt": "2026-06-04T12:00:20Z",
    }


async def ensure_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)
    query_cells = page.locator("[data-query-cell]:visible")
    create_button = page.locator(
        "[data-query-workbench-entry-page] [data-create-notebook]",
        has_text="Create New Workbench",
    ).first

    while asyncio.get_running_loop().time() < deadline:
        if await query_cells.count():
            await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
            return
        if await create_button.count():
            try:
                await create_button.scroll_into_view_if_needed(timeout=2000)
                await create_button.click(force=True, timeout=2000)
            except PlaywrightTimeoutError:
                pass
            await page.wait_for_timeout(1200)
            continue
        await page.wait_for_timeout(500)

    raise RuntimeError("A visible query notebook cell was not available after creating a workbench.")


async def prepare_cacheable_cell(page, timeout_ms: int) -> None:
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

          const editorRoot = cell.querySelector("[data-editor-root]");
          if (editorRoot instanceof HTMLElement) {
            editorRoot.dataset.editorLanguage = "sql";
          }
          cell.dataset.defaultCellLanguage = "sql";

          const notebookMeta = cell.closest("[data-workspace-notebook]")
            ?.querySelector("[data-notebook-meta]");
          if (notebookMeta instanceof HTMLElement) {
            notebookMeta.dataset.canEdit = "true";
          }

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
        CACHE_SQL,
    )


def assert_cache_payload(payload: dict[str, object], *, expected_mode: str) -> None:
    if payload.get("sql") != CACHE_SQL:
        raise RuntimeError(f"Unexpected cache SQL payload: {payload!r}")
    query_options = payload.get("queryOptions") or {}
    duckdb_options = (
        query_options.get("duckdb") if isinstance(query_options, dict) else {}
    ) or {}
    cache_options = (
        duckdb_options.get("cacheHydration")
        if isinstance(duckdb_options, dict)
        else {}
    ) or {}
    mode = cache_options.get("mode") if isinstance(cache_options, dict) else None
    if mode != expected_mode:
        raise RuntimeError(
            f"Hydrate cache payload mode was {mode!r}; expected {expected_mode!r}."
        )
    scope = cache_options.get("scope") if isinstance(cache_options, dict) else None
    if scope != "referencedS3Parquet":
        raise RuntimeError(f"Unexpected cache hydration scope: {payload!r}")
    local_relations = payload.get("localRelations")
    if local_relations not in ({}, None):
        raise RuntimeError(f"S3 cache payload should not carry local relations: {payload!r}")


async def assert_cache_plan_dialog(page, timeout_ms: int) -> None:
    preview_requests: list[dict[str, object]] = []
    rehydrate_requests: list[dict[str, object]] = []

    async def fulfill_preview(route):
        try:
            payload = json.loads(route.request.post_data or "{}")
        except json.JSONDecodeError:
            payload = {}
        preview_requests.append(payload)
        await route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(cache_status_payload()),
        )

    async def fulfill_rehydrate(route):
        try:
            payload = json.loads(route.request.post_data or "{}")
        except json.JSONDecodeError:
            payload = {}
        rehydrate_requests.append(payload)
        await route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(
                {
                    **cache_status_payload(),
                    "hydration": {
                        "enabled": True,
                        "status": "ready",
                        "sources": cache_status_payload()["sources"],
                    },
                }
            ),
        )

    await page.route("**/api/query-cache/preview", fulfill_preview)
    await page.route("**/api/query-cache/rehydrate", fulfill_rehydrate)
    try:
        switch_button = page.locator(
            "[data-query-cell]:visible [data-cache-hydration-switch]"
        ).first
        await switch_button.wait_for(state="visible", timeout=timeout_ms)
        await switch_button.click()
        await page.wait_for_function(
            """
            () => {
              const cell = document.querySelector("[data-query-cell]");
              const cacheRoot = cell?.querySelector("[data-cell-cache-hydration]");
              const stateLabel = cell?.querySelector("[data-cache-hydration-state-label]");
              return cacheRoot?.dataset.cacheHydrationState === "hit"
                && (stateLabel?.textContent || "").includes("Hit");
            }
            """,
            timeout=timeout_ms,
        )

        details_button = page.locator(
            "[data-query-cell]:visible [data-cache-hydration-details]"
        ).first
        await details_button.wait_for(state="visible", timeout=timeout_ms)
        await details_button.click()

        dialog = page.locator("[data-cache-hydration-dialog]").first
        await dialog.wait_for(state="visible", timeout=timeout_ms)
        body = dialog.locator("[data-cache-hydration-dialog-body]").first
        await body.get_by_text(SOURCE_RELATION, exact=False).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        await body.get_by_text(S3_PATH, exact=False).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        await body.get_by_text("Cache hit", exact=False).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )

        body_text = await body.inner_text()
        if NO_SOURCE_COPY in body_text:
            raise RuntimeError("Cache plan dialog rendered the no-source fallback.")
        if MISSING_SOURCE_COPY in body_text:
            raise RuntimeError("Cache plan dialog rendered a missing-source error.")
    finally:
        await page.unroute("**/api/query-cache/preview", fulfill_preview)
        await page.unroute("**/api/query-cache/rehydrate", fulfill_rehydrate)

    if not rehydrate_requests:
        raise RuntimeError("The Hydrate cache toggle did not call /api/query-cache/rehydrate.")
    if not preview_requests:
        raise RuntimeError("The Cache hydration details dialog did not call /api/query-cache/preview.")
    assert_cache_payload(rehydrate_requests[-1], expected_mode="on")
    assert_cache_payload(preview_requests[-1], expected_mode="on")


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
            await prepare_cacheable_cell(page, args.timeout_ms)
            await assert_cache_plan_dialog(page, args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await context.close()
            await browser.close()
            return 1

        await context.close()
        await browser.close()

    if console_messages:
        print("\n".join(console_messages), file=sys.stderr)
        return 1
    print(f"Playwright cache hydration plan regression passed for {CACHE_ALIAS}.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

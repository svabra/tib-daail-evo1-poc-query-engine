from __future__ import annotations

import argparse
import asyncio
import sys
from urllib.parse import urljoin

from playwright.async_api import async_playwright


FIXTURE_SQL = "select * from s3.nav_fixture.sample.parquet"
DEFAULT_NOTEBOOK_PATH = "/notebooks/mwa-abrechnung-s3-parquet"
FIXTURE_SOURCE_OBJECT = {
    "label": "sample.parquet",
    "kind": "s3-object",
    "sourceId": "workspace.s3",
    "relation": "nav_fixture.sample",
    "queryAlias": "s3.nav_fixture.sample.parquet",
    "queryReference": 's3.nav_fixture."sample.parquet"',
    "bucket": "nav-fixture",
    "key": "sample.parquet",
    "path": "s3://nav-fixture/sample.parquet",
    "format": "parquet",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the per-cell Navigate to source object button and verify "
            "that it reveals and flashes the referenced object in the Data "
            "Sources sidebar."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


def fixture_sidebar_markup() -> str:
    return """
<aside id="sidebar" class="sidebar" data-sidebar data-source-browser-scope data-sidebar-mode="notebook">
  <div class="sidebar-header">
    <button type="button" class="sidebar-toggle sidebar-toggle-top" data-sidebar-toggle aria-expanded="true" aria-controls="sidebar" title="Collapse navigation">
      <span class="sidebar-toggle-icon" aria-hidden="true"></span>
      <span class="sidebar-toggle-label">Collapse navigation</span>
    </button>
  </div>
  <div class="sidebar-body" data-sidebar-body>
    <div class="sidebar-search">
      <label class="sidebar-label" for="sidebar-search-input">Search</label>
      <input id="sidebar-search-input" class="search-input" type="search" placeholder="Search notebooks and data sources" data-sidebar-search>
    </div>
    <details class="sidebar-section notebook-section" data-notebook-section open>
      <summary class="section-heading section-summary">
        <h2>Notebooks</h2>
        <div class="section-tools"><span class="section-count">0</span></div>
      </summary>
      <div class="notebook-tree" data-notebook-tree></div>
    </details>
    <div class="sidebar-divider" aria-hidden="true"></div>
    <details class="sidebar-section" data-data-sources-section>
      <summary class="section-heading section-summary">
        <h2>Data Sources</h2>
        <div class="section-tools"><span class="section-count">1</span></div>
      </summary>
      <div class="sidebar-source-operation-status" data-source-operation-status hidden>
        <strong class="sidebar-source-operation-status-title" data-source-operation-status-title></strong>
        <span class="sidebar-source-operation-status-copy" data-source-operation-status-copy></span>
      </div>
      <div class="source-tree" data-source-tree data-source-tree-scope="sidebar">
        <details class="source-catalog" data-source-catalog data-source-catalog-name="workspace" data-source-catalog-source-id="workspace.s3">
          <summary data-searchable-item="Shared Workspace S3">
            <span class="source-node-label"><span>Shared Workspace (S3)</span></span>
          </summary>
          <details class="source-schema" data-source-schema data-source-schema-key="workspace::nav-fixture" data-source-bucket="nav-fixture" data-source-schema-name="nav-fixture">
            <summary data-searchable-item="nav-fixture">
              <span class="source-node-label"><span>nav-fixture</span></span>
            </summary>
            <ul class="source-object-list">
              <li
                class="source-object source-object-file"
                data-searchable-item="sample.parquet nav_fixture.sample s3.nav_fixture.sample.parquet s3://nav-fixture/sample.parquet"
                data-source-object
                data-source-object-kind="file"
                data-source-object-name="sample.parquet"
                data-source-object-display-name="sample.parquet"
                data-source-object-relation="nav_fixture.sample"
                data-source-object-query-alias="s3.nav_fixture.sample.parquet"
                data-source-object-query-reference="s3.nav_fixture.&quot;sample.parquet&quot;"
                data-source-object-query-sql="read_parquet('s3://nav-fixture/sample.parquet')"
                data-source-option-id="workspace.s3"
                data-s3-bucket="nav-fixture"
                data-s3-key="sample.parquet"
                data-s3-path="s3://nav-fixture/sample.parquet"
                data-s3-file-format="parquet"
                data-s3-downloadable="true"
                data-s3-size-bytes="4096"
                data-s3-download-kind=""
                data-s3-part-prefix=""
                data-s3-part-file-format=""
                data-s3-part-count="0"
                data-s3-download-filename="sample.parquet"
                data-s3-merge-downloadable="false"
                data-s3-zip-downloadable="false"
                data-published-data-products="[]"
              >
                <span class="source-node-label"><span>sample.parquet</span></span>
              </li>
            </ul>
          </details>
        </details>
      </div>
    </details>
    <div class="sidebar-divider" aria-hidden="true"></div>
    <details class="sidebar-section sidebar-query-monitor" data-query-monitor-section>
      <summary class="section-heading section-summary"><h2>Query Monitor</h2></summary>
      <div class="query-monitor-body"><div class="query-monitor-list" data-query-monitor-list></div></div>
    </details>
  </div>
</aside>
"""


async def install_routes(page) -> None:
    async def handle_prepare(route):
        request = route.request
        if request.method != "POST":
            await route.continue_()
            return

        await route.fulfill(
            status=200,
            content_type="application/json",
            json={
                "displaySql": FIXTURE_SQL,
                "submittedSql": FIXTURE_SQL,
                "executionSql": "select * from read_parquet('s3://nav-fixture/sample.parquet')",
                "dataSources": ["workspace.s3"],
                "queryOptions": {},
                "touchedRelations": ["nav_fixture.sample"],
                "touchedBuckets": ["nav-fixture"],
                "sourceObjects": [FIXTURE_SOURCE_OBJECT],
                "executionMode": "duckdb-read",
                "duckdbExecutionPath": "isolated-read",
            },
        )

    async def handle_sidebar(route):
        await route.fulfill(
            status=200,
            content_type="text/html; charset=utf-8",
            body=fixture_sidebar_markup(),
        )

    await page.route("**/api/query-sql/prepare", handle_prepare)
    await page.route("**/sidebar?**", handle_sidebar)


async def open_query_notebook(page, args: argparse.Namespace) -> None:
    await page.add_init_script(
        """
        () => {
          const keys = [];
          for (let index = 0; index < window.localStorage.length; index += 1) {
            const key = window.localStorage.key(index);
            if (key && key.startsWith("bdw.")) {
              keys.push(key);
            }
          }
          for (const key of keys) {
            window.localStorage.removeItem(key);
          }
        }
        """
    )
    await page.goto(
        urljoin(args.base_url.rstrip("/") + "/", DEFAULT_NOTEBOOK_PATH.lstrip("/")),
        wait_until="commit",
        timeout=args.timeout_ms,
    )

    cell = page.locator("[data-query-cell]:visible").first
    await cell.wait_for(state="visible", timeout=args.timeout_ms)
    await cell.evaluate(
        """
        (cell, sql) => {
          const textarea = cell.querySelector("[data-editor-source]");
          if (!(textarea instanceof HTMLTextAreaElement)) {
            throw new Error("The SQL editor source was not found.");
          }
          textarea.value = sql;
          textarea.dispatchEvent(new Event("input", { bubbles: true }));
          textarea.dispatchEvent(new Event("change", { bubbles: true }));
        }
        """,
        FIXTURE_SQL,
    )
    await page.wait_for_timeout(2500)


async def assert_source_navigation(page, timeout_ms: int) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.hover()
    nav_button = cell.locator("[data-navigate-cell-source]").first
    await nav_button.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """
        (button) => {
          if (!(button instanceof HTMLElement)) {
            return false;
          }
          const style = getComputedStyle(button);
          return style.pointerEvents !== 'none' && Number(style.opacity || 0) > 0.9;
        }
        """,
        arg=await nav_button.element_handle(),
        timeout=timeout_ms,
    )

    await nav_button.click()
    await page.wait_for_function(
        """
        () => {
          const sources = document.querySelector('[data-data-sources-section]');
          const catalog = document.querySelector('[data-source-catalog-source-id="workspace.s3"]');
          const bucket = document.querySelector('[data-source-schema][data-source-bucket="nav-fixture"]');
          const object = document.querySelector('[data-source-object][data-s3-bucket="nav-fixture"][data-s3-key="sample.parquet"]');
          const label = object?.querySelector('.source-node-label > span:last-child') || object?.querySelector('.source-node-label');
          return Boolean(
            sources?.open &&
            catalog?.open &&
            bucket?.open &&
            object?.classList.contains('is-pipeline-inspect-flash') &&
            label?.classList.contains('is-pipeline-target-text-flash')
          );
        }
        """,
        timeout=timeout_ms,
    )

    state = await page.evaluate(
        """
        () => {
          const sources = document.querySelector('[data-data-sources-section]');
          const catalog = document.querySelector('[data-source-catalog-source-id="workspace.s3"]');
          const bucket = document.querySelector('[data-source-schema][data-source-bucket="nav-fixture"]');
          const object = document.querySelector('[data-source-object][data-s3-bucket="nav-fixture"][data-s3-key="sample.parquet"]');
          const label = object?.querySelector('.source-node-label > span:last-child') || object?.querySelector('.source-node-label');
          const shell = document.querySelector('[data-shell]');
          const statusTitle = document.querySelector('[data-source-operation-status-title]')?.textContent?.trim() || '';
          return {
            shellVisible: !shell?.classList.contains('shell-sidebar-hidden'),
            sourcesOpen: Boolean(sources?.open),
            catalogOpen: Boolean(catalog?.open),
            bucketOpen: Boolean(bucket?.open),
            objectFlashing: Boolean(object?.classList.contains('is-pipeline-inspect-flash')),
            labelFlashing: Boolean(label?.classList.contains('is-pipeline-target-text-flash')),
            statusTitle,
          };
        }
        """
    )
    expected = {
        "shellVisible": True,
        "sourcesOpen": True,
        "catalogOpen": True,
        "bucketOpen": True,
        "objectFlashing": True,
        "labelFlashing": True,
        "statusTitle": "Source object located",
    }
    if state != expected:
        raise RuntimeError(
            "Navigate to source object did not reveal and flash the fixture object: "
            f"{state}"
        )


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1100},
            base_url=args.base_url.rstrip("/"),
        )
        console_errors: list[str] = []
        page.on(
            "console",
            lambda message: (
                console_errors.append(message.text)
                if message.type == "error"
                and "Failed to load resource: the server responded with a status of 404" not in message.text
                else None
            ),
        )
        page.on("pageerror", lambda error: console_errors.append(str(error)))

        await install_routes(page)
        await open_query_notebook(page, args)
        await assert_source_navigation(page, args.timeout_ms)

        if console_errors:
            raise RuntimeError("Unexpected browser errors: " + " | ".join(console_errors))

        await browser.close()
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    sys.exit(main())

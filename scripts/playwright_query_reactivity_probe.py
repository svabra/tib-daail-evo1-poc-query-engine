from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid

from playwright.async_api import async_playwright


NOTEBOOK_METADATA_STORAGE_KEY = "bdw.notebookMeta.v1"
NOTEBOOK_LAST_STORAGE_KEY = "bdw.lastNotebook.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure browser responsiveness for a minimal query run."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--sql", default="SELECT 1 AS smoke_value")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=60000)
    parser.add_argument("--settle-ms", type=int, default=0)
    parser.add_argument(
        "--submit-mode",
        choices=("click", "request-submit"),
        default="click",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    notebook_id = f"local-notebook-playwright-reactivity-{uuid.uuid4().hex[:8]}"
    cell_id = f"local-cell-{uuid.uuid4().hex[:8]}"

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(viewport={"width": 1440, "height": 1000})
        probe_config = {
            "metadataKey": NOTEBOOK_METADATA_STORAGE_KEY,
            "lastKey": NOTEBOOK_LAST_STORAGE_KEY,
            "notebookId": notebook_id,
            "cellId": cell_id,
            "sql": args.sql,
        }
        init_script = """
            (() => {
              const config = __CONFIG__;
              const state = JSON.parse(window.localStorage.getItem(config.metadataKey) || "{}");
              state[config.notebookId] = {
                title: "Playwright reactivity probe",
                summary: "Small query responsiveness probe",
                pipelineMode: "exploration",
                cells: [{
                  cellId: config.cellId,
                  language: "sql",
                  sql: config.sql,
                  dataSources: [],
                  queryOptions: {},
                }],
                tags: ["playwright"],
                canEdit: true,
                canDelete: true,
                shared: false,
                deleted: false,
                versions: [],
              };
              window.localStorage.setItem(config.metadataKey, JSON.stringify(state));
              window.localStorage.setItem(config.lastKey, config.notebookId);
            })();
            """.replace("__CONFIG__", json.dumps(probe_config))
        await context.add_init_script(init_script)
        page = await context.new_page()
        events: list[tuple[str, str, str]] = []
        page.on(
            "console",
            lambda msg: events.append(("console", msg.type, msg.text)),
        )
        page.on("pageerror", lambda exc: events.append(("pageerror", "error", str(exc))))

        print("probe: goto query-workbench", flush=True)
        t0 = time.perf_counter()
        await page.goto(
            f"{base_url}/query-workbench",
            wait_until="domcontentloaded",
            timeout=args.timeout_ms,
        )
        t_dom = time.perf_counter()

        print("probe: wait for seeded cell", flush=True)
        cell = page.locator(f'[data-query-cell][data-cell-id="{cell_id}"]').first
        await cell.wait_for(state="visible", timeout=args.timeout_ms)
        t_cell = time.perf_counter()

        print("probe: set SQL", flush=True)
        await cell.evaluate(
            """(cell, sql) => {
              const textarea = cell.querySelector("[data-editor-source]");
              if (!(textarea instanceof HTMLTextAreaElement)) {
                throw new Error("missing editor source");
              }
              textarea.value = sql;
              textarea.dispatchEvent(new Event("input", { bubbles: true }));
              textarea.dispatchEvent(new Event("change", { bubbles: true }));
            }""",
            args.sql,
        )
        t_sql = time.perf_counter()

        if args.settle_ms > 0:
            print(f"probe: settle {args.settle_ms} ms", flush=True)
            await page.wait_for_timeout(args.settle_ms)
        t_ready = time.perf_counter()

        print("probe: click Run and wait for POST", flush=True)
        async with page.expect_response(
            lambda response: response.request.method == "POST"
            and response.url.endswith("/api/query-jobs"),
            timeout=args.timeout_ms,
        ) as response_info:
            if args.submit_mode == "request-submit":
                await cell.evaluate(
                    """cell => {
                      const form = cell.querySelector("[data-query-form]");
                      if (!(form instanceof HTMLFormElement)) {
                        throw new Error("missing query form");
                      }
                      form.requestSubmit();
                    }"""
                )
            else:
                await cell.locator("[data-run-cell]").click(timeout=args.timeout_ms)
        response = await response_info.value
        t_post = time.perf_counter()
        payload = await response.json()

        print("probe: wait for result row", flush=True)
        await cell.locator(".result-table tbody tr").first.wait_for(
            state="visible",
            timeout=args.timeout_ms,
        )
        t_result = time.perf_counter()

        first_value = (
            await cell.locator(".result-table tbody tr td").first.inner_text()
        ).strip()
        result_text = (await cell.locator("[data-cell-result]").first.inner_text()).strip()
        print("probe: close browser", flush=True)
        await context.close()
        await browser.close()

    print(
        json.dumps(
            {
                "ok": True,
                "status": response.status,
                "jobId": payload.get("jobId") or payload.get("job_id"),
                "notebookId": notebook_id,
                "cellId": cell_id,
                "submitMode": args.submit_mode,
                "firstValue": first_value,
                "timingsMs": {
                    "gotoDomContentLoaded": round((t_dom - t0) * 1000, 1),
                    "cellVisibleAfterGoto": round((t_cell - t_dom) * 1000, 1),
                    "setSql": round((t_sql - t_cell) * 1000, 1),
                    "settle": round((t_ready - t_sql) * 1000, 1),
                    "clickToPostResponse": round((t_post - t_ready) * 1000, 1),
                    "postResponseToResultDom": round((t_result - t_post) * 1000, 1),
                    "total": round((t_result - t0) * 1000, 1),
                },
                "resultExcerpt": result_text[:500],
                "events": events[:20],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())

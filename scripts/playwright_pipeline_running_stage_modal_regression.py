from __future__ import annotations

import argparse
import asyncio
import json
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


NOTEBOOK_METADATA_STORAGE_KEY = "bdw.notebookMeta.v1"
NOTEBOOK_LAST_STORAGE_KEY = "bdw.lastNotebook.v1"
NOTEBOOK_ID = "local-notebook-running-stage-modal-regression"
CELL_ID = "cell-running-stage"
STAGE_ID = "stage-running"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a still-running pipeline stage does not open a false failure dialog."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def install_notebook_seed(context) -> None:
    config = {
        "metadataKey": NOTEBOOK_METADATA_STORAGE_KEY,
        "lastKey": NOTEBOOK_LAST_STORAGE_KEY,
        "notebookId": NOTEBOOK_ID,
        "cellId": CELL_ID,
        "stageId": STAGE_ID,
    }
    script = """
      (() => {
        const { metadataKey, lastKey, notebookId, cellId, stageId } = __CONFIG__;
        const state = JSON.parse(window.localStorage.getItem(metadataKey) || "{}");
        state[notebookId] = {
          title: "Running stage modal regression",
          summary: "Regression notebook for stage running status handling.",
          pipelineMode: "pipeline",
          pipelinePaths: [],
          cells: [
            {
              cellId,
              language: "sql",
              sql: "SELECT 1 AS id;",
              dataSources: [],
              queryOptions: {},
              stage: {
                enabled: true,
                stageId,
                alias: "running_stage",
                title: "Running Stage",
                description: "Stage that first reports running.",
                kind: "intermediate",
                materialize: true,
                outputFileName: "running_stage.parquet",
                predecessorStageIds: [],
              },
            },
          ],
          tags: ["pipeline", "regression"],
          canEdit: true,
          canDelete: true,
          shared: false,
          deleted: false,
          versions: [],
        };
        window.localStorage.setItem(metadataKey, JSON.stringify(state));
        window.localStorage.setItem(lastKey, notebookId);
      })();
    """.replace("__CONFIG__", json.dumps(config))
    await context.add_init_script(script)


def graph_payload(status: str) -> dict[str, object]:
    latest_revision = None
    output_source: dict[str, object] = {}
    if status == "valid":
        latest_revision = {
            "revisionId": "rev-running-stage",
            "outputPath": "s3://stage-bucket/_bdw_stages/running-stage/data.parquet",
            "queryPath": "read_parquet('s3://stage-bucket/_bdw_stages/running-stage/data.parquet')",
            "queryReference": "read_parquet('s3://stage-bucket/_bdw_stages/running-stage/data.parquet')",
            "rowCount": 1,
        }
        output_source = {
            "sourceKind": "s3",
            "sourceId": "s3:stage-bucket:_bdw_stages/running-stage/data.parquet",
            "bucket": "stage-bucket",
            "key": "_bdw_stages/running-stage/data.parquet",
        }
    latest_run = None
    if status == "running":
        latest_run = {
            "runId": "run-running-stage",
            "status": "running",
            "message": "DuckDB is planning and executing the statement.",
        }
    elif status == "valid":
        latest_run = {
            "runId": "run-running-stage",
            "status": "completed",
            "message": "Materialized 1 rows.",
        }

    active_runs = []
    if status == "running":
        active_runs = [
            {
                "runId": "run-running-stage",
                "notebookId": NOTEBOOK_ID,
                "stageIds": [STAGE_ID],
                "currentStageId": STAGE_ID,
                "status": "running",
                "cancelRequested": False,
            }
        ]

    return {
        "notebookId": NOTEBOOK_ID,
        "notebookTitle": "Running stage modal regression",
        "version": 1,
        "nodes": [
            {
                "stageId": STAGE_ID,
                "cellId": CELL_ID,
                "alias": "running_stage",
                "title": "Running Stage",
                "description": "Stage that first reports running.",
                "kind": "intermediate",
                "enabled": True,
                "materialize": True,
                "outputFileName": "running_stage.parquet",
                "resolvedOutputFileName": "running_stage.parquet",
                "recommendedOutputFileName": "running_stage.parquet",
                "predecessorStageIds": [],
                "successorStageIds": [],
                "status": status,
                "layer": 0,
                "rank": 0,
                "latestRevision": latest_revision,
                "latestRun": latest_run,
                "outputSource": output_source,
            }
        ],
        "sourceNodes": [],
        "edges": [],
        "diagnostics": [],
        "order": [STAGE_ID],
        "paths": [],
        "defaultSelectedStageId": STAGE_ID,
        "activeRuns": active_runs,
    }


async def open_query_workbench(page, args: argparse.Namespace) -> None:
    await page.goto(
        urljoin(args.base_url, "query-workbench"),
        wait_until="domcontentloaded",
        timeout=args.timeout_ms,
    )
    await page.wait_for_selector(
        "[data-workspace-notebook], [data-query-workbench-entry-page] [data-create-notebook]:visible",
        timeout=args.timeout_ms,
    )
    deadline = asyncio.get_running_loop().time() + (args.timeout_ms / 1000)
    while asyncio.get_running_loop().time() < deadline:
        if await page.locator("[data-workspace-notebook]:visible").count():
            return
        create_button = page.locator("[data-query-workbench-entry-page] [data-create-notebook]:visible").first
        if await create_button.count():
            try:
                await create_button.click(timeout=5000)
            except PlaywrightTimeoutError:
                await page.wait_for_timeout(250)
                continue
        try:
            await page.locator("[data-workspace-notebook]").wait_for(state="visible", timeout=3000)
            return
        except PlaywrightTimeoutError:
            await page.wait_for_timeout(250)
    await page.locator("[data-workspace-notebook]").wait_for(state="visible", timeout=1000)


async def assert_no_stage_failed_dialog(page) -> None:
    failure_title = page.get_by_text("Stage run failed", exact=True)
    if await failure_title.count() and await failure_title.first.is_visible():
        copy = ""
        copy_locator = page.locator("[data-message-copy]").first
        if await copy_locator.count():
            copy = (await copy_locator.text_content()) or ""
        raise RuntimeError(f"Unexpected stage failure dialog while stage was still running: {copy!r}")
    running_copy = page.get_by_text("Stage finished with status Running.", exact=True)
    if await running_copy.count() and await running_copy.first.is_visible():
        raise RuntimeError("The stale running-status failure message was shown.")


async def wait_for_condition(predicate, timeout_ms: int, message: str) -> None:
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise RuntimeError(message)


async def main() -> None:
    args = parse_args()
    stage_status = "planned"
    run_started = False
    allow_completion = False
    graph_running_responses = 0
    query_job_calls = 0

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context()
        await install_notebook_seed(context)
        page = await context.new_page()

        async def handle_graph(route):
            nonlocal stage_status, graph_running_responses
            if run_started and allow_completion:
                stage_status = "valid"
            if stage_status == "running":
                graph_running_responses += 1
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(graph_payload(stage_status)),
            )

        async def handle_stage_run(route):
            nonlocal stage_status, run_started
            run_started = True
            stage_status = "running"
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "version": 2,
                        "records": [],
                        "activeRuns": graph_payload("running")["activeRuns"],
                    }
                ),
            )

        async def handle_stage_state(route):
            nonlocal stage_status
            if run_started and allow_completion:
                stage_status = "valid"
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "version": 3,
                        "records": [],
                        "activeRuns": graph_payload(stage_status)["activeRuns"],
                    }
                ),
            )

        async def handle_query_source_validation(route):
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "status": "valid",
                        "references": [],
                        "missingReferences": [],
                        "message": "Sources checked: all referenced sources exist.",
                    }
                ),
            )

        async def handle_query_job(route):
            nonlocal query_job_calls
            query_job_calls += 1
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "jobId": "job-after-running-stage",
                        "notebookId": NOTEBOOK_ID,
                        "cellId": CELL_ID,
                        "status": "completed",
                        "sql": "SELECT 1 AS id;",
                        "displaySql": "SELECT 1 AS id;",
                        "columns": ["id"],
                        "rows": [[1]],
                        "rowCount": 1,
                        "startedAt": "2026-06-17T00:00:00Z",
                        "completedAt": "2026-06-17T00:00:01Z",
                    }
                ),
            )

        async def handle_sidebar(route):
            await route.fulfill(
                status=200,
                content_type="text/html",
                body="""
                <aside class="sidebar" data-sidebar data-sidebar-mode="notebook">
                  <details data-notebook-section open><summary>Notebooks</summary></details>
                  <details data-data-sources-section open><summary>Data Sources</summary></details>
                  <details data-query-monitor-section open><summary>Query Monitoring</summary></details>
                </aside>
                """,
            )

        await page.route("**/api/materialized-stages/graph", handle_graph)
        await page.route(f"**/api/materialized-stages/stages/{STAGE_ID}/run", handle_stage_run)
        await page.route("**/api/materialized-stages/state", handle_stage_state)
        await page.route("**/api/query-sources/validate", handle_query_source_validation)
        await page.route("**/api/query-jobs", handle_query_job)
        await page.route("**/sidebar?**", handle_sidebar)

        try:
            await open_query_workbench(page, args)
            cell = page.locator(f'[data-query-cell][data-cell-id="{CELL_ID}"]').first
            await cell.scroll_into_view_if_needed(timeout=args.timeout_ms)
            await cell.hover(timeout=args.timeout_ms)
            run_button = cell.locator("[data-run-cell]").first
            await run_button.wait_for(state="visible", timeout=args.timeout_ms)
            await run_button.click(timeout=args.timeout_ms)

            await wait_for_condition(
                lambda: graph_running_responses > 0,
                args.timeout_ms,
                "The mocked stage never reached the running graph response.",
            )
            await page.wait_for_timeout(700)
            await assert_no_stage_failed_dialog(page)

            allow_completion = True
            await wait_for_condition(
                lambda: query_job_calls > 0,
                args.timeout_ms,
                "The cell query was not started after the materialized stage became valid.",
            )
            await assert_no_stage_failed_dialog(page)
        finally:
            await context.close()
            await browser.close()

    print("Pipeline running-stage modal regression passed.")


if __name__ == "__main__":
    asyncio.run(main())

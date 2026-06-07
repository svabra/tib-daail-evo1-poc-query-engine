from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from urllib.parse import urljoin

from playwright.async_api import async_playwright


NOTEBOOK_METADATA_STORAGE_KEY = "bdw.notebookMeta.v1"
NOTEBOOK_LAST_STORAGE_KEY = "bdw.lastNotebook.v1"
EXPECTED_STAGE_ORDER = ["stage-raw", "stage-scope", "stage-final"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a real three-stage notebook pipeline twice and verify valid stages are re-executed."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=90000)
    return parser.parse_args()


def regression_cells() -> list[dict[str, object]]:
    return [
        {
            "cellId": "cell-raw",
            "language": "sql",
            "sql": "SELECT 1 AS id, 'raw' AS label UNION ALL SELECT 2 AS id, 'scope' AS label",
            "dataSources": [],
            "queryOptions": {},
            "stage": {
                "enabled": True,
                "stageId": "stage-raw",
                "alias": "raw",
                "title": "Raw Stage",
                "description": "Real pipeline regression source stage",
                "kind": "intermediate",
                "predecessorStageIds": [],
                "materialize": True,
            },
        },
        {
            "cellId": "cell-scope",
            "language": "sql",
            "sql": "SELECT * FROM stage.raw WHERE id >= 1",
            "dataSources": [],
            "queryOptions": {},
            "stage": {
                "enabled": True,
                "stageId": "stage-scope",
                "alias": "scope",
                "title": "Scoped Stage",
                "description": "Real pipeline regression dependent stage",
                "kind": "intermediate",
                "predecessorStageIds": ["stage-raw"],
                "materialize": True,
            },
        },
        {
            "cellId": "cell-final",
            "language": "sql",
            "sql": "SELECT COUNT(*) AS row_count FROM stage.scope",
            "dataSources": [],
            "queryOptions": {},
            "stage": {
                "enabled": True,
                "stageId": "stage-final",
                "alias": "final_product",
                "title": "Final Stage",
                "description": "Real pipeline regression final stage",
                "kind": "final",
                "predecessorStageIds": ["stage-scope"],
                "materialize": True,
            },
        },
    ]


async def install_regression_notebook(context, notebook_id: str, cells: list[dict[str, object]]) -> None:
    config = {
        "metadataKey": NOTEBOOK_METADATA_STORAGE_KEY,
        "lastKey": NOTEBOOK_LAST_STORAGE_KEY,
        "notebookId": notebook_id,
        "cells": cells,
    }
    script = """
        (() => {
          const { metadataKey, lastKey, notebookId, cells } = __CONFIG__;
          const state = JSON.parse(window.localStorage.getItem(metadataKey) || "{}");
          state[notebookId] = {
            title: "Pipeline real execution regression",
            summary: "Verifies Run pipeline re-executes already-valid stages.",
            pipelineMode: "pipeline",
            cells,
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


async def fetch_graph(context, base_url: str, notebook_id: str, cells: list[dict[str, object]]) -> dict[str, object]:
    response = await context.request.post(
        urljoin(base_url, "api/materialized-stages/graph"),
        data={
            "notebookId": notebook_id,
            "notebookTitle": "Pipeline real execution regression",
            "cells": cells,
        },
    )
    if not response.ok:
        raise RuntimeError(f"Graph request failed: {response.status} {await response.text()}")
    return await response.json()


async def fetch_state(context, base_url: str) -> dict[str, object]:
    response = await context.request.get(urljoin(base_url, "api/materialized-stages/state"))
    if not response.ok:
        raise RuntimeError(f"State request failed: {response.status} {await response.text()}")
    return await response.json()


def node_run_statuses(graph: dict[str, object]) -> dict[str, str]:
    return {
        str(node.get("stageId") or ""): str((node.get("latestRun") or {}).get("status") or "")
        for node in graph.get("nodes", []) or []
        if isinstance(node, dict)
    }


def node_run_ids(graph: dict[str, object]) -> dict[str, str]:
    return {
        str(node.get("stageId") or ""): str((node.get("latestRun") or {}).get("runId") or "")
        for node in graph.get("nodes", []) or []
        if isinstance(node, dict)
    }


async def wait_for_pipeline_terminal(
    context,
    base_url: str,
    notebook_id: str,
    cells: list[dict[str, object]],
    timeout_ms: int,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_ms / 1000
    terminal_statuses = {"completed", "failed", "cancelled", "skipped"}
    latest_graph: dict[str, object] = {}
    while time.monotonic() < deadline:
        latest_graph = await fetch_graph(context, base_url, notebook_id, cells)
        diagnostics = [
            item
            for item in latest_graph.get("diagnostics", []) or []
            if isinstance(item, dict) and item.get("severity") == "error"
        ]
        if diagnostics:
            raise RuntimeError(f"Pipeline graph has diagnostics: {diagnostics}")
        active_runs = latest_graph.get("activeRuns", []) or []
        statuses = node_run_statuses(latest_graph)
        if len(statuses) == len(EXPECTED_STAGE_ORDER) and not active_runs:
            if all(statuses.get(stage_id) == "completed" for stage_id in EXPECTED_STAGE_ORDER):
                return latest_graph
            if all(statuses.get(stage_id) in terminal_statuses for stage_id in EXPECTED_STAGE_ORDER):
                raise RuntimeError(f"Pipeline reached non-completed terminal statuses: {statuses}")
        await asyncio.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for pipeline completion. Last graph: {latest_graph}")


async def click_run_pipeline(page, timeout_ms: int) -> None:
    await page.locator("[data-run-notebook-pipeline]:visible").first.click(timeout=timeout_ms)


async def assert_run_record_order(context, base_url: str, run_id: str) -> None:
    state = await fetch_state(context, base_url)
    completed_stage_ids = [
        str(record.get("stageId") or "")
        for record in state.get("records", []) or []
        if isinstance(record, dict)
        and str(record.get("runId") or "") == run_id
        and str(record.get("status") or "") == "completed"
    ]
    if completed_stage_ids != EXPECTED_STAGE_ORDER:
        raise RuntimeError(
            f"Pipeline run records were not completed in dependency order: {completed_stage_ids}"
        )


async def main() -> None:
    args = parse_args()
    notebook_id = f"local-notebook-pipeline-real-{uuid.uuid4().hex[:10]}"
    cells = regression_cells()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context()
        await install_regression_notebook(context, notebook_id, cells)
        page = await context.new_page()

        try:
            await page.goto(
                urljoin(args.base_url, "query-workbench"),
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await page.locator(f'[data-workspace-notebook][data-notebook-id="{notebook_id}"]').wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.locator("[data-notebook-pipeline-panel]:visible").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-pipeline-stage-row]').length === 3",
                timeout=args.timeout_ms,
            )

            await click_run_pipeline(page, args.timeout_ms)
            first_graph = await wait_for_pipeline_terminal(
                context,
                args.base_url,
                notebook_id,
                cells,
                args.timeout_ms,
            )
            first_run_ids = node_run_ids(first_graph)
            if not all(first_run_ids.get(stage_id) for stage_id in EXPECTED_STAGE_ORDER):
                raise RuntimeError(f"First pipeline run did not produce run ids: {first_run_ids}")

            await click_run_pipeline(page, args.timeout_ms)
            second_graph = await wait_for_pipeline_terminal(
                context,
                args.base_url,
                notebook_id,
                cells,
                args.timeout_ms,
            )
            second_run_ids = node_run_ids(second_graph)
            if not all(second_run_ids.get(stage_id) for stage_id in EXPECTED_STAGE_ORDER):
                raise RuntimeError(f"Second pipeline run did not produce run ids: {second_run_ids}")
            if second_run_ids == first_run_ids:
                raise RuntimeError(
                    "Second Run pipeline click reused the previous completed stage runs instead of re-executing them."
                )
            unique_second_run_ids = {second_run_ids[stage_id] for stage_id in EXPECTED_STAGE_ORDER}
            if len(unique_second_run_ids) != 1:
                raise RuntimeError(f"Second pipeline stages did not share a single pipeline run id: {second_run_ids}")
            await assert_run_record_order(context, args.base_url, unique_second_run_ids.pop())
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

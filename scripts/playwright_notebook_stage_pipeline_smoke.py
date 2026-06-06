from __future__ import annotations

import argparse
import asyncio
import json
import re
from urllib.parse import urljoin

from playwright.async_api import async_playwright


NOTEBOOK_METADATA_STORAGE_KEY = "bdw.notebookMeta.v1"
NOTEBOOK_LAST_STORAGE_KEY = "bdw.lastNotebook.v1"
FRESH_NOTEBOOK_ID = "local-notebook-pipeline-smoke"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise Notebook Pipeline mode, graph/table interactions, and stage actions."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


def graph_payload(cells: list[dict[str, object]], statuses: dict[str, str], revision_suffix: str) -> dict[str, object]:
    stage_cells = [cell for cell in cells if cell.get("language", "sql") == "sql"]
    nodes = []
    by_id = {}
    for index, cell in enumerate(stage_cells):
        stage = cell.get("stage") or {}
        stage_id = str(stage.get("stageId") or f"stage-{index + 1}")
        predecessors = [str(item) for item in stage.get("predecessorStageIds") or []]
        if "stage.raw" in str(cell.get("sql") or "") and "stage-raw" not in predecessors and stage_id != "stage-raw":
            predecessors.append("stage-raw")
        if "stage.scope" in str(cell.get("sql") or "") and "stage-scope" not in predecessors and stage_id != "stage-scope":
            predecessors.append("stage-scope")
        node = {
            "stageId": stage_id,
            "cellId": cell.get("cellId"),
            "cellIndex": index,
            "alias": stage.get("alias") or f"stage_{index + 1}",
            "title": stage.get("title") or f"Stage {index + 1}",
            "description": stage.get("description") or "",
            "kind": stage.get("kind") or "intermediate",
            "materialize": True,
            "predecessorStageIds": predecessors,
            "successorStageIds": [],
            "sql": cell.get("sql") or "",
            "dataSources": [],
            "status": statuses.get(stage_id, "planned"),
            "order": index,
            "layer": len(predecessors),
            "published": stage_id == "stage-final" and statuses.get(stage_id) == "valid",
            "publishedDataProducts": [],
            "obsoleteReason": "A predecessor was re-materialized with changed results." if statuses.get(stage_id) == "obsolete" else "",
        }
        if node["status"] in {"valid", "obsolete"}:
            key = f"_bdw_stages/notebook/{node['alias']}/{revision_suffix}/data.parquet"
            node["latestRevision"] = {
                "stageId": stage_id,
                "status": "completed",
                "revisionId": revision_suffix,
                "rowCount": 3,
                "outputBucket": "stage-bucket",
                "outputKey": key,
                "outputPath": f"s3://stage-bucket/{key}",
                "queryPath": f"s3.stage_bucket._bdw_stages.notebook.{node['alias']}.data.parquet",
            }
            node["outputSource"] = {
                "sourceKind": "object",
                "sourceId": "workspace.s3",
                "bucket": "stage-bucket",
                "key": key,
                "sourceDisplayName": f"{node['title']} materialized output",
                "sourcePlatform": "s3",
            }
        else:
            node["latestRevision"] = None
            node["outputSource"] = {}
        by_id[stage_id] = node
        nodes.append(node)
    edges = []
    for node in nodes:
        for predecessor in node["predecessorStageIds"]:
            edges.append({"fromStageId": predecessor, "toStageId": node["stageId"]})
            if predecessor in by_id:
                by_id[predecessor]["successorStageIds"].append(node["stageId"])
    default_stage = next((node["stageId"] for node in nodes if node["status"] == "obsolete"), nodes[0]["stageId"])
    return {
        "notebookId": "mock-notebook",
        "notebookTitle": "Pipeline smoke",
        "version": 1,
        "nodes": nodes,
        "sourceNodes": [],
        "edges": edges,
        "diagnostics": [],
        "order": [node["stageId"] for node in nodes],
        "defaultSelectedStageId": default_stage,
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
    if not await page.locator("[data-workspace-notebook]:visible").count():
        await page.locator("[data-query-workbench-entry-page] [data-create-notebook]:visible").first.click()
    await page.locator("[data-workspace-notebook]").wait_for(state="visible", timeout=args.timeout_ms)


async def install_fresh_notebook_seed(context) -> None:
    config = {
        "metadataKey": NOTEBOOK_METADATA_STORAGE_KEY,
        "lastKey": NOTEBOOK_LAST_STORAGE_KEY,
        "notebookId": FRESH_NOTEBOOK_ID,
    }
    script = """
        (() => {
          const { metadataKey, lastKey, notebookId } = __CONFIG__;
          const state = JSON.parse(window.localStorage.getItem(metadataKey) || "{}");
          if (!state[notebookId]) {
            state[notebookId] = {
              title: "Pipeline smoke",
              summary: "Playwright pipeline smoke notebook",
              pipelineMode: "exploration",
              cells: [
                {
                  cellId: "cell-fresh",
                  language: "sql",
                  sql: "SELECT 1 AS id;",
                  dataSources: [],
                  queryOptions: {},
                  stage: {},
                },
              ],
              tags: [],
              canEdit: true,
              canDelete: true,
              shared: false,
              deleted: false,
              versions: [],
            };
          }
          window.localStorage.setItem(metadataKey, JSON.stringify(state));
          window.localStorage.setItem(lastKey, notebookId);
        })();
        """.replace("__CONFIG__", json.dumps(config))
    await context.add_init_script(script)


async def seed_three_stage_notebook(page, timeout_ms: int) -> str:
    notebook_id = await page.locator("[data-notebook-meta]").first.get_attribute("data-notebook-id")
    if not notebook_id:
        raise RuntimeError("No active notebook id found.")
    cells = [
        {
            "cellId": "cell-raw",
            "language": "sql",
            "sql": "SELECT 1 AS id, 'raw' AS label;",
            "dataSources": [],
            "queryOptions": {},
            "stage": {
                "enabled": True,
                "stageId": "stage-raw",
                "alias": "raw",
                "title": "Raw Scope",
                "description": "Raw input stage",
                "kind": "intermediate",
                "predecessorStageIds": [],
                "materialize": True,
            },
        },
        {
            "cellId": "cell-scope",
            "language": "sql",
            "sql": "SELECT * FROM stage.raw;",
            "dataSources": [],
            "queryOptions": {},
            "stage": {
                "enabled": True,
                "stageId": "stage-scope",
                "alias": "scope",
                "title": "Scoped Data",
                "description": "Depends on raw",
                "kind": "intermediate",
                "predecessorStageIds": ["stage-raw"],
                "materialize": True,
            },
        },
        {
            "cellId": "cell-final",
            "language": "sql",
            "sql": "SELECT * FROM stage.scope;",
            "dataSources": [],
            "queryOptions": {},
            "stage": {
                "enabled": True,
                "stageId": "stage-final",
                "alias": "final_product",
                "title": "Final Product",
                "description": "Final published data product",
                "kind": "final",
                "predecessorStageIds": ["stage-scope"],
                "materialize": True,
            },
        },
    ]
    await page.evaluate(
        """
        ({ storageKey, notebookId, cells }) => {
          const raw = window.localStorage.getItem(storageKey);
          const state = raw ? JSON.parse(raw) : {};
          state[notebookId] = {
            title: "Pipeline smoke",
            summary: "Playwright pipeline smoke notebook",
            pipelineMode: "exploration",
            cells,
            tags: [],
            canEdit: true,
            canDelete: true,
            shared: false,
            deleted: false,
            versions: [],
          };
          window.localStorage.setItem(storageKey, JSON.stringify(state));
        }
        """,
        {
            "storageKey": NOTEBOOK_METADATA_STORAGE_KEY,
            "notebookId": notebook_id,
            "cells": cells,
        },
    )
    await page.reload(wait_until="domcontentloaded", timeout=timeout_ms)
    await page.locator("[data-workspace-notebook]").wait_for(state="visible", timeout=timeout_ms)
    return notebook_id


async def pipeline_stage_title_positions(page) -> list[dict[str, object]]:
    return await page.locator("[data-pipeline-stage-row]").evaluate_all(
        """
        (rows) => rows.map((row) => {
          const cell = row.cells[0];
          const title = row.querySelector(".pipeline-table-stage-title");
          const cellStyle = getComputedStyle(cell);
          const titleRect = title.getBoundingClientRect();
          const cellRect = cell.getBoundingClientRect();
          return {
            stageId: row.dataset.pipelineStageRow || "",
            selected: row.classList.contains("is-selected"),
            titleLeft: titleRect.left,
            cellLeft: cellRect.left,
            borderLeftWidth: cellStyle.borderLeftWidth,
            paddingLeft: cellStyle.paddingLeft,
          };
        })
        """
    )


def assert_pipeline_stage_title_alignment(positions: list[dict[str, object]]) -> None:
    if len(positions) < 2:
        raise RuntimeError(f"Pipeline stage table rendered too few rows for alignment testing: {positions}")
    title_lefts = [float(item["titleLeft"]) for item in positions]
    max_delta = max(title_lefts) - min(title_lefts)
    if max_delta > 0.5:
        raise RuntimeError(
            "Pipeline stage row text shifted between selected and unselected rows: "
            f"delta={max_delta}, positions={positions}"
        )


async def pipeline_focused_cell_gap(page) -> dict[str, object]:
    return await page.evaluate(
        """
        () => {
          const table = document.querySelector("[data-notebook-pipeline-table]");
          const cell = Array.from(document.querySelectorAll("[data-query-cell]"))
            .find((candidate) => {
              const style = getComputedStyle(candidate);
              return !candidate.hidden && style.display !== "none" && style.visibility !== "hidden";
            });
          if (!table || !cell) {
            return { error: "Pipeline table or visible focused cell was not found." };
          }
          const tableRect = table.getBoundingClientRect();
          const cellRect = cell.getBoundingClientRect();
          const style = getComputedStyle(cell);
          const before = getComputedStyle(cell, "::before");
          return {
            cellId: cell.dataset.cellId || "",
            gap: cellRect.top - tableRect.bottom,
            marginTop: style.marginTop,
            paddingTop: style.paddingTop,
            beforeDisplay: before.display,
          };
        }
        """
    )


def assert_pipeline_focused_cell_gap_alignment(measurements: list[dict[str, object]]) -> None:
    if len(measurements) < 2:
        raise RuntimeError(f"Pipeline focus gap check needs multiple stages: {measurements}")
    errors = [item for item in measurements if item.get("error")]
    if errors:
        raise RuntimeError(f"Pipeline focus gap could not be measured: {errors}")
    gaps = [float(item["gap"]) for item in measurements]
    max_delta = max(gaps) - min(gaps)
    if max_delta > 0.5:
        raise RuntimeError(
            "Pipeline focused cell gap shifted between selected stages: "
            f"delta={max_delta}, measurements={measurements}"
        )
    spacing_violations = [
        item
        for item in measurements
        if item.get("marginTop") != "0px" or item.get("paddingTop") != "12px" or item.get("beforeDisplay") != "none"
    ]
    if spacing_violations:
        raise RuntimeError(
            "Pipeline focused cells should not inherit hidden sibling spacing or separators: "
            f"{spacing_violations}"
        )


async def main() -> None:
    args = parse_args()
    statuses = {
        "stage-raw": "planned",
        "stage-scope": "planned",
        "stage-final": "planned",
    }
    revision_suffix = "rev-a"
    last_cells: list[dict[str, object]] = []
    stage_scope_run_count = 0
    last_query_job_sql = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(permissions=["clipboard-read", "clipboard-write"])
        await install_fresh_notebook_seed(context)
        page = await context.new_page()

        async def handle_graph(route):
            nonlocal last_cells
            request = route.request
            payload = json.loads(request.post_data or "{}")
            last_cells = payload.get("cells") or last_cells
            graph = graph_payload(last_cells, statuses, revision_suffix)
            graph["notebookId"] = payload.get("notebookId") or "mock-notebook"
            graph["notebookTitle"] = payload.get("notebookTitle") or "Pipeline smoke"
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(graph),
            )

        async def handle_pipeline_run(route):
            statuses.update(
                {
                    "stage-raw": "valid",
                    "stage-scope": "valid",
                    "stage-final": "valid",
                }
            )
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"version": 2, "records": []}),
            )

        async def handle_stage_run(route):
            nonlocal revision_suffix
            revision_suffix = "rev-b"
            statuses.update(
                {
                    "stage-raw": "valid",
                    "stage-scope": "obsolete",
                    "stage-final": "obsolete",
                }
            )
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"version": 3, "records": []}),
            )

        async def handle_stage_scope_run(route):
            nonlocal stage_scope_run_count
            stage_scope_run_count += 1
            statuses.update(
                {
                    "stage-raw": "valid",
                    "stage-scope": "valid",
                    "stage-final": "planned",
                }
            )
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"version": 5, "records": []}),
            )

        async def handle_stage_stop(route):
            statuses["stage-raw"] = "cancelled"
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"version": 4, "records": []}),
            )

        async def handle_stage_state(route):
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"version": 1, "records": []}),
            )

        async def handle_query_job(route):
            nonlocal last_query_job_sql
            body = route.request.post_data or ""
            match = re.search(r'name="sql"\r\n\r\n(.*?)\r\n--', body, re.DOTALL)
            last_query_job_sql = match.group(1) if match else body
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "jobId": "job-stage-scope",
                        "notebookId": "mock-notebook",
                        "cellId": "cell-scope",
                        "status": "completed",
                        "sql": "SELECT * FROM s3.stage_bucket._bdw_stages.notebook.raw.data.parquet",
                        "displaySql": "SELECT * FROM stage.raw;",
                        "columns": ["id", "label"],
                        "rows": [[1, "raw"]],
                        "rowCount": 1,
                        "startedAt": "2026-06-05T00:00:00Z",
                        "completedAt": "2026-06-05T00:00:01Z",
                    }
                ),
            )

        async def handle_sidebar(route):
            await route.fulfill(
                status=200,
                content_type="text/html",
                body="""
                <aside class="sidebar" data-sidebar data-sidebar-mode="notebook">
                  <details data-notebook-section open>
                    <summary>Notebooks</summary>
                  </details>
                  <details data-data-sources-section open>
                    <summary>Data Sources</summary>
                    <details data-source-catalog data-source-catalog-name="workspace.s3" data-source-catalog-source-id="workspace.s3" open>
                      <summary>Shared Workspace</summary>
                      <div
                        data-source-object
                        data-s3-bucket="stage-bucket"
                        data-s3-key="_bdw_stages/notebook/raw/rev-b/data.parquet"
                      >Raw materialized output</div>
                    </details>
                  </details>
                  <details data-query-monitor-section open><summary>Query Monitoring</summary></details>
                </aside>
                """,
            )

        await page.route("**/api/materialized-stages/graph", handle_graph)
        await page.route("**/api/materialized-stages/pipeline/run", handle_pipeline_run)
        await page.route("**/api/materialized-stages/stages/stage-raw/run", handle_stage_run)
        await page.route("**/api/materialized-stages/stages/stage-scope/run", handle_stage_scope_run)
        await page.route("**/api/materialized-stages/stages/stage-raw/stop", handle_stage_stop)
        await page.route("**/api/materialized-stages/state", handle_stage_state)
        await page.route("**/api/query-jobs", handle_query_job)

        try:
            await open_query_workbench(page, args)

            if await page.locator("[data-notebook-pipeline-panel]").is_visible():
                raise RuntimeError("Exploration mode should hide the pipeline panel on a fresh notebook.")
            mode_toggle = page.locator("[data-notebook-mode-toggle]").first
            exploration_tooltip = await mode_toggle.get_attribute("title")
            if "Notebook mode: Exploration" not in (exploration_tooltip or "") or "links SQL cells" not in (exploration_tooltip or ""):
                raise RuntimeError("Exploration mode switch tooltip was missing or unclear.")
            mode_toggle_style = await mode_toggle.evaluate(
                """
                (toggle) => {
                  const sharing = document.querySelector('[data-notebook-shared-toggle]');
                  const style = getComputedStyle(toggle);
                  const sharingStyle = sharing ? getComputedStyle(sharing) : null;
                  const switchEl = toggle.querySelector('.workspace-sharing-toggle-switch');
                  const switchStyle = switchEl ? getComputedStyle(switchEl) : null;
                  const toggleRect = toggle.getBoundingClientRect();
                  const sharingRect = sharing ? sharing.getBoundingClientRect() : null;
                  return {
                    hasSharingClass: toggle.classList.contains('workspace-sharing-toggle'),
                    parentClass: toggle.parentElement?.className || '',
                    sameParentAsSharing: sharing ? sharing.parentElement === toggle.parentElement : false,
                    borderRadius: style.borderRadius,
                    sharingBorderRadius: sharingStyle?.borderRadius || '',
                    display: style.display,
                    gap: style.columnGap || style.gap,
                    sharingGap: sharingStyle?.columnGap || sharingStyle?.gap || '',
                    paddingTop: style.paddingTop,
                    sharingPaddingTop: sharingStyle?.paddingTop || '',
                    backgroundColor: style.backgroundColor,
                    switchWidth: switchStyle?.width || '',
                    switchHeight: switchStyle?.height || '',
                    topDelta: sharingRect ? Math.abs(toggleRect.top - sharingRect.top) : 0,
                    leftAfterSharing: sharingRect ? toggleRect.left > sharingRect.left : false,
                    widthDelta: sharingRect ? Math.abs(toggleRect.width - sharingRect.width) : 0,
                    heightDelta: sharingRect ? Math.abs(toggleRect.height - sharingRect.height) : 0,
                    label: toggle.querySelector('[data-notebook-mode-toggle-label]')?.textContent?.trim() || '',
                    detail: toggle.querySelector('[data-notebook-mode-toggle-detail]')?.textContent?.trim() || '',
                  };
                }
                """
            )
            if not mode_toggle_style["hasSharingClass"]:
                raise RuntimeError(f"Notebook mode toggle does not reuse the sharing switch-card style: {mode_toggle_style}")
            if "workspace-header-toggle-row" not in mode_toggle_style["parentClass"] or not mode_toggle_style["sameParentAsSharing"]:
                raise RuntimeError(f"Notebook mode toggle is not placed beside the Private/Public toggle: {mode_toggle_style}")
            if mode_toggle_style["display"] not in {"inline-flex", "flex"}:
                raise RuntimeError(f"Notebook mode toggle did not render as a switch-card: {mode_toggle_style}")
            if mode_toggle_style["borderRadius"] != mode_toggle_style["sharingBorderRadius"]:
                raise RuntimeError(f"Notebook mode toggle radius does not match Private/Public: {mode_toggle_style}")
            if mode_toggle_style["gap"] != mode_toggle_style["sharingGap"] or mode_toggle_style["paddingTop"] != mode_toggle_style["sharingPaddingTop"]:
                raise RuntimeError(f"Notebook mode toggle spacing does not match Private/Public: {mode_toggle_style}")
            if mode_toggle_style["switchWidth"] != "44px" or mode_toggle_style["switchHeight"] != "24px":
                raise RuntimeError(f"Notebook mode toggle switch dimensions do not match Private/Public: {mode_toggle_style}")
            if mode_toggle_style["topDelta"] > 4 or not mode_toggle_style["leftAfterSharing"]:
                raise RuntimeError(f"Notebook mode toggle is not visually next to Private/Public: {mode_toggle_style}")
            if mode_toggle_style["widthDelta"] > 2 or mode_toggle_style["heightDelta"] > 2:
                raise RuntimeError(f"Notebook mode toggle does not match Private/Public card dimensions: {mode_toggle_style}")
            if (
                mode_toggle_style["label"] != "Exploration Mode"
                or mode_toggle_style["detail"] != "Keeps cells independent for ad-hoc SQL or Python work."
            ):
                raise RuntimeError(f"Notebook mode toggle did not show the Exploration copy: {mode_toggle_style}")

            await mode_toggle.click()
            await page.locator("[data-cell-stage-title-input]").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            pipeline_tooltip = await mode_toggle.get_attribute("title")
            pipeline_label = await mode_toggle.locator("[data-notebook-mode-toggle-label]").text_content()
            if "Notebook mode: Pipeline" not in (pipeline_tooltip or "") or "keeps cells independent" not in (pipeline_tooltip or ""):
                raise RuntimeError("Pipeline mode switch tooltip was missing or unclear.")
            if (pipeline_label or "").strip() != "Pipeline Mode":
                raise RuntimeError(f"Notebook mode toggle did not switch to Pipeline copy: {pipeline_label}")
            default_title = await page.locator("[data-cell-stage-title-input]").first.input_value()
            default_description = await page.locator("[data-cell-stage-description-input]").first.input_value()
            if default_title != "my first stage":
                raise RuntimeError(f"Fresh pipeline title default was unexpected: {default_title}")
            if default_description != "This is the stage description":
                raise RuntimeError(f"Fresh pipeline description default was unexpected: {default_description}")

            await mode_toggle.click()
            await page.locator("[data-notebook-pipeline-panel]").wait_for(
                state="hidden",
                timeout=args.timeout_ms,
            )
            if await page.locator("[data-cell-stage-strip]:visible").count():
                raise RuntimeError("Exploration mode should hide stage strips after switching back.")

            notebook_id = await seed_three_stage_notebook(page, args.timeout_ms)

            if await page.locator("[data-notebook-pipeline-panel]").is_visible():
                raise RuntimeError("Exploration mode should hide the pipeline panel.")

            await page.locator("[data-notebook-mode-toggle]").first.click()
            await page.locator("[data-notebook-pipeline-graph] .pipeline-node").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node').length >= 3",
                timeout=args.timeout_ms,
            )
            node_width = await page.locator("[data-notebook-pipeline-graph] .pipeline-node").first.evaluate(
                "(node) => node.getBoundingClientRect().width"
            )
            if node_width < 264:
                raise RuntimeError(f"Pipeline graph node rendered below its minimum width: {node_width}px")
            if node_width > 280:
                raise RuntimeError(f"Pipeline graph node rendered too large: {node_width}px")
            node_height = await page.locator("[data-notebook-pipeline-graph] .pipeline-node").first.evaluate(
                "(node) => node.getBoundingClientRect().height"
            )
            if node_height < 88:
                raise RuntimeError(f"Pipeline graph node rendered below its minimum height: {node_height}px")
            clipped_node_count = await page.locator("[data-notebook-pipeline-graph] .pipeline-node-body").evaluate_all(
                "(nodes) => nodes.filter((node) => node.scrollHeight > node.clientHeight + 1).length"
            )
            if clipped_node_count:
                raise RuntimeError(f"Pipeline graph rendered clipped stage node content: {clipped_node_count}")
            stage_box_violations = await page.locator("[data-notebook-pipeline-graph] .pipeline-node-body").evaluate_all(
                """(nodes) => {
                    const requiredSelectors = [
                        ".pipeline-node-icon",
                        ".pipeline-node-title",
                        ".pipeline-node-state",
                        ".pipeline-node-state svg",
                        ".pipeline-status-pill",
                        ".pipeline-node-alias",
                    ];
                    const optionalSelectors = [
                        ".pipeline-published-icon",
                        ".pipeline-obsolete-icon",
                    ];
                    const textSelectors = new Set([
                        ".pipeline-node-title",
                        ".pipeline-status-pill",
                        ".pipeline-node-alias",
                    ]);
                    const iconSelectors = new Set([
                        ".pipeline-node-icon",
                        ".pipeline-node-state",
                        ".pipeline-node-state svg",
                        ".pipeline-published-icon",
                        ".pipeline-obsolete-icon",
                    ]);
                    const selectors = requiredSelectors.concat(optionalSelectors);
                    const tolerance = 1;
                    const violations = [];
                    const visible = (element) => {
                        const style = window.getComputedStyle(element);
                        return style.display !== "none" && style.visibility !== "hidden";
                    };
                    nodes.forEach((node, nodeIndex) => {
                        const nodeRect = node.getBoundingClientRect();
                        requiredSelectors.forEach((selector) => {
                            if (!node.querySelector(selector)) {
                                violations.push({ nodeIndex, selector, reason: "missing" });
                            }
                        });
                        selectors.forEach((selector) => {
                            node.querySelectorAll(selector).forEach((element) => {
                                if (!visible(element)) {
                                    return;
                                }
                                const rect = element.getBoundingClientRect();
                                const text = (element.textContent || "").trim();
                                if (rect.width <= 0 || rect.height <= 0) {
                                    violations.push({ nodeIndex, selector, reason: "zero-size", text });
                                }
                                if (textSelectors.has(selector) && !text) {
                                    violations.push({ nodeIndex, selector, reason: "empty-text" });
                                }
                                if (textSelectors.has(selector) && rect.height < 8) {
                                    violations.push({ nodeIndex, selector, reason: "text-too-short", height: rect.height, text });
                                }
                                if (iconSelectors.has(selector) && (rect.width < 10 || rect.height < 10)) {
                                    violations.push({ nodeIndex, selector, reason: "icon-too-small", width: rect.width, height: rect.height });
                                }
                                const outOfBounds = (
                                    rect.left < nodeRect.left - tolerance ||
                                    rect.top < nodeRect.top - tolerance ||
                                    rect.right > nodeRect.right + tolerance ||
                                    rect.bottom > nodeRect.bottom + tolerance
                                );
                                if (outOfBounds) {
                                    violations.push({
                                        nodeIndex,
                                        selector,
                                        reason: "outside-stage-box",
                                        rect: {
                                            left: rect.left,
                                            top: rect.top,
                                            right: rect.right,
                                            bottom: rect.bottom,
                                        },
                                        nodeRect: {
                                            left: nodeRect.left,
                                            top: nodeRect.top,
                                            right: nodeRect.right,
                                            bottom: nodeRect.bottom,
                                        },
                                        text,
                                    });
                                }
                            });
                        });
                    });
                    return violations;
                }"""
            )
            if stage_box_violations:
                raise RuntimeError(
                    "Pipeline graph rendered unreadable or border-clipped stage box content: "
                    f"{stage_box_violations[:5]}"
                )
            node_icon_width = await page.locator("[data-notebook-pipeline-graph] .pipeline-node-icon").first.evaluate(
                "(node) => node.getBoundingClientRect().width"
            )
            if node_icon_width < 20:
                raise RuntimeError(f"Pipeline table icon rendered too small: {node_icon_width}px")
            node_state_count = await page.locator("[data-notebook-pipeline-graph] .pipeline-node-state").count()
            if node_state_count < 3:
                raise RuntimeError(f"Pipeline graph rendered too few node state circles: {node_state_count}")
            marker_end = await page.locator("[data-notebook-pipeline-graph] .pipeline-edge").first.get_attribute("marker-end")
            if marker_end != "url(#pipeline-arrowhead)":
                raise RuntimeError("Pipeline edge did not render with an arrowhead marker.")
            await page.locator("[data-notebook-pipeline-table] tr").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            if await page.locator("[data-query-cell]:visible").count() != 1:
                raise RuntimeError("Pipeline mode should focus one selected stage cell.")

            await page.evaluate(
                """
                () => {
                  const graph = document.querySelector('[data-notebook-pipeline-graph]');
                  const rect = graph.getBoundingClientRect();
                  window.scrollTo(0, window.scrollY + rect.top - 96);
                }
                """
            )
            await page.wait_for_timeout(100)
            graph_scroll_start = await page.evaluate("() => window.scrollY")
            await page.locator('[data-pipeline-stage-node="stage-scope"]').first.click()
            await page.wait_for_timeout(150)
            graph_scroll_after_scope = await page.evaluate("() => window.scrollY")
            visible_cell_id = await page.locator("[data-query-cell]:visible").first.get_attribute("data-cell-id")
            if visible_cell_id != "cell-scope":
                raise RuntimeError("Selecting a graph stage did not focus its owning cell.")
            assert_pipeline_stage_title_alignment(await pipeline_stage_title_positions(page))
            scope_focus_gap = await pipeline_focused_cell_gap(page)
            await page.locator('[data-pipeline-stage-node="stage-raw"]').first.click()
            await page.wait_for_timeout(150)
            graph_scroll_after_raw = await page.evaluate("() => window.scrollY")
            visible_cell_id = await page.locator("[data-query-cell]:visible").first.get_attribute("data-cell-id")
            if visible_cell_id != "cell-raw":
                raise RuntimeError("Selecting a second graph stage did not focus its owning cell.")
            assert_pipeline_stage_title_alignment(await pipeline_stage_title_positions(page))
            raw_focus_gap = await pipeline_focused_cell_gap(page)
            await page.locator('[data-pipeline-stage-node="stage-final"]').first.click()
            await page.wait_for_timeout(150)
            graph_scroll_after_final = await page.evaluate("() => window.scrollY")
            visible_cell_id = await page.locator("[data-query-cell]:visible").first.get_attribute("data-cell-id")
            if visible_cell_id != "cell-final":
                raise RuntimeError("Selecting a final graph stage did not focus its owning cell.")
            assert_pipeline_stage_title_alignment(await pipeline_stage_title_positions(page))
            final_focus_gap = await pipeline_focused_cell_gap(page)
            assert_pipeline_focused_cell_gap_alignment([scope_focus_gap, raw_focus_gap, final_focus_gap])
            max_graph_scroll_delta = max(
                abs(graph_scroll_after_scope - graph_scroll_start),
                abs(graph_scroll_after_raw - graph_scroll_after_scope),
                abs(graph_scroll_after_final - graph_scroll_after_raw),
            )
            if max_graph_scroll_delta > 2:
                raise RuntimeError(
                    "Selecting graph stage boxes should not jump-scroll the viewport: "
                    f"start={graph_scroll_start}, scope={graph_scroll_after_scope}, "
                    f"raw={graph_scroll_after_raw}, final={graph_scroll_after_final}"
                )

            await page.evaluate(
                """
                () => {
                  const graph = document.querySelector('[data-notebook-pipeline-graph]');
                  const rect = graph.getBoundingClientRect();
                  window.scrollTo(0, window.scrollY + rect.bottom + 36);
                }
                """
            )
            await page.wait_for_timeout(100)
            graph_rect_before_row_select = await page.evaluate(
                """
                () => {
                  const rect = document.querySelector('[data-notebook-pipeline-graph]').getBoundingClientRect();
                  return { top: rect.top, bottom: rect.bottom, viewportHeight: window.innerHeight };
                }
                """
            )
            await page.locator('[data-pipeline-stage-row="stage-scope"]').first.click()
            await page.wait_for_timeout(150)
            assert_pipeline_stage_title_alignment(await pipeline_stage_title_positions(page))
            graph_rect_after_row_select = await page.evaluate(
                """
                () => {
                  const rect = document.querySelector('[data-notebook-pipeline-graph]').getBoundingClientRect();
                  return { top: rect.top, bottom: rect.bottom, viewportHeight: window.innerHeight };
                }
                """
            )
            if graph_rect_before_row_select["bottom"] >= 0:
                raise RuntimeError(
                    "Stage table scroll setup did not move the graph above the viewport: "
                    f"{graph_rect_before_row_select}"
                )
            if (
                graph_rect_after_row_select["top"] < 8
                or graph_rect_after_row_select["bottom"] > graph_rect_after_row_select["viewportHeight"] - 8
            ):
                raise RuntimeError(
                    "Selecting from the stage table should minimally reveal the graph: "
                    f"before={graph_rect_before_row_select}, after={graph_rect_after_row_select}"
                )

            title_color = await page.locator("[data-cell-stage-title-input]").first.evaluate(
                "(node) => getComputedStyle(node).color"
            )
            if title_color in {"rgb(213, 43, 30)", "rgba(213, 43, 30, 1)"}:
                raise RuntimeError("Stage title input is using status color instead of black text.")

            row_title = await page.locator('[data-pipeline-stage-row="stage-scope"]').first.get_attribute("title")
            if "Depends on raw" not in (row_title or ""):
                raise RuntimeError("Stage table tooltip did not expose the description.")

            await page.locator('[data-pipeline-stage-row="stage-scope"]').first.click()
            visible_cell_id = await page.locator("[data-query-cell]:visible").first.get_attribute("data-cell-id")
            if visible_cell_id != "cell-scope":
                raise RuntimeError("Selecting a stage did not focus its owning cell.")
            await page.wait_for_function(
                """
                () => {
                  const cell = Array.from(document.querySelectorAll('[data-query-cell]'))
                    .find((candidate) => !candidate.hidden);
                  const message = cell?.querySelector('[data-query-source-validation-message]')?.textContent || '';
                  return message.includes('Sources checked') && !message.includes('Missing sources');
                }
                """,
                timeout=args.timeout_ms,
            )

            await page.locator("[data-query-cell]:visible [data-run-cell]").first.evaluate("(button) => button.click()")
            await page.wait_for_function(
                """
                () => Array.from(document.querySelectorAll('[data-pipeline-stage-row]'))
                  .some((row) => row.dataset.pipelineStageRow === 'stage-scope' && row.textContent.includes('OK'))
                """,
                timeout=args.timeout_ms,
            )
            for _ in range(30):
                if last_query_job_sql:
                    break
                await page.wait_for_timeout(250)
            if stage_scope_run_count != 1:
                raise RuntimeError(f"Pipeline Run Cell did not materialize the selected stage: {stage_scope_run_count}")
            if "stage.raw" in last_query_job_sql:
                raise RuntimeError("Pipeline Run Cell submitted an unresolved stage alias to the query engine.")
            if "s3.stage_bucket._bdw_stages.notebook.raw.data.parquet" not in last_query_job_sql:
                raise RuntimeError("Pipeline Run Cell did not rewrite the predecessor stage alias to its materialized S3 path.")

            await page.locator("[data-run-notebook-pipeline]").first.click()
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node-state-ok').length >= 3",
                timeout=args.timeout_ms,
            )
            if "OK" not in await page.locator('[data-pipeline-stage-row="stage-raw"]').first.inner_text():
                raise RuntimeError("Completed pipeline stage did not show an OK status.")
            if await page.locator("[data-notebook-pipeline-graph] .pipeline-node-state-ok").count() < 3:
                raise RuntimeError("Completed pipeline stages did not show green OK state circles.")
            run_button_rect = await page.locator("[data-run-notebook-pipeline]").first.evaluate(
                """
                (button) => {
                  const rect = button.getBoundingClientRect();
                  const header = button.closest(".notebook-pipeline-header");
                  const status = document.querySelector("[data-notebook-pipeline-status]");
                  const headerRect = header?.getBoundingClientRect();
                  const statusRect = status?.getBoundingClientRect();
                  const buttonCenterY = rect.top + (rect.height / 2);
                  const statusCenterY = statusRect ? statusRect.top + (statusRect.height / 2) : null;
                  return {
                    left: rect.left,
                    top: rect.top,
                    right: rect.right,
                    bottom: rect.bottom,
                    width: rect.width,
                    insidePipelineHeader: Boolean(header),
                    hasModeStrip: Boolean(document.querySelector("[data-notebook-mode-strip]")),
                    headerTop: headerRect?.top ?? null,
                    headerBottom: headerRect?.bottom ?? null,
                    statusButtonCenterDelta: statusCenterY === null ? null : Math.abs(buttonCenterY - statusCenterY),
                    viewportWidth: window.innerWidth,
                    documentScrollWidth: document.documentElement.scrollWidth,
                    documentClientWidth: document.documentElement.clientWidth,
                  };
                }
                """
            )
            if run_button_rect["left"] < 0 or run_button_rect["right"] > run_button_rect["viewportWidth"]:
                raise RuntimeError(f"Run pipeline button was pushed outside the viewport: {run_button_rect}")
            if run_button_rect["documentScrollWidth"] > run_button_rect["documentClientWidth"] + 2:
                raise RuntimeError(f"Pipeline view introduced page-level horizontal overflow: {run_button_rect}")
            if not run_button_rect["insidePipelineHeader"] or run_button_rect["hasModeStrip"]:
                raise RuntimeError(f"Run pipeline button should live in the pipeline header without a separate strip: {run_button_rect}")
            if (
                run_button_rect["top"] < run_button_rect["headerTop"] - 1
                or run_button_rect["bottom"] > run_button_rect["headerBottom"] + 1
            ):
                raise RuntimeError(f"Run pipeline button was not contained by the pipeline header: {run_button_rect}")
            if run_button_rect["statusButtonCenterDelta"] is None or run_button_rect["statusButtonCenterDelta"] > 14:
                raise RuntimeError(f"Run pipeline button was not aligned with the pipeline status: {run_button_rect}")

            await page.locator('[data-pipeline-stage-menu="stage-raw"]').first.click()
            await page.locator(".pipeline-context-menu.workspace-action-menu-panel").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            menu_icon_count = await page.locator(".pipeline-context-menu .pipeline-menu-icon").count()
            if menu_icon_count < 8:
                raise RuntimeError(f"Stage context menu rendered too few action icons: {menu_icon_count}")
            await page.locator('[data-pipeline-menu-action="copy-path"]').first.click()
            copied = await page.evaluate("() => navigator.clipboard.readText()")
            if "s3.stage_bucket._bdw_stages.notebook.raw.data.parquet" not in copied:
                raise RuntimeError(f"Copy target path wrote an unexpected value: {copied}")

            await page.locator('[data-pipeline-stage-menu="stage-final"]').first.click()
            await page.locator('[data-pipeline-menu-action="publish"]').first.click()
            await page.locator("[data-data-product-dialog]").first.wait_for(state="visible", timeout=args.timeout_ms)
            await page.keyboard.press("Escape")

            await page.locator('[data-pipeline-stage-menu="stage-raw"]').first.click()
            await page.locator('[data-pipeline-menu-action="run"]').first.click()
            await page.wait_for_function(
                """
                () => Array.from(document.querySelectorAll('[data-pipeline-stage-row]'))
                  .some((row) => row.dataset.pipelineStageRow === 'stage-scope' && row.textContent.includes('Obsolete'))
                """,
                timeout=args.timeout_ms,
            )

            await page.route("**/sidebar?**", handle_sidebar)
            await page.locator('[data-pipeline-stage-menu="stage-raw"]').first.click()
            await page.locator('[data-pipeline-menu-action="inspect"]').first.click()
            await page.locator(".is-pipeline-inspect-flash").first.wait_for(
                state="attached",
                timeout=args.timeout_ms,
            )

            await page.locator('[data-pipeline-stage-menu="stage-raw"]').first.click()
            await page.locator('[data-pipeline-menu-action="derive"]').first.click()
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-query-cell]').length >= 4",
                timeout=args.timeout_ms,
            )
            derived_sql = await page.evaluate(
                """
                ({ storageKey, notebookId }) => {
                  const state = JSON.parse(window.localStorage.getItem(storageKey) || "{}");
                  const cells = state[notebookId]?.cells || [];
                  return cells.find((cell) => cell.stage?.title === "Raw Scope Derived")?.sql || "";
                }
                """,
                {
                    "storageKey": NOTEBOOK_METADATA_STORAGE_KEY,
                    "notebookId": notebook_id,
                },
            )
            if "FROM s3.stage_bucket._bdw_stages.notebook.raw.data.parquet" not in derived_sql:
                raise RuntimeError(f"Derived stage did not use the S3 storage reference: {derived_sql}")
            if derived_sql.rstrip().endswith(";"):
                raise RuntimeError(f"Derived stage SQL should not end with a semicolon: {derived_sql}")

            await page.locator('[data-pipeline-stage-menu="stage-raw"]').first.click()
            await page.locator('[data-pipeline-menu-action="fork"]').first.click()
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-query-cell]').length >= 5",
                timeout=args.timeout_ms,
            )
            fork_sql = await page.evaluate(
                """
                ({ storageKey, notebookId }) => {
                  const state = JSON.parse(window.localStorage.getItem(storageKey) || "{}");
                  const cells = state[notebookId]?.cells || [];
                  return cells.find((cell) => cell.stage?.title === "Raw Scope Fork")?.sql || "";
                }
                """,
                {
                    "storageKey": NOTEBOOK_METADATA_STORAGE_KEY,
                    "notebookId": notebook_id,
                },
            )
            if "FROM s3.stage_bucket._bdw_stages.notebook.raw.data.parquet" not in fork_sql:
                raise RuntimeError(f"Forked stage did not use the S3 storage reference: {fork_sql}")

            await page.locator('[data-pipeline-stage-menu="stage-raw"]').first.click()
            await page.locator('[data-pipeline-menu-action="delete"]').first.click()
            await page.locator("[data-confirm-dialog]").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            confirm_text = await page.locator("[data-confirm-dialog]").first.inner_text()
            if "Delete stage" not in confirm_text:
                raise RuntimeError("Delete stage did not open the confirmation dialog.")
            await page.locator("[data-confirm-dialog] [data-confirm-submit]").first.click()
            await page.wait_for_function(
                """
                ({ storageKey, notebookId }) => {
                  const state = JSON.parse(window.localStorage.getItem(storageKey) || "{}");
                  const cells = state[notebookId]?.cells || [];
                  return !cells.some((cell) => cell.cellId === "cell-raw");
                }
                """,
                arg={
                    "storageKey": NOTEBOOK_METADATA_STORAGE_KEY,
                    "notebookId": notebook_id,
                },
                timeout=args.timeout_ms,
            )
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

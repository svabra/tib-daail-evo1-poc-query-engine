from __future__ import annotations

import argparse
import asyncio
import json
import re
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


NOTEBOOK_METADATA_STORAGE_KEY = "bdw.notebookMeta.v1"
NOTEBOOK_LAST_STORAGE_KEY = "bdw.lastNotebook.v1"
FRESH_NOTEBOOK_ID = "local-notebook-pipeline-smoke"
DATA_PIPELINES_TREE_PATH = ["PoC Tests", "Performance Evaluation", "Data Pipelines"]
DATA_PIPELINE_NOTEBOOK_IDS = [
    "mwa-abrechnung-s3-parquet-pipeline",
    "kostenbelege-3-1-s3-parquet-pipeline",
]
STAGE_REFERENCE_RE = re.compile(r"(?<![A-Za-z0-9_$])stage\.([A-Za-z_][A-Za-z0-9_$]*)", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise Notebook Pipeline mode, graph/table interactions, and stage actions."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


def stage_alias(value: object, fallback: object = "stage") -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or fallback or "stage").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "stage"


def normalize_pipeline_paths(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    normalized = []
    seen = set()
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            continue
        terminal_stage_id = str(item.get("terminalStageId") or item.get("terminal_stage_id") or "").strip()
        path_id = str(item.get("pathId") or item.get("path_id") or "").strip()
        if not terminal_stage_id and path_id.startswith("path-"):
            terminal_stage_id = path_id[5:]
        if not path_id and terminal_stage_id:
            path_id = f"path-{terminal_stage_id}"
        if not terminal_stage_id and not path_id:
            continue
        key = terminal_stage_id or path_id
        if key in seen:
            continue
        seen.add(key)
        priority = item.get("priority") or item.get("rank") or index + 1
        try:
            priority = max(1, int(priority))
        except (TypeError, ValueError):
            priority = index + 1
        normalized.append(
            {
                "pathId": path_id,
                "terminalStageId": terminal_stage_id,
                "label": str(item.get("label") or item.get("name") or "").strip(),
                "priority": priority,
                "_index": index,
            }
        )
    normalized.sort(key=lambda item: (item["priority"], item["_index"]))
    return [
        {
            "pathId": item["pathId"],
            "terminalStageId": item["terminalStageId"],
            "label": item["label"],
            "priority": index + 1,
        }
        for index, item in enumerate(normalized)
    ]


def topo_order(
    stage_ids: list[str],
    predecessors_by_id: dict[str, list[str]],
    cell_index_by_id: dict[str, int],
    priority_by_id: dict[str, int] | None = None,
) -> list[str]:
    priority_by_id = priority_by_id or {}
    successors = {stage_id: [] for stage_id in stage_ids}
    indegree = {stage_id: 0 for stage_id in stage_ids}
    known_stage_ids = set(stage_ids)
    for stage_id, predecessors in predecessors_by_id.items():
        for predecessor_id in predecessors:
            if predecessor_id not in known_stage_ids:
                continue
            indegree[stage_id] += 1
            successors[predecessor_id].append(stage_id)

    def sort_key(stage_id: str) -> tuple[int, int, str]:
        return (
            priority_by_id.get(stage_id, 1_000_000),
            cell_index_by_id.get(stage_id, 1_000_000),
            stage_id,
        )

    ready = sorted([stage_id for stage_id, degree in indegree.items() if degree == 0], key=sort_key)
    ordered = []
    while ready:
        stage_id = ready.pop(0)
        ordered.append(stage_id)
        for successor_id in sorted(successors.get(stage_id, []), key=sort_key):
            indegree[successor_id] -= 1
            if indegree[successor_id] == 0:
                ready.append(successor_id)
        ready.sort(key=sort_key)
    ordered.extend(stage_id for stage_id in stage_ids if stage_id not in ordered)
    return ordered


def descendant_stage_ids(stage_id: str, edges: list[dict[str, str]]) -> set[str]:
    successors: dict[str, list[str]] = {}
    for edge in edges:
        successors.setdefault(str(edge["fromStageId"]), []).append(str(edge["toStageId"]))
    required = {stage_id}
    queue = list(successors.get(stage_id, []))
    while queue:
        current = queue.pop(0)
        if current in required:
            continue
        required.add(current)
        queue.extend(successors.get(current, []))
    return required


def graph_payload(
    cells: list[dict[str, object]],
    statuses: dict[str, str],
    revision_suffix: str,
    pipeline_paths: object | None = None,
) -> dict[str, object]:
    stage_cells = [cell for cell in cells if cell.get("language", "sql") == "sql"]
    nodes = []
    by_id = {}
    by_alias = {}
    cell_index_by_id = {}
    for index, cell in enumerate(stage_cells):
        stage = cell.get("stage") or {}
        stage_id = str(stage.get("stageId") or f"stage-{index + 1}")
        alias = stage.get("alias") or f"stage_{index + 1}"
        by_alias[stage_alias(alias)] = stage_id
        cell_index_by_id[stage_id] = index
    predecessors_by_id = {}
    for index, cell in enumerate(stage_cells):
        stage = cell.get("stage") or {}
        stage_id = str(stage.get("stageId") or f"stage-{index + 1}")
        predecessors = []
        for item in stage.get("predecessorStageIds") or []:
            predecessor_id = str(item).strip()
            if predecessor_id and predecessor_id not in predecessors:
                predecessors.append(predecessor_id)
        for match in STAGE_REFERENCE_RE.finditer(str(cell.get("sql") or "")):
            predecessor_id = by_alias.get(stage_alias(match.group(1)))
            if predecessor_id and predecessor_id != stage_id and predecessor_id not in predecessors:
                predecessors.append(predecessor_id)
        predecessors_by_id[stage_id] = predecessors

    stage_ids = list(cell_index_by_id)
    fallback_order = topo_order(stage_ids, predecessors_by_id, cell_index_by_id)
    fallback_index = {stage_id: index for index, stage_id in enumerate(fallback_order)}
    successors_by_id = {stage_id: [] for stage_id in stage_ids}
    edges = []
    for stage_id, predecessors in predecessors_by_id.items():
        for predecessor in predecessors:
            edges.append({"fromStageId": predecessor, "toStageId": stage_id})
            if predecessor in successors_by_id:
                successors_by_id[predecessor].append(stage_id)
    terminal_stage_ids = [
        stage_id for stage_id in fallback_order if not [successor for successor in successors_by_id.get(stage_id, []) if successor in by_alias.values()]
    ]
    metadata_by_terminal = {}
    prioritized_terminal_ids = []
    for path in normalize_pipeline_paths(pipeline_paths):
        terminal_stage_id = str(path.get("terminalStageId") or "").strip()
        if terminal_stage_id in terminal_stage_ids and terminal_stage_id not in metadata_by_terminal:
            metadata_by_terminal[terminal_stage_id] = path
            prioritized_terminal_ids.append(terminal_stage_id)
    ordered_terminal_ids = [
        *prioritized_terminal_ids,
        *[stage_id for stage_id in terminal_stage_ids if stage_id not in metadata_by_terminal],
    ]
    title_by_stage_id = {
        str((cell.get("stage") or {}).get("stageId") or f"stage-{index + 1}"):
        str((cell.get("stage") or {}).get("title") or (cell.get("stage") or {}).get("alias") or f"Stage {index + 1}")
        for index, cell in enumerate(stage_cells)
    }
    paths = []
    for index, terminal_stage_id in enumerate(ordered_terminal_ids):
        required = {terminal_stage_id}
        queue = list(predecessors_by_id.get(terminal_stage_id, []))
        while queue:
            predecessor_id = queue.pop(0)
            if predecessor_id in required:
                continue
            required.add(predecessor_id)
            queue.extend(predecessors_by_id.get(predecessor_id, []))
        path_stage_ids = [stage_id for stage_id in fallback_order if stage_id in required]
        metadata = metadata_by_terminal.get(terminal_stage_id, {})
        terminal_title = title_by_stage_id.get(terminal_stage_id, terminal_stage_id)
        paths.append(
            {
                "pathId": metadata.get("pathId") or f"path-{terminal_stage_id}",
                "label": metadata.get("label") or terminal_title,
                "terminalStageId": terminal_stage_id,
                "terminalStageTitle": terminal_title,
                "stageIds": path_stage_ids,
                "priority": index + 1,
            }
        )
    priority_by_id = {}
    for path in paths:
        for stage_id in path["stageIds"]:
            priority_by_id.setdefault(stage_id, int(path["priority"]))
    order = topo_order(stage_ids, predecessors_by_id, cell_index_by_id, priority_by_id)

    layer_by_id = {}
    for stage_id in order:
        layer_by_id[stage_id] = max(
            [layer_by_id.get(predecessor_id, 0) + 1 for predecessor_id in predecessors_by_id.get(stage_id, [])]
            or [0]
        )

    cells_by_stage_id = {
        str((cell.get("stage") or {}).get("stageId") or f"stage-{index + 1}"): (index, cell)
        for index, cell in enumerate(stage_cells)
    }
    for order_index, stage_id in enumerate(order):
        index, cell = cells_by_stage_id[stage_id]
        stage = cell.get("stage") or {}
        predecessors = predecessors_by_id.get(stage_id, [])
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
            "order": order_index,
            "layer": layer_by_id.get(stage_id, 0),
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
                "rowCount": {
                    "stage-raw": 12,
                    "stage-scope": 9,
                    "stage-final": 4,
                    "stage-audit-candidates": 6,
                    "stage-audit-backlog": 2,
                }.get(stage_id, 3),
                "outputBucket": "stage-bucket",
                "outputKey": key,
                "outputPath": f"s3://stage-bucket/{key}",
                "queryPath": f"s3.stage_bucket._bdw_stages.notebook.{node['alias']}.data.parquet",
            }
            node["latestRun"] = {
                "runId": f"run-{revision_suffix}",
                "stageId": stage_id,
                "status": "completed",
                "startedAt": "2026-06-06T00:00:00Z",
                "completedAt": "2026-06-06T00:00:01.200Z",
                "durationMs": 1200,
            }
            node["outputSource"] = {
                "sourceKind": "object",
                "sourceId": "s3",
                "bucket": "stage-bucket",
                "key": key,
                "sourceDisplayName": f"{node['title']} materialized output",
                "sourcePlatform": "s3",
            }
        else:
            node["latestRevision"] = None
            node["latestRun"] = None
            node["outputSource"] = {}
        by_id[stage_id] = node
        nodes.append(node)
    for node in nodes:
        node["successorStageIds"] = successors_by_id.get(str(node["stageId"]), [])
    default_stage = next((node["stageId"] for node in nodes if node["status"] == "obsolete"), nodes[0]["stageId"])
    return {
        "notebookId": "mock-notebook",
        "notebookTitle": "Pipeline smoke",
        "version": 1,
        "nodes": nodes,
        "sourceNodes": [],
        "edges": edges,
        "diagnostics": [],
        "order": order,
        "paths": paths,
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


async def seed_forked_stage_notebook(page, timeout_ms: int) -> str:
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
                "alias": "status_pressure",
                "title": "Status Pressure",
                "description": "Status pressure branch",
                "kind": "final",
                "predecessorStageIds": ["stage-scope"],
                "materialize": True,
            },
        },
        {
            "cellId": "cell-audit-candidates",
            "language": "sql",
            "sql": "SELECT * FROM stage.scope WHERE label = 'audit';",
            "dataSources": [],
            "queryOptions": {},
            "stage": {
                "enabled": True,
                "stageId": "stage-audit-candidates",
                "alias": "audit_candidates",
                "title": "Audit Candidates",
                "description": "Audit branch candidates",
                "kind": "intermediate",
                "predecessorStageIds": ["stage-scope"],
                "materialize": True,
            },
        },
        {
            "cellId": "cell-audit-backlog",
            "language": "sql",
            "sql": "SELECT * FROM stage.audit_candidates;",
            "dataSources": [],
            "queryOptions": {},
            "stage": {
                "enabled": True,
                "stageId": "stage-audit-backlog",
                "alias": "audit_backlog",
                "title": "Audit Backlog",
                "description": "Prioritized audit backlog branch",
                "kind": "final",
                "predecessorStageIds": ["stage-audit-candidates"],
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


async def assert_data_pipeline_seed_links_if_present(page) -> None:
    seed_links = [
        page.locator(f'[data-notebook-id="{notebook_id}"]').first
        for notebook_id in DATA_PIPELINE_NOTEBOOK_IDS
    ]
    counts = [await link.count() for link in seed_links]
    if not all(counts):
        return
    for notebook_id, link in zip(DATA_PIPELINE_NOTEBOOK_IDS, seed_links):
        labels = await link.evaluate(
            """
            (link) => {
              const labels = [];
              let folder = link.closest("[data-tree-folder]");
              while (folder) {
                const label = folder.querySelector(":scope > summary .tree-folder-label")?.textContent?.trim();
                if (label) {
                  labels.unshift(label);
                }
                folder = folder.parentElement?.closest("[data-tree-folder]") || null;
              }
              return labels;
            }
            """
        )
        if labels[-len(DATA_PIPELINES_TREE_PATH) :] != DATA_PIPELINES_TREE_PATH:
            raise RuntimeError(
                f"Seeded pipeline notebook {notebook_id} was not under Data Pipelines: {labels}"
            )


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


async def app_tooltip_style_after_hover(page, locator, timeout_ms: int) -> dict[str, object]:
    await locator.hover()
    await page.wait_for_function(
        """
        () => {
          const tooltip = document.querySelector('[data-app-floating-tooltip]');
          return tooltip && !tooltip.hidden && Number(getComputedStyle(tooltip).opacity) > 0.95;
        }
        """,
        timeout=timeout_ms,
    )
    return await locator.evaluate(
        """
        (target) => {
          const tooltip = document.querySelector('[data-app-floating-tooltip]');
          const style = getComputedStyle(tooltip);
          const tooltipRect = tooltip.getBoundingClientRect();
          const targetRect = target.getBoundingClientRect();
          return {
            text: tooltip.textContent || '',
            backgroundColor: style.backgroundColor,
            color: style.color,
            borderRadius: style.borderRadius,
            fontSize: style.fontSize,
            opacity: style.opacity,
            paddingTop: style.paddingTop,
            paddingRight: style.paddingRight,
            tooltipBottom: tooltipRect.bottom,
            targetCenterY: targetRect.top + (targetRect.height / 2),
          };
        }
        """
    )


def assert_modern_app_tooltip(style: dict[str, object], expected_text: str) -> None:
    if (
        expected_text not in str(style.get("text") or "")
        or style.get("backgroundColor") != "rgba(255, 255, 255, 0.7)"
        or style.get("color") != "rgb(17, 17, 17)"
        or style.get("borderRadius") != "3px"
        or float(str(style.get("fontSize") or "0").replace("px", "")) > 12
        or float(style.get("opacity") or 0) < 0.95
        or float(str(style.get("paddingTop") or "0").replace("px", "")) < 9
        or float(str(style.get("paddingRight") or "0").replace("px", "")) < 11
        or float(style.get("tooltipBottom") or 0) > float(style.get("targetCenterY") or 0) - 6
    ):
        raise RuntimeError(f"App tooltip did not use the modern tooltip styling above the cursor: {style}")


async def main() -> None:
    args = parse_args()
    statuses = {
        "stage-raw": "planned",
        "stage-scope": "planned",
        "stage-final": "planned",
        "stage-audit-candidates": "planned",
        "stage-audit-backlog": "planned",
    }
    revision_suffix = "rev-a"
    last_cells: list[dict[str, object]] = []
    stage_scope_run_count = 0
    last_query_job_sql = ""
    pipeline_active = False
    pipeline_state_poll_count = 0
    pipeline_run_payload_stage_ids: list[str] = []
    pipeline_active_stage_timeline: list[str] = []
    pipeline_run_start_stage_id = ""
    active_pipeline_stage_ids: list[str] = []
    active_pipeline_notebook_id = "mock-notebook"

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
            graph = graph_payload(
                last_cells,
                statuses,
                revision_suffix,
                payload.get("pipelinePaths") or [],
            )
            graph["notebookId"] = payload.get("notebookId") or "mock-notebook"
            graph["notebookTitle"] = payload.get("notebookTitle") or "Pipeline smoke"
            if pipeline_active:
                graph["activeRuns"] = [
                    {
                        "runId": "pipeline-run-active",
                        "notebookId": graph["notebookId"],
                        "stageIds": active_pipeline_stage_ids
                        or ["stage-raw", "stage-scope", "stage-final", "stage-audit-candidates", "stage-audit-backlog"],
                        "status": "running",
                        "cancelRequested": False,
                    }
                ]
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(graph),
            )

        async def handle_pipeline_run(route):
            nonlocal active_pipeline_notebook_id, active_pipeline_stage_ids, pipeline_active, pipeline_run_payload_stage_ids
            nonlocal pipeline_run_start_stage_id, pipeline_state_poll_count
            payload = json.loads(route.request.post_data or "{}")
            active_pipeline_notebook_id = str(payload.get("notebookId") or "mock-notebook")
            graph = graph_payload(
                payload.get("cells") or [],
                statuses,
                revision_suffix,
                payload.get("pipelinePaths") or [],
            )
            ordered_stage_ids = [str(stage_id) for stage_id in graph.get("order") or []]
            pipeline_run_start_stage_id = str(payload.get("startStageId") or "")
            if pipeline_run_start_stage_id and pipeline_run_start_stage_id in ordered_stage_ids:
                required_stage_ids = descendant_stage_ids(
                    pipeline_run_start_stage_id,
                    [
                        {"fromStageId": str(edge.get("fromStageId") or ""), "toStageId": str(edge.get("toStageId") or "")}
                        for edge in graph.get("edges") or []
                    ],
                )
                active_pipeline_stage_ids = [
                    stage_id for stage_id in ordered_stage_ids if stage_id in required_stage_ids
                ]
            else:
                active_pipeline_stage_ids = ordered_stage_ids
            pipeline_run_payload_stage_ids = list(active_pipeline_stage_ids)
            pipeline_active = True
            pipeline_state_poll_count = 0
            pipeline_active_stage_timeline.clear()
            for stage_id in ordered_stage_ids:
                statuses[stage_id] = "queued" if stage_id in active_pipeline_stage_ids else "valid"
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "version": 2,
                        "records": [],
                        "activeRuns": [
                            {
                                "runId": "pipeline-run-active",
                                "notebookId": active_pipeline_notebook_id,
                                "stageIds": active_pipeline_stage_ids,
                                "status": "running",
                                "cancelRequested": False,
                            }
                        ],
                    }
                ),
            )
        async def handle_stage_run(route):
            nonlocal revision_suffix
            revision_suffix = "rev-b"
            statuses.update(
                {
                    "stage-raw": "valid",
                    "stage-scope": "obsolete",
                    "stage-final": "obsolete",
                    "stage-audit-candidates": "obsolete",
                    "stage-audit-backlog": "obsolete",
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
                    "stage-audit-candidates": "planned",
                    "stage-audit-backlog": "planned",
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
            nonlocal pipeline_active, pipeline_state_poll_count
            active_runs = []
            if pipeline_active and active_pipeline_stage_ids:
                pipeline_state_poll_count += 1
                current_stage_id = active_pipeline_stage_ids[
                    min(pipeline_state_poll_count - 1, len(active_pipeline_stage_ids) - 1)
                ]
                if not pipeline_active_stage_timeline or pipeline_active_stage_timeline[-1] != current_stage_id:
                    pipeline_active_stage_timeline.append(current_stage_id)
                for index, stage_id in enumerate(active_pipeline_stage_ids):
                    current_index = active_pipeline_stage_ids.index(current_stage_id)
                    statuses[stage_id] = (
                        "valid"
                        if index < current_index
                        else "running"
                        if stage_id == current_stage_id
                        else "queued"
                    )
                active_runs = [
                    {
                        "runId": "pipeline-run-active",
                        "notebookId": active_pipeline_notebook_id,
                        "stageIds": active_pipeline_stage_ids,
                        "currentStageId": current_stage_id,
                        "status": "running",
                        "cancelRequested": False,
                    }
                ]
                if pipeline_state_poll_count >= len(active_pipeline_stage_ids) + 1:
                    for stage_id in active_pipeline_stage_ids:
                        statuses[stage_id] = "valid"
                    active_runs = []
                    pipeline_active = False
            await route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"version": 1, "records": [], "activeRuns": active_runs}),
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
                    <details class="tree-folder" data-tree-folder open>
                      <summary class="tree-folder-summary"><span class="tree-folder-label">PoC Tests</span></summary>
                      <div class="tree-children" data-tree-children>
                        <details class="tree-folder" data-tree-folder open>
                          <summary class="tree-folder-summary"><span class="tree-folder-label">Performance Evaluation</span></summary>
                          <div class="tree-children" data-tree-children>
                            <details class="tree-folder" data-tree-folder open>
                              <summary class="tree-folder-summary"><span class="tree-folder-label">Data Pipelines</span></summary>
                              <div class="tree-children" data-tree-children>
                                <a class="notebook-link notebook-tree-leaf" data-notebook-id="mwa-abrechnung-s3-parquet-pipeline">MWA Abrechnung (3.2) S3 Parquet Pipeline</a>
                                <a class="notebook-link notebook-tree-leaf" data-notebook-id="kostenbelege-3-1-s3-parquet-pipeline">Kostenbelege (3.1) S3 Parquet Pipeline</a>
                              </div>
                            </details>
                          </div>
                        </details>
                      </div>
                    </details>
                  </details>
                  <details data-data-sources-section open>
                    <summary>Data Sources</summary>
                    <details data-source-catalog data-source-catalog-name="s3" data-source-catalog-source-id="s3" open>
                      <summary>Shared Workspace</summary>
                      <details data-source-schema data-source-bucket="stage-bucket" data-source-schema-key="s3:stage-bucket">
                        <summary>stage-bucket</summary>
                        <ul class="source-object-list">
                          <li
                            class="source-object"
                            data-source-object
                            data-s3-bucket="stage-bucket"
                            data-s3-key="_bdw_stages/notebook/raw/rev-b/data.parquet"
                          >
                            <span class="source-node-label"><span>Raw materialized output</span></span>
                          </li>
                        </ul>
                      </details>
                    </details>
                  </details>
                  <details data-query-monitor-section open><summary>Query Monitoring</summary></details>
                </aside>
                """,
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

        await page.route("**/api/materialized-stages/graph", handle_graph)
        await page.route("**/api/materialized-stages/pipeline/run", handle_pipeline_run)
        await page.route("**/api/materialized-stages/stages/stage-raw/run", handle_stage_run)
        await page.route("**/api/materialized-stages/stages/stage-scope/run", handle_stage_scope_run)
        await page.route("**/api/materialized-stages/stages/stage-raw/stop", handle_stage_stop)
        await page.route("**/api/materialized-stages/state", handle_stage_state)
        await page.route("**/api/query-sources/validate", handle_query_source_validation)
        await page.route("**/api/query-jobs", handle_query_job)

        try:
            await open_query_workbench(page, args)
            await assert_data_pipeline_seed_links_if_present(page)

            if await page.locator("[data-notebook-pipeline-panel]").is_visible():
                raise RuntimeError("Exploration mode should hide the pipeline panel on a fresh notebook.")
            mode_toggle = page.locator("[data-notebook-mode-toggle]").first
            exploration_tooltip = await mode_toggle.get_attribute("title")
            if "Notebook mode: Exploration" not in (exploration_tooltip or "") or "links SQL cells" not in (exploration_tooltip or ""):
                raise RuntimeError("Exploration mode switch tooltip was missing or unclear.")
            native_tooltip_style = await app_tooltip_style_after_hover(
                page,
                page.locator("[data-sidebar-resizer]").first,
                args.timeout_ms,
            )
            assert_modern_app_tooltip(native_tooltip_style, "Drag to resize navigation")
            await page.mouse.move(900, 20)
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

            notebook_id = await seed_forked_stage_notebook(page, args.timeout_ms)

            if await page.locator("[data-notebook-pipeline-panel]").is_visible():
                raise RuntimeError("Exploration mode should hide the pipeline panel.")

            await page.locator("[data-notebook-mode-toggle]").first.click()
            await page.locator("[data-notebook-pipeline-graph] .pipeline-node").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node').length >= 5",
                timeout=args.timeout_ms,
            )
            node_width = await page.locator("[data-notebook-pipeline-graph] .pipeline-node").first.evaluate(
                "(node) => node.getBoundingClientRect().width"
            )
            if node_width < 350:
                raise RuntimeError(f"Pipeline graph node rendered below its minimum width: {node_width}px")
            if node_width > 380:
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
                        ".pipeline-node-alias",
                        ".pipeline-stage-action-group",
                        ".pipeline-stage-action-button",
                    ];
                    const optionalSelectors = [
                        ".pipeline-published-icon",
                        ".pipeline-obsolete-icon",
                    ];
                    const textSelectors = new Set([
                        ".pipeline-node-title",
                        ".pipeline-node-alias",
                    ]);
                    const iconSelectors = new Set([
                        ".pipeline-node-icon",
                        ".pipeline-node-state",
                        ".pipeline-node-state svg",
                        ".pipeline-published-icon",
                        ".pipeline-obsolete-icon",
                        ".pipeline-stage-action-group",
                        ".pipeline-stage-action-button",
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
            if node_icon_width < 38:
                raise RuntimeError(f"Pipeline table icon rendered too small: {node_icon_width}px")
            node_state_count = await page.locator("[data-notebook-pipeline-graph] .pipeline-node-state").count()
            if node_state_count < 5:
                raise RuntimeError(f"Pipeline graph rendered too few node state circles: {node_state_count}")
            graph_action_alignment = await page.locator(
                "[data-notebook-pipeline-graph] .pipeline-node-body"
            ).evaluate_all(
                """
                (nodes) => nodes.map((node, nodeIndex) => {
                  const title = node.querySelector('.pipeline-node-title');
                  const action = node.querySelector('.pipeline-stage-action-group-graph');
                  const state = node.querySelector('.pipeline-node-state');
                  const titleRect = title?.getBoundingClientRect();
                  const actionRect = action?.getBoundingClientRect();
                  const stateRect = state?.getBoundingClientRect();
                  const centerY = (rect) => rect.top + rect.height / 2;
                  return {
                    nodeIndex,
                    missing: !(titleRect && actionRect && stateRect),
                    actionStateCenterDelta: actionRect && stateRect
                      ? Math.abs(centerY(actionRect) - centerY(stateRect))
                      : null,
                    titleActionCenterDelta: titleRect && actionRect
                      ? Math.abs(centerY(titleRect) - centerY(actionRect))
                      : null,
                    titleStateCenterDelta: titleRect && stateRect
                      ? Math.abs(centerY(titleRect) - centerY(stateRect))
                      : null,
                  };
                })
                """
            )
            misaligned_graph_actions = [
                item
                for item in graph_action_alignment
                if (
                    item.get("missing")
                    or float(item.get("actionStateCenterDelta") or 0) > 1.25
                    or float(item.get("titleActionCenterDelta") or 0) > 1.25
                    or float(item.get("titleStateCenterDelta") or 0) > 1.25
                )
            ]
            if misaligned_graph_actions:
                raise RuntimeError(
                    "Pipeline graph stage run/status controls were not centered on the title line: "
                    f"{misaligned_graph_actions}"
                )
            graph_stage_run_count = await page.locator("[data-notebook-pipeline-graph] [data-run-pipeline-stage]").count()
            table_stage_run_count = await page.locator("[data-notebook-pipeline-table] [data-run-pipeline-stage]").count()
            if graph_stage_run_count < 5 or table_stage_run_count < 5:
                raise RuntimeError(
                    "Pipeline stage run actions did not render in both graph and table: "
                    f"graph={graph_stage_run_count}, table={table_stage_run_count}"
                )
            graph_run_from_count = await page.locator("[data-notebook-pipeline-graph] [data-run-pipeline-from-stage]").count()
            table_run_from_count = await page.locator("[data-notebook-pipeline-table] [data-run-pipeline-from-stage]").count()
            if graph_run_from_count < 5 or table_run_from_count < 5:
                raise RuntimeError(
                    "Pipeline-from-stage actions did not render in both graph and table: "
                    f"graph={graph_run_from_count}, table={table_run_from_count}"
                )
            table_action_status_alignment = await page.locator(
                "[data-pipeline-stage-row]"
            ).evaluate_all(
                """
                (rows) => rows.map((row) => {
                  const runCell = row.cells[1];
                  const statusCell = row.cells[2];
                  const action = row.querySelector('.pipeline-stage-action-group-table');
                  const statusIcon = row.querySelector('.pipeline-status-icon');
                  const actionRect = action?.getBoundingClientRect();
                  const iconRect = statusIcon?.getBoundingClientRect();
                  const statusRect = statusCell?.getBoundingClientRect();
                  return {
                    stageId: row.dataset.pipelineStageRow || '',
                    missing: !(runCell && statusCell && actionRect && iconRect && statusRect),
                    actionStatusCenterYDelta: actionRect && iconRect
                      ? Math.abs(
                          (actionRect.top + actionRect.height / 2) -
                          (iconRect.top + iconRect.height / 2)
                        )
                      : null,
                    statusCenterXDelta: iconRect && statusRect
                      ? Math.abs(
                          (iconRect.left + iconRect.width / 2) -
                          (statusRect.left + statusRect.width / 2)
                        )
                      : null,
                  };
                })
                """
            )
            misaligned_table_actions = [
                item
                for item in table_action_status_alignment
                if (
                    item.get("missing")
                    or float(item.get("actionStatusCenterYDelta") or 0) > 0.5
                    or float(item.get("statusCenterXDelta") or 0) > 0.75
                )
            ]
            if misaligned_table_actions:
                raise RuntimeError(
                    "Pipeline table run/status icons were not centered in their columns: "
                    f"{misaligned_table_actions}"
                )
            table_end_geometry = await page.locator(".notebook-pipeline-table").first.evaluate(
                """
                (table) => {
                  const bodyRow = table.tBodies?.[0]?.rows?.[0];
                  const headerRow = table.tHead?.rows?.[0];
                  const lastCell = bodyRow?.cells?.[6];
                  const lastHeader = headerRow?.cells?.[6];
                  const tableRect = table.getBoundingClientRect();
                  const cellRect = lastCell?.getBoundingClientRect();
                  const headerRect = lastHeader?.getBoundingClientRect();
                  const buttonRect = lastCell?.querySelector('.pipeline-row-menu-button')?.getBoundingClientRect();
                  return {
                    missing: !(cellRect && headerRect && buttonRect),
                    tableRight: tableRect.right,
                    lastCellWidth: cellRect?.width ?? null,
                    lastHeaderWidth: headerRect?.width ?? null,
                    lastCellRightDelta: cellRect ? Math.abs(tableRect.right - cellRect.right) : null,
                    actionRightPadding: cellRect && buttonRect ? cellRect.right - buttonRect.right : null,
                  };
                }
                """
            )
            if (
                table_end_geometry.get("missing")
                or float(table_end_geometry.get("lastCellWidth") or 0) > 72
                or float(table_end_geometry.get("lastHeaderWidth") or 0) > 72
                or float(table_end_geometry.get("lastCellRightDelta") or 0) > 2
                or float(table_end_geometry.get("actionRightPadding") or 0) > 18
            ):
                raise RuntimeError(f"Pipeline stage table left an oversized trailing action gap: {table_end_geometry}")

            priority_button = page.locator("[data-pipeline-priority-paths]").first
            await priority_button.wait_for(state="visible", timeout=args.timeout_ms)
            priority_summary = (await priority_button.inner_text()).strip()
            if "Status Pressure first" not in priority_summary:
                raise RuntimeError(f"Priority paths should default to graph/cell order: {priority_summary}")
            await priority_button.click()
            priority_popover = page.locator("[data-pipeline-priority-popover]").first
            await priority_popover.wait_for(state="visible", timeout=args.timeout_ms)
            if await priority_popover.locator("[data-pipeline-priority-row]").count() != 2:
                raise RuntimeError("Priority paths popover did not show the two terminal paths.")
            audit_row = priority_popover.locator('[data-pipeline-terminal-stage-id="stage-audit-backlog"]').first
            await audit_row.locator('[data-pipeline-path-move="up"]').click()
            await page.wait_for_function(
                """
                () => {
                  const summary = document.querySelector('[data-pipeline-priority-summary]')?.textContent || '';
                  return summary.includes('Audit Backlog first');
                }
                """,
                timeout=args.timeout_ms,
            )
            stored_priority_paths = await page.evaluate(
                """
                ({ storageKey, notebookId }) => {
                  const state = JSON.parse(window.localStorage.getItem(storageKey) || "{}");
                  return state[notebookId]?.pipelinePaths || [];
                }
                """,
                {
                    "storageKey": NOTEBOOK_METADATA_STORAGE_KEY,
                    "notebookId": notebook_id,
                },
            )
            if (
                len(stored_priority_paths) != 2
                or stored_priority_paths[0].get("terminalStageId") != "stage-audit-backlog"
                or stored_priority_paths[1].get("terminalStageId") != "stage-final"
            ):
                raise RuntimeError(f"Priority path reorder was not persisted: {stored_priority_paths}")
            await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
            await page.locator("[data-notebook-pipeline-graph] .pipeline-node").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                """
                () => {
                  const summary = document.querySelector('[data-pipeline-priority-summary]')?.textContent || '';
                  return summary.includes('Audit Backlog first') &&
                    document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node').length >= 5;
                }
                """,
                timeout=args.timeout_ms,
            )
            priority_badges = await page.evaluate(
                """
                () => ({
                  auditGraph: document.querySelector('[data-pipeline-stage-node="stage-audit-backlog"] .pipeline-priority-rank-badge')?.textContent?.trim() || '',
                  auditTable: document.querySelector('[data-pipeline-stage-row="stage-audit-backlog"] .pipeline-priority-rank-badge')?.textContent?.trim() || '',
                  statusGraph: document.querySelector('[data-pipeline-stage-node="stage-final"] .pipeline-priority-rank-badge')?.textContent?.trim() || '',
                  statusTable: document.querySelector('[data-pipeline-stage-row="stage-final"] .pipeline-priority-rank-badge')?.textContent?.trim() || '',
                })
                """
            )
            if priority_badges != {
                "auditGraph": "P1",
                "auditTable": "P1",
                "statusGraph": "P2",
                "statusTable": "P2",
            }:
                raise RuntimeError(f"Priority rank badges were not shown on terminal stages: {priority_badges}")
            alias_state_before_hover = await page.locator(
                '[data-pipeline-stage-node="stage-raw"] .pipeline-node-alias'
            ).first.evaluate(
                """
                (node) => {
                  const style = getComputedStyle(node);
                  return { opacity: style.opacity, visibility: style.visibility };
                }
                """
            )
            if alias_state_before_hover["visibility"] != "hidden" or float(alias_state_before_hover["opacity"]) > 0.01:
                raise RuntimeError(
                    "Pipeline graph target path should be hidden until hover: "
                    f"{alias_state_before_hover}"
                )
            await page.locator('[data-pipeline-stage-node="stage-raw"]').first.hover()
            await page.wait_for_function(
                """
                () => {
                  const node = document.querySelector('[data-pipeline-stage-node="stage-raw"] .pipeline-node-alias');
                  const style = node ? getComputedStyle(node) : null;
                  return Boolean(style && style.visibility === 'visible' && Number(style.opacity) > 0.98);
                }
                """,
                timeout=args.timeout_ms,
            )
            alias_state_after_hover = await page.locator(
                '[data-pipeline-stage-node="stage-raw"] .pipeline-node-alias'
            ).first.evaluate(
                """
                (node) => {
                  const style = getComputedStyle(node);
                  return { opacity: style.opacity, visibility: style.visibility };
                }
                """
            )
            if alias_state_after_hover["visibility"] != "visible" or float(alias_state_after_hover["opacity"]) < 0.25:
                raise RuntimeError(
                    "Pipeline graph target path should appear on hover: "
                    f"{alias_state_after_hover}"
                )
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
            await page.locator('[data-pipeline-stage-row="stage-scope"] td').first.click()
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

            row_native_title = await page.locator('[data-pipeline-stage-row="stage-scope"]').first.get_attribute("title")
            if row_native_title:
                raise RuntimeError(f"Stage table should use the custom tooltip instead of a native title: {row_native_title}")
            row_stage_cell = page.locator('[data-pipeline-stage-row="stage-scope"] td').first
            row_tooltip = await row_stage_cell.get_attribute("data-pipeline-tooltip")
            if "Depends on raw" not in (row_tooltip or ""):
                raise RuntimeError("Stage table tooltip did not expose the description.")
            row_tooltip_style = await app_tooltip_style_after_hover(page, row_stage_cell, args.timeout_ms)
            assert_modern_app_tooltip(row_tooltip_style, "Depends on raw")

            await page.locator('[data-pipeline-stage-row="stage-scope"] td').first.click()
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
                  .some((row) =>
                    row.dataset.pipelineStageRow === 'stage-scope' &&
                    row.querySelector('.pipeline-status-icon-ok')
                  )
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
            await page.locator("[data-cancel-notebook-pipeline]").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                """
                () =>
                  document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node-running .pipeline-spinner').length === 1 &&
                  document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node-queued .pipeline-node-state-waiting').length >= 1
                """,
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                """
                () =>
                  document.querySelectorAll('[data-pipeline-stage-row] .pipeline-spinner').length === 1 &&
                  document.querySelectorAll('[data-pipeline-stage-row] .pipeline-status-icon-waiting').length >= 1
                """,
                timeout=args.timeout_ms,
            )
            row_glow_measurement_script = """
                (row) => {
                  const firstCell = row.cells[0];
                  const lastCell = row.cells[row.cells.length - 1];
                  const title = row.querySelector('.pipeline-table-stage-title');
                  const cells = Array.from(row.cells);
                  const rowRect = row.getBoundingClientRect();
                  const cellRect = firstCell?.getBoundingClientRect();
                  const lastCellRect = lastCell?.getBoundingClientRect();
                  const titleRect = title?.getBoundingClientRect();
                  const firstStyle = firstCell ? getComputedStyle(firstCell) : null;
                  const firstEdge = firstCell ? getComputedStyle(firstCell, '::after') : null;
                  const lastEdge = lastCell ? getComputedStyle(lastCell, '::after') : null;
                  const cellStyles = cells.map((cell) => {
                    const style = getComputedStyle(cell);
                    return {
                      animationName: style.animationName,
                      animationDuration: style.animationDuration,
                      backgroundColor: style.backgroundColor,
                      boxShadow: style.boxShadow,
                    };
                  });
                  return {
                    row: rowRect ? { left: rowRect.left, top: rowRect.top, width: rowRect.width, height: rowRect.height } : null,
                    cell: cellRect ? { left: cellRect.left, top: cellRect.top, width: cellRect.width, height: cellRect.height } : null,
                    lastCell: lastCellRect ? { left: lastCellRect.left, top: lastCellRect.top, width: lastCellRect.width, height: lastCellRect.height } : null,
                    title: titleRect ? { left: titleRect.left, top: titleRect.top, width: titleRect.width, height: titleRect.height } : null,
                    cellCount: cells.length,
                    glowingCellCount: cellStyles.filter((style) => style.animationName.includes('pipeline-stage-row-computing-glow')).length,
                    glowAnimation: firstStyle ? firstStyle.animationName : '',
                    glowDuration: firstStyle ? firstStyle.animationDuration : '',
                    glowBoxShadow: firstStyle ? firstStyle.boxShadow : '',
                    glowBackgroundColor: firstStyle ? firstStyle.backgroundColor : '',
                    firstEdgeAnimation: firstEdge ? firstEdge.animationName : '',
                    lastEdgeAnimation: lastEdge ? lastEdge.animationName : '',
                  };
                }
            """
            running_row_handle = await page.locator(
                "[data-pipeline-stage-row].pipeline-stage-row-running"
            ).first.element_handle()
            if running_row_handle is None:
                raise RuntimeError("Pipeline did not expose a running stage table row for glow measurement.")
            row_glow_before = await running_row_handle.evaluate(row_glow_measurement_script)
            if "pipeline-stage-row-computing-glow" not in str(row_glow_before.get("glowAnimation") or ""):
                raise RuntimeError(f"Running pipeline stage row was not glowing: {row_glow_before}")
            if "3s" not in str(row_glow_before.get("glowDuration") or ""):
                raise RuntimeError(f"Running pipeline stage row glow had the wrong duration: {row_glow_before}")
            if row_glow_before.get("glowingCellCount") != row_glow_before.get("cellCount"):
                raise RuntimeError(f"Running pipeline stage row glow did not cover the full row: {row_glow_before}")
            if "pipeline-stage-row-edge-glow" not in str(row_glow_before.get("firstEdgeAnimation") or ""):
                raise RuntimeError(f"Running pipeline stage row did not expose a left row edge glow: {row_glow_before}")
            if "pipeline-stage-row-edge-glow" not in str(row_glow_before.get("lastEdgeAnimation") or ""):
                raise RuntimeError(f"Running pipeline stage row did not expose a right row edge glow: {row_glow_before}")
            await page.wait_for_timeout(80)
            row_glow_after = await running_row_handle.evaluate(row_glow_measurement_script)
            row_glow_shift_violations = []
            for key in ["row", "cell", "lastCell", "title"]:
                before_rect = row_glow_before.get(key)
                after_rect = row_glow_after.get(key)
                if not before_rect or not after_rect:
                    row_glow_shift_violations.append({"part": key, "before": before_rect, "after": after_rect})
                    continue
                for field in ["left", "top", "width", "height"]:
                    delta = abs(float(before_rect[field]) - float(after_rect[field]))
                    if delta > 0.5:
                        row_glow_shift_violations.append(
                            {
                                "part": key,
                                "field": field,
                                "delta": delta,
                                "before": before_rect,
                                "after": after_rect,
                            }
                        )
            if row_glow_shift_violations:
                raise RuntimeError(
                    "Pipeline stage table row glow moved the row or its text: "
                    f"{row_glow_shift_violations}"
                )
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-notebook-pipeline-graph] [data-cancel-pipeline-stage]').length === 1",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-pipeline-stage-row] [data-cancel-pipeline-stage]').length === 1",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                """
                () =>
                  document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-stage-action-button.is-waiting').length >= 1 &&
                  document.querySelectorAll('[data-pipeline-stage-row] .pipeline-stage-action-button.is-waiting').length >= 1
                """,
                timeout=args.timeout_ms,
            )
            waiting_tooltip = page.locator("[data-notebook-pipeline-graph] .pipeline-node-state-waiting").first
            tooltip_style = await app_tooltip_style_after_hover(page, waiting_tooltip, args.timeout_ms)
            assert_modern_app_tooltip(tooltip_style, "Waiting for earlier stages")
            running_node_measurement_script = """
                (node) => {
                  const bounds = (element) => {
                    const rect = element?.getBoundingClientRect();
                    return rect
                      ? {
                          left: rect.left,
                          top: rect.top,
                          width: rect.width,
                          height: rect.height,
                        }
                      : null;
                  };
                  const rect = node.querySelector('.pipeline-node-rect');
                  const glow = node.querySelector('.pipeline-node-glow');
                  return {
                    node: bounds(node),
                    body: bounds(node.querySelector('.pipeline-node-body')),
                    border: bounds(rect),
                    glow: bounds(glow),
                    icon: bounds(node.querySelector('.pipeline-node-icon')),
                    title: bounds(node.querySelector('.pipeline-node-title')),
                    action: bounds(node.querySelector('.pipeline-stage-action-group-graph')),
                    state: bounds(node.querySelector('.pipeline-node-state')),
                    borderAnimation: rect ? getComputedStyle(rect).animationName : '',
                    glowAnimation: glow ? getComputedStyle(glow).animationName : '',
                  };
                }
            """
            running_node_handle = await page.locator(
                "[data-notebook-pipeline-graph] .pipeline-node-running"
            ).first.element_handle()
            if running_node_handle is None:
                raise RuntimeError("Pipeline did not expose a running stage node for glow measurement.")
            glow_before = await running_node_handle.evaluate(
                running_node_measurement_script
            )
            if "pipeline-node-computing-glow" not in str(glow_before.get("glowAnimation") or ""):
                raise RuntimeError(f"Running pipeline stage border was not glowing: {glow_before}")
            await page.wait_for_timeout(80)
            glow_after = await running_node_handle.evaluate(
                running_node_measurement_script
            )
            glow_shift_violations = []
            for key in ["node", "body", "border", "icon", "title", "action", "state"]:
                before_rect = glow_before.get(key)
                after_rect = glow_after.get(key)
                if not before_rect or not after_rect:
                    glow_shift_violations.append({"part": key, "before": before_rect, "after": after_rect})
                    continue
                for field in ["left", "top", "width", "height"]:
                    delta = abs(float(before_rect[field]) - float(after_rect[field]))
                    if delta > 0.5:
                        glow_shift_violations.append(
                            {
                                "part": key,
                                "field": field,
                                "delta": delta,
                                "before": before_rect,
                                "after": after_rect,
                            }
                        )
            if glow_shift_violations:
                raise RuntimeError(
                    "Pipeline stage border glow moved the stage box or its contents: "
                    f"{glow_shift_violations}"
                )
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node-state-ok').length >= 5",
                timeout=args.timeout_ms,
            )
            row_text_after_pipeline = await page.locator('[data-pipeline-stage-row="stage-raw"]').first.inner_text()
            if "OK" in row_text_after_pipeline:
                raise RuntimeError("Completed pipeline stage should not show a duplicate OK text tag.")
            if await page.locator('[data-pipeline-stage-row="stage-raw"] .pipeline-status-icon-ok').count() < 1:
                raise RuntimeError("Completed pipeline stage did not show the green status tick.")
            if "1s 200 ms" not in await page.locator('[data-pipeline-stage-row="stage-raw"]').first.inner_text():
                raise RuntimeError("Completed pipeline stage did not show its last run duration.")
            header_total_duration = (
                await page.locator("[data-notebook-pipeline-total-duration]").first.inner_text()
            )
            if "6s" not in header_total_duration:
                raise RuntimeError(
                    f"Pipeline header did not show the summed stage duration: {header_total_duration}"
                )
            footer_total_duration = (
                await page.locator("[data-notebook-pipeline-table-duration-total]").first.inner_text()
            )
            if footer_total_duration.strip() != "6s 0 ms":
                raise RuntimeError(
                    f"Pipeline table footer did not show the summed stage duration: {footer_total_duration}"
                )
            if await page.locator("[data-notebook-pipeline-graph] .pipeline-node-state-ok").count() < 5:
                raise RuntimeError("Completed pipeline stages did not show green OK state circles.")
            await page.locator("[data-run-notebook-pipeline]").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            if await page.locator("[data-cancel-notebook-pipeline]").first.is_visible():
                raise RuntimeError("Pipeline cancel button stayed visible after all stages completed.")
            expected_pipeline_stage_ids = [
                "stage-raw",
                "stage-scope",
                "stage-audit-candidates",
                "stage-audit-backlog",
                "stage-final",
            ]
            if pipeline_run_payload_stage_ids != expected_pipeline_stage_ids:
                raise RuntimeError(
                    "Run pipeline did not submit the full dependency-ordered stage set: "
                    f"{pipeline_run_payload_stage_ids}"
                )
            if pipeline_active_stage_timeline != expected_pipeline_stage_ids:
                raise RuntimeError(
                    "Run pipeline did not remain active through the full mocked stage sequence: "
                    f"{pipeline_active_stage_timeline}"
                )
            if pipeline_state_poll_count < 6:
                raise RuntimeError(
                    "Run pipeline returned to idle before Playwright observed the active run complete: "
                    f"{pipeline_state_poll_count} state polls"
                )

            await page.locator('[data-pipeline-stage-row="stage-scope"] [data-run-pipeline-from-stage]').first.click()
            await page.locator("[data-cancel-notebook-pipeline]").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                """
                () =>
                  document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node-running .pipeline-spinner').length === 1 &&
                  document.querySelectorAll('[data-notebook-pipeline-graph] .pipeline-node-queued .pipeline-node-state-waiting').length >= 1
                """,
                timeout=args.timeout_ms,
            )
            await page.locator("[data-run-notebook-pipeline]").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            expected_from_stage_ids = [
                "stage-scope",
                "stage-audit-candidates",
                "stage-audit-backlog",
                "stage-final",
            ]
            if pipeline_run_start_stage_id != "stage-scope":
                raise RuntimeError(
                    "Run pipeline from a stage did not submit the selected start stage: "
                    f"{pipeline_run_start_stage_id}"
                )
            if pipeline_run_payload_stage_ids != expected_from_stage_ids:
                raise RuntimeError(
                    "Run pipeline from a stage did not limit the active run to the selected stage and downstream stages: "
                    f"{pipeline_run_payload_stage_ids}"
                )
            if pipeline_active_stage_timeline != expected_from_stage_ids:
                raise RuntimeError(
                    "Run pipeline from a stage did not process selected and downstream stages in order: "
                    f"{pipeline_active_stage_timeline}"
                )
            if pipeline_state_poll_count < 5:
                raise RuntimeError(
                    "Run pipeline from a stage returned to idle before all downstream stages completed: "
                    f"{pipeline_state_poll_count} state polls"
                )
            if await page.locator("[data-notebook-pipeline-graph] .pipeline-status-pill").count():
                raise RuntimeError("Pipeline graph should not render duplicate status text pills.")
            if await page.locator("[data-pipeline-stage-row] .pipeline-status-pill").count():
                raise RuntimeError("Pipeline table should not render duplicate status text pills.")
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
                  .some((row) =>
                    row.dataset.pipelineStageRow === 'stage-scope' &&
                    row.querySelector('.pipeline-table-status[data-pipeline-tooltip="Obsolete"]')
                  )
                """,
                timeout=args.timeout_ms,
            )
            downstream_edge_styles = await page.locator("[data-notebook-pipeline-graph] .pipeline-edge").evaluate_all(
                """
                (edges) => edges.map((edge) => {
                  const style = getComputedStyle(edge);
                  return {
                    classes: edge.getAttribute('class') || '',
                    strokeDasharray: style.strokeDasharray,
                  };
                })
                """
            )
            dashed_downstream_edges = [
                item
                for item in downstream_edge_styles
                if "pipeline-edge-obsolete" in str(item.get("classes") or "")
                and str(item.get("strokeDasharray") or "").lower() not in {"none", "0px"}
            ]
            if len(dashed_downstream_edges) < 2:
                raise RuntimeError(
                    "Manual upstream stage run did not mark all downstream graph edges as dashed: "
                    f"{downstream_edge_styles}"
                )

            await page.route("**/sidebar?**", handle_sidebar)
            await page.locator('[data-pipeline-stage-menu="stage-raw"]').first.click()
            await page.locator('[data-pipeline-menu-action="navigate-target"]').first.click()
            await page.locator(".is-pipeline-target-text-flash").first.wait_for(
                state="attached",
                timeout=args.timeout_ms,
            )
            navigation_state = await page.evaluate(
                """
                () => {
                  const notebook = document.querySelector('[data-notebook-section]');
                  const sources = document.querySelector('[data-data-sources-section]');
                  const catalog = document.querySelector('[data-source-catalog-source-id="s3"]');
                  const bucket = document.querySelector('[data-source-schema][data-source-bucket="stage-bucket"]');
                  const object = document.querySelector('[data-source-object][data-s3-bucket="stage-bucket"][data-s3-key="_bdw_stages/notebook/raw/rev-b/data.parquet"]');
                  const label = object?.querySelector('.source-node-label span');
                  return {
                    notebookOpen: Boolean(notebook?.open),
                    sourcesOpen: Boolean(sources?.open),
                    catalogOpen: Boolean(catalog?.open),
                    bucketOpen: Boolean(bucket?.open),
                    objectFlashing: Boolean(object?.classList.contains('is-pipeline-inspect-flash')),
                    labelFlashing: Boolean(label?.classList.contains('is-pipeline-target-text-flash')),
                  };
                }
                """
            )
            if navigation_state != {
                "notebookOpen": False,
                "sourcesOpen": True,
                "catalogOpen": True,
                "bucketOpen": True,
                "objectFlashing": True,
                "labelFlashing": True,
            }:
                raise RuntimeError(
                    "Navigate to target data object did not reveal and flash the stage output correctly: "
                    f"{navigation_state}"
                )

            await page.locator('[data-pipeline-stage-menu="stage-raw"]').first.click()
            await page.locator('[data-pipeline-menu-action="derive"]').first.click()
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-query-cell]').length >= 6",
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
                "() => document.querySelectorAll('[data-query-cell]').length >= 7",
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

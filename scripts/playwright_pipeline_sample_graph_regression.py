from __future__ import annotations

import argparse
import json
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright


SAMPLE_NOTEBOOK_IDS = (
    "mwa-abrechnung-s3-parquet-pipeline",
    "kostenbelege-3-1-s3-parquet-pipeline",
)


def mock_pipeline_graph(notebook_id: str, *, running: bool = False) -> dict[str, object]:
    stages = [
        ("stage-raw", "raw", "Raw source", 0, []),
        ("stage-normalized", "normalized", "Normalized records", 1, ["stage-raw"]),
        ("stage-joined", "joined", "Joined records", 2, ["stage-normalized"]),
        ("stage-audit", "audit", "Audit candidates", 3, ["stage-joined"]),
        ("stage-output", "output", "Canonical output", 4, ["stage-audit"]),
    ]
    nodes = []
    for index, (stage_id, alias, title, layer, predecessors) in enumerate(stages):
        nodes.append(
            {
                "cellId": f"cell-{index + 1}",
                "stageId": stage_id,
                "alias": alias,
                "title": title,
                "description": "",
                "materialize": True,
                "dataSources": [],
                "predecessorStageIds": predecessors,
                "successorStageIds": [
                    next_stage_id
                    for next_stage_id, _next_alias, _next_title, _next_layer, next_predecessors in stages
                    if stage_id in next_predecessors
                ],
                "status": "running" if running and index == 0 else "valid",
                "order": index,
                "layer": layer,
                "latestRevision": None,
                "latestRun": None,
                "outputSource": {},
                "published": False,
                "publishedDataProducts": [],
                "obsoleteReason": "",
                "runWarning": "",
            }
        )
    return {
        "notebookId": notebook_id,
        "notebookTitle": "Pipeline Graph Flicker Regression",
        "version": 2 if running else 1,
        "nodes": nodes,
        "sourceNodes": [],
        "edges": [
            {"fromStageId": predecessor_id, "toStageId": stage_id}
            for stage_id, _alias, _title, _layer, predecessors in stages
            for predecessor_id in predecessors
        ],
        "diagnostics": [],
        "order": [stage_id for stage_id, _alias, _title, _layer, _predecessors in stages],
        "paths": [],
        "defaultSelectedStageId": "stage-raw",
        "activeRuns": (
            [
                {
                    "runId": "run-regression",
                    "notebookId": notebook_id,
                    "stageIds": [stage_id for stage_id, _alias, _title, _layer, _predecessors in stages],
                    "status": "running",
                    "cancelRequested": False,
                }
            ]
            if running
            else []
        ),
    }


def fulfill_json(route, payload: dict[str, object]) -> None:
    route.fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps(payload),
    )


def graph_dom_state(page) -> dict[str, object]:
    return page.evaluate(
        """
        () => {
          const graph = document.querySelector("[data-notebook-pipeline-graph]");
          const table = document.querySelector("[data-notebook-pipeline-table]");
          const status = document.querySelector("[data-notebook-pipeline-status]");
          const runningNodeGlow = graph?.querySelector(".pipeline-node-running .pipeline-node-glow");
          const runningRowCell = table?.querySelector(".pipeline-stage-row-running > td");
          return {
            nodeCount: graph?.querySelectorAll(".pipeline-node").length || 0,
            edgeCount: graph?.querySelectorAll(".pipeline-edge").length || 0,
            rowCount: table?.querySelectorAll("tr").length || 0,
            status: status?.textContent || "",
            placeholderCount: graph?.querySelectorAll("[data-pipeline-graph-placeholder]").length || 0,
            ariaBusy: graph?.getAttribute("aria-busy") || "",
            className: graph?.className || "",
            nodeGlowAnimationDuration: runningNodeGlow ? getComputedStyle(runningNodeGlow).animationDuration : "",
            rowAnimationDuration: runningRowCell ? getComputedStyle(runningRowCell).animationDuration : "",
            rowEdgeAnimationDuration: runningRowCell ? getComputedStyle(runningRowCell, "::after").animationDuration : "",
          };
        }
        """
    )


def assert_sample_graph_renders(
    base_url: str,
    notebook_id: str,
    *,
    headed: bool,
    timeout_ms: int,
) -> None:
    target_url = urljoin(base_url.rstrip("/") + "/", f"notebooks/{notebook_id}")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not headed)
        page = browser.new_page(viewport={"width": 1440, "height": 1200})
        console_issues: list[str] = []
        page.on(
            "console",
            lambda msg: console_issues.append(f"{msg.type}: {msg.text}")
            if msg.type in {"error", "warning"}
            else None,
        )
        page.on("pageerror", lambda exc: console_issues.append(f"pageerror: {exc}"))
        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.locator("[data-notebook-pipeline-panel]:visible").wait_for(timeout=timeout_ms)
            page.wait_for_function(
                """
                () => {
                  const graph = document.querySelector("[data-notebook-pipeline-graph]");
                  const table = document.querySelector("[data-notebook-pipeline-table]");
                  const status = document.querySelector("[data-notebook-pipeline-status]");
                  return graph
                    && graph.querySelectorAll(".pipeline-node").length >= 5
                    && graph.querySelectorAll(".pipeline-edge").length >= 4
                    && table
                    && table.querySelectorAll("tr").length >= 5
                    && status
                    && /stages in dependency order/i.test(status.textContent || "");
                }
                """,
                timeout=timeout_ms,
            )
            state = page.evaluate(
                """
                () => {
                  const graph = document.querySelector("[data-notebook-pipeline-graph]");
                  const table = document.querySelector("[data-notebook-pipeline-table]");
                  const status = document.querySelector("[data-notebook-pipeline-status]");
                  return {
                    nodeCount: graph?.querySelectorAll(".pipeline-node").length || 0,
                    edgeCount: graph?.querySelectorAll(".pipeline-edge").length || 0,
                    rowCount: table?.querySelectorAll("tr").length || 0,
                    status: status?.textContent || "",
                    placeholderCount: graph?.querySelectorAll("[data-pipeline-graph-placeholder]").length || 0,
                  };
                }
                """
            )
            if state["placeholderCount"]:
                raise AssertionError(f"Pipeline graph placeholder still visible for {notebook_id}: {state}")
            if console_issues:
                raise AssertionError(f"Console issues while rendering {notebook_id}: {console_issues}")
            print(f"{notebook_id}: {state}")
        finally:
            browser.close()


def assert_run_refresh_keeps_existing_graph(
    base_url: str,
    *,
    headed: bool,
    timeout_ms: int,
) -> None:
    notebook_id = "mwa-abrechnung-s3-parquet-pipeline"
    target_url = urljoin(base_url.rstrip("/") + "/", f"notebooks/{notebook_id}")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not headed)
        page = browser.new_page(viewport={"width": 1440, "height": 1200})
        console_issues: list[str] = []
        graph_requests = {"count": 0}
        empty_state = {"version": 1, "records": [], "stageStates": {}, "activeRuns": []}

        def handle_graph(route) -> None:
            graph_requests["count"] += 1
            fulfill_json(
                route,
                mock_pipeline_graph(notebook_id, running=graph_requests["count"] > 1),
            )

        page.add_init_script(
            """
            (() => {
              const originalFetch = window.fetch.bind(window);
              let delayedGraphCalls = 0;
              let delayMs = 0;
              window.__pipelineGraphRefreshRegression = {
                delayNextGraph(ms) {
                  delayMs = Number(ms) || 1000;
                  delayedGraphCalls = 1;
                },
                pending() {
                  return delayedGraphCalls;
                },
              };
              window.fetch = (input, init) => {
                const url = typeof input === "string" ? input : String(input?.url || "");
                if (url.includes("/api/materialized-stages/graph") && delayedGraphCalls > 0) {
                  delayedGraphCalls -= 1;
                  const ms = delayMs;
                  if (delayedGraphCalls <= 0) {
                    delayMs = 0;
                  }
                  return new Promise((resolve) => window.setTimeout(resolve, ms))
                    .then(() => originalFetch(input, init));
                }
                return originalFetch(input, init);
              };
            })();
            """
        )
        page.route("**/api/materialized-stages/graph", handle_graph)
        page.route("**/api/materialized-stages/pipeline/run", lambda route: fulfill_json(route, empty_state))
        page.route("**/api/materialized-stages/state", lambda route: fulfill_json(route, empty_state))
        page.on(
            "console",
            lambda msg: console_issues.append(f"{msg.type}: {msg.text}")
            if msg.type in {"error", "warning"}
            else None,
        )
        page.on("pageerror", lambda exc: console_issues.append(f"pageerror: {exc}"))
        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.locator("[data-notebook-pipeline-panel]:visible").wait_for(timeout=timeout_ms)
            page.wait_for_function(
                """
                () => {
                  const graph = document.querySelector("[data-notebook-pipeline-graph]");
                  const table = document.querySelector("[data-notebook-pipeline-table]");
                  return graph
                    && graph.querySelectorAll(".pipeline-node").length >= 5
                    && table
                    && table.querySelectorAll("tr").length >= 5
                    && !graph.querySelector("[data-pipeline-graph-placeholder]");
                }
                """,
                timeout=timeout_ms,
            )
            page.evaluate("window.__pipelineGraphRefreshRegression.delayNextGraph(1200)")
            page.locator("[data-run-notebook-pipeline]").click(timeout=timeout_ms)
            page.wait_for_timeout(250)
            during_refresh = graph_dom_state(page)
            if during_refresh["placeholderCount"]:
                raise AssertionError(
                    "Pipeline graph refresh replaced the rendered graph with a placeholder: "
                    f"{during_refresh}"
                )
            if during_refresh["nodeCount"] < 5 or during_refresh["rowCount"] < 5:
                raise AssertionError(
                    "Pipeline graph refresh dropped rendered graph/table content: "
                    f"{during_refresh}"
                )
            page.wait_for_function(
                """
                () => {
                  const graph = document.querySelector("[data-notebook-pipeline-graph]");
                  return graph
                    && graph.getAttribute("aria-busy") !== "true"
                    && graph.querySelectorAll(".pipeline-node").length >= 5
                    && !graph.querySelector("[data-pipeline-graph-placeholder]");
                }
                """,
                timeout=timeout_ms,
            )
            final_state = graph_dom_state(page)
            for key in (
                "nodeGlowAnimationDuration",
                "rowAnimationDuration",
                "rowEdgeAnimationDuration",
            ):
                if final_state[key] != "6s":
                    raise AssertionError(
                        f"Expected running pipeline glow duration to be 6s for {key}: "
                        f"{final_state}"
                    )
            if console_issues:
                raise AssertionError(
                    f"Console issues while refreshing the pipeline graph: {console_issues}"
                )
            print(f"{notebook_id} run refresh preserved graph: {during_refresh} -> {final_state}")
        finally:
            browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify seeded notebook pipeline sample graphs render in the browser."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--timeout-ms", type=int, default=60000)
    args = parser.parse_args()

    for notebook_id in SAMPLE_NOTEBOOK_IDS:
        assert_sample_graph_renders(
            args.base_url,
            notebook_id,
            headed=args.headed,
            timeout_ms=args.timeout_ms,
        )
    assert_run_refresh_keeps_existing_graph(
        args.base_url,
        headed=args.headed,
        timeout_ms=args.timeout_ms,
    )


if __name__ == "__main__":
    main()

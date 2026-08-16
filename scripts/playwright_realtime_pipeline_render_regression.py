from __future__ import annotations

import argparse
import asyncio
import json

from playwright.async_api import async_playwright


NOTEBOOK_ID = "kostenbelege-fact-builder-s3-pipeline-demo"
FIRST_CELL_ID = "kostenbelege-fact-builder-pipeline-cell-1"
SECOND_CELL_ID = "kostenbelege-fact-builder-pipeline-cell-2"


FAKE_EVENT_SOURCE = """
(() => {
  window.__fakeEventSources = [];
  class FakeEventSource {
    constructor(url) {
      this.url = url;
      this.readyState = FakeEventSource.CONNECTING;
      this.listeners = new Map();
      window.__fakeEventSources.push(this);
      setTimeout(() => {
        this.readyState = FakeEventSource.OPEN;
        this.onopen?.(new Event("open"));
      }, 0);
    }
    addEventListener(type, listener) {
      const listeners = this.listeners.get(type) || [];
      listeners.push(listener);
      this.listeners.set(type, listeners);
    }
    emit(type, payload) {
      const event = { data: JSON.stringify(payload) };
      (this.listeners.get(type) || []).forEach((listener) => listener(event));
    }
    close() {
      this.readyState = FakeEventSource.CLOSED;
    }
  }
  FakeEventSource.CONNECTING = 0;
  FakeEventSource.OPEN = 1;
  FakeEventSource.CLOSED = 2;
  window.EventSource = FakeEventSource;
  window.__emitRealtimeSnapshot = (type, payload) => {
    window.__fakeEventSources.forEach((source) => source.emit(type, payload));
  };
})();
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Guard pipeline realtime rendering against duplicate DOM and graph request storms."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def wait_for_graph_requests_to_finish(
    graph_metrics: dict[str, int | bool], timeout_ms: int
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
    while asyncio.get_running_loop().time() < deadline:
        if graph_metrics["requests"] > 0 and graph_metrics["in_flight"] == 0:
            await asyncio.sleep(0.2)
            if graph_metrics["in_flight"] == 0:
                return
        await asyncio.sleep(0.02)
    raise RuntimeError(f"Timed out waiting for graph refreshes: {graph_metrics}")


def assert_close(label: str, before: float, during: float, tolerance: float = 0.75) -> None:
    if abs(before - during) > tolerance:
        raise RuntimeError(
            f"Pipeline controls shifted {label}: before={before}, during={during}"
        )


def assert_stable_rect(
    label: str, before: dict[str, float], during: dict[str, float]
) -> None:
    for dimension in ("x", "y", "width", "height"):
        assert_close(
            f"{label}.{dimension}",
            float(before[dimension]),
            float(during[dimension]),
        )


async def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    graph_metrics: dict[str, int | bool] = {
        "measure": False,
        "requests": 0,
        "in_flight": 0,
        "max_in_flight": 0,
    }

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context()
        await context.add_init_script(FAKE_EVENT_SOURCE)
        page = await context.new_page()

        async def handle_query_jobs(route) -> None:
            if route.request.method == "GET":
                await route.fulfill(
                    status=200,
                    content_type="application/json",
                    body=json.dumps(
                        {
                            "version": 0,
                            "summary": {"runningCount": 0, "totalCount": 0},
                            "performance": {"recent": [], "stats": {}},
                            "jobs": [],
                        }
                    ),
                )
                return
            await route.continue_()

        async def handle_graph(route) -> None:
            measured = bool(graph_metrics["measure"])
            if measured:
                graph_metrics["requests"] = int(graph_metrics["requests"]) + 1
                graph_metrics["in_flight"] = int(graph_metrics["in_flight"]) + 1
                graph_metrics["max_in_flight"] = max(
                    int(graph_metrics["max_in_flight"]),
                    int(graph_metrics["in_flight"]),
                )
            try:
                response = await route.fetch()
                if measured:
                    await asyncio.sleep(0.15)
                await route.fulfill(response=response)
            finally:
                if measured:
                    graph_metrics["in_flight"] = int(graph_metrics["in_flight"]) - 1

        await page.route("**/api/query-jobs", handle_query_jobs)
        await page.route("**/api/materialized-stages/graph", handle_graph)

        try:
            await page.goto(
                f"{base_url}/notebooks/{NOTEBOOK_ID}",
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                "() => document.querySelectorAll('[data-query-cell]').length === 6",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                "() => window.__fakeEventSources?.length > 0",
                timeout=args.timeout_ms,
            )
            await page.locator("[data-notebook-pipeline-graph] .pipeline-node").first.wait_for(
                state="attached", timeout=args.timeout_ms
            )

            spinner_layout = await page.evaluate(
                """
                async () => {
                  await document.fonts?.ready;
                  const header = document.querySelector(".notebook-pipeline-header");
                  const titleRow = document.querySelector(".notebook-pipeline-title-row");
                  const titleLabel = titleRow?.querySelector(".workspace-tags-label");
                  const spinnerSlot = document.querySelector("[data-notebook-pipeline-running-slot]");
                  const headerSpinner = document.querySelector("[data-notebook-pipeline-running-indicator]");
                  const actions = document.querySelector("[data-notebook-pipeline-actions]");
                  const runButton = document.querySelector("[data-run-notebook-pipeline]");
                  const cancelButton = document.querySelector("[data-cancel-notebook-pipeline]");
                  const buttonSpinner = cancelButton?.querySelector(".notebook-pipeline-button-spinner");
                  const graph = document.querySelector("[data-notebook-pipeline-graph]");
                  const table = document.querySelector("[data-notebook-pipeline-table]");
                  const elements = {
                    header,
                    titleRow,
                    titleLabel,
                    spinnerSlot,
                    headerSpinner,
                    actions,
                    runButton,
                    cancelButton,
                    buttonSpinner,
                    graph,
                    table,
                  };
                  const missing = Object.entries(elements)
                    .filter(([, element]) => !element)
                    .map(([name]) => name);
                  if (missing.length) {
                    throw new Error(`Missing pipeline spinner elements: ${missing.join(", ")}`);
                  }

                  const nextPaint = () => new Promise((resolve) => {
                    requestAnimationFrame(() => requestAnimationFrame(resolve));
                  });
                  const rect = (element) => {
                    const bounds = element.getBoundingClientRect();
                    return {
                      x: bounds.x + window.scrollX,
                      y: bounds.y + window.scrollY,
                      width: bounds.width,
                      height: bounds.height,
                    };
                  };
                  const capture = (button) => ({
                    header: rect(header),
                    titleRow: rect(titleRow),
                    titleLabel: rect(titleLabel),
                    actions: rect(actions),
                    button: rect(button),
                    graph: rect(graph),
                    table: rect(table),
                  });
                  const original = {
                    runHidden: runButton.hidden,
                    cancelHidden: cancelButton.hidden,
                    cancelDisabled: cancelButton.disabled,
                    spinnerHidden: headerSpinner.hidden,
                  };

                  runButton.hidden = false;
                  cancelButton.hidden = true;
                  cancelButton.disabled = false;
                  headerSpinner.hidden = true;
                  await nextPaint();
                  const idle = capture(runButton);

                  runButton.hidden = true;
                  cancelButton.hidden = false;
                  headerSpinner.hidden = false;
                  await nextPaint();
                  const active = capture(cancelButton);
                  const headerSpinnerStyle = getComputedStyle(headerSpinner);
                  const buttonSpinnerStyle = getComputedStyle(buttonSpinner);
                  const details = {
                    idle,
                    active,
                    spinnerBeforeTitle: Boolean(
                      spinnerSlot.compareDocumentPosition(titleLabel) & Node.DOCUMENT_POSITION_FOLLOWING
                    ),
                    spinnerSlot: {
                      width: spinnerSlot.offsetWidth,
                      height: spinnerSlot.offsetHeight,
                    },
                    headerSpinner: {
                      width: headerSpinner.offsetWidth,
                      height: headerSpinner.offsetHeight,
                      borderWidth: headerSpinnerStyle.borderTopWidth,
                      animationName: headerSpinnerStyle.animationName,
                    },
                    buttonSpinner: {
                      tagName: buttonSpinner.tagName,
                      width: buttonSpinner.offsetWidth,
                      height: buttonSpinner.offsetHeight,
                      borderWidth: buttonSpinnerStyle.borderTopWidth,
                      animationName: buttonSpinnerStyle.animationName,
                    },
                    buttonSpinnerCount: cancelButton.querySelectorAll(
                      ".notebook-pipeline-button-spinner"
                    ).length,
                    buttonText: cancelButton.textContent.trim().replace(/\\s+/g, " "),
                    buttonContentFits: cancelButton.scrollWidth <= cancelButton.clientWidth,
                  };

                  runButton.hidden = original.runHidden;
                  cancelButton.hidden = original.cancelHidden;
                  cancelButton.disabled = original.cancelDisabled;
                  headerSpinner.hidden = original.spinnerHidden;
                  return details;
                }
                """
            )
            for element_name in ("header", "titleRow", "titleLabel", "actions", "graph", "table"):
                assert_stable_rect(
                    element_name,
                    spinner_layout["idle"][element_name],
                    spinner_layout["active"][element_name],
                )
            assert_stable_rect(
                "Run/Abort button",
                spinner_layout["idle"]["button"],
                spinner_layout["active"]["button"],
            )
            if not spinner_layout["spinnerBeforeTitle"]:
                raise RuntimeError("The large pipeline spinner is not before the title")
            if spinner_layout["spinnerSlot"] != {"width": 36, "height": 36}:
                raise RuntimeError(f"Pipeline spinner slot is not fixed at 36px: {spinner_layout}")
            if spinner_layout["headerSpinner"]["width"] != 36 or spinner_layout["headerSpinner"]["height"] != 36:
                raise RuntimeError(f"The large pipeline spinner is not 36px: {spinner_layout}")
            if spinner_layout["headerSpinner"]["borderWidth"] != "4px":
                raise RuntimeError(f"The large pipeline spinner border is not 4px: {spinner_layout}")
            if spinner_layout["buttonSpinnerCount"] != 1:
                raise RuntimeError(f"The Abort button spinner is missing or duplicated: {spinner_layout}")
            if spinner_layout["buttonText"] != "Abort pipeline":
                raise RuntimeError(f"The Abort button label is duplicated: {spinner_layout}")
            if not spinner_layout["buttonContentFits"]:
                raise RuntimeError(f"The Abort button content is clipped: {spinner_layout}")
            if spinner_layout["buttonSpinner"]["tagName"] != "I":
                raise RuntimeError(f"The Abort spinner is not cache-compatible markup: {spinner_layout}")
            if spinner_layout["buttonSpinner"]["width"] != 18 or spinner_layout["buttonSpinner"]["height"] != 18:
                raise RuntimeError(f"The Abort button spinner is not 18px: {spinner_layout}")
            if spinner_layout["buttonSpinner"]["borderWidth"] != "2px":
                raise RuntimeError(f"The Abort button spinner border is not 2px: {spinner_layout}")
            if spinner_layout["headerSpinner"]["animationName"] != "pipeline-spinner-rotate":
                raise RuntimeError(f"The large pipeline spinner does not rotate: {spinner_layout}")
            if spinner_layout["buttonSpinner"]["animationName"] != "pipeline-spinner-rotate":
                raise RuntimeError(f"The Abort button spinner does not rotate: {spinner_layout}")

            await page.emulate_media(reduced_motion="reduce")
            reduced_motion_animations = await page.evaluate(
                """
                () => ({
                  header: getComputedStyle(
                    document.querySelector("[data-notebook-pipeline-running-indicator]")
                  ).animationName,
                  button: getComputedStyle(
                    document.querySelector(".notebook-pipeline-button-spinner")
                  ).animationName,
                })
                """
            )
            if reduced_motion_animations != {"header": "none", "button": "none"}:
                raise RuntimeError(
                    "Pipeline spinners ignore reduced-motion preferences: "
                    f"{reduced_motion_animations}"
                )
            await page.emulate_media(reduced_motion="no-preference")

            await page.evaluate(
                """
                ({ notebookId, firstCellId, secondCellId }) => {
                  const rows = Array.from({ length: 80 }, (_, index) => [index + 1, `row-${index + 1}`]);
                  const completed = {
                    jobId: "query-pipeline-regression-completed",
                    notebookId,
                    notebookTitle: "Realtime pipeline regression",
                    cellId: firstCellId,
                    status: "completed",
                    sql: "SELECT id, label FROM completed_stage",
                    columns: ["id", "label"],
                    rows,
                    rowCount: rows.length,
                    rowsShown: rows.length,
                    truncated: false,
                    startedAt: "2026-08-16T10:00:00Z",
                    completedAt: "2026-08-16T10:00:01Z",
                    updatedAt: "2026-08-16T10:00:01Z",
                    durationMs: 1000,
                    progressEvents: [],
                    resourceSamples: [],
                    warnings: [],
                  };
                  for (let index = 1; index <= 40; index += 1) {
                    const running = {
                      jobId: "query-pipeline-regression-running",
                      notebookId,
                      notebookTitle: "Realtime pipeline regression",
                      cellId: secondCellId,
                      status: "running",
                      sql: "SELECT * FROM running_stage",
                      columns: [],
                      rows: [],
                      rowCount: 0,
                      rowsShown: 0,
                      progress: index / 40,
                      message: `Running update ${index}`,
                      startedAt: "2026-08-16T10:00:02Z",
                      updatedAt: `2026-08-16T10:00:${String(index + 2).padStart(2, "0")}Z`,
                      progressEvents: [],
                      resourceSamples: [],
                      warnings: [],
                    };
                    window.__emitRealtimeSnapshot("query-jobs", {
                      version: index,
                      summary: { runningCount: 1, totalCount: 2 },
                      performance: { recent: [], stats: {} },
                      jobs: [running, completed],
                    });
                  }
                }
                """,
                {
                    "notebookId": NOTEBOOK_ID,
                    "firstCellId": FIRST_CELL_ID,
                    "secondCellId": SECOND_CELL_ID,
                },
            )

            result_panel_counts = await page.locator("[data-query-cell]").evaluate_all(
                "(cells) => cells.map((cell) => cell.querySelectorAll('[data-cell-result]').length)"
            )
            if result_panel_counts != [1, 1, 1, 1, 1, 1]:
                raise RuntimeError(
                    "Realtime query snapshots duplicated cell result panels: "
                    f"{result_panel_counts}"
                )

            graph_metrics.update(
                {"measure": True, "requests": 0, "in_flight": 0, "max_in_flight": 0}
            )
            await page.evaluate(
                """
                ({ notebookId }) => {
                  for (let index = 1; index <= 40; index += 1) {
                    window.__emitRealtimeSnapshot("materialized-stages", {
                      version: 1000 + index,
                      records: [],
                      activeRuns: [{
                        runId: "pipeline-render-regression",
                        notebookId,
                        stageIds: ["stage-kfb-kbkp-today"],
                        currentStageId: "stage-kfb-kbkp-today",
                        status: "running",
                        cancelRequested: false,
                      }],
                    });
                  }
                }
                """,
                {"notebookId": NOTEBOOK_ID},
            )
            await wait_for_graph_requests_to_finish(graph_metrics, args.timeout_ms)
            if int(graph_metrics["max_in_flight"]) > 1:
                raise RuntimeError(f"Graph refreshes overlapped: {graph_metrics}")
            if int(graph_metrics["requests"]) > 2:
                raise RuntimeError(f"Graph refresh burst was not coalesced: {graph_metrics}")
        finally:
            await context.close()
            await browser.close()

    print(
        "Playwright realtime pipeline render regression passed: "
        f"stable pipeline spinner layout; one result panel per cell; graph metrics={graph_metrics}."
    )


if __name__ == "__main__":
    asyncio.run(main())

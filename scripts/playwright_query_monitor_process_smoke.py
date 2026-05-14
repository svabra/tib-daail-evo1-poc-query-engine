from __future__ import annotations

import argparse
import asyncio
from uuid import uuid4

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright


LONG_SQL = """
select sum(sin(i) + random()) as total_value
from range(200000000) as source_rows(i)
"""

SMOKE_NOTEBOOK_ID = "s3-smoke-test"
SMOKE_CELL_PREFIX = "query-monitor-process-smoke"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise process-backed query monitoring, simultaneous query execution, "
            "resource metrics, and cancellation visibility."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def ensure_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/notebooks/{SMOKE_NOTEBOOK_ID}",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-query-cell]").first.wait_for(state="visible", timeout=timeout_ms)


async def ensure_monitor_query_cells(page, cell_ids: list[str], timeout_ms: int) -> None:
    await page.locator("[data-query-cell]").first.wait_for(state="attached", timeout=timeout_ms)
    await page.evaluate(
        """
        (cellIds) => {
          const first = document.querySelector("[data-query-cell]");
          if (!(first instanceof HTMLElement)) {
            throw new Error("A query cell is required for the monitor smoke.");
          }
          document.querySelectorAll("[data-monitor-smoke-cell]").forEach((node) => node.remove());
          let insertAfter = first;
          cellIds.forEach((cellId, index) => {
            const clone = first.cloneNode(true);
            clone.dataset.cellId = cellId;
            clone.dataset.monitorSmokeCell = "true";
            clone.dataset.defaultCellSources = "";
            clone.dataset.defaultCellLanguage = "sql";
            clone.classList.remove("is-active", "is-query-running");
            const label = clone.querySelector(".cell-label");
            if (label) {
              label.textContent = `Cell ${index + 1}`;
            }
            const cellInput = clone.querySelector('input[name="cell_id"]');
            if (cellInput instanceof HTMLInputElement) {
              cellInput.value = cellId;
            }
            const result = clone.querySelector("[data-cell-result]");
            if (result instanceof HTMLElement) {
              result.id = `query-results-${cellId}`;
              result.dataset.queryJobId = "";
              result.hidden = true;
              result.innerHTML = "";
            }
            const editorRoot = clone.querySelector("[data-editor-root]");
            if (editorRoot instanceof HTMLElement) {
              editorRoot.dataset.editorName = `sql-${cellId}`;
              editorRoot.dataset.editorLanguage = "sql";
            }
            const textarea = clone.querySelector("[data-editor-source]");
            if (textarea instanceof HTMLTextAreaElement) {
              textarea.dataset.editorLanguage = "sql";
              textarea.dataset.defaultSql = "";
              textarea.value = "";
            }
            const queryRuns = clone.querySelector("[data-notebook-query-runs]");
            if (queryRuns instanceof HTMLElement) {
              queryRuns.dataset.queryRunsCellId = cellId;
              queryRuns.removeAttribute("open");
              queryRuns.open = false;
              const status = queryRuns.querySelector("[data-query-runs-status]");
              if (status instanceof HTMLElement) {
                status.textContent = "No recorded query runs yet.";
              }
              const list = queryRuns.querySelector("[data-query-runs-list]");
              if (list instanceof HTMLElement) {
                list.innerHTML = '<p class="home-empty">No recorded query runs yet.</p>';
              }
            }
            clone.querySelectorAll("[data-cell-source-option]").forEach((option) => {
              if (option instanceof HTMLInputElement) {
                option.checked = false;
              }
            });
            insertAfter.after(clone);
            insertAfter = clone;
          });
        }
        """,
        cell_ids,
    )
    await page.wait_for_function(
        """
        (expectedCount) => document.querySelectorAll("[data-monitor-smoke-cell]").length === expectedCount
        """,
        arg=len(cell_ids),
        timeout=timeout_ms,
    )


async def start_two_queries(page, cell_ids: list[str], timeout_ms: int) -> None:
    await ensure_monitor_query_cells(page, cell_ids, timeout_ms)
    snapshots = await page.evaluate(
        """
        async ({ sql, cellIds }) => {
          const cells = cellIds.map((cellId) =>
            Array.from(document.querySelectorAll("[data-monitor-smoke-cell]"))
              .find((cell) => cell.dataset.cellId === cellId)
          );
          if (cells.length < 2) {
            throw new Error("Expected at least two query cells.");
          }
          for (const cell of cells) {
            cell.dataset.defaultCellSources = "";
            cell.querySelectorAll("[data-cell-source-option]").forEach((option) => {
              if (option instanceof HTMLInputElement) {
                option.checked = false;
              }
            });
            const textarea = cell.querySelector("[data-editor-source]");
            if (!(textarea instanceof HTMLTextAreaElement)) {
              throw new Error("A query editor source could not be located.");
            }
            textarea.value = sql;
            textarea.dispatchEvent(new Event("input", { bubbles: true }));
            textarea.dispatchEvent(new Event("change", { bubbles: true }));
          }
          for (const cell of cells) {
            const form = cell.querySelector("[data-query-form]");
            if (!(form instanceof HTMLFormElement)) {
              throw new Error("A query form could not be located.");
            }
          }
          const notebookTitle =
            document.querySelector("[data-notebook-title-display]")?.textContent?.trim() ||
            "Query Monitor Smoke";
          return Promise.all(cells.map(async (cell) => {
            const form = cell.querySelector("[data-query-form]");
            const formData = new FormData(form);
            formData.set("sql", sql);
            formData.set("notebook_id", formData.get("notebook_id") || "");
            formData.set("cell_id", cell.dataset.cellId || "");
            formData.set("notebook_title", notebookTitle);
            formData.set("data_sources", "");
            const response = await fetch("/api/query-jobs", {
              method: "POST",
              body: formData,
              headers: { Accept: "application/json" },
            });
            if (!response.ok) {
              throw new Error(`Query job creation failed with status ${response.status}.`);
            }
            return response.json();
          }));
        }
        """,
        {"sql": LONG_SQL, "cellIds": cell_ids},
    )
    if len(snapshots or []) < 2:
        raise RuntimeError(f"Expected two query job snapshots, received {snapshots!r}.")


async def wait_for_two_live_monitor_items(page, timeout_ms: int) -> list[int]:
    for _attempt in range(5):
        try:
            await page.evaluate(
                """
                () => {
                  const monitor = document.querySelector("[data-query-monitor-section]");
                  if (monitor instanceof HTMLDetailsElement) {
                    monitor.open = true;
                    monitor.setAttribute("open", "");
                  }
                }
                """
            )
            break
        except PlaywrightError:
            await page.wait_for_timeout(500)
    await page.wait_for_function(
        """
        () => {
          const items = Array.from(document.querySelectorAll(".query-monitor-item-running"));
          const liveItems = items.filter((item) => {
            const text = item.textContent || "";
            return /PID\\s+\\d+/.test(text) && /CPU/.test(text) && /RAM/.test(text);
          });
          const pids = liveItems.map((item) => {
            const match = (item.textContent || "").match(/PID\\s+(\\d+)/);
            return match ? match[1] : "";
          }).filter(Boolean);
          return liveItems.length >= 2 && new Set(pids).size >= 2;
        }
        """,
        timeout=timeout_ms,
    )
    return await page.evaluate(
        """
        () => Array.from(document.querySelectorAll(".query-monitor-item-running"))
          .map((item) => {
            const match = (item.textContent || "").match(/PID\\s+(\\d+)/);
            return match ? Number(match[1]) : 0;
          })
          .filter(Boolean)
          .slice(0, 2)
        """
    )


async def assert_query_resource_chart_layout(
    page,
    timeout_ms: int,
    selector: str,
    min_count: int = 1,
) -> None:
    await page.wait_for_function(
        """
        ({ selector, minCount }) => {
          const visible = (node) => {
            if (!(node instanceof HTMLElement)) {
              return false;
            }
            const rect = node.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
          };
          const inside = (child, parent) => {
            if (!(child instanceof HTMLElement) || !(parent instanceof HTMLElement)) {
              return false;
            }
            const childRect = child.getBoundingClientRect();
            const parentRect = parent.getBoundingClientRect();
            return childRect.left >= parentRect.left - 1
              && childRect.right <= parentRect.right + 1
              && childRect.top >= parentRect.top - 1
              && childRect.bottom <= parentRect.bottom + 1;
          };
          const coloredPixels = (canvas) => {
            try {
              const context = canvas.getContext("2d");
              if (!context) {
                return 0;
              }
              const pixels = context.getImageData(0, 0, canvas.width, canvas.height).data;
              let count = 0;
              for (let index = 0; index < pixels.length; index += 4) {
                const red = pixels[index];
                const green = pixels[index + 1];
                const blue = pixels[index + 2];
                const alpha = pixels[index + 3];
                if (alpha > 0 && (red < 245 || green < 245 || blue < 245)) {
                  count += 1;
                }
              }
              return count;
            } catch (_error) {
              return 0;
            }
          };
          const cards = Array.from(document.querySelectorAll(selector)).filter(visible);
          const validCards = cards.filter((card) => {
            const plot = card.querySelector(".query-resource-sparkline-plot");
            const axis = card.querySelector(".query-resource-sparkline-axis");
            const canvasWrap = card.querySelector(".query-resource-sparkline-canvas");
            const canvas = card.querySelector("[data-query-resource-chart]");
            const legend = card.querySelector(".query-resource-sparkline-legend");
            if (!(plot instanceof HTMLElement)
              || !(axis instanceof HTMLElement)
              || !(canvasWrap instanceof HTMLElement)
              || !(canvas instanceof HTMLCanvasElement)
              || !(legend instanceof HTMLElement)) {
              return false;
            }
            const chart = canvas._bdwQueryResourceChart;
            if (!chart || chart.config?.type !== "line") {
              return false;
            }
            const xScale = chart.options?.scales?.x || {};
            const yScale = chart.options?.scales?.y || {};
            const renderedXScale = chart.scales?.x;
            const renderedYScale = chart.scales?.y;
            const datasets = chart.data?.datasets || [];
            const points = datasets.flatMap((_dataset, datasetIndex) =>
              (chart.getDatasetMeta(datasetIndex)?.data || []).map((point) => ({
                x: Number(point.x),
                y: Number(point.y),
              }))
            );
            const hasRenderedLine = points.some((point, index) => {
              if (!Number.isFinite(point.x) || !Number.isFinite(point.y)) {
                return false;
              }
              return points.slice(index + 1).some((nextPoint) =>
                Number.isFinite(nextPoint.x)
                  && Number.isFinite(nextPoint.y)
                  && Math.abs(nextPoint.x - point.x) >= 1
                  && Math.abs(nextPoint.y - point.y) >= 1
              );
            });
            const axisText = (axis.textContent || "").trim();
            const canvasRect = canvasWrap.getBoundingClientRect();
            const legendRect = legend.getBoundingClientRect();
            const xTickLabels = (renderedXScale?.ticks || []).map((tick) => String(tick.label || tick.value || "").trim()).filter(Boolean);
            const yTickLabels = (renderedYScale?.ticks || []).map((tick) => String(tick.label || tick.value || "").trim()).filter(Boolean);
            const chartArea = chart.chartArea || {};
            return chart.options?.plugins?.legend?.display === false
              && xScale.display === true
              && yScale.display === true
              && xScale.offset === true
              && xScale.grid?.display === false
              && yScale.grid?.display === true
              && String(yScale.grid?.color || "").includes("124, 140, 158")
              && Number(yScale.grid?.lineWidth || 0) <= 1
              && xScale.border?.display === false
              && yScale.border?.display === false
              && yScale.title?.display === false
              && xScale.ticks?.display === true
              && yScale.ticks?.display === true
              && Number(xScale.ticks?.maxTicksLimit || 0) <= 4
              && Number(yScale.ticks?.maxTicksLimit || 0) <= 4
              && xTickLabels.length >= 2
              && xTickLabels.length <= 4
              && yTickLabels.length >= 2
              && yTickLabels.length <= 4
              && yTickLabels.every((label) => /%|MB|GB/.test(label))
              && Number(chartArea.left || 0) >= 32
              && Number(canvas.width || 0) - Number(chartArea.right || 0) >= 8
              && Number(canvas.height || 0) - Number(chartArea.bottom || 0) >= 24
              && datasets.every((dataset) => Number(dataset.borderWidth || 0) >= 2 && dataset.fill === false && !dataset.type)
              && hasRenderedLine
              && coloredPixels(canvas) > 250
              && ["CPU %", "RAM (MB)"].includes(axisText)
              && inside(plot, card)
              && inside(axis, plot)
              && inside(canvasWrap, plot)
              && inside(legend, card)
              && legendRect.top >= canvasRect.bottom - 1
              && card.getBoundingClientRect().width >= 360
              && canvasWrap.getBoundingClientRect().height >= 158;
          });
          return validCards.length >= minCount;
        }
        """,
        arg={"selector": selector, "minCount": min_count},
        timeout=timeout_ms,
    )


async def assert_resource_monitoring_ui(page, timeout_ms: int) -> None:
    await page.wait_for_function(
        """
        () => {
          const monitorSparklines = document.querySelectorAll(
            ".query-monitor-item-running [data-query-resource-sparklines]"
          );
          const notebookSparklines = document.querySelectorAll(
            "[data-cell-result] [data-query-resource-sparklines]"
          );
          const metricText = Array.from(document.querySelectorAll(".query-monitor-item-running"))
            .map((item) => item.textContent || "")
            .join("\\n");
          return monitorSparklines.length >= 2
            && notebookSparklines.length >= 2
            && /CPU avg/.test(metricText)
            && /CPU peak/.test(metricText)
            && /RAM avg/.test(metricText)
            && /RAM peak/.test(metricText);
        }
        """,
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        """
        () => {
          const canvases = Array.from(document.querySelectorAll("[data-query-resource-chart]"));
          return Boolean(window.Chart)
            && canvases.length >= 4
            && canvases.some((canvas) => Boolean(canvas._bdwQueryResourceChart));
        }
        """,
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll("[data-query-resource-chart]"))
          .map((canvas) => canvas._bdwQueryResourceChart)
          .filter(Boolean)
          .some((chart) => {
            const labels = (chart?.data?.datasets || []).map((dataset) => dataset.label).join(" ");
            return /Current|Peak sample/.test(labels) && /Average/.test(labels);
          })
        """,
        timeout=timeout_ms,
    )
    await assert_query_resource_chart_layout(
        page,
        timeout_ms,
        ".query-monitor-item-running .query-resource-sparkline-card",
        min_count=2,
    )
    await assert_query_resource_chart_layout(
        page,
        timeout_ms,
        "[data-cell-result] .query-resource-sparkline-card",
        min_count=2,
    )
    await page.wait_for_function(
        """
        () => !/RSS/.test(document.body.textContent || "")
        """,
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        """
        () => !/Recent Runtime/.test(document.body.textContent || "")
        """,
        timeout=timeout_ms,
    )
    before = await page.evaluate(
        """
        () => Array.from(document.querySelectorAll(".query-monitor-item-running .query-process-metric"))
          .slice(0, 8)
          .map((node) => Math.round(node.getBoundingClientRect().width))
        """
    )
    fixed_style = await page.evaluate(
        """
        () => {
          const metric = document.querySelector(".query-monitor-item-running .query-process-metric");
          if (!(metric instanceof HTMLElement)) {
            return null;
          }
          const style = window.getComputedStyle(metric);
          return {
            minWidth: parseFloat(style.minWidth || "0"),
            fontVariantNumeric: style.fontVariantNumeric || "",
          };
        }
        """
    )
    await page.wait_for_timeout(2500)
    after = await page.evaluate(
        """
        () => Array.from(document.querySelectorAll(".query-monitor-item-running .query-process-metric"))
          .slice(0, 8)
          .map((node) => Math.round(node.getBoundingClientRect().width))
        """
    )
    if len(before) < 4:
        raise RuntimeError(f"Expected fixed metric badges, saw before={before!r}.")
    if not fixed_style or fixed_style.get("minWidth", 0) < 120:
        raise RuntimeError(f"Metric badges do not expose a fixed minimum width: {fixed_style!r}.")
    if "tabular" not in str(fixed_style.get("fontVariantNumeric", "")).lower():
        raise RuntimeError(f"Metric badges do not use tabular numbers: {fixed_style!r}.")
    if len(after) >= 4 and any(abs(left - right) > 2 for left, right in zip(before, after)):
        raise RuntimeError(f"Metric badge widths shifted too much: before={before!r}, after={after!r}.")


async def cancel_first_query_and_assert_visibility(page, timeout_ms: int) -> None:
    await page.locator(".query-monitor-item-running [data-cancel-query-job]").first.wait_for(
        state="attached",
        timeout=timeout_ms,
    )
    await page.evaluate(
        """
        () => {
          const button = document.querySelector(".query-monitor-item-running [data-cancel-query-job]");
          if (!(button instanceof HTMLButtonElement)) {
            throw new Error("No query monitor cancel button is attached.");
          }
          button.click();
        }
        """
    )

    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll(".query-monitor-item"))
          .some((item) => /Cancelling|Interrupting|Stopping|Hard-stopping/.test(item.textContent || ""))
        """,
        timeout=timeout_ms,
    )

    notifications = page.locator("[data-query-notifications]")
    await notifications.locator("summary").click()
    await page.wait_for_function(
        """
        () => {
          const text = document.querySelector("[data-query-notification-list]")?.textContent || "";
          return /Cancellation requested|Interrupting|Stopping|Hard-stopping|cancelled/i.test(text);
        }
        """,
        timeout=timeout_ms,
    )
    await notifications.locator("summary").click()

    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll("[data-cell-result]"))
          .some((result) => /Cancellation requested|Interrupting|Stopping|Hard-stopping|Query cancelled successfully/i.test(result.textContent || ""))
        """,
        timeout=timeout_ms,
    )

    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll("[data-cell-result]"))
          .some((result) => /Query cancelled successfully/i.test(result.textContent || ""))
        """,
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll("[data-cell-result] .query-process-metric-strip"))
          .some((strip) => {
            const labels = Array.from(strip.querySelectorAll(".query-process-metric strong"))
              .map((node) => (node.textContent || "").trim());
            return labels.includes("CPU avg")
              && labels.includes("CPU peak")
              && labels.includes("RAM avg")
              && labels.includes("RAM peak")
              && !labels.includes("CPU")
              && !labels.includes("RAM");
          })
        """,
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll("[data-cell-result] .query-resource-sparkline-legend"))
          .some((legend) => {
            const text = legend.textContent || "";
            return /Peak\\s+/.test(text) && /AVG\\s+/.test(text) && !/Now\\s+/.test(text);
          })
        """,
        timeout=timeout_ms,
    )


async def assert_query_run_history(page, timeout_ms: int, cell_ids: list[str]) -> None:
    await page.wait_for_function(
        """
        async () => {
          const response = await fetch("/api/query-runs?limit=20");
          if (!response.ok) {
            return false;
          }
          const payload = await response.json();
          if (payload.available === false) {
            return false;
          }
          return Array.isArray(payload.runs)
            && payload.runs.some((run) => run.status === "cancelled")
            && payload.runs.some((run) => Array.isArray(run.resourceSamples) && run.resourceSamples.length > 0);
        }
        """,
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        """
        (cellIds) => cellIds.some((cellId) => {
          const root = document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`);
          return root instanceof HTMLDetailsElement
            && !root.open
            && root.querySelector(".workspace-query-runs-summary")
            && root.querySelector(".workspace-query-runs-chevron");
        })
        """,
        arg=cell_ids,
        timeout=timeout_ms,
    )
    collapsed_spacing = await page.evaluate(
        """
        (cellIds) => {
          const root = cellIds
            .map((cellId) => document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`))
            .find((node) => node instanceof HTMLDetailsElement && !node.open);
          const summary = root?.querySelector(".workspace-query-runs-summary");
          const title = summary?.querySelector(".workspace-query-runs-title");
          if (!(root instanceof HTMLElement) || !(summary instanceof HTMLElement) || !(title instanceof HTMLElement)) {
            return null;
          }
          const rootRect = root.getBoundingClientRect();
          const summaryRect = summary.getBoundingClientRect();
          const titleRect = title.getBoundingClientRect();
          return {
            summaryHeight: Math.round(summaryRect.height),
            topGap: Math.round(titleRect.top - summaryRect.top),
            bottomGap: Math.round(summaryRect.bottom - titleRect.bottom),
            rootTopGap: Math.round(titleRect.top - rootRect.top),
            rootBottomGap: Math.round(rootRect.bottom - titleRect.bottom),
          };
        }
        """,
        cell_ids,
    )
    if (
        not collapsed_spacing
        or collapsed_spacing.get("summaryHeight", 0) < 56
        or collapsed_spacing.get("topGap", 0) < 14
        or abs(collapsed_spacing.get("topGap", 0) - collapsed_spacing.get("bottomGap", 0)) > 3
        or abs(collapsed_spacing.get("rootTopGap", 0) - collapsed_spacing.get("rootBottomGap", 0)) > 3
    ):
        raise RuntimeError(f"Collapsed Query Runs spacing is not balanced: {collapsed_spacing!r}.")
    chevron_state = await page.evaluate(
        """
        async (cellIds) => {
          const root = cellIds
            .map((cellId) => document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`))
            .find((node) => node instanceof HTMLDetailsElement && !node.open && node.querySelector(".workspace-query-runs-chevron"));
          if (!(root instanceof HTMLDetailsElement)) {
            throw new Error("No per-cell query-run history panel found.");
          }
          const summary = root.querySelector(".workspace-query-runs-summary");
          const chevron = root.querySelector(".workspace-query-runs-chevron");
          if (!(summary instanceof HTMLElement) || !(chevron instanceof HTMLElement)) {
            throw new Error("Query-run history panel is missing its chevron.");
          }
          const collapsedTransform = getComputedStyle(chevron, "::before").transform;
          summary.click();
          await new Promise((resolve) => setTimeout(resolve, 220));
          const expandedTransform = getComputedStyle(chevron, "::before").transform;
          return {
            opened: root.open,
            collapsedTransform,
            expandedTransform,
          };
        }
        """,
        cell_ids,
    )
    if not chevron_state.get("opened"):
        raise RuntimeError("Query-run history panel did not open from summary click.")
    if chevron_state.get("collapsedTransform") == chevron_state.get("expandedTransform"):
        raise RuntimeError("Query-run history chevron did not change state when opened.")
    await page.wait_for_function(
        """
        (cellIds) => cellIds.some((cellId) => {
          const root = document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`);
          if (!(root instanceof HTMLElement)) {
            return false;
          }
          const text = root.textContent || "";
          return !/Loading query runs/i.test(text)
            && root.querySelector(".query-run-history-item")
            && root.querySelector("[data-query-resource-chart]");
        })
        """,
        arg=cell_ids,
        timeout=timeout_ms,
    )
    await assert_query_resource_chart_layout(
        page,
        timeout_ms,
        "[data-notebook-query-runs][open] .query-run-history-item .query-resource-sparkline-card",
        min_count=2,
    )


async def cancel_remaining_queries(page) -> None:
    await page.evaluate(
        """
        async () => {
          for (const button of document.querySelectorAll("[data-cancel-query-job]")) {
            if (button instanceof HTMLButtonElement && !button.disabled) {
              button.click();
            }
          }
          for (const button of document.querySelectorAll("[data-cancel-query]")) {
            if (button instanceof HTMLButtonElement && !button.disabled) {
              button.click();
            }
          }
          try {
            const response = await fetch("/api/query-jobs", { headers: { Accept: "application/json" } });
            if (!response.ok) {
              return;
            }
            const payload = await response.json();
            for (const job of payload.jobs || []) {
              if (!["queued", "running"].includes(job.status) || !job.jobId) {
                continue;
              }
              await fetch(`/api/query-jobs/${encodeURIComponent(job.jobId)}/cancel`, {
                method: "POST",
                headers: { Accept: "application/json" },
              });
            }
          } catch (_error) {
            // Best-effort cleanup; the outer wait below will absorb transient failures.
          }
        }
        """
    )
    try:
        await page.wait_for_function(
            """
            async () => {
              const response = await fetch("/api/query-jobs", { headers: { Accept: "application/json" } });
              if (!response.ok) {
                return false;
              }
              const payload = await response.json();
              return Number(payload?.summary?.runningCount || 0) === 0;
            }
            """,
            timeout=30000,
        )
    except PlaywrightError:
        pass


async def run() -> None:
    args = parse_args()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1280, "height": 900})
        try:
            cell_ids = [
                f"{SMOKE_CELL_PREFIX}-{uuid4().hex[:8]}-1",
                f"{SMOKE_CELL_PREFIX}-{uuid4().hex[:8]}-2",
            ]
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            await start_two_queries(page, cell_ids, args.timeout_ms)
            pids = await wait_for_two_live_monitor_items(page, args.timeout_ms)
            if len(set(pids)) < 2:
                raise RuntimeError(f"Expected two distinct worker PIDs, received {pids!r}.")
            await assert_resource_monitoring_ui(page, args.timeout_ms)
            await cancel_first_query_and_assert_visibility(page, args.timeout_ms)
            await assert_query_run_history(page, args.timeout_ms, cell_ids)
            print(f"Playwright query monitor process smoke passed for worker PIDs: {pids}.")
        finally:
            await cancel_remaining_queries(page)
            await browser.close()


if __name__ == "__main__":
    asyncio.run(run())

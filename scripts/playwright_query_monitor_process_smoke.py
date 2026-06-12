from __future__ import annotations

import argparse
import asyncio

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright


LONG_SQL = """
select sum(sin(i) + random()) as total_value
from range(2000000000) as source_rows(i)
"""

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
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(500)
    query_cells = page.locator("[data-query-cell]:visible")
    if not await query_cells.count():
        create_button = page.locator(
            "[data-query-workbench-entry-page] [data-create-notebook]"
        )
        await create_button.wait_for(state="visible", timeout=timeout_ms)
        await create_button.click(force=True)
        await query_cells.first.wait_for(state="visible", timeout=timeout_ms)

    await page.wait_for_function(
        """
        () => {
          const notebook = document.querySelector("[data-workspace-notebook]");
          return notebook instanceof HTMLElement
            && notebook.dataset.canEdit !== "false"
            && document.querySelectorAll("[data-query-cell]").length >= 1;
        }
        """,
        timeout=timeout_ms,
    )


async def ensure_monitor_query_cells(page, timeout_ms: int) -> list[str]:
    await page.locator("[data-query-cell]").first.wait_for(state="attached", timeout=timeout_ms)
    await page.evaluate(
        """
        () => {
          document.querySelectorAll("[data-query-cell]").forEach((cell) => {
            cell.classList.remove("is-active", "is-query-running");
          });
        }
        """
    )
    while await page.locator("[data-query-cell]").count() < 2:
        add_button = page.locator("[data-add-cell]").first
        await add_button.wait_for(state="visible", timeout=timeout_ms)
        await add_button.click(force=True)
        await page.wait_for_timeout(250)

    cell_ids = await page.evaluate(
        """
        () => Array.from(document.querySelectorAll("[data-query-cell]"))
          .slice(0, 2)
          .map((cell) => String(cell.dataset.cellId || "").trim())
          .filter(Boolean)
        """
    )
    if len(cell_ids) < 2:
        raise RuntimeError(f"Expected two real notebook cells, received {cell_ids!r}.")
    return cell_ids


async def start_two_queries(page, timeout_ms: int) -> list[str]:
    cell_ids = await ensure_monitor_query_cells(page, timeout_ms)
    snapshots = await page.evaluate(
        """
        async ({ sql, cellIds }) => {
          const cells = cellIds.map((cellId) =>
            Array.from(document.querySelectorAll("[data-query-cell]"))
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
            formData.set("clientRunStartedAt", String(Date.now()));
            formData.set("clientPreSubmitMs", "0");
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
    return cell_ids


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


async def assert_live_query_runs_page(browser, base_url: str, timeout_ms: int) -> None:
    page = await browser.new_page(viewport={"width": 1280, "height": 900})
    try:
        await page.goto(
            f"{base_url.rstrip('/')}/query-workbench/query-runs",
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        await page.locator("[data-query-runs-page]").wait_for(state="visible", timeout=timeout_ms)
        await page.wait_for_function(
            """
            () => {
              const root = document.querySelector("[data-query-runs-page]");
              return root?.dataset.queryRunsLoaded === "true";
            }
            """,
            timeout=timeout_ms,
        )
        await page.locator("[data-query-runs-toggle-live]").click(timeout=timeout_ms)
        await page.wait_for_function(
            """
            () => {
              const root = document.querySelector("[data-query-runs-page]");
              const button = root?.querySelector("[data-query-runs-toggle-live]");
              const text = root?.textContent || "";
              return button instanceof HTMLButtonElement
                && button.getAttribute("aria-pressed") === "true"
                && root?.dataset.queryRunsLiveOnly === "true"
                && !/Refreshing recorded query runs/i.test(text)
                && (/live query run\\(s\\)/i.test(text) || /No live query runs right now/i.test(text));
            }
            """,
            timeout=timeout_ms,
        )
        progress_state = await page.evaluate(
            """
            async () => {
              const root = document.querySelector("[data-query-runs-page]");
              const button = root?.querySelector("[data-query-run-progress-toggle]");
              if (!(root instanceof HTMLElement)) {
                throw new Error("Live Query Runs page is missing.");
              }
              if (!(button instanceof HTMLButtonElement)) {
                return { noLiveRows: true };
              }
              const beforeExpanded = button.getAttribute("aria-expanded");
              button.click();
              await new Promise((resolve) => setTimeout(resolve, 200));
              const row = root.querySelector(".query-run-history-progress-row");
              const text = row?.textContent || "";
              return {
                beforeExpanded,
                afterExpanded: root.querySelector("[data-query-run-progress-toggle]")?.getAttribute("aria-expanded"),
                hasProgressRow: Boolean(row),
                hasFriendlyTime: /\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} (CET|CEST|UTC)/.test(text),
                hasBackendPhase: /(queued|prepared|worker started|querying|progress)/i.test(text),
              };
            }
            """
        )
        if progress_state.get("noLiveRows"):
            return
        if progress_state.get("beforeExpanded") != "false" or progress_state.get("afterExpanded") != "true":
            raise RuntimeError(f"Live Query Runs progress toggle did not expand: {progress_state!r}.")
        if not progress_state.get("hasProgressRow"):
            raise RuntimeError(f"Live Query Runs progress row did not render: {progress_state!r}.")
        if not progress_state.get("hasFriendlyTime") or not progress_state.get("hasBackendPhase"):
            raise RuntimeError(f"Live Query Runs progress details were incomplete: {progress_state!r}.")
    finally:
        await page.close()


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
              && yTickLabels.every((label) => /%|B|KB|MB|GB/.test(label))
              && Number(chartArea.left || 0) >= 32
              && Number(canvas.width || 0) - Number(chartArea.right || 0) >= 8
              && Number(canvas.height || 0) - Number(chartArea.bottom || 0) >= 24
              && datasets.every((dataset) => Number(dataset.borderWidth || 0) >= 1.5 && dataset.fill === false && !dataset.type)
              && hasRenderedLine
              && coloredPixels(canvas) > 250
              && ["CPU %", "CPU capacity %", "CPU core %", "RAM (MB)", "Spill"].includes(axisText)
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


async def assert_result_chart_toggle(page, timeout_ms: int) -> None:
    await page.wait_for_function(
        """
        () => Array.from(document.querySelectorAll("[data-cell-result]"))
          .some((root) => root.querySelector("[data-query-result-toggle-charts]:not([hidden])")
            && root.querySelector("[data-query-resource-sparklines]"))
        """,
        timeout=timeout_ms,
    )
    toggle_state = await page.evaluate(
        """
        async () => {
          let root = Array.from(document.querySelectorAll("[data-cell-result]"))
            .find((candidate) => candidate.querySelector("[data-query-result-toggle-charts]:not([hidden])")
              && candidate.querySelector("[data-query-resource-sparklines]"));
          const rootJobId = root?.dataset?.queryJobId || "";
          let button = root?.querySelector("[data-query-result-toggle-charts]");
          if (!(root instanceof HTMLElement) || !(button instanceof HTMLButtonElement)) {
            throw new Error("Result chart toggle is missing.");
          }
          const resultToggle = root.querySelector("[data-query-result-toggle]");
          if (
            resultToggle instanceof HTMLButtonElement &&
            resultToggle.getAttribute("aria-expanded") === "false"
          ) {
            resultToggle.click();
            await new Promise((resolve) => setTimeout(resolve, 350));
            const refreshedRoot = rootJobId
              ? document.querySelector(`[data-cell-result][data-query-job-id="${rootJobId}"]`)
              : null;
            if (refreshedRoot instanceof HTMLElement) {
              root = refreshedRoot;
              button = root.querySelector("[data-query-result-toggle-charts]");
            }
          }
          const chartRoots = Array.from(root.querySelectorAll("[data-query-resource-sparklines]"));
          const visibleCardCount = () => Array.from(root.querySelectorAll(".query-resource-sparkline-card"))
            .filter((card) => {
              const rect = card.getBoundingClientRect();
              return rect.width > 0 && rect.height > 0;
            }).length;
          const beforePressed = button.getAttribute("aria-pressed");
          const beforeLabel = button.textContent || "";
          const beforeChartsHidden = chartRoots.every((chartRoot) => chartRoot.hidden);
          const beforeVisibleCards = visibleCardCount();
          button.click();
          await new Promise((resolve) => setTimeout(resolve, 350));
          const refreshedRoot = rootJobId
            ? document.querySelector(`[data-cell-result][data-query-job-id="${rootJobId}"]`)
            : null;
          if (refreshedRoot instanceof HTMLElement) {
            root = refreshedRoot;
            button = root.querySelector("[data-query-result-toggle-charts]");
          }
          const refreshedChartRoots = Array.from(root.querySelectorAll("[data-query-resource-sparklines]"));
          return {
            beforePressed,
            beforeLabel,
            beforeChartsHidden,
            beforeVisibleCards,
            afterPressed: button?.getAttribute("aria-pressed") || "",
            afterLabel: button?.textContent || "",
            afterChartsHidden: refreshedChartRoots.every((chartRoot) => chartRoot.hidden),
            afterVisibleCards: visibleCardCount(),
            rootChartsVisible: root.dataset.queryResultChartsVisible,
          };
        }
        """
    )
    if toggle_state.get("beforePressed") != "false" or not toggle_state.get("beforeChartsHidden"):
        raise RuntimeError(f"Result charts should be hidden by default: {toggle_state!r}.")
    if toggle_state.get("beforeVisibleCards") != 0:
        raise RuntimeError(f"Hidden result charts still occupied visible layout: {toggle_state!r}.")
    if toggle_state.get("afterPressed") != "true" or toggle_state.get("afterChartsHidden"):
        raise RuntimeError(f"Result chart toggle did not switch charts on: {toggle_state!r}.")
    if "Hide resource charts" not in str(toggle_state.get("afterLabel", "")):
        raise RuntimeError(f"Result chart toggle label did not update: {toggle_state!r}.")
    if int(toggle_state.get("afterVisibleCards") or 0) < 2:
        raise RuntimeError(f"Result chart toggle did not reveal chart cards: {toggle_state!r}.")
    if toggle_state.get("rootChartsVisible") != "true":
        raise RuntimeError(f"Result chart visibility state was not stored on the result root: {toggle_state!r}.")
    await assert_query_resource_chart_layout(
        page,
        timeout_ms,
        "[data-cell-result] [data-query-resource-sparklines]:not([hidden]) .query-resource-sparkline-card",
        min_count=2,
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
          const metricText = Array.from(document.querySelectorAll(
            ".query-monitor-item-running, [data-cell-result]"
          ))
            .map((item) => item.textContent || "")
            .join("\\n");
          return (monitorSparklines.length >= 2 || notebookSparklines.length >= 2)
            && /CPU avg/.test(metricText)
            && /CPU peak/.test(metricText)
            && /Threads/.test(metricText)
            && /Thread limit/.test(metricText)
            && /Active cores/.test(metricText)
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
            && canvases.length >= 2
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
    await assert_result_chart_toggle(page, timeout_ms)
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
    running_metric_count = await page.locator(
        ".query-monitor-item-running .query-process-metric"
    ).count()
    if running_metric_count < 4:
        return
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


async def assert_spill_resource_chart_regression(page, timeout_ms: int) -> None:
    await page.evaluate(
        """
        async () => {
          document.querySelector("[data-monitor-spill-regression]")?.remove();
          const cacheKey = Date.now();
          const [{ createQueryUi }, jobState, chartsModule] = await Promise.all([
            import(`/static/js/query-ui.js?spillRegression=${cacheKey}`),
            import(`/static/js/query-job-state.js?spillRegression=${cacheKey}`),
            import(`/static/js/query-resource-charts.js?spillRegression=${cacheKey}`),
          ]);
          const escapeHtml = (value) => String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
          const ui = createQueryUi({
            escapeHtml,
            formatQueryDuration: jobState.formatQueryDuration,
            formatQueryTimestamp: () => "",
            queryJobElapsedMs: jobState.queryJobElapsedMs,
            queryJobEventDateTimeCopy: () => "",
            queryJobIsRunning: jobState.queryJobIsRunning,
            queryJobStatusCopy: jobState.queryJobStatusCopy,
            isQueryResultCollapsed: () => false,
          });
          const root = document.createElement("section");
          root.dataset.monitorSpillRegression = "true";
          root.innerHTML = ui.queryResourceSparklineMarkup({
            status: "running",
            startedAt: new Date(Date.now() - 5000).toISOString(),
            resourceSamples: [
              {
                elapsedMs: 1000,
                duckdbSpillBytes: 2 * 1024 * 1024 * 1024,
                duckdbSpillOtherBytes: 1 * 1024 * 1024 * 1024,
                duckdbSpillTotalBytes: 3 * 1024 * 1024 * 1024,
                duckdbSpillLimitBytes: 96 * 1024 * 1024 * 1024,
                duckdbSpillDiskFreeBytes: 70 * 1024 * 1024 * 1024,
              },
              {
                elapsedMs: 2000,
                duckdbSpillBytes: 4 * 1024 * 1024 * 1024,
                duckdbSpillOtherBytes: 2 * 1024 * 1024 * 1024,
                duckdbSpillTotalBytes: 6 * 1024 * 1024 * 1024,
                duckdbSpillLimitBytes: 96 * 1024 * 1024 * 1024,
                duckdbSpillDiskFreeBytes: 68 * 1024 * 1024 * 1024,
              },
            ],
          });
          document.body.append(root);
          const controller = chartsModule.createQueryResourceChartsController();
          await controller.initialize(root);
        }
        """
    )
    await page.wait_for_function(
        """
        () => {
          const root = document.querySelector("[data-monitor-spill-regression]");
          const canvas = root?.querySelector('[data-query-resource-kind="spill"]');
          const text = root?.textContent || "";
          const chart = canvas?._bdwQueryResourceChart;
          const labels = (chart?.data?.datasets || []).map((dataset) => dataset.label).join(" ");
          return canvas instanceof HTMLCanvasElement
            && /DuckDB spill/.test(text)
            && /This query/.test(text)
            && /Other/.test(text)
            && /Quota/.test(text)
            && /Shared/.test(text)
            && chart
            && /This query/.test(labels)
            && /Other spill/.test(labels)
            && /Quota/.test(labels);
        }
        """,
        timeout=timeout_ms,
    )
    await assert_query_resource_chart_layout(
        page,
        timeout_ms,
        "[data-monitor-spill-regression] .query-resource-sparkline-card",
        min_count=1,
    )


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
        async () => {
          const pageText = document.body.textContent || "";
          if (/Cancelling|Interrupting|Stopping|Hard-stopping|Query cancellation completed|Query cancelled successfully|cancelled/i.test(pageText)) {
            return true;
          }
          try {
            const response = await fetch("/api/query-jobs", { headers: { Accept: "application/json" } });
            if (!response.ok) {
              return false;
            }
            const payload = await response.json();
            return Array.isArray(payload.jobs)
              && payload.jobs.some((job) => job.status === "cancelled");
          } catch (_error) {
            return false;
          }
        }
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
        async () => {
          if (Array.from(document.querySelectorAll("[data-cell-result]"))
            .some((result) => /Cancellation requested|Interrupting|Stopping|Hard-stopping|Query cancellation completed|Query cancelled successfully|cancelled/i.test(result.textContent || ""))) {
            return true;
          }
          try {
            const response = await fetch("/api/query-jobs", { headers: { Accept: "application/json" } });
            if (!response.ok) {
              return false;
            }
            const payload = await response.json();
            return Array.isArray(payload.jobs)
              && payload.jobs.some((job) => job.status === "cancelled");
          } catch (_error) {
            return false;
          }
        }
        """,
        timeout=timeout_ms,
    )

    await page.wait_for_function(
        """
        async () => {
          if (Array.from(document.querySelectorAll("[data-cell-result]"))
            .some((result) => /Query cancellation completed|Query cancelled successfully|cancelled/i.test(result.textContent || ""))) {
            return true;
          }
          try {
            const response = await fetch("/api/query-jobs", { headers: { Accept: "application/json" } });
            if (!response.ok) {
              return false;
            }
            const payload = await response.json();
            return Array.isArray(payload.jobs)
              && payload.jobs.some((job) => job.status === "cancelled" && /cancellation completed|cancelled/i.test(job.message || ""));
          } catch (_error) {
            return false;
          }
        }
        """,
        timeout=timeout_ms,
    )
    result_metric_count = await page.locator(
        "[data-cell-result] .query-process-metric-strip"
    ).count()
    if result_metric_count < 1:
        return
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
            && payload.runs.some((run) => run.timings
              && Number.isFinite(Number(run.timings.backendTotalMs))
              && Number.isFinite(Number(run.timings.engineAccessWaitMs))
              && Number.isFinite(Number(run.timings.workerStartupMs))
              && Number.isFinite(Number(run.timings.engineQueryMs))
              && Number.isFinite(Number(run.timings.resultFetchMs))
            )
            && payload.runs.some((run) => Array.isArray(run.resourceSamples) && run.resourceSamples.length > 0)
            && payload.runs.some((run) =>
              Array.isArray(run.progressEvents)
              && run.progressEvents.some((event) =>
                /\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} (CET|CEST|UTC)/.test(event.displayTime || "")
              )
            );
        }
        """,
        timeout=timeout_ms,
    )
    matching_panel_count = await page.evaluate(
        """
        (cellIds) => cellIds
          .map((cellId) => document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`))
          .filter(Boolean)
          .length
        """,
        cell_ids,
    )
    if matching_panel_count < 1:
        return
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
    await page.wait_for_function(
        """
        (cellIds) => cellIds.some((cellId) => {
          const root = document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`);
          if (!(root instanceof HTMLElement)) {
            return false;
          }
          const text = root.textContent || "";
          return !/Loading query runs/i.test(text)
            && root.querySelector(".query-run-history-table")
            && root.querySelector(".query-run-history-row")
            && root.querySelector("[data-query-runs-toggle-charts]")
            && root.querySelector("[data-query-run-progress-toggle]")
            && root.querySelector("[data-query-run-sql-toggle]")
            && /Start date/.test(text)
            && /End date/.test(text)
            && /Progress/.test(text)
            && /file lock/.test(text)
            && /startup/.test(text)
            && /query/.test(text)
            && (/fetch/.test(text) || /Cancelled|Running/.test(text))
            && /CPU avg/.test(text)
            && /RAM peak/.test(text)
            && !root.querySelector("[data-query-runs-refresh]")
            && !root.querySelector("[data-query-resource-chart]");
        })
        """,
        arg=cell_ids,
        timeout=timeout_ms,
    )
    progress_state = await page.evaluate(
        """
        async (cellIds) => {
          const root = cellIds
            .map((cellId) => document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`))
            .find((node) => node instanceof HTMLDetailsElement && node.open && node.querySelector("[data-query-run-progress-toggle]"));
          const button = root?.querySelector("[data-query-run-progress-toggle]");
          if (!(root instanceof HTMLElement) || !(button instanceof HTMLButtonElement)) {
            throw new Error("Query Runs progress chevron is missing.");
          }
          const beforeExpanded = button.getAttribute("aria-expanded");
          button.click();
          await new Promise((resolve) => setTimeout(resolve, 150));
          const row = root.querySelector(".query-run-history-progress-row");
          const text = row?.textContent || "";
          return {
            beforeExpanded,
            afterExpanded: root.querySelector("[data-query-run-progress-toggle]")?.getAttribute("aria-expanded"),
            hasProgressRow: Boolean(row),
            hasFriendlyTime: /\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} (CET|CEST|UTC)/.test(text),
            hasBackendPhase: /(queued|prepared|worker started|querying|completed|cancelled|progress)/i.test(text),
          };
        }
        """,
        cell_ids,
    )
    if progress_state.get("beforeExpanded") != "false" or progress_state.get("afterExpanded") != "true":
        raise RuntimeError(f"Query Runs progress chevron did not expand: {progress_state!r}.")
    if not progress_state.get("hasProgressRow"):
        raise RuntimeError(f"Query Runs progress sub-row did not render: {progress_state!r}.")
    if (
        not progress_state.get("hasFriendlyTime")
        or not progress_state.get("hasBackendPhase")
    ):
        raise RuntimeError(f"Query Runs progress details were incomplete: {progress_state!r}.")
    sql_state = await page.evaluate(
        """
        async (cellIds) => {
          const root = cellIds
            .map((cellId) => document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`))
            .find((node) => node instanceof HTMLDetailsElement && node.open && node.querySelector("[data-query-run-sql-toggle]"));
          const button = root?.querySelector("[data-query-run-sql-toggle]");
          if (!(root instanceof HTMLElement) || !(button instanceof HTMLButtonElement)) {
            throw new Error("Query Runs SQL chevron is missing.");
          }
          const beforeExpanded = button.getAttribute("aria-expanded");
          button.click();
          await new Promise((resolve) => setTimeout(resolve, 150));
          return {
            beforeExpanded,
            afterExpanded: root.querySelector("[data-query-run-sql-toggle]")?.getAttribute("aria-expanded"),
            hasSqlRow: Boolean(root.querySelector(".query-run-history-sql-row pre")),
          };
        }
        """,
        cell_ids,
    )
    if sql_state.get("beforeExpanded") != "false" or sql_state.get("afterExpanded") != "true":
        raise RuntimeError(f"Query Runs SQL chevron did not expand: {sql_state!r}.")
    if not sql_state.get("hasSqlRow"):
        raise RuntimeError(f"Query Runs SQL sub-row did not render: {sql_state!r}.")
    toggle_state = await page.evaluate(
        """
        async (cellIds) => {
          const root = cellIds
            .map((cellId) => document.querySelector(`[data-notebook-query-runs][data-query-runs-cell-id="${CSS.escape(cellId)}"]`))
            .find((node) => node instanceof HTMLDetailsElement && node.open && node.querySelector(".query-run-history-row"));
          const button = root?.querySelector("[data-query-runs-toggle-charts]");
          if (!(root instanceof HTMLElement) || !(button instanceof HTMLButtonElement)) {
            throw new Error("Query Runs chart toggle is missing.");
          }
          const beforePressed = button.getAttribute("aria-pressed");
          button.click();
          await new Promise((resolve) => setTimeout(resolve, 350));
          return {
            beforePressed,
            afterPressed: button.getAttribute("aria-pressed"),
            label: button.textContent || "",
            hasChartRow: Boolean(root.querySelector(".query-run-history-chart-row")),
            hasChartCanvas: Boolean(root.querySelector(".query-run-history-chart-row [data-query-resource-chart]")),
          };
        }
        """,
        cell_ids,
    )
    if toggle_state.get("beforePressed") != "false" or toggle_state.get("afterPressed") != "true":
        raise RuntimeError(f"Query Runs chart toggle did not switch on: {toggle_state!r}.")
    if "Hide resource charts" not in str(toggle_state.get("label", "")):
        raise RuntimeError(f"Query Runs chart toggle label did not update: {toggle_state!r}.")
    if not toggle_state.get("hasChartRow") or not toggle_state.get("hasChartCanvas"):
        raise RuntimeError(f"Query Runs chart rows did not render after toggle: {toggle_state!r}.")
    await assert_query_resource_chart_layout(
        page,
        timeout_ms,
        "[data-notebook-query-runs][open] .query-run-history-chart-row .query-resource-sparkline-card",
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
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            cell_ids = await start_two_queries(page, args.timeout_ms)
            pids = await wait_for_two_live_monitor_items(page, args.timeout_ms)
            if len(set(pids)) < 2:
                raise RuntimeError(f"Expected two distinct worker PIDs, received {pids!r}.")
            await assert_resource_monitoring_ui(page, args.timeout_ms)
            await assert_spill_resource_chart_regression(page, args.timeout_ms)
            await cancel_first_query_and_assert_visibility(page, args.timeout_ms)
            await assert_query_run_history(page, args.timeout_ms, cell_ids)
            await assert_live_query_runs_page(browser, args.base_url, args.timeout_ms)
            print(f"Playwright query monitor process smoke passed for worker PIDs: {pids}.")
        finally:
            await cancel_remaining_queries(page)
            await browser.close()


if __name__ == "__main__":
    asyncio.run(run())

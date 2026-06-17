from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_CSS_PATH = REPO_ROOT / "bdw" / "bit_data_workbench" / "static" / "css" / "app.css"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify Result timing breadcrumbs sit below elapsed time and compact "
            "Query Monitoring history does not create page-level horizontal scroll."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


def fixture_html(css_source: str) -> str:
    timing_steps = "".join(
        f"""
        <li class="query-timing-step is-{state}" data-query-timing-step data-query-timing-step-state="{state}">
          <span class="query-timing-step-label">{label}</span>
          <span class="query-timing-step-value">{value}</span>
        </li>
        """
        for label, value, state in [
            ("Prepare", "0 ms", "completed"),
            ("Shared DuckDB wait", "0 ms", "completed"),
            ("Startup", "745 ms", "completed"),
            ("Source setup", "401 ms", "completed"),
            ("Query", "0 ms", "current"),
            ("Fetch", "0 ms", "pending"),
            ("Delivery", "19s 222 ms", "pending"),
            ("Overhead", "-", "pending"),
        ]
    )
    rows = "".join(
        f"""
        <tr class="query-run-history-row query-run-history-row-{status}">
          <td>
            <span class="query-run-history-status is-{status}">{label}</span>
            <span class="query-run-history-message">{message}</span>
          </td>
          <td><time>17.06.2026, 08:39:44</time></td>
          <td><time>17.06.2026, 08:40:10</time></td>
          <td><span>{duration}</span><div class="query-run-history-timing"><span>Shared DuckDB wait 0 ms</span><span>Delivery 19s 222 ms</span></div></td>
          <td>582%</td>
          <td>582%</td>
          <td>18 MB</td>
          <td>18 MB</td>
          <td>200</td>
          <td class="query-run-history-sql-cell"><button class="query-run-history-sql-toggle">Progress</button></td>
          <td class="query-run-history-sql-cell"><button class="query-run-history-sql-toggle">SQL</button></td>
        </tr>
        """
        for status, label, message, duration in [
            ("running", "Running", "DuckDB is planning and executing the statement.", "20s 363 ms"),
            ("completed", "Completed", "200 row(s) shown. The result was truncated for the UI.", "2m 10s"),
            ("cancelled", "Cancelled", "Query cancellation completed.", "1m 12s"),
        ]
    )
    return f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
          {css_source}
          body {{ margin: 0; padding: 16px; background: #f5f7fa; }}
          .layout-regression-host {{ width: min(100%, 760px); box-sizing: border-box; }}
          .query-run-history-table-wrap {{ outline: 1px solid rgba(170, 37, 20, 0.22); }}
        </style>
      </head>
      <body>
        <main class="layout-regression-host">
          <section class="result-panel">
            <header class="result-header">
              <div class="result-header-copy">
                <h3>Result</h3>
                <div class="result-meta-row">
                  <span class="result-duration-group">
                    <span class="result-duration-label">Running elapsed</span>
                    <button type="button" class="result-meta result-duration-toggle result-duration-value">20s 363 ms</button>
                    <span class="result-duration-help">?</span>
                  </span>
                  <div class="result-metric-strip" data-copy-query-timings>
                    <ol class="query-timing-breadcrumb" aria-label="Query timing progress">
                      {timing_steps}
                    </ol>
                    <span class="query-insight-pill">touches 3 relations | 1 bucket</span>
                  </div>
                </div>
              </div>
            </header>
            <div class="result-body"><div class="result-empty"><p>Running...</p></div></div>
          </section>

          <details class="workspace-query-runs workspace-query-runs-cell" open>
            <summary class="workspace-query-runs-summary">
              <span class="workspace-query-runs-title"><span class="workspace-query-runs-chevron"></span><span class="workspace-tags-label">Query Monitoring</span></span>
              <span class="query-runs-status">10 monitored run(s)</span>
            </summary>
            <div class="workspace-query-runs-header workspace-query-runs-header-cell">
              <p>Recorded runs for this cell.</p>
            </div>
            <div class="query-run-history-list query-run-history-list-compact">
              <div class="query-run-history-table-wrap">
                <table class="query-run-history-table">
                  <thead>
                    <tr>
                      <th>Status</th><th>Start date</th><th>End date</th><th>Duration</th>
                      <th>CPU avg</th><th>CPU peak</th><th>RAM avg</th><th>RAM peak</th>
                      <th>Rows</th><th>Progress</th><th>SQL</th>
                    </tr>
                  </thead>
                  <tbody>{rows}</tbody>
                </table>
              </div>
            </div>
          </details>
        </main>
      </body>
    </html>
    """


async def collect_layout_metrics(page):
    return await page.evaluate(
        """
        () => {
          const doc = document.documentElement;
          const duration = document.querySelector(".result-duration-group").getBoundingClientRect();
          const breadcrumbNode = document.querySelector(".query-timing-breadcrumb");
          const breadcrumb = breadcrumbNode.getBoundingClientRect();
          const breadcrumbStyle = window.getComputedStyle(breadcrumbNode);
          const stepData = Array.from(document.querySelectorAll(".query-timing-step"))
            .map((node) => {
              const rect = node.getBoundingClientRect();
              const label = node.querySelector(".query-timing-step-label").getBoundingClientRect();
              const value = node.querySelector(".query-timing-step-value").getBoundingClientRect();
              return {
                top: rect.top,
                width: rect.width,
                labelTop: label.top,
                labelBottom: label.bottom,
                valueTop: value.top,
              };
            });
          const insight = document.querySelector(".result-metric-strip .query-insight-pill").getBoundingClientRect();
          const resultRow = document.querySelector(".result-meta-row").getBoundingClientRect();
          const host = document.querySelector(".layout-regression-host").getBoundingClientRect();
          const monitor = document.querySelector(".workspace-query-runs-cell").getBoundingClientRect();
          const list = document.querySelector(".query-run-history-list").getBoundingClientRect();
          const tableWrapNode = document.querySelector(".query-run-history-table-wrap");
          const tableWrap = tableWrapNode.getBoundingClientRect();
          return {
            clientWidth: doc.clientWidth,
            scrollWidth: doc.scrollWidth,
            durationBottom: duration.bottom,
            breadcrumbTop: breadcrumb.top,
            breadcrumbBottom: breadcrumb.bottom,
            breadcrumbWidth: breadcrumb.width,
            breadcrumbScrollWidth: breadcrumbNode.scrollWidth,
            breadcrumbFlexWrap: breadcrumbStyle.flexWrap,
            stepCount: stepData.length,
            stepWidths: stepData.map((item) => item.width),
            stepTopMin: Math.min(...stepData.map((item) => item.top)),
            stepTopMax: Math.max(...stepData.map((item) => item.top)),
            stackedStepCount: stepData.filter((item) => item.valueTop >= item.labelBottom - 2).length,
            insightTop: insight.top,
            resultRowLeft: resultRow.left,
            resultRowRight: resultRow.right,
            hostRight: host.right,
            monitorRight: monitor.right,
            listRight: list.right,
            tableWrapRight: tableWrap.right,
            tableWrapClientWidth: tableWrapNode.clientWidth,
            tableWrapScrollWidth: tableWrapNode.scrollWidth,
          };
        }
        """
    )


async def assert_layout(page, width: int, timeout_ms: int) -> None:
    await page.set_viewport_size({"width": width, "height": 760})
    await page.wait_for_function(
        """
        () => {
          const breadcrumb = document.querySelector(".query-timing-breadcrumb");
          return Boolean(
            breadcrumb
            && window.getComputedStyle(breadcrumb).display.includes("flex")
            && document.querySelectorAll(".query-timing-step").length > 0
          );
        }
        """,
        timeout=timeout_ms,
    )
    metrics = await collect_layout_metrics(page)
    if metrics["breadcrumbTop"] < metrics["durationBottom"] - 1:
        raise RuntimeError(f"Timing breadcrumb is not below elapsed time at {width}px: {metrics}")
    if metrics["breadcrumbFlexWrap"] != "nowrap":
        raise RuntimeError(f"Timing breadcrumb is allowed to wrap at {width}px: {metrics}")
    if metrics["stepTopMax"] > metrics["stepTopMin"] + 1:
        raise RuntimeError(f"Timing breadcrumb steps wrapped onto multiple rows at {width}px: {metrics}")
    if metrics["stackedStepCount"] != metrics["stepCount"]:
        raise RuntimeError(f"Timing breadcrumb values are not on a second line at {width}px: {metrics}")
    if metrics["insightTop"] < metrics["breadcrumbBottom"] - 1:
        raise RuntimeError(f"Timing breadcrumb shares its row with another metric at {width}px: {metrics}")
    if metrics["scrollWidth"] > metrics["clientWidth"] + 1:
        raise RuntimeError(f"Document has horizontal overflow at {width}px: {metrics}")
    if metrics["monitorRight"] > metrics["hostRight"] + 1:
        raise RuntimeError(f"Query Monitoring exceeds its host at {width}px: {metrics}")
    if metrics["tableWrapRight"] > metrics["listRight"] + 1:
        raise RuntimeError(f"Query Monitoring table wrapper exceeds list at {width}px: {metrics}")
    if width <= 520 and metrics["tableWrapScrollWidth"] <= metrics["tableWrapClientWidth"]:
        raise RuntimeError(f"Compact Query Monitoring did not keep overflow internal at {width}px: {metrics}")

    await page.evaluate(
        """
        () => {
          const values = [
            "999 ms",
            "1m 11s 888 ms",
            "12s 345 ms",
            "2m 03s 004 ms",
            "59s 999 ms",
            "123 ms",
            "8s 765 ms",
            "-"
          ];
          document.querySelectorAll(".query-timing-step-value").forEach((node, index) => {
            node.textContent = values[index] || "0 ms";
          });
        }
        """
    )
    changed_metrics = await collect_layout_metrics(page)
    if abs(changed_metrics["breadcrumbWidth"] - metrics["breadcrumbWidth"]) > 1:
        raise RuntimeError(
            f"Timing breadcrumb width changed when values changed at {width}px: "
            f"before={metrics} after={changed_metrics}"
        )
    if abs(changed_metrics["breadcrumbScrollWidth"] - metrics["breadcrumbScrollWidth"]) > 1:
        raise RuntimeError(
            f"Timing breadcrumb scroll width changed when values changed at {width}px: "
            f"before={metrics} after={changed_metrics}"
        )
    for index, (before, after) in enumerate(zip(metrics["stepWidths"], changed_metrics["stepWidths"])):
        if abs(after - before) > 1:
            raise RuntimeError(
                f"Timing step {index} width changed when values changed at {width}px: "
                f"before={metrics} after={changed_metrics}"
            )


async def main() -> None:
    args = parse_args()
    css_source = APP_CSS_PATH.read_text(encoding="utf-8")
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page()
        try:
            await page.set_content(fixture_html(css_source), wait_until="domcontentloaded", timeout=args.timeout_ms)
            await assert_layout(page, 900, args.timeout_ms)
            await assert_layout(page, 390, args.timeout_ms)
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

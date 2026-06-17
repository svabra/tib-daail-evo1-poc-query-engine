from __future__ import annotations

import argparse
import asyncio

from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify the live query timing breadcrumb keeps Delivery pending while "
            "DuckDB is still executing, and highlights the current phase in blue."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        try:
            page = await browser.new_page(viewport={"width": 1280, "height": 760})
            await page.goto(
                args.base_url.rstrip("/") or "http://127.0.0.1:8000",
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            result = await page.evaluate(
                """
                async () => {
                  const moduleUrl = `/static/js/query-ui.js?phase-regression=${Date.now()}`;
                  const { createQueryUi } = await import(moduleUrl);
                  const now = Date.now();
                  const escapeHtml = (value) => String(value ?? "")
                    .replaceAll("&", "&amp;")
                    .replaceAll("<", "&lt;")
                    .replaceAll(">", "&gt;")
                    .replaceAll('"', "&quot;");
                  const formatQueryDuration = (value) => {
                    const ms = Math.max(0, Math.round(Number(value) || 0));
                    if (ms >= 1000) {
                      const seconds = Math.floor(ms / 1000);
                      const remainder = ms % 1000;
                      return `${seconds}s ${remainder} ms`;
                    }
                    return `${ms} ms`;
                  };
                  const queryJobIsRunning = (job) => ["queued", "running"].includes(String(job?.status || ""));
                  const queryJobElapsedMs = (job) => {
                    if (!queryJobIsRunning(job)) {
                      return Number(job?.durationMs || 0);
                    }
                    return Math.max(0, now - Date.parse(job.startedAt));
                  };
                  const queryUi = createQueryUi({
                    escapeHtml,
                    formatQueryDuration,
                    formatQueryTimestamp: (value) => String(value || ""),
                    queryJobElapsedMs,
                    queryJobEventDateTimeCopy: () => "now",
                    queryJobIsRunning,
                    queryJobStatusCopy: (job) => String(job?.status || "unknown"),
                  });
                  const runningJob = {
                    jobId: "job-running-duckdb",
                    notebookId: "nb",
                    notebookTitle: "Timing regression",
                    cellId: "cell-1",
                    sql: "SELECT SUM(sin(i)) FROM range(200000000) AS t(i)",
                    status: "running",
                    startedAt: new Date(now - 16632).toISOString(),
                    updatedAt: new Date(now).toISOString(),
                    backendName: "VMTP DUCKDB",
                    executionMode: "duckdb-read",
                    progressLabel: "Running...",
                    message: "DuckDB is planning and executing the statement.",
                    rowsShown: 0,
                    rowCount: 0,
                    truncated: false,
                    timings: {
                      backendPrepareMs: 0,
                      engineAccessWaitMs: 0,
                      workerStartupMs: 832,
                      sourceBootstrapMs: 423,
                      engineQueryMs: 0,
                    },
                    columns: [],
                    rows: [],
                    resourceSamples: [],
                    canCancel: true,
                  };
                  document.body.innerHTML = queryUi.queryResultPanelMarkup("cell-1", runningJob);
                  const steps = [...document.querySelectorAll("[data-query-timing-step]")].map((node) => ({
                    key: node.dataset.queryTimingStepKey,
                    state: node.dataset.queryTimingStepState,
                    label: node.querySelector(".query-timing-step-label")?.textContent?.trim() || "",
                    value: node.querySelector(".query-timing-step-value")?.textContent?.trim() || "",
                    background: getComputedStyle(node).backgroundColor,
                    boxShadow: getComputedStyle(node).boxShadow,
                  }));
                  return {
                    steps,
                    currentKey: document.querySelector('[data-query-timing-step-state="current"]')?.dataset.queryTimingStepKey || "",
                    deliveryState: document.querySelector('[data-query-timing-step-key="delivery"]')?.dataset.queryTimingStepState || "",
                    deliveryValue: document.querySelector('[data-query-timing-step-key="delivery"] .query-timing-step-value')?.textContent?.trim() || "",
                    queryState: document.querySelector('[data-query-timing-step-key="query"]')?.dataset.queryTimingStepState || "",
                    currentBackground: getComputedStyle(document.querySelector('[data-query-timing-step-state="current"]')).backgroundColor,
                    currentBoxShadow: getComputedStyle(document.querySelector('[data-query-timing-step-state="current"]')).boxShadow,
                  };
                }
                """
            )
        finally:
            await browser.close()

    assert result["currentKey"] == "query", result
    assert result["queryState"] == "current", result
    assert result["deliveryState"] == "pending", result
    assert result["deliveryValue"] == "-", result
    assert "11, 68, 121" in result["currentBackground"], result
    assert "11, 68, 121" in result["currentBoxShadow"], result
    print("Query timing breadcrumb phase regression passed.")


if __name__ == "__main__":
    asyncio.run(main())

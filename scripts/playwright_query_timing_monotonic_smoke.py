from __future__ import annotations

import argparse
import asyncio

from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that completed query timing does not jump backward from the "
            "running elapsed clock to a shorter client/backend sub-measurement."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        try:
            page = await browser.new_page()
            await page.goto(
                args.base_url.rstrip("/"),
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            result = await page.evaluate(
                """
                async () => {
                  const moduleUrl = `/static/js/query-job-state.js?monotonic=${Date.now()}`;
                  const { queryJobElapsedMs } = await import(moduleUrl);
                  const runningElapsed = queryJobElapsedMs({
                    status: "running",
                    startedAt: new Date(Date.now() - 2200).toISOString(),
                  });
                  const completedWithShortClient = queryJobElapsedMs({
                    status: "completed",
                    durationMs: 937,
                    timings: {
                      clientObservedMs: 937,
                      clientTotalMs: 937,
                      backendTotalMs: 2377,
                    },
                    resourceSamples: [
                      { elapsedMs: 1000, cpuPercent: 5 },
                      { elapsedMs: 2100, cpuPercent: 11 },
                    ],
                  });
                  const completedWithoutBackendTotal = queryJobElapsedMs({
                    status: "completed",
                    durationMs: 937,
                    timings: {
                      clientObservedMs: 937,
                      clientTotalMs: 937,
                    },
                    resourceSamples: [
                      { elapsedMs: 1000, cpuPercent: 5 },
                      { elapsedMs: 2100, cpuPercent: 11 },
                    ],
                  });
                  const completedWithoutSamples = queryJobElapsedMs({
                    status: "completed",
                    durationMs: 937,
                    timings: {
                      clientObservedMs: 937,
                      backendTotalMs: 2377,
                    },
                    resourceSamples: [],
                  });
                  return {
                    runningElapsed,
                    completedWithShortClient,
                    completedWithoutBackendTotal,
                    completedWithoutSamples,
                  };
                }
                """
            )
        finally:
            await browser.close()

    running_elapsed = float(result["runningElapsed"])
    completed_with_short_client = float(result["completedWithShortClient"])
    completed_without_backend_total = float(result["completedWithoutBackendTotal"])
    completed_without_samples = float(result["completedWithoutSamples"])

    assert running_elapsed >= 2000, result
    assert completed_with_short_client == 2377, result
    assert completed_with_short_client >= running_elapsed, result
    assert completed_without_backend_total == 2100, result
    assert completed_without_samples == 2377, result


if __name__ == "__main__":
    asyncio.run(main())

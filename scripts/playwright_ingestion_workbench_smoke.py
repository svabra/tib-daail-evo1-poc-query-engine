from __future__ import annotations

import argparse
import asyncio
import sys

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the ingestion workbench generator-launch flow in the browser "
            "using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


async def open_ingestion_workbench(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/ingestion-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-generator-card]").first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def reduce_requested_size(page, timeout_ms: int) -> None:
    first_card = page.locator("[data-generator-card]").first
    await first_card.wait_for(state="visible", timeout=timeout_ms)
    size_input = first_card.locator("[data-ingestion-size-input]")
    await size_input.wait_for(state="visible", timeout=timeout_ms)
    min_value = (await size_input.get_attribute("min") or "").strip()
    current_value = (await size_input.input_value()).strip()
    next_value = min_value or current_value
    if not next_value:
        raise RuntimeError("The ingestion generator did not expose a usable size input.")
    await size_input.fill(next_value)


async def start_generator_and_assert_job(page, timeout_ms: int) -> tuple[str, str]:
    first_card = page.locator("[data-generator-card]").first
    generator_id = (await first_card.get_attribute("data-generator-id") or "").strip()
    if not generator_id:
        raise RuntimeError("The first ingestion generator card is missing its generator id.")

    launch_button = first_card.locator("[data-start-data-generation]")
    await launch_button.wait_for(state="visible", timeout=timeout_ms)

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/data-generation-jobs"),
        timeout=timeout_ms,
    ) as response_info:
        await launch_button.click()

    response = await response_info.value
    if not response.ok:
        raise RuntimeError(
            f"Data generation job creation failed with status {response.status}."
        )

    payload = await response.json()
    job_id = str(payload.get("jobId") or payload.get("job_id") or "").strip()
    if not job_id:
        raise RuntimeError(
            "The data generation job response did not include a job id."
        )

    job_card = page.locator(
        f'[data-data-generation-job-card][data-job-id="{job_id}"]'
    )
    await job_card.wait_for(state="visible", timeout=timeout_ms)

    status_copy = (await job_card.locator(".ingestion-job-status").inner_text()).strip()
    if not status_copy:
        raise RuntimeError("The ingestion job card rendered without a status label.")

    return generator_id, job_id


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        console_messages: list[str] = []
        responses: list[tuple[str, str, int]] = []
        page.on(
            "console",
            lambda msg: console_messages.append(
                f"console:{msg.type}:{msg.text}"
            ),
        )
        page.on(
            "pageerror",
            lambda exc: console_messages.append(f"pageerror:{exc}"),
        )
        page.on(
            "response",
            lambda resp: responses.append(
                (resp.request.method, resp.url, resp.status)
            ),
        )

        try:
            await open_ingestion_workbench(page, args.base_url, args.timeout_ms)
            await reduce_requested_size(page, args.timeout_ms)
            generator_id, job_id = await start_generator_and_assert_job(
                page,
                args.timeout_ms,
            )
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for method, url, status in responses:
                if "/api/data-generation-jobs" in url:
                    print(f"HTTP {method} {status} {url}", file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(
        "Playwright ingestion workbench smoke passed "
        f"for generator {generator_id} and job {job_id}."
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
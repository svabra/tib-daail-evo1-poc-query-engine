from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


NOTEBOOK_ID = "data-analysts-journey-cantonal-business-tax"
CELL_ID = f"{NOTEBOOK_ID}-cell-1"
PRODUCT_SLUG = "kantonale-gewerbesteuer-soll-ist-2022-2026"
PRODUCT_BUCKET = "data-analysts-journey"
PRODUCT_KEY = f"products/{PRODUCT_SLUG}.parquet"
PRODUCT_PATH = f"s3://{PRODUCT_BUCKET}/{PRODUCT_KEY}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Data Analyst's Journey result-storage query and publish its "
            "Parquet result to DaCa as a relation-backed JSON data product."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=180000)
    return parser.parse_args()


async def fetch_json(page, path: str) -> dict[str, object]:
    response = await page.context.request.get(
        f"{page.url.split('/notebooks/', 1)[0]}{path}",
        headers={"Accept": "application/json"},
    )
    text = await response.text()
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status} for {path}: {text}")
    return json.loads(text) if text else {}


async def wait_for_query_job(page, job_id: str, timeout_ms: int) -> dict[str, object]:
    deadline = time.monotonic() + timeout_ms / 1000
    last_job: dict[str, object] | None = None
    while time.monotonic() < deadline:
        payload = await fetch_json(page, "/api/query-jobs")
        jobs = payload.get("jobs") if isinstance(payload, dict) else []
        for job in jobs if isinstance(jobs, list) else []:
            if not isinstance(job, dict) or str(job.get("jobId") or "") != job_id:
                continue
            last_job = job
            status = str(job.get("status") or "").strip().lower()
            if status == "completed":
                return job
            if status in {"failed", "cancelled", "canceled"}:
                raise RuntimeError(
                    f"Journey query {job_id} ended as {status}: "
                    f"{job.get('error') or job}"
                )
        await page.wait_for_timeout(250)
    raise RuntimeError(
        f"Journey query {job_id} did not complete in time. Last job: {last_job}"
    )


async def run_journey_query(page, timeout_ms: int) -> dict[str, object]:
    cell = page.locator(f'[data-query-cell][data-cell-id="{CELL_ID}"]').first
    await cell.wait_for(state="visible", timeout=timeout_ms)
    form = cell.locator("[data-query-form]").first
    run_button = cell.locator("[data-run-cell]").first
    await form.wait_for(state="visible", timeout=timeout_ms)
    await run_button.wait_for(state="visible", timeout=timeout_ms)

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/query-jobs"),
        timeout=timeout_ms,
    ) as response_info:
        await run_button.click(force=True, no_wait_after=True, timeout=timeout_ms)
    response = await response_info.value
    text = await response.text()
    if not response.ok:
        raise RuntimeError(f"Journey query submission failed with HTTP {response.status}: {text}")
    started = json.loads(text)
    job_id = str(started.get("jobId") or "").strip()
    if not job_id:
        raise RuntimeError(f"Journey query response has no job id: {started}")

    job = await wait_for_query_job(page, job_id, timeout_ms)
    storage = job.get("resultStorage") if isinstance(job.get("resultStorage"), dict) else {}
    if storage.get("status") != "completed" or storage.get("path") != PRODUCT_PATH:
        raise RuntimeError(f"Journey result was not stored at {PRODUCT_PATH}: {storage}")
    if int(job.get("rowCount") or 0) != 130:
        raise RuntimeError(f"Journey query returned an unexpected row count: {job.get('rowCount')}")

    result = cell.locator(f'[data-cell-result][data-query-job-id="{job_id}"]').first
    await result.wait_for(state="visible", timeout=timeout_ms)
    summary = result.locator("[data-result-storage-summary]").first
    if (await summary.get_attribute("data-result-storage-path") or "").strip() != PRODUCT_PATH:
        raise RuntimeError("Journey result UI did not expose the configured Parquet path.")
    await result.locator("[data-publish-journey-data-product]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    return job


async def publish_journey_product(page, timeout_ms: int) -> dict[str, object]:
    cell = page.locator(f'[data-query-cell][data-cell-id="{CELL_ID}"]').first
    await cell.locator("[data-publish-journey-data-product]").dispatch_event("click")
    dialog = page.locator("[data-data-product-dialog]").first
    await dialog.wait_for(state="visible", timeout=timeout_ms)

    next_button = dialog.locator("[data-data-product-dialog-next]").first
    await next_button.dispatch_event("click")
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/data-products/preview"),
        timeout=timeout_ms,
    ) as preview_info:
        await next_button.dispatch_event("click")
    preview_response = await preview_info.value
    preview_text = await preview_response.text()
    if not preview_response.ok:
        raise RuntimeError(
            f"Journey publication preview failed with HTTP {preview_response.status}: "
            f"{preview_text}"
        )
    preview_request = json.loads(preview_response.request.post_data or "{}")
    preview = json.loads(preview_text)
    requested_source = preview_request.get("source") or {}
    expected_source = {
        "sourceKind": "relation",
        "sourceId": "s3",
        "relation": "",
        "bucket": PRODUCT_BUCKET,
        "key": PRODUCT_KEY,
    }
    for name, expected in expected_source.items():
        if requested_source.get(name) != expected:
            raise RuntimeError(
                f"Journey preview source {name} is {requested_source.get(name)!r}, "
                f"expected {expected!r}: {requested_source}"
            )
    resolved_product = preview.get("product") or {}
    if (
        resolved_product.get("bucket") != PRODUCT_BUCKET
        or resolved_product.get("key") != PRODUCT_KEY
        or not resolved_product.get("relation")
        or preview.get("responseKind") != "relation"
    ):
        raise RuntimeError(f"Journey preview did not resolve the S3 relation: {preview}")

    overwrite = dialog.locator("[data-data-product-overwrite-confirm]")
    if await overwrite.count():
        await overwrite.check()

    publish_button = dialog.locator("[data-data-product-dialog-publish]").first
    await publish_button.wait_for(state="visible", timeout=timeout_ms)
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/data-products"),
        timeout=timeout_ms,
    ) as publish_info:
        await publish_button.dispatch_event("click")
    publish_response = await publish_info.value
    publish_text = await publish_response.text()
    if not publish_response.ok:
        raise RuntimeError(
            f"Journey DaCa publication failed with HTTP {publish_response.status}: "
            f"{publish_text}"
        )
    published = json.loads(publish_text)
    product = published.get("product") or {}
    if (
        product.get("slug") != PRODUCT_SLUG
        or product.get("bucket") != PRODUCT_BUCKET
        or product.get("key") != PRODUCT_KEY
        or not product.get("relation")
        or not published.get("dacaPublication")
    ):
        raise RuntimeError(f"Journey publication response is incomplete: {published}")

    message = page.locator("[data-message-dialog]").first
    await message.wait_for(state="visible", timeout=timeout_ms)
    title = (await message.locator("[data-message-title]").inner_text()).strip()
    if "DaCa" not in title:
        raise RuntimeError(f"Journey publication success dialog did not mention DaCa: {title}")
    await message.locator("[data-message-submit]").click()
    return published


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1600, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        diagnostics: list[str] = []
        page.on("console", lambda msg: diagnostics.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: diagnostics.append(f"pageerror:{exc}"))

        try:
            await page.goto(
                f"{args.base_url.rstrip('/')}/notebooks/{NOTEBOOK_ID}",
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await page.locator(
                f'[data-workspace-notebook][data-notebook-id="{NOTEBOOK_ID}"]'
            ).wait_for(state="visible", timeout=args.timeout_ms)
            await page.wait_for_function(
                "document.documentElement.dataset.workbenchInteractive === 'true'",
                timeout=args.timeout_ms,
            )
            print("Journey notebook opened.", flush=True)
            job = await run_journey_query(page, args.timeout_ms)
            print(f"Journey query completed: {job.get('jobId')}", flush=True)
            published = await publish_journey_product(page, args.timeout_ms)
            print("Journey data product published to DaCa.", flush=True)
        except (PlaywrightTimeoutError, RuntimeError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            for message in diagnostics:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(
        "Playwright Data Analyst's Journey publication smoke passed: "
        f"job={job.get('jobId')} relation={published['product']['relation']} "
        f"DaCa={published['dacaPublication']['productId']}"
    )
    return 0


def main() -> int:
    return asyncio.run(run_smoke(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

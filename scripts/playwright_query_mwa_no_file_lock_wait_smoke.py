from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from urllib.parse import quote
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


MWA_JOIN_SQL = """
SELECT ENTI.*, ZIFF.* 
FROM s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet" ENTI
JOIN s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet" ZIFF
ON ZIFF.abrechnung_refer = ENTI.id_;
""".strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the MWA parquet join query and fail if DuckDB file-lock waits are observed."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=120000)
    return parser.parse_args()


async def ensure_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    query_cells = page.locator("[data-query-cell]:visible")
    if await query_cells.count():
        await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
        return

    create_button = page.locator(
        "[data-query-workbench-entry-page] [data-create-notebook]"
    )
    await create_button.wait_for(state="visible", timeout=timeout_ms)
    await create_button.click(force=True)
    await query_cells.first.wait_for(state="visible", timeout=timeout_ms)


async def write_sql(page, cell, sql: str, timeout_ms: int) -> None:
    editor_content = cell.locator(".cm-content").first
    if await editor_content.count():
        await editor_content.click()
        control_key = "Meta+A" if sys.platform == "darwin" else "Control+A"
        await page.keyboard.press(control_key)
        await page.keyboard.type(sql)
        cell_handle = await cell.element_handle()
        await page.wait_for_function(
            """
            ([cell, expectedSql]) => {
              const textarea = cell.querySelector('[data-editor-source]');
              return textarea instanceof HTMLTextAreaElement && textarea.value === expectedSql;
            }
            """,
            arg=[cell_handle, sql],
            timeout=timeout_ms,
        )
        return

    await cell.evaluate(
        """
        (cellNode, sql) => {
          const textarea = cellNode.querySelector('[data-editor-source]');
          if (!(textarea instanceof HTMLTextAreaElement)) {
            throw new Error('The visible query editor source could not be located.');
          }
          textarea.value = sql;
          textarea.dispatchEvent(new Event('input', { bubbles: true }));
          textarea.dispatchEvent(new Event('change', { bubbles: true }));
        }
        """,
        sql,
    )


async def submit_query_and_extract_job_id(page, cell):
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/query-jobs"),
        timeout=30000,
    ) as response_info:
        await cell.evaluate(
            """
            (cellNode) => {
              const form = cellNode.querySelector('[data-query-form]');
              if (!(form instanceof HTMLFormElement)) {
                throw new Error('The visible query form could not be located.');
              }
              form.requestSubmit();
            }
            """
        )

    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"Query start failed with status {response.status}.")

    payload = await response.json()
    return (
        str(payload.get("jobId") or payload.get("job_id") or payload.get("jobID") or "")
        .strip()
    )


def _normalize_progress_entry(value) -> str:
    return str(value or "").strip().lower()


def assert_no_duckdb_file_wait(job_id: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"completed", "failed", "cancelled"}:
        raise RuntimeError(
            f"Query {job_id} is not in a terminal state: {payload.get('status')!r}."
        )

    if status != "completed":
        raise RuntimeError(
            f"Query {job_id} completed with non-complete status {payload.get('status')!r}:"
            f" {json.dumps(payload, default=str)}"
        )

    duckdb_execution_path = str(payload.get("duckdbExecutionPath") or "").strip()
    if duckdb_execution_path != "isolated-read":
        raise RuntimeError(
            f"Query {job_id} expected isolated-read but got {duckdb_execution_path!r}."
            f" Payload: {json.dumps(payload, default=str)}"
        )

    timings = payload.get("timings")
    if not isinstance(timings, dict):
        raise RuntimeError(
            f"Query {job_id} payload is missing timings."
            f" Payload: {json.dumps(payload, default=str)}"
        )

    engine_access_wait_ms = float(timings.get("engineAccessWaitMs") or 0.0)
    if engine_access_wait_ms > 0.5:
        raise RuntimeError(
            f"Query {job_id} has unexpected file-lock wait: {engine_access_wait_ms} ms."
            f" Payload: {json.dumps(payload, default=str)}"
        )

    progress_events = payload.get("progressEvents")
    if not isinstance(progress_events, list):
        raise RuntimeError(
            f"Query {job_id} payload is missing progress events."
            f" Payload: {json.dumps(payload, default=str)}"
        )

    for event in progress_events:
        if not isinstance(event, dict):
            continue
        phase = _normalize_progress_entry(event.get("phase"))
        message = _normalize_progress_entry(event.get("message"))
        if "waiting for duckdb file access" in phase:
            raise RuntimeError(
                f"Query {job_id} has lock-wait progress phase: {event!r}."
                f" Payload: {json.dumps(payload, default=str)}"
            )
        if "waiting for duckdb file access" in message:
            raise RuntimeError(
                f"Query {job_id} has lock-wait progress message: {event!r}."
                f" Payload: {json.dumps(payload, default=str)}"
            )

    row_count = int(payload.get("rowCount") or 0)
    if row_count <= 0:
        raise RuntimeError(
            f"Query {job_id} did not return rows."
            f" Payload: {json.dumps(payload, default=str)}"
        )


async def poll_query_run(request, base_url: str, job_id: str, timeout_ms: int) -> dict[str, object]:
    endpoint = urljoin(base_url.rstrip("/") + "/", f"api/query-runs/{quote(job_id)}")
    deadline = time.monotonic() + (timeout_ms / 1000)
    last_payload: dict[str, object] | None = None

    while time.monotonic() < deadline:
        response = await request.get(endpoint)
        if response.ok:
            payload = await response.json()
            last_payload = payload
            status = str(payload.get("status") or "").strip().lower()
            if status in {"completed", "failed", "cancelled"}:
                return payload
        elif response.status not in {404, 503}:
            raise RuntimeError(
                f"Query status polling failed for {job_id}: "
                f"HTTP {response.status} {await response.text()}"
            )
        await asyncio.sleep(0.5)

    raise RuntimeError(
        f"Timed out waiting for query terminal status for {job_id}."
        f" Last payload: {json.dumps(last_payload, default=str)}"
    )


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1280, "height": 900})
        console_messages: list[str] = []
        responses: list[tuple[str, str, int]] = []
        page.on(
            "console",
            lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"),
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

        job_id = ""
        try:
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            cell = page.locator("[data-query-cell]:visible").first
            await cell.wait_for(state="visible", timeout=args.timeout_ms)
            await write_sql(page, cell, MWA_JOIN_SQL, args.timeout_ms)
            job_id = await submit_query_and_extract_job_id(page, cell)
            if not job_id:
                raise RuntimeError(f"Did not receive a job id. Responses: {responses!r}")

            payload = await poll_query_run(
                request=page.request,
                base_url=args.base_url,
                job_id=job_id,
                timeout_ms=args.timeout_ms,
            )
            assert_no_duckdb_file_wait(job_id, payload)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            if job_id:
                print(f"Run JobId: {job_id}", file=sys.stderr)
            for method, url, status in responses:
                if "/api/query-jobs" in url or "/api/query-runs" in url:
                    print(f"HTTP {method} {status} {url}", file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(f"Playwright query MWA no-file-lock smoke passed for job {job_id}.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

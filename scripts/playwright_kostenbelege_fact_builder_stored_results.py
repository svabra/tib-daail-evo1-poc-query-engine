from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.source_references import s3_source_reference  # noqa: E402
from bit_data_workbench.data_generator.kostenbelege_fact_builder_sample import (  # noqa: E402
    KBPO_SOURCE_KEYS,
    KOSTENBELEGE_FACT_BUILDER_BUCKET,
    KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID,
    KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID,
    RESULT_SET_KEYS,
    result_path,
)


CELL_IDS = [
    "kostenbelege-fact-builder-cell-1",
    "kostenbelege-fact-builder-cell-2",
    "kostenbelege-fact-builder-cell-3",
    "kostenbelege-fact-builder-cell-4",
    "kostenbelege-fact-builder-cell-5",
    "kostenbelege-fact-builder-cell-6",
]
RESULT_NAMES = list(RESULT_SET_KEYS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate Kostenbelege S3 test data, open the stored-result demo "
            "notebook, and run all six cells through the browser."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=180000)
    return parser.parse_args()


async def fetch_json(page, path: str) -> dict[str, object]:
    return await page.evaluate(
        """
        async (path) => {
          const response = await fetch(path, { headers: { Accept: "application/json" } });
          const text = await response.text();
          let payload = {};
          try {
            payload = text ? JSON.parse(text) : {};
          } catch (_error) {
            payload = { raw: text };
          }
          if (!response.ok) {
            throw new Error(`HTTP ${response.status} for ${path}: ${text}`);
          }
          return payload;
        }
        """,
        path,
    )


async def post_form_json(page, path: str, fields: dict[str, str]) -> dict[str, object]:
    return await page.evaluate(
        """
        async ({ path, fields }) => {
          const form = new FormData();
          Object.entries(fields).forEach(([key, value]) => form.set(key, value));
          const response = await fetch(path, {
            method: "POST",
            body: form,
            headers: { Accept: "application/json" },
          });
          const text = await response.text();
          let payload = {};
          try {
            payload = text ? JSON.parse(text) : {};
          } catch (_error) {
            payload = { raw: text };
          }
          if (!response.ok) {
            throw new Error(`HTTP ${response.status} for ${path}: ${text}`);
          }
          return payload;
        }
        """,
        {"path": path, "fields": fields},
    )


async def ensure_generator_is_registered(page) -> None:
    payload = await fetch_json(page, "/api/data-generators")
    generators = payload.get("generators") if isinstance(payload, dict) else []
    generator_ids = {
        str(generator.get("generatorId") or "").strip()
        for generator in generators
        if isinstance(generator, dict)
    }
    if KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID not in generator_ids:
        raise RuntimeError(
            f"Data generator {KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID!r} is not registered."
        )


async def start_loader_job(page) -> str:
    payload = await post_form_json(
        page,
        "/api/data-generation-jobs",
        {
            "generator_id": KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID,
            "size_gb": "0.001",
        },
    )
    job_id = str(payload.get("jobId") or "").strip()
    if not job_id:
        raise RuntimeError(f"Loader response did not include a job id: {payload}")
    return job_id


async def wait_for_loader_job(page, job_id: str, timeout_ms: int) -> dict[str, object]:
    deadline = time.monotonic() + timeout_ms / 1000
    last_payload: dict[str, object] | None = None
    while time.monotonic() < deadline:
        payload = await fetch_json(page, "/api/data-generation-jobs")
        jobs = payload.get("jobs") if isinstance(payload, dict) else []
        for job in (jobs if isinstance(jobs, list) else []):
            if not isinstance(job, dict) or str(job.get("jobId") or "") != job_id:
                continue
            last_payload = job
            status = str(job.get("status") or "").strip().lower()
            if status == "completed":
                targets = job.get("writtenTargets") if isinstance(job.get("writtenTargets"), list) else []
                source_target_count = sum(
                    1
                    for target in targets
                    if isinstance(target, dict)
                    and str(target.get("location") or "").startswith(
                        f"s3://{KOSTENBELEGE_FACT_BUILDER_BUCKET}/generated/kostenbelege_fact_builder/source/"
                    )
                    and str(target.get("status") or "").lower() == "written"
                )
                if source_target_count < len(KBPO_SOURCE_KEYS) + 2:
                    raise RuntimeError(
                        "Loader completed without reporting all source S3 objects as written: "
                        f"{targets}"
                    )
                return job
            if status in {"failed", "cancelled", "canceled"}:
                raise RuntimeError(f"Loader job {job_id} ended as {status}: {job}")
        await page.wait_for_timeout(750)
    raise RuntimeError(f"Loader job {job_id} did not complete in time. Last payload: {last_payload}")


async def open_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/notebooks/{KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID}",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator(
        f'[data-workspace-notebook][data-notebook-id="{KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID}"]'
    ).wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """
        (expectedCount) => document.querySelectorAll("[data-query-cell]").length === expectedCount
        """,
        arg=len(CELL_IDS),
        timeout=timeout_ms,
    )


async def wait_for_query_job(page, job_id: str, timeout_ms: int) -> dict[str, object]:
    deadline = time.monotonic() + timeout_ms / 1000
    last_payload: dict[str, object] | None = None
    while time.monotonic() < deadline:
        payload = await fetch_json(page, "/api/query-jobs")
        jobs = payload.get("jobs") if isinstance(payload, dict) else []
        for job in (jobs if isinstance(jobs, list) else []):
            if not isinstance(job, dict) or str(job.get("jobId") or "") != job_id:
                continue
            last_payload = job
            status = str(job.get("status") or "").strip().lower()
            if status == "completed":
                return job
            if status in {"failed", "cancelled", "canceled"}:
                raise RuntimeError(f"Query job {job_id} ended as {status}: {job.get('error') or job}")
        await page.wait_for_timeout(500)
    raise RuntimeError(f"Query job {job_id} did not complete in time. Last payload: {last_payload}")


async def assert_cell_result_rendered(page, cell_id: str, job_id: str, expected_path: str, timeout_ms: int) -> None:
    await page.wait_for_function(
        """
        ({ cellId, jobId }) => {
          const cell = document.querySelector(`[data-query-cell][data-cell-id="${CSS.escape(cellId)}"]`);
          const result = cell?.querySelector("[data-cell-result]");
          if (!result || result.hidden || result.dataset.queryJobId !== jobId) {
            return false;
          }
          if (result.querySelector(".result-error pre")) {
            return true;
          }
          return Boolean(
            result.querySelector("[data-result-storage-summary]") &&
            result.querySelector(".result-table tbody tr")
          );
        }
        """,
        arg={"cellId": cell_id, "jobId": job_id},
        timeout=timeout_ms,
    )
    cell = page.locator(f'[data-query-cell][data-cell-id="{cell_id}"]').first
    if await cell.locator(".result-error pre").count():
        error_text = (await cell.locator(".result-error pre").first.inner_text()).strip()
        raise RuntimeError(f"Cell {cell_id} rendered an error: {error_text}")
    storage_summary = cell.locator("[data-result-storage-summary]").first
    rendered_path = (await storage_summary.get_attribute("data-result-storage-path") or "").strip()
    if rendered_path != expected_path:
        raise RuntimeError(
            f"Cell {cell_id} stored to {rendered_path!r}, expected {expected_path!r}."
        )
    row_count = await cell.locator(".result-table tbody tr").count()
    if row_count < 1:
        raise RuntimeError(f"Cell {cell_id} did not render result rows.")


async def assert_static_cell_options(page, cell_id: str, result_name: str) -> None:
    cell = page.locator(f'[data-query-cell][data-cell-id="{cell_id}"]').first
    await cell.scroll_into_view_if_needed()
    toggle = cell.locator("[data-result-storage-toggle]").first
    path_input = cell.locator("[data-result-storage-path]").first
    await toggle.wait_for(state="attached")
    await path_input.wait_for(state="attached")
    if not await toggle.is_checked():
        raise RuntimeError(f"Cell {cell_id} does not have result storage enabled.")
    configured_path = (await path_input.input_value()).strip()
    expected_path = result_path(result_name)
    if configured_path != expected_path:
        raise RuntimeError(
            f"Cell {cell_id} result path is {configured_path!r}, expected {expected_path!r}."
        )
    virtual_sql = await cell.locator("[data-editor-source]").first.input_value()
    if "read_parquet(" in virtual_sql.lower():
        raise RuntimeError(f"Cell {cell_id} virtual SQL contains DuckDB read_parquet syntax.")


async def run_cell(page, cell_id: str, result_name: str, timeout_ms: int) -> dict[str, object]:
    await assert_static_cell_options(page, cell_id, result_name)
    cell = page.locator(f'[data-query-cell][data-cell-id="{cell_id}"]').first
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/query-jobs"),
        timeout=timeout_ms,
    ) as response_info:
        await cell.locator("[data-query-form]").first.evaluate(
            """
            (form) => {
              if (!(form instanceof HTMLFormElement)) {
                throw new Error("Cell form is missing.");
              }
              form.requestSubmit();
            }
            """
        )
    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"Cell {cell_id} query submission failed with HTTP {response.status}.")
    payload = await response.json()
    job_id = str(payload.get("jobId") or "").strip()
    if not job_id:
        raise RuntimeError(f"Cell {cell_id} query response did not include a job id: {payload}")
    job = await wait_for_query_job(page, job_id, timeout_ms)
    await assert_cell_result_rendered(page, cell_id, job_id, result_path(result_name), timeout_ms)
    return job


async def assert_follow_up_cells_reference_stored_results(page) -> None:
    expected_refs = {
        "kostenbelege-fact-builder-cell-4": [
            s3_source_reference(
                bucket=KOSTENBELEGE_FACT_BUILDER_BUCKET,
                key=RESULT_SET_KEYS["kbhp_today"],
            )
        ],
        "kostenbelege-fact-builder-cell-5": [
            s3_source_reference(
                bucket=KOSTENBELEGE_FACT_BUILDER_BUCKET,
                key=RESULT_SET_KEYS["kbkp_today"],
            ),
            s3_source_reference(
                bucket=KOSTENBELEGE_FACT_BUILDER_BUCKET,
                key=RESULT_SET_KEYS["kbpo_today"],
            ),
            s3_source_reference(
                bucket=KOSTENBELEGE_FACT_BUILDER_BUCKET,
                key=RESULT_SET_KEYS["kbhp_today"],
            ),
            s3_source_reference(
                bucket=KOSTENBELEGE_FACT_BUILDER_BUCKET,
                key=RESULT_SET_KEYS["kbhp_pos1"],
            ),
        ],
        "kostenbelege-fact-builder-cell-6": [
            s3_source_reference(
                bucket=KOSTENBELEGE_FACT_BUILDER_BUCKET,
                key=RESULT_SET_KEYS["fact_buchungsbelegposition"],
            )
        ],
    }
    for cell_id, references in expected_refs.items():
        sql = await page.locator(
            f'[data-query-cell][data-cell-id="{cell_id}"] [data-editor-source]'
        ).first.input_value()
        missing = [reference for reference in references if reference not in sql]
        if missing:
            raise RuntimeError(f"Cell {cell_id} does not reference stored result(s): {missing}")


async def assert_final_metrics(job: dict[str, object]) -> None:
    columns = [str(column) for column in job.get("columns") or []]
    rows = job.get("rows") if isinstance(job.get("rows"), list) else []
    if "total_rows" not in columns or not rows:
        raise RuntimeError(f"Final metrics result did not include total_rows: {job}")
    total_rows_index = columns.index("total_rows")
    total_rows = rows[0][total_rows_index]
    if int(total_rows) <= 0:
        raise RuntimeError(f"Final metrics total_rows was not positive: {total_rows!r}")


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(
            viewport={"width": 1600, "height": 1100},
            base_url=args.base_url.rstrip("/"),
        )
        page = await context.new_page()
        console_messages: list[str] = []
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await page.goto(
                f"{args.base_url.rstrip('/')}/loader-workbench",
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await ensure_generator_is_registered(page)
            loader_job_id = await start_loader_job(page)
            await wait_for_loader_job(page, loader_job_id, args.timeout_ms)
            await open_notebook(page, args.base_url, args.timeout_ms)
            await assert_follow_up_cells_reference_stored_results(page)
            final_job: dict[str, object] | None = None
            for cell_id, result_name in zip(CELL_IDS, RESULT_NAMES, strict=True):
                final_job = await run_cell(page, cell_id, result_name, args.timeout_ms)
            if final_job is None:
                raise RuntimeError("No query cell was executed.")
            await assert_final_metrics(final_job)
        except (PlaywrightTimeoutError, RuntimeError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await context.close()
            await browser.close()
            return 1

        await context.close()
        await browser.close()

    print(
        "Playwright Kostenbelege fact-builder stored-results smoke passed "
        f"for notebook {KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID}."
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

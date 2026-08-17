from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.notebook_presets import (  # noqa: E402
    build_kostenbelege_fact_builder_s3_pipeline_notebook,
)
from bit_data_workbench.data_generator.kostenbelege_fact_builder_sample import (  # noqa: E402
    KBPO_SOURCE_KEYS,
    KOSTENBELEGE_FACT_BUILDER_BUCKET,
    KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID,
    KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID,
)


NOTEBOOK = build_kostenbelege_fact_builder_s3_pipeline_notebook()
STAGE_IDS = [str(cell.stage["stageId"]) for cell in NOTEBOOK.cells]
FINAL_STAGE_ID = "stage-kfb-fact-buchungsbelegposition-metrics"
FIRST_STAGE_ID = "stage-kfb-kbkp-today"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate Kostenbelege S3 test data, open the Pipeline Mode fact-builder "
            "notebook, run all materialized stages, export the truncated first-stage "
            "result through the S3 dialog, and verify the stored results."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
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


async def post_json(context, base_url: str, path: str, payload: dict[str, object]) -> dict[str, object]:
    response = await context.request.post(urljoin(base_url, path), data=payload)
    text = await response.text()
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status} for {path}: {text}")
    return await response.json()


async def delete_json(page, path: str, payload: dict[str, object]) -> dict[str, object]:
    return await page.evaluate(
        """
        async ({ path, payload }) => {
          const response = await fetch(path, {
            method: "DELETE",
            headers: {
              Accept: "application/json",
              "Content-Type": "application/json",
            },
            body: JSON.stringify(payload),
          });
          const text = await response.text();
          let body = {};
          try {
            body = text ? JSON.parse(text) : {};
          } catch (_error) {
            body = { raw: text };
          }
          if (!response.ok) {
            throw new Error(`HTTP ${response.status} for ${path}: ${text}`);
          }
          return body;
        }
        """,
        {"path": path, "payload": payload},
    )


async def delete_exported_result(page, file_name: str, timeout_ms: int) -> str:
    payload = await delete_json(
        page,
        "/api/s3/explorer/entries",
        {
            "entryKind": "file",
            "bucket": KOSTENBELEGE_FACT_BUILDER_BUCKET,
            "prefix": file_name,
        },
    )
    job_id = str(payload.get("jobId") or "").strip()
    if not job_id:
        raise RuntimeError(f"S3 export cleanup did not return a delete job id: {payload}")

    deadline = time.monotonic() + timeout_ms / 1000
    last_payload: dict[str, object] = payload
    while time.monotonic() < deadline:
        last_payload = await fetch_json(page, f"/api/s3/delete-jobs/{job_id}")
        status = str(last_payload.get("status") or "").strip().lower()
        if status == "completed":
            if int(last_payload.get("deletedKeys") or 0) < 1:
                raise RuntimeError(
                    f"S3 export cleanup completed without deleting the object: {last_payload}"
                )
            return job_id
        if status in {"failed", "cancelled", "canceled"}:
            raise RuntimeError(f"S3 export cleanup ended as {status}: {last_payload}")
        await page.wait_for_timeout(250)
    raise RuntimeError(f"S3 export cleanup did not finish in time: {last_payload}")


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


async def wait_for_loader_job(page, job_id: str, timeout_ms: int) -> None:
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
                return
            if status in {"failed", "cancelled", "canceled"}:
                raise RuntimeError(f"Loader job {job_id} ended as {status}: {job}")
        await page.wait_for_timeout(750)
    raise RuntimeError(f"Loader job {job_id} did not complete in time. Last payload: {last_payload}")


async def open_pipeline_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        urljoin(base_url, f"notebooks/{KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID}"),
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator(
        f'[data-workspace-notebook][data-notebook-id="{KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID}"]'
    ).wait_for(state="visible", timeout=timeout_ms)
    await page.locator("[data-notebook-pipeline-panel]:visible").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        "(expectedCount) => document.querySelectorAll('[data-pipeline-stage-row]').length === expectedCount",
        arg=len(STAGE_IDS),
        timeout=timeout_ms,
    )


async def assert_virtual_sql_uses_stage_references(page, timeout_ms: int) -> None:
    await page.wait_for_function(
        """
        () => document.querySelectorAll('[data-editor-source]').length >= 6
        """,
        timeout=timeout_ms,
    )
    sql_values = await page.evaluate(
        """
        () => Array.from(document.querySelectorAll('[data-editor-source]'))
          .map((input) => input.value || "")
        """
    )
    combined_sql = "\n".join(sql_values).lower()
    if "read_parquet(" in combined_sql:
        raise RuntimeError("Pipeline notebook virtual SQL contains DuckDB read_parquet syntax.")
    for stage_ref in (
        "stage.kbkp_today",
        "stage.kbpo_today",
        "stage.kbhp_today",
        "stage.kbhp_pos1",
        "stage.fact_buchungsbelegposition",
    ):
        if stage_ref not in combined_sql:
            raise RuntimeError(f"Pipeline notebook SQL does not contain expected reference {stage_ref!r}.")


async def assert_and_edit_first_stage_output_path(page, timeout_ms: int) -> str:
    expected_path = str(NOTEBOOK.cells[0].stage.get("outputPath") or "").strip()
    if not expected_path:
        raise RuntimeError("Pipeline sample notebook does not define a first-stage outputPath.")
    await page.locator(
        f'[data-pipeline-stage-node="{FIRST_STAGE_ID}"] .pipeline-node-title'
    ).click(timeout=timeout_ms)
    cell = page.locator('[data-query-cell][data-cell-id="kostenbelege-fact-builder-pipeline-cell-1"]')
    await cell.wait_for(state="visible", timeout=timeout_ms)
    path_input = cell.locator("[data-result-storage-path]")
    await path_input.wait_for(state="visible", timeout=timeout_ms)
    visible_path = await path_input.input_value(timeout=timeout_ms)
    if visible_path != expected_path:
        raise RuntimeError(f"First stage path field showed {visible_path!r}, expected {expected_path!r}.")
    await cell.locator("[data-result-storage-toggle]").evaluate(
        "(toggle) => { if (!toggle.checked) throw new Error('Pipeline result-storage toggle is not checked.'); }"
    )
    custom_path = expected_path.replace("/kbkp_today.parquet", "/kbkp_today_playwright.parquet")
    await path_input.fill(custom_path, timeout=timeout_ms)
    await page.wait_for_function(
        """
        ({ cellId, expected }) => {
          const input = document.querySelector(`[data-query-cell][data-cell-id="${cellId}"] [data-result-storage-path]`);
          return input && input.value === expected && input.title.includes(expected);
        }
        """,
        arg={
            "cellId": "kostenbelege-fact-builder-pipeline-cell-1",
            "expected": custom_path,
        },
        timeout=timeout_ms,
    )
    return custom_path


def stage_statuses(graph: dict[str, object]) -> dict[str, str]:
    return {
        str(node.get("stageId") or ""): str((node.get("latestRun") or {}).get("status") or "")
        for node in graph.get("nodes", []) or []
        if isinstance(node, dict)
    }


async def fetch_graph(context, base_url: str) -> dict[str, object]:
    return await post_json(
        context,
        base_url,
        "api/materialized-stages/graph",
        {
            "notebookId": KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID,
            "notebookTitle": NOTEBOOK.title,
            "cells": NOTEBOOK.cells_payload,
            "pipelinePaths": NOTEBOOK.pipeline_paths,
        },
    )


async def wait_for_pipeline_completion(context, base_url: str, timeout_ms: int) -> dict[str, object]:
    deadline = time.monotonic() + timeout_ms / 1000
    latest_graph: dict[str, object] = {}
    terminal_statuses = {"completed", "failed", "cancelled", "skipped"}
    while time.monotonic() < deadline:
        latest_graph = await fetch_graph(context, base_url)
        diagnostics = [
            item
            for item in latest_graph.get("diagnostics", []) or []
            if isinstance(item, dict) and item.get("severity") == "error"
        ]
        if diagnostics:
            raise RuntimeError(f"Pipeline graph has diagnostics: {diagnostics}")
        active_runs = latest_graph.get("activeRuns", []) or []
        statuses = stage_statuses(latest_graph)
        if len(statuses) == len(STAGE_IDS) and not active_runs:
            if all(statuses.get(stage_id) == "completed" for stage_id in STAGE_IDS):
                return latest_graph
            if all(statuses.get(stage_id) in terminal_statuses for stage_id in STAGE_IDS):
                raise RuntimeError(f"Pipeline reached non-completed terminal statuses: {statuses}")
        await asyncio.sleep(0.75)
    raise RuntimeError(f"Timed out waiting for pipeline completion. Last graph: {latest_graph}")


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


async def query_final_metrics(page, graph: dict[str, object], timeout_ms: int) -> dict[str, object]:
    final_node = next(
        (
            node
            for node in graph.get("nodes", []) or []
            if isinstance(node, dict) and str(node.get("stageId") or "") == FINAL_STAGE_ID
        ),
        None,
    )
    if not final_node:
        raise RuntimeError(f"Final stage {FINAL_STAGE_ID!r} is missing from the graph.")
    latest_revision = final_node.get("latestRevision") if isinstance(final_node, dict) else {}
    query_sql = str((latest_revision or {}).get("querySql") or "").strip()
    if not query_sql:
        raise RuntimeError(f"Final stage did not expose a querySql reference: {final_node}")

    payload = await post_form_json(
        page,
        "/api/query-jobs",
        {
            "sql": f"SELECT * FROM {query_sql}",
            "displaySql": "SELECT * FROM stage.fact_buchungsbelegposition_metrics",
            "notebook_id": KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID,
            "notebook_title": NOTEBOOK.title,
            "cell_id": "kostenbelege-fact-builder-pipeline-metrics-verification",
            "data_sources": "s3",
            "queryOptions": '{"validation":{"sourceExistence":"off"}}',
        },
    )
    job_id = str(payload.get("jobId") or "").strip()
    if not job_id:
        raise RuntimeError(f"Final metrics query response did not include a job id: {payload}")
    job = await wait_for_query_job(page, job_id, timeout_ms)
    rows = job.get("rows") if isinstance(job.get("rows"), list) else []
    columns = job.get("columns") if isinstance(job.get("columns"), list) else []
    if not rows or not columns:
        raise RuntimeError(f"Final metrics query returned no rows: {job}")
    metrics = dict(zip([str(column) for column in columns], rows[0], strict=False))
    total_rows = int(metrics.get("total_rows") or 0)
    min_betrag = float(metrics.get("min_betrag_hw") or 0)
    max_betrag = float(metrics.get("max_betrag_hw") or 0)
    if total_rows <= 0 or min_betrag >= max_betrag:
        raise RuntimeError(f"Final metrics look invalid: {metrics}")
    return metrics


async def export_first_stage_result_to_s3(
    page,
    graph: dict[str, object],
    timeout_ms: int,
) -> dict[str, object]:
    first_node = next(
        (
            node
            for node in graph.get("nodes", []) or []
            if isinstance(node, dict) and str(node.get("stageId") or "") == FIRST_STAGE_ID
        ),
        None,
    )
    if not isinstance(first_node, dict):
        raise RuntimeError(f"Pipeline graph is missing first stage {FIRST_STAGE_ID!r}.")
    latest_run = first_node.get("latestRun") if isinstance(first_node.get("latestRun"), dict) else {}
    expected_job_id = str((latest_run or {}).get("queryJobId") or "").strip()
    latest_revision = (
        first_node.get("latestRevision")
        if isinstance(first_node.get("latestRevision"), dict)
        else {}
    )
    expected_row_count = int((latest_revision or {}).get("rowCount") or 0)
    if not expected_job_id or expected_row_count <= 200:
        raise RuntimeError(
            "First stage must expose a truncated query job and its full materialized row count: "
            f"{first_node}"
        )

    job = await wait_for_query_job(page, expected_job_id, timeout_ms)
    if job.get("truncated") is not True or int(job.get("rowsShown") or 0) != 200:
        raise RuntimeError(f"First-stage job is not the expected truncated UI result: {job}")
    if not str(job.get("resultPreviewSql") or "").strip():
        raise RuntimeError(f"First-stage job did not retain its S3 result preview SQL: {job}")

    cell = page.locator(
        '[data-query-cell][data-cell-id="kostenbelege-fact-builder-pipeline-cell-1"]'
    )
    result = cell.locator("[data-cell-result]")
    await result.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """
        ({ cellId, jobId }) => document
          .querySelector(`[data-query-cell][data-cell-id="${cellId}"] [data-cell-result]`)
          ?.getAttribute("data-query-job-id") === jobId
        """,
        arg={
            "cellId": "kostenbelege-fact-builder-pipeline-cell-1",
            "jobId": expected_job_id,
        },
        timeout=timeout_ms,
    )
    menu = result.locator("[data-result-action-menu]")
    export_button = result.locator("[data-result-export-s3]")
    for locator, label in ((menu, "result action menu"), (export_button, "S3 export action")):
        actual_job_id = str(await locator.get_attribute("data-result-job-id") or "").strip()
        if actual_job_id != expected_job_id:
            raise RuntimeError(
                f"First-stage {label} targeted {actual_job_id!r}, expected {expected_job_id!r}."
            )

    await menu.locator("> summary").click(timeout=timeout_ms)
    await export_button.click(timeout=timeout_ms)
    dialog = page.locator("[data-result-export-dialog]")
    await dialog.wait_for(state="visible", timeout=timeout_ms)
    bucket_node = dialog.locator(
        "[data-s3-explorer-node]"
        '[data-s3-explorer-kind="bucket"]'
        f'[data-s3-explorer-bucket="{KOSTENBELEGE_FACT_BUILDER_BUCKET}"]'
        '[data-s3-explorer-prefix=""]'
    )
    await bucket_node.wait_for(state="visible", timeout=timeout_ms)
    await bucket_node.locator(":scope > summary.s3-explorer-node-summary").click(
        timeout=timeout_ms
    )
    selected_path = (
        await dialog.locator("[data-result-export-selected-path]").inner_text(timeout=timeout_ms)
    ).strip()
    expected_selected_path = f"s3://{KOSTENBELEGE_FACT_BUILDER_BUCKET}/"
    if selected_path != expected_selected_path:
        raise RuntimeError(
            f"S3 export dialog selected {selected_path!r}, expected {expected_selected_path!r}."
        )

    file_name = f"stage-result-export-playwright-{int(time.time() * 1000)}.parquet"
    await dialog.locator("[data-export-format-select]").select_option("parquet")
    await dialog.locator("[data-result-export-file-name]").fill(file_name, timeout=timeout_ms)
    export_endpoint = f"/api/query-jobs/{expected_job_id}/export/s3"
    async with page.expect_response(
        lambda response: response.url.endswith(export_endpoint)
        and response.request.method == "POST",
        timeout=timeout_ms,
    ) as response_info:
        await dialog.locator("[data-result-export-submit]").click(timeout=timeout_ms)
    response = await response_info.value
    request_payload = json.loads(response.request.post_data or "{}")
    response_payload = await response.json()
    expected_request = {
        "format": "parquet",
        "bucket": KOSTENBELEGE_FACT_BUILDER_BUCKET,
        "prefix": "",
        "fileName": file_name,
        "settings": {},
    }
    if response.status != 200 or request_payload != expected_request:
        raise RuntimeError(
            "First-stage S3 export request failed or changed contract: "
            f"status={response.status}, request={request_payload}, response={response_payload}"
        )
    expected_path = f"s3://{KOSTENBELEGE_FACT_BUILDER_BUCKET}/{file_name}"
    if str(response_payload.get("path") or "") != expected_path:
        raise RuntimeError(f"S3 export returned an unexpected path: {response_payload}")

    try:
        message_dialog = page.locator("[data-message-dialog]")
        await message_dialog.wait_for(state="visible", timeout=timeout_ms)
        message_title = (
            await message_dialog.locator("[data-message-title]").inner_text(timeout=timeout_ms)
        ).strip()
        message_copy = (
            await message_dialog.locator("[data-message-copy]").inner_text(timeout=timeout_ms)
        ).strip()
        if message_title != "Results saved to Shared Workspace" or message_copy != (
            f"Saved the exported result file to {expected_path}."
        ):
            raise RuntimeError(
                f"S3 export success dialog was unexpected: {message_title!r} / {message_copy!r}"
            )

        readback = await post_form_json(
            page,
            "/api/query-jobs",
            {
                "sql": f"SELECT COUNT(*) AS total_rows FROM read_parquet('{expected_path}')",
                "displaySql": f"SELECT COUNT(*) AS total_rows FROM read_parquet('{expected_path}')",
                "notebook_id": KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID,
                "notebook_title": NOTEBOOK.title,
                "cell_id": "kostenbelege-fact-builder-pipeline-export-verification",
                "data_sources": "s3",
                "queryOptions": '{"validation":{"sourceExistence":"off"}}',
            },
        )
        readback_job_id = str(readback.get("jobId") or "").strip()
        if not readback_job_id:
            raise RuntimeError(f"S3 export readback did not return a query job id: {readback}")
        readback_job = await wait_for_query_job(page, readback_job_id, timeout_ms)
        readback_rows = (
            readback_job.get("rows") if isinstance(readback_job.get("rows"), list) else []
        )
        actual_row_count = int(readback_rows[0][0]) if readback_rows and readback_rows[0] else 0
        if actual_row_count != expected_row_count:
            raise RuntimeError(
                f"Exported first-stage Parquet has {actual_row_count} rows, "
                f"expected {expected_row_count}."
            )
    finally:
        cleanup_job_id = await delete_exported_result(page, file_name, timeout_ms)
    return {
        "path": expected_path,
        "rowCount": actual_row_count,
        "sourceJobId": expected_job_id,
        "readbackJobId": readback_job_id,
        "cleanupJobId": cleanup_job_id,
    }


async def main() -> None:
    args = parse_args()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await page.goto(urljoin(args.base_url, "query-workbench"), wait_until="domcontentloaded")
            await ensure_generator_is_registered(page)
            loader_job_id = await start_loader_job(page)
            await wait_for_loader_job(page, loader_job_id, args.timeout_ms)

            await open_pipeline_notebook(page, args.base_url, args.timeout_ms)
            await assert_virtual_sql_uses_stage_references(page, args.timeout_ms)
            edited_first_stage_path = await assert_and_edit_first_stage_output_path(page, args.timeout_ms)
            await page.locator("[data-run-notebook-pipeline]:visible").first.click(timeout=args.timeout_ms)
            graph = await wait_for_pipeline_completion(context, args.base_url, args.timeout_ms)
            order = [str(stage_id) for stage_id in graph.get("order", [])]
            if order != STAGE_IDS:
                raise RuntimeError(f"Pipeline order was {order}, expected {STAGE_IDS}.")
            first_node = next(
                (
                    node
                    for node in graph.get("nodes", []) or []
                    if isinstance(node, dict) and str(node.get("stageId") or "") == FIRST_STAGE_ID
                ),
                {},
            )
            first_output_path = str((first_node.get("latestRevision") or {}).get("outputPath") or "")
            if first_output_path != edited_first_stage_path:
                raise RuntimeError(
                    "First stage completed output path did not match the edited field: "
                    f"{first_output_path!r} != {edited_first_stage_path!r}"
                )
            exported_stage = await export_first_stage_result_to_s3(page, graph, args.timeout_ms)
            metrics = await query_final_metrics(page, graph, args.timeout_ms)
            print(
                "Playwright Kostenbelege fact-builder pipeline smoke passed "
                f"for notebook {KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID}: "
                f"metrics={metrics}, exportedStage={exported_stage}"
            )
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

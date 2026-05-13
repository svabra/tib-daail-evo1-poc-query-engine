from __future__ import annotations

import argparse
import asyncio
import contextlib
from pathlib import Path
import sys
import tempfile
import time
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
import duckdb
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify generated loader S3 CSV/JSONL/Parquet parts expose download "
            "actions and truncated query exports download all rows."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=60000)
    return parser.parse_args()


def s3_client(args: argparse.Namespace):
    return boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret_key,
    )


def purge_bucket(client, bucket_name: str) -> None:
    with contextlib.suppress(ClientError):
        paginator = client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket_name):
            objects = []
            for item in page.get("Versions") or []:
                key = str(item.get("Key") or "").strip()
                version_id = str(item.get("VersionId") or "").strip()
                if key and version_id:
                    objects.append({"Key": key, "VersionId": version_id})
            for item in page.get("DeleteMarkers") or []:
                key = str(item.get("Key") or "").strip()
                version_id = str(item.get("VersionId") or "").strip()
                if key and version_id:
                    objects.append({"Key": key, "VersionId": version_id})
            if objects:
                client.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": objects, "Quiet": True},
                )

    with contextlib.suppress(ClientError):
        response = client.list_objects_v2(Bucket=bucket_name)
        objects = [
            {"Key": item["Key"]}
            for item in response.get("Contents") or []
            if item.get("Key")
        ]
        if objects:
            client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": objects, "Quiet": True},
            )

    with contextlib.suppress(ClientError):
        client.delete_bucket(Bucket=bucket_name)


def seed_loader_objects(client, bucket_name: str, dataset_name: str) -> None:
    purge_bucket(client, bucket_name)
    client.create_bucket(Bucket=bucket_name)
    client.put_object(
        Bucket=bucket_name,
        Key=f"generated/{dataset_name}/csv/mwa_abrechnung_entities/part-00001.csv",
        Body=b"id_,status\n1,3\n",
        ContentType="text/csv",
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"generated/{dataset_name}/csv/mwa_abrechnung_entities/part-00002.csv",
        Body=b"id_,status\n2,4\n",
        ContentType="text/csv",
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"generated/{dataset_name}/json/mwa_abrechnung_entities/part-00001.jsonl",
        Body=b'{"id_":1,"status":3}\n',
        ContentType="application/x-ndjson",
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"generated/{dataset_name}/json/mwa_abrechnung_entities/part-00002.jsonl",
        Body=b'{"id_":2,"status":4}\n',
        ContentType="application/x-ndjson",
    )
    with tempfile.TemporaryDirectory(prefix="bdw-pw-parquet-") as temp_dir:
        part_one = Path(temp_dir) / "part-00001.parquet"
        part_two = Path(temp_dir) / "part-00002.parquet"
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(
                f"COPY (SELECT 1 AS id_, 3 AS status) TO '{part_one.as_posix()}' (FORMAT PARQUET)"
            )
            connection.execute(
                f"COPY (SELECT 2 AS id_, 4 AS status) TO '{part_two.as_posix()}' (FORMAT PARQUET)"
            )
        finally:
            connection.close()
        client.put_object(
            Bucket=bucket_name,
            Key=f"generated/{dataset_name}/parquet/mwa_abrechnung_entities/part-00001.parquet",
            Body=part_one.read_bytes(),
            ContentType="application/octet-stream",
        )
        client.put_object(
            Bucket=bucket_name,
            Key=f"generated/{dataset_name}/parquet/mwa_abrechnung_entities/part-00002.parquet",
            Body=part_two.read_bytes(),
            ContentType="application/octet-stream",
        )


async def ensure_details_open(page, selector: str) -> None:
    locator = page.locator(selector)
    await locator.wait_for(state="attached")
    if not await locator.evaluate("node => node.hasAttribute('open')"):
        await locator.evaluate("node => node.setAttribute('open', '')")


async def open_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/notebooks/s3-smoke-test",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(2000)
    await page.locator("[data-workspace-notebook]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.locator("[data-data-sources-section]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def wait_for_loader_object(page, bucket_name: str, object_name: str, timeout_ms: int):
    deadline = time.monotonic() + (timeout_ms / 1000)
    object_locator = page.locator(
        f'[data-source-object][data-s3-bucket="{bucket_name}"]'
        f'[data-source-object-name="{object_name}"]'
    ).first
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            await ensure_details_open(page, "[data-data-sources-section]")
            await ensure_details_open(
                page,
                '[data-source-catalog][data-source-catalog-name="workspace"]',
            )
            bucket_root = page.locator(
                f'[data-source-schema][data-source-bucket="{bucket_name}"]'
            )
            await bucket_root.wait_for(state="attached", timeout=2000)
            await ensure_details_open(
                page,
                f'[data-source-schema][data-source-bucket="{bucket_name}"]',
            )
            await object_locator.wait_for(state="visible", timeout=2000)
            return object_locator
        except Exception as exc:
            last_error = exc
            await page.wait_for_timeout(1000)

    raise AssertionError(
        f"Timed out waiting for generated S3 object {object_name!r} "
        f"in bucket {bucket_name!r}: {last_error}"
    )


async def download_from_source_object(
    page,
    object_locator,
    *,
    action_selector: str,
    expected_suffix: str,
    timeout_ms: int,
) -> str:
    await object_locator.scroll_into_view_if_needed()
    await object_locator.hover()
    menu = object_locator.locator("[data-source-action-menu]").first
    await menu.evaluate("node => node.setAttribute('open', '')")
    download_button = object_locator.locator(action_selector).first
    await download_button.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    async with page.expect_download(timeout=timeout_ms) as download_info:
        await download_button.click(force=True)
    download = await download_info.value
    if not download.suggested_filename.endswith(expected_suffix):
        raise AssertionError(
            f"Unexpected download filename suffix {expected_suffix!r}: "
            f"{download.suggested_filename!r}"
        )
    return download.suggested_filename


async def write_query_sql(page, sql: str, timeout_ms: int) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.wait_for(state="visible", timeout=timeout_ms)
    await cell.evaluate(
        """
        (cell, sql) => {
          if (!(cell instanceof HTMLElement)) {
            throw new Error("The visible query cell could not be located.");
          }
          const textarea = cell.querySelector("[data-editor-source]");
          if (!(textarea instanceof HTMLTextAreaElement)) {
            throw new Error("The first query editor source could not be located.");
          }
          textarea.value = sql;
          textarea.dispatchEvent(new Event("input", { bubbles: true }));
          textarea.dispatchEvent(new Event("change", { bubbles: true }));
        }
        """,
        sql,
    )


async def run_truncated_query_and_download_all_rows(page, timeout_ms: int) -> None:
    await write_query_sql(page, "select range as id from range(250)", timeout_ms)
    cell = page.locator("[data-query-cell]:visible").first
    await cell.locator("[data-query-form]").wait_for(state="visible", timeout=timeout_ms)

    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/query-jobs"),
        timeout=timeout_ms,
    ):
        await cell.evaluate(
            """
            (cell) => {
              const form = cell.querySelector("[data-query-form]");
              if (!(form instanceof HTMLFormElement)) {
                throw new Error("The visible query form could not be located.");
              }
              form.requestSubmit();
            }
            """
        )

    result_root = cell.locator("[data-cell-result]")
    await result_root.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """
        (cell) => {
          const result = cell.querySelector("[data-cell-result]");
          return Boolean(result?.textContent?.includes("200 row(s) shown"));
        }
        """,
        arg=await cell.element_handle(),
        timeout=timeout_ms,
    )

    export_menu = cell.locator("[data-result-action-menu]").first
    await export_menu.wait_for(state="visible", timeout=timeout_ms)
    await export_menu.evaluate("node => node.setAttribute('open', '')")
    await export_menu.locator("[data-result-export-download]").evaluate("node => node.click()")

    download_dialog = page.locator("[data-result-download-dialog][open]").first
    await download_dialog.wait_for(state="visible", timeout=timeout_ms)
    await download_dialog.locator("[data-export-format-select]").select_option("csv")
    await download_dialog.locator("[data-result-download-file-name]").fill("full-query-export.csv")

    async with page.expect_download(timeout=timeout_ms) as download_info:
        await download_dialog.locator("[data-result-download-submit]").click(force=True)
    download = await download_info.value
    if not download.suggested_filename.endswith(".csv"):
        raise AssertionError(f"Unexpected query export filename: {download.suggested_filename!r}")

    downloaded_path = await download.path()
    if not downloaded_path:
        raise AssertionError("The query export download did not produce a readable local path.")
    lines = Path(downloaded_path).read_text(encoding="utf-8").splitlines()
    if len(lines) <= 201:
        raise AssertionError(
            f"Expected full query export to contain more than 200 data rows; got {len(lines) - 1}."
        )
    if lines[0] != "id" or lines[-1] != "249":
        raise AssertionError(f"Unexpected full query export content: first={lines[:2]!r}, last={lines[-2:]!r}")


async def run_smoke(args: argparse.Namespace) -> int:
    unique_id = uuid4().hex[:8]
    bucket_name = f"pw-loader-download-{unique_id}"
    dataset_name = f"mwa_abrechnung_{unique_id}"
    client = s3_client(args)
    seed_loader_objects(client, bucket_name, dataset_name)

    console_messages: list[str] = []
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await open_query_notebook(page, args.base_url, args.timeout_ms)
            csv_object = await wait_for_loader_object(
                page,
                bucket_name,
                "mwa_abrechnung_entities_csv",
                args.timeout_ms,
            )
            json_object = await wait_for_loader_object(
                page,
                bucket_name,
                "mwa_abrechnung_entities_json",
                args.timeout_ms,
            )
            parquet_object = await wait_for_loader_object(
                page,
                bucket_name,
                "mwa_abrechnung_entities_parquet",
                args.timeout_ms,
            )

            csv_display = await csv_object.get_attribute("data-source-object-display-name")
            json_display = await json_object.get_attribute("data-source-object-display-name")
            parquet_display = await parquet_object.get_attribute("data-source-object-display-name")
            if csv_display != "mwa_abrechnung_entities.csv":
                raise AssertionError(f"Unexpected CSV display name: {csv_display!r}")
            if json_display != "mwa_abrechnung_entities.jsonl":
                raise AssertionError(f"Unexpected JSONL display name: {json_display!r}")
            if parquet_display != "mwa_abrechnung_entities.parquet":
                raise AssertionError(f"Unexpected Parquet display name: {parquet_display!r}")
            if await csv_object.get_attribute("data-s3-part-count") != "2":
                raise AssertionError("CSV generated source did not report two S3 part files.")
            if await json_object.get_attribute("data-s3-part-count") != "2":
                raise AssertionError("JSONL generated source did not report two S3 part files.")
            if await parquet_object.get_attribute("data-s3-part-count") != "2":
                raise AssertionError("Parquet generated source did not report two S3 part files.")

            await download_from_source_object(
                page,
                csv_object,
                action_selector="[data-download-source-s3-generated-merged]",
                expected_suffix=".csv",
                timeout_ms=args.timeout_ms,
            )
            await download_from_source_object(
                page,
                json_object,
                action_selector="[data-download-source-s3-generated-merged]",
                expected_suffix=".jsonl",
                timeout_ms=args.timeout_ms,
            )
            await download_from_source_object(
                page,
                parquet_object,
                action_selector="[data-download-source-s3-generated-zip]",
                expected_suffix=".zip",
                timeout_ms=args.timeout_ms,
            )

            await run_truncated_query_and_download_all_rows(page, args.timeout_ms)
        except (AssertionError, PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            purge_bucket(client, bucket_name)
            return 1
        finally:
            await browser.close()
            purge_bucket(client, bucket_name)

    print(
        "Playwright loader S3 file display/download smoke passed for "
        f"s3://{bucket_name}/generated/{dataset_name}."
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

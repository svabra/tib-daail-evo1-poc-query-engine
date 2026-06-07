from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys
import tempfile
import time
from uuid import uuid4

import boto3
import duckdb
from botocore.exceptions import ClientError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend.query_aliases import (  # noqa: E402
    normalize_query_alias_segment,
    s3_query_alias,
)
from bit_data_workbench.backend.source_references import s3_source_reference  # noqa: E402


@dataclass(frozen=True)
class SeededS3Objects:
    csv_key: str
    parquet_key: str
    generated_parquet_keys: tuple[str, ...]
    csv_reference: str
    parquet_reference: str
    generated_parquet_reference: str
    duplicated_generated_parquet_alias: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise SQL autocomplete for discovered S3 source references "
            "and execute CSV and Parquet queries through the browser."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="vat-smoke-test")
    parser.add_argument("--s3-smoke-prefix-root", default="playwright/sql-path-completion")
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


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if status not in {400, 404} and code not in {"404", "NoSuchBucket"}:
            raise
        client.create_bucket(Bucket=bucket)


def create_parquet_payload(source_kind: str = "parquet_path_completion") -> bytes:
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = Path(temp_dir) / "records.parquet"
        escaped_path = parquet_path.as_posix().replace("'", "''")
        escaped_source_kind = source_kind.replace("'", "''")
        connection = duckdb.connect()
        try:
            connection.execute(
                (
                    "COPY (SELECT 2 AS record_id, "
                    f"'{escaped_source_kind}' AS source_kind, "
                    "987.65 AS amount_chf) "
                    f"TO '{escaped_path}' (FORMAT PARQUET)"
                )
            )
        finally:
            connection.close()
        return parquet_path.read_bytes()


def legacy_generated_parquet_aliases(bucket: str, dataset_name: str) -> tuple[str, str]:
    bucket_segment = normalize_query_alias_segment(bucket, fallback="bucket")
    dataset_segment = normalize_query_alias_segment(dataset_name, fallback="folder")
    deduplicated = (
        f"s3.{bucket_segment}.generated.{dataset_segment}."
        "parquet.mwa_abrechnung_entities.parquet"
    )
    duplicated = (
        f"s3.{bucket_segment}.generated.{dataset_segment}."
        "parquet.mwa_abrechnung_entities.mwa_abrechnung_entities.parquet"
    )
    return deduplicated, duplicated


def seed_s3_objects(args: argparse.Namespace) -> SeededS3Objects:
    run_id = uuid4().hex[:10]
    prefix = f"{args.s3_smoke_prefix_root.strip('/')}/{run_id}/test"
    csv_key = f"{prefix}/sample-tax-office-500kb (1).csv"
    parquet_key = f"{prefix}/sample-tax-office-500kb (1).parquet"
    generated_dataset = f"playwright_sql_path_{run_id}"
    generated_parquet_keys = (
        f"generated/{generated_dataset}/parquet/mwa_abrechnung_entities/part-00001.parquet",
        f"generated/{generated_dataset}/parquet/mwa_abrechnung_entities/part-00002.parquet",
    )
    generated_alias, duplicated_generated_alias = legacy_generated_parquet_aliases(
        args.bucket,
        generated_dataset,
    )
    generated_reference = s3_source_reference(
        bucket=args.bucket,
        key=f"generated/{generated_dataset}/parquet/mwa_abrechnung_entities/*.parquet",
    )
    client = s3_client(args)
    ensure_bucket(client, args.bucket)
    client.put_object(
        Bucket=args.bucket,
        Key=csv_key,
        Body=(
            b"record_id,source_kind,amount_chf\n"
            b"1,csv_path_completion,123.45\n"
            b"2,csv_path_completion_extra,234.56\n"
        ),
        ContentType="text/csv",
    )
    client.put_object(
        Bucket=args.bucket,
        Key=parquet_key,
        Body=create_parquet_payload(),
        ContentType="application/octet-stream",
    )
    generated_payload = create_parquet_payload("generated_parquet_path_completion")
    for key in generated_parquet_keys:
        client.put_object(
            Bucket=args.bucket,
            Key=key,
            Body=generated_payload,
            ContentType="application/octet-stream",
        )
    return SeededS3Objects(
        csv_key=csv_key,
        parquet_key=parquet_key,
        generated_parquet_keys=generated_parquet_keys,
        csv_reference=s3_source_reference(bucket=args.bucket, key=csv_key),
        parquet_reference=s3_source_reference(bucket=args.bucket, key=parquet_key),
        generated_parquet_reference=generated_reference,
        duplicated_generated_parquet_alias=duplicated_generated_alias,
    )


async def ensure_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    deadline = time.monotonic() + timeout_ms / 1000
    query_cells = page.locator("[data-query-cell]:visible")
    create_button = page.locator(
        "[data-query-workbench-entry-page] [data-create-notebook]"
    ).first

    while time.monotonic() < deadline:
        if await query_cells.count():
            await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
            return
        if await create_button.count():
            try:
                await create_button.wait_for(state="visible", timeout=3000)
                await create_button.click(force=True)
            except PlaywrightTimeoutError:
                pass
        await page.wait_for_timeout(1000)

    raise RuntimeError("A visible query notebook cell was not available after creating a workbench.")


async def wait_for_references_in_completion_schema(
    page,
    base_url: str,
    references: list[str],
    timeout_ms: int,
) -> None:
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        await page.goto(
            f"{base_url.rstrip('/')}/query-workbench",
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        await page.wait_for_timeout(1200)
        present = await page.evaluate(
            """
            (references) => {
              const schemaNode = document.getElementById("sql-schema");
              if (!schemaNode) {
                return false;
              }
              let schema;
              try {
                schema = JSON.parse(schemaNode.textContent || "{}");
              } catch (_error) {
                return false;
              }
              const labels = Array.isArray(schema.s3References)
                ? schema.s3References.map((item) => typeof item === "string" ? item : item?.label)
                : [];
              return references.every((reference) => labels.includes(reference));
            }
            """,
            references,
        )
        if present:
            return
        await page.wait_for_timeout(2000)

    raise RuntimeError(
        "Timed out waiting for S3 source references to enter the SQL completion schema: "
        f"{references!r}"
    )


async def write_sql_with_keyboard(page, sql: str, timeout_ms: int) -> None:
    editor = page.locator("[data-query-cell]:visible .cm-content").first
    await editor.wait_for(state="visible", timeout=timeout_ms)
    await page.keyboard.press("Escape")
    await editor.click(force=True)
    select_shortcut = "Meta+A" if sys.platform == "darwin" else "Control+A"
    await page.keyboard.press(select_shortcut)
    await page.keyboard.press("Backspace")
    await page.keyboard.type(sql)


async def completion_labels(page) -> list[str]:
    return await page.evaluate(
        """
        () => Array.from(document.querySelectorAll(
          ".cm-tooltip-autocomplete [role='option'], " +
          ".cm-tooltip-autocomplete li, " +
          ".cm-tooltip-autocomplete .cm-completionLabel"
        ))
          .map((node) => (node.textContent || "").trim())
          .filter(Boolean)
        """
    )


async def assert_completion_contains(
    page,
    prefix_sql: str,
    expected_label: str,
    timeout_ms: int,
) -> None:
    await write_sql_with_keyboard(page, prefix_sql, timeout_ms)
    await page.keyboard.press("Control+Space")
    deadline = time.monotonic() + timeout_ms / 1000
    labels: list[str] = []
    while time.monotonic() < deadline:
        labels = await completion_labels(page)
        if any(label == expected_label for label in labels):
            await page.keyboard.press("Escape")
            return
        await page.wait_for_timeout(200)
    raise RuntimeError(
        f"Autocomplete did not suggest {expected_label!r} for {prefix_sql!r}. "
        f"Observed labels: {labels!r}"
    )


def source_reference_completion_probe(reference: str) -> str:
    compact = re.sub(r'["/._-]+', "", str(reference or "").lower())
    compact = compact.split(" ", 1)[0].split("(", 1)[0]
    return f"select * from {compact[:96]}"


def css_attr_equals(attribute: str, value: str) -> str:
    return f"[{attribute}={json.dumps(str(value or ''))}]"


async def assert_s3_source_reference_completions(
    page,
    csv_reference: str,
    parquet_reference: str,
    timeout_ms: int,
) -> None:
    await assert_completion_contains(
        page,
        source_reference_completion_probe(csv_reference),
        csv_reference,
        timeout_ms,
    )
    await assert_completion_contains(
        page,
        source_reference_completion_probe(parquet_reference),
        parquet_reference,
        timeout_ms,
    )


async def write_cell_sql(page, sql: str) -> None:
    cell = page.locator("[data-query-cell]:visible").first
    await cell.evaluate(
        """
        (cell, sql) => {
          const textarea = cell.querySelector("[data-editor-source]");
          if (!(textarea instanceof HTMLTextAreaElement)) {
            throw new Error("The visible SQL editor source could not be located.");
          }
          textarea.value = sql;
          textarea.dispatchEvent(new Event("input", { bubbles: true }));
          textarea.dispatchEvent(new Event("change", { bubbles: true }));
        }
        """,
        sql,
    )


async def run_query_and_assert_text(
    page,
    sql: str,
    expected_text: str,
    timeout_ms: int,
) -> None:
    await write_cell_sql(page, sql)
    cell = page.locator("[data-query-cell]:visible").first
    await page.wait_for_function(
        """
        (cell) => {
          const button = cell.querySelector("[data-run-cell]");
          return button instanceof HTMLButtonElement && !button.disabled;
        }
        """,
        arg=await cell.element_handle(),
        timeout=timeout_ms,
    )
    run_button = cell.locator("[data-run-cell]").first
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/query-jobs"),
        timeout=timeout_ms,
    ) as response_info:
        await run_button.evaluate("(button) => button.click()")

    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"Query job creation failed with status {response.status}.")

    result_root = cell.locator("[data-cell-result]").first
    await result_root.wait_for(state="visible", timeout=timeout_ms)
    await result_root.get_by_text(expected_text).first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def open_s3_explorer_file(
    page,
    base_url: str,
    bucket: str,
    key: str,
    reference: str,
    timeout_ms: int,
) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/data-sources/browser?source_id=workspace.s3",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(1000)

    navigation = page.locator("[data-data-source-explorer-navigation]").first
    await navigation.wait_for(state="visible", timeout=timeout_ms)
    await page.evaluate(
        """
        () => {
          const navigation = document.querySelector('[data-data-source-explorer-navigation]');
          navigation?.querySelectorAll('[data-source-catalog], [data-source-schema]').forEach((node) => {
            node.open = true;
            node.setAttribute('open', '');
          });
        }
        """
    )
    source_tree_file = page.locator(
        f'[data-data-source-explorer-navigation] [data-source-object][data-s3-key="{key}"]'
    ).first
    if await source_tree_file.count():
        await source_tree_file.wait_for(state="visible", timeout=timeout_ms)
        await source_tree_file.click()
        detail = page.locator("[data-data-source-explorer-detail]").first
        await detail.get_by_text(reference, exact=True).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        await detail.get_by_text(f"s3://{bucket}/{key}", exact=True).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        return

    bucket_button = page.locator(
        f'[data-data-source-explorer-s3-location][data-bucket="{bucket}"][data-prefix=""]'
    ).first
    await bucket_button.wait_for(state="visible", timeout=timeout_ms)
    await bucket_button.click()

    segments = key.split("/")[:-1]
    cumulative = ""
    for segment in segments:
        cumulative = f"{cumulative}{segment}/"
        location_button = page.locator(
            f'[data-data-source-explorer-s3-location][data-bucket="{bucket}"][data-prefix="{cumulative}"]'
        ).first
        await location_button.wait_for(state="visible", timeout=timeout_ms)
        await location_button.click()

    navigation = page.locator("[data-data-source-explorer-navigation]").first
    await navigation.get_by_text(reference, exact=True).first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    breadcrumb_labels = [
        label.strip()
        for label in await page.locator(".data-source-explorer-breadcrumb").all_text_contents()
        if label.strip()
    ]
    expected_labels = ["s3", normalize_query_alias_segment(bucket, fallback="bucket")]
    expected_labels.extend(
        normalize_query_alias_segment(segment, fallback="folder")
        for segment in segments
    )
    if breadcrumb_labels != expected_labels:
        raise RuntimeError(
            "S3 explorer breadcrumbs did not mirror the query hierarchy. "
            f"Expected {expected_labels!r}, observed {breadcrumb_labels!r}."
        )

    file_button = page.locator(
        f'[data-data-source-explorer-s3-file="{key}"][data-bucket="{bucket}"]'
    ).first
    await file_button.wait_for(state="visible", timeout=timeout_ms)
    await file_button.click()

    detail = page.locator("[data-data-source-explorer-detail]").first
    await detail.get_by_text(reference, exact=True).first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await detail.get_by_text(f"s3://{bucket}/{key}", exact=True).first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def copy_visible_query_path(page, expected_reference: str, timeout_ms: int) -> str:
    copy_button = page.locator(
        '[data-data-source-explorer-action="copy-query-path"]'
    ).first
    if await copy_button.count():
        await copy_button.wait_for(state="visible", timeout=timeout_ms)
        await copy_button.click()
    else:
        source = page.locator(
            "[data-data-source-explorer-navigation] [data-source-object]"
            + css_attr_equals("data-source-object-query-reference", expected_reference)
        ).first
        await source.wait_for(state="visible", timeout=timeout_ms)
        await open_sidebar_source_action_menu(source)
        await source.locator("[data-copy-query-path]").first.evaluate("(button) => button.click()")
    copied = await page.evaluate("navigator.clipboard.readText()")
    if copied != expected_reference:
        raise RuntimeError(
            f"Copy source reference copied {copied!r}; expected {expected_reference!r}."
        )
    return copied


async def expand_sidebar_source_tree(page) -> None:
    await page.evaluate(
        """
        () => {
          for (const selector of [
            '[data-data-sources-section]',
            '[data-source-catalog]',
            '[data-source-schema]',
          ]) {
            document.querySelectorAll(selector).forEach((node) => {
              node.open = true;
              node.setAttribute('open', '');
            });
          }
        }
        """
    )
    await page.wait_for_timeout(250)


async def sidebar_source_for_reference(page, reference: str, timeout_ms: int):
    await expand_sidebar_source_tree(page)
    source = page.locator(
        "[data-source-object]"
        + css_attr_equals("data-source-object-query-reference", reference)
    ).first
    await source.wait_for(state="visible", timeout=timeout_ms)
    await source.scroll_into_view_if_needed(timeout=timeout_ms)
    return source


async def open_sidebar_source_action_menu(source) -> None:
    menu = source.locator("[data-source-action-menu]").first
    await menu.evaluate(
        """
        (menu) => {
          if (menu instanceof HTMLDetailsElement) {
            menu.open = true;
            menu.setAttribute('open', '');
          }
        }
        """
    )


async def copy_sidebar_query_path(page, expected_reference: str, timeout_ms: int) -> str:
    source = await sidebar_source_for_reference(page, expected_reference, timeout_ms)
    await open_sidebar_source_action_menu(source)
    await source.locator("[data-copy-query-path]").first.evaluate("(button) => button.click()")
    copied = await page.evaluate("navigator.clipboard.readText()")
    if copied != expected_reference:
        raise RuntimeError(
            f"Sidebar copy source reference copied {copied!r}; expected {expected_reference!r}."
        )
    return copied


async def query_sidebar_source_in_new_notebook(
    page,
    expected_reference: str,
    forbidden_alias: str,
    timeout_ms: int,
) -> str:
    previous_notebook_id = (
        await page.locator("[data-notebook-meta]").first.get_attribute("data-notebook-id")
    ) or ""
    source = await sidebar_source_for_reference(page, expected_reference, timeout_ms)
    await open_sidebar_source_action_menu(source)
    await source.locator("[data-query-source-new]").first.evaluate("(button) => button.click()")
    await page.wait_for_function(
        """
        ({ previousNotebookId, expectedReference }) => {
          const meta = document.querySelector('[data-notebook-meta]');
          const textarea = document.querySelector('[data-query-cell] [data-editor-source]');
          return Boolean(
            meta &&
            meta.dataset.notebookId &&
            meta.dataset.notebookId !== previousNotebookId &&
            textarea instanceof HTMLTextAreaElement &&
            textarea.value.includes(expectedReference)
          );
        }
        """,
        arg={
            "previousNotebookId": previous_notebook_id,
            "expectedReference": expected_reference,
        },
        timeout=timeout_ms,
    )
    sql_text = await page.locator("[data-query-cell] [data-editor-source]").first.input_value()
    if forbidden_alias in sql_text:
        raise RuntimeError(
            "Query in new notebook used a duplicated generated S3 alias: "
            f"{sql_text!r}."
        )
    return sql_text


async def seed_local_workspace_entry(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.evaluate(
        """
        async () => {
          const database = await new Promise((resolve, reject) => {
            const request = indexedDB.open("bdw.localWorkspace.v1", 1);
            request.onupgradeneeded = () => {
              const db = request.result;
              if (!db.objectStoreNames.contains("exports")) {
                db.createObjectStore("exports", { keyPath: "id" });
              }
            };
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
          });
          await new Promise((resolve, reject) => {
            const transaction = database.transaction("exports", "readwrite");
            const store = transaction.objectStore("exports");
            store.put({
              id: "playwright-query-path-local",
              fileName: "local-query-path.csv",
              folderPath: "playwright",
              exportFormat: "csv",
              mimeType: "text/csv",
              sizeBytes: 20,
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
              columnCount: 1,
              rowCount: 1,
              csvDelimiter: ",",
              csvHasHeader: true,
              blob: new Blob(["record_id\\n1\\n"], { type: "text/csv" }),
            });
            transaction.oncomplete = () => resolve();
            transaction.onerror = () => reject(transaction.error);
            transaction.onabort = () => reject(transaction.error);
          });
          database.close();
        }
        """
    )


async def wait_for_any_copy_query_path_action(page, timeout_ms: int) -> None:
    await page.wait_for_function(
        """
        () => Boolean(
          document.querySelector('[data-data-source-explorer-action="copy-query-path"]') ||
          document.querySelector('[data-data-source-explorer-navigation] [data-copy-query-path]')
        )
        """,
        timeout=timeout_ms,
    )


async def assert_copy_query_path_actions_for_other_sources(
    page,
    base_url: str,
    timeout_ms: int,
) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/data-sources/browser?source_id=pg_oltp",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await wait_for_any_copy_query_path_action(page, timeout_ms)

    await seed_local_workspace_entry(page, base_url, timeout_ms)
    await page.goto(
        f"{base_url.rstrip('/')}/data-sources/browser?source_id=workspace.local",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await wait_for_any_copy_query_path_action(page, timeout_ms)


async def run_smoke(args: argparse.Namespace) -> int:
    seeded = seed_s3_objects(args)
    client = s3_client(args)
    console_messages: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
            permissions=["clipboard-read", "clipboard-write"],
        )
        page = await context.new_page()
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await wait_for_references_in_completion_schema(
                page,
                args.base_url,
                [
                    seeded.csv_reference,
                    seeded.parquet_reference,
                    seeded.generated_parquet_reference,
                ],
                args.timeout_ms,
            )
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            await assert_s3_source_reference_completions(
                page,
                seeded.csv_reference,
                seeded.parquet_reference,
                args.timeout_ms,
            )
            await run_query_and_assert_text(
                page,
                f"select source_kind, amount_chf from {seeded.csv_reference} order by record_id limit 1",
                "csv_path_completion",
                args.timeout_ms,
            )
            await run_query_and_assert_text(
                page,
                f"select source_kind, amount_chf from {seeded.parquet_reference} limit 1",
                "parquet_path_completion",
                args.timeout_ms,
            )
            await run_query_and_assert_text(
                page,
                f"select source_kind, amount_chf from {seeded.generated_parquet_reference} limit 1",
                "generated_parquet_path_completion",
                args.timeout_ms,
            )
            await open_s3_explorer_file(
                page,
                args.base_url,
                args.bucket,
                seeded.parquet_key,
                seeded.parquet_reference,
                args.timeout_ms,
            )
            copied_alias = await copy_visible_query_path(
                page,
                seeded.parquet_reference,
                args.timeout_ms,
            )
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            await run_query_and_assert_text(
                page,
                f"select source_kind, amount_chf from {copied_alias} limit 1",
                "parquet_path_completion",
                args.timeout_ms,
            )
            await ensure_query_notebook(page, args.base_url, args.timeout_ms)
            await copy_sidebar_query_path(
                page,
                seeded.generated_parquet_reference,
                args.timeout_ms,
            )
            await query_sidebar_source_in_new_notebook(
                page,
                seeded.generated_parquet_reference,
                seeded.duplicated_generated_parquet_alias,
                args.timeout_ms,
            )
            await assert_copy_query_path_actions_for_other_sources(
                page,
                args.base_url,
                args.timeout_ms,
            )
        except (ClientError, PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            print(f"CSV reference: {seeded.csv_reference}", file=sys.stderr)
            print(f"Parquet reference: {seeded.parquet_reference}", file=sys.stderr)
            print(f"Generated Parquet reference: {seeded.generated_parquet_reference}", file=sys.stderr)
            print(
                f"Forbidden generated Parquet alias: {seeded.duplicated_generated_parquet_alias}",
                file=sys.stderr,
            )
            for message in console_messages:
                print(message, file=sys.stderr)
            await context.close()
            await browser.close()
            return 1
        finally:
            with contextlib.suppress(Exception):
                await context.close()
            with contextlib.suppress(Exception):
                await browser.close()
            for key in (
                seeded.csv_key,
                seeded.parquet_key,
                *seeded.generated_parquet_keys,
            ):
                with contextlib.suppress(Exception):
                    client.delete_object(Bucket=args.bucket, Key=key)

    print(
        "Playwright SQL S3 source reference completion smoke passed for "
        f"{seeded.csv_reference}, {seeded.parquet_reference}, "
        f"and {seeded.generated_parquet_reference}."
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise SQL autocomplete for discovered S3 object path aliases "
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
    parser.add_argument(
        "--completion-refresh-only",
        action="store_true",
        help="Only verify that an already-created SQL editor receives late S3 completion schema updates.",
    )
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


def create_parquet_payload() -> bytes:
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = Path(temp_dir) / "records.parquet"
        escaped_path = parquet_path.as_posix().replace("'", "''")
        connection = duckdb.connect()
        try:
            connection.execute(
                (
                    "COPY (SELECT 2 AS record_id, "
                    "'parquet_path_completion' AS source_kind, "
                    "987.65 AS amount_chf) "
                    f"TO '{escaped_path}' (FORMAT PARQUET)"
                )
            )
        finally:
            connection.close()
        return parquet_path.read_bytes()


def seed_s3_objects(args: argparse.Namespace) -> tuple[str, str, str, str]:
    run_id = uuid4().hex[:10]
    prefix = f"{args.s3_smoke_prefix_root.strip('/')}/{run_id}/test"
    csv_key = f"{prefix}/sample-tax-office-500kb (1).csv"
    parquet_key = f"{prefix}/sample-tax-office-500kb (1).parquet"
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
    return (
        csv_key,
        parquet_key,
        s3_query_alias(bucket=args.bucket, key=csv_key),
        s3_query_alias(bucket=args.bucket, key=parquet_key),
    )


async def ensure_query_notebook(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(1500)
    query_cells = page.locator("[data-query-cell]:visible")
    if await query_cells.count():
        await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
        return

    notebook_link = page.locator(".notebook-link[data-notebook-id]").first
    if await notebook_link.count():
        href = await notebook_link.get_attribute("href")
        if href:
            await page.goto(
                f"{base_url.rstrip('/')}{href}",
                wait_until="domcontentloaded",
                timeout=timeout_ms,
            )
        await query_cells.first.wait_for(state="visible", timeout=timeout_ms)
        return

    create_button = page.locator(
        "[data-query-workbench-entry-page] [data-create-notebook]"
    ).first
    await create_button.wait_for(state="visible", timeout=timeout_ms)
    await create_button.click(force=True)
    await query_cells.first.wait_for(state="visible", timeout=timeout_ms)


async def wait_for_aliases_in_completion_schema(
    page,
    base_url: str,
    aliases: list[str],
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
            (aliases) => {
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
              const hasAlias = (alias) => {
                let node = schema;
                for (const part of String(alias || "").split(".")) {
                  if (!node || typeof node !== "object" || !(part in node)) {
                    return false;
                  }
                  node = node[part];
                }
                return true;
              };
              return aliases.every(hasAlias);
            }
            """,
            aliases,
        )
        if present:
            return
        await page.wait_for_timeout(2000)

    raise RuntimeError(
        "Timed out waiting for S3 aliases to enter the SQL completion schema: "
        f"{aliases!r}"
    )


async def wait_for_aliases_in_current_completion_schema(
    page,
    aliases: list[str],
    timeout_ms: int,
) -> None:
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        present = await page.evaluate(
            """
            (aliases) => {
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
              const hasAlias = (alias) => {
                let node = schema;
                for (const part of String(alias || "").split(".")) {
                  if (!node || typeof node !== "object" || !(part in node)) {
                    return false;
                  }
                  node = node[part];
                }
                return true;
              };
              return aliases.every(hasAlias);
            }
            """,
            aliases,
        )
        if present:
            return
        await page.wait_for_timeout(500)

    raise RuntimeError(
        "Timed out waiting for S3 aliases to refresh in the current page "
        f"completion schema without a reload: {aliases!r}"
    )


async def write_sql_with_keyboard(page, sql: str, timeout_ms: int) -> None:
    editor = page.locator("[data-query-cell]:visible .cm-content").first
    await editor.wait_for(state="visible", timeout=timeout_ms)
    await editor.click()
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
            return
        await page.wait_for_timeout(200)
    raise RuntimeError(
        f"Autocomplete did not suggest {expected_label!r} for {prefix_sql!r}. "
        f"Observed labels: {labels!r}"
    )


async def assert_s3_path_completions(
    page,
    bucket: str,
    csv_alias: str,
    parquet_alias: str,
    timeout_ms: int,
) -> None:
    bucket_segment = normalize_query_alias_segment(bucket, fallback="bucket")
    await assert_completion_contains(
        page,
        "select * from s3.",
        bucket_segment,
        timeout_ms,
    )

    csv_parts = csv_alias.split(".")
    parquet_parts = parquet_alias.split(".")
    await assert_completion_contains(
        page,
        f"select * from {'.'.join(csv_parts[:-1])}.",
        csv_parts[-1],
        timeout_ms,
    )
    await assert_completion_contains(
        page,
        f"select * from {'.'.join(parquet_parts[:-1])}.",
        parquet_parts[-1],
        timeout_ms,
    )
    await assert_completion_contains(
        page,
        f"select * from s3.{bucket_segment}.sam",
        csv_alias,
        timeout_ms,
    )
    await assert_completion_contains(
        page,
        f"select * from s3.{bucket_segment}.sam",
        parquet_alias,
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
    alias: str,
    timeout_ms: int,
) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/data-sources/browser?source_id=workspace.s3",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(1000)

    source_tree_file = page.locator(
        f'[data-data-source-explorer-navigation] [data-source-object][data-source-object-query-alias="{alias}"]'
    ).first
    try:
        await source_tree_file.wait_for(state="attached", timeout=5000)
        await source_tree_file.evaluate(
            """
            (node) => {
              let current = node.parentElement;
              while (current) {
                if (current instanceof HTMLDetailsElement) {
                  current.open = true;
                }
                current = current.parentElement;
              }
              node.scrollIntoView({ block: "center" });
            }
            """
        )
        await source_tree_file.wait_for(state="visible", timeout=timeout_ms)
        await source_tree_file.click()
        detail = page.locator("[data-data-source-explorer-detail]").first
        await detail.get_by_text(alias, exact=True).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        await detail.get_by_text(f"s3://{bucket}/{key}", exact=True).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
        return
    except PlaywrightTimeoutError:
        pass

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
    await navigation.get_by_text(alias, exact=True).first.wait_for(
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
    await detail.get_by_text(alias, exact=True).first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await detail.get_by_text(f"s3://{bucket}/{key}", exact=True).first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def copy_visible_query_path(page, expected_alias: str, timeout_ms: int) -> str:
    source_tree_copy_button = page.locator(
        "[data-data-source-explorer-navigation] "
        f'[data-source-object][data-source-object-query-alias="{expected_alias}"] '
        "[data-copy-query-path]"
    ).first
    try:
        await source_tree_copy_button.wait_for(state="attached", timeout=1500)
        await source_tree_copy_button.evaluate(
            """
            (button) => {
              const menu = button.closest("details");
              if (menu instanceof HTMLDetailsElement) {
                menu.open = true;
              }
              let current = button.parentElement;
              while (current) {
                if (current instanceof HTMLDetailsElement) {
                  current.open = true;
                }
                current = current.parentElement;
              }
              button.scrollIntoView({ block: "center" });
            }
            """
        )
        await source_tree_copy_button.evaluate("(button) => button.click()")
        copied = await page.evaluate("navigator.clipboard.readText()")
        if copied != expected_alias:
            raise RuntimeError(
                f"Copy query path copied {copied!r}; expected {expected_alias!r}."
            )
        return copied
    except PlaywrightTimeoutError:
        pass

    copy_button = page.locator(
        '[data-data-source-explorer-action="copy-query-path"]'
    ).first
    await copy_button.wait_for(state="visible", timeout=timeout_ms)
    await copy_button.click()
    copied = await page.evaluate("navigator.clipboard.readText()")
    if copied != expected_alias:
        raise RuntimeError(
            f"Copy query path copied {copied!r}; expected {expected_alias!r}."
        )
    return copied


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
    await page.locator('[data-data-source-explorer-action="copy-query-path"]').first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )

    await seed_local_workspace_entry(page, base_url, timeout_ms)
    await page.goto(
        f"{base_url.rstrip('/')}/data-sources/browser?source_id=workspace.local",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator('[data-data-source-explorer-action="copy-query-path"]').first.wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def assert_late_completion_refresh_without_reload(
    page,
    args: argparse.Namespace,
    seeded_keys: list[str],
) -> tuple[str, str]:
    await ensure_query_notebook(page, args.base_url, args.timeout_ms)
    current_url = page.url
    late_csv_key, late_parquet_key, late_csv_alias, late_parquet_alias = seed_s3_objects(args)
    seeded_keys.extend([late_csv_key, late_parquet_key])
    await wait_for_aliases_in_current_completion_schema(
        page,
        [late_csv_alias, late_parquet_alias],
        args.timeout_ms,
    )
    if page.url != current_url:
        raise RuntimeError(
            "Late S3 completion schema refresh navigated the page. "
            f"Started at {current_url!r}, ended at {page.url!r}."
        )
    late_bucket_segment = normalize_query_alias_segment(args.bucket, fallback="bucket")
    await assert_completion_contains(
        page,
        f"select * from s3.{late_bucket_segment}.sam",
        late_csv_alias,
        args.timeout_ms,
    )
    return late_csv_alias, late_parquet_alias


async def run_smoke(args: argparse.Namespace) -> int:
    client = s3_client(args)
    console_messages: list[str] = []
    seeded_keys: list[str] = []
    csv_alias = ""
    parquet_alias = ""
    completion_refresh_aliases: tuple[str, str] | None = None

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
            if args.completion_refresh_only:
                completion_refresh_aliases = await assert_late_completion_refresh_without_reload(
                    page,
                    args,
                    seeded_keys,
                )
            else:
                csv_key, parquet_key, csv_alias, parquet_alias = seed_s3_objects(args)
                seeded_keys.extend([csv_key, parquet_key])
                await wait_for_aliases_in_completion_schema(
                    page,
                    args.base_url,
                    [csv_alias, parquet_alias],
                    args.timeout_ms,
                )
                await ensure_query_notebook(page, args.base_url, args.timeout_ms)
                await assert_s3_path_completions(
                    page,
                    args.bucket,
                    csv_alias,
                    parquet_alias,
                    args.timeout_ms,
                )
                await run_query_and_assert_text(
                    page,
                    f"select source_kind, amount_chf from {csv_alias} order by record_id limit 1",
                    "csv_path_completion",
                    args.timeout_ms,
                )
                await run_query_and_assert_text(
                    page,
                    f"select source_kind, amount_chf from {parquet_alias} limit 1",
                    "parquet_path_completion",
                    args.timeout_ms,
                )
                await open_s3_explorer_file(
                    page,
                    args.base_url,
                    args.bucket,
                    parquet_key,
                    parquet_alias,
                    args.timeout_ms,
                )
                copied_alias = await copy_visible_query_path(
                    page,
                    parquet_alias,
                    args.timeout_ms,
                )
                await ensure_query_notebook(page, args.base_url, args.timeout_ms)
                await run_query_and_assert_text(
                    page,
                    f"select source_kind, amount_chf from {copied_alias} limit 1",
                    "parquet_path_completion",
                    args.timeout_ms,
                )
                await assert_copy_query_path_actions_for_other_sources(
                    page,
                    args.base_url,
                    args.timeout_ms,
                )
                completion_refresh_aliases = await assert_late_completion_refresh_without_reload(
                    page,
                    args,
                    seeded_keys,
                )
        except (ClientError, PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            if csv_alias:
                print(f"CSV alias: {csv_alias}", file=sys.stderr)
            if parquet_alias:
                print(f"Parquet alias: {parquet_alias}", file=sys.stderr)
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
            for key in seeded_keys:
                with contextlib.suppress(Exception):
                    client.delete_object(Bucket=args.bucket, Key=key)

    if args.completion_refresh_only and completion_refresh_aliases:
        print(
            "Playwright SQL S3 late completion refresh regression passed for "
            f"{completion_refresh_aliases[0]} and {completion_refresh_aliases[1]}."
        )
        return 0

    print(
        "Playwright SQL S3 path completion smoke passed for "
        f"{csv_alias} and {parquet_alias}."
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

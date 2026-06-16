from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import sys
import zipfile
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
import psycopg
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise real browser ZIP CSV ingestion to S3 Object Storage and "
            "PostgreSQL. The target app and local dependencies must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="vat-smoke-test")
    parser.add_argument(
        "--pg-dsn",
        default="host=127.0.0.1 port=5432 dbname=evo1_oltp user=evo1 password=evo1",
    )
    parser.add_argument("--pg-schema", default="stage")
    parser.add_argument("--s3-smoke-prefix-root", default="playwright/csv-zip-imports")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=60000)
    return parser.parse_args()


def zip_bytes(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, payload in entries.items():
            archive.writestr(name, payload)
    return buffer.getvalue()


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


def delete_s3_prefix(client, bucket: str, prefix: str) -> None:
    normalized_prefix = "/".join(segment for segment in prefix.split("/") if segment)
    if not normalized_prefix:
        return
    list_prefix = f"{normalized_prefix}/"
    while True:
        response = client.list_objects_v2(Bucket=bucket, Prefix=list_prefix)
        objects = [{"Key": item["Key"]} for item in response.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        if not response.get("IsTruncated"):
            return


def verify_s3_objects(client, bucket: str, expected_payloads: dict[str, str]) -> None:
    for key, expected_text in expected_payloads.items():
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        if body != expected_text:
            raise RuntimeError(f"Unexpected S3 object payload for {key!r}: {body!r}")


def verify_pg_tables(pg_dsn: str, schema: str, table_payloads: dict[str, tuple[int, str]]) -> None:
    with psycopg.connect(pg_dsn) as connection:
        with connection.cursor() as cursor:
            for table, (expected_id, expected_name) in table_payloads.items():
                cursor.execute(
                    """
                    select count(*)
                    from information_schema.tables
                    where table_schema = %s and table_name = %s
                    """,
                    (schema, table),
                )
                if cursor.fetchone()[0] != 1:
                    raise RuntimeError(f"PostgreSQL ZIP import did not create {schema}.{table}.")
                cursor.execute(
                    f'select id, name from "{schema}"."{table}" order by id'
                )
                rows = cursor.fetchall()
                if rows != [(expected_id, expected_name)]:
                    raise RuntimeError(
                        f"Unexpected PostgreSQL rows for {schema}.{table}: {rows!r}"
                    )


def drop_pg_tables(pg_dsn: str, schema: str, tables: list[str]) -> None:
    if not tables:
        return
    with psycopg.connect(pg_dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            for table in tables:
                cursor.execute(f'drop table if exists "{schema}"."{table}"')


async def open_csv_ingestion(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        f"{base_url.rstrip('/')}/ingestion-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-ingestion-workbench-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )
    await page.wait_for_timeout(1000)
    csv_tile = page.locator('[data-ingestion-tile="csv"]').first
    form = page.locator("[data-csv-ingestion-form]")
    for _attempt in range(5):
        await csv_tile.click()
        try:
            await form.wait_for(state="visible", timeout=2000)
            break
        except PlaywrightTimeoutError:
            await page.wait_for_timeout(500)
    await form.wait_for(state="visible", timeout=timeout_ms)


async def assert_zip_preview(page, timeout_ms: int) -> None:
    preview = page.locator("[data-csv-preview-root] .ingestion-csv-preview-card").first
    await preview.wait_for(state="visible", timeout=timeout_ms)
    preview_text = (await preview.text_content() or "").strip()
    if "2 CSV file(s)" not in preview_text:
        raise RuntimeError(f"ZIP preview did not report two CSV files: {preview_text!r}")


async def assert_success_dialog(page, timeout_ms: int, expected_fragments: list[str]) -> None:
    dialog = page.locator("[data-message-dialog]")
    await dialog.wait_for(state="visible", timeout=timeout_ms)
    dialog_text = (await dialog.text_content() or "").strip()
    missing = [fragment for fragment in expected_fragments if fragment not in dialog_text]
    if missing:
        raise RuntimeError(f"Success dialog is missing {missing!r}: {dialog_text!r}")
    await page.locator("[data-message-submit]").click()
    await dialog.wait_for(state="hidden", timeout=timeout_ms)


async def poll_upload_session_result(
    page,
    base_url: str,
    session_id: str,
    timeout_ms: int,
) -> dict[str, object]:
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)
    session_url = f"{base_url.rstrip('/')}/api/ingestion/csv/upload-sessions/{session_id}"
    while asyncio.get_running_loop().time() < deadline:
        response = await page.context.request.get(
            session_url,
            headers={"Accept": "application/json"},
        )
        if not response.ok:
            raise RuntimeError(f"Upload session polling failed with HTTP {response.status}.")
        payload = await response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if isinstance(result, dict):
            return result
        if isinstance(payload, dict) and payload.get("status") == "failed":
            raise RuntimeError(f"Upload session failed: {payload!r}")
        await page.wait_for_timeout(500)
    raise RuntimeError("Timed out waiting for upload session import result.")


async def expect_complete_payload(page, base_url: str, timeout_ms: int, click_locator):
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and "/api/ingestion/csv/upload-sessions/" in response.url
        and response.url.endswith("/complete"),
        timeout=timeout_ms,
    ) as response_info:
        await click_locator.click()
    response = await response_info.value
    if not response.ok:
        raise RuntimeError(f"ZIP completion request failed with HTTP {response.status}.")
    payload = await response.json()
    if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
        return payload["result"]
    if isinstance(payload, dict) and "targetId" in payload:
        return payload
    session_id = str(payload.get("sessionId") or "") if isinstance(payload, dict) else ""
    if session_id:
        return await poll_upload_session_result(page, base_url, session_id, timeout_ms)
    raise RuntimeError(f"Unexpected ZIP completion response payload: {payload!r}")


async def import_zip_to_s3(
    page,
    args: argparse.Namespace,
    unique_id: str,
) -> tuple[str, dict[str, str]]:
    prefix = f"{args.s3_smoke_prefix_root.strip('/')}/{unique_id}"
    payloads = {
        "alpha.csv": "id,name\n1,alpha\n",
        "beta.csv": "id,name\n2,beta\n",
    }
    archive_payload = zip_bytes(
        {
            "alpha.csv": payloads["alpha.csv"].encode("utf-8"),
            "nested/beta.csv": payloads["beta.csv"].encode("utf-8"),
        }
    )

    await open_csv_ingestion(page, args.base_url, args.timeout_ms)
    await page.locator('[data-csv-target-option][value="s3"]').check()
    await page.locator('[data-csv-config-panel="s3"] [data-csv-s3-bucket]').fill(args.bucket)
    await page.locator('[data-csv-config-panel="s3"] [data-csv-s3-prefix]').fill(prefix)
    await page.locator('[data-csv-s3-storage-format][value="csv"]').check()
    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": "playwright-s3-csv-batch.zip",
                "mimeType": "application/zip",
                "buffer": archive_payload,
            }
        ]
    )
    await assert_zip_preview(page, args.timeout_ms)

    payload = await expect_complete_payload(
        page,
        args.base_url,
        args.timeout_ms,
        page.locator("[data-csv-import-submit]"),
    )
    if payload.get("targetId") != "s3":
        raise RuntimeError(f"Unexpected S3 target in completion payload: {payload!r}")
    if payload.get("importedCount") != 2 or payload.get("failedCount") != 0:
        raise RuntimeError(f"Unexpected S3 import counts: {payload!r}")

    expected_keys = {f"{prefix}/{file_name}": text for file_name, text in payloads.items()}
    result_list = page.locator("[data-csv-result-list]")
    await result_list.locator(".ingestion-csv-result-card-imported").nth(1).wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    result_text = (await result_list.text_content() or "").strip()
    missing = [
        fragment
        for fragment in ("alpha.csv", "beta.csv", "Key prefix", prefix)
        if fragment not in result_text
    ]
    if missing:
        raise RuntimeError(f"S3 result UI is missing {missing!r}: {result_text!r}")
    await assert_success_dialog(
        page,
        args.timeout_ms,
        ["CSV import finished", "2 file(s) processed", "S3 Object Storage"],
    )
    return prefix, expected_keys


async def import_zip_to_pg(
    page,
    args: argparse.Namespace,
    unique_id: str,
) -> tuple[str, list[str], dict[str, tuple[int, str]]]:
    table_prefix = f"pwzip_{unique_id}"
    table_payloads = {
        f"{table_prefix}_alpha": (1, "alpha"),
        f"{table_prefix}_beta": (2, "beta"),
    }
    archive_payload = zip_bytes(
        {
            "alpha.csv": b"id,name\n1,alpha\n",
            "nested/beta.csv": b"id,name\n2,beta\n",
        }
    )

    await open_csv_ingestion(page, args.base_url, args.timeout_ms)
    await page.locator('[data-csv-target-option][value="pg_oltp"]').check()
    await page.locator('[data-csv-config-panel="pg_oltp"] [data-csv-schema-name]').fill(args.pg_schema)
    await page.locator('[data-csv-config-panel="pg_oltp"] [data-csv-table-prefix]').fill(table_prefix)
    await page.locator("[data-csv-file-input]").set_input_files(
        files=[
            {
                "name": "playwright-pg-csv-batch.zip",
                "mimeType": "application/zip",
                "buffer": archive_payload,
            }
        ]
    )
    await assert_zip_preview(page, args.timeout_ms)

    payload = await expect_complete_payload(
        page,
        args.base_url,
        args.timeout_ms,
        page.locator("[data-csv-import-submit]"),
    )
    if payload.get("targetId") != "pg_oltp":
        raise RuntimeError(f"Unexpected PostgreSQL target in completion payload: {payload!r}")
    if payload.get("importedCount") != 2 or payload.get("failedCount") != 0:
        raise RuntimeError(f"Unexpected PostgreSQL import counts: {payload!r}")

    expected_relations = [f"{args.pg_schema}.{table}" for table in table_payloads]
    actual_relations = [item.get("relation") for item in payload.get("imports", [])]
    if actual_relations != expected_relations:
        raise RuntimeError(
            f"Unexpected PostgreSQL relations. Expected {expected_relations!r}, got {actual_relations!r}."
        )

    result_list = page.locator("[data-csv-result-list]")
    await result_list.locator(".ingestion-csv-result-card-imported").nth(1).wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    result_text = (await result_list.text_content() or "").strip()
    missing = [
        fragment
        for fragment in [*expected_relations, "alpha.csv", "beta.csv"]
        if fragment not in result_text
    ]
    if missing:
        raise RuntimeError(f"PostgreSQL result UI is missing {missing!r}: {result_text!r}")
    await assert_success_dialog(
        page,
        args.timeout_ms,
        ["CSV import finished", "2 file(s) processed", "PostgreSQL OLTP"],
    )
    return args.pg_schema, list(table_payloads), table_payloads


async def run_smoke(args: argparse.Namespace) -> int:
    unique_id = uuid4().hex[:8]
    client = s3_client(args)
    ensure_bucket(client, args.bucket)
    delete_s3_prefix(client, args.bucket, args.s3_smoke_prefix_root)
    s3_prefixes: list[str] = []
    pg_schema = args.pg_schema
    pg_tables: list[str] = []
    console_messages: list[str] = []
    responses: list[tuple[str, str, int]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            base_url=args.base_url.rstrip("/"),
        )
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))
        page.on(
            "response",
            lambda resp: responses.append((resp.request.method, resp.url, resp.status)),
        )

        try:
            s3_prefixes.append(f"{args.s3_smoke_prefix_root.strip('/')}/{unique_id}")
            s3_prefix, expected_s3_payloads = await import_zip_to_s3(page, args, unique_id)
            verify_s3_objects(client, args.bucket, expected_s3_payloads)

            pg_schema, pg_tables, expected_pg_payloads = await import_zip_to_pg(page, args, unique_id)
            verify_pg_tables(args.pg_dsn, pg_schema, expected_pg_payloads)
        except (ClientError, PlaywrightTimeoutError, RuntimeError, psycopg.Error) as exc:
            print(str(exc), file=sys.stderr)
            for method, url, status in responses:
                if "/api/ingestion/csv/upload-sessions" in url:
                    print(f"HTTP {method} {status} {url}", file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1
        finally:
            await browser.close()
            for prefix in s3_prefixes:
                with contextlib.suppress(Exception):
                    delete_s3_prefix(client, args.bucket, prefix)
            with contextlib.suppress(Exception):
                drop_pg_tables(args.pg_dsn, pg_schema, pg_tables)

    print(f"Playwright ZIP ingestion real backend smoke passed for id {unique_id}.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

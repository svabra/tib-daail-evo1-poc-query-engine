from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import tempfile
import time
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import duckdb
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


SOURCE_ID = "ora_bazg_zoll"
SCHEMA_NAME = "ZOLL"
RELATION_NAME = "ANMELDUNGEN"
STATE_PREFIX = "--bdw-internal--/source-ingestions/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the governed Oracle one-time/hourly S3 ingestion journey."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8010")
    parser.add_argument("--timeout-ms", type=int, default=120000)
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--artifact-dir", default="")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--s3-bucket", default="vat-smoke-test")
    return parser.parse_args()


def s3_client(args: argparse.Namespace):
    return boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def bucket_exists(client, bucket: str) -> bool:
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError as exc:
        error = exc.response.get("Error") if isinstance(exc.response, dict) else {}
        code = str((error or {}).get("Code") or "").strip()
        status = int((exc.response.get("ResponseMetadata") or {}).get("HTTPStatusCode") or 0)
        if code in {"404", "NoSuchBucket", "NotFound"} or status == 404:
            return False
        raise


async def response_json(response) -> dict[str, object]:
    text = await response.text()
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status} for {response.url}: {text}")
    payload = json.loads(text) if text else {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected an object from {response.url}, got: {payload}")
    return payload


async def select_actor(context, base_url: str, actor: str) -> None:
    response = await context.request.post(
        f"{base_url}/api/ingestion/sourcing/identity",
        data={"userId": actor},
        headers={"Content-Type": "application/json"},
    )
    await response_json(response)


async def wait_for_run_count(
    context,
    base_url: str,
    definition_id: str,
    expected_count: int,
    timeout_ms: int,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_ms / 1000
    latest: dict[str, object] = {}
    while time.monotonic() < deadline:
        response = await context.request.get(
            f"{base_url}/api/ingestion/source-ingestions/{definition_id}"
        )
        latest = await response_json(response)
        runs = latest.get("runs") if isinstance(latest.get("runs"), list) else []
        terminal = [run for run in runs if run.get("status") in {"completed", "failed", "blocked", "skipped"}]
        if len(terminal) >= expected_count:
            failures = [run for run in terminal[:expected_count] if run.get("status") != "completed"]
            if failures:
                raise RuntimeError(f"Oracle source ingestion failed: {failures}")
            return latest
        await asyncio.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {expected_count} completed run(s): {latest}")


async def assert_no_horizontal_overflow(page) -> None:
    overflow = await page.evaluate(
        "() => Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) "
        "- document.documentElement.clientWidth"
    )
    if overflow > 1:
        raise RuntimeError(f"Page has {overflow}px of horizontal overflow: {page.url}")


def parquet_row_count(client, bucket: str, key: str) -> tuple[int, int]:
    payload = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    with tempfile.TemporaryDirectory(prefix="bdw-oracle-ingestion-") as directory:
        path = Path(directory) / "snapshot.parquet"
        path.write_bytes(payload)
        connection = duckdb.connect(":memory:")
        try:
            rows = int(
                connection.execute(
                    "SELECT COUNT(*) FROM read_parquet(?)", [str(path)]
                ).fetchone()[0]
            )
            columns = int(
                connection.execute(
                    "SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_parquet(?))",
                    [str(path)],
                ).fetchone()[0]
            )
        finally:
            connection.close()
    return rows, columns


async def create_scheduled_ingestion(
    page,
    client,
    base_url: str,
    bucket: str,
    target_key: str,
    timeout_ms: int,
) -> tuple[dict[str, object], float]:
    await page.goto(
        f"{base_url}/ingestion-workbench/sourcing",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    hub = page.locator("[data-source-ingestion-hub]")
    await hub.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        "document.querySelector('[data-source-ingestion-hub]')?.dataset.sourceIngestionInitialized === 'true'",
        timeout=timeout_ms,
    )
    await assert_no_horizontal_overflow(page)
    source_link = hub.locator(
        'a[href="/ingestion-workbench/sourcing/ingestions/new?sourceId=ora_bazg_zoll"]'
    ).first
    await source_link.wait_for(state="attached", timeout=timeout_ms)
    await source_link.click()

    wizard = page.locator("[data-source-ingestion-wizard]")
    await wizard.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        "document.querySelector('[data-source-ingestion-wizard]')?.dataset.sourceIngestionInitialized === 'true'",
        timeout=timeout_ms,
    )
    await wizard.locator("[data-source-ingestion-source]").select_option(SOURCE_ID)
    await wizard.locator("[data-source-ingestion-relation]").select_option(
        f"{SCHEMA_NAME}.{RELATION_NAME}"
    )
    await wizard.locator('[data-source-ingestion-step="1"] [data-source-ingestion-next]').click()
    await wizard.locator("[data-source-ingestion-bucket]").fill(bucket)
    await wizard.locator("[data-source-ingestion-key]").fill(target_key)
    create_bucket_button = wizard.locator("[data-source-ingestion-create-bucket]")
    await create_bucket_button.wait_for(state="visible", timeout=timeout_ms)
    bucket_create_started = time.monotonic()
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/s3/explorer/buckets"),
        timeout=timeout_ms,
    ) as bucket_response_info:
        await create_bucket_button.click()
    bucket_response = await bucket_response_info.value
    bucket_payload = await response_json(bucket_response)
    bucket_create_ms = (time.monotonic() - bucket_create_started) * 1000
    if bucket_payload.get("bucket") != bucket:
        raise RuntimeError(f"Unexpected created bucket response: {bucket_payload}")
    await wizard.locator(
        "[data-source-ingestion-bucket-status]",
        has_text="is ready and selected.",
    ).wait_for(timeout=timeout_ms)
    if not bucket_exists(client, bucket):
        raise RuntimeError(f"Sourcing UI reported success but S3 bucket does not exist: {bucket}")
    client.put_object(Bucket=bucket, Key=target_key, Body=b"OLD-COMPLETE-SNAPSHOT")
    await wizard.locator('[data-source-ingestion-step="2"] [data-source-ingestion-next]').click()
    await wizard.locator('[data-source-ingestion-mode][value="scheduled"]').check()
    await wizard.locator("[data-source-ingestion-schedule-contract]").wait_for(
        state="visible", timeout=timeout_ms
    )
    await wizard.locator('[data-source-ingestion-step="3"] [data-source-ingestion-next]').click()
    await wizard.locator("[data-source-ingestion-name]").fill(
        "BAZG Zollanmeldungen – hourly full refresh"
    )
    await wizard.locator("[data-source-ingestion-confirm]").check()
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/ingestion/source-ingestions"),
        timeout=timeout_ms,
    ) as response_info:
        await wizard.locator("[data-source-ingestion-submit]").click()
    response = await response_info.value
    if not response.ok:
        raise RuntimeError(
            f"Source-ingestion creation failed with HTTP {response.status}."
        )
    await page.wait_for_url(
        f"{base_url}/ingestion-workbench/sourcing/ingestions/source-ingestion-*",
        timeout=timeout_ms,
    )
    definition_id = page.url.rsplit("/", 1)[-1]
    created = await response_json(
        await page.context.request.get(
            f"{base_url}/api/ingestion/source-ingestions/{definition_id}"
        )
    )
    return created, bucket_create_ms


async def verify_detail_and_second_run(
    page,
    context,
    base_url: str,
    definition_id: str,
    timeout_ms: int,
) -> dict[str, object]:
    first = await wait_for_run_count(context, base_url, definition_id, 1, timeout_ms)
    detail = page.locator("[data-source-ingestion-detail]")
    await detail.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        "() => document.querySelectorAll('[data-source-ingestion-runs] .source-ingestion-run-card h4').length >= 1 "
        "&& document.querySelector('[data-source-ingestion-runs] .source-ingestion-run-card h4')?.textContent.trim() === 'completed'",
        timeout=timeout_ms,
    )
    await page.wait_for_function(
        "() => document.querySelector('[data-source-ingestion-detail-status]')?.textContent.trim() "
        "=== 'Hourly schedule active'",
        timeout=timeout_ms,
    )
    if "4" not in await detail.locator("[data-source-ingestion-detail-summary]").inner_text():
        raise RuntimeError("The detail receipt does not show the four ingested Oracle rows.")

    await detail.locator("[data-source-ingestion-run-now]").click()
    second = await wait_for_run_count(context, base_url, definition_id, 2, timeout_ms)
    await page.wait_for_function(
        "() => Array.from(document.querySelectorAll('[data-source-ingestion-runs] .source-ingestion-run-card h4'))"
        ".filter(node => node.textContent.trim() === 'completed').length >= 2",
        timeout=timeout_ms,
    )
    await detail.locator('[data-source-ingestion-detail-mode][value="once"]').check()
    await page.wait_for_function(
        "() => document.querySelector('[data-source-ingestion-detail-status]')?.textContent.trim() === 'One-Time'",
        timeout=timeout_ms,
    )
    final = await response_json(
        await context.request.get(
            f"{base_url}/api/ingestion/source-ingestions/{definition_id}"
        )
    )
    definition = final.get("definition") if isinstance(final.get("definition"), dict) else {}
    if definition.get("schedule", {}).get("enabled") is not False or definition.get("nextRunAt"):
        raise RuntimeError(f"Switching to One-Time left a schedule active: {definition}")
    if len(final.get("runs", [])) != len(second.get("runs", [])):
        raise RuntimeError("Switching run mode unexpectedly changed the run history.")
    return final


async def verify_noemie_is_isolated(context, base_url: str, definition_id: str) -> None:
    await select_actor(context, base_url, "noemie.rochat")
    listed = await response_json(
        await context.request.get(f"{base_url}/api/ingestion/source-ingestions")
    )
    if listed.get("items"):
        raise RuntimeError(f"Noémie could see Joel's source ingestion definitions: {listed}")
    direct = await context.request.get(
        f"{base_url}/api/ingestion/source-ingestions/{definition_id}"
    )
    if direct.status != 404:
        raise RuntimeError(f"Noémie could open Joel's source ingestion: HTTP {direct.status}")


def cleanup_artifacts(
    client,
    state_bucket: str,
    destination_bucket: str,
    target_key: str,
    client_request_id: str,
) -> list[str]:
    definitions: list[dict[str, object]] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=state_bucket, Prefix=f"{STATE_PREFIX}definitions/"
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            payload = json.loads(
                client.get_object(Bucket=state_bucket, Key=key)["Body"].read()
            )
            if payload.get("clientRequestId") == client_request_id:
                payload["storeKey"] = key
                definitions.append(payload)
    definition_ids = {str(item["id"]) for item in definitions}
    state_keys = [str(item["storeKey"]) for item in definitions]
    for prefix in (f"{STATE_PREFIX}runs/", f"{STATE_PREFIX}staging/"):
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=state_bucket, Prefix=prefix
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                if prefix.endswith("staging/"):
                    if any(f"/{definition_id}/" in key for definition_id in definition_ids):
                        state_keys.append(key)
                    continue
                payload = json.loads(
                    client.get_object(Bucket=state_bucket, Key=key)["Body"].read()
                )
                if str(payload.get("definitionId") or "") in definition_ids:
                    state_keys.append(key)
    deleted_paths: list[str] = []
    unique_state_keys = sorted(set(state_keys))
    if unique_state_keys:
        client.delete_objects(
            Bucket=state_bucket,
            Delete={"Objects": [{"Key": key} for key in unique_state_keys]},
        )
        deleted_paths.extend(f"s3://{state_bucket}/{key}" for key in unique_state_keys)

    if bucket_exists(client, destination_bucket):
        client.delete_object(Bucket=destination_bucket, Key=target_key)
        deleted_paths.append(f"s3://{destination_bucket}/{target_key}")
        remaining = client.list_objects_v2(Bucket=destination_bucket).get("Contents", [])
        if remaining:
            raise RuntimeError(
                f"Refusing to remove non-empty test bucket {destination_bucket}: {remaining}"
            )
        client.delete_bucket(Bucket=destination_bucket)
    return deleted_paths


async def run_smoke(args: argparse.Namespace) -> int:
    base_url = args.base_url.rstrip("/")
    token = uuid.uuid4().hex
    client_request_id = f"oracle-ingestion-playwright:{token}"
    destination_bucket = f"pw-source-ingestion-{token[:24]}"
    target_key = f"codex-regression/source-ingestions/{token}/anmeldungen.parquet"
    artifacts = Path(args.artifact_dir).resolve() if args.artifact_dir else None
    if artifacts:
        artifacts.mkdir(parents=True, exist_ok=True)
    client = s3_client(args)
    if bucket_exists(client, destination_bucket):
        raise RuntimeError(f"Unique destination bucket unexpectedly exists: {destination_bucket}")
    definition_id = ""
    final: dict[str, object] = {}
    bucket_create_ms = 0.0
    diagnostics: list[str] = []
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=args.headless)
            context = await browser.new_context(viewport={"width": 1600, "height": 1100})
            await context.add_init_script("localStorage.setItem('daaif-demo-user', 'joel.ruod')")
            page = await context.new_page()
            page.on("console", lambda message: diagnostics.append(f"console:{message.type}:{message.text}"))
            page.on("pageerror", lambda error: diagnostics.append(f"pageerror:{error}"))
            try:
                await select_actor(context, base_url, "joel.ruod")
                created, bucket_create_ms = await create_scheduled_ingestion(
                    page,
                    client,
                    base_url,
                    destination_bucket,
                    target_key,
                    args.timeout_ms,
                )
                definition = created.get("definition") if isinstance(created.get("definition"), dict) else {}
                definition_id = str(definition.get("id") or "")
                if not definition_id or definition.get("clientRequestId") != client_request_id:
                    # The UI owns the real idempotency token. Keep it for deterministic cleanup.
                    client_request_id = str(definition.get("clientRequestId") or client_request_id)
                final = await verify_detail_and_second_run(
                    page, context, base_url, definition_id, args.timeout_ms
                )
                rows, columns = parquet_row_count(client, destination_bucket, target_key)
                if rows != 4 or columns < 1:
                    raise RuntimeError(
                        f"The promoted Parquet snapshot has rows={rows}, columns={columns}; expected 4 rows."
                    )
                staging = client.list_objects_v2(
                    Bucket=args.s3_bucket,
                    Prefix=f"{STATE_PREFIX}staging/{definition_id}/",
                ).get("Contents", [])
                if staging:
                    raise RuntimeError(
                        f"Completed source ingestion left staging objects behind: {staging}"
                    )
                if artifacts:
                    await page.screenshot(
                        path=artifacts / "oracle-source-ingestion-detail-desktop.png",
                        full_page=True,
                    )
                await page.set_viewport_size({"width": 390, "height": 844})
                await assert_no_horizontal_overflow(page)
                if artifacts:
                    await page.screenshot(
                        path=artifacts / "oracle-source-ingestion-detail-mobile.png",
                        full_page=True,
                    )
                await verify_noemie_is_isolated(context, base_url, definition_id)
            finally:
                await context.close()
                await browser.close()
    except (PlaywrightTimeoutError, RuntimeError, KeyError, ValueError) as exc:
        print(str(exc), flush=True)
        for diagnostic in diagnostics[-40:]:
            print(diagnostic, flush=True)
        return 1
    finally:
        deleted = cleanup_artifacts(
            client,
            args.s3_bucket,
            destination_bucket,
            target_key,
            client_request_id,
        )
        print(f"Cleaned {len(deleted)} exact source-ingestion test object(s).", flush=True)
    runs = final.get("runs") if isinstance(final.get("runs"), list) else []
    print(
        "Oracle source-ingestion journey passed: "
        f"definition={definition_id} completedRuns={len(runs)} rows=4 mode=one-time "
        f"bucketCreateMs={bucket_create_ms:.0f}",
        flush=True,
    )
    return 0


def main() -> int:
    return asyncio.run(run_smoke(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

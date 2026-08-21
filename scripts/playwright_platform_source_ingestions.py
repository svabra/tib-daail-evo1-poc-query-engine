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
import duckdb
from playwright.async_api import async_playwright


STATE_PREFIX = "--bdw-internal--/source-ingestions/"
CASES = (
    {
        "sourceId": "pg_oltp",
        "schema": "public",
        "relation": "vat_smoke_test_reference",
        "sourceKind": "postgresql",
        "technology": "postgresql",
        "expectedRows": 3,
    },
    {
        "sourceId": "s3",
        "schema": "vat_smoke_test_e93f1988",
        "relation": "vat_context_bootstrap",
        "sourceKind": "s3-object",
        "technology": "s3",
        "expectedRows": 12,
    },
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PostgreSQL and S3 source-to-Parquet ingestion smokes."
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


async def response_json(response) -> dict[str, object]:
    text = await response.text()
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status} for {response.url}: {text}")
    payload = json.loads(text) if text else {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object from {response.url}: {payload}")
    return payload


async def select_actor(context, base_url: str, actor: str) -> None:
    response = await context.request.post(
        f"{base_url}/api/ingestion/sourcing/identity",
        data={"userId": actor},
        headers={"Content-Type": "application/json"},
    )
    await response_json(response)


async def wait_for_terminal(
    context, base_url: str, definition_id: str, timeout_ms: int
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_ms / 1000
    latest: dict[str, object] = {}
    while time.monotonic() < deadline:
        latest = await response_json(
            await context.request.get(
                f"{base_url}/api/ingestion/source-ingestions/{definition_id}"
            )
        )
        runs = latest.get("runs") if isinstance(latest.get("runs"), list) else []
        if runs and runs[0].get("status") in {
            "completed",
            "failed",
            "blocked",
            "skipped",
        }:
            if runs[0].get("status") != "completed":
                raise RuntimeError(f"Source ingestion did not complete: {runs[0]}")
            return latest
        await asyncio.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for source ingestion: {latest}")


def parquet_shape(client, bucket: str, key: str) -> tuple[int, int]:
    payload = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    with tempfile.TemporaryDirectory(prefix="bdw-platform-source-ingestion-") as directory:
        path = Path(directory) / "snapshot.parquet"
        path.write_bytes(payload)
        connection = duckdb.connect(":memory:")
        try:
            rows = int(
                connection.execute(
                    "SELECT COUNT(*) FROM read_parquet(?)", [str(path)]
                ).fetchone()[0]
            )
            columns = len(
                connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
                ).fetchall()
            )
        finally:
            connection.close()
    return rows, columns


async def assert_no_horizontal_overflow(page) -> None:
    overflow = await page.evaluate(
        "() => Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) "
        "- document.documentElement.clientWidth"
    )
    if overflow > 1:
        raise RuntimeError(f"Page has {overflow}px of horizontal overflow: {page.url}")


async def create_ingestion(
    page,
    *,
    base_url: str,
    bucket: str,
    target_key: str,
    case: dict[str, object],
    timeout_ms: int,
) -> dict[str, object]:
    source_id = str(case["sourceId"])
    relation = f"{case['schema']}.{case['relation']}"
    await page.goto(
        f"{base_url}/ingestion-workbench/sourcing/ingestions/new?sourceId={source_id}",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    wizard = page.locator("[data-source-ingestion-wizard]")
    await wizard.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        "document.querySelector('[data-source-ingestion-wizard]')?.dataset.sourceIngestionInitialized === 'true'",
        timeout=timeout_ms,
    )
    await wizard.locator("[data-source-ingestion-source]").select_option(source_id)
    await wizard.locator("[data-source-ingestion-relation]").select_option(relation)
    await wizard.locator(
        '[data-source-ingestion-step="1"] [data-source-ingestion-next]'
    ).click()
    suggested = await wizard.locator("[data-source-ingestion-key]").input_value()
    expected_prefix = f"ingestions/{case['technology']}/{source_id}/"
    if not suggested.startswith(expected_prefix):
        raise RuntimeError(
            f"Unexpected technology-neutral target for {source_id}: {suggested}"
        )
    await wizard.locator("[data-source-ingestion-bucket]").fill(bucket)
    await wizard.locator("[data-source-ingestion-key]").fill(target_key)
    await wizard.locator(
        '[data-source-ingestion-step="2"] [data-source-ingestion-next]'
    ).click()
    await wizard.locator('[data-source-ingestion-mode][value="once"]').check()
    await wizard.locator(
        '[data-source-ingestion-step="3"] [data-source-ingestion-next]'
    ).click()
    await wizard.locator("[data-source-ingestion-name]").fill(
        f"{source_id} Playwright full refresh"
    )
    await wizard.locator("[data-source-ingestion-confirm]").check()
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/ingestion/source-ingestions"),
        timeout=timeout_ms,
    ) as response_info:
        await wizard.locator("[data-source-ingestion-submit]").click()
    response = await response_info.value
    request_payload = response.request.post_data_json
    forbidden = {"actorId", "actor", "sql", "sourceKind"}.intersection(request_payload)
    if forbidden:
        raise RuntimeError(f"Client submitted trusted server fields: {sorted(forbidden)}")
    if not response.ok:
        raise RuntimeError(f"Source-ingestion creation failed with HTTP {response.status}.")
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
    definition = created.get("definition")
    if not isinstance(definition, dict):
        raise RuntimeError(f"Source-ingestion creation returned no definition: {created}")
    return definition


def cleanup_definition(client, bucket: str, target_key: str, final: dict[str, object]) -> None:
    definition = final.get("definition") if isinstance(final.get("definition"), dict) else {}
    definition_id = str(definition.get("id") or "")
    keys = [target_key]
    if not definition_id:
        client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": target_key}]}
        )
        return
    keys.append(f"{STATE_PREFIX}definitions/{definition_id}.json")
    runs = final.get("runs") if isinstance(final.get("runs"), list) else []
    keys.extend(str(run.get("storeKey") or "") for run in runs if run.get("storeKey"))
    staging = client.list_objects_v2(
        Bucket=bucket, Prefix=f"{STATE_PREFIX}staging/{definition_id}/"
    ).get("Contents", [])
    keys.extend(str(item.get("Key") or "") for item in staging)
    unique = sorted({key for key in keys if key})
    if unique:
        client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": key} for key in unique]}
        )


async def run_smoke(args: argparse.Namespace) -> int:
    base_url = args.base_url.rstrip("/")
    client = s3_client(args)
    artifacts = Path(args.artifact_dir).resolve() if args.artifact_dir else None
    if artifacts:
        artifacts.mkdir(parents=True, exist_ok=True)
    diagnostics: list[str] = []
    completed: list[str] = []
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=args.headless)
            context = await browser.new_context(viewport={"width": 1440, "height": 1000})
            page = await context.new_page()
            page.on(
                "console",
                lambda message: diagnostics.append(
                    f"console:{message.type}:{message.text}"
                ),
            )
            page.on("pageerror", lambda error: diagnostics.append(f"pageerror:{error}"))
            await select_actor(context, base_url, "joel.ruod")
            for case in CASES:
                token = uuid.uuid4().hex
                source_id = str(case["sourceId"])
                target_key = (
                    f"codex-regression/platform-source-ingestions/{token}/"
                    f"{source_id}.parquet"
                )
                client.put_object(
                    Bucket=args.s3_bucket,
                    Key=target_key,
                    Body=b"OLD-COMPLETE-SNAPSHOT",
                )
                final: dict[str, object] = {}
                try:
                    definition = await create_ingestion(
                        page,
                        base_url=base_url,
                        bucket=args.s3_bucket,
                        target_key=target_key,
                        case=case,
                        timeout_ms=args.timeout_ms,
                    )
                    definition_id = str(definition.get("id") or "")
                    final = {"definition": definition, "runs": []}
                    final = await wait_for_terminal(
                        context, base_url, definition_id, args.timeout_ms
                    )
                    persisted = final.get("definition")
                    if not isinstance(persisted, dict):
                        raise RuntimeError(f"Missing persisted definition: {final}")
                    if persisted.get("sourceKind") != case["sourceKind"]:
                        raise RuntimeError(f"Unexpected source kind: {persisted}")
                    rows, columns = parquet_shape(client, args.s3_bucket, target_key)
                    if rows != case["expectedRows"] or columns < 1:
                        raise RuntimeError(
                            f"Unexpected {source_id} Parquet shape: rows={rows}, columns={columns}"
                        )
                    await assert_no_horizontal_overflow(page)
                    if artifacts:
                        await page.screenshot(
                            path=artifacts / f"{source_id}-source-ingestion.png",
                            full_page=True,
                        )
                    completed.append(f"{source_id}:{rows}")
                finally:
                    cleanup_definition(client, args.s3_bucket, target_key, final)
            await context.close()
            await browser.close()
    except Exception as exc:  # noqa: BLE001 - smoke diagnostics must be printed
        print(str(exc), flush=True)
        for diagnostic in diagnostics[-40:]:
            print(diagnostic, flush=True)
        return 1
    print(
        "Platform source-ingestion journeys passed: " + ", ".join(completed),
        flush=True,
    )
    return 0


def main() -> int:
    return asyncio.run(run_smoke(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

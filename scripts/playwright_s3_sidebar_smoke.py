from __future__ import annotations

import argparse
import asyncio
import contextlib
import sys
import time
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the sidebar S3 bucket create/delete flow in the "
            "browser using Playwright. "
            "The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


def unique_bucket_name(prefix: str) -> str:
    suffix = uuid4().hex[:10]
    return f"{prefix}-{suffix}"


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
        client.delete_bucket(Bucket=bucket_name)


def seed_versioned_bucket(client, bucket_name: str) -> None:
    purge_bucket(client, bucket_name)
    client.create_bucket(Bucket=bucket_name)
    client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    client.put_object(Bucket=bucket_name, Key="folder/item.txt", Body=b"first")
    client.put_object(
        Bucket=bucket_name,
        Key="folder/item.txt",
        Body=b"second",
    )
    client.delete_object(Bucket=bucket_name, Key="folder/item.txt")


async def ensure_details_open(page, selector: str) -> None:
    locator = page.locator(selector)
    await locator.wait_for(state="attached")
    if not await locator.evaluate("node => node.hasAttribute('open')"):
        await locator.locator(":scope > summary").click()


async def wait_for_bucket_summary(page, bucket_name: str, timeout_ms: int):
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )
    summary = page.locator(
        f'[data-source-schema][data-source-bucket="{bucket_name}"] > summary'
    )
    await summary.wait_for(state="visible", timeout=timeout_ms)
    return summary


async def create_bucket_via_sidebar(
    page,
    bucket_name: str,
    timeout_ms: int,
) -> float:
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )
    await page.locator("[data-create-source-bucket]").click()
    await page.locator("[data-folder-name-input]").fill(bucket_name)
    await page.locator("[data-folder-name-submit]").click()
    await page.locator("[data-confirm-submit]").click()

    started = time.perf_counter()
    await (
        page.locator("[data-source-operation-status-title]")
        .filter(has_text="Bucket created")
        .wait_for(timeout=timeout_ms)
    )
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )
    await wait_for_bucket_summary(page, bucket_name, timeout_ms)
    return (time.perf_counter() - started) * 1000


async def delete_bucket_via_sidebar(
    page,
    bucket_name: str,
    timeout_ms: int,
) -> float:
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )
    summary = await wait_for_bucket_summary(page, bucket_name, timeout_ms)
    await summary.hover()
    bucket_root = page.locator(
        f'[data-source-schema][data-source-bucket="{bucket_name}"]'
    )
    await bucket_root.locator("[data-source-action-menu-toggle]").click()
    await bucket_root.locator("[data-delete-source-s3-bucket]").click()
    option = page.locator("[data-confirm-option-input]")
    if await option.is_visible():
        await option.check()
    await page.locator("[data-confirm-submit]").click()

    started = time.perf_counter()
    await (
        page.locator("[data-source-operation-status-title]")
        .filter(has_text="Bucket deleted")
        .wait_for(timeout=timeout_ms)
    )
    await page.locator(
        f'[data-source-schema][data-source-bucket="{bucket_name}"] > summary'
    ).wait_for(state="detached", timeout=timeout_ms)
    return (time.perf_counter() - started) * 1000


def bucket_exists(client, bucket_name: str) -> bool:
    try:
        client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False


async def run_smoke(args: argparse.Namespace) -> int:
    created_bucket = unique_bucket_name("pw-sidebar-create")
    versioned_bucket = unique_bucket_name("pw-sidebar-versioned-delete")
    client = s3_client(args)

    purge_bucket(client, created_bucket)
    seed_versioned_bucket(client, versioned_bucket)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1440, "height": 1200})
        console_messages: list[str] = []
        page.on(
            "console",
            lambda msg: console_messages.append(
                f"console:{msg.type}:{msg.text}"
            ),
        )
        page.on(
            "pageerror",
            lambda exc: console_messages.append(f"pageerror:{exc}"),
        )
        responses: list[tuple[str, str, int]] = []
        page.on(
            "response",
            lambda resp: responses.append(
                (resp.request.method, resp.url, resp.status)
            ),
        )

        try:
            await page.goto(
                args.base_url,
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            create_ms = await create_bucket_via_sidebar(
                page,
                created_bucket,
                args.timeout_ms,
            )
            delete_ms = await delete_bucket_via_sidebar(
                page,
                created_bucket,
                args.timeout_ms,
            )
            versioned_delete_ms = await delete_bucket_via_sidebar(
                page,
                versioned_bucket,
                args.timeout_ms,
            )
        except PlaywrightTimeoutError as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    failures: list[str] = []
    if bucket_exists(client, created_bucket):
        failures.append(
            "Created bucket still exists after sidebar delete: "
            f"{created_bucket}"
        )
    if bucket_exists(client, versioned_bucket):
        failures.append(
            "Versioned bucket still exists after recursive sidebar delete: "
            f"{versioned_bucket}"
        )

    for method, url, status in responses:
        if "/api/s3/" in url:
            print(f"HTTP {method} {status} {url}")

    print(f"Sidebar create bucket: {create_ms:.0f} ms")
    print(f"Sidebar delete empty bucket: {delete_ms:.0f} ms")
    print(f"Sidebar delete versioned bucket: {versioned_delete_ms:.0f} ms")

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    print("Playwright S3 sidebar smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

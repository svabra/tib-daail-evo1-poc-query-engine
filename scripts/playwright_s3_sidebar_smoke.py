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


def seed_csv_bucket(client, bucket_name: str) -> None:
    purge_bucket(client, bucket_name)
    client.create_bucket(Bucket=bucket_name)
    client.put_object(
        Bucket=bucket_name,
        Key="samples/data.csv",
        Body=b"id,name\n1,Ada\n",
        ContentType="text/csv",
    )


async def ensure_details_open(page, selector: str) -> None:
    await page.locator(selector).first.wait_for(state="attached")
    await page.evaluate(
        """(selector) => {
            for (const node of document.querySelectorAll(selector)) {
                node.setAttribute("open", "");
            }
        }""",
        selector,
    )


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


async def wait_for_bucket_summary(page, bucket_name: str, timeout_ms: int):
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )
    summary = page.locator(
        f'[data-source-schema][data-source-bucket="{bucket_name}"] > summary'
    ).first
    await summary.wait_for(state="attached", timeout=timeout_ms)
    await summary.evaluate(
        """(node) => {
            let current = node.parentElement;
            while (current) {
                if (current instanceof HTMLDetailsElement) {
                    current.open = true;
                    current.setAttribute("open", "");
                }
                current = current.parentElement;
            }
        }"""
    )
    return summary


async def click_create_bucket_button(page, timeout_ms: int) -> None:
    await page.locator(
        '[data-source-catalog][data-source-catalog-name="workspace"] [data-create-source-bucket]'
    ).first.wait_for(state="attached", timeout=timeout_ms)
    clicked = await page.evaluate(
        """() => {
            const isVisible = (node) => Boolean(node && (
                node.offsetWidth || node.offsetHeight || node.getClientRects().length
            ));
            const catalogs = Array.from(document.querySelectorAll(
                '[data-source-catalog][data-source-catalog-name="workspace"]'
            ));
            const catalog = catalogs.find(isVisible) || catalogs[0];
            const button = catalog?.querySelector("[data-create-source-bucket]");
            if (!button) {
                return false;
            }
            button.click();
            return true;
        }"""
    )
    if not clicked:
        raise RuntimeError("Could not click the Shared Workspace bucket create button.")


async def submit_confirm_if_open(page, timeout_ms: int, *, wait_ms: int = 5000) -> bool:
    confirm_dialog = page.locator("[data-confirm-dialog][open]").first
    try:
        await confirm_dialog.wait_for(state="visible", timeout=min(timeout_ms, wait_ms))
    except PlaywrightTimeoutError:
        return False

    option = confirm_dialog.locator("[data-confirm-option-input]")
    if await option.is_visible():
        await option.check()
    await confirm_dialog.locator("[data-confirm-submit]").click(force=True)
    return True


async def wait_for_sidebar_status(page, title: str, timeout_ms: int) -> None:
    await page.wait_for_function(
        """(expectedTitle) => {
            return Array.from(document.querySelectorAll("[data-source-operation-status-title]"))
                .some((node) => node.textContent.trim() === expectedTitle);
        }""",
        arg=title,
        timeout=timeout_ms,
    )


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
    await click_create_bucket_button(page, timeout_ms)
    folder_dialog = page.locator("[data-folder-name-dialog][open]").first
    await folder_dialog.wait_for(state="visible", timeout=timeout_ms)
    await folder_dialog.locator("[data-folder-name-input]").fill(bucket_name)
    await folder_dialog.locator("[data-folder-name-submit]").click()
    await page.wait_for_timeout(250)
    confirm_dialog = page.locator("[data-confirm-dialog]")
    if await confirm_dialog.count() and await confirm_dialog.evaluate(
        "node => Boolean(node.open)"
    ):
        raise AssertionError(
            "Sidebar bucket creation opened a second confirmation prompt."
        )

    started = time.perf_counter()
    await wait_for_sidebar_status(page, "Bucket created", timeout_ms)
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )
    await wait_for_bucket_summary(page, bucket_name, timeout_ms)
    return (time.perf_counter() - started) * 1000


async def reject_invalid_bucket_via_sidebar(
    page,
    bucket_name: str,
    timeout_ms: int,
    bucket_create_requests: list[str],
) -> None:
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )

    request_count_before = len(bucket_create_requests)
    await click_create_bucket_button(page, timeout_ms)
    folder_dialog = page.locator("[data-folder-name-dialog][open]").first
    await folder_dialog.wait_for(state="visible", timeout=timeout_ms)
    await folder_dialog.locator("[data-folder-name-input]").fill(bucket_name)
    await folder_dialog.locator("[data-folder-name-submit]").click()
    await page.wait_for_timeout(250)

    confirm_dialog = page.locator("[data-confirm-dialog]")
    if await confirm_dialog.count() and await confirm_dialog.evaluate(
        "node => Boolean(node.open)"
    ):
        raise AssertionError(
            "Invalid sidebar bucket creation opened a second confirmation prompt."
        )

    message_dialog = page.locator("[data-message-dialog][open]")
    await message_dialog.wait_for(state="visible", timeout=timeout_ms)
    message_copy = await message_dialog.locator("[data-message-copy]").inner_text()
    expected_message = "lowercase letters, numbers, dots, or hyphens"
    if expected_message not in message_copy:
        raise AssertionError(
            "Invalid sidebar bucket creation showed an unexpected message: "
            f"{message_copy!r}"
        )

    await message_dialog.locator("[data-message-submit]").evaluate("node => node.click()")
    await page.wait_for_timeout(100)
    await page.evaluate(
        """() => {
            const dialog = document.querySelector("[data-message-dialog][open]");
            if (dialog && typeof dialog.close === "function") {
                dialog.close("confirm");
            }
        }"""
    )
    await page.wait_for_function(
        "() => !document.querySelector('[data-message-dialog][open]')",
        timeout=timeout_ms,
    )

    if len(bucket_create_requests) != request_count_before:
        raise AssertionError(
            "Invalid sidebar bucket creation sent a bucket-create request."
        )


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
    await wait_for_bucket_summary(page, bucket_name, timeout_ms)
    bucket_root = page.locator(
        f'[data-source-schema][data-source-bucket="{bucket_name}"]'
    ).first
    await bucket_root.locator(":scope > summary [data-delete-source-s3-bucket]").evaluate(
        "node => node.click()"
    )
    await submit_confirm_if_open(page, timeout_ms)

    started = time.perf_counter()
    await wait_for_sidebar_status(page, "Bucket deleted", timeout_ms)
    await page.locator(
        f'[data-source-schema][data-source-bucket="{bucket_name}"]:visible > summary'
    ).wait_for(state="detached", timeout=timeout_ms)
    return (time.perf_counter() - started) * 1000


async def delete_bucket_with_schema_bucket_fallback(
    page,
    bucket_name: str,
    timeout_ms: int,
) -> float:
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )
    await wait_for_bucket_summary(page, bucket_name, timeout_ms)
    bucket_root = page.locator(
        f'[data-source-schema][data-source-bucket="{bucket_name}"]'
    ).first
    schema_key = await bucket_root.get_attribute("data-source-schema-key")
    if not schema_key:
        raise AssertionError(f"Missing sidebar schema key for bucket {bucket_name}")

    await bucket_root.evaluate(
        """(node, bucketName) => {
            const schemaKey = node.dataset.sourceSchemaKey || "";
            const parts = schemaKey.split("::").filter(Boolean);
            node.dataset.sourceBucket = parts[parts.length - 1] || "invalid_bucket_name";

            let objectList = node.querySelector(".source-object-list");
            if (!objectList) {
                objectList = document.createElement("ul");
                objectList.className = "source-object-list";
                node.appendChild(objectList);
            }

            const sourceObject = document.createElement("li");
            sourceObject.className = "source-object";
            sourceObject.dataset.sourceObject = "";
            sourceObject.dataset.s3Bucket = bucketName;
            sourceObject.dataset.s3Key = "samples/data.csv";
            sourceObject.dataset.s3Path = `s3://${bucketName}/samples/data.csv`;
            sourceObject.dataset.s3Downloadable = "true";
            objectList.appendChild(sourceObject);
        }""",
        bucket_name,
    )

    schema_root = page.locator(
        f'[data-source-schema][data-source-schema-key="{schema_key}"]'
    ).first
    await schema_root.locator(":scope > summary [data-delete-source-s3-bucket]").evaluate(
        "node => node.click()"
    )
    await submit_confirm_if_open(page, timeout_ms)

    started = time.perf_counter()
    await wait_for_sidebar_status(page, "Bucket deleted", timeout_ms)
    await schema_root.wait_for(state="detached", timeout=timeout_ms)
    return (time.perf_counter() - started) * 1000


def bucket_exists(client, bucket_name: str) -> bool:
    try:
        client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False


def object_exists(client, bucket_name: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError:
        return False


async def delete_object_via_sidebar_shows_pending_strike(
    page,
    bucket_name: str,
    timeout_ms: int,
) -> float:
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(
        page,
        '[data-source-catalog][data-source-catalog-name="workspace"]',
    )
    await wait_for_bucket_summary(page, bucket_name, timeout_ms)
    await ensure_details_open(
        page,
        f'[data-source-schema][data-source-bucket="{bucket_name}"]',
    )

    object_key = "samples/data.csv"
    object_root = page.locator(
        f'[data-source-object][data-s3-bucket="{bucket_name}"][data-s3-key="{object_key}"]'
    ).first
    await object_root.wait_for(state="attached", timeout=timeout_ms)
    await object_root.evaluate(
        """(node) => {
            let current = node.parentElement;
            while (current) {
                if (current instanceof HTMLDetailsElement) {
                    current.open = true;
                    current.setAttribute("open", "");
                }
                current = current.parentElement;
            }
        }"""
    )

    async def slow_delete(route):
        if route.request.method == "DELETE":
            await asyncio.sleep(0.75)
        await route.continue_()

    await page.route("**/api/s3/explorer/entries", slow_delete)
    try:
        await object_root.locator("[data-delete-source-s3-object]").evaluate("node => node.click()")
        await submit_confirm_if_open(page, timeout_ms)

        started = time.perf_counter()
        await page.wait_for_function(
            """([bucketName, objectKey]) => {
                const object = Array.from(document.querySelectorAll("[data-source-object][data-s3-bucket]"))
                    .find((node) => node.dataset.s3Bucket === bucketName && node.dataset.s3Key === objectKey);
                if (!object?.classList.contains("is-pending-delete")) {
                    return false;
                }
                const label = object.querySelector(".source-node-label");
                return label && getComputedStyle(label).textDecorationLine.includes("line-through");
            }""",
            arg=[bucket_name, object_key],
            timeout=timeout_ms,
        )
        await wait_for_sidebar_status(page, "Object deleted", timeout_ms)
        await object_root.wait_for(state="detached", timeout=timeout_ms)
        return (time.perf_counter() - started) * 1000
    finally:
        await page.unroute("**/api/s3/explorer/entries", slow_delete)


async def run_smoke(args: argparse.Namespace) -> int:
    created_bucket = unique_bucket_name("pw-sidebar-create")
    content_bucket = unique_bucket_name("pw-sidebar-content-delete")
    versioned_bucket = unique_bucket_name("pw-sidebar-versioned-delete")
    object_delete_bucket = unique_bucket_name("pw-sidebar-object-delete")
    descriptor_bucket = unique_bucket_name("pw-sidebar-descriptor-delete")
    client = s3_client(args)

    purge_bucket(client, created_bucket)
    seed_csv_bucket(client, content_bucket)
    seed_versioned_bucket(client, versioned_bucket)
    seed_csv_bucket(client, object_delete_bucket)
    seed_csv_bucket(client, descriptor_bucket)

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
        bucket_create_requests: list[str] = []
        page.on(
            "request",
            lambda req: bucket_create_requests.append(req.url)
            if req.method == "POST" and "/api/s3/explorer/buckets" in req.url
            else None,
        )

        try:
            await open_query_notebook(page, args.base_url, args.timeout_ms)
            await reject_invalid_bucket_via_sidebar(
                page,
                "client_bucket",
                args.timeout_ms,
                bucket_create_requests,
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
            content_delete_ms = await delete_bucket_via_sidebar(
                page,
                content_bucket,
                args.timeout_ms,
            )
            object_delete_ms = await delete_object_via_sidebar_shows_pending_strike(
                page,
                object_delete_bucket,
                args.timeout_ms,
            )
            versioned_delete_ms = await delete_bucket_via_sidebar(
                page,
                versioned_bucket,
                args.timeout_ms,
            )
            descriptor_delete_ms = await delete_bucket_with_schema_bucket_fallback(
                page,
                descriptor_bucket,
                args.timeout_ms,
            )
        except PlaywrightTimeoutError as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            for bucket_name in (
                created_bucket,
                content_bucket,
                versioned_bucket,
                object_delete_bucket,
                descriptor_bucket,
            ):
                purge_bucket(client, bucket_name)
            return 1
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            for bucket_name in (
                created_bucket,
                content_bucket,
                versioned_bucket,
                object_delete_bucket,
                descriptor_bucket,
            ):
                purge_bucket(client, bucket_name)
            return 1

        await browser.close()

    failures: list[str] = []
    if bucket_exists(client, created_bucket):
        failures.append(
            "Created bucket still exists after sidebar delete: "
            f"{created_bucket}"
        )
    if bucket_exists(client, content_bucket):
        failures.append(
            "Non-empty bucket still exists after sidebar delete: "
            f"{content_bucket}"
        )
    if bucket_exists(client, versioned_bucket):
        failures.append(
            "Versioned bucket still exists after recursive sidebar delete: "
            f"{versioned_bucket}"
        )
    if object_exists(client, object_delete_bucket, "samples/data.csv"):
        failures.append(
            "Sidebar object still exists after delete: "
            f"{object_delete_bucket}/samples/data.csv"
        )
    if bucket_exists(client, descriptor_bucket):
        failures.append(
            "Descriptor fallback bucket still exists after sidebar delete: "
            f"{descriptor_bucket}"
        )

    for method, url, status in responses:
        if "/api/s3/" in url:
            print(f"HTTP {method} {status} {url}")

    print(f"Sidebar create bucket: {create_ms:.0f} ms")
    print(f"Sidebar delete empty bucket: {delete_ms:.0f} ms")
    print(f"Sidebar delete non-empty bucket: {content_delete_ms:.0f} ms")
    print(f"Sidebar delete object pending strike: {object_delete_ms:.0f} ms")
    print(f"Sidebar delete versioned bucket: {versioned_delete_ms:.0f} ms")
    print(f"Sidebar delete bucket descriptor fallback: {descriptor_delete_ms:.0f} ms")

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        for bucket_name in (
            created_bucket,
            content_bucket,
            versioned_bucket,
            object_delete_bucket,
            descriptor_bucket,
        ):
            purge_bucket(client, bucket_name)
        return 1

    purge_bucket(client, object_delete_bucket)
    print("Playwright S3 sidebar smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

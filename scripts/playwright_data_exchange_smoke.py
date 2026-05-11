from __future__ import annotations

import argparse
import asyncio
import contextlib
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise DataExchange browser upload, metadata, download, and copy-out flows."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--file-password", default="file-secret")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="vat-smoke-test")
    parser.add_argument("--exchange-prefix", default="--data-exchange--/")
    parser.add_argument("--copy-prefix-root", default="playwright/data-exchange")
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


def delete_prefix(client, bucket: str, prefix: str) -> None:
    normalized_prefix = "/".join(segment for segment in prefix.split("/") if segment)
    if not normalized_prefix:
        return
    if not normalized_prefix.endswith("/"):
        normalized_prefix = f"{normalized_prefix}/"
    while True:
        response = client.list_objects_v2(Bucket=bucket, Prefix=normalized_prefix)
        objects = [{"Key": item["Key"]} for item in response.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        if not response.get("IsTruncated"):
            return


async def close_message_dialog(page, timeout_ms: int) -> str:
    dialog = page.locator("[data-message-dialog]")
    await dialog.wait_for(state="visible", timeout=timeout_ms)
    text = (await dialog.text_content() or "").strip()
    await page.locator("[data-message-submit]").click()
    await dialog.wait_for(state="hidden", timeout=timeout_ms)
    return text


async def cleanup_records(context, args: argparse.Namespace, file_names: list[str]) -> None:
    response = await context.request.get(
        f"{args.base_url.rstrip('/')}/api/data-exchange/files",
    )
    if not response.ok:
        return
    payload = await response.json()
    for item in payload.get("files", []):
        if item.get("fileName") not in file_names:
            continue
        file_id = item.get("fileId")
        if not file_id:
            continue
        await context.request.delete(
            f"{args.base_url.rstrip('/')}/api/data-exchange/files/{file_id}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            data={"filePassword": args.file_password},
        )


async def folder_by_name(context, args: argparse.Namespace, folder_name: str) -> dict[str, object] | None:
    response = await context.request.get(
        f"{args.base_url.rstrip('/')}/api/data-exchange/files",
    )
    if not response.ok:
        return None
    payload = await response.json()
    for item in payload.get("folders", []):
        if item.get("name") == folder_name:
            return item
    return None


async def cleanup_folder(context, args: argparse.Namespace, folder_name: str) -> None:
    folder = await folder_by_name(context, args, folder_name)
    folder_id = str((folder or {}).get("folderId") or "")
    if not folder_id:
        return
    await context.request.delete(f"{args.base_url.rstrip('/')}/api/data-exchange/folders/{folder_id}")


async def main() -> None:
    args = parse_args()
    unique_id = uuid4().hex[:8]
    csv_name = f"exchange-alpha-{unique_id}.csv"
    pdf_name = f"exchange-notice-{unique_id}.pdf"
    folder_name = f"Playwright Folder {unique_id}"
    copied_key = f"{args.copy_prefix_root.strip('/')}/{unique_id}/{csv_name}"
    client = s3_client(args)
    ensure_bucket(client, args.bucket)
    delete_prefix(client, args.bucket, f"{args.copy_prefix_root.strip('/')}/{unique_id}")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            await page.goto(args.base_url.rstrip("/"), wait_until="domcontentloaded", timeout=args.timeout_ms)
            await page.locator("[data-home-page]").wait_for(state="visible", timeout=args.timeout_ms)
            await page.wait_for_timeout(1000)
            await page.locator(".home-workbench-card[data-open-data-exchange-workbench]").click()
            await page.locator("[data-data-exchange-page]").wait_for(state="visible", timeout=args.timeout_ms)
            await page.locator("[data-data-exchange-workbench]").wait_for(state="visible", timeout=args.timeout_ms)
            await page.locator("[data-data-exchange-page][data-data-exchange-initialized='true']").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )

            await page.locator("[data-data-exchange-create-folder]").first.click()
            await page.locator("[data-data-exchange-folder-name]").fill(folder_name)
            await page.locator("[data-data-exchange-folder-form]").evaluate("form => form.requestSubmit()")
            await page.locator(f".data-exchange-folder-node:has-text('{folder_name}')").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )

            await page.locator("[data-data-exchange-show-upload]").click()
            await page.locator("[data-data-exchange-upload-panel]").wait_for(state="visible", timeout=args.timeout_ms)
            await page.locator("[data-data-exchange-file-input]").set_input_files(
                files=[
                    {
                        "name": csv_name,
                        "mimeType": "text/csv",
                        "buffer": b"id,name\n1,alpha\n",
                    },
                    {
                        "name": pdf_name,
                        "mimeType": "application/pdf",
                        "buffer": b"%PDF-1.4\n% smoke file\n",
                    },
                ]
            )
            await page.locator("[data-data-exchange-display-name]").fill(f"Playwright exchange {unique_id}")
            await page.locator("[data-data-exchange-owner-contact]").fill("playwright@example.test")
            await page.locator("[data-data-exchange-folder]").select_option(label=folder_name)
            await page.locator("[data-data-exchange-description]").fill("Playwright DataExchange smoke upload")
            await page.locator("[data-data-exchange-tags]").fill("playwright, exchange")
            await page.locator("[data-data-exchange-file-password]").fill(args.file_password)
            await page.locator("[data-data-exchange-upload-submit]").click()
            upload_message = await close_message_dialog(page, args.timeout_ms)
            if "2 file(s) uploaded" not in upload_message:
                raise RuntimeError(f"Unexpected upload success message: {upload_message!r}")

            csv_row = page.locator(f'[data-data-exchange-file-row]:has-text("{csv_name}")').first
            pdf_row = page.locator(f'[data-data-exchange-file-row]:has-text("{pdf_name}")').first
            await page.locator(".data-exchange-source-tree .source-catalog:has-text('DataExchange (S3)')").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.locator(".data-exchange-source-tree .source-schema:has-text('Exchange Files')").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await csv_row.wait_for(state="visible", timeout=args.timeout_ms)
            await pdf_row.wait_for(state="visible", timeout=args.timeout_ms)
            await page.locator(f".data-exchange-folder-node:has-text('{folder_name}') [data-data-exchange-file-row]").first.wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            list_text = await page.locator("[data-data-exchange-file-list]").text_content()
            if args.exchange_prefix.strip("/") in (list_text or ""):
                raise RuntimeError("DataExchange UI leaked the hidden technical prefix.")

            await csv_row.click()
            detail = page.locator("[data-data-exchange-file-detail]")
            await detail.locator(".data-exchange-lock").wait_for(state="visible", timeout=args.timeout_ms)
            detail_password = detail.locator("[data-data-exchange-detail-password]")
            await detail_password.fill("wrong")
            await detail.locator("[data-data-exchange-download]").click()
            wrong_download_message = await close_message_dialog(page, args.timeout_ms)
            if "password" not in wrong_download_message.lower():
                raise RuntimeError(f"Wrong file password was not rejected: {wrong_download_message!r}")

            await detail_password.fill(args.file_password)
            async with page.expect_download(timeout=args.timeout_ms) as download_info:
                await detail.locator("[data-data-exchange-download]").click()
            download = await download_info.value
            if download.suggested_filename != csv_name:
                raise RuntimeError(f"Unexpected downloaded file name: {download.suggested_filename!r}")

            await detail.locator("[data-data-exchange-edit]").click()
            await page.locator("[data-data-exchange-edit-password]").fill(args.file_password)
            await page.locator("[data-data-exchange-edit-display-name]").fill(f"Edited exchange {unique_id}")
            await page.locator("[data-data-exchange-edit-form]").evaluate("form => form.requestSubmit()")
            await page.locator(f'[data-data-exchange-file-row]:has-text("Edited exchange {unique_id}")').wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )

            csv_row = page.locator(f'[data-data-exchange-file-row]:has-text("{csv_name}")').first
            await csv_row.click()
            detail = page.locator("[data-data-exchange-file-detail]")
            await detail.locator("[data-data-exchange-detail-password]").fill(args.file_password)
            await detail.locator("[data-data-exchange-copy-s3]").click()
            await page.locator("[data-data-exchange-copy-password]").fill(args.file_password)
            await page.locator("[data-data-exchange-copy-bucket]").fill(args.bucket)
            await page.locator("[data-data-exchange-copy-prefix]").fill(f"{args.copy_prefix_root.strip('/')}/{unique_id}")
            await page.locator("[data-data-exchange-copy-file-name]").fill(csv_name)
            await page.locator("[data-data-exchange-copy-form]").evaluate("form => form.requestSubmit()")
            copy_message = await close_message_dialog(page, args.timeout_ms)
            if "Copied to Shared Workspace S3" not in copy_message:
                raise RuntimeError(f"Unexpected copy success message: {copy_message!r}")
            copied_body = client.get_object(Bucket=args.bucket, Key=copied_key)["Body"].read().decode("utf-8")
            if copied_body != "id,name\n1,alpha\n":
                raise RuntimeError(f"Unexpected copied S3 payload: {copied_body!r}")

            await cleanup_records(context, args, [csv_name, pdf_name])
            await cleanup_folder(context, args, folder_name)
        finally:
            with contextlib.suppress(Exception):
                await cleanup_records(context, args, [csv_name, pdf_name])
            with contextlib.suppress(Exception):
                await cleanup_folder(context, args, folder_name)
            delete_prefix(client, args.bucket, f"{args.copy_prefix_root.strip('/')}/{unique_id}")
            await context.close()
            await browser.close()

    print(f"DataExchange Playwright smoke passed for id {unique_id}")


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from uuid import uuid4

import boto3
from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload sample_tax_data.csv through the DataExchange UI and verify protected access."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--sample-path", default=str(REPO_ROOT / "sample_tax_data.csv"))
    parser.add_argument("--file-password", default="")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="vat-smoke-test")
    parser.add_argument("--exchange-prefix", default="--data-exchange--/")
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


def exchange_object_key(args: argparse.Namespace, file_id: str, file_name: str) -> str:
    prefix = "/".join(segment for segment in args.exchange_prefix.replace("\\", "/").split("/") if segment)
    if prefix:
        prefix = f"{prefix}/"
    return f"{prefix}files/{file_id}/{file_name}"


async def close_message_dialog(page, timeout_ms: int) -> str:
    dialog = page.locator("[data-message-dialog]")
    await dialog.wait_for(state="visible", timeout=timeout_ms)
    text = (await dialog.text_content() or "").strip()
    await page.locator("[data-message-submit]").click()
    await dialog.wait_for(state="hidden", timeout=timeout_ms)
    return text


async def listed_file(context, args: argparse.Namespace, display_name: str) -> dict[str, object]:
    response = await context.request.get(f"{args.base_url.rstrip('/')}/api/data-exchange/files")
    if not response.ok:
        raise RuntimeError(f"DataExchange listing failed with HTTP {response.status}")
    payload = await response.json()
    for item in payload.get("files", []):
        if item.get("displayName") == display_name:
            return item
    raise RuntimeError(f"Uploaded DataExchange file {display_name!r} was not listed.")


async def delete_record(context, args: argparse.Namespace, file_id: str) -> None:
    await context.request.delete(
        f"{args.base_url.rstrip('/')}/api/data-exchange/files/{file_id}",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        data={"filePassword": args.file_password},
    )


async def main() -> None:
    args = parse_args()
    sample_path = Path(args.sample_path)
    if not sample_path.is_file():
        raise FileNotFoundError(f"Sample CSV file not found: {sample_path}")

    unique_id = uuid4().hex[:8]
    display_name = f"Playwright tax sample {unique_id}"
    sample_bytes = sample_path.read_bytes()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        file_id = ""
        try:
            await page.goto(f"{args.base_url.rstrip('/')}/data-exchange", wait_until="domcontentloaded", timeout=args.timeout_ms)
            await page.locator("[data-data-exchange-page]").wait_for(state="visible", timeout=args.timeout_ms)
            await page.locator("[data-data-exchange-workbench]").wait_for(state="visible", timeout=args.timeout_ms)
            await page.locator("[data-data-exchange-page][data-data-exchange-initialized='true']").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            if await page.locator("[data-data-exchange-workbench-password]").count():
                raise RuntimeError("DataExchange unexpectedly rendered a workbench password gate.")

            await page.locator("[data-data-exchange-show-upload]").click()
            await page.locator("[data-data-exchange-upload-panel]").wait_for(state="visible", timeout=args.timeout_ms)
            await page.locator("[data-data-exchange-file-input]").set_input_files(str(sample_path))
            await page.locator("[data-data-exchange-display-name]").fill(display_name)
            await page.locator("[data-data-exchange-owner-contact]").fill("tax-team@example.test")
            await page.locator("[data-data-exchange-description]").fill("Synthetic tax sample uploaded by Playwright.")
            await page.locator("[data-data-exchange-tags]").fill("tax, csv, playwright")
            if args.file_password:
                await page.locator("[data-data-exchange-file-password]").fill(args.file_password)
            await page.locator("[data-data-exchange-upload-submit]").click()
            upload_message = await close_message_dialog(page, args.timeout_ms)
            if "1 file(s) uploaded" not in upload_message:
                raise RuntimeError(f"Unexpected upload success message: {upload_message!r}")

            row = page.locator(f'[data-data-exchange-file-row]:has-text("{display_name}")').first
            await page.locator(".data-exchange-source-tree .source-catalog:has-text('DataExchange (S3)')").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.locator(".data-exchange-source-tree .source-schema:has-text('Exchange Files')").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await row.wait_for(state="visible", timeout=args.timeout_ms)
            list_text = await page.locator("[data-data-exchange-file-list]").text_content()
            if args.exchange_prefix.strip("/") in (list_text or ""):
                raise RuntimeError("DataExchange UI leaked the hidden technical prefix.")

            record = await listed_file(context, args, display_name)
            file_id = str(record.get("fileId") or "")
            file_name = str(record.get("fileName") or "")
            if file_name != sample_path.name:
                raise RuntimeError(f"Unexpected uploaded file name: {file_name!r}")

            object_key = exchange_object_key(args, file_id, file_name)
            stored = s3_client(args).get_object(Bucket=args.bucket, Key=object_key)["Body"].read()
            if stored != sample_bytes:
                raise RuntimeError("S3 stored payload does not match sample_tax_data.csv.")

            await row.click()
            detail = page.locator("[data-data-exchange-file-detail]")
            if args.file_password:
                await detail.locator(".data-exchange-lock").wait_for(state="visible", timeout=args.timeout_ms)
                password_input = detail.locator("[data-data-exchange-detail-password]")
                await password_input.fill("wrong")
                await detail.locator("[data-data-exchange-download]").click()
                wrong_message = await close_message_dialog(page, args.timeout_ms)
                if "password" not in wrong_message.lower():
                    raise RuntimeError(f"Wrong file password was not rejected: {wrong_message!r}")
                await password_input.fill(args.file_password)
            else:
                if await detail.locator(".data-exchange-lock").count():
                    raise RuntimeError("Unprotected DataExchange file unexpectedly shows a lock icon.")
                if await detail.locator("[data-data-exchange-detail-password]").count():
                    raise RuntimeError("Unprotected DataExchange file unexpectedly asks for a password.")

            async with page.expect_download(timeout=args.timeout_ms) as download_info:
                await detail.locator("[data-data-exchange-download]").click()
            download = await download_info.value
            if download.suggested_filename != sample_path.name:
                raise RuntimeError(f"Unexpected download filename: {download.suggested_filename!r}")
            downloaded = Path(await download.path()).read_bytes()
            if downloaded != sample_bytes:
                raise RuntimeError("Downloaded payload does not match sample_tax_data.csv.")
        finally:
            if file_id:
                await delete_record(context, args, file_id)
            await context.close()
            await browser.close()

    print(f"DataExchange sample tax CSV upload smoke passed for id {unique_id}")


if __name__ == "__main__":
    asyncio.run(main())

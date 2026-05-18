from __future__ import annotations

import argparse
import asyncio
import contextlib
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from urllib.parse import urlencode
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise browser download of a Shared Workspace S3 CSV object through "
            "the real API route. The target app and local dependencies must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="1-vat-smoke-test")
    parser.add_argument("--s3-smoke-prefix-root", default="playwright/s3-csv-download")
    parser.add_argument("--size-mib", type=int, default=8)
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


def build_csv_payload(size_mib: int) -> bytes:
    target_size = max(1, size_mib) * 1024 * 1024
    header = b"id,name,amount\n"
    line = b"1,alpha,123.45\n"
    repeat_count = max(1, (target_size - len(header)) // len(line))
    return header + (line * repeat_count)


async def run_smoke(args: argparse.Namespace) -> int:
    unique_id = uuid4().hex[:8]
    prefix_root = args.s3_smoke_prefix_root.strip("/")
    key = f"{prefix_root}/{unique_id}/large-download.csv"
    payload = build_csv_payload(args.size_mib)
    client = s3_client(args)
    ensure_bucket(client, args.bucket)
    client.put_object(
        Bucket=args.bucket,
        Key=key,
        Body=payload,
        ContentType="text/csv",
    )

    query = urlencode(
        {
            "bucket": args.bucket,
            "key": key,
            "filename": "large-download.csv",
        }
    )
    download_url = f"{args.base_url.rstrip('/')}/api/s3/object/download?{query}"
    console_messages: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1280, "height": 900},
            base_url=args.base_url.rstrip("/"),
        )
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await page.goto(args.base_url.rstrip("/"), wait_until="domcontentloaded", timeout=args.timeout_ms)
            async with page.expect_download(timeout=args.timeout_ms) as download_info:
                await page.evaluate(
                    """
                    url => {
                        const anchor = document.createElement("a");
                        anchor.href = url;
                        anchor.textContent = "download";
                        document.body.appendChild(anchor);
                        anchor.click();
                    }
                    """,
                    download_url,
                )
            download = await download_info.value
            if download.suggested_filename != "large-download.csv":
                raise RuntimeError(
                    f"Unexpected suggested filename: {download.suggested_filename!r}"
                )
            with TemporaryDirectory() as temp_dir:
                local_path = Path(temp_dir) / "large-download.csv"
                await download.save_as(local_path)
                downloaded = local_path.read_bytes()
            if downloaded != payload:
                raise RuntimeError(
                    f"Downloaded CSV payload mismatch: {len(downloaded)} bytes != {len(payload)} bytes."
                )
        except (ClientError, PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1
        finally:
            await browser.close()
            with contextlib.suppress(Exception):
                client.delete_object(Bucket=args.bucket, Key=key)

    print(
        f"Playwright S3 CSV browser download smoke passed for {len(payload)} bytes at s3://{args.bucket}/{key}."
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import asyncio
import contextlib
import sys
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise S3 special-key delete and generated ZIP job APIs through a browser context."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="vat-smoke-test")
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
    except ClientError:
        client.create_bucket(Bucket=bucket)


async def run_smoke(args: argparse.Namespace) -> int:
    client = s3_client(args)
    ensure_bucket(client, args.bucket)
    unique_id = uuid4().hex[:8]
    delete_key = f"playwright/delete special {unique_id}/large object.csv"
    parts_prefix = f"playwright/zip-job-{unique_id}/"
    artifact_key = f"{parts_prefix}archive.zip"
    client.put_object(Bucket=args.bucket, Key=delete_key, Body=(b"id,name\n1,alpha\n" * 1024 * 512))
    client.put_object(Bucket=args.bucket, Key=f"{parts_prefix}part-00001.csv", Body=b"id,name\n1,alpha\n")
    client.put_object(Bucket=args.bucket, Key=f"{parts_prefix}part-00002.csv", Body=b"id,name\n2,beta\n")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(base_url=args.base_url.rstrip("/"))
        try:
            await page.goto(args.base_url.rstrip("/"), wait_until="domcontentloaded", timeout=args.timeout_ms)
            delete_response = await page.request.delete(
                f"{args.base_url.rstrip('/')}/api/s3/explorer/entries",
                data={"entryKind": "file", "bucket": args.bucket, "prefix": delete_key},
            )
            if not delete_response.ok:
                raise RuntimeError(f"S3 delete failed: {delete_response.status} {await delete_response.text()}")
            with contextlib.suppress(ClientError):
                client.head_object(Bucket=args.bucket, Key=delete_key)
                raise RuntimeError("Deleted S3 object is still present.")

            start_response = await page.request.post(
                f"{args.base_url.rstrip('/')}/api/s3/generated/zip-jobs",
                data={"bucket": args.bucket, "prefix": parts_prefix, "format": "csv", "filename": "archive.zip"},
            )
            if not start_response.ok:
                raise RuntimeError(f"ZIP job start failed: {start_response.status} {await start_response.text()}")
            payload = await start_response.json()
            job_id = payload["jobId"]
            for _ in range(120):
                state_response = await page.request.get(f"{args.base_url.rstrip('/')}/api/s3/download-jobs/{job_id}")
                state = await state_response.json()
                if state["status"] in {"completed", "failed", "cancelled"}:
                    break
                await page.wait_for_timeout(500)
            else:
                raise RuntimeError("ZIP job did not finish before timeout.")
            if state["status"] != "completed":
                raise RuntimeError(f"ZIP job did not complete: {state}")
            client.head_object(Bucket=args.bucket, Key=artifact_key)
        except (PlaywrightTimeoutError, RuntimeError, ClientError) as exc:
            print(str(exc), file=sys.stderr)
            await browser.close()
            return 1
        finally:
            await browser.close()
            for key in [delete_key, f"{parts_prefix}part-00001.csv", f"{parts_prefix}part-00002.csv", artifact_key]:
                with contextlib.suppress(Exception):
                    client.delete_object(Bucket=args.bucket, Key=key)
    print("Playwright S3 delete and ZIP job smoke passed.")
    return 0


def main() -> int:
    return asyncio.run(run_smoke(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

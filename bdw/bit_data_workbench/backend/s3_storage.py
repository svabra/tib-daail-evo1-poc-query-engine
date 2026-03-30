from __future__ import annotations

from collections.abc import Iterator
import hashlib
import re

import boto3
from botocore.config import Config

from ..config import Settings


def s3_client(settings: Settings):
    endpoint = settings.s3_endpoint
    if not endpoint:
        raise ValueError("S3 endpoint configuration is required for this operation.")

    endpoint_url = endpoint if "://" in endpoint else f"{'https' if settings.s3_use_ssl else 'http'}://{endpoint}"
    config = Config(
        s3={
            "addressing_style": "path" if (settings.s3_url_style or "").lower() == "path" else "auto",
        }
    )
    verify: bool | str = settings.s3_verify_ssl
    if settings.s3_verify_ssl and settings.s3_ca_cert_file is not None:
        verify = settings.s3_ca_cert_file.as_posix()
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=settings.s3_region or "us-east-1",
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        aws_session_token=settings.s3_session_token,
        config=config,
        verify=verify,
    )


def iter_s3_keys(client, bucket: str, prefix: str = "") -> Iterator[str]:
    continuation_token = None
    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        response = client.list_objects_v2(**kwargs)
        for item in response.get("Contents") or []:
            key = item.get("Key")
            if key:
                yield key

        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")


def list_s3_buckets(settings: Settings) -> list[str]:
    client = s3_client(settings)
    response = client.list_buckets()
    buckets = [
        str(item.get("Name")).strip()
        for item in (response.get("Buckets") or [])
        if str(item.get("Name") or "").strip()
    ]
    return sorted(set(buckets))


def s3_bucket_schema_name(bucket_name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(bucket_name).strip()).strip("_").lower()
    if not normalized:
        normalized = "s3_bucket"
    if normalized[0].isdigit():
        normalized = f"s3_{normalized}"

    suffix = hashlib.sha1(str(bucket_name).encode("utf-8")).hexdigest()[:8]
    max_base_length = max(1, 56 - len(suffix) - 1)
    base = normalized[:max_base_length].rstrip("_") or "s3_bucket"
    return f"{base}_{suffix}"


def delete_s3_keys(client, bucket: str, keys: list[str]) -> int:
    if not keys:
        return 0

    deleted = 0
    for index in range(0, len(keys), 1000):
        chunk = keys[index : index + 1000]
        client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": key} for key in chunk],
                "Quiet": True,
            },
        )
        deleted += len(chunk)
    return deleted


def delete_s3_prefix(settings: Settings, bucket: str, prefix: str) -> int:
    client = s3_client(settings)
    keys = list(iter_s3_keys(client, bucket, prefix))
    return delete_s3_keys(client, bucket, keys)


def delete_s3_bucket(settings: Settings, bucket: str) -> int:
    client = s3_client(settings)
    keys = list(iter_s3_keys(client, bucket))
    return delete_s3_keys(client, bucket, keys)

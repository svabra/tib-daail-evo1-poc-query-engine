from __future__ import annotations

from collections.abc import Iterator
import hashlib
import re

import boto3
from botocore.config import Config

from ..config import Settings


def s3_endpoint_url(settings: Settings, *, use_ssl: bool | None = None) -> str:
    endpoint = settings.s3_endpoint
    if not endpoint:
        raise ValueError("S3 endpoint configuration is required for this operation.")

    ssl_enabled = settings.s3_use_ssl if use_ssl is None else use_ssl
    return endpoint if "://" in endpoint else f"{'https' if ssl_enabled else 'http'}://{endpoint}"


def s3_verify_value(
    settings: Settings,
    *,
    verify_ssl: bool | str | None = None,
) -> bool | str:
    if isinstance(verify_ssl, str):
        return verify_ssl

    verification_enabled = settings.s3_verify_ssl if verify_ssl is None else verify_ssl
    effective_ca_bundle = settings.effective_s3_ca_cert_file()
    if verification_enabled and effective_ca_bundle is not None:
        return effective_ca_bundle.as_posix()
    return verification_enabled


def s3_client(
    settings: Settings,
    *,
    use_ssl: bool | None = None,
    url_style: str | None = None,
    verify_ssl: bool | str | None = None,
):
    endpoint_url = s3_endpoint_url(settings, use_ssl=use_ssl)
    addressing_style = (
        (url_style or "").strip().lower()
        or ("path" if (settings.s3_url_style or "").strip().lower() == "path" else "auto")
    )
    config = Config(
        connect_timeout=3,
        read_timeout=5,
        retries={
            "max_attempts": 2,
            "mode": "standard",
        },
        s3={
            "addressing_style": addressing_style,
        }
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=settings.current_s3_access_key_id(),
        aws_secret_access_key=settings.current_s3_secret_access_key(),
        aws_session_token=settings.current_s3_session_token(),
        config=config,
        verify=s3_verify_value(settings, verify_ssl=verify_ssl),
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

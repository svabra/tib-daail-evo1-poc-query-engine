from __future__ import annotations

from collections.abc import Iterator
import hashlib
import re
import time
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from ..config import Settings


def _parsed_endpoint_host_port(raw_endpoint: str) -> tuple[str | None, int | None]:
    parsed = urlparse(raw_endpoint if "://" in raw_endpoint else f"//{raw_endpoint}")
    return parsed.hostname, parsed.port


def _is_likely_local_s3_host(hostname: str | None) -> bool:
    if not hostname:
        return False
    normalized = hostname.strip().lower()
    if normalized in {"localhost", "127.0.0.1", "::1", "minio"}:
        return True
    return normalized.endswith(".local")


def normalize_s3_endpoint(
    raw_endpoint: str,
    *,
    use_ssl: bool,
    verify_ssl: bool,
) -> tuple[str, bool, str | None]:
    if "://" in raw_endpoint:
        parsed = urlparse(raw_endpoint)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError(f"Unsupported S3 endpoint scheme: {parsed.scheme}")
        if not parsed.netloc:
            raise ValueError(f"Invalid S3 endpoint: {raw_endpoint}")
        return parsed.netloc, parsed.scheme == "https", None

    hostname, port = _parsed_endpoint_host_port(raw_endpoint)
    if use_ssl:
        return raw_endpoint, True, None

    if not _is_likely_local_s3_host(hostname) and verify_ssl:
        return (
            raw_endpoint,
            True,
            "forcing HTTPS because the endpoint has no scheme, is not local, and SSL verification is enabled",
        )

    if not _is_likely_local_s3_host(hostname) and port == 9021:
        return (
            raw_endpoint,
            True,
            "forcing HTTPS because the endpoint has no scheme, is not local, and port 9021 is expected to be TLS",
        )

    return raw_endpoint, False, None


def s3_endpoint_url(
    settings: Settings,
    *,
    use_ssl: bool | None = None,
    verify_ssl: bool | str | None = None,
) -> str:
    endpoint = settings.s3_endpoint
    if not endpoint:
        raise ValueError("S3 endpoint configuration is required for this operation.")

    verification_enabled = (
        settings.s3_verify_ssl
        if verify_ssl is None
        else (True if isinstance(verify_ssl, str) else verify_ssl)
    )
    normalized_endpoint, ssl_enabled, _reason = normalize_s3_endpoint(
        endpoint,
        use_ssl=settings.s3_use_ssl if use_ssl is None else use_ssl,
        verify_ssl=verification_enabled,
    )
    return f"{'https' if ssl_enabled else 'http'}://{normalized_endpoint}"


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
    endpoint_url = s3_endpoint_url(settings, use_ssl=use_ssl, verify_ssl=verify_ssl)
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
    return list_s3_buckets_from_client(client)


def list_s3_buckets_from_client(client) -> list[str]:
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


def derived_s3_bucket_name(base_bucket_name: str, suffix: str) -> str:
    normalized_base = re.sub(r"[^a-z0-9-]+", "-", str(base_bucket_name).strip().lower())
    normalized_base = re.sub(r"-{2,}", "-", normalized_base).strip("-") or "s3-bucket"
    normalized_suffix = re.sub(r"[^a-z0-9-]+", "-", str(suffix).strip().lower())
    normalized_suffix = re.sub(r"-{2,}", "-", normalized_suffix).strip("-")
    if not normalized_suffix:
        return normalized_base[:63].rstrip("-") or "s3-bucket"

    max_base_length = max(3, 63 - len(normalized_suffix) - 1)
    trimmed_base = normalized_base[:max_base_length].rstrip("-") or "s3"
    return f"{trimmed_base}-{normalized_suffix}"


def parse_s3_url(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Unsupported S3 path: {path}")
    return parsed.netloc, parsed.path.lstrip("/")


def _s3_error_code(error: Exception) -> str | None:
    if not isinstance(error, ClientError):
        return None
    code = error.response.get("Error", {}).get("Code")
    if code is None:
        return None
    return str(code).strip()


def _is_missing_bucket_error(error: Exception) -> bool:
    code = _s3_error_code(error)
    if code is None:
        return False
    return code in {"404", "NoSuchBucket", "NotFound"}


def _bucket_is_accessible(client, bucket: str) -> bool:
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except Exception as head_error:
        try:
            client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            return True
        except Exception as list_error:
            if _is_missing_bucket_error(head_error) or _is_missing_bucket_error(list_error):
                return False
            raise list_error from head_error


def ensure_s3_bucket(
    settings: Settings,
    bucket: str,
    *,
    max_create_attempts: int = 4,
    max_ready_attempts: int = 10,
    base_delay_seconds: float = 0.2,
) -> bool:
    client = s3_client(settings)
    if _bucket_is_accessible(client, bucket):
        return False

    created = False
    for create_attempt in range(max_create_attempts):
        try:
            client.create_bucket(Bucket=bucket)
            created = True
            break
        except Exception as exc:
            error_code = _s3_error_code(exc)
            if error_code in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                break
            if error_code == "OperationAborted" and create_attempt + 1 < max_create_attempts:
                time.sleep(base_delay_seconds * (create_attempt + 1))
                continue
            if _bucket_is_accessible(client, bucket):
                return False
            raise

    for ready_attempt in range(max_ready_attempts):
        if _bucket_is_accessible(client, bucket):
            return created
        time.sleep(base_delay_seconds * (ready_attempt + 1))

    client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    return created


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
    if not _bucket_is_accessible(client, bucket):
        return 0
    keys = list(iter_s3_keys(client, bucket, prefix))
    return delete_s3_keys(client, bucket, keys)


def delete_s3_bucket(settings: Settings, bucket: str) -> int:
    client = s3_client(settings)
    if not _bucket_is_accessible(client, bucket):
        return 0
    keys = list(iter_s3_keys(client, bucket))
    return delete_s3_keys(client, bucket, keys)


def remove_s3_bucket(settings: Settings, bucket: str) -> bool:
    client = s3_client(settings)
    if not _bucket_is_accessible(client, bucket):
        return False
    response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    if response.get("KeyCount") or response.get("Contents"):
        return False
    client.delete_bucket(Bucket=bucket)
    return True

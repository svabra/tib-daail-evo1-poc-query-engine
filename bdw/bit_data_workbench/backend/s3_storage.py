from __future__ import annotations

import base64
from collections.abc import Iterator, Sequence
import hashlib
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

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


def _canonicalize_local_s3_endpoint(
    endpoint: str,
    hostname: str | None,
    port: int | None,
) -> str:
    normalized_host = (hostname or "").strip().lower()
    if normalized_host not in {"localhost", "::1"}:
        return endpoint
    if port is None:
        return "127.0.0.1"
    return f"127.0.0.1:{port}"


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
        return (
            _canonicalize_local_s3_endpoint(
                parsed.netloc,
                parsed.hostname,
                parsed.port,
            ),
            parsed.scheme == "https",
            None,
        )

    hostname, port = _parsed_endpoint_host_port(raw_endpoint)
    normalized_endpoint = _canonicalize_local_s3_endpoint(
        raw_endpoint,
        hostname,
        port,
    )
    if use_ssl:
        return normalized_endpoint, True, None

    if not _is_likely_local_s3_host(hostname) and verify_ssl:
        return (
            normalized_endpoint,
            True,
            "forcing HTTPS because the endpoint has no scheme, is not local, and SSL verification is enabled",
        )

    if not _is_likely_local_s3_host(hostname) and port == 9021:
        return (
            normalized_endpoint,
            True,
            "forcing HTTPS because the endpoint has no scheme, is not local, and port 9021 is expected to be TLS",
        )

    return normalized_endpoint, False, None


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


def _request_body_bytes(body: object) -> bytes:
    if body is None:
        return b""
    if isinstance(body, bytes):
        return body
    if isinstance(body, bytearray):
        return bytes(body)
    if isinstance(body, str):
        return body.encode("utf-8")
    return str(body).encode("utf-8")


def _content_md5_header_value(body: object) -> str:
    digest = hashlib.md5(_request_body_bytes(body)).digest()
    return base64.b64encode(digest).decode("ascii")


def _inject_delete_objects_content_md5(request, **_kwargs) -> None:
    if request is None:
        return
    request.headers["Content-MD5"] = _content_md5_header_value(request.body)


def _register_delete_objects_md5_handler(client) -> None:
    client.meta.events.register(
        "before-sign.s3.DeleteObjects",
        _inject_delete_objects_content_md5,
    )


def effective_s3_url_style(
    settings: Settings,
    *,
    endpoint: str | None = None,
    use_ssl: bool | None = None,
    explicit_url_style: str | None = None,
) -> str | None:
    configured_url_style = (explicit_url_style or settings.s3_url_style or "").strip().lower()
    if configured_url_style:
        return configured_url_style

    endpoint_value = (endpoint or settings.s3_endpoint or "").strip()
    if not endpoint_value:
        return None

    try:
        normalized_endpoint, _ssl_enabled, _reason = normalize_s3_endpoint(
            endpoint_value,
            use_ssl=settings.s3_use_ssl if use_ssl is None else use_ssl,
            verify_ssl=settings.s3_verify_ssl,
        )
        hostname, _port = _parsed_endpoint_host_port(normalized_endpoint)
    except Exception:
        hostname, _port = _parsed_endpoint_host_port(endpoint_value)

    # Custom ECS-style endpoints work reliably in path mode and avoid TLS
    # hostname mismatches such as bucket.endpoint.example:9021.
    if not _is_likely_local_s3_host(hostname):
        return "path"
    return None


def s3_client(
    settings: Settings,
    *,
    use_ssl: bool | None = None,
    url_style: str | None = None,
    verify_ssl: bool | str | None = None,
):
    endpoint_url = s3_endpoint_url(settings, use_ssl=use_ssl, verify_ssl=verify_ssl)
    addressing_style = effective_s3_url_style(
        settings,
        use_ssl=use_ssl,
        explicit_url_style=url_style,
    ) or "auto"
    s3_config = {
        "addressing_style": addressing_style,
    }
    if endpoint_url.startswith("https://"):
        # ECS-compatible HTTPS endpoints work more reliably with unsigned
        # payloads for boto-managed uploads. This avoids SHA256 payload
        # mismatches on proxy-backed object-store writes.
        s3_config["payload_signing_enabled"] = False
    config = Config(
        connect_timeout=3,
        read_timeout=5,
        retries={
            "max_attempts": 2,
            "mode": "standard",
        },
        s3=s3_config,
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=settings.current_s3_access_key_id(),
        aws_secret_access_key=settings.current_s3_secret_access_key(),
        aws_session_token=settings.current_s3_session_token(),
        config=config,
        verify=s3_verify_value(settings, verify_ssl=verify_ssl),
    )
    _register_delete_objects_md5_handler(client)
    return client


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


def iter_s3_object_versions(client, bucket: str, prefix: str = "") -> Iterator[dict[str, str]]:
    key_marker = None
    version_id_marker = None

    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if key_marker is not None:
            kwargs["KeyMarker"] = key_marker
        if version_id_marker is not None:
            kwargs["VersionIdMarker"] = version_id_marker

        response = client.list_object_versions(**kwargs)
        for item in response.get("Versions") or []:
            key = item.get("Key")
            version_id = item.get("VersionId")
            if key and version_id:
                yield {"Key": key, "VersionId": version_id}

        for item in response.get("DeleteMarkers") or []:
            key = item.get("Key")
            version_id = item.get("VersionId")
            if key and version_id:
                yield {"Key": key, "VersionId": version_id}

        if not response.get("IsTruncated"):
            break
        key_marker = response.get("NextKeyMarker")
        version_id_marker = response.get("NextVersionIdMarker")


def _version_listing_fallback_allowed(error: Exception) -> bool:
    code = _s3_error_code(error)
    return code in {
        "501",
        "MethodNotAllowed",
        "NotImplemented",
        "XMinioNotImplemented",
    }


def _raise_s3_operation_error(
    error: Exception,
    *,
    action: str,
    bucket: str,
) -> None:
    if _is_missing_bucket_error(error):
        raise ValueError(
            f"The S3 bucket '{bucket}' does not exist."
        ) from error

    raise ValueError(
        f"Failed to {action} in S3 bucket '{bucket}': {_s3_error_message(error)}"
    ) from error


def delete_s3_objects(client, bucket: str, objects: list[str | dict[str, str]]) -> int:
    if not objects:
        return 0

    deleted = 0
    for index in range(0, len(objects), 1000):
        chunk = objects[index:index + 1000]
        identifiers = []
        for item in chunk:
            if isinstance(item, str):
                identifiers.append({"Key": item})
            else:
                identifier = {"Key": item["Key"]}
                if item.get("VersionId"):
                    identifier["VersionId"] = item["VersionId"]
                identifiers.append(identifier)

        try:
            client.delete_objects(
                Bucket=bucket,
                Delete={
                    "Objects": identifiers,
                    "Quiet": True,
                },
            )
        except (ClientError, BotoCoreError) as exc:
            _raise_s3_operation_error(
                exc,
                action="delete objects",
                bucket=bucket,
            )
        deleted += len(chunk)
    return deleted


def delete_s3_keys(client, bucket: str, keys: list[str]) -> int:
    return delete_s3_objects(client, bucket, keys)


def delete_s3_object_versions(client, bucket: str, key: str) -> int:
    try:
        versions = [
            item
            for item in iter_s3_object_versions(client, bucket, prefix=key)
            if item["Key"] == key
        ]
    except (ClientError, BotoCoreError) as exc:
        if _is_missing_bucket_error(exc):
            raise ValueError(f"The S3 bucket '{bucket}' does not exist.") from exc
        if not _version_listing_fallback_allowed(exc):
            _raise_s3_operation_error(
                exc,
                action=f"list object versions for '{key}'",
                bucket=bucket,
            )
        return delete_s3_keys(client, bucket, [key])

    if versions:
        return delete_s3_objects(client, bucket, versions)
    return delete_s3_keys(client, bucket, [key])


def delete_s3_prefix(settings: Settings, bucket: str, prefix: str) -> int:
    client = s3_client(settings)
    if not _bucket_is_accessible(client, bucket):
        return 0
    try:
        versions = list(iter_s3_object_versions(client, bucket, prefix))
    except (ClientError, BotoCoreError) as exc:
        if _is_missing_bucket_error(exc):
            raise ValueError(f"The S3 bucket '{bucket}' does not exist.") from exc
        if not _version_listing_fallback_allowed(exc):
            _raise_s3_operation_error(
                exc,
                action=f"list object versions under prefix '{prefix}'",
                bucket=bucket,
            )
        keys = list(iter_s3_keys(client, bucket, prefix))
        return delete_s3_keys(client, bucket, keys)
    if versions:
        return delete_s3_objects(client, bucket, versions)
    keys = list(iter_s3_keys(client, bucket, prefix))
    return delete_s3_keys(client, bucket, keys)


def delete_s3_bucket(settings: Settings, bucket: str) -> int:
    client = s3_client(settings)
    if not _bucket_is_accessible(client, bucket):
        return 0
    try:
        versions = list(iter_s3_object_versions(client, bucket))
    except (ClientError, BotoCoreError) as exc:
        if _is_missing_bucket_error(exc):
            raise ValueError(f"The S3 bucket '{bucket}' does not exist.") from exc
        if not _version_listing_fallback_allowed(exc):
            _raise_s3_operation_error(
                exc,
                action="list object versions",
                bucket=bucket,
            )
        keys = list(iter_s3_keys(client, bucket))
        return delete_s3_keys(client, bucket, keys)
    if versions:
        return delete_s3_objects(client, bucket, versions)
    keys = list(iter_s3_keys(client, bucket))
    return delete_s3_keys(client, bucket, keys)


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


def upload_s3_file(client, *, local_path: Path, bucket: str, key: str) -> None:
    client.upload_file(str(local_path), bucket, key)


def download_s3_file(client, *, bucket: str, key: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(local_path))


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def duckdb_scan_query(data_format: str, paths: Sequence[str]) -> str:
    normalized_paths = [str(path).strip() for path in paths if str(path).strip()]
    if not normalized_paths:
        raise ValueError("At least one file path is required to build a DuckDB scan query.")

    if len(normalized_paths) == 1:
        source_sql = _sql_literal(normalized_paths[0])
    else:
        source_sql = "[" + ", ".join(_sql_literal(path) for path in normalized_paths) + "]"

    if data_format == "parquet":
        return f"SELECT * FROM read_parquet({source_sql})"
    if data_format == "csv":
        return f"SELECT * FROM read_csv_auto({source_sql})"
    if data_format == "json":
        return f"SELECT * FROM read_json_auto({source_sql})"
    raise ValueError(f"Unsupported S3 discovery format: {data_format}")


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
        if _is_missing_bucket_error(head_error):
            return False
        try:
            client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            return True
        except Exception as list_error:
            if _is_missing_bucket_error(list_error):
                return False
            raise ValueError(
                f"Unable to access S3 bucket '{bucket}': {_s3_error_message(list_error)}"
            ) from list_error


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


def _s3_error_message(error: Exception) -> str:
    if not isinstance(error, ClientError):
        return str(error)
    error_info = error.response.get("Error", {})
    code = str(error_info.get("Code") or "").strip()
    message = str(error_info.get("Message") or "").strip()
    if code and message:
        return f"{code}: {message}"
    if message:
        return message
    return code or str(error)


def remove_s3_bucket(settings: Settings, bucket: str) -> bool:
    client = s3_client(settings)
    if not _bucket_is_accessible(client, bucket):
        return False
    try:
        response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    except (ClientError, BotoCoreError) as exc:
        if _is_missing_bucket_error(exc):
            raise ValueError(f"The S3 bucket '{bucket}' does not exist.") from exc
        _raise_s3_operation_error(
            exc,
            action="inspect bucket state",
            bucket=bucket,
        )
    if response.get("KeyCount") or response.get("Contents"):
        return False
    try:
        client.delete_bucket(Bucket=bucket)
    except (ClientError, BotoCoreError) as exc:
        _raise_s3_operation_error(
            exc,
            action="delete the bucket",
            bucket=bucket,
        )
    return True

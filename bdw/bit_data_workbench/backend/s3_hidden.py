from __future__ import annotations

from ..config import Settings
from .s3_storage import derived_s3_bucket_name


DATA_EXCHANGE_BUCKET_MARKERS = ("--data-exchange--", "data-exchange")
SHARED_NOTEBOOKS_BUCKET_SUFFIX = "shared-notebooks"
INTERNAL_S3_PREFIX = "--bdw-internal--/"
QUERY_RUN_HISTORY_S3_PREFIX = f"{INTERNAL_S3_PREFIX}query-runs/"
NOTEBOOK_ACTIVITY_S3_PREFIX = f"{INTERNAL_S3_PREFIX}notebook-activity/"
DOWNLOAD_JOB_S3_PREFIX = f"{INTERNAL_S3_PREFIX}download-jobs/"
DOWNLOAD_ARTIFACT_S3_PREFIX = f"{INTERNAL_S3_PREFIX}download-artifacts/"


def normalize_data_exchange_prefix(prefix: str | None) -> str:
    raw_value = str(prefix or "").strip().replace("\\", "/")
    parts = [segment.strip() for segment in raw_value.split("/") if segment.strip()]
    return "/".join(parts) + "/" if parts else "--data-exchange--/"


def is_data_exchange_key(key: str, prefix: str | None = None) -> bool:
    normalized_prefix = normalize_data_exchange_prefix(prefix)
    normalized_key = str(key or "").strip().replace("\\", "/")
    return bool(normalized_key) and (
        normalized_key == normalized_prefix.rstrip("/")
        or normalized_key.startswith(normalized_prefix)
    )


def normalize_s3_hidden_key(key: str | None) -> str:
    raw_value = str(key or "").strip().replace("\\", "/")
    parts = [segment.strip() for segment in raw_value.split("/") if segment.strip()]
    return "/".join(parts) + ("/" if raw_value.endswith("/") and parts else "")


def is_internal_s3_key(key: str | None) -> bool:
    normalized_key = normalize_s3_hidden_key(key)
    internal_prefix = INTERNAL_S3_PREFIX.rstrip("/")
    return bool(normalized_key) and (
        normalized_key == internal_prefix
        or normalized_key.startswith(INTERNAL_S3_PREFIX)
    )


def is_hidden_s3_key(key: str | None, data_exchange_prefix: str | None = None) -> bool:
    return is_internal_s3_key(key) or is_data_exchange_key(key or "", data_exchange_prefix)


def is_data_exchange_bucket_name(bucket_name: str) -> bool:
    normalized = str(bucket_name or "").strip().lower()
    if not normalized:
        return False
    return any(marker in normalized for marker in DATA_EXCHANGE_BUCKET_MARKERS)


def shared_notebooks_bucket_name(settings: Settings) -> str:
    explicit_bucket = str(getattr(settings, "shared_notebooks_bucket", "") or "").strip().lower()
    if explicit_bucket:
        return explicit_bucket

    base_bucket = str(getattr(settings, "s3_bucket", "") or "").strip()
    if not base_bucket:
        return ""
    return derived_s3_bucket_name(base_bucket, SHARED_NOTEBOOKS_BUCKET_SUFFIX)


def is_shared_notebooks_bucket_name(bucket_name: str, settings: Settings) -> bool:
    normalized = str(bucket_name or "").strip().lower()
    reserved_bucket = shared_notebooks_bucket_name(settings)
    return bool(normalized and reserved_bucket and normalized == reserved_bucket)


def is_hidden_s3_bucket_name(bucket_name: str, settings: Settings) -> bool:
    return is_data_exchange_bucket_name(bucket_name) or is_shared_notebooks_bucket_name(
        bucket_name,
        settings,
    )


def reject_hidden_s3_bucket(bucket_name: str, settings: Settings) -> None:
    if is_shared_notebooks_bucket_name(bucket_name, settings):
        raise ValueError(
            "The shared notebook storage bucket is reserved and cannot be used from Shared Workspace."
        )
    if is_data_exchange_bucket_name(bucket_name):
        raise ValueError(
            "DataExchange buckets are reserved and cannot be used from Shared Workspace."
        )


def reject_hidden_s3_location(
    bucket_name: str,
    key: str | None,
    settings: Settings,
    *,
    data_exchange_prefix: str | None = None,
) -> None:
    reject_hidden_s3_bucket(bucket_name, settings)
    if is_internal_s3_key(key):
        raise ValueError(
            "Internal Workbench S3 locations are reserved and cannot be used from Shared Workspace."
        )
    if is_data_exchange_key(key or "", data_exchange_prefix):
        raise ValueError(
            "DataExchange S3 locations are reserved and must be used from the DataExchange Workbench."
        )

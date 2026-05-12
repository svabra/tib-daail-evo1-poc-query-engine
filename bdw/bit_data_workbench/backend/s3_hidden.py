from __future__ import annotations

from ..config import Settings
from .s3_storage import derived_s3_bucket_name


DATA_EXCHANGE_BUCKET_MARKERS = ("--data-exchange--", "data-exchange")
SHARED_NOTEBOOKS_BUCKET_SUFFIX = "shared-notebooks"


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

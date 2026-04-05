from __future__ import annotations

import re
from pathlib import PurePosixPath

from ..config import Settings
from ..models import S3ExplorerBreadcrumb, S3ExplorerEntry, S3ExplorerSnapshot
from .s3_storage import ensure_s3_bucket, list_s3_buckets_from_client, s3_client


BUCKET_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def normalize_s3_prefix(prefix: str | None) -> str:
    raw_value = str(prefix or "").strip().replace("\\", "/")
    parts = [segment.strip() for segment in raw_value.split("/") if segment.strip()]
    return "/".join(parts) + ("/" if parts else "")


def normalize_s3_folder_name(name: str) -> str:
    raw_value = str(name or "").strip().replace("\\", "/")
    parts = [segment.strip() for segment in raw_value.split("/") if segment.strip()]
    if not parts:
        raise ValueError("Provide a folder name.")
    return "/".join(parts)


def normalize_s3_bucket_name(bucket_name: str) -> str:
    normalized = str(bucket_name or "").strip().lower()
    if not BUCKET_NAME_PATTERN.fullmatch(normalized):
        raise ValueError(
            "Bucket names must be 3-63 characters and use lowercase letters, numbers, dots, or hyphens."
        )
    return normalized


def s3_path(bucket: str, prefix: str = "") -> str:
    normalized_prefix = normalize_s3_prefix(prefix)
    return f"s3://{bucket}/{normalized_prefix}" if normalized_prefix else f"s3://{bucket}/"


class S3ExplorerManager:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def snapshot(self, *, bucket: str = "", prefix: str = "") -> S3ExplorerSnapshot:
        self._ensure_configured()
        normalized_bucket = str(bucket or "").strip()
        normalized_prefix = normalize_s3_prefix(prefix)
        client = s3_client(self._settings)

        if not normalized_bucket:
            bucket_entries = [
                S3ExplorerEntry(
                    entry_kind="bucket",
                    name=bucket_name,
                    bucket=bucket_name,
                    path=s3_path(bucket_name),
                    has_children=True,
                    selectable=True,
                )
                for bucket_name in list_s3_buckets_from_client(client)
            ]
            return S3ExplorerSnapshot(
                entries=bucket_entries,
                breadcrumbs=[S3ExplorerBreadcrumb(label="Buckets")],
                can_create_bucket=True,
                can_create_folder=False,
                empty_message="No buckets are available yet. Create one to start saving results.",
            )

        response = client.list_objects_v2(
            Bucket=normalized_bucket,
            Prefix=normalized_prefix,
            Delimiter="/",
            MaxKeys=1000,
        )
        folder_entries: list[S3ExplorerEntry] = []
        for item in response.get("CommonPrefixes") or []:
            child_prefix = normalize_s3_prefix(item.get("Prefix") or "")
            if not child_prefix:
                continue
            folder_name = PurePosixPath(child_prefix.rstrip("/")).name
            folder_entries.append(
                S3ExplorerEntry(
                    entry_kind="folder",
                    name=folder_name,
                    bucket=normalized_bucket,
                    prefix=child_prefix,
                    path=s3_path(normalized_bucket, child_prefix),
                    has_children=True,
                    selectable=True,
                )
            )

        file_entries: list[S3ExplorerEntry] = []
        for item in response.get("Contents") or []:
            key = str(item.get("Key") or "").strip()
            if not key or key == normalized_prefix or key.endswith("/"):
                continue
            if normalized_prefix and not key.startswith(normalized_prefix):
                continue
            relative_name = key[len(normalized_prefix) :] if normalized_prefix else key
            if "/" in relative_name:
                continue
            suffix = PurePosixPath(key).suffix.lstrip(".").lower()
            file_entries.append(
                S3ExplorerEntry(
                    entry_kind="file",
                    name=relative_name,
                    bucket=normalized_bucket,
                    prefix=key,
                    path=f"s3://{normalized_bucket}/{key}",
                    file_format=suffix,
                    size_bytes=int(item.get("Size") or 0),
                    has_children=False,
                    selectable=False,
                )
            )

        breadcrumbs = [S3ExplorerBreadcrumb(label="Buckets")]
        breadcrumbs.append(
            S3ExplorerBreadcrumb(
                label=normalized_bucket,
                bucket=normalized_bucket,
                prefix="",
                path=s3_path(normalized_bucket),
            )
        )
        current_prefix = ""
        for segment in [part for part in normalized_prefix.split("/") if part]:
            current_prefix = normalize_s3_prefix(f"{current_prefix}{segment}")
            breadcrumbs.append(
                S3ExplorerBreadcrumb(
                    label=segment,
                    bucket=normalized_bucket,
                    prefix=current_prefix,
                    path=s3_path(normalized_bucket, current_prefix),
                )
            )

        return S3ExplorerSnapshot(
            bucket=normalized_bucket,
            prefix=normalized_prefix,
            path=s3_path(normalized_bucket, normalized_prefix),
            entries=[
                *sorted(folder_entries, key=lambda item: item.name.lower()),
                *sorted(file_entries, key=lambda item: item.name.lower()),
            ],
            breadcrumbs=breadcrumbs,
            can_create_bucket=True,
            can_create_folder=True,
            empty_message=(
                "This location is empty. Create a folder here or save a result file into this location."
            ),
        )

    def create_bucket(self, bucket_name: str) -> S3ExplorerEntry:
        self._ensure_configured()
        normalized_bucket_name = normalize_s3_bucket_name(bucket_name)
        ensure_s3_bucket(self._settings, normalized_bucket_name)
        return S3ExplorerEntry(
            entry_kind="bucket",
            name=normalized_bucket_name,
            bucket=normalized_bucket_name,
            path=s3_path(normalized_bucket_name),
            has_children=True,
            selectable=True,
        )

    def create_folder(
        self,
        *,
        bucket: str,
        prefix: str = "",
        folder_name: str,
    ) -> S3ExplorerEntry:
        self._ensure_configured()
        normalized_bucket = str(bucket or "").strip()
        if not normalized_bucket:
            raise ValueError("Choose a bucket before creating a folder.")
        normalized_prefix = normalize_s3_prefix(prefix)
        normalized_folder_name = normalize_s3_folder_name(folder_name)
        child_prefix = normalize_s3_prefix(f"{normalized_prefix}{normalized_folder_name}")
        client = s3_client(self._settings)
        client.put_object(Bucket=normalized_bucket, Key=child_prefix, Body=b"")
        return S3ExplorerEntry(
            entry_kind="folder",
            name=PurePosixPath(child_prefix.rstrip("/")).name,
            bucket=normalized_bucket,
            prefix=child_prefix,
            path=s3_path(normalized_bucket, child_prefix),
            has_children=True,
            selectable=True,
        )

    def _ensure_configured(self) -> None:
        if not all(
            (
                self._settings.s3_endpoint,
                self._settings.current_s3_access_key_id(),
                self._settings.current_s3_secret_access_key(),
            )
        ):
            raise ValueError("S3 must be configured before browsing or saving result files.")

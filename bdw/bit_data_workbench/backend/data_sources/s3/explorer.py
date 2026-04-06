from __future__ import annotations

import mimetypes
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath

from ....config import Settings
from ....models import S3ExplorerBreadcrumb, S3ExplorerDeleteResult, S3ExplorerEntry, S3ExplorerSnapshot
from ...s3_storage import (
    delete_s3_bucket,
    delete_s3_object_versions,
    delete_s3_prefix,
    download_s3_file,
    ensure_s3_bucket,
    list_s3_buckets_from_client,
    remove_s3_bucket,
    s3_client,
)


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


def normalize_s3_object_key(key: str | None) -> str:
    raw_value = str(key or "").strip().replace("\\", "/")
    parts = [segment.strip() for segment in raw_value.split("/") if segment.strip()]
    normalized = "/".join(parts)
    if not normalized or normalized.endswith("/"):
        raise ValueError("Choose a concrete S3 object before downloading.")
    if any(token in normalized for token in "*?["):
        raise ValueError("Only single S3 objects can be downloaded.")
    return normalized


def normalize_s3_object_filename(file_name: str | None, *, fallback_key: str) -> str:
    candidate = str(file_name or "").strip()
    if candidate:
        candidate = PurePosixPath(candidate.replace("\\", "/")).name.strip()
    if not candidate:
        candidate = PurePosixPath(fallback_key).name.strip()
    if not candidate:
        raise ValueError("Could not determine a file name for the S3 object.")
    return candidate


@dataclass(slots=True)
class S3ObjectDownloadArtifact:
    local_path: Path
    cleanup_dir: Path
    filename: str
    content_type: str


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
            relative_name = key[len(normalized_prefix):] if normalized_prefix else key
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

    def download_object(
        self,
        *,
        bucket: str,
        key: str,
        file_name: str = "",
    ) -> S3ObjectDownloadArtifact:
        self._ensure_configured()
        normalized_bucket = normalize_s3_bucket_name(bucket)
        normalized_key = normalize_s3_object_key(key)
        filename = normalize_s3_object_filename(file_name, fallback_key=normalized_key)
        temp_dir = Path(tempfile.mkdtemp(prefix="bdw-s3-object-download-"))
        local_path = temp_dir / filename
        try:
            download_s3_file(
                s3_client(self._settings),
                bucket=normalized_bucket,
                key=normalized_key,
                local_path=local_path,
            )
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise

        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        return S3ObjectDownloadArtifact(
            local_path=local_path,
            cleanup_dir=temp_dir,
            filename=filename,
            content_type=content_type,
        )

    def delete_entry(
        self,
        *,
        entry_kind: str,
        bucket: str,
        prefix: str = "",
    ) -> S3ExplorerDeleteResult:
        self._ensure_configured()
        normalized_kind = str(entry_kind or "").strip().lower()
        normalized_bucket = normalize_s3_bucket_name(bucket)

        if normalized_kind == "file":
            normalized_key = normalize_s3_object_key(prefix)
            deleted_keys = delete_s3_object_versions(
                s3_client(self._settings),
                normalized_bucket,
                normalized_key,
            )
            return S3ExplorerDeleteResult(
                entry_kind="file",
                bucket=normalized_bucket,
                prefix=normalized_key,
                path=f"s3://{normalized_bucket}/{normalized_key}",
                deleted_keys=deleted_keys,
                message=f"Deleted S3 object s3://{normalized_bucket}/{normalized_key}.",
            )

        if normalized_kind == "folder":
            normalized_prefix = normalize_s3_prefix(prefix)
            if not normalized_prefix:
                raise ValueError("Choose a folder before deleting it.")
            deleted_keys = delete_s3_prefix(self._settings, normalized_bucket, normalized_prefix)
            return S3ExplorerDeleteResult(
                entry_kind="folder",
                bucket=normalized_bucket,
                prefix=normalized_prefix,
                path=s3_path(normalized_bucket, normalized_prefix),
                deleted_keys=deleted_keys,
                message=(
                    f"Deleted {deleted_keys} object(s) from {s3_path(normalized_bucket, normalized_prefix)}."
                ),
            )

        if normalized_kind == "bucket":
            deleted_keys = delete_s3_bucket(self._settings, normalized_bucket)
            bucket_deleted = remove_s3_bucket(self._settings, normalized_bucket)
            if not bucket_deleted:
                raise ValueError(
                    f"Failed to delete S3 bucket '{normalized_bucket}'. "
                    "Technical detail: object cleanup finished, but the "
                    "object store still reported the bucket as not yet "
                    "deletable."
                )
            return S3ExplorerDeleteResult(
                entry_kind="bucket",
                bucket=normalized_bucket,
                path=s3_path(normalized_bucket),
                deleted_keys=deleted_keys,
                bucket_deleted=True,
                message=(
                    f"Deleted bucket {normalized_bucket} and {deleted_keys} contained object(s)."
                ),
            )

        raise ValueError("Unsupported S3 explorer entry type.")

    def _ensure_configured(self) -> None:
        if not all(
            (
                self._settings.s3_endpoint,
                self._settings.current_s3_access_key_id(),
                self._settings.current_s3_secret_access_key(),
            )
        ):
            raise ValueError("S3 must be configured before browsing or saving result files.")

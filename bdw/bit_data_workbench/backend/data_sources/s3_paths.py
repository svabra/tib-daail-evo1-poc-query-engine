from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlparse

from ...models import SourceObject
from ..source_references import split_qualified_reference


S3_WILDCARD_TOKENS = "*?["


@dataclass(frozen=True, slots=True)
class S3SourceObjectLocation:
    bucket: str
    key: str
    logical_key: str
    logical_prefix: str
    leaf_label: str
    is_wildcard: bool


def source_object_s3_location(source_object: SourceObject | object) -> S3SourceObjectLocation | None:
    bucket = str(getattr(source_object, "s3_bucket", "") or "").strip()
    key = str(getattr(source_object, "s3_key", "") or "").strip().lstrip("/")

    reference_bucket, reference_key = _bucket_key_from_reference(
        str(getattr(source_object, "query_reference", "") or "")
    )
    if not key and reference_key:
        key = reference_key
    if not bucket and reference_bucket:
        bucket = reference_bucket

    path_bucket, path_key = _bucket_key_from_s3_path(
        str(getattr(source_object, "s3_path", "") or "")
    )
    if not key and path_key:
        key = path_key
    if not bucket and path_bucket:
        bucket = path_bucket

    if not bucket or not key:
        return None

    logical_key, leaf_label, is_wildcard = _logical_key_parts(key)
    if not logical_key or not leaf_label:
        return None

    logical_prefix = f"{logical_key.rstrip('/')}/"
    return S3SourceObjectLocation(
        bucket=bucket,
        key=key,
        logical_key=logical_key,
        logical_prefix=logical_prefix,
        leaf_label=leaf_label,
        is_wildcard=is_wildcard,
    )


def _bucket_key_from_reference(value: str) -> tuple[str, str]:
    parts = split_qualified_reference(value)
    if len(parts) == 3 and parts[0].strip().lower() == "s3":
        return parts[1].strip(), parts[2].strip().lstrip("/")
    if (
        len(parts) == 4
        and parts[0].strip().lower() == "workspace"
        and parts[1].strip().lower() == "s3"
    ):
        return parts[2].strip(), parts[3].strip().lstrip("/")
    return "", ""


def _bucket_key_from_s3_path(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    if not text.lower().startswith("s3://"):
        return "", ""
    parsed = urlparse(text)
    bucket = parsed.netloc.strip()
    key = unquote(parsed.path or "").lstrip("/")
    return bucket, key


def _logical_key_parts(key: str) -> tuple[str, str, bool]:
    segments = [segment for segment in str(key or "").strip("/").split("/") if segment]
    if not segments:
        return "", "", False

    wildcard_index = next(
        (
            index
            for index, segment in enumerate(segments)
            if any(token in segment for token in S3_WILDCARD_TOKENS)
        ),
        -1,
    )
    is_wildcard = wildcard_index >= 0
    logical_segments = segments[:wildcard_index] if is_wildcard else segments
    if not logical_segments:
        logical_segments = segments

    logical_key = "/".join(logical_segments).strip("/")
    return logical_key, logical_segments[-1], is_wildcard

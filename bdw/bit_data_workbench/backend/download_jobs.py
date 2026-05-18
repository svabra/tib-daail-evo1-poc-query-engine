from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import logging
import mimetypes
from pathlib import PurePosixPath
import re
from threading import RLock, Semaphore, Thread
import time
from typing import Any, Callable
from uuid import uuid4
import zipfile

from ..config import Settings
from .data_sources.s3.explorer import (
    normalize_s3_object_filename,
    normalize_s3_object_key,
    normalize_s3_storage_bucket_name,
)
from .s3_hidden import DOWNLOAD_ARTIFACT_S3_PREFIX, DOWNLOAD_JOB_S3_PREFIX, reject_hidden_s3_location
from .s3_storage import ensure_s3_bucket, iter_s3_keys, s3_client


logger = logging.getLogger(__name__)

DOWNLOAD_RUNNING_STATUSES = {"queued", "running"}
DOWNLOAD_TERMINAL_STATUSES = {"ready", "failed", "cancelled", "expired"}
DOWNLOAD_REUSABLE_STATUSES = {"queued", "running", "ready"}
DOWNLOAD_SUPPORTED_FORMATS = {"csv"}
RANGE_HEADER_PATTERN = re.compile(r"^bytes=(\d*)-(\d*)$")


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def parse_datetime(value: object) -> datetime | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    if raw_value.endswith("Z"):
        raw_value = f"{raw_value[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _safe_zip_filename(filename: str, *, fallback: str) -> str:
    candidate = PurePosixPath(str(filename or "").replace("\\", "/")).name.strip()
    if not candidate:
        candidate = PurePosixPath(str(fallback or "").replace("\\", "/")).name.strip()
    return candidate or "download.csv"


def _zip_artifact_filename(filename: str) -> str:
    base_name = PurePosixPath(filename).name.strip() or "download.csv"
    if base_name.lower().endswith(".zip"):
        return base_name
    return f"{base_name}.zip"


def _normalize_csv_format(file_format: str | None, *, filename: str, key: str = "") -> str:
    normalized_format = str(file_format or "").strip().lower().lstrip(".")
    if not normalized_format:
        normalized_format = PurePosixPath(filename or key).suffix.lstrip(".").lower()
    if normalized_format not in DOWNLOAD_SUPPORTED_FORMATS:
        raise ValueError("Prepared ZIP downloads currently support CSV files.")
    return normalized_format


def _strip_etag(value: object) -> str:
    return str(value or "").strip().strip('"')


@dataclass(slots=True)
class DownloadArtifactStream:
    body: Any
    filename: str
    content_type: str
    content_length: int
    status_code: int
    headers: dict[str, str]


class DownloadRangeNotSatisfiable(ValueError):
    def __init__(self, artifact_size: int) -> None:
        super().__init__("Requested byte range is not satisfiable.")
        self.content_range = f"bytes */{max(0, int(artifact_size or 0))}"


class MultipartS3UploadWriter:
    def __init__(
        self,
        *,
        client: Any,
        bucket: str,
        key: str,
        part_size: int,
        content_type: str,
        metadata: dict[str, str],
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._key = key
        self._part_size = max(5 * 1024 * 1024, int(part_size or 0))
        self._content_type = content_type
        self._metadata = metadata
        self._upload_id: str | None = None
        self._part_number = 1
        self._parts: list[dict[str, object]] = []
        self._buffer = bytearray()
        self._position = 0
        self._closed = False
        self._aborted = False
        self._completed = False

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._position

    def write(self, data: bytes | bytearray | memoryview) -> int:
        if self._closed:
            raise ValueError("Cannot write to a closed S3 upload writer.")
        payload = bytes(data)
        if not payload:
            return 0
        self._ensure_upload()
        self._buffer.extend(payload)
        self._position += len(payload)
        while len(self._buffer) >= self._part_size:
            self._upload_buffer_part(self._part_size)
        return len(payload)

    def flush(self) -> None:
        return None

    def complete(self) -> None:
        if self._completed:
            return
        if self._closed:
            raise ValueError("Cannot complete a closed S3 upload writer.")
        self._ensure_upload()
        if self._buffer:
            self._upload_buffer_part(len(self._buffer))
        self._client.complete_multipart_upload(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
            MultipartUpload={"Parts": self._parts},
        )
        self._completed = True
        self._closed = True

    def abort(self) -> None:
        if self._aborted or self._completed:
            return
        self._aborted = True
        self._closed = True
        if not self._upload_id:
            return
        try:
            self._client.abort_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
            )
        except Exception:
            logger.exception("Failed to abort prepared-download multipart upload.")

    def close(self) -> None:
        if self._closed:
            return
        self.complete()

    def _ensure_upload(self) -> None:
        if self._upload_id:
            return
        response = self._client.create_multipart_upload(
            Bucket=self._bucket,
            Key=self._key,
            ContentType=self._content_type,
            Metadata=self._metadata,
        )
        self._upload_id = str(response.get("UploadId") or "").strip()
        if not self._upload_id:
            raise ValueError("S3 did not return a multipart upload id.")

    def _upload_buffer_part(self, length: int) -> None:
        body = bytes(self._buffer[:length])
        del self._buffer[:length]
        response = self._client.upload_part(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
            PartNumber=self._part_number,
            Body=body,
        )
        etag = response.get("ETag")
        if not etag:
            raise ValueError("S3 did not return an ETag for an uploaded part.")
        self._parts.append({"PartNumber": self._part_number, "ETag": etag})
        self._part_number += 1


class DownloadJobManager:
    def __init__(
        self,
        *,
        settings: Settings,
        state_change_callback: Callable[[dict[str, Any]], None] | None = None,
        s3_client_factory: Callable[[Settings], Any] = s3_client,
        ensure_bucket_factory: Callable[[Settings, str], None] = ensure_s3_bucket,
        iter_keys_factory: Callable[[Any, str, str], Any] = iter_s3_keys,
    ) -> None:
        self._settings = settings
        self._state_change_callback = state_change_callback
        self._s3_client_factory = s3_client_factory
        self._ensure_bucket_factory = ensure_bucket_factory
        self._iter_keys_factory = iter_keys_factory
        self._lock = RLock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._version = 0
        self._semaphore = Semaphore(max(1, int(settings.download_max_concurrent_jobs or 1)))
        self._bucket_ensured = False
        self._load_existing_jobs()

    def state_payload(self) -> dict[str, Any]:
        snapshot_to_emit: dict[str, Any] | None = None
        with self._lock:
            changed_jobs = self._expire_ready_jobs_locked(utc_now())
            if changed_jobs:
                self._version += 1
                for job in changed_jobs:
                    self._persist_job_locked(job)
                snapshot_to_emit = self._state_payload_locked()
            snapshot = self._state_payload_locked()
        if snapshot_to_emit is not None:
            self._emit_state(snapshot_to_emit)
        return snapshot

    def job_payload(self, job_id: str) -> dict[str, Any]:
        snapshot_to_emit: dict[str, Any] | None = None
        with self._lock:
            job = self._require_job_locked(job_id)
            changed_jobs = self._expire_ready_jobs_locked(utc_now(), candidates=[job])
            if changed_jobs:
                self._version += 1
                for changed_job in changed_jobs:
                    self._persist_job_locked(changed_job)
                snapshot_to_emit = self._state_payload_locked()
            payload = self._public_payload(job)
        self._emit_state(snapshot_to_emit)
        return payload

    def start_s3_job(
        self,
        *,
        bucket: str,
        key: str,
        filename: str = "",
        file_format: str = "",
    ) -> dict[str, Any]:
        normalized_bucket = normalize_s3_storage_bucket_name(bucket)
        normalized_key = normalize_s3_object_key(key)
        reject_hidden_s3_location(
            normalized_bucket,
            normalized_key,
            self._settings,
            data_exchange_prefix=self._settings.data_exchange_prefix,
        )
        source_filename = normalize_s3_object_filename(filename, fallback_key=normalized_key)
        normalized_format = _normalize_csv_format(
            file_format,
            filename=source_filename,
            key=normalized_key,
        )
        return self.start_prepared_source(
            {
                "sourceKind": "s3_object",
                "bucket": normalized_bucket,
                "key": normalized_key,
                "filename": source_filename,
                "sourceName": source_filename,
                "sourceSizeBytes": 0,
                "sourceRevision": f"unverified:{normalized_bucket}:{normalized_key}",
                "format": normalized_format,
            }
        )

    def start_prepared_source(self, source: dict[str, Any]) -> dict[str, Any]:
        normalized_source = self._normalize_source(source)
        snapshot_to_emit: dict[str, Any] | None = None
        thread: Thread | None = None
        with self._lock:
            now = utc_now()
            changed_jobs = self._expire_ready_jobs_locked(now)
            for job in self._jobs.values():
                if job.get("sourceFingerprint") != normalized_source["sourceFingerprint"]:
                    continue
                if job.get("status") in DOWNLOAD_REUSABLE_STATUSES and not self._job_expired(job, now):
                    return self._public_payload(job)

            job = self._new_job_locked(normalized_source, now)
            self._jobs[job["jobId"]] = job
            self._version += 1
            for changed_job in changed_jobs:
                self._persist_job_locked(changed_job)
            self._persist_job_locked(job)
            snapshot_to_emit = self._state_payload_locked()
            thread = Thread(target=self._run_job, args=(job["jobId"],), daemon=True)
            payload = self._public_payload(job)

        self._emit_state(snapshot_to_emit)
        thread.start()
        return payload

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        snapshot_to_emit: dict[str, Any] | None = None
        with self._lock:
            job = self._require_job_locked(job_id)
            if job.get("status") not in DOWNLOAD_RUNNING_STATUSES:
                return self._public_payload(job)
            job["cancelRequested"] = True
            job["status"] = "cancelled"
            job["message"] = "Download preparation was cancelled."
            job["completedAt"] = isoformat(utc_now())
            job["canCancel"] = False
            self._touch_job_locked(job)
            self._version += 1
            self._persist_job_locked(job)
            snapshot_to_emit = self._state_payload_locked()
            payload = self._public_payload(job)
        self._emit_state(snapshot_to_emit)
        return payload

    def artifact_stream(
        self,
        *,
        job_id: str,
        token: str,
        range_header: str = "",
    ) -> DownloadArtifactStream:
        with self._lock:
            job = self._require_job_locked(job_id)
            if str(job.get("token") or "") != str(token or "").strip():
                raise PermissionError("The prepared download token is invalid.")
            now = utc_now()
            if self._job_expired(job, now):
                job["status"] = "expired"
                job["message"] = "The prepared download artifact has expired."
                job["canCancel"] = False
                self._touch_job_locked(job)
                self._version += 1
                self._persist_job_locked(job)
                snapshot = self._state_payload_locked()
                self._emit_state(snapshot)
                raise ValueError("The prepared download artifact has expired.")
            if job.get("status") != "ready":
                raise ValueError("The prepared download artifact is not ready yet.")

            artifact_size = int(job.get("artifactSizeBytes") or 0)
            artifact_key = str(job.get("artifactKey") or "")
            artifact_filename = str(job.get("artifactFilename") or "download.zip")
            etag = str(job.get("artifactEtag") or "").strip()

        byte_range = self._parse_range_header(range_header, artifact_size)
        client = self._client()
        kwargs: dict[str, Any] = {
            "Bucket": self._artifact_bucket(),
            "Key": artifact_key,
        }
        status_code = 200
        content_length = artifact_size
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(artifact_size),
        }
        if etag:
            headers["ETag"] = etag

        if byte_range is not None:
            start, end = byte_range
            kwargs["Range"] = f"bytes={start}-{end}"
            status_code = 206
            content_length = end - start + 1
            headers["Content-Length"] = str(content_length)
            headers["Content-Range"] = f"bytes {start}-{end}/{artifact_size}"

        response = client.get_object(**kwargs)
        return DownloadArtifactStream(
            body=response["Body"],
            filename=artifact_filename,
            content_type=str(response.get("ContentType") or "application/zip"),
            content_length=content_length,
            status_code=status_code,
            headers=headers,
        )

    def _run_job(self, job_id: str) -> None:
        acquired = False
        try:
            self._semaphore.acquire()
            acquired = True
            if not self._mark_running(job_id):
                return
            artifact_size, artifact_etag = self._prepare_zip_artifact(job_id)
            self._mark_ready(job_id, artifact_size=artifact_size, artifact_etag=artifact_etag)
        except _DownloadJobCancelled:
            self._mark_cancelled(job_id)
        except Exception as exc:
            logger.exception("Prepared download job failed: %s", job_id)
            self._mark_failed(job_id, str(exc) or "Download preparation failed.")
        finally:
            if acquired:
                self._semaphore.release()

    def _prepare_zip_artifact(self, job_id: str) -> tuple[int, str]:
        with self._lock:
            job = dict(self._require_job_locked(job_id))
        client = self._client()
        metadata = client.head_object(
            Bucket=str(job["sourceBucket"]),
            Key=str(job["sourceKey"]),
        )
        source_size = int(metadata.get("ContentLength") or 0)
        etag = _strip_etag(metadata.get("ETag"))
        if not etag:
            etag = str(metadata.get("LastModified") or "")
        self._update_source_metadata(
            job_id,
            source_size=source_size,
            source_revision=f"{etag}:{source_size}",
        )
        self._raise_if_cancelled(job_id)
        writer = MultipartS3UploadWriter(
            client=client,
            bucket=self._artifact_bucket(),
            key=str(job["artifactKey"]),
            part_size=int(self._settings.download_multipart_chunk_bytes),
            content_type="application/zip",
            metadata={
                "bdw-job-id": str(job_id),
                "bdw-source-kind": str(job.get("sourceKind") or ""),
            },
        )
        source_response = client.get_object(
            Bucket=str(job["sourceBucket"]),
            Key=str(job["sourceKey"]),
        )
        source_body = source_response["Body"]
        bytes_processed = int(job.get("bytesProcessed") or 0)
        source_size = max(0, source_size)
        last_emit = time.monotonic()
        last_emit_bytes = 0
        try:
            with zipfile.ZipFile(
                writer,
                "w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=int(self._settings.download_compression_level),
                allowZip64=True,
            ) as archive:
                zip_name = _safe_zip_filename(
                    str(job.get("sourceName") or ""),
                    fallback=str(job.get("sourceKey") or ""),
                )
                with archive.open(zip_name, "w", force_zip64=True) as member:
                    while True:
                        self._raise_if_cancelled(job_id)
                        chunk = source_body.read(1024 * 1024)
                        if not chunk:
                            break
                        member.write(chunk)
                        bytes_processed += len(chunk)
                        now = time.monotonic()
                        if (
                            now - last_emit >= 1.0
                            or bytes_processed - last_emit_bytes >= int(self._settings.download_multipart_chunk_bytes)
                            or bytes_processed >= source_size
                        ):
                            self._update_progress(job_id, bytes_processed=bytes_processed)
                            last_emit = now
                            last_emit_bytes = bytes_processed
            self._raise_if_cancelled(job_id)
            writer.complete()
        except Exception:
            writer.abort()
            raise
        finally:
            try:
                source_body.close()
            except Exception:
                pass

        artifact_size = writer.tell()
        artifact_etag = ""
        try:
            head = client.head_object(
                Bucket=self._artifact_bucket(),
                Key=str(job["artifactKey"]),
            )
            artifact_size = int(head.get("ContentLength") or artifact_size)
            artifact_etag = str(head.get("ETag") or "").strip()
        except Exception:
            logger.debug("Could not head prepared download artifact %s.", job_id, exc_info=True)
        return artifact_size, artifact_etag

    def _mark_running(self, job_id: str) -> bool:
        snapshot: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.get("status") == "cancelled" or job.get("cancelRequested"):
                return False
            job["status"] = "running"
            job["startedAt"] = job.get("startedAt") or isoformat(utc_now())
            job["message"] = "Preparing ZIP download."
            job["canCancel"] = True
            self._touch_job_locked(job)
            self._version += 1
            self._persist_job_locked(job)
            snapshot = self._state_payload_locked()
        self._emit_state(snapshot)
        return True

    def _mark_ready(self, job_id: str, *, artifact_size: int, artifact_etag: str) -> None:
        snapshot: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.get("status") == "cancelled" or job.get("cancelRequested"):
                return
            completed_at = utc_now()
            expires_at = completed_at + timedelta(hours=max(1, int(self._settings.download_artifact_ttl_hours)))
            job["status"] = "ready"
            job["progress"] = 1.0
            job["bytesProcessed"] = job.get("sourceSizeBytes") or 0
            job["artifactSizeBytes"] = int(artifact_size or 0)
            job["artifactEtag"] = artifact_etag
            job["completedAt"] = isoformat(completed_at)
            job["expiresAt"] = isoformat(expires_at)
            job["message"] = "Prepared ZIP download is ready."
            job["canCancel"] = False
            self._touch_job_locked(job)
            self._version += 1
            self._persist_job_locked(job)
            snapshot = self._state_payload_locked()
        self._emit_state(snapshot)

    def _update_source_metadata(
        self,
        job_id: str,
        *,
        source_size: int,
        source_revision: str,
    ) -> None:
        snapshot: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.get("status") not in DOWNLOAD_RUNNING_STATUSES:
                return
            job["sourceSizeBytes"] = max(0, int(source_size or 0))
            job["sourceRevision"] = str(source_revision or job["sourceSizeBytes"])
            job["sourceFingerprint"] = "|".join(
                [
                    str(job.get("sourceKind") or ""),
                    str(job.get("dataExchangeFileId") or job.get("sourceBucket") or ""),
                    str(job.get("sourceKey") or ""),
                    str(job.get("sourceRevision") or ""),
                    str(job.get("sourceSizeBytes") or 0),
                ]
            )
            self._touch_job_locked(job)
            self._version += 1
            self._persist_job_locked(job)
            snapshot = self._state_payload_locked()
        self._emit_state(snapshot)

    def _mark_failed(self, job_id: str, message: str) -> None:
        snapshot: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.get("status") in {"cancelled", "ready"}:
                return
            job["status"] = "failed"
            job["message"] = message
            job["completedAt"] = isoformat(utc_now())
            job["canCancel"] = False
            self._touch_job_locked(job)
            self._version += 1
            self._persist_job_locked(job)
            snapshot = self._state_payload_locked()
        self._emit_state(snapshot)

    def _mark_cancelled(self, job_id: str) -> None:
        snapshot: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job["status"] = "cancelled"
            job["message"] = "Download preparation was cancelled."
            job["completedAt"] = isoformat(utc_now())
            job["canCancel"] = False
            self._touch_job_locked(job)
            self._version += 1
            self._persist_job_locked(job)
            snapshot = self._state_payload_locked()
        self._emit_state(snapshot)

    def _update_progress(self, job_id: str, *, bytes_processed: int) -> None:
        snapshot: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.get("status") != "running":
                return
            source_size = max(0, int(job.get("sourceSizeBytes") or 0))
            job["bytesProcessed"] = int(bytes_processed)
            job["progress"] = min(0.99, bytes_processed / source_size) if source_size else 0.0
            job["message"] = "Preparing ZIP download."
            self._touch_job_locked(job)
            self._version += 1
            self._persist_job_locked(job)
            snapshot = self._state_payload_locked()
        self._emit_state(snapshot)

    def _raise_if_cancelled(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.get("cancelRequested") or job.get("status") == "cancelled":
                raise _DownloadJobCancelled()

    def _new_job_locked(self, source: dict[str, Any], now: datetime) -> dict[str, Any]:
        job_id = f"download-{uuid4().hex}"
        source_name = str(source["sourceName"])
        artifact_filename = _zip_artifact_filename(source_name)
        artifact_key = (
            f"{DOWNLOAD_ARTIFACT_S3_PREFIX}"
            f"{now:%Y/%m}/{job_id}/{artifact_filename}"
        )
        return {
            "jobId": job_id,
            "sourceKind": source["sourceKind"],
            "sourceBucket": source["bucket"],
            "sourceKey": source["key"],
            "sourceName": source_name,
            "sourceSizeBytes": int(source["sourceSizeBytes"]),
            "sourceRevision": source["sourceRevision"],
            "sourceFingerprint": source["sourceFingerprint"],
            "dataExchangeFileId": source.get("dataExchangeFileId") or "",
            "format": source["format"],
            "status": "queued",
            "progress": 0.0,
            "message": "Queued for ZIP preparation.",
            "bytesProcessed": 0,
            "artifactSizeBytes": None,
            "artifactFilename": artifact_filename,
            "artifactBucket": self._artifact_bucket(),
            "artifactKey": artifact_key,
            "artifactEtag": "",
            "manifestKey": f"{DOWNLOAD_JOB_S3_PREFIX}{job_id}.json",
            "token": uuid4().hex,
            "startedAt": None,
            "completedAt": None,
            "expiresAt": None,
            "createdAt": isoformat(now),
            "updatedAt": isoformat(now),
            "canCancel": True,
            "cancelRequested": False,
        }

    def _normalize_source(self, source: dict[str, Any]) -> dict[str, Any]:
        source_kind = str(source.get("sourceKind") or "").strip()
        if source_kind not in {"s3_object", "data_exchange_file"}:
            raise ValueError("Unsupported prepared download source.")
        bucket = str(source.get("bucket") or "").strip()
        key = normalize_s3_object_key(source.get("key"))
        filename = normalize_s3_object_filename(source.get("filename") or source.get("sourceName"), fallback_key=key)
        file_format = _normalize_csv_format(source.get("format"), filename=filename, key=key)
        source_size = max(0, int(source.get("sourceSizeBytes") or 0))
        source_revision = str(source.get("sourceRevision") or source_size).strip()
        if not bucket:
            raise ValueError("A prepared download source bucket is required.")
        if source_kind == "s3_object":
            reject_hidden_s3_location(
                bucket,
                key,
                self._settings,
                data_exchange_prefix=self._settings.data_exchange_prefix,
            )
        data_exchange_file_id = str(source.get("dataExchangeFileId") or source.get("fileId") or "").strip()
        fingerprint_parts = [
            source_kind,
            data_exchange_file_id if source_kind == "data_exchange_file" else bucket,
            key,
            source_revision,
            str(source_size),
        ]
        return {
            "sourceKind": source_kind,
            "bucket": bucket,
            "key": key,
            "filename": filename,
            "sourceName": str(source.get("sourceName") or filename).strip() or filename,
            "sourceSizeBytes": source_size,
            "sourceRevision": source_revision,
            "sourceFingerprint": "|".join(fingerprint_parts),
            "dataExchangeFileId": data_exchange_file_id,
            "format": file_format,
        }

    def _public_payload(self, job: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "jobId": job.get("jobId"),
            "sourceKind": job.get("sourceKind"),
            "status": job.get("status"),
            "progress": job.get("progress"),
            "message": job.get("message"),
            "sourceName": job.get("sourceName"),
            "bytesProcessed": job.get("bytesProcessed"),
            "sourceSizeBytes": job.get("sourceSizeBytes"),
            "artifactSizeBytes": job.get("artifactSizeBytes"),
            "artifactFilename": job.get("artifactFilename"),
            "startedAt": job.get("startedAt"),
            "completedAt": job.get("completedAt"),
            "expiresAt": job.get("expiresAt"),
            "createdAt": job.get("createdAt"),
            "updatedAt": job.get("updatedAt"),
            "canCancel": job.get("canCancel"),
            "dataExchangeFileId": job.get("dataExchangeFileId") or "",
            "sourceBucket": job.get("sourceBucket"),
            "sourceKey": job.get("sourceKey"),
            "sourceFingerprint": job.get("sourceFingerprint"),
        }
        if job.get("status") == "ready":
            payload["downloadUrl"] = (
                f"/api/download-jobs/{job.get('jobId')}/artifact?token={job.get('token')}"
            )
        else:
            payload["downloadUrl"] = ""
        return payload

    def _state_payload_locked(self) -> dict[str, Any]:
        jobs = sorted(
            (self._public_payload(job) for job in self._jobs.values()),
            key=lambda job: str(job.get("updatedAt") or job.get("createdAt") or ""),
            reverse=True,
        )
        running_count = sum(1 for job in jobs if job.get("status") in DOWNLOAD_RUNNING_STATUSES)
        ready_count = sum(1 for job in jobs if job.get("status") == "ready")
        return {
            "version": self._version,
            "summary": {
                "runningCount": running_count,
                "readyCount": ready_count,
                "totalCount": len(jobs),
            },
            "jobs": jobs,
        }

    def _persist_job_locked(self, job: dict[str, Any]) -> None:
        try:
            self._ensure_internal_bucket()
            payload = json.dumps(job, sort_keys=True).encode("utf-8")
            self._client().put_object(
                Bucket=self._artifact_bucket(),
                Key=str(job.get("manifestKey") or f"{DOWNLOAD_JOB_S3_PREFIX}{job['jobId']}.json"),
                Body=payload,
                ContentType="application/json",
            )
        except Exception:
            logger.exception("Failed to persist prepared download job manifest.")

    def _load_existing_jobs(self) -> None:
        try:
            bucket = self._artifact_bucket()
        except ValueError:
            return
        if not bucket:
            return
        now = utc_now()
        changed_jobs: list[dict[str, Any]] = []
        try:
            client = self._client()
            for key in self._iter_keys_factory(client, bucket, DOWNLOAD_JOB_S3_PREFIX):
                try:
                    response = client.get_object(Bucket=bucket, Key=key)
                    body = response["Body"]
                    try:
                        payload = json.loads(body.read().decode("utf-8"))
                    finally:
                        try:
                            body.close()
                        except Exception:
                            pass
                    if not isinstance(payload, dict) or not payload.get("jobId"):
                        continue
                    payload.setdefault("manifestKey", key)
                    payload.setdefault("token", uuid4().hex)
                    if payload.get("status") in DOWNLOAD_RUNNING_STATUSES:
                        payload["status"] = "failed"
                        payload["message"] = "Download preparation was interrupted by a service restart."
                        payload["completedAt"] = isoformat(now)
                        payload["canCancel"] = False
                        payload["cancelRequested"] = False
                        payload["updatedAt"] = isoformat(now)
                        changed_jobs.append(payload)
                    self._jobs[str(payload["jobId"])] = payload
                except Exception:
                    logger.exception("Failed to load prepared download job manifest %s.", key)
            if self._jobs:
                self._version = 1
            if changed_jobs:
                self._version += 1
                for job in changed_jobs:
                    self._persist_job_locked(job)
        except Exception:
            logger.exception("Failed to load prepared download job manifests.")

    def _expire_ready_jobs_locked(
        self,
        now: datetime,
        *,
        candidates: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        changed: list[dict[str, Any]] = []
        for job in candidates or list(self._jobs.values()):
            if job.get("status") != "ready" or not self._job_expired(job, now):
                continue
            job["status"] = "expired"
            job["message"] = "The prepared download artifact has expired."
            job["canCancel"] = False
            self._touch_job_locked(job, now=now)
            changed.append(job)
        return changed

    def _job_expired(self, job: dict[str, Any], now: datetime) -> bool:
        expires_at = parse_datetime(job.get("expiresAt"))
        return bool(expires_at and expires_at <= now)

    def _touch_job_locked(self, job: dict[str, Any], *, now: datetime | None = None) -> None:
        job["updatedAt"] = isoformat(now or utc_now())

    def _require_job_locked(self, job_id: str) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        job = self._jobs.get(normalized_job_id)
        if not job:
            raise KeyError("Prepared download job not found.")
        return job

    def _parse_range_header(self, range_header: str, artifact_size: int) -> tuple[int, int] | None:
        normalized = str(range_header or "").strip()
        if not normalized:
            return None
        match = RANGE_HEADER_PATTERN.fullmatch(normalized)
        if not match:
            raise DownloadRangeNotSatisfiable(artifact_size)
        start_text, end_text = match.groups()
        if not start_text and not end_text:
            raise DownloadRangeNotSatisfiable(artifact_size)
        if artifact_size <= 0:
            raise DownloadRangeNotSatisfiable(artifact_size)
        if not start_text:
            suffix_length = int(end_text)
            if suffix_length <= 0:
                raise DownloadRangeNotSatisfiable(artifact_size)
            return max(artifact_size - suffix_length, 0), artifact_size - 1
        start = int(start_text)
        end = int(end_text) if end_text else artifact_size - 1
        if start >= artifact_size or end < start:
            raise DownloadRangeNotSatisfiable(artifact_size)
        return start, min(end, artifact_size - 1)

    def _client(self) -> Any:
        return self._s3_client_factory(self._settings)

    def _artifact_bucket(self) -> str:
        bucket = str(self._settings.s3_bucket or "").strip()
        if not bucket:
            raise ValueError("S3 bucket configuration is required for prepared downloads.")
        return bucket

    def _ensure_internal_bucket(self) -> None:
        if self._bucket_ensured:
            return
        self._ensure_bucket_factory(self._settings, self._artifact_bucket())
        self._bucket_ensured = True

    def _emit_state(self, snapshot: dict[str, Any] | None) -> None:
        if snapshot is None or self._state_change_callback is None:
            return
        try:
            self._state_change_callback(snapshot)
        except Exception:
            logger.exception("Failed to publish prepared download job state.")


class _DownloadJobCancelled(Exception):
    pass

from __future__ import annotations

import mimetypes
from pathlib import PurePosixPath
import threading
import time
import uuid
import zipfile
from datetime import UTC, datetime
from typing import Any, Callable

from ..config import Settings
from .data_exchange import is_data_exchange_key
from .data_sources.s3.explorer import (
    normalize_s3_bucket_name,
    normalize_s3_object_filename,
    normalize_s3_prefix,
)
from .s3_hidden import reject_hidden_s3_bucket
from .s3_storage import iter_s3_keys, s3_client


RUNNING_DOWNLOAD_STATUSES = {"queued", "running"}
TERMINAL_DOWNLOAD_STATUSES = {"completed", "failed", "cancelled"}
DOWNLOAD_PROGRESS_POLL_BYTES = 8 * 1024 * 1024


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class MultipartS3Writer:
    def __init__(
        self,
        client,
        *,
        bucket: str,
        key: str,
        content_type: str,
        part_size: int = DOWNLOAD_PROGRESS_POLL_BYTES,
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._key = key
        self._part_size = max(5 * 1024 * 1024, part_size)
        self._buffer = bytearray()
        self._offset = 0
        self._parts: list[dict[str, Any]] = []
        self._closed = False
        response = client.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ContentType=content_type,
        )
        self._upload_id = response["UploadId"]

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._offset

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed multipart writer.")
        payload = bytes(data)
        self._buffer.extend(payload)
        while len(self._buffer) >= self._part_size:
            self._upload_part(bytes(self._buffer[: self._part_size]))
            del self._buffer[: self._part_size]
        self._offset += len(payload)
        return len(payload)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._buffer:
                self._upload_part(bytes(self._buffer))
                self._buffer.clear()
            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
                MultipartUpload={"Parts": self._parts},
            )
        except Exception:
            self.abort()
            raise

    def abort(self) -> None:
        try:
            self._client.abort_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
            )
        except Exception:
            pass

    def _upload_part(self, payload: bytes) -> None:
        response = self._client.upload_part(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
            PartNumber=len(self._parts) + 1,
            Body=payload,
        )
        self._parts.append(
            {
                "PartNumber": len(self._parts) + 1,
                "ETag": response["ETag"],
            }
        )


class S3DownloadJobManager:
    def __init__(
        self,
        *,
        settings: Settings,
        state_change_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._settings = settings
        self._state_change_callback = state_change_callback
        self._lock = threading.RLock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._state_version = 0

    def start_generated_zip_job(
        self,
        *,
        bucket: str,
        prefix: str,
        file_format: str,
        file_name: str = "",
    ) -> dict[str, Any]:
        normalized_bucket = normalize_s3_bucket_name(bucket)
        reject_hidden_s3_bucket(normalized_bucket, self._settings)
        normalized_prefix = normalize_s3_prefix(prefix)
        if not normalized_prefix:
            raise ValueError("Choose a generated S3 prefix before downloading.")
        if is_data_exchange_key(normalized_prefix, self._settings.data_exchange_prefix):
            raise ValueError("DataExchange files must be downloaded from the DataExchange Workbench.")
        normalized_format = self._normalize_format(file_format)
        fallback_name = f"{PurePosixPath(normalized_prefix.rstrip('/')).name or 'generated-parts'}.zip"
        filename = normalize_s3_object_filename(file_name, fallback_key=fallback_name)
        if not filename.lower().endswith(".zip"):
            filename = f"{filename.rsplit('.', 1)[0] if '.' in filename else filename}.zip"
        artifact_key = f"{normalized_prefix}{filename}"
        now = utc_now_iso()
        job_id = f"download-{uuid.uuid4().hex}"
        snapshot = {
            "jobId": job_id,
            "workloadType": "download",
            "status": "queued",
            "startedAt": now,
            "updatedAt": now,
            "completedAt": None,
            "durationMs": 0,
            "progress": 0.0,
            "progressLabel": "Queued...",
            "message": "Waiting to prepare ZIP archive.",
            "bucket": normalized_bucket,
            "prefix": normalized_prefix,
            "key": artifact_key,
            "path": f"s3://{normalized_bucket}/{artifact_key}",
            "filename": filename,
            "contentType": "application/zip",
            "bytesWritten": 0,
            "sourceBytes": 0,
            "partCount": 0,
            "error": None,
            "canCancel": True,
        }
        cancel_event = threading.Event()
        with self._lock:
            self._jobs[job_id] = {"snapshot": snapshot, "cancel": cancel_event}
            self._touch_locked()
        threading.Thread(
            target=self._run_generated_zip_job,
            args=(job_id, normalized_format, cancel_event),
            daemon=True,
            name=f"bdw-s3-download-{job_id[-8:]}",
        ).start()
        return dict(snapshot)

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown download job: {job_id}")
            snapshot = record["snapshot"]
            if snapshot["status"] in TERMINAL_DOWNLOAD_STATUSES:
                return dict(snapshot)
            record["cancel"].set()
            snapshot["status"] = "cancelled"
            snapshot["completedAt"] = utc_now_iso()
            snapshot["updatedAt"] = snapshot["completedAt"]
            snapshot["progress"] = None
            snapshot["progressLabel"] = "Cancelled"
            snapshot["message"] = "ZIP download job cancelled."
            snapshot["canCancel"] = False
            self._touch_locked()
            return dict(snapshot)

    def snapshot(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown download job: {job_id}")
            return dict(record["snapshot"])

    def state_payload(self) -> dict[str, Any]:
        with self._lock:
            return self._state_payload_locked()

    def _run_generated_zip_job(self, job_id: str, file_format: str, cancel_event: threading.Event) -> None:
        started = time.perf_counter()
        writer: MultipartS3Writer | None = None
        try:
            with self._lock:
                snapshot = self._jobs[job_id]["snapshot"]
                snapshot["status"] = "running"
                snapshot["progress"] = None
                snapshot["progressLabel"] = "Listing parts..."
                snapshot["message"] = "Finding generated S3 part files."
                snapshot["updatedAt"] = utc_now_iso()
                bucket = snapshot["bucket"]
                prefix = snapshot["prefix"]
                artifact_key = snapshot["key"]
                self._touch_locked()

            client = s3_client(self._settings)
            suffix = f".{file_format}"
            part_keys = [
                key
                for key in iter_s3_keys(client, bucket, prefix)
                if key.startswith(prefix)
                and key != artifact_key
                and not key.endswith("/")
                and PurePosixPath(key).suffix.lower() == suffix
                and not is_data_exchange_key(key, self._settings.data_exchange_prefix)
            ]
            part_keys.sort()
            if not part_keys:
                raise ValueError("No generated S3 part files were found for this source object.")

            source_bytes = 0
            for key in part_keys:
                try:
                    source_bytes += int(client.head_object(Bucket=bucket, Key=key).get("ContentLength") or 0)
                except Exception:
                    pass

            with self._lock:
                snapshot = self._jobs[job_id]["snapshot"]
                snapshot["partCount"] = len(part_keys)
                snapshot["sourceBytes"] = source_bytes
                snapshot["progressLabel"] = "Writing ZIP..."
                snapshot["message"] = f"Writing {len(part_keys)} S3 part file(s) into ZIP."
                snapshot["updatedAt"] = utc_now_iso()
                self._touch_locked()

            writer = MultipartS3Writer(
                client,
                bucket=bucket,
                key=artifact_key,
                content_type="application/zip",
            )
            with zipfile.ZipFile(writer, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
                for index, key in enumerate(part_keys, start=1):
                    if cancel_event.is_set():
                        raise RuntimeError("ZIP download job cancelled.")
                    response = client.get_object(Bucket=bucket, Key=key)
                    body = response["Body"]
                    try:
                        with archive.open(PurePosixPath(key).name, "w", force_zip64=True) as member:
                            while True:
                                if cancel_event.is_set():
                                    raise RuntimeError("ZIP download job cancelled.")
                                chunk = body.read(1024 * 1024)
                                if not chunk:
                                    break
                                member.write(chunk)
                    finally:
                        close = getattr(body, "close", None)
                        if callable(close):
                            close()
                    with self._lock:
                        snapshot = self._jobs[job_id]["snapshot"]
                        snapshot["bytesWritten"] = writer.tell()
                        snapshot["progress"] = index / max(1, len(part_keys))
                        snapshot["progressLabel"] = f"Writing ZIP... {index}/{len(part_keys)}"
                        snapshot["durationMs"] = (time.perf_counter() - started) * 1000
                        snapshot["updatedAt"] = utc_now_iso()
                        self._touch_locked()
            writer.close()
            final_bytes_written = writer.tell()
            writer = None
            with self._lock:
                snapshot = self._jobs[job_id]["snapshot"]
                snapshot["status"] = "completed"
                snapshot["completedAt"] = utc_now_iso()
                snapshot["updatedAt"] = snapshot["completedAt"]
                snapshot["durationMs"] = (time.perf_counter() - started) * 1000
                snapshot["progress"] = 1.0
                snapshot["progressLabel"] = "Completed"
                snapshot["message"] = f"ZIP archive ready at s3://{bucket}/{artifact_key}."
                snapshot["bytesWritten"] = max(snapshot["bytesWritten"], final_bytes_written)
                snapshot["canCancel"] = False
                self._touch_locked()
        except Exception as exc:
            if writer is not None:
                writer.abort()
            with self._lock:
                snapshot = self._jobs[job_id]["snapshot"]
                if snapshot["status"] != "cancelled":
                    snapshot["status"] = "failed"
                    snapshot["completedAt"] = utc_now_iso()
                    snapshot["updatedAt"] = snapshot["completedAt"]
                    snapshot["durationMs"] = (time.perf_counter() - started) * 1000
                    snapshot["progress"] = None
                    snapshot["progressLabel"] = "Failed"
                    snapshot["message"] = "ZIP download job failed."
                    snapshot["error"] = str(exc)
                    snapshot["canCancel"] = False
                self._touch_locked()

    def _normalize_format(self, file_format: str) -> str:
        normalized = str(file_format or "").strip().lower()
        if normalized in {"json", "ndjson"}:
            return "jsonl"
        if normalized in {"csv", "jsonl", "parquet"}:
            return normalized
        raise ValueError("Generated S3 downloads support CSV, JSONL, and Parquet files.")

    def _state_payload_locked(self) -> dict[str, Any]:
        jobs = [dict(record["snapshot"]) for record in self._jobs.values()]
        jobs.sort(key=lambda item: item.get("startedAt") or "", reverse=True)
        running = [job for job in jobs if job.get("status") in RUNNING_DOWNLOAD_STATUSES]
        return {
            "version": self._state_version,
            "summary": {
                "runningCount": len(running),
                "totalCount": len(jobs),
            },
            "jobs": jobs,
        }

    def _touch_locked(self) -> None:
        self._state_version += 1
        if self._state_change_callback is not None:
            self._state_change_callback(self._state_payload_locked())

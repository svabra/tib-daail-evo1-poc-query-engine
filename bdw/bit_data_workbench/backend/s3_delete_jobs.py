from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
import json
import logging
import time
from threading import Event, RLock, Semaphore, Thread
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from botocore.exceptions import BotoCoreError, ClientError

from ..config import Settings
from .data_sources.s3.explorer import (
    normalize_s3_bucket_name,
    normalize_s3_object_key,
    normalize_s3_prefix,
    s3_path,
)
from .s3_hidden import (
    is_data_exchange_bucket_name,
    is_hidden_s3_key,
    reject_hidden_s3_bucket,
)
from .s3_storage import (
    _bucket_delete_retry_allowed,
    _is_missing_bucket_error,
    _raise_s3_operation_error,
    _s3_error_code,
    _s3_error_message,
    _version_listing_access_denied,
    _version_listing_fallback_allowed,
    delete_s3_objects,
    iter_s3_keys,
    iter_s3_object_versions,
    s3_client,
)


logger = logging.getLogger(__name__)

S3_DELETE_RUNNING_STATUSES = {"queued", "running", "finalizing"}
S3_DELETE_TERMINAL_STATUSES = {"completed", "failed"}
S3_DELETE_LOG_MAX_TEXT_CHARS = 500
S3_DELETE_BATCH_SIZE = 1000


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _delete_log_timestamp(timezone_name: str) -> str:
    normalized_timezone = str(timezone_name or "").strip() or "Europe/Zurich"
    try:
        timezone = ZoneInfo(normalized_timezone)
    except Exception:
        with suppress(Exception):
            timezone = ZoneInfo("CET")
        if "timezone" not in locals():
            timezone = UTC
    return datetime.now(UTC).astimezone(timezone).strftime("%Y-%m-%d %H:%M:%S %Z")


def _truncate_log_text(value: object, max_chars: int = S3_DELETE_LOG_MAX_TEXT_CHARS) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if len(text) <= max_chars:
        return text
    return f"{text[: max(0, max_chars - 3)]}..."


def _format_log_value(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    return json.dumps(_truncate_log_text(value), ensure_ascii=True)


def _format_log_fields(fields: dict[str, object]) -> str:
    return " ".join(
        f"{key}={_format_log_value(value)}"
        for key, value in fields.items()
        if value not in (None, "", [], {})
    )


def _public_error_text(error: Exception) -> str:
    if isinstance(error, (ClientError, BotoCoreError)):
        return _truncate_log_text(_s3_error_message(error))
    return _truncate_log_text(str(error) or error.__class__.__name__)


class S3DeleteJobManager:
    def __init__(
        self,
        *,
        settings: Settings,
        state_change_callback: Callable[[dict[str, Any]], None] | None = None,
        completion_callback: Callable[[dict[str, Any]], None] | None = None,
        s3_client_factory: Callable[[Settings], Any] = s3_client,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        run_jobs_inline: bool = False,
    ) -> None:
        self._settings = settings
        self._state_change_callback = state_change_callback
        self._completion_callback = completion_callback
        self._s3_client_factory = s3_client_factory
        self._sleep = sleep
        self._monotonic = monotonic
        self._run_jobs_inline = run_jobs_inline
        self._lock = RLock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._version = 0
        self._semaphore = Semaphore(
            max(1, int(getattr(settings, "s3_delete_max_concurrent_jobs", 1) or 1))
        )

    def state_payload(self) -> dict[str, Any]:
        with self._lock:
            self._prune_jobs_locked(utc_now())
            return self._state_payload_locked()

    def job_payload(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            return self._public_payload(self._require_job_locked(job_id))

    def start_job(self, *, entry_kind: str, bucket: str, prefix: str = "") -> dict[str, Any]:
        job_id = f"s3-delete-{uuid4().hex}"
        raw_kind = str(entry_kind or "").strip().lower()
        raw_bucket = str(bucket or "").strip()
        raw_prefix = str(prefix or "").strip()
        self._log_event(
            "requested",
            {
                "job_id": job_id,
                "entry_kind": raw_kind,
                "bucket": raw_bucket,
                "prefix": raw_prefix,
                "path": self._raw_requested_path(raw_bucket, raw_prefix),
            },
        )

        try:
            descriptor = self._normalize_descriptor(
                entry_kind=raw_kind,
                bucket=raw_bucket,
                prefix=raw_prefix,
            )
        except Exception as exc:
            self._log_event(
                "failed",
                {
                    "job_id": job_id,
                    "entry_kind": raw_kind,
                    "bucket": raw_bucket,
                    "prefix": raw_prefix,
                    "path": self._raw_requested_path(raw_bucket, raw_prefix),
                    "error": _public_error_text(exc),
                },
                level=logging.WARNING,
            )
            raise

        thread: Thread | None = None
        snapshot_to_emit: dict[str, Any] | None = None
        with self._lock:
            self._prune_jobs_locked(utc_now())
            job = self._new_job(job_id, descriptor)
            self._jobs[job_id] = job
            self._version += 1
            snapshot_to_emit = self._state_payload_locked()
            payload = self._public_payload(job)
            self._log_job_event_locked(job, "queued")
            if not self._run_jobs_inline:
                thread = Thread(target=self._run_job, args=(job_id,), daemon=True)

        self._emit_state(snapshot_to_emit)
        if self._run_jobs_inline:
            self._run_job(job_id)
            with self._lock:
                return self._public_payload(self._require_job_locked(job_id))
        if thread is not None:
            thread.start()
        return payload

    def _run_job(self, job_id: str) -> None:
        acquired = False
        heartbeat_stop = Event()
        heartbeat_thread: Thread | None = None
        try:
            self._semaphore.acquire()
            acquired = True
            if not self._mark_running(job_id):
                return
            if not self._run_jobs_inline:
                heartbeat_thread = Thread(
                    target=self._heartbeat_loop,
                    args=(job_id, heartbeat_stop),
                    daemon=True,
                )
                heartbeat_thread.start()
            self._execute_delete(job_id)
            self._mark_completed(job_id)
        except Exception as exc:
            self._mark_failed(job_id, exc)
        finally:
            heartbeat_stop.set()
            if heartbeat_thread is not None:
                heartbeat_thread.join(timeout=0.2)
            if acquired:
                self._semaphore.release()

    def _execute_delete(self, job_id: str) -> None:
        with self._lock:
            job = dict(self._require_job_locked(job_id))
        client = self._s3_client_factory(self._settings)
        entry_kind = str(job["entryKind"])
        bucket = str(job["bucket"])
        prefix = str(job.get("prefix") or "")

        if entry_kind == "file":
            deleted_keys = self._delete_object_versions(job_id, client, bucket, prefix)
            self._update_job(
                job_id,
                phase="deleted_object",
                message=f"Deleted S3 object {job['path']}.",
                deleted_keys=deleted_keys,
                progress=1.0,
            )
            return

        if entry_kind == "folder":
            deleted_keys = self._delete_prefix(job_id, client, bucket, prefix)
            self._update_job(
                job_id,
                phase="deleted_prefix",
                message=f"Deleted {deleted_keys} object(s) from {job['path']}.",
                deleted_keys=deleted_keys,
                progress=1.0,
            )
            return

        if entry_kind == "bucket":
            deleted_keys = self._delete_bucket_contents(job_id, client, bucket)
            self._update_job(
                job_id,
                phase="bucket_cleanup_done",
                message=f"Deleted {deleted_keys} contained object(s); finalizing bucket removal.",
                deleted_keys=deleted_keys,
                progress=0.9,
            )
            self._finalize_bucket_delete(job_id, client, bucket)
            self._update_job(
                job_id,
                phase="bucket_deleted",
                message=f"Deleted bucket {bucket} and {deleted_keys} contained object(s).",
                deleted_keys=deleted_keys,
                bucket_deleted=True,
                progress=1.0,
            )
            return

        raise ValueError("Unsupported S3 explorer entry type.")

    def _delete_object_versions(self, job_id: str, client: Any, bucket: str, key: str) -> int:
        try:
            deleted, matched = self._delete_version_stream(
                job_id,
                client,
                bucket,
                prefix=key,
                exact_key=key,
            )
        except (ClientError, BotoCoreError) as exc:
            self._update_job(
                job_id,
                phase="list_object_versions_failed",
                message=f"Failed to list object versions for {key}; checking fallback.",
                error=_public_error_text(exc),
            )
            if _is_missing_bucket_error(exc):
                raise ValueError(f"The S3 bucket '{bucket}' does not exist.") from exc
            if not _version_listing_fallback_allowed(exc):
                _raise_s3_operation_error(
                    exc,
                    action=f"list object versions for '{key}'",
                    bucket=bucket,
                )
            return self._delete_visible_keys(job_id, client, bucket, [key])

        if matched:
            return deleted
        return self._delete_visible_keys(job_id, client, bucket, [key])

    def _delete_prefix(self, job_id: str, client: Any, bucket: str, prefix: str) -> int:
        try:
            deleted, matched = self._delete_version_stream(
                job_id,
                client,
                bucket,
                prefix=prefix,
            )
        except (ClientError, BotoCoreError) as exc:
            self._update_job(
                job_id,
                phase="list_prefix_versions_failed",
                message=f"Failed to list object versions under {prefix}; checking fallback.",
                error=_public_error_text(exc),
            )
            if _is_missing_bucket_error(exc):
                raise ValueError(f"The S3 bucket '{bucket}' does not exist.") from exc
            if not _version_listing_fallback_allowed(exc):
                _raise_s3_operation_error(
                    exc,
                    action=f"list object versions under prefix '{prefix}'",
                    bucket=bucket,
                )
            return self._delete_visible_prefix_keys(job_id, client, bucket, prefix)

        if matched:
            return deleted
        return self._delete_visible_prefix_keys(job_id, client, bucket, prefix)

    def _delete_bucket_contents(self, job_id: str, client: Any, bucket: str) -> int:
        if not self._bucket_exists(client, bucket):
            return 0
        try:
            deleted, matched = self._delete_version_stream(job_id, client, bucket, prefix="")
        except (ClientError, BotoCoreError) as exc:
            self._update_job(
                job_id,
                phase="list_bucket_versions_failed",
                message="Failed to list bucket object versions; checking fallback.",
                error=_public_error_text(exc),
            )
            if _is_missing_bucket_error(exc):
                raise ValueError(f"The S3 bucket '{bucket}' does not exist.") from exc
            if _version_listing_access_denied(exc):
                return self._delete_visible_prefix_keys(job_id, client, bucket, "")
            if not _version_listing_fallback_allowed(exc):
                _raise_s3_operation_error(exc, action="list object versions", bucket=bucket)
            return self._delete_visible_prefix_keys(job_id, client, bucket, "")

        if matched:
            return deleted
        return self._delete_visible_prefix_keys(job_id, client, bucket, "")

    def _delete_version_stream(
        self,
        job_id: str,
        client: Any,
        bucket: str,
        *,
        prefix: str,
        exact_key: str | None = None,
    ) -> tuple[int, int]:
        deleted = 0
        matched = 0
        listed = 0
        batch: list[dict[str, str]] = []
        for item in iter_s3_object_versions(client, bucket, prefix):
            listed += 1
            if exact_key is not None and item.get("Key") != exact_key:
                continue
            matched += 1
            batch.append(item)
            if len(batch) >= S3_DELETE_BATCH_SIZE:
                deleted += self._delete_batch(job_id, client, bucket, batch)
                batch = []
                self._update_job(
                    job_id,
                    phase="deleting_versions",
                    message="Deleting S3 object versions.",
                    listed_versions=listed,
                    deleted_keys=deleted,
                    progress=0.5,
                )
        if batch:
            deleted += self._delete_batch(job_id, client, bucket, batch)
        if matched:
            self._update_job(
                job_id,
                phase="deleted_versions",
                message="Deleted S3 object versions.",
                listed_versions=listed,
                deleted_keys=deleted,
                progress=0.75,
            )
        return deleted, matched

    def _delete_visible_prefix_keys(self, job_id: str, client: Any, bucket: str, prefix: str) -> int:
        deleted = 0
        batch: list[str] = []
        listed = 0
        for key in iter_s3_keys(client, bucket, prefix):
            listed += 1
            batch.append(key)
            if len(batch) >= S3_DELETE_BATCH_SIZE:
                deleted += self._delete_batch(job_id, client, bucket, batch)
                batch = []
                self._update_job(
                    job_id,
                    phase="deleting_visible_objects",
                    message="Deleting visible S3 objects.",
                    listed_keys=listed,
                    deleted_keys=deleted,
                    progress=0.5,
                )
        if batch:
            deleted += self._delete_batch(job_id, client, bucket, batch)
        self._update_job(
            job_id,
            phase="deleted_visible_objects",
            message="Deleted visible S3 objects.",
            listed_keys=listed,
            deleted_keys=deleted,
            progress=0.75,
        )
        return deleted

    def _delete_visible_keys(self, job_id: str, client: Any, bucket: str, keys: list[str]) -> int:
        deleted = 0
        for index in range(0, len(keys), S3_DELETE_BATCH_SIZE):
            batch = keys[index : index + S3_DELETE_BATCH_SIZE]
            deleted += self._delete_batch(job_id, client, bucket, batch)
            self._update_job(
                job_id,
                phase="deleting_visible_objects",
                message="Deleting visible S3 objects.",
                listed_keys=len(keys),
                deleted_keys=deleted,
                progress=0.5,
            )
        return deleted

    def _delete_batch(
        self,
        job_id: str,
        client: Any,
        bucket: str,
        objects: list[str | dict[str, str]],
    ) -> int:
        if not objects:
            return 0
        with self._lock:
            job = self._jobs.get(job_id)
            deleted_batches = int(job.get("deletedBatches") or 0) + 1 if job else 1
        deleted = delete_s3_objects(client, bucket, objects)
        self._update_job(
            job_id,
            phase="deleting_batch",
            message="Deleted S3 batch.",
            deleted_batches=deleted_batches,
        )
        return deleted

    def _finalize_bucket_delete(self, job_id: str, client: Any, bucket: str) -> None:
        timeout_seconds = max(
            1,
            int(getattr(self._settings, "s3_delete_bucket_finalize_timeout_seconds", 180) or 180),
        )
        deadline = self._monotonic() + timeout_seconds
        attempt = 0
        self._mark_finalizing(job_id)

        while True:
            attempt += 1
            self._update_job(
                job_id,
                phase="delete_bucket",
                message=f"Finalizing bucket deletion attempt {attempt}.",
                bucket_delete_attempts=attempt,
                progress=0.95,
                status="finalizing",
            )
            self._log_job_event_by_id(job_id, "bucket_finalizing", extra={"attempt": attempt})

            try:
                client.delete_bucket(Bucket=bucket)
            except (ClientError, BotoCoreError) as exc:
                if _is_missing_bucket_error(exc):
                    return
                if not _bucket_delete_retry_allowed(exc):
                    _raise_s3_operation_error(exc, action="delete the bucket", bucket=bucket)
                self._update_job(
                    job_id,
                    phase="bucket_not_ready",
                    message="Bucket is not deletable yet; waiting for object-store finalization.",
                    error=_public_error_text(exc),
                    bucket_delete_attempts=attempt,
                    status="finalizing",
                )

            if not self._bucket_exists(client, bucket):
                return

            if self._monotonic() >= deadline:
                raise ValueError(
                    f"Failed to delete S3 bucket '{bucket}': object cleanup finished, "
                    f"but the bucket was still visible after {timeout_seconds} seconds."
                )

            self._sleep(min(5.0, max(0.5, attempt * 0.5)))

    def _bucket_exists(self, client: Any, bucket: str) -> bool:
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
                return True

    def _mark_running(self, job_id: str) -> bool:
        snapshot_to_emit: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.get("status") not in {"queued", "running"}:
                return False
            now = utc_now()
            job["status"] = "running"
            job["phase"] = "worker_started"
            job["message"] = "S3 delete worker started."
            job["startedAt"] = isoformat(now)
            self._touch_job_locked(job, now)
            self._version += 1
            snapshot_to_emit = self._state_payload_locked()
            self._log_job_event_locked(job, "worker_started")
        self._emit_state(snapshot_to_emit)
        return True

    def _mark_finalizing(self, job_id: str) -> None:
        self._update_job(
            job_id,
            phase="bucket_finalizing",
            message="Object cleanup finished; waiting for the bucket to disappear.",
            status="finalizing",
            progress=0.95,
        )

    def _mark_completed(self, job_id: str) -> None:
        callback_payload: dict[str, Any] | None = None
        snapshot_to_emit: dict[str, Any] | None = None
        with self._lock:
            job = self._require_job_locked(job_id)
            now = utc_now()
            job["status"] = "completed"
            job["completedAt"] = isoformat(now)
            job["progress"] = 1.0
            job["error"] = ""
            self._touch_job_locked(job, now)
            self._version += 1
            snapshot_to_emit = self._state_payload_locked()
            callback_payload = self._public_payload(job)
            self._log_job_event_locked(job, "completed")
        if self._completion_callback is not None and callback_payload is not None:
            with suppress(Exception):
                self._completion_callback(callback_payload)
        self._emit_state(snapshot_to_emit)

    def _mark_failed(self, job_id: str, error: Exception) -> None:
        snapshot_to_emit: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            now = utc_now()
            job["status"] = "failed"
            job["completedAt"] = isoformat(now)
            job["error"] = _public_error_text(error)
            job["message"] = job["error"] or "S3 delete failed."
            self._touch_job_locked(job, now)
            self._version += 1
            snapshot_to_emit = self._state_payload_locked()
            self._log_job_event_locked(
                job,
                "failed",
                level=logging.WARNING,
                extra={"error": job["error"], "error_code": _s3_error_code(error)},
            )
        self._emit_state(snapshot_to_emit)

    def _update_job(
        self,
        job_id: str,
        *,
        phase: str,
        message: str,
        status: str | None = None,
        progress: float | None = None,
        deleted_keys: int | None = None,
        bucket_deleted: bool | None = None,
        listed_versions: int | None = None,
        listed_keys: int | None = None,
        deleted_batches: int | None = None,
        bucket_delete_attempts: int | None = None,
        error: str | None = None,
    ) -> None:
        snapshot_to_emit: dict[str, Any] | None = None
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            if status is not None:
                job["status"] = status
            job["phase"] = phase
            job["message"] = message
            if progress is not None:
                job["progress"] = max(0.0, min(1.0, float(progress)))
            if deleted_keys is not None:
                job["deletedKeys"] = int(deleted_keys)
            if bucket_deleted is not None:
                job["bucketDeleted"] = bool(bucket_deleted)
            if listed_versions is not None:
                job["listedVersions"] = int(listed_versions)
            if listed_keys is not None:
                job["listedKeys"] = int(listed_keys)
            if deleted_batches is not None:
                job["deletedBatches"] = int(deleted_batches)
            if bucket_delete_attempts is not None:
                job["bucketDeleteAttempts"] = int(bucket_delete_attempts)
            if error is not None:
                job["lastError"] = _truncate_log_text(error)
            self._touch_job_locked(job)
            self._version += 1
            snapshot_to_emit = self._state_payload_locked()
        self._emit_state(snapshot_to_emit)

    def _heartbeat_loop(self, job_id: str, stop_event: Event) -> None:
        interval = max(
            5,
            int(getattr(self._settings, "s3_delete_job_log_heartbeat_seconds", 10) or 10),
        )
        while not stop_event.wait(interval):
            self._log_heartbeat_if_due(job_id)

    def _log_heartbeat_if_due(self, job_id: str) -> None:
        if not bool(getattr(self._settings, "s3_delete_job_logging_enabled", True)):
            return
        now_monotonic = self._monotonic()
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.get("status") not in {"running", "finalizing"}:
                return
            interval = max(
                5,
                int(getattr(self._settings, "s3_delete_job_log_heartbeat_seconds", 10) or 10),
            )
            last_logged = float(job.get("lastLogHeartbeatMonotonic") or 0.0)
            if last_logged and now_monotonic - last_logged < interval:
                return
            job["lastLogHeartbeatMonotonic"] = now_monotonic
            self._log_job_event_locked(job, "heartbeat", once=False)

    def _new_job(self, job_id: str, descriptor: dict[str, str]) -> dict[str, Any]:
        now = utc_now()
        return {
            "jobId": job_id,
            "entryKind": descriptor["entryKind"],
            "name": descriptor.get("name") or "",
            "bucket": descriptor["bucket"],
            "prefix": descriptor.get("prefix") or "",
            "path": descriptor["path"],
            "status": "queued",
            "phase": "queued",
            "progress": 0.0,
            "message": "Queued for S3 deletion.",
            "deletedKeys": 0,
            "bucketDeleted": False,
            "listedVersions": 0,
            "listedKeys": 0,
            "deletedBatches": 0,
            "bucketDeleteAttempts": 0,
            "error": "",
            "lastError": "",
            "createdAt": isoformat(now),
            "updatedAt": isoformat(now),
            "startedAt": None,
            "completedAt": None,
            "lastLogHeartbeatMonotonic": 0.0,
            "loggedEvents": set(),
        }

    def _normalize_descriptor(self, *, entry_kind: str, bucket: str, prefix: str) -> dict[str, str]:
        normalized_kind = str(entry_kind or "").strip().lower()
        normalized_bucket = normalize_s3_bucket_name(bucket)
        reject_hidden_s3_bucket(normalized_bucket, self._settings)

        if normalized_kind == "file":
            normalized_key = normalize_s3_object_key(prefix)
            if is_data_exchange_bucket_name(normalized_bucket) or is_hidden_s3_key(
                normalized_key,
                self._settings.data_exchange_prefix,
            ):
                raise ValueError("Reserved S3 files must be deleted from the owning Workbench.")
            return {
                "entryKind": "file",
                "name": normalized_key.rsplit("/", 1)[-1],
                "bucket": normalized_bucket,
                "prefix": normalized_key,
                "path": f"s3://{normalized_bucket}/{normalized_key}",
            }

        if normalized_kind == "folder":
            normalized_prefix = normalize_s3_prefix(prefix)
            if not normalized_prefix:
                raise ValueError("Choose a folder before deleting it.")
            if is_data_exchange_bucket_name(normalized_bucket) or is_hidden_s3_key(
                normalized_prefix,
                self._settings.data_exchange_prefix,
            ):
                raise ValueError("Reserved S3 folders must be deleted from the owning Workbench.")
            return {
                "entryKind": "folder",
                "name": normalized_prefix.rstrip("/").rsplit("/", 1)[-1],
                "bucket": normalized_bucket,
                "prefix": normalized_prefix,
                "path": s3_path(normalized_bucket, normalized_prefix),
            }

        if normalized_kind == "bucket":
            if is_data_exchange_bucket_name(normalized_bucket):
                raise ValueError("DataExchange buckets must be deleted outside the normal explorer.")
            return {
                "entryKind": "bucket",
                "name": normalized_bucket,
                "bucket": normalized_bucket,
                "prefix": "",
                "path": s3_path(normalized_bucket),
            }

        raise ValueError("Unsupported S3 explorer entry type.")

    def _public_payload(self, job: dict[str, Any]) -> dict[str, Any]:
        return {
            "jobId": job.get("jobId"),
            "entryKind": job.get("entryKind"),
            "name": job.get("name"),
            "bucket": job.get("bucket"),
            "prefix": job.get("prefix"),
            "path": job.get("path"),
            "status": job.get("status"),
            "phase": job.get("phase"),
            "progress": job.get("progress"),
            "message": job.get("message"),
            "deletedKeys": job.get("deletedKeys"),
            "bucketDeleted": job.get("bucketDeleted"),
            "listedVersions": job.get("listedVersions"),
            "listedKeys": job.get("listedKeys"),
            "deletedBatches": job.get("deletedBatches"),
            "bucketDeleteAttempts": job.get("bucketDeleteAttempts"),
            "error": job.get("error"),
            "lastError": job.get("lastError"),
            "createdAt": job.get("createdAt"),
            "updatedAt": job.get("updatedAt"),
            "startedAt": job.get("startedAt"),
            "completedAt": job.get("completedAt"),
        }

    def _state_payload_locked(self) -> dict[str, Any]:
        jobs = sorted(
            (self._public_payload(job) for job in self._jobs.values()),
            key=lambda job: str(job.get("updatedAt") or job.get("createdAt") or ""),
            reverse=True,
        )
        running_count = sum(1 for job in jobs if job.get("status") in S3_DELETE_RUNNING_STATUSES)
        return {
            "version": self._version,
            "summary": {
                "runningCount": running_count,
                "totalCount": len(jobs),
            },
            "jobs": jobs,
        }

    def _touch_job_locked(self, job: dict[str, Any], now: datetime | None = None) -> None:
        job["updatedAt"] = isoformat(now or utc_now())

    def _prune_jobs_locked(self, now: datetime) -> None:
        retention_hours = max(
            1,
            int(getattr(self._settings, "s3_delete_job_retention_hours", 24) or 24),
        )
        cutoff = now - timedelta(hours=retention_hours)
        stale_job_ids: list[str] = []
        for job_id, job in self._jobs.items():
            if job.get("status") not in S3_DELETE_TERMINAL_STATUSES:
                continue
            completed_at = self._parse_job_time(job.get("completedAt") or job.get("updatedAt"))
            if completed_at is not None and completed_at < cutoff:
                stale_job_ids.append(job_id)
        for job_id in stale_job_ids:
            self._jobs.pop(job_id, None)
        if stale_job_ids:
            self._version += 1

    @staticmethod
    def _parse_job_time(value: object) -> datetime | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _require_job_locked(self, job_id: str) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        job = self._jobs.get(normalized_job_id)
        if job is None:
            raise KeyError("S3 delete job not found.")
        return job

    def _emit_state(self, snapshot: dict[str, Any] | None) -> None:
        if snapshot is None or self._state_change_callback is None:
            return
        try:
            self._state_change_callback(snapshot)
        except Exception:
            logger.exception("Failed to publish S3 delete job state.")

    def _log_job_event_by_id(
        self,
        job_id: str,
        event: str,
        *,
        level: int = logging.INFO,
        extra: dict[str, object] | None = None,
    ) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            self._log_job_event_locked(job, event, level=level, extra=extra or {}, once=False)

    def _log_job_event_locked(
        self,
        job: dict[str, Any],
        event: str,
        *,
        level: int = logging.INFO,
        extra: dict[str, object] | None = None,
        once: bool = True,
    ) -> None:
        if not bool(getattr(self._settings, "s3_delete_job_logging_enabled", True)):
            return
        logged_events = job.get("loggedEvents")
        if isinstance(logged_events, set) and once and event in logged_events:
            return
        if isinstance(logged_events, set) and once:
            logged_events.add(event)
        fields = {
            "s3_delete_time": _delete_log_timestamp(
                str(getattr(self._settings, "s3_delete_job_log_timezone", "Europe/Zurich") or "Europe/Zurich")
            ),
            "s3_delete_event": event,
            "job_id": job.get("jobId"),
            "entry_kind": job.get("entryKind"),
            "bucket": job.get("bucket"),
            "prefix": job.get("prefix"),
            "path": job.get("path"),
            "status": job.get("status"),
            "phase": job.get("phase"),
            "deleted_keys": job.get("deletedKeys"),
            "listed_versions": job.get("listedVersions"),
            "listed_keys": job.get("listedKeys"),
            "deleted_batches": job.get("deletedBatches"),
            "bucket_delete_attempts": job.get("bucketDeleteAttempts"),
        }
        if job.get("startedAt"):
            started_at = self._parse_job_time(job.get("startedAt"))
            if started_at is not None:
                elapsed_ms = (utc_now() - started_at).total_seconds() * 1000
                fields["duration_ms"] = round(elapsed_ms, 3)
        if job.get("error"):
            fields["error"] = job.get("error")
        if job.get("lastError") and event == "heartbeat":
            fields["last_error"] = job.get("lastError")
        for key, value in (extra or {}).items():
            fields[key] = value
        logger.log(level, "[bdw-s3-delete] %s", _format_log_fields(fields))

    def _log_event(
        self,
        event: str,
        fields: dict[str, object],
        *,
        level: int = logging.INFO,
    ) -> None:
        if not bool(getattr(self._settings, "s3_delete_job_logging_enabled", True)):
            return
        payload = {
            "s3_delete_time": _delete_log_timestamp(
                str(getattr(self._settings, "s3_delete_job_log_timezone", "Europe/Zurich") or "Europe/Zurich")
            ),
            "s3_delete_event": event,
            **fields,
        }
        logger.log(level, "[bdw-s3-delete] %s", _format_log_fields(payload))

    @staticmethod
    def _raw_requested_path(bucket: str, prefix: str = "") -> str:
        normalized_bucket = str(bucket or "").strip()
        normalized_prefix = str(prefix or "").strip().lstrip("/")
        if normalized_bucket and normalized_prefix:
            return f"s3://{normalized_bucket}/{normalized_prefix}"
        if normalized_bucket:
            return f"s3://{normalized_bucket}/"
        return "s3://"

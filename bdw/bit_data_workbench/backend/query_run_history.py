from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
import logging
from threading import Lock, Thread
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from ..config import Settings
from .s3_hidden import QUERY_RUN_HISTORY_S3_PREFIX
from .s3_storage import ensure_s3_bucket, iter_s3_keys, s3_client


logger = logging.getLogger(__name__)
QUERY_RUN_HISTORY_SCHEMA_VERSION = 1
DEFAULT_QUERY_RUN_HISTORY_LIMIT = 100


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_iso_date(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return datetime.now(UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _history_key_for_run(run: dict[str, Any]) -> str:
    job_id = str(run.get("jobId") or "").strip()
    if not job_id:
        raise ValueError("Query run history records require a job id.")
    completed_at = str(run.get("completedAt") or run.get("updatedAt") or run.get("startedAt") or "")
    completed_date = _safe_iso_date(completed_at)
    return (
        f"{QUERY_RUN_HISTORY_S3_PREFIX}"
        f"{completed_date:%Y/%m/%d}/"
        f"{job_id}.json"
    )


def _compact_query_job_payload(job: dict[str, Any]) -> dict[str, Any]:
    metrics = {
        "cpuPercent": job.get("cpuPercent"),
        "averageCpuPercent": job.get("averageCpuPercent"),
        "peakCpuPercent": job.get("peakCpuPercent"),
        "memoryRssBytes": job.get("memoryRssBytes"),
        "averageMemoryRssBytes": job.get("averageMemoryRssBytes"),
        "peakMemoryRssBytes": job.get("peakMemoryRssBytes"),
    }
    return {
        "schemaVersion": QUERY_RUN_HISTORY_SCHEMA_VERSION,
        "persistedAt": utc_now_iso(),
        "jobId": job.get("jobId"),
        "notebookId": job.get("notebookId"),
        "notebookTitle": job.get("notebookTitle"),
        "cellId": job.get("cellId"),
        "sql": job.get("sql"),
        "status": job.get("status"),
        "startedAt": job.get("startedAt"),
        "updatedAt": job.get("updatedAt"),
        "completedAt": job.get("completedAt"),
        "durationMs": job.get("durationMs"),
        "timings": {
            str(key): float(value)
            for key, value in (job.get("timings") or {}).items()
            if isinstance(value, (int, float))
        },
        "progressLabel": job.get("progressLabel"),
        "message": job.get("message"),
        "error": job.get("error"),
        "rowCount": job.get("rowCount"),
        "rowsShown": job.get("rowsShown"),
        "dataSources": list(job.get("dataSources") or []),
        "sourceTypes": list(job.get("sourceTypes") or []),
        "touchedRelations": list(job.get("touchedRelations") or []),
        "touchedBuckets": list(job.get("touchedBuckets") or []),
        "backendName": job.get("backendName"),
        "executionMode": job.get("executionMode"),
        "processId": job.get("processId"),
        "cancellationPhase": job.get("cancellationPhase"),
        "cancellationRequestedAt": job.get("cancellationRequestedAt"),
        "workerExitCode": job.get("workerExitCode"),
        "metrics": metrics,
        "resourceSamples": list(job.get("resourceSamples") or []),
        "progressEvents": list(job.get("progressEvents") or []),
    }


def _matches_filters(
    run: dict[str, Any],
    *,
    notebook_id: str = "",
    cell_id: str = "",
    status: str = "",
) -> bool:
    if notebook_id and str(run.get("notebookId") or "").strip() != notebook_id:
        return False
    if cell_id and str(run.get("cellId") or "").strip() != cell_id:
        return False
    if status and str(run.get("status") or "").strip().lower() != status:
        return False
    return True


@dataclass(slots=True)
class QueryRunHistoryStore:
    settings: Settings
    _threads: list[Thread] = field(default_factory=list, init=False)
    _thread_lock: Lock = field(default_factory=Lock, init=False)

    def __post_init__(self) -> None:
        pass

    @property
    def bucket(self) -> str:
        return str(getattr(self.settings, "s3_bucket", "") or "").strip()

    def available(self) -> bool:
        return bool(
            self.bucket
            and getattr(self.settings, "s3_endpoint", None)
            and self.settings.current_s3_access_key_id()
            and self.settings.current_s3_secret_access_key()
        )

    def record(self, job_payload: dict[str, Any]) -> dict[str, Any]:
        if not self.available():
            raise ValueError("S3 is not configured for query-run history.")

        run = _compact_query_job_payload(dict(job_payload or {}))
        key = _history_key_for_run(run)
        client = s3_client(self.settings)
        ensure_s3_bucket(self.settings, self.bucket)
        client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(run, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            Metadata={"bdw-query-run-job-id": str(run.get("jobId") or "")},
        )
        return {**run, "bucket": self.bucket, "key": key}

    def record_async(self, job_payload: dict[str, Any]) -> None:
        def target() -> None:
            try:
                self.record(job_payload)
            except Exception as exc:
                logger.warning("Failed to persist query-run history: %s", exc)

        thread = Thread(
            target=target,
            daemon=True,
            name=f"bdw-query-run-history-{str(job_payload.get('jobId') or '')[:8]}",
        )
        with self._thread_lock:
            self._threads.append(thread)
        thread.start()

    def list_runs(
        self,
        *,
        notebook_id: str = "",
        cell_id: str = "",
        status: str = "",
        limit: int = DEFAULT_QUERY_RUN_HISTORY_LIMIT,
    ) -> dict[str, Any]:
        normalized_limit = max(1, min(500, int(limit or DEFAULT_QUERY_RUN_HISTORY_LIMIT)))
        if not self.available():
            return {
                "available": False,
                "runs": [],
                "message": "S3 is not configured for query-run history.",
            }

        normalized_notebook_id = str(notebook_id or "").strip()
        normalized_cell_id = str(cell_id or "").strip()
        normalized_status = str(status or "").strip().lower()
        runs: list[dict[str, Any]] = []
        client = s3_client(self.settings)
        try:
            keys = sorted(
                (
                    key
                    for key in iter_s3_keys(client, self.bucket, QUERY_RUN_HISTORY_S3_PREFIX)
                    if key.endswith(".json")
                ),
                reverse=True,
            )
            for key in keys:
                if len(runs) >= normalized_limit:
                    break
                run = self._read_run_key(client, key)
                if run and _matches_filters(
                    run,
                    notebook_id=normalized_notebook_id,
                    cell_id=normalized_cell_id,
                    status=normalized_status,
                ):
                    runs.append({**run, "bucket": self.bucket, "key": key})
        except (ClientError, BotoCoreError) as exc:
            logger.warning("Failed to list query-run history: %s", exc)
            return {
                "available": False,
                "runs": [],
                "message": "Query-run history could not be loaded from S3.",
            }

        return {"available": True, "runs": runs}

    def get_run(self, job_id: str) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            raise KeyError("Missing query run id.")
        if not self.available():
            raise KeyError(f"Unknown query run: {normalized_job_id}")
        client = s3_client(self.settings)
        suffix = f"/{normalized_job_id}.json"
        for key in sorted(iter_s3_keys(client, self.bucket, QUERY_RUN_HISTORY_S3_PREFIX), reverse=True):
            if not key.endswith(suffix):
                continue
            run = self._read_run_key(client, key)
            if run:
                return {**run, "bucket": self.bucket, "key": key}
        raise KeyError(f"Unknown query run: {normalized_job_id}")

    def wait_for_idle(self, timeout: float = 5.0) -> None:
        with self._thread_lock:
            threads = list(self._threads)
        for thread in threads:
            thread.join(timeout=timeout)

    def _read_run_key(self, client, key: str) -> dict[str, Any] | None:
        try:
            response = client.get_object(Bucket=self.bucket, Key=key)
            body = response["Body"].read()
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            logger.warning("Failed to read query-run history object %s: %s", key, exc)
            return None
        return payload if isinstance(payload, dict) else None

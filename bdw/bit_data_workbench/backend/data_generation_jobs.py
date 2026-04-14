from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import UTC, datetime
from typing import Any, Callable

from ..config import Settings
from ..data_generator.base import DataGenerationCancelled, DataGeneratorContext, DataGeneratorResult
from ..data_generator.registry import DataGeneratorRegistry
from ..models import DataGenerationJobDefinition, normalize_data_generation_targets


RUNNING_GENERATION_STATUSES = {"queued", "running"}
TERMINAL_GENERATION_STATUSES = {"completed", "failed", "cancelled"}
MAX_GENERATION_HISTORY = 40
logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class DataGenerationJobRecord:
    def __init__(self, snapshot: DataGenerationJobDefinition, sort_index: int) -> None:
        self.snapshot = snapshot
        self.sort_index = sort_index
        self.cancel_requested = False
        self.thread: threading.Thread | None = None


class DataGenerationJobManager:
    def __init__(
        self,
        *,
        settings: Settings,
        registry: DataGeneratorRegistry,
        connection_factory: Callable[[], object],
        metadata_refresher: Callable[[], None],
        state_change_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._connection_factory = connection_factory
        self._metadata_refresher = metadata_refresher
        self._state_change_callback = state_change_callback
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._jobs: dict[str, DataGenerationJobRecord] = {}
        self._sort_counter = 0
        self._state_version = 0

    def generators_payload(self) -> list[dict[str, object]]:
        return self._registry.definitions()

    def start_job(self, *, generator_id: str, size_gb: float) -> DataGenerationJobDefinition:
        if size_gb != size_gb or size_gb <= 0:
            raise ValueError("Provide a positive generation size in GB.")
        generator = self._registry.generator(generator_id)
        normalized_size_gb = generator.normalize_size_gb(size_gb)
        now = utc_now_iso()
        snapshot = DataGenerationJobDefinition(
            job_id=f"ingest-{uuid.uuid4().hex}",
            generator_id=generator.generator_id,
            title=generator.title,
            description=generator.description,
            target_kind=generator.target_kind,
            requested_size_gb=normalized_size_gb,
            status="queued",
            started_at=now,
            updated_at=now,
            progress=0.0,
            progress_label="Queued...",
            message="Waiting to start.",
            written_targets=[],
            backend_name="Python module",
            can_cancel=generator.supports_cancel,
        )

        with self._condition:
            self._sort_counter += 1
            record = DataGenerationJobRecord(snapshot=snapshot, sort_index=self._sort_counter)
            self._jobs[snapshot.job_id] = record
            self._touch_locked()

        worker = threading.Thread(
            target=self._run_job,
            args=(snapshot.job_id,),
            daemon=True,
            name=f"bdw-ingest-{snapshot.job_id[:8]}",
        )
        with self._condition:
            record.thread = worker
        worker.start()
        return snapshot

    def cancel_job(self, job_id: str) -> DataGenerationJobDefinition:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown data generation job: {job_id}")

            if record.snapshot.status in TERMINAL_GENERATION_STATUSES:
                return record.snapshot

            record.cancel_requested = True
            if record.snapshot.status == "queued":
                completed_at = utc_now_iso()
                record.snapshot.status = "cancelled"
                record.snapshot.completed_at = completed_at
                record.snapshot.updated_at = completed_at
                record.snapshot.progress = None
                record.snapshot.progress_label = "Cancelled"
                record.snapshot.message = "Data generation cancelled."
                record.snapshot.can_cancel = False
                self._touch_locked()
                return record.snapshot

            record.snapshot.updated_at = utc_now_iso()
            record.snapshot.progress_label = "Cancelling..."
            record.snapshot.message = "Cancellation requested."
            self._touch_locked()
            return record.snapshot

    def cleanup_job(self, job_id: str) -> DataGenerationJobDefinition:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown data generation job: {job_id}")
            if record.snapshot.status in RUNNING_GENERATION_STATUSES:
                raise ValueError("Running data generation jobs cannot be cleaned yet.")
            if not record.snapshot.can_cleanup:
                raise ValueError("This data generation job has no generated target that can be cleaned.")

            record.snapshot.can_cleanup = False
            record.snapshot.updated_at = utc_now_iso()
            record.snapshot.message = "Cleaning generated data..."
            self._touch_locked()
            snapshot = record.snapshot

        generator = self._registry.generator(snapshot.generator_id)
        context = DataGeneratorContext(
            settings=self._settings,
            job_id=job_id,
            requested_size_gb=snapshot.requested_size_gb,
            connection_factory=self._connection_factory,
            progress_callback=lambda **changes: self._patch_job(job_id, **changes),
            is_cancelled=lambda: False,
        )

        try:
            result = generator.cleanup(context, snapshot)
        except Exception:
            with self._condition:
                record = self._jobs.get(job_id)
                if record is None:
                    raise
                record.snapshot.can_cleanup = True
                record.snapshot.updated_at = utc_now_iso()
                self._touch_locked()
            raise

        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(f"Unknown data generation job: {job_id}")
            record.snapshot.generated_rows = result.generated_rows
            record.snapshot.generated_size_gb = result.generated_size_gb
            record.snapshot.target_relation = result.target_relation
            record.snapshot.target_path = result.target_path
            if result.written_targets:
                record.snapshot.written_targets = normalize_data_generation_targets(result.written_targets)
            if result.target_name:
                record.snapshot.target_name = result.target_name
            record.snapshot.message = result.message
            record.snapshot.error = None
            record.snapshot.can_cleanup = False
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()
            return record.snapshot

    def state_payload(self) -> dict[str, Any]:
        with self._condition:
            return self._state_payload_locked()

    def _run_job(self, job_id: str) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None or record.snapshot.status == "cancelled":
                return
            started_at = utc_now_iso()
            record.snapshot.status = "running"
            record.snapshot.started_at = started_at
            record.snapshot.progress = 0.0
            record.snapshot.progress_label = "Preparing..."
            record.snapshot.message = "Preparing data generation."
            record.snapshot.updated_at = started_at
            self._touch_locked()

        started = time.perf_counter()
        execution_error: Exception | None = None
        execution_result: DataGeneratorResult | None = None

        try:
            generator = self._registry.generator(record.snapshot.generator_id)
            context = DataGeneratorContext(
                settings=self._settings,
                job_id=job_id,
                requested_size_gb=record.snapshot.requested_size_gb,
                connection_factory=self._connection_factory,
                progress_callback=lambda **changes: self._patch_job(job_id, **changes),
                is_cancelled=lambda: self._is_cancelled(job_id),
            )
            execution_result = generator.run(context)
        except DataGenerationCancelled:
            pass
        except Exception as exc:  # pragma: no cover - exercised in manual failure flows
            logger.exception(
                "Data generation job %s (%s) failed during execution.",
                job_id,
                record.snapshot.generator_id,
            )
            execution_error = exc

        duration_ms = (time.perf_counter() - started) * 1000
        if self._is_cancelled(job_id):
            self._finalize_job(
                job_id,
                status="cancelled",
                duration_ms=duration_ms,
                progress_label="Cancelled",
                message="Data generation cancelled.",
            )
        elif execution_error is not None:
            self._finalize_job(
                job_id,
                status="failed",
                duration_ms=duration_ms,
                progress_label="Failed",
                message="Data generation failed.",
                error=str(execution_error),
            )
        elif execution_result is not None:
            try:
                self._metadata_refresher()
            except Exception:
                pass
            can_cleanup = bool(
                getattr(generator, "supports_cleanup", False)
                and (execution_result.target_relation or execution_result.target_path)
            )
            self._finalize_job(
                job_id,
                status="completed",
                duration_ms=duration_ms,
                progress=1.0,
                progress_label="Completed",
                message=execution_result.message,
                target_name=execution_result.target_name,
                target_relation=execution_result.target_relation,
                target_path=execution_result.target_path,
                written_targets=execution_result.written_targets,
                generated_rows=execution_result.generated_rows,
                generated_size_gb=execution_result.generated_size_gb,
                can_cleanup=can_cleanup,
            )
        else:
            self._finalize_job(
                job_id,
                status="failed",
                duration_ms=duration_ms,
                progress_label="Failed",
                message="Data generation failed.",
                error="The generator finished without a result.",
            )

    def _is_cancelled(self, job_id: str) -> bool:
        with self._condition:
            return bool(self._jobs.get(job_id) and self._jobs[job_id].cancel_requested)

    def _patch_job(self, job_id: str, **changes: Any) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return
            for key, value in changes.items():
                if key == "written_targets":
                    if value is not None:
                        record.snapshot.written_targets = normalize_data_generation_targets(value)
                    continue
                if value is not None:
                    setattr(record.snapshot, key, value)
            record.snapshot.updated_at = utc_now_iso()
            self._touch_locked()

    def _finalize_job(self, job_id: str, *, status: str, duration_ms: float, **changes: Any) -> None:
        with self._condition:
            record = self._jobs.get(job_id)
            if record is None:
                return

            completed_at = utc_now_iso()
            record.snapshot.status = status
            record.snapshot.duration_ms = duration_ms
            record.snapshot.completed_at = completed_at
            record.snapshot.updated_at = completed_at
            record.snapshot.can_cancel = False
            if status != "completed":
                record.snapshot.progress = None
            for key, value in changes.items():
                if key == "written_targets":
                    if value:
                        record.snapshot.written_targets = normalize_data_generation_targets(value)
                    continue
                setattr(record.snapshot, key, value)
            self._prune_history_locked()
            self._touch_locked()

    def _state_payload_locked(self) -> dict[str, Any]:
        jobs = sorted(
            self._jobs.values(),
            key=lambda record: record.sort_index,
            reverse=True,
        )
        running_jobs = [record.snapshot for record in jobs if record.snapshot.status in RUNNING_GENERATION_STATUSES]
        return {
            "version": self._state_version,
            "summary": {
                "runningCount": len(running_jobs),
                "totalCount": len(jobs),
            },
            "jobs": [record.snapshot.payload for record in jobs],
        }

    def _touch_locked(self) -> None:
        self._state_version += 1
        payload = self._state_payload_locked()
        self._condition.notify_all()
        if self._state_change_callback is not None:
            self._state_change_callback(payload)

    def _prune_history_locked(self) -> None:
        terminal_jobs = [
            record
            for record in sorted(self._jobs.values(), key=lambda item: item.sort_index)
            if record.snapshot.status in TERMINAL_GENERATION_STATUSES
        ]
        overflow = max(0, len(terminal_jobs) - MAX_GENERATION_HISTORY)
        for record in terminal_jobs[:overflow]:
            self._jobs.pop(record.snapshot.job_id, None)

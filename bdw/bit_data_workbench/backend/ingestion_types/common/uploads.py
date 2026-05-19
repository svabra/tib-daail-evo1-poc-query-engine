from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import logging
from pathlib import Path
import shutil
from threading import RLock
from typing import Callable
import uuid

from ....config import Settings


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestionLocalSource:
    file_name: str
    local_path: Path


@dataclass(frozen=True, slots=True)
class IngestionUploadFileRequest:
    file_name: str
    size_bytes: int


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _isoformat(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _parse_isoformat(value: object) -> datetime:
    raw_value = str(value or "").strip()
    if raw_value.endswith("Z"):
        raw_value = f"{raw_value[:-1]}+00:00"
    return datetime.fromisoformat(raw_value)


def _parse_content_range(value: str, *, expected_total: int) -> tuple[int, int, int]:
    normalized = str(value or "").strip().lower()
    if not normalized.startswith("bytes "):
        raise ValueError("Content-Range must use the form 'bytes start-end/total'.")
    range_part, separator, total_part = normalized.removeprefix("bytes ").partition("/")
    if separator != "/":
        raise ValueError("Content-Range must use the form 'bytes start-end/total'.")
    start_part, dash, end_part = range_part.partition("-")
    if dash != "-":
        raise ValueError("Content-Range must use the form 'bytes start-end/total'.")
    try:
        start = int(start_part)
        end = int(end_part)
        total = int(total_part)
    except ValueError as exc:
        raise ValueError("Content-Range contains non-numeric byte offsets.") from exc
    if start < 0 or end < start or total <= 0:
        raise ValueError("Content-Range contains an invalid byte range.")
    if total != expected_total:
        raise ValueError("Content-Range total does not match the upload session file size.")
    return start, end, total


class IngestionUploadSessionManager:
    def __init__(
        self,
        *,
        settings: Settings,
        allowed_extensions: tuple[str, ...],
        format_label: str,
        empty_files_message: str,
        invalid_extension_message: str,
        direct_file_size_limit: Callable[[Settings], int],
        source_factory: Callable[[str, Path], object] | None = None,
    ) -> None:
        self._settings = settings
        self._root = settings.ingestion_upload_dir
        self._lock = RLock()
        self._allowed_extensions = tuple(str(item).lower() for item in allowed_extensions)
        self._format_label = str(format_label or "file").strip() or "file"
        self._empty_files_message = empty_files_message
        self._invalid_extension_message = invalid_extension_message
        self._direct_file_size_limit = direct_file_size_limit
        self._source_factory = source_factory or (
            lambda file_name, local_path: IngestionLocalSource(
                file_name=file_name,
                local_path=local_path,
            )
        )

    def create_session(self, files: list[IngestionUploadFileRequest]) -> dict[str, object]:
        if not files:
            raise ValueError(self._empty_files_message)

        with self._lock:
            self.cleanup_expired_sessions()
            self._root.mkdir(parents=True, exist_ok=True)
            session_id = uuid.uuid4().hex
            now = _utc_now()
            expires_at = now + timedelta(hours=self._settings.ingestion_upload_session_ttl_hours)
            session_dir = self._session_dir(session_id)
            files_dir = session_dir / "files"
            files_dir.mkdir(parents=True, exist_ok=False)
            file_entries: list[dict[str, object]] = []
            for request in files:
                file_name = self._safe_upload_file_name(request.file_name)
                size_bytes = int(request.size_bytes)
                max_size = (
                    self._settings.ingestion_upload_max_archive_bytes
                    if file_name.lower().endswith(".zip")
                    else self._direct_file_size_limit(self._settings)
                )
                if size_bytes <= 0:
                    raise ValueError(f"The file '{file_name}' is empty.")
                if size_bytes > max_size:
                    raise ValueError(f"The file '{file_name}' exceeds the configured upload size limit.")
                file_id = uuid.uuid4().hex
                file_entries.append(
                    {
                        "fileId": file_id,
                        "fileName": file_name,
                        "sizeBytes": size_bytes,
                        "receivedBytes": 0,
                        "complete": False,
                        "path": str(files_dir / f"{file_id}.upload"),
                    }
                )

            state = {
                "sessionId": session_id,
                "createdAt": _isoformat(now),
                "expiresAt": _isoformat(expires_at),
                "status": "uploading",
                "chunkSizeBytes": self._settings.ingestion_upload_chunk_bytes,
                "files": file_entries,
            }
            self._write_state(session_id, state)
            return self._public_state(state)

    def session_state(self, session_id: str) -> dict[str, object]:
        with self._lock:
            state = self._read_state(session_id)
            self._raise_if_expired(session_id, state)
            return self._public_state(state)

    def append_chunk(
        self,
        *,
        session_id: str,
        file_id: str,
        chunk_index: int,
        content_range: str,
        payload: bytes,
    ) -> dict[str, object]:
        if len(payload) > self._settings.ingestion_upload_chunk_bytes:
            raise ValueError("Upload chunk exceeds the configured chunk size.")
        with self._lock:
            state = self._read_state(session_id)
            self._raise_if_expired(session_id, state)
            file_entry = self._file_entry(state, file_id)
            expected_total = int(file_entry["sizeBytes"])
            start, end, _total = _parse_content_range(
                content_range,
                expected_total=expected_total,
            )
            expected_length = end - start + 1
            if len(payload) != expected_length:
                raise ValueError("Upload chunk size does not match Content-Range.")

            received_bytes = int(file_entry["receivedBytes"])
            if start < received_bytes and end + 1 <= received_bytes:
                return self._public_state(state)
            if start != received_bytes:
                raise ValueError(
                    f"Upload chunk is out of order. Expected byte offset {received_bytes}."
                )

            expected_chunk_index = received_bytes // self._settings.ingestion_upload_chunk_bytes
            if chunk_index != expected_chunk_index:
                raise ValueError(
                    f"Upload chunk index is out of order. Expected chunk {expected_chunk_index}."
                )

            file_path = Path(str(file_entry["path"]))
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with file_path.open("ab") as handle:
                handle.write(payload)
            received_bytes += len(payload)
            file_entry["receivedBytes"] = received_bytes
            file_entry["complete"] = received_bytes == expected_total
            self._write_state(session_id, state)
            return self._public_state(state)

    def source_files(self, session_id: str) -> list[object]:
        with self._lock:
            state = self._read_state(session_id)
            self._raise_if_expired(session_id, state)
            sources: list[object] = []
            for item in state.get("files") or []:
                file_entry = item if isinstance(item, dict) else {}
                if file_entry.get("complete") is not True:
                    raise ValueError("Upload session is not complete yet.")
                file_path = Path(str(file_entry.get("path") or ""))
                if not file_path.is_file():
                    raise ValueError("Upload session file is missing from staging storage.")
                expected_size = int(file_entry.get("sizeBytes") or 0)
                actual_size = file_path.stat().st_size
                if actual_size != expected_size:
                    raise ValueError("Upload session file size does not match the expected size.")
                sources.append(
                    self._source_factory(
                        str(file_entry.get("fileName") or ""),
                        file_path,
                    )
                )
            logger.info(
                "%s upload session staged files ready: session_id=%s file_count=%s total_bytes=%s",
                self._format_label,
                session_id,
                len(sources),
                sum(
                    int(item.get("sizeBytes") or 0)
                    for item in (state.get("files") or [])
                    if isinstance(item, dict)
                ),
            )
            return sources

    def start_processing(self, session_id: str) -> dict[str, object]:
        with self._lock:
            state = self._read_state(session_id)
            self._raise_if_expired(session_id, state)
            status = str(state.get("status") or "uploading").strip().lower()
            if status in {"processing", "completed", "failed"}:
                public_state = self._public_state(state)
                public_state["processingStarted"] = False
                return public_state
            for item in state.get("files") or []:
                file_entry = item if isinstance(item, dict) else {}
                if file_entry.get("complete") is not True:
                    raise ValueError("Upload session is not complete yet.")
            state["status"] = "processing"
            state["processingStartedAt"] = _isoformat(_utc_now())
            state["processingPhase"] = "queued"
            state["processingMessage"] = "Queued server-side import."
            state["processingDetail"] = (
                "Step 2 of 2: upload complete; waiting for the backend worker."
            )
            state["processingUpdatedAt"] = state["processingStartedAt"]
            state["processingEvents"] = [
                {
                    "at": state["processingStartedAt"],
                    "phase": state["processingPhase"],
                    "message": state["processingMessage"],
                    "detail": state["processingDetail"],
                }
            ]
            state.pop("processingCompletedAt", None)
            state.pop("result", None)
            state.pop("error", None)
            self._write_state(session_id, state)
            logger.info(
                "%s upload session entered server-side processing: session_id=%s file_count=%s total_bytes=%s",
                self._format_label,
                session_id,
                len([item for item in (state.get("files") or []) if isinstance(item, dict)]),
                sum(
                    int(item.get("sizeBytes") or 0)
                    for item in (state.get("files") or [])
                    if isinstance(item, dict)
                ),
            )
            public_state = self._public_state(state)
            public_state["processingStarted"] = True
            return public_state

    def update_processing_step(
        self,
        session_id: str,
        *,
        phase: str,
        message: str,
        detail: str = "",
        diagnostics: dict[str, object] | None = None,
    ) -> dict[str, object]:
        normalized_phase = str(phase or "processing").strip() or "processing"
        normalized_message = str(message or "Processing server-side import.").strip()
        normalized_detail = str(detail or "").strip()
        safe_diagnostics = {
            str(key): value
            for key, value in (diagnostics or {}).items()
            if str(key).strip()
        }
        with self._lock:
            state = self._read_state(session_id)
            now = _isoformat(_utc_now())
            event: dict[str, object] = {
                "at": now,
                "phase": normalized_phase,
                "message": normalized_message,
                "detail": normalized_detail,
            }
            if safe_diagnostics:
                event["diagnostics"] = safe_diagnostics
            events = [
                item for item in (state.get("processingEvents") or [])
                if isinstance(item, dict)
            ]
            events.append(event)
            state["processingPhase"] = normalized_phase
            state["processingMessage"] = normalized_message
            state["processingDetail"] = normalized_detail
            state["processingUpdatedAt"] = now
            state["processingEvents"] = events[-30:]
            self._write_state(session_id, state)

        logger.info(
            "%s upload session processing step: session_id=%s phase=%s message=%s detail=%s diagnostics=%s",
            self._format_label,
            session_id,
            normalized_phase,
            normalized_message,
            normalized_detail,
            safe_diagnostics,
        )
        return self.session_state(session_id)

    def finish_processing_success(self, session_id: str, result: dict[str, object]) -> dict[str, object]:
        with self._lock:
            state = self._read_state(session_id)
            state["status"] = "completed"
            state["processingCompletedAt"] = _isoformat(_utc_now())
            state["processingPhase"] = "completed"
            state["processingMessage"] = "Server-side import completed."
            state["processingDetail"] = "Step 2 of 2: import result is ready."
            state["processingUpdatedAt"] = state["processingCompletedAt"]
            state["result"] = result
            state.pop("error", None)
            self._remove_staged_files(session_id)
            self._write_state(session_id, state)
            logger.info(
                "%s upload session completed: session_id=%s imported=%s failed=%s",
                self._format_label,
                session_id,
                result.get("importedCount") if isinstance(result, dict) else None,
                result.get("failedCount") if isinstance(result, dict) else None,
            )
            return self._public_state(state)

    def finish_processing_failure(self, session_id: str, error: str) -> dict[str, object]:
        with self._lock:
            state = self._read_state(session_id)
            state["status"] = "failed"
            state["processingCompletedAt"] = _isoformat(_utc_now())
            state["error"] = str(error or f"The {self._format_label} files could not be imported.")
            state["processingPhase"] = "failed"
            state["processingMessage"] = "Server-side import failed."
            state["processingDetail"] = state["error"]
            state["processingUpdatedAt"] = state["processingCompletedAt"]
            state.pop("result", None)
            self._write_state(session_id, state)
            logger.warning(
                "%s upload session failed: session_id=%s error=%s",
                self._format_label,
                session_id,
                state["error"],
            )
            return self._public_state(state)

    def cancel_session(self, session_id: str) -> dict[str, object]:
        with self._lock:
            state = self._read_state(session_id)
            shutil.rmtree(self._session_dir(session_id), ignore_errors=True)
            public_state = self._public_state(state)
            public_state["cancelled"] = True
            return public_state

    def delete_session(self, session_id: str) -> None:
        with self._lock:
            shutil.rmtree(self._session_dir(session_id), ignore_errors=True)

    def cleanup_expired_sessions(self) -> int:
        if not self._root.exists():
            return 0
        removed = 0
        now = _utc_now()
        for session_dir in self._root.iterdir():
            if not session_dir.is_dir():
                continue
            state_path = session_dir / "session.json"
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
                expires_at = _parse_isoformat(state.get("expiresAt"))
            except Exception:
                continue
            if expires_at <= now:
                shutil.rmtree(session_dir, ignore_errors=True)
                removed += 1
        return removed

    def _safe_upload_file_name(self, file_name: str) -> str:
        name = Path(str(file_name or "")).name.strip()
        if not name:
            raise ValueError("Every upload file must include a fileName.")
        if not name.lower().endswith((*self._allowed_extensions, ".zip")):
            raise ValueError(self._invalid_extension_message)
        return name

    def _session_dir(self, session_id: str) -> Path:
        normalized = str(session_id or "").strip()
        if not normalized or not all(character in "0123456789abcdef" for character in normalized):
            raise ValueError("Unknown upload session.")
        return self._root / normalized

    def _state_path(self, session_id: str) -> Path:
        return self._session_dir(session_id) / "session.json"

    def _remove_staged_files(self, session_id: str) -> None:
        shutil.rmtree(self._session_dir(session_id) / "files", ignore_errors=True)

    def _read_state(self, session_id: str) -> dict[str, object]:
        state_path = self._state_path(session_id)
        if not state_path.is_file():
            raise KeyError("Unknown upload session.")
        return json.loads(state_path.read_text(encoding="utf-8"))

    def _write_state(self, session_id: str, state: dict[str, object]) -> None:
        state_path = self._state_path(session_id)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    def _raise_if_expired(self, session_id: str, state: dict[str, object]) -> None:
        try:
            expires_at = _parse_isoformat(state.get("expiresAt"))
        except Exception as exc:
            raise ValueError("Upload session state is invalid.") from exc
        if expires_at <= _utc_now():
            self.delete_session(session_id)
            raise KeyError("Upload session has expired.")

    def _file_entry(self, state: dict[str, object], file_id: str) -> dict[str, object]:
        normalized_file_id = str(file_id or "").strip()
        for item in state.get("files") or []:
            file_entry = item if isinstance(item, dict) else {}
            if str(file_entry.get("fileId") or "") == normalized_file_id:
                return file_entry
        raise KeyError("Unknown upload session file.")

    def _public_state(self, state: dict[str, object]) -> dict[str, object]:
        processing_events = [
            item
            for item in (state.get("processingEvents") or [])
            if isinstance(item, dict)
        ]
        return {
            "sessionId": state.get("sessionId"),
            "createdAt": state.get("createdAt"),
            "expiresAt": state.get("expiresAt"),
            "status": state.get("status") or "uploading",
            "processingStartedAt": state.get("processingStartedAt"),
            "processingCompletedAt": state.get("processingCompletedAt"),
            "processingPhase": state.get("processingPhase"),
            "processingMessage": state.get("processingMessage"),
            "processingDetail": state.get("processingDetail"),
            "processingUpdatedAt": state.get("processingUpdatedAt"),
            "processingEvents": processing_events[-12:],
            "result": state.get("result") if isinstance(state.get("result"), dict) else None,
            "error": state.get("error"),
            "chunkSizeBytes": state.get("chunkSizeBytes"),
            "files": [
                {
                    "fileId": item.get("fileId"),
                    "fileName": item.get("fileName"),
                    "sizeBytes": item.get("sizeBytes"),
                    "receivedBytes": item.get("receivedBytes"),
                    "complete": item.get("complete") is True,
                }
                for item in (state.get("files") or [])
                if isinstance(item, dict)
            ],
        }

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import shutil
from threading import RLock
import uuid

from ....config import Settings
from .manager import CsvLocalSource


@dataclass(frozen=True, slots=True)
class CsvUploadFileRequest:
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


def _safe_upload_file_name(file_name: str) -> str:
    name = Path(str(file_name or "")).name.strip()
    if not name:
        raise ValueError("Every upload file must include a fileName.")
    if not name.lower().endswith((".csv", ".zip")):
        raise ValueError("Only .csv and .zip files are supported in this ingestion flow.")
    return name


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


class CsvUploadSessionManager:
    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings
        self._root = settings.ingestion_upload_dir
        self._lock = RLock()

    def create_session(self, files: list[CsvUploadFileRequest]) -> dict[str, object]:
        if not files:
            raise ValueError("Choose at least one CSV or ZIP file before importing.")

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
                file_name = _safe_upload_file_name(request.file_name)
                size_bytes = int(request.size_bytes)
                max_size = (
                    self._settings.ingestion_upload_max_archive_bytes
                    if file_name.lower().endswith(".zip")
                    else self._settings.ingestion_upload_max_csv_bytes
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

    def source_files(self, session_id: str) -> list[CsvLocalSource]:
        with self._lock:
            state = self._read_state(session_id)
            self._raise_if_expired(session_id, state)
            sources: list[CsvLocalSource] = []
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
                    CsvLocalSource(
                        file_name=str(file_entry.get("fileName") or ""),
                        local_path=file_path,
                    )
                )
            return sources

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

    def _session_dir(self, session_id: str) -> Path:
        normalized = str(session_id or "").strip()
        if not normalized or not all(character in "0123456789abcdef" for character in normalized):
            raise ValueError("Unknown upload session.")
        return self._root / normalized

    def _state_path(self, session_id: str) -> Path:
        return self._session_dir(session_id) / "session.json"

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
        return {
            "sessionId": state.get("sessionId"),
            "createdAt": state.get("createdAt"),
            "expiresAt": state.get("expiresAt"),
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


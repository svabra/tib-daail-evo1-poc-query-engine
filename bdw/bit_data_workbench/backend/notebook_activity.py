from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import logging
import uuid
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from ..config import Settings
from .s3_hidden import NOTEBOOK_ACTIVITY_S3_PREFIX
from .s3_storage import ensure_s3_bucket, iter_s3_keys, s3_client


logger = logging.getLogger(__name__)
NOTEBOOK_ACTIVITY_SCHEMA_VERSION = 1
DEFAULT_NOTEBOOK_ACTIVITY_LIMIT = 100
VALID_NOTEBOOK_ACTIVITY_ACTIONS = {"open", "edit", "run"}


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


def _client_hash(client_id: str) -> str:
    normalized = str(client_id or "").strip()
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _activity_key(record: dict[str, Any]) -> str:
    touched_at = _safe_iso_date(str(record.get("touchedAt") or ""))
    stamp = touched_at.strftime("%Y%m%dT%H%M%S%fZ")
    return (
        f"{NOTEBOOK_ACTIVITY_S3_PREFIX}"
        f"{touched_at:%Y/%m/%d}/"
        f"{stamp}-{uuid.uuid4().hex[:12]}.json"
    )


def _normalize_action(action: str) -> str:
    normalized = str(action or "").strip().lower()
    if normalized == "edited":
        normalized = "edit"
    if normalized not in VALID_NOTEBOOK_ACTIVITY_ACTIONS:
        raise ValueError("Notebook activity action must be one of: open, edit, run.")
    return normalized


@dataclass(slots=True)
class NotebookActivityStore:
    settings: Settings

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

    def record(
        self,
        *,
        notebook_id: str,
        action: str,
        client_id: str = "",
    ) -> dict[str, Any]:
        if not self.available():
            return {
                "available": False,
                "recorded": False,
                "message": "S3 is not configured for notebook activity.",
            }

        normalized_notebook_id = str(notebook_id or "").strip()
        if not normalized_notebook_id:
            raise ValueError("Notebook activity requires a notebook id.")

        record = {
            "schemaVersion": NOTEBOOK_ACTIVITY_SCHEMA_VERSION,
            "notebookId": normalized_notebook_id,
            "action": _normalize_action(action),
            "touchedAt": utc_now_iso(),
            "clientHash": _client_hash(client_id),
        }
        key = _activity_key(record)
        client = s3_client(self.settings)
        ensure_s3_bucket(self.settings, self.bucket)
        client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            Metadata={"bdw-notebook-id": normalized_notebook_id},
        )
        return {**record, "available": True, "recorded": True, "bucket": self.bucket, "key": key}

    def list_recent(
        self,
        *,
        exclude_client_id: str = "",
        limit: int = DEFAULT_NOTEBOOK_ACTIVITY_LIMIT,
    ) -> dict[str, Any]:
        normalized_limit = max(1, min(500, int(limit or DEFAULT_NOTEBOOK_ACTIVITY_LIMIT)))
        if not self.available():
            return {
                "available": False,
                "activities": [],
                "message": "S3 is not configured for notebook activity.",
            }

        excluded_client_hash = _client_hash(exclude_client_id)
        activities: list[dict[str, Any]] = []
        client = s3_client(self.settings)
        try:
            keys = sorted(
                (
                    key
                    for key in iter_s3_keys(client, self.bucket, NOTEBOOK_ACTIVITY_S3_PREFIX)
                    if key.endswith(".json")
                ),
                reverse=True,
            )
            for key in keys:
                if len(activities) >= normalized_limit:
                    break
                record = self._read_activity_key(client, key)
                if not record:
                    continue
                if excluded_client_hash and str(record.get("clientHash") or "") == excluded_client_hash:
                    continue
                activities.append({**record, "bucket": self.bucket, "key": key})
        except (ClientError, BotoCoreError) as exc:
            logger.warning("Failed to list notebook activity: %s", exc)
            return {
                "available": False,
                "activities": [],
                "message": "Notebook activity could not be loaded from S3.",
            }

        return {"available": True, "activities": activities}

    def _read_activity_key(self, client, key: str) -> dict[str, Any]:
        try:
            response = client.get_object(Bucket=self.bucket, Key=key) or {}
        except Exception as exc:
            logger.warning("Skipping unreadable notebook activity object s3://%s/%s: %s", self.bucket, key, exc)
            return {}

        body = response.get("Body")
        raw = b"" if body is None else body.read()
        close = getattr(body, "close", None)
        if callable(close):
            close()
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("Skipping malformed notebook activity object s3://%s/%s: %s", self.bucket, key, exc)
            return {}
        if not isinstance(parsed, dict):
            return {}
        try:
            parsed["action"] = _normalize_action(str(parsed.get("action") or ""))
        except ValueError:
            return {}
        parsed["notebookId"] = str(parsed.get("notebookId") or "").strip()
        parsed["touchedAt"] = str(parsed.get("touchedAt") or "").strip()
        parsed["clientHash"] = str(parsed.get("clientHash") or "").strip()
        return parsed if parsed["notebookId"] and parsed["touchedAt"] else {}

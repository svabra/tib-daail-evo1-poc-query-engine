from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable
import uuid

import httpx

from .daca_client import normalize_loopback_url


class DacaPolicyError(RuntimeError):
    """Base error for the fail-closed DaCa policy enforcement point."""


class DacaPolicyDenied(DacaPolicyError):
    def __init__(self, reason: str = "default_deny") -> None:
        super().__init__("DaCa policy denied access to this data product.")
        self.reason = reason


class DacaPolicyUnavailable(DacaPolicyError):
    pass


@dataclass(slots=True, frozen=True)
class DacaPolicyDecision:
    allow: bool
    reason: str
    decision_id: str


def rfc3339_utc(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    return normalized.isoformat().replace("+00:00", "Z")


class DacaPolicyEnforcer:
    def __init__(
        self,
        *,
        decision_url: str,
        timeout_seconds: float = 2.0,
        now_provider: Callable[[], datetime] | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._decision_url = normalize_loopback_url(decision_url)
        self._timeout_seconds = max(0.1, float(timeout_seconds))
        self._now_provider = now_provider or (lambda: datetime.now(UTC))
        self._transport = transport

    def authorize(
        self,
        *,
        subject_id: str,
        product_id: str,
        method: str,
        path: str,
        request_id: str = "",
    ) -> DacaPolicyDecision:
        normalized_subject = str(subject_id or "").strip()
        if not normalized_subject:
            raise ValueError("A non-empty DaCa subject is required.")

        request_timestamp = rfc3339_utc(self._now_provider())
        normalized_request_id = str(request_id or "").strip() or str(uuid.uuid4())
        payload = {
            "input": {
                "subject": {"id": normalized_subject, "type": "person"},
                "action": "data.read",
                "resource": {"id": str(product_id)},
                "endpoint": {"protocol": "http-rest"},
                "request": {
                    "method": str(method or "GET").upper(),
                    "path": str(path or "/"),
                    "requestId": normalized_request_id,
                },
                "context": {
                    "requestTimestamp": request_timestamp,
                    # Kept for backward-compatible date-bounded policies while
                    # weeklyAvailability rolls out through the same OPA bundle.
                    "currentDate": request_timestamp[:10],
                },
            }
        }

        try:
            with httpx.Client(
                timeout=self._timeout_seconds,
                transport=self._transport,
            ) as client:
                response = client.post(
                    self._decision_url,
                    json=payload,
                    headers={"Accept": "application/json"},
                )
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            raise DacaPolicyUnavailable(
                "DaCa policy enforcement is currently unavailable."
            ) from exc

        if response.status_code != 200:
            raise DacaPolicyUnavailable(
                "DaCa policy enforcement returned an unexpected status."
            )
        try:
            response_payload = response.json()
        except ValueError as exc:
            raise DacaPolicyUnavailable(
                "DaCa policy enforcement returned malformed JSON."
            ) from exc
        if not isinstance(response_payload, dict):
            raise DacaPolicyUnavailable(
                "DaCa policy enforcement returned an invalid response."
            )
        result = response_payload.get("result")
        if not isinstance(result, dict) or type(result.get("allow")) is not bool:
            raise DacaPolicyUnavailable(
                "DaCa policy enforcement returned no valid decision."
            )

        decision = DacaPolicyDecision(
            allow=result["allow"],
            reason=str(result.get("reason") or "").strip()
            or ("policy_allow" if result["allow"] else "default_deny"),
            decision_id=str(response_payload.get("decision_id") or "").strip(),
        )
        if not decision.allow:
            raise DacaPolicyDenied(decision.reason)
        return decision

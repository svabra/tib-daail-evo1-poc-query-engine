from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit
import uuid

import httpx


METADATA_PUBLICATION_PATH = "/api/v1/metadata-publications"


def normalize_loopback_url(value: str) -> str:
    """Prefer IPv4 loopback for local integrations that are IPv6-sensitive."""

    normalized = str(value or "").strip().rstrip("/")
    parsed = urlsplit(normalized)
    if parsed.hostname != "localhost":
        return normalized

    port = f":{parsed.port}" if parsed.port is not None else ""
    return urlunsplit(
        (parsed.scheme, f"127.0.0.1{port}", parsed.path, parsed.query, parsed.fragment)
    ).rstrip("/")


def response_error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        detail = payload.get("detail")
        if isinstance(detail, str) and detail.strip():
            return detail.strip()[:1000]
        if isinstance(detail, list):
            return "DaCa rejected the metadata publication payload."
    return (response.text or response.reason_phrase or "DaCa request failed.").strip()[:1000]


class DacaPublicationError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        kind: str,
        upstream_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.upstream_status = upstream_status

    @property
    def client_status(self) -> int:
        if self.kind == "conflict":
            return 409
        if self.kind == "validation":
            return 422
        if self.kind == "timeout":
            return 504
        if self.kind in {"disabled", "unavailable"}:
            return 503
        return 502


@dataclass(slots=True, frozen=True)
class DacaPublicationResult:
    publication_id: str
    product_id: str
    state: str
    created: bool
    missing_fields: tuple[str, ...]
    task_ids: tuple[str, ...]

    @classmethod
    def from_payload(cls, payload: Any) -> "DacaPublicationResult":
        if not isinstance(payload, dict):
            raise DacaPublicationError(
                "DaCa returned an invalid metadata publication response.",
                kind="invalid_response",
            )

        publication_id = str(payload.get("publicationId") or "").strip()
        product_id = str(payload.get("productId") or "").strip()
        state = str(payload.get("state") or "").strip()
        try:
            uuid.UUID(publication_id)
            uuid.UUID(product_id)
        except (ValueError, AttributeError) as exc:
            raise DacaPublicationError(
                "DaCa returned invalid publication identifiers.",
                kind="invalid_response",
            ) from exc
        if state not in {"pending_review", "published_incomplete", "published"}:
            raise DacaPublicationError(
                "DaCa returned an unsupported metadata publication state.",
                kind="invalid_response",
            )

        raw_missing_fields = payload.get("missingFields") or []
        raw_task_ids = payload.get("taskIds") or []
        if not isinstance(raw_missing_fields, list) or not isinstance(raw_task_ids, list):
            raise DacaPublicationError(
                "DaCa returned an invalid metadata publication task list.",
                kind="invalid_response",
            )
        task_ids: list[str] = []
        for raw_task_id in raw_task_ids:
            task_id = str(raw_task_id or "").strip()
            try:
                uuid.UUID(task_id)
            except (ValueError, AttributeError) as exc:
                raise DacaPublicationError(
                    "DaCa returned an invalid workflow task identifier.",
                    kind="invalid_response",
                ) from exc
            task_ids.append(task_id)

        return cls(
            publication_id=publication_id,
            product_id=product_id,
            state=state,
            created=bool(payload.get("created")),
            missing_fields=tuple(
                str(item).strip()
                for item in raw_missing_fields
                if str(item).strip()
            ),
            task_ids=tuple(task_ids),
        )


@dataclass(slots=True, frozen=True)
class DacaProductStatus:
    product_id: str
    state: str
    missing_fields: tuple[str, ...]


class DacaMetadataPublicationClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float = 5.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = normalize_loopback_url(base_url)
        self._timeout_seconds = max(0.1, float(timeout_seconds))
        self._transport = transport

    def publish(self, payload: dict[str, object]) -> DacaPublicationResult:
        url = f"{self._base_url}{METADATA_PUBLICATION_PATH}"
        response: httpx.Response | None = None
        for attempt in range(2):
            try:
                with httpx.Client(
                    timeout=self._timeout_seconds,
                    transport=self._transport,
                ) as client:
                    response = client.post(
                        url,
                        json=payload,
                        headers={"Accept": "application/json"},
                    )
                break
            except httpx.TimeoutException as exc:
                # The first POST may have committed before its response was
                # lost. DaCa's source identity is idempotent, so one immediate
                # replay is the reconciliation check; both attempts use the
                # exact same sourceProductId and payload hash.
                if attempt == 0:
                    continue
                raise DacaPublicationError(
                    "DaCa did not confirm the publication before the timeout. "
                    "The local endpoint remains unpublished; retrying is idempotent.",
                    kind="timeout",
                ) from exc
            except httpx.RequestError as exc:
                raise DacaPublicationError(
                    "DaCa is currently unreachable. The local endpoint remains unpublished.",
                    kind="unavailable",
                ) from exc

        if response is None:  # pragma: no cover - guarded by the loop above
            raise DacaPublicationError(
                "DaCa did not return a publication response.",
                kind="unavailable",
            )

        if response.status_code not in {200, 201}:
            detail = response_error_detail(response)
            if response.status_code == 409:
                kind = "conflict"
            elif response.status_code == 422:
                kind = "validation"
            elif response.status_code == 404:
                kind = "disabled"
            else:
                kind = "unavailable" if response.status_code >= 500 else "upstream"
            raise DacaPublicationError(
                f"DaCa metadata publication failed: {detail}",
                kind=kind,
                upstream_status=response.status_code,
            )

        try:
            response_payload = response.json()
        except ValueError as exc:
            raise DacaPublicationError(
                "DaCa returned a non-JSON metadata publication response.",
                kind="invalid_response",
            ) from exc
        return DacaPublicationResult.from_payload(response_payload)

    def product_status(self, *, product_id: str, owner_user_id: str) -> DacaProductStatus:
        try:
            uuid.UUID(str(product_id or "").strip())
        except (ValueError, AttributeError) as exc:
            raise DacaPublicationError(
                "The stored DaCa product identifier is invalid.",
                kind="invalid_response",
            ) from exc
        url = f"{self._base_url}/api/v1/data-products/{product_id}"
        try:
            with httpx.Client(
                timeout=min(self._timeout_seconds, 1.0),
                transport=self._transport,
            ) as client:
                response = client.get(
                    url,
                    headers={
                        "Accept": "application/json",
                        "X-DaCa-User": str(owner_user_id or "").strip(),
                    },
                )
        except httpx.TimeoutException as exc:
            raise DacaPublicationError(
                "DaCa status reconciliation timed out.", kind="timeout"
            ) from exc
        except httpx.RequestError as exc:
            raise DacaPublicationError(
                "DaCa status reconciliation is unavailable.", kind="unavailable"
            ) from exc
        if response.status_code != 200:
            raise DacaPublicationError(
                f"DaCa status reconciliation failed: {response_error_detail(response)}",
                kind="unavailable",
                upstream_status=response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise DacaPublicationError(
                "DaCa returned a non-JSON product status.", kind="invalid_response"
            ) from exc
        if not isinstance(payload, dict) or str(payload.get("id") or "") != product_id:
            raise DacaPublicationError(
                "DaCa returned an invalid product status.", kind="invalid_response"
            )
        quality = payload.get("quality") if isinstance(payload.get("quality"), dict) else {}
        criteria = quality.get("criteria") if isinstance(quality, dict) else []
        missing_fields = tuple(
            str(item.get("label") or item.get("id") or "").strip()
            for item in (criteria if isinstance(criteria, list) else [])
            if isinstance(item, dict)
            and item.get("complete") is not True
            and str(item.get("label") or item.get("id") or "").strip()
        )
        active = (
            str(payload.get("lifecycle") or "") == "active"
            and payload.get("discoverable") is True
            and isinstance(payload.get("activePolicyRevision"), int)
        )
        score = quality.get("score") if isinstance(quality, dict) else 0
        state = "published" if active and score == 6 else (
            "published_incomplete" if active else "pending_review"
        )
        return DacaProductStatus(
            product_id=product_id,
            state=state,
            missing_fields=missing_fields,
        )

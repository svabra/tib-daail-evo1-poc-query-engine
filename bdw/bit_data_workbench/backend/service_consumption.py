from __future__ import annotations

from calendar import month_abbr
import json
import logging
import math
import ssl
import threading
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib import error as urllib_error
from urllib import request as urllib_request

from botocore.exceptions import BotoCoreError, ClientError

from ..config import Settings
from .s3_hidden import SERVICE_CONSUMPTION_S3_PREFIX
from .s3_storage import list_s3_buckets_from_client, s3_client


logger = logging.getLogger(__name__)

SERVICE_CONSUMPTION_WINDOWS: dict[str, dict[str, timedelta | int]] = {
    "48h": {
        "duration": timedelta(hours=48),
        "bucket_seconds": 600,
    },
}
SERVICE_CONSUMPTION_DEFAULT_WINDOW = "48h"
SERVICE_CONSUMPTION_SNAPSHOT_SCHEMA_VERSION = 1
SERVICE_CONSUMPTION_S3_STATE_KEY = f"{SERVICE_CONSUMPTION_S3_PREFIX}state.json"
SERVICE_CONSUMPTION_S3_FLUSH_INTERVAL = timedelta(minutes=5)
SERVICE_CONSUMPTION_S3_WARNING_INTERVAL = timedelta(minutes=15)
TOPOLOGY_COPY = (
    "The current PoC runs API, backend, frontend, and query execution on a single node."
)
SCALE_OUT_COPY = (
    "Query nodes will scale out automatically under higher query pressure once DAAIF goes live."
)
SERVICE_ACCOUNT_TOKEN_PATH = Path(
    "/var/run/secrets/kubernetes.io/serviceaccount/token"
)
SERVICE_ACCOUNT_CA_CERT_PATH = Path(
    "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)
KUBERNETES_API_BASE_URL = "https://kubernetes.default.svc"
KUBERNETES_REQUEST_TIMEOUT_SECONDS = 3.0
PRUNE_INTERVAL = timedelta(minutes=15)
COST_CURRENCY = "CHF"
HOURS_PER_MONTH_FOR_COSTING = 730.0
FORECAST_WINDOW_DAYS = 30
POC_CLIENT_START_YEAR = 2025
APPLICATION_SERVICE_ANNUAL_CHF = 500.0
PG_SERVICE_DAILY_CHF_PER_INSTANCE = 15.14
PG_INSTANCE_SIZE_GB = 80.0
PG_INSTANCE_SIZE_BYTES = int(PG_INSTANCE_SIZE_GB * 1_000_000_000)
PG_STATIC_INSTANCES: tuple[tuple[str, int], ...] = (
    ("OLTP", PG_INSTANCE_SIZE_BYTES),
    ("OLAP", PG_INSTANCE_SIZE_BYTES),
)
SERVICE_COST_CATEGORY_ORDER: tuple[str, ...] = (
    "container",
    "application",
    "filesystem",
    "s3",
    "pg",
)


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalized_float(value: float | None, *, digits: int = 4) -> float | None:
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return round(value, digits)


def _normalized_percent(
    numerator: float | None,
    denominator: float | None,
) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return _normalized_float((numerator / denominator) * 100.0, digits=2)


def _normalized_currency(value: float | None) -> float | None:
    return _normalized_float(value, digits=2)


def _normalized_cost_value(value: float | None) -> float | None:
    return _normalized_float(value, digits=8)


def _decimal_gb_from_bytes(value: int | float | None) -> float | None:
    if value is None:
        return None
    normalized = float(value)
    if math.isnan(normalized) or math.isinf(normalized) or normalized < 0:
        return None
    return normalized / 1_000_000_000.0


def parse_kubernetes_cpu_quantity(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        raise ValueError("CPU quantity is required.")
    if text.endswith("n"):
        return float(text[:-1]) / 1_000_000_000
    if text.endswith("u"):
        return float(text[:-1]) / 1_000_000
    if text.endswith("m"):
        return float(text[:-1]) / 1_000
    return float(text)


def parse_kubernetes_memory_quantity(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        raise ValueError("Memory quantity is required.")

    binary_suffixes = {
        "Ki": 1024**1,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "Pi": 1024**5,
        "Ei": 1024**6,
    }
    decimal_suffixes = {
        "n": 10**-9,
        "u": 10**-6,
        "m": 10**-3,
        "k": 10**3,
        "K": 10**3,
        "M": 10**6,
        "G": 10**9,
        "T": 10**12,
        "P": 10**15,
        "E": 10**18,
        "": 1,
    }

    suffix = ""
    number_text = text
    for candidate in sorted(binary_suffixes, key=len, reverse=True):
        if text.endswith(candidate):
            suffix = candidate
            number_text = text[: -len(candidate)]
            break
    else:
        for candidate in sorted(decimal_suffixes, key=len, reverse=True):
            if candidate and text.endswith(candidate):
                suffix = candidate
                number_text = text[: -len(candidate)]
                break

    multiplier = binary_suffixes.get(suffix)
    if multiplier is None:
        multiplier = decimal_suffixes.get(suffix)
    if multiplier is None:
        raise ValueError(f"Unsupported memory quantity: {text}")
    return int(float(number_text) * multiplier)


def parse_cgroup_cpu_usage_micros(cpu_stat_text: str) -> int:
    for raw_line in cpu_stat_text.splitlines():
        key, _separator, value = raw_line.partition(" ")
        if key.strip() == "usage_usec":
            return int(value.strip())
    raise ValueError("cpu.stat does not contain usage_usec.")


def parse_cgroup_cpu_limit_cores(raw_value: str) -> float | None:
    quota_text, _separator, period_text = raw_value.strip().partition(" ")
    if not quota_text or not period_text or quota_text == "max":
        return None
    quota = int(quota_text)
    period = int(period_text)
    if quota <= 0 or period <= 0:
        return None
    return quota / period


def parse_cgroup_memory_limit_bytes(raw_value: str) -> int | None:
    text = raw_value.strip()
    if not text or text == "max":
        return None
    limit = int(text)
    if limit <= 0 or limit >= 2**60:
        return None
    return limit


def _read_text_file(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _read_int_file(path: Path) -> int | None:
    raw_value = _read_text_file(path)
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except ValueError:
        return None


def _json_clone(payload: Any) -> Any:
    return json.loads(json.dumps(payload))


def _s3_missing_object_error(error: Exception) -> bool:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        code = str((response.get("Error") or {}).get("Code") or "").strip()
        return code in {"404", "NoSuchKey", "NotFound"}
    return error.__class__.__name__ in {"NoSuchKey", "NotFound"}


@dataclass(slots=True)
class AppMetricsReading:
    cpu_usage_micros: int | None
    cpu_limit_cores: float | None
    memory_usage_bytes: int | None
    memory_limit_bytes: int | None
    observed_at: datetime
    delta_usage_micros: int | None = None
    elapsed_seconds: float | None = None


@dataclass(slots=True)
class CachedS3Reading:
    metrics: dict[str, int | None]
    status: dict[str, object]
    sampled_at: datetime


@dataclass(slots=True)
class FinancialYearCache:
    year: int
    annual_budget_chf: float | None
    budget_updated_at: datetime | None
    breakdown_year_to_date: dict[str, float]
    daily_totals: dict[str, float]


class KubernetesApiClient:
    def __init__(self) -> None:
        self._token: str | None = None
        self._ssl_context: ssl.SSLContext | None = None

    def available(self) -> bool:
        return SERVICE_ACCOUNT_TOKEN_PATH.is_file()

    def get_json(self, path: str) -> dict[str, object]:
        token = self._service_account_token()
        ssl_context = self._service_account_ssl_context()
        if not token:
            raise RuntimeError("Kubernetes service account token is unavailable.")

        request = urllib_request.Request(
            f"{KUBERNETES_API_BASE_URL.rstrip('/')}{path}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        try:
            with urllib_request.urlopen(
                request,
                timeout=KUBERNETES_REQUEST_TIMEOUT_SECONDS,
                context=ssl_context,
            ) as response:
                payload = response.read().decode("utf-8")
        except urllib_error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(
                f"Kubernetes API request failed with {exc.code}: {detail or exc.reason}"
            ) from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"Kubernetes API request failed: {exc.reason}") from exc

        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Kubernetes API returned invalid JSON.") from exc

    def _service_account_token(self) -> str | None:
        if self._token is not None:
            return self._token
        token = _read_text_file(SERVICE_ACCOUNT_TOKEN_PATH)
        self._token = token
        return token

    def _service_account_ssl_context(self) -> ssl.SSLContext | None:
        if self._ssl_context is not None:
            return self._ssl_context
        if SERVICE_ACCOUNT_CA_CERT_PATH.is_file():
            self._ssl_context = ssl.create_default_context(
                cafile=SERVICE_ACCOUNT_CA_CERT_PATH.as_posix()
            )
            return self._ssl_context
        self._ssl_context = ssl.create_default_context()
        return self._ssl_context


class ServiceConsumptionMonitor:
    def __init__(
        self,
        settings: Settings,
        *,
        state_change_callback: Callable[[dict[str, Any]], None],
        s3_client_factory=s3_client,
    ) -> None:
        self._settings = settings
        self._state_change_callback = state_change_callback
        self._s3_client_factory = s3_client_factory
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._version = 0
        self._samples: list[dict[str, Any]] = []
        self._cost_events: list[dict[str, Any]] = []
        self._budget_entries: dict[str, dict[str, Any]] = {}
        self._latest_sample: dict[str, Any] | None = None
        self._latest_sample_timestamp: datetime | None = None
        self._last_app_metrics_reading: AppMetricsReading | None = None
        self._last_s3_reading: CachedS3Reading | None = None
        self._last_pruned_at: datetime | None = None
        self._financial_cache: FinancialYearCache | None = None
        self._kubernetes_client = KubernetesApiClient()
        self._s3_snapshot_dirty = False
        self._s3_snapshot_change_version = 0
        self._s3_snapshot_flush_in_progress = False
        self._last_s3_snapshot_flush_at: datetime | None = datetime.now(UTC)
        self._last_s3_snapshot_warning_at: datetime | None = None

    def start(self) -> None:
        restored_snapshot = self._load_s3_snapshot_payload()
        snapshot_to_publish: dict[str, Any] | None = None
        now = datetime.now(UTC)
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                return
            self._stop_event.clear()
            if restored_snapshot is not None:
                self._restore_s3_snapshot_payload_locked(restored_snapshot)
            self._prune_history_locked(reference_time=now, force=True)
            restored_sample = self._load_latest_sample_locked()
            if restored_sample is not None:
                self._latest_sample = restored_sample
                self._latest_sample_timestamp = _parse_iso_datetime(
                    restored_sample.get("timestampUtc")
                )
                self._restore_s3_cache_locked(restored_sample)
                self._version += 1
                snapshot_to_publish = self._build_realtime_payload_locked()
            self._last_s3_snapshot_flush_at = now
            self._worker = threading.Thread(
                target=self._run,
                name="bdw-service-consumption",
                daemon=True,
            )
            self._worker.start()
        if snapshot_to_publish is not None:
            self._state_change_callback(snapshot_to_publish)

    def stop(self) -> None:
        self._stop_event.set()
        worker: threading.Thread | None = None
        with self._lock:
            worker = self._worker
            self._worker = None
        if worker is not None and worker.is_alive():
            worker.join(timeout=1.0)

    def _service_consumption_s3_bucket(self) -> str:
        return str(getattr(self._settings, "s3_bucket", "") or "").strip()

    def _service_consumption_s3_available(self) -> bool:
        return bool(
            self._service_consumption_s3_bucket()
            and self._settings.s3_endpoint
            and self._settings.current_s3_access_key_id()
            and self._settings.current_s3_secret_access_key()
        )

    def _load_s3_snapshot_payload(self) -> dict[str, Any] | None:
        if not self._service_consumption_s3_available():
            return None

        bucket = self._service_consumption_s3_bucket()
        try:
            response = self._s3_client_factory(self._settings).get_object(
                Bucket=bucket,
                Key=SERVICE_CONSUMPTION_S3_STATE_KEY,
            )
        except Exception as exc:
            if _s3_missing_object_error(exc):
                return None
            logger.warning(
                "Failed to restore service-consumption S3 snapshot: %s",
                exc,
            )
            return None

        body = response.get("Body") if isinstance(response, dict) else None
        raw = b"" if body is None else body.read()
        close = getattr(body, "close", None)
        if callable(close):
            close()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning(
                "Skipping malformed service-consumption S3 snapshot: %s",
                exc,
            )
            return None
        return payload if isinstance(payload, dict) else None

    def _restore_s3_snapshot_payload_locked(self, payload: dict[str, Any]) -> None:
        cutoff = datetime.now(UTC) - timedelta(
            hours=self._settings.service_consumption_retention_hours
        )
        samples = [
            _json_clone(sample)
            for sample in payload.get("samples", []) or []
            if isinstance(sample, dict)
            and (observed_at := _parse_iso_datetime(sample.get("timestampUtc"))) is not None
            and observed_at >= cutoff
        ]
        samples.sort(
            key=lambda sample: _parse_iso_datetime(sample.get("timestampUtc"))
            or datetime.min.replace(tzinfo=UTC)
        )
        self._samples = samples

        cost_cutoff = self._financial_cost_event_cutoff(datetime.now(UTC))
        self._cost_events = [
            _json_clone(cost_event)
            for cost_event in payload.get("costEvents", []) or []
            if isinstance(cost_event, dict)
            and (observed_at := _parse_iso_datetime(cost_event.get("endAtUtc"))) is not None
            and observed_at >= cost_cutoff
        ]

        budgets = payload.get("budgets") or {}
        self._budget_entries = {
            str(year): _json_clone(entry)
            for year, entry in budgets.items()
            if isinstance(entry, dict)
        } if isinstance(budgets, dict) else {}

        latest_sample = payload.get("latestSample") if isinstance(payload.get("latestSample"), dict) else None
        if latest_sample is None and self._samples:
            latest_sample = self._samples[-1]
        self._latest_sample = _json_clone(latest_sample) if isinstance(latest_sample, dict) else None
        self._latest_sample_timestamp = (
            _parse_iso_datetime(self._latest_sample.get("timestampUtc"))
            if isinstance(self._latest_sample, dict)
            else None
        )
        if self._latest_sample is not None:
            self._restore_s3_cache_locked(self._latest_sample)

        self._financial_cache = None
        self._s3_snapshot_dirty = False
        self._s3_snapshot_change_version = 0

    def _mark_s3_snapshot_dirty_locked(self) -> None:
        self._s3_snapshot_dirty = True
        self._s3_snapshot_change_version += 1

    def _s3_snapshot_payload_locked(self) -> dict[str, Any]:
        return {
            "schemaVersion": SERVICE_CONSUMPTION_SNAPSHOT_SCHEMA_VERSION,
            "updatedAtUtc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "retentionHours": self._settings.service_consumption_retention_hours,
            "latestSample": _json_clone(self._latest_sample)
            if self._latest_sample is not None
            else None,
            "samples": _json_clone(self._samples),
            "costEvents": _json_clone(self._cost_events),
            "budgets": _json_clone(self._budget_entries),
            "version": self._version,
        }

    def _prepare_s3_snapshot_flush_locked(
        self,
        observed_at: datetime,
    ) -> tuple[dict[str, Any], int] | None:
        if (
            not self._service_consumption_s3_available()
            or not self._s3_snapshot_dirty
            or self._s3_snapshot_flush_in_progress
        ):
            return None
        if (
            self._last_s3_snapshot_flush_at is not None
            and observed_at - self._last_s3_snapshot_flush_at < SERVICE_CONSUMPTION_S3_FLUSH_INTERVAL
        ):
            return None

        self._s3_snapshot_flush_in_progress = True
        return self._s3_snapshot_payload_locked(), self._s3_snapshot_change_version

    def _flush_s3_snapshot_payload(
        self,
        payload: dict[str, Any],
        change_version: int,
        observed_at: datetime,
    ) -> None:
        success = False
        try:
            self._s3_client_factory(self._settings).put_object(
                Bucket=self._service_consumption_s3_bucket(),
                Key=SERVICE_CONSUMPTION_S3_STATE_KEY,
                Body=json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
                ContentType="application/json",
                Metadata={"bdw-internal": "service-consumption"},
            )
            success = True
        except Exception as exc:
            self._log_s3_snapshot_warning(exc)
        finally:
            with self._lock:
                self._s3_snapshot_flush_in_progress = False
                self._last_s3_snapshot_flush_at = observed_at
                if success:
                    if self._s3_snapshot_change_version == change_version:
                        self._s3_snapshot_dirty = False

    def _log_s3_snapshot_warning(self, exc: Exception) -> None:
        now = datetime.now(UTC)
        with self._lock:
            if (
                self._last_s3_snapshot_warning_at is not None
                and now - self._last_s3_snapshot_warning_at < SERVICE_CONSUMPTION_S3_WARNING_INTERVAL
            ):
                return
            self._last_s3_snapshot_warning_at = now
        logger.warning("Failed to persist service-consumption S3 snapshot: %s", exc)

    def realtime_payload(self) -> dict[str, Any]:
        with self._lock:
            return self._build_realtime_payload_locked()

    def update_budget(
        self,
        *,
        year: int,
        annual_budget_chf: float,
    ) -> dict[str, object]:
        normalized_budget = _normalized_currency(float(annual_budget_chf))
        current_year = datetime.now(UTC).year
        if year != current_year:
            raise ValueError(
                f"Budget updates only support the current UTC year ({current_year})."
            )
        if normalized_budget is None or normalized_budget < 0:
            raise ValueError("Annual budget must be a non-negative CHF amount.")

        budget_updated_at = datetime.now(UTC).replace(microsecond=0)
        snapshot_to_publish: dict[str, Any]
        with self._lock:
            self._write_budget_locked(
                year=year,
                annual_budget_chf=normalized_budget,
                updated_at=budget_updated_at,
            )
            if self._financial_cache is None or self._financial_cache.year != year:
                self._financial_cache = self._rebuild_financial_cache_locked(year)
            self._financial_cache.annual_budget_chf = normalized_budget
            self._financial_cache.budget_updated_at = budget_updated_at
            self._version += 1
            snapshot_to_publish = self._build_realtime_payload_locked()

        self._state_change_callback(snapshot_to_publish)
        return {
            "year": year,
            "annualBudgetChf": normalized_budget,
            "savedAtUtc": budget_updated_at.isoformat(),
            "version": snapshot_to_publish.get("version"),
        }

    def state_payload(
        self,
        *,
        window: str = SERVICE_CONSUMPTION_DEFAULT_WINDOW,
    ) -> dict[str, Any]:
        normalized_window = self._normalize_window(window)
        duration = self._window_duration(normalized_window)
        bucket_seconds = self._window_bucket_seconds(normalized_window)
        cutoff = datetime.now(UTC) - duration

        with self._lock:
            latest_sample = self._public_sample(self._latest_sample)
            status = self._status_payload_locked()
            topology = self._topology_payload_locked()
            version = self._version
            samples = self._load_samples_since_locked(cutoff)
            financial = self._financial_payload_locked()

        return {
            "version": version,
            "window": normalized_window,
            "latest": latest_sample,
            "status": status,
            "topology": topology,
            "cpuHistory": self._build_recent_metric_history(
                samples,
                metric_group="cpu",
                app_series="app",
                node_series="node",
                value_key="coresUsed",
                bucket_seconds=bucket_seconds,
                integer_values=False,
            ),
            "memoryHistory": self._build_recent_metric_history(
                samples,
                metric_group="memory",
                app_series="app",
                node_series="node",
                value_key="bytesUsed",
                bucket_seconds=bucket_seconds,
                integer_values=True,
            ),
            "s3History": self._build_s3_history(samples),
            "persistentVolumeHistory": self._build_persistent_volume_history(
                samples,
            ),
            "financial": financial,
        }

    def _normalize_window(self, value: str) -> str:
        _ = value
        return SERVICE_CONSUMPTION_DEFAULT_WINDOW

    def _window_duration(self, window: str) -> timedelta:
        return SERVICE_CONSUMPTION_WINDOWS[window]["duration"]  # type: ignore[return-value]

    def _window_bucket_seconds(self, window: str) -> int:
        return int(SERVICE_CONSUMPTION_WINDOWS[window]["bucket_seconds"])

    def _run(self) -> None:
        self._sample_once(initial=True)
        while not self._stop_event.wait(self._seconds_until_next_cpu_memory_sample()):
            self._sample_once(initial=False)

    def _sample_once(self, *, initial: bool) -> None:
        try:
            sample = self._collect_sample(initial=initial)
        except Exception:
            logger.exception("Service-consumption sample collection failed.")
            return
        snapshot = self._store_sample(sample)
        self._state_change_callback(snapshot)

    def _seconds_until_next_cpu_memory_sample(self) -> float:
        interval_seconds = max(
            1,
            self._settings.service_consumption_cpu_memory_interval_seconds,
        )
        epoch_seconds = datetime.now(UTC).timestamp()
        next_epoch = (math.floor(epoch_seconds / interval_seconds) + 1) * interval_seconds
        return max(0.25, next_epoch - epoch_seconds)

    def _store_sample(self, sample: dict[str, Any]) -> dict[str, Any]:
        sample_timestamp = _parse_iso_datetime(sample.get("timestampUtc")) or datetime.now(
            UTC
        )
        flush_request: tuple[dict[str, Any], int] | None = None
        with self._lock:
            previous_sample = self._latest_sample
            self._append_sample_locked(sample)
            cost_event = self._build_cost_event(previous_sample, sample)
            if cost_event is not None:
                self._append_cost_event_locked(cost_event)
                self._update_financial_cache_with_cost_event_locked(cost_event)
            self._latest_sample = sample
            self._latest_sample_timestamp = sample_timestamp
            self._restore_s3_cache_locked(sample)
            self._prune_history_locked(reference_time=sample_timestamp)
            self._version += 1
            flush_request = self._prepare_s3_snapshot_flush_locked(sample_timestamp)
            snapshot = self._build_realtime_payload_locked()
        if flush_request is not None:
            self._flush_s3_snapshot_payload(
                flush_request[0],
                flush_request[1],
                sample_timestamp,
            )
        return snapshot

    def _collect_sample(self, *, initial: bool) -> dict[str, Any]:
        app_metrics = self._read_app_metrics(initial=initial)
        node_metrics, node_status = self._collect_node_metrics()
        s3_metrics, s3_status, s3_sampled_at = self._current_s3_metrics(
            app_metrics.observed_at,
            force_refresh=initial,
        )
        persistent_volume_metrics, persistent_volume_status = (
            self._collect_persistent_volume_metrics()
        )
        timestamp = app_metrics.observed_at.astimezone(UTC).replace(microsecond=0)

        app_cpu_cores_used = self._app_cpu_cores_used(app_metrics)
        app_cpu_limit = _normalized_float(app_metrics.cpu_limit_cores)
        app_memory_used = app_metrics.memory_usage_bytes
        app_memory_limit = app_metrics.memory_limit_bytes

        sample: dict[str, Any] = {
            "timestampUtc": timestamp.isoformat(),
            "cpu": {
                "app": {
                    "coresUsed": _normalized_float(app_cpu_cores_used),
                    "percentOfLimit": _normalized_percent(
                        app_cpu_cores_used,
                        app_metrics.cpu_limit_cores,
                    ),
                    "limitCores": app_cpu_limit,
                },
                "node": {
                    "coresUsed": _normalized_float(node_metrics.get("cpuCoresUsed")),
                    "percentOfCapacity": _normalized_percent(
                        node_metrics.get("cpuCoresUsed"),
                        node_metrics.get("cpuCapacityCores"),
                    ),
                    "capacityCores": _normalized_float(
                        node_metrics.get("cpuCapacityCores")
                    ),
                },
            },
            "memory": {
                "app": {
                    "bytesUsed": int(app_memory_used)
                    if app_memory_used is not None
                    else None,
                    "percentOfLimit": _normalized_percent(
                        float(app_memory_used) if app_memory_used is not None else None,
                        float(app_memory_limit)
                        if app_memory_limit is not None
                        else None,
                    ),
                    "limitBytes": int(app_memory_limit)
                    if app_memory_limit is not None
                    else None,
                },
                "node": {
                    "bytesUsed": int(node_metrics.get("memoryBytesUsed"))
                    if node_metrics.get("memoryBytesUsed") is not None
                    else None,
                    "percentOfCapacity": _normalized_percent(
                        float(node_metrics.get("memoryBytesUsed"))
                        if node_metrics.get("memoryBytesUsed") is not None
                        else None,
                        float(node_metrics.get("memoryCapacityBytes"))
                        if node_metrics.get("memoryCapacityBytes") is not None
                        else None,
                    ),
                    "capacityBytes": int(node_metrics.get("memoryCapacityBytes"))
                    if node_metrics.get("memoryCapacityBytes") is not None
                    else None,
                },
            },
            "s3": {
                "totalBytes": int(s3_metrics.get("totalBytes"))
                if s3_metrics.get("totalBytes") is not None
                else None,
                "bucketCount": int(s3_metrics.get("bucketCount") or 0),
                "sampledAtUtc": s3_sampled_at.astimezone(UTC)
                .replace(microsecond=0)
                .isoformat(),
            },
            "persistentVolume": {
                "bytesUsed": int(persistent_volume_metrics.get("bytesUsed"))
                if persistent_volume_metrics.get("bytesUsed") is not None
                else None,
                "bytesCapacity": int(persistent_volume_metrics.get("bytesCapacity"))
                if persistent_volume_metrics.get("bytesCapacity") is not None
                else None,
                "bytesProvisioned": int(
                    persistent_volume_metrics.get("bytesProvisioned")
                )
                if persistent_volume_metrics.get("bytesProvisioned") is not None
                else None,
                "percentOfCapacity": _normalized_percent(
                    float(persistent_volume_metrics.get("bytesUsed"))
                    if persistent_volume_metrics.get("bytesUsed") is not None
                    else None,
                    float(persistent_volume_metrics.get("bytesCapacity"))
                    if persistent_volume_metrics.get("bytesCapacity") is not None
                    else None,
                ),
                "mountPath": str(
                    persistent_volume_metrics.get("mountPath") or ""
                ).strip(),
            },
            "status": {
                "nodeMetrics": node_status,
                "s3Metrics": s3_status,
                "persistentVolumeMetrics": persistent_volume_status,
            },
            "nodeName": str(self._settings.node_name or "").strip(),
            "podName": str(self._settings.pod_name or "").strip(),
            "podNamespace": str(self._settings.pod_namespace or "").strip(),
        }
        if app_metrics.cpu_usage_micros is not None:
            sample["internal"] = {
                "appCpuUsageMicros": app_metrics.cpu_usage_micros,
            }
        return sample

    def _read_app_metrics(self, *, initial: bool) -> AppMetricsReading:
        reading = self._read_raw_app_metrics()
        if reading.cpu_usage_micros is None:
            self._last_app_metrics_reading = reading
            return reading

        previous = self._last_app_metrics_reading
        if previous is None and initial and not self._stop_event.wait(0.5):
            first_reading = reading
            reading = self._read_raw_app_metrics()
            if reading.cpu_usage_micros is None:
                reading = first_reading
            else:
                previous = first_reading

        if previous is not None:
            delta_usage = reading.cpu_usage_micros - (previous.cpu_usage_micros or 0)
            elapsed_seconds = (
                reading.observed_at - previous.observed_at
            ).total_seconds()
            if delta_usage >= 0 and elapsed_seconds > 0:
                reading = AppMetricsReading(
                    cpu_usage_micros=reading.cpu_usage_micros,
                    cpu_limit_cores=reading.cpu_limit_cores,
                    memory_usage_bytes=reading.memory_usage_bytes,
                    memory_limit_bytes=reading.memory_limit_bytes,
                    observed_at=reading.observed_at,
                    delta_usage_micros=delta_usage,
                    elapsed_seconds=elapsed_seconds,
                )

        self._last_app_metrics_reading = reading
        return reading

    def _read_raw_app_metrics(self) -> AppMetricsReading:
        observed_at = datetime.now(UTC)
        cpu_usage_micros: int | None = None
        cpu_limit_cores: float | None = None

        cpu_stat_text = _read_text_file(Path("/sys/fs/cgroup/cpu.stat"))
        if cpu_stat_text is not None:
            try:
                cpu_usage_micros = parse_cgroup_cpu_usage_micros(cpu_stat_text)
            except ValueError:
                cpu_usage_micros = None
        if cpu_usage_micros is None:
            cpuacct_usage_ns = _read_int_file(Path("/sys/fs/cgroup/cpuacct.usage"))
            if cpuacct_usage_ns is not None:
                cpu_usage_micros = int(cpuacct_usage_ns / 1_000)

        cpu_max_text = _read_text_file(Path("/sys/fs/cgroup/cpu.max"))
        if cpu_max_text is not None:
            try:
                cpu_limit_cores = parse_cgroup_cpu_limit_cores(cpu_max_text)
            except ValueError:
                cpu_limit_cores = None
        if cpu_limit_cores is None:
            quota = _read_int_file(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"))
            period = _read_int_file(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us"))
            if quota is not None and period and quota > 0 and period > 0:
                cpu_limit_cores = quota / period

        memory_usage_bytes = _read_int_file(Path("/sys/fs/cgroup/memory.current"))
        if memory_usage_bytes is None:
            memory_usage_bytes = _read_int_file(
                Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
            )

        memory_limit_bytes: int | None = None
        memory_max_text = _read_text_file(Path("/sys/fs/cgroup/memory.max"))
        if memory_max_text is not None:
            try:
                memory_limit_bytes = parse_cgroup_memory_limit_bytes(memory_max_text)
            except ValueError:
                memory_limit_bytes = None
        if memory_limit_bytes is None:
            legacy_memory_limit = _read_int_file(
                Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
            )
            if legacy_memory_limit is not None and legacy_memory_limit < 2**60:
                memory_limit_bytes = legacy_memory_limit

        return AppMetricsReading(
            cpu_usage_micros=cpu_usage_micros,
            cpu_limit_cores=cpu_limit_cores,
            memory_usage_bytes=memory_usage_bytes,
            memory_limit_bytes=memory_limit_bytes,
            observed_at=observed_at,
        )

    def _app_cpu_cores_used(self, reading: AppMetricsReading) -> float | None:
        if reading.delta_usage_micros is None or reading.elapsed_seconds in (None, 0):
            return None
        return (reading.delta_usage_micros / 1_000_000.0) / float(
            reading.elapsed_seconds
        )

    def _collect_node_metrics(
        self,
    ) -> tuple[dict[str, float | int | None], dict[str, object]]:
        empty_metrics = {
            "cpuCoresUsed": None,
            "cpuCapacityCores": None,
            "memoryBytesUsed": None,
            "memoryCapacityBytes": None,
        }
        if not self._settings.service_consumption_node_metrics_enabled:
            return empty_metrics, {
                "available": False,
                "detail": "Kubernetes node metrics collection is disabled.",
            }
        node_name = str(self._settings.node_name or "").strip()
        if not node_name:
            return empty_metrics, {
                "available": False,
                "detail": "NODE_NAME is not available in this runtime.",
            }
        if not self._kubernetes_client.available():
            return empty_metrics, {
                "available": False,
                "detail": "Kubernetes service account credentials are unavailable.",
            }

        try:
            node_payload = self._kubernetes_client.get_json(f"/api/v1/nodes/{node_name}")
            metrics_payload = self._kubernetes_client.get_json(
                f"/apis/metrics.k8s.io/v1beta1/nodes/{node_name}"
            )
            node_capacity = (
                node_payload.get("status", {}).get("capacity", {})
                if isinstance(node_payload, dict)
                else {}
            )
            usage = (
                metrics_payload.get("usage", {})
                if isinstance(metrics_payload, dict)
                else {}
            )
            return {
                "cpuCoresUsed": parse_kubernetes_cpu_quantity(usage.get("cpu")),
                "cpuCapacityCores": parse_kubernetes_cpu_quantity(
                    node_capacity.get("cpu")
                ),
                "memoryBytesUsed": parse_kubernetes_memory_quantity(
                    usage.get("memory")
                ),
                "memoryCapacityBytes": parse_kubernetes_memory_quantity(
                    node_capacity.get("memory")
                ),
            }, {
                "available": True,
                "detail": "Node metrics are available from metrics.k8s.io.",
            }
        except Exception as exc:
            logger.warning("Failed to collect node metrics: %s", exc)
            return empty_metrics, {
                "available": False,
                "detail": str(exc),
            }

    def _current_s3_metrics(
        self,
        observed_at: datetime,
        *,
        force_refresh: bool,
    ) -> tuple[dict[str, int | None], dict[str, object], datetime]:
        with self._lock:
            cached = self._last_s3_reading

        interval_seconds = max(1, self._settings.service_consumption_s3_interval_seconds)
        if (
            not force_refresh
            and cached is not None
            and (observed_at - cached.sampled_at).total_seconds() < interval_seconds
        ):
            return cached.metrics, cached.status, cached.sampled_at

        metrics, status = self._collect_s3_metrics()
        sampled_at = observed_at.astimezone(UTC).replace(microsecond=0)
        cached = CachedS3Reading(
            metrics=metrics,
            status=status,
            sampled_at=sampled_at,
        )
        with self._lock:
            self._last_s3_reading = cached
        return cached.metrics, cached.status, cached.sampled_at

    def _collect_s3_metrics(self) -> tuple[dict[str, int | None], dict[str, object]]:
        if not any(
            (
                self._settings.s3_endpoint,
                self._settings.current_s3_access_key_id(),
                self._settings.current_s3_secret_access_key(),
            )
        ):
            return {
                "totalBytes": None,
                "bucketCount": 0,
            }, {
                "available": False,
                "detail": "S3 is not configured for this runtime.",
            }

        try:
            client = s3_client(self._settings)
            bucket_names = list_s3_buckets_from_client(client)
            total_bytes = 0
            for bucket_name in bucket_names:
                continuation_token: str | None = None
                while True:
                    kwargs: dict[str, object] = {
                        "Bucket": bucket_name,
                        "MaxKeys": 1000,
                    }
                    if continuation_token:
                        kwargs["ContinuationToken"] = continuation_token
                    response = client.list_objects_v2(**kwargs)
                    total_bytes += sum(
                        int(item.get("Size") or 0)
                        for item in (response.get("Contents") or [])
                    )
                    if not response.get("IsTruncated"):
                        break
                    continuation_token = (
                        str(response.get("NextContinuationToken") or "").strip()
                        or None
                    )
            return {
                "totalBytes": total_bytes,
                "bucketCount": len(bucket_names),
            }, {
                "available": True,
                "detail": "S3 usage is aggregated across all visible buckets.",
            }
        except (ClientError, BotoCoreError, ValueError) as exc:
            logger.warning("Failed to collect S3 metrics: %s", exc)
            return {
                "totalBytes": None,
                "bucketCount": 0,
            }, {
                "available": False,
                "detail": str(exc),
            }

    def _collect_persistent_volume_metrics(
        self,
    ) -> tuple[dict[str, int | str | None], dict[str, object]]:
        return {
            "bytesUsed": None,
            "bytesCapacity": None,
            "bytesProvisioned": None,
            "mountPath": "",
        }, {
            "available": False,
            "detail": "Persistent volume metrics are disabled; monitoring state is kept in memory and snapshotted to S3.",
        }

    def _build_realtime_payload_locked(self) -> dict[str, Any]:
        return {
            "version": self._version,
            "latest": self._public_sample(self._latest_sample),
            "status": self._status_payload_locked(),
            "topology": self._topology_payload_locked(),
            "financialSummary": self._financial_summary_locked(),
        }

    def _status_payload_locked(self) -> dict[str, object]:
        latest_status = {}
        latest_s3 = {}
        latest_persistent_volume = {}
        if isinstance(self._latest_sample, dict):
            latest_status = self._latest_sample.get("status") or {}
            latest_s3 = self._latest_sample.get("s3") or {}
            latest_persistent_volume = (
                self._latest_sample.get("persistentVolume") or {}
            )
        node_status = latest_status.get("nodeMetrics") or {
            "available": False,
            "detail": "Node metrics have not been sampled yet.",
        }
        s3_status = latest_status.get("s3Metrics") or {
            "available": False,
            "detail": "S3 metrics have not been sampled yet.",
        }
        persistent_volume_status = latest_status.get("persistentVolumeMetrics") or {
            "available": False,
            "detail": "Persistent volume usage has not been sampled yet.",
        }
        return {
            "nodeMetrics": node_status,
            "s3Metrics": s3_status,
            "persistentVolumeMetrics": persistent_volume_status,
            "nodeMetricsAvailable": bool(node_status.get("available")),
            "s3MetricsAvailable": bool(s3_status.get("available")),
            "persistentVolumeMetricsAvailable": bool(
                persistent_volume_status.get("available")
            ),
            "s3SampledAtUtc": str(latest_s3.get("sampledAtUtc") or "").strip() or None,
            "persistentVolumeMountPath": str(
                latest_persistent_volume.get("mountPath") or ""
            ).strip()
            or None,
        }

    def _topology_payload_locked(self) -> dict[str, object]:
        latest_sample = self._latest_sample or {}
        return {
            "copy": TOPOLOGY_COPY,
            "scaleOutCopy": SCALE_OUT_COPY,
            "nodeName": str(
                latest_sample.get("nodeName") or self._settings.node_name or ""
            ).strip(),
            "podName": str(
                latest_sample.get("podName") or self._settings.pod_name or ""
            ).strip(),
            "podNamespace": str(
                latest_sample.get("podNamespace")
                or self._settings.pod_namespace
                or ""
            ).strip(),
        }

    def _financial_summary_locked(self) -> dict[str, object]:
        financial = self._financial_payload_locked()
        return {
            "currency": financial.get("currency"),
            "yearUtc": financial.get("yearUtc"),
            "annualBudgetChf": financial.get("annualBudgetChf"),
            "spentYearToDateChf": financial.get("spentYearToDateChf"),
            "remainingBudgetChf": financial.get("remainingBudgetChf"),
            "forecastYearEndChf": financial.get("forecastYearEndChf"),
            "status": financial.get("status"),
            "budgetUpdatedAtUtc": financial.get("budgetUpdatedAtUtc"),
        }

    def _days_in_year(self, year: int) -> int:
        return (
            datetime(year + 1, 1, 1, tzinfo=UTC).date()
            - datetime(year, 1, 1, tzinfo=UTC).date()
        ).days

    def _application_monthly_fee_chf(self) -> float:
        return APPLICATION_SERVICE_ANNUAL_CHF / 12.0

    def _pg_annual_fee_chf(self, year: int) -> float:
        return len(PG_STATIC_INSTANCES) * PG_SERVICE_DAILY_CHF_PER_INSTANCE * self._days_in_year(year)

    def _pg_monthly_fee_chf(self, year: int) -> float:
        return self._pg_annual_fee_chf(year) / 12.0

    def _pg_daily_fee_per_instance_chf(self, year: int) -> float:
        _ = year
        return PG_SERVICE_DAILY_CHF_PER_INSTANCE

    def _annual_fee_ytd_chf(
        self,
        *,
        year: int,
        annual_fee_chf: float,
        as_of_date: date | None = None,
    ) -> float:
        effective_date = as_of_date or datetime.now(UTC).date()
        year_start = datetime(year, 1, 1, tzinfo=UTC).date()
        year_end = datetime(year + 1, 1, 1, tzinfo=UTC).date() - timedelta(days=1)
        if effective_date < year_start:
            return 0.0
        capped_date = min(effective_date, year_end)
        elapsed_days = (capped_date - year_start).days + 1
        return annual_fee_chf * (elapsed_days / self._days_in_year(year))

    def _application_ytd_chf(self, year: int, *, as_of_date: date | None = None) -> float:
        return self._annual_fee_ytd_chf(
            year=year,
            annual_fee_chf=APPLICATION_SERVICE_ANNUAL_CHF,
            as_of_date=as_of_date,
        )

    def _pg_ytd_chf(self, year: int, *, as_of_date: date | None = None) -> float:
        return self._annual_fee_ytd_chf(
            year=year,
            annual_fee_chf=self._pg_annual_fee_chf(year),
            as_of_date=as_of_date,
        )

    def _assumed_dynamic_daily_components_locked(
        self,
        latest_sample: dict[str, Any],
    ) -> dict[str, float]:
        container_costs = (
            self._interval_container_cost_components_chf(latest_sample, 24.0) or {}
        )
        return {
            "container": float(container_costs.get("totalChf") or 0.0),
            "containerCpu": float(container_costs.get("cpuChf") or 0.0),
            "containerRam": float(container_costs.get("ramChf") or 0.0),
            "s3": float(self._interval_s3_cost_chf(latest_sample, 24.0) or 0.0),
            "filesystem": float(
                self._interval_persistent_volume_cost_chf(latest_sample, 24.0) or 0.0
            ),
        }

    def _assumed_dynamic_backfill_locked(
        self,
        cache: FinancialYearCache,
        latest_sample: dict[str, Any],
    ) -> dict[str, Any]:
        effective_daily_totals = dict(cache.daily_totals)
        added_breakdown = {
            "container": 0.0,
            "containerCpu": 0.0,
            "containerRam": 0.0,
            "s3": 0.0,
            "filesystem": 0.0,
        }
        if cache.year < POC_CLIENT_START_YEAR or not latest_sample:
            return {
                "effectiveDailyTotals": effective_daily_totals,
                "addedBreakdown": added_breakdown,
            }

        today = datetime.now(UTC).date()
        start_date = datetime(cache.year, 1, 1, tzinfo=UTC).date()
        end_date = min(self._month_end_date(cache.year, 3), today)
        if end_date < start_date:
            return {
                "effectiveDailyTotals": effective_daily_totals,
                "addedBreakdown": added_breakdown,
            }

        daily_components = self._assumed_dynamic_daily_components_locked(latest_sample)
        daily_total = (
            daily_components["container"]
            + daily_components["s3"]
            + daily_components["filesystem"]
        )
        cursor = start_date
        while cursor <= end_date:
            day_key = cursor.isoformat()
            if day_key not in effective_daily_totals:
                effective_daily_totals[day_key] = daily_total
                for component_key, value in daily_components.items():
                    added_breakdown[component_key] += value
            cursor += timedelta(days=1)

        return {
            "effectiveDailyTotals": effective_daily_totals,
            "addedBreakdown": added_breakdown,
        }

    def _fixed_daily_burn_chf(self, year: int) -> float:
        return (APPLICATION_SERVICE_ANNUAL_CHF + self._pg_annual_fee_chf(year)) / self._days_in_year(year)

    def _financial_payload_locked(self) -> dict[str, object]:
        year = datetime.now(UTC).year
        cache = self._ensure_financial_cache_locked(year)
        latest_sample = self._latest_sample if isinstance(self._latest_sample, dict) else {}
        assumed_dynamic = self._assumed_dynamic_backfill_locked(cache, latest_sample)
        effective_dynamic_daily_totals = assumed_dynamic["effectiveDailyTotals"]
        dynamic_breakdown = {
            "container": cache.breakdown_year_to_date.get("container", 0.0)
            + assumed_dynamic["addedBreakdown"]["container"],
            "containerCpu": cache.breakdown_year_to_date.get("containerCpu", 0.0)
            + assumed_dynamic["addedBreakdown"]["containerCpu"],
            "containerRam": cache.breakdown_year_to_date.get("containerRam", 0.0)
            + assumed_dynamic["addedBreakdown"]["containerRam"],
            "s3": cache.breakdown_year_to_date.get("s3", 0.0)
            + assumed_dynamic["addedBreakdown"]["s3"],
            "filesystem": cache.breakdown_year_to_date.get("filesystem", 0.0)
            + assumed_dynamic["addedBreakdown"]["filesystem"],
        }
        status = self._financial_status_locked(cache, latest_sample)
        has_live_or_historic_cost = bool(effective_dynamic_daily_totals) or bool(status.get("available"))
        application_ytd_chf = _normalized_currency(self._application_ytd_chf(year))
        pg_ytd_chf = _normalized_currency(self._pg_ytd_chf(year))
        dynamic_year_to_date = _normalized_currency(sum(effective_dynamic_daily_totals.values()))

        spent_year_to_date = (
            _normalized_currency(
                float(dynamic_year_to_date or 0.0)
                + float(application_ytd_chf or 0.0)
                + float(pg_ytd_chf or 0.0)
            )
            if has_live_or_historic_cost
            else None
        )
        if has_live_or_historic_cost and spent_year_to_date is None:
            spent_year_to_date = 0.0

        annual_budget_chf = _normalized_currency(cache.annual_budget_chf)
        remaining_budget_chf = None
        if annual_budget_chf is not None and spent_year_to_date is not None:
            remaining_budget_chf = _normalized_currency(
                annual_budget_chf - spent_year_to_date
            )

        forecast_daily_chf = self._forecast_daily_spend_locked(
            cache,
            enabled=has_live_or_historic_cost,
        )
        forecast_year_end_chf = None
        if spent_year_to_date is not None and forecast_daily_chf is not None:
            today = datetime.now(UTC).date()
            year_end = datetime(year + 1, 1, 1, tzinfo=UTC).date() - timedelta(days=1)
            remaining_days = max((year_end - today).days, 0)
            forecast_year_end_chf = _normalized_currency(
                spent_year_to_date + (forecast_daily_chf * remaining_days)
            )

        breakdown_year_to_date = {
            "computeChf": (
                _normalized_currency(dynamic_breakdown.get("container", 0.0))
                if has_live_or_historic_cost
                else None
            ),
            "applicationChf": (
                application_ytd_chf
                if has_live_or_historic_cost
                else None
            ),
            "s3Chf": (
                _normalized_currency(dynamic_breakdown.get("s3", 0.0))
                if has_live_or_historic_cost
                else None
            ),
            "persistentVolumeChf": (
                _normalized_currency(dynamic_breakdown.get("filesystem", 0.0))
                if has_live_or_historic_cost
                else None
            ),
            "pgChf": (
                pg_ytd_chf
                if has_live_or_historic_cost
                else None
            ),
        }

        monthly_payload = self._financial_monthly_payload_locked(
            cache=cache,
            latest_sample=latest_sample,
            annual_budget_chf=annual_budget_chf,
            spent_year_to_date_chf=spent_year_to_date,
            forecast_daily_chf=forecast_daily_chf,
            effective_daily_totals=effective_dynamic_daily_totals,
            enabled=has_live_or_historic_cost,
        )

        return {
            "currency": COST_CURRENCY,
            "yearUtc": year,
            "annualBudgetChf": annual_budget_chf,
            "budgetUpdatedAtUtc": cache.budget_updated_at.isoformat()
            if cache.budget_updated_at is not None
            else None,
            "spentYearToDateChf": spent_year_to_date,
            "remainingBudgetChf": remaining_budget_chf,
            "forecastYearEndChf": forecast_year_end_chf,
            "breakdownYearToDate": breakdown_year_to_date,
            "servicesTotalChf": spent_year_to_date,
            "services": self._financial_services_payload_locked(
                cache=cache,
                latest_sample=latest_sample,
                spent_year_to_date_chf=spent_year_to_date,
                dynamic_breakdown=dynamic_breakdown,
            ),
            "monthly": monthly_payload,
            "status": status,
        }

    def _financial_status_locked(
        self,
        cache: FinancialYearCache,
        latest_sample: dict[str, Any],
    ) -> dict[str, object]:
        component_labels = {
            "container": "container service",
            "application": "DAAIF Fabric application service",
            "s3": "S3",
            "filesystem": "file system service",
            "pg": "PG service",
        }
        configured_components = {
            "container": self._settings.service_consumption_cost_node_chf_per_hour
            is not None,
            "application": True,
            "s3": self._settings.service_consumption_cost_s3_chf_per_gb_month is not None,
            "filesystem": self._settings.service_consumption_cost_pv_chf_per_gb_month
            is not None,
            "pg": True,
        }
        component_availability = {
            "container": configured_components["container"]
            and self._compute_cost_component_available(latest_sample),
            "application": configured_components["application"]
            and self._application_cost_component_available(),
            "s3": configured_components["s3"]
            and self._s3_cost_component_available(latest_sample),
            "filesystem": configured_components["filesystem"]
            and self._persistent_volume_cost_component_available(latest_sample),
            "pg": configured_components["pg"] and self._pg_cost_component_available(),
        }
        configured_labels = [
            label
            for label in SERVICE_COST_CATEGORY_ORDER
            if configured_components.get(label)
        ]
        available_labels = [
            label
            for label in SERVICE_COST_CATEGORY_ORDER
            if component_availability.get(label)
        ]
        missing_labels = [
            component_labels[label]
            for label in configured_labels
            if label not in available_labels
        ]

        if not configured_labels:
            detail = (
                "Estimated CHF is unavailable. Configure cost-rate inputs in the app ConfigMap."
            )
        elif available_labels and not missing_labels:
            configured_names = [component_labels[label] for label in configured_labels]
            detail = (
                "Estimated CHF is available from the configured "
                f"{', '.join(configured_names)} rates."
            )
        elif available_labels:
            detail = (
                "Estimated CHF is partially available. Waiting for current metrics from "
                f"{', '.join(missing_labels)}."
            )
        else:
            detail = (
                "Estimated CHF is not yet available from the configured cost inputs."
            )

        return {
            "available": bool(available_labels or cache.daily_totals),
            "detail": detail,
            "budgetConfigured": cache.annual_budget_chf is not None,
            "budgetUpdatedAtUtc": cache.budget_updated_at.isoformat()
            if cache.budget_updated_at is not None
            else None,
            "computeAvailable": component_availability["container"],
            "applicationAvailable": component_availability["application"],
            "s3Available": component_availability["s3"],
            "persistentVolumeAvailable": component_availability["filesystem"],
            "pgAvailable": component_availability["pg"],
        }

    def _compute_cost_component_available(self, sample: dict[str, Any]) -> bool:
        cpu_app = ((sample.get("cpu") or {}).get("app") or {}).get("coresUsed")
        cpu_capacity = ((sample.get("cpu") or {}).get("node") or {}).get(
            "capacityCores"
        )
        memory_app = ((sample.get("memory") or {}).get("app") or {}).get("bytesUsed")
        memory_capacity = ((sample.get("memory") or {}).get("node") or {}).get(
            "capacityBytes"
        )
        return all(
            value is not None
            for value in (cpu_app, cpu_capacity, memory_app, memory_capacity)
        )

    def _application_cost_component_available(self) -> bool:
        return self._application_monthly_fee_chf() >= 0

    def _s3_cost_component_available(self, sample: dict[str, Any]) -> bool:
        return ((sample.get("s3") or {}).get("totalBytes")) is not None

    def _persistent_volume_cost_component_available(
        self,
        sample: dict[str, Any],
    ) -> bool:
        return ((sample.get("persistentVolume") or {}).get("bytesProvisioned")) is not None

    def _pg_cost_component_available(self) -> bool:
        current_year = datetime.now(UTC).year
        return self._pg_monthly_fee_chf(current_year) >= 0

    def _financial_services_payload_locked(
        self,
        *,
        cache: FinancialYearCache,
        latest_sample: dict[str, Any],
        spent_year_to_date_chf: float | None,
        dynamic_breakdown: dict[str, float],
    ) -> list[dict[str, object]]:
        total_spend = (
            float(spent_year_to_date_chf)
            if spent_year_to_date_chf is not None and spent_year_to_date_chf > 0
            else 0.0
        )
        breakdown = dynamic_breakdown
        container_total = _normalized_currency(dynamic_breakdown.get("container", 0.0))
        application_total = _normalized_currency(self._application_ytd_chf(cache.year))
        filesystem_total = _normalized_currency(dynamic_breakdown.get("filesystem", 0.0))
        s3_total = _normalized_currency(dynamic_breakdown.get("s3", 0.0))
        pg_total = _normalized_currency(self._pg_ytd_chf(cache.year))
        if self._settings.service_consumption_pvc_capacity_enabled:
            filesystem_status = self._service_status(
                configured=self._settings.service_consumption_cost_pv_chf_per_gb_month
                is not None,
                available=self._persistent_volume_cost_component_available(
                    latest_sample
                ),
                has_cost=bool(filesystem_total),
            )
            filesystem_subtitle = "Provisioned PVC capacity allocated to the app"
        else:
            filesystem_status = {"state": "unavailable", "label": "Unavailable"}
            filesystem_subtitle = "PVC-backed filesystem costing is disabled"
        services = [
            {
                "key": "container",
                "label": "Container Service",
                "subtitle": "CPU and RAM share on the active node",
                "costYtdChf": container_total,
                "shareOfTotalPercent": self._service_share_percent(
                    container_total,
                    total_spend,
                ),
                "status": self._service_status(
                    configured=self._settings.service_consumption_cost_node_chf_per_hour
                    is not None,
                    available=self._compute_cost_component_available(latest_sample),
                    has_cost=bool(container_total),
                ),
                "details": {
                    "cpuChf": _normalized_currency(
                        breakdown.get("containerCpu", 0.0)
                    ),
                    "ramChf": _normalized_currency(
                        breakdown.get("containerRam", 0.0)
                    ),
                    "nodeChfPerHour": _normalized_currency(
                        self._settings.service_consumption_cost_node_chf_per_hour
                    ),
                    "cpuWeight": _normalized_float(
                        self._settings.service_consumption_cost_cpu_weight,
                        digits=2,
                    ),
                    "ramWeight": _normalized_float(
                        self._settings.service_consumption_cost_ram_weight,
                        digits=2,
                    ),
                },
            },
            {
                "key": "application",
                "label": "DAAIF Fabric Application Service",
                "subtitle": "Shared application fee prorated across the year",
                "costYtdChf": application_total,
                "shareOfTotalPercent": self._service_share_percent(
                    application_total,
                    total_spend,
                ),
                "status": self._service_status(
                    configured=True,
                    available=self._application_cost_component_available(),
                    has_cost=bool(application_total),
                ),
                "details": {
                    "annualFeeChf": _normalized_currency(APPLICATION_SERVICE_ANNUAL_CHF),
                    "monthlyFeeChf": _normalized_currency(
                        self._application_monthly_fee_chf()
                    ),
                },
            },
            {
                "key": "filesystem",
                "label": "FileSystem Service",
                "subtitle": filesystem_subtitle,
                "costYtdChf": filesystem_total,
                "shareOfTotalPercent": self._service_share_percent(
                    filesystem_total,
                    total_spend,
                ),
                "status": filesystem_status,
                "details": {
                    "provisionedBytes": (
                        (latest_sample.get("persistentVolume") or {}).get(
                            "bytesProvisioned"
                        )
                    ),
                    "rateChfPerGbMonth": _normalized_currency(
                        self._settings.service_consumption_cost_pv_chf_per_gb_month
                    ),
                },
            },
            {
                "key": "s3",
                "label": "S3 Service",
                "subtitle": "Visible shared workspace object storage",
                "costYtdChf": s3_total,
                "shareOfTotalPercent": self._service_share_percent(
                    s3_total,
                    total_spend,
                ),
                "status": self._service_status(
                    configured=self._settings.service_consumption_cost_s3_chf_per_gb_month
                    is not None,
                    available=self._s3_cost_component_available(latest_sample),
                    has_cost=bool(s3_total),
                ),
                "details": {
                    "totalBytes": ((latest_sample.get("s3") or {}).get("totalBytes")),
                    "bucketCount": ((latest_sample.get("s3") or {}).get("bucketCount")),
                    "rateChfPerGbMonth": _normalized_currency(
                        self._settings.service_consumption_cost_s3_chf_per_gb_month
                    ),
                },
            },
            {
                "key": "pg",
                "label": "PG Service",
                "subtitle": "Static OLTP and OLAP database capacity placeholders",
                "costYtdChf": pg_total,
                "shareOfTotalPercent": self._service_share_percent(pg_total, total_spend),
                "status": self._service_status(
                    configured=True,
                    available=self._pg_cost_component_available(),
                    has_cost=bool(pg_total),
                ),
                "details": {
                    "instances": [
                        {
                            "label": label,
                            "sizeBytes": size_bytes,
                            "sizeGb": _normalized_float(
                                _decimal_gb_from_bytes(size_bytes),
                                digits=1,
                            ),
                        }
                        for label, size_bytes in PG_STATIC_INSTANCES
                    ],
                    "annualFeePerInstanceChf": _normalized_currency(
                        PG_SERVICE_DAILY_CHF_PER_INSTANCE
                        * self._days_in_year(cache.year)
                    ),
                    "dailyFeePerInstanceChf": _normalized_currency(
                        self._pg_daily_fee_per_instance_chf(cache.year)
                    ),
                    "monthlyFeeChf": _normalized_currency(
                        self._pg_monthly_fee_chf(cache.year)
                    ),
                    "totalBytes": self._pg_total_bytes(),
                    "totalGb": _normalized_float(
                        _decimal_gb_from_bytes(self._pg_total_bytes()),
                        digits=1,
                    ),
                },
            },
        ]
        for service in services:
            cost_ytd = service.get("costYtdChf")
            details = service.get("details")
            if isinstance(details, dict):
                details["costYtdChf"] = cost_ytd
        return services

    def _service_share_percent(
        self,
        cost_ytd_chf: float | None,
        total_spend_chf: float,
    ) -> float:
        if cost_ytd_chf is None or total_spend_chf <= 0:
            return 0.0
        return _normalized_percent(cost_ytd_chf, total_spend_chf) or 0.0

    def _service_status(
        self,
        *,
        configured: bool,
        available: bool,
        has_cost: bool,
    ) -> dict[str, str]:
        if not configured:
            return {"state": "not-configured", "label": "Not configured"}
        if available or has_cost:
            return {"state": "available", "label": "Available"}
        return {"state": "waiting", "label": "Waiting for metrics"}

    def _financial_monthly_payload_locked(
        self,
        *,
        cache: FinancialYearCache,
        latest_sample: dict[str, Any],
        annual_budget_chf: float | None,
        spent_year_to_date_chf: float | None,
        forecast_daily_chf: float | None,
        effective_daily_totals: dict[str, float],
        enabled: bool,
    ) -> dict[str, list[object]]:
        year = cache.year
        today = datetime.now(UTC).date()
        current_month = today.month
        labels = [month_abbr[index] for index in range(1, 13)]

        cumulative_actual_by_month: dict[int, float] = {}
        running_total = 0.0
        for month in range(1, 13):
            for day_text in sorted(effective_daily_totals):
                day = datetime.fromisoformat(day_text).date()
                if day.month != month or day.year != year:
                    continue
                running_total += effective_daily_totals.get(day_text, 0.0)
            cumulative_actual_by_month[month] = running_total

        total_days_in_year = (
            datetime(year + 1, 1, 1, tzinfo=UTC).date()
            - datetime(year, 1, 1, tzinfo=UTC).date()
        ).days
        actual_cumulative: list[object] = []
        plan_cumulative: list[object] = []
        forecast_cumulative: list[object] = []

        for month in range(1, 13):
            month_end = self._month_end_date(year, month)
            if enabled:
                if month < current_month:
                    actual_cumulative.append(
                        _normalized_currency(
                            cumulative_actual_by_month.get(month, 0.0)
                            + self._application_ytd_chf(year, as_of_date=month_end)
                            + self._pg_ytd_chf(year, as_of_date=month_end)
                        )
                    )
                elif month == current_month:
                    actual_cumulative.append(spent_year_to_date_chf)
                else:
                    actual_cumulative.append(None)
            else:
                actual_cumulative.append(None)

            if annual_budget_chf is None:
                plan_cumulative.append(None)
            else:
                elapsed_days = (
                    month_end - datetime(year, 1, 1, tzinfo=UTC).date()
                ).days + 1
                plan_cumulative.append(
                    _normalized_currency(
                        annual_budget_chf * (elapsed_days / total_days_in_year)
                    )
                )

            if not enabled or spent_year_to_date_chf is None:
                forecast_cumulative.append(None)
            elif month < current_month:
                forecast_cumulative.append(
                    _normalized_currency(
                        cumulative_actual_by_month.get(month, 0.0)
                        + self._application_ytd_chf(year, as_of_date=month_end)
                        + self._pg_ytd_chf(year, as_of_date=month_end)
                    )
                )
            elif forecast_daily_chf is None:
                forecast_cumulative.append(None)
            elif month == current_month:
                forecast_cumulative.append(spent_year_to_date_chf)
            else:
                remaining_days = max((month_end - today).days, 0)
                forecast_cumulative.append(
                    _normalized_currency(
                        spent_year_to_date_chf + (forecast_daily_chf * remaining_days)
                    )
                )

        return {
            "labels": labels,
            "currentYear": year,
            "comparisonYear": year - 1 if (year - 1) >= POC_CLIENT_START_YEAR else None,
            "comparisonActualCumulativeChf": self._mock_previous_year_actual_cumulative_locked(
                comparison_year=year - 1,
                latest_sample=latest_sample,
            )
            if (year - 1) >= POC_CLIENT_START_YEAR
            else [None] * 12,
            "actualCumulativeChf": actual_cumulative,
            "planCumulativeChf": plan_cumulative,
            "forecastCumulativeChf": forecast_cumulative,
        }

    def _mock_previous_year_actual_cumulative_locked(
        self,
        *,
        comparison_year: int,
        latest_sample: dict[str, Any],
    ) -> list[object]:
        if comparison_year < POC_CLIENT_START_YEAR:
            return [None] * 12
        days_in_year = self._days_in_year(comparison_year)
        daily_total = 0.0
        compute_daily = self._interval_compute_cost_chf(latest_sample, 24.0) or 0.0
        application_daily = self._interval_application_cost_chf(24.0) or 0.0
        s3_daily = self._interval_s3_cost_chf(latest_sample, 24.0) or 0.0
        filesystem_daily = (
            self._interval_persistent_volume_cost_chf(latest_sample, 24.0) or 0.0
        )
        pg_daily = self._interval_pg_cost_chf(24.0) or 0.0
        daily_total = compute_daily + application_daily + s3_daily + filesystem_daily + pg_daily
        cumulative: list[object] = []
        year_start = datetime(comparison_year, 1, 1, tzinfo=UTC).date()
        for month in range(1, 13):
            month_end = self._month_end_date(comparison_year, month)
            elapsed_days = (month_end - year_start).days + 1
            ratio = elapsed_days / days_in_year
            cumulative.append(
                _normalized_currency(
                    daily_total * ratio * days_in_year
                )
            )
        return cumulative

    def _forecast_daily_spend_locked(
        self,
        cache: FinancialYearCache,
        *,
        enabled: bool,
    ) -> float | None:
        if not enabled:
            return None
        today = datetime.now(UTC).date()
        start_day = max(
            datetime(cache.year, 1, 1, tzinfo=UTC).date(),
            today - timedelta(days=FORECAST_WINDOW_DAYS - 1),
        )
        window_days = max((today - start_day).days + 1, 1)
        total = 0.0
        day = start_day
        while day <= today:
            total += cache.daily_totals.get(day.isoformat(), 0.0)
            total += self._fixed_daily_burn_chf(cache.year)
            day += timedelta(days=1)
        return total / window_days

    def _ensure_financial_cache_locked(self, year: int) -> FinancialYearCache:
        if self._financial_cache is not None and self._financial_cache.year == year:
            return self._financial_cache
        self._financial_cache = self._rebuild_financial_cache_locked(year)
        return self._financial_cache

    def _rebuild_financial_cache_locked(self, year: int) -> FinancialYearCache:
        budget_entry = self._read_budget_locked(year)
        cache = FinancialYearCache(
            year=year,
            annual_budget_chf=budget_entry.get("annualBudgetChf")
            if isinstance(budget_entry, dict)
            else None,
            budget_updated_at=_parse_iso_datetime(
                budget_entry.get("updatedAtUtc") if isinstance(budget_entry, dict) else None
            ),
            breakdown_year_to_date={
                "container": 0.0,
                "containerCpu": 0.0,
                "containerRam": 0.0,
                "application": 0.0,
                "s3": 0.0,
                "filesystem": 0.0,
                "pg": 0.0,
            },
            daily_totals={},
        )
        for cost_event in self._load_cost_events_for_year_locked(year):
            self._register_cost_event_in_cache(cache, cost_event)
        return cache

    def _update_financial_cache_with_cost_event_locked(
        self,
        cost_event: dict[str, Any],
    ) -> None:
        observed_at = _parse_iso_datetime(cost_event.get("endAtUtc"))
        if observed_at is None:
            return
        cache = self._ensure_financial_cache_locked(observed_at.year)
        self._register_cost_event_in_cache(cache, cost_event)

    def _register_cost_event_in_cache(
        self,
        cache: FinancialYearCache,
        cost_event: dict[str, Any],
    ) -> None:
        observed_at = _parse_iso_datetime(cost_event.get("endAtUtc"))
        if observed_at is None or observed_at.year != cache.year:
            return
        component_costs = cost_event.get("components") or {}
        container_cpu_chf = float(component_costs.get("containerCpuChf") or 0.0)
        container_ram_chf = float(component_costs.get("containerRamChf") or 0.0)
        container_total_chf = float(
            component_costs.get("containerTotalChf")
            or component_costs.get("computeChf")
            or 0.0
        )
        if container_total_chf and not (container_cpu_chf or container_ram_chf):
            total_weight = (
                self._settings.service_consumption_cost_cpu_weight
                + self._settings.service_consumption_cost_ram_weight
            )
            if total_weight > 0:
                container_cpu_chf = container_total_chf * (
                    self._settings.service_consumption_cost_cpu_weight / total_weight
                )
                container_ram_chf = container_total_chf * (
                    self._settings.service_consumption_cost_ram_weight / total_weight
                )
        s3_chf = float(component_costs.get("s3Chf") or 0.0)
        filesystem_chf = float(
            component_costs.get("filesystemChf")
            or component_costs.get("persistentVolumeChf")
            or 0.0
        )
        cache.breakdown_year_to_date["container"] += container_total_chf
        cache.breakdown_year_to_date["containerCpu"] += container_cpu_chf
        cache.breakdown_year_to_date["containerRam"] += container_ram_chf
        cache.breakdown_year_to_date["s3"] += s3_chf
        cache.breakdown_year_to_date["filesystem"] += filesystem_chf
        day_key = observed_at.date().isoformat()
        cache.daily_totals[day_key] = cache.daily_totals.get(day_key, 0.0) + (
            container_total_chf + s3_chf + filesystem_chf
        )

    def _build_cost_event(
        self,
        previous_sample: dict[str, Any] | None,
        current_sample: dict[str, Any],
    ) -> dict[str, Any] | None:
        previous_observed_at = _parse_iso_datetime(
            (previous_sample or {}).get("timestampUtc")
        )
        current_observed_at = _parse_iso_datetime(current_sample.get("timestampUtc"))
        if previous_observed_at is None or current_observed_at is None:
            return None
        if current_observed_at <= previous_observed_at:
            return None

        interval_hours = (
            current_observed_at - previous_observed_at
        ).total_seconds() / 3600.0
        if interval_hours <= 0:
            return None

        container_costs = self._interval_container_cost_components_chf(
            current_sample,
            interval_hours,
        )
        compute_chf = (
            container_costs.get("totalChf") if isinstance(container_costs, dict) else None
        )
        application_chf = self._interval_application_cost_chf(interval_hours)
        s3_chf = self._interval_s3_cost_chf(current_sample, interval_hours)
        persistent_volume_chf = self._interval_persistent_volume_cost_chf(
            current_sample,
            interval_hours,
        )
        pg_chf = self._interval_pg_cost_chf(interval_hours)
        if all(
            value is None
            for value in (compute_chf, application_chf, s3_chf, persistent_volume_chf, pg_chf)
        ):
            return None

        total_chf = sum(
            value
            for value in (compute_chf, application_chf, s3_chf, persistent_volume_chf, pg_chf)
            if value is not None
        )
        return {
            "startAtUtc": previous_observed_at.replace(microsecond=0).isoformat(),
            "endAtUtc": current_observed_at.replace(microsecond=0).isoformat(),
            "intervalHours": _normalized_cost_value(interval_hours),
            "components": {
                "computeChf": _normalized_cost_value(compute_chf),
                "containerCpuChf": _normalized_cost_value(
                    (container_costs or {}).get("cpuChf")
                ),
                "containerRamChf": _normalized_cost_value(
                    (container_costs or {}).get("ramChf")
                ),
                "containerTotalChf": _normalized_cost_value(
                    (container_costs or {}).get("totalChf")
                ),
                "applicationChf": _normalized_cost_value(application_chf),
                "s3Chf": _normalized_cost_value(s3_chf),
                "persistentVolumeChf": _normalized_cost_value(
                    persistent_volume_chf
                ),
                "filesystemChf": _normalized_cost_value(persistent_volume_chf),
                "pgChf": _normalized_cost_value(pg_chf),
            },
            "totalChf": _normalized_cost_value(total_chf),
        }

    def _interval_container_cost_components_chf(
        self,
        sample: dict[str, Any],
        interval_hours: float,
    ) -> dict[str, float] | None:
        node_hourly_rate = self._settings.service_consumption_cost_node_chf_per_hour
        if node_hourly_rate is None or node_hourly_rate < 0:
            return None
        cpu_used = ((sample.get("cpu") or {}).get("app") or {}).get("coresUsed")
        cpu_capacity = ((sample.get("cpu") or {}).get("node") or {}).get(
            "capacityCores"
        )
        memory_used = ((sample.get("memory") or {}).get("app") or {}).get("bytesUsed")
        memory_capacity = ((sample.get("memory") or {}).get("node") or {}).get(
            "capacityBytes"
        )
        if any(
            value in (None, 0)
            for value in (cpu_used, cpu_capacity, memory_used, memory_capacity)
        ):
            return None

        cpu_share = max(0.0, min(float(cpu_used) / float(cpu_capacity), 1.0))
        ram_share = max(0.0, min(float(memory_used) / float(memory_capacity), 1.0))
        cpu_weight = self._settings.service_consumption_cost_cpu_weight
        ram_weight = self._settings.service_consumption_cost_ram_weight
        total_weight = cpu_weight + ram_weight
        if total_weight <= 0:
            return None

        base_cost = float(node_hourly_rate) * interval_hours
        cpu_cost = (cpu_weight / total_weight) * cpu_share * base_cost
        ram_cost = (ram_weight / total_weight) * ram_share * base_cost
        return {
            "cpuChf": cpu_cost,
            "ramChf": ram_cost,
            "totalChf": cpu_cost + ram_cost,
        }

    def _interval_compute_cost_chf(
        self,
        sample: dict[str, Any],
        interval_hours: float,
    ) -> float | None:
        cost_components = self._interval_container_cost_components_chf(
            sample,
            interval_hours,
        )
        if cost_components is None:
            return None
        return cost_components.get("totalChf")

    def _interval_application_cost_chf(
        self,
        interval_hours: float,
    ) -> float | None:
        app_fee = self._application_monthly_fee_chf()
        return float(app_fee) * interval_hours / HOURS_PER_MONTH_FOR_COSTING

    def _interval_s3_cost_chf(
        self,
        sample: dict[str, Any],
        interval_hours: float,
    ) -> float | None:
        s3_rate = self._settings.service_consumption_cost_s3_chf_per_gb_month
        if s3_rate is None or s3_rate < 0:
            return None
        total_bytes = ((sample.get("s3") or {}).get("totalBytes"))
        total_gb = _decimal_gb_from_bytes(total_bytes)
        if total_gb is None:
            return None
        return total_gb * float(s3_rate) * interval_hours / HOURS_PER_MONTH_FOR_COSTING

    def _interval_persistent_volume_cost_chf(
        self,
        sample: dict[str, Any],
        interval_hours: float,
    ) -> float | None:
        pv_rate = self._settings.service_consumption_cost_pv_chf_per_gb_month
        if pv_rate is None or pv_rate < 0:
            return None
        bytes_capacity = ((sample.get("persistentVolume") or {}).get("bytesProvisioned"))
        capacity_gb = _decimal_gb_from_bytes(bytes_capacity)
        if capacity_gb is None:
            return None
        return (
            capacity_gb * float(pv_rate) * interval_hours / HOURS_PER_MONTH_FOR_COSTING
        )

    def _interval_pg_cost_chf(
        self,
        interval_hours: float,
    ) -> float | None:
        current_year = datetime.now(UTC).year
        pg_fee = self._pg_monthly_fee_chf(current_year)
        return float(pg_fee) * interval_hours / HOURS_PER_MONTH_FOR_COSTING

    def _pg_total_bytes(self) -> int:
        return sum(size_bytes for _label, size_bytes in PG_STATIC_INSTANCES)

    def _append_cost_event_locked(self, cost_event: dict[str, Any]) -> None:
        observed_at = _parse_iso_datetime(cost_event.get("endAtUtc"))
        if observed_at is None:
            raise ValueError("Cost event endAtUtc is required.")
        self._cost_events.append(_json_clone(cost_event))
        self._mark_s3_snapshot_dirty_locked()

    def _load_cost_events_for_year_locked(self, year: int) -> list[dict[str, Any]]:
        return [
            _json_clone(cost_event)
            for cost_event in self._cost_events
            if (observed_at := _parse_iso_datetime(cost_event.get("endAtUtc"))) is not None
            and observed_at.year == year
        ]

    def _read_budget_locked(self, year: int) -> dict[str, Any]:
        entry = self._budget_entries.get(str(year))
        return _json_clone(entry) if isinstance(entry, dict) else {}

    def _write_budget_locked(
        self,
        *,
        year: int,
        annual_budget_chf: float,
        updated_at: datetime,
    ) -> None:
        self._budget_entries[str(year)] = {
            "annualBudgetChf": _normalized_currency(annual_budget_chf),
            "updatedAtUtc": updated_at.isoformat(),
        }
        self._mark_s3_snapshot_dirty_locked()

    def _month_end_date(self, year: int, month: int) -> date:
        if month >= 12:
            return datetime(year + 1, 1, 1, tzinfo=UTC).date() - timedelta(days=1)
        return datetime(year, month + 1, 1, tzinfo=UTC).date() - timedelta(days=1)

    def _build_recent_metric_history(
        self,
        samples: list[dict[str, Any]],
        *,
        metric_group: str,
        app_series: str,
        node_series: str,
        value_key: str,
        bucket_seconds: int,
        integer_values: bool,
    ) -> dict[str, list[object]]:
        buckets: dict[int, dict[str, object]] = {}
        for sample in samples:
            observed_at = _parse_iso_datetime(sample.get("timestampUtc"))
            if observed_at is None:
                continue
            bucket_key = int(observed_at.timestamp()) // max(bucket_seconds, 1)
            bucket = buckets.setdefault(
                bucket_key,
                {
                    "timestamp": observed_at.replace(microsecond=0).isoformat(),
                    "service_sum": 0.0,
                    "service_count": 0,
                    "node_sum": 0.0,
                    "node_count": 0,
                },
            )
            bucket["timestamp"] = observed_at.replace(microsecond=0).isoformat()

            service_value = (
                ((sample.get(metric_group) or {}).get(app_series) or {}).get(value_key)
            )
            node_value = (
                ((sample.get(metric_group) or {}).get(node_series) or {}).get(value_key)
            )
            if service_value is not None:
                bucket["service_sum"] = float(bucket["service_sum"]) + float(service_value)
                bucket["service_count"] = int(bucket["service_count"]) + 1
            if node_value is not None:
                bucket["node_sum"] = float(bucket["node_sum"]) + float(node_value)
                bucket["node_count"] = int(bucket["node_count"]) + 1

        timestamps: list[str] = []
        service_values: list[object] = []
        node_values: list[object] = []
        for bucket_key in sorted(buckets):
            bucket = buckets[bucket_key]
            timestamps.append(str(bucket["timestamp"]))
            service_values.append(
                self._bucket_value(
                    total=float(bucket["service_sum"]),
                    count=int(bucket["service_count"]),
                    integer_values=integer_values,
                )
            )
            node_values.append(
                self._bucket_value(
                    total=float(bucket["node_sum"]),
                    count=int(bucket["node_count"]),
                    integer_values=integer_values,
                )
            )

        return {
            "timestamps": timestamps,
            "service": service_values,
            "node": node_values,
        }

    def _bucket_value(
        self,
        *,
        total: float,
        count: int,
        integer_values: bool,
    ) -> int | float | None:
        if count <= 0:
            return None
        average = total / count
        if integer_values:
            return int(round(average))
        return _normalized_float(average)

    def _build_s3_history(self, samples: list[dict[str, Any]]) -> dict[str, list[object]]:
        points: dict[str, int | None] = {}
        for sample in samples:
            s3_payload = sample.get("s3") or {}
            if not isinstance(s3_payload, dict):
                continue
            sampled_at = _parse_iso_datetime(s3_payload.get("sampledAtUtc"))
            if sampled_at is None:
                sampled_at = _parse_iso_datetime(sample.get("timestampUtc"))
            if sampled_at is None:
                continue
            points[sampled_at.replace(microsecond=0).isoformat()] = (
                s3_payload.get("totalBytes")
            )

        timestamps = sorted(points)
        return {
            "timestamps": timestamps,
            "values": [points[timestamp] for timestamp in timestamps],
        }

    def _build_persistent_volume_history(
        self,
        samples: list[dict[str, Any]],
    ) -> dict[str, list[object]]:
        points: dict[str, int | None] = {}
        for sample in samples:
            persistent_volume_payload = sample.get("persistentVolume") or {}
            if not isinstance(persistent_volume_payload, dict):
                continue
            observed_at = _parse_iso_datetime(sample.get("timestampUtc"))
            if observed_at is None:
                continue
            points[observed_at.replace(microsecond=0).isoformat()] = (
                persistent_volume_payload.get("bytesUsed")
            )

        timestamps = sorted(points)
        return {
            "timestamps": timestamps,
            "values": [points[timestamp] for timestamp in timestamps],
        }

    def _append_sample_locked(self, sample: dict[str, Any]) -> None:
        observed_at = _parse_iso_datetime(sample.get("timestampUtc"))
        if observed_at is None:
            raise ValueError("Sample timestampUtc is required.")
        self._samples.append(_json_clone(sample))
        self._samples.sort(
            key=lambda item: _parse_iso_datetime(item.get("timestampUtc"))
            or datetime.min.replace(tzinfo=UTC)
        )
        self._mark_s3_snapshot_dirty_locked()

    def _load_latest_sample_locked(self) -> dict[str, Any] | None:
        if not self._samples:
            return None
        return _json_clone(self._samples[-1])

    def _load_samples_since_locked(self, cutoff: datetime) -> list[dict[str, Any]]:
        return self._load_samples_between_locked(cutoff, datetime.now(UTC))

    def _load_samples_between_locked(
        self,
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        samples: list[dict[str, Any]] = []
        for sample in self._samples:
            observed_at = _parse_iso_datetime(sample.get("timestampUtc"))
            if observed_at is None:
                continue
            if start_at <= observed_at <= end_at:
                samples.append(_json_clone(sample))
        return samples

    def _prune_history_locked(
        self,
        *,
        reference_time: datetime,
        force: bool = False,
    ) -> None:
        if (
            not force
            and self._last_pruned_at is not None
            and (reference_time - self._last_pruned_at) < PRUNE_INTERVAL
        ):
            return

        cutoff = reference_time - timedelta(
            hours=self._settings.service_consumption_retention_hours
        )
        before_count = len(self._samples)
        self._samples = [
            sample
            for sample in self._samples
            if (observed_at := _parse_iso_datetime(sample.get("timestampUtc"))) is not None
            and observed_at >= cutoff
        ]
        if len(self._samples) != before_count:
            self._mark_s3_snapshot_dirty_locked()
        self._prune_financial_cost_events_locked(reference_time)
        self._last_pruned_at = reference_time

    def _prune_financial_cost_events_locked(self, reference_time: datetime) -> None:
        cutoff = self._financial_cost_event_cutoff(reference_time)
        before_count = len(self._cost_events)
        self._cost_events = [
            cost_event
            for cost_event in self._cost_events
            if (observed_at := _parse_iso_datetime(cost_event.get("endAtUtc"))) is not None
            and observed_at >= cutoff
        ]
        if len(self._cost_events) != before_count:
            self._mark_s3_snapshot_dirty_locked()

    def _financial_cost_event_cutoff(self, reference_time: datetime) -> datetime:
        return reference_time - timedelta(
            hours=self._settings.service_consumption_retention_hours
        )

    def _restore_s3_cache_locked(self, sample: dict[str, Any]) -> None:
        s3_payload = sample.get("s3") or {}
        if not isinstance(s3_payload, dict):
            return
        sampled_at = _parse_iso_datetime(s3_payload.get("sampledAtUtc"))
        if sampled_at is None:
            return
        status_payload = sample.get("status") or {}
        s3_status = (
            status_payload.get("s3Metrics")
            if isinstance(status_payload, dict)
            else None
        )
        if not isinstance(s3_status, dict):
            s3_status = {
                "available": False,
                "detail": "S3 metrics have not been sampled yet.",
            }
        self._last_s3_reading = CachedS3Reading(
            metrics={
                "totalBytes": int(s3_payload.get("totalBytes"))
                if s3_payload.get("totalBytes") is not None
                else None,
                "bucketCount": int(s3_payload.get("bucketCount") or 0),
            },
            status=s3_status,
            sampled_at=sampled_at,
        )

    def _public_sample(self, sample: dict[str, Any] | None) -> dict[str, Any] | None:
        if sample is None:
            return None
        public_sample = dict(sample)
        public_sample.pop("internal", None)
        return public_sample

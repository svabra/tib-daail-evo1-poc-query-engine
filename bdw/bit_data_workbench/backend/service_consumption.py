from __future__ import annotations

import json
import logging
import math
import shutil
import ssl
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib import error as urllib_error
from urllib import request as urllib_request

from botocore.exceptions import BotoCoreError, ClientError

from ..config import Settings
from .s3_storage import list_s3_buckets_from_client, s3_client


logger = logging.getLogger(__name__)

SERVICE_CONSUMPTION_WINDOWS: dict[str, dict[str, timedelta | int]] = {
    "15m": {
        "duration": timedelta(minutes=15),
        "bucket_seconds": 3,
    },
    "1h": {
        "duration": timedelta(hours=1),
        "bucket_seconds": 15,
    },
    "6h": {
        "duration": timedelta(hours=6),
        "bucket_seconds": 60,
    },
    "24h": {
        "duration": timedelta(hours=24),
        "bucket_seconds": 300,
    },
    "48h": {
        "duration": timedelta(hours=48),
        "bucket_seconds": 600,
    },
}
SERVICE_CONSUMPTION_DEFAULT_WINDOW = "24h"
TOPOLOGY_COPY = (
    "The current PoC runs API, backend, frontend, and query execution on a single node."
)
SCALE_OUT_COPY = (
    "Query nodes will scale out automatically under higher query pressure once DAAIFL goes live."
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


def _json_line(sample: dict[str, Any]) -> str:
    return json.dumps(sample, separators=(",", ":"), sort_keys=True)


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
    ) -> None:
        self._settings = settings
        self._state_change_callback = state_change_callback
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._version = 0
        self._latest_sample: dict[str, Any] | None = None
        self._latest_sample_timestamp: datetime | None = None
        self._last_app_metrics_reading: AppMetricsReading | None = None
        self._last_s3_reading: CachedS3Reading | None = None
        self._last_pruned_at: datetime | None = None
        self._kubernetes_client = KubernetesApiClient()

    @property
    def history_root(self) -> Path:
        return self._settings.service_consumption_data_dir / "history"

    def start(self) -> None:
        self.history_root.mkdir(parents=True, exist_ok=True)
        snapshot_to_publish: dict[str, Any] | None = None
        now = datetime.now(UTC)
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                return
            self._stop_event.clear()
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

    def realtime_payload(self) -> dict[str, Any]:
        with self._lock:
            return self._build_realtime_payload_locked()

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
        }

    def _normalize_window(self, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in SERVICE_CONSUMPTION_WINDOWS:
            raise ValueError(
                "Unsupported service-consumption window. "
                f"Expected one of: {', '.join(SERVICE_CONSUMPTION_WINDOWS)}"
            )
        return normalized

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
        with self._lock:
            self._append_sample_locked(sample)
            self._latest_sample = sample
            self._latest_sample_timestamp = sample_timestamp
            self._restore_s3_cache_locked(sample)
            self._prune_history_locked(reference_time=sample_timestamp)
            self._version += 1
            return self._build_realtime_payload_locked()

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
        mount_path = self._settings.service_consumption_data_dir
        try:
            mount_path.mkdir(parents=True, exist_ok=True)
            usage = shutil.disk_usage(mount_path)
            directory_usage_bytes = self._directory_usage_bytes(mount_path)
        except OSError as exc:
            logger.warning("Failed to collect persistent volume usage: %s", exc)
            return {
                "bytesUsed": None,
                "bytesCapacity": None,
                "mountPath": mount_path.as_posix(),
            }, {
                "available": False,
                "detail": str(exc),
            }

        return {
            "bytesUsed": int(directory_usage_bytes),
            "bytesCapacity": int(usage.total),
            "mountPath": mount_path.as_posix(),
        }, {
            "available": True,
            "detail": f"Persistent volume usage is available from {mount_path.as_posix()}.",
        }

    def _directory_usage_bytes(self, root: Path) -> int:
        total_bytes = 0
        for path in root.rglob("*"):
            try:
                if not path.is_file():
                    continue
                total_bytes += path.stat().st_size
            except OSError:
                continue
        return total_bytes

    def _build_realtime_payload_locked(self) -> dict[str, Any]:
        return {
            "version": self._version,
            "latest": self._public_sample(self._latest_sample),
            "status": self._status_payload_locked(),
            "topology": self._topology_payload_locked(),
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
        path = self._history_path_for(observed_at)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(_json_line(sample))
            handle.write("\n")

    def _history_path_for(self, observed_at: datetime) -> Path:
        return (
            self.history_root
            / f"{observed_at.year:04d}"
            / f"{observed_at.month:02d}"
            / f"{observed_at.day:02d}.jsonl"
        )

    def _load_latest_sample_locked(self) -> dict[str, Any] | None:
        history_root = self.history_root
        if not history_root.exists():
            return None

        latest_path = next(
            iter(
                sorted(
                    history_root.rglob("*.jsonl"),
                    key=lambda item: item.as_posix(),
                    reverse=True,
                )
            ),
            None,
        )
        if latest_path is None:
            return None
        try:
            lines = latest_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return None
        for raw_line in reversed(lines):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                return json.loads(raw_line)
            except json.JSONDecodeError:
                continue
        return None

    def _load_samples_since_locked(self, cutoff: datetime) -> list[dict[str, Any]]:
        return self._load_samples_between_locked(cutoff, datetime.now(UTC))

    def _load_samples_between_locked(
        self,
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        samples: list[dict[str, Any]] = []
        current_date = start_at.date()
        end_date = end_at.date()
        while current_date <= end_date:
            path = self._history_path_for(
                datetime(
                    current_date.year,
                    current_date.month,
                    current_date.day,
                    tzinfo=UTC,
                )
            )
            if path.is_file():
                try:
                    raw_lines = path.read_text(encoding="utf-8").splitlines()
                except OSError:
                    raw_lines = []
                for raw_line in raw_lines:
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    try:
                        sample = json.loads(raw_line)
                    except json.JSONDecodeError:
                        continue
                    observed_at = _parse_iso_datetime(sample.get("timestampUtc"))
                    if observed_at is None:
                        continue
                    if start_at <= observed_at <= end_at:
                        samples.append(sample)
            current_date += timedelta(days=1)
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
        history_root = self.history_root
        if not history_root.exists():
            self._last_pruned_at = reference_time
            return

        for path in sorted(history_root.rglob("*.jsonl")):
            try:
                raw_lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue

            kept_lines: list[str] = []
            for raw_line in raw_lines:
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    sample = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue
                observed_at = _parse_iso_datetime(sample.get("timestampUtc"))
                if observed_at is None or observed_at < cutoff:
                    continue
                kept_lines.append(_json_line(sample))

            if not kept_lines:
                try:
                    path.unlink()
                except OSError:
                    continue
                continue

            rewritten = "\n".join(kept_lines) + "\n"
            original = "\n".join(line.strip() for line in raw_lines if line.strip())
            if rewritten.rstrip("\n") == original:
                continue
            try:
                path.write_text(rewritten, encoding="utf-8")
            except OSError:
                continue

        for directory in sorted(history_root.rglob("*"), reverse=True):
            if not directory.is_dir():
                continue
            try:
                next(directory.iterdir())
            except StopIteration:
                try:
                    directory.rmdir()
                except OSError:
                    pass
            except OSError:
                continue

        self._last_pruned_at = reference_time

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

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from io import BytesIO
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

from botocore.exceptions import ClientError


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.service_consumption import (  # noqa: E402
    SERVICE_CONSUMPTION_S3_STATE_KEY,
    ServiceConsumptionMonitor,
    parse_cgroup_cpu_limit_cores,
    parse_cgroup_cpu_usage_micros,
    parse_kubernetes_cpu_quantity,
    parse_kubernetes_memory_quantity,
)
from bit_data_workbench.config import Settings  # noqa: E402


def build_settings(data_dir: Path, *, retention_hours: int = 48) -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIFL Workbench",
        image_version="test",
        port=8000,
        duckdb_database=data_dir / "workspace.duckdb",
        duckdb_extension_directory=data_dir / "extensions",
        service_consumption_data_dir=data_dir,
        service_consumption_cpu_memory_interval_seconds=3,
        service_consumption_s3_interval_seconds=3600,
        service_consumption_retention_hours=retention_hours,
        service_consumption_cost_node_chf_per_hour=None,
        service_consumption_cost_app_chf_per_month=None,
        service_consumption_cost_s3_chf_per_gb_month=None,
        service_consumption_cost_pv_chf_per_gb_month=None,
        service_consumption_cost_pg_chf_per_gb_month=None,
        service_consumption_cost_cpu_weight=0.5,
        service_consumption_cost_ram_weight=0.5,
        max_result_rows=200,
        s3_endpoint=None,
        s3_bucket=None,
        s3_access_key_id=None,
        s3_access_key_id_file=None,
        s3_secret_access_key=None,
        s3_secret_access_key_file=None,
        s3_url_style=None,
        s3_use_ssl=True,
        s3_verify_ssl=True,
        s3_ca_cert_file=None,
        s3_session_token=None,
        s3_session_token_file=None,
        s3_startup_view_schema="s3",
        s3_startup_views=None,
        pg_host=None,
        pg_port=None,
        pg_user=None,
        pg_password=None,
        pg_oltp_database=None,
        pg_olap_database=None,
        pod_name="bdw-pod",
        pod_namespace="bdw-namespace",
        pod_ip="127.0.0.1",
        node_name="bdw-node",
    )


def build_sample(
    timestamp: datetime,
    *,
    cpu_value: float = 1.25,
    cpu_capacity_cores: float = 12.0,
    memory_bytes_used: int = 2_000_000_000,
    memory_capacity_bytes: int = 24_000_000_000,
    s3_bytes: int = 0,
    s3_sampled_at: datetime | None = None,
    persistent_volume_bytes: int = 0,
    persistent_volume_capacity_bytes: int | None = None,
) -> dict[str, object]:
    s3_observed_at = (s3_sampled_at or timestamp).astimezone(UTC).replace(microsecond=0)
    return {
        "timestampUtc": timestamp.astimezone(UTC).replace(microsecond=0).isoformat(),
        "cpu": {
            "app": {
                "coresUsed": cpu_value,
                "percentOfLimit": 25.0,
                "limitCores": 5.0,
            },
            "node": {
                "coresUsed": cpu_value * 2,
                "percentOfCapacity": 22.5,
                "capacityCores": cpu_capacity_cores,
            },
        },
        "memory": {
            "app": {
                "bytesUsed": memory_bytes_used,
                "percentOfLimit": 40.0,
                "limitBytes": 5_000_000_000,
            },
            "node": {
                "bytesUsed": 8_000_000_000,
                "percentOfCapacity": 33.0,
                "capacityBytes": memory_capacity_bytes,
            },
        },
        "s3": {
            "totalBytes": s3_bytes,
            "bucketCount": 2,
            "sampledAtUtc": s3_observed_at.isoformat(),
        },
        "persistentVolume": {
            "bytesUsed": persistent_volume_bytes,
            "bytesCapacity": persistent_volume_capacity_bytes,
            "bytesProvisioned": persistent_volume_capacity_bytes,
            "percentOfCapacity": 25.0 if persistent_volume_capacity_bytes else None,
            "mountPath": "",
        },
        "status": {
            "nodeMetrics": {"available": True, "detail": "ok"},
            "s3Metrics": {"available": True, "detail": "ok"},
            "persistentVolumeMetrics": {"available": True, "detail": "ok"},
        },
        "nodeName": "bdw-node",
        "podName": "bdw-pod",
        "podNamespace": "bdw-namespace",
        "internal": {"appCpuUsageMicros": 123456},
    }


class RecordingKubernetesClient:
    def __init__(self) -> None:
        self.available_calls = 0
        self.paths: list[str] = []

    def available(self) -> bool:
        self.available_calls += 1
        return True

    def get_json(self, path: str) -> dict[str, object]:
        self.paths.append(path)
        raise AssertionError(f"Kubernetes API should not be called: {path}")


class FakeS3Client:
    def __init__(self, *, fail_put: bool = False) -> None:
        self.fail_put = fail_put
        self.objects: dict[tuple[str, str], bytes] = {}
        self.put_calls: list[dict[str, object]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        identifier = (Bucket, Key)
        if identifier not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "GetObject",
            )
        return {"Body": BytesIO(self.objects[identifier])}

    def put_object(self, **kwargs) -> dict[str, object]:
        self.put_calls.append(kwargs)
        if self.fail_put:
            raise RuntimeError("s3 write failed")
        body = kwargs.get("Body") or b""
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = bytes(body)
        return {}


def enable_s3_snapshot_settings(settings: Settings) -> None:
    settings.s3_endpoint = "http://127.0.0.1:9000"
    settings.s3_bucket = "bdw-tests"
    settings.s3_access_key_id = "access"
    settings.s3_secret_access_key = "secret"
    settings.s3_use_ssl = False
    settings.s3_verify_ssl = False


class ServiceConsumptionMonitorTests(unittest.TestCase):
    def test_quantity_parsers_cover_kubernetes_and_cgroup_formats(self) -> None:
        self.assertAlmostEqual(parse_kubernetes_cpu_quantity("250m"), 0.25)
        self.assertEqual(parse_kubernetes_memory_quantity("512Mi"), 536_870_912)
        self.assertEqual(parse_cgroup_cpu_usage_micros("usage_usec 12500"), 12_500)
        self.assertAlmostEqual(parse_cgroup_cpu_limit_cores("200000 100000"), 2.0)

    def test_disabled_node_metrics_do_not_call_kubernetes_api(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            settings.service_consumption_node_metrics_enabled = False
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
            )
            kubernetes_client = RecordingKubernetesClient()
            monitor._kubernetes_client = kubernetes_client

            metrics, status = monitor._collect_node_metrics()

            self.assertEqual(
                metrics,
                {
                    "cpuCoresUsed": None,
                    "cpuCapacityCores": None,
                    "memoryBytesUsed": None,
                    "memoryCapacityBytes": None,
                },
            )
            self.assertFalse(status["available"])
            self.assertEqual(
                status["detail"],
                "Kubernetes node metrics collection is disabled.",
            )
            self.assertEqual(kubernetes_client.available_calls, 0)
            self.assertEqual(kubernetes_client.paths, [])

    def test_persistent_volume_metrics_disabled_without_kubernetes_or_filesystem_scan(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir)
            (data_dir / "usage.bin").write_bytes(b"usage")
            settings = build_settings(data_dir)
            settings.service_consumption_pvc_capacity_enabled = False
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
            )
            kubernetes_client = RecordingKubernetesClient()
            monitor._kubernetes_client = kubernetes_client

            metrics, status = monitor._collect_persistent_volume_metrics()

            self.assertFalse(status["available"])
            self.assertIsNone(metrics["bytesUsed"])
            self.assertIsNone(metrics["bytesCapacity"])
            self.assertIsNone(metrics["bytesProvisioned"])
            self.assertEqual(metrics["mountPath"], "")
            self.assertEqual(kubernetes_client.available_calls, 0)
            self.assertEqual(kubernetes_client.paths, [])

    def test_state_payload_builds_recent_history_and_hourly_s3_series(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
            )
            now = datetime.now(UTC).replace(microsecond=0)
            s3_hour_one = now - timedelta(hours=2)
            s3_hour_two = now - timedelta(hours=1)

            monitor._store_sample(
                build_sample(
                    s3_hour_one - timedelta(minutes=20),
                    cpu_value=0.8,
                    s3_bytes=120,
                    s3_sampled_at=s3_hour_one,
                    persistent_volume_bytes=320,
                )
            )
            monitor._store_sample(
                build_sample(
                    s3_hour_one - timedelta(minutes=5),
                    cpu_value=1.0,
                    s3_bytes=120,
                    s3_sampled_at=s3_hour_one,
                    persistent_volume_bytes=384,
                )
            )
            monitor._store_sample(
                build_sample(
                    s3_hour_two,
                    cpu_value=1.4,
                    s3_bytes=240,
                    s3_sampled_at=s3_hour_two,
                    persistent_volume_bytes=448,
                )
            )

            payload = monitor.state_payload(window="24h")

            self.assertEqual(payload["window"], "48h")
            self.assertEqual(payload["latest"]["s3"]["sampledAtUtc"], s3_hour_two.isoformat())
            self.assertTrue(payload["cpuHistory"]["timestamps"])
            self.assertTrue(payload["memoryHistory"]["timestamps"])
            self.assertEqual(payload["s3History"]["timestamps"], [s3_hour_one.isoformat(), s3_hour_two.isoformat()])
            self.assertEqual(payload["s3History"]["values"], [120, 240])
            self.assertEqual(
                payload["persistentVolumeHistory"]["values"],
                [320, 384, 448],
            )
            self.assertTrue(payload["status"]["persistentVolumeMetricsAvailable"])
            self.assertNotIn("internal", payload["latest"])

    def test_state_payload_builds_financial_year_to_date_and_forecast(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            settings.service_consumption_cost_node_chf_per_hour = 10.0
            settings.service_consumption_cost_s3_chf_per_gb_month = 73.0
            settings.service_consumption_cost_pv_chf_per_gb_month = 7.3
            settings.service_consumption_cost_cpu_weight = 0.5
            settings.service_consumption_cost_ram_weight = 0.5
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
            )
            now = datetime.now(UTC).replace(microsecond=0)

            monitor.update_budget(year=now.year, annual_budget_chf=120_000.0)
            monitor._store_sample(
                build_sample(
                    now - timedelta(hours=2),
                    cpu_value=1.0,
                    cpu_capacity_cores=10.0,
                    memory_bytes_used=2_000_000_000,
                    memory_capacity_bytes=10_000_000_000,
                    s3_bytes=1_000_000_000,
                    s3_sampled_at=now - timedelta(hours=2),
                    persistent_volume_bytes=512_000_000,
                )
            )
            monitor._store_sample(
                build_sample(
                    now - timedelta(hours=1),
                    cpu_value=1.0,
                    cpu_capacity_cores=10.0,
                    memory_bytes_used=2_000_000_000,
                    memory_capacity_bytes=10_000_000_000,
                    s3_bytes=1_000_000_000,
                    s3_sampled_at=now - timedelta(hours=1),
                    persistent_volume_bytes=768_000_000,
                )
            )

            payload = monitor.state_payload(window="24h")
            elapsed_days = (
                now.date() - datetime(now.year, 1, 1, tzinfo=UTC).date()
            ).days + 1
            assumed_dynamic_end = min(
                datetime(now.year, 3, 31, tzinfo=UTC).date(),
                now.date(),
            )
            assumed_dynamic_days = (
                (assumed_dynamic_end - datetime(now.year, 1, 1, tzinfo=UTC).date()).days
                + 1
                if assumed_dynamic_end >= datetime(now.year, 1, 1, tzinfo=UTC).date()
                else 0
            )
            days_in_year = (
                datetime(now.year + 1, 1, 1, tzinfo=UTC).date()
                - datetime(now.year, 1, 1, tzinfo=UTC).date()
            ).days
            expected_compute_ytd = round((36.0 * assumed_dynamic_days) + 1.5, 2)
            expected_s3_ytd = round((2.4 * assumed_dynamic_days) + 0.1, 2)
            expected_pv_ytd = 0.0
            expected_container_cpu_ytd = round((12.0 * assumed_dynamic_days) + 0.5, 2)
            expected_container_ram_ytd = round((24.0 * assumed_dynamic_days) + 1.0, 2)
            expected_application_ytd = round(500.0 * (elapsed_days / days_in_year), 2)
            expected_pg_ytd = round(2 * 15.14 * elapsed_days, 2)
            expected_total_ytd = round(
                expected_compute_ytd
                + expected_s3_ytd
                + expected_pv_ytd
                + expected_application_ytd
                + expected_pg_ytd,
                2,
            )

            self.assertEqual(payload["financial"]["currency"], "CHF")
            self.assertEqual(payload["financial"]["annualBudgetChf"], 120_000.0)
            self.assertAlmostEqual(payload["financial"]["spentYearToDateChf"], expected_total_ytd, places=2)
            self.assertAlmostEqual(
                payload["financial"]["breakdownYearToDate"]["computeChf"],
                expected_compute_ytd,
                places=2,
            )
            self.assertAlmostEqual(
                payload["financial"]["breakdownYearToDate"]["applicationChf"],
                expected_application_ytd,
                places=2,
            )
            self.assertAlmostEqual(
                payload["financial"]["breakdownYearToDate"]["s3Chf"],
                expected_s3_ytd,
                places=2,
            )
            self.assertAlmostEqual(
                payload["financial"]["breakdownYearToDate"]["persistentVolumeChf"],
                expected_pv_ytd,
                places=2,
            )
            self.assertAlmostEqual(
                payload["financial"]["breakdownYearToDate"]["pgChf"],
                expected_pg_ytd,
                places=2,
            )
            self.assertTrue(payload["financial"]["status"]["budgetConfigured"])
            self.assertEqual(len(payload["financial"]["monthly"]["labels"]), 12)
            self.assertEqual(payload["financial"]["monthly"]["comparisonYear"], now.year - 1)
            services = payload["financial"]["services"]
            self.assertEqual(
                [service["key"] for service in services],
                ["container", "application", "filesystem", "s3", "pg"],
            )
            self.assertAlmostEqual(
                services[0]["details"]["cpuChf"],
                expected_container_cpu_ytd,
                places=2,
            )
            self.assertAlmostEqual(
                services[0]["details"]["ramChf"],
                expected_container_ram_ytd,
                places=2,
            )
            self.assertAlmostEqual(services[1]["costYtdChf"], expected_application_ytd, places=2)
            self.assertEqual(services[1]["details"]["annualFeeChf"], 500.0)
            self.assertEqual(services[2]["status"]["state"], "unavailable")
            self.assertEqual(services[2]["costYtdChf"], 0.0)
            self.assertAlmostEqual(services[4]["costYtdChf"], expected_pg_ytd, places=2)
            self.assertEqual(services[4]["details"]["instances"][0]["label"], "OLTP")
            self.assertEqual(services[4]["details"]["instances"][1]["label"], "OLAP")
            self.assertEqual(services[4]["details"]["instances"][0]["sizeGb"], 80.0)
            self.assertEqual(services[4]["details"]["instances"][1]["sizeGb"], 80.0)
            self.assertEqual(
                services[4]["details"]["annualFeePerInstanceChf"],
                round(15.14 * days_in_year, 2),
            )

    def test_prune_history_removes_samples_older_than_retention_hours(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir), retention_hours=48)
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
            )
            stale_timestamp = datetime.now(UTC) - timedelta(hours=49)
            fresh_timestamp = datetime.now(UTC) - timedelta(hours=2)
            stale_sample = build_sample(stale_timestamp, s3_bytes=32)
            fresh_sample = build_sample(fresh_timestamp, s3_bytes=64)

            with monitor._lock:
                monitor._append_sample_locked(stale_sample)
                monitor._append_sample_locked(fresh_sample)
                monitor._cost_events = [
                    {
                        "startAtUtc": (stale_timestamp - timedelta(minutes=1)).isoformat(),
                        "endAtUtc": stale_timestamp.isoformat(),
                    },
                    {
                        "startAtUtc": (fresh_timestamp - timedelta(minutes=1)).isoformat(),
                        "endAtUtc": fresh_timestamp.isoformat(),
                    },
                ]
                monitor._prune_history_locked(
                    reference_time=datetime.now(UTC),
                    force=True,
                )

            self.assertEqual(len(monitor._samples), 1)
            self.assertEqual(
                monitor._samples[0]["timestampUtc"],
                fresh_timestamp.replace(microsecond=0).isoformat(),
            )
            self.assertEqual(len(monitor._cost_events), 1)
            self.assertEqual(
                monitor._cost_events[0]["endAtUtc"],
                fresh_timestamp.isoformat(),
            )

    def test_budget_persists_to_in_memory_financial_store(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
            )
            current_year = datetime.now(UTC).year

            payload = monitor.update_budget(year=current_year, annual_budget_chf=42_500.0)

            self.assertEqual(payload["year"], current_year)
            self.assertEqual(
                monitor.state_payload(window="24h")["financial"]["annualBudgetChf"],
                42_500.0,
            )

    def test_s3_snapshot_does_not_flush_before_five_minutes(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            enable_s3_snapshot_settings(settings)
            fake_s3 = FakeS3Client()
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
                s3_client_factory=lambda _settings: fake_s3,
            )
            now = datetime.now(UTC).replace(microsecond=0)
            monitor._last_s3_snapshot_flush_at = now

            monitor._store_sample(build_sample(now + timedelta(minutes=1), s3_bytes=64))

            self.assertEqual(fake_s3.put_calls, [])

    def test_s3_snapshot_flushes_one_compact_object_after_five_minutes(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            enable_s3_snapshot_settings(settings)
            fake_s3 = FakeS3Client()
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
                s3_client_factory=lambda _settings: fake_s3,
            )
            now = datetime.now(UTC).replace(microsecond=0)
            monitor._last_s3_snapshot_flush_at = now - timedelta(minutes=5)
            current_year = now.year
            monitor.update_budget(year=current_year, annual_budget_chf=42_500.0)

            monitor._store_sample(build_sample(now, s3_bytes=64))

            self.assertEqual(len(fake_s3.put_calls), 1)
            put_call = fake_s3.put_calls[0]
            self.assertEqual(put_call["Bucket"], "bdw-tests")
            self.assertEqual(put_call["Key"], SERVICE_CONSUMPTION_S3_STATE_KEY)
            snapshot = json.loads(bytes(put_call["Body"]).decode("utf-8"))
            self.assertEqual(len(snapshot["samples"]), 1)
            self.assertEqual(snapshot["budgets"][str(current_year)]["annualBudgetChf"], 42_500.0)

    def test_s3_snapshot_skips_overlapping_flush(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            enable_s3_snapshot_settings(settings)
            fake_s3 = FakeS3Client()
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
                s3_client_factory=lambda _settings: fake_s3,
            )
            now = datetime.now(UTC).replace(microsecond=0)
            monitor._last_s3_snapshot_flush_at = now - timedelta(minutes=5)
            monitor._s3_snapshot_flush_in_progress = True

            monitor._store_sample(build_sample(now, s3_bytes=64))

            self.assertEqual(fake_s3.put_calls, [])
            monitor._s3_snapshot_flush_in_progress = False

    def test_s3_snapshot_failure_is_throttled_and_keeps_monitoring_alive(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            enable_s3_snapshot_settings(settings)
            fake_s3 = FakeS3Client(fail_put=True)
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
                s3_client_factory=lambda _settings: fake_s3,
            )
            now = datetime.now(UTC).replace(microsecond=0)
            monitor._last_s3_snapshot_flush_at = now - timedelta(minutes=5)

            with self.assertLogs(
                "bit_data_workbench.backend.service_consumption",
                level="WARNING",
            ) as logs:
                first_payload = monitor._store_sample(build_sample(now, s3_bytes=64))
                monitor._store_sample(build_sample(now + timedelta(minutes=1), s3_bytes=96))
                monitor._store_sample(build_sample(now + timedelta(minutes=5), s3_bytes=128))

            self.assertEqual(len(fake_s3.put_calls), 2)
            self.assertEqual(
                sum(
                    "Failed to persist service-consumption S3 snapshot" in message
                    for message in logs.output
                ),
                1,
            )
            self.assertEqual(first_payload["latest"]["s3"]["totalBytes"], 64)
            self.assertEqual(
                monitor.state_payload(window="48h")["latest"]["s3"]["totalBytes"],
                128,
            )

    def test_s3_snapshot_restore_loads_budget_and_recent_samples(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            enable_s3_snapshot_settings(settings)
            fake_s3 = FakeS3Client()
            now = datetime.now(UTC).replace(microsecond=0)
            current_year = now.year
            fake_s3.objects[("bdw-tests", SERVICE_CONSUMPTION_S3_STATE_KEY)] = json.dumps(
                {
                    "schemaVersion": 1,
                    "latestSample": build_sample(now, s3_bytes=123),
                    "samples": [build_sample(now - timedelta(minutes=5), s3_bytes=100), build_sample(now, s3_bytes=123)],
                    "costEvents": [],
                    "budgets": {
                        str(current_year): {
                            "annualBudgetChf": 12_345.0,
                            "updatedAtUtc": now.isoformat(),
                        },
                    },
                }
            ).encode("utf-8")
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
                s3_client_factory=lambda _settings: fake_s3,
            )

            with monitor._lock:
                monitor._restore_s3_snapshot_payload_locked(monitor._load_s3_snapshot_payload())

            payload = monitor.state_payload(window="24h")
            self.assertEqual(payload["window"], "48h")
            self.assertEqual(payload["latest"]["s3"]["totalBytes"], 123)
            self.assertEqual(payload["financial"]["annualBudgetChf"], 12_345.0)


if __name__ == "__main__":
    unittest.main()

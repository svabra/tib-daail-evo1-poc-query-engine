from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.service_consumption import (  # noqa: E402
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
        app_storage_pvc_name="evo1-bdw-storage",
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
    persistent_volume_capacity_bytes: int = 10_737_418_240,
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
            "percentOfCapacity": 25.0,
            "mountPath": "/workspace/service-consumption",
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


class ServiceConsumptionMonitorTests(unittest.TestCase):
    def test_quantity_parsers_cover_kubernetes_and_cgroup_formats(self) -> None:
        self.assertAlmostEqual(parse_kubernetes_cpu_quantity("250m"), 0.25)
        self.assertEqual(parse_kubernetes_memory_quantity("512Mi"), 536_870_912)
        self.assertEqual(parse_cgroup_cpu_usage_micros("usage_usec 12500"), 12_500)
        self.assertAlmostEqual(parse_cgroup_cpu_limit_cores("200000 100000"), 2.0)

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

            self.assertEqual(payload["window"], "24h")
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
                    persistent_volume_capacity_bytes=10_000_000_000,
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
                    persistent_volume_capacity_bytes=10_000_000_000,
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
            expected_pv_ytd = round((2.4 * assumed_dynamic_days) + 0.1, 2)
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
            stale_path = monitor._history_path_for(stale_timestamp)
            fresh_path = monitor._history_path_for(fresh_timestamp)

            with monitor._lock:
                monitor._append_sample_locked(stale_sample)
                monitor._append_sample_locked(fresh_sample)
                monitor._prune_history_locked(
                    reference_time=datetime.now(UTC),
                    force=True,
                )

            if stale_path == fresh_path:
                remaining_lines = fresh_path.read_text(encoding="utf-8").splitlines()
                self.assertEqual(len([line for line in remaining_lines if line.strip()]), 1)
            else:
                self.assertFalse(stale_path.exists())
            self.assertTrue(fresh_path.exists())

    def test_budget_persists_to_shared_financial_store(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings = build_settings(Path(tmp_dir))
            monitor = ServiceConsumptionMonitor(
                settings,
                state_change_callback=lambda snapshot: None,
            )
            current_year = datetime.now(UTC).year

            payload = monitor.update_budget(year=current_year, annual_budget_chf=42_500.0)

            self.assertEqual(payload["year"], current_year)
            self.assertTrue((settings.service_consumption_data_dir / "financial" / "budgets.json").is_file())
            self.assertEqual(
                monitor.state_payload(window="24h")["financial"]["annualBudgetChf"],
                42_500.0,
            )


if __name__ == "__main__":
    unittest.main()

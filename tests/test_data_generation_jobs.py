from __future__ import annotations

import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest import TestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.data_generation_jobs import DataGenerationJobManager  # noqa: E402
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.data_generator.base import (  # noqa: E402
    DataGenerator,
    DataGeneratorContext,
    DataGeneratorResult,
)
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


def make_settings(root: Path) -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIFL Workbench",
        image_version=CURRENT_VERSION,
        port=8000,
        duckdb_database=root / "workspace.duckdb",
        duckdb_extension_directory=root / "duckdb-ext",
        service_consumption_data_dir=root / "service-consumption",
        service_consumption_cpu_memory_interval_seconds=3,
        service_consumption_s3_interval_seconds=3600,
        service_consumption_retention_hours=48,
        max_result_rows=50,
        s3_endpoint=None,
        s3_bucket=None,
        s3_access_key_id=None,
        s3_access_key_id_file=None,
        s3_secret_access_key=None,
        s3_secret_access_key_file=None,
        s3_url_style=None,
        s3_use_ssl=False,
        s3_verify_ssl=False,
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
        pod_name=None,
        pod_namespace=None,
        pod_ip=None,
        node_name=None,
    )


def wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    last_value = None
    while time.monotonic() < deadline:
        last_value = predicate()
        if last_value:
            return last_value
        time.sleep(interval)
    return last_value


class ConnectionOpeningGenerator(DataGenerator):
    generator_id = "connection-opening-loader"
    title = "Connection Opening Loader"
    description = "Opens a DuckDB connection."
    target_kind = "test"
    min_size_gb = 0.01
    max_size_gb = 1.0
    default_size_gb = 0.01

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        connection = context.connect()
        close = getattr(connection, "close", None)
        if callable(close):
            close()
        return DataGeneratorResult(target_name="connection-opening-loader")

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        return DataGeneratorResult(target_name="connection-opening-loader")


class SingleGeneratorRegistry:
    def __init__(self, generator: DataGenerator) -> None:
        self.generator_instance = generator

    def definitions(self) -> list[dict[str, object]]:
        return [self.generator_instance.definition().payload]

    def generator(self, generator_id: str) -> DataGenerator:
        if generator_id != self.generator_instance.generator_id:
            raise KeyError(generator_id)
        return self.generator_instance


class DataGenerationJobManagerTests(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="bdw-generation-jobs-")
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_loader_connection_wait_reports_progress_and_can_cancel(self) -> None:
        waiting_callback_called = threading.Event()

        def connection_factory(*, is_cancelled, waiting_callback):
            waiting_callback()
            waiting_callback_called.set()
            while not is_cancelled():
                time.sleep(0.01)
            raise RuntimeError("connection acquisition cancelled")

        manager = DataGenerationJobManager(
            settings=make_settings(self.root),
            registry=SingleGeneratorRegistry(ConnectionOpeningGenerator()),
            connection_factory=connection_factory,
            metadata_refresher=lambda: None,
        )

        job = manager.start_job(
            generator_id=ConnectionOpeningGenerator.generator_id,
            size_gb=0.01,
        )

        waiting = wait_until(
            lambda: manager.state_payload()["jobs"][0]
            if manager.state_payload()["jobs"][0]["progressLabel"] == "Waiting for active queries..."
            else None,
        )

        self.assertIsNotNone(waiting)
        self.assertTrue(waiting_callback_called.is_set())
        self.assertIn("Waiting for active queries", waiting["message"])

        cancelling = manager.cancel_job(job.job_id)

        self.assertEqual(cancelling.progress_label, "Cancelling...")
        self.assertFalse(cancelling.can_cancel)

        cancelled = wait_until(
            lambda: manager.state_payload()["jobs"][0]
            if manager.state_payload()["jobs"][0]["status"] == "cancelled"
            else None,
        )

        self.assertIsNotNone(cancelled)
        self.assertEqual(cancelled["progressLabel"], "Cancelled")
        self.assertEqual(cancelled["message"], "Data generation cancelled.")

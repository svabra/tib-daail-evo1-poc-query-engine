from __future__ import annotations

import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest import TestCase

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.query_jobs import (  # noqa: E402
    QUERY_EXECUTION_DUCKDB_READ,
    QUERY_EXECUTION_DUCKDB_WRITE,
    QUERY_EXECUTION_POSTGRES_NATIVE,
    DuckDBQueryAccessCoordinator,
    QueryJobManager,
    classify_query_execution,
)
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.models import QueryJobDefinition  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)
LONG_QUERY = """
select sum(a.i * b.j) as total_value
from range(2000000) as a(i)
cross join range(2000) as b(j)
"""


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


def wait_until(predicate, *, timeout: float = 20.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    last_value = None
    while time.monotonic() < deadline:
        last_value = predicate()
        if last_value:
            return last_value
        time.sleep(interval)
    return last_value


class QueryJobClassifierTests(TestCase):
    def test_classifies_read_queries_for_parallel_duckdb_execution(self) -> None:
        for sql in (
            "select 1",
            "with sample as (select 1 as value) select * from sample",
            "explain select * from range(10)",
            "describe select * from range(10)",
        ):
            self.assertEqual(classify_query_execution(sql, []), QUERY_EXECUTION_DUCKDB_READ)

    def test_classifies_writes_and_ambiguous_sql_as_exclusive(self) -> None:
        for sql in (
            "create table t as select 1",
            "insert into t values (1)",
            "select 1; select 2",
            "pragma version",
        ):
            self.assertEqual(classify_query_execution(sql, []), QUERY_EXECUTION_DUCKDB_WRITE)

    def test_classifies_native_postgres_sources(self) -> None:
        self.assertEqual(
            classify_query_execution("select 1", ["pg_oltp_native"]),
            QUERY_EXECUTION_POSTGRES_NATIVE,
        )


class QueryJobPayloadTests(TestCase):
    def test_payload_includes_process_metrics_and_cancellation_fields(self) -> None:
        job = QueryJobDefinition(
            job_id="query-test",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell",
            sql="select 1",
            status="running",
            started_at="2026-05-13T00:00:00+00:00",
            updated_at="2026-05-13T00:00:01+00:00",
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            process_id=1234,
            cpu_percent=12.5,
            memory_rss_bytes=1024,
            peak_memory_rss_bytes=2048,
            cancellation_phase="interrupting",
            cancellation_requested_at="2026-05-13T00:00:02+00:00",
            worker_exit_code=-15,
        )

        payload = job.payload

        self.assertEqual(payload["executionMode"], QUERY_EXECUTION_DUCKDB_READ)
        self.assertEqual(payload["processId"], 1234)
        self.assertEqual(payload["cpuPercent"], 12.5)
        self.assertEqual(payload["memoryRssBytes"], 1024)
        self.assertEqual(payload["peakMemoryRssBytes"], 2048)
        self.assertEqual(payload["cancellationPhase"], "interrupting")
        self.assertEqual(payload["workerExitCode"], -15)


class DuckDBQueryAccessCoordinatorTests(TestCase):
    def test_waiting_writer_reports_wait_and_honors_cancellation(self) -> None:
        coordinator = DuckDBQueryAccessCoordinator()
        self.assertTrue(coordinator.acquire(QUERY_EXECUTION_DUCKDB_READ, lambda: False))

        cancel_requested = threading.Event()
        waiting_reported = threading.Event()
        writer_result: list[bool] = []

        def acquire_writer() -> None:
            writer_result.append(
                coordinator.acquire(
                    QUERY_EXECUTION_DUCKDB_WRITE,
                    cancel_requested.is_set,
                    on_waiting=waiting_reported.set,
                )
            )

        thread = threading.Thread(target=acquire_writer, daemon=True)
        thread.start()

        self.assertTrue(waiting_reported.wait(timeout=2))
        cancel_requested.set()
        thread.join(timeout=2)
        coordinator.release(QUERY_EXECUTION_DUCKDB_READ)

        self.assertFalse(thread.is_alive())
        self.assertEqual(writer_result, [False])


class ProcessQueryJobManagerTests(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="bdw-query-jobs-")
        self.root = Path(self.temp_dir.name)
        self.settings = make_settings(self.root)
        self.settings.duckdb_database.parent.mkdir(parents=True, exist_ok=True)
        duckdb.connect(str(self.settings.duckdb_database)).close()
        self.manager = QueryJobManager(
            settings=self.settings,
            max_result_rows=self.settings.max_result_rows,
            notebook_title_resolver=lambda _notebook_id: "Notebook",
            metadata_refresher=lambda: None,
        )

    def tearDown(self) -> None:
        self.manager.shutdown()
        self.temp_dir.cleanup()

    def test_simple_query_completes_in_worker_process(self) -> None:
        job = self.manager.start_job(
            sql="select 1 as value",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=[],
        )

        completed = wait_until(
            lambda: self.manager.snapshot(job.job_id)
            if self.manager.snapshot(job.job_id).status == "completed"
            else None,
            timeout=20,
        )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(completed.execution_mode, QUERY_EXECUTION_DUCKDB_READ)
        self.assertIsNotNone(completed.process_id)
        self.assertEqual(completed.rows, [(1,)])
        self.assertEqual(completed.progress, 1.0)

    def test_duckdb_read_queries_run_simultaneously_in_separate_processes(self) -> None:
        first = self.manager.start_job(
            sql=LONG_QUERY,
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=[],
        )
        second = self.manager.start_job(
            sql=LONG_QUERY,
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-2",
            data_sources=[],
        )

        running_pair = wait_until(
            lambda: (
                self.manager.snapshot(first.job_id),
                self.manager.snapshot(second.job_id),
            )
            if self.manager.snapshot(first.job_id).status == "running"
            and self.manager.snapshot(second.job_id).status == "running"
            and self.manager.snapshot(first.job_id).process_id
            and self.manager.snapshot(second.job_id).process_id
            else None,
            timeout=20,
        )

        self.assertIsNotNone(running_pair)
        first_snapshot, second_snapshot = running_pair
        self.assertNotEqual(first_snapshot.process_id, second_snapshot.process_id)
        self.assertEqual(first_snapshot.execution_mode, QUERY_EXECUTION_DUCKDB_READ)
        self.assertEqual(second_snapshot.execution_mode, QUERY_EXECUTION_DUCKDB_READ)

        metrics_seen = wait_until(
            lambda: (
                self.manager.snapshot(first.job_id),
                self.manager.snapshot(second.job_id),
            )
            if self.manager.snapshot(first.job_id).memory_rss_bytes is not None
            and self.manager.snapshot(second.job_id).memory_rss_bytes is not None
            else None,
            timeout=10,
        )
        self.assertIsNotNone(metrics_seen)

        self.manager.cancel_job(first.job_id)
        self.manager.cancel_job(second.job_id)

        cancelled_pair = wait_until(
            lambda: (
                self.manager.snapshot(first.job_id),
                self.manager.snapshot(second.job_id),
            )
            if self.manager.snapshot(first.job_id).status == "cancelled"
            and self.manager.snapshot(second.job_id).status == "cancelled"
            else None,
            timeout=20,
        )

        self.assertIsNotNone(cancelled_pair)
        self.assertEqual(cancelled_pair[0].cancellation_phase, "cancelled")
        self.assertEqual(cancelled_pair[1].cancellation_phase, "cancelled")

    def test_cancellation_reports_progress_before_terminal_state(self) -> None:
        job = self.manager.start_job(
            sql=LONG_QUERY,
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            data_sources=[],
        )
        started = wait_until(
            lambda: self.manager.snapshot(job.job_id)
            if self.manager.snapshot(job.job_id).status == "running"
            and self.manager.snapshot(job.job_id).process_id
            else None,
            timeout=20,
        )
        self.assertIsNotNone(started)

        cancelling = self.manager.cancel_job(job.job_id)

        self.assertIn(cancelling.cancellation_phase, {"requested", "interrupting"})
        self.assertIn("Cancellation", cancelling.message or "")

        terminal = wait_until(
            lambda: self.manager.snapshot(job.job_id)
            if self.manager.snapshot(job.job_id).status == "cancelled"
            else None,
            timeout=20,
        )

        self.assertIsNotNone(terminal)
        self.assertEqual(terminal.cancellation_phase, "cancelled")
        self.assertIn("cancel", (terminal.message or "").lower())

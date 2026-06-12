from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend import query_jobs as query_jobs_module  # noqa: E402
from bit_data_workbench.backend.query_jobs import (  # noqa: E402
    _bootstrap_duckdb_source_views,
    _is_direct_file_relation,
    DUCKDB_EXECUTION_PATH_SHARED_FILE_READ,
    QUERY_EXECUTION_DUCKDB_READ,
    QUERY_EXECUTION_DUCKDB_WRITE,
    QUERY_EXECUTION_POSTGRES_NATIVE,
    QUERY_METRICS_SAMPLE_SECONDS,
    DuckDBQueryAccessCoordinator,
    QueryJobManager,
    QueryJobRecord,
    classify_query_execution,
    utc_now_iso,
)
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.models import QueryJobDefinition, QueryResourceSample  # noqa: E402
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
            "-- Call 1\nselect 1",
            "/* insert into t values (1) */\nselect 'create table t as select 1' as note",
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

    def test_identifies_workspace_s3_glob_relations_as_direct_file_relations(self) -> None:
        self.assertTrue(
            _is_direct_file_relation('workspace.s3."bucket"."path/to/file.parquet"')
        )
        self.assertTrue(
            _is_direct_file_relation('workspace.s3."bucket"."path/to/*.parquet"')
        )
        self.assertFalse(
            _is_direct_file_relation("workspace.s3.test")
        )
        self.assertTrue(
            _is_direct_file_relation('s3."bucket"."path/to/file.parquet"')
        )
        self.assertTrue(
            _is_direct_file_relation('s3."bucket"."path/to/*.parquet"')
        )
        self.assertFalse(
            _is_direct_file_relation("s3.test")
        )

    def test_source_view_bootstrap_skips_direct_s3_file_references(self) -> None:
        class FakeConnection:
            def __init__(self) -> None:
                self.executed_sql: list[str] = []

            def execute(self, sql: str):
                self.executed_sql.append(sql)
                return self

        connection = FakeConnection()

        _bootstrap_duckdb_source_views(
            connection,
            [
                {
                    "relation": (
                        's3."poc-tests-performance-evaluation-kostenbelege-3-1".'
                        '"generated/kostenbelege_3_1/parquet/kbpo_2019/*.parquet"'
                    ),
                    "query_sql": (
                        "(SELECT *, "
                        '"KBKP_AusgleichBelegnummer" AS "KBKP_Belegnummer" '
                        "FROM read_parquet('s3://bucket/key/*.parquet'))"
                    ),
                }
            ],
        )

        self.assertEqual(connection.executed_sql, [])


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
            query_options={"duckdb": {"parquetHivePartitioning": "on"}},
            process_id=1234,
            cpu_percent=12.5,
            average_cpu_percent=8.5,
            peak_cpu_percent=18.0,
            cpu_capacity_percent=6.25,
            average_cpu_capacity_percent=4.25,
            peak_cpu_capacity_percent=9.0,
            cpu_capacity_cores=2.0,
            memory_rss_bytes=1024,
            average_memory_rss_bytes=1536,
            peak_memory_rss_bytes=2048,
            process_thread_count=12,
            peak_process_thread_count=16,
            duckdb_thread_limit=8,
            duckdb_spill_bytes=4096,
            duckdb_spill_peak_bytes=8192,
            duckdb_spill_total_bytes=12288,
            duckdb_spill_other_bytes=4096,
            duckdb_spill_limit_bytes=96 * 1024**3,
            duckdb_spill_disk_free_bytes=10 * 1024**3,
            resource_samples=[
                QueryResourceSample(
                    elapsed_ms=2000,
                    cpu_percent=12.5,
                    average_cpu_percent=8.5,
                    cpu_capacity_percent=6.25,
                    average_cpu_capacity_percent=4.25,
                    memory_rss_bytes=1024,
                    average_memory_rss_bytes=1536,
                    process_thread_count=12,
                    duckdb_thread_limit=8,
                    duckdb_spill_bytes=4096,
                    duckdb_spill_total_bytes=12288,
                    duckdb_spill_other_bytes=4096,
                    duckdb_spill_limit_bytes=96 * 1024**3,
                    duckdb_spill_disk_free_bytes=10 * 1024**3,
                )
            ],
            warnings=["DuckDB reported a non-fatal warning."],
            error="DuckDB failed while scanning source parquet.",
            progress_events=[
                {
                    "event": "querying",
                    "phase": "Scanning S3 Parquet",
                    "message": "DuckDB is scanning KBPO.",
                    "occurredAt": "2026-05-13T00:00:01+00:00",
                    "durationMs": 1000,
                }
            ],
            cancellation_phase="interrupting",
            cancellation_requested_at="2026-05-13T00:00:02+00:00",
            worker_exit_code=-15,
            timings={
                "backendPrepareMs": 4.5,
                "engineAccessWaitMs": 0.0,
                "workerStartupMs": 12.0,
                "engineQueryMs": 20.0,
                "resultFetchMs": 3.0,
                "backendTotalMs": 41.0,
            },
        )

        payload = job.payload

        self.assertEqual(payload["executionMode"], QUERY_EXECUTION_DUCKDB_READ)
        self.assertEqual(payload["processId"], 1234)
        self.assertEqual(payload["cpuPercent"], 12.5)
        self.assertEqual(payload["averageCpuPercent"], 8.5)
        self.assertEqual(payload["peakCpuPercent"], 18.0)
        self.assertEqual(payload["cpuCapacityPercent"], 6.25)
        self.assertEqual(payload["averageCpuCapacityPercent"], 4.25)
        self.assertEqual(payload["peakCpuCapacityPercent"], 9.0)
        self.assertEqual(payload["cpuCapacityCores"], 2.0)
        self.assertEqual(payload["memoryRssBytes"], 1024)
        self.assertEqual(payload["averageMemoryRssBytes"], 1536)
        self.assertEqual(payload["peakMemoryRssBytes"], 2048)
        self.assertEqual(payload["processThreadCount"], 12)
        self.assertEqual(payload["peakProcessThreadCount"], 16)
        self.assertEqual(payload["duckdbThreadLimit"], 8)
        self.assertEqual(payload["duckdbSpillBytes"], 4096)
        self.assertEqual(payload["duckdbSpillPeakBytes"], 8192)
        self.assertEqual(payload["duckdbSpillTotalBytes"], 12288)
        self.assertEqual(payload["duckdbSpillOtherBytes"], 4096)
        self.assertEqual(payload["duckdbSpillLimitBytes"], 96 * 1024**3)
        self.assertEqual(payload["duckdbSpillDiskFreeBytes"], 10 * 1024**3)
        self.assertEqual(payload["resourceSamples"][0]["elapsedMs"], 2000)
        self.assertEqual(payload["resourceSamples"][0]["cpuCapacityPercent"], 6.25)
        self.assertEqual(payload["warnings"], ["DuckDB reported a non-fatal warning."])
        self.assertEqual(payload["resourceSamples"][0]["processThreadCount"], 12)
        self.assertEqual(payload["resourceSamples"][0]["duckdbThreadLimit"], 8)
        self.assertEqual(payload["resourceSamples"][0]["duckdbSpillBytes"], 4096)
        self.assertEqual(payload["resourceSamples"][0]["duckdbSpillTotalBytes"], 12288)
        self.assertEqual(payload["resourceSamples"][0]["duckdbSpillOtherBytes"], 4096)
        self.assertEqual(payload["error"], "DuckDB failed while scanning source parquet.")
        self.assertEqual(payload["progressEvents"][0]["event"], "querying")
        self.assertEqual(payload["progressEvents"][0]["phase"], "Scanning S3 Parquet")
        self.assertEqual(payload["cancellationPhase"], "interrupting")
        self.assertEqual(payload["workerExitCode"], -15)
        self.assertEqual(payload["timings"]["backendPrepareMs"], 4.5)
        self.assertEqual(payload["timings"]["workerStartupMs"], 12.0)
        self.assertEqual(
            payload["queryOptions"]["duckdb"]["parquetHivePartitioning"],
            "on",
        )
        self.assertEqual(payload["cacheHydration"], {})


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

    def test_read_queries_are_not_blocked_by_waiting_writers(self) -> None:
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
        read_waiting_reported = threading.Event()
        self.assertTrue(
            coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_READ,
                lambda: False,
                on_waiting=read_waiting_reported.set,
            )
        )
        self.assertFalse(read_waiting_reported.is_set())

        coordinator.release(QUERY_EXECUTION_DUCKDB_READ)
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
        self.settings.duckdb_threads = 8
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

    def test_worker_payload_warnings_are_normalized(self) -> None:
        snapshot = QueryJobDefinition(
            job_id="query-warning-normalization",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select 1",
            status="running",
            started_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        self.manager._patch_job(
            snapshot.job_id,
            warnings=[" first warning ", "", None, "second warning"],
        )

        updated = self.manager.snapshot(snapshot.job_id)
        self.assertEqual(updated.warnings, ["first warning", "second warning"])
        self.assertEqual(updated.payload["warnings"], ["first warning", "second warning"])

    def test_preflight_job_appears_in_state_and_can_fail_before_worker_start(self) -> None:
        snapshot = self.manager.start_preflight_job(
            sql="select * from missing.schema_table",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            requested_job_id="query-client-preflight-failure",
            data_sources=["workspace.s3"],
            query_options={"validation": {"sourceExistence": "on"}},
            client_pre_submit_ms=12.5,
        )

        self.assertEqual(snapshot.job_id, "query-client-preflight-failure")
        state = self.manager.state_payload()
        queued = state["jobs"][0]
        self.assertEqual(queued["jobId"], "query-client-preflight-failure")
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["progressLabel"], "Preparing query...")
        self.assertEqual(queued["timings"]["clientPreSubmitMs"], 12.5)

        failed = self.manager.fail_preflight_job(
            snapshot.job_id,
            error="Referenced source(s) were not found: missing.schema_table",
            message="Query preparation failed before DuckDB execution started.",
            backend_prepare_ms=7.25,
        )

        self.assertEqual(failed.status, "failed")
        self.assertIn("missing.schema_table", failed.error)
        self.assertEqual(failed.timings["backendPrepareMs"], 7.25)
        self.assertTrue(
            any(event.get("event") == "failed" for event in failed.progress_events)
        )

    def test_unexpected_worker_exit_reports_last_phase_progress_and_code(self) -> None:
        snapshot = QueryJobDefinition(
            job_id="query-worker-exit",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select 1",
            status="running",
            started_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            progress=0.37,
            progress_label="Scanning S3 Parquet",
            message="DuckDB is scanning KBPO.",
            cancellation_phase="interrupt_requested",
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        with patch.object(query_jobs_module.logger, "log"):
            self.manager._finalize_after_process_exit(snapshot.job_id, 7)

        failed = self.manager.snapshot(snapshot.job_id)
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.worker_exit_code, 7)
        self.assertIn("Query worker exited unexpectedly with code 7.", failed.error)
        self.assertIn("Last phase: Scanning S3 Parquet.", failed.error)
        self.assertIn("Last message: DuckDB is scanning KBPO.", failed.error)
        self.assertIn("Last DuckDB progress: 37.0%.", failed.error)
        self.assertIn("Cancellation phase: interrupt_requested.", failed.error)

    def _create_mwa_parquet_join_fixture(self) -> tuple[Path, Path]:
        entities_directory = (
            self.root
            / "generated"
            / "mwa_abrechnung"
            / "parquet"
            / "mwa_abrechnung_entities"
        )
        ziffern_directory = (
            self.root
            / "generated"
            / "mwa_abrechnung"
            / "parquet"
            / "mwa_abrechnungs_ziffern_entities"
        )
        entities_directory.mkdir(parents=True, exist_ok=True)
        ziffern_directory.mkdir(parents=True, exist_ok=True)

        entities_file = entities_directory / "part-00000.parquet"
        ziffern_file = ziffern_directory / "part-00000.parquet"

        with duckdb.connect(":memory:") as connection:
            connection.execute(
                """
                CREATE TABLE mwa_abrechnung_entities AS
                SELECT * FROM (
                    VALUES
                        (1, 'A', TIMESTAMP '2024-01-01 00:00:00'),
                        (2, 'B', TIMESTAMP '2024-01-02 00:00:00')
                ) AS t(id_, moe_id, einreiche_datum)
                """
            )
            connection.execute(
                f"COPY mwa_abrechnung_entities TO '{entities_file.as_posix()}' (FORMAT PARQUET)"
            )
            connection.execute(
                """
                CREATE TABLE mwa_abrechnungs_ziffern_entities AS
                SELECT * FROM (
                    VALUES
                        (1, 'z1', 1),
                        (1, 'z2', 1),
                        (2, 'z3', 2)
                ) AS t(id_, ziffer_nummer, abrechnung_refer)
                """
            )
            connection.execute(
                f"COPY mwa_abrechnungs_ziffern_entities TO '{ziffern_file.as_posix()}' (FORMAT PARQUET)"
            )

        return entities_directory, ziffern_directory

    def _create_stage_parquet_fixture(self) -> Path:
        stage_file = self.root / "materialized-stage" / "data.parquet"
        stage_file.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(":memory:") as connection:
            connection.execute(
                """
                CREATE TABLE stage_output AS
                SELECT * FROM (
                    VALUES
                        (1, 'A'),
                        (2, 'B')
                ) AS t(id_, code)
                """
            )
            connection.execute(
                f"COPY stage_output TO '{stage_file.as_posix()}' (FORMAT PARQUET)"
            )
        return stage_file

    def test_process_metrics_sample_once_per_second(self) -> None:
        self.assertEqual(QUERY_METRICS_SAMPLE_SECONDS, 1.0)

    def test_process_metrics_samples_current_average_peak_and_throttles(self) -> None:
        class FakeProcessMetrics:
            def __init__(self) -> None:
                self.samples = iter([(10.0, 1000, 6), (30.0, 3000, 9), (70.0, 7000, 12)])
                self.current = (0.0, 0, 0)

            def cpu_percent(self, interval=None) -> float:
                self.current = next(self.samples)
                return self.current[0]

            def memory_info(self):
                return SimpleNamespace(rss=self.current[1])

            def num_threads(self) -> int:
                return self.current[2]

            def children(self, recursive=True):
                return []

        snapshot = QueryJobDefinition(
            job_id="query-metrics-test",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select 1",
            status="running",
            started_at="2026-05-13T00:00:00+00:00",
            updated_at="2026-05-13T00:00:00+00:00",
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            process_id=4321,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
            process_metrics=FakeProcessMetrics(),
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        self.manager._sample_process_metrics(snapshot.job_id, force=True)
        self.manager._sample_process_metrics(snapshot.job_id)
        self.manager._sample_process_metrics(snapshot.job_id, force=True)

        sampled = self.manager.snapshot(snapshot.job_id)
        self.assertEqual(sampled.cpu_percent, 30.0)
        self.assertEqual(sampled.average_cpu_percent, 20.0)
        self.assertEqual(sampled.peak_cpu_percent, 30.0)
        self.assertEqual(sampled.memory_rss_bytes, 3000)
        self.assertEqual(sampled.average_memory_rss_bytes, 2000)
        self.assertEqual(sampled.peak_memory_rss_bytes, 3000)
        self.assertEqual(sampled.process_thread_count, 9)
        self.assertEqual(sampled.peak_process_thread_count, 9)
        self.assertEqual(sampled.duckdb_thread_limit, 8)
        self.assertEqual(len(sampled.resource_samples), 2)
        self.assertEqual(sampled.resource_samples[-1].process_thread_count, 9)
        self.assertEqual(sampled.resource_samples[-1].duckdb_thread_limit, 8)
        with self.manager._condition:
            state_payload = self.manager._state_payload_locked()
        self.assertEqual(state_payload["summary"]["runningProcessCount"], 1)

    def test_process_metrics_samples_duckdb_spill_usage(self) -> None:
        class FakeProcessMetrics:
            def cpu_percent(self, interval=None) -> float:
                return 10.0

            def memory_info(self):
                return SimpleNamespace(rss=1000)

            def children(self, recursive=True):
                return []

        spill_root = self.root / "duckdb-spill"
        query_spill = spill_root / "query-spill-test"
        other_spill = spill_root / "stale"
        query_spill.mkdir(parents=True)
        other_spill.mkdir()
        (query_spill / "block.tmp").write_bytes(b"a" * 11)
        (other_spill / "block.tmp").write_bytes(b"b" * 7)
        self.settings.duckdb_temp_directory = spill_root
        self.settings.duckdb_max_temp_directory_size = "96GiB"
        snapshot = QueryJobDefinition(
            job_id="query-spill-test",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select 1",
            status="running",
            started_at="2026-05-13T00:00:00+00:00",
            updated_at="2026-05-13T00:00:00+00:00",
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
            process_metrics=FakeProcessMetrics(),
            spill_temp_directory=query_spill,
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        self.manager._sample_process_metrics(snapshot.job_id, force=True)

        sampled = self.manager.snapshot(snapshot.job_id)
        self.assertEqual(sampled.duckdb_spill_bytes, 11)
        self.assertEqual(sampled.duckdb_spill_total_bytes, 18)
        self.assertEqual(sampled.duckdb_spill_other_bytes, 7)
        self.assertEqual(sampled.duckdb_spill_limit_bytes, 96 * 1024**3)
        self.assertEqual(sampled.duckdb_spill_peak_bytes, 11)
        self.assertEqual(sampled.resource_samples[-1].duckdb_spill_bytes, 11)

    def test_process_metrics_include_capacity_normalized_cpu(self) -> None:
        class FakeProcessMetrics:
            def __init__(self) -> None:
                self.samples = iter([(200.0, 1000), (400.0, 3000)])
                self.current = (0.0, 0)

            def cpu_percent(self, interval=None) -> float:
                self.current = next(self.samples)
                return self.current[0]

            def memory_info(self):
                return SimpleNamespace(rss=self.current[1])

            def children(self, recursive=True):
                return []

        self.manager._cpu_capacity_cores = 4.0
        snapshot = QueryJobDefinition(
            job_id="query-cpu-capacity-test",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select 1",
            status="running",
            started_at="2026-05-13T00:00:00+00:00",
            updated_at="2026-05-13T00:00:00+00:00",
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            cpu_capacity_cores=4.0,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
            process_metrics=FakeProcessMetrics(),
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        self.manager._sample_process_metrics(snapshot.job_id, force=True)
        self.manager._sample_process_metrics(snapshot.job_id, force=True)

        sampled = self.manager.snapshot(snapshot.job_id)
        self.assertEqual(sampled.cpu_percent, 400.0)
        self.assertEqual(sampled.cpu_capacity_percent, 100.0)
        self.assertEqual(sampled.average_cpu_capacity_percent, 75.0)
        self.assertEqual(sampled.peak_cpu_capacity_percent, 100.0)
        self.assertEqual(sampled.cpu_capacity_cores, 4.0)
        self.assertEqual(sampled.resource_samples[-1].cpu_capacity_percent, 100.0)

    def test_query_job_logs_lifecycle_metadata_and_duckdb_profile_summary(self) -> None:
        with self.assertLogs("bit_data_workbench.backend.query_jobs", level="INFO") as captured:
            job = self.manager.start_job(
                sql="select 8675309 as sensitive_value",
                notebook_id="notebook-y",
                notebook_title="Notebook Y",
                cell_id="cell-x",
                data_sources=["workspace.s3"],
                touched_relations=["test.sample_csv"],
                touched_buckets=["test"],
                client_pre_submit_ms=6.25,
                backend_prepare_ms=3.5,
                source_summaries=[
                    {
                        "relation": "test.sample_csv",
                        "query_alias": "s3.test.sample.csv",
                        "bucket": "test",
                        "key": "sample.csv",
                        "path": "s3://test/sample.csv",
                        "format": "csv",
                    }
                ],
            )
            completed = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).status == "completed"
                else None,
                timeout=20,
            )

        self.assertIsNotNone(completed)
        output = "\n".join(captured.output)
        for event in (
            "queued",
            "backend_prepared",
            "prepared",
            "engine_allocated",
            "worker_starting",
            "worker_started",
            "querying",
            "fetching_rows",
            "completed",
        ):
            self.assertIn(f'query_job_event="{event}"', output)
        self.assertRegex(
            output,
            r'query_job_time="\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (?:CET|CEST|UTC)"',
        )
        self.assertRegex(
            output,
            r"INFO:bit_data_workbench\.backend\.query_jobs:"
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (?:CET|CEST|UTC) "
            r"\[bdw-query\] query_job_time=",
        )
        self.assertIn('notebook_id="notebook-y"', output)
        self.assertIn('notebook_title="Notebook Y"', output)
        self.assertIn('cell_id="cell-x"', output)
        self.assertIn('"query_alias":"s3.test.sample.csv"', output)
        self.assertIn("duckdb_latency_ms=", output)
        self.assertIn("duckdb_rows_returned=", output)
        self.assertIn("duckdb_operator_count=", output)
        self.assertIn("client_pre_submit_ms=6.25", output)
        self.assertIn("backend_prepare_ms=3.5", output)
        self.assertIn("engine_access_wait_ms=", output)
        self.assertIn("worker_startup_ms=", output)
        self.assertIn("engine_query_ms=", output)
        self.assertIn("result_fetch_ms=", output)
        self.assertIn("backend_total_ms=", output)
        self.assertNotIn("select 8675309", output)
        self.assertNotIn("sensitive_value", output)
        self.assertNotIn("children", output)
        self.assertNotIn("query_name", output)
        self.assertTrue(completed.progress_events)
        stored_events = [event["event"] for event in completed.progress_events]
        self.assertIn("queued", stored_events)
        self.assertIn("completed", stored_events)
        terminal_events = [
            event for event in completed.progress_events if event.get("event") == "completed"
        ]
        self.assertTrue(terminal_events)
        self.assertIn("duckdbProfile", terminal_events[-1])
        self.assertIn("duckdb_operator_count", terminal_events[-1]["duckdbProfile"])
        self.assertGreaterEqual(completed.timings.get("backendPrepareMs", -1), 0)
        self.assertGreaterEqual(completed.timings.get("engineAccessWaitMs", -1), 0)
        self.assertGreaterEqual(completed.timings.get("workerStartupMs", -1), 0)
        self.assertGreaterEqual(completed.timings.get("engineQueryMs", -1), 0)
        self.assertGreaterEqual(completed.timings.get("resultFetchMs", -1), 0)
        self.assertGreaterEqual(completed.timings.get("backendTotalMs", -1), 0)
        self.assertNotIn("select 8675309", json.dumps(completed.progress_events))

    def test_query_job_heartbeat_is_throttled_and_stops_after_cancellation(self) -> None:
        snapshot = QueryJobDefinition(
            job_id="query-heartbeat",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select secret_value from private_table",
            status="running",
            started_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            progress=0.42,
            progress_label="Running... 42%",
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            process_id=1234,
            cpu_percent=12.345,
            memory_rss_bytes=64 * 1024 * 1024,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
            last_log_heartbeat_monotonic=time.monotonic(),
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        with patch.object(query_jobs_module.logger, "info") as log_info:
            self.manager._log_query_heartbeat_if_due(snapshot.job_id)
            log_info.assert_not_called()

            with self.manager._condition:
                record.last_log_heartbeat_monotonic -= 11
            self.manager._log_query_heartbeat_if_due(snapshot.job_id)
            self.assertEqual(log_info.call_count, 1)
            heartbeat_line = str(log_info.call_args.args[1])
            self.assertRegex(
                heartbeat_line,
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (?:CET|CEST|UTC) "
                r"\[bdw-query\] ",
            )
            self.assertIn('query_job_event="progress"', heartbeat_line)
            self.assertIn('progress_kind="heartbeat"', heartbeat_line)
            self.assertIn("duckdb_progress_percent=42.0", heartbeat_line)
            self.assertIn("duckdb_coordinator_active_reads=", heartbeat_line)
            self.assertIn("duckdb_coordinator_active_write=", heartbeat_line)
            self.assertIn("duckdb_coordinator_waiting_writes=", heartbeat_line)
            self.assertIn("cpu_percent=12.35", heartbeat_line)
            self.assertIn("ram_mb=64.0", heartbeat_line)
            self.assertNotIn("private_table", heartbeat_line)
            heartbeat_events = [
                event for event in snapshot.progress_events if event.get("event") == "progress"
            ]
            self.assertEqual(len(heartbeat_events), 1)
            self.assertEqual(heartbeat_events[0]["progress_kind"], "heartbeat")
            self.assertEqual(heartbeat_events[0]["duckdb_progress_percent"], 42.0)
            self.assertIn("displayTime", heartbeat_events[0])
            self.assertNotIn("private_table", json.dumps(heartbeat_events))

            with self.manager._condition:
                record.last_log_heartbeat_monotonic -= 11
                record.cancel_requested = True
            self.manager._log_query_heartbeat_if_due(snapshot.job_id)
            self.assertEqual(log_info.call_count, 1)

    def test_repeated_progress_events_are_compacted_with_first_and_last_occurrence(self) -> None:
        snapshot = QueryJobDefinition(
            job_id="query-repeated-progress",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select 1",
            status="running",
            started_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            progress_label="Running...",
            message="DuckDB is planning and executing the statement.",
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            cpu_percent=10.0,
            cpu_capacity_percent=5.0,
            cpu_capacity_cores=2.0,
            memory_rss_bytes=64 * 1024 * 1024,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
            last_log_heartbeat_monotonic=time.monotonic() - 11,
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        with patch.object(query_jobs_module.logger, "info"):
            self.manager._log_query_heartbeat_if_due(snapshot.job_id)
            with self.manager._condition:
                record.last_log_heartbeat_monotonic -= 11
                snapshot.cpu_percent = 20.0
                snapshot.cpu_capacity_percent = 10.0
            self.manager._log_query_heartbeat_if_due(snapshot.job_id)

        progress_events = [
            event for event in snapshot.progress_events if event.get("event") == "progress"
        ]
        self.assertEqual(len(progress_events), 1)
        self.assertEqual(progress_events[0]["occurrenceCount"], 2)
        self.assertIn("firstOccurredAt", progress_events[0])
        self.assertIn("lastOccurredAt", progress_events[0])
        self.assertEqual(progress_events[0]["cpu_percent"], 20.0)
        self.assertEqual(progress_events[0]["cpu_capacity_percent"], 10.0)

    def test_progress_event_retention_preserves_earliest_event(self) -> None:
        events = [
            {
                "occurredAt": f"2026-05-13T00:00:{index:02d}+00:00",
                "displayTime": f"event {index}",
                "event": "progress",
                "status": "running",
                "message": f"event {index}",
            }
            for index in range(10)
        ]

        compacted = query_jobs_module._progress_events_with_preserved_edges(events, 5)

        self.assertEqual(len(compacted), 5)
        self.assertEqual(compacted[0]["message"], "event 0")
        summary_event = next(
            event for event in compacted if event["event"] == "progress_events_compacted"
        )
        self.assertIn("middle progress event(s)", summary_event["message"])
        self.assertEqual(compacted[-1]["message"], "event 9")

    def test_prepared_query_log_caps_s3_source_summaries(self) -> None:
        snapshot = QueryJobDefinition(
            job_id="query-source-cap",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select 1",
            status="queued",
            started_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
            source_summaries=[
                {
                    "relation": f"test.source_{index}",
                    "query_alias": f"s3.test.source_{index}.csv",
                    "bucket": "test",
                    "key": f"source-{index}.csv",
                    "format": "csv",
                }
                for index in range(7)
            ],
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        with patch.object(query_jobs_module.logger, "log") as log_call:
            self.manager._log_query_job_event(snapshot.job_id, "prepared")

        prepared_line = str(log_call.call_args.args[2])
        self.assertIn("source_overflow_count=2", prepared_line)
        self.assertIn("source_0", prepared_line)
        self.assertIn("source_4", prepared_line)
        self.assertNotIn("source_5", prepared_line)

    def test_queued_query_cancellation_logs_request_and_cancelled_state(self) -> None:
        snapshot = QueryJobDefinition(
            job_id="query-cancel-log",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-1",
            sql="select 1",
            status="queued",
            started_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            can_cancel=True,
        )
        record = QueryJobRecord(
            snapshot=snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_READ,
            execution_sql="select 1",
        )
        with self.manager._condition:
            self.manager._jobs[snapshot.job_id] = record

        with self.assertLogs("bit_data_workbench.backend.query_jobs", level="INFO") as captured:
            cancelled = self.manager.cancel_job(snapshot.job_id)

        self.assertEqual(cancelled.status, "cancelled")
        output = "\n".join(captured.output)
        self.assertIn('query_job_event="cancel_requested"', output)
        self.assertIn('query_job_event="cancelled"', output)

    def test_failed_query_logs_warning_without_sql_text(self) -> None:
        with self.assertLogs("bit_data_workbench.backend.query_jobs", level="WARNING") as captured:
            job = self.manager.start_job(
                sql="select * from very_sensitive_missing_relation",
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-1",
                data_sources=[],
            )
            failed = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).status == "failed"
                else None,
                timeout=20,
            )

        self.assertIsNotNone(failed)
        output = "\n".join(captured.output)
        self.assertIn("WARNING", output)
        self.assertIn('query_job_event="failed"', output)
        self.assertIn("error=", output)
        self.assertNotIn("select * from", output.lower())
        failed_events = [event for event in failed.progress_events if event.get("event") == "failed"]
        self.assertTrue(failed_events)
        self.assertIn("error", failed_events[-1])
        self.assertNotIn("select * from", json.dumps(failed_events))

    def test_missing_duckdb_profile_summary_is_silent(self) -> None:
        missing_path = self.root / "missing-profile.json"

        self.assertEqual(query_jobs_module._read_duckdb_profile_summary(missing_path), {})

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
        self.assertEqual(completed.timings.get("engineAccessWaitMs"), 0.0)
        for key in ("workerStartupMs", "engineQueryMs", "resultFetchMs", "backendTotalMs"):
            self.assertGreaterEqual(completed.timings.get(key, -1), 0)

    def test_file_backed_read_reports_duckdb_access_wait(self) -> None:
        self.assertTrue(
            self.manager._access_coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_WRITE,
                lambda: False,
                owner_job_id="writer-job",
            )
        )
        try:
            job = self.manager.start_job(
                sql="select 1 as value",
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-wait",
                data_sources=["workspace.s3"],
                touched_relations=["s3.test.sample_data"],
                touched_buckets=["test"],
            )
            waiting = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).progress_label == "Waiting for DuckDB access..."
                else None,
                timeout=5,
            )
            self.assertIsNotNone(waiting)
            time.sleep(0.2)
        finally:
            self.manager._access_coordinator.release(
                QUERY_EXECUTION_DUCKDB_WRITE,
                owner_job_id="writer-job",
            )

        completed = wait_until(
            lambda: self.manager.snapshot(job.job_id)
            if self.manager.snapshot(job.job_id).status == "completed"
            else None,
            timeout=20,
        )

        self.assertIsNotNone(completed)
        self.assertGreater(completed.timings.get("engineAccessWaitMs", 0), 0)
        events = completed.progress_events
        self.assertIn("engine_waiting", [event.get("event") for event in events])
        waiting_events = [event for event in events if event.get("event") == "engine_waiting"]
        self.assertTrue(waiting_events)
        self.assertIn("duckdb_coordinator_active_write", waiting_events[-1])
        self.assertEqual(waiting_events[-1]["duckdb_lock_owner_job_id"], "writer-job")
        self.assertEqual(completed.duckdb_execution_path, "shared-file-read")

    def test_materialized_stage_s3_parquet_summary_isolated_read_skips_duckdb_file_lock(self) -> None:
        stage_file = self._create_stage_parquet_fixture()
        query_sql = f"read_parquet('{stage_file.as_posix()}')"
        query = f"SELECT * FROM {query_sql}"

        self.assertTrue(
            self.manager._access_coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_WRITE,
                lambda: False,
                owner_job_id="writer-job",
            )
        )
        try:
            job = self.manager.start_job(
                sql="SELECT * FROM stage.mwa_joined_abrechnungen",
                execution_sql=query,
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-stage-read",
                data_sources=["workspace.s3"],
                touched_relations=["stage.mwa_joined_abrechnungen"],
                touched_buckets=["vat-smoke-test"],
                source_summaries=[
                    {
                        "relation": "stage.mwa_joined_abrechnungen",
                        "bucket": "vat-smoke-test",
                        "path": "s3://vat-smoke-test/_bdw_stages/notebook/stage/data.parquet",
                        "format": "parquet",
                        "query_sql": query_sql,
                    }
                ],
            )
            completed = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).status == "completed"
                else None,
                timeout=20,
            )
        finally:
            self.manager._access_coordinator.release(
                QUERY_EXECUTION_DUCKDB_WRITE,
                owner_job_id="writer-job",
            )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.duckdb_execution_path, "isolated-read")
        self.assertEqual(completed.timings.get("engineAccessWaitMs"), 0.0)
        self.assertNotIn("engine_waiting", [event.get("event") for event in completed.progress_events])
        self.assertEqual(completed.rows, [(1, "A"), (2, "B")])

    def test_mwa_s3_glob_join_isolated_read_skips_duckdb_file_lock(self) -> None:
        entities_directory, ziffern_directory = self._create_mwa_parquet_join_fixture()
        entities_pattern = entities_directory / "*.parquet"
        ziffern_pattern = ziffern_directory / "*.parquet"
        query = (
            "SELECT ENTI.*, ZIFF.* "
            f"FROM read_parquet('{entities_pattern.as_posix()}') AS ENTI "
            f"JOIN read_parquet('{ziffern_pattern.as_posix()}') AS ZIFF "
            "ON ZIFF.abrechnung_refer = ENTI.id_"
        )
        touched_relations = [
            's3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"',
            's3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet"',
        ]

        self.assertTrue(
            self.manager._access_coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_WRITE,
                lambda: False,
                owner_job_id="writer-job",
            )
        )
        try:
            job = self.manager.start_job(
                sql=query,
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-mwa-glob",
                data_sources=["workspace.s3"],
                touched_relations=touched_relations,
                touched_buckets=["poc-tests-performance-evaluation-mwa-abrechnung-3-2"],
            )
            completed = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).status == "completed"
                else None,
                timeout=20,
            )
        finally:
            self.manager._access_coordinator.release(
                QUERY_EXECUTION_DUCKDB_WRITE,
                owner_job_id="writer-job",
            )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.duckdb_execution_path, "isolated-read")
        self.assertEqual(completed.timings.get("engineAccessWaitMs"), 0.0)
        self.assertNotIn("engine_waiting", [event.get("event") for event in completed.progress_events])
        self.assertEqual(len(completed.rows), 3)

    def test_workspace_prefixed_mwa_s3_glob_join_isolated_read_skips_duckdb_file_lock(self) -> None:
        entities_directory, ziffern_directory = self._create_mwa_parquet_join_fixture()
        entities_pattern = entities_directory / "*.parquet"
        ziffern_pattern = ziffern_directory / "*.parquet"
        query = (
            "SELECT ENTI.*, ZIFF.* "
            f"FROM read_parquet('{entities_pattern.as_posix()}') AS ENTI "
            f"JOIN read_parquet('{ziffern_pattern.as_posix()}') AS ZIFF "
            "ON ZIFF.abrechnung_refer = ENTI.id_"
        )
        touched_relations = [
            'workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"',
            'workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/*.parquet"',
        ]

        self.assertTrue(
            self.manager._access_coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_WRITE,
                lambda: False,
                owner_job_id="writer-job",
            )
        )
        try:
            job = self.manager.start_job(
                sql=query,
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-mwa-glob-workspace",
                data_sources=["workspace.s3"],
                touched_relations=touched_relations,
                touched_buckets=["poc-tests-performance-evaluation-mwa-abrechnung-3-2"],
            )
            completed = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).status == "completed"
                else None,
                timeout=20,
            )
        finally:
            self.manager._access_coordinator.release(
                QUERY_EXECUTION_DUCKDB_WRITE,
                owner_job_id="writer-job",
            )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.duckdb_execution_path, "isolated-read")
        self.assertEqual(completed.timings.get("engineAccessWaitMs"), 0.0)
        self.assertNotIn(
            "engine_waiting",
            [event.get("event") for event in completed.progress_events],
        )
        self.assertEqual(len(completed.rows), 3)

    def test_workspace_prefixed_mwa_s3_single_file_join_isolated_read_skips_duckdb_file_lock(self) -> None:
        entities_directory, ziffern_directory = self._create_mwa_parquet_join_fixture()
        entities_file = entities_directory / "part-00000.parquet"
        ziffern_file = ziffern_directory / "part-00000.parquet"
        query = (
            "SELECT ENTI.*, ZIFF.* "
            f"FROM read_parquet('{entities_file.as_posix()}') AS ENTI "
            f"JOIN read_parquet('{ziffern_file.as_posix()}') AS ZIFF "
            "ON ZIFF.abrechnung_refer = ENTI.id_"
        )
        touched_relations = [
            'workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
            '."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/part-00000.parquet"',
            'workspace.s3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"'
            '."generated/mwa_abrechnung/parquet/mwa_abrechnungs_ziffern_entities/part-00000.parquet"',
        ]

        self.assertTrue(
            self.manager._access_coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_WRITE,
                lambda: False,
                owner_job_id="writer-job",
            )
        )
        try:
            job = self.manager.start_job(
                sql=query,
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-mwa-single",
                data_sources=["workspace.s3"],
                touched_relations=touched_relations,
                touched_buckets=["poc-tests-performance-evaluation-mwa-abrechnung-3-2"],
            )
            completed = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).status == "completed"
                else None,
                timeout=20,
            )
        finally:
            self.manager._access_coordinator.release(
                QUERY_EXECUTION_DUCKDB_WRITE,
                owner_job_id="writer-job",
            )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.duckdb_execution_path, "isolated-read")
        self.assertEqual(completed.timings.get("engineAccessWaitMs"), 0.0)
        self.assertNotIn(
            "engine_waiting",
            [event.get("event") for event in completed.progress_events],
        )
        self.assertEqual(len(completed.rows), 3)

    def test_plain_s3_glob_join_isolated_read_skips_duckdb_file_lock(self) -> None:
        entities_directory, ziffern_directory = self._create_mwa_parquet_join_fixture()
        entities_pattern = entities_directory / "*.parquet"
        ziffern_pattern = ziffern_directory / "*.parquet"
        query = (
            "SELECT ENTI.*, ZIFF.* "
            f"FROM read_parquet('{entities_pattern.as_posix()}') AS ENTI "
            f"JOIN read_parquet('{ziffern_pattern.as_posix()}') AS ZIFF "
            "ON ZIFF.abrechnung_refer = ENTI.id_"
        )
        touched_relations = [
            's3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"."generated/mwa_abrechnung/parquet/mwa_abrechnung_entities/*.parquet"',
            's3."poc-tests-performance-evaluation-mwa-abrechnung-3-2"."generated/mwa_abrechnungs_ziffern_entities/*.parquet"',
        ]

        self.assertTrue(
            self.manager._access_coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_WRITE,
                lambda: False,
                owner_job_id="writer-job",
            )
        )
        try:
            job = self.manager.start_job(
                sql=query,
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-mwa-glob-plain",
                data_sources=["workspace.s3"],
                touched_relations=touched_relations,
                touched_buckets=["poc-tests-performance-evaluation-mwa-abrechnung-3-2"],
            )
            completed = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).status == "completed"
                else None,
                timeout=20,
            )
        finally:
            self.manager._access_coordinator.release(
                QUERY_EXECUTION_DUCKDB_WRITE,
                owner_job_id="writer-job",
            )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.duckdb_execution_path, "isolated-read")
        self.assertEqual(completed.timings.get("engineAccessWaitMs"), 0.0)
        self.assertNotIn(
            "engine_waiting",
            [event.get("event") for event in completed.progress_events],
        )
        self.assertEqual(len(completed.rows), 3)

    def test_isolated_s3_csv_and_parquet_reads_skip_duckdb_file_lock(self) -> None:
        parquet_path = self.root / "sample.parquet"
        csv_path = self.root / "sample.csv"
        with duckdb.connect(":memory:") as connection:
            connection.execute(
                f"COPY (SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, value)) "
                f"TO '{parquet_path.as_posix()}' (FORMAT PARQUET)"
            )
        csv_path.write_text("id,value\n1,a\n2,b\n", encoding="utf-8")

        self.assertTrue(
            self.manager._access_coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_WRITE,
                lambda: False,
                owner_job_id="writer-job",
            )
        )
        try:
            job = self.manager.start_job(
                sql=(
                    "select "
                    "(select count(*) from test.sample_parquet) as parquet_rows, "
                    "(select count(*) from test.sample_csv) as csv_rows"
                ),
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-isolated",
                data_sources=["workspace.s3"],
                touched_relations=["test.sample_parquet", "test.sample_csv"],
                touched_buckets=["test"],
                source_summaries=[
                    {
                        "relation": "test.sample_parquet",
                        "query_alias": "s3.test.sample.parquet",
                        "bucket": "test",
                        "key": "sample.parquet",
                        "path": f"s3://test/{parquet_path.name}",
                        "format": "parquet",
                        "query_sql": f"SELECT * FROM read_parquet('{parquet_path.as_posix()}')",
                    },
                    {
                        "relation": "test.sample_csv",
                        "query_alias": "s3.test.sample.csv",
                        "bucket": "test",
                        "key": "sample.csv",
                        "path": f"s3://test/{csv_path.name}",
                        "format": "csv",
                        "query_sql": f"SELECT * FROM read_csv_auto('{csv_path.as_posix()}', HEADER = TRUE)",
                    },
                ],
            )
            completed = wait_until(
                lambda: self.manager.snapshot(job.job_id)
                if self.manager.snapshot(job.job_id).status == "completed"
                else None,
                timeout=20,
            )
        finally:
            self.manager._access_coordinator.release(
                QUERY_EXECUTION_DUCKDB_WRITE,
                owner_job_id="writer-job",
            )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.rows, [(2, 2)])
        self.assertEqual(completed.duckdb_execution_path, "isolated-read")
        self.assertEqual(completed.timings.get("engineAccessWaitMs"), 0.0)
        self.assertGreaterEqual(completed.timings.get("sourceBootstrapMs", -1), 0)
        self.assertNotIn("engine_waiting", [event.get("event") for event in completed.progress_events])

    def test_shared_file_read_does_not_bootstrap_sources_in_read_only_workspace(self) -> None:
        connection = duckdb.connect(str(self.settings.duckdb_database))
        try:
            connection.execute("CREATE TABLE existing_relation AS SELECT 1 AS value")
        finally:
            connection.close()

        job = self.manager.start_job(
            sql="SELECT value FROM existing_relation",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-shared-file-read-bootstrap",
            data_sources=["workspace.s3"],
            touched_relations=["existing_relation", "s3.unused_source"],
            touched_buckets=["test"],
            source_summaries=[
                {
                    "relation": "s3.unused_source",
                    "query_alias": "s3.test.unused.parquet",
                    "bucket": "test",
                    "key": "unused.parquet",
                    "path": "s3://test/unused.parquet",
                    "format": "parquet",
                    "query_sql": "SELECT 1 AS value",
                },
            ],
        )

        completed = wait_until(
            lambda: self.manager.snapshot(job.job_id)
            if self.manager.snapshot(job.job_id).status in {"completed", "failed"}
            else None,
            timeout=20,
        )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.status, "completed", completed.error)
        self.assertEqual(completed.rows, [(1,)])
        self.assertEqual(completed.duckdb_execution_path, DUCKDB_EXECUTION_PATH_SHARED_FILE_READ)
        self.assertNotIn("sourceBootstrapMs", completed.timings)

    def test_stale_duckdb_lock_owner_is_recovered(self) -> None:
        stale_snapshot = QueryJobDefinition(
            job_id="stale-writer",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="stale",
            sql="create table stale as select 1",
            status="completed",
            started_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            completed_at=utc_now_iso(),
            execution_mode=QUERY_EXECUTION_DUCKDB_WRITE,
            duckdb_execution_path="shared-file-write",
        )
        stale_record = QueryJobRecord(
            snapshot=stale_snapshot,
            sort_index=1,
            execution_mode=QUERY_EXECUTION_DUCKDB_WRITE,
            execution_sql="create table stale as select 1",
        )
        with self.manager._condition:
            self.manager._jobs[stale_snapshot.job_id] = stale_record
        self.assertTrue(
            self.manager._access_coordinator.acquire(
                QUERY_EXECUTION_DUCKDB_WRITE,
                lambda: False,
                owner_job_id=stale_snapshot.job_id,
            )
        )

        job = self.manager.start_job(
            sql="select 1 as value",
            notebook_id="nb",
            notebook_title="Notebook",
            cell_id="cell-recovery",
            data_sources=["workspace.s3"],
            touched_relations=["legacy.sample"],
            touched_buckets=["legacy"],
        )
        completed = wait_until(
            lambda: self.manager.snapshot(job.job_id)
            if self.manager.snapshot(job.job_id).status == "completed"
            else None,
            timeout=20,
        )

        self.assertIsNotNone(completed)
        self.assertEqual(completed.rows, [(1,)])
        self.assertFalse(self.manager._access_coordinator.state()["active_write"])
        self.assertIn("duckdb_lock_recovered", [event.get("event") for event in completed.progress_events])

    def test_client_timing_ack_updates_terminal_payload_and_history_callback(self) -> None:
        terminal_payloads: list[dict[str, object]] = []
        manager = QueryJobManager(
            settings=self.settings,
            max_result_rows=self.settings.max_result_rows,
            notebook_title_resolver=lambda _notebook_id: "Notebook",
            metadata_refresher=lambda: None,
            terminal_job_callback=terminal_payloads.append,
        )
        try:
            job = manager.start_job(
                sql="select 1 as value",
                notebook_id="nb",
                notebook_title="Notebook",
                cell_id="cell-client",
                data_sources=[],
                client_pre_submit_ms=7.0,
                backend_prepare_ms=2.0,
            )
            completed = wait_until(
                lambda: manager.snapshot(job.job_id)
                if manager.snapshot(job.job_id).status == "completed"
                else None,
                timeout=20,
            )
            self.assertIsNotNone(completed)

            updated = manager.record_client_timing(job.job_id, client_total_ms=123.456)

            self.assertEqual(updated.timings["clientTotalMs"], 123.456)
            self.assertTrue(terminal_payloads)
            self.assertEqual(terminal_payloads[-1]["timings"]["clientTotalMs"], 123.456)
        finally:
            manager.shutdown()

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
        self.assertRegex(cancelling.message or "", r"Cancellation|Interrupting")

        terminal = wait_until(
            lambda: self.manager.snapshot(job.job_id)
            if self.manager.snapshot(job.job_id).status == "cancelled"
            else None,
            timeout=20,
        )

        self.assertIsNotNone(terminal)
        self.assertEqual(terminal.cancellation_phase, "cancelled")
        self.assertIn("cancel", (terminal.message or "").lower())

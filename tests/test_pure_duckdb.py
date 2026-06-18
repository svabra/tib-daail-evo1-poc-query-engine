from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import duckdb
from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.pure_duckdb import (  # noqa: E402
    FACT_BUPO_TARGET,
    KBHP_FULL_PATH,
    KBKP_FULL_PATH,
    KBKP_TODAY_TARGET,
    KALENDER_PATH,
    KBPO_PATHS,
    PURE_DUCKDB_CELLS,
    _query_1_sql,
    _query_2_sql,
    pure_duckdb_cells_payload,
)
from bit_data_workbench.backend.pure_duckdb_jobs import (  # noqa: E402
    PURE_DUCKDB_DIRECT_EXECUTION_PATH,
    PureDuckDBJobManager,
)
from bit_data_workbench.config import Settings  # noqa: E402
from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402
from bit_data_workbench.web.router import pure_duckdb_page  # noqa: E402
from bit_data_workbench.version_info import current_repo_version  # noqa: E402


CURRENT_VERSION = current_repo_version(REPO_ROOT)


def build_request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "scheme": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"host", b"testserver")],
            "server": ("testserver", 80),
        }
    )


def make_settings(root: Path) -> Settings:
    return Settings(
        service_name="bit-data-workbench",
        ui_title="DAAIF Workbench",
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


class FakeWorkbenchService:
    settings = None

    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": CURRENT_VERSION,
            "hostname": "test-host",
            "pod_name": "unknown",
            "pod_namespace": "unknown",
            "pod_ip": "unknown",
            "node_name": "unknown",
            "duckdb_database": "/tmp/workspace.duckdb",
            "timestamp_utc": "2026-06-17T00:00:00+00:00",
        }


def wait_until(predicate, *, timeout: float = 20.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    last_value = None
    while time.monotonic() < deadline:
        last_value = predicate()
        if last_value:
            return last_value
        time.sleep(interval)
    return last_value


def _copy_table(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> None:
    con.execute(f"COPY {table} TO '{path.as_posix()}' (FORMAT parquet)")


def _write_fixture_parquet_files(root: Path) -> dict[str, Path]:
    paths = {
        KBKP_FULL_PATH: root / "kbkpfull.parquet",
        KBHP_FULL_PATH: root / "kbhpfull.parquet",
        KALENDER_PATH: root / "dim_kalender.parquet",
        FACT_BUPO_TARGET: root / "fact_bupo.parquet",
        KBKP_TODAY_TARGET: root / "kbkp_today.parquet",
    }
    for index, s3_path in enumerate(KBPO_PATHS):
        paths[s3_path] = root / f"kbpo_{index}.parquet"

    con = duckdb.connect(":memory:")
    try:
        con.execute(
            """
            CREATE TABLE kbkp AS
            SELECT
                1001::INTEGER AS KBKP_Belegnummer,
                'BA'::VARCHAR AS DOCO_Belegart,
                CURRENT_DATE AS KBKP_BelegDt,
                CURRENT_DATE AS KBKP_BuchungDt,
                'tester'::VARCHAR AS KBKP_ErstellungVon,
                NULL::INTEGER AS KBKP_StorniertBelegNummer,
                NULL::INTEGER AS KBKP_StornoBelegNummer,
                'SRC'::VARCHAR AS DOCO_BelegHerkunft,
                'GR'::VARCHAR AS DOCO_Buchunggrund,
                DATE '2000-01-01' AS KBKP_TechBeginnDt,
                DATE '2999-12-31' AS KBKP_TechEndeDt
            """
        )
        con.execute(
            """
            CREATE TABLE kbpo AS
            SELECT
                1001::INTEGER AS KBKP_Belegnummer,
                0::INTEGER AS KBPO_VtgKtoWiederholPos,
                1::INTEGER AS KBPO_VtgKtoPositionNr,
                0::INTEGER AS KBPO_Teilposition,
                'GF'::VARCHAR AS GEFA_GeschaeftFall,
                'P1'::VARCHAR AS PART_Partner,
                'KF'::VARCHAR AS KBPO_KtoFindMerkmal,
                'HV'::VARCHAR AS DOCO_Hauptvorgang,
                'TV'::VARCHAR AS DOCO_Teilvorgang,
                'BT'::VARCHAR AS DOCO_Belegtyp,
                'VK'::VARCHAR AS DOCO_VtrKtoTyp,
                'CHF'::VARCHAR AS DOCO_Waehrung,
                'FA'::VARCHAR AS DOCO_FormArt,
                10.0::DOUBLE AS KBPO_GesamtBetrag,
                10.0::DOUBLE AS KBPO_TWhrBetrag,
                'CHF'::VARCHAR AS KBPO_HbWaehrung,
                10.0::DOUBLE AS KBPO_HbBetrag,
                10.0::DOUBLE AS KBPO_HWhrBetrag1,
                1.0::DOUBLE AS KBPO_Umrechnungkurs,
                CURRENT_DATE AS KBPO_NettoFaelligkeitDT,
                'VG'::VARCHAR AS VTGP_VtrGegenstand,
                'VKN'::VARCHAR AS KBPO_VtrKtoNummer,
                1001::INTEGER AS KBKP_AusgleichBelegnummer,
                'A'::VARCHAR AS KBPO_AusgleichStatus,
                'AG'::VARCHAR AS KBPO_Ausgleichgrund,
                CURRENT_DATE AS KBPO_AusgleichDt,
                CURRENT_DATE AS KBPO_AusgleichBuchungDt,
                'NB'::VARCHAR AS KBPO_HBSachkto,
                'text'::VARCHAR AS KBPO_Beschreibung,
                'ST'::VARCHAR AS DOCO_SteuerCd,
                CURRENT_DATE AS KBPO_WertInternDt,
                'BANK'::VARCHAR AS KBPO_Bankverbindung,
                'RA'::VARCHAR AS DOCO_RecordArt,
                DATE '2000-01-01' AS KBPO_TechBeginnDt,
                DATE '2999-12-31' AS KBPO_TechEndeDt
            """
        )
        con.execute(
            """
            CREATE TABLE kbhp AS
            SELECT
                1001::INTEGER AS KBKP_BelegNummer,
                1::INTEGER AS KBHP_VTGKtoPositionNr,
                '4000'::VARCHAR AS KBHP_SachKto,
                'ABS'::VARCHAR AS KBHP_HBAbstimmschluessel,
                DATE '2000-01-01' AS KBHP_TechBeginnDt,
                DATE '2999-12-31' AS KBHP_TechEndeDt
            """
        )
        con.execute("CREATE TABLE kalender AS SELECT CURRENT_DATE AS Datum")
        _copy_table(con, "kbkp", paths[KBKP_FULL_PATH])
        _copy_table(con, "kbhp", paths[KBHP_FULL_PATH])
        _copy_table(con, "kalender", paths[KALENDER_PATH])
        for s3_path in KBPO_PATHS:
            _copy_table(con, "kbpo", paths[s3_path])
    finally:
        con.close()
    return paths


def _with_local_paths(sql: str, paths: dict[str, Path]) -> str:
    output = sql
    for s3_path, local_path in paths.items():
        output = output.replace(s3_path, local_path.as_posix())
    return output


class PureDuckDBPageTests(unittest.TestCase):
    def test_route_renders_standalone_page_without_notebook_shell(self) -> None:
        response = pure_duckdb_page(
            request=build_request("/pure-duckdb"),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-pure-duckdb-page', body)
        self.assertEqual(body.count('data-pure-duckdb-cell'), 19)
        self.assertIn('/static/js/pure-duckdb.js', body)
        self.assertNotIn('/static/js/app.js', body)
        self.assertNotIn('data-sidebar', body)
        self.assertNotIn('topbar', body)
        self.assertNotIn('data-query-cell', body)
        self.assertNotIn('/api/events/stream', body)

    def test_query_1b_renders_after_query_1_with_collapsed_optimization_remarks(self) -> None:
        response = pure_duckdb_page(
            request=build_request("/pure-duckdb"),
            service=FakeWorkbenchService(),
        )

        body = response.body.decode("utf-8")
        self.assertLess(
            body.index('data-cell-id="pure-duckdb-query-1"'),
            body.index('data-cell-id="pure-duckdb-query-1b"'),
        )
        self.assertLess(
            body.index('data-cell-id="pure-duckdb-query-1b"'),
            body.index('data-cell-id="pure-duckdb-query-2"'),
        )
        self.assertIn("Query 1b", body)
        self.assertIn("Optimization Remarks", body)
        self.assertIn("The expensive joined row set is built once", body)
        self.assertIn("The result remains consistent", body)
        self.assertEqual(body.count('class="pure-duckdb-remarks"'), 2)
        self.assertNotIn('class="pure-duckdb-remarks" open', body)

    def test_local_pure_duckdb_page_uses_local_compatible_s3_buckets(self) -> None:
        class LocalPureDuckDBService(FakeWorkbenchService):
            settings = type("LocalSettings", (), {"s3_endpoint": "localhost:9000"})()

        response = pure_duckdb_page(
            request=build_request("/pure-duckdb"),
            service=LocalPureDuckDBService(),
        )

        body = response.body.decode("utf-8")
        self.assertIn("s3://core/KBKPfull.parquet", body)
        self.assertIn("s3://core/KBHPfull.parquet", body)
        self.assertIn("s3://core/fact_bupo.parquet", body)
        self.assertIn("s3://core/kbkp_today.parquet", body)
        self.assertIn("s3://kbpoimports/KBPO_2018undvorher.parquet", body)
        self.assertIn("s3://3-1-imports/DIM_Kalender.parquet", body)
        self.assertNotIn("s3://CORE/KBKPfull.parquet", body)
        self.assertNotIn("s3://CORE/fact_bupo.parquet", body)
        self.assertNotIn("s3://KBPOimports/KBPO_2018undvorher.parquet", body)

    def test_query_2b_renders_after_query_2_with_collapsed_optimization_remarks(self) -> None:
        response = pure_duckdb_page(
            request=build_request("/pure-duckdb"),
            service=FakeWorkbenchService(),
        )

        body = response.body.decode("utf-8")
        self.assertLess(
            body.index('data-cell-id="pure-duckdb-query-2"'),
            body.index('data-cell-id="pure-duckdb-query-2b"'),
        )
        self.assertLess(
            body.index('data-cell-id="pure-duckdb-query-2b"'),
            body.index('data-cell-id="pure-duckdb-query-3"'),
        )
        self.assertIn("Query 2b", body)
        self.assertIn("Optimization Remarks", body)
        self.assertIn("builds the resolved joined row set once", body)
        self.assertIn("schema, row count, grouped fingerprints, and amount totals match Query 2", body)
        self.assertEqual(body.count('class="pure-duckdb-remarks"'), 2)
        self.assertNotIn('class="pure-duckdb-remarks" open', body)

    def test_home_page_contains_pure_duckdb_tile(self) -> None:
        home_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "home.html"
        ).read_text(encoding="utf-8")

        self.assertIn('href="/pure-duckdb"', home_template)
        self.assertIn("data-open-pure-duckdb", home_template)
        self.assertIn(">pure duckdb<", home_template)

    def test_result_rows_can_be_downloaded_as_csv(self) -> None:
        script = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "static" / "js" / "pure-duckdb.js"
        ).read_text(encoding="utf-8")
        styles = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "static" / "css" / "pure-duckdb.css"
        ).read_text(encoding="utf-8")

        self.assertIn("data-download-pure-duckdb-csv", script)
        self.assertIn("Download CSV", script)
        self.assertIn("text/csv;charset=utf-8", script)
        self.assertIn('link.download = `${safeCellId}.csv`', script)
        self.assertIn("completedResults.set", script)
        self.assertIn(".pure-duckdb-download-button", styles)

    def test_big_data_benchmark_script_targets_pure_duckdb_s3_fixtures(self) -> None:
        script = (REPO_ROOT / "scripts" / "pure_duckdb_big_data_benchmark.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("default=500.0", script)
        self.assertIn("KBKP_FULL_PATH", script)
        self.assertIn("KBHP_FULL_PATH", script)
        self.assertIn("KALENDER_PATH", script)
        self.assertIn("KBPO_PATHS", script)
        self.assertIn("run_api_benchmark", script)
        self.assertIn("run_ui_benchmark", script)
        self.assertIn("/api/pure-duckdb/jobs", script)
        self.assertIn("--local-compatible-s3-names", script)
        self.assertIn("Object key casing is preserved", script)

    def test_presets_are_final_duckdb_sql_without_virtual_s3_references(self) -> None:
        self.assertEqual(len(PURE_DUCKDB_CELLS), 19)
        self.assertEqual(
            [(cell.cell_id, cell.label) for cell in PURE_DUCKDB_CELLS[:4]],
            [
                ("pure-duckdb-query-1", "Query 1"),
                ("pure-duckdb-query-1b", "Query 1b"),
                ("pure-duckdb-query-2", "Query 2"),
                ("pure-duckdb-query-2b", "Query 2b"),
            ],
        )
        self.assertEqual(PURE_DUCKDB_CELLS[0].sql, _query_1_sql())
        self.assertEqual(PURE_DUCKDB_CELLS[2].sql, _query_2_sql())
        for cell in PURE_DUCKDB_CELLS:
            self.assertNotRegex(cell.sql, r"\bs3\.[A-Za-z0-9_\"]")
            self.assertNotIn("s3://n_3_1_imports/", cell.sql)
            self.assertNotIn("s3://3-1-imports/", cell.sql)
            self.assertNotIn("s3://core/kbkpfull.parquet", cell.sql)
            self.assertNotIn("s3://core/kbhpfull.parquet", cell.sql)
            self.assertNotIn("s3://kbpoimports/", cell.sql)
            self.assertTrue(
                "read_parquet('s3://" in cell.sql
                or "read_parquet([\n" in cell.sql
                or "TO 's3://" in cell.sql
            )
        self.assertIn("read_parquet('s3://CORE/KBKPfull.parquet')", PURE_DUCKDB_CELLS[0].sql)
        self.assertIn("read_parquet('s3://CORE/KBHPfull.parquet')", PURE_DUCKDB_CELLS[0].sql)
        self.assertIn(
            "read_parquet('s3://3_1_imports/DIM_Kalender.parquet')",
            PURE_DUCKDB_CELLS[0].sql,
        )
        self.assertIn("'s3://KBPOimports/KBPO_2018undvorher.parquet'", PURE_DUCKDB_CELLS[0].sql)
        self.assertIn("'s3://KBPOimports/KBPO2025.parquet'", PURE_DUCKDB_CELLS[0].sql)
        self.assertIn("union_by_name = true", PURE_DUCKDB_CELLS[0].sql)
        self.assertIn("union_by_name = true", PURE_DUCKDB_CELLS[1].sql)
        self.assertIn("CROSS JOIN (VALUES", PURE_DUCKDB_CELLS[1].sql)
        self.assertIn("current_kalender AS", PURE_DUCKDB_CELLS[1].sql)
        self.assertIn("resolved_positions AS", PURE_DUCKDB_CELLS[1].sql)
        self.assertEqual(PURE_DUCKDB_CELLS[1].remarks[0].split(":", 1)[0], "Query 1b keeps the result shape and business semantics of Query 1")
        self.assertIn("union_by_name = true", PURE_DUCKDB_CELLS[3].sql)
        self.assertIn("CROSS JOIN (VALUES", PURE_DUCKDB_CELLS[3].sql)
        self.assertIn("current_kalender AS", PURE_DUCKDB_CELLS[3].sql)
        self.assertIn("resolved_positions AS", PURE_DUCKDB_CELLS[3].sql)
        self.assertIn("COPY (", PURE_DUCKDB_CELLS[3].sql)
        self.assertIn("TO 's3://CORE/fact_bupo.parquet'", PURE_DUCKDB_CELLS[3].sql)
        self.assertIn("COMPRESSION zstd", PURE_DUCKDB_CELLS[3].sql)
        self.assertEqual(PURE_DUCKDB_CELLS[3].remarks[0].split(":", 1)[0], "Query 2b keeps the output contract of Query 2")

    def test_static_pure_duckdb_payload_keeps_production_s3_casing(self) -> None:
        payload = pure_duckdb_cells_payload()
        sql = "\n\n".join(str(cell["sql"]) for cell in payload[:4])

        self.assertIn("s3://CORE/KBKPfull.parquet", sql)
        self.assertIn("s3://CORE/KBHPfull.parquet", sql)
        self.assertIn("s3://CORE/fact_bupo.parquet", sql)
        self.assertIn("s3://KBPOimports/KBPO_2018undvorher.parquet", sql)
        self.assertIn("s3://3_1_imports/DIM_Kalender.parquet", sql)

    def test_appended_analytical_queries_are_translated_to_duckdb_sql(self) -> None:
        appended_sql = "\n\n".join(cell.sql for cell in PURE_DUCKDB_CELLS[11:])

        self.assertEqual(len(PURE_DUCKDB_CELLS[11:]), 8)
        self.assertIn("-- 5. HIGH CARDINALITY GROUP BY", PURE_DUCKDB_CELLS[11].sql)
        self.assertIn("read_parquet('s3://CORE/fact_bupo.parquet')", appended_sql)
        self.assertNotRegex(appended_sql, r"\bFROM\s+fact_bupo\b")
        self.assertNotRegex(appended_sql, r"\bSELECT\s+TOP\b")
        self.assertIn("LIMIT 10", PURE_DUCKDB_CELLS[15].sql)
        self.assertIn("DATE_TRUNC('month', Buchungsdatum)::DATE AS mmonth", PURE_DUCKDB_CELLS[18].sql)
        self.assertIn("ORDER BY mmonth, total DESC", PURE_DUCKDB_CELLS[18].sql)
        self.assertNotIn("ADD_MONTHS", appended_sql)
        self.assertNotIn("monthh", appended_sql)
        self.assertNotIn("GROUP BY Belegart\n", appended_sql)

    def test_pure_duckdb_manager_runs_directly_in_process(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            manager = PureDuckDBJobManager(
                settings=make_settings(Path(tmp_dir)),
                max_result_rows=10,
            )
            snapshot = manager.start_job(
                cell_id="pure-duckdb-query-test",
                sql="SELECT 1 AS pure_value",
            )

            terminal = manager.wait_for_terminal(snapshot.job_id, timeout=20)
            payload = terminal.payload
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["duckdbExecutionPath"], PURE_DUCKDB_DIRECT_EXECUTION_PATH)
            self.assertEqual(payload["timings"].get("engineAccessWaitMs"), 0.0)
            self.assertEqual(payload["rows"], [[1]])
            self.assertIn("backendTotalMs", payload["timings"])

    def test_service_pure_duckdb_bypasses_query_job_manager(self) -> None:
        class ExplodingQueryJobs:
            def start_job(self, **_kwargs):
                raise AssertionError("Pure DuckDB must not use QueryJobManager.start_job")

            def snapshot(self, _job_id):
                raise AssertionError("Pure DuckDB must not use QueryJobManager.snapshot")

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            service = WorkbenchService(make_settings(Path(tmp_dir)))
            service._query_jobs = ExplodingQueryJobs()  # type: ignore[assignment]

            started = service.start_pure_duckdb_job(
                cell_id="pure-duckdb-query-test",
                sql="SELECT 7 AS pure_value",
            )
            terminal = service._pure_duckdb_jobs.wait_for_terminal(  # type: ignore[attr-defined]
                started["jobId"],
                timeout=20,
            )
            payload = service.pure_duckdb_job(terminal.job_id)

            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["duckdbExecutionPath"], PURE_DUCKDB_DIRECT_EXECUTION_PATH)
            self.assertEqual(payload["rows"], [[7]])

    def test_pure_duckdb_s3_copy_writes_local_file_then_uploads(self) -> None:
        test_case = self

        class FakeS3Client:
            def __init__(self) -> None:
                self.put_object_calls: list[dict[str, object]] = []
                self.upload_parent: Path | None = None

            def put_object(self, **kwargs):
                body = kwargs["Body"]
                source_path = Path(body.name)
                self.upload_parent = source_path.parent
                test_case.assertTrue(source_path.is_file())
                uploaded_copy = self.upload_parent / "uploaded-copy.parquet"
                uploaded_copy.write_bytes(body.read())
                con = duckdb.connect(":memory:")
                try:
                    test_case.assertEqual(
                        con.execute(
                            f"SELECT COUNT(*) FROM read_parquet('{uploaded_copy.as_posix()}')"
                        ).fetchone()[0],
                        1,
                    )
                finally:
                    con.close()
                self.put_object_calls.append(
                    {
                        "Bucket": kwargs["Bucket"],
                        "Key": kwargs["Key"],
                    }
                )

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            settings = make_settings(Path(tmp_dir))
            settings.s3_endpoint = "ecspr01.sz.admin.ch:9021"
            settings.s3_access_key_id = "key"
            settings.s3_secret_access_key = "secret"
            settings.s3_region = "us-east-1"
            fake_client = FakeS3Client()

            manager = PureDuckDBJobManager(settings=settings, max_result_rows=10)
            with patch(
                "bit_data_workbench.backend.pure_duckdb_jobs.s3_client",
                return_value=fake_client,
            ):
                snapshot = manager.start_job(
                    cell_id="pure-duckdb-query-4",
                    sql=(
                        "COPY (SELECT 1 AS id, 'alpha' AS name) "
                        "TO 's3://CORE/kbkp_today.parquet' "
                        "(FORMAT parquet, OVERWRITE_OR_IGNORE true);"
                    ),
                )
                terminal = manager.wait_for_terminal(snapshot.job_id, timeout=20)

            payload = terminal.payload
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(
                fake_client.put_object_calls,
                [{"Bucket": "CORE", "Key": "kbkp_today.parquet"}],
            )
            self.assertGreater(payload["timings"].get("s3UploadMs", 0), 0)
            self.assertIsNotNone(fake_client.upload_parent)
            self.assertTrue(
                wait_until(
                    lambda: fake_client.upload_parent is not None
                    and not fake_client.upload_parent.exists(),
                    timeout=5,
                )
            )

    def test_pure_duckdb_s3_copy_rejects_outputs_above_single_put_limit(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            settings = make_settings(Path(tmp_dir))
            settings.s3_endpoint = "ecspr01.sz.admin.ch:9021"
            settings.s3_access_key_id = "key"
            settings.s3_secret_access_key = "secret"
            settings.s3_region = "us-east-1"
            manager = PureDuckDBJobManager(settings=settings, max_result_rows=10)
            with patch(
                "bit_data_workbench.backend.pure_duckdb_jobs.s3_client",
                return_value=object(),
            ), patch(
                "bit_data_workbench.backend.pure_duckdb_jobs.PURE_DUCKDB_SINGLE_PUT_MAX_BYTES",
                1,
            ):
                snapshot = manager.start_job(
                    cell_id="pure-duckdb-query-4",
                    sql=(
                        "COPY (SELECT 1 AS id, 'alpha' AS name) "
                        "TO 's3://CORE/kbkp_today.parquet' "
                        "(FORMAT parquet, OVERWRITE_OR_IGNORE true);"
                    ),
                )
                terminal = manager.wait_for_terminal(snapshot.job_id, timeout=20)

            payload = terminal.payload
            self.assertEqual(payload["status"], "failed")
            self.assertIn("single PUT limit", payload["error"])

    def test_local_duckdb_can_execute_all_preset_queries_with_tiny_parquet_fixtures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = _write_fixture_parquet_files(root)
            con = duckdb.connect(":memory:")
            try:
                for cell in PURE_DUCKDB_CELLS:
                    sql = _with_local_paths(cell.sql, paths)
                    result = con.execute(sql)
                    if cell.cell_id in {
                        "pure-duckdb-query-2",
                        "pure-duckdb-query-2b",
                        "pure-duckdb-query-4",
                    }:
                        self.assertIsNotNone(result)
                    else:
                        rows = result.fetchall()
                        self.assertGreaterEqual(len(rows), 1, cell.cell_id)
                self.assertTrue(paths[FACT_BUPO_TARGET].is_file())
                self.assertTrue(paths[KBKP_TODAY_TARGET].is_file())
                self.assertGreater(
                    con.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{paths[FACT_BUPO_TARGET].as_posix()}')"
                    ).fetchone()[0],
                    0,
                )
            finally:
                con.close()

    def test_query_2b_matches_query_2_against_tiny_parquet_fixtures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = _write_fixture_parquet_files(root)
            con = duckdb.connect(":memory:")
            try:
                query_2 = _with_local_paths(PURE_DUCKDB_CELLS[2].sql, paths)
                query_2b = _with_local_paths(PURE_DUCKDB_CELLS[3].sql, paths)
                con.execute(query_2)
                fact_path = paths[FACT_BUPO_TARGET].as_posix()
                con.execute(f"CREATE TEMP TABLE q2_baseline AS SELECT * FROM read_parquet('{fact_path}')")
                con.execute(query_2b)
                difference_count = con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        (SELECT * FROM q2_baseline
                         EXCEPT ALL
                         SELECT * FROM read_parquet('{fact_path}'))
                        UNION ALL
                        (SELECT * FROM read_parquet('{fact_path}')
                         EXCEPT ALL
                         SELECT * FROM q2_baseline)
                    ) differences
                    """
                ).fetchone()[0]

                self.assertEqual(difference_count, 0)
            finally:
                con.close()

    def test_query_1b_matches_query_1_against_tiny_parquet_fixtures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = _write_fixture_parquet_files(root)
            con = duckdb.connect(":memory:")
            try:
                query_1 = _with_local_paths(PURE_DUCKDB_CELLS[0].sql, paths)
                query_1b = _with_local_paths(PURE_DUCKDB_CELLS[1].sql, paths)
                baseline = con.execute(query_1).fetchone()
                optimized = con.execute(query_1b).fetchone()

                self.assertEqual(optimized[0], baseline[0])
                if baseline[1] is None or optimized[1] is None:
                    self.assertIsNone(baseline[1])
                    self.assertIsNone(optimized[1])
                else:
                    self.assertAlmostEqual(float(optimized[1]), float(baseline[1]), places=6)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()

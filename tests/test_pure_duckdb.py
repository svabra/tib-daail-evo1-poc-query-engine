from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import time
import unittest

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
)
from bit_data_workbench.backend.query_jobs import (  # noqa: E402
    DUCKDB_EXECUTION_PATH_ISOLATED_READ,
    DUCKDB_EXECUTION_PATH_ISOLATED_WRITE,
    QUERY_EXECUTION_DUCKDB_READ,
    QUERY_EXECUTION_DUCKDB_WRITE,
    QueryJobManager,
    classify_query_execution,
)
from bit_data_workbench.config import Settings  # noqa: E402
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
        self.assertEqual(body.count('data-pure-duckdb-cell'), 9)
        self.assertIn('/static/js/pure-duckdb.js', body)
        self.assertNotIn('/static/js/app.js', body)
        self.assertNotIn('data-sidebar', body)
        self.assertNotIn('topbar', body)
        self.assertNotIn('data-query-cell', body)
        self.assertNotIn('/api/events/stream', body)

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

    def test_presets_are_final_duckdb_sql_without_virtual_s3_references(self) -> None:
        self.assertEqual(len(PURE_DUCKDB_CELLS), 9)
        for cell in PURE_DUCKDB_CELLS:
            self.assertNotRegex(cell.sql, r"\bs3\.[A-Za-z0-9_\"]")
            self.assertNotIn("s3://n_3_1_imports/", cell.sql)
            self.assertNotIn("s3://3_1_imports/", cell.sql)
            self.assertTrue(
                "read_parquet('s3://" in cell.sql
                or "read_parquet([\n" in cell.sql
                or "TO 's3://" in cell.sql
            )
        self.assertIn("union_by_name = true", PURE_DUCKDB_CELLS[0].sql)
        self.assertIn("union_by_name = true", PURE_DUCKDB_CELLS[1].sql)

    def test_copy_artifact_queries_use_isolated_write_path(self) -> None:
        for cell in (PURE_DUCKDB_CELLS[1], PURE_DUCKDB_CELLS[3]):
            self.assertEqual(
                classify_query_execution(cell.sql, []),
                QUERY_EXECUTION_DUCKDB_WRITE,
            )
            self.assertEqual(
                QueryJobManager._duckdb_execution_path(
                    execution_mode=QUERY_EXECUTION_DUCKDB_WRITE,
                    source_ids=[],
                    touched_relations=[],
                    touched_buckets=[],
                    source_summaries=[],
                    execution_sql=cell.sql,
                ),
                DUCKDB_EXECUTION_PATH_ISOLATED_WRITE,
            )

    def test_pure_read_job_runs_isolated_without_shared_duckdb_wait(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            manager = QueryJobManager(
                settings=make_settings(Path(tmp_dir)),
                max_result_rows=10,
                notebook_title_resolver=lambda _notebook_id: "Pure DuckDB",
                metadata_refresher=lambda: None,
            )
            snapshot = manager.start_job(
                sql="SELECT 1 AS pure_value",
                execution_sql="SELECT 1 AS pure_value",
                notebook_id="pure-duckdb",
                notebook_title="Pure DuckDB",
                cell_id="pure-duckdb-query-test",
                data_sources=[],
                source_summaries=[],
            )

            terminal = manager.wait_for_terminal(snapshot.job_id, timeout=20)
            payload = terminal.payload
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["duckdbExecutionPath"], DUCKDB_EXECUTION_PATH_ISOLATED_READ)
            self.assertEqual(payload["timings"].get("engineAccessWaitMs"), 0.0)
            self.assertEqual(payload["rows"], [[1]])

    def test_local_duckdb_can_execute_all_preset_queries_with_tiny_parquet_fixtures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = _write_fixture_parquet_files(root)
            con = duckdb.connect(":memory:")
            try:
                for cell in PURE_DUCKDB_CELLS:
                    sql = _with_local_paths(cell.sql, paths)
                    result = con.execute(sql)
                    if cell.cell_id in {"pure-duckdb-query-2", "pure-duckdb-query-4"}:
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


if __name__ == "__main__":
    unittest.main()

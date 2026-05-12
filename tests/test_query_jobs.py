from __future__ import annotations

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.query_jobs import is_read_only_sql  # noqa: E402
from bit_data_workbench.models import QueryJobDefinition  # noqa: E402


def test_is_read_only_sql_allows_select_and_with() -> None:
    assert is_read_only_sql("select * from table_a")
    assert is_read_only_sql("with rows as (select 1) select * from rows")


def test_is_read_only_sql_rejects_write_and_configuration_statements() -> None:
    assert not is_read_only_sql("delete from table_a")
    assert not is_read_only_sql("create table x as select 1")
    assert not is_read_only_sql("select 1; drop table x")
    assert not is_read_only_sql("pragma enable_profiling")


def test_query_job_payload_includes_process_metrics_and_analyze_plan() -> None:
    job = QueryJobDefinition(
        job_id="analyze-1",
        notebook_id="nb",
        notebook_title="Notebook",
        cell_id="cell",
        sql="select 1",
        status="completed",
        started_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:01+00:00",
        workload_type="analyze",
        engine="duckdb",
        process_id=123,
        cpu_percent=4.5,
        memory_rss_bytes=1024,
        peak_memory_rss_bytes=2048,
        bytes_touched_estimate=4096,
        plan_text="analyzed plan",
        plan_rows=[("analyzed_plan", "analyzed plan")],
    )

    payload = job.payload

    assert payload["workloadType"] == "analyze"
    assert payload["engine"] == "duckdb"
    assert payload["processId"] == 123
    assert payload["cpuPercent"] == 4.5
    assert payload["peakMemoryRssBytes"] == 2048
    assert payload["bytesTouchedEstimate"] == 4096
    assert payload["planText"] == "analyzed plan"
    assert payload["planRows"] == [["analyzed_plan", "analyzed plan"]]

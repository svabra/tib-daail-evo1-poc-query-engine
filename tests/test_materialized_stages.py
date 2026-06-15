from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import threading
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_stage_components():
    from bit_data_workbench.backend.materialized_stages import (
        MaterializedStageManager,
        MaterializedStageStore,
        StageRecord,
        build_notebook_stage_graph,
        materialized_stage_query_sql,
        utc_now_iso,
    )

    return (
        MaterializedStageManager,
        MaterializedStageStore,
        StageRecord,
        build_notebook_stage_graph,
        materialized_stage_query_sql,
        utc_now_iso,
    )


def stage_cell(cell_id, alias, sql, predecessors=None):
    return {
        "cellId": cell_id,
        "language": "sql",
        "sql": sql,
        "dataSources": [],
        "stage": {
            "enabled": True,
            "stageId": f"stage-{alias}",
            "alias": alias,
            "title": alias.replace("_", " ").title(),
            "predecessorStageIds": predecessors or [],
            "materialize": True,
        },
    }


def forked_priority_cells():
    return [
        stage_cell("cell-1", "source", "select 1"),
        stage_cell("cell-2", "normalized", "select * from stage.source"),
        stage_cell("cell-3", "status_pressure", "select * from stage.normalized"),
        stage_cell("cell-4", "audit_candidates", "select * from stage.normalized"),
        stage_cell("cell-5", "audit_backlog", "select * from stage.audit_candidates"),
    ]


class FakeStageManager(import_stage_components()[0]):
    def __init__(self, store, fingerprints, sql_rewriter=None):
        MaterializedStageManager, _, _, _, _, _ = import_stage_components()
        super().__init__(
            settings=SimpleNamespace(s3_bucket="stage-bucket", shared_notebooks_bucket=None),
            store=store,
            connection_factory=lambda: None,
            source_summaries_provider=lambda _sql, _sources, _options: [],
            bootstrap_source_views=lambda _connection, _summaries: None,
            sql_rewriter=sql_rewriter,
            metadata_refresher=lambda: None,
            state_change_callback=lambda _snapshot: None,
            published_products_for_source=lambda _source: [],
            object_writer=lambda *_args: {},
        )
        self.fingerprints = dict(fingerprints)
        self.execution_order = []
        self.executed_sql = []

    def _execute_stage(
        self,
        *,
        run_id,
        graph,
        node,
        sql,
        execution_sql,
        sql_hash,
        revision_id,
        predecessor_records,
        predecessor_revision_ids,
        started_at,
        query_job_id,
    ):
        _, _, StageRecord, _, _, utc_now_iso = import_stage_components()
        stage_id = str(node["stageId"])
        self.execution_order.append(stage_id)
        self.executed_sql.append(execution_sql)
        now = utc_now_iso()
        fingerprint = self.fingerprints.get(stage_id, f"fingerprint-{stage_id}")
        return StageRecord(
            run_id=run_id,
            notebook_id=str(graph["notebookId"]),
            stage_id=stage_id,
            cell_id=str(node["cellId"]),
            stage_alias=str(node["alias"]),
            stage_title=str(node["title"]),
            status="completed",
            revision_id=revision_id,
            sql_hash=sql_hash,
            predecessor_revision_ids=list(predecessor_revision_ids),
            schema_fingerprint=f"schema-{stage_id}",
            row_count=1,
            size_bytes=100,
            result_fingerprint=fingerprint,
            output_bucket="stage-bucket",
            output_key=f"_bdw_stages/test/{node['alias']}/{revision_id}/data.parquet",
            output_path=f"s3://stage-bucket/_bdw_stages/test/{node['alias']}/{revision_id}/data.parquet",
            query_path=f"s3.stage_bucket._bdw_stages.test.{node['alias']}.data.parquet",
            started_at=started_at or now,
            completed_at=now,
            updated_at=now,
        )


class NotebookStagePipelineTests(unittest.TestCase):
    def test_stage_record_payload_includes_duration_ms(self) -> None:
        _, _, StageRecord, _, _, _ = import_stage_components()
        record = StageRecord(
            run_id="run-1",
            notebook_id="nb-1",
            stage_id="stage-raw",
            cell_id="cell-1",
            stage_alias="raw",
            stage_title="Raw",
            status="completed",
            started_at="2026-06-06T00:00:00+00:00",
            completed_at="2026-06-06T00:00:01.250000+00:00",
            updated_at="2026-06-06T00:00:01.250000+00:00",
        )

        self.assertEqual(record.payload["durationMs"], 1250)

    def test_stage_record_payload_exposes_simple_s3_reference(self) -> None:
        _, _, StageRecord, _, _, _ = import_stage_components()
        record = StageRecord.from_payload(
            {
                "runId": "run-1",
                "notebookId": "nb-1",
                "stageId": "stage-raw",
                "cellId": "cell-1",
                "stageAlias": "raw",
                "stageTitle": "Raw",
                "status": "completed",
                "outputBucket": "stage-bucket",
                "outputKey": "_bdw_stages/notebook/raw/n_20260607/data.parquet",
                "outputPath": "s3://stage-bucket/_bdw_stages/notebook/raw/n_20260607/data.parquet",
            }
        )

        self.assertIsNotNone(record)
        self.assertEqual(
            record.query_reference,
            's3."stage-bucket"."_bdw_stages/notebook/raw/n_20260607/data.parquet"',
        )
        self.assertEqual(record.payload["queryReference"], record.query_reference)
        self.assertEqual(record.payload["queryPath"], record.query_reference)
        self.assertEqual(
            record.payload["querySql"],
            "read_parquet('s3://stage-bucket/_bdw_stages/notebook/raw/n_20260607/data.parquet')",
        )

    def test_graph_orders_sql_stage_references_and_reports_missing_and_cycles(self) -> None:
        _, _, _, build_graph, _, _ = import_stage_components()
        cells = [
            stage_cell("cell-1", "raw", "select 1"),
            stage_cell("cell-2", "scope", "select * from stage.raw"),
            stage_cell("cell-3", "final", "select * from stage.scope"),
        ]

        graph = build_graph(notebook_id="nb-1", cells=cells)

        self.assertEqual(
            graph["order"],
            ["stage-raw", "stage-scope", "stage-final"],
        )
        self.assertEqual(graph["nodes"][1]["predecessorStageIds"], ["stage-raw"])
        self.assertEqual(graph["nodes"][2]["predecessorStageIds"], ["stage-scope"])
        self.assertEqual(graph["diagnostics"], [])

        missing = build_graph(
            notebook_id="nb-1",
            cells=[stage_cell("cell-1", "scope", "select * from stage.raw")],
        )
        self.assertEqual(missing["diagnostics"][0]["code"], "missing-stage-reference")

        cycle = build_graph(
            notebook_id="nb-1",
            cells=[
                stage_cell("cell-1", "a", "select * from stage.b"),
                stage_cell("cell-2", "b", "select * from stage.a"),
            ],
        )
        self.assertTrue(any(item["code"] == "cycle" for item in cycle["diagnostics"]))

    def test_graph_detects_terminal_priority_paths_in_forked_dag(self) -> None:
        _, _, _, build_graph, _, _ = import_stage_components()

        graph = build_graph(notebook_id="nb-1", cells=forked_priority_cells())

        self.assertEqual(
            [(path["terminalStageId"], path["label"], path["priority"]) for path in graph["paths"]],
            [
                ("stage-status_pressure", "Status Pressure", 1),
                ("stage-audit_backlog", "Audit Backlog", 2),
            ],
        )
        self.assertEqual(
            graph["paths"][0]["stageIds"],
            ["stage-source", "stage-normalized", "stage-status_pressure"],
        )
        self.assertEqual(
            graph["paths"][1]["stageIds"],
            [
                "stage-source",
                "stage-normalized",
                "stage-audit_candidates",
                "stage-audit_backlog",
            ],
        )

    def test_priority_paths_reorder_ready_siblings_without_skipping_stages(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        cells = forked_priority_cells()
        priority_paths = [
            {
                "pathId": "path-stage-audit_backlog",
                "terminalStageId": "stage-audit_backlog",
                "label": "Audit first",
                "priority": 1,
            },
            {
                "pathId": "path-stage-status_pressure",
                "terminalStageId": "stage-status_pressure",
                "label": "Status second",
                "priority": 2,
            },
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {
                    "stage-source": "source-v1",
                    "stage-normalized": "normalized-v1",
                    "stage-status_pressure": "status-v1",
                    "stage-audit_candidates": "audit-candidates-v1",
                    "stage-audit_backlog": "audit-backlog-v1",
                },
            )

            graph = manager.graph_payload(
                notebook_id="nb-1",
                cells=cells,
                pipeline_paths=priority_paths,
            )
            self.assertEqual(
                graph["order"],
                [
                    "stage-source",
                    "stage-normalized",
                    "stage-audit_candidates",
                    "stage-audit_backlog",
                    "stage-status_pressure",
                ],
            )

            manager.run_pipeline(
                notebook_id="nb-1",
                cells=cells,
                pipeline_paths=priority_paths,
            )
            manager.wait_for_idle()

            self.assertEqual(manager.execution_order, graph["order"])
            self.assertEqual(len(manager.execution_order), len(set(manager.execution_order)))
            self.assertEqual(set(manager.execution_order), set(graph["order"]))

    def test_stale_priority_path_metadata_is_ignored_when_terminal_disappears(self) -> None:
        _, _, _, build_graph, _, _ = import_stage_components()
        cells = [
            stage_cell("cell-1", "source", "select 1"),
            stage_cell("cell-2", "status_pressure", "select * from stage.source"),
        ]

        graph = build_graph(
            notebook_id="nb-1",
            cells=cells,
            pipeline_paths=[
                {
                    "pathId": "path-stage-audit_backlog",
                    "terminalStageId": "stage-audit_backlog",
                    "label": "Deleted terminal",
                    "priority": 1,
                }
            ],
        )

        self.assertEqual(len(graph["paths"]), 1)
        self.assertEqual(graph["paths"][0]["terminalStageId"], "stage-status_pressure")
        self.assertEqual(graph["paths"][0]["label"], "Status Pressure")

    def test_pipeline_run_order_and_fingerprint_obsolete_cascade(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "materialized-stages.json")
            manager = FakeStageManager(
                store,
                {
                    "stage-raw": "raw-v1",
                    "stage-scope": "scope-v1",
                    "stage-final": "final-v1",
                },
            )
            cells = [
                stage_cell("cell-1", "raw", "select 1"),
                stage_cell("cell-2", "scope", "select * from stage.raw"),
                stage_cell("cell-3", "final", "select * from stage.scope"),
            ]

            manager.run_pipeline(notebook_id="nb-1", cells=cells)
            manager.wait_for_idle()
            self.assertEqual(
                manager.execution_order,
                ["stage-raw", "stage-scope", "stage-final"],
            )
            graph = manager.graph_payload(notebook_id="nb-1", cells=cells)
            self.assertEqual([node["status"] for node in graph["nodes"]], ["valid", "valid", "valid"])

            manager.execution_order.clear()
            manager.run_stage(notebook_id="nb-1", stage_id="stage-raw", cells=cells)
            manager.wait_for_idle()
            graph = manager.graph_payload(notebook_id="nb-1", cells=cells)
            self.assertEqual([node["status"] for node in graph["nodes"]], ["valid", "obsolete", "obsolete"])

            manager.execution_order.clear()
            manager.fingerprints["stage-raw"] = "raw-v2"
            manager.run_stage(notebook_id="nb-1", stage_id="stage-raw", cells=cells)
            manager.wait_for_idle()

            graph = manager.graph_payload(notebook_id="nb-1", cells=cells)
            self.assertEqual(
                [node["status"] for node in graph["nodes"]],
                ["valid", "obsolete", "obsolete"],
            )

    def test_pipeline_run_reexecutes_valid_stages_in_dependency_order(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {
                    "stage-raw": "raw-v1",
                    "stage-scope": "scope-v1",
                    "stage-final": "final-v1",
                },
            )
            cells = [
                stage_cell("cell-1", "raw", "select 1"),
                stage_cell("cell-2", "scope", "select * from stage.raw"),
                stage_cell("cell-3", "final", "select * from stage.scope"),
            ]

            manager.run_pipeline(notebook_id="nb-1", cells=cells)
            manager.wait_for_idle()
            self.assertEqual(
                manager.execution_order,
                ["stage-raw", "stage-scope", "stage-final"],
            )
            graph = manager.graph_payload(notebook_id="nb-1", cells=cells)
            self.assertEqual([node["status"] for node in graph["nodes"]], ["valid", "valid", "valid"])

            manager.execution_order.clear()
            manager.run_pipeline(notebook_id="nb-1", cells=cells)
            manager.wait_for_idle()

            self.assertEqual(
                manager.execution_order,
                ["stage-raw", "stage-scope", "stage-final"],
            )

    def test_graph_keeps_completed_revision_usable_after_failed_rerun(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {"stage-raw": "raw-v1"},
            )
            cells = [stage_cell("cell-1", "raw", "select 1")]

            manager.run_pipeline(notebook_id="nb-1", cells=cells)
            manager.wait_for_idle()
            graph = manager.graph_payload(notebook_id="nb-1", cells=cells)
            self.assertEqual(graph["nodes"][0]["status"], "valid")

            failed_record = manager._record_for_node(
                "run-failed",
                {**graph["nodes"][0], "notebookId": "nb-1"},
                status="failed",
                error="boom",
            )
            manager._append_record(failed_record)

            graph = manager.graph_payload(notebook_id="nb-1", cells=cells)
            self.assertEqual(graph["nodes"][0]["status"], "valid")
            self.assertEqual(graph["nodes"][0]["latestRun"]["status"], "failed")
            self.assertIn("latest saved materialized revision", graph["nodes"][0]["runWarning"])

    def test_stop_stage_marks_matching_active_run_cancel_requested(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {"stage-raw": "raw-v1"},
            )
            with manager._lock:
                manager._active_runs["run-1"] = {
                    "runId": "run-1",
                    "notebookId": "nb-1",
                    "stageIds": ["stage-raw"],
                    "cancelRequested": False,
                }

            payload = manager.stop_stage(notebook_id="nb-1", stage_id="stage-raw")

            self.assertTrue(manager._active_runs["run-1"]["cancelRequested"])
            self.assertTrue(
                any(
                    record["stageId"] == "stage-raw" and record["status"] == "cancelled"
                    for record in payload["records"]
                )
            )

    def test_cancel_pipeline_marks_matching_active_runs_cancel_requested(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {"stage-raw": "raw-v1"},
            )
            with manager._lock:
                manager._active_runs["run-1"] = {
                    "runId": "run-1",
                    "notebookId": "nb-1",
                    "stageIds": ["stage-raw"],
                    "status": "running",
                    "cancelRequested": False,
                }
                manager._active_runs["run-2"] = {
                    "runId": "run-2",
                    "notebookId": "nb-2",
                    "stageIds": ["stage-other"],
                    "status": "running",
                    "cancelRequested": False,
                }

            payload = manager.cancel_pipeline(notebook_id="nb-1")

            self.assertTrue(manager._active_runs["run-1"]["cancelRequested"])
            self.assertEqual(manager._active_runs["run-1"]["status"], "cancelling")
            self.assertFalse(manager._active_runs["run-2"]["cancelRequested"])
            self.assertTrue(
                any(run["runId"] == "run-1" and run["cancelRequested"] for run in payload["activeRuns"])
            )

    def test_run_pipeline_from_stage_runs_selected_stage_and_successors_only(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {
                    "stage-raw": "raw-v1",
                    "stage-scope": "scope-v1",
                    "stage-final": "final-v1",
                },
            )
            cells = [
                stage_cell("cell-1", "raw", "select 1"),
                stage_cell("cell-2", "scope", "select * from stage.raw", ["stage-raw"]),
                stage_cell("cell-3", "final", "select * from stage.scope", ["stage-scope"]),
            ]

            manager.run_stage(notebook_id="nb-1", stage_id="stage-raw", cells=cells)
            manager.wait_for_idle()
            manager.execution_order.clear()

            manager.run_pipeline(
                notebook_id="nb-1",
                cells=cells,
                start_stage_id="stage-scope",
            )
            manager.wait_for_idle()

            self.assertEqual(manager.execution_order, ["stage-scope", "stage-final"])

    def test_run_pipeline_from_unknown_stage_fails(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {"stage-raw": "raw-v1"},
            )

            snapshot = manager.run_pipeline(
                notebook_id="nb-1",
                cells=[stage_cell("cell-1", "raw", "select 1")],
                start_stage_id="stage-missing",
            )

            failed = snapshot["records"][-1]
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["stageId"], "stage-missing")
            self.assertIn("Unknown stage: stage-missing", failed["error"])

    def test_atomic_stage_run_ignores_unrelated_downstream_diagnostics(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {"stage-raw": "raw-v1"},
            )
            cells = [
                stage_cell("cell-1", "raw", "select 1"),
                stage_cell("cell-2", "broken", "select * from stage.missing"),
            ]

            manager.run_stage(notebook_id="nb-1", stage_id="stage-raw", cells=cells)
            manager.wait_for_idle()

            self.assertEqual(manager.execution_order, ["stage-raw"])
            graph = manager.graph_payload(notebook_id="nb-1", cells=cells)
            self.assertEqual(graph["nodes"][0]["status"], "valid")

            snapshot = manager.run_pipeline(notebook_id="nb-1", cells=cells)
            failed = snapshot["records"][-1]
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["stageId"], "stage-broken")
            self.assertIn("stage.missing", failed["error"])

    def test_stage_execution_strips_trailing_semicolons_before_copy_wrapper(self) -> None:
        _, Store, _, _, normalize_stage_sql, _ = import_stage_components()
        self.assertEqual(normalize_stage_sql(" SELECT 1; \n ; "), "SELECT 1")
        self.assertEqual(
            normalize_stage_sql(" read_parquet('s3://bucket/path/data.parquet'); "),
            "SELECT * FROM read_parquet('s3://bucket/path/data.parquet')",
        )
        self.assertEqual(
            normalize_stage_sql(" parquet_scan('s3://bucket/path/*.parquet') "),
            "SELECT * FROM parquet_scan('s3://bucket/path/*.parquet')",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {"stage-raw": "raw-v1"},
            )

            manager.run_pipeline(
                notebook_id="nb-1",
                cells=[stage_cell("cell-1", "raw", " SELECT 1 AS id; \n")],
            )
            manager.wait_for_idle()

            self.assertEqual(manager.executed_sql, ["SELECT 1 AS id"])

    def test_stage_execution_uses_rewritten_sql_for_materialization(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {"stage-raw": "raw-v1"},
                sql_rewriter=lambda sql, _sources, _options: sql.replace(
                    "s3.vat_smoke_test.startup.vat_context_bootstrap.csv",
                    "vat_smoke_test_e93f1988.vat_context_bootstrap",
                ),
            )

            manager.run_pipeline(
                notebook_id="nb-1",
                cells=[
                    stage_cell(
                        "cell-1",
                        "raw",
                        "SELECT * FROM s3.vat_smoke_test.startup.vat_context_bootstrap.csv;",
                    )
                ],
            )
            manager.wait_for_idle()

            self.assertEqual(
                manager.executed_sql,
                ["SELECT * FROM vat_smoke_test_e93f1988.vat_context_bootstrap"],
            )

    def test_stage_execution_wraps_rewritten_bare_reader_for_materialization(self) -> None:
        _, Store, _, _, _, _ = import_stage_components()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeStageManager(
                Store(Path(temp_dir) / "materialized-stages.json"),
                {"stage-raw": "raw-v1"},
                sql_rewriter=lambda sql, _sources, _options: sql.replace(
                    's3."vat-smoke-test"."_bdw_stages/kostenbelege/data.parquet"',
                    "read_parquet('s3://vat-smoke-test/_bdw_stages/kostenbelege/data.parquet')",
                ),
            )

            manager.run_pipeline(
                notebook_id="nb-1",
                cells=[
                    stage_cell(
                        "cell-1",
                        "raw",
                        's3."vat-smoke-test"."_bdw_stages/kostenbelege/data.parquet";',
                    )
                ],
            )
            manager.wait_for_idle()

            self.assertEqual(
                manager.executed_sql,
                [
                    "SELECT * FROM "
                    "read_parquet('s3://vat-smoke-test/_bdw_stages/kostenbelege/data.parquet')"
                ],
            )

    def test_pipeline_stage_runs_kbpo_union_through_query_job_runner_with_custom_output_file(self) -> None:
        from bit_data_workbench.backend.query_aliases import rewrite_query_aliases
        from bit_data_workbench.backend.sql_utils import sql_literal

        _, Store, _, _, _, _ = import_stage_components()
        file_names = [
            "KBPO_2018undvorher.parquet",
            "KBPO_2019.parquet",
            "KBPO2020.parquet",
            "KBPO2021.parquet",
            "KBPO2022.parquet",
            "KBPO2023.parquet",
            "KBPO2024.parquet",
            "KBPO2025.parquet",
        ]
        query = "\nUNION ALL\n".join(
            f'SELECT * FROM s3.KBPOimports."{file_name}"'
            for file_name in file_names
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            parquet_dir = root / "kbpo"
            parquet_dir.mkdir()
            alias_map = {}
            for index, file_name in enumerate(file_names, start=1):
                parquet_path = parquet_dir / file_name
                connection = duckdb.connect()
                try:
                    connection.execute(
                        """
                        CREATE TABLE kbpo(
                            KBKP_Belegnummer BIGINT,
                            KBPO_VtgKtoWiederholPos BIGINT,
                            KBPO_VtgKtoPositionNr BIGINT,
                            KBPO_Teilposition BIGINT,
                            PART_Partner BIGINT,
                            GEFA_GeschaeftFall VARCHAR,
                            KBPO_BelegDt DATE,
                            KBPO_HWhrBetrag1 DOUBLE,
                            KBPO_ErfassDz TIMESTAMP
                        )
                        """
                    )
                    connection.execute(
                        """
                        INSERT INTO kbpo VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            10_000 + index,
                            1,
                            index,
                            0,
                            20_000 + index,
                            f"fall-{index}",
                            "2025-01-01",
                            float(index) * 10.5,
                            "2025-01-01 12:00:00",
                        ],
                    )
                    connection.execute(
                        f"COPY kbpo TO {sql_literal(parquet_path.as_posix())} (FORMAT PARQUET)"
                    )
                finally:
                    connection.close()
                alias_map[f's3.KBPOimports."{file_name}"'] = (
                    f"read_parquet({sql_literal(parquet_path.as_posix())})"
                )

            query_jobs = []
            writes = []

            def run_query_job(**kwargs):
                query_jobs.append(kwargs)
                connection = duckdb.connect()
                try:
                    connection.execute(str(kwargs["execution_sql"]))
                finally:
                    connection.close()
                return {
                    "jobId": kwargs["requested_job_id"],
                    "status": "completed",
                    "progressEvents": [{"event": "completed"}],
                }

            def write_object(bucket, output_key, local_output, metadata_key, metadata):
                connection = duckdb.connect()
                try:
                    rows = connection.execute(
                        f"SELECT COUNT(*) FROM read_parquet({sql_literal(local_output.as_posix())})"
                    ).fetchone()[0]
                finally:
                    connection.close()
                writes.append(
                    {
                        "bucket": bucket,
                        "output_key": output_key,
                        "metadata_key": metadata_key,
                        "metadata": dict(metadata),
                        "rows": rows,
                    }
                )
                return {"bucket": bucket, "key": output_key, "metadataKey": metadata_key}

            manager = import_stage_components()[0](
                settings=SimpleNamespace(s3_bucket="stage-bucket", shared_notebooks_bucket=None),
                store=Store(root / "materialized-stages.json"),
                connection_factory=lambda: duckdb.connect(),
                source_summaries_provider=lambda _sql, _sources, _options: [
                    {
                        "relation": f's3.KBPOimports."{file_name}"',
                        "bucket": "KBPOimports",
                        "key": file_name,
                        "path": f"s3://KBPOimports/{file_name}",
                        "format": "parquet",
                        "query_sql": alias_map[f's3.KBPOimports."{file_name}"'],
                    }
                    for file_name in file_names
                ],
                bootstrap_source_views=lambda _connection, _summaries: None,
                sql_rewriter=lambda sql, _sources, _options: rewrite_query_aliases(sql, alias_map),
                metadata_refresher=lambda: None,
                state_change_callback=lambda _snapshot: None,
                published_products_for_source=lambda _source: [],
                object_writer=write_object,
                query_job_runner=run_query_job,
            )
            cell = stage_cell("cell-kbpo", "kbpo_union", query)
            cell["dataSources"] = ["workspace.s3"]
            cell["stage"]["outputFileName"] = "kbpo_pipeline_result"

            manager.run_pipeline(notebook_id="nb-kbpo", notebook_title="KBPO", cells=[cell])
            manager.wait_for_idle()

            records = manager.state_payload()["records"]
            completed = [record for record in records if record["status"] == "completed"]
            self.assertEqual(len(completed), 1, records)
            self.assertEqual(completed[0]["rowCount"], len(file_names))
            self.assertEqual(completed[0]["outputFileName"], "kbpo_pipeline_result.parquet")
            self.assertTrue(completed[0]["outputKey"].endswith("/kbpo_pipeline_result.parquet"))
            self.assertTrue(completed[0]["queryJobId"].startswith("query-pipeline-"))
            self.assertEqual(writes[0]["rows"], len(file_names))
            self.assertEqual(writes[0]["metadata"]["outputFileName"], "kbpo_pipeline_result.parquet")
            self.assertEqual(writes[0]["metadata"]["queryJobId"], completed[0]["queryJobId"])
            self.assertEqual(len(query_jobs), 1)
            self.assertEqual(query_jobs[0]["requested_job_id"], completed[0]["queryJobId"])
            self.assertIn("COPY (", query_jobs[0]["execution_sql"])
            self.assertIn("kbpo_pipeline_result.parquet", query_jobs[0]["execution_sql"])
            self.assertEqual(len(query_jobs[0]["source_summaries"]), len(file_names))


if __name__ == "__main__":
    unittest.main()

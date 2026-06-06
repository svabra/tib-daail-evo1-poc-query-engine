from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import threading
import unittest


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
            started_at=now,
            completed_at=now,
            updated_at=now,
        )


class NotebookStagePipelineTests(unittest.TestCase):
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
            self.assertEqual([node["status"] for node in graph["nodes"]], ["valid", "valid", "valid"])

            manager.execution_order.clear()
            manager.fingerprints["stage-raw"] = "raw-v2"
            manager.run_stage(notebook_id="nb-1", stage_id="stage-raw", cells=cells)
            manager.wait_for_idle()

            graph = manager.graph_payload(notebook_id="nb-1", cells=cells)
            self.assertEqual(
                [node["status"] for node in graph["nodes"]],
                ["valid", "obsolete", "obsolete"],
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

            with self.assertRaises(ValueError):
                manager.run_pipeline(notebook_id="nb-1", cells=cells)

    def test_stage_execution_strips_trailing_semicolons_before_copy_wrapper(self) -> None:
        _, Store, _, _, normalize_stage_sql, _ = import_stage_components()
        self.assertEqual(normalize_stage_sql(" SELECT 1; \n ; "), "SELECT 1")
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


if __name__ == "__main__":
    unittest.main()

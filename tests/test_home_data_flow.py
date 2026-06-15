from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.home_data_flow import build_home_data_flows  # noqa: E402
from bit_data_workbench.models import (  # noqa: E402
    NotebookCellDefinition,
    NotebookDefinition,
)


def pipeline_notebook() -> NotebookDefinition:
    return NotebookDefinition(
        notebook_id="kostenbelege-pipeline",
        title="Kostenbelege Pipeline",
        summary="Pipeline summary",
        linked_generator_id="kostenbelege_3_1_multi_source_loader",
        pipeline_mode="pipeline",
        cells=[
            NotebookCellDefinition(
                cell_id="cell-1",
                sql="select 1",
                data_sources=["workspace.s3"],
                stage={
                    "stageId": "stage-1",
                    "title": "Settlement Audit",
                    "alias": "settlement_audit",
                },
            )
        ],
    )


def product_payload() -> dict[str, object]:
    return {
        "productId": "product-kostenbelege",
        "slug": "kostenbelege-settlement-audit",
        "title": "Kostenbelege Settlement Audit",
        "documentationPath": "/dataproducts/kostenbelege-settlement-audit",
        "publicPath": "/api/public/data-products/kostenbelege-settlement-audit",
        "sourceKind": "object",
        "sourceId": "workspace.s3",
        "bucket": "shared",
        "key": "generated/kostenbelege/output/*.parquet",
        "sourceDisplayName": "kostenbelege output",
        "sourcePlatform": "s3",
    }


class HomeDataFlowTests(unittest.TestCase):
    def test_product_lineage_uses_pipeline_graph_and_csv_intake_abstraction(self) -> None:
        notebook = pipeline_notebook()
        product = product_payload()

        def graph_provider(_notebook: NotebookDefinition) -> dict[str, object]:
            return {
                "nodes": [
                    {
                        "stageId": "stage-1",
                        "title": "Settlement Audit",
                        "status": "materialized",
                        "latestRevision": {"rowCount": 10},
                        "latestRun": {"status": "completed"},
                        "outputSource": {
                            "sourceKind": "object",
                            "sourceId": "workspace.s3",
                            "bucket": "shared",
                            "key": "generated/kostenbelege/output/*.parquet",
                        },
                        "publishedDataProducts": [product],
                    }
                ],
                "sourceNodes": [
                    {"sourceId": "source:workspace.s3", "label": "workspace.s3"},
                    {"sourceId": "source:pg_oltp", "label": "pg_oltp"},
                ],
                "diagnostics": [],
                "activeRuns": [],
            }

        flows = build_home_data_flows(
            notebooks=[notebook],
            data_products=[product],
            graph_provider=graph_provider,
        )

        self.assertEqual(len(flows), 1)
        flow = flows[0]
        self.assertEqual(flow["kind"], "dataProduct")
        self.assertEqual(flow["title"], "Kostenbelege Settlement Audit")
        self.assertEqual(flow["status"]["tone"], "success")
        self.assertIn("Published from Kostenbelege Pipeline", flow["subtitle"])
        node_titles = [node["title"] for node in flow["nodes"]]
        self.assertIn("CSV upload", node_titles)
        self.assertIn("CSV validation", node_titles)
        self.assertIn("S3 landing", node_titles)
        self.assertIn("PostgreSQL import", node_titles)
        self.assertIn("Individuals", node_titles)
        self.assertIn("Microsoft Business Innovator", node_titles)
        self.assertNotIn("sql", flow)

    def test_pipeline_without_product_becomes_publication_pending_slide(self) -> None:
        notebook = pipeline_notebook()

        def graph_provider(_notebook: NotebookDefinition) -> dict[str, object]:
            return {
                "nodes": [
                    {
                        "stageId": "stage-1",
                        "title": "Prepared Output",
                        "status": "materialized",
                        "latestRevision": {"rowCount": 10},
                        "outputSource": {
                            "sourceKind": "object",
                            "sourceId": "workspace.s3",
                            "bucket": "shared",
                            "key": "generated/output/*.parquet",
                        },
                        "publishedDataProducts": [],
                    }
                ],
                "sourceNodes": [],
                "diagnostics": [],
                "activeRuns": [],
            }

        flows = build_home_data_flows(
            notebooks=[notebook],
            data_products=[],
            graph_provider=graph_provider,
        )

        self.assertEqual(flows[0]["kind"], "pipeline")
        self.assertEqual(flows[0]["status"]["tone"], "success")
        self.assertTrue(
            any(badge["label"] == "Publication pending" for badge in flows[0]["badges"])
        )
        self.assertEqual(flows[0]["nodes"][-1]["detail"], "Pending publication")

    def test_graph_errors_surface_as_error_tone(self) -> None:
        notebook = pipeline_notebook()

        def graph_provider(_notebook: NotebookDefinition) -> dict[str, object]:
            return {
                "nodes": [
                    {
                        "stageId": "stage-1",
                        "title": "Broken Stage",
                        "status": "failed",
                    }
                ],
                "sourceNodes": [],
                "diagnostics": [{"severity": "error", "message": "Parser error"}],
            }

        flows = build_home_data_flows(
            notebooks=[notebook],
            data_products=[],
            graph_provider=graph_provider,
        )

        self.assertEqual(flows[0]["status"]["tone"], "error")
        self.assertTrue(
            any("error" in badge["label"] for badge in flows[0]["badges"])
        )


if __name__ == "__main__":
    unittest.main()

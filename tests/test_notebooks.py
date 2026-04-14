from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_notebook_helpers():
    from bit_data_workbench.backend.notebooks import build_generator_notebook_links
    from bit_data_workbench.models import NotebookCellDefinition, NotebookDefinition

    return build_generator_notebook_links, NotebookCellDefinition, NotebookDefinition


class GeneratorNotebookLinkTests(unittest.TestCase):
    def test_build_generator_notebook_links_groups_all_notebooks_for_a_loader(self) -> None:
        (
            build_generator_notebook_links,
            notebook_cell_type,
            notebook_type,
        ) = import_notebook_helpers()
        notebooks = [
            notebook_type(
                notebook_id="pg-vs-s3-contest-oltp",
                title="PG vs S3 Contest OLTP via DuckDB",
                summary="Contest OLTP",
                cells=[notebook_cell_type(cell_id="contest-1", sql="select 1")],
                linked_generator_id="pg_vs_s3_contest_loader",
            ),
            notebook_type(
                notebook_id="pg-vs-s3-contest-s3",
                title="PG vs S3 Contest S3 via DuckDB",
                summary="Contest S3",
                cells=[notebook_cell_type(cell_id="contest-2", sql="select 2")],
                linked_generator_id="pg_vs_s3_contest_loader",
            ),
            notebook_type(
                notebook_id="pg-vs-s3-contest-pg-native",
                title="PG vs S3 Contest OLTP via Native",
                summary="Contest Native",
                cells=[notebook_cell_type(cell_id="contest-3", sql="select 3")],
                linked_generator_id="pg_vs_s3_contest_loader",
            ),
            notebook_type(
                notebook_id="postgres-smoke-test",
                title="PostgreSQL Smoke Test",
                summary="Smoke",
                cells=[notebook_cell_type(cell_id="smoke-1", sql="select 4")],
                linked_generator_id="postgres_oltp_smoke_orders",
            ),
        ]

        result = build_generator_notebook_links(notebooks)

        self.assertEqual(
            [item.payload for item in result["pg_vs_s3_contest_loader"]],
            [
                {
                    "notebookId": "pg-vs-s3-contest-oltp",
                    "title": "PG vs S3 Contest OLTP via DuckDB",
                },
                {
                    "notebookId": "pg-vs-s3-contest-s3",
                    "title": "PG vs S3 Contest S3 via DuckDB",
                },
                {
                    "notebookId": "pg-vs-s3-contest-pg-native",
                    "title": "PG vs S3 Contest OLTP via Native",
                },
            ],
        )
        self.assertEqual(
            [item.payload for item in result["postgres_oltp_smoke_orders"]],
            [
                {
                    "notebookId": "postgres-smoke-test",
                    "title": "PostgreSQL Smoke Test",
                }
            ],
        )

    def test_build_generator_notebook_links_skips_empty_and_duplicate_entries(self) -> None:
        (
            build_generator_notebook_links,
            notebook_cell_type,
            notebook_type,
        ) = import_notebook_helpers()
        notebooks = [
            notebook_type(
                notebook_id="shared-a",
                title="Shared Notebook A",
                summary="Shared",
                cells=[notebook_cell_type(cell_id="shared-1", sql="select 1")],
                linked_generator_id="loader-a",
            ),
            notebook_type(
                notebook_id="shared-a",
                title="Shared Notebook A duplicate",
                summary="Shared duplicate",
                cells=[notebook_cell_type(cell_id="shared-2", sql="select 2")],
                linked_generator_id="loader-a",
            ),
            notebook_type(
                notebook_id="orphan",
                title="Orphan Notebook",
                summary="No loader",
                cells=[notebook_cell_type(cell_id="orphan-1", sql="select 3")],
                linked_generator_id="",
            ),
        ]

        result = build_generator_notebook_links(notebooks)

        self.assertEqual(list(result.keys()), ["loader-a"])
        self.assertEqual(
            [item.payload for item in result["loader-a"]],
            [{"notebookId": "shared-a", "title": "Shared Notebook A"}],
        )


if __name__ == "__main__":
    unittest.main()

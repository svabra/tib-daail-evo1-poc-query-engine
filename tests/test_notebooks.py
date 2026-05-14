from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_notebook_helpers():
    from bit_data_workbench.backend.notebooks import (
        build_generator_notebook_links,
        build_notebook_tree,
        build_notebooks,
    )
    from bit_data_workbench.models import (
        NotebookCellDefinition,
        NotebookDefinition,
        SourceCatalog,
        SourceObject,
        SourceSchema,
    )

    return (
        build_generator_notebook_links,
        build_notebook_tree,
        build_notebooks,
        NotebookCellDefinition,
        NotebookDefinition,
        SourceCatalog,
        SourceObject,
        SourceSchema,
    )


class GeneratorNotebookLinkTests(unittest.TestCase):
    def test_build_generator_notebook_links_groups_all_notebooks_for_a_loader(
        self,
    ) -> None:
        (
            build_generator_notebook_links,
            _,
            _,
            notebook_cell_type,
            notebook_type,
            _,
            _,
            _,
        ) = import_notebook_helpers()
        notebooks = [
            notebook_type(
                notebook_id="pg-vs-s3-contest-oltp",
                title="PG vs S3 Contest OLTP via DuckDB",
                summary="Contest OLTP",
                cells=[
                    notebook_cell_type(cell_id="contest-1", sql="select 1")
                ],
                linked_generator_id="pg_vs_s3_contest_loader",
            ),
            notebook_type(
                notebook_id="pg-vs-s3-contest-s3",
                title="PG vs S3 Contest S3 via DuckDB",
                summary="Contest S3",
                cells=[
                    notebook_cell_type(cell_id="contest-2", sql="select 2")
                ],
                linked_generator_id="pg_vs_s3_contest_loader",
            ),
            notebook_type(
                notebook_id="pg-vs-s3-contest-pg-native",
                title="PG vs S3 Contest OLTP via Native",
                summary="Contest Native",
                cells=[
                    notebook_cell_type(cell_id="contest-3", sql="select 3")
                ],
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

    def test_build_generator_notebook_links_skips_empty_and_duplicate_entries(
        self,
    ) -> None:
        (
            build_generator_notebook_links,
            _,
            _,
            notebook_cell_type,
            notebook_type,
            _,
            _,
            _,
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

    def test_build_notebooks_uses_fallback_sql_when_sources_are_missing(
        self,
    ) -> None:
        (
            _,
            _,
            build_notebooks,
            _,
            _,
            _,
            _,
            _,
        ) = import_notebook_helpers()

        notebooks = {
            notebook.notebook_id: notebook
            for notebook in build_notebooks([])
        }

        self.assertIn(
            "Run the S3 VAT Smoke Loader",
            notebooks["s3-smoke-test"].cells[0].sql,
        )
        self.assertIn(
            "Run the PostgreSQL OLTP VAT Smoke Loader",
            notebooks["postgres-smoke-test"].cells[0].sql,
        )
        self.assertIn(
            "Run the MWA Abrechnung Multi-Format Loader (3.2)",
            notebooks["mwa-abrechnung-s3-parquet"].cells[0].sql,
        )

    def test_immutable_preset_folders_are_public(self) -> None:
        (
            _,
            build_notebook_tree,
            build_notebooks,
            _,
            _,
            _,
            _,
            _,
        ) = import_notebook_helpers()

        tree = build_notebook_tree(build_notebooks([]))
        poc_folder = next(folder for folder in tree if folder.name == "PoC Tests")
        self.assertFalse(poc_folder.can_edit)
        self.assertFalse(poc_folder.can_delete)
        self.assertTrue(poc_folder.is_shared)
        self.assertTrue(all(child.is_shared for child in poc_folder.folders))

    def test_build_notebooks_uses_discovered_relations_for_smoke_presets(
        self,
    ) -> None:
        (
            _,
            _,
            build_notebooks,
            _,
            _,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_notebook_helpers()

        catalogs = [
            source_catalog_type(
                name="workspace",
                schemas=[
                    source_schema_type(
                        name="s3",
                        objects=[
                            source_object_type(
                                name="vat_smoke",
                                kind="view",
                                relation="workspace.s3.vat_smoke_generated",
                                s3_key="generated/vat_smoke/part-0001.parquet",
                            )
                        ],
                    )
                ],
            ),
            source_catalog_type(
                name="pg_oltp",
                schemas=[
                    source_schema_type(
                        name="public",
                        objects=[
                            source_object_type(
                                name="vat_smoke_test_reference",
                                kind="table",
                                relation=(
                                    "pg_oltp.public."
                                    "vat_smoke_test_reference"
                                ),
                            )
                        ],
                    )
                ],
            ),
        ]

        notebooks = {
            notebook.notebook_id: notebook
            for notebook in build_notebooks(catalogs)
        }

        self.assertIn(
            "FROM workspace.s3.vat_smoke_generated",
            notebooks["s3-smoke-test"].cells[0].sql,
        )
        self.assertIn(
            "FROM pg_oltp.public.vat_smoke_test_reference",
            notebooks["postgres-smoke-test"].cells[0].sql,
        )

    def test_build_notebooks_includes_mwa_multi_format_presets(
        self,
    ) -> None:
        (
            _,
            _,
            build_notebooks,
            _,
            _,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_notebook_helpers()

        catalogs = [
            source_catalog_type(
                name="pg_oltp",
                schemas=[
                    source_schema_type(
                        name="public",
                        objects=[
                            source_object_type(
                                name="mwa_abrechnung_entities",
                                kind="table",
                                relation="pg_oltp.public.mwa_abrechnung_entities",
                            ),
                            source_object_type(
                                name="mwa_abrechnungs_ziffern_entities",
                                kind="table",
                                relation="pg_oltp.public.mwa_abrechnungs_ziffern_entities",
                            ),
                        ],
                    )
                ],
            ),
            source_catalog_type(
                name="workspace",
                schemas=[
                    source_schema_type(
                        name="mwa",
                        objects=[
                            source_object_type(
                                name="mwa_abrechnung_entities_parquet",
                                kind="view",
                                relation="workspace.mwa.mwa_abrechnung_entities_parquet",
                            ),
                            source_object_type(
                                name="mwa_abrechnungs_ziffern_entities_parquet",
                                kind="view",
                                relation=(
                                    "workspace.mwa."
                                    "mwa_abrechnungs_ziffern_entities_parquet"
                                ),
                            ),
                            source_object_type(
                                name="mwa_abrechnung_entities_csv",
                                kind="view",
                                relation="workspace.mwa.mwa_abrechnung_entities_csv",
                            ),
                            source_object_type(
                                name="mwa_abrechnungs_ziffern_entities_csv",
                                kind="view",
                                relation="workspace.mwa.mwa_abrechnungs_ziffern_entities_csv",
                            ),
                            source_object_type(
                                name="mwa_abrechnung_entities_json",
                                kind="view",
                                relation="workspace.mwa.mwa_abrechnung_entities_json",
                            ),
                            source_object_type(
                                name="mwa_abrechnungs_ziffern_entities_json",
                                kind="view",
                                relation="workspace.mwa.mwa_abrechnungs_ziffern_entities_json",
                            ),
                        ],
                    )
                ],
            ),
        ]

        notebooks = {
            notebook.notebook_id: notebook
            for notebook in build_notebooks(catalogs)
        }

        self.assertEqual(
            notebooks["mwa-abrechnung-oltp"].cells[0].data_sources,
            ["pg_oltp"],
        )
        self.assertEqual(
            notebooks["mwa-abrechnung-oltp"].title,
            "MWA Abrechnung (3.2) OLTP via DuckDB",
        )
        self.assertEqual(
            notebooks["mwa-abrechnung-oltp"].tree_path,
            ("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
        )
        self.assertIn(
            "FROM pg_oltp.public.mwa_abrechnung_entities",
            notebooks["mwa-abrechnung-oltp"].cells[0].sql,
        )
        self.assertEqual(
            notebooks["mwa-abrechnung-pg-native"].cells[0].data_sources,
            ["pg_oltp_native"],
        )
        self.assertIn(
            "FROM public.mwa_abrechnung_entities",
            notebooks["mwa-abrechnung-pg-native"].cells[0].sql,
        )
        self.assertIn(
            "FROM workspace.mwa.mwa_abrechnung_entities_parquet",
            notebooks["mwa-abrechnung-s3-parquet"].cells[0].sql,
        )
        self.assertIn(
            "FROM workspace.mwa.mwa_abrechnung_entities_csv",
            notebooks["mwa-abrechnung-s3-csv"].cells[0].sql,
        )
        self.assertIn(
            "FROM workspace.mwa.mwa_abrechnung_entities_json",
            notebooks["mwa-abrechnung-s3-json"].cells[0].sql,
        )
        self.assertEqual(
            {
                notebooks["mwa-abrechnung-oltp"].linked_generator_id,
                notebooks["mwa-abrechnung-pg-native"].linked_generator_id,
                notebooks["mwa-abrechnung-s3-parquet"].linked_generator_id,
                notebooks["mwa-abrechnung-s3-csv"].linked_generator_id,
                notebooks["mwa-abrechnung-s3-json"].linked_generator_id,
            },
            {"mwa_abrechnung_multi_format_loader"},
        )

    def test_build_notebooks_includes_immutable_python_demo_presets(
        self,
    ) -> None:
        (
            _,
            _,
            build_notebooks,
            _,
            _,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_notebook_helpers()

        postgres_relation = "pg_oltp.public.vat_smoke_test_reference"
        catalogs = [
            source_catalog_type(
                name="pg_oltp",
                schemas=[
                    source_schema_type(
                        name="public",
                        objects=[
                            source_object_type(
                                name="vat_smoke_test_reference",
                                kind="table",
                                relation=postgres_relation,
                            )
                        ],
                    )
                ],
            )
        ]

        notebooks = {
            notebook.notebook_id: notebook
            for notebook in build_notebooks(catalogs)
        }

        pandas_demo = notebooks["python-pandas-vat-demo"]
        self.assertEqual(
            pandas_demo.tree_path,
            ("PoC Tests", "General Functionalities"),
        )
        self.assertFalse(pandas_demo.can_edit)
        self.assertFalse(pandas_demo.can_delete)
        self.assertTrue(pandas_demo.shared)
        self.assertEqual(
            [cell.language for cell in pandas_demo.cells],
            ["sql", "python", "python"],
        )
        self.assertTrue(
            all(cell.data_sources == ["pg_oltp"] for cell in pandas_demo.cells)
        )
        self.assertIn(
            f'vat_df = source("{postgres_relation}").df()',
            pandas_demo.cells[1].sql,
        )
        self.assertIn(
            'quarter=vat_df["tax_period_end"].dt.to_period("Q").astype(str)',
            pandas_demo.cells[2].sql,
        )

        chart_demo = notebooks["python-chart-vat-demo"]
        self.assertEqual(
            chart_demo.tree_path,
            ("PoC Tests", "General Functionalities"),
        )
        self.assertFalse(chart_demo.can_edit)
        self.assertFalse(chart_demo.can_delete)
        self.assertTrue(chart_demo.shared)
        self.assertEqual(
            [cell.language for cell in chart_demo.cells],
            ["python", "python"],
        )
        self.assertTrue(
            all(cell.data_sources == ["pg_oltp"] for cell in chart_demo.cells)
        )
        self.assertIn(
            f"FROM {postgres_relation}",
            chart_demo.cells[0].sql,
        )
        self.assertIn(
            "import matplotlib.pyplot as plt",
            chart_demo.cells[1].sql,
        )


if __name__ == "__main__":
    unittest.main()

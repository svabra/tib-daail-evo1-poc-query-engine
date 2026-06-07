from __future__ import annotations

from pathlib import Path
import re
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


def import_restart_seed_helpers():
    from bit_data_workbench.backend.notebooks import (
        build_restart_seeded_shared_notebooks,
    )
    from bit_data_workbench.backend.materialized_stages import (
        build_notebook_stage_graph,
    )
    from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema

    return (
        build_restart_seeded_shared_notebooks,
        build_notebook_stage_graph,
        SourceCatalog,
        SourceObject,
        SourceSchema,
    )


def import_completion_schema_helpers():
    from bit_data_workbench.backend.notebooks import build_completion_schema
    from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema

    return build_completion_schema, SourceCatalog, SourceObject, SourceSchema


class CompletionSchemaTests(unittest.TestCase):
    def test_completion_schema_includes_s3_query_alias_paths(self) -> None:
        (
            build_completion_schema,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_completion_schema_helpers()

        catalogs = [
            source_catalog_type(
                name="workspace",
                schemas=[
                    source_schema_type(
                        name="vat_smoke_test",
                        objects=[
                            source_object_type(
                                name="tax_csv",
                                kind="view",
                                relation="vat_smoke_test.tax_csv",
                                query_alias=(
                                    "s3.vat_smoke_test.incoming.tax_data.csv"
                                ),
                                query_reference='s3.vat_smoke_test."incoming/tax-data.csv"',
                                s3_bucket="vat_smoke_test",
                                s3_key="incoming/tax-data.csv",
                                s3_file_format="csv",
                            ),
                            source_object_type(
                                name="tax_parquet",
                                kind="view",
                                relation="vat_smoke_test.tax_parquet",
                                query_alias=(
                                    "s3.vat_smoke_test.generated.vat_smoke."
                                    "part_00001.parquet"
                                ),
                                query_reference=(
                                    's3.vat_smoke_test."generated/vat_smoke/'
                                    'part_00001.parquet"'
                                ),
                                s3_bucket="vat_smoke_test",
                                s3_key="generated/vat_smoke/part_00001.parquet",
                                s3_file_format="parquet",
                            ),
                            source_object_type(
                                name="tax_csv_duplicate",
                                kind="view",
                                relation="vat_smoke_test.tax_csv_duplicate",
                                query_alias=(
                                    "s3.vat_smoke_test.incoming.tax_data.csv"
                                ),
                            ),
                            source_object_type(
                                name="invalid_alias",
                                kind="view",
                                relation="vat_smoke_test.invalid_alias",
                                query_alias="s3.vat_smoke_test",
                            ),
                            source_object_type(
                                name="local_alias",
                                kind="view",
                                relation="vat_smoke_test.local_alias",
                                query_alias="local.folder.local_alias.csv",
                            ),
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
                                name="sales_orders",
                                kind="table",
                                relation="pg_oltp.public.sales_orders",
                            )
                        ],
                    )
                ],
            ),
        ]

        schema = build_completion_schema(catalogs)

        self.assertEqual(
            schema["vat_smoke_test"],
            [
                "tax_csv",
                "tax_parquet",
                "tax_csv_duplicate",
                "invalid_alias",
                "local_alias",
            ],
        )
        self.assertEqual(
            schema["pg_oltp"],
            {"public": ["sales_orders"]},
        )
        self.assertEqual(
            [item["label"] for item in schema["s3References"]],
            [
                's3.vat_smoke_test."generated/vat_smoke/part_00001.parquet"',
                's3.vat_smoke_test."incoming/tax-data.csv"',
            ],
        )
        self.assertEqual(
            [item["label"] for item in schema["pgReferences"]],
            ['pg.pg_oltp."public.sales_orders"'],
        )
        self.assertNotIn("local", schema)

    def test_completion_schema_merges_workspace_schema_named_s3(self) -> None:
        (
            build_completion_schema,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_completion_schema_helpers()

        schema = build_completion_schema(
            [
                source_catalog_type(
                    name="workspace",
                    schemas=[
                        source_schema_type(
                            name="s3",
                            objects=[
                                source_object_type(
                                    name="legacy_relation",
                                    kind="view",
                                    relation="s3.legacy_relation",
                                ),
                                source_object_type(
                                    name="path_relation",
                                    kind="view",
                                    relation="s3.path_relation",
                                    query_alias="s3.bucket.folder.file.csv",
                                ),
                            ],
                        )
                    ],
                )
            ]
        )

        self.assertEqual(schema["s3"], ["legacy_relation", "path_relation"])
        self.assertNotIn("s3References", schema)


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

    def test_all_registered_loaders_have_linked_notebooks_with_simple_source_refs(
        self,
    ) -> None:
        (
            build_generator_notebook_links,
            _,
            build_notebooks,
            _,
            _,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_notebook_helpers()
        from bit_data_workbench.backend.source_references import (  # noqa: WPS433
            pg_source_reference,
            s3_source_reference,
        )
        from bit_data_workbench.data_generator.registry import DataGeneratorRegistry  # noqa: WPS433

        def pg_object(name: str, *, source_id: str = "pg_oltp"):
            relation = f"{source_id}.public.{name}"
            return source_object_type(
                name=name,
                kind="table",
                relation=relation,
                query_reference=pg_source_reference(source_id=source_id, relation=relation),
            )

        def s3_object(name: str, *, schema: str = "workspace_s3"):
            key = f"generated/{name}/parquet/{name}/part-00001.parquet"
            return source_object_type(
                name=name,
                kind="view",
                relation=f"{schema}.{name}",
                query_reference=s3_source_reference(bucket=schema, key=key),
                s3_bucket=schema,
                s3_key=key,
                s3_path=f"s3://{schema}/{key}",
                s3_file_format="parquet",
            )

        multi_table_names = (
            "federal_tax_taxpayers_mt",
            "federal_tax_filings_mt",
            "federal_tax_assessments_mt",
            "federal_tax_payments_mt",
            "federal_tax_audits_mt",
            "federal_tax_enforcements_mt",
            "federal_tax_appeals_mt",
        )
        kostenbelege_names = ("kbkp_2019", "kbpo_2019", "kbhp_2019", "dim_kalender")
        mwa_names = ("mwa_abrechnung_entities", "mwa_abrechnungs_ziffern_entities")
        parquet_option_names = (
            "federal_tax_parquet_off",
            "federal_tax_parquet_recommended",
            "federal_tax_parquet_manual_partition",
            "federal_tax_parquet_manual_hive",
            "federal_tax_parquet_manual_cache",
        )

        catalogs = [
            source_catalog_type(
                name="pg_oltp",
                connection_source_id="pg_oltp",
                schemas=[
                    source_schema_type(
                        name="public",
                        objects=[
                            pg_object("vat_smoke_test_reference"),
                            pg_object("tax_assessment_pg_vs_s3"),
                            pg_object("pg_union_tax_reference"),
                            pg_object("pg_union_tax_reference_s3"),
                            *[pg_object(name) for name in multi_table_names],
                            *[pg_object(name) for name in mwa_names],
                            *[pg_object(name) for name in kostenbelege_names],
                        ],
                    )
                ],
            ),
            source_catalog_type(
                name="pg_olap",
                connection_source_id="pg_olap",
                schemas=[
                    source_schema_type(
                        name="public",
                        objects=[
                            pg_object("tax_assessment_olap_smoke", source_id="pg_olap"),
                            pg_object("pg_union_tax_reference", source_id="pg_olap"),
                            *[
                                pg_object(name, source_id="pg_olap")
                                for name in kostenbelege_names
                            ],
                        ],
                    )
                ],
            ),
            source_catalog_type(
                name="workspace",
                connection_source_id="workspace.s3",
                schemas=[
                    source_schema_type(
                        name="workspace_s3",
                        objects=[
                            s3_object("vat_smoke"),
                            s3_object("tax_assessment_pg_vs_s3"),
                            s3_object("pg_union_tax_reference_s3"),
                            *[s3_object(name) for name in multi_table_names],
                            *[s3_object(name) for name in parquet_option_names],
                            *[
                                s3_object(f"{name}_{suffix}")
                                for name in mwa_names
                                for suffix in ("parquet", "csv", "json")
                            ],
                        ],
                    ),
                    source_schema_type(
                        name="s3_3_1_imports_a08e7385",
                        objects=[
                            s3_object(name, schema="s3_3_1_imports_a08e7385")
                            for name in kostenbelege_names
                        ],
                    ),
                ],
            ),
        ]

        notebooks = build_notebooks(catalogs)
        links = build_generator_notebook_links(notebooks)
        registered_loader_ids = {
            generator.generator_id for generator in DataGeneratorRegistry().discover()
        }

        self.assertEqual(registered_loader_ids - set(links), set())

        linked_notebooks = [
            notebook
            for notebook in notebooks
            if notebook.linked_generator_id in registered_loader_ids
        ]
        self.assertGreaterEqual(len(linked_notebooks), len(registered_loader_ids))
        legacy_virtual_s3_pattern = re.compile(
            r"\bs3\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+"
        )
        for notebook in linked_notebooks:
            sql = "\n".join(cell.sql for cell in notebook.cells)
            self.assertNotRegex(sql, legacy_virtual_s3_pattern, notebook.notebook_id)
            if "s3." in sql:
                self.assertIn('"generated/', sql, notebook.notebook_id)

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
            build_generator_notebook_links,
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
            build_generator_notebook_links,
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
        self.assertEqual(len(notebooks["mwa-abrechnung-s3-parquet"].cells), 2)
        self.assertIn(
            "CREATE INDEX mwa_abrechnung_art_id_idx",
            notebooks["mwa-abrechnung-s3-parquet"].cells[1].sql,
        )
        self.assertIn(
            "EXPLAIN ANALYZE",
            notebooks["mwa-abrechnung-s3-parquet"].cells[1].sql,
        )
        self.assertIn(
            "FROM workspace.mwa.mwa_abrechnung_entities_parquet",
            notebooks["mwa-abrechnung-s3-parquet"].cells[1].sql,
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

    def test_restart_seeded_mwa_parquet_pipeline_is_editable_and_forked(
        self,
    ) -> None:
        (
            build_restart_seeded_shared_notebooks,
            build_stage_graph,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_restart_seed_helpers()

        catalogs = [
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
                                name="mwa_abrechnung_entities_json",
                                kind="view",
                                relation="workspace.mwa.mwa_abrechnung_entities_json",
                            ),
                        ],
                    )
                ],
            )
        ]

        notebooks = {
            notebook.notebook_id: notebook
            for notebook in build_restart_seeded_shared_notebooks(catalogs)
        }
        self.assertIn("mwa-abrechnung-s3-parquet-pipeline", notebooks)

        notebook = notebooks["mwa-abrechnung-s3-parquet-pipeline"]
        self.assertEqual(
            notebook.notebook_id,
            "mwa-abrechnung-s3-parquet-pipeline",
        )
        self.assertTrue(notebook.can_edit)
        self.assertTrue(notebook.can_delete)
        self.assertTrue(notebook.shared)
        self.assertEqual(notebook.pipeline_mode, "pipeline")
        self.assertEqual(
            notebook.tree_path,
            ("PoC Tests", "Performance Evaluation", "Data Pipelines"),
        )
        self.assertEqual(len(notebook.cells), 5)
        self.assertEqual(notebook.cells[0].data_sources, ["workspace.s3"])
        self.assertEqual(notebook.cells[1].data_sources, ["workspace.s3"])
        self.assertEqual(notebook.cells[2].data_sources, [])

        all_sql = "\n".join(cell.sql for cell in notebook.cells)
        self.assertIn("workspace.mwa.mwa_abrechnung_entities_parquet", all_sql)
        self.assertIn(
            "workspace.mwa.mwa_abrechnungs_ziffern_entities_parquet",
            all_sql,
        )
        self.assertNotIn("_csv", all_sql)
        self.assertNotIn("_json", all_sql)

        graph = build_stage_graph(
            notebook_id=notebook.notebook_id,
            notebook_title=notebook.title,
            cells=notebook.cells_payload,
        )
        self.assertEqual(graph["diagnostics"], [])
        self.assertEqual(
            graph["order"],
            [
                "stage-mwa-abrechnung-scope",
                "stage-mwa-ziffer-rollup",
                "stage-mwa-joined-abrechnungen",
                "stage-mwa-status-pressure",
                "stage-mwa-audit-backlog",
            ],
        )
        node_by_id = {node["stageId"]: node for node in graph["nodes"]}
        self.assertEqual(
            node_by_id["stage-mwa-joined-abrechnungen"]["predecessorStageIds"],
            ["stage-mwa-abrechnung-scope", "stage-mwa-ziffer-rollup"],
        )
        self.assertEqual(
            set(node_by_id["stage-mwa-joined-abrechnungen"]["successorStageIds"]),
            {"stage-mwa-status-pressure", "stage-mwa-audit-backlog"},
        )
        self.assertEqual(
            node_by_id["stage-mwa-status-pressure"]["kind"],
            "final",
        )
        self.assertEqual(
            node_by_id["stage-mwa-audit-backlog"]["kind"],
            "final",
        )

    def test_restart_seeded_kostenbelege_parquet_pipeline_is_editable_and_forked(
        self,
    ) -> None:
        (
            build_restart_seeded_shared_notebooks,
            build_stage_graph,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_restart_seed_helpers()
        table_names = (
            "kbkp_2019",
            "kbpo_2019",
            "kbhp_2019",
            "dim_kalender",
        )
        catalogs = [
            source_catalog_type(
                name="workspace",
                schemas=[
                    source_schema_type(
                        name="s3_3_1_imports_a08e7385",
                        objects=[
                            source_object_type(
                                name=table_name,
                                kind="view",
                                relation=f"workspace.s3_3_1_imports_a08e7385.{table_name}",
                            )
                            for table_name in table_names
                        ],
                    )
                ],
            )
        ]

        notebooks = {
            notebook.notebook_id: notebook
            for notebook in build_restart_seeded_shared_notebooks(catalogs)
        }
        notebook = notebooks["kostenbelege-3-1-s3-parquet-pipeline"]

        self.assertTrue(notebook.can_edit)
        self.assertTrue(notebook.can_delete)
        self.assertTrue(notebook.shared)
        self.assertEqual(notebook.pipeline_mode, "pipeline")
        self.assertEqual(
            notebook.tree_path,
            ("PoC Tests", "Performance Evaluation", "Data Pipelines"),
        )
        self.assertEqual(len(notebook.cells), 9)
        self.assertEqual(notebook.cells[0].data_sources, ["workspace.s3"])
        self.assertEqual(notebook.cells[1].data_sources, ["workspace.s3"])
        self.assertEqual(notebook.cells[2].data_sources, ["workspace.s3"])
        self.assertTrue(
            all(str(cell.stage.get("description") or "").strip() for cell in notebook.cells)
        )

        all_sql = "\n".join(cell.sql for cell in notebook.cells)
        self.assertIn("workspace.s3_3_1_imports_a08e7385.kbkp_2019", all_sql)
        self.assertIn("workspace.s3_3_1_imports_a08e7385.kbpo_2019", all_sql)
        self.assertIn("workspace.s3_3_1_imports_a08e7385.kbhp_2019", all_sql)
        self.assertIn("workspace.s3_3_1_imports_a08e7385.dim_kalender", all_sql)
        self.assertIn("stage.kb_original_positions", all_sql)
        self.assertIn("stage.kb_settlement_positions", all_sql)
        self.assertNotIn("_csv", all_sql)
        self.assertNotIn("_json", all_sql)
        self.assertNotIn("pg_oltp", all_sql)
        self.assertNotIn("pg_olap", all_sql)
        self.assertNotIn("public.kb", all_sql)
        canonical_output_sql = next(
            cell.sql
            for cell in notebook.cells
            if cell.stage.get("stageId") == "stage-kb-canonical-output"
        )
        audit_sql = next(
            cell.sql
            for cell in notebook.cells
            if cell.stage.get("stageId") == "stage-kb-settlement-exception-candidates"
        )
        self.assertNotIn("LedgerResolution", canonical_output_sql)
        self.assertIn("LedgerResolution", audit_sql)

        graph = build_stage_graph(
            notebook_id=notebook.notebook_id,
            notebook_title=notebook.title,
            cells=notebook.cells_payload,
        )
        self.assertEqual(graph["diagnostics"], [])
        self.assertEqual(
            graph["order"],
            [
                "stage-kb-current-headers",
                "stage-kb-current-positions",
                "stage-kb-current-ledger-accounts",
                "stage-kb-resolved-positions",
                "stage-kb-original-positions",
                "stage-kb-settlement-positions",
                "stage-kb-canonical-output",
                "stage-kb-settlement-exception-candidates",
                "stage-kb-settlement-audit-backlog",
            ],
        )
        self.assertEqual(
            [path["terminalStageId"] for path in graph["paths"]],
            [
                "stage-kb-canonical-output",
                "stage-kb-settlement-audit-backlog",
            ],
        )
        node_by_id = {node["stageId"]: node for node in graph["nodes"]}
        self.assertEqual(
            set(node_by_id["stage-kb-resolved-positions"]["predecessorStageIds"]),
            {
                "stage-kb-current-headers",
                "stage-kb-current-positions",
                "stage-kb-current-ledger-accounts",
            },
        )
        self.assertEqual(
            set(node_by_id["stage-kb-resolved-positions"]["successorStageIds"]),
            {"stage-kb-original-positions", "stage-kb-settlement-positions"},
        )
        self.assertEqual(node_by_id["stage-kb-canonical-output"]["kind"], "final")
        self.assertEqual(
            node_by_id["stage-kb-settlement-audit-backlog"]["kind"],
            "final",
        )

    def test_restart_seeded_kostenbelege_pipeline_uses_generated_s3_folders(
        self,
    ) -> None:
        (
            build_restart_seeded_shared_notebooks,
            _build_stage_graph,
            source_catalog_type,
            _source_object_type,
            source_schema_type,
        ) = import_restart_seed_helpers()
        from bit_data_workbench.backend.s3_storage import s3_bucket_schema_name

        bucket = "poc-tests-performance-evaluation-kostenbelege-3-1"
        catalogs = [
            source_catalog_type(
                name="workspace",
                schemas=[
                    source_schema_type(
                        name=s3_bucket_schema_name(bucket),
                        objects=[],
                    )
                ],
            )
        ]

        notebooks = {
            notebook.notebook_id: notebook
            for notebook in build_restart_seeded_shared_notebooks(catalogs)
        }
        notebook = notebooks["kostenbelege-3-1-s3-parquet-pipeline"]
        all_sql = "\n".join(cell.sql for cell in notebook.cells)

        self.assertEqual(len(notebook.cells), 9)
        self.assertNotIn("Run the Kostenbelege Multi-Source Loader", all_sql)
        for table_name in ("kbkp_2019", "kbpo_2019", "kbhp_2019", "dim_kalender"):
            self.assertIn(
                f"read_parquet('s3://{bucket}/generated/kostenbelege_3_1/parquet/{table_name}/*.parquet'",
                all_sql,
            )
        self.assertIn("hive_partitioning=false", all_sql)

    def test_build_notebooks_includes_parquet_performance_options_presets(
        self,
    ) -> None:
        (
            build_generator_notebook_links,
            _,
            build_notebooks,
            _,
            _,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_notebook_helpers()

        object_names = (
            "federal_tax_parquet_off",
            "federal_tax_parquet_recommended",
            "federal_tax_parquet_manual_partition",
            "federal_tax_parquet_manual_hive",
            "federal_tax_parquet_manual_cache",
        )
        catalogs = [
            source_catalog_type(
                name="workspace",
                schemas=[
                    source_schema_type(
                        name="poc_tests_performance_options",
                        objects=[
                            source_object_type(
                                name=object_name,
                                kind="view",
                                relation=f"poc_tests_performance_options.{object_name}",
                            )
                            for object_name in object_names
                        ],
                    )
                ],
            )
        ]

        notebooks = {
            notebook.notebook_id: notebook
            for notebook in build_notebooks(catalogs)
        }

        expected = {
            "federal-tax-parquet-optimization-off": (
                "federal_tax_parquet_off",
                "parquet_performance_options_off_loader",
                "auto",
            ),
            "federal-tax-parquet-optimization-recommended": (
                "federal_tax_parquet_recommended",
                "parquet_performance_options_recommended_loader",
                "auto",
            ),
            "federal-tax-parquet-optimization-manual-no-hive": (
                "federal_tax_parquet_manual_partition",
                "parquet_performance_options_manual_partition_no_hive_loader",
                "off",
            ),
            "federal-tax-parquet-optimization-manual-hive": (
                "federal_tax_parquet_manual_hive",
                "parquet_performance_options_manual_partition_hive_loader",
                "on",
            ),
            "federal-tax-parquet-optimization-manual-cache": (
                "federal_tax_parquet_manual_cache",
                "parquet_performance_options_manual_cache_only_loader",
                "auto",
            ),
        }

        for notebook_id, (object_name, generator_id, hive_option) in expected.items():
            notebook = notebooks[notebook_id]
            self.assertEqual(notebook.tree_path, ("PoC Tests", "Performance Options"))
            self.assertFalse(notebook.can_edit)
            self.assertFalse(notebook.can_delete)
            self.assertTrue(notebook.shared)
            self.assertEqual(notebook.linked_generator_id, generator_id)
            self.assertIn(
                f"FROM poc_tests_performance_options.{object_name}",
                notebook.cells[0].sql,
            )
            self.assertIn("WHERE tax_year = 2025", notebook.cells[0].sql)
            self.assertEqual(
                notebook.cells[0].query_options["duckdb"]["parquetHivePartitioning"],
                hive_option,
            )

        cache_notebook = notebooks["federal-tax-parquet-optimization-manual-cache"]
        self.assertEqual(len(cache_notebook.cells), 2)
        self.assertIn(
            "FROM poc_tests_performance_options.federal_tax_parquet_manual_cache_duckdb_cache",
            cache_notebook.cells[0].sql,
        )
        self.assertIn(
            "FROM poc_tests_performance_options.federal_tax_parquet_manual_cache_duckdb_cache",
            cache_notebook.cells[1].sql,
        )
        self.assertIn("WHERE taxpayer_id = 'TX-100001'", cache_notebook.cells[1].sql)
        self.assertIn("Loader-created DuckDB ART cache lookup", cache_notebook.cells[1].sql)

        linked = build_generator_notebook_links(notebooks.values())
        for notebook_id, (_object_name, generator_id, _hive_option) in expected.items():
            self.assertEqual(
                [reference.notebook_id for reference in linked[generator_id]],
                [notebook_id],
            )

    def test_build_notebooks_includes_kostenbelege_3_1_presets(
        self,
    ) -> None:
        (
            build_generator_notebook_links,
            _,
            build_notebooks,
            _,
            _,
            source_catalog_type,
            source_object_type,
            source_schema_type,
        ) = import_notebook_helpers()

        table_names = ("kbkp_2019", "kbpo_2019", "kbhp_2019", "dim_kalender")
        catalogs = [
            source_catalog_type(
                name="pg_oltp",
                schemas=[
                    source_schema_type(
                        name="public",
                        objects=[
                            source_object_type(
                                name=table_name,
                                kind="table",
                                relation=f"pg_oltp.public.{table_name}",
                            )
                            for table_name in table_names
                        ],
                    )
                ],
            ),
            source_catalog_type(
                name="pg_olap",
                schemas=[
                    source_schema_type(
                        name="public",
                        objects=[
                            source_object_type(
                                name=table_name,
                                kind="table",
                                relation=f"pg_olap.public.{table_name}",
                            )
                            for table_name in table_names
                        ],
                    )
                ],
            ),
            source_catalog_type(
                name="workspace",
                schemas=[
                    source_schema_type(
                        name="s3_3_1_imports_a08e7385",
                        objects=[
                            source_object_type(
                                name=table_name,
                                kind="view",
                                relation=f"workspace.s3_3_1_imports_a08e7385.{table_name}",
                            )
                            for table_name in table_names
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
            notebooks["kostenbelege-3-1-oltp"].cells[0].data_sources,
            ["pg_oltp"],
        )
        self.assertEqual(
            notebooks["kostenbelege-3-1-oltp"].tree_path,
            ("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
        )
        self.assertEqual(
            notebooks["kostenbelege-3-1-oltp-native"].cells[0].data_sources,
            ["pg_oltp_native"],
        )
        self.assertEqual(
            notebooks["kostenbelege-3-1-olap-native"].cells[0].data_sources,
            ["pg_olap_native"],
        )
        self.assertIn(
            "FROM pg_oltp.public.kbkp_2019",
            notebooks["kostenbelege-3-1-oltp"].cells[0].sql,
        )
        self.assertIn(
            "INNER JOIN pg_olap.public.kbpo_2019",
            notebooks["kostenbelege-3-1-olap"].cells[0].sql,
        )
        self.assertIn(
            "FROM workspace.s3_3_1_imports_a08e7385.kbkp_2019",
            notebooks["kostenbelege-3-1-s3-parquet"].cells[0].sql,
        )
        self.assertEqual(
            notebooks["kostenbelege-3-1-s3-parquet-optimized"].cells[0].data_sources,
            ["workspace.s3"],
        )
        self.assertIn(
            "WITH current_kalender AS",
            notebooks["kostenbelege-3-1-s3-parquet-optimized"].cells[0].sql,
        )
        self.assertIn(
            "resolved_positions AS",
            notebooks["kostenbelege-3-1-s3-parquet-optimized"].cells[0].sql,
        )
        self.assertIn(
            "CROSS JOIN (VALUES",
            notebooks["kostenbelege-3-1-s3-parquet-optimized"].cells[0].sql,
        )
        self.assertIn(
            'FROM public.kbkp_2019 KBKP',
            notebooks["kostenbelege-3-1-oltp-native"].cells[0].sql,
        )
        self.assertIn(
            'KALE."Datum" BETWEEN KBKP."KBKP_TechBeginnDt"',
            notebooks["kostenbelege-3-1-oltp-native"].cells[0].sql,
        )
        self.assertEqual(
            {
                notebooks["kostenbelege-3-1-oltp"].linked_generator_id,
                notebooks["kostenbelege-3-1-olap"].linked_generator_id,
                notebooks["kostenbelege-3-1-s3-parquet"].linked_generator_id,
                notebooks["kostenbelege-3-1-s3-parquet-optimized"].linked_generator_id,
                notebooks["kostenbelege-3-1-oltp-native"].linked_generator_id,
                notebooks["kostenbelege-3-1-olap-native"].linked_generator_id,
            },
            {"kostenbelege_3_1_multi_source_loader"},
        )
        self.assertEqual(
            [
                reference.payload
                for reference in build_generator_notebook_links(notebooks.values())[
                    "kostenbelege_3_1_multi_source_loader"
                ]
            ],
            [
                {
                    "notebookId": "kostenbelege-3-1-oltp",
                    "title": "Kostenbelege (3.1) OLTP via DuckDB",
                },
                {
                    "notebookId": "kostenbelege-3-1-olap",
                    "title": "Kostenbelege (3.1) OLAP via DuckDB",
                },
                {
                    "notebookId": "kostenbelege-3-1-s3-parquet",
                    "title": "Kostenbelege (3.1) S3 Parquet via DuckDB",
                },
                {
                    "notebookId": "kostenbelege-3-1-s3-parquet-optimized",
                    "title": "Kostenbelege (3.1) S3 Parquet Optimized via DuckDB",
                },
                {
                    "notebookId": "kostenbelege-3-1-oltp-native",
                    "title": "Kostenbelege (3.1) OLTP via Native PostgreSQL",
                },
                {
                    "notebookId": "kostenbelege-3-1-olap-native",
                    "title": "Kostenbelege (3.1) OLAP via Native PostgreSQL",
                },
            ],
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

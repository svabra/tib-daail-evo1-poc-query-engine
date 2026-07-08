from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import threading
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.notebook_presets import (  # noqa: E402
    build_kostenbelege_fact_builder_s3_demo_notebook,
    build_kostenbelege_fact_builder_s3_pipeline_notebook,
)
from bit_data_workbench.backend.notebooks import build_notebooks  # noqa: E402
from bit_data_workbench.backend.source_references import s3_source_reference  # noqa: E402
from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402
from bit_data_workbench.data_generator.kostenbelege_fact_builder_sample import (  # noqa: E402
    GENERATOR,
    KBHP_COLUMNS,
    KBHP_SOURCE_KEY,
    KBKP_COLUMNS,
    KBKP_SOURCE_KEY,
    KBPO_COLUMNS,
    KBPO_SOURCE_KEYS,
    KOSTENBELEGE_FACT_BUILDER_BUCKET,
    KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID,
    KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID,
    KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID,
    KOSTENBELEGE_FACT_BUILDER_TREE_PATH,
    RESULT_SET_KEYS,
    kbhp_full_select,
    kbkp_full_select,
    kbpo_slice_select,
    result_path,
)
from bit_data_workbench.data_generator.registry import DataGeneratorRegistry  # noqa: E402


def virtual_reference(key: str) -> str:
    return s3_source_reference(
        bucket=KOSTENBELEGE_FACT_BUILDER_BUCKET,
        key=key,
    )


def result_reference(result_name: str) -> str:
    return virtual_reference(RESULT_SET_KEYS[result_name])


def table_names(columns: tuple[str, ...]) -> list[str]:
    return [column.split(" ", 1)[0] for column in columns]


def create_fact_builder_source_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    document_count: int = 32,
    kbpo_rows: int = 64,
    kbhp_rows: int = 128,
) -> dict[str, str]:
    source_table_by_reference = {
        virtual_reference(KBKP_SOURCE_KEY): "src_kbkpfull",
        virtual_reference(KBHP_SOURCE_KEY): "src_kbhpfull",
    }
    connection.execute(
        f"CREATE TEMP TABLE src_kbkpfull AS {kbkp_full_select(0, document_count)}"
    )
    connection.execute(
        f"CREATE TEMP TABLE src_kbhpfull AS {kbhp_full_select(0, kbhp_rows, document_count)}"
    )
    batch_size = kbpo_rows // len(KBPO_SOURCE_KEYS)
    for index, source_key in enumerate(KBPO_SOURCE_KEYS):
        table_name = f"src_kbpo_{index}"
        start_row = index * batch_size
        source_table_by_reference[virtual_reference(source_key)] = table_name
        connection.execute(
            "CREATE TEMP TABLE "
            f"{table_name} AS "
            f"{kbpo_slice_select(0, batch_size, document_count, row_offset=start_row)}"
        )
    return source_table_by_reference


def translate_sql_references(sql: str, mappings: dict[str, str]) -> str:
    translated_sql = sql
    for reference, table_name in sorted(
        mappings.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        translated_sql = translated_sql.replace(reference, table_name)
    return translated_sql.rstrip().rstrip(";")


def execute_exploration_notebook(
    connection: duckdb.DuckDBPyConnection,
    source_table_by_reference: dict[str, str],
) -> dict[str, str]:
    notebook = build_kostenbelege_fact_builder_s3_demo_notebook()
    result_table_by_reference = {
        result_reference(result_name): f"stored_{result_name}"
        for result_name in RESULT_SET_KEYS
    }

    for cell, result_name in zip(notebook.cells, RESULT_SET_KEYS, strict=True):
        sql = translate_sql_references(
            cell.sql,
            {
                **source_table_by_reference,
                **result_table_by_reference,
            },
        )
        result_table = result_table_by_reference[result_reference(result_name)]
        connection.execute(f"CREATE TEMP TABLE {result_table} AS {sql}")

    return result_table_by_reference


def execute_pipeline_notebook(
    connection: duckdb.DuckDBPyConnection,
    source_table_by_reference: dict[str, str],
) -> dict[str, str]:
    notebook = build_kostenbelege_fact_builder_s3_pipeline_notebook()
    stage_table_by_reference: dict[str, str] = {}

    for cell in notebook.cells:
        stage = cell.stage
        stage_alias = str(stage["alias"])
        stage_table = f"pipeline_{stage_alias}"
        sql = translate_sql_references(
            cell.sql,
            {
                **source_table_by_reference,
                **stage_table_by_reference,
            },
        )
        connection.execute(f"CREATE TEMP TABLE {stage_table} AS {sql}")
        stage_table_by_reference[f"stage.{stage_alias}"] = stage_table

    return stage_table_by_reference


class KostenbelegeFactBuilderSampleLoaderTests(unittest.TestCase):
    def test_registry_discovers_kostenbelege_fact_builder_loader(self) -> None:
        generator = DataGeneratorRegistry().generator(KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID)
        payload = generator.definition().payload

        self.assertEqual(payload["generatorId"], KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID)
        self.assertEqual(payload["targetKind"], "s3")
        self.assertEqual(payload["title"], "Kostenbelege Fact Builder S3 Loader")
        self.assertEqual(payload["treePath"], list(KOSTENBELEGE_FACT_BUILDER_TREE_PATH))
        self.assertIn("result-storage", payload["tags"])
        self.assertIn("kostenbelege", payload["tags"])

    def test_loader_bucket_name_matches_notebook_folder(self) -> None:
        self.assertEqual(
            GENERATOR._loader_bucket_name("configured-bucket"),
            "poc-tests-general-functionalities-kostenbelege-fact-builder",
        )
        self.assertEqual(
            GENERATOR._loader_bucket_name("configured-bucket"),
            KOSTENBELEGE_FACT_BUILDER_BUCKET,
        )

    def test_generated_selects_match_declared_schemas(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(f"CREATE TEMP TABLE kbkp AS {kbkp_full_select(0, 12)}")
            connection.execute(f"CREATE TEMP TABLE kbpo AS {kbpo_slice_select(0, 24, 12)}")
            connection.execute(f"CREATE TEMP TABLE kbhp AS {kbhp_full_select(0, 48, 12)}")

            self.assertEqual(
                [
                    row[0]
                    for row in connection.execute(
                        "DESCRIBE SELECT * FROM kbkp"
                    ).fetchall()
                ],
                table_names(KBKP_COLUMNS),
            )
            self.assertEqual(
                [
                    row[0]
                    for row in connection.execute(
                        "DESCRIBE SELECT * FROM kbpo"
                    ).fetchall()
                ],
                table_names(KBPO_COLUMNS),
            )
            self.assertEqual(
                [
                    row[0]
                    for row in connection.execute(
                        "DESCRIBE SELECT * FROM kbhp"
                    ).fetchall()
                ],
                table_names(KBHP_COLUMNS),
            )
        finally:
            connection.close()

    def test_notebook_cells_store_and_reuse_intermediate_results(self) -> None:
        connection = duckdb.connect(":memory:")

        try:
            source_table_by_reference = create_fact_builder_source_tables(connection)
            execute_exploration_notebook(connection, source_table_by_reference)

            metrics = connection.execute(
                """
                SELECT total_rows, sum_betrag_hw, min_betrag_hw, max_betrag_hw
                FROM stored_fact_buchungsbelegposition_metrics
                """
            ).fetchone()
        finally:
            connection.close()

        self.assertIsNotNone(metrics)
        self.assertGreater(metrics[0], 0)
        self.assertIsNotNone(metrics[1])
        self.assertLess(metrics[2], metrics[3])

    def test_notebook_virtual_s3_references_rewrite_to_duckdb_reads(self) -> None:
        notebook = build_kostenbelege_fact_builder_s3_demo_notebook()
        service = WorkbenchService.__new__(WorkbenchService)
        service._lock = threading.RLock()
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(s3_relation_specs=lambda: {})

        for cell in notebook.cells:
            relation_index = service._query_source_relation_index(sql=cell.sql)
            rewritten_sql = service._rewrite_query_source_aliases(
                cell.sql,
                relation_index,
            )
            self.assertNotIn(
                f's3."{KOSTENBELEGE_FACT_BUILDER_BUCKET}"',
                rewritten_sql,
                cell.cell_id,
            )
            self.assertIn("read_parquet(", rewritten_sql, cell.cell_id)
            self.assertIn("s3://", rewritten_sql, cell.cell_id)

    def test_notebook_definition_enables_s3_result_storage_for_every_cell(self) -> None:
        notebook = build_kostenbelege_fact_builder_s3_demo_notebook()

        self.assertEqual(notebook.notebook_id, KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID)
        self.assertEqual(notebook.tree_path, KOSTENBELEGE_FACT_BUILDER_TREE_PATH)
        self.assertEqual(notebook.linked_generator_id, KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID)
        self.assertFalse(notebook.can_edit)
        self.assertFalse(notebook.can_delete)
        self.assertTrue(notebook.shared)
        self.assertEqual(len(notebook.cells), 6)
        self.assertTrue(all(cell.data_sources == ["s3"] for cell in notebook.cells))
        self.assertTrue(all(cell.language == "sql" for cell in notebook.cells))

        for cell, result_name in zip(notebook.cells, RESULT_SET_KEYS, strict=True):
            self.assertEqual(
                cell.query_options["duckdb"]["resultStorage"],
                {
                    "mode": "on",
                    "path": result_path(result_name),
                },
            )
            self.assertEqual(
                cell.query_options["validation"]["sourceExistence"],
                "off",
            )
            self.assertNotIn("read_parquet(", cell.sql)

        self.assertIn(result_reference("kbhp_today"), notebook.cells[3].sql)
        self.assertIn(result_reference("kbkp_today"), notebook.cells[4].sql)
        self.assertIn(result_reference("kbpo_today"), notebook.cells[4].sql)
        self.assertIn(result_reference("kbhp_pos1"), notebook.cells[4].sql)
        self.assertIn(
            result_reference("fact_buchungsbelegposition"),
            notebook.cells[5].sql,
        )

    def test_pipeline_notebook_definition_uses_materialized_stage_references(self) -> None:
        notebook = build_kostenbelege_fact_builder_s3_pipeline_notebook()

        self.assertEqual(
            notebook.notebook_id,
            KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID,
        )
        self.assertEqual(notebook.tree_path, KOSTENBELEGE_FACT_BUILDER_TREE_PATH)
        self.assertEqual(notebook.linked_generator_id, KOSTENBELEGE_FACT_BUILDER_GENERATOR_ID)
        self.assertEqual(notebook.pipeline_mode, "pipeline")
        self.assertTrue(notebook.can_edit)
        self.assertTrue(notebook.can_delete)
        self.assertTrue(notebook.shared)
        self.assertEqual(len(notebook.cells), 6)
        self.assertEqual(
            [cell.stage["alias"] for cell in notebook.cells],
            [
                "kbkp_today",
                "kbpo_today",
                "kbhp_today",
                "kbhp_pos1",
                "fact_buchungsbelegposition",
                "fact_buchungsbelegposition_metrics",
            ],
        )
        self.assertTrue(all(cell.stage["enabled"] is True for cell in notebook.cells))
        self.assertTrue(all(cell.stage["materialize"] is True for cell in notebook.cells))
        notebook_slug = KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID.replace("-", "_")
        self.assertEqual(
            [cell.stage["outputPath"] for cell in notebook.cells],
            [
                (
                    f"s3://{KOSTENBELEGE_FACT_BUILDER_BUCKET}/generated/kostenbelege_fact_builder/"
                    f"pipeline-results/{notebook_slug}/{cell.stage['alias']}.parquet"
                )
                for cell in notebook.cells
            ],
        )
        self.assertEqual(notebook.cells[0].data_sources, ["s3"])
        self.assertEqual(notebook.cells[1].data_sources, ["s3"])
        self.assertEqual(notebook.cells[2].data_sources, ["s3"])
        self.assertTrue(all(cell.data_sources == [] for cell in notebook.cells[3:]))
        self.assertTrue(
            all(
                cell.query_options["validation"]["sourceExistence"] == "off"
                for cell in notebook.cells
            )
        )
        self.assertTrue(
            all("resultStorage" not in cell.query_options.get("duckdb", {}) for cell in notebook.cells)
        )
        self.assertIn("stage.kbhp_today", notebook.cells[3].sql)
        self.assertIn("stage.kbkp_today", notebook.cells[4].sql)
        self.assertIn("stage.kbpo_today", notebook.cells[4].sql)
        self.assertIn("stage.kbhp_today", notebook.cells[4].sql)
        self.assertIn("stage.kbhp_pos1", notebook.cells[4].sql)
        self.assertIn("stage.fact_buchungsbelegposition", notebook.cells[5].sql)
        self.assertTrue(all("read_parquet(" not in cell.sql for cell in notebook.cells))
        self.assertEqual(
            notebook.pipeline_paths,
            [
                {
                    "pathId": "path-kfb-fact-metrics",
                    "terminalStageId": "stage-kfb-fact-buchungsbelegposition-metrics",
                    "label": "Fact metrics",
                    "priority": 1,
                }
            ],
        )

    def test_pipeline_notebook_produces_same_final_result_as_exploration_flow(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            source_table_by_reference = create_fact_builder_source_tables(connection)
            execute_exploration_notebook(connection, source_table_by_reference)
            execute_pipeline_notebook(connection, source_table_by_reference)

            exploration_metrics = connection.execute(
                """
                SELECT total_rows, sum_betrag_hw, avg_betrag_hw, min_betrag_hw, max_betrag_hw
                FROM stored_fact_buchungsbelegposition_metrics
                """
            ).fetchall()
            pipeline_metrics = connection.execute(
                """
                SELECT total_rows, sum_betrag_hw, avg_betrag_hw, min_betrag_hw, max_betrag_hw
                FROM pipeline_fact_buchungsbelegposition_metrics
                """
            ).fetchall()
            fact_difference_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM (
                    (
                        SELECT * FROM stored_fact_buchungsbelegposition
                        EXCEPT ALL
                        SELECT * FROM pipeline_fact_buchungsbelegposition
                    )
                    UNION ALL
                    (
                        SELECT * FROM pipeline_fact_buchungsbelegposition
                        EXCEPT ALL
                        SELECT * FROM stored_fact_buchungsbelegposition
                    )
                ) AS differences
                """
            ).fetchone()[0]
        finally:
            connection.close()

        self.assertEqual(pipeline_metrics, exploration_metrics)
        self.assertEqual(fact_difference_count, 0)

    def test_static_notebook_catalog_includes_kostenbelege_fact_builder_demo(self) -> None:
        notebooks = {notebook.notebook_id: notebook for notebook in build_notebooks([])}

        self.assertIn(KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID, notebooks)
        self.assertEqual(
            notebooks[KOSTENBELEGE_FACT_BUILDER_NOTEBOOK_ID].tree_path,
            KOSTENBELEGE_FACT_BUILDER_TREE_PATH,
        )
        self.assertIn(KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID, notebooks)
        self.assertEqual(
            notebooks[KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID].tree_path,
            KOSTENBELEGE_FACT_BUILDER_TREE_PATH,
        )
        self.assertEqual(
            notebooks[KOSTENBELEGE_FACT_BUILDER_PIPELINE_NOTEBOOK_ID].pipeline_mode,
            "pipeline",
        )


if __name__ == "__main__":
    unittest.main()

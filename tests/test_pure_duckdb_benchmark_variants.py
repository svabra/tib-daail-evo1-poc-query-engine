from __future__ import annotations

from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
SCRIPTS_ROOT = REPO_ROOT / "scripts"
for path in (BDW_ROOT, SCRIPTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


from bit_data_workbench.backend.pure_duckdb import (  # noqa: E402
    FACT_BUPO_TARGET,
    KBHP_FULL_PATH,
    KBKP_FULL_PATH,
    KALENDER_PATH,
    KBPO_PATHS,
)
from bit_data_workbench.backend.pure_duckdb_benchmark_variants import (  # noqa: E402
    PureDuckDBBenchmarkVariant,
    pure_duckdb_q1_q2_benchmark_variants,
    pure_duckdb_q1_q2_comparison_columns,
)
from pure_duckdb_big_data_benchmark import (  # noqa: E402
    copy_query,
    kalender_select,
    kbhp_select,
    kbkp_select,
    kbpo_select,
)
from pure_duckdb_q1_q2_optimization_benchmark import _selected_variants  # noqa: E402


def _write_fixture(root: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {
        KBKP_FULL_PATH: root / "CORE" / "KBKPfull.parquet",
        KBHP_FULL_PATH: root / "CORE" / "KBHPfull.parquet",
        KALENDER_PATH: root / "3_1_imports" / "DIM_Kalender.parquet",
        FACT_BUPO_TARGET: root / "core" / "fact_bupo.parquet",
    }
    for index, s3_url in enumerate(KBPO_PATHS):
        paths[s3_url] = root / "KBPOimports" / f"kbpo_{index}.parquet"

    connection = duckdb.connect(":memory:")
    try:
        copy_query(connection, kbkp_select(80), paths[KBKP_FULL_PATH])
        copy_query(connection, kbhp_select(80), paths[KBHP_FULL_PATH])
        copy_query(connection, kalender_select(), paths[KALENDER_PATH])
        for index, s3_url in enumerate(KBPO_PATHS, start=1):
            copy_query(
                connection,
                kbpo_select(file_index=index, rows=96, dimension_rows=80, seed=311),
                paths[s3_url],
            )
    finally:
        connection.close()
    return paths


def _add_variant_outputs(paths: dict[str, Path], root: Path) -> None:
    for variant in pure_duckdb_q1_q2_benchmark_variants():
        if variant.output_s3_url:
            relative = variant.output_s3_url.removeprefix("s3://")
            paths[variant.output_s3_url] = root / relative


def _local_sql(sql: str, paths: dict[str, Path]) -> str:
    rewritten = sql
    for s3_url, local_path in sorted(paths.items(), key=lambda item: len(item[0]), reverse=True):
        replacement = local_path.as_posix()
        if s3_url.endswith("/"):
            replacement = f"{replacement}/"
        rewritten = rewritten.replace(s3_url, replacement)
    return rewritten


def _cleanup_output(variant: PureDuckDBBenchmarkVariant, paths: dict[str, Path]) -> None:
    if not variant.output_s3_url:
        return
    path = paths[variant.output_s3_url]
    if variant.output_s3_url.endswith("/"):
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)


def _execute_variant(
    connection: duckdb.DuckDBPyConnection,
    variant: PureDuckDBBenchmarkVariant,
    paths: dict[str, Path],
) -> None:
    _cleanup_output(variant, paths)
    for setting in variant.duckdb_settings:
        connection.execute(setting)
    for statement in variant.statements:
        connection.execute(_local_sql(statement, paths))


def _q2_difference_count(
    connection: duckdb.DuckDBPyConnection,
    baseline: PureDuckDBBenchmarkVariant,
    candidate: PureDuckDBBenchmarkVariant,
    paths: dict[str, Path],
) -> int:
    baseline_sql = _local_sql(baseline.validation_sql, paths).rstrip().rstrip(";")
    candidate_sql = _local_sql(candidate.validation_sql, paths).rstrip().rstrip(";")
    return connection.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            (SELECT * FROM ({baseline_sql}) baseline_query
             EXCEPT ALL
             SELECT * FROM ({candidate_sql}) candidate_query)
            UNION ALL
            (SELECT * FROM ({candidate_sql}) candidate_query
             EXCEPT ALL
             SELECT * FROM ({baseline_sql}) baseline_query)
        ) differences
        """
    ).fetchone()[0]


class PureDuckDBBenchmarkVariantTests(unittest.TestCase):
    def test_variants_include_explanatory_comparison_metadata(self) -> None:
        columns = pure_duckdb_q1_q2_comparison_columns()
        variants = pure_duckdb_q1_q2_benchmark_variants()
        variant_ids = {variant.variant_id for variant in variants}

        self.assertIn("change_summary", columns)
        self.assertIn("sql_strategy", columns)
        self.assertIn("output_layout", columns)
        self.assertIn("duckdb_settings", columns)
        self.assertIn("expected_effect", columns)
        self.assertIn("q1_baseline_current", variant_ids)
        self.assertIn("q1_pushdown_v1", variant_ids)
        self.assertIn("q2_dataset_folder_v1", variant_ids)
        self.assertIn("q2_staged_materialization_v1", variant_ids)
        for variant in variants:
            metadata = variant.comparison_metadata
            self.assertTrue(metadata["change_summary"], variant.variant_id)
            self.assertTrue(metadata["sql_strategy"], variant.variant_id)
            self.assertTrue(metadata["expected_effect"], variant.variant_id)
            for statement in variant.statements:
                self.assertNotRegex(statement, r"\bs3\.[A-Za-z0-9_\"]")

    def test_q1_pushdown_matches_current_baseline_on_local_parquet(self) -> None:
        variants = {variant.variant_id: variant for variant in pure_duckdb_q1_q2_benchmark_variants()}
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = _write_fixture(root)
            _add_variant_outputs(paths, root)
            connection = duckdb.connect(":memory:")
            try:
                baseline = connection.execute(
                    _local_sql(variants["q1_baseline_current"].validation_sql, paths)
                ).fetchone()
                pushed_down = connection.execute(
                    _local_sql(variants["q1_pushdown_v1"].validation_sql, paths)
                ).fetchone()
                optimized = connection.execute(
                    _local_sql(variants["q1_optimized_fact_v1"].validation_sql, paths)
                ).fetchone()

                self.assertEqual(pushed_down[0], baseline[0])
                self.assertAlmostEqual(float(pushed_down[1]), float(baseline[1]), places=6)
                self.assertEqual(optimized[0], baseline[0])
                self.assertAlmostEqual(float(optimized[1]), float(baseline[1]), places=6)
            finally:
                connection.close()

    def test_q2_candidates_match_baseline_with_except_all(self) -> None:
        variants = {variant.variant_id: variant for variant in pure_duckdb_q1_q2_benchmark_variants()}
        candidate_ids = (
            "q2_optimized_single_file_v1",
            "q2_dataset_folder_v1",
            "q2_materialized_ctes_v1",
            "q2_staged_materialization_v1",
            "q2_runtime_unordered_threads4_v1",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = _write_fixture(root)
            _add_variant_outputs(paths, root)
            connection = duckdb.connect(":memory:")
            try:
                baseline = variants["q2_baseline_current"]
                _execute_variant(connection, baseline, paths)
                baseline_columns = [
                    row[0]
                    for row in connection.execute(
                        f"DESCRIBE SELECT * FROM ({_local_sql(baseline.validation_sql, paths)}) q"
                    ).fetchall()
                ]
                self.assertIn("BetragHauswaehrung", baseline_columns)

                for candidate_id in candidate_ids:
                    with self.subTest(candidate_id=candidate_id):
                        candidate = variants[candidate_id]
                        _execute_variant(connection, candidate, paths)
                        candidate_columns = [
                            row[0]
                            for row in connection.execute(
                                f"DESCRIBE SELECT * FROM ({_local_sql(candidate.validation_sql, paths)}) q"
                            ).fetchall()
                        ]
                        self.assertEqual(candidate_columns, baseline_columns)
                        self.assertEqual(_q2_difference_count(connection, baseline, candidate, paths), 0)
            finally:
                connection.close()

    def test_dataset_folder_output_is_readable_with_wildcard_scan(self) -> None:
        variants = {variant.variant_id: variant for variant in pure_duckdb_q1_q2_benchmark_variants()}
        dataset_variant = variants["q2_dataset_folder_v1"]
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = _write_fixture(root)
            _add_variant_outputs(paths, root)
            connection = duckdb.connect(":memory:")
            try:
                _execute_variant(connection, dataset_variant, paths)
                output_dir = paths[dataset_variant.output_s3_url]
                self.assertTrue(output_dir.is_dir())
                self.assertGreater(len(list(output_dir.glob("*.parquet"))), 0)
                row_count = connection.execute(
                    f"SELECT COUNT(*) FROM ({_local_sql(dataset_variant.validation_sql, paths)}) q"
                ).fetchone()[0]
                self.assertGreater(row_count, 0)
            finally:
                connection.close()

    def test_benchmark_runner_documents_comparison_columns(self) -> None:
        script = (REPO_ROOT / "scripts" / "pure_duckdb_q1_q2_optimization_benchmark.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("pure_duckdb_q1_q2_comparison_columns", script)
        self.assertIn("change_summary", script)
        self.assertIn("sql_strategy", script)
        self.assertIn("consistency_details", script)
        self.assertIn("--target-compressed-mib", script)
        self.assertIn("default=20.0", script)
        self.assertIn("--rerun-top-candidates", script)
        self.assertIn("--no-json", script)
        self.assertIn("--json-output", script)

    def test_runner_includes_baseline_when_single_candidate_is_selected(self) -> None:
        selected = _selected_variants(
            SimpleNamespace(variant=["q1_pushdown_v1"], query=[])
        )

        self.assertEqual(
            [variant.variant_id for variant in selected],
            ["q1_baseline_current", "q1_pushdown_v1"],
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_optimized_mock_data_generator_tiny_run_creates_readable_parquet(tmp_path) -> None:
    output_dir = tmp_path / "kostenbelege-3-1-mock"
    script = REPO_ROOT / "scripts" / "generate_kostenbelege_3_1_optimized_mock_data.py"

    subprocess.run(
        [
            sys.executable,
            str(script),
            "--output-dir",
            str(output_dir),
            "--force",
            "--rows-per-kbpo-file",
            "8",
            "--dimension-rows",
            "8",
            "--target-compressed-mib",
            "0",
            "--quiet",
        ],
        check=True,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )

    kbpo_file = output_dir / "KBPOimports" / "KBPO2020.parquet"
    expected_files = [
        kbpo_file,
        output_dir / "CORE" / "kbkpfull.parquet",
        output_dir / "CORE" / "kbhpfull.parquet",
        output_dir / "n_3_1_imports" / "dim_kalender.parquet",
    ]
    for path in expected_files:
        assert path.exists(), path

    with duckdb.connect(":memory:") as connection:
        row_count, belegnummer_count, ausgleich_count, amount_count = connection.execute(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(KBKP_Belegnummer) AS belegnummer_count,
                COUNT(KBKP_AusgleichBelegnummer) AS ausgleich_count,
                COUNT(KBPO_HWhrBetrag1) AS amount_count
            FROM read_parquet('{kbpo_file.as_posix()}')
            """
        ).fetchone()

    assert row_count == 8
    assert belegnummer_count == 8
    assert ausgleich_count == 8
    assert amount_count == 8

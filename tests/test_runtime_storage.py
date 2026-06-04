from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.runtime_storage import (  # noqa: E402
    delete_runtime_query_cache,
    runtime_storage_snapshot,
)


class RuntimeStorageTests(unittest.TestCase):
    def test_runtime_storage_snapshot_reports_cache_and_spill_usage(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            cache_root = root / "query-cache"
            spill_root = root / "duckdb-spill"
            cache_root.mkdir()
            spill_root.mkdir()
            cache_key = "b" * 40
            (cache_root / f"{cache_key}.duckdb").write_bytes(b"database")
            (cache_root / f"{cache_key}.json").write_text(
                '{"relation": "s3.core.table.parquet", "rowCount": 12}',
                encoding="utf-8",
            )
            (spill_root / "duckdb-temp-block.tmp").write_bytes(b"spill")
            settings = SimpleNamespace(
                query_cache_dir=cache_root,
                duckdb_temp_directory=spill_root,
                duckdb_memory_limit="20GiB",
                duckdb_threads=4,
                duckdb_max_temp_directory_size="96GiB",
                duckdb_preserve_insertion_order=False,
            )

            payload = runtime_storage_snapshot(settings)  # type: ignore[arg-type]

        self.assertEqual(payload["queryCache"]["datasetCount"], 1)
        self.assertGreater(payload["queryCache"]["sizeBytes"], 0)
        self.assertGreater(payload["duckdbSpill"]["sizeBytes"], 0)
        self.assertFalse(payload["duckdbSpill"]["deletable"])
        self.assertEqual(payload["duckdbSettings"]["memoryLimit"], "20GiB")
        self.assertEqual(payload["duckdbSettings"]["threads"], 4)

    def test_delete_runtime_query_cache_returns_refreshed_storage_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            cache_root = root / "query-cache"
            spill_root = root / "duckdb-spill"
            cache_root.mkdir()
            spill_root.mkdir()
            cache_key = "c" * 40
            database_path = cache_root / f"{cache_key}.duckdb"
            database_path.write_bytes(b"database")
            (cache_root / f"{cache_key}.json").write_text("{}", encoding="utf-8")
            settings = SimpleNamespace(
                query_cache_dir=cache_root,
                duckdb_temp_directory=spill_root,
                duckdb_memory_limit="20GiB",
                duckdb_threads=4,
                duckdb_max_temp_directory_size="96GiB",
                duckdb_preserve_insertion_order=False,
            )

            payload = delete_runtime_query_cache(settings, cache_key)  # type: ignore[arg-type]

            self.assertTrue(payload["deleted"])
            self.assertFalse(database_path.exists())
            self.assertEqual(payload["storage"]["queryCache"]["datasetCount"], 0)


if __name__ == "__main__":
    unittest.main()

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
    parse_storage_size_bytes,
    runtime_storage_snapshot,
    runtime_storage_usage_metrics,
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
                duckdb_threads=8,
                duckdb_max_temp_directory_size="96GiB",
                duckdb_preserve_insertion_order=False,
            )

            payload = runtime_storage_snapshot(settings)  # type: ignore[arg-type]

        self.assertEqual(payload["queryCache"]["datasetCount"], 1)
        self.assertGreater(payload["queryCache"]["sizeBytes"], 0)
        self.assertGreater(payload["duckdbSpill"]["sizeBytes"], 0)
        self.assertFalse(payload["duckdbSpill"]["deletable"])
        self.assertEqual(payload["duckdbSettings"]["memoryLimit"], "20GiB")
        self.assertEqual(payload["duckdbSettings"]["threads"], 8)
        self.assertEqual(payload["duckdbSettings"]["maxTempDirectorySizeBytes"], 96 * 1024**3)

    def test_parse_storage_size_bytes_supports_duckdb_size_strings(self) -> None:
        self.assertEqual(parse_storage_size_bytes("96GiB"), 96 * 1024**3)
        self.assertEqual(parse_storage_size_bytes("100Gi"), 100 * 1024**3)
        self.assertEqual(parse_storage_size_bytes("1.5GB"), int(1.5 * 1000**3))
        self.assertIsNone(parse_storage_size_bytes(""))
        self.assertIsNone(parse_storage_size_bytes("not-a-size"))

    def test_runtime_storage_usage_metrics_reports_active_spill_and_cache(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            cache_root = root / "query-cache"
            spill_root = root / "duckdb-spill"
            active_spill = spill_root / "query-abc"
            stale_spill = spill_root / "stale"
            cache_root.mkdir()
            active_spill.mkdir(parents=True)
            stale_spill.mkdir()
            (active_spill / "block.tmp").write_bytes(b"a" * 11)
            (stale_spill / "block.tmp").write_bytes(b"b" * 7)
            (cache_root / "cache.duckdb").write_bytes(b"c" * 5)
            settings = SimpleNamespace(
                query_cache_dir=cache_root,
                duckdb_temp_directory=spill_root,
                duckdb_memory_limit="20GiB",
                duckdb_threads=8,
                duckdb_max_temp_directory_size="96GiB",
                duckdb_preserve_insertion_order=False,
            )

            payload = runtime_storage_usage_metrics(settings)  # type: ignore[arg-type]

        self.assertEqual(payload["duckdbSpill"]["activeQueryBytes"], 11)
        self.assertEqual(payload["duckdbSpill"]["totalBytes"], 18)
        self.assertEqual(payload["duckdbSpill"]["otherBytes"], 7)
        self.assertEqual(payload["duckdbSpill"]["maxTempDirectorySizeBytes"], 96 * 1024**3)
        self.assertEqual(payload["queryCache"]["sizeBytes"], 5)

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
                duckdb_threads=8,
                duckdb_max_temp_directory_size="96GiB",
                duckdb_preserve_insertion_order=False,
            )

            payload = delete_runtime_query_cache(settings, cache_key)  # type: ignore[arg-type]

            self.assertTrue(payload["deleted"])
            self.assertFalse(database_path.exists())
            self.assertEqual(payload["storage"]["queryCache"]["datasetCount"], 0)


if __name__ == "__main__":
    unittest.main()

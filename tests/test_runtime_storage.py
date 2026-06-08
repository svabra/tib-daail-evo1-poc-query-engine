from __future__ import annotations

import threading
from contextlib import contextmanager
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
    cleanup_stale_query_spill_directories,
    delete_query_spill_directory,
    delete_runtime_query_cache,
    parse_storage_size_bytes,
    runtime_storage_snapshot,
    runtime_storage_usage_metrics,
)
from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402


class RuntimeStorageTests(unittest.TestCase):
    def test_cleanup_stale_query_spill_directories_deletes_only_query_directories(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            spill_root = root / "duckdb-spill"
            spill_root.mkdir()
            query_a = spill_root / "query-old-a"
            query_b = spill_root / "query-old-b"
            stale_dir = spill_root / "stale"
            query_file = spill_root / "query-note.txt"
            query_a.mkdir()
            query_b.mkdir()
            stale_dir.mkdir()
            (query_a / "block.tmp").write_bytes(b"a" * 11)
            (query_b / "block.tmp").write_bytes(b"b" * 7)
            (stale_dir / "keep.tmp").write_bytes(b"c" * 5)
            query_file.write_bytes(b"q" * 3)

            payload = cleanup_stale_query_spill_directories(spill_root)

            self.assertFalse(query_a.exists())
            self.assertFalse(query_b.exists())
            self.assertTrue(stale_dir.exists())
            self.assertTrue(query_file.exists())

        self.assertEqual(payload["root"], spill_root.resolve().as_posix())
        self.assertEqual(payload["inspectedCount"], 4)
        self.assertEqual(payload["deletedCount"], 2)
        self.assertEqual(payload["failedCount"], 0)
        self.assertEqual(payload["reclaimedBytes"], 18)
        self.assertEqual(payload["skippedCount"], 2)

    def test_delete_query_spill_directory_rejects_root_and_outside_paths(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            spill_root = root / "duckdb-spill"
            spill_root.mkdir()
            outside_query = root / "outside" / "query-escape"
            outside_query.mkdir(parents=True)

            deleted_root, reclaimed_root = delete_query_spill_directory(spill_root, spill_root)
            deleted_outside, reclaimed_outside = delete_query_spill_directory(
                spill_root,
                outside_query,
            )

            self.assertTrue(outside_query.exists())

        self.assertFalse(deleted_root)
        self.assertEqual(reclaimed_root, 0)
        self.assertFalse(deleted_outside)
        self.assertEqual(reclaimed_outside, 0)

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

    def test_workbench_start_cleans_stale_query_spill_before_startup_connection(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            spill_root = root / "duckdb-spill"
            spill_root.mkdir()
            stale_query = spill_root / "query-old"
            stale_query.mkdir()
            (stale_query / "block.tmp").write_bytes(b"spill")
            preserved_dir = spill_root / "stale"
            preserved_dir.mkdir()
            calls: list[str] = []

            service = WorkbenchService.__new__(WorkbenchService)
            service.settings = SimpleNamespace(
                duckdb_database=root / "workspace" / "workspace.duckdb",
                duckdb_extension_directory=root / "duckdb-ext",
                duckdb_temp_directory=spill_root,
                image_version="test",
            )
            service._lock = threading.RLock()
            service._log_startup_section = lambda _title: None
            service._log_startup = lambda _message, *args, **_kwargs: None
            service._initialize_shared_notebook_store = lambda: calls.append("shared-store")

            @contextmanager
            def fake_duckdb_connection(**_kwargs):
                calls.append("connect")
                self.assertFalse(stale_query.exists())
                self.assertTrue(preserved_dir.exists())
                yield object()

            service._duckdb_connection = fake_duckdb_connection
            service._refresh_state = lambda _conn: calls.append("refresh")
            service._set_minimal_state = lambda: calls.append("minimal")
            service._sync_startup_seed_data_sources = lambda: calls.append("seed-sources")
            service._ensure_startup_shared_notebook_seeds = lambda: calls.append("seed-notebooks")
            service._migrate_shared_notebook_source_references = lambda: 0
            service._data_source_discovery = SimpleNamespace(start=lambda: calls.append("discovery"))
            service._start_background_s3_startup_diagnostics = lambda: calls.append("diagnostics")
            service._service_consumption = SimpleNamespace(start=lambda: calls.append("consumption"))

            service.start()

            self.assertFalse(stale_query.exists())
            self.assertTrue(preserved_dir.exists())

        self.assertIn("connect", calls)


if __name__ == "__main__":
    unittest.main()

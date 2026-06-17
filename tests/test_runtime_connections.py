from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend import runtime_connections
from bit_data_workbench.config import Settings


class DuckDBWorkerConnectionRetryTests(TestCase):
    def test_connect_retries_transient_file_lock_conflicts(self) -> None:
        connection = object()
        calls = {"count": 0}

        def fake_connect(connection_target: str, *, read_only: bool = False):
            calls["count"] += 1
            if calls["count"] < 3:
                raise duckdb.IOException(
                    'IO Error: Cannot open file "workspace.duckdb": '
                    "The process cannot access the file because it is being used by another process."
                )
            return connection

        with (
            patch.object(runtime_connections.duckdb, "connect", side_effect=fake_connect),
            patch.object(runtime_connections.time, "sleep"),
        ):
            result = runtime_connections._connect_duckdb_with_lock_retry(
                "workspace.duckdb",
                read_only=True,
            )

        self.assertIs(result, connection)
        self.assertEqual(calls["count"], 3)

    def test_settings_parse_duckdb_runtime_resource_environment(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BDW_DUCKDB_MEMORY_LIMIT": "20GiB",
                "BDW_DUCKDB_THREADS": "8",
                "BDW_DUCKDB_TEMP_DIRECTORY": "/workspace/tmp/duckdb-spill",
                "BDW_DUCKDB_MAX_TEMP_DIRECTORY_SIZE": "100GiB",
                "BDW_DUCKDB_PRESERVE_INSERTION_ORDER": "false",
                "BDW_QUERY_CACHE_DIR": "/workspace/query-cache",
            },
            clear=False,
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.duckdb_memory_limit, "20GiB")
        self.assertEqual(settings.duckdb_threads, 8)
        self.assertEqual(settings.duckdb_temp_directory, Path("/workspace/tmp/duckdb-spill"))
        self.assertEqual(settings.duckdb_max_temp_directory_size, "100GiB")
        self.assertFalse(settings.duckdb_preserve_insertion_order)
        self.assertEqual(settings.query_cache_dir, Path("/workspace/query-cache"))

    def test_apply_duckdb_runtime_settings_executes_expected_set_statements(self) -> None:
        class FakeConnection:
            def __init__(self) -> None:
                self.commands: list[str] = []

            def execute(self, command: str):
                self.commands.append(command)
                return self

        with tempfile.TemporaryDirectory() as raw_tmp:
            spill_dir = Path(raw_tmp) / "duckdb-spill"
            settings = SimpleNamespace(
                duckdb_memory_limit="20GiB",
                duckdb_threads=8,
                duckdb_temp_directory=spill_dir,
                duckdb_max_temp_directory_size="96GiB",
                duckdb_preserve_insertion_order=False,
            )
            connection = FakeConnection()

            applied = runtime_connections.apply_duckdb_runtime_settings(connection, settings)  # type: ignore[arg-type]

            self.assertEqual(applied["memoryLimit"], "20GiB")
            self.assertEqual(applied["threads"], 8)
            self.assertEqual(applied["tempDirectory"], spill_dir.as_posix())
            self.assertEqual(applied["maxTempDirectorySize"], "96GiB")
            self.assertFalse(applied["preserveInsertionOrder"])
            self.assertIn("SET memory_limit = '20GiB'", connection.commands)
            self.assertIn("SET threads = 8", connection.commands)
            self.assertIn("SET preserve_insertion_order = false", connection.commands)
            self.assertIn(f"SET temp_directory = '{spill_dir.as_posix()}'", connection.commands)
            self.assertIn("SET max_temp_directory_size = '96GiB'", connection.commands)
            self.assertTrue(spill_dir.exists())

    def test_apply_duckdb_runtime_settings_defaults_spill_quota_when_temp_dir_is_configured(self) -> None:
        class FakeConnection:
            def __init__(self) -> None:
                self.commands: list[str] = []

            def execute(self, command: str):
                self.commands.append(command)
                return self

        with tempfile.TemporaryDirectory() as raw_tmp:
            spill_dir = Path(raw_tmp) / "duckdb-spill"
            settings = SimpleNamespace(
                duckdb_memory_limit="20GiB",
                duckdb_threads=8,
                duckdb_temp_directory=spill_dir,
                duckdb_max_temp_directory_size=None,
                duckdb_preserve_insertion_order=False,
            )
            connection = FakeConnection()

            applied = runtime_connections.apply_duckdb_runtime_settings(connection, settings)  # type: ignore[arg-type]

            self.assertEqual(applied["maxTempDirectorySize"], "100GiB")
            self.assertIn("SET max_temp_directory_size = '100GiB'", connection.commands)
            self.assertTrue(spill_dir.exists())

    def test_apply_duckdb_runtime_settings_allows_temp_directory_override(self) -> None:
        class FakeConnection:
            def __init__(self) -> None:
                self.commands: list[str] = []

            def execute(self, command: str):
                self.commands.append(command)
                return self

        with tempfile.TemporaryDirectory() as raw_tmp:
            configured_spill_dir = Path(raw_tmp) / "duckdb-spill"
            query_spill_dir = configured_spill_dir / "query-123"
            settings = SimpleNamespace(
                duckdb_memory_limit="20GiB",
                duckdb_threads=8,
                duckdb_temp_directory=configured_spill_dir,
                duckdb_max_temp_directory_size="96GiB",
                duckdb_preserve_insertion_order=False,
            )
            connection = FakeConnection()

            applied = runtime_connections.apply_duckdb_runtime_settings(
                connection,  # type: ignore[arg-type]
                settings,
                temp_directory_override=query_spill_dir,
            )

            self.assertEqual(applied["tempDirectory"], query_spill_dir.as_posix())
            self.assertIn(
                f"SET temp_directory = '{query_spill_dir.as_posix()}'",
                connection.commands,
            )
            self.assertTrue(query_spill_dir.exists())
            self.assertTrue(configured_spill_dir.exists())

    def test_apply_duckdb_runtime_settings_sets_effective_duckdb_spill_quota(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            spill_dir = Path(raw_tmp) / "duckdb-spill"
            settings = SimpleNamespace(
                duckdb_memory_limit="20GiB",
                duckdb_threads=8,
                duckdb_temp_directory=spill_dir,
                duckdb_max_temp_directory_size="96GiB",
                duckdb_preserve_insertion_order=False,
            )
            connection = duckdb.connect(":memory:")
            try:
                runtime_connections.apply_duckdb_runtime_settings(connection, settings)  # type: ignore[arg-type]

                effective_settings = connection.execute(
                    """
                    SELECT
                        current_setting('memory_limit'),
                        current_setting('threads'),
                        current_setting('preserve_insertion_order'),
                        current_setting('temp_directory'),
                        current_setting('max_temp_directory_size')
                    """
                ).fetchone()
            finally:
                connection.close()

            self.assertEqual(effective_settings[0], "20.0 GiB")
            self.assertEqual(effective_settings[1], 8)
            self.assertFalse(effective_settings[2])
            self.assertEqual(effective_settings[3], spill_dir.as_posix())
            self.assertEqual(effective_settings[4], "96.0 GiB")

    def test_apply_duckdb_runtime_settings_sets_effective_default_spill_quota(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            spill_dir = Path(raw_tmp) / "duckdb-spill"
            settings = SimpleNamespace(
                duckdb_memory_limit="20GiB",
                duckdb_threads=8,
                duckdb_temp_directory=spill_dir,
                duckdb_max_temp_directory_size="",
                duckdb_preserve_insertion_order=False,
            )
            connection = duckdb.connect(":memory:")
            try:
                runtime_connections.apply_duckdb_runtime_settings(connection, settings)  # type: ignore[arg-type]
                effective_limit = connection.execute(
                    "SELECT current_setting('max_temp_directory_size')"
                ).fetchone()[0]
            finally:
                connection.close()

            self.assertEqual(effective_limit, "100.0 GiB")

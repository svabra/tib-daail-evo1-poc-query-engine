from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend import runtime_connections


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

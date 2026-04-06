from __future__ import annotations

import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_launcher():
    from bit_data_workbench import launcher

    return launcher


class LauncherTests(unittest.TestCase):
    def test_file_logging_disabled_by_default(self) -> None:
        launcher = import_launcher()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("BDW_ENABLE_FILE_LOGGING", None)
            kwargs = launcher.build_uvicorn_run_kwargs()

        self.assertNotIn("log_config", kwargs)
        self.assertEqual(kwargs["host"], "0.0.0.0")
        self.assertEqual(kwargs["port"], 8000)

    def test_file_logging_enabled_when_env_true(self) -> None:
        launcher = import_launcher()
        with patch.dict(
            os.environ,
            {"BDW_ENABLE_FILE_LOGGING": "true"},
            clear=False,
        ):
            kwargs = launcher.build_uvicorn_run_kwargs()

        self.assertEqual(
            kwargs["log_config"],
            str(launcher.logging_config_path()),
        )

    def test_file_logging_disabled_when_env_false(self) -> None:
        launcher = import_launcher()
        with patch.dict(
            os.environ,
            {"BDW_ENABLE_FILE_LOGGING": "false"},
            clear=False,
        ):
            kwargs = launcher.build_uvicorn_run_kwargs()

        self.assertNotIn("log_config", kwargs)


if __name__ == "__main__":
    unittest.main()

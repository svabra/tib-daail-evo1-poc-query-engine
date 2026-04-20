from __future__ import annotations

import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.version_info import current_repo_version, runtime_image_version  # noqa: E402


class VersionInfoTests(TestCase):
    def test_runtime_image_version_prefers_non_empty_env_value(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            (repo_root / "VERSION").write_text("1.2.3\n", encoding="utf-8")
            anchor = repo_root / "bdw" / "bit_data_workbench" / "config.py"
            anchor.parent.mkdir(parents=True)
            anchor.write_text("", encoding="utf-8")

            with patch.dict(os.environ, {"IMAGE_VERSION": "9.9.9"}, clear=True):
                self.assertEqual(runtime_image_version(anchor), "9.9.9")

    def test_runtime_image_version_reads_version_file_when_env_missing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            (repo_root / "VERSION").write_text("2.3.4\n", encoding="utf-8")
            anchor = repo_root / "app" / "bit_data_workbench" / "config.py"
            anchor.parent.mkdir(parents=True)
            anchor.write_text("", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(runtime_image_version(anchor), "2.3.4")
                self.assertEqual(current_repo_version(anchor), "2.3.4")

    def test_runtime_image_version_falls_back_to_dev_without_env_or_version_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            anchor = Path(temp_dir) / "app" / "bit_data_workbench" / "config.py"
            anchor.parent.mkdir(parents=True)
            anchor.write_text("", encoding="utf-8")

            with patch.dict(os.environ, {"IMAGE_VERSION": "   "}, clear=True):
                self.assertEqual(runtime_image_version(anchor), "dev")
                self.assertEqual(current_repo_version(anchor), "dev")

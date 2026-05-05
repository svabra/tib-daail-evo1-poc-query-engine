from __future__ import annotations

from pathlib import Path
import sys
from unittest.mock import patch

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


class _ConfiguredSettings:
    s3_endpoint = "http://127.0.0.1:9000"

    def current_s3_access_key_id(self) -> str:
        return "key"

    def current_s3_secret_access_key(self) -> str:
        return "secret"


def import_s3_explorer():
    from bit_data_workbench.backend.data_sources.s3 import explorer

    return explorer


def test_normalize_s3_bucket_name_rejects_underscores() -> None:
    explorer = import_s3_explorer()

    with pytest.raises(ValueError, match="lowercase letters, numbers, dots, or hyphens"):
        explorer.normalize_s3_bucket_name("client_bucket")


def test_create_bucket_rejects_underscores_before_s3_call() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with patch.object(explorer, "ensure_s3_bucket") as ensure_s3_bucket:
        with pytest.raises(ValueError, match="lowercase letters, numbers, dots, or hyphens"):
            manager.create_bucket("client_bucket")

    ensure_s3_bucket.assert_not_called()


def test_create_bucket_normalizes_valid_bucket_name() -> None:
    explorer = import_s3_explorer()
    manager = explorer.S3ExplorerManager(_ConfiguredSettings())

    with patch.object(explorer, "ensure_s3_bucket") as ensure_s3_bucket:
        created = manager.create_bucket(" Client-Bucket ")

    ensure_s3_bucket.assert_called_once_with(manager._settings, "client-bucket")
    assert created.bucket == "client-bucket"

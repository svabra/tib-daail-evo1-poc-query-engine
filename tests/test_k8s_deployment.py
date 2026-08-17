from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend.runtime_storage import parse_storage_size_bytes


DEPLOYMENT = REPO_ROOT / "k8s" / "bdw-deployment.yaml"
CONFIGMAP = REPO_ROOT / "k8s" / "bdw-configmap.yaml"


def _deployment_source() -> str:
    return DEPLOYMENT.read_text(encoding="utf-8")


def _configmap_source() -> str:
    return CONFIGMAP.read_text(encoding="utf-8")


def _env_value(source: str, name: str) -> str:
    match = re.search(
        rf"(?m)^\s+- name: {re.escape(name)}\s*\n\s+value:\s*\"?([^\"\n]+)\"?\s*$",
        source,
    )
    assert match is not None, f"Missing env var {name}"
    return match.group(1).strip()


def _empty_dir_size_limit(source: str, name: str) -> str:
    match = re.search(
        rf"(?ms)^\s+- name: {re.escape(name)}\s*\n\s+emptyDir:\s*\n\s+sizeLimit:\s*\"?([^\"\n]+)\"?\s*$",
        source,
    )
    assert match is not None, f"Missing emptyDir sizeLimit for volume {name}"
    return match.group(1).strip()


def _configmap_value(source: str, name: str) -> str:
    match = re.search(
        rf"(?m)^\s+{re.escape(name)}:\s*\"?([^\"\n]+)\"?\s*$",
        source,
    )
    assert match is not None, f"Missing ConfigMap key {name}"
    return match.group(1).strip()


def test_duckdb_spill_uses_dedicated_empty_dir_volume() -> None:
    source = _deployment_source()

    assert _env_value(source, "BDW_DUCKDB_TEMP_DIRECTORY") == "/workspace/tmp/duckdb-spill"
    assert re.search(
        r"(?ms)^\s+- name: duckdb-spill\s*\n\s+mountPath:\s*/workspace/tmp/duckdb-spill\s*$",
        source,
    )
    assert re.search(
        r"(?ms)^\s+- name: workspace\s*\n\s+mountPath:\s*/workspace\s*$",
        source,
    )


def test_duckdb_spill_volume_exceeds_configured_duckdb_spill_quota() -> None:
    source = _deployment_source()
    duckdb_spill_quota = parse_storage_size_bytes(
        _env_value(source, "BDW_DUCKDB_MAX_TEMP_DIRECTORY_SIZE")
    )
    spill_volume_limit = parse_storage_size_bytes(_empty_dir_size_limit(source, "duckdb-spill"))

    assert duckdb_spill_quota is not None
    assert spill_volume_limit is not None
    assert spill_volume_limit > duckdb_spill_quota


def test_daca_configmap_matches_rhos_service_contract() -> None:
    source = _configmap_source()

    assert _configmap_value(source, "DACA_BASE_URL") == "http://daca-catalog-api:8001"
    assert (
        _configmap_value(source, "DACA_OPA_URL")
        == "http://daca-opa:8181/v1/data/daca/authz/decision"
    )
    assert _configmap_value(source, "DACA_UI_URL").startswith("https://")
    assert _configmap_value(source, "DAAIF_PUBLIC_BASE_URL").startswith("https://")

    deployment = _deployment_source()
    assert re.search(
        r"(?ms)^\s+envFrom:\s*\n\s+- configMapRef:\s*\n"
        r"\s+name: tib-daail-evo1-poc-query-engine-config\s*$",
        deployment,
    )

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SMOKE_MANIFEST_PATH = REPO_ROOT / "scripts" / "playwright-smokes.json"
PLAYWRIGHT_SCOPE = os.environ.get("RUN_PLAYWRIGHT_SMOKES", "").strip().lower()


def load_smoke_manifest() -> list[dict[str, object]]:
    payload = json.loads(SMOKE_MANIFEST_PATH.read_text(encoding="utf-8"))
    smokes = payload.get("smokes")
    if not isinstance(smokes, list):
        raise RuntimeError(
            "The Playwright smoke manifest does not define a 'smokes' list."
        )
    return [smoke for smoke in smokes if isinstance(smoke, dict)]


def smoke_ids() -> list[str]:
    return [
        str(smoke.get("id") or smoke.get("script") or "playwright-smoke")
        for smoke in load_smoke_manifest()
    ]


pytestmark = [pytest.mark.playwright, pytest.mark.no_cover]


@pytest.mark.skipif(
    not PLAYWRIGHT_SCOPE,
    reason=(
        "Set RUN_PLAYWRIGHT_SMOKES=1 or RUN_PLAYWRIGHT_SMOKES=all "
        "to run browser regression smokes."
    ),
)
@pytest.mark.parametrize("smoke", load_smoke_manifest(), ids=smoke_ids())
def test_playwright_smoke(smoke: dict[str, object]) -> None:
    if PLAYWRIGHT_SCOPE != "all" and not bool(smoke.get("enabledByDefault")):
        pytest.skip(
            "Extended Playwright smoke is disabled in the default scope."
        )

    script_name = str(smoke.get("script") or "").strip()
    if not script_name:
        raise AssertionError(
            "Playwright smoke manifest entry is missing its script name."
        )

    script_path = REPO_ROOT / "scripts" / script_name
    if not script_path.exists():
        raise AssertionError(
            f"Playwright smoke script does not exist: {script_path}"
        )

    command = [
        sys.executable,
        str(script_path),
        "--base-url",
        os.environ.get("PLAYWRIGHT_BASE_URL", "http://127.0.0.1:8000"),
        "--timeout-ms",
        os.environ.get("PLAYWRIGHT_TIMEOUT_MS", "30000"),
    ]
    if os.environ.get("PLAYWRIGHT_HEADED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        command.append("--headed")

    result = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        f"Playwright smoke failed: {script_name}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )

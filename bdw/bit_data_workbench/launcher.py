from __future__ import annotations

from pathlib import Path

import uvicorn

from .config import env, env_bool


def logging_config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "logging.json"


def file_logging_enabled() -> bool:
    return env_bool("BDW_ENABLE_FILE_LOGGING", False)


def build_uvicorn_run_kwargs() -> dict[str, object]:
    kwargs: dict[str, object] = {
        "host": env("HOST", "0.0.0.0"),
        "port": int(env("PORT", "8000")),
    }
    if file_logging_enabled():
        kwargs["log_config"] = str(logging_config_path())
    return kwargs


def main() -> None:
    uvicorn.run("bit_data_workbench.main:app", **build_uvicorn_run_kwargs())


if __name__ == "__main__":
    main()

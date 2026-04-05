from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from pathlib import Path
import time

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .api.router import router as api_router
from .backend.service import WorkbenchService
from .config import Settings
from .web.router import router as web_router


BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)
STARTUP_DIVIDER = "-------------------------------"


def _render_startup_line(message: str) -> str:
    if message.startswith("[bdw-startup] "):
        return message
    return f"[bdw-startup] {message}"


def _log_startup(
    message: str,
    *args: object,
    level: int = logging.INFO,
    exc_info: bool = False,
) -> None:
    logger.log(level, _render_startup_line(message), *args, exc_info=exc_info)


def _log_startup_line(line: str) -> None:
    logger.info(_render_startup_line(line))


def _log_startup_section(title: str) -> None:
    _log_startup(STARTUP_DIVIDER)
    _log_startup("Startup task: %s", title)


@asynccontextmanager
async def lifespan(app: FastAPI):
    started = time.perf_counter()
    _log_startup_section("FastAPI lifespan startup")
    _log_startup("FastAPI lifespan startup begin")
    # There is no explicit "local" or "cluster" mode switch in the app.
    # The same startup path runs everywhere and the launcher/deployment decides
    # the runtime behavior by injecting different environment variables.
    _log_startup_section("Load settings from environment")
    settings = Settings.from_env()
    _log_startup("Settings loaded from environment")
    _log_startup_section("Apply runtime environment")
    settings.apply_runtime_environment()
    _log_startup("Runtime environment applied")
    _log_startup_section("Print startup environment summary")
    for line in Settings.startup_environment_lines():
        _log_startup_line(line)
    _log_startup_section("Print masked os.environ dump")
    for line in Settings.startup_all_environment_lines():
        _log_startup_line(line)
    _log_startup_section("Print visible config mounts and files")
    for line in Settings.startup_config_lines():
        _log_startup_line(line)
    _log_startup_section("Inspect S3 certificate mapping")
    for line in settings.startup_s3_certificate_lines():
        _log_startup_line(line)
    workbench = WorkbenchService(settings)
    _log_startup_section("Start workbench service")
    _log_startup("Starting workbench service")
    try:
        workbench.start()
    except Exception:
        _log_startup("Workbench service startup failed", level=logging.ERROR)
        logger.exception("Workbench service startup failed")
        raise
    _log_startup(
        "Workbench service startup completed in %.2fs",
        time.perf_counter() - started,
    )
    app.state.workbench = workbench
    try:
        yield
    finally:
        _log_startup_section("FastAPI lifespan shutdown")
        _log_startup("FastAPI lifespan shutdown begin")
        workbench.stop()
        _log_startup("FastAPI lifespan shutdown complete")


app = FastAPI(title="DAAIFL Workbench", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/node", StaticFiles(directory=BASE_DIR / "static" / "vendor" / "node"), name="node")
app.include_router(api_router)
app.include_router(web_router)

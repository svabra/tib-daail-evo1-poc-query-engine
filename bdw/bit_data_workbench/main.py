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


def _print_startup_section(title: str) -> None:
    print(f"[bdw-startup] {STARTUP_DIVIDER}", flush=True)
    print(f"[bdw-startup] Startup task: {title}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    started = time.perf_counter()
    _print_startup_section("FastAPI lifespan startup")
    print("[bdw-startup] FastAPI lifespan startup begin", flush=True)
    # There is no explicit "local" or "cluster" mode switch in the app.
    # The same startup path runs everywhere and the launcher/deployment decides
    # the runtime behavior by injecting different environment variables.
    _print_startup_section("Load settings from environment")
    settings = Settings.from_env()
    print("[bdw-startup] Settings loaded from environment", flush=True)
    _print_startup_section("Apply runtime environment")
    settings.apply_runtime_environment()
    print("[bdw-startup] Runtime environment applied", flush=True)
    _print_startup_section("Print startup environment summary")
    for line in Settings.startup_environment_lines():
        print(line, flush=True)
    _print_startup_section("Print masked os.environ dump")
    for line in Settings.startup_all_environment_lines():
        print(line, flush=True)
    _print_startup_section("Print visible config mounts and files")
    for line in Settings.startup_config_lines():
        print(line, flush=True)
    _print_startup_section("Inspect S3 certificate mapping")
    for line in settings.startup_s3_certificate_lines():
        print(line, flush=True)
    workbench = WorkbenchService(settings)
    _print_startup_section("Start workbench service")
    print("[bdw-startup] Starting workbench service", flush=True)
    try:
        workbench.start()
    except Exception:
        print("[bdw-startup] Workbench service startup failed", flush=True)
        logger.exception("Workbench service startup failed")
        raise
    print(
        f"[bdw-startup] Workbench service startup completed in {time.perf_counter() - started:.2f}s",
        flush=True,
    )
    app.state.workbench = workbench
    try:
        yield
    finally:
        _print_startup_section("FastAPI lifespan shutdown")
        print("[bdw-startup] FastAPI lifespan shutdown begin", flush=True)
        workbench.stop()
        print("[bdw-startup] FastAPI lifespan shutdown complete", flush=True)


app = FastAPI(title="DAAIFL Workbench", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/node", StaticFiles(directory=BASE_DIR / "static" / "vendor" / "node"), name="node")
app.include_router(api_router)
app.include_router(web_router)

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .api.router import router as api_router
from .backend.service import WorkbenchService
from .config import Settings
from .web.router import router as web_router


BASE_DIR = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    for line in Settings.startup_environment_lines():
        print(line, flush=True)
    workbench = WorkbenchService(settings)
    workbench.start()
    app.state.workbench = workbench
    try:
        yield
    finally:
        workbench.stop()


app = FastAPI(title="DAAIFL Workbench", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.mount("/node", StaticFiles(directory=BASE_DIR / "static" / "vendor" / "node"), name="node")
app.include_router(api_router)
app.include_router(web_router)

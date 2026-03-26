from __future__ import annotations

import os
import socket
from datetime import UTC, datetime

from fastapi import FastAPI, Request


app = FastAPI(title="bit-data-workbench")


def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip() or default


def runtime_info() -> dict[str, str]:
    return {
        "service": "bit-data-workbench",
        "image_version": env("IMAGE_VERSION", "dev"),
        "hostname": socket.gethostname(),
        "pod_name": env("POD_NAME", "unknown"),
        "pod_namespace": env("POD_NAMESPACE", "unknown"),
        "pod_ip": env("POD_IP", "unknown"),
        "node_name": env("NODE_NAME", "unknown"),
        "timestamp_utc": datetime.now(UTC).isoformat(),
    }


@app.get("/")
def root() -> dict[str, object]:
    return {
        "ok": True,
        "message": "Bit Data Workbench is reachable.",
        "runtime": runtime_info(),
    }


@app.get("/info")
def info() -> dict[str, object]:
    return {
        "ok": True,
        "runtime": runtime_info(),
    }


@app.get("/headers")
async def headers(request: Request) -> dict[str, object]:
    return {
        "ok": True,
        "runtime": runtime_info(),
        "headers": dict(request.headers),
        "url": str(request.url),
    }

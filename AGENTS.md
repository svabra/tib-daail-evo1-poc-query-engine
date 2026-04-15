# Repository Guide

## Scope
This file applies to the whole repository unless a deeper AGENTS.md overrides it.

## Architecture Map
- `bdw/bit_data_workbench/` is the FastAPI application package.
- `scripts/` contains local startup helpers and Playwright smoke scripts.
- `tests/` contains the automated test suite.
- `docker/` and `k8s/` contain local and cluster infrastructure code.

## High-value Commands
- Install dependencies: `./.venv/Scripts/python.exe -m pip install -r bdw/requirements.txt`
- Run tests with coverage: `./.venv/Scripts/python.exe -m pytest`
- Start local F5 dependencies: `./scripts/start-f5-dependencies.ps1`
- Start the local app: `./scripts/start-bdw-dev.ps1`

## Repo-specific Guardrails
- Prefer extracting code out of `backend/service.py` and `static/js/app.js` instead of growing them.
- Keep API and web routers thin; put behavior in backend modules or dedicated facades/managers.
- Unified realtime transport is one SSE stream at `/api/events/stream`.
- Local startup-sensitive networking should normalize `localhost`-style endpoints to `127.0.0.1` when integration libraries are sensitive to IPv6 resolution.
- Do not modify `workspace/`, `logs/`, `.local/`, `.venv/`, or `static/vendor/` as part of ordinary feature work.
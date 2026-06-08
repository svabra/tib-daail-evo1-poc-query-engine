# Scripts Folder Guide

## Responsibility
- Local development startup helpers and browser smoke-test utilities.

## Working Rules
- `start-f5-dependencies.ps1` and `start-bdw-dev.ps1` are the canonical local launchers.
- Keep startup scripts resilient to port conflicts and local loopback quirks.
- Browser regression checks live in the Playwright Python scripts here.
- Do not move application business logic into this folder.
- The harness scripts require a `.venv` interpreter at `.venv\Scripts\python.exe`; if missing, they create it and install `bdw/requirements.txt` before running.

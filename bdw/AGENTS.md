# bdw Folder Guide

## Responsibility
- `app.py` is the top-level Python entry point for the local app container/runtime.
- `requirements.txt` is the shared local development dependency set, including test tooling.

## Working Rules
- Keep importable application code inside `bit_data_workbench/`, not beside `app.py`.
- When adding dependencies, pin them and keep the workspace virtual environment flow working.
- Treat this folder as packaging and launch glue, not a place for feature logic.
# bit_data_workbench Package Guide

## Responsibility
- `main.py` wires the FastAPI app and mounted routers.
- `config.py`, `models.py`, `dependencies.py`, `launcher.py`, and `release_notes.py` are package-level cross-cutting modules.
- `api/` and `web/` own HTTP surfaces; `backend/` owns behavior and state; `data_generator/` owns seeded loader modules.

## Working Rules
- Keep composition in `main.py`; keep domain behavior in `backend/`.
- Preserve stable payload aliases defined in `models.py`.
- `static/js/` and `templates/` share `data-*` hooks; change both sides together.
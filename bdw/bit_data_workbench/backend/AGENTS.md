# Backend Folder Guide

## Responsibility
- Core domain layer for notebooks, query jobs, exports, source discovery, S3/PostgreSQL integration, and realtime coordination.
- `service.py` is still the top-level facade/orchestrator.

## Working Rules
- Prefer adding focused helpers, facades, or modules instead of growing `service.py` further.
- Reuse `sql_utils.py` for quoting and startup-view parsing.
- Reuse `notebook_presets.py` for built-in notebook definitions and SQL templates.
- Keep unified realtime behavior compatible with `/api/events/stream` and `client-connections` snapshots.

## Validation
- Start with the narrowest backend test that covers the touched behavior.
- Run the full suite before finishing larger refactors.
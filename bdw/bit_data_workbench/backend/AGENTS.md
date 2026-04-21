# Backend Folder Guide

## Responsibility
- Core domain layer for notebooks, query jobs, exports, source discovery, S3/PostgreSQL integration, JSON-backed shared state, and realtime coordination.
- `service.py` is still the top-level facade/orchestrator.
- The current query-engine cluster is still spread across `query_jobs.py`, `query_analysis.py`, `query_result_exports.py`, `sql_utils.py`, `s3_explorer.py`, and `s3_storage.py`.

## Working Rules
- Prefer adding focused helpers, facades, or modules instead of growing `service.py` further.
- For JSON-backed registries, follow the atomic read/write pattern in `shared_notebooks.py`.
- If Data Products or similar publication features land here, prefer a dedicated package such as `backend/data_products/` with `service.py` only forwarding calls.
- Reuse `sql_utils.py` for quoting and startup-view parsing.
- Reuse `notebook_presets.py` for built-in notebook definitions and SQL templates.
- Keep public read paths separate from query-job lifecycle logic and SSE event emission.
- Keep unified realtime behavior compatible with `/api/events/stream` and `client-connections` snapshots.

## Validation
- Start with the narrowest backend test that covers the touched behavior.
- Run the full suite before finishing larger refactors.

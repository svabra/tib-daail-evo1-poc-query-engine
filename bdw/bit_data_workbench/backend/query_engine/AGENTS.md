# Query Engine Guide

## Scope
- This folder is the intended landing zone for reusable query-runtime helpers when they are extracted from the backend root.
- Today, most query-engine responsibilities still live in sibling modules such as `../query_jobs.py`, `../query_analysis.py`, `../query_result_exports.py`, `../sql_utils.py`, `../s3_explorer.py`, and `../s3_storage.py`.

## Responsibility
- Shared read-only data access helpers.
- Pagination, projection, and contract-shaping helpers that should not depend on notebook or query-job state.
- Reusable adapters for live publication features such as Data Products.

## Working Rules
- Keep read paths side-effect free.
- Keep request/response contracts out of this folder; that belongs in `api/` and `models.py`.
- Avoid coupling reusable reads to SSE, job management, or browser-specific concepts.
- Centralize default limits, max limits, and streaming/content-type decisions here if multiple features need them.

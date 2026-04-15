# PostgreSQL Data Source Guide

## Responsibility
- PostgreSQL provider implementations for plugin and explorer behavior.

## Working Rules
- Keep `pg_oltp`, `pg_olap`, and native/direct flows behaviorally stable.
- Preserve connection behavior used by local DuckDB attachment/bootstrap flows.
- Do not leak PostgreSQL-specific branching back into generic source abstractions.
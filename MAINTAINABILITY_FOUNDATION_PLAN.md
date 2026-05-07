# Maintainability Foundation Plan

## Goal

Make this repository easier to maintain for the next decade without a rewrite.
The work should reduce accidental complexity, make ownership boundaries clearer,
and prepare the codebase for a later separation of backend, API, DuckDB query
engine, frontend, and infrastructure concerns.

This is a long-term TODO. It documents the direction for future implementation
branches; it does not require the maintainability work to happen all at once.

## Principles

- Delete confirmed unused code before adding abstractions.
- Extract code only around real responsibilities that already exist.
- Preserve public endpoint paths, response shapes, browser behavior, and local
  development commands unless a later task explicitly changes them.
- Keep `WorkbenchService` as a compatibility facade while moving behavior into
  focused backend modules.
- Keep routers thin: parse requests, call application services, shape responses.
- Keep frontend page bootstrap small and move repeated DOM behavior into
  feature controllers.
- Prefer boring Python modules and narrow interfaces over architecture theater.
- Do not introduce microservices, framework migrations, or broad rewrites as
  part of this foundation work.

## Target Boundaries

- `api`: HTTP request parsing, validation, response models, and streaming
  responses. Feature routers should be split out of the large router over time.
- `web`: HTML routes, page context assembly, and template selection.
- `backend/application`: use-case orchestration and facade methods that combine
  managers, data sources, and runtime events.
- `backend/query_engine`: DuckDB execution, query planning helpers, source
  registration, file readers, and result materialization.
- `backend/storage`: S3, PostgreSQL, and Local Workspace transfer adapters.
- `backend/ingestion_types`: format-specific ingestion behavior plus shared
  upload, archive, and destination primitives.
- `static/js`: feature controllers by workbench area, with shared UI utilities
  for repeated browser behavior.

## Workstreams

### 1. Inventory hotspots and dead code

- Measure largest modules, highest churn areas, and high fan-in/fan-out imports.
- Review obvious hotspots first: `backend/service.py`, `api/router.py`, and
  `static/js/app.js`.
- Use tests, search, and cautious static tools to identify unused functions,
  exports, scripts, and templates.
- Remove only code with clear evidence that it is unused or superseded.

### 2. Reduce god modules safely

- Move coherent behavior out of `backend/service.py` into existing managers or
  new focused modules.
- Split API endpoints into feature routers when a group can move without
  changing public paths.
- Keep the old facade methods as pass-throughs until call sites are naturally
  migrated.
- Avoid mixed commits: each commit should extract one responsibility or delete
  one confirmed dead area.

### 3. Isolate DuckDB query-engine concerns

- Move DuckDB-specific SQL generation, source registration, and file reader
  logic behind query-engine modules.
- Keep data-source plugins focused on discovery, object operations, and
  credentials rather than query construction.
- Preserve current in-process DuckDB execution. A separate query-engine service
  is a future deployment decision, not part of this foundation.

### 4. Keep frontend maintainable

- Continue extracting workbench-specific browser logic from `static/js/app.js`.
- Prefer one controller per workbench or feature area.
- Keep templates responsible for markup and data attributes, not hidden
  business rules.
- Add Playwright coverage before moving browser flows with user-visible risk.

### 5. Prepare future component separation

- Keep stable contracts between API, application services, query engine,
  storage adapters, and frontend controllers.
- Introduce DTOs or schema modules only when they prevent coupling across those
  boundaries.
- Keep local development and deployment behavior unchanged while boundaries are
  clarified inside the monolith.

## Execution Model

- Create one branch per maintainability slice.
- Prefer small commits that can be reviewed and reverted independently.
- Add or identify tests before moving risky behavior.
- Run targeted tests for the moved area and the full pytest suite before
  merging larger refactors.
- Run relevant Playwright smokes when frontend behavior, browser downloads,
  ingestion, query handoff, or sidebar flows are touched.

## Non-goals

- No big-bang rewrite.
- No microservice split in this phase.
- No framework migration.
- No public API behavior changes.
- No broad formatting-only churn.
- No new abstraction unless it removes real duplication or makes an existing
  responsibility clear.

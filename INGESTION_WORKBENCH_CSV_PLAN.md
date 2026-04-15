# Loader Workbench and CSV Ingestion Plan

## Goal

Split the current loader-centric ingestion surface into two distinct workbenches:

- `Loader Workbench`: run existing Python loader modules and monitor their jobs.
- `Ingestion Workbench`: start from an empty ingestion landing page with tiles for user-driven ingestion flows.

The first user-driven ingestion flow is drag-and-drop CSV import into:

- `PostgreSQL OLTP`
- `PostgreSQL OLAP`
- `Shared Workspace (S3)`
- `Local Workspace (browser IndexedDB)`

## Current State

- `/ingestion-workbench` is currently the loader runbook screen.
- Loader execution is backed by `DataGenerationJobManager`.
- S3 writes already exist through `backend/s3_storage.py` and the S3 data source plugin.
- PostgreSQL connectivity already exists through `psycopg` and the PostgreSQL data source plugins.
- Local Workspace is browser-local IndexedDB and is already used for saved result exports.

## Design Decisions

### 1. Workbench split

- Move the current loader UI from `/ingestion-workbench` to `/loader-workbench`.
- Keep the existing loader backend and realtime job monitoring intact.
- Rebuild `/ingestion-workbench` as a clean landing page for ingestion tools.

### 2. First ingestion flow

The first ingestion tile is `CSV Files`.

The user flow is:

1. Open `Ingestion Workbench`.
2. Click the `CSV Files` tile.
3. Drop one or more `.csv` files or browse for files.
4. Choose a destination target.
5. Provide destination-specific settings.
6. Review the resolved target names.
7. Start the import.
8. See per-file success or failure.
9. Open the target data source from the sidebar or continue with more imports.

### 3. Target behavior

#### PostgreSQL OLTP / OLAP

- Server-side import.
- Infer columns from the CSV with DuckDB `read_csv_auto`.
- Normalize column names to SQL-safe identifiers.
- Create a target table in the chosen schema.
- Load the CSV into PostgreSQL with `COPY ... FROM STDIN`.
- Refresh source discovery after import so the table becomes visible in the sidebar.

#### Shared Workspace (S3)

- Server-side import.
- Save the uploaded CSV file as-is into the chosen bucket/prefix.
- Create the bucket if needed.
- Refresh source discovery after import so the object becomes visible in the sidebar.

#### Local Workspace

- Browser-side import.
- Store the dropped CSV file blob in IndexedDB using the existing Local Workspace export store.
- Reuse the existing Local Workspace folder model and sidebar rendering.

## UI Plan

## Slice 1: Workbench split

- Add a new `Loader Workbench` route and move the current runbook UI there.
- Convert linked notebook loader buttons and recent-ingestion shortcuts to open `Loader Workbench`.
- Keep the loader sidebar and monitor behavior on that route.
- Make `Ingestion Workbench` a separate landing page with no runbook content.

## Slice 2: CSV landing experience

- Add a large `CSV Files` tile on the Ingestion landing page.
- Add a guided panel under the tile with four visible steps:
  - `Step 1`: choose files
  - `Step 2`: choose destination
  - `Step 3`: configure destination
  - `Step 4`: import and review
- Show a file list with size and resolved base names before import.
- Keep placeholder tiles for future ingestion types, but disabled.

## Slice 3: CSV import behavior

- `Local Workspace` path is client-side only.
- `S3` and `PostgreSQL` paths go through a new backend ingestion endpoint.
- Show destination-specific forms:
  - `Local Workspace`: folder path
  - `S3`: bucket and optional prefix
  - `PostgreSQL`: target instance, schema, table mode
- Default target names derive from the CSV file names.

## Slice 4: Result feedback

- After import, show a per-file result card:
  - status
  - destination
  - resolved table or object path
  - row count when available
  - error message when failed
- Refresh the relevant source tree after successful imports.

## Backend Plan

## Module split

Add a focused backend module instead of expanding `service.py` further:

- `backend/csv_ingestion.py`

Responsibilities:

- validate CSV import requests
- infer CSV schema
- map DuckDB types to PostgreSQL types
- upload CSV files to S3
- import CSV files into PostgreSQL
- return normalized per-file import results

`WorkbenchService` should stay as the facade and call into this manager.

## API surface

Add endpoints for the CSV flow:

- `GET /api/ingestion/targets`
- `POST /api/ingestion/csv/import`

The `POST` endpoint should accept multipart form data with:

- one or more files
- `targetId`
- destination-specific fields such as `bucket`, `prefix`, `schemaName`, `tablePrefix`, `folderPath`

## Testing Plan

## Backend regression tests

- table-name normalization
- DuckDB-to-PostgreSQL type mapping
- S3 CSV upload behavior
- PostgreSQL CSV import DDL and COPY behavior
- service facade dispatch for each supported target

These should stay small and reusable as future unit tests.

## Browser regression tests

- current loader smoke updated to use `/loader-workbench`
- new ingestion smoke to verify:
  - the CSV tile renders
  - the file chooser updates the review list
  - Local Workspace CSV import stores files and updates the sidebar

## dlt decision

Do not introduce `dlt` in the first slice.

Reason:

- the repo already has direct S3, PostgreSQL, DuckDB, and browser-local primitives
- the first CSV importer is narrow and can be implemented with less risk using those primitives
- `dlt` becomes more attractive later when we add resumable pipelines, schema evolution policies, transforms, and more destinations

The implementation should stay compatible with swapping in a `dlt`-backed server pipeline later for the server-side targets.

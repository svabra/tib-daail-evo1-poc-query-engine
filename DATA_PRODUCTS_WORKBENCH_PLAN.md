# Data Products Workbench and Managed Publication Plan

## Goal

Add a new `Data Products` workbench at `/data-products` that:

- shows published data products immediately on first load
- supports page-level search and metadata-driven browsing
- provides a guided `Create data product` flow from both the page and existing source menus
- makes this app the source of truth for v1 live, read-only publications

The published product should be registered once in this app, then served live through stable read-only endpoints in the same deployment.

## Current Repo Fit

- The app already follows a `web -> api -> backend` split, with `WorkbenchService` as the facade.
- Workbench pages are server-rendered partials in `templates/partials/` and activated through `static/js/app.js` plus extracted controllers.
- Source-object action menus are split between server-rendered sidebar markup in `templates/partials/sidebar.html` and browser-rendered Local Workspace markup in `static/js/local-workspace-sidebar.js`.
- JSON-backed shared state already exists through `backend/shared_notebooks.py`, persisted beside the configured DuckDB database file.
- Query runtime behavior is still in-process and lives across `backend/query_jobs.py`, `backend/query_analysis.py`, `backend/query_result_exports.py`, `backend/sql_utils.py`, `backend/s3_explorer.py`, and `backend/s3_storage.py`.

## Repo-Specific Adjustments

### 1. Keep absolute URLs out of persisted state

The original plan stores `publishedUrl` directly. For this repo that is brittle because the same state can move between local runs, Docker, and cluster deployments.

Adjustment:

- persist `publicPath`, for example `/api/public/data-products/{slug}`
- derive `publishedUrl` from the current request base URL when returning API payloads or rendering the page

### 2. Add a focused backend package instead of growing `service.py`

Follow the existing shared-notebook pattern, but do not place registry logic directly in `backend/service.py`.

Recommended package:

- `bdw/bit_data_workbench/backend/data_products/registry.py`
- `bdw/bit_data_workbench/backend/data_products/manager.py`
- `bdw/bit_data_workbench/backend/data_products/sources.py`
- `bdw/bit_data_workbench/backend/data_products/readers.py`

`WorkbenchService` should only expose thin pass-through methods and a path helper such as `data-products.json` beside `shared-notebooks.json`.

### 3. Keep routers thin by splitting new endpoints out of the large router file

`api/router.py` is already large. Add a dedicated API module and include its router rather than extending the existing file indefinitely.

Recommended module:

- `bdw/bit_data_workbench/api/data_products.py`

Do the same on the HTML side if the workbench context grows beyond a few helper functions:

- `bdw/bit_data_workbench/web/data_products.py`

### 4. Do not introduce a separate query-engine service in v1

This repo currently runs DuckDB in-process and already has the right primitives for read-only publication. A separate `query_engine` service would add architectural weight without helping the first slice.

Adjustment:

- keep v1 publication reads in-process
- isolate reusable read helpers under backend modules
- only extract a larger `query_engine/` package if future work genuinely reuses pagination, streaming, or contract-shaping logic across multiple features

### 5. Keep the Local Workspace path visible but explicitly unsupported

This matches the current product model:

- `Shared Workspace` is backend-visible
- `Local Workspace` is browser-only IndexedDB

So the source-menu entry should stay visible for Local Workspace files, but the guide should enter a blocked state with a clear move-to-shared-workspace message.

### 6. Add low-cost metadata now to prepare for DataHub later

The original plan already includes `title`, `description`, and a stable public endpoint. For this repo it is worth adding a little more metadata immediately because it gives search/filter value now and reduces migration work later.

Recommended optional metadata in v1:

- `owner`
- `domain`
- `tags`
- `sourcePlatform`
- `sourceDisplayName`
- `accessLevel`
- `accessNote`
- `requestAccessContact`
- `customProperties`

These can remain advisory for now. No hard authorization model needs to ship in the first slice.

## Folder Plan

### Frontend

### HTML and route composition

- Add a new partial: `templates/partials/data_products_workbench.html`
- Add a new page route: `/data-products`
- Add a home tile in `templates/partials/home.html`
- Wire workbench navigation through the same pattern already used for `Query`, `Data Source`, `Ingestion`, and `Loader`

### JS split

Avoid growing `static/js/app.js` with more large render blocks.

Recommended modules:

- `static/js/data-products-ui.js`
- `static/js/data-products-controller.js`
- `static/js/data-products-sample-contracts.js`

Touch existing files only for wiring:

- `static/js/app.js`
- `static/js/workbench-navigation-controller.js`
- `static/js/home-ui.js`
- `static/js/source-sidebar-click-controller.js`
- `static/js/dialogs.js` if the guided publication dialog stays client-rendered

### Source menu integration

Update both sidebar render paths:

- `templates/partials/sidebar.html` for S3 buckets, objects, PostgreSQL tables, and views
- `static/js/local-workspace-sidebar.js` for browser-local files

This keeps the `Create data product` action label consistent regardless of where the menu came from.

### Guided flow shape

Use one multi-step dialog:

1. Select or confirm source
2. Show compatibility and live/read-only behavior
3. Collect `title`, `slug`, and `description`
4. Show endpoint details, OpenAPI namespace, sample contract, and publish

If launched from a source menu, lock the source and start at step 2.

### Backend

### Data model

Add dataclasses in `models.py` for persisted/public domain state:

- `DataProductDefinition`
- `DataProductSourceDescriptor`

Recommended persisted fields:

- `productId`
- `slug`
- `title`
- `description`
- `publicationMode = "live"`
- `publicPath`
- `sourceKind`
- `sourceId`
- source locator fields:
  - `relation` for relations
  - `bucket`
  - `key`
- metadata fields:
  - `owner`
  - `domain`
  - `tags`
  - `sourcePlatform`
  - `sourceDisplayName`
  - `accessLevel`
  - `accessNote`
  - `requestAccessContact`
  - `customProperties`
- `createdAt`
- `updatedAt`

Expose `publishedUrl` as a derived response field rather than a stored field.

### Registry persistence

Model the store after `backend/shared_notebooks.py`.

Recommended store path:

- `settings.duckdb_database.parent / "data-products.json"`

Responsibilities:

- atomic read/write
- slug uniqueness
- create/update/delete lifecycle
- normalization of optional metadata fields

### Source normalization

Add a focused source-normalization layer that converts selected sidebar objects into publishable descriptors and rejects browser-local sources for live publication.

This layer should understand:

- PostgreSQL tables/views
- Shared Workspace S3 buckets
- Shared Workspace S3 objects/files
- Local Workspace files as unsupported-for-live-publication entries

### Service facade

`WorkbenchService` should only gain thin methods such as:

- `list_data_products`
- `preview_data_product`
- `create_data_product`
- `update_data_product_metadata`
- `delete_data_product`
- `read_public_data_product`

### Query Engine

This repo does not have a separate query-engine package today. The query engine is currently the backend query-runtime cluster.

### v1 approach

Keep Data Products live reads inside the current process and reuse existing primitives:

- DuckDB worker connections for relation reads
- `s3_explorer` helpers for bucket listings
- `s3_storage.download_s3_file` plus existing content-type information for object/file responses

### Extraction boundary

If Data Products adds enough read-path logic to justify a reusable cluster, place it under:

- `backend/data_products/readers.py` first
- later, if needed, under `backend/query_engine/`

Do not couple public data-product reads to query-job creation, notebook state, or realtime events.

### Relation contract

`GET /api/public/data-products/{slug}` for relation-backed products should return:

- `product`
- `columns`
- `items`
- `limit`
- `offset`
- `hasMore`

Default `limit = 100`, maximum `limit = 1000`.

### Bucket and object contract

Bucket-backed products should return:

- `product`
- `prefix`
- `entries`

Object/file-backed products should stream raw content with original or inferred media type.

### API

### Management endpoints

Add management endpoints under `/api/data-products`:

- `GET /api/data-products`
- `POST /api/data-products/preview`
- `POST /api/data-products`
- `PUT /api/data-products/{product_id}`
- `DELETE /api/data-products/{product_id}`

`PUT` should update metadata only in v1:

- `title`
- `description`
- optional advisory metadata such as `owner`, `domain`, `tags`, and access notes

### Public endpoints

Add one stable public namespace:

- `GET /api/public/data-products/{slug}`

Keep `slug` unique and immutable after creation so public URLs remain stable.

### OpenAPI structure

Document the feature once in OpenAPI with separate tags:

- `data-products`
- `data-products-public`

Keep request models in the API layer and domain/persisted models in `models.py`.

### Request-derived fields

Whenever the management API returns a product, include:

- `publicPath`
- `publishedUrl`

with `publishedUrl` derived from the current request base URL.

## DataHub Readiness With Immediate Value

These are low-cost additions that help now and later:

### 1. Better metadata collection

Require or strongly encourage:

- business description
- owner
- domain
- tags

Immediate value:

- better landing-page search
- better card grouping and filtering
- better API/OpenAPI descriptions

### 2. Stable external identifiers

Add a field such as `catalogKey` or `externalId` derived from the slug and source platform.

Immediate value:

- easier export/import
- easier future sync to DataHub URNs
- stable references in logs and support flows

### 3. Pseudo access management

Add advisory metadata now:

- `accessLevel` such as `internal` or `restricted`
- `accessNote`
- `requestAccessContact`

Immediate value:

- users can at least see intended handling rules
- cards can display handling badges
- this creates a migration path toward real auth later

### 4. Source and lineage hints

Persist upstream source metadata:

- source platform
- source kind
- source locator
- source display name

Immediate value:

- users understand where the product came from
- future DataHub lineage mapping becomes straightforward

### 5. OpenAPI as catalog artifact

Give each product a stable contract description and keep the public namespace clearly documented.

Immediate value:

- consumers can self-serve against a predictable API
- future catalog sync has a clean machine-readable source of endpoint metadata

## Suggested Delivery Slices

### Slice 1

- data model
- JSON registry store
- management API
- focused backend tests

### Slice 2

- public read endpoints for relation, bucket, and object/file products
- response contracts
- API tests

### Slice 3

- `/data-products` page
- home tile
- search and product cards
- workbench navigation wiring

### Slice 4

- guided publication dialog
- source-menu entry points
- unsupported Local Workspace path

### Slice 5

- optional metadata for owner/domain/tags/access notes
- improved OpenAPI descriptions
- future catalog export hooks

## Test Plan

### Backend unit tests

- registry store serialization and atomic writes
- slug uniqueness
- create/update/delete lifecycle
- preview validation for each supported source kind
- rejection path for Local Workspace publication attempts
- relation pagination
- bucket prefix listing
- file/object streaming behavior

Recommended new test files:

- `tests/test_data_products_store.py`
- `tests/test_data_products_service.py`
- `tests/test_data_products_public_api.py`

### Web and API route tests

- `Data Products` page renders correctly as full page and partial
- home tile opens the new workbench
- management endpoints return expected shapes
- public endpoints return the expected relation, bucket, and object/file contracts

Recommended new test files:

- `tests/test_data_products_routes.py`
- `tests/test_data_products_sidebar_labels.py`

### Frontend tests

- sidebar source menus show `Create data product` for supported sources
- Local Workspace files still show the action but enter the blocked flow
- landing-page search filters cards by title, slug, description, source name, and endpoint path
- guided flow works both from the page CTA and from a source-menu shortcut

### Playwright smoke

- open `Data Products`
- create a product from an eligible source object
- verify the card appears
- open the published endpoint successfully

## Assumptions and Non-Goals

- v1 publishes live, read-only products only
- v1 does not add writes, snapshots, filter DSL, sorting, or projection controls
- v1 supports only backend-visible sources:
  - PostgreSQL tables/views
  - Shared Workspace S3 buckets/objects/files
- Local Workspace browser files can enter the guide but cannot publish live products
- no real auth/authorization model is added in v1
- public endpoints stay inside the same deployment/network boundary as the workbench

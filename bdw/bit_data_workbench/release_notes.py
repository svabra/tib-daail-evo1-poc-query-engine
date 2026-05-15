from __future__ import annotations


# Derived from git history through version 0.9.2. Keep entries concise and
# focused on user-visible improvements or severe reliability fixes.
RELEASE_NOTES: list[dict[str, object]] = [
    {
        "version": "0.9.2",
        "releasedAt": "2026-05-15T11:38:49+02:00",
        "features": [
            (
                "The Kubernetes deployment no longer depends on an application PVC, "
                "stopping PVC creation failures and related service-consumption warnings "
                "in production where S3 is the durable storage backend."
            ),
            (
                "Service Consumption now keeps a fixed 48-hour in-memory history, "
                "removes retention/window controls, and snapshots monitoring and budget "
                "state to hidden S3 at most once every five minutes."
            ),
            (
                "Production defaults reduce monitoring overhead by sampling app CPU/RAM "
                "once per minute, keeping S3 metrics hourly, and leaving Kubernetes node "
                "and PVC capacity collectors disabled."
            ),
        ],
    },
    {
        "version": "0.9.1",
        "releasedAt": "2026-05-15T10:29:41+02:00",
        "features": [
            (
                "Service Consumption can now disable Kubernetes node and PVC capacity "
                "collectors with environment flags, stopping repeated 403 log warnings "
                "while keeping local app CPU/RAM and mounted-volume usage readings."
            ),
            (
                "The production deployment disables those Kubernetes collectors by default "
                "until RBAC is repaired, with regression coverage proving disabled collectors "
                "do not contact the Kubernetes API."
            ),
        ],
    },
    {
        "version": "0.9.0",
        "releasedAt": "2026-05-14T21:59:47+02:00",
        "features": [
            (
                "SQL notebook cells now include a DuckDB-only Explain action beside Run Cell, "
                "enabled through the same source-validation checks and hidden for Python or native "
                "PostgreSQL cells."
            ),
            (
                "DuckDB EXPLAIN runs without executing the query, rewrites notebook aliases the same "
                "way as Run Cell, and returns logical, optimized, physical, and raw JSON plans."
            ),
            (
                "The Explain modal now provides an analyst briefing with operator counts, touched "
                "sources, estimated row signals, warnings, hints, optimizer notes, and tabbed plan "
                "inspection."
            ),
            (
                "Regression coverage now verifies the query explain API, validation failures, native "
                "PostgreSQL rejection, local alias rewriting, summary extraction, and the Explain modal "
                "smoke flow."
            ),
        ],
    },
    {
        "version": "0.8.16",
        "releasedAt": "2026-05-14T17:11:07+02:00",
        "features": [
            (
                "Shared S3 and DataExchange CSV files now support prepared ZIP downloads for large "
                "files, using background jobs, high-compression ZIP64 artifacts, hidden S3 storage, "
                "and resumable Range-capable download links."
            ),
            (
                "Prepared download jobs hydrate on page load and update through the shared SSE "
                "channel, so ready, failed, cancelled, and expired states appear in the Message "
                "Centre even after navigation or refresh."
            ),
            (
                "The prepared download modal now shows live progress, cancellation, ready download "
                "actions, and clear guidance that users may navigate away while the ZIP is prepared."
            ),
            (
                "DataExchange now shows a virtual Zip downloads folder that points to existing "
                "prepared artifacts, while S3 and DataExchange objects expose clickable ready badges "
                "and Download prepared ZIP file actions once the artifact is available."
            ),
            (
                "Regression coverage now verifies prepared S3 and DataExchange jobs, password "
                "enforcement, hidden-prefix rejection, ZIP contents, startup recovery, Range "
                "responses, modal behavior, ready badge downloads, and virtual ZIP downloads."
            ),
        ],
    },
    {
        "version": "0.8.15",
        "releasedAt": "2026-05-14T15:19:57+02:00",
        "features": [
            (
                "Per-cell Query Runs now load their existing history on initial notebook page load "
                "and recover when the panel is opened before startup initialization has completed."
            ),
            (
                "Query Runs now default to an analyst-friendly table with start date, end date, "
                "duration, CPU average, CPU peak, RAM average, RAM peak, rows, status, and SQL access."
            ),
            (
                "The manual Refresh action was removed from Query Runs because the list refreshes "
                "through the shared SSE event-driven channel."
            ),
            (
                "Resource charts are hidden by default and can be shown with a compact switch-style "
                "toggle that matches the notebook visibility control and sits clear of the table border."
            ),
            (
                "SQL details now expand from a chevron in each run row into a dedicated sub-row, "
                "making longer SQL easier to read without adding a separate global SQL toggle."
            ),
            (
                "Regression coverage now checks first-load query-run history, absence of the Refresh "
                "button, the chart switch state, SQL sub-row expansion, and table-first rendering."
            ),
        ],
    },
    {
        "version": "0.8.14",
        "releasedAt": "2026-05-14T13:57:07+02:00",
        "features": [
            (
                "Immutable PoC notebooks and notebook folders now stay Public / Shared, matching "
                "their shared catalogue behavior instead of exposing Private / Local controls."
            ),
            (
                "Recorded Query Runs now live inside each notebook cell between the SQL editor and "
                "results, start collapsed with a stateful chevron, refresh from the shared SSE event "
                "stream, and show a quiet empty state instead of an endless loading message."
            ),
            (
                "The workbench shell now uses DAAIF Fabric branding, shows realtime SSE connection "
                "status in the top bar, and exposes Query Runs from navigation."
            ),
            (
                "Query run history, notebook results, and the Query Monitor now share one resource "
                "chart renderer with larger readable line charts, CPU percent and RAM MB axes, peak "
                "and AVG legends, and light horizontal guide lines."
            ),
            (
                "Loader job details now rely on the Write targets panel for materialized outputs "
                "and no longer repeat the legacy Relation and Path summary above it."
            ),
            (
                "Regression coverage now verifies SSE-driven query run history, per-cell collapsed "
                "spacing, chart layout and labels across all query surfaces, immutable public "
                "notebook visibility, and loader write-target cleanup."
            ),
        ],
    },
    {
        "version": "0.8.13",
        "releasedAt": "2026-05-13T22:19:03+02:00",
        "features": [
            (
                "Query in new notebook now generates readable logical aliases for Local Workspace "
                "and Shared Workspace objects, such as local.folder.file.csv and s3.bucket.file.csv, "
                "while keeping internal DuckDB relation names hidden."
            ),
            (
                "Readable aliases are validated before execution, rewritten to physical relations "
                "only inside the execution path, and remain backward-compatible with existing "
                "workspace.local.saved_results notebooks."
            ),
            (
                "Run Cell now checks source existence at click time, blocks missing Local Workspace "
                "or S3 aliases with clear UI copy, and shows Query completed briefly once a query "
                "finishes instead of staying on Starting query."
            ),
            (
                "Regression coverage now verifies readable Local Workspace and S3 handoffs, source "
                "validation blocking, legacy Local Workspace SQL compatibility, and the completion "
                "state in the notebook validation badge."
            ),
        ],
    },
    {
        "version": "0.8.12",
        "releasedAt": "2026-05-13T18:00:03+02:00",
        "features": [
            (
                "Query execution now runs through a process-oriented manager so simultaneous "
                "queries can run independently, expose worker PID, CPU, RAM, peak RAM, and DuckDB "
                "progress in the Query Monitor, and be hard-stopped when cancellation is requested."
            ),
            (
                "Query cancellation now reports clear progress through the Query Monitor, message "
                "center, and notebook result panel, including the final successful cancellation state."
            ),
            (
                "Loader jobs now report when they are waiting for active DuckDB queries, can be "
                "cancelled cleanly while waiting, and keep the Loader Workbench from getting stuck in "
                "Preparing after query contention."
            ),
            (
                "Regression coverage now verifies simultaneous process-backed queries, loader "
                "cancellation behavior, Query in new notebook handoff, Local Workspace handoff, "
                "DataExchange uploads, S3 sidebar operations, and the full Playwright smoke suite."
            ),
        ],
    },
    {
        "version": "0.8.11",
        "releasedAt": "2026-05-13T11:12:06+02:00",
        "features": [
            (
                "Restores the stable 0.8.9 query execution path so Query in new notebook "
                "and regular source queries can run against the shared DuckDB workspace again."
            ),
            (
                "Rolls back the 0.8.10 query isolation, Explain / Analyze, and S3 download-job "
                "changes that caused DuckDB connection configuration failures."
            ),
        ],
    },
    {
        "version": "0.8.9",
        "releasedAt": "2026-05-12T14:33:30+02:00",
        "features": [
            (
                "Shared notebook folders now store their Public or Private visibility in the hidden "
                "S3 shared-notebooks bucket, keeping folder visibility synchronized across users "
                "without exposing the storage bucket in Shared Workspace explorers."
            ),
            (
                "New notebooks inherit the visibility of their parent folder, default root creates "
                "land in Shared Notebooks, and notebook/folder badges now clearly show Public / "
                "Shared or Private / Local with tooltip context."
            ),
        ],
    },
    {
        "version": "0.8.5",
        "releasedAt": "2026-05-11T10:42:30+02:00",
        "features": [
            (
                "Performance and SQL-functionality loaders now write S3 targets into buckets named from "
                "their Loader Workbench notebook structure instead of inheriting the local smoke-test "
                "bucket prefix."
            ),
            (
                "Only the S3 smoke loader keeps the configured smoke-test bucket lineage, with regression "
                "coverage for the generated bucket names across smoke, performance, SQL functionality, "
                "and MWA loader runbooks."
            ),
        ],
    },
    {
        "version": "0.8.4",
        "releasedAt": "2026-05-11T10:12:19+02:00",
        "features": [
            (
                "The Loader Workbench now includes the MWA Abrechnung Multi-Format Loader (3.2), "
                "which writes paired Abrechnung and Abrechnungs-Ziffern data to PostgreSQL OLTP, "
                "PostgreSQL Native access, and S3 Parquet, CSV, and JSONL comparison targets."
            ),
            (
                "The MWA S3 Parquet output is sorted for parent-child locality and written with "
                "ZSTD compression plus fixed row groups so DuckDB benchmark notebooks can scan the "
                "columnar target separately from CSV and JSONL."
            ),
        ],
    },
    {
        "version": "0.8.3",
        "releasedAt": "2026-05-07T15:10:49+02:00",
        "features": [
            (
                "Shared Workspace S3 downloads now stream CSV objects directly from S3 instead of staging "
                "the full file locally first, making GiB-scale CSV downloads reliable through the browser."
            ),
            (
                "CSV-to-Parquet ingestion now handles late CSV type drift by using full-file DuckDB inference "
                "and a VARCHAR retry path, with browser smokes covering S3 downloads and Parquet conversion."
            ),
        ],
    },
    {
        "version": "0.8.1",
        "releasedAt": "2026-05-07T14:49:22+02:00",
        "features": [
            (
                "The Ingestion Workbench tiles now render the intended Parquet, JSON, Excel, and XML "
                "taglines instead of exposing the Python dictionary copy method in the browser."
            ),
            (
                "The ingestion Playwright smoke now checks every non-CSV tile tagline so this template "
                "lookup regression is caught before release."
            ),
        ],
    },
    {
        "version": "0.7.14",
        "releasedAt": "2026-05-07T14:33:25+02:00",
        "features": [
            (
                "The Ingestion Workbench now offers dedicated Parquet, JSON/JSONL/NDJSON, Excel XLSX, "
                "and simple XML ingestors alongside CSV, with direct-file and ZIP upload support for each format."
            ),
            (
                "Shared ZIP extraction, upload sessions, and destination writers now keep each ingestor isolated "
                "while preserving one S3 object or PostgreSQL table per uploaded file or archive member."
            ),
            (
                "The ingestion landing page now includes search, clearer format guidance, and browser regression "
                "smokes covering direct and ZIP imports to Shared Workspace S3 and PostgreSQL for every file ingestor."
            ),
        ],
    },
    {
        "version": "0.7.13",
        "releasedAt": "2026-05-07T12:01:05+02:00",
        "features": [
            (
                "CSV ingestion now accepts direct CSV uploads, ZIP upload files, and extracted ZIP CSV data "
                "up to 30 GiB, with matching backend, Kubernetes, and browser-side limits."
            ),
            (
                "ZIP CSV ingestion is now covered end to end for Shared Workspace S3 and PostgreSQL, "
                "including Playwright verification that the browser upload succeeds and each CSV is imported."
            ),
        ],
    },
    {
        "version": "0.7.12",
        "releasedAt": "2026-05-05T21:30:52+02:00",
        "features": [
            (
                "The Data Source Workbench now has its own canonical /data-sources URL, "
                "with the previous query-workbench data-source routes kept compatible."
            ),
            (
                "Browse Data now opens the existing left-side Data Sources tree instead of a separate "
                "object browser, so browsing uses the same controls as the Query Workbench."
            ),
        ],
    },
    {
        "version": "0.7.7",
        "releasedAt": "2026-05-05T16:42:17+02:00",
        "features": [
            (
                "CSV ingestion progress now clearly separates upload transfer from the server-side "
                "processing step, including format conversion such as CSV to Parquet."
            ),
            (
                "The ingestion Playwright smoke now verifies the upload progress handoff from step 1 "
                "to step 2 before the server completes the import."
            ),
        ],
    },
    {
        "version": "0.7.6",
        "releasedAt": "2026-05-05T16:25:48+02:00",
        "features": [
            (
                "Shared Workspace bucket creation now rejects names with underscores before contacting S3, "
                "matching the backend S3 bucket-name policy."
            ),
            (
                "Creating a bucket now uses a single name prompt instead of asking for a second confirmation, "
                "with Playwright coverage for invalid-name rejection and the one-prompt create flow."
            ),
        ],
    },
    {
        "version": "0.7.5",
        "releasedAt": "2026-05-05T13:13:07+02:00",
        "features": [
            (
                "Deleting the Unassigned notebook folder now works: a standard delete preserves notebooks "
                "at the notebook tree root, while recursive delete still removes notebooks in that folder."
            ),
            (
                "Notebook-folder regression coverage now includes focused unit checks and a Playwright smoke "
                "for moving notebooks into Unassigned, deleting Unassigned, and recursive cleanup."
            ),
        ],
    },
    {
        "version": "0.7.3",
        "releasedAt": "2026-05-05T11:26:00+02:00",
        "features": [
            (
                "Source object action menus now include Download DDL for PostgreSQL, Shared Workspace S3, "
                "and Local Workspace sources, returning catalog-backed DDL for PostgreSQL and suggested "
                "CREATE TABLE statements for file-backed sources."
            ),
            (
                "CSV ingestion schema handling is now covered with explicit tests for PostgreSQL type inference "
                "and Parquet typed-schema preservation, plus a Playwright regression for DDL download content."
            ),
        ],
    },
    {
        "version": "0.7.2",
        "releasedAt": "2026-05-05T10:13:00+02:00",
        "features": [
            (
                "Shared Workspace S3 bucket delete actions now resolve the real bucket name from object metadata "
                "when the sidebar schema label contains a DuckDB-safe derived name, avoiding false bucket-name "
                "validation errors."
            ),
            (
                "Playwright regression coverage now verifies the bucket-delete fallback and hardens navigation "
                "smokes against same-document route changes and startup timing races."
            ),
        ],
    },
    {
        "version": "0.7.1",
        "releasedAt": "2026-04-22T16:10:00+02:00",
        "features": [
            (
                "Python notebook execution no longer races the shared DuckDB workspace file for PostgreSQL-only cells, "
                "so headless Jupyter kernels can run reliably in k8s and other multi-process deployments."
            ),
            (
                "The Python kernel now detects DuckDB lock conflicts more explicitly and returns a clear runtime error "
                "for Shared Workspace or Local Workspace cells instead of surfacing the raw low-level IOException."
            ),
            (
                "Regression coverage now includes the k8s-style DuckDB lock message and the PostgreSQL-only in-memory "
                "kernel path, reducing the chance of this concurrency bug returning in future releases."
            ),
        ],
    },
    {
        "version": "0.7.0",
        "releasedAt": "2026-04-22T14:15:00+02:00",
        "features": [
            (
                "Notebook cells can now run as either SQL or Python, with an explicit per-cell runtime toggle, "
                "persistent per-user notebook kernel state, and the existing SQL path preserved."
            ),
            (
                "Python notebook execution now runs through a headless Jupyter kernel with pandas helpers such as "
                "sql(...) and source(...).df(), plus rich outputs for tables, HTML, JSON, exceptions, and matplotlib charts."
            ),
            (
                "PoC Tests now include immutable demo notebooks under General Functionalities that show pandas "
                "wrangling and chart rendering against the static PostgreSQL VAT smoke reference data."
            ),
        ],
    },
    {
        "version": "0.6.1",
        "releasedAt": "2026-04-21T13:46:00+02:00",
        "features": [
            (
                "The Data Source Workbench now opens directly to the selected source from the home page "
                "and adds dedicated browse flows for PostgreSQL, Shared Workspace, and Local Workspace "
                "sources, with quick switching between source details and explorer mode."
            ),
            (
                "Local Workspace files can now be copied or moved into Shared Workspace S3 from the object menu, "
                "so users can promote browser-local data without losing the original unless they choose to move it."
            ),
            (
                "Relations, buckets, and files that are already published as Data Products are now clearly flagged "
                "while browsing data sources and link directly to the corresponding Data Product page."
            ),
        ],
    },
    {
        "version": "0.6.0",
        "releasedAt": "2026-04-21T11:30:00+02:00",
        "features": [
            (
                "The new Data Products Workbench lets users publish PostgreSQL tables/views "
                "and Shared Workspace buckets or objects as stable read-only data products, "
                "then manage titles, descriptions, ownership, and access metadata in one place."
            ),
            (
                "Published data products now have dedicated catalog and product pages under "
                "/dataproducts/, including request parameters, response schema, an OpenAPI excerpt, "
                "sample responses, and direct links to the live endpoint."
            ),
            (
                "Creating and browsing data products is now faster through sidebar publication entry points, "
                "data-source-type filtering in the publish flow, and a denser tile-based workbench layout."
            ),
        ],
    },
    {
        "version": "0.5.8",
        "releasedAt": "2026-04-20T12:00:00+02:00",
        "features": [
            (
                "Service Consumption keeps its recent layout cleanup, slimmer diagnostics, "
                "and true 1px chart lines."
            ),
        ],
    },
    {
        "version": "0.5.7",
        "releasedAt": "2026-04-17T16:45:00+02:00",
        "features": [
            (
                "Service Consumption now presents a finance-first annual budget dashboard "
                "with service mix, expandable cost drivers, and current-vs-forecast burn tracking."
            ),
            (
                "The monitoring page now blends fixed DAAIF and PostgreSQL fees with "
                "usage-based container, S3, and filesystem costs in one client-facing view."
            ),
            (
                "Operational diagnostics now stay secondary and denser, with smaller "
                "resource charts, preserved service-row state during refreshes, and clearer chart labeling."
            ),
        ],
    },
    {
        "version": "0.5.6",
        "releasedAt": "2026-04-17T11:30:00+02:00",
        "features": [
            (
                "Service Consumption now centers on annual CHF budget tracking, "
                "with spend YTD, remaining budget, forecast, and a client-facing service-cost breakdown."
            ),
            (
                "The monitoring page now preserves shared annual budgets on app storage "
                "and combines fixed service fees with usage-driven S3, filesystem, and container estimates."
            ),
            (
                "Financial charts now compare the current year against 2025 mock data, "
                "include a service-mix view, and keep service breakdown state stable during live refreshes."
            ),
        ],
    },
    {
        "version": "0.5.5",
        "releasedAt": "2026-04-16T17:00:00+02:00",
        "features": [
            (
                "The new Service Consumption workbench now adds a dedicated page "
                "for CPU, RAM, S3, and persistent-volume monitoring with recent-history charts."
            ),
            (
                "Service-consumption data is now persisted on mounted storage so "
                "the monitoring page can survive restarts and keep recent history."
            ),
            (
                "The monitoring UI now includes clearer chart legends, dynamic "
                "resource limits on the summary cards, and a balanced two-column chart layout."
            ),
        ],
    },
    {
        "version": "0.5.4",
        "releasedAt": "2026-04-16T16:00:00+02:00",
        "features": [
            (
                "Opening a notebook now resets the page to the top of the notebook, "
                "so switching from long workspaces no longer leaves the new notebook out of view."
            ),
            (
                "The notebook tree now opens only the branch that contains the active notebook, "
                "instead of expanding unrelated branches."
            ),
            (
                "Home-page workbench navigation is reliable again, "
                "and every workbench tile stays clickable."
            ),
        ],
    },
    {
        "version": "0.5.3",
        "releasedAt": "2026-04-16T15:00:00+02:00",
        "features": [
            (
                "Result export is now destination-first, with dedicated save or "
                "download dialogs and support for CSV, JSON Array, JSONL, "
                "Parquet, XML, and Excel export formats."
            ),
            (
                "Export formats now expose format-specific settings where they "
                "matter, including CSV delimiter and header controls plus XML and "
                "Excel output options."
            ),
            (
                "Local Workspace (IndexDB) and Shared Workspace (S3) data sources "
                "now behave more consistently in the sidebar, and XML plus Excel "
                "files can now be queried through DuckDB-backed conversion."
            ),
        ],
    },
    {
        "version": "0.5.2",
        "releasedAt": "2026-04-16T10:30:00+02:00",
        "features": [
            (
                "The new Ingestion Workbench now opens on a dedicated tile-based "
                "landing page, while loader-specific flows move into a separate "
                "Loader Workbench."
            ),
            (
                "CSV ingestion now guides users step by step into Local Workspace, "
                "Shared Workspace S3, PostgreSQL OLTP, or PostgreSQL OLAP with "
                "preview, delimiter and header controls, and destination-specific configuration."
            ),
            (
                "Imported CSV files can now be handed off directly into the Query "
                "Workbench, and Shared Workspace S3 uploads support explicit object "
                "names plus CSV, Parquet, and JSON storage options with clear tradeoff guidance."
            ),
        ],
    },
    {
        "version": "0.5.1",
        "releasedAt": "2026-04-15T12:00:00+02:00",
        "features": [
            (
                "Landing page and workbench navigation now remain clickable "
                "after overlay and shell initialization changes."
            ),
            (
                "Runtime status overlay now shows a PoC attribution line "
                "under the Workbench version for clearer provenance."
            ),
            (
                "Runtime overlay styling has been tuned to reduce visual "
                "obstruction while keeping version and connection readouts "
                "visible."
            ),
        ],
    },
    {
        "version": "0.4.7",
        "releasedAt": "2026-04-14T10:09:33.5282495+02:00",
        "features": [
            (
                "Query cells now persist a per-cell S3 query mode toggle, "
                "so notebooks can explicitly switch between direct S3 reads "
                "and supercharged local-cache reads across reloads and shared copies."
            ),
            (
                "Supercharged notebook execution now rewrites both quoted "
                "and unquoted relation references correctly, and the sidebar "
                "no longer reloads from no-op realtime source churn."
            ),
            (
                "Ingestion runbooks now show linked notebooks and concrete "
                "write targets, while running loaders preserve open target "
                "sections and Generate size input focus."
            ),
            (
                "The fixed runtime overlay now shows the current workbench "
                "version together with the live count of SSE-connected clients."
            ),
        ],
    },
    {
        "version": "0.4.5",
        "releasedAt": "2026-04-13T20:27:59.5688530+02:00",
        "features": [
            (
                "Shared Workspace S3 cleanup now handles null version ids more reliably, "
                "so recursive bucket deletion fails less often on stricter object stores."
            ),
        ],
    },
    {
        "version": "0.4.4",
        "releasedAt": "2026-04-13T20:12:33.2787759+02:00",
        "features": [
            (
                "Shared Workspace S3 explorer deletes now retry more reliably "
                "when object stores reject versioned deletes for null-version objects."
            ),
        ],
    },
    {
        "version": "0.4.3",
        "releasedAt": "2026-04-09T22:03:37.7329770+02:00",
        "features": [
            (
                "Local Workspace saved results now render as a folder tree "
                "in the sidebar, with persistent browser-local folders kept "
                "across reloads in IndexedDB-backed UI state."
            ),
            (
                "Users can now create and delete Local Workspace folders "
                "directly from the sidebar, including nested folders and "
                "branch cleanup for browser-local saved files."
            ),
            (
                "Saved Local Workspace files can now be moved or renamed "
                "from the sidebar."
            ),
        ],
    },
    {
        "version": "0.4.2",
        "releasedAt": "2026-04-09T13:04:44+02:00",
        "features": [
            (
                "The browser now uses one multiplexed realtime SSE stream "
                "instead of opening separate query, ingestion, source, and "
                "notebook event streams per page."
            ),
            (
                "Workbench pages no longer stall during local F5 runs when "
                "background discovery publishes realtime updates while the "
                "page shell is building its template context."
            ),
            (
                "The main frontend bundle now gets a cache-busting URL on "
                "page loads, so a normal refresh picks up the current "
                "realtime client after backend changes."
            ),
        ],
    },
    {
        "version": "0.4.1",
        "releasedAt": "2026-04-07T01:14:29+02:00",
        "features": [
            (
                "Opening the Loader Workbench now immediately reopens "
                "the loader navigation, uncollapses the sidebar, and "
                "expands the selected runbook path when a generator is in "
                "focus."
            ),
        ],
    },
    {
        "version": "0.4.0",
        "releasedAt": "2026-04-07T00:38:59+02:00",
        "features": [
            (
                "Query Workbench topbar navigation now reopens the notebook "
                "sidebar immediately, and the entry page now prioritizes the "
                "Notebook Browser over the shared-notebook overview."
            ),
            (
                "Cell, result, source, and S3 action popups no longer "
                "collapse while the pointer moves from the trigger into the "
                "menu panel."
            ),
            (
                "Shared Workspace S3 bucket deletion now retries transient "
                "bucket-not-empty states and keeps cleaning up visible bucket "
                "contents even when version listing is denied by the object "
                "store credentials."
            ),
            (
                "S3 delete failures now explain when hidden object versions "
                "or delete markers still block bucket removal, with focused "
                "regression coverage for RHOS and ECS-style cleanup paths."
            ),
        ],
    },
    {
        "version": "0.3.36",
        "releasedAt": "2026-04-06T23:40:12+02:00",
        "features": [
            (
                "Shared Workspace S3 bucket cleanup is more reliable on ECS-backed object stores, "
                "so contest and loader jobs can recreate their buckets reliably."
            ),
        ],
    },
    {
        "version": "0.3.35",
        "releasedAt": "2026-04-06T23:19:27+02:00",
        "features": [
            (
                "Sidebar source status icons now stay vertically aligned on "
                "the far right even when only some data sources expose "
                "persistent action buttons."
            ),
        ],
    },
    {
        "version": "0.3.34",
        "releasedAt": "2026-04-06T21:57:11+02:00",
        "features": [
            (
                "The workbench now has a dedicated Data Source Workbench, "
                "including topbar navigation, landing-page entry points, "
                "and section-aware titles for home, query, ingestion, and "
                "data-source views."
            ),
            (
                "Shared Workspace and Local Workspace terminology now runs "
                "consistently across source management, the sidebar, source "
                "pickers, tooltips, and settings."
            ),
            (
                "Local Workspace is now always visible at the top of the "
                "data-source list and can store JSON and Parquet query "
                "result exports directly in browser-local IndexedDB."
            ),
            (
                "Saving to Local Workspace now opens a modal where users "
                "can choose a file name and folder path, create folders, "
                "and then see saved files back in the sidebar with "
                "download, delete, and location details."
            ),
        ],
    },
    {
        "version": "0.3.33",
        "releasedAt": "2026-04-06T18:07:12+02:00",
        "features": [
            (
                "Workspace S3 buckets can now be created and deleted directly "
                "from the sidebar, including recursive cleanup of versioned "
                "buckets and object delete markers."
            ),
            (
                "The S3 explorer delete flow now exposes clearer success and "
                "failure feedback."
            ),
            (
                "Settings and notification popups no longer collapse while "
                "moving the pointer from the topbar button into the popup "
                "panel."
            ),
        ],
    },
    {
        "version": "0.3.32",
        "releasedAt": "2026-04-06T12:20:00+02:00",
        "features": [
            (
                "The navigation sidebar can now be resized at runtime, making "
                "it easier to inspect long notebook, table, and S3 object "
                "names without changing the default layout."
            ),
            (
                "Concrete S3-backed source objects now expose a direct "
                "download action in the sidebar, so saved result files and "
                "other workspace objects can be retrieved without opening a "
                "notebook first."
            ),
        ],
    },
    {
        "version": "0.3.31",
        "releasedAt": "2026-04-05T16:20:00+02:00",
        "features": [
            (
                "Performance Evaluation benchmarks are now split into "
                "Single-Table Test and Multi-Table Test folders, with a new "
                "multi-table federal-tax benchmark spanning DuckDB on S3, "
                "DuckDB on PostgreSQL, and PostgreSQL native execution."
            ),
            (
                "Benchmark notebooks now explain the business semantics of "
                "the query, so users can see what each single-table and "
                "multi-table test is approximating."
            ),
            (
                "Query results now use a single Export / Save menu for JSON, "
                "CSV, and Parquet downloads, and can be saved directly to S3 "
                "through a reusable explorer with bucket and folder creation."
            ),
            (
                "Result export handling was hardened so downloads and S3 "
                "saves work reliably for completed query jobs, including "
                "DuckDB plus PostgreSQL benchmark runs."
            ),
        ],
    },
    {
        "version": "0.3.30",
        "releasedAt": "2026-04-02T22:25:28+02:00",
        "features": [
            (
                "Running queries now show clearer progress feedback, "
                "including a percentage when the backend can provide one."
            ),
            (
                "Queued or indeterminate queries now explain what the "
                "backend is doing instead of only showing a spinner."
            ),
        ],
    },
    {
        "version": "0.3.29",
        "releasedAt": "2026-04-02T22:01:16+02:00",
        "features": [
            (
                "S3-backed workflows are now more stable and predictable."
            ),
        ],
    },
    {
        "version": "0.3.28",
        "releasedAt": "2026-04-02T17:52:27+02:00",
        "features": [
            (
                "S3 connections are now more compatible with stricter "
                "object-storage endpoints."
            ),
        ],
    },
    {
        "version": "0.3.27",
        "releasedAt": "2026-04-02T17:26:38+02:00",
        "features": [
            (
                "S3 loaders now upload more reliably when generated files "
                "are written to object storage."
            ),
            (
                "Loader write and cleanup flow is more reliable for "
                "S3-backed generated data."
            ),
        ],
    },
    {
        "version": "0.3.26",
        "releasedAt": "2026-04-02T17:02:20+02:00",
        "features": [
            (
                "Loader compatibility is restored for cluster S3 environments."
            ),
        ],
    },
    {
        "version": "0.3.25",
        "releasedAt": "2026-04-02T16:42:25+02:00",
        "features": [
            (
                "Sidebar controls and navigation interactions were polished "
                "for quicker notebook browsing."
            ),
        ],
    },
    {
        "version": "0.3.24",
        "releasedAt": "2026-04-02T16:31:26+02:00",
        "features": [
            (
                "Notebooks can be shared with all connected users and stay "
                "synchronized through server-side events."
            ),
            (
                "Shared notebooks are marked in the sidebar and can be "
                "switched back to local mode."
            ),
        ],
    },
    {
        "version": "0.3.22",
        "releasedAt": "2026-04-02T15:17:53+02:00",
        "features": [
            "Empty Shared Workspace S3 storage is seeded automatically when needed.",
            (
                "S3 Smoke and PG vs S3 Contest loaders now keep their data "
                "in separate buckets, so cleaning up one loader no longer wipes another."
            ),
        ],
    },
    {
        "version": "0.3.21",
        "releasedAt": "2026-04-02T14:35:10+02:00",
        "features": [
            (
                "S3 connectivity is more reliable in RHOS/OpenShift environments."
            ),
        ],
    },
]


def release_notes() -> list[dict[str, object]]:
    return [
        {
            "version": str(entry["version"]),
            "releasedAt": str(entry["releasedAt"]),
            "features": [str(feature) for feature in entry["features"]],
        }
        for entry in RELEASE_NOTES
    ]

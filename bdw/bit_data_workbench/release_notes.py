from __future__ import annotations


# Derived from git history through version 0.5.3. Keep entries concise and
# focused on user-visible improvements or severe reliability fixes.
RELEASE_NOTES: list[dict[str, object]] = [
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
        "version": "0.4.6",
        "releasedAt": "2026-04-13T20:53:03.2246641+02:00",
        "features": [
            (
                "S3 explorer delete failures now log the exact failing S3 "
                "operation, bucket, key or prefix, version id, and backend "
                "error details so RHOS permission denials are visible in pod logs."
            ),
            (
                "A focused S3 bootstrap lifecycle regression now covers "
                "create bucket, upload object, recursive object cleanup, and "
                "final bucket deletion in one storage-level test."
            ),
        ],
    },
    {
        "version": "0.4.5",
        "releasedAt": "2026-04-13T20:27:59.5688530+02:00",
        "features": [
            (
                "RHOS S3 cleanup now strips the null version id before bulk "
                "or single-object deletes are sent, preventing recursive "
                "bucket cleanup from retrying forbidden versioned deletes."
            ),
            (
                "API request payload models now use explicit validation and "
                "serialization aliases, removing the Pydantic field-alias "
                "warning emitted during S3 explorer delete requests."
            ),
        ],
    },
    {
        "version": "0.4.4",
        "releasedAt": "2026-04-13T20:12:33.2787759+02:00",
        "features": [
            (
                "RHOS S3 explorer deletes now handle object entries reported "
                "with the null version id more reliably, retrying a plain "
                "delete when the object store rejects the versioned request."
            ),
            (
                "Focused S3 storage regression coverage now protects the RHOS "
                "delete fallback path used during recursive bucket cleanup."
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
                "from the sidebar, and the new folder-management flows are "
                "covered by focused Playwright smoke scripts."
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
                "menu panel, and the query-cell ellipsis behavior is covered "
                "by a Playwright smoke test."
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
                "RHOS ECS-backed S3 bucket cleanup now sends the required "
                "Content-MD5 header for DeleteObjects requests, so contest "
                "and loader jobs can recreate their S3 buckets reliably."
            ),
            (
                "A focused S3 regression test now verifies the DeleteObjects "
                "Content-MD5 handling and client hook registration used by "
                "shared S3 storage operations."
            ),
        ],
    },
    {
        "version": "0.3.35",
        "releasedAt": "2026-04-06T23:19:27+02:00",
        "features": [
            (
                "Production container runs now default to console logging, "
                "so Kubernetes or OpenShift can collect stdout and stderr "
                "without requiring writable in-container log files."
            ),
            (
                "Local development can still opt into file logging through "
                "an explicit launcher setting, preserving the existing "
                "logs/bdw/server.log workflow for developers."
            ),
            (
                "A launcher test suite now verifies that file logging stays "
                "disabled by default and is only enabled when explicitly "
                "requested."
            ),
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
                "failure feedback, and the sidebar bucket create/delete path "
                "is covered by a Playwright smoke test."
            ),
            (
                "Settings and notification popups no longer collapse while "
                "moving the pointer from the topbar button into the popup "
                "panel."
            ),
            (
                "Local F5 startup now waits for MinIO and PostgreSQL "
                "readiness and normalizes localhost S3 and PostgreSQL "
                "endpoints more reliably for local runs."
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
                "Pinned DuckDB to 1.4.4 for a more stable and predictable "
                "runtime."
            ),
            (
                "Aligned S3 integration with the pinned DuckDB version to "
                "reduce storage compatibility issues."
            ),
        ],
    },
    {
        "version": "0.3.28",
        "releasedAt": "2026-04-02T17:52:27+02:00",
        "features": [
            (
                "Adjusted S3 checksum handling to avoid compatibility "
                "failures on stricter object-storage endpoints."
            ),
        ],
    },
    {
        "version": "0.3.27",
        "releasedAt": "2026-04-02T17:26:38+02:00",
        "features": [
            (
                "S3 loaders now upload through boto3, improving reliability "
                "when generated files are written to object storage."
            ),
            (
                "Loader write and cleanup flow was hardened for S3-backed "
                "test data."
            ),
        ],
    },
    {
        "version": "0.3.26",
        "releasedAt": "2026-04-02T17:02:20+02:00",
        "features": [
            (
                "Fixed cluster S3 URL-style handling to restore loader "
                "compatibility in RHOS/OpenShift environments."
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
        "version": "0.3.23",
        "releasedAt": "2026-04-02T15:21:56+02:00",
        "features": [
            (
                "Startup diagnostics now print environment, config mounts, "
                "and certificate mapping more clearly."
            ),
            (
                "Deployment issues around S3 certificates and mounted config "
                "are easier to troubleshoot."
            ),
        ],
    },
    {
        "version": "0.3.22",
        "releasedAt": "2026-04-02T15:17:53+02:00",
        "features": [
            "Empty S3 startup storage is seeded automatically when needed.",
            (
                "S3 Smoke and PG vs S3 Contest loaders now use separate "
                "buckets, so cleanup stays isolated."
            ),
        ],
    },
    {
        "version": "0.3.21",
        "releasedAt": "2026-04-02T14:35:10+02:00",
        "features": [
            (
                "Improved RHOS/OpenShift S3 connectivity with mounted "
                "trust-store support and secret-backed credentials."
            ),
            (
                "Safer HTTPS and transport defaults reduce cluster S3 setup "
                "issues."
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

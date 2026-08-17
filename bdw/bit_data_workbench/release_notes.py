from __future__ import annotations


# Plain-language capabilities for the current build. The feature-list response
# attaches these to the first release entry so the displayed version and the
# runtime release notes cannot drift apart.
CURRENT_FEATURE_LIST: dict[str, object] = {
    "title": "Was kann DAAIF Factory?",
    "introduction": (
        "DAAIF Factory bündelt Datenzugriff, Analyse, Pipelines und die "
        "Veröffentlichung von Datenprodukten in einer gemeinsamen Arbeitsumgebung."
    ),
    "pocNote": (
        "Hinweis: Diese Liste beschreibt den aktuellen PoC-Stand. Einzelne "
        "Abläufe sind simuliert und noch keine produktive Leistung."
    ),
    "features": [
        {
            "title": "Datenquellen finden und verstehen",
            "description": (
                "Durchsuchen Sie S3-, PostgreSQL- und lokale Quellen, prüfen Sie "
                "Schemas und übernehmen Sie die passende Referenz direkt in Ihre Analyse."
            ),
        },
        {
            "title": "SQL und Python gemeinsam analysieren",
            "description": (
                "Erstellen Sie versionierte Notebooks, führen Sie SQL- und Python-Zellen "
                "aus und vergleichen, speichern oder exportieren Sie Ergebnisse."
            ),
        },
        {
            "title": "Datenpipelines aufbauen und überwachen",
            "description": (
                "Verknüpfen Sie Notebook-Stufen über Abhängigkeiten, materialisieren Sie "
                "Zwischenergebnisse in S3 und verfolgen Sie Status, Laufzeit und Zeilen."
            ),
        },
        {
            "title": "Daten kontrolliert einlesen und bereitstellen",
            "description": (
                "Importieren Sie Dateien, verwalten Sie Ziele im Shared Workspace und "
                "publizieren Sie kuratierte Datenprodukte mit nachvollziehbaren Metadaten."
            ),
        },
        {
            "title": "Ressourcen und Qualität nachvollziehen",
            "description": (
                "Prüfen Sie Laufzeiten, CPU- und RAM-Verbrauch, Validierungshinweise und "
                "frühere Ausführungen direkt am Ergebnis."
            ),
        },
        {
            "title": "Mit DaCa zusammenarbeiten",
            "description": (
                "Übergeben Sie veröffentlichte Datenprodukte an die DaCa-Governance und "
                "behalten Sie Identität, Freigaben und Endpunkte konsistent im Blick."
            ),
        },
    ],
}


# Derived from git history through version 0.10.43. Keep entries concise and
# focused on user-visible improvements or severe reliability fixes.
RELEASE_NOTES: list[dict[str, object]] = [
    {
        "version": "0.10.43",
        "releasedAt": "2026-08-17T13:11:43+02:00",
        "features": [
            (
                "Export / Save now writes complete truncated virtual-S3 and "
                "materialized pipeline results by replaying the prepared result "
                "snapshot instead of display aliases or stage COPY statements."
            ),
            (
                "Stored-result exports preserve the exact S3 snapshot, including "
                "non-deterministic query values, and failed exports now remove "
                "partial temporary artifacts."
            ),
            (
                "The RHOS deployment reference now documents the DaCa Catalog API "
                "and OPA service endpoints plus the external Catalog UI and DAAIF "
                "route settings required by the Data Analyst's Journey."
            ),
            (
                "The Data Analyst's Journey loader now marks the manual Aargau "
                "artifact as Plain CSV and warns against Parquet or JSON conversion, "
                "keeping the ingestion handoff compatible with notebook cell 1."
            ),
        ],
    },
    {
        "version": "0.10.42",
        "releasedAt": "2026-08-17T00:22:22+02:00",
        "features": [
            (
                "DAAIF now delivers the DaCa-aligned federal header, shared "
                "PoC identities, rotating Swiss landing-page imagery, notebook "
                "search, and the governed end-to-end Data Analyst's Journey."
            ),
            (
                "The fixed runtime version overlay now opens a DaCa-aligned "
                "current-feature dialog with keyboard focus management, backdrop "
                "closing, and responsive scrolling."
            ),
            (
                "CSV ingestion now accepts a complete s3:// destination, "
                "splits it into bucket, prefix, and object name, previews and "
                "copies the final URI, and reports failed imports accurately."
            ),
            (
                "PoC Tests now separates Jupyter/Python and Data Pipelines, "
                "adds refined Kostenbelege and VAT analysis examples, and "
                "exposes curated Sample Notebooks on the landing page."
            ),
            (
                "Saved notebook results now reopen their canonical materialized "
                "S3 destination and format, preventing the Journey Parquet "
                "result from defaulting to an unrelated generated file name."
            ),
            (
                "Expert search now lists all selected content when empty, "
                "filters from the first character, and separates five Data "
                "Source connectors from their S3 and PostgreSQL data objects."
            ),
            (
                "Data-product publishing now supports confirmed in-place "
                "replacement while preserving product identity, endpoint, "
                "and DaCa governance linkage with optimistic conflict checks."
            ),
            (
                "Pipeline realtime rendering now coalesces graph refreshes, "
                "ignores unchanged snapshots, prevents duplicate result panels, "
                "and keeps accessible Run and Abort controls visually stable."
            ),
            (
                "Local development can disable static asset caching while "
                "versioned production caching remains intact; expanded tests "
                "cover discovery, export, ingestion, notebooks, and pipelines."
            ),
        ],
    },
    {
        "version": "0.10.40",
        "releasedAt": "2026-07-08T10:23:01+02:00",
        "features": [
            (
                "Pipeline Mode stages now expose one editable full S3 Parquet "
                "output path that drives stage materialization, copy actions, "
                "and downstream stage references."
            ),
            (
                "Materialized stages now run DuckDB COPY TO directly against "
                "the configured s3:// target and write stage metadata beside "
                "the Parquet file, so reruns overwrite the analyst-selected "
                "data-product path."
            ),
            (
                "Pipeline result-storage controls now stay semantically on "
                "without submitting a separate queryOptions.duckdb.resultStorage "
                "copy operation, while Exploration Mode result storage remains "
                "unchanged."
            ),
            (
                "PoC Tests now includes Kostenbelege fact-builder Exploration "
                "and Pipeline notebooks plus loader coverage that store and "
                "reuse intermediate S3 Parquet outputs."
            ),
            (
                "Regression coverage now verifies stage output-path planning, "
                "direct S3 COPY targets, UI persistence and copy controls, and "
                "a Playwright Pipeline smoke that edits a stage path and "
                "validates the final result."
            ),
        ],
    },
    {
        "version": "0.10.39",
        "releasedAt": "2026-07-07T08:30:57+02:00",
        "features": [
            (
                "Exploration notebook SQL cells can now store the complete "
                "DuckDB result set to an editable S3 Parquet path while the "
                "HTML preview remains capped for display."
            ),
            (
                "Stored result sets are written with DuckDB COPY TO and expose "
                "copyable virtual and DuckDB S3 references both before and "
                "after query execution."
            ),
            (
                "The DuckDB SQL view is now editable and keeps simple S3 "
                "read_parquet references synchronized with the virtual SQL "
                "editor in both directions."
            ),
            (
                "PoC Tests now includes a Store Result Set in S3 demo notebook "
                "and a matching loader so the stored Parquet result can be "
                "loaded by a later cell."
            ),
            (
                "Regression coverage now verifies backend result storage, "
                "the sample loader, notebook placement, copy controls, and "
                "virtual/DuckDB SQL editor synchronization."
            ),
        ],
    },
    {
        "version": "0.10.38",
        "releasedAt": "2026-06-19T12:13:39+02:00",
        "features": [
            (
                "Pure DuckDB result previews are now capped at 20 displayed "
                "rows so large result sets do not overload the page."
            ),
            (
                "The Pure DuckDB Download CSV action now exports the complete "
                "read-query result as a compressed CSV ZIP through a backend "
                "download endpoint instead of exporting only the visible rows."
            ),
            (
                "Pure DuckDB now includes Query 3b, an optimized FACT-builder "
                "variant of Query 3 that preserves the aggregate result shape "
                "while avoiding duplicated wide UNION ALL join branches."
            ),
            (
                "Query 3b includes collapsed Optimization Remarks documenting "
                "the q3_optimized_fact_v1 strategy and its consistency checks."
            ),
            (
                "The Pure DuckDB Q1/Q2 benchmark suite now supports Q3 "
                "optimization variants and validates count, sum, average, "
                "minimum, and maximum against the current Query 3 baseline."
            ),
            (
                "Regression coverage now verifies the 20-row preview cap, "
                "full-result ZIP export, Query 3b rendering, and Q3/Q3b "
                "aggregate consistency on local Parquet fixtures."
            ),
        ],
    },
    {
        "version": "0.10.30",
        "releasedAt": "2026-06-18T08:18:01+02:00",
        "features": [
            (
                "Pure DuckDB page jobs now bypass QueryJobManager and run "
                "through a dedicated direct in-process DuckDB job manager "
                "instead of the notebook one-query-one-process pipeline."
            ),
            (
                "Direct Pure DuckDB execution uses an in-memory DuckDB "
                "connection with the existing S3, extension, runtime, and "
                "spill configuration bootstrap while reporting zero shared "
                "DuckDB access wait."
            ),
            (
                "A Pure DuckDB big-data benchmark script now generates "
                "large Kostenbelege Parquet fixtures, uploads them to S3, "
                "and can run all Pure DuckDB cells through both API and UI "
                "with local S3 bucket-name compatibility."
            ),
            (
                "Regression coverage now proves Pure DuckDB does not call "
                "QueryJobManager, verifies the direct execution payload, "
                "and keeps local DuckDB spill artifacts out of release "
                "status."
            ),
        ],
    },
    {
        "version": "0.10.28",
        "releasedAt": "2026-06-17T17:33:41+02:00",
        "features": [
            (
                "Pure DuckDB now includes eight additional analytical cells, "
                "expanding the page from 9 to 17 executable DuckDB SQL cells."
            ),
            (
                "The appended Kostenbelege analytics cover high-cardinality "
                "grouping, time-series aggregation, derived expressions, "
                "conditional aggregation, top-N sorting, window functions, "
                "distinct counts, and monthly analytical rollups."
            ),
            (
                "ANSI/Teradata syntax from the supplied examples is translated "
                "to final DuckDB SQL, including fact_bupo Parquet reads, "
                "TOP 10 to LIMIT 10, and ADD_MONTHS/monthh month bucketing to "
                "DATE_TRUNC('month', ...) AS mmonth."
            ),
            (
                "Regression coverage now executes all 17 presets against tiny "
                "local Parquet fixtures and the Pure DuckDB Playwright smoke "
                "asserts the expanded cell count."
            ),
        ],
    },
    {
        "version": "0.10.27",
        "releasedAt": "2026-06-17T17:23:44+02:00",
        "features": [
            (
                "Pure DuckDB result tables now include a Download CSV action "
                "whenever a completed cell returns displayed rows."
            ),
            (
                "Pure DuckDB CSV export is generated client-side from the "
                "returned result rows with standard CSV quoting and stable "
                "cell-based filenames."
            ),
            (
                "The Pure DuckDB Kostenbelege calendar source now uses the "
                "valid physical S3 bucket path s3://3-1-imports/... instead "
                "of legacy or underscore-based virtual bucket names."
            ),
            (
                "Regression coverage now asserts CSV export wiring, rejects "
                "the legacy calendar S3 paths, and verifies the CSV download "
                "content in the Pure DuckDB Playwright smoke."
            ),
        ],
    },
    {
        "version": "0.10.26",
        "releasedAt": "2026-06-17T16:47:23+02:00",
        "features": [
            (
                "The main page now includes a lightweight pure duckdb tile "
                "that opens a standalone /pure-duckdb page without the "
                "notebook shell, sidebar, app bundle, or SSE transport."
            ),
            (
                "Pure DuckDB cells run final DuckDB SQL directly through a "
                "narrow jobs API, while still reusing the existing DuckDB "
                "worker runtime for S3 secrets, spill settings, isolated "
                "reads, and isolated artifact writes."
            ),
            (
                "The predefined Pure DuckDB cells contain final s3:// "
                "read_parquet and COPY SQL, including union_by_name input "
                "merges and S3 artifact outputs for the FACT_BUPO flow."
            ),
            (
                "Regression coverage now executes all nine preset queries "
                "against tiny local Parquet fixtures and adds a Playwright "
                "smoke for the standalone page, direct query run, frozen "
                "elapsed time, and desktop/mobile overflow checks."
            ),
        ],
    },
    {
        "version": "0.10.25",
        "releasedAt": "2026-06-17T16:12:09+02:00",
        "features": [
            (
                "Kostenbelege 3.1 now includes the optimized Parquet dataset "
                "notebook that materializes FACT_Buchungsbelegposition to an "
                "S3 dataset folder and queries that optimized output."
            ),
            (
                "DuckDB SQL translation now collapses SELECT * UNION ALL "
                "chains over S3 Parquet files into read_parquet([...], "
                "union_by_name = true), preserving mixed schemas without "
                "the notebook author needing DuckDB-specific syntax."
            ),
            (
                "COPY ... TO artifact writes now run as isolated in-memory "
                "DuckDB jobs, while shared catalog mutations remain on the "
                "serialized shared-write path."
            ),
            (
                "Regression coverage now includes optimized Kostenbelege "
                "notebook translation, mock Parquet generation, effective "
                "DuckDB spill quota handling, and source-tree loading that "
                "avoids unnecessary startup weight."
            ),
        ],
    },
    {
        "version": "0.10.24",
        "releasedAt": "2026-06-17T11:01:22+02:00",
        "features": [
            (
                "Timing breadcrumb steps now use fixed-width arrow blocks "
                "with the step label on the first line and the duration on "
                "a second line, so live timing values no longer resize the "
                "progress bar."
            ),
            (
                "Duration text keeps tabular digits and ellipsis handling "
                "inside each fixed step, preserving the single-line "
                "breadcrumb track while long values update."
            ),
            (
                "The Playwright layout regression now mutates breadcrumb "
                "duration values and asserts that the breadcrumb, scroll "
                "track, and every step width remain stable."
            ),
        ],
    },
    {
        "version": "0.10.23",
        "releasedAt": "2026-06-17T10:46:36+02:00",
        "features": [
            (
                "Result timing breadcrumbs now occupy one dedicated line "
                "below the elapsed clock and never share that line with "
                "comparison, cache, or footprint metric pills."
            ),
            (
                "Timing breadcrumb arrow steps no longer wrap onto multiple "
                "rows on compact screens; the breadcrumb keeps a single "
                "horizontal track with internal overflow when needed."
            ),
            (
                "Regression coverage now asserts the single-line breadcrumb "
                "contract, separated metric rows, and narrow-screen internal "
                "overflow behavior in both static UI checks and Playwright."
            ),
        ],
    },
    {
        "version": "0.10.22",
        "releasedAt": "2026-06-17T10:01:33+02:00",
        "features": [
            (
                "Result timing now keeps the elapsed clock on its own row "
                "and renders the timing breadcrumb below it, preventing "
                "small screens from pushing the live timing strip into a "
                "line-break-heavy layout."
            ),
            (
                "The timing breadcrumb now derives the active phase from "
                "backend progress messages instead of assuming the next "
                "unmeasured step, so long-running DuckDB execution is shown "
                "as Query rather than Delivery until result delivery has "
                "actually begun."
            ),
            (
                "Pipeline stage cell runs now wait while a stage is still "
                "planned, queued, running, or cancelling, removing the false "
                "'Stage finished with status Running' modal and preserving "
                "clear terminal errors for real failures."
            ),
            (
                "Query Monitoring history is constrained to its result "
                "panel with an internal horizontal scroll when needed, and "
                "Playwright regressions now cover the timing layout, active "
                "breadcrumb phase, and running-stage modal behavior."
            ),
        ],
    },
    {
        "version": "0.10.21",
        "releasedAt": "2026-06-17T07:20:10+02:00",
        "features": [
            (
                "Pipeline stage COPY TO SQL is now generated with the "
                "closing parenthesis and TO clause on fresh lines, so "
                "user SQL ending in a DuckDB -- line comment can no longer "
                "comment out the artifact write wrapper."
            ),
            (
                "The DuckDB SQL preview and the real materialized-stage "
                "runner now share the same COPY TO parquet builder, keeping "
                "the displayed SQL and submitted SQL aligned for pipeline "
                "stages."
            ),
            (
                "Regression coverage now includes the commented UNION case, "
                "isolated-write validation for COPY wrappers with trailing "
                "line comments, real materialized pipeline execution, and a "
                "Playwright smoke for the preview endpoint."
            ),
        ],
    },
    {
        "version": "0.10.20",
        "releasedAt": "2026-06-16T16:25:08+02:00",
        "features": [
            (
                "The DuckDB SQL preview now includes the actual pipeline "
                "stage COPY ... TO parquet wrapper for materialized stages, "
                "so Run Stage users see the same write-shaped SQL that the "
                "backend submits to DuckDB instead of only the inner SELECT."
            ),
            (
                "Pipeline stage previews keep canonical DuckDB S3 URIs such "
                "as s3://bucket/key and clearly expose the runtime-local "
                "Parquet COPY target pattern, avoiding misleading HTTP "
                "endpoint rewrites that can produce false 404 paths."
            ),
            (
                "Regression coverage now exercises materialized pipeline "
                "COPY TO execution end to end, including rewritten S3 "
                "aliases, sanitized output filenames, parquet artifact "
                "readback, isolated-write routing, and preview/API/UI wiring."
            ),
        ],
    },
    {
        "version": "0.10.19",
        "releasedAt": "2026-06-16T14:57:43+02:00",
        "features": [
            (
                "S3 source resolution now preserves the actual bucket and "
                "object-key casing for catalog-backed Parquet files, so "
                "lowercase SQL aliases such as s3.kbpoimports.\"kbpo2020.parquet\" "
                "rewrite to the discovered physical S3 object instead of a "
                "case-mismatched URL."
            ),
            (
                "Read and stage materialization source checks now distinguish "
                "verified source metadata from syntactic direct-S3 fallbacks, "
                "so unresolved single-file aliases fail during preparation "
                "instead of surfacing a late DuckDB 404."
            ),
            (
                "Result timing now uses compact left-to-right breadcrumb "
                "arrow blocks with completed and active steps highlighted in "
                "blue, a fixed-width elapsed clock, and live active-step "
                "timing updates that do not shift the row."
            ),
            (
                "Stage and pipeline failure dialogs now render multiline "
                "DuckDB errors in a bounded preformatted block, preserving "
                "SQL line and caret context without breaking the popup layout."
            ),
        ],
    },
    {
        "version": "0.10.18",
        "releasedAt": "2026-06-16T13:58:40+02:00",
        "features": [
            (
                "Pipeline stage materialization now runs Parquet artifact "
                "exports as isolated in-memory DuckDB writes, so COPY ... "
                "TO stage output files no longer waits for or opens the "
                "shared workspace.duckdb catalog."
            ),
            (
                "True shared DuckDB catalog mutations still use the "
                "serialized shared-file-write path, while stage exports keep "
                "the existing isolated S3, PostgreSQL, stage, and local "
                "workspace source bootstrapping."
            ),
            (
                "Regression coverage now reproduces the production lock "
                "scenario with a held shared DuckDB file, verifies "
                "engineAccessWaitMs stays at zero for isolated stage writes, "
                "and confirms normal shared catalog writes remain serialized."
            ),
        ],
    },
    {
        "version": "0.10.17",
        "releasedAt": "2026-06-16T13:17:26+02:00",
        "features": [
            (
                "DuckDB read queries no longer enter the shared-file-read "
                "path; they run in isolated in-memory workers or fail during "
                "preparation when a relation cannot be resolved as an "
                "isolated S3, PostgreSQL, stage, or local workspace source."
            ),
            (
                "The former File lock timing is now shown as Shared DuckDB "
                "wait, making clear that this wait belongs to exclusive "
                "shared DuckDB write/materialization access and should not "
                "appear for read-only analytical queries."
            ),
            (
                "Regression coverage now verifies that read jobs skip shared "
                "DuckDB waits even while a writer holds the coordinator, and "
                "that unisolated read relations fail fast instead of waiting."
            ),
        ],
    },
    {
        "version": "0.10.16",
        "releasedAt": "2026-06-16T12:37:20+02:00",
        "features": [
            (
                "DuckDB spill now uses a dedicated OpenShift emptyDir "
                "volume at /workspace/tmp/duckdb-spill, keeping query "
                "cache, uploads, and workspace files from consuming the "
                "configured DuckDB temp quota during large joins and "
                "aggregations."
            ),
            (
                "Regression coverage now verifies effective DuckDB runtime "
                "settings, the configured spill quota, and that the "
                "Kubernetes spill volume is larger than the DuckDB "
                "max_temp_directory_size."
            ),
            (
                "A PlantUML and SVG DAAIF architecture sketch documents the "
                "Workbench UI, FastAPI/SSE layer, query execution, ingestion, "
                "DuckDB, S3, PostgreSQL, and runtime configuration."
            ),
        ],
    },
    {
        "version": "0.10.15",
        "releasedAt": "2026-06-16T11:01:07+02:00",
        "features": [
            (
                "S3 source references now consistently use the canonical "
                "s3.<bucket>.<object> form across notebooks, source "
                "selection, ingestion, explorer payloads, and smoke tests; "
                "the former workspace-prefixed S3 namespace is no longer "
                "emitted or accepted."
            ),
            (
                "Fully quoted S3 Parquet references such as "
                '"s3"."bucket"."file.parquet" now use isolated S3 reads and '
                "avoid waiting on shared DuckDB catalog access."
            ),
        ],
    },
    {
        "version": "0.10.14",
        "releasedAt": "2026-06-16T10:21:59+02:00",
        "features": [
            (
                "Query process regression coverage now verifies that shared "
                "DuckDB access and Parquet source file handles are released "
                "after both completed and cancelled query runs."
            ),
        ],
    },
    {
        "version": "0.10.13",
        "releasedAt": "2026-06-16T10:07:05+02:00",
        "features": [
            (
                "Pipeline mode now labels SQL work as stages in both the "
                "cell header and primary run action, while exploration mode "
                "continues to show Run Cell."
            ),
        ],
    },
    {
        "version": "0.10.12",
        "releasedAt": "2026-06-16T09:37:13+02:00",
        "features": [
            (
                "SQL query results now reconcile live job status while a run "
                "is active, so fast count-style queries that already show "
                "rows automatically switch from queued or running to "
                "completed even if the final realtime event is delayed."
            ),
        ],
    },
    {
        "version": "0.10.11",
        "releasedAt": "2026-06-15T20:26:14+02:00",
        "features": [
            (
                "Pipeline stage monitoring now previews the generated Parquet "
                "output after materialization, so pipeline runs report the "
                "same rows, columns, and progress-event granularity as "
                "exploration queries."
            ),
            (
                "The Query Monitoring page now loads query-job snapshots "
                "before materialized-stage fallbacks, preventing pipeline "
                "rows from losing CPU, RAM, timing, and event telemetry on "
                "initial render."
            ),
        ],
    },
    {
        "version": "0.10.9",
        "releasedAt": "2026-06-13T18:56:21+02:00",
        "features": [
            (
                "Data pipeline runs now appear in the top-right message "
                "centre with running, progress, completed, cancelled, "
                "warning, and failure states from the materialized-stage SSE "
                "stream."
            ),
            (
                "Pipeline stage execution now updates the selected cell's "
                "Query Monitoring panel immediately with live materialized "
                "stage rows, so running, completed, warning, and failed "
                "stage work is visible without waiting for an exploration "
                "query job."
            ),
        ],
    },
    {
        "version": "0.10.8",
        "releasedAt": "2026-06-13T08:02:57+02:00",
        "features": [
            (
                "SQL notebook cells now include a Compare action that opens "
                "a side-by-side Virtual SQL diff against another SQL cell in "
                "the same or another notebook."
            ),
            (
                "The Compare dialog defaults to another SQL cell in the "
                "current notebook when available, uses live unsaved editor "
                "text for the active notebook, and shows line-level changed, "
                "added, removed, and unchanged counts."
            ),
        ],
    },
    {
        "version": "0.10.7",
        "releasedAt": "2026-06-12T23:19:11+02:00",
        "features": [
            (
                "S3 source navigation now renders bucket contents as a deep "
                "folder tree, keeps generated Parquet datasets copyable as "
                "folder/*.parquet references, and uses exact references for "
                "true single-file objects."
            ),
            (
                "Notebook and pipeline source handling now rewrites virtual "
                "S3 references consistently for exploration and materialized "
                "stage execution, while source navigation resolves generated "
                "part files back to their logical dataset objects."
            ),
            (
                "Query monitoring now records Run Cell attempts immediately, "
                "keeps warnings and failures visible through SSE updates, and "
                "uses blue running states, green completed states, orange "
                "warnings, and red failed, cancelled, aborted, or incomplete "
                "states."
            ),
            (
                "Nested CTEs such as the Kostenbelege UNIO branch are no "
                "longer reported as missing external sources during source "
                "validation."
            ),
            (
                "Clicking query result or query-history timing values now "
                "copies a tab-separated timing table with Total elapsed and "
                "phase timings."
            ),
        ],
    },
    {
        "version": "0.10.6",
        "releasedAt": "2026-06-11T14:21:26+02:00",
        "features": [
            (
                "Shared notebook deletes now immediately show a DELETION IN "
                "PROGRESS state in the sidebar and workspace header while the "
                "server request is still pending, with notebook edit, delete, "
                "copy, share, version, and stale shared-sync actions disabled "
                "until the request completes."
            ),
            (
                "Navigate to source object now resolves direct S3 "
                "read_parquet paths and Local Workspace physical DuckDB "
                "relations back to the logical source objects shown in the "
                "sidebar."
            ),
        ],
    },
    {
        "version": "0.10.5",
        "releasedAt": "2026-06-11T13:04:52+02:00",
        "features": [
            (
                "Shared notebook deletes now persist tombstone markers in "
                "local and S3-backed shared notebook stores, so accidental "
                "Test 3.1 - Problem Solving copies can be removed without "
                "restart seeding or stale browser sync recreating them."
            ),
            (
                "Data pipeline materialized stages now apply the same virtual "
                "S3 source rewrite used by exploration mode before writing "
                "each stage to Parquet, including the Kostenbelege KBPO "
                "compatibility projection for the original query."
            ),
        ],
    },
    {
        "version": "0.10.4",
        "releasedAt": "2026-06-11T11:26:32+02:00",
        "features": [
            (
                "Editable seeded shared notebooks now preserve their stable "
                "notebook id when syncing edits, preventing the Test 3.1 - "
                "Problem Solving notebook from creating duplicate shared "
                "copies."
            ),
            (
                "Shared notebook saves now repair stale browser requests that "
                "omit the seeded notebook id by matching restart-seeded "
                "notebook metadata before creating a new shared notebook."
            ),
        ],
    },
    {
        "version": "0.10.3",
        "releasedAt": "2026-06-10T22:44:41+02:00",
        "features": [
            (
                "Kostenbelege 3.1 now includes a shared Test 3.1 - Problem "
                "Solving notebook linked from the loader card, with editable "
                "cell processing hints and result expectations for each SQL "
                "investigation step."
            ),
            (
                "Notebook SQL now keeps canonical virtual S3 paths visible "
                "while DuckDB execution rewrites them to generated S3 Parquet "
                "read_parquet scans, including KBPO compatibility binding for "
                "the original Kostenbelege query."
            ),
            (
                "Notebook actions now include Share Notebook, with copyable "
                "references, email drafts, and share-and-link handling for "
                "private local notebooks."
            ),
            (
                "Query results now surface worker warnings and clearer "
                "unexpected worker-exit diagnostics alongside existing SQL "
                "errors."
            ),
        ],
    },
    {
        "version": "0.10.2",
        "releasedAt": "2026-06-09T22:33:38+02:00",
        "features": [
            (
                "SQL notebook cells now include a Navigate to source object "
                "button that opens the Data Sources navigation, expands the "
                "referenced table or S3 object, and flashes the matching row."
            ),
            (
                "Prepared query SQL now returns source-object metadata for "
                "PostgreSQL relations, direct S3 objects, and completed "
                "stage-backed S3 Parquet outputs so inspection controls can "
                "resolve the real source behind a cell."
            ),
        ],
    },
    {
        "version": "0.10.1",
        "releasedAt": "2026-06-09T11:28:31+02:00",
        "features": [
            (
                "Query result timing can now expand Total elapsed into the "
                "recorded timestamp checkpoints and the computed total."
            ),
            (
                "Notebook cells now include a Check sources toggle that is off "
                "by default, allowing proven queries to skip expensive source "
                "existence preflight validation."
            ),
            (
                "Pipeline graph refreshes preserve the rendered chart and table "
                "while a run is starting, and running stage box and row glows now "
                "animate at half speed for calmer progress feedback."
            ),
        ],
    },
    {
        "version": "0.10.0",
        "releasedAt": "2026-06-09T08:30:00+02:00",
        "features": [
            (
                "Exploration notebook cells now resolve completed stage outputs "
                "and direct S3 Parquet aliases to isolated DuckDB "
                "read_parquet reads, avoiding shared DuckDB file-lock waits for "
                "read-only S3 queries."
            ),
            (
                "Query cells now include a Virtual/DuckDB SQL toggle that shows "
                "either the user-facing SQL or the final SQL submitted to DuckDB "
                "without mutating the editable cell SQL."
            ),
            (
                "DuckDB execution classification now ignores SQL comments and "
                "quoted strings while scanning for mutating keywords, so comments "
                "such as '-- Call 1' no longer route read-only SELECT queries "
                "through the shared write path."
            ),
        ],
    },
    {
        "version": "0.9.28",
        "releasedAt": "2026-06-04T17:49:49+02:00",
        "features": [
            (
                "Query monitoring now shows a DuckDB spill chart beside CPU "
                "and RAM, including this-query spill, other shared spill, the "
                "configured spill quota, and shared workspace disk headroom."
            ),
            (
                "DuckDB query workers now use per-query spill subdirectories "
                "under the configured temp root, allowing the app to attribute "
                "temporary spill usage to the active query and clean up the "
                "worker spill directory after the query exits."
            ),
            (
                "Service Consumption now includes a DuckDB spill quota panel "
                "that charts active query spill, total DuckDB spill, hydrated "
                "query cache usage, and the configured DuckDB temp quota over "
                "the retained monitoring window."
            ),
            (
                "Hydrate cache and source validation now also use current S3 "
                "discovery specs when the rendered source catalog is stale, so "
                "SQL aliases such as s3.bucket.path.file.parquet can be cached "
                "without waiting for a sidebar/catalog refresh."
            ),
            (
                "Hydrated DuckDB runtime caches now coordinate cache database "
                "writes with a local write lock, retry transient WAL lock "
                "conflicts, and stop waiting cleanly when another process still "
                "owns the cache lock."
            ),
            (
                "Settings now includes Runtime Storage, showing temporary "
                "DuckDB spill usage, hydrated cache datasets, linked notebook "
                "cells, cache sizes, source revisions, and per-dataset cache "
                "deletion without deleting active spill files."
            ),
            (
                "DuckDB query workers now support explicit runtime resource "
                "settings for memory limit, threads, temp directory, temp "
                "directory quota, and insertion-order preservation; the RHOS "
                "deployment uses a 20 GiB memory limit, four threads, and an "
                "expanded 96 GiB spill quota inside the 100 GiB emptyDir."
            ),
            (
                "The RHOS deployment now provides a 100 GiB workspace emptyDir, "
                "leaving more temporary SSD headroom for hydrated runtime query "
                "caches and larger DuckDB spill workloads."
            ),
            (
                "Hydrate cache preview, rehydrate, expire, and delete actions "
                "now resolve Local Workspace aliases before calling the backend, "
                "so runnable cells no longer send an empty local relation map "
                "to cache validation."
            ),
            (
                "Query cache endpoints now return structured JSON errors for "
                "DuckDB or runtime hydration failures instead of surfacing an "
                "uncaught ASGI exception, and the notebook marks the cache state "
                "as Error without crashing the page."
            ),
            (
                "Query-job log lines now place the local date and time immediately "
                "after the log level and before [bdw-query], making long-running "
                "query progress logs easier to scan chronologically."
            ),
            (
                "The notebook menu now says Restart Python session and explains "
                "that it clears Python variables, imports, and in-memory state "
                "for the current notebook while leaving saved cell code intact."
            ),
            (
                "SQL cells now use an accessible Hydrate cache switch that "
                "hydrates runtime DuckDB cache tables immediately, shows "
                "building/deleting feedback in the cell, and deletes the "
                "matching runtime cache files when turned off."
            ),
            (
                "The cache hydration details dialog now clearly separates the "
                "temporary source view/relation from the runtime cache table, "
                "labels cache tables as Runtime table, and surfaces source "
                "revision, row count, cache size, indexes, and last hydrated "
                "metadata."
            ),
            (
                "Query results now indicate when a runtime cache was used, "
                "while per-cell cache tables remain hidden from the normal Data "
                "Source Explorer so temporary execution artifacts do not appear "
                "beside durable business data."
            ),
        ],
    },
    {
        "version": "0.9.19",
        "releasedAt": "2026-05-21T17:39:42+02:00",
        "features": [
            (
                "Shared Workspace S3 Parquet uploads now expose clear "
                "optimization controls for Off, Recommended, and Manual modes, "
                "including Hive partitioning guidance, partition/sort/ART index "
                "column selectors, schema preview for Parquet files, CSV and ZIP "
                "column recommendations, and safe backend handling for manual "
                "partitioned writes."
            ),
            (
                "SQL cells now include DuckDB runtime options for S3 Parquet "
                "Hive partition interpretation and Hydrate cache, with persisted "
                "settings, cache status checks, visible stale/missing/expired "
                "states, a detailed cache hydration plan modal, and automatic "
                "local DuckDB table plus ART index hydration for known referenced "
                "S3 Parquet sources."
            ),
            (
                "The PoC Tests / Performance Options folder now includes linked "
                "federal tax Parquet optimization notebooks and loaders for Off, "
                "Recommended, Manual partitioning with and without Hive folders, "
                "and DuckDB cache mode; those loaders default to 1 GB for "
                "repeatable performance comparison runs."
            ),
            (
                "Query result timing now shows an intuitive Total elapsed value "
                "that cannot move backward when a run completes, explains the "
                "number directly on hover, and keeps backend phase timings, "
                "CPU/RAM sample charts, and comparison badges as secondary "
                "diagnostics."
            ),
            (
                "MWA Abrechnung S3 Parquet notebooks now include an ART index "
                "demonstration cell that materializes Parquet data into DuckDB, "
                "creates an index, and uses EXPLAIN ANALYZE to show index-scan "
                "behavior for equality lookups."
            ),
        ],
    },
    {
        "version": "0.9.18",
        "releasedAt": "2026-05-19T20:25:32+02:00",
        "features": [
            (
                "The Data Sources workbench now reuses the notebook source-tree "
                "browser for Shared Workspace S3, Local Workspace IndexDB, and "
                "PostgreSQL sources, keeping catalog, bucket, folder, object, "
                "and row action visuals consistent while preserving the "
                "right-side selection details."
            ),
        ],
    },
    {
        "version": "0.9.17",
        "releasedAt": "2026-05-19T12:53:04+02:00",
        "features": [
            (
                "CSV ingestion now exposes server-side Step 2 diagnostics in "
                "the UI, browser console, upload-session state, and backend "
                "logs, including CSV validation, conversion, S3 bucket checks, "
                "S3 upload start/completion, and object verification."
            ),
        ],
    },
    {
        "version": "0.9.16",
        "releasedAt": "2026-05-19T12:35:24+02:00",
        "features": [
            (
                "The shell tagline now reads Data Analytics and AI Feed, "
                "matching the updated DAAIF Factory browser and page branding."
            ),
        ],
    },
    {
        "version": "0.9.15",
        "releasedAt": "2026-05-19T12:25:34+02:00",
        "features": [
            (
                "The workbench has been softly renamed to DAAIF Factory "
                "across page headings, browser titles, runtime branding, "
                "and visible application service labels without changing "
                "routes, identifiers, or backend behavior."
            ),
        ],
    },
    {
        "version": "0.9.14",
        "releasedAt": "2026-05-19T11:41:41+02:00",
        "features": [
            (
                "The Kostenbelege 3.1 loader now reports each setup step and "
                "batch start before the writes run, making slow production "
                "runs visible while it opens connections, cleans S3, creates "
                "tables, and pushes OLTP, OLAP, and S3 Parquet data."
            ),
        ],
    },
    {
        "version": "0.9.13",
        "releasedAt": "2026-05-19T11:21:03+02:00",
        "features": [
            (
                "DuckDB read-only query workers now skip transient S3 source "
                "view bootstrapping when they must use the shared workspace "
                "database, avoiding CREATE failures on immutable notebooks "
                "that resolve through the read-only workspace path."
            ),
        ],
    },
    {
        "version": "0.9.12",
        "releasedAt": "2026-05-19T11:04:53+02:00",
        "features": [
            (
                "SQL notebook editors now include an expand/collapse control "
                "next to Copy SQL, allowing long SQL cells to grow to their "
                "full content height and then return to the default size."
            ),
            (
                "Query in new notebook now opens the notebook immediately with "
                "a runnable SELECT * query, then enriches it with explicit "
                "columns after source metadata arrives if the cell was not edited."
            ),
            (
                "Creating and opening local notebooks now avoids a full notebook "
                "sidebar metadata sweep, keeping the plus-button flow responsive "
                "as the notebook tree grows."
            ),
        ],
    },
    {
        "version": "0.9.11",
        "releasedAt": "2026-05-19T10:32:28+02:00",
        "features": [
            (
                "Kostenbelege 3.1 now includes an optimized S3 Parquet DuckDB "
                "notebook that preserves the original fallback semantics while "
                "splitting the KBHP lookup into hash-joinable branches."
            ),
            (
                "Notebook browser titles now include the active notebook name, "
                "so copied notebook URLs resolve to meaningful tab names such as "
                "DAAIF Factory - Notebook XY."
            ),
            (
                "S3 object, folder, and bucket deletes now emit detailed backend "
                "diagnostic logs for client requests, S3 list/delete invocations, "
                "S3 responses, confirmations, and failures."
            ),
        ],
    },
    {
        "version": "0.9.10",
        "releasedAt": "2026-05-19T10:08:54+02:00",
        "features": [
            (
                "Kostenbelege 3.1 now includes linked native PostgreSQL notebooks "
                "for OLTP and OLAP so loader users can compare the same query "
                "against DuckDB-over-S3, DuckDB-over-Postgres, and direct Postgres."
            ),
            (
                "The native PostgreSQL Kostenbelege notebooks quote generated "
                "mixed-case source columns, allowing the direct Postgres execution "
                "path to run the 3.1 query successfully."
            ),
        ],
    },
    {
        "version": "0.9.9",
        "releasedAt": "2026-05-18T17:02:07+02:00",
        "features": [
            (
                "Query Runs is now named Query Monitoring across the workbench, "
                "including navigation shortcuts, headers, browser labels, and route tests."
            ),
            (
                "Query Monitoring now keeps the oldest progress records and summarizes "
                "repeated messages with occurrence counts plus first and last timestamps."
            ),
            (
                "CPU telemetry is now reported as process capacity percent with raw "
                "core-percent details preserved, while running DuckDB jobs no longer "
                "show misleading 100 percent completion when live progress is unavailable."
            ),
            (
                "Resource charts are shown above event details when enabled, and each "
                "recorded query links back to the notebook that executed it."
            ),
            (
                "Kostenbelege 3.1 now has a seeded data loader and ready-to-run notebooks "
                "for OLTP Postgres, OLAP Postgres, and S3 Parquet sources."
            ),
        ],
    },
    {
        "version": "0.9.8",
        "releasedAt": "2026-05-18T13:43:12+02:00",
        "features": [
            (
                "Read-only S3 and local workspace DuckDB queries now run in "
                "isolated worker-local connections, avoiding long waits on the "
                "shared DuckDB file lock while keeping process-based cancel and "
                "kill handling."
            ),
            (
                "Query timing and progress diagnostics now identify the DuckDB "
                "execution path, shared-lock owner, queue depth, source setup time, "
                "and stale lock recovery details."
            ),
            (
                "Notebook SQL editors now expose a hover copy action, and query "
                "result panels can be collapsed and expanded without losing the "
                "rendered result state."
            ),
            (
                "New S3 bucket names are normalized to SQL-friendly, S3-valid names, "
                "while existing digit-start buckets continue to work for listing, "
                "querying, CSV downloads, and prepared ZIP downloads."
            ),
            (
                "Prepared S3 ZIP downloads now create a background job before "
                "object validation, so production users see the download popup and "
                "any later object-store failure is reported inside that job."
            ),
        ],
    },
    {
        "version": "0.9.7",
        "releasedAt": "2026-05-18T11:25:57+02:00",
        "features": [
            (
                "Read-only DuckDB query jobs no longer wait behind a pending "
                "writer when no writer is active, preventing unrelated write-like "
                "operations from causing long allocation waits for analyst queries."
            ),
            (
                "Regression coverage now proves read queries can still start while "
                "a writer is queued, while writers continue to wait for active reads "
                "and active writes still block new readers."
            ),
        ],
    },
    {
        "version": "0.9.6",
        "releasedAt": "2026-05-18T10:49:27+02:00",
        "features": [
            (
                "The Query Workbench landing area now includes shortcut tiles "
                "for creating a new notebook, continuing with the last notebook, "
                "and opening Query Monitoring, with icons and tooltips for each action."
            ),
            (
                "Query Runs now includes a Live Queries only toggle and stores "
                "backend query progress events in the monitoring history so queued, "
                "running, completed, failed, and cancelled work can be inspected."
            ),
            (
                "SQL query jobs now report a timing breakdown across browser "
                "pre-submit, backend preparation, DuckDB allocation wait, isolated "
                "worker startup, engine execution, result fetch, delivery, and total time."
            ),
            (
                "Backend query logs and progress events now distinguish real DuckDB "
                "access waits from worker startup time and include DuckDB coordinator "
                "state, making production queueing delays easier to diagnose."
            ),
        ],
    },
    {
        "version": "0.9.5",
        "releasedAt": "2026-05-15T16:20:17+02:00",
        "features": [
            (
                "S3 object, folder, and bucket deletes now run as background "
                "jobs so the UI receives an immediate accepted response instead "
                "of waiting long enough to hit gateway timeouts."
            ),
            (
                "Backend S3 delete logs now record every requested path, job "
                "phase, throttled heartbeat, completion, and failure with "
                "Zurich timestamps and aggregate delete counts."
            ),
            (
                "Bucket deletes now handle object-store finalization lag by "
                "staying in a finalizing state and retrying before reporting a "
                "real failure."
            ),
            (
                "The Data Sources sidebar and S3 explorer show compact file "
                "names again while keeping exact SQL query paths available for "
                "copy actions, tooltips, validation, and autocomplete."
            ),
        ],
    },
    {
        "version": "0.9.4",
        "releasedAt": "2026-05-15T15:01:16+02:00",
        "features": [
            (
                "SQL query jobs now emit low-noise backend lifecycle logs with "
                "CET/Zurich timestamps, notebook and cell metadata, start, "
                "heartbeat, completion, failure, and cancellation events."
            ),
            (
                "Ongoing query logs are throttled to a configurable heartbeat "
                "interval and include only metadata such as progress label, "
                "DuckDB progress percentage, PID, CPU, and RAM when available."
            ),
            (
                "DuckDB-backed SQL jobs can attach compact JSON profiling "
                "summaries to terminal logs without writing verbose operator "
                "trees, raw SQL, result rows, or secrets into production logs."
            ),
            (
                "S3 source log summaries are capped and include discovered "
                "bucket, key, format, and query alias metadata for CSV and "
                "Parquet sources while keeping long source lists bounded."
            ),
        ],
    },
    {
        "version": "0.9.3",
        "releasedAt": "2026-05-15T11:44:33+02:00",
        "features": [
            (
                "SQL completion now suggests discovered Shared Workspace S3 "
                "object paths through readable aliases such as "
                "s3.bucket.folder.file.csv, helping analysts query existing "
                "CSV and Parquet objects without guessing path segments."
            ),
            (
                "The recommender keeps the existing alias execution path, so "
                "selected S3 paths continue to validate and rewrite to physical "
                "DuckDB relations only when a query runs."
            ),
            (
                "The Data Sources sidebar and S3 explorer now show the exact "
                "query path hierarchy for discovered objects, including prefixes, "
                "and object menus can copy query paths for S3, PostgreSQL, and "
                "Local Workspace sources."
            ),
            (
                "Regression coverage now verifies S3 CSV and Parquet alias "
                "completion paths, source validation, explorer query-path display, "
                "copy actions, and a browser smoke that executes both file formats "
                "through the SQL editor."
            ),
        ],
    },
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
                "The workbench shell now uses DAAIF Factory branding, shows realtime SSE connection "
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
    notes = [
        {
            "version": str(entry["version"]),
            "releasedAt": str(entry["releasedAt"]),
            "features": [str(feature) for feature in entry["features"]],
        }
        for entry in RELEASE_NOTES
    ]
    if notes:
        notes[0]["featureList"] = {
            "title": str(CURRENT_FEATURE_LIST["title"]),
            "introduction": str(CURRENT_FEATURE_LIST["introduction"]),
            "pocNote": str(CURRENT_FEATURE_LIST["pocNote"]),
            "features": [
                {
                    "title": str(feature["title"]),
                    "description": str(feature["description"]),
                }
                for feature in CURRENT_FEATURE_LIST["features"]
            ],
        }
    return notes

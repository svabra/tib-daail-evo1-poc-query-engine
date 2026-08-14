# DAAIF customer-journey UI performance

Measured on 2026-08-13 from the current source tree. Payload sizes are raw UTF-8
response-body bytes before transport compression. The measurement renders the
real FastAPI/Jinja route functions with all 17 discovered loader definitions and
with 7 MB of sentinel editor metadata (4 MB source options plus 3 MB completion
schema), so an accidental return to inline metadata is visible immediately.

| Surface | Baseline | Current | Reduction | Acceptance |
| --- | ---: | ---: | ---: | --- |
| Direct `GET /ingestion-workbench` shell | 7.72 MB | 109,131 B | 98.586% | at least 80% smaller: pass |
| Deferred `GET /sidebar?mode=loader&source_tree=deferred&notebook_tree=deferred&runbook_tree=deferred` | 6.29 MB | 5,221 B | 99.917% | at most 25,000 B: pass |

The rebuilt live application was also sampled over HTTP on `127.0.0.1:8000`.
Its complete direct Ingestion response was 586,116 B (92.4% below the 7.72 MB
baseline), the Loader workbench response was 24,682 B, the deferred Loader
sidebar remained 5,221 B, and the lightweight notebook search index was
44,370 B. The larger live Ingestion value includes normal shell content that is
not part of the sentinel-heavy route regression; both measurements independently
meet the requested payload acceptance threshold.

The executable regression is
`tests/test_customer_journey_ui.py::CustomerJourneyUiTests::test_direct_ingestion_and_deferred_loader_sidebar_payloads`.
It also proves that the source-options and completion-schema sentinels do not
occur in the ingestion response. The Loader source and runbook trees are fetched
only when their visible disclosure is opened. Loader or ingestion completion
marks a hidden source tree dirty; the complete tree is refreshed when it next
becomes visible.

## P0/P1 changes included

- Editor source options, SQL completion schema and release notes are no longer
  embedded in Home, Ingestion or Loader shells. ETagged metadata endpoints load
  them on demand.
- Workspace navigation uses a monotonically increasing epoch and one shared
  `AbortController`. A newer route aborts the older request; responses recheck
  their epoch before DOM, history, URL or sidebar commits.
- Startup continuation derives its action from the currently visible route and
  exits if navigation occurred while initial job snapshots were loading.
- Query, Python and loader elapsed-time clocks run at no more than 1 Hz and stop
  while the document is hidden. SSE terminal/error/notification events still
  render immediately.
- Versioned static URLs use the stable build version and receive
  `Cache-Control: public, max-age=31536000, immutable`.
- Query monitor, notification list and result panels keep render signatures so
  unchanged versions do not replace their DOM.

## Deferred next stage

These changes need production-like traffic measurements and are deliberately
outside this safe P0/P1 patch:

- Delta SSE payloads keyed by topic version/job ID, rather than full snapshots.
- HTTP compression for HTML/JSON/static responses with an explicit exclusion or
  flush-safe configuration for `/api/events/stream`, so SSE is never buffered.
- JavaScript module bundling, code splitting and preload selection based on route
  coverage; retain source maps and cache chunks by content hash.
- A browser performance trace for long tasks, layout shifts, editor initialization
  and IndexedDB serialization after the full cross-app demo data is running.

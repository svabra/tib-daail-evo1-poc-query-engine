# Repository maintenance

## Supported entry points

- Application: `bdw/app.py` and `bdw/bit_data_workbench/main.py`.
- Local runtime: `scripts/start-f5-dependencies.ps1` and `scripts/start-bdw-dev.ps1`.
- Backend regression suite: `tests/`, collected by `pytest`.
- Browser regression suite: `scripts/playwright-smokes.json`, consumed by both
  `scripts/run-playwright-regressions.ps1` and `tests/test_playwright_smokes.py`.
- Current product behavior: [MWST journey](mwst-journey.md) and
  [HTTP product formats](http-product-formats.md).

## Cleanup on 13 November 2026

Removed the original ingestion and data-product delivery plans: those flows have shipped,
and their old assumptions no longer describe the implemented multi-format DaCa integration.
The maintainability foundation plan remains an active roadmap.

Removed the unused PostgreSQL explorer and query-export forwarding modules. Consumers already
import the implementations in `backend/data_sources/postgres/` and
`static/js/data-exporters/export-format-definitions.js` directly. Query results are rendered
by `static/js/query-ui.js` and `python-ui.js`; the old, unreferenced Jinja result partial was removed.

Removed the earlier recent-notebooks smoke, whose recency and five-item limit are covered by
the registered recent-activity smoke together with private/shared isolation and blank-notebook
reuse. Removed the unregistered, one-off responsiveness probe. Timing regressions and the
documented DuckDB benchmarks remain. The existing real three-stage pipeline rerun regression
is now registered in the shared smoke manifest, with its existing 90-second execution budget
honored by both harnesses. An explicit harness timeout still takes precedence.

Browser import and handoff checks now open `/ingestion-workbench/manual`. The parent route
has become the sourcing-method selection page; the dedicated navigation smoke still verifies
that choice, while format-specific checks exercise the manual import screen.

Source validation checks now follow tracked backend query jobs: a missing catalog relation
produces a failed job with a named error, followed by a successful valid query. Missing local
file aliases still fail during browser preparation before a job is submitted. Execution and
monitoring checks use independent browser-local notebooks; the workbench and recent-activity
checks continue to exercise creation and blank-notebook reuse through the UI. Monitoring
checks allow pending timings and flat resource curves while retaining worker, cancellation,
history, chart rendering and layout assertions. Sidebar folder checks now wait for the existing
`workbenchInteractive` readiness signal after reloads before opening deferred navigation trees.

The browser checks exposed two application regressions. Notebook creation and folder actions
now load the deferred sidebar tree before using it, including when the sidebar is hidden.
LocalWorkspace file schemas now keep the prefix required by isolated read workers. A backend
regression verifies that a synced CSV can be read independently while the shared database is open.

The repository-wide Ruff check also removed unused imports and local variables in active
backend modules, scripts and tests. Calls and assertions with observable behavior are retained.
The DataExchange bucket-name helper remains an explicit public re-export.

The configured dependency audit found advisories in the old Bleach, Starlette and multipart
pins. Their fixed versions are pinned in `bdw/requirements.txt`, together with a compatible
FastAPI version, and are included in the regression verification.

Active tests, data generators, runtime data, migration/deployment configuration, licensed
assets and offline vendor dependencies are retained. Git history retains the removed files.

## Verification before committing

Run from the repository root:

```powershell
./.venv/Scripts/python.exe -m pytest --cov=bdw/bit_data_workbench --cov-branch --cov-report=xml
./.venv/Scripts/python.exe scripts/versioning.py check
uvx --from pre-commit==4.6.0 pre-commit run --all-files
```

With a temporary local runtime available, run the browser checks through the shared harness:

```powershell
$env:RUN_PLAYWRIGHT_SMOKES = '1'
$env:PLAYWRIGHT_BASE_URL = 'http://127.0.0.1:8011'
./.venv/Scripts/python.exe -m pytest tests/test_playwright_smokes.py
```

Generated browser evidence belongs in ignored `output/playwright/`, not in application source.
All of `output/` is ignored, including isolated verification runtime files.
Stop temporary servers and dependencies after verification when the workstation started stopped.

## Cleanup verification

Verified on 13 November 2026:

- The complete backend run passed 757 tests and 39 subtests, with 62 browser entries skipped
  in that run because they require an explicitly enabled live runtime. Coverage XML was generated.
- All 33 default browser regressions were verified against an isolated runtime with temporary
  PostgreSQL, MinIO and DaCa services: 31 passed in the complete matrix, and the two folder
  scenarios passed on a focused rerun after correcting their post-reload readiness checks.
  The 29 extended browser scenarios remain opt-in.
- The application Docker image built successfully with the updated dependencies and
  preinstalled DuckDB extensions.
- Version consistency remains valid for 0.10.56, and `pip check` reports no broken dependencies.
- The configured Ruff and pip-audit hooks passed; the resolved Python requirements have no
  known vulnerabilities reported by pip-audit.

Temporary runtime state and generated evidence are disposable. Ordinary workspace data and
the volumes of the previously stopped application stacks are retained.

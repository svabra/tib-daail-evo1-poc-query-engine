# Data Source Catalog visual QA

## Evidence

- Reference: `output/playwright/daca-products-table-reference.png` — DaCa `/products`, desktop viewport 1440 × 1000 at DPR 1.
- Implementation: `output/playwright/data-sources-table-desktop-final.png` — DAAIF `/data-sources`, desktop viewport 1440 × 1000 at DPR 1.
- Side-by-side comparison: `output/playwright/design-qa-comparison.png`.
- Interaction state: `output/playwright/data-sources-table-hover-final.png` verifies progressive-disclosure actions on hover.
- Mobile state: `output/playwright/data-sources-table-mobile.png` verifies stacked rows without horizontal page overflow.
- Shared Sourcing Hub component: `output/playwright/sourcing-hub-table-desktop.png`.

## Pass 1 findings and corrections

- Filter selects could truncate their values at the desktop reference width. Minimum widths were added while retaining responsive wrapping.
- Row actions could wrap into visually noisy multi-line controls. The action group now stays compact and uses hover/focus disclosure on pointer devices.
- The old card/detail split consumed excessive vertical and horizontal space. It was replaced by the DaCa-like table/list catalog and a separate full-width detail route.
- Technology recognition was too text-heavy. Self-hosted Simple Icons assets now identify Oracle, PostgreSQL, S3/MinIO, and local workspace sources.

## Final comparison

- Typography: heading scale, uppercase red section labels, body rhythm, and table hierarchy match the established DaCa visual language.
- Spacing and layout: metric strip, filters, result count, view toggle, row density, and full-width content align with the reference while preserving DAAIF's existing application shell.
- Colors and tokens: neutral canvas, white surfaces, Swiss red accents, navy actions, subtle borders, and semantic status badges use the existing DAAIF/DaCa token family.
- Asset fidelity: vendor icons are self-hosted library assets rather than approximated drawings or emoji.
- Copy: labels are technology-neutral and consistently describe platform access versus DaCa grants.
- Interaction: Table is the default; List/Table preference persists for the browser session; hover, keyboard focus, touch actions, filtering, pagination, and contextual actions are covered.
- Responsive behavior: desktop and mobile states have no horizontal page overflow; mobile rows stack into readable labeled fields.

The wider DAAIF navigation and workbench context remain intentionally different from DaCa; the catalog component itself follows the supplied DaCa data-product reference.

## Bucket creation follow-up

- Reference: the user-provided desktop screenshot of Step 4 and the inaccessible-bucket error.
- Implemented state: Step 2 now combines the existing-bucket selector with a compact `Create bucket` action, inline pending/success/error feedback, and a helper explaining the two valid paths.
- Functional verification: a uniquely named bucket was created through the same `/api/s3/explorer/buckets` endpoint, appeared immediately in the ingestion context, and was deleted successfully through the normal S3 delete job.
- Visual capture is currently blocked because no in-app or connected browser surface is available in this session. The new interaction therefore has not received the required same-viewport screenshot comparison.

## Data Source Explorer follow-up

- Reference: the user-provided desktop screenshot of `/data-sources/browser?source_id=pg_oltp` and the previously verified DaCa-like DAAIF source catalog states listed above.
- Audit finding: the oversized hero, two-card source switcher, nested source-detail promotion and split master/detail frame delayed the real browsing task and did not scale beyond a handful of sources.
- Implemented state: the explorer now reuses the shared searchable, filterable List/Table catalog with technology icons, selected-row treatment, hover/focus actions and pagination. The chosen source is summarized once, and its schema/object explorer occupies a separate full-width work area.
- Functional verification: the live route and source-catalog API return successfully, the PostgreSQL source is selected, shared catalog controls and icon assets are present, and focused route/UI tests pass.
- Visual capture remains blocked because no in-app or connected browser surface is available in this session. A same-viewport reference/implementation comparison is still required before marking visual QA as passed.

final result: blocked

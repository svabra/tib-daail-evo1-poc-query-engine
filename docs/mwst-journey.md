# MWST-Erträge für St. Gallen

This is a synthetic, fixed demonstration dated **12 November 2026**. No actual company or
taxpayer data or official revenue figures are included. DAAIF stores payloads; DaCa owns
catalog metadata, SLA revisions, approval tasks, audit history and access policies.

General creation requests also support a fixed data-basis interval or a rolling window
(days, weeks, months, years), relative to the data-as-of point of each update. The DAAIF
banner and publication retain that requirement; frequency and access-grant validity remain
separate. Owners implement the requested query and updates through the existing workflow.

The initial national and company source products grant access only to Kassandra. Jan can
read their catalog metadata and request access; he has no pre-granted source-data access.
The separate ready SG example has its own Jan grant. On an existing setup, revoke any older
Jan source grant through DaCa's owner and independent approval workflow rather than editing
published policy records. REST and all download formats must deny Jan until a new approved grant.

Jan Waser has the same synthetic profile portrait in both applications. DAAIF serves its
local copy at `/static/img/daca/jan-waser.webp`; the adjacent provenance file records the prompt.

## Local preparation

Start the existing dependencies and DAAIF on port 8011:

```powershell
./scripts/start-f5-dependencies.ps1
./scripts/start-bdw-dev.ps1 -Port 8011
```

Start the sibling DaCa repository using its Compose setup. Configure `DAAIF_UI_URL` as
`http://127.0.0.1:8011` in DaCa. DAAIF uses its existing DaCa publication/PDP configuration;
`DACA_UI_URL` selects the catalog UI address (local launcher: `http://localhost:8080`). API/PDP loopback endpoints are normalized to
`127.0.0.1`. Run the setup in a second terminal:

```powershell
./.venv/Scripts/python.exe scripts/setup-mwst-journey.py --daaif-url http://127.0.0.1:8011 --daca-url http://127.0.0.1:8080
```

The loader `mwst_journey_loader` writes three deterministic Parquet objects in bucket
`mwst-journey`. The setup registers missing products, reviews field semantics, supplements
the existing PoC SLA template, and submits and approves SLA/publication revisions as
Kassandra Valdata and Thomas Kriegli. OPA and PostgreSQL activation must both succeed.
Existing catalog products and audit events are retained. Re-running only replaces the
Journey-owned fixed source/example objects and completes missing fixture approvals. It
does not overwrite Kassandra's manually saved result or create her final product.

| Slug | Rows and access |
| --- | --- |
| `mwst-ertraege-schweiz-2016-2026` | 11 annual totals, Jan and Kassandra |
| `mwst-ertraege-firmen-2016-2026` | 2,860 company-year rows, Kassandra |
| `mwst-ertraege-firmen-sg-2026-beispiel` | Separate ready example, 60 SG firms for 2026, Jan and Kassandra |
| `mwst-ertraege-firmen-sg-2026` | Reserved for Kassandra's own manual publication |

All 26 cantons have fictitious companies: 60 in SG and eight in each other canton. Amounts
are stored as `DECIMAL(18,2)` CHF. The national sums derive from those same company values.
The canton is the registered company canton, not an estimate of where consumption occurred.
2016–2025 cover complete years; 2026 ends on 12 November and is provisional. Field metadata
documents `company_id`, `company_name`, `canton_code`, `tax_year`, `period_start`, `period_end`,
`vat_revenue_chf`, `currency`, `provisional`, `synthetic` and national `company_count`.

## Manual journey

1. Open DAAIF `/documentation/journeys/mwst-ertraege-st-gallen-2026`, or the matching
   DaCa User Journey. Choose **Jan Waser**, Data Analyst, Kanton St. Gallen. Inspect the
   national time series and the company product's prose, field descriptions, contact and
   approved SLA in DaCa.
2. Jan directly requests a new data product electronically using **Erstellungsantrag** in
   DaCa. Only the source is selected. Enter the desired name,
   data-basis dates, purpose, legal mandate, requested formats and update frequency; the
   request form no longer invents canton/year or a product name. Specify SG company rows in
   the purpose for this journey. Existing requests with explicit canton/year remain readable.
   Choose Excel, Parquet and CSV and submit. This is a request to create
   a separate product; it does not grant access to the national company payload.
3. As **Kassandra Valdata**, open the notification/task, accept the request, and follow its
   DAAIF link and open the appropriate notebook. `demoUser` and `creationRequest` preserve
   the context across systems and workspace/notebook navigation.
4. Review and run the editable SQL in `mwst-sg-filter`:

   ```sql
   SELECT * FROM s3."mwst-journey"."sources/mwst_firmen_2016_2026.parquet"
   WHERE tax_year = 2026 AND canton_code = 'SG'
   ORDER BY company_id;
   ```

   Check **60 individual company rows**. The existing result-storage option saves Parquet
   at `s3://mwst-journey/products/mwst_firmen_sg_2026.parquet`.
5. Choose **Datenprodukt veröffentlichen** on the completed saved result. Source is locked
   to the saved file; title, description and metadata suggestions remain editable. Review
   metadata and create the product. Continue in DaCa to edit/submit the product-specific
   SLA and publication, granting Jan `data.read` via HTTP. **Thomas Kriegli** performs the
   normal four-eyes decisions. Wait for confirmed activation.
6. Once the new product is published and accessible to Jan, Kassandra assigns it to the
   creation request and completes the order. Jan then receives a result notification.
   From the product's DaCa **Nutzung** tab, open the DAAIF download page and download
   Excel, Parquet or CSV.

## API and authorization

DaCa migration `0049_product_creation_requests` adds revision-controlled requests and task
links. Its `/api/v1/product-creation-requests` resource supports create/list/detail, owner
`/{id}/decision` and owner `/{id}/fulfillment`. Mutations require `If-Match` after creation.
Only the requester and assigned Data Owner can read a request. The owner is derived from
the source catalog records. Fulfillment requires the correct owner, published product,
approved SLA, matching request/canton/year scope, and effective access for the requester.
Requests transition `submitted → in_progress → fulfilled` or `submitted → rejected`.

DAAIF exposes read-only request and SLA bridges at `/api/product-creation-requests/{id}` and
`/api/public/data-products/{slug}/service-level`. Complete tabular exports are served at
`/api/public/data-products/{slug}/download/{format}` for `xlsx`, `parquet`, `csv`. REST and
downloads check the existing DaCa policy before reading data: absent identity is 401,
denial is 403, unavailable policy evaluation is 503. Downloads ignore preview limits.
The published SG source is validated for canton/year; both delivery paths retain that
product scope if the referenced source changes. This does not introduce general row-level
authorization. Demo identities are local PoC identities; the existing shared-workspace
audience model still applies to query execution.

Publication behavior lives in `static/js/result-product-publication.js`; source validation
and export logic live in `backend/data_products/tabular.py` and `downloads.py`. Any completed
Parquet result can open the existing publication dialog. The prior Gewerbesteuer journey
retains its action hook and behavior.

## Checks

### Known limitations from the 13 November 2026 replay

The MWST product's generic access UI has no four-eyes publication-submission action.
The replay submitted the existing governance API as Kassandra, then completed Thomas's
independent decision in the UI. This remains an interface gap.

Date-bounded grants without a weekly schedule use the UTC date in OPA, while DaCa's
effective-access view uses Europe/Zurich. At 00:48 Swiss time, a grant starting on
13 November appeared active in DaCa but HTTP downloads still returned 403. The replay
completed with a separately approved policy revision starting on the fixed journey date,
12 November. The timezone mismatch remains unresolved.

The quality wizard breadcrumb can reference an unrelated product, and some handoff/task
labels still name Joel Ruod instead of Kassandra. The replay verified the correct product
and owner IDs in the actual publication, SLA, governance and fulfillment records.

The completed request `NEW-286BFA7DCF17` uses the separate result slug
`mwst-ertraege-firmen-sg-2026-wiederholung-20261113`. All three browser downloads matched
every source field across 60 SG firms and CHF 10,703,041.83; 24 authorization checks passed.

`tests/test_mwst_journey.py` checks deterministic counts, annual sums, scope validation,
the editable notebook and actual read-back of all three formats, including the complete
2,860-row export. `tests/test_product_download_authorization.py` verifies default deny and
policy unavailability before any payload read. DaCa's `test_product_creation_requests.py`
checks owner derivation, foreign access, revisions, rejection, tasks and fulfillment guards.
Browser evidence belongs in `output/playwright/`.

Local acceptance on 12 November 2026 exercised Jan's form submission, Kassandra's acceptance,
editable filter execution and saved-result publication, Thomas's SLA/publication decisions,
fulfillment and Jan's result notification. Browser-downloaded Excel, CSV and Parquet each
contained the same 60 SG firms and CHF 10,703,041.83 in total, with period end 2026-11-12.
Live REST checks confirmed 11 national rows, 2,860 company-year rows and exact annual totals.
Anonymous and unauthorized access, foreign requests and attempted scope overrides were denied.
The documentation, request form and download page fit mobile viewports of 390 and 320 pixels.
Repeating setup preserved existing product identities, definitions and the manual result; the
ordinary manual-result slug remains free. The completed test uses a separate `-prueflauf` slug.

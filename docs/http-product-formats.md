# HTTP representations of one data product

Tabular products expose REST/JSON at `/api/public/data-products/{slug}` and full CSV,
Excel and Parquet files at the same path followed by `/download/csv`, `/download/xlsx`
and `/download/parquet`. The existing exports preserve the source's typed Parquet
snapshot and trusted product scope; they do not export only the JSON preview page.
REST remains paginated through `limit` and `offset`.

Every route calls `enforce_daca_policy` before reading records, using the same trusted
DaCa product ID, HTTP protocol and subject. A product's individual or group HTTP grant
therefore applies to every representation. Missing identity returns 401, denied access
403, and unavailable/malformed policy enforcement denies access. No format-specific grant
or permission bypass is introduced.

`backend/data_products/http_formats.py` adds missing format descriptors through DaCa's
existing owner-scoped endpoint API with `If-Match`, normal revision/audit behavior and
bounded conflict retries. Registration preserves the REST endpoint and all catalog
metadata, identities, SLA and policies. New local publication activates only after
registration succeeds; managed legacy products retry during product-list reconciliation.
Once complete, registration is cached for the process lifetime. No registry schema change
or modification of the original metadata-publication replay payload is needed.

DaCa's selector uses these descriptors to open the DAAIF product landing page with
`format=rest|csv|xlsx|parquet` and the existing `demoUser` context. The page automatically
fetches the selected representation with `X-DaCa-User`, downloads files, or displays the
REST JSON response. Unauthorized requests show the denial and produce no download.
DaCa does not retrieve or store the product records. The demo selector remains PoC-only.

Opening the DAAIF Data Products workbench upgrades existing managed tabular products by
adding missing descriptors. Repeated reconciliation does not create duplicate endpoints.

## Validation

66 focused backend tests and 18 subtests passed, covering initial publication, backfill,
idempotent registration, concurrent revision conflicts, full exports and fail-closed policy
checks. Live browser downloads from DaCa were read back and compared field by field with
REST: 60 SG firms (CHF 10,703,041.83) and 2,860 company-year rows (CHF 485,860,821.75).
Jan received 403 for all four formats of each national/company source and retained his
separate SG access. Missing identity returned 401 for all four. The catalog's 28 targeted
frontend tests, production build, data-model documentation check and desktop/mobile browser
checks also passed. Active MWST policy revisions were unchanged by endpoint registration.

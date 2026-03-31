# tib-daail-evo1-poc-query-engine

This repository now contains the `DAAIFL Data Workbench` web UI and the supporting local integration stack.

`DAAIFL Data Workbench` is a small FastAPI application that:

- serves a notebook-style SQL UI with Jinja2, HTMX and CodeMirror 6
- executes SQL through DuckDB in the backend
- exposes S3/MinIO and PostgreSQL as configured data sources
- ships two preinstalled smoke-test notebooks for S3 and PostgreSQL

## Architecture

```text
+---------+      HTMX / HTTP       +--------------------+
| Browser | ---------------------> | FastAPI web + api  |
+---------+                        +--------------------+
                                          |
                                          | Python call
                                          v
                                  +--------------------+
                                  | WorkbenchService   |
                                  | DuckDB connection  |
                                  +--------------------+
                                    |              |
                                    |              |
                                    v              v
                             +-------------+   +------------+
                             | S3 / MinIO  |   | PostgreSQL |
                             +-------------+   +------------+
```

The application is intentionally split into:

- `bdw/bit_data_workbench/web`: HTML routes and page composition
- `bdw/bit_data_workbench/api`: query execution and health endpoints
- `bdw/bit_data_workbench/backend`: DuckDB bootstrap, query execution and metadata

This keeps the FastAPI interface separate from the backend data-processing layer from the start.

## Local Development

Create the virtual environment:

```bash
python -m venv .venv
.\.venv\Scripts\python -m pip install -r bdw\requirements.txt
```

Start only the local integration dependencies:

```bash
docker compose up -d minio minio-init minio-seed postgres pgadmin
```

Start the FastAPI application locally with auto-reload:

```powershell
.\scripts\start-bdw-dev.ps1
```

Optional custom port:

```powershell
.\scripts\start-bdw-dev.ps1 -Port 8011
```

This starts `uvicorn --reload`, so Python, template, CSS and JS changes are picked up without a manual restart. The script uses port `8000` by default and frees that port first by stopping the local `bit-data-workbench` Docker container or any remaining listener process on the same port.

If you want to launch the app directly from VS Code with `F5`, use the checked-in `DAAIFL Workbench` launch configuration. It uses the workspace venv, brings up the local dependency stack, opens the browser automatically, and runs on `http://127.0.0.1:8010` with a dedicated local DuckDB file (`workspace/bit-data-workbench.f5.duckdb`) so it does not fight with the script-based dev server.

If you want the full app inside Docker instead:

```bash
docker compose up -d --build bit-data-workbench
```

Open the UI:

```text
http://localhost:8000
```

Local services:

- `http://localhost:8000` -> DAAIFL Data Workbench local dev server
- `http://localhost:9000` -> MinIO S3 endpoint
- `http://localhost:9001` -> MinIO console
- `http://localhost:5050` -> pgAdmin
- `localhost:5432` -> PostgreSQL

Default local credentials:

```text
MinIO root user: minioadmin
MinIO root password: minioadmin
PostgreSQL user: evo1
PostgreSQL password: evo1
PostgreSQL OLTP database: evo1_oltp
PostgreSQL OLAP database: evo1_olap
pgAdmin email: admin@daail.io
pgAdmin password: admin
S3 bucket: vat-smoke-test
```

## Application Behavior

At startup the backend:

- opens the local DuckDB database file
- loads the `httpfs` and `postgres` extensions
- creates the S3 secret from `S3_*`
- creates startup views from `S3_STARTUP_VIEWS`
- attaches PostgreSQL as `pg_oltp` and `pg_olap`
- introspects available schemas, tables and views for the sidebar and SQL completion

The UI ships two preinstalled notebooks:

- `S3 Smoke Test`
- `PostgreSQL Smoke Test`

The left navigation shows the currently available sources:

- `workspace.s3.*` for S3-backed startup views
- `pg_oltp.*` for the PostgreSQL source database
- `pg_olap.*` for the PostgreSQL target database

## Environment Variables

The same environment variable names are used locally and in Kubernetes/OpenShift:

```text
DUCKDB_DATABASE
DUCKDB_EXTENSION_DIRECTORY
MAX_RESULT_ROWS
S3_ENDPOINT
S3_REGION
S3_BUCKET
S3_ACCESS_KEY_ID
S3_SECRET_ACCESS_KEY
S3_URL_STYLE
S3_USE_SSL
S3_VERIFY_SSL
S3_CA_CERT_FILE
S3_SESSION_TOKEN
S3_STARTUP_VIEW_SCHEMA
S3_STARTUP_VIEWS
PG_HOST
PG_PORT
PG_USER
PG_PASSWORD
PG_OLTP_DATABASE
PG_OLAP_DATABASE
```

Local values are provided by `compose.yaml`.

In Kubernetes/OpenShift the BDW deployment currently reads them from:

- `tib-daail-evo1-poc-query-engine-config`
- `tib-daail-evo1-poc-query-engine-secret`

via `envFrom`.

## Build the BDW Image

Build locally:

```bash
docker build -f bdw/Dockerfile -t bit-data-workbench:0.3.13 .
```

Run directly without Compose-managed service wiring:

```bash
docker run --rm -d ^
  --name bit-data-workbench ^
  -p 8000:8000 ^
  -v "%cd%\\workspace:/workspace" ^
  -e IMAGE_VERSION=0.3.13 ^
  -e DUCKDB_DATABASE=/workspace/bit-data-workbench.duckdb ^
  -e DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions ^
  -e S3_ENDPOINT=minio:9000 ^
  -e S3_REGION=us-east-1 ^
  -e S3_BUCKET=vat-smoke-test ^
  -e S3_ACCESS_KEY_ID=minioadmin ^
  -e S3_SECRET_ACCESS_KEY=minioadmin ^
  -e S3_URL_STYLE=path ^
  -e S3_USE_SSL=false ^
  -e S3_VERIFY_SSL=false ^
  -e S3_STARTUP_VIEW_SCHEMA=s3 ^
  -e S3_STARTUP_VIEWS=vat_smoke=csv:s3://vat-smoke-test/startup/vat_smoke.csv ^
  -e PG_HOST=postgres ^
  -e PG_PORT=5432 ^
  -e PG_USER=evo1 ^
  -e PG_PASSWORD=evo1 ^
  -e PG_OLTP_DATABASE=evo1_oltp ^
  -e PG_OLAP_DATABASE=evo1_olap ^
  bit-data-workbench:0.3.13
```

### TODO

- Create notebook auto-save/versioning on query run:
  every time the user presses `Run Cell`, compare the current notebook state against the latest saved version and create a new version only if something changed.
  The comparison must include notebook title, description, tags, cells, cell order, selected data sources, SQL text, and any other persisted notebook attributes.

- Decide how frontend dependencies should be packaged into the image build:
  - keep the vendored CodeMirror / `lang-sql` tree in `bdw/bit_data_workbench/static/vendor/**` for a simple self-contained image build without npm/CDN dependencies
  - or switch to an npm/pnpm-based frontend build step for a cleaner repository and generate the browser assets during CI / image build

## Kubernetes / OpenShift

Relevant manifests:

- `k8s/bdw-deployment.yaml`
- `k8s/bdw-service.yaml`
- `k8s/bdw-route.yaml`

The route is an OpenShift `edge` route and exposes the HTTP service externally through the cluster ingress.

Current image:

```text
docker-hub.nexus.bit.admin.ch/svabra/bit-data-workbench:0.3.13
```

## Verification

Local backend verification already covered:

- S3 startup view query against `s3.vat_smoke`
- PostgreSQL query against `pg_oltp.public.vat_smoke_test_reference`

Health endpoint:

```text
http://localhost:8000/info
```

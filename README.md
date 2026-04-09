# tib-daail-evo1-poc-query-engine

## TODO

1. [AM-1] Formalize the audience model before introducing real multi-user isolation. See [AM-1] Audience Model below.

### [AM-1] Audience Model

The current workbench keeps its existing single-user behavior.

- Local Workspace notebooks, saved results, drafts, and browser UI state stay local to one browser profile through IndexedDB.
- Shared Workspace content and shared notebooks remain shared through the backend runtime and configured S3-backed storage.
- Realtime events now travel over one SSE connection, but that transport consolidation does not create per-user isolation by itself.
- Until authentication and session-aware partitioning exist, backend-managed query jobs, ingestion jobs, source discovery state, and shared notebook events still follow the current single-runtime visibility model.

This repository now contains the `DAAIFL Data Workbench` web UI and the supporting local integration stack.

`DAAIFL Workbench` is a small FastAPI application that:

- serves a notebook-style SQL UI with Jinja2, HTMX and CodeMirror 6
- executes SQL through DuckDB in the backend
- exposes S3/MinIO and PostgreSQL as configured data sources
- ships two preinstalled smoke-test notebooks for S3 and PostgreSQL

## PoC in Progress
For the sake for fast development the DAAIFL Workbench is developed in one single container. Later it will be decomposed in multiple components (UI, query cordinator, ingestion engine, API, etc.)

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

## Realtime Event Flow

The browser now uses one multiplexed SSE connection instead of opening separate streams per feature area.

```text
           commands
          HTMX / fetch / POST requests

  +---------+ ----------------------------------------------> +----------------------+
  | Browser |                                                 | FastAPI web + api    |
  |         | <---------------------------------------------- | /api/events/stream   |
  +---------+              1 x SSE connection                 +----------+-----------+
                    |
                    | topic snapshots
                    v
                    +-----------------------------+
                    | WorkbenchService realtime    |
                    | broker                       |
                    | - query-jobs                 |
                    | - data-generation-jobs       |
                    | - data-source-events         |
                    | - notebook-events            |
                    +------+------------+----------+
                      |            |
                state callbacks |            | notebook events
                      v            v
                +----------------+  +----------------+
                | Query / ingest |  | Source / note- |
                | managers       |  | book state     |
                +----------------+  +----------------+
```

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
Runtime logs now live under `logs/<context>/`. The BDW app writes to `logs/bdw/server.log`, and `.\scripts\cleanup-logs.ps1` migrates older scattered `.log` files into the matching context folders.
File logging is intended for local development only. In containerized production runs, the BDW image defaults to console logging so Kubernetes or OpenShift can collect stdout and stderr directly.

If you want to launch the app directly from VS Code with `F5`, use the checked-in `DAAIFL Workbench` launch configuration. It uses the workspace venv, brings up only the local dependency stack, stops the Docker app container if it is holding port `8000`, opens the browser automatically, and runs on `http://127.0.0.1:8000` with a dedicated local DuckDB file (`workspace/bit-data-workbench.f5.duckdb`).

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
- ensures the configured bootstrap S3 bucket exists and seeds `startup/vat_context_bootstrap.csv` when the ordered storage is empty
- creates startup views from `S3_STARTUP_VIEWS`
- attaches PostgreSQL as `pg_oltp` and `pg_olap`
- introspects available schemas, tables and views for the sidebar and SQL completion

The S3-backed loaders do not share one output bucket anymore:

- `S3 VAT Smoke Loader` writes into a dedicated bucket derived from `S3_BUCKET` with suffix `-s3-smoke`
- `PG vs S3 Contest Loader` writes its parquet output into a dedicated bucket derived from `S3_BUCKET` with suffix `-pg-vs-s3-contest`

That keeps loader cleanup isolated so removing one loader's S3 data does not wipe another loader's test bucket.

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
BDW_ENABLE_FILE_LOGGING
DUCKDB_DATABASE
DUCKDB_EXTENSION_DIRECTORY
MAX_RESULT_ROWS
S3_ENDPOINT
S3_BUCKET
S3_ACCESS_KEY_ID
S3_ACCESS_KEY_ID_FILE
S3_SECRET_ACCESS_KEY
S3_SECRET_ACCESS_KEY_FILE
S3_URL_STYLE
S3_USE_SSL
S3_VERIFY_SSL
S3_CA_CERT_FILE
S3_SESSION_TOKEN
S3_SESSION_TOKEN_FILE
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

In Kubernetes/OpenShift:

- `tib-daail-evo1-poc-query-engine-config`
- `tib-daail-evo1-poc-query-engine-secret` for PostgreSQL and S3 credentials
- `/myconfigmap/daai-brs-d/bit-ros-trusted-certs/*` for the mounted trusted CA material

The deployment uses `envFrom` for the ConfigMap and Secret, so RHOS currently supplies
`S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` directly as environment variables.

## Runtime Selection

There is no explicit `local` versus `cluster` mode flag in the application.

The same Python startup path runs in both places:

```text
Settings.from_env() -> apply_runtime_environment() -> WorkbenchService(settings)
```

What changes is who injects the environment values:

- local scripts and VS Code inject `localhost` endpoints and direct credentials
- RHOS/OpenShift injects cluster endpoints, pod metadata, TLS settings, Secret-backed S3 credentials, and the mounted trusted-certs ConfigMap

ASCII diagram:

```text
                  same application code

        +--------------------------------------+
        | Settings.from_env()                  |
        | apply_runtime_environment()          |
        | WorkbenchService(settings)           |
        +--------------------------------------+
                    ^                 ^
                    |                 |
                    |                 |
     local launcher injects           RHOS deployment injects
     localhost values                 cluster values

  +---------------------------+    +------------------------------+
  | start-bdw-dev.ps1         |    | bdw-deployment.yaml          |
  | VS Code launch.json       |    | ConfigMap + Secret           |
  |                           |    | ConfigMap volume mount       |
  | S3_ENDPOINT=localhost     |    | S3_ENDPOINT=https://...      |
  | PG_HOST=localhost         |    | S3_ACCESS_KEY_ID=...         |
  | S3_ACCESS_KEY_ID=...      |    | S3_SECRET_ACCESS_KEY=...     |
  | S3_SECRET_ACCESS_KEY=...  |    | /myconfigmap/.../ca-bundle...|
  +---------------------------+    +------------------------------+
```

The practical rule is simple: the app does not detect "where it is" by hostname. It behaves according to the environment variables and file paths it receives at startup.

## Build the BDW Image

Build locally:

```bash
docker build -f bdw/Dockerfile -t bit-data-workbench:0.4.2 .
```

Run directly without Compose-managed service wiring:

```bash
docker run --rm -d ^
  --name bit-data-workbench ^
  -p 8000:8000 ^
  -v "%cd%\\logs:/app/logs" ^
  -v "%cd%\\workspace:/workspace" ^
  -e IMAGE_VERSION=0.4.2 ^
  -e DUCKDB_DATABASE=/workspace/bit-data-workbench.duckdb ^
  -e DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions ^
  -e S3_ENDPOINT=minio:9000 ^
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
  bit-data-workbench:0.4.2
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

- `k8s/bdw-serviceaccount.yaml`
- `k8s/bdw-deployment.yaml`
- `k8s/bdw-service.yaml`
- `k8s/bdw-route.yaml`

The route is an OpenShift `edge` route and exposes the HTTP service externally through the cluster ingress.

Current image:

```text
docker-hub.nexus.bit.admin.ch/svabra/bit-data-workbench:0.4.2
```

### RHOS S3 Authentication

For the current RHOS/OpenShift setup, S3 authentication comes directly from the
Kubernetes Secret, not from Vault annotations.

Use this model in the cluster:

1. Put `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` into `tib-daail-evo1-poc-query-engine-secret`.
2. Mount the namespace ConfigMap `bit-ros-trusted-certs` into the pod at `/myconfigmap/daai-brs-d/bit-ros-trusted-certs`.
3. Set `S3_CA_CERT_FILE=/myconfigmap/daai-brs-d/bit-ros-trusted-certs/ca-bundle.crt` in `k8s/duckdb-configmap.yaml`.
4. Keep `S3_ENDPOINT=https://...` and `S3_VERIFY_SSL=true`.
5. If `ca-bundle.crt` is not the real file name, use the startup logs to see which files actually exist in the mounted directory.

This is how the S3 connection is wired in the deployment:

```yaml
envFrom:
  - configMapRef:
      name: tib-daail-evo1-poc-query-engine-config
  - secretRef:
      name: tib-daail-evo1-poc-query-engine-secret

volumeMounts:
  - name: bit-ros-trusted-certs
    mountPath: /myconfigmap/daai-brs-d/bit-ros-trusted-certs
    readOnly: true

volumes:
  - name: bit-ros-trusted-certs
    configMap:
      name: bit-ros-trusted-certs
```

And this is the matching cluster config for the application:

```yaml
data:
  S3_ENDPOINT: https://ecspr01.sz.admin.ch:9021
  S3_USE_SSL: "true"
  S3_VERIFY_SSL: "true"
  S3_CA_CERT_FILE: /myconfigmap/daai-brs-d/bit-ros-trusted-certs/ca-bundle.crt
```

In plain terms:

- the Secret provides `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`
- the trusted-certs ConfigMap is mounted as files into the pod
- boto3 and DuckDB use the mounted `ca-bundle.crt` file for TLS verification
- the startup logs recursively print the mounted cert directory so the actual file name can be verified quickly

RHOS currently injects:

```text
S3_ACCESS_KEY_ID=<from Secret>
S3_SECRET_ACCESS_KEY=<from Secret>
S3_CA_CERT_FILE=/myconfigmap/daai-brs-d/bit-ros-trusted-certs/ca-bundle.crt
```

If the platform later requires a session token as well, set it in the same Secret:

```text
S3_SESSION_TOKEN=<from Secret>
```

ASCII diagram:

```text
                   RHOS / OpenShift

    +-----------------------------------------------+
    | Pod: evo1-bdw                                 |
    |                                               |
    |  +-------------------+    reads files         |
    |  | DAAIFL Workbench  | -------------------+   |
    |  | FastAPI + DuckDB  |                    |   |
    |  +-------------------+                    |   |
    |                                               v   |
    |  /myconfigmap/daai-brs-d/bit-ros-trusted-certs/ |
    |  ca-bundle.crt                                   |
    +-----------------------------------------------+
                   ^
                   | envFrom Secret
                   |
    +-----------------------------------------------+
    | Secret: tib-daail-evo1-poc-query-engine-secret|
    +-----------------------------------------------+
                   |
                   | S3_ACCESS_KEY_ID
                   | S3_SECRET_ACCESS_KEY
                   v
    +-----------------------------------------------+
    | RHOS S3 / ECS endpoint                        |
    +-----------------------------------------------+

    +-----------------------------------------------+
    | ConfigMap: bit-ros-trusted-certs              |
    +-----------------------------------------------+
                   |
                   | ConfigMap volume mount
                   v
    +-----------------------------------------------+
    | Pod mount: /myconfigmap/daai-brs-d/           |
    |            bit-ros-trusted-certs              |
    +-----------------------------------------------+
```

Notes:

- Keep `S3_VERIFY_SSL=true` in RHOS.
- If RHOS still supplies `S3_ENDPOINT=host:9021` without a scheme, the app now normalizes that non-local endpoint to HTTPS automatically when SSL verification is enabled.
- Only set `S3_URL_STYLE` if the target ECS endpoint explicitly requires it.
- Prefer a concrete file path such as `.../ca-bundle.crt` over a bare directory for boto `verify=...`.
- Startup logs print masked environment values, the mounted configmap contents, the configured `S3_CA_CERT_FILE`, the visible files under each cert search root, and the effective CA path that was finally used.
- Startup tasks are separated with `-------------------------------` markers so the sequential work is easy to follow in the logs.

## Verification

Local backend verification already covered:

- S3 startup view query against `s3.vat_smoke`
- PostgreSQL query against `pg_oltp.public.vat_smoke_test_reference`

Health endpoint:

```text
http://localhost:8000/info
```

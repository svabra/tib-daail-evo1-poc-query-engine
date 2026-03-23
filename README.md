# tib-daail-evo1-poc-query-engine

This repository builds a small Docker image that provides:

- the DuckDB CLI
- the DuckDB UI extension
- a default startup mode that keeps the DuckDB UI reachable over HTTP

The image is intended as a local development and PoC base. By default it starts the DuckDB UI. You can still use the CLI or a shell via `docker exec` or by overriding the command.

## Scope of this image

This image only packages the DuckDB runtime and UI.

It does not embed PostgreSQL or S3/MinIO.

PostgreSQL and S3/MinIO are only needed when you want to test integrations:

- S3/MinIO for reading and writing object storage data
- PostgreSQL for later cross-system query tests

For local development, this repository provides these systems via `docker compose`.
For Kubernetes/OpenShift, the same connection values should come from `ConfigMap` and `Secret` objects.

## What the image contains

- Python 3.12
- `duckdb` Python package
- `duckdb-cli`
- a small Python launcher that starts the DuckDB UI server
- `socat`, to rebind the DuckDB UI port from loopback to the container's non-loopback interface without changing the browser-visible port

## Build the image

Build with the default DuckDB version:

```bash
docker build -t tib-daail-evo1-poc-query-engine:latest .
```

Build with an explicit DuckDB version:

```bash
docker build --build-arg DUCKDB_VERSION=1.5.0 -t tib-daail-evo1-poc-query-engine:1.5.0 .
```

## Publish to Docker Hub

GitHub Actions workflow:

- [`.github/workflows/docker-publish.yml`](./.github/workflows/docker-publish.yml)

Required GitHub repository settings:

- Actions secret: `DOCKERHUB_TOKEN`
- Actions variable: `DOCKERHUB_USERNAME`
- Optional Actions variable: `DOCKERHUB_NAMESPACE`

Image target:

```text
docker.io/<DOCKERHUB_NAMESPACE or DOCKERHUB_USERNAME>/tib-daail-evo1-poc-query-engine
```

Trigger behavior:

- `push` to `main`: build and push
- `push` of tags matching `v*`: build and push
- `pull_request` to `main`: build only
- `workflow_dispatch`: manual run

Docker Hub repository visibility is controlled on Docker Hub.
If the repository does not exist yet, `docker push` creates it with the namespace default repository privacy.
For a public image, either create the repository as public first or set the namespace default repository privacy to public.

## Start the image

Run the container and publish the DuckDB UI on port `4213`:

```bash
docker run --rm -d \
  --name tib-daail-evo1-poc-query-engine \
  -p 4213:4213 \
  -v "$(pwd)/workspace:/workspace" \
  tib-daail-evo1-poc-query-engine:latest
```

What this does:

- stores the DuckDB database file under `/workspace`
- starts the DuckDB UI server inside the container
- exposes the UI on `http://localhost:4213`

## Access the UI

Open this URL in the browser:

```text
http://localhost:4213
```

The UI runs against the database file defined by `DUCKDB_DATABASE`. By default that is:

```text
/workspace/workspace.duckdb
```

## Access the CLI

Open the DuckDB CLI inside the running container:

```bash
docker exec -it tib-daail-evo1-poc-query-engine duckdb
```

Open a shell first:

```bash
docker exec -it tib-daail-evo1-poc-query-engine bash
```

Start the image directly in CLI mode instead of UI mode:

```bash
docker run --rm -it \
  -v "$(pwd)/workspace:/workspace" \
  tib-daail-evo1-poc-query-engine:latest cli /workspace/workspace.duckdb
```

To access the same file-backed database from the CLI, stop the UI container first or use a separate database file. DuckDB does not support general multi-process read/write access to the same database file.

## Local test stack with MinIO and PostgreSQL

For local integration tests, the repository also contains a `docker compose` stack with:

- the DuckDB UI container from this repository
- MinIO as an S3-compatible object store
- PostgreSQL 17
- a small MinIO init step that creates the `vat-smoke-test` bucket
- a small MinIO seed step that uploads `startup/vat_smoke.csv`

These services are for local testing only. They are not part of the actual image.

Start the stack:

```bash
docker compose up -d --build
```

Stop the stack:

```bash
docker compose down
```

Stop the stack and remove volumes:

```bash
docker compose down -v
```

Services exposed locally:

- DuckDB UI: `http://localhost:4213`
- MinIO S3 endpoint: `http://localhost:9000`
- MinIO console: `http://localhost:9001`
- PostgreSQL: `localhost:5432`

Default local credentials:

```text
MinIO root user: minioadmin
MinIO root password: minioadmin
PostgreSQL instance: postgres
PostgreSQL OLTP database: evo1_oltp
PostgreSQL OLAP database: evo1_olap
PostgreSQL user: evo1
PostgreSQL password: evo1
S3 bucket: vat-smoke-test
```

Open a shell in the DuckDB container:

```bash
docker exec -it tib-daail-evo1-poc-query-engine bash
```

Example S3 query flow inside the DuckDB container:

```sql
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='minio:9000';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SELECT * FROM 's3://vat-smoke-test/example.parquet';
```

In the local Compose stack, a startup view named `vat_smoke` is created automatically from:

```text
s3://vat-smoke-test/startup/vat_smoke.csv
```

That view appears in the DuckDB UI under `workspace.s3`.

Example PostgreSQL query flow inside the DuckDB container:

```sql
INSTALL postgres;
LOAD postgres;
ATTACH 'dbname=evo1_oltp host=postgres user=evo1 password=evo1 port=5432' AS pg_oltp (TYPE postgres, READ_ONLY);
ATTACH 'dbname=evo1_olap host=postgres user=evo1 password=evo1 port=5432' AS pg_olap (TYPE postgres);
SELECT * FROM pg_oltp.public.vat_smoke_test_reference;
```

## Environment variable strategy

Configuration model:

- localhost/dev: `docker compose` sets environment variables that point to the local MinIO and PostgreSQL containers
- Kubernetes/OpenShift: a `ConfigMap` provides non-sensitive values and a `Secret` provides credentials, but they are injected into the container under the same environment variable names

That keeps the image environment-agnostic. The image does not need to know whether it is running locally or in the cluster.

Config split:

- `ConfigMap`: hosts, ports, bucket names, region, database names
- `Secret`: usernames, passwords, access keys, secret keys

Suggested variable names:

```text
S3_ENDPOINT
S3_REGION
S3_BUCKET
S3_ACCESS_KEY_ID
S3_SECRET_ACCESS_KEY
S3_URL_STYLE
S3_USE_SSL
S3_STARTUP_VIEW_SCHEMA
S3_STARTUP_VIEWS
PG_HOST
PG_PORT
PG_USER
PG_PASSWORD
PG_OLTP_DATABASE
PG_OLAP_DATABASE
```

Local/dev example:

```text
S3_ENDPOINT=minio:9000
S3_REGION=us-east-1
S3_BUCKET=vat-smoke-test
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_URL_STYLE=path
S3_USE_SSL=false
S3_STARTUP_VIEW_SCHEMA=s3
S3_STARTUP_VIEWS=vat_smoke=csv:s3://vat-smoke-test/startup/vat_smoke.csv
PG_HOST=postgres
PG_PORT=5432
PG_USER=evo1
PG_PASSWORD=evo1
PG_OLTP_DATABASE=evo1_oltp
PG_OLAP_DATABASE=evo1_olap
```

Kubernetes/OpenShift example:

```text
S3_ENDPOINT=your-object-store.example
S3_REGION=eu-central-1
S3_BUCKET=vat-smoke-test
S3_ACCESS_KEY_ID=<from-secret>
S3_SECRET_ACCESS_KEY=<from-secret>
S3_URL_STYLE=path
S3_USE_SSL=true
S3_STARTUP_VIEW_SCHEMA=s3
S3_STARTUP_VIEWS=vat_data=parquet:s3://vat-smoke-test/path/to/data.parquet
PG_HOST=your-postgres-service
PG_PORT=5432
PG_USER=<from-secret>
PG_PASSWORD=<from-secret>
PG_OLTP_DATABASE=evo1_oltp
PG_OLAP_DATABASE=evo1_olap
```

Minimal Kubernetes/OpenShift manifests are provided under [`k8s/`](./k8s):

- [`duckdb-configmap.yaml`](./k8s/duckdb-configmap.yaml) for non-sensitive settings
- [`duckdb-secret.example.yaml`](./k8s/duckdb-secret.example.yaml) as a secret template
- [`duckdb-deployment.yaml`](./k8s/duckdb-deployment.yaml) for the DuckDB pod
- [`duckdb-service.yaml`](./k8s/duckdb-service.yaml) for in-cluster HTTP access on port `4213`

The deployment intentionally uses the same environment variable names as local `docker compose`.
That means:

- local/dev: values come from `compose.yaml`
- cluster: values come from `ConfigMap` and `Secret`

Apply flow:

```bash
kubectl apply -f k8s/duckdb-configmap.yaml
kubectl apply -f k8s/duckdb-secret.yaml
kubectl apply -f k8s/duckdb-deployment.yaml
kubectl apply -f k8s/duckdb-service.yaml
```

Before applying:

- copy `k8s/duckdb-secret.example.yaml` to `k8s/duckdb-secret.yaml`
- replace the placeholder secret values
- replace the placeholder image reference in `k8s/duckdb-deployment.yaml`
- replace `S3_ENDPOINT`, `PG_HOST`, `PG_OLTP_DATABASE`, `PG_OLAP_DATABASE`, and other environment values in `k8s/duckdb-configmap.yaml`
- the deployment currently uses `emptyDir` for `/workspace`, so the local DuckDB database file is ephemeral unless you replace it with a PVC

## Environment Variable Consumption

The container receives all connection values via environment variables. DuckDB does not automatically map arbitrary environment variables into S3 `SET` statements or PostgreSQL `ATTACH` statements.

The runtime behavior is split into two layers:

1. Container configuration
   The container reads environment variables made available by Docker Compose or Kubernetes.

2. DuckDB session setup
   You still need to translate those values into DuckDB commands such as:

   - `SET s3_endpoint=...`
   - `SET s3_access_key_id=...`
   - `ATTACH 'dbname=... host=... user=... password=...' AS ... (TYPE postgres)`

The startup script performs the bootstrap step:

- it creates a temporary DuckDB S3 secret from the `S3_*` environment variables
- it optionally creates startup views in `workspace.<schema>` from `S3_STARTUP_VIEWS`
- it attaches the PostgreSQL source database as `pg_oltp` in read-only mode
- it attaches the PostgreSQL target database as `pg_olap` in read-write mode

PostgreSQL databases are immediately visible in the DuckDB UI under attached databases.
S3 credentials are also ready immediately, but S3 itself does not appear as an attached database in the UI tree because object storage is not a database attachment in DuckDB.
`S3_STARTUP_VIEWS` creates UI-visible views from explicit S3 paths at startup.
A DuckDB database file stored on S3 can be attached directly with `ATTACH 's3://.../file.duckdb' AS ...`.

In the local `docker compose` setup, `pg_oltp` and `pg_olap` use one PostgreSQL instance with two logical databases.
The same pattern can be used in Kubernetes/OpenShift.

`S3_STARTUP_VIEWS` syntax:

```text
view_name=s3://bucket/path/file.parquet
view_name=format:s3://bucket/path/file.csv
```

Multiple views can be separated with semicolons or newlines:

```text
vat_smoke=csv:s3://vat-smoke-test/startup/vat_smoke.csv;
vat_data=parquet:s3://vat-smoke-test/data/vat_data.parquet
```

The same image behavior applies in both environments:

- local: values come from `docker compose`
- cluster: values come from `ConfigMap` and `Secret`

## Useful environment variables

The container supports a few runtime settings:

```text
DUCKDB_DATABASE=/workspace/workspace.duckdb
DUCKDB_UI_PORT=4213
DUCKDB_EXTENSION_DIRECTORY=/workspace/.duckdb/extensions
DUCKDB_EXTRA_EXTENSIONS=
```

Example with a custom database path and extra extensions:

```bash
docker run --rm -d \
  --name tib-daail-evo1-poc-query-engine \
  -p 4213:4213 \
  -e DUCKDB_DATABASE=/workspace/demo.duckdb \
  -e DUCKDB_EXTRA_EXTENSIONS=httpfs,postgres \
  -v "$(pwd)/workspace:/workspace" \
  tib-daail-evo1-poc-query-engine:latest
```

## How the UI is exposed

DuckDB's UI server is started on `localhost:${DUCKDB_UI_PORT}` inside the container. A lightweight TCP proxy then binds the same port on the container's non-loopback interface, so Docker can publish it without changing the browser-visible origin.

## About `.dockerignore`

`compose.yaml`, Kubernetes manifests, SQL init scripts, and helper shell scripts should stay in the repository. They are part of the project, but they are not "inside the image" unless the `Dockerfile` copies them into the image.

What belongs in `.dockerignore` is mostly:

- local runtime state such as `workspace/`
- caches and virtual environments
- large generated files
- local-only test data directories if you create them later

What does not need to be in `.dockerignore` just because it helps local tests:

- `compose.yaml`
- MinIO/PostgreSQL init scripts
- Kubernetes YAML files
- documentation

For the local Compose stack, the bucket uses `vat-smoke-test` instead of `vat_smoke_test`, because MinIO enforces standard S3 bucket naming rules and rejects underscores.

## Notes and limitations

- The DuckDB UI is tied to the running DuckDB instance. If the container stops, the UI stops.
- A second DuckDB process should not be treated as a general shared read/write session against the same `.duckdb` file.
- The browser-visible port must match `DUCKDB_UI_PORT`, because DuckDB UI validates the local browser origin for `/localToken` and `/ddb/*`.
- The UI frontend is fetched by DuckDB from `https://ui.duckdb.org`. The container therefore needs outbound HTTPS access to that host.
- Additional DuckDB extensions installed at runtime may require outbound access to DuckDB's extension download endpoint as well.

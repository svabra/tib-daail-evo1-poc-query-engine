# Kind Validation

Use this path to validate the service-consumption deployment outside RHOS.

## Prerequisites

- A running `kind` cluster with a default `StorageClass`
- `kubectl` configured against that cluster
- `metrics-server` installed if you want node-level CPU and RAM series
- A reachable S3-compatible endpoint for S3 validation
  - simplest option: a local MinIO deployment in the cluster
  - or reuse an external MinIO endpoint reachable from the pod
- A reachable PostgreSQL endpoint for OLTP and OLAP validation
  - in the current local setup, kind reuses the Docker Compose PostgreSQL at `host.docker.internal:5432`
  - in the current local setup, kind also reuses the Docker Compose MinIO at `host.docker.internal:9000`

## Apply

1. Apply the shared config, service account, and service:
   - `kubectl apply -f k8s/duckdb-configmap.yaml`
   - `kubectl apply -f k8s/bdw-serviceaccount.yaml`
   - `kubectl apply -f k8s/bdw-service.yaml`
2. Apply the app storage and node-metrics RBAC:
   - `kubectl apply -f k8s/bdw-storage-pvc.yaml`
   - `kubectl apply -f k8s/bdw-node-reader-clusterrole.yaml`
   - `kubectl apply -f k8s/bdw-node-reader-clusterrolebinding.yaml`
3. Apply the updated deployment:
   - `kubectl apply -f k8s/bdw-deployment.yaml`

Set the CHF rate-card values in the ConfigMap before testing the financial view. The shared annual budget itself is entered through the service-consumption page and persisted on the PVC.

Do not apply `k8s/bdw-route.yaml` in kind. Access the app with `kubectl port-forward`.

## Access

- `kubectl port-forward deployment/evo1-bdw 8000:8000`
- Open `http://127.0.0.1:8000/service-consumption`

## Expected Checks

- The pod starts with `/workspace/service-consumption` mounted from the PVC
- JSONL samples appear under `/workspace/service-consumption/history/...`
- `/service-consumption` loads with the sidebar hidden
- CPU and RAM charts render and refresh from the SSE-backed monitor cadence
- The S3 recent-history chart renders when S3/MinIO is configured
- The node series render only when `metrics-server` is installed and the RBAC objects are applied

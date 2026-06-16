# RHOS YAML Checklist For The Current Workbench

This checklist matches the current `evo1-bdw` deployment manifests in `k8s/`.

## Deployment Requirements

- Use `serviceAccountName: evo1-bdw`.
- Keep `/workspace` as an `emptyDir`; do not mount an application PVC.
- Mount DuckDB spill as its own `emptyDir` at `/workspace/tmp/duckdb-spill`.
- Keep the DuckDB spill `emptyDir.sizeLimit` above `BDW_DUCKDB_MAX_TEMP_DIRECTORY_SIZE`
  so query cache, uploads, and notebook workspace files cannot consume the spill quota.
- Do not set `BDW_APP_STORAGE_PVC_NAME`.
- Set `BDW_SERVICE_CONSUMPTION_CPU_MEMORY_INTERVAL_SECONDS` to `"60"`.
- Set `BDW_SERVICE_CONSUMPTION_S3_INTERVAL_SECONDS` to `"3600"`.
- Set `BDW_SERVICE_CONSUMPTION_RETENTION_HOURS` to `"48"`.
- Set `BDW_SERVICE_CONSUMPTION_NODE_METRICS_ENABLED` to `"false"` unless node RBAC is intentionally enabled.
- Set `BDW_SERVICE_CONSUMPTION_PVC_CAPACITY_ENABLED` to `"false"`.
- Keep the downward API env vars for `POD_NAME`, `POD_NAMESPACE`, `POD_IP`, and `NODE_NAME`.
- Keep S3 and trusted-cert wiring because service-consumption snapshots use hidden S3 state.

## PVC Status

The current deployment intentionally has no PVC dependency.

- Do not apply `k8s/bdw-storage-pvc.yaml`; it has been removed.
- Do not add `persistentVolumeClaim` volumes for service-consumption state.
- PVC provisioned-capacity metrics and filesystem CHF are unavailable by design.
- Service-consumption samples and budget state are held in memory and best-effort snapshotted to:
  `s3://$S3_BUCKET/--bdw-internal--/service-consumption/state.json`

## RBAC

Node RBAC is optional now because node metrics are disabled by default.

- Apply `k8s/bdw-serviceaccount.yaml`.
- Apply `k8s/bdw-node-reader-clusterrole.yaml` and `k8s/bdw-node-reader-clusterrolebinding.yaml` only if node metrics are intentionally re-enabled.
- PVC permissions are not required.

## S3/TLS Wiring

Keep these settings aligned with `Settings.from_env()`:

- `S3_ENDPOINT`
- `S3_BUCKET`
- `S3_USE_SSL`
- `S3_VERIFY_SSL`
- `S3_CA_CERT_FILE`
- `S3_URL_STYLE`
- `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`
- the `bit-ros-trusted-certs` ConfigMap mount

## Recommended Apply Order

```bash
oc apply -f k8s/bdw-configmap.yaml
oc apply -f k8s/bdw-serviceaccount.yaml
oc apply -f k8s/bdw-node-reader-clusterrole.yaml
oc apply -f k8s/bdw-node-reader-clusterrolebinding.yaml
oc apply -f k8s/bdw-deployment.yaml
```

## Post-Deploy Checks

1. The pod starts without a PVC mount.
2. Logs do not contain `Failed to collect PVC capacity`.
3. `/service-consumption` loads.
4. Hidden S3 state appears after the five-minute flush interval when S3 is writable.
5. App CPU/RAM and S3 metrics continue to render.

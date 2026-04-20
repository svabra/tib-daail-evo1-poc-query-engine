# RHOS YAML Checklist For The Current Workbench

This note is for the current workbench codebase and the current RHOS/OpenShift manifests in `k8s/`.

Important context:

- The immediate previous release commit `e1f2b61` only changed the image tag from `0.5.6` to `0.5.7`.
- The YAML changes that matter for the service-consumption page were introduced earlier with the service-consumption rollout and are now represented by the current files in `k8s/`.
- If your RHOS cluster is still using older manifests, apply the YAML changes below before expecting the current service-consumption page to be fully populated.

## 1. Deployment changes you need in `k8s/bdw-deployment.yaml`

The BDW deployment must include all of the following:

- `serviceAccountName: evo1-bdw`
- `BDW_SERVICE_CONSUMPTION_DATA_DIR=/workspace/service-consumption`
- `BDW_SERVICE_CONSUMPTION_CPU_MEMORY_INTERVAL_SECONDS`
- `BDW_SERVICE_CONSUMPTION_S3_INTERVAL_SECONDS`
- `BDW_SERVICE_CONSUMPTION_RETENTION_HOURS`
- `BDW_APP_STORAGE_PVC_NAME=evo1-bdw-storage`
- downward API env vars for:
  - `POD_NAME`
  - `POD_NAMESPACE`
  - `POD_IP`
  - `NODE_NAME`
- a PVC-backed mount at `/workspace/service-consumption`

Current expected shape:

```yaml
spec:
  template:
    spec:
      serviceAccountName: evo1-bdw
      containers:
        - name: bdw
          env:
            - name: BDW_SERVICE_CONSUMPTION_DATA_DIR
              value: /workspace/service-consumption
            - name: BDW_SERVICE_CONSUMPTION_CPU_MEMORY_INTERVAL_SECONDS
              value: "3"
            - name: BDW_SERVICE_CONSUMPTION_S3_INTERVAL_SECONDS
              value: "3600"
            - name: BDW_SERVICE_CONSUMPTION_RETENTION_HOURS
              value: "48"
            - name: BDW_APP_STORAGE_PVC_NAME
              value: evo1-bdw-storage
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          volumeMounts:
            - name: app-storage
              mountPath: /workspace/service-consumption
      volumes:
        - name: app-storage
          persistentVolumeClaim:
            claimName: evo1-bdw-storage
```

Why this matters:

- `BDW_SERVICE_CONSUMPTION_DATA_DIR` is where the app writes history JSONL files and the shared annual budget.
- `BDW_APP_STORAGE_PVC_NAME` and `POD_NAMESPACE` are used to query PVC capacity from the Kubernetes API.
- `NODE_NAME` is used to query node capacity and `metrics.k8s.io` usage for the "Current node" series.

## 2. PVC you need in `k8s/bdw-storage-pvc.yaml`

The current deployment expects a PVC named `evo1-bdw-storage`.

Current manifest:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: evo1-bdw-storage
  namespace: daai-brs-d
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

What to adjust on RHOS:

- Keep the PVC name aligned with:
  - `spec.volumes[].persistentVolumeClaim.claimName`
  - `BDW_APP_STORAGE_PVC_NAME`
- If your cluster does not have a suitable default `StorageClass`, add `spec.storageClassName`.
- If you already have an older PVC name such as `evo1-bdw-service-consumption`, either:
  - rename the deployment references to match the old PVC name, or
  - create/migrate to `evo1-bdw-storage` and update the env var accordingly.

Why this matters:

- Without a writable mounted PVC, the page may still load, but history and budget persistence will not survive pod restarts.
- Without the correct PVC name, provisioned-capacity lookup fails and the PVC cost component stays incomplete.

## 3. RBAC you need for node and PVC reads

Apply these objects:

- `k8s/bdw-serviceaccount.yaml`
- `k8s/bdw-node-reader-clusterrole.yaml`
- `k8s/bdw-node-reader-clusterrolebinding.yaml`

Current required permissions:

```yaml
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["metrics.k8s.io"]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch"]
```

Why this matters:

- `nodes` and `metrics.k8s.io/nodes` are needed for node CPU and RAM usage plus node capacity.
- `persistentvolumeclaims` is needed for PVC provisioned capacity.
- The binding subject namespace must match the namespace where BDW runs.

If you deploy into a namespace other than `daai-brs-d`, change:

- `metadata.namespace` in the ServiceAccount and PVC
- the `subjects[].namespace` in the ClusterRoleBinding
- the Deployment namespace

## 4. ConfigMap values you need in `k8s/duckdb-configmap.yaml`

The service-consumption financial view depends on the following cost-rate keys:

```yaml
BDW_SERVICE_CONSUMPTION_COST_NODE_CHF_PER_HOUR: ""
BDW_SERVICE_CONSUMPTION_COST_APP_CHF_PER_MONTH: ""
BDW_SERVICE_CONSUMPTION_COST_S3_CHF_PER_GB_MONTH: "0.03954"
BDW_SERVICE_CONSUMPTION_COST_PV_CHF_PER_GB_MONTH: ""
BDW_SERVICE_CONSUMPTION_COST_PG_CHF_PER_GB_MONTH: ""
BDW_SERVICE_CONSUMPTION_COST_CPU_WEIGHT: "0.5"
BDW_SERVICE_CONSUMPTION_COST_RAM_WEIGHT: "0.5"
```

What is actually important right now:

- `BDW_SERVICE_CONSUMPTION_COST_NODE_CHF_PER_HOUR`
  - required if you want computed CHF from node CPU/RAM usage
- `BDW_SERVICE_CONSUMPTION_COST_S3_CHF_PER_GB_MONTH`
  - required if you want S3 CHF estimates
- `BDW_SERVICE_CONSUMPTION_COST_PV_CHF_PER_GB_MONTH`
  - required if you want PVC/filesystem CHF estimates
- `BDW_SERVICE_CONSUMPTION_COST_CPU_WEIGHT`
  - controls how node cost is split between CPU and RAM
- `BDW_SERVICE_CONSUMPTION_COST_RAM_WEIGHT`
  - controls how node cost is split between CPU and RAM

Important current-code nuance:

- `BDW_SERVICE_CONSUMPTION_COST_APP_CHF_PER_MONTH` exists in the ConfigMap schema but is not currently used by the backend.
- `BDW_SERVICE_CONSUMPTION_COST_PG_CHF_PER_GB_MONTH` also exists in the ConfigMap schema but is not currently used by the backend.
- The app cost and PG placeholder cost are currently hard-coded in the Python backend, so changing those two ConfigMap keys does not change the displayed values today.

## 5. Existing S3/TLS cluster wiring you still need to keep

Do not remove the current S3 and trusted-cert wiring. The service-consumption S3 usage chart depends on the same S3 settings the app already uses:

- `S3_ENDPOINT`
- `S3_BUCKET`
- `S3_USE_SSL`
- `S3_VERIFY_SSL`
- `S3_CA_CERT_FILE`
- `S3_URL_STYLE` when required by ECS
- `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` from the Secret
- the mounted `bit-ros-trusted-certs` ConfigMap

If S3 credentials or TLS trust are broken:

- the app may still start
- the S3 usage chart will show unavailable status
- the S3 CHF estimate will remain unavailable

## 6. What is required vs optional

### Required for the current version to start and keep service-consumption data

- a writable `BDW_SERVICE_CONSUMPTION_DATA_DIR`
- a matching PVC mount and PVC name if you want persistence across pod restarts
- the existing S3 Secret and trusted-cert mount if you want S3 integration to work

### Required for a fully populated service-consumption dashboard

- `POD_NAMESPACE` and `NODE_NAME`
- the ServiceAccount and RBAC objects
- `metrics.k8s.io` access for node usage
- `BDW_SERVICE_CONSUMPTION_COST_NODE_CHF_PER_HOUR`
- `BDW_SERVICE_CONSUMPTION_COST_S3_CHF_PER_GB_MONTH`
- `BDW_SERVICE_CONSUMPTION_COST_PV_CHF_PER_GB_MONTH`
- `BDW_APP_STORAGE_PVC_NAME` matching a real PVC

### Optional tuning only

- `BDW_SERVICE_CONSUMPTION_CPU_MEMORY_INTERVAL_SECONDS`
- `BDW_SERVICE_CONSUMPTION_S3_INTERVAL_SECONDS`
- `BDW_SERVICE_CONSUMPTION_RETENTION_HOURS`

## 7. Recommended RHOS apply order

```bash
oc apply -f k8s/duckdb-configmap.yaml
oc apply -f k8s/bdw-serviceaccount.yaml
oc apply -f k8s/bdw-storage-pvc.yaml
oc apply -f k8s/bdw-node-reader-clusterrole.yaml
oc apply -f k8s/bdw-node-reader-clusterrolebinding.yaml
oc apply -f k8s/bdw-deployment.yaml
```

## 8. Quick post-deploy checks

After rollout, verify:

1. The pod starts without `Service consumption monitor failed to start`.
2. `/workspace/service-consumption` exists inside the pod and is writable.
3. History files appear under `/workspace/service-consumption/history/...`.
4. `/service-consumption` loads.
5. The "Current node" series is present.
6. The PVC chart shows provisioned capacity, not just local filesystem usage.
7. The financial section stops saying that cost inputs or metrics are unavailable once the relevant ConfigMap rates are set.

export function s3ExplorerPath(bucket, prefix = "") {
  const normalizedBucket = String(bucket || "").trim();
  const parts = String(prefix || "")
    .split("/")
    .map((segment) => String(segment || "").trim())
    .filter(Boolean);
  const normalizedPrefix = parts.length ? `${parts.join("/")}/` : "";
  return normalizedBucket ? `s3://${normalizedBucket}/${normalizedPrefix}` : "";
}

function normalizeS3ExplorerEntry(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }

  return {
    entryKind: String(entry.entryKind || "").trim(),
    name: String(entry.name || "").trim(),
    bucket: String(entry.bucket || "").trim(),
    prefix: String(entry.prefix || "").trim(),
    path: String(entry.path || "").trim(),
    fileFormat: String(entry.fileFormat || "").trim(),
    sizeBytes: Number.isFinite(Number(entry.sizeBytes)) ? Number(entry.sizeBytes) : 0,
    hasChildren: entry.hasChildren === true,
    selectable: entry.selectable === true,
  };
}

function normalizeS3ExplorerSnapshot(snapshot) {
  const normalized = snapshot && typeof snapshot === "object" ? snapshot : {};
  return {
    bucket: String(normalized.bucket || "").trim(),
    prefix: String(normalized.prefix || "").trim(),
    path: String(normalized.path || "").trim(),
    entries: Array.isArray(normalized.entries)
      ? normalized.entries.map((entry) => normalizeS3ExplorerEntry(entry)).filter(Boolean)
      : [],
    breadcrumbs: Array.isArray(normalized.breadcrumbs) ? normalized.breadcrumbs : [],
    canCreateBucket: normalized.canCreateBucket !== false,
    canCreateFolder: normalized.canCreateFolder === true,
    emptyMessage: String(normalized.emptyMessage || "").trim(),
  };
}

export function createS3ExplorerLoader({
  fetchJsonOrThrow,
  getResultExportTreeRoot,
  nodeRequests,
  renderChildrenMarkup,
  selectResultExportLocation,
  syncResultExportSelectionState,
  s3ExplorerNodeKey,
}) {
  async function loadS3ExplorerSnapshot(bucket = "", prefix = "") {
    const params = new URLSearchParams();
    if (bucket) {
      params.set("bucket", bucket);
    }
    if (prefix) {
      params.set("prefix", prefix);
    }
    const suffix = params.toString();
    const snapshot = await fetchJsonOrThrow(`/api/s3/explorer${suffix ? `?${suffix}` : ""}`, {
      headers: {
        Accept: "application/json",
      },
    });
    return normalizeS3ExplorerSnapshot(snapshot);
  }

  function s3ExplorerNodeForLocation(kind, bucket, prefix = "") {
    const normalizedKind = String(kind || "").trim();
    const normalizedBucket = String(bucket || "").trim();
    const normalizedPrefix = String(prefix || "").trim();
    if (!normalizedKind || !normalizedBucket) {
      return null;
    }

    return document.querySelector(
      `[data-s3-explorer-node][data-s3-explorer-kind="${CSS.escape(normalizedKind)}"][data-s3-explorer-bucket="${CSS.escape(
        normalizedBucket
      )}"][data-s3-explorer-prefix="${CSS.escape(normalizedPrefix)}"]`
    );
  }

  async function loadS3ExplorerNode(node, { force = false } = {}) {
    if (!(node instanceof HTMLElement)) {
      return null;
    }

    const bucket = node.dataset.s3ExplorerBucket || "";
    const prefix = node.dataset.s3ExplorerPrefix || "";
    const requestKey = s3ExplorerNodeKey(node.dataset.s3ExplorerKind, bucket, prefix);
    if (node.dataset.s3ExplorerLoaded === "true" && !force) {
      return null;
    }
    if (nodeRequests.has(requestKey)) {
      return nodeRequests.get(requestKey);
    }

    const childrenRoot = node.querySelector("[data-s3-explorer-children]");
    if (childrenRoot) {
      childrenRoot.innerHTML = '<p class="s3-explorer-empty">Loading...</p>';
    }

    const request = loadS3ExplorerSnapshot(bucket, prefix)
      .then((snapshot) => {
        if (childrenRoot) {
          childrenRoot.innerHTML = renderChildrenMarkup(snapshot);
        }
        node.dataset.s3ExplorerLoaded = "true";
        syncResultExportSelectionState();
        return snapshot;
      })
      .finally(() => {
        nodeRequests.delete(requestKey);
      });

    nodeRequests.set(requestKey, request);
    return request;
  }

  async function revealS3ExplorerLocation(bucket, prefix = "") {
    const normalizedBucket = String(bucket || "").trim();
    const normalizedPrefix = String(prefix || "").trim();
    if (!normalizedBucket) {
      selectResultExportLocation("", "");
      return true;
    }

    const bucketNode = s3ExplorerNodeForLocation("bucket", normalizedBucket, "");
    if (!(bucketNode instanceof HTMLElement)) {
      return false;
    }

    selectResultExportLocation(normalizedBucket, "");
    bucketNode.open = true;
    await loadS3ExplorerNode(bucketNode);

    if (!normalizedPrefix) {
      syncResultExportSelectionState();
      return true;
    }

    let currentNode = bucketNode;
    let currentPrefix = "";
    let fullyRevealed = true;
    for (const segment of normalizedPrefix.split("/").filter(Boolean)) {
      currentPrefix = currentPrefix ? `${currentPrefix}${segment}/` : `${segment}/`;
      currentNode.open = true;
      await loadS3ExplorerNode(currentNode);
      const nextNode =
        currentNode.querySelector(
          `[data-s3-explorer-node][data-s3-explorer-kind="folder"][data-s3-explorer-bucket="${CSS.escape(
            normalizedBucket
          )}"][data-s3-explorer-prefix="${CSS.escape(currentPrefix)}"]`
        ) ?? null;
      if (!(nextNode instanceof HTMLElement)) {
        fullyRevealed = false;
        break;
      }
      currentNode = nextNode;
      selectResultExportLocation(normalizedBucket, currentPrefix);
    }

    syncResultExportSelectionState();
    return fullyRevealed;
  }

  async function loadS3ExplorerRoot({ preferredBucket = "", preferredPrefix = "" } = {}) {
    const treeRoot = getResultExportTreeRoot();
    if (!treeRoot) {
      return null;
    }

    treeRoot.innerHTML = '<p class="s3-explorer-empty">Loading buckets...</p>';
    const snapshot = await loadS3ExplorerSnapshot("", "");
    treeRoot.innerHTML = renderChildrenMarkup(snapshot);

    if (preferredBucket) {
      const revealed = await revealS3ExplorerLocation(preferredBucket, preferredPrefix);
      if (!revealed) {
        selectResultExportLocation("", "");
      }
    } else if (snapshot.entries.length === 1 && snapshot.entries[0].entryKind === "bucket") {
      await revealS3ExplorerLocation(snapshot.entries[0].bucket, "");
    } else {
      selectResultExportLocation("", "");
    }

    return snapshot;
  }

  return {
    loadS3ExplorerNode,
    loadS3ExplorerRoot,
    revealS3ExplorerLocation,
    s3ExplorerNodeForLocation,
  };
}
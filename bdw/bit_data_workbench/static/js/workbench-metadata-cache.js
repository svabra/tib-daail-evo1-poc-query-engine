const pendingMetadataRequests = new Map();

function parseEmbeddedJson(node, fallback) {
  try {
    return JSON.parse(node?.textContent || "") ?? fallback;
  } catch (_error) {
    return fallback;
  }
}

export async function ensureEmbeddedWorkbenchMetadata(scriptId, { signal = null } = {}) {
  const node = document.getElementById(scriptId);
  if (!(node instanceof HTMLScriptElement)) {
    return null;
  }
  if (node.dataset.deferred !== "true") {
    return parseEmbeddedJson(node, null);
  }

  const url = String(node.dataset.deferredUrl || "").trim();
  if (!url) {
    return parseEmbeddedJson(node, null);
  }
  if (!pendingMetadataRequests.has(scriptId)) {
    const request = window
      .fetch(url, { headers: { Accept: "application/json" }, signal })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Failed to load deferred workbench metadata: ${response.status}`);
        }
        return response.json();
      })
      .then((payload) => {
        node.textContent = JSON.stringify(payload);
        node.dataset.deferred = "false";
        return payload;
      })
      .finally(() => pendingMetadataRequests.delete(scriptId));
    pendingMetadataRequests.set(scriptId, request);
  }
  return pendingMetadataRequests.get(scriptId);
}

export function ensureNotebookEditorMetadata(options = {}) {
  return Promise.all([
    ensureEmbeddedWorkbenchMetadata("source-options", options),
    ensureEmbeddedWorkbenchMetadata("sql-schema", options),
  ]);
}

export function ensureFeatureReleaseNotes(options = {}) {
  return ensureEmbeddedWorkbenchMetadata("feature-release-notes", options);
}

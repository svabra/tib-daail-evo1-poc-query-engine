function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function createLocalWorkspaceQueryBridge(helpers) {
  const {
    getLocalWorkspaceExport,
    isLocalWorkspaceRelation,
    localWorkspaceEntryIdFromRelation,
    localWorkspaceRelation,
    normalizeSourceObjectFields,
    workbenchClientId,
  } = helpers;

  const syncCache = new Map();

  function localWorkspaceEntryIdFromSourceObject(sourceObjectRoot) {
    return String(sourceObjectRoot?.dataset.localWorkspaceEntryId || "").trim();
  }

  function entrySignature(entry) {
    return JSON.stringify({
      updatedAt: String(entry?.updatedAt || entry?.createdAt || "").trim(),
      sizeBytes: Number(entry?.sizeBytes || 0),
      exportFormat: String(entry?.exportFormat || "").trim().toLowerCase(),
      mimeType: String(entry?.mimeType || "").trim().toLowerCase(),
      csvDelimiter: String(entry?.csvDelimiter || "").trim(),
      csvHasHeader: entry?.csvHasHeader !== false,
    });
  }

  function resolvedFileName(entry) {
    return String(entry?.fileName || "").trim() || "local-workspace-file";
  }

  async function syncLocalWorkspaceEntry(entryId, { force = false } = {}) {
    const normalizedEntryId = String(entryId || "").trim();
    if (!normalizedEntryId) {
      throw new Error("The Local Workspace file is missing its entry id.");
    }

    const entry = await getLocalWorkspaceExport(normalizedEntryId);
    if (!entry || !(entry.blob instanceof Blob)) {
      throw new Error("The Local Workspace file is not available in this browser anymore.");
    }

    const signature = entrySignature(entry);
    const cached = syncCache.get(normalizedEntryId);
    if (!force && cached?.signature === signature) {
      return cached.result;
    }

    const formData = new FormData();
    formData.set("entryId", normalizedEntryId);
    formData.set("relation", localWorkspaceRelation(normalizedEntryId));
    formData.set("fileName", resolvedFileName(entry));
    formData.set("exportFormat", String(entry.exportFormat || "").trim().toLowerCase());
    formData.set("mimeType", String(entry.mimeType || "").trim());
    formData.set("csvDelimiter", String(entry.csvDelimiter || "").trim());
    formData.set("csvHasHeader", entry.csvHasHeader === false ? "false" : "true");
    formData.append("file", entry.blob, resolvedFileName(entry));

    const response = await window.fetch("/api/local-workspace/query-sources/sync", {
      method: "POST",
      body: formData,
      headers: {
        Accept: "application/json",
        "X-Workbench-Client-Id": workbenchClientId(),
      },
    });
    if (!response.ok) {
      let message = "The Local Workspace file could not be prepared for querying.";
      try {
        const payload = await response.json();
        message = payload?.detail || message;
      } catch (_error) {
        // Ignore invalid JSON bodies.
      }
      throw new Error(message);
    }

    const payload = await response.json();
    const result = {
      entryId: normalizedEntryId,
      logicalRelation: String(payload?.logicalRelation || localWorkspaceRelation(normalizedEntryId)).trim(),
      relation: String(payload?.relation || "").trim(),
      fields: normalizeSourceObjectFields(payload?.fields || []),
    };
    if (!result.relation) {
      throw new Error("The Local Workspace file could not be mapped to a query relation.");
    }
    syncCache.set(normalizedEntryId, {
      signature,
      result,
    });
    return result;
  }

  async function loadLocalWorkspaceSourceFields(sourceObjectRoot) {
    const entryId = localWorkspaceEntryIdFromSourceObject(sourceObjectRoot);
    if (!entryId) {
      throw new Error("The Local Workspace file is missing its entry id.");
    }

    const result = await syncLocalWorkspaceEntry(entryId);
    return result.fields;
  }

  function localWorkspaceRelationsInText(sqlText = "") {
    const matches = new Set();
    const sourceText = String(sqlText || "");
    const pattern = /workspace\.local\.saved_results\.[A-Za-z0-9_-]+/g;
    let match = pattern.exec(sourceText);
    while (match) {
      const logicalRelation = String(match[0] || "").trim();
      if (isLocalWorkspaceRelation(logicalRelation)) {
        matches.add(logicalRelation);
      }
      match = pattern.exec(sourceText);
    }
    return Array.from(matches);
  }

  async function prepareQuerySql(sqlText = "") {
    let rewrittenSql = String(sqlText || "");
    const synchronizedSources = [];

    for (const logicalRelation of localWorkspaceRelationsInText(rewrittenSql)) {
      const entryId = localWorkspaceEntryIdFromRelation(logicalRelation);
      if (!entryId) {
        continue;
      }
      const result = await syncLocalWorkspaceEntry(entryId);
      synchronizedSources.push(result);
      if (result.relation && result.relation !== logicalRelation) {
        rewrittenSql = rewrittenSql.replace(
          new RegExp(escapeRegex(logicalRelation), "g"),
          result.relation
        );
      }
    }

    return {
      sql: rewrittenSql,
      synchronizedSources,
    };
  }

  async function preparePythonExecution(codeText = "") {
    const sourceText = String(codeText || "");
    const synchronizedSources = [];
    const localRelationMap = {};

    for (const logicalRelation of localWorkspaceRelationsInText(sourceText)) {
      const entryId = localWorkspaceEntryIdFromRelation(logicalRelation);
      if (!entryId) {
        continue;
      }
      const result = await syncLocalWorkspaceEntry(entryId);
      synchronizedSources.push(result);
      if (result.relation && result.relation !== logicalRelation) {
        localRelationMap[logicalRelation] = result.relation;
      }
    }

    return {
      code: sourceText,
      synchronizedSources,
      localRelationMap,
    };
  }

  function clearLocalWorkspaceQuerySourceCache(entryId = "") {
    const normalizedEntryId = String(entryId || "").trim();
    if (!normalizedEntryId) {
      syncCache.clear();
      return;
    }
    syncCache.delete(normalizedEntryId);
  }

  async function deleteLocalWorkspaceQuerySource(entryId = "") {
    const normalizedEntryId = String(entryId || "").trim();
    if (!normalizedEntryId) {
      return;
    }

    clearLocalWorkspaceQuerySourceCache(normalizedEntryId);
    const response = await window.fetch("/api/local-workspace/query-sources/delete", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "X-Workbench-Client-Id": workbenchClientId(),
      },
      body: JSON.stringify({
        entryId: normalizedEntryId,
      }),
    });
    if (!response.ok) {
      let message = "The Local Workspace query source could not be removed.";
      try {
        const payload = await response.json();
        message = payload?.detail || message;
      } catch (_error) {
        // Ignore invalid JSON bodies.
      }
      throw new Error(message);
    }
  }

  async function clearLocalWorkspaceQuerySources() {
    clearLocalWorkspaceQuerySourceCache();
    const response = await window.fetch("/api/local-workspace/query-sources/clear", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "X-Workbench-Client-Id": workbenchClientId(),
      },
    });
    if (!response.ok) {
      let message = "The Local Workspace query sources could not be cleared.";
      try {
        const payload = await response.json();
        message = payload?.detail || message;
      } catch (_error) {
        // Ignore invalid JSON bodies.
      }
      throw new Error(message);
    }
  }

  async function moveLocalWorkspaceEntryToS3(
    entryId,
    { bucket = "", prefix = "", fileName = "" } = {}
  ) {
    return transferLocalWorkspaceEntryToS3(
      "/api/local-workspace/exports/move-to-s3",
      "The Local Workspace file could not be moved to Shared Workspace.",
      entryId,
      {
        bucket,
        prefix,
        fileName,
      }
    );
  }

  async function copyLocalWorkspaceEntryToS3(
    entryId,
    { bucket = "", prefix = "", fileName = "" } = {}
  ) {
    return transferLocalWorkspaceEntryToS3(
      "/api/local-workspace/exports/copy-to-s3",
      "The Local Workspace file could not be copied to Shared Workspace.",
      entryId,
      {
        bucket,
        prefix,
        fileName,
      }
    );
  }

  async function transferLocalWorkspaceEntryToS3(
    endpoint,
    fallbackError,
    entryId,
    { bucket = "", prefix = "", fileName = "" } = {}
  ) {
    const normalizedEntryId = String(entryId || "").trim();
    if (!normalizedEntryId) {
      throw new Error("The Local Workspace file is missing its entry id.");
    }

    const entry = await getLocalWorkspaceExport(normalizedEntryId);
    if (!entry || !(entry.blob instanceof Blob)) {
      throw new Error("The Local Workspace file is not available in this browser anymore.");
    }

    const resolvedName = String(fileName || "").trim() || resolvedFileName(entry);
    const formData = new FormData();
    formData.set("entryId", normalizedEntryId);
    formData.set("bucket", String(bucket || "").trim());
    formData.set("prefix", String(prefix || "").trim());
    formData.set("fileName", resolvedName);
    formData.set("mimeType", String(entry.mimeType || "").trim());
    formData.append("file", entry.blob, resolvedName);

    const response = await window.fetch(endpoint, {
      method: "POST",
      body: formData,
      headers: {
        Accept: "application/json",
        "X-Workbench-Client-Id": workbenchClientId(),
      },
    });
    if (!response.ok) {
      let message = fallbackError;
      try {
        const payload = await response.json();
        message = payload?.detail || message;
      } catch (_error) {
        // Ignore invalid JSON bodies.
      }
      throw new Error(message);
    }

    clearLocalWorkspaceQuerySourceCache(normalizedEntryId);
    return response.json();
  }

  return {
    clearLocalWorkspaceQuerySourceCache,
    clearLocalWorkspaceQuerySources,
    copyLocalWorkspaceEntryToS3,
    deleteLocalWorkspaceQuerySource,
    loadLocalWorkspaceSourceFields,
    localWorkspaceEntryIdFromSourceObject,
    localWorkspaceRelationsInText,
    moveLocalWorkspaceEntryToS3,
    preparePythonExecution,
    prepareQuerySql,
    syncLocalWorkspaceEntry,
  };
}

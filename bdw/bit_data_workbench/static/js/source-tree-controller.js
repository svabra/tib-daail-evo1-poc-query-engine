export function createSourceTreeController(helpers) {
  const {
    allLocalWorkspaceFolderPaths,
    captureSidebarState,
    clearSourceObjectFieldCacheForRelations,
    currentActiveNotebookId,
    currentWorkspaceCanEdit,
    currentWorkspaceMode,
    dataSourcesSection,
    getActiveSourceObjectRelation,
    getRenderSidebarSourceOperationStatus,
    getRenderSourceInspectorMarkup,
    getRestoreSelectedSourceObject,
    getSetSelectedSourceObjectState,
    listLocalWorkspaceExports,
    loadNotebookWorkspace,
    localWorkspaceCatalogSourceId,
    localWorkspaceRelationPrefix,
    localWorkspaceSchemaKey,
    localWorkspaceSchemaMarkup,
    normalizeLocalWorkspaceFolderPath,
    restoreSidebarState,
    showMessageDialog,
    syncOpenLocalWorkspaceMoveDialog,
    syncOpenLocalWorkspaceSaveDialog,
    workspaceNotebookId,
  } = helpers;

  let dataSourceEventsStateVersion = null;
  let dataSourceEventsLatestEventId = null;
  let pendingDataSourceSidebarRefreshHandle = null;
  let dataSourceSidebarRefreshPromise = null;
  let dataSourceSidebarRefreshQueued = false;
  const pendingSourceCatalogBlinks = new Set();
  const sourceConnectionRequests = new Set();

  function escapeSelectorValue(value) {
    return typeof window.CSS?.escape === "function" ? window.CSS.escape(String(value ?? "")) : String(value ?? "");
  }

  function sourceCatalogSelector(sourceId) {
    return `[data-source-catalog-source-id="${escapeSelectorValue(sourceId)}"]`;
  }

  function sourceCatalogNode(sourceId) {
    return document.querySelector(sourceCatalogSelector(sourceId));
  }

  function sourceSchemaBucketNode(bucketName) {
    const normalizedBucketName = String(bucketName ?? "").trim();
    if (!normalizedBucketName) {
      return null;
    }
    return document.querySelector(
      `[data-source-schema][data-source-bucket="${escapeSelectorValue(normalizedBucketName)}"]`
    );
  }

  function localWorkspaceSchemaNode() {
    return document.querySelector(
      `[data-source-schema][data-source-schema-key="${escapeSelectorValue(localWorkspaceSchemaKey)}"]`
    );
  }

  function localWorkspaceFolderNode(folderPath = "") {
    const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
    if (!normalizedFolderPath) {
      return null;
    }

    return document.querySelector(
      `[data-local-workspace-folder-node][data-local-workspace-folder-path="${escapeSelectorValue(normalizedFolderPath)}"]`
    );
  }

  function localWorkspaceEntryNode(entryId) {
    const normalizedEntryId = String(entryId || "").trim();
    if (!normalizedEntryId) {
      return null;
    }

    return document.querySelector(
      `[data-local-workspace-entry-id="${escapeSelectorValue(normalizedEntryId)}"]`
    );
  }

  function ensureLocalWorkspaceCatalogOrder() {
    const sourceTree = document.querySelector(".source-tree");
    const localWorkspaceCatalog = sourceCatalogNode(localWorkspaceCatalogSourceId);
    if (!(sourceTree instanceof Element) || !(localWorkspaceCatalog instanceof Element)) {
      return;
    }

    if (sourceTree.firstElementChild !== localWorkspaceCatalog) {
      sourceTree.prepend(localWorkspaceCatalog);
    }
  }

  function localWorkspaceFolderNodes() {
    return Array.from(document.querySelectorAll("[data-local-workspace-folder-node]"));
  }

  function localWorkspaceOpenFolderPaths() {
    return new Set(
      localWorkspaceFolderNodes()
        .filter((node) => node instanceof HTMLDetailsElement && node.open)
        .map((node) => normalizeLocalWorkspaceFolderPath(node.dataset.localWorkspaceFolderPath || ""))
        .filter(Boolean)
    );
  }

  async function renderLocalWorkspaceSidebarEntries() {
    ensureLocalWorkspaceCatalogOrder();

    const localWorkspaceCatalog = sourceCatalogNode(localWorkspaceCatalogSourceId);
    if (!(localWorkspaceCatalog instanceof Element)) {
      return;
    }

    const entries = await listLocalWorkspaceExports();
    const existingSchema = localWorkspaceSchemaNode();
    const schemaOpen = existingSchema instanceof HTMLDetailsElement ? existingSchema.open : true;
    const openFolderPaths = localWorkspaceOpenFolderPaths();
    const folderPaths = allLocalWorkspaceFolderPaths(entries.map((entry) => entry.folderPath));

    if (!entries.length && !folderPaths.filter(Boolean).length) {
      existingSchema?.remove();
      if (getActiveSourceObjectRelation()?.startsWith(localWorkspaceRelationPrefix)) {
        getSetSelectedSourceObjectState()?.(null);
        getRenderSourceInspectorMarkup()?.("", true);
      }
      await syncOpenLocalWorkspaceSaveDialog();
      await syncOpenLocalWorkspaceMoveDialog();
      return;
    }

    const markup = localWorkspaceSchemaMarkup(entries, folderPaths, schemaOpen, openFolderPaths);
    if (existingSchema instanceof Element) {
      existingSchema.outerHTML = markup;
    } else {
      localWorkspaceCatalog.insertAdjacentHTML("beforeend", markup);
    }
    await syncOpenLocalWorkspaceSaveDialog();
    await syncOpenLocalWorkspaceMoveDialog();
  }

  function syncSourceConnectionControls(catalogNode, status) {
    if (!(catalogNode instanceof Element)) {
      return;
    }

    const meta = catalogNode.querySelector(":scope > summary [data-source-catalog-meta]");
    if (!(meta instanceof Element)) {
      return;
    }

    const state = status?.state || "unknown";
    meta.dataset.sourceState = state;

    meta.querySelectorAll("[data-source-connect], [data-source-disconnect]").forEach((button) => {
      const sourceId = catalogNode.dataset.sourceCatalogSourceId?.trim() || catalogNode.dataset.sourceCatalogName?.trim() || "";
      const sourceLabel = catalogNode.dataset.sourceCatalogName?.trim() || sourceId;
      const isPending = sourceConnectionRequests.has(sourceId);
      button.disabled = isPending;
      button.hidden = false;
      if (button instanceof HTMLButtonElement) {
        const isConnect = button.hasAttribute("data-source-connect");
        button.title = isConnect ? `Connect ${sourceLabel}` : `Disconnect ${sourceLabel}`;
        button.setAttribute("aria-label", button.title);
      }
    });
  }

  function upsertSourceConnectionStatus(catalogNode, status) {
    if (!(catalogNode instanceof Element)) {
      return;
    }

    const summary = catalogNode.querySelector(":scope > summary");
    if (!(summary instanceof Element)) {
      return;
    }

    const meta =
      summary.querySelector(":scope > [data-source-catalog-meta]") ||
      summary.querySelector(":scope > .source-catalog-meta");
    let badge = meta?.querySelector(":scope > .source-connection-status") || null;
    if (!status?.label) {
      badge?.remove();
      return;
    }

    if (!(badge instanceof Element)) {
      badge = document.createElement("span");
      badge.className = "source-connection-status";
      badge.innerHTML = `
        <svg class="source-connection-icon" viewBox="0 0 16 16" aria-hidden="true">
          <path d="M5.5 4.2H4.2a1.8 1.8 0 0 0 0 3.6h1.3"></path>
          <path d="M10.5 4.2h1.3a1.8 1.8 0 0 1 0 3.6h-1.3"></path>
          <path d="M5.9 8.4l4.2-4.2"></path>
          <path d="M5.9 7.6l4.2 4.2"></path>
        </svg>
      `;
      (meta || summary).appendChild(badge);
    }

    badge.className = `source-connection-status source-connection-status-${status.state || "unknown"}`;
    badge.setAttribute("title", status.detail || status.label);
    badge.setAttribute("aria-label", status.label);
    syncSourceConnectionControls(catalogNode, status);
  }

  function blinkSourceCatalog(sourceId) {
    const summary = sourceCatalogNode(sourceId)?.querySelector(":scope > summary");
    if (!(summary instanceof Element)) {
      pendingSourceCatalogBlinks.add(sourceId);
      return;
    }

    pendingSourceCatalogBlinks.delete(sourceId);
    summary.classList.remove("is-source-updated");
    void summary.offsetWidth;
    summary.classList.add("is-source-updated");

    window.setTimeout(() => {
      summary.classList.remove("is-source-updated");
    }, 2400);
  }

  function replayPendingSourceCatalogBlinks() {
    if (!pendingSourceCatalogBlinks.size) {
      return;
    }

    Array.from(pendingSourceCatalogBlinks).forEach((sourceId) => {
      const summary = sourceCatalogNode(sourceId)?.querySelector(":scope > summary");
      if (!(summary instanceof Element)) {
        return;
      }
      blinkSourceCatalog(sourceId);
    });
  }

  function blinkSourceSchemaBucket(bucketName) {
    const summary = sourceSchemaBucketNode(bucketName)?.querySelector(":scope > summary");
    if (!(summary instanceof Element)) {
      return;
    }

    summary.classList.remove("is-source-updated");
    void summary.offsetWidth;
    summary.classList.add("is-source-updated");
    window.setTimeout(() => {
      summary.classList.remove("is-source-updated");
    }, 2400);
  }

  async function revealSidebarS3Bucket(bucketName) {
    const normalizedBucketName = String(bucketName ?? "").trim();
    if (!normalizedBucketName) {
      return;
    }

    const sourcesRoot = dataSourcesSection();
    if (sourcesRoot instanceof HTMLDetailsElement) {
      sourcesRoot.open = true;
    }

    const workspaceCatalog = sourceCatalogNode("workspace.s3");
    if (workspaceCatalog instanceof HTMLDetailsElement) {
      workspaceCatalog.open = true;
    }

    const schemaNode = sourceSchemaBucketNode(normalizedBucketName);
    if (!(schemaNode instanceof HTMLDetailsElement)) {
      return;
    }

    schemaNode.open = true;
    blinkSourceCatalog("workspace.s3");
    blinkSourceSchemaBucket(normalizedBucketName);
    schemaNode.scrollIntoView({ block: "nearest" });
  }

  async function setDataSourceConnectionState(sourceId, action) {
    const normalizedSourceId = String(sourceId ?? "").trim();
    const normalizedAction = String(action ?? "").trim();
    if (!normalizedSourceId || !["connect", "disconnect"].includes(normalizedAction)) {
      return;
    }

    sourceConnectionRequests.add(normalizedSourceId);
    const catalogNode = sourceCatalogNode(normalizedSourceId);
    syncSourceConnectionControls(catalogNode, {
      state: catalogNode?.querySelector("[data-source-catalog-meta]")?.dataset.sourceState || "unknown",
      label: catalogNode?.querySelector(".source-connection-status")?.getAttribute("aria-label") || "",
    });

    try {
      const response = await window.fetch(
        `/api/data-sources/${encodeURIComponent(normalizedSourceId)}/${normalizedAction}`,
        { method: "POST" }
      );
      if (!response.ok) {
        throw new Error(`Failed to ${normalizedAction} ${normalizedSourceId}: ${response.status}`);
      }
      applyDataSourceEventsState(await response.json());
    } catch (error) {
      console.error(`Failed to ${normalizedAction} data source.`, error);
      await showMessageDialog({
        title: "Data source error",
        copy: `Could not ${normalizedAction} ${normalizedSourceId}.`,
      });
    } finally {
      sourceConnectionRequests.delete(normalizedSourceId);
      syncSourceConnectionControls(sourceCatalogNode(normalizedSourceId), {
        state:
          sourceCatalogNode(normalizedSourceId)
            ?.querySelector("[data-source-catalog-meta]")
            ?.dataset.sourceState || "unknown",
        label: sourceCatalogNode(normalizedSourceId)?.querySelector(".source-connection-status")?.getAttribute("aria-label") || "",
      });
    }
  }

  function applyDataSourceStatusIndicators(snapshot) {
    const statuses = Array.isArray(snapshot?.statuses) ? snapshot.statuses : [];
    const statusMap = new Map(
      statuses
        .filter((status) => typeof status?.sourceId === "string" && status.sourceId.trim())
        .map((status) => [status.sourceId.trim(), status])
    );

    document.querySelectorAll("[data-source-catalog]").forEach((catalogNode) => {
      const sourceId =
        catalogNode.dataset.sourceCatalogSourceId?.trim() || catalogNode.dataset.sourceCatalogName?.trim() || "";
      upsertSourceConnectionStatus(catalogNode, statusMap.get(sourceId) || null);
    });
  }

  async function refreshDataSourcesSection(mode = currentWorkspaceMode()) {
    const currentSection = dataSourcesSection();
    if (!currentSection) {
      return;
    }

    const sidebarState = captureSidebarState();
    const activeNotebookId = currentActiveNotebookId() || workspaceNotebookId() || "";
    const response = await window.fetch(
      `/sidebar?active_notebook_id=${encodeURIComponent(activeNotebookId)}&mode=${encodeURIComponent(mode)}`,
      {
        headers: { Accept: "text/html" },
      }
    );
    if (!response.ok) {
      throw new Error(`Failed to refresh the data sources section: ${response.status}`);
    }

    const container = document.createElement("div");
    container.innerHTML = await response.text();
    const nextSection = container.querySelector("[data-data-sources-section]");
    if (!(nextSection instanceof Element)) {
      throw new Error("Failed to locate the refreshed data sources section.");
    }

    currentSection.outerHTML = nextSection.outerHTML;
    await renderLocalWorkspaceSidebarEntries();
    restoreSidebarState(sidebarState);
    getRestoreSelectedSourceObject()?.();
    getRenderSidebarSourceOperationStatus()?.();
    replayPendingSourceCatalogBlinks();
  }

  function queueDataSourcesSectionRefresh() {
    if (pendingDataSourceSidebarRefreshHandle !== null) {
      return;
    }

    pendingDataSourceSidebarRefreshHandle = window.setTimeout(() => {
      pendingDataSourceSidebarRefreshHandle = null;

      if (dataSourceSidebarRefreshPromise) {
        dataSourceSidebarRefreshQueued = true;
        return;
      }

      const runRefresh = async () => {
        await refreshDataSourcesSection(currentWorkspaceMode());

        if (currentWorkspaceMode() !== "notebook") {
          return;
        }

        const notebookId = currentActiveNotebookId() || workspaceNotebookId();
        if (!notebookId || currentWorkspaceCanEdit()) {
          return;
        }

        await loadNotebookWorkspace(notebookId);
      };

      dataSourceSidebarRefreshPromise = runRefresh()
        .catch((error) => {
          console.error("Failed to refresh the sidebar after a data source change.", error);
        })
        .finally(() => {
          dataSourceSidebarRefreshPromise = null;
          if (dataSourceSidebarRefreshQueued) {
            dataSourceSidebarRefreshQueued = false;
            queueDataSourcesSectionRefresh();
          }
        });
    }, 120);
  }

  function applyDataSourceEventsState(snapshot) {
    const previousVersion = dataSourceEventsStateVersion;
    dataSourceEventsStateVersion = snapshot?.version ?? null;
    const latestEvent = Array.isArray(snapshot?.events) ? snapshot.events[0] : null;
    const previousLatestEventId = dataSourceEventsLatestEventId;
    dataSourceEventsLatestEventId = typeof latestEvent?.eventId === "string" ? latestEvent.eventId : null;

    if (previousVersion === null || dataSourceEventsStateVersion === previousVersion) {
      applyDataSourceStatusIndicators(snapshot);
      return;
    }

    applyDataSourceStatusIndicators(snapshot);

    if (!latestEvent || dataSourceEventsLatestEventId === previousLatestEventId) {
      return;
    }

    if (typeof latestEvent?.sourceId === "string" && latestEvent.sourceId.trim()) {
      pendingSourceCatalogBlinks.add(latestEvent.sourceId.trim());
    }
    const touchedRelations = [
      ...(latestEvent?.addedRelations ?? []),
      ...(latestEvent?.removedRelations ?? []),
      ...(latestEvent?.updatedRelations ?? []),
    ];
    if (touchedRelations.length) {
      clearSourceObjectFieldCacheForRelations(touchedRelations);
    }
    queueDataSourcesSectionRefresh();
  }

  function getDataSourceEventsStateVersion() {
    return dataSourceEventsStateVersion;
  }

  return {
    applyDataSourceEventsState,
    blinkSourceCatalog,
    getDataSourceEventsStateVersion,
    localWorkspaceEntryNode,
    localWorkspaceFolderNode,
    localWorkspaceSchemaNode,
    renderLocalWorkspaceSidebarEntries,
    revealSidebarS3Bucket,
    setDataSourceConnectionState,
    sourceCatalogNode,
    sourceSchemaBucketNode,
  };
}
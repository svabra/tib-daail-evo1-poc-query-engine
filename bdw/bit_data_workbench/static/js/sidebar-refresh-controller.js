export function createSidebarRefreshController(helpers) {
  const {
    applyNotebookMetadata,
    applySidebarSearchFilter,
    currentActiveNotebookId,
    currentSidebarMode,
    currentWorkspaceMode,
    dataSourcesSection,
    getInitializeNotebookTree,
    getInitializeSidebarResizer,
    getInitializeSidebarSearch,
    getInitializeSidebarToggle,
    getRenderDataGenerationMonitor,
    getRenderHomePage,
    getRenderLocalWorkspaceSidebarEntries,
    getRenderQueryMonitor,
    getRenderQueryNotificationMenu,
    getRenderSidebarSourceOperationStatus,
    getRestoreSelectedSourceObject,
    getSyncSelectedIngestionRunbookState,
    notebookSection,
    workspaceNotebookId,
  } = helpers;

  let sidebarRefreshRequestId = 0;

  function captureSidebarState() {
    return {
      sidebarMode: currentSidebarMode(),
      searchTerm: document.querySelector("[data-sidebar-search]")?.value ?? "",
      notebookSectionOpen: Boolean(notebookSection()?.open),
      ingestionRunbookSectionOpen: Boolean(document.querySelector("[data-ingestion-runbook-section]")?.open),
      runbookFoldersOpen: Array.from(document.querySelectorAll("[data-runbook-folder][open]")).map(
        (node) => node.dataset.runbookFolderId || ""
      ),
      dataSourcesSectionOpen: Boolean(dataSourcesSection()?.open),
      generationMonitorSectionOpen: Boolean(document.querySelector("[data-generation-monitor-section]")?.open),
      queryMonitorSectionOpen: Boolean(document.querySelector("[data-query-monitor-section]")?.open),
      sourceCatalogsOpen: Array.from(document.querySelectorAll("[data-source-catalog][open]")).map(
        (node) => node.dataset.sourceCatalogName || ""
      ),
      sourceSchemasOpen: Array.from(document.querySelectorAll("[data-source-schema][open]")).map(
        (node) => node.dataset.sourceSchemaKey || ""
      ),
    };
  }

  function restoreSidebarState(state) {
    if (!state) {
      return;
    }

    const stateSidebarMode = state.sidebarMode === "ingestion" ? "ingestion" : "notebook";
    const sidebarMode = currentSidebarMode();

    const search = document.querySelector("[data-sidebar-search]");
    if (search) {
      search.value = state.searchTerm || "";
    }

    const notebookSectionRoot = notebookSection();
    if (notebookSectionRoot && stateSidebarMode === "notebook" && sidebarMode === "notebook") {
      notebookSectionRoot.open = Boolean(state.notebookSectionOpen);
    }

    const ingestionRunbookSectionRoot = document.querySelector("[data-ingestion-runbook-section]");
    if (ingestionRunbookSectionRoot && stateSidebarMode === "ingestion" && sidebarMode === "ingestion") {
      ingestionRunbookSectionRoot.open = Boolean(state.ingestionRunbookSectionOpen);
    }

    if (stateSidebarMode === "ingestion" && sidebarMode === "ingestion") {
      const openRunbookFolders = new Set(Array.isArray(state.runbookFoldersOpen) ? state.runbookFoldersOpen : []);
      document.querySelectorAll("[data-runbook-folder]").forEach((node) => {
        node.open = openRunbookFolders.has(node.dataset.runbookFolderId || "");
      });
    }

    const dataSourcesRoot = dataSourcesSection();
    if (dataSourcesRoot) {
      dataSourcesRoot.open = Boolean(state.dataSourcesSectionOpen);
    }

    const generationMonitorSectionRoot = document.querySelector("[data-generation-monitor-section]");
    if (generationMonitorSectionRoot && stateSidebarMode === "ingestion" && sidebarMode === "ingestion") {
      generationMonitorSectionRoot.open = Boolean(state.generationMonitorSectionOpen);
    }

    const queryMonitorSectionRoot = document.querySelector("[data-query-monitor-section]");
    if (queryMonitorSectionRoot && stateSidebarMode === "notebook" && sidebarMode === "notebook") {
      queryMonitorSectionRoot.open = Boolean(state.queryMonitorSectionOpen);
    }

    const openCatalogs = new Set(Array.isArray(state.sourceCatalogsOpen) ? state.sourceCatalogsOpen : []);
    document.querySelectorAll("[data-source-catalog]").forEach((node) => {
      node.open = openCatalogs.has(node.dataset.sourceCatalogName || "");
    });

    const openSchemas = new Set(Array.isArray(state.sourceSchemasOpen) ? state.sourceSchemasOpen : []);
    document.querySelectorAll("[data-source-schema]").forEach((node) => {
      node.open = openSchemas.has(node.dataset.sourceSchemaKey || "");
    });

    applySidebarSearchFilter();
  }

  async function refreshSidebar(mode = currentWorkspaceMode()) {
    const sidebar = document.querySelector("[data-sidebar]");
    if (!sidebar) {
      return;
    }

    const requestId = ++sidebarRefreshRequestId;
    const sidebarState = captureSidebarState();
    const activeNotebookId = currentActiveNotebookId() || workspaceNotebookId() || "";
    const response = await window.fetch(
      `/sidebar?active_notebook_id=${encodeURIComponent(activeNotebookId)}&mode=${encodeURIComponent(mode)}`,
      {
        headers: { Accept: "text/html" },
      }
    );
    if (!response.ok) {
      throw new Error(`Failed to refresh the sidebar: ${response.status}`);
    }

    const markup = await response.text();
    if (requestId !== sidebarRefreshRequestId) {
      return;
    }

    const currentSidebar = document.querySelector("[data-sidebar]");
    if (!(currentSidebar instanceof Element) || !currentSidebar.isConnected || !currentSidebar.parentElement) {
      return;
    }

    currentSidebar.outerHTML = markup;
    getInitializeSidebarSearch()?.();
    getInitializeNotebookTree()?.();
    getInitializeSidebarToggle()?.();
    getInitializeSidebarResizer()?.();
    applyNotebookMetadata();
    await getRenderLocalWorkspaceSidebarEntries()?.();
    restoreSidebarState(sidebarState);
    getSyncSelectedIngestionRunbookState()?.();
    getRestoreSelectedSourceObject()?.();
    getRenderSidebarSourceOperationStatus()?.();
    getRenderDataGenerationMonitor()?.();
    getRenderQueryMonitor()?.();
    getRenderQueryNotificationMenu()?.();
    getRenderHomePage()?.();
  }

  return {
    captureSidebarState,
    refreshSidebar,
    restoreSidebarState,
  };
}
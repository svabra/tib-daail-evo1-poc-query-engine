import { createLocalWorkspaceDataSourceExplorer } from "./local-workspace-explorer.js";
import { createPostgresDataSourceExplorer } from "./postgres-explorer.js";
import { createS3DataSourceExplorer } from "./s3-explorer.js";

export function createDataSourceExplorerController(helpers) {
  const {
    allLocalWorkspaceFolderPaths,
    downloadLocalWorkspaceExportFromSource,
    downloadSourceS3Object,
    escapeHtml,
    fetchJsonOrThrow,
    formatByteCount,
    getPageRoot,
    listLocalWorkspaceExports,
    localWorkspaceDisplayPath,
    localWorkspaceFolderName,
    localWorkspaceRelation,
    normalizeLocalWorkspaceFolderPath,
    openDataProductPublishDialog,
    querySourceInCurrentNotebook,
    querySourceInNewNotebook,
    showMessageDialog,
    viewSourceData,
  } = helpers;

  const providerByKind = {
    postgres: createPostgresDataSourceExplorer({
      escapeHtml,
      fetchJsonOrThrow,
      openDataProductPublishDialog,
      querySourceInCurrentNotebook,
      querySourceInNewNotebook,
      showMessageDialog,
      viewSourceData,
    }),
    s3: createS3DataSourceExplorer({
      downloadSourceS3Object,
      escapeHtml,
      fetchJsonOrThrow,
      formatByteCount,
      openDataProductPublishDialog,
      showMessageDialog,
    }),
    "local-workspace": createLocalWorkspaceDataSourceExplorer({
      allLocalWorkspaceFolderPaths,
      downloadLocalWorkspaceExportFromSource,
      escapeHtml,
      formatByteCount,
      listLocalWorkspaceExports,
      localWorkspaceDisplayPath,
      localWorkspaceFolderName,
      localWorkspaceRelation,
      normalizeLocalWorkspaceFolderPath,
      openDataProductPublishDialog,
      querySourceInCurrentNotebook,
      querySourceInNewNotebook,
      showMessageDialog,
      viewSourceData,
    }),
  };

  async function handleRootClick(event) {
    const root = getPageRoot();
    if (!(root instanceof Element) || !root.contains(event.target)) {
      return;
    }

    const provider = providerByKind[root.dataset.explorerKind || ""];
    if (!provider?.handleClick) {
      return;
    }

    try {
      await provider.handleClick(event, root);
    } catch (error) {
      console.error("Failed to handle the data source explorer action.", error);
      await showMessageDialog({
        title: "Explorer action failed",
        copy:
          error instanceof Error
            ? error.message
            : "The selected explorer action could not be completed.",
      });
    }
  }

  async function initializeCurrentPage() {
    const root = getPageRoot();
    if (!(root instanceof HTMLElement)) {
      return;
    }

    if (root.dataset.dataSourceExplorerBound !== "true") {
      root.addEventListener("click", (event) => {
        handleRootClick(event).catch((error) => {
          console.error("Failed to route the data source explorer click.", error);
        });
      });
      root.dataset.dataSourceExplorerBound = "true";
    }

    const provider = providerByKind[root.dataset.explorerKind || ""];
    if (!provider?.initialize) {
      return;
    }

    await provider.initialize(root);
  }

  return {
    initializeCurrentPage,
  };
}

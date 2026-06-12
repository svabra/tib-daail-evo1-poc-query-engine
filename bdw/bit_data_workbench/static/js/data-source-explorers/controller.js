import { createLocalWorkspaceDataSourceExplorer } from "./local-workspace-explorer.js";
import { createPostgresDataSourceExplorer } from "./postgres-explorer.js";
import { createS3DataSourceExplorer } from "./s3-explorer.js";
import {
  detailCardMarkup,
  explorerEmptyStateMarkup,
  fieldListMarkup,
  publicationLinksMarkup,
} from "./utils.js";

export function createDataSourceExplorerController(helpers) {
  const {
    allLocalWorkspaceFolderPaths,
    copySourceQueryPath,
    downloadJobsController,
    downloadLocalWorkspaceExportFromSource,
    downloadSourceObjectDdl,
    downloadSourceS3GeneratedParts,
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
    prepareSourceS3Download,
    querySourceInCurrentNotebook,
    querySourceInNewNotebook,
    renderLocalWorkspaceSidebarEntries,
    showMessageDialog,
    viewSourceData,
  } = helpers;

  const providerByKind = {
    postgres: createPostgresDataSourceExplorer({
      escapeHtml,
      fetchJsonOrThrow,
      copySourceQueryPath,
      openDataProductPublishDialog,
      querySourceInCurrentNotebook,
      querySourceInNewNotebook,
      showMessageDialog,
      viewSourceData,
      downloadSourceObjectDdl,
    }),
    s3: createS3DataSourceExplorer({
      downloadSourceObjectDdl,
      downloadSourceS3Object,
      downloadSourceS3GeneratedParts,
      downloadJobsController,
      escapeHtml,
      fetchJsonOrThrow,
      formatByteCount,
      copySourceQueryPath,
      openDataProductPublishDialog,
      querySourceInCurrentNotebook,
      querySourceInNewNotebook,
      prepareSourceS3Download,
      showMessageDialog,
      viewSourceData,
    }),
    "local-workspace": createLocalWorkspaceDataSourceExplorer({
      allLocalWorkspaceFolderPaths,
      copySourceQueryPath,
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
      downloadSourceObjectDdl,
    }),
  };

  function navigationRoot(root) {
    return root.querySelector("[data-data-source-explorer-navigation]");
  }

  function detailRoot(root) {
    return root.querySelector("[data-data-source-explorer-detail]");
  }

  function sourceTreeBrowserNavigation(root) {
    const navigation = navigationRoot(root);
    if (
      navigation instanceof Element &&
      navigation.hasAttribute("data-data-source-explorer-source-tree-browser") &&
      navigation.querySelector("[data-source-tree]")
    ) {
      return navigation;
    }
    return null;
  }

  function parsePublishedProducts(node) {
    try {
      const parsed = JSON.parse(node?.dataset.publishedDataProducts || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      return [];
    }
  }

  function selectedSourceObjectLabel(node) {
    return (
      node?.dataset.sourceObjectDisplayName?.trim() ||
      node?.dataset.sourceObjectName?.trim() ||
      node?.dataset.sourceObjectRelation?.trim() ||
      "Selected source"
    );
  }

  function selectedSourceObjectQueryPath(node) {
    return (
      node?.dataset.sourceObjectQueryReference?.trim() ||
      node?.dataset.sourceObjectQueryAlias?.trim() ||
      node?.dataset.sourceObjectRelation?.trim() ||
      ""
    );
  }

  function selectedSourceObjectEyebrow(node) {
    const s3Format = node?.dataset.s3FileFormat?.trim();
    if (s3Format) {
      return `${s3Format.toUpperCase()} file - Shared Workspace`;
    }

    const localFormat = node?.dataset.localWorkspaceExportFormat?.trim();
    if (localFormat) {
      return `${localFormat.toUpperCase()} file - Local Workspace`;
    }

    return `${(node?.dataset.sourceObjectKind || "table").toUpperCase()} - Data source`;
  }

  function detailRows(rows = []) {
    const visibleRows = rows.filter((row) => row?.label && row?.value !== undefined && row?.value !== null && String(row.value).trim());
    if (!visibleRows.length) {
      return "";
    }

    return `
      <ul class="sidebar-source-field-list">
        ${visibleRows
          .map(
            (row) => `
              <li class="sidebar-source-field">
                <span class="sidebar-source-field-name">
                  <span class="sidebar-source-field-name-text">${escapeHtml(row.label)}</span>
                </span>
                <span class="sidebar-source-field-type">${escapeHtml(row.value)}</span>
              </li>
            `
          )
          .join("")}
      </ul>
    `;
  }

  function clearSourceTreeBrowserSelection(navigation) {
    navigation.querySelectorAll(".source-object.is-selected").forEach((node) => {
      node.classList.remove("is-selected");
    });
  }

  async function renderSourceTreeObjectDetail(root, sourceObjectRoot) {
    const detail = detailRoot(root);
    const navigation = navigationRoot(root);
    if (!(detail instanceof Element) || !(sourceObjectRoot instanceof Element)) {
      return;
    }

    if (navigation instanceof Element) {
      clearSourceTreeBrowserSelection(navigation);
      sourceObjectRoot.classList.add("is-selected");
    }

    const relation = sourceObjectRoot.dataset.sourceObjectRelation?.trim() || "";
    const queryPath = selectedSourceObjectQueryPath(sourceObjectRoot);
    const s3Size = Number(sourceObjectRoot.dataset.s3SizeBytes || 0) || 0;
    const localSize = Number(sourceObjectRoot.dataset.localWorkspaceSizeBytes || 0) || 0;
    const fieldRows = [
      { label: "Source reference", value: queryPath || "Not queryable yet" },
      { label: "Relation", value: relation && relation !== queryPath ? relation : "" },
      { label: "Storage path", value: sourceObjectRoot.dataset.s3Path || "" },
      { label: "Bucket", value: sourceObjectRoot.dataset.s3Bucket || "" },
      { label: "Object key", value: sourceObjectRoot.dataset.s3Key || "" },
      { label: "Folder", value: sourceObjectRoot.dataset.localWorkspaceFolderPath || "" },
      { label: "Rows", value: sourceObjectRoot.dataset.localWorkspaceRowCount || "" },
      { label: "Columns", value: sourceObjectRoot.dataset.localWorkspaceColumnCount || "" },
      {
        label: "Size",
        value: s3Size || localSize ? formatByteCount(s3Size || localSize) : "",
      },
    ];

    let fieldsMarkup = "";
    const isPostgresRelation =
      relation &&
      !sourceObjectRoot.dataset.s3Bucket &&
      !sourceObjectRoot.dataset.localWorkspaceEntryId;
    if (isPostgresRelation) {
      try {
        const payload = await fetchJsonOrThrow(
          `/api/source-object-fields?relation=${encodeURIComponent(relation)}`
        );
        fieldsMarkup = fieldListMarkup(Array.isArray(payload?.fields) ? payload.fields : [], escapeHtml);
      } catch (error) {
        fieldsMarkup = explorerEmptyStateMarkup(
          error instanceof Error ? error.message : "Fields could not be loaded for this source.",
          { tone: "danger" },
          escapeHtml
        );
      }
    }

    detail.innerHTML = detailCardMarkup(
      {
        eyebrow: selectedSourceObjectEyebrow(sourceObjectRoot),
        title: selectedSourceObjectLabel(sourceObjectRoot),
        copy: queryPath
          ? `Use ${queryPath} in SQL or open the row action menu for source actions.`
          : "Open the row action menu for the available source actions.",
        body: `
          ${publicationLinksMarkup(parsePublishedProducts(sourceObjectRoot), escapeHtml)}
          ${detailRows(fieldRows)}
          ${fieldsMarkup}
        `,
      },
      escapeHtml
    );
  }

  function summaryLabel(summary) {
    return (
      summary?.querySelector(".source-node-label span:last-child")?.textContent?.trim() ||
      summary?.textContent?.trim() ||
      "Selected source"
    );
  }

  function renderSourceTreeContainerDetail(root, containerRoot) {
    const detail = detailRoot(root);
    if (!(detail instanceof Element) || !(containerRoot instanceof Element)) {
      return;
    }

    const summary = containerRoot.querySelector(":scope > summary");
    const label = summaryLabel(summary);
    const sourceId =
      containerRoot.dataset.sourceCatalogSourceId?.trim() ||
      containerRoot.dataset.sourceCatalogName?.trim() ||
      "";
    const bucket =
      containerRoot.dataset.sourceBucket?.trim() ||
      containerRoot.dataset.s3Bucket?.trim() ||
      "";
    const s3Prefix = containerRoot.dataset.s3Prefix?.trim() || "";
    const schemaKey = containerRoot.dataset.sourceSchemaKey?.trim() || "";
    const objectCount = containerRoot.querySelectorAll("[data-source-object]").length;
    const isCatalog = containerRoot.hasAttribute("data-source-catalog");
    const isS3Folder = containerRoot.hasAttribute("data-source-s3-folder");
    const isLocalFolder = containerRoot.hasAttribute("data-local-workspace-folder-node");
    const eyebrow = isCatalog
      ? "Data source"
      : isS3Folder
        ? "Shared Workspace prefix"
      : bucket
        ? "Shared Workspace bucket"
        : isLocalFolder
          ? "Local Workspace folder"
          : "Source schema";

    detail.innerHTML = detailCardMarkup(
      {
        eyebrow,
        title: label,
        copy: isCatalog
          ? "Browse the expandable tree and open a row action menu for source actions."
          : "Expand this node to inspect its objects.",
        body: detailRows([
          { label: "Source", value: sourceId },
          { label: "Bucket", value: bucket },
          { label: "Prefix", value: s3Prefix },
          { label: "Schema", value: schemaKey },
          { label: "Objects", value: objectCount ? String(objectCount) : "" },
        ]),
      },
      escapeHtml
    );
  }

  async function initializeSourceTreeBrowser(root) {
    await renderLocalWorkspaceSidebarEntries?.();

    const navigation = sourceTreeBrowserNavigation(root);
    if (!(navigation instanceof Element)) {
      return false;
    }

    const catalogRoot = navigation.querySelector("[data-source-catalog]");
    if (catalogRoot instanceof HTMLDetailsElement) {
      catalogRoot.open = true;
      renderSourceTreeContainerDetail(root, catalogRoot);
      return true;
    }

    const detail = detailRoot(root);
    if (detail instanceof Element) {
      detail.innerHTML = explorerEmptyStateMarkup(
        "No browsable source tree is available for this data source.",
        {},
        escapeHtml
      );
    }
    return true;
  }

  async function handleSourceTreeBrowserClick(event, root) {
    const navigation = sourceTreeBrowserNavigation(root);
    if (!(navigation instanceof Element) || !navigation.contains(event.target)) {
      return false;
    }

    if (event.target.closest("[data-source-action-menu]")) {
      return false;
    }

    const sourceObjectRoot = event.target.closest("[data-source-object]");
    if (sourceObjectRoot && navigation.contains(sourceObjectRoot)) {
      event.preventDefault();
      event.stopPropagation();
      await renderSourceTreeObjectDetail(root, sourceObjectRoot);
      return true;
    }

    const summary = event.target.closest("summary");
    const containerRoot = summary?.closest("[data-source-s3-folder], [data-source-schema], [data-source-catalog]");
    if (containerRoot && navigation.contains(containerRoot)) {
      window.setTimeout(() => {
        renderSourceTreeContainerDetail(root, containerRoot);
      }, 0);
    }

    return false;
  }

  async function handleRootClick(event) {
    const root = getPageRoot();
    if (!(root instanceof Element) || !root.contains(event.target)) {
      return;
    }

    if (sourceTreeBrowserNavigation(root)) {
      try {
        await handleSourceTreeBrowserClick(event, root);
      } catch (error) {
        console.error("Failed to handle the data source tree browser action.", error);
        await showMessageDialog({
          title: "Explorer action failed",
          copy:
            error instanceof Error
              ? error.message
              : "The selected explorer action could not be completed.",
        });
      }
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

    if (await initializeSourceTreeBrowser(root)) {
      return;
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

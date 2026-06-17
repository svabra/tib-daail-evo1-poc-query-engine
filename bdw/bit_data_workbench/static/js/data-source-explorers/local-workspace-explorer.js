import {
  actionButtonMarkup,
  detailCardMarkup,
  explorerEmptyStateMarkup,
  sourceActionMenuMarkup,
  sourceObjectElement,
  sourceObjectRowMarkup,
  sourceSchemaDetailsMarkup,
} from "./utils.js";
import { localWorkspaceQueryAliases } from "../query-alias-utils.js";

export function createLocalWorkspaceDataSourceExplorer(helpers) {
  const {
    allLocalWorkspaceFolderPaths,
    copySourceDuckdbReference,
    copySourceQueryPath,
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
    downloadLocalWorkspaceExportFromSource,
  } = helpers;

  const stateByRoot = new WeakMap();

  function explorerState(root) {
    return stateByRoot.get(root) ?? null;
  }

  function navigationRoot(root) {
    return root.querySelector("[data-data-source-explorer-navigation]");
  }

  function detailRoot(root) {
    return root.querySelector("[data-data-source-explorer-detail]");
  }

  function createFolderNode(path = "") {
    return {
      path,
      name: localWorkspaceFolderName(path),
      folders: new Map(),
      entries: [],
    };
  }

  function buildTree(entries, folderPaths = []) {
    const rootNode = createFolderNode("");

    function ensureBranch(folderPath) {
      let currentNode = rootNode;
      let currentPath = "";
      normalizeLocalWorkspaceFolderPath(folderPath)
        .split("/")
        .filter(Boolean)
        .forEach((segment) => {
          currentPath = currentPath ? `${currentPath}/${segment}` : segment;
          if (!currentNode.folders.has(segment)) {
            currentNode.folders.set(segment, createFolderNode(currentPath));
          }
          currentNode = currentNode.folders.get(segment);
        });
      return currentNode;
    }

    allLocalWorkspaceFolderPaths(folderPaths).forEach((folderPath) => {
      if (folderPath) {
        ensureBranch(folderPath);
      }
    });

    entries.forEach((entry) => {
      ensureBranch(entry.folderPath).entries.push(entry);
    });

    return rootNode;
  }

  function entryById(state, entryId) {
    return state.entries.find((entry) => entry.id === entryId) ?? null;
  }

  function entryQueryPath(entry, state) {
    return localWorkspaceQueryAliases(state.entries).get(entry.id) || localWorkspaceRelation(entry.id);
  }

  function selectedDescriptorElement(state) {
    const entry = entryById(state, state.selectedEntryId);
    if (!entry) {
      return null;
    }
    const queryAlias = localWorkspaceQueryAliases(state.entries).get(entry.id) || "";
    return sourceObjectElement({
      relation: localWorkspaceRelation(entry.id),
      queryAlias,
      name: entry.fileName,
      displayName: entry.fileName,
      kind: "file",
      sourceOptionId: "workspace.local",
      localWorkspaceEntryId: entry.id,
      localWorkspaceFolderPath: entry.folderPath,
      localWorkspaceExportFormat: entry.exportFormat,
      localWorkspaceSizeBytes: entry.sizeBytes,
      localWorkspaceCreatedAt: entry.createdAt,
      localWorkspaceColumnCount: entry.columnCount,
      localWorkspaceRowCount: entry.rowCount,
      localWorkspaceMimeType: entry.mimeType,
    });
  }

  function entryActionMenuMarkup() {
    return sourceActionMenuMarkup(
      [
        {
          label: "View Data",
          action: "view",
          attrs: { "data-view-source-data": true },
          title: "Insert and run a query with all fields in the current notebook",
        },
        {
          label: "Query in current notebook",
          action: "query-current",
          attrs: { "data-query-source-current": true },
          title: "Insert a query into the current notebook",
        },
        {
          label: "Query in new notebook",
          action: "query-new",
          attrs: { "data-query-source-new": true },
          title: "Create a new notebook with this query",
        },
        {
          label: "Copy query path",
          action: "copy-query-path",
          attrs: { "data-copy-query-path": true },
          title: "Copy the SQL query path for this Local Workspace file",
        },
        {
          label: "Copy source reference - DuckDB",
          action: "copy-duckdb-source-reference",
          attrs: { "data-copy-duckdb-source-reference": true },
          title: "Copy the DuckDB source reference for this Local Workspace file",
        },
        {
          label: "Create data product ...",
          action: "create-data-product",
          attrs: { "data-create-data-product": true },
          title: "Start the managed publication flow for this file",
        },
        "separator",
        {
          label: "Download",
          action: "download",
          attrs: { "data-download-local-workspace-object": true },
          title: "Download the Local Workspace file",
        },
        {
          label: "Download DDL",
          action: "download-ddl",
          attrs: { "data-download-source-ddl": true },
          title: "Download DDL for this Local Workspace file",
        },
      ],
      escapeHtml
    );
  }

  function entryButtonMarkup(entry, state) {
    const queryPath = entryQueryPath(entry, state);
    const displayPath = localWorkspaceDisplayPath(entry.folderPath, entry.fileName);
    const formatLabel = String(entry.exportFormat || "file").toUpperCase();
    const queryAlias = localWorkspaceQueryAliases(state.entries).get(entry.id) || "";
    return sourceObjectRowMarkup(
      {
        kind: "file",
        displayName: entry.fileName,
        title: `${entry.fileName} | Query path: ${queryPath}`,
        searchable: `${entry.fileName} ${displayPath} ${formatLabel} ${queryPath}`,
        selected: state.selectedEntryId === entry.id,
        attrs: {
          "data-source-object": true,
          "data-source-object-kind": "file",
          "data-source-object-name": entry.fileName,
          "data-source-object-display-name": entry.fileName,
          "data-source-object-relation": localWorkspaceRelation(entry.id),
          "data-source-object-query-alias": queryAlias,
          "data-source-option-id": "workspace.local",
          "data-local-workspace-entry-id": entry.id,
          "data-local-workspace-folder-path": entry.folderPath,
          "data-local-workspace-export-format": entry.exportFormat,
          "data-local-workspace-size-bytes": entry.sizeBytes,
          "data-local-workspace-created-at": entry.createdAt,
          "data-local-workspace-column-count": entry.columnCount,
          "data-local-workspace-row-count": entry.rowCount,
          "data-local-workspace-mime-type": entry.mimeType,
          "data-data-source-explorer-local-entry": entry.id,
        },
        meta: `
          <small class="source-query-path-label" title="${escapeHtml(`Query path: ${queryPath}`)}">${escapeHtml(queryPath)}</small>
          <small>${escapeHtml(formatLabel)}</small>
          <small title="${escapeHtml(displayPath)}">${escapeHtml(formatByteCount(entry.sizeBytes))}</small>
        `,
        actions: entryActionMenuMarkup(),
      },
      escapeHtml
    );
  }

  function folderSummaryLabel(node) {
    const folderCount = node.folders.size;
    const fileCount = node.entries.length;
    const segments = [];
    if (folderCount) {
      segments.push(`${folderCount} folder${folderCount === 1 ? "" : "s"}`);
    }
    if (fileCount || !segments.length) {
      segments.push(`${fileCount} file${fileCount === 1 ? "" : "s"}`);
    }
    return segments.join(" | ");
  }

  function folderMarkup(node, state) {
    const childFolders = Array.from(node.folders.values())
      .sort((left, right) => left.name.localeCompare(right.name, undefined, { sensitivity: "base" }))
      .map((childNode) => folderMarkup(childNode, state))
      .join("");
    const files = node.entries
      .slice()
      .sort((left, right) => left.fileName.localeCompare(right.fileName, undefined, { sensitivity: "base" }))
      .map((entry) => entryButtonMarkup(entry, state))
      .join("");

    return sourceSchemaDetailsMarkup(
      {
        label: node.name,
        searchable: `${node.name} ${localWorkspaceDisplayPath(node.path)}`,
        attrs: {
          "data-local-workspace-folder-node": true,
          "data-local-workspace-folder-path": node.path,
        },
        meta: `<small>${escapeHtml(folderSummaryLabel(node))}</small>`,
        iconKind: "folder",
        children: `
          <div class="local-workspace-folder-branch">
            ${childFolders}
            ${files ? `<ul class="source-object-list">${files}</ul>` : ""}
          </div>
        `,
      },
      escapeHtml
    );
  }

  function renderNavigation(root) {
    const state = explorerState(root);
    const navigation = navigationRoot(root);
    if (!state || !(navigation instanceof Element)) {
      return;
    }

    if (!state.entries.length && !state.folderPaths.length) {
      navigation.innerHTML = explorerEmptyStateMarkup(
        "No Local Workspace files are available in this browser yet.",
        {},
        escapeHtml
      );
      return;
    }

    const tree = buildTree(state.entries, state.folderPaths);
    const folderMarkupHtml = Array.from(tree.folders.values())
      .sort((left, right) => left.name.localeCompare(right.name, undefined, { sensitivity: "base" }))
      .map((node) => folderMarkup(node, state))
      .join("");
    const rootFiles = tree.entries
      .slice()
      .sort((left, right) => left.fileName.localeCompare(right.fileName, undefined, { sensitivity: "base" }))
      .map((entry) => entryButtonMarkup(entry, state))
      .join("");

    navigation.innerHTML = `
      <div class="source-tree data-source-explorer-source-tree">
        ${folderMarkupHtml}
        ${rootFiles ? `<ul class="source-object-list data-source-explorer-root-object-list">${rootFiles}</ul>` : ""}
      </div>
    `;
  }

  function renderDetail(root) {
    const state = explorerState(root);
    const detail = detailRoot(root);
    if (!state || !(detail instanceof Element)) {
      return;
    }

    const entry = entryById(state, state.selectedEntryId);
    if (!entry) {
      detail.innerHTML = explorerEmptyStateMarkup(
        "Select a Local Workspace file to inspect it and open it in a notebook.",
        {},
        escapeHtml
      );
      return;
    }

    const queryPath = entryQueryPath(entry, state);
    detail.innerHTML = detailCardMarkup(
      {
        eyebrow: `${String(entry.exportFormat || "file").toUpperCase()} • Local Workspace`,
        title: entry.fileName,
        copy: `Browser-local file at ${localWorkspaceDisplayPath(entry.folderPath, entry.fileName)}.`,
        actions: [
          actionButtonMarkup("View Data", "view", escapeHtml),
          actionButtonMarkup("Query In Current Notebook", "query-current", escapeHtml),
          actionButtonMarkup("Query In New Notebook", "query-new", escapeHtml),
          actionButtonMarkup("Copy query path", "copy-query-path", escapeHtml),
          actionButtonMarkup("Copy source reference - DuckDB", "copy-duckdb-source-reference", escapeHtml),
          actionButtonMarkup("Create Data Product ...", "create-data-product", escapeHtml),
          actionButtonMarkup("Download", "download", escapeHtml),
          actionButtonMarkup("Download DDL", "download-ddl", escapeHtml),
        ].join(""),
        body: `
          <ul class="sidebar-source-field-list">
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Query path</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(queryPath)}</span>
            </li>
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Folder</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(entry.folderPath || "Root")}</span>
            </li>
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Rows</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(String(entry.rowCount || 0))}</span>
            </li>
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Columns</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(String(entry.columnCount || 0))}</span>
            </li>
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Size</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(formatByteCount(entry.sizeBytes || 0))}</span>
            </li>
          </ul>
        `,
      },
      escapeHtml
    );
  }

  async function render(root) {
    renderNavigation(root);
    renderDetail(root);
  }

  async function initialize(root) {
    const entries = await listLocalWorkspaceExports();
    const folderPaths = allLocalWorkspaceFolderPaths(entries.map((entry) => entry.folderPath));
    stateByRoot.set(root, {
      entries,
      folderPaths,
      selectedEntryId: entries[0]?.id || "",
    });
    await render(root);
  }

  async function selectEntry(root, entryId, { renderAfter = true } = {}) {
    const state = explorerState(root);
    if (!state) {
      return;
    }
    state.selectedEntryId = String(entryId || "").trim();
    if (renderAfter) {
      await render(root);
    }
  }

  async function handleClick(event, root) {
    const actionButton = event.target.closest("[data-data-source-explorer-action]");
    if (actionButton && root.contains(actionButton)) {
      event.preventDefault();
      event.stopPropagation();

      const actionEntry = actionButton.closest("[data-data-source-explorer-local-entry]");
      if (actionEntry && root.contains(actionEntry)) {
        await selectEntry(root, actionEntry.dataset.dataSourceExplorerLocalEntry || "", {
          renderAfter: true,
        });
      }

      const descriptor = selectedDescriptorElement(explorerState(root));
      if (!(descriptor instanceof Element)) {
        return true;
      }

      const action = String(
        actionButton.dataset.dataSourceExplorerAction || ""
      ).trim();
      if (action === "view") {
        const viewed = await viewSourceData(descriptor);
        if (viewed === false) {
          await showMessageDialog({
            title: "Notebook required",
            copy: "Open an editable notebook first, or use 'Query In New Notebook'.",
          });
        }
        return true;
      }

      if (action === "query-current") {
        const inserted = await querySourceInCurrentNotebook(descriptor);
        if (inserted === false) {
          await showMessageDialog({
            title: "Notebook required",
            copy: "Open an editable notebook first, or use 'Query In New Notebook'.",
          });
        }
        return true;
      }

      if (action === "query-new") {
        await querySourceInNewNotebook(descriptor);
        return true;
      }

      if (action === "copy-query-path") {
        if ((await copySourceQueryPath?.(descriptor)) === false) {
          await showMessageDialog({
            title: "Query path unavailable",
            copy: "This Local Workspace file does not expose a query path.",
          });
        }
        return true;
      }

      if (action === "copy-duckdb-source-reference") {
        if ((await copySourceDuckdbReference?.(descriptor)) === false) {
          await showMessageDialog({
            title: "DuckDB source reference unavailable",
            copy: "This Local Workspace file does not expose a DuckDB source reference.",
          });
        }
        return true;
      }

      if (action === "create-data-product") {
        await openDataProductPublishDialog({
          sourceObjectRoot: descriptor,
        });
        return true;
      }

      if (action === "download") {
        const downloaded = await downloadLocalWorkspaceExportFromSource(descriptor);
        if (downloaded === false) {
          await showMessageDialog({
            title: "Download unavailable",
            copy: "The selected Local Workspace file could not be downloaded from browser storage.",
          });
        }
        return true;
      }

      if (action === "download-ddl") {
        const downloaded = await downloadSourceObjectDdl(descriptor);
        if (downloaded === false) {
          await showMessageDialog({
            title: "DDL download unavailable",
            copy: "The selected Local Workspace file could not be prepared for DDL generation.",
          });
        }
        return true;
      }

      return false;
    }

    if (event.target.closest("[data-source-action-menu]")) {
      return false;
    }

    const entryButton = event.target.closest("[data-data-source-explorer-local-entry]");
    if (entryButton && root.contains(entryButton)) {
      event.preventDefault();
      event.stopPropagation();
      await selectEntry(root, entryButton.dataset.dataSourceExplorerLocalEntry || "");
      return true;
    }

    return false;
  }

  return {
    initialize,
    handleClick,
  };
}

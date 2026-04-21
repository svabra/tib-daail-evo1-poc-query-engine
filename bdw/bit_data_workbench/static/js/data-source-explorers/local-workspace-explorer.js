import {
  actionButtonMarkup,
  detailCardMarkup,
  explorerEmptyStateMarkup,
  sourceObjectElement,
} from "./utils.js";

export function createLocalWorkspaceDataSourceExplorer(helpers) {
  const {
    allLocalWorkspaceFolderPaths,
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

  function selectedDescriptorElement(state) {
    const entry = entryById(state, state.selectedEntryId);
    if (!entry) {
      return null;
    }
    return sourceObjectElement({
      relation: localWorkspaceRelation(entry.id),
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

  function entryButtonMarkup(entry, state) {
    const displayPath = localWorkspaceDisplayPath(entry.folderPath, entry.fileName);
    return `
      <button
        type="button"
        class="data-source-explorer-object${state.selectedEntryId === entry.id ? " is-active" : ""}"
        data-data-source-explorer-local-entry="${escapeHtml(entry.id)}"
      >
        <span class="data-source-explorer-object-copy">
          <strong>${escapeHtml(entry.fileName)}</strong>
          <span>${escapeHtml(
            `${String(entry.exportFormat || "file").toUpperCase()} • ${displayPath}`
          )}</span>
        </span>
      </button>
    `;
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

    return `
      <details class="data-source-explorer-group" open>
        <summary>
          <span>${escapeHtml(node.name)}</span>
          <small>${escapeHtml(String(node.entries.length))} file(s)</small>
        </summary>
        <div class="data-source-explorer-group-body">
          ${childFolders}
          ${files}
        </div>
      </details>
    `;
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
      <div class="data-source-explorer-tree">
        ${folderMarkupHtml}
        ${rootFiles ? `<div class="data-source-explorer-group-body">${rootFiles}</div>` : ""}
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

    detail.innerHTML = detailCardMarkup(
      {
        eyebrow: `${String(entry.exportFormat || "file").toUpperCase()} • Local Workspace`,
        title: entry.fileName,
        copy: `Browser-local file at ${localWorkspaceDisplayPath(entry.folderPath, entry.fileName)}.`,
        actions: [
          actionButtonMarkup("View Data", "view", escapeHtml),
          actionButtonMarkup("Query In Current Notebook", "query-current", escapeHtml),
          actionButtonMarkup("Query In New Notebook", "query-new", escapeHtml),
          actionButtonMarkup("Create Data Product ...", "create-data-product", escapeHtml),
          actionButtonMarkup("Download", "download", escapeHtml),
        ].join(""),
        body: `
          <ul class="sidebar-source-field-list">
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

  async function handleClick(event, root) {
    const entryButton = event.target.closest("[data-data-source-explorer-local-entry]");
    if (entryButton && root.contains(entryButton)) {
      event.preventDefault();
      event.stopPropagation();
      const state = explorerState(root);
      if (!state) {
        return true;
      }
      state.selectedEntryId =
        entryButton.dataset.dataSourceExplorerLocalEntry || "";
      await render(root);
      return true;
    }

    const actionButton = event.target.closest("[data-data-source-explorer-action]");
    if (!(actionButton && root.contains(actionButton))) {
      return false;
    }

    event.preventDefault();
    event.stopPropagation();

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

    return false;
  }

  return {
    initialize,
    handleClick,
  };
}

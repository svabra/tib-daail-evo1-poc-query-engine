export function createLocalWorkspaceSidebarUi(helpers) {
  const {
    allLocalWorkspaceFolderPaths,
    escapeHtml,
    formatByteCount,
    getLocalWorkspaceCatalogSourceId,
    localWorkspaceDisplayPath,
    localWorkspaceFolderDepth,
    localWorkspaceFolderName,
    localWorkspaceRelation,
    getLocalWorkspaceSchemaKey,
    normalizeLocalWorkspaceFolderPath,
  } = helpers;

  function createLocalWorkspaceTreeNode(path = "") {
    return {
      path,
      name: localWorkspaceFolderName(path),
      folders: new Map(),
      entries: [],
    };
  }

  function buildLocalWorkspaceTree(entries, folderPaths = []) {
    const root = createLocalWorkspaceTreeNode("");

    const ensureBranch = (folderPath) => {
      let currentNode = root;
      let currentPath = "";
      normalizeLocalWorkspaceFolderPath(folderPath)
        .split("/")
        .filter(Boolean)
        .forEach((segment) => {
          currentPath = currentPath ? `${currentPath}/${segment}` : segment;
          if (!currentNode.folders.has(segment)) {
            currentNode.folders.set(segment, createLocalWorkspaceTreeNode(currentPath));
          }
          currentNode = currentNode.folders.get(segment);
        });
      return currentNode;
    };

    allLocalWorkspaceFolderPaths(folderPaths).forEach((folderPath) => {
      if (folderPath) {
        ensureBranch(folderPath);
      }
    });

    entries.forEach((entry) => {
      ensureBranch(entry.folderPath).entries.push(entry);
    });

    return root;
  }

  function localWorkspaceEntryMarkup(entry) {
    const relation = localWorkspaceRelation(entry.id);
    const formatLabel = String(entry.exportFormat || "file").toUpperCase();
    const displayPath = localWorkspaceDisplayPath(entry.folderPath, entry.fileName);
    const sizeLabel = formatByteCount(entry.sizeBytes);

    return `
      <li
        class="source-object source-object-file"
        data-searchable-item="${escapeHtml(entry.fileName)} ${escapeHtml(displayPath)} ${escapeHtml(formatLabel)}"
        data-source-object
        data-source-object-kind="file"
        data-source-object-name="${escapeHtml(entry.fileName)}"
        data-source-object-relation="${escapeHtml(relation)}"
        data-source-option-id="${escapeHtml(getLocalWorkspaceCatalogSourceId())}"
        data-local-workspace-entry-id="${escapeHtml(entry.id)}"
        data-local-workspace-folder-path="${escapeHtml(entry.folderPath)}"
        data-local-workspace-export-format="${escapeHtml(entry.exportFormat)}"
        data-local-workspace-size-bytes="${escapeHtml(entry.sizeBytes)}"
        data-local-workspace-created-at="${escapeHtml(entry.createdAt)}"
        data-local-workspace-column-count="${escapeHtml(entry.columnCount)}"
        data-local-workspace-row-count="${escapeHtml(entry.rowCount)}"
        data-local-workspace-mime-type="${escapeHtml(entry.mimeType)}"
      >
        <span class="source-node-label">
          <svg
            class="source-icon source-icon-object source-icon-object-view"
            viewBox="0 0 16 16"
            aria-hidden="true"
          >
            <rect x="2.4" y="3" width="11.2" height="9.6" rx="1.1"></rect>
            <path d="M4.2 5.2h7.6M4.2 7.7h7.6"></path>
            <path d="M3.2 10.7c1.5-1.7 3.1-2.6 4.8-2.6s3.3.9 4.8 2.6"></path>
            <circle cx="8" cy="10.2" r="1"></circle>
          </svg>
          <span>${escapeHtml(entry.fileName)}</span>
        </span>
        <span class="source-object-meta">
          <small>${escapeHtml(formatLabel)}</small>
          <small title="${escapeHtml(displayPath)}">${escapeHtml(sizeLabel)}</small>
          <details class="workspace-action-menu source-action-menu" data-source-action-menu>
            <summary
              class="workspace-action-menu-toggle"
              data-source-action-menu-toggle
              aria-label="Source actions"
              title="Source actions"
            >
              <span class="workspace-action-menu-dots" aria-hidden="true">...</span>
            </summary>
            <div class="workspace-action-menu-panel">
              <button
                type="button"
                class="workspace-action-menu-item"
                data-move-local-workspace-object
                title="Move the Local Workspace file"
              >
                Move local file
              </button>
              <button
                type="button"
                class="workspace-action-menu-item"
                data-download-local-workspace-object
                title="Download the Local Workspace file"
              >
                Download local file
              </button>
              <div class="workspace-action-menu-separator" aria-hidden="true"></div>
              <button
                type="button"
                class="workspace-action-menu-item workspace-action-menu-item-danger"
                data-delete-local-workspace-object
                title="Delete the Local Workspace file"
              >
                Delete local file
              </button>
            </div>
          </details>
        </span>
      </li>
    `;
  }

  function localWorkspaceFolderSummaryLabel(node) {
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

  function localWorkspaceFolderMarkup(node, openPaths = new Set()) {
    const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(node.path);
    const childFolderMarkup = Array.from(node.folders.values())
      .sort((left, right) => left.name.localeCompare(right.name, undefined, { sensitivity: "base" }))
      .map((childNode) => localWorkspaceFolderMarkup(childNode, openPaths))
      .join("");
    const fileMarkup = node.entries
      .slice()
      .sort((left, right) => left.fileName.localeCompare(right.fileName, undefined, { sensitivity: "base" }))
      .map((entry) => localWorkspaceEntryMarkup(entry))
      .join("");
    const shouldOpen = openPaths.size
      ? openPaths.has(normalizedFolderPath)
      : localWorkspaceFolderDepth(normalizedFolderPath) <= 1;

    return `
      <details
        class="source-schema local-workspace-folder-node"
        data-local-workspace-folder-node
        data-local-workspace-folder-path="${escapeHtml(normalizedFolderPath)}"
        ${shouldOpen ? "open" : ""}
      >
        <summary
          class="local-workspace-folder-summary"
          data-searchable-item="${escapeHtml(node.name)} ${escapeHtml(localWorkspaceDisplayPath(normalizedFolderPath))}"
        >
          <span class="source-node-label">
            <svg class="source-icon source-icon-schema" viewBox="0 0 16 16" aria-hidden="true">
              <path d="M2.2 4.1a1.1 1.1 0 0 1 1.1-1.1h3l1.2 1.5h5.2a1.1 1.1 0 0 1 1.1 1.1v5.9a1.1 1.1 0 0 1-1.1 1.1H3.3a1.1 1.1 0 0 1-1.1-1.1z"></path>
            </svg>
            <span>${escapeHtml(node.name)}</span>
          </span>
          <span class="source-schema-meta">
            <small>${escapeHtml(localWorkspaceFolderSummaryLabel(node))}</small>
            <details class="workspace-action-menu source-action-menu" data-source-action-menu>
              <summary
                class="workspace-action-menu-toggle"
                data-source-action-menu-toggle
                aria-label="Folder actions"
                title="Folder actions"
              >
                <span class="workspace-action-menu-dots" aria-hidden="true">...</span>
              </summary>
              <div class="workspace-action-menu-panel">
                <button
                  type="button"
                  class="workspace-action-menu-item"
                  data-create-local-workspace-folder-path="${escapeHtml(normalizedFolderPath)}"
                  title="Create a subfolder"
                >
                  New subfolder
                </button>
                <div class="workspace-action-menu-separator" aria-hidden="true"></div>
                <button
                  type="button"
                  class="workspace-action-menu-item workspace-action-menu-item-danger"
                  data-delete-local-workspace-folder-path="${escapeHtml(normalizedFolderPath)}"
                  title="Delete this folder and its saved files"
                >
                  Delete folder
                </button>
              </div>
            </details>
          </span>
        </summary>
        <div class="local-workspace-folder-branch">
          ${childFolderMarkup}
          ${fileMarkup ? `<ul class="source-object-list">${fileMarkup}</ul>` : ""}
        </div>
      </details>
    `;
  }

  function localWorkspaceSchemaMarkup(entries, folderPaths, open = false, openPaths = new Set()) {
    const tree = buildLocalWorkspaceTree(entries, folderPaths);
    const folderMarkup = Array.from(tree.folders.values())
      .sort((left, right) => left.name.localeCompare(right.name, undefined, { sensitivity: "base" }))
      .map((node) => localWorkspaceFolderMarkup(node, openPaths))
      .join("");
    const rootFileMarkup = tree.entries
      .slice()
      .sort((left, right) => left.fileName.localeCompare(right.fileName, undefined, { sensitivity: "base" }))
      .map((entry) => localWorkspaceEntryMarkup(entry))
      .join("");
    const folderCount = allLocalWorkspaceFolderPaths(folderPaths).filter(Boolean).length;

    return `
      <details
        class="source-schema"
        data-source-schema
        data-source-schema-key="${escapeHtml(getLocalWorkspaceSchemaKey())}"
        ${open ? "open" : ""}
      >
        <summary data-searchable-item="Saved Results Local Workspace IndexedDB">
          <span class="source-node-label">
            <svg class="source-icon source-icon-schema" viewBox="0 0 16 16" aria-hidden="true">
              <rect x="1.6" y="1.8" width="3.7" height="3.7" rx="0.7"></rect>
              <rect x="10.7" y="1.8" width="3.7" height="3.7" rx="0.7"></rect>
              <rect x="10.7" y="10.5" width="3.7" height="3.7" rx="0.7"></rect>
              <path d="M5.3 3.7h2.8a2 2 0 0 1 2 2v1.5"></path>
              <path d="M10.1 8H7.4a2 2 0 0 0-2 2v.5"></path>
            </svg>
            <span>Saved Results</span>
          </span>
          <span class="source-schema-meta">
            <small>${escapeHtml(String(folderCount))} folder${folderCount === 1 ? "" : "s"}</small>
            <small>${escapeHtml(String(entries.length))} file${entries.length === 1 ? "" : "s"}</small>
          </span>
        </summary>
        <div class="local-workspace-folder-branch local-workspace-root-branch">
          ${folderMarkup}
          ${rootFileMarkup ? `<ul class="source-object-list">${rootFileMarkup}</ul>` : ""}
        </div>
      </details>
    `;
  }

  return {
    localWorkspaceSchemaMarkup,
  };
}
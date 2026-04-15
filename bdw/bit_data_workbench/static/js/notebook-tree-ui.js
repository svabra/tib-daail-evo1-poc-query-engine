export function createNotebookTreeUi(helpers) {
  const {
    applyNotebookMetadata,
    createNotebookLinkElement,
    defaultFolderPermissions,
    deriveFolderId,
    getDraggedNotebook,
    isLocalNotebookId,
    loadNotebookWorkspace,
    nextVisibleNotebookId,
    notebookMetadata,
    notebookSection,
    notebookTreeRoot,
    persistNotebookDraft,
    readStoredNotebookTree,
    renderEmptyWorkspace,
    unassignedFolderName,
    updateLastNotebookId,
    visibleNotebookLinks,
    workspaceNotebookId,
    writeStoredNotebookTree,
    deleteStoredNotebookState,
  } = helpers;

  function directChildrenContainer(folder) {
    return folder?.querySelector(":scope > [data-tree-children]") ?? null;
  }

  function updateFolderCounts(root = document) {
    root.querySelectorAll("[data-tree-folder]").forEach((folder) => {
      const countLabel = folder.querySelector(":scope > summary .tree-folder-count");
      const children = directChildrenContainer(folder);
      if (!countLabel || !children) {
        return;
      }

      const notebookCount = children.querySelectorAll("[data-draggable-notebook]:not([hidden])").length;
      countLabel.textContent = String(notebookCount);
    });
  }

  function updateNotebookSectionCount() {
    const count = notebookSection()?.querySelector(".section-count");
    if (!count) {
      return;
    }

    count.textContent = String(visibleNotebookLinks().length);
  }

  function applyFolderActionState(button, { allowed, enabledTitle, disabledTitle }) {
    button.classList.toggle("is-action-disabled", !allowed);
    button.disabled = !allowed;
    button.title = allowed ? enabledTitle : disabledTitle;
  }

  function createFolderNode(
    name,
    { open = false, folderId = "", canEdit = true, canDelete = true } = {}
  ) {
    const folder = document.createElement("details");
    folder.className = "tree-folder";
    folder.dataset.treeFolder = "";
    folder.open = open;
    folder.dataset.folderId = folderId || "";
    folder.dataset.canEdit = String(canEdit);
    folder.dataset.canDelete = String(canDelete);

    const summary = document.createElement("summary");
    summary.className = "tree-folder-summary";
    summary.dataset.searchableItem = name;

    const label = document.createElement("span");
    label.className = "tree-folder-label";
    label.textContent = name;

    const tools = document.createElement("span");
    tools.className = "tree-folder-tools";

    const createNotebookButton = document.createElement("button");
    createNotebookButton.type = "button";
    createNotebookButton.className = `tree-add-button tree-add-button-inline${canEdit ? "" : " is-action-disabled"}`;
    createNotebookButton.dataset.createNotebook = "";
    createNotebookButton.title = canEdit ? "Create notebook" : "This folder cannot receive new notebooks.";
    createNotebookButton.setAttribute("aria-label", "Create notebook");
    createNotebookButton.disabled = !canEdit;
    createNotebookButton.innerHTML = `
    <svg class="tree-action-glyph" viewBox="0 0 16 16" aria-hidden="true">
      <path d="M4 2.2h5.2l2.8 2.8v8.8H4z"></path>
      <path d="M9.2 2.2v2.7h2.8"></path>
      <path d="M8 6.9v4.2M5.9 9h4.2"></path>
    </svg>
  `;

    const renameButton = document.createElement("button");
    renameButton.type = "button";
    renameButton.className = "tree-add-button tree-add-button-inline";
    renameButton.dataset.renameTreeFolder = "";
    renameButton.setAttribute("aria-label", "Rename folder");
    renameButton.textContent = "Edit";
    applyFolderActionState(renameButton, {
      allowed: canEdit,
      enabledTitle: "Rename folder",
      disabledTitle: "This folder cannot be renamed.",
    });

    const deleteButton = document.createElement("button");
    deleteButton.type = "button";
    deleteButton.className = "tree-add-button tree-add-button-inline tree-delete-button";
    deleteButton.dataset.deleteTreeFolder = "";
    deleteButton.setAttribute("aria-label", "Delete folder");
    deleteButton.textContent = "Delete";
    applyFolderActionState(deleteButton, {
      allowed: canDelete,
      enabledTitle: "Delete folder. Notebooks will be moved to the unassigned folder.",
      disabledTitle: "This folder cannot be deleted.",
    });

    const addButton = document.createElement("button");
    addButton.type = "button";
    addButton.className = `tree-add-button tree-add-button-inline${canEdit ? "" : " is-action-disabled"}`;
    addButton.dataset.addTreeItem = "";
    addButton.title = canEdit ? "Add subfolder" : "This folder cannot be changed.";
    addButton.setAttribute("aria-label", "Add subfolder");
    addButton.textContent = "+";
    addButton.disabled = !canEdit;

    const count = document.createElement("span");
    count.className = "tree-folder-count";
    count.textContent = "0";

    tools.append(createNotebookButton, renameButton, deleteButton, addButton, count);
    summary.append(label, tools);
    folder.append(summary);

    const children = document.createElement("div");
    children.className = "tree-children";
    children.dataset.treeChildren = "";
    folder.append(children);

    return folder;
  }

  function folderLabel(folder) {
    return folder?.querySelector(":scope > summary .tree-folder-label") ?? null;
  }

  function isUnassignedFolder(folder) {
    if (!(folder instanceof Element) || !folder.matches("[data-tree-folder]")) {
      return false;
    }

    return (
      folder.dataset.systemFolder === "unassigned" ||
      folderLabel(folder)?.textContent?.trim() === unassignedFolderName
    );
  }

  function folderCanEdit(folder) {
    if (!(folder instanceof Element)) {
      return false;
    }

    return folder.dataset.canEdit !== "false";
  }

  function folderCanDelete(folder) {
    if (!(folder instanceof Element)) {
      return false;
    }

    return folder.dataset.canDelete !== "false";
  }

  function notebookCountInFolder(folder) {
    if (!(folder instanceof Element)) {
      return 0;
    }

    return folder.querySelectorAll("[data-draggable-notebook]:not([hidden])").length;
  }

  function rootUnassignedFolder() {
    const root = notebookTreeRoot();
    if (!root) {
      return null;
    }

    return (
      Array.from(root.querySelectorAll(":scope > [data-tree-folder]")).find((folder) =>
        isUnassignedFolder(folder)
      ) ?? null
    );
  }

  function syncRootUnassignedFolder() {
    const root = notebookTreeRoot();
    const folder = rootUnassignedFolder();
    if (!root || !folder) {
      return null;
    }

    folder.dataset.systemFolder = "unassigned";
    if (notebookCountInFolder(folder) === 0) {
      folder.remove();
      return null;
    }

    root.appendChild(folder);
    return folder;
  }

  function ensureRootUnassignedFolder() {
    const root = notebookTreeRoot();
    if (!root) {
      return null;
    }

    const existing = rootUnassignedFolder();
    if (existing) {
      existing.dataset.systemFolder = "unassigned";
      existing.open = true;
      root.appendChild(existing);
      return existing;
    }

    const folder = createFolderNode(unassignedFolderName, { open: true });
    folder.dataset.systemFolder = "unassigned";
    root.appendChild(folder);
    return folder;
  }

  function collectFolderNotebooks(folder) {
    return Array.from(folder.querySelectorAll("[data-draggable-notebook]"));
  }

  async function deleteTreeFolder(folder, { recursive = false } = {}) {
    const notebooks = collectFolderNotebooks(folder);
    if (isUnassignedFolder(folder) && notebooks.length > 0) {
      return;
    }

    const removedNotebookIds = notebooks
      .map((notebook) => notebook.dataset.notebookId)
      .filter(Boolean);
    const activeNotebookId = workspaceNotebookId(document);

    if (!recursive) {
      let targetContainer = null;
      if (notebooks.length > 0) {
        const targetFolder = ensureRootUnassignedFolder();
        targetContainer = directChildrenContainer(targetFolder);
        if (!targetContainer) {
          return;
        }
      }

      for (const notebook of notebooks) {
        targetContainer?.appendChild(notebook);
      }
    } else {
      for (const notebookId of removedNotebookIds) {
        if (isLocalNotebookId(notebookId)) {
          deleteStoredNotebookState(notebookId);
        } else {
          persistNotebookDraft(notebookId, { deleted: true });
        }
      }
    }

    folder.remove();
    syncRootUnassignedFolder();
    updateFolderCounts();
    persistNotebookTree();
    applyNotebookMetadata();

    if (!recursive || !activeNotebookId || !removedNotebookIds.includes(activeNotebookId)) {
      return;
    }

    const fallbackNotebookId = nextVisibleNotebookId(activeNotebookId);
    if (!fallbackNotebookId) {
      renderEmptyWorkspace();
      updateLastNotebookId("");
      return;
    }

    try {
      await loadNotebookWorkspace(fallbackNotebookId);
    } catch (error) {
      if (error?.name === "AbortError") {
        return;
      }
      console.error("Failed to load the fallback notebook after recursive folder deletion.", error);
    }
  }

  function serializeTreeNode(node) {
    if (!(node instanceof Element)) {
      return null;
    }

    if (node.matches("[data-tree-folder]")) {
      const name =
        node.querySelector(":scope > summary .tree-folder-label")?.textContent?.trim() || "Folder";
      const children = directChildrenContainer(node);
      return {
        type: "folder",
        folderId: node.dataset.folderId || null,
        name,
        open: node.open,
        systemFolder: node.dataset.systemFolder || null,
        canEdit: node.dataset.canEdit !== "false",
        canDelete: node.dataset.canDelete !== "false",
        children: children
          ? Array.from(children.children)
              .map((child) => serializeTreeNode(child))
              .filter(Boolean)
          : [],
      };
    }

    if (node.matches("[data-draggable-notebook]")) {
      return {
        type: "notebook",
        notebookId: node.dataset.notebookId,
      };
    }

    return null;
  }

  function persistNotebookTree() {
    const root = notebookTreeRoot();
    if (!root) {
      return;
    }

    syncRootUnassignedFolder();
    const state = Array.from(root.children)
      .map((child) => serializeTreeNode(child))
      .filter(Boolean);
    writeStoredNotebookTree(state);
  }

  function renderStoredTreeNode(nodeState, notebookLookup, parentFolderId = "") {
    if (!nodeState || typeof nodeState !== "object") {
      return null;
    }

    if (nodeState.type === "notebook") {
      const notebookEntry = notebookLookup.get(nodeState.notebookId);
      if (!notebookEntry) {
        if (isLocalNotebookId(nodeState.notebookId)) {
          const metadata = notebookMetadata(nodeState.notebookId);
          if (!metadata.deleted) {
            return createNotebookLinkElement(nodeState.notebookId, metadata);
          }
        }
        return null;
      }

      notebookLookup.delete(nodeState.notebookId);
      return notebookEntry.element;
    }

    if (nodeState.type === "folder") {
      const resolvedFolderId = nodeState.folderId || deriveFolderId(nodeState.name || "Folder", parentFolderId);
      const fallbackPolicy = defaultFolderPermissions(resolvedFolderId);
      const folder = createFolderNode(nodeState.name || "Folder", {
        open: Boolean(nodeState.open),
        folderId: resolvedFolderId,
        canEdit: fallbackPolicy.canEdit
          ? typeof nodeState.canEdit === "boolean"
            ? nodeState.canEdit
            : true
          : false,
        canDelete: fallbackPolicy.canDelete
          ? typeof nodeState.canDelete === "boolean"
            ? nodeState.canDelete
            : true
          : false,
      });
      if (nodeState.systemFolder) {
        folder.dataset.systemFolder = nodeState.systemFolder;
      }
      const container = directChildrenContainer(folder);

      for (const child of nodeState.children ?? []) {
        const renderedChild = renderStoredTreeNode(child, notebookLookup, resolvedFolderId);
        if (renderedChild) {
          container.appendChild(renderedChild);
        }
      }

      return folder;
    }

    return null;
  }

  function resolveAddTarget(button) {
    const folder = button.closest("[data-tree-folder]");
    if (folder) {
      folder.open = true;
      return directChildrenContainer(folder);
    }

    return notebookTreeRoot();
  }

  function clearDropTargets() {
    document.querySelectorAll(".tree-children.is-drag-over").forEach((node) => {
      node.classList.remove("is-drag-over");
    });
    document.querySelectorAll(".tree-folder.is-drag-over").forEach((node) => {
      node.classList.remove("is-drag-over");
    });
  }

  function clearDragState() {
    clearDropTargets();
    const draggedNotebook = getDraggedNotebook();
    if (draggedNotebook) {
      draggedNotebook.classList.remove("is-dragging");
    }
  }

  function resolveDropTarget(target) {
    if (!(target instanceof Element)) {
      return null;
    }

    const explicitContainer = target.closest("[data-tree-children]");
    if (explicitContainer) {
      return explicitContainer;
    }

    const folder = target.closest("[data-tree-folder]");
    if (folder) {
      return directChildrenContainer(folder);
    }

    return notebookTreeRoot();
  }

  function dropTargetAcceptsNotebookDrop(target) {
    if (!(target instanceof Element)) {
      return false;
    }

    const folder = target.closest("[data-tree-folder]");
    return !folder || folderCanEdit(folder);
  }

  function notebookDefaultFolderPath(notebook) {
    if (!(notebook instanceof Element)) {
      return [];
    }

    const path = [];
    let currentFolder = notebook.closest("[data-tree-folder]");

    while (currentFolder) {
      const label = folderLabel(currentFolder)?.textContent?.trim();
      if (label) {
        path.push(label);
      }
      currentFolder = currentFolder.parentElement?.closest("[data-tree-folder]") ?? null;
    }

    return path.reverse();
  }

  function ensureTreeFolderPath(root, folderPath) {
    if (!(root instanceof Element)) {
      return null;
    }

    const normalizedPath = Array.isArray(folderPath)
      ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
      : [];
    if (normalizedPath.length === 0) {
      return root;
    }

    let container = root;
    let parentFolderId = "";

    for (const folderName of normalizedPath) {
      const folderId = deriveFolderId(folderName, parentFolderId);
      let folder =
        Array.from(container.children).find(
          (child) =>
            child instanceof Element &&
            child.matches("[data-tree-folder]") &&
            (child.dataset.folderId === folderId || folderLabel(child)?.textContent?.trim() === folderName)
        ) ?? null;

      if (!folder) {
        const permissions = defaultFolderPermissions(folderId);
        folder = createFolderNode(folderName, {
          open: true,
          folderId,
          canEdit: permissions.canEdit,
          canDelete: permissions.canDelete,
        });
        container.appendChild(folder);
      } else {
        folder.open = true;
      }

      container = directChildrenContainer(folder) ?? container;
      parentFolderId = folderId;
    }

    return container;
  }

  function placeNotebookInDefaultFolder(root, notebook, folderPath) {
    if (!(root instanceof Element) || !(notebook instanceof Element)) {
      return false;
    }

    const targetContainer = ensureTreeFolderPath(root, folderPath);
    if (!(targetContainer instanceof Element)) {
      return false;
    }

    targetContainer.appendChild(notebook);
    return true;
  }

  function initializeNotebookTree(root = document) {
    const treeRoot =
      root instanceof Element && root.matches("[data-notebook-tree]") ? root : notebookTreeRoot();

    if (!treeRoot) {
      return;
    }

    const storedTree = readStoredNotebookTree();
    if (storedTree) {
      const notebookLookup = new Map(
        Array.from(treeRoot.querySelectorAll("[data-draggable-notebook]")).map((notebook) => [
          notebook.dataset.notebookId,
          {
            element: notebook,
            defaultFolderPath: notebookDefaultFolderPath(notebook),
          },
        ])
      );
      const fragment = document.createDocumentFragment();

      for (const nodeState of storedTree) {
        const renderedNode = renderStoredTreeNode(nodeState, notebookLookup);
        if (renderedNode) {
          fragment.appendChild(renderedNode);
        }
      }

      treeRoot.replaceChildren(fragment);

      let treeChanged = false;
      for (const notebookEntry of notebookLookup.values()) {
        const placed = placeNotebookInDefaultFolder(
          treeRoot,
          notebookEntry.element,
          notebookEntry.defaultFolderPath
        );
        if (!placed) {
          treeRoot.appendChild(notebookEntry.element);
        }
        treeChanged = true;
      }

      if (treeChanged) {
        persistNotebookTree();
      }
    } else {
      persistNotebookTree();
    }

    syncRootUnassignedFolder();
    updateFolderCounts(root);
    updateNotebookSectionCount();
  }

  return {
    clearDragState,
    clearDropTargets,
    createFolderNode,
    deleteTreeFolder,
    directChildrenContainer,
    dropTargetAcceptsNotebookDrop,
    ensureRootUnassignedFolder,
    folderCanDelete,
    folderCanEdit,
    folderLabel,
    initializeNotebookTree,
    persistNotebookTree,
    resolveAddTarget,
    resolveDropTarget,
    rootUnassignedFolder,
    syncRootUnassignedFolder,
    updateFolderCounts,
    updateNotebookSectionCount,
    notebookDefaultFolderPath,
  };
}
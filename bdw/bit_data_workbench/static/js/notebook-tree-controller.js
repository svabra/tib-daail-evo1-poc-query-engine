export function createNotebookTreeController(helpers) {
  const {
    applySidebarSearchFilter,
    clearDragState,
    clearDropTargets,
    createFolderNode,
    createNotebook,
    defaultFolderPermissions,
    deleteTreeFolder,
    deriveFolderId,
    dropTargetAcceptsNotebookDrop,
    folderCanDelete,
    folderCanEdit,
    folderIsShared,
    folderLabel,
    getDraggedNotebook,
    isUnassignedFolder,
    notebookTreeRoot,
    persistNotebookTree,
    refreshSidebar,
    resolveAddTarget,
    resolveDropTarget,
    resolveNotebookCreateTarget,
    setFolderShared,
    setDraggedNotebook,
    setSharedNotebookFolderVisibility,
    showConfirmDialog,
    showFolderNameDialog,
    syncRootUnassignedFolder,
    unassignedFolderName,
    updateFolderCounts,
    upsertSharedNotebookFolder,
  } = helpers;

  async function handleCreateNotebookClick(event) {
    const createNotebookButton = event.target.closest("[data-create-notebook]");
    if (!createNotebookButton) {
      return false;
    }

    event.preventDefault();
    event.stopPropagation();
    if (!notebookTreeRoot()) {
      await refreshSidebar("notebook");
    }

    const target = resolveNotebookCreateTarget(createNotebookButton);
    await createNotebook(target);
    return true;
  }

  async function handleRenameFolderClick(event) {
    const renameFolderButton = event.target.closest("[data-rename-tree-folder]");
    if (!renameFolderButton) {
      return false;
    }

    event.preventDefault();
    event.stopPropagation();

    const folder = renameFolderButton.closest("[data-tree-folder]");
    const label = folderLabel(folder);
    if (!folder || !label || !folderCanEdit(folder)) {
      return true;
    }

    const nextName = await showFolderNameDialog({
      title: "Rename folder",
      copy: "Update the folder name used in the notebook tree.",
      submitLabel: "Rename",
      initialValue: label.textContent?.trim() ?? "",
    });
    if (!nextName) {
      return true;
    }

    label.textContent = nextName;
    const summary = folder.querySelector(":scope > summary");
    if (summary) {
      summary.dataset.searchableItem = nextName;
    }
    persistNotebookTree();
    try {
      await upsertSharedNotebookFolder(folder);
    } catch (error) {
      console.error("Failed to save renamed notebook folder metadata.", error);
    }
    applySidebarSearchFilter();
    return true;
  }

  async function handleDeleteFolderClick(event) {
    const deleteFolderButton = event.target.closest("[data-delete-tree-folder]");
    if (!deleteFolderButton) {
      return false;
    }

    event.preventDefault();
    event.stopPropagation();

    const folder = deleteFolderButton.closest("[data-tree-folder]");
    if (!folder || !folderCanDelete(folder)) {
      return true;
    }

    const label = folderLabel(folder)?.textContent?.trim() ?? "this folder";
    const isDeletingUnassignedFolder = isUnassignedFolder(folder);
    const { confirmed, optionChecked } = await showConfirmDialog({
      title: "Delete folder",
      copy: isDeletingUnassignedFolder
        ? `Delete "${label}"? All notebooks in this folder will be moved to the notebook tree root.`
        : `Delete "${label}"? All notebooks in this folder will be moved to "${unassignedFolderName}" at the bottom of the notebook tree.`,
      confirmLabel: "Delete folder",
      option: {
        label: "Delete this folder recursively, including nested folders and notebooks.",
        checkedCopy: `Delete "${label}" recursively? All nested folders and notebooks in this subtree will be permanently removed from this browser workspace.`,
        checkedConfirmLabel: "Delete recursively",
      },
    });
    if (!confirmed) {
      return true;
    }

    await deleteTreeFolder(folder, { recursive: optionChecked });
    return true;
  }

  async function handleAddFolderClick(event) {
    const addButton = event.target.closest("[data-add-tree-item]");
    if (!addButton) {
      return false;
    }

    event.preventDefault();
    event.stopPropagation();

    const folderName = await showFolderNameDialog({
      title: "New folder",
      copy: "Enter a name for the new notebook folder.",
      submitLabel: "Create folder",
    });
    if (!folderName) {
      return true;
    }

    const target = resolveAddTarget(addButton);
    if (!target) {
      return true;
    }

    const parentFolder = addButton.closest("[data-tree-folder]");
    const nextFolderId = deriveFolderId(folderName, parentFolder?.dataset.folderId || "");
    const nextFolderPolicy = defaultFolderPermissions(nextFolderId);

    const folder = createFolderNode(folderName, {
      open: true,
      folderId: nextFolderId,
      canEdit: nextFolderPolicy.canEdit,
      canDelete: nextFolderPolicy.canDelete,
      isShared: false,
    });
    target.appendChild(folder);
    updateFolderCounts();
    persistNotebookTree();
    try {
      await upsertSharedNotebookFolder(folder, { isPublic: false });
    } catch (error) {
      console.error("Failed to save new notebook folder metadata.", error);
    }
    applySidebarSearchFilter();
    return true;
  }

  async function handleToggleFolderSharedClick(event) {
    const toggleButton = event.target.closest("[data-toggle-folder-shared]");
    if (!toggleButton) {
      return false;
    }

    event.preventDefault();
    event.stopPropagation();

    const folder = toggleButton.closest("[data-tree-folder]");
    if (!folder || toggleButton.disabled || !folderCanEdit(folder)) {
      return true;
    }

    const previousValue = folderIsShared(folder);
    const nextValue = !previousValue;
    setFolderShared(folder, nextValue);
    persistNotebookTree();

    try {
      await setSharedNotebookFolderVisibility(folder, nextValue);
    } catch (error) {
      setFolderShared(folder, previousValue);
      persistNotebookTree();
      console.error("Failed to update notebook folder visibility.", error);
      window.alert("The folder visibility could not be saved.");
      return true;
    }

    return true;
  }

  function handleNotebookDragStart(event) {
    const notebook = event.target.closest("[data-draggable-notebook]");
    if (!notebook) {
      return false;
    }

    if (notebook.dataset.canEdit === "false") {
      event.preventDefault();
      return true;
    }

    setDraggedNotebook(notebook);
    notebook.classList.add("is-dragging");

    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData("text/plain", notebook.dataset.notebookId ?? "");
    }

    return true;
  }

  function handleNotebookDragOver(event) {
    const draggedNotebook = getDraggedNotebook();
    if (!draggedNotebook) {
      return false;
    }

    const dropTarget = resolveDropTarget(event.target);
    if (!dropTarget || !dropTargetAcceptsNotebookDrop(dropTarget)) {
      return false;
    }

    event.preventDefault();
    clearDropTargets();
    dropTarget.classList.add("is-drag-over");

    const folder = dropTarget.closest("[data-tree-folder]");
    if (folder) {
      folder.open = true;
      folder.classList.add("is-drag-over");
    }

    return true;
  }

  function handleNotebookDrop(event) {
    const draggedNotebook = getDraggedNotebook();
    if (!draggedNotebook) {
      return false;
    }

    const dropTarget = resolveDropTarget(event.target);
    if (!dropTarget || !dropTargetAcceptsNotebookDrop(dropTarget)) {
      return false;
    }

    event.preventDefault();
    dropTarget.appendChild(draggedNotebook);
    clearDragState();
    updateFolderCounts();
    syncRootUnassignedFolder();
    persistNotebookTree();
    setDraggedNotebook(null);
    return true;
  }

  function handleNotebookDragEnd() {
    clearDragState();
    setDraggedNotebook(null);
  }

  function handleNotebookTreeToggle(event) {
    const folder = event.target;
    if (!(folder instanceof HTMLDetailsElement) || !folder.matches("[data-tree-folder]")) {
      return false;
    }

    persistNotebookTree();
    return true;
  }

  return {
    handleAddFolderClick,
    handleCreateNotebookClick,
    handleDeleteFolderClick,
    handleNotebookDragEnd,
    handleNotebookDragOver,
    handleNotebookDragStart,
    handleNotebookDrop,
    handleNotebookTreeToggle,
    handleToggleFolderSharedClick,
    handleRenameFolderClick,
  };
}

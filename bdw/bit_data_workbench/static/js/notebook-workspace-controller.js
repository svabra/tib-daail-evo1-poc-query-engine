export function createNotebookWorkspaceController(helpers) {
  const {
    activateNotebookLink,
    addCell,
    autosizeEditor,
    closeCellActionMenus,
    closeCellSourcePicker,
    closeWorkspaceActionMenus,
    copyNotebook,
    deleteCell,
    deleteNotebook,
    duplicateCell,
    focusNotebookMetadata,
    formatCellSql,
    loadNotebookVersion,
    moveCell,
    notebookMetadata,
    renameNotebook,
    revealNotebookLink,
    saveNotebookVersion,
    restartPythonKernel,
    setActiveCell,
    setCellDataSources,
    setCellLanguage,
    setCellSql,
    setNotebookSummary,
    setNotebookTags,
    setSummaryEditing,
    setTagControlsOpen,
    setVersionPanelExpanded,
    shareNotebook,
    showMessageDialog,
    unshareNotebook,
    workspaceNotebookId,
    writeLastNotebookId,
  } = helpers;

  function notebookContext(target) {
    const workspaceRoot = target?.closest?.("[data-workspace-notebook]") ?? null;
    const notebookId = workspaceNotebookId(workspaceRoot);
    return {
      notebookId,
      workspaceRoot,
    };
  }

  function notebookCellContext(target) {
    const context = notebookContext(target);
    return {
      ...context,
      cellId: target?.closest?.("[data-query-cell]")?.dataset.cellId ?? "",
    };
  }

  function addNotebookTag(metaRoot, rawValue) {
    const notebookId = metaRoot?.dataset.notebookId;
    if (!notebookId || metaRoot?.dataset.canEdit === "false") {
      return false;
    }

    const nextTag = String(rawValue ?? "").trim();
    if (!nextTag) {
      return true;
    }

    setNotebookTags(notebookId, [...notebookMetadata(notebookId).tags, nextTag]);
    setTagControlsOpen(metaRoot, false);
    return true;
  }

  async function handleClick(event) {
    const tagToggleButton = event.target.closest("[data-tag-toggle]");
    if (tagToggleButton) {
      event.preventDefault();

      const metaRoot = tagToggleButton.closest("[data-notebook-meta]");
      if (!metaRoot || metaRoot.dataset.canEdit === "false") {
        return true;
      }

      const controls = metaRoot.querySelector("[data-tag-controls]");
      setTagControlsOpen(metaRoot, controls?.hidden ?? true);
      return true;
    }

    const tagAddButton = event.target.closest("[data-tag-add]");
    if (tagAddButton) {
      event.preventDefault();

      const metaRoot = tagAddButton.closest("[data-notebook-meta]");
      const input = metaRoot?.querySelector("[data-tag-input]");
      if (input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement) {
        addNotebookTag(metaRoot, input.value);
        input.value = "";
        return true;
      }

      return true;
    }

    const tagChip = event.target.closest("[data-tag-remove]");
    if (tagChip) {
      event.preventDefault();

      const metaRoot = tagChip.closest("[data-notebook-meta]");
      const notebookId = metaRoot?.dataset.notebookId;
      const tagValue = tagChip.dataset.tagRemove;
      if (!notebookId || !tagValue || metaRoot?.dataset.canEdit === "false") {
        return true;
      }

      const remainingTags = notebookMetadata(notebookId).tags.filter((tag) => tag !== tagValue);
      setNotebookTags(notebookId, remainingTags);
      return true;
    }

    const summaryDisplay = event.target.closest("[data-summary-display]");
    if (summaryDisplay) {
      const { notebookId, workspaceRoot } = notebookContext(summaryDisplay);
      if (!notebookId || notebookMetadata(notebookId).canEdit === false) {
        return true;
      }

      setSummaryEditing(workspaceRoot, true);
      return true;
    }

    const renameNotebookTrigger = event.target.closest("[data-rename-notebook], [data-rename-notebook-title]");
    if (renameNotebookTrigger) {
      event.preventDefault();
      closeWorkspaceActionMenus();

      const { notebookId } = notebookContext(renameNotebookTrigger);
      if (notebookId) {
        await renameNotebook(notebookId);
      }
      return true;
    }

    const editNotebookButton = event.target.closest("[data-edit-notebook]");
    if (editNotebookButton) {
      event.preventDefault();
      closeWorkspaceActionMenus();

      const { notebookId } = notebookContext(editNotebookButton);
      if (notebookId) {
        focusNotebookMetadata(notebookId);
      }
      return true;
    }

    const copyNotebookButton = event.target.closest("[data-copy-notebook]");
    if (copyNotebookButton) {
      event.preventDefault();
      closeWorkspaceActionMenus();

      const { notebookId } = notebookContext(copyNotebookButton);
      if (notebookId) {
        copyNotebook(notebookId);
      }
      return true;
    }

    const restartPythonKernelButton = event.target.closest("[data-restart-python-kernel]");
    if (restartPythonKernelButton) {
      event.preventDefault();
      closeWorkspaceActionMenus();

      const { notebookId } = notebookContext(restartPythonKernelButton);
      if (notebookId) {
        await restartPythonKernel(notebookId);
      }
      return true;
    }

    const addCellButton = event.target.closest("[data-add-cell]");
    if (addCellButton) {
      event.preventDefault();
      closeCellActionMenus();

      const { notebookId } = notebookContext(addCellButton);
      if (notebookId) {
        addCell(notebookId);
      }
      return true;
    }

    const addCellAfterButton = event.target.closest("[data-add-cell-after]");
    if (addCellAfterButton) {
      event.preventDefault();
      closeCellActionMenus();

      const { notebookId, cellId } = notebookCellContext(addCellAfterButton);
      if (notebookId && cellId) {
        addCell(notebookId, cellId);
      }
      return true;
    }

    const setCellLanguageButton = event.target.closest("[data-set-cell-language]");
    if (setCellLanguageButton) {
      event.preventDefault();
      closeCellActionMenus();

      const nextLanguage = setCellLanguageButton.dataset.setCellLanguage;
      const { notebookId, cellId } = notebookCellContext(setCellLanguageButton);
      if (notebookId && cellId && nextLanguage) {
        setCellLanguage(notebookId, cellId, nextLanguage);
      }
      return true;
    }

    const formatCellSqlButton = event.target.closest("[data-format-cell-sql]");
    if (formatCellSqlButton) {
      event.preventDefault();
      closeCellActionMenus();

      const { notebookId, cellId } = notebookCellContext(formatCellSqlButton);
      if (notebookId && cellId) {
        formatCellSql(notebookId, cellId);
      }
      return true;
    }

    const copyCellButton = event.target.closest("[data-copy-cell]");
    if (copyCellButton) {
      event.preventDefault();
      closeCellActionMenus();

      const { notebookId, cellId } = notebookCellContext(copyCellButton);
      if (notebookId && cellId) {
        duplicateCell(notebookId, cellId);
      }
      return true;
    }

    const moveCellUpButton = event.target.closest("[data-move-cell-up]");
    if (moveCellUpButton) {
      event.preventDefault();
      closeCellActionMenus();

      const { notebookId, cellId } = notebookCellContext(moveCellUpButton);
      if (notebookId && cellId) {
        moveCell(notebookId, cellId, "up");
      }
      return true;
    }

    const moveCellDownButton = event.target.closest("[data-move-cell-down]");
    if (moveCellDownButton) {
      event.preventDefault();
      closeCellActionMenus();

      const { notebookId, cellId } = notebookCellContext(moveCellDownButton);
      if (notebookId && cellId) {
        moveCell(notebookId, cellId, "down");
      }
      return true;
    }

    const deleteCellButton = event.target.closest("[data-delete-cell]");
    if (deleteCellButton) {
      event.preventDefault();
      closeCellActionMenus();

      const { notebookId, cellId } = notebookCellContext(deleteCellButton);
      if (notebookId && cellId) {
        deleteCell(notebookId, cellId);
      }
      return true;
    }

    const deleteNotebookButton = event.target.closest("[data-delete-notebook]");
    if (deleteNotebookButton) {
      event.preventDefault();
      closeWorkspaceActionMenus();

      const { notebookId } = notebookContext(deleteNotebookButton);
      if (notebookId) {
        await deleteNotebook(notebookId);
      }
      return true;
    }

    const saveVersionButton = event.target.closest("[data-save-version]");
    if (saveVersionButton) {
      event.preventDefault();

      const { notebookId } = notebookContext(saveVersionButton);
      if (notebookId) {
        saveNotebookVersion(notebookId);
      }
      return true;
    }

    const versionToggle = event.target.closest("[data-version-toggle]");
    if (versionToggle) {
      event.preventDefault();

      const metaRoot = versionToggle.closest("[data-notebook-meta]");
      const panel = metaRoot?.querySelector("[data-version-panel]");
      if (metaRoot && panel && !versionToggle.disabled) {
        setVersionPanelExpanded(metaRoot, panel.hidden);
      }
      return true;
    }

    const versionButton = event.target.closest("[data-version-load]");
    if (versionButton) {
      event.preventDefault();

      const { notebookId } = notebookContext(versionButton);
      const versionId = versionButton.dataset.versionId;
      if (notebookId && versionId) {
        await loadNotebookVersion(notebookId, versionId);
      }
      return true;
    }

    return false;
  }

  function handleFocusIn(event) {
    setActiveCell(event.target.closest("[data-query-cell]"));
  }

  function handleInput(event) {
    const summaryInput = event.target.closest("[data-summary-input]");
    if (summaryInput) {
      const { notebookId } = notebookContext(summaryInput);
      if (notebookId) {
        setNotebookSummary(notebookId, summaryInput.value);
      }
      return true;
    }

    const editorSource = event.target.closest("[data-editor-source]");
    if (editorSource) {
      const { notebookId, cellId } = notebookCellContext(editorSource);
      autosizeEditor(editorSource.closest("[data-editor-root]"));
      if (notebookId && cellId) {
        setCellSql(notebookId, cellId, editorSource.value);
      }
      return true;
    }

    return false;
  }

  function handleSharedToggleClick(event) {
    const sharedToggle = event.target.closest("[data-notebook-shared-toggle]");
    if (!sharedToggle) {
      return false;
    }

    const { notebookId } = notebookContext(sharedToggle);
    if (!notebookId) {
      return true;
    }

    const nextSharedState = sharedToggle.getAttribute("aria-pressed") !== "true";
    sharedToggle.classList.toggle("is-on", nextSharedState);
    sharedToggle.setAttribute("aria-pressed", nextSharedState ? "true" : "false");
    sharedToggle.disabled = true;

    const action = nextSharedState ? shareNotebook(notebookId) : unshareNotebook(notebookId);
    action
      .catch(async (error) => {
        console.error("Failed to toggle shared notebook state.", error);
        sharedToggle.classList.toggle("is-on", !nextSharedState);
        sharedToggle.setAttribute("aria-pressed", !nextSharedState ? "true" : "false");
        await showMessageDialog({
          title: "Notebook sharing failed",
          copy: "The notebook could not be updated for shared access.",
        });
      })
      .finally(() => {
        sharedToggle.disabled = false;
      });

    return true;
  }

  function handleChange(event) {
    const sourceOption = event.target.closest("[data-cell-source-option]");
    if (!sourceOption) {
      return false;
    }

    const { notebookId, cellId } = notebookCellContext(sourceOption);
    const metaRoot = sourceOption.closest("[data-notebook-meta]");
    const cellRoot = sourceOption.closest("[data-query-cell]");
    if (!notebookId || !cellId || !cellRoot || metaRoot?.dataset.canEdit === "false") {
      return true;
    }

    const selectedSources = Array.from(cellRoot.querySelectorAll("[data-cell-source-option]:checked")).map(
      (option) => option.value
    );
    setCellDataSources(notebookId, cellId, selectedSources);
    closeCellSourcePicker(cellRoot);
    return true;
  }

  function handleSummaryFocusOut(event) {
    const summaryInput = event.target.closest("[data-summary-input]");
    if (!summaryInput) {
      return false;
    }

    const container = summaryInput.closest("[data-summary-container]");
    const nextFocused = event.relatedTarget;
    if (container && nextFocused instanceof Node && container.contains(nextFocused)) {
      return true;
    }

    container?.classList.remove("is-editing");
    return true;
  }

  function handleSummaryEscapeKeydown(event) {
    if (event.key !== "Escape") {
      return false;
    }

    const summaryInput = event.target.closest("[data-summary-input]");
    if (!summaryInput) {
      return false;
    }

    const container = summaryInput.closest("[data-summary-container]");
    container?.classList.remove("is-editing");
    summaryInput.blur();
    return true;
  }

  function handleTagInputKeydown(event) {
    if (event.key !== "Enter") {
      return false;
    }

    const input = event.target.closest("[data-tag-input]");
    const metaRoot = input?.closest("[data-notebook-meta]");
    if (!input || !metaRoot) {
      return false;
    }

    if (metaRoot.dataset.canEdit === "false") {
      return true;
    }

    event.preventDefault();
    const handled = addNotebookTag(metaRoot, input.value);
    if (handled) {
      input.value = "";
    }
    return true;
  }

  async function handleRenameTitleKeydown(event) {
    if (event.key !== "Enter" && event.key !== " ") {
      return false;
    }

    const titleTrigger = event.target.closest("[data-rename-notebook-title]");
    if (!titleTrigger) {
      return false;
    }

    const { notebookId } = notebookContext(titleTrigger);
    if (!notebookId || notebookMetadata(notebookId).canEdit === false) {
      return true;
    }

    event.preventDefault();
    await renameNotebook(notebookId);
    return true;
  }

  function syncActiveNotebookSelection(event) {
    const { notebookId } = notebookContext(event.target);
    if (!notebookId) {
      return;
    }

    activateNotebookLink(notebookId);
    revealNotebookLink(notebookId);
    writeLastNotebookId(notebookId);
  }

  return {
    handleChange,
    handleClick,
    handleFocusIn,
    handleInput,
    handleRenameTitleKeydown,
    handleSharedToggleClick,
    handleSummaryEscapeKeydown,
    handleSummaryFocusOut,
    handleTagInputKeydown,
    syncActiveNotebookSelection,
  };
}

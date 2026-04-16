import {
  ensureLocalWorkspaceMoveDialog,
  ensureLocalWorkspaceSaveDialog,
  localWorkspaceMoveDialog,
  localWorkspaceSaveDialog,
} from "./dialogs.js";
import {
  ensureResultExportFileNameExtension,
  normalizeResultExportFormat,
} from "./data-exporters/export-format-definitions.js";
import {
  defaultResultExportSettings,
  normalizeResultExportSettings,
  readResultExportSettings,
  renderResultExportSettings,
} from "./data-exporters/export-settings.js";

export function createLocalWorkspaceDialogController(helpers) {
  const {
    allLocalWorkspaceFolderPaths,
    closestExistingLocalWorkspaceFolderPath,
    createLocalWorkspaceFolder,
    defaultQueryResultExportFilename,
    getEntryIdFromSourceObject,
    getLocalWorkspaceExport,
    getMoveState,
    getSaveState,
    listLocalWorkspaceExports,
    localWorkspaceDisplayPath,
    localWorkspaceFolderListMarkup,
    localWorkspaceMoveFolderListMarkup,
    normalizeLocalWorkspaceFolderPath,
    renderLocalWorkspaceMoveBreadcrumbs,
    renderLocalWorkspaceSaveBreadcrumbs,
  } = helpers;

  function localWorkspaceSaveFolderListRoot() {
    return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-folder-list]") ?? null;
  }

  function localWorkspaceSaveSelectedPathNode() {
    return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-selected-path]") ?? null;
  }

  function localWorkspaceSaveFolderPathInput() {
    return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-folder-path]") ?? null;
  }

  function localWorkspaceSaveFileNameInput() {
    return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-file-name]") ?? null;
  }

  function localWorkspaceSaveSubmitButton() {
    return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-save-submit]") ?? null;
  }

  function syncLocalWorkspaceSaveDialogState() {
    const dialog = localWorkspaceSaveDialog();
    if (!dialog) {
      return;
    }

    const state = getSaveState();
    state.folderPath = normalizeLocalWorkspaceFolderPath(state.folderPath);
    renderLocalWorkspaceSaveBreadcrumbs(state.folderPath);

    const selectedPathNode = localWorkspaceSaveSelectedPathNode();
    if (selectedPathNode) {
      selectedPathNode.textContent = localWorkspaceDisplayPath(state.folderPath);
    }

    const folderPathInput = localWorkspaceSaveFolderPathInput();
    if (folderPathInput && folderPathInput.value !== state.folderPath) {
      folderPathInput.value = state.folderPath;
    }

    const fileNameInput = localWorkspaceSaveFileNameInput();
    if (fileNameInput && fileNameInput.value !== state.fileName) {
      fileNameInput.value = state.fileName;
    }

    const formatSelect = dialog.querySelector("[data-export-format-select]");
    if (formatSelect instanceof HTMLSelectElement && formatSelect.value !== state.exportFormat) {
      formatSelect.value = state.exportFormat;
    }
    renderResultExportSettings(dialog, state.exportFormat, state.exportSettings);

    const submitButton = localWorkspaceSaveSubmitButton();
    if (submitButton) {
      submitButton.disabled = state.saving || !String(state.fileName || "").trim();
      submitButton.textContent = state.saving ? "Saving..." : "Save to Local Workspace (IndexDB)";
    }

    dialog.querySelectorAll("[data-local-workspace-folder-option]").forEach((node) => {
      const selected =
        normalizeLocalWorkspaceFolderPath(node.dataset.localWorkspaceFolderPath || "") ===
        state.folderPath;
      node.classList.toggle("is-selected", selected);
    });
  }

  function setLocalWorkspaceSaveDialogBusy(busy) {
    const state = getSaveState();
    state.saving = busy;
    const dialog = localWorkspaceSaveDialog();
    if (dialog) {
      const createFolderButton = dialog.querySelector("[data-local-workspace-create-folder]");
      if (createFolderButton instanceof HTMLButtonElement) {
        createFolderButton.disabled = busy;
      }

      const folderPathInput = localWorkspaceSaveFolderPathInput();
      if (folderPathInput instanceof HTMLInputElement) {
        folderPathInput.disabled = busy;
      }

      const fileNameInput = localWorkspaceSaveFileNameInput();
      if (fileNameInput instanceof HTMLInputElement) {
        fileNameInput.disabled = busy;
      }
    }

    syncLocalWorkspaceSaveDialogState();
  }

  async function renderLocalWorkspaceSaveFolderList() {
    const root = localWorkspaceSaveFolderListRoot();
    if (!(root instanceof Element)) {
      return;
    }

    const state = getSaveState();
    const entries = await listLocalWorkspaceExports();
    const folderPaths = allLocalWorkspaceFolderPaths([
      ...entries.map((entry) => entry.folderPath),
      ...state.createdFolderPaths,
    ]);
    root.innerHTML = localWorkspaceFolderListMarkup(folderPaths);
    syncLocalWorkspaceSaveDialogState();
  }

  async function createLocalWorkspaceFolderFromDialog() {
    const state = getSaveState();
    const createdPath = await createLocalWorkspaceFolder(state.folderPath, {
      confirmCreation: false,
      showSidebarStatus: false,
      revealSidebar: false,
    });
    if (createdPath) {
      state.folderPath = createdPath;
      await renderLocalWorkspaceSaveFolderList();
    }
  }

  async function openLocalWorkspaceSaveDialog(job, exportFormat) {
    if (!job?.jobId || !job?.columns?.length) {
      return;
    }

    const dialog = ensureLocalWorkspaceSaveDialog();
    const state = getSaveState();
    state.jobId = job.jobId;
    state.exportFormat = normalizeResultExportFormat(exportFormat);
    state.exportSettings = defaultResultExportSettings(state.exportFormat);
    state.fileName = defaultQueryResultExportFilename(job, state.exportFormat);
    state.folderPath = "";
    state.saving = false;
    state.createdFolderPaths = [];

    const titleNode = dialog.querySelector("[data-local-workspace-save-title]");
    const copyNode = dialog.querySelector("[data-local-workspace-save-copy]");
    if (titleNode) {
      titleNode.textContent = "Save Results in Local Workspace (IndexDB) ...";
    }
    if (copyNode) {
      copyNode.textContent =
        "Choose a Local Workspace (IndexDB) folder path, then select the export format and format-specific settings.";
    }

    syncLocalWorkspaceSaveDialogState();
    dialog.showModal();
    await renderLocalWorkspaceSaveFolderList();
  }

  function localWorkspaceMoveFolderListRoot() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-folder-list]") ?? null;
  }

  function localWorkspaceMoveSelectedPathNode() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-selected-path]") ?? null;
  }

  function localWorkspaceMoveFolderPathInput() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-folder-path]") ?? null;
  }

  function localWorkspaceMoveFileNameInput() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-file-name]") ?? null;
  }

  function localWorkspaceMoveSubmitButton() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-submit]") ?? null;
  }

  function syncLocalWorkspaceMoveDialogState() {
    const dialog = localWorkspaceMoveDialog();
    if (!dialog) {
      return;
    }

    const state = getMoveState();
    state.folderPath = normalizeLocalWorkspaceFolderPath(state.folderPath);
    renderLocalWorkspaceMoveBreadcrumbs(state.folderPath);

    const selectedPathNode = localWorkspaceMoveSelectedPathNode();
    if (selectedPathNode) {
      selectedPathNode.textContent = localWorkspaceDisplayPath(state.folderPath, state.fileName);
    }

    const folderPathInput = localWorkspaceMoveFolderPathInput();
    if (folderPathInput && folderPathInput.value !== state.folderPath) {
      folderPathInput.value = state.folderPath;
    }

    const fileNameInput = localWorkspaceMoveFileNameInput();
    if (fileNameInput && fileNameInput.value !== state.fileName) {
      fileNameInput.value = state.fileName;
    }

    const submitButton = localWorkspaceMoveSubmitButton();
    if (submitButton) {
      submitButton.disabled = state.moving || !String(state.fileName || "").trim();
      submitButton.textContent = state.moving ? "Moving..." : "Move file";
    }

    dialog.querySelectorAll("[data-local-workspace-move-folder-option]").forEach((node) => {
      const selected =
        normalizeLocalWorkspaceFolderPath(node.dataset.localWorkspaceFolderPath || "") ===
        state.folderPath;
      node.classList.toggle("is-selected", selected);
    });
  }

  function setLocalWorkspaceMoveDialogBusy(busy) {
    const state = getMoveState();
    state.moving = busy;
    const dialog = localWorkspaceMoveDialog();
    if (dialog) {
      const createFolderButton = dialog.querySelector("[data-local-workspace-move-create-folder]");
      if (createFolderButton instanceof HTMLButtonElement) {
        createFolderButton.disabled = busy;
      }

      const folderPathInput = localWorkspaceMoveFolderPathInput();
      if (folderPathInput instanceof HTMLInputElement) {
        folderPathInput.disabled = busy;
      }

      const fileNameInput = localWorkspaceMoveFileNameInput();
      if (fileNameInput instanceof HTMLInputElement) {
        fileNameInput.disabled = busy;
      }
    }

    syncLocalWorkspaceMoveDialogState();
  }

  async function renderLocalWorkspaceMoveFolderList() {
    const root = localWorkspaceMoveFolderListRoot();
    if (!(root instanceof Element)) {
      return;
    }

    const state = getMoveState();
    const entries = await listLocalWorkspaceExports();
    const folderPaths = allLocalWorkspaceFolderPaths([
      ...entries.map((entry) => entry.folderPath),
      ...state.createdFolderPaths,
    ]);
    root.innerHTML = localWorkspaceMoveFolderListMarkup(folderPaths);
    syncLocalWorkspaceMoveDialogState();
  }

  async function createLocalWorkspaceFolderFromMoveDialog() {
    const state = getMoveState();
    const createdPath = await createLocalWorkspaceFolder(state.folderPath, {
      confirmCreation: false,
      showSidebarStatus: false,
      revealSidebar: false,
    });
    if (createdPath) {
      state.folderPath = createdPath;
      await renderLocalWorkspaceMoveFolderList();
    }
  }

  async function openLocalWorkspaceMoveDialog(sourceObjectRoot) {
    const entryId = getEntryIdFromSourceObject(sourceObjectRoot);
    if (!entryId) {
      return false;
    }

    const entry = await getLocalWorkspaceExport(entryId);
    if (!entry) {
      return false;
    }

    const dialog = ensureLocalWorkspaceMoveDialog();
    const state = getMoveState();
    state.entryId = entry.id;
    state.fileName = entry.fileName;
    state.folderPath = normalizeLocalWorkspaceFolderPath(entry.folderPath);
    state.moving = false;
    state.createdFolderPaths = [];

    const titleNode = dialog.querySelector("[data-local-workspace-move-title]");
    const copyNode = dialog.querySelector("[data-local-workspace-move-copy]");
    if (titleNode) {
      titleNode.textContent = "Move Local Workspace file";
    }
    if (copyNode) {
      copyNode.textContent = `Move ${entry.fileName} to another Local Workspace folder in this browser and optionally rename it.`;
    }

    syncLocalWorkspaceMoveDialogState();
    dialog.showModal();
    await renderLocalWorkspaceMoveFolderList();
    return true;
  }

  async function syncOpenLocalWorkspaceSaveDialog() {
    const dialog = localWorkspaceSaveDialog();
    if (!(dialog instanceof HTMLDialogElement) || !dialog.open) {
      return;
    }

    const state = getSaveState();
    const entries = await listLocalWorkspaceExports();
    const folderPaths = allLocalWorkspaceFolderPaths(entries.map((entry) => entry.folderPath));
    state.folderPath = closestExistingLocalWorkspaceFolderPath(state.folderPath, folderPaths);
    state.createdFolderPaths = [];
    await renderLocalWorkspaceSaveFolderList();
  }

  async function syncOpenLocalWorkspaceMoveDialog() {
    const dialog = localWorkspaceMoveDialog();
    if (!(dialog instanceof HTMLDialogElement) || !dialog.open) {
      return;
    }

    const state = getMoveState();
    const entries = await listLocalWorkspaceExports();
    const folderPaths = allLocalWorkspaceFolderPaths(entries.map((entry) => entry.folderPath));
    state.folderPath = closestExistingLocalWorkspaceFolderPath(state.folderPath, folderPaths);
    state.createdFolderPaths = [];
    await renderLocalWorkspaceMoveFolderList();
  }

  function updateLocalWorkspaceSaveFolderPath(value) {
    getSaveState().folderPath = normalizeLocalWorkspaceFolderPath(value);
    syncLocalWorkspaceSaveDialogState();
  }

  function updateLocalWorkspaceSaveFileName(value) {
    getSaveState().fileName = String(value ?? "");
    syncLocalWorkspaceSaveDialogState();
  }

  function updateLocalWorkspaceSaveExportFormat(value) {
    const state = getSaveState();
    state.exportFormat = normalizeResultExportFormat(value);
    state.exportSettings = defaultResultExportSettings(state.exportFormat);
    state.fileName = ensureResultExportFileNameExtension(
      state.fileName,
      state.exportFormat,
      "query-result"
    );
    syncLocalWorkspaceSaveDialogState();
  }

  function updateLocalWorkspaceSaveExportSettingsFromDialog() {
    const dialog = localWorkspaceSaveDialog();
    if (!(dialog instanceof HTMLDialogElement)) {
      return;
    }
    const state = getSaveState();
    state.exportSettings = normalizeResultExportSettings(
      state.exportFormat,
      readResultExportSettings(dialog, state.exportFormat)
    );
  }

  function updateLocalWorkspaceMoveFolderPath(value) {
    getMoveState().folderPath = normalizeLocalWorkspaceFolderPath(value);
    syncLocalWorkspaceMoveDialogState();
  }

  function updateLocalWorkspaceMoveFileName(value) {
    getMoveState().fileName = String(value ?? "");
    syncLocalWorkspaceMoveDialogState();
  }

  return {
    createLocalWorkspaceFolderFromDialog,
    createLocalWorkspaceFolderFromMoveDialog,
    openLocalWorkspaceMoveDialog,
    openLocalWorkspaceSaveDialog,
    renderLocalWorkspaceMoveFolderList,
    renderLocalWorkspaceSaveFolderList,
    setLocalWorkspaceMoveDialogBusy,
    setLocalWorkspaceSaveDialogBusy,
    syncLocalWorkspaceMoveDialogState,
    syncLocalWorkspaceSaveDialogState,
    syncOpenLocalWorkspaceMoveDialog,
    syncOpenLocalWorkspaceSaveDialog,
    updateLocalWorkspaceMoveFileName,
    updateLocalWorkspaceMoveFolderPath,
    updateLocalWorkspaceSaveExportFormat,
    updateLocalWorkspaceSaveExportSettingsFromDialog,
    updateLocalWorkspaceSaveFileName,
    updateLocalWorkspaceSaveFolderPath,
  };
}

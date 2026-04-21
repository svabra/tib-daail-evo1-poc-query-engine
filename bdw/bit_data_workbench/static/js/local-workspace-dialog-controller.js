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
import { createS3ExplorerLoader, s3ExplorerPath } from "./s3-explorer-loader.js";

export function createLocalWorkspaceDialogController(helpers) {
  const {
    allLocalWorkspaceFolderPaths,
    closestExistingLocalWorkspaceFolderPath,
    createLocalWorkspaceFolder,
    currentWorkspaceMode,
    defaultQueryResultExportFilename,
    fetchJsonOrThrow,
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
    renderS3ExplorerChildrenMarkup,
    refreshSidebar,
    showConfirmDialog,
    showFolderNameDialog,
  } = helpers;

  const moveS3ExplorerNodeRequests = new Map();
  let moveS3ExplorerLoadPromise = null;

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

  function localWorkspaceMoveDestinationSelect() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-destination]") ?? null;
  }

  function localWorkspaceMoveLocalPanel() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-local-panel]") ?? null;
  }

  function localWorkspaceMoveS3Panel() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-s3-panel]") ?? null;
  }

  function localWorkspaceMoveLocalToolbar() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-local-toolbar]") ?? null;
  }

  function localWorkspaceMoveS3Toolbar() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-s3-toolbar]") ?? null;
  }

  function localWorkspaceMoveSelectedLabelNode() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-selected-label]") ?? null;
  }

  function localWorkspaceMoveSelectedPathNode() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-selected-path]") ?? null;
  }

  function localWorkspaceMoveFolderField() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-folder-field]") ?? null;
  }

  function localWorkspaceMoveFolderPathInput() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-folder-path]") ?? null;
  }

  function localWorkspaceMoveFileNameInput() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-file-name]") ?? null;
  }

  function localWorkspaceMoveActionCopyNode() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-action-copy]") ?? null;
  }

  function localWorkspaceMoveS3BreadcrumbRoot() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-s3-breadcrumbs]") ?? null;
  }

  function localWorkspaceMoveS3TreeRoot() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-s3-tree]") ?? null;
  }

  function localWorkspaceMoveSubmitButton() {
    return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-submit]") ?? null;
  }

  function localWorkspaceMoveS3NodeKey(kind, bucket, prefix = "") {
    return `${String(kind || "").trim()}:${String(bucket || "").trim()}:${String(prefix || "").trim()}`;
  }

  function buildMoveS3Breadcrumbs(bucket, prefix = "") {
    const breadcrumbs = [{ label: "Buckets", bucket: "", prefix: "" }];
    const normalizedBucket = String(bucket || "").trim();
    if (!normalizedBucket) {
      return breadcrumbs;
    }

    breadcrumbs.push({
      label: normalizedBucket,
      bucket: normalizedBucket,
      prefix: "",
    });

    let currentPrefix = "";
    for (const segment of String(prefix || "").split("/").filter(Boolean)) {
      currentPrefix = currentPrefix ? `${currentPrefix}${segment}/` : `${segment}/`;
      breadcrumbs.push({
        label: segment,
        bucket: normalizedBucket,
        prefix: currentPrefix,
      });
    }

    return breadcrumbs;
  }

  function renderLocalWorkspaceMoveS3Breadcrumbs(bucket, prefix = "") {
    const root = localWorkspaceMoveS3BreadcrumbRoot();
    if (!(root instanceof Element)) {
      return;
    }

    root.replaceChildren();
    const breadcrumbs = buildMoveS3Breadcrumbs(bucket, prefix);
    breadcrumbs.forEach((crumb, index) => {
      if (index > 0) {
        const separator = document.createElement("span");
        separator.className = "result-export-breadcrumb-separator";
        separator.textContent = "/";
        root.append(separator);
      }

      const button = document.createElement("button");
      button.type = "button";
      button.className = `result-export-breadcrumb${index === breadcrumbs.length - 1 ? " is-current" : ""}`;
      button.dataset.localWorkspaceMoveS3Breadcrumb = "true";
      button.dataset.localWorkspaceMoveS3Bucket = crumb.bucket;
      button.dataset.localWorkspaceMoveS3Prefix = crumb.prefix;
      button.textContent = crumb.label;
      root.append(button);
    });
  }

  const {
    loadS3ExplorerNode: loadLocalWorkspaceMoveS3ExplorerNode,
    loadS3ExplorerRoot: loadLocalWorkspaceMoveS3ExplorerRoot,
    revealS3ExplorerLocation: revealLocalWorkspaceMoveS3Location,
  } = createS3ExplorerLoader({
    fetchJsonOrThrow,
    getQueryRoot: localWorkspaceMoveDialog,
    getResultExportTreeRoot: localWorkspaceMoveS3TreeRoot,
    nodeRequests: moveS3ExplorerNodeRequests,
    renderChildrenMarkup: renderS3ExplorerChildrenMarkup,
    selectResultExportLocation: updateLocalWorkspaceMoveS3Location,
    syncResultExportSelectionState: syncLocalWorkspaceMoveDialogState,
    s3ExplorerNodeKey: localWorkspaceMoveS3NodeKey,
  });

  function resetLocalWorkspaceMoveS3Tree() {
    const treeRoot = localWorkspaceMoveS3TreeRoot();
    if (treeRoot instanceof Element) {
      treeRoot.innerHTML = "";
    }
    moveS3ExplorerNodeRequests.clear();
    moveS3ExplorerLoadPromise = null;
  }

  function localWorkspaceMoveDestinationPath(state) {
    if (state.destinationKind === "s3") {
      const sharedWorkspacePath = s3ExplorerPath(state.selectedBucket, state.selectedPrefix);
      if (!sharedWorkspacePath) {
        return "Select a bucket or folder from the Shared Workspace explorer.";
      }
      return `${sharedWorkspacePath}${String(state.fileName || "").trim()}`;
    }
    return localWorkspaceDisplayPath(state.folderPath, state.fileName);
  }

  function localWorkspaceTransferActionLabel(state) {
    return state.operationKind === "copy" ? "Copy" : "Move";
  }

  function localWorkspaceTransferActionIngLabel(state) {
    return state.operationKind === "copy" ? "Copying" : "Moving";
  }

  async function ensureLocalWorkspaceMoveS3ExplorerLoaded({ force = false } = {}) {
    const dialog = localWorkspaceMoveDialog();
    if (!(dialog instanceof HTMLDialogElement)) {
      return null;
    }

    const state = getMoveState();
    if (moveS3ExplorerLoadPromise && !force) {
      return moveS3ExplorerLoadPromise;
    }
    if (state.s3Loaded && !force) {
      return null;
    }

    state.loadingSharedWorkspace = true;
    syncLocalWorkspaceMoveDialogState();

    const request = loadLocalWorkspaceMoveS3ExplorerRoot({
      preferredBucket: state.selectedBucket,
      preferredPrefix: state.selectedPrefix,
    })
      .then((snapshot) => {
        state.s3Loaded = true;
        state.loadingSharedWorkspace = false;
        state.sharedWorkspaceLoadError = "";
        syncLocalWorkspaceMoveDialogState();
        return snapshot;
      })
      .catch((error) => {
        state.s3Loaded = false;
        state.loadingSharedWorkspace = false;
        state.sharedWorkspaceLoadError =
          error instanceof Error ? error.message : "The Shared Workspace explorer could not be loaded.";
        const treeRoot = localWorkspaceMoveS3TreeRoot();
        if (treeRoot instanceof Element) {
          treeRoot.innerHTML = '<p class="s3-explorer-empty">Could not load Shared Workspace locations.</p>';
        }
        syncLocalWorkspaceMoveDialogState();
        throw error;
      })
      .finally(() => {
        moveS3ExplorerLoadPromise = null;
      });

    moveS3ExplorerLoadPromise = request;
    return request;
  }

  function syncLocalWorkspaceMoveDialogState() {
    const dialog = localWorkspaceMoveDialog();
    if (!dialog) {
      return;
    }

    const state = getMoveState();
    state.folderPath = normalizeLocalWorkspaceFolderPath(state.folderPath);
    state.operationKind = state.operationKind === "copy" ? "copy" : "move";
    state.destinationKind = state.destinationKind === "s3" ? "s3" : "local";
    renderLocalWorkspaceMoveBreadcrumbs(state.folderPath);
    renderLocalWorkspaceMoveS3Breadcrumbs(state.selectedBucket, state.selectedPrefix);

    const movingToSharedWorkspace = state.destinationKind === "s3";
    const actionLabel = localWorkspaceTransferActionLabel(state);
    const actionIngLabel = localWorkspaceTransferActionIngLabel(state);

    const destinationSelect = localWorkspaceMoveDestinationSelect();
    if (destinationSelect instanceof HTMLSelectElement && destinationSelect.value !== state.destinationKind) {
      destinationSelect.value = state.destinationKind;
    }

    const selectedLabelNode = localWorkspaceMoveSelectedLabelNode();
    if (selectedLabelNode) {
      selectedLabelNode.textContent = movingToSharedWorkspace
        ? "Selected Shared Workspace (S3) destination"
        : "Selected Local Workspace (IndexDB) destination";
    }

    const selectedPathNode = localWorkspaceMoveSelectedPathNode();
    if (selectedPathNode) {
      selectedPathNode.textContent = localWorkspaceMoveDestinationPath(state);
    }

    const folderField = localWorkspaceMoveFolderField();
    if (folderField instanceof HTMLElement) {
      folderField.hidden = movingToSharedWorkspace;
    }

    const folderPathInput = localWorkspaceMoveFolderPathInput();
    if (folderPathInput instanceof HTMLInputElement) {
      folderPathInput.disabled = state.moving || movingToSharedWorkspace;
      if (folderPathInput.value !== state.folderPath) {
        folderPathInput.value = state.folderPath;
      }
    }

    const fileNameInput = localWorkspaceMoveFileNameInput();
    if (fileNameInput instanceof HTMLInputElement) {
      fileNameInput.disabled = state.moving;
      if (fileNameInput.value !== state.fileName) {
        fileNameInput.value = state.fileName;
      }
    }

    const actionCopyNode = localWorkspaceMoveActionCopyNode();
    if (actionCopyNode) {
      actionCopyNode.textContent = movingToSharedWorkspace
        ? state.operationKind === "copy"
          ? "Upload the stored file to Shared Workspace (S3) and keep the browser-local copy in IndexedDB."
          : "Upload the stored file to Shared Workspace (S3) and remove it from this browser's Local Workspace."
        : state.operationKind === "copy"
          ? "Create a second Local Workspace file in this browser and keep the current one unchanged."
          : "Move the stored file within this browser's Local Workspace.";
    }

    const localPanel = localWorkspaceMoveLocalPanel();
    if (localPanel instanceof HTMLElement) {
      localPanel.hidden = movingToSharedWorkspace;
    }

    const s3Panel = localWorkspaceMoveS3Panel();
    if (s3Panel instanceof HTMLElement) {
      s3Panel.hidden = !movingToSharedWorkspace;
    }

    const localToolbar = localWorkspaceMoveLocalToolbar();
    if (localToolbar instanceof HTMLElement) {
      localToolbar.hidden = movingToSharedWorkspace;
    }

    const s3Toolbar = localWorkspaceMoveS3Toolbar();
    if (s3Toolbar instanceof HTMLElement) {
      s3Toolbar.hidden = !movingToSharedWorkspace;
    }

    const submitButton = localWorkspaceMoveSubmitButton();
    if (submitButton instanceof HTMLButtonElement) {
      submitButton.disabled =
        state.moving ||
        state.loadingSharedWorkspace ||
        !String(state.fileName || "").trim() ||
        (movingToSharedWorkspace && !String(state.selectedBucket || "").trim());
      submitButton.textContent = state.moving
        ? `${actionIngLabel}...`
        : movingToSharedWorkspace
          ? `${actionLabel} to Shared Workspace`
          : `${actionLabel} file`;
    }

    dialog.querySelectorAll("[data-local-workspace-move-folder-option]").forEach((node) => {
      const selected =
        !movingToSharedWorkspace &&
        normalizeLocalWorkspaceFolderPath(node.dataset.localWorkspaceFolderPath || "") === state.folderPath;
      node.classList.toggle("is-selected", selected);
    });

    dialog.querySelectorAll("[data-s3-explorer-node]").forEach((node) => {
      const selected =
        movingToSharedWorkspace &&
        (node.dataset.s3ExplorerBucket || "") === state.selectedBucket &&
        (node.dataset.s3ExplorerPrefix || "") === state.selectedPrefix;
      node.classList.toggle("is-selected", selected);
    });

    const createLocalFolderButton = dialog.querySelector("[data-local-workspace-move-create-folder]");
    if (createLocalFolderButton instanceof HTMLButtonElement) {
      createLocalFolderButton.disabled = state.moving || movingToSharedWorkspace;
    }

    const createS3BucketButton = dialog.querySelector("[data-local-workspace-move-s3-create-bucket]");
    if (createS3BucketButton instanceof HTMLButtonElement) {
      createS3BucketButton.disabled =
        state.moving || state.loadingSharedWorkspace || !movingToSharedWorkspace;
    }

    const createS3FolderButton = dialog.querySelector("[data-local-workspace-move-s3-create-folder]");
    if (createS3FolderButton instanceof HTMLButtonElement) {
      createS3FolderButton.disabled =
        state.moving ||
        state.loadingSharedWorkspace ||
        !movingToSharedWorkspace ||
        !String(state.selectedBucket || "").trim();
    }
  }

  function setLocalWorkspaceMoveDialogBusy(busy) {
    const state = getMoveState();
    state.moving = busy;
    const dialog = localWorkspaceMoveDialog();
    if (dialog) {
      const destinationSelect = localWorkspaceMoveDestinationSelect();
      if (destinationSelect instanceof HTMLSelectElement) {
        destinationSelect.disabled = busy;
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

  async function createSharedWorkspaceBucketFromMoveDialog() {
    const bucketName = await showFolderNameDialog({
      title: "New bucket",
      copy: "Enter the bucket name to create in Shared Workspace.",
      submitLabel: "Create bucket",
    });
    if (!bucketName) {
      return;
    }

    const normalizedBucketName = String(bucketName).trim().toLowerCase();
    const confirmation = await showConfirmDialog({
      title: "Create bucket",
      copy: `Create bucket "${normalizedBucketName}" in Shared Workspace?`,
      confirmLabel: "Create bucket",
      confirmTone: "primary",
    });
    if (!confirmation.confirmed) {
      return;
    }

    const created = await fetchJsonOrThrow("/api/s3/explorer/buckets", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ bucketName: normalizedBucketName }),
    });

    const state = getMoveState();
    state.destinationKind = "s3";
    state.selectedBucket = String(created.bucket || "").trim();
    state.selectedPrefix = "";
    state.s3Loaded = false;
    resetLocalWorkspaceMoveS3Tree();
    await ensureLocalWorkspaceMoveS3ExplorerLoaded({ force: true });
    await revealLocalWorkspaceMoveS3Location(state.selectedBucket, "");
    await refreshSidebar(currentWorkspaceMode());
  }

  async function createSharedWorkspaceFolderFromMoveDialog() {
    const state = getMoveState();
    if (!state.selectedBucket) {
      return;
    }

    const folderName = await showFolderNameDialog({
      title: "New folder",
      copy: `Create a folder under ${s3ExplorerPath(state.selectedBucket, state.selectedPrefix)}.`,
      submitLabel: "Create folder",
    });
    if (!folderName) {
      return;
    }

    const created = await fetchJsonOrThrow("/api/s3/explorer/folders", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        bucket: state.selectedBucket,
        prefix: state.selectedPrefix,
        folderName,
      }),
    });

    state.s3Loaded = false;
    resetLocalWorkspaceMoveS3Tree();
    await ensureLocalWorkspaceMoveS3ExplorerLoaded({ force: true });
    await revealLocalWorkspaceMoveS3Location(
      String(created.bucket || "").trim(),
      String(created.prefix || "").trim()
    );
    await refreshSidebar(currentWorkspaceMode());
  }

  async function openLocalWorkspaceMoveDialog(sourceObjectRoot, { operationKind = "move" } = {}) {
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
    state.operationKind = String(operationKind || "").trim().toLowerCase() === "copy" ? "copy" : "move";
    state.fileName = entry.fileName;
    state.folderPath = normalizeLocalWorkspaceFolderPath(entry.folderPath);
    state.moving = false;
    state.createdFolderPaths = [];
    state.destinationKind = "local";
    state.s3Loaded = false;
    state.loadingSharedWorkspace = false;
    state.sharedWorkspaceLoadError = "";
    state.selectedBucket = String(state.selectedBucket || "").trim();
    state.selectedPrefix = String(state.selectedPrefix || "").trim();
    resetLocalWorkspaceMoveS3Tree();

    const titleNode = dialog.querySelector("[data-local-workspace-move-title]");
    const copyNode = dialog.querySelector("[data-local-workspace-move-copy]");
    if (titleNode) {
      titleNode.textContent =
        state.operationKind === "copy"
          ? "Copy Local Workspace file"
          : "Move Local Workspace file";
    }
    if (copyNode) {
      copyNode.textContent =
        state.operationKind === "copy"
          ? `Copy ${entry.fileName} to another Local Workspace folder in this browser or to Shared Workspace (S3), and optionally rename the copied file.`
          : `Move ${entry.fileName} to another Local Workspace folder in this browser or to Shared Workspace (S3), and optionally rename it.`;
    }

    syncLocalWorkspaceMoveDialogState();
    dialog.showModal();
    await renderLocalWorkspaceMoveFolderList();
    return true;
  }

  function openLocalWorkspaceCopyDialog(sourceObjectRoot) {
    return openLocalWorkspaceMoveDialog(sourceObjectRoot, {
      operationKind: "copy",
    });
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

  async function updateLocalWorkspaceMoveDestinationKind(value) {
    const state = getMoveState();
    state.destinationKind = String(value || "").trim().toLowerCase() === "s3" ? "s3" : "local";
    syncLocalWorkspaceMoveDialogState();
    if (state.destinationKind === "s3") {
      await ensureLocalWorkspaceMoveS3ExplorerLoaded();
    }
  }

  function updateLocalWorkspaceMoveFolderPath(value) {
    getMoveState().folderPath = normalizeLocalWorkspaceFolderPath(value);
    syncLocalWorkspaceMoveDialogState();
  }

  function updateLocalWorkspaceMoveFileName(value) {
    getMoveState().fileName = String(value ?? "");
    syncLocalWorkspaceMoveDialogState();
  }

  function updateLocalWorkspaceMoveS3Location(bucket, prefix = "") {
    const state = getMoveState();
    state.selectedBucket = String(bucket || "").trim();
    state.selectedPrefix = String(prefix || "").trim();
    syncLocalWorkspaceMoveDialogState();
  }

  return {
    createLocalWorkspaceFolderFromDialog,
    createLocalWorkspaceFolderFromMoveDialog,
    createSharedWorkspaceBucketFromMoveDialog,
    createSharedWorkspaceFolderFromMoveDialog,
    ensureLocalWorkspaceMoveS3ExplorerLoaded,
    loadLocalWorkspaceMoveS3ExplorerNode,
    openLocalWorkspaceCopyDialog,
    openLocalWorkspaceMoveDialog,
    openLocalWorkspaceSaveDialog,
    renderLocalWorkspaceMoveFolderList,
    renderLocalWorkspaceSaveFolderList,
    revealLocalWorkspaceMoveS3Location,
    setLocalWorkspaceMoveDialogBusy,
    setLocalWorkspaceSaveDialogBusy,
    syncLocalWorkspaceMoveDialogState,
    syncLocalWorkspaceSaveDialogState,
    syncOpenLocalWorkspaceMoveDialog,
    syncOpenLocalWorkspaceSaveDialog,
    updateLocalWorkspaceMoveDestinationKind,
    updateLocalWorkspaceMoveFileName,
    updateLocalWorkspaceMoveFolderPath,
    updateLocalWorkspaceMoveS3Location,
    updateLocalWorkspaceSaveExportFormat,
    updateLocalWorkspaceSaveExportSettingsFromDialog,
    updateLocalWorkspaceSaveFileName,
    updateLocalWorkspaceSaveFolderPath,
  };
}

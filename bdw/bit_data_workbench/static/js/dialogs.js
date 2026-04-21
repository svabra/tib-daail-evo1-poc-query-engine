import { resultExportFormatOptionsMarkup, renderResultExportSettings } from "./data-exporters/export-settings.js";

export function folderNameDialog() {
  return document.querySelector("[data-folder-name-dialog]");
}

export function confirmDialog() {
  return document.querySelector("[data-confirm-dialog]");
}

export function messageDialog() {
  return document.querySelector("[data-message-dialog]");
}

export function aboutDialog() {
  return document.querySelector("[data-about-dialog]");
}

export function featureListDialog() {
  return document.querySelector("[data-feature-list-dialog]");
}

export function resultExportDialog() {
  return document.querySelector("[data-result-export-dialog]");
}

export function localWorkspaceSaveDialog() {
  return document.querySelector("[data-local-workspace-save-dialog]");
}

export function localWorkspaceMoveDialog() {
  return document.querySelector("[data-local-workspace-move-dialog]");
}

export function resultDownloadDialog() {
  return document.querySelector("[data-result-download-dialog]");
}

function appendModalDialog(markup) {
  document.body.insertAdjacentHTML("beforeend", markup.trim());
}

export function ensureConfirmDialog() {
  let dialog = confirmDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog" data-confirm-dialog>
      <form method="dialog" class="modal-card" data-confirm-form>
        <h2 class="modal-title" data-confirm-title>Confirm</h2>
        <p class="modal-copy" data-confirm-copy>Continue?</p>
        <label class="modal-toggle-option" data-confirm-option-container hidden>
          <input class="modal-toggle-checkbox" type="checkbox" data-confirm-option-input>
          <span class="modal-toggle-copy" data-confirm-option-label></span>
        </label>
        <menu class="modal-actions">
          <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
            Cancel
          </button>
          <button class="modal-button modal-button-danger" type="submit" value="confirm" data-confirm-submit>
            Delete
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = confirmDialog();
  return dialog;
}

export function ensureMessageDialog() {
  let dialog = messageDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog" data-message-dialog>
      <form method="dialog" class="modal-card" data-message-form>
        <h2 class="modal-title" data-message-title>Notice</h2>
        <p class="modal-copy" data-message-copy>Done.</p>
        <div class="modal-link-list" data-message-links hidden></div>
        <menu class="modal-actions">
          <button class="modal-button" type="submit" value="confirm" data-message-submit>
            OK
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = messageDialog();
  return dialog;
}

export function ensureAboutDialog() {
  let dialog = aboutDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-about-dialog>
      <form method="dialog" class="modal-card modal-card-wide" data-about-form>
        <div class="about-dialog-header">
          <div class="about-dialog-copy">
            <h2 class="modal-title">About</h2>
            <p class="modal-copy">
              This ProofOfConcept work is a collaborative effort of BIT and ESTV.
            </p>
          </div>
          <div class="about-dialog-version" data-about-version>Version unknown</div>
        </div>
        <div class="about-dialog-body">
          <p class="modal-copy">
            It's main purpose is to proof the speed of query execution.
          </p>
          <p class="modal-copy">
            The UI shall accelerate the testing experience for a wide range of roles:
            Data Analyst, Scientist, Data Owner, BIT Product Owner, Software and
            System Engineer, Decision Makers, etc.
            This way the client can evaluate and assess without any BIT employee beeing
            involed in tests.
          </p>
        </div>
        <menu class="modal-actions">
          <button class="modal-button" type="submit" value="confirm" data-about-submit>
            Close
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = aboutDialog();
  return dialog;
}

export function ensureFeatureListDialog() {
  let dialog = featureListDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-feature-list-dialog>
      <form method="dialog" class="modal-card modal-card-wide" data-feature-list-form>
        <div class="about-dialog-header">
          <div class="about-dialog-copy">
            <h2 class="modal-title">Feature list</h2>
            <p class="modal-copy">
              Recent user-facing changes and important fixes by release.
            </p>
          </div>
        </div>
        <div class="feature-list-dialog-body" data-feature-list-body>
        </div>
        <menu class="modal-actions">
          <button class="modal-button" type="submit" value="confirm" data-feature-list-submit>
            Close
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = featureListDialog();
  return dialog;
}

export function ensureResultExportDialog() {
  let dialog = resultExportDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-result-export-dialog>
      <form method="dialog" class="modal-card modal-card-wide result-export-dialog-card" data-result-export-form>
        <div class="result-export-dialog-header">
          <div class="result-export-dialog-copy">
            <h2 class="modal-title" data-result-export-title>Save Results to Shared Workspace</h2>
            <p class="modal-copy" data-result-export-copy>
              Choose a Shared Workspace bucket or folder, create new locations if needed, and provide a file name.
            </p>
          </div>
          <div class="result-export-dialog-toolbar">
            <button type="button" class="modal-button modal-button-secondary" data-s3-create-bucket>
              New bucket
            </button>
            <button type="button" class="modal-button modal-button-secondary" data-s3-create-folder>
              New folder
            </button>
          </div>
        </div>
        <div class="result-export-dialog-body">
          <section class="result-export-explorer-panel">
            <div class="result-export-explorer-header">
              <span
                class="workspace-tags-label"
                title="Shared Workspace data is stored in the configured MinIO / S3 bucket."
              >Shared Workspace Explorer</span>
              <div class="result-export-breadcrumbs" data-s3-explorer-breadcrumbs></div>
            </div>
            <div class="result-export-explorer-shell">
              <div class="result-export-explorer-tree" data-s3-explorer-tree></div>
            </div>
          </section>
          <aside class="result-export-target-panel">
            <div class="result-export-target-card">
              <span class="workspace-tags-label">Selected Shared Workspace Location</span>
              <p class="result-export-target-path" data-result-export-selected-path>
                Select a bucket or folder from the Shared Workspace explorer.
              </p>
            </div>
            <label class="result-export-field">
              <span class="result-export-field-label">File name</span>
              <input
                class="modal-input"
                type="text"
                data-result-export-file-name
                autocomplete="off"
                placeholder="query-result.parquet"
              >
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">Export format</span>
              <select class="modal-input" data-export-format-select>
                ${resultExportFormatOptionsMarkup("csv")}
              </select>
            </label>
            <section class="result-export-target-card">
              <span class="workspace-tags-label">Format settings</span>
              <div data-export-settings-panel></div>
            </section>
          </aside>
        </div>
        <menu class="modal-actions">
          <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
            Cancel
          </button>
          <button class="modal-button" type="submit" value="confirm" data-result-export-submit disabled>
            Save to Shared Workspace
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = resultExportDialog();
  renderResultExportSettings(dialog, "csv");
  return dialog;
}

export function ensureLocalWorkspaceSaveDialog() {
  let dialog = localWorkspaceSaveDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-local-workspace-save-dialog>
      <form method="dialog" class="modal-card modal-card-wide result-export-dialog-card" data-local-workspace-save-form>
        <div class="result-export-dialog-header">
          <div class="result-export-dialog-copy">
            <h2 class="modal-title" data-local-workspace-save-title>Save Results to Local Workspace (IndexDB)</h2>
            <p class="modal-copy" data-local-workspace-save-copy>
              Choose a Local Workspace (IndexDB) folder path and provide the file name to save in this browser.
            </p>
          </div>
          <div class="result-export-dialog-toolbar">
            <button type="button" class="modal-button modal-button-secondary" data-local-workspace-create-folder>
              New folder
            </button>
          </div>
        </div>
        <div class="result-export-dialog-body">
          <section class="result-export-explorer-panel">
            <div class="result-export-explorer-header">
              <span
                class="workspace-tags-label"
                title="Local Workspace data is stored in this browser profile using IndexedDB."
              >Local Workspace (IndexDB) Folders</span>
              <div class="result-export-breadcrumbs" data-local-workspace-breadcrumbs></div>
            </div>
            <div class="result-export-explorer-shell">
              <div class="local-workspace-folder-list" data-local-workspace-folder-list></div>
            </div>
          </section>
          <aside class="result-export-target-panel">
            <div class="result-export-target-card">
              <span class="workspace-tags-label">Selected Local Workspace (IndexDB) Location</span>
              <p class="result-export-target-path" data-local-workspace-selected-path>
                Local Workspace (IndexDB) /
              </p>
            </div>
            <label class="result-export-field">
              <span class="result-export-field-label">Folder path</span>
              <input
                class="modal-input"
                type="text"
                data-local-workspace-folder-path
                autocomplete="off"
                placeholder="optional/subfolder"
              >
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">File name</span>
              <input
                class="modal-input"
                type="text"
                data-local-workspace-file-name
                autocomplete="off"
                placeholder="query-result.parquet"
              >
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">Export format</span>
              <select class="modal-input" data-export-format-select>
                ${resultExportFormatOptionsMarkup("csv")}
              </select>
            </label>
            <section class="result-export-target-card">
              <span class="workspace-tags-label">Format settings</span>
              <div data-export-settings-panel></div>
            </section>
          </aside>
        </div>
        <menu class="modal-actions">
          <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
            Cancel
          </button>
          <button class="modal-button" type="submit" value="confirm" data-local-workspace-save-submit disabled>
            Save to Local Workspace (IndexDB)
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = localWorkspaceSaveDialog();
  renderResultExportSettings(dialog, "csv");
  return dialog;
}

export function ensureResultDownloadDialog() {
  let dialog = resultDownloadDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-result-download-dialog>
      <form method="dialog" class="modal-card modal-card-wide result-export-dialog-card" data-result-download-form>
        <div class="result-export-dialog-header">
          <div class="result-export-dialog-copy">
            <h2 class="modal-title" data-result-download-title>Download Results as ...</h2>
            <p class="modal-copy" data-result-download-copy>
              Choose the export format, adjust any format-specific settings, and confirm the download file name.
            </p>
          </div>
        </div>
        <div class="result-export-dialog-body">
          <aside class="result-export-target-panel result-export-target-panel-single">
            <label class="result-export-field">
              <span class="result-export-field-label">File name</span>
              <input
                class="modal-input"
                type="text"
                data-result-download-file-name
                autocomplete="off"
                placeholder="query-result.csv"
              >
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">Export format</span>
              <select class="modal-input" data-export-format-select>
                ${resultExportFormatOptionsMarkup("csv")}
              </select>
            </label>
            <section class="result-export-target-card">
              <span class="workspace-tags-label">Format settings</span>
              <div data-export-settings-panel></div>
            </section>
          </aside>
        </div>
        <menu class="modal-actions">
          <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
            Cancel
          </button>
          <button class="modal-button" type="submit" value="confirm" data-result-download-submit>
            Download Results
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = resultDownloadDialog();
  renderResultExportSettings(dialog, "csv");
  return dialog;
}

export function ensureLocalWorkspaceMoveDialog() {
  let dialog = localWorkspaceMoveDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-local-workspace-move-dialog>
      <form method="dialog" class="modal-card modal-card-wide result-export-dialog-card" data-local-workspace-move-form>
        <div class="result-export-dialog-header">
          <div class="result-export-dialog-copy">
            <h2 class="modal-title" data-local-workspace-move-title>Move Local Workspace file</h2>
            <p class="modal-copy" data-local-workspace-move-copy>
              Choose the destination folder in this browser and optionally rename the file.
            </p>
          </div>
          <div class="result-export-dialog-toolbar">
            <button type="button" class="modal-button modal-button-secondary" data-local-workspace-move-create-folder>
              New folder
            </button>
          </div>
        </div>
        <div class="result-export-dialog-body">
          <section class="result-export-explorer-panel">
            <div class="result-export-explorer-header">
              <span
                class="workspace-tags-label"
                title="Local Workspace data is stored in this browser profile using IndexedDB."
              >Local Workspace Folders</span>
              <div class="result-export-breadcrumbs" data-local-workspace-move-breadcrumbs></div>
            </div>
            <div class="result-export-explorer-shell">
              <div class="local-workspace-folder-list" data-local-workspace-move-folder-list></div>
            </div>
          </section>
          <aside class="result-export-target-panel">
            <div class="result-export-target-card">
              <span class="workspace-tags-label">Destination</span>
              <p class="result-export-target-path" data-local-workspace-move-selected-path>
                Local Workspace /
              </p>
            </div>
            <label class="result-export-field">
              <span class="result-export-field-label">Folder path</span>
              <input
                class="modal-input"
                type="text"
                data-local-workspace-move-folder-path
                autocomplete="off"
                placeholder="optional/subfolder"
              >
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">File name</span>
              <input
                class="modal-input"
                type="text"
                data-local-workspace-move-file-name
                autocomplete="off"
                placeholder="query-result.parquet"
              >
            </label>
            <div class="result-export-target-card">
              <span class="workspace-tags-label">Action</span>
              <p class="result-export-target-path">Move the stored file within this browser's Local Workspace.</p>
            </div>
          </aside>
        </div>
        <menu class="modal-actions">
          <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
            Cancel
          </button>
          <button class="modal-button" type="submit" value="confirm" data-local-workspace-move-submit disabled>
            Move file
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = localWorkspaceMoveDialog();
  return dialog;
}

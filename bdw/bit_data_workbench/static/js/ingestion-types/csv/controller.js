import {
  buildCsvPreviewState,
  delimiterCharacterForMode,
  delimiterLabelFromCharacter,
} from "./preview.js";
import {
  csvImportBaseNameFromFileName,
  csvImportNameFieldLabel,
  csvImportNameSuffix,
  normalizeCsvImportBaseName,
  resolveCsvDestinationFileName,
  resolveCsvSourceUploadFileName,
} from "./file-names.js";
import {
  csvS3StorageFormatDefinition,
  normalizeCsvS3StorageFormat,
} from "./s3-storage-formats.js";
import { resolveCsvS3LocationDetails } from "./s3-location.js";

function normalizeCsvIdentifier(value, defaultPrefix) {
  let normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!normalized) {
    normalized = defaultPrefix;
  }
  if (/^\d/.test(normalized)) {
    normalized = `${defaultPrefix}_${normalized}`;
  }
  return normalized;
}

function normalizeCsvTableName(fileName, prefix = "") {
  const stem = String(fileName || "").replace(/\.[^.]+$/, "");
  const normalizedBase = normalizeCsvIdentifier(stem, "csv_import");
  const normalizedPrefix = prefix
    ? normalizeCsvIdentifier(prefix, "csv").replace(/^_+|_+$/g, "")
    : "";
  return normalizedPrefix ? `${normalizedPrefix}_${normalizedBase}` : normalizedBase;
}

function emptyPreviewState() {
  return {
    status: "empty",
    fileName: "",
    delimiter: "",
    hasHeader: true,
    columns: [],
    rows: [],
    error: "",
  };
}

function buildSelectedFileEntry(file) {
  return {
    id: `csv-file-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    file,
    importBaseName: csvImportBaseNameFromFileName(file?.name || ""),
  };
}

export function createCsvIngestionController(helpers) {
  const {
    ensureLocalWorkspaceFolderPath,
    escapeHtml,
    formatByteCount,
    localWorkspaceDisplayPath,
    localWorkspaceRelation,
    normalizeLocalWorkspaceFolderPath,
    openQueryWorkbench,
    querySourceInNewNotebook,
    refreshSidebar,
    renderLocalWorkspaceSidebarEntries,
    saveLocalWorkspaceExport,
    showMessageDialog,
  } = helpers;

  let selectedFiles = [];
  let busy = false;
  let latestResults = [];
  let activeEntryId = "";
  let previewState = emptyPreviewState();
  let previewRequestVersion = 0;

  function workbenchRoot() {
    return document.querySelector("[data-ingestion-workbench-page]");
  }

  function root() {
    return document.querySelector("[data-csv-ingestion-form]");
  }

  function entryGrid() {
    return document.querySelector(".ingestion-entry-grid");
  }

  function previewRoot() {
    return document.querySelector("[data-csv-preview-root]");
  }

  function fileInput() {
    return document.querySelector("[data-csv-file-input]");
  }

  function fileListRoot() {
    return document.querySelector("[data-csv-file-list]");
  }

  function reviewListRoot() {
    return document.querySelector("[data-csv-review-list]");
  }

  function resultListRoot() {
    return document.querySelector("[data-csv-result-list]");
  }

  function submitButton() {
    return document.querySelector("[data-csv-import-submit]");
  }

  function activeEntryPanel() {
    return document.querySelector(
      `[data-ingestion-entry-panel="${CSS.escape(String(activeEntryId || "").trim())}"]`
    );
  }

  function selectedTargetId() {
    const checked = document.querySelector("[data-csv-target-option]:checked");
    return String(checked?.value || "workspace.local").trim();
  }

  function configPanel(targetId) {
    return document.querySelector(
      `[data-csv-config-panel="${CSS.escape(String(targetId || "").trim())}"]`
    );
  }

  function currentConfig() {
    const delimiterMode = String(
      document.querySelector("[data-csv-delimiter-mode]")?.value || "auto"
    )
      .trim()
      .toLowerCase();
    return {
      folderPath: normalizeLocalWorkspaceFolderPath(
        document.querySelector("[data-csv-folder-path]")?.value || ""
      ),
      bucket: String(document.querySelector("[data-csv-s3-bucket]")?.value || "").trim(),
      prefix: String(document.querySelector("[data-csv-s3-prefix]")?.value || "")
        .split("/")
        .map((segment) => String(segment || "").trim())
        .filter(Boolean)
        .join("/"),
      s3StorageFormat: normalizeCsvS3StorageFormat(
        document.querySelector("[data-csv-s3-storage-format]:checked")?.value || "csv"
      ),
      schemaName: normalizeCsvIdentifier(
        document.querySelector(
          `[data-csv-config-panel="${CSS.escape(selectedTargetId())}"] [data-csv-schema-name]`
        )?.value || "public",
        "public"
      ),
      tablePrefix: String(
        document.querySelector(
          `[data-csv-config-panel="${CSS.escape(selectedTargetId())}"] [data-csv-table-prefix]`
        )?.value || ""
      ).trim(),
      delimiterMode,
      delimiter: delimiterCharacterForMode(delimiterMode),
      hasHeader: document.querySelector("[data-csv-has-header]")?.checked !== false,
      replaceExisting: document.querySelector("[data-csv-replace-existing]")?.checked !== false,
    };
  }

  function selectedFileEntries() {
    return Array.isArray(selectedFiles) ? selectedFiles : [];
  }

  function previewFileEntry() {
    return selectedFileEntries()[0] || null;
  }

  function resolvedImportBaseName(entry) {
    return normalizeCsvImportBaseName(entry?.importBaseName || "", entry?.file?.name || "");
  }

  function resolvedSourceUploadFileName(entry) {
    return resolveCsvSourceUploadFileName(
      resolvedImportBaseName(entry),
      entry?.file?.name || ""
    );
  }

  function resolvedDestinationFileName(
    entry,
    targetId = selectedTargetId(),
    config = currentConfig()
  ) {
    return resolveCsvDestinationFileName(resolvedImportBaseName(entry), {
      targetId,
      storageFormat: config.s3StorageFormat,
      fallbackFileName: entry?.file?.name || "",
    });
  }

  function resolvedS3ObjectKey(entry, config = currentConfig()) {
    const prefix = config.prefix ? `${config.prefix}/` : "";
    return `${prefix}${resolvedDestinationFileName(entry, "workspace.s3", config)}`;
  }

  function s3LocationSummaryMarkup({
    bucket = "",
    prefix = "",
    objectName = "",
    objectKey = "",
    mode = "review",
  } = {}) {
    const details = resolveCsvS3LocationDetails({
      bucket,
      prefix,
      objectName,
      objectKey,
    });
    const emptyPrefixCopy =
      mode === "result" ? "No key prefix" : "No key prefix configured";
    return `
      <dl class="ingestion-csv-s3-summary" data-csv-s3-summary>
        <div class="ingestion-csv-s3-summary-row">
          <dt>Bucket</dt>
          <dd data-csv-s3-summary-bucket>${escapeHtml(details.bucket || "<bucket>")}</dd>
        </div>
        <div class="ingestion-csv-s3-summary-row">
          <dt>Key prefix</dt>
          <dd data-csv-s3-summary-prefix>${escapeHtml(details.keyPrefix || emptyPrefixCopy)}</dd>
        </div>
        <div class="ingestion-csv-s3-summary-row">
          <dt>Object name</dt>
          <dd data-csv-s3-summary-object-name>${escapeHtml(details.objectName)}</dd>
        </div>
      </dl>
    `;
  }

  function resolvedDestinationCopy(entry, targetId = selectedTargetId(), config = currentConfig()) {
    if (targetId === "workspace.local") {
      return localWorkspaceDisplayPath(config.folderPath, resolvedDestinationFileName(entry, targetId, config));
    }
    if (targetId === "workspace.s3") {
      const bucket = config.bucket || "<bucket>";
      return `s3://${bucket}/${resolvedS3ObjectKey(entry, config)}`;
    }
    return `${config.schemaName}.${normalizeCsvTableName(
      resolvedSourceUploadFileName(entry),
      config.tablePrefix
    )}`;
  }

  function targetLabel(targetId = selectedTargetId()) {
    switch (targetId) {
      case "workspace.s3":
        return "Shared Workspace S3";
      case "pg_oltp":
        return "PostgreSQL OLTP";
      case "pg_olap":
        return "PostgreSQL OLAP";
      default:
        return "Local Workspace";
    }
  }

  function csvSettingsLabel(config = currentConfig(), targetId = selectedTargetId()) {
    const delimiterLabel =
      config.delimiterMode === "auto"
        ? delimiterLabelFromCharacter(previewState.delimiter || ",")
        : delimiterLabelFromCharacter(config.delimiter);
    const baseLabel = `${delimiterLabel} delimiter, ${
      config.hasHeader ? "header row" : "no header row"
    }`;
    if (targetId !== "workspace.s3") {
      return baseLabel;
    }
    return `${baseLabel}, ${csvS3StorageFormatDefinition(config.s3StorageFormat).reviewLabel}`;
  }

  function fileListMarkup() {
    if (!selectedFileEntries().length) {
      return '<p class="ingestion-empty">No CSV files selected yet.</p>';
    }

    return selectedFileEntries()
      .map(
        (entry) => `
          <article class="ingestion-csv-file-card">
            <strong>${escapeHtml(entry.file.name)}</strong>
            <span>${escapeHtml(formatByteCount(entry.file.size))}</span>
          </article>
        `
      )
      .join("");
  }

  function reviewMarkup() {
    if (!selectedFileEntries().length) {
      return '<p class="ingestion-empty">Select files to see the resolved destination names.</p>';
    }

    const targetId = selectedTargetId();
    const config = currentConfig();
    const importNameLabel = csvImportNameFieldLabel(targetId);
    const importNameSuffix = csvImportNameSuffix(targetId, config.s3StorageFormat);
    return selectedFileEntries()
      .map(
        (entry) => `
          <article class="ingestion-csv-review-card">
            <span class="ingestion-csv-review-name">${escapeHtml(entry.file.name)}</span>
            <span class="ingestion-csv-review-target">${escapeHtml(targetLabel(targetId))}</span>
            <label class="result-export-field ingestion-csv-review-name-field">
              <span class="result-export-field-label">${escapeHtml(importNameLabel)}</span>
              <span class="ingestion-csv-review-name-input">
                <input
                  class="modal-input"
                  type="text"
                  value="${escapeHtml(resolvedImportBaseName(entry))}"
                  data-csv-import-base-name
                  data-csv-file-id="${escapeHtml(entry.id)}"
                  spellcheck="false"
                  autocomplete="off"
                >
                <span class="ingestion-csv-review-name-suffix">${escapeHtml(importNameSuffix)}</span>
              </span>
            </label>
            <span class="ingestion-csv-review-copy">${escapeHtml(
              csvSettingsLabel(config, targetId)
            )}</span>
            ${
              targetId === "workspace.s3"
                ? `
                  <span class="ingestion-csv-review-copy">
                    S3 stores a bucket, an optional key prefix, and an object name. The prefix is not a directory.
                  </span>
                  ${s3LocationSummaryMarkup({
                    bucket: config.bucket || "<bucket>",
                    prefix: config.prefix,
                    objectName: resolvedDestinationFileName(entry, targetId, config),
                    objectKey: resolvedS3ObjectKey(entry, config),
                    mode: "review",
                  })}
                `
                : ""
            }
            ${
              targetId === "workspace.s3"
                ? ""
                : `<code class="ingestion-csv-review-path">${escapeHtml(
                    resolvedDestinationCopy(entry, targetId, config)
                  )}</code>`
            }
          </article>
        `
      )
      .join("");
  }

  function previewMarkup() {
    if (previewState.status === "empty") {
      return '<p class="ingestion-empty">Select CSV files to preview the detected columns and sample rows.</p>';
    }

    if (previewState.status === "loading") {
      return `<p class="ingestion-empty">Inspecting ${escapeHtml(
        previewState.fileName || "CSV file"
      )} ...</p>`;
    }

    if (previewState.status === "error") {
      return `
        <article class="ingestion-csv-preview-card ingestion-csv-preview-card-error">
          <strong>${escapeHtml(previewState.fileName || "CSV file")}</strong>
          <p class="ingestion-csv-result-error">${escapeHtml(
            previewState.error || "The CSV preview could not be generated."
          )}</p>
        </article>
      `;
    }

    const columnCount = previewState.columns.length;
    const sampleCount = previewState.rows.length;
    const previewTableHead = previewState.columns.length
      ? `
        <thead>
          <tr>
            ${previewState.columns
              .map((column) => `<th>${escapeHtml(column)}</th>`)
              .join("")}
          </tr>
        </thead>
      `
      : "";
    const previewTableBody = previewState.rows.length
      ? `
        <tbody>
          ${previewState.rows
            .map(
              (row) => `
                <tr>
                  ${previewState.columns
                    .map((_, index) => `<td>${escapeHtml(String(row[index] ?? ""))}</td>`)
                    .join("")}
                </tr>
              `
            )
            .join("")}
        </tbody>
      `
      : `
        <tbody>
          <tr>
            <td colspan="${Math.max(columnCount, 1)}">No sample rows were detected in the preview window.</td>
          </tr>
        </tbody>
      `;

    return `
      <article class="ingestion-csv-preview-card">
        <div class="ingestion-csv-preview-header">
          <strong>${escapeHtml(previewState.fileName || "CSV file")}</strong>
          <span class="ingestion-csv-review-target">${escapeHtml(
            delimiterLabelFromCharacter(previewState.delimiter || ",")
          )} delimiter</span>
        </div>
        <p class="ingestion-csv-preview-copy">
          Previewing ${escapeHtml(String(sampleCount))} sample row(s) from the first selected file.
          ${escapeHtml(previewState.hasHeader ? "The first row is treated as column names." : "Column names are synthesized because the file is treated as headerless.")}
        </p>
        <div class="ingestion-csv-preview-columns">
          ${previewState.columns
            .map(
              (column) =>
                `<span class="ingestion-csv-preview-column">${escapeHtml(column)}</span>`
            )
            .join("")}
        </div>
        <div class="ingestion-csv-preview-table-shell">
          <table class="ingestion-csv-preview-table">
            ${previewTableHead}
            ${previewTableBody}
          </table>
        </div>
      </article>
    `;
  }

  async function refreshPreview() {
    const entry = previewFileEntry();
    const file = entry?.file || null;
    if (!file) {
      previewState = emptyPreviewState();
      renderCsvIngestionWorkbench();
      return;
    }

    const requestVersion = (previewRequestVersion += 1);
    previewState = {
      status: "loading",
      fileName: file.name,
      delimiter: "",
      hasHeader: currentConfig().hasHeader,
      columns: [],
      rows: [],
      error: "",
    };
    renderCsvIngestionWorkbench();

    try {
      const nextPreviewState = await buildCsvPreviewState(file, currentConfig());
      if (requestVersion !== previewRequestVersion) {
        return;
      }
      previewState = nextPreviewState;
    } catch (error) {
      if (requestVersion !== previewRequestVersion) {
        return;
      }
      previewState = {
        status: "error",
        fileName: file.name,
        delimiter: "",
        hasHeader: currentConfig().hasHeader,
        columns: [],
        rows: [],
        error: error instanceof Error ? error.message : "The CSV preview could not be generated.",
      };
    }

    renderCsvIngestionWorkbench();
  }

  function resultMarkup() {
    if (!latestResults.length) {
      return "";
    }

    return latestResults
      .map((item) => {
        const resultDisplayName = item.storedFileName || item.fileName || "CSV file";
        const s3LocationDetails = item.storageFormat
          ? resolveCsvS3LocationDetails({
              bucket: item.bucket,
              prefix: item.objectKeyPrefix,
              objectName: item.storedFileName || item.fileName,
              objectKey: item.objectKey,
              storedFileName: item.storedFileName || item.fileName,
            })
          : null;
        return `
          <article class="ingestion-csv-result-card ingestion-csv-result-card-${escapeHtml(
            item.status || "unknown"
          )}">
            <div class="ingestion-csv-result-header">
              <strong>${escapeHtml(resultDisplayName)}</strong>
              <span class="ingestion-csv-result-status">${escapeHtml(item.status || "unknown")}</span>
            </div>
            ${
              item.storedFileName && item.fileName && item.storedFileName !== item.fileName
                ? `<span class="ingestion-csv-result-copy">${escapeHtml(
                    `Imported from ${item.fileName}`
                  )}</span>`
                : ""
            }
            ${
              s3LocationDetails
                ? `
                  <span class="ingestion-csv-result-copy">
                    Shared Workspace S3 stores this import as an object, not a directory entry.
                  </span>
                  ${s3LocationSummaryMarkup({
                    bucket: s3LocationDetails.bucket,
                    prefix: s3LocationDetails.keyPrefix,
                    objectName: s3LocationDetails.objectName,
                    objectKey: s3LocationDetails.objectKey,
                    mode: "result",
                  })}
                `
                : ""
            }
            ${
              item.path && !s3LocationDetails
                ? `<code class="ingestion-csv-review-path">${escapeHtml(item.path)}</code>`
                : ""
            }
            ${
              item.relation
                ? `<code class="ingestion-csv-review-path">${escapeHtml(item.relation)}</code>`
                : ""
            }
            ${
              Number.isFinite(Number(item.rowCount))
                ? `<span class="ingestion-csv-result-copy">${escapeHtml(
                    `${Number(item.rowCount).toLocaleString()} row(s)`
                  )}</span>`
                : ""
            }
            ${
              item.storageFormat
                ? `<span class="ingestion-csv-result-copy">${escapeHtml(
                    `${targetLabel("workspace.s3")}: ${
                      csvS3StorageFormatDefinition(item.storageFormat).reviewLabel
                    }. DuckDB will query that stored object format directly.`
                  )}</span>`
                : ""
            }
            ${
              item.error
                ? `<span class="ingestion-csv-result-copy ingestion-csv-result-error">${escapeHtml(
                    item.error
                  )}</span>`
                : ""
            }
            ${
              item.queryUnavailableReason
                ? `<span class="ingestion-csv-result-copy" data-csv-result-query-note>${escapeHtml(
                    item.queryUnavailableReason
                  )}</span>`
                : ""
            }
            ${
              item.querySource
                ? `
                  <div class="ingestion-csv-result-actions">
                    <button
                      type="button"
                      class="modal-button modal-button-secondary"
                      data-csv-import-open-query
                      data-csv-query-source-id="${escapeHtml(item.querySource.sourceId || "")}"
                      data-csv-query-source-relation="${escapeHtml(item.querySource.relation || "")}"
                      data-csv-query-source-name="${escapeHtml(
                        item.querySource.name || item.relation || item.path || "Imported source"
                      )}"
                    >
                      Query in new notebook
                    </button>
                  </div>
                `
                : ""
            }
          </article>
        `;
      })
      .join("");
  }

  function querySourceForActionTarget(actionTarget) {
    const button = actionTarget.closest("[data-csv-import-open-query]");
    if (!button) {
      return null;
    }

    const sourceId = String(button.dataset.csvQuerySourceId || "").trim();
    const relation = String(button.dataset.csvQuerySourceRelation || "").trim();
    const name = String(button.dataset.csvQuerySourceName || "").trim();
    if (!sourceId || !relation) {
      return null;
    }

    return {
      sourceId,
      relation,
      name,
    };
  }

  function sourceObjectRootForQuerySource(querySource) {
    if (!querySource?.sourceId || !querySource?.relation) {
      return null;
    }

    return document.querySelector(
      `[data-source-object][data-source-option-id="${CSS.escape(querySource.sourceId)}"][data-source-object-relation="${CSS.escape(querySource.relation)}"]`
    );
  }

  function revealQuerySourceInSidebar(sourceObjectRoot) {
    if (!(sourceObjectRoot instanceof Element)) {
      return;
    }

    const dataSourcesRoot = document.querySelector("[data-data-sources-section]");
    if (dataSourcesRoot instanceof HTMLDetailsElement) {
      dataSourcesRoot.open = true;
    }

    const catalogRoot = sourceObjectRoot.closest("[data-source-catalog]");
    if (catalogRoot instanceof HTMLDetailsElement) {
      catalogRoot.open = true;
    }

    const schemaRoot = sourceObjectRoot.closest("[data-source-schema]");
    if (schemaRoot instanceof HTMLDetailsElement) {
      schemaRoot.open = true;
    }

    sourceObjectRoot.scrollIntoView({ block: "nearest" });
  }

  async function openImportedSourceInNewNotebook(querySource) {
    await refreshSidebar("notebook");
    const sourceObjectRoot = sourceObjectRootForQuerySource(querySource);
    if (!(sourceObjectRoot instanceof Element)) {
      throw new Error(
        `The imported source ${querySource?.name || querySource?.relation || ""} is not visible in Data Sources yet.`
      );
    }

    revealQuerySourceInSidebar(sourceObjectRoot);
    const notebookId = await querySourceInNewNotebook(sourceObjectRoot);
    if (!notebookId) {
      throw new Error("The Query Workbench could not open a notebook for the imported source.");
    }
    await openQueryWorkbench(notebookId);
    return notebookId;
  }

  function syncConfigPanels() {
    const targetId = selectedTargetId();
    document.querySelectorAll("[data-csv-config-panel]").forEach((panel) => {
      panel.hidden = panel.dataset.csvConfigPanel !== targetId;
    });
    document.querySelectorAll(".ingestion-csv-target-card").forEach((card) => {
      const input = card.querySelector("[data-csv-target-option]");
      card.classList.toggle("is-selected", Boolean(input?.checked));
    });

    const replaceExistingRow = document.querySelector("[data-csv-replace-existing-row]");
    if (replaceExistingRow) {
      replaceExistingRow.hidden = targetId === "workspace.local";
      const copy = replaceExistingRow.querySelector("span");
      if (copy) {
        copy.textContent =
          targetId === "workspace.s3"
            ? "Overwrite the object if the resolved key already exists"
            : "Replace the target table if it already exists";
      }
    }
  }

  function syncEntryPanels() {
    const grid = entryGrid();
    if (grid) {
      grid.hidden = Boolean(activeEntryId);
    }

    document.querySelectorAll("[data-ingestion-tile]").forEach((tile) => {
      const selected = tile.dataset.ingestionTile === activeEntryId;
      tile.classList.toggle("is-selected", selected);
      tile.setAttribute("aria-pressed", selected ? "true" : "false");
    });

    document.querySelectorAll("[data-ingestion-entry-panel]").forEach((panel) => {
      panel.hidden = panel.dataset.ingestionEntryPanel !== activeEntryId;
    });
  }

  function syncSubmitState() {
    const button = submitButton();
    if (!button) {
      return;
    }
    button.disabled =
      busy ||
      !selectedFiles.length ||
      previewState.status === "loading" ||
      previewState.status === "error";
    button.textContent = busy ? "Importing ..." : "Import CSV files";
  }

  function renderCsvIngestionWorkbench() {
    if (!workbenchRoot()) {
      return;
    }

    syncEntryPanels();

    if (!root() || activeEntryId !== "csv") {
      return;
    }

    syncConfigPanels();

    const fileList = fileListRoot();
    if (fileList) {
      fileList.innerHTML = fileListMarkup();
    }

    const reviewList = reviewListRoot();
    if (reviewList) {
      reviewList.innerHTML = reviewMarkup();
    }

    const preview = previewRoot();
    if (preview) {
      preview.innerHTML = previewMarkup();
    }

    const results = resultListRoot();
    if (results) {
      results.hidden = latestResults.length === 0;
      results.innerHTML = resultMarkup();
    }

    syncSubmitState();
  }

  function setSelectedFiles(files) {
    selectedFiles = Array.from(files || [])
      .filter((file) => String(file?.name || "").trim().toLowerCase().endsWith(".csv"))
      .map((file) => buildSelectedFileEntry(file));
    latestResults = [];
    renderCsvIngestionWorkbench();
    void refreshPreview();
  }

  function updateSelectedFileImportBaseName(fileId, nextBaseName) {
    const normalizedFileId = String(fileId || "").trim();
    if (!normalizedFileId) {
      return false;
    }

    let changed = false;
    selectedFiles = selectedFileEntries().map((entry) => {
      if (entry.id !== normalizedFileId) {
        return entry;
      }
      const normalizedBaseName = normalizeCsvImportBaseName(
        nextBaseName,
        entry?.file?.name || ""
      );
      if (normalizedBaseName === entry.importBaseName) {
        return entry;
      }
      changed = true;
      return {
        ...entry,
        importBaseName: normalizedBaseName,
      };
    });
    return changed;
  }

  function openIngestionEntry(entryId) {
    activeEntryId = String(entryId || "").trim().toLowerCase();
    renderCsvIngestionWorkbench();
    activeEntryPanel()?.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  function closeIngestionEntry() {
    activeEntryId = "";
    renderCsvIngestionWorkbench();
  }

  function showIngestionLanding() {
    activeEntryId = "";
    renderCsvIngestionWorkbench();
  }

  async function importToLocalWorkspace() {
    const config = currentConfig();
    const folderPath = normalizeLocalWorkspaceFolderPath(config.folderPath);
    ensureLocalWorkspaceFolderPath(folderPath);
    const timestamp = new Date().toISOString();

    const results = [];
    for (const entry of selectedFileEntries()) {
      const storedFileName = resolvedSourceUploadFileName(entry);
      const storedEntry = await saveLocalWorkspaceExport({
        id: `local-workspace-csv-${Date.now().toString(36)}-${Math.random()
          .toString(36)
          .slice(2, 8)}`,
        fileName: storedFileName,
        folderPath,
        exportFormat: "csv",
        mimeType: entry.file.type || "text/csv",
        sizeBytes: entry.file.size,
        createdAt: timestamp,
        updatedAt: timestamp,
        notebookTitle: "CSV Ingestion",
        cellId: "",
        columnCount: Array.isArray(previewState.columns) ? previewState.columns.length : 0,
        rowCount: 0,
        csvDelimiter: config.delimiterMode === "auto" ? previewState.delimiter || config.delimiter : config.delimiter,
        csvHasHeader: config.hasHeader,
        blob: entry.file,
      });
      results.push({
        fileName: storedEntry.fileName,
        status: "imported",
        path: localWorkspaceDisplayPath(storedEntry.folderPath, storedEntry.fileName),
        querySource: {
          sourceId: "workspace.local",
          relation: localWorkspaceRelation(storedEntry.id),
          name: storedEntry.fileName,
        },
      });
    }

    await renderLocalWorkspaceSidebarEntries();
    return results;
  }

  async function importToServerTarget() {
    const formData = new FormData();
    const targetId = selectedTargetId();
    const config = currentConfig();
    const resolvedDelimiter =
      config.delimiterMode === "auto" ? previewState.delimiter || config.delimiter : config.delimiter;
    formData.set("targetId", targetId);
    formData.set("bucket", config.bucket);
    formData.set("prefix", config.prefix);
    formData.set("schemaName", config.schemaName);
    formData.set("tablePrefix", config.tablePrefix);
    formData.set("delimiter", resolvedDelimiter);
    formData.set("hasHeader", config.hasHeader ? "true" : "false");
    formData.set("replaceExisting", config.replaceExisting ? "true" : "false");
    formData.set("storageFormat", config.s3StorageFormat);
    selectedFileEntries().forEach((entry) => {
      formData.append("files", entry.file, resolvedSourceUploadFileName(entry));
    });

    const response = await window.fetch("/api/ingestion/csv/import", {
      method: "POST",
      body: formData,
      headers: {
        Accept: "application/json",
      },
    });
    if (!response.ok) {
      let message = "The CSV files could not be imported.";
      try {
        const payload = await response.json();
        message = payload?.detail || message;
      } catch (_error) {
        // Ignore invalid JSON error payloads.
      }
      throw new Error(message);
    }

    const payload = await response.json();
    await refreshSidebar("notebook");
    return Array.isArray(payload?.imports) ? payload.imports : [];
  }

  async function submitCsvIngestionForm() {
    if (!selectedFiles.length || busy) {
      return false;
    }

    busy = true;
    latestResults = [];
    renderCsvIngestionWorkbench();

    try {
      latestResults =
        selectedTargetId() === "workspace.local"
          ? await importToLocalWorkspace()
          : await importToServerTarget();
      renderCsvIngestionWorkbench();
      const completedCount = latestResults.filter((item) => item.status === "imported").length;
      await showMessageDialog({
        title: "CSV import finished",
        copy:
          selectedTargetId() === "workspace.local"
            ? `${completedCount} file(s) stored in Local Workspace and ready for Query Workbench handoff.`
            : `${completedCount} file(s) processed for ${targetLabel()}.`,
      });
    } finally {
      busy = false;
      renderCsvIngestionWorkbench();
    }

    return true;
  }

  function handleCsvIngestionInput(event) {
    const relevantInput = event.target.closest(
      "[data-csv-folder-path], [data-csv-s3-bucket], [data-csv-s3-prefix], [data-csv-schema-name], [data-csv-table-prefix]"
    );
    if (!relevantInput) {
      return false;
    }
    renderCsvIngestionWorkbench();
    return true;
  }

  function handleCsvIngestionChange(event) {
    const ingestionTile = event.target.closest("[data-ingestion-tile]");
    if (ingestionTile) {
      openIngestionEntry(ingestionTile.dataset.ingestionTile);
      return true;
    }

    const csvFileInput = event.target.closest("[data-csv-file-input]");
    if (csvFileInput instanceof HTMLInputElement) {
      setSelectedFiles(csvFileInput.files || []);
      return true;
    }

    const targetOption = event.target.closest("[data-csv-target-option]");
    if (targetOption) {
      renderCsvIngestionWorkbench();
      return true;
    }

    const previewOption = event.target.closest(
      "[data-csv-delimiter-mode], [data-csv-has-header], [data-csv-replace-existing], [data-csv-s3-storage-format]"
    );
    if (previewOption) {
      renderCsvIngestionWorkbench();
      if (
        previewOption.matches("[data-csv-delimiter-mode]") ||
        previewOption.matches("[data-csv-has-header]")
      ) {
        void refreshPreview();
      }
      return true;
    }

    const importBaseNameInput = event.target.closest("[data-csv-import-base-name]");
    if (importBaseNameInput instanceof HTMLInputElement) {
      if (
        updateSelectedFileImportBaseName(
          importBaseNameInput.dataset.csvFileId,
          importBaseNameInput.value
        )
      ) {
        renderCsvIngestionWorkbench();
      }
      return true;
    }

    return false;
  }

  function handleCsvIngestionClick(event) {
    const ingestionTile = event.target.closest("[data-ingestion-tile]");
    if (ingestionTile) {
      event.preventDefault();
      openIngestionEntry(ingestionTile.dataset.ingestionTile);
      return true;
    }

    const backButton = event.target.closest("[data-close-ingestion-entry]");
    if (backButton) {
      event.preventDefault();
      closeIngestionEntry();
      return true;
    }

    const querySource = querySourceForActionTarget(event.target);
    if (querySource) {
      event.preventDefault();
      openImportedSourceInNewNotebook(querySource).catch(async (error) => {
        console.error("Failed to open the imported CSV source in a notebook.", error);
        await showMessageDialog({
          title: "Query handoff failed",
          copy:
            error instanceof Error
              ? error.message
              : "The imported source could not be opened in the Query Workbench.",
        });
      });
      return true;
    }

    return false;
  }

  function handleCsvDrop(event) {
    const dropzone = event.target.closest("[data-csv-dropzone]");
    if (!dropzone) {
      return false;
    }
    event.preventDefault();
    dropzone.classList.remove("is-drag-over");
    if (event.dataTransfer?.files?.length) {
      setSelectedFiles(event.dataTransfer.files);
      const input = fileInput();
      if (input instanceof HTMLInputElement) {
        try {
          input.files = event.dataTransfer.files;
        } catch (_error) {
          // Some browsers expose a read-only FileList; the selectedFiles state is already updated.
        }
      }
    }
    return true;
  }

  function handleCsvDragOver(event) {
    const dropzone = event.target.closest("[data-csv-dropzone]");
    if (!dropzone) {
      return false;
    }
    event.preventDefault();
    dropzone.classList.add("is-drag-over");
    return true;
  }

  function handleCsvDragLeave(event) {
    const dropzone = event.target.closest("[data-csv-dropzone]");
    if (!dropzone) {
      return false;
    }
    dropzone.classList.remove("is-drag-over");
    return true;
  }

  return {
    handleCsvIngestionClick,
    handleCsvDragLeave,
    handleCsvDragOver,
    handleCsvDrop,
    handleCsvIngestionChange,
    handleCsvIngestionInput,
    renderCsvIngestionWorkbench,
    showIngestionLanding,
    submitCsvIngestionForm,
  };
}

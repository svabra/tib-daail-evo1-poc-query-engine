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
import {
  buildCsvZipPreviewState,
  isZipFile,
  readCsvFilesFromZip,
} from "./zip-reader.js";

const DEFAULT_CSV_UPLOAD_CHUNK_BYTES = 5 * 1024 * 1024;
const CSV_UPLOAD_CHUNK_ATTEMPTS = 5;
const CSV_UPLOAD_COMPLETION_POLL_INTERVAL_MS = 2000;
const CSV_UPLOAD_COMPLETION_TIMEOUT_MS = 60 * 60 * 1000;

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
  let busyPhase = "";
  let uploadProgress = null;
  let latestResults = [];
  let latestImportPayload = null;
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

  function entrySearch() {
    return document.querySelector(".ingestion-entry-search");
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

  function uploadProgressRoot() {
    return document.querySelector("[data-csv-upload-progress]");
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

  function normalizedCsvImportStatus(item) {
    return String(item?.status || "").trim().toLowerCase();
  }

  function csvImportPayloadFromResults(results, targetId = selectedTargetId()) {
    const imports = Array.isArray(results) ? results : [];
    const importedCount = imports.filter(
      (item) => normalizedCsvImportStatus(item) === "imported"
    ).length;
    return {
      targetId,
      importedCount,
      failedCount: Math.max(0, imports.length - importedCount),
      imports,
    };
  }

  function csvImportResultsFromPayload(payload) {
    const imports = Array.isArray(payload?.imports) ? payload.imports : [];
    const firstQuerySource =
      payload?.firstQuerySource && typeof payload.firstQuerySource === "object"
        ? payload.firstQuerySource
        : null;
    if (!firstQuerySource) {
      return imports;
    }
    let attachedFallback = false;
    return imports.map((item) => {
      if (
        attachedFallback ||
        item?.querySource ||
        normalizedCsvImportStatus(item) !== "imported"
      ) {
        return item;
      }
      attachedFallback = true;
      return {
        ...item,
        querySource: firstQuerySource,
      };
    });
  }

  function csvImportedCount(payload, imports = csvImportResultsFromPayload(payload)) {
    const importedCount = Number(payload?.importedCount);
    if (Number.isFinite(importedCount)) {
      return importedCount;
    }
    return imports.filter((item) => normalizedCsvImportStatus(item) === "imported").length;
  }

  function isCsvOrZipFile(file) {
    const fileName = String(file?.name || "").trim().toLowerCase();
    return fileName.endsWith(".csv") || fileName.endsWith(".zip");
  }

  function formatMegabytes(bytes) {
    const megabytes = Number(bytes || 0) / (1024 * 1024);
    return megabytes.toLocaleString(undefined, {
      maximumFractionDigits: megabytes >= 100 ? 0 : 1,
    });
  }

  function uploadFailureProgressDetails({ progressBaseBytes = 0, totalBytes = 0 } = {}) {
    const safeTotalBytes = Math.max(0, Number(totalBytes || 0));
    const transferredBytes = Math.min(
      safeTotalBytes || Number.MAX_SAFE_INTEGER,
      Math.max(0, Number(progressBaseBytes || 0))
    );
    const percentage = safeTotalBytes > 0
      ? Math.min(100, Math.max(0, Math.round((transferredBytes / safeTotalBytes) * 100)))
      : 0;
    return {
      percentage,
      transferredBytes,
      totalBytes: safeTotalBytes,
    };
  }

  function chunkUploadFailureMessage(options = {}, error = null) {
    const progressDetails = uploadFailureProgressDetails(options);
    const chunkNumber = Math.max(1, Number(options.chunkNumber || Number(options.chunkIndex || 0) + 1));
    const totalChunks = Math.max(0, Number(options.totalChunks || 0));
    const chunkCopy = totalChunks > 0
      ? `${Math.min(chunkNumber, totalChunks)}/${totalChunks}`
      : String(chunkNumber);
    const detail = error instanceof Error
      ? error.message
      : String(error || "The CSV upload chunk failed.");
    const mbCopy = progressDetails.totalBytes > 0
      ? `${formatMegabytes(progressDetails.transferredBytes)} / ${formatMegabytes(progressDetails.totalBytes)} MB uploaded`
      : `${formatMegabytes(progressDetails.transferredBytes)} MB uploaded`;
    const startBytes = Number(options.start || 0);
    const endBytes = Number(options.end || 0);
    const rangeCopy = Number.isFinite(startBytes) && Number.isFinite(endBytes) && endBytes >= startBytes
      ? `; chunk range ${formatMegabytes(startBytes)}-${formatMegabytes(endBytes)} MB`
      : "";
    return `Upload failed at chunk ${chunkCopy} after ${CSV_UPLOAD_CHUNK_ATTEMPTS} attempts (${progressDetails.percentage}% complete, ${mbCopy}${rangeCopy}). ${detail}`;
  }

  function processingFailureMessage(error, totalBytes) {
    const safeTotalBytes = Math.max(0, Number(totalBytes || 0));
    const detail = error instanceof Error
      ? error.message
      : String(error || "The CSV files could not be imported.");
    const mbCopy = safeTotalBytes > 0
      ? `${formatMegabytes(safeTotalBytes)} / ${formatMegabytes(safeTotalBytes)} MB`
      : "0 / 0 MB";
    return `Upload finished (100%, ${mbCopy}), but the server-side processing step failed during Step 2 of 2: Transforming file to match target data format. ${detail}`;
  }

  function uploadChunkCount(fileSize, chunkSize) {
    const safeFileSize = Math.max(0, Number(fileSize || 0));
    const safeChunkSize = Math.max(1, Number(chunkSize || 1));
    return safeFileSize > 0 ? Math.ceil(safeFileSize / safeChunkSize) : 0;
  }

  function committedChunkCount(receivedBytes, fileSize, chunkSize) {
    const totalChunks = uploadChunkCount(fileSize, chunkSize);
    if (totalChunks <= 0) {
      return 0;
    }
    const safeReceivedBytes = Math.min(
      Math.max(0, Number(receivedBytes || 0)),
      Math.max(0, Number(fileSize || 0))
    );
    return Math.min(totalChunks, Math.ceil(safeReceivedBytes / Math.max(1, Number(chunkSize || 1))));
  }

  function previewFileEntry() {
    return selectedFileEntries()[0] || null;
  }

  function resolvedImportBaseName(entry) {
    return normalizeCsvImportBaseName(entry?.importBaseName || "", entry?.file?.name || "");
  }

  function resolvedSourceUploadFileName(entry) {
    if (isZipFile(entry?.file)) {
      return `${resolvedImportBaseName(entry)}.zip`;
    }
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
    if (isZipFile(entry?.file)) {
      return targetId === "workspace.local"
        ? localWorkspaceDisplayPath(config.folderPath, "extracted CSV files")
        : `Extracted CSV files from ${resolvedSourceUploadFileName(entry)}`;
    }
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
        return "Local Workspace (IndexDB)";
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
      return '<p class="ingestion-empty">No CSV or ZIP files selected yet.</p>';
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
    return selectedFileEntries()
      .map(
        (entry) => {
          const importNameSuffix = isZipFile(entry.file)
            ? ".zip"
            : csvImportNameSuffix(targetId, config.s3StorageFormat);
          return `
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
        `;
        }
      )
      .join("");
  }

  function previewMarkup() {
    if (previewState.status === "empty") {
      return '<p class="ingestion-empty">Select CSV or ZIP files to preview the detected columns and sample rows.</p>';
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
    if (Number.isFinite(Number(previewState.archiveEntryCount))) {
      return `
        <article class="ingestion-csv-preview-card">
          <div class="ingestion-csv-preview-header">
            <strong>${escapeHtml(previewState.fileName || "ZIP archive")}</strong>
            <span class="ingestion-csv-review-target">ZIP archive</span>
          </div>
          <p class="ingestion-csv-preview-copy">
            ${escapeHtml(String(previewState.archiveEntryCount))} CSV file(s) will be extracted during import.
          </p>
        </article>
      `;
    }
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
      const nextPreviewState = isZipFile(file)
        ? await buildCsvZipPreviewState(file)
        : await buildCsvPreviewState(file, currentConfig());
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
            normalizedCsvImportStatus(item) || "unknown"
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
    const sourceObjectRoot = await waitForQuerySourceObject(querySource);
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
    revealQuerySourceInSidebar((await waitForQuerySourceObject(querySource)) || sourceObjectRoot);
    return notebookId;
  }

  async function waitForQuerySourceObject(querySource) {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const sourceObjectRoot = sourceObjectRootForQuerySource(querySource);
      if (sourceObjectRoot instanceof Element) {
        return sourceObjectRoot;
      }
      await renderLocalWorkspaceSidebarEntries();
      await sleep(100);
    }
    return null;
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
    const search = entrySearch();
    if (search) {
      search.hidden = Boolean(activeEntryId);
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

  function setUploadProgress(nextProgress) {
    uploadProgress = nextProgress;
    syncUploadProgress();
    syncSubmitState();
  }

  function syncUploadProgress() {
    const progressRoot = uploadProgressRoot();
    if (!progressRoot) {
      return;
    }
    if (!uploadProgress) {
      progressRoot.hidden = true;
      progressRoot.innerHTML = "";
      return;
    }

    const totalBytes = Number(uploadProgress.totalBytes || 0);
    const transferredBytes = Number(uploadProgress.transferredBytes || 0);
    const percentage = totalBytes > 0
      ? Math.min(100, Math.max(0, Math.round((transferredBytes / totalBytes) * 100)))
      : Number(uploadProgress.percentage || 0);
    const label = uploadProgress.label || "Uploading ...";
    const phase = uploadProgress.phase || (uploadProgress.indeterminate ? "processing" : "uploading");
    const chunkNumber = Math.max(0, Number(uploadProgress.chunkNumber || 0));
    const totalChunks = Math.max(0, Number(uploadProgress.totalChunks || 0));
    const chunkCopy = phase === "uploading" && chunkNumber > 0 && totalChunks > 0
      ? `, chunk ${Math.min(chunkNumber, totalChunks)}/${totalChunks}`
      : "";
    const detail = uploadProgress.detail
      || (phase === "processing"
        ? "Step 2 of 2: Upload complete. Processing server-side import."
        : totalBytes > 0
          ? `Step 1 of 2: ${percentage}% (${formatMegabytes(transferredBytes)} / ${formatMegabytes(totalBytes)} MB) uploaded${chunkCopy}`
          : "Step 1 of 2: Uploading ...");
    progressRoot.hidden = false;
    progressRoot.innerHTML = `
      <span class="ingestion-csv-upload-progress-copy">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(detail)}</span>
      </span>
      <span class="ingestion-csv-upload-progress-track ${
        uploadProgress.indeterminate ? "is-indeterminate" : ""
      }" aria-hidden="true">
        <span style="width: ${escapeHtml(String(percentage))}%"></span>
      </span>
    `;
  }

  function serverProcessingProgressDetail(fileEntries = []) {
    const hasArchive = fileEntries.some((entry) => isZipFile(entry?.file));
    const archiveCopy = hasArchive ? "Extracting ZIP archive contents. " : "";
    return `Step 2 of 2: Upload complete. ${archiveCopy}Transforming file to match target data format.`;
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
    if (!busy) {
      button.textContent = "Import CSV files";
    } else if (busyPhase === "uploading") {
      button.textContent = "Uploading ...";
    } else if (busyPhase === "processing") {
      button.textContent = "Processing ...";
    } else {
      button.textContent = "Importing ...";
    }
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

    syncUploadProgress();
    syncSubmitState();
  }

  function setSelectedFiles(files) {
    selectedFiles = Array.from(files || [])
      .filter((file) => isCsvOrZipFile(file))
      .map((file) => buildSelectedFileEntry(file));
    latestResults = [];
    latestImportPayload = null;
    uploadProgress = null;
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
    const totalBytes = selectedFileEntries().reduce(
      (sum, entry) => sum + Number(entry.file?.size || 0),
      0
    );
    let transferredBytes = 0;

    const results = [];
    for (const entry of selectedFileEntries()) {
      const csvFiles = isZipFile(entry.file)
        ? await readCsvFilesFromZip(entry.file, {
            onProgress: ({ transferredBytes: extractedBytes }) => {
              setUploadProgress({
                label: "Extracting ...",
                transferredBytes: Math.min(totalBytes, transferredBytes + extractedBytes),
                totalBytes,
              });
            },
          })
        : [
            {
              fileName: resolvedSourceUploadFileName(entry),
              blob: entry.file,
              sizeBytes: entry.file.size,
            },
          ];

      for (const csvFile of csvFiles) {
        const storedEntry = await saveLocalWorkspaceExport({
          id: `local-workspace-csv-${Date.now().toString(36)}-${Math.random()
            .toString(36)
            .slice(2, 8)}`,
          fileName: csvFile.fileName,
          folderPath,
          exportFormat: "csv",
          mimeType: csvFile.blob.type || "text/csv",
          sizeBytes: csvFile.sizeBytes,
          createdAt: timestamp,
          updatedAt: timestamp,
          notebookTitle: "CSV Ingestion",
          cellId: "",
          columnCount: Array.isArray(previewState.columns) ? previewState.columns.length : 0,
          rowCount: 0,
          csvDelimiter: config.delimiterMode === "auto" ? previewState.delimiter || config.delimiter : config.delimiter,
          csvHasHeader: config.hasHeader,
          blob: csvFile.blob,
        });
        transferredBytes += Number(csvFile.sizeBytes || 0);
        setUploadProgress({
          label: "Saving ...",
          transferredBytes: Math.min(totalBytes, transferredBytes),
          totalBytes,
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
    }

    await renderLocalWorkspaceSidebarEntries();
    return results;
  }

  async function readJsonResponse(response, fallbackMessage) {
    let payload = null;
    try {
      payload = await response.json();
    } catch (_error) {
      // Ignore invalid JSON payloads from infrastructure errors.
    }
    if (!response.ok) {
      throw new Error(payload?.detail || fallbackMessage);
    }
    return payload || {};
  }

  function sleep(milliseconds) {
    return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
  }

  function isAmbiguousCompletionStatus(status) {
    return [502, 503, 504].includes(Number(status));
  }

  function csvUploadCompletionResult(payload) {
    if (Array.isArray(payload?.imports)) {
      return payload;
    }
    if (payload?.status === "completed" && payload?.result && typeof payload.result === "object") {
      return payload.result;
    }
    if (payload?.status === "failed") {
      throw new Error(payload?.error || "The CSV files could not be imported.");
    }
    return null;
  }

  async function waitForCsvUploadSessionCompletion(sessionId, fileEntries, totalBytes) {
    const deadline = Date.now() + CSV_UPLOAD_COMPLETION_TIMEOUT_MS;
    while (Date.now() < deadline) {
      const response = await window.fetch(
        `/api/ingestion/csv/upload-sessions/${encodeURIComponent(sessionId)}`,
        { headers: { Accept: "application/json" } }
      );
      const payload = await readJsonResponse(
        response,
        "The CSV upload processing status could not be read."
      );
      const result = csvUploadCompletionResult(payload);
      if (result) {
        return result;
      }
      setUploadProgress({
        label: "Processing ...",
        phase: "processing",
        detail: serverProcessingProgressDetail(fileEntries),
        transferredBytes: totalBytes,
        totalBytes,
        indeterminate: true,
      });
      await sleep(CSV_UPLOAD_COMPLETION_POLL_INTERVAL_MS);
    }
    throw new Error("The CSV upload processing step did not finish before the client-side wait timeout.");
  }

  async function readCsvUploadCompletionResult(response, sessionId, fileEntries, totalBytes) {
    if (isAmbiguousCompletionStatus(response.status)) {
      return waitForCsvUploadSessionCompletion(sessionId, fileEntries, totalBytes);
    }
    const payload = await readJsonResponse(
      response,
      "The CSV files could not be imported."
    );
    const result = csvUploadCompletionResult(payload);
    if (result) {
      return result;
    }
    return waitForCsvUploadSessionCompletion(sessionId, fileEntries, totalBytes);
  }

  function uploadCsvChunk({
    sessionId,
    fileId,
    chunkIndex,
    chunk,
    start,
    end,
    totalSize,
    progressBaseBytes,
    totalBytes,
    chunkNumber,
    totalChunks,
  }) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open(
        "PUT",
        `/api/ingestion/csv/upload-sessions/${encodeURIComponent(sessionId)}/files/${encodeURIComponent(fileId)}/chunks/${chunkIndex}`
      );
      xhr.setRequestHeader("Accept", "application/json");
      xhr.setRequestHeader("Content-Range", `bytes ${start}-${end - 1}/${totalSize}`);
      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable) {
          return;
        }
        setUploadProgress({
          label: "Uploading ...",
          transferredBytes: Math.min(totalBytes, progressBaseBytes + event.loaded),
          totalBytes,
          chunkNumber,
          totalChunks,
        });
      };
      xhr.onload = () => {
        let payload = {};
        try {
          payload = xhr.responseText ? JSON.parse(xhr.responseText) : {};
        } catch (_error) {
          reject(new Error("The CSV upload server returned an invalid response."));
          return;
        }
        if (xhr.status < 200 || xhr.status >= 300) {
          reject(new Error(payload?.detail || "The CSV upload chunk failed."));
          return;
        }
        resolve(payload);
      };
      xhr.onerror = () => reject(new Error("The CSV upload chunk failed."));
      xhr.ontimeout = () => reject(new Error("The CSV upload chunk timed out."));
      xhr.send(chunk);
    });
  }

  async function uploadCsvChunkWithRetries(options) {
    let lastError = null;
    for (let attempt = 0; attempt < CSV_UPLOAD_CHUNK_ATTEMPTS; attempt += 1) {
      try {
        return await uploadCsvChunk(options);
      } catch (error) {
        lastError = error;
        if (attempt + 1 < CSV_UPLOAD_CHUNK_ATTEMPTS) {
          await new Promise((resolve) => window.setTimeout(resolve, 300 * (attempt + 1)));
        }
      }
    }
    throw new Error(chunkUploadFailureMessage(options, lastError));
  }

  async function importToServerTarget() {
    const targetId = selectedTargetId();
    const config = currentConfig();
    const resolvedDelimiter =
      config.delimiterMode === "auto" ? previewState.delimiter || config.delimiter : config.delimiter;
    const fileEntries = selectedFileEntries();
    const totalBytes = fileEntries.reduce((sum, entry) => sum + Number(entry.file?.size || 0), 0);
    setUploadProgress({
      label: "Uploading ...",
      transferredBytes: 0,
      totalBytes,
    });

    const createResponse = await window.fetch("/api/ingestion/csv/upload-sessions", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        files: fileEntries.map((entry) => ({
          fileName: resolvedSourceUploadFileName(entry),
          sizeBytes: entry.file.size,
        })),
      }),
    });
    const session = await readJsonResponse(
      createResponse,
      "The CSV upload session could not be created."
    );

    const sessionId = String(session?.sessionId || "").trim();
    const sessionFiles = Array.isArray(session?.files) ? session.files : [];
    const chunkSize = Number(session?.chunkSizeBytes || DEFAULT_CSV_UPLOAD_CHUNK_BYTES);
    if (!sessionId || sessionFiles.length !== fileEntries.length || !Number.isFinite(chunkSize) || chunkSize < 1) {
      throw new Error("The CSV upload session response is incomplete.");
    }
    const totalChunks = fileEntries.reduce(
      (sum, entry) => sum + uploadChunkCount(entry.file?.size, chunkSize),
      0
    );

    let committedBytes = 0;
    let committedChunks = 0;
    let shouldDeleteSessionOnError = true;
    try {
      for (let fileIndex = 0; fileIndex < fileEntries.length; fileIndex += 1) {
        const entry = fileEntries[fileIndex];
        const sessionFile = sessionFiles[fileIndex];
        const fileId = String(sessionFile?.fileId || "").trim();
        if (!fileId) {
          throw new Error("The CSV upload session did not return a file id.");
        }
        let offset = Number(sessionFile?.receivedBytes || 0);
        committedBytes += offset;
        committedChunks += committedChunkCount(offset, entry.file.size, chunkSize);
        while (offset < entry.file.size) {
          const end = Math.min(offset + chunkSize, entry.file.size);
          const chunk = entry.file.slice(offset, end);
          const chunkIndex = Math.floor(offset / chunkSize);
          const chunkNumber = totalChunks > 0 ? Math.min(totalChunks, committedChunks + 1) : 0;
          await uploadCsvChunkWithRetries({
            sessionId,
            fileId,
            chunkIndex,
            chunk,
            start: offset,
            end,
            totalSize: entry.file.size,
            progressBaseBytes: committedBytes,
            totalBytes,
            chunkNumber,
            totalChunks,
          });
          const chunkBytes = end - offset;
          committedBytes += chunkBytes;
          committedChunks += 1;
          offset = end;
          setUploadProgress({
            label: "Uploading ...",
            transferredBytes: Math.min(totalBytes, committedBytes),
            totalBytes,
            chunkNumber: totalChunks > 0 ? Math.min(totalChunks, committedChunks) : 0,
            totalChunks,
          });
        }
      }

      busyPhase = "processing";
      setUploadProgress({
        label: "Processing ...",
        phase: "processing",
        detail: serverProcessingProgressDetail(fileEntries),
        transferredBytes: totalBytes,
        totalBytes,
        indeterminate: true,
      });
      const completeResponse = await window.fetch(
        `/api/ingestion/csv/upload-sessions/${encodeURIComponent(sessionId)}/complete`,
        {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            targetId,
            bucket: config.bucket,
            prefix: config.prefix,
            schemaName: config.schemaName,
            tablePrefix: config.tablePrefix,
            delimiter: resolvedDelimiter,
            hasHeader: config.hasHeader,
            replaceExisting: config.replaceExisting,
            storageFormat: config.s3StorageFormat,
          }),
        }
      );
      if (completeResponse.ok || isAmbiguousCompletionStatus(completeResponse.status)) {
        shouldDeleteSessionOnError = false;
      }
      let payload = {};
      try {
        payload = await readCsvUploadCompletionResult(
          completeResponse,
          sessionId,
          fileEntries,
          totalBytes
        );
      } catch (error) {
        throw new Error(processingFailureMessage(error, totalBytes));
      }
      await refreshSidebar("notebook");
      return payload && typeof payload === "object"
        ? payload
        : csvImportPayloadFromResults([], targetId);
    } catch (error) {
      if (shouldDeleteSessionOnError) {
        window.fetch(`/api/ingestion/csv/upload-sessions/${encodeURIComponent(sessionId)}`, {
          method: "DELETE",
          headers: { Accept: "application/json" },
        }).catch(() => {});
      }
      throw error;
    }
  }

  async function submitCsvIngestionForm() {
    if (!selectedFiles.length || busy) {
      return false;
    }

    busy = true;
    busyPhase = selectedTargetId() === "workspace.local" ? "processing" : "uploading";
    uploadProgress = null;
    latestResults = [];
    latestImportPayload = null;
    renderCsvIngestionWorkbench();

    try {
      if (selectedTargetId() === "workspace.local") {
        latestResults = await importToLocalWorkspace();
        latestImportPayload = csvImportPayloadFromResults(latestResults, "workspace.local");
      } else {
        latestImportPayload = await importToServerTarget();
        latestResults = csvImportResultsFromPayload(latestImportPayload);
      }
      renderCsvIngestionWorkbench();
      const completedCount = csvImportedCount(latestImportPayload, latestResults);
      await showMessageDialog({
        title: "CSV import finished",
        copy:
          selectedTargetId() === "workspace.local"
            ? `${completedCount} file(s) stored in Local Workspace (IndexDB) and ready for Query Workbench handoff.`
            : `${completedCount} file(s) processed for ${targetLabel()}.`,
      });
    } finally {
      busy = false;
      busyPhase = "";
      uploadProgress = null;
      renderCsvIngestionWorkbench();
    }

    return true;
  }

  function handleCsvIngestionInput(event) {
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

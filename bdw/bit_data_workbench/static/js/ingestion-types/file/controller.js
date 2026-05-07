import {
  FILE_ZIP_READER_LIMITS,
  buildZipPreviewState,
  isZipFile,
  readFilesFromZip,
} from "../csv/zip-reader.js";

const DEFAULT_UPLOAD_CHUNK_BYTES = 5 * 1024 * 1024;
const UPLOAD_COMPLETION_POLL_INTERVAL_MS = 2000;
const UPLOAD_COMPLETION_TIMEOUT_MS = 60 * 60 * 1000;

const FILE_INGESTORS = {
  parquet: {
    label: "Parquet",
    title: "Parquet import",
    extensions: [".parquet"],
    exportFormat: "parquet",
    mimeType: "application/vnd.apache.parquet",
    maxEntryBytes: FILE_ZIP_READER_LIMITS.maxLargeFileBytes,
  },
  json: {
    label: "JSON",
    title: "JSON import",
    extensions: [".json", ".jsonl", ".ndjson"],
    exportFormat: "json",
    mimeType: "application/json",
    maxEntryBytes: FILE_ZIP_READER_LIMITS.maxLargeFileBytes,
  },
  xlsx: {
    label: "Excel",
    title: "Excel import",
    extensions: [".xlsx"],
    exportFormat: "xlsx",
    mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    maxEntryBytes: FILE_ZIP_READER_LIMITS.maxTabularConversionBytes,
  },
  xml: {
    label: "XML",
    title: "XML import",
    extensions: [".xml"],
    exportFormat: "xml",
    mimeType: "application/xml",
    maxEntryBytes: FILE_ZIP_READER_LIMITS.maxTabularConversionBytes,
  },
};

function normalizeIdentifier(value, defaultPrefix) {
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

function normalizeTableName(fileName, prefix = "", defaultPrefix = "file") {
  const stem = String(fileName || "").replace(/\.[^.]+$/, "");
  const normalizedBase = normalizeIdentifier(stem, `${defaultPrefix}_import`);
  const normalizedPrefix = prefix
    ? normalizeIdentifier(prefix, defaultPrefix).replace(/^_+|_+$/g, "")
    : defaultPrefix;
  return normalizedPrefix ? `${normalizedPrefix}_${normalizedBase}` : normalizedBase;
}

function stateKey(form) {
  return String(form?.dataset?.fileIngestorId || "").trim().toLowerCase();
}

function selectedTargetId(form) {
  return String(form?.querySelector("[data-file-target-option]:checked")?.value || "workspace.local").trim();
}

function normalizePrefix(value) {
  return String(value || "")
    .split("/")
    .map((segment) => String(segment || "").trim())
    .filter(Boolean)
    .join("/");
}

function isSupportedFile(file, spec) {
  const fileName = String(file?.name || "").trim().toLowerCase();
  return isZipFile(file) || spec.extensions.some((extension) => fileName.endsWith(extension));
}

function buildSelectedEntry(file, ingestorId) {
  return {
    id: `${ingestorId}-file-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    file,
  };
}

function importedCount(payload, results) {
  if (payload && Number.isFinite(Number(payload.importedCount))) {
    return Number(payload.importedCount);
  }
  return (Array.isArray(results) ? results : []).filter(
    (item) => String(item?.status || "").trim().toLowerCase() === "imported"
  ).length;
}

export function createFileIngestionController(helpers) {
  const {
    ensureLocalWorkspaceFolderPath,
    escapeHtml,
    formatByteCount,
    localWorkspaceDisplayPath,
    localWorkspaceRelation,
    normalizeLocalWorkspaceFolderPath,
    renderLocalWorkspaceSidebarEntries,
    saveLocalWorkspaceExport,
    showMessageDialog,
  } = helpers;

  const states = new Map();

  function formState(form) {
    const ingestorId = stateKey(form);
    if (!states.has(ingestorId)) {
      states.set(ingestorId, {
        selectedFiles: [],
        busy: false,
        busyPhase: "",
        latestResults: [],
        latestImportPayload: null,
        uploadProgress: null,
        previewState: { status: "empty" },
        previewRequestVersion: 0,
      });
    }
    return states.get(ingestorId);
  }

  function specFor(form) {
    const ingestorId = stateKey(form);
    return FILE_INGESTORS[ingestorId] || null;
  }

  function currentConfig(form) {
    const targetId = selectedTargetId(form);
    const panel = form.querySelector(
      `[data-file-config-panel="${CSS.escape(targetId)}"]`
    );
    return {
      targetId,
      folderPath: normalizeLocalWorkspaceFolderPath(
        form.querySelector("[data-file-folder-path]")?.value || ""
      ),
      bucket: String(form.querySelector("[data-file-s3-bucket]")?.value || "").trim(),
      prefix: normalizePrefix(form.querySelector("[data-file-s3-prefix]")?.value || ""),
      schemaName: normalizeIdentifier(
        panel?.querySelector("[data-file-schema-name]")?.value || "public",
        "public"
      ),
      tablePrefix: String(panel?.querySelector("[data-file-table-prefix]")?.value || "").trim(),
      replaceExisting: true,
    };
  }

  function syncConfigPanels(form) {
    const targetId = selectedTargetId(form);
    form.querySelectorAll("[data-file-config-panel]").forEach((panel) => {
      panel.hidden = panel.dataset.fileConfigPanel !== targetId;
    });
    form.querySelectorAll(".ingestion-csv-target-card").forEach((card) => {
      const input = card.querySelector("[data-file-target-option]");
      card.classList.toggle("is-selected", Boolean(input?.checked));
    });
  }

  function fileListMarkup(form, state) {
    const spec = specFor(form);
    if (!state.selectedFiles.length) {
      return `<p class="ingestion-empty">No ${escapeHtml(spec?.label || "file")} or ZIP files selected yet.</p>`;
    }
    return state.selectedFiles
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

  function reviewMarkup(form, state) {
    if (!state.selectedFiles.length) {
      return '<p class="ingestion-empty">Select files to see the resolved destination names.</p>';
    }
    const spec = specFor(form);
    const config = currentConfig(form);
    return state.selectedFiles
      .map((entry) => {
        const targetCopy =
          config.targetId === "workspace.s3"
            ? `s3://${config.bucket || "<bucket>"}/${normalizePrefix(`${config.prefix}/${entry.file.name}`)}`
            : config.targetId === "workspace.local"
              ? localWorkspaceDisplayPath(config.folderPath, entry.file.name)
              : `${config.schemaName}.${normalizeTableName(
                  entry.file.name,
                  config.tablePrefix,
                  spec?.exportFormat || "file"
                )}`;
        return `
          <article class="ingestion-csv-review-card">
            <span class="ingestion-csv-review-name">${escapeHtml(entry.file.name)}</span>
            <span class="ingestion-csv-review-target">${escapeHtml(targetLabel(config.targetId))}</span>
            <span class="ingestion-csv-review-copy">
              ZIP archives are expanded server-side for Shared Workspace and PostgreSQL targets.
            </span>
            <code class="ingestion-csv-review-path">${escapeHtml(targetCopy)}</code>
          </article>
        `;
      })
      .join("");
  }

  function previewMarkup(form, state) {
    const spec = specFor(form);
    const preview = state.previewState || { status: "empty" };
    if (preview.status === "loading") {
      return `<p class="ingestion-empty">Inspecting ${escapeHtml(preview.fileName || spec?.label || "file")} ...</p>`;
    }
    if (preview.status === "error") {
      return `
        <article class="ingestion-csv-preview-card ingestion-csv-preview-card-error">
          <strong>${escapeHtml(preview.fileName || spec?.label || "file")}</strong>
          <p class="ingestion-csv-result-error">${escapeHtml(preview.error || "The file preview could not be generated.")}</p>
        </article>
      `;
    }
    if (Number.isFinite(Number(preview.archiveEntryCount))) {
      return `
        <article class="ingestion-csv-preview-card">
          <div class="ingestion-csv-preview-header">
            <strong>${escapeHtml(preview.fileName || "ZIP archive")}</strong>
            <span class="ingestion-csv-review-target">ZIP archive</span>
          </div>
          <p class="ingestion-csv-preview-copy">
            ${escapeHtml(String(preview.archiveEntryCount))} ${escapeHtml(spec?.label || "file")} file(s) will be extracted during import.
          </p>
        </article>
      `;
    }
    return `
      <article class="ingestion-csv-preview-card">
        <div class="ingestion-csv-preview-header">
          <strong>${escapeHtml(spec?.label || "File")}</strong>
          <span class="ingestion-csv-review-target">${escapeHtml((spec?.extensions || []).join(", "))}</span>
        </div>
        <p class="ingestion-csv-preview-copy">
          ${escapeHtml(formatRuleCopy(stateKey(form)))}
        </p>
      </article>
    `;
  }

  function resultMarkup(state) {
    if (!state.latestResults.length) {
      return "";
    }
    return state.latestResults
      .map((item) => {
        const status = String(item?.status || "").trim().toLowerCase();
        const imported = status === "imported";
        return `
          <article class="ingestion-csv-result-card ${
            imported
              ? "ingestion-csv-result-card-imported"
              : "ingestion-csv-result-card-failed"
          }">
            <div class="ingestion-csv-result-header">
              <strong>${escapeHtml(item?.fileName || item?.storedFileName || "Imported file")}</strong>
              <span class="ingestion-csv-result-status">${escapeHtml(imported ? "Imported" : "Failed")}</span>
            </div>
            ${
              imported
                ? `<p class="ingestion-csv-result-copy">${escapeHtml(item?.path || item?.relation || item?.objectKey || "Import completed.")}</p>`
                : `<p class="ingestion-csv-result-error">${escapeHtml(item?.error || "The file could not be imported.")}</p>`
            }
          </article>
        `;
      })
      .join("");
  }

  function syncUploadProgress(form, state) {
    const progressRoot = form.querySelector("[data-file-upload-progress]");
    if (!progressRoot) {
      return;
    }
    if (!state.uploadProgress) {
      progressRoot.hidden = true;
      progressRoot.innerHTML = "";
      return;
    }
    const totalBytes = Math.max(0, Number(state.uploadProgress.totalBytes || 0));
    const transferredBytes = Math.max(0, Number(state.uploadProgress.transferredBytes || 0));
    const percentage = totalBytes > 0 ? Math.min(100, Math.round((transferredBytes / totalBytes) * 100)) : 0;
    const detail = state.uploadProgress.detail
      || (state.uploadProgress.phase === "processing"
        ? "Step 2 of 2: Upload complete. Processing server-side import."
        : totalBytes > 0
          ? `Step 1 of 2: ${percentage}% uploaded`
          : "Step 1 of 2: Uploading ...");
    progressRoot.hidden = false;
    progressRoot.innerHTML = `
      <span class="ingestion-csv-upload-progress-copy">
        <strong>${escapeHtml(state.uploadProgress.label || "Uploading ...")}</strong>
        <span>${escapeHtml(detail)}</span>
      </span>
      <span class="ingestion-csv-upload-progress-track ${
        state.uploadProgress.indeterminate ? "is-indeterminate" : ""
      }" aria-hidden="true">
        <span style="width: ${escapeHtml(String(percentage))}%"></span>
      </span>
    `;
  }

  function syncSubmitState(form, state) {
    const button = form.querySelector("[data-file-import-submit]");
    if (!button) {
      return;
    }
    const spec = specFor(form);
    button.disabled = state.busy || !state.selectedFiles.length || state.previewState?.status === "error";
    if (!state.busy) {
      button.textContent = `Import ${spec?.label || "file"} files`;
    } else if (state.busyPhase === "uploading") {
      button.textContent = "Uploading ...";
    } else if (state.busyPhase === "processing") {
      button.textContent = "Processing ...";
    } else {
      button.textContent = "Importing ...";
    }
  }

  function renderForm(form) {
    const state = formState(form);
    syncConfigPanels(form);
    const fileList = form.querySelector("[data-file-list]");
    if (fileList) {
      fileList.innerHTML = fileListMarkup(form, state);
    }
    const reviewList = form.querySelector("[data-file-review-list]");
    if (reviewList) {
      reviewList.innerHTML = reviewMarkup(form, state);
    }
    const previewRoot = form.querySelector("[data-file-preview-root]");
    if (previewRoot) {
      previewRoot.innerHTML = previewMarkup(form, state);
    }
    const results = form.querySelector("[data-file-result-list]");
    if (results) {
      results.hidden = state.latestResults.length === 0;
      results.innerHTML = resultMarkup(state);
    }
    syncUploadProgress(form, state);
    syncSubmitState(form, state);
  }

  function renderAll() {
    document.querySelectorAll("[data-file-ingestion-form]").forEach((form) => renderForm(form));
  }

  function updateLandingSearch(input) {
    const query = String(input?.value || "").trim().toLowerCase();
    let visibleCount = 0;
    document.querySelectorAll("[data-ingestion-tile]").forEach((tile) => {
      const haystack = `${tile.dataset.ingestionSearchText || ""} ${tile.textContent || ""}`.toLowerCase();
      const visible = !query || haystack.includes(query);
      tile.hidden = !visible;
      if (visible) {
        visibleCount += 1;
      }
    });
    const noMatches = document.querySelector("[data-ingestion-no-matches]");
    if (noMatches) {
      noMatches.hidden = visibleCount !== 0;
    }
  }

  function setUploadProgress(form, nextProgress) {
    const state = formState(form);
    state.uploadProgress = nextProgress;
    syncUploadProgress(form, state);
    syncSubmitState(form, state);
  }

  async function refreshPreview(form) {
    const state = formState(form);
    const spec = specFor(form);
    const entry = state.selectedFiles[0];
    const file = entry?.file || null;
    if (!file || !spec) {
      state.previewState = { status: "empty" };
      renderForm(form);
      return;
    }
    const requestVersion = (state.previewRequestVersion += 1);
    state.previewState = { status: "loading", fileName: file.name };
    renderForm(form);
    try {
      const nextPreview = isZipFile(file)
        ? await buildZipPreviewState(file, {
            allowedExtensions: spec.extensions,
            formatLabel: spec.label,
            mimeType: spec.mimeType,
            maxEntryBytes: spec.maxEntryBytes,
          })
        : { status: "ready", fileName: file.name };
      if (requestVersion !== state.previewRequestVersion) {
        return;
      }
      state.previewState = nextPreview;
    } catch (error) {
      if (requestVersion !== state.previewRequestVersion) {
        return;
      }
      state.previewState = {
        status: "error",
        fileName: file.name,
        error: error instanceof Error ? error.message : "The file preview could not be generated.",
      };
    }
    renderForm(form);
  }

  function setSelectedFiles(form, files) {
    const state = formState(form);
    const spec = specFor(form);
    state.selectedFiles = Array.from(files || [])
      .filter((file) => spec && isSupportedFile(file, spec))
      .map((file) => buildSelectedEntry(file, stateKey(form)));
    state.latestResults = [];
    state.latestImportPayload = null;
    state.uploadProgress = null;
    renderForm(form);
    void refreshPreview(form);
  }

  async function importToLocalWorkspace(form) {
    const state = formState(form);
    const spec = specFor(form);
    const config = currentConfig(form);
    const folderPath = normalizeLocalWorkspaceFolderPath(config.folderPath);
    ensureLocalWorkspaceFolderPath(folderPath);
    const timestamp = new Date().toISOString();
    const totalBytes = state.selectedFiles.reduce(
      (sum, entry) => sum + Number(entry.file?.size || 0),
      0
    );
    let transferredBytes = 0;
    const results = [];
    for (const entry of state.selectedFiles) {
      const files = isZipFile(entry.file)
        ? await readFilesFromZip(entry.file, {
            allowedExtensions: spec.extensions,
            formatLabel: spec.label,
            mimeType: spec.mimeType,
            maxEntryBytes: spec.maxEntryBytes,
            onProgress: ({ transferredBytes: extractedBytes }) => {
              setUploadProgress(form, {
                label: "Extracting ...",
                transferredBytes: Math.min(totalBytes, transferredBytes + extractedBytes),
                totalBytes,
              });
            },
          })
        : [
            {
              fileName: entry.file.name,
              blob: entry.file,
              sizeBytes: entry.file.size,
            },
          ];
      for (const fileItem of files) {
        const storedEntry = await saveLocalWorkspaceExport({
          id: `local-workspace-${stateKey(form)}-${Date.now().toString(36)}-${Math.random()
            .toString(36)
            .slice(2, 8)}`,
          fileName: fileItem.fileName,
          folderPath,
          exportFormat: spec.exportFormat,
          mimeType: fileItem.blob.type || spec.mimeType,
          sizeBytes: fileItem.sizeBytes,
          createdAt: timestamp,
          updatedAt: timestamp,
          notebookTitle: `${spec.label} Ingestion`,
          cellId: "",
          columnCount: 0,
          rowCount: 0,
          blob: fileItem.blob,
        });
        transferredBytes += Number(fileItem.sizeBytes || 0);
        setUploadProgress(form, {
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

  function uploadCompletionResult(payload, spec) {
    if (Array.isArray(payload?.imports)) {
      return payload;
    }
    if (payload?.status === "completed" && payload?.result && typeof payload.result === "object") {
      return payload.result;
    }
    if (payload?.status === "failed") {
      throw new Error(payload?.error || `The ${spec.label} files could not be imported.`);
    }
    return null;
  }

  async function waitForUploadSessionCompletion(form, sessionId, totalBytes) {
    const spec = specFor(form);
    const ingestorId = stateKey(form);
    const deadline = Date.now() + UPLOAD_COMPLETION_TIMEOUT_MS;
    while (Date.now() < deadline) {
      const response = await window.fetch(
        `/api/ingestion/${encodeURIComponent(ingestorId)}/upload-sessions/${encodeURIComponent(sessionId)}`,
        { headers: { Accept: "application/json" } }
      );
      const payload = await readJsonResponse(
        response,
        `The ${spec.label} upload processing status could not be read.`
      );
      const result = uploadCompletionResult(payload, spec);
      if (result) {
        return result;
      }
      setUploadProgress(form, {
        label: "Processing ...",
        phase: "processing",
        detail: "Step 2 of 2: Upload complete. Processing server-side import.",
        transferredBytes: totalBytes,
        totalBytes,
        indeterminate: true,
      });
      await sleep(UPLOAD_COMPLETION_POLL_INTERVAL_MS);
    }
    throw new Error("The upload processing step did not finish before the client-side wait timeout.");
  }

  function uploadChunk(form, {
    sessionId,
    fileId,
    chunkIndex,
    chunk,
    start,
    end,
    totalSize,
    progressBaseBytes,
    totalBytes,
  }) {
    const ingestorId = stateKey(form);
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open(
        "PUT",
        `/api/ingestion/${encodeURIComponent(ingestorId)}/upload-sessions/${encodeURIComponent(sessionId)}/files/${encodeURIComponent(fileId)}/chunks/${chunkIndex}`
      );
      xhr.setRequestHeader("Accept", "application/json");
      xhr.setRequestHeader("Content-Range", `bytes ${start}-${end - 1}/${totalSize}`);
      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable) {
          return;
        }
        setUploadProgress(form, {
          label: "Uploading ...",
          transferredBytes: Math.min(totalBytes, progressBaseBytes + event.loaded),
          totalBytes,
        });
      };
      xhr.onload = () => {
        let payload = {};
        try {
          payload = xhr.responseText ? JSON.parse(xhr.responseText) : {};
        } catch (_error) {
          reject(new Error("The upload server returned an invalid response."));
          return;
        }
        if (xhr.status < 200 || xhr.status >= 300) {
          reject(new Error(payload?.detail || "The upload chunk failed."));
          return;
        }
        resolve(payload);
      };
      xhr.onerror = () => reject(new Error("The upload chunk failed because the network request failed."));
      xhr.send(chunk);
    });
  }

  async function importToServerTarget(form) {
    const state = formState(form);
    const spec = specFor(form);
    const config = currentConfig(form);
    const fileEntries = state.selectedFiles;
    const totalBytes = fileEntries.reduce((sum, entry) => sum + Number(entry.file?.size || 0), 0);
    const createResponse = await window.fetch(
      `/api/ingestion/${encodeURIComponent(stateKey(form))}/upload-sessions`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          files: fileEntries.map((entry) => ({
            fileName: entry.file.name,
            sizeBytes: entry.file.size,
          })),
        }),
      }
    );
    const sessionState = await readJsonResponse(
      createResponse,
      `The ${spec.label} upload session could not be created.`
    );
    const chunkSize = Math.max(
      1024 * 1024,
      Number(sessionState?.chunkSizeBytes || DEFAULT_UPLOAD_CHUNK_BYTES)
    );
    let progressBaseBytes = 0;
    for (const uploadFile of sessionState.files || []) {
      const entry = fileEntries.find((candidate) => candidate.file.name === uploadFile.fileName);
      if (!entry) {
        throw new Error(`The upload session returned an unknown file '${uploadFile.fileName}'.`);
      }
      let chunkIndex = 0;
      for (let start = 0; start < entry.file.size; start += chunkSize) {
        const end = Math.min(entry.file.size, start + chunkSize);
        await uploadChunk(form, {
          sessionId: sessionState.sessionId,
          fileId: uploadFile.fileId,
          chunkIndex,
          chunk: entry.file.slice(start, end),
          start,
          end,
          totalSize: entry.file.size,
          progressBaseBytes: progressBaseBytes + start,
          totalBytes,
        });
        chunkIndex += 1;
      }
      progressBaseBytes += entry.file.size;
    }
    setUploadProgress(form, {
      label: "Processing ...",
      phase: "processing",
      transferredBytes: totalBytes,
      totalBytes,
      indeterminate: true,
    });
    const completeResponse = await window.fetch(
      `/api/ingestion/${encodeURIComponent(stateKey(form))}/upload-sessions/${encodeURIComponent(sessionState.sessionId)}/complete`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          targetId: config.targetId,
          bucket: config.bucket,
          prefix: config.prefix,
          schemaName: config.schemaName,
          tablePrefix: config.tablePrefix,
          replaceExisting: config.replaceExisting,
        }),
      }
    );
    const payload = await readJsonResponse(
      completeResponse,
      `The ${spec.label} files could not be imported.`
    );
    const result = uploadCompletionResult(payload, spec);
    if (result) {
      return result;
    }
    return waitForUploadSessionCompletion(form, sessionState.sessionId, totalBytes);
  }

  function importPayloadFromResults(results, targetId) {
    const imports = Array.isArray(results) ? results : [];
    const count = imports.filter(
      (item) => String(item?.status || "").trim().toLowerCase() === "imported"
    ).length;
    return {
      targetId,
      importedCount: count,
      failedCount: Math.max(0, imports.length - count),
      imports,
    };
  }

  async function submitFileIngestionForm(form) {
    const state = formState(form);
    const spec = specFor(form);
    if (!form || !spec || !state.selectedFiles.length || state.busy) {
      return false;
    }
    state.busy = true;
    state.busyPhase = selectedTargetId(form) === "workspace.local" ? "processing" : "uploading";
    state.uploadProgress = null;
    state.latestResults = [];
    state.latestImportPayload = null;
    renderForm(form);
    try {
      if (selectedTargetId(form) === "workspace.local") {
        state.latestResults = await importToLocalWorkspace(form);
        state.latestImportPayload = importPayloadFromResults(state.latestResults, "workspace.local");
      } else {
        state.latestImportPayload = await importToServerTarget(form);
        state.latestResults = Array.isArray(state.latestImportPayload?.imports)
          ? state.latestImportPayload.imports
          : [];
      }
      renderForm(form);
      await showMessageDialog({
        title: `${spec.label} import finished`,
        copy: `${importedCount(state.latestImportPayload, state.latestResults)} file(s) processed for ${targetLabel(selectedTargetId(form))}.`,
      });
    } finally {
      state.busy = false;
      state.busyPhase = "";
      state.uploadProgress = null;
      renderForm(form);
    }
    return true;
  }

  function handleFileIngestionInput(event) {
    const searchInput = event.target.closest("[data-ingestion-search-input]");
    if (searchInput instanceof HTMLInputElement) {
      updateLandingSearch(searchInput);
      return true;
    }
    const relevantInput = event.target.closest(
      "[data-file-folder-path], [data-file-s3-bucket], [data-file-s3-prefix], [data-file-schema-name], [data-file-table-prefix]"
    );
    const form = relevantInput?.closest("[data-file-ingestion-form]");
    if (!form) {
      return false;
    }
    renderForm(form);
    return true;
  }

  function handleFileIngestionChange(event) {
    const fileInput = event.target.closest("[data-file-input]");
    if (fileInput instanceof HTMLInputElement) {
      const form = fileInput.closest("[data-file-ingestion-form]");
      if (form) {
        setSelectedFiles(form, fileInput.files || []);
        return true;
      }
    }
    const targetOption = event.target.closest("[data-file-target-option]");
    const form = targetOption?.closest("[data-file-ingestion-form]");
    if (form) {
      renderForm(form);
      return true;
    }
    return false;
  }

  function handleFileDrop(event) {
    const dropzone = event.target.closest("[data-file-dropzone]");
    if (!dropzone) {
      return false;
    }
    const form = dropzone.closest("[data-file-ingestion-form]");
    if (!form) {
      return false;
    }
    event.preventDefault();
    dropzone.classList.remove("is-drag-over");
    if (event.dataTransfer?.files?.length) {
      setSelectedFiles(form, event.dataTransfer.files);
      const input = form.querySelector("[data-file-input]");
      if (input instanceof HTMLInputElement) {
        try {
          input.files = event.dataTransfer.files;
        } catch (_error) {
          // Some browsers expose a read-only FileList; state already holds the files.
        }
      }
    }
    return true;
  }

  function handleFileDragOver(event) {
    const dropzone = event.target.closest("[data-file-dropzone]");
    if (!dropzone) {
      return false;
    }
    event.preventDefault();
    dropzone.classList.add("is-drag-over");
    return true;
  }

  function handleFileDragLeave(event) {
    const dropzone = event.target.closest("[data-file-dropzone]");
    if (!dropzone) {
      return false;
    }
    dropzone.classList.remove("is-drag-over");
    return true;
  }

  return {
    handleFileDragLeave,
    handleFileDragOver,
    handleFileDrop,
    handleFileIngestionChange,
    handleFileIngestionInput,
    renderFileIngestionWorkbench: renderAll,
    submitFileIngestionForm,
  };
}

function targetLabel(targetId) {
  if (targetId === "workspace.s3") {
    return "Shared Workspace S3";
  }
  if (targetId === "pg_oltp") {
    return "PostgreSQL OLTP";
  }
  if (targetId === "pg_olap") {
    return "PostgreSQL OLAP";
  }
  return "Local Workspace (IndexDB)";
}

function formatRuleCopy(ingestorId) {
  if (ingestorId === "xml") {
    return "XML support expects a simple table-like document: one root element containing repeated row elements, where each row has child elements that become columns. Attributes, deeply nested structures, mixed content, and multiple unrelated record types are not mapped in this first version.";
  }
  if (ingestorId === "xlsx") {
    return "Excel support imports the active worksheet only. The first non-empty row becomes the header row; following non-empty rows become records. Formulas are read as stored values.";
  }
  if (ingestorId === "json") {
    return "JSON support uses DuckDB automatic JSON inference. JSONL/NDJSON is preferred for large datasets.";
  }
  return "Parquet is imported directly and is the preferred format for repeated analytical scans.";
}

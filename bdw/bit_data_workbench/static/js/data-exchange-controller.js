export function createDataExchangeController(helpers) {
  const {
    createLocalWorkspaceEntryId,
    escapeHtml,
    fetchJsonOrThrow,
    formatByteCount,
    formatRelativeTimestamp,
    openQueryWorkbenchDataSources,
    refreshSidebar,
    renderLocalWorkspaceSidebarEntries,
    saveLocalWorkspaceExport,
    showConfirmDialog,
    showMessageDialog,
    syncLocalWorkspaceEntry,
  } = helpers;

  const apiRoot = "/api/data-exchange";
  let selectedFiles = [];
  let exchangeFiles = [];
  let exchangeFolders = [];
  let selectedFileId = "";
  let editFileId = "";
  let copyFileId = "";
  let createFolderParentId = "";

  function pageRoot() {
    return document.querySelector("[data-data-exchange-page]");
  }

  function query(selector, root = pageRoot()) {
    return root?.querySelector?.(selector) ?? null;
  }

  function headers(extra = {}) {
    return {
      Accept: "application/json",
      ...extra,
    };
  }

  function jsonHeaders() {
    return headers({ "Content-Type": "application/json" });
  }

  function tagsFromInput(value = "") {
    const seen = new Set();
    return String(value || "")
      .split(",")
      .map((tag) => tag.trim())
      .filter((tag) => {
        if (!tag) {
          return false;
        }
        const key = tag.toLowerCase();
        if (seen.has(key)) {
          return false;
        }
        seen.add(key);
        return true;
      });
  }

  function fileExtension(file) {
    return String(file?.extension || file?.fileName?.split(".").pop() || "")
      .trim()
      .toLowerCase();
  }

  function selectedFile() {
    return exchangeFiles.find((candidate) => candidate.fileId === selectedFileId) || null;
  }

  function folderById(folderId) {
    const normalizedFolderId = String(folderId || "");
    return exchangeFolders.find((candidate) => candidate.folderId === normalizedFolderId) || null;
  }

  function folderPath(folderId) {
    const path = [];
    const seen = new Set();
    let current = folderById(folderId);
    while (current && !seen.has(current.folderId)) {
      seen.add(current.folderId);
      path.unshift(current.name || "Folder");
      current = folderById(current.parentFolderId);
    }
    return path;
  }

  function folderLabel(folderId) {
    const path = folderPath(folderId);
    return path.length ? path.join(" / ") : "Exchange Files";
  }

  function selectedFilePassword() {
    const file = selectedFile();
    if (!file?.hasPassword) {
      return "";
    }
    return String(query("[data-data-exchange-detail-password]")?.value || "");
  }

  function setStatus(selector, message = "") {
    const target = query(selector);
    if (target) {
      target.textContent = message;
    }
  }

  function setUploadVisible(visible) {
    const panel = query("[data-data-exchange-upload-panel]");
    const toggle = query("[data-data-exchange-show-upload]");
    if (visible) {
      panel?.removeAttribute("hidden");
      toggle?.setAttribute("hidden", "");
      return;
    }
    panel?.setAttribute("hidden", "");
    toggle?.removeAttribute("hidden");
  }

  function selectedFileMarkup(file) {
    return `
      <article class="ingestion-csv-file-card">
        <strong>${escapeHtml(file.name)}</strong>
        <span class="ingestion-csv-result-copy">${escapeHtml(formatByteCount(file.size))}</span>
      </article>
    `;
  }

  function renderSelectedFiles() {
    const root = query("[data-data-exchange-selected-files]");
    if (!root) {
      return;
    }
    if (!selectedFiles.length) {
      root.innerHTML = `<p class="ingestion-empty">No files selected yet.</p>`;
      return;
    }
    root.innerHTML = selectedFiles.map((file) => selectedFileMarkup(file)).join("");
  }

  function searchMatches(file, queryText) {
    const needle = String(queryText || "").trim().toLowerCase();
    if (!needle) {
      return true;
    }
    const haystack = [
      file.fileName,
      file.displayName,
      file.description,
      file.ownerContact,
      file.contentType,
      file.extension,
      folderLabel(file.folderId),
      file.hasPassword ? "protected locked password" : "unlocked unprotected public",
      ...(file.tags || []),
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(needle);
  }

  function filteredFiles() {
    const searchText = String(query("[data-data-exchange-search]")?.value || "");
    return exchangeFiles.filter((file) => searchMatches(file, searchText));
  }

  function folderSearchMatches(folder, queryText) {
    const needle = String(queryText || "").trim().toLowerCase();
    if (!needle) {
      return true;
    }
    return folderLabel(folder.folderId).toLowerCase().includes(needle);
  }

  function folderChildrenMap() {
    const children = new Map();
    for (const folder of exchangeFolders) {
      const parentId = String(folder.parentFolderId || "");
      if (!children.has(parentId)) {
        children.set(parentId, []);
      }
      children.get(parentId).push(folder);
    }
    for (const folderList of children.values()) {
      folderList.sort((left, right) =>
        String(left.name || "").localeCompare(String(right.name || ""), undefined, {
          sensitivity: "base",
        })
      );
    }
    return children;
  }

  function folderOptionsMarkup(selectedId = "") {
    const children = folderChildrenMap();
    const optionRows = [`<option value="">Exchange Files</option>`];
    function visit(parentId, depth) {
      for (const folder of children.get(parentId) || []) {
        const folderId = String(folder.folderId || "");
        const indent = "\u00a0\u00a0".repeat(depth);
        optionRows.push(
          `<option value="${escapeHtml(folderId)}" ${
            folderId === selectedId ? "selected" : ""
          }>${indent}${escapeHtml(folder.name || "Folder")}</option>`
        );
        visit(folderId, depth + 1);
      }
    }
    visit("", 0);
    return optionRows.join("");
  }

  function renderFolderSelectors() {
    const selectors = [
      query("[data-data-exchange-folder]"),
      query("[data-data-exchange-edit-folder]"),
    ].filter(Boolean);
    for (const select of selectors) {
      const selectedId = String(select.value || "");
      select.innerHTML = folderOptionsMarkup(selectedId);
      if (selectedId && folderById(selectedId)) {
        select.value = selectedId;
      }
    }
  }

  function lockIconMarkup(file) {
    if (!file.hasPassword) {
      return "";
    }
    return `
      <span class="data-exchange-lock" title="Password protected" aria-label="Password protected">
        <svg viewBox="0 0 16 16" aria-hidden="true">
          <rect x="3.2" y="7" width="9.6" height="6.4" rx="1.2"></rect>
          <path d="M5.1 7V5.2a2.9 2.9 0 0 1 5.8 0V7"></path>
        </svg>
      </span>
    `;
  }

  function sourceCatalogIconMarkup() {
    return `
      <svg class="source-icon source-icon-catalog" viewBox="0 0 16 16" aria-hidden="true">
        <ellipse cx="8" cy="3.3" rx="4.8" ry="1.9"></ellipse>
        <path d="M3.2 3.3v8c0 1 2.15 1.9 4.8 1.9s4.8-.9 4.8-1.9v-8"></path>
        <path d="M3.2 7c0 1 2.15 1.9 4.8 1.9s4.8-0.9 4.8-1.9"></path>
      </svg>
    `;
  }

  function sourceSchemaIconMarkup() {
    return `
      <svg class="source-icon source-icon-schema" viewBox="0 0 16 16" aria-hidden="true">
        <rect x="1.6" y="1.8" width="3.7" height="3.7" rx="0.7"></rect>
        <rect x="10.7" y="1.8" width="3.7" height="3.7" rx="0.7"></rect>
        <rect x="10.7" y="10.5" width="3.7" height="3.7" rx="0.7"></rect>
        <path d="M5.3 3.7h2.8a2 2 0 0 1 2 2v1.5"></path>
        <path d="M10.1 8H7.4a2 2 0 0 0-2 2v.5"></path>
      </svg>
    `;
  }

  function sourceFileIconMarkup() {
    return `
      <svg class="source-icon data-exchange-file-icon" viewBox="0 0 16 16" aria-hidden="true">
        <path d="M4 1.8h5.2L12.8 5.4v8.8H4z"></path>
        <path d="M9.2 1.8v3.6h3.6"></path>
        <path d="M5.9 8h4.3"></path>
        <path d="M5.9 10.4h4.3"></path>
      </svg>
    `;
  }

  function createFolderButtonMarkup(parentFolderId = "") {
    return `
      <button
        type="button"
        class="source-control-button source-control-button-create data-exchange-create-folder-button"
        data-data-exchange-create-folder
        data-data-exchange-folder-parent="${escapeHtml(parentFolderId)}"
        title="Create folder"
        aria-label="Create folder"
      >
        +
      </button>
    `;
  }

  function fileButtonMarkup(file) {
    const extension = fileExtension(file) || "file";
    const active = file.fileId === selectedFileId ? " is-selected" : "";
    const protectedLabel = file.hasPassword ? "Protected" : "Open";
    const displayName = file.displayName || "";
    const displayNameMeta =
      displayName && displayName !== file.fileName ? `<small>${escapeHtml(displayName)}</small>` : "";
    return `
      <li>
        <button
          type="button"
          class="source-object source-object-table data-exchange-file-row${active}"
          data-data-exchange-file-row="${escapeHtml(file.fileId)}"
          title="${escapeHtml(file.fileName)}"
        >
          <span class="source-node-label">
            ${sourceFileIconMarkup()}
            <span>${escapeHtml(file.fileName)}</span>
          </span>
          <span class="source-object-meta">
            ${lockIconMarkup(file)}
            ${displayNameMeta}
            <small>${escapeHtml(extension.toUpperCase())}</small>
            <small>${escapeHtml(formatByteCount(file.sizeBytes))}</small>
            <small>${escapeHtml(protectedLabel)}</small>
          </span>
        </button>
      </li>
    `;
  }

  function folderNodeMarkup(folder, visibleFiles, children, queryText) {
    const folderId = String(folder.folderId || "");
    const directFiles = visibleFiles.filter((file) => String(file.folderId || "") === folderId);
    const childMarkup = (children.get(folderId) || [])
      .map((child) => folderNodeMarkup(child, visibleFiles, children, queryText))
      .filter(Boolean)
      .join("");
    if (
      String(queryText || "").trim()
      && !folderSearchMatches(folder, queryText)
      && !directFiles.length
      && !childMarkup
    ) {
      return "";
    }
    const itemCount = directFiles.length;
    return `
      <details class="source-schema data-exchange-folder-node" data-data-exchange-folder-id="${escapeHtml(folderId)}" open>
        <summary>
          <span class="source-node-label">
            ${sourceSchemaIconMarkup()}
            <span title="${escapeHtml(folderLabel(folderId))}">${escapeHtml(folder.name || "Folder")}</span>
          </span>
          <span class="source-schema-meta">
            <small>${itemCount} item(s)</small>
            ${createFolderButtonMarkup(folderId)}
          </span>
        </summary>
        ${
          directFiles.length
            ? `<ul class="source-object-list data-exchange-source-object-list">
                ${directFiles.map((file) => fileButtonMarkup(file)).join("")}
              </ul>`
            : ""
        }
        ${childMarkup}
      </details>
    `;
  }

  function renderFileList() {
    const root = query("[data-data-exchange-file-list]");
    if (!root) {
      return;
    }
    const visibleFiles = filteredFiles();
    const searchText = String(query("[data-data-exchange-search]")?.value || "");
    const children = folderChildrenMap();
    const rootFolderMarkup = (children.get("") || [])
      .map((folder) => folderNodeMarkup(folder, visibleFiles, children, searchText))
      .filter(Boolean)
      .join("");
    const rootFiles = visibleFiles.filter((file) => !String(file.folderId || ""));
    if (!visibleFiles.length && !rootFolderMarkup && searchText.trim()) {
      root.innerHTML = `<div class="data-source-explorer-empty"><p>${
        exchangeFiles.length ? "No DataExchange files match this search." : "No files have been uploaded yet."
      }</p></div>`;
      selectedFileId = "";
      renderFileDetail();
      return;
    }
    if (!visibleFiles.some((file) => file.fileId === selectedFileId)) {
      selectedFileId = visibleFiles[0]?.fileId || "";
    }
    root.innerHTML = `
      <div class="source-tree data-exchange-source-tree" data-source-tree-scope="data-exchange">
        <details class="source-catalog data-exchange-source-catalog" open>
          <summary>
            <span class="source-node-label">
              ${sourceCatalogIconMarkup()}
              <span>DataExchange (S3)</span>
            </span>
            <span class="source-catalog-meta">
              <span class="source-publication-pill">Hidden Source</span>
            </span>
          </summary>
          <details class="source-schema data-exchange-source-schema" open>
            <summary>
              <span class="source-node-label">
                ${sourceSchemaIconMarkup()}
                <span>Exchange Files</span>
              </span>
              <span class="source-schema-meta">
                <small>${visibleFiles.length} item(s)</small>
                ${createFolderButtonMarkup("")}
              </span>
            </summary>
            ${
              rootFiles.length
                ? `<ul class="source-object-list data-exchange-source-object-list">
                    ${rootFiles.map((file) => fileButtonMarkup(file)).join("")}
                  </ul>`
                : ""
            }
            ${rootFolderMarkup}
            ${
              !visibleFiles.length && !rootFolderMarkup
                ? `<div class="data-source-explorer-empty data-exchange-tree-empty">
                    <p>No files have been uploaded yet.</p>
                  </div>`
                : ""
            }
          </details>
        </details>
      </div>
    `;
    renderFileDetail();
  }

  function detailMetricMarkup(label, value) {
    return `
      <div>
        <dt>${escapeHtml(label)}</dt>
        <dd>${escapeHtml(value || "Not provided")}</dd>
      </div>
    `;
  }

  function fileDetailMarkup(file) {
    const tagsMarkup = (file.tags || [])
      .map((tag) => `<span class="ingestion-generator-tag">${escapeHtml(tag)}</span>`)
      .join("");
    const extension = fileExtension(file) || "file";
    const passwordField = file.hasPassword
      ? `
        <label class="result-export-field data-exchange-detail-password">
          <span class="result-export-field-label">File password</span>
          <input
            class="modal-input"
            type="password"
            autocomplete="current-password"
            data-data-exchange-detail-password
          >
        </label>
      `
      : `<p class="data-exchange-password-note">No password is required for this file.</p>`;
    return `
      <article class="data-source-explorer-detail-card data-exchange-detail-card">
        <div class="data-source-explorer-detail-copy">
          <h4>
            ${escapeHtml(file.displayName || file.fileName)}
            ${lockIconMarkup(file)}
          </h4>
          <p>${escapeHtml(file.fileName)} - ${escapeHtml(extension.toUpperCase())}</p>
        </div>
        <p class="data-exchange-description">${
          escapeHtml(file.description || (file.isQueryable
            ? "Can be copied into a queryable workspace."
            : "Downloadable exchange asset only."))
        }</p>
        <dl class="data-exchange-facts">
          ${detailMetricMarkup("Owner/contact", file.ownerContact)}
          ${detailMetricMarkup("Uploaded", formatRelativeTimestamp(file.uploadedAt))}
          ${detailMetricMarkup("Updated", formatRelativeTimestamp(file.updatedAt))}
          ${detailMetricMarkup("Size", formatByteCount(file.sizeBytes))}
          ${detailMetricMarkup("Type", file.contentType || "application/octet-stream")}
          ${detailMetricMarkup("Folder", folderLabel(file.folderId))}
          ${detailMetricMarkup("Access", file.hasPassword ? "Password protected" : "No password")}
        </dl>
        ${tagsMarkup ? `<div class="ingestion-generator-tags">${tagsMarkup}</div>` : ""}
        ${passwordField}
        <div class="data-source-explorer-action-row">
          <button type="button" class="data-source-explorer-action" data-data-exchange-download>
            Download
          </button>
          <button type="button" class="data-source-explorer-action" data-data-exchange-edit>
            Edit metadata
          </button>
          <button type="button" class="data-source-explorer-action" data-data-exchange-copy-s3 ${
            file.isQueryable ? "" : "disabled"
          }>
            Copy to Shared S3
          </button>
          <button type="button" class="data-source-explorer-action" data-data-exchange-copy-local ${
            file.isQueryable ? "" : "disabled"
          }>
            Copy to Local Workspace
          </button>
          <button type="button" class="data-source-explorer-action" data-tone="danger" data-data-exchange-delete>
            Delete
          </button>
        </div>
      </article>
    `;
  }

  function renderFileDetail() {
    const root = query("[data-data-exchange-file-detail]");
    if (!root) {
      return;
    }
    const file = selectedFile();
    if (!file) {
      root.innerHTML = `
        <div class="data-source-explorer-empty">
          <p>Select a file to view metadata and actions.</p>
        </div>
      `;
      return;
    }
    root.innerHTML = fileDetailMarkup(file);
  }

  async function loadFiles() {
    const payload = await fetchJsonOrThrow(`${apiRoot}/files`, {
      headers: headers(),
    });
    exchangeFiles = Array.isArray(payload.files) ? payload.files : [];
    exchangeFolders = Array.isArray(payload.folders) ? payload.folders : [];
    renderFolderSelectors();
    renderFileList();
  }

  function onFileInputChange(event) {
    selectedFiles = Array.from(event.target?.files || []);
    renderSelectedFiles();
  }

  function onDrop(event) {
    const dropzone = event.target.closest("[data-data-exchange-dropzone]");
    if (!dropzone) {
      return false;
    }
    event.preventDefault();
    dropzone.classList.remove("is-drag-over");
    selectedFiles = Array.from(event.dataTransfer?.files || []);
    const input = query("[data-data-exchange-file-input]");
    if (input) {
      input.files = event.dataTransfer.files;
    }
    renderSelectedFiles();
    return true;
  }

  function onDragOver(event) {
    const dropzone = event.target.closest("[data-data-exchange-dropzone]");
    if (!dropzone) {
      return false;
    }
    event.preventDefault();
    dropzone.classList.add("is-drag-over");
    return true;
  }

  function onDragLeave(event) {
    const dropzone = event.target.closest("[data-data-exchange-dropzone]");
    if (!dropzone) {
      return false;
    }
    dropzone.classList.remove("is-drag-over");
    return true;
  }

  async function uploadFileChunk(sessionId, fileEntry, file, offset, chunkIndex, chunkSize) {
    const end = Math.min(offset + chunkSize, file.size);
    const chunk = file.slice(offset, end);
    const response = await window.fetch(
      `${apiRoot}/upload-sessions/${encodeURIComponent(sessionId)}/files/${encodeURIComponent(
        fileEntry.fileId
      )}/chunks/${chunkIndex}`,
      {
        method: "PUT",
        headers: headers({
          "Content-Range": `bytes ${offset}-${end - 1}/${file.size}`,
        }),
        body: chunk,
      }
    );
    if (!response.ok) {
      throw new Error(await response.text());
    }
  }

  async function pollUploadResult(sessionId) {
    const deadline = Date.now() + 120_000;
    while (Date.now() < deadline) {
      const state = await fetchJsonOrThrow(`${apiRoot}/upload-sessions/${encodeURIComponent(sessionId)}`, {
        headers: headers(),
      });
      if (state?.result) {
        return state.result;
      }
      if (state?.status === "failed") {
        throw new Error(state.error || "The DataExchange upload failed.");
      }
      await new Promise((resolve) => window.setTimeout(resolve, 500));
    }
    throw new Error("Timed out waiting for DataExchange upload processing.");
  }

  async function submitUpload(event) {
    event.preventDefault();
    if (!selectedFiles.length) {
      await showMessageDialog({
        title: "No files selected",
        copy: "Choose one or more files before uploading to DataExchange.",
      });
      return;
    }

    const submitButton = query("[data-data-exchange-upload-submit]");
    submitButton?.setAttribute("disabled", "");
    setStatus("[data-data-exchange-upload-status]", "Creating upload session...");
    try {
      const session = await fetchJsonOrThrow(`${apiRoot}/upload-sessions`, {
        method: "POST",
        headers: jsonHeaders(),
        body: JSON.stringify({
          files: selectedFiles.map((file) => ({
            fileName: file.name,
            sizeBytes: file.size,
          })),
        }),
      });
      const sessionId = String(session.sessionId || "");
      const chunkSize = Number(session.chunkSizeBytes || 5 * 1024 * 1024);
      const sessionFiles = Array.isArray(session.files) ? session.files : [];
      for (let index = 0; index < selectedFiles.length; index += 1) {
        const file = selectedFiles[index];
        const fileEntry = sessionFiles[index];
        if (!fileEntry?.fileId) {
          throw new Error("The upload session did not return a file id.");
        }
        for (let offset = 0; offset < file.size; offset += chunkSize) {
          setStatus(
            "[data-data-exchange-upload-status]",
            `Uploading ${file.name} (${Math.min(offset + chunkSize, file.size)} of ${file.size} bytes)...`
          );
          await uploadFileChunk(
            sessionId,
            fileEntry,
            file,
            offset,
            Math.floor(offset / chunkSize),
            chunkSize
          );
        }
      }

      setStatus("[data-data-exchange-upload-status]", "Finalizing upload...");
      const completeState = await fetchJsonOrThrow(
        `${apiRoot}/upload-sessions/${encodeURIComponent(sessionId)}/complete`,
        {
          method: "POST",
          headers: jsonHeaders(),
          body: JSON.stringify({
            filePassword: query("[data-data-exchange-file-password]")?.value || "",
            displayName: query("[data-data-exchange-display-name]")?.value || "",
            description: query("[data-data-exchange-description]")?.value || "",
            ownerContact: query("[data-data-exchange-owner-contact]")?.value || "",
            tags: tagsFromInput(query("[data-data-exchange-tags]")?.value || ""),
            folderId: query("[data-data-exchange-folder]")?.value || "",
          }),
        }
      );
      const result = completeState.result || (await pollUploadResult(sessionId));
      selectedFiles = [];
      const fileInput = query("[data-data-exchange-file-input]");
      const filePasswordInput = query("[data-data-exchange-file-password]");
      if (fileInput) {
        fileInput.value = "";
      }
      if (filePasswordInput) {
        filePasswordInput.value = "";
      }
      renderSelectedFiles();
      await loadFiles();
      setUploadVisible(false);
      setStatus("[data-data-exchange-upload-status]", "");
      await showMessageDialog({
        title: "DataExchange upload complete",
        copy: `${result.importedCount || 0} file(s) uploaded, ${result.failedCount || 0} failed.`,
      });
    } catch (error) {
      setStatus("[data-data-exchange-upload-status]", "");
      await showMessageDialog({
        title: "DataExchange upload failed",
        copy: error instanceof Error ? error.message : "The files could not be uploaded.",
      });
    } finally {
      submitButton?.removeAttribute("disabled");
    }
  }

  async function createDownloadToken(fileId, filePassword) {
    return fetchJsonOrThrow(`${apiRoot}/files/${encodeURIComponent(fileId)}/download-token`, {
      method: "POST",
      headers: jsonHeaders(),
      body: JSON.stringify({ filePassword }),
    });
  }

  async function downloadFile(fileId) {
    const token = await createDownloadToken(fileId, selectedFilePassword());
    const link = document.createElement("a");
    link.href = token.downloadUrl;
    link.download = "";
    document.body.appendChild(link);
    link.click();
    link.remove();
  }

  function openEditDialog(fileId) {
    const file = exchangeFiles.find((candidate) => candidate.fileId === fileId);
    if (!file) {
      return;
    }
    editFileId = fileId;
    const passwordInput = query("[data-data-exchange-edit-password]");
    const passwordField = query("[data-data-exchange-edit-password-field]");
    const displayNameInput = query("[data-data-exchange-edit-display-name]");
    const ownerInput = query("[data-data-exchange-edit-owner-contact]");
    const descriptionInput = query("[data-data-exchange-edit-description]");
    const tagsInput = query("[data-data-exchange-edit-tags]");
    const folderInput = query("[data-data-exchange-edit-folder]");
    if (passwordField) {
      passwordField.hidden = !file.hasPassword;
    }
    if (passwordInput) {
      passwordInput.required = Boolean(file.hasPassword);
      passwordInput.value = selectedFilePassword();
    }
    if (displayNameInput) {
      displayNameInput.value = file.displayName || file.fileName || "";
    }
    if (ownerInput) {
      ownerInput.value = file.ownerContact || "";
    }
    if (descriptionInput) {
      descriptionInput.value = file.description || "";
    }
    if (tagsInput) {
      tagsInput.value = (file.tags || []).join(", ");
    }
    if (folderInput) {
      folderInput.innerHTML = folderOptionsMarkup(String(file.folderId || ""));
      folderInput.value = String(file.folderId || "");
    }
    query("[data-data-exchange-edit-dialog]")?.showModal();
  }

  async function submitEdit(event) {
    event.preventDefault();
    if (!editFileId) {
      return;
    }
    try {
      await fetchJsonOrThrow(`${apiRoot}/files/${encodeURIComponent(editFileId)}`, {
        method: "PATCH",
        headers: jsonHeaders(),
        body: JSON.stringify({
          filePassword: query("[data-data-exchange-edit-password]")?.value || "",
          displayName: query("[data-data-exchange-edit-display-name]")?.value || "",
          ownerContact: query("[data-data-exchange-edit-owner-contact]")?.value || "",
          description: query("[data-data-exchange-edit-description]")?.value || "",
          tags: tagsFromInput(query("[data-data-exchange-edit-tags]")?.value || ""),
          folderId: query("[data-data-exchange-edit-folder]")?.value || "",
        }),
      });
      query("[data-data-exchange-edit-dialog]")?.close();
      await loadFiles();
    } catch (error) {
      await showMessageDialog({
        title: "Metadata update failed",
        copy: error instanceof Error ? error.message : "The metadata could not be saved.",
      });
    }
  }

  function openCreateFolderDialog(parentFolderId = "") {
    createFolderParentId = String(parentFolderId || "");
    const parentLabel = query("[data-data-exchange-folder-parent-label]");
    const nameInput = query("[data-data-exchange-folder-name]");
    if (parentLabel) {
      parentLabel.textContent = folderLabel(createFolderParentId);
    }
    if (nameInput) {
      nameInput.value = "";
    }
    query("[data-data-exchange-folder-dialog]")?.showModal();
    nameInput?.focus();
  }

  async function submitCreateFolder(event) {
    event.preventDefault();
    const name = String(query("[data-data-exchange-folder-name]")?.value || "").trim();
    if (!name) {
      await showMessageDialog({
        title: "Folder name required",
        copy: "Choose a name before creating the DataExchange folder.",
      });
      return;
    }
    try {
      await fetchJsonOrThrow(`${apiRoot}/folders`, {
        method: "POST",
        headers: jsonHeaders(),
        body: JSON.stringify({
          name,
          parentFolderId: createFolderParentId,
        }),
      });
      query("[data-data-exchange-folder-dialog]")?.close();
      await loadFiles();
    } catch (error) {
      await showMessageDialog({
        title: "Folder creation failed",
        copy: error instanceof Error ? error.message : "The folder could not be created.",
      });
    }
  }

  async function deleteFile(fileId) {
    const confirmed = await showConfirmDialog({
      title: "Delete DataExchange file?",
      copy: "This removes the exchange registry entry and the hidden S3 object.",
      confirmLabel: "Delete file",
      danger: true,
    });
    if (!confirmed) {
      return;
    }
    await fetchJsonOrThrow(`${apiRoot}/files/${encodeURIComponent(fileId)}`, {
      method: "DELETE",
      headers: jsonHeaders(),
      body: JSON.stringify({ filePassword: selectedFilePassword() }),
    });
    await loadFiles();
  }

  function openCopyDialog(fileId) {
    const file = exchangeFiles.find((candidate) => candidate.fileId === fileId);
    if (!file) {
      return;
    }
    copyFileId = fileId;
    const passwordInput = query("[data-data-exchange-copy-password]");
    const passwordField = query("[data-data-exchange-copy-password-field]");
    const fileNameInput = query("[data-data-exchange-copy-file-name]");
    if (passwordField) {
      passwordField.hidden = !file.hasPassword;
    }
    if (passwordInput) {
      passwordInput.required = Boolean(file.hasPassword);
      passwordInput.value = selectedFilePassword();
    }
    if (fileNameInput) {
      fileNameInput.value = file.fileName || "";
    }
    query("[data-data-exchange-copy-dialog]")?.showModal();
  }

  async function submitCopyToS3(event) {
    event.preventDefault();
    if (!copyFileId) {
      return;
    }
    try {
      const result = await fetchJsonOrThrow(
        `${apiRoot}/files/${encodeURIComponent(copyFileId)}/copy-to-shared-s3`,
        {
          method: "POST",
          headers: jsonHeaders(),
          body: JSON.stringify({
            filePassword: query("[data-data-exchange-copy-password]")?.value || "",
            bucket: query("[data-data-exchange-copy-bucket]")?.value || "",
            prefix: query("[data-data-exchange-copy-prefix]")?.value || "",
            fileName: query("[data-data-exchange-copy-file-name]")?.value || "",
          }),
        }
      );
      query("[data-data-exchange-copy-dialog]")?.close();
      await showMessageDialog({
        title: "Copied to Shared Workspace S3",
        copy: `${result.importedCount || 0} file(s) copied into queryable storage.`,
      });
    } catch (error) {
      await showMessageDialog({
        title: "Copy to Shared Workspace failed",
        copy: error instanceof Error ? error.message : "The file could not be copied.",
      });
    }
  }

  async function copyToLocalWorkspace(fileId) {
    const handoff = await fetchJsonOrThrow(
      `${apiRoot}/files/${encodeURIComponent(fileId)}/local-workspace-handoff`,
      {
        method: "POST",
        headers: jsonHeaders(),
        body: JSON.stringify({ filePassword: selectedFilePassword() }),
      }
    );
    const response = await window.fetch(handoff.downloadUrl);
    if (!response.ok) {
      throw new Error(`Download failed: ${response.status}`);
    }
    const blob = await response.blob();
    const workspace = handoff.localWorkspace || {};
    const now = new Date().toISOString();
    const saved = await saveLocalWorkspaceExport({
      id: createLocalWorkspaceEntryId(),
      fileName: workspace.fileName || handoff.file?.fileName || "data-exchange-file",
      folderPath: workspace.folderPath || "DataExchange",
      exportFormat: workspace.exportFormat || fileExtension(handoff.file) || "json",
      mimeType: workspace.mimeType || blob.type || "application/octet-stream",
      sizeBytes: Number(workspace.sizeBytes || blob.size || 0),
      createdAt: now,
      updatedAt: now,
      blob,
    });
    await syncLocalWorkspaceEntry(saved.id, { force: true });
    await renderLocalWorkspaceSidebarEntries();
    await refreshSidebar("notebook");
    await showMessageDialog({
      title: "Copied to Local Workspace",
      copy: "The file is now available in Local Workspace and can be opened from the Query Workbench.",
    });
    await openQueryWorkbenchDataSources();
  }

  async function handleClick(event) {
    const showUpload = event.target.closest("[data-data-exchange-show-upload]");
    if (showUpload) {
      event.preventDefault();
      setUploadVisible(true);
      return true;
    }

    const hideUpload = event.target.closest("[data-data-exchange-hide-upload]");
    if (hideUpload) {
      event.preventDefault();
      setUploadVisible(false);
      return true;
    }

    const uploadButton = event.target.closest("[data-data-exchange-upload-submit]");
    if (uploadButton) {
      await submitUpload(event);
      return true;
    }

    const createFolder = event.target.closest("[data-data-exchange-create-folder]");
    if (createFolder) {
      event.preventDefault();
      event.stopPropagation();
      openCreateFolderDialog(createFolder.dataset.dataExchangeFolderParent || "");
      return true;
    }

    const folderCancel = event.target.closest("[data-data-exchange-folder-cancel]");
    if (folderCancel) {
      event.preventDefault();
      query("[data-data-exchange-folder-dialog]")?.close();
      return true;
    }

    const fileRow = event.target.closest("[data-data-exchange-file-row]");
    if (fileRow) {
      event.preventDefault();
      selectedFileId = fileRow.dataset.dataExchangeFileRow || "";
      renderFileList();
      return true;
    }

    const file = selectedFile();
    if (!file) {
      return false;
    }

    const downloadButton = event.target.closest("[data-data-exchange-download]");
    if (downloadButton) {
      event.preventDefault();
      await downloadFile(file.fileId || "");
      return true;
    }

    const editButton = event.target.closest("[data-data-exchange-edit]");
    if (editButton) {
      event.preventDefault();
      openEditDialog(file.fileId || "");
      return true;
    }

    const deleteButton = event.target.closest("[data-data-exchange-delete]");
    if (deleteButton) {
      event.preventDefault();
      await deleteFile(file.fileId || "");
      return true;
    }

    const copyS3Button = event.target.closest("[data-data-exchange-copy-s3]");
    if (copyS3Button) {
      event.preventDefault();
      openCopyDialog(file.fileId || "");
      return true;
    }

    const copyLocalButton = event.target.closest("[data-data-exchange-copy-local]");
    if (copyLocalButton) {
      event.preventDefault();
      await copyToLocalWorkspace(file.fileId || "");
      return true;
    }

    const editCancel = event.target.closest("[data-data-exchange-edit-cancel]");
    if (editCancel) {
      event.preventDefault();
      query("[data-data-exchange-edit-dialog]")?.close();
      return true;
    }

    const copyCancel = event.target.closest("[data-data-exchange-copy-cancel]");
    if (copyCancel) {
      event.preventDefault();
      query("[data-data-exchange-copy-dialog]")?.close();
      return true;
    }

    return false;
  }

  function initializeCurrentPage() {
    const root = pageRoot();
    if (!root || root.dataset.dataExchangeInitialized === "true") {
      return;
    }
    root.dataset.dataExchangeInitialized = "true";
    query("[data-data-exchange-upload-form]", root)?.addEventListener("submit", submitUpload);
    query("[data-data-exchange-file-input]", root)?.addEventListener("change", onFileInputChange);
    query("[data-data-exchange-search]", root)?.addEventListener("input", renderFileList);
    query("[data-data-exchange-edit-form]", root)?.addEventListener("submit", submitEdit);
    query("[data-data-exchange-copy-form]", root)?.addEventListener("submit", submitCopyToS3);
    query("[data-data-exchange-folder-form]", root)?.addEventListener("submit", submitCreateFolder);
    root.addEventListener("click", (event) => {
      handleClick(event).catch((error) => {
        showMessageDialog({
          title: "DataExchange action failed",
          copy: error instanceof Error ? error.message : "The action could not be completed.",
        });
      });
    });
    root.addEventListener("dragover", onDragOver);
    root.addEventListener("dragleave", onDragLeave);
    root.addEventListener("drop", onDrop);

    loadFiles().catch((error) => {
      console.error("Failed to refresh DataExchange files.", error);
      const list = query("[data-data-exchange-file-list]", root);
      if (list) {
        list.innerHTML = `<div class="data-source-explorer-empty"><p>${
          error instanceof Error ? escapeHtml(error.message) : "DataExchange files could not be loaded."
        }</p></div>`;
      }
    });
  }

  return {
    initializeCurrentPage,
  };
}

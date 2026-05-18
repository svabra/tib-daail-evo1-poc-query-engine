export function createDownloadJobsController(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    formatByteCount,
    formatRelativeTimestamp,
    showMessageDialog,
    onStateChanged = () => {},
  } = helpers;

  let stateVersion = null;
  let jobs = [];
  let summary = { runningCount: 0, readyCount: 0, totalCount: 0 };
  let activeDialogJobId = "";

  function normalizeJob(job) {
    if (!job || typeof job !== "object") {
      return null;
    }
    return {
      ...job,
      jobId: String(job.jobId || ""),
      sourceKind: String(job.sourceKind || ""),
      status: String(job.status || "").trim().toLowerCase() || "queued",
      progress: Number.isFinite(Number(job.progress)) ? Number(job.progress) : 0,
      bytesProcessed: Number(job.bytesProcessed || 0) || 0,
      sourceSizeBytes: Number(job.sourceSizeBytes || 0) || 0,
      artifactSizeBytes: Number(job.artifactSizeBytes || 0) || 0,
      sourceName: String(job.sourceName || "Prepared download"),
      sourceBucket: String(job.sourceBucket || ""),
      sourceKey: String(job.sourceKey || ""),
      dataExchangeFileId: String(job.dataExchangeFileId || ""),
      downloadUrl: String(job.downloadUrl || ""),
      artifactFilename: String(job.artifactFilename || ""),
    };
  }

  function compareJobs(left, right) {
    const leftUpdated = Date.parse(left?.updatedAt || left?.createdAt || "");
    const rightUpdated = Date.parse(right?.updatedAt || right?.createdAt || "");
    if (!Number.isNaN(leftUpdated) || !Number.isNaN(rightUpdated)) {
      const normalizedLeft = Number.isNaN(leftUpdated) ? 0 : leftUpdated;
      const normalizedRight = Number.isNaN(rightUpdated) ? 0 : rightUpdated;
      if (normalizedLeft !== normalizedRight) {
        return normalizedRight - normalizedLeft;
      }
    }
    return String(right?.jobId || "").localeCompare(String(left?.jobId || ""));
  }

  function isRunning(job) {
    return ["queued", "running"].includes(String(job?.status || "").toLowerCase());
  }

  function isTerminal(job) {
    return ["ready", "failed", "cancelled", "expired"].includes(String(job?.status || "").toLowerCase());
  }

  function isCsvDescriptor(descriptor) {
    const format = String(descriptor?.fileFormat || "").trim().toLowerCase();
    if (format) {
      return format === "csv";
    }
    const name = String(descriptor?.fileName || descriptor?.key || "").trim().toLowerCase();
    return name.endsWith(".csv");
  }

  function formatDownloadSize(sizeBytes) {
    const normalizedSize = Number(sizeBytes) || 0;
    if (normalizedSize >= 1024 * 1024 * 1024) {
      return `${(normalizedSize / (1024 * 1024 * 1024)).toFixed(1)} GB`;
    }
    return formatByteCount(normalizedSize);
  }

  function applyState(payload = {}) {
    stateVersion = payload?.version ?? stateVersion;
    jobs = Array.isArray(payload?.jobs)
      ? payload.jobs.map((job) => normalizeJob(job)).filter(Boolean).sort(compareJobs)
      : [];
    summary = payload?.summary || {
      runningCount: jobs.filter(isRunning).length,
      readyCount: jobs.filter((job) => job.status === "ready").length,
      totalCount: jobs.length,
    };
    renderDialog();
    syncPreparedDownloadIndicators();
    onStateChanged();
  }

  async function loadState() {
    const payload = await fetchJsonOrThrow("/api/download-jobs", {
      headers: { Accept: "application/json" },
    });
    applyState(payload);
    return payload;
  }

  function upsertJob(job) {
    const normalized = normalizeJob(job);
    if (!normalized) {
      return null;
    }
    jobs = [normalized, ...jobs.filter((candidate) => candidate.jobId !== normalized.jobId)].sort(compareJobs);
    summary = {
      runningCount: jobs.filter(isRunning).length,
      readyCount: jobs.filter((candidate) => candidate.status === "ready").length,
      totalCount: jobs.length,
    };
    renderDialog();
    syncPreparedDownloadIndicators();
    onStateChanged();
    return normalized;
  }

  function currentState() {
    return {
      version: stateVersion,
      snapshot: jobs,
      summary,
    };
  }

  function getStateVersion() {
    return stateVersion;
  }

  function jobById(jobId) {
    return jobs.find((job) => job.jobId === String(jobId || "")) || null;
  }

  function jobForS3Object(bucket, key) {
    const normalizedBucket = String(bucket || "").trim();
    const normalizedKey = String(key || "").trim();
    if (!normalizedBucket || !normalizedKey) {
      return null;
    }
    return jobs.find((job) =>
      job.sourceKind === "s3_object" &&
      job.sourceBucket === normalizedBucket &&
      job.sourceKey === normalizedKey &&
      ["queued", "running", "ready"].includes(job.status)
    ) || null;
  }

  function jobForDataExchangeFile(fileId) {
    const normalizedFileId = String(fileId || "").trim();
    if (!normalizedFileId) {
      return null;
    }
    return jobs.find((job) =>
      job.sourceKind === "data_exchange_file" &&
      job.dataExchangeFileId === normalizedFileId &&
      ["queued", "running", "ready"].includes(job.status)
    ) || null;
  }

  function readyJob(job) {
    return job?.status === "ready" && Boolean(job.downloadUrl);
  }

  function statusLabel(job) {
    switch (String(job?.status || "").toLowerCase()) {
      case "queued":
        return "Queued";
      case "running":
        return "Preparing";
      case "ready":
        return "Ready";
      case "failed":
        return "Failed";
      case "cancelled":
        return "Cancelled";
      case "expired":
        return "Expired";
      default:
        return "Preparing";
    }
  }

  function progressPercent(job) {
    return Math.max(0, Math.min(100, Math.round((Number(job?.progress || 0) || 0) * 100)));
  }

  function progressCopy(job) {
    const processed = formatDownloadSize(job?.bytesProcessed || 0);
    const total = formatDownloadSize(job?.sourceSizeBytes || 0);
    if (job?.status === "ready") {
      return `${formatDownloadSize(job.artifactSizeBytes || 0)} ZIP ready`;
    }
    if (job?.status === "failed" || job?.status === "cancelled" || job?.status === "expired") {
      return job.message || statusLabel(job);
    }
    return `${progressPercent(job)}% - ${processed} of ${total}`;
  }

  function timestampCopy(value) {
    const timestamp = Date.parse(value || "");
    if (!timestamp) {
      return "";
    }
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "short",
    }).format(new Date(timestamp));
  }

  function expiryCopy(job) {
    if (!job?.expiresAt) {
      return "";
    }
    return `Expires ${timestampCopy(job.expiresAt) || formatRelativeTimestamp(job.expiresAt)}`;
  }

  function ensureDialog() {
    let dialog = document.querySelector("[data-download-job-dialog]");
    if (dialog instanceof HTMLDialogElement) {
      return dialog;
    }
    dialog = document.createElement("dialog");
    dialog.className = "modal-dialog modal-dialog-wide download-job-dialog";
    dialog.dataset.downloadJobDialog = "true";
    dialog.innerHTML = `
      <form method="dialog" class="modal-card modal-card-wide download-job-dialog-card" data-download-job-form>
        <div class="modal-header">
          <div>
            <p class="home-eyebrow">Prepared Download</p>
            <h2 class="modal-title" data-download-job-dialog-title>Preparing ZIP download</h2>
          </div>
        </div>
        <div class="download-job-dialog-body" data-download-job-dialog-body></div>
        <div class="modal-actions" data-download-job-dialog-actions></div>
      </form>
    `;
    document.body.appendChild(dialog);
    return dialog;
  }

  function renderDialog() {
    if (!activeDialogJobId) {
      return;
    }
    const dialog = ensureDialog();
    const job = jobById(activeDialogJobId);
    const body = dialog.querySelector("[data-download-job-dialog-body]");
    const title = dialog.querySelector("[data-download-job-dialog-title]");
    const actions = dialog.querySelector("[data-download-job-dialog-actions]");
    if (!body || !title || !actions) {
      return;
    }
    if (!job) {
      title.textContent = "Prepared ZIP download";
      body.innerHTML = '<p class="download-job-dialog-note">The prepared download job is no longer available.</p>';
      actions.innerHTML = '<button class="modal-button" type="submit" value="close">Close</button>';
      return;
    }

    title.textContent =
      job.status === "ready"
        ? "ZIP download ready"
        : job.status === "failed"
          ? "Prepared ZIP failed"
          : "Preparing ZIP download";
    const percent = progressPercent(job);
    body.innerHTML = `
      <div class="download-job-dialog-status">
        <strong>${escapeHtml(job.sourceName)}</strong>
        <span class="download-job-status-badge download-job-status-${escapeHtml(job.status)}">${escapeHtml(statusLabel(job))}</span>
      </div>
      <p class="download-job-dialog-note">
        You may continue to navigate away. You will be informed in the Message Centre once the ZIP file is ready to download. When ready, you can download it from this modal-dialogue, the data sources sidebar or the data exchange workbench.
      </p>
      <div class="download-job-progress">
        <div class="download-job-progress-copy">
          <span>${escapeHtml(progressCopy(job))}</span>
          ${job.expiresAt ? `<span>${escapeHtml(expiryCopy(job))}</span>` : ""}
        </div>
        <div class="download-job-progress-track${isRunning(job) && percent <= 0 ? " is-indeterminate" : ""}">
          <span style="width:${escapeHtml(String(percent))}%;"></span>
        </div>
      </div>
      ${job.message ? `<p class="download-job-dialog-message">${escapeHtml(job.message)}</p>` : ""}
    `;

    actions.innerHTML = `
      <button class="modal-button modal-button-secondary" type="submit" value="close" data-download-job-close>
        Close
      </button>
      ${
        isRunning(job)
          ? `<button class="modal-button modal-button-secondary" type="button" data-download-job-cancel="${escapeHtml(job.jobId)}">Cancel</button>`
          : ""
      }
      ${
        job.status === "ready" && job.downloadUrl
          ? `<button class="modal-button" type="button" data-download-job-download="${escapeHtml(job.jobId)}">Download ZIP</button>`
          : ""
      }
    `;
  }

  function openDialog(jobId) {
    activeDialogJobId = String(jobId || "");
    const dialog = ensureDialog();
    renderDialog();
    if (typeof dialog.showModal === "function" && !dialog.open) {
      dialog.showModal();
    }
  }

  function downloadJob(job) {
    if (!job?.downloadUrl) {
      return false;
    }
    const anchor = document.createElement("a");
    anchor.href = job.downloadUrl;
    anchor.download = job.artifactFilename || "";
    anchor.style.display = "none";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    return true;
  }

  function downloadJobById(jobId) {
    return downloadJob(jobById(jobId));
  }

  async function startS3PreparedDownload(descriptor) {
    if (!descriptor?.bucket || !descriptor?.key) {
      return false;
    }
    if (!isCsvDescriptor(descriptor)) {
      await showMessageDialog({
        title: "Prepared ZIP unavailable",
        copy: "Prepared ZIP downloads currently support CSV files.",
      });
      return true;
    }
    const job = await fetchJsonOrThrow("/api/s3/download-jobs", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        bucket: descriptor.bucket,
        key: descriptor.key,
        filename: descriptor.fileName || "",
        format: "csv",
      }),
    });
    const normalized = upsertJob(job);
    if (normalized) {
      openDialog(normalized.jobId);
    }
    return true;
  }

  async function startDataExchangePreparedDownload(fileId, filePassword = "") {
    const normalizedFileId = String(fileId || "").trim();
    if (!normalizedFileId) {
      return false;
    }
    const job = await fetchJsonOrThrow(
      `/api/data-exchange/files/${encodeURIComponent(normalizedFileId)}/download-jobs`,
      {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ filePassword }),
      }
    );
    const normalized = upsertJob(job);
    if (normalized) {
      openDialog(normalized.jobId);
    }
    return true;
  }

  function indicatorMarkupForJob(job) {
    if (!job) {
      return "";
    }
    const ready = job.status === "ready";
    const running = isRunning(job);
    if (!ready && !running) {
      return "";
    }
    const title = ready
      ? "Prepared ZIP download is ready. Click to download"
      : "Preparing ZIP download";
    const actionAttributes =
      ready && job.downloadUrl
        ? `role="button" tabindex="0" data-download-job-download="${escapeHtml(job.jobId)}"`
        : running
          ? `role="button" tabindex="0" data-download-job-open="${escapeHtml(job.jobId)}"`
          : "";
    return `
      <span
        data-prepared-download-indicator
        class="prepared-download-indicator ${ready ? "is-ready" : "is-running"}"
        title="${escapeHtml(title)}"
        aria-label="${escapeHtml(title)}"
        ${actionAttributes}
      >
        <span class="prepared-download-indicator-glyph" aria-hidden="true">${ready ? "ZIP" : "..."}</span>
      </span>
    `;
  }

  function s3IndicatorMarkup(bucket, key) {
    return indicatorMarkupForJob(jobForS3Object(bucket, key));
  }

  function dataExchangeIndicatorMarkup(fileId) {
    return indicatorMarkupForJob(jobForDataExchangeFile(fileId));
  }

  function preparedDownloadMenuButtonMarkup(job) {
    if (!readyJob(job)) {
      return "";
    }
    return `
      <button
        type="button"
        class="workspace-action-menu-item"
        data-download-prepared-zip-file
        data-download-job-download="${escapeHtml(job.jobId)}"
        title="Download the prepared ZIP file"
      >
        Download prepared ZIP file
      </button>
    `;
  }

  function syncPreparedDownloadMenus(root = document) {
    if (!root || typeof root.querySelectorAll !== "function") {
      return;
    }
    root.querySelectorAll("[data-source-object][data-s3-bucket][data-s3-key]").forEach((node) => {
      const panel = node.querySelector(".workspace-action-menu-panel");
      if (!panel) {
        return;
      }
      panel.querySelectorAll("[data-download-prepared-zip-file]").forEach((button) => button.remove());
      const job = jobForS3Object(node.dataset.s3Bucket, node.dataset.s3Key);
      if (!readyJob(job)) {
        return;
      }
      const markup = preparedDownloadMenuButtonMarkup(job);
      const before =
        panel.querySelector("[data-download-source-ddl]") ||
        panel.querySelector("[data-view-source-data]") ||
        panel.querySelector("[data-delete-source-s3-object]");
      if (before) {
        before.insertAdjacentHTML("beforebegin", markup);
      } else {
        panel.insertAdjacentHTML("beforeend", markup);
      }
    });
  }

  function syncPreparedDownloadIndicators(root = document) {
    if (!root || typeof root.querySelectorAll !== "function") {
      return;
    }
    root.querySelectorAll("[data-source-object][data-s3-bucket][data-s3-key]").forEach((node) => {
      const titleRow = node.querySelector(".source-node-label");
      if (!titleRow) {
        return;
      }
      titleRow.querySelectorAll("[data-prepared-download-indicator]").forEach((node) => node.remove());
      const markup = s3IndicatorMarkup(node.dataset.s3Bucket, node.dataset.s3Key);
      if (markup) {
        titleRow.insertAdjacentHTML("beforeend", markup);
      }
    });

    root.querySelectorAll("[data-data-source-explorer-s3-file][data-bucket][data-prefix]").forEach((node) => {
      const titleRow = node.querySelector(".data-source-explorer-object-title-row");
      if (!titleRow) {
        return;
      }
      titleRow.querySelectorAll("[data-prepared-download-indicator]").forEach((node) => node.remove());
      const markup = s3IndicatorMarkup(node.dataset.bucket, node.dataset.prefix);
      if (markup) {
        titleRow.insertAdjacentHTML("beforeend", markup);
      }
    });

    root.querySelectorAll("[data-data-exchange-file-row]").forEach((node) => {
      const label = node.querySelector(".source-node-label");
      if (!label) {
        return;
      }
      label.querySelectorAll("[data-prepared-download-indicator]").forEach((node) => node.remove());
      const markup = dataExchangeIndicatorMarkup(node.dataset.dataExchangeFileRow);
      if (markup) {
        label.insertAdjacentHTML("beforeend", markup);
      }
    });

    syncPreparedDownloadMenus(root);
  }

  function downloadJobNotificationMarkup(job) {
    const ready = job.status === "ready";
    const running = isRunning(job);
    const statusCopy = running ? "Preparing download" : `${statusLabel(job)} download`;
    const details = ready && job.expiresAt
      ? `${formatDownloadSize(job.artifactSizeBytes || 0)} ZIP - ${expiryCopy(job)}`
      : progressCopy(job);
    return `
      <div class="topbar-notification-item topbar-notification-item-download">
        <span class="topbar-notification-item-status topbar-notification-item-status-notice${running ? " is-live" : ""}">
          ${escapeHtml(statusCopy)}
        </span>
        <span class="topbar-notification-item-title">${escapeHtml(job.sourceName)}</span>
        <span class="topbar-notification-item-copy">${escapeHtml(details)}</span>
        ${
          ready && job.downloadUrl
            ? `<a class="topbar-notification-item-action" href="${escapeHtml(job.downloadUrl)}" download="${escapeHtml(job.artifactFilename || "")}">Download ZIP</a>`
            : ""
        }
      </div>
    `;
  }

  function notificationItems({ dismissedKeys, notificationItemKey }) {
    return jobs
      .filter((job) => isRunning(job) || isTerminal(job))
      .map((job) => ({
        type: "download",
        job,
        updatedAt: job.updatedAt,
        dismissalKey: notificationItemKey("download", job),
        dismissible: isTerminal(job),
        markup: downloadJobNotificationMarkup(job),
      }))
      .filter((item) => !dismissedKeys.has(item.dismissalKey));
  }

  async function handleClick(event) {
    const openButton = event.target.closest("[data-download-job-open]");
    if (openButton) {
      event.preventDefault();
      const jobId = String(openButton.dataset.downloadJobOpen || "").trim();
      if (jobId) {
        openDialog(jobId);
      }
      return true;
    }

    const cancelButton = event.target.closest("[data-download-job-cancel]");
    if (cancelButton) {
      event.preventDefault();
      const jobId = String(cancelButton.dataset.downloadJobCancel || "").trim();
      if (!jobId) {
        return true;
      }
      const job = await fetchJsonOrThrow(`/api/download-jobs/${encodeURIComponent(jobId)}`, {
        method: "DELETE",
        headers: { Accept: "application/json" },
      });
      upsertJob(job);
      return true;
    }

    const downloadButton = event.target.closest("[data-download-job-download]");
    if (downloadButton) {
      event.preventDefault();
      const job = jobById(downloadButton.dataset.downloadJobDownload || "");
      if (!downloadJob(job)) {
        await showMessageDialog({
          title: "Download unavailable",
          copy: "The prepared ZIP download is not ready yet.",
        });
      }
      return true;
    }

    return false;
  }

  return {
    applyState,
    currentState,
    dataExchangeIndicatorMarkup,
    downloadJobById,
    getStateVersion,
    handleClick,
    isCsvDescriptor,
    isRunning,
    jobForDataExchangeFile,
    jobForS3Object,
    loadState,
    notificationItems,
    openDialog,
    s3IndicatorMarkup,
    startDataExchangePreparedDownload,
    startS3PreparedDownload,
    syncPreparedDownloadIndicators,
  };
}

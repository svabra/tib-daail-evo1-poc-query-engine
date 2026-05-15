export function createS3DeleteJobsController(helpers) {
  const {
    blinkSourceCatalog = () => {},
    currentWorkspaceMode = () => "notebook",
    escapeHtml,
    fetchJsonOrThrow,
    getDeleteDialogOptions,
    getPreferredLocationAfterDelete,
    loadS3ExplorerRoot,
    onStateChanged = () => {},
    refreshActiveDataSourceViews = async () => {},
    refreshSidebar,
    setPendingDeleteState,
    setSidebarSourceOperationStatus = () => {},
    showConfirmDialog,
    showMessageDialog,
  } = helpers;

  const runningStatuses = new Set(["queued", "running", "finalizing"]);
  const terminalStatuses = new Set(["completed", "failed"]);
  let stateVersion = null;
  let jobs = [];
  let summary = { runningCount: 0, totalCount: 0 };
  const activeJobIds = new Set();
  const handledTerminalJobIds = new Set();
  const jobContexts = new Map();
  const pollHandles = new Map();

  function normalizeJob(job) {
    if (!job || typeof job !== "object") {
      return null;
    }
    return {
      ...job,
      jobId: String(job.jobId || ""),
      entryKind: String(job.entryKind || "").trim().toLowerCase(),
      bucket: String(job.bucket || "").trim(),
      prefix: String(job.prefix || "").trim(),
      path: String(job.path || "").trim(),
      status: String(job.status || "").trim().toLowerCase() || "queued",
      phase: String(job.phase || "").trim(),
      progress: Number.isFinite(Number(job.progress)) ? Number(job.progress) : 0,
      message: String(job.message || "").trim(),
      error: String(job.error || "").trim(),
      lastError: String(job.lastError || "").trim(),
      deletedKeys: Number(job.deletedKeys || 0) || 0,
      bucketDeleteAttempts: Number(job.bucketDeleteAttempts || 0) || 0,
      updatedAt: String(job.updatedAt || job.createdAt || ""),
    };
  }

  function compareJobs(left, right) {
    const leftUpdated = Date.parse(left?.updatedAt || left?.createdAt || "");
    const rightUpdated = Date.parse(right?.updatedAt || right?.createdAt || "");
    if (!Number.isNaN(leftUpdated) || !Number.isNaN(rightUpdated)) {
      return (Number.isNaN(rightUpdated) ? 0 : rightUpdated) - (Number.isNaN(leftUpdated) ? 0 : leftUpdated);
    }
    return String(right?.jobId || "").localeCompare(String(left?.jobId || ""));
  }

  function descriptorFromJob(job) {
    if (!job) {
      return null;
    }
    return {
      entryKind: job.entryKind,
      name: job.name || job.prefix || job.bucket,
      bucket: job.bucket,
      prefix: job.prefix,
      path: job.path,
      fileFormat: job.fileFormat || "",
    };
  }

  function isRunning(job) {
    return runningStatuses.has(String(job?.status || "").toLowerCase());
  }

  function isTerminal(job) {
    return terminalStatuses.has(String(job?.status || "").toLowerCase());
  }

  function statusLabel(job) {
    switch (String(job?.status || "").toLowerCase()) {
      case "queued":
        return "Queued";
      case "running":
        return "Deleting";
      case "finalizing":
        return "Finalizing";
      case "completed":
        return "Completed";
      case "failed":
        return "Failed";
      default:
        return "Deleting";
    }
  }

  function operationTitle(job, terminalTone = "") {
    const kind = String(job?.entryKind || "").toLowerCase();
    if (terminalTone === "success") {
      return kind === "bucket" ? "Bucket deleted" : kind === "folder" ? "Folder deleted" : "Object deleted";
    }
    if (terminalTone === "danger") {
      return kind === "bucket" ? "Bucket delete failed" : kind === "folder" ? "Folder delete failed" : "Object delete failed";
    }
    return kind === "bucket" ? "Deleting bucket" : kind === "folder" ? "Deleting folder" : "Deleting object";
  }

  function runningCopy(job) {
    if (job?.status === "finalizing") {
      return `${job.path || "S3 bucket"} is waiting for object-store finalization.`;
    }
    if (job?.message) {
      return job.message;
    }
    return `Deleting ${job?.path || "the selected S3 entry"}...`;
  }

  function applyState(payload = {}) {
    stateVersion = payload?.version ?? stateVersion;
    jobs = Array.isArray(payload?.jobs)
      ? payload.jobs.map((job) => normalizeJob(job)).filter(Boolean).sort(compareJobs)
      : [];
    summary = payload?.summary || {
      runningCount: jobs.filter(isRunning).length,
      totalCount: jobs.length,
    };
    syncPendingDeleteState();
    syncActiveRunningStatuses();
    handleTerminalJobs();
    onStateChanged();
  }

  function upsertJob(job, { active = false, context = null } = {}) {
    const normalized = normalizeJob(job);
    if (!normalized?.jobId) {
      return null;
    }
    jobs = [normalized, ...jobs.filter((candidate) => candidate.jobId !== normalized.jobId)].sort(compareJobs);
    summary = {
      runningCount: jobs.filter(isRunning).length,
      totalCount: jobs.length,
    };
    if (active) {
      activeJobIds.add(normalized.jobId);
    }
    if (context) {
      jobContexts.set(normalized.jobId, context);
    }
    syncPendingDeleteState();
    syncActiveRunningStatuses();
    handleTerminalJobs();
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

  async function loadState() {
    const payload = await fetchJsonOrThrow("/api/s3/delete-jobs", {
      headers: { Accept: "application/json" },
    });
    applyState(payload);
    return payload;
  }

  function syncPendingDeleteState() {
    jobs.filter(isRunning).forEach((job) => {
      setPendingDeleteState(descriptorFromJob(job), true);
    });
  }

  function syncActiveRunningStatuses() {
    jobs
      .filter((job) => isRunning(job) && activeJobIds.has(job.jobId))
      .forEach((job) => {
        const context = jobContexts.get(job.jobId) || {};
        if (!context.showSidebarStatus) {
          return;
        }
        setSidebarSourceOperationStatus({
          tone: "info",
          title: operationTitle(job),
          copy: runningCopy(job),
        });
      });
  }

  function startPolling(jobId) {
    const normalizedJobId = String(jobId || "").trim();
    if (!normalizedJobId || pollHandles.has(normalizedJobId)) {
      return;
    }
    const poll = async () => {
      try {
        const job = await fetchJsonOrThrow(`/api/s3/delete-jobs/${encodeURIComponent(normalizedJobId)}`, {
          headers: { Accept: "application/json" },
        });
        const normalized = upsertJob(job);
        if (!normalized || isTerminal(normalized)) {
          stopPolling(normalizedJobId);
          return;
        }
      } catch (_error) {
        // SSE is the primary transport. Polling is only a quiet fallback.
      }
      pollHandles.set(normalizedJobId, window.setTimeout(poll, 1500));
    };
    pollHandles.set(normalizedJobId, window.setTimeout(poll, 1500));
  }

  function stopPolling(jobId) {
    const handle = pollHandles.get(jobId);
    if (handle) {
      window.clearTimeout(handle);
      pollHandles.delete(jobId);
    }
  }

  function handleTerminalJobs() {
    jobs
      .filter((job) => isTerminal(job) && activeJobIds.has(job.jobId) && !handledTerminalJobIds.has(job.jobId))
      .forEach((job) => {
        handledTerminalJobIds.add(job.jobId);
        stopPolling(job.jobId);
        handleTerminalJob(job).catch((error) => {
          console.error("Failed to handle completed S3 delete job.", error);
        });
      });
  }

  async function handleTerminalJob(job) {
    const descriptor = descriptorFromJob(job);
    const context = jobContexts.get(job.jobId) || {};
    setPendingDeleteState(descriptor, false);

    if (job.status === "completed") {
      if (context.refreshExplorerAfter) {
        await loadS3ExplorerRoot(context.preferredLocation || getPreferredLocationAfterDelete(descriptor));
      }
      if (context.refreshSidebarAfter) {
        await refreshSidebar(currentWorkspaceMode());
        if (job.entryKind === "bucket") {
          blinkSourceCatalog("workspace.s3");
        }
      }
      await refreshActiveDataSourceViews();
      if (context.showSidebarStatus) {
        setSidebarSourceOperationStatus(
          {
            tone: "success",
            title: operationTitle(job, "success"),
            copy: job.message || "The selected S3 entry was deleted.",
          },
          { autoClearMs: 6000 }
        );
      }
      return;
    }

    if (context.showSidebarStatus) {
      setSidebarSourceOperationStatus(
        {
          tone: "danger",
          title: operationTitle(job, "danger"),
          copy: job.error || job.message || "The selected S3 entry could not be deleted.",
        },
        { autoClearMs: 10000 }
      );
    }
    await showMessageDialog({
      title: operationTitle(job, "danger"),
      copy: job.error || job.message || "The selected S3 entry could not be deleted.",
    });
  }

  async function startDelete(descriptor, options = {}) {
    const dialogOptions = getDeleteDialogOptions(descriptor);
    if (!descriptor || !dialogOptions) {
      return false;
    }

    const confirmation = await showConfirmDialog(dialogOptions);
    if (!confirmation.confirmed) {
      return null;
    }

    const context = {
      refreshSidebarAfter: Boolean(options.refreshSidebarAfter),
      refreshExplorerAfter: Boolean(options.refreshExplorerAfter),
      showSidebarStatus: Boolean(options.showSidebarStatus),
      preferredLocation: getPreferredLocationAfterDelete(descriptor),
    };

    if (context.showSidebarStatus) {
      setSidebarSourceOperationStatus({
        tone: "info",
        title: operationTitle(descriptor),
        copy: `Starting delete job for ${descriptor.path || "the selected S3 entry"}...`,
      });
    }

    setPendingDeleteState(descriptor, true);

    try {
      const job = await fetchJsonOrThrow("/api/s3/explorer/entries", {
        method: "DELETE",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          entryKind: descriptor.entryKind,
          bucket: descriptor.bucket,
          prefix: descriptor.prefix,
        }),
      });
      const normalized = upsertJob(job, { active: true, context });
      if (context.showSidebarStatus && normalized) {
        setSidebarSourceOperationStatus({
          tone: "info",
          title: operationTitle(normalized),
          copy: runningCopy(normalized),
        });
      }
      if (normalized && !isTerminal(normalized)) {
        startPolling(normalized.jobId);
      }
      return normalized;
    } catch (error) {
      setPendingDeleteState(descriptor, false);
      if (context.showSidebarStatus) {
        setSidebarSourceOperationStatus(
          {
            tone: "danger",
            title: operationTitle(descriptor, "danger"),
            copy:
              error instanceof Error
                ? error.message
                : "The selected S3 entry could not be deleted.",
          },
          { autoClearMs: 8000 }
        );
      }
      throw error;
    }
  }

  function notificationItems({ dismissedKeys, notificationItemKey }) {
    return jobs
      .filter((job) => isRunning(job) || isTerminal(job))
      .map((job) => ({
        type: "s3-delete",
        job,
        updatedAt: job.updatedAt,
        dismissalKey: notificationItemKey("s3-delete", job),
        dismissible: isTerminal(job),
        markup: `
          <div class="topbar-notification-item topbar-notification-item-s3-delete">
            <span class="topbar-notification-item-status topbar-notification-item-status-notice${isRunning(job) ? " is-live" : ""}">
              ${escapeHtml(statusLabel(job))} S3 delete
            </span>
            <span class="topbar-notification-item-title">${escapeHtml(job.path || job.bucket || "S3 delete")}</span>
            <span class="topbar-notification-item-copy">${escapeHtml(job.error || job.message || statusLabel(job))}</span>
          </div>
        `,
      }))
      .filter((item) => !dismissedKeys.has(item.dismissalKey));
  }

  return {
    applyState,
    currentState,
    getStateVersion,
    loadState,
    notificationItems,
    startDelete,
  };
}

import { createVisibilityAwareClock } from "./visibility-clock.js";

export function createRealtimeController(helpers) {
  const {
    collectVisibleNotifications,
    compareDataGenerationJobsByStartedAt,
    compareQueryJobsByStartedAt,
    currentWorkspaceMode,
    dataGenerationJobCopy,
    dataGenerationJobElapsedMs,
    dataGenerationJobIsRunning,
    decorateQueryJobsWithInsights,
    formatQueryDuration,
    getDataGenerationState,
    getDownloadState = () => ({ version: null, snapshot: [], summary: {} }),
    getPipelineNotificationItems = () => [],
    getPipelineNotificationSummary = () => ({ version: null, runningCount: 0, totalCount: 0 }),
    getS3DeleteState = () => ({ version: null, snapshot: [], summary: {} }),
    getDismissedNotificationKeys,
    getQueryState,
    normalizeDataGenerationJob,
    normalizeQueryJob,
    notificationClearButton,
    notificationItemKey,
    queryJobElapsedMs,
    queryJobForCell,
    queryJobIsRunning,
    queryMonitorCount,
    queryMonitorItemMarkup,
    queryMonitorList,
    queryNotificationCount,
    queryNotificationList,
    queryNotificationMenu,
    queryPerformanceChart,
    queryPerformanceDistribution,
    queryPerformanceSection,
    queryPerformanceStats,
    queryPerformanceStatsMarkup,
    queryResultPanelMarkup,
    queryRowsShownLabel,
    querySourceValidationController = null,
    renderDataGenerationMonitor,
    renderHomePage,
    renderIngestionWorkbench,
    renderPerformanceChartMarkup,
    renderPerformanceDistributionMarkup,
    refreshSidebar,
    setDataGenerationState,
    setQueryState,
    sidebarQueryCounts,
    syncCellCacheHydrationJobState = null,
    writeDismissedNotificationKeys,
    workspaceNotebookId,
  } = helpers;

  let queryJobsLoaded = false;
  let dataGenerationJobsLoaded = false;
  const refreshedDataGenerationJobIds = new Set();

  function queryState() {
    return getQueryState();
  }

  function runCellButtonLabelForCell(cellRoot) {
    const cellLanguage = String(
      cellRoot?.dataset?.defaultCellLanguage ||
        cellRoot?.querySelector?.("[data-editor-root]")?.dataset?.editorLanguage ||
        cellRoot?.querySelector?.("[data-editor-source]")?.dataset?.editorLanguage ||
        "sql"
    )
      .trim()
      .toLowerCase();
    const workspaceRoot = cellRoot?.closest?.("[data-workspace-notebook]");
    const mode = String(
      workspaceRoot?.dataset?.defaultPipelineMode ||
        workspaceRoot?.querySelector?.("[data-notebook-meta]")?.dataset?.defaultPipelineMode ||
        ""
    )
      .trim()
      .toLowerCase();
    return mode === "pipeline" && cellLanguage !== "python" ? "Run Stage" : "Run Cell";
  }

  function dataGenerationState() {
    return getDataGenerationState();
  }

  function downloadState() {
    return getDownloadState();
  }

  function s3DeleteState() {
    return getS3DeleteState();
  }

  function pruneDismissedNotificationKeys() {
    const queryJobsSnapshot = queryState().snapshot;
    const dataGenerationJobsSnapshot = dataGenerationState().snapshot;
    const downloadJobsState = downloadState();
    const downloadJobsSnapshot = Array.isArray(downloadJobsState.snapshot)
      ? downloadJobsState.snapshot
      : [];
    const s3DeleteJobsState = s3DeleteState();
    const s3DeleteJobsSnapshot = Array.isArray(s3DeleteJobsState.snapshot)
      ? s3DeleteJobsState.snapshot
      : [];
    const pipelineNotificationSummary = getPipelineNotificationSummary();
    const pipelineNotificationItems = getPipelineNotificationItems({
      dismissedKeys: new Set(),
      notificationItemKey,
    });
    const validKeys = new Set([
      ...queryJobsSnapshot.map((job) => notificationItemKey("query", job)),
      ...dataGenerationJobsSnapshot.map((job) => notificationItemKey("ingestion", job)),
      ...downloadJobsSnapshot.map((job) => notificationItemKey("download", job)),
      ...s3DeleteJobsSnapshot.map((job) => notificationItemKey("s3-delete", job)),
      ...pipelineNotificationItems.map((item) => item.dismissalKey),
    ]);
    const dismissedNotificationKeys = getDismissedNotificationKeys();
    let changed = false;

    for (const key of dismissedNotificationKeys) {
      if (key.startsWith("query:") && !queryJobsLoaded) {
        continue;
      }
      if (key.startsWith("ingestion:") && !dataGenerationJobsLoaded) {
        continue;
      }
      if (key.startsWith("download:") && downloadJobsState.version === null) {
        continue;
      }
      if (key.startsWith("s3-delete:") && s3DeleteJobsState.version === null) {
        continue;
      }
      if (key.startsWith("pipeline:") && pipelineNotificationSummary.version === null) {
        continue;
      }
      if (validKeys.has(key)) {
        continue;
      }
      dismissedNotificationKeys.delete(key);
      changed = true;
    }

    if (changed) {
      writeDismissedNotificationKeys();
    }
  }

  function clearVisibleNotifications() {
    const visibleItems = collectVisibleNotifications();
    if (!visibleItems.length) {
      return;
    }

    const dismissedNotificationKeys = getDismissedNotificationKeys();
    visibleItems.forEach((item) => dismissedNotificationKeys.add(item.dismissalKey));
    writeDismissedNotificationKeys();
    renderQueryNotificationMenu();
  }

  function refreshLiveDataGenerationClock() {
    const jobsById = new Map(dataGenerationState().snapshot.map((job) => [job.jobId, job]));

    document.querySelectorAll("[data-ingestion-job-duration]").forEach((node) => {
      const job = jobsById.get(node.dataset.jobId || "");
      if (!job) {
        return;
      }
      node.textContent = formatQueryDuration(dataGenerationJobElapsedMs(job));
    });

    document.querySelectorAll("[data-generation-monitor-duration]").forEach((node) => {
      const job = jobsById.get(node.dataset.jobId || "");
      if (!job) {
        return;
      }
      node.textContent = formatQueryDuration(dataGenerationJobElapsedMs(job));
    });

    document.querySelectorAll("[data-data-generation-notification-copy]").forEach((node) => {
      const job = jobsById.get(node.dataset.jobId || "");
      if (!job) {
        return;
      }
      node.textContent = dataGenerationJobCopy(job);
    });
  }

  function syncDataGenerationClockLoop() {
    const hasRunningJobs = dataGenerationState().snapshot.some((job) => dataGenerationJobIsRunning(job));
    dataGenerationClock.setEnabled(hasRunningJobs);
    dataGenerationClock.refresh();
  }

  function maybeRefreshSidebarForCompletedGenerationJobs() {
    const newCompletedJobs = dataGenerationState().snapshot.filter(
      (job) => job.status === "completed" && !refreshedDataGenerationJobIds.has(job.jobId)
    );
    if (!newCompletedJobs.length) {
      return;
    }

    newCompletedJobs.forEach((job) => refreshedDataGenerationJobIds.add(job.jobId));
    refreshSidebar().catch((error) => {
      console.error("Failed to refresh the sidebar after data generation.", error);
    });
  }

  function refreshLiveQueryClock() {
    const jobsById = new Map(queryState().snapshot.map((job) => [job.jobId, job]));

    document.querySelectorAll("[data-query-duration]").forEach((node) => {
      const job = jobsById.get(node.dataset.jobId || "");
      if (!job) {
        return;
      }
      node.textContent = formatQueryDuration(queryJobElapsedMs(job));
    });

    document.querySelectorAll("[data-query-duration-total]").forEach((node) => {
      const job = jobsById.get(node.dataset.jobId || "");
      if (!job) {
        return;
      }
      node.textContent = formatQueryDuration(queryJobElapsedMs(job));
    });

    document.querySelectorAll("[data-query-timing-current-step]").forEach((node) => {
      const job = jobsById.get(node.dataset.jobId || "");
      if (!job) {
        return;
      }
      const completedMs = Number(node.dataset.queryTimingCompletedMs);
      const safeCompletedMs = Number.isFinite(completedMs) && completedMs >= 0 ? completedMs : 0;
      node.textContent = formatQueryDuration(Math.max(0, queryJobElapsedMs(job) - safeCompletedMs));
    });

    document.querySelectorAll("[data-query-monitor-duration]").forEach((node) => {
      const job = jobsById.get(node.dataset.jobId || "");
      if (!job) {
        return;
      }
      node.textContent = formatQueryDuration(queryJobElapsedMs(job));
    });

    document.querySelectorAll("[data-query-notification-copy]").forEach((node) => {
      const job = jobsById.get(node.dataset.jobId || "");
      if (!job) {
        return;
      }
      const suffix = node.dataset.queryCopySuffix || queryRowsShownLabel(job);
      node.textContent = `${formatQueryDuration(queryJobElapsedMs(job))} | ${suffix}`;
    });
  }

  function syncQueryClockLoop() {
    const hasRunningJobs = queryState().snapshot.some((job) => queryJobIsRunning(job));
    queryJobsClock.setEnabled(hasRunningJobs);
    queryJobsClock.refresh();
  }

  const dataGenerationClock = createVisibilityAwareClock(refreshLiveDataGenerationClock);
  const queryJobsClock = createVisibilityAwareClock(refreshLiveQueryClock);

  function renderQueryMonitor() {
    const listRoot = queryMonitorList();
    const countRoot = queryMonitorCount();
    const toggleCountRoots = sidebarQueryCounts();
    const performanceRoot = queryPerformanceSection();
    const performanceStatsRoot = queryPerformanceStats();
    const performanceChartRoot = queryPerformanceChart();
    const performanceDistributionRoot = queryPerformanceDistribution();
    if (!listRoot || !countRoot) {
      return;
    }

    const { snapshot: queryJobsSnapshot, summary: queryJobsSummary, performance: queryPerformanceState } = queryState();
    const runningCount = Number(queryJobsSummary.runningCount || 0);
    const runningProcessCount = Math.max(0, Math.round(Number(queryJobsSummary.runningProcessCount || 0)));
    countRoot.textContent = String(runningCount);
    countRoot.classList.toggle("is-live", runningCount > 0);
    toggleCountRoots.forEach((toggleCountRoot) => {
      toggleCountRoot.textContent = String(runningCount);
      toggleCountRoot.hidden = runningCount === 0;
      toggleCountRoot.classList.toggle("is-live", runningCount > 0);
    });

    const listMarkup = !queryJobsSnapshot.length
      ? '<p class="query-monitor-empty">No query jobs yet.</p>'
      : `
        <p class="query-monitor-process-summary">Running processes: ${runningProcessCount}</p>
        ${queryJobsSnapshot.slice(0, 8).map((job) => queryMonitorItemMarkup(job)).join("")}
      `;
    const monitorSignature = JSON.stringify([
      queryState().version,
      queryJobsSnapshot.slice(0, 8).map((job) => [job.jobId, job.status, job.updatedAt]),
    ]);
    if (listRoot.dataset.renderSignature !== monitorSignature) {
      listRoot.innerHTML = listMarkup;
      listRoot.dataset.renderSignature = monitorSignature;
    }

    if (performanceRoot && performanceStatsRoot && performanceChartRoot && performanceDistributionRoot) {
      const hasPerformance = Array.isArray(queryPerformanceState?.recent) && queryPerformanceState.recent.length > 0;
      performanceRoot.hidden = !hasPerformance;
      if (hasPerformance) {
        performanceStatsRoot.innerHTML = queryPerformanceStatsMarkup(queryPerformanceState);
        performanceChartRoot.innerHTML = renderPerformanceChartMarkup(queryPerformanceState);
        performanceDistributionRoot.innerHTML = renderPerformanceDistributionMarkup(queryPerformanceState);
      } else {
        performanceStatsRoot.innerHTML = "";
        performanceChartRoot.innerHTML = "";
        performanceDistributionRoot.innerHTML = "";
      }
    }
  }

  function renderQueryNotificationMenu() {
    const menu = queryNotificationMenu();
    const listRoot = queryNotificationList();
    const countRoot = queryNotificationCount();
    const clearButton = notificationClearButton();
    if (!menu || !listRoot || !countRoot) {
      return;
    }

    const visibleNotifications = collectVisibleNotifications();
    const { summary: queryJobsSummary } = queryState();
    const { summary: dataGenerationJobsSummary } = dataGenerationState();
    const { summary: downloadJobsSummary } = downloadState();
    const { summary: s3DeleteJobsSummary } = s3DeleteState();
    const pipelineNotificationSummary = getPipelineNotificationSummary();
    const hasRunningActivity =
      Number(queryJobsSummary.runningCount || 0) > 0 ||
      Number(dataGenerationJobsSummary.runningCount || 0) > 0 ||
      Number(downloadJobsSummary?.runningCount || 0) > 0 ||
      Number(s3DeleteJobsSummary?.runningCount || 0) > 0 ||
      Number(pipelineNotificationSummary?.runningCount || 0) > 0;
    const badgeCount = visibleNotifications.length;
    countRoot.textContent = String(badgeCount);
    countRoot.hidden = badgeCount === 0;
    countRoot.classList.toggle("is-live", hasRunningActivity);
    if (clearButton) {
      clearButton.hidden = !visibleNotifications.length;
    }

    const notificationMarkup = visibleNotifications.length
      ? visibleNotifications.slice(0, 12).map((item) => item.markup).join("")
      : '<p class="topbar-notification-empty">No notifications yet.</p>';
    const notificationSignature = JSON.stringify([
      queryState().version,
      dataGenerationState().version,
      downloadState().version,
      s3DeleteState().version,
      pipelineNotificationSummary?.version,
      [...getDismissedNotificationKeys()],
    ]);
    if (listRoot.dataset.renderSignature !== notificationSignature) {
      listRoot.innerHTML = notificationMarkup;
      listRoot.dataset.renderSignature = notificationSignature;
    }
  }

  function syncQueryCellJobState(cellRoot) {
    if (!(cellRoot instanceof Element)) {
      return;
    }

    const cellLanguage = String(
      cellRoot.dataset.defaultCellLanguage ||
      cellRoot.querySelector("[data-editor-root]")?.dataset.editorLanguage ||
      cellRoot.querySelector("[data-editor-source]")?.dataset.editorLanguage ||
      "sql"
    )
      .trim()
      .toLowerCase();
    if (cellLanguage === "python") {
      return;
    }

    const workspaceRoot = cellRoot.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = cellRoot.dataset.cellId;
    const job = queryJobForCell(notebookId, cellId);
    const cancelling = queryJobIsRunning(job) && Boolean(job?.cancellationPhase);
    const canCancel = queryJobIsRunning(job) && job?.canCancel !== false;
    const runButton = cellRoot.querySelector("[data-run-cell]");
    const explainButton = cellRoot.querySelector("[data-explain-cell]");
    const cancelButton = cellRoot.querySelector("[data-cancel-query]");
    const resultRoot = cellRoot.querySelector("[data-cell-result]");

    cellRoot.classList.toggle("is-query-running", queryJobIsRunning(job));

    if (runButton) {
      if (queryJobIsRunning(job)) {
        runButton.disabled = true;
        runButton.classList.add("is-running");
        runButton.innerHTML =
          `<span class="query-button-spinner" aria-hidden="true"></span><span class="query-button-running-copy">${cancelling ? "Cancelling ..." : "Running ..."}</span>`;
      } else {
        runButton.disabled = false;
        runButton.classList.remove("is-running");
        runButton.textContent = runCellButtonLabelForCell(cellRoot);
      }
    }

    if (explainButton) {
      explainButton.disabled = queryJobIsRunning(job);
      explainButton.title = queryJobIsRunning(job)
        ? "Explain is unavailable while this cell is running."
        : "Explain this SQL cell without running it.";
    }

    if (cancelButton) {
      cancelButton.hidden = !canCancel;
      cancelButton.dataset.jobId = canCancel ? job?.jobId || "" : "";
      cancelButton.dataset.jobKind = "query";
      cancelButton.disabled = !canCancel || cancelling;
      cancelButton.textContent = cancelling ? "Cancelling..." : "Cancel";
      cancelButton.classList.toggle("is-cancelling", cancelling);
    }

    const resultSignature = JSON.stringify(job ?? null);
    if (resultRoot && resultRoot.dataset.resultSignature !== resultSignature) {
      resultRoot.outerHTML = queryResultPanelMarkup(cellId, job);
      const nextResultRoot = cellRoot.querySelector("[data-cell-result]");
      if (nextResultRoot) {
        nextResultRoot.dataset.resultSignature = resultSignature;
      }
    } else if (cellId) {
      cellRoot
        .querySelector("[data-query-form]")
        ?.insertAdjacentHTML("afterend", queryResultPanelMarkup(cellId, job));
      const nextResultRoot = cellRoot.querySelector("[data-cell-result]");
      if (nextResultRoot) {
        nextResultRoot.dataset.resultSignature = resultSignature;
      }
    }

    querySourceValidationController?.syncQueryJobState?.(cellRoot, job);
    syncCellCacheHydrationJobState?.(cellRoot, job);
  }

  function syncVisibleQueryCells() {
    document.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
      syncQueryCellJobState(cellRoot);
    });
  }

  function applyQueryJobsState(snapshot) {
    queryJobsLoaded = true;
    const normalizedJobs = Array.isArray(snapshot?.jobs)
      ? snapshot.jobs.map((job) => normalizeQueryJob(job)).filter(Boolean)
      : [];
    setQueryState({
      version: snapshot?.version ?? null,
      summary: snapshot?.summary ?? { runningCount: 0, totalCount: 0 },
      performance: snapshot?.performance ?? { recent: [], stats: {} },
      snapshot: decorateQueryJobsWithInsights(normalizedJobs).sort(compareQueryJobsByStartedAt),
    });

    pruneDismissedNotificationKeys();
    renderQueryMonitor();
    renderQueryNotificationMenu();
    syncVisibleQueryCells();
    syncQueryClockLoop();
    renderHomePage();
  }

  function applyDataGenerationJobsState(snapshot) {
    dataGenerationJobsLoaded = true;
    setDataGenerationState({
      version: snapshot?.version ?? null,
      summary: snapshot?.summary ?? { runningCount: 0, totalCount: 0 },
      snapshot: Array.isArray(snapshot?.jobs)
        ? snapshot.jobs
            .map((job) => normalizeDataGenerationJob(job))
            .filter(Boolean)
            .sort(compareDataGenerationJobsByStartedAt)
        : [],
    });

    pruneDismissedNotificationKeys();
    renderIngestionWorkbench({
      refreshGeneratorCards: currentWorkspaceMode() !== "loader",
    });
    renderDataGenerationMonitor();
    renderQueryNotificationMenu();
    syncDataGenerationClockLoop();
    maybeRefreshSidebarForCompletedGenerationJobs();
    renderHomePage();
  }

  return {
    applyDataGenerationJobsState,
    applyQueryJobsState,
    clearVisibleNotifications,
    renderQueryMonitor,
    renderQueryNotificationMenu,
    syncVisibleQueryCells,
  };
}

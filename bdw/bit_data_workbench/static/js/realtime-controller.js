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
    renderDataGenerationMonitor,
    renderHomePage,
    renderIngestionWorkbench,
    renderPerformanceChartMarkup,
    renderPerformanceDistributionMarkup,
    refreshSidebar,
    setDataGenerationState,
    setQueryState,
    sidebarQueryCounts,
    writeDismissedNotificationKeys,
    workspaceNotebookId,
  } = helpers;

  let queryJobsClockHandle = null;
  let queryJobsLoaded = false;
  let dataGenerationClockHandle = null;
  let dataGenerationJobsLoaded = false;
  const refreshedDataGenerationJobIds = new Set();

  function queryState() {
    return getQueryState();
  }

  function dataGenerationState() {
    return getDataGenerationState();
  }

  function pruneDismissedNotificationKeys() {
    const queryJobsSnapshot = queryState().snapshot;
    const dataGenerationJobsSnapshot = dataGenerationState().snapshot;
    const validKeys = new Set([
      ...queryJobsSnapshot.map((job) => notificationItemKey("query", job)),
      ...dataGenerationJobsSnapshot.map((job) => notificationItemKey("ingestion", job)),
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
    if (hasRunningJobs && dataGenerationClockHandle === null) {
      refreshLiveDataGenerationClock();
      dataGenerationClockHandle = window.setInterval(refreshLiveDataGenerationClock, 100);
      return;
    }

    if (!hasRunningJobs && dataGenerationClockHandle !== null) {
      window.clearInterval(dataGenerationClockHandle);
      dataGenerationClockHandle = null;
    }

    refreshLiveDataGenerationClock();
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
    if (hasRunningJobs && queryJobsClockHandle === null) {
      refreshLiveQueryClock();
      queryJobsClockHandle = window.setInterval(refreshLiveQueryClock, 100);
      return;
    }

    if (!hasRunningJobs && queryJobsClockHandle !== null) {
      window.clearInterval(queryJobsClockHandle);
      queryJobsClockHandle = null;
    }

    refreshLiveQueryClock();
  }

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
    countRoot.textContent = String(runningCount);
    countRoot.classList.toggle("is-live", runningCount > 0);
    toggleCountRoots.forEach((toggleCountRoot) => {
      toggleCountRoot.textContent = String(runningCount);
      toggleCountRoot.hidden = runningCount === 0;
      toggleCountRoot.classList.toggle("is-live", runningCount > 0);
    });

    if (!queryJobsSnapshot.length) {
      listRoot.innerHTML = '<p class="query-monitor-empty">No query jobs yet.</p>';
    } else {
      listRoot.innerHTML = queryJobsSnapshot.slice(0, 8).map((job) => queryMonitorItemMarkup(job)).join("");
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
    const hasRunningActivity =
      Number(queryJobsSummary.runningCount || 0) > 0 || Number(dataGenerationJobsSummary.runningCount || 0) > 0;
    const badgeCount = visibleNotifications.length;
    countRoot.textContent = String(badgeCount);
    countRoot.hidden = badgeCount === 0;
    countRoot.classList.toggle("is-live", hasRunningActivity);
    if (clearButton) {
      clearButton.hidden = !visibleNotifications.length;
    }

    if (!visibleNotifications.length) {
      listRoot.innerHTML = '<p class="topbar-notification-empty">No notifications yet.</p>';
      return;
    }

    listRoot.innerHTML = visibleNotifications
      .slice(0, 12)
      .map((item) => item.markup)
      .join("");
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
    const runButton = cellRoot.querySelector("[data-run-cell]");
    const cancelButton = cellRoot.querySelector("[data-cancel-query]");
    const resultRoot = cellRoot.querySelector("[data-cell-result]");

    cellRoot.classList.toggle("is-query-running", queryJobIsRunning(job));

    if (runButton) {
      if (queryJobIsRunning(job)) {
        runButton.disabled = true;
        runButton.classList.add("is-running");
        runButton.innerHTML =
          '<span class="query-button-spinner" aria-hidden="true"></span><span class="query-button-running-copy">Running ...</span>';
      } else {
        runButton.disabled = false;
        runButton.classList.remove("is-running");
        runButton.textContent = "Run Cell";
      }
    }

    if (cancelButton) {
      cancelButton.hidden = !queryJobIsRunning(job);
      cancelButton.dataset.jobId = job?.jobId || "";
      cancelButton.dataset.jobKind = "query";
      cancelButton.disabled = !queryJobIsRunning(job);
    }

    if (resultRoot) {
      resultRoot.outerHTML = queryResultPanelMarkup(cellId, job);
    }
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

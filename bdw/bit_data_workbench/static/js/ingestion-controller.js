export function createIngestionController(helpers) {
  const {
    currentWorkspaceMode,
    currentWorkspaceNotebookId,
    dataGenerationJobCardMarkup,
    dataGenerationJobIsRunning,
    dataGenerationMonitorCount,
    dataGenerationMonitorItemMarkup,
    dataGenerationMonitorList,
    dataGenerationNotificationItemMarkup,
    escapeHtml,
    getDataGenerationJobsSnapshot,
    getDataGenerationTerminalStatuses,
    getDismissedNotificationKeys,
    getQueryJobsSnapshot,
    getQueryJobTerminalStatuses,
    ingestionGeneratorById,
    ingestionGeneratorList,
    ingestionGeneratorSectionCopy,
    ingestionGeneratorSectionTitle,
    ingestionJobList,
    ingestionJobSectionCopy,
    ingestionJobSectionTitle,
    notificationItemKey,
    queryJobTerminalStatuses,
    queryNotificationItemMarkup,
    resolveSelectedIngestionRunbookId,
    sidebarQueryCounts,
    dataGeneratorCardMarkup,
  } = helpers;

  function filteredIngestionGenerators() {
    const selectedGenerator = ingestionGeneratorById(resolveSelectedIngestionRunbookId());
    return selectedGenerator ? [selectedGenerator] : [];
  }

  function filteredDataGenerationJobs() {
    const selectedGeneratorId = resolveSelectedIngestionRunbookId();
    if (!selectedGeneratorId) {
      return [];
    }

    return getDataGenerationJobsSnapshot().filter((job) => job.generatorId === selectedGeneratorId);
  }

  function captureIngestionWorkbenchRenderState(generatorList, jobList) {
    const generatorSizes = new Map();
    if (generatorList instanceof Element) {
      generatorList.querySelectorAll("[data-generator-card]").forEach((card) => {
        const generatorId = String(card.dataset.generatorId || "").trim();
        const sizeInput = card.querySelector("[data-ingestion-size-input]");
        if (!generatorId || !(sizeInput instanceof HTMLInputElement)) {
          return;
        }
        generatorSizes.set(generatorId, sizeInput.value);
      });
    }

    let focusedGeneratorId = "";
    const activeElement = document.activeElement;
    if (
      generatorList instanceof Element &&
      activeElement instanceof HTMLInputElement &&
      activeElement.matches("[data-ingestion-size-input]")
    ) {
      const generatorCard = activeElement.closest("[data-generator-card]");
      if (generatorCard instanceof Element && generatorList.contains(generatorCard)) {
        focusedGeneratorId = String(generatorCard.dataset.generatorId || "").trim();
      }
    }

    const openJobTargetDetails = new Set();
    if (jobList instanceof Element) {
      jobList.querySelectorAll("[data-ingestion-job-target-details][open]").forEach((detailsRoot) => {
        const jobId = String(
          detailsRoot.closest("[data-data-generation-job-card]")?.dataset.jobId || ""
        ).trim();
        if (jobId) {
          openJobTargetDetails.add(jobId);
        }
      });
    }

    return {
      generatorSizes,
      focusedGeneratorId,
      openJobTargetDetails,
    };
  }

  function restoreIngestionWorkbenchRenderState(
    state,
    generatorList,
    jobList,
    { refreshGeneratorCards = true } = {}
  ) {
    if (!state) {
      return;
    }

    if (refreshGeneratorCards && generatorList instanceof Element) {
      state.generatorSizes.forEach((value, generatorId) => {
        const input = generatorList.querySelector(
          `[data-generator-card][data-generator-id="${CSS.escape(generatorId)}"] [data-ingestion-size-input]`
        );
        if (input instanceof HTMLInputElement) {
          input.value = value;
        }
      });

      if (state.focusedGeneratorId) {
        const focusedInput = generatorList.querySelector(
          `[data-generator-card][data-generator-id="${CSS.escape(state.focusedGeneratorId)}"] [data-ingestion-size-input]`
        );
        if (focusedInput instanceof HTMLInputElement) {
          focusedInput.focus({ preventScroll: true });
        }
      }
    }

    if (jobList instanceof Element) {
      jobList.querySelectorAll("[data-ingestion-job-target-details]").forEach((detailsRoot) => {
        const jobId = String(
          detailsRoot.closest("[data-data-generation-job-card]")?.dataset.jobId || ""
        ).trim();
        detailsRoot.open = Boolean(jobId && state.openJobTargetDetails.has(jobId));
      });
    }
  }

  function renderIngestionWorkbench({ refreshGeneratorCards = true } = {}) {
    const generatorList = ingestionGeneratorList();
    const jobList = ingestionJobList();
    const generatorSectionTitle = ingestionGeneratorSectionTitle();
    const generatorSectionCopy = ingestionGeneratorSectionCopy();
    const jobSectionTitle = ingestionJobSectionTitle();
    const jobSectionCopy = ingestionJobSectionCopy();
    const selectedGeneratorId = resolveSelectedIngestionRunbookId();
    const selectedGenerator = ingestionGeneratorById(selectedGeneratorId);
    const visibleGenerators = filteredIngestionGenerators();
    const visibleJobs = filteredDataGenerationJobs();
    const renderState = captureIngestionWorkbenchRenderState(generatorList, jobList);

    if (generatorSectionTitle) {
      generatorSectionTitle.textContent = selectedGenerator ? selectedGenerator.title : "Selected Loader";
    }

    if (generatorSectionCopy) {
      generatorSectionCopy.innerHTML = selectedGenerator
        ? `Only the selected loader is shown here. Module: <code>${escapeHtml(
            selectedGenerator.moduleName || selectedGenerator.generatorId
          )}</code>`
        : "No loader is currently selected.";
    }

    if (jobSectionTitle) {
      jobSectionTitle.textContent = selectedGenerator ? `${selectedGenerator.title} Jobs` : "Loader Jobs";
    }

    if (jobSectionCopy) {
      jobSectionCopy.textContent = selectedGenerator
        ? "Only executions for the selected loader are listed here."
        : "Select a loader from the left navigation to inspect its executions.";
    }

    if (generatorList && refreshGeneratorCards) {
      if (!visibleGenerators.length) {
        generatorList.innerHTML = '<p class="ingestion-empty">No loader modules discovered.</p>';
      } else {
        generatorList.innerHTML = visibleGenerators.map((generator) => dataGeneratorCardMarkup(generator)).join("");
      }
    }

    if (jobList) {
      if (!selectedGenerator) {
        jobList.innerHTML = '<p class="ingestion-empty">Select a loader from the left navigation.</p>';
      } else if (!visibleJobs.length) {
        jobList.innerHTML =
          '<p class="ingestion-empty">No loader jobs for this loader yet. Run it first, then clean its output from the completed job card.</p>';
      } else {
        jobList.innerHTML = visibleJobs.map((job) => dataGenerationJobCardMarkup(job)).join("");
      }
    }

    restoreIngestionWorkbenchRenderState(renderState, generatorList, jobList, {
      refreshGeneratorCards,
    });
  }

  function renderDataGenerationMonitor() {
    const listRoot = dataGenerationMonitorList();
    const countRoot = dataGenerationMonitorCount();
    const toggleCountRoots = sidebarQueryCounts();
    if (!listRoot || !countRoot) {
      return;
    }

    const visibleJobs = currentWorkspaceMode() === "loader"
      ? filteredDataGenerationJobs()
      : getDataGenerationJobsSnapshot();
    const runningCount = visibleJobs.filter((job) => dataGenerationJobIsRunning(job)).length;
    countRoot.textContent = String(runningCount);
    countRoot.classList.toggle("is-live", runningCount > 0);

    if (currentWorkspaceMode() === "loader") {
      toggleCountRoots.forEach((toggleCountRoot) => {
        toggleCountRoot.textContent = String(runningCount);
        toggleCountRoot.hidden = runningCount === 0;
        toggleCountRoot.classList.toggle("is-live", runningCount > 0);
      });
    }

    if (!visibleJobs.length) {
      listRoot.innerHTML =
        currentWorkspaceMode() === "loader"
          ? '<p class="query-monitor-empty">No loader jobs for this loader yet.</p>'
          : '<p class="query-monitor-empty">No loader jobs yet.</p>';
      return;
    }

    listRoot.innerHTML = visibleJobs
      .slice(0, 8)
      .map((job) => dataGenerationMonitorItemMarkup(job))
      .join("");
  }

  function collectVisibleNotifications() {
    const activeNotebookId = currentWorkspaceNotebookId();
    const queryNotifications = getQueryJobsSnapshot()
      .filter((job) => job.notebookId !== activeNotebookId)
      .map((job) => ({
        type: "query",
        job,
        updatedAt: job.updatedAt,
        dismissalKey: notificationItemKey("query", job),
        dismissible: getQueryJobTerminalStatuses().has(job.status),
        markup: queryNotificationItemMarkup(job),
      }));
    const dataGenerationNotifications = getDataGenerationJobsSnapshot().map((job) => ({
      type: "ingestion",
      job,
      updatedAt: job.updatedAt,
      dismissalKey: notificationItemKey("ingestion", job),
      dismissible: getDataGenerationTerminalStatuses().has(job.status),
      markup: dataGenerationNotificationItemMarkup(job),
    }));

    return [...dataGenerationNotifications, ...queryNotifications]
      .filter((item) => !getDismissedNotificationKeys().has(item.dismissalKey))
      .sort((left, right) => Date.parse(right.updatedAt || "") - Date.parse(left.updatedAt || ""));
  }

  return {
    collectVisibleNotifications,
    renderDataGenerationMonitor,
    renderIngestionWorkbench,
  };
}

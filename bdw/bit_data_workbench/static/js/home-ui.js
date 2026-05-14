export function createHomeUi(helpers) {
  const {
    dataGenerationJobElapsedMs,
    escapeHtml,
    formatQueryDuration,
    formatRelativeTimestamp,
    getDataGenerationJobsSnapshot,
    homePageRoot,
    homeRecentIngestionsRoot,
    homeRecentNotebooksRoot,
    notebookLinks,
    readNotebookActivity,
  } = helpers;

  function notebookActivityReason(entry, { compact = false } = {}) {
    if (entry.reason === "run") {
      return compact ? "Run" : "Last action: Run";
    }
    if (entry.reason === "open") {
      return compact ? "Open" : "Last action: Open";
    }
    return compact ? "Edit" : "Last action: Edit";
  }

  function notebookActivityMarkup(entry) {
    return `
      <button
        type="button"
        class="home-activity-card"
        data-open-recent-notebook="${escapeHtml(entry.notebookId)}"
      >
        <span class="home-activity-title-row">
          <span class="home-activity-title">${escapeHtml(entry.title || "Notebook")}</span>
          <span class="home-activity-meta">${escapeHtml(formatRelativeTimestamp(entry.touchedAt))}</span>
        </span>
        <span class="home-activity-copy">${escapeHtml(entry.summary || "No description saved.")}</span>
        <span class="home-activity-meta">${escapeHtml(notebookActivityReason(entry))}</span>
      </button>
    `;
  }

  function recentNotebookActivityEntries(limit) {
    return Object.values(readNotebookActivity())
      .filter((entry) => entry && typeof entry === "object")
      .map((entry) => ({
        notebookId: String(entry.notebookId || "").trim(),
        title: String(entry.title || "").trim(),
        summary: String(entry.summary || "").trim(),
        touchedAt: String(entry.touchedAt || "").trim(),
        reason: ["open", "run"].includes(entry.reason) ? entry.reason : "edited",
      }))
      .filter((entry) => entry.notebookId && notebookLinks(entry.notebookId).length)
      .sort((left, right) => Date.parse(right.touchedAt || "") - Date.parse(left.touchedAt || ""))
      .slice(0, limit);
  }

  function renderRecentNotebooks(root, { limit } = {}) {
    if (!root) {
      return;
    }

    const entries = recentNotebookActivityEntries(limit);
    if (!entries.length) {
      root.innerHTML = '<p class="home-empty">No recent notebook activity yet.</p>';
      return;
    }

    root.innerHTML = entries.map((entry) => notebookActivityMarkup(entry)).join("");
  }

  function ingestionActivityMarkup(job) {
    return `
      <button
        type="button"
        class="home-activity-card"
        data-open-loader-workbench
        data-focus-generation-job="${escapeHtml(job.jobId || "")}" 
      >
        <span class="home-activity-title-row">
          <span class="home-activity-title">${escapeHtml(job.title || "Loader run")}</span>
          <span class="home-activity-meta">${escapeHtml(formatRelativeTimestamp(job.startedAt || job.updatedAt))}</span>
        </span>
        <span class="home-activity-copy">${escapeHtml(job.message || job.description || "No loader message yet.")}</span>
        <span class="home-activity-meta">${escapeHtml((job.status || "unknown").replace(/^./, (match) => match.toUpperCase()))} • ${escapeHtml(formatQueryDuration(dataGenerationJobElapsedMs(job)))}</span>
      </button>
    `;
  }

  function renderHomePage() {
    renderRecentNotebooks(homeRecentNotebooksRoot(), { limit: 3 });

    if (!homePageRoot()) {
      return;
    }

    const recentIngestionsRoot = homeRecentIngestionsRoot();
    if (recentIngestionsRoot) {
      const recentJobs = [...getDataGenerationJobsSnapshot()]
        .sort((left, right) => Date.parse(right.startedAt || "") - Date.parse(left.startedAt || ""))
        .slice(0, 3);
      if (!recentJobs.length) {
        recentIngestionsRoot.innerHTML = '<p class="home-empty">No loader runs yet.</p>';
      } else {
        recentIngestionsRoot.innerHTML = recentJobs.map((job) => ingestionActivityMarkup(job)).join("");
      }
    }
  }

  return {
    renderHomePage,
  };
}

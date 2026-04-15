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

  function notebookActivityMarkup(entry) {
    const reasonCopy = entry.reason === "run" ? "Last action: Run" : "Last action: Edit";
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
        <span class="home-activity-meta">${escapeHtml(reasonCopy)}</span>
      </button>
    `;
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
    if (!homePageRoot()) {
      return;
    }

    const recentNotebooksRoot = homeRecentNotebooksRoot();
    if (recentNotebooksRoot) {
      const activityEntries = Object.values(readNotebookActivity())
        .filter((entry) => entry && typeof entry === "object")
        .map((entry) => ({
          notebookId: String(entry.notebookId || "").trim(),
          title: String(entry.title || "").trim(),
          summary: String(entry.summary || "").trim(),
          touchedAt: String(entry.touchedAt || "").trim(),
          reason: entry.reason === "run" ? "run" : "edited",
        }))
        .filter((entry) => entry.notebookId && notebookLinks(entry.notebookId).length)
        .sort((left, right) => Date.parse(right.touchedAt || "") - Date.parse(left.touchedAt || ""))
        .slice(0, 3);

      if (!activityEntries.length) {
        recentNotebooksRoot.innerHTML = '<p class="home-empty">No recent notebook activity yet.</p>';
      } else {
        recentNotebooksRoot.innerHTML = activityEntries.map((entry) => notebookActivityMarkup(entry)).join("");
      }
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

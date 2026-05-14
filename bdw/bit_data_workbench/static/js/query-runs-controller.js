export function createQueryRunsController(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    formatByteCount,
    formatQueryDuration,
    formatQueryTimestamp,
    queryResourceSparklineMarkup,
  } = helpers;

  const terminalStatuses = new Set(["completed", "failed", "cancelled"]);
  const refreshTimers = new WeakMap();

  function pageRoot() {
    return document.querySelector("[data-query-runs-page]");
  }

  function notebookRoots(root = document) {
    return Array.from(root.querySelectorAll?.("[data-notebook-query-runs]") || []);
  }

  function statusCopy(status) {
    switch (String(status || "").trim().toLowerCase()) {
      case "completed":
        return "Completed";
      case "failed":
        return "Failed";
      case "cancelled":
        return "Cancelled";
      default:
        return "Unknown";
    }
  }

  function runMetrics(run) {
    const metrics = run?.metrics && typeof run.metrics === "object" ? run.metrics : {};
    return {
      cpuPercent: metrics.cpuPercent ?? run?.cpuPercent,
      averageCpuPercent: metrics.averageCpuPercent ?? run?.averageCpuPercent,
      peakCpuPercent: metrics.peakCpuPercent ?? run?.peakCpuPercent,
      memoryRssBytes: metrics.memoryRssBytes ?? run?.memoryRssBytes,
      averageMemoryRssBytes: metrics.averageMemoryRssBytes ?? run?.averageMemoryRssBytes,
      peakMemoryRssBytes: metrics.peakMemoryRssBytes ?? run?.peakMemoryRssBytes,
    };
  }

  function formatCpu(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0%";
    }
    return `${numeric.toFixed(numeric >= 10 ? 0 : 1)}%`;
  }

  function runMetricsMarkup(run) {
    const metrics = runMetrics(run);
    const entries = [
      ["CPU avg", formatCpu(metrics.averageCpuPercent)],
      ["CPU peak", formatCpu(metrics.peakCpuPercent)],
      ["RAM avg", formatByteCount(metrics.averageMemoryRssBytes)],
      ["RAM peak", formatByteCount(metrics.peakMemoryRssBytes)],
    ];
    return `
      <div class="query-run-history-metrics">
        ${entries
          .map(
            ([label, value]) => `
              <span class="query-run-history-metric">
                <strong>${escapeHtml(label)}</strong>
                <span>${escapeHtml(value)}</span>
              </span>
            `
          )
          .join("")}
      </div>
    `;
  }

  function runResourceChartMarkup(run) {
    if (typeof queryResourceSparklineMarkup !== "function") {
      return "";
    }
    return queryResourceSparklineMarkup(
      {
        ...run,
        ...runMetrics(run),
        resourceSamples: Array.isArray(run?.resourceSamples) ? run.resourceSamples : [],
      },
      { compact: true }
    );
  }

  function runItemMarkup(run) {
    const status = String(run?.status || "").trim().toLowerCase();
    const title = String(run?.notebookTitle || run?.notebookId || "Notebook").trim();
    const completed = run?.completedAt || run?.updatedAt || run?.startedAt || "";
    const message = run?.error || run?.message || "";
    return `
      <article class="query-run-history-item query-run-history-item-${escapeHtml(status || "unknown")}">
        <div class="query-run-history-main">
          <div class="query-run-history-title-row">
            <strong>${escapeHtml(title)}</strong>
            <span class="query-run-history-status is-${escapeHtml(status || "unknown")}">${escapeHtml(statusCopy(status))}</span>
          </div>
          <div class="query-run-history-meta">
            <span>${escapeHtml(formatQueryTimestamp(completed) || "Unknown time")}</span>
            <span>${escapeHtml(formatQueryDuration(Number(run?.durationMs || 0)))}</span>
            <span>${escapeHtml(run?.cellId || "Cell")}</span>
          </div>
          ${runMetricsMarkup(run)}
          ${runResourceChartMarkup(run)}
          ${message ? `<p class="query-run-history-message">${escapeHtml(message)}</p>` : ""}
          <details class="query-run-history-sql">
            <summary>SQL</summary>
            <pre>${escapeHtml(run?.sql || "")}</pre>
          </details>
        </div>
      </article>
    `;
  }

  function emptyMessage(payload) {
    return payload?.message || "No recorded query runs yet.";
  }

  function renderList(root, payload) {
    const listRoot = root.querySelector("[data-query-runs-list]");
    const statusRoot = root.querySelector("[data-query-runs-status]");
    if (!(listRoot instanceof HTMLElement)) {
      return;
    }
    const runs = Array.isArray(payload?.runs) ? payload.runs : [];
    if (statusRoot instanceof HTMLElement) {
      statusRoot.textContent = payload?.available === false
        ? payload?.message || "Query-run history is not available."
        : runs.length
          ? `${runs.length} recorded run(s)`
          : emptyMessage(payload);
    }
    if (!runs.length) {
      listRoot.innerHTML = `<p class="home-empty">${escapeHtml(emptyMessage(payload))}</p>`;
      return;
    }
    listRoot.innerHTML = runs.map((run) => runItemMarkup(run)).join("");
  }

  async function loadInto(root, { quiet = false } = {}) {
    if (!(root instanceof HTMLElement)) {
      return;
    }
    const params = new URLSearchParams();
    const notebookId = String(root.dataset.queryRunsNotebookId || "").trim();
    const cellId = String(root.dataset.queryRunsCellId || "").trim();
    if (notebookId) {
      params.set("notebookId", notebookId);
    }
    if (cellId) {
      params.set("cellId", cellId);
    }
    params.set("limit", root.dataset.queryRunsLimit || "100");
    const statusRoot = root.querySelector("[data-query-runs-status]");
    if (!quiet && statusRoot instanceof HTMLElement && root.dataset.queryRunsLoaded === "true") {
      statusRoot.textContent = "Refreshing recorded query runs...";
    }
    try {
      const payload = await fetchJsonOrThrow(`/api/query-runs?${params.toString()}`);
      root.dataset.queryRunsLoaded = "true";
      renderList(root, payload);
    } catch (error) {
      root.dataset.queryRunsLoaded = "true";
      renderList(root, {
        available: false,
        runs: [],
        message: error instanceof Error ? error.message : "Query-run history could not be loaded.",
      });
    }
  }

  function rootMatchesJob(root, job) {
    const notebookId = String(root?.dataset?.queryRunsNotebookId || "").trim();
    const cellId = String(root?.dataset?.queryRunsCellId || "").trim();
    const jobNotebookId = String(job?.notebookId || "").trim();
    const jobCellId = String(job?.cellId || "").trim();
    if (notebookId && notebookId !== jobNotebookId) {
      return false;
    }
    if (cellId && cellId !== jobCellId) {
      return false;
    }
    return true;
  }

  function scheduleLoadInto(root, { delayMs = 750, followUpDelayMs = 2500 } = {}) {
    if (!(root instanceof HTMLElement)) {
      return;
    }
    const refresh = () => {
      if (!root.isConnected) {
        return;
      }
      loadInto(root, { quiet: true }).catch((error) => {
        console.error("Failed to refresh query runs.", error);
      });
    };
    const existingTimer = refreshTimers.get(root);
    if (existingTimer) {
      window.clearTimeout(existingTimer);
    }
    const timer = window.setTimeout(() => {
      refreshTimers.delete(root);
      refresh();
      if (followUpDelayMs > delayMs) {
        window.setTimeout(refresh, followUpDelayMs - delayMs);
      }
    }, delayMs);
    refreshTimers.set(root, timer);
  }

  async function initializeCurrentPage(root = document) {
    const globalRoot = pageRoot();
    if (globalRoot) {
      await loadInto(globalRoot);
    }
    await Promise.all(notebookRoots(root).map((node) => loadInto(node)));
  }

  async function handleClick(event) {
    const refreshButton = event.target.closest("[data-query-runs-refresh]");
    if (!refreshButton) {
      return false;
    }
    event.preventDefault();
    event.stopPropagation();
    const root = refreshButton.closest("[data-query-runs-page], [data-notebook-query-runs]");
    await loadInto(root);
    return true;
  }

  function refreshForQueryJobsSnapshot(snapshot) {
    const jobs = Array.isArray(snapshot?.jobs) ? snapshot.jobs : [];
    const terminalJobs = jobs.filter((job) => terminalStatuses.has(String(job?.status || "").trim().toLowerCase()));
    if (!terminalJobs.length) {
      return;
    }
    const roots = [pageRoot(), ...notebookRoots(document)].filter(Boolean);
    roots.forEach((root) => {
      if (terminalJobs.some((job) => rootMatchesJob(root, job))) {
        scheduleLoadInto(root);
      }
    });
  }

  return {
    handleClick,
    initializeCurrentPage,
    refreshForQueryJobsSnapshot,
  };
}

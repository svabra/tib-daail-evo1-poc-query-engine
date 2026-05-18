export function createQueryRunsController(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    formatByteCount,
    formatQueryDuration,
    queryResourceSparklineMarkup,
  } = helpers;

  const terminalStatuses = new Set(["completed", "failed", "cancelled"]);
  const refreshTimers = new WeakMap();

  function pageRoot() {
    return document.querySelector("[data-query-runs-page]");
  }

  function notebookRoots(root = document) {
    const roots = [];
    if (root instanceof Element && root.matches("[data-notebook-query-runs]")) {
      roots.push(root);
    }
    roots.push(...Array.from(root.querySelectorAll?.("[data-notebook-query-runs]") || []));
    return roots;
  }

  function statusCopy(status) {
    switch (String(status || "").trim().toLowerCase()) {
      case "queued":
        return "Queued";
      case "running":
        return "Running";
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

  function runStatusMarkup(status) {
    const normalizedStatus = String(status || "").trim().toLowerCase();
    return `<span class="query-run-history-status is-${escapeHtml(normalizedStatus || "unknown")}">${escapeHtml(statusCopy(normalizedStatus))}</span>`;
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

  function queryRunsShowCharts(root) {
    return root?.dataset?.queryRunsShowCharts === "true";
  }

  function queryRunsLiveOnly(root) {
    return root?.dataset?.queryRunsLiveOnly === "true";
  }

  function syncChartToggle(root) {
    const toggleButton = root?.querySelector?.("[data-query-runs-toggle-charts]");
    if (!(toggleButton instanceof HTMLButtonElement)) {
      return;
    }
    const showCharts = queryRunsShowCharts(root);
    toggleButton.classList.toggle("is-on", showCharts);
    toggleButton.setAttribute("aria-pressed", showCharts ? "true" : "false");
    toggleButton.title = showCharts ? "Hide resource charts" : "Show resource charts";
    const label = toggleButton.querySelector("[data-query-runs-toggle-label]");
    if (label) {
      label.textContent = showCharts ? "Hide resource charts" : "Show resource charts";
      return;
    }
    toggleButton.textContent = showCharts ? "Hide resource charts" : "Show resource charts";
  }

  function syncLiveToggle(root) {
    const toggleButton = root?.querySelector?.("[data-query-runs-toggle-live]");
    if (!(toggleButton instanceof HTMLButtonElement)) {
      return;
    }
    const liveOnly = queryRunsLiveOnly(root);
    toggleButton.classList.toggle("is-on", liveOnly);
    toggleButton.setAttribute("aria-pressed", liveOnly ? "true" : "false");
    toggleButton.title = liveOnly
      ? "Show all recorded and live query runs"
      : "Show live queued and running queries only";
  }

  function expandedSqlRuns(root) {
    if (!root._bdwQueryRunsExpandedSql) {
      root._bdwQueryRunsExpandedSql = new Set();
    }
    return root._bdwQueryRunsExpandedSql;
  }

  function expandedProgressRuns(root) {
    if (!root._bdwQueryRunsExpandedProgress) {
      root._bdwQueryRunsExpandedProgress = new Set();
    }
    return root._bdwQueryRunsExpandedProgress;
  }

  function runKey(run, index) {
    const jobId = String(run?.jobId || "").trim();
    if (jobId) {
      return jobId;
    }
    return `run-${index}`;
  }

  function formatRunDateTime(value) {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return "n/a";
    }
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "medium",
    }).format(parsed);
  }

  function formatCpu(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "0%";
    }
    return `${numeric.toFixed(numeric >= 10 ? 0 : 1)}%`;
  }

  function formatMetric(value, formatter) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "n/a";
    }
    return formatter(numeric);
  }

  function formatRows(run) {
    const rowCount = Number(run?.rowCount);
    const rowsShown = Number(run?.rowsShown);
    if (Number.isFinite(rowCount) && rowCount > 0) {
      return `${rowCount.toLocaleString()} row(s)`;
    }
    if (Number.isFinite(rowsShown) && rowsShown > 0) {
      return `${rowsShown.toLocaleString()} shown`;
    }
    return "n/a";
  }

  function normalizedRunTimings(run) {
    const timings = run?.timings && typeof run.timings === "object" ? run.timings : {};
    const valueFor = (key) => {
      const numeric = Number(timings[key]);
      return Number.isFinite(numeric) && numeric >= 0 ? numeric : null;
    };
    const backendTotalMs = valueFor("backendTotalMs");
    const clientTotalMs = valueFor("clientTotalMs");
    const totalMs =
      clientTotalMs ??
      backendTotalMs ??
      (Number.isFinite(Number(run?.durationMs)) && Number(run.durationMs) >= 0 ? Number(run.durationMs) : null);
    const deliveryMs =
      clientTotalMs !== null && backendTotalMs !== null
        ? Math.max(0, clientTotalMs - backendTotalMs)
        : null;
    return [
      ["total", totalMs],
      ["prepare", valueFor("backendPrepareMs")],
      ["file lock", valueFor("engineAccessWaitMs")],
      ["startup", valueFor("workerStartupMs")],
      ["source setup", valueFor("sourceBootstrapMs")],
      ["query", valueFor("engineQueryMs")],
      ["fetch", valueFor("resultFetchMs")],
      ["delivery", deliveryMs],
    ].filter(([, value]) => value !== null && Number.isFinite(value));
  }

  function runTimingBreakdownMarkup(run) {
    const parts = normalizedRunTimings(run);
    if (!parts.length) {
      return "";
    }
    return `
      <small class="query-run-history-timing">
        ${parts
          .map(([label, value]) => `<span>${escapeHtml(label)} ${escapeHtml(formatQueryDuration(value))}</span>`)
          .join("")}
      </small>
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

  function runSqlToggleMarkup(run, key, expanded) {
    const sql = String(run?.sql || "").trim();
    if (!sql) {
      return '<span class="query-run-history-muted">n/a</span>';
    }
    return `
      <button
        type="button"
        class="query-run-history-sql-toggle"
        data-query-run-sql-toggle
        data-query-run-id="${escapeHtml(key)}"
        aria-expanded="${expanded ? "true" : "false"}"
      >
        <span class="query-run-history-sql-chevron" aria-hidden="true"></span>
        <span>SQL</span>
      </button>
    `;
  }

  function runProgressEvents(run) {
    return Array.isArray(run?.progressEvents) ? run.progressEvents : [];
  }

  function runProgressToggleMarkup(run, key, expanded) {
    const events = runProgressEvents(run);
    if (!events.length) {
      return '<span class="query-run-history-muted">n/a</span>';
    }
    return `
      <button
        type="button"
        class="query-run-history-sql-toggle"
        data-query-run-progress-toggle
        data-query-run-id="${escapeHtml(key)}"
        aria-expanded="${expanded ? "true" : "false"}"
      >
        <span class="query-run-history-sql-chevron" aria-hidden="true"></span>
        <span>${escapeHtml(events.length.toLocaleString())} event(s)</span>
      </button>
    `;
  }

  function progressEventTime(event) {
    const displayTime = String(event?.displayTime || "").trim();
    if (displayTime) {
      return displayTime;
    }
    return formatRunDateTime(event?.occurredAt || "");
  }

  function formatProgressValue(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "";
    }
    return `${numeric.toFixed(numeric >= 10 ? 1 : 2)}`;
  }

  function duckdbProfilePillsMarkup(profile) {
    if (!profile || typeof profile !== "object") {
      return "";
    }
    const summaryEntries = [
      ["latency", profile.duckdb_latency_ms, "ms"],
      ["cpu", profile.duckdb_cpu_ms, "ms"],
      ["blocked", profile.duckdb_blocked_thread_ms, "ms"],
      ["rows scanned", profile.duckdb_rows_scanned, ""],
      ["rows returned", profile.duckdb_rows_returned, ""],
      ["bytes read", profile.duckdb_bytes_read, "bytes"],
      ["bytes written", profile.duckdb_bytes_written, "bytes"],
      ["peak buffer", profile.duckdb_peak_buffer_memory_bytes, "bytes"],
      ["peak temp", profile.duckdb_peak_temp_dir_bytes, "bytes"],
      ["operators", profile.duckdb_operator_count, ""],
    ]
      .filter(([, value]) => value !== undefined && value !== null && value !== "")
      .map(([label, value, unit]) => {
        const copy = unit === "bytes"
          ? formatByteCount(Number(value))
          : `${formatProgressValue(value)}${unit ? ` ${unit}` : ""}`;
        return `<span class="query-run-progress-pill"><strong>${escapeHtml(label)}</strong> ${escapeHtml(copy)}</span>`;
      });
    const topOperators = Array.isArray(profile.duckdb_top_operators)
      ? profile.duckdb_top_operators.slice(0, 4)
      : [];
    const operatorMarkup = topOperators.length
      ? `
          <div class="query-run-progress-operators">
            ${topOperators
              .map((operator) => {
                const timing = operator?.time_ms ?? operator?.cpu_ms;
                const rows = operator?.rows_scanned ?? operator?.cardinality;
                return `
                  <span class="query-run-progress-operator">
                    <strong>${escapeHtml(operator?.type || operator?.name || "operator")}</strong>
                    ${timing !== undefined && timing !== null ? `<span>${escapeHtml(formatProgressValue(timing))} ms</span>` : ""}
                    ${rows !== undefined && rows !== null ? `<span>${escapeHtml(Number(rows).toLocaleString())} rows</span>` : ""}
                  </span>
                `;
              })
              .join("")}
          </div>
        `
      : "";
    if (!summaryEntries.length && !operatorMarkup) {
      return "";
    }
    return `
      <div class="query-run-progress-duckdb">
        ${summaryEntries.join("")}
        ${operatorMarkup}
      </div>
    `;
  }

  function progressEventMarkup(event) {
    const eventName = String(event?.event || "progress").replace(/_/g, " ");
    const message = String(event?.message || event?.phase || "").trim();
    const percent = Number(event?.duckdb_progress_percent ?? event?.progress);
    const progressCopy = Number.isFinite(percent)
      ? event?.duckdb_progress_percent !== undefined
        ? `${percent.toFixed(1)}%`
        : `${Math.round(percent * 100)}%`
      : "";
    const metrics = [
      event?.elapsed_seconds !== undefined ? `${formatProgressValue(event.elapsed_seconds)}s` : "",
      event?.cpu_percent !== undefined ? `CPU ${formatProgressValue(event.cpu_percent)}%` : "",
      event?.ram_mb !== undefined ? `RAM ${formatProgressValue(event.ram_mb)} MB` : "",
      progressCopy ? `DuckDB ${progressCopy}` : "",
    ].filter(Boolean);
    return `
      <li class="query-run-progress-event">
        <time datetime="${escapeHtml(String(event?.occurredAt || ""))}">${escapeHtml(progressEventTime(event))}</time>
        <div>
          <strong>${escapeHtml(eventName)}</strong>
          ${message ? `<span>${escapeHtml(message)}</span>` : ""}
          ${metrics.length ? `<small>${metrics.map((item) => escapeHtml(item)).join(" | ")}</small>` : ""}
          ${duckdbProfilePillsMarkup(event?.duckdbProfile)}
        </div>
      </li>
    `;
  }

  function runProgressRowMarkup(run, columnCount) {
    const events = runProgressEvents(run);
    if (!events.length) {
      return "";
    }
    return `
      <tr class="query-run-history-progress-row">
        <td colspan="${escapeHtml(String(columnCount))}">
          ${runTimingBreakdownMarkup(run)}
          <ol class="query-run-progress-list">
            ${events.map((event) => progressEventMarkup(event)).join("")}
          </ol>
        </td>
      </tr>
    `;
  }

  function runSqlRowMarkup(run, columnCount) {
    const sql = String(run?.sql || "").trim();
    if (!sql) {
      return "";
    }
    return `
      <tr class="query-run-history-sql-row">
        <td colspan="${escapeHtml(String(columnCount))}">
          <pre>${escapeHtml(sql)}</pre>
        </td>
      </tr>
    `;
  }

  function runTableRowsMarkup(
    run,
    { root, index = 0, includeNotebook = false, showCharts = false, columnCount = 10 } = {}
  ) {
    const key = runKey(run, index);
    const sqlExpanded = expandedSqlRuns(root).has(key);
    const progressExpanded = expandedProgressRuns(root).has(key);
    const status = String(run?.status || "").trim().toLowerCase();
    const title = String(run?.notebookTitle || run?.notebookId || "Notebook").trim();
    const message = run?.error || run?.message || "";
    const metrics = runMetrics(run);
    const started = run?.startedAt || "";
    const ended = run?.completedAt || run?.updatedAt || "";
    const chartMarkup = showCharts ? runResourceChartMarkup(run) : "";
    const chartRow = showCharts
      ? `
          <tr class="query-run-history-chart-row">
            <td colspan="${escapeHtml(String(columnCount))}">
              ${chartMarkup || '<span class="query-run-history-empty-chart">No resource samples recorded for this run.</span>'}
            </td>
          </tr>
        `
      : "";
    const sqlRow = sqlExpanded ? runSqlRowMarkup(run, columnCount) : "";
    const progressRow = progressExpanded ? runProgressRowMarkup(run, columnCount) : "";
    const messageMarkup = message
      ? `<span class="query-run-history-message">${escapeHtml(message)}</span>`
      : "";
    const notebookCell = includeNotebook
      ? `
          <td class="query-run-history-title-cell">
            <strong>${escapeHtml(title)}</strong>
            <span>${escapeHtml(run?.cellId || "Cell")}</span>
            ${messageMarkup}
          </td>
        `
      : "";
    return `
      <tr class="query-run-history-row query-run-history-row-${escapeHtml(status || "unknown")}">
        ${notebookCell}
        <td>${runStatusMarkup(status)}</td>
        <td><time datetime="${escapeHtml(String(started))}">${escapeHtml(formatRunDateTime(started))}</time></td>
        <td><time datetime="${escapeHtml(String(ended))}">${escapeHtml(formatRunDateTime(ended))}</time></td>
        <td>
          <span>${escapeHtml(formatQueryDuration(Number(run?.durationMs || 0)))}</span>
          ${runTimingBreakdownMarkup(run)}
        </td>
        <td>${escapeHtml(formatMetric(metrics.averageCpuPercent, formatCpu))}</td>
        <td>${escapeHtml(formatMetric(metrics.peakCpuPercent, formatCpu))}</td>
        <td>${escapeHtml(formatMetric(metrics.averageMemoryRssBytes, formatByteCount))}</td>
        <td>${escapeHtml(formatMetric(metrics.peakMemoryRssBytes, formatByteCount))}</td>
        <td>${escapeHtml(formatRows(run))}</td>
        <td class="query-run-history-sql-cell">${runProgressToggleMarkup(run, key, progressExpanded)}</td>
        <td class="query-run-history-sql-cell">${runSqlToggleMarkup(run, key, sqlExpanded)}${includeNotebook ? "" : messageMarkup}</td>
      </tr>
      ${progressRow}
      ${sqlRow}
      ${chartRow}
    `;
  }

  function runTableMarkup(root, runs) {
    const includeNotebook = root?.matches?.("[data-query-runs-page]") ?? false;
    const showCharts = queryRunsShowCharts(root);
    const headers = [
      ...(includeNotebook ? ["Notebook / Cell"] : []),
      "Status",
      "Start date",
      "End date",
      "Duration",
      "CPU avg",
      "CPU peak",
      "RAM avg",
      "RAM peak",
      "Rows",
      "Progress",
      "SQL",
    ];
    return `
      <div class="query-run-history-table-wrap">
        <table class="query-run-history-table">
          <thead>
            <tr>
              ${headers.map((header) => `<th scope="col">${escapeHtml(header)}</th>`).join("")}
            </tr>
          </thead>
          <tbody>
            ${runs
              .map((run, index) =>
                runTableRowsMarkup(run, {
                  root,
                  index,
                  includeNotebook,
                  showCharts,
                  columnCount: headers.length,
                })
              )
              .join("")}
          </tbody>
        </table>
      </div>
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
    root._bdwQueryRunsPayload = payload;
    syncChartToggle(root);
    syncLiveToggle(root);
    if (statusRoot instanceof HTMLElement) {
      const liveOnly = queryRunsLiveOnly(root);
      statusRoot.textContent = payload?.available === false
        ? payload?.message || "Query-run history is not available."
        : runs.length
          ? liveOnly
            ? `${runs.length} live query run(s)`
            : `${runs.length} query run(s)`
          : emptyMessage(payload);
    }
    if (!runs.length) {
      listRoot.innerHTML = `<p class="home-empty">${escapeHtml(emptyMessage(payload))}</p>`;
      return;
    }
    listRoot.innerHTML = runTableMarkup(root, runs);
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
    if (queryRunsLiveOnly(root)) {
      params.set("liveOnly", "1");
    }
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
    const roots = [...(globalRoot ? [globalRoot] : []), ...notebookRoots(root)];
    await Promise.all(roots.map((node) => loadInto(node)));
    window.setTimeout(() => {
      roots.forEach((node) => {
        if (node?.isConnected && node.dataset.queryRunsLoaded !== "true") {
          loadInto(node, { quiet: true }).catch((error) => {
            console.error("Failed to initialize query-run history.", error);
          });
        }
      });
    }, 250);
  }

  async function handleClick(event) {
    const summary = event.target.closest(".workspace-query-runs-summary");
    if (summary) {
      const root = summary.closest("[data-notebook-query-runs]");
      if (root instanceof HTMLElement && root.dataset.queryRunsLoaded !== "true") {
        window.setTimeout(() => {
          loadInto(root, { quiet: true }).catch((error) => {
            console.error("Failed to initialize query-run history.", error);
          });
        }, 0);
      }
      return false;
    }

    const sqlToggle = event.target.closest("[data-query-run-sql-toggle]");
    if (sqlToggle) {
      event.preventDefault();
      event.stopPropagation();
      const root = sqlToggle.closest("[data-query-runs-page], [data-notebook-query-runs]");
      if (!(root instanceof HTMLElement)) {
        return true;
      }
      const key = String(sqlToggle.dataset.queryRunId || "").trim();
      if (key) {
        const expanded = expandedSqlRuns(root);
        if (expanded.has(key)) {
          expanded.delete(key);
        } else {
          expanded.add(key);
        }
      }
      if (root._bdwQueryRunsPayload) {
        renderList(root, root._bdwQueryRunsPayload);
      } else {
        await loadInto(root, { quiet: true });
      }
      return true;
    }

    const progressToggle = event.target.closest("[data-query-run-progress-toggle]");
    if (progressToggle) {
      event.preventDefault();
      event.stopPropagation();
      const root = progressToggle.closest("[data-query-runs-page], [data-notebook-query-runs]");
      if (!(root instanceof HTMLElement)) {
        return true;
      }
      const key = String(progressToggle.dataset.queryRunId || "").trim();
      if (key) {
        const expanded = expandedProgressRuns(root);
        if (expanded.has(key)) {
          expanded.delete(key);
        } else {
          expanded.add(key);
        }
      }
      if (root._bdwQueryRunsPayload) {
        renderList(root, root._bdwQueryRunsPayload);
      } else {
        await loadInto(root, { quiet: true });
      }
      return true;
    }

    const liveToggle = event.target.closest("[data-query-runs-toggle-live]");
    if (liveToggle) {
      event.preventDefault();
      event.stopPropagation();
      const root = liveToggle.closest("[data-query-runs-page], [data-notebook-query-runs]");
      if (!(root instanceof HTMLElement)) {
        return true;
      }
      root.dataset.queryRunsLiveOnly = queryRunsLiveOnly(root) ? "false" : "true";
      syncLiveToggle(root);
      await loadInto(root, { quiet: true });
      return true;
    }

    const toggleButton = event.target.closest("[data-query-runs-toggle-charts]");
    if (toggleButton) {
      event.preventDefault();
      event.stopPropagation();
      const root = toggleButton.closest("[data-query-runs-page], [data-notebook-query-runs]");
      if (!(root instanceof HTMLElement)) {
        return true;
      }
      root.dataset.queryRunsShowCharts = queryRunsShowCharts(root) ? "false" : "true";
      syncChartToggle(root);
      if (root._bdwQueryRunsPayload) {
        renderList(root, root._bdwQueryRunsPayload);
      } else {
        await loadInto(root, { quiet: true });
      }
      return true;
    }

    return false;
  }

  function refreshForQueryJobsSnapshot(snapshot) {
    const jobs = Array.isArray(snapshot?.jobs) ? snapshot.jobs : [];
    const terminalJobs = jobs.filter((job) => terminalStatuses.has(String(job?.status || "").trim().toLowerCase()));
    const liveJobs = jobs.filter((job) => !terminalStatuses.has(String(job?.status || "").trim().toLowerCase()));
    const roots = [pageRoot(), ...notebookRoots(document)].filter(Boolean);
    roots.forEach((root) => {
      if (terminalJobs.some((job) => rootMatchesJob(root, job))) {
        scheduleLoadInto(root);
      } else if (queryRunsLiveOnly(root) && liveJobs.some((job) => rootMatchesJob(root, job))) {
        scheduleLoadInto(root, { delayMs: 150, followUpDelayMs: 1500 });
      }
    });
  }

  return {
    handleClick,
    initializeCurrentPage,
    refreshForQueryJobsSnapshot,
  };
}

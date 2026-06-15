export function createQueryRunsController(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    formatByteCount,
    formatQueryDuration,
    queryResourceSparklineMarkup,
  } = helpers;

  const terminalStatuses = new Set(["completed", "failed", "cancelled", "canceled", "aborted", "incomplete", "warning", "warned"]);
  const refreshTimers = new WeakMap();
  let latestQueryJobsSnapshot = { jobs: [] };
  let latestPipelineStageSnapshot = { records: [], activeRuns: [] };
  let initialMonitorSnapshotsLoaded = false;

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
      case "canceled":
        return "Cancelled";
      case "aborted":
        return "Aborted";
      case "incomplete":
        return "Incomplete";
      case "warning":
      case "warned":
        return "Warning";
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
      cpuCapacityPercent: metrics.cpuCapacityPercent ?? run?.cpuCapacityPercent,
      averageCpuCapacityPercent: metrics.averageCpuCapacityPercent ?? run?.averageCpuCapacityPercent,
      peakCpuCapacityPercent: metrics.peakCpuCapacityPercent ?? run?.peakCpuCapacityPercent,
      cpuCapacityCores: metrics.cpuCapacityCores ?? run?.cpuCapacityCores,
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

  function runDateMs(value) {
    const parsed = Date.parse(value || "");
    return Number.isNaN(parsed) ? 0 : parsed;
  }

  function compareRunsByStartedAt(left, right) {
    return runDateMs(right?.startedAt || right?.updatedAt) - runDateMs(left?.startedAt || left?.updatedAt);
  }

  function runDurationMs(run) {
    const explicit = Number(run?.durationMs);
    if (Number.isFinite(explicit) && explicit >= 0) {
      return explicit;
    }
    const startedAtMs = runDateMs(run?.startedAt || run?.updatedAt);
    if (!startedAtMs) {
      return 0;
    }
    const completedAtMs = runDateMs(run?.completedAt || run?.updatedAt);
    const status = String(run?.status || "").trim().toLowerCase();
    if (completedAtMs && terminalStatuses.has(status)) {
      return Math.max(0, completedAtMs - startedAtMs);
    }
    return Math.max(0, Date.now() - startedAtMs);
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

  function runTimingClipboardTable(run) {
    const parts = normalizedRunTimings(run);
    if (!parts.length) {
      return "";
    }
    const displayLabel = (label) => {
      switch (label) {
        case "total":
          return "Total elapsed";
        case "file lock":
          return "File lock";
        case "source setup":
          return "Source setup";
        default:
          return `${label.charAt(0).toUpperCase()}${label.slice(1)}`;
      }
    };
    return [
      "Metric\tValue",
      ...parts.map(([label, value]) => `${displayLabel(label)}\t${formatQueryDuration(value)}`),
    ].join("\n");
  }

  function runTimingBreakdownMarkup(run) {
    const parts = normalizedRunTimings(run);
    if (!parts.length) {
      return "";
    }
    const timingTable = runTimingClipboardTable(run);
    return `
      <small
        class="query-run-history-timing"
        data-copy-query-timings
        data-query-timing-table="${escapeHtml(encodeURIComponent(timingTable))}"
        role="button"
        tabindex="0"
        title="Copy timing table"
      >
        ${parts
          .map(([label, value]) => `<span>${escapeHtml(label)} ${escapeHtml(formatQueryDuration(value))}</span>`)
          .join("")}
      </small>
    `;
  }

  function runWarningsMarkup(run) {
    const warnings = Array.isArray(run?.warnings)
      ? run.warnings.map((warning) => String(warning ?? "").trim()).filter(Boolean)
      : [];
    if (!warnings.length) {
      return "";
    }
    return `<span class="query-run-history-message query-run-history-warning">Warnings: ${escapeHtml(warnings.join(" | "))}</span>`;
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

  function notebookLinkMarkup(run, title) {
    const notebookId = String(run?.notebookId || "").trim();
    if (!notebookId) {
      return `<strong>${escapeHtml(title)}</strong>`;
    }
    const cellId = String(run?.cellId || "").trim();
    return `
      <a
        href="/notebooks/${encodeURIComponent(notebookId)}"
        class="query-run-history-notebook-link"
        data-open-query-notebook="${escapeHtml(notebookId)}"
        data-open-query-cell="${escapeHtml(cellId)}"
      >${escapeHtml(title)}</a>
    `;
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

  function progressEventOccurrenceCount(event) {
    const count = Number(event?.occurrenceCount);
    return Number.isFinite(count) && count > 1 ? Math.round(count) : 1;
  }

  function progressEventTotalCount(events) {
    return events.reduce((total, event) => total + progressEventOccurrenceCount(event), 0);
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
        <span>${escapeHtml(progressEventTotalCount(events).toLocaleString())} event(s)</span>
      </button>
    `;
  }

  function progressEventTime(event) {
    if (progressEventOccurrenceCount(event) > 1) {
      const firstTime = String(event?.firstDisplayTime || event?.displayTime || "").trim();
      const lastTime = String(event?.lastDisplayTime || "").trim();
      if (firstTime && lastTime && firstTime !== lastTime) {
        return `${firstTime} - ${lastTime}`;
      }
    }
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

  function progressCpuMetric(event) {
    const capacity = Number(event?.cpu_capacity_percent);
    const raw = Number(event?.cpu_percent);
    if (Number.isFinite(capacity)) {
      const rawCopy = Number.isFinite(raw) && Math.abs(raw - capacity) >= 0.05
        ? ` (${formatProgressValue(raw)}% core)`
        : "";
      return `CPU ${formatProgressValue(capacity)}% capacity${rawCopy}`;
    }
    if (Number.isFinite(raw)) {
      return `CPU ${formatProgressValue(raw)}% core`;
    }
    return "";
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
    const occurrenceCount = progressEventOccurrenceCount(event);
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
      progressCpuMetric(event),
      event?.ram_mb !== undefined ? `RAM ${formatProgressValue(event.ram_mb)} MB` : "",
      progressCopy ? `DuckDB ${progressCopy}` : "",
    ].filter(Boolean);
    return `
      <li class="query-run-progress-event">
        <time datetime="${escapeHtml(String(event?.occurredAt || ""))}">${escapeHtml(progressEventTime(event))}</time>
        <div>
          <strong>${escapeHtml(occurrenceCount > 1 ? `${eventName} x${occurrenceCount.toLocaleString()}` : eventName)}</strong>
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
    const warningsMarkup = runWarningsMarkup(run);
    const notebookCell = includeNotebook
      ? `
          <td class="query-run-history-title-cell">
            ${notebookLinkMarkup(run, title)}
            <span>${escapeHtml(run?.cellId || "Cell")}</span>
            ${messageMarkup}
            ${warningsMarkup}
          </td>
        `
      : "";
    return `
      <tr class="query-run-history-row query-run-history-row-${escapeHtml(status || "unknown")}">
        ${notebookCell}
        <td>${runStatusMarkup(status)}${includeNotebook ? "" : `${messageMarkup}${warningsMarkup}`}</td>
        <td><time datetime="${escapeHtml(String(started))}">${escapeHtml(formatRunDateTime(started))}</time></td>
        <td><time datetime="${escapeHtml(String(ended))}">${escapeHtml(formatRunDateTime(ended))}</time></td>
        <td>
          <span>${escapeHtml(formatQueryDuration(runDurationMs(run)))}</span>
          ${runTimingBreakdownMarkup(run)}
        </td>
        <td>${escapeHtml(formatMetric(metrics.averageCpuCapacityPercent ?? metrics.averageCpuPercent, formatCpu))}</td>
        <td>${escapeHtml(formatMetric(metrics.peakCpuCapacityPercent ?? metrics.peakCpuPercent, formatCpu))}</td>
        <td>${escapeHtml(formatMetric(metrics.averageMemoryRssBytes, formatByteCount))}</td>
        <td>${escapeHtml(formatMetric(metrics.peakMemoryRssBytes, formatByteCount))}</td>
        <td>${escapeHtml(formatRows(run))}</td>
        <td class="query-run-history-sql-cell">${runProgressToggleMarkup(run, key, progressExpanded)}</td>
        <td class="query-run-history-sql-cell">${runSqlToggleMarkup(run, key, sqlExpanded)}</td>
      </tr>
      ${chartRow}
      ${progressRow}
      ${sqlRow}
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

  function liveRunFromQueryJob(job) {
    const status = String(job?.status || "queued").trim().toLowerCase() || "queued";
    const startedAt = String(job?.startedAt || job?.updatedAt || new Date().toISOString()).trim();
    const completedAt = String(job?.completedAt || (terminalStatuses.has(status) ? job?.updatedAt || "" : "") || "");
    return {
      ...job,
      status,
      startedAt,
      completedAt,
      updatedAt: String(job?.updatedAt || startedAt).trim(),
      durationMs: runDurationMs({ ...job, status, startedAt, completedAt }),
      rowCount: job?.rowCount ?? job?.rowsShown ?? 0,
      rowsShown: job?.rowsShown ?? (Array.isArray(job?.rows) ? job.rows.length : 0),
      progressEvents: Array.isArray(job?.progressEvents) ? job.progressEvents : [],
      warnings: Array.isArray(job?.warnings) ? job.warnings : [],
    };
  }

  function pipelineStageRecordStatus(record) {
    const status = String(record?.status || "").trim().toLowerCase();
    if (status === "skipped") {
      return "warning";
    }
    return status || "running";
  }

  function pipelineStageRecordUpdatedMs(record) {
    return Math.max(
      runDateMs(record?.updatedAt),
      runDateMs(record?.completedAt),
      runDateMs(record?.startedAt)
    );
  }

  function latestPipelineStageRecords() {
    const records = Array.isArray(latestPipelineStageSnapshot?.records)
      ? latestPipelineStageSnapshot.records
      : [];
    const latestByStageRun = new Map();
    records.forEach((record) => {
      const runId = String(record?.runId || "").trim();
      const stageId = String(record?.stageId || "").trim();
      const cellId = String(record?.cellId || "").trim();
      const notebookId = String(record?.notebookId || "").trim();
      if (!runId || !stageId || !cellId || !notebookId) {
        return;
      }
      const key = `${runId}:${stageId}`;
      const existing = latestByStageRun.get(key);
      if (!existing || pipelineStageRecordUpdatedMs(record) >= pipelineStageRecordUpdatedMs(existing)) {
        latestByStageRun.set(key, record);
      }
    });
    return Array.from(latestByStageRun.values());
  }

  function activeRunForPipelineRecord(record) {
    const runId = String(record?.runId || "").trim();
    if (!runId) {
      return null;
    }
    return (Array.isArray(latestPipelineStageSnapshot?.activeRuns) ? latestPipelineStageSnapshot.activeRuns : [])
      .find((run) => String(run?.runId || "").trim() === runId) || null;
  }

  function pipelineStageProgressEvents(record) {
    const status = pipelineStageRecordStatus(record);
    const stageTitle = String(record?.stageTitle || record?.stageAlias || record?.stageId || "stage").trim();
    const eventName = terminalStatuses.has(status) || status === "warning"
      ? `pipeline_stage_${status}`
      : "pipeline_stage_running";
    const message = String(record?.error || record?.message || `Materializing ${stageTitle}`).trim();
    return [
      {
        event: eventName,
        message,
        occurredAt: record?.updatedAt || record?.startedAt || new Date().toISOString(),
        displayTime: "",
      },
    ];
  }

  function liveRunFromPipelineStageRecord(record) {
    const status = pipelineStageRecordStatus(record);
    const activeRun = activeRunForPipelineRecord(record);
    const stageTitle = String(record?.stageTitle || record?.stageAlias || record?.stageId || "Pipeline stage").trim();
    const notebookTitle = String(activeRun?.notebookTitle || stageTitle || record?.notebookId || "Data pipeline").trim();
    const startedAt = String(record?.startedAt || record?.updatedAt || new Date().toISOString()).trim();
    const completedAt = String(
      record?.completedAt || (terminalStatuses.has(status) || status === "warning" ? record?.updatedAt || "" : "") || ""
    );
    const durationMs = Number(record?.durationMs);
    const normalizedDurationMs = Number.isFinite(durationMs) && durationMs >= 0
      ? durationMs
      : runDurationMs({ status, startedAt, completedAt });
    const message = String(
      record?.error ||
      record?.message ||
      (status === "running" ? `Materializing stage ${stageTitle}.` : `Pipeline stage ${stageTitle}.`)
    ).trim();
    return {
      jobId: `pipeline-stage:${record?.runId || ""}:${record?.stageId || ""}`,
      queryJobId: String(record?.queryJobId || "").trim(),
      notebookId: String(record?.notebookId || "").trim(),
      notebookTitle,
      cellId: String(record?.cellId || "").trim(),
      status,
      startedAt,
      completedAt,
      updatedAt: String(record?.updatedAt || completedAt || startedAt).trim(),
      durationMs: normalizedDurationMs,
      rowCount: Number(record?.rowCount || 0),
      rowsShown: 0,
      sql: String(record?.querySql || record?.queryReference || record?.outputPath || stageTitle).trim(),
      message,
      error: String(record?.error || "").trim(),
      progressEvents: pipelineStageProgressEvents(record),
      warnings: status === "warning" && message ? [message] : [],
      timings: Number.isFinite(normalizedDurationMs) && normalizedDurationMs >= 0
        ? { backendTotalMs: normalizedDurationMs }
        : {},
      source: "pipeline-stage",
    };
  }

  function pipelineStageRunsForRoot(root) {
    const liveOnly = queryRunsLiveOnly(root);
    return latestPipelineStageRecords()
      .map((record) => liveRunFromPipelineStageRecord(record))
      .filter((run) => rootMatchesJob(root, run))
      .filter((run) => !liveOnly || !terminalStatuses.has(String(run?.status || "").trim().toLowerCase()));
  }

  function liveRunsForRoot(root) {
    const jobs = Array.isArray(latestQueryJobsSnapshot?.jobs) ? latestQueryJobsSnapshot.jobs : [];
    const liveOnly = queryRunsLiveOnly(root);
    return jobs
      .filter((job) => rootMatchesJob(root, job))
      .filter((job) => !liveOnly || !terminalStatuses.has(String(job?.status || "").trim().toLowerCase()))
      .map((job) => liveRunFromQueryJob(job));
  }

  function queryRunsLimit(root) {
    const value = Number(root?.dataset?.queryRunsLimit || 100);
    return Number.isFinite(value) && value > 0 ? Math.round(value) : 100;
  }

  function runsForRender(root, payload) {
    const recordedRuns = Array.isArray(payload?.runs) ? payload.runs : [];
    const liveRuns = liveRunsForRoot(root);
    const realQueryJobIds = new Set(
      [...liveRuns, ...recordedRuns]
        .map((run) => String(run?.jobId || "").trim())
        .filter(Boolean)
    );
    const pipelineStageRuns = pipelineStageRunsForRoot(root).filter((run) => {
      const queryJobId = String(run?.queryJobId || "").trim();
      return !queryJobId || !realQueryJobIds.has(queryJobId);
    });
    if (!liveRuns.length && !pipelineStageRuns.length) {
      return recordedRuns;
    }
    const liveAndPipelineRuns = [...liveRuns, ...pipelineStageRuns];
    const liveIds = new Set(liveAndPipelineRuns.map((run) => String(run?.jobId || "").trim()).filter(Boolean));
    return [
      ...liveAndPipelineRuns,
      ...recordedRuns.filter((run) => {
        const jobId = String(run?.jobId || "").trim();
        return !jobId || !liveIds.has(jobId);
      }),
    ]
      .sort(compareRunsByStartedAt)
      .slice(0, queryRunsLimit(root));
  }

  function renderList(root, payload) {
    const listRoot = root.querySelector("[data-query-runs-list]");
    const statusRoot = root.querySelector("[data-query-runs-status]");
    if (!(listRoot instanceof HTMLElement)) {
      return;
    }
    root._bdwQueryRunsPayload = payload;
    const runs = runsForRender(root, payload);
    syncChartToggle(root);
    syncLiveToggle(root);
    if (statusRoot instanceof HTMLElement) {
      const liveOnly = queryRunsLiveOnly(root);
      statusRoot.textContent = payload?.available === false
        ? payload?.message || "Query-run history is not available."
        : runs.length
          ? liveOnly
            ? `${runs.length} live query run(s)`
            : `${runs.length} monitored run(s)`
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

  async function loadInitialMonitorSnapshots() {
    const [queryJobsSnapshot, materializedStagesSnapshot] = await Promise.all([
      fetchJsonOrThrow("/api/query-jobs").catch(() => null),
      fetchJsonOrThrow("/api/materialized-stages/state").catch(() => null),
    ]);
    if (queryJobsSnapshot && typeof queryJobsSnapshot === "object") {
      latestQueryJobsSnapshot = queryJobsSnapshot;
    }
    if (materializedStagesSnapshot && typeof materializedStagesSnapshot === "object") {
      latestPipelineStageSnapshot = materializedStagesSnapshot;
    }
    initialMonitorSnapshotsLoaded = true;
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
    await loadInitialMonitorSnapshots();
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
    initialMonitorSnapshotsLoaded = true;
    latestQueryJobsSnapshot = snapshot && typeof snapshot === "object" ? snapshot : { jobs: [] };
    const jobs = Array.isArray(snapshot?.jobs) ? snapshot.jobs : [];
    const terminalJobs = jobs.filter((job) => terminalStatuses.has(String(job?.status || "").trim().toLowerCase()));
    const liveJobs = jobs.filter((job) => !terminalStatuses.has(String(job?.status || "").trim().toLowerCase()));
    const roots = [pageRoot(), ...notebookRoots(document)].filter(Boolean);
    roots.forEach((root) => {
      if (jobs.some((job) => rootMatchesJob(root, job))) {
        renderList(root, root._bdwQueryRunsPayload || { available: true, runs: [] });
      }
      if (terminalJobs.some((job) => rootMatchesJob(root, job))) {
        scheduleLoadInto(root);
      } else if (queryRunsLiveOnly(root) && liveJobs.some((job) => rootMatchesJob(root, job))) {
        scheduleLoadInto(root, { delayMs: 150, followUpDelayMs: 1500 });
      }
    });
  }

  function refreshForMaterializedStagesSnapshot(snapshot) {
    latestPipelineStageSnapshot = snapshot && typeof snapshot === "object" ? snapshot : { records: [], activeRuns: [] };
    if (!initialMonitorSnapshotsLoaded) {
      return;
    }
    const stageRuns = latestPipelineStageRecords().map((record) => liveRunFromPipelineStageRecord(record));
    if (!stageRuns.length) {
      return;
    }
    const roots = [pageRoot(), ...notebookRoots(document)].filter(Boolean);
    roots.forEach((root) => {
      if (stageRuns.some((run) => rootMatchesJob(root, run))) {
        renderList(root, root._bdwQueryRunsPayload || { available: true, runs: [] });
      }
    });
  }

  return {
    handleClick,
    initializeCurrentPage,
    refreshForMaterializedStagesSnapshot,
    refreshForQueryJobsSnapshot,
  };
}

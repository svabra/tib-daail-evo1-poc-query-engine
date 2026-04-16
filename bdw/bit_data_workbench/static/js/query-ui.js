export function createQueryUi(helpers) {
  const {
    escapeHtml,
    formatQueryDuration,
    formatQueryTimestamp,
    queryJobElapsedMs,
    queryJobEventDateTimeCopy,
    queryJobIsRunning,
    queryJobStatusCopy,
  } = helpers;

  function emptyQueryResultsMarkup(cellId) {
    return `
      <section id="query-results-${escapeHtml(cellId)}" class="result-panel" data-cell-result data-query-job-id="" hidden>
        <header class="result-header">
          <div class="result-header-copy">
            <h3>Result</h3>
            <div class="result-meta-row">
              <p class="result-meta">0 ms</p>
            </div>
          </div>
          <div class="result-header-actions">
            <span class="result-badge">Run this cell to inspect the selected data sources.</span>
            ${resultExportMenuMarkup(false, "")}
          </div>
        </header>
        <div class="result-empty">
          <p>Run this cell to inspect the selected data sources.</p>
        </div>
      </section>
    `;
  }

  function queryInsightPillMarkup(insight, { compact = false } = {}) {
    if (!insight?.value) {
      return "";
    }

    const toneClass = insight.tone ? ` is-${escapeHtml(insight.tone)}` : "";
    const compactClass = compact ? " query-insight-pill-compact" : "";
    const titleAttribute = insight.title ? ` title="${escapeHtml(insight.title)}"` : "";
    return `
      <span class="query-insight-pill${toneClass}${compactClass}"${titleAttribute}>
        <strong>${escapeHtml(insight.label || "")}</strong>
        <span>${escapeHtml(insight.value)}</span>
      </span>
    `;
  }

  function queryRowsShownLabel(job) {
    if (!job) {
      return "Run this cell to inspect the selected data sources.";
    }

    if (job.rowsShown > 0) {
      if (job.truncated) {
        return `${job.rowsShown} row(s) shown. The result was truncated for the UI.`;
      }
      return `${job.rowsShown} row(s) shown.`;
    }

    if (queryJobIsRunning(job)) {
      return "Waiting for the first rows...";
    }

    return job.message || "Statement executed successfully.";
  }

  function queryProgressActivityCopy(job) {
    if (!job || !queryJobIsRunning(job)) {
      return "Query activity is idle.";
    }

    if (job.status === "queued") {
      return "Waiting for the query worker to start this statement.";
    }

    if (Number(job.rowsShown || 0) > 0) {
      return `${job.rowsShown} row(s) are already available in the live preview.`;
    }

    const progressLabel = String(job.progressLabel || "").toLowerCase();
    const message = String(job.message || "").toLowerCase();
    const combined = `${progressLabel} ${message}`;

    if (combined.includes("fetch")) {
      return "Fetching the first rows for the live preview.";
    }

    if (combined.includes("finaliz")) {
      return "Finalizing the statement result.";
    }

    return "Completion percent is not available for this query yet.";
  }

  function queryProgressMarkup(job) {
    if (!queryJobIsRunning(job)) {
      return "";
    }

    const progressValue =
      typeof job.progress === "number" && Number.isFinite(job.progress)
        ? Math.max(0, Math.min(100, job.progress * 100))
        : null;
    const backendCopy = escapeHtml(job.backendName || "VMTP DUCKDB");
    const progressLabel = escapeHtml(job.progressLabel || "Running...");

    if (progressValue === null) {
      return `
        <div class="query-progress-card query-progress-card-indeterminate">
          <div class="query-progress-copy">
            <strong>${progressLabel}</strong>
            <span>${backendCopy}</span>
          </div>
          <div class="query-progress-status">
            <span class="query-progress-status-dot" aria-hidden="true"></span>
            <span>${escapeHtml(queryProgressActivityCopy(job))}</span>
          </div>
        </div>
      `;
    }

    return `
      <div class="query-progress-card">
        <div class="query-progress-copy">
          <strong>${progressLabel}</strong>
          <span>${backendCopy} | ${Math.round(progressValue)}%</span>
        </div>
        <div class="query-progress-track">
          <span style="width:${progressValue}%;"></span>
        </div>
      </div>
    `;
  }

  function queryResultTableMarkup(job) {
    if (!job?.columns?.length) {
      return "";
    }

    return `
      <div class="result-table-wrap">
        <table class="result-table">
          <thead>
            <tr>
              ${job.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}
            </tr>
          </thead>
          <tbody>
            ${job.rows
              .map(
                (row) => `
                  <tr>
                    ${row
                      .map((value) =>
                        value === null
                          ? '<td><span class="cell-null">NULL</span></td>'
                          : `<td>${escapeHtml(value)}</td>`
                      )
                      .join("")}
                  </tr>
                `
              )
              .join("")}
          </tbody>
        </table>
      </div>
    `;
  }

  function resultExportMenuMarkup(showActions, jobId = "") {
    const normalizedJobId = String(jobId || "").trim();
    const sharedWorkspaceTooltip =
      "Saves into the configured Shared Workspace MinIO / S3 bucket.";
    const localWorkspaceTooltip =
      "Saves in this browser's Local Workspace (IndexDB).";
    return `
      <details
        class="workspace-action-menu result-action-menu"
        data-result-action-menu
        data-result-job-id="${escapeHtml(normalizedJobId)}"
        ${showActions ? "" : "hidden"}
      >
        <summary
          class="workspace-action-menu-toggle result-action-menu-toggle"
          aria-label="Export or save query results"
          title="Export or save query results"
        >
          <span class="result-action-menu-label">Export / Save</span>
        </summary>
        <div class="workspace-action-menu-panel result-action-menu-panel">
          <button
            type="button"
            class="workspace-action-menu-item"
            data-result-export-local=""
            data-result-job-id="${escapeHtml(normalizedJobId)}"
            title="${escapeHtml(localWorkspaceTooltip)}"
          >Save Results in Local Workspace (IndexDB) ...</button>
          <button
            type="button"
            class="workspace-action-menu-item"
            data-result-export-s3=""
            data-result-job-id="${escapeHtml(normalizedJobId)}"
            title="${escapeHtml(sharedWorkspaceTooltip)}"
          >Save Results in Shared Workspace (S3) ...</button>
          <div class="workspace-action-menu-separator"></div>
          <button
            type="button"
            class="workspace-action-menu-item"
            data-result-export-download=""
            data-result-job-id="${escapeHtml(normalizedJobId)}"
          >Download Results as ...</button>
        </div>
      </details>
    `;
  }

  function resultMetricStripMarkup(job) {
    if (!job) {
      return "";
    }

    const metricPills = [];
    if (job.comparisonInsights?.previous) {
      metricPills.push(queryInsightPillMarkup(job.comparisonInsights.previous));
    }
    if (job.comparisonInsights?.median) {
      metricPills.push(queryInsightPillMarkup(job.comparisonInsights.median));
    }
    if (job.timingInsights) {
      metricPills.push(queryInsightPillMarkup(job.timingInsights));
    }
    if (job.footprintInsights) {
      metricPills.push(queryInsightPillMarkup(job.footprintInsights));
    }

    if (!metricPills.length) {
      return "";
    }

    return `<div class="result-metric-strip">${metricPills.join("")}</div>`;
  }

  function queryMonitorInsightStripMarkup(job) {
    if (!job) {
      return "";
    }

    const metricPills = [];
    const comparisonInsight = job.comparisonInsights?.previous || job.comparisonInsights?.median || null;
    if (comparisonInsight) {
      metricPills.push(queryInsightPillMarkup(comparisonInsight, { compact: true }));
    }
    if (job.footprintInsights) {
      metricPills.push(queryInsightPillMarkup(job.footprintInsights, { compact: true }));
    }

    if (!metricPills.length) {
      return "";
    }

    return `<div class="query-monitor-item-insights">${metricPills.join("")}</div>`;
  }

  function queryResultPanelMarkup(cellId, job = null) {
    if (!job) {
      return emptyQueryResultsMarkup(cellId);
    }

    const showExportActions = job.status === "completed" && job.columns.length > 0;
    const rowsBadge = queryRowsShownLabel(job);
    const showRowsBadge = queryJobIsRunning(job) || Number(job.rowsShown || 0) > 0 || Boolean(job.truncated);
    const resultBody = job.error
      ? `
          <div class="result-error">
            <strong>${escapeHtml(job.status === "cancelled" ? "Query cancelled." : "Query failed.")}</strong>
            <pre>${escapeHtml(job.error)}</pre>
          </div>
        `
      : job.columns.length
        ? `
            ${queryProgressMarkup(job)}
            ${queryResultTableMarkup(job)}
          `
        : queryJobIsRunning(job)
          ? `
              ${queryProgressMarkup(job)}
              <div class="result-empty result-empty-running">
                <p>${escapeHtml(job.message || "Running query...")}</p>
              </div>
            `
          : `
              <div class="result-empty">
                <p>${escapeHtml(job.message || "Statement executed successfully.")}</p>
              </div>
            `;

    return `
      <section
        id="query-results-${escapeHtml(cellId)}"
        class="result-panel"
        data-cell-result
        data-query-job-id="${escapeHtml(job.jobId || "")}" 
      >
        <header class="result-header">
          <div class="result-header-copy">
            <h3>Result</h3>
            <div class="result-meta-row">
              <p class="result-meta" data-query-duration data-job-id="${escapeHtml(job.jobId || "")}">${escapeHtml(formatQueryDuration(queryJobElapsedMs(job)))}</p>
              ${resultMetricStripMarkup(job)}
            </div>
          </div>
          <div class="result-header-actions">
            <span class="result-badge${queryJobIsRunning(job) ? " is-live" : ""}" ${showRowsBadge ? "" : "hidden"}>${escapeHtml(rowsBadge)}</span>
            ${resultExportMenuMarkup(showExportActions, job.jobId || "")}
          </div>
        </header>
        ${resultBody}
      </section>
    `;
  }

  function renderPerformanceChartMarkup(performance) {
    const points = Array.isArray(performance?.recent) ? performance.recent : [];
    if (!points.length) {
      return "";
    }

    const width = 280;
    const height = 92;
    const paddingX = 10;
    const paddingY = 10;
    const values = points.map((point) => Math.max(1, Number(point.durationMs || 0)));
    const transformedValues = values.map((value) => Math.log10(value + 1));
    const minValue = Math.min(...transformedValues);
    const maxValue = Math.max(...transformedValues);
    const spread = Math.max(maxValue - minValue, 0.0001);
    const stepX = points.length > 1 ? (width - paddingX * 2) / (points.length - 1) : 0;

    const yForValue = (durationMs) => {
      const transformed = Math.log10(Math.max(1, Number(durationMs || 0)) + 1);
      const ratio = (transformed - minValue) / spread;
      return height - paddingY - ratio * (height - paddingY * 2);
    };

    const polyline = points
      .map((point, index) => `${paddingX + index * stepX},${yForValue(point.durationMs).toFixed(2)}`)
      .join(" ");
    const p50Y =
      typeof performance?.stats?.p50Ms === "number" ? yForValue(performance.stats.p50Ms).toFixed(2) : null;
    const p95Y =
      typeof performance?.stats?.p95Ms === "number" ? yForValue(performance.stats.p95Ms).toFixed(2) : null;

    return `
      <svg viewBox="0 0 ${width} ${height}" class="query-monitor-chart-svg" preserveAspectRatio="none" aria-hidden="true">
        ${p95Y ? `<line x1="${paddingX}" y1="${p95Y}" x2="${width - paddingX}" y2="${p95Y}" class="query-monitor-chart-line query-monitor-chart-line-p95"></line>` : ""}
        ${p50Y ? `<line x1="${paddingX}" y1="${p50Y}" x2="${width - paddingX}" y2="${p50Y}" class="query-monitor-chart-line query-monitor-chart-line-p50"></line>` : ""}
        <polyline points="${polyline}" class="query-monitor-chart-path"></polyline>
        ${points
          .map((point, index) => {
            const x = paddingX + index * stepX;
            const y = yForValue(point.durationMs);
            return `
              <circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="2.4" class="query-monitor-chart-point query-monitor-chart-point-${escapeHtml(point.status)}">
                <title>${escapeHtml(point.notebookTitle)} | ${escapeHtml(formatQueryDuration(point.durationMs))}</title>
              </circle>
            `;
          })
          .join("")}
      </svg>
    `;
  }

  function renderPerformanceDistributionMarkup(performance) {
    const points = Array.isArray(performance?.recent) ? performance.recent : [];
    if (!points.length) {
      return "";
    }

    const values = points
      .map((point) => Math.max(1, Number(point.durationMs || 0)))
      .filter((value) => Number.isFinite(value));
    if (!values.length) {
      return "";
    }

    const width = 280;
    const height = 86;
    const paddingX = 8;
    const paddingTop = 6;
    const paddingBottom = 18;
    const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
    const variance =
      values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / Math.max(1, values.length - 1);
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const fallbackSpread = Math.max((maxValue - minValue) / 6, mean * 0.18, 1);
    const standardDeviation = Math.max(Math.sqrt(variance), fallbackSpread);
    const domainStart = Math.max(0, Math.min(minValue, mean - standardDeviation * 3));
    const domainEnd = Math.max(domainStart + 1, Math.max(maxValue, mean + standardDeviation * 3));
    const plotWidth = width - paddingX * 2;
    const plotHeight = height - paddingTop - paddingBottom;
    const sampleCount = 48;
    const gaussianPoints = [];

    let peakDensity = 0;
    for (let index = 0; index < sampleCount; index += 1) {
      const ratio = sampleCount === 1 ? 0 : index / (sampleCount - 1);
      const xValue = domainStart + ratio * (domainEnd - domainStart);
      const density = Math.exp(-0.5 * ((xValue - mean) / standardDeviation) ** 2);
      peakDensity = Math.max(peakDensity, density);
      gaussianPoints.push({ ratio, xValue, density });
    }

    const pathPoints = gaussianPoints.map(({ ratio, density }) => {
      const x = paddingX + ratio * plotWidth;
      const y = paddingTop + (1 - density / Math.max(peakDensity, 0.0001)) * plotHeight;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    });
    const areaPath = [
      `M ${paddingX} ${height - paddingBottom}`,
      ...gaussianPoints.map(({ ratio, density }) => {
        const x = paddingX + ratio * plotWidth;
        const y = paddingTop + (1 - density / Math.max(peakDensity, 0.0001)) * plotHeight;
        return `L ${x.toFixed(2)} ${y.toFixed(2)}`;
      }),
      `L ${width - paddingX} ${height - paddingBottom}`,
      "Z",
    ].join(" ");

    const markerForValue = (value, className, label) => {
      if (typeof value !== "number" || !Number.isFinite(value)) {
        return "";
      }
      const ratio = Math.max(0, Math.min(1, (value - domainStart) / (domainEnd - domainStart)));
      const x = paddingX + ratio * plotWidth;
      return `
        <line x1="${x.toFixed(2)}" y1="${paddingTop}" x2="${x.toFixed(2)}" y2="${(height - paddingBottom).toFixed(
          2
        )}" class="query-monitor-distribution-marker ${className}"></line>
        <title>${escapeHtml(`${label}: ${formatQueryDuration(value)}`)}</title>
      `;
    };

    const tickEntries = [
      ["Min", minValue],
      ["Mean", mean],
      ["Max", maxValue],
    ];

    return `
      <div class="query-monitor-distribution-header">
        <h4>Runtime Distribution</h4>
        <p>Bell curve of recent successful query runtimes.</p>
      </div>
      <svg viewBox="0 0 ${width} ${height}" class="query-monitor-distribution-svg" preserveAspectRatio="none" aria-hidden="true">
        <path d="${areaPath}" class="query-monitor-distribution-area"></path>
        <polyline points="${pathPoints.join(" ")}" class="query-monitor-distribution-curve"></polyline>
        ${markerForValue(mean, "is-mean", "Mean")}
        ${markerForValue(performance?.stats?.p50Ms, "is-p50", "p50")}
        ${markerForValue(performance?.stats?.p95Ms, "is-p95", "p95")}
      </svg>
      <div class="query-monitor-distribution-ticks">
        ${tickEntries
          .map(
            ([label, value]) => `
              <span class="query-monitor-distribution-tick">
                <strong>${escapeHtml(label)}</strong>
                <span>${escapeHtml(formatQueryDuration(value))}</span>
              </span>
            `
          )
          .join("")}
      </div>
    `;
  }

  function queryPerformanceStatsMarkup(performance) {
    const stats = performance?.stats ?? {};
    const statEntries = [
      [
        "Latest",
        stats.latestMs,
        "Runtime of the most recently completed query.",
      ],
      [
        "p50",
        stats.p50Ms,
        "Median runtime. 50% of recent completed queries finished at or below this duration.",
      ],
      [
        "p95",
        stats.p95Ms,
        "Tail runtime. 95% of recent completed queries finished at or below this duration. The slowest 5% took longer.",
      ],
    ].filter((entry) => typeof entry[1] === "number");

    return statEntries
      .map(
        ([label, value, tooltip]) => `
          <span class="query-monitor-stat-pill" title="${escapeHtml(tooltip)}" aria-label="${escapeHtml(
            `${label}: ${tooltip}`
          )}">
            <strong>${escapeHtml(label)}</strong>
            <span>${escapeHtml(formatQueryDuration(value))}</span>
          </span>
        `
      )
      .join("");
  }

  function queryMonitorItemMarkup(job) {
    const running = queryJobIsRunning(job);
    const rowsCopy = job.rowsShown > 0 ? `${job.rowsShown} row(s)` : "No rows yet";
    const timestamp = job.startedAt || job.updatedAt;
    return `
      <article class="query-monitor-item query-monitor-item-${escapeHtml(job.status)}" data-query-job-id="${escapeHtml(job.jobId)}">
        <div class="query-monitor-item-copy">
          <button
            type="button"
            class="query-monitor-open"
            data-open-query-notebook="${escapeHtml(job.notebookId)}"
            data-open-query-cell="${escapeHtml(job.cellId)}"
            title="Open ${escapeHtml(job.notebookTitle)}"
          >
            ${escapeHtml(job.notebookTitle)}
          </button>
          <div class="query-monitor-item-meta">
            <span class="query-monitor-status-badge${running ? " is-live" : ""}">${escapeHtml(queryJobStatusCopy(job))}</span>
            <span data-query-monitor-duration data-job-id="${escapeHtml(job.jobId)}">${escapeHtml(formatQueryDuration(queryJobElapsedMs(job)))}</span>
            <span>${escapeHtml(rowsCopy)}</span>
          </div>
          ${queryMonitorInsightStripMarkup(job)}
          <p class="query-monitor-sql">${escapeHtml(job.sql)}</p>
        </div>
        <div class="query-monitor-item-actions">
          ${running ? `<button type="button" class="query-monitor-cancel" data-cancel-query-job="${escapeHtml(job.jobId)}">Cancel</button>` : ""}
          <span class="query-monitor-updated">${escapeHtml(formatQueryTimestamp(timestamp))}</span>
        </div>
      </article>
    `;
  }

  function queryNotificationItemMarkup(job) {
    const rowsLabel = queryRowsShownLabel(job);
    return `
      <button
        type="button"
        class="topbar-notification-item"
        data-open-query-notebook="${escapeHtml(job.notebookId)}"
        data-open-query-cell="${escapeHtml(job.cellId)}"
        title="Open ${escapeHtml(job.notebookTitle)}"
      >
        <span class="topbar-notification-item-status${queryJobIsRunning(job) ? " is-live" : ""}">${escapeHtml(queryJobStatusCopy(job))}</span>
        <span class="topbar-notification-item-title">${escapeHtml(job.notebookTitle)}</span>
        <span class="topbar-notification-item-copy" data-query-notification-copy data-job-id="${escapeHtml(job.jobId)}" data-query-copy-suffix="${escapeHtml(rowsLabel)}">${escapeHtml(formatQueryDuration(queryJobElapsedMs(job)))} | ${escapeHtml(rowsLabel)}</span>
        <span class="topbar-notification-item-copy topbar-notification-item-copy-secondary">${escapeHtml(queryJobEventDateTimeCopy(job))}</span>
      </button>
    `;
  }

  return {
    queryRowsShownLabel,
    queryResultPanelMarkup,
    renderPerformanceChartMarkup,
    renderPerformanceDistributionMarkup,
    queryPerformanceStatsMarkup,
    queryMonitorItemMarkup,
    queryNotificationItemMarkup,
  };
}

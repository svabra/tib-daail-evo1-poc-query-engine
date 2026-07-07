export function createQueryUi(helpers) {
  const {
    escapeHtml,
    formatQueryDuration,
    formatQueryTimestamp,
    queryJobElapsedMs,
    queryJobEventDateTimeCopy,
    queryJobIsRunning,
    queryJobStatusCopy,
    isQueryResultCollapsed = () => false,
    isQueryResultChartsVisible = () => false,
    isQueryTimingDetailsVisible = () => false,
  } = helpers;

  const bytesPerMegabyte = 1024 * 1024;
  const queryMonitorSqlPreviewMaxChars = 480;

  function queryMonitorSqlPreview(sql) {
    const text = String(sql || "").replace(/\s+/g, " ").trim();
    if (text.length <= queryMonitorSqlPreviewMaxChars) {
      return text;
    }
    return `${text.slice(0, queryMonitorSqlPreviewMaxChars - 24).trimEnd()} ... [SQL truncated]`;
  }

  function emptyQueryResultsMarkup(cellId) {
    return `
      <section id="query-results-${escapeHtml(cellId)}" class="result-panel" data-cell-result data-query-job-id="" hidden>
        <header class="result-header">
          <div class="result-header-copy">
            <h3>Result</h3>
            <div class="result-meta-row">
              ${resultDurationMarkup(null)}
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

  function resultChartsToggleMarkup(showToggle, visible = false) {
    const title = visible ? "Hide resource charts" : "Show resource charts";
    return `
      <button
        type="button"
        class="query-runs-chart-toggle result-chart-toggle"
        data-query-result-toggle-charts
        aria-pressed="${visible ? "true" : "false"}"
        title="${title}"
        ${showToggle ? "" : "hidden"}
      >
        <span class="query-runs-chart-toggle-switch" aria-hidden="true">
          <span class="query-runs-chart-toggle-thumb"></span>
        </span>
        <span class="query-runs-chart-toggle-copy" data-query-result-charts-toggle-label>${title}</span>
      </button>
    `;
  }

  function resultDurationMarkup(job, { detailsId = "", detailsVisible = false } = {}) {
    const running = Boolean(job && queryJobIsRunning(job));
    const duration = job ? formatQueryDuration(queryJobElapsedMs(job)) : "0 ms";
    const label = running ? "Running elapsed" : "Total elapsed";
    const tooltip = running
      ? (
          "Running elapsed is the wall-clock time since this cell was submitted. "
          + "It keeps increasing while the backend prepares sources, waits for shared DuckDB access if needed, starts the worker, runs DuckDB, and fetches rows for the UI."
        )
      : (
          "Total elapsed is the main runtime for this cell run: from the Run Cell click until the completed, failed, or cancelled job update reaches this browser. "
          + "Use this number when comparing runs. It includes backend preparation, any shared DuckDB access wait, worker startup, source setup such as S3 Parquet view creation or cache hydration, DuckDB execution, result fetching, and delivery back to the browser. "
          + "When the cell finishes, this number will not move backward; if backend timing or the resource chart observed a longer elapsed time, the result keeps that longer elapsed time. "
          + "The timing breadcrumb next to it shows backend phase measurements. Those sub-times are diagnostics and can differ slightly because they are measured on different clocks and rounded."
        );
    const jobId = job?.jobId || "";
    const durationControl = job
      ? `
        <button
          type="button"
          class="result-meta result-duration-toggle result-duration-value"
          data-query-duration
          data-query-duration-details-toggle
          data-job-id="${escapeHtml(jobId)}"
          aria-expanded="${detailsVisible ? "true" : "false"}"
          aria-controls="${escapeHtml(detailsId)}"
          title="${escapeHtml(tooltip)}"
          aria-label="${escapeHtml(`${label}: ${duration}. Click to show recorded timestamps and total elapsed time. ${tooltip}`)}"
        >${escapeHtml(duration)}</button>
      `
      : `
        <strong
          class="result-meta result-duration-value"
          data-query-duration
          data-job-id=""
          title="${escapeHtml(tooltip)}"
          aria-label="${escapeHtml(`${label}: ${duration}. ${tooltip}`)}"
        >${escapeHtml(duration)}</strong>
      `;
    return `
      <span class="result-duration-group">
        <span class="result-duration-label">${escapeHtml(label)}</span>
        ${durationControl}
        <span class="result-duration-help" title="${escapeHtml(tooltip)}" aria-label="${escapeHtml(tooltip)}">?</span>
      </span>
    `;
  }

  function timingEventLabel(event) {
    const phase = String(event?.phase || "").trim();
    if (phase) {
      return phase.replace(/\s*\.\.\.$/, "");
    }
    const message = String(event?.message || "").trim();
    if (message) {
      return message;
    }
    const rawEvent = String(event?.event || "").trim();
    return rawEvent
      ? rawEvent
          .split("_")
          .filter(Boolean)
          .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
          .join(" ")
      : "Progress event";
  }

  function queryTimingDetailsRows(job) {
    if (!job) {
      return [];
    }

    const rows = [];
    const seenKeys = new Set();
    const pushRow = ({ label, timestamp = "", elapsedMs = null, note = "", tone = "" }) => {
      const normalizedLabel = String(label || "").trim();
      const normalizedTimestamp = String(timestamp || "").trim();
      const numericElapsed = Number(elapsedMs);
      const hasElapsed = Number.isFinite(numericElapsed) && numericElapsed >= 0;
      const key = `${normalizedLabel}::${normalizedTimestamp}::${hasElapsed ? Math.round(numericElapsed) : ""}`;
      if (!normalizedLabel || seenKeys.has(key)) {
        return;
      }
      seenKeys.add(key);
      rows.push({
        label: normalizedLabel,
        timestamp: normalizedTimestamp,
        elapsedMs: hasElapsed ? numericElapsed : null,
        note: String(note || "").trim(),
        tone: String(tone || "").trim(),
      });
    };

    pushRow({
      label: "Submitted",
      timestamp: job.startedAt || "",
      elapsedMs: 0,
      note: "Cell run accepted by the backend.",
    });

    (Array.isArray(job.progressEvents) ? job.progressEvents : []).forEach((event) => {
      pushRow({
        label: timingEventLabel(event),
        timestamp: event?.occurredAt || "",
        elapsedMs: event?.durationMs,
        note: event?.event || "",
      });
    });

    const terminalTimestamp = job.completedAt || job.updatedAt || "";
    if (!queryJobIsRunning(job) && terminalTimestamp) {
      pushRow({
        label: queryJobStatusCopy(job),
        timestamp: terminalTimestamp,
        elapsedMs: job.durationMs,
        note: "Terminal job update.",
      });
    }

    pushRow({
      label: "Total",
      timestamp: queryJobIsRunning(job) ? "Now" : terminalTimestamp || job.updatedAt || "",
      elapsedMs: queryJobElapsedMs(job),
      note: "Same value shown by Total elapsed.",
      tone: "total",
    });

    return rows;
  }

  function queryTimingDetailsTableMarkup(job, { panelId = "", visible = false } = {}) {
    if (!job) {
      return "";
    }

    const rows = queryTimingDetailsRows(job);
    if (!rows.length) {
      return "";
    }

    return `
      <div
        id="${escapeHtml(panelId)}"
        class="query-timing-details"
        data-query-duration-details-panel
        data-job-id="${escapeHtml(job.jobId || "")}"
        ${visible ? "" : "hidden"}
      >
        <table class="query-timing-table">
          <thead>
            <tr>
              <th>Event</th>
              <th>Timestamp</th>
              <th>Elapsed</th>
              <th>Note</th>
            </tr>
          </thead>
          <tbody>
            ${rows
              .map((row) => {
                const timestamp = row.timestamp === "Now"
                  ? "Now"
                  : formatQueryTimestamp(row.timestamp) || row.timestamp || "-";
                const elapsed = row.elapsedMs === null ? "-" : formatQueryDuration(row.elapsedMs);
                const totalAttributes = row.tone === "total"
                  ? ` class="is-total" data-query-duration-total-row`
                  : "";
                const elapsedAttributes = row.tone === "total"
                  ? ` data-query-duration-total data-job-id="${escapeHtml(job.jobId || "")}"`
                  : "";
                return `
                  <tr${totalAttributes}>
                    <th scope="row">${escapeHtml(row.label)}</th>
                    <td>${escapeHtml(timestamp)}</td>
                    <td${elapsedAttributes}>${escapeHtml(elapsed)}</td>
                    <td>${escapeHtml(row.note || "-")}</td>
                  </tr>
                `;
              })
              .join("")}
          </tbody>
        </table>
      </div>
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

  function queryTimingValue(job, key) {
    const numeric = Number(job?.timings?.[key]);
    return Number.isFinite(numeric) && numeric >= 0 ? numeric : null;
  }

  function queryTimingRows(job) {
    if (!job) {
      return [];
    }

    const running = Boolean(queryJobIsRunning(job));
    const totalMs = queryJobElapsedMs(job);
    const backendTotalMs = queryTimingValue(job, "backendTotalMs");
    const deliveryMs =
      !running && Number.isFinite(totalMs) && backendTotalMs !== null
        ? Math.max(0, Number(totalMs) - backendTotalMs)
        : null;
    const fetchMs =
      queryTimingValue(job, "resultFetchMs") ??
      (Number.isFinite(Number(job?.fetchMs)) && Number(job.fetchMs) >= 0 ? Number(job.fetchMs) : null);
    const rows = [
      ["Total elapsed", Number.isFinite(totalMs) ? totalMs : null],
      ["Backend reported", backendTotalMs],
      ["Prepare", queryTimingValue(job, "backendPrepareMs")],
      ["Shared DuckDB wait", queryTimingValue(job, "engineAccessWaitMs")],
      ["Startup", queryTimingValue(job, "workerStartupMs")],
      ["Source setup", queryTimingValue(job, "sourceBootstrapMs")],
      ["Query", queryTimingValue(job, "engineQueryMs")],
      ["Fetch", fetchMs],
      ["Delivery", deliveryMs],
    ].filter(([, value]) => value !== null && Number.isFinite(value));

    const visiblePartTotalMs = rows
      .filter(([label]) => !["Total elapsed", "Backend reported"].includes(label))
      .reduce((sum, [, value]) => sum + Number(value || 0), 0);
    if (Number.isFinite(totalMs)) {
      const overheadMs = totalMs - visiblePartTotalMs;
      if (overheadMs > 1) {
        rows.push(["Overhead", overheadMs]);
      }
    }
    return rows;
  }

  function queryTimingStepDefinitions(job) {
    if (!job) {
      return [];
    }

    const running = Boolean(queryJobIsRunning(job));
    const totalMs = queryJobElapsedMs(job);
    const backendTotalMs = queryTimingValue(job, "backendTotalMs");
    const deliveryMs =
      !running && Number.isFinite(totalMs) && backendTotalMs !== null
        ? Math.max(0, Number(totalMs) - backendTotalMs)
        : null;
    const fetchMs =
      queryTimingValue(job, "resultFetchMs") ??
      (Number.isFinite(Number(job?.fetchMs)) && Number(job.fetchMs) >= 0 ? Number(job.fetchMs) : null);
    const steps = [
      { key: "prepare", label: "Prepare", valueMs: queryTimingValue(job, "backendPrepareMs") },
      { key: "shared-duckdb-wait", label: "Shared DuckDB wait", valueMs: queryTimingValue(job, "engineAccessWaitMs") },
      { key: "startup", label: "Startup", valueMs: queryTimingValue(job, "workerStartupMs") },
      { key: "source-setup", label: "Source setup", valueMs: queryTimingValue(job, "sourceBootstrapMs") },
      { key: "query", label: "Query", valueMs: queryTimingValue(job, "engineQueryMs") },
      { key: "fetch", label: "Fetch", valueMs: fetchMs },
      { key: "delivery", label: "Delivery", valueMs: deliveryMs },
    ];
    const measuredStepTotalMs = steps.reduce(
      (sum, step) => sum + (step.valueMs !== null && Number.isFinite(Number(step.valueMs)) ? Number(step.valueMs) : 0),
      0
    );
    if (Number.isFinite(totalMs)) {
      const overheadMs = Number(totalMs) - measuredStepTotalMs;
      const allMeasuredStepsComplete = steps.every((step) => step.valueMs !== null);
      steps.push({
        key: "overhead",
        label: "Overhead",
        valueMs: (!running || allMeasuredStepsComplete) && overheadMs > 1 ? overheadMs : null,
      });
    }
    return steps;
  }

  function activeTimingStepKey(job, steps) {
    if (!job || !queryJobIsRunning(job)) {
      return "";
    }

    const stepKeys = new Set((steps || []).map((step) => step.key));
    const progressLabel = String(job.progressLabel || "").toLowerCase();
    const message = String(job.message || "").toLowerCase();
    const combined = `${progressLabel} ${message}`;
    const hasStep = (key) => stepKeys.has(key);

    if (job.status === "queued" || combined.includes("preparing query")) {
      return hasStep("prepare") ? "prepare" : "";
    }
    if (combined.includes("waiting for duckdb") || combined.includes("shared duckdb")) {
      return hasStep("shared-duckdb-wait") ? "shared-duckdb-wait" : "";
    }
    if (combined.includes("starting isolated query worker") || combined.includes("worker process")) {
      return hasStep("startup") ? "startup" : "";
    }
    if (
      combined.includes("preparing query sources") ||
      combined.includes("preparing isolated query sources") ||
      combined.includes("cache hydration")
    ) {
      return hasStep("source-setup") ? "source-setup" : "";
    }
    if (
      combined.includes("fetch") ||
      combined.includes("streaming rows") ||
      combined.includes("query is fetching rows") ||
      combined.includes("finalizing") ||
      Number(job.rowsShown || 0) > 0 ||
      Number(job.firstRowMs || 0) > 0
    ) {
      return hasStep("fetch") ? "fetch" : "";
    }
    if (
      combined.includes("query") ||
      combined.includes("duckdb") ||
      combined.includes("running") ||
      Number.isFinite(Number(job.progress))
    ) {
      return hasStep("query") ? "query" : "";
    }
    if (queryTimingValue(job, "workerStartupMs") !== null) {
      return hasStep("query") ? "query" : "";
    }
    if (queryTimingValue(job, "engineAccessWaitMs") !== null) {
      return hasStep("startup") ? "startup" : "";
    }
    return hasStep("prepare") ? "prepare" : "";
  }

  function queryTimingBreadcrumbSteps(job) {
    if (!job) {
      return [];
    }

    const running = Boolean(queryJobIsRunning(job));
    const totalMs = queryJobElapsedMs(job);
    const rawSteps = queryTimingStepDefinitions(job);
    const visibleSteps = running ? rawSteps : rawSteps.filter((step) => step.valueMs !== null);
    if (!visibleSteps.length) {
      return [];
    }

    const activeKey = activeTimingStepKey(job, visibleSteps);
    let currentIndex = running
      ? visibleSteps.findIndex((step) => step.key === activeKey)
      : -1;
    if (running && currentIndex < 0) {
      const lastMeasuredIndex = visibleSteps.reduce(
        (lastIndex, step, index) => (step.valueMs !== null ? index : lastIndex),
        -1
      );
      currentIndex = Math.min(lastMeasuredIndex + 1, visibleSteps.length - 1);
    }
    let completedBeforeMs = 0;

    return visibleSteps.map((step, index) => {
      const state = !running
        ? "completed"
        : index < currentIndex
          ? "completed"
          : index === currentIndex
            ? "current"
            : "pending";
      const previousCompletedMs = completedBeforeMs;
      let displayMs = null;

      if (state === "completed") {
        displayMs = step.valueMs !== null && Number.isFinite(Number(step.valueMs)) ? Number(step.valueMs) : 0;
        completedBeforeMs += displayMs;
      } else if (state === "current" && Number.isFinite(totalMs)) {
        displayMs = Math.max(0, Number(totalMs) - completedBeforeMs);
      }

      return {
        ...step,
        state,
        displayMs,
        completedBeforeMs: previousCompletedMs,
      };
    });
  }

  function queryTimingBreadcrumbMarkup(job) {
    const steps = queryTimingBreadcrumbSteps(job);
    if (!steps.length) {
      return "";
    }

    return `
      <ol class="query-timing-breadcrumb" aria-label="Query timing progress">
        ${steps
          .map((step) => {
            const valueCopy = step.displayMs === null ? "-" : formatQueryDuration(step.displayMs);
            const currentAttributes = step.state === "current"
              ? ` data-query-timing-current-step data-job-id="${escapeHtml(job.jobId || "")}" data-query-timing-completed-ms="${escapeHtml(String(Math.max(0, step.completedBeforeMs || 0)))}"`
              : "";
            return `
              <li
                class="query-timing-step is-${escapeHtml(step.state)}"
                data-query-timing-step
                data-query-timing-step-state="${escapeHtml(step.state)}"
                data-query-timing-step-key="${escapeHtml(step.key)}"
                title="${escapeHtml(`${step.label}: ${valueCopy}`)}"
              >
                <span class="query-timing-step-label">${escapeHtml(step.label)}</span>
                <span class="query-timing-step-value"${currentAttributes}>${escapeHtml(valueCopy)}</span>
              </li>
            `;
          })
          .join("")}
      </ol>
    `;
  }

  function queryTimingClipboardTable(job) {
    const rows = queryTimingRows(job);
    if (!rows.length) {
      return "";
    }
    return [
      "Metric\tValue",
      ...rows.map(([label, value]) => `${label}\t${formatQueryDuration(value)}`),
    ].join("\n");
  }

  function queryRowsShownLabel(job) {
    if (!job) {
      return "Run this cell to inspect the selected data sources.";
    }

    if (job.status === "cancelled") {
      return "Query cancelled successfully.";
    }

    if (job.cancellationPhase) {
      return queryCancellationCopy(job);
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

  function queryCancellationCopy(job) {
    switch (job?.cancellationPhase) {
      case "requested":
        return "Cancellation requested.";
      case "interrupting":
        return "Interrupting the query.";
      case "terminating":
        return "Stopping the query worker process.";
      case "killing":
        return "Hard-stopping the query worker process.";
      case "cancelled":
        return "Query cancelled successfully.";
      default:
        return "Cancelling query.";
    }
  }

  function formatQueryByteCount(value) {
    const bytes = Number(value);
    if (!Number.isFinite(bytes) || bytes <= 0) {
      return "0 B";
    }
    if (bytes < 1024) {
      return `${Math.round(bytes)} B`;
    }
    if (bytes < 1024 * 1024) {
      return `${(bytes / 1024).toFixed(bytes >= 10 * 1024 ? 0 : 1)} KB`;
    }
    if (bytes < 1024 * 1024 * 1024) {
      return `${(bytes / (1024 * 1024)).toFixed(bytes >= 10 * 1024 * 1024 ? 0 : 1)} MB`;
    }
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(bytes >= 10 * 1024 * 1024 * 1024 ? 0 : 1)} GB`;
  }

  function bytesToMegabytes(value) {
    const bytes = Number(value);
    if (!Number.isFinite(bytes) || bytes <= 0) {
      return 0;
    }
    return Math.round((bytes / bytesPerMegabyte) * 10) / 10;
  }

  function formatQueryMegabytes(value) {
    const megabytes = Number(value);
    if (!Number.isFinite(megabytes) || megabytes <= 0) {
      return "0 MB";
    }
    const decimals = megabytes >= 100 ? 0 : 1;
    return `${megabytes.toFixed(decimals).replace(/\.0$/, "")} MB`;
  }

  function formatResourceElapsedLabel(value) {
    const elapsedMs = Number(value);
    if (!Number.isFinite(elapsedMs) || elapsedMs <= 0) {
      return "0s";
    }
    const elapsedSeconds = elapsedMs / 1000;
    if (elapsedSeconds < 60) {
      return `${elapsedSeconds.toFixed(elapsedSeconds >= 10 ? 0 : 1).replace(/\.0$/, "")}s`;
    }
    const minutes = Math.floor(elapsedSeconds / 60);
    const seconds = Math.round(elapsedSeconds % 60);
    return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
  }

  function queryProcessMetricStripMarkup(job, { compact = false, includeCurrent = null } = {}) {
    if (!job) {
      return "";
    }

    const showCurrent = includeCurrent === null ? queryJobIsRunning(job) : Boolean(includeCurrent);
    const metrics = [];
    if (job.processId) {
      metrics.push(["PID", String(job.processId)]);
    }
    if (showCurrent && typeof job.processThreadCount === "number") {
      metrics.push(["Threads", String(job.processThreadCount)]);
    } else if (!showCurrent && typeof job.peakProcessThreadCount === "number") {
      metrics.push(["Threads peak", String(job.peakProcessThreadCount)]);
    }
    if (typeof job.duckdbThreadLimit === "number") {
      metrics.push(["Thread limit", String(job.duckdbThreadLimit)]);
    } else if (
      typeof job.processId === "number" ||
      typeof job.processThreadCount === "number" ||
      typeof job.peakProcessThreadCount === "number"
    ) {
      metrics.push(["Thread limit", "Auto"]);
    }
    if (showCurrent && typeof job.cpuPercent === "number") {
      metrics.push(["Active cores", formatQueryCoreCount(job.cpuPercent / 100)]);
    }
    if (showCurrent && typeof job.cpuPercent === "number") {
      metrics.push(["CPU", formatQueryCpuPercent(job.cpuPercent)]);
    }
    if (typeof job.averageCpuPercent === "number") {
      metrics.push(["CPU avg", formatQueryCpuPercent(job.averageCpuPercent)]);
    }
    if (typeof job.peakCpuPercent === "number") {
      metrics.push(["CPU peak", formatQueryCpuPercent(job.peakCpuPercent)]);
    }
    if (showCurrent && typeof job.memoryRssBytes === "number") {
      metrics.push(["RAM", formatQueryByteCount(job.memoryRssBytes)]);
    }
    if (typeof job.averageMemoryRssBytes === "number") {
      metrics.push(["RAM avg", formatQueryByteCount(job.averageMemoryRssBytes)]);
    }
    if (typeof job.peakMemoryRssBytes === "number") {
      metrics.push(["RAM peak", formatQueryByteCount(job.peakMemoryRssBytes)]);
    }
    if (!metrics.length) {
      return "";
    }

    const compactClass = compact ? " query-process-metric-strip-compact" : "";
    return `
      <div class="query-process-metric-strip${compactClass}">
        ${metrics
          .map(
            ([label, value]) => `
              <span class="query-process-metric">
                <strong>${escapeHtml(label)}</strong>
                <span>${escapeHtml(value)}</span>
              </span>
            `
          )
          .join("")}
      </div>
    `;
  }

  function formatQueryCpuPercent(value) {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return "";
    }
    return `${value.toFixed(value >= 10 ? 0 : 1)}%`;
  }

  function formatQueryCoreCount(value) {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return "";
    }
    return value.toFixed(value >= 10 ? 0 : 1);
  }

  function queryResourceSeries(samples, valueKey, valueMapper = (value) => value) {
    return samples.map((sample) => {
      const value = Number(sample?.[valueKey]);
      if (!Number.isFinite(value)) {
        return null;
      }
      const mappedValue = Number(valueMapper(value));
      return Number.isFinite(mappedValue) ? Math.max(0, mappedValue) : null;
    });
  }

  function lastFiniteSeriesValue(values) {
    for (let index = values.length - 1; index >= 0; index -= 1) {
      const value = Number(values[index]);
      if (Number.isFinite(value)) {
        return value;
      }
    }
    return null;
  }

  function maxFiniteSeriesValue(values) {
    const finiteValues = values.filter((value) => Number.isFinite(Number(value)));
    return finiteValues.length ? Math.max(...finiteValues.map((value) => Number(value))) : null;
  }

  function queryResourceSparklineChartMarkup({
    label,
    unitLabel,
    axisLabel,
    samples,
    valueKey,
    averageKey,
    limitKey = "",
    maxValue,
    formatter,
    running,
    valueMapper,
    chartKind = "",
    currentDatasetLabel = "",
    averageDatasetLabel = "Average",
    currentLegendLabel = "",
    averageLegendLabel = "AVG",
    extraLegendItems = [],
    helpTooltip = "",
  }) {
    const currentValues = queryResourceSeries(samples, valueKey, valueMapper);
    const averageValues = queryResourceSeries(samples, averageKey, valueMapper);
    const limitValues = limitKey ? queryResourceSeries(samples, limitKey, valueMapper) : [];
    const hasCurrent = currentValues.some((value) => value !== null);
    const hasAverage = averageValues.some((value) => value !== null);
    const hasLimit = limitValues.some((value) => value !== null);
    if (!hasCurrent && !hasAverage && !hasLimit) {
      return "";
    }
    const resolvedCurrentLegendLabel = currentLegendLabel || (running ? "Now" : "Peak");
    const currentCopy = formatter(running ? lastFiniteSeriesValue(currentValues) : maxFiniteSeriesValue(currentValues));
    const averageCopy = formatter(lastFiniteSeriesValue(averageValues));
    const resolvedChartKind = chartKind || label.toLowerCase();
    const currentAttribute = escapeHtml(JSON.stringify(currentValues));
    const averageAttribute = escapeHtml(JSON.stringify(averageValues));
    const limitAttribute = escapeHtml(JSON.stringify(limitValues));
    const labelAttribute = escapeHtml(JSON.stringify(
      samples.map((sample, index) => {
        const elapsedMs = Number(sample?.elapsedMs);
        return Number.isFinite(elapsedMs) ? formatResourceElapsedLabel(elapsedMs) : String(index + 1);
      })
    ));
    const chartMax = Math.max(
      1,
      Number(maxValue || 0),
      maxFiniteSeriesValue(currentValues) || 0,
      maxFiniteSeriesValue(averageValues) || 0,
      maxFiniteSeriesValue(limitValues) || 0
    );
    const resolvedCurrentDatasetLabel = currentDatasetLabel || (running ? "Current" : "Peak sample");
    const resolvedHelpTooltip = helpTooltip || (
      `${label} samples are collected periodically while the query worker is running. `
      + "The x-axis follows the same elapsed clock as Total elapsed, but it only labels the moments when CPU or RAM was sampled. "
      + "The last tick can differ slightly from Total elapsed because the query can finish between two samples and the labels are rounded."
    );
    const extraMarkup = (Array.isArray(extraLegendItems) ? extraLegendItems : [])
      .filter((item) => item && item.label && item.value)
      .map((item) => `
        <span>
          <i class="${escapeHtml(item.className || "is-extra")}"></i>${escapeHtml(item.label)} ${escapeHtml(item.value)}
        </span>
      `)
      .join("");
    return `
      <article class="query-resource-sparkline-card">
        <div class="query-resource-sparkline-header">
          <span class="query-resource-sparkline-title">
            <strong>${escapeHtml(label)}</strong>
            <span class="query-resource-sparkline-help" title="${escapeHtml(resolvedHelpTooltip)}" aria-label="${escapeHtml(resolvedHelpTooltip)}">?</span>
          </span>
        </div>
        <div class="query-resource-sparkline-plot">
          <span class="query-resource-sparkline-axis" aria-hidden="true">${escapeHtml(axisLabel || unitLabel)}</span>
          <div class="query-resource-sparkline-canvas">
            <canvas
              data-query-resource-chart
              data-query-resource-kind="${escapeHtml(resolvedChartKind)}"
            data-query-resource-current="${currentAttribute}"
            data-query-resource-average="${averageAttribute}"
            data-query-resource-limit="${limitAttribute}"
            data-query-resource-labels="${labelAttribute}"
            data-query-resource-max="${escapeHtml(String(chartMax))}"
              data-query-resource-axis-label="${escapeHtml(axisLabel)}"
              data-query-resource-current-label="${escapeHtml(resolvedCurrentDatasetLabel)}"
              data-query-resource-average-label="${escapeHtml(averageDatasetLabel)}"
              aria-label="${escapeHtml(label)} query resource chart"
            ></canvas>
          </div>
        </div>
        <div class="query-resource-sparkline-legend">
          <span><i class="is-current"></i>${escapeHtml(resolvedCurrentLegendLabel)} ${escapeHtml(currentCopy)}</span>
          ${hasAverage ? `<span><i class="is-average"></i>${escapeHtml(averageLegendLabel)} ${escapeHtml(averageCopy)}</span>` : ""}
          ${extraMarkup}
        </div>
      </article>
    `;
  }

  function queryResourceSparklineMarkup(job, { compact = false, hidden = false } = {}) {
    const samples = Array.isArray(job?.resourceSamples)
      ? job.resourceSamples.filter((sample) => sample && typeof sample === "object")
      : [];
    if (!samples.length) {
      return "";
    }
    const cpuValueKey = samples.some((sample) => Number.isFinite(Number(sample.cpuCapacityPercent)))
      ? "cpuCapacityPercent"
      : "cpuPercent";
    const averageCpuValueKey = samples.some((sample) => Number.isFinite(Number(sample.averageCpuCapacityPercent)))
      ? "averageCpuCapacityPercent"
      : "averageCpuPercent";
    const cpuMax = Math.max(
      1,
      100,
      ...samples.map((sample) => Number(sample[cpuValueKey] || 0)),
      ...samples.map((sample) => Number(sample[averageCpuValueKey] || 0))
    );
    const compactClass = compact ? " query-resource-sparklines-compact" : "";
    const running = queryJobIsRunning(job);
    const spillSamples = samples.filter((sample) =>
      Number.isFinite(Number(sample.duckdbSpillBytes)) ||
      Number.isFinite(Number(sample.duckdbSpillTotalBytes))
    );
    const latestSpillSample = spillSamples.length ? spillSamples[spillSamples.length - 1] : null;
    const spillLimit = Math.max(
      0,
      ...spillSamples.map((sample) => Number(sample.duckdbSpillLimitBytes || 0))
    );
    const latestTotalSpill = Number(latestSpillSample?.duckdbSpillTotalBytes);
    const latestOtherSpill = Number(latestSpillSample?.duckdbSpillOtherBytes);
    const latestSpillFree = Number(latestSpillSample?.duckdbSpillDiskFreeBytes);
    const spillExtraLegendItems = [
      Number.isFinite(latestTotalSpill)
        ? { label: "Shared", value: formatQueryByteCount(latestTotalSpill), className: "is-total" }
        : null,
      spillLimit > 0
        ? { label: "Quota", value: formatQueryByteCount(spillLimit), className: "is-limit" }
        : null,
      Number.isFinite(latestSpillFree)
        ? { label: "Disk free", value: formatQueryByteCount(latestSpillFree), className: "is-free" }
        : null,
    ].filter(Boolean);
    return `
      <div class="query-resource-sparklines${compactClass}" data-query-resource-sparklines ${hidden ? "hidden" : ""}>
        ${queryResourceSparklineChartMarkup({
          label: "CPU",
          unitLabel: "%",
          axisLabel: cpuValueKey === "cpuCapacityPercent" ? "CPU capacity %" : "CPU core %",
          samples,
          valueKey: cpuValueKey,
          averageKey: averageCpuValueKey,
          maxValue: cpuMax,
          formatter: formatQueryCpuPercent,
          running,
        })}
        ${queryResourceSparklineChartMarkup({
          label: "RAM",
          unitLabel: "MB",
          axisLabel: "RAM (MB)",
          samples,
          valueKey: "memoryRssBytes",
          averageKey: "averageMemoryRssBytes",
          maxValue: 1,
          formatter: formatQueryMegabytes,
          running,
          valueMapper: bytesToMegabytes,
        })}
        ${queryResourceSparklineChartMarkup({
          label: "DuckDB spill",
          unitLabel: "bytes",
          axisLabel: "Spill",
          samples,
          valueKey: "duckdbSpillBytes",
          averageKey: "duckdbSpillOtherBytes",
          limitKey: "duckdbSpillLimitBytes",
          maxValue: spillLimit || 1,
          formatter: formatQueryByteCount,
          running,
          chartKind: "spill",
          currentDatasetLabel: "This query",
          averageDatasetLabel: "Other spill",
          currentLegendLabel: running ? "This query" : "Peak query",
          averageLegendLabel: "Other",
          extraLegendItems: spillExtraLegendItems,
          helpTooltip: (
            "DuckDB spill samples show temporary disk used by this query worker, other DuckDB spill files in the shared temp root, "
            + "and the configured DuckDB temp quota. Query cache files are separate, but they use the same workspace disk."
          ),
        })}
      </div>
    `;
  }

  function queryProgressActivityCopy(job) {
    if (!job || !queryJobIsRunning(job)) {
      return "Query activity is idle.";
    }

    if (job.cancellationPhase) {
      return queryCancellationCopy(job);
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

  function queryProgressMarkup(job, { chartsHidden = false } = {}) {
    if (!queryJobIsRunning(job)) {
      return "";
    }

    const progressValue =
      typeof job.progress === "number" && Number.isFinite(job.progress)
        ? Math.max(0, Math.min(100, job.progress * 100))
        : null;
    const backendCopy = escapeHtml(job.backendName || "VMTP DUCKDB");
    const progressLabel = escapeHtml(job.cancellationPhase ? queryCancellationCopy(job) : job.progressLabel || "Running...");
    const metricsMarkup = queryProcessMetricStripMarkup(job);
    const sparklineMarkup = queryResourceSparklineMarkup(job, { hidden: chartsHidden });

    if (progressValue === null) {
      return `
        <div class="query-progress-card query-progress-card-indeterminate">
          <div class="query-progress-copy">
            <strong>${progressLabel}</strong>
            <span>${backendCopy}${job.executionMode ? ` | ${escapeHtml(job.executionMode)}` : ""}</span>
          </div>
          <div class="query-progress-status">
            <span class="query-progress-status-dot" aria-hidden="true"></span>
            <span>${escapeHtml(queryProgressActivityCopy(job))}</span>
          </div>
          ${metricsMarkup}
          ${sparklineMarkup}
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
        ${metricsMarkup}
        ${sparklineMarkup}
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

  function queryWarningsMarkup(job) {
    const warnings = Array.isArray(job?.warnings)
      ? job.warnings.map((warning) => String(warning ?? "").trim()).filter(Boolean)
      : [];
    if (!warnings.length) {
      return "";
    }
    return `
      <div class="result-warning-list" data-query-warnings>
        <strong>Warnings</strong>
        <ul>
          ${warnings.map((warning) => `<li>${escapeHtml(warning)}</li>`).join("")}
        </ul>
      </div>
    `;
  }

  function queryResultStorageMarkup(job) {
    const storage = job?.resultStorage && typeof job.resultStorage === "object" ? job.resultStorage : null;
    if (!storage || storage.enabled === false || !storage.path) {
      return "";
    }
    const status = String(storage.status || "planned").trim().toLowerCase() || "planned";
    const statusLabel = status === "completed"
      ? "Stored"
      : status === "failed"
        ? "Failed"
        : status === "cancelled" || status === "canceled"
          ? "Cancelled"
        : status === "storing"
          ? "Storing"
          : "Planned";
    const message = String(storage.message || "").trim();
    const path = String(storage.path || "").trim();
    const virtualPath = String(storage.virtualPath || "").trim();
    const duckdbReference = String(storage.duckdbReference || storage.duckdbPath || "").trim();
    return `
      <div
        class="result-storage-summary is-${escapeHtml(status)}"
        data-result-storage-summary
        data-result-storage-path="${escapeHtml(path)}"
        data-result-storage-virtual-path="${escapeHtml(virtualPath)}"
        data-result-storage-duckdb-reference="${escapeHtml(duckdbReference)}"
      >
        <div class="result-storage-copy">
          <strong>S3 result set storage</strong>
          <span>${escapeHtml(path)}</span>
          ${message ? `<small>${escapeHtml(message)}</small>` : ""}
        </div>
        <div class="result-storage-actions">
          <span class="result-storage-status">${escapeHtml(statusLabel)}</span>
          <button type="button" class="result-storage-copy-button" data-copy-result-storage-virtual title="Copy virtual S3 source path">Virtual</button>
          <button type="button" class="result-storage-copy-button" data-copy-result-storage-duckdb title="Copy DuckDB read_parquet path">DuckDB</button>
        </div>
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
          >Save Results in S3 Object Storage ...</button>
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
    const timingBreadcrumb = queryTimingBreadcrumbMarkup(job);
    if (job.comparisonInsights?.previous) {
      metricPills.push(queryInsightPillMarkup(job.comparisonInsights.previous));
    }
    if (job.comparisonInsights?.median) {
      metricPills.push(queryInsightPillMarkup(job.comparisonInsights.median));
    }
    if (job.cacheInsights) {
      metricPills.push(queryInsightPillMarkup(job.cacheInsights));
    }
    if (job.footprintInsights) {
      metricPills.push(queryInsightPillMarkup(job.footprintInsights));
    }

    if (!timingBreadcrumb && !metricPills.length) {
      return "";
    }

    const timingTable = queryTimingClipboardTable(job);
    const timingAttributes = timingTable
      ? ` data-copy-query-timings data-query-timing-table="${escapeHtml(encodeURIComponent(timingTable))}" role="button" tabindex="0" title="Copy timing table"`
      : "";

    return `<div class="result-metric-strip"${timingAttributes}>${timingBreadcrumb}${metricPills.join("")}</div>`;
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

  function queryMonitorWarningsMarkup(job) {
    const warnings = Array.isArray(job?.warnings)
      ? job.warnings.map((warning) => String(warning ?? "").trim()).filter(Boolean)
      : [];
    if (!warnings.length) {
      return "";
    }

    return `
      <div class="query-monitor-warning-list">
        <strong>Warnings</strong>
        <ul>
          ${warnings.map((warning) => `<li>${escapeHtml(warning)}</li>`).join("")}
        </ul>
      </div>
    `;
  }

  function queryMonitorErrorMarkup(job) {
    const error = String(job?.error || "").trim();
    if (!error) {
      return "";
    }

    return `
      <div class="query-monitor-error">
        <strong>${escapeHtml(job.status === "cancelled" ? "Query cancelled." : "Query failed.")}</strong>
        <pre>${escapeHtml(error)}</pre>
      </div>
    `;
  }

  function queryMonitorProgressEventsMarkup(job) {
    const events = Array.isArray(job?.progressEvents)
      ? job.progressEvents.filter((event) => event && typeof event === "object")
      : [];
    if (!events.length) {
      return "";
    }

    const visibleEvents = events.slice(-5);
    return `
      <details class="query-monitor-progress-events">
        <summary>Progress updates (${events.length})</summary>
        <ol>
          ${visibleEvents
            .map((event) => {
              const label = timingEventLabel(event);
              const message = String(event?.message || event?.phase || event?.event || "").trim();
              const timestamp = String(event?.displayTime || event?.occurredAt || "").trim();
              return `
                <li>
                  <strong>${escapeHtml(label)}</strong>
                  ${message && message !== label ? `<span>${escapeHtml(message)}</span>` : ""}
                  ${timestamp ? `<time>${escapeHtml(timestamp)}</time>` : ""}
                </li>
              `;
            })
            .join("")}
        </ol>
      </details>
    `;
  }

  function queryResultPanelMarkup(cellId, job = null) {
    if (!job) {
      return emptyQueryResultsMarkup(cellId);
    }

    const collapsed = Boolean(isQueryResultCollapsed(cellId, job));
    const chartsVisible = Boolean(isQueryResultChartsVisible(cellId, job));
    const resultBodyId = `query-result-body-${cellId}`;
    const timingDetailsId = `query-timing-details-${cellId}`;
    const timingDetailsVisible = Boolean(isQueryTimingDetailsVisible(cellId, job));
    const showExportActions = job.status === "completed" && job.columns.length > 0;
    const rowsBadge = queryRowsShownLabel(job);
    const showRowsBadge = queryJobIsRunning(job) || Number(job.rowsShown || 0) > 0 || Boolean(job.truncated);
    const terminalMetricsMarkup = queryJobIsRunning(job) ? "" : queryProcessMetricStripMarkup(job);
    const hasResourceCharts = Boolean(queryResourceSparklineMarkup(job));
    const terminalSparklineMarkup = queryJobIsRunning(job) ? "" : queryResourceSparklineMarkup(job, { hidden: !chartsVisible });
    const warningsMarkup = queryWarningsMarkup(job);
    const resultStorageMarkup = queryResultStorageMarkup(job);
    const resultBody = job.status === "cancelled"
      ? `
          <div class="result-empty result-empty-cancelled">
            ${warningsMarkup}
            ${resultStorageMarkup}
            <p>${escapeHtml(queryCancellationCopy({ ...job, cancellationPhase: "cancelled" }))}</p>
            ${terminalMetricsMarkup}
            ${terminalSparklineMarkup}
          </div>
        `
      : job.error
      ? `
          <div class="result-error">
            <strong>${escapeHtml(job.status === "cancelled" ? "Query cancelled." : "Query failed.")}</strong>
            ${warningsMarkup}
            ${resultStorageMarkup}
            <pre>${escapeHtml(job.error)}</pre>
            ${terminalMetricsMarkup}
            ${terminalSparklineMarkup}
          </div>
        `
      : job.columns.length
        ? `
            ${queryProgressMarkup(job, { chartsHidden: !chartsVisible })}
            ${warningsMarkup}
            ${resultStorageMarkup}
            ${terminalMetricsMarkup}
            ${terminalSparklineMarkup}
            ${queryResultTableMarkup(job)}
          `
        : queryJobIsRunning(job)
          ? `
              ${queryProgressMarkup(job, { chartsHidden: !chartsVisible })}
              <div class="result-empty result-empty-running">
                ${warningsMarkup}
                ${resultStorageMarkup}
                <p>${escapeHtml(job.message || "Running query...")}</p>
              </div>
            `
          : `
              <div class="result-empty">
                ${warningsMarkup}
                ${resultStorageMarkup}
                <p>${escapeHtml(job.message || "Statement executed successfully.")}</p>
                ${terminalMetricsMarkup}
                ${terminalSparklineMarkup}
              </div>
            `;

    return `
      <section
        id="query-results-${escapeHtml(cellId)}"
        class="result-panel"
        data-cell-result
        data-query-job-id="${escapeHtml(job.jobId || "")}" 
        data-query-result-collapse-key="${escapeHtml(job.jobId || cellId || "")}"
        data-query-result-collapsed="${collapsed ? "true" : "false"}"
        data-query-result-charts-key="${escapeHtml(job.jobId || cellId || "")}"
        data-query-result-charts-visible="${chartsVisible ? "true" : "false"}"
      >
        <header class="result-header">
          <div class="result-header-copy">
            <h3>Result</h3>
            <div class="result-meta-row">
              ${resultDurationMarkup(job, { detailsId: timingDetailsId, detailsVisible: timingDetailsVisible })}
              ${resultMetricStripMarkup(job)}
            </div>
          </div>
          <div class="result-header-actions">
            ${resultChartsToggleMarkup(hasResourceCharts, chartsVisible)}
            <button
              type="button"
              class="result-collapse-toggle"
              data-query-result-toggle
              aria-label="${collapsed ? "Show result" : "Hide result"}"
              aria-expanded="${collapsed ? "false" : "true"}"
              aria-controls="${escapeHtml(resultBodyId)}"
              title="${collapsed ? "Show result" : "Hide result"}"
            >
              <span class="result-collapse-chevron" aria-hidden="true"></span>
            </button>
            <span class="result-badge${queryJobIsRunning(job) ? " is-live" : ""}" ${showRowsBadge ? "" : "hidden"}>${escapeHtml(rowsBadge)}</span>
            ${resultExportMenuMarkup(showExportActions, job.jobId || "")}
          </div>
        </header>
        ${queryTimingDetailsTableMarkup(job, { panelId: timingDetailsId, visible: timingDetailsVisible })}
        <div id="${escapeHtml(resultBodyId)}" class="result-body" data-query-result-body ${collapsed ? "hidden" : ""}>
          ${resultBody}
        </div>
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
    const cancelling = running && Boolean(job.cancellationPhase);
    const canCancel = running && job.canCancel !== false;
    const rowsCopy = job.rowsShown > 0 ? `${job.rowsShown} row(s)` : "No rows yet";
    const timestamp = job.startedAt || job.updatedAt;
    const progressValue =
      typeof job.progress === "number" && Number.isFinite(job.progress)
        ? Math.max(0, Math.min(100, job.progress * 100))
        : null;
    const progressMarkup = running
      ? `
          <div class="query-monitor-progress">
            <div class="query-monitor-progress-copy">
              <span>${escapeHtml(cancelling ? queryCancellationCopy(job) : job.progressLabel || "Running...")}</span>
              <span>${progressValue === null ? "Progress unavailable" : `${Math.round(progressValue)}%`}</span>
            </div>
            <div class="query-progress-track">
              <span style="width:${progressValue === null ? 100 : progressValue}%;"></span>
            </div>
          </div>
        `
      : "";
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
          ${queryProcessMetricStripMarkup(job, { compact: true })}
          ${queryResourceSparklineMarkup(job, { compact: true })}
          ${progressMarkup}
          ${queryMonitorInsightStripMarkup(job)}
          ${queryMonitorWarningsMarkup(job)}
          ${queryMonitorErrorMarkup(job)}
          ${queryMonitorProgressEventsMarkup(job)}
          <p class="query-monitor-sql">${escapeHtml(queryMonitorSqlPreview(job.sql))}</p>
        </div>
        <div class="query-monitor-item-actions">
          ${running && canCancel
            ? cancelling
              ? `<button type="button" class="query-monitor-cancel is-cancelling" disabled>Cancelling...</button>`
              : `<button type="button" class="query-monitor-cancel" data-cancel-query-job="${escapeHtml(job.jobId)}">Cancel</button>`
            : ""}
          <span class="query-monitor-updated">${escapeHtml(formatQueryTimestamp(timestamp))}</span>
        </div>
      </article>
    `;
  }

  function queryNotificationItemMarkup(job) {
    const rowsLabel = queryRowsShownLabel(job);
    const statusLabel = job.cancellationPhase ? queryCancellationCopy(job) : queryJobStatusCopy(job);
    const statusClass = String(job.status || "unknown").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-") || "unknown";
    return `
      <button
        type="button"
        class="topbar-notification-item"
        data-open-query-notebook="${escapeHtml(job.notebookId)}"
        data-open-query-cell="${escapeHtml(job.cellId)}"
        title="Open ${escapeHtml(job.notebookTitle)}"
      >
        <span class="topbar-notification-item-status is-${escapeHtml(statusClass)}${queryJobIsRunning(job) ? " is-live" : ""}">${escapeHtml(statusLabel)}</span>
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
    queryResourceSparklineMarkup,
  };
}

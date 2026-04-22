function jsonMarkup(value, escapeHtml) {
  let serialized = "";
  try {
    serialized = JSON.stringify(value, null, 2);
  } catch (_error) {
    serialized = String(value ?? "");
  }
  return `<pre class="python-output-json">${escapeHtml(serialized)}</pre>`;
}

export function createPythonUi(helpers) {
  const {
    escapeHtml,
    formatQueryDuration,
    pythonJobElapsedMs,
    pythonJobIsRunning,
    pythonJobStatusCopy,
  } = helpers;

  function emptyPythonResultsMarkup(cellId) {
    return `
      <section id="python-results-${escapeHtml(cellId)}" class="result-panel python-result-panel" data-cell-result data-python-job-id="" hidden>
        <header class="result-header">
          <div class="result-header-copy">
            <h3>Python Output</h3>
            <div class="result-meta-row">
              <p class="result-meta">0 ms</p>
            </div>
          </div>
          <div class="result-header-actions">
            <span class="result-badge">Run this Python cell to inspect results.</span>
          </div>
        </header>
        <div class="result-empty">
          <p>Run this Python cell to inspect results.</p>
        </div>
      </section>
    `;
  }

  function pythonProgressMarkup(job) {
    if (!pythonJobIsRunning(job)) {
      return "";
    }

    return `
      <div class="query-progress-card query-progress-card-indeterminate python-progress-card">
        <div class="query-progress-copy">
          <strong>${escapeHtml(job.progressLabel || "Running...")}</strong>
          <span>${escapeHtml(job.backendName || "Headless Jupyter Kernel")}</span>
        </div>
        <div class="query-progress-status">
          <span class="query-progress-status-dot" aria-hidden="true"></span>
          <span>${escapeHtml(job.message || "Running Python cell...")}</span>
        </div>
      </div>
    `;
  }

  function pythonOutputMarkup(output) {
    const outputType = String(output?.type || "").trim().toLowerCase();

    switch (outputType) {
      case "stream":
        return `
          <section class="python-output python-output-stream-block">
            <header class="python-output-header">
              <span class="python-output-kind">${escapeHtml(output.name || "stdout")}</span>
            </header>
            <pre class="python-output-stream python-output-stream-${escapeHtml(output.name || "stdout")}">${escapeHtml(output.text || "")}</pre>
          </section>
        `;
      case "table":
        return `
          <section class="python-output python-output-table-block">
            <header class="python-output-header">
              <span class="python-output-kind">Table</span>
            </header>
            <div class="python-output-html result-table-wrap">${output.html || ""}</div>
          </section>
        `;
      case "html":
        return `
          <section class="python-output python-output-html-block">
            <header class="python-output-header">
              <span class="python-output-kind">HTML</span>
            </header>
            <div class="python-output-html">${output.html || ""}</div>
          </section>
        `;
      case "json":
        return `
          <section class="python-output python-output-json-block">
            <header class="python-output-header">
              <span class="python-output-kind">JSON</span>
            </header>
            ${jsonMarkup(output.data, escapeHtml)}
          </section>
        `;
      case "image":
        return `
          <section class="python-output python-output-image-block">
            <header class="python-output-header">
              <span class="python-output-kind">Image</span>
            </header>
            <figure class="python-output-image-wrap">
              <img class="python-output-image" src="data:${escapeHtml(output.mimeType || "image/png")};base64,${escapeHtml(output.data || "")}" alt="Python output image">
            </figure>
          </section>
        `;
      case "error":
        return `
          <section class="python-output python-output-error-block">
            <div class="result-error python-output-error">
              <strong>${escapeHtml(output.errorName || "Python execution failed.")}</strong>
              <pre>${escapeHtml(output.text || output.errorValue || "")}</pre>
            </div>
          </section>
        `;
      case "text":
      default:
        return `
          <section class="python-output python-output-text-block">
            <header class="python-output-header">
              <span class="python-output-kind">Text</span>
            </header>
            <pre class="python-output-text">${escapeHtml(output.text || "")}</pre>
          </section>
        `;
    }
  }

  function pythonOutputsMarkup(job) {
    const outputs = Array.isArray(job?.outputs) ? job.outputs : [];
    if (!outputs.length) {
      return `
        <div class="result-empty${pythonJobIsRunning(job) ? " result-empty-running" : ""}">
          <p>${escapeHtml(job?.message || "Python execution finished without display output.")}</p>
        </div>
      `;
    }

    return `<div class="python-output-stack">${outputs.map((output) => pythonOutputMarkup(output)).join("")}</div>`;
  }

  function pythonResultSummary(job) {
    if (!job) {
      return "Run this Python cell to inspect results.";
    }

    const outputCount = Array.isArray(job.outputs) ? job.outputs.length : 0;
    if (pythonJobIsRunning(job)) {
      return job.message || "Running Python cell...";
    }
    if (outputCount > 0) {
      return `${outputCount} output item(s)`;
    }
    return job.message || "Python execution completed.";
  }

  function pythonResultPanelMarkup(cellId, job = null) {
    if (!job) {
      return emptyPythonResultsMarkup(cellId);
    }

    return `
      <section
        id="python-results-${escapeHtml(cellId)}"
        class="result-panel python-result-panel"
        data-cell-result
        data-python-job-id="${escapeHtml(job.jobId || "")}"
      >
        <header class="result-header">
          <div class="result-header-copy">
            <h3>Python Output</h3>
            <div class="result-meta-row">
              <p class="result-meta" data-python-duration data-job-id="${escapeHtml(job.jobId || "")}">${escapeHtml(formatQueryDuration(pythonJobElapsedMs(job)))}</p>
            </div>
          </div>
          <div class="result-header-actions">
            <span class="result-badge${pythonJobIsRunning(job) ? " is-live" : ""}">${escapeHtml(pythonResultSummary(job))}</span>
            <span class="workspace-access-badge workspace-access-badge-small workspace-access-badge-language">${escapeHtml(pythonJobStatusCopy(job))}</span>
          </div>
        </header>
        ${pythonProgressMarkup(job)}
        ${pythonOutputsMarkup(job)}
      </section>
    `;
  }

  return {
    pythonResultPanelMarkup,
  };
}

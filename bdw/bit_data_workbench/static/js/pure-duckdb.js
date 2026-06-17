const terminalStatuses = new Set(["completed", "failed", "cancelled", "canceled", "aborted", "incomplete"]);
const activeRuns = new WeakMap();

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatDuration(durationMs) {
  const value = Math.max(0, Math.round(Number.isFinite(Number(durationMs)) ? Number(durationMs) : 0));
  if (value < 1000) {
    return `${value} ms`;
  }
  const seconds = Math.floor(value / 1000);
  const ms = value - seconds * 1000;
  if (seconds < 60) {
    return `${seconds}s ${ms} ms`;
  }
  const minutes = Math.floor(seconds / 60);
  const restSeconds = seconds - minutes * 60;
  return `${minutes}m ${restSeconds}s ${ms} ms`;
}

function cellParts(cell) {
  return {
    button: cell.querySelector("[data-run-pure-duckdb-cell]"),
    textarea: cell.querySelector("[data-pure-duckdb-sql]"),
    duration: cell.querySelector("[data-pure-duckdb-duration]"),
    result: cell.querySelector("[data-pure-duckdb-result]"),
  };
}

function setDuration(cell, durationMs) {
  const { duration } = cellParts(cell);
  if (duration) {
    duration.textContent = formatDuration(durationMs);
  }
}

function renderStatus(cell, copy) {
  const { result } = cellParts(cell);
  if (result) {
    result.innerHTML = `<p class="pure-duckdb-status">${escapeHtml(copy)}</p>`;
  }
}

function renderError(cell, copy) {
  const { result } = cellParts(cell);
  if (result) {
    result.innerHTML = `<pre class="pure-duckdb-error">${escapeHtml(copy || "Query failed.")}</pre>`;
  }
}

function renderRows(cell, job) {
  const { result } = cellParts(cell);
  if (!result) {
    return;
  }
  const columns = Array.isArray(job?.columns) ? job.columns : [];
  const rows = Array.isArray(job?.rows) ? job.rows : [];
  if (!columns.length) {
    result.innerHTML = `<p class="pure-duckdb-status">${escapeHtml(job?.message || "Statement executed successfully.")}</p>`;
    return;
  }
  const head = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  const body = rows
    .map((row) => {
      const values = Array.isArray(row) ? row : [];
      return `<tr>${columns.map((_column, index) => `<td>${escapeHtml(values[index])}</td>`).join("")}</tr>`;
    })
    .join("");
  const message = job?.message ? `<p class="pure-duckdb-status">${escapeHtml(job.message)}</p>` : "";
  result.innerHTML = `
    ${message}
    <table class="pure-duckdb-table">
      <thead><tr>${head}</tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function terminalDuration(job, fallbackMs) {
  const duration = Number(job?.durationMs);
  if (Number.isFinite(duration) && duration >= 0) {
    return duration;
  }
  const backendTotal = Number(job?.timings?.backendTotalMs);
  if (Number.isFinite(backendTotal) && backendTotal >= 0) {
    return backendTotal;
  }
  return fallbackMs;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.headers || {}),
    },
  });
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Keep the HTTP status message.
    }
    throw new Error(message);
  }
  return response.json();
}

async function pollJob(cell, jobId, startedAt) {
  const state = activeRuns.get(cell);
  if (!state || state.jobId !== jobId) {
    return;
  }
  const elapsedMs = performance.now() - startedAt;
  setDuration(cell, elapsedMs);
  let job = null;
  try {
    job = await fetchJson(`/api/pure-duckdb/jobs/${encodeURIComponent(jobId)}`);
  } catch (error) {
    renderError(cell, error instanceof Error ? error.message : "Failed to load query status.");
    finishRun(cell);
    return;
  }

  const status = String(job?.status || "").toLowerCase();
  if (!terminalStatuses.has(status)) {
    renderStatus(cell, job?.message || "Running...");
    window.setTimeout(() => pollJob(cell, jobId, startedAt), 500);
    return;
  }

  setDuration(cell, terminalDuration(job, elapsedMs));
  if (status === "completed") {
    renderRows(cell, job);
  } else {
    renderError(cell, job?.error || job?.message || `Query finished with status ${status}.`);
  }
  finishRun(cell);
}

function finishRun(cell) {
  const { button, textarea } = cellParts(cell);
  if (button) {
    button.disabled = false;
  }
  if (textarea) {
    textarea.disabled = false;
  }
  activeRuns.delete(cell);
}

async function runCell(cell) {
  const { button, textarea } = cellParts(cell);
  const sql = textarea?.value || "";
  const cellId = cell?.dataset?.cellId || "";
  if (!cell || !textarea || !button || !sql.trim()) {
    return;
  }

  button.disabled = true;
  textarea.disabled = true;
  setDuration(cell, 0);
  renderStatus(cell, "Starting...");
  const startedAt = performance.now();

  try {
    const job = await fetchJson("/api/pure-duckdb/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cellId, sql }),
    });
    const jobId = String(job?.jobId || "").trim();
    if (!jobId) {
      throw new Error("Backend did not return a job id.");
    }
    activeRuns.set(cell, { jobId });
    pollJob(cell, jobId, startedAt);
  } catch (error) {
    setDuration(cell, performance.now() - startedAt);
    renderError(cell, error instanceof Error ? error.message : "Failed to start query.");
    finishRun(cell);
  }
}

document.addEventListener("click", (event) => {
  const button = event.target.closest?.("[data-run-pure-duckdb-cell]");
  if (!button) {
    return;
  }
  const cell = button.closest("[data-pure-duckdb-cell]");
  runCell(cell);
});

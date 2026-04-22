const pythonJobRunningStatuses = new Set(["queued", "running"]);

function normalizePythonJobOutput(output) {
  if (!output || typeof output !== "object") {
    return null;
  }

  return {
    ...output,
    type: String(output.type || "").trim().toLowerCase(),
    text: String(output.text || ""),
    name: String(output.name || "").trim(),
    html: String(output.html || ""),
    mimeType: String(output.mimeType || "").trim(),
    traceback: Array.isArray(output.traceback) ? output.traceback.map((line) => String(line || "")) : [],
    errorName: String(output.errorName || "").trim(),
    errorValue: String(output.errorValue || ""),
  };
}

export function normalizePythonJob(job) {
  if (!job || typeof job !== "object") {
    return null;
  }

  return {
    ...job,
    outputs: Array.isArray(job.outputs) ? job.outputs.map((output) => normalizePythonJobOutput(output)).filter(Boolean) : [],
    dataSources: Array.isArray(job.dataSources) ? job.dataSources : [],
    sourceTypes: Array.isArray(job.sourceTypes) ? job.sourceTypes : [],
  };
}

export function pythonJobIsRunning(job) {
  return Boolean(job && pythonJobRunningStatuses.has(job.status));
}

export function pythonJobElapsedMs(job) {
  if (!job) {
    return 0;
  }

  if (pythonJobIsRunning(job)) {
    const startedAtMs = Date.parse(job.startedAt || "");
    if (!Number.isNaN(startedAtMs)) {
      return Math.max(0, Date.now() - startedAtMs);
    }
  }

  return Number.isFinite(Number(job.durationMs)) ? Math.max(0, Number(job.durationMs)) : 0;
}

export function pythonJobStatusCopy(job) {
  if (!job) {
    return "Idle";
  }

  switch (job.status) {
    case "queued":
      return "Queued";
    case "running":
      return "Running";
    case "completed":
      return "Completed";
    case "cancelled":
      return "Cancelled";
    case "failed":
      return "Failed";
    default:
      return "Idle";
  }
}

export function createPythonJobState({ getPythonJobsSnapshot, workspaceNotebookId }) {
  function pythonJobForCell(notebookId, cellId) {
    if (!notebookId || !cellId) {
      return null;
    }

    return getPythonJobsSnapshot().find((job) => job.notebookId === notebookId && job.cellId === cellId) ?? null;
  }

  function pythonJobById(jobId) {
    const normalizedJobId = String(jobId || "").trim();
    if (!normalizedJobId) {
      return null;
    }

    return getPythonJobsSnapshot().find((job) => job.jobId === normalizedJobId) ?? null;
  }

  function pythonJobForResultActionTarget(target) {
    if (!(target instanceof Element)) {
      return null;
    }

    const resultRoot = target.closest("[data-cell-result]");
    const jobId =
      target.dataset.resultJobId ||
      resultRoot?.dataset.pythonJobId ||
      resultRoot?.querySelector("[data-python-duration]")?.dataset.jobId ||
      "";
    const directJob = pythonJobById(jobId);
    if (directJob) {
      return directJob;
    }

    const cellId = target.closest("[data-query-cell]")?.dataset.cellId || "";
    const notebookId = workspaceNotebookId(target.closest("[data-workspace-notebook]"));
    return pythonJobForCell(notebookId, cellId);
  }

  return {
    pythonJobById,
    pythonJobForCell,
    pythonJobForResultActionTarget,
  };
}

export async function loadPythonJobsState({
  applyPythonJobsState,
  fetchImpl = (...args) => window.fetch(...args),
}) {
  const response = await fetchImpl("/api/python-jobs", {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to load python jobs: ${response.status}`);
  }

  applyPythonJobsState(await response.json());
}

export function applyOptimisticPythonJobSnapshot({
  snapshot,
  getPythonState,
  applyPythonJobsState,
  incrementRunningCount = false,
}) {
  if (!snapshot) {
    return;
  }

  const currentState = getPythonState();
  const currentSummary = currentState.summary ?? { runningCount: 0, totalCount: 0 };

  applyPythonJobsState({
    version: currentState.version,
    summary: incrementRunningCount
      ? {
          ...currentSummary,
          runningCount: currentSummary.runningCount + 1,
        }
      : currentSummary,
    jobs: [snapshot, ...(currentState.snapshot ?? []).filter((job) => job.jobId !== snapshot.jobId)],
  });
}

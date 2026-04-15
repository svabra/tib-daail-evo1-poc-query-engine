const queryJobRunningStatuses = new Set(["queued", "running"]);

export function normalizeQueryJob(job) {
  if (!job || typeof job !== "object") {
    return null;
  }

  const firstRowMs = Number(job.firstRowMs);
  const fetchMs = Number(job.fetchMs);

  return {
    ...job,
    columns: Array.isArray(job.columns) ? job.columns : [],
    rows: Array.isArray(job.rows) ? job.rows : [],
    dataSources: Array.isArray(job.dataSources) ? job.dataSources : [],
    sourceTypes: Array.isArray(job.sourceTypes) ? job.sourceTypes : [],
    touchedRelations: Array.isArray(job.touchedRelations)
      ? job.touchedRelations.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [],
    touchedBuckets: Array.isArray(job.touchedBuckets)
      ? job.touchedBuckets.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [],
    firstRowMs: Number.isFinite(firstRowMs) ? Math.max(0, firstRowMs) : null,
    fetchMs: Number.isFinite(fetchMs) ? Math.max(0, fetchMs) : null,
  };
}

export function queryJobIsRunning(job) {
  return Boolean(job && queryJobRunningStatuses.has(job.status));
}

export function queryJobStatusCopy(job) {
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

export function queryJobElapsedMs(job) {
  if (!job) {
    return 0;
  }

  if (queryJobIsRunning(job)) {
    const startedAtMs = Date.parse(job.startedAt || "");
    if (!Number.isNaN(startedAtMs)) {
      return Math.max(0, Date.now() - startedAtMs);
    }
  }

  return Number.isFinite(Number(job.durationMs)) ? Math.max(0, Number(job.durationMs)) : 0;
}

export function formatQueryDuration(durationMs) {
  let remaining = Math.max(0, Math.round(Number.isFinite(Number(durationMs)) ? Number(durationMs) : 0));
  const units = [
    ["d", 24 * 60 * 60 * 1000],
    ["h", 60 * 60 * 1000],
    ["m", 60 * 1000],
    ["s", 1000],
  ];
  const parts = [];
  let started = false;

  for (const [suffix, size] of units) {
    const value = Math.floor(remaining / size);
    remaining -= value * size;
    if (value > 0 || started) {
      parts.push(`${value}${suffix}`);
      started = true;
    }
  }

  if (!parts.length) {
    return `${remaining} ms`;
  }

  parts.push(`${remaining} ms`);
  return parts.join(" ");
}

export function compareQueryJobsByCompletedAt(left, right) {
  const leftCompletedAt = Date.parse(left?.completedAt || left?.updatedAt || left?.startedAt || "");
  const rightCompletedAt = Date.parse(right?.completedAt || right?.updatedAt || right?.startedAt || "");

  if (!Number.isNaN(leftCompletedAt) || !Number.isNaN(rightCompletedAt)) {
    const normalizedLeft = Number.isNaN(leftCompletedAt) ? 0 : leftCompletedAt;
    const normalizedRight = Number.isNaN(rightCompletedAt) ? 0 : rightCompletedAt;
    if (normalizedLeft !== normalizedRight) {
      return normalizedLeft - normalizedRight;
    }
  }

  return String(left?.jobId || "").localeCompare(String(right?.jobId || ""));
}

export function createQueryJobState({ getQueryJobsSnapshot, workspaceNotebookId }) {
  function queryJobForCell(notebookId, cellId) {
    if (!notebookId || !cellId) {
      return null;
    }

    return getQueryJobsSnapshot().find((job) => job.notebookId === notebookId && job.cellId === cellId) ?? null;
  }

  function queryJobById(jobId) {
    const normalizedJobId = String(jobId || "").trim();
    if (!normalizedJobId) {
      return null;
    }

    return getQueryJobsSnapshot().find((job) => job.jobId === normalizedJobId) ?? null;
  }

  function queryJobForResultActionTarget(target) {
    if (!(target instanceof Element)) {
      return null;
    }

    const resultRoot = target.closest("[data-cell-result]");
    const jobId =
      target.dataset.resultJobId ||
      resultRoot?.dataset.queryJobId ||
      resultRoot?.querySelector("[data-query-duration]")?.dataset.jobId ||
      "";
    const directJob = queryJobById(jobId);
    if (directJob) {
      return directJob;
    }

    const cellId = target.closest("[data-query-cell]")?.dataset.cellId || "";
    const notebookId = workspaceNotebookId(target.closest("[data-workspace-notebook]"));
    return queryJobForCell(notebookId, cellId);
  }

  return {
    queryJobById,
    queryJobForCell,
    queryJobForResultActionTarget,
  };
}

export async function loadQueryJobsState({
  applyQueryJobsState,
  fetchImpl = (...args) => window.fetch(...args),
}) {
  const response = await fetchImpl("/api/query-jobs", {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to load query jobs: ${response.status}`);
  }

  applyQueryJobsState(await response.json());
}

export function applyOptimisticQueryJobSnapshot({
  snapshot,
  getQueryState,
  applyQueryJobsState,
  incrementRunningCount = false,
}) {
  if (!snapshot) {
    return;
  }

  const currentState = getQueryState();
  const currentSummary = currentState.summary ?? { runningCount: 0, totalCount: 0 };

  applyQueryJobsState({
    version: currentState.version,
    summary: incrementRunningCount
      ? {
          ...currentSummary,
          runningCount: currentSummary.runningCount + 1,
        }
      : currentSummary,
    jobs: [snapshot, ...(currentState.snapshot ?? []).filter((job) => job.jobId !== snapshot.jobId)],
    performance: currentState.performance ?? { recent: [], stats: {} },
  });
}
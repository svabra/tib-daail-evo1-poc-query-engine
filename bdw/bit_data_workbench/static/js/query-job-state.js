const queryJobRunningStatuses = new Set(["queued", "running"]);

export function normalizeQueryJob(job) {
  if (!job || typeof job !== "object") {
    return null;
  }

  const firstRowMs = Number(job.firstRowMs);
  const fetchMs = Number(job.fetchMs);
  const progress = Number(job.progress);
  const processId = Number(job.processId);
  const cpuPercent = Number(job.cpuPercent);
  const averageCpuPercent = Number(job.averageCpuPercent);
  const peakCpuPercent = Number(job.peakCpuPercent);
  const cpuCapacityPercent = Number(job.cpuCapacityPercent);
  const averageCpuCapacityPercent = Number(job.averageCpuCapacityPercent);
  const peakCpuCapacityPercent = Number(job.peakCpuCapacityPercent);
  const cpuCapacityCores = Number(job.cpuCapacityCores);
  const memoryRssBytes = Number(job.memoryRssBytes);
  const averageMemoryRssBytes = Number(job.averageMemoryRssBytes);
  const peakMemoryRssBytes = Number(job.peakMemoryRssBytes);
  const processThreadCount = Number(job.processThreadCount);
  const peakProcessThreadCount = Number(job.peakProcessThreadCount);
  const duckdbThreadLimit = Number(job.duckdbThreadLimit);
  const duckdbSpillBytes = Number(job.duckdbSpillBytes);
  const duckdbSpillPeakBytes = Number(job.duckdbSpillPeakBytes);
  const duckdbSpillTotalBytes = Number(job.duckdbSpillTotalBytes);
  const duckdbSpillOtherBytes = Number(job.duckdbSpillOtherBytes);
  const duckdbSpillLimitBytes = Number(job.duckdbSpillLimitBytes);
  const duckdbSpillDiskFreeBytes = Number(job.duckdbSpillDiskFreeBytes);
  const workerExitCode = Number(job.workerExitCode);
  const timings = {};
  if (job.timings && typeof job.timings === "object") {
    Object.entries(job.timings).forEach(([key, value]) => {
      const numeric = Number(value);
      if (key && Number.isFinite(numeric) && numeric >= 0) {
        timings[key] = numeric;
      }
    });
  }

  return {
    ...job,
    columns: Array.isArray(job.columns) ? job.columns : [],
    rows: Array.isArray(job.rows) ? job.rows : [],
    warnings: Array.isArray(job.warnings)
      ? job.warnings.map((warning) => String(warning ?? "").trim()).filter(Boolean)
      : [],
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
    progress: Number.isFinite(progress) ? Math.max(0, Math.min(1, progress)) : null,
    executionMode: String(job.executionMode ?? "").trim(),
    duckdbExecutionPath: String(job.duckdbExecutionPath ?? "").trim(),
    processId: Number.isFinite(processId) && processId > 0 ? Math.round(processId) : null,
    cpuPercent: Number.isFinite(cpuPercent) ? Math.max(0, cpuPercent) : null,
    averageCpuPercent: Number.isFinite(averageCpuPercent) ? Math.max(0, averageCpuPercent) : null,
    peakCpuPercent: Number.isFinite(peakCpuPercent) ? Math.max(0, peakCpuPercent) : null,
    cpuCapacityPercent: Number.isFinite(cpuCapacityPercent) ? Math.max(0, cpuCapacityPercent) : null,
    averageCpuCapacityPercent: Number.isFinite(averageCpuCapacityPercent)
      ? Math.max(0, averageCpuCapacityPercent)
      : null,
    peakCpuCapacityPercent: Number.isFinite(peakCpuCapacityPercent)
      ? Math.max(0, peakCpuCapacityPercent)
      : null,
    cpuCapacityCores: Number.isFinite(cpuCapacityCores) && cpuCapacityCores > 0 ? cpuCapacityCores : null,
    memoryRssBytes: Number.isFinite(memoryRssBytes) ? Math.max(0, Math.round(memoryRssBytes)) : null,
    averageMemoryRssBytes: Number.isFinite(averageMemoryRssBytes)
      ? Math.max(0, Math.round(averageMemoryRssBytes))
      : null,
    peakMemoryRssBytes: Number.isFinite(peakMemoryRssBytes)
      ? Math.max(0, Math.round(peakMemoryRssBytes))
      : null,
    processThreadCount: Number.isFinite(processThreadCount)
      ? Math.max(0, Math.round(processThreadCount))
      : null,
    peakProcessThreadCount: Number.isFinite(peakProcessThreadCount)
      ? Math.max(0, Math.round(peakProcessThreadCount))
      : null,
    duckdbThreadLimit: Number.isFinite(duckdbThreadLimit) && duckdbThreadLimit > 0
      ? Math.round(duckdbThreadLimit)
      : null,
    duckdbSpillBytes: Number.isFinite(duckdbSpillBytes) ? Math.max(0, Math.round(duckdbSpillBytes)) : null,
    duckdbSpillPeakBytes: Number.isFinite(duckdbSpillPeakBytes)
      ? Math.max(0, Math.round(duckdbSpillPeakBytes))
      : null,
    duckdbSpillTotalBytes: Number.isFinite(duckdbSpillTotalBytes)
      ? Math.max(0, Math.round(duckdbSpillTotalBytes))
      : null,
    duckdbSpillOtherBytes: Number.isFinite(duckdbSpillOtherBytes)
      ? Math.max(0, Math.round(duckdbSpillOtherBytes))
      : null,
    duckdbSpillLimitBytes: Number.isFinite(duckdbSpillLimitBytes)
      ? Math.max(0, Math.round(duckdbSpillLimitBytes))
      : null,
    duckdbSpillDiskFreeBytes: Number.isFinite(duckdbSpillDiskFreeBytes)
      ? Math.max(0, Math.round(duckdbSpillDiskFreeBytes))
      : null,
    resourceSamples: Array.isArray(job.resourceSamples)
      ? job.resourceSamples
          .map((sample) => {
            const elapsedMs = Number(sample?.elapsedMs);
            const sampleCpu = Number(sample?.cpuPercent);
            const sampleAverageCpu = Number(sample?.averageCpuPercent);
            const sampleCpuCapacity = Number(sample?.cpuCapacityPercent);
            const sampleAverageCpuCapacity = Number(sample?.averageCpuCapacityPercent);
            const sampleMemory = Number(sample?.memoryRssBytes);
            const sampleAverageMemory = Number(sample?.averageMemoryRssBytes);
            const sampleProcessThreadCount = Number(sample?.processThreadCount);
            const sampleDuckdbThreadLimit = Number(sample?.duckdbThreadLimit);
            const sampleDuckdbSpill = Number(sample?.duckdbSpillBytes);
            const sampleDuckdbSpillTotal = Number(sample?.duckdbSpillTotalBytes);
            const sampleDuckdbSpillOther = Number(sample?.duckdbSpillOtherBytes);
            const sampleDuckdbSpillLimit = Number(sample?.duckdbSpillLimitBytes);
            const sampleDuckdbSpillDiskFree = Number(sample?.duckdbSpillDiskFreeBytes);
            return {
              elapsedMs: Number.isFinite(elapsedMs) ? Math.max(0, elapsedMs) : 0,
              cpuPercent: Number.isFinite(sampleCpu) ? Math.max(0, sampleCpu) : null,
              averageCpuPercent: Number.isFinite(sampleAverageCpu) ? Math.max(0, sampleAverageCpu) : null,
              cpuCapacityPercent: Number.isFinite(sampleCpuCapacity) ? Math.max(0, sampleCpuCapacity) : null,
              averageCpuCapacityPercent: Number.isFinite(sampleAverageCpuCapacity)
                ? Math.max(0, sampleAverageCpuCapacity)
                : null,
              memoryRssBytes: Number.isFinite(sampleMemory) ? Math.max(0, Math.round(sampleMemory)) : null,
              averageMemoryRssBytes: Number.isFinite(sampleAverageMemory)
                ? Math.max(0, Math.round(sampleAverageMemory))
                : null,
              processThreadCount: Number.isFinite(sampleProcessThreadCount)
                ? Math.max(0, Math.round(sampleProcessThreadCount))
                : null,
              duckdbThreadLimit: Number.isFinite(sampleDuckdbThreadLimit) && sampleDuckdbThreadLimit > 0
                ? Math.round(sampleDuckdbThreadLimit)
                : null,
              duckdbSpillBytes: Number.isFinite(sampleDuckdbSpill)
                ? Math.max(0, Math.round(sampleDuckdbSpill))
                : null,
              duckdbSpillTotalBytes: Number.isFinite(sampleDuckdbSpillTotal)
                ? Math.max(0, Math.round(sampleDuckdbSpillTotal))
                : null,
              duckdbSpillOtherBytes: Number.isFinite(sampleDuckdbSpillOther)
                ? Math.max(0, Math.round(sampleDuckdbSpillOther))
                : null,
              duckdbSpillLimitBytes: Number.isFinite(sampleDuckdbSpillLimit)
                ? Math.max(0, Math.round(sampleDuckdbSpillLimit))
                : null,
              duckdbSpillDiskFreeBytes: Number.isFinite(sampleDuckdbSpillDiskFree)
                ? Math.max(0, Math.round(sampleDuckdbSpillDiskFree))
                : null,
            };
          })
          .filter((sample) =>
            sample.cpuPercent !== null ||
            sample.memoryRssBytes !== null ||
            sample.processThreadCount !== null ||
            sample.duckdbSpillBytes !== null ||
            sample.duckdbSpillTotalBytes !== null
          )
      : [],
    cancellationPhase: String(job.cancellationPhase ?? "").trim(),
    cancellationRequestedAt: String(job.cancellationRequestedAt ?? "").trim(),
    workerExitCode: Number.isFinite(workerExitCode) ? Math.round(workerExitCode) : null,
    timings,
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

  const timings = job.timings && typeof job.timings === "object" ? job.timings : {};
  const elapsedCandidates = [];
  for (const key of ["clientObservedMs", "clientTotalMs", "backendTotalMs"]) {
    const timing = Number(timings[key]);
    if (Number.isFinite(timing) && timing >= 0) {
      elapsedCandidates.push(timing);
    }
  }

  if (Array.isArray(job.resourceSamples)) {
    job.resourceSamples
      .map((sample) => Number(sample?.elapsedMs))
      .filter((value) => Number.isFinite(value) && value >= 0)
      .forEach((value) => elapsedCandidates.push(value));
  }

  if (Number.isFinite(Number(job.durationMs))) {
    elapsedCandidates.push(Math.max(0, Number(job.durationMs)));
  }

  if (elapsedCandidates.length) {
    return Math.max(0, ...elapsedCandidates);
  }

  return 0;
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

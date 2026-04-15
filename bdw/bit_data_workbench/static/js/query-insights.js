export function createQueryInsights({
  compareQueryJobsByCompletedAt,
  formatQueryDuration,
  normalizeDataSources,
  sourceLabelsForIds,
}) {
  function formatRelativePercent(percentValue) {
    const absoluteValue = Math.abs(Number.isFinite(Number(percentValue)) ? Number(percentValue) : 0);
    if (absoluteValue >= 10) {
      return `${Math.round(absoluteValue)}%`;
    }
    if (absoluteValue >= 1) {
      return `${absoluteValue.toFixed(1)}%`;
    }
    return `${absoluteValue.toFixed(2)}%`;
  }

  function medianDuration(values) {
    const normalizedValues = (values ?? [])
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value))
      .sort((left, right) => left - right);
    if (!normalizedValues.length) {
      return null;
    }

    const midpoint = Math.floor(normalizedValues.length / 2);
    if (normalizedValues.length % 2 === 1) {
      return normalizedValues[midpoint];
    }
    return (normalizedValues[midpoint - 1] + normalizedValues[midpoint]) / 2;
  }

  function queryJobComparisonKey(job) {
    const sourceKey = normalizeDataSources(job?.dataSources ?? [])
      .slice()
      .sort()
      .join("||");
    return [
      String(job?.notebookId || "").trim(),
      String(job?.cellId || "").trim(),
      String(job?.backendName || "").trim(),
      sourceKey,
    ].join("::");
  }

  function queryComparisonTone(deltaMs) {
    const normalizedDelta = Number.isFinite(Number(deltaMs)) ? Number(deltaMs) : 0;
    if (normalizedDelta <= -0.5) {
      return "faster";
    }
    if (normalizedDelta >= 0.5) {
      return "slower";
    }
    return "neutral";
  }

  function buildQueryComparisonInsight(label, baselineMs, deltaMs, tooltip) {
    if (!Number.isFinite(Number(baselineMs)) || Number(baselineMs) <= 0 || !Number.isFinite(Number(deltaMs))) {
      return null;
    }

    const tone = queryComparisonTone(deltaMs);
    const normalizedDelta = Number(deltaMs);
    const percentDelta = (normalizedDelta / Number(baselineMs)) * 100;
    const value = tone === "neutral" ? "no material change" : `${formatRelativePercent(percentDelta)} ${tone}`;

    return {
      label,
      value,
      tone,
      title: tooltip,
    };
  }

  function buildQueryJobComparisonMetrics(job, history) {
    const durationMs = Number(job?.durationMs);
    if (!Number.isFinite(durationMs) || durationMs <= 0) {
      return null;
    }

    const previousJob = history.length ? history[history.length - 1] : null;
    const previous =
      previousJob && Number.isFinite(Number(previousJob.durationMs))
        ? buildQueryComparisonInsight(
            "vs previous",
            Number(previousJob.durationMs),
            durationMs - Number(previousJob.durationMs),
            (() => {
              const deltaMs = durationMs - Number(previousJob.durationMs);
              const tone = queryComparisonTone(deltaMs);
              if (tone === "neutral") {
                return `Essentially unchanged versus the previous comparable run (${formatQueryDuration(previousJob.durationMs)}).`;
              }
              return `${formatQueryDuration(Math.abs(deltaMs))} ${tone} than the previous comparable run (${formatQueryDuration(
                previousJob.durationMs
              )}). Comparable means the same notebook cell, backend, and selected source set.`;
            })()
          )
        : null;

    const medianWindow = history
      .slice(-5)
      .map((entry) => Number(entry.durationMs))
      .filter((value) => Number.isFinite(value) && value > 0);
    const rollingMedianMs = medianWindow.length >= 2 ? medianDuration(medianWindow) : null;
    const median =
      Number.isFinite(rollingMedianMs)
        ? buildQueryComparisonInsight(
            "vs median",
            rollingMedianMs,
            durationMs - rollingMedianMs,
            (() => {
              const deltaMs = durationMs - rollingMedianMs;
              const tone = queryComparisonTone(deltaMs);
              if (tone === "neutral") {
                return `Essentially unchanged versus the rolling median of the last ${medianWindow.length} comparable completed runs (${formatQueryDuration(
                  rollingMedianMs
                )}).`;
              }
              return `${formatQueryDuration(Math.abs(deltaMs))} ${tone} than the rolling median of the last ${medianWindow.length} comparable completed runs (${formatQueryDuration(
                rollingMedianMs
              )}).`;
            })()
          )
        : null;

    if (!previous && !median) {
      return null;
    }

    return { previous, median };
  }

  function buildQueryJobFootprint(job) {
    const touchedRelations = [];
    const seenRelations = new Set();
    for (const value of job?.touchedRelations ?? []) {
      const relationName = String(value ?? "").trim();
      const relationKey = relationName.toLowerCase();
      if (!relationName || seenRelations.has(relationKey)) {
        continue;
      }
      seenRelations.add(relationKey);
      touchedRelations.push(relationName);
    }

    const touchedBuckets = [];
    const seenBuckets = new Set();
    for (const value of job?.touchedBuckets ?? []) {
      const bucketName = String(value ?? "").trim();
      const bucketKey = bucketName.toLowerCase();
      if (!bucketName || seenBuckets.has(bucketKey)) {
        continue;
      }
      seenBuckets.add(bucketKey);
      touchedBuckets.push(bucketName);
    }

    const selectedSources = normalizeDataSources(job?.dataSources ?? []);
    const parts = [];
    if (touchedRelations.length) {
      parts.push(`${touchedRelations.length} relation${touchedRelations.length === 1 ? "" : "s"}`);
    }
    if (selectedSources.length) {
      parts.push(`${selectedSources.length} source${selectedSources.length === 1 ? "" : "s"}`);
    }
    if (touchedBuckets.length) {
      parts.push(`${touchedBuckets.length} bucket${touchedBuckets.length === 1 ? "" : "s"}`);
    }

    if (!parts.length) {
      return null;
    }

    const tooltipSections = [];
    if (touchedRelations.length) {
      tooltipSections.push(`Touched relations: ${touchedRelations.join(", ")}`);
    }
    const sourceLabels = sourceLabelsForIds(selectedSources);
    if (sourceLabels.length) {
      tooltipSections.push(`Selected sources: ${sourceLabels.join(", ")}`);
    }
    if (touchedBuckets.length) {
      tooltipSections.push(`S3 buckets: ${touchedBuckets.join(", ")}`);
    }

    return {
      label: "touches",
      value: parts.join(" | "),
      tone: "neutral",
      title: tooltipSections.join("\n"),
    };
  }

  function buildQueryJobTimingInsight(job) {
    const firstRowMs = Number(job?.firstRowMs);
    if (!Number.isFinite(firstRowMs) || firstRowMs < 0) {
      return null;
    }

    const fetchMs = Number(job?.fetchMs);
    const valueParts = [`first row ${formatQueryDuration(firstRowMs)}`];
    if (Number.isFinite(fetchMs) && fetchMs >= 0) {
      valueParts.push(`fetch ${formatQueryDuration(fetchMs)}`);
    }

    return {
      label: "split",
      value: valueParts.join(" | "),
      tone: "neutral",
      title:
        "Breaks the runtime into time until the first row was available and the remaining time spent fetching result rows for the UI.",
    };
  }

  function decorateQueryJobsWithInsights(jobs) {
    const historyByKey = new Map();
    const comparisonById = new Map();
    const completedJobs = jobs
      .filter((job) => job?.status === "completed")
      .slice()
      .sort(compareQueryJobsByCompletedAt);

    completedJobs.forEach((job) => {
      const comparisonKey = queryJobComparisonKey(job);
      const history = historyByKey.get(comparisonKey) ?? [];
      comparisonById.set(job.jobId, buildQueryJobComparisonMetrics(job, history));
      history.push(job);
      if (history.length > 12) {
        history.shift();
      }
      historyByKey.set(comparisonKey, history);
    });

    return jobs.map((job) => ({
      ...job,
      comparisonInsights: comparisonById.get(job.jobId) ?? null,
      footprintInsights: buildQueryJobFootprint(job),
      timingInsights: buildQueryJobTimingInsight(job),
    }));
  }

  return { decorateQueryJobsWithInsights };
}
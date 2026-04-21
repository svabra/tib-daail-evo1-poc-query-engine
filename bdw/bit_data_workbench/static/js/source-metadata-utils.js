export function readSourceOptions() {
  const node = document.getElementById("source-options");
  if (!node?.textContent) {
    return [];
  }

  try {
    const parsed = JSON.parse(node.textContent);
    return Array.isArray(parsed) ? parsed.filter((item) => item?.source_id && item?.label) : [];
  } catch (_error) {
    return [];
  }
}

export function normalizeDataSources(sources) {
  const options = readSourceOptions();
  const knownSourceIds = new Set(options.map((option) => option.source_id));
  const uniqueSources = [];
  const seen = new Set();

  for (const value of sources ?? []) {
    const sourceId = String(value ?? "").trim();
    if (!sourceId || seen.has(sourceId)) {
      continue;
    }
    if (knownSourceIds.size > 0 && !knownSourceIds.has(sourceId)) {
      continue;
    }
    seen.add(sourceId);
    uniqueSources.push(sourceId);
  }

  return uniqueSources;
}

export function parseDefaultDataSources(value) {
  if (!value) {
    return [];
  }

  return normalizeDataSources(String(value).split("||"));
}

export function sourceIdFromLegacyTargetLabel(value) {
  const targetLabel = String(value ?? "").trim();
  if (!targetLabel) {
    return null;
  }

  const option = readSourceOptions().find(
    (candidate) =>
      candidate.source_id === targetLabel ||
      candidate.label.toLowerCase() === targetLabel.toLowerCase()
  );
  return option?.source_id ?? null;
}

function sourceOptionForId(sourceId) {
  return readSourceOptions().find((option) => option.source_id === sourceId) ?? null;
}

function sourceLabelForId(sourceId) {
  return sourceOptionForId(sourceId)?.label ?? sourceId;
}

function sourceClassificationForId(sourceId) {
  return sourceOptionForId(sourceId)?.classification ?? "Internal";
}

function sourceComputationModeForId(sourceId) {
  return sourceOptionForId(sourceId)?.computation_mode ?? "VMTP";
}

function sourceStorageTooltipForId(sourceId) {
  return sourceOptionForId(sourceId)?.storage_tooltip ?? "";
}

export function sourceLabelsForIds(sourceIds) {
  return normalizeDataSources(sourceIds).map((sourceId) => sourceLabelForId(sourceId));
}

export function sourceClassificationForIds(sourceIds) {
  const selectedSourceIds = normalizeDataSources(sourceIds);
  if (!selectedSourceIds.length) {
    return "NA";
  }

  const classifications = [...new Set(selectedSourceIds.map((sourceId) => sourceClassificationForId(sourceId)))];
  return classifications.length === 1 ? classifications[0] : "Mixed";
}

export function sourceComputationModeForIds(sourceIds) {
  const selectedSourceIds = normalizeDataSources(sourceIds);
  if (!selectedSourceIds.length) {
    return "NA";
  }

  const computationModes = [...new Set(selectedSourceIds.map((sourceId) => sourceComputationModeForId(sourceId)))];
  return computationModes.length === 1 ? computationModes[0] : "Mixed";
}

export function sourceClassificationDisplayText(dataSources) {
  return `Classification: ${sourceClassificationForIds(dataSources)}`;
}

export function sourceComputationModeDisplayText(dataSources) {
  return `Processing Mode: ${sourceComputationModeForIds(dataSources)}`;
}

export function sourceStorageTooltipForIds(sourceIds) {
  const selectedSourceIds = normalizeDataSources(sourceIds);
  if (!selectedSourceIds.length) {
    return "";
  }

  if (selectedSourceIds.length === 1) {
    return sourceStorageTooltipForId(selectedSourceIds[0]);
  }

  return "Selected sources span multiple storage locations.";
}

export function sourceComputationModeTooltipText() {
  return [
    "MPP = Massive Parallel Processing. Distributed query execution across multiple workers and partitions for larger-scale data processing.",
    "VMTP = Vectorized Multi-Threaded Processing. Single-node vectorized execution across multiple CPU threads for fast local analytical queries.",
    "PostgreSQL Native = Direct execution by the PostgreSQL planner and executor, without DuckDB in the query path.",
  ].join("\n");
}

export function accessModeForDataSources(sourceIds) {
  return normalizeDataSources(sourceIds).length > 1 ? "Read / Query only" : "Read / Write";
}

export function accessModeHintForDataSources(sourceIds) {
  return normalizeDataSources(sourceIds).length > 1
    ? "Multiple selected sources keep this cell in query-only mode."
    : "A single selected source keeps this cell read/write capable.";
}

export function normalizeSourceObjectFields(fields) {
  if (!Array.isArray(fields)) {
    return [];
  }

  return fields
    .map((field) => ({
      name: typeof field?.name === "string" ? field.name.trim() : "",
      dataType: typeof field?.dataType === "string" ? field.dataType.trim() : "UNKNOWN",
    }))
    .filter((field) => field.name);
}

export function sourceObjectDisplayName(sourceObjectRoot) {
  return (
    sourceObjectRoot?.dataset.sourceObjectDisplayName?.trim() ||
    sourceObjectRoot?.dataset.sourceObjectName?.trim() ||
    sourceObjectRoot?.dataset.sourceObjectRelation?.trim() ||
    "Selected source"
  );
}

export function sourceObjectS3DownloadDescriptor(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    return null;
  }

  const downloadable = String(sourceObjectRoot.dataset.s3Downloadable || "").trim().toLowerCase() === "true";
  const bucket = String(sourceObjectRoot.dataset.s3Bucket || "").trim();
  const key = String(sourceObjectRoot.dataset.s3Key || "").trim();
  if (!downloadable || !bucket || !key) {
    return null;
  }

  const path = String(sourceObjectRoot.dataset.s3Path || "").trim();
  const keySegments = key.split("/").filter(Boolean);
  return {
    bucket,
    key,
    path,
    fileName: keySegments[keySegments.length - 1] || sourceObjectDisplayName(sourceObjectRoot),
  };
}

export function sourceObjectS3DeleteDescriptor(sourceObjectRoot) {
  const descriptor = sourceObjectS3DownloadDescriptor(sourceObjectRoot);
  if (!descriptor) {
    return null;
  }

  return {
    entryKind: "file",
    name: descriptor.fileName,
    bucket: descriptor.bucket,
    prefix: descriptor.key,
    path: descriptor.path || `s3://${descriptor.bucket}/${descriptor.key}`,
    fileFormat: String(sourceObjectRoot?.dataset.s3FileFormat || "").trim(),
  };
}

export function sourceSchemaS3BucketDescriptor(sourceSchemaRoot) {
  if (!(sourceSchemaRoot instanceof Element)) {
    return null;
  }

  const bucket = String(sourceSchemaRoot.dataset.sourceBucket || "").trim();
  if (!bucket) {
    return null;
  }

  return {
    entryKind: "bucket",
    name: bucket,
    bucket,
    prefix: "",
    path: `s3://${bucket}/`,
    fileFormat: "",
  };
}

export function dataProductSourceDescriptorFromSourceSchema(sourceSchemaRoot) {
  const descriptor = sourceSchemaS3BucketDescriptor(sourceSchemaRoot);
  if (!descriptor) {
    return null;
  }

  return {
    sourceKind: "bucket",
    sourceId: "workspace.s3",
    bucket: descriptor.bucket,
    sourceDisplayName: descriptor.name,
    sourcePlatform: "s3",
  };
}

function sqlQueryIdentifier(name) {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    return name;
  }

  return `"${String(name).replace(/"/g, '""')}"`;
}

export function sourceObjectDisplayKind(sourceObjectRoot) {
  const localWorkspaceFormat = String(
    sourceObjectRoot?.dataset.localWorkspaceExportFormat || ""
  )
    .trim()
    .toUpperCase();
  if (localWorkspaceFormat) {
    return `${localWorkspaceFormat} FILE`;
  }

  const s3FileFormat = String(sourceObjectRoot?.dataset.s3FileFormat || "")
    .trim()
    .toUpperCase();
  if (s3FileFormat) {
    return `${s3FileFormat} FILE`;
  }

  return sourceObjectRoot?.dataset.sourceObjectKind?.trim()?.toUpperCase() || "TABLE";
}

export function sourceQuerySql(relation, fields = []) {
  const fieldNames = normalizeSourceObjectFields(fields).map((field) => field.name);
  if (!fieldNames.length) {
    return `SELECT * FROM ${relation};`;
  }

  return [
    "SELECT",
    fieldNames
      .map(
        (fieldName, index) =>
          `  ${sqlQueryIdentifier(fieldName)}${index < fieldNames.length - 1 ? "," : ""}`
      )
      .join("\n"),
    `FROM ${relation};`,
  ].join("\n");
}

export function sourceQueryDescriptor(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    return null;
  }

  const relation = sourceObjectRoot.dataset.sourceObjectRelation?.trim();
  if (!relation) {
    return null;
  }

  return {
    name: sourceObjectDisplayName(sourceObjectRoot),
    relation,
    sourceId: sourceObjectRoot.dataset.sourceOptionId?.trim() || "",
  };
}

export function dataProductSourceDescriptorFromSourceObject(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    return null;
  }

  const sourceId =
    sourceObjectRoot.dataset.sourceOptionId?.trim() ||
    sourceIdFromLegacyTargetLabel(sourceObjectRoot.dataset.sourceOptionId?.trim()) ||
    "";
  const relation = sourceObjectRoot.dataset.sourceObjectRelation?.trim() || "";
  const sourceDisplayName = sourceObjectDisplayName(sourceObjectRoot);
  const s3Bucket = sourceObjectRoot.dataset.s3Bucket?.trim() || "";
  const s3Key = sourceObjectRoot.dataset.s3Key?.trim() || "";
  const s3Downloadable =
    String(sourceObjectRoot.dataset.s3Downloadable || "").trim().toLowerCase() === "true";

  if (sourceId === "workspace.local") {
    return {
      sourceKind: "local-object",
      sourceId: "workspace.local",
      relation,
      sourceDisplayName,
      sourcePlatform: "indexeddb",
      unsupportedReason:
        "Live publication requires a server-visible source; move this file to Shared Workspace first.",
    };
  }

  if (sourceId === "workspace.s3" && s3Downloadable && s3Bucket && s3Key) {
    return {
      sourceKind: "object",
      sourceId: "workspace.s3",
      bucket: s3Bucket,
      key: s3Key,
      sourceDisplayName,
      sourcePlatform: "s3",
    };
  }

  if (!relation) {
    return null;
  }

  return {
    sourceKind: "relation",
    sourceId:
      sourceId ||
      (relation.startsWith("pg_oltp.")
        ? "pg_oltp"
        : relation.startsWith("pg_olap.")
          ? "pg_olap"
          : "workspace.s3"),
    relation,
    sourceDisplayName,
    sourcePlatform: sourceId === "workspace.s3" ? "s3" : "postgres",
  };
}

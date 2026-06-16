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
    if (
      knownSourceIds.size > 0 &&
      !knownSourceIds.has(sourceId) &&
      !sourceId.toLowerCase().startsWith("s3.")
    ) {
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

function isS3ObjectSourceId(sourceId) {
  return String(sourceId || "").trim().toLowerCase().startsWith("s3.");
}

function sourceLabelForId(sourceId) {
  return sourceOptionForId(sourceId)?.label ?? sourceId;
}

function sourceClassificationForId(sourceId) {
  return sourceOptionForId(sourceId)?.classification ?? (isS3ObjectSourceId(sourceId) ? "Workspace Storage" : "Internal");
}

function sourceComputationModeForId(sourceId) {
  return sourceOptionForId(sourceId)?.computation_mode ?? "VMTP";
}

function sourceStorageTooltipForId(sourceId) {
  return sourceOptionForId(sourceId)?.storage_tooltip ?? (isS3ObjectSourceId(sourceId) ? "Stored in S3 Object Storage." : "");
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
  const path = String(sourceObjectRoot.dataset.s3Path || "").trim();
  let bucket = String(sourceObjectRoot.dataset.s3Bucket || "").trim();
  const key = String(sourceObjectRoot.dataset.s3Key || "").trim();
  if (!isValidS3BucketName(bucket) && path.startsWith("s3://")) {
    try {
      const parsedPath = new URL(path);
      if (isValidS3BucketName(parsedPath.hostname)) {
        bucket = parsedPath.hostname;
      }
    } catch (_error) {
      // Keep the original bucket so the backend can return a precise validation error.
    }
  }
  if (!downloadable || !bucket || !key) {
    return null;
  }

  const keySegments = key.split("/").filter(Boolean);
  const displayName = sourceObjectDisplayName(sourceObjectRoot);
  const keyFileName = keySegments[keySegments.length - 1] || "";
  return {
    bucket,
    key,
    path,
    fileFormat: String(sourceObjectRoot.dataset.s3FileFormat || "").trim().toLowerCase(),
    sizeBytes: Number(sourceObjectRoot.dataset.s3SizeBytes || 0) || 0,
    fileName: displayName.includes(".") ? displayName : keyFileName || displayName || "s3-object",
  };
}

export function sourceObjectS3GeneratedDownloadDescriptor(sourceObjectRoot, mode = "merged") {
  if (!(sourceObjectRoot instanceof Element)) {
    return null;
  }

  const downloadKind = String(sourceObjectRoot.dataset.s3DownloadKind || "").trim();
  const path = String(sourceObjectRoot.dataset.s3Path || "").trim();
  let bucket = String(sourceObjectRoot.dataset.s3Bucket || "").trim();
  if (!isValidS3BucketName(bucket) && path.startsWith("s3://")) {
    try {
      const parsedPath = new URL(path);
      if (isValidS3BucketName(parsedPath.hostname)) {
        bucket = parsedPath.hostname;
      }
    } catch (_error) {
      // Keep the original bucket so the backend can return a precise validation error.
    }
  }
  const prefix = String(sourceObjectRoot.dataset.s3PartPrefix || "").trim();
  const fileFormat = String(
    sourceObjectRoot.dataset.s3PartFileFormat ||
      sourceObjectRoot.dataset.s3FileFormat ||
      ""
  ).trim();
  const normalizedMode = String(mode || "").trim().toLowerCase() || "merged";
  if (downloadKind !== "generated_parts" || !bucket || !prefix || !fileFormat) {
    return null;
  }

  const mergeDownloadable =
    String(sourceObjectRoot.dataset.s3MergeDownloadable || "").trim().toLowerCase() === "true";
  const zipDownloadable =
    String(sourceObjectRoot.dataset.s3ZipDownloadable || "").trim().toLowerCase() === "true";
  if (normalizedMode === "merged" && !mergeDownloadable) {
    return null;
  }
  if (normalizedMode === "zip" && !zipDownloadable) {
    return null;
  }
  if (!["merged", "zip"].includes(normalizedMode)) {
    return null;
  }

  const configuredFileName = String(sourceObjectRoot.dataset.s3DownloadFilename || "").trim();
  const displayName = sourceObjectDisplayName(sourceObjectRoot);
  return {
    bucket,
    prefix,
    fileFormat,
    mode: normalizedMode,
    fileName: configuredFileName || displayName || "generated-parts",
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

export function sourceObjectDdlDescriptor(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    return null;
  }

  const relation = sourceObjectRoot.dataset.sourceObjectRelation?.trim() || "";
  const sourceId = sourceObjectRoot.dataset.sourceOptionId?.trim() || "";
  const bucket = sourceObjectRoot.dataset.s3Bucket?.trim() || "";
  const key = sourceObjectRoot.dataset.s3Key?.trim() || "";
  const objectName = sourceObjectDisplayName(sourceObjectRoot);
  const fileFormat =
    sourceObjectRoot.dataset.s3FileFormat?.trim() ||
    sourceObjectRoot.dataset.localWorkspaceExportFormat?.trim() ||
    "";
  const localWorkspaceEntryId = sourceObjectRoot.dataset.localWorkspaceEntryId?.trim() || "";

  if (!relation && !(bucket && key) && !localWorkspaceEntryId) {
    return null;
  }

  const baseName = objectName.replace(/\.[^.]+$/, "") || "source-ddl";
  return {
    relation,
    sourceId,
    bucket,
    key,
    objectName,
    fileFormat,
    localWorkspaceEntryId,
    fileName: `${baseName}.sql`,
  };
}

const S3_BUCKET_NAME_PATTERN = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const S3_BUCKET_CREATE_NAME_PATTERN = /^[a-z][a-z0-9.-]{1,61}[a-z0-9]$/;
export const S3_BUCKET_NAME_VALIDATION_MESSAGE =
  "Bucket names must be 3-63 characters and use lowercase letters, numbers, dots, or hyphens.";
export const S3_BUCKET_CREATE_VALIDATION_MESSAGE =
  "Bucket names must normalize to 3-63 characters, start with a lowercase letter, and use lowercase letters, numbers, dots, or hyphens.";

export function normalizeS3BucketNameInput(value) {
  return String(value ?? "").trim().toLowerCase();
}

export function isValidS3BucketName(value) {
  return S3_BUCKET_NAME_PATTERN.test(normalizeS3BucketNameInput(value));
}

export function normalizeS3BucketNameForCreate(value) {
  let bucket = normalizeS3BucketNameInput(value)
    .replace(/[^a-z0-9.-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/\.+/g, ".")
    .replace(/^[.-]+|[.-]+$/g, "");
  if (/^[0-9]/.test(bucket)) {
    bucket = `bdw-${bucket}`;
  }
  if (bucket.length > 63) {
    bucket = bucket.slice(0, 63).replace(/[.-]+$/g, "");
  }
  if (!S3_BUCKET_CREATE_NAME_PATTERN.test(bucket)) {
    throw new Error(S3_BUCKET_CREATE_VALIDATION_MESSAGE);
  }
  return bucket;
}

function normalizedS3BucketName(value) {
  const bucket = normalizeS3BucketNameInput(value);
  return isValidS3BucketName(bucket) ? bucket : "";
}

function sourceSchemaObjectBucket(sourceSchemaRoot) {
  const buckets = new Set(
    Array.from(sourceSchemaRoot.querySelectorAll("[data-source-object][data-s3-bucket]"))
      .map((node) => normalizedS3BucketName(node.dataset.s3Bucket))
      .filter(Boolean)
  );
  return buckets.size === 1 ? Array.from(buckets)[0] : "";
}

export function sourceSchemaS3BucketDescriptor(sourceSchemaRoot) {
  if (!(sourceSchemaRoot instanceof Element)) {
    return null;
  }

  const bucket =
    normalizedS3BucketName(sourceSchemaRoot.dataset.sourceBucket) ||
    sourceSchemaObjectBucket(sourceSchemaRoot);
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
    sourceId: "s3",
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

function s3RelationPrefixFromIdentifier(value) {
  const relation = String(value || "").trim();
  const lower = relation.toLowerCase();
  if (lower.startsWith("s3.")) {
    return "s3";
  }
  if (lower.startsWith("s3.")) {
    return "s3";
  }
  return "";
}

function s3RelationWithSingleFileFallback(sourceObjectRoot, relation) {
  const relationText = String(relation || "").trim();
  if (!relationText.includes("*") && !relationText.includes("?")) {
    return relationText;
  }

  const partCountValue = String(sourceObjectRoot.dataset.s3PartCount || "").trim();
  const partCount = Number(partCountValue);
  if (!Number.isFinite(partCount) || partCount !== 1) {
    return relationText;
  }

  const bucket = String(sourceObjectRoot.dataset.s3Bucket || "").trim();
  const key = String(sourceObjectRoot.dataset.s3Key || "").trim();
  const relationPrefix = s3RelationPrefixFromIdentifier(relationText);
  if (!bucket || !key || !relationPrefix) {
    return relationText;
  }

  return `${relationPrefix}.${sqlQueryIdentifier(bucket)}.${sqlQueryIdentifier(key)}`;
}

export function sourceQueryDescriptor(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    return null;
  }

  const physicalRelation = sourceObjectRoot.dataset.sourceObjectRelation?.trim() || "";
  const queryAlias = sourceObjectRoot.dataset.sourceObjectQueryAlias?.trim() || "";
  const queryReference = sourceObjectRoot.dataset.sourceObjectQueryReference?.trim() || "";
  const relation = s3RelationWithSingleFileFallback(
    sourceObjectRoot,
    queryReference || queryAlias || physicalRelation
  );
  if (!relation) {
    return null;
  }

  return {
    name: sourceObjectDisplayName(sourceObjectRoot),
    relation,
    physicalRelation,
    queryReference,
    queryAlias,
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

  if (sourceId === "s3" && s3Downloadable && s3Bucket && s3Key) {
    return {
      sourceKind: "object",
      sourceId: "s3",
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
          : "s3"),
    relation,
    sourceDisplayName,
    sourcePlatform: sourceId === "s3" ? "s3" : "postgres",
  };
}

const RESULT_EXPORT_FORMATS = new Set(["csv", "json", "jsonl", "parquet", "xml", "xlsx"]);

function normalizedText(value) {
  return String(value ?? "").trim();
}

function s3LocationFromPath(path) {
  const normalizedPath = normalizedText(path);
  const match = normalizedPath.match(/^s3:\/\/([^/?#]+)\/(.+)$/i);
  if (!match || /[?#]/.test(match[2])) {
    return null;
  }
  return {
    bucket: match[1],
    key: match[2].replace(/^\/+/, ""),
    path: normalizedPath,
  };
}

export function resultStorageExportTarget(job) {
  const storage = job?.resultStorage;
  if (!storage || typeof storage !== "object" || Array.isArray(storage) || storage.enabled === false) {
    return null;
  }

  const pathLocation = s3LocationFromPath(storage.path);
  const bucket = normalizedText(storage.bucket) || pathLocation?.bucket || "";
  const key = (normalizedText(storage.key) || pathLocation?.key || "").replace(/^\/+/, "");
  if (!bucket || !key || key.endsWith("/")) {
    return null;
  }

  const segments = key.split("/").filter(Boolean);
  const fileName = segments.pop() || "";
  if (!fileName) {
    return null;
  }

  const extension = fileName.includes(".") ? fileName.split(".").pop().toLowerCase() : "";
  const configuredFormat = normalizedText(storage.format).toLowerCase();
  const exportFormat = RESULT_EXPORT_FORMATS.has(configuredFormat)
    ? configuredFormat
    : RESULT_EXPORT_FORMATS.has(extension)
      ? extension
      : "";
  if (!exportFormat) {
    return null;
  }

  const prefix = segments.length ? `${segments.join("/")}/` : "";
  return {
    bucket,
    prefix,
    fileName,
    exportFormat,
    path: pathLocation?.path || `s3://${bucket}/${key}`,
  };
}

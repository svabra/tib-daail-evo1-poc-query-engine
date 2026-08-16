function normalizedText(value) {
  return String(value || "").trim();
}

const CSV_S3_OBJECT_FORMATS = Object.freeze({
  ".csv": "csv",
  ".jsonl": "json",
  ".parquet": "parquet",
});

function csvS3ObjectStorageFormat(objectName = "") {
  const normalizedObjectName = normalizedText(objectName).toLowerCase();
  return Object.entries(CSV_S3_OBJECT_FORMATS).find(([suffix]) =>
    normalizedObjectName.endsWith(suffix)
  )?.[1] || "";
}

export function parseCsvS3LocationInput(value) {
  const rawValue = normalizedText(value);
  if (!rawValue || /[\u0000-\u001f\u007f\\?#%]/.test(rawValue)) {
    return null;
  }

  const explicitScheme = /^[a-z][a-z0-9+.-]*:\/\//i.exec(rawValue)?.[0] || "";
  if (explicitScheme && explicitScheme.toLowerCase() !== "s3://") {
    return null;
  }
  const pathValue = explicitScheme ? rawValue.slice(explicitScheme.length) : rawValue;
  if (!explicitScheme && !pathValue.includes("/")) {
    return null;
  }

  const trailingSlash = pathValue.endsWith("/");
  const segments = pathValue
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean);
  const bucket = String(segments.shift() || "").toLowerCase();
  if (!bucket || !segments.length) {
    return null;
  }

  const candidateObjectName = trailingSlash ? "" : segments[segments.length - 1];
  const storageFormat = csvS3ObjectStorageFormat(candidateObjectName);
  const objectName = storageFormat ? String(segments.pop() || "") : "";
  const keyPrefix = segments.join("/");
  if (!objectName && !keyPrefix) {
    return null;
  }

  return {
    bucket,
    keyPrefix,
    objectName,
    objectKey: objectName
      ? keyPrefix
        ? `${keyPrefix}/${objectName}`
        : objectName
      : "",
    storageFormat,
  };
}

export function buildCsvS3Uri(bucket = "", objectKey = "") {
  const normalizedBucket = normalizedText(bucket) || "<bucket>";
  const normalizedObjectKey = normalizedText(objectKey)
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean)
    .join("/");
  return `s3://${normalizedBucket}/${normalizedObjectKey}`;
}

export function deriveCsvS3ObjectKeyPrefix(objectKey = "", storedFileName = "") {
  const normalizedObjectKey = normalizedText(objectKey);
  const normalizedStoredFileName = normalizedText(storedFileName);
  if (!normalizedObjectKey) {
    return "";
  }
  if (
    normalizedStoredFileName &&
    normalizedObjectKey !== normalizedStoredFileName &&
    normalizedObjectKey.endsWith(`/${normalizedStoredFileName}`)
  ) {
    return normalizedObjectKey.slice(
      0,
      normalizedObjectKey.length - normalizedStoredFileName.length - 1
    );
  }
  const segments = normalizedObjectKey.split("/").filter(Boolean);
  if (segments.length <= 1) {
    return "";
  }
  segments.pop();
  return segments.join("/");
}

export function resolveCsvS3LocationDetails({
  bucket = "",
  prefix = "",
  objectName = "",
  objectKey = "",
  storedFileName = "",
} = {}) {
  const normalizedBucket = normalizedText(bucket);
  const normalizedObjectName =
    normalizedText(objectName) || normalizedText(storedFileName) || "csv-import.csv";
  const normalizedObjectKey = normalizedText(objectKey);
  const normalizedPrefix =
    normalizedText(prefix) ||
    deriveCsvS3ObjectKeyPrefix(normalizedObjectKey, normalizedObjectName);

  const resolvedObjectKey =
    normalizedObjectKey ||
    (normalizedPrefix
      ? `${normalizedPrefix}/${normalizedObjectName}`
      : normalizedObjectName);

  return {
    bucket: normalizedBucket,
    keyPrefix: normalizedPrefix,
    objectName: normalizedObjectName,
    objectKey: resolvedObjectKey,
    uri: buildCsvS3Uri(normalizedBucket, resolvedObjectKey),
  };
}

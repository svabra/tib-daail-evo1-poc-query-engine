function normalizedText(value) {
  return String(value || "").trim();
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

  return {
    bucket: normalizedBucket,
    keyPrefix: normalizedPrefix,
    objectName: normalizedObjectName,
    objectKey:
      normalizedObjectKey ||
      (normalizedPrefix
        ? `${normalizedPrefix}/${normalizedObjectName}`
        : normalizedObjectName),
  };
}

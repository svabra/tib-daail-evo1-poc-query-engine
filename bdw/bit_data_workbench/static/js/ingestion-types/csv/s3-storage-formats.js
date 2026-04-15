const CSV_S3_STORAGE_FORMATS = {
  csv: {
    value: "csv",
    label: "Plain CSV",
    reviewLabel: "stored as plain CSV",
    description: "Best when you want the raw file preserved and easy to inspect outside the workbench.",
    tooltip:
      "Pros: raw, human-readable, easiest to exchange with other tools.\n" +
      "Cons: slowest repeated analytics, larger scans, delimiter and header settings remain important.\n" +
      "DuckDB query path: read_csv_auto(...).",
  },
  parquet: {
    value: "parquet",
    label: "Parquet",
    reviewLabel: "stored as Parquet",
    description: "Best default for repeated analytics and larger datasets because columnar reads are smaller and faster.",
    tooltip:
      "Pros: fastest analytical scans, columnar compression, strongest default for DuckDB workloads.\n" +
      "Cons: not human-readable and costs a conversion step during ingestion.\n" +
      "DuckDB query path: read_parquet(...).",
  },
  json: {
    value: "json",
    label: "JSON",
    reviewLabel: "stored as JSON",
    description: "Best when downstream consumers want schemaless records or APIs expect JSON-shaped payloads.",
    tooltip:
      "Pros: easy to integrate with JSON-first tools and services.\n" +
      "Cons: less efficient than Parquet for analytics and larger than compressed columnar storage.\n" +
      "DuckDB query path: read_json_auto(...).",
  },
};

export function normalizeCsvS3StorageFormat(value) {
  const normalizedValue = String(value || "").trim().toLowerCase() || "csv";
  return CSV_S3_STORAGE_FORMATS[normalizedValue] ? normalizedValue : "csv";
}

export function csvS3StorageFormatDefinition(value) {
  return CSV_S3_STORAGE_FORMATS[normalizeCsvS3StorageFormat(value)];
}

export function resolveCsvS3StoredFileName(fileName, storageFormat) {
  const normalizedStorageFormat = normalizeCsvS3StorageFormat(storageFormat);
  const normalizedFileName = String(fileName || "").trim() || "csv-import.csv";
  if (normalizedStorageFormat === "csv") {
    return normalizedFileName;
  }

  const stem = normalizedFileName.replace(/\.[^.]+$/, "") || "csv_import";
  return `${stem}.${normalizedStorageFormat}`;
}

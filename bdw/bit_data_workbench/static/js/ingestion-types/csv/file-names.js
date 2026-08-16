import {
  normalizeCsvS3StorageFormat,
  resolveCsvS3StoredFileName,
} from "./s3-storage-formats.js";

const DEFAULT_CSV_IMPORT_BASE_NAME = "csv-import";

function cleanCsvImportBaseName(value) {
  return String(value || "")
    .replace(/[\\/]+/g, " ")
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .trim();
}

function stripSourceFileExtension(value) {
  return String(value || "").replace(/\.[^.]+$/, "");
}

function stripKnownDestinationExtension(value) {
  return String(value || "").replace(/\.(?:csv|jsonl|parquet)$/i, "");
}

export function csvImportBaseNameFromFileName(fileName) {
  const normalizedFileName =
    String(fileName || "")
      .split(/[\\/]/)
      .pop()
      ?.trim() || "";
  return stripSourceFileExtension(cleanCsvImportBaseName(normalizedFileName)) || DEFAULT_CSV_IMPORT_BASE_NAME;
}

export function normalizeCsvImportBaseName(value, fallbackFileName = "") {
  return (
    stripKnownDestinationExtension(cleanCsvImportBaseName(value)) ||
    csvImportBaseNameFromFileName(fallbackFileName) ||
    DEFAULT_CSV_IMPORT_BASE_NAME
  );
}

export function resolveCsvSourceUploadFileName(baseName, fallbackFileName = "") {
  return `${normalizeCsvImportBaseName(baseName, fallbackFileName)}.csv`;
}

export function resolveCsvDestinationFileName(
  baseName,
  {
    targetId = "workspace.local",
    storageFormat = "csv",
    fallbackFileName = "",
  } = {}
) {
  const sourceUploadFileName = resolveCsvSourceUploadFileName(baseName, fallbackFileName);
  if (String(targetId || "").trim() !== "s3") {
    return sourceUploadFileName;
  }
  return resolveCsvS3StoredFileName(
    sourceUploadFileName,
    normalizeCsvS3StorageFormat(storageFormat)
  );
}

export function csvImportNameFieldLabel(targetId = "workspace.local") {
  switch (String(targetId || "").trim()) {
    case "s3":
      return "Object name";
    case "workspace.local":
      return "Stored file name";
    default:
      return "Import file name";
  }
}

export function csvImportNameSuffix(
  targetId = "workspace.local",
  storageFormat = "csv"
) {
  if (String(targetId || "").trim() === "s3") {
    const normalizedStorageFormat = normalizeCsvS3StorageFormat(storageFormat);
    return normalizedStorageFormat === "json" ? ".jsonl" : `.${normalizedStorageFormat}`;
  }
  return ".csv";
}

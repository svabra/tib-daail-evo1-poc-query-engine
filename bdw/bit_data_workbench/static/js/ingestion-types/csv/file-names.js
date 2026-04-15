import {
  normalizeCsvS3StorageFormat,
  resolveCsvS3StoredFileName,
} from "./s3-storage-formats.js";

const DEFAULT_CSV_IMPORT_BASE_NAME = "csv-import";

function sanitizeCsvImportBaseName(value) {
  return String(value || "")
    .replace(/[\\/]+/g, " ")
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\.[^.]+$/, "")
    .trim();
}

export function csvImportBaseNameFromFileName(fileName) {
  const normalizedFileName =
    String(fileName || "")
      .split(/[\\/]/)
      .pop()
      ?.trim() || "";
  return sanitizeCsvImportBaseName(normalizedFileName) || DEFAULT_CSV_IMPORT_BASE_NAME;
}

export function normalizeCsvImportBaseName(value, fallbackFileName = "") {
  return (
    sanitizeCsvImportBaseName(value) ||
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
  if (String(targetId || "").trim() !== "workspace.s3") {
    return sourceUploadFileName;
  }
  return resolveCsvS3StoredFileName(
    sourceUploadFileName,
    normalizeCsvS3StorageFormat(storageFormat)
  );
}

export function csvImportNameFieldLabel(targetId = "workspace.local") {
  switch (String(targetId || "").trim()) {
    case "workspace.s3":
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
  if (String(targetId || "").trim() === "workspace.s3") {
    return `.${normalizeCsvS3StorageFormat(storageFormat)}`;
  }
  return ".csv";
}

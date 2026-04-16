const RESULT_EXPORT_FORMATS = {
  csv: {
    value: "csv",
    label: "CSV",
    extension: "csv",
    description: "Best for broad interoperability and spreadsheet-friendly handoff.",
  },
  json: {
    value: "json",
    label: "JSON Array",
    extension: "json",
    description: "Best when downstream tools expect one JSON array document.",
  },
  jsonl: {
    value: "jsonl",
    label: "JSONL",
    extension: "jsonl",
    description: "Best for streaming-style JSON records and DuckDB-friendly line-delimited processing.",
  },
  parquet: {
    value: "parquet",
    label: "Parquet",
    extension: "parquet",
    description: "Best for analytical storage and repeated workbench queries.",
  },
  xml: {
    value: "xml",
    label: "XML",
    extension: "xml",
    description: "Best for XML-oriented integration payloads and legacy exchange formats.",
  },
  xlsx: {
    value: "xlsx",
    label: "Excel",
    extension: "xlsx",
    description: "Best for business handoff into Excel and spreadsheet workflows.",
  },
};

export function normalizeResultExportFormat(value) {
  const normalizedValue = String(value || "").trim().toLowerCase() || "csv";
  return RESULT_EXPORT_FORMATS[normalizedValue] ? normalizedValue : "csv";
}

export function resultExportFormatDefinition(value) {
  return RESULT_EXPORT_FORMATS[normalizeResultExportFormat(value)];
}

export function resultExportFormatDefinitions() {
  return Object.values(RESULT_EXPORT_FORMATS);
}

export function resultExportFormatLabel(value) {
  return resultExportFormatDefinition(value).label;
}

export function ensureResultExportFileNameExtension(fileName, format, fallbackBase = "query-result") {
  const definition = resultExportFormatDefinition(format);
  const extension = definition.extension;
  const normalizedCandidate = String(fileName || "").trim();
  const stem = normalizedCandidate.replace(/\.[^.]+$/, "").trim() || fallbackBase;
  return `${stem}.${extension}`;
}

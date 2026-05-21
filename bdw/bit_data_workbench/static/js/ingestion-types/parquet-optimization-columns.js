function normalizedName(value) {
  return String(value || "").trim().toLowerCase().replace(/[\s-]+/g, "_");
}

function uniqueByName(options) {
  const seen = new Set();
  const result = [];
  options.forEach((option) => {
    const name = String(option?.name || "").trim();
    if (!name) {
      return;
    }
    const key = name.toLowerCase();
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    result.push({
      name,
      dataType: String(option?.dataType || "").trim(),
    });
  });
  return result;
}

export function normalizeColumnOptions(columns) {
  if (!Array.isArray(columns)) {
    return [];
  }
  return uniqueByName(
    columns.map((column) => {
      if (typeof column === "string") {
        return { name: column, dataType: "" };
      }
      return {
        name: column?.name || column?.columnName || "",
        dataType: column?.dataType || column?.type || "",
      };
    })
  );
}

export function recommendedPartitionColumns(columnOptions) {
  const match = normalizeColumnOptions(columnOptions).find((column) => {
    const name = normalizedName(column.name);
    return name.includes("year") || name.includes("jahr");
  });
  return match ? [match.name] : [];
}

function isDateLike(columnName) {
  const name = normalizedName(columnName);
  return (
    name.includes("date") ||
    name.includes("datum") ||
    name.includes("timestamp") ||
    name.includes("time")
  );
}

function isIdLike(columnName) {
  const name = normalizedName(columnName);
  return name === "id" || name === "id_" || name.endsWith("_id");
}

export function recommendedSortColumns(columnOptions) {
  const options = normalizeColumnOptions(columnOptions);
  const selected = [];
  options.forEach((column) => {
    if (selected.length < 2 && isDateLike(column.name)) {
      selected.push(column.name);
    }
  });
  options.forEach((column) => {
    if (selected.length < 2 && isIdLike(column.name) && !selected.includes(column.name)) {
      selected.push(column.name);
    }
  });
  return selected;
}

export function recommendedIndexColumns(columnOptions, createDuckdbCache = false) {
  if (!createDuckdbCache) {
    return [];
  }
  const match = normalizeColumnOptions(columnOptions).find((column) => {
    const name = normalizedName(column.name);
    return name === "id" || name === "id_";
  });
  return match ? [match.name] : [];
}

export function filterAvailableColumns(selectedColumns, columnOptions) {
  const available = new Map(
    normalizeColumnOptions(columnOptions).map((column) => [column.name.toLowerCase(), column.name])
  );
  const result = [];
  (Array.isArray(selectedColumns) ? selectedColumns : []).forEach((column) => {
    const availableName = available.get(String(column || "").trim().toLowerCase());
    if (availableName && !result.includes(availableName)) {
      result.push(availableName);
    }
  });
  return result;
}

export function columnBasisCopy({ fileName = "", sourceKind = "" } = {}) {
  const normalizedFileName = String(fileName || "").trim();
  if (!normalizedFileName) {
    return "Column choices appear after a file preview is available.";
  }
  if (sourceKind === "zip-first-member") {
    return `Recommendations are based on first ZIP member: ${normalizedFileName}.`;
  }
  return `Considering columns from ${normalizedFileName}.`;
}

export function hivePartitioningCopy({ selectedColumns = [], fileName = "", sourceKind = "" } = {}) {
  const basis = columnBasisCopy({ fileName, sourceKind });
  const selected = Array.isArray(selectedColumns) ? selectedColumns.filter(Boolean) : [];
  if (!selected.length) {
    return `${basis} No partition columns selected; Hive partitioning will not be requested.`;
  }
  return `${basis} Hive partitioning would use ${selected.map((column) => `\`${column}\``).join(", ")}.`;
}

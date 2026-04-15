const CSV_PREVIEW_BYTE_LIMIT = 128 * 1024;
const CSV_PREVIEW_ROW_LIMIT = 5;

export function delimiterCharacterForMode(mode) {
  switch (String(mode || "").trim().toLowerCase()) {
    case "comma":
      return ",";
    case "semicolon":
      return ";";
    case "tab":
      return "\t";
    case "pipe":
      return "|";
    default:
      return "";
  }
}

export function delimiterLabelFromCharacter(delimiter) {
  switch (delimiter) {
    case ",":
      return "Comma";
    case ";":
      return "Semicolon";
    case "\t":
      return "Tab";
    case "|":
      return "Pipe";
    default:
      return "Auto-detect";
  }
}

function countDelimiterOutsideQuotes(line, delimiter) {
  let count = 0;
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    if (character === '"') {
      if (inQuotes && line[index + 1] === '"') {
        index += 1;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }
    if (!inQuotes && character === delimiter) {
      count += 1;
    }
  }

  return count;
}

export function detectCsvDelimiter(text) {
  const sampleLines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, 8);

  if (!sampleLines.length) {
    return ",";
  }

  const candidates = [",", ";", "\t", "|"];
  let bestDelimiter = ",";
  let bestScore = -1;

  candidates.forEach((candidate, index) => {
    const counts = sampleLines.map((line) => countDelimiterOutsideQuotes(line, candidate));
    const nonZeroCounts = counts.filter((count) => count > 0);
    if (!nonZeroCounts.length) {
      return;
    }

    const averageCount =
      nonZeroCounts.reduce((sum, count) => sum + count, 0) / nonZeroCounts.length;
    const spread = Math.max(...nonZeroCounts) - Math.min(...nonZeroCounts);
    const score = averageCount * 10 - spread - index * 0.01;
    if (score > bestScore) {
      bestScore = score;
      bestDelimiter = candidate;
    }
  });

  return bestDelimiter;
}

function parseCsvRows(text, delimiter, maxRows) {
  const rows = [];
  let currentRow = [];
  let currentField = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];

    if (character === '"') {
      if (inQuotes && text[index + 1] === '"') {
        currentField += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && character === delimiter) {
      currentRow.push(currentField);
      currentField = "";
      continue;
    }

    if (!inQuotes && character === "\n") {
      currentRow.push(currentField);
      currentField = "";
      if (currentRow.some((value) => String(value || "").length > 0) || currentRow.length > 1) {
        rows.push(currentRow);
      }
      currentRow = [];
      if (rows.length >= maxRows) {
        return rows;
      }
      continue;
    }

    if (!inQuotes && character === "\r") {
      continue;
    }

    currentField += character;
  }

  if (currentField.length || currentRow.length) {
    currentRow.push(currentField);
    if (currentRow.some((value) => String(value || "").length > 0) || currentRow.length > 1) {
      rows.push(currentRow);
    }
  }

  return rows.slice(0, maxRows);
}

function invalidShapeMessage(expectedWidth, actualWidth, lineNumber) {
  return (
    `CSV row width mismatch at line ${lineNumber}: expected ${expectedWidth} fields but found ` +
    `${actualWidth}. This usually means the delimiter is wrong or a value contains an unquoted delimiter.`
  );
}

function validateParsedRows(rows, hasHeader) {
  if (!Array.isArray(rows) || !rows.length) {
    return "";
  }

  const expectedWidth = Array.isArray(rows[0]) ? rows[0].length : 0;
  if (!expectedWidth) {
    return "";
  }

  const startIndex = hasHeader ? 1 : 0;
  for (let index = startIndex; index < rows.length; index += 1) {
    const row = Array.isArray(rows[index]) ? rows[index] : [];
    if (row.length !== expectedWidth) {
      return invalidShapeMessage(expectedWidth, row.length, index + 1);
    }
  }

  return "";
}

export async function buildCsvPreviewState(file, config) {
  const previewText = await file.slice(0, CSV_PREVIEW_BYTE_LIMIT).text();
  const delimiter = config.delimiter || detectCsvDelimiter(previewText);
  const parsedRows = parseCsvRows(
    previewText,
    delimiter || ",",
    config.hasHeader ? CSV_PREVIEW_ROW_LIMIT + 1 : CSV_PREVIEW_ROW_LIMIT
  );
  const validationError = validateParsedRows(parsedRows, config.hasHeader);
  if (validationError) {
    return {
      status: "error",
      fileName: file.name,
      delimiter: delimiter || ",",
      hasHeader: config.hasHeader,
      columns: [],
      rows: [],
      error: validationError,
    };
  }

  let columns = [];
  let sampleRows = [];
  if (config.hasHeader && parsedRows.length) {
    columns = parsedRows[0].map((value, index) =>
      String(value || "").trim() || `column_${index + 1}`
    );
    sampleRows = parsedRows.slice(1, CSV_PREVIEW_ROW_LIMIT + 1);
  } else {
    const maximumWidth = parsedRows.reduce(
      (width, row) => Math.max(width, Array.isArray(row) ? row.length : 0),
      0
    );
    columns = Array.from({ length: maximumWidth }, (_, index) => `column_${index + 1}`);
    sampleRows = parsedRows.slice(0, CSV_PREVIEW_ROW_LIMIT);
  }

  return {
    status: "ready",
    fileName: file.name,
    delimiter: delimiter || ",",
    hasHeader: config.hasHeader,
    columns,
    rows: sampleRows,
    error: "",
  };
}

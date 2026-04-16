import {
  normalizeResultExportFormat,
  resultExportFormatDefinition,
  resultExportFormatDefinitions,
} from "./export-format-definitions.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeBoolean(value, fallback = true) {
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = String(value ?? "").trim().toLowerCase();
  if (["true", "1", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["false", "0", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function normalizeCsvDelimiter(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (normalized === ";") {
    return ";";
  }
  if (normalized === "tab" || normalized === "\\t" || normalized === "\t") {
    return "\t";
  }
  if (normalized === "|") {
    return "|";
  }
  return ",";
}

function normalizeExcelSheetName(value) {
  const normalized = String(value ?? "")
    .replace(/[:\\/?*\[\]]+/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim()
    .replace(/^'+|'+$/g, "")
    .slice(0, 31);
  return normalized || "Results";
}

function normalizeXmlName(value, fallback) {
  let normalized = String(value ?? "").trim().replace(/[^a-zA-Z0-9_.-]+/g, "_");
  normalized = normalized.replace(/^[_\-.]+|[_\-.]+$/g, "");
  if (!normalized) {
    return fallback;
  }
  if (!/^[a-zA-Z_]/.test(normalized)) {
    normalized = `n_${normalized}`;
  }
  return normalized;
}

export function defaultResultExportSettings(format) {
  const normalizedFormat = normalizeResultExportFormat(format);
  if (normalizedFormat === "csv") {
    return {
      delimiter: ",",
      includeHeader: true,
    };
  }
  if (normalizedFormat === "xlsx") {
    return {
      sheetName: "Results",
      includeHeader: true,
      freezeHeader: true,
    };
  }
  if (normalizedFormat === "xml") {
    return {
      rootName: "results",
      rowName: "row",
      prettyPrint: true,
    };
  }
  return {};
}

export function normalizeResultExportSettings(format, rawSettings = {}) {
  const normalizedFormat = normalizeResultExportFormat(format);
  const settings =
    rawSettings && typeof rawSettings === "object" ? rawSettings : defaultResultExportSettings(normalizedFormat);

  if (normalizedFormat === "csv") {
    return {
      delimiter: normalizeCsvDelimiter(settings.delimiter),
      includeHeader: normalizeBoolean(settings.includeHeader, true),
    };
  }
  if (normalizedFormat === "xlsx") {
    return {
      sheetName: normalizeExcelSheetName(settings.sheetName),
      includeHeader: normalizeBoolean(settings.includeHeader, true),
      freezeHeader: normalizeBoolean(settings.freezeHeader, true),
    };
  }
  if (normalizedFormat === "xml") {
    return {
      rootName: normalizeXmlName(settings.rootName, "results"),
      rowName: normalizeXmlName(settings.rowName, "row"),
      prettyPrint: normalizeBoolean(settings.prettyPrint, true),
    };
  }
  return {};
}

export function resultExportFormatOptionsMarkup(selectedFormat) {
  const normalizedFormat = normalizeResultExportFormat(selectedFormat);
  return resultExportFormatDefinitions()
    .map((definition) => {
      const selected = definition.value === normalizedFormat ? ' selected' : "";
      return `<option value="${escapeHtml(definition.value)}"${selected}>${escapeHtml(
        definition.label
      )}</option>`;
    })
    .join("");
}

export function resultExportSettingsMarkup(format, rawSettings = {}) {
  const normalizedFormat = normalizeResultExportFormat(format);
  const definition = resultExportFormatDefinition(normalizedFormat);
  const settings = normalizeResultExportSettings(normalizedFormat, rawSettings);

  if (normalizedFormat === "csv") {
    const delimiterValue = String(settings.delimiter || ",");
    return `
      <p class="result-export-settings-copy">${escapeHtml(definition.description)}</p>
      <div class="result-export-settings-grid">
        <label class="result-export-field">
          <span class="result-export-field-label">Delimiter</span>
          <select class="modal-input" data-export-setting="delimiter">
            <option value=","${delimiterValue === "," ? " selected" : ""}>Comma (,)</option>
            <option value=";"${delimiterValue === ";" ? " selected" : ""}>Semicolon (;)</option>
            <option value="tab"${delimiterValue === "\t" ? " selected" : ""}>Tab</option>
            <option value="|"${delimiterValue === "|" ? " selected" : ""}>Pipe (|)</option>
          </select>
        </label>
        <label class="modal-toggle-option">
          <input class="modal-toggle-checkbox" type="checkbox" data-export-setting="includeHeader"${
            settings.includeHeader ? " checked" : ""
          }>
          <span class="modal-toggle-copy">Include a header row with column names.</span>
        </label>
      </div>
    `;
  }

  if (normalizedFormat === "xlsx") {
    return `
      <p class="result-export-settings-copy">${escapeHtml(definition.description)}</p>
      <div class="result-export-settings-grid">
        <label class="result-export-field">
          <span class="result-export-field-label">Worksheet name</span>
          <input
            class="modal-input"
            type="text"
            maxlength="31"
            value="${escapeHtml(settings.sheetName)}"
            data-export-setting="sheetName"
            autocomplete="off"
          >
        </label>
        <label class="modal-toggle-option">
          <input class="modal-toggle-checkbox" type="checkbox" data-export-setting="includeHeader"${
            settings.includeHeader ? " checked" : ""
          }>
          <span class="modal-toggle-copy">Include a header row with column names.</span>
        </label>
        <label class="modal-toggle-option">
          <input class="modal-toggle-checkbox" type="checkbox" data-export-setting="freezeHeader"${
            settings.freezeHeader ? " checked" : ""
          }>
          <span class="modal-toggle-copy">Freeze the header row for scrolling.</span>
        </label>
      </div>
    `;
  }

  if (normalizedFormat === "xml") {
    return `
      <p class="result-export-settings-copy">${escapeHtml(definition.description)}</p>
      <div class="result-export-settings-grid">
        <label class="result-export-field">
          <span class="result-export-field-label">Root element</span>
          <input
            class="modal-input"
            type="text"
            value="${escapeHtml(settings.rootName)}"
            data-export-setting="rootName"
            autocomplete="off"
          >
        </label>
        <label class="result-export-field">
          <span class="result-export-field-label">Row element</span>
          <input
            class="modal-input"
            type="text"
            value="${escapeHtml(settings.rowName)}"
            data-export-setting="rowName"
            autocomplete="off"
          >
        </label>
        <label class="modal-toggle-option">
          <input class="modal-toggle-checkbox" type="checkbox" data-export-setting="prettyPrint"${
            settings.prettyPrint ? " checked" : ""
          }>
          <span class="modal-toggle-copy">Pretty-print the XML output for readability.</span>
        </label>
      </div>
    `;
  }

  return `<p class="result-export-settings-copy">${escapeHtml(definition.description)}</p>`;
}

export function renderResultExportSettings(root, format, settings = {}) {
  const panel = root?.querySelector?.("[data-export-settings-panel]");
  if (!(panel instanceof Element)) {
    return;
  }
  panel.innerHTML = resultExportSettingsMarkup(format, settings);
}

export function readResultExportSettings(root, format) {
  const normalizedFormat = normalizeResultExportFormat(format);
  if (!(root instanceof Element)) {
    return defaultResultExportSettings(normalizedFormat);
  }

  if (normalizedFormat === "csv") {
    return normalizeResultExportSettings(normalizedFormat, {
      delimiter: root.querySelector('[data-export-setting="delimiter"]')?.value || ",",
      includeHeader:
        root.querySelector('[data-export-setting="includeHeader"]')?.checked !== false,
    });
  }

  if (normalizedFormat === "xlsx") {
    return normalizeResultExportSettings(normalizedFormat, {
      sheetName: root.querySelector('[data-export-setting="sheetName"]')?.value || "Results",
      includeHeader:
        root.querySelector('[data-export-setting="includeHeader"]')?.checked !== false,
      freezeHeader:
        root.querySelector('[data-export-setting="freezeHeader"]')?.checked !== false,
    });
  }

  if (normalizedFormat === "xml") {
    return normalizeResultExportSettings(normalizedFormat, {
      rootName: root.querySelector('[data-export-setting="rootName"]')?.value || "results",
      rowName: root.querySelector('[data-export-setting="rowName"]')?.value || "row",
      prettyPrint:
        root.querySelector('[data-export-setting="prettyPrint"]')?.checked !== false,
    });
  }

  return {};
}

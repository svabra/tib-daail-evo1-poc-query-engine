import { EditorView, basicSetup } from "../vendor/codemirror.bundle.mjs";
import { sql, PostgreSQL } from "../vendor/lang-sql.bundle.mjs";

const editorRegistry = new WeakMap();
const editorSizingRegistry = new WeakMap();
let draggedNotebook = null;
let restoreController = null;
let applyingNotebookState = false;
let activeCellId = null;
let activeSourceObjectRelation = null;
const sourceObjectFieldCache = new Map();
const sourceObjectFieldRequests = new Map();
let queryJobsStateVersion = null;
let queryJobsSnapshot = [];
let queryJobsSummary = { runningCount: 0, totalCount: 0 };
let queryPerformanceState = { recent: [], stats: {} };
let queryJobsEventSource = null;
let queryJobsClockHandle = null;

const notebookTreeStorageKey = "bdw.notebookTree.v1";
const notebookMetadataStorageKey = "bdw.notebookMeta.v1";
const lastNotebookStorageKey = "bdw.lastNotebook.v1";
const sidebarCollapsedStorageKey = "bdw.sidebarCollapsed.v1";
const unassignedFolderName = "Unassigned";
const localNotebookPrefix = "local-notebook-";
const localCellPrefix = "local-cell-";
const queryJobTerminalStatuses = new Set(["completed", "failed", "cancelled"]);
const queryJobRunningStatuses = new Set(["queued", "running"]);
const sqlFormatKeywordPhrases = [
  ["LEFT", "OUTER", "JOIN"],
  ["RIGHT", "OUTER", "JOIN"],
  ["FULL", "OUTER", "JOIN"],
  ["INSERT", "INTO"],
  ["DELETE", "FROM"],
  ["GROUP", "BY"],
  ["ORDER", "BY"],
  ["UNION", "ALL"],
  ["INNER", "JOIN"],
  ["LEFT", "JOIN"],
  ["RIGHT", "JOIN"],
  ["FULL", "JOIN"],
  ["CROSS", "JOIN"],
];
const sqlFormatKeywords = new Set([
  "ALL",
  "AND",
  "AS",
  "ASC",
  "BETWEEN",
  "BY",
  "CASE",
  "DELETE",
  "DESC",
  "DISTINCT",
  "ELSE",
  "END",
  "EXCEPT",
  "EXISTS",
  "FETCH",
  "FROM",
  "FULL",
  "GROUP",
  "HAVING",
  "ILIKE",
  "IN",
  "INNER",
  "INSERT",
  "INTERSECT",
  "INTO",
  "IS",
  "JOIN",
  "LEFT",
  "LIKE",
  "LIMIT",
  "NOT",
  "NULL",
  "OFFSET",
  "ON",
  "OR",
  "ORDER",
  "OUTER",
  "QUALIFY",
  "RETURNING",
  "RIGHT",
  "SELECT",
  "SET",
  "THEN",
  "UNION",
  "UPDATE",
  "USING",
  "VALUES",
  "WHEN",
  "WHERE",
  "WINDOW",
  "WITH",
]);
const sqlFormatClauseKeywords = new Set([
  "DELETE FROM",
  "EXCEPT",
  "FETCH",
  "FROM",
  "GROUP BY",
  "HAVING",
  "INSERT INTO",
  "INTERSECT",
  "LIMIT",
  "OFFSET",
  "ORDER BY",
  "QUALIFY",
  "RETURNING",
  "SELECT",
  "SET",
  "UNION",
  "UNION ALL",
  "UPDATE",
  "VALUES",
  "WHERE",
  "WINDOW",
  "WITH",
]);
const sqlFormatJoinKeywords = new Set([
  "CROSS JOIN",
  "FULL JOIN",
  "FULL OUTER JOIN",
  "INNER JOIN",
  "JOIN",
  "LEFT JOIN",
  "LEFT OUTER JOIN",
  "RIGHT JOIN",
  "RIGHT OUTER JOIN",
]);
const sqlFormatBreakAfterKeywords = new Set([
  "GROUP BY",
  "HAVING",
  "ORDER BY",
  "QUALIFY",
  "RETURNING",
  "SELECT",
  "SET",
  "VALUES",
  "WHERE",
]);
const sqlFormatListKeywords = new Set([
  "GROUP BY",
  "ORDER BY",
  "RETURNING",
  "SELECT",
  "SET",
  "VALUES",
]);
const sqlFormatLogicalClauses = new Set(["HAVING", "ON", "USING", "WHERE"]);

function folderNameDialog() {
  return document.querySelector("[data-folder-name-dialog]");
}

function confirmDialog() {
  return document.querySelector("[data-confirm-dialog]");
}

function readSchema() {
  const element = document.getElementById("sql-schema");
  if (!element) {
    return {};
  }

  try {
    return JSON.parse(element.textContent ?? "{}");
  } catch (_error) {
    return {};
  }
}

function notebookSection() {
  return document.querySelector("[data-notebook-section]");
}

function dataSourcesSection() {
  return document.querySelector("[data-data-sources-section]");
}

function notebookTreeRoot() {
  return document.querySelector("[data-notebook-tree]");
}

function notebookFolders() {
  return Array.from(document.querySelectorAll("[data-tree-folder]"));
}

function dataSourceNodes() {
  return Array.from(document.querySelectorAll("[data-source-catalog], [data-source-schema]"));
}

function sourceObjectNodes() {
  return Array.from(document.querySelectorAll("[data-source-object]"));
}

function sourceInspector() {
  return document.querySelector("[data-source-inspector]");
}

function sourceInspectorPanel() {
  return document.querySelector("[data-source-inspector-panel]");
}

function queryMonitorList() {
  return document.querySelector("[data-query-monitor-list]");
}

function queryMonitorCount() {
  return document.querySelector("[data-query-monitor-count]");
}

function sidebarQueryCount() {
  return document.querySelector("[data-sidebar-query-count]");
}

function queryPerformanceSection() {
  return document.querySelector("[data-query-performance]");
}

function queryPerformanceStats() {
  return document.querySelector("[data-query-performance-stats]");
}

function queryPerformanceChart() {
  return document.querySelector("[data-query-performance-chart]");
}

function queryNotificationMenu() {
  return document.querySelector("[data-query-notifications]");
}

function queryNotificationList() {
  return document.querySelector("[data-query-notification-list]");
}

function queryNotificationCount() {
  return document.querySelector("[data-query-notification-count]");
}

function sidebarToggle() {
  return document.querySelector("[data-sidebar-toggle]");
}

function currentActiveNotebookId() {
  return document.querySelector(".notebook-link.is-active")?.dataset.notebookId ?? null;
}

function workspaceNotebookId(root = document) {
  return (
    root.querySelector("input[name='notebook_id']")?.value ??
    root.querySelector("[data-notebook-meta]")?.dataset.notebookId ??
    null
  );
}

function activateNotebookLink(notebookId) {
  document.querySelectorAll(".notebook-link").forEach((link) => {
    link.classList.toggle("is-active", link.dataset.notebookId === notebookId);
  });
  renderQueryNotificationMenu();
}

function notebookLinks(notebookId) {
  return Array.from(document.querySelectorAll(".notebook-link[data-notebook-id]")).filter(
    (link) => link.dataset.notebookId === notebookId
  );
}

function isLocalNotebookId(notebookId) {
  return String(notebookId ?? "").startsWith(localNotebookPrefix);
}

function createCellId() {
  return `${localCellPrefix}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
}

function readLastNotebookId() {
  try {
    return window.localStorage.getItem(lastNotebookStorageKey);
  } catch (_error) {
    return null;
  }
}

function writeLastNotebookId(notebookId) {
  try {
    window.localStorage.setItem(lastNotebookStorageKey, notebookId);
  } catch (_error) {
    // Ignore persistence failures and keep the session functional.
  }
}

function readSidebarCollapsed() {
  try {
    return window.localStorage.getItem(sidebarCollapsedStorageKey) === "true";
  } catch (_error) {
    return false;
  }
}

function writeSidebarCollapsed(collapsed) {
  try {
    window.localStorage.setItem(sidebarCollapsedStorageKey, collapsed ? "true" : "false");
  } catch (_error) {
    // Ignore persistence failures and keep the UI usable.
  }
}

function applySidebarCollapsedState(collapsed) {
  document.body.classList.toggle("sidebar-collapsed", collapsed);

  const toggle = sidebarToggle();
  if (!toggle) {
    return;
  }

  toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
  toggle.title = collapsed ? "Expand navigation" : "Collapse navigation";

  const label = toggle.querySelector(".sidebar-toggle-label");
  if (label) {
    label.textContent = collapsed ? "Expand navigation" : "Collapse navigation";
  }
}

function initializeSidebarToggle() {
  applySidebarCollapsedState(readSidebarCollapsed());
}

function setNotebookTreeExpanded(expanded) {
  if (expanded) {
    notebookSection()?.setAttribute("open", "");
  }

  notebookFolders().forEach((folder) => {
    folder.open = expanded;
  });

  persistNotebookTree();
  applySidebarSearchFilter();
}

function setDataSourceTreeExpanded(expanded) {
  if (expanded) {
    dataSourcesSection()?.setAttribute("open", "");
  }

  dataSourceNodes().forEach((node) => {
    node.open = expanded;
  });

  applySidebarSearchFilter();
}

function closeDialog(dialog, returnValue = "") {
  if (!dialog) {
    return;
  }

  if (typeof dialog.close === "function") {
    dialog.close(returnValue);
  }
}

function showFolderNameDialog({ title, copy, submitLabel, initialValue = "" }) {
  const dialog = folderNameDialog();
  if (!dialog) {
    const fallback = window.prompt(copy, initialValue);
    return Promise.resolve(fallback ? fallback.trim() : null);
  }

  const form = dialog.querySelector("[data-folder-name-form]");
  const titleNode = dialog.querySelector("[data-folder-name-title]");
  const copyNode = dialog.querySelector("[data-folder-name-copy]");
  const input = dialog.querySelector("[data-folder-name-input]");
  const submit = dialog.querySelector("[data-folder-name-submit]");
  const cancel = dialog.querySelector("[data-modal-cancel]");

  titleNode.textContent = title;
  copyNode.textContent = copy;
  submit.textContent = submitLabel;
  input.value = initialValue;

  return new Promise((resolve) => {
    const teardown = () => {
      form.removeEventListener("submit", onSubmit);
      cancel?.removeEventListener("click", onCancel);
      dialog.removeEventListener("close", onClose);
    };

    const onSubmit = (event) => {
      event.preventDefault();
      closeDialog(dialog, "confirm");
    };

    const onCancel = () => closeDialog(dialog, "cancel");

    const onClose = () => {
      const confirmed = dialog.returnValue === "confirm";
      const value = confirmed ? input.value.trim() : null;
      teardown();
      resolve(value || null);
    };

    form.addEventListener("submit", onSubmit);
    cancel?.addEventListener("click", onCancel);
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
    input.focus();
    input.select();
  });
}

function showConfirmDialog({ title, copy, confirmLabel, option = null }) {
  const dialog = confirmDialog();
  if (!dialog) {
    return Promise.resolve({
      confirmed: window.confirm(copy),
      optionChecked: false,
    });
  }

  const titleNode = dialog.querySelector("[data-confirm-title]");
  const copyNode = dialog.querySelector("[data-confirm-copy]");
  const submit = dialog.querySelector("[data-confirm-submit]");
  const cancel = dialog.querySelector("[data-modal-cancel]");
  const optionContainer = dialog.querySelector("[data-confirm-option-container]");
  const optionInput = dialog.querySelector("[data-confirm-option-input]");
  const optionLabel = dialog.querySelector("[data-confirm-option-label]");

  titleNode.textContent = title;
  copyNode.textContent = copy;
  submit.textContent = confirmLabel;

  if (optionContainer && optionInput && optionLabel) {
    optionInput.checked = false;

    if (option) {
      optionContainer.hidden = false;
      optionLabel.textContent = option.label;
    } else {
      optionContainer.hidden = true;
      optionLabel.textContent = "";
    }
  }

  return new Promise((resolve) => {
    const applyOptionState = () => {
      if (!optionInput || !option) {
        copyNode.textContent = copy;
        submit.textContent = confirmLabel;
        return;
      }

      const optionChecked = optionInput.checked;
      copyNode.textContent = optionChecked ? option.checkedCopy ?? copy : copy;
      submit.textContent = optionChecked
        ? option.checkedConfirmLabel ?? confirmLabel
        : confirmLabel;
    };

    const onCancel = () => closeDialog(dialog, "cancel");
    const onClose = () => {
      cancel?.removeEventListener("click", onCancel);
      optionInput?.removeEventListener("change", applyOptionState);
      resolve({
        confirmed: dialog.returnValue === "confirm",
        optionChecked: Boolean(optionInput?.checked),
      });
    };

    applyOptionState();
    cancel?.addEventListener("click", onCancel);
    optionInput?.addEventListener("change", applyOptionState);
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
  });
}

function normalizeTags(tags) {
  const uniqueTags = [];
  const seen = new Set();

  for (const value of tags) {
    const tag = String(value ?? "").trim();
    if (!tag) {
      continue;
    }

    const normalized = tag.toLowerCase();
    if (seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    uniqueTags.push(tag);
  }

  return uniqueTags;
}

function readStoredNotebookMetadata() {
  try {
    const rawValue = window.localStorage.getItem(notebookMetadataStorageKey);
    if (!rawValue) {
      return {};
    }

    const parsed = JSON.parse(rawValue);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_error) {
    return {};
  }
}

function writeStoredNotebookMetadata(state) {
  try {
    window.localStorage.setItem(notebookMetadataStorageKey, JSON.stringify(state));
  } catch (_error) {
    // Ignore persistence failures and keep the in-memory editor functional.
  }
}

function parseDefaultTags(value) {
  if (!value) {
    return [];
  }

  return normalizeTags(String(value).split("||"));
}

function readSourceOptions() {
  const node = document.getElementById("source-options");
  if (!node?.textContent) {
    return [];
  }

  try {
    const parsed = JSON.parse(node.textContent);
    return Array.isArray(parsed) ? parsed.filter((item) => item?.source_id && item?.label) : [];
  } catch (_error) {
    return [];
  }
}

function normalizeDataSources(sources) {
  const options = readSourceOptions();
  const knownSourceIds = new Set(options.map((option) => option.source_id));
  const uniqueSources = [];
  const seen = new Set();

  for (const value of sources ?? []) {
    const sourceId = String(value ?? "").trim();
    if (!sourceId || seen.has(sourceId)) {
      continue;
    }
    if (knownSourceIds.size > 0 && !knownSourceIds.has(sourceId)) {
      continue;
    }
    seen.add(sourceId);
    uniqueSources.push(sourceId);
  }

  return uniqueSources;
}

function parseDefaultDataSources(value) {
  if (!value) {
    return [];
  }

  return normalizeDataSources(String(value).split("||"));
}

function sourceIdFromLegacyTargetLabel(value) {
  const targetLabel = String(value ?? "").trim();
  if (!targetLabel) {
    return null;
  }

  const option = readSourceOptions().find(
    (candidate) =>
      candidate.source_id === targetLabel ||
      candidate.label.toLowerCase() === targetLabel.toLowerCase()
  );
  return option?.source_id ?? null;
}

function sourceOptionForId(sourceId) {
  return readSourceOptions().find((option) => option.source_id === sourceId) ?? null;
}

function sourceLabelForId(sourceId) {
  return sourceOptionForId(sourceId)?.label ?? sourceId;
}

function sourceClassificationForId(sourceId) {
  return sourceOptionForId(sourceId)?.classification ?? "Internal";
}

function sourceComputationModeForId(sourceId) {
  return sourceOptionForId(sourceId)?.computation_mode ?? "VMTP";
}

function sourceLabelsForIds(sourceIds) {
  return normalizeDataSources(sourceIds).map((sourceId) => sourceLabelForId(sourceId));
}

function sourceClassificationForIds(sourceIds) {
  const selectedSourceIds = normalizeDataSources(sourceIds);
  if (!selectedSourceIds.length) {
    return "NA";
  }

  const classifications = [...new Set(selectedSourceIds.map((sourceId) => sourceClassificationForId(sourceId)))];
  return classifications.length === 1 ? classifications[0] : "Mixed";
}

function sourceComputationModeForIds(sourceIds) {
  const selectedSourceIds = normalizeDataSources(sourceIds);
  if (!selectedSourceIds.length) {
    return "NA";
  }

  const computationModes = [...new Set(selectedSourceIds.map((sourceId) => sourceComputationModeForId(sourceId)))];
  return computationModes.length === 1 ? computationModes[0] : "Mixed";
}

function sourceClassificationDisplayText(dataSources) {
  return `Classification: ${sourceClassificationForIds(dataSources)}`;
}

function sourceComputationModeDisplayText(dataSources) {
  return `Processing Mode: ${sourceComputationModeForIds(dataSources)}`;
}

function sourceComputationModeTooltipText() {
  return [
    "MPP = Massive Parallel Processing. Distributed query execution across multiple workers and partitions for larger-scale data processing.",
    "VMTP = Vectorized Multi-Threaded Processing. Single-node vectorized execution across multiple CPU threads for fast local analytical queries.",
  ].join("\n");
}

function accessModeForDataSources(sourceIds) {
  return normalizeDataSources(sourceIds).length > 1 ? "Read / Query only" : "Read / Write";
}

function accessModeHintForDataSources(sourceIds) {
  return normalizeDataSources(sourceIds).length > 1
    ? "Multiple selected sources keep this cell in query-only mode."
    : "A single selected source keeps this cell read/write capable.";
}

function parseCellsPayload(value) {
  if (!value) {
    return [];
  }

  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
}

function parseVersionsPayload(value) {
  if (!value) {
    return [];
  }

  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map((version) => normalizeVersionEntry(version)).filter(Boolean) : [];
  } catch (_error) {
    return [];
  }
}

function normalizeCellEntry(cell, fallback = {}) {
  if (!cell || typeof cell !== "object" || Array.isArray(cell)) {
    return null;
  }

  const cellId =
    typeof cell.cellId === "string" && cell.cellId
      ? cell.cellId
      : typeof cell.cell_id === "string" && cell.cell_id
        ? cell.cell_id
        : fallback.cellId ?? createCellId();

  return {
    cellId,
    dataSources: Array.isArray(cell.dataSources)
      ? normalizeDataSources(cell.dataSources)
      : Array.isArray(cell.data_sources)
        ? normalizeDataSources(cell.data_sources)
        : normalizeDataSources(fallback.dataSources ?? []),
    sql:
      typeof cell.sql === "string"
        ? cell.sql
        : typeof fallback.sql === "string"
          ? fallback.sql
          : "",
  };
}

function normalizeNotebookCells(cells, fallback = {}) {
  const normalized = Array.isArray(cells)
    ? cells
        .map((cell) => normalizeCellEntry(cell))
        .filter(Boolean)
    : [];

  if (normalized.length) {
    return normalized;
  }

  return [
    normalizeCellEntry(
      {
        dataSources: fallback.dataSources ?? [],
        sql: fallback.sql ?? "",
      },
      {
        cellId: createCellId(),
        dataSources: fallback.dataSources ?? [],
        sql: fallback.sql ?? "",
      }
    ),
  ].filter(Boolean);
}

function notebookSourceIds(metadata) {
  const sources = [];
  const seen = new Set();

  for (const cell of metadata.cells ?? []) {
    for (const sourceId of normalizeDataSources(cell.dataSources)) {
      if (seen.has(sourceId)) {
        continue;
      }

      seen.add(sourceId);
      sources.push(sourceId);
    }
  }

  return sources;
}

function notebookAccessMode(metadata) {
  return (metadata.cells ?? []).some((cell) => normalizeDataSources(cell.dataSources).length > 1)
    ? "Read / Query only"
    : "Read / Write";
}

function notebookAccessModeHint(metadata) {
  return (metadata.cells ?? []).some((cell) => normalizeDataSources(cell.dataSources).length > 1)
    ? "At least one cell selects multiple sources and stays in query-only mode."
    : "All cells are currently configured for single-source read/write execution.";
}

function activeWorkspaceMetaRoot(notebookId) {
  return document.querySelector(`[data-notebook-meta][data-notebook-id="${notebookId}"]`);
}

function readNotebookDefaults(notebookId) {
  const link = notebookLinks(notebookId)[0];
  const metaRoot = activeWorkspaceMetaRoot(notebookId);
  const metaTags = parseDefaultTags(metaRoot?.dataset.defaultTags);
  const linkTags = parseDefaultTags(link?.dataset.defaultNotebookTags);
  const metaCells = parseCellsPayload(metaRoot?.dataset.defaultCells);
  const linkCells = parseCellsPayload(link?.dataset.defaultNotebookCells);
  const metaVersions = parseVersionsPayload(metaRoot?.dataset.defaultVersions);
  const metaDataSources = parseDefaultDataSources(metaRoot?.dataset.defaultDataSources);
  const linkDataSources = parseDefaultDataSources(link?.dataset.defaultNotebookDataSources);
  const legacyDataSource = sourceIdFromLegacyTargetLabel(
    metaRoot?.dataset.defaultTargetLabel ??
      link?.dataset.defaultNotebookTargetLabel ??
      link?.dataset.notebookTargetLabel ??
      ""
  );
  const legacySql =
    metaRoot
      ?.closest("[data-workspace-notebook]")
      ?.querySelector("[data-editor-source]")
      ?.dataset.defaultSql ??
    metaRoot
      ?.closest("[data-workspace-notebook]")
      ?.querySelector("[data-editor-source]")
      ?.defaultValue ??
    metaRoot?.closest("[data-workspace-notebook]")?.querySelector("[data-editor-source]")?.value ??
    "";
  const domTags = normalizeTags(
    Array.from(link?.querySelectorAll(".notebook-tag") ?? []).map((tag) => tag.textContent ?? "")
  );
  const fallbackDataSources = metaDataSources.length
    ? metaDataSources
    : linkDataSources.length
      ? linkDataSources
      : legacyDataSource
        ? [legacyDataSource]
        : [];

  const defaults = {
    title: metaRoot?.dataset.defaultTitle ?? link?.dataset.defaultNotebookTitle ?? link?.dataset.notebookTitle ?? "",
    summary:
      metaRoot?.dataset.defaultSummary ??
      link?.dataset.defaultNotebookSummary ??
      link?.dataset.notebookSummary ??
      "",
    cells: normalizeNotebookCells(metaCells.length ? metaCells : linkCells, {
      dataSources: fallbackDataSources,
      sql: legacySql,
    }),
    tags: metaTags.length ? metaTags : linkTags.length ? linkTags : domTags,
    canEdit: (metaRoot?.dataset.canEdit ?? link?.dataset.canEdit ?? "true") !== "false",
    canDelete: (metaRoot?.dataset.canDelete ?? link?.dataset.canDelete ?? "true") !== "false",
    deleted: false,
    versions: metaVersions,
  };
  if (!defaults.versions.length) {
    defaults.versions = [createInitialNotebookVersion(notebookId, defaults)];
  }
  return defaults;
}

function normalizeVersionEntry(version) {
  if (!version || typeof version !== "object") {
    return null;
  }

  const versionId =
    typeof version.versionId === "string" && version.versionId
      ? version.versionId
      : `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const createdAt =
    typeof version.createdAt === "string" && version.createdAt
      ? version.createdAt
      : new Date().toISOString();

  return {
    versionId,
    createdAt,
    title: typeof version.title === "string" ? version.title : "",
    summary: typeof version.summary === "string" ? version.summary : "",
    tags: normalizeTags(Array.isArray(version.tags) ? version.tags : []),
    cells: normalizeNotebookCells(version.cells, {
      dataSources: Array.isArray(version.dataSources)
        ? normalizeDataSources(version.dataSources)
        : version.targetLabel
          ? normalizeDataSources([sourceIdFromLegacyTargetLabel(version.targetLabel)].filter(Boolean))
          : [],
      sql: typeof version.sql === "string" ? version.sql : "",
    }),
  };
}

function createInitialNotebookVersion(notebookId, metadata, createdAt = null) {
  return {
    versionId: `initial-${notebookId}`,
    createdAt: createdAt || new Date().toISOString(),
    title: metadata.title,
    summary: metadata.summary,
    tags: normalizeTags(metadata.tags),
    cells: (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      dataSources: normalizeDataSources(cell.dataSources),
      sql: cell.sql,
    })),
  };
}

function sortVersionsDescending(versions) {
  return [...versions].sort((left, right) => {
    const leftTime = Date.parse(left.createdAt || "") || 0;
    const rightTime = Date.parse(right.createdAt || "") || 0;
    return rightTime - leftTime;
  });
}

function normalizeStoredNotebookState(storedState) {
  if (!storedState || typeof storedState !== "object" || Array.isArray(storedState)) {
    return {};
  }

  return {
    title: typeof storedState.title === "string" ? storedState.title : undefined,
    summary: typeof storedState.summary === "string" ? storedState.summary : undefined,
    tags: Array.isArray(storedState.tags) ? normalizeTags(storedState.tags) : undefined,
    cells:
      Array.isArray(storedState.cells) && storedState.cells.length
        ? normalizeNotebookCells(storedState.cells)
        : storedState.dataSources !== undefined || storedState.sql !== undefined || storedState.targetLabel
          ? normalizeNotebookCells([], {
              dataSources: Array.isArray(storedState.dataSources)
                ? normalizeDataSources(storedState.dataSources)
                : typeof storedState.targetLabel === "string"
                  ? normalizeDataSources([sourceIdFromLegacyTargetLabel(storedState.targetLabel)].filter(Boolean))
                  : [],
              sql: typeof storedState.sql === "string" ? storedState.sql : "",
            })
          : undefined,
    deleted: storedState.deleted === true,
    versions: sortVersionsDescending(
      (storedState.versions ?? []).map((version) => normalizeVersionEntry(version)).filter(Boolean)
    ),
  };
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeQueryJob(job) {
  if (!job || typeof job !== "object") {
    return null;
  }

  return {
    ...job,
    columns: Array.isArray(job.columns) ? job.columns : [],
    rows: Array.isArray(job.rows) ? job.rows : [],
    dataSources: Array.isArray(job.dataSources) ? job.dataSources : [],
    sourceTypes: Array.isArray(job.sourceTypes) ? job.sourceTypes : [],
  };
}

function currentWorkspaceNotebookTitle(workspaceRoot = document.querySelector("[data-workspace-notebook]")) {
  const titleDisplay = workspaceRoot?.querySelector("[data-notebook-title-display]");
  return titleDisplay?.textContent?.trim() || "Notebook";
}

function currentWorkspaceNotebookId() {
  return workspaceNotebookId(document.querySelector("[data-workspace-notebook]"));
}

function selectedDataSourcesForCell(cellRoot) {
  if (!(cellRoot instanceof Element)) {
    return [];
  }

  const checkedValues = Array.from(cellRoot.querySelectorAll("[data-cell-source-option]:checked")).map(
    (option) => option.value
  );
  if (checkedValues.length) {
    return normalizeDataSources(checkedValues);
  }

  return normalizeDataSources((cellRoot.dataset.defaultCellSources || "").split("||"));
}

function queryJobForCell(notebookId, cellId) {
  if (!notebookId || !cellId) {
    return null;
  }

  return (
    queryJobsSnapshot.find((job) => job.notebookId === notebookId && job.cellId === cellId) ?? null
  );
}

function queryJobIsRunning(job) {
  return Boolean(job && queryJobRunningStatuses.has(job.status));
}

function queryJobStatusCopy(job) {
  if (!job) {
    return "Idle";
  }

  switch (job.status) {
    case "queued":
      return "Queued";
    case "running":
      return "Running";
    case "completed":
      return "Completed";
    case "cancelled":
      return "Cancelled";
    case "failed":
      return "Failed";
    default:
      return "Idle";
  }
}

function queryJobElapsedMs(job) {
  if (!job) {
    return 0;
  }

  if (queryJobIsRunning(job)) {
    const startedAtMs = Date.parse(job.startedAt || "");
    if (!Number.isNaN(startedAtMs)) {
      return Math.max(0, Date.now() - startedAtMs);
    }
  }

  return Number.isFinite(Number(job.durationMs)) ? Math.max(0, Number(job.durationMs)) : 0;
}

function formatQueryDuration(durationMs) {
  let remaining = Math.max(0, Math.round(Number.isFinite(Number(durationMs)) ? Number(durationMs) : 0));
  const units = [
    ["d", 24 * 60 * 60 * 1000],
    ["h", 60 * 60 * 1000],
    ["m", 60 * 1000],
    ["s", 1000],
  ];
  const parts = [];
  let started = false;

  for (const [suffix, size] of units) {
    const value = Math.floor(remaining / size);
    remaining -= value * size;
    if (value > 0 || started) {
      parts.push(`${value}${suffix}`);
      started = true;
    }
  }

  if (!parts.length) {
    return `${remaining} ms`;
  }

  parts.push(`${remaining} ms`);
  return parts.join(" ");
}

function formatQueryTimestamp(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return parsed.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function queryRowsShownLabel(job) {
  if (!job) {
    return "Run this cell to inspect the selected data sources.";
  }

  if (job.rowsShown > 0) {
    if (job.truncated) {
      return `${job.rowsShown} row(s) shown. The result was truncated for the UI.`;
    }
    return `${job.rowsShown} row(s) shown.`;
  }

  if (queryJobIsRunning(job)) {
    return "Waiting for the first rows...";
  }

  return job.message || "Statement executed successfully.";
}

function queryProgressMarkup(job) {
  if (!queryJobIsRunning(job)) {
    return "";
  }

  const progressValue =
    typeof job.progress === "number" && Number.isFinite(job.progress)
      ? Math.max(0, Math.min(100, job.progress * 100))
      : null;

  return `
    <div class="query-progress-card">
      <div class="query-progress-copy">
        <strong>${escapeHtml(job.progressLabel || "Running...")}</strong>
        <span>${escapeHtml(job.backendName || "VMTP DUCKDB")}</span>
      </div>
      <div class="query-progress-track${progressValue === null ? " is-indeterminate" : ""}">
        <span style="${progressValue === null ? "" : `width:${progressValue}%;`}"></span>
      </div>
    </div>
  `;
}

function queryResultTableMarkup(job) {
  if (!job?.columns?.length) {
    return "";
  }

  return `
    <div class="result-table-wrap">
      <table class="result-table">
        <thead>
          <tr>
            ${job.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}
          </tr>
        </thead>
        <tbody>
          ${job.rows
            .map(
              (row) => `
                <tr>
                  ${row
                    .map((value) =>
                      value === null
                        ? '<td><span class="cell-null">NULL</span></td>'
                        : `<td>${escapeHtml(value)}</td>`
                    )
                    .join("")}
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function queryResultPanelMarkup(cellId, job = null) {
  if (!job) {
    return emptyQueryResultsMarkup(cellId);
  }

  const showDownloads = job.status === "completed" && job.columns.length > 0;
  const rowsBadge = queryRowsShownLabel(job);
  const showRowsBadge = queryJobIsRunning(job) || Number(job.rowsShown || 0) > 0 || Boolean(job.truncated);
  const resultBody = job.error
    ? `
        <div class="result-error">
          <strong>${escapeHtml(job.status === "cancelled" ? "Query cancelled." : "Query failed.")}</strong>
          <pre>${escapeHtml(job.error)}</pre>
        </div>
      `
    : job.columns.length
      ? `
          ${queryProgressMarkup(job)}
          ${queryResultTableMarkup(job)}
        `
      : queryJobIsRunning(job)
        ? `
            ${queryProgressMarkup(job)}
            <div class="result-empty result-empty-running">
              <p>${escapeHtml(job.message || "Running query...")}</p>
            </div>
          `
        : `
            <div class="result-empty">
              <p>${escapeHtml(job.message || "Statement executed successfully.")}</p>
            </div>
          `;

  return `
    <section id="query-results-${escapeHtml(cellId)}" class="result-panel" data-cell-result>
      <header class="result-header">
        <div class="result-header-copy">
          <h3>Result</h3>
          <p class="result-meta" data-query-duration data-job-id="${escapeHtml(job.jobId || "")}">${escapeHtml(formatQueryDuration(queryJobElapsedMs(job)))}</p>
        </div>
        <div class="result-header-actions">
          <span class="result-badge${queryJobIsRunning(job) ? " is-live" : ""}" ${showRowsBadge ? "" : "hidden"}>${escapeHtml(rowsBadge)}</span>
          <div class="result-download-actions" aria-label="Result download actions" ${showDownloads ? "" : "hidden"}>
            <button
              type="button"
              class="result-action-button"
              data-result-download-json
              title="Download result as JSON"
            >JSON</button>
            <button
              type="button"
              class="result-action-button"
              data-result-download-csv
              title="Download result as CSV"
            >CSV</button>
          </div>
        </div>
      </header>
      ${resultBody}
    </section>
  `;
}

function renderPerformanceChartMarkup(performance) {
  const points = Array.isArray(performance?.recent) ? performance.recent : [];
  if (!points.length) {
    return "";
  }

  const width = 280;
  const height = 92;
  const paddingX = 10;
  const paddingY = 10;
  const values = points.map((point) => Math.max(1, Number(point.durationMs || 0)));
  const transformedValues = values.map((value) => Math.log10(value + 1));
  const minValue = Math.min(...transformedValues);
  const maxValue = Math.max(...transformedValues);
  const spread = Math.max(maxValue - minValue, 0.0001);
  const stepX = points.length > 1 ? (width - paddingX * 2) / (points.length - 1) : 0;

  const yForValue = (durationMs) => {
    const transformed = Math.log10(Math.max(1, Number(durationMs || 0)) + 1);
    const ratio = (transformed - minValue) / spread;
    return height - paddingY - ratio * (height - paddingY * 2);
  };

  const polyline = points
    .map((point, index) => `${paddingX + index * stepX},${yForValue(point.durationMs).toFixed(2)}`)
    .join(" ");
  const p50Y =
    typeof performance?.stats?.p50Ms === "number" ? yForValue(performance.stats.p50Ms).toFixed(2) : null;
  const p95Y =
    typeof performance?.stats?.p95Ms === "number" ? yForValue(performance.stats.p95Ms).toFixed(2) : null;

  return `
    <svg viewBox="0 0 ${width} ${height}" class="query-monitor-chart-svg" preserveAspectRatio="none" aria-hidden="true">
      ${p95Y ? `<line x1="${paddingX}" y1="${p95Y}" x2="${width - paddingX}" y2="${p95Y}" class="query-monitor-chart-line query-monitor-chart-line-p95"></line>` : ""}
      ${p50Y ? `<line x1="${paddingX}" y1="${p50Y}" x2="${width - paddingX}" y2="${p50Y}" class="query-monitor-chart-line query-monitor-chart-line-p50"></line>` : ""}
      <polyline points="${polyline}" class="query-monitor-chart-path"></polyline>
      ${points
        .map((point, index) => {
          const x = paddingX + index * stepX;
          const y = yForValue(point.durationMs);
          return `
            <circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="2.4" class="query-monitor-chart-point query-monitor-chart-point-${escapeHtml(point.status)}">
              <title>${escapeHtml(point.notebookTitle)} | ${escapeHtml(formatQueryDuration(point.durationMs))}</title>
            </circle>
          `;
        })
        .join("")}
    </svg>
  `;
}

function queryPerformanceStatsMarkup(performance) {
  const stats = performance?.stats ?? {};
  const statEntries = [
    [
      "Latest",
      stats.latestMs,
      "Runtime of the most recently completed query.",
    ],
    [
      "p50",
      stats.p50Ms,
      "Median runtime. 50% of recent completed queries finished at or below this duration.",
    ],
    [
      "p95",
      stats.p95Ms,
      "Tail runtime. 95% of recent completed queries finished at or below this duration. The slowest 5% took longer.",
    ],
  ].filter((entry) => typeof entry[1] === "number");

  return statEntries
    .map(
      ([label, value, tooltip]) => `
        <span class="query-monitor-stat-pill" title="${escapeHtml(tooltip)}" aria-label="${escapeHtml(
          `${label}: ${tooltip}`
        )}">
          <strong>${escapeHtml(label)}</strong>
          <span>${escapeHtml(formatQueryDuration(value))}</span>
        </span>
      `
    )
    .join("");
}

function queryMonitorItemMarkup(job) {
  const running = queryJobIsRunning(job);
  const rowsCopy = job.rowsShown > 0 ? `${job.rowsShown} row(s)` : "No rows yet";
  return `
    <article class="query-monitor-item query-monitor-item-${escapeHtml(job.status)}" data-query-job-id="${escapeHtml(job.jobId)}">
      <div class="query-monitor-item-copy">
        <button
          type="button"
          class="query-monitor-open"
          data-open-query-notebook="${escapeHtml(job.notebookId)}"
          data-open-query-cell="${escapeHtml(job.cellId)}"
          title="Open ${escapeHtml(job.notebookTitle)}"
        >
          ${escapeHtml(job.notebookTitle)}
        </button>
        <div class="query-monitor-item-meta">
          <span class="query-monitor-status-badge${running ? " is-live" : ""}">${escapeHtml(queryJobStatusCopy(job))}</span>
          <span data-query-monitor-duration data-job-id="${escapeHtml(job.jobId)}">${escapeHtml(formatQueryDuration(queryJobElapsedMs(job)))}</span>
          <span>${escapeHtml(rowsCopy)}</span>
        </div>
        <p class="query-monitor-sql">${escapeHtml(job.sql)}</p>
      </div>
      <div class="query-monitor-item-actions">
        ${running ? `<button type="button" class="query-monitor-cancel" data-cancel-query-job="${escapeHtml(job.jobId)}">Cancel</button>` : ""}
        <span class="query-monitor-updated">${escapeHtml(formatQueryTimestamp(job.updatedAt))}</span>
      </div>
    </article>
  `;
}

function queryNotificationItemMarkup(job) {
  const rowsLabel = queryRowsShownLabel(job);
  return `
    <button
      type="button"
      class="topbar-notification-item"
      data-open-query-notebook="${escapeHtml(job.notebookId)}"
      data-open-query-cell="${escapeHtml(job.cellId)}"
      title="Open ${escapeHtml(job.notebookTitle)}"
    >
      <span class="topbar-notification-item-status${queryJobIsRunning(job) ? " is-live" : ""}">${escapeHtml(queryJobStatusCopy(job))}</span>
      <span class="topbar-notification-item-title">${escapeHtml(job.notebookTitle)}</span>
      <span class="topbar-notification-item-copy" data-query-notification-copy data-job-id="${escapeHtml(job.jobId)}" data-query-copy-suffix="${escapeHtml(rowsLabel)}">${escapeHtml(formatQueryDuration(queryJobElapsedMs(job)))} | ${escapeHtml(rowsLabel)}</span>
    </button>
  `;
}

function refreshLiveQueryClock() {
  const jobsById = new Map(queryJobsSnapshot.map((job) => [job.jobId, job]));

  document.querySelectorAll("[data-query-duration]").forEach((node) => {
    const job = jobsById.get(node.dataset.jobId || "");
    if (!job) {
      return;
    }
    node.textContent = formatQueryDuration(queryJobElapsedMs(job));
  });

  document.querySelectorAll("[data-query-monitor-duration]").forEach((node) => {
    const job = jobsById.get(node.dataset.jobId || "");
    if (!job) {
      return;
    }
    node.textContent = formatQueryDuration(queryJobElapsedMs(job));
  });

  document.querySelectorAll("[data-query-notification-copy]").forEach((node) => {
    const job = jobsById.get(node.dataset.jobId || "");
    if (!job) {
      return;
    }
    const suffix = node.dataset.queryCopySuffix || queryRowsShownLabel(job);
    node.textContent = `${formatQueryDuration(queryJobElapsedMs(job))} | ${suffix}`;
  });
}

function syncQueryClockLoop() {
  const hasRunningJobs = queryJobsSnapshot.some((job) => queryJobIsRunning(job));
  if (hasRunningJobs && queryJobsClockHandle === null) {
    refreshLiveQueryClock();
    queryJobsClockHandle = window.setInterval(refreshLiveQueryClock, 100);
    return;
  }

  if (!hasRunningJobs && queryJobsClockHandle !== null) {
    window.clearInterval(queryJobsClockHandle);
    queryJobsClockHandle = null;
  }

  refreshLiveQueryClock();
}

function renderQueryMonitor() {
  const listRoot = queryMonitorList();
  const countRoot = queryMonitorCount();
  const toggleCountRoot = sidebarQueryCount();
  const performanceRoot = queryPerformanceSection();
  const performanceStatsRoot = queryPerformanceStats();
  const performanceChartRoot = queryPerformanceChart();
  if (!listRoot || !countRoot) {
    return;
  }

  const runningCount = Number(queryJobsSummary.runningCount || 0);
  countRoot.textContent = String(runningCount);
  countRoot.classList.toggle("is-live", runningCount > 0);
  if (toggleCountRoot) {
    toggleCountRoot.textContent = String(runningCount);
    toggleCountRoot.hidden = runningCount === 0;
    toggleCountRoot.classList.toggle("is-live", runningCount > 0);
  }

  if (!queryJobsSnapshot.length) {
    listRoot.innerHTML = '<p class="query-monitor-empty">No query jobs yet.</p>';
  } else {
    listRoot.innerHTML = queryJobsSnapshot.slice(0, 8).map((job) => queryMonitorItemMarkup(job)).join("");
  }

  if (performanceRoot && performanceStatsRoot && performanceChartRoot) {
    const hasPerformance = Array.isArray(queryPerformanceState?.recent) && queryPerformanceState.recent.length > 0;
    performanceRoot.hidden = !hasPerformance;
    if (hasPerformance) {
      performanceStatsRoot.innerHTML = queryPerformanceStatsMarkup(queryPerformanceState);
      performanceChartRoot.innerHTML = renderPerformanceChartMarkup(queryPerformanceState);
    } else {
      performanceStatsRoot.innerHTML = "";
      performanceChartRoot.innerHTML = "";
    }
  }
}

function renderQueryNotificationMenu() {
  const menu = queryNotificationMenu();
  const listRoot = queryNotificationList();
  const countRoot = queryNotificationCount();
  if (!menu || !listRoot || !countRoot) {
    return;
  }

  const activeNotebookId = currentActiveNotebookId();
  const visibleNotifications = queryJobsSnapshot.filter((job) => job.notebookId !== activeNotebookId);
  const runningCount = Number(queryJobsSummary.runningCount || 0);
  const badgeCount = runningCount > 0 ? runningCount : visibleNotifications.length;
  countRoot.textContent = String(badgeCount);
  countRoot.hidden = badgeCount === 0;
  countRoot.classList.toggle("is-live", runningCount > 0);

  if (!visibleNotifications.length) {
    listRoot.innerHTML = '<p class="topbar-notification-empty">No background query activity yet.</p>';
    return;
  }

  listRoot.innerHTML = visibleNotifications.slice(0, 10).map((job) => queryNotificationItemMarkup(job)).join("");
}

function syncQueryCellJobState(cellRoot) {
  if (!(cellRoot instanceof Element)) {
    return;
  }

  const workspaceRoot = cellRoot.closest("[data-workspace-notebook]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellId = cellRoot.dataset.cellId;
  const job = queryJobForCell(notebookId, cellId);
  const runButton = cellRoot.querySelector("[data-run-cell]");
  const cancelButton = cellRoot.querySelector("[data-cancel-query]");
  const resultRoot = cellRoot.querySelector("[data-cell-result]");

  cellRoot.classList.toggle("is-query-running", queryJobIsRunning(job));

  if (runButton) {
    if (queryJobIsRunning(job)) {
      runButton.disabled = true;
      runButton.classList.add("is-running");
      runButton.innerHTML =
        '<span class="query-button-spinner" aria-hidden="true"></span><span class="query-button-running-copy">Running ...</span>';
    } else {
      runButton.disabled = false;
      runButton.classList.remove("is-running");
      runButton.textContent = "Run Cell";
    }
  }

  if (cancelButton) {
    cancelButton.hidden = !queryJobIsRunning(job);
    cancelButton.dataset.jobId = job?.jobId || "";
    cancelButton.disabled = !queryJobIsRunning(job);
  }

  if (resultRoot) {
    resultRoot.outerHTML = queryResultPanelMarkup(cellId, job);
  }
}

function syncVisibleQueryCells() {
  document.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
    syncQueryCellJobState(cellRoot);
  });
}

function applyQueryJobsState(snapshot) {
  queryJobsStateVersion = snapshot?.version ?? null;
  queryJobsSummary = snapshot?.summary ?? { runningCount: 0, totalCount: 0 };
  queryPerformanceState = snapshot?.performance ?? { recent: [], stats: {} };
  queryJobsSnapshot = Array.isArray(snapshot?.jobs)
    ? snapshot.jobs.map((job) => normalizeQueryJob(job)).filter(Boolean)
    : [];

  renderQueryMonitor();
  renderQueryNotificationMenu();
  syncVisibleQueryCells();
  syncQueryClockLoop();
}

function normalizeSourceObjectFields(fields) {
  if (!Array.isArray(fields)) {
    return [];
  }

  return fields
    .map((field) => ({
      name: typeof field?.name === "string" ? field.name.trim() : "",
      dataType: typeof field?.dataType === "string" ? field.dataType.trim() : "UNKNOWN",
    }))
    .filter((field) => field.name);
}

function sourceObjectRelation(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    return "";
  }

  return sourceObjectRoot.dataset.sourceObjectRelation?.trim() || "";
}

function sourceObjectFieldCacheKey(sourceObjectRoot) {
  return sourceObjectRelation(sourceObjectRoot);
}

function sourceObjectDisplayName(sourceObjectRoot) {
  return (
    sourceObjectRoot?.dataset.sourceObjectName?.trim() ||
    sourceObjectRoot?.dataset.sourceObjectRelation?.trim() ||
    "Selected source"
  );
}

function sourceObjectDisplayKind(sourceObjectRoot) {
  return sourceObjectRoot?.dataset.sourceObjectKind?.trim()?.toUpperCase() || "TABLE";
}

function normalizedSourceFieldDataType(dataType) {
  return String(dataType ?? "")
    .trim()
    .toUpperCase();
}

function sourceFieldTypeFamily(dataType) {
  const normalized = normalizedSourceFieldDataType(dataType);
  if (!normalized) {
    return "unknown";
  }

  if (
    /(BIGINT|HUGEINT|INTEGER|INT|SMALLINT|TINYINT|DECIMAL|NUMERIC|REAL|DOUBLE|FLOAT|SERIAL|UBIGINT|UHUGEINT|UINTEGER|USMALLINT|UTINYINT)/.test(
      normalized
    )
  ) {
    return "number";
  }

  if (/(DATE|TIME|TIMESTAMP|INTERVAL)/.test(normalized)) {
    return "temporal";
  }

  if (/(BOOL)/.test(normalized)) {
    return "boolean";
  }

  if (/(JSON|JSONB|XML)/.test(normalized)) {
    return "document";
  }

  if (/(BYTEA|BLOB|BINARY|VARBINARY)/.test(normalized)) {
    return "binary";
  }

  if (/(ARRAY|LIST)/.test(normalized)) {
    return "list";
  }

  if (/(MAP|STRUCT|UNION)/.test(normalized)) {
    return "object";
  }

  if (/(CHAR|TEXT|STRING|VARCHAR|UUID|ENUM|INET|CIDR|NAME)/.test(normalized)) {
    return "text";
  }

  return "unknown";
}

function sourceFieldIconMarkup(dataType) {
  switch (sourceFieldTypeFamily(dataType)) {
    case "number":
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-number" aria-hidden="true">
          <span class="sidebar-source-field-icon-glyph">123</span>
        </span>
      `;
    case "text":
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-text" aria-hidden="true">
          <span class="sidebar-source-field-icon-glyph">T</span>
        </span>
      `;
    case "temporal":
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-temporal" aria-hidden="true">
          <svg viewBox="0 0 16 16" focusable="false">
            <rect x="2.25" y="3.25" width="11.5" height="10.5" rx="1.5"></rect>
            <path d="M5 2.4v2.4M11 2.4v2.4M2.5 6.15h11"></path>
            <path d="M4.9 8.6h2.1M9 8.6h2.1M4.9 11h2.1"></path>
          </svg>
        </span>
      `;
    case "boolean":
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-boolean" aria-hidden="true">
          <svg viewBox="0 0 16 16" focusable="false">
            <rect x="2.5" y="2.5" width="11" height="11" rx="2"></rect>
            <path d="M5.3 8.2 7.1 10l3.7-4"></path>
          </svg>
        </span>
      `;
    case "document":
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-document" aria-hidden="true">
          <span class="sidebar-source-field-icon-glyph sidebar-source-field-icon-glyph-code">{ }</span>
        </span>
      `;
    case "binary":
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-binary" aria-hidden="true">
          <span class="sidebar-source-field-icon-glyph sidebar-source-field-icon-glyph-binary">01</span>
        </span>
      `;
    case "list":
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-list" aria-hidden="true">
          <svg viewBox="0 0 16 16" focusable="false">
            <circle cx="4.2" cy="4.3" r="0.95"></circle>
            <circle cx="4.2" cy="8" r="0.95"></circle>
            <circle cx="4.2" cy="11.7" r="0.95"></circle>
            <path d="M6.6 4.3h5.2M6.6 8h5.2M6.6 11.7h5.2"></path>
          </svg>
        </span>
      `;
    case "object":
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-object" aria-hidden="true">
          <svg viewBox="0 0 16 16" focusable="false">
            <rect x="2.1" y="2.2" width="3.6" height="3.6" rx="0.7"></rect>
            <rect x="10.3" y="2.2" width="3.6" height="3.6" rx="0.7"></rect>
            <rect x="10.3" y="10.2" width="3.6" height="3.6" rx="0.7"></rect>
            <path d="M5.7 4h3a1.8 1.8 0 0 1 1.8 1.8v1.1"></path>
            <path d="M10.5 8.4H7.7a1.8 1.8 0 0 0-1.8 1.8v0.2"></path>
          </svg>
        </span>
      `;
    default:
      return `
        <span class="sidebar-source-field-icon sidebar-source-field-icon-unknown" aria-hidden="true">
          <svg viewBox="0 0 16 16" focusable="false">
            <circle cx="8" cy="8" r="4.4"></circle>
          </svg>
        </span>
      `;
  }
}

function sourceInspectorMarkup(sourceObjectRoot, fields) {
  const objectName = sourceObjectDisplayName(sourceObjectRoot);
  const objectKind = sourceObjectDisplayKind(sourceObjectRoot);
  const fieldCountLabel = `${fields.length} ${fields.length === 1 ? "field" : "fields"}`;

  const fieldsMarkup = fields.length
    ? `
        <ul class="sidebar-source-field-list">
          ${fields
            .map(
              (field) => `
                <li class="sidebar-source-field">
                  <span class="sidebar-source-field-name">
                    ${sourceFieldIconMarkup(field.dataType)}
                    <span class="sidebar-source-field-name-text">${escapeHtml(field.name)}</span>
                  </span>
                  <span class="sidebar-source-field-type">${escapeHtml(field.dataType)}</span>
                </li>
              `
            )
            .join("")}
        </ul>
      `
    : '<p class="sidebar-source-inspector-empty">No fields are available for this source object.</p>';

  return `
    <header class="sidebar-source-inspector-header">
      <div class="sidebar-source-inspector-copy">
        <h3 class="sidebar-source-inspector-title">${escapeHtml(objectName)}</h3>
        <p class="sidebar-source-inspector-meta">${escapeHtml(objectKind)} - ${escapeHtml(fieldCountLabel)}</p>
      </div>
    </header>
    <div class="sidebar-source-inspector-body">
      ${fieldsMarkup}
    </div>
  `;
}

function sourceInspectorLoadingMarkup(sourceObjectRoot) {
  return `
    <header class="sidebar-source-inspector-header">
      <div class="sidebar-source-inspector-copy">
        <h3 class="sidebar-source-inspector-title">${escapeHtml(sourceObjectDisplayName(sourceObjectRoot))}</h3>
        <p class="sidebar-source-inspector-meta">${escapeHtml(sourceObjectDisplayKind(sourceObjectRoot))}</p>
      </div>
    </header>
    <div class="sidebar-source-inspector-loading">
      <span class="sidebar-loading-spinner" aria-hidden="true"></span>
      <span>Loading fields...</span>
    </div>
  `;
}

function sourceInspectorErrorMarkup(sourceObjectRoot, message) {
  return `
    <header class="sidebar-source-inspector-header">
      <div class="sidebar-source-inspector-copy">
        <h3 class="sidebar-source-inspector-title">${escapeHtml(sourceObjectDisplayName(sourceObjectRoot))}</h3>
        <p class="sidebar-source-inspector-meta">${escapeHtml(sourceObjectDisplayKind(sourceObjectRoot))}</p>
      </div>
    </header>
    <p class="sidebar-source-inspector-empty">${escapeHtml(message)}</p>
  `;
}

function renderSourceInspectorMarkup(markup, hidden = false) {
  const inspectorRoot = sourceInspector();
  const inspectorPanel = sourceInspectorPanel();
  if (!inspectorRoot || !inspectorPanel) {
    return;
  }

  if (hidden) {
    inspectorRoot.hidden = true;
    inspectorPanel.innerHTML = "";
    return;
  }

  inspectorPanel.innerHTML = markup;
  inspectorRoot.hidden = false;
}

function renderSourceInspector(sourceObjectRoot = null, fields = []) {
  if (!(sourceObjectRoot instanceof Element)) {
    renderSourceInspectorMarkup("", true);
    return;
  }

  renderSourceInspectorMarkup(sourceInspectorMarkup(sourceObjectRoot, normalizeSourceObjectFields(fields)));
}

function renderSourceInspectorLoading(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    renderSourceInspectorMarkup("", true);
    return;
  }

  renderSourceInspectorMarkup(sourceInspectorLoadingMarkup(sourceObjectRoot));
}

function renderSourceInspectorError(sourceObjectRoot, message) {
  if (!(sourceObjectRoot instanceof Element)) {
    renderSourceInspectorMarkup("", true);
    return;
  }

  renderSourceInspectorMarkup(
    sourceInspectorErrorMarkup(
      sourceObjectRoot,
      message || "The fields could not be loaded for this source object."
    )
  );
}

function setSourceObjectLoadingState(sourceObjectRoot, loading) {
  if (!(sourceObjectRoot instanceof Element)) {
    return;
  }

  sourceObjectRoot.classList.toggle("is-loading", loading);
  sourceObjectRoot.setAttribute("aria-busy", loading ? "true" : "false");
}

function setSelectedSourceObjectState(sourceObjectRoot = null) {
  const selectedRelation = sourceObjectRoot?.dataset.sourceObjectRelation?.trim() || null;
  activeSourceObjectRelation = selectedRelation;

  sourceObjectNodes().forEach((item) => {
    const isSelected = item === sourceObjectRoot;
    item.classList.toggle("is-selected", isSelected);
    item.setAttribute("aria-selected", isSelected ? "true" : "false");
    if (!isSelected) {
      setSourceObjectLoadingState(item, false);
    }
  });

  if (!(sourceObjectRoot instanceof Element)) {
    renderSourceInspectorMarkup("", true);
  }
}

async function fetchSourceObjectFields(relation) {
  const response = await window.fetch(
    `/api/source-object-fields?relation=${encodeURIComponent(relation)}`,
    {
      headers: {
        Accept: "application/json",
      },
    }
  );

  if (!response.ok) {
    throw new Error("The fields could not be loaded for this source object.");
  }

  const payload = await response.json();
  return normalizeSourceObjectFields(payload?.fields ?? []);
}

async function loadSourceObjectFields(sourceObjectRoot, { renderLoading = true } = {}) {
  const relation = sourceObjectFieldCacheKey(sourceObjectRoot);
  if (!relation) {
    return [];
  }

  if (sourceObjectFieldCache.has(relation)) {
    const fields = sourceObjectFieldCache.get(relation) ?? [];
    if (activeSourceObjectRelation === relation) {
      renderSourceInspector(sourceObjectRoot, fields);
    }
    return fields;
  }

  if (renderLoading && activeSourceObjectRelation === relation) {
    setSourceObjectLoadingState(sourceObjectRoot, true);
    renderSourceInspectorLoading(sourceObjectRoot);
  }

  let pendingRequest = sourceObjectFieldRequests.get(relation);
  if (!pendingRequest) {
    pendingRequest = fetchSourceObjectFields(relation)
      .then((fields) => {
        sourceObjectFieldCache.set(relation, fields);
        return fields;
      })
      .finally(() => {
        sourceObjectFieldRequests.delete(relation);
      });
    sourceObjectFieldRequests.set(relation, pendingRequest);
  }

  try {
    const fields = await pendingRequest;
    if (activeSourceObjectRelation === relation) {
      renderSourceInspector(sourceObjectRoot, fields);
    }
    return fields;
  } catch (error) {
    if (activeSourceObjectRelation === relation) {
      renderSourceInspectorError(
        sourceObjectRoot,
        error instanceof Error ? error.message : "The fields could not be loaded for this source object."
      );
    }
    throw error;
  } finally {
    setSourceObjectLoadingState(sourceObjectRoot, false);
  }
}

async function selectSourceObject(sourceObjectRoot = null, { renderLoading = true } = {}) {
  setSelectedSourceObjectState(sourceObjectRoot);
  if (!(sourceObjectRoot instanceof Element)) {
    return [];
  }

  return loadSourceObjectFields(sourceObjectRoot, { renderLoading });
}

function restoreSelectedSourceObject() {
  const sourceObjectRoot =
    sourceObjectNodes().find(
      (item) => item.dataset.sourceObjectRelation?.trim() === activeSourceObjectRelation
    ) ?? null;

  if (!sourceObjectRoot) {
    activeSourceObjectRelation = null;
  }

  selectSourceObject(sourceObjectRoot, {
    renderLoading: !sourceObjectFieldCache.has(sourceObjectFieldCacheKey(sourceObjectRoot)),
  }).catch(() => {
    // Keep the last selected state, but do not interrupt the rest of the UI.
  });
}

function readSqlDollarQuotedLiteral(sqlText, startIndex) {
  const delimiterMatch = sqlText.slice(startIndex).match(/^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/);
  if (!delimiterMatch) {
    return null;
  }

  const delimiter = delimiterMatch[0];
  const endIndex = sqlText.indexOf(delimiter, startIndex + delimiter.length);
  if (endIndex === -1) {
    return {
      value: sqlText.slice(startIndex),
      nextIndex: sqlText.length,
    };
  }

  return {
    value: sqlText.slice(startIndex, endIndex + delimiter.length),
    nextIndex: endIndex + delimiter.length,
  };
}

function readSqlQuotedLiteral(sqlText, startIndex, delimiter) {
  let index = startIndex + 1;
  while (index < sqlText.length) {
    const current = sqlText[index];
    if (current === delimiter) {
      if (delimiter !== "`" && sqlText[index + 1] === delimiter) {
        index += 2;
        continue;
      }

      index += 1;
      break;
    }

    if (current === "\\" && index + 1 < sqlText.length) {
      index += 2;
      continue;
    }

    index += 1;
  }

  return {
    value: sqlText.slice(startIndex, index),
    nextIndex: index,
  };
}

function tokenizeSql(sqlText) {
  const tokens = [];
  let index = 0;

  while (index < sqlText.length) {
    const current = sqlText[index];

    if (/\s/.test(current)) {
      index += 1;
      continue;
    }

    if (current === "-" && sqlText[index + 1] === "-") {
      const startIndex = index;
      index += 2;
      while (index < sqlText.length && sqlText[index] !== "\n") {
        index += 1;
      }
      tokens.push({ type: "comment", value: sqlText.slice(startIndex, index) });
      continue;
    }

    if (current === "/" && sqlText[index + 1] === "*") {
      const startIndex = index;
      index += 2;
      while (index < sqlText.length && !(sqlText[index] === "*" && sqlText[index + 1] === "/")) {
        index += 1;
      }
      index = Math.min(index + 2, sqlText.length);
      tokens.push({ type: "comment", value: sqlText.slice(startIndex, index) });
      continue;
    }

    if (current === "$") {
      const dollarQuoted = readSqlDollarQuotedLiteral(sqlText, index);
      if (dollarQuoted) {
        tokens.push({ type: "string", value: dollarQuoted.value });
        index = dollarQuoted.nextIndex;
        continue;
      }
    }

    if (current === "'" || current === '"' || current === "`") {
      const quoted = readSqlQuotedLiteral(sqlText, index, current);
      tokens.push({ type: current === "'" ? "string" : "identifier", value: quoted.value });
      index = quoted.nextIndex;
      continue;
    }

    if (current === "[") {
      const endIndex = sqlText.indexOf("]", index + 1);
      tokens.push({
        type: "identifier",
        value: endIndex === -1 ? sqlText.slice(index) : sqlText.slice(index, endIndex + 1),
      });
      index = endIndex === -1 ? sqlText.length : endIndex + 1;
      continue;
    }

    if (/[A-Za-z_]/.test(current)) {
      const startIndex = index;
      index += 1;
      while (index < sqlText.length && /[A-Za-z0-9_$]/.test(sqlText[index])) {
        index += 1;
      }
      tokens.push({ type: "word", value: sqlText.slice(startIndex, index) });
      continue;
    }

    if (/[0-9]/.test(current)) {
      const startIndex = index;
      index += 1;
      while (index < sqlText.length && /[0-9.]/.test(sqlText[index])) {
        index += 1;
      }
      tokens.push({ type: "number", value: sqlText.slice(startIndex, index) });
      continue;
    }

    const doubleCharacterSymbol = sqlText.slice(index, index + 2);
    if (["!=", "<=", "<>", "::", "=>", ">=", "||"].includes(doubleCharacterSymbol)) {
      tokens.push({ type: "symbol", value: doubleCharacterSymbol });
      index += 2;
      continue;
    }

    tokens.push({ type: "symbol", value: current });
    index += 1;
  }

  return tokens;
}

function sqlKeywordPhraseMatches(tokens, startIndex, phrase) {
  if (startIndex + phrase.length > tokens.length) {
    return false;
  }

  return phrase.every((part, offset) => {
    const token = tokens[startIndex + offset];
    return token?.type === "word" && token.value.toUpperCase() === part;
  });
}

function combineSqlKeywordTokens(tokens) {
  const combined = [];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token.type !== "word") {
      combined.push(token);
      continue;
    }

    const matchedPhrase = sqlFormatKeywordPhrases.find((phrase) =>
      sqlKeywordPhraseMatches(tokens, index, phrase)
    );
    if (matchedPhrase) {
      combined.push({ type: "keyword", value: matchedPhrase.join(" ") });
      index += matchedPhrase.length - 1;
      continue;
    }

    const uppercaseValue = token.value.toUpperCase();
    if (sqlFormatKeywords.has(uppercaseValue)) {
      combined.push({ type: "keyword", value: uppercaseValue });
      continue;
    }

    combined.push(token);
  }

  return combined;
}

function formatSqlText(sqlText) {
  const normalizedSql = String(sqlText ?? "").replace(/\r\n?/g, "\n").trim();
  if (!normalizedSql) {
    return "";
  }

  const tokens = combineSqlKeywordTokens(tokenizeSql(normalizedSql));
  const parts = [];
  let lineStart = true;
  let pendingSpace = false;
  let lineIndent = 0;
  let parenDepth = 0;
  let currentClause = null;
  let currentClauseDepth = 0;
  let currentClauseValueIndent = 0;
  let previousToken = null;

  const trimTrailingSpace = () => {
    while (parts[parts.length - 1] === " ") {
      parts.pop();
    }
  };

  const newline = (indent = lineIndent) => {
    trimTrailingSpace();
    if (parts.length && parts[parts.length - 1] !== "\n") {
      parts.push("\n");
    }
    lineStart = true;
    pendingSpace = false;
    lineIndent = Math.max(indent, 0);
  };

  const write = (value, { spaceBefore = true } = {}) => {
    if (!value) {
      return;
    }

    if (lineStart) {
      parts.push("  ".repeat(Math.max(lineIndent, 0)));
      lineStart = false;
    } else if (pendingSpace && spaceBefore) {
      parts.push(" ");
    }

    parts.push(value);
    pendingSpace = false;
  };

  const setClauseState = (keyword, clauseDepth, valueIndent) => {
    currentClause = keyword;
    currentClauseDepth = clauseDepth;
    currentClauseValueIndent = valueIndent;
  };

  tokens.forEach((token, index) => {
    if (token.type === "comment") {
      if (!lineStart) {
        newline(lineIndent);
      }
      write(token.value, { spaceBefore: false });
      if (index < tokens.length - 1) {
        newline(lineIndent);
      }
      previousToken = token;
      return;
    }

    if (token.type === "keyword") {
      if (sqlFormatJoinKeywords.has(token.value)) {
        newline(parenDepth);
        write(token.value, { spaceBefore: false });
        setClauseState(token.value, parenDepth, parenDepth + 1);
        pendingSpace = true;
        previousToken = token;
        return;
      }

      if (token.value === "ON" || token.value === "USING") {
        newline(parenDepth + 1);
        write(token.value, { spaceBefore: false });
        setClauseState(token.value, parenDepth, parenDepth + 1);
        pendingSpace = true;
        previousToken = token;
        return;
      }

      if ((token.value === "AND" || token.value === "OR") && sqlFormatLogicalClauses.has(currentClause)) {
        newline(currentClauseValueIndent);
        write(token.value, { spaceBefore: false });
        pendingSpace = true;
        previousToken = token;
        return;
      }

      if (sqlFormatClauseKeywords.has(token.value)) {
        if (!lineStart) {
          newline(parenDepth);
        }
        write(token.value, { spaceBefore: false });
        const clauseIndent = parenDepth;
        const valueIndent = sqlFormatBreakAfterKeywords.has(token.value) ? clauseIndent + 1 : clauseIndent;
        setClauseState(token.value, clauseIndent, valueIndent);
        if (sqlFormatBreakAfterKeywords.has(token.value)) {
          newline(valueIndent);
        } else {
          pendingSpace = true;
        }
        previousToken = token;
        return;
      }

      if (token.value === "WHEN" || token.value === "ELSE") {
        newline(parenDepth + 1);
        write(token.value, { spaceBefore: false });
        pendingSpace = true;
        previousToken = token;
        return;
      }

      write(token.value);
      pendingSpace = true;
      previousToken = token;
      return;
    }

    if (token.type === "symbol") {
      if (token.value === ",") {
        write(",", { spaceBefore: false });
        if (sqlFormatListKeywords.has(currentClause) && parenDepth === currentClauseDepth) {
          newline(currentClauseValueIndent);
        } else {
          pendingSpace = true;
        }
        previousToken = token;
        return;
      }

      if (token.value === ".") {
        write(".", { spaceBefore: false });
        previousToken = token;
        return;
      }

      if (token.value === "(") {
        write("(", {
          spaceBefore: previousToken?.type === "keyword",
        });
        parenDepth += 1;
        previousToken = token;
        return;
      }

      if (token.value === ")") {
        parenDepth = Math.max(parenDepth - 1, 0);
        if (lineStart) {
          lineIndent = parenDepth;
        }
        write(")", { spaceBefore: false });
        if (currentClause && parenDepth < currentClauseDepth) {
          currentClause = null;
          currentClauseDepth = 0;
          currentClauseValueIndent = 0;
        }
        previousToken = token;
        return;
      }

      if (token.value === ";") {
        write(";", { spaceBefore: false });
        if (index < tokens.length - 1) {
          newline(0);
          newline(0);
        }
        currentClause = null;
        currentClauseDepth = 0;
        currentClauseValueIndent = 0;
        previousToken = token;
        return;
      }

      if (token.value === "::") {
        write("::", { spaceBefore: false });
        previousToken = token;
        return;
      }

      write(token.value);
      pendingSpace = true;
      previousToken = token;
      return;
    }

    write(token.value);
    pendingSpace = true;
    previousToken = token;
  });

  return parts
    .join("")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function defaultLocalNotebookTitle() {
  const localNotebookCount = Object.keys(readStoredNotebookMetadata()).filter((key) =>
    isLocalNotebookId(key)
  ).length;

  return `Untitled Notebook ${localNotebookCount + 1}`;
}

function sqlQueryIdentifier(name) {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    return name;
  }

  return `"${String(name).replace(/"/g, '""')}"`;
}

function sourceQuerySql(relation, fields = []) {
  const fieldNames = normalizeSourceObjectFields(fields).map((field) => field.name);
  if (!fieldNames.length) {
    return `SELECT * FROM ${relation};`;
  }

  return [
    "SELECT",
    fieldNames
      .map(
        (fieldName, index) =>
          `  ${sqlQueryIdentifier(fieldName)}${index < fieldNames.length - 1 ? "," : ""}`
      )
      .join("\n"),
    `FROM ${relation};`,
  ].join("\n");
}

function sourceQueryDescriptor(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    return null;
  }

  const relation = sourceObjectRoot.dataset.sourceObjectRelation?.trim();
  if (!relation) {
    return null;
  }

  return {
    name: sourceObjectRoot.dataset.sourceObjectName?.trim() || relation,
    relation,
    sourceId: sourceObjectRoot.dataset.sourceOptionId?.trim() || "",
  };
}

function emptyQueryResultsMarkup(cellId) {
  return `
    <section id="query-results-${escapeHtml(cellId)}" class="result-panel" data-cell-result hidden>
      <header class="result-header">
        <div>
          <h3>Result</h3>
          <p class="result-meta">0 ms</p>
        </div>
        <span class="result-badge">Run this cell to inspect the selected data sources.</span>
      </header>
      <div class="result-empty">
        <p>Run this cell to inspect the selected data sources.</p>
      </div>
    </section>
  `;
}

function cellSourceSummaryText(dataSources) {
  const labels = sourceLabelsForIds(dataSources);
  if (!labels.length) {
    return "Select sources";
  }
  if (labels.length === 1) {
    return labels[0];
  }
  return `${labels.length} sources`;
}

function cellSourceSummaryMarkup(dataSources) {
  const selectedSources = normalizeDataSources(dataSources);
  const metadataMarkup = selectedSources.length
    ? `
        <span class="cell-source-classification" data-cell-source-classification>${escapeHtml(sourceClassificationDisplayText(selectedSources))}</span>
        <span class="cell-source-computation-mode" data-cell-source-computation-mode title="${escapeHtml(sourceComputationModeTooltipText())}">${escapeHtml(sourceComputationModeDisplayText(selectedSources))}</span>
      `
    : "";

  return `
    <span class="cell-source-summary-label" data-cell-source-summary-label>${escapeHtml(cellSourceSummaryText(dataSources))}</span>
    ${metadataMarkup}
  `;
}

function buildCellMarkup(notebookId, cell, index, canEdit, totalCells) {
  const selectedSources = normalizeDataSources(cell.dataSources);
  const canMoveUp = canEdit && index > 0;
  const canMoveDown = canEdit && index < totalCells - 1;
  const sovereigntyHint =
    "Your data is exclusivly stored and processed in Swiss Government facilities. Hybrid or 3rd-party storage will be available with the Swiss Government Cloud for insensitive data.";
  const sourceOptionsMarkup =
    readSourceOptions()
      .map((option) => {
        const selected = selectedSources.includes(option.source_id);
        return `
          <label class="workspace-source-option cell-source-option${selected ? " is-selected" : ""}">
            <input
              class="workspace-source-checkbox"
              type="checkbox"
              value="${escapeHtml(option.source_id)}"
              data-cell-source-option
              ${selected ? "checked" : ""}
              ${canEdit ? "" : "disabled"}
            >
            <span>${escapeHtml(option.label)}</span>
          </label>
        `;
      })
      .join("") || '<p class="workspace-source-empty">No data sources available.</p>';

  return `
    <article
      class="workspace-cell${cell.cellId === activeCellId ? " is-active" : ""}"
      data-query-cell
      data-cell-id="${escapeHtml(cell.cellId)}"
      data-default-cell-sources="${escapeHtml(selectedSources.join("||"))}"
    >
      <form class="query-form query-form-cell" data-query-form>
        <input type="hidden" name="notebook_id" value="${escapeHtml(notebookId)}">
        <input type="hidden" name="cell_id" value="${escapeHtml(cell.cellId)}">
        <div class="cell-toolbar">
          <div class="cell-heading">
            <span class="cell-label">Cell ${index + 1}</span>
            <span class="workspace-access-badge workspace-access-badge-small" data-cell-access-badge title="${escapeHtml(accessModeHintForDataSources(selectedSources))}">${escapeHtml(accessModeForDataSources(selectedSources))}</span>
            <span class="workspace-access-badge workspace-access-badge-small workspace-access-badge-static" title="${escapeHtml(sovereigntyHint)}">CHE Data Souvereignity</span>
            <details class="cell-source-picker" data-cell-source-picker>
              <summary class="cell-source-picker-toggle" data-cell-source-summary>${cellSourceSummaryMarkup(selectedSources)}</summary>
              <div class="cell-source-selection" data-cell-source-selection>
                ${sourceOptionsMarkup}
              </div>
            </details>
          </div>
          <div class="cell-actions">
            <div class="cell-run-actions">
              <button class="run-button" type="submit" title="Run with Ctrl/Cmd + Enter" data-run-cell>Run Cell</button>
              <button class="query-cancel-button" type="button" data-cancel-query hidden>Cancel</button>
            </div>
            <details class="workspace-action-menu cell-action-menu" data-cell-action-menu>
              <summary class="workspace-action-menu-toggle" aria-label="Cell actions" title="Cell actions">
                <span class="workspace-action-menu-dots" aria-hidden="true">...</span>
              </summary>
              <div class="workspace-action-menu-panel">
                <button type="button" class="workspace-action-menu-item${canEdit ? "" : " is-action-disabled"}" data-format-cell-sql ${canEdit ? "" : "disabled"} title="${canEdit ? "Format SQL" : "This notebook cannot be edited."}">Format SQL</button>
                <button type="button" class="workspace-action-menu-item workspace-action-menu-item-placeholder is-action-disabled" disabled title="disabled.">Optimize SQL</button>
                <button type="button" class="workspace-action-menu-item workspace-action-menu-item-placeholder is-action-disabled" disabled title="disabled.">Explain SQL Execution Plan</button>
                <button type="button" class="workspace-action-menu-item workspace-action-menu-item-placeholder is-action-disabled" disabled title="disabled.">Explain Semantics of this Query</button>
                <div class="workspace-action-menu-separator" aria-hidden="true"></div>
                <button type="button" class="workspace-action-menu-item workspace-action-menu-item-no-strike${canMoveUp ? "" : " is-action-disabled"}" data-move-cell-up ${canMoveUp ? "" : "disabled"} title="${
                  canMoveUp
                    ? "Move cell up"
                    : canEdit
                      ? "This cell is already first."
                      : "This notebook cannot be edited."
                }">Move up</button>
                <button type="button" class="workspace-action-menu-item workspace-action-menu-item-no-strike${canMoveDown ? "" : " is-action-disabled"}" data-move-cell-down ${canMoveDown ? "" : "disabled"} title="${
                  canMoveDown
                    ? "Move cell down"
                    : canEdit
                      ? "This cell is already last."
                      : "This notebook cannot be edited."
                }">Move down</button>
                <div class="workspace-action-menu-separator" aria-hidden="true"></div>
                <button type="button" class="workspace-action-menu-item${canEdit ? "" : " is-action-disabled"}" data-add-cell-after ${canEdit ? "" : "disabled"} title="${canEdit ? "Add cell below" : "This notebook cannot be edited."}">Add cell</button>
                <button type="button" class="workspace-action-menu-item${canEdit ? "" : " is-action-disabled"}" data-copy-cell ${canEdit ? "" : "disabled"} title="${canEdit ? "Copy cell" : "This notebook cannot be edited."}">Copy cell</button>
                <button type="button" class="workspace-action-menu-item workspace-action-menu-item-danger${canEdit ? "" : " is-action-disabled"}" data-delete-cell ${canEdit ? "" : "disabled"} title="${canEdit ? "Delete cell" : "This notebook cannot be edited."}">Delete cell</button>
              </div>
            </details>
          </div>
        </div>
        <div class="editor-frame" data-editor-root data-editor-name="sql-${escapeHtml(cell.cellId)}">
          <textarea name="sql" data-editor-source data-default-sql="${escapeHtml(cell.sql)}" rows="3" spellcheck="false">${escapeHtml(cell.sql)}</textarea>
        </div>
      </form>
      ${emptyQueryResultsMarkup(cell.cellId)}
    </article>
  `;
}

function buildWorkspaceMarkup(notebookId, metadata) {
  const tagsMarkup = metadata.tags
    .map(
      (tag) => `
        <button type="button" class="workspace-tag-chip" data-tag-remove="${escapeHtml(tag)}">
          <span>${escapeHtml(tag)}</span>
          <span class="workspace-tag-remove" aria-hidden="true">×</span>
        </button>
      `
    )
    .join("");
  const currentVersion = metadata.versions?.[0] ?? null;
  const versionSummaryMarkup = currentVersion
    ? `
        <span class="workspace-version-current-stack">
          <span class="workspace-version-current-primary">
            <span class="workspace-version-current-timestamp">${escapeHtml(formatVersionTimestamp(currentVersion.createdAt))}</span>
            <span class="workspace-version-current-name">${escapeHtml(currentVersion.title || "Notebook version")}</span>
          </span>
          <span class="workspace-version-current-secondary">${escapeHtml(truncateWords(currentVersion.summary || "No description saved.", 10))}</span>
        </span>
      `
    : `<span class="workspace-version-current-empty">No saved versions yet.</span>`;
  const cellsMarkup = (metadata.cells ?? [])
    .map((cell, index, cells) => buildCellMarkup(notebookId, cell, index, metadata.canEdit, cells.length))
    .join("");

  return `
    <article
      class="workspace-card"
      data-workspace-notebook
      data-notebook-meta
      data-notebook-id="${escapeHtml(notebookId)}"
      data-can-edit="true"
      data-can-delete="true"
      data-default-title="${escapeHtml(metadata.title)}"
      data-default-summary="${escapeHtml(metadata.summary)}"
      data-default-cells='${escapeHtml(JSON.stringify((metadata.cells ?? []).map((cell) => ({
        cellId: cell.cellId,
        dataSources: normalizeDataSources(cell.dataSources),
        sql: cell.sql,
      }))))}'
      data-default-versions='${escapeHtml(JSON.stringify((metadata.versions ?? []).map((version) => ({
        versionId: version.versionId,
        createdAt: version.createdAt,
        title: version.title,
        summary: version.summary,
        tags: normalizeTags(version.tags),
        cells: normalizeNotebookCells(version.cells).map((cell) => ({
          cellId: cell.cellId,
          dataSources: normalizeDataSources(cell.dataSources),
          sql: cell.sql,
        })),
      }))))}'
      data-default-tags="${escapeHtml(metadata.tags.join("||"))}"
    >
      <header class="workspace-header">
        <div class="workspace-title-block">
          <div class="workspace-title-row">
            <h2 class="workspace-notebook-title is-editable" data-notebook-title-display data-rename-notebook-title tabindex="0" role="button" title="Click to rename the notebook">${escapeHtml(metadata.title)}</h2>
          </div>
          <div class="workspace-summary-field" data-summary-container>
            <p class="workspace-summary-display is-editable" data-summary-display tabindex="0" role="button" title="Click to edit the notebook description">${escapeHtml(metadata.summary)}</p>
            <textarea class="workspace-summary-input" data-summary-input rows="3" placeholder="Notebook description">${escapeHtml(metadata.summary)}</textarea>
          </div>
          <div class="workspace-header-tags">
            <div class="workspace-tag-toolbar">
              <div class="workspace-tag-list" data-tag-list>${tagsMarkup}</div>
              <button type="button" class="workspace-tag-badge workspace-tag-badge-add" data-tag-toggle title="Add tag" aria-label="Add tag">+</button>
            </div>
            <div class="workspace-tag-controls workspace-tag-controls-inline" data-tag-controls hidden>
              <input class="workspace-tag-input workspace-tag-input-inline" type="text" placeholder="Add tag" data-tag-input>
              <button class="workspace-tag-add workspace-tag-add-inline" type="button" data-tag-add>Add</button>
            </div>
          </div>
        </div>
        <div class="workspace-actions">
          <details class="workspace-action-menu" data-workspace-action-menu>
            <summary class="workspace-action-menu-toggle" aria-label="Notebook actions" title="Notebook actions">
              <span class="workspace-action-menu-dots" aria-hidden="true">•••</span>
            </summary>
            <div class="workspace-action-menu-panel">
              <button type="button" class="workspace-action-menu-item" data-rename-notebook title="Rename notebook">Rename</button>
              <button type="button" class="workspace-action-menu-item" data-edit-notebook title="Edit notebook metadata">Edit</button>
              <button type="button" class="workspace-action-menu-item" data-copy-notebook title="Create a copy of this notebook">Copy notebook</button>
              <button type="button" class="workspace-action-menu-item workspace-action-menu-item-danger" data-delete-notebook title="Delete notebook">Delete</button>
            </div>
          </details>
        </div>
      </header>

      <section class="workspace-version-strip" data-version-strip>
        <div class="workspace-version-strip-header">
          <div class="workspace-version-strip-copy">
            <span class="workspace-tags-label">Versions</span>
            <button type="button" class="workspace-version-current" data-version-toggle aria-expanded="false" title="${escapeHtml(currentVersion ? "Expand version history" : "No saved versions yet.")}"${currentVersion ? "" : " disabled"}>
              <span class="workspace-version-current-copy" data-version-current>${versionSummaryMarkup}</span>
              <span class="workspace-version-toggle-icon" data-version-toggle-icon aria-hidden="true">›</span>
            </button>
          </div>
          <button type="button" class="workspace-version-save" data-save-version>Save version</button>
        </div>
        <div class="workspace-version-panel" data-version-panel hidden>
          <div class="workspace-version-list" data-version-list></div>
        </div>
      </section>

      <section class="workspace-cells" data-cell-list>
        ${cellsMarkup}
      </section>
      <div class="workspace-cell-footer">
        <button type="button" class="workspace-cell-add-button" data-add-cell>Add Cell</button>
      </div>
    </article>
  `;
}

function createNotebookLinkElement(notebookId, metadata) {
  const link = document.createElement("a");
  link.href = "#";
  link.className = "notebook-link notebook-tree-leaf";
  link.dataset.notebookId = notebookId;
  link.dataset.notebookTitle = metadata.title;
  link.dataset.notebookSummary = metadata.summary;
  link.dataset.notebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookTitle = metadata.title;
  link.dataset.defaultNotebookSummary = metadata.summary;
  link.dataset.defaultNotebookCells = JSON.stringify(
    (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      dataSources: normalizeDataSources(cell.dataSources),
      sql: cell.sql,
    }))
  );
  link.dataset.defaultNotebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookTags = metadata.tags.join("||");
  link.dataset.canEdit = metadata.canEdit ? "true" : "false";
  link.dataset.canDelete = metadata.canDelete ? "true" : "false";
  link.dataset.draggableNotebook = "";
  link.draggable = true;

  const titleRow = document.createElement("span");
  titleRow.className = "notebook-title-row";

  const title = document.createElement("span");
  title.className = "notebook-title";
  title.textContent = metadata.title;

  const tools = document.createElement("span");
  tools.className = "notebook-item-tools";

  const renameButton = document.createElement("button");
  renameButton.type = "button";
  renameButton.className = `tree-add-button tree-add-button-inline notebook-action-pill${
    metadata.canEdit ? "" : " is-action-disabled"
  }`;
  renameButton.dataset.sidebarRenameNotebook = "";
  renameButton.textContent = "Rename";
  renameButton.title = metadata.canEdit
    ? "Rename notebook"
    : "This notebook cannot be renamed.";
  renameButton.disabled = !metadata.canEdit;

  const editButton = document.createElement("button");
  editButton.type = "button";
  editButton.className = `tree-add-button tree-add-button-inline notebook-action-pill${
    metadata.canEdit ? "" : " is-action-disabled"
  }`;
  editButton.dataset.sidebarEditNotebook = "";
  editButton.textContent = "Edit";
  editButton.title = metadata.canEdit
    ? "Edit notebook metadata"
    : "This notebook cannot be edited.";
  editButton.disabled = !metadata.canEdit;

  const deleteButton = document.createElement("button");
  deleteButton.type = "button";
  deleteButton.className = `tree-add-button tree-add-button-inline notebook-action-pill tree-delete-button${
    metadata.canDelete ? "" : " is-action-disabled"
  }`;
  deleteButton.dataset.sidebarDeleteNotebook = "";
  deleteButton.textContent = "Delete";
  deleteButton.title = metadata.canDelete
    ? "Delete notebook"
    : "This notebook cannot be deleted.";
  deleteButton.disabled = !metadata.canDelete;

  tools.append(renameButton, editButton, deleteButton);
  titleRow.append(title, tools);
  link.append(titleRow);

  const summary = document.createElement("span");
  summary.className = "notebook-summary";
  summary.textContent = metadata.summary;
  link.append(summary);

  renderSidebarTags(link, metadata.tags);
  updateNotebookSearchableItem(link, metadata);
  return link;
}

function notebookMetadata(notebookId) {
  const defaults = readNotebookDefaults(notebookId);
  const storedState = normalizeStoredNotebookState(readStoredNotebookMetadata()[notebookId]);
  const cells = normalizeNotebookCells(storedState.cells ?? defaults.cells);
  const baseMetadata = {
    ...defaults,
    notebookId,
    title: storedState.title ?? defaults.title,
    summary: storedState.summary ?? defaults.summary,
    cells,
    dataSources: notebookSourceIds({ cells }),
    tags: normalizeTags(storedState.tags ?? defaults.tags),
    sql: cells[0]?.sql ?? "",
    deleted: storedState.deleted ?? defaults.deleted,
  };
  const versions =
    storedState.versions && storedState.versions.length
      ? storedState.versions
      : [createInitialNotebookVersion(notebookId, baseMetadata)];

  if (!storedState.versions || !storedState.versions.length) {
    updateStoredNotebookState(notebookId, (currentState) => ({
      ...currentState,
      title: currentState.title ?? baseMetadata.title,
      summary: currentState.summary ?? baseMetadata.summary,
      tags: currentState.tags ?? baseMetadata.tags,
      cells: currentState.cells ?? baseMetadata.cells,
      deleted: currentState.deleted ?? baseMetadata.deleted,
      versions,
    }));
  }

  return {
    ...baseMetadata,
    versions,
  };
}

function updateStoredNotebookState(notebookId, updater) {
  const state = readStoredNotebookMetadata();
  const currentState = normalizeStoredNotebookState(state[notebookId]);
  const nextState = normalizeStoredNotebookState(updater({ ...currentState }));
  state[notebookId] = nextState;
  writeStoredNotebookMetadata(state);
  return nextState;
}

function persistNotebookDraft(notebookId, draftPatch) {
  updateStoredNotebookState(notebookId, (currentState) => ({
    ...currentState,
    ...draftPatch,
    cells:
      draftPatch.cells !== undefined
        ? normalizeNotebookCells(draftPatch.cells, {
            dataSources: currentState.dataSources ?? [],
            sql: currentState.sql ?? "",
          })
        : currentState.cells,
    tags:
      draftPatch.tags !== undefined
        ? normalizeTags(draftPatch.tags)
        : currentState.tags,
  }));
}

function createNotebookVersionSnapshot(metadata) {
  return {
    versionId: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    createdAt: new Date().toISOString(),
    title: metadata.title,
    summary: metadata.summary,
    tags: normalizeTags(metadata.tags),
    cells: (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      dataSources: normalizeDataSources(cell.dataSources),
      sql: cell.sql,
    })),
  };
}

function formatVersionTimestamp(value) {
  const timestamp = Date.parse(value || "");
  if (!timestamp) {
    return "Unknown";
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(timestamp));
}

function truncateWords(value, maxWords = 6) {
  const text = String(value ?? "").trim().replace(/\s+/g, " ");
  if (!text) {
    return "";
  }

  const words = text.split(" ");
  if (words.length <= maxWords) {
    return text;
  }

  return `${words.slice(0, maxWords).join(" ")}…`;
}

function createVersionListEntry(version) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "workspace-version-item";
  button.dataset.versionId = version.versionId;
  button.dataset.versionLoad = "";

  const title = document.createElement("div");
  title.className = "workspace-version-title";

  const timestamp = document.createElement("span");
  timestamp.className = "workspace-version-timestamp";
  timestamp.textContent = formatVersionTimestamp(version.createdAt);

  const name = document.createElement("span");
  name.className = "workspace-version-name";
  name.textContent = version.title || "Notebook version";

  const description = document.createElement("span");
  description.className = "workspace-version-description";
  description.textContent = truncateWords(version.summary || "No description saved.", 6);

  const cellLines = (version.cells ?? []).map((cell, index) => {
    const sources = sourceLabelsForIds(cell.dataSources).join(", ") || "No data sources";
    const sqlText = cell.sql || "No SQL saved.";
    return `Cell ${index + 1} Sources: ${sources}\nCell ${index + 1} SQL:\n${sqlText}`;
  });
  const tooltipLines = [
    `Description: ${version.summary || "No description saved."}`,
    `Tags: ${version.tags.length ? version.tags.join(", ") : "No tags"}`,
    "",
    ...cellLines,
  ];
  button.title = tooltipLines.join("\n");

  title.append(timestamp, name);
  button.append(title, description);
  return button;
}

function createVersionCurrentSummary(version) {
  const wrapper = document.createElement("span");
  wrapper.className = "workspace-version-current-stack";

  if (!version) {
    const empty = document.createElement("span");
    empty.className = "workspace-version-current-empty";
    empty.textContent = "No saved versions yet.";
    wrapper.append(empty);
    return wrapper;
  }

  const primary = document.createElement("span");
  primary.className = "workspace-version-current-primary";

  const timestamp = document.createElement("span");
  timestamp.className = "workspace-version-current-timestamp";
  timestamp.textContent = formatVersionTimestamp(version.createdAt);

  const name = document.createElement("span");
  name.className = "workspace-version-current-name";
  name.textContent = version.title || "Notebook version";

  const secondary = document.createElement("span");
  secondary.className = "workspace-version-current-secondary";
  secondary.textContent = truncateWords(version.summary || "No description saved.", 10);

  primary.append(timestamp, name);
  wrapper.append(primary, secondary);
  return wrapper;
}

function setVersionPanelExpanded(metaRoot, expanded) {
  const panel = metaRoot.querySelector("[data-version-panel]");
  const toggle = metaRoot.querySelector("[data-version-toggle]");
  if (!panel || !toggle) {
    return;
  }

  const nextExpanded = Boolean(expanded) && !toggle.disabled;
  panel.hidden = !nextExpanded;
  toggle.setAttribute("aria-expanded", nextExpanded ? "true" : "false");
  toggle.title = nextExpanded ? "Collapse version history" : "Expand version history";
}

function renderWorkspaceVersions(metaRoot, versions) {
  const versionList = metaRoot.querySelector("[data-version-list]");
  const versionCurrent = metaRoot.querySelector("[data-version-current]");
  const versionToggle = metaRoot.querySelector("[data-version-toggle]");
  const panel = metaRoot.querySelector("[data-version-panel]");
  if (!versionList || !versionCurrent || !versionToggle || !panel) {
    return;
  }

  const wasExpanded = !panel.hidden;
  versionCurrent.replaceChildren(createVersionCurrentSummary(versions[0]));

  if (!versions.length) {
    const emptyState = document.createElement("div");
    emptyState.className = "workspace-version-empty";
    emptyState.textContent = "No saved versions yet.";
    versionList.replaceChildren(emptyState);
    versionToggle.disabled = true;
    setVersionPanelExpanded(metaRoot, false);
    return;
  }

  versionToggle.disabled = false;
  versionList.replaceChildren(...versions.map((version) => createVersionListEntry(version)));
  setVersionPanelExpanded(metaRoot, wasExpanded);
}

function updateNotebookSearchableItem(link, metadata) {
  link.dataset.searchableItem = [
    metadata.title,
    metadata.summary,
    ...sourceLabelsForIds(metadata.dataSources),
    ...metadata.tags,
  ]
    .filter(Boolean)
    .join(" ");
}

function updateSidebarNotebookLink(link, metadata) {
  link.dataset.notebookTitle = metadata.title;
  link.dataset.notebookSummary = metadata.summary;
  link.dataset.notebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookCells = JSON.stringify(
    (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      dataSources: normalizeDataSources(cell.dataSources),
      sql: cell.sql,
    }))
  );

  const titleNode = link.querySelector(".notebook-title");
  if (titleNode) {
    titleNode.textContent = metadata.title;
  }

  const summaryNode = link.querySelector(".notebook-summary");
  if (summaryNode) {
    summaryNode.textContent = metadata.summary;
  }

  const renameButton = link.querySelector("[data-sidebar-rename-notebook]");
  if (renameButton) {
    renameButton.disabled = !metadata.canEdit;
    renameButton.classList.toggle("is-action-disabled", !metadata.canEdit);
    renameButton.title = metadata.canEdit
      ? "Rename notebook"
      : "This notebook cannot be renamed.";
  }

  const editButton = link.querySelector("[data-sidebar-edit-notebook]");
  if (editButton) {
    editButton.disabled = !metadata.canEdit;
    editButton.classList.toggle("is-action-disabled", !metadata.canEdit);
    editButton.title = metadata.canEdit
      ? "Edit notebook metadata"
      : "This notebook cannot be edited.";
  }

  const deleteButton = link.querySelector("[data-sidebar-delete-notebook]");
  if (deleteButton) {
    deleteButton.disabled = !metadata.canDelete;
    deleteButton.classList.toggle("is-action-disabled", !metadata.canDelete);
    deleteButton.title = metadata.canDelete
      ? "Delete notebook"
      : "This notebook cannot be deleted.";
  }

  renderSidebarTags(link, metadata.tags);
  updateNotebookSearchableItem(link, metadata);
}

function setNotebookTitle(notebookId, title) {
  persistNotebookDraft(notebookId, { title });
  applyNotebookMetadata();
}

function setNotebookSummary(notebookId, summary) {
  persistNotebookDraft(notebookId, { summary });
  const metadata = notebookMetadata(notebookId);
  notebookLinks(notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));
  const summaryDisplay = activeWorkspaceMetaRoot(notebookId)
    ?.closest("[data-workspace-notebook]")
    ?.querySelector("[data-summary-display]");
  if (summaryDisplay) {
    summaryDisplay.textContent = metadata.summary;
  }
  applySidebarSearchFilter();
}

function createEmptyCellState(initial = {}) {
  return normalizeCellEntry(
    {
      cellId: initial.cellId ?? createCellId(),
      dataSources: initial.dataSources ?? [],
      sql: initial.sql ?? "",
    },
    {
      cellId: initial.cellId ?? createCellId(),
      dataSources: initial.dataSources ?? [],
      sql: initial.sql ?? "",
    }
  );
}

function createSourceQueryCellState(sourceDescriptor, fields = []) {
  return createEmptyCellState({
    dataSources: sourceDescriptor?.sourceId ? [sourceDescriptor.sourceId] : [],
    sql: sourceQuerySql(sourceDescriptor?.relation ?? "", fields),
  });
}

function setNotebookCells(notebookId, cells, options = {}) {
  persistNotebookDraft(notebookId, { cells: normalizeNotebookCells(cells) });
  const metadata = notebookMetadata(notebookId);
  notebookLinks(notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));

  if (options.rerender && isLocalNotebookId(notebookId)) {
    renderLocalNotebookWorkspace(notebookId);
    return metadata;
  }

  applyNotebookMetadata();
  applySidebarSearchFilter();
  return metadata;
}

function setNotebookTags(notebookId, tags) {
  persistNotebookDraft(notebookId, { tags: normalizeTags(tags) });
  applyNotebookMetadata();
}

function setCellDataSources(notebookId, cellId, dataSources) {
  updateStoredNotebookState(notebookId, (currentState) => {
    const baseCells = normalizeNotebookCells(currentState.cells ?? notebookMetadata(notebookId).cells);
    return {
      ...currentState,
      cells: baseCells.map((cell) =>
        cell.cellId === cellId
          ? {
              ...cell,
              dataSources: normalizeDataSources(dataSources),
            }
          : cell
      ),
    };
  });

  const metadata = notebookMetadata(notebookId);
  notebookLinks(notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));
  applyNotebookMetadata();
  applySidebarSearchFilter();
}

function setCellSql(notebookId, cellId, sqlText) {
  updateStoredNotebookState(notebookId, (currentState) => {
    const baseCells = normalizeNotebookCells(currentState.cells ?? notebookMetadata(notebookId).cells);
    return {
      ...currentState,
      cells: baseCells.map((cell) =>
        cell.cellId === cellId
          ? {
              ...cell,
              sql: sqlText,
            }
          : cell
      ),
    };
  });
}

function saveNotebookVersion(notebookId) {
  const metadata = notebookMetadata(notebookId);
  const version = createNotebookVersionSnapshot(metadata);
  updateStoredNotebookState(notebookId, (currentState) => ({
    ...currentState,
    title: metadata.title,
    summary: metadata.summary,
    tags: metadata.tags,
    cells: metadata.cells,
    versions: [version, ...(currentState.versions ?? [])],
  }));
  applyNotebookMetadata();
}

async function loadNotebookVersion(notebookId, versionId) {
  const metadata = notebookMetadata(notebookId);
  const version = metadata.versions.find((item) => item.versionId === versionId);
  if (!version) {
    return;
  }

  const { confirmed } = await showConfirmDialog({
    title: "Load notebook version",
    copy: "Load this version and discard the current notebook state?",
    confirmLabel: "Load version",
  });
  if (!confirmed) {
    return;
  }

  persistNotebookDraft(notebookId, {
    title: version.title,
    summary: version.summary,
    tags: version.tags,
    cells: version.cells,
  });
  if (isLocalNotebookId(notebookId)) {
    renderLocalNotebookWorkspace(notebookId);
    return;
  }

  applyNotebookMetadata();
}

function addCell(notebookId, afterCellId = null) {
  const metadata = notebookMetadata(notebookId);
  if (!metadata.canEdit) {
    return;
  }

  const nextCell = createEmptyCellState();
  const nextCells = [...metadata.cells];

  if (!afterCellId) {
    nextCells.push(nextCell);
  } else {
    const index = nextCells.findIndex((cell) => cell.cellId === afterCellId);
    if (index === -1) {
      nextCells.push(nextCell);
    } else {
      nextCells.splice(index + 1, 0, nextCell);
    }
  }

  setNotebookCells(notebookId, nextCells, { rerender: true });
}

function duplicateCell(notebookId, cellId) {
  const metadata = notebookMetadata(notebookId);
  if (!metadata.canEdit) {
    return;
  }

  const nextCells = [...metadata.cells];
  const index = nextCells.findIndex((cell) => cell.cellId === cellId);
  if (index === -1) {
    return;
  }

  const duplicate = createEmptyCellState({
    dataSources: [...nextCells[index].dataSources],
    sql: nextCells[index].sql,
  });
  nextCells.splice(index + 1, 0, duplicate);
  setNotebookCells(notebookId, nextCells, { rerender: true });
}

function moveCell(notebookId, cellId, direction) {
  const metadata = notebookMetadata(notebookId);
  if (!metadata.canEdit) {
    return;
  }

  const nextCells = [...metadata.cells];
  const index = nextCells.findIndex((cell) => cell.cellId === cellId);
  if (index === -1) {
    return;
  }

  const targetIndex = direction === "up" ? index - 1 : direction === "down" ? index + 1 : index;
  if (targetIndex < 0 || targetIndex >= nextCells.length || targetIndex === index) {
    return;
  }

  const [movedCell] = nextCells.splice(index, 1);
  nextCells.splice(targetIndex, 0, movedCell);
  activeCellId = cellId;
  setNotebookCells(notebookId, nextCells, { rerender: true });
  setActiveCell(
    Array.from(document.querySelectorAll("[data-query-cell]")).find((cellRoot) => cellRoot.dataset.cellId === cellId) ??
      null
  );
}

function deleteCell(notebookId, cellId) {
  const metadata = notebookMetadata(notebookId);
  if (!metadata.canEdit) {
    return;
  }

  const remainingCells = metadata.cells.filter((cell) => cell.cellId !== cellId);
  setNotebookCells(
    notebookId,
    remainingCells.length ? remainingCells : [createEmptyCellState()],
    { rerender: true }
  );
}

function numericCssValue(styles, property) {
  return Number.parseFloat(styles?.[property] ?? "") || 0;
}

function editorHeightMetrics(root) {
  const editor = editorRegistry.get(root);
  if (editor) {
    const editorStyles = window.getComputedStyle(editor.dom);
    const scroller = editor.dom.querySelector(".cm-scroller");
    const scrollerStyles = scroller ? window.getComputedStyle(scroller) : null;
    const lineHeight =
      editor.defaultLineHeight ||
      numericCssValue(editorStyles, "lineHeight") ||
      numericCssValue(scrollerStyles, "lineHeight") ||
      22;
    const borderHeight =
      numericCssValue(editorStyles, "borderTopWidth") +
      numericCssValue(editorStyles, "borderBottomWidth");
    const scrollerPadding =
      numericCssValue(scrollerStyles, "paddingTop") +
      numericCssValue(scrollerStyles, "paddingBottom");
    const minHeight = Math.ceil(lineHeight * 3 + scrollerPadding + borderHeight);
    const maxAutoHeight = Math.ceil(lineHeight * 10 + scrollerPadding + borderHeight);
    const contentHeight = Math.ceil((scroller?.scrollHeight ?? editor.dom.scrollHeight) + borderHeight);
    return {
      minHeight,
      nextHeight: Math.max(minHeight, Math.min(contentHeight, maxAutoHeight)),
    };
  }

  const textarea = root.querySelector("[data-editor-source]");
  if (!textarea) {
    return null;
  }

  const styles = window.getComputedStyle(textarea);
  const lineHeight = numericCssValue(styles, "lineHeight") || 22;
  const chromeHeight =
    numericCssValue(styles, "paddingTop") +
    numericCssValue(styles, "paddingBottom") +
    numericCssValue(styles, "borderTopWidth") +
    numericCssValue(styles, "borderBottomWidth");
  const minHeight = Math.ceil(lineHeight * 3 + chromeHeight);
  const maxAutoHeight = Math.ceil(lineHeight * 10 + chromeHeight);

  const previousHeight = textarea.style.height;
  textarea.style.height = "auto";
  const contentHeight = Math.ceil(textarea.scrollHeight);
  textarea.style.height = previousHeight;

  return {
    minHeight,
    nextHeight: Math.max(minHeight, Math.min(contentHeight, maxAutoHeight)),
  };
}

function editorSizingState(root) {
  let state = editorSizingRegistry.get(root);
  if (!state) {
    state = {
      applying: false,
      autoHeight: 0,
      manual: false,
      observer: null,
    };
    editorSizingRegistry.set(root, state);
  }
  return state;
}

function observeEditorResize(root) {
  if (!(root instanceof Element) || typeof window.ResizeObserver !== "function") {
    return;
  }

  const state = editorSizingState(root);
  if (state.observer) {
    return;
  }

  state.observer = new window.ResizeObserver((entries) => {
    const nextHeight = entries[0]?.contentRect?.height ?? 0;
    if (!nextHeight || state.applying || !state.autoHeight) {
      return;
    }

    state.manual = Math.abs(nextHeight - state.autoHeight) > 2;
  });
  state.observer.observe(root);
}

function autosizeEditor(root) {
  if (!(root instanceof Element)) {
    return;
  }

  observeEditorResize(root);
  const state = editorSizingState(root);
  if (state.manual) {
    return;
  }

  const metrics = editorHeightMetrics(root);
  if (!metrics) {
    return;
  }

  state.autoHeight = metrics.nextHeight;
  state.applying = true;
  root.style.minHeight = `${metrics.minHeight}px`;
  root.style.height = `${metrics.nextHeight}px`;
  window.setTimeout(() => {
    state.applying = false;
  }, 0);
}

function createEditor(root) {
  if (editorRegistry.has(root)) {
    return editorRegistry.get(root);
  }

  const textarea = root.querySelector("[data-editor-source]");
  if (!textarea) {
    return null;
  }

  const schema = readSchema();
  const form = root.closest("form");
  const shell = document.createElement("div");
  shell.className = "editor-shell";
  root.appendChild(shell);

  try {
    const editor = new EditorView({
      doc: textarea.value,
      extensions: [
        basicSetup,
        sql({
          dialect: PostgreSQL,
          schema,
          upperCaseKeywords: true,
        }),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            textarea.value = update.state.doc.toString();
            autosizeEditor(root);
            const workspaceRoot = root.closest("[data-workspace-notebook]") ?? root;
            const notebookId = workspaceNotebookId(workspaceRoot);
            const cellId = root.closest("[data-query-cell]")?.dataset.cellId;
            if (!applyingNotebookState && notebookId && cellId) {
              setCellSql(notebookId, cellId, textarea.value);
            }
          }
        }),
      ],
      parent: shell,
    });

    editor.dom.addEventListener("keydown", (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key === "Enter" && form) {
        event.preventDefault();
        textarea.value = editor.state.doc.toString();
        form.requestSubmit();
      }
    });

    root.classList.add("editor-ready");
    editorRegistry.set(root, editor);
    autosizeEditor(root);
    return editor;
  } catch (error) {
    shell.remove();
    console.error("Failed to initialize CodeMirror. Falling back to textarea.", error);
    autosizeEditor(root);
    return null;
  }
}

function initializeEditors(root = document) {
  root.querySelectorAll("[data-editor-root]").forEach((editorRoot) => {
    createEditor(editorRoot);
  });
}

function createSidebarTag(tag) {
  const node = document.createElement("small");
  node.className = "notebook-tag";
  node.textContent = tag;
  return node;
}

function renderSidebarTags(link, tags) {
  let container = link.querySelector(".notebook-tags");
  if (!tags.length) {
    container?.remove();
    return;
  }

  if (!container) {
    container = document.createElement("span");
    container.className = "notebook-tags";
    link.appendChild(container);
  }

  container.replaceChildren(...tags.map((tag) => createSidebarTag(tag)));
}

function createWorkspaceTagChip(tag, editable = true) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "workspace-tag-chip";
  button.dataset.tagRemove = tag;
  button.disabled = !editable;

  const label = document.createElement("span");
  label.textContent = tag;

  const remove = document.createElement("span");
  remove.className = "workspace-tag-remove";
  remove.setAttribute("aria-hidden", "true");
  remove.textContent = "\u00D7";

  button.append(label, remove);
  return button;
}

function renderWorkspaceTags(metaRoot, tags, editable = true) {
  const tagList = metaRoot.querySelector("[data-tag-list]");
  if (!tagList) {
    return;
  }

  tagList.replaceChildren(...tags.map((tag) => createWorkspaceTagChip(tag, editable)));
}

function setInputValue(input, value) {
  if (!input || input.value === value) {
    return;
  }

  input.value = value;
}

function setSummaryEditing(workspaceRoot, editing) {
  const container = workspaceRoot?.querySelector("[data-summary-container]");
  const input = container?.querySelector("[data-summary-input]");
  if (!container || !input || input.disabled) {
    return;
  }

  container.classList.toggle("is-editing", editing);
  if (editing) {
    input.focus();
    input.select();
  }
}

function setTagControlsOpen(metaRoot, open) {
  const controls = metaRoot?.querySelector("[data-tag-controls]");
  if (!controls) {
    return;
  }

  controls.hidden = !open;
  if (!open) {
    const input = controls.querySelector("[data-tag-input]");
    if (input) {
      input.value = "";
    }
    return;
  }

  const input = controls.querySelector("[data-tag-input]");
  if (input && !input.disabled) {
    input.focus();
    input.select();
  }
}

function syncWorkspaceActionButton(button, { allowed, enabledTitle, disabledTitle }) {
  if (!button) {
    return;
  }

  button.disabled = !allowed;
  button.classList.toggle("is-action-disabled", !allowed);
  button.title = allowed ? enabledTitle : disabledTitle;
}

function closeWorkspaceActionMenus() {
  document.querySelectorAll("[data-workspace-action-menu][open]").forEach((menu) => {
    menu.removeAttribute("open");
  });
}

function visibleNotebookLinks() {
  return Array.from(document.querySelectorAll("[data-draggable-notebook]")).filter((link) => !link.hidden);
}

function nextVisibleNotebookId(currentNotebookId) {
  const notebooks = visibleNotebookLinks();
  if (!notebooks.length) {
    return null;
  }

  const currentIndex = notebooks.findIndex((link) => link.dataset.notebookId === currentNotebookId);
  if (currentIndex < 0) {
    return notebooks[0]?.dataset.notebookId ?? null;
  }

  return (
    notebooks[currentIndex + 1]?.dataset.notebookId ??
    notebooks[currentIndex - 1]?.dataset.notebookId ??
    null
  );
}

function renderEmptyWorkspace() {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  panel.innerHTML = `
    <article class="workspace-card">
      <header class="workspace-header">
        <div class="workspace-title-block">
          <p class="workspace-kicker">Notebook</p>
          <h2>No notebook selected</h2>
          <p class="workspace-summary">Select a notebook from the navigation to continue.</p>
        </div>
      </header>
    </article>
  `;
  renderQueryNotificationMenu();
}

function renderLocalNotebookWorkspace(notebookId) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  const metadata = notebookMetadata(notebookId);
  panel.innerHTML = buildWorkspaceMarkup(notebookId, metadata);
  processHtmx(panel);
  initializeEditors(panel);
  applyNotebookMetadata();
  activateNotebookLink(notebookId);
  revealNotebookLink(notebookId);
  writeLastNotebookId(notebookId);
  syncVisibleQueryCells();
}

function defaultNotebookCreateTarget() {
  return directChildrenContainer(ensureRootUnassignedFolder());
}

function resolveNotebookCreateTarget(button) {
  const folder = button.closest("[data-tree-folder]");
  if (folder) {
    folder.open = true;
    return directChildrenContainer(folder);
  }

  const unassignedFolder = ensureRootUnassignedFolder();
  return directChildrenContainer(unassignedFolder);
}

function createNotebook(targetContainer, initialMetadata = {}) {
  if (!targetContainer) {
    return null;
  }

  const notebookId = `${localNotebookPrefix}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
  const metadata = {
    title: initialMetadata.title ?? defaultLocalNotebookTitle(),
    summary: initialMetadata.summary ?? "Describe this notebook.",
    cells: normalizeNotebookCells(initialMetadata.cells ?? [createEmptyCellState()]),
    tags: normalizeTags(initialMetadata.tags ?? []),
    canEdit: true,
    canDelete: true,
    deleted: false,
    versions: [],
  };
  metadata.versions = [createInitialNotebookVersion(notebookId, metadata)];

  persistNotebookDraft(notebookId, metadata);
  const link = createNotebookLinkElement(notebookId, metadata);
  targetContainer.appendChild(link);
  updateFolderCounts();
  updateNotebookSectionCount();
  persistNotebookTree();
  applyNotebookMetadata();
  renderLocalNotebookWorkspace(notebookId);
  return notebookId;
}

function activeEditableNotebookId() {
  const notebookId = currentActiveNotebookId();
  if (!notebookId) {
    return null;
  }

  const metadata = notebookMetadata(notebookId);
  return metadata.canEdit && !metadata.deleted ? notebookId : null;
}

function requestCellRun(cellId) {
  if (!cellId) {
    return false;
  }

  window.requestAnimationFrame(() => {
    const cellRoot = document.querySelector(`[data-query-cell][data-cell-id="${cellId}"]`);
    const form = cellRoot?.querySelector("form.query-form-cell");
    if (!cellRoot || !form) {
      return;
    }

    setActiveCell(cellRoot);
    form.requestSubmit();
  });

  return true;
}

async function insertSourceQueryIntoCurrentNotebook(sourceObjectRoot, { runImmediately = false } = {}) {
  const sourceDescriptor = sourceQueryDescriptor(sourceObjectRoot);
  const notebookId = activeEditableNotebookId();
  if (!sourceDescriptor || !notebookId) {
    return false;
  }

  let fields;
  try {
    fields = await selectSourceObject(sourceObjectRoot);
  } catch (error) {
    console.error("Failed to load source object fields.", error);
    window.alert("The fields for this source object could not be loaded.");
    return null;
  }

  const metadata = notebookMetadata(notebookId);
  const nextCell = createSourceQueryCellState(sourceDescriptor, fields);
  activeCellId = nextCell.cellId;
  setNotebookCells(notebookId, [...metadata.cells, nextCell], { rerender: true });
  if (runImmediately) {
    requestCellRun(nextCell.cellId);
  }
  return true;
}

function querySourceInCurrentNotebook(sourceObjectRoot) {
  return insertSourceQueryIntoCurrentNotebook(sourceObjectRoot);
}

function viewSourceData(sourceObjectRoot) {
  return insertSourceQueryIntoCurrentNotebook(sourceObjectRoot, { runImmediately: true });
}

async function querySourceInNewNotebook(sourceObjectRoot) {
  const sourceDescriptor = sourceQueryDescriptor(sourceObjectRoot);
  if (!sourceDescriptor) {
    return null;
  }

  const targetContainer = defaultNotebookCreateTarget();
  if (!targetContainer) {
    return null;
  }

  let fields;
  try {
    fields = await selectSourceObject(sourceObjectRoot);
  } catch (error) {
    console.error("Failed to load source object fields.", error);
    window.alert("The fields for this source object could not be loaded.");
    return null;
  }

  const nextCell = createSourceQueryCellState(sourceDescriptor, fields);
  activeCellId = nextCell.cellId;
  return createNotebook(targetContainer, {
    cells: [nextCell],
  });
}

function updateWorkspaceCellEditor(cellRoot, sqlText) {
  const editorRoot = cellRoot?.querySelector("[data-editor-root]");
  const textarea = cellRoot?.querySelector("[data-editor-source]");
  if (!editorRoot || !textarea) {
    return;
  }

  textarea.dataset.defaultSql = sqlText;
  if (textarea.value !== sqlText) {
    textarea.value = sqlText;
  }

  const editor = editorRegistry.get(editorRoot);
  if (!editor) {
    return;
  }

  const currentValue = editor.state.doc.toString();
  if (currentValue === sqlText) {
    return;
  }

  applyingNotebookState = true;
  editor.dispatch({
    changes: {
      from: 0,
      to: currentValue.length,
      insert: sqlText,
    },
  });
  applyingNotebookState = false;
  autosizeEditor(editorRoot);
}

function formatCellSql(notebookId, cellId) {
  const cellRoot = document.querySelector(`[data-query-cell][data-cell-id="${cellId}"]`);
  const editorRoot = cellRoot?.querySelector("[data-editor-root]");
  const textarea = cellRoot?.querySelector("[data-editor-source]");
  const editor = editorRoot ? editorRegistry.get(editorRoot) : null;
  const currentSql = editor?.state.doc.toString() ?? textarea?.value ?? "";
  const formattedSql = formatSqlText(currentSql);

  if (!formattedSql || formattedSql === currentSql || !textarea) {
    return;
  }

  textarea.value = formattedSql;
  textarea.dataset.defaultSql = formattedSql;

  if (editor) {
    const nextCursor = Math.min(editor.state.selection.main.head, formattedSql.length);
    applyingNotebookState = true;
    editor.dispatch({
      changes: {
        from: 0,
        to: currentSql.length,
        insert: formattedSql,
      },
      selection: {
        anchor: nextCursor,
      },
    });
    applyingNotebookState = false;
    editor.focus();
  }

  setCellSql(notebookId, cellId, formattedSql);
}

function syncCellActionButtons(cellRoot, editable, index, totalCells) {
  syncWorkspaceActionButton(cellRoot?.querySelector("[data-format-cell-sql]"), {
    allowed: editable,
    enabledTitle: "Format SQL",
    disabledTitle: "This notebook cannot be edited.",
  });
  syncWorkspaceActionButton(cellRoot?.querySelector("[data-add-cell-after]"), {
    allowed: editable,
    enabledTitle: "Add cell below",
    disabledTitle: "This notebook cannot be edited.",
  });
  syncWorkspaceActionButton(cellRoot?.querySelector("[data-move-cell-up]"), {
    allowed: editable && index > 0,
    enabledTitle: "Move cell up",
    disabledTitle: editable ? "This cell is already first." : "This notebook cannot be edited.",
  });
  syncWorkspaceActionButton(cellRoot?.querySelector("[data-move-cell-down]"), {
    allowed: editable && index < totalCells - 1,
    enabledTitle: "Move cell down",
    disabledTitle: editable ? "This cell is already last." : "This notebook cannot be edited.",
  });
  syncWorkspaceActionButton(cellRoot?.querySelector("[data-copy-cell]"), {
    allowed: editable,
    enabledTitle: "Copy cell",
    disabledTitle: "This notebook cannot be edited.",
  });
  syncWorkspaceActionButton(cellRoot?.querySelector("[data-delete-cell]"), {
    allowed: editable,
    enabledTitle: "Delete cell",
    disabledTitle: "This notebook cannot be edited.",
  });
}

function closeCellActionMenus() {
  document.querySelectorAll("[data-cell-action-menu][open]").forEach((menu) => {
    menu.removeAttribute("open");
  });
}

function syncSourceActionMenu(menu) {
  const currentNotebookId = currentActiveNotebookId();
  const currentNotebook = currentNotebookId ? notebookMetadata(currentNotebookId) : null;
  const currentNotebookEditable = Boolean(currentNotebook?.canEdit && !currentNotebook?.deleted);
  syncWorkspaceActionButton(menu?.querySelector("[data-view-source-data]"), {
    allowed: currentNotebookEditable,
    enabledTitle: currentNotebook
      ? `Insert and run a query with all fields in "${currentNotebook.title}"`
      : "Insert and run a query with all fields in the current notebook",
    disabledTitle: currentNotebookId
      ? "The current notebook cannot be edited. Use 'Query in new notebook' instead."
      : "No notebook is currently selected.",
  });
  syncWorkspaceActionButton(menu?.querySelector("[data-query-source-current]"), {
    allowed: currentNotebookEditable,
    enabledTitle: currentNotebook
      ? `Insert a query into "${currentNotebook.title}"`
      : "Insert a query into the current notebook",
    disabledTitle: currentNotebookId
      ? "The current notebook cannot be edited. Use 'Query in new notebook' instead."
      : "No notebook is currently selected.",
  });
}

function closeSourceActionMenus() {
  document.querySelectorAll("[data-source-action-menu][open]").forEach((menu) => {
    menu.removeAttribute("open");
  });
}

function closeCellSourcePicker(cellRoot) {
  const picker = cellRoot?.querySelector("[data-cell-source-picker]");
  if (!picker) {
    return;
  }

  picker.open = false;
  picker.removeAttribute("open");
}

function setActiveCell(cellRoot = null) {
  activeCellId = cellRoot?.dataset.cellId ?? null;
  document.querySelectorAll("[data-query-cell].is-active").forEach((activeCell) => {
    if (activeCell !== cellRoot) {
      activeCell.classList.remove("is-active");
    }
  });

  cellRoot?.classList.add("is-active");
}

function applyWorkspaceCellState(workspaceRoot, cell, index, editable, totalCells) {
  const cellRoot = workspaceRoot?.querySelector(`[data-query-cell][data-cell-id="${cell.cellId}"]`);
  if (!cellRoot) {
    return;
  }

  cellRoot.dataset.defaultCellSources = normalizeDataSources(cell.dataSources).join("||");

  const label = cellRoot.querySelector(".cell-label");
  if (label) {
    label.textContent = `Cell ${index + 1}`;
  }

  const accessBadge = cellRoot.querySelector("[data-cell-access-badge]");
  if (accessBadge) {
    accessBadge.textContent = accessModeForDataSources(cell.dataSources);
    accessBadge.title = accessModeHintForDataSources(cell.dataSources);
  }

  const sourceSummary = cellRoot.querySelector("[data-cell-source-summary]");
  if (sourceSummary) {
    sourceSummary.innerHTML = cellSourceSummaryMarkup(cell.dataSources);
  }

  const selectedSources = new Set(normalizeDataSources(cell.dataSources));
  cellRoot.querySelectorAll("[data-cell-source-option]").forEach((optionInput) => {
    optionInput.disabled = !editable;
    optionInput.checked = selectedSources.has(optionInput.value);
    optionInput
      .closest(".workspace-source-option")
      ?.classList.toggle("is-selected", optionInput.checked);
  });

  if (!editable) {
    cellRoot.querySelector("[data-cell-source-picker]")?.removeAttribute("open");
  }

  syncCellActionButtons(cellRoot, editable, index, totalCells);
  updateWorkspaceCellEditor(cellRoot, cell.sql);
}

function workspaceCellIds(workspaceRoot) {
  return Array.from(workspaceRoot?.querySelectorAll("[data-query-cell]") ?? []).map(
    (cellRoot) => cellRoot.dataset.cellId
  );
}

function applyWorkspaceMetadata(metaRoot, metadata) {
  const workspaceRoot = metaRoot.closest("[data-workspace-notebook]");
  metaRoot.dataset.canEdit = metadata.canEdit ? "true" : "false";
  metaRoot.dataset.canDelete = metadata.canDelete ? "true" : "false";
  metaRoot.dataset.defaultCells = JSON.stringify(
    (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      dataSources: normalizeDataSources(cell.dataSources),
      sql: cell.sql,
    }))
  );

  const titleDisplay = workspaceRoot?.querySelector("[data-notebook-title-display]");
  if (titleDisplay) {
    titleDisplay.textContent = metadata.title;
  }

  const summaryDisplay = workspaceRoot?.querySelector("[data-summary-display]");
  if (summaryDisplay) {
    summaryDisplay.textContent = metadata.summary;
    summaryDisplay.classList.toggle("is-editable", metadata.canEdit);
  }

  const summaryInput = metaRoot.querySelector("[data-summary-input]");
  if (summaryInput) {
    summaryInput.disabled = !metadata.canEdit;
    setInputValue(summaryInput, metadata.summary);
  }

  const accessBadge = workspaceRoot?.querySelector("[data-access-badge]");
  if (accessBadge) {
    accessBadge.textContent = notebookAccessMode(metadata);
    accessBadge.title = notebookAccessModeHint(metadata);
  }

  const tagInput = metaRoot.querySelector("[data-tag-input]");
  if (tagInput) {
    tagInput.disabled = !metadata.canEdit;
  }

  const tagAddButton = metaRoot.querySelector("[data-tag-add]");
  if (tagAddButton) {
    tagAddButton.disabled = !metadata.canEdit;
  }

  const tagToggleButton = metaRoot.querySelector("[data-tag-toggle]");
  if (tagToggleButton) {
    tagToggleButton.disabled = !metadata.canEdit;
    tagToggleButton.classList.toggle("is-action-disabled", !metadata.canEdit);
    tagToggleButton.title = metadata.canEdit ? "Add tag" : "This notebook cannot be edited.";
  }

  if (!metadata.canEdit) {
    workspaceRoot?.querySelector("[data-summary-container]")?.classList.remove("is-editing");
    setTagControlsOpen(metaRoot, false);
  }

  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-rename-notebook]"), {
    allowed: metadata.canEdit,
    enabledTitle: "Rename notebook",
    disabledTitle: "This notebook cannot be renamed.",
  });
  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-edit-notebook]"), {
    allowed: metadata.canEdit,
    enabledTitle: "Edit notebook metadata",
    disabledTitle: "This notebook cannot be edited.",
  });
  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-delete-notebook]"), {
    allowed: metadata.canDelete,
    enabledTitle: "Delete notebook",
    disabledTitle: "This notebook cannot be deleted.",
  });
  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-copy-notebook]"), {
    allowed: true,
    enabledTitle: "Create a copy of this notebook",
    disabledTitle: "Create a copy of this notebook",
  });
  syncWorkspaceActionButton(metaRoot.querySelector("[data-save-version]"), {
    allowed: metadata.canEdit,
    enabledTitle: "Save the current notebook state as a version",
    disabledTitle: "This notebook cannot be versioned.",
  });

  renderWorkspaceTags(metaRoot, metadata.tags, metadata.canEdit);
  renderWorkspaceVersions(metaRoot, metadata.versions);

  const renderedCellIds = workspaceCellIds(workspaceRoot);
  const expectedCellIds = (metadata.cells ?? []).map((cell) => cell.cellId);
  const cellsMismatch =
    renderedCellIds.length !== expectedCellIds.length ||
    renderedCellIds.some((cellId, index) => cellId !== expectedCellIds[index]);

  if (cellsMismatch && isLocalNotebookId(metadata.notebookId ?? metaRoot.dataset.notebookId)) {
    renderLocalNotebookWorkspace(metaRoot.dataset.notebookId);
    return;
  }

  const totalCells = metadata.cells?.length ?? 0;
  (metadata.cells ?? []).forEach((cell, index) => {
    applyWorkspaceCellState(workspaceRoot, cell, index, metadata.canEdit, totalCells);
  });

  const addCellButton = workspaceRoot?.querySelector("[data-add-cell]");
  if (addCellButton) {
    addCellButton.disabled = !metadata.canEdit;
    addCellButton.hidden = !metadata.canEdit;
  }
}

function applyNotebookMetadata() {
  document.querySelectorAll("[data-draggable-notebook]").forEach((link) => {
    const notebookId = link.dataset.notebookId;
    if (!notebookId) {
      return;
    }

    const metadata = notebookMetadata(notebookId);
    link.hidden = metadata.deleted;
    link.dataset.canEdit = metadata.canEdit ? "true" : "false";
    link.dataset.canDelete = metadata.canDelete ? "true" : "false";
    updateSidebarNotebookLink(link, metadata);
  });

  updateFolderCounts();
  updateNotebookSectionCount();
  syncRootUnassignedFolder();

  document.querySelectorAll("[data-notebook-meta]").forEach((metaRoot) => {
    const notebookId = metaRoot.dataset.notebookId;
    if (!notebookId) {
      return;
    }

    applyWorkspaceMetadata(metaRoot, notebookMetadata(notebookId));
  });

  applySidebarSearchFilter();
}

async function renameNotebook(notebookId) {
  const metadata = notebookMetadata(notebookId);
  if (!metadata.canEdit) {
    return;
  }

  const nextTitle = await showFolderNameDialog({
    title: "Rename notebook",
    copy: "Enter a new title for this notebook.",
    submitLabel: "Rename",
    initialValue: metadata.title,
  });
  if (!nextTitle) {
    return;
  }

  setNotebookTitle(notebookId, nextTitle);
}

function nextNotebookCopyTitle(baseTitle) {
  const sourceTitle = String(baseTitle ?? "").trim() || "Untitled Notebook";
  const rootTitle = `${sourceTitle} Copy`;
  const existingTitles = new Set(
    visibleNotebookLinks().map((link) => (link.dataset.notebookTitle ?? "").trim().toLowerCase())
  );

  let candidate = rootTitle;
  let index = 2;
  while (existingTitles.has(candidate.toLowerCase())) {
    candidate = `${rootTitle} ${index}`;
    index += 1;
  }

  return candidate;
}

function notebookContainerForCopy(notebookId) {
  const sourceLink = notebookLinks(notebookId)[0];
  const sourceContainer = sourceLink?.parentElement;
  if (sourceContainer instanceof HTMLElement) {
    return sourceContainer;
  }

  const unassignedFolder = ensureRootUnassignedFolder();
  return directChildrenContainer(unassignedFolder);
}

function copyNotebook(notebookId) {
  const sourceMetadata = notebookMetadata(notebookId);
  const targetContainer = notebookContainerForCopy(notebookId);
  if (!targetContainer) {
    return null;
  }

  const duplicateId = `${localNotebookPrefix}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
  const duplicateMetadata = {
    title: nextNotebookCopyTitle(sourceMetadata.title),
    summary: sourceMetadata.summary,
    cells: sourceMetadata.cells.map((cell) =>
      createEmptyCellState({
        dataSources: [...normalizeDataSources(cell.dataSources)],
        sql: cell.sql,
      })
    ),
    tags: [...normalizeTags(sourceMetadata.tags)],
    canEdit: true,
    canDelete: true,
    deleted: false,
    versions: [],
  };
  duplicateMetadata.versions = [createInitialNotebookVersion(duplicateId, duplicateMetadata)];

  persistNotebookDraft(duplicateId, duplicateMetadata);
  const link = createNotebookLinkElement(duplicateId, duplicateMetadata);
  targetContainer.appendChild(link);
  updateFolderCounts();
  updateNotebookSectionCount();
  persistNotebookTree();
  applyNotebookMetadata();
  renderLocalNotebookWorkspace(duplicateId);
  return duplicateId;
}

function focusNotebookMetadata(notebookId) {
  const metaRoot = activeWorkspaceMetaRoot(notebookId);
  const workspaceRoot = metaRoot?.closest("[data-workspace-notebook]");
  const summaryInput = metaRoot?.querySelector("[data-summary-input]");
  if (summaryInput && !summaryInput.disabled) {
    setSummaryEditing(workspaceRoot, true);
    return;
  }

  const sourceOption = workspaceRoot?.querySelector("[data-cell-source-option]:not(:disabled)");
  if (!sourceOption) {
    return;
  }

  sourceOption.focus();
}

async function deleteNotebook(notebookId) {
  const metadata = notebookMetadata(notebookId);
  if (!metadata.canDelete) {
    return;
  }

  const { confirmed } = await showConfirmDialog({
    title: "Delete notebook",
    copy: `Delete "${metadata.title}" from this browser workspace?`,
    confirmLabel: "Delete notebook",
  });
  if (!confirmed) {
    return;
  }

  persistNotebookDraft(notebookId, { deleted: true });
  applyNotebookMetadata();

  const fallbackNotebookId = nextVisibleNotebookId(notebookId);
  if (!fallbackNotebookId) {
    renderEmptyWorkspace();
    writeLastNotebookId("");
    return;
  }

  try {
    await loadNotebookWorkspace(fallbackNotebookId);
  } catch (error) {
    if (error?.name === "AbortError") {
      return;
    }
    console.error("Failed to load the fallback notebook after deletion.", error);
  }
}

function directChildrenContainer(folder) {
  return folder?.querySelector(":scope > [data-tree-children]") ?? null;
}

function readStoredNotebookTree() {
  try {
    const rawValue = window.localStorage.getItem(notebookTreeStorageKey);
    if (!rawValue) {
      return null;
    }

    const parsed = JSON.parse(rawValue);
    return Array.isArray(parsed) ? parsed : null;
  } catch (_error) {
    return null;
  }
}

function writeStoredNotebookTree(state) {
  try {
    window.localStorage.setItem(notebookTreeStorageKey, JSON.stringify(state));
  } catch (_error) {
    // Ignore persistence failures and keep the in-memory tree functional.
  }
}

function updateFolderCounts(root = document) {
  root.querySelectorAll("[data-tree-folder]").forEach((folder) => {
    const countLabel = folder.querySelector(":scope > summary .tree-folder-count");
    const children = directChildrenContainer(folder);
    if (!countLabel || !children) {
      return;
    }

    const notebookCount = children.querySelectorAll("[data-draggable-notebook]:not([hidden])").length;
    countLabel.textContent = String(notebookCount);
  });
}

function updateNotebookSectionCount() {
  const count = document.querySelector("[data-notebook-section] .section-count");
  if (!count) {
    return;
  }

  count.textContent = String(visibleNotebookLinks().length);
}

function slugifyFolderSegment(name) {
  return String(name ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-");
}

function deriveFolderId(name, parentFolderId = "") {
  const slug = slugifyFolderSegment(name);
  if (!slug) {
    return parentFolderId || "";
  }

  return parentFolderId ? `${parentFolderId}-${slug}` : slug;
}

function defaultFolderPermissions(folderId = "") {
  if (folderId === "smoke-tests" || folderId.startsWith("smoke-tests-")) {
    return {
      canEdit: false,
      canDelete: false,
    };
  }

  return {
    canEdit: true,
    canDelete: true,
  };
}

function applyFolderActionState(button, { allowed, enabledTitle, disabledTitle }) {
  button.classList.toggle("is-action-disabled", !allowed);
  button.disabled = !allowed;
  button.title = allowed ? enabledTitle : disabledTitle;
}

function createFolderNode(
  name,
  { open = false, folderId = "", canEdit = true, canDelete = true } = {}
) {
  const folder = document.createElement("details");
  folder.className = "tree-folder";
  folder.dataset.treeFolder = "";
  folder.open = open;
  folder.dataset.folderId = folderId || "";
  folder.dataset.canEdit = String(canEdit);
  folder.dataset.canDelete = String(canDelete);

  const summary = document.createElement("summary");
  summary.className = "tree-folder-summary";
  summary.dataset.searchableItem = name;

  const label = document.createElement("span");
  label.className = "tree-folder-label";
  label.textContent = name;

  const tools = document.createElement("span");
  tools.className = "tree-folder-tools";

  const createNotebookButton = document.createElement("button");
  createNotebookButton.type = "button";
  createNotebookButton.className = "tree-add-button tree-add-button-inline";
  createNotebookButton.dataset.createNotebook = "";
  createNotebookButton.title = "Create notebook";
  createNotebookButton.setAttribute("aria-label", "Create notebook");
  createNotebookButton.innerHTML = `
    <svg class="tree-action-glyph" viewBox="0 0 16 16" aria-hidden="true">
      <path d="M4 2.2h5.2l2.8 2.8v8.8H4z"></path>
      <path d="M9.2 2.2v2.7h2.8"></path>
      <path d="M8 6.9v4.2M5.9 9h4.2"></path>
    </svg>
  `;

  const renameButton = document.createElement("button");
  renameButton.type = "button";
  renameButton.className = "tree-add-button tree-add-button-inline";
  renameButton.dataset.renameTreeFolder = "";
  renameButton.setAttribute("aria-label", "Rename folder");
  renameButton.textContent = "Edit";
  applyFolderActionState(renameButton, {
    allowed: canEdit,
    enabledTitle: "Rename folder",
    disabledTitle: "This folder cannot be renamed.",
  });

  const deleteButton = document.createElement("button");
  deleteButton.type = "button";
  deleteButton.className = "tree-add-button tree-add-button-inline tree-delete-button";
  deleteButton.dataset.deleteTreeFolder = "";
  deleteButton.setAttribute("aria-label", "Delete folder");
  deleteButton.textContent = "Delete";
  applyFolderActionState(deleteButton, {
    allowed: canDelete,
    enabledTitle: "Delete folder. Notebooks will be moved to the unassigned folder.",
    disabledTitle: "This folder cannot be deleted.",
  });

  const addButton = document.createElement("button");
  addButton.type = "button";
  addButton.className = "tree-add-button tree-add-button-inline";
  addButton.dataset.addTreeItem = "";
  addButton.title = "Add subfolder";
  addButton.setAttribute("aria-label", "Add subfolder");
  addButton.textContent = "+";

  const count = document.createElement("span");
  count.className = "tree-folder-count";
  count.textContent = "0";

  tools.append(createNotebookButton, renameButton, deleteButton, addButton, count);
  summary.append(label, tools);
  folder.append(summary);

  const children = document.createElement("div");
  children.className = "tree-children";
  children.dataset.treeChildren = "";
  folder.append(children);

  return folder;
}

function folderLabel(folder) {
  return folder?.querySelector(":scope > summary .tree-folder-label") ?? null;
}

function isUnassignedFolder(folder) {
  if (!(folder instanceof Element) || !folder.matches("[data-tree-folder]")) {
    return false;
  }

  return (
    folder.dataset.systemFolder === "unassigned" ||
    folderLabel(folder)?.textContent?.trim() === unassignedFolderName
  );
}

function folderCanEdit(folder) {
  if (!(folder instanceof Element)) {
    return false;
  }

  return folder.dataset.canEdit !== "false";
}

function folderCanDelete(folder) {
  if (!(folder instanceof Element)) {
    return false;
  }

  return folder.dataset.canDelete !== "false";
}

function notebookCountInFolder(folder) {
  if (!(folder instanceof Element)) {
    return 0;
  }

  return folder.querySelectorAll("[data-draggable-notebook]:not([hidden])").length;
}

function rootUnassignedFolder() {
  const root = notebookTreeRoot();
  if (!root) {
    return null;
  }

  return (
    Array.from(root.querySelectorAll(":scope > [data-tree-folder]")).find((folder) =>
      isUnassignedFolder(folder)
    ) ?? null
  );
}

function syncRootUnassignedFolder() {
  const root = notebookTreeRoot();
  const folder = rootUnassignedFolder();
  if (!root || !folder) {
    return null;
  }

  folder.dataset.systemFolder = "unassigned";
  if (notebookCountInFolder(folder) === 0) {
    folder.remove();
    return null;
  }

  root.appendChild(folder);
  return folder;
}

function ensureRootUnassignedFolder() {
  const root = notebookTreeRoot();
  if (!root) {
    return null;
  }

  const existing = rootUnassignedFolder();
  if (existing) {
    existing.dataset.systemFolder = "unassigned";
    existing.open = true;
    root.appendChild(existing);
    return existing;
  }

  const folder = createFolderNode(unassignedFolderName, { open: true });
  folder.dataset.systemFolder = "unassigned";
  root.appendChild(folder);
  return folder;
}

function collectFolderNotebooks(folder) {
  return Array.from(folder.querySelectorAll("[data-draggable-notebook]"));
}

function deleteStoredNotebookState(notebookId) {
  if (!notebookId) {
    return;
  }

  const state = readStoredNotebookMetadata();
  if (!(notebookId in state)) {
    return;
  }

  delete state[notebookId];
  writeStoredNotebookMetadata(state);
}

async function deleteTreeFolder(folder, { recursive = false } = {}) {
  const notebooks = collectFolderNotebooks(folder);
  if (isUnassignedFolder(folder) && notebooks.length > 0) {
    return;
  }

  const removedNotebookIds = notebooks
    .map((notebook) => notebook.dataset.notebookId)
    .filter(Boolean);
  const activeNotebookId = workspaceNotebookId(document);

  if (!recursive) {
    let targetContainer = null;
    if (notebooks.length > 0) {
      const targetFolder = ensureRootUnassignedFolder();
      targetContainer = directChildrenContainer(targetFolder);
      if (!targetContainer) {
        return;
      }
    }

    for (const notebook of notebooks) {
      targetContainer?.appendChild(notebook);
    }
  } else {
    for (const notebookId of removedNotebookIds) {
      if (isLocalNotebookId(notebookId)) {
        deleteStoredNotebookState(notebookId);
      } else {
        persistNotebookDraft(notebookId, { deleted: true });
      }
    }
  }

  folder.remove();
  syncRootUnassignedFolder();
  updateFolderCounts();
  persistNotebookTree();
  applyNotebookMetadata();

  if (!recursive || !activeNotebookId || !removedNotebookIds.includes(activeNotebookId)) {
    return;
  }

  const fallbackNotebookId = nextVisibleNotebookId(activeNotebookId);
  if (!fallbackNotebookId) {
    renderEmptyWorkspace();
    writeLastNotebookId("");
    return;
  }

  try {
    await loadNotebookWorkspace(fallbackNotebookId);
  } catch (error) {
    if (error?.name === "AbortError") {
      return;
    }
    console.error("Failed to load the fallback notebook after recursive folder deletion.", error);
  }
}

function serializeTreeNode(node) {
  if (!(node instanceof Element)) {
    return null;
  }

  if (node.matches("[data-tree-folder]")) {
    const name =
      node.querySelector(":scope > summary .tree-folder-label")?.textContent?.trim() || "Folder";
    const children = directChildrenContainer(node);
    return {
      type: "folder",
      folderId: node.dataset.folderId || null,
      name,
      open: node.open,
      systemFolder: node.dataset.systemFolder || null,
      canEdit: node.dataset.canEdit !== "false",
      canDelete: node.dataset.canDelete !== "false",
      children: children
        ? Array.from(children.children)
            .map((child) => serializeTreeNode(child))
            .filter(Boolean)
        : [],
    };
  }

  if (node.matches("[data-draggable-notebook]")) {
    return {
      type: "notebook",
      notebookId: node.dataset.notebookId,
    };
  }

  return null;
}

function persistNotebookTree() {
  const root = notebookTreeRoot();
  if (!root) {
    return;
  }

  syncRootUnassignedFolder();
  const state = Array.from(root.children)
    .map((child) => serializeTreeNode(child))
    .filter(Boolean);
  writeStoredNotebookTree(state);
}

function renderStoredTreeNode(nodeState, notebookLookup, parentFolderId = "") {
  if (!nodeState || typeof nodeState !== "object") {
    return null;
  }

  if (nodeState.type === "notebook") {
    const notebook = notebookLookup.get(nodeState.notebookId);
    if (!notebook) {
      if (isLocalNotebookId(nodeState.notebookId)) {
        const metadata = notebookMetadata(nodeState.notebookId);
        if (!metadata.deleted) {
          return createNotebookLinkElement(nodeState.notebookId, metadata);
        }
      }
      return null;
    }

    notebookLookup.delete(nodeState.notebookId);
    return notebook;
  }

  if (nodeState.type === "folder") {
    const resolvedFolderId = nodeState.folderId || deriveFolderId(nodeState.name || "Folder", parentFolderId);
    const fallbackPolicy = defaultFolderPermissions(resolvedFolderId);
    const folder = createFolderNode(nodeState.name || "Folder", {
      open: Boolean(nodeState.open),
      folderId: resolvedFolderId,
      canEdit: fallbackPolicy.canEdit
        ? typeof nodeState.canEdit === "boolean"
          ? nodeState.canEdit
          : true
        : false,
      canDelete: fallbackPolicy.canDelete
        ? typeof nodeState.canDelete === "boolean"
          ? nodeState.canDelete
          : true
        : false,
    });
    if (nodeState.systemFolder) {
      folder.dataset.systemFolder = nodeState.systemFolder;
    }
    const container = directChildrenContainer(folder);

    for (const child of nodeState.children ?? []) {
      const renderedChild = renderStoredTreeNode(child, notebookLookup, resolvedFolderId);
      if (renderedChild) {
        container.appendChild(renderedChild);
      }
    }

    return folder;
  }

  return null;
}

function resolveAddTarget(button) {
  const folder = button.closest("[data-tree-folder]");
  if (folder) {
    folder.open = true;
    return directChildrenContainer(folder);
  }

  return notebookTreeRoot();
}

function clearDropTargets() {
  document.querySelectorAll(".tree-children.is-drag-over").forEach((node) => {
    node.classList.remove("is-drag-over");
  });
  document.querySelectorAll(".tree-folder.is-drag-over").forEach((node) => {
    node.classList.remove("is-drag-over");
  });
}

function clearDragState() {
  clearDropTargets();
  if (draggedNotebook) {
    draggedNotebook.classList.remove("is-dragging");
  }
}

function resolveDropTarget(target) {
  if (!(target instanceof Element)) {
    return null;
  }

  const explicitContainer = target.closest("[data-tree-children]");
  if (explicitContainer) {
    return explicitContainer;
  }

  const folder = target.closest("[data-tree-folder]");
  if (folder) {
    return directChildrenContainer(folder);
  }

  return notebookTreeRoot();
}

function initializeNotebookTree(root = document) {
  const treeRoot =
    root instanceof Element && root.matches("[data-notebook-tree]") ? root : notebookTreeRoot();

  if (!treeRoot) {
    return;
  }

  const storedTree = readStoredNotebookTree();
  if (storedTree) {
    const notebookLookup = new Map(
      Array.from(treeRoot.querySelectorAll("[data-draggable-notebook]")).map((notebook) => [
        notebook.dataset.notebookId,
        notebook,
      ])
    );
    const fragment = document.createDocumentFragment();

    for (const nodeState of storedTree) {
      const renderedNode = renderStoredTreeNode(nodeState, notebookLookup);
      if (renderedNode) {
        fragment.appendChild(renderedNode);
      }
    }

    for (const notebook of notebookLookup.values()) {
      fragment.appendChild(notebook);
    }

    treeRoot.replaceChildren(fragment);
  } else {
    persistNotebookTree();
  }

  syncRootUnassignedFolder();
  updateFolderCounts(root);
  updateNotebookSectionCount();
}

function revealNotebookLink(notebookId) {
  const link = notebookLinks(notebookId)[0];
  if (!link) {
    return;
  }

  notebookSection()?.setAttribute("open", "");

  let parent = link.parentElement;
  while (parent) {
    const folder = parent.closest("[data-tree-folder]");
    if (!folder) {
      break;
    }
    folder.open = true;
    parent = folder.parentElement;
  }

  persistNotebookTree();
}

function applySidebarSearchFilter() {
  const search = document.querySelector("[data-sidebar-search]");
  const sidebar = document.getElementById("sidebar");
  if (!search || !sidebar) {
    return;
  }

  const term = search.value.trim().toLowerCase();
  const matches = (element) => {
    const haystack = (element?.dataset.searchableItem ?? "").toLowerCase();
    return !term || haystack.includes(term);
  };

  sidebar.querySelectorAll("[data-draggable-notebook]").forEach((link) => {
    link.dataset.searchHidden = matches(link) ? "false" : "true";
  });

  const notebookFolders = Array.from(sidebar.querySelectorAll("[data-tree-folder]")).reverse();
  for (const folder of notebookFolders) {
    const selfMatches = matches(folder.querySelector(":scope > summary"));
    const visibleChildren = folder.querySelector(
      ":scope > [data-tree-children] > :not([data-search-hidden='true'])"
    );
    const visible = !term || selfMatches || Boolean(visibleChildren);
    folder.dataset.searchHidden = visible ? "false" : "true";
    if (term && visibleChildren) {
      folder.open = true;
    }
  }

  sidebar.querySelectorAll(".source-object").forEach((item) => {
    item.dataset.searchHidden = matches(item) ? "false" : "true";
  });

  const sourceSchemas = Array.from(sidebar.querySelectorAll("[data-source-schema]")).reverse();
  for (const schema of sourceSchemas) {
    const selfMatches = matches(schema.querySelector(":scope > summary"));
    const visibleChildren = schema.querySelector(
      ":scope > .source-object-list > :not([data-search-hidden='true'])"
    );
    const visible = !term || selfMatches || Boolean(visibleChildren);
    schema.dataset.searchHidden = visible ? "false" : "true";
    if (term && visibleChildren) {
      schema.open = true;
    }
  }

  const sourceCatalogs = Array.from(sidebar.querySelectorAll("[data-source-catalog]")).reverse();
  for (const catalog of sourceCatalogs) {
    const selfMatches = matches(catalog.querySelector(":scope > summary"));
    const visibleChildren = catalog.querySelector(
      ":scope > :not(summary):not([data-search-hidden='true'])"
    );
    const visible = !term || selfMatches || Boolean(visibleChildren);
    catalog.dataset.searchHidden = visible ? "false" : "true";
    if (term && visibleChildren) {
      catalog.open = true;
    }
  }

  if (term && sidebar.querySelector("[data-draggable-notebook][data-search-hidden='false']")) {
    notebookSection()?.setAttribute("open", "");
  }

  if (term && sidebar.querySelector("[data-source-catalog][data-search-hidden='false']")) {
    dataSourcesSection()?.setAttribute("open", "");
  }
}

function initializeSidebarSearch() {
  const search = document.querySelector("[data-sidebar-search]");
  const sidebar = document.getElementById("sidebar");
  if (!search || !sidebar || search.dataset.bound === "true") {
    return;
  }

  search.dataset.bound = "true";
  search.addEventListener("input", () => applySidebarSearchFilter());
  applySidebarSearchFilter();
}

function processHtmx(root) {
  if (!root || typeof window.htmx?.process !== "function") {
    return;
  }

  window.htmx.process(root);
}

async function loadQueryJobsState() {
  const response = await window.fetch("/api/query-jobs", {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to load query jobs: ${response.status}`);
  }

  applyQueryJobsState(await response.json());
}

function ensureQueryJobsEventSource() {
  if (queryJobsEventSource || typeof window.EventSource !== "function") {
    return;
  }

  const eventSource = new window.EventSource("/api/query-jobs/stream");
  eventSource.addEventListener("jobs", (event) => {
    try {
      applyQueryJobsState(JSON.parse(event.data));
    } catch (error) {
      console.error("Failed to parse query job event.", error);
    }
  });
  eventSource.onerror = () => {
    // Keep the browser-managed reconnect loop, but refresh once to reduce stale UI if needed.
    if (queryJobsStateVersion !== null) {
      loadQueryJobsState().catch(() => {
        // Ignore transient reconnect issues.
      });
    }
  };
  queryJobsEventSource = eventSource;
}

async function openNotebookForQueryJob(notebookId, cellId = "") {
  if (!notebookId) {
    return;
  }

  await loadNotebookWorkspace(notebookId);
  renderQueryNotificationMenu();

  if (!cellId) {
    return;
  }

  const cellRoot = document.querySelector(`[data-query-cell][data-cell-id="${cellId}"]`);
  if (cellRoot) {
    setActiveCell(cellRoot);
    cellRoot.scrollIntoView({ block: "center", behavior: "smooth" });
  }
}

async function startQueryJobForForm(form) {
  const workspaceRoot = form.closest("[data-workspace-notebook]");
  const cellRoot = form.closest("[data-query-cell]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellId = cellRoot?.dataset.cellId;
  if (!workspaceRoot || !cellRoot || !notebookId || !cellId) {
    return;
  }

  const existingJob = queryJobForCell(notebookId, cellId);
  if (queryJobIsRunning(existingJob)) {
    return;
  }

  const formData = new FormData(form);
  const editorSource = cellRoot.querySelector("[data-editor-source]");
  formData.set("sql", editorSource?.value ?? "");
  formData.set("notebook_id", notebookId);
  formData.set("cell_id", cellId);
  formData.set("notebook_title", currentWorkspaceNotebookTitle(workspaceRoot));
  formData.set("data_sources", selectedDataSourcesForCell(cellRoot).join("||"));

  const response = await window.fetch("/api/query-jobs", {
    method: "POST",
    body: formData,
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let message = "The query could not be started.";
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }

    const resultRoot = cellRoot.querySelector("[data-cell-result]");
    if (resultRoot) {
      resultRoot.outerHTML = queryResultPanelMarkup(cellId, {
        jobId: `local-error-${cellId}`,
        notebookId,
        notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
        cellId,
        sql: editorSource?.value ?? "",
        status: "failed",
        durationMs: 0,
        updatedAt: new Date().toISOString(),
        rowsShown: 0,
        truncated: false,
        message: "Query failed.",
        error: message,
        columns: [],
        rows: [],
      });
    }
    return;
  }

  const snapshot = normalizeQueryJob(await response.json());
  if (!snapshot) {
    return;
  }

  const nextJobs = [snapshot, ...queryJobsSnapshot.filter((job) => job.jobId !== snapshot.jobId)];
  applyQueryJobsState({
    version: queryJobsStateVersion,
    summary: {
      ...queryJobsSummary,
      runningCount: queryJobsSummary.runningCount + 1,
    },
    jobs: nextJobs,
    performance: queryPerformanceState,
  });
}

async function cancelQueryJob(jobId) {
  if (!jobId) {
    return;
  }

  const response = await window.fetch(`/api/query-jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: "POST",
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    return;
  }

  const snapshot = normalizeQueryJob(await response.json());
  if (!snapshot) {
    return;
  }

  applyQueryJobsState({
    version: queryJobsStateVersion,
    summary: queryJobsSummary,
    jobs: [snapshot, ...queryJobsSnapshot.filter((job) => job.jobId !== snapshot.jobId)],
    performance: queryPerformanceState,
  });
}

function downloadTextFile(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = window.URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.URL.revokeObjectURL(url);
}

function downloadQueryResult(job, format) {
  if (!job?.columns?.length) {
    return;
  }

  const baseName = `${job.notebookTitle || "query"}-${job.cellId || "cell"}`
    .replace(/[^\w.-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase();

  if (format === "json") {
    const payload = job.rows.map((row) =>
      Object.fromEntries(job.columns.map((column, index) => [column, row[index] ?? null]))
    );
    downloadTextFile(`${baseName || "query-result"}.json`, JSON.stringify(payload, null, 2), "application/json");
    return;
  }

  const lines = [
    job.columns.join(","),
    ...job.rows.map((row) =>
      row
        .map((value) => {
          if (value === null || value === undefined) {
            return "";
          }
          const serialized = String(value);
          return /[",\n]/.test(serialized) ? `"${serialized.replace(/"/g, '""')}"` : serialized;
        })
        .join(",")
    ),
  ];
  downloadTextFile(`${baseName || "query-result"}.csv`, lines.join("\n"), "text/csv;charset=utf-8");
}

async function loadNotebookWorkspace(notebookId) {
  const panel = document.getElementById("workspace-panel");
  if (!panel || !notebookId) {
    return;
  }

  if (notebookMetadata(notebookId).deleted) {
    const fallbackNotebookId = nextVisibleNotebookId(notebookId);
    if (!fallbackNotebookId) {
      renderEmptyWorkspace();
      writeLastNotebookId("");
      return;
    }

    notebookId = fallbackNotebookId;
  }

  if (isLocalNotebookId(notebookId)) {
    renderLocalNotebookWorkspace(notebookId);
    return;
  }

  const controller = new AbortController();
  restoreController = controller;

  const response = await window.fetch(`/notebooks/${encodeURIComponent(notebookId)}`, {
    headers: { "HX-Request": "true" },
    signal: controller.signal,
  });
  if (controller.signal.aborted || restoreController !== controller) {
    return;
  }
  if (restoreController === controller) {
    restoreController = null;
  }
  if (!response.ok) {
    throw new Error(`Failed to load notebook ${notebookId}: ${response.status}`);
  }

  const workspaceMarkup = await response.text();
  if (controller.signal.aborted) {
    return;
  }

  panel.innerHTML = workspaceMarkup;
  processHtmx(panel);
  initializeEditors(panel);
  applyNotebookMetadata();
  activateNotebookLink(notebookId);
  revealNotebookLink(notebookId);
  writeLastNotebookId(notebookId);
  syncVisibleQueryCells();
  renderQueryNotificationMenu();
}

async function restoreLastNotebook() {
  const storedNotebookId = readLastNotebookId();
  const activeNotebookId = currentActiveNotebookId();
  const notebookId = [storedNotebookId, activeNotebookId]
    .filter(Boolean)
    .find((candidate) => !notebookMetadata(candidate).deleted);

  if (!notebookId) {
    const fallbackNotebookId = visibleNotebookLinks()[0]?.dataset.notebookId ?? null;
    if (!fallbackNotebookId) {
      return;
    }

    await loadNotebookWorkspace(fallbackNotebookId);
    return;
  }

  if (activeNotebookId === notebookId) {
    revealNotebookLink(notebookId);
    writeLastNotebookId(notebookId);
    return;
  }

  try {
    await loadNotebookWorkspace(notebookId);
  } catch (error) {
    if (error?.name === "AbortError") {
      return;
    }
    console.error("Failed to restore the last active notebook.", error);
    if (activeNotebookId) {
      revealNotebookLink(activeNotebookId);
      writeLastNotebookId(activeNotebookId);
    }
  }
}

document.body.addEventListener(
  "submit",
  async (event) => {
    const form = event.target.closest("[data-query-form]");
    if (!form) {
      return;
    }

    event.preventDefault();
    await startQueryJobForForm(form);
  },
  true
);

document.body.addEventListener("click", async (event) => {
  setActiveCell(event.target.closest("[data-query-cell]"));

  if (!event.target.closest("[data-workspace-action-menu]")) {
    closeWorkspaceActionMenus();
  }
  if (!event.target.closest("[data-cell-action-menu]")) {
    closeCellActionMenus();
  }
  if (!event.target.closest("[data-source-action-menu]")) {
    closeSourceActionMenus();
  }
  if (!event.target.closest("[data-query-notifications]")) {
    queryNotificationMenu()?.removeAttribute("open");
  }

  const sourceActionMenu = event.target.closest("[data-source-action-menu]");
  if (sourceActionMenu) {
    syncSourceActionMenu(sourceActionMenu);
  }

  const sidebarToggleButton = event.target.closest("[data-sidebar-toggle]");
  if (sidebarToggleButton) {
    event.preventDefault();
    const collapsed = !document.body.classList.contains("sidebar-collapsed");
    applySidebarCollapsedState(collapsed);
    writeSidebarCollapsed(collapsed);
    return;
  }

  const createNotebookButton = event.target.closest("[data-create-notebook]");
  if (createNotebookButton) {
    event.preventDefault();
    event.stopPropagation();

    const target = resolveNotebookCreateTarget(createNotebookButton);
    createNotebook(target);
    return;
  }

  const cancelQueryButton = event.target.closest("[data-cancel-query]");
  if (cancelQueryButton) {
    event.preventDefault();
    await cancelQueryJob(cancelQueryButton.dataset.jobId || "");
    return;
  }

  const cancelQueryJobButton = event.target.closest("[data-cancel-query-job]");
  if (cancelQueryJobButton) {
    event.preventDefault();
    await cancelQueryJob(cancelQueryJobButton.dataset.cancelQueryJob || "");
    return;
  }

  const openQueryNotebookButton = event.target.closest("[data-open-query-notebook]");
  if (openQueryNotebookButton) {
    event.preventDefault();
    queryNotificationMenu()?.removeAttribute("open");
    await openNotebookForQueryJob(
      openQueryNotebookButton.dataset.openQueryNotebook || "",
      openQueryNotebookButton.dataset.openQueryCell || ""
    );
    return;
  }

  const downloadJsonButton = event.target.closest("[data-result-download-json]");
  if (downloadJsonButton) {
    event.preventDefault();
    const cellRoot = downloadJsonButton.closest("[data-query-cell]");
    const notebookId = workspaceNotebookId(downloadJsonButton.closest("[data-workspace-notebook]"));
    const cellId = cellRoot?.dataset.cellId;
    downloadQueryResult(queryJobForCell(notebookId, cellId), "json");
    return;
  }

  const downloadCsvButton = event.target.closest("[data-result-download-csv]");
  if (downloadCsvButton) {
    event.preventDefault();
    const cellRoot = downloadCsvButton.closest("[data-query-cell]");
    const notebookId = workspaceNotebookId(downloadCsvButton.closest("[data-workspace-notebook]"));
    const cellId = cellRoot?.dataset.cellId;
    downloadQueryResult(queryJobForCell(notebookId, cellId), "csv");
    return;
  }

  const collapseTreeButton = event.target.closest("[data-collapse-tree]");
  if (collapseTreeButton) {
    event.preventDefault();
    event.stopPropagation();
    setNotebookTreeExpanded(false);
    return;
  }

  const expandTreeButton = event.target.closest("[data-expand-tree]");
  if (expandTreeButton) {
    event.preventDefault();
    event.stopPropagation();
    setNotebookTreeExpanded(true);
    return;
  }

  const collapseSourcesButton = event.target.closest("[data-collapse-sources]");
  if (collapseSourcesButton) {
    event.preventDefault();
    event.stopPropagation();
    setDataSourceTreeExpanded(false);
    return;
  }

  const expandSourcesButton = event.target.closest("[data-expand-sources]");
  if (expandSourcesButton) {
    event.preventDefault();
    event.stopPropagation();
    setDataSourceTreeExpanded(true);
    return;
  }

  const sourceObjectRoot = event.target.closest("[data-source-object]");
  if (sourceObjectRoot && !event.target.closest("[data-source-action-menu]")) {
    try {
      await selectSourceObject(sourceObjectRoot);
    } catch (error) {
      console.error("Failed to load source object fields.", error);
    }
    return;
  }

  const querySourceCurrentButton = event.target.closest("[data-query-source-current]");
  if (querySourceCurrentButton) {
    event.preventDefault();
    closeSourceActionMenus();

    const sourceObjectRoot = querySourceCurrentButton.closest("[data-source-object]");
    const inserted = await querySourceInCurrentNotebook(sourceObjectRoot);
    if (inserted === false) {
      window.alert("Open an editable notebook first, or use 'Query in new notebook'.");
    }
    return;
  }

  const viewSourceDataButton = event.target.closest("[data-view-source-data]");
  if (viewSourceDataButton) {
    event.preventDefault();
    closeSourceActionMenus();

    const sourceObjectRoot = viewSourceDataButton.closest("[data-source-object]");
    const viewed = await viewSourceData(sourceObjectRoot);
    if (viewed === false) {
      window.alert("Open an editable notebook first, or use 'Query in new notebook'.");
    }
    return;
  }

  const querySourceNewButton = event.target.closest("[data-query-source-new]");
  if (querySourceNewButton) {
    event.preventDefault();
    closeSourceActionMenus();
    await querySourceInNewNotebook(querySourceNewButton.closest("[data-source-object]"));
    return;
  }

  const tagToggleButton = event.target.closest("[data-tag-toggle]");
  if (tagToggleButton) {
    event.preventDefault();

    const metaRoot = tagToggleButton.closest("[data-notebook-meta]");
    if (!metaRoot || metaRoot.dataset.canEdit === "false") {
      return;
    }

    const controls = metaRoot.querySelector("[data-tag-controls]");
    setTagControlsOpen(metaRoot, controls?.hidden ?? true);
    return;
  }

  const tagAddButton = event.target.closest("[data-tag-add]");
  if (tagAddButton) {
    event.preventDefault();

    const metaRoot = tagAddButton.closest("[data-notebook-meta]");
    const input = metaRoot?.querySelector("[data-tag-input]");
    const notebookId = metaRoot?.dataset.notebookId;
    if (!input || !notebookId || metaRoot?.dataset.canEdit === "false") {
      return;
    }

    const nextTag = input.value.trim();
    if (!nextTag) {
      return;
    }

    setNotebookTags(notebookId, [...notebookMetadata(notebookId).tags, nextTag]);
    input.value = "";
    setTagControlsOpen(metaRoot, false);
    return;
  }

  const tagChip = event.target.closest("[data-tag-remove]");
  if (tagChip) {
    event.preventDefault();

    const metaRoot = tagChip.closest("[data-notebook-meta]");
    const notebookId = metaRoot?.dataset.notebookId;
    const tagValue = tagChip.dataset.tagRemove;
    if (!notebookId || !tagValue || metaRoot?.dataset.canEdit === "false") {
      return;
    }

    const remainingTags = notebookMetadata(notebookId).tags.filter((tag) => tag !== tagValue);
    setNotebookTags(notebookId, remainingTags);
    return;
  }

  const summaryDisplay = event.target.closest("[data-summary-display]");
  if (summaryDisplay) {
    const workspaceRoot = summaryDisplay.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    if (!notebookId || notebookMetadata(notebookId).canEdit === false) {
      return;
    }

    setSummaryEditing(workspaceRoot, true);
    return;
  }

  const renameNotebookTrigger = event.target.closest("[data-rename-notebook], [data-rename-notebook-title]");
  if (renameNotebookTrigger) {
    event.preventDefault();
    closeWorkspaceActionMenus();

    const notebookId = workspaceNotebookId(renameNotebookTrigger.closest("[data-workspace-notebook]"));
    if (!notebookId) {
      return;
    }

    await renameNotebook(notebookId);
    return;
  }

  const editNotebookButton = event.target.closest("[data-edit-notebook]");
  if (editNotebookButton) {
    event.preventDefault();
    closeWorkspaceActionMenus();

    const notebookId = workspaceNotebookId(editNotebookButton.closest("[data-workspace-notebook]"));
    if (!notebookId) {
      return;
    }

    focusNotebookMetadata(notebookId);
    return;
  }

  const copyNotebookButton = event.target.closest("[data-copy-notebook]");
  if (copyNotebookButton) {
    event.preventDefault();
    closeWorkspaceActionMenus();

    const notebookId = workspaceNotebookId(copyNotebookButton.closest("[data-workspace-notebook]"));
    if (!notebookId) {
      return;
    }

    copyNotebook(notebookId);
    return;
  }

  const addCellButton = event.target.closest("[data-add-cell]");
  if (addCellButton) {
    event.preventDefault();
    closeCellActionMenus();

    const notebookId = workspaceNotebookId(addCellButton.closest("[data-workspace-notebook]"));
    if (!notebookId) {
      return;
    }

    addCell(notebookId);
    return;
  }

  const addCellAfterButton = event.target.closest("[data-add-cell-after]");
  if (addCellAfterButton) {
    event.preventDefault();
    closeCellActionMenus();

    const workspaceRoot = addCellAfterButton.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = addCellAfterButton.closest("[data-query-cell]")?.dataset.cellId;
    if (!notebookId || !cellId) {
      return;
    }

    addCell(notebookId, cellId);
    return;
  }

  const formatCellSqlButton = event.target.closest("[data-format-cell-sql]");
  if (formatCellSqlButton) {
    event.preventDefault();
    closeCellActionMenus();

    const workspaceRoot = formatCellSqlButton.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = formatCellSqlButton.closest("[data-query-cell]")?.dataset.cellId;
    if (!notebookId || !cellId) {
      return;
    }

    formatCellSql(notebookId, cellId);
    return;
  }

  const copyCellButton = event.target.closest("[data-copy-cell]");
  if (copyCellButton) {
    event.preventDefault();
    closeCellActionMenus();

    const workspaceRoot = copyCellButton.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = copyCellButton.closest("[data-query-cell]")?.dataset.cellId;
    if (!notebookId || !cellId) {
      return;
    }

    duplicateCell(notebookId, cellId);
    return;
  }

  const moveCellUpButton = event.target.closest("[data-move-cell-up]");
  if (moveCellUpButton) {
    event.preventDefault();
    closeCellActionMenus();

    const workspaceRoot = moveCellUpButton.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = moveCellUpButton.closest("[data-query-cell]")?.dataset.cellId;
    if (!notebookId || !cellId) {
      return;
    }

    moveCell(notebookId, cellId, "up");
    return;
  }

  const moveCellDownButton = event.target.closest("[data-move-cell-down]");
  if (moveCellDownButton) {
    event.preventDefault();
    closeCellActionMenus();

    const workspaceRoot = moveCellDownButton.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = moveCellDownButton.closest("[data-query-cell]")?.dataset.cellId;
    if (!notebookId || !cellId) {
      return;
    }

    moveCell(notebookId, cellId, "down");
    return;
  }

  const deleteCellButton = event.target.closest("[data-delete-cell]");
  if (deleteCellButton) {
    event.preventDefault();
    closeCellActionMenus();

    const workspaceRoot = deleteCellButton.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = deleteCellButton.closest("[data-query-cell]")?.dataset.cellId;
    if (!notebookId || !cellId) {
      return;
    }

    deleteCell(notebookId, cellId);
    return;
  }

  const deleteNotebookButton = event.target.closest("[data-delete-notebook]");
  if (deleteNotebookButton) {
    event.preventDefault();
    closeWorkspaceActionMenus();

    const notebookId = workspaceNotebookId(deleteNotebookButton.closest("[data-workspace-notebook]"));
    if (!notebookId) {
      return;
    }

    await deleteNotebook(notebookId);
    return;
  }

  const saveVersionButton = event.target.closest("[data-save-version]");
  if (saveVersionButton) {
    event.preventDefault();

    const notebookId = workspaceNotebookId(saveVersionButton.closest("[data-workspace-notebook]"));
    if (!notebookId) {
      return;
    }

    saveNotebookVersion(notebookId);
    return;
  }

  const versionToggle = event.target.closest("[data-version-toggle]");
  if (versionToggle) {
    event.preventDefault();

    const metaRoot = versionToggle.closest("[data-notebook-meta]");
    const panel = metaRoot?.querySelector("[data-version-panel]");
    if (!metaRoot || !panel || versionToggle.disabled) {
      return;
    }

    setVersionPanelExpanded(metaRoot, panel.hidden);
    return;
  }

  const versionButton = event.target.closest("[data-version-load]");
  if (versionButton) {
    event.preventDefault();

    const notebookId = workspaceNotebookId(versionButton.closest("[data-workspace-notebook]"));
    const versionId = versionButton.dataset.versionId;
    if (!notebookId || !versionId) {
      return;
    }

    await loadNotebookVersion(notebookId, versionId);
    return;
  }

  const renameFolderButton = event.target.closest("[data-rename-tree-folder]");
  if (renameFolderButton) {
    event.preventDefault();
    event.stopPropagation();

    const folder = renameFolderButton.closest("[data-tree-folder]");
    const label = folderLabel(folder);
    if (!folder || !label || !folderCanEdit(folder)) {
      return;
    }

    const nextName = await showFolderNameDialog({
      title: "Rename folder",
      copy: "Update the folder name used in the notebook tree.",
      submitLabel: "Rename",
      initialValue: label.textContent?.trim() ?? "",
    });
    if (!nextName) {
      return;
    }

    label.textContent = nextName;
    const summary = folder.querySelector(":scope > summary");
    if (summary) {
      summary.dataset.searchableItem = nextName;
    }
    persistNotebookTree();
    applySidebarSearchFilter();
    return;
  }

  const deleteFolderButton = event.target.closest("[data-delete-tree-folder]");
  if (deleteFolderButton) {
    event.preventDefault();
    event.stopPropagation();

    const folder = deleteFolderButton.closest("[data-tree-folder]");
    const label = folderLabel(folder)?.textContent?.trim() ?? "this folder";
    if (!folder || !folderCanDelete(folder)) {
      return;
    }

    const { confirmed, optionChecked } = await showConfirmDialog({
      title: "Delete folder",
      copy: `Delete "${label}"? All notebooks in this folder will be moved to "${unassignedFolderName}" at the bottom of the notebook tree.`,
      confirmLabel: "Delete folder",
      option: {
        label: "Delete this folder recursively, including nested folders and notebooks.",
        checkedCopy: `Delete "${label}" recursively? All nested folders and notebooks in this subtree will be permanently removed from this browser workspace.`,
        checkedConfirmLabel: "Delete recursively",
      },
    });
    if (!confirmed) {
      return;
    }

    await deleteTreeFolder(folder, { recursive: optionChecked });
    return;
  }

  const renameSidebarNotebookButton = event.target.closest("[data-sidebar-rename-notebook]");
  if (renameSidebarNotebookButton) {
    event.preventDefault();
    event.stopPropagation();

    const notebookId = renameSidebarNotebookButton.closest(".notebook-link")?.dataset.notebookId;
    if (!notebookId) {
      return;
    }

    await renameNotebook(notebookId);
    return;
  }

  const editSidebarNotebookButton = event.target.closest("[data-sidebar-edit-notebook]");
  if (editSidebarNotebookButton) {
    event.preventDefault();
    event.stopPropagation();

    const notebookId = editSidebarNotebookButton.closest(".notebook-link")?.dataset.notebookId;
    if (!notebookId) {
      return;
    }

    await loadNotebookWorkspace(notebookId);
    focusNotebookMetadata(notebookId);
    return;
  }

  const deleteSidebarNotebookButton = event.target.closest("[data-sidebar-delete-notebook]");
  if (deleteSidebarNotebookButton) {
    event.preventDefault();
    event.stopPropagation();

    const notebookId = deleteSidebarNotebookButton.closest(".notebook-link")?.dataset.notebookId;
    if (!notebookId) {
      return;
    }

    await deleteNotebook(notebookId);
    return;
  }

  const addButton = event.target.closest("[data-add-tree-item]");
  if (addButton) {
    event.preventDefault();
    event.stopPropagation();

    const folderName = await showFolderNameDialog({
      title: "New folder",
      copy: "Enter a name for the new notebook folder.",
      submitLabel: "Create folder",
    });
    if (!folderName) {
      return;
    }

    const target = resolveAddTarget(addButton);
    if (!target) {
      return;
    }

    const parentFolder = addButton.closest("[data-tree-folder]");
    const nextFolderId = deriveFolderId(folderName, parentFolder?.dataset.folderId || "");
    const nextFolderPolicy = defaultFolderPermissions(nextFolderId);

    target.appendChild(
      createFolderNode(folderName, {
        open: true,
        folderId: nextFolderId,
        canEdit: nextFolderPolicy.canEdit,
        canDelete: nextFolderPolicy.canDelete,
      })
    );
    updateFolderCounts();
    persistNotebookTree();
    applySidebarSearchFilter();
    return;
  }

  const link = event.target.closest(".notebook-link");
  if (link) {
    if (!link.hasAttribute("hx-get")) {
      event.preventDefault();
      await loadNotebookWorkspace(link.dataset.notebookId);
      return;
    }

    restoreController?.abort();
    restoreController = null;
    activateNotebookLink(link.dataset.notebookId);
    revealNotebookLink(link.dataset.notebookId);
    writeLastNotebookId(link.dataset.notebookId);
    return;
  }

  const workspaceRoot = event.target.closest("[data-workspace-notebook]");
  if (!workspaceRoot) {
    return;
  }

  const notebookId = workspaceNotebookId(workspaceRoot);
  if (!notebookId) {
    return;
  }

  activateNotebookLink(notebookId);
  revealNotebookLink(notebookId);
  writeLastNotebookId(notebookId);
});

document.body.addEventListener("focusin", (event) => {
  setActiveCell(event.target.closest("[data-query-cell]"));
});

document.body.addEventListener("input", (event) => {
  const summaryInput = event.target.closest("[data-summary-input]");
  if (summaryInput) {
    const notebookId = workspaceNotebookId(summaryInput.closest("[data-workspace-notebook]"));
    if (!notebookId) {
      return;
    }

    setNotebookSummary(notebookId, summaryInput.value);
    return;
  }

  const editorSource = event.target.closest("[data-editor-source]");
  if (editorSource) {
    const workspaceRoot = editorSource.closest("[data-workspace-notebook]");
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = editorSource.closest("[data-query-cell]")?.dataset.cellId;
    autosizeEditor(editorSource.closest("[data-editor-root]"));
    if (!notebookId || !cellId) {
      return;
    }

    setCellSql(notebookId, cellId, editorSource.value);
  }
});

document.body.addEventListener("change", (event) => {
  const sourceOption = event.target.closest("[data-cell-source-option]");
  if (!sourceOption) {
    return;
  }

  const workspaceRoot = sourceOption.closest("[data-workspace-notebook]");
  const metaRoot = sourceOption.closest("[data-notebook-meta]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellRoot = sourceOption.closest("[data-query-cell]");
  const cellId = cellRoot?.dataset.cellId;
  if (!notebookId || !cellId || metaRoot?.dataset.canEdit === "false") {
    return;
  }

  const selectedSources = Array.from(cellRoot.querySelectorAll("[data-cell-source-option]:checked")).map(
    (option) => option.value
  );
  setCellDataSources(notebookId, cellId, selectedSources);
  closeCellSourcePicker(cellRoot);
});

document.body.addEventListener(
  "focusout",
  (event) => {
    const summaryInput = event.target.closest("[data-summary-input]");
    if (!summaryInput) {
      return;
    }

    const container = summaryInput.closest("[data-summary-container]");
    const nextFocused = event.relatedTarget;
    if (container && nextFocused instanceof Node && container.contains(nextFocused)) {
      return;
    }

    container?.classList.remove("is-editing");
  },
  true
);

document.body.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") {
    return;
  }

  const summaryInput = event.target.closest("[data-summary-input]");
  if (!summaryInput) {
    return;
  }

  const container = summaryInput.closest("[data-summary-container]");
  container?.classList.remove("is-editing");
  summaryInput.blur();
});

document.body.addEventListener("dragstart", (event) => {
  const notebook = event.target.closest("[data-draggable-notebook]");
  if (!notebook) {
    return;
  }

  draggedNotebook = notebook;
  draggedNotebook.classList.add("is-dragging");

  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", notebook.dataset.notebookId ?? "");
  }
});

document.body.addEventListener("dragover", (event) => {
  if (!draggedNotebook) {
    return;
  }

  const dropTarget = resolveDropTarget(event.target);
  if (!dropTarget) {
    return;
  }

  event.preventDefault();
  clearDropTargets();
  dropTarget.classList.add("is-drag-over");

  const folder = dropTarget.closest("[data-tree-folder]");
  if (folder) {
    folder.open = true;
    folder.classList.add("is-drag-over");
  }
});

document.body.addEventListener("drop", (event) => {
  if (!draggedNotebook) {
    return;
  }

  const dropTarget = resolveDropTarget(event.target);
  if (!dropTarget) {
    return;
  }

  event.preventDefault();
  dropTarget.appendChild(draggedNotebook);
  clearDragState();
  updateFolderCounts();
  syncRootUnassignedFolder();
  persistNotebookTree();
  draggedNotebook = null;
});

document.body.addEventListener("dragend", () => {
  clearDragState();
  draggedNotebook = null;
});

document.body.addEventListener(
  "toggle",
  (event) => {
    const folder = event.target;
    if (!(folder instanceof HTMLDetailsElement) || !folder.matches("[data-tree-folder]")) {
      return;
    }

    persistNotebookTree();
  },
  true
);

document.body.addEventListener("keydown", (event) => {
  if (event.key !== "Enter") {
    return;
  }

  const input = event.target.closest("[data-tag-input]");
  const metaRoot = input?.closest("[data-notebook-meta]");
  const notebookId = metaRoot?.dataset.notebookId;
  if (!input || !notebookId || metaRoot?.dataset.canEdit === "false") {
    return;
  }

  event.preventDefault();
  const nextTag = input.value.trim();
  if (!nextTag) {
    return;
  }

  setNotebookTags(notebookId, [...notebookMetadata(notebookId).tags, nextTag]);
  input.value = "";
  setTagControlsOpen(metaRoot, false);
});

document.body.addEventListener("keydown", async (event) => {
  if (event.key !== "Enter" && event.key !== " ") {
    return;
  }

  const titleTrigger = event.target.closest("[data-rename-notebook-title]");
  if (!titleTrigger) {
    return;
  }

  const notebookId = workspaceNotebookId(titleTrigger.closest("[data-workspace-notebook]"));
  if (!notebookId || notebookMetadata(notebookId).canEdit === false) {
    return;
  }

  event.preventDefault();
  await renameNotebook(notebookId);
});

document.body.addEventListener("htmx:afterSwap", (event) => {
  initializeEditors(event.target);
  initializeSidebarSearch();
  initializeNotebookTree();
  initializeSidebarToggle();
  applyNotebookMetadata();
  restoreSelectedSourceObject();
  renderQueryMonitor();
  syncVisibleQueryCells();
  renderQueryNotificationMenu();

  const notebookId =
    event.detail?.requestConfig?.parameters?.notebook_id ??
    event.detail?.requestConfig?.elt?.closest?.(".notebook-link")?.dataset?.notebookId ??
    workspaceNotebookId();

  if (notebookId) {
    activateNotebookLink(notebookId);
    revealNotebookLink(notebookId);
    writeLastNotebookId(notebookId);
  }
});

initializeEditors();
initializeSidebarSearch();
initializeNotebookTree();
initializeSidebarToggle();
applyNotebookMetadata();
restoreSelectedSourceObject();
ensureQueryJobsEventSource();
loadQueryJobsState()
  .catch((error) => {
    console.error("Failed to load query jobs.", error);
  })
  .finally(() => {
    restoreLastNotebook();
  });

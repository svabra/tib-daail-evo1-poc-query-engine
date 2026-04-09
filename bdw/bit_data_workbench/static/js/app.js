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
let realtimeEventsEventSource = null;
let queryJobsClockHandle = null;
let queryJobsLoaded = false;
let dataGeneratorsCatalog = [];
let dataGenerationJobsStateVersion = null;
let dataGenerationJobsSnapshot = [];
let dataGenerationJobsSummary = { runningCount: 0, totalCount: 0 };
let dataGenerationClockHandle = null;
let dataGenerationJobsLoaded = false;
let selectedIngestionRunbookId = "";
let spotlightIngestionRunbookId = "";
let ingestionRunbookSpotlightHandle = null;
let dataSourceEventsStateVersion = null;
let dataSourceEventsLatestEventId = null;
let notebookEventsStateVersion = null;
let notebookEventsLoaded = false;
const processedNotebookEventIds = new Set();
let pendingDataSourceSidebarRefreshHandle = null;
let dataSourceSidebarRefreshPromise = null;
let dataSourceSidebarRefreshQueued = false;
const pendingSourceCatalogBlinks = new Set();
const sourceConnectionRequests = new Set();
const refreshedDataGenerationJobIds = new Set();
let sidebarSourceOperationStatus = null;
let sidebarSourceOperationStatusClearHandle = null;
const sharedNotebookDrafts = new Map();
const sharedNotebookSyncHandles = new Map();
const s3ExplorerNodeRequests = new Map();
const resultExportDialogState = {
  jobId: "",
  exportFormat: "",
  selectedBucket: "",
  selectedPrefix: "",
  fileName: "",
  saving: false,
};
const localWorkspaceSaveDialogState = {
  jobId: "",
  exportFormat: "",
  fileName: "",
  folderPath: "",
  saving: false,
  createdFolderPaths: [],
};
const sidebarMinWidth = 360;
const sidebarMaxWidth = 720;
const sidebarResizeStep = 32;
const sidebarResizeState = {
  active: false,
  pointerId: null,
  startX: 0,
  startWidth: 0,
};

const notebookTreeStorageKey = "bdw.notebookTree.v2";
const notebookMetadataStorageKey = "bdw.notebookMeta.v1";
const notebookActivityStorageKey = "bdw.notebookActivity.v1";
const workbenchClientIdStorageKey = "bdw.clientId.v1";
const lastNotebookStorageKey = "bdw.lastNotebook.v1";
const sidebarCollapsedStorageKey = "bdw.sidebarCollapsed.v1";
const dismissedNotificationsStorageKey = "bdw.dismissedNotifications.v2";
const cacheResetStorageKey = "bdw.cacheReset.v1";
const localWorkspaceDatabaseName = "bdw.localWorkspace.v1";
const localWorkspaceDatabaseVersion = 1;
const localWorkspaceExportStoreName = "exports";
const localWorkspaceCatalogSourceId = "workspace.local";
const localWorkspaceSchemaKey = "workspace_local::saved-results";
const localWorkspaceRelationPrefix = "workspace.local.saved_results.";
const unassignedFolderName = "Unassigned";
const localNotebookPrefix = "local-notebook-";
const sharedNotebookPrefix = "shared-notebook-";
const localCellPrefix = "local-cell-";
const initialSqlEditorRows = 5;
const defaultSqlEditorAutoRows = 10;
const queryJobTerminalStatuses = new Set(["completed", "failed", "cancelled"]);
const queryJobRunningStatuses = new Set(["queued", "running"]);
const dataGenerationTerminalStatuses = new Set(["completed", "failed", "cancelled"]);
const dataGenerationRunningStatuses = new Set(["queued", "running"]);
let dismissedNotificationKeys = readDismissedNotificationKeys();
let localWorkspaceDatabasePromise = null;
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

function messageDialog() {
  return document.querySelector("[data-message-dialog]");
}

function aboutDialog() {
  return document.querySelector("[data-about-dialog]");
}

function featureListDialog() {
  return document.querySelector("[data-feature-list-dialog]");
}

function resultExportDialog() {
  return document.querySelector("[data-result-export-dialog]");
}

function localWorkspaceSaveDialog() {
  return document.querySelector("[data-local-workspace-save-dialog]");
}

function appendModalDialog(markup) {
  document.body.insertAdjacentHTML("beforeend", markup.trim());
}

function ensureConfirmDialog() {
  let dialog = confirmDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog" data-confirm-dialog>
      <form method="dialog" class="modal-card" data-confirm-form>
        <h2 class="modal-title" data-confirm-title>Confirm</h2>
        <p class="modal-copy" data-confirm-copy>Continue?</p>
        <label class="modal-toggle-option" data-confirm-option-container hidden>
          <input class="modal-toggle-checkbox" type="checkbox" data-confirm-option-input>
          <span class="modal-toggle-copy" data-confirm-option-label></span>
        </label>
        <menu class="modal-actions">
          <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
            Cancel
          </button>
          <button class="modal-button modal-button-danger" type="submit" value="confirm" data-confirm-submit>
            Delete
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = confirmDialog();
  return dialog;
}

function ensureMessageDialog() {
  let dialog = messageDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog" data-message-dialog>
      <form method="dialog" class="modal-card" data-message-form>
        <h2 class="modal-title" data-message-title>Notice</h2>
        <p class="modal-copy" data-message-copy>Done.</p>
        <menu class="modal-actions">
          <button class="modal-button" type="submit" value="confirm" data-message-submit>
            OK
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = messageDialog();
  return dialog;
}

function ensureAboutDialog() {
  let dialog = aboutDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-about-dialog>
      <form method="dialog" class="modal-card modal-card-wide" data-about-form>
        <div class="about-dialog-header">
          <div class="about-dialog-copy">
            <h2 class="modal-title">About</h2>
            <p class="modal-copy">
              This ProofOfConcept work is a collaborative effort of BIT and ESTV.
            </p>
          </div>
          <div class="about-dialog-version" data-about-version>Version unknown</div>
        </div>
        <div class="about-dialog-body">
          <p class="modal-copy">
            It's main purpose is to proof the speed of query execution.
          </p>
          <p class="modal-copy">
            The UI shall accelerate the testing experience for a wide range of roles:
            Data Analyst, Scientist, Data Owner, BIT Product Owner, Software and
            System Engineer, Decision Makers, etc.
            This way the client can evaluate and assess without any BIT employee beeing
            involed in tests.
          </p>
        </div>
        <menu class="modal-actions">
          <button class="modal-button" type="submit" value="confirm" data-about-submit>
            Close
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = aboutDialog();
  return dialog;
}

function ensureFeatureListDialog() {
  let dialog = featureListDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-feature-list-dialog>
      <form method="dialog" class="modal-card modal-card-wide" data-feature-list-form>
        <div class="about-dialog-header">
          <div class="about-dialog-copy">
            <h2 class="modal-title">Feature list</h2>
            <p class="modal-copy">
              Recent user-facing changes and important fixes by release.
            </p>
          </div>
        </div>
        <div class="feature-list-dialog-body" data-feature-list-body>
        </div>
        <menu class="modal-actions">
          <button class="modal-button" type="submit" value="confirm" data-feature-list-submit>
            Close
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = featureListDialog();
  return dialog;
}

function ensureResultExportDialog() {
  let dialog = resultExportDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-result-export-dialog>
      <form method="dialog" class="modal-card modal-card-wide result-export-dialog-card" data-result-export-form>
        <div class="result-export-dialog-header">
          <div class="result-export-dialog-copy">
            <h2 class="modal-title" data-result-export-title>Save Results to Shared Workspace</h2>
            <p class="modal-copy" data-result-export-copy>
              Choose a Shared Workspace bucket or folder, create new locations if needed, and provide a file name.
            </p>
          </div>
          <div class="result-export-dialog-toolbar">
            <button type="button" class="modal-button modal-button-secondary" data-s3-create-bucket>
              New bucket
            </button>
            <button type="button" class="modal-button modal-button-secondary" data-s3-create-folder>
              New folder
            </button>
          </div>
        </div>
        <div class="result-export-dialog-body">
          <section class="result-export-explorer-panel">
            <div class="result-export-explorer-header">
              <span
                class="workspace-tags-label"
                title="Shared Workspace data is stored in the configured MinIO / S3 bucket."
              >Shared Workspace Explorer</span>
              <div class="result-export-breadcrumbs" data-s3-explorer-breadcrumbs></div>
            </div>
            <div class="result-export-explorer-shell">
              <div class="result-export-explorer-tree" data-s3-explorer-tree></div>
            </div>
          </section>
          <aside class="result-export-target-panel">
            <div class="result-export-target-card">
              <span class="workspace-tags-label">Selected Shared Workspace Location</span>
              <p class="result-export-target-path" data-result-export-selected-path>
                Select a bucket or folder from the Shared Workspace explorer.
              </p>
            </div>
            <label class="result-export-field">
              <span class="result-export-field-label">File name</span>
              <input
                class="modal-input"
                type="text"
                data-result-export-file-name
                autocomplete="off"
                placeholder="query-result.parquet"
              >
            </label>
            <div class="result-export-target-card">
              <span class="workspace-tags-label">Export Format</span>
              <p class="result-export-target-path" data-result-export-format-copy>Format: Parquet</p>
            </div>
          </aside>
        </div>
        <menu class="modal-actions">
          <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
            Cancel
          </button>
          <button class="modal-button" type="submit" value="confirm" data-result-export-submit disabled>
            Save to Shared Workspace
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = resultExportDialog();
  return dialog;
}

function ensureLocalWorkspaceSaveDialog() {
  let dialog = localWorkspaceSaveDialog();
  if (dialog) {
    return dialog;
  }

  appendModalDialog(`
    <dialog class="modal-dialog modal-dialog-wide" data-local-workspace-save-dialog>
      <form method="dialog" class="modal-card modal-card-wide result-export-dialog-card" data-local-workspace-save-form>
        <div class="result-export-dialog-header">
          <div class="result-export-dialog-copy">
            <h2 class="modal-title" data-local-workspace-save-title>Save Results to Local Workspace</h2>
            <p class="modal-copy" data-local-workspace-save-copy>
              Choose a Local Workspace folder path and provide the file name to save in this browser.
            </p>
          </div>
          <div class="result-export-dialog-toolbar">
            <button type="button" class="modal-button modal-button-secondary" data-local-workspace-create-folder>
              New folder
            </button>
          </div>
        </div>
        <div class="result-export-dialog-body">
          <section class="result-export-explorer-panel">
            <div class="result-export-explorer-header">
              <span
                class="workspace-tags-label"
                title="Local Workspace data is stored in this browser profile using IndexedDB."
              >Local Workspace Folders</span>
              <div class="result-export-breadcrumbs" data-local-workspace-breadcrumbs></div>
            </div>
            <div class="result-export-explorer-shell">
              <div class="local-workspace-folder-list" data-local-workspace-folder-list></div>
            </div>
          </section>
          <aside class="result-export-target-panel">
            <div class="result-export-target-card">
              <span class="workspace-tags-label">Selected Local Workspace Location</span>
              <p class="result-export-target-path" data-local-workspace-selected-path>
                Local Workspace /
              </p>
            </div>
            <label class="result-export-field">
              <span class="result-export-field-label">Folder path</span>
              <input
                class="modal-input"
                type="text"
                data-local-workspace-folder-path
                autocomplete="off"
                placeholder="optional/subfolder"
              >
            </label>
            <label class="result-export-field">
              <span class="result-export-field-label">File name</span>
              <input
                class="modal-input"
                type="text"
                data-local-workspace-file-name
                autocomplete="off"
                placeholder="query-result.parquet"
              >
            </label>
            <div class="result-export-target-card">
              <span class="workspace-tags-label">Export Format</span>
              <p class="result-export-target-path" data-local-workspace-format-copy>Format: Parquet</p>
            </div>
          </aside>
        </div>
        <menu class="modal-actions">
          <button class="modal-button modal-button-secondary" type="button" data-modal-cancel>
            Cancel
          </button>
          <button class="modal-button" type="submit" value="confirm" data-local-workspace-save-submit disabled>
            Save to Local Workspace
          </button>
        </menu>
      </form>
    </dialog>
  `);

  dialog = localWorkspaceSaveDialog();
  return dialog;
}

function readFeatureReleaseNotes() {
  const element = document.getElementById("feature-release-notes");
  if (!element?.textContent) {
    return [];
  }

  try {
    const parsed = JSON.parse(element.textContent);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
}

function featureReleaseMarkup(release, currentVersion) {
  const version = String(release?.version || "").trim();
  const releasedAt = String(release?.releasedAt || "").trim();
  const features = Array.isArray(release?.features)
    ? release.features.map((feature) => String(feature).trim()).filter(Boolean)
    : [];
  const isCurrent = version && version === currentVersion;
  const featureItems = features.length
    ? features.map((feature) => `<li>${escapeHtml(feature)}</li>`).join("")
    : "<li>No release notes captured for this version.</li>";

  return `
    <section class="feature-release-entry">
      <header class="feature-release-header">
        <div class="feature-release-title-row">
          <h3 class="feature-release-version">Version ${escapeHtml(version || "unknown")}</h3>
          ${isCurrent ? '<span class="feature-release-current">Current</span>' : ""}
        </div>
        <p class="feature-release-time">${escapeHtml(formatVersionTimestamp(releasedAt))}</p>
      </header>
      <ul class="feature-release-items">
        ${featureItems}
      </ul>
    </section>
  `;
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

function ingestionRunbookSection() {
  return document.querySelector("[data-ingestion-runbook-section]");
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

function runbookFolders() {
  return Array.from(document.querySelectorAll("[data-runbook-folder]"));
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

function sidebarQueryCounts() {
  return Array.from(document.querySelectorAll("[data-sidebar-query-count]"));
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

function queryPerformanceDistribution() {
  return document.querySelector("[data-query-performance-distribution]");
}

function queryNotificationMenu() {
  return document.querySelector("[data-query-notifications]");
}

function settingsMenu() {
  return document.querySelector("[data-settings-menu]");
}

function queryNotificationList() {
  return document.querySelector("[data-query-notification-list]");
}

function queryNotificationCount() {
  return document.querySelector("[data-query-notification-count]");
}

function homePageRoot() {
  return document.querySelector("[data-home-page]");
}

function queryWorkbenchEntryPageRoot() {
  return document.querySelector("[data-query-workbench-entry-page]");
}

function queryWorkbenchDataSourcesPageRoot() {
  return document.querySelector("[data-data-source-management-page]");
}

function shellRoot() {
  return document.querySelector("[data-shell]");
}

function setShellSidebarHidden(hidden) {
  const shell = shellRoot();
  if (!shell) {
    return;
  }

  shell.classList.toggle("shell-sidebar-hidden", hidden);
  syncSidebarResizerAria();
}

function restoreSidebarVisibilityForWorkspace() {
  setShellSidebarHidden(false);
  applySidebarCollapsedState(readSidebarCollapsed());
}

function openNotebookNavigation(notebookId = "") {
  setShellSidebarHidden(false);
  applySidebarCollapsedState(false);
  writeSidebarCollapsed(false);
  notebookSection()?.setAttribute("open", "");
  if (notebookId) {
    revealNotebookLink(notebookId);
  }
}

function openIngestionNavigation(generatorId = "") {
  setShellSidebarHidden(false);
  applySidebarCollapsedState(false);
  writeSidebarCollapsed(false);
  ingestionRunbookSection()?.setAttribute("open", "");

  if (!generatorId) {
    return;
  }

  const activeRunbookLink = Array.from(document.querySelectorAll("[data-open-ingestion-runbook]"))
    .find((button) => (button.dataset.openIngestionRunbook || "") === generatorId);
  if (activeRunbookLink) {
    openRunbookAncestors(activeRunbookLink);
  }
}

function syncShellVisibility() {
  if (homePageRoot() || queryWorkbenchEntryPageRoot() || queryWorkbenchDataSourcesPageRoot()) {
    setShellSidebarHidden(true);
    return;
  }

  restoreSidebarVisibilityForWorkspace();
}

function homeRecentNotebooksRoot() {
  return document.querySelector("[data-home-recent-notebooks]");
}

function homeRecentIngestionsRoot() {
  return document.querySelector("[data-home-recent-ingestions]");
}

function notificationClearButton() {
  return document.querySelector("[data-clear-notifications]");
}

function ingestionGeneratorList() {
  return document.querySelector("[data-ingestion-generator-list]");
}

function ingestionJobList() {
  return document.querySelector("[data-ingestion-job-list]");
}

function ingestionGeneratorSectionTitle() {
  return document.querySelector("[data-ingestion-generator-section-title]");
}

function ingestionGeneratorSectionCopy() {
  return document.querySelector("[data-ingestion-generator-section-copy]");
}

function ingestionJobSectionTitle() {
  return document.querySelector("[data-ingestion-job-section-title]");
}

function ingestionJobSectionCopy() {
  return document.querySelector("[data-ingestion-job-section-copy]");
}

function dataGenerationMonitorList() {
  return document.querySelector("[data-generation-monitor-list]");
}

function dataGenerationMonitorCount() {
  return document.querySelector("[data-generation-monitor-count]");
}

function sidebarToggles() {
  return Array.from(document.querySelectorAll("[data-sidebar-toggle]"));
}

function currentActiveNotebookId() {
  return document.querySelector(".notebook-link.is-active")?.dataset.notebookId ?? null;
}

function workspaceNotebookId(root = document) {
  if (!root || typeof root.querySelector !== "function") {
    return null;
  }

  return (
    root.querySelector("input[name='notebook_id']")?.value ??
    root.querySelector("[data-notebook-meta]")?.dataset.notebookId ??
    null
  );
}

function currentSidebarMode() {
  return document.querySelector("[data-sidebar]")?.dataset.sidebarMode || "notebook";
}

function currentWorkspaceMode() {
  return document.querySelector("[data-ingestion-workbench-page]") ? "ingestion" : "notebook";
}

function currentWorkbenchSection() {
  if (homePageRoot()) {
    return "home";
  }

  if (queryWorkbenchDataSourcesPageRoot()) {
    return "data-sources";
  }

  return currentWorkspaceMode() === "ingestion" ? "ingestion" : "query";
}

function applicationVersion() {
  const explicitVersion =
    settingsMenu()?.dataset.runtimeVersion ||
    document.querySelector("[data-runtime-version]")?.dataset.runtimeVersion ||
    "";
  if (explicitVersion) {
    return explicitVersion.trim();
  }

  const sidebarVersion = document.querySelector(".runtime-pill-sidebar dd")?.textContent?.trim() || "";
  return sidebarVersion.replace(/^V/i, "").trim() || "unknown";
}

function workbenchTitle(section = currentWorkbenchSection()) {
  if (section === "home") {
    return "DAAIFL Workbench";
  }

  if (section === "data-sources") {
    return "DAAIFL Data Source Workbench";
  }

  return section === "ingestion"
    ? "DAAIFL Ingestion Workbench"
    : "DAAIFL Query Workbench";
}

function applyWorkbenchTitle(section = currentWorkbenchSection()) {
  const title = workbenchTitle(section);
  const brandTitle = document.querySelector(".brand-copy h1");
  if (brandTitle) {
    brandTitle.textContent = title;
  }
  if (typeof document !== "undefined") {
    document.title = title;
  }
}

function formatRelativeTimestamp(value) {
  const timestamp = Date.parse(value || "");
  if (!timestamp) {
    return "Just now";
  }

  const deltaMs = Date.now() - timestamp;
  if (deltaMs < 60_000) {
    return "Just now";
  }

  const deltaMinutes = Math.floor(deltaMs / 60_000);
  if (deltaMinutes < 60) {
    return `${deltaMinutes} min ago`;
  }

  const deltaHours = Math.floor(deltaMinutes / 60);
  if (deltaHours < 24) {
    return `${deltaHours} h ago`;
  }

  const deltaDays = Math.floor(deltaHours / 24);
  if (deltaDays < 7) {
    return `${deltaDays} d ago`;
  }

  return formatVersionTimestamp(value);
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

function isSharedNotebookId(notebookId) {
  return String(notebookId ?? "").startsWith(sharedNotebookPrefix);
}

function createCellId() {
  return `${localCellPrefix}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
}

function workbenchClientId() {
  try {
    let clientId = window.localStorage.getItem(workbenchClientIdStorageKey);
    if (clientId) {
      return clientId;
    }
    clientId = `client-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
    window.localStorage.setItem(workbenchClientIdStorageKey, clientId);
    return clientId;
  } catch (_error) {
    return `client-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
  }
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

function notebookUrl(notebookId) {
  if (!notebookId || isLocalNotebookId(notebookId)) {
    return null;
  }

  return `/notebooks/${encodeURIComponent(notebookId)}`;
}

function pushNotebookHistory(notebookId) {
  const nextUrl = notebookUrl(notebookId);
  if (!nextUrl || window.location.pathname === nextUrl) {
    return;
  }

  window.history.pushState({ mode: "notebook", notebookId }, "", nextUrl);
}

function pushQueryWorkbenchHistory() {
  if (window.location.pathname === "/query-workbench") {
    return;
  }

  window.history.pushState({ mode: "query-workbench" }, "", "/query-workbench");
}

function queryWorkbenchDataSourcesUrl(sourceId = "") {
  const normalizedSourceId = String(sourceId || "").trim();
  if (!normalizedSourceId) {
    return "/query-workbench/data-sources";
  }

  return `/query-workbench/data-sources?source_id=${encodeURIComponent(normalizedSourceId)}`;
}

function pushQueryWorkbenchDataSourcesHistory(sourceId = "") {
  const nextUrl = queryWorkbenchDataSourcesUrl(sourceId);
  if (`${window.location.pathname}${window.location.search}` === nextUrl) {
    return;
  }

  window.history.pushState(
    { mode: "query-workbench-data-sources", sourceId: String(sourceId || "").trim() },
    "",
    nextUrl
  );
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

function readNotebookActivity() {
  try {
    const rawValue = window.localStorage.getItem(notebookActivityStorageKey);
    if (!rawValue) {
      return {};
    }

    const parsed = JSON.parse(rawValue);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch (_error) {
    return {};
  }
}

function writeNotebookActivity(activity) {
  try {
    window.localStorage.setItem(notebookActivityStorageKey, JSON.stringify(activity));
  } catch (_error) {
    // Ignore persistence failures and keep the UI usable.
  }
}

function readDismissedNotificationKeys() {
  try {
    const rawValue = window.localStorage.getItem(dismissedNotificationsStorageKey);
    if (!rawValue) {
      return new Set();
    }

    const parsed = JSON.parse(rawValue);
    return Array.isArray(parsed) ? new Set(parsed.map((entry) => String(entry))) : new Set();
  } catch (_error) {
    return new Set();
  }
}

function writeDismissedNotificationKeys() {
  try {
    window.localStorage.setItem(
      dismissedNotificationsStorageKey,
      JSON.stringify(Array.from(dismissedNotificationKeys))
    );
  } catch (_error) {
    // Ignore persistence failures and keep the UI functional.
  }
}

function clearWorkbenchLocalCache() {
  const storageKeys = [];
  for (let index = 0; index < window.localStorage.length; index += 1) {
    const key = window.localStorage.key(index);
    if (key && key.startsWith("bdw.")) {
      storageKeys.push(key);
    }
  }

  for (const key of storageKeys) {
    window.localStorage.removeItem(key);
  }

  const resetMarker = {
    clearedAt: new Date().toISOString(),
    reason: "clear-local-workspace",
    version: applicationVersion(),
  };
  window.localStorage.setItem(cacheResetStorageKey, JSON.stringify(resetMarker));
  dismissedNotificationKeys = new Set();
  return resetMarker;
}

async function promptClearLocalWorkspace() {
  const { confirmed } = await showConfirmDialog({
    title: "Clear Local Workspace",
    copy:
      "This will permanently delete all browser-local Local Workspace data in this browser, including notebooks, drafts, saved versions, folder layout, last-opened notebook, and notification state.",
    confirmLabel: "Clear Local Workspace",
    option: {
      label:
        "I understand that this permanently deletes all browser-local Local Workspace data for this workbench.",
      checkedCopy:
        "All Local Workspace data stored in this browser will be deleted immediately, including your notebooks. The page will then reload with a clean local state.",
      checkedConfirmLabel: "Delete Local Workspace",
      required: true,
    },
  });
  if (!confirmed) {
    return;
  }

  try {
    await clearLocalWorkspaceExports();
    clearWorkbenchLocalCache();
  } catch (_error) {
    await showMessageDialog({
      title: "Clear Local Workspace failed",
      copy: "The browser-local Local Workspace data could not be cleared.",
    });
    return;
  }

  window.location.reload();
}

function applySidebarCollapsedState(collapsed) {
  document.body.classList.toggle("sidebar-collapsed", collapsed);

  sidebarToggles().forEach((toggle) => {
    const labelText = collapsed ? "Expand navigation" : "Collapse navigation";
    toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
    toggle.setAttribute("aria-label", labelText);
    toggle.title = labelText;

    const label = toggle.querySelector(".sidebar-toggle-label");
    if (label) {
      label.textContent = labelText;
    }
  });

  syncSidebarResizerAria();
}

function initializeSidebarToggle() {
  applySidebarCollapsedState(readSidebarCollapsed());
}

function sidebarRoot() {
  return document.querySelector("[data-sidebar]");
}

function sidebarResizer() {
  return document.querySelector("[data-sidebar-resizer]");
}

function clampSidebarWidth(width) {
  return Math.min(sidebarMaxWidth, Math.max(sidebarMinWidth, Number(width) || sidebarMinWidth));
}

function currentSidebarWidth() {
  return sidebarRoot()?.getBoundingClientRect().width ?? sidebarMinWidth;
}

function resolveSidebarWidthValue(width) {
  if (Number.isFinite(width)) {
    return clampSidebarWidth(width);
  }

  const numericWidth = Number(width);
  if (Number.isFinite(numericWidth)) {
    return clampSidebarWidth(numericWidth);
  }

  const inlineWidth = Number.parseFloat(
    document.documentElement.style.getPropertyValue("--sidebar-width") || ""
  );
  if (Number.isFinite(inlineWidth)) {
    return clampSidebarWidth(inlineWidth);
  }

  return clampSidebarWidth(currentSidebarWidth());
}

function syncSidebarResizerAria(width) {
  const resizer = sidebarResizer();
  if (!resizer) {
    return;
  }

  const nextWidth = Math.round(resolveSidebarWidthValue(width));
  resizer.setAttribute("aria-valuemin", String(sidebarMinWidth));
  resizer.setAttribute("aria-valuemax", String(sidebarMaxWidth));
  resizer.setAttribute("aria-valuenow", String(nextWidth));
  resizer.setAttribute("aria-valuetext", `${nextWidth} pixels`);
}

function applySidebarWidth(width) {
  const nextWidth = clampSidebarWidth(width);
  document.documentElement.style.setProperty("--sidebar-width", `${nextWidth}px`);
  syncSidebarResizerAria(nextWidth);
  return nextWidth;
}

function finishSidebarResize() {
  if (!sidebarResizeState.active) {
    return;
  }

  sidebarResizeState.active = false;
  sidebarResizeState.pointerId = null;
  document.body.classList.remove("sidebar-resizing");
  window.removeEventListener("pointermove", handleSidebarResizePointerMove);
  window.removeEventListener("pointerup", handleSidebarResizePointerUp);
  window.removeEventListener("pointercancel", handleSidebarResizePointerUp);
  window.requestAnimationFrame(() => syncSidebarResizerAria());
}

function handleSidebarResizePointerMove(event) {
  if (!sidebarResizeState.active) {
    return;
  }

  applySidebarWidth(sidebarResizeState.startWidth + (event.clientX - sidebarResizeState.startX));
}

function handleSidebarResizePointerUp() {
  finishSidebarResize();
}

function handleSidebarResizePointerDown(event) {
  if (
    event.button !== 0 ||
    document.body.classList.contains("sidebar-collapsed") ||
    window.matchMedia("(max-width: 1080px)").matches
  ) {
    return;
  }

  event.preventDefault();
  sidebarResizeState.active = true;
  sidebarResizeState.pointerId = event.pointerId;
  sidebarResizeState.startX = event.clientX;
  sidebarResizeState.startWidth = currentSidebarWidth();
  document.body.classList.add("sidebar-resizing");
  window.addEventListener("pointermove", handleSidebarResizePointerMove);
  window.addEventListener("pointerup", handleSidebarResizePointerUp);
  window.addEventListener("pointercancel", handleSidebarResizePointerUp);
}

function handleSidebarResizeKeyDown(event) {
  if (
    document.body.classList.contains("sidebar-collapsed") ||
    window.matchMedia("(max-width: 1080px)").matches
  ) {
    return;
  }

  if (event.key === "ArrowLeft") {
    event.preventDefault();
    applySidebarWidth(currentSidebarWidth() - sidebarResizeStep);
    return;
  }

  if (event.key === "ArrowRight") {
    event.preventDefault();
    applySidebarWidth(currentSidebarWidth() + sidebarResizeStep);
    return;
  }

  if (event.key === "Home") {
    event.preventDefault();
    applySidebarWidth(sidebarMinWidth);
    return;
  }

  if (event.key === "End") {
    event.preventDefault();
    applySidebarWidth(sidebarMaxWidth);
  }
}

function resetSidebarWidth() {
  document.documentElement.style.removeProperty("--sidebar-width");
  window.requestAnimationFrame(() => syncSidebarResizerAria());
}

function initializeSidebarResizer() {
  const resizer = sidebarResizer();
  if (!resizer) {
    return;
  }

  if (resizer.dataset.bound !== "true") {
    resizer.dataset.bound = "true";
    resizer.addEventListener("pointerdown", handleSidebarResizePointerDown);
    resizer.addEventListener("keydown", handleSidebarResizeKeyDown);
    resizer.addEventListener("dblclick", () => {
      resetSidebarWidth();
    });
    window.addEventListener("resize", () => syncSidebarResizerAria());
  }

  syncSidebarResizerAria();
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

function setRunbookTreeExpanded(expanded) {
  if (expanded) {
    ingestionRunbookSection()?.setAttribute("open", "");
  }

  runbookFolders().forEach((folder) => {
    folder.open = expanded;
  });

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

function closeSettingsMenus() {
  document.querySelectorAll("[data-settings-menu][open]").forEach((menu) => {
    menu.removeAttribute("open");
  });
}

function menuContainsPointer(menu, event, panelSelector = ":scope > .topbar-notification-panel") {
  if (!(menu instanceof Element) || typeof event?.clientX !== "number" || typeof event?.clientY !== "number") {
    return false;
  }

  const summary = menu.querySelector(":scope > summary");
  const panel = menu.querySelector(panelSelector);
  const rects = [summary, panel]
    .filter((node) => node instanceof Element)
    .map((node) => node.getBoundingClientRect())
    .filter((rect) => rect.width > 0 && rect.height > 0);

  if (!rects.length) {
    return false;
  }

  const left = Math.min(...rects.map((rect) => rect.left));
  const right = Math.max(...rects.map((rect) => rect.right));
  const top = Math.min(...rects.map((rect) => rect.top));
  const bottom = Math.max(...rects.map((rect) => rect.bottom));

  return (
    event.clientX >= left
    && event.clientX <= right
    && event.clientY >= top
    && event.clientY <= bottom
  );
}

function anyOpenMenuContainsPointer(selector, event, panelSelector = ":scope > .workspace-action-menu-panel") {
  if (typeof event?.clientX !== "number" || typeof event?.clientY !== "number") {
    return false;
  }

  return Array.from(document.querySelectorAll(`${selector}[open]`)).some((menu) => (
    menuContainsPointer(menu, event, panelSelector)
  ));
}

function closePopupMenusForTarget(target, event = null) {
  const activeTarget = target instanceof Element ? target : null;

  if (
    !activeTarget?.closest("[data-workspace-action-menu]")
    && !anyOpenMenuContainsPointer("[data-workspace-action-menu]", event)
  ) {
    closeWorkspaceActionMenus();
  }
  if (
    !activeTarget?.closest("[data-cell-action-menu]")
    && !anyOpenMenuContainsPointer("[data-cell-action-menu]", event)
  ) {
    closeCellActionMenus();
  }
  if (
    !activeTarget?.closest("[data-source-action-menu]")
    && !anyOpenMenuContainsPointer("[data-source-action-menu]", event)
  ) {
    closeSourceActionMenus();
  }
  if (
    !activeTarget?.closest("[data-result-action-menu]")
    && !anyOpenMenuContainsPointer("[data-result-action-menu]", event)
  ) {
    closeResultActionMenus();
  }
  if (
    !activeTarget?.closest("[data-s3-explorer-action-menu]")
    && !anyOpenMenuContainsPointer("[data-s3-explorer-action-menu]", event)
  ) {
    closeS3ExplorerActionMenus();
  }
  const notifications = queryNotificationMenu();
  if (!activeTarget?.closest("[data-query-notifications]") && !menuContainsPointer(notifications, event)) {
    queryNotificationMenu()?.removeAttribute("open");
  }
  const settings = settingsMenu();
  if (!activeTarget?.closest("[data-settings-menu]") && !menuContainsPointer(settings, event)) {
    closeSettingsMenus();
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

function showConfirmDialog({ title, copy, confirmLabel, option = null, confirmTone = "danger" }) {
  const dialog = ensureConfirmDialog();

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
  submit.classList.toggle("modal-button-danger", confirmTone === "danger");

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
        submit.disabled = false;
        return;
      }

      const optionChecked = optionInput.checked;
      copyNode.textContent = optionChecked ? option.checkedCopy ?? copy : copy;
      submit.textContent = optionChecked
        ? option.checkedConfirmLabel ?? confirmLabel
        : confirmLabel;
      submit.disabled = Boolean(option.required) && !optionChecked;
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

function showAboutDialog() {
  const dialog = ensureAboutDialog();
  const versionNode = dialog.querySelector("[data-about-version]");
  if (versionNode) {
    versionNode.textContent = `Version ${applicationVersion()}`;
  }

  return new Promise((resolve) => {
    const onClose = () => resolve();
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
  });
}

function showFeatureListDialog() {
  const dialog = ensureFeatureListDialog();
  const body = dialog.querySelector("[data-feature-list-body]");
  const currentVersion = applicationVersion();
  const releases = readFeatureReleaseNotes();

  if (body) {
    body.innerHTML = releases.length
      ? releases.map((release) => featureReleaseMarkup(release, currentVersion)).join("")
      : '<p class="modal-copy">No feature history is available yet.</p>';
  }

  return new Promise((resolve) => {
    const onClose = () => resolve();
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
  });
}

function showMessageDialog({ title, copy, actionLabel = "OK" }) {
  const dialog = ensureMessageDialog();
  const titleNode = dialog.querySelector("[data-message-title]");
  const copyNode = dialog.querySelector("[data-message-copy]");
  const form = dialog.querySelector("[data-message-form]");
  const submit = dialog.querySelector("[data-message-submit]");

  titleNode.textContent = title;
  copyNode.textContent = copy;
  submit.textContent = actionLabel;

  return new Promise((resolve) => {
    const onSubmit = (event) => {
      event.preventDefault();
      closeDialog(dialog, "confirm");
    };

    const onClose = () => {
      form.removeEventListener("submit", onSubmit);
      resolve();
    };

    form.addEventListener("submit", onSubmit);
    dialog.addEventListener("close", onClose, { once: true });
    dialog.showModal();
  });
}

function sourceOperationStatusRoot() {
  return document.querySelector("[data-source-operation-status]");
}

function clearSidebarSourceOperationStatusTimer() {
  if (sidebarSourceOperationStatusClearHandle !== null) {
    window.clearTimeout(sidebarSourceOperationStatusClearHandle);
    sidebarSourceOperationStatusClearHandle = null;
  }
}

function renderSidebarSourceOperationStatus() {
  const root = sourceOperationStatusRoot();
  if (!(root instanceof HTMLElement)) {
    return;
  }

  const titleNode = root.querySelector("[data-source-operation-status-title]");
  const copyNode = root.querySelector("[data-source-operation-status-copy]");
  const status = sidebarSourceOperationStatus;
  if (!status?.title || !status?.copy) {
    root.hidden = true;
    root.classList.remove("is-success", "is-danger");
    if (titleNode) {
      titleNode.textContent = "";
    }
    if (copyNode) {
      copyNode.textContent = "";
    }
    return;
  }

  root.hidden = false;
  root.classList.toggle("is-success", status.tone === "success");
  root.classList.toggle("is-danger", status.tone === "danger");
  if (titleNode) {
    titleNode.textContent = status.title;
  }
  if (copyNode) {
    copyNode.textContent = status.copy;
  }
}

function setSidebarSourceOperationStatus(status, { autoClearMs = 0 } = {}) {
  clearSidebarSourceOperationStatusTimer();
  if (!status || !status.title || !status.copy) {
    sidebarSourceOperationStatus = null;
    renderSidebarSourceOperationStatus();
    return;
  }

  sidebarSourceOperationStatus = {
    tone: status.tone === "success" || status.tone === "danger" ? status.tone : "info",
    title: String(status.title || "").trim(),
    copy: String(status.copy || "").trim(),
  };
  const sourcesRoot = dataSourcesSection();
  if (sourcesRoot instanceof HTMLDetailsElement) {
    sourcesRoot.open = true;
  }
  renderSidebarSourceOperationStatus();

  if (autoClearMs > 0) {
    sidebarSourceOperationStatusClearHandle = window.setTimeout(() => {
      sidebarSourceOperationStatus = null;
      sidebarSourceOperationStatusClearHandle = null;
      renderSidebarSourceOperationStatus();
    }, autoClearMs);
  }
}

async function responseErrorMessage(response, fallback = "The request failed.") {
  try {
    const payload = await response.json();
    return typeof payload?.detail === "string" && payload.detail.trim()
      ? payload.detail.trim()
      : fallback;
  } catch (_error) {
    return fallback;
  }
}

async function fetchJsonOrThrow(url, options = {}) {
  const response = await window.fetch(url, options);
  if (!response.ok) {
    throw new Error(await responseErrorMessage(response, `Request failed: ${response.status}`));
  }
  return response.json();
}

function ensureLocalWorkspaceDatabaseSupport() {
  if (typeof window.indexedDB === "undefined") {
    throw new Error("IndexedDB is not available in this browser, so Local Workspace storage cannot be used.");
  }
}

function openLocalWorkspaceDatabase() {
  ensureLocalWorkspaceDatabaseSupport();
  if (localWorkspaceDatabasePromise) {
    return localWorkspaceDatabasePromise;
  }

  localWorkspaceDatabasePromise = new Promise((resolve, reject) => {
    const request = window.indexedDB.open(
      localWorkspaceDatabaseName,
      localWorkspaceDatabaseVersion
    );

    request.onupgradeneeded = () => {
      const database = request.result;
      if (!database.objectStoreNames.contains(localWorkspaceExportStoreName)) {
        database.createObjectStore(localWorkspaceExportStoreName, {
          keyPath: "id",
        });
      }
    };

    request.onsuccess = () => {
      const database = request.result;
      database.onversionchange = () => {
        database.close();
      };
      resolve(database);
    };

    request.onerror = () => {
      reject(request.error || new Error("Could not open the Local Workspace database."));
    };

    request.onblocked = () => {
      reject(new Error("The Local Workspace database is blocked by another tab or session."));
    };
  });

  return localWorkspaceDatabasePromise;
}

async function clearLocalWorkspaceExports() {
  const database = await openLocalWorkspaceDatabase();
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(localWorkspaceExportStoreName, "readwrite");
    const store = transaction.objectStore(localWorkspaceExportStoreName);
    const request = store.clear();

    request.onsuccess = () => {
      resolve();
    };

    request.onerror = () => {
      reject(request.error || new Error("Could not clear Local Workspace files."));
    };
  });
}

function normalizeLocalWorkspaceFolderPath(path) {
  return String(path || "")
    .split("/")
    .map((segment) => String(segment || "").trim())
    .filter(Boolean)
    .join("/");
}

function localWorkspaceFolderPaths(paths = []) {
  const knownPaths = new Set([""]);

  paths.forEach((path) => {
    const normalizedPath = normalizeLocalWorkspaceFolderPath(path);
    if (!normalizedPath) {
      return;
    }

    let currentPath = "";
    normalizedPath.split("/").forEach((segment) => {
      currentPath = currentPath ? `${currentPath}/${segment}` : segment;
      knownPaths.add(currentPath);
    });
  });

  return Array.from(knownPaths).sort((left, right) => {
    if (!left && right) {
      return -1;
    }
    if (left && !right) {
      return 1;
    }

    return left.localeCompare(right, undefined, { sensitivity: "base" });
  });
}

function localWorkspaceDisplayPath(folderPath = "", fileName = "") {
  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
  const folderSuffix = normalizedFolderPath ? `${normalizedFolderPath}/` : "";
  const normalizedFileName = String(fileName || "").trim();
  return `Local Workspace / ${folderSuffix}${normalizedFileName}`.trim();
}

function localWorkspaceFolderName(folderPath = "") {
  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
  if (!normalizedFolderPath) {
    return "Root";
  }

  return normalizedFolderPath.split("/").at(-1) || normalizedFolderPath;
}

function localWorkspaceFolderDepth(folderPath = "") {
  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
  return normalizedFolderPath ? normalizedFolderPath.split("/").length : 0;
}

function localWorkspaceRelation(entryId) {
  return `${localWorkspaceRelationPrefix}${String(entryId || "").trim()}`;
}

function normalizeLocalWorkspaceExportEntry(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }

  const id = String(entry.id || "").trim();
  if (!id) {
    return null;
  }

  return {
    id,
    fileName: String(entry.fileName || "").trim() || "local-workspace-file",
    folderPath: normalizeLocalWorkspaceFolderPath(entry.folderPath),
    exportFormat: String(entry.exportFormat || "").trim().toLowerCase() || "json",
    mimeType: String(entry.mimeType || "").trim(),
    sizeBytes: Number.isFinite(Number(entry.sizeBytes)) ? Number(entry.sizeBytes) : 0,
    createdAt: String(entry.createdAt || "").trim(),
    updatedAt: String(entry.updatedAt || entry.createdAt || "").trim(),
    notebookTitle: String(entry.notebookTitle || "").trim(),
    cellId: String(entry.cellId || "").trim(),
    columnCount: Number.isFinite(Number(entry.columnCount)) ? Number(entry.columnCount) : 0,
    rowCount: Number.isFinite(Number(entry.rowCount)) ? Number(entry.rowCount) : 0,
    blob: entry.blob instanceof Blob ? entry.blob : null,
  };
}

async function listLocalWorkspaceExports() {
  const database = await openLocalWorkspaceDatabase();
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(localWorkspaceExportStoreName, "readonly");
    const store = transaction.objectStore(localWorkspaceExportStoreName);
    const request = store.getAll();

    request.onsuccess = () => {
      const entries = Array.isArray(request.result)
        ? request.result.map((entry) => normalizeLocalWorkspaceExportEntry(entry)).filter(Boolean)
        : [];
      entries.sort((left, right) => {
        return String(right.updatedAt || right.createdAt || "").localeCompare(
          String(left.updatedAt || left.createdAt || "")
        );
      });
      resolve(entries);
    };

    request.onerror = () => {
      reject(request.error || new Error("Could not read Local Workspace files."));
    };
  });
}

async function getLocalWorkspaceExport(entryId) {
  const normalizedEntryId = String(entryId || "").trim();
  if (!normalizedEntryId) {
    return null;
  }

  const database = await openLocalWorkspaceDatabase();
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(localWorkspaceExportStoreName, "readonly");
    const store = transaction.objectStore(localWorkspaceExportStoreName);
    const request = store.get(normalizedEntryId);

    request.onsuccess = () => {
      resolve(normalizeLocalWorkspaceExportEntry(request.result));
    };

    request.onerror = () => {
      reject(request.error || new Error("Could not load the Local Workspace file."));
    };
  });
}

async function saveLocalWorkspaceExport(entry) {
  const normalizedEntry = normalizeLocalWorkspaceExportEntry(entry);
  if (!normalizedEntry || !(normalizedEntry.blob instanceof Blob)) {
    throw new Error("The Local Workspace file is incomplete and could not be saved.");
  }

  const database = await openLocalWorkspaceDatabase();
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(localWorkspaceExportStoreName, "readwrite");
    const store = transaction.objectStore(localWorkspaceExportStoreName);
    const request = store.put(normalizedEntry);

    request.onsuccess = () => {
      resolve(normalizedEntry);
    };

    request.onerror = () => {
      reject(request.error || new Error("Could not save the Local Workspace file."));
    };
  });
}

async function deleteLocalWorkspaceExport(entryId) {
  const normalizedEntryId = String(entryId || "").trim();
  if (!normalizedEntryId) {
    return;
  }

  const database = await openLocalWorkspaceDatabase();
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(localWorkspaceExportStoreName, "readwrite");
    const store = transaction.objectStore(localWorkspaceExportStoreName);
    const request = store.delete(normalizedEntryId);

    request.onsuccess = () => {
      resolve();
    };

    request.onerror = () => {
      reject(request.error || new Error("Could not delete the Local Workspace file."));
    };
  });
}

function localWorkspaceSaveFolderListRoot() {
  return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-folder-list]") ?? null;
}

function localWorkspaceSaveBreadcrumbRoot() {
  return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-breadcrumbs]") ?? null;
}

function localWorkspaceSaveSelectedPathNode() {
  return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-selected-path]") ?? null;
}

function localWorkspaceSaveFolderPathInput() {
  return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-folder-path]") ?? null;
}

function localWorkspaceSaveFileNameInput() {
  return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-file-name]") ?? null;
}

function localWorkspaceSaveSubmitButton() {
  return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-save-submit]") ?? null;
}

function localWorkspaceFolderOptionMarkup(folderPath) {
  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
  const depth = localWorkspaceFolderDepth(normalizedFolderPath);
  const selected = normalizedFolderPath === localWorkspaceSaveDialogState.folderPath;
  const locationCopy = localWorkspaceDisplayPath(normalizedFolderPath);

  return `
    <button
      type="button"
      class="local-workspace-folder-option${selected ? " is-selected" : ""}"
      data-local-workspace-folder-option
      data-local-workspace-folder-path="${escapeHtml(normalizedFolderPath)}"
      style="--local-workspace-folder-depth: ${escapeHtml(String(depth))}"
      title="${escapeHtml(locationCopy)}"
    >
      <span class="local-workspace-folder-option-name">${escapeHtml(localWorkspaceFolderName(normalizedFolderPath))}</span>
      <span class="local-workspace-folder-option-path">${escapeHtml(locationCopy)}</span>
    </button>
  `;
}

function localWorkspaceFolderListMarkup(folderPaths) {
  if (!folderPaths.length) {
    return '<p class="local-workspace-folder-empty">No Local Workspace folders exist yet. Save into Root or create a new folder.</p>';
  }

  return folderPaths.map((folderPath) => localWorkspaceFolderOptionMarkup(folderPath)).join("");
}

function renderLocalWorkspaceSaveBreadcrumbs(folderPath = "") {
  const root = localWorkspaceSaveBreadcrumbRoot();
  if (!(root instanceof Element)) {
    return;
  }

  const segments = normalizeLocalWorkspaceFolderPath(folderPath)
    .split("/")
    .filter(Boolean);
  const crumbs = [
    { label: "Local Workspace", path: "" },
    ...segments.map((segment, index) => ({
      label: segment,
      path: segments.slice(0, index + 1).join("/"),
    })),
  ];

  root.innerHTML = crumbs
    .map((crumb, index) => {
      const current = index === crumbs.length - 1;
      return `
        <button
          type="button"
          class="result-export-breadcrumb${current ? " is-current" : ""}"
          data-local-workspace-breadcrumb
          data-local-workspace-folder-path="${escapeHtml(crumb.path)}"
          ${current ? "aria-current=\"true\"" : ""}
        >${escapeHtml(crumb.label)}</button>
        ${current ? "" : '<span class="result-export-breadcrumb-separator" aria-hidden="true">/</span>'}
      `;
    })
    .join("");
}

function syncLocalWorkspaceSaveDialogState() {
  const dialog = localWorkspaceSaveDialog();
  if (!dialog) {
    return;
  }

  localWorkspaceSaveDialogState.folderPath = normalizeLocalWorkspaceFolderPath(
    localWorkspaceSaveDialogState.folderPath
  );
  renderLocalWorkspaceSaveBreadcrumbs(localWorkspaceSaveDialogState.folderPath);

  const selectedPathNode = localWorkspaceSaveSelectedPathNode();
  if (selectedPathNode) {
    selectedPathNode.textContent = localWorkspaceDisplayPath(localWorkspaceSaveDialogState.folderPath);
  }

  const folderPathInput = localWorkspaceSaveFolderPathInput();
  if (folderPathInput && folderPathInput.value !== localWorkspaceSaveDialogState.folderPath) {
    folderPathInput.value = localWorkspaceSaveDialogState.folderPath;
  }

  const fileNameInput = localWorkspaceSaveFileNameInput();
  if (fileNameInput && fileNameInput.value !== localWorkspaceSaveDialogState.fileName) {
    fileNameInput.value = localWorkspaceSaveDialogState.fileName;
  }

  const formatCopy = dialog.querySelector("[data-local-workspace-format-copy]");
  if (formatCopy) {
    formatCopy.textContent = `Format: ${String(localWorkspaceSaveDialogState.exportFormat || "").toUpperCase()}`;
  }

  const submitButton = localWorkspaceSaveSubmitButton();
  if (submitButton) {
    submitButton.disabled =
      localWorkspaceSaveDialogState.saving ||
      !String(localWorkspaceSaveDialogState.fileName || "").trim();
    submitButton.textContent = localWorkspaceSaveDialogState.saving
      ? "Saving..."
      : "Save to Local Workspace";
  }

  dialog.querySelectorAll("[data-local-workspace-folder-option]").forEach((node) => {
    const selected =
      normalizeLocalWorkspaceFolderPath(node.dataset.localWorkspaceFolderPath || "") ===
      localWorkspaceSaveDialogState.folderPath;
    node.classList.toggle("is-selected", selected);
  });
}

function setLocalWorkspaceSaveDialogBusy(busy) {
  localWorkspaceSaveDialogState.saving = busy;
  const dialog = localWorkspaceSaveDialog();
  if (dialog) {
    const createFolderButton = dialog.querySelector("[data-local-workspace-create-folder]");
    if (createFolderButton instanceof HTMLButtonElement) {
      createFolderButton.disabled = busy;
    }

    const folderPathInput = localWorkspaceSaveFolderPathInput();
    if (folderPathInput instanceof HTMLInputElement) {
      folderPathInput.disabled = busy;
    }

    const fileNameInput = localWorkspaceSaveFileNameInput();
    if (fileNameInput instanceof HTMLInputElement) {
      fileNameInput.disabled = busy;
    }
  }

  syncLocalWorkspaceSaveDialogState();
}

async function renderLocalWorkspaceSaveFolderList() {
  const root = localWorkspaceSaveFolderListRoot();
  if (!(root instanceof Element)) {
    return;
  }

  const entries = await listLocalWorkspaceExports();
  const folderPaths = localWorkspaceFolderPaths([
    ...entries.map((entry) => entry.folderPath),
    ...localWorkspaceSaveDialogState.createdFolderPaths,
  ]);
  root.innerHTML = localWorkspaceFolderListMarkup(folderPaths);
  syncLocalWorkspaceSaveDialogState();
}

async function createLocalWorkspaceFolderFromDialog() {
  const parentPath = localWorkspaceSaveDialogState.folderPath;
  const folderName = await showFolderNameDialog({
    title: "New Local Workspace folder",
    copy: `Create a folder under ${localWorkspaceDisplayPath(parentPath)}.`,
    submitLabel: "Create folder",
  });
  if (!folderName) {
    return;
  }

  const nextPath = normalizeLocalWorkspaceFolderPath(
    parentPath ? `${parentPath}/${folderName}` : folderName
  );
  if (!nextPath) {
    return;
  }

  if (!localWorkspaceSaveDialogState.createdFolderPaths.includes(nextPath)) {
    localWorkspaceSaveDialogState.createdFolderPaths.push(nextPath);
  }
  localWorkspaceSaveDialogState.folderPath = nextPath;
  await renderLocalWorkspaceSaveFolderList();
}

async function openLocalWorkspaceSaveDialog(job, exportFormat) {
  if (!job?.jobId || !job?.columns?.length) {
    return;
  }

  const dialog = ensureLocalWorkspaceSaveDialog();
  localWorkspaceSaveDialogState.jobId = job.jobId;
  localWorkspaceSaveDialogState.exportFormat = String(exportFormat || "").trim().toLowerCase();
  localWorkspaceSaveDialogState.fileName = defaultQueryResultExportFilename(
    job,
    localWorkspaceSaveDialogState.exportFormat
  );
  localWorkspaceSaveDialogState.folderPath = "";
  localWorkspaceSaveDialogState.saving = false;
  localWorkspaceSaveDialogState.createdFolderPaths = [];

  const titleNode = dialog.querySelector("[data-local-workspace-save-title]");
  const copyNode = dialog.querySelector("[data-local-workspace-save-copy]");
  if (titleNode) {
    titleNode.textContent = `Save Results in ${localWorkspaceSaveDialogState.exportFormat.toUpperCase()} Format to Local Workspace`;
  }
  if (copyNode) {
    copyNode.textContent =
      "Choose a Local Workspace folder path or create a new one, then provide the file name to save in this browser.";
  }

  syncLocalWorkspaceSaveDialogState();
  dialog.showModal();
  await renderLocalWorkspaceSaveFolderList();
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

function parseBooleanDatasetValue(value, fallback = false) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  return String(value).trim().toLowerCase() === "true";
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

function sourceStorageTooltipForId(sourceId) {
  return sourceOptionForId(sourceId)?.storage_tooltip ?? "";
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

function sourceStorageTooltipForIds(sourceIds) {
  const selectedSourceIds = normalizeDataSources(sourceIds);
  if (!selectedSourceIds.length) {
    return "";
  }

  if (selectedSourceIds.length === 1) {
    return sourceStorageTooltipForId(selectedSourceIds[0]);
  }

  return "Selected sources span multiple storage locations.";
}

function sourceComputationModeTooltipText() {
  return [
    "MPP = Massive Parallel Processing. Distributed query execution across multiple workers and partitions for larger-scale data processing.",
    "VMTP = Vectorized Multi-Threaded Processing. Single-node vectorized execution across multiple CPU threads for fast local analytical queries.",
    "PostgreSQL Native = Direct execution by the PostgreSQL planner and executor, without DuckDB in the query path.",
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

function normalizeNotebookTitleValue(value, fallback = "Untitled Notebook") {
  const title = typeof value === "string" ? value.trim() : "";
  if (title) {
    return title;
  }

  const fallbackTitle = typeof fallback === "string" ? fallback.trim() : "";
  return fallbackTitle || "Untitled Notebook";
}

function normalizeNotebookSummaryValue(value, fallback = "Describe this notebook.") {
  const summary = typeof value === "string" ? value.trim() : "";
  if (summary) {
    return summary;
  }

  const fallbackSummary = typeof fallback === "string" ? fallback.trim() : "";
  return fallbackSummary || "Describe this notebook.";
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
  const linkVersions = parseVersionsPayload(link?.dataset.defaultNotebookVersions);
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
    createdAt:
      metaRoot?.dataset.defaultCreatedAt ??
      metaRoot?.dataset.createdAt ??
      link?.dataset.createdAt ??
      new Date().toISOString(),
    linkedGeneratorId: metaRoot?.dataset.linkedGeneratorId ?? "",
    cells: normalizeNotebookCells(metaCells.length ? metaCells : linkCells, {
      dataSources: fallbackDataSources,
      sql: legacySql,
    }),
    tags: metaTags.length ? metaTags : linkTags.length ? linkTags : domTags,
    canEdit: (metaRoot?.dataset.canEdit ?? link?.dataset.canEdit ?? "true") !== "false",
    canDelete: (metaRoot?.dataset.canDelete ?? link?.dataset.canDelete ?? "true") !== "false",
    shared: parseBooleanDatasetValue(
      metaRoot?.dataset.defaultShared ?? metaRoot?.dataset.shared ?? link?.dataset.defaultNotebookShared ?? link?.dataset.shared,
      false
    ),
    deleted: false,
    versions: metaVersions.length ? metaVersions : linkVersions,
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
    title: normalizeNotebookTitleValue(metadata.title),
    summary: normalizeNotebookSummaryValue(metadata.summary),
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
    shared:
      storedState.shared === true
        ? true
        : storedState.shared === false
          ? false
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

  const firstRowMs = Number(job.firstRowMs);
  const fetchMs = Number(job.fetchMs);

  return {
    ...job,
    columns: Array.isArray(job.columns) ? job.columns : [],
    rows: Array.isArray(job.rows) ? job.rows : [],
    dataSources: Array.isArray(job.dataSources) ? job.dataSources : [],
    sourceTypes: Array.isArray(job.sourceTypes) ? job.sourceTypes : [],
    touchedRelations: Array.isArray(job.touchedRelations)
      ? job.touchedRelations.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [],
    touchedBuckets: Array.isArray(job.touchedBuckets)
      ? job.touchedBuckets.map((value) => String(value ?? "").trim()).filter(Boolean)
      : [],
    firstRowMs: Number.isFinite(firstRowMs) ? Math.max(0, firstRowMs) : null,
    fetchMs: Number.isFinite(fetchMs) ? Math.max(0, fetchMs) : null,
  };
}

function normalizeDataGenerator(generator) {
  if (!generator || typeof generator !== "object") {
    return null;
  }

  const title = String(generator.title ?? "").trim();
  const generatorId = String(generator.generatorId ?? "").trim();
  if (!title || !generatorId) {
    return null;
  }

  return {
    ...generator,
    generatorId,
    title,
    description: String(generator.description ?? "").trim(),
    targetKind: String(generator.targetKind ?? "").trim() || "unknown",
    moduleName: String(generator.moduleName ?? "").trim(),
    treePath: Array.isArray(generator.treePath)
      ? generator.treePath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
      : [],
    defaultTargetName: String(generator.defaultTargetName ?? "").trim(),
    defaultSizeGb: Number.isFinite(Number(generator.defaultSizeGb)) ? Number(generator.defaultSizeGb) : 1,
    minSizeGb: Number.isFinite(Number(generator.minSizeGb)) ? Number(generator.minSizeGb) : 0.01,
    maxSizeGb: Number.isFinite(Number(generator.maxSizeGb)) ? Number(generator.maxSizeGb) : 512,
    supportsCleanup: Boolean(generator.supportsCleanup),
    tags: Array.isArray(generator.tags) ? generator.tags : [],
  };
}

function normalizeDataGenerationJob(job) {
  if (!job || typeof job !== "object") {
    return null;
  }

  return {
    ...job,
    generatorId: String(job.generatorId ?? "").trim(),
    title: String(job.title ?? "").trim() || "Data generation",
    description: String(job.description ?? "").trim(),
    targetKind: String(job.targetKind ?? "").trim() || "unknown",
    targetName: String(job.targetName ?? "").trim(),
    targetRelation: String(job.targetRelation ?? "").trim(),
    targetPath: String(job.targetPath ?? "").trim(),
    canCleanup: Boolean(job.canCleanup),
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

function queryJobById(jobId) {
  const normalizedJobId = String(jobId || "").trim();
  if (!normalizedJobId) {
    return null;
  }

  return queryJobsSnapshot.find((job) => job.jobId === normalizedJobId) ?? null;
}

function queryJobForResultActionTarget(target) {
  if (!(target instanceof Element)) {
    return null;
  }

  const resultRoot = target.closest("[data-cell-result]");
  const jobId =
    target.dataset.resultJobId ||
    resultRoot?.dataset.queryJobId ||
    resultRoot?.querySelector("[data-query-duration]")?.dataset.jobId ||
    "";
  const directJob = queryJobById(jobId);
  if (directJob) {
    return directJob;
  }

  const cellId = target.closest("[data-query-cell]")?.dataset.cellId || "";
  const notebookId = workspaceNotebookId(target.closest("[data-workspace-notebook]"));
  return queryJobForCell(notebookId, cellId);
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

function formatRelativePercent(percentValue) {
  const absoluteValue = Math.abs(Number.isFinite(Number(percentValue)) ? Number(percentValue) : 0);
  if (absoluteValue >= 10) {
    return `${Math.round(absoluteValue)}%`;
  }
  if (absoluteValue >= 1) {
    return `${absoluteValue.toFixed(1)}%`;
  }
  return `${absoluteValue.toFixed(2)}%`;
}

function compareQueryJobsByCompletedAt(left, right) {
  const leftCompletedAt = Date.parse(left?.completedAt || left?.updatedAt || left?.startedAt || "");
  const rightCompletedAt = Date.parse(right?.completedAt || right?.updatedAt || right?.startedAt || "");

  if (!Number.isNaN(leftCompletedAt) || !Number.isNaN(rightCompletedAt)) {
    const normalizedLeft = Number.isNaN(leftCompletedAt) ? 0 : leftCompletedAt;
    const normalizedRight = Number.isNaN(rightCompletedAt) ? 0 : rightCompletedAt;
    if (normalizedLeft !== normalizedRight) {
      return normalizedLeft - normalizedRight;
    }
  }

  return String(left?.jobId || "").localeCompare(String(right?.jobId || ""));
}

function medianDuration(values) {
  const normalizedValues = (values ?? [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right);
  if (!normalizedValues.length) {
    return null;
  }

  const midpoint = Math.floor(normalizedValues.length / 2);
  if (normalizedValues.length % 2 === 1) {
    return normalizedValues[midpoint];
  }
  return (normalizedValues[midpoint - 1] + normalizedValues[midpoint]) / 2;
}

function queryJobComparisonKey(job) {
  const sourceKey = normalizeDataSources(job?.dataSources ?? [])
    .slice()
    .sort()
    .join("||");
  return [
    String(job?.notebookId || "").trim(),
    String(job?.cellId || "").trim(),
    String(job?.backendName || "").trim(),
    sourceKey,
  ].join("::");
}

function queryComparisonTone(deltaMs) {
  const normalizedDelta = Number.isFinite(Number(deltaMs)) ? Number(deltaMs) : 0;
  if (normalizedDelta <= -0.5) {
    return "faster";
  }
  if (normalizedDelta >= 0.5) {
    return "slower";
  }
  return "neutral";
}

function buildQueryComparisonInsight(label, baselineMs, deltaMs, tooltip) {
  if (!Number.isFinite(Number(baselineMs)) || Number(baselineMs) <= 0 || !Number.isFinite(Number(deltaMs))) {
    return null;
  }

  const tone = queryComparisonTone(deltaMs);
  const normalizedDelta = Number(deltaMs);
  const percentDelta = (normalizedDelta / Number(baselineMs)) * 100;
  const value =
    tone === "neutral"
      ? "no material change"
      : `${formatRelativePercent(percentDelta)} ${tone}`;

  return {
    label,
    value,
    tone,
    title: tooltip,
  };
}

function buildQueryJobComparisonMetrics(job, history) {
  const durationMs = Number(job?.durationMs);
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return null;
  }

  const previousJob = history.length ? history[history.length - 1] : null;
  const previous =
    previousJob && Number.isFinite(Number(previousJob.durationMs))
      ? buildQueryComparisonInsight(
          "vs previous",
          Number(previousJob.durationMs),
          durationMs - Number(previousJob.durationMs),
          (() => {
            const deltaMs = durationMs - Number(previousJob.durationMs);
            const tone = queryComparisonTone(deltaMs);
            if (tone === "neutral") {
              return `Essentially unchanged versus the previous comparable run (${formatQueryDuration(previousJob.durationMs)}).`;
            }
            return `${formatQueryDuration(Math.abs(deltaMs))} ${tone} than the previous comparable run (${formatQueryDuration(
              previousJob.durationMs
            )}). Comparable means the same notebook cell, backend, and selected source set.`;
          })()
        )
      : null;

  const medianWindow = history
    .slice(-5)
    .map((entry) => Number(entry.durationMs))
    .filter((value) => Number.isFinite(value) && value > 0);
  const rollingMedianMs = medianWindow.length >= 2 ? medianDuration(medianWindow) : null;
  const median =
    Number.isFinite(rollingMedianMs)
      ? buildQueryComparisonInsight(
          "vs median",
          rollingMedianMs,
          durationMs - rollingMedianMs,
          (() => {
            const deltaMs = durationMs - rollingMedianMs;
            const tone = queryComparisonTone(deltaMs);
            if (tone === "neutral") {
              return `Essentially unchanged versus the rolling median of the last ${medianWindow.length} comparable completed runs (${formatQueryDuration(
                rollingMedianMs
              )}).`;
            }
            return `${formatQueryDuration(Math.abs(deltaMs))} ${tone} than the rolling median of the last ${medianWindow.length} comparable completed runs (${formatQueryDuration(
              rollingMedianMs
            )}).`;
          })()
        )
      : null;

  if (!previous && !median) {
    return null;
  }

  return { previous, median };
}

function buildQueryJobFootprint(job) {
  const touchedRelations = [];
  const seenRelations = new Set();
  for (const value of job?.touchedRelations ?? []) {
    const relationName = String(value ?? "").trim();
    const relationKey = relationName.toLowerCase();
    if (!relationName || seenRelations.has(relationKey)) {
      continue;
    }
    seenRelations.add(relationKey);
    touchedRelations.push(relationName);
  }

  const touchedBuckets = [];
  const seenBuckets = new Set();
  for (const value of job?.touchedBuckets ?? []) {
    const bucketName = String(value ?? "").trim();
    const bucketKey = bucketName.toLowerCase();
    if (!bucketName || seenBuckets.has(bucketKey)) {
      continue;
    }
    seenBuckets.add(bucketKey);
    touchedBuckets.push(bucketName);
  }

  const selectedSources = normalizeDataSources(job?.dataSources ?? []);
  const parts = [];
  if (touchedRelations.length) {
    parts.push(`${touchedRelations.length} relation${touchedRelations.length === 1 ? "" : "s"}`);
  }
  if (selectedSources.length) {
    parts.push(`${selectedSources.length} source${selectedSources.length === 1 ? "" : "s"}`);
  }
  if (touchedBuckets.length) {
    parts.push(`${touchedBuckets.length} bucket${touchedBuckets.length === 1 ? "" : "s"}`);
  }

  if (!parts.length) {
    return null;
  }

  const tooltipSections = [];
  if (touchedRelations.length) {
    tooltipSections.push(`Touched relations: ${touchedRelations.join(", ")}`);
  }
  const sourceLabels = sourceLabelsForIds(selectedSources);
  if (sourceLabels.length) {
    tooltipSections.push(`Selected sources: ${sourceLabels.join(", ")}`);
  }
  if (touchedBuckets.length) {
    tooltipSections.push(`S3 buckets: ${touchedBuckets.join(", ")}`);
  }

  return {
    label: "touches",
    value: parts.join(" | "),
    tone: "neutral",
    title: tooltipSections.join("\n"),
  };
}

function buildQueryJobTimingInsight(job) {
  const firstRowMs = Number(job?.firstRowMs);
  if (!Number.isFinite(firstRowMs) || firstRowMs < 0) {
    return null;
  }

  const fetchMs = Number(job?.fetchMs);
  const valueParts = [`first row ${formatQueryDuration(firstRowMs)}`];
  if (Number.isFinite(fetchMs) && fetchMs >= 0) {
    valueParts.push(`fetch ${formatQueryDuration(fetchMs)}`);
  }

  return {
    label: "split",
    value: valueParts.join(" | "),
    tone: "neutral",
    title:
      "Breaks the runtime into time until the first row was available and the remaining time spent fetching result rows for the UI.",
  };
}

function decorateQueryJobsWithInsights(jobs) {
  const historyByKey = new Map();
  const comparisonById = new Map();
  const completedJobs = jobs
    .filter((job) => job?.status === "completed")
    .slice()
    .sort(compareQueryJobsByCompletedAt);

  completedJobs.forEach((job) => {
    const comparisonKey = queryJobComparisonKey(job);
    const history = historyByKey.get(comparisonKey) ?? [];
    comparisonById.set(job.jobId, buildQueryJobComparisonMetrics(job, history));
    history.push(job);
    if (history.length > 12) {
      history.shift();
    }
    historyByKey.set(comparisonKey, history);
  });

  return jobs.map((job) => ({
    ...job,
    comparisonInsights: comparisonById.get(job.jobId) ?? null,
    footprintInsights: buildQueryJobFootprint(job),
    timingInsights: buildQueryJobTimingInsight(job),
  }));
}

function queryInsightPillMarkup(insight, { compact = false } = {}) {
  if (!insight?.value) {
    return "";
  }

  const toneClass = insight.tone ? ` is-${escapeHtml(insight.tone)}` : "";
  const compactClass = compact ? " query-insight-pill-compact" : "";
  const titleAttribute = insight.title ? ` title="${escapeHtml(insight.title)}"` : "";
  return `
    <span class="query-insight-pill${toneClass}${compactClass}"${titleAttribute}>
      <strong>${escapeHtml(insight.label || "")}</strong>
      <span>${escapeHtml(insight.value)}</span>
    </span>
  `;
}

function formatQueryTimestamp(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return parsed.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatEventDateTime(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  const baseDateTime = new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(parsed);

  try {
    const resolvedTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const timeZoneLabel = new Intl.DateTimeFormat("en-GB", {
      timeZone: resolvedTimeZone,
      timeZoneName: "short",
    })
      .formatToParts(parsed)
      .find((part) => part.type === "timeZoneName")
      ?.value;

    return timeZoneLabel ? `${baseDateTime} ${timeZoneLabel}` : baseDateTime;
  } catch (error) {
    return baseDateTime;
  }
}

function dataGenerationJobIsRunning(job) {
  return Boolean(job && dataGenerationRunningStatuses.has(job.status));
}

function dataGenerationJobStatusCopy(job) {
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

function dataGenerationJobElapsedMs(job) {
  if (!job) {
    return 0;
  }

  if (dataGenerationJobIsRunning(job)) {
    const startedAtMs = Date.parse(job.startedAt || "");
    if (!Number.isNaN(startedAtMs)) {
      return Math.max(0, Date.now() - startedAtMs);
    }
  }

  return Number.isFinite(Number(job.durationMs)) ? Math.max(0, Number(job.durationMs)) : 0;
}

function formatDataGenerationSize(valueGb) {
  const sizeGb = Number(valueGb);
  if (!Number.isFinite(sizeGb) || sizeGb <= 0) {
    return "0 GB";
  }

  if (sizeGb >= 1) {
    return `${sizeGb.toFixed(sizeGb >= 10 ? 0 : 1)} GB`;
  }
  return `${(sizeGb * 1024).toFixed(sizeGb * 1024 >= 10 ? 0 : 1)} MB`;
}

function dataGenerationJobStartedCopy(job) {
  return formatQueryTimestamp(job?.startedAt || "") || "Pending";
}

function dataGenerationJobCompletedCopy(job) {
  if (job?.completedAt) {
    return formatQueryTimestamp(job.completedAt) || "Unavailable";
  }
  if (dataGenerationJobIsRunning(job)) {
    return "Running";
  }
  if (job?.status === "queued") {
    return "Pending";
  }
  return "Not finished";
}

function dataGenerationJobTimingCopy(job) {
  return `Start: ${dataGenerationJobStartedCopy(job)} | End: ${dataGenerationJobCompletedCopy(job)}`;
}

function queryJobEventDateTimeCopy(job) {
  const timestamp = queryJobIsRunning(job)
    ? job?.startedAt || job?.updatedAt || ""
    : job?.completedAt || job?.updatedAt || job?.startedAt || "";
  const formatted = formatEventDateTime(timestamp);
  if (!formatted) {
    return "Event: Unavailable";
  }

  return `${queryJobIsRunning(job) ? "Started" : "Event"}: ${formatted}`;
}

function dataGenerationJobEventDateTimeCopy(job) {
  const timestamp = dataGenerationJobIsRunning(job)
    ? job?.startedAt || job?.updatedAt || ""
    : job?.completedAt || job?.updatedAt || job?.startedAt || "";
  const formatted = formatEventDateTime(timestamp);
  if (!formatted) {
    return "Event: Unavailable";
  }

  return `${dataGenerationJobIsRunning(job) ? "Started" : "Event"}: ${formatted}`;
}

function dataGenerationJobCopy(job) {
  if (!job) {
    return "";
  }

  const sizeCopy = formatDataGenerationSize(job.generatedSizeGb || job.requestedSizeGb);
  const rowCount = Number(job.generatedRows || 0);
  const rowsCopy =
    rowCount > 0
      ? `${rowCount.toLocaleString()} rows`
      : dataGenerationJobIsRunning(job)
        ? "Starting"
        : "0 rows";
  return `${formatQueryDuration(dataGenerationJobElapsedMs(job))} | ${sizeCopy} | ${rowsCopy}`;
}

function firstAvailableIngestionRunbookId() {
  return String(dataGeneratorsCatalog[0]?.generatorId || "").trim();
}

function ingestionGeneratorById(generatorId) {
  const normalizedGeneratorId = String(generatorId ?? "").trim();
  if (!normalizedGeneratorId) {
    return null;
  }

  return dataGeneratorsCatalog.find((generator) => generator.generatorId === normalizedGeneratorId) ?? null;
}

function selectedIngestionGenerator() {
  return ingestionGeneratorById(selectedIngestionRunbookId);
}

function resolveSelectedIngestionRunbookId(preferredGeneratorId = "") {
  const preferred = ingestionGeneratorById(preferredGeneratorId);
  if (preferred) {
    selectedIngestionRunbookId = preferred.generatorId;
    return selectedIngestionRunbookId;
  }

  const existing = selectedIngestionGenerator();
  if (existing) {
    return existing.generatorId;
  }

  selectedIngestionRunbookId = firstAvailableIngestionRunbookId();
  return selectedIngestionRunbookId;
}

function filteredIngestionGenerators() {
  const selectedGenerator = selectedIngestionGenerator();
  return selectedGenerator ? [selectedGenerator] : [];
}

function filteredDataGenerationJobs() {
  const selectedGeneratorId = resolveSelectedIngestionRunbookId();
  if (!selectedGeneratorId) {
    return [];
  }

  return dataGenerationJobsSnapshot.filter((job) => job.generatorId === selectedGeneratorId);
}

function openRunbookAncestors(node) {
  if (!(node instanceof Element)) {
    return;
  }

  document.querySelector("[data-ingestion-runbook-section]")?.setAttribute("open", "");
  let currentFolder = node.closest("[data-runbook-folder]");
  while (currentFolder) {
    currentFolder.open = true;
    currentFolder = currentFolder.parentElement?.closest("[data-runbook-folder]") ?? null;
  }
}

function syncSelectedIngestionRunbookState() {
  const selectedGeneratorId = resolveSelectedIngestionRunbookId();
  let activeSidebarLink = null;

  document.querySelectorAll("[data-open-ingestion-runbook]").forEach((button) => {
    const isActive = (button.dataset.openIngestionRunbook || "") === selectedGeneratorId;
    const isSpotlighted = (button.dataset.openIngestionRunbook || "") === spotlightIngestionRunbookId;
    button.classList.toggle("is-active", isActive);
    button.classList.toggle("is-spotlighted", isSpotlighted);
    if (isActive && button.matches(".runbook-link")) {
      activeSidebarLink = button;
    }
  });

  if (activeSidebarLink) {
    openRunbookAncestors(activeSidebarLink);
  }
}

function scheduleIngestionRunbookSpotlight(generatorId) {
  spotlightIngestionRunbookId = String(generatorId ?? "").trim();
  if (ingestionRunbookSpotlightHandle !== null) {
    window.clearTimeout(ingestionRunbookSpotlightHandle);
  }
  syncSelectedIngestionRunbookState();
  if (currentWorkspaceMode() === "ingestion") {
    renderIngestionWorkbench();
  }

  ingestionRunbookSpotlightHandle = window.setTimeout(() => {
    spotlightIngestionRunbookId = "";
    ingestionRunbookSpotlightHandle = null;
    syncSelectedIngestionRunbookState();
    if (currentWorkspaceMode() === "ingestion") {
      renderIngestionWorkbench();
    }
  }, 3200);
}

function selectIngestionRunbook(generatorId, { spotlight = false } = {}) {
  const selectedGeneratorId = resolveSelectedIngestionRunbookId(generatorId);
  syncSelectedIngestionRunbookState();
  if (spotlight && selectedGeneratorId) {
    scheduleIngestionRunbookSpotlight(selectedGeneratorId);
  }
  return selectedGeneratorId;
}

function compareDataGenerationJobsByStartedAt(left, right) {
  const leftStartedAt = Date.parse(left?.startedAt || "");
  const rightStartedAt = Date.parse(right?.startedAt || "");

  if (!Number.isNaN(leftStartedAt) || !Number.isNaN(rightStartedAt)) {
    const normalizedLeft = Number.isNaN(leftStartedAt) ? 0 : leftStartedAt;
    const normalizedRight = Number.isNaN(rightStartedAt) ? 0 : rightStartedAt;
    if (normalizedLeft !== normalizedRight) {
      return normalizedRight - normalizedLeft;
    }
  }

  return String(right?.jobId || "").localeCompare(String(left?.jobId || ""));
}

function compareQueryJobsByStartedAt(left, right) {
  const leftStartedAt = Date.parse(left?.startedAt || left?.updatedAt || "");
  const rightStartedAt = Date.parse(right?.startedAt || right?.updatedAt || "");

  if (!Number.isNaN(leftStartedAt) || !Number.isNaN(rightStartedAt)) {
    const normalizedLeft = Number.isNaN(leftStartedAt) ? 0 : leftStartedAt;
    const normalizedRight = Number.isNaN(rightStartedAt) ? 0 : rightStartedAt;
    if (normalizedLeft !== normalizedRight) {
      return normalizedRight - normalizedLeft;
    }
  }

  return String(right?.jobId || "").localeCompare(String(left?.jobId || ""));
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

function queryProgressActivityCopy(job) {
  if (!job || !queryJobIsRunning(job)) {
    return "Query activity is idle.";
  }

  if (job.status === "queued") {
    return "Waiting for the query worker to start this statement.";
  }

  if (Number(job.rowsShown || 0) > 0) {
    return `${job.rowsShown} row(s) are already available in the live preview.`;
  }

  const progressLabel = String(job.progressLabel || "").toLowerCase();
  const message = String(job.message || "").toLowerCase();
  const combined = `${progressLabel} ${message}`;

  if (combined.includes("fetch")) {
    return "Fetching the first rows for the live preview.";
  }

  if (combined.includes("finaliz")) {
    return "Finalizing the statement result.";
  }

  return "Completion percent is not available for this query yet.";
}

function queryProgressMarkup(job) {
  if (!queryJobIsRunning(job)) {
    return "";
  }

  const progressValue =
    typeof job.progress === "number" && Number.isFinite(job.progress)
      ? Math.max(0, Math.min(100, job.progress * 100))
      : null;
  const backendCopy = escapeHtml(job.backendName || "VMTP DUCKDB");
  const progressLabel = escapeHtml(job.progressLabel || "Running...");

  if (progressValue === null) {
    return `
      <div class="query-progress-card query-progress-card-indeterminate">
        <div class="query-progress-copy">
          <strong>${progressLabel}</strong>
          <span>${backendCopy}</span>
        </div>
        <div class="query-progress-status">
          <span class="query-progress-status-dot" aria-hidden="true"></span>
          <span>${escapeHtml(queryProgressActivityCopy(job))}</span>
        </div>
      </div>
    `;
  }

  return `
    <div class="query-progress-card">
      <div class="query-progress-copy">
        <strong>${progressLabel}</strong>
        <span>${backendCopy} | ${Math.round(progressValue)}%</span>
      </div>
      <div class="query-progress-track">
        <span style="width:${progressValue}%;"></span>
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

function resultExportMenuMarkup(showActions, jobId = "") {
  const normalizedJobId = String(jobId || "").trim();
  const sharedWorkspaceTooltip =
    "Saves into the configured Shared Workspace MinIO / S3 bucket.";
  const localWorkspaceTooltip =
    "Saves in this browser's Local Workspace using IndexedDB.";
  return `
    <details
      class="workspace-action-menu result-action-menu"
      data-result-action-menu
      data-result-job-id="${escapeHtml(normalizedJobId)}"
      ${showActions ? "" : "hidden"}
    >
      <summary
        class="workspace-action-menu-toggle result-action-menu-toggle"
        aria-label="Export or save query results"
        title="Export or save query results"
      >
        <span class="result-action-menu-label">Export / Save</span>
      </summary>
      <div class="workspace-action-menu-panel result-action-menu-panel">
        <button
          type="button"
          class="workspace-action-menu-item"
          data-result-export-s3="parquet"
          data-result-job-id="${escapeHtml(normalizedJobId)}"
          title="${escapeHtml(sharedWorkspaceTooltip)}"
        >Save Results in Parquet Format to Shared Workspace ...</button>
        <button
          type="button"
          class="workspace-action-menu-item"
          data-result-export-s3="json"
          data-result-job-id="${escapeHtml(normalizedJobId)}"
          title="${escapeHtml(sharedWorkspaceTooltip)}"
        >Save Results in JSON Format to Shared Workspace ...</button>
        <div class="workspace-action-menu-separator"></div>
        <button
          type="button"
          class="workspace-action-menu-item"
          data-result-export-local="parquet"
          data-result-job-id="${escapeHtml(normalizedJobId)}"
          title="${escapeHtml(localWorkspaceTooltip)}"
        >Save Results in Parquet Format to Local Workspace</button>
        <button
          type="button"
          class="workspace-action-menu-item"
          data-result-export-local="json"
          data-result-job-id="${escapeHtml(normalizedJobId)}"
          title="${escapeHtml(localWorkspaceTooltip)}"
        >Save Results in JSON Format to Local Workspace</button>
        <div class="workspace-action-menu-separator"></div>
        <button
          type="button"
          class="workspace-action-menu-item"
          data-result-export-download="parquet"
          data-result-job-id="${escapeHtml(normalizedJobId)}"
        >Download Results in Parquet Format</button>
        <button
          type="button"
          class="workspace-action-menu-item"
          data-result-export-download="json"
          data-result-job-id="${escapeHtml(normalizedJobId)}"
        >Download Results in JSON Format</button>
        <button
          type="button"
          class="workspace-action-menu-item"
          data-result-export-download="csv"
          data-result-job-id="${escapeHtml(normalizedJobId)}"
        >Download Results in CSV Format</button>
      </div>
    </details>
  `;
}

function resultMetricStripMarkup(job) {
  if (!job) {
    return "";
  }

  const metricPills = [];
  if (job.comparisonInsights?.previous) {
    metricPills.push(queryInsightPillMarkup(job.comparisonInsights.previous));
  }
  if (job.comparisonInsights?.median) {
    metricPills.push(queryInsightPillMarkup(job.comparisonInsights.median));
  }
  if (job.timingInsights) {
    metricPills.push(queryInsightPillMarkup(job.timingInsights));
  }
  if (job.footprintInsights) {
    metricPills.push(queryInsightPillMarkup(job.footprintInsights));
  }

  if (!metricPills.length) {
    return "";
  }

  return `<div class="result-metric-strip">${metricPills.join("")}</div>`;
}

function queryMonitorInsightStripMarkup(job) {
  if (!job) {
    return "";
  }

  const metricPills = [];
  const comparisonInsight = job.comparisonInsights?.previous || job.comparisonInsights?.median || null;
  if (comparisonInsight) {
    metricPills.push(queryInsightPillMarkup(comparisonInsight, { compact: true }));
  }
  if (job.footprintInsights) {
    metricPills.push(queryInsightPillMarkup(job.footprintInsights, { compact: true }));
  }

  if (!metricPills.length) {
    return "";
  }

  return `<div class="query-monitor-item-insights">${metricPills.join("")}</div>`;
}

function queryResultPanelMarkup(cellId, job = null) {
  if (!job) {
    return emptyQueryResultsMarkup(cellId);
  }

  const showExportActions = job.status === "completed" && job.columns.length > 0;
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
    <section
      id="query-results-${escapeHtml(cellId)}"
      class="result-panel"
      data-cell-result
      data-query-job-id="${escapeHtml(job.jobId || "")}"
    >
      <header class="result-header">
        <div class="result-header-copy">
          <h3>Result</h3>
          <div class="result-meta-row">
            <p class="result-meta" data-query-duration data-job-id="${escapeHtml(job.jobId || "")}">${escapeHtml(formatQueryDuration(queryJobElapsedMs(job)))}</p>
            ${resultMetricStripMarkup(job)}
          </div>
        </div>
        <div class="result-header-actions">
          <span class="result-badge${queryJobIsRunning(job) ? " is-live" : ""}" ${showRowsBadge ? "" : "hidden"}>${escapeHtml(rowsBadge)}</span>
          ${resultExportMenuMarkup(showExportActions, job.jobId || "")}
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

function renderPerformanceDistributionMarkup(performance) {
  const points = Array.isArray(performance?.recent) ? performance.recent : [];
  if (!points.length) {
    return "";
  }

  const values = points
    .map((point) => Math.max(1, Number(point.durationMs || 0)))
    .filter((value) => Number.isFinite(value));
  if (!values.length) {
    return "";
  }

  const width = 280;
  const height = 86;
  const paddingX = 8;
  const paddingTop = 6;
  const paddingBottom = 18;
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / Math.max(1, values.length - 1);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const fallbackSpread = Math.max((maxValue - minValue) / 6, mean * 0.18, 1);
  const standardDeviation = Math.max(Math.sqrt(variance), fallbackSpread);
  const domainStart = Math.max(0, Math.min(minValue, mean - standardDeviation * 3));
  const domainEnd = Math.max(domainStart + 1, Math.max(maxValue, mean + standardDeviation * 3));
  const plotWidth = width - paddingX * 2;
  const plotHeight = height - paddingTop - paddingBottom;
  const sampleCount = 48;
  const gaussianPoints = [];

  let peakDensity = 0;
  for (let index = 0; index < sampleCount; index += 1) {
    const ratio = sampleCount === 1 ? 0 : index / (sampleCount - 1);
    const xValue = domainStart + ratio * (domainEnd - domainStart);
    const density = Math.exp(-0.5 * ((xValue - mean) / standardDeviation) ** 2);
    peakDensity = Math.max(peakDensity, density);
    gaussianPoints.push({ ratio, xValue, density });
  }

  const pathPoints = gaussianPoints.map(({ ratio, density }) => {
    const x = paddingX + ratio * plotWidth;
    const y = paddingTop + (1 - density / Math.max(peakDensity, 0.0001)) * plotHeight;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  });
  const areaPath = [
    `M ${paddingX} ${height - paddingBottom}`,
    ...gaussianPoints.map(({ ratio, density }) => {
      const x = paddingX + ratio * plotWidth;
      const y = paddingTop + (1 - density / Math.max(peakDensity, 0.0001)) * plotHeight;
      return `L ${x.toFixed(2)} ${y.toFixed(2)}`;
    }),
    `L ${width - paddingX} ${height - paddingBottom}`,
    "Z",
  ].join(" ");

  const markerForValue = (value, className, label) => {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return "";
    }
    const ratio = Math.max(0, Math.min(1, (value - domainStart) / (domainEnd - domainStart)));
    const x = paddingX + ratio * plotWidth;
    return `
      <line x1="${x.toFixed(2)}" y1="${paddingTop}" x2="${x.toFixed(2)}" y2="${(height - paddingBottom).toFixed(
        2
      )}" class="query-monitor-distribution-marker ${className}"></line>
      <title>${escapeHtml(`${label}: ${formatQueryDuration(value)}`)}</title>
    `;
  };

  const tickEntries = [
    ["Min", minValue],
    ["Mean", mean],
    ["Max", maxValue],
  ];

  return `
    <div class="query-monitor-distribution-header">
      <h4>Runtime Distribution</h4>
      <p>Bell curve of recent successful query runtimes.</p>
    </div>
    <svg viewBox="0 0 ${width} ${height}" class="query-monitor-distribution-svg" preserveAspectRatio="none" aria-hidden="true">
      <path d="${areaPath}" class="query-monitor-distribution-area"></path>
      <polyline points="${pathPoints.join(" ")}" class="query-monitor-distribution-curve"></polyline>
      ${markerForValue(mean, "is-mean", "Mean")}
      ${markerForValue(performance?.stats?.p50Ms, "is-p50", "p50")}
      ${markerForValue(performance?.stats?.p95Ms, "is-p95", "p95")}
    </svg>
    <div class="query-monitor-distribution-ticks">
      ${tickEntries
        .map(
          ([label, value]) => `
            <span class="query-monitor-distribution-tick">
              <strong>${escapeHtml(label)}</strong>
              <span>${escapeHtml(formatQueryDuration(value))}</span>
            </span>
          `
        )
        .join("")}
    </div>
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
  const timestamp = job.startedAt || job.updatedAt;
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
        ${queryMonitorInsightStripMarkup(job)}
        <p class="query-monitor-sql">${escapeHtml(job.sql)}</p>
      </div>
      <div class="query-monitor-item-actions">
        ${running ? `<button type="button" class="query-monitor-cancel" data-cancel-query-job="${escapeHtml(job.jobId)}">Cancel</button>` : ""}
        <span class="query-monitor-updated">${escapeHtml(formatQueryTimestamp(timestamp))}</span>
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
      <span class="topbar-notification-item-copy topbar-notification-item-copy-secondary">${escapeHtml(queryJobEventDateTimeCopy(job))}</span>
    </button>
  `;
}

function dataGeneratorCardMarkup(generator) {
  const isSelected = generator.generatorId === resolveSelectedIngestionRunbookId();
  const isSpotlighted = generator.generatorId === spotlightIngestionRunbookId;
  const tagsMarkup = (generator.tags || [])
    .map((tag) => `<span class="ingestion-generator-tag">${escapeHtml(tag)}</span>`)
    .join("");

  return `
    <article class="ingestion-generator-card${isSelected ? " is-selected" : ""}${isSpotlighted ? " is-spotlighted" : ""}" data-generator-card data-generator-id="${escapeHtml(generator.generatorId)}">
      <div class="ingestion-generator-card-header">
        <div class="ingestion-generator-copy">
          <h4>${escapeHtml(generator.title)}</h4>
          <p class="ingestion-generator-description">${escapeHtml(generator.description)}</p>
        </div>
        <div class="ingestion-generator-tags">
          <span class="ingestion-generator-tag">${escapeHtml(generator.targetKind.toUpperCase())}</span>
          ${tagsMarkup}
        </div>
      </div>
      <div class="ingestion-generator-controls">
        <div class="ingestion-generator-size">
          <label for="generator-size-${escapeHtml(generator.generatorId)}">Generate size</label>
          <div class="ingestion-generator-size-input">
            <input
              id="generator-size-${escapeHtml(generator.generatorId)}"
              class="modal-input"
              type="number"
              min="${escapeHtml(generator.minSizeGb)}"
              max="${escapeHtml(generator.maxSizeGb)}"
              step="0.01"
              value="${escapeHtml(generator.defaultSizeGb)}"
              data-ingestion-size-input
            >
            <span class="ingestion-generator-size-unit" aria-hidden="true">GB</span>
          </div>
        </div>
        <button
          type="button"
          class="modal-button ingestion-generator-run"
          data-start-data-generation="${escapeHtml(generator.generatorId)}"
        >
          Run Module
        </button>
        <p class="ingestion-job-meta-copy">
          Default target: <strong>${escapeHtml(generator.defaultTargetName || generator.generatorId)}</strong><br>
          Module: <code>${escapeHtml(generator.moduleName || generator.generatorId)}</code>
        </p>
      </div>
    </article>
  `;
}

function dataGenerationJobFactsMarkup(job) {
  const facts = [
    ["Target", job.targetKind.toUpperCase()],
    ["Requested size", formatDataGenerationSize(job.requestedSizeGb)],
    ["Started", dataGenerationJobStartedCopy(job)],
    ["Ended", dataGenerationJobCompletedCopy(job)],
    ["Elapsed", formatQueryDuration(dataGenerationJobElapsedMs(job))],
    ["Generated size", formatDataGenerationSize(job.generatedSizeGb || 0)],
    ["Rows", Number(job.generatedRows || 0) > 0 ? Number(job.generatedRows).toLocaleString() : "0"],
  ];

  return `
    <div class="ingestion-job-facts">
      ${facts
        .map(
          ([label, value]) => `
            <div class="ingestion-job-fact">
              <span class="ingestion-job-fact-label">${escapeHtml(label)}</span>
              <span class="ingestion-job-fact-value${label === "Elapsed" ? " ingestion-job-fact-value-live" : ""}"${
                label === "Elapsed" ? ` data-ingestion-job-duration data-job-id="${escapeHtml(job.jobId)}"` : ""
              }>${escapeHtml(String(value))}</span>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function dataGenerationJobProgressMarkup(job) {
  const progressValue =
    typeof job.progress === "number" && Number.isFinite(job.progress)
      ? Math.max(0, Math.min(100, job.progress * 100))
      : null;
  const showTrack = dataGenerationJobIsRunning(job) || progressValue !== null;

  return `
    <div class="ingestion-job-progress">
      <div class="ingestion-job-progress-header">
        <strong>${escapeHtml(job.progressLabel || dataGenerationJobStatusCopy(job))}</strong>
      </div>
      ${
        showTrack
          ? `
            <div class="ingestion-job-progress-track${progressValue === null ? " is-indeterminate" : ""}">
              <span style="${progressValue === null ? "" : `width:${progressValue}%;`}"></span>
            </div>
          `
          : ""
      }
    </div>
  `;
}

function dataGenerationJobCardMarkup(job) {
  return `
    <article class="ingestion-job-card" data-data-generation-job-card data-job-id="${escapeHtml(job.jobId)}">
      <div class="ingestion-job-card-header">
        <div class="ingestion-job-copy">
          <h4>${escapeHtml(job.title)}</h4>
          <p class="ingestion-job-description">${escapeHtml(job.description)}</p>
        </div>
        <div class="ingestion-job-actions">
          <span class="ingestion-job-status${dataGenerationJobIsRunning(job) ? " is-live" : ""}">${escapeHtml(
            dataGenerationJobStatusCopy(job)
          )}</span>
          ${
            dataGenerationJobIsRunning(job)
              ? `<button type="button" class="ingestion-job-cancel" data-cancel-data-generation-job="${escapeHtml(job.jobId)}">Cancel</button>`
              : ""
          }
          ${
            job.canCleanup
              ? `<button type="button" class="ingestion-job-clean" data-cleanup-data-generation-job="${escapeHtml(job.jobId)}">Clean loader data</button>`
              : ""
          }
        </div>
      </div>
      ${dataGenerationJobProgressMarkup(job)}
      ${dataGenerationJobFactsMarkup(job)}
      <div class="ingestion-job-targets">
        ${
          job.targetRelation
            ? `<span class="ingestion-job-target"><strong>Relation:</strong> ${escapeHtml(job.targetRelation)}</span>`
            : ""
        }
        ${
          job.targetPath
            ? `<span class="ingestion-job-target"><strong>Path:</strong> ${escapeHtml(job.targetPath)}</span>`
            : ""
        }
      </div>
      <p class="ingestion-job-message">${escapeHtml(job.message || "")}</p>
      ${job.error ? `<pre class="ingestion-job-error">${escapeHtml(job.error)}</pre>` : ""}
    </article>
  `;
}

function dataGenerationNotificationItemMarkup(job) {
  return `
    <button
      type="button"
      class="topbar-notification-item"
      data-open-ingestion-workbench
      data-focus-generation-job="${escapeHtml(job.jobId)}"
      title="Open the ingestion workbench"
    >
      <span class="topbar-notification-item-status topbar-notification-item-status-notice${dataGenerationJobIsRunning(job) ? " is-live" : ""}">${escapeHtml(
        `${dataGenerationJobStatusCopy(job)} ingestion`
      )}</span>
      <span class="topbar-notification-item-title">${escapeHtml(job.title)}</span>
      <span class="topbar-notification-item-copy" data-data-generation-notification-copy data-job-id="${escapeHtml(
        job.jobId
      )}">${escapeHtml(dataGenerationJobCopy(job))}</span>
      <span class="topbar-notification-item-copy topbar-notification-item-copy-secondary">${escapeHtml(dataGenerationJobTimingCopy(job))}</span>
      <span class="topbar-notification-item-copy topbar-notification-item-copy-secondary">${escapeHtml(
        dataGenerationJobEventDateTimeCopy(job)
      )}</span>
    </button>
  `;
}

function dataGenerationMonitorItemMarkup(job) {
  const running = dataGenerationJobIsRunning(job);
  const sizeCopy = formatDataGenerationSize(job.generatedSizeGb || job.requestedSizeGb);
  const rowsCopy =
    Number(job.generatedRows || 0) > 0 ? `${Number(job.generatedRows).toLocaleString()} rows` : "No rows yet";
  const summaryCopy = job.targetRelation || job.targetPath || job.message || job.description || "";

  return `
    <article class="query-monitor-item query-monitor-item-${escapeHtml(job.status)}" data-data-generation-job-id="${escapeHtml(job.jobId)}">
      <div class="query-monitor-item-copy">
        <button
          type="button"
          class="query-monitor-open"
          data-open-ingestion-workbench
          data-focus-generation-job="${escapeHtml(job.jobId)}"
          title="Open ${escapeHtml(job.title)}"
        >
          ${escapeHtml(job.title)}
        </button>
        <div class="query-monitor-item-meta">
          <span class="query-monitor-status-badge${running ? " is-live" : ""}">${escapeHtml(
            dataGenerationJobStatusCopy(job)
          )}</span>
          <span data-generation-monitor-duration data-job-id="${escapeHtml(job.jobId)}">${escapeHtml(
            formatQueryDuration(dataGenerationJobElapsedMs(job))
          )}</span>
          <span>${escapeHtml(sizeCopy)}</span>
          <span>${escapeHtml(rowsCopy)}</span>
        </div>
        <p class="query-monitor-sql">${escapeHtml(summaryCopy)}</p>
      </div>
      <div class="query-monitor-item-actions">
        ${
          running
            ? `<button type="button" class="query-monitor-cancel" data-cancel-data-generation-job="${escapeHtml(job.jobId)}">Cancel</button>`
            : ""
        }
        <div class="query-monitor-timestamps">
          <span class="query-monitor-updated">Start ${escapeHtml(dataGenerationJobStartedCopy(job))}</span>
          <span class="query-monitor-updated">End ${escapeHtml(dataGenerationJobCompletedCopy(job))}</span>
        </div>
      </div>
    </article>
  `;
}

function notificationItemKey(type, job) {
  const status = String(job?.status || "").trim().toLowerCase();
  const lifecycleKey =
    status === "completed" || status === "failed" || status === "cancelled" ? status : "active";
  return `${type}:${job?.jobId || ""}:${lifecycleKey}`;
}

function collectVisibleNotifications() {
  const activeNotebookId = currentWorkspaceNotebookId();
  const queryNotifications = queryJobsSnapshot
    .filter((job) => job.notebookId !== activeNotebookId)
    .map((job) => ({
      type: "query",
      job,
      updatedAt: job.updatedAt,
      dismissalKey: notificationItemKey("query", job),
      dismissible: queryJobTerminalStatuses.has(job.status),
      markup: queryNotificationItemMarkup(job),
    }));
  const dataGenerationNotifications = dataGenerationJobsSnapshot.map((job) => ({
    type: "ingestion",
    job,
    updatedAt: job.updatedAt,
    dismissalKey: notificationItemKey("ingestion", job),
    dismissible: dataGenerationTerminalStatuses.has(job.status),
    markup: dataGenerationNotificationItemMarkup(job),
  }));

  return [...dataGenerationNotifications, ...queryNotifications]
    .filter((item) => !dismissedNotificationKeys.has(item.dismissalKey))
    .sort((left, right) => Date.parse(right.updatedAt || "") - Date.parse(left.updatedAt || ""));
}

function pruneDismissedNotificationKeys() {
  const validKeys = new Set([
    ...queryJobsSnapshot.map((job) => notificationItemKey("query", job)),
    ...dataGenerationJobsSnapshot.map((job) => notificationItemKey("ingestion", job)),
  ]);
  let changed = false;

  for (const key of dismissedNotificationKeys) {
    if (key.startsWith("query:") && !queryJobsLoaded) {
      continue;
    }
    if (key.startsWith("ingestion:") && !dataGenerationJobsLoaded) {
      continue;
    }
    if (validKeys.has(key)) {
      continue;
    }
    dismissedNotificationKeys.delete(key);
    changed = true;
  }

  if (changed) {
    writeDismissedNotificationKeys();
  }
}

function clearVisibleNotifications() {
  const visibleItems = collectVisibleNotifications();
  if (!visibleItems.length) {
    return;
  }

  visibleItems.forEach((item) => dismissedNotificationKeys.add(item.dismissalKey));
  writeDismissedNotificationKeys();
  renderQueryNotificationMenu();
}

function renderIngestionWorkbench() {
  const generatorList = ingestionGeneratorList();
  const jobList = ingestionJobList();
  const generatorSectionTitle = ingestionGeneratorSectionTitle();
  const generatorSectionCopy = ingestionGeneratorSectionCopy();
  const jobSectionTitle = ingestionJobSectionTitle();
  const jobSectionCopy = ingestionJobSectionCopy();
  const selectedGeneratorId = resolveSelectedIngestionRunbookId();
  const selectedGenerator = ingestionGeneratorById(selectedGeneratorId);
  const visibleGenerators = filteredIngestionGenerators();
  const visibleJobs = filteredDataGenerationJobs();

  if (generatorSectionTitle) {
    generatorSectionTitle.textContent = selectedGenerator ? selectedGenerator.title : "Selected Runbook";
  }

  if (generatorSectionCopy) {
    generatorSectionCopy.innerHTML = selectedGenerator
      ? `Only the selected runbook is shown here. Module: <code>${escapeHtml(
          selectedGenerator.moduleName || selectedGenerator.generatorId
        )}</code>`
      : "No ingestion runbook is currently selected.";
  }

  if (jobSectionTitle) {
    jobSectionTitle.textContent = selectedGenerator ? `${selectedGenerator.title} Jobs` : "Runbook Jobs";
  }

  if (jobSectionCopy) {
    jobSectionCopy.textContent = selectedGenerator
      ? "Only executions for the selected runbook are listed here."
      : "Select a runbook from the left navigation to inspect its executions.";
  }

  if (generatorList) {
    if (!visibleGenerators.length) {
      generatorList.innerHTML = '<p class="ingestion-empty">No data generators discovered.</p>';
    } else {
      generatorList.innerHTML = visibleGenerators.map((generator) => dataGeneratorCardMarkup(generator)).join("");
    }
  }

  if (jobList) {
    if (!selectedGenerator) {
      jobList.innerHTML = '<p class="ingestion-empty">Select a runbook from the left navigation.</p>';
    } else if (!visibleJobs.length) {
      jobList.innerHTML =
        '<p class="ingestion-empty">No data generation jobs for this runbook yet. Run the loader first, then clean its output from the completed job card.</p>';
    } else {
      jobList.innerHTML = visibleJobs.map((job) => dataGenerationJobCardMarkup(job)).join("");
    }
  }
}

function recordNotebookActivity(notebookId, reason = "edited") {
  const normalizedNotebookId = String(notebookId ?? "").trim();
  if (!normalizedNotebookId) {
    return;
  }

  const metadata = notebookMetadata(normalizedNotebookId);
  const activity = readNotebookActivity();
  activity[normalizedNotebookId] = {
    notebookId: normalizedNotebookId,
    title: metadata.title,
    summary: metadata.summary,
    touchedAt: new Date().toISOString(),
    reason,
  };
  writeNotebookActivity(activity);
  renderHomePage();
}

function notebookActivityMarkup(entry) {
  const reasonCopy = entry.reason === "run" ? "Last action: Run" : "Last action: Edit";
  return `
    <button
      type="button"
      class="home-activity-card"
      data-open-recent-notebook="${escapeHtml(entry.notebookId)}"
    >
      <span class="home-activity-title-row">
        <span class="home-activity-title">${escapeHtml(entry.title || "Notebook")}</span>
        <span class="home-activity-meta">${escapeHtml(formatRelativeTimestamp(entry.touchedAt))}</span>
      </span>
      <span class="home-activity-copy">${escapeHtml(entry.summary || "No description saved.")}</span>
      <span class="home-activity-meta">${escapeHtml(reasonCopy)}</span>
    </button>
  `;
}

function ingestionActivityMarkup(job) {
  return `
    <button
      type="button"
      class="home-activity-card"
      data-open-ingestion-workbench
      data-focus-generation-job="${escapeHtml(job.jobId || "")}"
    >
      <span class="home-activity-title-row">
        <span class="home-activity-title">${escapeHtml(job.title || "Ingestion run")}</span>
        <span class="home-activity-meta">${escapeHtml(formatRelativeTimestamp(job.startedAt || job.updatedAt))}</span>
      </span>
      <span class="home-activity-copy">${escapeHtml(job.message || job.description || "No ingestion message yet.")}</span>
      <span class="home-activity-meta">${escapeHtml((job.status || "unknown").replace(/^./, (m) => m.toUpperCase()))} • ${escapeHtml(formatQueryDuration(dataGenerationJobElapsedMs(job)))}</span>
    </button>
  `;
}

function renderHomePage() {
  if (!homePageRoot()) {
    return;
  }

  const recentNotebooksRoot = homeRecentNotebooksRoot();
  if (recentNotebooksRoot) {
    const activityEntries = Object.values(readNotebookActivity())
      .filter((entry) => entry && typeof entry === "object")
      .map((entry) => ({
        notebookId: String(entry.notebookId || "").trim(),
        title: String(entry.title || "").trim(),
        summary: String(entry.summary || "").trim(),
        touchedAt: String(entry.touchedAt || "").trim(),
        reason: entry.reason === "run" ? "run" : "edited",
      }))
      .filter((entry) => entry.notebookId && notebookLinks(entry.notebookId).length)
      .sort((left, right) => Date.parse(right.touchedAt || "") - Date.parse(left.touchedAt || ""))
      .slice(0, 3);

    if (!activityEntries.length) {
      recentNotebooksRoot.innerHTML = '<p class="home-empty">No recent notebook activity yet.</p>';
    } else {
      recentNotebooksRoot.innerHTML = activityEntries.map((entry) => notebookActivityMarkup(entry)).join("");
    }
  }

  const recentIngestionsRoot = homeRecentIngestionsRoot();
  if (recentIngestionsRoot) {
    const recentJobs = [...dataGenerationJobsSnapshot]
      .sort((left, right) => Date.parse(right.startedAt || "") - Date.parse(left.startedAt || ""))
      .slice(0, 3);
    if (!recentJobs.length) {
      recentIngestionsRoot.innerHTML = '<p class="home-empty">No ingestion runs yet.</p>';
    } else {
      recentIngestionsRoot.innerHTML = recentJobs.map((job) => ingestionActivityMarkup(job)).join("");
    }
  }
}

function refreshLiveDataGenerationClock() {
  const jobsById = new Map(dataGenerationJobsSnapshot.map((job) => [job.jobId, job]));

  document.querySelectorAll("[data-ingestion-job-duration]").forEach((node) => {
    const job = jobsById.get(node.dataset.jobId || "");
    if (!job) {
      return;
    }
    node.textContent = formatQueryDuration(dataGenerationJobElapsedMs(job));
  });

  document.querySelectorAll("[data-generation-monitor-duration]").forEach((node) => {
    const job = jobsById.get(node.dataset.jobId || "");
    if (!job) {
      return;
    }
    node.textContent = formatQueryDuration(dataGenerationJobElapsedMs(job));
  });

  document.querySelectorAll("[data-data-generation-notification-copy]").forEach((node) => {
    const job = jobsById.get(node.dataset.jobId || "");
    if (!job) {
      return;
    }
    node.textContent = dataGenerationJobCopy(job);
  });
}

function syncDataGenerationClockLoop() {
  const hasRunningJobs = dataGenerationJobsSnapshot.some((job) => dataGenerationJobIsRunning(job));
  if (hasRunningJobs && dataGenerationClockHandle === null) {
    refreshLiveDataGenerationClock();
    dataGenerationClockHandle = window.setInterval(refreshLiveDataGenerationClock, 100);
    return;
  }

  if (!hasRunningJobs && dataGenerationClockHandle !== null) {
    window.clearInterval(dataGenerationClockHandle);
    dataGenerationClockHandle = null;
  }

  refreshLiveDataGenerationClock();
}

function captureSidebarState() {
  return {
    sidebarMode: currentSidebarMode(),
    searchTerm: document.querySelector("[data-sidebar-search]")?.value ?? "",
    notebookSectionOpen: Boolean(notebookSection()?.open),
    ingestionRunbookSectionOpen: Boolean(document.querySelector("[data-ingestion-runbook-section]")?.open),
    runbookFoldersOpen: Array.from(document.querySelectorAll("[data-runbook-folder][open]")).map(
      (node) => node.dataset.runbookFolderId || ""
    ),
    dataSourcesSectionOpen: Boolean(dataSourcesSection()?.open),
    generationMonitorSectionOpen: Boolean(document.querySelector("[data-generation-monitor-section]")?.open),
    queryMonitorSectionOpen: Boolean(document.querySelector("[data-query-monitor-section]")?.open),
    sourceCatalogsOpen: Array.from(document.querySelectorAll("[data-source-catalog][open]")).map(
      (node) => node.dataset.sourceCatalogName || ""
    ),
    sourceSchemasOpen: Array.from(document.querySelectorAll("[data-source-schema][open]")).map(
      (node) => node.dataset.sourceSchemaKey || ""
    ),
  };
}

function restoreSidebarState(state) {
  if (!state) {
    return;
  }

  const stateSidebarMode = state.sidebarMode === "ingestion" ? "ingestion" : "notebook";
  const sidebarMode = currentSidebarMode();

  const search = document.querySelector("[data-sidebar-search]");
  if (search) {
    search.value = state.searchTerm || "";
  }

  const notebookSectionRoot = notebookSection();
  if (notebookSectionRoot && stateSidebarMode === "notebook" && sidebarMode === "notebook") {
    notebookSectionRoot.open = Boolean(state.notebookSectionOpen);
  }

  const ingestionRunbookSectionRoot = document.querySelector("[data-ingestion-runbook-section]");
  if (ingestionRunbookSectionRoot && stateSidebarMode === "ingestion" && sidebarMode === "ingestion") {
    ingestionRunbookSectionRoot.open = Boolean(state.ingestionRunbookSectionOpen);
  }

  if (stateSidebarMode === "ingestion" && sidebarMode === "ingestion") {
    const openRunbookFolders = new Set(Array.isArray(state.runbookFoldersOpen) ? state.runbookFoldersOpen : []);
    document.querySelectorAll("[data-runbook-folder]").forEach((node) => {
      node.open = openRunbookFolders.has(node.dataset.runbookFolderId || "");
    });
  }

  const dataSourcesRoot = dataSourcesSection();
  if (dataSourcesRoot) {
    dataSourcesRoot.open = Boolean(state.dataSourcesSectionOpen);
  }

  const generationMonitorSectionRoot = document.querySelector("[data-generation-monitor-section]");
  if (generationMonitorSectionRoot && stateSidebarMode === "ingestion" && sidebarMode === "ingestion") {
    generationMonitorSectionRoot.open = Boolean(state.generationMonitorSectionOpen);
  }

  const queryMonitorSectionRoot = document.querySelector("[data-query-monitor-section]");
  if (queryMonitorSectionRoot && stateSidebarMode === "notebook" && sidebarMode === "notebook") {
    queryMonitorSectionRoot.open = Boolean(state.queryMonitorSectionOpen);
  }

  const openCatalogs = new Set(Array.isArray(state.sourceCatalogsOpen) ? state.sourceCatalogsOpen : []);
  document.querySelectorAll("[data-source-catalog]").forEach((node) => {
    node.open = openCatalogs.has(node.dataset.sourceCatalogName || "");
  });

  const openSchemas = new Set(Array.isArray(state.sourceSchemasOpen) ? state.sourceSchemasOpen : []);
  document.querySelectorAll("[data-source-schema]").forEach((node) => {
    node.open = openSchemas.has(node.dataset.sourceSchemaKey || "");
  });

  applySidebarSearchFilter();
}

async function refreshSidebar(mode = currentWorkspaceMode()) {
  const sidebar = document.querySelector("[data-sidebar]");
  if (!sidebar) {
    return;
  }

  const sidebarState = captureSidebarState();
  const activeNotebookId = currentActiveNotebookId() || workspaceNotebookId() || "";
  const response = await window.fetch(
    `/sidebar?active_notebook_id=${encodeURIComponent(activeNotebookId)}&mode=${encodeURIComponent(mode)}`,
    {
      headers: { Accept: "text/html" },
    }
  );
  if (!response.ok) {
    throw new Error(`Failed to refresh the sidebar: ${response.status}`);
  }

  sidebar.outerHTML = await response.text();
  initializeSidebarSearch();
  initializeNotebookTree();
  initializeSidebarToggle();
  initializeSidebarResizer();
  applyNotebookMetadata();
  await renderLocalWorkspaceSidebarEntries();
  restoreSidebarState(sidebarState);
  syncSelectedIngestionRunbookState();
  restoreSelectedSourceObject();
  renderSidebarSourceOperationStatus();
  renderDataGenerationMonitor();
  renderQueryMonitor();
  renderQueryNotificationMenu();
  renderHomePage();
}

function maybeRefreshSidebarForCompletedGenerationJobs() {
  const newCompletedJobs = dataGenerationJobsSnapshot.filter(
    (job) => job.status === "completed" && !refreshedDataGenerationJobIds.has(job.jobId)
  );
  if (!newCompletedJobs.length) {
    return;
  }

  newCompletedJobs.forEach((job) => refreshedDataGenerationJobIds.add(job.jobId));
  refreshSidebar().catch((error) => {
    console.error("Failed to refresh the sidebar after data generation.", error);
  });
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
  const toggleCountRoots = sidebarQueryCounts();
  const performanceRoot = queryPerformanceSection();
  const performanceStatsRoot = queryPerformanceStats();
  const performanceChartRoot = queryPerformanceChart();
  const performanceDistributionRoot = queryPerformanceDistribution();
  if (!listRoot || !countRoot) {
    return;
  }

  const runningCount = Number(queryJobsSummary.runningCount || 0);
  countRoot.textContent = String(runningCount);
  countRoot.classList.toggle("is-live", runningCount > 0);
  toggleCountRoots.forEach((toggleCountRoot) => {
    toggleCountRoot.textContent = String(runningCount);
    toggleCountRoot.hidden = runningCount === 0;
    toggleCountRoot.classList.toggle("is-live", runningCount > 0);
  });

  if (!queryJobsSnapshot.length) {
    listRoot.innerHTML = '<p class="query-monitor-empty">No query jobs yet.</p>';
  } else {
    listRoot.innerHTML = queryJobsSnapshot.slice(0, 8).map((job) => queryMonitorItemMarkup(job)).join("");
  }

  if (performanceRoot && performanceStatsRoot && performanceChartRoot && performanceDistributionRoot) {
    const hasPerformance = Array.isArray(queryPerformanceState?.recent) && queryPerformanceState.recent.length > 0;
    performanceRoot.hidden = !hasPerformance;
    if (hasPerformance) {
      performanceStatsRoot.innerHTML = queryPerformanceStatsMarkup(queryPerformanceState);
      performanceChartRoot.innerHTML = renderPerformanceChartMarkup(queryPerformanceState);
      performanceDistributionRoot.innerHTML = renderPerformanceDistributionMarkup(queryPerformanceState);
    } else {
      performanceStatsRoot.innerHTML = "";
      performanceChartRoot.innerHTML = "";
      performanceDistributionRoot.innerHTML = "";
    }
  }
}

function renderDataGenerationMonitor() {
  const listRoot = dataGenerationMonitorList();
  const countRoot = dataGenerationMonitorCount();
  const toggleCountRoots = sidebarQueryCounts();
  if (!listRoot || !countRoot) {
    return;
  }

  const visibleJobs = currentWorkspaceMode() === "ingestion"
    ? filteredDataGenerationJobs()
    : dataGenerationJobsSnapshot;
  const runningCount = visibleJobs.filter((job) => dataGenerationJobIsRunning(job)).length;
  countRoot.textContent = String(runningCount);
  countRoot.classList.toggle("is-live", runningCount > 0);

  if (currentWorkspaceMode() === "ingestion") {
    toggleCountRoots.forEach((toggleCountRoot) => {
      toggleCountRoot.textContent = String(runningCount);
      toggleCountRoot.hidden = runningCount === 0;
      toggleCountRoot.classList.toggle("is-live", runningCount > 0);
    });
  }

  if (!visibleJobs.length) {
    listRoot.innerHTML =
      currentWorkspaceMode() === "ingestion"
        ? '<p class="query-monitor-empty">No ingestion jobs for this runbook yet.</p>'
        : '<p class="query-monitor-empty">No ingestion jobs yet.</p>';
    return;
  }

  listRoot.innerHTML = visibleJobs
    .slice(0, 8)
    .map((job) => dataGenerationMonitorItemMarkup(job))
    .join("");
}

function renderQueryNotificationMenu() {
  const menu = queryNotificationMenu();
  const listRoot = queryNotificationList();
  const countRoot = queryNotificationCount();
  const clearButton = notificationClearButton();
  if (!menu || !listRoot || !countRoot) {
    return;
  }

  const visibleNotifications = collectVisibleNotifications();
  const hasRunningActivity =
    Number(queryJobsSummary.runningCount || 0) > 0 || Number(dataGenerationJobsSummary.runningCount || 0) > 0;
  const badgeCount = visibleNotifications.length;
  countRoot.textContent = String(badgeCount);
  countRoot.hidden = badgeCount === 0;
  countRoot.classList.toggle("is-live", hasRunningActivity);
  if (clearButton) {
    clearButton.hidden = !visibleNotifications.length;
  }

  if (!visibleNotifications.length) {
    listRoot.innerHTML = '<p class="topbar-notification-empty">No notifications yet.</p>';
    return;
  }

  listRoot.innerHTML = visibleNotifications
    .slice(0, 12)
    .map((item) => item.markup)
    .join("");
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
  queryJobsLoaded = true;
  queryJobsStateVersion = snapshot?.version ?? null;
  queryJobsSummary = snapshot?.summary ?? { runningCount: 0, totalCount: 0 };
  queryPerformanceState = snapshot?.performance ?? { recent: [], stats: {} };
  const normalizedJobs = Array.isArray(snapshot?.jobs)
    ? snapshot.jobs.map((job) => normalizeQueryJob(job)).filter(Boolean)
    : [];
  queryJobsSnapshot = decorateQueryJobsWithInsights(normalizedJobs).sort(compareQueryJobsByStartedAt);

  pruneDismissedNotificationKeys();
  renderQueryMonitor();
  renderQueryNotificationMenu();
  syncVisibleQueryCells();
  syncQueryClockLoop();
  renderHomePage();
}

function applyDataGenerationJobsState(snapshot) {
  dataGenerationJobsLoaded = true;
  dataGenerationJobsStateVersion = snapshot?.version ?? null;
  dataGenerationJobsSummary = snapshot?.summary ?? { runningCount: 0, totalCount: 0 };
  dataGenerationJobsSnapshot = Array.isArray(snapshot?.jobs)
    ? snapshot.jobs
        .map((job) => normalizeDataGenerationJob(job))
        .filter(Boolean)
        .sort(compareDataGenerationJobsByStartedAt)
    : [];

  pruneDismissedNotificationKeys();
  renderIngestionWorkbench();
  renderDataGenerationMonitor();
  renderQueryNotificationMenu();
  syncDataGenerationClockLoop();
  maybeRefreshSidebarForCompletedGenerationJobs();
  renderHomePage();
}

function clearSourceObjectFieldCacheForRelations(relations = []) {
  if (!Array.isArray(relations) || !relations.length) {
    sourceObjectFieldCache.clear();
    sourceObjectFieldRequests.clear();
    return;
  }

  relations.forEach((relation) => {
    const normalizedRelation = typeof relation === "string" ? relation.trim() : "";
    if (!normalizedRelation) {
      return;
    }
    sourceObjectFieldCache.delete(normalizedRelation);
    sourceObjectFieldRequests.delete(normalizedRelation);
  });
}

function currentWorkspaceCanEdit() {
  return document.querySelector("[data-notebook-meta]")?.dataset.canEdit !== "false";
}

function escapeSelectorValue(value) {
  return typeof window.CSS?.escape === "function" ? window.CSS.escape(String(value ?? "")) : String(value ?? "");
}

function sourceCatalogSelector(sourceId) {
  return `[data-source-catalog-source-id="${escapeSelectorValue(sourceId)}"]`;
}

function sourceCatalogNode(sourceId) {
  return document.querySelector(sourceCatalogSelector(sourceId));
}

function sourceSchemaBucketNode(bucketName) {
  const normalizedBucketName = String(bucketName ?? "").trim();
  if (!normalizedBucketName) {
    return null;
  }
  return document.querySelector(
    `[data-source-schema][data-source-bucket="${escapeSelectorValue(normalizedBucketName)}"]`
  );
}

function localWorkspaceSchemaNode() {
  return document.querySelector(
    `[data-source-schema][data-source-schema-key="${escapeSelectorValue(localWorkspaceSchemaKey)}"]`
  );
}

function localWorkspaceEntryNode(entryId) {
  const normalizedEntryId = String(entryId || "").trim();
  if (!normalizedEntryId) {
    return null;
  }

  return document.querySelector(
    `[data-local-workspace-entry-id="${escapeSelectorValue(normalizedEntryId)}"]`
  );
}

function isLocalWorkspaceSourceObject(sourceObjectRoot) {
  return Boolean(sourceObjectRoot?.dataset.localWorkspaceEntryId?.trim());
}

function formatByteCount(sizeBytes) {
  const normalizedSize = Number(sizeBytes) || 0;
  if (normalizedSize < 1024) {
    return `${normalizedSize} B`;
  }
  if (normalizedSize < 1024 * 1024) {
    return `${(normalizedSize / 1024).toFixed(1)} KB`;
  }
  return `${(normalizedSize / (1024 * 1024)).toFixed(1)} MB`;
}

function ensureLocalWorkspaceCatalogOrder() {
  const sourceTree = document.querySelector(".source-tree");
  const localWorkspaceCatalog = sourceCatalogNode(localWorkspaceCatalogSourceId);
  if (!(sourceTree instanceof Element) || !(localWorkspaceCatalog instanceof Element)) {
    return;
  }

  if (sourceTree.firstElementChild !== localWorkspaceCatalog) {
    sourceTree.prepend(localWorkspaceCatalog);
  }
}

function localWorkspaceSchemaMarkup(entries, open = false) {
  const entriesMarkup = entries
    .map((entry) => {
      const relation = localWorkspaceRelation(entry.id);
      const formatLabel = String(entry.exportFormat || "file").toUpperCase();
      const displayPath = localWorkspaceDisplayPath(entry.folderPath, entry.fileName);
      return `
        <li
          class="source-object source-object-file"
          data-searchable-item="${escapeHtml(entry.fileName)} ${escapeHtml(displayPath)} ${escapeHtml(formatLabel)}"
          data-source-object
          data-source-object-kind="file"
          data-source-object-name="${escapeHtml(entry.fileName)}"
          data-source-object-relation="${escapeHtml(relation)}"
          data-source-option-id="${escapeHtml(localWorkspaceCatalogSourceId)}"
          data-local-workspace-entry-id="${escapeHtml(entry.id)}"
          data-local-workspace-folder-path="${escapeHtml(entry.folderPath)}"
          data-local-workspace-export-format="${escapeHtml(entry.exportFormat)}"
          data-local-workspace-size-bytes="${escapeHtml(entry.sizeBytes)}"
          data-local-workspace-created-at="${escapeHtml(entry.createdAt)}"
          data-local-workspace-column-count="${escapeHtml(entry.columnCount)}"
          data-local-workspace-row-count="${escapeHtml(entry.rowCount)}"
          data-local-workspace-mime-type="${escapeHtml(entry.mimeType)}"
        >
          <span class="source-node-label">
            <svg
              class="source-icon source-icon-object source-icon-object-view"
              viewBox="0 0 16 16"
              aria-hidden="true"
            >
              <rect x="2.4" y="3" width="11.2" height="9.6" rx="1.1"></rect>
              <path d="M4.2 5.2h7.6M4.2 7.7h7.6"></path>
              <path d="M3.2 10.7c1.5-1.7 3.1-2.6 4.8-2.6s3.3.9 4.8 2.6"></path>
              <circle cx="8" cy="10.2" r="1"></circle>
            </svg>
            <span>${escapeHtml(entry.fileName)}</span>
          </span>
          <span class="source-object-meta">
            <small>${escapeHtml(formatLabel)}</small>
            <small title="${escapeHtml(displayPath)}">${escapeHtml(entry.folderPath || "Root")}</small>
            <details class="workspace-action-menu source-action-menu" data-source-action-menu>
              <summary
                class="workspace-action-menu-toggle"
                data-source-action-menu-toggle
                aria-label="Source actions"
                title="Source actions"
              >
                <span class="workspace-action-menu-dots" aria-hidden="true">...</span>
              </summary>
              <div class="workspace-action-menu-panel">
                <button
                  type="button"
                  class="workspace-action-menu-item"
                  data-download-local-workspace-object
                  title="Download the Local Workspace file"
                >
                  Download local file
                </button>
                <div class="workspace-action-menu-separator" aria-hidden="true"></div>
                <button
                  type="button"
                  class="workspace-action-menu-item workspace-action-menu-item-danger"
                  data-delete-local-workspace-object
                  title="Delete the Local Workspace file"
                >
                  Delete local file
                </button>
              </div>
            </details>
          </span>
        </li>
      `;
    })
    .join("");

  return `
    <details
      class="source-schema"
      data-source-schema
      data-source-schema-key="${escapeHtml(localWorkspaceSchemaKey)}"
      ${open ? "open" : ""}
    >
      <summary data-searchable-item="Saved Results Local Workspace IndexedDB">
        <span class="source-node-label">
          <svg class="source-icon source-icon-schema" viewBox="0 0 16 16" aria-hidden="true">
            <rect x="1.6" y="1.8" width="3.7" height="3.7" rx="0.7"></rect>
            <rect x="10.7" y="1.8" width="3.7" height="3.7" rx="0.7"></rect>
            <rect x="10.7" y="10.5" width="3.7" height="3.7" rx="0.7"></rect>
            <path d="M5.3 3.7h2.8a2 2 0 0 1 2 2v1.5"></path>
            <path d="M10.1 8H7.4a2 2 0 0 0-2 2v.5"></path>
          </svg>
          <span>Saved Results</span>
        </span>
        <span class="source-schema-meta">
          <small>${escapeHtml(String(entries.length))} file${entries.length === 1 ? "" : "s"}</small>
        </span>
      </summary>
      <ul class="source-object-list">
        ${entriesMarkup}
      </ul>
    </details>
  `;
}

async function renderLocalWorkspaceSidebarEntries() {
  ensureLocalWorkspaceCatalogOrder();

  const localWorkspaceCatalog = sourceCatalogNode(localWorkspaceCatalogSourceId);
  if (!(localWorkspaceCatalog instanceof Element)) {
    return;
  }

  const entries = await listLocalWorkspaceExports();
  const existingSchema = localWorkspaceSchemaNode();
  const schemaOpen = existingSchema instanceof HTMLDetailsElement ? existingSchema.open : true;

  if (!entries.length) {
    existingSchema?.remove();
    if (activeSourceObjectRelation?.startsWith(localWorkspaceRelationPrefix)) {
      setSelectedSourceObjectState(null);
      renderSourceInspectorMarkup("", true);
    }
    return;
  }

  const markup = localWorkspaceSchemaMarkup(entries, schemaOpen);
  if (existingSchema instanceof Element) {
    existingSchema.outerHTML = markup;
  } else {
    localWorkspaceCatalog.insertAdjacentHTML("beforeend", markup);
  }
}

function syncSourceConnectionControls(catalogNode, status) {
  if (!(catalogNode instanceof Element)) {
    return;
  }

  const meta = catalogNode.querySelector(":scope > summary [data-source-catalog-meta]");
  if (!(meta instanceof Element)) {
    return;
  }

  const state = status?.state || "unknown";
  meta.dataset.sourceState = state;

  meta.querySelectorAll("[data-source-connect], [data-source-disconnect]").forEach((button) => {
    const sourceId = catalogNode.dataset.sourceCatalogSourceId?.trim() || catalogNode.dataset.sourceCatalogName?.trim() || "";
    const sourceLabel = catalogNode.dataset.sourceCatalogName?.trim() || sourceId;
    const isPending = sourceConnectionRequests.has(sourceId);
    button.disabled = isPending;
    button.hidden = false;
    if (button instanceof HTMLButtonElement) {
      const isConnect = button.hasAttribute("data-source-connect");
      button.title = isConnect ? `Connect ${sourceLabel}` : `Disconnect ${sourceLabel}`;
      button.setAttribute("aria-label", button.title);
    }
  });
}

function upsertSourceConnectionStatus(catalogNode, status) {
  if (!(catalogNode instanceof Element)) {
    return;
  }

  const summary = catalogNode.querySelector(":scope > summary");
  if (!(summary instanceof Element)) {
    return;
  }

  const meta =
    summary.querySelector(":scope > [data-source-catalog-meta]") ||
    summary.querySelector(":scope > .source-catalog-meta");
  let badge = meta?.querySelector(":scope > .source-connection-status") || null;
  if (!status?.label) {
    badge?.remove();
    return;
  }

  if (!(badge instanceof Element)) {
    badge = document.createElement("span");
    badge.className = "source-connection-status";
    badge.innerHTML = `
      <svg class="source-connection-icon" viewBox="0 0 16 16" aria-hidden="true">
        <path d="M5.5 4.2H4.2a1.8 1.8 0 0 0 0 3.6h1.3"></path>
        <path d="M10.5 4.2h1.3a1.8 1.8 0 0 1 0 3.6h-1.3"></path>
        <path d="M5.9 8.4l4.2-4.2"></path>
        <path d="M5.9 7.6l4.2 4.2"></path>
      </svg>
    `;
    (meta || summary).appendChild(badge);
  }

  badge.className = `source-connection-status source-connection-status-${status.state || "unknown"}`;
  badge.setAttribute("title", status.detail || status.label);
  badge.setAttribute("aria-label", status.label);
  syncSourceConnectionControls(catalogNode, status);
}

function blinkSourceCatalog(sourceId) {
  const summary = sourceCatalogNode(sourceId)?.querySelector(":scope > summary");
  if (!(summary instanceof Element)) {
    pendingSourceCatalogBlinks.add(sourceId);
    return;
  }

  pendingSourceCatalogBlinks.delete(sourceId);
  summary.classList.remove("is-source-updated");
  void summary.offsetWidth;
  summary.classList.add("is-source-updated");

  window.setTimeout(() => {
    summary.classList.remove("is-source-updated");
  }, 2400);
}

function replayPendingSourceCatalogBlinks() {
  if (!pendingSourceCatalogBlinks.size) {
    return;
  }

  Array.from(pendingSourceCatalogBlinks).forEach((sourceId) => {
    const summary = sourceCatalogNode(sourceId)?.querySelector(":scope > summary");
    if (!(summary instanceof Element)) {
      return;
    }
    blinkSourceCatalog(sourceId);
  });
}

function blinkSourceSchemaBucket(bucketName) {
  const summary = sourceSchemaBucketNode(bucketName)?.querySelector(":scope > summary");
  if (!(summary instanceof Element)) {
    return;
  }

  summary.classList.remove("is-source-updated");
  void summary.offsetWidth;
  summary.classList.add("is-source-updated");
  window.setTimeout(() => {
    summary.classList.remove("is-source-updated");
  }, 2400);
}

async function revealSidebarS3Bucket(bucketName) {
  const normalizedBucketName = String(bucketName ?? "").trim();
  if (!normalizedBucketName) {
    return;
  }

  const sourcesRoot = dataSourcesSection();
  if (sourcesRoot instanceof HTMLDetailsElement) {
    sourcesRoot.open = true;
  }

  const workspaceCatalog = sourceCatalogNode("workspace.s3");
  if (workspaceCatalog instanceof HTMLDetailsElement) {
    workspaceCatalog.open = true;
  }

  const schemaNode = sourceSchemaBucketNode(normalizedBucketName);
  if (!(schemaNode instanceof HTMLDetailsElement)) {
    return;
  }

  schemaNode.open = true;
  blinkSourceCatalog("workspace.s3");
  blinkSourceSchemaBucket(normalizedBucketName);
  schemaNode.scrollIntoView({ block: "nearest" });
}

async function setDataSourceConnectionState(sourceId, action) {
  const normalizedSourceId = String(sourceId ?? "").trim();
  const normalizedAction = String(action ?? "").trim();
  if (!normalizedSourceId || !["connect", "disconnect"].includes(normalizedAction)) {
    return;
  }

  sourceConnectionRequests.add(normalizedSourceId);
  const catalogNode = sourceCatalogNode(normalizedSourceId);
  syncSourceConnectionControls(catalogNode, {
    state: catalogNode?.querySelector("[data-source-catalog-meta]")?.dataset.sourceState || "unknown",
    label: catalogNode?.querySelector(".source-connection-status")?.getAttribute("aria-label") || "",
  });

  try {
    const response = await window.fetch(
      `/api/data-sources/${encodeURIComponent(normalizedSourceId)}/${normalizedAction}`,
      { method: "POST" }
    );
    if (!response.ok) {
      throw new Error(`Failed to ${normalizedAction} ${normalizedSourceId}: ${response.status}`);
    }
    applyDataSourceEventsState(await response.json());
  } catch (error) {
    console.error(`Failed to ${normalizedAction} data source.`, error);
    await showMessageDialog({
      title: "Data source error",
      copy: `Could not ${normalizedAction} ${normalizedSourceId}.`,
    });
  } finally {
    sourceConnectionRequests.delete(normalizedSourceId);
    syncSourceConnectionControls(sourceCatalogNode(normalizedSourceId), {
      state:
        sourceCatalogNode(normalizedSourceId)
          ?.querySelector("[data-source-catalog-meta]")
          ?.dataset.sourceState || "unknown",
      label: sourceCatalogNode(normalizedSourceId)?.querySelector(".source-connection-status")?.getAttribute("aria-label") || "",
    });
  }
}

function applyDataSourceStatusIndicators(snapshot) {
  const statuses = Array.isArray(snapshot?.statuses) ? snapshot.statuses : [];
  const statusMap = new Map(
    statuses
      .filter((status) => typeof status?.sourceId === "string" && status.sourceId.trim())
      .map((status) => [status.sourceId.trim(), status])
  );

  document.querySelectorAll("[data-source-catalog]").forEach((catalogNode) => {
    const sourceId =
      catalogNode.dataset.sourceCatalogSourceId?.trim() || catalogNode.dataset.sourceCatalogName?.trim() || "";
    upsertSourceConnectionStatus(catalogNode, statusMap.get(sourceId) || null);
  });
}

async function refreshDataSourcesSection(mode = currentWorkspaceMode()) {
  const currentSection = dataSourcesSection();
  if (!currentSection) {
    return;
  }

  const sidebarState = captureSidebarState();
  const activeNotebookId = currentActiveNotebookId() || workspaceNotebookId() || "";
  const response = await window.fetch(
    `/sidebar?active_notebook_id=${encodeURIComponent(activeNotebookId)}&mode=${encodeURIComponent(mode)}`,
    {
      headers: { Accept: "text/html" },
    }
  );
  if (!response.ok) {
    throw new Error(`Failed to refresh the data sources section: ${response.status}`);
  }

  const container = document.createElement("div");
  container.innerHTML = await response.text();
  const nextSection = container.querySelector("[data-data-sources-section]");
  if (!(nextSection instanceof Element)) {
    throw new Error("Failed to locate the refreshed data sources section.");
  }

  currentSection.outerHTML = nextSection.outerHTML;
  await renderLocalWorkspaceSidebarEntries();
  restoreSidebarState(sidebarState);
  restoreSelectedSourceObject();
  renderSidebarSourceOperationStatus();
  replayPendingSourceCatalogBlinks();
}

function queueDataSourcesSectionRefresh() {
  if (pendingDataSourceSidebarRefreshHandle !== null) {
    return;
  }

  pendingDataSourceSidebarRefreshHandle = window.setTimeout(() => {
    pendingDataSourceSidebarRefreshHandle = null;

    if (dataSourceSidebarRefreshPromise) {
      dataSourceSidebarRefreshQueued = true;
      return;
    }

    const runRefresh = async () => {
      await refreshDataSourcesSection(currentWorkspaceMode());

      if (currentWorkspaceMode() !== "notebook") {
        return;
      }

      const notebookId = currentActiveNotebookId() || workspaceNotebookId();
      if (!notebookId || currentWorkspaceCanEdit()) {
        return;
      }

      await loadNotebookWorkspace(notebookId);
    };

    dataSourceSidebarRefreshPromise = runRefresh()
      .catch((error) => {
        console.error("Failed to refresh the sidebar after a data source change.", error);
      })
      .finally(() => {
        dataSourceSidebarRefreshPromise = null;
        if (dataSourceSidebarRefreshQueued) {
          dataSourceSidebarRefreshQueued = false;
          queueDataSourcesSectionRefresh();
        }
      });
  }, 120);
}

function applyDataSourceEventsState(snapshot) {
  const previousVersion = dataSourceEventsStateVersion;
  dataSourceEventsStateVersion = snapshot?.version ?? null;
  const latestEvent = Array.isArray(snapshot?.events) ? snapshot.events[0] : null;
  const previousLatestEventId = dataSourceEventsLatestEventId;
  dataSourceEventsLatestEventId = typeof latestEvent?.eventId === "string" ? latestEvent.eventId : null;

  if (previousVersion === null || dataSourceEventsStateVersion === previousVersion) {
    applyDataSourceStatusIndicators(snapshot);
    return;
  }

  applyDataSourceStatusIndicators(snapshot);

  if (!latestEvent || dataSourceEventsLatestEventId === previousLatestEventId) {
    return;
  }

  if (typeof latestEvent?.sourceId === "string" && latestEvent.sourceId.trim()) {
    pendingSourceCatalogBlinks.add(latestEvent.sourceId.trim());
  }
  const touchedRelations = [
    ...(latestEvent?.addedRelations ?? []),
    ...(latestEvent?.removedRelations ?? []),
    ...(latestEvent?.updatedRelations ?? []),
  ];
  if (touchedRelations.length) {
    clearSourceObjectFieldCacheForRelations(touchedRelations);
  }
  queueDataSourcesSectionRefresh();
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

function sourceObjectS3DownloadDescriptor(sourceObjectRoot) {
  if (!(sourceObjectRoot instanceof Element)) {
    return null;
  }

  const downloadable = String(sourceObjectRoot.dataset.s3Downloadable || "").trim().toLowerCase() === "true";
  const bucket = String(sourceObjectRoot.dataset.s3Bucket || "").trim();
  const key = String(sourceObjectRoot.dataset.s3Key || "").trim();
  if (!downloadable || !bucket || !key) {
    return null;
  }

  const path = String(sourceObjectRoot.dataset.s3Path || "").trim();
  const keySegments = key.split("/").filter(Boolean);
  return {
    bucket,
    key,
    path,
    fileName: keySegments[keySegments.length - 1] || sourceObjectDisplayName(sourceObjectRoot),
  };
}

function sourceObjectS3DeleteDescriptor(sourceObjectRoot) {
  const descriptor = sourceObjectS3DownloadDescriptor(sourceObjectRoot);
  if (!descriptor) {
    return null;
  }

  return {
    entryKind: "file",
    name: descriptor.fileName,
    bucket: descriptor.bucket,
    prefix: descriptor.key,
    path: descriptor.path || `s3://${descriptor.bucket}/${descriptor.key}`,
    fileFormat: String(sourceObjectRoot?.dataset.s3FileFormat || "").trim(),
  };
}

function sourceSchemaS3BucketDescriptor(sourceSchemaRoot) {
  if (!(sourceSchemaRoot instanceof Element)) {
    return null;
  }

  const bucket = String(sourceSchemaRoot.dataset.sourceBucket || "").trim();
  if (!bucket) {
    return null;
  }

  return {
    entryKind: "bucket",
    name: bucket,
    bucket,
    prefix: "",
    path: `s3://${bucket}/`,
    fileFormat: "",
  };
}

function downloadSourceS3Object(sourceObjectRoot) {
  const descriptor = sourceObjectS3DownloadDescriptor(sourceObjectRoot);
  if (!descriptor) {
    return false;
  }

  const search = new URLSearchParams({
    bucket: descriptor.bucket,
    key: descriptor.key,
    filename: descriptor.fileName,
  });
  const anchor = document.createElement("a");
  anchor.href = `/api/s3/object/download?${search.toString()}`;
  anchor.download = descriptor.fileName;
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  return true;
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

  if (isLocalWorkspaceSourceObject(sourceObjectRoot)) {
    if (activeSourceObjectRelation === relation) {
      renderSourceInspectorMarkup(localWorkspaceInspectorMarkup(sourceObjectRoot));
    }
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
    <section id="query-results-${escapeHtml(cellId)}" class="result-panel" data-cell-result data-query-job-id="" hidden>
      <header class="result-header">
        <div class="result-header-copy">
          <h3>Result</h3>
          <div class="result-meta-row">
            <p class="result-meta">0 ms</p>
          </div>
        </div>
        <div class="result-header-actions">
          <span class="result-badge">Run this cell to inspect the selected data sources.</span>
          ${resultExportMenuMarkup(false, "")}
        </div>
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
  const storageTooltip = sourceStorageTooltipForIds(selectedSources);
  const summaryTitle = storageTooltip ? ` title="${escapeHtml(storageTooltip)}"` : "";
  const metadataMarkup = selectedSources.length
    ? `
        <span class="cell-source-classification" data-cell-source-classification>${escapeHtml(sourceClassificationDisplayText(selectedSources))}</span>
        <span class="cell-source-computation-mode" data-cell-source-computation-mode title="${escapeHtml(sourceComputationModeTooltipText())}">${escapeHtml(sourceComputationModeDisplayText(selectedSources))}</span>
      `
    : "";

  return `
    <span class="cell-source-summary-label" data-cell-source-summary-label${summaryTitle}>${escapeHtml(cellSourceSummaryText(dataSources))}</span>
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
        const storageTitle = option.storage_tooltip
          ? ` title="${escapeHtml(option.storage_tooltip)}"`
          : "";
        return `
          <label class="workspace-source-option cell-source-option${selected ? " is-selected" : ""}"${storageTitle}>
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
          <textarea name="sql" data-editor-source data-default-sql="${escapeHtml(cell.sql)}" rows="${initialSqlEditorRows}" spellcheck="false">${escapeHtml(cell.sql)}</textarea>
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
      data-created-at="${escapeHtml(metadata.createdAt || new Date().toISOString())}"
      data-shared="${metadata.shared ? "true" : "false"}"
      data-can-edit="true"
      data-can-delete="true"
      data-default-title="${escapeHtml(metadata.title)}"
      data-default-summary="${escapeHtml(metadata.summary)}"
      data-default-created-at="${escapeHtml(metadata.createdAt || new Date().toISOString())}"
      data-linked-generator-id="${escapeHtml(metadata.linkedGeneratorId || "")}"
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
      data-default-shared="${metadata.shared ? "true" : "false"}"
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
            <button
              type="button"
              class="workspace-sharing-toggle${metadata.shared ? " is-on" : ""}"
              data-notebook-shared-toggle
              aria-pressed="${metadata.shared ? "true" : "false"}"
            >
              <span class="workspace-sharing-toggle-switch" aria-hidden="true">
                <span class="workspace-sharing-toggle-thumb"></span>
              </span>
              <span class="workspace-sharing-toggle-copy">
                Shared with all users
                <small>Stores this notebook on the server and announces it to connected users.</small>
              </span>
            </button>
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
  link.href = notebookUrl(notebookId) || "#";
  link.className = "notebook-link notebook-tree-leaf";
  link.dataset.notebookId = notebookId;
  link.dataset.notebookTitle = metadata.title;
  link.dataset.notebookSummary = metadata.summary;
  link.dataset.createdAt = metadata.createdAt || new Date().toISOString();
  link.dataset.notebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookTitle = metadata.title;
  link.dataset.defaultNotebookSummary = metadata.summary;
  link.dataset.defaultNotebookVersions = JSON.stringify(metadata.versions ?? []);
  link.dataset.defaultNotebookCells = JSON.stringify(
    (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      dataSources: normalizeDataSources(cell.dataSources),
      sql: cell.sql,
    }))
  );
  link.dataset.defaultNotebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookTags = metadata.tags.join("||");
  link.dataset.shared = metadata.shared ? "true" : "false";
  link.dataset.defaultNotebookShared = metadata.shared ? "true" : "false";
  link.dataset.canEdit = metadata.canEdit ? "true" : "false";
  link.dataset.canDelete = metadata.canDelete ? "true" : "false";
  link.dataset.draggableNotebook = "";
  link.draggable = Boolean(metadata.canEdit);

  const titleRow = document.createElement("span");
  titleRow.className = "notebook-title-row";

  const title = document.createElement("span");
  title.className = "notebook-title";
  title.textContent = metadata.title;
  titleRow.append(title);

  if (metadata.shared) {
    const sharedBadge = document.createElement("small");
    sharedBadge.className = "notebook-sharing-pill";
    sharedBadge.textContent = "Shared";
    titleRow.append(sharedBadge);
  }

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
  titleRow.append(tools);
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
  if (!defaults.canEdit) {
    const readOnlyMetadata = {
      ...defaults,
      notebookId,
      title: normalizeNotebookTitleValue(defaults.title),
      summary: normalizeNotebookSummaryValue(defaults.summary),
      cells: normalizeNotebookCells(defaults.cells),
      dataSources: notebookSourceIds({ cells: defaults.cells }),
      tags: normalizeTags(defaults.tags),
      sql: defaults.cells[0]?.sql ?? "",
      deleted: false,
      versions: defaults.versions?.length
        ? defaults.versions
        : [createInitialNotebookVersion(notebookId, defaults)],
    };

    updateStoredNotebookState(notebookId, () => ({
      title: readOnlyMetadata.title,
      summary: readOnlyMetadata.summary,
      tags: readOnlyMetadata.tags,
      cells: readOnlyMetadata.cells,
      deleted: false,
      versions: readOnlyMetadata.versions,
      shared: defaults.shared,
    }));

    return readOnlyMetadata;
  }

  const sharedDraftState = defaults.shared ? normalizeStoredNotebookState(sharedNotebookDrafts.get(notebookId)) : {};
  const storedState = defaults.shared
    ? sharedDraftState
    : normalizeStoredNotebookState(readStoredNotebookMetadata()[notebookId]);
  const cells = normalizeNotebookCells(storedState.cells ?? defaults.cells);
  const resolvedTitle = normalizeNotebookTitleValue(storedState.title, defaults.title);
  const resolvedSummary = normalizeNotebookSummaryValue(storedState.summary, defaults.summary);
  const baseMetadata = {
    ...defaults,
    notebookId,
    title: resolvedTitle,
    summary: resolvedSummary,
    createdAt: defaults.createdAt,
    linkedGeneratorId: defaults.linkedGeneratorId,
    cells,
    dataSources: notebookSourceIds({ cells }),
    tags: normalizeTags(storedState.tags ?? defaults.tags),
    sql: cells[0]?.sql ?? "",
    shared: storedState.shared ?? defaults.shared,
    deleted: storedState.deleted ?? defaults.deleted,
  };
  let versionsRepaired = false;
  const versions =
    storedState.versions && storedState.versions.length
      ? storedState.versions.map((version) => {
          const repairedTitle = normalizeNotebookTitleValue(version.title, baseMetadata.title);
          const repairedSummary = normalizeNotebookSummaryValue(version.summary, baseMetadata.summary);
          if (repairedTitle !== version.title || repairedSummary !== version.summary) {
            versionsRepaired = true;
          }
          return {
            ...version,
            title: repairedTitle,
            summary: repairedSummary,
          };
        })
      : [createInitialNotebookVersion(notebookId, baseMetadata)];

  const metadataRepaired =
    resolvedTitle !== (typeof storedState.title === "string" ? storedState.title : resolvedTitle) ||
    resolvedSummary !== (typeof storedState.summary === "string" ? storedState.summary : resolvedSummary);

  if (!storedState.versions || !storedState.versions.length || metadataRepaired || versionsRepaired) {
    updateStoredNotebookState(notebookId, (currentState) => ({
      ...currentState,
      title: normalizeNotebookTitleValue(currentState.title, baseMetadata.title),
      summary: normalizeNotebookSummaryValue(currentState.summary, baseMetadata.summary),
      tags: currentState.tags ?? baseMetadata.tags,
      cells: currentState.cells ?? baseMetadata.cells,
      shared: currentState.shared ?? baseMetadata.shared,
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
  const defaults = readNotebookDefaults(notebookId);
  const usingSharedDrafts = defaults.shared === true || sharedNotebookDrafts.has(notebookId);
  const state = usingSharedDrafts ? null : readStoredNotebookMetadata();
  const currentState = usingSharedDrafts
    ? normalizeStoredNotebookState(sharedNotebookDrafts.get(notebookId))
    : normalizeStoredNotebookState(state?.[notebookId]);
  const nextState = normalizeStoredNotebookState(updater({ ...currentState }));
  if (usingSharedDrafts) {
    sharedNotebookDrafts.set(notebookId, nextState);
  } else if (state) {
    state[notebookId] = nextState;
    writeStoredNotebookMetadata(state);
  }
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

function notebookTreePathForId(notebookId) {
  const link = notebookLinks(notebookId)[0];
  const path = notebookDefaultFolderPath(link);
  return path.length ? path : ["Shared Notebooks"];
}

function sharedNotebookPayload(notebookId) {
  const metadata = notebookMetadata(notebookId);
  return {
    notebookId: isSharedNotebookId(notebookId) ? notebookId : null,
    title: metadata.title,
    summary: metadata.summary,
    tags: normalizeTags(metadata.tags),
    treePath: notebookTreePathForId(notebookId),
    linkedGeneratorId: metadata.linkedGeneratorId || "",
    createdAt: metadata.createdAt || new Date().toISOString(),
    cells: (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      sql: cell.sql,
      dataSources: normalizeDataSources(cell.dataSources),
    })),
    versions: (metadata.versions ?? []).map((version) => ({
      versionId: version.versionId,
      createdAt: version.createdAt,
      title: version.title,
      summary: version.summary,
      tags: normalizeTags(version.tags),
      cells: normalizeNotebookCells(version.cells).map((cell) => ({
        cellId: cell.cellId,
        sql: cell.sql,
        dataSources: normalizeDataSources(cell.dataSources),
      })),
    })),
  };
}

function removeNotebookFromStoredTreeState(notebookId) {
  const currentTree = readStoredNotebookTree();
  if (!currentTree) {
    return;
  }

  const removal = removeNotebookFromStoredTree(currentTree, notebookId);
  if (!removal.changed) {
    return;
  }
  writeStoredNotebookTree(removal.nodes);
}

function insertNotebookIntoStoredTreePath(notebookId, folderPath) {
  const notebookNode = { type: "notebook", notebookId };
  const currentTree = readStoredNotebookTree() ?? [];
  const nextTree = Array.isArray(folderPath) && folderPath.length
    ? insertNotebookIntoStoredFolderPath(currentTree, notebookNode, folderPath)
    : { state: [...currentTree, notebookNode], changed: true };
  writeStoredNotebookTree(nextTree.state);
}

async function syncSharedNotebookNow(notebookId) {
  if (!notebookId || !notebookMetadata(notebookId).shared) {
    return null;
  }

  const response = await window.fetch("/api/notebooks/shared", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Workbench-Client-Id": workbenchClientId(),
    },
    body: JSON.stringify(sharedNotebookPayload(notebookId)),
  });
  if (!response.ok) {
    throw new Error(`Failed to sync shared notebook ${notebookId}: ${response.status}`);
  }

  const payload = await response.json();
  const sharedNotebook = payload?.notebook;
  if (!sharedNotebook?.notebookId) {
    return payload;
  }

  sharedNotebookDrafts.delete(sharedNotebook.notebookId);
  return payload;
}

function scheduleSharedNotebookSync(notebookId, delayMs = 450) {
  if (!notebookId || !notebookMetadata(notebookId).shared) {
    return;
  }

  const existingHandle = sharedNotebookSyncHandles.get(notebookId);
  if (existingHandle) {
    window.clearTimeout(existingHandle);
  }

  const handle = window.setTimeout(() => {
    sharedNotebookSyncHandles.delete(notebookId);
    syncSharedNotebookNow(notebookId).catch((error) => {
      console.error("Failed to sync shared notebook.", error);
    });
  }, delayMs);
  sharedNotebookSyncHandles.set(notebookId, handle);
}

async function shareNotebook(notebookId) {
  const response = await window.fetch("/api/notebooks/shared", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Workbench-Client-Id": workbenchClientId(),
    },
    body: JSON.stringify(sharedNotebookPayload(notebookId)),
  });
  if (!response.ok) {
    throw new Error(`Failed to share notebook ${notebookId}: ${response.status}`);
  }

  const payload = await response.json();
  const sharedNotebookId = payload?.notebook?.notebookId;
  if (!sharedNotebookId) {
    throw new Error("The server did not return a shared notebook identifier.");
  }

  const treePath = notebookTreePathForId(notebookId);
  if (isLocalNotebookId(notebookId)) {
    removeNotebookFromStoredTreeState(notebookId);
    deleteStoredNotebookState(notebookId);
  }

  await refreshSidebar(currentWorkspaceMode());
  await loadNotebookWorkspace(sharedNotebookId);
  pushNotebookHistory(sharedNotebookId);
  revealNotebookLink(sharedNotebookId);
  insertNotebookIntoStoredTreePath(sharedNotebookId, treePath);
  persistNotebookTree();
  return payload;
}

async function unshareNotebook(notebookId) {
  const metadata = notebookMetadata(notebookId);
  const folderPath = notebookTreePathForId(notebookId);
  const localNotebookId = `${localNotebookPrefix}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
  const localMetadata = {
    title: metadata.title,
    summary: metadata.summary,
    tags: normalizeTags(metadata.tags),
    cells: normalizeNotebookCells(metadata.cells),
    canEdit: true,
    canDelete: true,
    shared: false,
    deleted: false,
    versions: (metadata.versions ?? []).map((version) => ({
      versionId: version.versionId,
      createdAt: version.createdAt,
      title: version.title,
      summary: version.summary,
      tags: normalizeTags(version.tags),
      cells: normalizeNotebookCells(version.cells),
    })),
  };

  persistNotebookDraft(localNotebookId, localMetadata);
  insertNotebookIntoStoredTreePath(localNotebookId, folderPath);

  const response = await window.fetch(`/api/notebooks/shared/${encodeURIComponent(notebookId)}`, {
    method: "DELETE",
    headers: {
      Accept: "application/json",
      "X-Workbench-Client-Id": workbenchClientId(),
    },
  });
  if (!response.ok) {
    deleteStoredNotebookState(localNotebookId);
    throw new Error(`Failed to unshare notebook ${notebookId}: ${response.status}`);
  }

  deleteStoredNotebookState(notebookId);
  await refreshSidebar(currentWorkspaceMode());
  await loadNotebookWorkspace(localNotebookId);
  revealNotebookLink(localNotebookId);
  persistNotebookTree();
  return localNotebookId;
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
  link.dataset.shared = metadata.shared ? "true" : "false";
  link.dataset.defaultNotebookShared = metadata.shared ? "true" : "false";
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

  let sharedBadge = link.querySelector(".notebook-sharing-pill");
  if (metadata.shared && !sharedBadge) {
    sharedBadge = document.createElement("small");
    sharedBadge.className = "notebook-sharing-pill";
    sharedBadge.textContent = "Shared";
    titleNode?.after(sharedBadge);
  }
  if (!metadata.shared && sharedBadge) {
    sharedBadge.remove();
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
  recordNotebookActivity(notebookId, "edited");
  scheduleSharedNotebookSync(notebookId);
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
  recordNotebookActivity(notebookId, "edited");
  scheduleSharedNotebookSync(notebookId);
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
  recordNotebookActivity(notebookId, "edited");

  if (options.rerender && isLocalNotebookId(notebookId)) {
    renderLocalNotebookWorkspace(notebookId);
    scheduleSharedNotebookSync(notebookId);
    return metadata;
  }

  applyNotebookMetadata();
  applySidebarSearchFilter();
  scheduleSharedNotebookSync(notebookId);
  return metadata;
}

function setNotebookTags(notebookId, tags) {
  persistNotebookDraft(notebookId, { tags: normalizeTags(tags) });
  applyNotebookMetadata();
  recordNotebookActivity(notebookId, "edited");
  scheduleSharedNotebookSync(notebookId);
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
  recordNotebookActivity(notebookId, "edited");
  scheduleSharedNotebookSync(notebookId);
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
  recordNotebookActivity(notebookId, "edited");
  scheduleSharedNotebookSync(notebookId);
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
  scheduleSharedNotebookSync(notebookId);
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
    scheduleSharedNotebookSync(notebookId);
    return;
  }

  applyNotebookMetadata();
  scheduleSharedNotebookSync(notebookId);
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

function defaultEditorSql(textarea) {
  if (!(textarea instanceof HTMLTextAreaElement)) {
    return "";
  }

  return textarea.defaultValue ?? textarea.dataset.defaultSql ?? "";
}

function editorHeightMetrics(root) {
  const textarea = root.querySelector("[data-editor-source]");
  const editor = editorRegistry.get(root);
  const sizingState = editorSizingState(root);
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
    const minHeight = Math.ceil(lineHeight * initialSqlEditorRows + scrollerPadding + borderHeight);
    const contentHeight = Math.ceil((scroller?.scrollHeight ?? editor.dom.scrollHeight) + borderHeight);
    const maxAutoHeight = Math.ceil(
      lineHeight * (sizingState.interacted ? defaultSqlEditorAutoRows : initialSqlEditorRows) +
      scrollerPadding +
      borderHeight
    );
    return {
      minHeight,
      nextHeight: Math.max(minHeight, Math.min(contentHeight, maxAutoHeight)),
    };
  }

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
  const minHeight = Math.ceil(lineHeight * initialSqlEditorRows + chromeHeight);

  const previousHeight = textarea.style.height;
  textarea.style.height = "auto";
  const contentHeight = Math.ceil(textarea.scrollHeight);
  textarea.style.height = previousHeight;
  const maxAutoHeight = Math.ceil(
    lineHeight * (sizingState.interacted ? defaultSqlEditorAutoRows : initialSqlEditorRows) + chromeHeight
  );

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
      interacted: false,
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
            if (!applyingNotebookState) {
              editorSizingState(root).interacted = true;
            }
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
    window.requestAnimationFrame(() => autosizeEditor(root));
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
  syncShellVisibility();
  if (currentSidebarMode() !== "notebook") {
    refreshSidebar("notebook").catch((error) => {
      console.error("Failed to restore the notebook sidebar.", error);
    });
  }
  renderQueryNotificationMenu();
}

function renderLocalNotebookWorkspace(notebookId) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  const metadata = notebookMetadata(notebookId);
  panel.innerHTML = buildWorkspaceMarkup(notebookId, metadata);
  syncShellVisibility();
  processHtmx(panel);
  initializeEditors(panel);
  applyNotebookMetadata();
  if (currentSidebarMode() !== "notebook") {
    refreshSidebar("notebook")
      .then(() => {
        activateNotebookLink(notebookId);
        revealNotebookLink(notebookId);
      })
      .catch((error) => {
        console.error("Failed to restore the notebook sidebar.", error);
      });
  }
  activateNotebookLink(notebookId);
  revealNotebookLink(notebookId);
  writeLastNotebookId(notebookId);
  syncVisibleQueryCells();
  renderQueryNotificationMenu();
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
  const notebookId = currentWorkspaceNotebookId();
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
  textarea.defaultValue = sqlText;
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
  textarea.defaultValue = formattedSql;

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
  const currentNotebookId = currentWorkspaceNotebookId();
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

function closeSourceActionMenus(exceptMenu = null) {
  document.querySelectorAll("[data-source-action-menu][open]").forEach((menu) => {
    if (menu === exceptMenu) {
      return;
    }
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
  metaRoot.dataset.shared = metadata.shared ? "true" : "false";
  if (workspaceRoot) {
    workspaceRoot.dataset.shared = metadata.shared ? "true" : "false";
  }
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

  const sharedToggle = metaRoot.querySelector("[data-notebook-shared-toggle]");
  if (sharedToggle) {
    sharedToggle.classList.toggle("is-on", metadata.shared === true);
    sharedToggle.setAttribute("aria-pressed", metadata.shared === true ? "true" : "false");
    sharedToggle.disabled = !metadata.canEdit && metadata.shared !== true;
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
    copy: metadata.shared
      ? `Delete shared notebook "${metadata.title}" for all connected users?`
      : `Delete "${metadata.title}" from this browser workspace?`,
    confirmLabel: "Delete notebook",
  });
  if (!confirmed) {
    return;
  }

  if (metadata.shared) {
    const response = await window.fetch(`/api/notebooks/shared/${encodeURIComponent(notebookId)}`, {
      method: "DELETE",
      headers: {
        Accept: "application/json",
        "X-Workbench-Client-Id": workbenchClientId(),
      },
    });
    if (!response.ok) {
      throw new Error(`Failed to delete shared notebook ${notebookId}: ${response.status}`);
    }
    removeNotebookFromStoredTreeState(notebookId);
    deleteStoredNotebookState(notebookId);
    await refreshSidebar(currentWorkspaceMode());
    const fallbackNotebookId = nextVisibleNotebookId(notebookId);
    if (!fallbackNotebookId) {
      renderEmptyWorkspace();
      writeLastNotebookId("");
      return;
    }
    await loadNotebookWorkspace(fallbackNotebookId);
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
    if (!Array.isArray(parsed)) {
      return null;
    }

    const migration = migrateStoredNotebookTree(parsed);
    if (migration.changed) {
      writeStoredNotebookTree(migration.state);
    }

    return migration.state;
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

function createStoredFolderState(name, parentFolderId = "") {
  const folderId = deriveFolderId(name, parentFolderId);
  const permissions = defaultFolderPermissions(folderId);
  return {
    type: "folder",
    name,
    folderId,
    open: true,
    canEdit: permissions.canEdit,
    canDelete: permissions.canDelete,
    children: [],
  };
}

function removeNotebookFromStoredTree(nodes, notebookId) {
  let removed = null;
  let changed = false;
  const nextNodes = [];

  for (const node of Array.isArray(nodes) ? nodes : []) {
    if (!node || typeof node !== "object") {
      continue;
    }

    if (node.type === "notebook" && node.notebookId === notebookId) {
      removed = node;
      changed = true;
      continue;
    }

    if (node.type === "folder" && Array.isArray(node.children)) {
      const childResult = removeNotebookFromStoredTree(node.children, notebookId);
      if (childResult.changed) {
        changed = true;
      }
      if (!removed && childResult.removed) {
        removed = childResult.removed;
      }
      nextNodes.push({
        ...node,
        children: childResult.nodes,
      });
      continue;
    }

    nextNodes.push(node);
  }

  return {
    nodes: nextNodes,
    removed,
    changed,
  };
}

function folderContainsNotebookState(node, notebookId) {
  if (!node || typeof node !== "object") {
    return false;
  }

  if (node.type === "notebook") {
    return node.notebookId === notebookId;
  }

  if (node.type !== "folder" || !Array.isArray(node.children)) {
    return false;
  }

  return node.children.some((child) => folderContainsNotebookState(child, notebookId));
}

function folderMatchesStoredState(node, folderName, parentFolderId = "") {
  if (!node || typeof node !== "object" || node.type !== "folder") {
    return false;
  }

  const folderId = deriveFolderId(folderName, parentFolderId);
  return node.folderId === folderId || node.name === folderName;
}

function findStoredFolderPathState(nodes, folderPath, parentFolderId = "") {
  const normalizedPath = Array.isArray(folderPath)
    ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
    : [];
  if (normalizedPath.length === 0) {
    return null;
  }

  let currentNodes = Array.isArray(nodes) ? nodes : [];
  let currentParentFolderId = parentFolderId;
  let matchedFolder = null;

  for (const folderName of normalizedPath) {
    matchedFolder =
      currentNodes.find((node) => folderMatchesStoredState(node, folderName, currentParentFolderId)) ??
      null;
    if (!matchedFolder) {
      return null;
    }

    currentParentFolderId = deriveFolderId(folderName, currentParentFolderId);
    currentNodes = Array.isArray(matchedFolder.children) ? matchedFolder.children : [];
  }

  return matchedFolder;
}

function insertNotebookIntoStoredFolderPath(
  nodes,
  notebookNode,
  folderPath,
  parentFolderId = ""
) {
  const normalizedNodes = Array.isArray(nodes)
    ? nodes
        .map((node) => (node && typeof node === "object" ? node : null))
        .filter(Boolean)
    : [];
  const normalizedPath = Array.isArray(folderPath)
    ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
    : [];

  if (!notebookNode || normalizedPath.length === 0) {
    return {
      state: normalizedNodes,
      changed: false,
    };
  }

  const [folderName, ...remainingPath] = normalizedPath;
  const nextParentFolderId = deriveFolderId(folderName, parentFolderId);
  const folderIndex = normalizedNodes.findIndex((node) =>
    folderMatchesStoredState(node, folderName, parentFolderId)
  );
  const existingFolder = folderIndex >= 0 ? normalizedNodes[folderIndex] : null;
  const fallbackPolicy = defaultFolderPermissions(nextParentFolderId);

  let changed = folderIndex < 0;
  let folderState =
    existingFolder && existingFolder.type === "folder"
      ? {
          ...existingFolder,
          children: Array.isArray(existingFolder.children) ? [...existingFolder.children] : [],
        }
      : {
          ...createStoredFolderState(folderName, parentFolderId),
          canEdit: fallbackPolicy.canEdit,
          canDelete: fallbackPolicy.canDelete,
        };

  if (!folderState.open) {
    folderState.open = true;
    changed = true;
  }

  if (remainingPath.length > 0) {
    const childResult = insertNotebookIntoStoredFolderPath(
      folderState.children,
      notebookNode,
      remainingPath,
      nextParentFolderId
    );
    folderState.children = childResult.state;
    changed = changed || childResult.changed;
  } else if (!folderContainsNotebookState(folderState, notebookNode.notebookId)) {
    folderState.children = [...folderState.children, notebookNode];
    changed = true;
  }

  const nextNodes = [...normalizedNodes];
  if (folderIndex >= 0) {
    nextNodes[folderIndex] = folderState;
  } else {
    nextNodes.push(folderState);
  }

  return {
    state: nextNodes,
    changed,
  };
}

function ensureNotebookInRootFolderState(nodes, notebookId, folderName) {
  const folderId = deriveFolderId(folderName);
  const rootNodes = Array.isArray(nodes)
    ? nodes
        .map((node) => (node && typeof node === "object" ? node : null))
        .filter(Boolean)
    : [];

  const existingFolderIndex = rootNodes.findIndex(
    (node) => node.type === "folder" && (node.folderId === folderId || node.name === folderName)
  );

  if (
    existingFolderIndex >= 0 &&
    folderContainsNotebookState(rootNodes[existingFolderIndex], notebookId)
  ) {
    return {
      state: rootNodes,
      changed: false,
    };
  }

  const removal = removeNotebookFromStoredTree(rootNodes, notebookId);
  const notebookNode = removal.removed;
  if (!notebookNode) {
    return {
      state: rootNodes,
      changed: false,
    };
  }

  const nextNodes = removal.nodes;
  const targetFolderIndex = nextNodes.findIndex(
    (node) => node.type === "folder" && (node.folderId === folderId || node.name === folderName)
  );

  if (targetFolderIndex >= 0) {
    const targetFolder = nextNodes[targetFolderIndex];
    nextNodes[targetFolderIndex] = {
      ...targetFolder,
      open: true,
      children: [...(Array.isArray(targetFolder.children) ? targetFolder.children : []), notebookNode],
    };
  } else {
    nextNodes.push({
      ...createStoredFolderState(folderName),
      children: [notebookNode],
    });
  }

  return {
    state: nextNodes,
    changed: true,
  };
}

function ensureNotebookInFolderPathState(nodes, notebookId, folderPath) {
  const normalizedPath = Array.isArray(folderPath)
    ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
    : [];
  const rootNodes = Array.isArray(nodes)
    ? nodes
        .map((node) => (node && typeof node === "object" ? node : null))
        .filter(Boolean)
    : [];

  if (normalizedPath.length === 0) {
    return {
      state: rootNodes,
      changed: false,
    };
  }

  const existingFolder = findStoredFolderPathState(rootNodes, normalizedPath);
  if (existingFolder && folderContainsNotebookState(existingFolder, notebookId)) {
    return {
      state: rootNodes,
      changed: false,
    };
  }

  const removal = removeNotebookFromStoredTree(rootNodes, notebookId);
  const notebookNode = removal.removed;
  if (!notebookNode) {
    return {
      state: rootNodes,
      changed: false,
    };
  }

  return insertNotebookIntoStoredFolderPath(removal.nodes, notebookNode, normalizedPath);
}

function collectNotebookIdsFromStoredTree(nodes) {
  const notebookIds = [];

  const visit = (node) => {
    if (!node || typeof node !== "object") {
      return;
    }

    if (node.type === "notebook" && node.notebookId) {
      notebookIds.push(String(node.notebookId));
      return;
    }

    if (node.type === "folder" && Array.isArray(node.children)) {
      node.children.forEach((child) => visit(child));
    }
  };

  (Array.isArray(nodes) ? nodes : []).forEach((node) => visit(node));
  return notebookIds;
}

function migrateStoredNotebookTree(state) {
  let nextState = Array.isArray(state) ? state : [];
  let changed = false;

  const migrations = [
    {
      notebookId: "s3-smoke-test",
      folderPath: ["PoC Tests", "Smoke Tests", "Object Storage"],
    },
    {
      notebookId: "postgres-smoke-test",
      folderPath: ["PoC Tests", "Smoke Tests", "Relational"],
    },
    {
      notebookId: "postgres-oltp-write-test",
      folderPath: ["PoC Tests", "Smoke Tests", "Write Access"],
    },
    {
      notebookId: "postgres-oltp-olap-union-test",
      folderPath: ["PoC Tests", "SQL Functionalities"],
    },
    {
      notebookId: "postgres-oltp-s3-union-test",
      folderPath: ["PoC Tests", "SQL Functionalities"],
    },
    {
      notebookId: "pg-vs-s3-contest-oltp",
      folderPath: ["PoC Tests", "Performance Evaluation", "Single-Table Test"],
    },
    {
      notebookId: "pg-vs-s3-contest-s3",
      folderPath: ["PoC Tests", "Performance Evaluation", "Single-Table Test"],
    },
    {
      notebookId: "pg-vs-s3-contest-pg-native",
      folderPath: ["PoC Tests", "Performance Evaluation", "Single-Table Test"],
    },
    {
      notebookId: "pg-vs-s3-multi-table-oltp",
      folderPath: ["PoC Tests", "Performance Evaluation", "Multi-Table Test"],
    },
    {
      notebookId: "pg-vs-s3-multi-table-s3",
      folderPath: ["PoC Tests", "Performance Evaluation", "Multi-Table Test"],
    },
    {
      notebookId: "pg-vs-s3-multi-table-pg-native",
      folderPath: ["PoC Tests", "Performance Evaluation", "Multi-Table Test"],
    },
  ];

  for (const migration of migrations) {
    const result = ensureNotebookInFolderPathState(nextState, migration.notebookId, migration.folderPath);
    nextState = result.state;
    changed = changed || result.changed;
  }

  const obsoleteRootFolders = new Set(["Smoke Tests"]);
  const obsoleteRootNodes = nextState.filter(
    (node) =>
      node &&
      typeof node === "object" &&
      node.type === "folder" &&
      obsoleteRootFolders.has(String(node.name || "").trim())
  );
  if (obsoleteRootNodes.length) {
    collectNotebookIdsFromStoredTree(obsoleteRootNodes).forEach((notebookId) => {
      if (isLocalNotebookId(notebookId)) {
        deleteStoredNotebookState(notebookId);
      }
    });
    nextState = nextState.filter(
      (node) =>
        !(
          node &&
          typeof node === "object" &&
          node.type === "folder" &&
          obsoleteRootFolders.has(String(node.name || "").trim())
        )
    );
    changed = true;
  }

  return {
    state: nextState,
    changed,
  };
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

function isProtectedNotebookFolderId(folderId = "") {
  const normalizedFolderId = String(folderId ?? "").trim();
  return (
    normalizedFolderId === "poc-tests" ||
    normalizedFolderId.startsWith("poc-tests-") ||
    normalizedFolderId === "smoke-tests" ||
    normalizedFolderId.startsWith("smoke-tests-") ||
    normalizedFolderId === "performance-evaluation" ||
    normalizedFolderId.startsWith("performance-evaluation-")
  );
}

function defaultFolderPermissions(folderId = "") {
  if (isProtectedNotebookFolderId(folderId)) {
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
  createNotebookButton.className = `tree-add-button tree-add-button-inline${canEdit ? "" : " is-action-disabled"}`;
  createNotebookButton.dataset.createNotebook = "";
  createNotebookButton.title = canEdit ? "Create notebook" : "This folder cannot receive new notebooks.";
  createNotebookButton.setAttribute("aria-label", "Create notebook");
  createNotebookButton.disabled = !canEdit;
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
  addButton.className = `tree-add-button tree-add-button-inline${canEdit ? "" : " is-action-disabled"}`;
  addButton.dataset.addTreeItem = "";
  addButton.title = canEdit ? "Add subfolder" : "This folder cannot be changed.";
  addButton.setAttribute("aria-label", "Add subfolder");
  addButton.textContent = "+";
  addButton.disabled = !canEdit;

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

  sharedNotebookDrafts.delete(notebookId);
  removeNotebookFromStoredTreeState(notebookId);
  const pendingSync = sharedNotebookSyncHandles.get(notebookId);
  if (pendingSync) {
    window.clearTimeout(pendingSync);
    sharedNotebookSyncHandles.delete(notebookId);
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
    const notebookEntry = notebookLookup.get(nodeState.notebookId);
    if (!notebookEntry) {
      if (isLocalNotebookId(nodeState.notebookId)) {
        const metadata = notebookMetadata(nodeState.notebookId);
        if (!metadata.deleted) {
          return createNotebookLinkElement(nodeState.notebookId, metadata);
        }
      }
      return null;
    }

    notebookLookup.delete(nodeState.notebookId);
    return notebookEntry.element;
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

function dropTargetAcceptsNotebookDrop(target) {
  if (!(target instanceof Element)) {
    return false;
  }

  const folder = target.closest("[data-tree-folder]");
  return !folder || folderCanEdit(folder);
}

function notebookDefaultFolderPath(notebook) {
  if (!(notebook instanceof Element)) {
    return [];
  }

  const path = [];
  let currentFolder = notebook.closest("[data-tree-folder]");

  while (currentFolder) {
    const label = folderLabel(currentFolder)?.textContent?.trim();
    if (label) {
      path.push(label);
    }
    currentFolder = currentFolder.parentElement?.closest("[data-tree-folder]") ?? null;
  }

  return path.reverse();
}

function ensureTreeFolderPath(root, folderPath) {
  if (!(root instanceof Element)) {
    return null;
  }

  const normalizedPath = Array.isArray(folderPath)
    ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
    : [];
  if (normalizedPath.length === 0) {
    return root;
  }

  let container = root;
  let parentFolderId = "";

  for (const folderName of normalizedPath) {
    const folderId = deriveFolderId(folderName, parentFolderId);
    let folder =
      Array.from(container.children).find(
        (child) =>
          child instanceof Element &&
          child.matches("[data-tree-folder]") &&
          (child.dataset.folderId === folderId || folderLabel(child)?.textContent?.trim() === folderName)
      ) ?? null;

    if (!folder) {
      const permissions = defaultFolderPermissions(folderId);
      folder = createFolderNode(folderName, {
        open: true,
        folderId,
        canEdit: permissions.canEdit,
        canDelete: permissions.canDelete,
      });
      container.appendChild(folder);
    } else {
      folder.open = true;
    }

    container = directChildrenContainer(folder) ?? container;
    parentFolderId = folderId;
  }

  return container;
}

function placeNotebookInDefaultFolder(root, notebook, folderPath) {
  if (!(root instanceof Element) || !(notebook instanceof Element)) {
    return false;
  }

  const targetContainer = ensureTreeFolderPath(root, folderPath);
  if (!(targetContainer instanceof Element)) {
    return false;
  }

  targetContainer.appendChild(notebook);
  return true;
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
        {
          element: notebook,
          defaultFolderPath: notebookDefaultFolderPath(notebook),
        },
      ])
    );
    const fragment = document.createDocumentFragment();

    for (const nodeState of storedTree) {
      const renderedNode = renderStoredTreeNode(nodeState, notebookLookup);
      if (renderedNode) {
        fragment.appendChild(renderedNode);
      }
    }

    treeRoot.replaceChildren(fragment);

    let treeChanged = false;
    for (const notebookEntry of notebookLookup.values()) {
      const placed = placeNotebookInDefaultFolder(
        treeRoot,
        notebookEntry.element,
        notebookEntry.defaultFolderPath
      );
      if (!placed) {
        treeRoot.appendChild(notebookEntry.element);
      }
      treeChanged = true;
    }

    if (treeChanged) {
      persistNotebookTree();
    }
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

  sidebar.querySelectorAll("[data-open-ingestion-runbook]").forEach((button) => {
    button.dataset.searchHidden = matches(button) ? "false" : "true";
  });

  const runbookFolders = Array.from(sidebar.querySelectorAll("[data-runbook-folder]")).reverse();
  for (const folder of runbookFolders) {
    const selfMatches = matches(folder.querySelector(":scope > summary"));
    const visibleChildren = folder.querySelector(
      ":scope > [data-runbook-children] > :not([data-search-hidden='true'])"
    );
    const visible = !term || selfMatches || Boolean(visibleChildren);
    folder.dataset.searchHidden = visible ? "false" : "true";
    if (term && visibleChildren) {
      folder.open = true;
    }
  }

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

  const ingestionRunbookSection = sidebar.querySelector("[data-ingestion-runbook-section]");
  if (term && sidebar.querySelector("[data-open-ingestion-runbook][data-search-hidden='false']")) {
    ingestionRunbookSection?.setAttribute("open", "");
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

async function loadDataGeneratorCatalog() {
  const response = await window.fetch("/api/data-generators", {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to load data generators: ${response.status}`);
  }

  const payload = await response.json();
  dataGeneratorsCatalog = Array.isArray(payload?.generators)
    ? payload.generators.map((generator) => normalizeDataGenerator(generator)).filter(Boolean)
    : [];
  resolveSelectedIngestionRunbookId();
  syncSelectedIngestionRunbookState();
  renderIngestionWorkbench();
}

async function loadDataGenerationJobsState() {
  const response = await window.fetch("/api/data-generation-jobs", {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to load data generation jobs: ${response.status}`);
  }

  applyDataGenerationJobsState(await response.json());
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

async function loadDataSourceEventsState() {
  const response = await window.fetch("/api/data-source-events", {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to load data source events: ${response.status}`);
  }

  applyDataSourceEventsState(await response.json());
}

async function loadNotebookEventsState() {
  const response = await window.fetch("/api/notebooks/state", {
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to load notebook events: ${response.status}`);
  }

  applyNotebookEventsState(await response.json());
}

function applyRealtimeTopicSnapshot(topic, snapshot) {
  switch (topic) {
    case "query-jobs":
      applyQueryJobsState(snapshot);
      break;
    case "data-generation-jobs":
      applyDataGenerationJobsState(snapshot);
      break;
    case "data-source-events":
      applyDataSourceEventsState(snapshot);
      break;
    case "notebook-events":
      applyNotebookEventsState(snapshot);
      break;
    default:
      break;
  }
}

async function applyNotebookEvent(eventPayload) {
  if (!eventPayload || typeof eventPayload !== "object") {
    return;
  }

  if (String(eventPayload.originClientId || "").trim() === workbenchClientId()) {
    return;
  }

  const notebookId = String(eventPayload.notebookId || "").trim();
  if (!notebookId) {
    return;
  }

  sharedNotebookDrafts.delete(notebookId);
  const mode = currentWorkspaceMode();
  const activeNotebookId = currentWorkspaceNotebookId();

  await refreshSidebar(mode);

  if (eventPayload.eventType === "deleted" && activeNotebookId === notebookId) {
    const fallbackNotebookId = visibleNotebookLinks()[0]?.dataset.notebookId ?? "";
    if (fallbackNotebookId) {
      writeLastNotebookId(fallbackNotebookId);
      await loadNotebookWorkspace(fallbackNotebookId);
    } else {
      writeLastNotebookId("");
      renderEmptyWorkspace();
    }
    return;
  }

  if (eventPayload.eventType === "deleted" && readLastNotebookId() === notebookId) {
    writeLastNotebookId(visibleNotebookLinks()[0]?.dataset.notebookId ?? "");
  }

  if (mode === "notebook" && activeNotebookId === notebookId && eventPayload.eventType === "updated") {
    await loadNotebookWorkspace(notebookId);
  }
}

function applyNotebookEventsState(snapshot) {
  notebookEventsStateVersion = Number(snapshot?.version || 0);
  const events = Array.isArray(snapshot?.events) ? snapshot.events : [];
  const unseenEvents = events.filter((event) => {
    const eventId = String(event?.eventId || "").trim();
    if (!eventId || processedNotebookEventIds.has(eventId)) {
      return false;
    }
    processedNotebookEventIds.add(eventId);
    return true;
  });

  while (processedNotebookEventIds.size > 120) {
    const oldestId = processedNotebookEventIds.values().next().value;
    if (!oldestId) {
      break;
    }
    processedNotebookEventIds.delete(oldestId);
  }

  if (!notebookEventsLoaded) {
    notebookEventsLoaded = true;
    return;
  }

  unseenEvents.forEach((eventPayload) => {
    applyNotebookEvent(eventPayload).catch((error) => {
      console.error("Failed to apply notebook event.", error);
    });
  });
}

async function openQueryWorkbench(notebookId = "") {
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }

  if (notebookId) {
    openNotebookNavigation(notebookId);
    await loadNotebookWorkspace(notebookId);
    return;
  }

  await loadQueryWorkbenchEntry();
}

async function openQueryWorkbenchNavigation() {
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }

  const preferredNotebookId = [
    currentActiveNotebookId(),
    readLastNotebookId(),
    visibleNotebookLinks()[0]?.dataset.notebookId ?? "",
  ].find((candidate) => candidate && !notebookMetadata(candidate).deleted);

  if (preferredNotebookId) {
    openNotebookNavigation(preferredNotebookId);
    if (
      currentWorkspaceMode() === "notebook" &&
      currentActiveNotebookId() === preferredNotebookId &&
      !homePageRoot() &&
      !queryWorkbenchEntryPageRoot() &&
      !queryWorkbenchDataSourcesPageRoot()
    ) {
      applyWorkbenchTitle("query");
      return;
    }

    await loadNotebookWorkspace(preferredNotebookId);
    return;
  }

  await loadQueryWorkbenchEntry();
}

async function loadWorkspacePanelPartial(path) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return null;
  }

  const response = await window.fetch(path, {
    headers: {
      Accept: "text/html",
      "HX-Request": "true",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to load ${path}: ${response.status}`);
  }

  panel.innerHTML = await response.text();
  processHtmx(panel);
  initializeEditors(panel);
  applyNotebookMetadata();
  renderQueryNotificationMenu();
  return panel;
}

async function loadQueryWorkbenchEntry({ pushHistory = true } = {}) {
  const panel = await loadWorkspacePanelPartial("/query-workbench");
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("query");
  if (pushHistory) {
    pushQueryWorkbenchHistory();
  }
}

async function loadQueryWorkbenchDataSources(sourceId = "", { pushHistory = true } = {}) {
  const panel = await loadWorkspacePanelPartial(queryWorkbenchDataSourcesUrl(sourceId));
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("data-sources");
  if (pushHistory) {
    pushQueryWorkbenchDataSourcesHistory(sourceId);
  }
}

async function loadHomePage({ pushHistory = true } = {}) {
  const panel = await loadWorkspacePanelPartial("/");
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("home");
  renderHomePage();
  if (pushHistory && window.location.pathname !== "/") {
    window.history.pushState({ mode: "home" }, "", "/");
  }
}

async function openQueryWorkbenchDataSources() {
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }

  await loadQueryWorkbenchDataSources();
}

function ensureRealtimeEventsEventSource() {
  if (realtimeEventsEventSource || typeof window.EventSource !== "function") {
    return;
  }

  const params = new URLSearchParams();
  if (queryJobsStateVersion !== null) {
    params.set("queryJobsVersion", String(queryJobsStateVersion));
  }
  if (dataGenerationJobsStateVersion !== null) {
    params.set("dataGenerationJobsVersion", String(dataGenerationJobsStateVersion));
  }
  if (dataSourceEventsStateVersion !== null) {
    params.set("dataSourceEventsVersion", String(dataSourceEventsStateVersion));
  }
  if (notebookEventsStateVersion !== null) {
    params.set("notebookEventsVersion", String(notebookEventsStateVersion));
  }

  const streamUrl = params.size
    ? `/api/events/stream?${params.toString()}`
    : "/api/events/stream";
  const eventSource = new window.EventSource(streamUrl);
  ["query-jobs", "data-generation-jobs", "data-source-events", "notebook-events"].forEach((topic) => {
    eventSource.addEventListener(topic, (event) => {
      try {
        applyRealtimeTopicSnapshot(topic, JSON.parse(event.data));
      } catch (error) {
        console.error(`Failed to parse realtime event for ${topic}.`, error);
      }
    });
  });
  eventSource.onerror = () => {
    const refreshTasks = [];
    if (queryJobsStateVersion !== null) {
      refreshTasks.push(
        loadQueryJobsState().catch(() => {
          // Ignore transient reconnect issues.
        })
      );
    }
    if (dataGenerationJobsStateVersion !== null) {
      refreshTasks.push(
        loadDataGenerationJobsState().catch(() => {
          // Ignore transient reconnect issues.
        })
      );
    }
    if (dataSourceEventsStateVersion !== null) {
      refreshTasks.push(
        loadDataSourceEventsState().catch(() => {
          // Ignore transient reconnect issues.
        })
      );
    }
    if (notebookEventsStateVersion !== null) {
      refreshTasks.push(
        loadNotebookEventsState().catch(() => {
          // Ignore transient reconnect issues.
        })
      );
    }
    if (refreshTasks.length) {
      Promise.allSettled(refreshTasks);
    }
  };
  realtimeEventsEventSource = eventSource;
}

async function openIngestionWorkbench({ focusJobId = "", focusGeneratorId = "" } = {}) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  openIngestionNavigation();

  const response = await window.fetch("/ingestion-workbench", {
    headers: { "HX-Request": "true" },
  });
  if (!response.ok) {
    throw new Error(`Failed to load the ingestion workbench: ${response.status}`);
  }

  panel.innerHTML = await response.text();
  processHtmx(panel);
  applyWorkbenchTitle("ingestion");
  await Promise.allSettled([loadDataGeneratorCatalog(), loadDataGenerationJobsState()]);
  const focusedJob = focusJobId
    ? dataGenerationJobsSnapshot.find((job) => job.jobId === focusJobId) ?? null
    : null;
  const selectedGeneratorId = selectIngestionRunbook(
    focusGeneratorId || focusedJob?.generatorId || selectedIngestionRunbookId,
    { spotlight: Boolean(focusGeneratorId) }
  );
  renderIngestionWorkbench();
  if (currentSidebarMode() !== "ingestion") {
    await refreshSidebar("ingestion");
  } else {
    syncSelectedIngestionRunbookState();
    renderDataGenerationMonitor();
  }
  openIngestionNavigation(selectedGeneratorId || focusGeneratorId);
  renderQueryNotificationMenu();

  if (focusJobId) {
    const target = panel.querySelector(`[data-data-generation-job-card][data-job-id="${focusJobId}"]`);
    target?.scrollIntoView({ block: "center", behavior: "smooth" });
  }

  if (focusGeneratorId) {
    const target = panel.querySelector(
      `[data-generator-card][data-generator-id="${selectedGeneratorId || focusGeneratorId}"]`
    );
    target?.scrollIntoView({ block: "center", behavior: "smooth" });
  }
}

async function startDataGenerationJob(generatorId, sizeGb) {
  if (!generatorId || !Number.isFinite(Number(sizeGb)) || Number(sizeGb) <= 0) {
    window.alert("Provide a valid generation size in GB.");
    return;
  }

  const formData = new FormData();
  formData.set("generator_id", generatorId);
  formData.set("size_gb", String(sizeGb));

  const response = await window.fetch("/api/data-generation-jobs", {
    method: "POST",
    body: formData,
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let message = "The data generator could not be started.";
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }
    window.alert(message);
    return;
  }

  const snapshot = normalizeDataGenerationJob(await response.json());
  if (!snapshot) {
    return;
  }

  applyDataGenerationJobsState({
    version: dataGenerationJobsStateVersion,
    summary: {
      ...dataGenerationJobsSummary,
      runningCount: Number(dataGenerationJobsSummary.runningCount || 0) + 1,
    },
    jobs: [snapshot, ...dataGenerationJobsSnapshot.filter((job) => job.jobId !== snapshot.jobId)],
  });
}

async function cancelDataGenerationJob(jobId) {
  if (!jobId) {
    return;
  }

  const response = await window.fetch(`/api/data-generation-jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: "POST",
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    return;
  }

  try {
    await loadDataGenerationJobsState();
  } catch (_error) {
    const snapshot = normalizeDataGenerationJob(await response.json());
    if (!snapshot) {
      return;
    }

    applyDataGenerationJobsState({
      version: dataGenerationJobsStateVersion,
      summary: dataGenerationJobsSummary,
      jobs: [snapshot, ...dataGenerationJobsSnapshot.filter((job) => job.jobId !== snapshot.jobId)],
    });
  }
}

async function cleanupDataGenerationJob(jobId) {
  if (!jobId) {
    return;
  }

  const response = await window.fetch(`/api/data-generation-jobs/${encodeURIComponent(jobId)}/cleanup`, {
    method: "POST",
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let message = "The generated data could not be cleaned.";
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }
    await showMessageDialog({
      title: "Data cleanup failed",
      copy: message,
    });
    return;
  }

  try {
    await loadDataGenerationJobsState();
  } catch (_error) {
    const snapshot = normalizeDataGenerationJob(await response.json());
    if (!snapshot) {
      return;
    }

    applyDataGenerationJobsState({
      version: dataGenerationJobsStateVersion,
      summary: dataGenerationJobsSummary,
      jobs: [snapshot, ...dataGenerationJobsSnapshot.filter((job) => job.jobId !== snapshot.jobId)],
    });
  }

  refreshSidebar().catch((error) => {
    console.error("Failed to refresh the sidebar after cleanup.", error);
  });
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

  recordNotebookActivity(notebookId, "run");
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

  try {
    await loadQueryJobsState();
  } catch (_error) {
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
}

function closeResultActionMenus() {
  document.querySelectorAll("[data-result-action-menu][open]").forEach((menu) => {
    menu.removeAttribute("open");
  });
}

function downloadBlobFile(filename, blob) {
  const url = window.URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => {
    window.URL.revokeObjectURL(url);
  }, 1000);
}

function filenameFromContentDisposition(value) {
  const headerValue = String(value || "").trim();
  if (!headerValue) {
    return "";
  }

  const utf8Match = headerValue.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1].trim());
    } catch (_error) {
      return utf8Match[1].trim();
    }
  }

  const quotedMatch = headerValue.match(/filename="([^"]+)"/i);
  if (quotedMatch?.[1]) {
    return quotedMatch[1].trim();
  }

  const simpleMatch = headerValue.match(/filename=([^;]+)/i);
  return simpleMatch?.[1]?.trim() ?? "";
}

function defaultQueryResultExportFilename(job, format) {
  const normalizedFormat = String(format || "").trim().toLowerCase() || "json";
  const baseName = `${job?.notebookTitle || "query"}-${job?.cellId || "cell"}`
    .replace(/[^\w.-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase();
  return `${baseName || "query-result"}.${normalizedFormat}`;
}

async function fetchQueryResultExportBlob(job, exportFormat) {
  const response = await window.fetch(
    `/api/query-jobs/${encodeURIComponent(job.jobId)}/export/download?format=${encodeURIComponent(exportFormat)}`,
    {
      headers: {
        Accept: "application/octet-stream",
      },
    }
  );

  if (!response.ok) {
    throw new Error(
      await responseErrorMessage(response, "The query result could not be exported.")
    );
  }

  const blob = await response.blob();
  const fileName =
    filenameFromContentDisposition(response.headers.get("Content-Disposition")) ||
    defaultQueryResultExportFilename(job, exportFormat);

  return {
    blob,
    fileName,
  };
}

function localWorkspaceEntryIdFromSourceObject(sourceObjectRoot) {
  return String(sourceObjectRoot?.dataset.localWorkspaceEntryId || "").trim();
}

function localWorkspaceInspectorMarkup(sourceObjectRoot) {
  const objectName = sourceObjectDisplayName(sourceObjectRoot);
  const folderPath = normalizeLocalWorkspaceFolderPath(
    String(sourceObjectRoot?.dataset.localWorkspaceFolderPath || "").trim()
  );
  const exportFormat = String(sourceObjectRoot?.dataset.localWorkspaceExportFormat || "file")
    .trim()
    .toUpperCase();
  const createdAt = formatVersionTimestamp(
    String(sourceObjectRoot?.dataset.localWorkspaceCreatedAt || "").trim()
  );
  const sizeLabel = formatByteCount(sourceObjectRoot?.dataset.localWorkspaceSizeBytes);
  const columnCount = Number(sourceObjectRoot?.dataset.localWorkspaceColumnCount || 0) || 0;
  const rowCount = Number(sourceObjectRoot?.dataset.localWorkspaceRowCount || 0) || 0;

  return `
    <header class="sidebar-source-inspector-header">
      <div class="sidebar-source-inspector-copy">
        <h3 class="sidebar-source-inspector-title">${escapeHtml(objectName)}</h3>
        <p class="sidebar-source-inspector-meta">${escapeHtml(exportFormat)} FILE - ${escapeHtml(localWorkspaceDisplayPath(folderPath, objectName))}</p>
      </div>
    </header>
    <div class="sidebar-source-inspector-body">
      <ul class="sidebar-source-field-list">
        <li class="sidebar-source-field">
          <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Folder path</span></span>
          <span class="sidebar-source-field-type">${escapeHtml(folderPath || "Root")}</span>
        </li>
        <li class="sidebar-source-field">
          <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Storage backend</span></span>
          <span class="sidebar-source-field-type">IndexedDB</span>
        </li>
        <li class="sidebar-source-field">
          <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Saved at</span></span>
          <span class="sidebar-source-field-type">${escapeHtml(createdAt)}</span>
        </li>
        <li class="sidebar-source-field">
          <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Size</span></span>
          <span class="sidebar-source-field-type">${escapeHtml(sizeLabel)}</span>
        </li>
        <li class="sidebar-source-field">
          <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Columns</span></span>
          <span class="sidebar-source-field-type">${escapeHtml(String(columnCount))}</span>
        </li>
        <li class="sidebar-source-field">
          <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Rows</span></span>
          <span class="sidebar-source-field-type">${escapeHtml(String(rowCount))}</span>
        </li>
      </ul>
    </div>
  `;
}

async function saveQueryResultExportToLocalWorkspace(job, exportFormat, options = {}) {
  if (!job?.jobId || !job?.columns?.length) {
    return;
  }

  const exported = await fetchQueryResultExportBlob(job, exportFormat);
  const timestamp = new Date().toISOString();
  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(options.folderPath);
  const fileName = String(options.fileName || exported.fileName || "").trim() || exported.fileName;
  const storedEntry = await saveLocalWorkspaceExport({
    id: `local-workspace-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    fileName,
    folderPath: normalizedFolderPath,
    exportFormat: String(exportFormat || "").trim().toLowerCase(),
    mimeType: exported.blob.type,
    sizeBytes: exported.blob.size,
    createdAt: timestamp,
    updatedAt: timestamp,
    notebookTitle: String(job.notebookTitle || "").trim(),
    cellId: String(job.cellId || "").trim(),
    columnCount: Array.isArray(job.columns) ? job.columns.length : 0,
    rowCount: Array.isArray(job.rows) ? job.rows.length : 0,
    blob: exported.blob,
  });

  await renderLocalWorkspaceSidebarEntries();

  const sourcesRoot = dataSourcesSection();
  if (sourcesRoot instanceof HTMLDetailsElement) {
    sourcesRoot.open = true;
  }
  const localWorkspaceCatalog = sourceCatalogNode(localWorkspaceCatalogSourceId);
  if (localWorkspaceCatalog instanceof HTMLDetailsElement) {
    localWorkspaceCatalog.open = true;
  }
  const schemaNode = localWorkspaceSchemaNode();
  if (schemaNode instanceof HTMLDetailsElement) {
    schemaNode.open = true;
  }
  blinkSourceCatalog(localWorkspaceCatalogSourceId);

  const sourceObjectRoot = localWorkspaceEntryNode(storedEntry.id);
  if (sourceObjectRoot instanceof Element) {
    setSelectedSourceObjectState(sourceObjectRoot);
    renderSourceInspectorMarkup(localWorkspaceInspectorMarkup(sourceObjectRoot));
    sourceObjectRoot.scrollIntoView({ block: "nearest" });
  }

  await showMessageDialog({
    title: "Results saved to Local Workspace",
    copy: `${storedEntry.fileName} was saved to ${localWorkspaceDisplayPath(storedEntry.folderPath)} using IndexedDB in this browser.`,
  });
}

async function downloadLocalWorkspaceExportFromSource(sourceObjectRoot) {
  const entryId = localWorkspaceEntryIdFromSourceObject(sourceObjectRoot);
  if (!entryId) {
    return false;
  }

  const entry = await getLocalWorkspaceExport(entryId);
  if (!entry || !(entry.blob instanceof Blob)) {
    return false;
  }

  downloadBlobFile(entry.fileName, entry.blob);
  return true;
}

async function deleteLocalWorkspaceExportFromSource(sourceObjectRoot) {
  const entryId = localWorkspaceEntryIdFromSourceObject(sourceObjectRoot);
  if (!entryId) {
    return false;
  }

  const entry = await getLocalWorkspaceExport(entryId);
  if (!entry) {
    return false;
  }

  const { confirmed } = await showConfirmDialog({
    title: "Delete Local Workspace file",
    copy: `Delete ${entry.fileName} from this browser's Local Workspace?`,
    confirmLabel: "Delete local file",
  });
  if (!confirmed) {
    return true;
  }

  await deleteLocalWorkspaceExport(entryId);
  if (activeSourceObjectRelation === localWorkspaceRelation(entryId)) {
    setSelectedSourceObjectState(null);
    renderSourceInspectorMarkup("", true);
  }
  await renderLocalWorkspaceSidebarEntries();
  return true;
}

function s3ExplorerPath(bucket, prefix = "") {
  const normalizedBucket = String(bucket || "").trim();
  const parts = String(prefix || "")
    .split("/")
    .map((segment) => String(segment || "").trim())
    .filter(Boolean);
  const normalizedPrefix = parts.length ? `${parts.join("/")}/` : "";
  return normalizedBucket ? `s3://${normalizedBucket}/${normalizedPrefix}` : "";
}

function normalizeS3ExplorerEntry(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }

  return {
    entryKind: String(entry.entryKind || "").trim(),
    name: String(entry.name || "").trim(),
    bucket: String(entry.bucket || "").trim(),
    prefix: String(entry.prefix || "").trim(),
    path: String(entry.path || "").trim(),
    fileFormat: String(entry.fileFormat || "").trim(),
    sizeBytes: Number.isFinite(Number(entry.sizeBytes)) ? Number(entry.sizeBytes) : 0,
    hasChildren: entry.hasChildren === true,
    selectable: entry.selectable === true,
  };
}

function normalizeS3ExplorerSnapshot(snapshot) {
  const normalized = snapshot && typeof snapshot === "object" ? snapshot : {};
  return {
    bucket: String(normalized.bucket || "").trim(),
    prefix: String(normalized.prefix || "").trim(),
    path: String(normalized.path || "").trim(),
    entries: Array.isArray(normalized.entries)
      ? normalized.entries.map((entry) => normalizeS3ExplorerEntry(entry)).filter(Boolean)
      : [],
    breadcrumbs: Array.isArray(normalized.breadcrumbs) ? normalized.breadcrumbs : [],
    canCreateBucket: normalized.canCreateBucket !== false,
    canCreateFolder: normalized.canCreateFolder === true,
    emptyMessage: String(normalized.emptyMessage || "").trim(),
  };
}

function resultExportTreeRoot() {
  return resultExportDialog()?.querySelector("[data-s3-explorer-tree]") ?? null;
}

function resultExportBreadcrumbRoot() {
  return resultExportDialog()?.querySelector("[data-s3-explorer-breadcrumbs]") ?? null;
}

function resultExportSelectedPathNode() {
  return resultExportDialog()?.querySelector("[data-result-export-selected-path]") ?? null;
}

function resultExportFileNameInput() {
  return resultExportDialog()?.querySelector("[data-result-export-file-name]") ?? null;
}

function resultExportSubmitButton() {
  return resultExportDialog()?.querySelector("[data-result-export-submit]") ?? null;
}

function closeS3ExplorerActionMenus(exceptMenu = null) {
  document.querySelectorAll("[data-s3-explorer-action-menu][open]").forEach((menu) => {
    if (menu === exceptMenu) {
      return;
    }
    menu.removeAttribute("open");
  });
}

function s3ExplorerEntryRoot(target) {
  return target instanceof Element ? target.closest("[data-s3-explorer-entry]") : null;
}

function s3ExplorerEntryDescriptor(target) {
  const entryRoot = s3ExplorerEntryRoot(target);
  if (!(entryRoot instanceof Element)) {
    return null;
  }

  return {
    entryKind: String(entryRoot.dataset.s3ExplorerKind || "").trim(),
    name: String(entryRoot.dataset.s3ExplorerName || "").trim(),
    bucket: String(entryRoot.dataset.s3ExplorerBucket || "").trim(),
    prefix: String(entryRoot.dataset.s3ExplorerPrefix || "").trim(),
    path: String(entryRoot.dataset.s3ExplorerPath || "").trim(),
    fileFormat: String(entryRoot.dataset.s3ExplorerFileFormat || "").trim(),
  };
}

function s3ExplorerParentPrefix(prefix = "") {
  const parts = String(prefix || "")
    .split("/")
    .map((segment) => String(segment || "").trim())
    .filter(Boolean);
  if (!parts.length) {
    return "";
  }

  parts.pop();
  return parts.length ? `${parts.join("/")}/` : "";
}

function downloadS3ExplorerObject(target) {
  const descriptor = s3ExplorerEntryDescriptor(target);
  if (!descriptor || descriptor.entryKind !== "file" || !descriptor.bucket || !descriptor.prefix) {
    return false;
  }

  const search = new URLSearchParams({
    bucket: descriptor.bucket,
    key: descriptor.prefix,
    filename: descriptor.name || descriptor.prefix.split("/").filter(Boolean).at(-1) || "download",
  });
  const anchor = document.createElement("a");
  anchor.href = `/api/s3/object/download?${search.toString()}`;
  anchor.download =
    descriptor.name || descriptor.prefix.split("/").filter(Boolean).at(-1) || "download";
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  return true;
}

function s3ExplorerDeleteDialogOptions(descriptor) {
  if (!descriptor) {
    return null;
  }

  if (descriptor.entryKind === "bucket") {
    return {
      title: "Delete bucket",
      copy: `Delete bucket "${descriptor.bucket}" and all contained objects and versions from S3?`,
      confirmLabel: "Delete bucket",
      option: {
        label: "Delete this bucket recursively, including every object version and delete marker stored below it.",
        checkedCopy: `Delete bucket "${descriptor.bucket}" recursively? All contained objects, object versions, and delete markers will be removed before the bucket itself is deleted.`,
        checkedConfirmLabel: "Delete bucket recursively",
        required: true,
      },
    };
  }

  if (descriptor.entryKind === "folder") {
    return {
      title: "Delete folder",
      copy: `Delete folder ${descriptor.path || s3ExplorerPath(descriptor.bucket, descriptor.prefix)} and all contained objects, including all object versions?`,
      confirmLabel: "Delete folder",
    };
  }

  if (descriptor.entryKind === "file") {
    return {
      title: "Delete object",
      copy: `Delete object ${descriptor.path || `s3://${descriptor.bucket}/${descriptor.prefix}`} from S3, including all versions if this bucket is versioned?`,
      confirmLabel: "Delete object",
    };
  }

  return null;
}

function s3ExplorerPreferredLocationAfterDelete(descriptor) {
  const selectedBucket = String(resultExportDialogState.selectedBucket || "").trim();
  const selectedPrefix = String(resultExportDialogState.selectedPrefix || "").trim();
  if (!descriptor) {
    return {
      preferredBucket: selectedBucket,
      preferredPrefix: selectedPrefix,
    };
  }

  if (descriptor.entryKind === "bucket") {
    if (selectedBucket && selectedBucket !== descriptor.bucket) {
      return {
        preferredBucket: selectedBucket,
        preferredPrefix: selectedPrefix,
      };
    }
    return {
      preferredBucket: "",
      preferredPrefix: "",
    };
  }

  if (selectedBucket && selectedBucket !== descriptor.bucket) {
    return {
      preferredBucket: selectedBucket,
      preferredPrefix: selectedPrefix,
    };
  }

  const parentPrefix = s3ExplorerParentPrefix(descriptor.prefix);
  if (descriptor.entryKind === "folder") {
    const deletedBranchWasSelected =
      selectedBucket === descriptor.bucket && selectedPrefix.startsWith(descriptor.prefix);
    return {
      preferredBucket: descriptor.bucket,
      preferredPrefix: deletedBranchWasSelected ? parentPrefix : selectedPrefix || parentPrefix,
    };
  }

  return {
    preferredBucket: descriptor.bucket,
    preferredPrefix: selectedPrefix || parentPrefix,
  };
}

async function deleteS3ExplorerEntry(target) {
  const descriptor = s3ExplorerEntryDescriptor(target);
  if (!descriptor) {
    return false;
  }

  return deleteS3EntryDescriptor(descriptor, {
    refreshSidebarAfter: true,
    refreshExplorerAfter: true,
  });
}

async function deleteS3EntryDescriptor(
  descriptor,
  { refreshSidebarAfter = false, refreshExplorerAfter = false, showSidebarStatus = false } = {}
) {
  const dialogOptions = s3ExplorerDeleteDialogOptions(descriptor);
  if (!descriptor || !dialogOptions) {
    return false;
  }

  const confirmation = await showConfirmDialog(dialogOptions);
  if (!confirmation.confirmed) {
    return null;
  }

  if (showSidebarStatus) {
    const deleteTitle =
      descriptor.entryKind === "bucket"
        ? "Deleting bucket"
        : descriptor.entryKind === "folder"
          ? "Deleting folder"
          : "Deleting object";
    const deleteCopy =
      descriptor.entryKind === "bucket"
        ? `Deleting bucket "${descriptor.bucket}" from S3...`
        : descriptor.entryKind === "folder"
          ? `Deleting folder ${descriptor.path || s3ExplorerPath(descriptor.bucket, descriptor.prefix)} from S3...`
          : `Deleting object ${descriptor.path || `s3://${descriptor.bucket}/${descriptor.prefix}`} from S3...`;
    setSidebarSourceOperationStatus({
      tone: "info",
      title: deleteTitle,
      copy: deleteCopy,
    });
  }

  try {
    const result = await fetchJsonOrThrow("/api/s3/explorer/entries", {
      method: "DELETE",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        entryKind: descriptor.entryKind,
        bucket: descriptor.bucket,
        prefix: descriptor.prefix,
      }),
    });

    if (refreshExplorerAfter) {
      const preferredLocation = s3ExplorerPreferredLocationAfterDelete(descriptor);
      await loadS3ExplorerRoot(preferredLocation);
    }
    if (refreshSidebarAfter) {
      await refreshSidebar(currentWorkspaceMode());
      if (descriptor.entryKind === "bucket") {
        blinkSourceCatalog("workspace.s3");
      }
    }
    if (showSidebarStatus) {
      setSidebarSourceOperationStatus(
        {
          tone: "success",
          title:
            descriptor.entryKind === "bucket"
              ? "Bucket deleted"
              : descriptor.entryKind === "folder"
                ? "Folder deleted"
                : "Object deleted",
          copy: String(result?.message || "").trim() || "The selected S3 entry was deleted.",
        },
        { autoClearMs: 6000 }
      );
    }
    return result;
  } catch (error) {
    if (showSidebarStatus) {
      setSidebarSourceOperationStatus(
        {
          tone: "danger",
          title:
            descriptor.entryKind === "bucket"
              ? "Bucket delete failed"
              : descriptor.entryKind === "folder"
                ? "Folder delete failed"
                : "Object delete failed",
          copy:
            error instanceof Error
              ? error.message
              : "The selected S3 entry could not be deleted.",
        },
        { autoClearMs: 8000 }
      );
    }
    throw error;
  }
}

async function createSidebarS3Bucket() {
  const bucketName = await showFolderNameDialog({
    title: "New bucket",
    copy: "Enter the bucket name to create in S3.",
    submitLabel: "Create bucket",
  });
  if (!bucketName) {
    return null;
  }

  const normalizedBucketName = String(bucketName).trim().toLowerCase();
  const confirmation = await showConfirmDialog({
    title: "Create bucket",
    copy: `Create bucket "${normalizedBucketName}" in S3?`,
    confirmLabel: "Create bucket",
    confirmTone: "primary",
  });
  if (!confirmation.confirmed) {
    return null;
  }

  setSidebarSourceOperationStatus({
    tone: "info",
    title: "Creating bucket",
    copy: `Creating bucket "${normalizedBucketName}" in S3...`,
  });

  try {
    const created = await createS3BucketRecord(normalizedBucketName);
    await refreshSidebar(currentWorkspaceMode());
    await revealSidebarS3Bucket(String(created.bucket || normalizedBucketName).trim());
    setSidebarSourceOperationStatus(
      {
        tone: "success",
        title: "Bucket created",
        copy: `Created bucket "${String(created.bucket || normalizedBucketName).trim()}".`,
      },
      { autoClearMs: 6000 }
    );
    return created;
  } catch (error) {
    setSidebarSourceOperationStatus(
      {
        tone: "danger",
        title: "Bucket creation failed",
        copy: error instanceof Error ? error.message : "The S3 bucket could not be created.",
      },
      { autoClearMs: 8000 }
    );
    throw error;
  }
}

function buildS3ExplorerBreadcrumbs(bucket, prefix = "") {
  const breadcrumbs = [{ label: "Buckets", bucket: "", prefix: "", path: "" }];
  const normalizedBucket = String(bucket || "").trim();
  if (!normalizedBucket) {
    return breadcrumbs;
  }

  breadcrumbs.push({
    label: normalizedBucket,
    bucket: normalizedBucket,
    prefix: "",
    path: s3ExplorerPath(normalizedBucket),
  });

  let currentPrefix = "";
  for (const segment of String(prefix || "").split("/").filter(Boolean)) {
    currentPrefix = currentPrefix ? `${currentPrefix}${segment}/` : `${segment}/`;
    breadcrumbs.push({
      label: segment,
      bucket: normalizedBucket,
      prefix: currentPrefix,
      path: s3ExplorerPath(normalizedBucket, currentPrefix),
    });
  }

  return breadcrumbs;
}

function renderS3ExplorerBreadcrumbs(bucket, prefix = "") {
  const root = resultExportBreadcrumbRoot();
  if (!root) {
    return;
  }

  const breadcrumbs = buildS3ExplorerBreadcrumbs(bucket, prefix);
  root.innerHTML = breadcrumbs
    .map((crumb, index) => {
      const isLast = index === breadcrumbs.length - 1;
      if (!crumb.bucket) {
        return `<button type="button" class="result-export-breadcrumb${isLast ? " is-current" : ""}" data-s3-explorer-breadcrumb data-s3-breadcrumb-bucket="" data-s3-breadcrumb-prefix="">${escapeHtml(crumb.label)}</button>`;
      }
      return `
        <button
          type="button"
          class="result-export-breadcrumb${isLast ? " is-current" : ""}"
          data-s3-explorer-breadcrumb
          data-s3-breadcrumb-bucket="${escapeHtml(crumb.bucket)}"
          data-s3-breadcrumb-prefix="${escapeHtml(crumb.prefix)}"
        >${escapeHtml(crumb.label)}</button>
      `;
    })
    .join('<span class="result-export-breadcrumb-separator">/</span>');
}

function s3ExplorerNodeKey(kind, bucket, prefix = "") {
  return `${String(kind || "").trim()}:${String(bucket || "").trim()}:${String(prefix || "").trim()}`;
}

function s3ExplorerActionMenuMarkup(entry) {
  const deleteLabel =
    entry.entryKind === "bucket"
      ? "Delete bucket"
      : entry.entryKind === "folder"
        ? "Delete folder"
        : "Delete object";
  const downloadAction =
    entry.entryKind === "file"
      ? `
        <button
          type="button"
          class="workspace-action-menu-item"
          data-s3-explorer-entry-download
          title="Download this S3 object"
        >
          Download object
        </button>
        <div class="workspace-action-menu-separator" aria-hidden="true"></div>
      `
      : "";

  return `
    <details class="workspace-action-menu s3-explorer-entry-action-menu" data-workspace-action-menu data-s3-explorer-action-menu>
      <summary
        class="workspace-action-menu-toggle"
        data-s3-explorer-action-menu-toggle
        aria-label="S3 entry actions"
        title="S3 entry actions"
      >
        <span class="workspace-action-menu-dots" aria-hidden="true">...</span>
      </summary>
      <div class="workspace-action-menu-panel">
        ${downloadAction}
        <button
          type="button"
          class="workspace-action-menu-item workspace-action-menu-item-danger"
          data-s3-explorer-entry-delete
          title="${escapeHtml(deleteLabel)}"
        >
          ${escapeHtml(deleteLabel)}
        </button>
      </div>
    </details>
  `;
}

function s3ExplorerEntryMarkup(entry) {
  if (entry.entryKind === "file") {
    return `
      <div
        class="s3-explorer-file"
        data-s3-explorer-entry
        data-s3-explorer-file
        data-s3-explorer-kind="${escapeHtml(entry.entryKind)}"
        data-s3-explorer-name="${escapeHtml(entry.name)}"
        data-s3-explorer-bucket="${escapeHtml(entry.bucket)}"
        data-s3-explorer-prefix="${escapeHtml(entry.prefix)}"
        data-s3-explorer-path="${escapeHtml(entry.path)}"
        data-s3-explorer-file-format="${escapeHtml(entry.fileFormat)}"
      >
        <span class="s3-explorer-file-name">${escapeHtml(entry.name)}</span>
        <span class="s3-explorer-entry-tools">
          <span class="s3-explorer-file-meta">${escapeHtml((entry.fileFormat || "file").toUpperCase())}</span>
          ${s3ExplorerActionMenuMarkup(entry)}
        </span>
      </div>
    `;
  }

  const entryLabel = entry.entryKind === "bucket" ? "bucket" : "folder";
  return `
    <details
      class="tree-folder s3-explorer-node"
      data-s3-explorer-entry
      data-s3-explorer-node
      data-s3-explorer-kind="${escapeHtml(entry.entryKind)}"
      data-s3-explorer-name="${escapeHtml(entry.name)}"
      data-s3-explorer-bucket="${escapeHtml(entry.bucket)}"
      data-s3-explorer-prefix="${escapeHtml(entry.prefix)}"
      data-s3-explorer-path="${escapeHtml(entry.path)}"
      data-s3-explorer-node-key="${escapeHtml(s3ExplorerNodeKey(entry.entryKind, entry.bucket, entry.prefix))}"
    >
      <summary class="tree-folder-summary s3-explorer-node-summary" data-searchable-item="${escapeHtml(entry.name)}">
        <span class="tree-folder-label">${escapeHtml(entry.name)}</span>
        <div class="tree-folder-tools s3-explorer-entry-tools">
          <span class="tree-folder-count">${escapeHtml(entryLabel)}</span>
          ${s3ExplorerActionMenuMarkup(entry)}
        </div>
      </summary>
      <div class="tree-children s3-explorer-children" data-s3-explorer-children></div>
    </details>
  `;
}

function s3ExplorerChildrenMarkup(snapshot) {
  if (!snapshot.entries.length) {
    return `<p class="s3-explorer-empty">${escapeHtml(snapshot.emptyMessage || "This location is empty.")}</p>`;
  }
  return snapshot.entries.map((entry) => s3ExplorerEntryMarkup(entry)).join("");
}

function syncResultExportSelectionState() {
  const dialog = resultExportDialog();
  if (!dialog) {
    return;
  }

  renderS3ExplorerBreadcrumbs(resultExportDialogState.selectedBucket, resultExportDialogState.selectedPrefix);

  const selectedPathNode = resultExportSelectedPathNode();
  if (selectedPathNode) {
    selectedPathNode.textContent =
      s3ExplorerPath(resultExportDialogState.selectedBucket, resultExportDialogState.selectedPrefix) ||
      "Select a bucket or folder from the Shared Workspace explorer.";
  }

  const formatCopy = dialog.querySelector("[data-result-export-format-copy]");
  if (formatCopy) {
    formatCopy.textContent = `Format: ${String(resultExportDialogState.exportFormat || "").toUpperCase()}`;
  }

  const fileNameInput = resultExportFileNameInput();
  if (fileNameInput && fileNameInput.value !== resultExportDialogState.fileName) {
    fileNameInput.value = resultExportDialogState.fileName;
  }

  const createFolderButton = dialog.querySelector("[data-s3-create-folder]");
  if (createFolderButton) {
    createFolderButton.disabled = resultExportDialogState.saving || !resultExportDialogState.selectedBucket;
  }

  const submitButton = resultExportSubmitButton();
  if (submitButton) {
    submitButton.disabled =
      resultExportDialogState.saving ||
      !resultExportDialogState.selectedBucket ||
      !String(resultExportDialogState.fileName || "").trim();
    submitButton.textContent = resultExportDialogState.saving
      ? "Saving..."
      : "Save to Shared Workspace";
  }

  dialog.querySelectorAll("[data-s3-explorer-node]").forEach((node) => {
    const selected =
      (node.dataset.s3ExplorerBucket || "") === resultExportDialogState.selectedBucket &&
      (node.dataset.s3ExplorerPrefix || "") === resultExportDialogState.selectedPrefix;
    node.classList.toggle("is-selected", selected);
  });
}

function setResultExportDialogBusy(busy) {
  resultExportDialogState.saving = busy;
  const dialog = resultExportDialog();
  if (dialog) {
    const createBucketButton = dialog.querySelector("[data-s3-create-bucket]");
    if (createBucketButton instanceof HTMLButtonElement) {
      createBucketButton.disabled = busy;
    }

    const fileNameInput = resultExportFileNameInput();
    if (fileNameInput instanceof HTMLInputElement) {
      fileNameInput.disabled = busy;
    }
  }
  syncResultExportSelectionState();
}

function selectResultExportLocation(bucket, prefix = "") {
  resultExportDialogState.selectedBucket = String(bucket || "").trim();
  resultExportDialogState.selectedPrefix = String(prefix || "").trim();
  syncResultExportSelectionState();
}

async function loadS3ExplorerSnapshot(bucket = "", prefix = "") {
  const params = new URLSearchParams();
  if (bucket) {
    params.set("bucket", bucket);
  }
  if (prefix) {
    params.set("prefix", prefix);
  }
  const suffix = params.toString();
  const snapshot = await fetchJsonOrThrow(`/api/s3/explorer${suffix ? `?${suffix}` : ""}`, {
    headers: {
      Accept: "application/json",
    },
  });
  return normalizeS3ExplorerSnapshot(snapshot);
}

function s3ExplorerNodeForLocation(kind, bucket, prefix = "") {
  const normalizedKind = String(kind || "").trim();
  const normalizedBucket = String(bucket || "").trim();
  const normalizedPrefix = String(prefix || "").trim();
  if (!normalizedKind || !normalizedBucket) {
    return null;
  }

  return document.querySelector(
    `[data-s3-explorer-node][data-s3-explorer-kind="${CSS.escape(normalizedKind)}"][data-s3-explorer-bucket="${CSS.escape(
      normalizedBucket
    )}"][data-s3-explorer-prefix="${CSS.escape(normalizedPrefix)}"]`
  );
}

async function loadS3ExplorerNode(node, { force = false } = {}) {
  if (!(node instanceof HTMLElement)) {
    return null;
  }

  const bucket = node.dataset.s3ExplorerBucket || "";
  const prefix = node.dataset.s3ExplorerPrefix || "";
  const requestKey = s3ExplorerNodeKey(node.dataset.s3ExplorerKind, bucket, prefix);
  if (node.dataset.s3ExplorerLoaded === "true" && !force) {
    return null;
  }
  if (s3ExplorerNodeRequests.has(requestKey)) {
    return s3ExplorerNodeRequests.get(requestKey);
  }

  const childrenRoot = node.querySelector("[data-s3-explorer-children]");
  if (childrenRoot) {
    childrenRoot.innerHTML = '<p class="s3-explorer-empty">Loading...</p>';
  }

  const request = loadS3ExplorerSnapshot(bucket, prefix)
    .then((snapshot) => {
      if (childrenRoot) {
        childrenRoot.innerHTML = s3ExplorerChildrenMarkup(snapshot);
      }
      node.dataset.s3ExplorerLoaded = "true";
      syncResultExportSelectionState();
      return snapshot;
    })
    .finally(() => {
      s3ExplorerNodeRequests.delete(requestKey);
    });

  s3ExplorerNodeRequests.set(requestKey, request);
  return request;
}

async function loadS3ExplorerRoot({ preferredBucket = "", preferredPrefix = "" } = {}) {
  const treeRoot = resultExportTreeRoot();
  if (!treeRoot) {
    return null;
  }

  treeRoot.innerHTML = '<p class="s3-explorer-empty">Loading buckets...</p>';
  const snapshot = await loadS3ExplorerSnapshot("", "");
  treeRoot.innerHTML = s3ExplorerChildrenMarkup(snapshot);

  if (preferredBucket) {
    const revealed = await revealS3ExplorerLocation(preferredBucket, preferredPrefix);
    if (!revealed) {
      selectResultExportLocation("", "");
    }
  } else if (snapshot.entries.length === 1 && snapshot.entries[0].entryKind === "bucket") {
    await revealS3ExplorerLocation(snapshot.entries[0].bucket, "");
  } else {
    selectResultExportLocation("", "");
  }

  return snapshot;
}

async function createS3BucketRecord(bucketName) {
  return fetchJsonOrThrow("/api/s3/explorer/buckets", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ bucketName }),
  });
}

async function revealS3ExplorerLocation(bucket, prefix = "") {
  const normalizedBucket = String(bucket || "").trim();
  const normalizedPrefix = String(prefix || "").trim();
  if (!normalizedBucket) {
    selectResultExportLocation("", "");
    return true;
  }

  const bucketNode = s3ExplorerNodeForLocation("bucket", normalizedBucket, "");
  if (!(bucketNode instanceof HTMLElement)) {
    return false;
  }

  selectResultExportLocation(normalizedBucket, "");
  bucketNode.open = true;
  await loadS3ExplorerNode(bucketNode);

  if (!normalizedPrefix) {
    syncResultExportSelectionState();
    return true;
  }

  let currentNode = bucketNode;
  let currentPrefix = "";
  let fullyRevealed = true;
  for (const segment of normalizedPrefix.split("/").filter(Boolean)) {
    currentPrefix = currentPrefix ? `${currentPrefix}${segment}/` : `${segment}/`;
    currentNode.open = true;
    await loadS3ExplorerNode(currentNode);
    const nextNode =
      currentNode.querySelector(
        `[data-s3-explorer-node][data-s3-explorer-kind="folder"][data-s3-explorer-bucket="${CSS.escape(
          normalizedBucket
        )}"][data-s3-explorer-prefix="${CSS.escape(currentPrefix)}"]`
      ) ?? null;
    if (!(nextNode instanceof HTMLElement)) {
      fullyRevealed = false;
      break;
    }
    currentNode = nextNode;
    selectResultExportLocation(normalizedBucket, currentPrefix);
  }

  syncResultExportSelectionState();
  return fullyRevealed;
}

async function createS3ExplorerBucket() {
  const bucketName = await showFolderNameDialog({
    title: "New bucket",
    copy: "Enter the bucket name to create in S3.",
    submitLabel: "Create bucket",
  });
  if (!bucketName) {
    return;
  }

  const normalizedBucketName = String(bucketName).trim().toLowerCase();
  const confirmation = await showConfirmDialog({
    title: "Create bucket",
    copy: `Create bucket "${normalizedBucketName}" in S3?`,
    confirmLabel: "Create bucket",
    confirmTone: "primary",
  });
  if (!confirmation.confirmed) {
    return;
  }

  const created = await createS3BucketRecord(normalizedBucketName);
  await loadS3ExplorerRoot({ preferredBucket: String(created.bucket || "").trim(), preferredPrefix: "" });
  await refreshSidebar(currentWorkspaceMode());
}

async function createS3ExplorerFolder() {
  if (!resultExportDialogState.selectedBucket) {
    return;
  }

  const folderName = await showFolderNameDialog({
    title: "New folder",
    copy: `Create a folder under ${s3ExplorerPath(
      resultExportDialogState.selectedBucket,
      resultExportDialogState.selectedPrefix
    )}.`,
    submitLabel: "Create folder",
  });
  if (!folderName) {
    return;
  }

  const created = await fetchJsonOrThrow("/api/s3/explorer/folders", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      bucket: resultExportDialogState.selectedBucket,
      prefix: resultExportDialogState.selectedPrefix,
      folderName,
    }),
  });

  const selectedNode =
    s3ExplorerNodeForLocation(
      resultExportDialogState.selectedPrefix ? "folder" : "bucket",
      resultExportDialogState.selectedBucket,
      resultExportDialogState.selectedPrefix
    ) ?? null;
  if (selectedNode instanceof HTMLElement) {
    selectedNode.open = true;
    await loadS3ExplorerNode(selectedNode, { force: true });
  } else {
    await loadS3ExplorerRoot({
      preferredBucket: resultExportDialogState.selectedBucket,
      preferredPrefix: resultExportDialogState.selectedPrefix,
    });
  }
  await revealS3ExplorerLocation(String(created.bucket || "").trim(), String(created.prefix || "").trim());
}

async function saveResultExportToS3() {
  const dialog = resultExportDialog();
  if (!dialog || !resultExportDialogState.jobId || !resultExportDialogState.selectedBucket) {
    return;
  }

  setResultExportDialogBusy(true);
  try {
    const payload = await fetchJsonOrThrow(
      `/api/query-jobs/${encodeURIComponent(resultExportDialogState.jobId)}/export/s3`,
      {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          format: resultExportDialogState.exportFormat,
          bucket: resultExportDialogState.selectedBucket,
          prefix: resultExportDialogState.selectedPrefix,
          fileName: String(resultExportDialogState.fileName || "").trim(),
        }),
      }
    );
    closeDialog(dialog, "confirm");
    await showMessageDialog({
      title: "Results saved to Shared Workspace",
      copy: payload?.path
        ? `Saved the exported result file to ${payload.path}.`
        : String(payload?.message || "Saved the exported result file to Shared Workspace."),
    });
  } finally {
    setResultExportDialogBusy(false);
  }
}

async function openResultExportDialog(job, exportFormat) {
  if (!job?.jobId || !job?.columns?.length) {
    return;
  }

  const dialog = ensureResultExportDialog();
  resultExportDialogState.jobId = job.jobId;
  resultExportDialogState.exportFormat = String(exportFormat || "").trim().toLowerCase();
  resultExportDialogState.fileName = defaultQueryResultExportFilename(job, resultExportDialogState.exportFormat);
  resultExportDialogState.saving = false;

  const titleNode = dialog.querySelector("[data-result-export-title]");
  const copyNode = dialog.querySelector("[data-result-export-copy]");
  if (titleNode) {
    titleNode.textContent = `Save Results in ${resultExportDialogState.exportFormat.toUpperCase()} Format to Shared Workspace`;
  }
  if (copyNode) {
    copyNode.textContent =
      "Choose a Shared Workspace bucket or folder, create new locations if needed, and provide the file name to save.";
  }

  syncResultExportSelectionState();
  dialog.showModal();
  await loadS3ExplorerRoot({
    preferredBucket: resultExportDialogState.selectedBucket,
    preferredPrefix: resultExportDialogState.selectedPrefix,
  });
}

async function downloadQueryResultExport(job, exportFormat) {
  if (!job?.jobId || !job?.columns?.length) {
    return;
  }

  const downloadUrl =
    `/api/query-jobs/${encodeURIComponent(job.jobId)}/export/download?format=${encodeURIComponent(exportFormat)}`;
  const anchor = document.createElement("a");
  anchor.href = downloadUrl;
  anchor.download = defaultQueryResultExportFilename(job, exportFormat);
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
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
  syncShellVisibility();
  applyWorkbenchTitle("query");
  if (panel.querySelector(`[data-notebook-meta][data-notebook-id="${CSS.escape(notebookId)}"][data-shared="true"]`)) {
    sharedNotebookDrafts.delete(notebookId);
  }
  processHtmx(panel);
  initializeEditors(panel);
  applyNotebookMetadata();
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }
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
    const resultExportForm = event.target.closest("[data-result-export-form]");
    if (resultExportForm) {
      event.preventDefault();
      try {
        await saveResultExportToS3();
      } catch (error) {
        console.error("Failed to save the exported query result to Shared Workspace.", error);
        await showMessageDialog({
          title: "Result export failed",
          copy: error instanceof Error ? error.message : "The query result could not be saved to Shared Workspace.",
        });
      }
      return;
    }

    const localWorkspaceSaveForm = event.target.closest("[data-local-workspace-save-form]");
    if (localWorkspaceSaveForm) {
      event.preventDefault();
      const job = queryJobById(localWorkspaceSaveDialogState.jobId);
      if (!job) {
        await showMessageDialog({
          title: "Local Workspace save unavailable",
          copy: "Run the cell again so the current query result can be saved to Local Workspace.",
        });
        return;
      }

      try {
        setLocalWorkspaceSaveDialogBusy(true);
        await saveQueryResultExportToLocalWorkspace(job, localWorkspaceSaveDialogState.exportFormat, {
          fileName: localWorkspaceSaveDialogState.fileName,
          folderPath: localWorkspaceSaveDialogState.folderPath,
        });
        closeDialog(localWorkspaceSaveDialog(), "confirm");
      } catch (error) {
        console.error("Failed to save the query result to Local Workspace.", error);
        await showMessageDialog({
          title: "Local Workspace save failed",
          copy: error instanceof Error ? error.message : "The query result could not be saved to Local Workspace.",
        });
      } finally {
        setLocalWorkspaceSaveDialogBusy(false);
      }
      return;
    }

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
  closePopupMenusForTarget(event.target);

  const modalCancelButton = event.target.closest("[data-modal-cancel]");
  if (modalCancelButton) {
    event.preventDefault();
    closeDialog(modalCancelButton.closest("dialog"), "cancel");
    return;
  }

  const sourceActionMenu = event.target.closest("[data-source-action-menu]");
  if (sourceActionMenu) {
    syncSourceActionMenu(sourceActionMenu);
  }

  const sourceActionMenuToggle = event.target.closest("[data-source-action-menu-toggle]");
  if (sourceActionMenuToggle) {
    event.preventDefault();
    event.stopPropagation();
    const menu = sourceActionMenuToggle.closest("[data-source-action-menu]");
    if (menu instanceof HTMLDetailsElement) {
      const nextOpen = !menu.open;
      closeSourceActionMenus(nextOpen ? menu : null);
      menu.open = nextOpen;
      syncSourceActionMenu(menu);
    }
    return;
  }

  const runCellButton = event.target.closest("[data-run-cell]");
  if (runCellButton) {
    event.preventDefault();
    const form = runCellButton.closest("[data-query-form]");
    if (!form) {
      return;
    }
    await startQueryJobForForm(form);
    return;
  }

  const sidebarToggleButton = event.target.closest("[data-sidebar-toggle]");
  if (sidebarToggleButton) {
    event.preventDefault();
    const collapsed = !document.body.classList.contains("sidebar-collapsed");
    applySidebarCollapsedState(collapsed);
    writeSidebarCollapsed(collapsed);
    return;
  }

  const openIngestionWorkbenchButton = event.target.closest("[data-open-ingestion-workbench]");
  if (openIngestionWorkbenchButton) {
    event.preventDefault();
    event.stopPropagation();
    queryNotificationMenu()?.removeAttribute("open");
    await openIngestionWorkbench({
      focusJobId: openIngestionWorkbenchButton.dataset.focusGenerationJob || "",
    });
    return;
  }

  const openQueryWorkbenchButton = event.target.closest("[data-open-query-workbench]");
  if (openQueryWorkbenchButton) {
    event.preventDefault();
    event.stopPropagation();
    queryNotificationMenu()?.removeAttribute("open");
    if (openQueryWorkbenchButton.dataset.openQueryWorkbenchNavigation === "true") {
      await openQueryWorkbenchNavigation();
    } else {
      await openQueryWorkbench(openQueryWorkbenchButton.dataset.openRecentNotebook || "");
    }
    return;
  }

  const openQueryDataSourcesButton = event.target.closest("[data-open-query-data-sources]");
  if (openQueryDataSourcesButton) {
    event.preventDefault();
    event.stopPropagation();
    queryNotificationMenu()?.removeAttribute("open");
    await openQueryWorkbenchDataSources();
    return;
  }

  const openQueryDataSourceButton = event.target.closest("[data-open-query-data-source]");
  if (openQueryDataSourceButton) {
    event.preventDefault();
    event.stopPropagation();
    queryNotificationMenu()?.removeAttribute("open");
    await loadQueryWorkbenchDataSources(openQueryDataSourceButton.dataset.openQueryDataSource || "");
    return;
  }

  const openQueryWorkbenchEntryButton = event.target.closest("[data-open-query-workbench-entry]");
  if (openQueryWorkbenchEntryButton) {
    event.preventDefault();
    event.stopPropagation();
    queryNotificationMenu()?.removeAttribute("open");
    await loadQueryWorkbenchEntry();
    return;
  }

  const openRecentNotebookButton = event.target.closest("[data-open-recent-notebook]");
  if (openRecentNotebookButton) {
    event.preventDefault();
    event.stopPropagation();
    queryNotificationMenu()?.removeAttribute("open");
    await openQueryWorkbench(openRecentNotebookButton.dataset.openRecentNotebook || "");
    return;
  }

  const openIngestionRunbookButton = event.target.closest("[data-open-ingestion-runbook]");
  if (openIngestionRunbookButton) {
    event.preventDefault();
    const generatorId = openIngestionRunbookButton.dataset.openIngestionRunbook || "";
    selectIngestionRunbook(generatorId, { spotlight: true });
    await openIngestionWorkbench({
      focusGeneratorId: generatorId,
    });
    return;
  }

  const clearNotificationsButton = event.target.closest("[data-clear-notifications]");
  if (clearNotificationsButton) {
    event.preventDefault();
    clearVisibleNotifications();
    return;
  }

  const clearLocalWorkspaceButton = event.target.closest("[data-clear-local-workspace]");
  if (clearLocalWorkspaceButton) {
    event.preventDefault();
    event.stopPropagation();
    closeSettingsMenus();
    await promptClearLocalWorkspace();
    return;
  }

  const openAboutButton = event.target.closest("[data-open-about]");
  if (openAboutButton) {
    event.preventDefault();
    event.stopPropagation();
    closeSettingsMenus();
    await showAboutDialog();
    return;
  }

  const openFeatureListButton = event.target.closest("[data-open-feature-list]");
  if (openFeatureListButton) {
    event.preventDefault();
    event.stopPropagation();
    closeSettingsMenus();
    await showFeatureListDialog();
    return;
  }

  const createNotebookButton = event.target.closest("[data-create-notebook]");
  if (createNotebookButton) {
    event.preventDefault();
    event.stopPropagation();
    if (!notebookTreeRoot()) {
      await refreshSidebar("notebook");
    }

    const target = resolveNotebookCreateTarget(createNotebookButton);
    createNotebook(target);
    return;
  }

  const createSourceBucketButton = event.target.closest("[data-create-source-bucket]");
  if (createSourceBucketButton) {
    event.preventDefault();
    event.stopPropagation();
    closeSourceActionMenus();
    try {
      await createSidebarS3Bucket();
    } catch (error) {
      console.error("Failed to create the sidebar S3 bucket.", error);
      await showMessageDialog({
        title: "S3 bucket creation failed",
        copy: error instanceof Error ? error.message : "The S3 bucket could not be created.",
      });
    }
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

  const cancelDataGenerationButton = event.target.closest("[data-cancel-data-generation-job]");
  if (cancelDataGenerationButton) {
    event.preventDefault();
    await cancelDataGenerationJob(cancelDataGenerationButton.dataset.cancelDataGenerationJob || "");
    return;
  }

  const cleanupDataGenerationButton = event.target.closest("[data-cleanup-data-generation-job]");
  if (cleanupDataGenerationButton) {
    event.preventDefault();
    const jobCard = cleanupDataGenerationButton.closest("[data-data-generation-job-card]");
    const jobTitle =
      jobCard?.querySelector(".ingestion-job-copy h4")?.textContent?.trim() || "Generated data";
    const { confirmed } = await showConfirmDialog({
      title: "Clean loader data",
      copy: `Clean the generated loader data for ${jobTitle}? This keeps the target structure but removes the loaded data.`,
      confirmLabel: "Clean loader data",
    });
    if (!confirmed) {
      return;
    }
    await cleanupDataGenerationJob(cleanupDataGenerationButton.dataset.cleanupDataGenerationJob || "");
    return;
  }

  const startDataGenerationButton = event.target.closest("[data-start-data-generation]");
  if (startDataGenerationButton) {
    event.preventDefault();
    const generatorCard = startDataGenerationButton.closest("[data-generator-card]");
    const sizeInput = generatorCard?.querySelector("[data-ingestion-size-input]");
    const requestedSize = Number(sizeInput?.value ?? 0);
    await startDataGenerationJob(
      startDataGenerationButton.dataset.startDataGeneration || "",
      requestedSize
    );
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

  const downloadResultExportButton = event.target.closest("[data-result-export-download]");
  if (downloadResultExportButton) {
    event.preventDefault();
    closeResultActionMenus();
    const job = queryJobForResultActionTarget(downloadResultExportButton);
    if (!job) {
      await showMessageDialog({
        title: "Result export unavailable",
        copy: "Run the cell again so the current query result can be exported.",
      });
      return;
    }
    try {
      await downloadQueryResultExport(job, downloadResultExportButton.dataset.resultExportDownload || "");
    } catch (error) {
      console.error("Failed to download the query result export.", error);
      await showMessageDialog({
        title: "Result export failed",
        copy: error instanceof Error ? error.message : "The query result could not be downloaded.",
      });
    }
    return;
  }

  const saveResultExportButton = event.target.closest("[data-result-export-s3]");
  if (saveResultExportButton) {
    event.preventDefault();
    closeResultActionMenus();
    const job = queryJobForResultActionTarget(saveResultExportButton);
    if (!job) {
      await showMessageDialog({
        title: "Result export unavailable",
        copy: "Run the cell again so the current query result can be saved to Shared Workspace.",
      });
      return;
    }
    try {
      await openResultExportDialog(job, saveResultExportButton.dataset.resultExportS3 || "");
    } catch (error) {
      console.error("Failed to open the result export dialog.", error);
      await showMessageDialog({
        title: "Result export failed",
        copy: error instanceof Error ? error.message : "The query result export dialog could not be opened.",
      });
    }
    return;
  }

  const saveLocalResultExportButton = event.target.closest("[data-result-export-local]");
  if (saveLocalResultExportButton) {
    event.preventDefault();
    closeResultActionMenus();
    const job = queryJobForResultActionTarget(saveLocalResultExportButton);
    if (!job) {
      await showMessageDialog({
        title: "Result export unavailable",
        copy: "Run the cell again so the current query result can be saved to Local Workspace.",
      });
      return;
    }
    try {
      await openLocalWorkspaceSaveDialog(
        job,
        saveLocalResultExportButton.dataset.resultExportLocal || ""
      );
    } catch (error) {
      console.error("Failed to open the Local Workspace save dialog.", error);
      await showMessageDialog({
        title: "Local Workspace save unavailable",
        copy: error instanceof Error ? error.message : "The Local Workspace save dialog could not be opened.",
      });
    }
    return;
  }

  const createLocalWorkspaceFolderButton = event.target.closest(
    "[data-local-workspace-create-folder]"
  );
  if (createLocalWorkspaceFolderButton) {
    event.preventDefault();
    try {
      await createLocalWorkspaceFolderFromDialog();
    } catch (error) {
      console.error("Failed to create a Local Workspace folder.", error);
      await showMessageDialog({
        title: "Local Workspace folder error",
        copy: error instanceof Error ? error.message : "The Local Workspace folder could not be created.",
      });
    }
    return;
  }

  const localWorkspaceFolderOptionButton = event.target.closest(
    "[data-local-workspace-folder-option]"
  );
  if (localWorkspaceFolderOptionButton) {
    event.preventDefault();
    localWorkspaceSaveDialogState.folderPath = normalizeLocalWorkspaceFolderPath(
      localWorkspaceFolderOptionButton.dataset.localWorkspaceFolderPath || ""
    );
    syncLocalWorkspaceSaveDialogState();
    return;
  }

  const localWorkspaceBreadcrumbButton = event.target.closest(
    "[data-local-workspace-breadcrumb]"
  );
  if (localWorkspaceBreadcrumbButton) {
    event.preventDefault();
    localWorkspaceSaveDialogState.folderPath = normalizeLocalWorkspaceFolderPath(
      localWorkspaceBreadcrumbButton.dataset.localWorkspaceFolderPath || ""
    );
    syncLocalWorkspaceSaveDialogState();
    return;
  }

  const createS3BucketButton = event.target.closest("[data-s3-create-bucket]");
  if (createS3BucketButton) {
    event.preventDefault();
    try {
      await createS3ExplorerBucket();
    } catch (error) {
      console.error("Failed to create the S3 bucket.", error);
      await showMessageDialog({
        title: "S3 location error",
        copy: error instanceof Error ? error.message : "The bucket could not be created.",
      });
    }
    return;
  }

  const createS3FolderButton = event.target.closest("[data-s3-create-folder]");
  if (createS3FolderButton) {
    event.preventDefault();
    try {
      await createS3ExplorerFolder();
    } catch (error) {
      console.error("Failed to create the S3 folder.", error);
      await showMessageDialog({
        title: "S3 location error",
        copy: error instanceof Error ? error.message : "The folder could not be created.",
      });
    }
    return;
  }

  const s3ExplorerActionMenuToggle = event.target.closest("[data-s3-explorer-action-menu-toggle]");
  if (s3ExplorerActionMenuToggle) {
    event.preventDefault();
    event.stopPropagation();
    const menu = s3ExplorerActionMenuToggle.closest("[data-s3-explorer-action-menu]");
    if (menu instanceof HTMLDetailsElement) {
      const nextOpen = !menu.open;
      closeS3ExplorerActionMenus(nextOpen ? menu : null);
      menu.open = nextOpen;
    }
    return;
  }

  const s3ExplorerEntryDownloadButton = event.target.closest("[data-s3-explorer-entry-download]");
  if (s3ExplorerEntryDownloadButton) {
    event.preventDefault();
    event.stopPropagation();
    closeS3ExplorerActionMenus();
    const downloaded = downloadS3ExplorerObject(s3ExplorerEntryDownloadButton);
    if (downloaded === false) {
      await showMessageDialog({
        title: "S3 download unavailable",
        copy: "This S3 entry does not point to a single downloadable object.",
      });
    }
    return;
  }

  const s3ExplorerEntryDeleteButton = event.target.closest("[data-s3-explorer-entry-delete]");
  if (s3ExplorerEntryDeleteButton) {
    event.preventDefault();
    event.stopPropagation();
    closeS3ExplorerActionMenus();
    try {
      await deleteS3ExplorerEntry(s3ExplorerEntryDeleteButton);
    } catch (error) {
      console.error("Failed to delete the S3 explorer entry.", error);
      await showMessageDialog({
        title: "S3 delete failed",
        copy: error instanceof Error ? error.message : "The selected S3 entry could not be deleted.",
      });
    }
    return;
  }

  const s3ExplorerBreadcrumbButton = event.target.closest("[data-s3-explorer-breadcrumb]");
  if (s3ExplorerBreadcrumbButton) {
    event.preventDefault();
    try {
      await revealS3ExplorerLocation(
        s3ExplorerBreadcrumbButton.dataset.s3BreadcrumbBucket || "",
        s3ExplorerBreadcrumbButton.dataset.s3BreadcrumbPrefix || ""
      );
    } catch (error) {
      console.error("Failed to navigate the S3 explorer.", error);
      await showMessageDialog({
        title: "S3 explorer error",
        copy: error instanceof Error ? error.message : "The selected S3 location could not be opened.",
      });
    }
    return;
  }

  const s3ExplorerNodeSummary = event.target.closest(".s3-explorer-node-summary");
  if (s3ExplorerNodeSummary) {
    const node = s3ExplorerNodeSummary.closest("[data-s3-explorer-node]");
    if (node instanceof HTMLElement) {
      selectResultExportLocation(node.dataset.s3ExplorerBucket || "", node.dataset.s3ExplorerPrefix || "");
      window.setTimeout(() => {
        if (!node.open) {
          return;
        }
        loadS3ExplorerNode(node).catch(async (error) => {
          console.error("Failed to expand the S3 explorer node.", error);
          await showMessageDialog({
            title: "S3 explorer error",
            copy: error instanceof Error ? error.message : "The S3 location could not be loaded.",
          });
        });
      }, 0);
    }
    return;
  }

  const collapseTreeButton = event.target.closest("[data-collapse-tree]");
  if (collapseTreeButton) {
    event.preventDefault();
    event.stopPropagation();
    setNotebookTreeExpanded(false);
    return;
  }

  const collapseRunbooksButton = event.target.closest("[data-collapse-runbooks]");
  if (collapseRunbooksButton) {
    event.preventDefault();
    event.stopPropagation();
    setRunbookTreeExpanded(false);
    return;
  }

  const expandTreeButton = event.target.closest("[data-expand-tree]");
  if (expandTreeButton) {
    event.preventDefault();
    event.stopPropagation();
    setNotebookTreeExpanded(true);
    return;
  }

  const expandRunbooksButton = event.target.closest("[data-expand-runbooks]");
  if (expandRunbooksButton) {
    event.preventDefault();
    event.stopPropagation();
    setRunbookTreeExpanded(true);
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

  const sourceConnectButton = event.target.closest("[data-source-connect]");
  if (sourceConnectButton) {
    event.preventDefault();
    event.stopPropagation();
    await setDataSourceConnectionState(sourceConnectButton.dataset.sourceConnect, "connect");
    return;
  }

  const sourceDisconnectButton = event.target.closest("[data-source-disconnect]");
  if (sourceDisconnectButton) {
    event.preventDefault();
    event.stopPropagation();
    await setDataSourceConnectionState(sourceDisconnectButton.dataset.sourceDisconnect, "disconnect");
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

  const downloadSourceS3ObjectButton = event.target.closest("[data-download-source-s3-object]");
  if (downloadSourceS3ObjectButton) {
    event.preventDefault();
    closeSourceActionMenus();

    const downloaded = downloadSourceS3Object(downloadSourceS3ObjectButton.closest("[data-source-object]"));
    if (downloaded === false) {
      await showMessageDialog({
        title: "S3 download unavailable",
        copy: "This source object does not point to a single downloadable S3 object.",
      });
    }
    return;
  }

  const downloadLocalWorkspaceObjectButton = event.target.closest(
    "[data-download-local-workspace-object]"
  );
  if (downloadLocalWorkspaceObjectButton) {
    event.preventDefault();
    closeSourceActionMenus();

    const downloaded = await downloadLocalWorkspaceExportFromSource(
      downloadLocalWorkspaceObjectButton.closest("[data-source-object]")
    );
    if (downloaded === false) {
      await showMessageDialog({
        title: "Local Workspace download unavailable",
        copy: "This Local Workspace file could not be downloaded from browser storage.",
      });
    }
    return;
  }

  const deleteSourceS3ObjectButton = event.target.closest("[data-delete-source-s3-object]");
  if (deleteSourceS3ObjectButton) {
    event.preventDefault();
    event.stopPropagation();
    closeSourceActionMenus();

    const descriptor = sourceObjectS3DeleteDescriptor(
      deleteSourceS3ObjectButton.closest("[data-source-object]")
    );
    if (!descriptor) {
      await showMessageDialog({
        title: "S3 delete unavailable",
        copy: "This source object does not point to a single deletable S3 object.",
      });
      return;
    }

    try {
      await deleteS3EntryDescriptor(descriptor, { refreshSidebarAfter: true, showSidebarStatus: true });
    } catch (error) {
      console.error("Failed to delete the sidebar S3 object.", error);
      await showMessageDialog({
        title: "S3 delete failed",
        copy: error instanceof Error ? error.message : "The selected S3 object could not be deleted.",
      });
    }
    return;
  }

  const deleteLocalWorkspaceObjectButton = event.target.closest(
    "[data-delete-local-workspace-object]"
  );
  if (deleteLocalWorkspaceObjectButton) {
    event.preventDefault();
    event.stopPropagation();
    closeSourceActionMenus();

    const deleted = await deleteLocalWorkspaceExportFromSource(
      deleteLocalWorkspaceObjectButton.closest("[data-source-object]")
    );
    if (deleted === false) {
      await showMessageDialog({
        title: "Local Workspace delete unavailable",
        copy: "This Local Workspace file could not be deleted from browser storage.",
      });
    }
    return;
  }

  const deleteSourceS3BucketButton = event.target.closest("[data-delete-source-s3-bucket]");
  if (deleteSourceS3BucketButton) {
    event.preventDefault();
    event.stopPropagation();
    closeSourceActionMenus();

    const descriptor = sourceSchemaS3BucketDescriptor(
      deleteSourceS3BucketButton.closest("[data-source-schema]")
    );
    if (!descriptor) {
      await showMessageDialog({
        title: "Bucket delete unavailable",
        copy: "This source entry does not point to a deletable S3 bucket.",
      });
      return;
    }

    try {
      await deleteS3EntryDescriptor(descriptor, { refreshSidebarAfter: true, showSidebarStatus: true });
    } catch (error) {
      console.error("Failed to delete the sidebar S3 bucket.", error);
      await showMessageDialog({
        title: "Bucket delete failed",
        copy: error instanceof Error ? error.message : "The selected bucket could not be deleted.",
      });
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
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }

    event.preventDefault();
    restoreController?.abort();
    restoreController = null;
    await loadNotebookWorkspace(link.dataset.notebookId);
    pushNotebookHistory(link.dataset.notebookId);
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

  const resultExportFileName = event.target.closest("[data-result-export-file-name]");
  if (resultExportFileName) {
    resultExportDialogState.fileName = resultExportFileName.value;
    syncResultExportSelectionState();
    return;
  }

  const localWorkspaceFolderPathInput = event.target.closest("[data-local-workspace-folder-path]");
  if (localWorkspaceFolderPathInput) {
    localWorkspaceSaveDialogState.folderPath = normalizeLocalWorkspaceFolderPath(
      localWorkspaceFolderPathInput.value
    );
    syncLocalWorkspaceSaveDialogState();
    return;
  }

  const localWorkspaceFileNameInput = event.target.closest("[data-local-workspace-file-name]");
  if (localWorkspaceFileNameInput) {
    localWorkspaceSaveDialogState.fileName = localWorkspaceFileNameInput.value;
    syncLocalWorkspaceSaveDialogState();
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

document.body.addEventListener("click", (event) => {
  const sharedToggle = event.target.closest("[data-notebook-shared-toggle]");
  if (sharedToggle) {
    const notebookId = workspaceNotebookId(sharedToggle.closest("[data-workspace-notebook]"));
    if (!notebookId) {
      return;
    }

    const nextSharedState = sharedToggle.getAttribute("aria-pressed") !== "true";
    sharedToggle.classList.toggle("is-on", nextSharedState);
    sharedToggle.setAttribute("aria-pressed", nextSharedState ? "true" : "false");
    sharedToggle.disabled = true;
    const action = nextSharedState ? shareNotebook(notebookId) : unshareNotebook(notebookId);
    action.catch(async (error) => {
      console.error("Failed to toggle shared notebook state.", error);
      sharedToggle.classList.toggle("is-on", !nextSharedState);
      sharedToggle.setAttribute("aria-pressed", !nextSharedState ? "true" : "false");
      await showMessageDialog({
        title: "Notebook sharing failed",
        copy: "The notebook could not be updated for shared access.",
      });
    }).finally(() => {
      sharedToggle.disabled = false;
    });
    return;
  }
});

document.body.addEventListener("pointerover", (event) => {
  if (event.pointerType === "touch") {
    return;
  }

  closePopupMenusForTarget(event.target, event);
});

document.addEventListener("mouseout", (event) => {
  if (event.relatedTarget !== null) {
    return;
  }

  closePopupMenusForTarget(null);
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
  if (!notebook || notebook.dataset.canEdit === "false") {
    event.preventDefault();
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
  if (!dropTarget || !dropTargetAcceptsNotebookDrop(dropTarget)) {
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
  if (!dropTarget || !dropTargetAcceptsNotebookDrop(dropTarget)) {
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
  initializeSidebarResizer();
  renderLocalWorkspaceSidebarEntries().catch((error) => {
    console.error("Failed to render Local Workspace entries after a partial swap.", error);
  });
  syncShellVisibility();
  applyWorkbenchTitle();
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

window.addEventListener("popstate", async () => {
  if (window.location.pathname === "/query-workbench/data-sources") {
    try {
      await loadQueryWorkbenchDataSources(
        new URLSearchParams(window.location.search).get("source_id") || "",
        { pushHistory: false }
      );
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore managed data sources from browser history.", error);
      }
    }
    return;
  }

  if (window.location.pathname === "/query-workbench") {
    try {
      await loadQueryWorkbenchEntry({ pushHistory: false });
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore query workbench from browser history.", error);
      }
    }
    return;
  }

  if (window.location.pathname === "/") {
    try {
      await loadHomePage({ pushHistory: false });
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore the welcome page from browser history.", error);
      }
    }
    return;
  }

  if (window.location.pathname.startsWith("/notebooks/")) {
    const notebookId = decodeURIComponent(window.location.pathname.slice("/notebooks/".length));
    if (!notebookId) {
      return;
    }

    try {
      await loadNotebookWorkspace(notebookId);
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore notebook from browser history.", error);
      }
    }
  }
});

initializeEditors();
initializeSidebarSearch();
initializeNotebookTree();
initializeSidebarToggle();
initializeSidebarResizer();
renderLocalWorkspaceSidebarEntries().catch((error) => {
  console.error("Failed to render Local Workspace entries during startup.", error);
});
syncShellVisibility();
applyWorkbenchTitle();
applyNotebookMetadata();
restoreSelectedSourceObject();
const initialWorkspaceMode = currentWorkspaceMode();
const initialLoadTasks = [
  loadQueryJobsState().catch((error) => {
    console.error("Failed to load query jobs.", error);
  }),
  loadDataGenerationJobsState().catch((error) => {
    console.error("Failed to load data generation jobs.", error);
  }),
  loadDataSourceEventsState().catch((error) => {
    console.error("Failed to load data source events.", error);
  }),
  loadNotebookEventsState().catch((error) => {
    console.error("Failed to load notebook events.", error);
  }),
];

if (initialWorkspaceMode === "ingestion") {
  initialLoadTasks.push(
    loadDataGeneratorCatalog().catch((error) => {
      console.error("Failed to load data generators.", error);
    })
  );
}

Promise.allSettled(initialLoadTasks)
  .finally(() => {
    ensureRealtimeEventsEventSource();
    refreshSidebar(initialWorkspaceMode).catch((error) => {
      console.error("Failed to refresh the sidebar during startup.", error);
    });

    if (initialWorkspaceMode === "ingestion") {
      renderIngestionWorkbench();
      renderDataGenerationMonitor();
      renderQueryNotificationMenu();
      return;
    }

    if (homePageRoot()) {
      const notebookSectionRoot = notebookSection();
      if (notebookSectionRoot) {
        notebookSectionRoot.open = false;
      }

      const dataSourcesRoot = dataSourcesSection();
      if (dataSourcesRoot) {
        dataSourcesRoot.open = false;
      }

      const queryMonitorSectionRoot = document.querySelector("[data-query-monitor-section]");
      if (queryMonitorSectionRoot) {
        queryMonitorSectionRoot.open = false;
      }

      renderHomePage();
      renderQueryNotificationMenu();
      return;
    }

    const currentNotebookId = currentWorkspaceNotebookId();
    if (window.location.pathname.startsWith("/notebooks/") && currentNotebookId) {
      activateNotebookLink(currentNotebookId);
      revealNotebookLink(currentNotebookId);
      writeLastNotebookId(currentNotebookId);
      renderQueryNotificationMenu();
      return;
    }

    restoreLastNotebook();
  });

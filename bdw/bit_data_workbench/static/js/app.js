import { EditorView, basicSetup } from "../vendor/codemirror.bundle.mjs";
import { sql, PostgreSQL } from "../vendor/lang-sql.bundle.mjs";
import {
  ensureAboutDialog,
  ensureFeatureListDialog,
  ensureResultDownloadDialog,
  ensureResultExportDialog,
  localWorkspaceMoveDialog,
  localWorkspaceSaveDialog,
  resultDownloadDialog,
  resultExportDialog,
} from "./dialogs.js";
import {
  closeDialog,
  showConfirmDialog,
  showFolderNameDialog,
  showMessageDialog,
} from "./dialog-manager.js";
import { createIngestionController } from "./ingestion-controller.js";
import { createIngestionUi } from "./ingestion-ui.js";
import { createHomeUi } from "./home-ui.js";
import { createCsvIngestionController } from "./ingestion-types/csv/index.js";
import { createEditorAutosizeManager } from "./editor-autosize-manager.js";
import { createLocalWorkspaceDialogController } from "./local-workspace-dialog-controller.js";
import { createLocalWorkspaceExportManager } from "./local-workspace-export-manager.js";
import { createLocalWorkspacePathUtils } from "./local-workspace-path-utils.js";
import { createLocalWorkspaceQueryBridge } from "./local-workspace-query-bridge.js";
import { createLocalWorkspacePickerUi } from "./local-workspace-picker.js";
import { createLocalWorkspaceSidebarUi } from "./local-workspace-sidebar.js";
import {
  ensureResultExportFileNameExtension,
  normalizeResultExportFormat,
} from "./data-exporters/export-format-definitions.js";
import {
  defaultResultExportSettings,
  normalizeResultExportSettings,
  readResultExportSettings,
  renderResultExportSettings,
} from "./data-exporters/export-settings.js";
import { createNotebookModel } from "./notebook-model.js";
import { createNotebookWorkspaceMarkup } from "./notebook-workspace-markup.js";
import { createNotebookWorkspaceController } from "./notebook-workspace-controller.js";
import { createNotebookUrlHelpers } from "./notebook-url-helpers.js";
import { createNotebookTreeController } from "./notebook-tree-controller.js";
import { createNotebookTreeState } from "./notebook-tree-state.js";
import { createNotebookTreeUi } from "./notebook-tree-ui.js";
import { createPopupMenuManager } from "./popup-menu-manager.js";
import {
  dataGenerationMonitorCount,
  dataGenerationMonitorList,
  dataSourceNodes,
  dataSourcesSection,
  homePageRoot,
  homeRecentIngestionsRoot,
  homeRecentNotebooksRoot,
  ingestionGeneratorList,
  ingestionGeneratorSectionCopy,
  ingestionGeneratorSectionTitle,
  ingestionJobList,
  ingestionJobSectionCopy,
  ingestionJobSectionTitle,
  ingestionRunbookSection,
  notebookFolders,
  notebookSection,
  notebookTreeRoot,
  notificationClearButton,
  queryMonitorCount,
  queryMonitorList,
  queryNotificationCount,
  queryNotificationList,
  queryNotificationMenu,
  queryPerformanceChart,
  queryPerformanceDistribution,
  queryPerformanceSection,
  queryPerformanceStats,
  queryWorkbenchDataSourcesPageRoot,
  queryWorkbenchEntryPageRoot,
  runbookFolders,
  serviceConsumptionPageRoot,
  settingsMenu,
  shellRoot,
  sidebarQueryCounts,
  sidebarToggles,
  sourceInspector,
  sourceInspectorPanel,
  sourceObjectNodes,
} from "./dom-query-helpers.js";
import { createSourceInspectorController } from "./source-inspector-controller.js";
import { createSourceInspectorUi } from "./source-inspector-ui.js";
import { createQueryInsights } from "./query-insights.js";
import { createQueryUi } from "./query-ui.js";
import {
  applyOptimisticQueryJobSnapshot,
  compareQueryJobsByCompletedAt,
  createQueryJobState,
  formatQueryDuration,
  loadQueryJobsState as requestQueryJobsState,
  normalizeQueryJob,
  queryJobElapsedMs,
  queryJobIsRunning,
  queryJobStatusCopy,
} from "./query-job-state.js";
import { createS3ExplorerLoader, s3ExplorerPath } from "./s3-explorer-loader.js";
import { createRealtimeController } from "./realtime-controller.js";
import { createServiceConsumptionUi } from "./service-consumption-ui.js?v=2026-04-17-service-breakdown-5";
import { createSidebarLayoutManager } from "./sidebar-layout-manager.js";
import { createSidebarRefreshController } from "./sidebar-refresh-controller.js";
import { createSidebarSearchFilter } from "./sidebar-search-filter.js";
import { createWorkspaceScrollManager } from "./workspace-scroll-manager.js";
import {
  accessModeForDataSources,
  accessModeHintForDataSources,
  normalizeDataSources,
  normalizeSourceObjectFields,
  parseDefaultDataSources,
  readSourceOptions,
  sourceClassificationDisplayText,
  sourceComputationModeDisplayText,
  sourceComputationModeTooltipText,
  sourceIdFromLegacyTargetLabel,
  sourceLabelsForIds,
  sourceObjectDisplayKind,
  sourceObjectDisplayName,
  sourceObjectS3DeleteDescriptor,
  sourceObjectS3DownloadDescriptor,
  sourceQueryDescriptor,
  sourceQuerySql,
  sourceSchemaS3BucketDescriptor,
  sourceStorageTooltipForIds,
} from "./source-metadata-utils.js";
import { createSourceQueryActions } from "./source-query-actions.js";
import { createSourceSidebarClickController } from "./source-sidebar-click-controller.js";
import { createSourceTreeController } from "./source-tree-controller.js";
import { formatSqlText } from "./sql-formatter.js";
import { createWorkbenchNavigationController } from "./workbench-navigation-controller.js";
import { createWorkbenchStorage } from "./workbench-storage.js";

const editorRegistry = new WeakMap();
const editorSizingRegistry = new WeakMap();
let draggedNotebook = null;
let restoreController = null;
let applyingNotebookState = false;
let activeCellId = null;
let queryJobsStateVersion = null;
let queryJobsSnapshot = [];
let queryJobsSummary = { runningCount: 0, totalCount: 0 };
let queryPerformanceState = { recent: [], stats: {} };
let realtimeEventsEventSource = null;
let serviceConsumptionStateVersion = null;
let clientConnectionsStateVersion = 0;
let clientConnectionsCount = 0;
let dataGeneratorsCatalog = [];
let dataGenerationJobsStateVersion = null;
let dataGenerationJobsSnapshot = [];
let dataGenerationJobsSummary = { runningCount: 0, totalCount: 0 };
let selectedIngestionRunbookId = "";
let spotlightIngestionRunbookId = "";
let ingestionRunbookSpotlightHandle = null;
let notebookEventsStateVersion = null;
let notebookEventsLoaded = false;
const processedNotebookEventIds = new Set();
let sidebarSourceOperationStatus = null;
let sidebarSourceOperationStatusClearHandle = null;
const sharedNotebookDrafts = new Map();
const sharedNotebookSyncHandles = new Map();
const s3ExplorerNodeRequests = new Map();
const resultExportDialogState = {
  jobId: "",
  exportFormat: "csv",
  exportSettings: defaultResultExportSettings("csv"),
  selectedBucket: "",
  selectedPrefix: "",
  fileName: "",
  saving: false,
};
const localWorkspaceSaveDialogState = {
  jobId: "",
  exportFormat: "csv",
  exportSettings: defaultResultExportSettings("csv"),
  fileName: "",
  folderPath: "",
  saving: false,
  createdFolderPaths: [],
};
const resultDownloadDialogState = {
  jobId: "",
  exportFormat: "csv",
  exportSettings: defaultResultExportSettings("csv"),
  fileName: "",
  downloading: false,
};
const localWorkspaceMoveDialogState = {
  entryId: "",
  fileName: "",
  folderPath: "",
  moving: false,
  createdFolderPaths: [],
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
const localWorkspaceFolderStorageKey = "bdw.localWorkspaceFolders.v1";
const localWorkspaceCatalogSourceId = "workspace.local";
const localWorkspaceSchemaKey = "workspace_local::saved-results";
const localWorkspaceRelationPrefix = "workspace.local.saved_results.";
const unassignedFolderName = "Unassigned";
const localNotebookPrefix = "local-notebook-";
const sharedNotebookPrefix = "shared-notebook-";
const localCellPrefix = "local-cell-";
const initialSqlEditorRows = 5;
const populatedSqlEditorRows = 10;
const defaultSqlEditorAutoRows = 10;
const queryJobTerminalStatuses = new Set(["completed", "failed", "cancelled"]);
const dataGenerationTerminalStatuses = new Set(["completed", "failed", "cancelled"]);
const dataGenerationRunningStatuses = new Set(["queued", "running"]);
let dismissedNotificationKeys = new Set();

const {
  clearWorkbenchLocalCache,
  readDismissedNotificationKeys,
  readLastNotebookId,
  readNotebookActivity,
  readSidebarCollapsed,
  workbenchClientId,
  writeDismissedNotificationKeys,
  writeLastNotebookId,
  writeNotebookActivity,
  writeSidebarCollapsed,
} = createWorkbenchStorage({
  cacheResetStorageKey,
  dismissedNotificationsStorageKey,
  getApplicationVersion: applicationVersion,
  getDismissedNotificationKeys: () => dismissedNotificationKeys,
  lastNotebookStorageKey,
  notebookActivityStorageKey,
  setDismissedNotificationKeys: (notificationKeys) => {
    dismissedNotificationKeys = notificationKeys;
  },
  sidebarCollapsedStorageKey,
  workbenchClientIdStorageKey,
});

dismissedNotificationKeys = readDismissedNotificationKeys();

const {
  applySidebarCollapsedState,
  initializeSidebarResizer,
  initializeSidebarToggle,
  syncSidebarResizerAria,
} = createSidebarLayoutManager({
  readSidebarCollapsed,
  sidebarToggles,
});

const {
  allLocalWorkspaceFolderPaths,
  closestExistingLocalWorkspaceFolderPath,
  ensureLocalWorkspaceFolderPath,
  localWorkspaceDisplayPath,
  localWorkspaceFolderContainsPath,
  localWorkspaceFolderDepth,
  localWorkspaceFolderName,
  localWorkspaceFolderPaths,
  localWorkspaceParentFolderPath,
  localWorkspaceEntryIdFromRelation,
  localWorkspaceRelation,
  isLocalWorkspaceRelation,
  localWorkspaceStoredFolderPaths,
  normalizeLocalWorkspaceFolderPath,
  removeLocalWorkspaceFolderBranch,
} = createLocalWorkspacePathUtils({
  folderStorageKey: localWorkspaceFolderStorageKey,
  relationPrefix: localWorkspaceRelationPrefix,
});

const {
  clearLocalWorkspaceExports,
  deleteLocalWorkspaceExport,
  getLocalWorkspaceExport,
  listLocalWorkspaceExports,
  saveLocalWorkspaceExport,
} = createLocalWorkspaceExportManager({
  databaseName: localWorkspaceDatabaseName,
  databaseVersion: localWorkspaceDatabaseVersion,
  exportStoreName: localWorkspaceExportStoreName,
  normalizeFolderPath: normalizeLocalWorkspaceFolderPath,
});

const {
  clearLocalWorkspaceQuerySourceCache,
  clearLocalWorkspaceQuerySources,
  deleteLocalWorkspaceQuerySource,
  loadLocalWorkspaceSourceFields,
  prepareQuerySql: prepareLocalWorkspaceQuerySql,
} = createLocalWorkspaceQueryBridge({
  getLocalWorkspaceExport,
  isLocalWorkspaceRelation,
  localWorkspaceEntryIdFromRelation,
  localWorkspaceRelation,
  normalizeSourceObjectFields,
  workbenchClientId,
});

const {
  notebookUrl,
  pushHomeHistory,
  pushNotebookHistory,
  pushQueryWorkbenchDataSourcesHistory,
  pushQueryWorkbenchHistory,
  pushServiceConsumptionHistory,
  queryWorkbenchDataSourcesUrl,
} = createNotebookUrlHelpers({ isLocalNotebookId });

const serviceConsumptionUi = createServiceConsumptionUi({
  fetchJsonOrThrow,
  formatByteCount,
});

const {
  localWorkspaceFolderListMarkup,
  localWorkspaceMoveFolderListMarkup,
  renderLocalWorkspaceSaveBreadcrumbs,
  renderLocalWorkspaceMoveBreadcrumbs,
} = createLocalWorkspacePickerUi({
  normalizeFolderPath: normalizeLocalWorkspaceFolderPath,
  folderDepth: localWorkspaceFolderDepth,
  displayPath: localWorkspaceDisplayPath,
  escapeHtml,
  folderName: localWorkspaceFolderName,
  getSaveState: () => localWorkspaceSaveDialogState,
  getMoveState: () => localWorkspaceMoveDialogState,
  getSaveBreadcrumbRoot: localWorkspaceSaveBreadcrumbRoot,
  getMoveBreadcrumbRoot: localWorkspaceMoveBreadcrumbRoot,
});

const {
  createLocalWorkspaceFolderFromDialog,
  createLocalWorkspaceFolderFromMoveDialog,
  openLocalWorkspaceMoveDialog,
  openLocalWorkspaceSaveDialog,
  renderLocalWorkspaceMoveFolderList,
  renderLocalWorkspaceSaveFolderList,
  setLocalWorkspaceMoveDialogBusy,
  setLocalWorkspaceSaveDialogBusy,
  syncLocalWorkspaceMoveDialogState,
  syncLocalWorkspaceSaveDialogState,
  syncOpenLocalWorkspaceMoveDialog,
  syncOpenLocalWorkspaceSaveDialog,
  updateLocalWorkspaceMoveFileName,
  updateLocalWorkspaceMoveFolderPath,
  updateLocalWorkspaceSaveExportFormat,
  updateLocalWorkspaceSaveExportSettingsFromDialog,
  updateLocalWorkspaceSaveFileName,
  updateLocalWorkspaceSaveFolderPath,
} = createLocalWorkspaceDialogController({
  allLocalWorkspaceFolderPaths,
  closestExistingLocalWorkspaceFolderPath,
  createLocalWorkspaceFolder,
  defaultQueryResultExportFilename,
  getEntryIdFromSourceObject: localWorkspaceEntryIdFromSourceObject,
  getLocalWorkspaceExport,
  getMoveState: () => localWorkspaceMoveDialogState,
  getSaveState: () => localWorkspaceSaveDialogState,
  listLocalWorkspaceExports,
  localWorkspaceDisplayPath,
  localWorkspaceFolderListMarkup,
  localWorkspaceMoveFolderListMarkup,
  normalizeLocalWorkspaceFolderPath,
  renderLocalWorkspaceMoveBreadcrumbs,
  renderLocalWorkspaceSaveBreadcrumbs,
});

const { localWorkspaceSchemaMarkup } = createLocalWorkspaceSidebarUi({
  allLocalWorkspaceFolderPaths,
  escapeHtml,
  formatByteCount,
  getLocalWorkspaceCatalogSourceId: () => localWorkspaceCatalogSourceId,
  localWorkspaceDisplayPath,
  localWorkspaceFolderDepth,
  localWorkspaceFolderName,
  localWorkspaceRelation,
  getLocalWorkspaceSchemaKey: () => localWorkspaceSchemaKey,
  normalizeLocalWorkspaceFolderPath,
});

const {
  localWorkspaceInspectorMarkup,
  renderSourceInspector,
  renderSourceInspectorError,
  renderSourceInspectorLoading,
  renderSourceInspectorMarkup,
} = createSourceInspectorUi({
  escapeHtml,
  formatByteCount,
  formatVersionTimestamp,
  localWorkspaceDisplayPath,
  normalizeLocalWorkspaceFolderPath,
  normalizeSourceObjectFields,
  sourceInspector,
  sourceInspectorPanel,
  sourceObjectDisplayKind,
  sourceObjectDisplayName,
});

const {
  clearSourceObjectFieldCacheForRelations,
  getActiveSourceObjectRelation,
  restoreSelectedSourceObject,
  selectSourceObject,
  setSelectedSourceObjectState,
} = createSourceInspectorController({
  isLocalWorkspaceSourceObject,
  loadLocalWorkspaceSourceFields,
  normalizeSourceObjectFields,
  renderSourceInspector,
  renderSourceInspectorError,
  renderSourceInspectorLoading,
  renderSourceInspectorMarkup,
  sourceObjectNodes,
});

const { queryJobById, queryJobForCell, queryJobForResultActionTarget } = createQueryJobState({
  getQueryJobsSnapshot: () => queryJobsSnapshot,
  workspaceNotebookId,
});

const { decorateQueryJobsWithInsights } = createQueryInsights({
  compareQueryJobsByCompletedAt,
  formatQueryDuration,
  normalizeDataSources,
  sourceLabelsForIds,
});

const { autosizeEditor, markEditorInteracted } = createEditorAutosizeManager({
  currentEditorSql,
  defaultAutoRows: defaultSqlEditorAutoRows,
  editorRegistry,
  editorSizingRegistry,
  numericCssValue,
  preferredSqlEditorRows,
});

const {
  loadS3ExplorerNode,
  loadS3ExplorerRoot,
  revealS3ExplorerLocation,
  s3ExplorerNodeForLocation,
} = createS3ExplorerLoader({
  fetchJsonOrThrow,
  getResultExportTreeRoot: resultExportTreeRoot,
  nodeRequests: s3ExplorerNodeRequests,
  renderChildrenMarkup: s3ExplorerChildrenMarkup,
  selectResultExportLocation,
  syncResultExportSelectionState,
  s3ExplorerNodeKey,
});

const {
  queryRowsShownLabel,
  queryResultPanelMarkup,
  renderPerformanceChartMarkup,
  renderPerformanceDistributionMarkup,
  queryPerformanceStatsMarkup,
  queryMonitorItemMarkup,
  queryNotificationItemMarkup,
} = createQueryUi({
  escapeHtml,
  formatQueryDuration,
  formatQueryTimestamp,
  queryJobElapsedMs,
  queryJobEventDateTimeCopy,
  queryJobIsRunning,
  queryJobStatusCopy,
});

const { renderHomePage } = createHomeUi({
  dataGenerationJobElapsedMs,
  escapeHtml,
  formatQueryDuration,
  formatRelativeTimestamp,
  getDataGenerationJobsSnapshot: () => dataGenerationJobsSnapshot,
  homePageRoot,
  homeRecentIngestionsRoot,
  homeRecentNotebooksRoot,
  notebookLinks,
  readNotebookActivity,
});

const {
  defaultFolderPermissions,
  deriveFolderId,
  ensureNotebookInFolderPathState,
  readStoredNotebookTree,
  removeNotebookFromStoredTree,
  writeStoredNotebookTree,
} = createNotebookTreeState({
  deleteStoredNotebookState,
  isLocalNotebookId,
  notebookTreeStorageKey,
});

const {
  clearDragState,
  clearDropTargets,
  createFolderNode,
  deleteTreeFolder,
  directChildrenContainer,
  dropTargetAcceptsNotebookDrop,
  ensureRootUnassignedFolder,
  folderCanDelete,
  folderCanEdit,
  folderLabel,
  initializeNotebookTree,
  notebookDefaultFolderPath,
  persistNotebookTree,
  revealNotebookBranch,
  resolveAddTarget,
  resolveDropTarget,
  rootUnassignedFolder,
  syncRootUnassignedFolder,
  updateFolderCounts,
  updateNotebookSectionCount,
} = createNotebookTreeUi({
  applyNotebookMetadata,
  createNotebookLinkElement,
  defaultFolderPermissions,
  deleteStoredNotebookState,
  deriveFolderId,
  getDraggedNotebook: () => draggedNotebook,
  isLocalNotebookId,
  loadNotebookWorkspace,
  nextVisibleNotebookId,
  notebookMetadata,
  notebookSection,
  notebookTreeRoot,
  persistNotebookDraft,
  readStoredNotebookTree,
  renderEmptyWorkspace,
  unassignedFolderName,
  updateLastNotebookId: writeLastNotebookId,
  visibleNotebookLinks,
  workspaceNotebookId,
  writeStoredNotebookTree,
});

const { applySidebarSearchFilter, initializeSidebarSearch, updateNotebookSearchableItem } =
  createSidebarSearchFilter({
    dataSourcesSection,
    notebookSection,
    sourceLabelsForIds,
  });

const { scrollWorkspaceNotebookIntoView } = createWorkspaceScrollManager();

const { closePopupMenusForTarget, closeSettingsMenus } = createPopupMenuManager({
  closeCellActionMenus,
  closeResultActionMenus,
  closeS3ExplorerActionMenus,
  closeSourceActionMenus,
  closeWorkspaceActionMenus,
  getQueryNotificationMenu: queryNotificationMenu,
  getSettingsMenu: settingsMenu,
});

const { captureSidebarState, refreshSidebar, restoreSidebarState } = createSidebarRefreshController({
  applyNotebookMetadata,
  applySidebarSearchFilter,
  currentActiveNotebookId,
  currentSidebarMode,
  currentWorkspaceMode,
  dataSourcesSection,
  getInitializeNotebookTree: () => initializeNotebookTree,
  getInitializeSidebarResizer: () => initializeSidebarResizer,
  getInitializeSidebarSearch: () => initializeSidebarSearch,
  getInitializeSidebarToggle: () => initializeSidebarToggle,
  getRenderDataGenerationMonitor: () => renderDataGenerationMonitor,
  getRenderHomePage: () => renderHomePage,
  getRenderLocalWorkspaceSidebarEntries: () => renderLocalWorkspaceSidebarEntries,
  getRenderQueryMonitor: () => renderQueryMonitor,
  getRenderQueryNotificationMenu: () => renderQueryNotificationMenu,
  getRenderSidebarSourceOperationStatus: () => renderSidebarSourceOperationStatus,
  getRestoreSelectedSourceObject: () => restoreSelectedSourceObject,
  getSyncSelectedIngestionRunbookState: () => syncSelectedIngestionRunbookState,
  notebookSection,
  workspaceNotebookId,
});

const { querySourceInCurrentNotebook, querySourceInNewNotebook, viewSourceData } =
  createSourceQueryActions({
    createNotebook,
    createSourceQueryCellState,
    defaultNotebookCreateTarget,
    getActiveEditableNotebookId: activeEditableNotebookId,
    getCurrentSidebarMode: currentSidebarMode,
    getNotebookMetadata: notebookMetadata,
    getNotebookTreeRoot: notebookTreeRoot,
    refreshSidebar,
    requestCellRun,
    selectSourceObject,
    setActiveCellId: (cellId) => {
      activeCellId = cellId;
    },
    setNotebookCells,
  });

  const {
  handleClick: handleWorkbenchNavigationClick,
} = createWorkbenchNavigationController({
  applySidebarCollapsedState,
  closeSettingsMenus,
  getClearVisibleNotifications: () => clearVisibleNotifications,
  getQueryNotificationMenu: queryNotificationMenu,
  openLoaderWorkbench,
  loadQueryWorkbenchDataSources,
  loadQueryWorkbenchEntry,
  openServiceConsumptionPage,
  openIngestionWorkbench,
  openQueryWorkbench,
  openQueryWorkbenchDataSources,
  openQueryWorkbenchNavigation,
  promptClearLocalWorkspace,
  selectIngestionRunbook,
  showAboutDialog,
  showFeatureListDialog,
  writeSidebarCollapsed,
});

const {
  applyDataSourceEventsState,
  blinkSourceCatalog,
  getDataSourceEventsStateVersion,
  localWorkspaceEntryNode,
  localWorkspaceFolderNode,
  localWorkspaceSchemaNode,
  renderLocalWorkspaceSidebarEntries,
  revealSidebarS3Bucket,
  setDataSourceConnectionState,
  sourceCatalogNode,
  sourceSchemaBucketNode,
} = createSourceTreeController({
  allLocalWorkspaceFolderPaths,
  captureSidebarState,
  clearSourceObjectFieldCacheForRelations,
  currentActiveNotebookId,
  currentWorkspaceCanEdit,
  currentWorkspaceMode,
  dataSourcesSection,
  getActiveSourceObjectRelation,
  getRenderSidebarSourceOperationStatus: () => renderSidebarSourceOperationStatus,
  getRenderSourceInspectorMarkup: () => renderSourceInspectorMarkup,
  getRestoreSelectedSourceObject: () => restoreSelectedSourceObject,
  getSetSelectedSourceObjectState: () => setSelectedSourceObjectState,
  listLocalWorkspaceExports,
  loadNotebookWorkspace,
  localWorkspaceCatalogSourceId,
  localWorkspaceRelationPrefix,
  localWorkspaceSchemaKey,
  localWorkspaceSchemaMarkup,
  normalizeLocalWorkspaceFolderPath,
  restoreSidebarState,
  showMessageDialog,
  syncOpenLocalWorkspaceMoveDialog,
  syncOpenLocalWorkspaceSaveDialog,
  workspaceNotebookId,
});

const {
  handleCsvIngestionClick,
  handleCsvDragLeave,
  handleCsvDragOver,
  handleCsvDrop,
  handleCsvIngestionChange,
  handleCsvIngestionInput,
  renderCsvIngestionWorkbench,
  showIngestionLanding,
  submitCsvIngestionForm,
} = createCsvIngestionController({
  ensureLocalWorkspaceFolderPath,
  escapeHtml,
  formatByteCount,
  localWorkspaceDisplayPath,
  localWorkspaceRelation,
  normalizeLocalWorkspaceFolderPath,
  openQueryWorkbench,
  querySourceInNewNotebook,
  refreshSidebar,
  renderLocalWorkspaceSidebarEntries,
  saveLocalWorkspaceExport,
  showMessageDialog,
});

const {
  handleClick: handleSourceSidebarClick,
} = createSourceSidebarClickController({
  cancelDataGenerationJob,
  cancelQueryJob,
  cleanupDataGenerationJob,
  closeResultActionMenus,
  closeS3ExplorerActionMenus,
  closeSourceActionMenus,
  createLocalWorkspaceFolder,
  createLocalWorkspaceFolderFromDialog,
  createLocalWorkspaceFolderFromMoveDialog,
  createS3ExplorerBucket,
  createS3ExplorerFolder,
  createSidebarS3Bucket,
  deleteLocalWorkspaceExportFromSource,
  deleteLocalWorkspaceFolder,
  deleteS3EntryDescriptor,
  deleteS3ExplorerEntry,
  downloadLocalWorkspaceExportFromSource,
  downloadQueryResultExport,
  downloadS3ExplorerObject,
  downloadSourceS3Object,
  loadS3ExplorerNode,
  openLocalWorkspaceMoveDialog,
  openLocalWorkspaceSaveDialog,
  openNotebookForQueryJob,
  openResultDownloadDialog,
  openResultExportDialog,
  queryJobForResultActionTarget,
  queryNotificationMenu,
  querySourceInCurrentNotebook,
  querySourceInNewNotebook,
  revealS3ExplorerLocation,
  selectResultExportLocation,
  selectSourceObject,
  setDataSourceConnectionState,
  setDataSourceTreeExpanded,
  setNotebookTreeExpanded,
  setRunbookTreeExpanded,
  showConfirmDialog,
  showMessageDialog,
  sourceObjectS3DeleteDescriptor,
  sourceSchemaS3BucketDescriptor,
  startDataGenerationJob,
  syncSourceActionMenu,
  updateLocalWorkspaceMoveFolderPath,
  updateLocalWorkspaceSaveFolderPath,
  viewSourceData,
});

const {
  activeWorkspaceMetaRoot,
  createInitialNotebookVersion,
  normalizeCellEntry,
  normalizeNotebookCells,
  normalizeNotebookSummaryValue,
  normalizeNotebookTitleValue,
  normalizeStoredNotebookState,
  normalizeVersionEntry,
  notebookAccessMode,
  notebookAccessModeHint,
  notebookSourceIds,
  readNotebookDefaults,
  sortVersionsDescending,
} = createNotebookModel({
  createCellId,
  normalizeTags,
  notebookLinks,
  parseBooleanDatasetValue,
});

const { buildWorkspaceMarkup, cellSourceSummaryMarkup } = createNotebookWorkspaceMarkup({
  escapeHtml,
  formatVersionTimestamp,
  normalizeNotebookCells,
  normalizeTags,
  preferredSqlEditorRows,
  queryResultPanelMarkup,
  truncateWords,
});

const {
  handleChange: handleNotebookWorkspaceChange,
  handleClick: handleNotebookWorkspaceClick,
  handleFocusIn: handleNotebookWorkspaceFocusIn,
  handleInput: handleNotebookWorkspaceInput,
  handleRenameTitleKeydown: handleNotebookWorkspaceRenameTitleKeydown,
  handleSharedToggleClick: handleNotebookWorkspaceSharedToggleClick,
  handleSummaryEscapeKeydown: handleNotebookWorkspaceSummaryEscapeKeydown,
  handleSummaryFocusOut: handleNotebookWorkspaceSummaryFocusOut,
  handleTagInputKeydown: handleNotebookWorkspaceTagInputKeydown,
  syncActiveNotebookSelection,
} = createNotebookWorkspaceController({
  activateNotebookLink,
  addCell,
  autosizeEditor,
  closeCellActionMenus,
  closeCellSourcePicker,
  closeWorkspaceActionMenus,
  copyNotebook,
  deleteCell,
  deleteNotebook,
  duplicateCell,
  focusNotebookMetadata,
  formatCellSql,
  loadNotebookVersion,
  moveCell,
  notebookMetadata,
  renameNotebook,
  revealNotebookLink,
  saveNotebookVersion,
  setActiveCell,
  setCellDataSources,
  setCellSql,
  setNotebookSummary,
  setNotebookTags,
  setSummaryEditing,
  setTagControlsOpen,
  setVersionPanelExpanded,
  shareNotebook,
  showMessageDialog,
  unshareNotebook,
  workspaceNotebookId,
  writeLastNotebookId,
});

const {
  handleAddFolderClick,
  handleCreateNotebookClick,
  handleDeleteFolderClick,
  handleNotebookDragEnd,
  handleNotebookDragOver,
  handleNotebookDragStart,
  handleNotebookDrop,
  handleNotebookTreeToggle,
  handleRenameFolderClick,
} = createNotebookTreeController({
  applySidebarSearchFilter,
  clearDragState,
  clearDropTargets,
  createFolderNode,
  createNotebook,
  defaultFolderPermissions,
  deleteTreeFolder,
  deriveFolderId,
  dropTargetAcceptsNotebookDrop,
  folderCanDelete,
  folderCanEdit,
  folderLabel,
  getDraggedNotebook: () => draggedNotebook,
  notebookTreeRoot,
  persistNotebookTree,
  refreshSidebar,
  resolveAddTarget,
  resolveDropTarget,
  resolveNotebookCreateTarget,
  setDraggedNotebook: (notebook) => {
    draggedNotebook = notebook;
  },
  showConfirmDialog,
  showFolderNameDialog,
  syncRootUnassignedFolder,
  unassignedFolderName,
  updateFolderCounts,
});

const {
  dataGeneratorCardMarkup,
  dataGenerationJobCardMarkup,
  dataGenerationMonitorItemMarkup,
  dataGenerationNotificationItemMarkup,
} = createIngestionUi({
  dataGenerationJobCompletedCopy,
  dataGenerationJobCopy,
  dataGenerationJobElapsedMs,
  dataGenerationJobEventDateTimeCopy,
  dataGenerationJobIsRunning,
  dataGenerationJobStartedCopy,
  dataGenerationJobStatusCopy,
  dataGenerationJobTimingCopy,
  escapeHtml,
  formatDataGenerationSize,
  formatQueryDuration,
  getSpotlightIngestionRunbookId: () => spotlightIngestionRunbookId,
  notebookUrl,
  resolveSelectedIngestionRunbookId,
});

const {
  collectVisibleNotifications,
  renderDataGenerationMonitor,
  renderIngestionWorkbench,
} = createIngestionController({
  currentWorkspaceMode,
  currentWorkspaceNotebookId,
  dataGenerationJobCardMarkup,
  dataGenerationJobIsRunning,
  dataGenerationMonitorCount,
  dataGenerationMonitorItemMarkup,
  dataGenerationMonitorList,
  dataGenerationNotificationItemMarkup,
  escapeHtml,
  getDataGenerationJobsSnapshot: () => dataGenerationJobsSnapshot,
  getDataGenerationTerminalStatuses: () => dataGenerationTerminalStatuses,
  getDismissedNotificationKeys: () => dismissedNotificationKeys,
  getQueryJobsSnapshot: () => queryJobsSnapshot,
  getQueryJobTerminalStatuses: () => queryJobTerminalStatuses,
  ingestionGeneratorById,
  ingestionGeneratorList,
  ingestionGeneratorSectionCopy,
  ingestionGeneratorSectionTitle,
  ingestionJobList,
  ingestionJobSectionCopy,
  ingestionJobSectionTitle,
  notificationItemKey,
  queryJobTerminalStatuses,
  queryNotificationItemMarkup,
  resolveSelectedIngestionRunbookId,
  sidebarQueryCounts,
  dataGeneratorCardMarkup,
});

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
  if (document.querySelector("[data-loader-workbench-page]")) {
    return "loader";
  }
  if (document.querySelector("[data-ingestion-workbench-page]")) {
    return "ingestion";
  }
  return "notebook";
}

function currentWorkbenchSection() {
  if (homePageRoot()) {
    return "home";
  }

  if (serviceConsumptionPageRoot()) {
    return "service-consumption";
  }

  if (queryWorkbenchDataSourcesPageRoot()) {
    return "data-sources";
  }

  const mode = currentWorkspaceMode();
  if (mode === "loader") {
    return "loader";
  }
  if (mode === "ingestion") {
    return "ingestion";
  }
  return "query";
}

function applicationVersion() {
  const explicitVersion =
    settingsMenu()?.dataset.runtimeVersion ||
    document.querySelector("[data-runtime-version]")?.dataset.runtimeVersion ||
    "";
  if (explicitVersion) {
    return explicitVersion.trim();
  }

  const overlayVersion = Array.from(document.querySelectorAll(".app-version-overlay-row"))
    .find((row) => row.querySelector(".app-version-overlay-label")?.textContent?.trim() === "DAAIFL Workbench")
    ?.querySelector(".app-version-overlay-value")
    ?.textContent?.trim() || "";
  if (overlayVersion) {
    return overlayVersion.replace(/^V/i, "").trim() || "unknown";
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

  if (section === "service-consumption") {
    return "DAAIFL Service Consumption";
  }

  if (section === "loader") {
    return "DAAIFL Loader Workbench";
  }

  if (section === "ingestion") {
    return "DAAIFL Ingestion Workbench";
  }

  return "DAAIFL Query Workbench";
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

function currentQueryState() {
  return {
    version: queryJobsStateVersion,
    snapshot: queryJobsSnapshot,
    summary: queryJobsSummary,
    performance: queryPerformanceState,
  };
}

const {
  applyDataGenerationJobsState,
  applyQueryJobsState,
  clearVisibleNotifications,
  renderQueryMonitor,
  renderQueryNotificationMenu,
  syncVisibleQueryCells,
} = createRealtimeController({
  collectVisibleNotifications,
  compareDataGenerationJobsByStartedAt,
  compareQueryJobsByStartedAt,
  currentWorkspaceMode,
  dataGenerationJobCopy,
  dataGenerationJobElapsedMs,
  dataGenerationJobIsRunning,
  decorateQueryJobsWithInsights,
  formatQueryDuration,
  getDataGenerationState: () => ({
    version: dataGenerationJobsStateVersion,
    snapshot: dataGenerationJobsSnapshot,
    summary: dataGenerationJobsSummary,
  }),
  getDismissedNotificationKeys: () => dismissedNotificationKeys,
  getQueryState: currentQueryState,
  normalizeDataGenerationJob,
  normalizeQueryJob,
  notificationClearButton,
  notificationItemKey,
  queryJobElapsedMs,
  queryJobForCell,
  queryJobIsRunning,
  queryMonitorCount,
  queryMonitorItemMarkup,
  queryMonitorList,
  queryNotificationCount,
  queryNotificationList,
  queryNotificationMenu,
  queryPerformanceChart,
  queryPerformanceDistribution,
  queryPerformanceSection,
  queryPerformanceStats,
  queryPerformanceStatsMarkup,
  queryResultPanelMarkup,
  queryRowsShownLabel,
  renderDataGenerationMonitor,
  renderHomePage,
  renderIngestionWorkbench,
  renderPerformanceChartMarkup,
  renderPerformanceDistributionMarkup,
  refreshSidebar,
  setDataGenerationState: (nextState) => {
    dataGenerationJobsStateVersion = nextState.version;
    dataGenerationJobsSnapshot = nextState.snapshot;
    dataGenerationJobsSummary = nextState.summary;
  },
  setQueryState: (nextState) => {
    queryJobsStateVersion = nextState.version;
    queryJobsSnapshot = nextState.snapshot;
    queryJobsSummary = nextState.summary;
    queryPerformanceState = nextState.performance;
  },
  sidebarQueryCounts,
  writeDismissedNotificationKeys,
  workspaceNotebookId,
});

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
    await clearLocalWorkspaceQuerySources();
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

function openLoaderNavigation(generatorId = "") {
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
  if (
    homePageRoot() ||
    serviceConsumptionPageRoot() ||
    queryWorkbenchEntryPageRoot() ||
    queryWorkbenchDataSourcesPageRoot() ||
    currentWorkspaceMode() === "ingestion"
  ) {
    setShellSidebarHidden(true);
    return;
  }

  restoreSidebarVisibilityForWorkspace();
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

function localWorkspaceSaveBreadcrumbRoot() {
  return localWorkspaceSaveDialog()?.querySelector("[data-local-workspace-breadcrumbs]") ?? null;
}

function localWorkspaceMoveBreadcrumbRoot() {
  return localWorkspaceMoveDialog()?.querySelector("[data-local-workspace-move-breadcrumbs]") ?? null;
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

function parseBooleanDatasetValue(value, fallback = false) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  return String(value).trim().toLowerCase() === "true";
}

function readSchema() {
  const element = document.getElementById("sql-schema");
  if (!element?.textContent) {
    return {};
  }

  try {
    const parsed = JSON.parse(element.textContent);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch (_error) {
    return {};
  }
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
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

  const linkedNotebooks = Array.isArray(generator.linkedNotebooks)
    ? generator.linkedNotebooks
        .map((notebook) => {
          if (!notebook || typeof notebook !== "object") {
            return null;
          }

          const notebookId = String(notebook.notebookId ?? "").trim();
          const notebookTitle = String(notebook.title ?? "").trim();
          if (!notebookId || !notebookTitle) {
            return null;
          }

          return {
            notebookId,
            title: notebookTitle,
          };
        })
        .filter(Boolean)
    : [];

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
    linkedNotebooks,
  };
}

function normalizeDataGenerationJob(job) {
  if (!job || typeof job !== "object") {
    return null;
  }

  const writtenTargets = Array.isArray(job.writtenTargets)
    ? job.writtenTargets
        .map((target) => {
          if (!target || typeof target !== "object") {
            return null;
          }

          const targetKind = String(target.targetKind ?? target.target_kind ?? "").trim() || "target";
          const label = String(target.label ?? "").trim();
          const location = String(target.location ?? "").trim();
          const status = String(target.status ?? "").trim() || "pending";
          if (!location) {
            return null;
          }

          return {
            targetKind,
            label: label || location,
            location,
            status,
          };
        })
        .filter(Boolean)
    : [];

  return {
    ...job,
    generatorId: String(job.generatorId ?? "").trim(),
    title: String(job.title ?? "").trim() || "Data generation",
    description: String(job.description ?? "").trim(),
    targetKind: String(job.targetKind ?? "").trim() || "unknown",
    targetName: String(job.targetName ?? "").trim(),
    targetRelation: String(job.targetRelation ?? "").trim(),
    targetPath: String(job.targetPath ?? "").trim(),
    writtenTargets,
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
  if (currentWorkspaceMode() === "loader") {
    renderIngestionWorkbench();
  }

  ingestionRunbookSpotlightHandle = window.setTimeout(() => {
    spotlightIngestionRunbookId = "";
    ingestionRunbookSpotlightHandle = null;
    syncSelectedIngestionRunbookState();
    if (currentWorkspaceMode() === "loader") {
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

function notificationItemKey(type, job) {
  const status = String(job?.status || "").trim().toLowerCase();
  const lifecycleKey =
    status === "completed" || status === "failed" || status === "cancelled" ? status : "active";
  return `${type}:${job?.jobId || ""}:${lifecycleKey}`;
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

function currentWorkspaceCanEdit() {
  return document.querySelector("[data-notebook-meta]")?.dataset.canEdit !== "false";
}

function escapeSelectorValue(value) {
  return typeof window.CSS?.escape === "function" ? window.CSS.escape(String(value ?? "")) : String(value ?? "");
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


function defaultLocalNotebookTitle() {
  const localNotebookCount = Object.keys(readStoredNotebookMetadata()).filter((key) =>
    isLocalNotebookId(key)
  ).length;

  return `Untitled Notebook ${localNotebookCount + 1}`;
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

function preferredSqlEditorRows(sql) {
  return String(sql ?? "").trim() ? populatedSqlEditorRows : initialSqlEditorRows;
}

function currentEditorSql(root) {
  if (!(root instanceof Element)) {
    return "";
  }

  const editor = editorRegistry.get(root);
  if (editor) {
    return editor.state.doc.toString();
  }

  const textarea = root.querySelector("[data-editor-source]");
  return textarea?.value ?? defaultEditorSql(textarea);
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
              markEditorInteracted(root);
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

function renderLocalNotebookWorkspace(notebookId, options = {}) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  const { scrollToTop = false } = options;
  const metadata = notebookMetadata(notebookId);
  panel.innerHTML = buildWorkspaceMarkup(notebookId, metadata, activeCellId);
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
  if (scrollToTop) {
    scrollWorkspaceNotebookIntoView();
  }
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
  renderLocalNotebookWorkspace(notebookId, { scrollToTop: true });
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

function setNotebookTreeExpanded(open = false) {
  const section = notebookSection();
  if (section instanceof HTMLDetailsElement) {
    section.open = Boolean(open);
  }
}

function setRunbookTreeExpanded(open = false) {
  const section = ingestionRunbookSection();
  if (section instanceof HTMLDetailsElement) {
    section.open = Boolean(open);
  }
}

function setDataSourceTreeExpanded(open = false) {
  const section = dataSourcesSection();
  if (section instanceof HTMLDetailsElement) {
    section.open = Boolean(open);
  }
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
  renderLocalNotebookWorkspace(duplicateId, { scrollToTop: true });
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
function revealNotebookLink(notebookId) {
  revealNotebookBranch(notebookId);
}

function processHtmx(root) {
  if (!root || typeof window.htmx?.process !== "function") {
    return;
  }

  window.htmx.process(root);
}

function clientConnectionsCountRoot() {
  return document.querySelector("[data-client-connections-count]");
}

function applyClientConnectionsState(snapshot) {
  clientConnectionsStateVersion = Number(snapshot?.version ?? 0);
  clientConnectionsCount = Math.max(0, Number(snapshot?.count ?? 0) || 0);
  const countRoot = clientConnectionsCountRoot();
  if (countRoot) {
    countRoot.textContent = String(clientConnectionsCount);
  }
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
  await requestQueryJobsState({ applyQueryJobsState });
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

async function loadServiceConsumptionState({
  windowRange = "24h",
} = {}) {
  const payload = await serviceConsumptionUi.loadState({
    windowRange,
  });
  serviceConsumptionStateVersion = Number(payload?.version || 0);
  return payload;
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
    case "service-consumption":
      serviceConsumptionStateVersion = Number(snapshot?.version || 0);
      serviceConsumptionUi.applyRealtimeSnapshot(snapshot);
      break;
    case "notebook-events":
      applyNotebookEventsState(snapshot);
      break;
    case "client-connections":
      applyClientConnectionsState(snapshot);
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
    if (isLocalNotebookId(notebookId)) {
      pushQueryWorkbenchHistory();
    } else {
      pushNotebookHistory(notebookId);
    }
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
      if (isLocalNotebookId(preferredNotebookId)) {
        pushQueryWorkbenchHistory();
      } else {
        pushNotebookHistory(preferredNotebookId);
      }
      applyWorkbenchTitle("query");
      return;
    }

    await loadNotebookWorkspace(preferredNotebookId);
    if (isLocalNotebookId(preferredNotebookId)) {
      pushQueryWorkbenchHistory();
    } else {
      pushNotebookHistory(preferredNotebookId);
    }
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

async function loadServiceConsumptionPage({ pushHistory = true } = {}) {
  const panel = await loadWorkspacePanelPartial("/service-consumption");
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("service-consumption");
  await serviceConsumptionUi.initializeCurrentPage();
  if (pushHistory) {
    pushServiceConsumptionHistory();
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
  if (pushHistory) {
    pushHomeHistory();
  }
}

async function openQueryWorkbenchDataSources() {
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }

  await loadQueryWorkbenchDataSources();
}

async function openServiceConsumptionPage() {
  await loadServiceConsumptionPage();
}

function ensureRealtimeEventsEventSource() {
  if (realtimeEventsEventSource || typeof window.EventSource !== "function") {
    return;
  }

  const params = new URLSearchParams();
  const dataSourceEventsStateVersion = getDataSourceEventsStateVersion();
  if (queryJobsStateVersion !== null) {
    params.set("queryJobsVersion", String(queryJobsStateVersion));
  }
  if (dataGenerationJobsStateVersion !== null) {
    params.set("dataGenerationJobsVersion", String(dataGenerationJobsStateVersion));
  }
  if (dataSourceEventsStateVersion !== null) {
    params.set("dataSourceEventsVersion", String(dataSourceEventsStateVersion));
  }
  if (serviceConsumptionStateVersion !== null) {
    params.set("serviceConsumptionVersion", String(serviceConsumptionStateVersion));
  }
  if (notebookEventsStateVersion !== null) {
    params.set("notebookEventsVersion", String(notebookEventsStateVersion));
  }
  if (clientConnectionsStateVersion !== null) {
    params.set("clientConnectionsVersion", String(clientConnectionsStateVersion));
  }

  const streamUrl = params.size
    ? `/api/events/stream?${params.toString()}`
    : "/api/events/stream";
  const eventSource = new window.EventSource(streamUrl);
  [
    "query-jobs",
    "data-generation-jobs",
    "data-source-events",
    "service-consumption",
    "notebook-events",
    "client-connections",
  ].forEach((topic) => {
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
    const dataSourceEventsStateVersion = getDataSourceEventsStateVersion();
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
    if (serviceConsumptionStateVersion !== null) {
      refreshTasks.push(
        loadServiceConsumptionState({
          windowRange: serviceConsumptionPageRoot() ? serviceConsumptionUi.currentWindow() : "24h",
        }).catch(() => {
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

async function openIngestionWorkbench() {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  const response = await window.fetch("/ingestion-workbench", {
    headers: { "HX-Request": "true" },
  });
  if (!response.ok) {
    throw new Error(`Failed to load the ingestion workbench: ${response.status}`);
  }

  panel.innerHTML = await response.text();
  processHtmx(panel);
  setShellSidebarHidden(true);
  applyWorkbenchTitle("ingestion");
  if (window.location.pathname !== "/ingestion-workbench") {
    window.history.pushState({}, "", "/ingestion-workbench");
  }
  showIngestionLanding();
  renderQueryNotificationMenu();
}

async function openLoaderWorkbench({ focusJobId = "", focusGeneratorId = "" } = {}) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  openLoaderNavigation();

  const response = await window.fetch("/loader-workbench", {
    headers: { "HX-Request": "true" },
  });
  if (!response.ok) {
    throw new Error(`Failed to load the Loader Workbench: ${response.status}`);
  }

  panel.innerHTML = await response.text();
  processHtmx(panel);
  applyWorkbenchTitle("loader");
  if (window.location.pathname !== "/loader-workbench") {
    window.history.pushState({}, "", "/loader-workbench");
  }
  await Promise.allSettled([loadDataGeneratorCatalog(), loadDataGenerationJobsState()]);
  const focusedJob = focusJobId
    ? dataGenerationJobsSnapshot.find((job) => job.jobId === focusJobId) ?? null
    : null;
  const selectedGeneratorId = selectIngestionRunbook(
    focusGeneratorId || focusedJob?.generatorId || selectedIngestionRunbookId,
    { spotlight: Boolean(focusGeneratorId) }
  );
  renderIngestionWorkbench();
  if (currentSidebarMode() !== "loader") {
    await refreshSidebar("loader");
  } else {
    syncSelectedIngestionRunbookState();
    renderDataGenerationMonitor();
  }
  openLoaderNavigation(selectedGeneratorId || focusGeneratorId);
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
  const originalSql = editorSource?.value ?? "";
  let executionSql = originalSql;
  try {
    const preparedQuery = await prepareLocalWorkspaceQuerySql(originalSql);
    executionSql = preparedQuery.sql;
  } catch (error) {
    const resultRoot = cellRoot.querySelector("[data-cell-result]");
    if (resultRoot) {
      resultRoot.outerHTML = queryResultPanelMarkup(cellId, {
        jobId: `local-error-${cellId}`,
        notebookId,
        notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
        cellId,
        sql: originalSql,
        status: "failed",
        durationMs: 0,
        updatedAt: new Date().toISOString(),
        rowsShown: 0,
        truncated: false,
        message: "Query failed.",
        error:
          error instanceof Error
            ? error.message
            : "The Local Workspace sources could not be prepared for querying.",
        columns: [],
        rows: [],
      });
    }
    return;
  }
  formData.set("sql", executionSql);
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
        sql: originalSql,
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
  applyOptimisticQueryJobSnapshot({
    snapshot,
    applyQueryJobsState,
    getQueryState: currentQueryState,
    incrementRunningCount: true,
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

    applyOptimisticQueryJobSnapshot({
      snapshot,
      applyQueryJobsState,
      getQueryState: currentQueryState,
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
  const baseName = `${job?.notebookTitle || "query"}-${job?.cellId || "cell"}`
    .replace(/[^\w.-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase();
  return ensureResultExportFileNameExtension("", format, baseName || "query-result");
}

async function fetchQueryResultExportBlob(job, exportFormat, exportSettings = {}) {
  const response = await window.fetch(`/api/query-jobs/${encodeURIComponent(job.jobId)}/export/download`, {
    method: "POST",
    headers: {
      Accept: "application/octet-stream",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      format: normalizeResultExportFormat(exportFormat),
      settings: normalizeResultExportSettings(exportFormat, exportSettings),
    }),
  });

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

function blinkLocalWorkspaceFolder(folderPath = "") {
  const summary = localWorkspaceFolderNode(folderPath)?.querySelector(":scope > summary");
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

async function revealLocalWorkspaceFolderPath(folderPath = "") {
  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
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

  if (!normalizedFolderPath) {
    blinkSourceCatalog(localWorkspaceCatalogSourceId);
    localWorkspaceCatalog?.scrollIntoView({ block: "nearest" });
    return;
  }

  const folderAncestors = localWorkspaceFolderPaths([normalizedFolderPath]).filter(Boolean);
  folderAncestors.forEach((path) => {
    const folderNode = localWorkspaceFolderNode(path);
    if (folderNode instanceof HTMLDetailsElement) {
      folderNode.open = true;
    }
  });

  blinkSourceCatalog(localWorkspaceCatalogSourceId);
  blinkLocalWorkspaceFolder(normalizedFolderPath);
  localWorkspaceFolderNode(normalizedFolderPath)?.scrollIntoView({ block: "nearest" });
}

async function createLocalWorkspaceFolder(
  parentPath = "",
  { confirmCreation = false, showSidebarStatus = false, revealSidebar = false } = {}
) {
  const normalizedParentPath = normalizeLocalWorkspaceFolderPath(parentPath);
  const folderName = await showFolderNameDialog({
    title: "New Local Workspace folder",
    copy: `Create a folder under ${localWorkspaceDisplayPath(normalizedParentPath)}.`,
    submitLabel: "Create folder",
  });
  if (!folderName) {
    return null;
  }

  const nextPath = normalizeLocalWorkspaceFolderPath(
    normalizedParentPath ? `${normalizedParentPath}/${folderName}` : folderName
  );
  if (!nextPath) {
    return null;
  }

  const entries = await listLocalWorkspaceExports();
  const knownPaths = allLocalWorkspaceFolderPaths(entries.map((entry) => entry.folderPath));
  if (knownPaths.includes(nextPath)) {
    throw new Error(`The Local Workspace folder "${nextPath}" already exists.`);
  }

  if (confirmCreation) {
    const { confirmed } = await showConfirmDialog({
      title: "Create Local Workspace folder",
      copy: `Create folder ${localWorkspaceDisplayPath(nextPath)} in this browser's Local Workspace?`,
      confirmLabel: "Create folder",
      confirmTone: "primary",
    });
    if (!confirmed) {
      return null;
    }
  }

  if (showSidebarStatus) {
    setSidebarSourceOperationStatus({
      tone: "info",
      title: "Creating folder",
      copy: `Creating ${localWorkspaceDisplayPath(nextPath)} in this browser...`,
    });
  }

  try {
    ensureLocalWorkspaceFolderPath(nextPath);
    await renderLocalWorkspaceSidebarEntries();
    await syncOpenLocalWorkspaceSaveDialog();
    if (revealSidebar) {
      await revealLocalWorkspaceFolderPath(nextPath);
    }
    if (showSidebarStatus) {
      setSidebarSourceOperationStatus(
        {
          tone: "success",
          title: "Folder created",
          copy: `Created ${localWorkspaceDisplayPath(nextPath)} in this browser.`,
        },
        { autoClearMs: 6000 }
      );
    }
    return nextPath;
  } catch (error) {
    if (showSidebarStatus) {
      setSidebarSourceOperationStatus(
        {
          tone: "danger",
          title: "Folder creation failed",
          copy:
            error instanceof Error
              ? error.message
              : "The Local Workspace folder could not be created.",
        },
        { autoClearMs: 8000 }
      );
    }
    throw error;
  }
}

async function deleteLocalWorkspaceFolder(folderPath = "") {
  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
  if (!normalizedFolderPath) {
    return false;
  }

  const entries = await listLocalWorkspaceExports();
  const descendantEntries = entries.filter((entry) =>
    localWorkspaceFolderContainsPath(normalizedFolderPath, entry.folderPath)
  );
  const descendantFolders = localWorkspaceStoredFolderPaths().filter(
    (path) =>
      path !== normalizedFolderPath && localWorkspaceFolderContainsPath(normalizedFolderPath, path)
  );
  const objectSummary = [];
  if (descendantFolders.length) {
    objectSummary.push(`${descendantFolders.length} nested folder${descendantFolders.length === 1 ? "" : "s"}`);
  }
  if (descendantEntries.length) {
    objectSummary.push(`${descendantEntries.length} saved file${descendantEntries.length === 1 ? "" : "s"}`);
  }
  const summaryCopy = objectSummary.length ? ` This also removes ${objectSummary.join(" and ")}.` : "";

  const { confirmed } = await showConfirmDialog({
    title: "Delete Local Workspace folder",
    copy: `Delete ${localWorkspaceDisplayPath(normalizedFolderPath)} from this browser's Local Workspace?${summaryCopy}`,
    confirmLabel: "Delete folder",
  });
  if (!confirmed) {
    return null;
  }

  setSidebarSourceOperationStatus({
    tone: "info",
    title: "Deleting folder",
    copy: `Deleting ${localWorkspaceDisplayPath(normalizedFolderPath)} from this browser...`,
  });

  try {
    await Promise.all(
      descendantEntries.map((entry) => deleteLocalWorkspaceQuerySource(entry.id))
    );
    await Promise.all(descendantEntries.map((entry) => deleteLocalWorkspaceExport(entry.id)));
    removeLocalWorkspaceFolderBranch(normalizedFolderPath);

    const activeSourceObjectRelation = getActiveSourceObjectRelation();
    const deletedRelations = new Set(descendantEntries.map((entry) => localWorkspaceRelation(entry.id)));
    descendantEntries.forEach((entry) => clearLocalWorkspaceQuerySourceCache(entry.id));
    clearSourceObjectFieldCacheForRelations(Array.from(deletedRelations));
    if (activeSourceObjectRelation && deletedRelations.has(activeSourceObjectRelation)) {
      setSelectedSourceObjectState(null);
      renderSourceInspectorMarkup("", true);
    }

    await renderLocalWorkspaceSidebarEntries();
    await syncOpenLocalWorkspaceSaveDialog();
    await revealLocalWorkspaceFolderPath(localWorkspaceParentFolderPath(normalizedFolderPath));
    setSidebarSourceOperationStatus(
      {
        tone: "success",
        title: "Folder deleted",
        copy: `Deleted ${localWorkspaceDisplayPath(normalizedFolderPath)} from this browser.`,
      },
      { autoClearMs: 6000 }
    );
    return true;
  } catch (error) {
    setSidebarSourceOperationStatus(
      {
        tone: "danger",
        title: "Folder delete failed",
        copy:
          error instanceof Error
            ? error.message
            : "The Local Workspace folder could not be deleted.",
      },
      { autoClearMs: 8000 }
    );
    throw error;
  }
}

async function saveQueryResultExportToLocalWorkspace(job, exportFormat, options = {}) {
  if (!job?.jobId || !job?.columns?.length) {
    return;
  }

  const normalizedFormat = normalizeResultExportFormat(exportFormat);
  const exportSettings = normalizeResultExportSettings(normalizedFormat, options.exportSettings);
  const exported = await fetchQueryResultExportBlob(job, normalizedFormat, exportSettings);
  const timestamp = new Date().toISOString();
  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(options.folderPath);
  const fileName = String(options.fileName || exported.fileName || "").trim() || exported.fileName;
  ensureLocalWorkspaceFolderPath(normalizedFolderPath);
  const storedEntry = await saveLocalWorkspaceExport({
    id: `local-workspace-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
    fileName,
    folderPath: normalizedFolderPath,
    exportFormat: normalizedFormat,
    mimeType: exported.blob.type,
    sizeBytes: exported.blob.size,
    createdAt: timestamp,
    updatedAt: timestamp,
    notebookTitle: String(job.notebookTitle || "").trim(),
    cellId: String(job.cellId || "").trim(),
    columnCount: Array.isArray(job.columns) ? job.columns.length : 0,
    rowCount: Array.isArray(job.rows) ? job.rows.length : 0,
    csvDelimiter: normalizedFormat === "csv" ? String(exportSettings.delimiter || ",") : "",
    csvHasHeader: normalizedFormat === "csv" ? exportSettings.includeHeader !== false : true,
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
  if (normalizedFolderPath) {
    await revealLocalWorkspaceFolderPath(normalizedFolderPath);
  }

  const sourceObjectRoot = localWorkspaceEntryNode(storedEntry.id);
  if (sourceObjectRoot instanceof Element) {
    setSelectedSourceObjectState(sourceObjectRoot);
    renderSourceInspectorMarkup(localWorkspaceInspectorMarkup(sourceObjectRoot));
    sourceObjectRoot.scrollIntoView({ block: "nearest" });
  }

  await showMessageDialog({
    title: "Results saved to Local Workspace (IndexDB)",
    copy: `${storedEntry.fileName} was saved to ${localWorkspaceDisplayPath(storedEntry.folderPath)} in this browser.`,
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

async function moveLocalWorkspaceExport(entryId, options = {}) {
  const normalizedEntryId = String(entryId || "").trim();
  if (!normalizedEntryId) {
    return null;
  }

  const entry = await getLocalWorkspaceExport(normalizedEntryId);
  if (!entry) {
    return null;
  }

  const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(options.folderPath);
  const normalizedFileName = String(options.fileName || "").trim();
  if (!normalizedFileName) {
    throw new Error("Provide a file name before moving the Local Workspace file.");
  }

  const allEntries = await listLocalWorkspaceExports();
  const duplicateEntry = allEntries.find(
    (candidate) =>
      candidate.id !== normalizedEntryId &&
      normalizeLocalWorkspaceFolderPath(candidate.folderPath) === normalizedFolderPath &&
      String(candidate.fileName || "").trim().localeCompare(normalizedFileName, undefined, {
        sensitivity: "base",
      }) === 0
  );
  if (duplicateEntry) {
    throw new Error(
      `A Local Workspace file named "${normalizedFileName}" already exists in ${localWorkspaceDisplayPath(normalizedFolderPath)}.`
    );
  }

  ensureLocalWorkspaceFolderPath(normalizedFolderPath);
  const timestamp = new Date().toISOString();
  const updatedEntry = await saveLocalWorkspaceExport({
    ...entry,
    fileName: normalizedFileName,
    folderPath: normalizedFolderPath,
    updatedAt: timestamp,
  });

  await renderLocalWorkspaceSidebarEntries();
  await revealLocalWorkspaceFolderPath(normalizedFolderPath);

  const movedNode = localWorkspaceEntryNode(updatedEntry.id);
  if (movedNode instanceof Element) {
    if (getActiveSourceObjectRelation() === localWorkspaceRelation(updatedEntry.id)) {
      setSelectedSourceObjectState(movedNode);
      renderSourceInspectorMarkup(localWorkspaceInspectorMarkup(movedNode));
    }
    movedNode.scrollIntoView({ block: "nearest" });
  }

  return updatedEntry;
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

  await deleteLocalWorkspaceQuerySource(entryId);
  await deleteLocalWorkspaceExport(entryId);
  clearLocalWorkspaceQuerySourceCache(entryId);
  clearSourceObjectFieldCacheForRelations([localWorkspaceRelation(entryId)]);
  if (getActiveSourceObjectRelation() === localWorkspaceRelation(entryId)) {
    setSelectedSourceObjectState(null);
    renderSourceInspectorMarkup("", true);
  }
  await renderLocalWorkspaceSidebarEntries();
  return true;
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

  const formatSelect = dialog.querySelector("[data-export-format-select]");
  if (formatSelect instanceof HTMLSelectElement && formatSelect.value !== resultExportDialogState.exportFormat) {
    formatSelect.value = resultExportDialogState.exportFormat;
  }
  renderResultExportSettings(
    dialog,
    resultExportDialogState.exportFormat,
    resultExportDialogState.exportSettings
  );

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
      : "Save to Shared Workspace (S3)";
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
    const formatSelect = dialog.querySelector("[data-export-format-select]");
    if (formatSelect instanceof HTMLSelectElement) {
      formatSelect.disabled = busy;
    }
    dialog.querySelectorAll("[data-export-setting]").forEach((node) => {
      if (node instanceof HTMLInputElement || node instanceof HTMLSelectElement) {
        node.disabled = busy;
      }
    });
  }
  syncResultExportSelectionState();
}

function selectResultExportLocation(bucket, prefix = "") {
  resultExportDialogState.selectedBucket = String(bucket || "").trim();
  resultExportDialogState.selectedPrefix = String(prefix || "").trim();
  syncResultExportSelectionState();
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

  resultExportDialogState.exportSettings = normalizeResultExportSettings(
    resultExportDialogState.exportFormat,
    readResultExportSettings(dialog, resultExportDialogState.exportFormat)
  );
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
          settings: resultExportDialogState.exportSettings,
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

function updateResultExportFormat(value) {
  resultExportDialogState.exportFormat = normalizeResultExportFormat(value);
  resultExportDialogState.exportSettings = defaultResultExportSettings(resultExportDialogState.exportFormat);
  resultExportDialogState.fileName = ensureResultExportFileNameExtension(
    resultExportDialogState.fileName,
    resultExportDialogState.exportFormat,
    "query-result"
  );
  syncResultExportSelectionState();
}

async function openResultExportDialog(job, exportFormat = "csv") {
  if (!job?.jobId || !job?.columns?.length) {
    return;
  }

  const dialog = ensureResultExportDialog();
  resultExportDialogState.jobId = job.jobId;
  resultExportDialogState.exportFormat = normalizeResultExportFormat(exportFormat);
  resultExportDialogState.exportSettings = defaultResultExportSettings(resultExportDialogState.exportFormat);
  resultExportDialogState.fileName = defaultQueryResultExportFilename(job, resultExportDialogState.exportFormat);
  resultExportDialogState.saving = false;

  const titleNode = dialog.querySelector("[data-result-export-title]");
  const copyNode = dialog.querySelector("[data-result-export-copy]");
  if (titleNode) {
    titleNode.textContent = "Save Results in Shared Workspace (S3) ...";
  }
  if (copyNode) {
    copyNode.textContent =
      "Choose a Shared Workspace (S3) location, then select the export format and any format-specific settings.";
  }

  syncResultExportSelectionState();
  dialog.showModal();
  await loadS3ExplorerRoot({
    preferredBucket: resultExportDialogState.selectedBucket,
    preferredPrefix: resultExportDialogState.selectedPrefix,
  });
}

function resultDownloadFileNameInput() {
  return resultDownloadDialog()?.querySelector("[data-result-download-file-name]") ?? null;
}

function resultDownloadSubmitButton() {
  return resultDownloadDialog()?.querySelector("[data-result-download-submit]") ?? null;
}

function syncResultDownloadDialogState() {
  const dialog = resultDownloadDialog();
  if (!dialog) {
    return;
  }

  const fileNameInput = resultDownloadFileNameInput();
  if (fileNameInput instanceof HTMLInputElement && fileNameInput.value !== resultDownloadDialogState.fileName) {
    fileNameInput.value = resultDownloadDialogState.fileName;
  }

  const formatSelect = dialog.querySelector("[data-export-format-select]");
  if (formatSelect instanceof HTMLSelectElement && formatSelect.value !== resultDownloadDialogState.exportFormat) {
    formatSelect.value = resultDownloadDialogState.exportFormat;
  }

  renderResultExportSettings(
    dialog,
    resultDownloadDialogState.exportFormat,
    resultDownloadDialogState.exportSettings
  );

  const submitButton = resultDownloadSubmitButton();
  if (submitButton instanceof HTMLButtonElement) {
    submitButton.disabled =
      resultDownloadDialogState.downloading || !String(resultDownloadDialogState.fileName || "").trim();
    submitButton.textContent = resultDownloadDialogState.downloading
      ? "Downloading..."
      : "Download Results";
  }
}

function setResultDownloadDialogBusy(busy) {
  resultDownloadDialogState.downloading = busy;
  const dialog = resultDownloadDialog();
  if (dialog) {
    const fileNameInput = resultDownloadFileNameInput();
    if (fileNameInput instanceof HTMLInputElement) {
      fileNameInput.disabled = busy;
    }
    const formatSelect = dialog.querySelector("[data-export-format-select]");
    if (formatSelect instanceof HTMLSelectElement) {
      formatSelect.disabled = busy;
    }
    dialog.querySelectorAll("[data-export-setting]").forEach((node) => {
      if (node instanceof HTMLInputElement || node instanceof HTMLSelectElement) {
        node.disabled = busy;
      }
    });
  }
  syncResultDownloadDialogState();
}

function updateResultDownloadFormat(value) {
  resultDownloadDialogState.exportFormat = normalizeResultExportFormat(value);
  resultDownloadDialogState.exportSettings = defaultResultExportSettings(resultDownloadDialogState.exportFormat);
  resultDownloadDialogState.fileName = ensureResultExportFileNameExtension(
    resultDownloadDialogState.fileName,
    resultDownloadDialogState.exportFormat,
    "query-result"
  );
  syncResultDownloadDialogState();
}

async function openResultDownloadDialog(job, exportFormat = "csv") {
  if (!job?.jobId || !job?.columns?.length) {
    return;
  }

  const dialog = ensureResultDownloadDialog();
  resultDownloadDialogState.jobId = job.jobId;
  resultDownloadDialogState.exportFormat = normalizeResultExportFormat(exportFormat);
  resultDownloadDialogState.exportSettings = defaultResultExportSettings(resultDownloadDialogState.exportFormat);
  resultDownloadDialogState.fileName = defaultQueryResultExportFilename(job, resultDownloadDialogState.exportFormat);
  resultDownloadDialogState.downloading = false;

  const titleNode = dialog.querySelector("[data-result-download-title]");
  const copyNode = dialog.querySelector("[data-result-download-copy]");
  if (titleNode) {
    titleNode.textContent = "Download Results as ...";
  }
  if (copyNode) {
    copyNode.textContent =
      "Choose the export format, adjust any format-specific settings, and confirm the download file name.";
  }

  syncResultDownloadDialogState();
  dialog.showModal();
}

async function downloadQueryResultExport(job, exportFormat, exportSettings = {}, fileName = "") {
  if (!job?.jobId || !job?.columns?.length) {
    return;
  }

  const normalizedFormat = normalizeResultExportFormat(exportFormat);
  const normalizedSettings = normalizeResultExportSettings(normalizedFormat, exportSettings);
  const exported = await fetchQueryResultExportBlob(job, normalizedFormat, normalizedSettings);
  downloadBlobFile(
    ensureResultExportFileNameExtension(
      String(fileName || exported.fileName || "").trim(),
      normalizedFormat,
      "query-result"
    ),
    exported.blob
  );
}

async function loadNotebookWorkspace(notebookId, options = {}) {
  const panel = document.getElementById("workspace-panel");
  if (!panel || !notebookId) {
    return;
  }

  const { scrollToTop = true } = options;
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
    renderLocalNotebookWorkspace(notebookId, { scrollToTop });
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
  if (scrollToTop) {
    scrollWorkspaceNotebookIntoView();
  }
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
    const csvIngestionForm = event.target.closest("[data-csv-ingestion-form]");
    if (csvIngestionForm) {
      event.preventDefault();
      try {
        await submitCsvIngestionForm();
      } catch (error) {
        console.error("Failed to import CSV files.", error);
        await showMessageDialog({
          title: "CSV import failed",
          copy: error instanceof Error ? error.message : "The CSV files could not be imported.",
        });
      }
      return;
    }

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
          title: "Local Workspace (IndexDB) save unavailable",
          copy: "Run the cell again so the current query result can be saved to Local Workspace (IndexDB).",
        });
        return;
      }

      try {
        updateLocalWorkspaceSaveExportSettingsFromDialog();
        setLocalWorkspaceSaveDialogBusy(true);
        await saveQueryResultExportToLocalWorkspace(job, localWorkspaceSaveDialogState.exportFormat, {
          fileName: localWorkspaceSaveDialogState.fileName,
          folderPath: localWorkspaceSaveDialogState.folderPath,
          exportSettings: localWorkspaceSaveDialogState.exportSettings,
        });
        closeDialog(localWorkspaceSaveDialog(), "confirm");
      } catch (error) {
        console.error("Failed to save the query result to Local Workspace.", error);
        await showMessageDialog({
          title: "Local Workspace (IndexDB) save failed",
          copy:
            error instanceof Error
              ? error.message
              : "The query result could not be saved to Local Workspace (IndexDB).",
        });
      } finally {
        setLocalWorkspaceSaveDialogBusy(false);
      }
      return;
    }

    const resultDownloadForm = event.target.closest("[data-result-download-form]");
    if (resultDownloadForm) {
      event.preventDefault();
      const job = queryJobById(resultDownloadDialogState.jobId);
      if (!job) {
        await showMessageDialog({
          title: "Result download unavailable",
          copy: "Run the cell again so the current query result can be downloaded.",
        });
        return;
      }

      try {
        resultDownloadDialogState.exportSettings = normalizeResultExportSettings(
          resultDownloadDialogState.exportFormat,
          readResultExportSettings(resultDownloadDialog(), resultDownloadDialogState.exportFormat)
        );
        setResultDownloadDialogBusy(true);
        await downloadQueryResultExport(
          job,
          resultDownloadDialogState.exportFormat,
          resultDownloadDialogState.exportSettings,
          resultDownloadDialogState.fileName
        );
        closeDialog(resultDownloadDialog(), "confirm");
      } catch (error) {
        console.error("Failed to download the query result export.", error);
        await showMessageDialog({
          title: "Result download failed",
          copy: error instanceof Error ? error.message : "The query result could not be downloaded.",
        });
      } finally {
        setResultDownloadDialogBusy(false);
      }
      return;
    }

    const localWorkspaceMoveForm = event.target.closest("[data-local-workspace-move-form]");
    if (localWorkspaceMoveForm) {
      event.preventDefault();
      if (!localWorkspaceMoveDialogState.entryId) {
        await showMessageDialog({
          title: "Local Workspace move unavailable",
          copy: "Reopen the move dialog so the Local Workspace file can be moved.",
        });
        return;
      }

      try {
        setLocalWorkspaceMoveDialogBusy(true);
        const movedEntry = await moveLocalWorkspaceExport(localWorkspaceMoveDialogState.entryId, {
          fileName: localWorkspaceMoveDialogState.fileName,
          folderPath: localWorkspaceMoveDialogState.folderPath,
        });
        closeDialog(localWorkspaceMoveDialog(), "confirm");
        if (movedEntry) {
          await showMessageDialog({
            title: "Local Workspace file moved",
            copy: `${movedEntry.fileName} was moved to ${localWorkspaceDisplayPath(movedEntry.folderPath)} in this browser.`,
          });
        }
      } catch (error) {
        console.error("Failed to move the Local Workspace file.", error);
        await showMessageDialog({
          title: "Local Workspace move failed",
          copy:
            error instanceof Error
              ? error.message
              : "The Local Workspace file could not be moved.",
        });
      } finally {
        setLocalWorkspaceMoveDialogBusy(false);
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

  if (await handleWorkbenchNavigationClick(event)) {
    return;
  }

  if (await serviceConsumptionUi.handleClick(event)) {
    return;
  }

  if (handleCsvIngestionClick(event)) {
    return;
  }

  if (await handleCreateNotebookClick(event)) {
    return;
  }

  if (await handleSourceSidebarClick(event)) {
    return;
  }

  if (await handleNotebookWorkspaceClick(event)) {
    return;
  }

  if (await handleRenameFolderClick(event)) {
    return;
  }

  if (await handleDeleteFolderClick(event)) {
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

  if (await handleAddFolderClick(event)) {
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

  syncActiveNotebookSelection(event);
});

document.body.addEventListener("focusin", (event) => {
  handleNotebookWorkspaceFocusIn(event);
});

document.body.addEventListener("input", (event) => {
  if (handleCsvIngestionInput(event)) {
    return;
  }

  if (handleNotebookWorkspaceInput(event)) {
    return;
  }

  const resultExportFileName = event.target.closest("[data-result-export-file-name]");
  if (resultExportFileName) {
    resultExportDialogState.fileName = resultExportFileName.value;
    syncResultExportSelectionState();
    return;
  }

  const resultDownloadFileName = event.target.closest("[data-result-download-file-name]");
  if (resultDownloadFileName) {
    resultDownloadDialogState.fileName = resultDownloadFileName.value;
    syncResultDownloadDialogState();
    return;
  }

  const exportSettingInput = event.target.closest("[data-export-setting]");
  if (exportSettingInput) {
    const sharedDialog = resultExportDialog();
    const localDialog = localWorkspaceSaveDialog();
    const downloadDialog = resultDownloadDialog();
    if (sharedDialog?.contains(exportSettingInput)) {
      resultExportDialogState.exportSettings = normalizeResultExportSettings(
        resultExportDialogState.exportFormat,
        readResultExportSettings(sharedDialog, resultExportDialogState.exportFormat)
      );
      return;
    }
    if (localDialog?.contains(exportSettingInput)) {
      updateLocalWorkspaceSaveExportSettingsFromDialog();
      return;
    }
    if (downloadDialog?.contains(exportSettingInput)) {
      resultDownloadDialogState.exportSettings = normalizeResultExportSettings(
        resultDownloadDialogState.exportFormat,
        readResultExportSettings(downloadDialog, resultDownloadDialogState.exportFormat)
      );
      return;
    }
  }

  const localWorkspaceFolderPathInput = event.target.closest("[data-local-workspace-folder-path]");
  if (localWorkspaceFolderPathInput) {
    updateLocalWorkspaceSaveFolderPath(localWorkspaceFolderPathInput.value);
    return;
  }

  const localWorkspaceFileNameInput = event.target.closest("[data-local-workspace-file-name]");
  if (localWorkspaceFileNameInput) {
    updateLocalWorkspaceSaveFileName(localWorkspaceFileNameInput.value);
    return;
  }

  const localWorkspaceMoveFolderPathInput = event.target.closest(
    "[data-local-workspace-move-folder-path]"
  );
  if (localWorkspaceMoveFolderPathInput) {
    updateLocalWorkspaceMoveFolderPath(localWorkspaceMoveFolderPathInput.value);
    return;
  }

  const localWorkspaceMoveFileNameInput = event.target.closest(
    "[data-local-workspace-move-file-name]"
  );
  if (localWorkspaceMoveFileNameInput) {
    updateLocalWorkspaceMoveFileName(localWorkspaceMoveFileNameInput.value);
    return;
  }
});

document.body.addEventListener("click", (event) => {
  handleNotebookWorkspaceSharedToggleClick(event);
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

document.body.addEventListener("change", async (event) => {
  if (handleCsvIngestionChange(event)) {
    return;
  }

  if (handleNotebookWorkspaceChange(event)) {
    return;
  }

  if (await serviceConsumptionUi.handleChange(event)) {
    return;
  }

  const exportFormatSelect = event.target.closest("[data-export-format-select]");
  if (exportFormatSelect instanceof HTMLSelectElement) {
    const sharedDialog = resultExportDialog();
    const localDialog = localWorkspaceSaveDialog();
    const downloadDialog = resultDownloadDialog();
    if (sharedDialog?.contains(exportFormatSelect)) {
      updateResultExportFormat(exportFormatSelect.value);
      return;
    }
    if (localDialog?.contains(exportFormatSelect)) {
      updateLocalWorkspaceSaveExportFormat(exportFormatSelect.value);
      return;
    }
    if (downloadDialog?.contains(exportFormatSelect)) {
      updateResultDownloadFormat(exportFormatSelect.value);
      return;
    }
  }

  const exportSettingInput = event.target.closest("[data-export-setting]");
  if (exportSettingInput) {
    const sharedDialog = resultExportDialog();
    const localDialog = localWorkspaceSaveDialog();
    const downloadDialog = resultDownloadDialog();
    if (sharedDialog?.contains(exportSettingInput)) {
      resultExportDialogState.exportSettings = normalizeResultExportSettings(
        resultExportDialogState.exportFormat,
        readResultExportSettings(sharedDialog, resultExportDialogState.exportFormat)
      );
      return;
    }
    if (localDialog?.contains(exportSettingInput)) {
      updateLocalWorkspaceSaveExportSettingsFromDialog();
      return;
    }
    if (downloadDialog?.contains(exportSettingInput)) {
      resultDownloadDialogState.exportSettings = normalizeResultExportSettings(
        resultDownloadDialogState.exportFormat,
        readResultExportSettings(downloadDialog, resultDownloadDialogState.exportFormat)
      );
      return;
    }
  }
});

document.body.addEventListener(
  "focusout",
  (event) => {
    handleNotebookWorkspaceSummaryFocusOut(event);
  },
  true
);

document.body.addEventListener("keydown", (event) => {
  handleNotebookWorkspaceSummaryEscapeKeydown(event);
});

document.body.addEventListener("dragstart", (event) => {
  handleNotebookDragStart(event);
});

document.body.addEventListener("dragover", (event) => {
  if (handleCsvDragOver(event)) {
    return;
  }

  handleNotebookDragOver(event);
});

document.body.addEventListener("dragleave", (event) => {
  handleCsvDragLeave(event);
});

document.body.addEventListener("drop", (event) => {
  handleCsvDrop(event);
});

document.body.addEventListener("drop", (event) => {
  handleNotebookDrop(event);
});

document.body.addEventListener("dragend", () => {
  handleNotebookDragEnd();
});

document.body.addEventListener(
  "toggle",
  (event) => {
    handleNotebookTreeToggle(event);
  },
  true
);

document.body.addEventListener("keydown", (event) => {
  handleNotebookWorkspaceTagInputKeydown(event);
});

document.body.addEventListener("keydown", async (event) => {
  await handleNotebookWorkspaceRenameTitleKeydown(event);
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
  serviceConsumptionUi.initializeCurrentPage().catch((error) => {
    console.error("Failed to initialize the service-consumption page after a partial swap.", error);
  });

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

  if (window.location.pathname === "/loader-workbench") {
    try {
      await openLoaderWorkbench();
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore the Loader Workbench from browser history.", error);
      }
    }
    return;
  }

  if (window.location.pathname === "/ingestion-workbench") {
    try {
      await openIngestionWorkbench();
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore the Ingestion Workbench from browser history.", error);
      }
    }
    return;
  }

  if (window.location.pathname === "/service-consumption") {
    try {
      await loadServiceConsumptionPage({ pushHistory: false });
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore service consumption from browser history.", error);
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
  loadServiceConsumptionState({
    windowRange: serviceConsumptionPageRoot() ? serviceConsumptionUi.currentWindow() : "24h",
  }).catch((error) => {
    console.error("Failed to load service consumption metrics.", error);
  }),
];

if (initialWorkspaceMode === "loader") {
  initialLoadTasks.push(
    loadDataGeneratorCatalog().catch((error) => {
      console.error("Failed to load data generators.", error);
    })
  );
}

Promise.allSettled(initialLoadTasks)
  .finally(() => {
    ensureRealtimeEventsEventSource();
    const initialSidebarMode = initialWorkspaceMode === "loader" ? "loader" : "notebook";
    refreshSidebar(initialSidebarMode).catch((error) => {
      console.error("Failed to refresh the sidebar during startup.", error);
    });

    if (initialWorkspaceMode === "loader") {
      renderIngestionWorkbench();
      renderDataGenerationMonitor();
      renderQueryNotificationMenu();
      return;
    }

    if (initialWorkspaceMode === "ingestion") {
      showIngestionLanding();
      renderQueryNotificationMenu();
      return;
    }

    if (serviceConsumptionPageRoot()) {
      serviceConsumptionUi.initializeCurrentPage().catch((error) => {
        console.error("Failed to initialize the service-consumption page.", error);
      });
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

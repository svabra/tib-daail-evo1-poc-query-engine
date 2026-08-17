import { EditorView, basicSetup } from "../vendor/codemirror.bundle.mjs";
import { sql, PostgreSQL } from "../vendor/lang-sql.bundle.mjs";
import {
  ensureAboutDialog,
  ensureFeatureListDialog,
  ensureNotebookShareDialog,
  ensureQueryExplainDialog,
  ensureResultDownloadDialog,
  ensureResultExportDialog,
  localWorkspaceMoveDialog,
  localWorkspaceSaveDialog,
  queryExplainDialog,
  resultDownloadDialog,
  resultExportDialog,
} from "./dialogs.js?v=2026-08-17-feature-list-1";
import {
  closeDialog,
  showConfirmDialog,
  showFolderNameDialog,
  showMessageDialog,
} from "./dialog-manager.js";
import { createIngestionController } from "./ingestion-controller.js";
import { createIngestionUi } from "./ingestion-ui.js";
import { createHomeUi } from "./home-ui.js";
import { createFeatureListController } from "./feature-list-controller.js?v=2026-08-17-feature-list-1";
import { initializeWorkbenchExpertSearch } from "./expert-search.js";
import {
  initializeDaaifDemoIdentity,
  syncDaaifFederalNavigation,
} from "./demo-identity.js";
import { initializeHomeNotebookSearch } from "./home-notebook-search.js";
import { createCsvIngestionController } from "./ingestion-types/csv/index.js";
import { createFileIngestionController } from "./ingestion-types/file/index.js";
import { createDataProductsController } from "./data-products-controller.js";
import { createDataProductsSampleContracts } from "./data-products-sample-contracts.js";
import { createDataProductsUi } from "./data-products-ui.js";
import { createDataExchangeController } from "./data-exchange-controller.js";
import { createDataSourceExplorerController } from "./data-source-explorers/controller.js";
import { createDownloadJobsController } from "./download-jobs-controller.js";
import { createS3DeleteJobsController } from "./s3-delete-jobs-controller.js";
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
import { resultStorageExportTarget } from "./data-exporters/result-storage-export-target.js";
import { createNotebookModel } from "./notebook-model.js";
import { createNotebookStagePipelineController } from "./notebook-stage-pipeline-controller.js?v=2026-08-16-pipeline-ui-1";
import { createNotebookWorkspaceMarkup } from "./notebook-workspace-markup.js?v=2026-08-16-pipeline-ui-1";
import { createNotebookWorkspaceController } from "./notebook-workspace-controller.js";
import { createNotebookUrlHelpers } from "./notebook-url-helpers.js";
import { createNotebookTreeController } from "./notebook-tree-controller.js";
import { createNotebookTreeState } from "./notebook-tree-state.js";
import { createNotebookTreeUi } from "./notebook-tree-ui.js";
import { pythonLanguageSupport } from "./python-editor-language.js";
import {
  applyOptimisticPythonJobSnapshot,
  createPythonJobState,
  loadPythonJobsState as requestPythonJobsState,
  normalizePythonJob,
  pythonJobElapsedMs,
  pythonJobIsRunning,
  pythonJobStatusCopy,
} from "./python-job-state.js";
import { createPythonUi } from "./python-ui.js";
import { createPopupMenuManager } from "./popup-menu-manager.js";
import { createQueryCompareController } from "./query-compare-controller.js";
import {
  dataGenerationMonitorCount,
  dataGenerationMonitorList,
  dataSourceExplorerPageRoot,
  dataExchangePageRoot,
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
  queryRunsPageRoot,
  queryWorkbenchDataSourcesPageRoot,
  queryWorkbenchEntryPageRoot,
  runbookFolders,
  serviceConsumptionPageRoot,
  settingsMenu,
  shellRoot,
  sidebarQueryCounts,
  sidebarToggles,
  sseConnectionStatusIndicator,
  sourceInspector,
  sourceInspectorPanel,
  sourceObjectNodes,
  workbenchExpertSearchPageRoot,
} from "./dom-query-helpers.js";
import { createSourceInspectorController } from "./source-inspector-controller.js";
import { createSourceInspectorUi } from "./source-inspector-ui.js";
import { createQueryInsights } from "./query-insights.js";
import { createQueryResourceChartsController } from "./query-resource-charts.js";
import { createQueryRunsController } from "./query-runs-controller.js";
import { createQuerySourceValidationController } from "./query-source-validation-controller.js";
import { createQueryUi } from "./query-ui.js";
import { createQueryWorkbenchEntryController } from "./query-workbench-entry-controller.js";
import { createAppTooltipController } from "./app-tooltip-controller.js";
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
import { createRealtimeController } from "./realtime-controller.js?v=2026-08-16-pipeline-ui-1";
import { createRealtimeConnectionStatusController } from "./realtime-connection-status-controller.js";
import { createServiceConsumptionUi } from "./service-consumption-ui.js?v=2026-04-19-service-mix-refresh-2";
import { createSidebarLayoutManager } from "./sidebar-layout-manager.js";
import { createSidebarRefreshController } from "./sidebar-refresh-controller.js";
import { createSidebarSearchFilter } from "./sidebar-search-filter.js";
import { createWorkspaceScrollManager } from "./workspace-scroll-manager.js";
import { createWorkspaceNavigationEpoch } from "./workspace-navigation-epoch.js";
import {
  captureIngestionWorkbenchNavigationState,
  restoreIngestionWorkbenchNavigationState,
} from "./ingestion-workbench-navigation-state.js";
import { createVisibilityAwareClock } from "./visibility-clock.js";
import {
  ensureFeatureReleaseNotes,
  ensureNotebookEditorMetadata,
} from "./workbench-metadata-cache.js";
import {
  accessModeForDataSources,
  accessModeHintForDataSources,
  normalizeS3BucketNameForCreate,
  normalizeDataSources,
  normalizeSourceObjectFields,
  parseDefaultDataSources,
  readSourceOptions,
  sourceClassificationDisplayText,
  sourceComputationModeDisplayText,
  sourceComputationModeTooltipText,
  dataProductSourceDescriptorFromSourceObject,
  dataProductSourceDescriptorFromSourceSchema,
  sourceIdFromLegacyTargetLabel,
  sourceLabelsForIds,
  sourceObjectDisplayKind,
  sourceObjectDisplayName,
  sourceObjectDdlDescriptor,
  sourceObjectS3GeneratedDownloadDescriptor,
  sourceObjectS3DeleteDescriptor,
  sourceObjectS3DownloadDescriptor,
  sourceDuckdbReference,
  sourceQueryDescriptor,
  sourceQuerySql,
  sourceSchemaS3BucketDescriptor,
  sourceStorageTooltipForIds,
} from "./source-metadata-utils.js";
import { createSourceQueryActions } from "./source-query-actions.js";
import { createSourceSidebarClickController } from "./source-sidebar-click-controller.js";
import { createSourceTreeController } from "./source-tree-controller.js";
import { formatSqlText } from "./sql-formatter.js";
import { createWorkbenchNavigationController } from "./workbench-navigation-controller.js?v=2026-08-17-feature-list-1";
import { createWorkbenchStorage } from "./workbench-storage.js";

createAppTooltipController().install();
initializeDaaifDemoIdentity();

const editorRegistry = new WeakMap();
const editorSizingRegistry = new WeakMap();
let draggedNotebook = null;
const workspaceNavigation = createWorkspaceNavigationEpoch();
let applyingNotebookState = false;
let activeCellId = null;
let queryJobsStateVersion = null;
let queryJobsSnapshot = [];
let queryJobsSummary = { runningCount: 0, totalCount: 0 };
let queryPerformanceState = { recent: [], stats: {} };
let queryJobsReconcileHandle = null;
let queryJobsReconcileInFlight = false;
let sidebarNotebookTreeLoadPromise = null;
let sidebarRunbookTreeLoadPromise = null;
const queryJobsReconcileInitialDelayMs = 1500;
const queryJobsReconcilePollMs = 4000;
const collapsedQueryResultKeys = new Set();
const visibleQueryResultChartKeys = new Set();
const visibleQueryTimingDetailKeys = new Set();
let pythonJobsStateVersion = null;
let pythonJobsSnapshot = [];
let pythonJobsSummary = { runningCount: 0, totalCount: 0 };
let realtimeEventsEventSource = null;
let serviceConsumptionStateVersion = null;
let clientConnectionsStateVersion = 0;
let clientConnectionsCount = 0;
let dataGeneratorsCatalog = [];
let dataGenerationJobsStateVersion = null;
let dataGenerationJobsSnapshot = [];
let dataGenerationJobsSummary = { runningCount: 0, totalCount: 0 };
let downloadJobsStateVersion = null;
let downloadJobsSnapshot = [];
let downloadJobsSummary = { runningCount: 0, readyCount: 0, totalCount: 0 };
let s3DeleteJobsStateVersion = null;
let s3DeleteJobsSnapshot = [];
let s3DeleteJobsSummary = { runningCount: 0, totalCount: 0 };
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
const sharedNotebookActivityTouchHandles = new Map();
const notebookDeletionInProgressIds = new Set();
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
  operationKind: "move",
  fileName: "",
  folderPath: "",
  moving: false,
  createdFolderPaths: [],
  destinationKind: "local",
  selectedBucket: "",
  selectedPrefix: "",
  s3Loaded: false,
  loadingSharedWorkspace: false,
  sharedWorkspaceLoadError: "",
};
const queryExplainDialogState = {
  payload: null,
  activeTab: "briefing",
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
const sharedNotebookFolderName = "Shared Notebooks";
const localNotebookPrefix = "local-notebook-";
const sharedNotebookPrefix = "shared-notebook-";
const localCellPrefix = "local-cell-";
const initialSqlEditorRows = 5;
const populatedSqlEditorRows = 10;
const defaultSqlEditorAutoRows = 10;
const queryJobTerminalStatuses = new Set(["completed", "failed", "cancelled", "canceled", "aborted", "incomplete"]);
const queryClientTimingStarts = new Map();
const acknowledgedQueryClientTimings = new Set();
const dataGenerationTerminalStatuses = new Set(["completed", "failed", "cancelled", "canceled", "aborted", "incomplete"]);
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
  copyLocalWorkspaceEntryToS3,
  deleteLocalWorkspaceQuerySource,
  loadLocalWorkspaceSourceFields,
  moveLocalWorkspaceEntryToS3,
  preparePythonExecution: prepareLocalWorkspacePythonExecution,
  prepareQuerySql: prepareLocalWorkspaceQuerySql,
  syncLocalWorkspaceEntry,
  validateLocalWorkspaceAliases,
} = createLocalWorkspaceQueryBridge({
  getLocalWorkspaceExport,
  isLocalWorkspaceRelation,
  listLocalWorkspaceExports,
  localWorkspaceEntryIdFromRelation,
  localWorkspaceRelation,
  normalizeSourceObjectFields,
  workbenchClientId,
});

const {
  notebookUrl,
  pushDataExchangeHistory,
  pushDataProductsHistory,
  pushHomeHistory,
  pushNotebookHistory,
  pushQueryRunsHistory,
  pushQueryWorkbenchDataSourceExplorerHistory,
  pushQueryWorkbenchDataSourcesHistory,
  pushQueryWorkbenchHistory,
  pushServiceConsumptionHistory,
  queryWorkbenchDataSourceExplorerUrl,
  queryWorkbenchDataSourcesUrl,
} = createNotebookUrlHelpers({ isLocalNotebookId });

const serviceConsumptionUi = createServiceConsumptionUi({
  fetchJsonOrThrow,
  formatByteCount,
});

const { previewContractMarkup } = createDataProductsSampleContracts({
  escapeHtml,
});

const {
  dataProductCardNodes,
  dataProductSearchEmpty,
  dataProductSearchInput,
  dataProductsPageRoot,
  ensureEditDialog,
  ensurePublicationDialog,
  readDataProductSourceOptions,
} = createDataProductsUi();

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
  createSharedWorkspaceBucketFromMoveDialog,
  createSharedWorkspaceFolderFromMoveDialog,
  loadLocalWorkspaceMoveS3ExplorerNode,
  openLocalWorkspaceCopyDialog,
  openLocalWorkspaceMoveDialog,
  openLocalWorkspaceSaveDialog,
  renderLocalWorkspaceMoveFolderList,
  renderLocalWorkspaceSaveFolderList,
  revealLocalWorkspaceMoveS3Location,
  setLocalWorkspaceMoveDialogBusy,
  setLocalWorkspaceSaveDialogBusy,
  syncLocalWorkspaceMoveDialogState,
  syncLocalWorkspaceSaveDialogState,
  syncOpenLocalWorkspaceMoveDialog,
  syncOpenLocalWorkspaceSaveDialog,
  updateLocalWorkspaceMoveDestinationKind,
  updateLocalWorkspaceMoveFileName,
  updateLocalWorkspaceMoveFolderPath,
  updateLocalWorkspaceMoveS3Location,
  updateLocalWorkspaceSaveExportFormat,
  updateLocalWorkspaceSaveExportSettingsFromDialog,
  updateLocalWorkspaceSaveFileName,
  updateLocalWorkspaceSaveFolderPath,
} = createLocalWorkspaceDialogController({
  allLocalWorkspaceFolderPaths,
  closestExistingLocalWorkspaceFolderPath,
  createLocalWorkspaceFolder,
  currentWorkspaceMode,
  defaultQueryResultExportFilename,
  fetchJsonOrThrow,
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
  renderS3ExplorerChildrenMarkup: s3ExplorerPickerChildrenMarkup,
  refreshSidebar: (mode) => refreshSidebar(mode),
  showFolderNameDialog,
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

const { pythonJobById, pythonJobForCell, pythonJobForResultActionTarget } = createPythonJobState({
  getPythonJobsSnapshot: () => pythonJobsSnapshot,
  workspaceNotebookId,
});

const { decorateQueryJobsWithInsights } = createQueryInsights({
  compareQueryJobsByCompletedAt,
  formatQueryDuration,
  normalizeDataSources,
  sourceLabelsForIds,
});

const {
  autosizeEditor,
  markEditorInteracted,
  resetEditorManualSizing,
} = createEditorAutosizeManager({
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
  getQueryRoot: resultExportDialog,
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
  queryResourceSparklineMarkup,
} = createQueryUi({
  escapeHtml,
  formatQueryDuration,
  formatQueryTimestamp,
  isQueryResultCollapsed,
  isQueryResultChartsVisible,
  isQueryTimingDetailsVisible,
  queryJobElapsedMs,
  queryJobEventDateTimeCopy,
  queryJobIsRunning,
  queryJobStatusCopy,
});

const { pythonResultPanelMarkup } = createPythonUi({
  escapeHtml,
  formatQueryDuration,
  pythonJobElapsedMs,
  pythonJobIsRunning,
  pythonJobStatusCopy,
});

let notebookStagePipelineController = null;
const preparedSqlViewCache = new WeakMap();
let cellSourceNavigationMenu = null;
let cellSourceNavigationChoices = [];

const querySourceValidationController = createQuerySourceValidationController({
  cellLanguageForCellRoot,
  selectedDataSourcesForCell,
  sourceExistenceValidationEnabledForCell: cellSourceExistenceValidationEnabled,
  validatePipelineStageAliases: (cellRoot, sql) =>
    notebookStagePipelineController?.validateStageAliasesForCell?.(cellRoot, sql) ?? {
      aliases: [],
      localRelations: {},
      missingAliases: [],
      validationSql: String(sql || ""),
    },
  validateLocalWorkspaceAliases,
});

const queryResourceChartsController = createQueryResourceChartsController();
queryResourceChartsController.start();

const realtimeConnectionStatusController = createRealtimeConnectionStatusController({
  getIndicator: sseConnectionStatusIndicator,
});

const queryRunsController = createQueryRunsController({
  escapeHtml,
  fetchJsonOrThrow,
  formatByteCount,
  formatQueryDuration,
  formatQueryTimestamp,
  queryResourceSparklineMarkup,
});

const queryWorkbenchEntryController = createQueryWorkbenchEntryController({
  escapeHtml,
  fetchJsonOrThrow,
  formatRelativeTimestamp,
  notebookLinks,
  readNotebookActivity,
  workbenchClientId,
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
  initializeHomeNotebookSearch,
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
  ensureRootSharedNotebooksFolder,
  ensureRootUnassignedFolder,
  folderCanDelete,
  folderCanEdit,
  folderIsShared,
  folderLabel,
  initializeNotebookTree,
  isUnassignedFolder,
  notebookDefaultFolderPath,
  persistNotebookTree,
  revealNotebookBranch,
  resolveAddTarget,
  resolveDropTarget,
  rootUnassignedFolder,
  setFolderShared,
  syncRootUnassignedFolder,
  treeFolderPath,
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
  sharedNotebookFolderName,
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
    isLocalWorkspaceSourceObject,
    refreshSidebar,
    requestCellRun,
    selectSourceObject,
    setActiveCellId: (cellId) => {
      activeCellId = cellId;
    },
    setNotebookCells,
    setSelectedSourceObjectState,
  });

const dataProductsController = createDataProductsController({
  ensureEditDialog,
  ensurePublicationDialog,
  fetchJsonOrThrow,
  getCardNodes: dataProductCardNodes,
  getPageRoot: dataProductsPageRoot,
  getSearchEmpty: dataProductSearchEmpty,
  getSearchInput: dataProductSearchInput,
  loadDataProductsPage,
  previewContractMarkup,
  readSourceOptions: readDataProductSourceOptions,
  showConfirmDialog,
  showMessageDialog,
});

const downloadJobsController = createDownloadJobsController({
  escapeHtml,
  fetchJsonOrThrow,
  formatByteCount,
  formatRelativeTimestamp,
  showMessageDialog,
  onStateChanged: syncDownloadJobsUi,
});

const s3DeleteJobsController = createS3DeleteJobsController({
  blinkSourceCatalog: (...args) => blinkSourceCatalog(...args),
  currentWorkspaceMode,
  escapeHtml,
  fetchJsonOrThrow,
  getDeleteDialogOptions: s3ExplorerDeleteDialogOptions,
  getPreferredLocationAfterDelete: s3ExplorerPreferredLocationAfterDelete,
  loadS3ExplorerRoot,
  onStateChanged: syncS3DeleteJobsUi,
  refreshActiveDataSourceViews: async () => {
    await refreshActiveDataSourceWorkbenchBrowser();
    if (dataSourceExplorerPageRoot()) {
      await dataSourceExplorerController.initializeCurrentPage();
    }
  },
  refreshSidebar,
  setPendingDeleteState: setS3PendingDeleteState,
  setSidebarSourceOperationStatus,
  showConfirmDialog,
  showMessageDialog,
});

const dataSourceExplorerController = createDataSourceExplorerController({
  allLocalWorkspaceFolderPaths,
  copySourceDuckdbReference,
  copySourceQueryPath,
  downloadLocalWorkspaceExportFromSource,
  downloadSourceObjectDdl,
  downloadSourceS3GeneratedParts,
  downloadSourceS3Object,
  downloadJobsController,
  escapeHtml,
  fetchJsonOrThrow,
  formatByteCount,
  getPageRoot: dataSourceExplorerPageRoot,
  listLocalWorkspaceExports,
  localWorkspaceDisplayPath,
  localWorkspaceFolderName,
  localWorkspaceRelation,
  normalizeLocalWorkspaceFolderPath,
  openDataProductPublishDialog,
  prepareSourceS3Download,
  querySourceInCurrentNotebook,
  querySourceInNewNotebook,
  renderLocalWorkspaceSidebarEntries: () => renderLocalWorkspaceSidebarEntries(),
  showMessageDialog,
  viewSourceData,
});

const featureListController = createFeatureListController({
  applicationVersion,
  ensureDialog: ensureFeatureListDialog,
  ensureReleaseNotes: ensureFeatureReleaseNotes,
  escapeHtml,
});

const {
  handleClick: handleWorkbenchNavigationClick,
} = createWorkbenchNavigationController({
  applySidebarCollapsedState,
  browseDataSourceInSidebar,
  closeSettingsMenus,
  getClearVisibleNotifications: () => clearVisibleNotifications,
  getQueryNotificationMenu: queryNotificationMenu,
  openDataExchangeWorkbench,
  openDataProductsWorkbench,
  openHomePage: loadHomePage,
  openLoaderWorkbench,
  loadQueryWorkbenchDataSourceExplorer,
  loadQueryWorkbenchDataSources,
  loadQueryWorkbenchEntry,
  openServiceConsumptionPage,
  openIngestionWorkbench,
  openQueryWorkbench,
  openQueryWorkbenchDataSources,
  openQueryWorkbenchNavigation,
  openQueryRunsPage,
  openRuntimeStorageDialog,
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
  loadDeferredSidebarSourceTree,
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
  isWorkspaceNavigationSettled: () => {
    const panel = document.getElementById("workspace-panel");
    const epochMarker = panel?.dataset?.workspaceNavigationEpoch;
    return epochMarker
      ? Number(epochMarker) === workspaceNavigation.currentEpoch()
      : workspaceNavigation.currentEpoch() === 0;
  },
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

const dataExchangeController = createDataExchangeController({
  createLocalWorkspaceEntryId,
  downloadJobsController,
  escapeHtml,
  fetchJsonOrThrow,
  formatByteCount,
  formatRelativeTimestamp,
  openQueryWorkbenchDataSources,
  refreshSidebar,
  renderLocalWorkspaceSidebarEntries,
  saveLocalWorkspaceExport,
  showConfirmDialog,
  showMessageDialog,
  startDataExchangePreparedDownload,
  syncLocalWorkspaceEntry,
});

const {
  handleCsvIngestionClick,
  handleCsvDragLeave,
  handleCsvDragOver,
  handleCsvDrop,
  handleCsvIngestionChange,
  handleCsvIngestionInput,
  handleCsvIngestionPaste,
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
  handleFileDragLeave,
  handleFileDragOver,
  handleFileDrop,
  handleFileIngestionChange,
  handleFileIngestionInput,
  renderFileIngestionWorkbench,
  submitFileIngestionForm,
} = createFileIngestionController({
  ensureLocalWorkspaceFolderPath,
  escapeHtml,
  formatByteCount,
  localWorkspaceDisplayPath,
  localWorkspaceRelation,
  normalizeLocalWorkspaceFolderPath,
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
  copySourceDuckdbReference,
  copySourceQueryPath,
  createLocalWorkspaceFolder,
  createLocalWorkspaceFolderFromDialog,
  createLocalWorkspaceFolderFromMoveDialog,
  createSharedWorkspaceBucketFromMoveDialog,
  createSharedWorkspaceFolderFromMoveDialog,
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
  downloadSourceObjectDdl,
  downloadSourceS3GeneratedParts,
  downloadSourceS3Object,
  loadS3ExplorerNode,
  loadLocalWorkspaceMoveS3ExplorerNode,
  openDataProductPublishDialog,
  openLocalWorkspaceCopyDialog,
  openLocalWorkspaceMoveDialog,
  openLocalWorkspaceSaveDialog,
  openNotebookForQueryJob,
  openResultDownloadDialog,
  openResultExportDialog,
  prepareSourceS3Download,
  queryJobForResultActionTarget,
  queryNotificationMenu,
  querySourceInCurrentNotebook,
  querySourceInNewNotebook,
  revealS3ExplorerLocation,
  revealLocalWorkspaceMoveS3Location,
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
  updateLocalWorkspaceMoveS3Location,
  updateLocalWorkspaceSaveFolderPath,
  viewSourceData,
});

const {
  activeWorkspaceMetaRoot,
  createInitialNotebookVersion,
  normalizeCellEntry,
  normalizeCellStage,
  normalizeCellQueryOptions,
  normalizeCellLanguage,
  normalizeNotebookCells,
  normalizePipelinePaths,
  normalizeNotebookPipelineMode,
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
  normalizeCellStage,
  normalizeNotebookCells,
  normalizePipelinePaths,
  normalizeTags,
  pythonResultPanelMarkup,
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
  openCacheHydrationDialog,
  openNotebookShareDialog,
  applyCellCacheHydrationToggle,
  queryOptionsForCellRoot,
  refreshCellCacheHydrationStatus,
  refreshQuerySourceValidationForCell: (cellRoot) => {
    querySourceValidationController.scheduleValidationForCell(cellRoot);
  },
  renameNotebook,
  restartPythonKernel,
  revealNotebookLink,
  saveNotebookVersion,
  setActiveCell,
  setCellDataSources,
  setCellLanguage,
  setCellQueryOptions,
  setCellDescriptor,
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

const queryCompareController = createQueryCompareController({
  cellLanguageForCellRoot,
  currentEditorSql,
  currentWorkspaceNotebookId,
  currentWorkspaceNotebookTitle,
  escapeHtml,
  normalizeCellLanguage,
  normalizeNotebookCells,
  normalizeNotebookTitleValue,
  notebookMetadata,
  queryOptionsForCellRoot,
  selectedDataSourcesForCell,
  truncateWords,
});

notebookStagePipelineController = createNotebookStagePipelineController({
  createCellId,
  escapeHtml,
  fetchJsonOrThrow,
  formatQueryDuration,
  getCurrentNotebookId: currentWorkspaceNotebookId,
  getNotebookMetadata: notebookMetadata,
  normalizeCellLanguage,
  normalizeCellStage,
  normalizePipelinePaths,
  normalizeNotebookPipelineMode,
  openPublishDialogForSource: async (source) => {
    await dataProductsController.openPublishDialog({
      source,
      lockSource: true,
      startStep: 2,
    });
  },
  refreshSidebar,
  requestCellRun,
  revealDataSourceSidebarBrowser,
  onPipelineNotificationStateChanged: (snapshot) => {
    renderQueryNotificationMenu();
    queryRunsController.refreshForMaterializedStagesSnapshot(snapshot);
  },
  setCellStage,
  setNotebookCells,
  setNotebookPipelinePaths,
  setNotebookPipelineMode,
  showConfirmDialog,
  showMessageDialog,
  syncResultStorageState: (cellRoot) => syncCellResultStorageState(cellRoot),
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
  handleToggleFolderSharedClick,
  handleRenameFolderClick,
} = createNotebookTreeController({
  applySidebarSearchFilter,
  clearDragState,
  clearDropTargets,
  createFolderNode,
  createNotebook,
  defaultFolderPermissions,
  deleteSharedNotebookFolder,
  deleteTreeFolder,
  deriveFolderId,
  applyWorkbenchTitle,
  dropTargetAcceptsNotebookDrop,
  folderCanDelete,
  folderCanEdit,
  folderIsShared,
  folderLabel,
  getCurrentWorkspaceMode: currentWorkspaceMode,
  getDraggedNotebook: () => draggedNotebook,
  getHomePageRoot: homePageRoot,
  getQueryWorkbenchEntryPageRoot: queryWorkbenchEntryPageRoot,
  isLocalNotebookId,
  isUnassignedFolder,
  notebookTreeRoot,
  persistNotebookTree,
  pushNotebookHistory,
  pushQueryWorkbenchHistory,
  refreshSidebar,
  resolveAddTarget,
  resolveDropTarget,
  resolveNotebookCreateTarget,
  setFolderShared,
  setDraggedNotebook: (notebook) => {
    draggedNotebook = notebook;
  },
  setSharedNotebookFolderVisibility,
  showConfirmDialog,
  showFolderNameDialog,
  syncRootUnassignedFolder,
  unassignedFolderName,
  updateFolderCounts,
  upsertSharedNotebookFolder,
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
  handleClick: handleIngestionClick,
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
  getDownloadNotificationItems: () =>
    downloadJobsController.notificationItems({
      dismissedKeys: dismissedNotificationKeys,
      notificationItemKey,
    }),
  getPipelineNotificationItems: () =>
    notebookStagePipelineController?.pipelineNotificationItems?.({
      dismissedKeys: dismissedNotificationKeys,
      notificationItemKey,
    }) ?? [],
  getS3DeleteNotificationItems: () =>
    s3DeleteJobsController.notificationItems({
      dismissedKeys: dismissedNotificationKeys,
      notificationItemKey,
    }),
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
  writeTextToClipboard,
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

  if (workbenchExpertSearchPageRoot()) {
    return "expert-search";
  }

  if (dataProductsPageRoot()) {
    return "data-products";
  }

  if (dataExchangePageRoot()) {
    return "data-exchange";
  }

  if (serviceConsumptionPageRoot()) {
    return "service-consumption";
  }

  if (queryRunsPageRoot()) {
    return "query-runs";
  }

  if (queryWorkbenchDataSourcesPageRoot()) {
    return "data-sources";
  }

  if (dataSourceExplorerPageRoot()) {
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
    .find((row) => row.querySelector(".app-version-overlay-label")?.textContent?.trim() === "DAAIF Factory")
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
    return "DAAIF Factory";
  }

  if (section === "expert-search") {
    return "DAAIF Factory - Expertensuche";
  }

  if (section === "data-sources") {
    return "DAAIF Factory - Data Source Workbench";
  }

  if (section === "data-products") {
    return "DAAIF Factory - Data Products Workbench";
  }

  if (section === "data-exchange") {
    return "DAAIF Factory - DataExchange Workbench";
  }

  if (section === "service-consumption") {
    return "DAAIF Factory - Service Consumption";
  }

  if (section === "query-runs") {
    return "DAAIF Factory - Query Monitoring";
  }

  if (section === "loader") {
    return "DAAIF Factory - Loader Workbench";
  }

  if (section === "ingestion") {
    return "DAAIF Factory - Ingestion Workbench";
  }

  return "DAAIF Factory - Query Workbench";
}

function browserTitleForNotebook(notebookTitle = "") {
  const normalizedTitle = String(notebookTitle || "").trim();
  return `DAAIF Factory - ${normalizedTitle || "Notebook"}`;
}

function workbenchBrowserTitle(section = currentWorkbenchSection()) {
  if (section === "query") {
    const workspaceRoot = document.querySelector("[data-workspace-notebook]");
    if (workspaceRoot) {
      return browserTitleForNotebook(currentWorkspaceNotebookTitle(workspaceRoot));
    }
  }

  return workbenchTitle(section);
}

function applyWorkbenchTitle(section = currentWorkbenchSection()) {
  const title = workbenchTitle(section);
  const brandTitle = document.querySelector(".brand-copy h1");
  if (brandTitle) {
    brandTitle.textContent = title;
  }
  if (typeof document !== "undefined") {
    document.title = workbenchBrowserTitle(section);
  }
  syncDaaifFederalNavigation(section);
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

function createLocalWorkspaceEntryId() {
  return `local-workspace-entry-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 9)}`;
}

function currentQueryState() {
  return {
    version: queryJobsStateVersion,
    snapshot: queryJobsSnapshot,
    summary: queryJobsSummary,
    performance: queryPerformanceState,
  };
}

function currentPythonState() {
  return {
    version: pythonJobsStateVersion,
    snapshot: pythonJobsSnapshot,
    summary: pythonJobsSummary,
  };
}

function clientObservedQueryElapsedMs(job) {
  const jobId = String(job?.jobId || "").trim();
  if (!jobId || !queryJobTerminalStatuses.has(String(job?.status || "").trim())) {
    return null;
  }

  const timingStart = queryClientTimingStarts.get(jobId);
  if (!timingStart || !Number.isFinite(Number(timingStart.startedPerf))) {
    return null;
  }

  const existingObservedMs = Number(timingStart.observedMs);
  if (Number.isFinite(existingObservedMs) && existingObservedMs >= 0) {
    return existingObservedMs;
  }

  const observedMs = Math.max(0, performance.now() - Number(timingStart.startedPerf));
  queryClientTimingStarts.set(jobId, {
    ...timingStart,
    observedMs,
  });
  return observedMs;
}

function normalizeQueryJobForDisplay(job) {
  const normalizedJob = normalizeQueryJob(job);
  if (!normalizedJob) {
    return null;
  }

  const observedMs = clientObservedQueryElapsedMs(normalizedJob);
  if (!Number.isFinite(Number(observedMs)) || Number(observedMs) < 0) {
    return normalizedJob;
  }

  return {
    ...normalizedJob,
    timings: {
      ...(normalizedJob.timings || {}),
      clientObservedMs: Number(observedMs),
    },
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
  getDownloadState: () => ({
    version: downloadJobsStateVersion,
    snapshot: downloadJobsSnapshot,
    summary: downloadJobsSummary,
  }),
  getPipelineNotificationItems: (options = {}) =>
    notebookStagePipelineController?.pipelineNotificationItems?.(options) ?? [],
  getPipelineNotificationSummary: () =>
    notebookStagePipelineController?.pipelineNotificationSummary?.() ?? {
      version: null,
      runningCount: 0,
      totalCount: 0,
    },
  getS3DeleteState: () => ({
    version: s3DeleteJobsStateVersion,
    snapshot: s3DeleteJobsSnapshot,
    summary: s3DeleteJobsSummary,
  }),
  getDismissedNotificationKeys: () => dismissedNotificationKeys,
  getQueryState: currentQueryState,
  normalizeDataGenerationJob,
  normalizeQueryJob: normalizeQueryJobForDisplay,
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
  querySourceValidationController,
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
    syncQueryJobsReconciliation();
  },
  sidebarQueryCounts,
  syncCellCacheHydrationJobState,
  writeDismissedNotificationKeys,
  workspaceNotebookId,
});

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

function showFeatureListDialog(trigger) {
  return featureListController.show(trigger);
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

function loadDeferredSidebarNotebookTree() {
  const placeholder = document.querySelector("[data-deferred-notebook-tree]");
  if (!placeholder) {
    return Promise.resolve();
  }
  if (sidebarNotebookTreeLoadPromise) {
    return sidebarNotebookTreeLoadPromise;
  }

  placeholder.classList.add("is-loading");
  sidebarNotebookTreeLoadPromise = refreshSidebar("notebook")
    .catch((error) => {
      console.error("Failed to load deferred notebook tree.", error);
    })
    .finally(() => {
      sidebarNotebookTreeLoadPromise = null;
    });
  return sidebarNotebookTreeLoadPromise;
}

function loadDeferredSidebarRunbookTree() {
  const placeholder = document.querySelector("[data-deferred-runbook-tree]");
  if (!placeholder) {
    return Promise.resolve();
  }
  if (sidebarRunbookTreeLoadPromise) {
    return sidebarRunbookTreeLoadPromise;
  }

  placeholder.classList.add("is-loading");
  sidebarRunbookTreeLoadPromise = refreshSidebar("loader", { force: true })
    .catch((error) => {
      console.error("Failed to load deferred Loader runbooks.", error);
    })
    .finally(() => {
      sidebarRunbookTreeLoadPromise = null;
    });
  return sidebarRunbookTreeLoadPromise;
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
  loadDeferredSidebarNotebookTree().finally(() => {
    notebookSection()?.setAttribute("open", "");
    if (notebookId) {
      revealNotebookLink(notebookId);
    }
  });
}

function openLoaderNavigation(generatorId = "") {
  setShellSidebarHidden(false);
  applySidebarCollapsedState(false);
  writeSidebarCollapsed(false);
  ingestionRunbookSection()?.setAttribute("open", "");
  loadDeferredSidebarRunbookTree().finally(() => {
    ingestionRunbookSection()?.setAttribute("open", "");
  });

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
    workbenchExpertSearchPageRoot() ||
    dataProductsPageRoot() ||
    dataExchangePageRoot() ||
    serviceConsumptionPageRoot() ||
    queryRunsPageRoot() ||
    queryWorkbenchEntryPageRoot() ||
    queryWorkbenchDataSourcesPageRoot() ||
    dataSourceExplorerPageRoot() ||
    currentWorkspaceMode() === "ingestion"
  ) {
    setShellSidebarHidden(true);
    return;
  }

  restoreSidebarVisibilityForWorkspace();
}

function sourceOperationStatusRoots() {
  return Array.from(document.querySelectorAll("[data-source-operation-status]"));
}

function sourceBrowserScopeRoot(node = null) {
  return node?.closest?.("[data-source-browser-scope]") || document;
}

function clearSidebarSourceOperationStatusTimer() {
  if (sidebarSourceOperationStatusClearHandle !== null) {
    window.clearTimeout(sidebarSourceOperationStatusClearHandle);
    sidebarSourceOperationStatusClearHandle = null;
  }
}

function renderSidebarSourceOperationStatus() {
  const roots = sourceOperationStatusRoots();
  if (!roots.length) {
    return;
  }

  roots.forEach((root) => {
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
  });
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

function flattenS3AliasSchema(schema) {
  if (Array.isArray(schema?.s3References)) {
    return schema.s3References
      .map((item) => {
        if (typeof item === "string") {
          return { label: item, detail: "S3 object" };
        }
        const label = String(item?.label || "").trim();
        return label
          ? {
              label,
              detail: String(item?.detail || "S3 object"),
              relation: String(item?.relation || ""),
            }
          : null;
      })
      .filter(Boolean);
  }

  const aliases = [];
  const root = schema?.s3;
  if (!root || typeof root !== "object" || Array.isArray(root)) {
    return aliases;
  }

  const visit = (node, parts) => {
    if (Array.isArray(node)) {
      if (parts.length >= 4) {
        aliases.push({ label: parts.join("."), detail: "Legacy S3 alias" });
      }
      return;
    }
    if (!node || typeof node !== "object") {
      return;
    }
    Object.keys(node)
      .sort()
      .forEach((part) => visit(node[part], [...parts, part]));
  };

  visit(root, ["s3"]);
  return aliases;
}

function s3AliasCompletionSource(schema) {
  const aliases = flattenS3AliasSchema(schema);
  const pgReferences = Array.isArray(schema?.pgReferences)
    ? schema.pgReferences
        .map((item) => {
          if (typeof item === "string") {
            return { label: item, detail: "PostgreSQL relation" };
          }
          const label = String(item?.label || "").trim();
          return label
            ? {
                label,
                detail: String(item?.detail || "PostgreSQL relation"),
                relation: String(item?.relation || ""),
              }
            : null;
        })
        .filter(Boolean)
    : [];
  const sourceReferences = [...aliases, ...pgReferences];
  if (!sourceReferences.length) {
    return () => null;
  }

  return (context) => {
    const match = context.matchBefore(/[A-Za-z0-9_.$"\/-]*/);
    const typed = String(match?.text || "").trim();
    const typedLower = typed.toLowerCase();
    if (
      !match ||
      !typed ||
      (!typedLower.startsWith("s3") && !typedLower.startsWith("pg"))
    ) {
      return null;
    }

    const options = [];
    const seen = new Set();

    sourceReferences.forEach((entry) => {
      const alias = String(entry?.label || "").trim();
      const aliasLower = alias.toLowerCase();
      const matchesPrefix = aliasLower.startsWith(typedLower);
      const compactAlias = aliasLower.replace(/["/._-]/g, "");
      const compactTyped = typedLower.replace(/["/._-]/g, "");
      const matchesCompact = compactTyped.length >= 3 && compactAlias.includes(compactTyped);

      if ((!matchesPrefix && !matchesCompact) || seen.has(alias)) {
        return;
      }

      seen.add(alias);
      options.push({
        label: alias,
        type: "table",
        apply: alias,
        detail: entry?.detail || (alias.startsWith("pg.") ? "PostgreSQL relation" : "S3 object"),
        boost: matchesPrefix ? 110 : 100,
      });
    });

    if (!options.length) {
      return null;
    }

    return {
      from: match.from,
      options,
      validFor: /^[A-Za-z0-9_.$"\/-]*$/,
    };
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

function queryExplainArray(values) {
  return Array.isArray(values) ? values.filter(Boolean) : [];
}

function queryExplainSummary(payload) {
  return payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
}

function queryExplainPlan(payload, key) {
  const plans = payload?.plans && typeof payload.plans === "object" ? payload.plans : {};
  const plan = plans[key] && typeof plans[key] === "object" ? plans[key] : {};
  return {
    label: String(plan.label || key || "Plan"),
    text: String(plan.text || ""),
    json: Array.isArray(plan.json) ? plan.json : [],
  };
}

function queryExplainListMarkup(values, emptyCopy) {
  const items = queryExplainArray(values);
  if (!items.length) {
    return `<p class="query-explain-empty">${escapeHtml(emptyCopy)}</p>`;
  }
  return `<ul class="query-explain-list">${items
    .map((item) => `<li>${escapeHtml(item)}</li>`)
    .join("")}</ul>`;
}

function queryExplainOperatorCountsMarkup(summary) {
  const counts = queryExplainArray(summary.operatorCounts);
  if (!counts.length) {
    return '<p class="query-explain-empty">No operator counts were returned.</p>';
  }
  return `
    <div class="query-explain-operator-grid">
      ${counts
        .slice(0, 12)
        .map(
          (entry) => `
            <span class="query-explain-operator-pill">
              <strong>${escapeHtml(entry.name)}</strong>
              <span>${escapeHtml(entry.count)}</span>
            </span>
          `
        )
        .join("")}
    </div>
  `;
}

function queryExplainEstimatedRowsMarkup(summary) {
  const rows = queryExplainArray(summary.estimatedRows);
  if (!rows.length) {
    return '<p class="query-explain-empty">DuckDB did not return estimated row counts for this plan.</p>';
  }
  return `
    <table class="query-explain-mini-table">
      <thead>
        <tr>
          <th>Operator</th>
          <th>Estimated rows</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .slice(0, 8)
          .map(
            (entry) => `
              <tr>
                <td>${escapeHtml(entry.operator)}</td>
                <td>${Number(entry.estimatedRows || 0).toLocaleString()}</td>
              </tr>
            `
          )
          .join("")}
      </tbody>
    </table>
  `;
}

function queryExplainSourcesMarkup(summary) {
  const sources = summary.sources && typeof summary.sources === "object" ? summary.sources : {};
  const relations = queryExplainArray(sources.relations);
  const buckets = queryExplainArray(sources.buckets);
  const dataSources = queryExplainArray(sources.dataSources);
  if (!relations.length && !buckets.length && !dataSources.length) {
    return '<p class="query-explain-empty">No catalog sources were detected for this statement.</p>';
  }
  const rows = [
    ...dataSources.map((value) => ["Data source", value]),
    ...relations.map((value) => ["Relation", value]),
    ...buckets.map((value) => ["S3 bucket", value]),
  ];
  return `
    <table class="query-explain-mini-table">
      <tbody>
        ${rows
          .map(
            ([label, value]) => `
              <tr>
                <th>${escapeHtml(label)}</th>
                <td>${escapeHtml(value)}</td>
              </tr>
            `
          )
          .join("")}
      </tbody>
    </table>
  `;
}

function queryExplainBriefingMarkup(payload) {
  const summary = queryExplainSummary(payload);
  return `
    <div class="query-explain-briefing">
      <section class="query-explain-panel">
        <h3>Plan Warnings</h3>
        ${queryExplainListMarkup(summary.warnings, "No warnings were generated.")}
      </section>
      <section class="query-explain-panel">
        <h3>Hints</h3>
        ${queryExplainListMarkup(summary.hints, "No hints were generated.")}
      </section>
      <section class="query-explain-panel">
        <h3>Optimization Notes</h3>
        ${queryExplainListMarkup(summary.optimizationNotes, "No optimizer notes were generated.")}
      </section>
      <section class="query-explain-panel">
        <h3>Operators</h3>
        ${queryExplainOperatorCountsMarkup(summary)}
      </section>
      <section class="query-explain-panel">
        <h3>Estimated Rows</h3>
        ${queryExplainEstimatedRowsMarkup(summary)}
      </section>
      <section class="query-explain-panel">
        <h3>Sources</h3>
        ${queryExplainSourcesMarkup(summary)}
      </section>
    </div>
  `;
}

function queryExplainPlanMarkup(payload, key) {
  const plan = queryExplainPlan(payload, key);
  const planText = plan.text || "DuckDB did not return this plan.";
  return `
    <div class="query-explain-plan-view">
      <h3>${escapeHtml(plan.label)}</h3>
      <pre class="query-explain-plan-text">${escapeHtml(planText)}</pre>
    </div>
  `;
}

function queryExplainRawJsonMarkup(payload) {
  const rawPayload = {
    summary: payload?.summary || {},
    plans: payload?.plans || {},
  };
  return `
    <div class="query-explain-plan-view">
      <h3>Raw JSON</h3>
      <pre class="query-explain-plan-text">${escapeHtml(JSON.stringify(rawPayload, null, 2))}</pre>
    </div>
  `;
}

function renderQueryExplainDialog() {
  const dialog = queryExplainDialog();
  const payload = queryExplainDialogState.payload;
  if (!dialog || !payload) {
    return;
  }

  const activeTab = queryExplainDialogState.activeTab || "briefing";
  const title = dialog.querySelector("[data-query-explain-title]");
  const copy = dialog.querySelector("[data-query-explain-copy]");
  const meta = dialog.querySelector("[data-query-explain-meta]");
  const body = dialog.querySelector("[data-query-explain-body]");

  if (title) {
    title.textContent = "DuckDB Query Plan";
  }
  if (copy) {
    copy.textContent = "Non-executing EXPLAIN output. Estimates can differ from actual runtime.";
  }
  if (meta) {
    const duration = Number(payload.durationMs || 0);
    meta.textContent = `DuckDB ${payload.duckdbVersion || "unknown"} | ${formatQueryDuration(duration)}`;
  }

  dialog.querySelectorAll("[data-query-explain-tab]").forEach((tab) => {
    const selected = tab.dataset.queryExplainTab === activeTab;
    tab.classList.toggle("is-active", selected);
    tab.setAttribute("aria-selected", selected ? "true" : "false");
  });

  if (body) {
    if (activeTab === "briefing") {
      body.innerHTML = queryExplainBriefingMarkup(payload);
    } else if (activeTab === "raw_json") {
      body.innerHTML = queryExplainRawJsonMarkup(payload);
    } else {
      body.innerHTML = queryExplainPlanMarkup(payload, activeTab);
    }
  }
}

function openQueryExplainDialog(payload) {
  const dialog = ensureQueryExplainDialog();
  queryExplainDialogState.payload = payload;
  queryExplainDialogState.activeTab = "briefing";
  renderQueryExplainDialog();
  if (typeof dialog.showModal === "function" && !dialog.open) {
    dialog.showModal();
  }
}

function setQueryExplainButtonBusy(button, busy) {
  if (!button) {
    return;
  }
  button.disabled = Boolean(busy);
  button.classList.toggle("is-loading", Boolean(busy));
  button.textContent = busy ? "Explaining..." : "Explain";
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
  const downloadableFiles = Array.isArray(generator.downloadableFiles)
    ? generator.downloadableFiles
        .map((file) => {
          if (!file || typeof file !== "object") {
            return null;
          }
          const downloadUrl = String(file.downloadUrl ?? file.href ?? "").trim();
          const targetPath = String(file.targetPath ?? "").trim();
          if (!downloadUrl || !targetPath) {
            return null;
          }
          return {
            fileName: String(file.fileName ?? "").trim(),
            label: String(file.label ?? file.fileName ?? "CSV-Datei").trim(),
            downloadUrl,
            targetPath,
            storageFormat: String(file.storageFormat ?? "").trim().toLowerCase(),
            storageFormatLabel: String(file.storageFormatLabel ?? "").trim(),
            storageFormatInstruction: String(file.storageFormatInstruction ?? "").trim(),
            replaceExisting: file.replaceExisting === true,
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
    downloadableFiles,
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
    canCancel: Boolean(job.canCancel),
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

function cellLanguageForCellRoot(cellRoot) {
  if (!(cellRoot instanceof Element)) {
    return "sql";
  }

  return normalizeCellLanguage(
    cellRoot.dataset.defaultCellLanguage ||
      cellRoot.querySelector("[data-editor-root]")?.dataset.editorLanguage ||
      cellRoot.querySelector("[data-editor-source]")?.dataset.editorLanguage ||
      "sql"
  );
}

function formCellLanguage(form) {
  return cellLanguageForCellRoot(form?.closest("[data-query-cell]") ?? null);
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

let cachedRuntimeInfo = null;

function readRuntimeInfo() {
  if (cachedRuntimeInfo !== null) {
    return cachedRuntimeInfo;
  }
  const script = document.getElementById("runtime-info");
  if (!script?.textContent) {
    cachedRuntimeInfo = {};
    return cachedRuntimeInfo;
  }
  try {
    const parsed = JSON.parse(script.textContent);
    cachedRuntimeInfo = parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch (_error) {
    cachedRuntimeInfo = {};
  }
  return cachedRuntimeInfo;
}

function slugForResultStoragePath(value, fallback = "notebook") {
  const slug = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  return slug || fallback;
}

function defaultResultStorageBucket() {
  const runtime = readRuntimeInfo();
  const runtimeBucket = String(runtime.s3_bucket || runtime.s3Bucket || "").trim();
  if (runtimeBucket) {
    return runtimeBucket;
  }
  const sourceBucket = document.querySelector("[data-s3-bucket]")?.dataset?.s3Bucket;
  return String(sourceBucket || "workspace").trim() || "workspace";
}

function proposedResultStorageS3Path(cellRoot) {
  const workspaceRoot = cellRoot?.closest?.("[data-workspace-notebook]");
  const notebookId = slugForResultStoragePath(workspaceNotebookId(workspaceRoot), "notebook");
  const cellId = slugForResultStoragePath(cellRoot?.dataset?.cellId || "", "cell");
  return `s3://${defaultResultStorageBucket()}/query-results/${notebookId}/${cellId}/result.parquet`;
}

function pipelineModeForCellRoot(cellRoot) {
  const workspaceRoot = cellRoot?.closest?.("[data-workspace-notebook]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  if (!notebookId) {
    return "exploration";
  }
  return normalizeNotebookPipelineMode(
    workspaceRoot?.dataset?.defaultPipelineMode ||
      workspaceRoot?.querySelector?.("[data-notebook-meta]")?.dataset?.defaultPipelineMode ||
      notebookMetadata(notebookId).pipelineMode
  );
}

function pipelineResultStorageForCellRoot(cellRoot) {
  return pipelineModeForCellRoot(cellRoot) === "pipeline" && cellLanguageForCellRoot(cellRoot) === "sql";
}

function currentCellStage(cellRoot) {
  const workspaceRoot = cellRoot?.closest?.("[data-workspace-notebook]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellId = String(cellRoot?.dataset?.cellId || "").trim();
  const cell = notebookMetadata(notebookId).cells?.find((item) => item.cellId === cellId);
  return normalizeCellStage(cell?.stage);
}

function stageOutputFileNameForCellRoot(cellRoot) {
  const stage = currentCellStage(cellRoot);
  const source = String(stage.outputFileName || stage.alias || stage.title || cellRoot?.dataset?.cellId || "stage")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_") || "stage";
  return source.toLowerCase().endsWith(".parquet") ? source : `${source}.parquet`;
}

function firstVirtualS3PathFromSql(sql) {
  const pattern =
    /(^|[^A-Za-z0-9_$])s3\s*\.\s*(?:"((?:[^"]|"")*)"|([A-Za-z_][A-Za-z0-9_$]*))\s*\.\s*(?:"((?:[^"]|"")*)"|([A-Za-z0-9_./*?[\]${}-]+))/gi;
  const match = pattern.exec(String(sql || ""));
  if (!match) {
    return "";
  }
  const bucket = String(match[2] || match[3] || "").replace(/""/g, '"').trim();
  const key = String(match[4] || match[5] || "").replace(/""/g, '"').trim();
  return bucket && key ? `s3://${bucket}/${key}` : "";
}

function parseS3Path(path) {
  const text = String(path || "").trim();
  if (!text.toLowerCase().startsWith("s3://")) {
    return null;
  }
  try {
    const parsed = new URL(text);
    const bucket = decodeURIComponent(parsed.hostname || "").trim();
    const key = decodeURIComponent(parsed.pathname || "").replace(/^\/+/, "").trim();
    return bucket && key ? { bucket, key } : null;
  } catch (_error) {
    return null;
  }
}

function sourceBasedPipelineOutputPath(sourcePath, notebookSlug, outputFileName) {
  const parsed = parseS3Path(sourcePath);
  if (!parsed) {
    return "";
  }
  const sourceMarker = "/source/";
  const basePrefix = parsed.key.includes(sourceMarker)
    ? parsed.key.split(sourceMarker, 1)[0]
    : parsed.key.includes("/")
      ? parsed.key.slice(0, parsed.key.lastIndexOf("/"))
      : "";
  const key = [basePrefix, "pipeline-results", notebookSlug, outputFileName]
    .map((item) => String(item || "").replace(/^\/+|\/+$/g, ""))
    .filter(Boolean)
    .join("/");
  return `s3://${parsed.bucket}/${key}`;
}

function predecessorPipelineOutputPath(cellRoot, notebookSlug, outputFileName) {
  const sql = currentEditorSql(cellRoot?.querySelector?.("[data-editor-root]")) || "";
  const aliases = [...String(sql || "").matchAll(/(^|[^A-Za-z0-9_$])stage\.([A-Za-z_][A-Za-z0-9_$]*)/gi)]
    .map((match) => String(match[2] || "").trim().toLowerCase())
    .filter(Boolean);
  if (!aliases.length) {
    return "";
  }
  const workspaceRoot = cellRoot.closest("[data-workspace-notebook]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cells = notebookMetadata(notebookId).cells || [];
  const aliasToCell = new Map(
    cells.map((cell) => {
      const stage = normalizeCellStage(cell.stage);
      return [String(stage.alias || "").trim().toLowerCase(), cell.cellId];
    })
  );
  for (const alias of aliases) {
    const predecessorCellId = aliasToCell.get(alias);
    if (!predecessorCellId) {
      continue;
    }
    const predecessorRoot = workspaceRoot?.querySelector(
      `[data-query-cell][data-cell-id="${CSS.escape(predecessorCellId)}"]`
    );
    const predecessorPath =
      predecessorRoot?.querySelector?.("[data-cell-result-storage]")?.dataset?.resultStorageS3Path ||
      normalizeCellStage(cells.find((cell) => cell.cellId === predecessorCellId)?.stage).outputPath ||
      "";
    const parsed = parseS3Path(predecessorPath);
    if (!parsed) {
      continue;
    }
    const parent = parsed.key.includes("/") ? parsed.key.slice(0, parsed.key.lastIndexOf("/")) : "";
    const key = [parent || `pipeline-results/${notebookSlug}`, outputFileName]
      .map((item) => String(item || "").replace(/^\/+|\/+$/g, ""))
      .filter(Boolean)
      .join("/");
    return `s3://${parsed.bucket}/${key}`;
  }
  return "";
}

function proposedPipelineStageOutputS3Path(cellRoot) {
  const workspaceRoot = cellRoot?.closest?.("[data-workspace-notebook]");
  const notebookSlug = slugForResultStoragePath(workspaceNotebookId(workspaceRoot), "notebook");
  const outputFileName = stageOutputFileNameForCellRoot(cellRoot);
  const sql = currentEditorSql(cellRoot?.querySelector?.("[data-editor-root]")) || "";
  const sourcePath = firstVirtualS3PathFromSql(sql);
  if (sourcePath) {
    return sourceBasedPipelineOutputPath(sourcePath, notebookSlug, outputFileName);
  }
  const predecessorPath = predecessorPipelineOutputPath(cellRoot, notebookSlug, outputFileName);
  if (predecessorPath) {
    return predecessorPath;
  }
  return `s3://${defaultResultStorageBucket()}/_bdw_stages/${notebookSlug}/${outputFileName}`;
}

function sqlStringLiteral(value) {
  return `'${String(value || "").replace(/'/g, "''")}'`;
}

function decodeSqlStringLiteralContent(value) {
  return String(value || "").replace(/''/g, "'");
}

function virtualReferencePart(value) {
  const text = String(value || "").trim();
  if (/^[A-Za-z_][A-Za-z0-9_$]*$/.test(text)) {
    return text;
  }
  return `"${text.replace(/"/g, '""')}"`;
}

function virtualS3ReferenceForPath(path) {
  const text = String(path || "").trim();
  if (!text.toLowerCase().startsWith("s3://")) {
    return "";
  }
  let parsed = null;
  try {
    parsed = new URL(text);
  } catch (_error) {
    return "";
  }
  const bucket = decodeURIComponent(parsed.hostname || "").trim();
  const key = decodeURIComponent(parsed.pathname || "").replace(/^\/+/, "").trim();
  if (!bucket || !key) {
    return "";
  }
  return `s3.${virtualReferencePart(bucket)}.${virtualReferencePart(key)}`;
}

function duckdbSqlToVirtualSql(sql) {
  return String(sql ?? "").replace(
    /\bread_parquet\s*\(\s*'((?:[^']|'')*)'\s*\)/gi,
    (match, rawPath) => virtualS3ReferenceForPath(decodeSqlStringLiteralContent(rawPath)) || match
  );
}

function resultStorageReferencesForPath(path) {
  const text = String(path || "").trim();
  if (!text.toLowerCase().startsWith("s3://")) {
    return null;
  }
  let parsed = null;
  try {
    parsed = new URL(text);
  } catch (_error) {
    return null;
  }
  const bucket = decodeURIComponent(parsed.hostname || "").trim();
  const key = decodeURIComponent(parsed.pathname || "").replace(/^\/+/, "").trim();
  if (!bucket || !key || key.endsWith("/") || !key.toLowerCase().endsWith(".parquet")) {
    return null;
  }
  return {
    path: `s3://${bucket}/${key}`,
    virtualPath: virtualS3ReferenceForPath(`s3://${bucket}/${key}`),
    duckdbPath: `s3://${bucket}/${key}`,
    duckdbReference: `read_parquet(${sqlStringLiteral(`s3://${bucket}/${key}`)})`,
  };
}

function syncCellResultStorageState(cellRoot, { proposeIfEmpty = true } = {}) {
  if (!(cellRoot instanceof Element)) {
    return null;
  }
  const root = cellRoot.querySelector("[data-cell-result-storage]");
  const toggle = cellRoot.querySelector('[data-cell-query-option="duckdb.resultStorage.mode"]');
  const pathInput = cellRoot.querySelector('[data-cell-query-option="duckdb.resultStorage.path"]');
  if (!root || !pathInput) {
    return null;
  }
  const pipelineStageStorage = pipelineResultStorageForCellRoot(cellRoot);
  if (pipelineStageStorage && toggle) {
    if (toggle instanceof HTMLInputElement) {
      toggle.checked = true;
    } else {
      toggle.setAttribute("aria-checked", "true");
    }
  }
  const enabled =
    pipelineStageStorage ||
    toggle?.checked === true ||
    toggle?.getAttribute?.("aria-checked") === "true";
  if (proposeIfEmpty && !String(pathInput.value || "").trim()) {
    pathInput.value = pipelineStageStorage
      ? proposedPipelineStageOutputS3Path(cellRoot)
      : proposedResultStorageS3Path(cellRoot);
  }
  const references = resultStorageReferencesForPath(pathInput.value);
  root.dataset.resultStorageState = enabled ? (references ? "on" : "invalid") : "off";
  root.dataset.resultStoragePurpose = pipelineStageStorage ? "pipeline-stage-output" : "exploration-result-storage";
  root.classList.toggle("is-on", enabled);
  root.classList.toggle("is-invalid", enabled && !references);
  if (references) {
    root.dataset.resultStorageS3Path = references.path;
    root.dataset.resultStorageVirtualPath = references.virtualPath;
    root.dataset.resultStorageDuckdbPath = references.duckdbPath;
    root.dataset.resultStorageDuckdbReference = references.duckdbReference;
  } else {
    delete root.dataset.resultStorageS3Path;
    delete root.dataset.resultStorageVirtualPath;
    delete root.dataset.resultStorageDuckdbPath;
    delete root.dataset.resultStorageDuckdbReference;
  }
  root.querySelectorAll("[data-copy-result-storage-virtual], [data-copy-result-storage-duckdb]").forEach((button) => {
    button.disabled = !enabled || !references;
  });
  pathInput.title =
    references?.path || String(pathInput.value || "").trim() || "S3 path for the stored result set";
  const metaRoot = cellRoot.closest("[data-notebook-meta]");
  const editable = metaRoot?.dataset.canEdit !== "false";
  pathInput.disabled = pipelineStageStorage
    ? !editable || cellLanguageForCellRoot(cellRoot) !== "sql"
    : toggle?.disabled === true;
  return references;
}

function syncVisibleResultStorageControls(root = document) {
  root.querySelectorAll?.("[data-query-cell]").forEach((cellRoot) => {
    syncCellResultStorageState(cellRoot);
  });
}

function queryOptionsForCellRoot(cellRoot) {
  if (!(cellRoot instanceof Element)) {
    return normalizeCellQueryOptions({});
  }
  const select = cellRoot.querySelector('[data-cell-query-option="duckdb.parquetHivePartitioning"]');
  const cacheToggle = cellRoot.querySelector('[data-cell-query-option="duckdb.cacheHydration.mode"]');
  const sourceCheckToggle = cellRoot.querySelector('[data-cell-query-option="validation.sourceExistence"]');
  const resultStorageToggle = cellRoot.querySelector('[data-cell-query-option="duckdb.resultStorage.mode"]');
  const resultStoragePathInput = cellRoot.querySelector('[data-cell-query-option="duckdb.resultStorage.path"]');
  const cacheEnabled =
    cacheToggle?.checked === true || cacheToggle?.getAttribute?.("aria-checked") === "true";
  const sourceCheckEnabled =
    sourceCheckToggle?.checked === true || sourceCheckToggle?.getAttribute?.("aria-checked") === "true";
  const pipelineStageStorage = pipelineResultStorageForCellRoot(cellRoot);
  const resultStorageEnabled =
    !pipelineStageStorage &&
    (resultStorageToggle?.checked === true || resultStorageToggle?.getAttribute?.("aria-checked") === "true");
  if (resultStorageEnabled || pipelineStageStorage) {
    syncCellResultStorageState(cellRoot);
  }
  return normalizeCellQueryOptions({
    duckdb: {
      parquetHivePartitioning: select?.value || "auto",
      cacheHydration: {
        mode: cacheEnabled ? "on" : "off",
        scope: "referencedS3Parquet",
        indexPolicy: "autoPredicates",
      },
      resultStorage: {
        mode: resultStorageEnabled ? "on" : "off",
        path: resultStorageEnabled ? resultStoragePathInput?.value || "" : "",
      },
    },
    validation: {
      sourceExistence: sourceCheckEnabled ? "on" : "off",
    },
  });
}

function cellSourceExistenceValidationEnabled(cellRoot) {
  const toggle = cellRoot?.querySelector?.('[data-cell-query-option="validation.sourceExistence"]');
  return toggle?.checked === true || toggle?.getAttribute?.("aria-checked") === "true";
}

const cacheHydrationToggleRequests = new WeakMap();

function cellCacheHydrationEnabled(cellRoot) {
  const toggle = cellRoot?.querySelector?.('[data-cell-query-option="duckdb.cacheHydration.mode"]');
  return toggle?.checked === true || toggle?.getAttribute?.("aria-checked") === "true";
}

function cacheHydrationStateLabel(status) {
  const state = String(status?.status || "unknown").trim().toLowerCase() || "unknown";
  if (state === "rehydrating") {
    return "Building";
  }
  if (state === "deleting") {
    return "Deleting";
  }
  if (state === "unsupported") {
    return "No source";
  }
  if (state === "off") {
    return "Off";
  }
  const cleaned = String(status?.statusLabel || state).replace(/\bcache\b/gi, "").trim();
  return cleaned ? `${cleaned.slice(0, 1).toUpperCase()}${cleaned.slice(1)}` : "Unknown";
}

function setCellCacheHydrationVisualState(cellRoot, status) {
  const root = cellRoot?.querySelector?.("[data-cell-cache-hydration]");
  const badge = cellRoot?.querySelector?.("[data-cache-hydration-badge]");
  const toggle = cellRoot?.querySelector?.("[data-cache-hydration-switch]");
  const stateLabel = cellRoot?.querySelector?.("[data-cache-hydration-state-label]");
  if (!root) {
    return;
  }
  const state = String(status?.status || "unknown").trim().toLowerCase() || "unknown";
  const label = String(status?.statusLabel || status?.label || (state === "off" ? "Off" : "Unknown"));
  const reason = String(status?.statusReason || "");
  const busy = ["checking", "unknown", "rehydrating", "deleting"].includes(state);
  root.dataset.cacheHydrationState = state;
  root.title = reason || root.title;
  if (toggle) {
    const wasBusy = toggle.dataset.cacheHydrationBusy === "true";
    if (busy && !wasBusy) {
      toggle.dataset.cacheHydrationWasDisabled = toggle.disabled ? "true" : "false";
    }
    toggle.classList.toggle("is-loading", busy);
    if (busy) {
      toggle.setAttribute("aria-busy", "true");
      toggle.disabled = true;
      toggle.dataset.cacheHydrationBusy = "true";
    } else {
      toggle.removeAttribute("aria-busy");
      if (wasBusy && toggle.dataset.cacheHydrationWasDisabled !== "true") {
        toggle.disabled = false;
      }
      delete toggle.dataset.cacheHydrationBusy;
      delete toggle.dataset.cacheHydrationWasDisabled;
    }
  }
  if (stateLabel) {
    stateLabel.textContent = cacheHydrationStateLabel(status);
  }
  if (badge) {
    badge.hidden = state === "off";
    const shortReason = reason.length > 90 ? `${reason.slice(0, 87)}...` : reason;
    badge.textContent =
      state === "error" && shortReason ? `Runtime cache: Error` : `Runtime cache: ${label}`;
    badge.title = reason || label;
  }
}

async function cacheHydrationPayloadForCellRoot(cellRoot) {
  const sql = cellRoot?.querySelector?.("[data-editor-source]")?.value || "";
  const workspaceRoot = cellRoot?.closest?.("[data-workspace-notebook]");
  const localValidation = await validateLocalWorkspaceAliases(sql);
  const missingAliases = Array.isArray(localValidation?.missingAliases)
    ? localValidation.missingAliases.map((alias) => String(alias || "").trim()).filter(Boolean)
    : [];
  if (missingAliases.length) {
    throw new Error(`Referenced source(s) were not found: ${missingAliases.join(", ")}.`);
  }
  const localRelations =
    localValidation?.localRelations && typeof localValidation.localRelations === "object"
      ? Object.fromEntries(
          Object.entries(localValidation.localRelations)
            .map(([key, value]) => [String(key || "").trim(), String(value || "").trim()])
            .filter(([key, value]) => key && value)
        )
      : {};
  return {
    sql,
    notebookId: workspaceNotebookId(workspaceRoot),
    notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
    cellId: cellRoot?.dataset?.cellId || "",
    dataSources: selectedDataSourcesForCell(cellRoot),
    localRelations,
    queryOptions: queryOptionsForCellRoot(cellRoot),
  };
}

async function fetchCacheHydrationStatus(cellRoot, endpoint = "/api/query-cache/preview") {
  const payload = await cacheHydrationPayloadForCellRoot(cellRoot);
  const response = await window.fetch(endpoint, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    let message = "The cache hydration status could not be checked.";
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }
    throw new Error(message);
  }
  return response.json();
}

function primaryCacheHydrationStatus(payload) {
  const sources = Array.isArray(payload?.sources) ? payload.sources : [];
  if (!sources.length) {
    return {
      status: payload?.enabled ? "unsupported" : "off",
      statusLabel: payload?.enabled ? "No cacheable source" : "Off",
      statusReason: payload?.enabled
        ? "Hydrate cache applies to known S3 Parquet sources selected in the notebook. Direct read_parquet('s3://...') calls are not rewritten in this version."
        : "Hydrate cache is off for this SQL cell.",
    };
  }
  const priority = ["error", "expired", "stale", "miss", "unknown", "hit"];
  return (
    [...sources].sort(
      (left, right) =>
        priority.indexOf(String(left.status || "unknown")) -
        priority.indexOf(String(right.status || "unknown"))
    )[0] || sources[0]
  );
}

async function refreshCellCacheHydrationStatus(cellRoot) {
  const enabled = cellCacheHydrationEnabled(cellRoot);
  if (!enabled) {
    setCellCacheHydrationVisualState(cellRoot, {
      status: "off",
      statusLabel: "Off",
      statusReason: "Hydrate cache is off for this SQL cell.",
    });
    return null;
  }
  setCellCacheHydrationVisualState(cellRoot, {
    status: "checking",
    statusLabel: "Checking",
    statusReason: "Checking whether the temporary DuckDB cache exists and still matches the S3 source revision.",
  });
  try {
    const payload = await fetchCacheHydrationStatus(cellRoot);
    setCellCacheHydrationVisualState(cellRoot, primaryCacheHydrationStatus(payload));
    return payload;
  } catch (error) {
    setCellCacheHydrationVisualState(cellRoot, {
      status: "error",
      statusLabel: "Error",
      statusReason: error instanceof Error ? error.message : "The cache hydration status could not be checked.",
    });
    return null;
  }
}

function refreshVisibleCacheHydrationStatuses(root = document) {
  root.querySelectorAll?.("[data-query-cell]").forEach((cellRoot) => {
    if (cellCacheHydrationEnabled(cellRoot)) {
      refreshCellCacheHydrationStatus(cellRoot);
    }
  });
}

function syncCellCacheHydrationJobState(cellRoot, job) {
  if (!cellCacheHydrationEnabled(cellRoot)) {
    return;
  }
  const hydration = job?.cacheHydration;
  if (!hydration || typeof hydration !== "object") {
    return;
  }
  if (String(hydration.status || "").toLowerCase() === "rehydrating") {
    setCellCacheHydrationVisualState(cellRoot, {
      status: "rehydrating",
      statusLabel: "Rehydrating",
      statusReason: "Rebuilds the local DuckDB table from S3 and recreates the selected ART indexes.",
    });
    return;
  }
  if (String(hydration.status || "").toLowerCase() === "error") {
    setCellCacheHydrationVisualState(cellRoot, {
      status: "error",
      statusLabel: "Error",
      statusReason: hydration.statusReason || hydration.error || "Cache hydration failed before the query could run.",
    });
    return;
  }
  if (Array.isArray(hydration.sources) && hydration.sources.length) {
    setCellCacheHydrationVisualState(
      cellRoot,
      primaryCacheHydrationStatus({ enabled: hydration.enabled !== false, sources: hydration.sources })
    );
  }
}

async function applyCellCacheHydrationToggle(cellRoot, enabled) {
  if (!(cellRoot instanceof Element)) {
    return null;
  }
  const existing = cacheHydrationToggleRequests.get(cellRoot);
  if (existing) {
    return existing;
  }

  const endpoint = enabled ? "/api/query-cache/rehydrate" : "/api/query-cache/delete";
  setCellCacheHydrationVisualState(cellRoot, {
    status: enabled ? "rehydrating" : "deleting",
    statusLabel: enabled ? "Building runtime cache" : "Deleting runtime cache",
    statusReason: enabled
      ? "Building the runtime DuckDB cache table for the current SQL cell."
      : "Deleting the runtime DuckDB cache table and metadata for the current SQL cell.",
  });
  const request = fetchCacheHydrationStatus(cellRoot, endpoint)
    .then((payload) => {
      if (enabled) {
        setCellCacheHydrationVisualState(cellRoot, primaryCacheHydrationStatus(payload));
        return payload;
      }
      const failedSource = (Array.isArray(payload?.sources) ? payload.sources : []).find(
        (source) => String(source?.status || "").toLowerCase() === "error"
      );
      if (failedSource) {
        setCellCacheHydrationVisualState(cellRoot, failedSource);
        return payload;
      }
      setCellCacheHydrationVisualState(cellRoot, {
        status: "off",
        statusLabel: "Off",
        statusReason: payload?.deleted
          ? "The runtime cache for this cell was deleted."
          : "Hydrate cache is off for this SQL cell.",
      });
      return payload;
    })
    .catch((error) => {
      setCellCacheHydrationVisualState(cellRoot, {
        status: "error",
        statusLabel: "Error",
        statusReason: error instanceof Error ? error.message : "The cache action could not be completed.",
      });
      return null;
    })
    .finally(() => {
      cacheHydrationToggleRequests.delete(cellRoot);
    });
  cacheHydrationToggleRequests.set(cellRoot, request);
  return request;
}

function ensureCacheHydrationDialog() {
  let dialog = document.querySelector("[data-cache-hydration-dialog]");
  if (dialog) {
    return dialog;
  }
  document.body.insertAdjacentHTML(
    "beforeend",
    `
      <dialog class="modal-dialog modal-dialog-wide" data-cache-hydration-dialog>
        <form method="dialog" class="modal-card modal-card-wide">
          <h2 class="modal-title">Cache hydration plan</h2>
          <p class="modal-copy">
            This dialog explains what the Hydrate cache option does before a SQL cell runs.
          </p>
          <div class="cache-hydration-dialog-body" data-cache-hydration-dialog-body></div>
          <p class="cache-hydration-dialog-status" data-cache-hydration-dialog-status role="status" hidden></p>
          <menu class="modal-actions">
            <button class="modal-button modal-button-secondary" type="button" data-cache-hydration-refresh data-cache-hydration-action>Refresh status</button>
            <button class="modal-button" type="button" data-cache-hydration-rehydrate data-cache-hydration-action>Rehydrate now</button>
            <button class="modal-button modal-button-secondary" type="button" data-cache-hydration-expire data-cache-hydration-action>Expire cache</button>
            <button class="modal-button modal-button-secondary" type="submit" value="confirm">Close</button>
          </menu>
        </form>
      </dialog>
    `
  );
  return document.querySelector("[data-cache-hydration-dialog]");
}

function renderCacheHydrationDialogBody(dialog, payload) {
  const body = dialog?.querySelector("[data-cache-hydration-dialog-body]");
  if (!body) {
    return;
  }
  const sources = Array.isArray(payload?.sources) ? payload.sources : [];
  const unsupported = Array.isArray(payload?.unsupportedSources) ? payload.unsupportedSources : [];
  const sourceMarkup = sources.length
    ? sources
        .map(
          (source) => `
            <article class="query-explain-card">
              <h3>${escapeHtml(source.statusLabel || "Cache source")}</h3>
              <dl class="query-explain-meta-list">
                <div><dt>Source view/relation</dt><dd>${escapeHtml(source.sourceViewRelation || source.relation || "")}</dd></div>
                <div><dt>S3 path</dt><dd>${escapeHtml(source.path || "")}</dd></div>
                <div><dt>Source revision</dt><dd>${escapeHtml(source.sourceRevision || "Unknown")}</dd></div>
                <div><dt>Source size</dt><dd>${escapeHtml(formatByteCount(source.sourceSizeBytes || 0))}</dd></div>
                <div><dt>Cache table</dt><dd>${escapeHtml(source.cacheTable || "")} <span class="runtime-cache-pill">Runtime table</span></dd></div>
                <div><dt>Rows cached</dt><dd>${escapeHtml(String(source.rowCount ?? 0))}</dd></div>
                <div><dt>Cache size</dt><dd>${escapeHtml(formatByteCount(source.cacheSizeBytes || 0))}</dd></div>
                <div><dt>ART index columns</dt><dd>${escapeHtml((source.indexColumns || []).join(", ") || "No ART index columns selected yet.")}</dd></div>
                <div><dt>Last checked</dt><dd>${escapeHtml(source.lastCheckedAt || "")}</dd></div>
                <div><dt>Last hydrated</dt><dd>${escapeHtml(source.lastHydratedAt || "Not hydrated yet")}</dd></div>
                <div><dt>Expected behavior on next run</dt><dd>${escapeHtml(source.expectedBehavior || "The cell checks this cache before it runs and rebuilds it if it is missing, stale, or expired.")}</dd></div>
              </dl>
              <p class="cache-hydration-runtime-warning"><strong>Temporary storage</strong>: ${escapeHtml(source.temporaryWarning || payload?.ephemeralWarning || "This runtime cache table lives in temporary compute storage.")}</p>
              <p class="modal-copy">${escapeHtml(source.statusReason || "")}</p>
              <p class="modal-copy">ART indexes speed up equality lookups such as WHERE taxpayer_id = ...; they do not make every query faster.</p>
            </article>
          `
        )
        .join("")
    : '<p class="modal-copy">No known S3 Parquet source relation is referenced by this cell. Hydrate cache does not rewrite direct read_parquet(\'s3://...\') calls in this version.</p>';
  const unsupportedMarkup = unsupported.length
    ? `<p class="modal-copy">${escapeHtml(unsupported.map((item) => item.statusReason).filter(Boolean)[0] || "")}</p>`
    : "";
  body.innerHTML = `
    <p class="modal-copy">${escapeHtml(payload?.copy || "Copies referenced S3 Parquet data into temporary DuckDB cache tables before the query runs.")}</p>
    <p class="modal-copy">${escapeHtml(payload?.ephemeralWarning || "This runtime cache table lives in temporary compute storage and can disappear after a pod restart.")}</p>
    <p class="modal-copy">Stale cache means the source data changed since this cache was built; the next run rebuilds it before querying. Expired cache means you manually marked it expired, so it will not be reused until it is rebuilt.</p>
    ${sourceMarkup}
    ${unsupportedMarkup}
  `;
}

function setCacheHydrationDialogStatus(dialog, message, tone = "info") {
  const status = dialog?.querySelector?.("[data-cache-hydration-dialog-status]");
  if (!status) {
    return;
  }
  const text = String(message || "").trim();
  status.hidden = !text;
  status.textContent = text;
  status.dataset.tone = tone;
}

function setCacheHydrationDialogActionsBusy(dialog, activeButton, busy, busyLabel = "") {
  const actionButtons = Array.from(dialog?.querySelectorAll?.("[data-cache-hydration-action]") || []);
  actionButtons.forEach((button) => {
    if (!button.dataset.defaultText) {
      button.dataset.defaultText = button.textContent || "";
    }
    button.disabled = Boolean(busy);
    button.classList.toggle("is-loading", busy && button === activeButton);
    if (busy && button === activeButton) {
      button.innerHTML = `<span class="query-button-spinner" aria-hidden="true"></span><span>${escapeHtml(busyLabel || button.dataset.defaultText || "")}</span>`;
    } else if (!busy) {
      button.textContent = button.dataset.defaultText || button.textContent || "";
    }
  });
}

function markCacheHydrationDialogActionDone(button, label) {
  if (!button) {
    return;
  }
  const defaultText = button.dataset.defaultText || button.textContent || "";
  button.textContent = label;
  window.setTimeout(() => {
    if (!button.disabled) {
      button.textContent = defaultText;
    }
  }, 1200);
}

async function openCacheHydrationDialog(cellRoot) {
  const dialog = ensureCacheHydrationDialog();
  if (!dialog) {
    return;
  }
  dialog.dataset.cellId = cellRoot?.dataset.cellId || "";
  const refreshButton = dialog.querySelector("[data-cache-hydration-refresh]");
  const rehydrateButton = dialog.querySelector("[data-cache-hydration-rehydrate]");
  const expireButton = dialog.querySelector("[data-cache-hydration-expire]");
  const runAction = async (endpoint, button, labels) => {
    setCacheHydrationDialogActionsBusy(dialog, button, true, labels.busy);
    setCacheHydrationDialogStatus(dialog, labels.busy, "info");
    setCellCacheHydrationVisualState(cellRoot, {
      status: endpoint.includes("rehydrate") ? "rehydrating" : "unknown",
      statusLabel: endpoint.includes("rehydrate") ? "Rehydrating" : "Checking",
      statusReason: endpoint.includes("rehydrate")
        ? "Rebuilds the local DuckDB table from S3 and recreates the selected ART indexes."
        : "Checking whether the cache exists and matches the S3 source revision.",
    });
    try {
      const payload = await fetchCacheHydrationStatus(cellRoot, endpoint);
      renderCacheHydrationDialogBody(dialog, payload);
      setCellCacheHydrationVisualState(cellRoot, primaryCacheHydrationStatus(payload));
      setCacheHydrationDialogStatus(dialog, labels.done, "success");
      setCacheHydrationDialogActionsBusy(dialog, button, false);
      markCacheHydrationDialogActionDone(button, labels.done);
    } catch (error) {
      const message = error instanceof Error ? error.message : "The cache action could not be completed.";
      setCacheHydrationDialogStatus(dialog, message, "error");
      setCacheHydrationDialogActionsBusy(dialog, button, false);
      setCellCacheHydrationVisualState(cellRoot, {
        status: "error",
        statusLabel: "Error",
        statusReason: message,
      });
    }
  };
  refreshButton.onclick = () =>
    runAction("/api/query-cache/preview", refreshButton, {
      busy: "Refreshing...",
      done: "Refreshed",
    });
  rehydrateButton.onclick = () =>
    runAction("/api/query-cache/rehydrate", rehydrateButton, {
      busy: "Rehydrating...",
      done: "Rehydrated",
    });
  expireButton.onclick = () =>
    runAction("/api/query-cache/expire", expireButton, {
      busy: "Expiring...",
      done: "Expired",
    });
  try {
    const payload = await fetchCacheHydrationStatus(cellRoot);
    renderCacheHydrationDialogBody(dialog, payload);
    setCellCacheHydrationVisualState(cellRoot, primaryCacheHydrationStatus(payload));
    setCacheHydrationDialogStatus(dialog, "", "info");
  } catch (error) {
    renderCacheHydrationDialogBody(dialog, {
      copy: error instanceof Error ? error.message : "The cache hydration plan could not be loaded.",
      sources: [],
    });
    setCacheHydrationDialogStatus(
      dialog,
      error instanceof Error ? error.message : "The cache hydration plan could not be loaded.",
      "error"
    );
    setCellCacheHydrationVisualState(cellRoot, {
      status: "error",
      statusLabel: "Error",
      statusReason: error instanceof Error ? error.message : "The cache hydration plan could not be loaded.",
    });
  }
  if (typeof dialog.showModal === "function" && !dialog.open) {
    dialog.showModal();
  }
}

function ensureRuntimeStorageDialog() {
  let dialog = document.querySelector("[data-runtime-storage-dialog]");
  if (dialog) {
    return dialog;
  }
  document.body.insertAdjacentHTML(
    "beforeend",
    `
      <dialog class="modal-dialog modal-dialog-wide" data-runtime-storage-dialog>
        <form method="dialog" class="modal-card modal-card-wide">
          <h2 class="modal-title">Runtime Storage</h2>
          <p class="modal-copy">
            Temporary DuckDB storage used by query workers and hydrated cache datasets.
          </p>
          <div class="runtime-storage-dialog-body" data-runtime-storage-body></div>
          <p class="cache-hydration-dialog-status" data-runtime-storage-status role="status" hidden></p>
          <menu class="modal-actions">
            <button class="modal-button modal-button-secondary" type="button" data-runtime-storage-refresh>Refresh</button>
            <button class="modal-button modal-button-secondary" type="submit" value="confirm">Close</button>
          </menu>
        </form>
      </dialog>
    `
  );
  dialog = document.querySelector("[data-runtime-storage-dialog]");
  if (dialog && dialog.dataset.runtimeStorageWired !== "true") {
    dialog.dataset.runtimeStorageWired = "true";
    dialog.addEventListener("click", async (event) => {
      const refreshButton = event.target.closest("[data-runtime-storage-refresh]");
      if (refreshButton) {
        event.preventDefault();
        await refreshRuntimeStorageDialog(dialog, refreshButton);
        return;
      }
      const deleteButton = event.target.closest("[data-runtime-cache-delete]");
      if (deleteButton) {
        event.preventDefault();
        await deleteRuntimeStorageCache(dialog, deleteButton);
      }
    });
  }
  return dialog;
}

async function fetchRuntimeStorageState() {
  const response = await window.fetch("/api/runtime-storage", {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    let message = "Runtime storage could not be loaded.";
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }
    throw new Error(message);
  }
  return response.json();
}

function runtimeStorageHydratedCells() {
  const notebookIds = Array.from(document.querySelectorAll(".notebook-link[data-notebook-id]"))
    .map((link) => String(link.dataset.notebookId || "").trim())
    .filter(Boolean);
  const seen = new Set();
  const cells = [];
  notebookIds.forEach((notebookId) => {
    if (seen.has(notebookId)) {
      return;
    }
    seen.add(notebookId);
    const metadata = notebookMetadata(notebookId);
    normalizeNotebookCells(metadata.cells || []).forEach((cell) => {
      const mode = String(cell?.queryOptions?.duckdb?.cacheHydration?.mode || "")
        .trim()
        .toLowerCase();
      if (normalizeCellLanguage(cell?.language) !== "sql" || mode !== "on") {
        return;
      }
      cells.push({
        notebookId,
        notebookTitle: metadata.title || "Notebook",
        cellId: cell.cellId || "",
        sqlPreview: String(cell.sql || "").replace(/\s+/g, " ").trim().slice(0, 180),
      });
    });
  });
  return cells;
}

function runtimeStorageCellRefsMarkup(refs) {
  const normalizedRefs = Array.isArray(refs) ? refs.filter((ref) => ref && typeof ref === "object") : [];
  if (!normalizedRefs.length) {
    return "No linked cell recorded yet.";
  }
  return normalizedRefs
    .map((ref) => {
      const title = ref.notebookTitle || ref.notebookId || "Notebook";
      const cell = ref.cellId ? ` / ${ref.cellId}` : "";
      return `${escapeHtml(title)}${escapeHtml(cell)}`;
    })
    .join("<br>");
}

function runtimeStorageDatasetMarkup(dataset) {
  const cacheKey = String(dataset?.cacheKey || "");
  return `
    <article class="query-explain-card runtime-storage-cache-card">
      <h3>${escapeHtml(dataset?.relation || dataset?.sourceViewRelation || "Cached dataset")}</h3>
      <dl class="query-explain-meta-list">
        <div><dt>S3 path</dt><dd>${escapeHtml(dataset?.path || "")}</dd></div>
        <div><dt>Rows cached</dt><dd>${escapeHtml(String(dataset?.rowCount ?? 0))}</dd></div>
        <div><dt>Cache size</dt><dd>${escapeHtml(formatByteCount(dataset?.cacheSizeBytes || 0))}</dd></div>
        <div><dt>Last hydrated</dt><dd>${escapeHtml(dataset?.lastHydratedAt || "Unknown")}</dd></div>
        <div><dt>Last used</dt><dd>${escapeHtml(dataset?.lastUsedAt || "Unknown")}</dd></div>
        <div><dt>Source revision</dt><dd>${escapeHtml(dataset?.sourceRevision || "Unknown")}</dd></div>
        <div><dt>Linked cells</dt><dd>${runtimeStorageCellRefsMarkup(dataset?.cellRefs)}</dd></div>
      </dl>
      <div class="runtime-storage-card-actions">
        <button class="modal-button modal-button-secondary" type="button" data-runtime-cache-delete data-cache-key="${escapeHtml(cacheKey)}">
          Delete cached dataset
        </button>
      </div>
    </article>
  `;
}

function renderRuntimeStorageDialog(dialog, payload) {
  const body = dialog?.querySelector("[data-runtime-storage-body]");
  if (!body) {
    return;
  }
  const datasets = Array.isArray(payload?.queryCache?.datasets) ? payload.queryCache.datasets : [];
  const hydratedCells = runtimeStorageHydratedCells();
  const datasetMarkup = datasets.length
    ? datasets.map(runtimeStorageDatasetMarkup).join("")
    : '<p class="modal-copy">No hydrated cache datasets are currently stored.</p>';
  const cellMarkup = hydratedCells.length
    ? hydratedCells
        .map(
          (cell) => `
            <article class="query-explain-card runtime-storage-cell-card">
              <h3>${escapeHtml(cell.notebookTitle || "Notebook")}</h3>
              <dl class="query-explain-meta-list">
                <div><dt>Notebook ID</dt><dd>${escapeHtml(cell.notebookId || "")}</dd></div>
                <div><dt>Cell ID</dt><dd>${escapeHtml(cell.cellId || "")}</dd></div>
                <div><dt>SQL preview</dt><dd>${escapeHtml(cell.sqlPreview || "")}</dd></div>
              </dl>
            </article>
          `
        )
        .join("")
    : '<p class="modal-copy">No currently known notebook cell has Hydrate cache enabled.</p>';

  body.innerHTML = `
    <section class="runtime-storage-section">
      <h3>Storage usage</h3>
      <dl class="query-explain-meta-list">
        <div><dt>Runtime storage root</dt><dd>${escapeHtml(payload?.storageRoot?.path || "")}</dd></div>
        <div><dt>Root free</dt><dd>${escapeHtml(formatByteCount(payload?.storageRoot?.freeBytes || 0))}</dd></div>
        <div><dt>Root used</dt><dd>${escapeHtml(formatByteCount(payload?.storageRoot?.usedBytes || 0))}</dd></div>
        <div><dt>Query cache</dt><dd>${escapeHtml(formatByteCount(payload?.queryCache?.sizeBytes || 0))} at ${escapeHtml(payload?.queryCache?.path || "")}</dd></div>
        <div><dt>DuckDB spill</dt><dd>${escapeHtml(formatByteCount(payload?.duckdbSpill?.sizeBytes || 0))} at ${escapeHtml(payload?.duckdbSpill?.path || "Not configured")}</dd></div>
      </dl>
      <p class="cache-hydration-runtime-warning"><strong>DuckDB spill is read-only here</strong>: ${escapeHtml(payload?.duckdbSpill?.warning || "")}</p>
    </section>
    <section class="runtime-storage-section">
      <h3>DuckDB settings</h3>
      <dl class="query-explain-meta-list">
        <div><dt>Memory limit</dt><dd>${escapeHtml(payload?.duckdbSettings?.memoryLimit || "DuckDB default")}</dd></div>
        <div><dt>Threads</dt><dd>${escapeHtml(String(payload?.duckdbSettings?.threads ?? "DuckDB default"))}</dd></div>
        <div><dt>Temp limit</dt><dd>${escapeHtml(payload?.duckdbSettings?.maxTempDirectorySize || "DuckDB default")}</dd></div>
        <div><dt>Preserve insertion order</dt><dd>${payload?.duckdbSettings?.preserveInsertionOrder === false ? "Disabled" : "Default/enabled"}</dd></div>
      </dl>
    </section>
    <section class="runtime-storage-section">
      <h3>Cached datasets</h3>
      ${datasetMarkup}
    </section>
    <section class="runtime-storage-section">
      <h3>Cells using Hydrate cache</h3>
      ${cellMarkup}
    </section>
  `;
}

function setRuntimeStorageStatus(dialog, message, tone = "info") {
  const status = dialog?.querySelector("[data-runtime-storage-status]");
  if (!status) {
    return;
  }
  status.hidden = !message;
  status.textContent = message || "";
  status.dataset.tone = tone;
}

async function refreshRuntimeStorageDialog(dialog, activeButton = null) {
  if (activeButton) {
    activeButton.disabled = true;
  }
  setRuntimeStorageStatus(dialog, "Refreshing runtime storage...", "info");
  try {
    const payload = await fetchRuntimeStorageState();
    renderRuntimeStorageDialog(dialog, payload);
    setRuntimeStorageStatus(dialog, "Runtime storage refreshed.", "success");
  } catch (error) {
    setRuntimeStorageStatus(
      dialog,
      error instanceof Error ? error.message : "Runtime storage could not be loaded.",
      "error"
    );
  } finally {
    if (activeButton) {
      activeButton.disabled = false;
    }
  }
}

async function deleteRuntimeStorageCache(dialog, button) {
  const cacheKey = String(button?.dataset?.cacheKey || "").trim();
  if (!cacheKey) {
    return;
  }
  const { confirmed } = await showConfirmDialog({
    title: "Delete cached dataset",
    copy: "This deletes one hydrated DuckDB cache dataset from temporary compute storage. It does not delete the source data in S3.",
    confirmLabel: "Delete cached dataset",
  });
  if (!confirmed) {
    return;
  }
  button.disabled = true;
  setRuntimeStorageStatus(dialog, "Deleting cached dataset...", "info");
  try {
    const response = await window.fetch(`/api/runtime-storage/query-cache/${encodeURIComponent(cacheKey)}`, {
      method: "DELETE",
      headers: { Accept: "application/json" },
    });
    if (!response.ok) {
      let message = "The cached dataset could not be deleted.";
      try {
        const payload = await response.json();
        message = payload?.detail || message;
      } catch (_error) {
        // Ignore invalid JSON bodies.
      }
      throw new Error(message);
    }
    const payload = await response.json();
    renderRuntimeStorageDialog(dialog, payload?.storage || {});
    setRuntimeStorageStatus(
      dialog,
      payload?.deleted ? "Cached dataset deleted." : "No cache files existed for that dataset.",
      "success"
    );
  } catch (error) {
    setRuntimeStorageStatus(
      dialog,
      error instanceof Error ? error.message : "The cached dataset could not be deleted.",
      "error"
    );
  } finally {
    button.disabled = false;
  }
}

async function openRuntimeStorageDialog() {
  const dialog = ensureRuntimeStorageDialog();
  if (!dialog) {
    return;
  }
  renderRuntimeStorageDialog(dialog, {
    queryCache: { datasets: [] },
    duckdbSpill: {},
    storageRoot: {},
    duckdbSettings: {},
  });
  if (typeof dialog.showModal === "function" && !dialog.open) {
    dialog.showModal();
  }
  await refreshRuntimeStorageDialog(dialog);
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
  return Boolean(job && dataGenerationRunningStatuses.has(String(job.status || "").trim().toLowerCase()));
}

function dataGenerationJobStatusCopy(job) {
  if (!job) {
    return "Idle";
  }

  switch (String(job.status || "").trim().toLowerCase()) {
    case "queued":
      return "Queued";
    case "running":
      return "Running";
    case "completed":
      return "Completed";
    case "cancelled":
    case "canceled":
      return "Cancelled";
    case "aborted":
      return "Aborted";
    case "incomplete":
      return "Incomplete";
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
  const metricCopy = `${formatQueryDuration(dataGenerationJobElapsedMs(job))} | ${sizeCopy} | ${rowsCopy}`;
  const messageCopy = String(job.message || "").trim();
  const status = String(job.status || "").trim().toLowerCase();
  if (
    messageCopy &&
    (dataGenerationJobIsRunning(job) || ["cancelled", "canceled", "aborted", "incomplete", "failed"].includes(status))
  ) {
    return `${messageCopy} | ${metricCopy}`;
  }
  return metricCopy;
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

function comparePythonJobsByStartedAt(left, right) {
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
    status === "completed" ||
    status === "ready" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "canceled" ||
    status === "aborted" ||
    status === "incomplete" ||
    status === "expired" ||
    status === "warning" ||
    status === "warned" ||
    status === "skipped"
      ? status
      : "active";
  return `${type}:${job?.jobId || ""}:${lifecycleKey}`;
}

function normalizeNotebookActivityAction(reason = "edited") {
  const normalized = String(reason || "").trim().toLowerCase();
  if (normalized === "run") {
    return "run";
  }
  if (normalized === "open") {
    return "open";
  }
  return "edit";
}

function notebookDeletionInProgress(notebookId) {
  return notebookDeletionInProgressIds.has(String(notebookId || "").trim());
}

function clearSharedNotebookPendingWork(notebookId) {
  const normalizedNotebookId = String(notebookId || "").trim();
  if (!normalizedNotebookId) {
    return;
  }

  const pendingSync = sharedNotebookSyncHandles.get(normalizedNotebookId);
  if (pendingSync) {
    window.clearTimeout(pendingSync);
    sharedNotebookSyncHandles.delete(normalizedNotebookId);
  }

  const pendingActivityTouch = sharedNotebookActivityTouchHandles.get(normalizedNotebookId);
  if (pendingActivityTouch) {
    window.clearTimeout(pendingActivityTouch);
    sharedNotebookActivityTouchHandles.delete(normalizedNotebookId);
  }
}

function setNotebookDeletionInProgress(notebookId, inProgress) {
  const normalizedNotebookId = String(notebookId || "").trim();
  if (!normalizedNotebookId) {
    return;
  }

  if (inProgress) {
    notebookDeletionInProgressIds.add(normalizedNotebookId);
    clearSharedNotebookPendingWork(normalizedNotebookId);
  } else {
    notebookDeletionInProgressIds.delete(normalizedNotebookId);
  }
  applyNotebookMetadata();
}

function scheduleSharedNotebookActivityTouch(notebookId, reason = "edited") {
  const normalizedNotebookId = String(notebookId || "").trim();
  if (
    !normalizedNotebookId ||
    notebookDeletionInProgress(normalizedNotebookId) ||
    !notebookMetadata(normalizedNotebookId).shared
  ) {
    return;
  }

  const existingHandle = sharedNotebookActivityTouchHandles.get(normalizedNotebookId);
  if (existingHandle) {
    window.clearTimeout(existingHandle);
  }

  const action = normalizeNotebookActivityAction(reason);
  const delayMs = action === "edit" ? 1200 : 0;
  const handle = window.setTimeout(() => {
    sharedNotebookActivityTouchHandles.delete(normalizedNotebookId);
    window
      .fetch("/api/notebook-activity/touch", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-Workbench-Client-Id": workbenchClientId(),
        },
        body: JSON.stringify({
          notebookId: normalizedNotebookId,
          action,
        }),
      })
      .catch((error) => {
        console.error("Failed to record shared notebook activity.", error);
      });
  }, delayMs);
  sharedNotebookActivityTouchHandles.set(normalizedNotebookId, handle);
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
  queryWorkbenchEntryController.renderMyLatest();
  scheduleSharedNotebookActivityTouch(normalizedNotebookId, reason);
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

function syncDownloadJobsUi() {
  const state = downloadJobsController.currentState();
  downloadJobsStateVersion = state.version;
  downloadJobsSnapshot = state.snapshot;
  downloadJobsSummary = state.summary;
  downloadJobsController.syncPreparedDownloadIndicators();
  dataExchangeController.refreshPreparedDownloadState?.();
  renderQueryNotificationMenu();
}

function applyDownloadJobsState(snapshot) {
  downloadJobsController.applyState(snapshot);
}

async function loadDownloadJobsState() {
  return downloadJobsController.loadState();
}

function syncS3DeleteJobsUi() {
  const state = s3DeleteJobsController.currentState();
  s3DeleteJobsStateVersion = state.version;
  s3DeleteJobsSnapshot = state.snapshot;
  s3DeleteJobsSummary = state.summary;
  renderQueryNotificationMenu();
}

function applyS3DeleteJobsState(snapshot) {
  s3DeleteJobsController.applyState(snapshot);
}

async function loadS3DeleteJobsState() {
  return s3DeleteJobsController.loadState();
}

async function prepareSourceS3Download(sourceObjectRoot) {
  const descriptor = sourceObjectS3DownloadDescriptor(sourceObjectRoot);
  if (!descriptor) {
    return false;
  }
  return downloadJobsController.startS3PreparedDownload(descriptor);
}

async function startDataExchangePreparedDownload(fileId, filePassword = "") {
  return downloadJobsController.startDataExchangePreparedDownload(fileId, filePassword);
}

function downloadSourceS3GeneratedParts(sourceObjectRoot, mode = "merged") {
  const descriptor = sourceObjectS3GeneratedDownloadDescriptor(sourceObjectRoot, mode);
  if (!descriptor) {
    return false;
  }

  const search = new URLSearchParams({
    bucket: descriptor.bucket,
    prefix: descriptor.prefix,
    format: descriptor.fileFormat,
    mode: descriptor.mode,
    filename: descriptor.fileName,
  });
  const anchor = document.createElement("a");
  anchor.href = `/api/s3/generated/download?${search.toString()}`;
  anchor.download = descriptor.fileName;
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  return true;
}

async function downloadSourceObjectDdl(sourceObjectRoot) {
  const descriptor = sourceObjectDdlDescriptor(sourceObjectRoot);
  if (!descriptor) {
    return false;
  }

  const params = new URLSearchParams();
  let relation = descriptor.relation;
  if (descriptor.localWorkspaceEntryId) {
    const synced = await syncLocalWorkspaceEntry(descriptor.localWorkspaceEntryId);
    relation = synced.relation || relation;
  }

  if (relation) {
    params.set("relation", relation);
  }
  if (descriptor.sourceId) {
    params.set("sourceId", descriptor.sourceId);
  }
  if (descriptor.bucket) {
    params.set("bucket", descriptor.bucket);
  }
  if (descriptor.key) {
    params.set("key", descriptor.key);
  }
  if (descriptor.objectName) {
    params.set("objectName", descriptor.objectName);
  }
  if (descriptor.fileFormat) {
    params.set("fileFormat", descriptor.fileFormat);
  }

  const anchor = document.createElement("a");
  anchor.href = `/api/source-object-ddl/download?${params.toString()}`;
  anchor.download = descriptor.fileName || "source-ddl.sql";
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  return true;
}

async function writeTextToClipboard(text, { trim = true, emptyMessage = "There is no query path to copy." } = {}) {
  const value = trim ? String(text || "").trim() : String(text ?? "");
  if (!value) {
    throw new Error(emptyMessage);
  }

  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value);
      return;
    } catch (_error) {
      // Fall back to the textarea path below for browsers that expose the
      // clipboard API but block it for the current context.
    }
  }

  const textArea = document.createElement("textarea");
  textArea.value = value;
  textArea.setAttribute("readonly", "");
  textArea.style.position = "fixed";
  textArea.style.left = "-1000px";
  textArea.style.top = "-1000px";
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();
  const copied = document.execCommand("copy");
  textArea.remove();
  if (!copied) {
    throw new Error("The browser blocked clipboard access.");
  }
}

function sqlViewModeForEditor(editorRoot) {
  return editorRoot?.classList?.contains("is-duckdb-sql-view") ? "duckdb" : "virtual";
}

function syncSqlViewToggle(editorRoot, mode = sqlViewModeForEditor(editorRoot)) {
  if (!(editorRoot instanceof Element)) {
    return;
  }
  editorRoot.querySelectorAll("[data-editor-sql-view]").forEach((button) => {
    const active = button.dataset.editorSqlView === mode;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
}

function invalidatePreparedSqlViewForCell(cellRoot) {
  if (!(cellRoot instanceof Element)) {
    return;
  }
  preparedSqlViewCache.delete(cellRoot);
  const editorRoot = cellRoot.querySelector("[data-editor-root]");
  if (sqlViewModeForEditor(editorRoot) === "duckdb") {
    return;
  }
  const panel = cellRoot.querySelector("[data-duckdb-sql-panel]");
  if (panel) {
    if ("value" in panel) {
      panel.value = "";
    }
    panel.textContent = "";
    delete panel.dataset.sql;
    panel.classList.remove("is-error");
    panel.removeAttribute("aria-busy");
  }
}

async function prepareSqlSubmissionForCell(cellRoot, originalSql) {
  const executionSql = String(originalSql ?? "");
  const localRelationMap = {};
  const preparedQuery = await prepareLocalWorkspaceQuerySql(executionSql);
  (preparedQuery.synchronizedSources || []).forEach((source) => {
    const logicalRelation = String(source?.logicalRelation || "").trim();
    const physicalRelation = String(source?.relation || "").trim();
    if (logicalRelation && physicalRelation) {
      localRelationMap[logicalRelation] = physicalRelation;
    }
  });
  return {
    originalSql: executionSql,
    executionSql: preparedQuery.sql ?? executionSql,
    localRelationMap,
    dataSources: selectedDataSourcesForCell(cellRoot),
    queryOptions: queryOptionsForCellRoot(cellRoot),
  };
}

function stageSqlPreviewPayloadForCell(cellRoot) {
  return notebookStagePipelineController?.stagePayloadForCell?.(cellRoot) ?? null;
}

function sqlPreparationCacheKey(cellRoot, preparedSubmission, stagePayload = null) {
  return JSON.stringify({
    sql: preparedSubmission.originalSql,
    executionSql: preparedSubmission.executionSql,
    dataSources: preparedSubmission.dataSources,
    localRelations: preparedSubmission.localRelationMap,
    queryOptions: preparedSubmission.queryOptions,
    stage: stagePayload,
    materializedStagesVersion:
      notebookStagePipelineController?.getMaterializedStagesVersion?.() ?? null,
  });
}

async function prepareDuckdbSqlForCell(cellRoot) {
  if (!(cellRoot instanceof Element)) {
    throw new Error("The SQL cell could not be found.");
  }
  const editorRoot = cellRoot.querySelector("[data-editor-root]");
  const workspaceRoot = cellRoot.closest("[data-workspace-notebook]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellId = cellRoot.dataset.cellId;
  if (!editorRoot || !workspaceRoot || !notebookId || !cellId) {
    throw new Error("The SQL cell is missing notebook context.");
  }

  syncVisibleDuckdbSqlToVirtual(cellRoot);
  const originalSql = currentEditorSql(editorRoot);
  const preparedSubmission = await prepareSqlSubmissionForCell(cellRoot, originalSql);
  const stagePayload = stageSqlPreviewPayloadForCell(cellRoot);
  const cacheKey = sqlPreparationCacheKey(cellRoot, preparedSubmission, stagePayload);
  const cached = preparedSqlViewCache.get(cellRoot);
  if (cached?.key === cacheKey && cached.payload) {
    return cached.payload;
  }

  const response = await window.fetch("/api/query-sql/prepare", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      sql: preparedSubmission.executionSql,
      displaySql: preparedSubmission.originalSql,
      notebookId,
      notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
      cellId,
      dataSources: preparedSubmission.dataSources,
      localRelations: preparedSubmission.localRelationMap,
      queryOptions: preparedSubmission.queryOptions,
      stage: stagePayload,
    }),
  });

  if (!response.ok) {
    let message = "The DuckDB SQL could not be prepared.";
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }
    throw new Error(message);
  }

  const payload = await response.json();
  preparedSqlViewCache.set(cellRoot, { key: cacheKey, payload });
  return payload;
}

function normalizeSourceNavigationObjects(items) {
  if (!Array.isArray(items)) {
    return [];
  }
  const seen = new Set();
  const normalized = [];
  for (const item of items) {
    const source = {
      label: String(item?.label || item?.displayName || item?.relation || item?.key || "Source object").trim(),
      kind: String(item?.kind || "").trim(),
      sourceId: String(item?.sourceId || "").trim(),
      relation: String(item?.relation || "").trim(),
      queryAlias: String(item?.queryAlias || "").trim(),
      queryReference: String(item?.queryReference || "").trim(),
      bucket: String(item?.bucket || "").trim(),
      key: String(item?.key || "").trim(),
      path: String(item?.path || "").trim(),
      format: String(item?.format || "").trim(),
    };
    if (!source.relation && !(source.bucket && source.key)) {
      continue;
    }
    const dedupeKey = [
      source.sourceId.toLowerCase(),
      source.relation.toLowerCase(),
      source.bucket,
      source.key,
    ].join("||");
    if (seen.has(dedupeKey)) {
      continue;
    }
    seen.add(dedupeKey);
    normalized.push(source);
  }
  return normalized;
}

function sourceNavigationReferenceKeys(source) {
  return [
    source?.relation,
    source?.queryReference,
    source?.queryAlias,
    source?.path,
  ]
    .map((value) => String(value || "").trim().toLowerCase())
    .filter(Boolean);
}

function sourceObjectReferenceKeys(sourceObjectRoot) {
  return [
    sourceObjectRoot?.dataset?.sourceObjectRelation,
    sourceObjectRoot?.dataset?.sourceObjectQueryReference,
    sourceObjectRoot?.dataset?.sourceObjectQueryAlias,
    sourceObjectRoot?.dataset?.s3Path,
  ]
    .map((value) => String(value || "").trim().toLowerCase())
    .filter(Boolean);
}

function sourceNavigationDisplayDetail(source) {
  if (source?.bucket && source?.key) {
    return `s3://${source.bucket}/${source.key}`;
  }
  return source?.queryReference || source?.queryAlias || source?.relation || "";
}

function closeCellSourceNavigationMenu() {
  cellSourceNavigationMenu?.remove();
  cellSourceNavigationMenu = null;
  cellSourceNavigationChoices = [];
}

function cellSourceNavigationMenuItemMarkup(source, index) {
  const detail = sourceNavigationDisplayDetail(source);
  return `
    <button
      type="button"
      class="workspace-action-menu-item cell-source-navigation-item"
      data-navigate-cell-source-choice="${index}"
      title="${escapeHtml(detail || source.label)}"
    >
      <span aria-hidden="true">&rarr;</span>
      <span>
        <strong>${escapeHtml(source.label || "Source object")}</strong>
        ${detail ? `<small>${escapeHtml(detail)}</small>` : ""}
      </span>
    </button>
  `;
}

function openCellSourceNavigationMenu(triggerButton, sources) {
  closeCellSourceNavigationMenu();
  if (!(triggerButton instanceof Element) || !sources.length) {
    return;
  }
  cellSourceNavigationChoices = [...sources];
  const menu = document.createElement("div");
  menu.className = "workspace-action-menu-panel cell-source-navigation-menu";
  menu.dataset.cellSourceNavigationMenu = "";
  menu.innerHTML = sources
    .map((source, index) => cellSourceNavigationMenuItemMarkup(source, index))
    .join("");
  document.body.appendChild(menu);
  const rect = triggerButton.getBoundingClientRect();
  const menuWidth = menu.offsetWidth || 280;
  const menuHeight = menu.offsetHeight || 160;
  const left = Math.max(8, Math.min(rect.left, window.innerWidth - menuWidth - 8));
  const top = Math.max(8, Math.min(rect.bottom + 6, window.innerHeight - menuHeight - 8));
  menu.style.left = `${left}px`;
  menu.style.top = `${top}px`;
  cellSourceNavigationMenu = menu;
}

function sourceObjectNodeForNavigation(source) {
  const bucket = String(source?.bucket || "").trim();
  const key = String(source?.key || "").trim();
  if (bucket && key) {
    const exactS3Object = document.querySelector(
      `[data-source-object][data-s3-bucket="${CSS.escape(bucket)}"][data-s3-key="${CSS.escape(key)}"]`
    );
    if (exactS3Object) {
      return exactS3Object;
    }
  }

  const desiredKeys = new Set(sourceNavigationReferenceKeys(source));
  if (!desiredKeys.size) {
    return null;
  }
  return Array.from(document.querySelectorAll("[data-source-object]")).find((node) =>
    sourceObjectReferenceKeys(node).some((candidate) => desiredKeys.has(candidate))
  ) || null;
}

function openSidebarSourceObjectAncestors(target) {
  const sourcesRoot = dataSourcesSection();
  if (sourcesRoot instanceof HTMLDetailsElement) {
    sourcesRoot.open = true;
  }

  let ancestor = target?.parentElement ?? null;
  while (ancestor instanceof Element) {
    if (
      ancestor instanceof HTMLDetailsElement &&
      (ancestor.hasAttribute("data-source-catalog") ||
        ancestor.hasAttribute("data-source-schema") ||
        ancestor.hasAttribute("data-source-s3-folder"))
    ) {
      ancestor.open = true;
    }
    ancestor = ancestor.parentElement;
  }
}

function closeNotebookSidebarSection() {
  const section = notebookSection();
  if (section instanceof HTMLDetailsElement) {
    section.open = false;
  }
}

function flashSourceNavigationTarget(target) {
  if (!(target instanceof Element)) {
    return;
  }
  const label = target.querySelector(".source-node-label > span:last-child") || target.querySelector(".source-node-label");
  target.classList.remove("is-pipeline-inspect-flash");
  label?.classList?.remove("is-pipeline-target-text-flash");
  void target.offsetWidth;
  target.classList.add("is-pipeline-inspect-flash");
  label?.classList?.add("is-pipeline-target-text-flash");
  window.setTimeout(() => {
    target.classList.remove("is-pipeline-inspect-flash");
    label?.classList?.remove("is-pipeline-target-text-flash");
  }, 3200);
}

async function navigateToPreparedSourceObject(source) {
  const sourceId = source?.sourceId || (source?.bucket && source?.key ? "s3" : "");
  await revealDataSourceSidebarBrowser(sourceId);
  await refreshSidebar("notebook");
  closeNotebookSidebarSection();
  const target = sourceObjectNodeForNavigation(source);
  if (target) {
    openSidebarSourceObjectAncestors(target);
    target.scrollIntoView({ block: "center" });
    flashSourceNavigationTarget(target);
    setSidebarSourceOperationStatus(
      {
        tone: "success",
        title: "Source object located",
        copy: sourceNavigationDisplayDetail(source) || source.label,
      },
      { autoClearMs: 2500 }
    );
    return true;
  }

  await showMessageDialog({
    title: "Source object not visible",
    copy: "The cell references a source object, but it is not currently visible in the Data Sources navigation. Refresh the data sources and try again.",
  });
  return false;
}

async function navigateCellSourceObject(triggerButton) {
  const cellRoot = triggerButton?.closest?.("[data-query-cell]");
  if (!(cellRoot instanceof Element)) {
    return;
  }
  triggerButton.classList.add("is-loading");
  triggerButton.setAttribute("aria-busy", "true");
  try {
    const payload = await prepareDuckdbSqlForCell(cellRoot);
    const sources = normalizeSourceNavigationObjects(payload?.sourceObjects || []);
    if (!sources.length) {
      await showMessageDialog({
        title: "No source object found",
        copy: "This cell does not reference a known table or S3 object in the Data Sources navigation.",
      });
      return;
    }
    if (sources.length === 1) {
      closeCellSourceNavigationMenu();
      await navigateToPreparedSourceObject(sources[0]);
      return;
    }
    openCellSourceNavigationMenu(triggerButton, sources);
  } catch (error) {
    console.error("Failed to navigate to the cell source object.", error);
    await showMessageDialog({
      title: "Source navigation failed",
      copy: error instanceof Error ? error.message : "The source object could not be resolved.",
    });
  } finally {
    triggerButton.classList.remove("is-loading");
    triggerButton.removeAttribute("aria-busy");
  }
}

function renderDuckdbSqlPanel(editorRoot, { sql = "", error = "" } = {}) {
  const panel = editorRoot?.querySelector?.("[data-duckdb-sql-panel]");
  if (!panel) {
    return;
  }
  const text = error || sql || "";
  panel.hidden = false;
  if ("value" in panel) {
    panel.value = text;
  } else {
    panel.textContent = text;
  }
  panel.dataset.sql = text;
  panel.classList.toggle("is-error", Boolean(error));
  if (panel instanceof HTMLTextAreaElement) {
    panel.readOnly = Boolean(error);
  }
  panel.removeAttribute("aria-busy");
}

function duckdbSqlPanelIsPreparing(panel) {
  return panel?.getAttribute?.("aria-busy") === "true";
}

function syncVirtualSqlFromDuckdbPanel(panel) {
  if (!(panel instanceof HTMLTextAreaElement)) {
    return false;
  }
  const editorRoot = panel.closest("[data-editor-root]");
  if (
    !(editorRoot instanceof Element)
    || panel.classList.contains("is-error")
    || duckdbSqlPanelIsPreparing(panel)
  ) {
    return false;
  }
  const duckdbSql = panel.value ?? "";
  panel.dataset.sql = duckdbSql;
  const virtualSql = duckdbSqlToVirtualSql(duckdbSql);
  setVirtualEditorSql(editorRoot, virtualSql);
  preparedSqlViewCache.delete(editorRoot.closest("[data-query-cell]"));
  return true;
}

function syncVisibleDuckdbSqlToVirtual(cellRoot) {
  const editorRoot = cellRoot?.querySelector?.("[data-editor-root]");
  if (sqlViewModeForEditor(editorRoot) !== "duckdb") {
    return false;
  }
  return syncVirtualSqlFromDuckdbPanel(editorRoot.querySelector("[data-duckdb-sql-panel]"));
}

async function setEditorSqlViewMode(editorRoot, mode) {
  if (!(editorRoot instanceof Element)) {
    return;
  }
  const normalizedMode = mode === "duckdb" ? "duckdb" : "virtual";
  const cellRoot = editorRoot.closest("[data-query-cell]");
  if (normalizedMode === "virtual") {
    const panel = editorRoot.querySelector("[data-duckdb-sql-panel]");
    syncVirtualSqlFromDuckdbPanel(panel);
    editorRoot.classList.remove("is-duckdb-sql-view");
    if (panel) {
      panel.hidden = true;
      panel.removeAttribute("aria-busy");
    }
    syncSqlViewToggle(editorRoot, "virtual");
    return;
  }

  const panel = editorRoot.querySelector("[data-duckdb-sql-panel]");
  if (panel) {
    panel.hidden = false;
    panel.classList.remove("is-error");
    panel.setAttribute("aria-busy", "true");
    if ("value" in panel) {
      panel.value = "Preparing DuckDB SQL...";
    } else {
      panel.textContent = "Preparing DuckDB SQL...";
    }
  }
  editorRoot.classList.add("is-duckdb-sql-view");
  syncSqlViewToggle(editorRoot, "duckdb");

  try {
    const payload = await prepareDuckdbSqlForCell(cellRoot);
    renderDuckdbSqlPanel(editorRoot, {
      sql: String(payload?.executionSql || payload?.submittedSql || ""),
    });
  } catch (error) {
    renderDuckdbSqlPanel(editorRoot, {
      error: error instanceof Error ? error.message : "The DuckDB SQL could not be prepared.",
    });
  }
}

function currentVisibleEditorSql(editorRoot) {
  if (sqlViewModeForEditor(editorRoot) === "duckdb") {
    const panel = editorRoot?.querySelector?.("[data-duckdb-sql-panel]");
    if (duckdbSqlPanelIsPreparing(panel)) {
      return currentEditorSql(editorRoot);
    }
    return panel?.value ?? panel?.dataset?.sql ?? panel?.textContent ?? "";
  }
  return currentEditorSql(editorRoot);
}

async function copySourceQueryPath(sourceObjectRoot) {
  const descriptor = sourceQueryDescriptor(sourceObjectRoot);
  if (!descriptor?.relation) {
    return false;
  }

  await writeTextToClipboard(descriptor.relation);
  setSidebarSourceOperationStatus(
    {
      tone: "success",
      title: "Query path copied",
      copy: descriptor.relation,
    },
    { autoClearMs: 2500 }
  );
  return true;
}

async function copySourceDuckdbReference(sourceObjectRoot) {
  const reference = sourceDuckdbReference(sourceObjectRoot);
  if (!reference) {
    return false;
  }

  await writeTextToClipboard(reference);
  setSidebarSourceOperationStatus(
    {
      tone: "success",
      title: "DuckDB source reference copied",
      copy: reference,
    },
    { autoClearMs: 2500 }
  );
  return true;
}

async function copyEditorSql(editorRoot, triggerButton = null) {
  if (!(editorRoot instanceof Element)) {
    return false;
  }
  const copyingDuckdbSql = sqlViewModeForEditor(editorRoot) === "duckdb";
  const sqlText = currentVisibleEditorSql(editorRoot);
  await writeTextToClipboard(sqlText, {
    trim: false,
    emptyMessage: "There is no SQL to copy.",
  });
  const textarea = editorRoot.querySelector("[data-editor-source]");
  if (!copyingDuckdbSql && textarea && textarea.value !== sqlText) {
    textarea.value = sqlText;
  }
  if (triggerButton instanceof HTMLButtonElement) {
    const previousTitle = triggerButton.title;
    triggerButton.classList.add("is-copied");
    triggerButton.title = "Copied";
    triggerButton.setAttribute("aria-label", "SQL copied");
    window.setTimeout(() => {
      triggerButton.classList.remove("is-copied");
      triggerButton.title = previousTitle || "Copy SQL";
      triggerButton.setAttribute("aria-label", "Copy SQL");
    }, 1200);
  }
  return true;
}

async function copyQueryTimingTable(trigger) {
  if (!(trigger instanceof HTMLElement)) {
    return false;
  }
  const encoded = String(trigger.dataset.queryTimingTable || "");
  let timingTable = "";
  try {
    timingTable = decodeURIComponent(encoded);
  } catch (_error) {
    timingTable = encoded;
  }
  await writeTextToClipboard(timingTable, {
    trim: false,
    emptyMessage: "There are no query timing values to copy.",
  });
  const previousTitle = trigger.title;
  trigger.classList.add("is-copied");
  trigger.title = "Timing table copied";
  window.setTimeout(() => {
    trigger.classList.remove("is-copied");
    trigger.title = previousTitle || "Copy timing table";
  }, 1200);
  return true;
}

function resultStorageReferencesForCopyTrigger(trigger) {
  if (!(trigger instanceof Element)) {
    return null;
  }
  const cellStorageRoot = trigger.closest("[data-cell-result-storage]");
  if (cellStorageRoot) {
    const cellRoot = cellStorageRoot.closest("[data-query-cell]");
    syncCellResultStorageState(cellRoot);
    return {
      virtualPath: cellStorageRoot.dataset.resultStorageVirtualPath || "",
      duckdbReference: cellStorageRoot.dataset.resultStorageDuckdbReference || "",
      duckdbPath: cellStorageRoot.dataset.resultStorageDuckdbPath || "",
    };
  }

  const summaryRoot = trigger.closest("[data-result-storage-summary]");
  if (summaryRoot) {
    return {
      virtualPath: summaryRoot.dataset.resultStorageVirtualPath || "",
      duckdbReference: summaryRoot.dataset.resultStorageDuckdbReference || "",
      duckdbPath: summaryRoot.dataset.resultStorageDuckdbPath || "",
    };
  }

  const storage = queryJobForResultActionTarget(trigger)?.resultStorage;
  if (storage && typeof storage === "object") {
    return {
      virtualPath: String(storage.virtualPath || ""),
      duckdbReference: String(storage.duckdbReference || ""),
      duckdbPath: String(storage.duckdbPath || ""),
    };
  }
  return null;
}

async function copyResultStorageReference(trigger, kind = "virtual") {
  const references = resultStorageReferencesForCopyTrigger(trigger);
  const value = kind === "duckdb"
    ? references?.duckdbReference || references?.duckdbPath || ""
    : references?.virtualPath || "";
  await writeTextToClipboard(value, {
    emptyMessage:
      kind === "duckdb"
        ? "There is no DuckDB result storage path to copy."
        : "There is no virtual result storage path to copy.",
  });
  if (trigger instanceof HTMLButtonElement) {
    const previousTitle = trigger.title;
    trigger.classList.add("is-copied");
    trigger.title = "Copied";
    window.setTimeout(() => {
      trigger.classList.remove("is-copied");
      trigger.title = previousTitle || (kind === "duckdb" ? "Copy DuckDB path" : "Copy virtual path");
    }, 1200);
  }
  return true;
}

function syncEditorExpandButton(editorRoot) {
  if (!(editorRoot instanceof Element)) {
    return;
  }

  const button = editorRoot.querySelector("[data-expand-editor]");
  if (!(button instanceof HTMLButtonElement)) {
    return;
  }

  const expanded = editorRoot.classList.contains("is-editor-expanded");
  button.textContent = expanded ? "-" : "+";
  button.title = expanded ? "Collapse SQL editor" : "Expand SQL editor";
  button.setAttribute("aria-label", button.title);
  button.setAttribute("aria-pressed", expanded ? "true" : "false");
}

function setEditorExpanded(editorRoot, expanded) {
  if (!(editorRoot instanceof Element)) {
    return false;
  }

  editorRoot.classList.toggle("is-editor-expanded", expanded);
  resetEditorManualSizing(editorRoot);
  autosizeEditor(editorRoot);
  window.requestAnimationFrame(() => autosizeEditor(editorRoot));
  syncEditorExpandButton(editorRoot);
  return true;
}

function toggleEditorExpanded(editorRoot) {
  if (!(editorRoot instanceof Element)) {
    return false;
  }

  return setEditorExpanded(editorRoot, !editorRoot.classList.contains("is-editor-expanded"));
}

function toggleQueryResultPanel(button) {
  if (!(button instanceof HTMLButtonElement)) {
    return false;
  }
  const resultRoot = button.closest("[data-cell-result]");
  const body = resultRoot?.querySelector("[data-query-result-body]");
  if (!(resultRoot instanceof Element) || !(body instanceof Element)) {
    return false;
  }

  const key = String(resultRoot.dataset.queryResultCollapseKey || resultRoot.dataset.queryJobId || "").trim();
  const expanded = button.getAttribute("aria-expanded") === "true";
  const nextCollapsed = expanded;
  if (key) {
    if (nextCollapsed) {
      collapsedQueryResultKeys.add(key);
    } else {
      collapsedQueryResultKeys.delete(key);
    }
  }
  body.hidden = nextCollapsed;
  resultRoot.dataset.queryResultCollapsed = nextCollapsed ? "true" : "false";
  button.setAttribute("aria-expanded", nextCollapsed ? "false" : "true");
  button.setAttribute("aria-label", nextCollapsed ? "Show result" : "Hide result");
  button.title = nextCollapsed ? "Show result" : "Hide result";
  return true;
}

function syncQueryResultChartsToggle(button, visible) {
  if (!(button instanceof HTMLButtonElement)) {
    return;
  }
  const title = visible ? "Hide resource charts" : "Show resource charts";
  button.setAttribute("aria-pressed", visible ? "true" : "false");
  button.title = title;
  const label = button.querySelector("[data-query-result-charts-toggle-label]");
  if (label) {
    label.textContent = title;
  }
}

function toggleQueryResultCharts(button) {
  if (!(button instanceof HTMLButtonElement)) {
    return false;
  }
  const resultRoot = button.closest("[data-cell-result]");
  if (!(resultRoot instanceof Element)) {
    return false;
  }
  const nextVisible = button.getAttribute("aria-pressed") !== "true";
  const key = String(resultRoot.dataset.queryResultChartsKey || resultRoot.dataset.queryJobId || "").trim();
  if (key) {
    if (nextVisible) {
      visibleQueryResultChartKeys.add(key);
    } else {
      visibleQueryResultChartKeys.delete(key);
    }
  }
  resultRoot.dataset.queryResultChartsVisible = nextVisible ? "true" : "false";
  resultRoot.querySelectorAll("[data-query-resource-sparklines]").forEach((sparklineRoot) => {
    sparklineRoot.hidden = !nextVisible;
  });
  syncQueryResultChartsToggle(button, nextVisible);
  if (nextVisible) {
    window.requestAnimationFrame(() => {
      queryResourceChartsController.initialize(resultRoot);
    });
  }
  return true;
}

function syncQueryTimingDetailsToggle(button, visible) {
  if (!(button instanceof HTMLButtonElement)) {
    return;
  }
  button.setAttribute("aria-expanded", visible ? "true" : "false");
}

function toggleQueryTimingDetails(button) {
  if (!(button instanceof HTMLButtonElement)) {
    return false;
  }
  const resultRoot = button.closest("[data-cell-result]");
  const panel = resultRoot?.querySelector("[data-query-duration-details-panel]");
  if (!(resultRoot instanceof Element) || !(panel instanceof Element)) {
    return false;
  }
  const nextVisible = button.getAttribute("aria-expanded") !== "true";
  const key = String(resultRoot.dataset.queryTimingDetailsKey || resultRoot.dataset.queryJobId || "").trim();
  if (key) {
    if (nextVisible) {
      visibleQueryTimingDetailKeys.add(key);
    } else {
      visibleQueryTimingDetailKeys.delete(key);
    }
  }
  panel.hidden = !nextVisible;
  resultRoot.dataset.queryTimingDetailsVisible = nextVisible ? "true" : "false";
  syncQueryTimingDetailsToggle(button, nextVisible);
  return true;
}


function defaultLocalNotebookTitle() {
  const localNotebookCount = Object.keys(readStoredNotebookMetadata()).filter((key) =>
    isLocalNotebookId(key)
  ).length;

  return `Untitled Notebook ${localNotebookCount + 1}`;
}

function notebookVisibilityLabel(shared) {
  return shared ? "Public / Shared" : "Private / Local";
}

function notebookVisibilityTitle(shared) {
  return shared
    ? "Shared with connected users and stored on the server."
    : "Private to this browser workspace.";
}

function createNotebookLinkElement(notebookId, metadata) {
  const deleteInProgress = Boolean(metadata.deleteInProgress);
  const link = document.createElement("a");
  link.href = notebookUrl(notebookId) || "#";
  link.className = "notebook-link notebook-tree-leaf";
  link.classList.toggle("is-deleting", deleteInProgress);
  link.dataset.notebookId = notebookId;
  link.dataset.notebookTitle = metadata.title;
  link.dataset.notebookSummary = metadata.summary;
  link.dataset.createdAt = metadata.createdAt || new Date().toISOString();
  link.dataset.notebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookTitle = metadata.title;
  link.dataset.defaultNotebookSummary = metadata.summary;
  link.dataset.defaultNotebookPipelineMode = normalizeNotebookPipelineMode(metadata.pipelineMode);
  link.dataset.defaultNotebookPipelinePaths = JSON.stringify(normalizePipelinePaths(metadata.pipelinePaths));
  link.dataset.defaultNotebookVersions = JSON.stringify(metadata.versions ?? []);
  link.dataset.defaultNotebookCells = JSON.stringify(
    (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      language: normalizeCellLanguage(cell.language),
      processingHints: cell.processingHints || "",
      resultExpectations: cell.resultExpectations || "",
      dataSources: normalizeDataSources(cell.dataSources),
      queryOptions: normalizeCellQueryOptions(cell.queryOptions),
      stage: normalizeCellStage(cell.stage),
      sql: cell.sql,
    }))
  );
  link.dataset.defaultNotebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookTags = metadata.tags.join("||");
  link.dataset.shared = metadata.shared ? "true" : "false";
  link.dataset.defaultNotebookShared = metadata.shared ? "true" : "false";
  link.dataset.canEdit = metadata.canEdit ? "true" : "false";
  link.dataset.canDelete = metadata.canDelete ? "true" : "false";
  link.dataset.deleteInProgress = deleteInProgress ? "true" : "false";
  link.dataset.draggableNotebook = "";
  link.draggable = Boolean(metadata.canEdit && !deleteInProgress);

  const titleRow = document.createElement("span");
  titleRow.className = "notebook-title-row";

  const title = document.createElement("span");
  title.className = "notebook-title";
  title.textContent = metadata.title;
  titleRow.append(title);

  const sharedBadge = document.createElement("small");
  sharedBadge.className = "notebook-sharing-pill";
  sharedBadge.textContent = deleteInProgress
    ? "DELETION IN PROGRESS"
    : notebookVisibilityLabel(metadata.shared);
  sharedBadge.title = deleteInProgress
    ? "Notebook deletion is in progress."
    : notebookVisibilityTitle(metadata.shared);
  sharedBadge.dataset.tone = deleteInProgress ? "deleting" : "";
  titleRow.append(sharedBadge);

  const tools = document.createElement("span");
  tools.className = "notebook-item-tools";

  const renameButton = document.createElement("button");
  renameButton.type = "button";
  renameButton.className = `tree-add-button tree-add-button-inline notebook-action-pill${
    metadata.canEdit ? "" : " is-action-disabled"
  }`;
  renameButton.dataset.sidebarRenameNotebook = "";
  renameButton.textContent = "Rename";
  renameButton.title = deleteInProgress
    ? "Notebook deletion is in progress."
    : metadata.canEdit
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
  editButton.title = deleteInProgress
    ? "Notebook deletion is in progress."
    : metadata.canEdit
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
  deleteButton.title = deleteInProgress
    ? "Notebook deletion is in progress."
    : metadata.canDelete
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
  const deleteInProgress = notebookDeletionInProgress(notebookId);
  if (!defaults.canEdit) {
    const readOnlyMetadata = {
      ...defaults,
      notebookId,
      title: normalizeNotebookTitleValue(defaults.title),
      summary: normalizeNotebookSummaryValue(defaults.summary),
      pipelineMode: normalizeNotebookPipelineMode(defaults.pipelineMode),
      pipelinePaths: normalizePipelinePaths(defaults.pipelinePaths),
      cells: normalizeNotebookCells(defaults.cells),
      dataSources: notebookSourceIds({ cells: defaults.cells }),
      tags: normalizeTags(defaults.tags),
      sql: defaults.cells[0]?.sql ?? "",
      deleted: false,
      versions: defaults.versions?.length
        ? defaults.versions
        : [createInitialNotebookVersion(notebookId, defaults)],
    };
    readOnlyMetadata.deleteInProgress = deleteInProgress;
    if (deleteInProgress) {
      readOnlyMetadata.canEdit = false;
      readOnlyMetadata.canDelete = false;
    }

    if (!defaults.payloadsDeferred) {
      updateStoredNotebookState(notebookId, () => ({
        title: readOnlyMetadata.title,
        summary: readOnlyMetadata.summary,
        pipelineMode: readOnlyMetadata.pipelineMode,
        pipelinePaths: readOnlyMetadata.pipelinePaths,
        tags: readOnlyMetadata.tags,
        cells: readOnlyMetadata.cells,
        deleted: false,
        versions: readOnlyMetadata.versions,
        shared: defaults.shared,
      }));
    }

    return readOnlyMetadata;
  }

  const sharedDraftState = defaults.shared ? normalizeStoredNotebookState(sharedNotebookDrafts.get(notebookId)) : {};
  const storedState = defaults.shared
    ? sharedDraftState
    : normalizeStoredNotebookState(readStoredNotebookMetadata()[notebookId]);
  const cells = normalizeNotebookCells(storedState.cells ?? defaults.cells);
  const resolvedTitle = normalizeNotebookTitleValue(storedState.title, defaults.title);
  const resolvedSummary = normalizeNotebookSummaryValue(storedState.summary, defaults.summary);
  const resolvedPipelineMode = normalizeNotebookPipelineMode(storedState.pipelineMode, defaults.pipelineMode);
  const resolvedPipelinePaths = normalizePipelinePaths(storedState.pipelinePaths, defaults.pipelinePaths);
  const baseMetadata = {
    ...defaults,
    notebookId,
    title: resolvedTitle,
    summary: resolvedSummary,
    createdAt: defaults.createdAt,
    linkedGeneratorId: defaults.linkedGeneratorId,
    pipelineMode: resolvedPipelineMode,
    pipelinePaths: resolvedPipelinePaths,
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
      pipelineMode: normalizeNotebookPipelineMode(currentState.pipelineMode, baseMetadata.pipelineMode),
      pipelinePaths: normalizePipelinePaths(currentState.pipelinePaths, baseMetadata.pipelinePaths),
      tags: currentState.tags ?? baseMetadata.tags,
      cells: currentState.cells ?? baseMetadata.cells,
      shared: currentState.shared ?? baseMetadata.shared,
      deleted: currentState.deleted ?? baseMetadata.deleted,
      versions,
    }));
  }

  return {
    ...baseMetadata,
    canEdit: deleteInProgress ? false : baseMetadata.canEdit,
    canDelete: deleteInProgress ? false : baseMetadata.canDelete,
    deleteInProgress,
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
      language: normalizeCellLanguage(cell.language),
      processingHints: cell.processingHints || "",
      resultExpectations: cell.resultExpectations || "",
      dataSources: normalizeDataSources(cell.dataSources),
      queryOptions: normalizeCellQueryOptions(cell.queryOptions),
      stage: normalizeCellStage(cell.stage),
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
  const shouldPreserveNotebookId = metadata.shared === true && !isLocalNotebookId(notebookId);
  return {
    notebookId: shouldPreserveNotebookId ? notebookId : null,
    title: metadata.title,
    summary: metadata.summary,
    tags: normalizeTags(metadata.tags),
    pipelineMode: normalizeNotebookPipelineMode(metadata.pipelineMode),
    pipelinePaths: normalizePipelinePaths(metadata.pipelinePaths),
    treePath: notebookTreePathForId(notebookId),
    linkedGeneratorId: metadata.linkedGeneratorId || "",
    createdAt: metadata.createdAt || new Date().toISOString(),
    cells: (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      language: normalizeCellLanguage(cell.language),
      sql: cell.sql,
      processingHints: cell.processingHints || "",
      resultExpectations: cell.resultExpectations || "",
      dataSources: normalizeDataSources(cell.dataSources),
      queryOptions: normalizeCellQueryOptions(cell.queryOptions),
      stage: normalizeCellStage(cell.stage),
    })),
    versions: (metadata.versions ?? []).map((version) => ({
      versionId: version.versionId,
      createdAt: version.createdAt,
      title: version.title,
      summary: version.summary,
      tags: normalizeTags(version.tags),
      cells: normalizeNotebookCells(version.cells).map((cell) => ({
        cellId: cell.cellId,
        language: normalizeCellLanguage(cell.language),
        sql: cell.sql,
        processingHints: cell.processingHints || "",
        resultExpectations: cell.resultExpectations || "",
        dataSources: normalizeDataSources(cell.dataSources),
        queryOptions: normalizeCellQueryOptions(cell.queryOptions),
        stage: normalizeCellStage(cell.stage),
      })),
    })),
  };
}

function metadataFromSharedNotebookPayload(notebook) {
  const notebookId = String(notebook?.notebookId || "").trim();
  const cells = normalizeNotebookCells(notebook?.cells ?? []);
  const metadata = {
    notebookId,
    title: normalizeNotebookTitleValue(notebook?.title),
    summary: normalizeNotebookSummaryValue(notebook?.summary),
    pipelineMode: normalizeNotebookPipelineMode(notebook?.pipelineMode),
    pipelinePaths: normalizePipelinePaths(notebook?.pipelinePaths),
    createdAt: String(notebook?.createdAt || new Date().toISOString()),
    linkedGeneratorId: String(notebook?.linkedGeneratorId || ""),
    cells,
    dataSources: notebookSourceIds({ cells }),
    tags: normalizeTags(notebook?.tags ?? []),
    canEdit: notebook?.canEdit !== false,
    canDelete: notebook?.canDelete !== false,
    shared: true,
    deleted: false,
    versions: sortVersionsDescending(
      (notebook?.versions ?? []).map((version) => normalizeVersionEntry(version)).filter(Boolean)
    ),
  };
  if (!metadata.versions.length) {
    metadata.versions = [createInitialNotebookVersion(notebookId, metadata)];
  }
  return metadata;
}

function writeNotebookDefaultsToMetaRoot(metaRoot, metadata) {
  if (!metaRoot || !metadata?.notebookId) {
    return;
  }

  metaRoot.dataset.defaultTitle = metadata.title;
  metaRoot.dataset.defaultSummary = metadata.summary;
  metaRoot.dataset.defaultPipelineMode = normalizeNotebookPipelineMode(metadata.pipelineMode);
  metaRoot.dataset.defaultPipelinePaths = JSON.stringify(normalizePipelinePaths(metadata.pipelinePaths));
  metaRoot.dataset.createdAt = metadata.createdAt;
  metaRoot.dataset.defaultCreatedAt = metadata.createdAt;
  metaRoot.dataset.linkedGeneratorId = metadata.linkedGeneratorId || "";
  metaRoot.dataset.defaultCells = JSON.stringify(
    (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      language: normalizeCellLanguage(cell.language),
      processingHints: cell.processingHints || "",
      resultExpectations: cell.resultExpectations || "",
      dataSources: normalizeDataSources(cell.dataSources),
      queryOptions: normalizeCellQueryOptions(cell.queryOptions),
      stage: normalizeCellStage(cell.stage),
      sql: cell.sql,
    }))
  );
  metaRoot.dataset.defaultVersions = JSON.stringify(metadata.versions ?? []);
  metaRoot.dataset.defaultTags = normalizeTags(metadata.tags ?? []).join("||");
  metaRoot.dataset.defaultShared = metadata.shared ? "true" : "false";
  metaRoot.dataset.shared = metadata.shared ? "true" : "false";
  metaRoot.dataset.canEdit = metadata.canEdit ? "true" : "false";
  metaRoot.dataset.canDelete = metadata.canDelete ? "true" : "false";
}

function promoteSyncedSharedNotebook(sharedNotebook) {
  if (!sharedNotebook?.notebookId) {
    return null;
  }

  const metadata = metadataFromSharedNotebookPayload(sharedNotebook);
  notebookLinks(metadata.notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));
  const metaRoot = activeWorkspaceMetaRoot(metadata.notebookId);
  if (metaRoot) {
    writeNotebookDefaultsToMetaRoot(metaRoot, metadata);
    applyWorkspaceMetadata(metaRoot, metadata);
  }
  applySidebarSearchFilter();
  return metadata;
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
  const normalizedFolderPath = Array.isArray(folderPath)
    ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
    : [];

  const removal = removeNotebookFromStoredTree(currentTree, notebookId);
  const treeAfterRemoval = removal.nodes;

  if (normalizedFolderPath.length === 0) {
    writeStoredNotebookTree([
      ...(Array.isArray(treeAfterRemoval) ? treeAfterRemoval : []),
      notebookNode,
    ]);
    return;
  }

  const nextTree = ensureNotebookInFolderPathState(
    treeAfterRemoval,
    notebookId,
    normalizedFolderPath,
  );
  writeStoredNotebookTree(nextTree.state);
}

function sharedNotebookFolderPayload(folder, overrides = {}) {
  const folderPath = treeFolderPath(folder);
  return {
    path: folderPath,
    displayName: folderPath[folderPath.length - 1] || "",
    isPublic: overrides.isPublic ?? folderIsShared(folder),
    canEdit: folder?.dataset?.canEdit !== "false",
    canDelete: folder?.dataset?.canDelete !== "false",
  };
}

async function upsertSharedNotebookFolder(folder, overrides = {}) {
  const payload = sharedNotebookFolderPayload(folder, overrides);
  if (!payload.path.length) {
    return null;
  }

  const response = await window.fetch("/api/notebooks/shared/folders", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`Failed to save notebook folder metadata: ${response.status}`);
  }

  return response.json();
}

async function setSharedNotebookFolderVisibility(folder, isPublic) {
  const payload = sharedNotebookFolderPayload(folder, { isPublic });
  if (!payload.path.length) {
    return null;
  }

  const response = await window.fetch("/api/notebooks/shared/folders/visibility", {
    method: "PATCH",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      path: payload.path,
      displayName: payload.displayName,
      isPublic,
    }),
  });
  if (!response.ok) {
    throw new Error(`Failed to update notebook folder visibility: ${response.status}`);
  }

  return response.json();
}

async function deleteSharedNotebookFolder(folder) {
  const payload = sharedNotebookFolderPayload(folder);
  if (!payload.path.length) {
    return null;
  }

  const response = await window.fetch("/api/notebooks/shared/folders", {
    method: "DELETE",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      path: payload.path,
    }),
  });
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Failed to delete notebook folder metadata: ${response.status}`);
  }

  return response.json();
}

async function syncSharedNotebookNow(notebookId) {
  if (!notebookId || notebookDeletionInProgress(notebookId) || !notebookMetadata(notebookId).shared) {
    return null;
  }

  const requestPayload = sharedNotebookPayload(notebookId);
  const draftAtRequest = sharedNotebookDrafts.get(notebookId);

  const response = await window.fetch("/api/notebooks/shared", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Workbench-Client-Id": workbenchClientId(),
    },
    body: JSON.stringify(requestPayload),
  });
  if (!response.ok) {
    throw new Error(`Failed to sync shared notebook ${notebookId}: ${response.status}`);
  }

  const payload = await response.json();
  const sharedNotebook = payload?.notebook;
  if (!sharedNotebook?.notebookId) {
    return payload;
  }

  if (sharedNotebookDrafts.get(sharedNotebook.notebookId) === draftAtRequest) {
    promoteSyncedSharedNotebook(sharedNotebook);
    sharedNotebookDrafts.delete(sharedNotebook.notebookId);
  }
  return payload;
}

function scheduleSharedNotebookSync(notebookId, delayMs = 450) {
  if (!notebookId || notebookDeletionInProgress(notebookId) || !notebookMetadata(notebookId).shared) {
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

function notebookAbsoluteReference(notebookId) {
  const relativeUrl = notebookUrl(notebookId);
  if (!relativeUrl) {
    return "";
  }
  return new URL(relativeUrl, window.location.origin).href;
}

function notebookShareDialogElements(dialog) {
  return {
    copy: dialog.querySelector("[data-notebook-share-copy]"),
    copyButton: dialog.querySelector("[data-notebook-share-copy-reference]"),
    emailButton: dialog.querySelector("[data-notebook-share-email]"),
    promoteButton: dialog.querySelector("[data-notebook-share-promote]"),
    referenceInput: dialog.querySelector("[data-notebook-share-reference]"),
    status: dialog.querySelector("[data-notebook-share-status]"),
    title: dialog.querySelector("[data-notebook-share-title]"),
  };
}

function setNotebookShareDialogStatus(dialog, message = "", tone = "neutral") {
  const status = notebookShareDialogElements(dialog).status;
  if (!status) {
    return;
  }
  status.textContent = message;
  status.dataset.tone = tone;
}

function renderNotebookShareDialog(dialog, notebookId) {
  const metadata = notebookMetadata(notebookId);
  const title = metadata.title || "Untitled Notebook";
  const reference = notebookAbsoluteReference(notebookId);
  const isLocalNotebook = isLocalNotebookId(notebookId);
  const {
    copy,
    copyButton,
    emailButton,
    promoteButton,
    referenceInput,
    title: titleNode,
  } = notebookShareDialogElements(dialog);

  dialog.dataset.notebookId = notebookId;
  if (titleNode) {
    titleNode.textContent = "Share Notebook";
  }
  if (copy) {
    copy.textContent = isLocalNotebook
      ? `Share "${title}" to create a stable notebook reference.`
      : `Use this reference to open "${title}" directly.`;
  }
  if (referenceInput) {
    referenceInput.value = reference;
    referenceInput.placeholder = isLocalNotebook
      ? "Share this local notebook to create a stable reference."
      : "";
    referenceInput.disabled = !reference;
  }
  if (promoteButton) {
    promoteButton.hidden = !isLocalNotebook;
    promoteButton.disabled = false;
  }
  if (copyButton) {
    copyButton.disabled = !reference;
  }
  if (emailButton) {
    emailButton.disabled = !reference;
  }
  setNotebookShareDialogStatus(
    dialog,
    isLocalNotebook ? "This notebook is private to this browser until it is shared." : "",
    "neutral"
  );
}

function ensureNotebookShareDialogController(dialog) {
  if (!dialog || dialog.dataset.notebookShareControllerReady === "true") {
    return;
  }
  dialog.dataset.notebookShareControllerReady = "true";

  dialog.addEventListener("click", async (event) => {
    const copyButton = event.target.closest("[data-notebook-share-copy-reference]");
    const emailButton = event.target.closest("[data-notebook-share-email]");
    const promoteButton = event.target.closest("[data-notebook-share-promote]");
    if (!copyButton && !emailButton && !promoteButton) {
      return;
    }
    event.preventDefault();

    const notebookId = dialog.dataset.notebookId || "";
    const reference = notebookShareDialogElements(dialog).referenceInput?.value || "";
    if (copyButton) {
      try {
        await writeTextToClipboard(reference, {
          emptyMessage: "There is no notebook reference to copy yet.",
        });
        setNotebookShareDialogStatus(dialog, "Notebook reference copied.", "success");
      } catch (error) {
        setNotebookShareDialogStatus(
          dialog,
          error instanceof Error ? error.message : "The notebook reference could not be copied.",
          "error"
        );
      }
      return;
    }

    if (emailButton) {
      if (!reference) {
        setNotebookShareDialogStatus(dialog, "Share the notebook before sending an email draft.", "error");
        return;
      }
      const metadata = notebookMetadata(notebookId);
      const subject = encodeURIComponent(`Notebook reference: ${metadata.title || "Untitled Notebook"}`);
      const body = encodeURIComponent(`${metadata.title || "Untitled Notebook"}\n\n${reference}`);
      window.location.href = `mailto:?subject=${subject}&body=${body}`;
      return;
    }

    if (!promoteButton || !notebookId) {
      return;
    }
    promoteButton.disabled = true;
    setNotebookShareDialogStatus(dialog, "Sharing notebook and creating reference...", "neutral");
    try {
      const payload = await shareNotebook(notebookId);
      const sharedNotebookId = payload?.notebook?.notebookId || currentWorkspaceNotebookId();
      if (sharedNotebookId) {
        renderNotebookShareDialog(dialog, sharedNotebookId);
        setNotebookShareDialogStatus(dialog, "Notebook is shared and the reference is ready.", "success");
      }
    } catch (error) {
      promoteButton.disabled = false;
      console.error("Failed to share notebook from share dialog.", error);
      setNotebookShareDialogStatus(dialog, "The notebook could not be shared.", "error");
    }
  });
}

async function openNotebookShareDialog(notebookId) {
  const dialog = ensureNotebookShareDialog();
  ensureNotebookShareDialogController(dialog);
  renderNotebookShareDialog(dialog, notebookId);
  if (dialog.showModal) {
    dialog.showModal();
  } else {
    dialog.setAttribute("open", "");
  }
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
    const cellLanguage = normalizeCellLanguage(cell.language);
    const sqlText = cell.sql || `No ${cellLanguage === "python" ? "code" : "SQL"} saved.`;
    return `Cell ${index + 1} (${cellLanguage.toUpperCase()}) Sources: ${sources}\nCell ${index + 1} ${cellLanguage === "python" ? "Code" : "SQL"}:\n${sqlText}`;
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
  const deleteInProgress = Boolean(metadata.deleteInProgress);
  link.dataset.notebookTitle = metadata.title;
  link.dataset.notebookSummary = metadata.summary;
  link.dataset.createdAt = metadata.createdAt || link.dataset.createdAt || new Date().toISOString();
  link.dataset.notebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookTitle = metadata.title;
  link.dataset.defaultNotebookSummary = metadata.summary;
  link.dataset.defaultNotebookPipelineMode = normalizeNotebookPipelineMode(metadata.pipelineMode);
  link.dataset.defaultNotebookPipelinePaths = JSON.stringify(normalizePipelinePaths(metadata.pipelinePaths));
  link.dataset.defaultNotebookVersions = JSON.stringify(metadata.versions ?? []);
  link.dataset.defaultNotebookDataSources = normalizeDataSources(metadata.dataSources).join("||");
  link.dataset.defaultNotebookTags = normalizeTags(metadata.tags ?? []).join("||");
  link.dataset.shared = metadata.shared ? "true" : "false";
  link.dataset.defaultNotebookShared = metadata.shared ? "true" : "false";
  link.dataset.canEdit = metadata.canEdit ? "true" : "false";
  link.dataset.canDelete = metadata.canDelete ? "true" : "false";
  link.dataset.deleteInProgress = deleteInProgress ? "true" : "false";
  link.classList.toggle("is-deleting", deleteInProgress);
  link.draggable = Boolean(metadata.canEdit && !deleteInProgress);
  link.dataset.defaultNotebookCells = JSON.stringify(
    (metadata.cells ?? []).map((cell) => ({
        cellId: cell.cellId,
        language: normalizeCellLanguage(cell.language),
        processingHints: cell.processingHints || "",
        resultExpectations: cell.resultExpectations || "",
        dataSources: normalizeDataSources(cell.dataSources),
      queryOptions: normalizeCellQueryOptions(cell.queryOptions),
      stage: normalizeCellStage(cell.stage),
      sql: cell.sql,
    }))
  );

  const titleNode = link.querySelector(".notebook-title");
  if (titleNode) {
    titleNode.textContent = metadata.title;
  }

  let sharedBadge = link.querySelector(".notebook-sharing-pill");
  if (!sharedBadge) {
    sharedBadge = document.createElement("small");
    sharedBadge.className = "notebook-sharing-pill";
    titleNode?.after(sharedBadge);
  }
  sharedBadge.textContent = deleteInProgress
    ? "DELETION IN PROGRESS"
    : notebookVisibilityLabel(metadata.shared);
  sharedBadge.title = deleteInProgress
    ? "Notebook deletion is in progress."
    : notebookVisibilityTitle(metadata.shared);
  sharedBadge.dataset.tone = deleteInProgress ? "deleting" : "";

  const summaryNode = link.querySelector(".notebook-summary");
  if (summaryNode) {
    summaryNode.textContent = metadata.summary;
  }

  const renameButton = link.querySelector("[data-sidebar-rename-notebook]");
  if (renameButton) {
    renameButton.disabled = !metadata.canEdit;
    renameButton.classList.toggle("is-action-disabled", !metadata.canEdit);
    renameButton.title = deleteInProgress
      ? "Notebook deletion is in progress."
      : metadata.canEdit
        ? "Rename notebook"
        : "This notebook cannot be renamed.";
  }

  const editButton = link.querySelector("[data-sidebar-edit-notebook]");
  if (editButton) {
    editButton.disabled = !metadata.canEdit;
    editButton.classList.toggle("is-action-disabled", !metadata.canEdit);
    editButton.title = deleteInProgress
      ? "Notebook deletion is in progress."
      : metadata.canEdit
        ? "Edit notebook metadata"
        : "This notebook cannot be edited.";
  }

  const deleteButton = link.querySelector("[data-sidebar-delete-notebook]");
  if (deleteButton) {
    deleteButton.disabled = !metadata.canDelete;
    deleteButton.classList.toggle("is-action-disabled", !metadata.canDelete);
    deleteButton.title = deleteInProgress
      ? "Notebook deletion is in progress."
      : metadata.canDelete
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

function setNotebookPipelineMode(notebookId, pipelineMode, options = {}) {
  const normalizedMode = normalizeNotebookPipelineMode(pipelineMode);
  persistNotebookDraft(notebookId, { pipelineMode: normalizedMode });
  const metadata = notebookMetadata(notebookId);
  notebookLinks(notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));
  recordNotebookActivity(notebookId, "edited");
  if (options.rerender) {
    renderLocalNotebookWorkspace(notebookId);
  } else {
    applyNotebookMetadata();
  }
  scheduleSharedNotebookSync(notebookId);
  return metadata;
}

function setNotebookPipelinePaths(notebookId, pipelinePaths, options = {}) {
  persistNotebookDraft(notebookId, { pipelinePaths: normalizePipelinePaths(pipelinePaths) });
  const metadata = notebookMetadata(notebookId);
  const syncSharedNotebook = () => {
    if (options.syncNow) {
      const existingHandle = sharedNotebookSyncHandles.get(notebookId);
      if (existingHandle) {
        window.clearTimeout(existingHandle);
        sharedNotebookSyncHandles.delete(notebookId);
      }
      syncSharedNotebookNow(notebookId).catch((error) => {
        console.error("Failed to sync shared notebook pipeline paths.", error);
      });
      return;
    }
    scheduleSharedNotebookSync(notebookId);
  };
  if (options.silent) {
    recordNotebookActivity(notebookId, "edited");
    syncSharedNotebook();
    return metadata;
  }
  notebookLinks(notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));
  recordNotebookActivity(notebookId, "edited");
  if (options.rerender) {
    renderLocalNotebookWorkspace(notebookId);
  } else {
    applyNotebookMetadata();
  }
  syncSharedNotebook();
  return metadata;
}

function createEmptyCellState(initial = {}) {
  return normalizeCellEntry(
    {
      cellId: initial.cellId ?? createCellId(),
      language: normalizeCellLanguage(initial.language),
      processingHints: initial.processingHints ?? "",
      resultExpectations: initial.resultExpectations ?? "",
      dataSources: initial.dataSources ?? [],
      queryOptions: initial.queryOptions ?? {},
      stage: initial.stage ?? {},
      sql: initial.sql ?? "",
    },
    {
      cellId: initial.cellId ?? createCellId(),
      language: normalizeCellLanguage(initial.language),
      processingHints: initial.processingHints ?? "",
      resultExpectations: initial.resultExpectations ?? "",
      dataSources: initial.dataSources ?? [],
      queryOptions: initial.queryOptions ?? {},
      stage: initial.stage ?? {},
      sql: initial.sql ?? "",
    }
  );
}

function createSourceQueryCellState(sourceDescriptor, fields = []) {
  const relation = String(sourceDescriptor?.relation ?? "").trim();
  const sourceId = String(sourceDescriptor?.sourceId ?? "").trim();
  const dataSourceId = sourceId === "s3" && relation.toLowerCase().startsWith("s3.")
    ? relation
    : sourceId;
  return createEmptyCellState({
    language: "sql",
    dataSources: dataSourceId ? [dataSourceId] : [],
    sql: sourceQuerySql(relation, fields),
  });
}

function setNotebookCells(notebookId, cells, options = {}) {
  persistNotebookDraft(notebookId, { cells: normalizeNotebookCells(cells) });
  const metadata = notebookMetadata(notebookId);
  notebookLinks(notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));
  recordNotebookActivity(notebookId, "edited");

  if (options.rerender) {
    renderLocalNotebookWorkspace(notebookId);
    scheduleSharedNotebookSync(notebookId);
    return metadata;
  }

  applyNotebookMetadata();
  applySidebarSearchFilter();
  scheduleSharedNotebookSync(notebookId);
  return metadata;
}

function flushNotebookEditorValues(notebookId) {
  if (!notebookId) {
    return;
  }

  const workspaceRoot = document.querySelector(
    `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"]`
  );
  if (!workspaceRoot) {
    return;
  }

  const metadataCells = new Map(
    (notebookMetadata(notebookId).cells ?? []).map((cell) => [cell.cellId, cell])
  );
  workspaceRoot.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
    const cellId = cellRoot.dataset.cellId;
    const editorRoot = cellRoot.querySelector("[data-editor-root]");
    if (!cellId || !editorRoot) {
      return;
    }

    const sqlText = currentEditorSql(editorRoot);
    const textarea = editorRoot.querySelector("[data-editor-source]");
    if (textarea && textarea.value !== sqlText) {
      textarea.value = sqlText;
    }
    if (metadataCells.get(cellId)?.sql !== sqlText) {
      setCellSql(notebookId, cellId, sqlText);
    }
  });
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
  const cellRoot = document.querySelector(
    `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"] [data-query-cell][data-cell-id="${CSS.escape(cellId)}"]`
  );
  querySourceValidationController.refreshCell(cellRoot);
}

function setCellQueryOptions(notebookId, cellId, queryOptions) {
  const normalizedQueryOptions = normalizeCellQueryOptions(queryOptions);
  updateStoredNotebookState(notebookId, (currentState) => {
    const baseCells = normalizeNotebookCells(currentState.cells ?? notebookMetadata(notebookId).cells);
    return {
      ...currentState,
      cells: baseCells.map((cell) =>
        cell.cellId === cellId
          ? {
              ...cell,
              queryOptions: normalizedQueryOptions,
            }
          : cell
      ),
    };
  });

  const metadata = notebookMetadata(notebookId);
  notebookLinks(notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));
  applyNotebookMetadata();
  recordNotebookActivity(notebookId, "edited");
  scheduleSharedNotebookSync(notebookId);
}

function setCellStage(notebookId, cellId, stagePatch, options = {}) {
  updateStoredNotebookState(notebookId, (currentState) => {
    const baseCells = normalizeNotebookCells(currentState.cells ?? notebookMetadata(notebookId).cells);
    return {
      ...currentState,
      cells: baseCells.map((cell) =>
        cell.cellId === cellId
          ? {
              ...cell,
              stage: normalizeCellStage({
                ...normalizeCellStage(cell.stage),
                ...(stagePatch && typeof stagePatch === "object" ? stagePatch : {}),
              }),
            }
          : cell
      ),
    };
  });

  const metadata = notebookMetadata(notebookId);
  notebookLinks(notebookId).forEach((link) => updateSidebarNotebookLink(link, metadata));
  recordNotebookActivity(notebookId, "edited");
  if (options.rerender) {
    renderLocalNotebookWorkspace(notebookId);
  } else {
    applyNotebookMetadata();
  }
  scheduleSharedNotebookSync(notebookId);
  return metadata;
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

function setCellDescriptor(notebookId, cellId, descriptorName, value) {
  const normalizedName = String(descriptorName || "").trim();
  if (!["processingHints", "resultExpectations"].includes(normalizedName)) {
    return;
  }

  updateStoredNotebookState(notebookId, (currentState) => {
    const baseCells = normalizeNotebookCells(currentState.cells ?? notebookMetadata(notebookId).cells);
    return {
      ...currentState,
      cells: baseCells.map((cell) =>
        cell.cellId === cellId
          ? {
              ...cell,
              [normalizedName]: String(value ?? ""),
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
  flushNotebookEditorValues(notebookId);
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
  flushNotebookEditorValues(notebookId);
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
    language: nextCells[index].language,
    dataSources: [...nextCells[index].dataSources],
    queryOptions: normalizeCellQueryOptions(nextCells[index].queryOptions),
    sql: nextCells[index].sql,
  });
  nextCells.splice(index + 1, 0, duplicate);
  setNotebookCells(notebookId, nextCells, { rerender: true });
}

function moveCell(notebookId, cellId, direction) {
  flushNotebookEditorValues(notebookId);
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
  flushNotebookEditorValues(notebookId);
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

function setVirtualEditorSql(editorRoot, sql) {
  if (!(editorRoot instanceof Element)) {
    return false;
  }
  const nextSql = String(sql ?? "");
  const textarea = editorRoot.querySelector("[data-editor-source]");
  const editor = editorRegistry.get(editorRoot);
  if (editor) {
    const currentSql = editor.state.doc.toString();
    if (currentSql !== nextSql) {
      editor.dispatch({
        changes: {
          from: 0,
          to: editor.state.doc.length,
          insert: nextSql,
        },
      });
    } else if (textarea instanceof HTMLTextAreaElement && textarea.value !== nextSql) {
      textarea.value = nextSql;
    }
  } else if (textarea instanceof HTMLTextAreaElement && textarea.value !== nextSql) {
    textarea.value = nextSql;
    invalidatePreparedSqlViewForCell(editorRoot.closest("[data-query-cell]"));
    autosizeEditor(editorRoot);
    const workspaceRoot = editorRoot.closest("[data-workspace-notebook]") ?? editorRoot;
    const notebookId = workspaceNotebookId(workspaceRoot);
    const cellId = editorRoot.closest("[data-query-cell]")?.dataset.cellId;
    if (!applyingNotebookState && notebookId && cellId) {
      setCellSql(notebookId, cellId, nextSql);
    }
    querySourceValidationController.handleTextareaInput(textarea);
  }
  return true;
}

function queryResultCollapseKey(cellId, job = null) {
  return String(job?.jobId || cellId || "").trim();
}

function isQueryResultCollapsed(cellId, job = null) {
  const key = queryResultCollapseKey(cellId, job);
  return Boolean(key && collapsedQueryResultKeys.has(key));
}

function queryResultChartsKey(cellId, job = null) {
  return String(job?.jobId || cellId || "").trim();
}

function isQueryResultChartsVisible(cellId, job = null) {
  const key = queryResultChartsKey(cellId, job);
  return Boolean(key && visibleQueryResultChartKeys.has(key));
}

function queryTimingDetailsKey(cellId, job = null) {
  return String(job?.jobId || cellId || "").trim();
}

function isQueryTimingDetailsVisible(cellId, job = null) {
  const key = queryTimingDetailsKey(cellId, job);
  return Boolean(key && visibleQueryTimingDetailKeys.has(key));
}

function editorExtensionsForLanguage(language, schema) {
  const normalizedLanguage = normalizeCellLanguage(language);
  if (normalizedLanguage === "python") {
    const pythonSupport = pythonLanguageSupport();
    return Array.isArray(pythonSupport) ? pythonSupport : [pythonSupport];
  }

  return [
    sql({
      dialect: PostgreSQL,
      schema,
      upperCaseKeywords: true,
    }),
    PostgreSQL.language.data.of({
      autocomplete: s3AliasCompletionSource(schema),
    }),
  ];
}

function destroyEditor(root) {
  if (!(root instanceof Element)) {
    return;
  }

  const editor = editorRegistry.get(root);
  if (editor) {
    editor.destroy();
    editorRegistry.delete(root);
  }
  root.querySelector(".editor-shell")?.remove();
  root.classList.remove("editor-ready");
  delete root.dataset.editorInitializedLanguage;
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
  const editorLanguage = normalizeCellLanguage(root.dataset.editorLanguage || textarea.dataset.editorLanguage);

  try {
    const editor = new EditorView({
      doc: textarea.value,
      extensions: [
        basicSetup,
        ...editorExtensionsForLanguage(editorLanguage, schema),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            textarea.value = update.state.doc.toString();
            invalidatePreparedSqlViewForCell(root.closest("[data-query-cell]"));
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
            querySourceValidationController.handleEditorChanged(root);
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
    root.dataset.editorInitializedLanguage = editorLanguage;
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
    syncEditorExpandButton(editorRoot);
    syncSqlViewToggle(editorRoot);
    createEditor(editorRoot);
    querySourceValidationController.refreshCell(editorRoot.closest("[data-query-cell]"));
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
  applyWorkbenchTitle("query");
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
  applyWorkbenchTitle("query");
  processHtmx(panel);
  initializeEditors(panel);
  const metaRoot = panel.querySelector("[data-notebook-meta]");
  if (metaRoot) {
    applyWorkspaceMetadata(metaRoot, metadata);
  }
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
  recordNotebookActivity(notebookId, "open");
  syncVisibleQueryCells();
  syncVisiblePythonCells();
  syncVisibleResultStorageControls(panel);
  querySourceValidationController.refreshAll(panel);
  refreshVisibleCacheHydrationStatuses(panel);
  if (metadata.pipelineMode === "pipeline") {
    notebookStagePipelineController.initializeCurrentWorkspace().catch((error) => {
      console.error("Failed to initialize notebook pipeline.", error);
    });
  }
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

  return directChildrenContainer(ensureRootUnassignedFolder());
}

function initialMetadataIsEmpty(initialMetadata = {}) {
  return (
    initialMetadata &&
    typeof initialMetadata === "object" &&
    !Array.isArray(initialMetadata) &&
    Object.keys(initialMetadata).length === 0
  );
}

function isReusableBlankLocalNotebook(notebookId) {
  if (!isLocalNotebookId(notebookId)) {
    return false;
  }

  const metadata = notebookMetadata(notebookId);
  const cells = Array.isArray(metadata.cells) ? metadata.cells : [];
  const firstCell = cells[0] || {};
  return (
    metadata.canEdit &&
    !metadata.shared &&
    !metadata.deleted &&
    /^Untitled Notebook \d+$/.test(String(metadata.title || "").trim()) &&
    String(metadata.summary || "").trim() === "Describe this notebook." &&
    !String(metadata.linkedGeneratorId || "").trim() &&
    !normalizeTags(metadata.tags || []).length &&
    cells.length === 1 &&
    normalizeCellLanguage(firstCell.language, "sql") === "sql" &&
    !String(firstCell.sql || "").trim() &&
    !normalizeDataSources(firstCell.dataSources || []).length &&
    (!Array.isArray(metadata.versions) || metadata.versions.length <= 1)
  );
}

function reusableBlankNotebookId(targetContainer) {
  if (!targetContainer) {
    return "";
  }

  return Array.from(targetContainer.querySelectorAll(":scope > .notebook-tree-leaf"))
    .map((link) => ({
      notebookId: String(link.dataset.notebookId || "").trim(),
      createdAt: Date.parse(link.dataset.createdAt || "") || 0,
    }))
    .filter((entry) => entry.notebookId && isReusableBlankLocalNotebook(entry.notebookId))
    .sort((left, right) => right.createdAt - left.createdAt)[0]?.notebookId || "";
}

async function createNotebook(targetContainer, initialMetadata = {}) {
  const notebookId = `${localNotebookPrefix}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
  const targetFolder = targetContainer?.closest("[data-tree-folder]") ?? null;
  const inheritedShared = Boolean(initialMetadata.shared ?? folderIsShared(targetFolder));
  if (initialMetadataIsEmpty(initialMetadata) && !inheritedShared) {
    const existingBlankNotebookId = reusableBlankNotebookId(targetContainer);
    if (existingBlankNotebookId) {
      renderLocalNotebookWorkspace(existingBlankNotebookId, { scrollToTop: true });
      return existingBlankNotebookId;
    }
  }

  const metadata = {
    title: initialMetadata.title ?? defaultLocalNotebookTitle(),
    summary: initialMetadata.summary ?? "Describe this notebook.",
    pipelineMode: normalizeNotebookPipelineMode(initialMetadata.pipelineMode),
    pipelinePaths: normalizePipelinePaths(initialMetadata.pipelinePaths),
    cells: normalizeNotebookCells(initialMetadata.cells ?? [createEmptyCellState()]),
    tags: normalizeTags(initialMetadata.tags ?? []),
    canEdit: true,
    canDelete: true,
    shared: inheritedShared,
    deleted: false,
    versions: [],
  };
  metadata.versions = [createInitialNotebookVersion(notebookId, metadata)];

  persistNotebookDraft(notebookId, metadata);
  if (targetContainer) {
    const link = createNotebookLinkElement(notebookId, metadata);
    targetContainer.appendChild(link);
    updateFolderCounts();
    updateNotebookSectionCount();
    persistNotebookTree();
  }
  applyNotebookMetadata();
  renderLocalNotebookWorkspace(notebookId, { scrollToTop: true });
  if (metadata.shared) {
    const result = await shareNotebook(notebookId);
    return result?.notebook?.notebookId || notebookId;
  }
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

  const cellLanguage = normalizeCellLanguage(cellRoot.dataset.defaultCellLanguage || textarea.dataset.editorLanguage);
  editorRoot.dataset.editorLanguage = cellLanguage;
  textarea.dataset.editorLanguage = cellLanguage;
  cellRoot.dataset.defaultCellLanguage = cellLanguage;

  textarea.dataset.defaultSql = sqlText;
  textarea.defaultValue = sqlText;
  if (textarea.value !== sqlText) {
    textarea.value = sqlText;
  }

  const existingEditor = editorRegistry.get(editorRoot);
  const existingLanguage = normalizeCellLanguage(editorRoot.dataset.editorInitializedLanguage || "sql");
  if (existingEditor && existingLanguage !== cellLanguage) {
    destroyEditor(editorRoot);
  }

  const editor = createEditor(editorRoot);
  if (!editor) {
    autosizeEditor(editorRoot);
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
  if (cellLanguageForCellRoot(cellRoot) !== "sql") {
    return;
  }
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
  const cellLanguage = cellLanguageForCellRoot(cellRoot);
  syncWorkspaceActionButton(cellRoot?.querySelector("[data-format-cell-sql]"), {
    allowed: editable && cellLanguage === "sql",
    enabledTitle: "Format SQL",
    disabledTitle: editable ? "Available in SQL cells only." : "This notebook cannot be edited.",
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

function cellQueryRunsPanelMarkup(notebookId, cellId) {
  return `
    <details
      class="workspace-query-runs workspace-query-runs-cell"
      data-notebook-query-runs
      data-query-runs-notebook-id="${escapeHtml(notebookId)}"
      data-query-runs-cell-id="${escapeHtml(cellId)}"
      data-query-runs-limit="10"
    >
      <summary class="workspace-query-runs-summary">
        <span class="workspace-query-runs-title">
          <span class="workspace-query-runs-chevron" aria-hidden="true"></span>
          <span class="workspace-tags-label">Query Monitoring</span>
        </span>
        <span class="query-runs-status" data-query-runs-status>No recorded query runs yet.</span>
      </summary>
      <div class="workspace-query-runs-header workspace-query-runs-header-cell">
        <p>Recorded runs for this cell.</p>
        <button type="button" class="query-runs-chart-toggle" data-query-runs-toggle-charts aria-pressed="false" title="Show resource charts">
          <span class="query-runs-chart-toggle-switch" aria-hidden="true">
            <span class="query-runs-chart-toggle-thumb"></span>
          </span>
          <span class="query-runs-chart-toggle-copy" data-query-runs-toggle-label>Show resource charts</span>
        </button>
      </div>
      <div class="query-run-history-list query-run-history-list-compact" data-query-runs-list>
        <p class="home-empty">No recorded query runs yet.</p>
      </div>
    </details>
  `;
}

function applyWorkspaceCellState(workspaceRoot, cell, index, editable, totalCells) {
  const cellRoot = workspaceRoot?.querySelector(`[data-query-cell][data-cell-id="${cell.cellId}"]`);
  if (!cellRoot) {
    return;
  }

  const cellLanguage = normalizeCellLanguage(cell.language);
  const notebookId = workspaceNotebookId(workspaceRoot);
  cellRoot.dataset.defaultCellLanguage = cellLanguage;
  cellRoot.dataset.defaultCellSources = normalizeDataSources(cell.dataSources).join("||");

  const label = cellRoot.querySelector(".cell-label");
  if (label) {
    const pipelineMode = normalizeNotebookPipelineMode(
      workspaceRoot?.dataset?.defaultPipelineMode ||
        workspaceRoot?.querySelector?.("[data-notebook-meta]")?.dataset?.defaultPipelineMode ||
        notebookMetadata(notebookId).pipelineMode
    );
    const labelPrefix = pipelineMode === "pipeline" && cellLanguage === "sql" ? "Stage" : "Cell";
    label.textContent = `${labelPrefix} ${index + 1}`;
  }

  const accessBadge = cellRoot.querySelector("[data-cell-access-badge]");
  if (accessBadge) {
    accessBadge.textContent = accessModeForDataSources(cell.dataSources);
    accessBadge.title = accessModeHintForDataSources(cell.dataSources);
  }

  const languageBadge = cellRoot.querySelector("[data-cell-language-badge]");
  if (languageBadge) {
    languageBadge.textContent =
      cellLanguage === "python" ? "Python / Headless Jupyter Kernel" : "SQL / Query Engine";
  }

  cellRoot.querySelectorAll("[data-set-cell-language]").forEach((button) => {
    const buttonLanguage = normalizeCellLanguage(button.dataset.setCellLanguage);
    button.disabled = !editable;
    button.classList.toggle("is-active", buttonLanguage === cellLanguage);
  });

  const explainButton = cellRoot.querySelector("[data-explain-cell]");
  if (explainButton) {
    explainButton.hidden = cellLanguage !== "sql";
    explainButton.disabled = cellLanguage !== "sql";
  }

  const duckdbOptionsRoot = cellRoot.querySelector("[data-cell-duckdb-options]");
  const parquetHiveSelect = cellRoot.querySelector(
    '[data-cell-query-option="duckdb.parquetHivePartitioning"]'
  );
  const cacheHydrationToggle = cellRoot.querySelector(
    '[data-cell-query-option="duckdb.cacheHydration.mode"]'
  );
  const sourceCheckToggle = cellRoot.querySelector(
    '[data-cell-query-option="validation.sourceExistence"]'
  );
  const resultStorageToggle = cellRoot.querySelector(
    '[data-cell-query-option="duckdb.resultStorage.mode"]'
  );
  const resultStoragePathInput = cellRoot.querySelector(
    '[data-cell-query-option="duckdb.resultStorage.path"]'
  );
  const normalizedQueryOptions = normalizeCellQueryOptions(cell.queryOptions);
  if (duckdbOptionsRoot) {
    duckdbOptionsRoot.hidden = cellLanguage !== "sql";
  }
  if (parquetHiveSelect) {
    parquetHiveSelect.disabled = !editable || cellLanguage !== "sql";
    parquetHiveSelect.value = normalizedQueryOptions.duckdb.parquetHivePartitioning;
  }
  if (cacheHydrationToggle) {
    cacheHydrationToggle.disabled = !editable || cellLanguage !== "sql";
    const cacheEnabled = normalizedQueryOptions.duckdb.cacheHydration.mode === "on";
    if (cacheHydrationToggle instanceof HTMLButtonElement) {
      cacheHydrationToggle.setAttribute("aria-checked", cacheEnabled ? "true" : "false");
    } else {
      cacheHydrationToggle.checked = cacheEnabled;
    }
    if (!cacheEnabled) {
      setCellCacheHydrationVisualState(cellRoot, {
        status: "off",
        statusLabel: "Off",
        statusReason: "Hydrate cache is off for this SQL cell.",
      });
    }
  }
  if (sourceCheckToggle) {
    sourceCheckToggle.disabled = !editable || cellLanguage !== "sql";
    const sourceCheckEnabled = normalizedQueryOptions.validation.sourceExistence !== "off";
    if (sourceCheckToggle instanceof HTMLButtonElement) {
      sourceCheckToggle.setAttribute("aria-checked", sourceCheckEnabled ? "true" : "false");
    } else {
      sourceCheckToggle.checked = sourceCheckEnabled;
    }
    const label = sourceCheckToggle.querySelector?.("[data-source-check-state-label]");
    if (label) {
      label.textContent = sourceCheckEnabled ? "On" : "Off";
    }
  }
  if (resultStorageToggle) {
    const pipelineStageStorage = pipelineResultStorageForCellRoot(cellRoot);
    resultStorageToggle.disabled = pipelineStageStorage || !editable || cellLanguage !== "sql";
    const storageEnabled = pipelineStageStorage || normalizedQueryOptions.duckdb.resultStorage.mode === "on";
    if (resultStorageToggle instanceof HTMLInputElement) {
      resultStorageToggle.checked = storageEnabled;
    } else {
      resultStorageToggle.setAttribute("aria-checked", storageEnabled ? "true" : "false");
    }
  }
  if (resultStoragePathInput) {
    const pipelineStageStorage = pipelineResultStorageForCellRoot(cellRoot);
    const stage = normalizeCellStage(cell.stage);
    resultStoragePathInput.value =
      (pipelineStageStorage ? stage.outputPath : normalizedQueryOptions.duckdb.resultStorage.path) ||
      resultStoragePathInput.value ||
      "";
    resultStoragePathInput.disabled = !editable || cellLanguage !== "sql";
    syncCellResultStorageState(cellRoot);
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

  const descriptorValues = {
    processingHints: String(cell.processingHints || ""),
    resultExpectations: String(cell.resultExpectations || ""),
  };
  cellRoot.querySelectorAll("[data-cell-descriptor]").forEach((descriptorInput) => {
    const descriptorName = descriptorInput.dataset.cellDescriptor;
    if (!Object.prototype.hasOwnProperty.call(descriptorValues, descriptorName)) {
      return;
    }
    descriptorInput.disabled = !editable;
    setInputValue(descriptorInput, descriptorValues[descriptorName]);
  });
  cellRoot.querySelectorAll("[data-cell-descriptor-readonly]").forEach((descriptorNode) => {
    const descriptorName = descriptorNode.dataset.cellDescriptorReadonly;
    if (!Object.prototype.hasOwnProperty.call(descriptorValues, descriptorName)) {
      return;
    }
    descriptorNode.textContent =
      descriptorValues[descriptorName] ||
      (descriptorName === "processingHints"
        ? "No processing hints saved."
        : "No result expectations saved.");
  });

  if (!editable) {
    cellRoot.querySelector("[data-cell-source-picker]")?.removeAttribute("open");
  }

  syncCellActionButtons(cellRoot, editable, index, totalCells);
  updateWorkspaceCellEditor(cellRoot, cell.sql);

  const queryRunsRoot = cellRoot.querySelector(":scope > [data-notebook-query-runs]");
  if (cellLanguage === "python") {
    queryRunsRoot?.remove();
  } else if (!queryRunsRoot) {
    const resultRoot = cellRoot.querySelector("[data-cell-result]");
    if (resultRoot) {
      resultRoot.insertAdjacentHTML("beforebegin", cellQueryRunsPanelMarkup(notebookId, cell.cellId));
    } else {
      cellRoot.querySelector("[data-query-form]")?.insertAdjacentHTML(
        "afterend",
        cellQueryRunsPanelMarkup(notebookId, cell.cellId)
      );
    }
  }

  const resultRoot = cellRoot.querySelector("[data-cell-result]");
  const job = cellLanguage === "python" ? pythonJobForCell(notebookId, cell.cellId) : queryJobForCell(notebookId, cell.cellId);
  const resultMarkup =
    cellLanguage === "python"
      ? pythonResultPanelMarkup(cell.cellId, job)
      : queryResultPanelMarkup(cell.cellId, job);
  if (resultRoot) {
    resultRoot.outerHTML = resultMarkup;
  } else {
    cellRoot.querySelector("[data-query-form]")?.insertAdjacentHTML("afterend", resultMarkup);
  }
  if (cellLanguage !== "python" && job && queryJobTerminalStatuses.has(String(job.status || "").trim())) {
    acknowledgeQueryClientTiming(job);
  }
}

function workspaceCellIds(workspaceRoot) {
  return Array.from(workspaceRoot?.querySelectorAll("[data-query-cell]") ?? []).map(
    (cellRoot) => cellRoot.dataset.cellId
  );
}

function applyWorkspaceMetadata(metaRoot, metadata) {
  const workspaceRoot = metaRoot.closest("[data-workspace-notebook]");
  const deleteInProgress = Boolean(metadata.deleteInProgress);
  metaRoot.dataset.shared = metadata.shared ? "true" : "false";
  metaRoot.dataset.deleteInProgress = deleteInProgress ? "true" : "false";
  if (workspaceRoot) {
    workspaceRoot.dataset.shared = metadata.shared ? "true" : "false";
    workspaceRoot.dataset.deleteInProgress = deleteInProgress ? "true" : "false";
  }
  metaRoot.dataset.canEdit = metadata.canEdit ? "true" : "false";
  metaRoot.dataset.canDelete = metadata.canDelete ? "true" : "false";
  metaRoot.dataset.defaultPipelineMode = normalizeNotebookPipelineMode(metadata.pipelineMode);
  metaRoot.dataset.defaultPipelinePaths = JSON.stringify(normalizePipelinePaths(metadata.pipelinePaths));
  metaRoot.dataset.defaultCells = JSON.stringify(
    (metadata.cells ?? []).map((cell) => ({
      cellId: cell.cellId,
      language: normalizeCellLanguage(cell.language),
      processingHints: cell.processingHints || "",
      resultExpectations: cell.resultExpectations || "",
      dataSources: normalizeDataSources(cell.dataSources),
      queryOptions: normalizeCellQueryOptions(cell.queryOptions),
      stage: normalizeCellStage(cell.stage),
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
    accessBadge.textContent = deleteInProgress
      ? "DELETION IN PROGRESS"
      : notebookAccessMode(metadata);
    accessBadge.title = deleteInProgress
      ? "Notebook deletion is in progress."
      : notebookAccessModeHint(metadata);
    accessBadge.dataset.tone = deleteInProgress ? "deleting" : "";
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
    const sharedToggleTitle = deleteInProgress
      ? "Notebook deletion is in progress."
      : metadata.canEdit
        ? notebookVisibilityTitle(metadata.shared === true)
        : "Immutable preset notebooks are public.";
    sharedToggle.title = sharedToggleTitle;
    sharedToggle.disabled = !metadata.canEdit;
    const sharedToggleCopy = sharedToggle.querySelector(".workspace-sharing-toggle-copy");
    if (sharedToggleCopy) {
      const toggleLabel = deleteInProgress
        ? "DELETION IN PROGRESS"
        : notebookVisibilityLabel(metadata.shared);
      const toggleDetail = deleteInProgress
        ? "The notebook will disappear when the server confirms deletion."
        : metadata.shared
          ? "Stores this notebook on the server and announces it to connected users."
          : "Keeps this notebook local to this browser workspace.";
      const detail = document.createElement("small");
      detail.textContent = toggleDetail;
      sharedToggleCopy.replaceChildren(document.createTextNode(toggleLabel), detail);
    }
  }

  const disabledByDeletionTitle = "Notebook deletion is in progress.";

  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-rename-notebook]"), {
    allowed: metadata.canEdit,
    enabledTitle: "Rename notebook",
    disabledTitle: deleteInProgress ? disabledByDeletionTitle : "This notebook cannot be renamed.",
  });
  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-edit-notebook]"), {
    allowed: metadata.canEdit,
    enabledTitle: "Edit notebook metadata",
    disabledTitle: deleteInProgress ? disabledByDeletionTitle : "This notebook cannot be edited.",
  });
  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-delete-notebook]"), {
    allowed: metadata.canDelete,
    enabledTitle: "Delete notebook",
    disabledTitle: deleteInProgress ? disabledByDeletionTitle : "This notebook cannot be deleted.",
  });
  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-copy-notebook]"), {
    allowed: !deleteInProgress,
    enabledTitle: "Create a copy of this notebook",
    disabledTitle: deleteInProgress ? disabledByDeletionTitle : "Create a copy of this notebook",
  });
  syncWorkspaceActionButton(workspaceRoot?.querySelector("[data-share-notebook]"), {
    allowed: !deleteInProgress,
    enabledTitle: "Copy or email a notebook reference",
    disabledTitle: deleteInProgress ? disabledByDeletionTitle : "Copy or email a notebook reference",
  });
  syncWorkspaceActionButton(metaRoot.querySelector("[data-save-version]"), {
    allowed: metadata.canEdit,
    enabledTitle: "Save the current notebook state as a version",
    disabledTitle: deleteInProgress ? disabledByDeletionTitle : "This notebook cannot be versioned.",
  });

  renderWorkspaceTags(metaRoot, metadata.tags, metadata.canEdit);
  renderWorkspaceVersions(metaRoot, metadata.versions);

  const renderedCellIds = workspaceCellIds(workspaceRoot);
  const expectedCellIds = (metadata.cells ?? []).map((cell) => cell.cellId);
  const cellsMismatch =
    renderedCellIds.length !== expectedCellIds.length ||
    renderedCellIds.some((cellId, index) => cellId !== expectedCellIds[index]);

  if (cellsMismatch) {
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
  syncVisibleQueryCells();
  syncVisiblePythonCells();
  notebookStagePipelineController.initializeCurrentWorkspace().catch((error) => {
    console.error("Failed to sync notebook pipeline.", error);
  });
  applyWorkbenchTitle();
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
        processingHints: cell.processingHints || "",
        resultExpectations: cell.resultExpectations || "",
        queryOptions: normalizeCellQueryOptions(cell.queryOptions),
        stage: normalizeCellStage(cell.stage),
        sql: cell.sql,
      })
    ),
    pipelineMode: normalizeNotebookPipelineMode(sourceMetadata.pipelineMode),
    pipelinePaths: normalizePipelinePaths(sourceMetadata.pipelinePaths),
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
    setNotebookDeletionInProgress(notebookId, true);
    try {
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
    } catch (error) {
      setNotebookDeletionInProgress(notebookId, false);
      throw error;
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
  clearSharedNotebookPendingWork(notebookId);
  notebookDeletionInProgressIds.delete(notebookId);

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

function queryJobsNeedReconciliation() {
  return queryJobsSnapshot.some((job) => queryJobIsRunning(job));
}

function clearQueryJobsReconciliation() {
  if (queryJobsReconcileHandle !== null) {
    window.clearTimeout(queryJobsReconcileHandle);
    queryJobsReconcileHandle = null;
  }
}

function scheduleQueryJobsReconciliation({ delayMs = queryJobsReconcileInitialDelayMs } = {}) {
  if (!queryJobsNeedReconciliation()) {
    clearQueryJobsReconciliation();
    return;
  }
  if (queryJobsReconcileInFlight) {
    return;
  }
  if (queryJobsReconcileHandle !== null) {
    return;
  }

  queryJobsReconcileHandle = window.setTimeout(refreshQueryJobsForReconciliation, delayMs);
}

async function refreshQueryJobsForReconciliation() {
  queryJobsReconcileHandle = null;
  if (!queryJobsNeedReconciliation()) {
    return;
  }
  if (queryJobsReconcileInFlight) {
    scheduleQueryJobsReconciliation({ delayMs: queryJobsReconcilePollMs });
    return;
  }

  queryJobsReconcileInFlight = true;
  try {
    await loadQueryJobsState();
  } catch (error) {
    console.warn("Failed to reconcile live query jobs.", error);
  } finally {
    queryJobsReconcileInFlight = false;
    if (queryJobsNeedReconciliation()) {
      scheduleQueryJobsReconciliation({ delayMs: queryJobsReconcilePollMs });
    }
  }
}

function syncQueryJobsReconciliation() {
  if (queryJobsNeedReconciliation()) {
    scheduleQueryJobsReconciliation();
  } else {
    clearQueryJobsReconciliation();
  }
}

function setCellLanguage(notebookId, cellId, language) {
  const normalizedLanguage = normalizeCellLanguage(language);
  updateStoredNotebookState(notebookId, (currentState) => {
    const baseCells = normalizeNotebookCells(currentState.cells ?? notebookMetadata(notebookId).cells);
    return {
      ...currentState,
      cells: baseCells.map((cell) =>
        cell.cellId === cellId
          ? {
              ...cell,
              language: normalizedLanguage,
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
  const cellRoot = document.querySelector(
    `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"] [data-query-cell][data-cell-id="${CSS.escape(cellId)}"]`
  );
  querySourceValidationController.refreshCell(cellRoot);
}

function refreshLivePythonClock() {
  const jobsById = new Map(pythonJobsSnapshot.map((job) => [job.jobId, job]));
  document.querySelectorAll("[data-python-duration]").forEach((node) => {
    const job = jobsById.get(node.dataset.jobId || "");
    if (!job) {
      return;
    }
    node.textContent = formatQueryDuration(pythonJobElapsedMs(job));
  });
}

const pythonJobsClock = createVisibilityAwareClock(refreshLivePythonClock);

function syncPythonClockLoop() {
  const hasRunningJobs = pythonJobsSnapshot.some((job) => pythonJobIsRunning(job));
  pythonJobsClock.setEnabled(hasRunningJobs);
  pythonJobsClock.refresh();
}

function syncPythonCellJobState(cellRoot) {
  if (!(cellRoot instanceof Element) || cellLanguageForCellRoot(cellRoot) !== "python") {
    return;
  }

  const workspaceRoot = cellRoot.closest("[data-workspace-notebook]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellId = cellRoot.dataset.cellId;
  const job = pythonJobForCell(notebookId, cellId);
  const runButton = cellRoot.querySelector("[data-run-cell]");
  const cancelButton = cellRoot.querySelector("[data-cancel-query]");
  const resultRoot = cellRoot.querySelector("[data-cell-result]");

  cellRoot.classList.toggle("is-query-running", pythonJobIsRunning(job));

  if (runButton) {
    if (pythonJobIsRunning(job)) {
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
    cancelButton.hidden = !pythonJobIsRunning(job);
    cancelButton.dataset.jobId = job?.jobId || "";
    cancelButton.dataset.jobKind = "python";
    cancelButton.disabled = !pythonJobIsRunning(job);
  }

  if (resultRoot) {
    resultRoot.outerHTML = pythonResultPanelMarkup(cellId, job);
  }
}

function syncVisiblePythonCells() {
  document.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
    syncPythonCellJobState(cellRoot);
  });
}

function applyPythonJobsState(snapshot) {
  pythonJobsStateVersion = snapshot?.version ?? null;
  pythonJobsSummary = snapshot?.summary ?? { runningCount: 0, totalCount: 0 };
  pythonJobsSnapshot = Array.isArray(snapshot?.jobs)
    ? snapshot.jobs.map((job) => normalizePythonJob(job)).filter(Boolean).sort(comparePythonJobsByStartedAt)
    : [];

  syncVisiblePythonCells();
  syncPythonClockLoop();
}

async function loadPythonJobsState() {
  await requestPythonJobsState({ applyPythonJobsState });
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
      queryRunsController.refreshForQueryJobsSnapshot(snapshot);
      break;
    case "python-jobs":
      applyPythonJobsState(snapshot);
      break;
    case "data-generation-jobs":
      applyDataGenerationJobsState(snapshot);
      break;
    case "download-jobs":
      applyDownloadJobsState(snapshot);
      break;
    case "s3-delete-jobs":
      applyS3DeleteJobsState(snapshot);
      break;
    case "data-source-events":
      applyDataSourceEventsState(snapshot);
      break;
    case "service-consumption":
      serviceConsumptionStateVersion = Number(snapshot?.version || 0);
      serviceConsumptionUi.applyRealtimeSnapshot(snapshot);
      break;
    case "materialized-stages":
      notebookStagePipelineController.applyRealtimeState(snapshot);
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
  if (eventPayload.eventType === "deleted") {
    clearSharedNotebookPendingWork(notebookId);
    notebookDeletionInProgressIds.delete(notebookId);
  }
  const mode = currentWorkspaceMode();
  const activeNotebookId = currentWorkspaceNotebookId();
  const navigationSnapshot = workspaceNavigation.snapshot({
    notebookId: activeNotebookId,
    reason: "notebook-sse",
  });
  const stillOnCapturedWorkspace = () =>
    workspaceNavigationIsCurrent(navigationSnapshot) &&
    currentWorkspaceMode() === mode &&
    currentWorkspaceNotebookId() === activeNotebookId;

  await refreshSidebar(mode, { isCurrent: stillOnCapturedWorkspace });
  if (!stillOnCapturedWorkspace()) {
    return;
  }

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
  const navigationToken = workspaceNavigation.begin({
    path: notebookId && !isLocalNotebookId(notebookId)
      ? `/notebooks/${encodeURIComponent(notebookId)}`
      : "/query-workbench",
    notebookId,
  });
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook", {
      signal: navigationToken.signal,
      isCurrent: () => workspaceNavigationIsCurrent(navigationToken),
    });
    if (!workspaceNavigationIsCurrent(navigationToken)) {
      return;
    }
  }

  if (notebookId) {
    openNotebookNavigation(notebookId);
    await loadNotebookWorkspace(notebookId, { navigationToken });
    if (!workspaceNavigationIsCurrent(navigationToken)) {
      return;
    }
    if (isLocalNotebookId(notebookId)) {
      pushQueryWorkbenchHistory();
    } else {
      pushNotebookHistory(notebookId);
    }
    return;
  }

  await loadQueryWorkbenchEntry({ navigationToken });
}

async function openQueryWorkbenchNavigation() {
  const preferredNotebookId = [
    currentActiveNotebookId(),
    readLastNotebookId(),
    visibleNotebookLinks()[0]?.dataset.notebookId ?? "",
  ].find((candidate) => candidate && !notebookMetadata(candidate).deleted);
  const navigationToken = workspaceNavigation.begin({
    path: preferredNotebookId && !isLocalNotebookId(preferredNotebookId)
      ? `/notebooks/${encodeURIComponent(preferredNotebookId)}`
      : "/query-workbench",
    notebookId: preferredNotebookId || "",
  });
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook", {
      signal: navigationToken.signal,
      isCurrent: () => workspaceNavigationIsCurrent(navigationToken),
    });
    if (!workspaceNavigationIsCurrent(navigationToken)) {
      return;
    }
  }

  if (preferredNotebookId) {
    openNotebookNavigation(preferredNotebookId);
    if (
      currentWorkspaceMode() === "notebook" &&
      currentActiveNotebookId() === preferredNotebookId &&
      !homePageRoot() &&
      !queryWorkbenchEntryPageRoot() &&
      !queryWorkbenchDataSourcesPageRoot() &&
      !dataSourceExplorerPageRoot()
    ) {
      if (isLocalNotebookId(preferredNotebookId)) {
        pushQueryWorkbenchHistory();
      } else {
        pushNotebookHistory(preferredNotebookId);
      }
      applyWorkbenchTitle("query");
      return;
    }

    await loadNotebookWorkspace(preferredNotebookId, { navigationToken });
    if (!workspaceNavigationIsCurrent(navigationToken)) {
      return;
    }
    if (isLocalNotebookId(preferredNotebookId)) {
      pushQueryWorkbenchHistory();
    } else {
      pushNotebookHistory(preferredNotebookId);
    }
    return;
  }

  await loadQueryWorkbenchEntry({ navigationToken });
}

function workspaceNavigationIsCurrent(token) {
  return workspaceNavigation.isCurrent(token);
}

function workspacePanelNavigationIsCurrent(panel) {
  if (!(panel instanceof Element) || !panel.isConnected) {
    return false;
  }
  return Number(panel.dataset.workspaceNavigationEpoch) === workspaceNavigation.currentEpoch();
}

async function loadWorkspacePanelPartial(path, { navigationToken = null } = {}) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return null;
  }

  const token = navigationToken ?? workspaceNavigation.begin({ path });
  const response = await window.fetch(path, {
    headers: {
      Accept: "text/html",
      "HX-Request": "true",
    },
    signal: token.signal,
  });
  if (!workspaceNavigationIsCurrent(token)) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Failed to load ${path}: ${response.status}`);
  }

  const markup = await response.text();
  if (!workspaceNavigationIsCurrent(token)) {
    return null;
  }
  panel.innerHTML = markup;
  panel.dataset.workspaceNavigationEpoch = String(token.epoch);
  processHtmx(panel);
  initializeEditors(panel);
  applyNotebookMetadata();
  syncVisiblePythonCells();
  querySourceValidationController.refreshAll(panel);
  queryRunsController.initializeCurrentPage(panel).catch((error) => {
    console.error("Failed to load query-run history.", error);
  });
  renderQueryNotificationMenu();
  return panel;
}

async function loadQueryWorkbenchEntry({ pushHistory = true, navigationToken = null } = {}) {
  const panel = await loadWorkspacePanelPartial("/query-workbench", { navigationToken });
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("query");
  renderHomePage();
  queryWorkbenchEntryController.initializeCurrentPage(panel).catch((error) => {
    console.error("Failed to initialize the Query Workbench entry page.", error);
  });
  if (pushHistory) {
    pushQueryWorkbenchHistory();
  }
}

async function loadQueryRunsPage({ pushHistory = true } = {}) {
  const panel = await loadWorkspacePanelPartial("/query-workbench/query-runs");
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("query-runs");
  if (pushHistory) {
    pushQueryRunsHistory();
  }
}

function sidebarSourceIdForDataSource(sourceId = "", explicitSidebarSourceId = "") {
  const normalizedExplicit = String(explicitSidebarSourceId || "").trim();
  if (normalizedExplicit) {
    return normalizedExplicit;
  }
  const normalizedSourceId = String(sourceId || "").trim();
  if (normalizedSourceId === "pg_oltp_native") {
    return "pg_oltp";
  }
  return normalizedSourceId;
}

async function revealDataSourceSidebarBrowser(sourceId = "", { sidebarSourceId = "" } = {}) {
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }

  const normalizedSidebarSourceId = sidebarSourceIdForDataSource(sourceId, sidebarSourceId);
  setShellSidebarHidden(false);
  applySidebarCollapsedState(false);
  writeSidebarCollapsed(false);

  const sourcesRoot = dataSourcesSection();
  if (sourcesRoot instanceof HTMLDetailsElement) {
    sourcesRoot.open = true;
  }

  const catalogRoot =
    sourceCatalogNode(normalizedSidebarSourceId) ||
    sourceCatalogNode(String(sourceId || "").trim());
  if (catalogRoot instanceof HTMLDetailsElement) {
    catalogRoot.open = true;
    catalogRoot.scrollIntoView({ block: "nearest" });
    blinkSourceCatalog(
      catalogRoot.dataset.sourceCatalogSourceId?.trim() ||
        catalogRoot.dataset.sourceCatalogName?.trim() ||
        normalizedSidebarSourceId
    );
  }
}

async function initializeDataSourceManagementPage() {
  const root = queryWorkbenchDataSourcesPageRoot();
  if (!(root instanceof Element)) {
    return;
  }

  await renderLocalWorkspaceSidebarEntries();

  const browseSourceId = String(root.dataset.browseSourceId || "").trim();
  if (!browseSourceId) {
    return;
  }

  const inlineBrowser = root.querySelector("[data-inline-source-browser]");
  if (!(inlineBrowser instanceof Element)) {
    return;
  }

  const normalizedSidebarSourceId = sidebarSourceIdForDataSource(
    browseSourceId,
    root.dataset.browseSidebarSourceId || ""
  );
  const catalogRoot =
    inlineBrowser.querySelector(
      `[data-source-catalog-source-id="${CSS.escape(normalizedSidebarSourceId)}"]`
    ) ||
    inlineBrowser.querySelector(
      `[data-source-catalog-source-id="${CSS.escape(browseSourceId)}"]`
    );
  if (catalogRoot instanceof HTMLDetailsElement) {
    catalogRoot.open = true;
  }
  inlineBrowser.scrollIntoView({ block: "start" });
}

async function loadQueryWorkbenchDataSources(
  sourceId = "",
  { pushHistory = true, browse = false, sidebarSourceId = "" } = {}
) {
  const panel = await loadWorkspacePanelPartial(queryWorkbenchDataSourcesUrl(sourceId, { browse }));
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("data-sources");
  await initializeDataSourceManagementPage();
  if (!workspacePanelNavigationIsCurrent(panel)) {
    return;
  }
  if (pushHistory) {
    pushQueryWorkbenchDataSourcesHistory(sourceId, { browse });
  }
}

async function browseDataSourceInSidebar(sourceId = "", { sidebarSourceId = "" } = {}) {
  await loadQueryWorkbenchDataSources(sourceId, {
    browse: true,
    sidebarSourceId,
  });
}

async function refreshActiveDataSourceWorkbenchBrowser() {
  const root = queryWorkbenchDataSourcesPageRoot();
  const browseSourceId = String(root?.dataset.browseSourceId || "").trim();
  if (!browseSourceId) {
    return;
  }
  await loadQueryWorkbenchDataSources(browseSourceId, {
    pushHistory: false,
    browse: true,
    sidebarSourceId: String(root?.dataset.browseSidebarSourceId || "").trim(),
  });
}

async function loadQueryWorkbenchDataSourceExplorer(sourceId = "", { pushHistory = true } = {}) {
  const panel = await loadWorkspacePanelPartial(queryWorkbenchDataSourceExplorerUrl(sourceId));
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("data-sources");
  await dataSourceExplorerController.initializeCurrentPage();
  if (!workspacePanelNavigationIsCurrent(panel)) {
    return;
  }
  if (pushHistory) {
    pushQueryWorkbenchDataSourceExplorerHistory(sourceId);
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
  if (!workspacePanelNavigationIsCurrent(panel)) {
    return;
  }
  if (pushHistory) {
    pushServiceConsumptionHistory();
  }
}

async function loadDataProductsPage({ pushHistory = true } = {}) {
  const panel = await loadWorkspacePanelPartial("/data-products");
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("data-products");
  dataProductsController.initializeCurrentPage();
  if (pushHistory) {
    pushDataProductsHistory();
  }
}

async function loadDataExchangePage({ pushHistory = true } = {}) {
  const panel = await loadWorkspacePanelPartial("/data-exchange");
  if (!panel) {
    return;
  }

  syncShellVisibility();
  activateNotebookLink("");
  applyWorkbenchTitle("data-exchange");
  dataExchangeController.initializeCurrentPage();
  if (pushHistory) {
    pushDataExchangeHistory();
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

async function openQueryRunsPage() {
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }

  await loadQueryRunsPage();
}

async function openDataProductsWorkbench() {
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }

  await loadDataProductsPage();
}

async function openDataExchangeWorkbench() {
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook");
  }

  await loadDataExchangePage();
}

async function openServiceConsumptionPage() {
  await loadServiceConsumptionPage();
}

async function openDataProductPublishDialog({
  sourceObjectRoot = null,
  sourceSchemaRoot = null,
} = {}) {
  const source =
    dataProductSourceDescriptorFromSourceObject(sourceObjectRoot) ||
    dataProductSourceDescriptorFromSourceSchema(sourceSchemaRoot);

  if (!source) {
    await showMessageDialog({
      title: "Data product source unavailable",
      copy: "This source cannot be used to start managed publication.",
    });
    return;
  }

  await dataProductsController.openPublishDialog({
    source,
    lockSource: true,
    startStep: 2,
  });
}

function ensureRealtimeEventsEventSource() {
  if (realtimeEventsEventSource) {
    return;
  }
  if (typeof window.EventSource !== "function") {
    realtimeConnectionStatusController.setDisconnected();
    return;
  }

  realtimeConnectionStatusController.setConnecting();

  const params = new URLSearchParams();
  const dataSourceEventsStateVersion = getDataSourceEventsStateVersion();
  if (queryJobsStateVersion !== null) {
    params.set("queryJobsVersion", String(queryJobsStateVersion));
  }
  if (pythonJobsStateVersion !== null) {
    params.set("pythonJobsVersion", String(pythonJobsStateVersion));
  }
  if (dataGenerationJobsStateVersion !== null) {
    params.set("dataGenerationJobsVersion", String(dataGenerationJobsStateVersion));
  }
  if (downloadJobsStateVersion !== null) {
    params.set("downloadJobsVersion", String(downloadJobsStateVersion));
  }
  if (s3DeleteJobsStateVersion !== null) {
    params.set("s3DeleteJobsVersion", String(s3DeleteJobsStateVersion));
  }
  if (dataSourceEventsStateVersion !== null) {
    params.set("dataSourceEventsVersion", String(dataSourceEventsStateVersion));
  }
  if (serviceConsumptionStateVersion !== null) {
    params.set("serviceConsumptionVersion", String(serviceConsumptionStateVersion));
  }
  const materializedStagesVersion = notebookStagePipelineController.getMaterializedStagesVersion();
  if (materializedStagesVersion !== null) {
    params.set("materializedStagesVersion", String(materializedStagesVersion));
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
  eventSource.onopen = () => {
    realtimeConnectionStatusController.setConnected();
  };
  [
    "query-jobs",
    "python-jobs",
    "data-generation-jobs",
    "download-jobs",
    "s3-delete-jobs",
    "data-source-events",
    "service-consumption",
    "materialized-stages",
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
    realtimeConnectionStatusController.setDisconnected();
    const refreshTasks = [];
    const dataSourceEventsStateVersion = getDataSourceEventsStateVersion();
    if (queryJobsStateVersion !== null) {
      refreshTasks.push(
        loadQueryJobsState().catch(() => {
          // Ignore transient reconnect issues.
        })
      );
    }
    if (pythonJobsStateVersion !== null) {
      refreshTasks.push(
        loadPythonJobsState().catch(() => {
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
    if (downloadJobsStateVersion !== null) {
      refreshTasks.push(
        loadDownloadJobsState().catch(() => {
          // Ignore transient reconnect issues.
        })
      );
    }
    if (s3DeleteJobsStateVersion !== null) {
      refreshTasks.push(
        loadS3DeleteJobsState().catch(() => {
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
    if (notebookStagePipelineController.getMaterializedStagesVersion() !== null) {
      refreshTasks.push(
        notebookStagePipelineController.loadState().catch(() => {
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

async function openIngestionWorkbench({ pushHistory = true, navigationToken = null } = {}) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  const previousState = captureIngestionWorkbenchNavigationState(panel.querySelector("[data-ingestion-workbench-page]"));
  const token = navigationToken ?? workspaceNavigation.begin({ path: "/ingestion-workbench" });
  const response = await window.fetch("/ingestion-workbench", {
    headers: { "HX-Request": "true" },
    signal: token.signal,
  });
  if (!workspaceNavigationIsCurrent(token)) {
    return;
  }
  if (!response.ok) {
    throw new Error(`Failed to load the ingestion workbench: ${response.status}`);
  }

  const markup = await response.text();
  if (!workspaceNavigationIsCurrent(token)) {
    return;
  }
  panel.innerHTML = markup;
  panel.dataset.workspaceNavigationEpoch = String(token.epoch);
  processHtmx(panel);
  setShellSidebarHidden(true);
  applyWorkbenchTitle("ingestion");
  if (pushHistory && window.location.pathname !== "/ingestion-workbench") {
    window.history.pushState({}, "", "/ingestion-workbench");
  }
  if (!restoreIngestionWorkbenchNavigationState(previousState, panel.querySelector("[data-ingestion-workbench-page]"))) {
    showIngestionLanding();
  }
  renderQueryNotificationMenu();
}

async function openLoaderWorkbench({
  focusJobId = "",
  focusGeneratorId = "",
  pushHistory = true,
  navigationToken = null,
} = {}) {
  const panel = document.getElementById("workspace-panel");
  if (!panel) {
    return;
  }

  const token = navigationToken ?? workspaceNavigation.begin({ path: "/loader-workbench" });
  openLoaderNavigation();

  const response = await window.fetch("/loader-workbench", {
    headers: { "HX-Request": "true" },
    signal: token.signal,
  });
  if (!workspaceNavigationIsCurrent(token)) {
    return;
  }
  if (!response.ok) {
    throw new Error(`Failed to load the Loader Workbench: ${response.status}`);
  }

  const markup = await response.text();
  if (!workspaceNavigationIsCurrent(token)) {
    return;
  }
  panel.innerHTML = markup;
  panel.dataset.workspaceNavigationEpoch = String(token.epoch);
  processHtmx(panel);
  applyWorkbenchTitle("loader");
  if (pushHistory && window.location.pathname !== "/loader-workbench") {
    window.history.pushState({}, "", "/loader-workbench");
  }
  await Promise.allSettled([loadDataGeneratorCatalog(), loadDataGenerationJobsState()]);
  if (!workspaceNavigationIsCurrent(token)) {
    return;
  }
  const focusedJob = focusJobId
    ? dataGenerationJobsSnapshot.find((job) => job.jobId === focusJobId) ?? null
    : null;
  const selectedGeneratorId = selectIngestionRunbook(
    focusGeneratorId || focusedJob?.generatorId || selectedIngestionRunbookId,
    { spotlight: Boolean(focusGeneratorId) }
  );
  renderIngestionWorkbench();
  if (currentSidebarMode() !== "loader") {
    await refreshSidebar("loader", {
      signal: token.signal,
      isCurrent: () => workspaceNavigationIsCurrent(token),
    });
    if (!workspaceNavigationIsCurrent(token)) {
      return;
    }
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

function renderLocalQueryFailure(cellRoot, { cellId, notebookId, workspaceRoot, sql, error }) {
  const resultRoot = cellRoot?.querySelector?.("[data-cell-result]");
  if (!resultRoot) {
    return;
  }

  resultRoot.outerHTML = queryResultPanelMarkup(cellId, {
    jobId: `local-error-${cellId}`,
    notebookId,
    notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
    cellId,
    sql,
    status: "failed",
    durationMs: 0,
    updatedAt: new Date().toISOString(),
    rowsShown: 0,
    truncated: false,
    message: "Query failed.",
    error,
    columns: [],
    rows: [],
  });
}

function renderLocalQueryProgress(cellRoot, { cellId, notebookId, workspaceRoot, snapshot }) {
  const resultRoot = cellRoot?.querySelector?.("[data-cell-result]");
  if (!resultRoot || !snapshot) {
    return;
  }
  resultRoot.outerHTML = queryResultPanelMarkup(cellId, {
    ...snapshot,
    notebookId,
    notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
    cellId,
    columns: [],
    rows: [],
    rowsShown: 0,
    truncated: false,
  });
}

function localQueryJobId() {
  return `query-client-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function queryMonitorSummaryForJobs(jobs) {
  const runningJobs = jobs.filter((job) => queryJobIsRunning(job));
  return {
    runningCount: runningJobs.length,
    runningProcessCount: runningJobs.filter((job) => Number(job.processId || 0) > 0).length,
    totalCount: jobs.length,
  };
}

function applyLocalQueryJobSnapshot(snapshot, { removeJobIds = [] } = {}) {
  const normalizedSnapshot = normalizeQueryJobForDisplay(snapshot);
  if (!normalizedSnapshot?.jobId) {
    return null;
  }

  const removeIds = new Set(removeJobIds.map((jobId) => String(jobId || "").trim()).filter(Boolean));
  const currentState = currentQueryState();
  const nextJobs = [
    normalizedSnapshot,
    ...(currentState.snapshot ?? []).filter(
      (job) => job.jobId !== normalizedSnapshot.jobId && !removeIds.has(job.jobId)
    ),
  ];

  const nextSnapshot = {
    version: currentState.version,
    summary: queryMonitorSummaryForJobs(nextJobs),
    jobs: nextJobs,
    performance: currentState.performance ?? { recent: [], stats: {} },
  };
  applyQueryJobsState(nextSnapshot);
  queryRunsController.refreshForQueryJobsSnapshot(nextSnapshot);
  return normalizedSnapshot;
}

function trackLocalQueryJobSnapshot(snapshot, { removeJobIds = [] } = {}) {
  const normalizedSnapshot = normalizeQueryJobForDisplay(snapshot);
  if (!normalizedSnapshot?.jobId) {
    return null;
  }

  const removeIds = new Set(removeJobIds.map((jobId) => String(jobId || "").trim()).filter(Boolean));
  const currentState = currentQueryState();
  const nextJobs = [
    normalizedSnapshot,
    ...(currentState.snapshot ?? []).filter(
      (job) => job.jobId !== normalizedSnapshot.jobId && !removeIds.has(job.jobId)
    ),
  ];

  queryJobsStateVersion = currentState.version;
  queryJobsSnapshot = nextJobs;
  queryJobsSummary = queryMonitorSummaryForJobs(nextJobs);
  queryPerformanceState = currentState.performance ?? { recent: [], stats: {} };
  syncQueryJobsReconciliation();
  return normalizedSnapshot;
}

function appendLocalQueryProgressEvent(snapshot, event, { phase = "", message = "", progress = null } = {}) {
  const now = new Date();
  const startedAtMs = Date.parse(snapshot.startedAt || "");
  const durationMs = Number.isNaN(startedAtMs) ? 0 : Math.max(0, now.getTime() - startedAtMs);
  return {
    ...snapshot,
    updatedAt: now.toISOString(),
    progress: typeof progress === "number" ? Math.max(0, Math.min(1, progress)) : snapshot.progress,
    progressLabel: phase || snapshot.progressLabel,
    message: message || snapshot.message,
    progressEvents: [
      ...(Array.isArray(snapshot.progressEvents) ? snapshot.progressEvents : []),
      {
        event,
        phase,
        message,
        occurredAt: now.toISOString(),
        durationMs,
      },
    ],
  };
}

function createLocalQueryJobSnapshot({ jobId, notebookId, notebookTitle, cellId, sql, startedAt }) {
  const initialSnapshot = {
    jobId,
    notebookId,
    notebookTitle,
    cellId,
    sql,
    executionSql: "",
    status: "queued",
    startedAt,
    updatedAt: startedAt,
    completedAt: null,
    durationMs: 0,
    progress: 0.02,
    progressLabel: "Preparing submission...",
    message: "The browser accepted Run Cell and is preparing the query request.",
    error: "",
    warnings: [],
    columns: [],
    rows: [],
    rowCount: 0,
    rowsShown: 0,
    truncated: false,
    dataSources: [],
    sourceTypes: [],
    touchedRelations: [],
    touchedBuckets: [],
    backendName: "Browser",
    executionMode: "",
    duckdbExecutionPath: "",
    resourceSamples: [],
    progressEvents: [],
    canCancel: false,
  };
  return appendLocalQueryProgressEvent(initialSnapshot, "client_submitted", {
    phase: "Preparing submission",
    message: "Run Cell was clicked. Preparing validation, source rewrites, and the backend request.",
    progress: 0.02,
  });
}

function failLocalQueryJobSnapshot(snapshot, error, { message = "Query failed before the backend accepted it." } = {}) {
  const now = new Date();
  const startedAtMs = Date.parse(snapshot.startedAt || "");
  const durationMs = Number.isNaN(startedAtMs) ? 0 : Math.max(0, now.getTime() - startedAtMs);
  return {
    ...appendLocalQueryProgressEvent(snapshot, "failed", {
      phase: "Failed",
      message,
      progress: null,
    }),
    status: "failed",
    completedAt: now.toISOString(),
    updatedAt: now.toISOString(),
    durationMs,
    progress: null,
    progressLabel: "Failed",
    message,
    error,
    canCancel: false,
  };
}

async function startQueryJobForForm(form) {
  if (formCellLanguage(form) === "python") {
    await startPythonJobForForm(form);
    return;
  }

  const clientRunStartedPerf = performance.now();
  const clientRunStartedAt = Date.now();
  const workspaceRoot = form.closest("[data-workspace-notebook]");
  const cellRoot = form.closest("[data-query-cell]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellId = cellRoot?.dataset.cellId;
  if (!workspaceRoot || !cellRoot || !notebookId || !cellId) {
    return;
  }

  const existingJob = queryJobForCell(notebookId, cellId);
  const existingPythonJob = pythonJobForCell(notebookId, cellId);
  if (queryJobIsRunning(existingJob) || pythonJobIsRunning(existingPythonJob)) {
    return;
  }

  syncCellResultStorageState(cellRoot);
  syncVisibleDuckdbSqlToVirtual(cellRoot);
  const formData = new FormData(form);
  const editorSource = cellRoot.querySelector("[data-editor-source]");
  const originalSql = editorSource?.value ?? "";
  const clientJobId = localQueryJobId();
  let clientSnapshot = createLocalQueryJobSnapshot({
    jobId: clientJobId,
    notebookId,
    notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
    cellId,
    sql: originalSql,
    startedAt: new Date(clientRunStartedAt).toISOString(),
  });
  queryClientTimingStarts.set(clientJobId, {
    startedPerf: clientRunStartedPerf,
  });
  renderLocalQueryProgress(cellRoot, { cellId, notebookId, workspaceRoot, snapshot: clientSnapshot });

  let preparedSubmission = null;
  try {
    clientSnapshot = appendLocalQueryProgressEvent(clientSnapshot, "client_preparing_sql", {
      phase: "Preparing SQL",
      message: "Rewriting virtual source paths and Local Workspace references for DuckDB.",
      progress: 0.08,
    });
    renderLocalQueryProgress(cellRoot, { cellId, notebookId, workspaceRoot, snapshot: clientSnapshot });
    preparedSubmission = await prepareSqlSubmissionForCell(cellRoot, originalSql);
  } catch (error) {
    const errorMessage =
      error instanceof Error
        ? error.message
        : "The Local Workspace sources could not be prepared for querying.";
    clientSnapshot = failLocalQueryJobSnapshot(clientSnapshot, errorMessage, {
      message: "SQL preparation failed before backend submission.",
    });
    applyLocalQueryJobSnapshot(clientSnapshot);
    renderLocalQueryFailure(cellRoot, {
      cellId,
      notebookId,
      workspaceRoot,
      sql: originalSql,
      error: errorMessage,
    });
    queryClientTimingStarts.delete(clientJobId);
    return;
  }
  formData.set("sql", preparedSubmission.executionSql);
  formData.set("displaySql", originalSql);
  formData.set("notebook_id", notebookId);
  formData.set("cell_id", cellId);
  formData.set("notebook_title", currentWorkspaceNotebookTitle(workspaceRoot));
  formData.set("data_sources", preparedSubmission.dataSources.join("||"));
  formData.set("localRelations", JSON.stringify(preparedSubmission.localRelationMap));
  formData.set("queryOptions", JSON.stringify(preparedSubmission.queryOptions));
  formData.set("clientJobId", clientJobId);
  formData.set("clientRunStartedAt", String(clientRunStartedAt));
  formData.set("clientPreSubmitMs", String(Math.max(0, performance.now() - clientRunStartedPerf)));
  if (cellCacheHydrationEnabled(cellRoot)) {
    setCellCacheHydrationVisualState(cellRoot, {
      status: "rehydrating",
      statusLabel: "Rehydrating",
      statusReason: "The query worker will verify the local DuckDB cache and rebuild it from S3 if it is missing, stale, expired, or physically gone.",
    });
  }

  clientSnapshot = appendLocalQueryProgressEvent(clientSnapshot, "client_submitting", {
    phase: "Submitting to backend",
    message: "Sending the query job request to the backend. Backend/SSE updates will replace this local monitor row.",
    progress: 0.16,
  });
  renderLocalQueryProgress(cellRoot, { cellId, notebookId, workspaceRoot, snapshot: clientSnapshot });

  let response = null;
  try {
    response = await window.fetch("/api/query-jobs", {
      method: "POST",
      body: formData,
      headers: {
        Accept: "application/json",
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "The query job request failed before a response arrived.";
    clientSnapshot = failLocalQueryJobSnapshot(clientSnapshot, message, {
      message: "The backend did not accept the query job request.",
    });
    applyLocalQueryJobSnapshot(clientSnapshot);
    renderLocalQueryFailure(cellRoot, {
      cellId,
      notebookId,
      workspaceRoot,
      sql: originalSql,
      error: message,
    });
    queryClientTimingStarts.delete(clientJobId);
    return;
  }

  if (!response.ok) {
    let message = `The query could not be started. HTTP ${response.status}.`;
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }

    clientSnapshot = failLocalQueryJobSnapshot(clientSnapshot, message, {
      message: `The backend rejected the query job request with HTTP ${response.status}.`,
    });
    applyLocalQueryJobSnapshot(clientSnapshot);
    renderLocalQueryFailure(cellRoot, {
      cellId,
      notebookId,
      workspaceRoot,
      sql: originalSql,
      error: message,
    });
    queryClientTimingStarts.delete(clientJobId);
    return;
  }

  const snapshot = normalizeQueryJob(await response.json());
  if (!snapshot) {
    return;
  }

  if (snapshot.jobId) {
    queryClientTimingStarts.delete(clientJobId);
    queryClientTimingStarts.set(snapshot.jobId, {
      startedPerf: clientRunStartedPerf,
    });
  }
  const displaySnapshot = normalizeQueryJobForDisplay(snapshot) || snapshot;
  if (displaySnapshot.executionSql) {
    const preparedPayload = {
      displaySql: originalSql,
      submittedSql: preparedSubmission.executionSql,
      executionSql: displaySnapshot.executionSql,
      touchedRelations: displaySnapshot.touchedRelations || [],
      touchedBuckets: displaySnapshot.touchedBuckets || [],
      executionMode: displaySnapshot.executionMode || "",
      duckdbExecutionPath: displaySnapshot.duckdbExecutionPath || "",
    };
    preparedSqlViewCache.set(cellRoot, {
      key: sqlPreparationCacheKey(cellRoot, preparedSubmission),
      payload: preparedPayload,
    });
    const editorRoot = cellRoot.querySelector("[data-editor-root]");
    if (sqlViewModeForEditor(editorRoot) === "duckdb") {
      renderDuckdbSqlPanel(editorRoot, { sql: displaySnapshot.executionSql });
    }
  }
  recordNotebookActivity(notebookId, "run");
  trackLocalQueryJobSnapshot(displaySnapshot, { removeJobIds: [clientJobId] });
  renderLocalQueryProgress(cellRoot, { cellId, notebookId, workspaceRoot, snapshot: displaySnapshot });
}

async function startQueryExplainForForm(form) {
  if (formCellLanguage(form) !== "sql") {
    return;
  }

  const workspaceRoot = form.closest("[data-workspace-notebook]");
  const cellRoot = form.closest("[data-query-cell]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellId = cellRoot?.dataset.cellId;
  if (!workspaceRoot || !cellRoot || !notebookId || !cellId) {
    return;
  }

  const explainButton = cellRoot.querySelector("[data-explain-cell]");
  if (explainButton?.disabled) {
    return;
  }

  const selectedSources = selectedDataSourcesForCell(cellRoot);
  if (selectedSources.some((sourceId) => String(sourceId || "").trim().toLowerCase().endsWith("_native"))) {
    await showMessageDialog({
      title: "Explain unavailable",
      copy: "Explain is available for DuckDB-backed SQL cells only.",
    });
    return;
  }

  const existingJob = queryJobForCell(notebookId, cellId);
  const existingPythonJob = pythonJobForCell(notebookId, cellId);
  if (queryJobIsRunning(existingJob) || pythonJobIsRunning(existingPythonJob)) {
    return;
  }

  const editorSource = cellRoot.querySelector("[data-editor-source]");
  syncVisibleDuckdbSqlToVirtual(cellRoot);
  const originalSql = editorSource?.value ?? "";
  const sourceValidation = await querySourceValidationController.validateBeforeExplain(cellRoot, originalSql);
  if (sourceValidation?.status === "invalid") {
    await showMessageDialog({
      title: "Explain blocked",
      copy: sourceValidation.message || "Referenced source(s) were not found.",
    });
    return;
  }

  let preparedSubmission = null;
  try {
    preparedSubmission = await prepareSqlSubmissionForCell(cellRoot, originalSql);
  } catch (error) {
    await showMessageDialog({
      title: "Explain failed",
      copy:
        error instanceof Error
          ? error.message
          : "The Local Workspace sources could not be prepared for explaining.",
    });
    return;
  }

  setQueryExplainButtonBusy(explainButton, true);
  try {
    const response = await window.fetch("/api/query-explain", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        sql: preparedSubmission.executionSql,
        displaySql: originalSql,
        notebookId,
        notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
        cellId,
        dataSources: selectedSources,
        localRelations: preparedSubmission.localRelationMap,
        queryOptions: preparedSubmission.queryOptions,
      }),
    });

    if (!response.ok) {
      let message = "The query plan could not be generated.";
      try {
        const payload = await response.json();
        message = payload?.detail || message;
      } catch (_error) {
        // Ignore invalid JSON bodies.
      }
      throw new Error(message);
    }

    openQueryExplainDialog(await response.json());
  } catch (error) {
    await showMessageDialog({
      title: "Explain failed",
      copy: error instanceof Error ? error.message : "The query plan could not be generated.",
    });
  } finally {
    setQueryExplainButtonBusy(explainButton, false);
    querySourceValidationController.refreshCell(cellRoot);
  }
}

async function startPythonJobForForm(form) {
  const workspaceRoot = form.closest("[data-workspace-notebook]");
  const cellRoot = form.closest("[data-query-cell]");
  const notebookId = workspaceNotebookId(workspaceRoot);
  const cellId = cellRoot?.dataset.cellId;
  if (!workspaceRoot || !cellRoot || !notebookId || !cellId) {
    return;
  }

  const existingJob = pythonJobForCell(notebookId, cellId);
  const existingQueryJob = queryJobForCell(notebookId, cellId);
  if (pythonJobIsRunning(existingJob) || queryJobIsRunning(existingQueryJob)) {
    return;
  }

  const formData = new FormData(form);
  const editorSource = cellRoot.querySelector("[data-editor-source]");
  const originalCode = editorSource?.value ?? "";
  let localRelationMap = {};
  try {
    const preparedExecution = await prepareLocalWorkspacePythonExecution(originalCode);
    localRelationMap = preparedExecution.localRelationMap || {};
  } catch (error) {
    const resultRoot = cellRoot.querySelector("[data-cell-result]");
    if (resultRoot) {
      resultRoot.outerHTML = pythonResultPanelMarkup(cellId, {
        jobId: `local-python-error-${cellId}`,
        notebookId,
        notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
        cellId,
        code: originalCode,
        language: "python",
        status: "failed",
        durationMs: 0,
        updatedAt: new Date().toISOString(),
        outputs: [
          {
            type: "error",
            errorName: "Local Workspace Error",
            errorValue:
              error instanceof Error
                ? error.message
                : "The Local Workspace sources could not be prepared for Python execution.",
            text:
              error instanceof Error
                ? error.message
                : "The Local Workspace sources could not be prepared for Python execution.",
          },
        ],
        message: "Python execution failed.",
        error:
          error instanceof Error
            ? error.message
            : "The Local Workspace sources could not be prepared for Python execution.",
      });
    }
    return;
  }

  formData.set("code", originalCode);
  formData.set("sql", originalCode);
  formData.set("notebook_id", notebookId);
  formData.set("cell_id", cellId);
  formData.set("notebook_title", currentWorkspaceNotebookTitle(workspaceRoot));
  formData.set("data_sources", selectedDataSourcesForCell(cellRoot).join("||"));
  formData.set("localRelations", JSON.stringify(localRelationMap));

  const response = await window.fetch("/api/python-jobs", {
    method: "POST",
    body: formData,
    headers: {
      Accept: "application/json",
      "X-Workbench-Client-Id": workbenchClientId(),
    },
  });

  if (!response.ok) {
    let message = "The Python cell could not be started.";
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }

    const resultRoot = cellRoot.querySelector("[data-cell-result]");
    if (resultRoot) {
      resultRoot.outerHTML = pythonResultPanelMarkup(cellId, {
        jobId: `local-python-error-${cellId}`,
        notebookId,
        notebookTitle: currentWorkspaceNotebookTitle(workspaceRoot),
        cellId,
        code: originalCode,
        language: "python",
        status: "failed",
        durationMs: 0,
        updatedAt: new Date().toISOString(),
        outputs: [
          {
            type: "error",
            errorName: "Python execution failed",
            errorValue: message,
            text: message,
          },
        ],
        message: "Python execution failed.",
        error: message,
      });
    }
    return;
  }

  const snapshot = normalizePythonJob(await response.json());
  if (!snapshot) {
    return;
  }

  recordNotebookActivity(notebookId, "run");
  applyOptimisticPythonJobSnapshot({
    snapshot,
    applyPythonJobsState,
    getPythonState: currentPythonState,
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
    const snapshot = normalizeQueryJobForDisplay(await response.json());
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

async function acknowledgeQueryClientTiming(job) {
  const jobId = String(job?.jobId || "").trim();
  if (!jobId || !queryJobTerminalStatuses.has(String(job?.status || "").trim())) {
    return;
  }
  if (acknowledgedQueryClientTimings.has(jobId)) {
    return;
  }
  const timingStart = queryClientTimingStarts.get(jobId);
  if (!timingStart || !Number.isFinite(Number(timingStart.startedPerf))) {
    return;
  }

  acknowledgedQueryClientTimings.add(jobId);
  const observedMs = Number(timingStart.observedMs);
  const clientTotalMs =
    Number.isFinite(observedMs) && observedMs >= 0
      ? observedMs
      : Math.max(0, performance.now() - Number(timingStart.startedPerf));
  try {
    const response = await window.fetch(`/api/query-jobs/${encodeURIComponent(jobId)}/client-timing`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ clientTotalMs }),
    });
    if (!response.ok) {
      return;
    }
    const snapshot = normalizeQueryJobForDisplay(await response.json());
    if (!snapshot) {
      return;
    }
    queryClientTimingStarts.delete(jobId);
    applyOptimisticQueryJobSnapshot({
      snapshot,
      applyQueryJobsState,
      getQueryState: currentQueryState,
    });
  } catch (error) {
    console.warn("Failed to record query client timing.", error);
  }
}

async function cancelPythonJob(jobId) {
  if (!jobId) {
    return;
  }

  const response = await window.fetch(`/api/python-jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: "POST",
    headers: {
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    return;
  }

  try {
    await loadPythonJobsState();
  } catch (_error) {
    const snapshot = normalizePythonJob(await response.json());
    if (!snapshot) {
      return;
    }

    applyOptimisticPythonJobSnapshot({
      snapshot,
      applyPythonJobsState,
      getPythonState: currentPythonState,
    });
  }
}

async function restartPythonKernel(notebookId) {
  const normalizedNotebookId = String(notebookId || "").trim();
  if (!normalizedNotebookId) {
    return;
  }

  const response = await window.fetch(
    `/api/python-kernels/${encodeURIComponent(normalizedNotebookId)}/restart`,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "X-Workbench-Client-Id": workbenchClientId(),
      },
    }
  );

  if (!response.ok) {
    let message = "The Python session could not be restarted.";
    try {
      const payload = await response.json();
      message = payload?.detail || message;
    } catch (_error) {
      // Ignore invalid JSON bodies.
    }
    await showMessageDialog({
      title: "Python session restart failed",
      copy: message,
    });
    return;
  }

  const payload = await response.json();
  await showMessageDialog({
    title: "Python session restarted",
    copy: String(payload?.message || "Python session restarted."),
  });
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
    renderSourceInspectorMarkup(
      localWorkspaceInspectorMarkup(sourceObjectRoot),
      false,
      sourceBrowserScopeRoot(sourceObjectRoot)
    );
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
      renderSourceInspectorMarkup(
        localWorkspaceInspectorMarkup(movedNode),
        false,
        sourceBrowserScopeRoot(movedNode)
      );
    }
    movedNode.scrollIntoView({ block: "nearest" });
  }

  return updatedEntry;
}

async function copyLocalWorkspaceExport(entryId, options = {}) {
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
    throw new Error("Provide a file name before copying the Local Workspace file.");
  }

  const allEntries = await listLocalWorkspaceExports();
  const duplicateEntry = allEntries.find(
    (candidate) =>
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
  const copiedEntry = await saveLocalWorkspaceExport({
    ...entry,
    id: createLocalWorkspaceEntryId(),
    fileName: normalizedFileName,
    folderPath: normalizedFolderPath,
    createdAt: timestamp,
    updatedAt: timestamp,
  });

  await renderLocalWorkspaceSidebarEntries();
  await revealLocalWorkspaceFolderPath(normalizedFolderPath);

  const copiedNode = localWorkspaceEntryNode(copiedEntry.id);
  if (copiedNode instanceof Element) {
    setSelectedSourceObjectState(copiedNode);
    renderSourceInspectorMarkup(
      localWorkspaceInspectorMarkup(copiedNode),
      false,
      sourceBrowserScopeRoot(copiedNode)
    );
    copiedNode.scrollIntoView({ block: "nearest" });
  }

  return copiedEntry;
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

function s3DeleteDescriptorMatchesKey(descriptor, bucket, key) {
  const descriptorBucket = String(descriptor?.bucket || "").trim();
  const descriptorPrefix = String(descriptor?.prefix || "").trim();
  const entryKind = String(descriptor?.entryKind || "").trim();
  const candidateBucket = String(bucket || "").trim();
  const candidateKey = String(key || "").trim();

  if (!descriptorBucket || descriptorBucket !== candidateBucket) {
    return false;
  }
  if (entryKind === "bucket") {
    return true;
  }
  if (entryKind === "folder") {
    return Boolean(descriptorPrefix) && candidateKey.startsWith(descriptorPrefix);
  }
  return entryKind === "file" && descriptorPrefix === candidateKey;
}

function collectS3PendingDeleteTargets(descriptor) {
  const targets = new Set();
  const descriptorBucket = String(descriptor?.bucket || "").trim();
  const descriptorPrefix = String(descriptor?.prefix || "").trim();
  const entryKind = String(descriptor?.entryKind || "").trim();
  if (!descriptorBucket || !entryKind) {
    return [];
  }

  document.querySelectorAll("[data-source-object][data-s3-bucket]").forEach((node) => {
    if (s3DeleteDescriptorMatchesKey(descriptor, node.dataset.s3Bucket, node.dataset.s3Key)) {
      targets.add(node);
    }
  });

  if (entryKind === "bucket") {
    document.querySelectorAll("[data-source-schema]").forEach((node) => {
      const schemaBucket = String(node.dataset.sourceBucket || "").trim();
      const hasBucketObject = Array.from(node.querySelectorAll("[data-source-object][data-s3-bucket]")).some(
        (sourceObject) => String(sourceObject.dataset.s3Bucket || "").trim() === descriptorBucket
      );
      if (schemaBucket === descriptorBucket || hasBucketObject) {
        targets.add(node);
      }
    });
  }

  document.querySelectorAll("[data-s3-explorer-entry]").forEach((node) => {
    const candidateBucket = String(node.dataset.s3ExplorerBucket || "").trim();
    const candidatePrefix = String(node.dataset.s3ExplorerPrefix || "").trim();
    if (s3DeleteDescriptorMatchesKey(descriptor, candidateBucket, candidatePrefix)) {
      targets.add(node);
    }
  });

  document.querySelectorAll(".data-source-explorer-object[data-bucket]").forEach((node) => {
    const candidateBucket = String(node.dataset.bucket || "").trim();
    const candidatePrefix = String(node.dataset.prefix || "").trim();
    if (s3DeleteDescriptorMatchesKey(descriptor, candidateBucket, candidatePrefix)) {
      targets.add(node);
    }
  });

  if (entryKind === "file" && descriptorPrefix) {
    document
      .querySelectorAll(
        `[data-source-object][data-s3-bucket="${escapeSelectorValue(
          descriptorBucket
        )}"][data-s3-key="${escapeSelectorValue(descriptorPrefix)}"]`
      )
      .forEach((node) => targets.add(node));
  }

  return Array.from(targets);
}

function setS3PendingDeleteState(descriptor, pending) {
  collectS3PendingDeleteTargets(descriptor).forEach((node) => {
    node.classList.toggle("is-pending-delete", pending);
    if (pending) {
      node.dataset.pendingDelete = "true";
      node.setAttribute("aria-busy", "true");
    } else {
      delete node.dataset.pendingDelete;
      node.removeAttribute("aria-busy");
    }

    node
      .querySelectorAll(
        "[data-delete-source-s3-object], [data-delete-source-s3-bucket], [data-s3-explorer-entry-delete]"
      )
      .forEach((button) => {
        if (button instanceof HTMLButtonElement) {
          button.disabled = pending;
        }
      });
  });
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
  return s3DeleteJobsController.startDelete(descriptor, {
    refreshSidebarAfter,
    refreshExplorerAfter,
    showSidebarStatus,
  });
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

  const normalizedBucketName = normalizeS3BucketNameForCreate(bucketName);

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

function s3ExplorerPickerEntryMarkup(entry) {
  if (entry.entryKind === "file") {
    return `
      <div
        class="s3-explorer-file s3-explorer-file-readonly"
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
        </div>
      </summary>
      <div class="tree-children s3-explorer-children" data-s3-explorer-children></div>
    </details>
  `;
}

function s3ExplorerPickerChildrenMarkup(snapshot) {
  if (!snapshot.entries.length) {
    return `<p class="s3-explorer-empty">${escapeHtml(snapshot.emptyMessage || "This location is empty.")}</p>`;
  }
  return snapshot.entries.map((entry) => s3ExplorerPickerEntryMarkup(entry)).join("");
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
      : "Save to S3 Object Storage";
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

  const normalizedBucketName = normalizeS3BucketNameForCreate(bucketName);

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
  const configuredTarget = resultStorageExportTarget(job);
  resultExportDialogState.jobId = job.jobId;
  resultExportDialogState.exportFormat = normalizeResultExportFormat(
    configuredTarget?.exportFormat || exportFormat
  );
  resultExportDialogState.exportSettings = defaultResultExportSettings(resultExportDialogState.exportFormat);
  resultExportDialogState.fileName = configuredTarget?.fileName ||
    defaultQueryResultExportFilename(job, resultExportDialogState.exportFormat);
  if (configuredTarget) {
    resultExportDialogState.selectedBucket = configuredTarget.bucket;
    resultExportDialogState.selectedPrefix = configuredTarget.prefix;
  }
  resultExportDialogState.saving = false;

  const titleNode = dialog.querySelector("[data-result-export-title]");
  const copyNode = dialog.querySelector("[data-result-export-copy]");
  if (titleNode) {
    titleNode.textContent = "Save Results in S3 Object Storage ...";
  }
  if (copyNode) {
    copyNode.textContent = configuredTarget
      ? `This result is already materialized at ${configuredTarget.path}. Its canonical destination is preselected; saving again replaces that object.`
      : "Choose a S3 Object Storage location, then select the export format and any format-specific settings.";
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
    return false;
  }

  const { scrollToTop = true, navigationToken = null } = options;
  const token =
    navigationToken ??
    workspaceNavigation.begin({
      path: isLocalNotebookId(notebookId)
        ? "/query-workbench"
        : `/notebooks/${encodeURIComponent(notebookId)}`,
      notebookId,
    });
  if (notebookMetadata(notebookId).deleted) {
    const fallbackNotebookId = nextVisibleNotebookId(notebookId);
    if (!fallbackNotebookId) {
      if (workspaceNavigationIsCurrent(token)) {
        renderEmptyWorkspace();
        writeLastNotebookId("");
      }
      return false;
    }

    notebookId = fallbackNotebookId;
  }

  if (isLocalNotebookId(notebookId)) {
    if (workspaceNavigationIsCurrent(token)) {
      renderLocalNotebookWorkspace(notebookId, { scrollToTop });
      panel.dataset.workspaceNavigationEpoch = String(token.epoch);
    }
    return workspaceNavigationIsCurrent(token);
  }

  const response = await window.fetch(`/notebooks/${encodeURIComponent(notebookId)}`, {
    headers: { "HX-Request": "true" },
    signal: token.signal,
  });
  if (!workspaceNavigationIsCurrent(token)) {
    return false;
  }
  if (!response.ok) {
    throw new Error(`Failed to load notebook ${notebookId}: ${response.status}`);
  }

  const workspaceMarkup = await response.text();
  if (!workspaceNavigationIsCurrent(token)) {
    return false;
  }

  await ensureNotebookEditorMetadata({ signal: token.signal });
  if (!workspaceNavigationIsCurrent(token)) {
    return false;
  }

  panel.innerHTML = workspaceMarkup;
  panel.dataset.workspaceNavigationEpoch = String(token.epoch);
  syncShellVisibility();
  applyWorkbenchTitle("query");
  if (panel.querySelector(`[data-notebook-meta][data-notebook-id="${CSS.escape(notebookId)}"][data-shared="true"]`)) {
    sharedNotebookDrafts.delete(notebookId);
  }
  processHtmx(panel);
  initializeEditors(panel);
  applyNotebookMetadata();
  if (currentSidebarMode() !== "notebook") {
    await refreshSidebar("notebook", {
      signal: token.signal,
      isCurrent: () => workspaceNavigationIsCurrent(token),
    });
    if (!workspaceNavigationIsCurrent(token)) {
      return false;
    }
  }
  activateNotebookLink(notebookId);
  revealNotebookLink(notebookId);
  writeLastNotebookId(notebookId);
  recordNotebookActivity(notebookId, "open");
  syncVisibleQueryCells();
  syncVisiblePythonCells();
  syncVisibleResultStorageControls(panel);
  querySourceValidationController.refreshAll(panel);
  refreshVisibleCacheHydrationStatuses(panel);
  if (panel.querySelector('[data-notebook-meta][data-default-pipeline-mode="pipeline"]')) {
    notebookStagePipelineController.initializeCurrentWorkspace().catch((error) => {
      console.error("Failed to initialize notebook pipeline.", error);
    });
  }
  renderQueryNotificationMenu();
  if (scrollToTop) {
    scrollWorkspaceNotebookIntoView();
  }
  return workspaceNavigationIsCurrent(token);
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

document.addEventListener(
  "submit",
  async (event) => {
    const queryForm = event.target.closest("[data-query-form]");
    if (queryForm) {
      event.preventDefault();
      if (await notebookStagePipelineController.handleQueryFormSubmit(queryForm)) {
        return;
      }
      await startQueryJobForForm(queryForm);
      return;
    }

    if (await dataProductsController.handleSubmit(event)) {
      return;
    }

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

    const fileIngestionForm = event.target.closest("[data-file-ingestion-form]");
    if (fileIngestionForm) {
      event.preventDefault();
      try {
        await submitFileIngestionForm(fileIngestionForm);
      } catch (error) {
        console.error("Failed to import files.", error);
        await showMessageDialog({
          title: "File import failed",
          copy: error instanceof Error ? error.message : "The files could not be imported.",
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
        const copyingFile = localWorkspaceMoveDialogState.operationKind === "copy";
        const transferringToSharedWorkspace =
          localWorkspaceMoveDialogState.destinationKind === "s3";
        let localCleanupFailed = false;
        const movedEntry = transferringToSharedWorkspace
          ? copyingFile
            ? await copyLocalWorkspaceEntryToS3(localWorkspaceMoveDialogState.entryId, {
                bucket: localWorkspaceMoveDialogState.selectedBucket,
                prefix: localWorkspaceMoveDialogState.selectedPrefix,
                fileName: localWorkspaceMoveDialogState.fileName,
              })
            : await moveLocalWorkspaceEntryToS3(localWorkspaceMoveDialogState.entryId, {
                bucket: localWorkspaceMoveDialogState.selectedBucket,
                prefix: localWorkspaceMoveDialogState.selectedPrefix,
                fileName: localWorkspaceMoveDialogState.fileName,
              })
          : copyingFile
            ? await copyLocalWorkspaceExport(localWorkspaceMoveDialogState.entryId, {
                fileName: localWorkspaceMoveDialogState.fileName,
                folderPath: localWorkspaceMoveDialogState.folderPath,
              })
            : await moveLocalWorkspaceExport(localWorkspaceMoveDialogState.entryId, {
                fileName: localWorkspaceMoveDialogState.fileName,
                folderPath: localWorkspaceMoveDialogState.folderPath,
              });

        if (transferringToSharedWorkspace && !copyingFile) {
          try {
            await deleteLocalWorkspaceExport(localWorkspaceMoveDialogState.entryId);
            clearLocalWorkspaceQuerySourceCache(localWorkspaceMoveDialogState.entryId);
            clearSourceObjectFieldCacheForRelations([
              localWorkspaceRelation(localWorkspaceMoveDialogState.entryId),
            ]);
            if (
              getActiveSourceObjectRelation() ===
              localWorkspaceRelation(localWorkspaceMoveDialogState.entryId)
            ) {
              setSelectedSourceObjectState(null);
              renderSourceInspectorMarkup("", true);
            }
          } catch (cleanupError) {
            localCleanupFailed = true;
            console.error("Shared Workspace upload succeeded but the Local Workspace cleanup failed.", cleanupError);
          }
        }

        if (transferringToSharedWorkspace) {
          await refreshSidebar(currentWorkspaceMode());
        }

        closeDialog(localWorkspaceMoveDialog(), "confirm");
        if (!transferringToSharedWorkspace && movedEntry) {
          await revealLocalWorkspaceFolderPath(movedEntry.folderPath);
        }
        if (movedEntry) {
          await showMessageDialog({
            title: copyingFile ? "Local Workspace file copied" : "Local Workspace file moved",
            copy: transferringToSharedWorkspace
              ? copyingFile
                ? `${movedEntry.fileName} was copied to ${movedEntry.path}. The Local Workspace copy was kept in this browser.`
                : localCleanupFailed
                  ? `${movedEntry.fileName} was uploaded to ${movedEntry.path}, but the browser-local copy could not be removed automatically.`
                  : `${movedEntry.fileName} was moved to ${movedEntry.path}.`
              : copyingFile
                ? `${movedEntry.fileName} was copied to ${localWorkspaceDisplayPath(movedEntry.folderPath)} in this browser.`
                : `${movedEntry.fileName} was moved to ${localWorkspaceDisplayPath(movedEntry.folderPath)} in this browser.`,
          });
        }
      } catch (error) {
        console.error(
          `Failed to ${localWorkspaceMoveDialogState.operationKind === "copy" ? "copy" : "move"} the Local Workspace file.`,
          error
        );
        await showMessageDialog({
          title:
            localWorkspaceMoveDialogState.operationKind === "copy"
              ? "Local Workspace copy failed"
              : "Local Workspace move failed",
          copy:
            error instanceof Error
              ? error.message
              : localWorkspaceMoveDialogState.operationKind === "copy"
                ? "The Local Workspace file could not be copied."
                : "The Local Workspace file could not be moved.",
        });
      } finally {
        setLocalWorkspaceMoveDialogBusy(false);
      }
      return;
    }

    return;
  },
  true
);

document.body.addEventListener("click", async (event) => {
  setActiveCell(event.target.closest("[data-query-cell]"));
  closePopupMenusForTarget(event.target);
  if (
    !event.target.closest("[data-cell-source-navigation-menu]")
    && !event.target.closest("[data-navigate-cell-source]")
  ) {
    closeCellSourceNavigationMenu();
  }

  const modalCancelButton = event.target.closest("[data-modal-cancel]");
  if (modalCancelButton) {
    event.preventDefault();
    closeDialog(modalCancelButton.closest("dialog"), "cancel");
    return;
  }

  const cellSourceNavigationChoice = event.target.closest("[data-navigate-cell-source-choice]");
  if (cellSourceNavigationChoice) {
    event.preventDefault();
    event.stopPropagation();
    const index = Number(cellSourceNavigationChoice.dataset.navigateCellSourceChoice || -1);
    const source = Number.isInteger(index) ? cellSourceNavigationChoices[index] : null;
    closeCellSourceNavigationMenu();
    if (source) {
      await navigateToPreparedSourceObject(source);
    }
    return;
  }

  const cellSourceNavigationButton = event.target.closest("[data-navigate-cell-source]");
  if (cellSourceNavigationButton) {
    event.preventDefault();
    event.stopPropagation();
    await navigateCellSourceObject(cellSourceNavigationButton);
    return;
  }

  const editorCopyButton = event.target.closest("[data-copy-editor-sql]");
  if (editorCopyButton) {
    event.preventDefault();
    event.stopPropagation();
    try {
      await copyEditorSql(editorCopyButton.closest("[data-editor-root]"), editorCopyButton);
    } catch (error) {
      console.error("Failed to copy SQL from the editor.", error);
      await showMessageDialog({
        title: "Copy SQL failed",
        copy: error instanceof Error ? error.message : "The SQL could not be copied to the clipboard.",
      });
    }
    return;
  }

  const editorCompareButton = event.target.closest("[data-compare-editor-sql]");
  if (editorCompareButton) {
    event.preventDefault();
    event.stopPropagation();
    queryCompareController.open(editorCompareButton);
    return;
  }

  const editorExpandButton = event.target.closest("[data-expand-editor]");
  if (editorExpandButton) {
    event.preventDefault();
    event.stopPropagation();
    toggleEditorExpanded(editorExpandButton.closest("[data-editor-root]"));
    return;
  }

  const editorSqlViewButton = event.target.closest("[data-editor-sql-view]");
  if (editorSqlViewButton) {
    event.preventDefault();
    event.stopPropagation();
    await setEditorSqlViewMode(
      editorSqlViewButton.closest("[data-editor-root]"),
      editorSqlViewButton.dataset.editorSqlView
    );
    return;
  }

  const resultCollapseButton = event.target.closest("[data-query-result-toggle]");
  if (resultCollapseButton) {
    event.preventDefault();
    event.stopPropagation();
    toggleQueryResultPanel(resultCollapseButton);
    return;
  }

  const resultChartsToggle = event.target.closest("[data-query-result-toggle-charts]");
  if (resultChartsToggle) {
    event.preventDefault();
    event.stopPropagation();
    toggleQueryResultCharts(resultChartsToggle);
    return;
  }

  const queryTimingsCopyTrigger = event.target.closest("[data-copy-query-timings]");
  if (queryTimingsCopyTrigger) {
    event.preventDefault();
    event.stopPropagation();
    try {
      await copyQueryTimingTable(queryTimingsCopyTrigger);
    } catch (error) {
      console.error("Failed to copy query timing table.", error);
      await showMessageDialog({
        title: "Copy timing table failed",
        copy: error instanceof Error ? error.message : "The query timing table could not be copied.",
      });
    }
    return;
  }

  const resultStorageVirtualCopyTrigger = event.target.closest("[data-copy-result-storage-virtual]");
  if (resultStorageVirtualCopyTrigger) {
    event.preventDefault();
    event.stopPropagation();
    try {
      await copyResultStorageReference(resultStorageVirtualCopyTrigger, "virtual");
    } catch (error) {
      console.error("Failed to copy virtual result storage path.", error);
      await showMessageDialog({
        title: "Copy result path failed",
        copy: error instanceof Error ? error.message : "The virtual result storage path could not be copied.",
      });
    }
    return;
  }

  const publishJourneyDataProductTrigger = event.target.closest(
    "[data-publish-journey-data-product]"
  );
  if (publishJourneyDataProductTrigger) {
    event.preventDefault();
    event.stopPropagation();
    await dataProductsController.openPublishDialog({
      source: {
        sourceKind: "relation",
        sourceId: "s3",
        relation:
          "data_analysts_journey_6f15a669.kantonale_gewerbesteuer_soll_ist_2022_2026",
        sourceDisplayName: "Kantonale Gewerbesteuer Soll/Ist 2022–2026",
        sourcePlatform: "s3",
      },
      lockSource: true,
      startStep: 2,
    });
    return;
  }

  const resultStorageDuckdbCopyTrigger = event.target.closest("[data-copy-result-storage-duckdb]");
  if (resultStorageDuckdbCopyTrigger) {
    event.preventDefault();
    event.stopPropagation();
    try {
      await copyResultStorageReference(resultStorageDuckdbCopyTrigger, "duckdb");
    } catch (error) {
      console.error("Failed to copy DuckDB result storage path.", error);
      await showMessageDialog({
        title: "Copy result path failed",
        copy: error instanceof Error ? error.message : "The DuckDB result storage path could not be copied.",
      });
    }
    return;
  }

  const timingDetailsToggle = event.target.closest("[data-query-duration-details-toggle]");
  if (timingDetailsToggle) {
    event.preventDefault();
    event.stopPropagation();
    toggleQueryTimingDetails(timingDetailsToggle);
    return;
  }

  const queryExplainTab = event.target.closest("[data-query-explain-tab]");
  if (queryExplainTab) {
    event.preventDefault();
    queryExplainDialogState.activeTab = queryExplainTab.dataset.queryExplainTab || "briefing";
    renderQueryExplainDialog();
    return;
  }

  const explainCellButton = event.target.closest("[data-explain-cell]");
  if (explainCellButton) {
    event.preventDefault();
    const form = explainCellButton.closest("[data-query-form]");
    if (!form) {
      return;
    }
    await startQueryExplainForForm(form);
    return;
  }

  const runCellButton = event.target.closest("[data-run-cell]");
  if (runCellButton) {
    event.preventDefault();
    if (await notebookStagePipelineController.handleRunCellButton(runCellButton)) {
      return;
    }
    const form = runCellButton.closest("[data-query-form]");
    if (!form) {
      return;
    }
    await startQueryJobForForm(form);
    return;
  }

  const cancelCellButton = event.target.closest("[data-cancel-query]");
  if (cancelCellButton) {
    event.preventDefault();
    const jobId = cancelCellButton.dataset.jobId || "";
    if (!jobId) {
      return;
    }
    if (cancelCellButton.dataset.jobKind === "python") {
      await cancelPythonJob(jobId);
      return;
    }
    await cancelQueryJob(jobId);
    return;
  }

  if (await downloadJobsController.handleClick(event)) {
    return;
  }

  if (await handleWorkbenchNavigationClick(event)) {
    return;
  }

  if (await dataProductsController.handleClick(event)) {
    return;
  }

  if (await serviceConsumptionUi.handleClick(event)) {
    return;
  }

  if (await notebookStagePipelineController.handleClick(event)) {
    return;
  }

  if (await queryRunsController.handleClick(event)) {
    return;
  }

  if (await handleIngestionClick(event)) {
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

  if (await handleToggleFolderSharedClick(event)) {
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
    const loaded = await loadNotebookWorkspace(link.dataset.notebookId);
    if (loaded) {
      pushNotebookHistory(link.dataset.notebookId);
    }
    return;
  }

  syncActiveNotebookSelection(event);
});

document.body.addEventListener("focusin", (event) => {
  handleNotebookWorkspaceFocusIn(event);
});

document.body.addEventListener("contextmenu", (event) => {
  notebookStagePipelineController.handleContextMenu(event);
});

document.body.addEventListener("paste", (event) => {
  handleCsvIngestionPaste(event);
});

document.body.addEventListener("input", (event) => {
  if (dataProductsController.handleInput(event)) {
    return;
  }

  if (handleCsvIngestionInput(event)) {
    return;
  }

  if (handleFileIngestionInput(event)) {
    return;
  }

  const duckdbSqlPanel = event.target.closest("[data-duckdb-sql-panel]");
  if (duckdbSqlPanel) {
    syncVirtualSqlFromDuckdbPanel(duckdbSqlPanel);
    return;
  }

  const queryEditorSource = event.target.closest("[data-editor-source]");
  if (queryEditorSource) {
    invalidatePreparedSqlViewForCell(queryEditorSource.closest("[data-query-cell]"));
    querySourceValidationController.handleTextareaInput(queryEditorSource);
  }

  const resultStoragePathInput = event.target.closest("input[data-result-storage-path]");
  if (resultStoragePathInput) {
    const cellRoot = resultStoragePathInput.closest("[data-query-cell]");
    syncCellResultStorageState(cellRoot, { proposeIfEmpty: false });
    if (pipelineResultStorageForCellRoot(cellRoot)) {
      const workspaceRoot = cellRoot.closest("[data-workspace-notebook]");
      const notebookId = workspaceNotebookId(workspaceRoot);
      const cellId = String(cellRoot?.dataset?.cellId || "").trim();
      if (notebookId && cellId) {
        setCellStage(notebookId, cellId, { outputPath: resultStoragePathInput.value }, { rerender: false });
        notebookStagePipelineController?.refreshGraph?.(notebookId)?.catch?.((error) => {
          console.error("Failed to refresh notebook pipeline after stage output path edit.", error);
        });
      }
    }
    invalidatePreparedSqlViewForCell(cellRoot);
  }

  if (handleNotebookWorkspaceInput(event)) {
    return;
  }

  if (notebookStagePipelineController.handleInput(event)) {
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

document.body.addEventListener("change", async (event) => {
  if (queryCompareController.handleChange(event)) {
    return;
  }

  const localWorkspaceMoveDestinationSelect = event.target.closest(
    "[data-local-workspace-move-destination]"
  );
  if (localWorkspaceMoveDestinationSelect instanceof HTMLSelectElement) {
    try {
      await updateLocalWorkspaceMoveDestinationKind(localWorkspaceMoveDestinationSelect.value);
    } catch (error) {
      console.error("Failed to load Shared Workspace locations for the Local Workspace move dialog.", error);
      await showMessageDialog({
        title: "Shared Workspace unavailable",
        copy:
          error instanceof Error
            ? error.message
            : "The Shared Workspace explorer could not be loaded.",
      });
    }
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
  if (dataProductsController.handleChange(event)) {
    return;
  }

  if (handleCsvIngestionChange(event)) {
    return;
  }

  if (handleFileIngestionChange(event)) {
    return;
  }

  const changedCellSourceOption = event.target.closest("[data-cell-source-option]");
  const changedCellQueryOption = event.target.closest("[data-cell-query-option]");
  if (changedCellSourceOption || changedCellQueryOption) {
    const changedCellRoot = (changedCellSourceOption || changedCellQueryOption).closest("[data-query-cell]");
    if (changedCellQueryOption?.dataset?.cellQueryOption?.startsWith("duckdb.resultStorage")) {
      if (pipelineResultStorageForCellRoot(changedCellRoot)) {
        const toggle = changedCellRoot.querySelector('[data-cell-query-option="duckdb.resultStorage.mode"]');
        if (toggle instanceof HTMLInputElement) {
          toggle.checked = true;
        }
      }
      syncCellResultStorageState(changedCellRoot);
    }
    invalidatePreparedSqlViewForCell(changedCellRoot);
  }
  if (handleNotebookWorkspaceChange(event)) {
    if (changedCellSourceOption) {
      querySourceValidationController.scheduleValidationForCell(
        changedCellSourceOption.closest("[data-query-cell]")
      );
    }
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

  if (handleFileDragOver(event)) {
    return;
  }

  handleNotebookDragOver(event);
});

document.body.addEventListener("dragleave", (event) => {
  handleCsvDragLeave(event);
  handleFileDragLeave(event);
});

document.body.addEventListener("drop", (event) => {
  if (handleCsvDrop(event)) {
    return;
  }
  handleFileDrop(event);
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
    const notebookRoot = event.target?.closest?.("[data-notebook-section]");
    if (notebookRoot === event.target && notebookRoot.open) {
      loadDeferredSidebarNotebookTree().catch((error) => {
        console.error("Failed to load deferred notebook tree.", error);
      });
    }
    const runbookRoot = event.target?.closest?.("[data-ingestion-runbook-section]");
    if (runbookRoot === event.target && runbookRoot.open) {
      loadDeferredSidebarRunbookTree().catch((error) => {
        console.error("Failed to load deferred Loader runbooks.", error);
      });
    }
    const dataSourcesRoot = event.target?.closest?.("[data-data-sources-section]");
    if (dataSourcesRoot === event.target && dataSourcesRoot.open) {
      loadDeferredSidebarSourceTree().catch((error) => {
        console.error("Failed to load deferred source tree.", error);
      });
    }
  },
  true
);

document.body.addEventListener("keydown", (event) => {
  handleNotebookWorkspaceTagInputKeydown(event);
});

document.body.addEventListener("keydown", async (event) => {
  await handleNotebookWorkspaceRenameTitleKeydown(event);
});

document.body.addEventListener("keydown", async (event) => {
  if (!["Enter", " "].includes(event.key)) {
    return;
  }
  const queryTimingsCopyTrigger = event.target.closest?.("[data-copy-query-timings]");
  if (!queryTimingsCopyTrigger) {
    return;
  }
  event.preventDefault();
  event.stopPropagation();
  try {
    await copyQueryTimingTable(queryTimingsCopyTrigger);
  } catch (error) {
    console.error("Failed to copy query timing table.", error);
    await showMessageDialog({
      title: "Copy timing table failed",
      copy: error instanceof Error ? error.message : "The query timing table could not be copied.",
    });
  }
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
  syncVisiblePythonCells();
  syncVisibleResultStorageControls(event.target);
  querySourceValidationController.refreshAll(event.target);
  refreshVisibleCacheHydrationStatuses(event.target);
  queryRunsController.initializeCurrentPage(event.target).catch((error) => {
    console.error("Failed to initialize query-run history after a partial swap.", error);
  });
  queryWorkbenchEntryController.initializeCurrentPage(event.target).catch((error) => {
    console.error("Failed to initialize Query Workbench entry after a partial swap.", error);
  });
  renderQueryNotificationMenu();
  dataProductsController.initializeCurrentPage();
  initializeWorkbenchExpertSearch(event.target);
  dataExchangeController.initializeCurrentPage();
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
  if (window.location.pathname === "/data-products") {
    try {
      await loadDataProductsPage({ pushHistory: false });
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore data products from browser history.", error);
      }
    }
    return;
  }

  if (window.location.pathname === "/data-exchange") {
    try {
      await loadDataExchangePage({ pushHistory: false });
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore DataExchange from browser history.", error);
      }
    }
    return;
  }

  if (
    window.location.pathname === "/data-sources" ||
    window.location.pathname === "/query-workbench/data-sources"
  ) {
    try {
      const params = new URLSearchParams(window.location.search);
      await loadQueryWorkbenchDataSources(
        params.get("source_id") || "",
        { pushHistory: false, browse: params.get("browse") === "1" }
      );
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore managed data sources from browser history.", error);
      }
    }
    return;
  }

  if (
    window.location.pathname === "/data-sources/browser" ||
    window.location.pathname === "/query-workbench/data-sources/explorer"
  ) {
    try {
      await loadQueryWorkbenchDataSourceExplorer(
        new URLSearchParams(window.location.search).get("source_id") || "",
        { pushHistory: false }
      );
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore the data source explorer from browser history.", error);
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

  if (window.location.pathname === "/query-workbench/query-runs") {
    try {
      await loadQueryRunsPage({ pushHistory: false });
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore query runs from browser history.", error);
      }
    }
    return;
  }

  if (window.location.pathname === "/loader-workbench") {
    try {
      await openLoaderWorkbench({ pushHistory: false });
    } catch (error) {
      if (error?.name !== "AbortError") {
        console.error("Failed to restore the Loader Workbench from browser history.", error);
      }
    }
    return;
  }

  if (window.location.pathname === "/ingestion-workbench") {
    try {
      await openIngestionWorkbench({ pushHistory: false });
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

window.addEventListener("focus", () => {
  if (document.visibilityState === "hidden") {
    return;
  }
  refreshVisibleCacheHydrationStatuses(document.getElementById("workspace-panel") || document);
});

document.documentElement.dataset.workbenchInteractive = "true";
initializeEditors();
initializeSidebarSearch();
initializeNotebookTree();
initializeSidebarToggle();
initializeSidebarResizer();
syncVisibleResultStorageControls(document.getElementById("workspace-panel") || document);
renderLocalWorkspaceSidebarEntries().catch((error) => {
  console.error("Failed to render Local Workspace entries during startup.", error);
});
syncShellVisibility();
applyWorkbenchTitle();
applyNotebookMetadata();
restoreSelectedSourceObject();
const startupWorkspaceMode = currentWorkspaceMode();
const startupNavigationEpoch = workspaceNavigation.currentEpoch();
const initialLoadTasks = [
  loadQueryJobsState().catch((error) => {
    console.error("Failed to load query jobs.", error);
  }),
  loadPythonJobsState().catch((error) => {
    console.error("Failed to load python jobs.", error);
  }),
  loadDataGenerationJobsState().catch((error) => {
    console.error("Failed to load data generation jobs.", error);
  }),
  loadDownloadJobsState().catch((error) => {
    console.error("Failed to load prepared download jobs.", error);
  }),
  loadS3DeleteJobsState().catch((error) => {
    console.error("Failed to load S3 delete jobs.", error);
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

if (startupWorkspaceMode === "loader") {
  initialLoadTasks.push(
    loadDataGeneratorCatalog().catch((error) => {
      console.error("Failed to load data generators.", error);
    })
  );
}

Promise.allSettled(initialLoadTasks)
  .finally(() => {
    ensureRealtimeEventsEventSource();
    if (workspaceNavigation.currentEpoch() !== startupNavigationEpoch) {
      return;
    }
    const visibleWorkspaceMode = currentWorkspaceMode();
    const initialSidebarMode = visibleWorkspaceMode === "loader" ? "loader" : "notebook";
    const shouldRefreshSidebarDuringStartup = !(
      homePageRoot() ||
      workbenchExpertSearchPageRoot() ||
      dataProductsPageRoot() ||
      dataExchangePageRoot() ||
      serviceConsumptionPageRoot() ||
      queryRunsPageRoot() ||
      queryWorkbenchEntryPageRoot() ||
      queryWorkbenchDataSourcesPageRoot() ||
      dataSourceExplorerPageRoot() ||
      currentWorkspaceMode() === "ingestion"
    );
    const sidebarRefreshTask = shouldRefreshSidebarDuringStartup
      ? refreshSidebar(initialSidebarMode).catch((error) => {
          console.error("Failed to refresh the sidebar during startup.", error);
        })
      : Promise.resolve();

    if (visibleWorkspaceMode === "loader") {
      renderIngestionWorkbench();
      renderDataGenerationMonitor();
      renderQueryNotificationMenu();
      return;
    }

    if (visibleWorkspaceMode === "ingestion") {
      renderCsvIngestionWorkbench();
      renderFileIngestionWorkbench();
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

    if (dataProductsPageRoot()) {
      dataProductsController.initializeCurrentPage();
      renderQueryNotificationMenu();
      return;
    }

    if (workbenchExpertSearchPageRoot()) {
      initializeWorkbenchExpertSearch();
      renderQueryNotificationMenu();
      return;
    }

    if (dataExchangePageRoot()) {
      dataExchangeController.initializeCurrentPage();
      renderQueryNotificationMenu();
      return;
    }

    if (queryRunsPageRoot()) {
      queryRunsController.initializeCurrentPage().catch((error) => {
        console.error("Failed to initialize query-run history.", error);
      });
      renderQueryNotificationMenu();
      return;
    }

    if (queryWorkbenchDataSourcesPageRoot()) {
      sidebarRefreshTask.finally(() => {
        initializeDataSourceManagementPage().catch((error) => {
          console.error("Failed to initialize the Data Source Workbench page.", error);
        });
      });
      renderQueryNotificationMenu();
      return;
    }

    if (dataSourceExplorerPageRoot()) {
      sidebarRefreshTask.finally(() => {
        dataSourceExplorerController.initializeCurrentPage().catch((error) => {
          console.error("Failed to initialize the Data Source Explorer page.", error);
        });
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

    if (queryWorkbenchEntryPageRoot()) {
      const storedNotebookId = readLastNotebookId();
      if (storedNotebookId && !notebookMetadata(storedNotebookId).deleted) {
        restoreLastNotebook().catch((error) => {
          console.error("Failed to restore the last active notebook from the Query Workbench entry.", error);
        });
        return;
      }
      renderHomePage();
      queryWorkbenchEntryController.initializeCurrentPage().catch((error) => {
        console.error("Failed to initialize the Query Workbench entry page.", error);
      });
      renderQueryNotificationMenu();
      return;
    }

    const currentNotebookId = currentWorkspaceNotebookId();
    if (window.location.pathname.startsWith("/notebooks/") && currentNotebookId) {
      activateNotebookLink(currentNotebookId);
      revealNotebookLink(currentNotebookId);
      writeLastNotebookId(currentNotebookId);
      queryRunsController.initializeCurrentPage().catch((error) => {
        console.error("Failed to initialize query-run history.", error);
      });
      renderQueryNotificationMenu();
      return;
    }

    restoreLastNotebook();
  });

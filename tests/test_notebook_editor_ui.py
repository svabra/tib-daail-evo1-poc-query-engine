from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = REPO_ROOT / "bdw" / "bit_data_workbench" / "static"


class NotebookEditorUiRegressionTests(unittest.TestCase):
    def test_client_rendered_notebooks_include_editor_expand_control(self) -> None:
        source = (STATIC_ROOT / "js" / "notebook-workspace-markup.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("data-copy-editor-sql", source)
        self.assertIn("data-expand-editor", source)
        self.assertIn("data-compare-editor-sql", source)
        self.assertIn('aria-label="Expand SQL editor"', source)
        self.assertIn('aria-label="Compare"', source)
        self.assertIn('aria-pressed="false"', source)

    def test_exploration_result_storage_controls_have_runtime_wiring(self) -> None:
        markup_source = (
            STATIC_ROOT / "js" / "notebook-workspace-markup.js"
        ).read_text(encoding="utf-8")
        workspace_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "workspace.html"
        ).read_text(encoding="utf-8")
        index_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "index.html"
        ).read_text(encoding="utf-8")
        model_source = (STATIC_ROOT / "js" / "notebook-model.js").read_text(
            encoding="utf-8"
        )
        controller_source = (
            STATIC_ROOT / "js" / "notebook-workspace-controller.js"
        ).read_text(encoding="utf-8")
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        pipeline_source = (
            STATIC_ROOT / "js" / "notebook-stage-pipeline-controller.js"
        ).read_text(encoding="utf-8")
        query_ui_source = (STATIC_ROOT / "js" / "query-ui.js").read_text(encoding="utf-8")
        query_state_source = (
            STATIC_ROOT / "js" / "query-job-state.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        for source in (markup_source, workspace_template):
            self.assertIn("store result set in S3", source)
            self.assertIn("cell-result-storage-path-row", source)
            self.assertIn('data-cell-query-option="duckdb.resultStorage.mode"', source)
            self.assertIn('data-cell-query-option="duckdb.resultStorage.path"', source)
            self.assertIn('title=', source)
            self.assertIn("data-copy-result-storage-virtual", source)
            self.assertIn("data-copy-result-storage-duckdb", source)

        self.assertIn("runtime-info", index_template)
        self.assertIn("normalizeResultStorageOptions", model_source)
        self.assertIn("outputPath: String", model_source)
        self.assertIn("queryOptionInput", controller_source)
        self.assertIn("queryOptionsForCellRoot(cellRoot)", controller_source)
        self.assertIn("proposedResultStorageS3Path", app_source)
        self.assertIn("proposedPipelineStageOutputS3Path", app_source)
        self.assertIn("pipelineResultStorageForCellRoot", app_source)
        self.assertIn("setCellStage(notebookId, cellId, { outputPath", app_source)
        self.assertIn('resultStorage: {\n        mode: resultStorageEnabled ? "on" : "off"', app_source)
        self.assertIn("path: resultStorageEnabled ? resultStoragePathInput?.value || \"\" : \"\"", app_source)
        self.assertIn("syncCellResultStorageState", app_source)
        self.assertIn("pathInput.title =", app_source)
        self.assertIn("copyResultStorageReference", app_source)
        self.assertIn("read_parquet(${sqlStringLiteral", app_source)
        self.assertIn("outputPath: String(node.outputPath || node.plannedOutputPath", pipeline_source)
        self.assertIn("syncResultStorageState(cellRoot)", pipeline_source)
        self.assertIn("queryResultStorageMarkup", query_ui_source)
        self.assertIn("data-result-storage-summary", query_ui_source)
        self.assertIn("resultStorage", query_state_source)
        self.assertIn(".cell-result-storage-option", css_source)
        self.assertIn(".cell-result-storage-path-row", css_source)
        self.assertIn("flex-direction: column;", css_source)
        self.assertGreaterEqual(css_source.count("flex: 0 0 100%;"), 2)
        self.assertIn("width: 100%;", css_source)
        self.assertIn("min-width: min(100%, 520px);", css_source)
        self.assertGreaterEqual(css_source.count("white-space: nowrap;"), 2)
        self.assertIn(".result-storage-summary", css_source)

    def test_editor_expand_control_has_runtime_wiring_and_styles(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        autosize_source = (
            STATIC_ROOT / "js" / "editor-autosize-manager.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("function toggleEditorExpanded", app_source)
        self.assertIn("[data-expand-editor]", app_source)
        self.assertIn('classList.contains("is-editor-expanded")', autosize_source)
        self.assertIn(".editor-expand-button", css_source)
        self.assertIn("isolation: isolate;", css_source)
        self.assertIn("z-index: 20;", css_source)
        self.assertIn("opacity: 0.78;", css_source)
        self.assertIn("pointer-events: auto;", css_source)
        self.assertIn(".editor-shell {\n  position: relative;\n  z-index: 1;", css_source)
        self.assertIn(".editor-frame:hover .editor-source-nav-button", css_source)
        self.assertIn(".editor-frame:focus-within .editor-source-nav-button", css_source)

    def test_sql_compare_button_has_runtime_dialog_and_styles(self) -> None:
        markup_source = (
            STATIC_ROOT / "js" / "notebook-workspace-markup.js"
        ).read_text(encoding="utf-8")
        workspace_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "workspace.html"
        ).read_text(encoding="utf-8")
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        compare_source = (
            STATIC_ROOT / "js" / "query-compare-controller.js"
        ).read_text(encoding="utf-8")
        dialogs_source = (STATIC_ROOT / "js" / "dialogs.js").read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("data-compare-editor-sql", markup_source)
        self.assertIn('aria-label="Compare"', markup_source)
        self.assertIn('title="Compare"', markup_source)
        self.assertIn("data-compare-editor-sql", workspace_template)
        self.assertIn('aria-label="Compare"', workspace_template)
        self.assertIn("ensureQueryCompareDialog", dialogs_source)
        self.assertIn("data-query-compare-dialog", dialogs_source)
        self.assertIn("data-query-compare-target-notebook", dialogs_source)
        self.assertIn("data-query-compare-target-cell", dialogs_source)
        self.assertIn("createQueryCompareController", app_source)
        self.assertIn("queryCompareDiff", compare_source)
        self.assertIn("function open", compare_source)
        self.assertIn("currentEditorSql(editorRoot)", compare_source)
        self.assertIn("[data-compare-editor-sql]", app_source)
        self.assertIn("[data-query-compare-target-notebook]", compare_source)
        self.assertIn("[data-query-compare-target-cell]", compare_source)
        self.assertIn("No other SQL cells are available to compare.", compare_source)
        self.assertIn(".editor-compare-button", css_source)
        self.assertIn(".query-compare-table", css_source)
        self.assertIn(".query-compare-row.is-changed", css_source)
        self.assertIn('.editor-frame[data-editor-language="python"] .editor-compare-button', css_source)

    def test_cell_descriptors_share_action_and_warning_panel_have_wiring(self) -> None:
        markup_source = (
            STATIC_ROOT / "js" / "notebook-workspace-markup.js"
        ).read_text(encoding="utf-8")
        controller_source = (
            STATIC_ROOT / "js" / "notebook-workspace-controller.js"
        ).read_text(encoding="utf-8")
        model_source = (STATIC_ROOT / "js" / "notebook-model.js").read_text(
            encoding="utf-8"
        )
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        dialogs_source = (STATIC_ROOT / "js" / "dialogs.js").read_text(encoding="utf-8")
        query_ui_source = (STATIC_ROOT / "js" / "query-ui.js").read_text(encoding="utf-8")
        query_runs_source = (STATIC_ROOT / "js" / "query-runs-controller.js").read_text(
            encoding="utf-8"
        )
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("Cell processing hints", markup_source)
        self.assertIn("Cell result expectations", markup_source)
        self.assertIn('data-cell-descriptor="processingHints"', markup_source)
        self.assertIn('data-cell-descriptor="resultExpectations"', markup_source)
        self.assertIn("data-cell-descriptor", controller_source)
        self.assertIn("setCellDescriptor", controller_source)
        self.assertIn("processingHints", model_source)
        self.assertIn("resultExpectations", model_source)
        self.assertIn("processingHints: cell.processingHints", app_source)
        self.assertIn("resultExpectations: cell.resultExpectations", app_source)
        self.assertIn(
            "const shouldPreserveNotebookId = metadata.shared === true && !isLocalNotebookId(notebookId);",
            app_source,
        )
        self.assertIn(
            "notebookId: shouldPreserveNotebookId ? notebookId : null",
            app_source,
        )
        self.assertNotIn(
            "notebookId: isSharedNotebookId(notebookId) ? notebookId : null",
            app_source,
        )
        self.assertIn("data-share-notebook", markup_source)
        self.assertIn("openNotebookShareDialog", controller_source)
        self.assertIn("ensureNotebookShareDialog", dialogs_source)
        self.assertIn("data-notebook-share-copy-reference", dialogs_source)
        self.assertIn("Share and get link", dialogs_source)
        self.assertIn("mailto:?subject", app_source)
        self.assertIn("queryWarningsMarkup", query_ui_source)
        self.assertIn("data-query-warnings", query_ui_source)
        self.assertIn("queryMonitorErrorMarkup", query_ui_source)
        self.assertIn("queryMonitorWarningsMarkup", query_ui_source)
        self.assertIn("queryMonitorProgressEventsMarkup", query_ui_source)
        self.assertIn("runWarningsMarkup", query_runs_source)
        self.assertIn(".cell-descriptor-grid", css_source)
        self.assertIn(".result-warning-list", css_source)
        self.assertIn(".query-monitor-error", css_source)
        self.assertIn(".query-monitor-warning-list", css_source)
        self.assertIn(".query-monitor-progress-events", css_source)
        self.assertIn(".query-run-history-warning", css_source)
        self.assertIn(".notebook-share-actions", css_source)

    def test_run_cell_immediately_updates_query_monitor(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        state_source = (STATIC_ROOT / "js" / "query-job-state.js").read_text(
            encoding="utf-8"
        )
        realtime_source = (STATIC_ROOT / "js" / "realtime-controller.js").read_text(
            encoding="utf-8"
        )
        query_runs_source = (STATIC_ROOT / "js" / "query-runs-controller.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("function createLocalQueryJobSnapshot", app_source)
        self.assertIn("function applyLocalQueryJobSnapshot", app_source)
        self.assertIn("function failLocalQueryJobSnapshot", app_source)
        self.assertIn("function renderLocalQueryProgress", app_source)
        self.assertIn("function trackLocalQueryJobSnapshot", app_source)
        self.assertIn("const shouldRefreshSidebarDuringStartup = !(", app_source)
        self.assertIn("queryWorkbenchEntryPageRoot()", app_source)
        self.assertIn('metadata.pipelineMode === "pipeline"', app_source)
        self.assertIn('[data-notebook-meta][data-default-pipeline-mode="pipeline"]', app_source)
        self.assertIn("Run Cell was clicked", app_source)
        self.assertIn("renderLocalQueryProgress(cellRoot, { cellId, notebookId, workspaceRoot, snapshot: clientSnapshot });", app_source)
        self.assertIn("renderLocalQueryProgress(cellRoot, { cellId, notebookId, workspaceRoot, snapshot: displaySnapshot });", app_source)
        self.assertIn("trackLocalQueryJobSnapshot(displaySnapshot, { removeJobIds: [clientJobId] });", app_source)
        self.assertIn("queryRunsController.refreshForQueryJobsSnapshot(nextSnapshot);", app_source)
        self.assertIn("The backend rejected the query job request with HTTP", app_source)
        self.assertIn('formData.set("clientJobId", clientJobId);', app_source)
        self.assertNotIn("applyLocalQueryJobSnapshot(displaySnapshot", app_source)
        self.assertIn("let queryJobsReconcileHandle = null;", app_source)
        self.assertIn("function scheduleQueryJobsReconciliation", app_source)
        self.assertIn("function refreshQueryJobsForReconciliation", app_source)
        self.assertIn("queryJobsSnapshot.some((job) => queryJobIsRunning(job))", app_source)
        self.assertIn("await loadQueryJobsState();", app_source)
        self.assertIn("syncQueryJobsReconciliation();", app_source)
        self.assertNotIn("sourceValidation blocked the query before backend submission", app_source)
        self.assertIn("progressEvents: Array.isArray(job.progressEvents)", state_source)
        self.assertIn("canCancel: job.canCancel !== false", state_source)
        self.assertIn("const canCancel = queryJobIsRunning(job) && job?.canCancel !== false;", realtime_source)
        self.assertIn("let latestQueryJobsSnapshot = { jobs: [] };", query_runs_source)
        self.assertIn("function liveRunsForRoot", query_runs_source)
        self.assertIn("function runsForRender", query_runs_source)
        self.assertIn("queryJobId", query_runs_source)
        self.assertIn("realQueryJobIds", query_runs_source)
        self.assertIn("renderList(root, root._bdwQueryRunsPayload || { available: true, runs: [] });", query_runs_source)

    def test_query_monitor_uses_bounded_sql_preview(self) -> None:
        query_ui_source = (STATIC_ROOT / "js" / "query-ui.js").read_text(encoding="utf-8")

        self.assertIn("const queryMonitorSqlPreviewMaxChars = 480;", query_ui_source)
        self.assertIn("function queryMonitorSqlPreview(sql)", query_ui_source)
        self.assertIn('replace(/\\s+/g, " ").trim()', query_ui_source)
        self.assertIn("... [SQL truncated]", query_ui_source)
        self.assertIn("queryMonitorSqlPreview(job.sql)", query_ui_source)
        self.assertNotIn('<p class="query-monitor-sql">${escapeHtml(job.sql)}</p>', query_ui_source)

    def test_notebook_sidebar_defers_heavy_source_tree(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        source_tree_source = (STATIC_ROOT / "js" / "source-tree-controller.js").read_text(
            encoding="utf-8"
        )
        sidebar_refresh_source = (
            STATIC_ROOT / "js" / "sidebar-refresh-controller.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")
        sidebar_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "sidebar.html"
        ).read_text(encoding="utf-8")
        notebook_tree_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "notebook_tree_node.html"
        ).read_text(encoding="utf-8")
        notebook_model_source = (STATIC_ROOT / "js" / "notebook-model.js").read_text(
            encoding="utf-8"
        )
        pipeline_source = (
            STATIC_ROOT / "js" / "notebook-stage-pipeline-controller.js"
        ).read_text(encoding="utf-8")
        router_source = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "web" / "router.py"
        ).read_text(encoding="utf-8")

        self.assertIn("defer_sidebar_source_tree = workspace_mode == \"notebook\"", router_source)
        self.assertIn("defer_sidebar_notebook_tree = workspace_mode == \"notebook\" and shell_sidebar_hidden", router_source)
        self.assertIn('source_tree: str = Query(default="deferred")', router_source)
        self.assertIn('notebook_tree: str = Query(default="full")', router_source)
        self.assertIn('source_tree != "full"', router_source)
        self.assertIn('notebook_tree != "full"', router_source)
        self.assertIn("data-deferred-source-tree", sidebar_template)
        self.assertIn("data-deferred-notebook-tree", sidebar_template)
        self.assertIn("Open Data Sources to load source objects.", sidebar_template)
        self.assertIn("Open navigation to load notebooks.", sidebar_template)
        self.assertIn("{% set notebook_browser_payloads = false %}", sidebar_template)
        self.assertIn("data-default-notebook-payloads-deferred", notebook_tree_template)
        self.assertIn("payloadsDeferred", notebook_model_source)
        self.assertIn("!defaults.payloadsDeferred", app_source)
        self.assertIn("function loadDeferredSidebarSourceTree", source_tree_source)
        self.assertIn('sourceTree: "full"', source_tree_source)
        self.assertIn("loadDeferredSidebarSourceTree().catch", app_source)
        self.assertIn("function loadDeferredSidebarNotebookTree", app_source)
        self.assertIn("loadDeferredSidebarNotebookTree().catch", app_source)
        self.assertIn("function shouldRefreshSidebarForMaterializedOutputs", pipeline_source)
        self.assertIn("const hadPreviousSignature = Boolean(materializedOutputSignature);", pipeline_source)
        self.assertIn("hadPreviousSignature && shouldRefreshSidebarForMaterializedOutputs()", pipeline_source)
        self.assertIn("&source_tree=${sourceTreeMode}", sidebar_refresh_source)
        self.assertIn(".source-tree-deferred", css_source)
        self.assertIn(".notebook-tree-deferred", css_source)

    def test_local_workspace_sql_prepare_skips_indexeddb_without_local_references(self) -> None:
        bridge_source = (
            STATIC_ROOT / "js" / "local-workspace-query-bridge.js"
        ).read_text(encoding="utf-8")

        self.assertIn("const logicalAliases = localWorkspaceAliasesInText(rewrittenSql);", bridge_source)
        self.assertIn("const logicalRelations = localWorkspaceRelationsInText(rewrittenSql);", bridge_source)
        self.assertIn("if (!logicalAliases.length && !logicalRelations.length)", bridge_source)
        self.assertIn("return {\n        sql: rewrittenSql,\n        synchronizedSources,", bridge_source)
        self.assertIn(
            "const aliasIndex = logicalAliases.length ? await localWorkspaceAliasIndex() : new Map();",
            bridge_source,
        )

    def test_status_colors_match_runtime_semantics(self) -> None:
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")
        query_ui_source = (STATIC_ROOT / "js" / "query-ui.js").read_text(encoding="utf-8")
        query_state_source = (STATIC_ROOT / "js" / "query-job-state.js").read_text(
            encoding="utf-8"
        )
        query_runs_source = (STATIC_ROOT / "js" / "query-runs-controller.js").read_text(
            encoding="utf-8"
        )
        ingestion_ui_source = (STATIC_ROOT / "js" / "ingestion-ui.js").read_text(
            encoding="utf-8"
        )
        download_jobs_source = (
            STATIC_ROOT / "js" / "download-jobs-controller.js"
        ).read_text(encoding="utf-8")
        s3_delete_jobs_source = (
            STATIC_ROOT / "js" / "s3-delete-jobs-controller.js"
        ).read_text(encoding="utf-8")
        pipeline_source = (
            STATIC_ROOT / "js" / "notebook-stage-pipeline-controller.js"
        ).read_text(encoding="utf-8")

        self.assertIn("--status-blue: #0b4479;", css_source)
        self.assertIn("--status-green:", css_source)
        self.assertIn("--status-orange:", css_source)
        self.assertIn("--status-red:", css_source)
        self.assertIn(".query-monitor-item-running .query-monitor-status-badge", css_source)
        self.assertIn(".query-monitor-item-queued .query-monitor-status-badge", css_source)
        self.assertIn(".query-monitor-item-completed .query-monitor-status-badge", css_source)
        self.assertIn(".query-monitor-item-warning .query-monitor-status-badge", css_source)
        self.assertIn(".query-monitor-item-failed .query-monitor-status-badge", css_source)
        self.assertIn(".query-monitor-item-aborted .query-monitor-status-badge", css_source)
        self.assertIn(".query-monitor-item-incomplete .query-monitor-status-badge", css_source)
        self.assertIn(".query-run-history-status.is-running", css_source)
        self.assertIn(".query-run-history-status.is-cancelled", css_source)
        self.assertIn(".query-run-history-status.is-canceled", css_source)
        self.assertIn(".query-run-history-status.is-aborted", css_source)
        self.assertIn(".query-run-history-status.is-incomplete", css_source)
        self.assertIn(".topbar-notification-item-status.is-running", css_source)
        self.assertIn(".topbar-notification-item-status.is-cancelling", css_source)
        self.assertIn(".topbar-notification-item-status.is-failed", css_source)
        self.assertIn(".ingestion-job-status.is-running", css_source)
        self.assertIn(".ingestion-job-status.is-failed", css_source)
        self.assertIn(".download-job-status-cancelled", css_source)
        self.assertIn(".prepared-download-indicator.is-running", css_source)
        self.assertIn(".pipeline-node-incomplete .pipeline-node-rect", css_source)
        self.assertIn(".pipeline-status-incomplete", css_source)
        self.assertIn(".pipeline-stage-action-button.is-running", css_source)
        self.assertIn("background: var(--status-blue-bg);", css_source)
        self.assertIn("color: var(--status-blue);", css_source)
        self.assertIn("background: var(--status-green-bg);", css_source)
        self.assertIn("background: var(--status-orange-bg);", css_source)
        self.assertIn("background: var(--status-red-bg);", css_source)
        self.assertNotIn(
            ".query-run-history-status.is-running {\n  background: rgba(170, 37, 20",
            css_source,
        )
        self.assertNotIn(
            ".prepared-download-indicator.is-running {\n  animation: prepared-download-blink 1.1s ease-in-out infinite;\n  background: rgba(214, 126, 116",
            css_source,
        )

        self.assertIn("is-${escapeHtml(statusClass)}", query_ui_source)
        self.assertIn("is-${escapeHtml(statusClass)}", ingestion_ui_source)
        self.assertIn("is-${escapeHtml(statusClass)}", download_jobs_source)
        self.assertIn("is-${escapeHtml(", s3_delete_jobs_source)
        self.assertIn('"completed", "failed", "cancelled", "canceled", "aborted", "incomplete", "warning", "warned"', query_runs_source)
        self.assertIn('case "canceled":', query_state_source)
        self.assertIn('case "aborted":', query_state_source)
        self.assertIn('case "incomplete":', query_state_source)
        self.assertIn('const failedStageStatuses = new Set(["failed", "cancelled", "canceled", "aborted", "incomplete"]);', pipeline_source)
        self.assertIn("statusIsFailure(status)", pipeline_source)

    def test_pipeline_runs_announce_to_message_centre(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        realtime_source = (
            STATIC_ROOT / "js" / "realtime-controller.js"
        ).read_text(encoding="utf-8")
        ingestion_source = (
            STATIC_ROOT / "js" / "ingestion-controller.js"
        ).read_text(encoding="utf-8")
        pipeline_source = (
            STATIC_ROOT / "js" / "notebook-stage-pipeline-controller.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("onPipelineNotificationStateChanged: (snapshot) => {", app_source)
        self.assertIn("queryRunsController.refreshForMaterializedStagesSnapshot(snapshot);", app_source)
        self.assertIn("getPipelineNotificationItems", app_source)
        self.assertIn("getPipelineNotificationSummary", app_source)
        self.assertIn('status === "aborted"', app_source)
        self.assertIn('status === "incomplete"', app_source)
        self.assertIn('status === "skipped"', app_source)
        self.assertIn('case "materialized-stages":', app_source)
        self.assertIn("notebookStagePipelineController.applyRealtimeState(snapshot);", app_source)

        self.assertIn("getPipelineNotificationItems = () => []", ingestion_source)
        self.assertIn("const pipelineNotifications = getPipelineNotificationItems();", ingestion_source)
        self.assertIn("...pipelineNotifications", ingestion_source)

        self.assertIn("getPipelineNotificationItems = () => []", realtime_source)
        self.assertIn("getPipelineNotificationSummary = () => ({ version: null, runningCount: 0, totalCount: 0 })", realtime_source)
        self.assertIn('key.startsWith("pipeline:")', realtime_source)
        self.assertIn("pipelineNotificationSummary?.runningCount", realtime_source)
        self.assertIn('notificationItemKey("pipeline", model)', pipeline_source)

        self.assertIn("let materializedStagesState = {", pipeline_source)
        self.assertIn("function pipelineNotificationModels()", pipeline_source)
        self.assertIn("function pipelineNotificationItems", pipeline_source)
        self.assertIn("function pipelineNotificationSummary", pipeline_source)
        self.assertIn("topbar-notification-item-pipeline", pipeline_source)
        self.assertIn("data-open-query-notebook", pipeline_source)
        self.assertIn("Pipeline completed", pipeline_source)
        self.assertIn("Pipeline failed", pipeline_source)
        self.assertIn("Pipeline cancelled", pipeline_source)
        self.assertIn("Pipeline warning", pipeline_source)
        self.assertIn("Pipeline incomplete", pipeline_source)
        self.assertIn("Current: ${currentStage}", pipeline_source)
        self.assertIn("applyRealtimeState(startSnapshot);", pipeline_source)
        self.assertIn("applyRealtimeState(snapshot);", pipeline_source)
        self.assertIn("onPipelineNotificationStateChanged(materializedStagesState);", pipeline_source)
        self.assertIn(".topbar-notification-item-status.is-cancelling", css_source)
        self.assertIn(".topbar-notification-item-status-notice.is-cancelling", css_source)

    def test_pipeline_stage_runs_refresh_cell_query_monitoring(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        query_runs_source = (
            STATIC_ROOT / "js" / "query-runs-controller.js"
        ).read_text(encoding="utf-8")

        self.assertIn("queryRunsController.refreshForMaterializedStagesSnapshot(snapshot);", app_source)
        self.assertIn("let latestPipelineStageSnapshot = { records: [], activeRuns: [] };", query_runs_source)
        self.assertIn("function latestPipelineStageRecords()", query_runs_source)
        self.assertIn("function liveRunFromPipelineStageRecord(record)", query_runs_source)
        self.assertIn("function pipelineStageRunsForRoot(root)", query_runs_source)
        self.assertIn("const realQueryJobIds = new Set(", query_runs_source)
        self.assertIn("const pipelineStageRuns = pipelineStageRunsForRoot(root).filter", query_runs_source)
        self.assertIn("queryJobId", query_runs_source)
        self.assertIn("async function loadInitialMonitorSnapshots()", query_runs_source)
        self.assertIn('fetchJsonOrThrow("/api/query-jobs")', query_runs_source)
        self.assertIn('fetchJsonOrThrow("/api/materialized-stages/state")', query_runs_source)
        self.assertIn("await loadInitialMonitorSnapshots();", query_runs_source)
        self.assertIn("let initialMonitorSnapshotsLoaded = false;", query_runs_source)
        self.assertIn("initialMonitorSnapshotsLoaded = true;", query_runs_source)
        self.assertIn("if (!initialMonitorSnapshotsLoaded)", query_runs_source)
        self.assertIn('jobId: `pipeline-stage:${record?.runId || ""}:${record?.stageId || ""}`', query_runs_source)
        self.assertIn('source: "pipeline-stage"', query_runs_source)
        self.assertIn("Pipeline stage ${stageTitle}.", query_runs_source)
        self.assertIn("function refreshForMaterializedStagesSnapshot(snapshot)", query_runs_source)
        self.assertIn("refreshForMaterializedStagesSnapshot,", query_runs_source)

    def test_primary_run_button_is_visible_without_hover(self) -> None:
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertNotIn(".cell-actions .run-button,\n.cell-actions .explain-button", css_source)
        self.assertNotIn(".workspace-cell:hover .cell-actions .run-button", css_source)
        self.assertIn(".cell-actions .explain-button {\n  opacity: 0;", css_source)
        self.assertIn(".run-button {", css_source)
        self.assertIn("background: var(--accent);", css_source)

    def test_shared_notebook_delete_marks_pending_state_in_ui(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("const notebookDeletionInProgressIds = new Set();", app_source)
        self.assertIn("function setNotebookDeletionInProgress", app_source)
        self.assertIn("function clearSharedNotebookPendingWork", app_source)
        self.assertIn("setNotebookDeletionInProgress(notebookId, true);", app_source)
        self.assertIn("setNotebookDeletionInProgress(notebookId, false);", app_source)
        self.assertIn("notebookDeletionInProgress(notebookId)", app_source)
        self.assertIn("link.classList.toggle(\"is-deleting\", deleteInProgress);", app_source)
        self.assertIn("link.draggable = Boolean(metadata.canEdit && !deleteInProgress);", app_source)
        self.assertIn("sharedBadge.textContent = deleteInProgress", app_source)
        self.assertIn("DELETION IN PROGRESS", app_source)
        self.assertIn("dataset.deleteInProgress", app_source)
        self.assertIn("Notebook deletion is in progress.", app_source)
        self.assertIn("clearSharedNotebookPendingWork(notebookId);", app_source)
        self.assertIn(".notebook-link.is-deleting", css_source)
        self.assertIn('.notebook-sharing-pill[data-tone="deleting"]', css_source)

    def test_sql_view_toggle_has_markup_runtime_wiring_and_styles(self) -> None:
        markup_source = (
            STATIC_ROOT / "js" / "notebook-workspace-markup.js"
        ).read_text(encoding="utf-8")
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        validation_source = (
            STATIC_ROOT / "js" / "query-source-validation-controller.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")
        workspace_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "workspace.html"
        ).read_text(encoding="utf-8")

        self.assertIn("data-editor-sql-view-toggle", markup_source)
        self.assertIn('data-editor-sql-view="duckdb"', markup_source)
        self.assertIn("data-duckdb-sql-panel", markup_source)
        self.assertIn('<textarea class="editor-duckdb-sql-panel"', markup_source)
        self.assertNotIn('<pre class="editor-duckdb-sql-panel"', markup_source)
        self.assertIn("data-editor-sql-view-toggle", workspace_template)
        self.assertIn("data-duckdb-sql-panel", workspace_template)
        self.assertIn("class=\"editor-duckdb-sql-panel\"", workspace_template)
        self.assertNotIn('<pre class="editor-duckdb-sql-panel"', workspace_template)
        self.assertIn("/api/query-sql/prepare", app_source)
        self.assertIn("prepareDuckdbSqlForCell", app_source)
        self.assertIn("stageSqlPreviewPayloadForCell", app_source)
        self.assertIn("stage: stagePayload", app_source)
        self.assertIn("stagePayloadForCell", app_source)
        self.assertIn("currentVisibleEditorSql", app_source)
        self.assertIn("invalidatePreparedSqlViewForCell", app_source)
        self.assertIn("function duckdbSqlToVirtualSql", app_source)
        self.assertIn("function setVirtualEditorSql", app_source)
        self.assertIn("function syncVirtualSqlFromDuckdbPanel", app_source)
        self.assertIn("function syncVisibleDuckdbSqlToVirtual", app_source)
        self.assertIn("function duckdbSqlPanelIsPreparing", app_source)
        self.assertIn('panel?.getAttribute?.("aria-busy") === "true"', app_source)
        self.assertIn("syncVisibleDuckdbSqlToVirtual(cellRoot);", app_source)
        self.assertIn("panel.value = \"Preparing DuckDB SQL...\";", app_source)
        self.assertIn("panel.value = text;", app_source)
        self.assertIn("duckdbSqlToVirtualSql(duckdbSql)", app_source)
        self.assertIn("virtualS3ReferenceForPath(`s3://${bucket}/${key}`)", app_source)
        self.assertIn("notebookId", validation_source)
        self.assertIn(".editor-sql-view-toggle", css_source)
        self.assertIn(".editor-frame:hover .editor-sql-view-toggle", css_source)
        self.assertIn(".editor-frame:focus-within .editor-sql-view-toggle", css_source)
        self.assertNotIn(".editor-frame.is-duckdb-sql-view .editor-sql-view-toggle", css_source)
        self.assertNotIn(".editor-frame[data-editor-language=\"sql\"] textarea", css_source)
        self.assertIn(".editor-duckdb-sql-panel", css_source)
        self.assertIn(".editor-frame.editor-ready textarea[data-editor-source]", css_source)
        self.assertIn(".editor-frame.is-duckdb-sql-view textarea[data-editor-source]", css_source)
        self.assertNotIn(".editor-frame.editor-ready textarea {\n", css_source)
        self.assertNotIn(".editor-frame.is-duckdb-sql-view textarea,\n", css_source)
        self.assertIn("resize: none;", css_source)
        self.assertIn("padding: 10px 12px;", css_source)

    def test_duckdb_parquet_hive_query_option_has_markup_and_payload_wiring(self) -> None:
        markup_source = (
            STATIC_ROOT / "js" / "notebook-workspace-markup.js"
        ).read_text(encoding="utf-8")
        controller_source = (
            STATIC_ROOT / "js" / "notebook-workspace-controller.js"
        ).read_text(encoding="utf-8")
        model_source = (STATIC_ROOT / "js" / "notebook-model.js").read_text(
            encoding="utf-8"
        )
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        realtime_source = (
            STATIC_ROOT / "js" / "realtime-controller.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")
        workspace_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "workspace.html"
        ).read_text(encoding="utf-8")

        self.assertIn('data-cell-query-option="duckdb.parquetHivePartitioning"', markup_source)
        self.assertIn("Auto uses source discovery defaults", markup_source)
        self.assertIn("data-cell-query-option", controller_source)
        self.assertIn("setCellQueryOptions", controller_source)
        self.assertIn("normalizeCellQueryOptions", model_source)
        self.assertIn("queryOptionsForCellRoot", app_source)
        self.assertIn('formData.set("queryOptions"', app_source)
        self.assertIn("queryOptions: queryOptionsForCellRoot(cellRoot)", app_source)
        self.assertIn(".cell-duckdb-option", css_source)
        self.assertIn('data-cell-query-option="duckdb.parquetHivePartitioning"', workspace_template)

    def test_source_existence_validation_option_has_markup_and_skip_wiring(self) -> None:
        markup_source = (
            STATIC_ROOT / "js" / "notebook-workspace-markup.js"
        ).read_text(encoding="utf-8")
        controller_source = (
            STATIC_ROOT / "js" / "notebook-workspace-controller.js"
        ).read_text(encoding="utf-8")
        model_source = (STATIC_ROOT / "js" / "notebook-model.js").read_text(
            encoding="utf-8"
        )
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        validation_source = (
            STATIC_ROOT / "js" / "query-source-validation-controller.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")
        workspace_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "workspace.html"
        ).read_text(encoding="utf-8")

        self.assertIn('data-cell-query-option="validation.sourceExistence"', markup_source)
        self.assertIn("data-source-check-switch", markup_source)
        self.assertIn("Check sources", markup_source)
        self.assertIn('data-cell-query-option="validation.sourceExistence"', workspace_template)
        self.assertIn("data-source-check-switch", workspace_template)
        self.assertIn("normalizeSourceExistenceValidationOption", model_source)
        self.assertIn('return normalized === "on" ? "on" : "off";', model_source)
        self.assertIn("sourceExistence", model_source)
        self.assertIn("sourceExistenceValidationEnabledForCell", app_source)
        self.assertIn("sourceExistenceValidationEnabledForCell = () => false", validation_source)
        self.assertIn("cellSourceExistenceValidationEnabled", app_source)
        self.assertIn("refreshQuerySourceValidationForCell", controller_source)
        self.assertIn("data-source-check-state-label", controller_source)
        self.assertIn("skippedValidationResult", validation_source)
        self.assertIn("Source existence check skipped", validation_source)
        self.assertIn("!sourceExistenceValidationEnabled(cellRoot)", validation_source)
        self.assertIn(".cell-source-check-switch", css_source)
        self.assertIn(".query-source-validation.is-skipped", css_source)

    def test_cache_hydration_option_has_markup_tooltips_modal_and_status_styles(self) -> None:
        markup_source = (
            STATIC_ROOT / "js" / "notebook-workspace-markup.js"
        ).read_text(encoding="utf-8")
        controller_source = (
            STATIC_ROOT / "js" / "notebook-workspace-controller.js"
        ).read_text(encoding="utf-8")
        model_source = (STATIC_ROOT / "js" / "notebook-model.js").read_text(
            encoding="utf-8"
        )
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        realtime_source = (
            STATIC_ROOT / "js" / "realtime-controller.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn('data-cell-query-option="duckdb.cacheHydration.mode"', markup_source)
        self.assertIn('role="switch"', markup_source)
        self.assertIn('aria-checked="${hydrateCache ? "true" : "false"}"', markup_source)
        self.assertIn("data-cache-hydration-state-label", markup_source)
        self.assertIn("Hydrate cache", markup_source)
        self.assertIn("temporary local DuckDB table", markup_source)
        self.assertIn("ART indexes speed up equality lookups", app_source)
        self.assertIn("openCacheHydrationDialog", controller_source)
        self.assertIn("applyCellCacheHydrationToggle", controller_source)
        self.assertIn("/api/query-cache/delete", app_source)
        self.assertIn("Building runtime cache", app_source)
        self.assertIn("aria-busy", app_source)
        self.assertIn("Runtime table", app_source)
        self.assertIn("Temporary storage", app_source)
        self.assertIn("Runtime cache used", (STATIC_ROOT / "js" / "query-insights.js").read_text(encoding="utf-8"))
        self.assertIn("refreshCellCacheHydrationStatus", controller_source)
        self.assertIn("normalizeCacheHydrationOptions", model_source)
        self.assertIn("/api/query-cache/preview", app_source)
        self.assertIn("Cache hydration plan", app_source)
        self.assertIn("async function cacheHydrationPayloadForCellRoot", app_source)
        self.assertIn("validateLocalWorkspaceAliases(sql)", app_source)
        self.assertIn("localRelations,", app_source)
        self.assertIn("body: JSON.stringify(payload)", app_source)
        self.assertIn('window.addEventListener("focus"', app_source)
        self.assertIn("syncCellCacheHydrationJobState", app_source)
        self.assertIn("syncCellCacheHydrationJobState", realtime_source)
        self.assertIn("Expire cache", app_source)
        self.assertIn("Rehydrate now", app_source)
        self.assertIn("Refreshing...", app_source)
        self.assertIn("Rehydrated", app_source)
        self.assertIn("source data changed", app_source)
        self.assertIn('[data-cache-hydration-state="hit"]', css_source)
        self.assertIn('[data-cache-hydration-state="expired"]', css_source)
        self.assertIn('[data-cache-hydration-state="rehydrating"]', css_source)
        self.assertIn('[data-cache-hydration-state="deleting"]', css_source)
        self.assertIn('[data-cache-hydration-state="error"]', css_source)
        self.assertIn(".cell-cache-hydration-switch", css_source)
        self.assertIn(".runtime-cache-pill", css_source)

    def test_notebook_stage_pipeline_has_mode_graph_table_and_actions(self) -> None:
        markup_source = (
            STATIC_ROOT / "js" / "notebook-workspace-markup.js"
        ).read_text(encoding="utf-8")
        controller_source = (
            STATIC_ROOT / "js" / "notebook-stage-pipeline-controller.js"
        ).read_text(encoding="utf-8")
        model_source = (STATIC_ROOT / "js" / "notebook-model.js").read_text(
            encoding="utf-8"
        )
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        app_tooltip_source = (STATIC_ROOT / "js" / "app-tooltip-controller.js").read_text(
            encoding="utf-8"
        )
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")
        workspace_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "workspace.html"
        ).read_text(encoding="utf-8")
        notebook_tree_source = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "partials" / "notebook_tree_node.html"
        ).read_text(encoding="utf-8")

        self.assertIn("workspace-header-toggle-row", markup_source)
        self.assertIn("workspace-sharing-toggle notebook-mode-toggle", markup_source)
        self.assertIn("data-notebook-mode-toggle", markup_source)
        self.assertIn("data-notebook-mode-toggle-label", markup_source)
        self.assertIn("data-notebook-mode-toggle-detail", markup_source)
        self.assertIn("Notebook mode: Exploration", markup_source)
        self.assertIn("Notebook mode: Pipeline", controller_source)
        self.assertIn("Pipeline Mode", markup_source)
        self.assertIn("Exploration Mode", markup_source)
        self.assertIn("dependency-aware runs", markup_source)
        self.assertIn("keeps cells independent", markup_source)
        self.assertIn("links SQL cells", markup_source)
        self.assertIn("data-notebook-pipeline-graph", markup_source)
        self.assertIn("data-notebook-pipeline-table", markup_source)
        self.assertIn("data-notebook-pipeline-total-duration", markup_source)
        self.assertIn("data-notebook-pipeline-table-duration-total", markup_source)
        self.assertIn("data-notebook-pipeline-running-indicator", markup_source)
        self.assertIn("data-cancel-notebook-pipeline", markup_source)
        self.assertIn("<th>Duration</th>", markup_source)
        self.assertIn("data-cell-stage-title-input", markup_source)
        self.assertIn("data-cell-stage-description-input", markup_source)
        self.assertIn("data-navigate-cell-source", markup_source)
        self.assertIn("Navigate to source object", markup_source)
        self.assertIn("data-navigate-cell-source", workspace_template)
        self.assertIn("normalizeCellStage", model_source)
        self.assertIn("normalizeNotebookPipelineMode", model_source)
        self.assertIn("createNotebookStagePipelineController", app_source)
        self.assertIn("validatePipelineStageAliases", app_source)
        self.assertIn("/api/query-sql/prepare", app_source)
        self.assertIn("normalizeSourceNavigationObjects", app_source)
        self.assertIn("navigateCellSourceObject", app_source)
        self.assertIn("navigateToPreparedSourceObject", app_source)
        self.assertIn("data-navigate-cell-source-choice", app_source)
        self.assertIn("handleRunCellButton", app_source)
        self.assertIn("handleQueryFormSubmit", app_source)
        self.assertIn('case "materialized-stages"', app_source)
        self.assertIn("materializedStagesVersion", app_source)
        self.assertIn("/api/materialized-stages/graph", controller_source)
        self.assertIn("/api/materialized-stages/pipeline/run", controller_source)
        self.assertIn("/api/materialized-stages/pipeline/cancel", controller_source)
        self.assertIn("/api/materialized-stages/state", controller_source)
        self.assertIn("validateStageAliasesForCell", controller_source)
        self.assertIn("prepareQuerySqlForCell", controller_source)
        self.assertIn("renderGraphPlaceholder", controller_source)
        self.assertIn("graphHasRenderedContent", controller_source)
        self.assertIn("setGraphRefreshPending", controller_source)
        self.assertIn("renderGraphRefreshError", controller_source)
        self.assertIn("Loading pipeline graph...", controller_source)
        self.assertIn("Failed to initialize notebook pipeline graph.", controller_source)
        self.assertIn("materializeCellStageThenRun", controller_source)
        self.assertIn("Abort pipeline", controller_source)
        self.assertIn("Aborting", controller_source)
        self.assertIn("Pipeline run failed", controller_source)
        self.assertNotIn("Pipeline run is still running", controller_source)
        self.assertNotIn("Stage run is still running", controller_source)
        self.assertIn("Pipeline abort failed", controller_source)
        self.assertIn("stageDurationCopy", controller_source)
        self.assertIn("pipelineTotalDurationCopy", controller_source)
        self.assertIn("pipeline-table-duration", controller_source)
        self.assertIn("pipeline-spinner", controller_source)
        self.assertIn("pipelinePaths: normalizePipelinePaths(metadata.pipelinePaths)", controller_source)
        self.assertIn("graphPaths(graph)", controller_source)
        self.assertIn("prioritySummaryCopy", controller_source)
        self.assertIn("priorityRankBadge(priorityPath", controller_source)
        self.assertIn("data-pipeline-priority-paths", controller_source)
        self.assertIn("data-pipeline-path-label-input", controller_source)
        self.assertIn("data-pipeline-path-move", controller_source)
        self.assertIn("data-pipeline-priority-reset", controller_source)
        self.assertIn("setNotebookPipelinePaths(notebookId", controller_source)
        self.assertIn("syncNow: true", controller_source)
        self.assertIn("syncSharedNotebookNow(notebookId)", app_source)
        self.assertIn("data-run-pipeline-stage", controller_source)
        self.assertIn("data-run-pipeline-from-stage", controller_source)
        self.assertIn("data-cancel-pipeline-stage", controller_source)
        self.assertIn("data-pipeline-stage-waiting", controller_source)
        self.assertIn("Run pipeline from this stage", controller_source)
        self.assertIn("startStageId", controller_source)
        self.assertIn("Waiting for earlier stages to finish", controller_source)
        self.assertIn('cellOrdinalLabel(pipelineMode, cellLanguage, index)', markup_source)
        self.assertIn('pipelineMode === "pipeline" && cellLanguage === "sql" ? "Stage" : "Cell"', markup_source)
        self.assertIn('label.textContent = `${labelPrefix} ${index + 1}`;', app_source)
        self.assertIn('const labelPrefix = pipelineMode === "pipeline" && cellLanguage === "sql" ? "Stage" : "Cell";', app_source)
        self.assertIn('runCellButtonLabel(pipelineMode, cellLanguage)', markup_source)
        self.assertIn('pipelineMode === "pipeline" && cellLanguage === "sql" ? "Run Stage" : "Run Cell"', markup_source)
        self.assertIn("runCellButtonLabelForCell(cellRoot)", controller_source)
        self.assertIn("cellOrdinalLabelForCell(cellRoot, index)", controller_source)
        self.assertIn("refreshCellLabels(workspaceRoot)", controller_source)
        self.assertIn("refreshRunCellButtonLabels(workspaceRoot)", controller_source)
        self.assertIn('button.textContent = busy ? "Run Stage" : runCellButtonLabelForCell(cellRoot);', controller_source)
        self.assertIn('("Stage " if notebook_pipeline_enabled and cell_language == \'sql\' else "Cell ") ~ loop.index', workspace_template)
        self.assertIn('"Run Stage" if notebook_pipeline_enabled and cell_language == \'sql\' else "Run Cell"', workspace_template)

        self.assertIn("stageActionButton(node", controller_source)
        self.assertIn("stageActionButtons(node", controller_source)
        self.assertIn("createAppTooltipController", app_source)
        self.assertIn("createAppTooltipController().install();", app_source)
        self.assertIn('[data-app-tooltip], [data-pipeline-tooltip], [title]', app_tooltip_source)
        self.assertIn("suppressNativeTitle", app_tooltip_source)
        self.assertIn("restoreNativeTitle", app_tooltip_source)
        self.assertIn("leavingActiveTarget", app_tooltip_source)
        self.assertIn("activeTarget.contains(event.target)", app_tooltip_source)
        self.assertIn('data-pipeline-tooltip="${escapeHtml(copy)}"', controller_source)
        self.assertIn(
            'data-pipeline-tooltip="Runs all stages in dependency order; priority paths run first when branches fork."',
            markup_source,
        )
        self.assertIn(
            'data-pipeline-tooltip="Runs all stages in dependency order; priority paths run first when branches fork."',
            workspace_template,
        )
        self.assertIn('data-pipeline-tooltip="Abort the active pipeline run"', markup_source)
        self.assertIn('data-pipeline-tooltip="Abort the active pipeline run"', workspace_template)
        self.assertIn("activeRunsForGraph(snapshot)", controller_source)
        self.assertIn("graphHasActiveWholePipelineRun(graph)", controller_source)
        self.assertIn("runCoversWholePipeline(run, graph)", controller_source)
        self.assertIn("data-notebook-pipeline-running-indicator", controller_source)
        self.assertIn("wholePipelineRunning: !normalizedStartStageId", controller_source)
        self.assertIn("pipeline-stage-row-${rowStatus}", controller_source)
        self.assertIn('action: "navigate-target"', controller_source)
        self.assertIn('label: "Navigate to target data object"', controller_source)
        self.assertIn("closeNotebookExplorer", controller_source)
        self.assertIn("openSourceObjectAncestors", controller_source)
        self.assertIn("flashTargetDataObject", controller_source)
        self.assertIn('action: "copy-path", label: "Copy target path", icon: "copy"', controller_source)
        self.assertIn('action: "publish", label: "Publish data product", icon: "publish"', controller_source)
        self.assertIn('action: "derive", label: "Derive new stage", icon: "derive"', controller_source)
        self.assertIn('action: "fork", label: "Fork new stage", icon: "fork"', controller_source)
        self.assertIn('action: "delete", label: "Delete stage ...", icon: "delete"', controller_source)
        self.assertIn("pipeline-menu-icon", controller_source)
        self.assertIn("showConfirmDialog", controller_source)
        self.assertIn("stageStorageReference(node)", controller_source)
        self.assertIn("sql: `SELECT *\\nFROM ${reference}`", controller_source)
        self.assertNotIn("FROM stage.${node.alias};", controller_source)
        self.assertIn('defaultFirstStageTitle = "my first stage"', controller_source)
        self.assertIn('defaultFirstStageDescription = "This is the stage description"', controller_source)
        self.assertIn("recommendedStageOutputFileName", controller_source)
        self.assertIn("data-cell-stage-output-file-input", controller_source)
        self.assertIn("outputFileName", controller_source)
        self.assertIn("setNotebookPipelineMode(notebookId, nextMode, { rerender: false })", controller_source)
        self.assertIn("updateModeToggle", controller_source)
        self.assertIn('event.target.closest("[data-notebook-mode-toggle]")', controller_source)
        self.assertIn('return "OK";', controller_source)
        self.assertIn("pipeline-status-icon-ok", controller_source)
        self.assertIn("nodeStatusMarker(node)", controller_source)
        self.assertIn("pipeline-node-state-${tone}", controller_source)
        self.assertIn('tone = "ok"', controller_source)
        self.assertIn('tone = "failed"', controller_source)
        self.assertIn("marker-end=\"url(#pipeline-arrowhead)\"", controller_source)
        self.assertIn('width="${width}" height="${height}"', controller_source)
        self.assertIn('refreshSidebar("notebook")', controller_source)
        self.assertIn("workspace-action-menu-panel", controller_source)
        self.assertIn("scrollIntoView", controller_source)
        self.assertNotIn("<title>${escapeHtml(description", controller_source)
        self.assertNotIn('title="Run all pipeline stages in dependency order"', markup_source)
        self.assertNotIn('title="Run all pipeline stages in dependency order"', workspace_template)
        self.assertNotIn('title="Abort the active pipeline run"', markup_source)
        self.assertNotIn('title="Abort the active pipeline run"', workspace_template)
        self.assertIn(".notebook-pipeline-graph-band", css_source)
        self.assertIn(".pipeline-graph-placeholder", css_source)
        self.assertIn(".editor-source-nav-button", css_source)
        self.assertIn(".cell-source-navigation-menu", css_source)
        self.assertIn(".cell-source-navigation-item", css_source)
        self.assertIn(".notebook-pipeline-table", css_source)
        self.assertIn(".notebook-pipeline-title-row", css_source)
        self.assertIn(".notebook-pipeline-running-indicator", css_source)
        self.assertIn(".notebook-pipeline-total-duration", css_source)
        self.assertIn(".pipeline-table-total-duration", css_source)
        self.assertIn("flex: 0 0 190px;", css_source)
        self.assertIn("width: 190px;", css_source)
        self.assertIn('font-feature-settings: "tnum" 1;', css_source)
        self.assertIn(".notebook-pipeline-priority-button", css_source)
        self.assertIn(".pipeline-priority-rank-badge", css_source)
        self.assertIn(".pipeline-priority-popover", css_source)
        self.assertIn(".pipeline-priority-row", css_source)
        self.assertIn(".workspace-header-toggle-row", css_source)
        self.assertIn(".workspace-header-toggle-row-paired", css_source)
        self.assertIn("grid-template-columns: repeat(2, minmax(0, 1fr));", css_source)
        self.assertIn(".notebook-mode-toggle", css_source)
        self.assertIn("width: min(100%, 1180px);", css_source)
        self.assertIn("min-width: 0;", css_source)
        self.assertIn("table-layout: fixed;", css_source)
        self.assertIn("min-width: 980px;", css_source)
        self.assertIn(".cell-stage-title-input", css_source)
        self.assertIn("cell-stage-output-file-input", markup_source)
        self.assertIn("Destination file", markup_source)
        self.assertIn("outputFileName", model_source)
        self.assertIn("width: auto;", css_source)
        self.assertIn(".pipeline-arrowhead", css_source)
        self.assertIn(".pipeline-table-status", css_source)
        self.assertIn(".pipeline-table-duration", css_source)
        self.assertIn(".pipeline-table-run-cell", css_source)
        self.assertIn(".pipeline-stage-action-group", css_source)
        self.assertIn(".pipeline-stage-action-button", css_source)
        self.assertIn(".pipeline-stage-action-button-graph", css_source)
        self.assertIn("flex: 0 0 auto;", css_source)
        self.assertIn("@keyframes pipeline-node-computing-glow", css_source)
        self.assertIn("pipeline-node-glow", controller_source)
        self.assertIn("pipeline-node-running-glow", controller_source)
        self.assertIn("<feGaussianBlur", controller_source)
        self.assertIn(".pipeline-node-running .pipeline-node-glow", css_source)
        self.assertIn("animation: pipeline-node-computing-glow 6s ease-in-out infinite;", css_source)
        self.assertIn("stroke-width: 8;", css_source)
        self.assertIn("box-shadow: inset 4px 0 0 var(--accent);", css_source)
        self.assertIn(".pipeline-stage-row-running > td", css_source)
        self.assertIn(".pipeline-stage-row-running > td:first-child::after", css_source)
        self.assertIn(".pipeline-stage-row-running > td:last-child::after", css_source)
        self.assertIn("@keyframes pipeline-stage-row-computing-glow", css_source)
        self.assertIn("@keyframes pipeline-stage-row-edge-glow", css_source)
        self.assertIn("animation: pipeline-stage-row-computing-glow 6s ease-in-out infinite;", css_source)
        self.assertIn("animation: pipeline-stage-row-edge-glow 6s ease-in-out infinite;", css_source)
        self.assertIn(".pipeline-col-actions", css_source)
        self.assertIn("<colgroup>", markup_source)
        self.assertIn("<colgroup>", workspace_template)
        self.assertIn("justify-content: center;", css_source)
        self.assertIn(".notebook-pipeline-cancel-button", css_source)
        self.assertIn("@keyframes pipeline-spinner-rotate", css_source)
        self.assertIn("border-radius: 3px;", css_source)
        self.assertIn("rx: 3;", css_source)
        self.assertIn(".pipeline-node-state", css_source)
        self.assertIn(".pipeline-node-state-ok", css_source)
        self.assertIn(".pipeline-node-state-attention", css_source)
        self.assertIn(".pipeline-node-state-failed", css_source)
        self.assertIn(".pipeline-node-state-waiting", css_source)
        self.assertIn(".pipeline-stage-action-button.is-waiting", css_source)
        self.assertIn("[data-pipeline-tooltip]::after", css_source)
        self.assertIn(".app-floating-tooltip", css_source)
        self.assertIn(".app-tooltips-ready", css_source)
        self.assertIn("background: rgba(255, 255, 255, 0.7);", css_source)
        self.assertIn("border-radius: 3px;", css_source)
        self.assertIn("padding: 10px 12px;", css_source)
        self.assertIn("width: 40px;", css_source)
        self.assertIn("min-width: 350px;", css_source)
        self.assertIn("min-height: 88px;", css_source)
        self.assertIn("box-sizing: border-box;", css_source)
        self.assertIn("white-space: nowrap;", css_source)
        self.assertIn("max-width: 100%;", css_source)
        self.assertIn("margin-left: auto;", css_source)
        self.assertIn(".pipeline-menu-icon", css_source)
        self.assertIn("grid-template-columns: 18px minmax(0, 1fr);", css_source)
        self.assertIn("color: var(--text);", css_source)
        self.assertIn(".is-pipeline-inspect-flash", css_source)
        self.assertIn(".is-pipeline-target-text-flash", css_source)
        self.assertIn("@keyframes pipeline-target-text-flash", css_source)
        self.assertIn("data-notebook-pipeline-graph", workspace_template)
        self.assertIn("data-notebook-pipeline-total-duration", workspace_template)
        self.assertIn("data-notebook-pipeline-running-indicator", workspace_template)
        self.assertIn("data-pipeline-priority-paths", workspace_template)
        self.assertIn("data-pipeline-priority-summary", workspace_template)
        self.assertIn("data-default-pipeline-paths", workspace_template)
        self.assertIn("data-default-notebook-pipeline-paths", notebook_tree_source)
        self.assertIn("data-notebook-pipeline-table-duration-total", workspace_template)
        self.assertIn("data-cancel-notebook-pipeline", workspace_template)
        self.assertIn("<th>Duration</th>", workspace_template)
        self.assertIn("workspace-header-toggle-row", workspace_template)
        self.assertIn("workspace-header-toggle-row-paired", workspace_template)
        self.assertIn("data-notebook-mode-toggle", workspace_template)
        self.assertIn("data-cell-stage-title-input", workspace_template)
        self.assertIn("data-cell-stage-output-file-input", workspace_template)
        self.assertIn("keeps cells independent", workspace_template)

    def test_pipeline_failure_dialog_uses_preformatted_error_copy(self) -> None:
        dialog_manager_source = (
            STATIC_ROOT / "js" / "dialog-manager.js"
        ).read_text(encoding="utf-8")
        dialogs_source = (STATIC_ROOT / "js" / "dialogs.js").read_text(encoding="utf-8")
        controller_source = (
            STATIC_ROOT / "js" / "notebook-stage-pipeline-controller.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("preformatted = false", dialog_manager_source)
        self.assertIn("normalizedMessageCopy(copy)", dialog_manager_source)
        self.assertIn('pre.className = "modal-preformatted-copy"', dialog_manager_source)
        self.assertIn("copyNode.replaceChildren(pre)", dialog_manager_source)
        self.assertIn("modal-copy-preformatted", dialog_manager_source)
        self.assertIn('<div class="modal-copy" data-message-copy>Done.</div>', dialogs_source)
        self.assertIn("preformatted: true", controller_source)
        self.assertIn(".modal-preformatted-copy", css_source)
        self.assertIn("white-space: pre-wrap;", css_source)
        self.assertIn("overflow-wrap: anywhere;", css_source)
        self.assertIn("max-height: min(52vh, 360px);", css_source)

    def test_pipeline_stage_cell_run_waits_instead_of_failing_on_running_status(self) -> None:
        controller_source = (
            STATIC_ROOT / "js" / "notebook-stage-pipeline-controller.js"
        ).read_text(encoding="utf-8")

        self.assertIn(
            'const activeStageStatuses = new Set(["planned", "queued", "running", "cancelling"]);',
            controller_source,
        )
        self.assertIn("function statusIsTerminalProblem(value)", controller_source)
        self.assertIn("function stageRunErrorMessage(node, status = \"\")", controller_source)
        self.assertIn("function waitForStageValidForCellRun(notebookId, stageId)", controller_source)
        self.assertIn('if (status === "valid")', controller_source)
        self.assertIn("if (statusIsTerminalProblem(status))", controller_source)
        self.assertIn("throw new Error(stageRunErrorMessage(latestNode, status));", controller_source)
        self.assertIn("waitForValid: true", controller_source)
        self.assertIn("Stage run was cancelled.", controller_source)
        self.assertIn("Stage run failed. Review the stage run details.", controller_source)
        self.assertNotIn("Stage finished with status ${statusLabel(status)}", controller_source)

    def test_runtime_storage_settings_dialog_has_menu_api_and_delete_wiring(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        navigation_source = (
            STATIC_ROOT / "js" / "workbench-navigation-controller.js"
        ).read_text(encoding="utf-8")
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")
        index_template = (
            REPO_ROOT / "bdw" / "bit_data_workbench" / "templates" / "index.html"
        ).read_text(encoding="utf-8")

        self.assertIn("Runtime Storage", index_template)
        self.assertIn("data-open-runtime-storage", index_template)
        self.assertIn("openRuntimeStorageDialog", navigation_source)
        self.assertIn("openRuntimeStorageDialog", app_source)
        self.assertIn("/api/runtime-storage", app_source)
        self.assertIn("/api/runtime-storage/query-cache", app_source)
        self.assertIn("data-runtime-cache-delete", app_source)
        self.assertIn("Cells using Hydrate cache", app_source)
        self.assertIn(".runtime-storage-dialog-body", css_source)

    def test_query_result_duration_has_visible_label_and_help_tooltip(self) -> None:
        query_ui_source = (STATIC_ROOT / "js" / "query-ui.js").read_text(
            encoding="utf-8"
        )
        query_state_source = (STATIC_ROOT / "js" / "query-job-state.js").read_text(
            encoding="utf-8"
        )
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")
        realtime_source = (STATIC_ROOT / "js" / "realtime-controller.js").read_text(
            encoding="utf-8"
        )
        query_runs_source = (STATIC_ROOT / "js" / "query-runs-controller.js").read_text(
            encoding="utf-8"
        )
        insights_source = (STATIC_ROOT / "js" / "query-insights.js").read_text(
            encoding="utf-8"
        )
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("Total elapsed", query_ui_source)
        self.assertIn("Running elapsed", query_ui_source)
        self.assertIn("result-duration-help", query_ui_source)
        self.assertIn("result-duration-value", query_ui_source)
        self.assertIn("data-query-duration-details-toggle", query_ui_source)
        self.assertIn("queryTimingDetailsTableMarkup", query_ui_source)
        self.assertIn("queryTimingBreadcrumbMarkup", query_ui_source)
        self.assertIn("queryTimingBreadcrumbSteps", query_ui_source)
        self.assertIn("queryTimingStepDefinitions", query_ui_source)
        self.assertIn("function activeTimingStepKey(job, steps)", query_ui_source)
        self.assertIn("data-query-duration-total", query_ui_source)
        self.assertIn("data-query-timing-step", query_ui_source)
        self.assertIn("data-query-timing-step-state", query_ui_source)
        self.assertIn("data-query-timing-current-step", query_ui_source)
        self.assertIn("data-query-timing-completed-ms", query_ui_source)
        self.assertIn('state === "current"', query_ui_source)
        self.assertIn("const activeKey = activeTimingStepKey(job, visibleSteps);", query_ui_source)
        self.assertIn('combined.includes("duckdb")', query_ui_source)
        self.assertIn('combined.includes("fetch")', query_ui_source)
        self.assertIn('!running && Number.isFinite(totalMs) && backendTotalMs !== null', query_ui_source)
        self.assertIn("Number(totalMs) - completedBeforeMs", query_ui_source)
        self.assertIn("Same value shown by Total elapsed", query_ui_source)
        self.assertIn('title="${escapeHtml(tooltip)}"', query_ui_source)
        self.assertIn("Click to show recorded timestamps and total elapsed time", query_ui_source)
        self.assertIn("from the Run Cell click until the completed, failed, or cancelled job update reaches this browser", query_ui_source)
        self.assertIn("the same elapsed clock as Total elapsed", query_ui_source)
        self.assertIn("query-resource-sparkline-help", query_ui_source)
        self.assertIn("DuckDB spill", query_ui_source)
        self.assertIn("duckdbSpillBytes", query_ui_source)
        self.assertIn("duckdbSpillLimitBytes", query_ui_source)
        self.assertIn("Threads", query_ui_source)
        self.assertIn("Thread limit", query_ui_source)
        self.assertIn("Active cores", query_ui_source)
        self.assertIn("processThreadCount", query_state_source)
        self.assertIn("duckdbThreadLimit", query_state_source)
        self.assertIn("Running processes", realtime_source)
        self.assertIn("runningProcessCount", realtime_source)
        self.assertIn("data-query-resource-limit", query_ui_source)
        self.assertIn('kind === "spill"', (STATIC_ROOT / "js" / "query-resource-charts.js").read_text(encoding="utf-8"))
        self.assertIn("Timing details explain the headline Total elapsed value shown under Result", insights_source)
        self.assertIn("Shared DuckDB wait", query_ui_source)
        self.assertIn("Shared DuckDB wait", query_runs_source)
        self.assertIn("shared duckdb wait", insights_source)
        self.assertIn("clientObservedMs", insights_source)
        self.assertIn("Query is DuckDB execution time only", insights_source)
        self.assertIn("overhead", insights_source)
        self.assertIn("queryTimingClipboardTable", query_ui_source)
        self.assertIn("data-copy-query-timings", query_ui_source)
        self.assertIn("data-query-timing-table", query_ui_source)
        self.assertIn("Metric\\tValue", query_ui_source)
        self.assertIn("Total elapsed", query_runs_source)
        self.assertIn("runTimingClipboardTable", query_runs_source)
        self.assertIn("data-copy-query-timings", query_runs_source)
        self.assertIn("data-query-timing-table", query_runs_source)
        self.assertIn("copyQueryTimingTable", app_source)
        self.assertIn("Copy timing table failed", app_source)
        self.assertIn(".result-metric-strip[data-copy-query-timings]", css_source)
        self.assertIn(".query-run-history-timing[data-copy-query-timings]", css_source)
        self.assertIn("visibleQueryTimingDetailKeys", app_source)
        self.assertIn("toggleQueryTimingDetails", app_source)
        self.assertIn("[data-query-duration-total]", realtime_source)
        self.assertIn("[data-query-timing-current-step]", realtime_source)
        self.assertIn("dataset.queryTimingCompletedMs", realtime_source)
        self.assertIn("queryJobElapsedMs(job) - safeCompletedMs", realtime_source)
        self.assertIn(".result-duration-group", css_source)
        self.assertIn(".result-duration-toggle", css_source)
        self.assertIn(".result-duration-value", css_source)
        self.assertIn("width: 136px", css_source)
        self.assertIn(".result-duration-help", css_source)
        self.assertIn(".query-timing-breadcrumb", css_source)
        self.assertIn(".query-timing-step", css_source)
        self.assertIn(
            ".result-meta-row {\n"
            "  display: grid;\n"
            "  align-items: center;\n"
            "  grid-template-columns: minmax(0, 1fr);\n"
            "  gap: 6px;",
            css_source,
        )
        self.assertIn(
            ".result-metric-strip {\n"
            "  display: flex;\n"
            "  align-items: center;\n"
            "  flex-wrap: wrap;\n"
            "  width: 100%;\n"
            "  overflow: hidden;",
            css_source,
        )
        self.assertIn("--query-timing-arrow-depth", css_source)
        self.assertIn("clip-path: polygon", css_source)
        self.assertIn("margin-right: calc(var(--query-timing-arrow-depth) * -0.72);", css_source)
        self.assertIn("gap: 6px 0;", css_source)
        self.assertIn("--query-timing-step-width: 148px;", css_source)
        self.assertIn("flex: 0 0 100%;", css_source)
        self.assertIn("flex-wrap: nowrap;", css_source)
        self.assertIn("overflow-x: auto;", css_source)
        self.assertIn("scrollbar-width: thin;", css_source)
        self.assertIn(".query-timing-step-label,\n.query-timing-step-value", css_source)
        self.assertIn("white-space: nowrap;", css_source)
        self.assertIn("grid-template-rows: auto auto;", css_source)
        self.assertIn("flex: 0 0 var(--query-timing-step-width);", css_source)
        self.assertIn("width: var(--query-timing-step-width);", css_source)
        self.assertIn("min-width: var(--query-timing-step-width);", css_source)
        self.assertIn("max-width: var(--query-timing-step-width);", css_source)
        self.assertIn("text-overflow: ellipsis;", css_source)
        self.assertIn("font-variant-numeric: tabular-nums;", css_source)
        self.assertIn("--query-timing-step-width: 138px;", css_source)
        self.assertIn(".query-timing-step:first-child", css_source)
        self.assertIn(".query-timing-step:last-child", css_source)
        self.assertIn(".query-timing-step:only-child", css_source)
        self.assertIn(".query-timing-step.is-completed,\n.query-timing-step.is-current", css_source)
        self.assertIn(".query-timing-step.is-completed", css_source)
        self.assertIn(".query-timing-step.is-current", css_source)
        self.assertIn("background: rgba(11, 68, 121, 0.24);", css_source)
        self.assertIn("box-shadow: inset 0 0 0 1px var(--status-blue-border);", css_source)
        self.assertIn(".query-timing-step.is-pending", css_source)
        self.assertIn(".query-timing-table", css_source)
        self.assertIn(".query-resource-sparkline-help", css_source)
        self.assertIn(".workspace-query-runs-cell {\n  margin-top: 10px;", css_source)
        self.assertIn(".workspace-query-runs,\n.query-runs-page {\n  display: grid;\n  grid-template-columns: minmax(0, 1fr);", css_source)
        self.assertIn("overflow: hidden;", css_source)
        self.assertIn(".workspace-query-runs-cell .query-run-history-list", css_source)
        self.assertIn(".query-run-history-list {\n  width: 100%;", css_source)
        self.assertIn("overflow-x: auto;", css_source)
        self.assertIn(".workspace-query-runs-cell .query-run-history-table {\n  table-layout: fixed;", css_source)
        self.assertIn("min-width: 760px;", css_source)
        self.assertIn(".workspace-query-runs-cell .query-run-history-message", css_source)
        self.assertIn("overflow-wrap: anywhere;", css_source)

    def test_sql_completion_uses_simple_s3_and_pg_source_references(self) -> None:
        app_source = (STATIC_ROOT / "js" / "app.js").read_text(encoding="utf-8")

        self.assertIn("schema?.s3References", app_source)
        self.assertIn("schema?.pgReferences", app_source)
        self.assertIn("PostgreSQL relation", app_source)
        self.assertIn("S3 object", app_source)
        self.assertIn("[A-Za-z0-9_.$", app_source)
        self.assertNotIn("data/recommended/virtual-s3", app_source)


if __name__ == "__main__":
    unittest.main()

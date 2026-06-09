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
        self.assertIn('aria-label="Expand SQL editor"', source)
        self.assertIn('aria-pressed="false"', source)

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
        self.assertIn("data-editor-sql-view-toggle", workspace_template)
        self.assertIn("/api/query-sql/prepare", app_source)
        self.assertIn("prepareDuckdbSqlForCell", app_source)
        self.assertIn("currentVisibleEditorSql", app_source)
        self.assertIn("invalidatePreparedSqlViewForCell", app_source)
        self.assertIn("notebookId", validation_source)
        self.assertIn(".editor-sql-view-toggle", css_source)
        self.assertIn(".editor-duckdb-sql-panel", css_source)

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
        self.assertIn("normalizeCellStage", model_source)
        self.assertIn("normalizeNotebookPipelineMode", model_source)
        self.assertIn("createNotebookStagePipelineController", app_source)
        self.assertIn("validatePipelineStageAliases", app_source)
        self.assertIn("/api/query-sql/prepare", app_source)
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
        self.assertIn("stageActionButton(node", controller_source)
        self.assertIn("stageActionButtons(node", controller_source)
        self.assertIn("createAppTooltipController", app_source)
        self.assertIn("createAppTooltipController().install();", app_source)
        self.assertIn('[data-app-tooltip], [data-pipeline-tooltip], [title]', app_tooltip_source)
        self.assertIn("suppressNativeTitle", app_tooltip_source)
        self.assertIn("restoreNativeTitle", app_tooltip_source)
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
        self.assertIn("animation: pipeline-node-computing-glow 3s ease-in-out infinite;", css_source)
        self.assertIn("stroke-width: 8;", css_source)
        self.assertIn("box-shadow: inset 4px 0 0 var(--accent);", css_source)
        self.assertIn(".pipeline-stage-row-running > td", css_source)
        self.assertIn(".pipeline-stage-row-running > td:first-child::after", css_source)
        self.assertIn(".pipeline-stage-row-running > td:last-child::after", css_source)
        self.assertIn("@keyframes pipeline-stage-row-computing-glow", css_source)
        self.assertIn("@keyframes pipeline-stage-row-edge-glow", css_source)
        self.assertIn("animation: pipeline-stage-row-computing-glow 3s ease-in-out infinite;", css_source)
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
        self.assertIn("keeps cells independent", workspace_template)

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
        realtime_source = (STATIC_ROOT / "js" / "realtime-controller.js").read_text(
            encoding="utf-8"
        )
        insights_source = (STATIC_ROOT / "js" / "query-insights.js").read_text(
            encoding="utf-8"
        )
        css_source = (STATIC_ROOT / "css" / "app.css").read_text(encoding="utf-8")

        self.assertIn("Total elapsed", query_ui_source)
        self.assertIn("Running elapsed", query_ui_source)
        self.assertIn("result-duration-help", query_ui_source)
        self.assertIn('title="${escapeHtml(tooltip)}"', query_ui_source)
        self.assertIn('aria-label="${escapeHtml(`${label}: ${duration}. ${tooltip}`)}"', query_ui_source)
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
        self.assertIn("clientObservedMs", insights_source)
        self.assertIn("Query is DuckDB execution time only", insights_source)
        self.assertIn("overhead", insights_source)
        self.assertIn(".result-duration-group", css_source)
        self.assertIn(".result-duration-help", css_source)
        self.assertIn(".query-resource-sparkline-help", css_source)

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

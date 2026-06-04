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
        self.assertIn("Timing details explain the headline Total elapsed value shown under Result", insights_source)
        self.assertIn("clientObservedMs", insights_source)
        self.assertIn("Query is DuckDB execution time only", insights_source)
        self.assertIn("overhead", insights_source)
        self.assertIn(".result-duration-group", css_source)
        self.assertIn(".result-duration-help", css_source)
        self.assertIn(".query-resource-sparkline-help", css_source)


if __name__ == "__main__":
    unittest.main()

export function notebookFolders() {
  return Array.from(document.querySelectorAll("[data-tree-folder]"));
}

export function runbookFolders() {
  return Array.from(document.querySelectorAll("[data-runbook-folder]"));
}

export function dataSourceNodes() {
  return Array.from(document.querySelectorAll("[data-source-catalog], [data-source-schema]"));
}

export function sourceObjectNodes() {
  return Array.from(document.querySelectorAll("[data-source-object]"));
}

export function sourceInspector() {
  return document.querySelector("[data-source-inspector]");
}

export function sourceInspectorPanel() {
  return document.querySelector("[data-source-inspector-panel]");
}

export function queryMonitorList() {
  return document.querySelector("[data-query-monitor-list]");
}

export function queryMonitorCount() {
  return document.querySelector("[data-query-monitor-count]");
}

export function sidebarQueryCounts() {
  return Array.from(document.querySelectorAll("[data-sidebar-query-count]"));
}

export function queryPerformanceSection() {
  return document.querySelector("[data-query-performance]");
}

export function queryPerformanceStats() {
  return document.querySelector("[data-query-performance-stats]");
}

export function queryPerformanceChart() {
  return document.querySelector("[data-query-performance-chart]");
}

export function queryPerformanceDistribution() {
  return document.querySelector("[data-query-performance-distribution]");
}

export function queryNotificationMenu() {
  return document.querySelector("[data-query-notifications]");
}

export function settingsMenu() {
  return document.querySelector("[data-settings-menu]");
}

export function queryNotificationList() {
  return document.querySelector("[data-query-notification-list]");
}

export function queryNotificationCount() {
  return document.querySelector("[data-query-notification-count]");
}

export function homePageRoot() {
  return document.querySelector("[data-home-page]");
}

export function queryWorkbenchEntryPageRoot() {
  return document.querySelector("[data-query-workbench-entry-page]");
}

export function queryWorkbenchDataSourcesPageRoot() {
  return document.querySelector("[data-data-source-management-page]");
}

export function notebookSection() {
  return document.querySelector("[data-notebook-section]");
}

export function notebookTreeRoot() {
  return document.querySelector("[data-notebook-tree]");
}

export function dataSourcesSection() {
  return document.querySelector("[data-data-sources-section]");
}

export function ingestionRunbookSection() {
  return document.querySelector("[data-ingestion-runbook-section]");
}

export function shellRoot() {
  return document.querySelector("[data-shell]");
}

export function homeRecentNotebooksRoot() {
  return document.querySelector("[data-home-recent-notebooks]");
}

export function homeRecentIngestionsRoot() {
  return document.querySelector("[data-home-recent-ingestions]");
}

export function notificationClearButton() {
  return document.querySelector("[data-clear-notifications]");
}

export function ingestionGeneratorList() {
  return document.querySelector("[data-ingestion-generator-list]");
}

export function ingestionJobList() {
  return document.querySelector("[data-ingestion-job-list]");
}

export function ingestionGeneratorSectionTitle() {
  return document.querySelector("[data-ingestion-generator-section-title]");
}

export function ingestionGeneratorSectionCopy() {
  return document.querySelector("[data-ingestion-generator-section-copy]");
}

export function ingestionJobSectionTitle() {
  return document.querySelector("[data-ingestion-job-section-title]");
}

export function ingestionJobSectionCopy() {
  return document.querySelector("[data-ingestion-job-section-copy]");
}

export function dataGenerationMonitorList() {
  return document.querySelector("[data-generation-monitor-list]");
}

export function dataGenerationMonitorCount() {
  return document.querySelector("[data-generation-monitor-count]");
}

export function sidebarToggles() {
  return Array.from(document.querySelectorAll("[data-sidebar-toggle]"));
}
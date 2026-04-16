export function createWorkbenchNavigationController(helpers) {
  const {
    applySidebarCollapsedState,
    closeSettingsMenus,
    getClearVisibleNotifications,
    getQueryNotificationMenu,
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
  } = helpers;

  function closeNotificationMenu() {
    getQueryNotificationMenu()?.removeAttribute("open");
  }

  async function handleClick(event) {
    const sidebarToggleButton = event.target.closest("[data-sidebar-toggle]");
    if (sidebarToggleButton) {
      event.preventDefault();
      const collapsed = !document.body.classList.contains("sidebar-collapsed");
      applySidebarCollapsedState(collapsed);
      writeSidebarCollapsed(collapsed);
      return true;
    }

    const openIngestionWorkbenchButton = event.target.closest("[data-open-ingestion-workbench]");
    if (openIngestionWorkbenchButton) {
      event.preventDefault();
      event.stopPropagation();
      closeNotificationMenu();
      await openIngestionWorkbench();
      return true;
    }

    const openLoaderWorkbenchButton = event.target.closest("[data-open-loader-workbench]");
    if (openLoaderWorkbenchButton) {
      event.preventDefault();
      event.stopPropagation();
      closeNotificationMenu();
      await openLoaderWorkbench({
        focusJobId: openLoaderWorkbenchButton.dataset.focusGenerationJob || "",
      });
      return true;
    }

    const openQueryWorkbenchButton = event.target.closest("[data-open-query-workbench]");
    if (openQueryWorkbenchButton) {
      event.preventDefault();
      event.stopPropagation();
      closeNotificationMenu();
      if (openQueryWorkbenchButton.dataset.openQueryWorkbenchNavigation === "true") {
        await openQueryWorkbenchNavigation();
      } else {
        await openQueryWorkbench(openQueryWorkbenchButton.dataset.openRecentNotebook || "");
      }
      return true;
    }

    const openQueryDataSourcesButton = event.target.closest("[data-open-query-data-sources]");
    if (openQueryDataSourcesButton) {
      event.preventDefault();
      event.stopPropagation();
      closeNotificationMenu();
      await openQueryWorkbenchDataSources();
      return true;
    }

    const openQueryDataSourceButton = event.target.closest("[data-open-query-data-source]");
    if (openQueryDataSourceButton) {
      event.preventDefault();
      event.stopPropagation();
      closeNotificationMenu();
      await loadQueryWorkbenchDataSources(openQueryDataSourceButton.dataset.openQueryDataSource || "");
      return true;
    }

    const openQueryWorkbenchEntryButton = event.target.closest("[data-open-query-workbench-entry]");
    if (openQueryWorkbenchEntryButton) {
      event.preventDefault();
      event.stopPropagation();
      closeNotificationMenu();
      await loadQueryWorkbenchEntry();
      return true;
    }

    const openRecentNotebookButton = event.target.closest("[data-open-recent-notebook]");
    if (openRecentNotebookButton) {
      event.preventDefault();
      event.stopPropagation();
      closeNotificationMenu();
      await openQueryWorkbench(openRecentNotebookButton.dataset.openRecentNotebook || "");
      return true;
    }

    const openIngestionRunbookButton = event.target.closest("[data-open-ingestion-runbook]");
    if (openIngestionRunbookButton) {
      event.preventDefault();
      const generatorId = openIngestionRunbookButton.dataset.openIngestionRunbook || "";
      selectIngestionRunbook(generatorId, { spotlight: true });
      await openLoaderWorkbench({
        focusGeneratorId: generatorId,
      });
      return true;
    }

    const clearNotificationsButton = event.target.closest("[data-clear-notifications]");
    if (clearNotificationsButton) {
      event.preventDefault();
      getClearVisibleNotifications?.()?.();
      return true;
    }

    const clearLocalWorkspaceButton = event.target.closest("[data-clear-local-workspace]");
    if (clearLocalWorkspaceButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSettingsMenus();
      await promptClearLocalWorkspace();
      return true;
    }

    const openAboutButton = event.target.closest("[data-open-about]");
    if (openAboutButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSettingsMenus();
      await showAboutDialog();
      return true;
    }

    const openFeatureListButton = event.target.closest("[data-open-feature-list]");
    if (openFeatureListButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSettingsMenus();
      await showFeatureListDialog();
      return true;
    }

    const openServiceConsumptionButton = event.target.closest(
      "[data-open-service-consumption]"
    );
    if (openServiceConsumptionButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSettingsMenus();
      await openServiceConsumptionPage();
      return true;
    }

    return false;
  }

  return {
    handleClick,
  };
}

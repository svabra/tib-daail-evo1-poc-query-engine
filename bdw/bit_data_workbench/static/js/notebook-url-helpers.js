export function createNotebookUrlHelpers({ isLocalNotebookId }) {
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

  function queryWorkbenchDataSourceExplorerUrl(sourceId = "") {
    const normalizedSourceId = String(sourceId || "").trim();
    if (!normalizedSourceId) {
      return "/query-workbench/data-sources/explorer";
    }

    return `/query-workbench/data-sources/explorer?source_id=${encodeURIComponent(normalizedSourceId)}`;
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

  function pushQueryWorkbenchDataSourceExplorerHistory(sourceId = "") {
    const nextUrl = queryWorkbenchDataSourceExplorerUrl(sourceId);
    if (`${window.location.pathname}${window.location.search}` === nextUrl) {
      return;
    }

    window.history.pushState(
      { mode: "query-workbench-data-source-explorer", sourceId: String(sourceId || "").trim() },
      "",
      nextUrl
    );
  }

  function pushHomeHistory() {
    if (window.location.pathname === "/") {
      return;
    }

    window.history.pushState({ mode: "home" }, "", "/");
  }

  function pushDataProductsHistory() {
    if (window.location.pathname === "/data-products") {
      return;
    }

    window.history.pushState({ mode: "data-products" }, "", "/data-products");
  }

  function pushServiceConsumptionHistory() {
    if (window.location.pathname === "/service-consumption") {
      return;
    }

    window.history.pushState(
      { mode: "service-consumption" },
      "",
      "/service-consumption"
    );
  }

  return {
    pushDataProductsHistory,
    notebookUrl,
    pushHomeHistory,
    pushNotebookHistory,
    pushQueryWorkbenchDataSourceExplorerHistory,
    pushQueryWorkbenchDataSourcesHistory,
    pushQueryWorkbenchHistory,
    pushServiceConsumptionHistory,
    queryWorkbenchDataSourceExplorerUrl,
    queryWorkbenchDataSourcesUrl,
  };
}

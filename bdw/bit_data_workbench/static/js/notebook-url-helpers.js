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

  function queryWorkbenchDataSourcesUrl(sourceId = "", { browse = false } = {}) {
    const normalizedSourceId = String(sourceId || "").trim();
    const params = new URLSearchParams();
    if (normalizedSourceId) {
      params.set("source_id", normalizedSourceId);
    }
    if (browse) {
      params.set("browse", "1");
    }
    const query = params.toString();
    return query ? `/data-sources?${query}` : "/data-sources";
  }

  function queryWorkbenchDataSourceExplorerUrl(sourceId = "") {
    const normalizedSourceId = String(sourceId || "").trim();
    if (!normalizedSourceId) {
      return "/data-sources/browser";
    }

    return `/data-sources/browser?source_id=${encodeURIComponent(normalizedSourceId)}`;
  }

  function pushQueryWorkbenchDataSourcesHistory(sourceId = "", { browse = false } = {}) {
    const nextUrl = queryWorkbenchDataSourcesUrl(sourceId, { browse });
    if (`${window.location.pathname}${window.location.search}` === nextUrl) {
      return;
    }

    window.history.pushState(
      { mode: "data-sources", sourceId: String(sourceId || "").trim(), browse: Boolean(browse) },
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
      { mode: "data-sources-browser", sourceId: String(sourceId || "").trim() },
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

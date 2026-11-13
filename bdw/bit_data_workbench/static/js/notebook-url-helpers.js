export function createNotebookUrlHelpers({ isLocalNotebookId }) {
  function withCreationRequestContext(path) {
    const href = globalThis.window?.location?.href;
    if (!href) return path;
    const current = new URL(href);
    const requestId = current.searchParams.get("creationRequest");
    if (!requestId) return path;
    const next = new URL(path, current);
    next.searchParams.set("creationRequest", requestId);
    const user = current.searchParams.get("demoUser");
    if (user) next.searchParams.set("demoUser", user);
    return `${next.pathname}${next.search}`;
  }

  function notebookUrl(notebookId) {
    if (!notebookId || isLocalNotebookId(notebookId)) {
      return null;
    }

    return withCreationRequestContext(`/notebooks/${encodeURIComponent(notebookId)}`);
  }

  function pushNotebookHistory(notebookId) {
    const nextUrl = notebookUrl(notebookId);
    if (!nextUrl || window.location.pathname === nextUrl.split("?")[0]) {
      return;
    }

    window.history.pushState({ mode: "notebook", notebookId }, "", nextUrl);
  }

  function pushQueryWorkbenchHistory() {
    if (window.location.pathname === "/query-workbench") {
      return;
    }

    window.history.pushState({ mode: "query-workbench" }, "", withCreationRequestContext("/query-workbench"));
  }

  function pushQueryRunsHistory() {
    if (window.location.pathname === "/query-workbench/query-runs") {
      return;
    }

    window.history.pushState(
      { mode: "query-runs" },
      "",
      withCreationRequestContext("/query-workbench/query-runs")
    );
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
    return withCreationRequestContext(query ? `/data-sources?${query}` : "/data-sources");
  }

  function queryWorkbenchDataSourceExplorerUrl(sourceId = "") {
    const normalizedSourceId = String(sourceId || "").trim();
    if (!normalizedSourceId) {
      return withCreationRequestContext("/data-sources/browser");
    }

    return withCreationRequestContext(`/data-sources/browser?source_id=${encodeURIComponent(normalizedSourceId)}`);
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

  function pushDataExchangeHistory() {
    if (window.location.pathname === "/data-exchange") {
      return;
    }

    window.history.pushState({ mode: "data-exchange" }, "", "/data-exchange");
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
    pushDataExchangeHistory,
    notebookUrl,
    pushHomeHistory,
    pushNotebookHistory,
    pushQueryRunsHistory,
    pushQueryWorkbenchDataSourceExplorerHistory,
    pushQueryWorkbenchDataSourcesHistory,
    pushQueryWorkbenchHistory,
    pushServiceConsumptionHistory,
    queryWorkbenchDataSourceExplorerUrl,
    queryWorkbenchDataSourcesUrl,
  };
}

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

  function pushHomeHistory() {
    if (window.location.pathname === "/") {
      return;
    }

    window.history.pushState({ mode: "home" }, "", "/");
  }

  return {
    notebookUrl,
    pushHomeHistory,
    pushNotebookHistory,
    pushQueryWorkbenchDataSourcesHistory,
    pushQueryWorkbenchHistory,
    queryWorkbenchDataSourcesUrl,
  };
}
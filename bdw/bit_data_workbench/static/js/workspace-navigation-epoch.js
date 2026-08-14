export function createWorkspaceNavigationEpoch() {
  let epoch = 0;
  let controller = null;

  function begin({ path = window.location.pathname, notebookId = "", reason = "navigation" } = {}) {
    controller?.abort();
    controller = new AbortController();
    epoch += 1;
    return Object.freeze({
      epoch,
      path: String(path || ""),
      notebookId: String(notebookId || ""),
      reason,
      signal: controller.signal,
    });
  }

  function snapshot({ notebookId = "", reason = "snapshot" } = {}) {
    return Object.freeze({
      epoch,
      path: window.location.pathname,
      notebookId: String(notebookId || ""),
      reason,
      signal: controller?.signal ?? null,
    });
  }

  function isCurrent(token, { path = null, notebookId = null } = {}) {
    if (!token || token.epoch !== epoch || token.signal?.aborted) {
      return false;
    }
    if (path !== null && window.location.pathname !== path) {
      return false;
    }
    if (notebookId !== null && String(token.notebookId || "") !== String(notebookId || "")) {
      return false;
    }
    return true;
  }

  return Object.freeze({ begin, isCurrent, snapshot, currentEpoch: () => epoch });
}

export function createWorkbenchStorage({
  cacheResetStorageKey,
  dismissedNotificationsStorageKey,
  getApplicationVersion,
  getDismissedNotificationKeys,
  lastNotebookStorageKey,
  notebookActivityStorageKey,
  setDismissedNotificationKeys,
  sidebarCollapsedStorageKey,
  workbenchClientIdStorageKey,
}) {
  function generatedWorkbenchClientId() {
    return `client-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
  }

  function workbenchClientId() {
    try {
      let clientId = window.localStorage.getItem(workbenchClientIdStorageKey);
      if (clientId) {
        return clientId;
      }
      clientId = generatedWorkbenchClientId();
      window.localStorage.setItem(workbenchClientIdStorageKey, clientId);
      return clientId;
    } catch (_error) {
      return generatedWorkbenchClientId();
    }
  }

  function readLastNotebookId() {
    try {
      return window.localStorage.getItem(lastNotebookStorageKey);
    } catch (_error) {
      return null;
    }
  }

  function writeLastNotebookId(notebookId) {
    try {
      window.localStorage.setItem(lastNotebookStorageKey, notebookId);
    } catch (_error) {
      // Ignore persistence failures and keep the session functional.
    }
  }

  function readSidebarCollapsed() {
    try {
      return window.localStorage.getItem(sidebarCollapsedStorageKey) === "true";
    } catch (_error) {
      return false;
    }
  }

  function writeSidebarCollapsed(collapsed) {
    try {
      window.localStorage.setItem(sidebarCollapsedStorageKey, collapsed ? "true" : "false");
    } catch (_error) {
      // Ignore persistence failures and keep the UI usable.
    }
  }

  function readNotebookActivity() {
    try {
      const rawValue = window.localStorage.getItem(notebookActivityStorageKey);
      if (!rawValue) {
        return {};
      }

      const parsed = JSON.parse(rawValue);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    } catch (_error) {
      return {};
    }
  }

  function writeNotebookActivity(activity) {
    try {
      window.localStorage.setItem(notebookActivityStorageKey, JSON.stringify(activity));
    } catch (_error) {
      // Ignore persistence failures and keep the UI usable.
    }
  }

  function readDismissedNotificationKeys() {
    try {
      const rawValue = window.localStorage.getItem(dismissedNotificationsStorageKey);
      if (!rawValue) {
        return new Set();
      }

      const parsed = JSON.parse(rawValue);
      return Array.isArray(parsed) ? new Set(parsed.map((entry) => String(entry))) : new Set();
    } catch (_error) {
      return new Set();
    }
  }

  function writeDismissedNotificationKeys() {
    try {
      const notificationKeys = getDismissedNotificationKeys?.() ?? new Set();
      window.localStorage.setItem(
        dismissedNotificationsStorageKey,
        JSON.stringify(Array.from(notificationKeys))
      );
    } catch (_error) {
      // Ignore persistence failures and keep the UI functional.
    }
  }

  function clearWorkbenchLocalCache() {
    const storageKeys = [];
    for (let index = 0; index < window.localStorage.length; index += 1) {
      const key = window.localStorage.key(index);
      if (key && key.startsWith("bdw.")) {
        storageKeys.push(key);
      }
    }

    for (const key of storageKeys) {
      window.localStorage.removeItem(key);
    }

    const resetMarker = {
      clearedAt: new Date().toISOString(),
      reason: "clear-local-workspace",
      version: getApplicationVersion(),
    };
    window.localStorage.setItem(cacheResetStorageKey, JSON.stringify(resetMarker));
    setDismissedNotificationKeys?.(new Set());
    return resetMarker;
  }

  return {
    clearWorkbenchLocalCache,
    readDismissedNotificationKeys,
    readLastNotebookId,
    readNotebookActivity,
    readSidebarCollapsed,
    workbenchClientId,
    writeDismissedNotificationKeys,
    writeLastNotebookId,
    writeNotebookActivity,
    writeSidebarCollapsed,
  };
}
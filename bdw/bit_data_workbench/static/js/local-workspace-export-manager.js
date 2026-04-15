export function createLocalWorkspaceExportManager({
  databaseName,
  databaseVersion,
  exportStoreName,
  normalizeFolderPath,
}) {
  let databasePromise = null;

  function ensureLocalWorkspaceDatabaseSupport() {
    if (typeof window.indexedDB === "undefined") {
      throw new Error("IndexedDB is not available in this browser, so Local Workspace storage cannot be used.");
    }
  }

  function openLocalWorkspaceDatabase() {
    ensureLocalWorkspaceDatabaseSupport();
    if (databasePromise) {
      return databasePromise;
    }

    databasePromise = new Promise((resolve, reject) => {
      const request = window.indexedDB.open(databaseName, databaseVersion);

      request.onupgradeneeded = () => {
        const database = request.result;
        if (!database.objectStoreNames.contains(exportStoreName)) {
          database.createObjectStore(exportStoreName, {
            keyPath: "id",
          });
        }
      };

      request.onsuccess = () => {
        const database = request.result;
        database.onversionchange = () => {
          database.close();
        };
        resolve(database);
      };

      request.onerror = () => {
        reject(request.error || new Error("Could not open the Local Workspace database."));
      };

      request.onblocked = () => {
        reject(new Error("The Local Workspace database is blocked by another tab or session."));
      };
    });

    return databasePromise;
  }

  function normalizeLocalWorkspaceExportEntry(entry) {
    if (!entry || typeof entry !== "object") {
      return null;
    }

    const id = String(entry.id || "").trim();
    if (!id) {
      return null;
    }

    return {
      id,
      fileName: String(entry.fileName || "").trim() || "local-workspace-file",
      folderPath: normalizeFolderPath(entry.folderPath),
      exportFormat: String(entry.exportFormat || "").trim().toLowerCase() || "json",
      mimeType: String(entry.mimeType || "").trim(),
      sizeBytes: Number.isFinite(Number(entry.sizeBytes)) ? Number(entry.sizeBytes) : 0,
      createdAt: String(entry.createdAt || "").trim(),
      updatedAt: String(entry.updatedAt || entry.createdAt || "").trim(),
      notebookTitle: String(entry.notebookTitle || "").trim(),
      cellId: String(entry.cellId || "").trim(),
      columnCount: Number.isFinite(Number(entry.columnCount)) ? Number(entry.columnCount) : 0,
      rowCount: Number.isFinite(Number(entry.rowCount)) ? Number(entry.rowCount) : 0,
      blob: entry.blob instanceof Blob ? entry.blob : null,
    };
  }

  async function clearLocalWorkspaceExports() {
    const database = await openLocalWorkspaceDatabase();
    return new Promise((resolve, reject) => {
      const transaction = database.transaction(exportStoreName, "readwrite");
      const store = transaction.objectStore(exportStoreName);
      const request = store.clear();

      request.onsuccess = () => {
        resolve();
      };

      request.onerror = () => {
        reject(request.error || new Error("Could not clear Local Workspace files."));
      };
    });
  }

  async function listLocalWorkspaceExports() {
    const database = await openLocalWorkspaceDatabase();
    return new Promise((resolve, reject) => {
      const transaction = database.transaction(exportStoreName, "readonly");
      const store = transaction.objectStore(exportStoreName);
      const request = store.getAll();

      request.onsuccess = () => {
        const entries = Array.isArray(request.result)
          ? request.result.map((entry) => normalizeLocalWorkspaceExportEntry(entry)).filter(Boolean)
          : [];
        entries.sort((left, right) => {
          return String(right.updatedAt || right.createdAt || "").localeCompare(
            String(left.updatedAt || left.createdAt || "")
          );
        });
        resolve(entries);
      };

      request.onerror = () => {
        reject(request.error || new Error("Could not read Local Workspace files."));
      };
    });
  }

  async function getLocalWorkspaceExport(entryId) {
    const normalizedEntryId = String(entryId || "").trim();
    if (!normalizedEntryId) {
      return null;
    }

    const database = await openLocalWorkspaceDatabase();
    return new Promise((resolve, reject) => {
      const transaction = database.transaction(exportStoreName, "readonly");
      const store = transaction.objectStore(exportStoreName);
      const request = store.get(normalizedEntryId);

      request.onsuccess = () => {
        resolve(normalizeLocalWorkspaceExportEntry(request.result));
      };

      request.onerror = () => {
        reject(request.error || new Error("Could not load the Local Workspace file."));
      };
    });
  }

  async function saveLocalWorkspaceExport(entry) {
    const normalizedEntry = normalizeLocalWorkspaceExportEntry(entry);
    if (!normalizedEntry || !(normalizedEntry.blob instanceof Blob)) {
      throw new Error("The Local Workspace file is incomplete and could not be saved.");
    }

    const database = await openLocalWorkspaceDatabase();
    return new Promise((resolve, reject) => {
      const transaction = database.transaction(exportStoreName, "readwrite");
      const store = transaction.objectStore(exportStoreName);
      const request = store.put(normalizedEntry);

      request.onsuccess = () => {
        resolve(normalizedEntry);
      };

      request.onerror = () => {
        reject(request.error || new Error("Could not save the Local Workspace file."));
      };
    });
  }

  async function deleteLocalWorkspaceExport(entryId) {
    const normalizedEntryId = String(entryId || "").trim();
    if (!normalizedEntryId) {
      return;
    }

    const database = await openLocalWorkspaceDatabase();
    return new Promise((resolve, reject) => {
      const transaction = database.transaction(exportStoreName, "readwrite");
      const store = transaction.objectStore(exportStoreName);
      const request = store.delete(normalizedEntryId);

      request.onsuccess = () => {
        resolve();
      };

      request.onerror = () => {
        reject(request.error || new Error("Could not delete the Local Workspace file."));
      };
    });
  }

  return {
    clearLocalWorkspaceExports,
    deleteLocalWorkspaceExport,
    getLocalWorkspaceExport,
    listLocalWorkspaceExports,
    saveLocalWorkspaceExport,
  };
}
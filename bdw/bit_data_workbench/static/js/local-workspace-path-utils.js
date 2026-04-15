export function createLocalWorkspacePathUtils({ folderStorageKey, relationPrefix }) {
  function normalizeLocalWorkspaceFolderPath(path) {
    return String(path || "")
      .split("/")
      .map((segment) => String(segment || "").trim())
      .filter(Boolean)
      .join("/");
  }

  function localWorkspaceFolderPaths(paths = []) {
    const knownPaths = new Set([""]);

    paths.forEach((path) => {
      const normalizedPath = normalizeLocalWorkspaceFolderPath(path);
      if (!normalizedPath) {
        return;
      }

      let currentPath = "";
      normalizedPath.split("/").forEach((segment) => {
        currentPath = currentPath ? `${currentPath}/${segment}` : segment;
        knownPaths.add(currentPath);
      });
    });

    return Array.from(knownPaths).sort((left, right) => {
      if (!left && right) {
        return -1;
      }
      if (left && !right) {
        return 1;
      }

      return left.localeCompare(right, undefined, { sensitivity: "base" });
    });
  }

  function localWorkspaceDisplayPath(folderPath = "", fileName = "") {
    const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
    const folderSuffix = normalizedFolderPath ? `${normalizedFolderPath}/` : "";
    const normalizedFileName = String(fileName || "").trim();
    return `Local Workspace / ${folderSuffix}${normalizedFileName}`.trim();
  }

  function localWorkspaceFolderName(folderPath = "") {
    const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
    if (!normalizedFolderPath) {
      return "Root";
    }

    return normalizedFolderPath.split("/").at(-1) || normalizedFolderPath;
  }

  function localWorkspaceFolderDepth(folderPath = "") {
    const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
    return normalizedFolderPath ? normalizedFolderPath.split("/").length : 0;
  }

  function readLocalWorkspaceStoredFolderPaths() {
    try {
      const rawValue = window.localStorage.getItem(folderStorageKey);
      if (!rawValue) {
        return [];
      }

      const parsed = JSON.parse(rawValue);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      return [];
    }
  }

  function writeLocalWorkspaceStoredFolderPaths(folderPaths = []) {
    try {
      const normalizedPaths = localWorkspaceFolderPaths(folderPaths).filter(Boolean);
      if (!normalizedPaths.length) {
        window.localStorage.removeItem(folderStorageKey);
        return;
      }

      window.localStorage.setItem(folderStorageKey, JSON.stringify(normalizedPaths));
    } catch (_error) {
      // Ignore persistence failures and keep the browser-local workspace usable.
    }
  }

  function localWorkspaceStoredFolderPaths() {
    return localWorkspaceFolderPaths(readLocalWorkspaceStoredFolderPaths()).filter(Boolean);
  }

  function allLocalWorkspaceFolderPaths(paths = []) {
    return localWorkspaceFolderPaths([...paths, ...localWorkspaceStoredFolderPaths()]);
  }

  function localWorkspaceParentFolderPath(folderPath = "") {
    const segments = normalizeLocalWorkspaceFolderPath(folderPath)
      .split("/")
      .filter(Boolean);
    if (!segments.length) {
      return "";
    }

    segments.pop();
    return segments.join("/");
  }

  function localWorkspaceFolderContainsPath(folderPath = "", candidatePath = "") {
    const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
    const normalizedCandidatePath = normalizeLocalWorkspaceFolderPath(candidatePath);
    if (!normalizedFolderPath) {
      return !normalizedCandidatePath;
    }
    return (
      normalizedCandidatePath === normalizedFolderPath ||
      normalizedCandidatePath.startsWith(`${normalizedFolderPath}/`)
    );
  }

  function ensureLocalWorkspaceFolderPath(folderPath = "") {
    const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
    if (!normalizedFolderPath) {
      return localWorkspaceStoredFolderPaths();
    }

    const nextPaths = localWorkspaceFolderPaths([
      ...localWorkspaceStoredFolderPaths(),
      normalizedFolderPath,
    ]).filter(Boolean);
    writeLocalWorkspaceStoredFolderPaths(nextPaths);
    return nextPaths;
  }

  function removeLocalWorkspaceFolderBranch(folderPath = "") {
    const normalizedFolderPath = normalizeLocalWorkspaceFolderPath(folderPath);
    if (!normalizedFolderPath) {
      writeLocalWorkspaceStoredFolderPaths([]);
      return [];
    }

    const nextPaths = localWorkspaceStoredFolderPaths().filter(
      (path) => !localWorkspaceFolderContainsPath(normalizedFolderPath, path)
    );
    writeLocalWorkspaceStoredFolderPaths(nextPaths);
    return nextPaths;
  }

  function closestExistingLocalWorkspaceFolderPath(folderPath = "", availablePaths = []) {
    const normalizedAvailablePaths = new Set(allLocalWorkspaceFolderPaths(availablePaths));
    let currentPath = normalizeLocalWorkspaceFolderPath(folderPath);

    while (currentPath && !normalizedAvailablePaths.has(currentPath)) {
      currentPath = localWorkspaceParentFolderPath(currentPath);
    }

    return normalizedAvailablePaths.has(currentPath) ? currentPath : "";
  }

  function localWorkspaceRelation(entryId) {
    return `${relationPrefix}${String(entryId || "").trim()}`;
  }

  return {
    allLocalWorkspaceFolderPaths,
    closestExistingLocalWorkspaceFolderPath,
    ensureLocalWorkspaceFolderPath,
    localWorkspaceDisplayPath,
    localWorkspaceFolderContainsPath,
    localWorkspaceFolderDepth,
    localWorkspaceFolderName,
    localWorkspaceFolderPaths,
    localWorkspaceParentFolderPath,
    localWorkspaceRelation,
    localWorkspaceStoredFolderPaths,
    normalizeLocalWorkspaceFolderPath,
    readLocalWorkspaceStoredFolderPaths,
    removeLocalWorkspaceFolderBranch,
    writeLocalWorkspaceStoredFolderPaths,
  };
}
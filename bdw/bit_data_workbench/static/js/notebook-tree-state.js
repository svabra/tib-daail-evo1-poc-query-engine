export function createNotebookTreeState(helpers) {
  const {
    deleteStoredNotebookState,
    isLocalNotebookId,
    notebookTreeStorageKey,
  } = helpers;

  function readStoredNotebookTree() {
    try {
      const rawValue = window.localStorage.getItem(notebookTreeStorageKey);
      if (!rawValue) {
        return null;
      }

      const parsed = JSON.parse(rawValue);
      if (!Array.isArray(parsed)) {
        return null;
      }

      const migration = migrateStoredNotebookTree(parsed);
      if (migration.changed) {
        writeStoredNotebookTree(migration.state);
      }

      return migration.state;
    } catch (_error) {
      return null;
    }
  }

  function writeStoredNotebookTree(state) {
    try {
      window.localStorage.setItem(notebookTreeStorageKey, JSON.stringify(state));
    } catch (_error) {
      // Ignore persistence failures and keep the in-memory tree functional.
    }
  }

  function createStoredFolderState(name, parentFolderId = "") {
    const folderId = deriveFolderId(name, parentFolderId);
    const permissions = defaultFolderPermissions(folderId);
    return {
      type: "folder",
      name,
      folderId,
      open: true,
      canEdit: permissions.canEdit,
      canDelete: permissions.canDelete,
      children: [],
    };
  }

  function removeNotebookFromStoredTree(nodes, notebookId) {
    let removed = null;
    let changed = false;
    const nextNodes = [];

    for (const node of Array.isArray(nodes) ? nodes : []) {
      if (!node || typeof node !== "object") {
        continue;
      }

      if (node.type === "notebook" && node.notebookId === notebookId) {
        removed = node;
        changed = true;
        continue;
      }

      if (node.type === "folder" && Array.isArray(node.children)) {
        const childResult = removeNotebookFromStoredTree(node.children, notebookId);
        if (childResult.changed) {
          changed = true;
        }
        if (!removed && childResult.removed) {
          removed = childResult.removed;
        }
        nextNodes.push({
          ...node,
          children: childResult.nodes,
        });
        continue;
      }

      nextNodes.push(node);
    }

    return {
      nodes: nextNodes,
      removed,
      changed,
    };
  }

  function folderContainsNotebookState(node, notebookId) {
    if (!node || typeof node !== "object") {
      return false;
    }

    if (node.type === "notebook") {
      return node.notebookId === notebookId;
    }

    if (node.type !== "folder" || !Array.isArray(node.children)) {
      return false;
    }

    return node.children.some((child) => folderContainsNotebookState(child, notebookId));
  }

  function folderMatchesStoredState(node, folderName, parentFolderId = "") {
    if (!node || typeof node !== "object" || node.type !== "folder") {
      return false;
    }

    const folderId = deriveFolderId(folderName, parentFolderId);
    return node.folderId === folderId || node.name === folderName;
  }

  function findStoredFolderPathState(nodes, folderPath, parentFolderId = "") {
    const normalizedPath = Array.isArray(folderPath)
      ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
      : [];
    if (normalizedPath.length === 0) {
      return null;
    }

    let currentNodes = Array.isArray(nodes) ? nodes : [];
    let currentParentFolderId = parentFolderId;
    let matchedFolder = null;

    for (const folderName of normalizedPath) {
      matchedFolder =
        currentNodes.find((node) => folderMatchesStoredState(node, folderName, currentParentFolderId)) ??
        null;
      if (!matchedFolder) {
        return null;
      }

      currentParentFolderId = deriveFolderId(folderName, currentParentFolderId);
      currentNodes = Array.isArray(matchedFolder.children) ? matchedFolder.children : [];
    }

    return matchedFolder;
  }

  function insertNotebookIntoStoredFolderPath(nodes, notebookNode, folderPath, parentFolderId = "") {
    const normalizedNodes = Array.isArray(nodes)
      ? nodes
          .map((node) => (node && typeof node === "object" ? node : null))
          .filter(Boolean)
      : [];
    const normalizedPath = Array.isArray(folderPath)
      ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
      : [];

    if (!notebookNode || normalizedPath.length === 0) {
      return {
        state: normalizedNodes,
        changed: false,
      };
    }

    const [folderName, ...remainingPath] = normalizedPath;
    const nextParentFolderId = deriveFolderId(folderName, parentFolderId);
    const folderIndex = normalizedNodes.findIndex((node) =>
      folderMatchesStoredState(node, folderName, parentFolderId)
    );
    const existingFolder = folderIndex >= 0 ? normalizedNodes[folderIndex] : null;
    const fallbackPolicy = defaultFolderPermissions(nextParentFolderId);

    let changed = folderIndex < 0;
    let folderState =
      existingFolder && existingFolder.type === "folder"
        ? {
            ...existingFolder,
            children: Array.isArray(existingFolder.children) ? [...existingFolder.children] : [],
          }
        : {
            ...createStoredFolderState(folderName, parentFolderId),
            canEdit: fallbackPolicy.canEdit,
            canDelete: fallbackPolicy.canDelete,
          };

    if (!folderState.open) {
      folderState.open = true;
      changed = true;
    }

    if (remainingPath.length > 0) {
      const childResult = insertNotebookIntoStoredFolderPath(
        folderState.children,
        notebookNode,
        remainingPath,
        nextParentFolderId
      );
      folderState.children = childResult.state;
      changed = changed || childResult.changed;
    } else if (!folderContainsNotebookState(folderState, notebookNode.notebookId)) {
      folderState.children = [...folderState.children, notebookNode];
      changed = true;
    }

    const nextNodes = [...normalizedNodes];
    if (folderIndex >= 0) {
      nextNodes[folderIndex] = folderState;
    } else {
      nextNodes.push(folderState);
    }

    return {
      state: nextNodes,
      changed,
    };
  }

  function ensureNotebookInRootFolderState(nodes, notebookId, folderName) {
    const folderId = deriveFolderId(folderName);
    const rootNodes = Array.isArray(nodes)
      ? nodes
          .map((node) => (node && typeof node === "object" ? node : null))
          .filter(Boolean)
      : [];

    const existingFolderIndex = rootNodes.findIndex(
      (node) => node.type === "folder" && (node.folderId === folderId || node.name === folderName)
    );

    if (
      existingFolderIndex >= 0 &&
      folderContainsNotebookState(rootNodes[existingFolderIndex], notebookId)
    ) {
      return {
        state: rootNodes,
        changed: false,
      };
    }

    const removal = removeNotebookFromStoredTree(rootNodes, notebookId);
    const notebookNode = removal.removed;
    if (!notebookNode) {
      return {
        state: rootNodes,
        changed: false,
      };
    }

    const nextNodes = removal.nodes;
    const targetFolderIndex = nextNodes.findIndex(
      (node) => node.type === "folder" && (node.folderId === folderId || node.name === folderName)
    );

    if (targetFolderIndex >= 0) {
      const targetFolder = nextNodes[targetFolderIndex];
      nextNodes[targetFolderIndex] = {
        ...targetFolder,
        open: true,
        children: [...(Array.isArray(targetFolder.children) ? targetFolder.children : []), notebookNode],
      };
    } else {
      nextNodes.push({
        ...createStoredFolderState(folderName),
        children: [notebookNode],
      });
    }

    return {
      state: nextNodes,
      changed: true,
    };
  }

  function ensureNotebookInFolderPathState(nodes, notebookId, folderPath) {
    const normalizedPath = Array.isArray(folderPath)
      ? folderPath.map((segment) => String(segment ?? "").trim()).filter(Boolean)
      : [];
    const rootNodes = Array.isArray(nodes)
      ? nodes
          .map((node) => (node && typeof node === "object" ? node : null))
          .filter(Boolean)
      : [];

    if (normalizedPath.length === 0) {
      return {
        state: rootNodes,
        changed: false,
      };
    }

    const existingFolder = findStoredFolderPathState(rootNodes, normalizedPath);
    if (existingFolder && folderContainsNotebookState(existingFolder, notebookId)) {
      return {
        state: rootNodes,
        changed: false,
      };
    }

    const removal = removeNotebookFromStoredTree(rootNodes, notebookId);
    const notebookNode = removal.removed;
    if (!notebookNode) {
      return {
        state: rootNodes,
        changed: false,
      };
    }

    return insertNotebookIntoStoredFolderPath(removal.nodes, notebookNode, normalizedPath);
  }

  function collectNotebookIdsFromStoredTree(nodes) {
    const notebookIds = [];

    const visit = (node) => {
      if (!node || typeof node !== "object") {
        return;
      }

      if (node.type === "notebook" && node.notebookId) {
        notebookIds.push(String(node.notebookId));
        return;
      }

      if (node.type === "folder" && Array.isArray(node.children)) {
        node.children.forEach((child) => visit(child));
      }
    };

    (Array.isArray(nodes) ? nodes : []).forEach((node) => visit(node));
    return notebookIds;
  }

  function migrateStoredNotebookTree(state) {
    let nextState = Array.isArray(state) ? state : [];
    let changed = false;

    const migrations = [
      {
        notebookId: "s3-smoke-test",
        folderPath: ["PoC Tests", "Smoke Tests", "Object Storage"],
      },
      {
        notebookId: "postgres-smoke-test",
        folderPath: ["PoC Tests", "Smoke Tests", "Relational"],
      },
      {
        notebookId: "postgres-oltp-write-test",
        folderPath: ["PoC Tests", "Smoke Tests", "Write Access"],
      },
      {
        notebookId: "postgres-oltp-olap-union-test",
        folderPath: ["PoC Tests", "SQL Functionalities"],
      },
      {
        notebookId: "postgres-oltp-s3-union-test",
        folderPath: ["PoC Tests", "SQL Functionalities"],
      },
      {
        notebookId: "pg-vs-s3-contest-oltp",
        folderPath: ["PoC Tests", "Performance Evaluation", "Single-Table Test"],
      },
      {
        notebookId: "pg-vs-s3-contest-s3",
        folderPath: ["PoC Tests", "Performance Evaluation", "Single-Table Test"],
      },
      {
        notebookId: "pg-vs-s3-contest-pg-native",
        folderPath: ["PoC Tests", "Performance Evaluation", "Single-Table Test"],
      },
      {
        notebookId: "pg-vs-s3-multi-table-oltp",
        folderPath: ["PoC Tests", "Performance Evaluation", "Multi-Table Test"],
      },
      {
        notebookId: "pg-vs-s3-multi-table-s3",
        folderPath: ["PoC Tests", "Performance Evaluation", "Multi-Table Test"],
      },
      {
        notebookId: "pg-vs-s3-multi-table-pg-native",
        folderPath: ["PoC Tests", "Performance Evaluation", "Multi-Table Test"],
      },
    ];

    for (const migration of migrations) {
      const result = ensureNotebookInFolderPathState(nextState, migration.notebookId, migration.folderPath);
      nextState = result.state;
      changed = changed || result.changed;
    }

    const obsoleteRootFolders = new Set(["Smoke Tests"]);
    const obsoleteRootNodes = nextState.filter(
      (node) =>
        node &&
        typeof node === "object" &&
        node.type === "folder" &&
        obsoleteRootFolders.has(String(node.name || "").trim())
    );
    if (obsoleteRootNodes.length) {
      collectNotebookIdsFromStoredTree(obsoleteRootNodes).forEach((notebookId) => {
        if (isLocalNotebookId(notebookId)) {
          deleteStoredNotebookState(notebookId);
        }
      });
      nextState = nextState.filter(
        (node) =>
          !(
            node &&
            typeof node === "object" &&
            node.type === "folder" &&
            obsoleteRootFolders.has(String(node.name || "").trim())
          )
      );
      changed = true;
    }

    return {
      state: nextState,
      changed,
    };
  }

  function slugifyFolderSegment(name) {
    return String(name ?? "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "-");
  }

  function deriveFolderId(name, parentFolderId = "") {
    const slug = slugifyFolderSegment(name);
    if (!slug) {
      return parentFolderId || "";
    }

    return parentFolderId ? `${parentFolderId}-${slug}` : slug;
  }

  function isProtectedNotebookFolderId(folderId = "") {
    const normalizedFolderId = String(folderId ?? "").trim();
    return (
      normalizedFolderId === "poc-tests" ||
      normalizedFolderId.startsWith("poc-tests-") ||
      normalizedFolderId === "smoke-tests" ||
      normalizedFolderId.startsWith("smoke-tests-") ||
      normalizedFolderId === "performance-evaluation" ||
      normalizedFolderId.startsWith("performance-evaluation-")
    );
  }

  function defaultFolderPermissions(folderId = "") {
    if (isProtectedNotebookFolderId(folderId)) {
      return {
        canEdit: false,
        canDelete: false,
      };
    }

    return {
      canEdit: true,
      canDelete: true,
    };
  }

  return {
    defaultFolderPermissions,
    deriveFolderId,
    ensureNotebookInFolderPathState,
    readStoredNotebookTree,
    removeNotebookFromStoredTree,
    writeStoredNotebookTree,
  };
}
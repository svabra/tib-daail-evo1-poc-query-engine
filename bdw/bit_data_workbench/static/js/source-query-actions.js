import { sourceQueryDescriptor, sourceQuerySql } from "./source-metadata-utils.js";

export function createSourceQueryActions(helpers) {
  const {
    createNotebook,
    createSourceQueryCellState,
    defaultNotebookCreateTarget,
    getActiveEditableNotebookId,
    getCurrentSidebarMode,
    getNotebookMetadata,
    getNotebookTreeRoot,
    isLocalWorkspaceSourceObject = () => false,
    refreshSidebar,
    requestCellRun,
    selectSourceObject,
    setActiveCellId,
    setNotebookCells,
    setSelectedSourceObjectState = () => {},
  } = helpers;

  async function loadSourceObjectFields(sourceObjectRoot, { alertOnFailure = true } = {}) {
    try {
      return await selectSourceObject(sourceObjectRoot);
    } catch (error) {
      console.error("Failed to load source object fields.", error);
      if (alertOnFailure) {
        window.alert("The fields for this source object could not be loaded.");
      }
      return null;
    }
  }

  async function insertSourceQueryIntoCurrentNotebook(sourceObjectRoot, { runImmediately = false } = {}) {
    const sourceDescriptor = sourceQueryDescriptor(sourceObjectRoot);
    const notebookId = getActiveEditableNotebookId();
    if (!sourceDescriptor || !notebookId) {
      return false;
    }

    const fields = await loadFieldsForSourceQuery(sourceObjectRoot);
    if (!fields) {
      return null;
    }

    const metadata = getNotebookMetadata(notebookId);
    const nextCell = createSourceQueryCellState(sourceDescriptor, fields);
    setActiveCellId(nextCell.cellId);
    setNotebookCells(notebookId, [...metadata.cells, nextCell], { rerender: true });
    if (runImmediately) {
      requestCellRun(nextCell.cellId);
    }
    return true;
  }

  function querySourceInCurrentNotebook(sourceObjectRoot) {
    return insertSourceQueryIntoCurrentNotebook(sourceObjectRoot);
  }

  function viewSourceData(sourceObjectRoot) {
    return insertSourceQueryIntoCurrentNotebook(sourceObjectRoot, { runImmediately: true });
  }

  async function loadFieldsForSourceQuery(sourceObjectRoot, options = {}) {
    if (isLocalWorkspaceSourceObject(sourceObjectRoot)) {
      setSelectedSourceObjectState(sourceObjectRoot);
      return [];
    }
    return loadSourceObjectFields(sourceObjectRoot, options);
  }

  function updateSourceQueryCellWhenUnchanged(notebookId, cellId, expectedSql, nextSql) {
    if (!notebookId || !cellId || !nextSql || nextSql === expectedSql) {
      return false;
    }

    const metadata = getNotebookMetadata(notebookId);
    const cells = Array.isArray(metadata?.cells) ? metadata.cells : [];
    const currentCell = cells.find((cell) => cell.cellId === cellId);
    if (!currentCell || currentCell.sql !== expectedSql) {
      return false;
    }

    setNotebookCells(
      notebookId,
      cells.map((cell) => (cell.cellId === cellId ? { ...cell, sql: nextSql } : cell)),
      { rerender: true }
    );
    return true;
  }

  function enrichSourceQueryNotebook(notebookId, cellId, sourceObjectRoot, sourceDescriptor, initialSql) {
    loadFieldsForSourceQuery(sourceObjectRoot, { alertOnFailure: false })
      .then((fields) => {
        if (!fields?.length) {
          return;
        }

        updateSourceQueryCellWhenUnchanged(
          notebookId,
          cellId,
          initialSql,
          sourceQuerySql(sourceDescriptor.relation, fields)
        );
      })
      .catch((error) => {
        console.error("Failed to enrich the source query with explicit fields.", error);
      });
  }

  async function querySourceInNewNotebook(sourceObjectRoot) {
    const sourceDescriptor = sourceQueryDescriptor(sourceObjectRoot);
    if (!sourceDescriptor) {
      return null;
    }

    if (getCurrentSidebarMode() !== "notebook" || !getNotebookTreeRoot()) {
      await refreshSidebar("notebook", {
        force: true,
        forceNotebookTree: true,
      });
    }

    const targetContainer = defaultNotebookCreateTarget();
    if (!targetContainer) {
      return null;
    }

    setSelectedSourceObjectState(sourceObjectRoot);
    const nextCell = createSourceQueryCellState(sourceDescriptor, []);
    setActiveCellId(nextCell.cellId);
    const notebookId = await createNotebook(targetContainer, {
      cells: [nextCell],
    });
    enrichSourceQueryNotebook(notebookId, nextCell.cellId, sourceObjectRoot, sourceDescriptor, nextCell.sql);
    return notebookId;
  }

  return {
    querySourceInCurrentNotebook,
    querySourceInNewNotebook,
    viewSourceData,
  };
}

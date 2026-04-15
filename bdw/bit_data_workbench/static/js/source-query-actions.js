import { sourceQueryDescriptor } from "./source-metadata-utils.js";

export function createSourceQueryActions(helpers) {
  const {
    createNotebook,
    createSourceQueryCellState,
    defaultNotebookCreateTarget,
    getActiveEditableNotebookId,
    getCurrentSidebarMode,
    getNotebookMetadata,
    getNotebookTreeRoot,
    refreshSidebar,
    requestCellRun,
    selectSourceObject,
    setActiveCellId,
    setNotebookCells,
  } = helpers;

  async function loadSourceObjectFields(sourceObjectRoot) {
    try {
      return await selectSourceObject(sourceObjectRoot);
    } catch (error) {
      console.error("Failed to load source object fields.", error);
      window.alert("The fields for this source object could not be loaded.");
      return null;
    }
  }

  async function insertSourceQueryIntoCurrentNotebook(sourceObjectRoot, { runImmediately = false } = {}) {
    const sourceDescriptor = sourceQueryDescriptor(sourceObjectRoot);
    const notebookId = getActiveEditableNotebookId();
    if (!sourceDescriptor || !notebookId) {
      return false;
    }

    const fields = await loadSourceObjectFields(sourceObjectRoot);
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

  async function querySourceInNewNotebook(sourceObjectRoot) {
    const sourceDescriptor = sourceQueryDescriptor(sourceObjectRoot);
    if (!sourceDescriptor) {
      return null;
    }

    if (getCurrentSidebarMode() !== "notebook" || !getNotebookTreeRoot()) {
      await refreshSidebar("notebook");
    }

    const targetContainer = defaultNotebookCreateTarget();
    if (!targetContainer) {
      return null;
    }

    const fields = await loadSourceObjectFields(sourceObjectRoot);
    if (!fields) {
      return null;
    }

    const nextCell = createSourceQueryCellState(sourceDescriptor, fields);
    setActiveCellId(nextCell.cellId);
    return createNotebook(targetContainer, {
      cells: [nextCell],
    });
  }

  return {
    querySourceInCurrentNotebook,
    querySourceInNewNotebook,
    viewSourceData,
  };
}
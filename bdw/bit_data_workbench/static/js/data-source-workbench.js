import { initializeRemoteSourceCatalog } from "./source-catalog.js";


export function initializeDataSourceWorkbenchControls(scope = document) {
  scope.querySelectorAll("[data-data-source-management-page]").forEach((root) => {
    if (!root.dataset.selectedSourceId) initializeRemoteSourceCatalog(root);
  });
}

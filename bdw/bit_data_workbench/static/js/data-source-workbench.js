const VIEW_STORAGE_KEY = "bdw.dataSources.viewMode";

function normalize(value = "") {
  return String(value || "").trim().toLocaleLowerCase();
}

function applyFilters(root) {
  const query = normalize(root.querySelector("[data-source-filter-query]")?.value);
  const technology = normalize(root.querySelector("[data-source-filter-technology]")?.value);
  const cards = Array.from(root.querySelectorAll("[data-source-filter-card]"));
  let visibleCount = 0;

  cards.forEach((card) => {
    const matchesQuery = !query || normalize(card.dataset.sourceSearch).includes(query);
    const matchesTechnology =
      !technology || normalize(card.dataset.sourceTechnology) === technology;
    const visible = matchesQuery && matchesTechnology;
    card.hidden = !visible;
    visibleCount += visible ? 1 : 0;
  });

  const status = root.querySelector("[data-source-filter-status]");
  if (status) {
    status.textContent = `${visibleCount} of ${cards.length} data sources shown`;
  }
  const empty = root.querySelector("[data-source-filter-empty]");
  if (empty) {
    empty.hidden = visibleCount !== 0;
  }
}

function setViewMode(root, requestedMode) {
  const viewMode = requestedMode === "list" ? "list" : "cards";
  root.dataset.sourceViewMode = viewMode;
  root.querySelectorAll("[data-source-view-mode]").forEach((button) => {
    const active = button.dataset.sourceViewMode === viewMode;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
  try {
    window.sessionStorage.setItem(VIEW_STORAGE_KEY, viewMode);
  } catch {
    // The view remains usable when browser storage is unavailable.
  }
}

function storedViewMode() {
  try {
    return window.sessionStorage.getItem(VIEW_STORAGE_KEY) || "cards";
  } catch {
    return "cards";
  }
}

export function initializeDataSourceWorkbenchControls(scope = document) {
  scope.querySelectorAll("[data-data-source-management-page]").forEach((root) => {
    if (root.dataset.sourceControlsInitialized === "true") {
      applyFilters(root);
      return;
    }
    root.dataset.sourceControlsInitialized = "true";
    setViewMode(root, storedViewMode());
    applyFilters(root);

    root.addEventListener("input", (event) => {
      if (event.target.closest("[data-source-filter-query]")) {
        applyFilters(root);
      }
    });
    root.addEventListener("change", (event) => {
      if (event.target.closest("[data-source-filter-technology]")) {
        applyFilters(root);
      }
    });
    root.addEventListener("click", (event) => {
      const viewButton = event.target.closest("[data-source-view-mode]");
      if (viewButton) {
        event.preventDefault();
        setViewMode(root, viewButton.dataset.sourceViewMode);
        return;
      }
      const resetButton = event.target.closest("[data-source-filter-reset]");
      if (!resetButton) {
        return;
      }
      event.preventDefault();
      const queryInput = root.querySelector("[data-source-filter-query]");
      const technologySelect = root.querySelector("[data-source-filter-technology]");
      if (queryInput) {
        queryInput.value = "";
      }
      if (technologySelect) {
        technologySelect.value = "";
      }
      applyFilters(root);
      queryInput?.focus();
    });
  });
}

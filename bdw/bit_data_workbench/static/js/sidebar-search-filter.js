export function createSidebarSearchFilter({ dataSourcesSection, notebookSection, sourceLabelsForIds }) {
  function updateNotebookSearchableItem(link, metadata) {
    link.dataset.searchableItem = [
      metadata.title,
      metadata.summary,
      ...sourceLabelsForIds(metadata.dataSources),
      ...metadata.tags,
    ]
      .filter(Boolean)
      .join(" ");
  }

  function applySidebarSearchFilter() {
    const search = document.querySelector("[data-sidebar-search]");
    const sidebar = document.getElementById("sidebar");
    if (!search || !sidebar) {
      return;
    }

    const term = search.value.trim().toLowerCase();
    const matches = (element) => {
      const haystack = (element?.dataset.searchableItem ?? "").toLowerCase();
      return !term || haystack.includes(term);
    };

    sidebar.querySelectorAll("[data-open-ingestion-runbook]").forEach((button) => {
      button.dataset.searchHidden = matches(button) ? "false" : "true";
    });

    const runbookFolders = Array.from(sidebar.querySelectorAll("[data-runbook-folder]")).reverse();
    for (const folder of runbookFolders) {
      const selfMatches = matches(folder.querySelector(":scope > summary"));
      const visibleChildren = folder.querySelector(
        ":scope > [data-runbook-children] > :not([data-search-hidden='true'])"
      );
      const visible = !term || selfMatches || Boolean(visibleChildren);
      folder.dataset.searchHidden = visible ? "false" : "true";
      if (term && visibleChildren) {
        folder.open = true;
      }
    }

    sidebar.querySelectorAll("[data-draggable-notebook]").forEach((link) => {
      link.dataset.searchHidden = matches(link) ? "false" : "true";
    });

    const notebookFolders = Array.from(sidebar.querySelectorAll("[data-tree-folder]")).reverse();
    for (const folder of notebookFolders) {
      const selfMatches = matches(folder.querySelector(":scope > summary"));
      const visibleChildren = folder.querySelector(
        ":scope > [data-tree-children] > :not([data-search-hidden='true'])"
      );
      const visible = !term || selfMatches || Boolean(visibleChildren);
      folder.dataset.searchHidden = visible ? "false" : "true";
      if (term && visibleChildren) {
        folder.open = true;
      }
    }

    sidebar.querySelectorAll(".source-object").forEach((item) => {
      item.dataset.searchHidden = matches(item) ? "false" : "true";
    });

    const sourceSchemas = Array.from(sidebar.querySelectorAll("[data-source-schema]")).reverse();
    for (const schema of sourceSchemas) {
      const selfMatches = matches(schema.querySelector(":scope > summary"));
      const visibleChildren = schema.querySelector(
        ":scope > .source-object-list > :not([data-search-hidden='true'])"
      );
      const visible = !term || selfMatches || Boolean(visibleChildren);
      schema.dataset.searchHidden = visible ? "false" : "true";
      if (term && visibleChildren) {
        schema.open = true;
      }
    }

    const sourceCatalogs = Array.from(sidebar.querySelectorAll("[data-source-catalog]")).reverse();
    for (const catalog of sourceCatalogs) {
      const selfMatches = matches(catalog.querySelector(":scope > summary"));
      const visibleChildren = catalog.querySelector(
        ":scope > :not(summary):not([data-search-hidden='true'])"
      );
      const visible = !term || selfMatches || Boolean(visibleChildren);
      catalog.dataset.searchHidden = visible ? "false" : "true";
      if (term && visibleChildren) {
        catalog.open = true;
      }
    }

    if (term && sidebar.querySelector("[data-draggable-notebook][data-search-hidden='false']")) {
      notebookSection()?.setAttribute("open", "");
    }

    const ingestionRunbookSection = sidebar.querySelector("[data-ingestion-runbook-section]");
    if (term && sidebar.querySelector("[data-open-ingestion-runbook][data-search-hidden='false']")) {
      ingestionRunbookSection?.setAttribute("open", "");
    }

    if (term && sidebar.querySelector("[data-source-catalog][data-search-hidden='false']")) {
      dataSourcesSection()?.setAttribute("open", "");
    }
  }

  function initializeSidebarSearch() {
    const search = document.querySelector("[data-sidebar-search]");
    const sidebar = document.getElementById("sidebar");
    if (!search || !sidebar || search.dataset.bound === "true") {
      return;
    }

    search.dataset.bound = "true";
    search.addEventListener("input", () => applySidebarSearchFilter());
    applySidebarSearchFilter();
  }

  return {
    applySidebarSearchFilter,
    initializeSidebarSearch,
    updateNotebookSearchableItem,
  };
}
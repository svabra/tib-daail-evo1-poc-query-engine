import {
  actionButtonMarkup,
  detailCardMarkup,
  explorerEmptyStateMarkup,
  fieldListMarkup,
  publicationBadgeMarkup,
  publicationLinksMarkup,
  sourceObjectElement,
} from "./utils.js";

export function createPostgresDataSourceExplorer(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    openDataProductPublishDialog,
    querySourceInCurrentNotebook,
    querySourceInNewNotebook,
    showMessageDialog,
    viewSourceData,
  } = helpers;

  const stateByRoot = new WeakMap();

  function explorerState(root) {
    return stateByRoot.get(root) ?? null;
  }

  function navigationRoot(root) {
    return root.querySelector("[data-data-source-explorer-navigation]");
  }

  function detailRoot(root) {
    return root.querySelector("[data-data-source-explorer-detail]");
  }

  function allRelations(state) {
    return state.schemas.flatMap((schema) =>
      (schema.objects || []).map((object) => ({
        ...object,
        schemaName: schema.name,
      }))
    );
  }

  function relationById(state, relation) {
    return allRelations(state).find(
      (object) => String(object.relation || "").trim() === relation
    );
  }

  async function loadFields(state, relation) {
    const normalizedRelation = String(relation || "").trim();
    if (!normalizedRelation) {
      return [];
    }
    if (state.fieldCache.has(normalizedRelation)) {
      return state.fieldCache.get(normalizedRelation) || [];
    }
    const payload = await fetchJsonOrThrow(
      `/api/source-object-fields?relation=${encodeURIComponent(normalizedRelation)}`
    );
    const fields = Array.isArray(payload?.fields) ? payload.fields : [];
    state.fieldCache.set(normalizedRelation, fields);
    return fields;
  }

  function selectedDescriptorElement(state) {
    if (!state.selectedRelation) {
      return null;
    }
    const selectedObject = relationById(state, state.selectedRelation);
    if (!selectedObject) {
      return null;
    }
    return sourceObjectElement({
      relation: selectedObject.relation,
      name: selectedObject.name,
      displayName: selectedObject.displayName,
      kind: selectedObject.kind,
      sourceOptionId: state.selectedSourceId,
    });
  }

  function renderNavigation(root) {
    const state = explorerState(root);
    const navigation = navigationRoot(root);
    if (!state || !(navigation instanceof Element)) {
      return;
    }

    if (!state.schemas.length) {
      navigation.innerHTML = explorerEmptyStateMarkup(
        "No schemas are available for this PostgreSQL source right now.",
        {},
        escapeHtml
      );
      return;
    }

    navigation.innerHTML = `
      <div class="data-source-explorer-tree">
        ${state.schemas
          .map(
            (schema) => `
              <details class="data-source-explorer-group" open>
                <summary>
                  <span>${escapeHtml(schema.label || schema.name || "")}</span>
                  <small>${escapeHtml(String(schema.objectCount || 0))} object(s)</small>
                </summary>
                <div class="data-source-explorer-group-body">
                  ${(schema.objects || [])
                    .map(
                      (object) => `
                        <button
                          type="button"
                          class="data-source-explorer-object${state.selectedRelation === object.relation ? " is-active" : ""}"
                          data-data-source-explorer-postgres-object="${escapeHtml(object.relation || "")}"
                        >
                          <span class="data-source-explorer-object-copy">
                            <span class="data-source-explorer-object-title-row">
                              <strong>${escapeHtml(object.displayName || object.name || "")}</strong>
                              ${publicationBadgeMarkup(object.publishedDataProducts, escapeHtml)}
                            </span>
                            <span>${escapeHtml(String(object.kind || "table").toUpperCase())}</span>
                          </span>
                        </button>
                      `
                    )
                    .join("")}
                </div>
              </details>
            `
          )
          .join("")}
      </div>
    `;
  }

  function renderDetail(root) {
    const state = explorerState(root);
    const detail = detailRoot(root);
    if (!state || !(detail instanceof Element)) {
      return;
    }

    const selectedObject = relationById(state, state.selectedRelation);
    if (!selectedObject) {
      detail.innerHTML = explorerEmptyStateMarkup(
        "Select a table or view to inspect its fields and open the data in a notebook.",
        {},
        escapeHtml
      );
      return;
    }

    const fields = state.fieldCache.get(state.selectedRelation) || [];
    detail.innerHTML = detailCardMarkup(
      {
        eyebrow: `${selectedObject.schemaName} • ${String(selectedObject.kind || "table").toUpperCase()}`,
        title: selectedObject.displayName || selectedObject.name || "Selected relation",
        copy: `Browse ${selectedObject.relation} and hand it off into notebook-driven query flows.`,
        actions: [
          actionButtonMarkup("View Data", "view", escapeHtml),
          actionButtonMarkup("Query In Current Notebook", "query-current", escapeHtml),
          actionButtonMarkup("Query In New Notebook", "query-new", escapeHtml),
          actionButtonMarkup("Create Data Product ...", "create-data-product", escapeHtml),
        ].join(""),
        body: `
          ${publicationLinksMarkup(selectedObject.publishedDataProducts, escapeHtml)}
          ${fieldListMarkup(fields, escapeHtml)}
        `,
      },
      escapeHtml
    );
  }

  async function render(root) {
    renderNavigation(root);
    renderDetail(root);
  }

  async function selectRelation(root, relation) {
    const state = explorerState(root);
    if (!state) {
      return;
    }
    state.selectedRelation = String(relation || "").trim();
    await loadFields(state, state.selectedRelation);
    await render(root);
  }

  async function initialize(root) {
    const state = {
      selectedSourceId: String(root.dataset.selectedSourceId || "").trim(),
      browseSourceId: String(root.dataset.browseSourceId || "").trim(),
      schemas: [],
      selectedRelation: "",
      fieldCache: new Map(),
    };
    stateByRoot.set(root, state);

    const navigation = navigationRoot(root);
    if (navigation instanceof Element) {
      navigation.innerHTML = explorerEmptyStateMarkup(
        "Loading PostgreSQL relations...",
        {},
        escapeHtml
      );
    }

    try {
      const payload = await fetchJsonOrThrow(
        `/api/data-sources/${encodeURIComponent(state.browseSourceId)}/explorer`
      );
      state.schemas = Array.isArray(payload?.schemas) ? payload.schemas : [];
      state.selectedRelation =
        String(payload?.defaultRelation || "").trim() ||
        String(allRelations(state)[0]?.relation || "").trim();
      if (state.selectedRelation) {
        await loadFields(state, state.selectedRelation);
      }
      await render(root);
    } catch (error) {
      const detail = detailRoot(root);
      if (navigation instanceof Element) {
        navigation.innerHTML = explorerEmptyStateMarkup(
          error instanceof Error
            ? error.message
            : "The PostgreSQL explorer could not be loaded.",
          { tone: "danger" },
          escapeHtml
        );
      }
      if (detail instanceof Element) {
        detail.innerHTML = explorerEmptyStateMarkup(
          "The PostgreSQL explorer is unavailable right now.",
          { tone: "danger" },
          escapeHtml
        );
      }
    }
  }

  async function handleClick(event, root) {
    const relationButton = event.target.closest(
      "[data-data-source-explorer-postgres-object]"
    );
    if (relationButton && root.contains(relationButton)) {
      event.preventDefault();
      event.stopPropagation();
      await selectRelation(
        root,
        relationButton.dataset.dataSourceExplorerPostgresObject || ""
      );
      return true;
    }

    const actionButton = event.target.closest("[data-data-source-explorer-action]");
    if (!(actionButton && root.contains(actionButton))) {
      return false;
    }

    event.preventDefault();
    event.stopPropagation();

    const selectedElement = selectedDescriptorElement(explorerState(root));
    if (!(selectedElement instanceof Element)) {
      return true;
    }

    const action = String(
      actionButton.dataset.dataSourceExplorerAction || ""
    ).trim();
    if (action === "view") {
      const viewed = await viewSourceData(selectedElement);
      if (viewed === false) {
        await showMessageDialog({
          title: "Notebook required",
          copy: "Open an editable notebook first, or use 'Query In New Notebook'.",
        });
      }
      return true;
    }

    if (action === "query-current") {
      const inserted = await querySourceInCurrentNotebook(selectedElement);
      if (inserted === false) {
        await showMessageDialog({
          title: "Notebook required",
          copy: "Open an editable notebook first, or use 'Query In New Notebook'.",
        });
      }
      return true;
    }

    if (action === "query-new") {
      await querySourceInNewNotebook(selectedElement);
      return true;
    }

    if (action === "create-data-product") {
      await openDataProductPublishDialog({
        sourceObjectRoot: selectedElement,
      });
      return true;
    }

    return false;
  }

  return {
    initialize,
    handleClick,
  };
}

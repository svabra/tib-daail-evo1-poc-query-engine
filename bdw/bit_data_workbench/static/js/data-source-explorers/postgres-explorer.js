import {
  actionButtonMarkup,
  detailCardMarkup,
  explorerEmptyStateMarkup,
  fieldListMarkup,
  publicationLinksMarkup,
  sourceActionMenuMarkup,
  sourceObjectElement,
  sourceObjectRowMarkup,
  sourcePublicationBadgeMarkup,
  sourceSchemaDetailsMarkup,
} from "./utils.js";

export function createPostgresDataSourceExplorer(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    copySourceQueryPath,
    openDataProductPublishDialog,
    querySourceInCurrentNotebook,
    querySourceInNewNotebook,
    showMessageDialog,
    viewSourceData,
    downloadSourceObjectDdl,
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

  function publicationMenuItems(publishedProducts) {
    const normalizedProducts = Array.isArray(publishedProducts) ? publishedProducts : [];
    return normalizedProducts.map((product) => ({
      label: normalizedProducts.length === 1
        ? "Open Data Product"
        : `Open Data Product - ${product.title || product.slug || "source"}`,
      href: product.documentationPath || "",
      title: "Open the published Data Product page",
    }));
  }

  function relationActionMenuMarkup(object) {
    const publishedItems = publicationMenuItems(object.publishedDataProducts);
    return sourceActionMenuMarkup(
      [
        ...publishedItems,
        publishedItems.length ? "separator" : null,
        {
          label: "View Data",
          action: "view",
          attrs: { "data-view-source-data": true },
          title: "Insert and run a query with all fields in the current notebook",
        },
        {
          label: "Query in current notebook",
          action: "query-current",
          attrs: { "data-query-source-current": true },
          title: "Insert a query into the current notebook",
        },
        {
          label: "Query in new notebook",
          action: "query-new",
          attrs: { "data-query-source-new": true },
          title: "Create a new notebook with this query",
        },
        {
          label: "Copy query path",
          action: "copy-query-path",
          attrs: { "data-copy-query-path": true },
          title: "Copy the SQL query path for this source",
        },
        {
          label: "Create data product ...",
          action: "create-data-product",
          attrs: { "data-create-data-product": true },
          title: "Publish this source as a managed data product",
        },
        {
          label: "Download DDL",
          action: "download-ddl",
          attrs: { "data-download-source-ddl": true },
          title: "Download DDL for this source",
        },
      ],
      escapeHtml
    );
  }

  function relationRowMarkup(object, state) {
    const displayName = object.displayName || object.name || "";
    const relation = object.relation || "";
    const kind = object.kind || "table";
    return sourceObjectRowMarkup(
      {
        kind,
        displayName,
        title: `${displayName}${relation ? ` | Query path: ${relation}` : ""}`,
        searchable: `${displayName} ${object.name || ""} ${relation} ${kind}`,
        selected: state.selectedRelation === relation,
        attrs: {
          "data-source-object": true,
          "data-source-object-kind": kind,
          "data-source-object-name": object.name || "",
          "data-source-object-display-name": displayName,
          "data-source-object-relation": relation,
          "data-source-option-id": state.selectedSourceId,
          "data-data-source-explorer-postgres-object": relation,
          "data-published-data-products": JSON.stringify(object.publishedDataProducts || []),
        },
        meta: `
          ${sourcePublicationBadgeMarkup(object.publishedDataProducts, escapeHtml)}
          <small class="source-query-path-label" title="${escapeHtml(`Query path: ${relation}`)}">${escapeHtml(relation)}</small>
          <small>${escapeHtml(String(kind).toUpperCase())}</small>
        `,
        actions: relationActionMenuMarkup(object),
      },
      escapeHtml
    );
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
      <div class="source-tree data-source-explorer-source-tree">
        ${state.schemas
          .map(
            (schema) =>
              sourceSchemaDetailsMarkup(
                {
                  label: schema.label || schema.name || "",
                  searchable: `${schema.label || ""} ${schema.name || ""}`,
                  attrs: {
                    "data-source-schema": true,
                    "data-source-schema-key": `${state.selectedSourceId || state.browseSourceId}::${schema.name || ""}`,
                  },
                  meta: `<small>${escapeHtml(String(schema.objectCount || 0))} object(s)</small>`,
                  children: `
                    <ul class="source-object-list">
                      ${(schema.objects || []).map((object) => relationRowMarkup(object, state)).join("")}
                    </ul>
                  `,
                },
                escapeHtml
              )
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
          actionButtonMarkup("Copy query path", "copy-query-path", escapeHtml),
          actionButtonMarkup("Create Data Product ...", "create-data-product", escapeHtml),
          actionButtonMarkup("Download DDL", "download-ddl", escapeHtml),
        ].join(""),
        body: `
          ${publicationLinksMarkup(selectedObject.publishedDataProducts, escapeHtml)}
          <ul class="sidebar-source-field-list">
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Query path</span></span>
              <span class="sidebar-source-field-type">${escapeHtml(selectedObject.relation || "")}</span>
            </li>
          </ul>
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

  async function selectRelation(root, relation, { renderAfter = true } = {}) {
    const state = explorerState(root);
    if (!state) {
      return;
    }
    state.selectedRelation = String(relation || "").trim();
    await loadFields(state, state.selectedRelation);
    if (renderAfter) {
      await render(root);
    }
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
    const actionButton = event.target.closest("[data-data-source-explorer-action]");
    if (actionButton && root.contains(actionButton)) {
      event.preventDefault();
      event.stopPropagation();

      const actionRelation = actionButton.closest("[data-data-source-explorer-postgres-object]");
      if (actionRelation && root.contains(actionRelation)) {
        await selectRelation(
          root,
          actionRelation.dataset.dataSourceExplorerPostgresObject || "",
          { renderAfter: true }
        );
      }

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

      if (action === "copy-query-path") {
        if ((await copySourceQueryPath?.(selectedElement)) === false) {
          await showMessageDialog({
            title: "Query path unavailable",
            copy: "This PostgreSQL relation does not expose a query path.",
          });
        }
        return true;
      }

      if (action === "create-data-product") {
        await openDataProductPublishDialog({
          sourceObjectRoot: selectedElement,
        });
        return true;
      }

      if (action === "download-ddl") {
        const downloaded = await downloadSourceObjectDdl(selectedElement);
        if (downloaded === false) {
          await showMessageDialog({
            title: "DDL download unavailable",
            copy: "The selected PostgreSQL relation does not expose DDL metadata.",
          });
        }
        return true;
      }

      return false;
    }

    if (event.target.closest("[data-source-action-menu]")) {
      return false;
    }

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

    return false;
  }

  return {
    initialize,
    handleClick,
  };
}

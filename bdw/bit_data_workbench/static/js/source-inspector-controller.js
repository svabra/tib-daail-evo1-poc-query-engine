export function createSourceInspectorController(helpers) {
  const {
    isLocalWorkspaceSourceObject,
    loadLocalWorkspaceSourceFields,
    normalizeSourceObjectFields,
    renderSourceInspector,
    renderSourceInspectorError,
    renderSourceInspectorLoading,
    renderSourceInspectorMarkup,
    sourceObjectNodes,
  } = helpers;

  let activeSourceObjectRelation = null;
  const activeSourceObjectRelations = new WeakMap();
  const sourceObjectFieldCache = new Map();
  const sourceObjectFieldRequests = new Map();

  function sourceBrowserScope(sourceObjectRoot = null) {
    return sourceObjectRoot?.closest?.("[data-source-browser-scope]") || document;
  }

  function sourceBrowserScopes() {
    const scopes = Array.from(document.querySelectorAll("[data-source-browser-scope]"));
    return scopes.length ? scopes : [document];
  }

  function sourceObjectRelation(sourceObjectRoot) {
    if (!(sourceObjectRoot instanceof Element)) {
      return "";
    }

    return sourceObjectRoot.dataset.sourceObjectRelation?.trim() || "";
  }

  function sourceObjectFieldCacheKey(sourceObjectRoot) {
    return sourceObjectRelation(sourceObjectRoot);
  }

  function clearSourceObjectFieldCacheForRelations(relations = []) {
    if (!Array.isArray(relations) || !relations.length) {
      sourceObjectFieldCache.clear();
      sourceObjectFieldRequests.clear();
      return;
    }

    relations.forEach((relation) => {
      const normalizedRelation = String(relation || "").trim();
      if (!normalizedRelation) {
        return;
      }
      sourceObjectFieldCache.delete(normalizedRelation);
      sourceObjectFieldRequests.delete(normalizedRelation);
    });
  }

  function getActiveSourceObjectRelation(scopeRoot = null) {
    if (scopeRoot && activeSourceObjectRelations.has(scopeRoot)) {
      return activeSourceObjectRelations.get(scopeRoot) || null;
    }
    return activeSourceObjectRelation;
  }

  function setSourceObjectLoadingState(sourceObjectRoot, loading) {
    if (!(sourceObjectRoot instanceof Element)) {
      return;
    }

    sourceObjectRoot.classList.toggle("is-loading", loading);
    sourceObjectRoot.setAttribute("aria-busy", loading ? "true" : "false");
  }

  function setSelectedSourceObjectState(sourceObjectRoot = null) {
    if (!(sourceObjectRoot instanceof Element)) {
      activeSourceObjectRelation = null;
      sourceBrowserScopes().forEach((scopeRoot) => {
        activeSourceObjectRelations.delete(scopeRoot);
        sourceObjectNodes(scopeRoot).forEach((item) => {
          item.classList.remove("is-selected");
          item.setAttribute("aria-selected", "false");
          setSourceObjectLoadingState(item, false);
        });
        renderSourceInspectorMarkup("", true, scopeRoot);
      });
      return;
    }

    const scopeRoot = sourceBrowserScope(sourceObjectRoot);
    const selectedRelation = sourceObjectRoot?.dataset.sourceObjectRelation?.trim() || null;
    activeSourceObjectRelation = selectedRelation;
    if (selectedRelation) {
      activeSourceObjectRelations.set(scopeRoot, selectedRelation);
    } else {
      activeSourceObjectRelations.delete(scopeRoot);
    }

    sourceObjectNodes(scopeRoot).forEach((item) => {
      const isSelected = item === sourceObjectRoot;
      item.classList.toggle("is-selected", isSelected);
      item.setAttribute("aria-selected", isSelected ? "true" : "false");
      if (!isSelected) {
        setSourceObjectLoadingState(item, false);
      }
    });

  }

  async function fetchSourceObjectFields(relation) {
    const response = await window.fetch(
      `/api/source-object-fields?relation=${encodeURIComponent(relation)}`,
      {
        headers: {
          Accept: "application/json",
        },
      }
    );

    if (!response.ok) {
      throw new Error("The fields could not be loaded for this source object.");
    }

    const payload = await response.json();
    return normalizeSourceObjectFields(payload?.fields ?? []);
  }

  async function loadSourceObjectFields(sourceObjectRoot, { renderLoading = true } = {}) {
    const relation = sourceObjectFieldCacheKey(sourceObjectRoot);
    const scopeRoot = sourceBrowserScope(sourceObjectRoot);
    if (!relation) {
      return [];
    }

    if (isLocalWorkspaceSourceObject(sourceObjectRoot)) {
      if (sourceObjectFieldCache.has(relation)) {
        const fields = sourceObjectFieldCache.get(relation) ?? [];
        if (getActiveSourceObjectRelation(scopeRoot) === relation) {
          renderSourceInspector(sourceObjectRoot, fields);
        }
        return fields;
      }

      if (renderLoading && getActiveSourceObjectRelation(scopeRoot) === relation) {
        setSourceObjectLoadingState(sourceObjectRoot, true);
        renderSourceInspectorLoading(sourceObjectRoot);
      }

      let pendingRequest = sourceObjectFieldRequests.get(relation);
      if (!pendingRequest) {
        pendingRequest = loadLocalWorkspaceSourceFields(sourceObjectRoot)
          .then((fields) => {
            const normalizedFields = normalizeSourceObjectFields(fields);
            sourceObjectFieldCache.set(relation, normalizedFields);
            return normalizedFields;
          })
          .finally(() => {
            sourceObjectFieldRequests.delete(relation);
          });
        sourceObjectFieldRequests.set(relation, pendingRequest);
      }

      try {
        const fields = await pendingRequest;
        if (getActiveSourceObjectRelation(scopeRoot) === relation) {
          renderSourceInspector(sourceObjectRoot, fields);
        }
        return fields;
      } catch (error) {
        if (getActiveSourceObjectRelation(scopeRoot) === relation) {
          renderSourceInspectorError(
            sourceObjectRoot,
            error instanceof Error
              ? error.message
              : "The fields could not be loaded for this source object."
          );
        }
        throw error;
      } finally {
        setSourceObjectLoadingState(sourceObjectRoot, false);
      }
    }

    if (sourceObjectFieldCache.has(relation)) {
      const fields = sourceObjectFieldCache.get(relation) ?? [];
      if (getActiveSourceObjectRelation(scopeRoot) === relation) {
        renderSourceInspector(sourceObjectRoot, fields);
      }
      return fields;
    }

    if (renderLoading && getActiveSourceObjectRelation(scopeRoot) === relation) {
      setSourceObjectLoadingState(sourceObjectRoot, true);
      renderSourceInspectorLoading(sourceObjectRoot);
    }

    let pendingRequest = sourceObjectFieldRequests.get(relation);
    if (!pendingRequest) {
      pendingRequest = fetchSourceObjectFields(relation)
        .then((fields) => {
          sourceObjectFieldCache.set(relation, fields);
          return fields;
        })
        .finally(() => {
          sourceObjectFieldRequests.delete(relation);
        });
      sourceObjectFieldRequests.set(relation, pendingRequest);
    }

    try {
      const fields = await pendingRequest;
      if (getActiveSourceObjectRelation(scopeRoot) === relation) {
        renderSourceInspector(sourceObjectRoot, fields);
      }
      return fields;
    } catch (error) {
      if (getActiveSourceObjectRelation(scopeRoot) === relation) {
        renderSourceInspectorError(
          sourceObjectRoot,
          error instanceof Error ? error.message : "The fields could not be loaded for this source object."
        );
      }
      throw error;
    } finally {
      setSourceObjectLoadingState(sourceObjectRoot, false);
    }
  }

  async function selectSourceObject(sourceObjectRoot = null, { renderLoading = true } = {}) {
    setSelectedSourceObjectState(sourceObjectRoot);
    if (!(sourceObjectRoot instanceof Element)) {
      return [];
    }

    return loadSourceObjectFields(sourceObjectRoot, { renderLoading });
  }

  function restoreSelectedSourceObject() {
    sourceBrowserScopes().forEach((scopeRoot) => {
      const activeRelation = getActiveSourceObjectRelation(scopeRoot);
      if (!activeRelation) {
        return;
      }
      const sourceObjectRoot =
        sourceObjectNodes(scopeRoot).find(
          (item) => item.dataset.sourceObjectRelation?.trim() === activeRelation
        ) ?? null;

      if (!sourceObjectRoot) {
        activeSourceObjectRelations.delete(scopeRoot);
        if (activeSourceObjectRelation === activeRelation) {
          activeSourceObjectRelation = null;
        }
        renderSourceInspectorMarkup("", true, scopeRoot);
        return;
      }

      selectSourceObject(sourceObjectRoot, {
        renderLoading: !sourceObjectFieldCache.has(sourceObjectFieldCacheKey(sourceObjectRoot)),
      }).catch(() => {
        // Keep the last selected state, but do not interrupt the rest of the UI.
      });
    });
  }

  return {
    clearSourceObjectFieldCacheForRelations,
    getActiveSourceObjectRelation,
    restoreSelectedSourceObject,
    selectSourceObject,
    setSelectedSourceObjectState,
  };
}

export function createSourceInspectorController(helpers) {
  const {
    isLocalWorkspaceSourceObject,
    localWorkspaceInspectorMarkup,
    normalizeSourceObjectFields,
    renderSourceInspector,
    renderSourceInspectorError,
    renderSourceInspectorLoading,
    renderSourceInspectorMarkup,
    sourceObjectNodes,
  } = helpers;

  let activeSourceObjectRelation = null;
  const sourceObjectFieldCache = new Map();
  const sourceObjectFieldRequests = new Map();

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

  function getActiveSourceObjectRelation() {
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
    const selectedRelation = sourceObjectRoot?.dataset.sourceObjectRelation?.trim() || null;
    activeSourceObjectRelation = selectedRelation;

    sourceObjectNodes().forEach((item) => {
      const isSelected = item === sourceObjectRoot;
      item.classList.toggle("is-selected", isSelected);
      item.setAttribute("aria-selected", isSelected ? "true" : "false");
      if (!isSelected) {
        setSourceObjectLoadingState(item, false);
      }
    });

    if (!(sourceObjectRoot instanceof Element)) {
      renderSourceInspectorMarkup("", true);
    }
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
    if (!relation) {
      return [];
    }

    if (isLocalWorkspaceSourceObject(sourceObjectRoot)) {
      if (getActiveSourceObjectRelation() === relation) {
        renderSourceInspectorMarkup(localWorkspaceInspectorMarkup(sourceObjectRoot));
      }
      return [];
    }

    if (sourceObjectFieldCache.has(relation)) {
      const fields = sourceObjectFieldCache.get(relation) ?? [];
      if (getActiveSourceObjectRelation() === relation) {
        renderSourceInspector(sourceObjectRoot, fields);
      }
      return fields;
    }

    if (renderLoading && getActiveSourceObjectRelation() === relation) {
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
      if (getActiveSourceObjectRelation() === relation) {
        renderSourceInspector(sourceObjectRoot, fields);
      }
      return fields;
    } catch (error) {
      if (getActiveSourceObjectRelation() === relation) {
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
    const activeRelation = getActiveSourceObjectRelation();
    const sourceObjectRoot =
      sourceObjectNodes().find((item) => item.dataset.sourceObjectRelation?.trim() === activeRelation) ?? null;

    if (!sourceObjectRoot) {
      activeSourceObjectRelation = null;
    }

    selectSourceObject(sourceObjectRoot, {
      renderLoading: !sourceObjectFieldCache.has(sourceObjectFieldCacheKey(sourceObjectRoot)),
    }).catch(() => {
      // Keep the last selected state, but do not interrupt the rest of the UI.
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
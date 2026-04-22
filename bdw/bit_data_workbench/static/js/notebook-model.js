import {
  normalizeDataSources,
  parseDefaultDataSources,
  sourceIdFromLegacyTargetLabel,
} from "./source-metadata-utils.js";

export function createNotebookModel(helpers) {
  const { createCellId, normalizeTags, notebookLinks, parseBooleanDatasetValue } = helpers;

  function normalizeCellLanguage(value, fallback = "sql") {
    const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
    if (normalized === "python") {
      return "python";
    }
    return fallback === "python" ? "python" : "sql";
  }

  function parseDefaultTags(value) {
    if (!value) {
      return [];
    }

    return normalizeTags(String(value).split("||"));
  }

  function parseCellsPayload(value) {
    if (!value) {
      return [];
    }

    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      return [];
    }
  }

  function normalizeNotebookTitleValue(value, fallback = "Untitled Notebook") {
    const title = typeof value === "string" ? value.trim() : "";
    if (title) {
      return title;
    }

    const fallbackTitle = typeof fallback === "string" ? fallback.trim() : "";
    return fallbackTitle || "Untitled Notebook";
  }

  function normalizeNotebookSummaryValue(value, fallback = "Describe this notebook.") {
    const summary = typeof value === "string" ? value.trim() : "";
    if (summary) {
      return summary;
    }

    const fallbackSummary = typeof fallback === "string" ? fallback.trim() : "";
    return fallbackSummary || "Describe this notebook.";
  }

  function normalizeCellEntry(cell, fallback = {}) {
    if (!cell || typeof cell !== "object" || Array.isArray(cell)) {
      return null;
    }

    const cellId =
      typeof cell.cellId === "string" && cell.cellId
        ? cell.cellId
        : typeof cell.cell_id === "string" && cell.cell_id
          ? cell.cell_id
          : fallback.cellId ?? createCellId();

    return {
      cellId,
      language: normalizeCellLanguage(cell.language ?? cell.cell_language, fallback.language ?? "sql"),
      dataSources: Array.isArray(cell.dataSources)
        ? normalizeDataSources(cell.dataSources)
        : Array.isArray(cell.data_sources)
          ? normalizeDataSources(cell.data_sources)
          : normalizeDataSources(fallback.dataSources ?? []),
      sql:
        typeof cell.sql === "string"
          ? cell.sql
          : typeof fallback.sql === "string"
            ? fallback.sql
            : "",
    };
  }

  function normalizeNotebookCells(cells, fallback = {}) {
    const normalized = Array.isArray(cells)
      ? cells
          .map((cell) => normalizeCellEntry(cell))
          .filter(Boolean)
      : [];

    if (normalized.length) {
      return normalized;
    }

    return [
      normalizeCellEntry(
        {
          dataSources: fallback.dataSources ?? [],
          sql: fallback.sql ?? "",
        },
        {
          cellId: createCellId(),
          language: normalizeCellLanguage(fallback.language ?? "sql"),
          dataSources: fallback.dataSources ?? [],
          sql: fallback.sql ?? "",
        }
      ),
    ].filter(Boolean);
  }

  function notebookSourceIds(metadata) {
    const sources = [];
    const seen = new Set();

    for (const cell of metadata.cells ?? []) {
      for (const sourceId of normalizeDataSources(cell.dataSources)) {
        if (seen.has(sourceId)) {
          continue;
        }

        seen.add(sourceId);
        sources.push(sourceId);
      }
    }

    return sources;
  }

  function notebookAccessMode(metadata) {
    return (metadata.cells ?? []).some((cell) => normalizeDataSources(cell.dataSources).length > 1)
      ? "Read / Query only"
      : "Read / Write";
  }

  function notebookAccessModeHint(metadata) {
    return (metadata.cells ?? []).some((cell) => normalizeDataSources(cell.dataSources).length > 1)
      ? "At least one cell selects multiple sources and stays in query-only mode."
      : "All cells are currently configured for single-source read/write execution.";
  }

  function activeWorkspaceMetaRoot(notebookId) {
    return document.querySelector(`[data-notebook-meta][data-notebook-id="${notebookId}"]`);
  }

  function normalizeVersionEntry(version) {
    if (!version || typeof version !== "object") {
      return null;
    }

    const versionId =
      typeof version.versionId === "string" && version.versionId
        ? version.versionId
        : `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
    const createdAt =
      typeof version.createdAt === "string" && version.createdAt
        ? version.createdAt
        : new Date().toISOString();

    return {
      versionId,
      createdAt,
      title: typeof version.title === "string" ? version.title : "",
      summary: typeof version.summary === "string" ? version.summary : "",
      tags: normalizeTags(Array.isArray(version.tags) ? version.tags : []),
      cells: normalizeNotebookCells(version.cells, {
        dataSources: Array.isArray(version.dataSources)
          ? normalizeDataSources(version.dataSources)
          : version.targetLabel
            ? normalizeDataSources([sourceIdFromLegacyTargetLabel(version.targetLabel)].filter(Boolean))
            : [],
        sql: typeof version.sql === "string" ? version.sql : "",
      }),
    };
  }

  function parseVersionsPayload(value) {
    if (!value) {
      return [];
    }

    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed.map((version) => normalizeVersionEntry(version)).filter(Boolean) : [];
    } catch (_error) {
      return [];
    }
  }

  function createInitialNotebookVersion(notebookId, metadata, createdAt = null) {
    return {
      versionId: `initial-${notebookId}`,
      createdAt: createdAt || new Date().toISOString(),
      title: normalizeNotebookTitleValue(metadata.title),
      summary: normalizeNotebookSummaryValue(metadata.summary),
      tags: normalizeTags(metadata.tags),
      cells: (metadata.cells ?? []).map((cell) => ({
        cellId: cell.cellId,
        language: normalizeCellLanguage(cell.language, "sql"),
        dataSources: normalizeDataSources(cell.dataSources),
        sql: cell.sql,
      })),
    };
  }

  function readNotebookDefaults(notebookId) {
    const link = notebookLinks(notebookId)[0];
    const metaRoot = activeWorkspaceMetaRoot(notebookId);
    const metaTags = parseDefaultTags(metaRoot?.dataset.defaultTags);
    const linkTags = parseDefaultTags(link?.dataset.defaultNotebookTags);
    const metaCells = parseCellsPayload(metaRoot?.dataset.defaultCells);
    const linkCells = parseCellsPayload(link?.dataset.defaultNotebookCells);
    const metaVersions = parseVersionsPayload(metaRoot?.dataset.defaultVersions);
    const linkVersions = parseVersionsPayload(link?.dataset.defaultNotebookVersions);
    const metaDataSources = parseDefaultDataSources(metaRoot?.dataset.defaultDataSources);
    const linkDataSources = parseDefaultDataSources(link?.dataset.defaultNotebookDataSources);
    const legacyDataSource = sourceIdFromLegacyTargetLabel(
      metaRoot?.dataset.defaultTargetLabel ??
        link?.dataset.defaultNotebookTargetLabel ??
        link?.dataset.notebookTargetLabel ??
        ""
    );
    const legacySql =
      metaRoot
        ?.closest("[data-workspace-notebook]")
        ?.querySelector("[data-editor-source]")
        ?.dataset.defaultSql ??
      metaRoot
        ?.closest("[data-workspace-notebook]")
        ?.querySelector("[data-editor-source]")
        ?.defaultValue ??
      metaRoot?.closest("[data-workspace-notebook]")?.querySelector("[data-editor-source]")?.value ??
      "";
    const domTags = normalizeTags(
      Array.from(link?.querySelectorAll(".notebook-tag") ?? []).map((tag) => tag.textContent ?? "")
    );
    const fallbackDataSources = metaDataSources.length
      ? metaDataSources
      : linkDataSources.length
        ? linkDataSources
        : legacyDataSource
          ? [legacyDataSource]
          : [];

    const defaults = {
      title: metaRoot?.dataset.defaultTitle ?? link?.dataset.defaultNotebookTitle ?? link?.dataset.notebookTitle ?? "",
      summary:
        metaRoot?.dataset.defaultSummary ??
        link?.dataset.defaultNotebookSummary ??
        link?.dataset.notebookSummary ??
        "",
      createdAt:
        metaRoot?.dataset.defaultCreatedAt ??
        metaRoot?.dataset.createdAt ??
        link?.dataset.createdAt ??
        new Date().toISOString(),
      linkedGeneratorId: metaRoot?.dataset.linkedGeneratorId ?? "",
      cells: normalizeNotebookCells(metaCells.length ? metaCells : linkCells, {
        language: "sql",
        dataSources: fallbackDataSources,
        sql: legacySql,
      }),
      tags: metaTags.length ? metaTags : linkTags.length ? linkTags : domTags,
      canEdit: (metaRoot?.dataset.canEdit ?? link?.dataset.canEdit ?? "true") !== "false",
      canDelete: (metaRoot?.dataset.canDelete ?? link?.dataset.canDelete ?? "true") !== "false",
      shared: parseBooleanDatasetValue(
        metaRoot?.dataset.defaultShared ?? metaRoot?.dataset.shared ?? link?.dataset.defaultNotebookShared ?? link?.dataset.shared,
        false
      ),
      deleted: false,
      versions: metaVersions.length ? metaVersions : linkVersions,
    };
    if (!defaults.versions.length) {
      defaults.versions = [createInitialNotebookVersion(notebookId, defaults)];
    }
    return defaults;
  }

  function sortVersionsDescending(versions) {
    return [...versions].sort((left, right) => {
      const leftTime = Date.parse(left.createdAt || "") || 0;
      const rightTime = Date.parse(right.createdAt || "") || 0;
      return rightTime - leftTime;
    });
  }

  function normalizeStoredNotebookState(storedState) {
    if (!storedState || typeof storedState !== "object" || Array.isArray(storedState)) {
      return {};
    }

    return {
      title: typeof storedState.title === "string" ? storedState.title : undefined,
      summary: typeof storedState.summary === "string" ? storedState.summary : undefined,
      tags: Array.isArray(storedState.tags) ? normalizeTags(storedState.tags) : undefined,
      cells:
        Array.isArray(storedState.cells) && storedState.cells.length
          ? normalizeNotebookCells(storedState.cells)
          : storedState.dataSources !== undefined || storedState.sql !== undefined || storedState.targetLabel
            ? normalizeNotebookCells([], {
                dataSources: Array.isArray(storedState.dataSources)
                  ? normalizeDataSources(storedState.dataSources)
                  : typeof storedState.targetLabel === "string"
                    ? normalizeDataSources([sourceIdFromLegacyTargetLabel(storedState.targetLabel)].filter(Boolean))
                    : [],
                language: "sql",
                sql: typeof storedState.sql === "string" ? storedState.sql : "",
              })
            : undefined,
      shared:
        storedState.shared === true
          ? true
          : storedState.shared === false
            ? false
            : undefined,
      deleted: storedState.deleted === true,
      versions: sortVersionsDescending(
        (storedState.versions ?? []).map((version) => normalizeVersionEntry(version)).filter(Boolean)
      ),
    };
  }

  return {
    activeWorkspaceMetaRoot,
    createInitialNotebookVersion,
    normalizeCellLanguage,
    normalizeCellEntry,
    normalizeNotebookCells,
    normalizeNotebookSummaryValue,
    normalizeNotebookTitleValue,
    normalizeStoredNotebookState,
    normalizeVersionEntry,
    notebookAccessMode,
    notebookAccessModeHint,
    notebookSourceIds,
    readNotebookDefaults,
    sortVersionsDescending,
  };
}

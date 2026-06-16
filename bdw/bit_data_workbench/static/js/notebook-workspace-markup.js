import {
  accessModeForDataSources,
  accessModeHintForDataSources,
  normalizeDataSources,
  readSourceOptions,
  sourceClassificationDisplayText,
  sourceComputationModeDisplayText,
  sourceComputationModeTooltipText,
  sourceLabelsForIds,
  sourceStorageTooltipForIds,
} from "./source-metadata-utils.js";

function cellSourceSummaryText(dataSources) {
  const labels = sourceLabelsForIds(dataSources);
  if (!labels.length) {
    return "Select sources";
  }
  if (labels.length === 1) {
    return labels[0];
  }
  return `${labels.length} sources`;
}

function normalizeCellLanguage(value) {
  return String(value || "").trim().toLowerCase() === "python" ? "python" : "sql";
}

function recommendedStageOutputFileName(stage) {
  const base = String(stage?.alias || stage?.title || "stage")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_") || "stage";
  return `${base}.parquet`;
}

function runCellButtonLabel(pipelineMode, cellLanguage) {
  return pipelineMode === "pipeline" && cellLanguage === "sql" ? "Run Stage" : "Run Cell";
}

function cellOrdinalLabel(pipelineMode, cellLanguage, index) {
  const prefix = pipelineMode === "pipeline" && cellLanguage === "sql" ? "Stage" : "Cell";
  return `${prefix} ${index + 1}`;
}

export function createNotebookWorkspaceMarkup(helpers) {
  const {
    escapeHtml,
    formatVersionTimestamp,
    normalizeCellStage,
    normalizeNotebookCells,
    normalizePipelinePaths,
    normalizeTags,
    pythonResultPanelMarkup,
    preferredSqlEditorRows,
    queryResultPanelMarkup,
    truncateWords,
  } = helpers;

  function cellSourceSummaryMarkup(dataSources) {
    const selectedSources = normalizeDataSources(dataSources);
    const storageTooltip = sourceStorageTooltipForIds(selectedSources);
    const summaryTitle = storageTooltip ? ` title="${escapeHtml(storageTooltip)}"` : "";
    const metadataMarkup = selectedSources.length
      ? `
          <span class="cell-source-classification" data-cell-source-classification>${escapeHtml(sourceClassificationDisplayText(selectedSources))}</span>
          <span class="cell-source-computation-mode" data-cell-source-computation-mode title="${escapeHtml(sourceComputationModeTooltipText())}">${escapeHtml(sourceComputationModeDisplayText(selectedSources))}</span>
        `
      : "";

    return `
      <span class="cell-source-summary-label" data-cell-source-summary-label${summaryTitle}>${escapeHtml(cellSourceSummaryText(dataSources))}</span>
      ${metadataMarkup}
    `;
  }

  function parquetHivePartitioningOption(cell) {
    const value = String(cell?.queryOptions?.duckdb?.parquetHivePartitioning || "auto")
      .trim()
      .toLowerCase();
    return ["auto", "on", "off"].includes(value) ? value : "auto";
  }

  function cacheHydrationEnabled(cell) {
    const value = String(cell?.queryOptions?.duckdb?.cacheHydration?.mode || "off")
      .trim()
      .toLowerCase();
    return value === "on";
  }

  function sourceExistenceValidationEnabled(cell) {
    const value = String(cell?.queryOptions?.validation?.sourceExistence || "off")
      .trim()
      .toLowerCase();
    return value === "on";
  }

  function duckdbOptionsMarkup(cell, canEdit, cellLanguage) {
    const selected = parquetHivePartitioningOption(cell);
    const hydrateCache = cacheHydrationEnabled(cell);
    const checkSources = sourceExistenceValidationEnabled(cell);
    const hidden = cellLanguage === "sql" ? "" : " hidden";
    const disabled = canEdit && cellLanguage === "sql" ? "" : " disabled";
    const hiveTitle =
      "DuckDB Parquet Hive partitioning controls how read_parquet interprets partition folders for known S3 Parquet query sources. Auto uses source discovery defaults; On forces hive_partitioning=true; Off forces hive_partitioning=false.";
    const cacheTitle =
      "Copies the S3 Parquet data referenced by this cell into a temporary local DuckDB table before the query runs. DuckDB can then reuse the local table and optional ART indexes for repeated filters and lookups. This cache lives in temporary compute storage and can disappear after a pod restart.";
    const sourceCheckTitle =
      "Checks whether referenced sources exist before running or explaining this cell. Turn off for proven queries to skip the preflight check and let DuckDB fail only if a source is actually missing.";
    return `
      <span class="cell-duckdb-options"${hidden} data-cell-duckdb-options>
        <label class="cell-duckdb-option" title="${escapeHtml(hiveTitle)}">
          <span>Hive partitions</span>
          <select data-cell-query-option="duckdb.parquetHivePartitioning"${disabled}>
            <option value="auto"${selected === "auto" ? " selected" : ""}>Auto</option>
            <option value="on"${selected === "on" ? " selected" : ""}>On</option>
            <option value="off"${selected === "off" ? " selected" : ""}>Off</option>
          </select>
        </label>
        <span class="cell-cache-hydration-option" data-cache-hydration-state="unknown" title="${escapeHtml(cacheTitle)}" data-cell-cache-hydration>
          <button
            type="button"
            class="cell-cache-hydration-switch"
            role="switch"
            aria-checked="${hydrateCache ? "true" : "false"}"
            data-cell-query-option="duckdb.cacheHydration.mode"
            data-cache-hydration-switch
            title="${escapeHtml(cacheTitle)}"
            ${disabled}
          >
            <span class="cell-cache-hydration-switch-track" aria-hidden="true">
              <span class="cell-cache-hydration-switch-thumb"></span>
            </span>
            <span class="cell-cache-hydration-switch-copy">
              <span>Hydrate cache</span>
              <strong data-cache-hydration-state-label>${hydrateCache ? "Unknown" : "Off"}</strong>
            </span>
          </button>
          <span class="cell-cache-hydration-badge" data-cache-hydration-badge hidden>Unknown</span>
          <button type="button" class="cell-cache-hydration-details" data-cache-hydration-details title="Open the Cache hydration plan. It explains what will be cached, source size, cache size, ART indexes, cache freshness, and what happens on the next run." ${cellLanguage === "sql" ? "" : "hidden"}>Details</button>
        </span>
        <span class="cell-source-check-option" title="${escapeHtml(sourceCheckTitle)}">
          <button
            type="button"
            class="cell-source-check-switch"
            role="switch"
            aria-checked="${checkSources ? "true" : "false"}"
            data-cell-query-option="validation.sourceExistence"
            data-source-check-switch
            title="${escapeHtml(sourceCheckTitle)}"
            ${disabled}
          >
            <span class="cell-cache-hydration-switch-track" aria-hidden="true">
              <span class="cell-cache-hydration-switch-thumb"></span>
            </span>
            <span class="cell-cache-hydration-switch-copy">
              <span>Check sources</span>
              <strong data-source-check-state-label>${checkSources ? "On" : "Off"}</strong>
            </span>
          </button>
        </span>
      </span>
    `;
  }

  function cellStageStripMarkup(cell, canEdit, pipelineMode, cellLanguage) {
    const stage = normalizeCellStage(cell.stage);
    const recommendedOutputFile = recommendedStageOutputFileName(stage);
    const hidden = pipelineMode === "pipeline" && cellLanguage === "sql" ? "" : " hidden";
    return `
      <div class="cell-stage-strip"${hidden} data-cell-stage-strip>
        <div class="cell-stage-main">
          <label class="cell-stage-field cell-stage-title-field">
            <span>Stage</span>
            <input
              class="cell-stage-title-input"
              type="text"
              value="${escapeHtml(stage.title)}"
              placeholder="Stage title"
              data-cell-stage-title-input
              ${canEdit ? "" : "disabled"}
            >
          </label>
          <label class="cell-stage-field cell-stage-output-field">
            <span>Destination file</span>
            <input
              class="cell-stage-output-file-input"
              type="text"
              value="${escapeHtml(stage.outputFileName)}"
              placeholder="${escapeHtml(recommendedOutputFile)}"
              title="Recommended: ${escapeHtml(recommendedOutputFile)}"
              data-cell-stage-output-file-input
              ${canEdit ? "" : "disabled"}
            >
          </label>
          <label class="cell-stage-field cell-stage-description-field">
            <span>Description</span>
            <input
              class="cell-stage-description-input"
              type="text"
              value="${escapeHtml(stage.description)}"
              placeholder="Analyst description"
              data-cell-stage-description-input
              ${canEdit ? "" : "disabled"}
            >
          </label>
        </div>
        <div class="cell-stage-meta">
          <span class="cell-stage-status-badge" data-cell-stage-status>Planned</span>
          <span class="cell-stage-chip-row" data-cell-stage-predecessors></span>
          <span class="cell-stage-chip-row" data-cell-stage-successors></span>
        </div>
      </div>
    `;
  }

  function queryRunsPanelMarkup(notebookId, cellId) {
    return `
      <details
        class="workspace-query-runs workspace-query-runs-cell"
        data-notebook-query-runs
        data-query-runs-notebook-id="${escapeHtml(notebookId)}"
        data-query-runs-cell-id="${escapeHtml(cellId)}"
        data-query-runs-limit="10"
      >
        <summary class="workspace-query-runs-summary">
          <span class="workspace-query-runs-title">
            <span class="workspace-query-runs-chevron" aria-hidden="true"></span>
            <span class="workspace-tags-label">Query Monitoring</span>
          </span>
          <span class="query-runs-status" data-query-runs-status>No recorded query runs yet.</span>
        </summary>
        <div class="workspace-query-runs-header workspace-query-runs-header-cell">
          <p>Recorded runs for this cell.</p>
          <button type="button" class="query-runs-chart-toggle" data-query-runs-toggle-charts aria-pressed="false" title="Show resource charts">
            <span class="query-runs-chart-toggle-switch" aria-hidden="true">
              <span class="query-runs-chart-toggle-thumb"></span>
            </span>
            <span class="query-runs-chart-toggle-copy" data-query-runs-toggle-label>Show resource charts</span>
          </button>
        </div>
        <div class="query-run-history-list query-run-history-list-compact" data-query-runs-list>
          <p class="home-empty">No recorded query runs yet.</p>
        </div>
      </details>
    `;
  }

  function buildCellMarkup(notebookId, cell, index, canEdit, totalCells, activeCellId, pipelineMode) {
    const selectedSources = normalizeDataSources(cell.dataSources);
    const cellLanguage = normalizeCellLanguage(cell.language);
    const canMoveUp = canEdit && index > 0;
    const canMoveDown = canEdit && index < totalCells - 1;
    const canFormatSql = canEdit && cellLanguage === "sql";
    const sovereigntyHint =
      "Your data is exclusivly stored and processed in Swiss Government facilities. Hybrid or 3rd-party storage will be available with the Swiss Government Cloud for insensitive data.";
    const languageBadge = cellLanguage === "python" ? "Python / Headless Jupyter Kernel" : "SQL / Query Engine";
    const processingHints = String(cell.processingHints || "");
    const resultExpectations = String(cell.resultExpectations || "");
    const descriptorMarkup = `
      <div class="cell-descriptor-grid" data-cell-descriptors>
        <label class="cell-descriptor-field">
          <span>Cell processing hints</span>
          ${canEdit
            ? `<textarea class="cell-descriptor-input" rows="2" data-cell-descriptor="processingHints" placeholder="Describe processing hints for this cell.">${escapeHtml(processingHints)}</textarea>`
            : `<div class="cell-descriptor-readonly" data-cell-descriptor-readonly="processingHints">${escapeHtml(processingHints || "No processing hints saved.")}</div>`}
        </label>
        <label class="cell-descriptor-field">
          <span>Cell result expectations</span>
          ${canEdit
            ? `<textarea class="cell-descriptor-input" rows="2" data-cell-descriptor="resultExpectations" placeholder="Describe expected results for this cell.">${escapeHtml(resultExpectations)}</textarea>`
            : `<div class="cell-descriptor-readonly" data-cell-descriptor-readonly="resultExpectations">${escapeHtml(resultExpectations || "No result expectations saved.")}</div>`}
        </label>
      </div>
    `;
    const sourceOptionsMarkup =
      readSourceOptions()
        .map((option) => {
          const selected = selectedSources.includes(option.source_id);
          const storageTitle = option.storage_tooltip
            ? ` title="${escapeHtml(option.storage_tooltip)}"`
            : "";
          return `
            <label class="workspace-source-option cell-source-option${selected ? " is-selected" : ""}"${storageTitle}>
              <input
                class="workspace-source-checkbox"
                type="checkbox"
                value="${escapeHtml(option.source_id)}"
                data-cell-source-option
                ${selected ? "checked" : ""}
                ${canEdit ? "" : "disabled"}
              >
              <span>${escapeHtml(option.label)}</span>
            </label>
          `;
        })
        .join("") || '<p class="workspace-source-empty">No data sources available.</p>';

    return `
      <article
        class="workspace-cell${cell.cellId === activeCellId ? " is-active" : ""}"
        data-query-cell
        data-cell-id="${escapeHtml(cell.cellId)}"
        data-default-cell-language="${escapeHtml(cellLanguage)}"
        data-default-cell-sources="${escapeHtml(selectedSources.join("||"))}"
      >
        <form class="query-form query-form-cell" data-query-form>
          <input type="hidden" name="notebook_id" value="${escapeHtml(notebookId)}">
          <input type="hidden" name="cell_id" value="${escapeHtml(cell.cellId)}">
          <div class="cell-toolbar">
            <div class="cell-heading">
              <span class="cell-label">${escapeHtml(cellOrdinalLabel(pipelineMode, cellLanguage, index))}</span>
              <div class="cell-language-toggle" data-cell-language-toggle>
                <button
                  type="button"
                  class="cell-language-toggle-button${cellLanguage === "sql" ? " is-active" : ""}"
                  data-set-cell-language="sql"
                  ${canEdit ? "" : "disabled"}
                >SQL</button>
                <button
                  type="button"
                  class="cell-language-toggle-button${cellLanguage === "python" ? " is-active" : ""}"
                  data-set-cell-language="python"
                  ${canEdit ? "" : "disabled"}
                >Python</button>
              </div>
              <span class="workspace-access-badge workspace-access-badge-small workspace-access-badge-language" data-cell-language-badge>${escapeHtml(languageBadge)}</span>
              <span class="workspace-access-badge workspace-access-badge-small" data-cell-access-badge title="${escapeHtml(accessModeHintForDataSources(selectedSources))}">${escapeHtml(accessModeForDataSources(selectedSources))}</span>
              <span class="workspace-access-badge workspace-access-badge-small workspace-access-badge-static" title="${escapeHtml(sovereigntyHint)}">CHE Data Souvereignity</span>
              ${duckdbOptionsMarkup(cell, canEdit, cellLanguage)}
              <details class="cell-source-picker" data-cell-source-picker>
                <summary class="cell-source-picker-toggle" data-cell-source-summary>${cellSourceSummaryMarkup(selectedSources)}</summary>
                <div class="cell-source-selection" data-cell-source-selection>
                  ${sourceOptionsMarkup}
                </div>
              </details>
            </div>
            <div class="cell-actions">
              <div class="cell-run-actions">
                <button class="run-button" type="submit" title="Run with Ctrl/Cmd + Enter" data-run-cell>${escapeHtml(runCellButtonLabel(pipelineMode, cellLanguage))}</button>
                <button class="explain-button" type="button" title="Explain this SQL cell without running it." data-explain-cell ${cellLanguage === "sql" ? "" : "hidden"}>Explain</button>
                <button class="query-cancel-button" type="button" data-cancel-query hidden>Cancel</button>
              </div>
              <details class="workspace-action-menu cell-action-menu" data-cell-action-menu>
                <summary class="workspace-action-menu-toggle" aria-label="Cell actions" title="Cell actions">
                  <span class="workspace-action-menu-dots" aria-hidden="true">...</span>
                </summary>
                <div class="workspace-action-menu-panel">
                  <button type="button" class="workspace-action-menu-item${canFormatSql ? "" : " is-action-disabled"}" data-format-cell-sql ${canFormatSql ? "" : "disabled"} title="${canEdit ? (cellLanguage === "sql" ? "Format SQL" : "Available in SQL cells only.") : "This notebook cannot be edited."}">Format SQL</button>
                  <button type="button" class="workspace-action-menu-item workspace-action-menu-item-placeholder is-action-disabled" disabled title="disabled.">Optimize SQL</button>
                  <button type="button" class="workspace-action-menu-item workspace-action-menu-item-placeholder is-action-disabled" disabled title="disabled.">Explain SQL Execution Plan</button>
                  <button type="button" class="workspace-action-menu-item workspace-action-menu-item-placeholder is-action-disabled" disabled title="disabled.">Explain Semantics of this Query</button>
                  <div class="workspace-action-menu-separator" aria-hidden="true"></div>
                  <button type="button" class="workspace-action-menu-item workspace-action-menu-item-no-strike${canMoveUp ? "" : " is-action-disabled"}" data-move-cell-up ${canMoveUp ? "" : "disabled"} title="${
                    canMoveUp
                      ? "Move cell up"
                      : canEdit
                        ? "This cell is already first."
                        : "This notebook cannot be edited."
                  }">Move up</button>
                  <button type="button" class="workspace-action-menu-item workspace-action-menu-item-no-strike${canMoveDown ? "" : " is-action-disabled"}" data-move-cell-down ${canMoveDown ? "" : "disabled"} title="${
                    canMoveDown
                      ? "Move cell down"
                      : canEdit
                        ? "This cell is already last."
                        : "This notebook cannot be edited."
                  }">Move down</button>
                  <div class="workspace-action-menu-separator" aria-hidden="true"></div>
                  <button type="button" class="workspace-action-menu-item${canEdit ? "" : " is-action-disabled"}" data-add-cell-after ${canEdit ? "" : "disabled"} title="${canEdit ? "Add cell below" : "This notebook cannot be edited."}">Add cell</button>
                  <button type="button" class="workspace-action-menu-item${canEdit ? "" : " is-action-disabled"}" data-copy-cell ${canEdit ? "" : "disabled"} title="${canEdit ? "Copy cell" : "This notebook cannot be edited."}">Copy cell</button>
                  <button type="button" class="workspace-action-menu-item workspace-action-menu-item-danger${canEdit ? "" : " is-action-disabled"}" data-delete-cell ${canEdit ? "" : "disabled"} title="${canEdit ? "Delete cell" : "This notebook cannot be edited."}">Delete cell</button>
                </div>
              </details>
            </div>
          </div>
          ${cellStageStripMarkup(cell, canEdit, pipelineMode, cellLanguage)}
          ${descriptorMarkup}
          <div class="editor-frame" data-editor-root data-editor-name="sql-${escapeHtml(cell.cellId)}" data-editor-language="${escapeHtml(cellLanguage)}">
            <button
              type="button"
              class="editor-copy-button"
              data-copy-editor-sql
              aria-label="Copy SQL"
              title="Copy SQL"
            >
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <rect x="8" y="8" width="11" height="11" rx="2"></rect>
                <path d="M5 15H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v1"></path>
              </svg>
            </button>
            <button
              type="button"
              class="editor-expand-button"
              data-expand-editor
              aria-label="Expand SQL editor"
              aria-pressed="false"
              title="Expand SQL editor"
            >+</button>
            <button
              type="button"
              class="editor-source-nav-button"
              data-navigate-cell-source
              aria-label="Navigate to source object"
              title="Navigate to source object"
            >
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <circle cx="12" cy="12" r="8"></circle>
                <path d="M14.8 9.2 13 13l-3.8 1.8L11 11z"></path>
              </svg>
            </button>
            <button
              type="button"
              class="editor-compare-button"
              data-compare-editor-sql
              aria-label="Compare"
              title="Compare"
            >
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <rect x="3" y="4" width="7" height="16" rx="2"></rect>
                <rect x="14" y="4" width="7" height="16" rx="2"></rect>
                <path d="M10 9h4"></path>
                <path d="m12 7 2 2-2 2"></path>
                <path d="M14 15h-4"></path>
                <path d="m12 13-2 2 2 2"></path>
              </svg>
            </button>
            <div class="editor-sql-view-toggle" data-editor-sql-view-toggle role="group" aria-label="SQL view">
              <button
                type="button"
                class="editor-sql-view-button is-active"
                data-editor-sql-view="virtual"
                aria-pressed="true"
                title="Show the editable virtual SQL"
              >Virtual</button>
              <button
                type="button"
                class="editor-sql-view-button"
                data-editor-sql-view="duckdb"
                aria-pressed="false"
                title="Show the final SQL sent to DuckDB"
              >DuckDB</button>
            </div>
            <textarea name="sql" data-editor-source data-default-sql="${escapeHtml(cell.sql)}" data-editor-language="${escapeHtml(cellLanguage)}" rows="${preferredSqlEditorRows(cell.sql)}" spellcheck="false">${escapeHtml(cell.sql)}</textarea>
            <pre class="editor-duckdb-sql-panel" data-duckdb-sql-panel hidden></pre>
          </div>
          <div class="query-source-validation" data-query-source-validation data-query-source-validation-status="unchecked" aria-live="polite" ${cellLanguage === "python" ? "hidden" : ""}>
            <span data-query-source-validation-message>No source references found. Sources will be checked before execution.</span>
          </div>
        </form>
        ${cellLanguage === "python" ? pythonResultPanelMarkup(cell.cellId, null) : `${queryRunsPanelMarkup(notebookId, cell.cellId)}${queryResultPanelMarkup(cell.cellId, null)}`}
      </article>
    `;
  }

  function buildWorkspaceMarkup(notebookId, metadata, activeCellId) {
    const pipelineMode = String(metadata.pipelineMode || "exploration").trim().toLowerCase() === "pipeline" ? "pipeline" : "exploration";
    const pipelineModeEnabled = pipelineMode === "pipeline";
    const modeToggleTitle = pipelineModeEnabled
      ? "Notebook mode: Pipeline. Click to return to Exploration mode, which keeps cells independent for ad-hoc SQL or Python work."
      : "Notebook mode: Exploration. Click to enable Pipeline mode, which links SQL cells into staged materialized data products with dependency-aware runs.";
    const modeToggleLabel = pipelineModeEnabled ? "Pipeline Mode" : "Exploration Mode";
    const modeToggleDetail = pipelineModeEnabled
      ? "Links SQL cells into staged materialized data products and dependency-aware runs."
      : "Keeps cells independent for ad-hoc SQL or Python work.";
    const tagsMarkup = metadata.tags
      .map(
        (tag) => `
          <button type="button" class="workspace-tag-chip" data-tag-remove="${escapeHtml(tag)}">
            <span>${escapeHtml(tag)}</span>
            <span class="workspace-tag-remove" aria-hidden="true">×</span>
          </button>
        `
      )
      .join("");
    const currentVersion = metadata.versions?.[0] ?? null;
    const versionSummaryMarkup = currentVersion
      ? `
          <span class="workspace-version-current-stack">
            <span class="workspace-version-current-primary">
              <span class="workspace-version-current-timestamp">${escapeHtml(formatVersionTimestamp(currentVersion.createdAt))}</span>
              <span class="workspace-version-current-name">${escapeHtml(currentVersion.title || "Notebook version")}</span>
            </span>
            <span class="workspace-version-current-secondary">${escapeHtml(truncateWords(currentVersion.summary || "No description saved.", 10))}</span>
          </span>
        `
      : '<span class="workspace-version-current-empty">No saved versions yet.</span>';
    const cellsMarkup = (metadata.cells ?? [])
      .map((cell, index, cells) => buildCellMarkup(notebookId, cell, index, metadata.canEdit, cells.length, activeCellId, pipelineMode))
      .join("");

    return `
      <article
        class="workspace-card"
        data-workspace-notebook
        data-notebook-meta
        data-notebook-id="${escapeHtml(notebookId)}"
        data-created-at="${escapeHtml(metadata.createdAt || new Date().toISOString())}"
        data-shared="${metadata.shared ? "true" : "false"}"
        data-can-edit="true"
        data-can-delete="true"
        data-default-title="${escapeHtml(metadata.title)}"
        data-default-summary="${escapeHtml(metadata.summary)}"
        data-default-created-at="${escapeHtml(metadata.createdAt || new Date().toISOString())}"
        data-linked-generator-id="${escapeHtml(metadata.linkedGeneratorId || "")}" 
        data-default-pipeline-mode="${escapeHtml(pipelineMode)}"
        data-default-pipeline-paths='${escapeHtml(JSON.stringify(normalizePipelinePaths(metadata.pipelinePaths)))}'
        data-default-cells='${escapeHtml(JSON.stringify((metadata.cells ?? []).map((cell) => ({
          cellId: cell.cellId,
          language: normalizeCellLanguage(cell.language),
          processingHints: cell.processingHints || "",
          resultExpectations: cell.resultExpectations || "",
          dataSources: normalizeDataSources(cell.dataSources),
          queryOptions: cell.queryOptions,
          stage: normalizeCellStage(cell.stage),
          sql: cell.sql,
        }))))}'
        data-default-versions='${escapeHtml(JSON.stringify((metadata.versions ?? []).map((version) => ({
          versionId: version.versionId,
          createdAt: version.createdAt,
          title: version.title,
          summary: version.summary,
          tags: normalizeTags(version.tags),
          cells: normalizeNotebookCells(version.cells).map((cell) => ({
            cellId: cell.cellId,
            language: normalizeCellLanguage(cell.language),
            processingHints: cell.processingHints || "",
            resultExpectations: cell.resultExpectations || "",
            dataSources: normalizeDataSources(cell.dataSources),
            queryOptions: cell.queryOptions,
            stage: normalizeCellStage(cell.stage),
            sql: cell.sql,
          })),
        }))))}'
        data-default-tags="${escapeHtml(metadata.tags.join("||"))}"
        data-default-shared="${metadata.shared ? "true" : "false"}"
      >
        <header class="workspace-header">
          <div class="workspace-title-block">
            <div class="workspace-title-row">
              <h2 class="workspace-notebook-title is-editable" data-notebook-title-display data-rename-notebook-title tabindex="0" role="button" title="Click to rename the notebook">${escapeHtml(metadata.title)}</h2>
            </div>
            <div class="workspace-summary-field" data-summary-container>
              <p class="workspace-summary-display is-editable" data-summary-display tabindex="0" role="button" title="Click to edit the notebook description">${escapeHtml(metadata.summary)}</p>
              <textarea class="workspace-summary-input" data-summary-input rows="3" placeholder="Notebook description">${escapeHtml(metadata.summary)}</textarea>
            </div>
            <div class="workspace-header-tags">
              <div class="workspace-tag-toolbar">
                <div class="workspace-tag-list" data-tag-list>${tagsMarkup}</div>
                <button type="button" class="workspace-tag-badge workspace-tag-badge-add" data-tag-toggle title="Add tag" aria-label="Add tag">+</button>
              </div>
              <div class="workspace-tag-controls workspace-tag-controls-inline" data-tag-controls hidden>
                <input class="workspace-tag-input workspace-tag-input-inline" type="text" placeholder="Add tag or labels (comma-separated)" data-tag-input>
                <button class="workspace-tag-add workspace-tag-add-inline" type="button" data-tag-add>Add</button>
              </div>
              <div class="workspace-header-toggle-row${metadata.canEdit || metadata.shared ? " workspace-header-toggle-row-paired" : ""}">
              <button
                type="button"
                class="workspace-sharing-toggle${metadata.shared ? " is-on" : ""}"
                data-notebook-shared-toggle
                aria-pressed="${metadata.shared ? "true" : "false"}"
                title="${metadata.canEdit ? (metadata.shared ? "Shared with connected users and stored on the server." : "Private to this browser workspace.") : "Immutable preset notebooks are public."}"
                ${metadata.canEdit ? "" : "disabled"}
              >
                <span class="workspace-sharing-toggle-switch" aria-hidden="true">
                  <span class="workspace-sharing-toggle-thumb"></span>
                </span>
              <span class="workspace-sharing-toggle-copy">
                  ${metadata.shared ? "Public / Shared" : "Private / Local"}
                  <small>${metadata.shared ? "Stores this notebook on the server and announces it to connected users." : "Keeps this notebook local to this browser workspace."}</small>
                </span>
              </button>
                <button
                  type="button"
                  class="workspace-sharing-toggle notebook-mode-toggle${pipelineModeEnabled ? " is-on" : ""}"
                  data-notebook-mode-toggle
                  aria-pressed="${pipelineModeEnabled ? "true" : "false"}"
                  title="${escapeHtml(modeToggleTitle)}"
                  aria-label="${escapeHtml(modeToggleTitle)}"
                >
                  <span class="workspace-sharing-toggle-switch" aria-hidden="true">
                    <span class="workspace-sharing-toggle-thumb"></span>
                  </span>
                  <span class="workspace-sharing-toggle-copy notebook-mode-toggle-copy">
                    <span data-notebook-mode-toggle-label>${escapeHtml(modeToggleLabel)}</span>
                    <small data-notebook-mode-toggle-detail>${escapeHtml(modeToggleDetail)}</small>
                  </span>
                </button>
              </div>
            </div>
          </div>
          <div class="workspace-actions">
            <details class="workspace-action-menu" data-workspace-action-menu>
              <summary class="workspace-action-menu-toggle" aria-label="Notebook actions" title="Notebook actions">
                <span class="workspace-action-menu-dots" aria-hidden="true">•••</span>
              </summary>
              <div class="workspace-action-menu-panel">
                <button type="button" class="workspace-action-menu-item" data-rename-notebook title="Rename notebook">Rename</button>
                <button type="button" class="workspace-action-menu-item" data-edit-notebook title="Edit notebook metadata">Edit</button>
                <button type="button" class="workspace-action-menu-item" data-restart-python-kernel title="Clear variables, imports, and stuck Python state for this notebook without changing any cells">Restart Python session</button>
                <button type="button" class="workspace-action-menu-item" data-copy-notebook title="Create a copy of this notebook">Copy notebook</button>
                <button type="button" class="workspace-action-menu-item" data-share-notebook title="Copy or email a notebook reference">Share Notebook ...</button>
                <button type="button" class="workspace-action-menu-item workspace-action-menu-item-danger" data-delete-notebook title="Delete notebook">Delete notebook</button>
              </div>
            </details>
          </div>
        </header>

        <section class="workspace-version-strip" data-version-strip>
          <div class="workspace-version-strip-header">
            <div class="workspace-version-strip-copy">
              <span class="workspace-tags-label">Versions</span>
              <button type="button" class="workspace-version-current" data-version-toggle aria-expanded="false" title="${escapeHtml(currentVersion ? "Expand version history" : "No saved versions yet.")}"${currentVersion ? "" : " disabled"}>
                <span class="workspace-version-current-copy" data-version-current>${versionSummaryMarkup}</span>
                <span class="workspace-version-toggle-icon" data-version-toggle-icon aria-hidden="true">›</span>
              </button>
            </div>
            <button type="button" class="workspace-version-save" data-save-version>Save version</button>
          </div>
          <div class="workspace-version-panel" data-version-panel hidden>
            <div class="workspace-version-list" data-version-list></div>
          </div>
        </section>

        <section class="notebook-pipeline-panel"${pipelineMode === "pipeline" ? "" : " hidden"} data-notebook-pipeline-panel>
          <div class="notebook-pipeline-header">
            <div>
              <span class="notebook-pipeline-title-row">
                <span class="workspace-tags-label">Notebook Pipeline</span>
                <span class="notebook-pipeline-running-indicator" data-notebook-pipeline-running-indicator hidden aria-label="Pipeline is running"></span>
              </span>
            </div>
            <div class="notebook-pipeline-header-actions">
              <p class="notebook-pipeline-status" data-notebook-pipeline-status>Pipeline graph has not been built yet.</p>
              <p class="notebook-pipeline-total-duration" data-notebook-pipeline-total-duration>Total duration -</p>
              <button
                type="button"
                class="notebook-pipeline-priority-button"
                data-pipeline-priority-paths
                aria-expanded="false"
                data-pipeline-tooltip="Prioritize terminal paths when branches are ready to run."
                disabled
              >
                <span data-pipeline-priority-summary>Priority paths</span>
              </button>
              <div class="notebook-pipeline-actions"${pipelineMode === "pipeline" ? "" : " hidden"} data-notebook-pipeline-actions>
                <button type="button" class="notebook-pipeline-run-button" data-run-notebook-pipeline aria-label="Run all pipeline stages in dependency order" data-pipeline-tooltip="Runs all stages in dependency order; priority paths run first when branches fork.">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 4v16l14-8z"></path></svg>
                  <span>Run pipeline</span>
                </button>
                <button type="button" class="notebook-pipeline-cancel-button" data-cancel-notebook-pipeline aria-label="Abort the active pipeline run" data-pipeline-tooltip="Abort the active pipeline run" hidden>
                  <svg viewBox="0 0 24 24" aria-hidden="true"><rect x="7" y="7" width="10" height="10"></rect></svg>
                  <span>Abort pipeline</span>
                </button>
              </div>
            </div>
          </div>
          <div class="notebook-pipeline-graph-band" data-notebook-pipeline-graph></div>
          <div class="notebook-pipeline-table-wrap">
            <table class="notebook-pipeline-table">
              <colgroup>
                <col class="pipeline-col-stage">
                <col class="pipeline-col-run">
                <col class="pipeline-col-status">
                <col class="pipeline-col-dependencies">
                <col class="pipeline-col-duration">
                <col class="pipeline-col-rows">
                <col class="pipeline-col-actions">
              </colgroup>
              <thead>
                <tr>
                  <th>Stage</th>
                  <th></th>
                  <th>Status</th>
                  <th>Dependencies</th>
                  <th>Duration</th>
                  <th>Rows</th>
                  <th></th>
                </tr>
              </thead>
              <tbody data-notebook-pipeline-table></tbody>
              <tfoot>
                <tr class="pipeline-stage-total-row">
                  <td></td>
                  <td></td>
                  <td></td>
                  <td class="pipeline-table-total-label">Total duration</td>
                  <td><span class="pipeline-table-duration pipeline-table-total-duration" data-notebook-pipeline-table-duration-total>-</span></td>
                  <td></td>
                  <td></td>
                </tr>
              </tfoot>
            </table>
          </div>
        </section>

        <section class="workspace-cells" data-cell-list>
          ${cellsMarkup}
        </section>
        <div class="workspace-cell-footer">
          <button type="button" class="workspace-cell-add-button" data-add-cell>Add Cell</button>
        </div>
      </article>
    `;
  }

  return {
    buildWorkspaceMarkup,
    cellSourceSummaryMarkup,
  };
}

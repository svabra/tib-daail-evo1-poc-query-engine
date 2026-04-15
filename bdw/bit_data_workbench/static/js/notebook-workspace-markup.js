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

export function createNotebookWorkspaceMarkup(helpers) {
  const {
    escapeHtml,
    formatVersionTimestamp,
    normalizeNotebookCells,
    normalizeTags,
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

  function buildCellMarkup(notebookId, cell, index, canEdit, totalCells, activeCellId) {
    const selectedSources = normalizeDataSources(cell.dataSources);
    const canMoveUp = canEdit && index > 0;
    const canMoveDown = canEdit && index < totalCells - 1;
    const sovereigntyHint =
      "Your data is exclusivly stored and processed in Swiss Government facilities. Hybrid or 3rd-party storage will be available with the Swiss Government Cloud for insensitive data.";
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
        data-default-cell-sources="${escapeHtml(selectedSources.join("||"))}"
      >
        <form class="query-form query-form-cell" data-query-form>
          <input type="hidden" name="notebook_id" value="${escapeHtml(notebookId)}">
          <input type="hidden" name="cell_id" value="${escapeHtml(cell.cellId)}">
          <div class="cell-toolbar">
            <div class="cell-heading">
              <span class="cell-label">Cell ${index + 1}</span>
              <span class="workspace-access-badge workspace-access-badge-small" data-cell-access-badge title="${escapeHtml(accessModeHintForDataSources(selectedSources))}">${escapeHtml(accessModeForDataSources(selectedSources))}</span>
              <span class="workspace-access-badge workspace-access-badge-small workspace-access-badge-static" title="${escapeHtml(sovereigntyHint)}">CHE Data Souvereignity</span>
              <details class="cell-source-picker" data-cell-source-picker>
                <summary class="cell-source-picker-toggle" data-cell-source-summary>${cellSourceSummaryMarkup(selectedSources)}</summary>
                <div class="cell-source-selection" data-cell-source-selection>
                  ${sourceOptionsMarkup}
                </div>
              </details>
            </div>
            <div class="cell-actions">
              <div class="cell-run-actions">
                <button class="run-button" type="submit" title="Run with Ctrl/Cmd + Enter" data-run-cell>Run Cell</button>
                <button class="query-cancel-button" type="button" data-cancel-query hidden>Cancel</button>
              </div>
              <details class="workspace-action-menu cell-action-menu" data-cell-action-menu>
                <summary class="workspace-action-menu-toggle" aria-label="Cell actions" title="Cell actions">
                  <span class="workspace-action-menu-dots" aria-hidden="true">...</span>
                </summary>
                <div class="workspace-action-menu-panel">
                  <button type="button" class="workspace-action-menu-item${canEdit ? "" : " is-action-disabled"}" data-format-cell-sql ${canEdit ? "" : "disabled"} title="${canEdit ? "Format SQL" : "This notebook cannot be edited."}">Format SQL</button>
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
          <div class="editor-frame" data-editor-root data-editor-name="sql-${escapeHtml(cell.cellId)}">
            <textarea name="sql" data-editor-source data-default-sql="${escapeHtml(cell.sql)}" rows="${preferredSqlEditorRows(cell.sql)}" spellcheck="false">${escapeHtml(cell.sql)}</textarea>
          </div>
        </form>
        ${queryResultPanelMarkup(cell.cellId, null)}
      </article>
    `;
  }

  function buildWorkspaceMarkup(notebookId, metadata, activeCellId) {
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
      .map((cell, index, cells) => buildCellMarkup(notebookId, cell, index, metadata.canEdit, cells.length, activeCellId))
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
        data-default-cells='${escapeHtml(JSON.stringify((metadata.cells ?? []).map((cell) => ({
          cellId: cell.cellId,
          dataSources: normalizeDataSources(cell.dataSources),
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
            dataSources: normalizeDataSources(cell.dataSources),
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
              <button
                type="button"
                class="workspace-sharing-toggle${metadata.shared ? " is-on" : ""}"
                data-notebook-shared-toggle
                aria-pressed="${metadata.shared ? "true" : "false"}"
              >
                <span class="workspace-sharing-toggle-switch" aria-hidden="true">
                  <span class="workspace-sharing-toggle-thumb"></span>
                </span>
                <span class="workspace-sharing-toggle-copy">
                  Shared with all users
                  <small>Stores this notebook on the server and announces it to connected users.</small>
                </span>
              </button>
              <div class="workspace-tag-toolbar">
                <div class="workspace-tag-list" data-tag-list>${tagsMarkup}</div>
                <button type="button" class="workspace-tag-badge workspace-tag-badge-add" data-tag-toggle title="Add tag" aria-label="Add tag">+</button>
              </div>
              <div class="workspace-tag-controls workspace-tag-controls-inline" data-tag-controls hidden>
                <input class="workspace-tag-input workspace-tag-input-inline" type="text" placeholder="Add tag" data-tag-input>
                <button class="workspace-tag-add workspace-tag-add-inline" type="button" data-tag-add>Add</button>
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
                <button type="button" class="workspace-action-menu-item" data-copy-notebook title="Create a copy of this notebook">Copy notebook</button>
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
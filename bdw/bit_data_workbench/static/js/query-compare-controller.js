import { ensureQueryCompareDialog, queryCompareDialog } from "./dialogs.js";

export function createQueryCompareController(helpers) {
  const {
    cellLanguageForCellRoot,
    currentEditorSql,
    currentWorkspaceNotebookId,
    currentWorkspaceNotebookTitle,
    escapeHtml,
    normalizeCellLanguage,
    normalizeNotebookCells,
    normalizeNotebookTitleValue,
    notebookMetadata,
    queryOptionsForCellRoot,
    selectedDataSourcesForCell,
    truncateWords,
  } = helpers;

  const state = {
    current: null,
    notebooks: [],
    targetNotebookId: "",
    targetCellId: "",
  };

  function normalizeCompareSql(sql) {
    return String(sql ?? "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  }

  function compareSqlLines(sql) {
    const normalized = normalizeCompareSql(sql);
    return normalized ? normalized.split("\n") : [];
  }

  function compareCellPreview(sql) {
    const preview = compareSqlLines(sql)
      .map((line) => line.trim())
      .find(Boolean);
    return truncateWords(preview || "Empty SQL", 8);
  }

  function compareCellLabel(cell, index) {
    return `Cell ${index + 1} | ${compareCellPreview(cell?.sql)}`;
  }

  function currentCompareCellIndex(cellRoot) {
    if (!(cellRoot instanceof Element)) {
      return 0;
    }
    return Math.max(
      0,
      Array.from(cellRoot.parentElement?.querySelectorAll("[data-query-cell]") ?? []).indexOf(cellRoot)
    );
  }

  function cssEscape(value) {
    if (window.CSS && typeof window.CSS.escape === "function") {
      return window.CSS.escape(String(value ?? ""));
    }
    return String(value ?? "").replace(/["\\]/g, "\\$&");
  }

  function notebookCompareIds(currentNotebookId = "") {
    const ids = [];
    const seen = new Set();
    const addId = (notebookId) => {
      const normalizedId = String(notebookId || "").trim();
      if (!normalizedId || seen.has(normalizedId)) {
        return;
      }
      seen.add(normalizedId);
      ids.push(normalizedId);
    };

    addId(currentNotebookId);
    document.querySelectorAll(".notebook-link[data-notebook-id]").forEach((link) => {
      addId(link.dataset.notebookId);
    });
    return ids;
  }

  function notebookCellsForCompare(notebookId) {
    const metadata = notebookMetadata(notebookId);
    const cells = normalizeNotebookCells(metadata.cells || []);
    const mergedById = new Map(cells.map((cell) => [cell.cellId, { ...cell }]));
    const orderedIds = cells.map((cell) => cell.cellId);
    const workspaceRoot = document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${cssEscape(notebookId)}"]`
    );

    if (workspaceRoot instanceof Element) {
      workspaceRoot.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
        const cellId = String(cellRoot.dataset.cellId || "").trim();
        const editorRoot = cellRoot.querySelector("[data-editor-root]");
        if (!cellId || !(editorRoot instanceof Element)) {
          return;
        }
        const existing = mergedById.get(cellId) || {
          cellId,
          language: cellLanguageForCellRoot(cellRoot),
          dataSources: selectedDataSourcesForCell(cellRoot),
          queryOptions: queryOptionsForCellRoot(cellRoot),
          stage: {},
          sql: "",
        };
        mergedById.set(cellId, {
          ...existing,
          cellId,
          language: cellLanguageForCellRoot(cellRoot),
          sql: currentEditorSql(editorRoot),
        });
        if (!orderedIds.includes(cellId)) {
          orderedIds.push(cellId);
        }
      });
    }

    return orderedIds
      .map((cellId) => mergedById.get(cellId))
      .filter(Boolean)
      .map((cell, index) => ({
        ...cell,
        sql: normalizeCompareSql(cell.sql),
        compareIndex: index,
        compareLabel: compareCellLabel(cell, index),
      }));
  }

  function sqlCellsForCompare(notebookId) {
    return notebookCellsForCompare(notebookId).filter(
      (cell) => normalizeCellLanguage(cell.language) === "sql"
    );
  }

  function notebookCompareTargets(current) {
    const currentNotebookId = current?.notebookId || "";
    return notebookCompareIds(currentNotebookId)
      .map((notebookId) => {
        try {
          const metadata = notebookMetadata(notebookId);
          if (metadata?.deleted) {
            return null;
          }
          const cells = sqlCellsForCompare(notebookId).filter(
            (cell) => notebookId !== currentNotebookId || cell.cellId !== current?.cellId
          );
          if (!cells.length) {
            return null;
          }
          return {
            notebookId,
            title: normalizeNotebookTitleValue(metadata?.title, notebookId),
            cells,
          };
        } catch (_error) {
          return null;
        }
      })
      .filter(Boolean);
  }

  function defaultCompareTargetNotebookId(current, targets) {
    const currentNotebookId = current?.notebookId || "";
    if (targets.some((target) => target.notebookId === currentNotebookId)) {
      return currentNotebookId;
    }
    return targets[0]?.notebookId || "";
  }

  function queryCompareDiff(leftSql, rightSql) {
    const leftLines = compareSqlLines(leftSql);
    const rightLines = compareSqlLines(rightSql);
    const dp = Array.from({ length: leftLines.length + 1 }, () =>
      Array(rightLines.length + 1).fill(0)
    );

    for (let leftIndex = leftLines.length - 1; leftIndex >= 0; leftIndex -= 1) {
      for (let rightIndex = rightLines.length - 1; rightIndex >= 0; rightIndex -= 1) {
        dp[leftIndex][rightIndex] =
          leftLines[leftIndex] === rightLines[rightIndex]
            ? dp[leftIndex + 1][rightIndex + 1] + 1
            : Math.max(dp[leftIndex + 1][rightIndex], dp[leftIndex][rightIndex + 1]);
      }
    }

    const operations = [];
    let leftIndex = 0;
    let rightIndex = 0;
    while (leftIndex < leftLines.length && rightIndex < rightLines.length) {
      if (leftLines[leftIndex] === rightLines[rightIndex]) {
        operations.push({
          type: "equal",
          leftNumber: leftIndex + 1,
          rightNumber: rightIndex + 1,
          leftText: leftLines[leftIndex],
          rightText: rightLines[rightIndex],
        });
        leftIndex += 1;
        rightIndex += 1;
      } else if (dp[leftIndex + 1][rightIndex] >= dp[leftIndex][rightIndex + 1]) {
        operations.push({
          type: "remove",
          leftNumber: leftIndex + 1,
          rightNumber: null,
          leftText: leftLines[leftIndex],
          rightText: "",
        });
        leftIndex += 1;
      } else {
        operations.push({
          type: "add",
          leftNumber: null,
          rightNumber: rightIndex + 1,
          leftText: "",
          rightText: rightLines[rightIndex],
        });
        rightIndex += 1;
      }
    }

    while (leftIndex < leftLines.length) {
      operations.push({
        type: "remove",
        leftNumber: leftIndex + 1,
        rightNumber: null,
        leftText: leftLines[leftIndex],
        rightText: "",
      });
      leftIndex += 1;
    }

    while (rightIndex < rightLines.length) {
      operations.push({
        type: "add",
        leftNumber: null,
        rightNumber: rightIndex + 1,
        leftText: "",
        rightText: rightLines[rightIndex],
      });
      rightIndex += 1;
    }

    const rows = [];
    const summary = {
      unchanged: 0,
      changed: 0,
      added: 0,
      removed: 0,
    };

    for (let index = 0; index < operations.length; index += 1) {
      const operation = operations[index];
      const nextOperation = operations[index + 1];
      if (operation.type === "remove" && nextOperation?.type === "add") {
        rows.push({
          type: "changed",
          leftNumber: operation.leftNumber,
          rightNumber: nextOperation.rightNumber,
          leftText: operation.leftText,
          rightText: nextOperation.rightText,
        });
        summary.changed += 1;
        index += 1;
        continue;
      }
      if (operation.type === "equal") {
        summary.unchanged += 1;
        rows.push(operation);
        continue;
      }
      if (operation.type === "add") {
        summary.added += 1;
        rows.push({ ...operation, type: "added" });
        continue;
      }
      summary.removed += 1;
      rows.push({ ...operation, type: "removed" });
    }

    return {
      rows,
      summary,
    };
  }

  function queryCompareSummaryText(summary) {
    return [
      `${summary.unchanged} unchanged`,
      `${summary.changed} changed`,
      `${summary.added} added`,
      `${summary.removed} removed`,
    ].join(" | ");
  }

  function queryCompareLineNumber(value) {
    return Number.isInteger(value) ? String(value) : "";
  }

  function queryCompareLineMarkup(value) {
    const text = String(value ?? "");
    return text ? escapeHtml(text) : "&nbsp;";
  }

  function queryCompareTableMarkup(diff) {
    if (!diff.rows.length) {
      return `
        <div class="query-compare-empty-state">
          Both cells are empty.
        </div>
      `;
    }

    const rows = diff.rows
      .map(
        (row) => `
          <tr class="query-compare-row is-${escapeHtml(row.type)}">
            <td class="query-compare-line-number">${escapeHtml(queryCompareLineNumber(row.leftNumber))}</td>
            <td class="query-compare-line-code"><code>${queryCompareLineMarkup(row.leftText)}</code></td>
            <td class="query-compare-line-number">${escapeHtml(queryCompareLineNumber(row.rightNumber))}</td>
            <td class="query-compare-line-code"><code>${queryCompareLineMarkup(row.rightText)}</code></td>
          </tr>
        `
      )
      .join("");

    return `
      <table class="query-compare-table">
        <colgroup>
          <col class="query-compare-line-number-col">
          <col>
          <col class="query-compare-line-number-col">
          <col>
        </colgroup>
        <thead>
          <tr>
            <th colspan="2">Current cell</th>
            <th colspan="2">Target cell</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  }

  function setQueryCompareSelectOptions(select, options, selectedValue) {
    if (!(select instanceof HTMLSelectElement)) {
      return;
    }
    select.replaceChildren(
      ...options.map((option) => {
        const element = document.createElement("option");
        element.value = option.value;
        element.textContent = option.label;
        element.selected = option.value === selectedValue;
        return element;
      })
    );
  }

  function selectedQueryCompareTarget() {
    const notebook = state.notebooks.find((entry) => entry.notebookId === state.targetNotebookId);
    const cell =
      notebook?.cells.find((entry) => entry.cellId === state.targetCellId) ??
      notebook?.cells[0] ??
      null;
    return { notebook, cell };
  }

  function syncQueryCompareTargetSelection() {
    const { notebook, cell } = selectedQueryCompareTarget();
    if (!notebook) {
      state.targetNotebookId = "";
      state.targetCellId = "";
      return;
    }
    state.targetNotebookId = notebook.notebookId;
    state.targetCellId = cell?.cellId || "";
  }

  function render() {
    const dialog = queryCompareDialog();
    if (!dialog || !state.current) {
      return;
    }

    syncQueryCompareTargetSelection();
    const current = state.current;
    const hasTargets = state.notebooks.length > 0;
    const notebookSelect = dialog.querySelector("[data-query-compare-target-notebook]");
    const cellSelect = dialog.querySelector("[data-query-compare-target-cell]");
    const controls = dialog.querySelector("[data-query-compare-controls]");
    const empty = dialog.querySelector("[data-query-compare-empty]");
    const currentMeta = dialog.querySelector("[data-query-compare-current-meta]");
    const targetMeta = dialog.querySelector("[data-query-compare-target-meta]");
    const summary = dialog.querySelector("[data-query-compare-summary]");
    const body = dialog.querySelector("[data-query-compare-body]");
    const { notebook, cell } = selectedQueryCompareTarget();

    if (notebookSelect instanceof HTMLSelectElement) {
      setQueryCompareSelectOptions(
        notebookSelect,
        state.notebooks.map((entry) => ({
          value: entry.notebookId,
          label: entry.title,
        })),
        state.targetNotebookId
      );
      notebookSelect.disabled = !hasTargets;
    }

    if (cellSelect instanceof HTMLSelectElement) {
      setQueryCompareSelectOptions(
        cellSelect,
        (notebook?.cells || []).map((entry) => ({
          value: entry.cellId,
          label: entry.compareLabel,
        })),
        state.targetCellId
      );
      cellSelect.disabled = !hasTargets;
    }

    if (controls) {
      controls.hidden = !hasTargets;
    }
    if (empty) {
      empty.hidden = hasTargets;
      empty.textContent = "No other SQL cells are available to compare.";
    }
    if (currentMeta) {
      currentMeta.innerHTML = `
        <strong>Current cell</strong>
        <span>${escapeHtml(current.notebookTitle)}</span>
        <span>${escapeHtml(compareCellLabel(current, current.cellIndex))}</span>
      `;
    }

    if (!hasTargets || !cell || !notebook) {
      if (targetMeta) {
        targetMeta.innerHTML = "";
      }
      if (summary) {
        summary.textContent = "No comparison target selected.";
      }
      if (body) {
        body.innerHTML = "";
      }
      return;
    }

    if (targetMeta) {
      targetMeta.innerHTML = `
        <strong>Target cell</strong>
        <span>${escapeHtml(notebook.title)}</span>
        <span>${escapeHtml(cell.compareLabel)}</span>
      `;
    }

    const diff = queryCompareDiff(current.sql, cell.sql);
    if (summary) {
      summary.textContent = queryCompareSummaryText(diff.summary);
    }
    if (body) {
      body.innerHTML = queryCompareTableMarkup(diff);
    }
  }

  function open(triggerButton) {
    const editorRoot = triggerButton?.closest?.("[data-editor-root]");
    const cellRoot = triggerButton?.closest?.("[data-query-cell]");
    const notebookId = currentWorkspaceNotebookId();
    if (!(editorRoot instanceof Element) || !(cellRoot instanceof Element) || !notebookId) {
      return;
    }

    const currentSql = normalizeCompareSql(currentEditorSql(editorRoot));
    const current = {
      notebookId,
      notebookTitle: currentWorkspaceNotebookTitle(),
      cellId: String(cellRoot.dataset.cellId || "").trim(),
      cellIndex: currentCompareCellIndex(cellRoot),
      language: "sql",
      sql: currentSql,
    };
    const notebooks = notebookCompareTargets(current);
    const targetNotebookId = defaultCompareTargetNotebookId(current, notebooks);
    const targetCellId =
      notebooks.find((entry) => entry.notebookId === targetNotebookId)?.cells[0]?.cellId || "";

    state.current = current;
    state.notebooks = notebooks;
    state.targetNotebookId = targetNotebookId;
    state.targetCellId = targetCellId;

    const dialog = ensureQueryCompareDialog();
    render();
    if (typeof dialog.showModal === "function" && !dialog.open) {
      dialog.showModal();
    }
  }

  function handleChange(event) {
    const compareTargetNotebook = event.target.closest?.("[data-query-compare-target-notebook]");
    if (compareTargetNotebook instanceof HTMLSelectElement) {
      state.targetNotebookId = compareTargetNotebook.value;
      state.targetCellId =
        state.notebooks.find((entry) => entry.notebookId === compareTargetNotebook.value)
          ?.cells[0]?.cellId || "";
      render();
      return true;
    }

    const compareTargetCell = event.target.closest?.("[data-query-compare-target-cell]");
    if (compareTargetCell instanceof HTMLSelectElement) {
      state.targetCellId = compareTargetCell.value;
      render();
      return true;
    }

    return false;
  }

  return {
    handleChange,
    open,
  };
}

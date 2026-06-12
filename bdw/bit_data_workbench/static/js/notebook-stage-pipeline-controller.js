export function createNotebookStagePipelineController(helpers) {
  const {
    createCellId,
    escapeHtml,
    fetchJsonOrThrow,
    formatQueryDuration,
    getCurrentNotebookId,
    getNotebookMetadata,
    normalizeCellLanguage,
    normalizeCellStage,
    normalizeNotebookPipelineMode,
    normalizePipelinePaths,
    openPublishDialogForSource,
    refreshSidebar,
    requestCellRun,
    revealDataSourceSidebarBrowser,
    setCellStage,
    setNotebookCells,
    setNotebookPipelinePaths,
    setNotebookPipelineMode,
    showConfirmDialog,
    showMessageDialog,
  } = helpers;

  let materializedStagesVersion = null;
  const graphByNotebookId = new Map();
  const selectedStageByNotebookId = new Map();
  let contextMenu = null;
  let priorityPopover = null;
  let modeChangeVersion = 0;
  let materializedOutputSignature = "";
  const passThroughCellRuns = new Set();
  const defaultFirstStageTitle = "my first stage";
  const defaultFirstStageDescription = "This is the stage description";
  const explorationModeDetail = "Keeps cells independent for ad-hoc SQL or Python work.";
  const pipelineModeDetail = "Links SQL cells into staged materialized data products and dependency-aware runs.";
  const explorationModeTitle =
    "Notebook mode: Exploration. Click to enable Pipeline mode, which links SQL cells into staged materialized data products with dependency-aware runs.";
  const pipelineModeTitle =
    "Notebook mode: Pipeline. Click to return to Exploration mode, which keeps cells independent for ad-hoc SQL or Python work.";
  const failedStageStatuses = new Set(["failed", "cancelled", "canceled", "aborted", "incomplete"]);
  const terminalStageRunStatuses = new Set(["completed", ...failedStageStatuses]);
  function stageReferencePattern() {
    return /(^|[^A-Za-z0-9_$])stage\.([A-Za-z_][A-Za-z0-9_$]*)/gi;
  }

  function stageAlias(value, fallback) {
    const normalized = String(value || fallback || "stage")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .replace(/_+/g, "_");
    return normalized || "stage";
  }

  function cellStageDefaults(cell, index) {
    const stage = normalizeCellStage(cell.stage);
    const defaultTitle =
      index === 0 && !stage.title ? defaultFirstStageTitle : "";
    const title =
      stage.title ||
      defaultTitle ||
      stageAlias(stage.alias, cell.cellId || `stage_${index + 1}`)
        .replace(/_/g, " ")
        .replace(/\b\w/g, (letter) => letter.toUpperCase());
    const alias = stageAlias(stage.alias, title || cell.cellId || `stage_${index + 1}`);
    return {
      ...stage,
      enabled: true,
      stageId: stage.stageId || `stage-${cell.cellId || index + 1}`,
      alias,
      title,
      description:
        stage.description ||
        (index === 0 ? defaultFirstStageDescription : ""),
      kind: stage.kind || "intermediate",
      materialize: stage.materialize !== false,
    };
  }

  function initializedPipelineCells(cells) {
    return (cells || []).map((cell, index) => {
      if (normalizeCellLanguage(cell.language) !== "sql") {
        return cell;
      }
      return {
        ...cell,
        stage: cellStageDefaults(cell, index),
      };
    });
  }

  function currentWorkspaceRoot() {
    const notebookId = getCurrentNotebookId();
    if (!notebookId) {
      return null;
    }
    return document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"]`
    );
  }

  function pipelinePayload(notebookId, { startStageId = "" } = {}) {
    const metadata = getNotebookMetadata(notebookId);
    const payload = {
      notebookId,
      notebookTitle: metadata.title || "Notebook",
      pipelinePaths: normalizePipelinePaths(metadata.pipelinePaths),
      cells: (metadata.cells || []).map((cell) => ({
        cellId: cell.cellId,
        language: normalizeCellLanguage(cell.language),
        sql: cell.sql || "",
        dataSources: Array.isArray(cell.dataSources) ? cell.dataSources : [],
        queryOptions: cell.queryOptions || {},
        stage: normalizeCellStage(cell.stage),
      })),
    };
    const normalizedStartStageId = String(startStageId || "").trim();
    if (normalizedStartStageId) {
      payload.startStageId = normalizedStartStageId;
    }
    return payload;
  }

  function pipelineEnabled(notebookId) {
    return normalizeNotebookPipelineMode(getNotebookMetadata(notebookId).pipelineMode) === "pipeline";
  }

  function statusLabel(value) {
    const status = String(value || "planned").trim().toLowerCase();
    if (status === "valid") {
      return "OK";
    }
    if (status === "queued") {
      return "Waiting";
    }
    return status ? `${status.slice(0, 1).toUpperCase()}${status.slice(1)}` : "Planned";
  }

  function statusClass(value) {
    return String(value || "planned").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-") || "planned";
  }

  function statusIsFailure(value) {
    return failedStageStatuses.has(String(value || "").trim().toLowerCase());
  }

  function pipelineTooltipAttributes(label) {
    const copy = String(label || "").trim();
    if (!copy) {
      return "";
    }
    return `aria-label="${escapeHtml(copy)}" data-pipeline-tooltip="${escapeHtml(copy)}"`;
  }

  function graphPaths(graph) {
    return Array.isArray(graph?.paths)
      ? graph.paths
          .filter((path) => path && typeof path === "object")
          .map((path, index) => ({
            pathId: String(path.pathId || path.path_id || "").trim(),
            terminalStageId: String(path.terminalStageId || path.terminal_stage_id || "").trim(),
            terminalStageTitle: String(path.terminalStageTitle || path.terminal_stage_title || "").trim(),
            label: String(path.label || path.name || path.terminalStageTitle || path.terminal_stage_title || "").trim(),
            stageIds: Array.isArray(path.stageIds) ? path.stageIds.map((item) => String(item || "").trim()).filter(Boolean) : [],
            priority: Number.parseInt(path.priority || path.rank || index + 1, 10) || index + 1,
          }))
          .filter((path) => path.pathId || path.terminalStageId)
          .sort((left, right) => left.priority - right.priority)
      : [];
  }

  function priorityPathPayload(paths) {
    return normalizePipelinePaths(
      (paths || []).map((path, index) => ({
        pathId: path.pathId || (path.terminalStageId ? `path-${path.terminalStageId}` : ""),
        terminalStageId: path.terminalStageId,
        label: path.label || path.terminalStageTitle || "",
        priority: index + 1,
      }))
    );
  }

  function terminalPathForStage(graph, stageId) {
    const normalizedStageId = String(stageId || "").trim();
    if (!normalizedStageId) {
      return null;
    }
    return graphPaths(graph).find((path) => String(path.terminalStageId || "") === normalizedStageId) || null;
  }

  function priorityRankBadge(path, variant = "node", pathCount = 0) {
    if (!path || pathCount <= 1) {
      return "";
    }
    const rank = Number.parseInt(path.priority || 0, 10);
    if (!Number.isFinite(rank) || rank <= 0) {
      return "";
    }
    const label = path.label || path.terminalStageTitle || "Priority path";
    return `
      <span
        class="pipeline-priority-rank-badge pipeline-priority-rank-badge-${escapeHtml(variant)}"
        ${pipelineTooltipAttributes(`Priority path ${rank}: ${label}`)}
      >P${escapeHtml(String(rank))}</span>
    `;
  }

  function prioritySummaryCopy(paths) {
    if (!paths.length) {
      return "Priority paths";
    }
    if (paths.length === 1) {
      return "Priority: single path";
    }
    return `Priority: ${paths[0].label || paths[0].terminalStageTitle || "path"} first`;
  }

  function renderPrioritySummary(workspaceRoot, graph) {
    const button = workspaceRoot?.querySelector("[data-pipeline-priority-paths]");
    if (!button) {
      return;
    }
    const paths = graphPaths(graph);
    const summary = button.querySelector("[data-pipeline-priority-summary]");
    if (summary) {
      summary.textContent = prioritySummaryCopy(paths);
    }
    const disabled = paths.length <= 1;
    button.disabled = disabled;
    button.classList.toggle("is-disabled", disabled);
    button.dataset.pipelineTooltip = disabled
      ? "This pipeline has one terminal path, so no path priority is needed."
      : "Rank terminal paths; priority is used only when branches are ready to run.";
    button.setAttribute("aria-expanded", priorityPopover?.dataset?.notebookId === String(graph?.notebookId || "") ? "true" : "false");
  }

  function formatStageDuration(durationMs) {
    const numeric = Number(durationMs);
    if (!Number.isFinite(numeric) || numeric < 0) {
      return "-";
    }
    if (typeof formatQueryDuration === "function") {
      return formatQueryDuration(numeric);
    }
    if (numeric < 1000) {
      return `${Math.round(numeric)} ms`;
    }
    if (numeric < 60000) {
      return `${(numeric / 1000).toFixed(numeric < 10000 ? 1 : 0)} s`;
    }
    return `${Math.floor(numeric / 60000)} min ${Math.round((numeric % 60000) / 1000)} s`;
  }

  function timestampMs(value) {
    const parsed = Date.parse(String(value || ""));
    return Number.isFinite(parsed) ? parsed : null;
  }

  function stageRunDurationMs(node) {
    const run = node?.latestRun;
    if (!run) {
      return null;
    }
    const direct = Number(run.durationMs);
    if (Number.isFinite(direct) && direct >= 0) {
      return direct;
    }
    const started = timestampMs(run.startedAt);
    if (started === null) {
      return null;
    }
    const status = String(run.status || "").toLowerCase();
    const completed = timestampMs(run.completedAt);
    const ended = completed ?? (status === "running" || status === "queued" ? Date.now() : null);
    return ended === null ? null : Math.max(0, ended - started);
  }

  function stageDurationCopy(node) {
    const durationMs = stageRunDurationMs(node);
    return durationMs === null ? "-" : formatStageDuration(durationMs);
  }

  function pipelineTotalDurationMs(graph) {
    const durations = (Array.isArray(graph?.nodes) ? graph.nodes : [])
      .map((node) => stageRunDurationMs(node))
      .filter((durationMs) => durationMs !== null);
    if (!durations.length) {
      return null;
    }
    return durations.reduce((total, durationMs) => total + durationMs, 0);
  }

  function pipelineTotalDurationCopy(graph) {
    const durationMs = pipelineTotalDurationMs(graph);
    return durationMs === null ? "-" : formatStageDuration(durationMs);
  }

  function activeRunsForGraph(graph) {
    const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
    const nodeById = new Map(nodes.map((node) => [String(node.stageId || ""), node]));
    return (Array.isArray(graph?.activeRuns) ? graph.activeRuns : []).filter((run) => {
      const status = String(run?.status || "running").toLowerCase();
      if (terminalStageRunStatuses.has(status)) {
        return false;
      }
      const stageIds = Array.isArray(run?.stageIds) ? run.stageIds.map((item) => String(item)) : [];
      if (!nodes.length || !stageIds.length) {
        return true;
      }
      return stageIds.some((stageId) => stageIsActiveInRun(nodeById.get(stageId)));
    });
  }

  function graphHasActiveRun(graph) {
    return activeRunsForGraph(graph).length > 0;
  }

  function runCoversWholePipeline(run, graph) {
    const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
    if (!nodes.length) {
      return false;
    }
    const runStageIds = new Set((Array.isArray(run?.stageIds) ? run.stageIds : []).map((item) => String(item)));
    if (!runStageIds.size) {
      return false;
    }
    return nodes.every((node) => runStageIds.has(String(node?.stageId || "")));
  }

  function graphHasActiveWholePipelineRun(graph) {
    return activeRunsForGraph(graph).some((run) => runCoversWholePipeline(run, graph));
  }

  function nodeDescription(node) {
    return [
      node?.description,
      node?.runWarning,
      node?.latestRun?.error,
      node?.latestRun?.message,
      node?.title,
    ]
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .join(" ");
  }

  function stageQueryReference(node) {
    const queryPath = String(
      node?.latestRevision?.queryReference || node?.latestRevision?.queryPath || ""
    ).trim();
    if (queryPath) {
      return queryPath;
    }
    const alias = String(node?.alias || "").trim();
    return alias ? `stage.${alias}` : "";
  }

  function stageStorageReference(node) {
    const queryPath = String(
      node?.latestRevision?.queryReference || node?.latestRevision?.queryPath || ""
    ).trim();
    return queryPath.startsWith("s3.") ? queryPath : "";
  }

  function sleep(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  function errorMessage(error, fallback = "The pipeline action failed.") {
    return error instanceof Error && error.message ? error.message : fallback;
  }

  function notebookIdForCellRoot(cellRoot) {
    return String(
      cellRoot?.closest?.("[data-workspace-notebook]")?.dataset?.notebookId ||
        getCurrentNotebookId() ||
        ""
    ).trim();
  }

  function stageAliasReferences(sql) {
    const seen = new Set();
    const references = [];
    for (const match of String(sql || "").matchAll(stageReferencePattern())) {
      const alias = stageAlias(match[2], "stage");
      const normalized = alias.toLowerCase();
      if (seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      references.push({
        alias,
        reference: `stage.${alias}`,
      });
    }
    return references;
  }

  function stageNodesForNotebook(notebookId) {
    const graph = graphByNotebookId.get(notebookId);
    if (Array.isArray(graph?.nodes) && graph.nodes.length) {
      return graph.nodes;
    }
    const metadata = getNotebookMetadata(notebookId);
    return (metadata.cells || [])
      .map((cell, index) => {
        if (normalizeCellLanguage(cell.language) !== "sql") {
          return null;
        }
        const stage = cellStageDefaults(cell, index);
        return {
          ...stage,
          cellId: cell.cellId,
          latestRevision: null,
        };
      })
      .filter(Boolean);
  }

  function stageNodeForCell(notebookId, cellId) {
    const normalizedCellId = String(cellId || "").trim();
    return stageNodesForNotebook(notebookId).find(
      (node) => String(node?.cellId || "") === normalizedCellId
    );
  }

  function stageAliasMap(notebookId) {
    const entries = new Map();
    stageNodesForNotebook(notebookId).forEach((node) => {
      const alias = stageAlias(node?.alias, node?.title || node?.cellId || "stage");
      if (alias) {
        entries.set(alias.toLowerCase(), node);
      }
    });
    return entries;
  }

  function validateStageAliasesForCell(cellRoot, sql) {
    const notebookId = notebookIdForCellRoot(cellRoot);
    if (!notebookId || !pipelineEnabled(notebookId)) {
      return {
        aliases: [],
        localRelations: {},
        missingAliases: [],
        validationSql: String(sql || ""),
      };
    }
    const aliasMap = stageAliasMap(notebookId);
    const aliases = [];
    const missingAliases = [];
    const localRelations = {};
    stageAliasReferences(sql).forEach(({ alias, reference }) => {
      aliases.push(reference);
      const node = aliasMap.get(alias.toLowerCase());
      if (!node) {
        missingAliases.push(reference);
        return;
      }
      localRelations[reference] = String(
        node.latestRevision?.queryReference || node.latestRevision?.queryPath || reference
      );
    });
    return {
      aliases,
      localRelations,
      missingAliases,
      validationSql: String(sql || ""),
    };
  }

  function prepareQuerySqlForCell(cellRoot, sql) {
    const notebookId = notebookIdForCellRoot(cellRoot);
    if (!notebookId || !pipelineEnabled(notebookId)) {
      return String(sql || "");
    }
    const aliasMap = stageAliasMap(notebookId);
    return String(sql || "").replace(stageReferencePattern(), (match, prefix, rawAlias) => {
      const alias = stageAlias(rawAlias, "stage");
      const node = aliasMap.get(alias.toLowerCase());
      const queryPath = String(
        node?.latestRevision?.queryReference || node?.latestRevision?.queryPath || ""
      ).trim();
      return queryPath ? `${prefix}${queryPath}` : match;
    });
  }

  function graphNodeIcon(node) {
    return `
      <svg class="pipeline-node-icon" viewBox="0 0 24 24" aria-hidden="true">
        <rect x="4.5" y="3.5" width="15" height="17"></rect>
        <path d="M4.5 9h15"></path>
        <path d="M4.5 15h15"></path>
        <path d="M9.5 3.5v17"></path>
        <path d="M14.5 3.5v17"></path>
      </svg>
    `;
  }

  function publishedIcon(node) {
    if (!node?.published) {
      return "";
    }
    return `
      <svg class="pipeline-published-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M20 6 9 17l-5-5"></path>
      </svg>
    `;
  }

  function obsoleteIcon(node) {
    if (String(node?.status || "").toLowerCase() !== "obsolete") {
      return "";
    }
    return `
      <svg class="pipeline-obsolete-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M12 8v5"></path>
        <path d="M12 17h.01"></path>
        <path d="M10 3h4l8 16H2z"></path>
      </svg>
    `;
  }

  function statusIcon(node) {
    const status = String(node?.status || "").trim().toLowerCase();
    const label = statusLabel(node?.status);
    if (status === "valid") {
      return `
        <svg class="pipeline-status-icon pipeline-status-icon-ok" viewBox="0 0 24 24" role="img" aria-label="${escapeHtml(label)}">
          <path d="M20 6 9 17l-5-5"></path>
        </svg>
      `;
    }
    if (status === "running") {
      return `
        <svg class="pipeline-status-icon pipeline-status-icon-running pipeline-spinner" viewBox="0 0 24 24" role="img" aria-label="${escapeHtml(label)}">
          <path d="M12 3a9 9 0 1 1-9 9"></path>
        </svg>
      `;
    }
    if (status === "queued") {
      return `
        <svg class="pipeline-status-icon pipeline-status-icon-waiting" viewBox="0 0 24 24" role="img" aria-label="${escapeHtml(label)}">
          <circle cx="12" cy="12" r="8"></circle>
          <path d="M12 7v5l3 2"></path>
        </svg>
      `;
    }
    if (statusIsFailure(status)) {
      return `
        <svg class="pipeline-status-icon pipeline-status-icon-failed" viewBox="0 0 24 24" role="img" aria-label="${escapeHtml(label)}">
          <path d="M8 8l8 8"></path>
          <path d="M16 8l-8 8"></path>
        </svg>
      `;
    }
    return `
      <svg class="pipeline-status-icon pipeline-status-icon-attention" viewBox="0 0 24 24" role="img" aria-label="${escapeHtml(label)}">
        <path d="M12 8v5"></path>
        <path d="M12 17h.01"></path>
        <path d="M10 3h4l8 16H2z"></path>
      </svg>
    `;
  }

  function stageIsProcessing(node) {
    const status = String(node?.status || "").trim().toLowerCase();
    return status === "running";
  }

  function stageIsWaiting(node) {
    const status = String(node?.status || "").trim().toLowerCase();
    return status === "queued";
  }

  function stageIsActiveInRun(node) {
    const status = String(node?.status || "").trim().toLowerCase();
    return status === "running" || status === "queued";
  }

  function stageActionButton(node, variant = "graph") {
    const stageId = String(node?.stageId || "");
    if (!stageId) {
      return "";
    }
    const processing = stageIsProcessing(node);
    const waiting = stageIsWaiting(node);
    const dataAttribute = processing
      ? `data-cancel-pipeline-stage="${escapeHtml(stageId)}"`
      : waiting
      ? `data-pipeline-stage-waiting="${escapeHtml(stageId)}" aria-disabled="true"`
      : `data-run-pipeline-stage="${escapeHtml(stageId)}"`;
    const label = processing
      ? "Cancel this running stage"
      : waiting
      ? "Waiting for earlier stages to finish"
      : "Run this stage";
    return `
      <button
        type="button"
        class="pipeline-stage-action-button pipeline-stage-action-button-${escapeHtml(variant)}${processing ? " is-running" : ""}${waiting ? " is-waiting" : ""}"
        ${dataAttribute}
        ${pipelineTooltipAttributes(label)}
      >
        ${menuIcon(processing ? "stop" : "run")}
      </button>
    `;
  }

  function stageRunFromButton(node, variant = "graph") {
    const stageId = String(node?.stageId || "");
    if (!stageId) {
      return "";
    }
    const disabled = stageIsActiveInRun(node);
    const label = disabled
      ? "Wait for the active stage run to finish"
      : "Run pipeline from this stage";
    return `
      <button
        type="button"
        class="pipeline-stage-action-button pipeline-stage-action-button-${escapeHtml(variant)} pipeline-stage-action-button-from${disabled ? " is-waiting" : ""}"
        ${disabled ? `data-pipeline-stage-waiting-from="${escapeHtml(stageId)}" aria-disabled="true"` : `data-run-pipeline-from-stage="${escapeHtml(stageId)}"`}
        ${pipelineTooltipAttributes(label)}
      >
        ${menuIcon("runFrom")}
      </button>
    `;
  }

  function stageActionButtons(node, variant = "graph") {
    return `
      <span class="pipeline-stage-action-group pipeline-stage-action-group-${escapeHtml(variant)}">
        ${stageActionButton(node, variant)}
        ${stageRunFromButton(node, variant)}
      </span>
    `;
  }

  function nodeStatusMarker(node) {
    const status = String(node?.status || "").trim().toLowerCase();
    let tone = "attention";
    let icon = '<path d="M12 7v6"></path><path d="M12 17h.01"></path>';
    let label = "Stage needs attention";
    if (status === "valid") {
      tone = "ok";
      icon = '<path d="M18 8 10.5 15.5 6 11"></path>';
      label = "Stage is ready";
    } else if (statusIsFailure(status)) {
      tone = "failed";
      icon = '<path d="M8 8l8 8"></path><path d="M16 8l-8 8"></path>';
      label =
        status === "cancelled" || status === "canceled"
          ? "Stage cancelled"
          : status === "aborted"
            ? "Stage aborted"
            : status === "incomplete"
              ? "Stage incomplete"
              : "Stage failed";
    } else if (status === "running") {
      tone = "running";
      icon = '<path d="M12 3a9 9 0 1 1-9 9"></path>';
      label = "Stage is running now";
    } else if (status === "queued") {
      tone = "waiting";
      icon = '<circle cx="12" cy="12" r="7"></circle><path d="M12 8v4l3 2"></path>';
      label = "Waiting for earlier stages to finish";
    } else if (status === "obsolete") {
      label = "Stage is obsolete and should be rerun";
    }
    return `
      <span class="pipeline-node-state pipeline-node-state-${tone}" ${pipelineTooltipAttributes(label)}>
        <svg class="${tone === "running" ? "pipeline-spinner" : ""}" viewBox="0 0 24 24" aria-hidden="true">${icon}</svg>
      </span>
    `;
  }

  function menuIcon(name) {
    const icons = {
      inspect: '<path d="M3 12s3.2-5 9-5 9 5 9 5-3.2 5-9 5-9-5-9-5z"></path><circle cx="12" cy="12" r="2.3"></circle>',
      navigate: '<path d="M4 5h16v14H4z"></path><path d="M8 9h8"></path><path d="M8 13h5"></path><path d="M15 13l3 3"></path><path d="M18 16l-3 3"></path>',
      copy: '<rect x="8" y="8" width="10" height="10"></rect><path d="M6 16H4V4h12v2"></path>',
      publish: '<path d="M12 19V5"></path><path d="M7 10l5-5 5 5"></path><path d="M5 19h14"></path>',
      derive: '<path d="M5 6h5a4 4 0 0 1 4 4v8"></path><path d="M10 6l-3-3"></path><path d="M10 6 7 9"></path><path d="M14 18l-3-3"></path><path d="M14 18l3-3"></path>',
      fork: '<path d="M5 12h5"></path><path d="M10 12c3 0 4-5 8-5"></path><path d="M10 12c3 0 4 5 8 5"></path><path d="M18 7l-2-2"></path><path d="M18 7l-2 2"></path><path d="M18 17l-2-2"></path><path d="M18 17l-2 2"></path>',
      run: '<path d="M8 5v14l11-7z"></path>',
      runFrom: '<path d="M6 5v14l9-7z"></path><path d="M15 7h3v10h-3"></path><path d="M18 12h3"></path>',
      stop: '<rect x="7" y="7" width="10" height="10"></rect>',
      up: '<path d="M12 5v14"></path><path d="M6 11l6-6 6 6"></path>',
      down: '<path d="M12 19V5"></path><path d="M6 13l6 6 6-6"></path>',
      delete: '<path d="M4 7h16"></path><path d="M9 7V4h6v3"></path><path d="M7 7l1 13h8l1-13"></path><path d="M10 11v5"></path><path d="M14 11v5"></path>',
    };
    return `
      <svg class="pipeline-menu-icon pipeline-menu-icon-${escapeHtml(name)}" viewBox="0 0 24 24" aria-hidden="true">
        ${icons[name] || icons.inspect}
      </svg>
    `;
  }

  function menuItem({ action, label, icon, disabled = false, danger = false }) {
    return `
      <button
        type="button"
        class="workspace-action-menu-item${danger ? " workspace-action-menu-item-danger" : ""}"
        data-pipeline-menu-action="${escapeHtml(action)}"
        ${disabled ? "disabled" : ""}
      >
        ${menuIcon(icon || action)}
        <span>${escapeHtml(label)}</span>
      </button>
    `;
  }

  function graphLayout(graph) {
    const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
    const layers = new Map();
    nodes.forEach((node) => {
      const layer = Math.max(0, Number(node.layer || 0));
      const entries = layers.get(layer) || [];
      entries.push(node);
      layers.set(layer, entries);
    });
    const positions = new Map();
    const nodeMinWidth = 350;
    const nodeMinHeight = 88;
    const nodeWidth = nodeMinWidth;
    const nodeHeight = nodeMinHeight;
    const gapX = 62;
    const gapY = 24;
    const maxLayer = Math.max(0, ...Array.from(layers.keys()));
    let height = 120;
    layers.forEach((layerNodes, layer) => {
      layerNodes
        .sort((left, right) => Number(left.order || 0) - Number(right.order || 0))
        .forEach((node, index) => {
          const x = 24 + layer * (nodeWidth + gapX);
          const y = 26 + index * (nodeHeight + gapY);
          positions.set(String(node.stageId), { x, y, width: nodeWidth, height: nodeHeight });
          height = Math.max(height, y + nodeHeight + 26);
        });
    });
    return {
      positions,
      width: Math.max(540, 48 + (maxLayer + 1) * nodeWidth + maxLayer * gapX),
      height,
    };
  }

  function connectorPath(from, to) {
    const startX = from.x + from.width;
    const startY = from.y + from.height / 2;
    const endX = to.x;
    const endY = to.y + to.height / 2;
    const midX = startX + Math.max(28, (endX - startX) / 2);
    return `M ${startX} ${startY} L ${midX} ${startY} L ${midX} ${endY} L ${endX} ${endY}`;
  }

  function renderGraph(workspaceRoot, graph) {
    const graphRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-graph]");
    if (!graphRoot) {
      return;
    }
    graphRoot.removeAttribute("aria-busy");
    graphRoot.classList.remove("is-loading", "is-error", "is-refreshing");
    const { positions, width, height } = graphLayout(graph);
    const selectedStageId = selectedStageByNotebookId.get(graph.notebookId) || graph.defaultSelectedStageId || "";
    const paths = graphPaths(graph);
    const edgesMarkup = (graph.edges || [])
      .map((edge) => {
        const from = positions.get(String(edge.fromStageId || ""));
        const to = positions.get(String(edge.toStageId || ""));
        if (!from || !to) {
          return "";
        }
        const targetNode = (graph.nodes || []).find((node) => String(node.stageId) === String(edge.toStageId || ""));
        return `<path class="pipeline-edge pipeline-edge-${statusClass(targetNode?.status)}" d="${connectorPath(from, to)}" marker-end="url(#pipeline-arrowhead)"></path>`;
      })
      .join("");
    const glowFilterId = `pipeline-node-running-glow-${String(graph.notebookId || "default").replace(/[^A-Za-z0-9_-]/g, "_")}`;
    const nodesMarkup = (graph.nodes || [])
      .map((node) => {
        const box = positions.get(String(node.stageId));
        if (!box) {
          return "";
        }
        const selected = String(node.stageId) === selectedStageId;
        const description = nodeDescription(node);
        const tooltip = description || node.title || node.alias || "Stage";
        const priorityPath = terminalPathForStage(graph, node.stageId);
        return `
          <g
            class="pipeline-node pipeline-node-${statusClass(node.status)}${selected ? " is-selected" : ""}"
            data-pipeline-stage-node="${escapeHtml(node.stageId)}"
            transform="translate(${box.x} ${box.y})"
            tabindex="0"
          >
            <rect class="pipeline-node-glow" x="-2" y="-2" width="${box.width + 4}" height="${box.height + 4}" filter="url(#${escapeHtml(glowFilterId)})"></rect>
            <rect class="pipeline-node-rect" width="${box.width}" height="${box.height}"></rect>
            <foreignObject x="0" y="0" width="${box.width}" height="${box.height}">
              <div class="pipeline-node-body">
                <div class="pipeline-node-top">
                  ${graphNodeIcon(node)}
                  <span class="pipeline-node-title" ${pipelineTooltipAttributes(tooltip)}>${escapeHtml(node.title || node.alias || "Stage")}</span>
                  ${publishedIcon(node)}
                  ${obsoleteIcon(node)}
                  ${priorityRankBadge(priorityPath, "node", paths.length)}
                  ${stageActionButtons(node, "graph")}
                  ${nodeStatusMarker(node)}
                </div>
                <div class="pipeline-node-bottom">
                  <span class="pipeline-node-alias">${escapeHtml(stageQueryReference(node))}</span>
                </div>
              </div>
            </foreignObject>
          </g>
        `;
      })
      .join("");
    graphRoot.innerHTML = `
      <svg class="notebook-pipeline-svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="Notebook pipeline graph">
        <defs>
          <filter id="${escapeHtml(glowFilterId)}" x="-40%" y="-40%" width="180%" height="180%">
            <feGaussianBlur in="SourceGraphic" stdDeviation="4" result="pipelineNodeGlowBlur"></feGaussianBlur>
            <feColorMatrix
              in="pipelineNodeGlowBlur"
              type="matrix"
              values="0 0 0 0 0.12  0 0 0 0 0.48  0 0 0 0 0.62  0 0 0 0.95 0"
              result="pipelineNodeGlowColor"
            ></feColorMatrix>
            <feMerge>
              <feMergeNode in="pipelineNodeGlowColor"></feMergeNode>
              <feMergeNode in="SourceGraphic"></feMergeNode>
            </feMerge>
          </filter>
          <marker id="pipeline-arrowhead" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
            <path d="M0 0 L8 4 L0 8 Z" class="pipeline-arrowhead"></path>
          </marker>
        </defs>
        <g class="pipeline-edges">${edgesMarkup}</g>
        <g class="pipeline-nodes">${nodesMarkup}</g>
      </svg>
    `;
  }

  function renderGraphPlaceholder(workspaceRoot, { status = "", tone = "loading", message = "" } = {}) {
    const graphRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-graph]");
    const statusRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-status]");
    const tableRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-table]");
    const totalDurationRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-total-duration]");
    const tableTotalRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-table-duration-total]");
    const normalizedTone = tone === "error" ? "error" : "loading";
    if (statusRoot) {
      statusRoot.textContent = status || (normalizedTone === "error" ? "Pipeline graph could not be built." : "Loading pipeline graph...");
      statusRoot.classList.toggle("is-error", normalizedTone === "error");
    }
    if (totalDurationRoot) {
      totalDurationRoot.textContent = "Total duration -";
    }
    if (tableRoot) {
      tableRoot.innerHTML = "";
    }
    if (tableTotalRoot) {
      tableTotalRoot.textContent = "-";
    }
    if (!graphRoot) {
      return;
    }
    graphRoot.setAttribute("aria-busy", normalizedTone === "loading" ? "true" : "false");
    graphRoot.classList.toggle("is-loading", normalizedTone === "loading");
    graphRoot.classList.toggle("is-error", normalizedTone === "error");
    graphRoot.classList.remove("is-refreshing");
    const copy = message || (normalizedTone === "error" ? "The graph request failed. Check the browser console or retry." : "Preparing stages and dependencies...");
    graphRoot.innerHTML = `
      <div class="pipeline-graph-placeholder pipeline-graph-placeholder-${normalizedTone}" data-pipeline-graph-placeholder>
        ${escapeHtml(copy)}
      </div>
    `;
  }

  function graphHasRenderedContent(workspaceRoot) {
    const graphRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-graph]");
    const tableRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-table]");
    return Boolean(
      graphRoot?.querySelector(".notebook-pipeline-svg, .pipeline-node") ||
      tableRoot?.querySelector(".pipeline-stage-row")
    );
  }

  function setGraphRefreshPending(workspaceRoot, pending) {
    const graphRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-graph]");
    if (!graphRoot) {
      return;
    }
    if (pending) {
      graphRoot.setAttribute("aria-busy", "true");
    } else {
      graphRoot.removeAttribute("aria-busy");
    }
    graphRoot.classList.toggle("is-refreshing", Boolean(pending));
  }

  function renderGraphRefreshError(workspaceRoot, message) {
    setGraphRefreshPending(workspaceRoot, false);
    const statusRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-status]");
    if (statusRoot) {
      statusRoot.textContent = message || "Pipeline graph could not be built.";
      statusRoot.classList.add("is-error");
    }
  }

  function dependencyCopy(node, graph) {
    const byId = new Map((graph.nodes || []).map((item) => [String(item.stageId), item]));
    const names = (node.predecessorStageIds || [])
      .map((stageId) => byId.get(String(stageId))?.title || "")
      .filter(Boolean);
    return names.length ? names.join(", ") : "None";
  }

  function renderTable(workspaceRoot, graph) {
    const tableRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-table]");
    if (!tableRoot) {
      return;
    }
    const selectedStageId = selectedStageByNotebookId.get(graph.notebookId) || graph.defaultSelectedStageId || "";
    const paths = graphPaths(graph);
    tableRoot.innerHTML = (graph.nodes || [])
      .map((node) => {
        const priorityPath = terminalPathForStage(graph, node.stageId);
        const rowStatus = statusClass(node.status);
        return `
          <tr
            class="pipeline-stage-row pipeline-stage-row-${rowStatus}${String(node.stageId) === selectedStageId ? " is-selected" : ""}"
            data-pipeline-stage-row="${escapeHtml(node.stageId)}"
          >
            <td ${pipelineTooltipAttributes(nodeDescription(node))}>
              <span class="pipeline-table-stage-title-row">
                <span class="pipeline-table-stage-title">${escapeHtml(node.title || node.alias || "Stage")}</span>
                ${priorityRankBadge(priorityPath, "table", paths.length)}
              </span>
              <span class="pipeline-table-stage-alias">${escapeHtml(stageQueryReference(node))}</span>
            </td>
            <td class="pipeline-table-run-cell">${stageActionButtons(node, "table")}</td>
            <td><span class="pipeline-table-status" ${pipelineTooltipAttributes(statusLabel(node.status))}>${statusIcon(node)}</span></td>
            <td>${escapeHtml(dependencyCopy(node, graph))}</td>
            <td><span class="pipeline-table-duration">${escapeHtml(stageDurationCopy(node))}</span></td>
            <td>${node.latestRevision ? escapeHtml(String(node.latestRevision.rowCount ?? 0)) : "-"}</td>
            <td>
              ${publishedIcon(node)}
              <button type="button" class="pipeline-row-menu-button" data-pipeline-stage-menu="${escapeHtml(node.stageId)}" ${pipelineTooltipAttributes("Stage actions")}>...</button>
            </td>
          </tr>
        `;
      })
      .join("");
    const tableTotalRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-table-duration-total]");
    if (tableTotalRoot) {
      tableTotalRoot.textContent = pipelineTotalDurationCopy(graph);
    }
  }

  function renderStatus(workspaceRoot, graph) {
    const statusRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-status]");
    if (!statusRoot) {
      return;
    }
    const totalDurationRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-total-duration]");
    if (totalDurationRoot) {
      totalDurationRoot.textContent = `Total duration ${pipelineTotalDurationCopy(graph)}`;
    }
    const errors = (graph.diagnostics || []).filter((item) => item.severity === "error");
    if (errors.length) {
      statusRoot.textContent = errors[0].message || "Pipeline graph needs attention.";
      statusRoot.classList.add("is-error");
      return;
    }
    statusRoot.classList.remove("is-error");
    statusRoot.textContent = `${graph.nodes?.length || 0} stages in dependency order.`;
  }

  function renderCellStageState(workspaceRoot, graph) {
    const byCellId = new Map((graph.nodes || []).map((node) => [String(node.cellId), node]));
    const byId = new Map((graph.nodes || []).map((node) => [String(node.stageId), node]));
    workspaceRoot?.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
      const node = byCellId.get(cellRoot.dataset.cellId || "");
      const strip = cellRoot.querySelector("[data-cell-stage-strip]");
      if (!strip) {
        return;
      }
      strip.hidden = !node;
      const titleInput = strip.querySelector("[data-cell-stage-title-input]");
      if (titleInput && node && document.activeElement !== titleInput) {
        titleInput.value = node.title || "";
      }
      const descriptionInput = strip.querySelector("[data-cell-stage-description-input]");
      if (descriptionInput && node && document.activeElement !== descriptionInput) {
        descriptionInput.value = node.description || "";
      }
      const status = strip.querySelector("[data-cell-stage-status]");
      if (status) {
        status.textContent = statusLabel(node?.status);
        status.className = `cell-stage-status-badge pipeline-status-${statusClass(node?.status)}`;
      }
      const predecessors = strip.querySelector("[data-cell-stage-predecessors]");
      if (predecessors) {
        predecessors.innerHTML = (node?.predecessorStageIds || [])
          .map((stageId) => byId.get(String(stageId))?.title || "")
          .filter(Boolean)
          .map((title) => `<span class="cell-stage-chip">${escapeHtml(title)}</span>`)
          .join("");
      }
      const successors = strip.querySelector("[data-cell-stage-successors]");
      if (successors) {
        successors.innerHTML = (node?.successorStageIds || [])
          .map((stageId) => byId.get(String(stageId))?.title || "")
          .filter(Boolean)
          .map((title) => `<span class="cell-stage-chip">${escapeHtml(title)}</span>`)
          .join("");
      }
    });
  }

  function focusSelectedCell(workspaceRoot, graph) {
    const selectedStageId = selectedStageByNotebookId.get(graph.notebookId) || graph.defaultSelectedStageId || "";
    const selectedNode = (graph.nodes || []).find((node) => String(node.stageId) === selectedStageId);
    workspaceRoot?.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
      const visible = !selectedNode || cellRoot.dataset.cellId === String(selectedNode.cellId || "");
      cellRoot.hidden = !visible;
      cellRoot.classList.toggle("is-pipeline-focused-cell", visible && Boolean(selectedNode));
    });
  }

  function renderWorkspaceGraph(workspaceRoot, graph) {
    if (!workspaceRoot || !graph) {
      return;
    }
    if (!selectedStageByNotebookId.get(graph.notebookId)) {
      selectedStageByNotebookId.set(graph.notebookId, graph.defaultSelectedStageId || "");
    }
    renderStatus(workspaceRoot, graph);
    renderPrioritySummary(workspaceRoot, graph);
    renderGraph(workspaceRoot, graph);
    renderTable(workspaceRoot, graph);
    renderCellStageState(workspaceRoot, graph);
    updatePipelineRunControlsFromGraph(workspaceRoot, graph);
    focusSelectedCell(workspaceRoot, graph);
  }

  function ensurePipelineGraphVisible(workspaceRoot) {
    const graphRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-graph]");
    if (!graphRoot) {
      return;
    }
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    if (!viewportHeight) {
      return;
    }
    const rect = graphRoot.getBoundingClientRect();
    const topMargin = 12;
    const bottomMargin = 24;
    if (rect.bottom > topMargin && rect.top < viewportHeight - bottomMargin) {
      return;
    }
    window.scrollBy({ top: rect.top - topMargin, behavior: "auto" });
  }

  function togglePipelineVisibility(workspaceRoot, enabled) {
    workspaceRoot?.querySelector("[data-notebook-pipeline-panel]")?.toggleAttribute("hidden", !enabled);
    workspaceRoot?.querySelector("[data-notebook-pipeline-actions]")?.toggleAttribute("hidden", !enabled);
    workspaceRoot?.querySelectorAll("[data-cell-stage-strip]").forEach((strip) => {
      const cellRoot = strip.closest("[data-query-cell]");
      strip.hidden = !enabled || normalizeCellLanguage(cellRoot?.dataset.defaultCellLanguage) !== "sql";
    });
    if (!enabled) {
      workspaceRoot?.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
        cellRoot.hidden = false;
        cellRoot.classList.remove("is-pipeline-focused-cell");
      });
    }
  }

  function updateModeToggle(button, enabled) {
    const title = enabled ? pipelineModeTitle : explorationModeTitle;
    button.classList.toggle("is-on", enabled);
    button.setAttribute("aria-pressed", enabled ? "true" : "false");
    button.setAttribute("title", title);
    button.setAttribute("aria-label", title);
    const label = button.querySelector("[data-notebook-mode-toggle-label]");
    if (label) {
      label.textContent = enabled ? "Pipeline Mode" : "Exploration Mode";
    }
    const detail = button.querySelector("[data-notebook-mode-toggle-detail]");
    if (detail) {
      detail.textContent = enabled ? pipelineModeDetail : explorationModeDetail;
    }
  }

  function applyModeDom(workspaceRoot, mode) {
    const normalizedMode = normalizeNotebookPipelineMode(mode);
    const enabled = normalizedMode === "pipeline";
    const metaRoot = workspaceRoot?.querySelector("[data-notebook-meta]");
    if (metaRoot) {
      metaRoot.dataset.defaultPipelineMode = normalizedMode;
    }
    if (workspaceRoot) {
      workspaceRoot.dataset.defaultPipelineMode = normalizedMode;
    }
    togglePipelineVisibility(workspaceRoot, enabled);
    workspaceRoot?.querySelectorAll("[data-notebook-mode-toggle]").forEach((button) => {
      updateModeToggle(button, enabled);
    });
    workspaceRoot?.querySelectorAll("[data-set-notebook-mode]").forEach((button) => {
      const active = button.dataset.setNotebookMode === normalizedMode;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  async function refreshGraph(notebookId = getCurrentNotebookId()) {
    if (!notebookId || !pipelineEnabled(notebookId)) {
      return null;
    }
    const workspaceRoot = document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"]`
    );
    if (!workspaceRoot) {
      return null;
    }
    const preserveExistingGraph = graphHasRenderedContent(workspaceRoot);
    if (preserveExistingGraph) {
      setGraphRefreshPending(workspaceRoot, true);
    } else {
      renderGraphPlaceholder(workspaceRoot, { tone: "loading" });
    }
    let graph = null;
    try {
      graph = await fetchJsonOrThrow("/api/materialized-stages/graph", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(pipelinePayload(notebookId)),
      });
    } catch (error) {
      const message = errorMessage(error, "Pipeline graph could not be built.");
      if (preserveExistingGraph) {
        renderGraphRefreshError(workspaceRoot, message);
      } else {
        renderGraphPlaceholder(workspaceRoot, {
          tone: "error",
          status: message,
          message,
        });
      }
      throw error;
    }
    if (!pipelineEnabled(notebookId)) {
      setGraphRefreshPending(workspaceRoot, false);
      return null;
    }
    graphByNotebookId.set(notebookId, graph);
    renderWorkspaceGraph(workspaceRoot, graph);
    return graph;
  }

  async function initializeCurrentWorkspace() {
    const notebookId = getCurrentNotebookId();
    const workspaceRoot = currentWorkspaceRoot();
    if (!notebookId || !workspaceRoot) {
      return;
    }
    const version = modeChangeVersion;
    const enabled = pipelineEnabled(notebookId);
    applyModeDom(workspaceRoot, enabled ? "pipeline" : "exploration");
    if (enabled) {
      try {
        await refreshGraph(notebookId);
      } catch (error) {
        console.error("Failed to initialize notebook pipeline graph.", error);
      }
      if (version !== modeChangeVersion || !pipelineEnabled(notebookId)) {
        applyModeDom(workspaceRoot, pipelineEnabled(notebookId) ? "pipeline" : "exploration");
      }
    }
  }

  function selectStage(notebookId, stageId) {
    if (!notebookId || !stageId) {
      return;
    }
    selectedStageByNotebookId.set(notebookId, stageId);
    const graph = graphByNotebookId.get(notebookId);
    const workspaceRoot = document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"]`
    );
    renderWorkspaceGraph(workspaceRoot, graph);
    ensurePipelineGraphVisible(workspaceRoot);
    window.requestAnimationFrame(() => ensurePipelineGraphVisible(workspaceRoot));
  }

  function closeContextMenu() {
    contextMenu?.remove();
    contextMenu = null;
  }

  function closePriorityPopover() {
    const notebookId = priorityPopover?.dataset?.notebookId || "";
    priorityPopover?.remove();
    priorityPopover = null;
    if (notebookId) {
      document
        .querySelectorAll(`[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"] [data-pipeline-priority-paths]`)
        .forEach((button) => button.setAttribute("aria-expanded", "false"));
    }
  }

  function priorityButtonForNotebook(notebookId) {
    return document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${CSS.escape(String(notebookId || ""))}"] [data-pipeline-priority-paths]`
    );
  }

  function positionPriorityPopover(popover, target) {
    const rect = target?.getBoundingClientRect?.();
    if (!rect) {
      return;
    }
    const popoverWidth = popover.offsetWidth || 360;
    const popoverHeight = popover.offsetHeight || 280;
    const left = Math.max(8, Math.min(rect.right - popoverWidth, window.innerWidth - popoverWidth - 8));
    const top = Math.max(8, Math.min(rect.bottom + 6, window.innerHeight - popoverHeight - 8));
    popover.style.left = `${left}px`;
    popover.style.top = `${top}px`;
  }

  function renderPriorityPopoverContent(popover, graph) {
    const paths = graphPaths(graph);
    if (paths.length <= 1) {
      popover.innerHTML = `
        <div class="pipeline-priority-popover-heading">
          <strong>Priority paths</strong>
          <span>This pipeline has one terminal path.</span>
        </div>
      `;
      return;
    }
    popover.innerHTML = `
      <div class="pipeline-priority-popover-heading">
        <strong>Priority paths</strong>
        <span>Priority applies only when branches are ready.</span>
      </div>
      <div class="pipeline-priority-list">
        ${paths
          .map((path, index) => {
            const stageCount = Array.isArray(path.stageIds) ? path.stageIds.length : 0;
            return `
              <div
                class="pipeline-priority-row"
                data-pipeline-priority-row
                data-pipeline-path-id="${escapeHtml(path.pathId)}"
                data-pipeline-terminal-stage-id="${escapeHtml(path.terminalStageId)}"
              >
                <span class="pipeline-priority-rank-badge">${escapeHtml(String(index + 1))}</span>
                <label class="pipeline-priority-label">
                  <span>${escapeHtml(path.terminalStageTitle || "Terminal path")}</span>
                  <input
                    type="text"
                    value="${escapeHtml(path.label || path.terminalStageTitle || "")}"
                    data-pipeline-path-label-input
                    data-pipeline-path-id="${escapeHtml(path.pathId)}"
                  >
                </label>
                <div class="pipeline-priority-row-actions">
                  <button type="button" data-pipeline-path-move="up" data-pipeline-path-id="${escapeHtml(path.pathId)}" ${index === 0 ? "disabled" : ""} ${pipelineTooltipAttributes("Move path earlier")}>${menuIcon("up")}</button>
                  <button type="button" data-pipeline-path-move="down" data-pipeline-path-id="${escapeHtml(path.pathId)}" ${index === paths.length - 1 ? "disabled" : ""} ${pipelineTooltipAttributes("Move path later")}>${menuIcon("down")}</button>
                </div>
                <small>${escapeHtml(`${stageCount} ${stageCount === 1 ? "stage" : "stages"}`)}</small>
              </div>
            `;
          })
          .join("")}
      </div>
      <div class="pipeline-priority-footer">
        <button type="button" data-pipeline-priority-reset>Reset</button>
      </div>
    `;
  }

  function openPriorityPopover(target) {
    const notebookId = getCurrentNotebookId();
    const graph = graphByNotebookId.get(notebookId);
    if (!notebookId || !graph || graphPaths(graph).length <= 1 || target?.disabled) {
      return;
    }
    if (priorityPopover?.dataset?.notebookId === notebookId) {
      closePriorityPopover();
      return;
    }
    closeContextMenu();
    closePriorityPopover();
    priorityPopover = document.createElement("div");
    priorityPopover.className = "pipeline-priority-popover workspace-action-menu-panel";
    priorityPopover.dataset.pipelinePriorityPopover = "";
    priorityPopover.dataset.notebookId = notebookId;
    renderPriorityPopoverContent(priorityPopover, graph);
    document.body.appendChild(priorityPopover);
    positionPriorityPopover(priorityPopover, target);
    target.setAttribute("aria-expanded", "true");
  }

  function priorityPathsFromPopover(popover) {
    const notebookId = popover?.dataset?.notebookId || "";
    const graph = graphByNotebookId.get(notebookId);
    const pathsById = new Map(graphPaths(graph).map((path) => [String(path.pathId || ""), path]));
    return Array.from(popover?.querySelectorAll("[data-pipeline-priority-row]") || []).map((row, index) => {
      const pathId = String(row.dataset.pipelinePathId || "").trim();
      const original = pathsById.get(pathId) || {};
      const input = row.querySelector("[data-pipeline-path-label-input]");
      const terminalStageId = String(row.dataset.pipelineTerminalStageId || original.terminalStageId || "").trim();
      const terminalStageTitle = String(original.terminalStageTitle || "").trim();
      return {
        ...original,
        pathId,
        terminalStageId,
        terminalStageTitle,
        label: String(input?.value || terminalStageTitle || "").trim(),
        priority: index + 1,
      };
    });
  }

  function updatePriorityLabelsFromInput(input) {
    const popover = input?.closest?.("[data-pipeline-priority-popover]");
    const notebookId = popover?.dataset?.notebookId || "";
    if (!popover || !notebookId) {
      return false;
    }
    const nextPaths = priorityPathsFromPopover(popover);
    setNotebookPipelinePaths(notebookId, priorityPathPayload(nextPaths), { silent: true });
    const graph = graphByNotebookId.get(notebookId);
    if (graph) {
      const nextGraph = { ...graph, paths: nextPaths };
      graphByNotebookId.set(notebookId, nextGraph);
      const workspaceRoot = document.querySelector(
        `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"]`
      );
      renderPrioritySummary(workspaceRoot, nextGraph);
    }
    return true;
  }

  async function persistPriorityPathsFromPopover(popover) {
    const notebookId = popover?.dataset?.notebookId || "";
    if (!notebookId) {
      return;
    }
    const nextPaths = priorityPathsFromPopover(popover);
    setNotebookPipelinePaths(notebookId, priorityPathPayload(nextPaths), { silent: true, syncNow: true });
    const graph = await refreshGraph(notebookId);
    if (priorityPopover === popover && graph) {
      renderPriorityPopoverContent(popover, graph);
      positionPriorityPopover(popover, priorityButtonForNotebook(notebookId));
    }
  }

  async function movePriorityPath(button) {
    const popover = button?.closest?.("[data-pipeline-priority-popover]");
    if (!popover || button.disabled) {
      return;
    }
    const direction = button.dataset.pipelinePathMove;
    const pathId = String(button.dataset.pipelinePathId || "").trim();
    const paths = priorityPathsFromPopover(popover);
    const index = paths.findIndex((path) => String(path.pathId || "") === pathId);
    const targetIndex = direction === "up" ? index - 1 : index + 1;
    if (index < 0 || targetIndex < 0 || targetIndex >= paths.length) {
      return;
    }
    [paths[index], paths[targetIndex]] = [paths[targetIndex], paths[index]];
    setNotebookPipelinePaths(popover.dataset.notebookId || "", priorityPathPayload(paths), { silent: true, syncNow: true });
    const graph = await refreshGraph(popover.dataset.notebookId || "");
    if (priorityPopover === popover && graph) {
      renderPriorityPopoverContent(popover, graph);
      positionPriorityPopover(popover, priorityButtonForNotebook(popover.dataset.notebookId || ""));
    }
  }

  async function resetPriorityPaths(button) {
    const popover = button?.closest?.("[data-pipeline-priority-popover]");
    const notebookId = popover?.dataset?.notebookId || "";
    if (!popover || !notebookId) {
      return;
    }
    setNotebookPipelinePaths(notebookId, [], { silent: true, syncNow: true });
    const graph = await refreshGraph(notebookId);
    if (priorityPopover === popover && graph) {
      renderPriorityPopoverContent(popover, graph);
      positionPriorityPopover(popover, priorityButtonForNotebook(notebookId));
    }
  }

  function selectedNodeForMenu(button) {
    const notebookId = getCurrentNotebookId();
    const stageId =
      button?.dataset?.pipelineStageMenu ||
      button?.closest?.("[data-pipeline-stage-node]")?.dataset.pipelineStageNode ||
      button?.closest?.("[data-pipeline-stage-row]")?.dataset.pipelineStageRow ||
      "";
    const graph = graphByNotebookId.get(notebookId);
    const node = (graph?.nodes || []).find((item) => String(item.stageId) === String(stageId));
    return { notebookId, graph, node };
  }

  function openContextMenu(target, event) {
    const { notebookId, node } = selectedNodeForMenu(target);
    if (!notebookId || !node) {
      return;
    }
    closeContextMenu();
    const hasRevision = Boolean(
      node.latestRevision?.outputPath ||
        node.latestRevision?.queryReference ||
        node.latestRevision?.queryPath
    );
    const hasTargetObject = Boolean(node.outputSource?.bucket && node.outputSource?.key);
    const hasStorageReference = Boolean(stageStorageReference(node));
    contextMenu = document.createElement("div");
    contextMenu.className = "pipeline-context-menu workspace-action-menu-panel";
    contextMenu.dataset.pipelineContextMenu = "";
    contextMenu.innerHTML = `
      ${menuItem({
        action: "navigate-target",
        label: "Navigate to target data object",
        icon: "navigate",
        disabled: !hasTargetObject,
      })}
      ${menuItem({ action: "copy-path", label: "Copy target path", icon: "copy", disabled: !hasRevision })}
      ${menuItem({ action: "publish", label: "Publish data product", icon: "publish", disabled: !hasRevision })}
      <div class="workspace-action-menu-separator" aria-hidden="true"></div>
      ${menuItem({ action: "derive", label: "Derive new stage", icon: "derive", disabled: !hasStorageReference })}
      ${menuItem({ action: "fork", label: "Fork new stage", icon: "fork", disabled: !hasStorageReference })}
      <div class="workspace-action-menu-separator" aria-hidden="true"></div>
      ${menuItem({ action: "run", label: "Run stage", icon: "run" })}
      ${menuItem({ action: "stop", label: "Stop stage", icon: "stop" })}
      <div class="workspace-action-menu-separator" aria-hidden="true"></div>
      ${menuItem({ action: "delete", label: "Delete stage ...", icon: "delete", danger: true })}
    `;
    contextMenu.dataset.notebookId = notebookId;
    contextMenu.dataset.stageId = String(node.stageId);
    document.body.appendChild(contextMenu);
    const rect = target?.getBoundingClientRect?.();
    const left = event?.clientX || rect?.left || 0;
    const top = event?.clientY || rect?.bottom || 0;
    const menuWidth = contextMenu.offsetWidth || 240;
    const menuHeight = contextMenu.offsetHeight || 340;
    contextMenu.style.left = `${Math.max(8, Math.min(left, window.innerWidth - menuWidth - 8))}px`;
    contextMenu.style.top = `${Math.max(8, Math.min(top, window.innerHeight - menuHeight - 8))}px`;
  }

  async function copyText(value) {
    const text = String(value || "").trim();
    if (!text) {
      return false;
    }
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "readonly");
    textarea.style.position = "absolute";
    textarea.style.left = "-9999px";
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand("copy");
    textarea.remove();
    return true;
  }

  function setPipelineStatusError(notebookId, message) {
    const workspaceRoot = document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"]`
    );
    const statusRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-status]");
    if (!statusRoot) {
      return;
    }
    statusRoot.textContent = message;
    statusRoot.classList.add("is-error");
  }

  async function reportPipelineError(notebookId, title, error) {
    const message = errorMessage(error);
    if (notebookId) {
      setPipelineStatusError(notebookId, message);
      try {
        await refreshGraph(notebookId);
      } catch (_refreshError) {
        // Preserve the original failure message for the user.
      }
    }
    await showMessageDialog({
      title,
      copy: message,
    });
  }

  function setPipelineRunControls(
    notebookId,
    { running = false, cancelling = false, wholePipelineRunning = false } = {}
  ) {
    const workspaceRoot = document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"]`
    );
    const runButton = workspaceRoot?.querySelector("[data-run-notebook-pipeline]");
    const cancelButton = workspaceRoot?.querySelector("[data-cancel-notebook-pipeline]");
    const runningIndicator = workspaceRoot?.querySelector("[data-notebook-pipeline-running-indicator]");
    if (runButton) {
      runButton.hidden = Boolean(running);
      runButton.disabled = false;
      runButton.classList.toggle("is-running", Boolean(running));
      const label = runButton.querySelector("span");
      if (label) {
        label.textContent = "Run pipeline";
      }
    }
    if (cancelButton) {
      cancelButton.hidden = !running;
      cancelButton.disabled = Boolean(cancelling);
      cancelButton.classList.toggle("is-running", Boolean(running));
      cancelButton.classList.toggle("is-cancelling", Boolean(cancelling));
      const label = cancelButton.querySelector("span");
      if (label) {
        label.textContent = cancelling ? "Aborting" : "Abort pipeline";
      }
    }
    if (runningIndicator) {
      runningIndicator.hidden = !wholePipelineRunning;
    }
  }

  function setPipelineRunButtonBusy(notebookId, busy) {
    setPipelineRunControls(notebookId, { running: Boolean(busy), cancelling: false });
  }

  function updatePipelineRunControlsFromGraph(workspaceRoot, graph) {
    const activeRuns = activeRunsForGraph(graph);
    const cancelling = activeRuns.some((run) => {
      const status = String(run?.status || "").toLowerCase();
      return Boolean(run?.cancelRequested) || status === "cancelling";
    });
    const notebookId = String(graph?.notebookId || workspaceRoot?.dataset?.notebookId || "").trim();
    if (notebookId) {
      setPipelineRunControls(notebookId, {
        running: activeRuns.length > 0,
        cancelling,
        wholePipelineRunning: graphHasActiveWholePipelineRun(graph),
      });
    }
  }

  function setCellRunStageBusy(cellId, busy) {
    const cellRoot = document.querySelector(
      `[data-query-cell][data-cell-id="${CSS.escape(String(cellId || ""))}"]`
    );
    const button = cellRoot?.querySelector("[data-run-cell]");
    if (!button) {
      return;
    }
    button.disabled = Boolean(busy);
    button.classList.toggle("is-running", Boolean(busy));
    button.textContent = busy ? "Run Stage" : "Run Cell";
  }

  async function materializedStageState() {
    return fetchJsonOrThrow("/api/materialized-stages/state", {
      headers: { Accept: "application/json" },
    });
  }

  async function waitForNotebookRunsIdle(notebookId, timeoutMs = 60000) {
    const deadline = Date.now() + timeoutMs;
    let latestSnapshot = null;
    while (Date.now() < deadline) {
      const snapshot = await materializedStageState();
      latestSnapshot = snapshot;
      applyRealtimeState(snapshot);
      const active = activeRunsForGraph(snapshot).some(
        (run) => String(run?.notebookId || "") === String(notebookId || "")
      );
      if (!active) {
        return snapshot;
      }
      await sleep(450);
    }
    return latestSnapshot;
  }

  async function waitForStageTerminal(notebookId, stageId, timeoutMs = 60000) {
    const normalizedStageId = String(stageId || "").trim();
    const deadline = Date.now() + timeoutMs;
    let latestNode = null;
    while (Date.now() < deadline) {
      const graph = await refreshGraph(notebookId);
      latestNode = (graph?.nodes || []).find(
        (node) => String(node?.stageId || "") === normalizedStageId
      );
      const status = String(latestNode?.status || "").toLowerCase();
      if (status && !["planned", "queued", "running"].includes(status)) {
        return latestNode;
      }
      const snapshot = await materializedStageState();
      applyRealtimeState(snapshot);
      const active = activeRunsForGraph(snapshot).some(
        (run) =>
          String(run?.notebookId || "") === String(notebookId || "") &&
          (run?.stageIds || []).map((item) => String(item)).includes(normalizedStageId)
      );
      if (!active && status && status !== "planned") {
        return latestNode;
      }
      await sleep(450);
    }
    return latestNode;
  }

  async function runPipeline(notebookId = getCurrentNotebookId(), startStageId = "") {
    if (!notebookId) {
      return;
    }
    const normalizedStartStageId = String(startStageId || "").trim();
    let keepRunningControls = false;
    setPipelineRunControls(notebookId, {
      running: true,
      cancelling: false,
      wholePipelineRunning: !normalizedStartStageId,
    });
    try {
      await fetchJsonOrThrow("/api/materialized-stages/pipeline/run", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(pipelinePayload(notebookId, { startStageId: normalizedStartStageId })),
      });
      await refreshGraph(notebookId);
      const snapshot = await waitForNotebookRunsIdle(notebookId);
      keepRunningControls = activeRunsForGraph(snapshot).some(
        (run) => String(run?.notebookId || "") === String(notebookId || "")
      );
      const graph = await refreshGraph(notebookId);
      keepRunningControls = graph ? graphHasActiveRun(graph) : keepRunningControls;
    } catch (error) {
      await reportPipelineError(notebookId, "Pipeline run failed", error);
    } finally {
      setPipelineRunControls(notebookId, { running: keepRunningControls, cancelling: false });
    }
  }

  async function runPipelineFromStageButton(notebookId, stageId) {
    await runPipeline(notebookId, stageId);
  }

  async function cancelPipeline(notebookId = getCurrentNotebookId()) {
    if (!notebookId) {
      return;
    }
    setPipelineRunControls(notebookId, { running: true, cancelling: true });
    try {
      const snapshot = await fetchJsonOrThrow("/api/materialized-stages/pipeline/cancel", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(pipelinePayload(notebookId)),
      });
      applyRealtimeState(snapshot);
      await refreshGraph(notebookId);
      await waitForNotebookRunsIdle(notebookId);
      await refreshGraph(notebookId);
    } catch (error) {
      await reportPipelineError(notebookId, "Pipeline abort failed", error);
    } finally {
      const graph = graphByNotebookId.get(notebookId);
      setPipelineRunControls(notebookId, {
        running: graphHasActiveRun(graph),
        cancelling: false,
      });
    }
  }

  async function runStage(notebookId, stageId) {
    await fetchJsonOrThrow(`/api/materialized-stages/stages/${encodeURIComponent(stageId)}/run`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(pipelinePayload(notebookId)),
    });
    await waitForStageTerminal(notebookId, stageId);
    await refreshGraph(notebookId);
  }

  function setStageTransientStatus(notebookId, stageId, status) {
    const graph = graphByNotebookId.get(notebookId);
    const workspaceRoot = document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${CSS.escape(String(notebookId || ""))}"]`
    );
    if (!graph || !workspaceRoot) {
      return;
    }
    const nodes = (graph.nodes || []).map((node) =>
      String(node.stageId || "") === String(stageId || "")
        ? { ...node, status }
        : node
    );
    const nextGraph = { ...graph, nodes };
    graphByNotebookId.set(notebookId, nextGraph);
    renderWorkspaceGraph(workspaceRoot, nextGraph);
  }

  async function runStageFromButton(notebookId, stageId) {
    setStageTransientStatus(notebookId, stageId, "running");
    try {
      await runStage(notebookId, stageId);
    } catch (error) {
      await reportPipelineError(notebookId, "Stage run failed", error);
    } finally {
      await refreshGraph(notebookId);
    }
  }

  async function stopStage(notebookId, stageId) {
    await fetchJsonOrThrow(`/api/materialized-stages/stages/${encodeURIComponent(stageId)}/stop`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(pipelinePayload(notebookId)),
    });
    await refreshGraph(notebookId);
  }

  async function cancelStageFromButton(notebookId, stageId) {
    try {
      await stopStage(notebookId, stageId);
    } catch (error) {
      await reportPipelineError(notebookId, "Stage cancellation failed", error);
    } finally {
      await refreshGraph(notebookId);
    }
  }

  function closeNotebookExplorer() {
    const notebookSection = document.querySelector("[data-notebook-section]");
    if (notebookSection instanceof HTMLDetailsElement) {
      notebookSection.open = false;
    }
  }

  function openSourceObjectAncestors(target) {
    const dataSourcesSection = document.querySelector("[data-data-sources-section]");
    if (dataSourcesSection instanceof HTMLDetailsElement) {
      dataSourcesSection.open = true;
    }
    const catalog = target?.closest?.("[data-source-catalog]");
    if (catalog instanceof HTMLDetailsElement) {
      catalog.open = true;
    }
    const schema = target?.closest?.("[data-source-schema]");
    if (schema instanceof HTMLDetailsElement) {
      schema.open = true;
    }
  }

  function flashTargetDataObject(target) {
    if (!(target instanceof Element)) {
      return;
    }
    const label = target.querySelector(".source-node-label > span:last-child") || target.querySelector(".source-node-label");
    target.classList.remove("is-pipeline-inspect-flash");
    label?.classList?.remove("is-pipeline-target-text-flash");
    void target.offsetWidth;
    target.classList.add("is-pipeline-inspect-flash");
    label?.classList?.add("is-pipeline-target-text-flash");
    window.setTimeout(() => {
      target.classList.remove("is-pipeline-inspect-flash");
      label?.classList?.remove("is-pipeline-target-text-flash");
    }, 3200);
  }

  async function navigateToTargetDataObject(node) {
    const source = node?.outputSource || {};
    if (!source.bucket || !source.key) {
      await showMessageDialog({
        title: "Materialized data unavailable",
        copy: "Run this stage before navigating to its target data object.",
      });
      return;
    }
    await revealDataSourceSidebarBrowser("workspace.s3");
    await refreshSidebar("notebook");
    closeNotebookExplorer();
    const selector = `[data-source-object][data-s3-bucket="${CSS.escape(source.bucket)}"][data-s3-key="${CSS.escape(source.key)}"]`;
    const target = document.querySelector(selector);
    if (target) {
      openSourceObjectAncestors(target);
      target.scrollIntoView({ block: "center" });
      flashTargetDataObject(target);
      return;
    }
    await showMessageDialog({
      title: "Target data object not visible",
      copy: "The stage has a materialized target, but it is not currently visible in the Data Sources navigation. Refresh the data sources and try again.",
    });
  }

  function appendDerivedStage(notebookId, node, mode) {
    const reference = stageStorageReference(node);
    if (!reference) {
      showMessageDialog({
        title: "Materialized data unavailable",
        copy: "Run this stage before deriving another stage from its S3 materialized output.",
      });
      return;
    }
    const metadata = getNotebookMetadata(notebookId);
    const stageTitle = mode === "fork" ? `${node.title} Fork` : `${node.title} Derived`;
    const nextCell = {
      cellId: createCellId(),
      language: "sql",
      dataSources: node.outputSource?.sourceId ? [node.outputSource.sourceId] : [],
      queryOptions: {},
      sql: `SELECT *\nFROM ${reference}`,
      stage: {
        enabled: true,
        stageId: `stage-${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 6)}`,
        alias: stageAlias(stageTitle, stageTitle),
        title: stageTitle,
        description: "",
        kind: "intermediate",
        predecessorStageIds: [String(node.stageId)],
        materialize: true,
      },
    };
    setNotebookCells(notebookId, [...(metadata.cells || []), nextCell], { rerender: true });
  }

  async function deleteStage(notebookId, node) {
    const title = String(node?.title || node?.alias || "this stage").trim();
    const { confirmed } = await showConfirmDialog({
      title: "Delete stage",
      copy: `Delete "${title}" and its notebook cell? Downstream stages keep their SQL and must be adjusted if they referenced this stage.`,
      confirmLabel: "Delete stage",
      confirmTone: "danger",
    });
    if (!confirmed) {
      return;
    }
    const deletedCellId = String(node?.cellId || "");
    const deletedStageId = String(node?.stageId || "");
    const metadata = getNotebookMetadata(notebookId);
    const nextCells = (metadata.cells || [])
      .filter((cell) => String(cell.cellId || "") !== deletedCellId)
      .map((cell) => {
        const stage = normalizeCellStage(cell.stage);
        const predecessors = (stage.predecessorStageIds || []).filter(
          (stageId) => String(stageId) !== deletedStageId
        );
        return {
          ...cell,
          stage: {
            ...stage,
            predecessorStageIds: predecessors,
          },
        };
      });
    selectedStageByNotebookId.delete(notebookId);
    setNotebookCells(notebookId, nextCells, { rerender: true });
    await refreshGraph(notebookId);
  }

  function shouldHandleCellRun(cellRoot) {
    const notebookId = notebookIdForCellRoot(cellRoot);
    const cellId = String(cellRoot?.dataset?.cellId || "").trim();
    if (!notebookId || !cellId || !pipelineEnabled(notebookId)) {
      return false;
    }
    return Boolean(stageNodeForCell(notebookId, cellId));
  }

  async function materializeCellStageThenRun(cellRoot) {
    const notebookId = notebookIdForCellRoot(cellRoot);
    const cellId = String(cellRoot?.dataset?.cellId || "").trim();
    const node = stageNodeForCell(notebookId, cellId);
    if (!notebookId || !cellId || !node) {
      return false;
    }
    setCellRunStageBusy(cellId, true);
    try {
      await runStage(notebookId, String(node.stageId || ""));
      const graph = await refreshGraph(notebookId);
      const updatedNode = (graph?.nodes || []).find(
        (item) => String(item?.stageId || "") === String(node.stageId || "")
      );
      const status = String(updatedNode?.status || "").toLowerCase();
      if (status !== "valid") {
        throw new Error(
          updatedNode?.latestRun?.error ||
            updatedNode?.latestRun?.message ||
            `Stage finished with status ${statusLabel(status)}.`
        );
      }
      passThroughCellRuns.add(cellId);
      requestCellRun(cellId);
      return true;
    } catch (error) {
      await reportPipelineError(notebookId, "Stage run failed", error);
      return true;
    } finally {
      setCellRunStageBusy(cellId, false);
    }
  }

  async function handleRunCellButton(button) {
    const cellRoot = button?.closest?.("[data-query-cell]");
    if (!shouldHandleCellRun(cellRoot)) {
      return false;
    }
    return materializeCellStageThenRun(cellRoot);
  }

  async function handleQueryFormSubmit(form) {
    const cellRoot = form?.closest?.("[data-query-cell]");
    const cellId = String(cellRoot?.dataset?.cellId || "").trim();
    if (cellId && passThroughCellRuns.has(cellId)) {
      passThroughCellRuns.delete(cellId);
      return false;
    }
    if (!shouldHandleCellRun(cellRoot)) {
      return false;
    }
    return materializeCellStageThenRun(cellRoot);
  }

  async function handleMenuAction(action, notebookId, stageId) {
    const graph = graphByNotebookId.get(notebookId);
    const node = (graph?.nodes || []).find((item) => String(item.stageId) === String(stageId));
    if (!node) {
      return;
    }
    if (action === "navigate-target") {
      await navigateToTargetDataObject(node);
      return;
    }
    if (action === "copy-path") {
      await copyText(stageStorageReference(node) || node.latestRevision?.outputPath || "");
      return;
    }
    if (action === "publish") {
      if (node.outputSource?.sourceKind) {
        await openPublishDialogForSource(node.outputSource);
      }
      return;
    }
    if (action === "derive" || action === "fork") {
      appendDerivedStage(notebookId, node, action);
      return;
    }
    if (action === "run") {
      try {
        await runStage(notebookId, stageId);
      } catch (error) {
        await reportPipelineError(notebookId, "Stage run failed", error);
      }
      return;
    }
    if (action === "stop") {
      await stopStage(notebookId, stageId);
      return;
    }
    if (action === "delete") {
      await deleteStage(notebookId, node);
    }
  }

  async function handleClick(event) {
    const clickedPrioritySurface = Boolean(
      event.target.closest("[data-pipeline-priority-popover]") ||
        event.target.closest("[data-pipeline-priority-paths]")
    );
    if (priorityPopover && !clickedPrioritySurface) {
      closePriorityPopover();
    }

    const modeToggle = event.target.closest("[data-notebook-mode-toggle]");
    const modeButton = event.target.closest("[data-set-notebook-mode]");
    if (modeToggle || modeButton) {
      event.preventDefault();
      const notebookId = getCurrentNotebookId();
      if (!notebookId) {
        return true;
      }
      const currentMode = normalizeNotebookPipelineMode(getNotebookMetadata(notebookId).pipelineMode);
      const nextMode = modeToggle
        ? currentMode === "pipeline"
          ? "exploration"
          : "pipeline"
        : modeButton.dataset.setNotebookMode === "pipeline"
          ? "pipeline"
          : "exploration";
      const version = ++modeChangeVersion;
      if (nextMode === "pipeline") {
        const metadata = getNotebookMetadata(notebookId);
        setNotebookCells(notebookId, initializedPipelineCells(metadata.cells), { rerender: false });
      }
      setNotebookPipelineMode(notebookId, nextMode, { rerender: false });
      const workspaceRoot = currentWorkspaceRoot();
      applyModeDom(workspaceRoot, nextMode);
      if (nextMode === "pipeline") {
        try {
          await refreshGraph(notebookId);
        } catch (error) {
          console.error("Failed to refresh notebook pipeline after mode change.", error);
        }
        if (version !== modeChangeVersion || !pipelineEnabled(notebookId)) {
          applyModeDom(workspaceRoot, pipelineEnabled(notebookId) ? "pipeline" : "exploration");
        }
      }
      return true;
    }

    const priorityButton = event.target.closest("[data-pipeline-priority-paths]");
    if (priorityButton) {
      event.preventDefault();
      openPriorityPopover(priorityButton);
      return true;
    }

    const priorityMoveButton = event.target.closest("[data-pipeline-path-move]");
    if (priorityMoveButton) {
      event.preventDefault();
      event.stopPropagation();
      await movePriorityPath(priorityMoveButton);
      return true;
    }

    const priorityResetButton = event.target.closest("[data-pipeline-priority-reset]");
    if (priorityResetButton) {
      event.preventDefault();
      event.stopPropagation();
      await resetPriorityPaths(priorityResetButton);
      return true;
    }

    const runPipelineButton = event.target.closest("[data-run-notebook-pipeline]");
    if (runPipelineButton) {
      event.preventDefault();
      await runPipeline();
      return true;
    }

    const cancelPipelineButton = event.target.closest("[data-cancel-notebook-pipeline]");
    if (cancelPipelineButton) {
      event.preventDefault();
      await cancelPipeline();
      return true;
    }

    const runStageButton = event.target.closest("[data-run-pipeline-stage]");
    if (runStageButton) {
      event.preventDefault();
      event.stopPropagation();
      await runStageFromButton(
        getCurrentNotebookId(),
        runStageButton.dataset.runPipelineStage || ""
      );
      return true;
    }

    const runPipelineFromStageButtonNode = event.target.closest("[data-run-pipeline-from-stage]");
    if (runPipelineFromStageButtonNode) {
      event.preventDefault();
      event.stopPropagation();
      await runPipelineFromStageButton(
        getCurrentNotebookId(),
        runPipelineFromStageButtonNode.dataset.runPipelineFromStage || ""
      );
      return true;
    }

    const cancelStageButton = event.target.closest("[data-cancel-pipeline-stage]");
    if (cancelStageButton) {
      event.preventDefault();
      event.stopPropagation();
      await cancelStageFromButton(
        getCurrentNotebookId(),
        cancelStageButton.dataset.cancelPipelineStage || ""
      );
      return true;
    }

    const stageMenuButton = event.target.closest("[data-pipeline-stage-menu]");
    if (stageMenuButton) {
      event.preventDefault();
      event.stopPropagation();
      openContextMenu(stageMenuButton, event);
      return true;
    }

    const graphNode = event.target.closest("[data-pipeline-stage-node]");
    if (graphNode) {
      event.preventDefault();
      selectStage(getCurrentNotebookId(), graphNode.dataset.pipelineStageNode);
      return true;
    }

    const tableRow = event.target.closest("[data-pipeline-stage-row]");
    if (tableRow) {
      event.preventDefault();
      selectStage(getCurrentNotebookId(), tableRow.dataset.pipelineStageRow);
      return true;
    }

    const menuAction = event.target.closest("[data-pipeline-menu-action]");
    if (menuAction) {
      event.preventDefault();
      const notebookId = contextMenu?.dataset.notebookId || "";
      const stageId = contextMenu?.dataset.stageId || "";
      const action = menuAction.dataset.pipelineMenuAction || "";
      closeContextMenu();
      await handleMenuAction(action, notebookId, stageId);
      return true;
    }

    if (contextMenu && !event.target.closest("[data-pipeline-context-menu]")) {
      closeContextMenu();
    }
    if (
      priorityPopover &&
      !event.target.closest("[data-pipeline-priority-popover]") &&
      !event.target.closest("[data-pipeline-priority-paths]")
    ) {
      closePriorityPopover();
    }
    return false;
  }

  function handleContextMenu(event) {
    const node = event.target.closest("[data-pipeline-stage-node], [data-pipeline-stage-row]");
    if (!node) {
      return false;
    }
    event.preventDefault();
    closePriorityPopover();
    openContextMenu(node, event);
    return true;
  }

  function handleInput(event) {
    const priorityLabelInput = event.target.closest("[data-pipeline-path-label-input]");
    if (priorityLabelInput) {
      return updatePriorityLabelsFromInput(priorityLabelInput);
    }

    const titleInput = event.target.closest("[data-cell-stage-title-input]");
    const descriptionInput = event.target.closest("[data-cell-stage-description-input]");
    if (!titleInput && !descriptionInput) {
      return false;
    }
    const notebookId = getCurrentNotebookId();
    const cellRoot = event.target.closest("[data-query-cell]");
    const cellId = cellRoot?.dataset.cellId || "";
    if (!notebookId || !cellId) {
      return true;
    }
    const patch = titleInput
      ? { title: titleInput.value, alias: stageAlias(titleInput.value, cellId) }
      : { description: descriptionInput.value };
    setCellStage(notebookId, cellId, patch, { rerender: false });
    refreshGraph(notebookId).catch((error) => {
      console.error("Failed to refresh notebook pipeline after stage edit.", error);
    });
    return true;
  }

  function applyRealtimeState(snapshot) {
    materializedStagesVersion = Number(snapshot?.version ?? 0);
    const completedOutputs = (Array.isArray(snapshot?.records) ? snapshot.records : [])
      .filter((record) => String(record?.status || "").toLowerCase() === "completed")
      .map((record) => `${record.outputBucket || ""}/${record.outputKey || ""}/${record.revisionId || ""}`)
      .filter((value) => value !== "//")
      .sort()
      .join("|");
    if (completedOutputs && completedOutputs !== materializedOutputSignature) {
      materializedOutputSignature = completedOutputs;
      refreshSidebar("notebook").catch((error) => {
        console.error("Failed to refresh data sources after materialized stage output.", error);
      });
    }
    const notebookId = getCurrentNotebookId();
    if (notebookId && pipelineEnabled(notebookId)) {
      refreshGraph(notebookId).catch((error) => {
        console.error("Failed to refresh notebook pipeline from realtime state.", error);
      });
    }
  }

  async function loadState() {
    const snapshot = await fetchJsonOrThrow("/api/materialized-stages/state", {
      headers: { Accept: "application/json" },
    });
    applyRealtimeState(snapshot);
    return snapshot;
  }

  function getMaterializedStagesVersion() {
    return materializedStagesVersion;
  }

  return {
    applyRealtimeState,
    getMaterializedStagesVersion,
    handleClick,
    handleContextMenu,
    handleInput,
    handleQueryFormSubmit,
    handleRunCellButton,
    initializeCurrentWorkspace,
    loadState,
    prepareQuerySqlForCell,
    refreshGraph,
    requestCellRun,
    validateStageAliasesForCell,
  };
}

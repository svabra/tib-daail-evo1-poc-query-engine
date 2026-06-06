export function createNotebookStagePipelineController(helpers) {
  const {
    createCellId,
    escapeHtml,
    fetchJsonOrThrow,
    getCurrentNotebookId,
    getNotebookMetadata,
    normalizeCellLanguage,
    normalizeCellStage,
    normalizeNotebookPipelineMode,
    openPublishDialogForSource,
    refreshSidebar,
    requestCellRun,
    revealDataSourceSidebarBrowser,
    setCellStage,
    setNotebookCells,
    setNotebookPipelineMode,
    showConfirmDialog,
    showMessageDialog,
  } = helpers;

  let materializedStagesVersion = null;
  const graphByNotebookId = new Map();
  const selectedStageByNotebookId = new Map();
  let contextMenu = null;
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

  function pipelinePayload(notebookId) {
    const metadata = getNotebookMetadata(notebookId);
    return {
      notebookId,
      notebookTitle: metadata.title || "Notebook",
      cells: (metadata.cells || []).map((cell) => ({
        cellId: cell.cellId,
        language: normalizeCellLanguage(cell.language),
        sql: cell.sql || "",
        dataSources: Array.isArray(cell.dataSources) ? cell.dataSources : [],
        queryOptions: cell.queryOptions || {},
        stage: normalizeCellStage(cell.stage),
      })),
    };
  }

  function pipelineEnabled(notebookId) {
    return normalizeNotebookPipelineMode(getNotebookMetadata(notebookId).pipelineMode) === "pipeline";
  }

  function statusLabel(value) {
    const status = String(value || "planned").trim().toLowerCase();
    if (status === "valid") {
      return "OK";
    }
    return status ? `${status.slice(0, 1).toUpperCase()}${status.slice(1)}` : "Planned";
  }

  function statusClass(value) {
    return String(value || "planned").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-") || "planned";
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
    const queryPath = String(node?.latestRevision?.queryPath || "").trim();
    if (queryPath) {
      return queryPath;
    }
    const alias = String(node?.alias || "").trim();
    return alias ? `stage.${alias}` : "";
  }

  function stageStorageReference(node) {
    const queryPath = String(node?.latestRevision?.queryPath || "").trim();
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
      localRelations[reference] = String(node.latestRevision?.queryPath || reference);
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
      const queryPath = String(node?.latestRevision?.queryPath || "").trim();
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
    if (status === "valid") {
      return `
        <svg class="pipeline-status-icon pipeline-status-icon-ok" viewBox="0 0 24 24" aria-hidden="true">
          <path d="M20 6 9 17l-5-5"></path>
        </svg>
      `;
    }
    if (status === "running" || status === "queued") {
      return `
        <svg class="pipeline-status-icon pipeline-status-icon-running" viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 3v4"></path>
          <path d="M12 17v4"></path>
          <path d="M3 12h4"></path>
          <path d="M17 12h4"></path>
        </svg>
      `;
    }
    return "";
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
    } else if (status === "failed" || status === "cancelled") {
      tone = "failed";
      icon = '<path d="M8 8l8 8"></path><path d="M16 8l-8 8"></path>';
      label = "Stage failed";
    } else if (status === "running" || status === "queued") {
      label = "Stage is running";
    } else if (status === "obsolete") {
      label = "Stage is obsolete and should be rerun";
    }
    return `
      <span class="pipeline-node-state pipeline-node-state-${tone}" title="${escapeHtml(label)}" aria-label="${escapeHtml(label)}">
        <svg viewBox="0 0 24 24" aria-hidden="true">${icon}</svg>
      </span>
    `;
  }

  function menuIcon(name) {
    const icons = {
      inspect: '<path d="M3 12s3.2-5 9-5 9 5 9 5-3.2 5-9 5-9-5-9-5z"></path><circle cx="12" cy="12" r="2.3"></circle>',
      copy: '<rect x="8" y="8" width="10" height="10"></rect><path d="M6 16H4V4h12v2"></path>',
      publish: '<path d="M12 19V5"></path><path d="M7 10l5-5 5 5"></path><path d="M5 19h14"></path>',
      derive: '<path d="M5 6h5a4 4 0 0 1 4 4v8"></path><path d="M10 6l-3-3"></path><path d="M10 6 7 9"></path><path d="M14 18l-3-3"></path><path d="M14 18l3-3"></path>',
      fork: '<path d="M5 12h5"></path><path d="M10 12c3 0 4-5 8-5"></path><path d="M10 12c3 0 4 5 8 5"></path><path d="M18 7l-2-2"></path><path d="M18 7l-2 2"></path><path d="M18 17l-2-2"></path><path d="M18 17l-2 2"></path>',
      run: '<path d="M8 5v14l11-7z"></path>',
      stop: '<rect x="7" y="7" width="10" height="10"></rect>',
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
    const nodeMinWidth = 264;
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
    const { positions, width, height } = graphLayout(graph);
    const selectedStageId = selectedStageByNotebookId.get(graph.notebookId) || graph.defaultSelectedStageId || "";
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
    const nodesMarkup = (graph.nodes || [])
      .map((node) => {
        const box = positions.get(String(node.stageId));
        if (!box) {
          return "";
        }
        const selected = String(node.stageId) === selectedStageId;
        const description = nodeDescription(node);
        return `
          <g
            class="pipeline-node pipeline-node-${statusClass(node.status)}${selected ? " is-selected" : ""}"
            data-pipeline-stage-node="${escapeHtml(node.stageId)}"
            transform="translate(${box.x} ${box.y})"
            tabindex="0"
          >
            <title>${escapeHtml(description || node.title || "")}</title>
            <rect class="pipeline-node-rect" width="${box.width}" height="${box.height}"></rect>
            <foreignObject x="0" y="0" width="${box.width}" height="${box.height}">
              <div class="pipeline-node-body">
                ${nodeStatusMarker(node)}
                <div class="pipeline-node-top">
                  ${graphNodeIcon(node)}
                  <span class="pipeline-node-title">${escapeHtml(node.title || node.alias || "Stage")}</span>
                  ${publishedIcon(node)}
                  ${obsoleteIcon(node)}
                </div>
                <div class="pipeline-node-bottom">
                  <span class="pipeline-status-pill pipeline-status-${statusClass(node.status)}">${escapeHtml(statusLabel(node.status))}</span>
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
          <marker id="pipeline-arrowhead" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
            <path d="M0 0 L8 4 L0 8 Z" class="pipeline-arrowhead"></path>
          </marker>
        </defs>
        <g class="pipeline-edges">${edgesMarkup}</g>
        <g class="pipeline-nodes">${nodesMarkup}</g>
      </svg>
    `;
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
    tableRoot.innerHTML = (graph.nodes || [])
      .map((node) => `
        <tr
          class="pipeline-stage-row${String(node.stageId) === selectedStageId ? " is-selected" : ""}"
          data-pipeline-stage-row="${escapeHtml(node.stageId)}"
          title="${escapeHtml(nodeDescription(node))}"
        >
          <td>
            <span class="pipeline-table-stage-title">${escapeHtml(node.title || node.alias || "Stage")}</span>
            <span class="pipeline-table-stage-alias">${escapeHtml(stageQueryReference(node))}</span>
          </td>
          <td><span class="pipeline-table-status">${statusIcon(node)}<span class="pipeline-status-pill pipeline-status-${statusClass(node.status)}">${escapeHtml(statusLabel(node.status))}</span></span></td>
          <td>${escapeHtml(dependencyCopy(node, graph))}</td>
          <td>${node.latestRevision ? escapeHtml(String(node.latestRevision.rowCount ?? 0)) : "-"}</td>
          <td>
            ${publishedIcon(node)}
            <button type="button" class="pipeline-row-menu-button" data-pipeline-stage-menu="${escapeHtml(node.stageId)}" aria-label="Stage actions" title="Stage actions">...</button>
          </td>
        </tr>
      `)
      .join("");
  }

  function renderStatus(workspaceRoot, graph) {
    const statusRoot = workspaceRoot?.querySelector("[data-notebook-pipeline-status]");
    if (!statusRoot) {
      return;
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
    renderGraph(workspaceRoot, graph);
    renderTable(workspaceRoot, graph);
    renderCellStageState(workspaceRoot, graph);
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
    const graph = await fetchJsonOrThrow("/api/materialized-stages/graph", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(pipelinePayload(notebookId)),
    });
    if (!pipelineEnabled(notebookId)) {
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
      await refreshGraph(notebookId);
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
  }

  function closeContextMenu() {
    contextMenu?.remove();
    contextMenu = null;
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
    const hasRevision = Boolean(node.latestRevision?.outputPath || node.latestRevision?.queryPath);
    const hasStorageReference = Boolean(stageStorageReference(node));
    contextMenu = document.createElement("div");
    contextMenu.className = "pipeline-context-menu workspace-action-menu-panel";
    contextMenu.dataset.pipelineContextMenu = "";
    contextMenu.innerHTML = `
      ${menuItem({ action: "inspect", label: "Inspect data", icon: "inspect", disabled: !hasRevision })}
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
    }
    await showMessageDialog({
      title,
      copy: message,
    });
  }

  function setPipelineRunButtonBusy(notebookId, busy) {
    const workspaceRoot = document.querySelector(
      `[data-workspace-notebook][data-notebook-id="${CSS.escape(notebookId)}"]`
    );
    const button = workspaceRoot?.querySelector("[data-run-notebook-pipeline]");
    if (!button) {
      return;
    }
    button.disabled = Boolean(busy);
    button.classList.toggle("is-running", Boolean(busy));
    const label = button.querySelector("span");
    if (label) {
      label.textContent = busy ? "Running pipeline" : "Run pipeline";
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
    while (Date.now() < deadline) {
      const snapshot = await materializedStageState();
      applyRealtimeState(snapshot);
      const active = (Array.isArray(snapshot?.activeRuns) ? snapshot.activeRuns : []).some(
        (run) => String(run?.notebookId || "") === String(notebookId || "")
      );
      if (!active) {
        return snapshot;
      }
      await sleep(450);
    }
    throw new Error("Pipeline run is still running. The graph will keep updating from live events.");
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
      const active = (Array.isArray(snapshot?.activeRuns) ? snapshot.activeRuns : []).some(
        (run) =>
          String(run?.notebookId || "") === String(notebookId || "") &&
          (run?.stageIds || []).map((item) => String(item)).includes(normalizedStageId)
      );
      if (!active && status && status !== "planned") {
        return latestNode;
      }
      await sleep(450);
    }
    throw new Error("Stage run is still running. The graph will keep updating from live events.");
  }

  async function runPipeline(notebookId = getCurrentNotebookId()) {
    if (!notebookId) {
      return;
    }
    setPipelineRunButtonBusy(notebookId, true);
    try {
      await fetchJsonOrThrow("/api/materialized-stages/pipeline/run", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(pipelinePayload(notebookId)),
      });
      await waitForNotebookRunsIdle(notebookId);
      await refreshGraph(notebookId);
    } catch (error) {
      await reportPipelineError(notebookId, "Pipeline run failed", error);
    } finally {
      setPipelineRunButtonBusy(notebookId, false);
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

  async function inspectStage(node) {
    const source = node?.outputSource || {};
    if (!source.bucket || !source.key) {
      await showMessageDialog({
        title: "Materialized data unavailable",
        copy: "Run this stage before inspecting its materialized data.",
      });
      return;
    }
    await revealDataSourceSidebarBrowser("workspace.s3");
    await refreshSidebar("notebook");
    const selector = `[data-source-object][data-s3-bucket="${CSS.escape(source.bucket)}"][data-s3-key="${CSS.escape(source.key)}"]`;
    const target = document.querySelector(selector);
    if (target) {
      target.scrollIntoView({ block: "center" });
      target.classList.add("is-pipeline-inspect-flash");
      window.setTimeout(() => target.classList.remove("is-pipeline-inspect-flash"), 3000);
    }
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
    if (action === "inspect") {
      await inspectStage(node);
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
        await refreshGraph(notebookId);
        if (version !== modeChangeVersion || !pipelineEnabled(notebookId)) {
          applyModeDom(workspaceRoot, pipelineEnabled(notebookId) ? "pipeline" : "exploration");
        }
      }
      return true;
    }

    const runPipelineButton = event.target.closest("[data-run-notebook-pipeline]");
    if (runPipelineButton) {
      event.preventDefault();
      await runPipeline();
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
    return false;
  }

  function handleContextMenu(event) {
    const node = event.target.closest("[data-pipeline-stage-node], [data-pipeline-stage-row]");
    if (!node) {
      return false;
    }
    event.preventDefault();
    openContextMenu(node, event);
    return true;
  }

  function handleInput(event) {
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

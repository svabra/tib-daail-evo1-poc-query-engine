export function createHomeUi(helpers) {
  const {
    dataGenerationJobElapsedMs,
    escapeHtml,
    formatQueryDuration,
    formatRelativeTimestamp,
    getDataGenerationJobsSnapshot,
    homePageRoot,
    homeRecentIngestionsRoot,
    homeRecentNotebooksRoot,
    notebookLinks,
    readNotebookActivity,
  } = helpers;

  let dataFlowIndex = 0;
  let dataFlowPaused = false;
  let dataFlowAnimationFrame = 0;

  const DATA_FLOW_TONES = new Set(["neutral", "running", "success", "warning", "error"]);

  function dataFlowTone(value) {
    const tone = String(value || "").trim().toLowerCase();
    return DATA_FLOW_TONES.has(tone) ? tone : "neutral";
  }

  function dataFlowHref(value) {
    const href = String(value || "").trim();
    if (!href || href.toLowerCase().startsWith("javascript:")) {
      return "#";
    }
    return href;
  }

  function dataFlowIconMarkup(icon) {
    switch (String(icon || "").trim()) {
      case "csv":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M6 3.8h8.2L18 7.6v12.6H6z"></path>
            <path d="M14.2 3.8v3.8H18"></path>
            <path d="M8.5 11h7"></path>
            <path d="M8.5 14h7"></path>
            <path d="M8.5 17h4.5"></path>
          </svg>
        `;
      case "validation":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 5.4h14"></path>
            <path d="M5 18.6h14"></path>
            <path d="M7.2 8.8h4.8"></path>
            <path d="M7.2 12h9.6"></path>
            <path d="M7.2 15.2h6.8"></path>
          </svg>
        `;
      case "s3":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5.5 7.2 12 4l6.5 3.2v9.6L12 20l-6.5-3.2z"></path>
            <path d="M5.5 7.2 12 10.5l6.5-3.3"></path>
            <path d="M12 10.5V20"></path>
          </svg>
        `;
      case "postgres":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <ellipse cx="12" cy="6.7" rx="6.6" ry="2.4"></ellipse>
            <path d="M5.4 6.7v6.8c0 1.3 3 2.4 6.6 2.4s6.6-1.1 6.6-2.4V6.7"></path>
            <path d="M5.4 10.1c0 1.3 3 2.4 6.6 2.4s6.6-1.1 6.6-2.4"></path>
          </svg>
        `;
      case "loader":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 6h14"></path>
            <path d="M5 12h8"></path>
            <path d="M5 18h14"></path>
            <path d="m15.5 9 3 3-3 3"></path>
          </svg>
        `;
      case "transform":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 7h5.2l3.6 10H19"></path>
            <path d="M5 17h5.2l3.6-10H19"></path>
            <path d="m17 5 2 2-2 2"></path>
            <path d="m17 15 2 2-2 2"></path>
          </svg>
        `;
      case "output":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M4.5 6.5h15"></path>
            <path d="M4.5 12h15"></path>
            <path d="M4.5 17.5h15"></path>
            <path d="M8 4.5v15"></path>
            <path d="M16 4.5v15"></path>
          </svg>
        `;
      case "catalog":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 4.5h14v15H5z"></path>
            <path d="M8 8.2h8"></path>
            <path d="M8 11.8h8"></path>
            <path d="M8 15.4h5"></path>
          </svg>
        `;
      case "individual":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M12 11.4a3.7 3.7 0 1 0 0-7.4 3.7 3.7 0 0 0 0 7.4z"></path>
            <path d="M5.2 20c.9-4.2 3.2-6.2 6.8-6.2s5.9 2 6.8 6.2"></path>
          </svg>
        `;
      case "analytics":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M4.8 18.8h14.4"></path>
            <path d="M6.5 16.5 10 12.8l3.1 2.4 4.4-6.6"></path>
            <path d="M17.4 8.6h-3.2"></path>
            <path d="M17.4 8.6v3.2"></path>
          </svg>
        `;
      case "consumers":
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M8.2 11.2a3.2 3.2 0 1 0 0-6.4 3.2 3.2 0 0 0 0 6.4z"></path>
            <path d="M3.8 18.8c.6-3 2.2-4.5 4.4-4.5s3.8 1.5 4.4 4.5"></path>
            <path d="M16.6 10.4a2.6 2.6 0 1 0 0-5.2 2.6 2.6 0 0 0 0 5.2z"></path>
            <path d="M14 18.8c.4-2.4 1.5-3.6 3.1-3.6 1.5 0 2.6 1.1 3.1 3.6"></path>
          </svg>
        `;
      default:
        return `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 5h14v14H5z"></path>
            <path d="M8 9h8"></path>
            <path d="M8 12h8"></path>
            <path d="M8 15h5"></path>
          </svg>
        `;
    }
  }

  function dataFlowSlides(root) {
    if (!root) {
      return [];
    }
    if (Array.isArray(root.__homeDataFlowSlides)) {
      return root.__homeDataFlowSlides;
    }
    const script = root.querySelector("[data-home-data-flow-json]");
    try {
      const parsed = JSON.parse(script?.textContent || "[]");
      root.__homeDataFlowSlides = Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      root.__homeDataFlowSlides = [];
    }
    return root.__homeDataFlowSlides;
  }

  function dataFlowBadgeMarkup(badge) {
    if (!badge || typeof badge !== "object") {
      return "";
    }
    const tone = dataFlowTone(badge.tone);
    return `
      <span class="home-flow-badge home-flow-tone-${tone}">
        <span class="home-flow-badge-label">${escapeHtml(badge.label || "")}</span>
        <span class="home-flow-badge-detail">${escapeHtml(badge.detail || "")}</span>
      </span>
    `;
  }

  function dataFlowNodeMarkup(node) {
    if (!node || typeof node !== "object") {
      return "";
    }
    const column = Math.max(1, Math.min(6, Number.parseInt(node.column, 10) || 1));
    const row = Math.max(1, Math.min(3, Number.parseInt(node.row, 10) || 1));
    const span = Math.max(1, Math.min(3, Number.parseInt(node.span, 10) || 1));
    const tone = dataFlowTone(node.tone);
    const nodeId = String(node.nodeId || "").trim();
    const nodeClass = nodeId.replace(/[^a-z0-9-]+/gi, "-").toLowerCase();
    const meterMarkup =
      nodeId === "sql-stages"
        ? `
          <span class="home-flow-stage-meter" aria-hidden="true">
            <span></span><span></span><span></span><span></span><span></span><span></span><span></span>
          </span>
        `
        : "";
    return `
      <a
        class="home-flow-node home-flow-node-${escapeHtml(nodeClass)} home-flow-tone-${tone}"
        href="${escapeHtml(dataFlowHref(node.href))}"
        style="--flow-col: ${column}; --flow-row: ${row}; --flow-row-span: ${span};"
        data-home-flow-node="${escapeHtml(nodeId)}"
      >
        <span class="home-flow-node-icon" aria-hidden="true">${dataFlowIconMarkup(node.icon)}</span>
        <span class="home-flow-node-copy">
          <span class="home-flow-node-title">${escapeHtml(node.title || "")}</span>
          <span class="home-flow-node-detail">${escapeHtml(node.detail || "")}</span>
          ${meterMarkup}
        </span>
      </a>
    `;
  }

  function dataFlowActionMarkup(action) {
    if (!action || typeof action !== "object") {
      return "";
    }
    return `
      <a class="home-flow-action" href="${escapeHtml(dataFlowHref(action.href))}">
        ${escapeHtml(action.label || "Open")}
      </a>
    `;
  }

  function dataFlowPathMarkup(flow) {
    const statusTone = dataFlowTone(flow?.status?.tone);
    const flowKey = String(flow?.flowId || "lineage")
      .replace(/[^a-z0-9_-]+/gi, "-")
      .toLowerCase();
    const pathId = (suffix) => `home-flow-${flowKey}-${suffix}`;
    const warningMarker =
      statusTone === "warning" || statusTone === "error"
        ? '<span class="home-flow-warning-marker" aria-hidden="true"></span>'
        : "";
    return `
      <div class="home-flow-canvas" aria-label="Lineage lifecycle visualization">
        <svg class="home-flow-paths" viewBox="0 0 1528 222" preserveAspectRatio="none" aria-hidden="true">
          <path id="${escapeHtml(pathId("route-catalog"))}" d="M74 33 L285 33 C370 33 420 75 517 75 L804 75 L1111 75 C1210 75 1270 33 1403 33"></path>
          <path id="${escapeHtml(pathId("route-individuals"))}" d="M74 117 C155 117 200 33 285 33 C370 33 420 75 517 75 L804 75 L1111 75 C1215 75 1270 117 1403 117"></path>
          <path id="${escapeHtml(pathId("route-innovator"))}" d="M74 117 L285 117 C370 117 420 75 517 75 L804 75 L1111 75 C1215 75 1270 201 1403 201"></path>
          <circle class="home-flow-particle home-flow-particle-one" r="5.6" data-home-flow-particle data-flow-path="${escapeHtml(pathId("route-catalog"))}" data-flow-duration="3625" data-flow-offset="0"></circle>
          <circle class="home-flow-particle home-flow-particle-two" r="5.6" data-home-flow-particle data-flow-path="${escapeHtml(pathId("route-catalog"))}" data-flow-duration="3625" data-flow-offset="0.34"></circle>
          <circle class="home-flow-particle home-flow-particle-three" r="5.6" data-home-flow-particle data-flow-path="${escapeHtml(pathId("route-individuals"))}" data-flow-duration="3938" data-flow-offset="0.12"></circle>
          <circle class="home-flow-particle home-flow-particle-four" r="5.6" data-home-flow-particle data-flow-path="${escapeHtml(pathId("route-individuals"))}" data-flow-duration="3938" data-flow-offset="0.48"></circle>
          <circle class="home-flow-particle home-flow-particle-five" r="5.6" data-home-flow-particle data-flow-path="${escapeHtml(pathId("route-innovator"))}" data-flow-duration="4250" data-flow-offset="0.22"></circle>
          <circle class="home-flow-particle home-flow-particle-six" r="5.6" data-home-flow-particle data-flow-path="${escapeHtml(pathId("route-innovator"))}" data-flow-duration="4250" data-flow-offset="0.58"></circle>
        </svg>
        ${warningMarker}
        <div class="home-flow-lane-labels" aria-hidden="true">
          <span style="--flow-col: 1;">Intake</span>
          <span style="--flow-col: 2;">Landing targets</span>
          <span style="--flow-col: 3;">Loader / normalization</span>
          <span style="--flow-col: 4;">SQL transformation pipeline</span>
          <span style="--flow-col: 5;">Materialized output</span>
          <span style="--flow-col: 6;">Publication / consumers</span>
        </div>
        <div class="home-flow-map">
          ${(flow.nodes || []).map((node) => dataFlowNodeMarkup(node)).join("")}
        </div>
      </div>
    `;
  }

  function syncDataFlowAnimation(root) {
    if (dataFlowAnimationFrame) {
      window.cancelAnimationFrame(dataFlowAnimationFrame);
      dataFlowAnimationFrame = 0;
    }
    const reduceMotion =
      typeof window.matchMedia === "function" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const svg = root.querySelector(".home-flow-paths");
    if (!svg || reduceMotion) {
      return;
    }
    const particles = [...svg.querySelectorAll("[data-home-flow-particle]")]
      .map((particle) => {
        const pathId = particle.getAttribute("data-flow-path");
        const path = pathId ? svg.querySelector(`#${window.CSS?.escape ? CSS.escape(pathId) : pathId}`) : null;
        if (!path || typeof path.getTotalLength !== "function") {
          return null;
        }
        return {
          particle,
          path,
          length: path.getTotalLength(),
          duration: Math.max(1200, Number.parseFloat(particle.dataset.flowDuration || "5000")),
          offset: Number.parseFloat(particle.dataset.flowOffset || "0") || 0,
        };
      })
      .filter(Boolean);
    if (!particles.length) {
      return;
    }
    const startedAt = window.performance?.now?.() || Date.now();
    const draw = (timestamp) => {
      const now = Number.isFinite(timestamp) ? timestamp : (window.performance?.now?.() || Date.now());
      particles.forEach(({ particle, path, length, duration, offset }) => {
        const progress = ((now - startedAt) / duration + offset) % 1;
        const point = path.getPointAtLength(length * progress);
        particle.setAttribute("transform", `translate(${point.x.toFixed(2)} ${point.y.toFixed(2)})`);
      });
      if (!dataFlowPaused) {
        dataFlowAnimationFrame = window.requestAnimationFrame(draw);
      }
    };
    draw(startedAt);
  }

  function bindDataFlowCarousel(root) {
    if (!root || root.dataset.homeFlowBound === "true") {
      return;
    }
    root.dataset.homeFlowBound = "true";
    root.addEventListener("click", (event) => {
      const previousButton = event.target.closest("[data-home-flow-prev]");
      const nextButton = event.target.closest("[data-home-flow-next]");
      const pauseButton = event.target.closest("[data-home-flow-pause]");
      if (!previousButton && !nextButton && !pauseButton) {
        return;
      }
      event.preventDefault();
      const slides = dataFlowSlides(root);
      if (!slides.length) {
        return;
      }
      if (previousButton) {
        dataFlowIndex = (dataFlowIndex - 1 + slides.length) % slides.length;
      } else if (nextButton) {
        dataFlowIndex = (dataFlowIndex + 1) % slides.length;
      } else if (pauseButton) {
        dataFlowPaused = !dataFlowPaused;
      }
      renderDataFlowCarousel(root);
    });
  }

  function renderDataFlowCarousel(root = document.querySelector("[data-home-data-flow]")) {
    if (!root) {
      return;
    }
    bindDataFlowCarousel(root);
    const slides = dataFlowSlides(root);
    if (!slides.length) {
      root.innerHTML = `
        <div class="home-flow-loading">
          <span class="home-flow-loading-title">Data product lineage</span>
          <span class="home-flow-loading-copy">No lineage data is available yet.</span>
        </div>
      `;
      return;
    }
    dataFlowIndex = Math.max(0, Math.min(dataFlowIndex, slides.length - 1));
    const flow = slides[dataFlowIndex] || slides[0];
    const hasMultipleSlides = slides.length > 1;
    root.classList.toggle("is-paused", dataFlowPaused);
    root.dataset.homeFlowPaused = dataFlowPaused ? "true" : "false";
    root.innerHTML = `
      <div class="home-flow-header">
        <div class="home-flow-title-block">
          <h3 id="home-flow-title">Data product lineage</h3>
        </div>
        <div class="home-flow-carousel-shell" aria-label="Selected data product lineage">
          <button type="button" data-home-flow-prev aria-label="Previous lineage slide" ${hasMultipleSlides ? "" : "disabled"}>
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m14.5 6-6 6 6 6"></path></svg>
          </button>
          <span class="home-flow-selected-product">
            <span class="home-flow-selected-icon" aria-hidden="true">${dataFlowIconMarkup("output")}</span>
            <span class="home-flow-selected-title">${escapeHtml(flow.title || "Data product lineage")}</span>
            <span class="home-flow-count">${escapeHtml(String(dataFlowIndex + 1))} / ${escapeHtml(String(slides.length))}</span>
          </span>
          <button type="button" data-home-flow-next aria-label="Next lineage slide" ${hasMultipleSlides ? "" : "disabled"}>
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m9.5 6 6 6-6 6"></path></svg>
          </button>
        </div>
        <div class="home-flow-controls" aria-label="Data product lineage carousel controls">
          <button
            type="button"
            data-home-flow-pause
            aria-label="${dataFlowPaused ? "Resume lineage animation" : "Pause lineage animation"}"
            aria-pressed="${dataFlowPaused ? "true" : "false"}"
          >
            <svg viewBox="0 0 24 24" aria-hidden="true">
              ${dataFlowPaused ? '<path d="M8 5v14l10-7z"></path>' : '<path d="M8 5v14"></path><path d="M16 5v14"></path>'}
            </svg>
          </button>
        </div>
      </div>
      ${dataFlowPathMarkup(flow)}
      <div class="home-flow-badges" aria-label="Lineage metrics">
        ${(flow.badges || []).map((badge) => dataFlowBadgeMarkup(badge)).join("")}
      </div>
      <div class="home-flow-footer">
        ${(flow.actions || []).map((action) => dataFlowActionMarkup(action)).join("")}
      </div>
    `;
    syncDataFlowAnimation(root);
  }

  function notebookActivityReason(entry, { compact = false } = {}) {
    if (entry.reason === "run") {
      return compact ? "Run" : "Last action: Run";
    }
    if (entry.reason === "open") {
      return compact ? "Open" : "Last action: Open";
    }
    return compact ? "Edit" : "Last action: Edit";
  }

  function notebookActivityMarkup(entry) {
    return `
      <button
        type="button"
        class="home-activity-card"
        data-open-recent-notebook="${escapeHtml(entry.notebookId)}"
      >
        <span class="home-activity-title-row">
          <span class="home-activity-title">${escapeHtml(entry.title || "Notebook")}</span>
          <span class="home-activity-meta">${escapeHtml(formatRelativeTimestamp(entry.touchedAt))}</span>
        </span>
        <span class="home-activity-copy">${escapeHtml(entry.summary || "No description saved.")}</span>
        <span class="home-activity-meta">${escapeHtml(notebookActivityReason(entry))}</span>
      </button>
    `;
  }

  function recentNotebookActivityEntries(limit) {
    return Object.values(readNotebookActivity())
      .filter((entry) => entry && typeof entry === "object")
      .map((entry) => ({
        notebookId: String(entry.notebookId || "").trim(),
        title: String(entry.title || "").trim(),
        summary: String(entry.summary || "").trim(),
        touchedAt: String(entry.touchedAt || "").trim(),
        reason: ["open", "run"].includes(entry.reason) ? entry.reason : "edited",
      }))
      .filter((entry) => entry.notebookId && notebookLinks(entry.notebookId).length)
      .sort((left, right) => Date.parse(right.touchedAt || "") - Date.parse(left.touchedAt || ""))
      .slice(0, limit);
  }

  function renderRecentNotebooks(root, { limit } = {}) {
    if (!root) {
      return;
    }

    const entries = recentNotebookActivityEntries(limit);
    if (!entries.length) {
      root.innerHTML = '<p class="home-empty">No recent notebook activity yet.</p>';
      return;
    }

    root.innerHTML = entries.map((entry) => notebookActivityMarkup(entry)).join("");
  }

  function updateQueryWorkbenchShortcuts() {
    const continueShortcut = document.querySelector("[data-home-continue-last-notebook]");
    if (!continueShortcut) {
      return;
    }

    const latestNotebook = recentNotebookActivityEntries(1)[0] || null;
    const latestCopy = continueShortcut.querySelector("[data-home-continue-last-copy]");
    const latestTooltip = continueShortcut.querySelector("[data-home-continue-last-tooltip]");
    if (!latestNotebook) {
      delete continueShortcut.dataset.openQueryWorkbench;
      delete continueShortcut.dataset.openRecentNotebook;
      continueShortcut.classList.add("is-disabled");
      continueShortcut.setAttribute("aria-disabled", "true");
      continueShortcut.title = "No recent notebook is available in this browser yet.";
      if (latestCopy) {
        latestCopy.textContent = "No recent notebook";
      }
      if (latestTooltip) {
        latestTooltip.textContent = "No recent notebook is available in this browser yet.";
      }
      return;
    }

    const notebookTitle = latestNotebook.title || "Notebook";
    continueShortcut.dataset.openQueryWorkbench = "";
    continueShortcut.dataset.openRecentNotebook = latestNotebook.notebookId;
    continueShortcut.classList.remove("is-disabled");
    continueShortcut.setAttribute("aria-disabled", "false");
    continueShortcut.title = `Continue with ${notebookTitle}.`;
    if (latestCopy) {
      latestCopy.textContent = notebookTitle;
    }
    if (latestTooltip) {
      latestTooltip.textContent = `Open ${notebookTitle}, last touched ${formatRelativeTimestamp(
        latestNotebook.touchedAt
      )}.`;
    }
  }

  function ingestionActivityMarkup(job) {
    return `
      <button
        type="button"
        class="home-activity-card"
        data-open-loader-workbench
        data-focus-generation-job="${escapeHtml(job.jobId || "")}" 
      >
        <span class="home-activity-title-row">
          <span class="home-activity-title">${escapeHtml(job.title || "Loader run")}</span>
          <span class="home-activity-meta">${escapeHtml(formatRelativeTimestamp(job.startedAt || job.updatedAt))}</span>
        </span>
        <span class="home-activity-copy">${escapeHtml(job.message || job.description || "No loader message yet.")}</span>
        <span class="home-activity-meta">${escapeHtml((job.status || "unknown").replace(/^./, (match) => match.toUpperCase()))} • ${escapeHtml(formatQueryDuration(dataGenerationJobElapsedMs(job)))}</span>
      </button>
    `;
  }

  function renderHomePage() {
    renderRecentNotebooks(homeRecentNotebooksRoot(), { limit: 3 });
    updateQueryWorkbenchShortcuts();

    if (!homePageRoot()) {
      return;
    }

    renderDataFlowCarousel();

    const recentIngestionsRoot = homeRecentIngestionsRoot();
    if (recentIngestionsRoot) {
      const recentJobs = [...getDataGenerationJobsSnapshot()]
        .sort((left, right) => Date.parse(right.startedAt || "") - Date.parse(left.startedAt || ""))
        .slice(0, 3);
      if (!recentJobs.length) {
        recentIngestionsRoot.innerHTML = '<p class="home-empty">No loader runs yet.</p>';
      } else {
        recentIngestionsRoot.innerHTML = recentJobs.map((job) => ingestionActivityMarkup(job)).join("");
      }
    }
  }

  return {
    renderHomePage,
  };
}

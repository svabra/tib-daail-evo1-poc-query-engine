const VIEW_STORAGE_KEY = "bdw.dataSources.viewMode";
const ICON_ROOT = "/static/img/source-icons";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function iconPath(source) {
  const key = ["oracle", "postgresql", "s3", "local"].includes(source.technologyKey)
    ? source.technologyKey
    : "local";
  return `${ICON_ROOT}/${key}.svg`;
}

function ingestionHref(source) {
  return `/ingestion-workbench/sourcing/ingestions/new?sourceId=${encodeURIComponent(source.id)}`;
}

function actions(source, { compact = false, mode = "management", selectedSourceId = "" } = {}) {
  const browserMode = mode === "browser";
  const selected = browserMode && source.id === selectedSourceId;
  if (browserMode) {
    return `<div class="source-catalog-row-actions">
      <a class="source-catalog-button${selected ? " is-current" : ""}" href="${escapeHtml(source.browsePath)}" data-open-data-source-explorer="${escapeHtml(source.id)}"${selected ? ' aria-current="page"' : ""}>${selected ? "Browsing" : "Browse data"}</a>
      <a class="source-catalog-action-link" href="${escapeHtml(source.managementPath)}" data-open-query-data-source="${escapeHtml(source.id)}">Source details</a>
      ${source.ingestionCapable ? `<a href="${ingestionHref(source)}">Create ingestion</a>` : ""}
      <div class="source-catalog-context-menu">
        <button type="button" aria-label="More actions for ${escapeHtml(source.name)}" aria-expanded="false" data-source-context-toggle><img src="${ICON_ROOT}/more.svg" alt=""></button>
        <div hidden data-source-context-panel><a href="${escapeHtml(source.browsePath)}" data-open-data-source-explorer="${escapeHtml(source.id)}">Browse data</a><a href="${escapeHtml(source.managementPath)}" data-open-query-data-source="${escapeHtml(source.id)}">Source details</a>${source.ingestionCapable ? `<a href="${ingestionHref(source)}">Create ingestion</a>` : ""}</div>
      </div>
    </div>`;
  }
  const openLabel = compact ? "Select" : "Open";
  const openHref = compact ? ingestionHref(source) : source.managementPath;
  const ingestion = source.ingestionCapable && !compact
    ? `<a href="${ingestionHref(source)}">Create ingestion</a>`
    : "";
  return `<div class="source-catalog-row-actions">
    <a class="source-catalog-button source-catalog-button-secondary" href="${escapeHtml(openHref)}">${openLabel}</a>
    <a class="source-catalog-action-link" href="${escapeHtml(source.browsePath)}" data-open-data-source-explorer="${escapeHtml(source.id)}">Browse data</a>
    ${ingestion}
    ${compact ? "" : `<div class="source-catalog-context-menu">
      <button type="button" aria-label="More actions for ${escapeHtml(source.name)}" aria-expanded="false" data-source-context-toggle><img src="${ICON_ROOT}/more.svg" alt=""></button>
      <div hidden data-source-context-panel><a href="${escapeHtml(source.managementPath)}">Source details</a>${source.ingestionCapable ? `<a href="${ingestionHref(source)}">Create ingestion</a>` : ""}</div>
    </div>`}
  </div>`;
}

function renderTable(items, options) {
  return `<div class="source-catalog-table-wrap"><table class="source-catalog-table">
    <thead><tr><th>Data source</th><th>Technology</th><th>Status</th><th>Location</th><th>Schemas / objects</th><th><span class="visually-hidden">Actions</span></th></tr></thead>
    <tbody>${items.map((source) => `<tr class="${source.id === options.selectedSourceId ? "is-selected" : ""}" data-source-catalog-row-id="${escapeHtml(source.id)}">
      <td data-label="Data source"><div class="source-catalog-source-cell"><span class="source-technology-icon"><img src="${iconPath(source)}" alt="${escapeHtml(source.technology)}"></span><div><strong>${escapeHtml(source.name)}</strong><span>${escapeHtml(source.databaseName || source.id)}</span><small>${escapeHtml(source.accessModel)}</small></div></div></td>
      <td data-label="Technology">${escapeHtml(source.technology)}</td>
      <td data-label="Status"><span class="source-catalog-status" data-status="${escapeHtml(source.status)}">${escapeHtml(source.statusLabel)}</span></td>
      <td data-label="Location">${escapeHtml(source.location)}</td>
      <td data-label="Schemas / objects"><strong>${Number(source.schemaCount || 0).toLocaleString("de-CH")}</strong> / ${Number(source.objectCount || 0).toLocaleString("de-CH")}</td>
      <td data-label="Actions">${actions(source, options)}</td>
    </tr>`).join("")}</tbody>
  </table></div>`;
}

function renderList(items, options) {
  return `<div class="source-catalog-records">${items.map((source) => `<article class="source-catalog-record${source.id === options.selectedSourceId ? " is-selected" : ""}" data-source-catalog-row-id="${escapeHtml(source.id)}">
    <div class="source-catalog-record-main"><span class="source-technology-icon"><img src="${iconPath(source)}" alt="${escapeHtml(source.technology)}"></span><div><span class="source-catalog-record-kicker">${escapeHtml(source.technology)} · ${escapeHtml(source.location)}</span><h4>${escapeHtml(source.name)}</h4><p>${escapeHtml(source.summary)}</p><div class="source-catalog-record-tags"><span>${escapeHtml(source.accessModel)}</span><span>${Number(source.schemaCount || 0)} schemas</span><span>${Number(source.objectCount || 0).toLocaleString("de-CH")} objects</span></div></div></div>
    <div class="source-catalog-record-side"><span class="source-catalog-status" data-status="${escapeHtml(source.status)}">${escapeHtml(source.statusLabel)}</span>${actions(source, options)}</div>
  </article>`).join("")}</div>`;
}

export function renderSourceCatalog(target, items, { viewMode = "table", compact = false, mode = "management", selectedSourceId = "" } = {}) {
  if (!target) return;
  if (!items.length) {
    target.innerHTML = '<div class="source-catalog-empty"><strong>No matching data sources</strong><p>Adjust the filters or request access to another source.</p></div>';
    return;
  }
  target.dataset.sourceCatalogView = viewMode;
  const options = { compact, mode, selectedSourceId };
  target.innerHTML = viewMode === "list" ? renderList(items, options) : renderTable(items, options);
}

export function storedSourceCatalogView() {
  try {
    const value = window.sessionStorage.getItem(VIEW_STORAGE_KEY);
    return value === "list" ? "list" : "table";
  } catch {
    return "table";
  }
}

export function storeSourceCatalogView(viewMode) {
  try {
    window.sessionStorage.setItem(VIEW_STORAGE_KEY, viewMode === "list" ? "list" : "table");
  } catch {
    // The table default remains available without browser storage.
  }
}

function fillFacet(select, values, allLabel) {
  if (!select || select.dataset.sourceFacetReady === "true") return;
  select.replaceChildren(new Option(allLabel, ""));
  Object.entries(values || {}).forEach(([value, count]) => select.append(new Option(`${value} (${count})`, value)));
  select.dataset.sourceFacetReady = "true";
}

async function loadRemoteCatalog(root, state) {
  const target = root.querySelector("[data-source-catalog]");
  const params = new URLSearchParams({
    q: root.querySelector("[data-source-catalog-query]")?.value.trim() || "",
    technology: root.querySelector("[data-source-catalog-technology]")?.value || "",
    status: root.querySelector("[data-source-catalog-status]")?.value || "",
    location: root.querySelector("[data-source-catalog-location]")?.value || "",
    offset: String(state.offset),
    limit: "25",
  });
  target.setAttribute("aria-busy", "true");
  try {
    const response = await fetch(`/api/data-sources?${params}`, { headers: { Accept: "application/json" } });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `Request failed: ${response.status}`);
    state.payload = payload;
    ["total", "available", "schemas", "objects"].forEach((key) => {
      const node = root.querySelector(`[data-source-catalog-summary="${key}"]`);
      if (node) node.textContent = Number(payload.summary?.[key] || 0).toLocaleString("de-CH");
    });
    fillFacet(root.querySelector("[data-source-catalog-technology]"), payload.facets?.technologies, "All technologies");
    fillFacet(root.querySelector("[data-source-catalog-status]"), payload.facets?.statuses, "All statuses");
    fillFacet(root.querySelector("[data-source-catalog-location]"), payload.facets?.locations, "All locations");
    renderSourceCatalog(target, payload.items || [], {
      viewMode: state.viewMode,
      mode: root.dataset.sourceCatalogMode || "management",
      selectedSourceId: root.dataset.selectedSourceId || "",
    });
    const matched = Number(payload.summary?.matched || 0);
    root.querySelector("[data-source-catalog-count]").textContent = `${matched.toLocaleString("de-CH")} data source${matched === 1 ? "" : "s"} found`;
    const pagination = root.querySelector("[data-source-catalog-pagination]");
    pagination.hidden = matched <= 25;
    pagination.querySelector('[data-source-catalog-page="previous"]').disabled = !payload.pagination?.hasPrevious;
    pagination.querySelector('[data-source-catalog-page="next"]').disabled = !payload.pagination?.hasNext;
    const first = matched ? Number(payload.pagination.offset || 0) + 1 : 0;
    const last = Math.min(matched, Number(payload.pagination.offset || 0) + Number(payload.pagination.limit || 25));
    pagination.querySelector("[data-source-catalog-page-label]").textContent = `${first}–${last} of ${matched}`;
  } catch (error) {
    target.innerHTML = `<div class="source-catalog-empty" role="alert"><strong>Data sources could not be loaded</strong><p>${escapeHtml(error.message)}</p></div>`;
  } finally {
    target.removeAttribute("aria-busy");
  }
}

export function initializeRemoteSourceCatalog(root) {
  if (!root || root.dataset.sourceCatalogInitialized === "true") return;
  root.dataset.sourceCatalogInitialized = "true";
  const state = { offset: 0, viewMode: storedSourceCatalogView(), payload: null, timer: null };
  root.querySelectorAll("[data-source-catalog-view]").forEach((button) => {
    const active = button.dataset.sourceCatalogView === state.viewMode;
    button.setAttribute("aria-pressed", String(active));
    button.classList.toggle("is-active", active);
  });
  const reloadFromStart = () => { state.offset = 0; loadRemoteCatalog(root, state); };
  root.addEventListener("input", (event) => {
    if (!event.target.closest("[data-source-catalog-query]")) return;
    window.clearTimeout(state.timer);
    state.timer = window.setTimeout(reloadFromStart, 180);
  });
  root.addEventListener("change", (event) => {
    if (event.target.closest("[data-source-catalog-technology], [data-source-catalog-status], [data-source-catalog-location]")) reloadFromStart();
  });
  root.addEventListener("click", (event) => {
    const view = event.target.closest("[data-source-catalog-view]");
    if (view) {
      state.viewMode = view.dataset.sourceCatalogView === "list" ? "list" : "table";
      storeSourceCatalogView(state.viewMode);
      root.querySelectorAll("[data-source-catalog-view]").forEach((button) => {
        const active = button.dataset.sourceCatalogView === state.viewMode;
        button.setAttribute("aria-pressed", String(active));
        button.classList.toggle("is-active", active);
      });
      renderSourceCatalog(root.querySelector("[data-source-catalog]"), state.payload?.items || [], {
        viewMode: state.viewMode,
        mode: root.dataset.sourceCatalogMode || "management",
        selectedSourceId: root.dataset.selectedSourceId || "",
      });
      return;
    }
    const page = event.target.closest("[data-source-catalog-page]");
    if (page) {
      state.offset = Math.max(0, state.offset + (page.dataset.sourceCatalogPage === "next" ? 25 : -25));
      loadRemoteCatalog(root, state);
      return;
    }
    if (event.target.closest("[data-source-catalog-reset]")) {
      root.querySelectorAll("[data-source-catalog-query], [data-source-catalog-technology], [data-source-catalog-status], [data-source-catalog-location]").forEach((field) => { field.value = ""; });
      reloadFromStart();
      return;
    }
    const toggle = event.target.closest("[data-source-context-toggle]");
    if (toggle) {
      const panel = toggle.parentElement.querySelector("[data-source-context-panel]");
      const expanded = toggle.getAttribute("aria-expanded") === "true";
      toggle.setAttribute("aria-expanded", String(!expanded));
      panel.hidden = expanded;
    }
  });
  loadRemoteCatalog(root, state);
}

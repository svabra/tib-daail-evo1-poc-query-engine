import {
  WORKBENCH_SEARCH_KINDS,
  loadWorkbenchSearchIndex,
} from "./home-notebook-search.js";
import {
  expertSearchKindFromParams,
  searchExpertWorkbenchIndex,
} from "./expert-search-filter.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function resultMarkup(item) {
  const tags = (item.tags || []).slice(0, 4);
  return `
    <article
      class="workbench-expert-search-result"
      data-workbench-expert-search-result-kind="${escapeHtml(item.kind || "content")}"
    >
      <div>
        <span class="workbench-expert-search-result-kind">${escapeHtml(
          item.kindLabel || WORKBENCH_SEARCH_KINDS[item.kind] || "Inhalt"
        )}</span>
        <h3><a href="${escapeHtml(item.targetUrl)}">${escapeHtml(item.title)}</a></h3>
        <p>${escapeHtml(item.summary || "Keine Beschreibung hinterlegt.")}</p>
        ${
          tags.length
            ? `<ul>${tags.map((tag) => `<li>${escapeHtml(tag)}</li>`).join("")}</ul>`
            : ""
        }
      </div>
      <aside>
        <dl>
          <div><dt>Typ</dt><dd>${escapeHtml(item.kindLabel || "Inhalt")}</dd></div>
          <div><dt>Kontext</dt><dd>${escapeHtml(item.path || "DAAIF")}</dd></div>
        </dl>
        <a class="home-primary-button" href="${escapeHtml(item.targetUrl)}">Öffnen</a>
      </aside>
    </article>`;
}

export function initializeWorkbenchExpertSearch(root = document) {
  const page = root.matches?.("[data-workbench-expert-search-page]")
    ? root
    : root.querySelector?.("[data-workbench-expert-search-page]");
  if (!(page instanceof HTMLElement) || page.dataset.bound === "true") {
    return;
  }
  const form = page.querySelector("[data-workbench-expert-search-form]");
  const input = page.querySelector("[data-workbench-expert-search-input]");
  const kindSelect = page.querySelector("[data-workbench-expert-search-kind]");
  const clearButton = page.querySelector("[data-workbench-expert-search-clear]");
  const heading = page.querySelector("[data-workbench-expert-search-heading]");
  const summary = page.querySelector("[data-workbench-expert-search-summary]");
  const resultsRoot = page.querySelector("[data-workbench-expert-search-results]");
  if (
    !(form instanceof HTMLFormElement) ||
    !(input instanceof HTMLInputElement) ||
    !(kindSelect instanceof HTMLSelectElement) ||
    !(clearButton instanceof HTMLButtonElement) ||
    !(heading instanceof HTMLElement) ||
    !(summary instanceof HTMLElement) ||
    !(resultsRoot instanceof HTMLElement)
  ) {
    return;
  }
  page.dataset.bound = "true";
  const params = new URLSearchParams(window.location.search);
  input.value = params.get("q") || "";
  kindSelect.value = expertSearchKindFromParams(params);
  let items = [];
  let loaded = false;

  function updateUrl() {
    const query = input.value.trim();
    const nextParams = new URLSearchParams();
    if (query) nextParams.set("q", query);
    if (kindSelect.value !== "all") nextParams.set("kind", kindSelect.value);
    window.history.replaceState({}, "", `${window.location.pathname}${nextParams.size ? `?${nextParams}` : ""}`);
  }

  function render() {
    const query = input.value.trim();
    updateUrl();
    clearButton.hidden = !query;
    if (!loaded) {
      heading.textContent = "Suchindex wird geladen …";
      summary.textContent = "Notebooks, Data Sources, Datenobjekte und DAAIF-Datenprodukte werden vorbereitet.";
      resultsRoot.replaceChildren();
      return;
    }
    const results = searchExpertWorkbenchIndex(items, query, kindSelect.value);
    heading.textContent = `${results.length} ${results.length === 1 ? "Ergebnis" : "Ergebnisse"}`;
    summary.textContent = kindSelect.options[kindSelect.selectedIndex]?.textContent || "Alle Inhalte";
    resultsRoot.innerHTML = results.length
      ? results.map(resultMarkup).join("")
      : query
        ? `<div class="workbench-expert-search-empty"><strong>Keine passenden Inhalte gefunden.</strong><p>Versuchen Sie einen anderen Suchbegriff oder wählen Sie alle Inhaltstypen.</p></div>`
        : `<div class="workbench-expert-search-empty"><strong>Keine Inhalte vorhanden.</strong><p>Für den gewählten Inhaltstyp sind noch keine Einträge verfügbar.</p></div>`;
  }

  form.addEventListener("submit", (event) => event.preventDefault());
  input.addEventListener("input", render);
  kindSelect.addEventListener("change", render);
  clearButton.addEventListener("click", () => {
    input.value = "";
    input.focus();
    render();
  });
  loadWorkbenchSearchIndex()
    .then((searchItems) => {
      items = searchItems;
      loaded = true;
      render();
    })
    .catch((error) => {
      loaded = true;
      heading.textContent = "Suche momentan nicht verfügbar";
      summary.textContent = "Der lokale Suchindex konnte nicht geladen werden.";
      resultsRoot.replaceChildren();
      console.error("Failed to load expert workbench search index.", error);
    });
  render();
  input.focus({ preventScroll: true });
}

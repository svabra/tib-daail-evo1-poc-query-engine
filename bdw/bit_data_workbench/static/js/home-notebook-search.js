const HERO_THEME_STORAGE_KEY = "daaif.home.hero-theme";
export const WORKBENCH_SEARCH_MIN_LENGTH = 2;
export const WORKBENCH_LIVE_RESULT_LIMIT = 3;

export const WORKBENCH_SEARCH_KINDS = Object.freeze({
  notebook: "Notebook",
  source: "Data Source",
  object: "Datenobjekt",
  product: "Datenprodukt",
});

const workbenchSearchFieldCache = new WeakMap();

export const HOME_HERO_THEMES = Object.freeze([
  "swiss-alps-glacier",
  "swiss-federal-palace-summer",
  "swiss-aarau-old-town-summer",
  "swiss-neuchatel-castle-lake",
  "swiss-lion-monument-lucerne",
  "swiss-ticino-morcote-summer",
  "swiss-rhine-falls-schaffhausen",
  "swiss-graubuenden-vineyards",
  "swiss-lausanne-cathedral-summer",
  "swiss-rural-jura-summer",
]);

export function selectNextHomeHero(previousTheme, random = Math.random) {
  const previous = previousTheme || HOME_HERO_THEMES[0];
  const candidates = HOME_HERO_THEMES.filter((theme) => theme !== previous);
  const index = Math.min(
    candidates.length - 1,
    Math.max(0, Math.floor(random() * candidates.length))
  );
  return candidates[index] ?? HOME_HERO_THEMES[0];
}

export function normalizeNotebookSearchText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLocaleLowerCase("de-CH")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizedNotebook(item, fallbackType = "built-in") {
  const id = String(item?.id ?? item?.notebookId ?? "").trim();
  if (!id) {
    return null;
  }
  return {
    id,
    kind: "notebook",
    kindLabel: WORKBENCH_SEARCH_KINDS.notebook,
    title: String(item?.title || "Notebook").trim() || "Notebook",
    summary: String(item?.summary || "").trim(),
    tags: Array.isArray(item?.tags)
      ? item.tags.map((tag) => String(tag || "").trim()).filter(Boolean)
      : [],
    path: String(item?.path || "").trim(),
    type: String(item?.type || fallbackType).trim() || fallbackType,
    targetUrl: String(item?.targetUrl || "").trim() || `/notebooks/${encodeURIComponent(id)}`,
  };
}

export function normalizedDataSource(item) {
  const sourceId = String(item?.source_id ?? item?.sourceId ?? "").trim();
  if (!sourceId) {
    return null;
  }
  return {
    id: `source:${sourceId}`,
    kind: "source",
    kindLabel: WORKBENCH_SEARCH_KINDS.source,
    title: String(item?.label || sourceId).trim() || sourceId,
    summary: String(item?.storage_tooltip || "").trim(),
    tags: [item?.classification, item?.computation_mode]
      .map((value) => String(value || "").trim())
      .filter(Boolean),
    path: sourceId,
    type: String(item?.computation_mode || "source").trim() || "source",
    targetUrl: `/data-sources?source_id=${encodeURIComponent(sourceId)}`,
  };
}

export function normalizedDataProduct(item) {
  const productId = String(item?.productId ?? item?.id ?? "").trim();
  const slug = String(item?.slug || "").trim();
  if (!productId || !slug) {
    return null;
  }
  return {
    id: `product:${productId}`,
    kind: "product",
    kindLabel: WORKBENCH_SEARCH_KINDS.product,
    title: String(item?.title || slug).trim() || slug,
    summary: String(item?.description || "").trim(),
    tags: Array.isArray(item?.tags)
      ? item.tags.map((tag) => String(tag || "").trim()).filter(Boolean)
      : [],
    path: [item?.owner, item?.domain]
      .map((value) => String(value || "").trim())
      .filter(Boolean)
      .join(" · "),
    type: String(item?.accessLevel || "internal").trim() || "internal",
    targetUrl: String(item?.documentationPath || `/dataproducts/${encodeURIComponent(slug)}`),
  };
}

function localNotebookPaths(storage = window.localStorage) {
  const paths = new Map();
  let tree = [];
  try {
    tree = JSON.parse(storage.getItem("bdw.notebookTree.v2") || "[]");
  } catch (_error) {
    return paths;
  }

  function visit(nodes, path = []) {
    if (!Array.isArray(nodes)) {
      return;
    }
    nodes.forEach((node) => {
      if (!node || typeof node !== "object") {
        return;
      }
      if (node.type === "notebook" && node.notebookId) {
        paths.set(String(node.notebookId), path.join(" / "));
        return;
      }
      const name = String(node.name || node.label || "").trim();
      const children = node.children ?? node.nodes ?? [];
      if (name) {
        visit(children, [...path, name]);
      }
    });
  }
  visit(tree);
  return paths;
}

export function browserLocalNotebookIndex(storage = window.localStorage) {
  let metadata = {};
  try {
    metadata = JSON.parse(storage.getItem("bdw.notebookMeta.v1") || "{}");
  } catch (_error) {
    return [];
  }
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return [];
  }
  const paths = localNotebookPaths(storage);
  return Object.entries(metadata)
    .filter(([id, value]) => id.startsWith("local-notebook-") && value?.deleted !== true)
    .map(([id, value]) =>
      normalizedNotebook(
        {
          id,
          title: value?.title,
          summary: value?.summary,
          tags: value?.tags,
          path: paths.get(id) || "Local Workspace",
          type: "browser-local",
          targetUrl: "/query-workbench",
        },
        "browser-local"
      )
    )
    .filter(Boolean);
}

export function mergeNotebookSearchIndexes(serverItems, localItems) {
  const byId = new Map();
  (Array.isArray(serverItems) ? serverItems : []).forEach((item) => {
    const normalized = normalizedNotebook(item);
    if (normalized) {
      byId.set(normalized.id, normalized);
    }
  });
  (Array.isArray(localItems) ? localItems : []).forEach((item) => {
    const normalized = normalizedNotebook(item, "browser-local");
    if (normalized) {
      byId.set(normalized.id, normalized);
    }
  });
  return [...byId.values()];
}

export function searchNotebookIndex(items, query, limit = 8) {
  return searchWorkbenchIndex(items, query, limit, "notebook");
}

export function workbenchSearchIsReady(query) {
  return normalizeNotebookSearchText(query).length >= WORKBENCH_SEARCH_MIN_LENGTH;
}

function workbenchSearchFields(item) {
  if (!item || typeof item !== "object") {
    return null;
  }
  const cached = workbenchSearchFieldCache.get(item);
  if (cached) {
    return cached;
  }
  const fields = {
    title: normalizeNotebookSearchText(item.title),
    tags: normalizeNotebookSearchText((item.tags || []).join(" ")),
    path: normalizeNotebookSearchText(item.path),
    summary: normalizeNotebookSearchText(item.summary),
  };
  fields.searchable = `${fields.title} ${fields.tags} ${fields.path} ${fields.summary}`;
  workbenchSearchFieldCache.set(item, fields);
  return fields;
}

function scoreWorkbenchSearchItem(item, terms, kind) {
  if (
    kind !== "all" &&
    item.kind !== kind &&
    !(kind === "notebook" && !item.kind)
  ) {
    return null;
  }
  const fields = workbenchSearchFields(item);
  if (!fields || !terms.every((term) => fields.searchable.includes(term))) {
    return null;
  }
  const phrase = terms.join(" ");
  const score =
    (fields.title === phrase ? 100 : 0) +
    (fields.title.startsWith(phrase) ? 40 : 0) +
    terms.reduce(
      (total, term) =>
        total +
        (fields.title.includes(term) ? 12 : 0) +
        (fields.tags.includes(term) ? 6 : 0) +
        (fields.path.includes(term) ? 3 : 0) +
        (fields.summary.includes(term) ? 1 : 0),
      0
    );
  return { item, score };
}

function compareWorkbenchSearchEntries(left, right) {
  return (
    right.score - left.score ||
    left.item.title.localeCompare(right.item.title, "de-CH", { sensitivity: "base" })
  );
}

function primeWorkbenchSearchIndex(items) {
  (Array.isArray(items) ? items : []).forEach(workbenchSearchFields);
  return items;
}

export function searchWorkbenchIndex(
  items,
  query,
  limit = Number.POSITIVE_INFINITY,
  kind = "all"
) {
  const terms = normalizeNotebookSearchText(query).split(/\s+/).filter(Boolean);
  if (!workbenchSearchIsReady(query) || !terms.length) {
    return [];
  }
  return (Array.isArray(items) ? items : [])
    .map((item) => scoreWorkbenchSearchItem(item, terms, kind))
    .filter(Boolean)
    .sort(compareWorkbenchSearchEntries)
    .slice(0, Number.isFinite(Number(limit)) ? Math.max(0, Number(limit)) : undefined)
    .map((entry) => entry.item);
}

export function searchWorkbenchPreview(
  items,
  query,
  limit = WORKBENCH_LIVE_RESULT_LIMIT,
  kind = "all"
) {
  const terms = normalizeNotebookSearchText(query).split(/\s+/).filter(Boolean);
  const normalizedLimit = Math.max(0, Number(limit) || 0);
  if (!workbenchSearchIsReady(query) || !terms.length || normalizedLimit === 0) {
    return { totalCount: 0, items: [] };
  }
  let totalCount = 0;
  const topEntries = [];
  (Array.isArray(items) ? items : []).forEach((item) => {
    const entry = scoreWorkbenchSearchItem(item, terms, kind);
    if (!entry) {
      return;
    }
    totalCount += 1;
    topEntries.push(entry);
    topEntries.sort(compareWorkbenchSearchEntries);
    if (topEntries.length > normalizedLimit) {
      topEntries.pop();
    }
  });
  return { totalCount, items: topEntries.map((entry) => entry.item) };
}

export async function loadWorkbenchSearchIndex({
  fetchImpl = window.fetch.bind(window),
  storage = window.localStorage,
} = {}) {
  const [notebookResponse, catalogResponse, productResponse] = await Promise.all([
    fetchImpl("/api/notebooks/search-index", {
      headers: { Accept: "application/json" },
      cache: "no-cache",
    }),
    fetchImpl("/api/workbench/catalog-search-index", {
      headers: { Accept: "application/json" },
      cache: "no-cache",
    }),
    fetchImpl("/api/data-products", {
      headers: { Accept: "application/json" },
      cache: "no-cache",
    }),
  ]);
  for (const response of [notebookResponse, catalogResponse, productResponse]) {
    if (!response.ok) {
      throw new Error(`Workbench search index request failed: ${response.status}`);
    }
  }
  const [notebookPayload, catalogPayload, productPayload] = await Promise.all([
    notebookResponse.json(),
    catalogResponse.json(),
    productResponse.json(),
  ]);
  const notebooks = mergeNotebookSearchIndexes(
    notebookPayload?.items,
    browserLocalNotebookIndex(storage)
  );
  const catalogItems = Array.isArray(catalogPayload?.items) ? catalogPayload.items : [];
  const products = (Array.isArray(productPayload?.products) ? productPayload.products : [])
    .map(normalizedDataProduct)
    .filter(Boolean);
  return primeWorkbenchSearchIndex([...notebooks, ...catalogItems, ...products]);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setHeroTheme(root) {
  const picture = root.querySelector("[data-home-hero-picture]");
  if (!(picture instanceof HTMLElement)) {
    return;
  }
  let previous = "";
  try {
    previous = window.sessionStorage.getItem(HERO_THEME_STORAGE_KEY) || "";
  } catch (_error) {
    // Rotation remains available without storage.
  }
  const theme = selectNextHomeHero(previous);
  try {
    window.sessionStorage.setItem(HERO_THEME_STORAGE_KEY, theme);
  } catch (_error) {
    // Rotation remains available without storage.
  }
  const assetRoot = `/static/img/daca/${theme}`;
  const avif = picture.querySelector('[data-home-hero-source="avif"]');
  const webp = picture.querySelector('[data-home-hero-source="webp"]');
  const image = picture.querySelector("[data-home-hero-image]");
  if (avif instanceof HTMLSourceElement) {
    avif.srcset = `${assetRoot}-960.avif 960w, ${assetRoot}-1600.avif 1600w`;
  }
  if (webp instanceof HTMLSourceElement) {
    webp.srcset = `${assetRoot}-960.webp 960w, ${assetRoot}-1600.webp 1600w`;
  }
  if (image instanceof HTMLImageElement) {
    image.addEventListener("load", () => picture.classList.add("is-loaded"), { once: true });
    image.src = `${assetRoot}-1600.webp`;
  }
}

function resultMarkup(item, index, activeIndex) {
  const meta = [item.kindLabel || WORKBENCH_SEARCH_KINDS.notebook, item.path]
    .filter(Boolean)
    .join(" · ");
  return `
    <li role="presentation">
      <a
        id="home-notebook-result-${index}"
        class="home-notebook-search-result${index === activeIndex ? " is-active" : ""}"
        href="${escapeHtml(item.targetUrl)}"
        role="option"
        aria-selected="${index === activeIndex ? "true" : "false"}"
        data-home-notebook-result-index="${index}"
        ${item.kind === "notebook" ? `data-open-recent-notebook="${escapeHtml(item.id)}"` : ""}
      >
        <span class="home-notebook-search-result-title">${escapeHtml(item.title)}</span>
        <span class="home-notebook-search-result-meta">${escapeHtml(meta)}</span>
      </a>
    </li>`;
}

export function initializeHomeNotebookSearch(root = document) {
  const page = root.matches?.("[data-home-page]") ? root : root.querySelector?.("[data-home-page]");
  if (!(page instanceof HTMLElement)) {
    return;
  }

  const form = page.querySelector("[data-home-notebook-search-form]");
  const input = page.querySelector("[data-home-notebook-search-input]");
  const feedback = page.querySelector("[data-home-notebook-search-feedback]");
  const resultsRoot = page.querySelector("[data-home-notebook-search-results]");
  const allResultsLink = page.querySelector("[data-home-notebook-search-all]");
  if (
    !(form instanceof HTMLFormElement) ||
    !(input instanceof HTMLInputElement) ||
    !(feedback instanceof HTMLElement) ||
    !(resultsRoot instanceof HTMLElement) ||
    !(allResultsLink instanceof HTMLAnchorElement) ||
    form.dataset.bound === "true"
  ) {
    return;
  }
  setHeroTheme(page);
  form.dataset.bound = "true";

  let items = primeWorkbenchSearchIndex(browserLocalNotebookIndex());
  let results = [];
  let activeIndex = -1;
  let loaded = false;

  function setExpanded(expanded) {
    form.classList.toggle("is-expanded", expanded);
    input.setAttribute("aria-expanded", String(expanded));
  }

  function syncActiveResultState() {
    resultsRoot.querySelectorAll("[data-home-notebook-result-index]").forEach((link) => {
      const selected = Number(link.dataset.homeNotebookResultIndex) === activeIndex;
      link.classList.toggle("is-active", selected);
      link.setAttribute("aria-selected", String(selected));
    });
    if (activeIndex >= 0) {
      input.setAttribute("aria-activedescendant", `home-notebook-result-${activeIndex}`);
    } else {
      input.removeAttribute("aria-activedescendant");
    }
  }

  function syncAllResultsLink(query, totalCount, visibleCount) {
    const showLink = totalCount > visibleCount;
    allResultsLink.hidden = !showLink;
    if (!showLink) {
      allResultsLink.href = "/search";
      allResultsLink.textContent = "Alle Ergebnisse in der Expertensuche anzeigen";
      return;
    }
    allResultsLink.href = `/search?q=${encodeURIComponent(query)}`;
    allResultsLink.textContent = `Alle ${totalCount} Ergebnisse in der Expertensuche anzeigen`;
  }

  function render() {
    const query = input.value.trim();
    const preview = searchWorkbenchPreview(items, query, WORKBENCH_LIVE_RESULT_LIMIT);
    results = preview.items;
    if (!query) {
      activeIndex = -1;
      resultsRoot.replaceChildren();
      syncAllResultsLink("", 0, 0);
      feedback.textContent = loaded
        ? "Tipp: Suchen Sie nach Notebook, Data Source, Datenobjekt oder DAAIF-Datenprodukt."
        : "Suchindex wird geladen …";
      input.removeAttribute("aria-activedescendant");
      return;
    }
    if (!workbenchSearchIsReady(query)) {
      activeIndex = -1;
      resultsRoot.replaceChildren();
      syncAllResultsLink("", 0, 0);
      feedback.textContent = `Geben Sie mindestens ${WORKBENCH_SEARCH_MIN_LENGTH} Zeichen ein.`;
      input.removeAttribute("aria-activedescendant");
      return;
    }
    activeIndex = Math.min(activeIndex, results.length - 1);
    feedback.textContent = preview.totalCount
      ? `${preview.totalCount} Treffer gefunden.`
      : "Keine passenden Inhalte gefunden.";
    resultsRoot.innerHTML = results.map((item, index) => resultMarkup(item, index, activeIndex)).join("");
    syncAllResultsLink(query, preview.totalCount, results.length);
    syncActiveResultState();
  }

  loadWorkbenchSearchIndex()
    .then((searchItems) => {
      items = searchItems;
      loaded = true;
      render();
    })
    .catch((error) => {
      loaded = true;
      console.error("Failed to load workbench search index.", error);
      render();
    });

  form.addEventListener("focusin", () => {
    setExpanded(true);
  });
  form.addEventListener("focusout", (event) => {
    const nextTarget = event.relatedTarget;
    if (nextTarget instanceof Node && form.contains(nextTarget)) {
      return;
    }
    setExpanded(false);
  });
  input.addEventListener("input", () => {
    activeIndex = -1;
    render();
  });
  input.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      activeIndex = -1;
      input.value = "";
      render();
      input.blur();
      setExpanded(false);
      return;
    }
    if (!results.length || !["ArrowDown", "ArrowUp", "Enter"].includes(event.key)) {
      return;
    }
    if (event.key === "Enter" && activeIndex >= 0) {
      event.preventDefault();
      resultsRoot.querySelector(`[data-home-notebook-result-index="${activeIndex}"]`)?.click();
      return;
    }
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const offset = event.key === "ArrowDown" ? 1 : -1;
      activeIndex = (activeIndex + offset + results.length) % results.length;
      render();
      resultsRoot
        .querySelector(`[data-home-notebook-result-index="${activeIndex}"]`)
        ?.scrollIntoView({ block: "nearest" });
    }
  });
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const query = input.value.trim();
    if (workbenchSearchIsReady(query)) {
      window.location.assign(`/search?q=${encodeURIComponent(query)}`);
      return;
    }
    render();
  });
  resultsRoot.addEventListener("pointermove", (event) => {
    const link = event.target.closest?.("[data-home-notebook-result-index]");
    if (!link) {
      return;
    }
    const index = Number(link.dataset.homeNotebookResultIndex);
    if (Number.isInteger(index) && index !== activeIndex) {
      activeIndex = index;
      syncActiveResultState();
    }
  });
  render();
}

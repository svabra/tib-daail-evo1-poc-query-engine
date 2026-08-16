import { normalizeNotebookSearchText } from "./home-notebook-search.js";

const expertSearchFieldCache = new WeakMap();
const EXPERT_SEARCH_KINDS = new Set(["all", "notebook", "source", "object", "product"]);

export function expertSearchKindFromParams(params) {
  const value = params instanceof URLSearchParams
    ? params.get("kind")
    : new URLSearchParams(String(params || "")).get("kind");
  return EXPERT_SEARCH_KINDS.has(value) ? value : "all";
}

function kindMatches(item, kind) {
  return (
    kind === "all" ||
    item?.kind === kind ||
    (kind === "notebook" && !item?.kind)
  );
}

function searchableFields(item) {
  if (!item || typeof item !== "object") {
    return null;
  }
  const cached = expertSearchFieldCache.get(item);
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
  expertSearchFieldCache.set(item, fields);
  return fields;
}

function scoreItem(item, terms) {
  const fields = searchableFields(item);
  if (!fields || !terms.every((term) => fields.searchable.includes(term))) {
    return null;
  }
  if (!terms.length) {
    return 0;
  }
  const phrase = terms.join(" ");
  return (
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
    )
  );
}

export function searchExpertWorkbenchIndex(items, query, kind = "all") {
  const terms = normalizeNotebookSearchText(query).split(/\s+/).filter(Boolean);
  return (Array.isArray(items) ? items : [])
    .filter((item) => kindMatches(item, kind))
    .map((item) => ({ item, score: scoreItem(item, terms) }))
    .filter((entry) => entry.score !== null)
    .sort(
      (left, right) =>
        right.score - left.score ||
        String(left.item?.title || "").localeCompare(
          String(right.item?.title || ""),
          "de-CH",
          { sensitivity: "base" }
        )
    )
    .map((entry) => entry.item);
}

function normalizeActivityAction(action) {
  const normalized = String(action || "").trim().toLowerCase();
  if (normalized === "edited") {
    return "edit";
  }
  return ["open", "edit", "run"].includes(normalized) ? normalized : "open";
}

function localActionCopy(action) {
  switch (normalizeActivityAction(action)) {
    case "run":
      return "Run";
    case "edit":
      return "Edit";
    default:
      return "Open";
  }
}

function sharedActionCopy(action) {
  switch (normalizeActivityAction(action)) {
    case "run":
      return "Run by another browser";
    case "edit":
      return "Edited by another browser";
    default:
      return "Opened by another browser";
  }
}

export function createQueryWorkbenchEntryController(helpers) {
  const {
    escapeHtml,
    fetchJsonOrThrow,
    formatRelativeTimestamp,
    notebookLinks,
    readNotebookActivity,
    workbenchClientId,
  } = helpers;

  function pageRoot(root = document) {
    return root?.querySelector?.("[data-query-workbench-entry-page]") ?? null;
  }

  function myLatestRoot(root = document) {
    return root?.querySelector?.("[data-query-entry-my-notebooks]") ?? null;
  }

  function sharedActivityRoot(root = document) {
    return root?.querySelector?.("[data-query-entry-shared-activity]") ?? null;
  }

  function recentNotebookActivityEntries(limit) {
    return Object.values(readNotebookActivity())
      .filter((entry) => entry && typeof entry === "object")
      .map((entry) => ({
        notebookId: String(entry.notebookId || "").trim(),
        title: String(entry.title || "").trim(),
        summary: String(entry.summary || "").trim(),
        touchedAt: String(entry.touchedAt || "").trim(),
        action: normalizeActivityAction(entry.reason || entry.action),
      }))
      .filter((entry) => entry.notebookId && notebookLinks(entry.notebookId).length)
      .sort((left, right) => Date.parse(right.touchedAt || "") - Date.parse(left.touchedAt || ""))
      .slice(0, limit);
  }

  function localActivityMarkup(entry) {
    return `
      <button
        type="button"
        class="query-entry-activity-item"
        data-open-recent-notebook="${escapeHtml(entry.notebookId)}"
      >
        <span class="query-entry-activity-title-row">
          <span class="query-entry-activity-title">${escapeHtml(entry.title || "Notebook")}</span>
          <span class="query-entry-activity-meta">${escapeHtml(formatRelativeTimestamp(entry.touchedAt))}</span>
        </span>
        <span class="query-entry-activity-copy">${escapeHtml(entry.summary || "No description saved.")}</span>
        <span class="query-entry-activity-meta">${escapeHtml(localActionCopy(entry.action))}</span>
      </button>
    `;
  }

  function sharedActivityMarkup(entry) {
    return `
      <button
        type="button"
        class="query-entry-activity-item"
        data-open-recent-notebook="${escapeHtml(entry.notebookId)}"
      >
        <span class="query-entry-activity-title-row">
          <span class="query-entry-activity-title">${escapeHtml(entry.title || "Notebook")}</span>
          <span class="query-entry-activity-meta">${escapeHtml(formatRelativeTimestamp(entry.touchedAt))}</span>
        </span>
        <span class="query-entry-activity-copy">${escapeHtml(entry.summary || "No description saved.")}</span>
        <span class="query-entry-activity-meta">${escapeHtml(sharedActionCopy(entry.action))}</span>
      </button>
    `;
  }

  function renderEmpty(root, copy) {
    if (root) {
      root.innerHTML = `<p class="home-empty">${escapeHtml(copy)}</p>`;
    }
  }

  function renderMyLatest(root = document) {
    const target = myLatestRoot(root);
    if (!target) {
      return;
    }

    const entries = recentNotebookActivityEntries(5);
    if (!entries.length) {
      renderEmpty(target, "No recent notebook activity yet.");
      return;
    }
    target.innerHTML = entries.map((entry) => localActivityMarkup(entry)).join("");
  }

  async function renderSharedActivity(root = document) {
    const target = sharedActivityRoot(root);
    if (!target) {
      return;
    }

    target.innerHTML = '<p class="home-empty">Loading shared activity...</p>';
    try {
      const payload = await fetchJsonOrThrow("/api/notebook-activity/recent?limit=5", {
        headers: {
          Accept: "application/json",
          "X-Workbench-Client-Id": workbenchClientId(),
        },
      });
      if (!payload?.available) {
        renderEmpty(target, payload?.message || "Shared activity is unavailable right now.");
        return;
      }
      const entries = Array.isArray(payload.activities) ? payload.activities : [];
      if (!entries.length) {
        renderEmpty(target, "No shared notebook activity from other browsers yet.");
        return;
      }
      target.innerHTML = entries.map((entry) => sharedActivityMarkup(entry)).join("");
    } catch (error) {
      console.error("Failed to load shared notebook activity.", error);
      renderEmpty(target, "Shared activity is unavailable right now.");
    }
  }

  async function initializeCurrentPage(root = document) {
    if (!pageRoot(root)) {
      return;
    }
    renderMyLatest(root);
    await renderSharedActivity(root);
  }

  return {
    initializeCurrentPage,
    renderMyLatest,
    renderSharedActivity,
  };
}

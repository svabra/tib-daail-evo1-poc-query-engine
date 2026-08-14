const INGESTION_FOCUS_SELECTORS = Object.freeze([
  "[data-ingestion-search-input]",
  "[data-csv-file-input]",
  "[data-csv-delimiter-mode]",
  "[data-csv-has-header]",
  "[data-csv-replace-existing]",
  "[data-csv-target-option]",
  "[data-csv-folder-path]",
  "[data-csv-s3-bucket]",
  "[data-csv-s3-prefix]",
  "[data-csv-s3-storage-format]",
  "[data-csv-parquet-optimization-mode]",
  "[data-csv-hive-partitioning]",
  "[data-csv-create-duckdb-cache]",
  "[data-csv-schema-name]",
  "[data-csv-table-prefix]",
  "[data-csv-import-base-name]",
  "[data-file-input]",
  "[data-file-target-option]",
  "[data-file-folder-path]",
  "[data-file-s3-bucket]",
  "[data-file-s3-prefix]",
  "[data-file-schema-name]",
  "[data-file-table-prefix]",
]);
let scrollRestoreFrame = null;

function cloneFileList(files) {
  return Array.from(files || []).filter((file) => file instanceof File);
}

function selectorForElement(element, root) {
  if (!(element instanceof Element) || !root?.contains(element)) {
    return "";
  }
  for (const selector of INGESTION_FOCUS_SELECTORS) {
    if (!element.matches(selector)) {
      continue;
    }
    if (element.hasAttribute("data-csv-import-base-name")) {
      const fileId = String(element.dataset.csvFileId || "").trim();
      return fileId
        ? `${selector}[data-csv-file-id="${CSS.escape(fileId)}"]`
        : selector;
    }
    const panel = element.closest("[data-ingestion-entry-panel]");
    const entryId = String(panel?.dataset?.ingestionEntryPanel || "").trim();
    return entryId
      ? `[data-ingestion-entry-panel="${CSS.escape(entryId)}"] ${selector}`
      : selector;
  }
  return "";
}

function captureNamedControls(root) {
  return Array.from(root.querySelectorAll("input, select, textarea"))
    .filter((control) => INGESTION_FOCUS_SELECTORS.some((selector) => control.matches(selector)))
    .filter((control) => !control.disabled && control.type !== "file")
    .map((control) => ({
      name: control.name,
      type: control.type || control.tagName.toLowerCase(),
      value: control.value,
      checked: "checked" in control ? Boolean(control.checked) : null,
      selector: selectorForElement(control, root),
    }));
}

function restoreNamedControls(root, controls) {
  (Array.isArray(controls) ? controls : []).forEach((state) => {
    const candidates = state?.selector
      ? Array.from(root.querySelectorAll(state.selector))
      : Array.from(root.querySelectorAll(`[name="${CSS.escape(String(state?.name || ""))}"]`));
    const control = candidates.find((candidate) => {
      if (state?.type === "radio") {
        return candidate.value === state.value;
      }
      return candidate.type === state?.type || candidates.length === 1;
    });
    if (!(control instanceof HTMLInputElement || control instanceof HTMLSelectElement || control instanceof HTMLTextAreaElement)) {
      return;
    }
    if (control instanceof HTMLInputElement && ["checkbox", "radio"].includes(control.type)) {
      control.checked = Boolean(state.checked);
    } else {
      control.value = String(state?.value || "");
    }
  });
}

export function captureIngestionWorkbenchNavigationState(root = document.querySelector("[data-ingestion-workbench-page]")) {
  if (!(root instanceof Element)) {
    return null;
  }
  const searchInput = root.querySelector("[data-ingestion-search-input]");
  const activePanel = Array.from(root.querySelectorAll("[data-ingestion-entry-panel]"))
    .find((panel) => !panel.hidden);
  const filesByEntry = {};
  root.querySelectorAll("[data-csv-file-input], [data-file-input]").forEach((input) => {
    const entryId = String(input.closest("[data-ingestion-entry-panel]")?.dataset?.ingestionEntryPanel || "").trim();
    if (entryId) {
      filesByEntry[entryId] = cloneFileList(input.files);
    }
  });
  return {
    activeEntryId: String(activePanel?.dataset?.ingestionEntryPanel || "").trim(),
    searchTerm: String(searchInput?.value || ""),
    controls: captureNamedControls(root),
    filesByEntry,
    focusSelector: selectorForElement(document.activeElement, root),
    scrollX: window.scrollX,
    scrollY: window.scrollY,
    focusOffsetY: document.activeElement instanceof HTMLElement
      ? document.activeElement.getBoundingClientRect().top
      : null,
  };
}

function assignFiles(input, files) {
  const selected = Array.isArray(files) ? files : [];
  if (!(input instanceof HTMLInputElement) || input.type !== "file" || !selected.length) {
    return;
  }
  const transfer = new DataTransfer();
  selected.forEach((file) => transfer.items.add(file));
  input.files = transfer.files;
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

export function restoreIngestionWorkbenchNavigationState(
  state,
  root = document.querySelector("[data-ingestion-workbench-page]")
) {
  if (!state || !(root instanceof Element)) {
    return false;
  }
  restoreNamedControls(root, state.controls);
  const searchInput = root.querySelector("[data-ingestion-search-input]");
  if (searchInput instanceof HTMLInputElement) {
    searchInput.value = String(state.searchTerm || "");
    searchInput.dispatchEvent(new Event("input", { bubbles: true }));
  }
  const activeEntryId = String(state.activeEntryId || "").trim();
  if (activeEntryId) {
    const tile = root.querySelector(`[data-ingestion-tile="${CSS.escape(activeEntryId)}"]`);
    if (tile instanceof HTMLElement) {
      tile.click();
    }
  }
  restoreNamedControls(root, state.controls);
  Object.entries(state.filesByEntry || {}).forEach(([entryId, files]) => {
    const input = root.querySelector(
      `[data-ingestion-entry-panel="${CSS.escape(entryId)}"] [data-csv-file-input], ` +
      `[data-ingestion-entry-panel="${CSS.escape(entryId)}"] [data-file-input]`
    );
    assignFiles(input, files);
  });
  restoreNamedControls(root, state.controls);
  const focusTarget = state.focusSelector ? root.querySelector(state.focusSelector) : null;
  if (focusTarget instanceof HTMLElement) {
    focusTarget.focus({ preventScroll: true });
  }
  const scrollX = Number(state.scrollX || 0);
  const capturedScrollY = Number(state.scrollY || 0);
  const restoreScrollPosition = () => {
    window.scrollTo(scrollX, capturedScrollY);
  };
  window.scrollTo(scrollX, capturedScrollY);
  restoreScrollPosition();
  if (scrollRestoreFrame !== null) {
    window.cancelAnimationFrame(scrollRestoreFrame);
  }
  scrollRestoreFrame = window.requestAnimationFrame(() => {
    scrollRestoreFrame = null;
    const delayedFocusTarget = state.focusSelector ? root.querySelector(state.focusSelector) : null;
    if (delayedFocusTarget instanceof HTMLElement) {
      delayedFocusTarget.focus({ preventScroll: true });
    }
    restoreScrollPosition();
  });
  return true;
}

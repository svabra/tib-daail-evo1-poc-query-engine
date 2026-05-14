const liveValidationDelayMs = 700;
const terminalQueryStatusDisplayMs = 3500;

const runTooltips = {
  unchecked: "Run this SQL cell. Sources will be checked before execution.",
  checking: "Checking whether referenced sources exist before this cell can run.",
  starting: "Sources checked. Starting query...",
  completed: "Run this SQL cell again. The last query completed.",
  failed: "Run this SQL cell again. The last query failed.",
  cancelled: "Run this SQL cell again. The last query was cancelled.",
  valid: "Run this SQL cell. Referenced sources were found.",
};

const explainTooltips = {
  unchecked: "Explain this SQL cell without running it. Sources will be checked first.",
  checking: "Checking whether referenced sources exist before this cell can be explained.",
  starting: "Sources checked. Preparing explain request...",
  completed: "Explain this SQL cell again. The last query completed.",
  failed: "Explain this SQL cell. The last query failed.",
  cancelled: "Explain this SQL cell. The last query was cancelled.",
  valid: "Explain this SQL cell without running it.",
  native: "Explain is available for DuckDB-backed SQL cells only.",
  python: "Explain is available for SQL cells only.",
};

function normalizedStatus(status) {
  const value = String(status || "").trim().toLowerCase();
  return [
    "valid",
    "invalid",
    "unchecked",
    "checking",
    "starting",
    "completed",
    "failed",
    "cancelled",
  ].includes(value)
    ? value
    : "unchecked";
}

function sourceReferencesNeedValidation(sql) {
  return /\b(from|join|table)\b/i.test(String(sql || ""));
}

function validationMessageFor(result, runtimePhase = "") {
  const status = normalizedStatus(result?.status || runtimePhase);
  if (runtimePhase === "checking") {
    return "Checking source existence before running...";
  }
  if (runtimePhase === "starting") {
    return "Sources checked. Starting query...";
  }
  if (runtimePhase === "completed" || status === "completed") {
    return "Query completed.";
  }
  if (runtimePhase === "failed" || status === "failed") {
    return "Query failed.";
  }
  if (runtimePhase === "cancelled" || status === "cancelled") {
    return "Query cancelled.";
  }
  if (status === "checking") {
    return "Checking source existence...";
  }
  if (status === "valid") {
    return "Sources checked: all referenced sources exist.";
  }
  if (status === "invalid") {
    const missing = Array.isArray(result?.missingReferences)
      ? result.missingReferences.filter(Boolean).join(", ")
      : "";
    return missing
      ? `Missing sources: ${missing}. Run Cell is blocked.`
      : "Source existence check failed. Run Cell is blocked.";
  }
  return "No source references found. Sources will be checked before execution.";
}

function runTooltipFor(result, runtimePhase = "") {
  const status = normalizedStatus(runtimePhase || result?.status);
  if (status === "checking" || runtimePhase === "checking") {
    return runTooltips.checking;
  }
  if (status === "starting") {
    return runTooltips.starting;
  }
  if (status === "completed") {
    return runTooltips.completed;
  }
  if (status === "failed") {
    return runTooltips.failed;
  }
  if (status === "cancelled") {
    return runTooltips.cancelled;
  }
  if (status === "valid") {
    return runTooltips.valid;
  }
  if (status === "invalid") {
    const missing = Array.isArray(result?.missingReferences)
      ? result.missingReferences.filter(Boolean).join(", ")
      : "";
    return missing
      ? `Run Cell is disabled because these sources were not found: ${missing}`
      : "Run Cell is disabled because source existence validation failed.";
  }
  return runTooltips.unchecked;
}

function explainTooltipFor(result, runtimePhase = "") {
  const status = normalizedStatus(runtimePhase || result?.status);
  if (status === "checking" || runtimePhase === "checking") {
    return explainTooltips.checking;
  }
  if (status === "starting") {
    return explainTooltips.starting;
  }
  if (status === "completed") {
    return explainTooltips.completed;
  }
  if (status === "failed") {
    return explainTooltips.failed;
  }
  if (status === "cancelled") {
    return explainTooltips.cancelled;
  }
  if (status === "valid") {
    return explainTooltips.valid;
  }
  if (status === "invalid") {
    const missing = Array.isArray(result?.missingReferences)
      ? result.missingReferences.filter(Boolean).join(", ")
      : "";
    return missing
      ? `Explain is disabled because these sources were not found: ${missing}`
      : "Explain is disabled because source existence validation failed.";
  }
  return explainTooltips.unchecked;
}

function normalizeValidationPayload(payload) {
  const status = normalizedStatus(payload?.status);
  return {
    status,
    canRun: payload?.canRun !== false && status !== "invalid",
    references: Array.isArray(payload?.references) ? payload.references.filter(Boolean) : [],
    matchedReferences: Array.isArray(payload?.matchedReferences) ? payload.matchedReferences : [],
    missingReferences: Array.isArray(payload?.missingReferences) ? payload.missingReferences.filter(Boolean) : [],
    message: String(payload?.message || "").trim(),
  };
}

function uniqueStrings(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean)
    )
  );
}

function normalizeLocalValidationPayload(payload, sql) {
  return {
    aliases: uniqueStrings(payload?.aliases),
    localRelations:
      payload?.localRelations && typeof payload.localRelations === "object"
        ? payload.localRelations
        : {},
    missingAliases: uniqueStrings(payload?.missingAliases),
    validationSql: String(payload?.validationSql ?? sql ?? ""),
  };
}

function mergeLocalAliasValidation(result, localValidation) {
  const aliases = uniqueStrings(localValidation?.aliases);
  if (!aliases.length) {
    return result;
  }

  const matchedReferences = Array.isArray(result?.matchedReferences)
    ? [...result.matchedReferences]
    : [];
  const matchedKeys = new Set(
    matchedReferences.map((reference) => String(reference?.reference || "").trim().toLowerCase())
  );
  aliases.forEach((alias) => {
    if (matchedKeys.has(alias.toLowerCase())) {
      return;
    }
    matchedReferences.push({
      reference: alias,
      matchedRelation: String(localValidation?.localRelations?.[alias] || alias),
    });
    matchedKeys.add(alias.toLowerCase());
  });

  return normalizeValidationPayload({
    ...result,
    status: result.status === "unchecked" ? "valid" : result.status,
    references: uniqueStrings([...(result.references || []), ...aliases]),
    matchedReferences,
    message:
      result.status === "unchecked"
        ? "All referenced sources exist."
        : result.message,
  });
}

export function createQuerySourceValidationController(helpers) {
  const {
    cellLanguageForCellRoot,
    fetchImpl = (...args) => window.fetch(...args),
    selectedDataSourcesForCell,
    validateLocalWorkspaceAliases = async (sql) => ({
      aliases: [],
      localRelations: {},
      missingAliases: [],
      validationSql: String(sql || ""),
    }),
  } = helpers;

  const cellStates = new WeakMap();

  function stateForCell(cellRoot) {
    let state = cellStates.get(cellRoot);
    if (!state) {
      state = {
        abortController: null,
        lastTerminalQueryKey: "",
        requestId: 0,
        result: null,
        sql: "",
        terminalTimer: null,
        timer: null,
      };
      cellStates.set(cellRoot, state);
    }
    return state;
  }

  function currentSqlForCell(cellRoot) {
    return cellRoot?.querySelector?.("[data-editor-source]")?.value ?? "";
  }

  function cellUsesNativePostgres(cellRoot) {
    return selectedDataSourcesForCell(cellRoot).some((sourceId) =>
      String(sourceId || "").trim().toLowerCase().endsWith("_native")
    );
  }

  function validationRootForCell(cellRoot) {
    if (!(cellRoot instanceof Element)) {
      return null;
    }
    let root = cellRoot.querySelector("[data-query-source-validation]");
    if (root) {
      return root;
    }

    const editorRoot = cellRoot.querySelector("[data-editor-root]");
    if (!editorRoot) {
      return null;
    }
    root = document.createElement("div");
    root.className = "query-source-validation";
    root.dataset.querySourceValidation = "";
    root.dataset.querySourceValidationStatus = "unchecked";
    root.setAttribute("aria-live", "polite");
    root.innerHTML = '<span data-query-source-validation-message></span>';
    editorRoot.insertAdjacentElement("afterend", root);
    return root;
  }

  function setRunButtonState(cellRoot, result, runtimePhase = "") {
    const runButton = cellRoot?.querySelector?.("[data-run-cell]");
    const explainButton = cellRoot?.querySelector?.("[data-explain-cell]");
    const isRunning =
      cellRoot?.classList?.contains?.("is-query-running") ||
      runButton?.classList?.contains?.("is-running");
    const status = normalizedStatus(runtimePhase || result?.status);
    const disabled = status === "invalid" || status === "checking" || status === "starting";

    if (explainButton) {
      const isSqlCell = cellLanguageForCellRoot(cellRoot) === "sql";
      const nativePostgres = isSqlCell && cellUsesNativePostgres(cellRoot);
      explainButton.hidden = !isSqlCell;
      explainButton.disabled = !isSqlCell || nativePostgres || disabled || Boolean(isRunning);
      explainButton.title = !isSqlCell
        ? explainTooltips.python
        : nativePostgres
          ? explainTooltips.native
          : explainTooltipFor(result, runtimePhase);
    }

    if (
      !runButton ||
      cellRoot.classList.contains("is-query-running") ||
      runButton.classList.contains("is-running")
    ) {
      return;
    }

    runButton.disabled = disabled;
    runButton.title = runTooltipFor(result, runtimePhase);
    if (!runButton.classList.contains("is-running")) {
      runButton.textContent = "Run Cell";
    }
  }

  function renderCellState(cellRoot, result = null, runtimePhase = "") {
    if (!(cellRoot instanceof Element)) {
      return;
    }

    const root = validationRootForCell(cellRoot);
    const isSqlCell = cellLanguageForCellRoot(cellRoot) === "sql";
    if (!isSqlCell) {
      if (root) {
        root.hidden = true;
      }
      const runButton = cellRoot.querySelector("[data-run-cell]");
      if (runButton && !runButton.classList.contains("is-running")) {
        runButton.title = "Run this Python cell.";
      }
      const explainButton = cellRoot.querySelector("[data-explain-cell]");
      if (explainButton) {
        explainButton.hidden = true;
        explainButton.disabled = true;
        explainButton.title = explainTooltips.python;
      }
      return;
    }

    const status = normalizedStatus(runtimePhase || result?.status);
    const message = validationMessageFor(result, runtimePhase);
    if (root) {
      root.hidden = false;
      root.dataset.querySourceValidationStatus = status;
      root.classList.toggle("is-checking", status === "checking");
      root.classList.toggle("is-valid", status === "valid" || status === "completed");
      root.classList.toggle("is-invalid", status === "invalid" || status === "failed");
      root.classList.toggle("is-starting", status === "starting");
      root.classList.toggle("is-completed", status === "completed");
      root.classList.toggle("is-failed", status === "failed");
      root.classList.toggle("is-cancelled", status === "cancelled");
      root.classList.toggle("is-unchecked", status === "unchecked");
      const messageRoot = root.querySelector("[data-query-source-validation-message]");
      if (messageRoot) {
        messageRoot.textContent = message;
      }
    }
    setRunButtonState(cellRoot, result || { status }, runtimePhase);
  }

  function abortPendingRequest(state) {
    if (state.abortController) {
      state.abortController.abort();
      state.abortController = null;
    }
  }

  function clearScheduledValidation(state) {
    if (state.timer !== null) {
      window.clearTimeout(state.timer);
      state.timer = null;
    }
  }

  function clearTerminalTimer(state) {
    if (state.terminalTimer !== null) {
      window.clearTimeout(state.terminalTimer);
      state.terminalTimer = null;
    }
  }

  async function requestValidation(cellRoot, sql, { runtime = false } = {}) {
    const state = stateForCell(cellRoot);
    clearScheduledValidation(state);
    abortPendingRequest(state);
    clearTerminalTimer(state);

    const requestId = state.requestId + 1;
    state.requestId = requestId;
    state.sql = sql;
    state.result = { status: "checking" };
    renderCellState(cellRoot, state.result, runtime ? "checking" : "");

    const abortController = new AbortController();
    state.abortController = abortController;
    try {
      const localValidation = normalizeLocalValidationPayload(
        await validateLocalWorkspaceAliases(sql),
        sql
      );
      if (abortController.signal.aborted || state.requestId !== requestId) {
        return null;
      }

      if (localValidation.missingAliases.length) {
        const result = normalizeValidationPayload({
          status: "invalid",
          references: localValidation.aliases,
          missingReferences: localValidation.missingAliases,
          message: `Referenced source(s) were not found: ${localValidation.missingAliases.join(", ")}.`,
        });
        state.abortController = null;
        state.result = result;
        renderCellState(cellRoot, result);
        return result;
      }

      const response = await fetchImpl("/api/query-sources/validate", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          sql,
          dataSources: selectedDataSourcesForCell(cellRoot),
          localRelations: localValidation.localRelations,
        }),
        signal: abortController.signal,
      });
      if (!response.ok) {
        throw new Error(`Source validation failed with HTTP ${response.status}.`);
      }

      const result = mergeLocalAliasValidation(
        normalizeValidationPayload(await response.json()),
        localValidation
      );
      if (state.requestId !== requestId) {
        return null;
      }

      state.abortController = null;
      state.result = result;
      renderCellState(cellRoot, result, runtime && result.status !== "invalid" ? "starting" : "");
      return result;
    } catch (error) {
      if (abortController.signal.aborted) {
        return null;
      }
      const result = normalizeValidationPayload({
        status: "invalid",
        missingReferences: [],
        message:
          error instanceof Error
            ? error.message
            : "Source existence validation failed.",
      });
      if (state.requestId === requestId) {
        state.abortController = null;
        state.result = result;
        renderCellState(cellRoot, result);
      }
      return result;
    }
  }

  function scheduleValidationForCell(cellRoot, sql = currentSqlForCell(cellRoot)) {
    if (!(cellRoot instanceof Element)) {
      return;
    }

    const state = stateForCell(cellRoot);
    clearScheduledValidation(state);
    abortPendingRequest(state);
    clearTerminalTimer(state);
    state.sql = sql;

    if (cellLanguageForCellRoot(cellRoot) !== "sql") {
      renderCellState(cellRoot, { status: "unchecked" });
      return;
    }

    if (!sourceReferencesNeedValidation(sql)) {
      const result = normalizeValidationPayload({ status: "unchecked" });
      state.result = result;
      renderCellState(cellRoot, result);
      return;
    }

    state.result = { status: "checking" };
    renderCellState(cellRoot, state.result);
    state.timer = window.setTimeout(() => {
      state.timer = null;
      requestValidation(cellRoot, sql).catch((error) => {
        console.error("Failed to validate query sources.", error);
      });
    }, liveValidationDelayMs);
  }

  async function validateBeforeRun(cellRoot, sql = currentSqlForCell(cellRoot)) {
    if (!(cellRoot instanceof Element) || cellLanguageForCellRoot(cellRoot) !== "sql") {
      return normalizeValidationPayload({ status: "unchecked" });
    }

    if (!sourceReferencesNeedValidation(sql)) {
      const result = normalizeValidationPayload({ status: "unchecked" });
      const state = stateForCell(cellRoot);
      clearScheduledValidation(state);
      abortPendingRequest(state);
      state.result = result;
      renderCellState(cellRoot, result, "starting");
      return result;
    }

    return requestValidation(cellRoot, sql, { runtime: true });
  }

  async function validateBeforeExplain(cellRoot, sql = currentSqlForCell(cellRoot)) {
    if (!(cellRoot instanceof Element) || cellLanguageForCellRoot(cellRoot) !== "sql") {
      return normalizeValidationPayload({ status: "unchecked" });
    }

    if (!sourceReferencesNeedValidation(sql)) {
      const result = normalizeValidationPayload({ status: "unchecked" });
      const state = stateForCell(cellRoot);
      clearScheduledValidation(state);
      abortPendingRequest(state);
      state.result = result;
      renderCellState(cellRoot, result);
      return result;
    }

    return requestValidation(cellRoot, sql);
  }

  function handleEditorChanged(editorRoot) {
    const cellRoot = editorRoot?.closest?.("[data-query-cell]");
    if (cellRoot) {
      scheduleValidationForCell(cellRoot);
    }
  }

  function handleTextareaInput(textarea) {
    const cellRoot = textarea?.closest?.("[data-query-cell]");
    if (cellRoot) {
      scheduleValidationForCell(cellRoot, textarea.value ?? "");
    }
  }

  function refreshCell(cellRoot) {
    if (!(cellRoot instanceof Element)) {
      return;
    }
    const state = stateForCell(cellRoot);
    if (state.result) {
      renderCellState(cellRoot, state.result);
      return;
    }
    scheduleValidationForCell(cellRoot);
  }

  function refreshAll(root = document) {
    root.querySelectorAll("[data-query-cell]").forEach((cellRoot) => {
      refreshCell(cellRoot);
    });
  }

  function syncQueryJobState(cellRoot, job = null) {
    if (!(cellRoot instanceof Element) || cellLanguageForCellRoot(cellRoot) !== "sql") {
      return;
    }

    const status = normalizedStatus(job?.status);
    if (!["completed", "failed", "cancelled"].includes(status)) {
      return;
    }

    const state = stateForCell(cellRoot);
    const terminalKey = [
      String(job?.jobId || ""),
      status,
      String(job?.completedAt || job?.updatedAt || ""),
    ].join(":");
    if (state.lastTerminalQueryKey === terminalKey) {
      return;
    }

    state.lastTerminalQueryKey = terminalKey;
    clearScheduledValidation(state);
    abortPendingRequest(state);
    clearTerminalTimer(state);

    const baseResult = state.result || normalizeValidationPayload({ status: "unchecked" });
    renderCellState(cellRoot, baseResult, status);
    state.terminalTimer = window.setTimeout(() => {
      if (state.lastTerminalQueryKey !== terminalKey) {
        return;
      }
      state.terminalTimer = null;
      scheduleValidationForCell(cellRoot, currentSqlForCell(cellRoot));
    }, terminalQueryStatusDisplayMs);
  }

  return {
    handleEditorChanged,
    handleTextareaInput,
    refreshAll,
    refreshCell,
    scheduleValidationForCell,
    syncQueryJobState,
    validateBeforeRun,
    validateBeforeExplain,
  };
}

import {
  renderSourceCatalog,
  storedSourceCatalogView,
  storeSourceCatalogView,
} from "./source-catalog.js";
import { normalizeS3BucketNameForCreate } from "./source-metadata-utils.js";

const API_ROOT = "/api/ingestion/source-ingestions";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function newClientRequestId(prefix = "source-ingestion") {
  const token = globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `${prefix}:${token}`;
}

async function requestJson(url, options = {}) {
  const response = await window.fetch(url, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });
  const raw = await response.text();
  let payload = {};
  if (raw) {
    try {
      payload = JSON.parse(raw);
    } catch (_error) {
      payload = { detail: raw };
    }
  }
  if (!response.ok) {
    throw new Error(String(payload.detail || payload.message || `Request failed: ${response.status}`));
  }
  return payload;
}

function showAlert(root, message = "", tone = "danger") {
  const alert = root?.querySelector("[data-source-ingestion-alert]");
  if (!alert) return;
  alert.hidden = !message;
  alert.dataset.tone = tone;
  alert.textContent = message;
}

function statusLabel(definition) {
  const state = String(definition?.state || "draft");
  if (state === "active") return "Hourly schedule active";
  if (state === "attention") return "Needs attention";
  if (state === "paused" && definition?.lastSuccessfulRunAt) return "One-Time";
  if (state === "paused") return "Paused";
  if (state === "draft") return "First run pending";
  return state.replaceAll("-", " ");
}

function formatTimestamp(value, fallback = "Not yet") {
  if (!value) return fallback;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return new Intl.DateTimeFormat("de-CH", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Europe/Zurich",
  }).format(parsed);
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const amount = bytes / 1024 ** index;
  return `${amount.toFixed(index ? 1 : 0)} ${units[index]}`;
}

function relationQuery(sourceId, relation) {
  const params = new URLSearchParams({
    sourceId,
    schema: String(relation.schema || ""),
    relation: String(relation.name || ""),
  });
  return `/ingestion-workbench/sourcing/ingestions/new?${params.toString()}`;
}

function renderHubSources(root, context) {
  const target = root.querySelector("[data-source-ingestion-sources]");
  const sources = Array.isArray(context.sources) ? context.sources : [];
  root.querySelector('[data-source-ingestion-summary="sources"]').textContent = String(sources.length);
  root.sourceIngestionContext = context;
  if (!sources.length) {
    target.innerHTML = '<div class="sourcing-empty"><strong>No ingestion-capable source is available</strong><p>Check the platform connections or request access to an Oracle source.</p><a href="/ingestion-workbench/sourcing/request">Request a new Data Source</a></div>';
    return;
  }
  const query = String(root.querySelector("[data-source-ingestion-catalog-query]")?.value || "").trim().toLocaleLowerCase();
  const normalized = sources.map((source) => ({
    id: source.id,
    name: source.displayName || source.id,
    databaseName: source.databaseName || source.id,
    technology: source.technology || source.platform,
    technologyKey: source.technologyKey,
    status: "available",
    statusLabel: source.sourceKind === "oracle-poc" ? "Grant active" : "Available",
    location: source.site || "BIT",
    schemaCount: new Set((source.relations || []).map((relation) => relation.schema)).size,
    objectCount: (source.relations || []).length,
    accessModel: source.accessModel,
    ingestionCapable: true,
    managementPath: `/data-sources?source_id=${encodeURIComponent(source.id)}`,
    browsePath: `/data-sources/browser?source_id=${encodeURIComponent(source.id)}`,
    summary: `${source.databaseName || source.id} · ${source.owner || "Data Platform BIT"}`,
  }));
  const filtered = normalized.filter((source) => !query || Object.values(source).join(" ").toLocaleLowerCase().includes(query));
  const viewMode = root.dataset.sourceIngestionCatalogView === "list" ? "list" : "table";
  renderSourceCatalog(target, filtered, { viewMode, compact: true });
  root.querySelector("[data-source-ingestion-catalog-count]").textContent = `${filtered.length} of ${normalized.length} sources shown`;
}

function latestRunForDefinition(definition, runs) {
  return runs.find((run) => run.definitionId === definition.id) || null;
}

function renderHubDefinitions(root, payload) {
  const target = root.querySelector("[data-source-ingestion-definitions]");
  const definitions = Array.isArray(payload.items) ? payload.items : [];
  const runs = Array.isArray(payload.runs) ? payload.runs : [];
  const summary = payload.summary || {};
  ["activeSchedules", "attention", "runsLast24Hours"].forEach((key) => {
    const node = root.querySelector(`[data-source-ingestion-summary="${key}"]`);
    if (node) node.textContent = String(Number(summary[key] || 0));
  });
  if (!definitions.length) {
    target.innerHTML = '<div class="sourcing-empty"><strong>No ingestion definitions yet</strong><p>Start with an available source relation. The same definition can run once or every hour.</p><a href="/ingestion-workbench/sourcing/ingestions/new">Create your first ingestion</a></div>';
    return;
  }
  target.innerHTML = definitions.map((definition) => {
    const run = latestRunForDefinition(definition, runs);
    const status = String(run?.status || definition.state || "draft");
    return `<a class="source-ingestion-definition-card" href="/ingestion-workbench/sourcing/ingestions/${encodeURIComponent(definition.id)}">
      <div><span class="source-ingestion-status" data-status="${escapeHtml(definition.state)}">${escapeHtml(statusLabel(definition))}</span><h4>${escapeHtml(definition.name)}</h4><p>${escapeHtml(definition.sourceId)}.${escapeHtml(definition.schemaName)}.${escapeHtml(definition.relationName)}</p><code>s3://${escapeHtml(definition.destinationBucket)}/${escapeHtml(definition.destinationKey)}</code></div>
      <dl><div><dt>Last result</dt><dd>${escapeHtml(status)}</dd></div><div><dt>Rows</dt><dd>${Number(run?.rowCount || 0).toLocaleString("de-CH")}</dd></div><div><dt>Next run</dt><dd>${escapeHtml(formatTimestamp(definition.nextRunAt, "One-Time"))}</dd></div></dl>
    </a>`;
  }).join("");
}

async function refreshHub(root) {
  try {
    const [context, definitions] = await Promise.all([
      requestJson(`${API_ROOT}/context`),
      requestJson(API_ROOT),
    ]);
    if (!root.isConnected) return;
    renderHubSources(root, context);
    renderHubDefinitions(root, definitions);
    showAlert(root);
  } catch (error) {
    showAlert(root, error.message);
  }
}

function optionForRelation(relation) {
  const option = document.createElement("option");
  option.value = `${relation.schema}.${relation.name}`;
  option.textContent = `${relation.schema}.${relation.displayName || relation.name} · ${relation.kind}`;
  option.dataset.schema = relation.schema;
  option.dataset.relation = relation.name;
  option.dataset.kind = relation.kind;
  return option;
}

function defaultObjectKey(sourceId, schema, relation, technologyKey = "source") {
  return `ingestions/${technologyKey}/${sourceId}/${schema.toLowerCase()}/${relation.toLowerCase()}.parquet`;
}

function selectedRelation(root) {
  const option = root.querySelector("[data-source-ingestion-relation]")?.selectedOptions?.[0];
  if (!option?.dataset.relation) return null;
  return { schema: option.dataset.schema, name: option.dataset.relation, kind: option.dataset.kind };
}

function setWizardStep(root, step) {
  root.dataset.currentStep = String(step);
  root.querySelectorAll("[data-source-ingestion-step]").forEach((panel) => {
    panel.hidden = Number(panel.dataset.sourceIngestionStep) !== step;
  });
  root.querySelectorAll("[data-source-ingestion-step-indicator]").forEach((indicator) => {
    const number = Number(indicator.dataset.sourceIngestionStepIndicator);
    indicator.classList.toggle("is-active", number === step);
    indicator.classList.toggle("is-complete", number < step);
    if (number === step) indicator.setAttribute("aria-current", "step");
    else indicator.removeAttribute("aria-current");
  });
  root.querySelector(`[data-source-ingestion-step="${step}"]`)?.focus?.({ preventScroll: true });
}

function updateWizardRelationOptions(root, context, preselectedRelation = "") {
  const sourceId = root.querySelector("[data-source-ingestion-source]").value;
  const source = (context.sources || []).find((item) => item.id === sourceId);
  const relationSelect = root.querySelector("[data-source-ingestion-relation]");
  relationSelect.replaceChildren();
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = source ? "Choose one table or view" : "Choose a source first";
  relationSelect.append(placeholder);
  (source?.relations || []).forEach((relation) => relationSelect.append(optionForRelation(relation)));
  relationSelect.disabled = !source;
  if (preselectedRelation) {
    const match = Array.from(relationSelect.options).find((option) => option.dataset.relation === preselectedRelation);
    if (match) relationSelect.value = match.value;
  }
  const grantNote = root.querySelector("[data-source-ingestion-grant-note]");
  grantNote.hidden = !source;
  if (source) grantNote.textContent = `${source.accessModel} · ${source.displayName} · ${source.site || "BIT"}`;
}

function updateWizardDestination(root, { force = false } = {}) {
  const sourceId = root.querySelector("[data-source-ingestion-source]").value;
  const source = (root.sourceIngestionContext?.sources || []).find((item) => item.id === sourceId);
  const relation = selectedRelation(root);
  const keyInput = root.querySelector("[data-source-ingestion-key]");
  if (relation && (force || !keyInput.value)) keyInput.value = defaultObjectKey(sourceId, relation.schema, relation.name, source?.technologyKey || "source");
  const bucket = root.querySelector("[data-source-ingestion-bucket]").value.trim();
  root.querySelector("[data-source-ingestion-path-preview]").textContent = bucket && keyInput.value ? `Stable target: s3://${bucket}/${keyInput.value.trim()}` : "";
}

function visibleWizardBuckets(root) {
  return new Set(
    (root.sourceIngestionContext?.visibleBuckets || [])
      .map((bucket) => String(bucket || "").trim())
      .filter(Boolean)
  );
}

function renderWizardBucketOptions(root) {
  const bucketOptions = root.querySelector("[data-source-ingestion-buckets]");
  if (!bucketOptions) return;
  bucketOptions.replaceChildren();
  Array.from(visibleWizardBuckets(root))
    .sort((left, right) => left.localeCompare(right))
    .forEach((bucket) => bucketOptions.append(new Option(bucket, bucket)));
}

function showWizardBucketStatus(root, message = "", tone = "info") {
  const status = root.querySelector("[data-source-ingestion-bucket-status]");
  if (!status) return;
  status.hidden = !message;
  status.dataset.tone = tone;
  status.textContent = message;
}

function syncWizardBucketCreation(root) {
  const input = root.querySelector("[data-source-ingestion-bucket]");
  const button = root.querySelector("[data-source-ingestion-create-bucket]");
  if (!input || !button) return;
  const bucket = input.value.trim();
  const exists = visibleWizardBuckets(root).has(bucket);
  let validCandidate = false;
  if (bucket && !exists) {
    try {
      normalizeS3BucketNameForCreate(bucket);
      validCandidate = true;
    } catch (_error) {
      validCandidate = false;
    }
  }
  button.disabled = root.dataset.sourceIngestionBucketCreating === "true" || !validCandidate;
  button.textContent = exists ? "Bucket available" : "Create bucket";
  if (exists && root.dataset.sourceIngestionBucketStatus !== "success") {
    showWizardBucketStatus(root);
  }
}

async function createWizardBucket(root) {
  const input = root.querySelector("[data-source-ingestion-bucket]");
  const button = root.querySelector("[data-source-ingestion-create-bucket]");
  if (!input || !button) return;
  let bucket;
  try {
    bucket = normalizeS3BucketNameForCreate(input.value);
  } catch (error) {
    showWizardBucketStatus(root, error.message, "danger");
    showAlert(root, error.message);
    input.focus();
    return;
  }
  root.dataset.sourceIngestionBucketCreating = "true";
  root.dataset.sourceIngestionBucketStatus = "pending";
  button.disabled = true;
  button.textContent = "Creating...";
  showWizardBucketStatus(root, `Creating s3://${bucket}/ ...`, "info");
  showAlert(root);
  try {
    const created = await requestJson("/api/s3/explorer/buckets", {
      method: "POST",
      body: JSON.stringify({ bucketName: bucket }),
    });
    const createdBucket = String(created.bucket || bucket).trim();
    const buckets = new Set(root.sourceIngestionContext?.visibleBuckets || []);
    buckets.add(createdBucket);
    root.sourceIngestionContext.visibleBuckets = Array.from(buckets).sort((left, right) => left.localeCompare(right));
    input.value = createdBucket;
    renderWizardBucketOptions(root);
    updateWizardDestination(root);
    root.dataset.sourceIngestionBucketStatus = "success";
    showWizardBucketStatus(root, `Bucket “${createdBucket}” is ready and selected.`, "success");
  } catch (error) {
    root.dataset.sourceIngestionBucketStatus = "danger";
    showWizardBucketStatus(root, error.message, "danger");
    showAlert(root, error.message);
  } finally {
    root.dataset.sourceIngestionBucketCreating = "false";
    syncWizardBucketCreation(root);
  }
}

function updateWizardMode(root) {
  const scheduled = root.querySelector('[data-source-ingestion-mode][value="scheduled"]')?.checked;
  root.querySelector("[data-source-ingestion-schedule-contract]").hidden = !scheduled;
  root.querySelector("[data-source-ingestion-submit]").textContent = scheduled ? "Run once & activate schedule" : "Run once";
}

function renderWizardReview(root) {
  const sourceSelect = root.querySelector("[data-source-ingestion-source]");
  const relation = selectedRelation(root);
  const scheduled = root.querySelector('[data-source-ingestion-mode][value="scheduled"]')?.checked;
  const bucket = root.querySelector("[data-source-ingestion-bucket]").value.trim();
  const key = root.querySelector("[data-source-ingestion-key]").value.trim();
  root.querySelector("[data-source-ingestion-review]").innerHTML = [
    ["Source", sourceSelect.selectedOptions[0]?.textContent || ""],
    ["Relation", relation ? `${relation.schema}.${relation.name}` : ""],
    ["Target", `s3://${bucket}/${key}`],
    ["Format", "Parquet"],
    ["Write mode", "Atomic Full Replace"],
    ["Run mode", scheduled ? "Every hour · minute 00 · Europe/Zurich" : "One-Time"],
  ].map(([label, value]) => `<div><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd></div>`).join("");
  const name = root.querySelector("[data-source-ingestion-name]");
  if (!name.value.trim() && relation) name.value = `${sourceSelect.selectedOptions[0]?.textContent || sourceSelect.value} · ${relation.schema}.${relation.name}`;
}

function validateWizardStep(root, step) {
  const fields = Array.from(root.querySelector(`[data-source-ingestion-step="${step}"]`)?.querySelectorAll("input, select, textarea") || []);
  for (const field of fields) {
    if (!field.checkValidity()) {
      field.reportValidity();
      return false;
    }
  }
  if (step === 1 && !selectedRelation(root)) {
    showAlert(root, "Choose one source table, view or object.");
    return false;
  }
  if (step === 2) {
    const bucketInput = root.querySelector("[data-source-ingestion-bucket]");
    const bucket = bucketInput?.value.trim() || "";
    if (!visibleWizardBuckets(root).has(bucket)) {
      showAlert(root, `Create the S3 bucket “${bucket}” before continuing.`);
      showWizardBucketStatus(root, "This bucket does not exist yet. Create it to use it as the ingestion target.", "danger");
      bucketInput?.focus();
      return false;
    }
  }
  showAlert(root);
  return true;
}

async function initializeWizard(root) {
  if (root.dataset.sourceIngestionInitialized === "true") return;
  root.dataset.sourceIngestionInitialized = "true";
  try {
    const context = await requestJson(`${API_ROOT}/context`);
    if (!root.isConnected) return;
    root.sourceIngestionContext = context;
    const sourceSelect = root.querySelector("[data-source-ingestion-source]");
    sourceSelect.replaceChildren(new Option("Choose an available source", ""));
    (context.sources || []).forEach((source) => sourceSelect.append(new Option(`${source.displayName} · ${source.technology}`, source.id)));
    const bucketInput = root.querySelector("[data-source-ingestion-bucket]");
    renderWizardBucketOptions(root);
    bucketInput.value = context.defaultBucket || context.visibleBuckets?.[0] || "";
    const query = new URLSearchParams(window.location.search);
    const sourceId = query.get("sourceId") || "";
    if (sourceId && Array.from(sourceSelect.options).some((option) => option.value === sourceId)) sourceSelect.value = sourceId;
    const relationReference = query.get("relationReference") || "";
    const referenceParts = relationReference.split(".");
    updateWizardRelationOptions(
      root,
      context,
      query.get("relation") || referenceParts.at(-1) || ""
    );
    updateWizardDestination(root, { force: true });

    sourceSelect.addEventListener("change", () => {
      updateWizardRelationOptions(root, context);
      updateWizardDestination(root, { force: true });
    });
    root.querySelector("[data-source-ingestion-relation]").addEventListener("change", () => updateWizardDestination(root, { force: true }));
    root.querySelector("[data-source-ingestion-bucket]").addEventListener("input", () => {
      root.dataset.sourceIngestionBucketStatus = "";
      showWizardBucketStatus(root);
      syncWizardBucketCreation(root);
      updateWizardDestination(root);
    });
    root.querySelector("[data-source-ingestion-create-bucket]").addEventListener("click", () => createWizardBucket(root));
    root.querySelector("[data-source-ingestion-key]").addEventListener("input", () => updateWizardDestination(root));
    root.querySelectorAll("[data-source-ingestion-mode]").forEach((input) => input.addEventListener("change", () => updateWizardMode(root)));
    root.addEventListener("click", (event) => {
      const next = event.target.closest("[data-source-ingestion-next]");
      const back = event.target.closest("[data-source-ingestion-back]");
      const current = Number(root.dataset.currentStep || 1);
      if (next) {
        if (!validateWizardStep(root, current)) return;
        if (current === 3) renderWizardReview(root);
        setWizardStep(root, Math.min(4, current + 1));
      } else if (back) setWizardStep(root, Math.max(1, current - 1));
    });
    root.querySelector("[data-source-ingestion-form]").addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!validateWizardStep(root, 4)) return;
      const submit = root.querySelector("[data-source-ingestion-submit]");
      submit.disabled = true;
      const relation = selectedRelation(root);
      const scheduled = root.querySelector('[data-source-ingestion-mode][value="scheduled"]')?.checked;
      try {
        const result = await requestJson(API_ROOT, {
          method: "POST",
          body: JSON.stringify({
            clientRequestId: newClientRequestId("create"),
            name: root.querySelector("[data-source-ingestion-name]").value.trim(),
            sourceId: sourceSelect.value,
            relation: { schema: relation.schema, name: relation.name },
            destination: { bucket: bucketInput.value.trim(), key: root.querySelector("[data-source-ingestion-key]").value.trim(), format: "parquet", writeMode: "replace" },
            schedule: { enabled: Boolean(scheduled), cadence: "hourly", minute: 0, timeZone: "Europe/Zurich" },
            activateAfterSuccessfulRun: Boolean(scheduled),
          }),
        });
        window.location.assign(`/ingestion-workbench/sourcing/ingestions/${encodeURIComponent(result.definition.id)}`);
      } catch (error) {
        showAlert(root, error.message);
        submit.disabled = false;
      }
    });
    setWizardStep(root, 1);
    updateWizardMode(root);
    syncWizardBucketCreation(root);
  } catch (error) {
    showAlert(root, error.message);
  }
}

function renderDetail(root, payload) {
  const definition = payload.definition || {};
  const runs = Array.isArray(payload.runs) ? payload.runs : [];
  root.querySelector("[data-source-ingestion-detail-name]").textContent = definition.name || "Source ingestion";
  root.querySelector("[data-source-ingestion-detail-path]").textContent = `s3://${definition.destinationBucket || ""}/${definition.destinationKey || ""}`;
  const status = root.querySelector("[data-source-ingestion-detail-status]");
  status.textContent = statusLabel(definition);
  status.dataset.status = definition.state || "draft";
  root.querySelector("[data-source-ingestion-detail-next-run]").textContent = formatTimestamp(definition.nextRunAt, "No active schedule");
  const scheduled = Boolean(definition.schedule?.enabled || definition.pendingActivation);
  const mode = root.querySelector(`[data-source-ingestion-detail-mode][value="${scheduled ? "scheduled" : "once"}"]`);
  if (mode) mode.checked = true;
  const scheduleAction = root.querySelector("[data-source-ingestion-schedule-action]");
  scheduleAction.textContent = scheduled ? "Pause schedule" : "Resume hourly";
  scheduleAction.dataset.enableSchedule = String(!scheduled);
  const latest = runs[0] || {};
  root.querySelector("[data-source-ingestion-detail-summary]").innerHTML = [
    ["Source relation", `${definition.sourceId || ""}.${definition.schemaName || ""}.${definition.relationName || ""}`],
    ["Last result", latest.status || "No run yet"],
    ["Rows", Number(latest.rowCount || 0).toLocaleString("de-CH")],
    ["File size", formatBytes(latest.sizeBytes)],
    ["Last completed", formatTimestamp(latest.completedAt)],
    ["Query job", latest.queryJobId || "Not available"],
  ].map(([label, value]) => `<article><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></article>`).join("");
  const runList = root.querySelector("[data-source-ingestion-runs]");
  if (!runs.length) {
    runList.innerHTML = '<div class="sourcing-empty"><strong>The first run is queued.</strong><p>Status changes arrive through the shared Workbench event stream.</p></div>';
  } else {
    runList.innerHTML = runs.map((run) => `<article class="source-ingestion-run-card" data-status="${escapeHtml(run.status)}"><div><span>${escapeHtml(String(run.trigger || "manual").replaceAll("-", " "))}</span><h4>${escapeHtml(run.status || "queued")}</h4><p>${escapeHtml(run.message || run.error || "")}</p></div><dl><div><dt>Started</dt><dd>${escapeHtml(formatTimestamp(run.startedAt, "Queued"))}</dd></div><div><dt>Rows</dt><dd>${Number(run.rowCount || 0).toLocaleString("de-CH")}</dd></div><div><dt>Size</dt><dd>${escapeHtml(formatBytes(run.sizeBytes))}</dd></div><div><dt>Run ID</dt><dd><code>${escapeHtml(run.id)}</code></dd></div></dl></article>`).join("");
  }
  root.dataset.definitionState = definition.state || "draft";
  root.dataset.scheduleEnabled = String(Boolean(definition.schedule?.enabled));
}

async function refreshDetail(root) {
  const id = root.dataset.sourceIngestionId;
  try {
    const payload = await requestJson(`${API_ROOT}/${encodeURIComponent(id)}`);
    if (!root.isConnected) return;
    renderDetail(root, payload);
    showAlert(root);
  } catch (error) {
    showAlert(root, error.message);
  }
}

async function initializeDetail(root) {
  if (root.dataset.sourceIngestionInitialized === "true") return;
  root.dataset.sourceIngestionInitialized = "true";
  root.querySelector("[data-source-ingestion-run-now]").addEventListener("click", async (event) => {
    const button = event.currentTarget;
    button.disabled = true;
    try {
      await requestJson(`${API_ROOT}/${encodeURIComponent(root.dataset.sourceIngestionId)}/runs`, { method: "POST", body: JSON.stringify({ clientRequestId: newClientRequestId("manual"), trigger: "manual" }) });
      await refreshDetail(root);
    } catch (error) {
      showAlert(root, error.message);
    } finally {
      button.disabled = false;
    }
  });
  root.querySelector("[data-source-ingestion-schedule-action]").addEventListener("click", async (event) => {
    const button = event.currentTarget;
    const enabled = button.dataset.enableSchedule === "true";
    button.disabled = true;
    try {
      await requestJson(`${API_ROOT}/${encodeURIComponent(root.dataset.sourceIngestionId)}/schedule`, { method: "PUT", body: JSON.stringify({ enabled, clientRequestId: newClientRequestId("schedule") }) });
      await refreshDetail(root);
    } catch (error) {
      showAlert(root, error.message);
    } finally {
      button.disabled = false;
    }
  });
  root.querySelectorAll("[data-source-ingestion-detail-mode]").forEach((input) => input.addEventListener("change", async (event) => {
    const enabled = event.currentTarget.value === "scheduled";
    root.querySelectorAll("[data-source-ingestion-detail-mode]").forEach((node) => { node.disabled = true; });
    try {
      await requestJson(`${API_ROOT}/${encodeURIComponent(root.dataset.sourceIngestionId)}/schedule`, { method: "PUT", body: JSON.stringify({ enabled, clientRequestId: newClientRequestId("schedule") }) });
      await refreshDetail(root);
    } catch (error) {
      showAlert(root, error.message);
      await refreshDetail(root);
    } finally {
      root.querySelectorAll("[data-source-ingestion-detail-mode]").forEach((node) => { node.disabled = false; });
    }
  }));
  await refreshDetail(root);
}

export function initializeSourceIngestionWorkbench() {
  const hub = document.querySelector("[data-source-ingestion-hub]");
  if (hub && hub.dataset.sourceIngestionInitialized !== "true") {
    hub.dataset.sourceIngestionInitialized = "true";
    hub.dataset.sourceIngestionCatalogView = storedSourceCatalogView();
    const updateViewButtons = () => {
      hub.querySelectorAll("[data-source-ingestion-catalog-view]").forEach((button) => {
        const active = button.dataset.sourceIngestionCatalogView === hub.dataset.sourceIngestionCatalogView;
        button.classList.toggle("is-active", active);
        button.setAttribute("aria-pressed", String(active));
      });
    };
    updateViewButtons();
    hub.addEventListener("input", (event) => {
      if (event.target.closest("[data-source-ingestion-catalog-query]") && hub.sourceIngestionContext) {
        renderHubSources(hub, hub.sourceIngestionContext);
      }
    });
    hub.addEventListener("click", (event) => {
      const button = event.target.closest("[data-source-ingestion-catalog-view]");
      if (!button) return;
      hub.dataset.sourceIngestionCatalogView = button.dataset.sourceIngestionCatalogView === "list" ? "list" : "table";
      storeSourceCatalogView(hub.dataset.sourceIngestionCatalogView);
      updateViewButtons();
      if (hub.sourceIngestionContext) renderHubSources(hub, hub.sourceIngestionContext);
    });
    refreshHub(hub);
  }
  const wizard = document.querySelector("[data-source-ingestion-wizard]");
  if (wizard) initializeWizard(wizard);
  const detail = document.querySelector("[data-source-ingestion-detail]");
  if (detail) initializeDetail(detail);
}

export function applySourceIngestionRealtimeSnapshot(_snapshot) {
  const hub = document.querySelector("[data-source-ingestion-hub]");
  if (hub) refreshHub(hub);
  const detail = document.querySelector("[data-source-ingestion-detail]");
  if (detail) refreshDetail(detail);
}

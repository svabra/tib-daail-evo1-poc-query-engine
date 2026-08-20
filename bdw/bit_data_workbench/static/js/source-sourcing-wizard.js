import { currentDaaifDemoUser } from "./demo-identity.js";

const PAGE_SIZE = 12;

async function api(path, options = {}) {
  const response = await fetch(path, { credentials: "same-origin", ...options });
  let payload = null;
  try { payload = await response.json(); } catch (_error) { payload = null; }
  if (!response.ok) {
    throw new Error(String(payload?.detail || `Request failed: ${response.status}`));
  }
  return payload;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[char]));
}

function todayIso() {
  const now = new Date();
  const shifted = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
  return shifted.toISOString().slice(0, 10);
}

export function initializeSourceSourcingWizard() {
  const root = document.querySelector("[data-source-sourcing-wizard]");
  if (!(root instanceof HTMLElement) || root.dataset.initialized === "true") return;
  root.dataset.initialized = "true";

  const state = { step: 1, offset: 0, matched: 0, selectedSource: null, context: null, request: null, pollId: null };
  const alert = root.querySelector("[data-sourcing-alert]");
  const query = root.querySelector("[data-sourcing-query]");
  const site = root.querySelector("[data-sourcing-site]");
  const sourceType = root.querySelector("[data-sourcing-source-type]");
  const form = root.querySelector("[data-sourcing-request-form]");
  const validFrom = form?.elements.namedItem("validFrom");
  const validUntil = form?.elements.namedItem("validUntil");
  if (validFrom instanceof HTMLInputElement) { validFrom.min = todayIso(); validFrom.value = todayIso(); }
  if (validUntil instanceof HTMLInputElement) validUntil.min = todayIso();

  const announce = (message, tone = "info") => {
    if (!(alert instanceof HTMLElement)) return;
    alert.hidden = !message;
    alert.textContent = message;
    alert.dataset.tone = tone;
  };

  const showStep = (step) => {
    state.step = step;
    root.querySelectorAll("[data-sourcing-step]").forEach((panel) => { panel.hidden = Number(panel.dataset.sourcingStep) !== step; });
    root.querySelectorAll("[data-sourcing-step-indicator]").forEach((item) => {
      const index = Number(item.dataset.sourcingStepIndicator);
      item.classList.toggle("is-active", index === step);
      item.classList.toggle("is-complete", index < step);
      if (index === step) item.setAttribute("aria-current", "step"); else item.removeAttribute("aria-current");
    });
    announce("");
    root.scrollIntoView({ block: "start", behavior: "smooth" });
  };

  const syncIdentity = async () => {
    const userId = currentDaaifDemoUser()?.id || "joel.ruod";
    await api("/api/ingestion/sourcing/identity", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ userId }) });
  };

  const renderCatalog = (payload) => {
    const summary = payload?.summary || {};
    state.matched = Number(summary.matched || 0);
    root.querySelector("[data-sourcing-catalog-summary]").textContent = `${summary.discoverable || 0} of ${summary.total || 0} Oracle databases are discoverable to you. ${summary.hidden || 0} remain hidden by policy.`;
    const results = root.querySelector("[data-sourcing-results]");
    const items = Array.isArray(payload?.items) ? payload.items : [];
    results.innerHTML = items.length ? items.map((item) => {
      const objects = (item.objects || []).map((object) => `${object.schema}.${object.name}`).join(" · ");
      const sites = (item.sites || []).join(" + ");
      return `<label class="sourcing-source-card${state.selectedSource?.id === item.id ? " is-selected" : ""}">
        <input type="radio" name="sourceId" value="${escapeHtml(item.id)}" ${state.selectedSource?.id === item.id ? "checked" : ""}>
        <span class="sourcing-source-card-main"><span class="sourcing-source-title-row"><strong>${escapeHtml(item.displayName)}</strong><span>${escapeHtml(item.databaseName)}</span></span>
        <span class="sourcing-source-description">${escapeHtml(item.description)}</span><span class="sourcing-source-objects">${escapeHtml(objects)}</span></span>
        <span class="sourcing-source-meta"><span>${escapeHtml(item.ownerName)} · ${escapeHtml(item.organization)}</span><strong>${escapeHtml(sites)}</strong><small>${escapeHtml(item.accessStatus || "none")}</small></span>
      </label>`;
    }).join("") : `<div class="sourcing-empty"><strong>No discoverable databases match these filters.</strong><span>Hidden sources are never disclosed through search or direct links.</span></div>`;
    const page = Math.floor(state.offset / PAGE_SIZE) + 1;
    const pages = Math.max(1, Math.ceil(state.matched / PAGE_SIZE));
    root.querySelector("[data-sourcing-page-copy]").textContent = `Page ${page} of ${pages}`;
    root.querySelector('[data-sourcing-page="previous"]').disabled = state.offset === 0;
    root.querySelector('[data-sourcing-page="next"]').disabled = state.offset + PAGE_SIZE >= state.matched;
    root.querySelector('[data-sourcing-step="2"] [data-sourcing-next]').disabled = !state.selectedSource;
    results.querySelectorAll('input[name="sourceId"]').forEach((input) => input.addEventListener("change", () => {
      state.selectedSource = items.find((item) => item.id === input.value) || null;
      renderCatalog(payload);
    }));
  };

  const loadCatalog = async () => {
    announce("Loading your governed Oracle catalog …");
    try {
      await syncIdentity();
      const params = new URLSearchParams({ q: query?.value || "", site: site?.value || "", offset: String(state.offset), limit: String(PAGE_SIZE) });
      renderCatalog(await api(`/api/ingestion/sourcing/catalog?${params}`));
      announce("");
    } catch (error) { announce(error.message, "danger"); }
  };

  const loadContext = async () => {
    state.context = await api(`/api/ingestion/sourcing/catalog/${encodeURIComponent(state.selectedSource.id)}/access-context`);
    root.querySelector("[data-sourcing-selection-summary]").innerHTML = `<strong>${escapeHtml(state.context.source.displayName)}</strong><span>${escapeHtml(state.context.source.databaseName)} · Owner ${escapeHtml(state.context.source.ownerName)} · ${(state.context.source.sites || []).map(escapeHtml).join(" + ")}</span>`;
    root.querySelector("[data-sourcing-subjects]").innerHTML = (state.context.subjects || []).map((subject) => `<label class="sourcing-subject-card"><input type="radio" name="subject" value="${escapeHtml(`${subject.type}:${subject.id}`)}" ${subject.recommended ? "checked" : ""}><span><strong>${escapeHtml(subject.label)}</strong><small>${subject.type === "group" ? `${subject.memberCount} members · revision ${subject.membershipRevision}${subject.recommended ? " · Recommended" : ""}` : "Personal access for Joel Ruod"}</small></span></label>`).join("");
  };

  const formPayload = () => {
    const data = new FormData(form);
    const [type, id] = String(data.get("subject") || "").split(":", 2);
    return {
      clientRequestId: state.request?.clientRequestId || crypto.randomUUID(),
      sourceId: state.selectedSource.id,
      requestTitle: data.get("requestTitle"), subject: { type, id }, purpose: data.get("purpose"), legalBasis: data.get("legalBasis"),
      validFrom: data.get("validFrom"), validUntil: data.get("validUntil") || null, conditionsAccepted: data.get("conditionsAccepted") === "on",
    };
  };

  const renderReview = () => {
    const payload = formPayload();
    const subject = state.context.subjects.find((item) => item.type === payload.subject.type && item.id === payload.subject.id);
    state.request = payload;
    root.querySelector("[data-sourcing-review]").innerHTML = [
      ["Source", `${state.selectedSource.displayName} (${state.selectedSource.databaseName})`], ["Owner", `${state.selectedSource.ownerName} · ${state.selectedSource.organization}`], ["Access subject", subject?.label],
      ["Purpose", payload.purpose], ["Legal basis", payload.legalBasis], ["Validity", `${payload.validFrom} – ${payload.validUntil || "Unlimited"}`],
    ].map(([term, value]) => `<div><dt>${escapeHtml(term)}</dt><dd>${escapeHtml(value)}</dd></div>`).join("");
  };

  const pollStatus = async () => {
    if (!state.request) return;
    try {
      const requests = await api("/api/ingestion/sourcing/requests/mine");
      const current = requests.find((item) => item.clientRequestId === state.request.clientRequestId);
      const status = root.querySelector("[data-sourcing-receipt-status]");
      if (current) status.textContent = current.status === "submitted" ? "Awaiting decision by Sandro Wenger · BAZG. This page refreshes every 3 seconds." : `${current.status === "approved" ? "Approved" : "Rejected"}${current.decisionComment ? ` · ${current.decisionComment}` : ""}`;
      if (current?.status === "approved") {
        const grants = await api("/api/ingestion/sourcing/grants/mine");
        const grant = grants.find((item) => item.source?.id === state.selectedSource.id);
        if (grant) {
          status.textContent = grant.state === "active" ? "Approved · grant active. The source is now available in DAAIF." : `Approved · active from ${grant.validFrom}.`;
          const sourceLink = root.querySelector("[data-sourcing-open-source]");
          sourceLink.href = `/data-sources?source_id=${encodeURIComponent(state.selectedSource.id)}`;
          sourceLink.hidden = grant.state !== "active";
          if (grant.state === "active" && state.pollId) { clearInterval(state.pollId); state.pollId = null; }
        }
      }
    } catch (_error) { /* next poll retries without exposing another source */ }
  };

  const syncSourceTypeSelection = () => {
    root.querySelector('[data-sourcing-step="1"] [data-sourcing-next]').disabled = sourceType?.value !== "oracle";
  };
  sourceType?.addEventListener("change", syncSourceTypeSelection);
  syncSourceTypeSelection();
  let debounce = 0;
  query?.addEventListener("input", () => { clearTimeout(debounce); state.offset = 0; debounce = setTimeout(loadCatalog, 250); });
  site?.addEventListener("change", () => { state.offset = 0; void loadCatalog(); });
  root.querySelectorAll("[data-sourcing-page]").forEach((button) => button.addEventListener("click", () => { state.offset = Math.max(0, state.offset + (button.dataset.sourcingPage === "next" ? PAGE_SIZE : -PAGE_SIZE)); void loadCatalog(); }));
  root.querySelectorAll("[data-sourcing-back]").forEach((button) => button.addEventListener("click", () => showStep(Math.max(1, state.step - 1))));
  root.querySelectorAll("[data-sourcing-next]").forEach((button) => button.addEventListener("click", async () => {
    try {
      if (state.step === 1) { showStep(2); await loadCatalog(); return; }
      if (state.step === 2) { await loadContext(); showStep(3); return; }
      if (state.step === 3) {
        if (!form.reportValidity()) return;
        if (validUntil?.value && validUntil.value < validFrom.value) { validUntil.setCustomValidity("Valid until cannot be before valid from."); validUntil.reportValidity(); return; }
        validUntil?.setCustomValidity(""); renderReview(); showStep(4);
      }
    } catch (error) { announce(error.message, "danger"); }
  }));
  root.querySelector("[data-sourcing-submit]")?.addEventListener("click", async (event) => {
    const button = event.currentTarget; button.disabled = true; announce("Submitting the governed access request …");
    try {
      const receipt = await api("/api/ingestion/sourcing/requests", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(state.request) });
      root.querySelectorAll("[data-sourcing-step]").forEach((panel) => { panel.hidden = true; });
      const receiptPanel = root.querySelector("[data-sourcing-receipt]"); receiptPanel.hidden = false;
      root.querySelector("[data-sourcing-receipt-title]").textContent = `${receipt.requestNumber} · Request submitted`;
      root.querySelector("[data-sourcing-receipt-copy]").textContent = `${receipt.source.displayName} will be reviewed by ${receipt.ownerName}.`;
      announce(""); await pollStatus(); state.pollId = setInterval(pollStatus, 3000);
    } catch (error) { announce(error.message, "danger"); button.disabled = false; }
  });
}

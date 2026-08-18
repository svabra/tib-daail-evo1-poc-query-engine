import { closeDialog } from "./dialog-manager.js";

import { dataProductSourceForPublication } from "./data-products-source.js";

export function createDataProductsController(helpers) {
  const {
    fetchJsonOrThrow,
    getCardNodes,
    getSearchEmpty,
    getSearchInput,
    getPageRoot,
    loadDataProductsPage,
    previewContractMarkup,
    readSourceOptions,
    showConfirmDialog,
    showMessageDialog,
    ensureEditDialog,
    ensurePublicationDialog,
  } = helpers;

  const publicationState = {
    accessLevel: "internal",
    accessNote: "",
    description: "",
    domain: "",
    owner: "",
    overwriteConfirmed: false,
    publishToDaca: true,
    preview: null,
    previewing: false,
    publishing: false,
    requestAccessContact: "",
    selectedSource: null,
    selectedSourceOptionId: "",
    selectedSourceType: "all",
    slug: "",
    slugTouched: false,
    sourceLocked: false,
    sourceOptions: [],
    step: 1,
    tagsText: "",
    targetAudience: "",
    title: "",
    updateFrequency: "",
  };

  const demoUsers = {
    "beat.stalder": { displayName: "Beat Stalder", email: "beat.stalder@sg.ch" },
    "joel.ruod": { displayName: "Joel Ruod", email: "joel.ruod@estv.admin.ch" },
    "kassandra.valdata": {
      displayName: "Kassandra Valdata",
      email: "kassandra.valdata@estv.admin.ch",
    },
    "noemie.rochat": { displayName: "Noémie Rochat", email: "noemie.rochat@ne.ch" },
    "thomas.kriegli": {
      displayName: "Thomas Kriegli",
      email: "thomas.kriegli@estv.admin.ch",
    },
  };

  function currentDemoUser() {
    let storedUser = "";
    try {
      storedUser = window.localStorage.getItem("daaif-demo-user") || "";
    } catch (_error) {
      storedUser = "";
    }
    const userId =
      document.documentElement.dataset.daaifDemoUser || storedUser || "joel.ruod";
    return demoUsers[userId] || demoUsers["joel.ruod"];
  }

  function isJourneySource(source) {
    const searchable = JSON.stringify(source || {}).toLowerCase();
    return (
      searchable.includes("kantonale-gewerbesteuer") ||
      searchable.includes("kantonale_gewerbesteuer") ||
      searchable.includes("data-analysts-journey") ||
      searchable.includes("gewerbesteuer")
    );
  }

  function applyMetadataDefaults(source) {
    const user = currentDemoUser();
    publicationState.owner = user.displayName;
    publicationState.requestAccessContact = user.email;
    publicationState.accessLevel = "internal";
    publicationState.publishToDaca = true;

    if (isJourneySource(source)) {
      publicationState.title =
        "Kantonale Gewerbesteuer: Soll/Ist und Jahreshochrechnung 2022–2026";
      publicationState.slug = "kantonale-gewerbesteuer-soll-ist-2022-2026";
      publicationState.description =
        "Vollständig synthetische kantonale Gewerbesteuer-Kennzahlen: Jahresplan, Ist bis 12.08.2026 und Jahreshochrechnung für 2022–2026.";
      publicationState.owner = "Joel Ruod";
      publicationState.domain = "Unternehmens-/Gewerbesteuer";
      publicationState.tagsText = "Gewerbesteuer, Kantone, Soll/Ist, synthetisch";
      publicationState.accessNote =
        "DaCa Default Deny bis zum Abschluss der Governance- und Vier-Augen-Freigabe.";
      publicationState.requestAccessContact = "joel.ruod@estv.admin.ch";
      publicationState.updateFrequency = "monthly";
      publicationState.targetAudience = "EFD – EFV / Bundestresorerie";
      return;
    }

    publicationState.title = source ? sourceLabel(source) : "";
    publicationState.slug = publicationState.title ? toSlug(publicationState.title) : "";
    publicationState.description = "";
    publicationState.domain = "";
    publicationState.tagsText = "";
    publicationState.accessNote = "";
    publicationState.updateFrequency = "";
    publicationState.targetAudience = "";
  }

  function toSlug(value) {
    return String(value ?? "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
  }

  function parseTagsInput(value) {
    const uniqueTags = [];
    const seen = new Set();
    String(value ?? "")
      .split(/[,;\n]/)
      .map((item) => item.trim())
      .filter(Boolean)
      .forEach((tag) => {
        const normalized = tag.toLowerCase();
        if (seen.has(normalized)) {
          return;
        }
        seen.add(normalized);
        uniqueTags.push(tag);
      });
    return uniqueTags;
  }

  function escapeMarkup(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function publicationDialog() {
    return ensurePublicationDialog();
  }

  function editDialog() {
    return ensureEditDialog();
  }

  function currentSourceDescriptor() {
    if (publicationState.sourceLocked) {
      return publicationState.selectedSource;
    }
    const selectedOption = publicationState.sourceOptions.find(
      (option) => option.optionId === publicationState.selectedSourceOptionId
    );
    return selectedOption?.source ?? null;
  }

  function publicationSourceDescriptor() {
    return dataProductSourceForPublication(currentSourceDescriptor(), {
      publishToDaca: publicationState.publishToDaca,
    });
  }

  function sourceLabel(source) {
    if (!source) {
      return "No source selected";
    }
    return (
      source.sourceDisplayName ||
      source.relation ||
      source.bucket ||
      source.key ||
      "Selected source"
    );
  }

  function sourceTypeForSource(source) {
    if (!source) {
      return "other";
    }
    if (source.sourceKind === "bucket") {
      return "shared-bucket";
    }
    if (source.sourceKind === "object") {
      return "shared-object";
    }
    if (source.sourceKind === "local-object") {
      return "local-object";
    }
    if (source.sourceKind === "relation" && source.sourcePlatform === "postgres") {
      return "postgres-relation";
    }
    if (source.sourceKind === "relation") {
      return "shared-relation";
    }
    return "other";
  }

  function sourceTypeLabel(value) {
    if (value === "postgres-relation") {
      return "PostgreSQL relations";
    }
    if (value === "shared-relation") {
      return "Shared Workspace relations";
    }
    if (value === "shared-bucket") {
      return "Shared Workspace buckets";
    }
    if (value === "shared-object") {
      return "Shared Workspace objects";
    }
    if (value === "local-object") {
      return "Local Workspace files";
    }
    return "All server-visible sources";
  }

  function filteredSourceOptions() {
    if (publicationState.selectedSourceType === "all") {
      return publicationState.sourceOptions;
    }
    return publicationState.sourceOptions.filter(
      (option) => sourceTypeForSource(option?.source) === publicationState.selectedSourceType
    );
  }

  function sourceTypeOptionsMarkup() {
    const counts = new Map();
    publicationState.sourceOptions.forEach((option) => {
      const type = sourceTypeForSource(option?.source);
      counts.set(type, (counts.get(type) || 0) + 1);
    });

    const orderedTypes = [
      "postgres-relation",
      "shared-relation",
      "shared-bucket",
      "shared-object",
      "local-object",
    ].filter((type) => counts.has(type));

    return [
      `<option value="all">All server-visible sources (${publicationState.sourceOptions.length})</option>`,
      ...orderedTypes.map(
        (type) => `<option value="${type}">${sourceTypeLabel(type)} (${counts.get(type) || 0})</option>`
      ),
    ].join("");
  }

  function resetPublicationSourceSelection() {
    publicationState.selectedSourceOptionId = "";
    publicationState.selectedSource = null;
    invalidatePublicationPreview();
    if (!publicationState.slugTouched) {
      publicationState.title = "";
      publicationState.slug = "";
    }
  }

  function invalidatePublicationPreview() {
    publicationState.preview = null;
    publicationState.overwriteConfirmed = false;
  }

  function overwriteConflict() {
    const preview = publicationState.preview;
    const existingProduct = preview?.existingProduct;
    if (!preview?.overwriteRequired || !existingProduct?.productId) {
      return null;
    }
    return {
      canOverwrite: preview.canOverwrite === true,
      existingProduct,
    };
  }

  function overwriteIsConfirmed() {
    const conflict = overwriteConflict();
    return Boolean(conflict?.canOverwrite && publicationState.overwriteConfirmed);
  }

  function existingProductSourceCopy(product) {
    const location = product?.relation
      ? product.relation
      : product?.bucket && product?.key
        ? `s3://${product.bucket}/${product.key}`
        : product?.bucket
          ? `s3://${product.bucket}`
          : product?.key || "Source not available";
    return [product?.sourcePlatform, location]
      .map((value) => String(value || "").trim())
      .filter(Boolean)
      .join(" / ");
  }

  function overwriteConflictMarkup(conflict) {
    if (!conflict) {
      return "";
    }
    const product = conflict.existingProduct;
    const endpoint = product.publishedUrl || product.publicPath || "Endpoint not available";
    const dacaStatus = product.dacaManaged
      ? `DaCa managed / ${product.dacaPublication?.state || "state unavailable"}`
      : "Local publication only";
    const blockedCopy = publicationState.preview?.blockedReason ||
      "The selected slug is already in use.";

    return `
      <section class="data-product-overwrite-conflict" role="alert" aria-live="polite">
        <div class="data-product-overwrite-conflict-heading">
          <span class="data-product-preview-summary-label">Slug conflict</span>
          <h3>This slug is already published</h3>
          <p>${escapeMarkup(blockedCopy)}</p>
        </div>
        <dl class="data-product-overwrite-details">
          <div>
            <dt>Existing title</dt>
            <dd>${escapeMarkup(product.title || product.slug)}</dd>
          </div>
          <div>
            <dt>Endpoint</dt>
            <dd>${escapeMarkup(endpoint)}</dd>
          </div>
          <div>
            <dt>Current source</dt>
            <dd>${escapeMarkup(existingProductSourceCopy(product))}</dd>
          </div>
          <div>
            <dt>Governance</dt>
            <dd>${escapeMarkup(dacaStatus)}</dd>
          </div>
          <div>
            <dt>Last updated</dt>
            <dd>${escapeMarkup(product.updatedAt || "Timestamp not available")}</dd>
          </div>
        </dl>
        ${
          conflict.canOverwrite
            ? `
              <label class="data-product-overwrite-confirmation">
                <input
                  type="checkbox"
                  data-data-product-overwrite-confirm
                  ${publicationState.overwriteConfirmed ? "checked" : ""}
                >
                <span>
                  <strong>Replace the existing data product</strong>
                  <small>
                    I understand that the source and metadata for
                    <code>${escapeMarkup(product.slug)}</code> will be replaced. Its endpoint URL,
                    product identity and DaCa governance remain unchanged.
                  </small>
                </span>
              </label>
            `
            : '<p class="data-product-overwrite-not-allowed">This product cannot be replaced from the current publication flow.</p>'
        }
      </section>
    `;
  }

  function sourceKindCopy(source) {
    if (!source) {
      return "";
    }
    if (source.sourceKind === "bucket") {
      return `Shared Workspace bucket ${source.bucket}`;
    }
    if (source.sourceKind === "object") {
      return `Shared Workspace object s3://${source.bucket}/${source.key}`;
    }
    if (source.sourceKind === "local-object") {
      return "Local Workspace browser file";
    }
    return `Relation ${source.relation}`;
  }

  function sourceCompatibilityMarkup(source) {
    if (!source) {
      return `
        <h3>Choose a source first</h3>
        <p class="modal-copy">Select or confirm the source before continuing to the publication rules.</p>
      `;
    }

    if (source.sourceKind === "local-object") {
      return `
        <h3>Unsupported for live publication</h3>
        <p class="modal-copy">${source.unsupportedReason || "Live publication requires a server-visible source."}</p>
        <p class="modal-copy">Move the file into Shared Workspace first, then publish from there.</p>
      `;
    }

    let responseCopy = "Published endpoints stay live and read-only in v1.";
    if (source.sourceKind === "bucket") {
      responseCopy += " Consumers receive a JSON bucket listing with optional prefix filtering.";
    } else if (source.sourceKind === "object") {
      responseCopy += publicationState.publishToDaca
        ? " DAAIF registers this tabular object as a typed relation and consumers receive paginated JSON rows."
        : " Consumers receive the raw object content with the original or inferred media type.";
    } else {
      responseCopy += " Consumers receive paginated JSON rows with columns, items, limit, offset, and hasMore.";
    }

    return `
      <h3>Compatible for live publication</h3>
      <p class="modal-copy"><strong>Source:</strong> ${sourceLabel(source)}</p>
      <p class="modal-copy"><strong>Behavior:</strong> ${responseCopy}</p>
      <p class="modal-copy"><strong>Mutability:</strong> The published URL is stable and the slug is immutable in v1.</p>
    `;
  }

  function publicationPayload({ includeOverwrite = false } = {}) {
    const payload = {
      source: publicationSourceDescriptor(),
      title: publicationState.title,
      slug: publicationState.slug,
      description: publicationState.description,
      owner: publicationState.owner,
      domain: publicationState.domain,
      tags: parseTagsInput(publicationState.tagsText),
      accessLevel: publicationState.accessLevel,
      accessNote: publicationState.accessNote,
      requestAccessContact: publicationState.requestAccessContact,
      customProperties: {
        ...(publicationState.updateFrequency
          ? { updateFrequency: publicationState.updateFrequency }
          : {}),
        ...(publicationState.targetAudience
          ? { targetAudience: publicationState.targetAudience }
          : {}),
      },
      publishToDaca: publicationState.publishToDaca,
    };
    if (!includeOverwrite) {
      return payload;
    }

    const existingProduct = overwriteConflict()?.existingProduct;
    const overwriteExisting = overwriteIsConfirmed();
    return {
      ...payload,
      overwriteExisting,
      expectedProductId: overwriteExisting ? existingProduct?.productId || "" : "",
      expectedUpdatedAt: overwriteExisting ? existingProduct?.updatedAt || "" : "",
    };
  }

  function syncPublicationActionState(dialog) {
    const preview = publicationState.preview;
    const conflict = overwriteConflict();
    const overwriteConfirmed = overwriteIsConfirmed();
    const publishButton = dialog.querySelector("[data-data-product-dialog-publish]");
    const publishToDacaInput = dialog.querySelector("[data-data-product-publish-to-daca]");
    const dacaManagedExisting = Boolean(conflict?.existingProduct?.dacaManaged);

    if (dacaManagedExisting) {
      publicationState.publishToDaca = true;
    }
    publishToDacaInput.checked = publicationState.publishToDaca;
    publishToDacaInput.disabled = dacaManagedExisting;
    publishToDacaInput
      .closest(".data-product-daca-publication-option")
      ?.classList.toggle("is-locked", dacaManagedExisting);

    publishButton.disabled =
      publicationState.publishing ||
      !preview ||
      Boolean(preview?.blocked && !overwriteConfirmed);
    publishButton.textContent = publicationState.publishing
      ? conflict
        ? "Replacing..."
        : "Publishing..."
      : conflict
        ? "Replace existing data product"
        : "Publish";
  }

  function publicationStepCanContinue(step) {
    const source = currentSourceDescriptor();
    if (step === 1) {
      return Boolean(source);
    }
    if (step === 2) {
      return Boolean(source && source.sourceKind !== "local-object");
    }
    if (step === 3) {
      return Boolean(publicationState.title.trim());
    }
    return false;
  }

  function renderPublicationDialog() {
    const dialog = publicationDialog();
    const source = currentSourceDescriptor();
    const sourceTypeSelect = dialog.querySelector("[data-data-product-source-type-select]");
    const select = dialog.querySelector("[data-data-product-source-select]");
    const sourceEmpty = dialog.querySelector("[data-data-product-source-empty]");
    const sourcePreview = dialog.querySelector("[data-data-product-source-preview]");
    const compatibility = dialog.querySelector("[data-data-product-compatibility-card]");
    const previewSummary = dialog.querySelector("[data-data-product-preview-summary]");
    const overwritePanel = dialog.querySelector("[data-data-product-overwrite-panel]");
    const contractPanel = dialog.querySelector("[data-data-product-contract-panel]");
    const backButton = dialog.querySelector("[data-data-product-dialog-back]");
    const nextButton = dialog.querySelector("[data-data-product-dialog-next]");
    const publishButton = dialog.querySelector("[data-data-product-dialog-publish]");
    const titleInput = dialog.querySelector("[data-data-product-title-input]");
    const slugInput = dialog.querySelector("[data-data-product-slug-input]");
    const descriptionInput = dialog.querySelector("[data-data-product-description-input]");
    const ownerInput = dialog.querySelector("[data-data-product-owner-input]");
    const domainInput = dialog.querySelector("[data-data-product-domain-input]");
    const updateFrequencyInput = dialog.querySelector(
      "[data-data-product-update-frequency-input]"
    );
    const targetAudienceInput = dialog.querySelector(
      "[data-data-product-target-audience-input]"
    );
    const tagsInput = dialog.querySelector("[data-data-product-tags-input]");
    const accessLevelInput = dialog.querySelector("[data-data-product-access-level-input]");
    const requestAccessContactInput = dialog.querySelector(
      "[data-data-product-request-access-contact-input]"
    );
    const accessNoteInput = dialog.querySelector("[data-data-product-access-note-input]");
    const publishToDacaInput = dialog.querySelector("[data-data-product-publish-to-daca]");
    const visibleSourceOptions = filteredSourceOptions();

    sourceTypeSelect.innerHTML = sourceTypeOptionsMarkup();
    sourceTypeSelect.value = publicationState.selectedSourceType;
    sourceTypeSelect.hidden = publicationState.sourceLocked;
    sourceTypeSelect.disabled = publicationState.sourceLocked || !publicationState.sourceOptions.length;
    select.innerHTML = visibleSourceOptions.length
      ? [
          '<option value="">Choose a source</option>',
          ...visibleSourceOptions.map(
            (option) =>
              `<option value="${option.optionId}">${option.label}</option>`
          ),
        ].join("")
      : "";
    select.value = publicationState.selectedSourceOptionId;
    select.hidden = publicationState.sourceLocked;
    select.disabled = publicationState.sourceLocked || !visibleSourceOptions.length;
    sourceEmpty.hidden = publicationState.sourceLocked || Boolean(visibleSourceOptions.length);
    sourceEmpty.textContent = publicationState.sourceOptions.length
      ? `No sources match the selected data source type: ${sourceTypeLabel(publicationState.selectedSourceType)}.`
      : "No publishable server-visible sources are currently visible in this runtime.";
    sourcePreview.innerHTML = source
      ? `
          <strong>${sourceLabel(source)}</strong>
          <p class="modal-copy">${sourceKindCopy(source)}</p>
        `
      : '<p class="modal-copy">Choose a source to continue.</p>';
    compatibility.innerHTML = sourceCompatibilityMarkup(source);

    titleInput.value = publicationState.title;
    slugInput.value = publicationState.slug;
    descriptionInput.value = publicationState.description;
    ownerInput.value = publicationState.owner;
    domainInput.value = publicationState.domain;
    updateFrequencyInput.value = publicationState.updateFrequency;
    targetAudienceInput.value = publicationState.targetAudience;
    tagsInput.value = publicationState.tagsText;
    accessLevelInput.value = publicationState.accessLevel;
    requestAccessContactInput.value = publicationState.requestAccessContact;
    accessNoteInput.value = publicationState.accessNote;

    dialog
      .querySelectorAll("[data-data-product-step-panel]")
      .forEach((panel) => {
        panel.hidden =
          panel.dataset.dataProductStepPanel !== String(publicationState.step);
      });
    dialog
      .querySelectorAll("[data-data-product-step-indicator]")
      .forEach((indicator) => {
        const step = Number(indicator.dataset.dataProductStepIndicator || 0);
        indicator.classList.toggle("is-active", step === publicationState.step);
        indicator.classList.toggle("is-complete", step < publicationState.step);
      });

    const firstStep = publicationState.sourceLocked ? 2 : 1;
    backButton.hidden = publicationState.step <= firstStep;
    nextButton.hidden = publicationState.step >= 4;
    publishButton.hidden = publicationState.step !== 4;
    nextButton.disabled =
      publicationState.previewing || !publicationStepCanContinue(publicationState.step);
    nextButton.textContent =
      publicationState.step === 3
        ? publicationState.previewing
          ? "Preparing preview..."
          : "Preview Endpoint"
        : "Continue";
    syncPublicationActionState(dialog);

    if (publicationState.step === 4) {
      const preview = publicationState.preview;
      const conflict = overwriteConflict();
      previewSummary.innerHTML = preview
        ? `
            <div class="data-product-preview-summary-grid">
              <article class="data-product-preview-summary-card">
                <span class="data-product-preview-summary-label">Endpoint</span>
                <strong>${preview.product.publicPath}</strong>
                <p>${preview.product.publishedUrl}</p>
              </article>
              <article class="data-product-preview-summary-card">
                <span class="data-product-preview-summary-label">Mode</span>
                <strong>${preview.product.publicationMode}</strong>
                <p>${preview.sourceSummary}</p>
              </article>
              <article class="data-product-preview-summary-card">
                <span class="data-product-preview-summary-label">Data Product Page</span>
                <strong>${preview.product.documentationPath}</strong>
                <p>
                  <a
                    href="${preview.product.documentationUrl}"
                    target="_blank"
                    rel="noreferrer"
                    class="data-product-inline-link"
                  >
                    Open Data Product Page
                  </a>
                </p>
              </article>
            </div>
            ${
              preview.blocked && !conflict
                ? `<p class="data-product-preview-blocked">${preview.blockedReason}</p>`
                : `<p class="modal-copy">${preview.liveReadOnlyCopy}</p>`
            }
          `
        : '<p class="modal-copy">Generate a preview to review the endpoint contract.</p>';
      contractPanel.innerHTML = previewContractMarkup(preview);
      overwritePanel.hidden = !conflict;
      overwritePanel.innerHTML = overwriteConflictMarkup(conflict);
    } else {
      overwritePanel.hidden = true;
      overwritePanel.innerHTML = "";
    }
  }

  async function openPublishDialog({
    source = null,
    lockSource = false,
    startStep = 1,
  } = {}) {
    publicationState.step = lockSource ? Math.max(startStep, 2) : 1;
    publicationState.sourceLocked = lockSource;
    publicationState.sourceOptions = readSourceOptions();
    publicationState.selectedSource = source;
    publicationState.selectedSourceOptionId = "";
    invalidatePublicationPreview();
    publicationState.previewing = false;
    publicationState.publishing = false;
    publicationState.slugTouched = false;
    publicationState.selectedSourceType = "all";
    applyMetadataDefaults(source);

    renderPublicationDialog();
    publicationDialog().showModal();
  }

  async function previewPublication() {
    publicationState.overwriteConfirmed = false;
    publicationState.previewing = true;
    renderPublicationDialog();
    try {
      publicationState.preview = await fetchJsonOrThrow("/api/data-products/preview", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(publicationPayload()),
      });
      if (publicationState.preview?.existingProduct?.dacaManaged) {
        publicationState.publishToDaca = true;
      }
      publicationState.step = 4;
      renderPublicationDialog();
    } catch (error) {
      publicationState.previewing = false;
      renderPublicationDialog();
      await showMessageDialog({
        title: "Data product preview failed",
        copy:
          error instanceof Error
            ? error.message
            : "The data product preview could not be generated.",
      });
      return;
    }
    publicationState.previewing = false;
    renderPublicationDialog();
  }

  async function publishDataProduct() {
    if (publicationState.publishing) {
      return;
    }
    const overwriteExisting = overwriteIsConfirmed();
    publicationState.publishing = true;
    renderPublicationDialog();
    try {
      const payload = await fetchJsonOrThrow("/api/data-products", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(publicationPayload({ includeOverwrite: true })),
      });
      closeDialog(publicationDialog(), "confirm");
      await loadDataProductsPage();
      const dacaPublication = payload.dacaPublication;
      await showMessageDialog({
        title: overwriteExisting
          ? dacaPublication
            ? "Data product replaced and updated in DaCa"
            : "Data product replaced"
          : dacaPublication
            ? "Data product published and sent to DaCa"
            : "Data product published",
        copy: overwriteExisting
          ? dacaPublication
            ? `${payload.product.title} was replaced while retaining its endpoint, product identity and DaCa governance. The updated metadata was sent to DaCa.`
            : `${payload.product.title} was replaced while retaining its endpoint and product identity.`
          : dacaPublication
            ? `${payload.product.title} is locally published and registered for DaCa pre-review. Nobody has a DaCa access grant yet; configure governance and access in DaCa.`
            : `${payload.product.title} is now published. Data product page: ${payload.product.documentationUrl}.`,
        links: [
          ...(dacaPublication?.catalogUrl
            ? [
                {
                  href: dacaPublication.catalogUrl,
                  label: "Open DaCa quality wizard as Joel Ruod",
                  external: true,
                },
              ]
            : []),
          {
            href: payload.product.documentationUrl,
            label: "Open data product page",
            external: true,
          },
          {
            href: payload.product.publishedUrl,
            label: "Open raw endpoint",
            external: true,
          },
        ],
      });
    } catch (error) {
      await showMessageDialog({
        title: "Data product publish failed",
        copy:
          error instanceof Error
            ? error.message
            : "The data product could not be published.",
      });
    } finally {
      publicationState.publishing = false;
      renderPublicationDialog();
    }
  }

  function applySearchFilter() {
    const term = String(getSearchInput()?.value || "")
      .trim()
      .toLowerCase();
    const cards = getCardNodes();
    let visibleCount = 0;
    cards.forEach((card) => {
      const searchableText = String(card.dataset.dataProductSearchText || "").trim();
      const visible = !term || searchableText.includes(term);
      card.hidden = !visible;
      if (visible) {
        visibleCount += 1;
      }
    });

    const searchEmpty = getSearchEmpty();
    if (searchEmpty) {
      searchEmpty.hidden = !(term && cards.length && visibleCount === 0);
    }
  }

  function readCardRecord(card) {
    return {
      productId: card.dataset.dataProductId || "",
      slug: card.dataset.dataProductSlug || "",
      title: card.dataset.dataProductTitle || "",
      description: card.dataset.dataProductDescription || "",
      owner: card.dataset.dataProductOwner || "",
      domain: card.dataset.dataProductDomain || "",
      tagsText: String(card.dataset.dataProductTags || "")
        .split("||")
        .filter(Boolean)
        .join(", "),
      accessLevel: card.dataset.dataProductAccessLevel || "internal",
      accessNote: card.dataset.dataProductAccessNote || "",
      requestAccessContact: card.dataset.dataProductRequestAccessContact || "",
      publishedUrl:
        card.dataset.dataProductPublishedUrl || card.dataset.dataProductPublicPath || "",
      documentationPath: card.dataset.dataProductDocumentationPath || "",
      documentationUrl:
        card.dataset.dataProductDocumentationUrl ||
        card.dataset.dataProductDocumentationPath ||
        "",
      source: {
        sourceKind: card.dataset.dataProductSourceKind || "",
        sourceId: card.dataset.dataProductSourceId || "",
        relation: card.dataset.dataProductRelation || "",
        bucket: card.dataset.dataProductBucket || "",
        key: card.dataset.dataProductKey || "",
        sourceDisplayName: card.dataset.dataProductSourceDisplayName || "",
        sourcePlatform: card.dataset.dataProductSourcePlatform || "",
      },
    };
  }

  function closeCardActionMenu(element) {
    const menu = element?.closest("[data-workspace-action-menu]");
    if (menu instanceof HTMLDetailsElement) {
      menu.open = false;
    }
  }

  async function copyPublishedUrl(card) {
    const url = readCardRecord(card).publishedUrl;
    if (!url) {
      await showMessageDialog({
        title: "Published URL unavailable",
        copy: "The published URL is not available for this data product.",
      });
      return;
    }

    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(url);
      } else {
        const textarea = document.createElement("textarea");
        textarea.value = url;
        textarea.setAttribute("readonly", "readonly");
        textarea.style.position = "absolute";
        textarea.style.left = "-9999px";
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand("copy");
        textarea.remove();
      }
      await showMessageDialog({
        title: "Published URL copied",
        copy: url,
      });
    } catch (error) {
      await showMessageDialog({
        title: "Copy failed",
        copy:
          error instanceof Error
            ? error.message
            : "The published URL could not be copied.",
      });
    }
  }

  async function openEditDialog(card) {
    const record = readCardRecord(card);
    const dialog = editDialog();
    dialog.querySelector("[data-data-product-edit-id]").value = record.productId;
    dialog.querySelector("[data-data-product-edit-title-input]").value = record.title;
    dialog.querySelector("[data-data-product-edit-slug-input]").value = record.slug;
    dialog.querySelector("[data-data-product-edit-description-input]").value = record.description;
    dialog.querySelector("[data-data-product-edit-owner-input]").value = record.owner;
    dialog.querySelector("[data-data-product-edit-domain-input]").value = record.domain;
    dialog.querySelector("[data-data-product-edit-tags-input]").value = record.tagsText;
    dialog.querySelector("[data-data-product-edit-access-level-input]").value = record.accessLevel;
    dialog.querySelector("[data-data-product-edit-request-access-contact-input]").value =
      record.requestAccessContact;
    dialog.querySelector("[data-data-product-edit-access-note-input]").value = record.accessNote;
    dialog.showModal();
  }

  async function saveEditDialog(form) {
    const productId = form.querySelector("[data-data-product-edit-id]").value;
    try {
      await fetchJsonOrThrow(`/api/data-products/${encodeURIComponent(productId)}`, {
        method: "PUT",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          title: form.querySelector("[data-data-product-edit-title-input]").value,
          description: form.querySelector("[data-data-product-edit-description-input]").value,
          owner: form.querySelector("[data-data-product-edit-owner-input]").value,
          domain: form.querySelector("[data-data-product-edit-domain-input]").value,
          tags: parseTagsInput(form.querySelector("[data-data-product-edit-tags-input]").value),
          accessLevel: form.querySelector("[data-data-product-edit-access-level-input]").value,
          requestAccessContact: form.querySelector(
            "[data-data-product-edit-request-access-contact-input]"
          ).value,
          accessNote: form.querySelector("[data-data-product-edit-access-note-input]").value,
          customProperties: {},
        }),
      });
      closeDialog(editDialog(), "confirm");
      await loadDataProductsPage();
      await showMessageDialog({
        title: "Metadata updated",
        copy: "The data product metadata was updated.",
      });
    } catch (error) {
      await showMessageDialog({
        title: "Metadata update failed",
        copy:
          error instanceof Error
            ? error.message
            : "The data product metadata could not be updated.",
      });
    }
  }

  async function deleteDataProduct(card) {
    const record = readCardRecord(card);
    const { confirmed } = await showConfirmDialog({
      title: "Unpublish data product",
      copy: `Unpublish ${record.title}? The public endpoint /api/public/data-products/${record.slug} will stop resolving.`,
      confirmLabel: "Unpublish",
    });
    if (!confirmed) {
      return;
    }

    try {
      await fetchJsonOrThrow(`/api/data-products/${encodeURIComponent(record.productId)}`, {
        method: "DELETE",
        headers: {
          Accept: "application/json",
        },
      });
      await loadDataProductsPage();
      await showMessageDialog({
        title: "Data product unpublished",
        copy: `${record.title} was removed from the managed publication registry.`,
      });
    } catch (error) {
      await showMessageDialog({
        title: "Unpublish failed",
        copy:
          error instanceof Error
            ? error.message
            : "The data product could not be unpublished.",
      });
    }
  }

  async function handleClick(event) {
    const openDialogButton = event.target.closest("[data-open-data-product-dialog]");
    if (openDialogButton) {
      event.preventDefault();
      await openPublishDialog();
      return true;
    }

    const backButton = event.target.closest("[data-data-product-dialog-back]");
    if (backButton) {
      event.preventDefault();
      const firstStep = publicationState.sourceLocked ? 2 : 1;
      publicationState.step = Math.max(firstStep, publicationState.step - 1);
      renderPublicationDialog();
      return true;
    }

    const nextButton = event.target.closest("[data-data-product-dialog-next]");
    if (nextButton) {
      event.preventDefault();
      if (publicationState.step === 1) {
        publicationState.step = 2;
        renderPublicationDialog();
        return true;
      }
      if (publicationState.step === 2) {
        publicationState.step = 3;
        renderPublicationDialog();
        return true;
      }
      if (publicationState.step === 3) {
        await previewPublication();
        return true;
      }
    }

    const publishButton = event.target.closest("[data-data-product-dialog-publish]");
    if (publishButton) {
      event.preventDefault();
      await publishDataProduct();
      return true;
    }

    const copyButton = event.target.closest("[data-copy-data-product-url]");
    if (copyButton) {
      event.preventDefault();
      closeCardActionMenu(copyButton);
      await copyPublishedUrl(copyButton.closest("[data-data-product-card]"));
      return true;
    }

    const editButton = event.target.closest("[data-edit-data-product]");
    if (editButton) {
      event.preventDefault();
      closeCardActionMenu(editButton);
      await openEditDialog(editButton.closest("[data-data-product-card]"));
      return true;
    }

    const deleteButton = event.target.closest("[data-delete-data-product]");
    if (deleteButton) {
      event.preventDefault();
      closeCardActionMenu(deleteButton);
      await deleteDataProduct(deleteButton.closest("[data-data-product-card]"));
      return true;
    }

    return false;
  }

  function handleInput(event) {
    if (event.target.matches("[data-data-product-search]")) {
      applySearchFilter();
      return true;
    }

    const dialog = document.querySelector("[data-data-product-dialog]");
    if (!(dialog instanceof HTMLDialogElement) || !dialog.contains(event.target)) {
      return false;
    }

    if (event.target.matches("[data-data-product-title-input]")) {
      const previousSlug = publicationState.slug;
      publicationState.title = event.target.value;
      if (!publicationState.slugTouched) {
        publicationState.slug = toSlug(publicationState.title);
        const slugInput = dialog.querySelector("[data-data-product-slug-input]");
        if (slugInput) {
          slugInput.value = publicationState.slug;
        }
      }
      if (publicationState.slug !== previousSlug) {
        invalidatePublicationPreview();
      }
      return true;
    }
    if (event.target.matches("[data-data-product-slug-input]")) {
      const previousSlug = publicationState.slug;
      publicationState.slugTouched = true;
      publicationState.slug = toSlug(event.target.value);
      event.target.value = publicationState.slug;
      if (publicationState.slug !== previousSlug) {
        invalidatePublicationPreview();
      }
      return true;
    }
    if (event.target.matches("[data-data-product-description-input]")) {
      publicationState.description = event.target.value;
      return true;
    }
    if (event.target.matches("[data-data-product-owner-input]")) {
      publicationState.owner = event.target.value;
      return true;
    }
    if (event.target.matches("[data-data-product-domain-input]")) {
      publicationState.domain = event.target.value;
      return true;
    }
    if (event.target.matches("[data-data-product-update-frequency-input]")) {
      publicationState.updateFrequency = event.target.value;
      return true;
    }
    if (event.target.matches("[data-data-product-target-audience-input]")) {
      publicationState.targetAudience = event.target.value;
      return true;
    }
    if (event.target.matches("[data-data-product-tags-input]")) {
      publicationState.tagsText = event.target.value;
      return true;
    }
    if (event.target.matches("[data-data-product-request-access-contact-input]")) {
      publicationState.requestAccessContact = event.target.value;
      return true;
    }
    if (event.target.matches("[data-data-product-access-note-input]")) {
      publicationState.accessNote = event.target.value;
      return true;
    }

    return false;
  }

  function handleChange(event) {
    const dialog = document.querySelector("[data-data-product-dialog]");
    if (dialog instanceof HTMLDialogElement && dialog.contains(event.target)) {
      if (event.target.matches("[data-data-product-source-type-select]")) {
        publicationState.selectedSourceType = event.target.value || "all";
        const selectedOptionStillVisible = filteredSourceOptions().some(
          (option) => option.optionId === publicationState.selectedSourceOptionId
        );
        if (!selectedOptionStillVisible) {
          resetPublicationSourceSelection();
        }
        renderPublicationDialog();
        return true;
      }
      if (event.target.matches("[data-data-product-source-select]")) {
        publicationState.selectedSourceOptionId = event.target.value;
        publicationState.selectedSource = currentSourceDescriptor();
        publicationState.slugTouched = false;
        applyMetadataDefaults(currentSourceDescriptor());
        invalidatePublicationPreview();
        renderPublicationDialog();
        return true;
      }
      if (event.target.matches("[data-data-product-access-level-input]")) {
        publicationState.accessLevel = event.target.value;
        return true;
      }
      if (event.target.matches("[data-data-product-publish-to-daca]")) {
        const changed = publicationState.publishToDaca !== event.target.checked;
        publicationState.publishToDaca = event.target.checked;
        if (changed && publicationState.preview) {
          invalidatePublicationPreview();
          publicationState.step = 3;
          renderPublicationDialog();
        }
        return true;
      }
      if (event.target.matches("[data-data-product-overwrite-confirm]")) {
        publicationState.overwriteConfirmed = event.target.checked;
        syncPublicationActionState(dialog);
        return true;
      }
    }

    const edit = document.querySelector("[data-data-product-edit-dialog]");
    if (
      edit instanceof HTMLDialogElement &&
      edit.contains(event.target) &&
      event.target.matches("[data-data-product-edit-access-level-input]")
    ) {
      return true;
    }

    return false;
  }

  async function handleSubmit(event) {
    const editForm = event.target.closest("[data-data-product-edit-form]");
    if (editForm) {
      event.preventDefault();
      await saveEditDialog(editForm);
      return true;
    }
    return false;
  }

  function initializeCurrentPage() {
    if (!getPageRoot()) {
      return;
    }
    applySearchFilter();
  }

  return {
    handleChange,
    handleClick,
    handleInput,
    handleSubmit,
    initializeCurrentPage,
    openPublishDialog,
  };
}

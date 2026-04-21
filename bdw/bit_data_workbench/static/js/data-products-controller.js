import { closeDialog } from "./dialog-manager.js";

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
    title: "",
  };

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
    publicationState.preview = null;
    if (!publicationState.slugTouched) {
      publicationState.title = "";
      publicationState.slug = "";
    }
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
      responseCopy += " Consumers receive the raw object content with the original or inferred media type.";
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

  function publicationPayload() {
    return {
      source: currentSourceDescriptor(),
      title: publicationState.title,
      slug: publicationState.slug,
      description: publicationState.description,
      owner: publicationState.owner,
      domain: publicationState.domain,
      tags: parseTagsInput(publicationState.tagsText),
      accessLevel: publicationState.accessLevel,
      accessNote: publicationState.accessNote,
      requestAccessContact: publicationState.requestAccessContact,
      customProperties: {},
    };
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
    const contractPanel = dialog.querySelector("[data-data-product-contract-panel]");
    const backButton = dialog.querySelector("[data-data-product-dialog-back]");
    const nextButton = dialog.querySelector("[data-data-product-dialog-next]");
    const publishButton = dialog.querySelector("[data-data-product-dialog-publish]");
    const titleInput = dialog.querySelector("[data-data-product-title-input]");
    const slugInput = dialog.querySelector("[data-data-product-slug-input]");
    const descriptionInput = dialog.querySelector("[data-data-product-description-input]");
    const ownerInput = dialog.querySelector("[data-data-product-owner-input]");
    const domainInput = dialog.querySelector("[data-data-product-domain-input]");
    const tagsInput = dialog.querySelector("[data-data-product-tags-input]");
    const accessLevelInput = dialog.querySelector("[data-data-product-access-level-input]");
    const requestAccessContactInput = dialog.querySelector(
      "[data-data-product-request-access-contact-input]"
    );
    const accessNoteInput = dialog.querySelector("[data-data-product-access-note-input]");
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
    publishButton.disabled =
      publicationState.publishing ||
      !publicationState.preview ||
      Boolean(publicationState.preview?.blocked);
    publishButton.textContent = publicationState.publishing
      ? "Publishing..."
      : "Publish";

    if (publicationState.step === 4) {
      const preview = publicationState.preview;
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
              preview.blocked
                ? `<p class="data-product-preview-blocked">${preview.blockedReason}</p>`
                : `<p class="modal-copy">${preview.liveReadOnlyCopy}</p>`
            }
          `
        : '<p class="modal-copy">Generate a preview to review the endpoint contract.</p>';
      contractPanel.innerHTML = previewContractMarkup(preview);
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
    publicationState.preview = null;
    publicationState.previewing = false;
    publicationState.publishing = false;
    publicationState.slugTouched = false;
    publicationState.selectedSourceType = "all";
    publicationState.title = sourceLabel(source === null ? null : source);
    if (!source) {
      publicationState.title = "";
    }
    publicationState.slug = publicationState.title ? toSlug(publicationState.title) : "";
    publicationState.description = "";
    publicationState.owner = "";
    publicationState.domain = "";
    publicationState.tagsText = "";
    publicationState.accessLevel = "internal";
    publicationState.accessNote = "";
    publicationState.requestAccessContact = "";

    renderPublicationDialog();
    publicationDialog().showModal();
  }

  async function previewPublication() {
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
    publicationState.publishing = true;
    renderPublicationDialog();
    try {
      const payload = await fetchJsonOrThrow("/api/data-products", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(publicationPayload()),
      });
      closeDialog(publicationDialog(), "confirm");
      await loadDataProductsPage();
      await showMessageDialog({
        title: "Data product published",
        copy: `${payload.product.title} is now published. Data product page: ${payload.product.documentationUrl}.`,
        links: [
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
      publicationState.title = event.target.value;
      if (!publicationState.slugTouched) {
        publicationState.slug = toSlug(publicationState.title);
        const slugInput = dialog.querySelector("[data-data-product-slug-input]");
        if (slugInput) {
          slugInput.value = publicationState.slug;
        }
      }
      return true;
    }
    if (event.target.matches("[data-data-product-slug-input]")) {
      publicationState.slugTouched = true;
      publicationState.slug = toSlug(event.target.value);
      event.target.value = publicationState.slug;
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
        if (!publicationState.slugTouched) {
          publicationState.title = sourceLabel(currentSourceDescriptor());
          publicationState.slug = toSlug(publicationState.title);
        }
        publicationState.preview = null;
        renderPublicationDialog();
        return true;
      }
      if (event.target.matches("[data-data-product-access-level-input]")) {
        publicationState.accessLevel = event.target.value;
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

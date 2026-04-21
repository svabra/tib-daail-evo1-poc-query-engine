export function explorerEmptyStateMarkup(copy, { tone = "default" } = {}, escapeHtml) {
  return `
    <div class="data-source-explorer-empty" data-tone="${escapeHtml(tone)}">
      <p>${escapeHtml(copy)}</p>
    </div>
  `;
}

export function fieldListMarkup(fields, escapeHtml) {
  const normalizedFields = Array.isArray(fields) ? fields : [];
  if (!normalizedFields.length) {
    return explorerEmptyStateMarkup(
      "No fields are available for the current selection.",
      {},
      escapeHtml
    );
  }

  return `
    <ul class="sidebar-source-field-list">
      ${normalizedFields
        .map(
          (field) => `
            <li class="sidebar-source-field">
              <span class="sidebar-source-field-name">
                <span class="sidebar-source-field-name-text">${escapeHtml(field.name || "")}</span>
              </span>
              <span class="sidebar-source-field-type">${escapeHtml(field.dataType || "UNKNOWN")}</span>
            </li>
          `
        )
        .join("")}
    </ul>
  `;
}

export function actionButtonMarkup(label, action, escapeHtml, { tone = "default" } = {}) {
  return `
    <button
      type="button"
      class="data-source-explorer-action"
      data-data-source-explorer-action="${escapeHtml(action)}"
      data-tone="${escapeHtml(tone)}"
    >
      ${escapeHtml(label)}
    </button>
  `;
}

export function publicationBadgeMarkup(publishedProducts, escapeHtml) {
  const normalizedProducts = Array.isArray(publishedProducts) ? publishedProducts : [];
  if (!normalizedProducts.length) {
    return "";
  }

  const label =
    normalizedProducts.length === 1
      ? "Data Product"
      : `${normalizedProducts.length} Data Products`;

  return `
    <span class="data-source-publication-pill" title="Published as a managed Data Product">
      ${escapeHtml(label)}
    </span>
  `;
}

export function publicationLinksMarkup(publishedProducts, escapeHtml) {
  const normalizedProducts = Array.isArray(publishedProducts) ? publishedProducts : [];
  if (!normalizedProducts.length) {
    return "";
  }

  return `
    <section class="data-source-publication-panel">
      <div class="data-source-publication-header">
        ${publicationBadgeMarkup(normalizedProducts, escapeHtml)}
        <p>This source is already published as a managed Data Product.</p>
      </div>
      <div class="data-source-publication-links">
        ${normalizedProducts
          .map(
            (product) => `
              <a
                href="${escapeHtml(product.documentationPath || "")}"
                class="data-source-publication-link"
              >
                ${escapeHtml(product.title || product.slug || "Open Data Product")}
              </a>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

export function sourceObjectElement(descriptor = {}) {
  const element = document.createElement("div");
  if (descriptor.relation) {
    element.dataset.sourceObjectRelation = String(descriptor.relation);
  }
  if (descriptor.name) {
    element.dataset.sourceObjectName = String(descriptor.name);
  }
  if (descriptor.displayName) {
    element.dataset.sourceObjectDisplayName = String(descriptor.displayName);
  }
  if (descriptor.kind) {
    element.dataset.sourceObjectKind = String(descriptor.kind);
  }
  if (descriptor.sourceOptionId) {
    element.dataset.sourceOptionId = String(descriptor.sourceOptionId);
  }
  if (descriptor.localWorkspaceEntryId) {
    element.dataset.localWorkspaceEntryId = String(descriptor.localWorkspaceEntryId);
  }
  if (descriptor.localWorkspaceFolderPath) {
    element.dataset.localWorkspaceFolderPath = String(descriptor.localWorkspaceFolderPath);
  }
  if (descriptor.localWorkspaceExportFormat) {
    element.dataset.localWorkspaceExportFormat = String(descriptor.localWorkspaceExportFormat);
  }
  if (descriptor.localWorkspaceSizeBytes !== undefined) {
    element.dataset.localWorkspaceSizeBytes = String(descriptor.localWorkspaceSizeBytes);
  }
  if (descriptor.localWorkspaceCreatedAt) {
    element.dataset.localWorkspaceCreatedAt = String(descriptor.localWorkspaceCreatedAt);
  }
  if (descriptor.localWorkspaceColumnCount !== undefined) {
    element.dataset.localWorkspaceColumnCount = String(descriptor.localWorkspaceColumnCount);
  }
  if (descriptor.localWorkspaceRowCount !== undefined) {
    element.dataset.localWorkspaceRowCount = String(descriptor.localWorkspaceRowCount);
  }
  if (descriptor.localWorkspaceMimeType) {
    element.dataset.localWorkspaceMimeType = String(descriptor.localWorkspaceMimeType);
  }
  if (descriptor.s3Bucket) {
    element.dataset.s3Bucket = String(descriptor.s3Bucket);
  }
  if (descriptor.s3Key) {
    element.dataset.s3Key = String(descriptor.s3Key);
  }
  if (descriptor.s3Path) {
    element.dataset.s3Path = String(descriptor.s3Path);
  }
  if (descriptor.s3FileFormat) {
    element.dataset.s3FileFormat = String(descriptor.s3FileFormat);
  }
  if (descriptor.s3Downloadable !== undefined) {
    element.dataset.s3Downloadable = descriptor.s3Downloadable ? "true" : "false";
  }
  return element;
}

export function sourceSchemaElement(bucket) {
  const element = document.createElement("div");
  element.dataset.sourceBucket = String(bucket || "").trim();
  return element;
}

export function detailCardMarkup({
  eyebrow = "",
  title = "",
  copy = "",
  actions = "",
  body = "",
}, escapeHtml) {
  return `
    <article class="data-source-explorer-detail-card">
      <div class="data-source-explorer-detail-copy">
        ${eyebrow ? `<span class="home-eyebrow">${escapeHtml(eyebrow)}</span>` : ""}
        <h4>${escapeHtml(title)}</h4>
        <p>${escapeHtml(copy)}</p>
      </div>
      ${actions ? `<div class="data-source-explorer-action-row">${actions}</div>` : ""}
      <div class="data-source-explorer-detail-body">
        ${body}
      </div>
    </article>
  `;
}

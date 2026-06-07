import { truncateSourceNavigationLabel } from "../source-navigation-labels.js";

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

export function actionButtonMarkup(
  label,
  action,
  escapeHtml,
  { tone = "default", disabled = false, title = "" } = {}
) {
  return `
    <button
      type="button"
      class="data-source-explorer-action"
      data-data-source-explorer-action="${escapeHtml(action)}"
      data-tone="${escapeHtml(tone)}"
      ${title ? `title="${escapeHtml(title)}"` : ""}
      ${disabled ? "disabled" : ""}
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

function sourceClassSuffix(value) {
  return String(value || "table")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "") || "table";
}

function attributeMarkup(attributes = {}, escapeHtml) {
  return Object.entries(attributes)
    .filter(([, value]) => value !== undefined && value !== null && value !== false)
    .map(([name, value]) => {
      if (value === true) {
        return name;
      }
      return `${name}="${escapeHtml(value)}"`;
    })
    .join(" ");
}

export function sourcePublicationBadgeMarkup(publishedProducts, escapeHtml) {
  const normalizedProducts = Array.isArray(publishedProducts) ? publishedProducts : [];
  if (!normalizedProducts.length) {
    return "";
  }

  const label =
    normalizedProducts.length === 1
      ? "Data Product"
      : `${normalizedProducts.length} Data Products`;

  return `
    <span class="source-publication-pill" title="Published as a managed Data Product">
      ${escapeHtml(label)}
    </span>
  `;
}

export function sourceSchemaIconMarkup(kind = "schema") {
  const normalizedKind = String(kind || "schema").trim().toLowerCase();
  if (normalizedKind === "folder" || normalizedKind === "bucket" || normalizedKind === "prefix") {
    return `
      <svg class="source-icon source-icon-schema" viewBox="0 0 16 16" aria-hidden="true">
        <path d="M2.2 4.1a1.1 1.1 0 0 1 1.1-1.1h3l1.2 1.5h5.2a1.1 1.1 0 0 1 1.1 1.1v5.9a1.1 1.1 0 0 1-1.1 1.1H3.3a1.1 1.1 0 0 1-1.1-1.1z"></path>
      </svg>
    `;
  }

  return `
    <svg class="source-icon source-icon-schema" viewBox="0 0 16 16" aria-hidden="true">
      <rect x="1.6" y="1.8" width="3.7" height="3.7" rx="0.7"></rect>
      <rect x="10.7" y="1.8" width="3.7" height="3.7" rx="0.7"></rect>
      <rect x="10.7" y="10.5" width="3.7" height="3.7" rx="0.7"></rect>
      <path d="M5.3 3.7h2.8a2 2 0 0 1 2 2v1.5"></path>
      <path d="M10.1 8H7.4a2 2 0 0 0-2 2v.5"></path>
    </svg>
  `;
}

export function sourceObjectIconMarkup(kind = "table") {
  const normalizedKind = String(kind || "table").trim().toLowerCase();
  if (["folder", "bucket", "prefix"].includes(normalizedKind)) {
    return sourceSchemaIconMarkup(normalizedKind);
  }
  if (normalizedKind === "view" || normalizedKind === "materialized view" || normalizedKind === "file") {
    return `
      <svg class="source-icon source-icon-object source-icon-object-view" viewBox="0 0 16 16" aria-hidden="true">
        <rect x="2.4" y="3" width="11.2" height="9.6" rx="1.1"></rect>
        <path d="M4.2 5.2h7.6M4.2 7.7h7.6"></path>
        <path d="M3.2 10.7c1.5-1.7 3.1-2.6 4.8-2.6s3.3.9 4.8 2.6"></path>
        <circle cx="8" cy="10.2" r="1"></circle>
      </svg>
    `;
  }

  return `
    <svg class="source-icon source-icon-object source-icon-object-table" viewBox="0 0 16 16" aria-hidden="true">
      <rect x="2.1" y="2.3" width="11.8" height="11.1" rx="1.1"></rect>
      <path d="M2.8 6.1h10.4M2.8 9.4h10.4M6 2.9v9.8M10 2.9v9.8"></path>
    </svg>
  `;
}

export function sourceActionMenuMarkup(items = [], escapeHtml, { label = "Source actions" } = {}) {
  const normalizedItems = Array.isArray(items) ? items.filter(Boolean) : [];
  if (!normalizedItems.length) {
    return "";
  }

  return `
    <details class="workspace-action-menu source-action-menu" data-source-action-menu>
      <summary
        class="workspace-action-menu-toggle"
        data-source-action-menu-toggle
        aria-label="${escapeHtml(label)}"
        title="${escapeHtml(label)}"
      >
        <span class="workspace-action-menu-dots" aria-hidden="true">...</span>
      </summary>
      <div class="workspace-action-menu-panel">
        ${normalizedItems
          .map((item) => {
            if (item === "separator" || item.separator) {
              return `<div class="workspace-action-menu-separator" aria-hidden="true"></div>`;
            }
            const itemLabel = String(item.label || "").trim();
            if (!itemLabel) {
              return "";
            }
            const dangerClass = item.tone === "danger" ? " workspace-action-menu-item-danger" : "";
            const attrs = attributeMarkup(item.attrs || {}, escapeHtml);
            if (item.href) {
              return `
                <a
                  href="${escapeHtml(item.href)}"
                  class="workspace-action-menu-item${dangerClass}"
                  ${item.title ? `title="${escapeHtml(item.title)}"` : ""}
                  ${attrs}
                >
                  ${escapeHtml(itemLabel)}
                </a>
              `;
            }
            return `
              <button
                type="button"
                class="workspace-action-menu-item${dangerClass}"
                data-data-source-explorer-action="${escapeHtml(item.action || "")}"
                ${item.title ? `title="${escapeHtml(item.title)}"` : ""}
                ${item.disabled ? "disabled" : ""}
                ${attrs}
              >
                ${escapeHtml(itemLabel)}
              </button>
            `;
          })
          .join("")}
      </div>
    </details>
  `;
}

export function sourceObjectRowMarkup({
  kind = "table",
  displayName = "",
  title = "",
  searchable = "",
  selected = false,
  attrs = {},
  meta = "",
  actions = "",
  labelExtras = "",
  extraClass = "",
} = {}, escapeHtml) {
  const visibleName = String(displayName || "").trim();
  const label = truncateSourceNavigationLabel(visibleName);
  const className = [
    "source-object",
    `source-object-${sourceClassSuffix(kind)}`,
    selected ? "is-selected" : "",
    extraClass,
  ]
    .filter(Boolean)
    .join(" ");
  const rowAttrs = attributeMarkup(
    {
      "data-searchable-item": searchable || visibleName,
      ...attrs,
    },
    escapeHtml
  );

  return `
    <li class="${escapeHtml(className)}" ${rowAttrs}>
      <span class="source-node-label">
        ${sourceObjectIconMarkup(kind)}
        <span title="${escapeHtml(title || visibleName)}">${escapeHtml(label || visibleName)}</span>
        ${labelExtras}
      </span>
      <span class="source-object-meta">
        ${meta}
        ${actions}
      </span>
    </li>
  `;
}

export function sourceSchemaDetailsMarkup({
  label = "",
  searchable = "",
  open = true,
  attrs = {},
  meta = "",
  children = "",
  iconKind = "schema",
  extraClass = "",
} = {}, escapeHtml) {
  const visibleLabel = String(label || "").trim();
  const detailsAttrs = attributeMarkup(attrs, escapeHtml);
  const className = ["source-schema", extraClass].filter(Boolean).join(" ");
  return `
    <details class="${escapeHtml(className)}" ${detailsAttrs} ${open ? "open" : ""}>
      <summary data-searchable-item="${escapeHtml(searchable || visibleLabel)}">
        <span class="source-node-label">
          ${sourceSchemaIconMarkup(iconKind)}
          <span title="${escapeHtml(visibleLabel)}">${escapeHtml(truncateSourceNavigationLabel(visibleLabel) || visibleLabel)}</span>
        </span>
        <span class="source-schema-meta">
          ${meta}
        </span>
      </summary>
      ${children}
    </details>
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
  if (descriptor.queryAlias) {
    element.dataset.sourceObjectQueryAlias = String(descriptor.queryAlias);
  }
  if (descriptor.queryReference) {
    element.dataset.sourceObjectQueryReference = String(descriptor.queryReference);
  }
  if (descriptor.querySql) {
    element.dataset.sourceObjectQuerySql = String(descriptor.querySql);
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
  if (descriptor.sizeBytes !== undefined) {
    element.dataset.s3SizeBytes = String(descriptor.sizeBytes);
  }
  if (descriptor.s3Downloadable !== undefined) {
    element.dataset.s3Downloadable = descriptor.s3Downloadable ? "true" : "false";
  }
  if (descriptor.s3DownloadKind) {
    element.dataset.s3DownloadKind = String(descriptor.s3DownloadKind);
  }
  if (descriptor.s3PartPrefix) {
    element.dataset.s3PartPrefix = String(descriptor.s3PartPrefix);
  }
  if (descriptor.s3PartFileFormat) {
    element.dataset.s3PartFileFormat = String(descriptor.s3PartFileFormat);
  }
  if (descriptor.s3PartCount !== undefined) {
    element.dataset.s3PartCount = String(descriptor.s3PartCount);
  }
  if (descriptor.s3DownloadFilename) {
    element.dataset.s3DownloadFilename = String(descriptor.s3DownloadFilename);
  }
  if (descriptor.s3MergeDownloadable !== undefined) {
    element.dataset.s3MergeDownloadable = descriptor.s3MergeDownloadable ? "true" : "false";
  }
  if (descriptor.s3ZipDownloadable !== undefined) {
    element.dataset.s3ZipDownloadable = descriptor.s3ZipDownloadable ? "true" : "false";
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

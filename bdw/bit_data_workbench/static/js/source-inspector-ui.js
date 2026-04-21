export function createSourceInspectorUi(helpers) {
  const {
    escapeHtml,
    formatByteCount,
    formatVersionTimestamp,
    localWorkspaceDisplayPath,
    normalizeLocalWorkspaceFolderPath,
    normalizeSourceObjectFields,
    sourceInspector,
    sourceInspectorPanel,
    sourceObjectDisplayKind,
    sourceObjectDisplayName,
  } = helpers;

  function normalizedSourceFieldDataType(dataType) {
    return String(dataType ?? "")
      .trim()
      .toUpperCase();
  }

  function sourceFieldTypeFamily(dataType) {
    const normalized = normalizedSourceFieldDataType(dataType);
    if (!normalized) {
      return "unknown";
    }

    if (
      /(BIGINT|HUGEINT|INTEGER|INT|SMALLINT|TINYINT|DECIMAL|NUMERIC|REAL|DOUBLE|FLOAT|SERIAL|UBIGINT|UHUGEINT|UINTEGER|USMALLINT|UTINYINT)/.test(
        normalized
      )
    ) {
      return "number";
    }

    if (/(DATE|TIME|TIMESTAMP|INTERVAL)/.test(normalized)) {
      return "temporal";
    }

    if (/(BOOL)/.test(normalized)) {
      return "boolean";
    }

    if (/(JSON|JSONB|XML)/.test(normalized)) {
      return "document";
    }

    if (/(BYTEA|BLOB|BINARY|VARBINARY)/.test(normalized)) {
      return "binary";
    }

    if (/(ARRAY|LIST)/.test(normalized)) {
      return "list";
    }

    if (/(MAP|STRUCT|UNION)/.test(normalized)) {
      return "object";
    }

    if (/(CHAR|TEXT|STRING|VARCHAR|UUID|ENUM|INET|CIDR|NAME)/.test(normalized)) {
      return "text";
    }

    return "unknown";
  }

  function sourceFieldIconMarkup(dataType) {
    switch (sourceFieldTypeFamily(dataType)) {
      case "number":
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-number" aria-hidden="true">
            <span class="sidebar-source-field-icon-glyph">123</span>
          </span>
        `;
      case "temporal":
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-temporal" aria-hidden="true">
            <svg viewBox="0 0 16 16" focusable="false">
              <rect x="2.5" y="3.2" width="11" height="10" rx="1.4"></rect>
              <path d="M5 1.9v3M11 1.9v3M2.8 6.1h10.4"></path>
            </svg>
          </span>
        `;
      case "boolean":
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-boolean" aria-hidden="true">
            <svg viewBox="0 0 16 16" focusable="false">
              <path d="M3.2 8.1 6.4 11.3 12.8 4.9"></path>
            </svg>
          </span>
        `;
      case "document":
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-document" aria-hidden="true">
            <svg viewBox="0 0 16 16" focusable="false">
              <path d="M4 2.2h5.2l2.8 2.8v8.8H4z"></path>
              <path d="M9.2 2.2v2.7h2.8"></path>
              <path d="M5.2 7h5.4M5.2 9.2h5.4"></path>
            </svg>
          </span>
        `;
      case "binary":
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-binary" aria-hidden="true">
            <span class="sidebar-source-field-icon-glyph">01</span>
          </span>
        `;
      case "list":
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-list" aria-hidden="true">
            <svg viewBox="0 0 16 16" focusable="false">
              <circle cx="4" cy="4" r="1"></circle>
              <circle cx="4" cy="8" r="1"></circle>
              <circle cx="4" cy="12" r="1"></circle>
              <path d="M6.5 4h5.3M6.5 8h5.3M6.5 12h5.3"></path>
            </svg>
          </span>
        `;
      case "object":
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-object" aria-hidden="true">
            <svg viewBox="0 0 16 16" focusable="false">
              <rect x="2" y="2" width="4.2" height="4.2" rx="0.8"></rect>
              <rect x="9.8" y="2" width="4.2" height="4.2" rx="0.8"></rect>
              <rect x="9.8" y="9.8" width="4.2" height="4.2" rx="0.8"></rect>
              <path d="M6.2 4.1h2.2a1.4 1.4 0 0 1 1.4 1.4v1.1"></path>
              <path d="M9.8 10H7.5a1.4 1.4 0 0 1-1.4-1.4V8"></path>
            </svg>
          </span>
        `;
      case "text":
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-text" aria-hidden="true">
            <svg viewBox="0 0 16 16" focusable="false">
              <path d="M3 3.2h10M8 3.2v9.6M5.2 12.8h5.6"></path>
            </svg>
          </span>
        `;
      default:
        return `
          <span class="sidebar-source-field-icon sidebar-source-field-icon-unknown" aria-hidden="true">
            <svg viewBox="0 0 16 16" focusable="false">
              <circle cx="8" cy="8" r="4.4"></circle>
            </svg>
          </span>
        `;
    }
  }

  function publishedDataProductsForSourceObject(sourceObjectRoot) {
    if (!(sourceObjectRoot instanceof Element)) {
      return [];
    }

    try {
      const payload = JSON.parse(sourceObjectRoot.dataset.publishedDataProducts || "[]");
      return Array.isArray(payload) ? payload : [];
    } catch {
      return [];
    }
  }

  function publicationMarkup(sourceObjectRoot) {
    const publishedProducts = publishedDataProductsForSourceObject(sourceObjectRoot);
    if (!publishedProducts.length) {
      return "";
    }

    return `
      <section class="sidebar-source-publication-panel">
        <div class="sidebar-source-publication-header">
          <span class="source-publication-pill">Data Product${publishedProducts.length === 1 ? "" : "s"}</span>
          <p class="sidebar-source-publication-copy">This source object is already published as a managed Data Product.</p>
        </div>
        <div class="sidebar-source-publication-links">
          ${publishedProducts
            .map(
              (product) => `
                <a
                  href="${escapeHtml(product.documentationPath || "")}"
                  class="sidebar-source-publication-link"
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

  function sourceInspectorMarkup(sourceObjectRoot, fields) {
    const objectName = sourceObjectDisplayName(sourceObjectRoot);
    const objectKind = sourceObjectDisplayKind(sourceObjectRoot);
    const fieldCountLabel = `${fields.length} ${fields.length === 1 ? "field" : "fields"}`;

    const fieldsMarkup = fields.length
      ? `
          <ul class="sidebar-source-field-list">
            ${fields
              .map(
                (field) => `
                  <li class="sidebar-source-field">
                    <span class="sidebar-source-field-name">
                      ${sourceFieldIconMarkup(field.dataType)}
                      <span class="sidebar-source-field-name-text">${escapeHtml(field.name)}</span>
                    </span>
                    <span class="sidebar-source-field-type">${escapeHtml(field.dataType)}</span>
                  </li>
                `
              )
              .join("")}
          </ul>
        `
      : '<p class="sidebar-source-inspector-empty">No fields are available for this source object.</p>';

    return `
      <header class="sidebar-source-inspector-header">
        <div class="sidebar-source-inspector-copy">
          <h3 class="sidebar-source-inspector-title">${escapeHtml(objectName)}</h3>
          <p class="sidebar-source-inspector-meta">${escapeHtml(objectKind)} - ${escapeHtml(fieldCountLabel)}</p>
        </div>
      </header>
      <div class="sidebar-source-inspector-body">
        ${publicationMarkup(sourceObjectRoot)}
        ${fieldsMarkup}
      </div>
    `;
  }

  function sourceInspectorLoadingMarkup(sourceObjectRoot) {
    return `
      <header class="sidebar-source-inspector-header">
        <div class="sidebar-source-inspector-copy">
          <h3 class="sidebar-source-inspector-title">${escapeHtml(sourceObjectDisplayName(sourceObjectRoot))}</h3>
          <p class="sidebar-source-inspector-meta">${escapeHtml(sourceObjectDisplayKind(sourceObjectRoot))}</p>
        </div>
      </header>
      <div class="sidebar-source-inspector-loading">
        <span class="sidebar-loading-spinner" aria-hidden="true"></span>
        <span>Loading fields...</span>
      </div>
    `;
  }

  function sourceInspectorErrorMarkup(sourceObjectRoot, message) {
    return `
      <header class="sidebar-source-inspector-header">
        <div class="sidebar-source-inspector-copy">
          <h3 class="sidebar-source-inspector-title">${escapeHtml(sourceObjectDisplayName(sourceObjectRoot))}</h3>
          <p class="sidebar-source-inspector-meta">${escapeHtml(sourceObjectDisplayKind(sourceObjectRoot))}</p>
        </div>
      </header>
      <p class="sidebar-source-inspector-empty">${escapeHtml(message)}</p>
    `;
  }

  function renderSourceInspectorMarkup(markup, hidden = false) {
    const inspectorRoot = sourceInspector();
    const inspectorPanel = sourceInspectorPanel();
    if (!inspectorRoot || !inspectorPanel) {
      return;
    }

    if (hidden) {
      inspectorRoot.hidden = true;
      inspectorPanel.innerHTML = "";
      return;
    }

    inspectorPanel.innerHTML = markup;
    inspectorRoot.hidden = false;
  }

  function renderSourceInspector(sourceObjectRoot = null, fields = []) {
    if (!(sourceObjectRoot instanceof Element)) {
      renderSourceInspectorMarkup("", true);
      return;
    }

    renderSourceInspectorMarkup(
      sourceInspectorMarkup(sourceObjectRoot, normalizeSourceObjectFields(fields))
    );
  }

  function renderSourceInspectorLoading(sourceObjectRoot) {
    if (!(sourceObjectRoot instanceof Element)) {
      renderSourceInspectorMarkup("", true);
      return;
    }

    renderSourceInspectorMarkup(sourceInspectorLoadingMarkup(sourceObjectRoot));
  }

  function renderSourceInspectorError(sourceObjectRoot, message) {
    if (!(sourceObjectRoot instanceof Element)) {
      renderSourceInspectorMarkup("", true);
      return;
    }

    renderSourceInspectorMarkup(
      sourceInspectorErrorMarkup(
        sourceObjectRoot,
        message || "The fields could not be loaded for this source object."
      )
    );
  }

  function localWorkspaceInspectorMarkup(sourceObjectRoot) {
    const objectName = sourceObjectDisplayName(sourceObjectRoot);
    const folderPath = normalizeLocalWorkspaceFolderPath(
      String(sourceObjectRoot?.dataset.localWorkspaceFolderPath || "").trim()
    );
    const exportFormat = String(sourceObjectRoot?.dataset.localWorkspaceExportFormat || "file")
      .trim()
      .toUpperCase();
    const createdAt = formatVersionTimestamp(
      String(sourceObjectRoot?.dataset.localWorkspaceCreatedAt || "").trim()
    );
    const sizeLabel = formatByteCount(sourceObjectRoot?.dataset.localWorkspaceSizeBytes);
    const columnCount = Number(sourceObjectRoot?.dataset.localWorkspaceColumnCount || 0) || 0;
    const rowCount = Number(sourceObjectRoot?.dataset.localWorkspaceRowCount || 0) || 0;

    return `
      <header class="sidebar-source-inspector-header">
        <div class="sidebar-source-inspector-copy">
          <h3 class="sidebar-source-inspector-title">${escapeHtml(objectName)}</h3>
          <p class="sidebar-source-inspector-meta">${escapeHtml(exportFormat)} FILE - ${escapeHtml(
            localWorkspaceDisplayPath(folderPath, objectName)
          )}</p>
        </div>
      </header>
      <div class="sidebar-source-inspector-body">
        ${publicationMarkup(sourceObjectRoot)}
        <ul class="sidebar-source-field-list">
          <li class="sidebar-source-field">
            <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Folder path</span></span>
            <span class="sidebar-source-field-type">${escapeHtml(folderPath || "Root")}</span>
          </li>
          <li class="sidebar-source-field">
            <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Storage backend</span></span>
            <span class="sidebar-source-field-type">IndexedDB</span>
          </li>
          <li class="sidebar-source-field">
            <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Saved at</span></span>
            <span class="sidebar-source-field-type">${escapeHtml(createdAt)}</span>
          </li>
          <li class="sidebar-source-field">
            <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Size</span></span>
            <span class="sidebar-source-field-type">${escapeHtml(sizeLabel)}</span>
          </li>
          <li class="sidebar-source-field">
            <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Columns</span></span>
            <span class="sidebar-source-field-type">${escapeHtml(String(columnCount))}</span>
          </li>
          <li class="sidebar-source-field">
            <span class="sidebar-source-field-name"><span class="sidebar-source-field-name-text">Rows</span></span>
            <span class="sidebar-source-field-type">${escapeHtml(String(rowCount))}</span>
          </li>
        </ul>
      </div>
    `;
  }

  return {
    localWorkspaceInspectorMarkup,
    renderSourceInspector,
    renderSourceInspectorError,
    renderSourceInspectorLoading,
    renderSourceInspectorMarkup,
  };
}
